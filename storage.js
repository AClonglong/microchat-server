'use strict';
/**
 * 微聊存储层 —— 三种后端，接口完全一致（全部 async）
 *
 *   1. postgres  外部 Postgres（DATABASE_URL）—— 云端部署用，重启/休眠都不丢数据
 *   2. sqlite    Node 22 内置 node:sqlite —— 本地/自建服务器用
 *   3. json      JSON 文件全量落盘 —— 兜底，老 Node 也能跑
 *
 * 云平台（Render 免费版）的容器文件系统是临时的，重启即清空，
 * 所以云端必须用 postgres 或外部数据库；sqlite/json 只适合有持久磁盘的机器。
 *
 * 统一返回：所有方法返回 Promise。
 */

const fs = require('fs');
const path = require('path');

/* ------------------------------------------------------------------ 工具 */

function wrapUser(u) {
  if (!u) return null;
  return {
    id: Number(u.id),
    username: u.username,
    nickname: u.nickname,
    salt: u.salt,
    hash: u.hash,
    created_at: Number(u.created_at || 0),
  };
}

function normMsg(m) {
  if (!m) return null;
  return {
    id: Number(m.id),
    from_id: Number(m.from_id),
    to_id: Number(m.to_id),
    body: m.body,
    client_id: m.client_id == null ? null : String(m.client_id),
    read_flag: Number(m.read_flag || 0),
    created_at: Number(m.created_at || 0),
  };
}

/* ------------------------------------------------------------------ JSON 后端 */

function openJson(DATA_DIR) {
  const file = path.join(DATA_DIR, 'microchat.json');
  const state = fs.existsSync(file)
    ? JSON.parse(fs.readFileSync(file, 'utf8'))
    : { users: [], friends: [], messages: [], seq: { user: 0, msg: 0 } };

  let timer = null;
  const flush = () => {
    if (!timer) return;
    clearTimeout(timer);
    timer = null;
    try {
      const tmp = file + '.tmp';
      fs.writeFileSync(tmp, JSON.stringify(state));
      fs.renameSync(tmp, file);
    } catch (e) {
      console.error('[db] 写 JSON 失败：', e.message);
    }
  };
  // 合并短时间内的多次写，避免每条消息都同步落盘
  const save = () => {
    if (!timer) timer = setTimeout(flush, 300);
  };
  setInterval(flush, 5000).unref();
  process.on('exit', flush);

  const api = {
    kind: 'json',
    async init() {},
    async ping() {},

    async getUserByName(n) {
      return wrapUser(state.users.find((u) => u.username === n));
    },
    async getUserById(id) {
      return wrapUser(state.users.find((u) => u.id === Number(id)));
    },
    async countUsers() {
      return { n: state.users.length };
    },
    async insUser(username, nickname, salt, hash, at) {
      const u = { id: ++state.seq.user, username, nickname, salt, hash, created_at: at };
      state.users.push(u);
      save();
      return { changes: 1, lastInsertRowid: u.id };
    },

    async friends(id) {
      return state.friends
        .filter((f) => f.a === id)
        .map((f) => wrapUser(state.users.find((u) => u.id === f.b)))
        .filter(Boolean);
    },
    async hasFriend(a, b) {
      return state.friends.some((f) => f.a === a && f.b === b) ? { x: 1 } : null;
    },
    async addFriend(a, b, at) {
      if (!state.friends.some((f) => f.a === a && f.b === b)) {
        state.friends.push({ a, b, created_at: at });
        save();
        return { changes: 1 };
      }
      return { changes: 0 };
    },

    async insMsg(from, to, body, clientId, at) {
      const m = {
        id: ++state.seq.msg,
        from_id: from,
        to_id: to,
        body,
        client_id: clientId,
        read_flag: 0,
        created_at: at,
      };
      state.messages.push(m);
      save();
      return { changes: 1, lastInsertRowid: m.id };
    },
    async msgById(id) {
      return normMsg(state.messages.find((m) => m.id === Number(id)));
    },
    async history(a, b, c, d, limit) {
      return state.messages
        .filter((m) => (m.from_id === a && m.to_id === b) || (m.from_id === c && m.to_id === d))
        .slice(-limit)
        .map(normMsg);
    },
    async markRead(from, to) {
      let n = 0;
      state.messages.forEach((m) => {
        if (m.from_id === from && m.to_id === to && !m.read_flag) {
          m.read_flag = 1;
          n++;
        }
      });
      if (n) save();
      return { changes: n };
    },
    async unread(id) {
      const map = new Map();
      state.messages.forEach((m) => {
        if (m.to_id === id && !m.read_flag) map.set(m.from_id, (map.get(m.from_id) || 0) + 1);
      });
      return [...map].map(([fid, n]) => ({ fid, n }));
    },
    async lastOf(a, b, c, d) {
      const list = state.messages.filter(
        (m) => (m.from_id === a && m.to_id === b) || (m.from_id === c && m.to_id === d)
      );
      return normMsg(list[list.length - 1]);
    },
  };

  return { db: api, flush };
}

/* ------------------------------------------------------------------ SQLite 后端 */

function openSqlite(DATA_DIR) {
  const { DatabaseSync } = require('node:sqlite');
  const file = path.join(DATA_DIR, 'microchat.db');
  const sqlite = new DatabaseSync(file);
  sqlite.exec('PRAGMA journal_mode = WAL');
  sqlite.exec(`
    CREATE TABLE IF NOT EXISTS users (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      username TEXT UNIQUE NOT NULL,
      nickname TEXT NOT NULL,
      salt TEXT NOT NULL,
      hash TEXT NOT NULL,
      created_at INTEGER NOT NULL
    );
    CREATE TABLE IF NOT EXISTS friends (
      a INTEGER NOT NULL, b INTEGER NOT NULL, created_at INTEGER NOT NULL,
      PRIMARY KEY (a, b)
    );
    CREATE TABLE IF NOT EXISTS messages (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      from_id INTEGER NOT NULL, to_id INTEGER NOT NULL,
      body TEXT NOT NULL,
      client_id TEXT,
      read_flag INTEGER NOT NULL DEFAULT 0,
      created_at INTEGER NOT NULL
    );
    CREATE INDEX IF NOT EXISTS idx_msg_pair ON messages(from_id, to_id, id);
    CREATE INDEX IF NOT EXISTS idx_msg_to ON messages(to_id, id);
  `);

  const cache = new Map();
  const stmt = (sql) => {
    let s = cache.get(sql);
    if (!s) {
      s = sqlite.prepare(sql);
      cache.set(sql, s);
    }
    return s;
  };

  const api = {
    kind: 'sqlite',
    async init() {},
    async ping() {},

    async getUserByName(n) {
      return wrapUser(stmt('SELECT * FROM users WHERE username = ?').get(n));
    },
    async getUserById(id) {
      return wrapUser(stmt('SELECT * FROM users WHERE id = ?').get(Number(id)));
    },
    async countUsers() {
      return stmt('SELECT COUNT(*) AS n FROM users').get() || { n: 0 };
    },
    async insUser(username, nickname, salt, hash, at) {
      return stmt('INSERT INTO users(username, nickname, salt, hash, created_at) VALUES (?,?,?,?,?)').run(
        username, nickname, salt, hash, at
      );
    },

    async friends(id) {
      return stmt(
        'SELECT u.id, u.username, u.nickname FROM friends f JOIN users u ON u.id = f.b WHERE f.a = ?'
      ).all(id).map(wrapUser);
    },
    async hasFriend(a, b) {
      return stmt('SELECT 1 AS x FROM friends WHERE a = ? AND b = ?').get(a, b) || null;
    },
    async addFriend(a, b, at) {
      return stmt('INSERT OR IGNORE INTO friends(a, b, created_at) VALUES (?,?,?)').run(a, b, at);
    },

    async insMsg(from, to, body, clientId, at) {
      return stmt(
        'INSERT INTO messages(from_id, to_id, body, client_id, read_flag, created_at) VALUES (?,?,?,?,0,?)'
      ).run(from, to, body, clientId, at);
    },
    async msgById(id) {
      return normMsg(stmt('SELECT * FROM messages WHERE id = ?').get(Number(id)));
    },
    async history(a, b, c, d, limit) {
      return stmt(
        `SELECT * FROM (SELECT * FROM messages
          WHERE (from_id = ? AND to_id = ?) OR (from_id = ? AND to_id = ?)
          ORDER BY id DESC LIMIT ?) ORDER BY id ASC`
      ).all(a, b, c, d, limit).map(normMsg);
    },
    async markRead(from, to) {
      return stmt(
        'UPDATE messages SET read_flag = 1 WHERE from_id = ? AND to_id = ? AND read_flag = 0'
      ).run(from, to);
    },
    async unread(id) {
      return stmt(
        'SELECT from_id AS fid, COUNT(*) AS n FROM messages WHERE to_id = ? AND read_flag = 0 GROUP BY from_id'
      ).all(id).map((r) => ({ fid: Number(r.fid), n: Number(r.n) }));
    },
    async lastOf(a, b, c, d) {
      return normMsg(
        stmt(
          `SELECT * FROM messages WHERE (from_id = ? AND to_id = ?) OR (from_id = ? AND to_id = ?)
           ORDER BY id DESC LIMIT 1`
        ).get(a, b, c, d)
      );
    },
  };

  return { db: api, file };
}

/* ------------------------------------------------------------------ Postgres 后端 */

/**
 * 零依赖 Postgres 客户端：只说 Postgres 的前端/后端协议 v3。
 *
 * 云平台给的连接串（DATABASE_URL）通常是最简单的
 *   postgresql://user:pass@host:5432/dbname?sslmode=require
 * 为了不引入 node-postgres（会让镜像大几 MB 且需要联网装包），
 * 这里手写最简单的握手 + 扩展查询协议；用不到连接池、用不到监听通知。
 * 每条语句开一条短连接，聊天这种量级完全够；同时开 SSL 由 sslmode 决定。
 */
const net = require('net');
const tls = require('tls');

function parseUrl(url) {
  const u = new URL(url);
  return {
    host: u.hostname,
    port: Number(u.port || 5432),
    user: decodeURIComponent(u.username || 'postgres'),
    password: decodeURIComponent(u.password || ''),
    database: decodeURIComponent(u.pathname.replace(/^\//, '') || 'postgres'),
    sslmode: u.searchParams.get('sslmode') || '',
  };
}

function pgStartup(user, database) {
  const params = Buffer.from(`user\0${user}\0database\0${database}\0client_encoding\0UTF8\0\0`, 'utf8');
  // 报文 = 长度(4) + 协议号(4) + 参数，长度字段含自身 4 字节
  const buf = Buffer.alloc(8 + params.length);
  buf.writeInt32BE(8 + params.length, 0);
  buf.writeInt32BE(196608, 4); // 协议版本 3.0
  params.copy(buf, 8);
  return buf;
}

function pgMsg(type, body) {
  const b = Buffer.alloc(1 + 4 + body.length);
  b.write(type, 0, 1, 'ascii');
  b.writeInt32BE(4 + body.length, 1);
  body.copy(b, 5);
  return b;
}

function cstr(s) {
  return Buffer.from(String(s) + '\0', 'utf8');
}

/** 解析一条服务端消息，返回 {type, body}，不够就返回 null */
function pgParse(buf) {
  if (buf.length < 5) return null;
  const type = String.fromCharCode(buf[0]);
  const len = buf.readInt32BE(1);
  if (buf.length < 1 + len) return null;
  return { type, body: buf.slice(5, 1 + len), used: 1 + len };
}

/** 极简查询：返回 rows（对象数组）与 rowCount */
function pgQuery(conn, text, values) {
  return new Promise((resolve, reject) => {
    const chunks = [pgMsg('Q', cstr(text))];
    conn.write(Buffer.concat(chunks));

    let buf = Buffer.alloc(0);
    let cols = [];
    const rows = [];
    let err = null;
    let done = false;

    const onData = (d) => {
      buf = Buffer.concat([buf, d]);
      for (;;) {
        const m = pgParse(buf);
        if (!m) break;
        buf = buf.slice(m.used);
        if (m.type === 'T') {
          // RowDescription：解析列名
          cols = [];
          const n = m.body.readInt16BE(0);
          let off = 2;
          for (let i = 0; i < n; i++) {
            const end = m.body.indexOf(0, off);
            cols.push(m.body.slice(off, end).toString('utf8'));
            off = end + 1 + 18;
          }
        } else if (m.type === 'D') {
          // DataRow
          const n = m.body.readInt16BE(0);
          let off = 2;
          const row = {};
          for (let i = 0; i < n; i++) {
            const len = m.body.readInt32BE(off);
            off += 4;
            if (len === -1) {
              row[cols[i]] = null;
            } else {
              row[cols[i]] = m.body.slice(off, off + len).toString('utf8');
              off += len;
            }
          }
          rows.push(row);
        } else if (m.type === 'E') {
          // ErrorResponse：取 M 字段（message）
          let off = 0;
          let msg = '';
          while (off < m.body.length && m.body[off] !== 0) {
            const code = String.fromCharCode(m.body[off]);
            const end = m.body.indexOf(0, off + 1);
            const val = m.body.slice(off + 1, end).toString('utf8');
            if (code === 'M') msg = val;
            off = end + 1;
          }
          err = new Error(msg || 'postgres error');
        } else if (m.type === 'Z') {
          // ReadyForQuery
          done = true;
        }
      }
      if (done) {
        conn.off('data', onData);
        if (err) reject(err);
        else resolve({ rows, rowCount: rows.length });
      }
    };

    conn.on('data', onData);
    conn.once('error', reject);
    conn.once('close', () => {
      if (!done) reject(new Error('postgres 连接意外关闭'));
    });
  });
}

function pgConnect(cfg) {
  return new Promise((resolve, reject) => {
    const needSsl = cfg.sslmode !== 'disable' && cfg.sslmode !== 'allow';
    let settled = false;

    const finish = (sock) => {
      if (settled) return;
      settled = true;
      resolve(sock);
    };

    const startTls = (sock) => {
      // 主机是 IP 时不要传 servername（RFC 6066 不允许），否则 Node 会告警
      const opts = { socket: sock, rejectUnauthorized: false };
      if (!net.isIP(cfg.host)) opts.servername = cfg.host;
      const secure = tls.connect(opts, () => {
        finish(secure);
      });
      secure.once('error', (e) => {
        if (!settled) {
          settled = true;
          reject(e);
        }
      });
    };

    const sock = net.connect({ host: cfg.host, port: cfg.port }, () => {
      if (!needSsl) return finish(sock);
      // SSLRequest
      const req = Buffer.alloc(8);
      req.writeInt32BE(8, 0);
      req.writeInt32BE(80877103, 4);
      sock.write(req);
    });

    const onFirst = (d) => {
      if (needSsl) {
        sock.off('data', onFirst);
        if (d[0] === 0x53 /* S */) return startTls(sock);
        // 服务端不支持 SSL，退回明文
        return finish(sock);
      }
      sock.off('data', onFirst);
      finish(sock);
    };
    sock.once('data', onFirst);

    sock.once('error', (e) => {
      if (!settled) {
        settled = true;
        reject(e);
      }
    });
  });
}

/** 认证：支持 AuthenticationOk / MD5 / SASL(SCRAM-SHA-256) */
async function pgAuth(sock, cfg) {
  let buf = Buffer.alloc(0);

  const readMsg = () =>
    new Promise((resolve, reject) => {
      const check = () => {
        const m = pgParse(buf);
        if (!m) return null;
        buf = buf.slice(m.used);
        return m;
      };
      const got = check();
      if (got) return resolve(got);
      const onData = (d) => {
        buf = Buffer.concat([buf, d]);
        const m = check();
        if (m) {
          sock.off('data', onData);
          resolve(m);
        }
      };
      sock.on('data', onData);
      sock.once('error', reject);
    });

  // 1) 启动
  sock.write(pgStartup(cfg.user, cfg.database));

  let scram = null;

  for (let i = 0; i < 20; i++) {
    const m = await readMsg();
    if (m.type === 'E') throw new Error('Postgres 认证失败：' + m.body.toString('utf8'));
    if (m.type === 'Z') return; // ReadyForQuery，说明免密登录成功

    if (m.type === 'R') {
      const code = m.body.readInt32BE(0);
      if (code === 0) continue; // AuthenticationOk

      if (code === 5) {
        // MD5
        const salt = m.body.slice(4, 8);
        const inner = require('crypto')
          .createHash('md5')
          .update(cfg.password + cfg.user)
          .digest('hex');
        const outer = require('crypto')
          .createHash('md5')
          .update(Buffer.concat([Buffer.from(inner, 'hex'), salt]))
          .digest('hex');
        sock.write(pgMsg('p', cstr('md5' + outer)));
        continue;
      }

      if (code === 10) {
        // SASL，列出机制
        const mechs = m.body.slice(4).toString('utf8').split('\0').filter(Boolean);
        if (!mechs.includes('SCRAM-SHA-256')) throw new Error('Postgres 不支持 SCRAM-SHA-256');
        const nonce = require('crypto').randomBytes(18).toString('base64');
        const clientFirstBare = 'n=,r=' + nonce;
        scram = { nonce, clientFirstBare };
        const first = 'n,,' + clientFirstBare;
        // SASLInitialResponse = 机制名 + \0 + int32 长度 + 首条消息
        const mechBuf = Buffer.from('SCRAM-SHA-256\0', 'ascii');
        const firstBuf = Buffer.from(first, 'utf8');
        const body = Buffer.alloc(mechBuf.length + 4 + firstBuf.length);
        mechBuf.copy(body, 0);
        body.writeInt32BE(firstBuf.length, mechBuf.length);
        firstBuf.copy(body, mechBuf.length + 4);
        sock.write(pgMsg('p', body));
        continue;
      }

      if (code === 11) {
        // SASLContinue
        const serverFirst = m.body.slice(4).toString('utf8');
        const attrs = {};
        serverFirst.split(',').forEach((kv) => {
          attrs[kv[0]] = kv.slice(2);
        });
        const salt = Buffer.from(attrs.s, 'base64');
        const iter = Number(attrs.i);
        const salted = require('crypto').pbkdf2Sync(cfg.password.normalize('NFKC'), salt, iter, 32, 'sha256');
        const clientKey = require('crypto')
          .createHmac('sha256', salted)
          .update('Client Key')
          .digest();
        const storedKey = require('crypto').createHash('sha256').update(clientKey).digest();
        const clientFinalNoProof = 'c=biws,r=' + attrs.r;
        const authMessage = scram.clientFirstBare + ',' + serverFirst + ',' + clientFinalNoProof;
        const clientSig = require('crypto')
          .createHmac('sha256', storedKey)
          .update(authMessage)
          .digest();
        const proof = Buffer.alloc(clientKey.length);
        for (let k = 0; k < clientKey.length; k++) proof[k] = clientKey[k] ^ clientSig[k];
        const clientFinal = clientFinalNoProof + ',p=' + proof.toString('base64');
        sock.write(pgMsg('p', Buffer.from(clientFinal, 'utf8')));
        continue;
      }

      if (code === 12) continue; // SASLFinal
      throw new Error('Postgres 认证方式不支持（code=' + code + '）');
    }
  }
  throw new Error('Postgres 认证超时');
}

function openPostgres(url, log) {
  const cfg = parseUrl(url);

  // 每条语句一条短连接：实现简单、无状态残留，聊天这种频次足够
  const q = async (text, values) => {
    // 参数内联（服务端全程使用 $1..$n 的等价物：这里手工转义成字面量）
    const sql = interpolate(text, values || []);
    const sock = await pgConnect(cfg);
    try {
      await pgAuth(sock, cfg);
      const r = await pgQuery(sock, sql);
      return r;
    } finally {
      try {
        sock.end();
      } catch (e) {
        /* ignore */
      }
    }
  };

  const one = async (text, values) => {
    const r = await q(text, values);
    return r.rows[0] || null;
  };

  const api = {
    kind: 'postgres',
    url: cfg.host + ':' + cfg.port + '/' + cfg.database,

    async init() {
      await q(`
        CREATE TABLE IF NOT EXISTS users (
          id BIGSERIAL PRIMARY KEY,
          username TEXT UNIQUE NOT NULL,
          nickname TEXT NOT NULL,
          salt TEXT NOT NULL,
          hash TEXT NOT NULL,
          created_at BIGINT NOT NULL
        )`);
      await q(`
        CREATE TABLE IF NOT EXISTS friends (
          a BIGINT NOT NULL, b BIGINT NOT NULL, created_at BIGINT NOT NULL,
          PRIMARY KEY (a, b)
        )`);
      await q(`
        CREATE TABLE IF NOT EXISTS messages (
          id BIGSERIAL PRIMARY KEY,
          from_id BIGINT NOT NULL, to_id BIGINT NOT NULL,
          body TEXT NOT NULL,
          client_id TEXT,
          read_flag INTEGER NOT NULL DEFAULT 0,
          created_at BIGINT NOT NULL
        )`);
      await q('CREATE INDEX IF NOT EXISTS idx_msg_pair ON messages(from_id, to_id, id)');
      await q('CREATE INDEX IF NOT EXISTS idx_msg_to ON messages(to_id, id)');
      await api.ping();
    },

    async ping() {
      await one('SELECT 1 AS ok');
    },

    async getUserByName(n) {
      return wrapUser(await one('SELECT * FROM users WHERE username = $1', [n]));
    },
    async getUserById(id) {
      return wrapUser(await one('SELECT * FROM users WHERE id = $1', [Number(id)]));
    },
    async countUsers() {
      const r = await one('SELECT COUNT(*)::int AS n FROM users');
      return { n: r ? Number(r.n) : 0 };
    },
    async insUser(username, nickname, salt, hash, at) {
      const r = await one(
        'INSERT INTO users(username, nickname, salt, hash, created_at) VALUES ($1,$2,$3,$4,$5) RETURNING id',
        [username, nickname, salt, hash, at]
      );
      return { changes: 1, lastInsertRowid: Number(r.id) };
    },

    async friends(id) {
      const r = await q(
        'SELECT u.id, u.username, u.nickname FROM friends f JOIN users u ON u.id = f.b WHERE f.a = $1',
        [id]
      );
      return r.rows.map(wrapUser);
    },
    async hasFriend(a, b) {
      return one('SELECT 1 AS x FROM friends WHERE a = $1 AND b = $2', [a, b]);
    },
    async addFriend(a, b, at) {
      await q('INSERT INTO friends(a, b, created_at) VALUES ($1,$2,$3) ON CONFLICT DO NOTHING', [a, b, at]);
      return { changes: 1 };
    },

    async insMsg(from, to, body, clientId, at) {
      const r = await one(
        'INSERT INTO messages(from_id, to_id, body, client_id, read_flag, created_at) VALUES ($1,$2,$3,$4,0,$5) RETURNING id',
        [from, to, body, clientId, at]
      );
      return { changes: 1, lastInsertRowid: Number(r.id) };
    },
    async msgById(id) {
      return normMsg(await one('SELECT * FROM messages WHERE id = $1', [Number(id)]));
    },
    async history(a, b, c, d, limit) {
      const r = await q(
        `SELECT * FROM (SELECT * FROM messages
          WHERE (from_id = $1 AND to_id = $2) OR (from_id = $3 AND to_id = $4)
          ORDER BY id DESC LIMIT $5) t ORDER BY id ASC`,
        [a, b, c, d, limit]
      );
      return r.rows.map(normMsg);
    },
    async markRead(from, to) {
      await q('UPDATE messages SET read_flag = 1 WHERE from_id = $1 AND to_id = $2 AND read_flag = 0', [
        from,
        to,
      ]);
      return { changes: 1 };
    },
    async unread(id) {
      const r = await q(
        'SELECT from_id AS fid, COUNT(*)::int AS n FROM messages WHERE to_id = $1 AND read_flag = 0 GROUP BY from_id',
        [id]
      );
      return r.rows.map((x) => ({ fid: Number(x.fid), n: Number(x.n) }));
    },
    async lastOf(a, b, c, d) {
      return normMsg(
        await one(
          `SELECT * FROM messages WHERE (from_id = $1 AND to_id = $2) OR (from_id = $3 AND to_id = $4)
           ORDER BY id DESC LIMIT 1`,
          [a, b, c, d]
        )
      );
    },
  };

  return { db: api };
}

/** 把 $1..$n 占位符替换成安全的 SQL 字面量（供上面的极简客户端使用） */
function interpolate(text, values) {
  let out = '';
  let i = 0;
  while (i < text.length) {
    const ch = text[i];
    if (ch === '$' && /\d/.test(text[i + 1] || '')) {
      let j = i + 1;
      while (j < text.length && /\d/.test(text[j])) j++;
      const idx = Number(text.slice(i + 1, j)) - 1;
      out += sqlLiteral(values[idx]);
      i = j;
    } else {
      out += ch;
      i++;
    }
  }
  return out;
}

function sqlLiteral(v) {
  if (v === null || v === undefined) return 'NULL';
  if (typeof v === 'number' || typeof v === 'bigint') return String(v);
  if (typeof v === 'boolean') return v ? 'TRUE' : 'FALSE';
  return "'" + String(v).replace(/'/g, "''") + "'";
}

/* ------------------------------------------------------------------ 入口 */

/**
 * 打开存储。优先级：
 *   DATABASE_URL 有值 → postgres
 *   否则 → sqlite（失败再退 json）
 * 另外可用 STORAGE=json 强制走文件（调试用）
 */
async function open(DATA_DIR, env) {
  const e = env || process.env;
  const wantPg = (e.DATABASE_URL || '').trim();
  const force = (e.STORAGE || '').trim().toLowerCase();

  if (wantPg && force !== 'json' && force !== 'sqlite') {
    const { db } = openPostgres(wantPg);
    await db.init(); // 第一次连不上就直接抛，让启动脚本给出明确提示
    console.log('[db] 使用 Postgres：' + db.url);
    return { db, flush: async () => {} };
  }

  if (force !== 'json') {
    try {
      const r = openSqlite(DATA_DIR);
      console.log('[db] 使用 SQLite：' + r.file);
      return { db: r.db, flush: async () => {} };
    } catch (err) {
      console.warn('[db] node:sqlite 不可用（' + err.message + '），降级为 JSON 文件存储');
    }
  }

  const r = openJson(DATA_DIR);
  await r.db.init();
  console.log('[db] 使用 JSON 文件：' + path.join(DATA_DIR, 'microchat.json'));
  return { db: r.db, flush: r.flush };
}

module.exports = { open, wrapUser, normMsg };

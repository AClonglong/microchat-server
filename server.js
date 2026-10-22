#!/usr/bin/env node
/**
 * 微聊服务端 —— 零依赖单文件实现
 *
 * 功能：
 *   - 用户名 + 密码注册 / 登录（PBKDF2-SHA256 加盐哈希，不存明文）
 *   - 好友关系、会话列表、未读数
 *   - WebSocket 长连接实时收发消息、在线状态、已读回执
 *   - 离线消息存储（上线后拉取）
 *   - SQLite 持久化（复用 Node 22 内置 node:sqlite；不可用时自动降级为 JSON 文件）
 *
 * 启动：node server.js [--port 8080] [--data ./data]
 */

'use strict';

const http = require('http');
const crypto = require('crypto');
const fs = require('fs');
const path = require('path');
const os = require('os');

// ---------------------------------------------------------------- 参数
function argOf(name, def) {
  const i = process.argv.indexOf('--' + name);
  return i > -1 && process.argv[i + 1] ? process.argv[i + 1] : def;
}
const PORT = parseInt(argOf('port', process.env.PORT || '8080'), 10);
const DATA_DIR = path.resolve(argOf('data', process.env.DATA_DIR || path.join(__dirname, 'data')));

if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });

// ---------------------------------------------------------------- 存储层
/**
 * 具体后端在 storage.js：
 *   - DATABASE_URL 有值 → Postgres（云端部署必须用这个，重启不丢数据）
 *   - 否则 → SQLite（本地/自建服务器）
 *   - 都不行 → JSON 文件兜底
 * 全部接口是 async，启动时 await storage.open() 再开始监听。
 */
const storage = require('./storage');
const VERSION = require('./package.json').version || '1.0';
let db = null;


// ---------------------------------------------------------------- 密码
function hashPassword(password, salt) {
  const s = salt || crypto.randomBytes(16).toString('hex');
  const h = crypto.pbkdf2Sync(password, s, 120000, 32, 'sha256').toString('hex');
  return { salt: s, hash: h };
}

function verifyPassword(password, salt, expected) {
  const h = crypto.pbkdf2Sync(password, salt, 120000, 32, 'sha256').toString('hex');
  // 定长比较，避免时序侧信道
  const a = Buffer.from(h, 'hex');
  const b = Buffer.from(expected, 'hex');
  return a.length === b.length && crypto.timingSafeEqual(a, b);
}

// ---------------------------------------------------------------- Token
const SECRET = process.env.MICROCHAT_SECRET || crypto.randomBytes(32).toString('hex');
console.log('[auth] SECRET ' + (process.env.MICROCHAT_SECRET ? '来自环境变量（固定）' : '为随机值，重启后旧 token 失效'));

function b64url(buf) {
  return Buffer.from(buf).toString('base64').replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
}
function b64urlDecode(str) {
  return Buffer.from(str.replace(/-/g, '+').replace(/_/g, '/'), 'base64').toString('utf8');
}

function signToken(userId) {
  const payload = b64url(JSON.stringify({ uid: userId, exp: Date.now() + 30 * 864e5, t: Date.now() }));
  const sig = b64url(crypto.createHmac('sha256', SECRET).update(payload).digest());
  return payload + '.' + sig;
}

function verifyToken(token) {
  if (!token || token.indexOf('.') < 0) return null;
  const [payload, sig] = token.split('.');
  const expect = b64url(crypto.createHmac('sha256', SECRET).update(payload).digest());
  if (sig !== expect) return null;
  try {
    const obj = JSON.parse(b64urlDecode(payload));
    if (!obj.uid || obj.exp < Date.now()) return null;
    return obj;
  } catch (e) {
    return null;
  }
}

// ---------------------------------------------------------------- 工具
const now = () => Date.now();

function publicUser(u) {
  return { id: u.id, username: u.username, nickname: u.nickname };
}

function normalizeUsername(s) {
  return String(s || '').trim().toLowerCase();
}

function validUsername(s) {
  return /^[a-z0-9_]{3,20}$/.test(s);
}

function readBody(req) {
  return new Promise((resolve, reject) => {
    let size = 0;
    const chunks = [];
    req.on('data', (c) => {
      size += c.length;
      if (size > 1e6) {
        reject(new Error('body too large'));
        req.destroy();
      } else chunks.push(c);
    });
    req.on('end', () => {
      if (!chunks.length) return resolve({});
      try {
        resolve(JSON.parse(Buffer.concat(chunks).toString('utf8')));
      } catch (e) {
        reject(new Error('invalid json'));
      }
    });
    req.on('error', reject);
  });
}

const CORS = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'Content-Type,Authorization',
  'Access-Control-Allow-Methods': 'GET,POST,OPTIONS',
};

function send(res, code, obj) {
  const buf = Buffer.from(JSON.stringify(obj), 'utf8');
  res.writeHead(code, {
    'Content-Type': 'application/json; charset=utf-8',
    'Content-Length': buf.length,
    'Cache-Control': 'no-store',
    ...CORS,
  });
  res.end(buf);
}

// ---------------------------------------------------------------- 在线连接表
/** userId -> Set<WebSocket> */
const online = new Map();

function setOnline(uid, ws) {
  if (!online.has(uid)) online.set(uid, new Set());
  online.get(uid).add(ws);
}
function setOffline(uid, ws) {
  const s = online.get(uid);
  if (!s) return;
  s.delete(ws);
  if (!s.size) online.delete(uid);
}
function isOnline(uid) {
  return online.has(uid);
}
function pushTo(uid, payload) {
  const s = online.get(uid);
  if (!s) return false;
  const text = JSON.stringify(payload);
  for (const ws of s) {
    if (ws.readyState === 1) {
      try {
        ws.send(text);
      } catch (e) {
        /* ignore */
      }
    }
  }
  return true;
}

// ---------------------------------------------------------------- WebSocket 实现（RFC 6455 最小子集）
function wsAccept(req, socket, head) {
  const key = req.headers['sec-websocket-key'];
  if (!key) {
    socket.end('HTTP/1.1 400 Bad Request\r\n\r\n');
    return null;
  }
  const accept = crypto
    .createHash('sha1')
    .update(key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11')
    .digest('base64');
  socket.write(
    'HTTP/1.1 101 Switching Protocols\r\n' +
      'Upgrade: websocket\r\n' +
      'Connection: Upgrade\r\n' +
      'Sec-WebSocket-Accept: ' + accept + '\r\n\r\n'
  );
  return new WsConn(socket, head);
}

class WsConn {
  constructor(socket, head) {
    this.socket = socket;
    this.buf = Buffer.alloc(0);
    this.alive = true;
    this.handlers = { message: [], close: [] };
    socket.on('data', (d) => this._onData(d));
    socket.on('close', () => this._closed());
    socket.on('error', () => this._closed());
    socket.setTimeout(0);
    if (head && head.length) this._onData(head);
  }
  get readyState() {
    return this.alive ? 1 : 3;
  }
  on(evt, fn) {
    if (this.handlers[evt]) this.handlers[evt].push(fn);
  }
  _closed() {
    if (!this.alive) return;
    this.alive = false;
    this.handlers.close.forEach((f) => f());
  }
  _onData(d) {
    this.buf = Buffer.concat([this.buf, d]);
    for (;;) {
      const f = this._parse();
      if (!f) return;
      if (f.op === 0x8) {
        this.close();
        return;
      }
      if (f.op === 0x9) {
        this._write(0xa, f.payload);
        continue;
      }
      if (f.op === 0x1 || f.op === 0x2) {
        const text = f.payload.toString('utf8');
        this.handlers.message.forEach((fn) => fn(text));
      }
    }
  }
  _parse() {
    const b = this.buf;
    if (b.length < 2) return null;
    const fin = (b[0] & 0x80) !== 0;
    const op = b[0] & 0x0f;
    const masked = (b[1] & 0x80) !== 0;
    let len = b[1] & 0x7f;
    let off = 2;
    if (len === 126) {
      if (b.length < 4) return null;
      len = b.readUInt16BE(2);
      off = 4;
    } else if (len === 127) {
      if (b.length < 10) return null;
      len = Number(b.readBigUInt64BE(2));
      off = 10;
    }
    const maskLen = masked ? 4 : 0;
    if (b.length < off + maskLen + len) return null;
    let payload = b.slice(off + maskLen, off + maskLen + len);
    if (masked) {
      const mask = b.slice(off, off + 4);
      payload = Buffer.from(payload);
      for (let i = 0; i < payload.length; i++) payload[i] ^= mask[i & 3];
    }
    this.buf = b.slice(off + maskLen + len);
    if (!fin) {
      // 暂不支持分片，直接丢弃本帧
      return this._parse();
    }
    return { op, payload };
  }
  _write(op, payload) {
    if (!this.alive) return;
    const len = payload.length;
    let header;
    if (len < 126) {
      header = Buffer.alloc(2);
      header[1] = len;
    } else if (len < 65536) {
      header = Buffer.alloc(4);
      header[1] = 126;
      header.writeUInt16BE(len, 2);
    } else {
      header = Buffer.alloc(10);
      header[1] = 127;
      header.writeBigUInt64BE(BigInt(len), 2);
    }
    header[0] = 0x80 | op;
    try {
      this.socket.write(Buffer.concat([header, payload]));
    } catch (e) {
      this._closed();
    }
  }
  send(text) {
    this._write(0x1, Buffer.from(text, 'utf8'));
  }
  ping() {
    this._write(0x9, Buffer.alloc(0));
  }
  close() {
    if (!this.alive) return;
    this._write(0x8, Buffer.alloc(0));
    this.alive = false;
    try {
      this.socket.end();
    } catch (e) {
      /* ignore */
    }
    this.handlers.close.forEach((f) => f());
  }
}

// 每 30 秒心跳，回收死连接
setInterval(() => {
  for (const [, set] of online) for (const ws of set) ws.ping();
}, 30000).unref();

// ---------------------------------------------------------------- 业务处理
async function authUser(req) {
  const h = req.headers['authorization'] || '';
  const token = h.replace(/^Bearer\s+/i, '') || req.headers['x-token'];
  const p = verifyToken(token);
  if (!p) return null;
  return db.getUserById(p.uid);
}

async function requireAuth(req, res) {
  const u = await authUser(req);
  if (!u) {
    send(res, 401, { error: 'unauthorized', message: '登录已失效，请重新登录' });
    return null;
  }
  return u;
}

function messageJson(row) {
  return {
    id: row.id,
    from: row.from_id,
    to: row.to_id,
    body: row.body,
    clientId: row.client_id,
    read: !!row.read_flag,
    time: row.created_at,
  };
}

/** 给 a 的会话列表补充预览 */
async function conversationFor(me, friend) {
  const last = await db.lastOf(me.id, friend.id, friend.id, me.id);
  const unreadRows = (await db.unread(me.id)).find((r) => r.fid === friend.id);
  return {
    user: publicUser(friend),
    online: isOnline(friend.id),
    last: last ? messageJson(last) : null,
    unread: unreadRows ? unreadRows.n : 0,
  };
}

async function handleApi(req, res, pathname) {
  // ---------- 注册 ----------
  if (pathname === '/api/register' && req.method === 'POST') {
    const { username, password, nickname } = await readBody(req);
    const uname = normalizeUsername(username);
    if (!validUsername(uname))
      return send(res, 400, { error: 'bad_username', message: '用户名需为 3-20 位字母、数字或下划线' });
    if (!password || String(password).length < 6)
      return send(res, 400, { error: 'bad_password', message: '密码至少 6 位' });
    if (await db.getUserByName(uname))
      return send(res, 409, { error: 'user_exists', message: '该用户名已被注册' });
    const nick = String(nickname || '').trim().slice(0, 20) || uname;
    const { salt, hash } = hashPassword(String(password));
    const r = await db.insUser(uname, nick, salt, hash, now());
    const uid = Number(r.lastInsertRowid);
    return send(res, 200, {
      token: signToken(uid),
      user: { id: uid, username: uname, nickname: nick },
    });
  }

  // ---------- 登录 ----------
  if (pathname === '/api/login' && req.method === 'POST') {
    const { username, password } = await readBody(req);
    const uname = normalizeUsername(username);
    const u = await db.getUserByName(uname);
    if (!u || !verifyPassword(String(password || ''), u.salt, u.hash))
      return send(res, 401, { error: 'bad_credentials', message: '用户名或密码错误' });
    return send(res, 200, { token: signToken(u.id), user: publicUser(u) });
  }

  // ---------- 我的资料 ----------
  if (pathname === '/api/me' && req.method === 'GET') {
    const u = await requireAuth(req, res);
    if (!u) return;
    return send(res, 200, { user: publicUser(u) });
  }

  // ---------- 会话列表（好友 + 最后一条消息 + 未读）----------
  if (pathname === '/api/conversations' && req.method === 'GET') {
    const me = await requireAuth(req, res);
    if (!me) return;
    const friends = await db.friends(me.id);
    const list = [];
    for (const f of friends) list.push(await conversationFor(me, f));
    list.sort((x, y) => {
      const tx = x.last ? x.last.time : 0;
      const ty = y.last ? y.last.time : 0;
      return ty - tx;
    });
    return send(res, 200, { conversations: list });
  }

  // ---------- 历史消息 ----------
  if (pathname === '/api/messages' && req.method === 'GET') {
    const me = await requireAuth(req, res);
    if (!me) return;
    const url = new URL(req.url, 'http://x');
    const peerId = parseInt(url.searchParams.get('peer') || '0', 10);
    const limit = Math.min(parseInt(url.searchParams.get('limit') || '50', 10), 200);
    if (!peerId) return send(res, 400, { error: 'bad_peer', message: '缺少 peer 参数' });
    const rows = await db.history(me.id, peerId, peerId, me.id, limit);
    // 拉取历史即视为已读
    await db.markRead(peerId, me.id);
    pushTo(peerId, { type: 'read', by: me.id });
    return send(res, 200, { messages: rows.map(messageJson) });
  }

  // ---------- 发送消息（HTTP 兜底通道）----------
  if (pathname === '/api/send' && req.method === 'POST') {
    const me = await requireAuth(req, res);
    if (!me) return;
    const { to, body, clientId } = await readBody(req);
    const toId = parseInt(to, 10);
    if (!toId || !body) return send(res, 400, { error: 'bad_args', message: '参数不完整' });
    if (!(await db.hasFriend(me.id, toId)))
      return send(res, 403, { error: 'not_friend', message: '对方还不是你的好友' });
    const text = String(body).slice(0, 4000);
    const r = await db.insMsg(me.id, toId, text, clientId || null, now());
    const row = await db.msgById(Number(r.lastInsertRowid));
    const m = messageJson(row);
    pushTo(toId, { type: 'message', message: m });
    return send(res, 200, { message: m });
  }

  // ---------- 添加好友 ----------
  if (pathname === '/api/friends/add' && req.method === 'POST') {
    const me = await requireAuth(req, res);
    if (!me) return;
    const { username } = await readBody(req);
    const target = await db.getUserByName(normalizeUsername(username));
    if (!target) return send(res, 404, { error: 'not_found', message: '用户不存在' });
    if (target.id === me.id) return send(res, 400, { error: 'self', message: '不能添加自己' });
    if (await db.hasFriend(me.id, target.id)) return send(res, 200, { user: publicUser(target) });
    await db.addFriend(me.id, target.id, now());
    // 双向好友：对方列表也立即出现
    await db.addFriend(target.id, me.id, now());
    pushTo(target.id, { type: 'friend', user: publicUser(me) });
    return send(res, 200, { user: publicUser(target) });
  }

  // ---------- 用户搜索 ----------
  if (pathname === '/api/users/search' && req.method === 'GET') {
    const me = await requireAuth(req, res);
    if (!me) return;
    const url = new URL(req.url, 'http://x');
    const q = normalizeUsername(url.searchParams.get('q'));
    if (!q) return send(res, 200, { users: [] });
    const u = await db.getUserByName(q);
    return send(res, 200, { users: u && u.id !== me.id ? [publicUser(u)] : [] });
  }

  // ---------- 健康检查 ----------
  if (pathname === '/api/ping') {
    let n = 0;
    try {
      n = (await db.countUsers()).n;
    } catch (e) {
      /* 数据库抖一下不影响存活探测 */
    }
    return send(res, 200, {
      ok: true,
      server: 'microchat',
      time: now(),
      users: n,
      storage: db.kind,
      version: VERSION,
    });
  }

  return send(res, 404, { error: 'not_found', message: '接口不存在' });
}

// ---------------------------------------------------------------- WebSocket 协议
async function onWsMessage(conn, uid, text) {
  let msg;
  try {
    msg = JSON.parse(text);
  } catch (e) {
    return;
  }
  const me = await db.getUserById(uid);
  if (!me) return;

  switch (msg.type) {
    case 'ping':
      conn.send(JSON.stringify({ type: 'pong', time: now() }));
      break;

    case 'send': {
      const toId = parseInt(msg.to, 10);
      if (!toId || !msg.body) return;
      if (!(await db.hasFriend(uid, toId))) {
        conn.send(JSON.stringify({ type: 'error', code: 'not_friend', clientId: msg.clientId }));
        return;
      }
      const r = await db.insMsg(uid, toId, String(msg.body).slice(0, 4000), msg.clientId || null, now());
      const m = messageJson(await db.msgById(Number(r.lastInsertRowid)));
      // 回执给发送方（带 serverId，客户端将本地临时消息替换为正式）
      conn.send(JSON.stringify({ type: 'sent', message: m, clientId: msg.clientId }));
      // 推送给接收方
      pushTo(toId, { type: 'message', message: m });
      break;
    }

    case 'read': {
      const peer = parseInt(msg.peer, 10);
      if (!peer) return;
      await db.markRead(peer, uid);
      pushTo(peer, { type: 'read', by: uid });
      break;
    }

    case 'typing': {
      const peer = parseInt(msg.peer, 10);
      if (peer) pushTo(peer, { type: 'typing', from: uid });
      break;
    }
  }
}

// ---------------------------------------------------------------- HTTP 服务
const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, 'http://x');
  const pathname = url.pathname;

  if (req.method === 'OPTIONS') {
    res.writeHead(204, CORS);
    return res.end();
  }

  // WebSocket 握手
  if (pathname === '/ws') {
    const token = url.searchParams.get('token');
    const p = verifyToken(token);
    if (!p) {
      res.writeHead(401, CORS);
      return res.end('unauthorized');
    }
    const user = await db.getUserById(p.uid);
    if (!user) {
      res.writeHead(401, CORS);
      return res.end('unauthorized');
    }
    const conn = wsAccept(req, req.socket, null);
    if (!conn) return;
    setOnline(user.id, conn);
    conn.send(JSON.stringify({ type: 'ready', user: publicUser(user), time: now() }));
    // 通知好友我上线了
    const pu = publicUser(user);
    db.friends(user.id)
      .then((list) => list.forEach((f) => pushTo(f.id, { type: 'presence', user: pu, online: true })))
      .catch(() => {});
    conn.on('message', (t) => onWsMessage(conn, user.id, t));
    conn.on('close', () => {
      setOffline(user.id, conn);
      if (!isOnline(user.id))
        db.friends(user.id)
          .then((list) => list.forEach((f) => pushTo(f.id, { type: 'presence', user: pu, online: false })))
          .catch(() => {});
    });
    return;
  }

  if (pathname.startsWith('/api/')) {
    try {
      await handleApi(req, res, pathname);
    } catch (e) {
      console.error('[api]', e);
      send(res, 500, { error: 'server_error', message: '服务端异常' });
    }
    return;
  }

  // 简易落地页，方便部署后肉眼确认服务正常
  if (pathname === '/' || pathname === '/index.html') {
    let users = 0;
    try {
      users = (await db.countUsers()).n;
    } catch (e) {
      /* ignore */
    }
    const host = req.headers['x-forwarded-host'] || req.headers.host || '你的服务器IP:' + PORT;
    const proto = req.headers['x-forwarded-proto'] || 'http';
    const base = proto + '://' + host;
    const html = `<!doctype html><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>微聊服务端</title>
<style>body{font-family:-apple-system,"PingFang SC",sans-serif;max-width:640px;margin:48px auto;padding:0 20px;line-height:1.7;color:#222}
code{background:#f3f3f3;padding:2px 6px;border-radius:4px;word-break:break-all}.ok{color:#0a0}
.box{background:#f7f7f7;border:1px solid #e5e5e5;border-radius:10px;padding:14px;margin:14px 0}
.big{font-size:17px}</style>
<h1>微聊 · 服务端运行中</h1>
<p class="ok">✔ 服务已启动，监听端口 ${PORT}</p>
<div class="box">
  <div class="big">手机端「服务器地址」填这个：</div>
  <p><code>${base}</code></p>
</div>
<p>存储后端：<code>${db.kind === 'postgres' ? 'Postgres（云端，重启不丢）' : db.kind}</code>　在线用户：<b>${online.size}</b>　注册用户：<b>${users}</b></p>
<h3>接口自查</h3>
<p><a href="/api/ping">/api/ping</a></p>
<p style="color:#888;font-size:13px">版本 ${VERSION}　Android 客户端已允许明文流量，http/https 都能连。</p>`;
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    return res.end(html);
  }

  res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
  res.end('Not Found');
});

// 让升级请求走 WebSocket
server.on('upgrade', async (req, socket, head) => {
  const url = new URL(req.url, 'http://x');
  if (url.pathname !== '/ws') {
    socket.destroy();
    return;
  }
  // 手动处理：因为需要 token 校验后再握手，这里把 head 传回
  const token = url.searchParams.get('token');
  const p = verifyToken(token);
  let user = null;
  try {
    user = p ? await db.getUserById(p.uid) : null;
  } catch (e) {
    console.error('[ws] 查询用户失败', e.message);
  }
  if (!user) {
    socket.end('HTTP/1.1 401 Unauthorized\r\n\r\n');
    return;
  }
  // 复用 wsAccept，但把用户绑定逻辑放在这里
  const key = req.headers['sec-websocket-key'];
  if (!key) {
    socket.end('HTTP/1.1 400 Bad Request\r\n\r\n');
    return;
  }
  const accept = crypto
    .createHash('sha1')
    .update(key + '258EAFA5-E914-47DA-95CA-C5AB0DC85B11')
    .digest('base64');
  socket.write(
    'HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n' +
      'Sec-WebSocket-Accept: ' + accept + '\r\n\r\n'
  );
  const conn = new WsConn(socket, head);
  setOnline(user.id, conn);
  conn.send(JSON.stringify({ type: 'ready', user: publicUser(user), time: now() }));
  const u = publicUser(user);
  db.friends(user.id)
    .then((list) => list.forEach((f) => pushTo(f.id, { type: 'presence', user: u, online: true })))
    .catch(() => {});
  conn.on('message', (t) => onWsMessage(conn, user.id, t));
  conn.on('close', () => {
    setOffline(user.id, conn);
    if (!isOnline(user.id))
      db.friends(user.id)
        .then((list) => list.forEach((f) => pushTo(f.id, { type: 'presence', user: u, online: false })))
        .catch(() => {});
  });
});

async function main() {
  // 先把存储接上，接不上就别监听端口 —— 宁可启动失败，也不要带着空库对外服务
  const opened = await storage.open(DATA_DIR);
  db = opened.db;

  server.listen(PORT, '0.0.0.0', () => {
    const nets = os.networkInterfaces();
    const ips = [];
    Object.values(nets).forEach((arr) =>
      (arr || []).forEach((n) => n.family === 'IPv4' && !n.internal && ips.push(n.address))
    );
    console.log('');
    console.log('  微聊服务端已启动（v' + VERSION + '）');
    console.log('  ---------------------------------------------');
    console.log('  本地地址 : http://127.0.0.1:' + PORT);
    ips.forEach((ip) => console.log('  局域网   : http://' + ip + ':' + PORT));
    console.log('  存储后端 : ' + db.kind);
    console.log('  ---------------------------------------------');
    if (db.kind === 'postgres') console.log('  手机端"服务器地址"填本服务的公网 https 地址即可');
    else console.log('  手机端"服务器地址"请填上面局域网地址（同 WiFi 时）');
    console.log('');
    startDiscovery();
  });
}

main().catch((e) => {
  console.error('');
  console.error('  ✖ 启动失败：' + e.message);
  if (/postgres|ECONNREFUSED|getaddrinfo|password|auth/i.test(e.message)) {
    console.error('  检查 DATABASE_URL 是否正确、数据库是否允许外部连接。');
  }
  console.error('');
  process.exit(1);
});

// 收到停止信号时把没落盘的数据刷出去（JSON 后端用得上）
['SIGTERM', 'SIGINT'].forEach((sig) => process.on(sig, () => process.exit(0)));

// ---------------------------------------------------------------- 局域网自动发现
/**
 * 手机端向 255.255.255.255:MC_DISCOVERY_PORT 广播 "MICROCHAT_DISCOVER"，
 * 服务端原样回一个 JSON（含端口和本机所有内网 IP）。
 * 这样 App 不用用户手填地址就能找到服务端。
 *
 * 与 HTTP 各占一个端口，互不影响；端口被占用时静默跳过，不影响聊天。
 */
const DISCOVERY_PORT = parseInt(argOf('discovery-port', process.env.DISCOVERY_PORT || '18089'), 10);
const MAGIC = 'MICROCHAT_DISCOVER';

function startDiscovery() {
  let dgram;
  try {
    dgram = require('dgram');
  } catch (e) {
    return;
  }
  const sock = dgram.createSocket({ type: 'udp4', reuseAddr: true });

  sock.on('message', (msg, rinfo) => {
    if (msg.toString('utf8').trim() !== MAGIC) return;
    const ips = [];
    const nets = os.networkInterfaces();
    Object.values(nets).forEach((arr) =>
      (arr || []).forEach((n) => n.family === 'IPv4' && !n.internal && ips.push(n.address))
    );
    const payload = JSON.stringify({
      app: 'microchat',
      port: PORT,
      ips,
      hostname: os.hostname(),
      version: require('./package.json').version || '1.0',
    });
    try {
      sock.send(payload, rinfo.port, rinfo.address);
    } catch (e) {
      /* 忽略 */
    }
  });

  sock.on('error', () => {
    // 端口被占用或系统不允许 → 自动发现不可用，但聊天照常
    try { sock.close(); } catch (e) {}
  });

  try {
    sock.bind(DISCOVERY_PORT, () => {
      try { sock.setBroadcast(true); } catch (e) {}
      console.log('  自动发现 : 已开启（UDP ' + DISCOVERY_PORT + '），App 可一键搜索');
      console.log('');
    });
  } catch (e) {
    /* 忽略 */
  }
}

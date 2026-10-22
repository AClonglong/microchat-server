FROM node:22-alpine
WORKDIR /app
COPY server.js storage.js package.json ./
ENV PORT=8080 DATA_DIR=/data
EXPOSE 8080
VOLUME ["/data"]
CMD ["node", "server.js"]

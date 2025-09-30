# Dockerfile
FROM node:20-alpine
WORKDIR /app
COPY package.json package-lock.json* /app/
RUN npm install --omit=dev || npm install --legacy-peer-deps --omit=dev
COPY . /app
ENV HEARTBEAT_MS=20000 RECONNECT_MS=5000
CMD ["node", "server.js"]
// server.js — Hyperliquid WS → n8n Webhook relay (com keepalive + polling fallback)

import 'dotenv/config';
import WebSocket from 'ws';
import fetch from 'node-fetch';
import http from 'http';

// ========= Env =========
const ADDRESSES = (process.env.ADDRESSES || '')
  .split(',')
  .map(s => s.trim().toLowerCase())
  .filter(Boolean);

const N8N_WEBHOOK = process.env.N8N_WEBHOOK;
const WS_URL = process.env.HYPERLIQUID_WS_URL || 'wss://api.hyperliquid.xyz/ws';
const HEARTBEAT_MS = parseInt(process.env.HEARTBEAT_MS || '20000', 10);     // ping/pong nativo
const RECONNECT_MS = parseInt(process.env.RECONNECT_MS || '5000', 10);      // reconexão
const APP_PING_MS = parseInt(process.env.WS_APP_PING_MS || '15000', 10);    // keepalive de app
const POLL_MS = parseInt(process.env.POLL_MS || '20000', 10);               // polling de segurança
const PORT = parseInt(process.env.PORT || '8080', 10);                      // /health

if (!N8N_WEBHOOK) {
  console.error('Missing N8N_WEBHOOK env var');
  process.exit(1);
}
if (ADDRESSES.length === 0) {
  console.error('Missing ADDRESSES env var (comma-separated 0x...)');
  process.exit(1);
}

// ========= State =========
let ws;
let alive = false;
let appPing;

const seen = new Map();               // (eventKey -> ts)
const SEEN_TTL_MS = 60 * 60 * 1000;   // 1h
function gcSeen() {
  const now = Date.now();
  for (const [k, ts] of seen.entries()) {
    if (now - ts > SEEN_TTL_MS) seen.delete(k);
  }
}

function eventKey(evt) {
  const a = (evt.address || evt.user || evt.wallet || '').toLowerCase();
  const t = evt.time || evt.timestamp || evt.ts || 0;
  const h = evt.txHash || evt.hash || '';
  const oid = evt.orderId || evt.cloid || '';
  const c = evt.coin || evt.symbol || evt.asset || '';
  const k = evt.kind || evt.type || evt.eventType || '';
  const px = evt.px || evt.price || '';
  const sz = evt.sz || evt.size || '';
  return [a, h, oid, t, k, c, px, sz].join('|');
}

function shouldEmit(evt) {
  const key = eventKey(evt);
  if (!key) return true;
  const now = Date.now();
  const prev = seen.get(key);
  if (prev && (now - prev) < SEEN_TTL_MS) return false;
  seen.set(key, now);
  return true;
}

// ========= n8n webhook =========
async function emitToN8N(payload) {
  try {
    const res = await fetch(N8N_WEBHOOK, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(payload),
    });
    if (!res.ok) {
      const text = await res.text();
      console.error('[webhook] non-200', res.status, text.slice(0, 500));
    }
  } catch (e) {
    console.error('[webhook] error', e.message);
  }
}

// ========= WS keepalive (app-level) =========
function startAppPing() {
  clearInterval(appPing);
  appPing = setInterval(() => {
    try {
      if (ws && ws.readyState === WebSocket.OPEN) {
        ws.send(JSON.stringify({ method: 'ping' })); // keepalive de app
      }
    } catch {}
  }, APP_PING_MS);
}

// ========= Subscriptions =========
function subscribeAll() {
  for (const addr of ADDRESSES) {
    const subs = [
      { method: 'subscribe', subscription: { type: 'userFills', user: addr } },
      { method: 'subscribe', subscription: { type: 'userEvents', user: addr } },
      { method: 'subscribe', subscription: { type: 'userNonFundingLedgerUpdates', user: addr } },
    ];
    for (const s of subs) {
      const msg = JSON.stringify(s);
      console.log('[sub]', msg);
      ws.send(msg);
    }
  }
}

// ========= WS connect loop =========
function connect() {
  ws = new WebSocket(WS_URL);

  const hb = setInterval(() => {
    if (!alive) {
      console.warn('[ws] heartbeat failed; reconnecting...');
      try { ws.terminate(); } catch {}
      clearInterval(hb);
      clearInterval(appPing);
      setTimeout(connect, RECONNECT_MS);
      return;
    }
    alive = false;
    try { ws.ping(); } catch {}
  }, HEARTBEAT_MS);

  ws.on('open', () => {
    console.log('[ws] open', WS_URL);
    alive = true;
    subscribeAll();
    startAppPing();
  });

  ws.on('pong', () => { alive = true; });

  ws.on('message', async (raw) => {
    const now = Date.now();
    try {
      const msg = JSON.parse(raw.toString());

      // Acks de assinatura (alguns servidores mandam um envelope de confirmação)
      if (msg && msg.subscription && msg.channel) {
        console.log('[ack]', JSON.stringify(msg));
        return;
      }

      const payloads = [];
      if (Array.isArray(msg)) {
        for (const item of msg) payloads.push(item);
      } else if (msg && msg.fills && Array.isArray(msg.fills)) {
        for (const f of msg.fills) payloads.push({ kind: 'fill', ...f });
      } else if (msg && msg.fill) {
        payloads.push({ kind: 'fill', ...msg.fill });
      } else if (msg && (msg.event || msg.events)) {
        const arr = msg.events || [msg.event];
        for (const e of arr) payloads.push({ kind: 'event', ...e });
      } else if (msg && (msg.update || msg.updates)) {
        const arr = msg.updates || [msg.update];
        for (const u of arr) payloads.push({ kind: 'ledger', ...u });
      } else if (msg && msg.type && (msg.type === 'pong' || msg.method === 'pong')) {
        // ignore pongs de app
      } else {
        // shape não mapeado → encaminha como raw
        payloads.push({ kind: 'raw', ...msg });
      }

      for (const p of payloads) {
        if (!p.address && (p.user || p.wallet)) p.address = p.user || p.wallet;
        if (!shouldEmit(p)) continue;
        await emitToN8N({ source: 'hyperliquid-ws', receivedAt: now, event: p });
      }

      gcSeen();
    } catch (e) {
      console.error('[ws] parse error', e.message, raw.toString().slice(0, 200));
    }
  });

  ws.on('close', (code, reason) => {
    console.warn('[ws] close', code, reason?.toString?.() || '');
    try { ws.terminate(); } catch {}
    clearInterval(hb);
    clearInterval(appPing);
    setTimeout(connect, RECONNECT_MS);
  });

  ws.on('error', (err) => {
    console.error('[ws] error', err.message);
  });
}

connect();

// ========= Polling de segurança (/info userFills) =========
// Mantém um watermark por endereço para não repetir fills
const lastTs = new Map();

async function pollUserFills() {
  const now = Date.now();
  for (const addr of ADDRESSES) {
    const since = lastTs.get(addr) || (now - 5 * 60 * 1000); // 5min atrás
    try {
      const res = await fetch('https://api.hyperliquid.xyz/info', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ type: 'userFills', user: addr, startTime: since }),
      });
      const data = await res.json().catch(() => ({}));
      const fills = Array.isArray(data) ? data : (data.fills || []);
      for (const f of fills) {
        f.kind = 'fill';
        f.address = addr;
        if (!shouldEmit(f)) continue;
        await emitToN8N({ source: 'hyperliquid-poll', receivedAt: now, event: f });
        if (f.time) lastTs.set(addr, Math.max(lastTs.get(addr) || 0, f.time));
      }
      if (fills.length === 0 && !lastTs.has(addr)) lastTs.set(addr, since);
    } catch (e) {
      console.error('[poll]', addr, e.message);
    }
  }
}
setInterval(pollUserFills, POLL_MS);

// ========= /health =========
http.createServer((req, res) => {
  if (req.url === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      ok: true,
      wsUrl: WS_URL,
      addresses: ADDRESSES.length,
      lastSeenCache: seen.size,
      pollMs: POLL_MS,
      appPingMs: APP_PING_MS,
    }));
  } else {
    res.writeHead(404); res.end();
  }
}).listen(PORT, () => {
  console.log('[health] listening on', PORT);
});

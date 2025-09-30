// server.js — Hyperliquid WS → n8n Webhook relay
// Notes:
// - Reads ADDRESSES (comma-separated 0x...), N8N_WEBHOOK (full URL), HYPERLIQUID_WS_URL.
// - Subscribes to user streams and POSTs every event to the webhook.
// - Includes reconnects, heartbeats and simple dedup by event id+time.

import 'dotenv/config';
import WebSocket from 'ws';
import fetch from 'node-fetch';

const ADDRESSES = (process.env.ADDRESSES || '').split(',').map(s => s.trim()).filter(Boolean);
const N8N_WEBHOOK = process.env.N8N_WEBHOOK;
const WS_URL = process.env.HYPERLIQUID_WS_URL || 'wss://api.hyperliquid.xyz/ws'; // adjust if needed
const HEARTBEAT_MS = parseInt(process.env.HEARTBEAT_MS || '20000', 10);
const RECONNECT_MS = parseInt(process.env.RECONNECT_MS || '5000', 10);
const LOG_EVERY = parseInt(process.env.LOG_EVERY || '60', 10); // seconds

if (!N8N_WEBHOOK) {
  console.error("Missing N8N_WEBHOOK env var");
  process.exit(1);
}
if (ADDRESSES.length === 0) {
  console.error("Missing ADDRESSES env var (comma-separated 0x...)");
  process.exit(1);
}

let ws;
let alive = false;
let lastLog = 0;

// very simple dedup cache (eventKey -> ts)
const seen = new Map();
const SEEN_TTL_MS = 60 * 60 * 1000; // 1h

function gcSeen() {
  const now = Date.now();
  for (const [k, ts] of seen.entries()) {
    if (now - ts > SEEN_TTL_MS) seen.delete(k);
  }
}

function eventKey(evt) {
  // Try to build a stable key across types
  // Prefer txHash || orderId || (address+time+kind+coin+px+sz)
  const a = evt.address || evt.user || evt.wallet || '';
  const t = evt.time || evt.timestamp || evt.ts || 0;
  const h = evt.txHash || evt.hash || '';
  const oid = evt.orderId || evt.cloid || '';
  const c = evt.coin || evt.symbol || evt.asset || '';
  const k = evt.kind || evt.type || evt.eventType || '';
  const px = evt.px || evt.price || '';
  const sz = evt.sz || evt.size || '';
  return [a,h,oid,t,k,c,px,sz].join('|');
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

function subscribeAll() {
  // Hyperliquid WS usually supports a "subscribe" message per channel.
  // We send 3 types for each address: userFills, userEvents, userNonFundingLedgerUpdates
  // If your cluster expects a different shape, adjust below.
  for (const addr of ADDRESSES) {
    const subs = [
      { method: "subscribe", subscription: { type: "userFills", user: addr } },
      { method: "subscribe", subscription: { type: "userEvents", user: addr } },
      { method: "subscribe", subscription: { type: "userNonFundingLedgerUpdates", user: addr } }
    ];
    for (const s of subs) {
      ws.send(JSON.stringify(s));
    }
  }
}

async function emitToN8N(payload) {
  try {
    const res = await fetch(N8N_WEBHOOK, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload)
    });
    if (!res.ok) {
      const text = await res.text();
      console.error("[webhook] non-200", res.status, text.slice(0, 400));
    }
  } catch (e) {
    console.error("[webhook] error", e.message);
  }
}

function connect() {
  ws = new WebSocket(WS_URL);

  const hb = setInterval(() => {
    if (!alive) {
      console.warn("[ws] heartbeat failed; reconnecting...");
      try { ws.terminate(); } catch {}
      clearInterval(hb);
      setTimeout(connect, RECONNECT_MS);
      return;
    }
    alive = false;
    try { ws.ping(); } catch {}
  }, HEARTBEAT_MS);

  ws.on('open', () => {
    console.log("[ws] open", WS_URL);
    alive = true;
    subscribeAll();
  });

  ws.on('pong', () => {
    alive = true;
  });

  ws.on('message', async (raw) => {
    const now = Date.now();
    try {
      const msg = JSON.parse(raw.toString());
      // Expected envelopes differ. Normalize a bit:
      // If this is a batch with 'fills' array, expand; otherwise forward.
      const payloads = [];

      if (Array.isArray(msg)) {
        for (const item of msg) payloads.push(item);
      } else if (msg && msg.fills && Array.isArray(msg.fills)) {
        for (const f of msg.fills) payloads.push({ kind: "fill", ...f });
      } else if (msg && msg.fill) {
        payloads.push({ kind: "fill", ...msg.fill });
      } else if (msg && (msg.event || msg.events)) {
        const arr = msg.events || [msg.event];
        for (const e of arr) payloads.push({ kind: "event", ...e });
      } else if (msg && (msg.update || msg.updates)) {
        const arr = msg.updates || [msg.update];
        for (const u of arr) payloads.push({ kind: "ledger", ...u });
      } else if (msg && msg.subscription && msg.channel) {
        // ack to subscription — ignore
      } else {
        // Unknown shape; forward raw
        payloads.push({ kind: "raw", ...msg });
      }

      for (const p of payloads) {
        // inject address if missing (some feeds echo it, others not)
        if (!p.address && (p.user || p.wallet)) p.address = p.user || p.wallet;
        if (!shouldEmit(p)) continue;
        await emitToN8N({ source: "hyperliquid-ws", receivedAt: now, event: p });
      }

      if (Math.floor(now / 1000) !== lastLog) {
        lastLog = Math.floor(now / 1000);
        gcSeen();
      }
    } catch (e) {
      console.error("[ws] parse error", e.message, raw.toString().slice(0, 200));
    }
  });

  ws.on('close', (code, reason) => {
    console.warn("[ws] close", code, reason.toString());
    try { ws.terminate(); } catch {}
    clearInterval(hb);
    setTimeout(connect, RECONNECT_MS);
  });

  ws.on('error', (err) => {
    console.error("[ws] error", err.message);
  });
}

connect();
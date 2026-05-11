// Minimal MagicMarkets data-feed listener (Node.js, ESM).
//
// Install:    npm install ws
// Run:        node listen.mjs <token>
//             MM_DATA_TOKEN=... node listen.mjs

import WebSocket from "ws";

const token = process.argv[2] ?? process.env.MM_DATA_TOKEN;
if (!token) {
  console.error("usage: node listen.mjs <token>   (or set MM_DATA_TOKEN)");
  process.exit(1);
}

const ws = new WebSocket(
  `wss://data.magicmarkets.com/v1/stream?token=${token}`,
  { maxPayload: 1 << 27 },
);

const store = { events: new Map(), sptmkt: new Map() };
let applied = 0;
let last = Date.now();

ws.on("message", (raw) => {
  const frame = JSON.parse(raw);
  for (const [op, collection, key, value] of frame.data) {
    const bucket = store[collection];
    if (!bucket) continue;
    const k = JSON.stringify(key);
    if (op === "upsert") bucket.set(k, value);
    else if (op === "delete") bucket.delete(k);
    applied++;
  }
  const now = Date.now();
  if (now - last >= 1000) {
    console.log(
      `events=${store.events.size}  sptmkt=${store.sptmkt.size}  applied=${applied}`,
    );
    last = now;
  }
});

ws.on("open", () => console.error("connected, draining snapshot…"));
ws.on("close", (code, reason) =>
  console.error(`closed code=${code} reason=${reason}`),
);
ws.on("error", (e) => console.error("error:", e.message));

// Print every record from the MagicMarkets data feed (Node.js 20+).
//
// Install:  npm install
// Run:      MM_DATA_TOKEN=<token> node listen.mjs
//
// One line per record on stdout, the same format as listen.py:
//   upsert sptmkt ["fb","2026-05-09,969,1738","for,h"] {"price":1.91}
//   delete events ["fb","2026-08-15,10050631,10037275"]
// Connection messages go to stderr, with one line when the snapshot replay
// is complete. Reconnects on its own. Stop with Ctrl-C.

import { getToken, RECONNECTED, SnapshotTracker, streamFrames } from "./mmfeed.mjs";

// Stop quietly when the reader goes away, e.g. `node listen.mjs | head`.
process.stdout.on("error", (err) => {
  if (err.code === "EPIPE") process.exit(0);
  throw err;
});

async function main() {
  const token = getToken();
  if (!token) return;
  const tracker = new SnapshotTracker();
  let connectedAt = performance.now();
  let announced = false;
  for await (const frame of streamFrames(token)) {
    if (frame === RECONNECTED) {
      console.error("reconnected: full snapshot replay follows");
      tracker.reset();
      connectedAt = performance.now();
      announced = false;
      continue;
    }
    tracker.isComplete(); // lets a quiet gap before this frame count
    tracker.observe(frame);
    if (!announced && tracker.isComplete()) {
      const waited = ((performance.now() - connectedAt) / 1000).toFixed(1);
      console.error(`snapshot complete (${tracker.reason}) ${waited} s after connect`);
      announced = true;
    }
    for (const record of Array.isArray(frame?.data) ? frame.data : []) {
      if (!Array.isArray(record) || record.length < 3) continue; // malformed record: skip it
      const [op, collection, key, ...rest] = record;
      let line = `${op} ${collection} ${JSON.stringify(key)}`;
      if (rest.length) line += ` ${JSON.stringify(rest[0])}`;
      process.stdout.write(`${line}\n`);
    }
  }
}

main().catch((err) => {
  console.error(`error: ${err.message}`);
  process.exitCode = 2;
});

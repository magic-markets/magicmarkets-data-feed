// Unit tests for mmfeed.mjs. No network. Run with: npm test
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { test } from "node:test";

import {
  backoffDelay,
  compareBetTypes,
  ConfigError,
  decodeLine,
  describeLine,
  getToken,
  handicapLine,
  parseEventId,
  redact,
  SnapshotTracker,
  splitBetType,
  Store,
  streamUrl,
} from "../mmfeed.mjs";

const session = JSON.parse(readFileSync(new URL("../../../tests/fixtures/feed_session.json", import.meta.url)));
const FAKE_TOKEN = "EXAMPLE-token/with+odd&chars=1 %20#?\u00e9";
const T = { timeout: 15000 };

function withEnv(name, value, fn) {
  const saved = process.env[name];
  try {
    if (value === undefined) delete process.env[name];
    else process.env[name] = value;
    return fn();
  } finally {
    if (saved === undefined) delete process.env[name];
    else process.env[name] = saved;
  }
}

test("getToken reads MM_DATA_TOKEN only", T, () => {
  assert.equal(
    withEnv("MM_DATA_TOKEN", " EXAMPLE-token-EXAMPLE-token-0000\n", () => getToken()),
    "EXAMPLE-token-EXAMPLE-token-0000",
  );
  const errors = [];
  const original = console.error;
  console.error = (msg) => errors.push(msg);
  try {
    assert.equal(
      withEnv("MM_DATA_TOKEN", undefined, () => getToken()),
      null,
    );
  } finally {
    console.error = original;
    process.exitCode = undefined;
  }
  assert.deepEqual(errors, ["error: set MM_DATA_TOKEN (see README)"]);
});

test("streamUrl encodes the token once and honours MM_DATA_URL", T, () => {
  withEnv("MM_DATA_URL", undefined, () => {
    assert.equal(streamUrl("abc123"), "wss://data.magicmarkets.com/v1/stream?token=abc123");
    const url = new URL(streamUrl(FAKE_TOKEN));
    assert.deepEqual(url.searchParams.getAll("token"), [FAKE_TOKEN]);
    assert.equal(url.hash, "");
  });
  withEnv("MM_DATA_URL", "ws://127.0.0.1:9999/v1/stream?foo=bar&token=old", () => {
    assert.equal(streamUrl("t"), "ws://127.0.0.1:9999/v1/stream?foo=bar&token=t");
    assert.equal(streamUrl("t", "ws://localhost:1/x"), "ws://localhost:1/x?token=t");
  });
});

test("streamUrl rejects other schemes without leaking the token", T, () => {
  for (const base of ["http://127.0.0.1:1/x", "https://data.magicmarkets.com/v1/stream", "not a url", "ws://"]) {
    assert.throws(
      () => streamUrl(FAKE_TOKEN, `${base}?token=${encodeURIComponent(FAKE_TOKEN)}`),
      (err) =>
        err instanceof ConfigError &&
        err.message.includes("ws:// or wss://") &&
        !err.message.includes(FAKE_TOKEN) &&
        !err.message.includes(encodeURIComponent(FAKE_TOKEN)),
    );
  }
});

test("redact hides token values", T, () => {
  assert.equal(redact("wss://h/v1/stream?foo=1&token=abc123&x=2"), "wss://h/v1/stream?foo=1&token=***&x=2");
  assert.equal(redact("Token EXAMPLE-token-EXAMPLE-token-0000"), "Token ***");
  assert.equal(redact("Token expired"), "Token expired");
  const out = redact(`raw=${FAKE_TOKEN} url=${streamUrl(FAKE_TOKEN, "ws://h/")}`, FAKE_TOKEN);
  assert.ok(!out.includes(FAKE_TOKEN) && !out.includes(encodeURIComponent(FAKE_TOKEN)));
});

test("splitBetType matches String.split for plain bet types", T, () => {
  for (const bt of ["for,h", "against,ah,h,-4", "for,cs,2,1", "for,over,2.5", "for,", ",for", "single"]) {
    assert.deepEqual(splitBetType(bt), bt.split(","));
  }
  assert.deepEqual(splitBetType(""), []);
});

test("splitBetType keeps proposition JSON as one token", T, () => {
  const payload = '["Player Hits, Over","1.5","Props \\"Batter\\", [late]"]';
  assert.deepEqual(splitBetType(`for,proposition,${payload}`), ["for", "proposition", payload]);
  assert.deepEqual(splitBetType('for,proposition,["a,b"],x'), ["for", "proposition", '["a,b"]', "x"]);
  assert.deepEqual(splitBetType('for,proposition,["open, list'), ["for", "proposition", '["open, list']);
  assert.deepEqual(splitBetType('for,proposition,["a"]junk,b'), ["for", "proposition", '["a"]junk,b']);
});

test("decodeLine and handicapLine (home perspective)", T, () => {
  const table = [
    [0, 0],
    [2, 0.5],
    [7, 1.75],
    [8, 2],
    [-4, -1],
    [-21, -5.25],
    [644, 161],
  ];
  for (const [wire, line] of table) assert.equal(decodeLine(wire), line);
  assert.equal(handicapLine("for,ah,a,-4"), -1);
  assert.equal(handicapLine("for,tahunder,a,2"), 0.5);
  assert.equal(handicapLine("for,tset,all,vwhole,game,ahover,62"), 15.5);
  for (const bt of ["for,over,2.5", "for,cs,2,1", "for,h", "for,ah,h", 'for,proposition,["ah","4"]', ""]) {
    assert.equal(handicapLine(bt), null, bt);
  }
});

test("describeLine follows the verified sign convention", T, () => {
  const table = {
    "for,ah,h,4": "home +1.0",
    "for,ah,a,4": "away -1.0",
    "for,ah,h,-4": "home -1.0",
    "for,ah,a,-4": "away +1.0",
    "for,ah,a,0": "away 0.0",
    "for,ah,h,-21": "home -5.25",
    "for,ahover,10": "over 2.5",
    "for,ahunder,7": "under 1.75",
    "for,tahover,h,2": "home over 0.5",
    "for,tp,all,ah,a,22": "away -5.5",
    "for,tset,all,vwhole,set,ah,p1,6": "p1 +1.5",
    "for,tset,all,vwhole,set,ah,p2,6": "p2 -1.5",
    "for,tset,all,vwhole,game,ahover,62": "over 15.5",
    "for,ir,0,2,ah,a,-2": "away +0.5",
    "for,ahover,644": "over 161.0",
  };
  for (const [bt, text] of Object.entries(table)) assert.equal(describeLine(bt), text, bt);
  for (const bt of ["for,h", "for,over,2.5", "for,cs,2,1", ""]) assert.equal(describeLine(bt), null, bt);
});

test("compareBetTypes sorts naturally", T, () => {
  const shuffled = [
    "for,score,both",
    "against,ah,h,-4",
    "for,ah,h,2",
    "for,ah,h,-8",
    "for,h",
    "for,ah,a,-4",
    "for,ah,h,-4",
    "for,a",
    "for,ahover,10",
    "for,ahover,2",
    "for,score,both,no",
  ];
  assert.deepEqual(shuffled.sort(compareBetTypes), [
    "for,a",
    "for,ah,a,-4",
    "for,ah,h,-8",
    "for,ah,h,-4",
    "against,ah,h,-4",
    "for,ah,h,2",
    "for,ahover,2",
    "for,ahover,10",
    "for,h",
    "for,score,both",
    "for,score,both,no",
  ]);
});

test("parseEventId never splits the empty outright event_id", T, () => {
  assert.deepEqual(parseEventId("2026-05-09,969,1738"), ["2026-05-09", 969, 1738]);
  assert.equal(parseEventId(""), null);
  assert.equal(parseEventId("2026-05-09,a,b"), null);
  assert.equal(parseEventId("2026-05-09,-1,2"), null);
});

test("Store applies the fixture session and skips bad records", T, () => {
  const store = new Store();
  for (const phase of ["events_phase", "sptmkt_phase", "deltas"]) {
    for (const frame of session[phase]) store.applyFrame(frame);
  }
  const want = session.expected.final;
  assert.equal(store.events.size, want.events);
  assert.equal(store.sptmkt.size, want.sptmkt);
  assert.equal(store.inRunning().length, want.in_running);
  assert.equal(store.applied, want.applied);
  assert.equal(store.skipped, want.skipped);
  assert.deepEqual(store.sptmkt.get('["fb","2026-05-11,19,42","for,h"]'), { price: 2.05 });
  assert.deepEqual(
    store.pricesFor("fb", "").map(([bt]) => bt),
    ["for,ir,0,2,ah,a,-2", "for,win,19", "for,win,42"],
  );
  assert.equal(store.applyFrame(null), 0);
  assert.equal(
    store.applyFrame({
      ts: 1,
      data: [
        ["upsert", "widgets", ["x"], {}],
        ["patch", "events", ["a"], {}],
      ],
    }),
    0,
  );
  store.reset();
  assert.equal(store.events.size + store.sptmkt.size + store.applied, 0);
  assert.deepEqual(store.pricesFor("fb", ""), []);
});

test("Store price index follows upserts, deletes and a full scan", T, () => {
  const store = new Store();
  const key = (bt) => ["fb", "x", bt];
  store.applyFrame({
    data: [
      ["upsert", "sptmkt", key("for,h"), { price: 1.5 }],
      ["upsert", "sptmkt", key("for,d"), { price: 3.4 }],
    ],
  });
  store.applyFrame({ data: [["upsert", "sptmkt", key("for,h"), { price: 1.6 }]] });
  assert.deepEqual(store.pricesFor("fb", "x"), [
    ["for,d", 3.4],
    ["for,h", 1.6],
  ]);
  store.applyFrame({
    data: [
      ["delete", "sptmkt", key("for,d")],
      ["delete", "sptmkt", key("for,h")],
    ],
  });
  assert.deepEqual(store.pricesFor("fb", "x"), []);

  const full = new Store();
  for (const phase of ["events_phase", "sptmkt_phase", "deltas", "steady"]) {
    for (const frame of session[phase]) full.applyFrame(frame);
  }
  for (const k of full.sptmkt.keys()) {
    const [sport, eventId] = JSON.parse(k);
    const scan = [...full.sptmkt]
      .map(([kk, v]) => [JSON.parse(kk), v.price])
      .filter(([kk]) => kk[0] === sport && kk[1] === eventId)
      .map(([kk, price]) => [kk[2], price])
      .sort((a, b) => (a[0] < b[0] ? -1 : 1));
    assert.deepEqual(full.pricesFor(sport, eventId), scan);
  }
});

const upsert = (collection, key, value) => ({ data: [["upsert", collection, key, value]] });
const ev = (i) => upsert("events", ["fb", `2026-05-10,${i},${i + 1}`], { ir: false });
const px = (i, price = 1.91) => upsert("sptmkt", ["fb", `2026-05-10,${i},${i + 1}`, "for,h"], { price });

/** All-new-keys replay, events first, one frame every 10 ms. Returns the last frame time. */
function replay(tracker, start) {
  let t = start;
  for (let i = 0; i < 300; i++) {
    tracker.observe(i < 100 ? ev(i) : px(i), t);
    assert.equal(tracker.isComplete(t), false, `completed early at ${t}`);
    t += 0.01;
  }
  return t - 0.01;
}

test("SnapshotTracker: an events-only phase does not complete, even with repeated keys", T, () => {
  const t = new SnapshotTracker();
  t.observe(ev(1), 1000);
  assert.equal(t.isComplete(1006.2), false); // a long gap inside the events phase
  for (let n = 0; n < 30; n++) {
    t.observe(ev(1 + (n % 2)), 1006.2 + n * 0.05); // known keys for 1.5 s
    assert.equal(t.isComplete(1006.2 + n * 0.05), false);
  }
  assert.equal(t.isComplete(1030), false);
});

test("SnapshotTracker: a steady flow of known keys completes by the new-key rule", T, () => {
  const t = new SnapshotTracker();
  const last = replay(t, 1000);
  let completedAt = null;
  for (let n = 1; n <= 40 && completedAt === null; n++) {
    const now = last + 0.1 * n; // every 0.1 s: never a quiet gap
    t.observe(px(100 + (n % 50), 2), now); // keys the replay already sent
    if (t.isComplete(now)) completedAt = now;
  }
  assert.equal(t.reason, "new keys");
  assert.ok(completedAt >= last + 0.5 && completedAt <= last + 1.2, String(completedAt - last));
});

test("SnapshotTracker: exactly half new keys is not below the ratio", T, () => {
  const t = new SnapshotTracker();
  t.observe(px(1), 1000);
  t.observe(px(1, 2), 1000.1);
  assert.equal(t.isComplete(1000.1), false);
  t.observe(px(1, 2.1), 1000.2);
  assert.equal(t.isComplete(1000.2), true);
});

test("SnapshotTracker: stops tracking once complete; reset clears seen keys", T, () => {
  const t = new SnapshotTracker();
  const last = replay(t, 1000);
  assert.equal(t.isComplete(last + 2), true);
  const size = t.seen.size;
  for (let i = 500; i < 600; i++) t.observe(px(i), last + 3);
  assert.equal(t.seen.size, size);
  t.reset();
  assert.equal(t.seen.size, 0);
  replay(t, last + 4); // the same keys again must count as new
});

test("SnapshotTracker: a silent feed completes by the quiet gap; timedOut", T, () => {
  const t = new SnapshotTracker({ maxWait: 30 });
  const last = replay(t, 1000);
  assert.equal(t.isComplete(last + 1.99), false);
  assert.equal(t.isComplete(last + 2), true);
  assert.equal(t.reason, "quiet gap");
  assert.equal(t.timedOut(last + 100), false);

  const waiting = new SnapshotTracker({ maxWait: 30 });
  assert.equal(waiting.timedOut(500), false); // the clock starts at the first call
  waiting.observe(ev(1), 501);
  assert.equal(waiting.timedOut(529.9), false);
  assert.equal(waiting.timedOut(530), true);
  waiting.reset();
  assert.equal(waiting.timedOut(530), true); // reset keeps the wait clock
});

test("backoffDelay stays within bounds and caps", T, () => {
  for (let attempt = 0; attempt < 12; attempt++) {
    const capped = Math.min(60, 2 ** attempt);
    assert.equal(
      backoffDelay(attempt, 1, 60, () => 0),
      capped / 2,
    );
    assert.equal(
      backoffDelay(attempt, 1, 60, () => 1),
      capped,
    );
  }
  assert.equal(
    backoffDelay(10_000, 1, 60, () => 1),
    60,
  );
});

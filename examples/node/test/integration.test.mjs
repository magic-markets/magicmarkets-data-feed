// Integration tests against a local ws server that replays the fixture frames.
import assert from "node:assert/strict";
import { execFile, spawn } from "node:child_process";
import { readFileSync } from "node:fs";
import { test } from "node:test";
import { fileURLToPath } from "node:url";
import { WebSocketServer } from "ws";

import { ConfigError, RECONNECTED, Store, streamFrames } from "../mmfeed.mjs";

const session = JSON.parse(readFileSync(new URL("../../../tests/fixtures/feed_session.json", import.meta.url)));
const FAKE_TOKEN = "EXAMPLE-token/with+odd&chars=1 %20#?\u00e9";
const T = { timeout: 15000 };
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

/**
 * Start a server that plays scripts[i] on accepted connection i (the last
 * script repeats). Upgrade attempts listed in `reject` get HTTP 502.
 */
async function mockFeed(scripts, { reject = new Set() } = {}) {
  let attempts = 0;
  const wss = new WebSocketServer({
    host: "127.0.0.1",
    port: 0,
    verifyClient: (info, done) => (reject.has(attempts++) ? done(false, 502, "Bad Gateway") : done(true)),
  });
  await new Promise((resolve) => wss.once("listening", resolve));
  const seen = [];
  wss.on("connection", async (ws, req) => {
    seen.push(req.url);
    for (const step of scripts[Math.min(seen.length - 1, scripts.length - 1)]) {
      if (ws.readyState !== ws.OPEN) return;
      if (step.send) ws.send(JSON.stringify({ ...step.send, ts: Date.now() / 1000 }));
      if (step.raw) ws.send(step.raw);
      if (step.sleep) await sleep(step.sleep);
      if (step.close) return ws.close();
      if (step.terminate) return ws.terminate();
      if (step.wedge) {
        ws._socket.pause(); // stop reading, so pings get no pong
        return;
      }
      // Continuous steady flow with no pauses, as on the live feed.
      for (let i = 0; step.steady && ws.readyState === ws.OPEN; i++) {
        ws.send(JSON.stringify({ ...step.steady[i % step.steady.length], ts: Date.now() / 1000 }));
        await sleep(50);
      }
    }
  });
  const { port } = wss.address();
  return {
    url: `ws://127.0.0.1:${port}/v1/stream`,
    seen,
    attempts: () => attempts,
    close: () => {
      for (const client of wss.clients) client.terminate();
      return new Promise((r) => wss.close(r));
    },
  };
}

const send = (frames) => frames.map((f) => ({ send: f }));
const fullSession = () => [
  ...send(session.events_phase),
  { sleep: 50 },
  ...send(session.sptmkt_phase),
  { sleep: 50 },
  ...send(session.deltas),
  { steady: session.steady },
];
const ghost = { data: [["upsert", "events", ["fb", "2026-05-01,555,556"], { home: "Gone FC", away: "Old Town" }]] };
const ghostThenFull = () => [[...send(session.events_phase), { send: ghost }, { close: true }], fullSession()];

/** Feed frames into a Store until stop(store, reconnects) is true. */
async function collect(options, stop, { deadline = 8000 } = {}) {
  const store = new Store();
  let reconnects = 0;
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), deadline);
  try {
    for await (const frame of streamFrames(FAKE_TOKEN, { signal: controller.signal, ...options })) {
      if (frame === RECONNECTED) {
        reconnects++;
        store.reset();
        continue;
      }
      store.applyFrame(frame);
      if (stop(store, reconnects)) break;
    }
  } finally {
    clearTimeout(timer);
  }
  assert.ok(!controller.signal.aborted, "deadline passed before the stop condition");
  return { store, reconnects };
}

function runScript(script, args, url, env = {}) {
  const path = fileURLToPath(new URL(`../${script}`, import.meta.url));
  return new Promise((resolve) => {
    execFile(
      process.execPath,
      [path, ...args],
      { env: { ...process.env, MM_DATA_URL: url, MM_DATA_TOKEN: FAKE_TOKEN, ...env }, timeout: 12000 },
      (err, stdout, stderr) => resolve({ code: err ? (err.code ?? 1) : 0, stdout, stderr }),
    );
  });
}

const assertTokenHidden = (text) => {
  assert.ok(!text.includes(FAKE_TOKEN), "raw token leaked");
  assert.ok(!text.includes(encodeURIComponent(FAKE_TOKEN)), "encoded token leaked");
};

test("streamFrames reconnects, the Store resets, and the token stays out of logs", T, async () => {
  const feed = await mockFeed(ghostThenFull());
  const logs = [];
  try {
    const { store, reconnects } = await collect(
      { url: feed.url, backoffInitial: 0.01, backoffMax: 0.05, log: (msg) => logs.push(msg) },
      (s, r) => r === 1 && s.applied === session.expected.final.applied,
    );
    assert.equal(reconnects, 1);
    assert.equal(store.events.size, session.expected.final.events);
    assert.equal(store.sptmkt.size, session.expected.final.sptmkt);
    assert.equal(store.events.has('["fb","2026-05-01,555,556"]'), false);
  } finally {
    await feed.close();
  }
  assert.equal(feed.seen.length, 2);
  for (const path of feed.seen) {
    assert.deepEqual(new URL(path, "ws://x").searchParams.getAll("token"), [FAKE_TOKEN]);
  }
  const text = logs.join("\n");
  assert.ok(text.includes("token=***"));
  assert.ok(text.includes("server closed the connection (code 100"));
  assert.ok(text.includes("reconnecting in"));
  assertTokenHidden(text);
});

test("keepalive: a wedged connection is dropped and reconnected", T, async () => {
  const feed = await mockFeed([[...send(session.events_phase), { wedge: true }], fullSession()]);
  const logs = [];
  const started = performance.now();
  try {
    const { reconnects } = await collect(
      {
        url: feed.url,
        backoffInitial: 0.01,
        backoffMax: 0.05,
        pingInterval: 0.2,
        pingTimeout: 0.2,
        log: (msg) => logs.push(msg),
      },
      (s, r) => r === 1 && s.sptmkt.size > 0,
    );
    assert.equal(reconnects, 1);
  } finally {
    await feed.close();
  }
  assert.ok(performance.now() - started < 3000);
  assert.match(logs.join("\n"), /connection closed abnormally \(code 1006\)/);
});

test("backoff restarts only after a stable connection; 502 hint after three in a row", T, async () => {
  async function run(stableAfter) {
    const feed = await mockFeed([[...send(session.events_phase), { close: true }], send(session.events_phase)], {
      reject: new Set([0, 1, 3]),
    });
    const delays = [];
    const logs = [];
    try {
      await collect(
        {
          url: feed.url,
          stableAfter,
          pingInterval: 0,
          log: (msg) => logs.push(msg),
          sleep: async (s) => delays.push(s),
        },
        (s, r) => r === 1 && s.events.size === 7,
      );
    } finally {
      await feed.close();
    }
    return { delays, logs };
  }
  const attempts = (delays) => delays.map((d) => [0, 1, 2, 3, 4].find((n) => 2 ** n / 2 <= d && d <= 2 ** n));
  assert.deepEqual(attempts((await run(30)).delays), [0, 1, 2, 3]);
  assert.deepEqual(attempts((await run(0)).delays), [0, 1, 0, 1]);

  const feed = await mockFeed([send(session.events_phase)], { reject: new Set([0, 1, 2, 3]) });
  const logs = [];
  try {
    await collect(
      { url: feed.url, pingInterval: 0, log: (m) => logs.push(m), sleep: async () => {} },
      (s) => s.events.size === 7,
    );
  } finally {
    await feed.close();
  }
  assert.equal(logs.filter((m) => m.includes("Check the token with GET /v1/config")).length, 1);
});

test("reconnect: false throws on an abnormal close and on config errors", T, async () => {
  const feed = await mockFeed([[...send(session.events_phase), { terminate: true }]]);
  try {
    await assert.rejects(
      collect({ url: feed.url, reconnect: false, log: () => {} }, () => false),
      /connection closed abnormally \(code 1006\)/,
    );
  } finally {
    await feed.close();
  }
  const bad = `http://127.0.0.1:1/v1/stream?token=${encodeURIComponent(FAKE_TOKEN)}`;
  await assert.rejects(
    collect({ url: bad, log: () => {} }, () => false),
    (err) => {
      assert.ok(err instanceof ConfigError);
      assertTokenHidden(err.message);
      return true;
    },
  );
});

test("backpressure: a slow consumer still gets every frame in order", T, async () => {
  const frames = Array.from({ length: 3000 }, (_, i) => ({
    send: { data: [["upsert", "sptmkt", ["fb", "x", `for,cs,${i},0`], { price: 2 }]] },
  }));
  const feed = await mockFeed([frames]);
  const seen = [];
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 8000);
  try {
    for await (const frame of streamFrames(FAKE_TOKEN, { url: feed.url, signal: controller.signal, log: () => {} })) {
      // Stall on the first frame so more than 1000 frames queue up and the socket is paused.
      if (seen.length === 0) await sleep(500);
      seen.push(Number(frame.data[0][2][2].split(",")[2]));
      if (seen.length === 3000) break;
    }
  } finally {
    clearTimeout(timer);
    await feed.close();
  }
  assert.ok(!controller.signal.aborted, `stalled after ${seen.length} frames`);
  assert.deepEqual(
    seen,
    Array.from({ length: 3000 }, (_, i) => i),
  );
});

test("find_event.mjs prints side-aware lines in natural order", T, async () => {
  const feed = await mockFeed([fullSession()]);
  try {
    const { code, stdout, stderr } = await runScript("find_event.mjs", ["arsenal"], feed.url);
    assert.equal(code, 0, stderr);
    assert.equal(stdout.split("Arsenal vs Chelsea").length - 1, 1);
    const rows = stdout
      .split("\n")
      .filter((l) => l.startsWith("    "))
      .map((l) => l.trim().split(/\s+/)[0]);
    assert.deepEqual(rows.slice(0, 6), [
      "for,a",
      "for,ah,a,-4",
      "for,ah,h,-4",
      "against,ah,h,-4",
      "for,ahover,10",
      "for,ahunder,10",
    ]);
    assert.match(stdout, /^ {4}for,ah,a,-4\s+2\.02 {2}line=away \+1\.0$/m);
    assert.match(stdout, /^ {4}for,ah,h,-4\s+1\.91 {2}line=home -1\.0$/m);
    assert.match(stdout, /^ {4}for,ahover,10\s+1\.95 {2}line=over 2\.5$/m);
    assert.match(stdout, /^ {4}for,over,2\.5\s+1\.87$/m);
    assert.match(stdout, /^ {2}no prices: fb_corn$/m);
    assert.ok(stdout.indexOf("  sport=fb\n") < stdout.indexOf("  sport=fb_ht\n"));
    assert.ok(!stdout.includes("win,"));
    assert.match(stderr, /snapshot complete in [\d.]+ s \(new keys\): events=6 sptmkt=24/);
    assert.match(stderr, /timing: first frame [\d.]+ s, first sptmkt upsert [\d.]+ s, gate [\d.]+ s/);
    assertTokenHidden(stdout + stderr);

    const order = await runScript("find_event.mjs", ["e"], feed.url);
    assert.deepEqual(
      order.stdout.split("\n").filter((l) => l.includes(" vs ")),
      [
        "1. FC Magdeburg vs Hertha BSC",
        "Boston Celtics vs New York Knicks",
        "Arsenal vs Chelsea",
        "New York Yankees vs Boston Red Sox",
      ],
    );
  } finally {
    await feed.close();
  }
});

test("find_event.mjs resets on reconnect, validates arguments and needs the token", T, async () => {
  const feed = await mockFeed(ghostThenFull());
  try {
    const gone = await runScript("find_event.mjs", ["Gone"], feed.url);
    assert.equal(gone.code, 1, gone.stdout);
    assert.match(gone.stderr, /no events match 'Gone'/);
    assert.equal(feed.seen.length, 2);

    const help = await runScript("find_event.mjs", ["--help"], feed.url);
    assert.equal(help.code, 0);
    assert.match(help.stdout, /MM_DATA_TOKEN/);
    for (const [args, message] of [
      [["--timeout", "0", "x"], "--timeout must be greater than 0"],
      [["--limit", "-1", "x"], "--limit must be 0 or more"],
      [[], "the following arguments are required: query"],
    ]) {
      const result = await runScript("find_event.mjs", args, feed.url);
      assert.equal(result.code, 2);
      assert.ok(result.stderr.includes(message), result.stderr);
    }
    const noToken = await runScript("find_event.mjs", ["arsenal"], feed.url, { MM_DATA_TOKEN: "" });
    assert.equal(noToken.code, 1);
    assert.match(noToken.stderr, /error: set MM_DATA_TOKEN \(see README\)/);
  } finally {
    await feed.close();
  }
});

test("listen.mjs exits 0 when the reader closes the pipe", T, async () => {
  const feed = await mockFeed([fullSession()]);
  try {
    const child = spawn(process.execPath, [fileURLToPath(new URL("../listen.mjs", import.meta.url))], {
      env: { ...process.env, MM_DATA_URL: feed.url, MM_DATA_TOKEN: FAKE_TOKEN },
      stdio: ["ignore", "pipe", "pipe"],
    });
    let stderr = "";
    child.stderr.on("data", (d) => (stderr += d));
    await new Promise((resolve) => child.stdout.once("data", resolve));
    child.stdout.destroy();
    const code = await new Promise((resolve) => child.on("exit", resolve));
    assert.equal(code, 0, stderr);
    assertTokenHidden(stderr);
  } finally {
    await feed.close();
  }
});

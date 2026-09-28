// Find fixtures by team name and print their current prices (Node.js 20+).
//
// Run:  MM_DATA_TOKEN=<token> node find_event.mjs Manchester United
//       node find_event.mjs --help
//
// Same behaviour and output as find_event.py: all arguments form the query,
// fixtures print in-running first, then upcoming, then past, with sport codes
// that have no prices on one line and side-aware lines such as
// `line=away -1.0`. Exit status: 0 found, 1 no match (or no token),
// 2 usage error, 3 no data received.

import { realpathSync } from "node:fs";
import { pathToFileURL } from "node:url";
import {
  compareBetTypes,
  describeLine,
  formatNumber,
  getToken,
  RECONNECTED,
  SnapshotTracker,
  Store,
  streamFrames,
} from "./mmfeed.mjs";

const MAX_FIXTURES = 10;
const USAGE = "usage: node find_event.mjs [-h] [--timeout SECONDS] [--limit N] query [query ...]";
const HELP = `${USAGE}

Print current prices for fixtures whose team names match the query. Reads the token from MM_DATA_TOKEN.

positional arguments:
  query              part of a team name, for example: Manchester United

options:
  -h, --help         show this help message and exit
  --timeout SECONDS  longest wait for the snapshot
  --limit N          prices per sport code, 0 for all`;

process.stdout.on("error", (err) => {
  if (err.code === "EPIPE") process.exit(0);
  throw err;
});

const show = (value) =>
  value === null || value === undefined ? "n/a" : typeof value === "number" ? formatNumber(value) : String(value);

/** Parse arguments. Returns { query, timeout, limit }, or an exit code. */
function parseArgs(args) {
  const values = { timeout: "30", limit: "25" };
  const words = [];
  const fail = (message) => {
    console.error(`${USAGE}\nfind_event.mjs: error: ${message}`);
    return 2;
  };
  for (let i = 0; i < args.length; i++) {
    const match = /^--(timeout|limit)(?:=(.*))?$/.exec(args[i]);
    if (args[i] === "-h" || args[i] === "--help") {
      console.log(HELP);
      return 0;
    } else if (match) {
      const value = match[2] ?? args[++i];
      if (value === undefined) return fail(`argument --${match[1]}: expected one argument`);
      values[match[1]] = value;
    } else if (args[i].startsWith("-") && args[i].length > 1 && !/^-\d/.test(args[i])) {
      return fail(`unrecognized arguments: ${args[i]}`);
    } else words.push(args[i]);
  }
  const timeout = Number(values.timeout);
  const limit = Number(values.limit);
  if (!Number.isFinite(timeout)) return fail(`argument --timeout: invalid float value: '${values.timeout}'`);
  if (!Number.isInteger(limit)) return fail(`argument --limit: invalid int value: '${values.limit}'`);
  if (words.length === 0) return fail("the following arguments are required: query");
  if (!(timeout > 0)) return fail("--timeout must be greater than 0");
  if (limit < 0) return fail("--limit must be 0 or more");
  return { query: words.join(" "), timeout, limit };
}

const hasSptmktUpsert = (frame) =>
  Array.isArray(frame?.data) && frame.data.some((r) => Array.isArray(r) && r[0] === "upsert" && r[1] === "sptmkt");

/** Apply frames until the tracker says the snapshot is complete or times out. */
async function loadSnapshot(token, maxWait, timing) {
  const store = new Store();
  const tracker = new SnapshotTracker({ maxWait });
  const started = performance.now();
  const elapsed = () => (performance.now() - started) / 1000;
  const controller = new AbortController();
  let failure = null;
  const consume = (async () => {
    for await (const frame of streamFrames(token, { signal: controller.signal })) {
      if (frame === RECONNECTED) {
        store.reset();
        tracker.reset();
        continue;
      }
      timing["first frame"] ??= elapsed();
      if (timing["first sptmkt upsert"] === undefined && hasSptmktUpsert(frame)) {
        timing["first sptmkt upsert"] = elapsed();
      }
      store.applyFrame(frame);
      tracker.observe(frame);
    }
  })().catch((err) => {
    failure = err;
  });
  let done = false;
  consume.finally(() => (done = true));
  while (!tracker.isComplete() && !tracker.timedOut() && !done) {
    await new Promise((resolve) => setTimeout(resolve, 50));
  }
  if (tracker.isComplete()) timing.gate = elapsed();
  controller.abort();
  await consume;
  if (failure) throw failure;
  return { store, tracker };
}

const startEpoch = (event) => {
  const t = Date.parse(event.start_ts);
  return Number.isNaN(t) ? 0 : t / 1000;
};

/** In-running first, then upcoming by start time, then past (most recent first). */
export function fixtureOrder(variants, now) {
  const start = Math.min(...variants.map(([, event]) => startEpoch(event)));
  if (variants.some(([, event]) => event.ir)) return [0, start];
  return start >= now ? [1, start] : [2, -start];
}

/** Matching fixtures as [eventId, [[sport, event], ...]], in display order. */
export function findFixtures(store, query, now) {
  const needle = query.toLowerCase();
  const fixtures = new Map();
  for (const [k, event] of store.events) {
    const [sport, eventId] = JSON.parse(k);
    const names = `${event.home ?? ""}\n${event.away ?? ""}`.toLowerCase();
    if (!names.includes(needle)) continue;
    if (!fixtures.has(eventId)) fixtures.set(eventId, []);
    fixtures.get(eventId).push([sport, event]);
  }
  const cmp = (a, b) => (a < b ? -1 : a > b ? 1 : 0);
  for (const variants of fixtures.values()) variants.sort((a, b) => cmp(a[0], b[0]));
  return [...fixtures].sort((a, b) => {
    const [ga, ta] = fixtureOrder(a[1], now);
    const [gb, tb] = fixtureOrder(b[1], now);
    return ga - gb || ta - tb || cmp(a[0], b[0]);
  });
}

function printFixture(store, eventId, variants, limit) {
  const event = variants[0][1];
  const score = Array.isArray(event.score) && event.score.length === 2 ? event.score.join("-") : "n/a";
  const lines = [
    "",
    `${show(event.home)} vs ${show(event.away)}`,
    `  event_id=${eventId}  competition=${show(event.competition_name)}  start=${show(event.start_ts)}`,
    `  in_running=${event.ir ? "yes" : "no"}  score=${score}  ir_time=${JSON.stringify(event.ir_time ?? null)}`,
  ];
  const noPrices = [];
  for (const [sport] of variants) {
    const prices = store.pricesFor(sport, eventId).sort((a, b) => compareBetTypes(a[0], b[0]));
    if (prices.length === 0) {
      noPrices.push(sport);
      continue;
    }
    lines.push(`  sport=${sport}`);
    const shown = limit === 0 ? prices : prices.slice(0, limit);
    for (const [betType, price] of shown) {
      const line = describeLine(betType);
      lines.push(`    ${betType.padEnd(44)} ${show(price).padStart(8)}${line ? `  line=${line}` : ""}`);
    }
    if (prices.length > shown.length) {
      lines.push(`    and ${prices.length - shown.length} more (use --limit 0 to show all)`);
    }
  }
  if (noPrices.length) lines.push(`  no prices: ${noPrices.join(", ")}`);
  process.stdout.write(`${lines.join("\n")}\n`);
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  if (typeof args === "number") return args;
  const token = getToken();
  if (!token) return 1;
  const timing = {};
  console.error("waiting for the snapshot...");
  const { store, tracker } = await loadSnapshot(token, args.timeout, timing);
  const summary = `events=${store.events.size} sptmkt=${store.sptmkt.size}`;
  const parts = Object.entries(timing).map(([k, v]) => `${k} ${v.toFixed(2)} s`);
  if (parts.length) console.error(`timing: ${parts.join(", ")}`);
  if (store.events.size === 0 && store.sptmkt.size === 0) {
    console.error(`error: no data received within ${args.timeout} s`);
    return 3;
  }
  if (tracker.isComplete()) {
    console.error(`snapshot complete in ${timing.gate.toFixed(1)} s (${tracker.reason}): ${summary}`);
  } else {
    console.error(
      `warning: snapshot not confirmed complete after ${args.timeout} s (${summary}); results may be partial`,
    );
  }
  const fixtures = findFixtures(store, args.query, Date.now() / 1000);
  if (fixtures.length === 0) {
    console.error(`no events match '${args.query}'`);
    return 1;
  }
  for (const [eventId, variants] of fixtures.slice(0, MAX_FIXTURES)) printFixture(store, eventId, variants, args.limit);
  if (fixtures.length > MAX_FIXTURES) process.stdout.write(`\nand ${fixtures.length - MAX_FIXTURES} more fixtures\n`);
  return 0;
}

// Set the exit code and let Node exit on its own, so piped output is never cut short.
if (process.argv[1] && import.meta.url === pathToFileURL(realpathSync(process.argv[1])).href) {
  main().then(
    (code) => (process.exitCode = code),
    (err) => {
      console.error(`error: ${err.message}`);
      process.exitCode = 2;
    },
  );
}

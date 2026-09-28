// Small helper library for the MagicMarkets data feed (Node.js 20+, ESM).
//
// Mirrors examples/python/mmfeed.py: getToken, streamUrl, redact,
// splitBetType, decodeLine, handicapLine, describeLine, compareBetTypes,
// parseEventId, Store, SnapshotTracker, backoffDelay and streamFrames.
// The only dependency is `ws`. The protocol is described in PROTOCOL.md.

import WebSocket from "ws";

export const DEFAULT_URL = "wss://data.magicmarkets.com/v1/stream";

// Frames are a few KB; this is a generous ceiling (4 MiB).
export const MAX_PAYLOAD = 2 ** 22;

// Families whose line parameter is an integer equal to 4 x the real line.
export const HANDICAP_FAMILIES = new Set(["ah", "ahover", "ahunder", "tahover", "tahunder"]);

// ---------------------------------------------------------------------------
// Token and URL handling
// ---------------------------------------------------------------------------

/**
 * Token from the MM_DATA_TOKEN environment variable. The examples never take
 * it as an argument (arguments show in `ps` and shell history). If it is
 * unset, prints a message, sets process.exitCode to 1 and returns null.
 */
export function getToken() {
  const token = (process.env.MM_DATA_TOKEN ?? "").trim();
  if (!token) {
    console.error("error: set MM_DATA_TOKEN (see README)");
    process.exitCode = 1;
    return null;
  }
  return token;
}

/**
 * Stream URL with the token URL-encoded and passed exactly once.
 * `base` defaults to MM_DATA_URL, then DEFAULT_URL. Any token already in
 * `base` is dropped, because the server uses the first token value.
 * Throws a ConfigError unless the URL starts with ws:// or wss://.
 */
export function streamUrl(token, base) {
  const raw = base || process.env.MM_DATA_URL || DEFAULT_URL;
  let url;
  try {
    url = new URL(raw);
  } catch {
    url = null;
  }
  if (!url || (url.protocol !== "ws:" && url.protocol !== "wss:") || !url.host) {
    throw new ConfigError(`stream URL must start with ws:// or wss:// (got '${redact(raw, token)}')`);
  }
  url.searchParams.delete("token");
  const rest = url.searchParams.toString();
  const tokenParam = `token=${encodeURIComponent(token)}`;
  url.search = rest ? `${rest}&${tokenParam}` : tokenParam;
  return url.toString();
}

/** A problem that retrying cannot fix: a bad URL, a TLS verification failure, a 4xx answer. */
export class ConfigError extends Error {}

/** Hide token values (`token=...` and `Token <value>`) in text that will be logged. */
export function redact(text, token) {
  let out = String(text);
  if (token) {
    for (const form of new Set([token, encodeURIComponent(token)])) {
      out = out.split(form).join("***");
    }
  }
  return out.replace(/(\btoken=)[^&\s#'"]+/gi, "$1***").replace(/(\bToken\s+)[A-Za-z0-9._~%+/=-]{16,}/g, "$1***");
}

// ---------------------------------------------------------------------------
// bet_type helpers
// ---------------------------------------------------------------------------

/** End index of the JSON value that starts at `start`, or -1 if it does not parse. */
function jsonEnd(text, start) {
  const open = text[start];
  const close = open === "[" ? "]" : "}";
  let depth = 0;
  let inString = false;
  for (let i = start; i < text.length; i++) {
    const c = text[i];
    if (inString) {
      if (c === "\\") i++;
      else if (c === '"') inString = false;
    } else if (c === '"') inString = true;
    else if (c === "[" || c === "{") depth++;
    else if (c === "]" || c === "}") {
      depth--;
      if (depth === 0) {
        if (c !== close) return -1;
        try {
          JSON.parse(text.slice(start, i + 1));
          return i + 1;
        } catch {
          return -1;
        }
      }
    }
  }
  return -1;
}

/**
 * Split a bet_type on commas, keeping an embedded JSON array (proposition
 * markets) as one token. Unparseable JSON leaves the rest as one opaque token.
 */
export function splitBetType(betType) {
  if (!betType) return [];
  const tokens = [];
  let i = 0;
  const n = betType.length;
  for (;;) {
    let end;
    if (i < n && (betType[i] === "[" || betType[i] === "{")) {
      end = jsonEnd(betType, i);
      if (end === -1 || (end < n && betType[end] !== ",")) {
        tokens.push(betType.slice(i));
        return tokens;
      }
    } else {
      end = betType.indexOf(",", i);
      if (end === -1) end = n;
    }
    tokens.push(betType.slice(i, end));
    if (end >= n) return tokens;
    i = end + 1;
  }
}

/** Handicap wire integer to the real line: 2 -> 0.5, -21 -> -5.25. */
export function decodeLine(n) {
  return Number.parseInt(n, 10) / 4;
}

/** [family, side token or null, wire integer] of the first handicap family, or null. */
function findHandicap(tokens) {
  for (let i = 1; i < tokens.length; i++) {
    if (!HANDICAP_FAMILIES.has(tokens[i])) continue;
    const params = tokens.slice(i + 1, i + 3);
    for (let j = 0; j < params.length; j++) {
      if (/^-?[0-9]+$/.test(params[j])) return [tokens[i], j === 1 ? params[0] : null, Number(params[j])];
    }
    return null;
  }
  return null;
}

/**
 * Raw decoded line (wire integer / 4) of the first handicap family, or null.
 * For `ah` this is the HOME handicap whichever side is named; use
 * describeLine for a side-aware display string.
 */
export function handicapLine(betType) {
  const found = findHandicap(splitBetType(betType));
  return found ? decodeLine(found[2]) : null;
}

/** Print numbers the way Python prints floats: 9 -> "9.0", 1.75 -> "1.75". */
export function formatNumber(x) {
  return Number.isInteger(x) ? x.toFixed(1) : String(x);
}

const SIDE_NAMES = { h: "home", a: "away" };

/**
 * The line as a person reads it, or null: "home +1.0", "away -1.0",
 * "over 2.5", "home over 0.5", "p1 +1.5". The ah integer is the home
 * handicap x 4, so the away side's line is its negation. Totals are not
 * signed by side.
 */
export function describeLine(betType) {
  const found = findHandicap(splitBetType(betType));
  if (!found) return null;
  const [family, side, wire] = found;
  let line = decodeLine(wire);
  const name = side === null ? null : (SIDE_NAMES[side] ?? side);
  if (family !== "ah") {
    const total = `${family.endsWith("over") ? "over" : "under"} ${formatNumber(line)}`;
    return name ? `${name} ${total}` : total;
  }
  if (side === "a" || side === "p2") line = -line;
  const signed = line === 0 ? "0.0" : `${line > 0 ? "+" : "-"}${formatNumber(Math.abs(line))}`;
  return `${name ?? "home"} ${signed}`;
}

function sortKey(betType) {
  const tokens = splitBetType(betType);
  const parts = tokens.slice(1).map((t) => (/^-?[0-9]+(?:\.[0-9]+)?$/.test(t) ? [0, Number(t), ""] : [1, 0, t]));
  const direction = { for: 0, against: 1 }[tokens[0]] ?? 2;
  return [...parts, [-1, direction, tokens[0] ?? ""]];
}

/** Natural order for display: family, side, numeric line, then direction (for before against). */
export function compareBetTypes(a, b) {
  const ka = sortKey(a);
  const kb = sortKey(b);
  for (let i = 0; i < Math.min(ka.length, kb.length); i++) {
    for (let j = 0; j < 3; j++) {
      if (ka[i][j] < kb[i][j]) return -1;
      if (ka[i][j] > kb[i][j]) return 1;
    }
  }
  return ka.length - kb.length;
}

/** Split "YYYY-MM-DD,<home_id>,<away_id>". Returns null for "" (outrights) and other shapes. */
export function parseEventId(eventId) {
  const parts = eventId ? eventId.split(",") : [];
  if (parts.length !== 3 || !/^[0-9]+$/.test(parts[1]) || !/^[0-9]+$/.test(parts[2])) return null;
  return [parts[0], Number(parts[1]), Number(parts[2])];
}

// ---------------------------------------------------------------------------
// Store: in-memory mirror
// ---------------------------------------------------------------------------

/**
 * In-memory mirror of the feed. Keys are JSON strings of the key array, e.g.
 * '["fb","2026-05-09,969,1738"]'. An upsert carries the complete value and
 * replaces the stored one. Unknown collections, unknown ops and malformed
 * records are counted in `skipped` and ignored. An index by
 * (sport, event_id) keeps pricesFor fast.
 */
export class Store {
  #prices = new Map(); // JSON [sport, event_id] -> Map(bet_type -> value)

  constructor() {
    this.reset();
  }

  /** Forget everything. Call on reconnect: the server replays the full snapshot. */
  reset() {
    this.events = new Map();
    this.sptmkt = new Map();
    this.#prices = new Map();
    this.lastTs = 0;
    this.applied = 0;
    this.skipped = 0;
  }

  /** Apply every record in a frame. Returns the number applied. */
  applyFrame(frame) {
    if (!frame || typeof frame !== "object" || Array.isArray(frame)) {
      this.skipped++;
      return 0;
    }
    if (typeof frame.ts === "number") this.lastTs = frame.ts;
    if (!Array.isArray(frame.data)) return 0;
    let count = 0;
    for (const record of frame.data) if (this.applyRecord(record)) count++;
    this.applied += count;
    return count;
  }

  applyRecord(record) {
    if (!Array.isArray(record) || record.length < 3 || !Array.isArray(record[2])) {
      this.skipped++;
      return false;
    }
    const [op, collection, key, value] = record;
    const bucket = collection === "events" ? this.events : collection === "sptmkt" ? this.sptmkt : null;
    if (!bucket) {
      this.skipped++; // unknown collection: ignore, do not fail
      return false;
    }
    const k = JSON.stringify(key);
    const indexed = bucket === this.sptmkt && key.length === 3;
    const eventKey = indexed ? JSON.stringify(key.slice(0, 2)) : null;
    if (op === "upsert" && value && typeof value === "object" && !Array.isArray(value)) {
      bucket.set(k, value);
      if (indexed) {
        if (!this.#prices.has(eventKey)) this.#prices.set(eventKey, new Map());
        this.#prices.get(eventKey).set(key[2], value);
      }
    } else if (op === "delete") {
      bucket.delete(k);
      const prices = indexed ? this.#prices.get(eventKey) : undefined;
      if (prices) {
        prices.delete(key[2]);
        if (prices.size === 0) this.#prices.delete(eventKey);
      }
    } else {
      this.skipped++;
      return false;
    }
    return true;
  }

  /** Keys (as arrays) of events that are in-running. */
  inRunning() {
    return [...this.events].filter(([, v]) => v.ir).map(([k]) => JSON.parse(k));
  }

  /** [betType, price] pairs for one event, sorted by bet_type. */
  pricesFor(sport, eventId) {
    const prices = this.#prices.get(JSON.stringify([sport, eventId]));
    if (!prices) return [];
    return [...prices]
      .map(([betType, v]) => [betType, v.price])
      .sort((a, b) => (a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0));
  }
}

// ---------------------------------------------------------------------------
// Snapshot completion
// ---------------------------------------------------------------------------

/**
 * Decides when the initial snapshot replay is complete, by key novelty:
 * during the replay almost every upsert introduces a new key; afterwards
 * almost none do. Complete once at least one sptmkt upsert has been seen AND
 * either (a) among the upserts of the last `window` seconds, the share that
 * introduced a new key is below `newKeyRatio`, or (b) no frame has arrived
 * for `quietGap` seconds. `timedOut()` turns true after `maxWait` seconds
 * without completion. Once complete, observe() does nothing until reset().
 * `now` is in seconds and defaults to a monotonic clock.
 */
export class SnapshotTracker {
  constructor({ quietGap = 2, window = 1, newKeyRatio = 0.5, maxWait = 30 } = {}) {
    Object.assign(this, { quietGap, window, newKeyRatio, maxWait });
    this.startedAt = null;
    this.reset();
  }

  /** Start over after a reconnect. The maxWait clock keeps running. */
  reset() {
    this.seen = new Set(); // JSON of [collection, ...key] for every upsert so far
    this.sptmktSeen = false;
    this.lastFrameAt = null;
    this.recent = []; // [time, upserts, newKeys] per frame inside the window
    this.recentUpserts = 0;
    this.recentNew = 0;
    this.reason = null; // "new keys" or "quiet gap" once complete
  }

  #now(now) {
    const t = now ?? performance.now() / 1000;
    this.startedAt ??= t;
    return t;
  }

  observe(frame, now) {
    const t = this.#now(now);
    if (this.reason !== null) return; // complete: stop tracking keys until reset()
    this.lastFrameAt = t;
    let upserts = 0;
    let fresh = 0;
    for (const r of Array.isArray(frame?.data) ? frame.data : []) {
      if (!Array.isArray(r) || r.length < 4 || r[0] !== "upsert" || !Array.isArray(r[2])) continue;
      if (r[1] !== "events" && r[1] !== "sptmkt") continue;
      const key = JSON.stringify([r[1], ...r[2]]);
      upserts++;
      if (!this.seen.has(key)) {
        this.seen.add(key);
        fresh++;
      }
      if (r[1] === "sptmkt") this.sptmktSeen = true;
    }
    if (!upserts) return;
    this.recent.push([t, upserts, fresh]);
    this.recentUpserts += upserts;
    this.recentNew += fresh;
    while (this.recent[0][0] < t - this.window) {
      const [, oldUpserts, oldNew] = this.recent.shift();
      this.recentUpserts -= oldUpserts;
      this.recentNew -= oldNew;
    }
    if (this.sptmktSeen && this.recentNew < this.newKeyRatio * this.recentUpserts) {
      this.reason = "new keys";
    }
  }

  isComplete(now) {
    const t = this.#now(now);
    if (this.reason === null && this.sptmktSeen && this.lastFrameAt !== null && t - this.lastFrameAt >= this.quietGap) {
      this.reason = "quiet gap";
    }
    return this.reason !== null;
  }

  timedOut(now) {
    const t = this.#now(now);
    return !this.isComplete(t) && t - this.startedAt >= this.maxWait;
  }
}

// ---------------------------------------------------------------------------
// Streaming with reconnect
// ---------------------------------------------------------------------------

/** Yielded by streamFrames after a reconnect: reset your Store and SnapshotTracker. */
export const RECONNECTED = Symbol("RECONNECTED");

/** Capped exponential backoff with equal jitter, in seconds. `attempt` is 0-based. */
export function backoffDelay(attempt, initial = 1, maximum = 60, rng = Math.random) {
  const capped = Math.min(maximum, initial * 2 ** Math.min(attempt, 32));
  return capped / 2 + (rng() * capped) / 2;
}

/** Sleep for `s` seconds; resolves early if `signal` aborts. */
function sleep(s, signal) {
  return new Promise((resolve) => {
    const timer = setTimeout(done, s * 1000);
    function done() {
      clearTimeout(timer);
      signal?.removeEventListener("abort", done);
      resolve();
    }
    signal?.addEventListener("abort", done, { once: true });
  });
}

const TLS_ERRORS = new Set([
  "CERT_HAS_EXPIRED",
  "DEPTH_ZERO_SELF_SIGNED_CERT",
  "SELF_SIGNED_CERT_IN_CHAIN",
  "UNABLE_TO_VERIFY_LEAF_SIGNATURE",
  "UNABLE_TO_GET_ISSUER_CERT_LOCALLY",
  "ERR_TLS_CERT_ALTNAME_INVALID",
]);

function isConfigError(err) {
  if (err instanceof ConfigError || TLS_ERRORS.has(err?.code)) return true;
  const status = err?.statusCode;
  return typeof status === "number" && status >= 400 && status < 500 && status !== 408 && status !== 429;
}

// Internal marker: connectionFrames yields it once the socket is open.
const OPENED = Symbol("OPENED");

// Close codes that mean a clean close: normal, going away, or no status sent.
const NORMAL_CLOSE = new Set([1000, 1001, 1005]);

// Backpressure: stop reading from the socket while this many frames wait unread.
const QUEUE_HIGH = 1000;
const QUEUE_LOW = 100;

/**
 * Open one connection and yield its parsed frames. Returns on a clean close
 * (1000, 1001 or 1005) or on abort; throws on an abnormal close or an error.
 */
async function* connectionFrames(target, { pingInterval, pingTimeout, openTimeout, log, signal }) {
  const ws = new WebSocket(target, { maxPayload: MAX_PAYLOAD, handshakeTimeout: openTimeout * 1000 });
  const queue = [];
  let wake = null;
  let paused = false;
  const push = (item) => {
    queue.push(item);
    if (!paused && queue.length > QUEUE_HIGH && ws.readyState === WebSocket.OPEN) {
      ws.pause();
      paused = true;
    }
    if (wake) wake();
  };
  let pingTimer = null;
  let pongTimer = null;
  const onAbort = () => push({ aborted: true });
  signal?.addEventListener("abort", onAbort, { once: true });

  ws.on("open", () => {
    push({ opened: true });
    // The server sends no pings, so the client keeps its own watch on the connection.
    if (pingInterval) {
      pingTimer = setInterval(() => {
        ws.ping();
        pongTimer ??= setTimeout(() => ws.terminate(), pingTimeout * 1000);
      }, pingInterval * 1000);
    }
  });
  ws.on("pong", () => {
    clearTimeout(pongTimer);
    pongTimer = null;
  });
  ws.on("message", (data) => push({ data }));
  ws.on("unexpected-response", (req, res) => {
    const error = new Error(`server rejected WebSocket connection: HTTP ${res.statusCode}`);
    error.statusCode = res.statusCode;
    push({ error });
    req.destroy();
  });
  ws.on("error", (error) => push({ error }));
  ws.on("close", (code, reason) => push({ closed: true, code, reason: String(reason) }));

  try {
    for (;;) {
      if (queue.length === 0) await new Promise((resolve) => (wake = resolve));
      wake = null;
      const item = queue.shift();
      if (paused && queue.length < QUEUE_LOW) {
        ws.resume();
        paused = false;
      }
      if (item.aborted) return;
      if (item.error) throw item.error;
      if (item.closed) {
        const detail = `code ${item.code}${item.reason ? `, reason ${redact(item.reason)}` : ""}`;
        if (NORMAL_CLOSE.has(item.code)) {
          log(`server closed the connection (${detail})`);
          return;
        }
        throw new Error(`connection closed abnormally (${detail})`);
      }
      if (item.opened) {
        yield OPENED;
        continue;
      }
      try {
        yield JSON.parse(item.data.toString());
      } catch (err) {
        if (!(err instanceof SyntaxError)) throw err;
        log("skipping a frame that is not valid JSON");
      }
    }
  } finally {
    signal?.removeEventListener("abort", onAbort);
    clearInterval(pingTimer);
    clearTimeout(pongTimer);
    ws.removeAllListeners();
    ws.on("error", () => {});
    ws.terminate();
  }
}

/**
 * Yield parsed frames forever, reconnecting with capped exponential backoff
 * and jitter. After a reconnect it yields RECONNECTED before the new replay:
 * reset your Store, or fill a new one and swap it in when its
 * SnapshotTracker completes. The backoff restarts only after a connection
 * stayed up for `stableAfter` seconds. Configuration errors (bad URL, TLS
 * verification, 4xx answers) throw a ConfigError at once; after three HTTP
 * 502 answers in a row it logs a hint to check the token. With
 * `reconnect: false` an error or an abnormal close throws and a normal
 * close ends the loop. Abort `signal` to stop. Times are in seconds;
 * `url` is the endpoint without the token; `sleep` is injectable for tests.
 */
export async function* streamFrames(
  token,
  {
    url,
    reconnect = true,
    backoffInitial = 1,
    backoffMax = 60,
    stableAfter = 30,
    pingInterval = 30,
    pingTimeout = 10,
    openTimeout = 10,
    signal,
    log = (msg) => console.error(msg),
    sleep: wait = sleep,
  } = {},
) {
  const target = streamUrl(token, url);
  const say = (msg) => log(redact(msg, token));
  let attempt = 0;
  let badGateways = 0;
  let connectedBefore = false;
  while (!signal?.aborted) {
    say(`connecting to ${target}`);
    let openedAt = null;
    let reason = "server closed the connection";
    try {
      for await (const frame of connectionFrames(target, {
        pingInterval,
        pingTimeout,
        openTimeout,
        log: say,
        signal,
      })) {
        if (frame === OPENED) {
          openedAt = performance.now() / 1000;
          badGateways = 0;
          say("connected");
          if (connectedBefore) yield RECONNECTED;
          connectedBefore = true;
          continue;
        }
        yield frame;
      }
    } catch (err) {
      if (isConfigError(err)) {
        throw new ConfigError(redact(`cannot connect: ${err.message ?? err}`, token));
      }
      if (!reconnect) throw new Error(redact(err.message ?? err, token));
      reason = String(err.message ?? err);
      if (err.statusCode === 502 && ++badGateways === 3) {
        say(
          "three HTTP 502 answers in a row: the token may be invalid or revoked, " +
            "or the feed may be unavailable. Check the token with GET /v1/config.",
        );
      }
    }
    if (!reconnect || signal?.aborted) return;
    if (openedAt !== null && performance.now() / 1000 - openedAt >= stableAfter) attempt = 0;
    const delay = backoffDelay(attempt, backoffInitial, backoffMax);
    attempt++;
    say(`${reason}; reconnecting in ${delay.toFixed(1)} s`);
    await wait(delay, signal);
  }
}

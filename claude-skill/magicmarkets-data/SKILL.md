---
name: magicmarkets-data
description: >
  MagicMarkets data feed assistant — a real-time, read-only WebSocket
  firehose of sports events and reference prices at data.magicmarkets.com.
  Use this skill when the user mentions the MagicMarkets data feed,
  data.magicmarkets.com, the firehose, building a local mirror of
  MagicMarkets prices, looking up live scores or current decimal-odds
  prices, the /v1/login or /v1/stream endpoints, or any task that requires
  connecting to wss://data.magicmarkets.com/v1/stream.
allowed-tools: Bash(curl:*), Bash(python3:*), Bash(node:*)
---

# MagicMarkets Data Feed

You are helping a developer use the **MagicMarkets data feed** — a
read-only, real-time stream of sports events and reference prices.

> **Source of truth.** Everything in this skill is derived from direct
> observation of the live feed. The feed exposes no schema endpoint, no
> OpenAPI document, and no introspection. When you're unsure about a
> market family, a sport code, or a line encoding, **read the live data**
> before guessing. The grammar in
> [`references/protocol.md`](references/protocol.md) is what was observed;
> it is not a closed specification.

| Detail | Value |
|---|---|
| **WebSocket** | `wss://data.magicmarkets.com/v1/stream?token=<token>` |
| **REST base** | `https://data.magicmarkets.com/v1/` |
| **Auth (REST)** | `Authorization: Token <token>` header (case-sensitive scheme) |
| **Auth (WebSocket)** | `?token=<token>` query parameter — **not** a header |
| **Wire format** | UTF-8 JSON text frames |
| **Direction** | Server → client (client frames are silently dropped) |
| **Compression** | REST: `Accept-Encoding: gzip`. WS: `permessage-deflate` by default. |

For the full wire-format reference, read
[`references/protocol.md`](references/protocol.md) before writing anything
non-trivial against the feed.

The feed is **read-only**. It carries reference prices, not order books,
trades, or stake sizes.

---

## Authentication

```bash
TOKEN=$(curl -sX POST https://data.magicmarkets.com/v1/login \
  -H 'Content-Type: application/json' \
  -d '{"username":"'"$MM_USER"'","password":"'"$MM_PASS"'"}' \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["token"])')
```

Verify with `GET /v1/config`:

```bash
curl -s https://data.magicmarkets.com/v1/config \
  -H "Authorization: Token $MM_DATA_TOKEN"
```

Revoke with `POST /v1/logout` (invalidates the token used to make the call):

```bash
curl -sX POST https://data.magicmarkets.com/v1/logout \
  -H "Authorization: Token $MM_DATA_TOKEN"
```

Auth rules to remember:

- The scheme keyword is **case-sensitive** — `Token <hex>` only. `Bearer`,
  `token` (lowercase), or no scheme all return
  `401 "Invalid 'Authorization' header, must be 'Token <token>'"`.
- Tokens cannot be sent via the REST query string — header only.
- A new login does **not** invalidate previously-issued tokens for the same
  account. Multiple tokens can be active in parallel.
- WebSocket auth must use the `?token=` query string. Sending only an
  `Authorization` header causes the handshake to fail with HTTP 502 (the
  edge strips the header).
- All WebSocket auth failures (missing, malformed, revoked, garbage)
  return the **same** `HTTP 502`. There is no separate "401 invalid token"
  signal for the WS upgrade. If you need to validate a token, call
  `GET /v1/config` over REST first.

---

## Frame format

Every frame is a JSON object with exactly two top-level fields:

```json
{
  "ts": 1778476931.285601,
  "data": [
    ["upsert", "events", ["fb", "2026-05-09,969,1738"], { ... }],
    ["upsert", "sptmkt", ["fb", "2026-05-09,969,1738", "for,h"], { "price": 1.91 }],
    ["delete", "sptmkt", ["fb", "2026-05-12,26000,37537", "for,ahunder,19"]]
  ]
}
```

- `ts` — server wall-clock at frame emission (Unix seconds, float).
- `data` — one or more **change records** in a single frame.

Each record is `[op, collection, key, value?]`:

- `op`: `"upsert"` or `"delete"`
- `collection`: `"events"` or `"sptmkt"`
- `key`: array, shape depends on the collection
- `value`: object, present only for `upsert`

Iterate `data` and apply records in order; a single frame can mix both
collections.

---

## Lifecycle

The replay is **two-phase**: the server upserts the full `events`
collection first, then the full `sptmkt` collection, then transitions to
steady-state deltas.

Observed timings on the test account:

| Phase | Records | Typical wall-clock |
|---|---|---|
| `events` replay | ~45 000 | starts at t≈1–5 s |
| `sptmkt` replay | ~145 000 | starts at t≈3–8 s |
| Steady-state | mixed | ~70 records/sec average |

There is **no explicit "snapshot complete" sentinel**.

### Detecting "snapshot complete" — IMPORTANT

The replay itself contains many gaps. An 8-minute capture showed **70
gaps of >500 ms within the snapshot**, the largest being 6 seconds *within*
the events phase. A "quiet gap ≥500 ms" heuristic will fire mid-snapshot
and leave the client with zero `sptmkt` rows.

Use one of these robust heuristics instead:

1. **Wait until you have seen at least one `sptmkt` upsert AND then a
   quiet gap ≥2 s.** Survives the events/sptmkt phase boundary and
   intra-phase gaps.
2. **Wait until `frame.ts` is within 1 s of wall-clock.** When the server
   catches up to real-time, the snapshot has drained. Most robust.
3. **Wait a fixed 20 s after connect** — simplest, blocks a few seconds
   longer than necessary.

For interactive tools, option 1 is usually right.

### Reconnect, concurrency, keepalive

- Every reconnect replays the full ~190 000-record snapshot. No resume.
- Multiple concurrent connections under the same token are permitted
  (tested with 3 in parallel). Multiple tokens per account also permitted.
- Server sends no WebSocket ping/pong control frames during idle.
  Configure your own client-side keepalive (e.g. 30 s ping, 10 s timeout).
- Server ignores **all** client-to-server frames (invalid JSON, unknown
  commands, binary garbage, 100 KB blobs — all silently discarded; stream
  continues).
- Filter query parameters (`sport=fb`, `collection=events`, anything) are
  **silently ignored**. Every connection gets the full firehose.

---

## Collections

### `events`

**Key**: `[sport, event_id]`

- `event_id` is consistently `"YYYY-MM-DD,<home_id>,<away_id>"` in every
  observed `events` record.
- IDs are stable across time — the same team / runner always has the same
  ID.

**Value** — exactly these fields, every time:

| Field | Type | Notes |
|---|---|---|
| `competition_id` | int | Stable league / tournament ID. |
| `competition_name` | string | Human-readable. |
| `competition_country` | string | Two-letter code; `"XX"` and `"EU"` also observed. |
| `home`, `away` | string | Team / player names. |
| `start_ts` | string | RFC 3339, UTC. |
| `ir` | bool | `true` while in-running. |
| `score` | `[home, away]` \| null | Integers, only while in-running. |
| `ir_time` | `[period_token, minute]` \| null | Period token + minute. Observed: `"1h"` (e.g. `["1h", 60]`) and `"2h"` (e.g. `["2h", 14]`) for football. Other tokens almost certainly exist for extra time, basket quarters, tennis sets, etc.; read whatever comes. |

`delete` records on `events` happen continuously during normal operation
(~12/sec on the test account), not only at end-of-life.

### `sptmkt`

**Key**: `[sport, event_id, bet_type]`

**Value**: `{ "price": <decimal_odds> }` — and nothing else, verified
across ~190 000 records. No size, no depth, no per-update timestamp.

**Outright / generic markets** use `event_id = ""` with the runner encoded
in the bet_type:

```json
["upsert", "sptmkt", ["fb", "",     "against,win,374"],     { "price": 1.012 }]
["upsert", "sptmkt", ["basket", "", "for,win,40897"],       { "price": 1.584 }]
["upsert", "sptmkt", ["fb", "",     "for,ir,0,2,ah,a,-2"],  { "price": 2.176 }]
```

Treat empty-event_id records as a separate "namespace" alongside
fixture-keyed markets — they update in the same way.

---

## Sport codes (as observed)

The feed broadcasts events and markets under short sport codes. The list
is open-ended; treat unknown codes as opaque rather than rejecting
records.

| Sport code | Has events | Has prices |
|---|---|---|
| `fb` (football, 90 min) | ✓ | ✓ |
| `fb_ht` (football, 1st half) | ✓ | ✓ |
| `fb_corn` (football, corners) | ✓ | ✓ |
| `fb_corn_ht` (football, corners 1st half) | ✓ | ✓ |
| `fb_book` (football, bookings) | ✓ | – |
| `basket` (basketball) | ✓ | ✓ |
| `basket_ht` (basketball, 1st half) | ✓ | – |
| `baseball` | ✓ | ✓ |
| `af` (American football) | ✓ | ✓ (very rare) |
| `ih` (ice hockey) | ✓ | ✓ |
| `tennis` | ✓ | ✓ |
| `mma` | ✓ | ✓ |
| `boxing` | ✓ | ✓ |
| `hand` (handball) | ✓ | – |
| `rl` (rugby league) | ✓ | – |
| `ru` (rugby union) | ✓ | – |
| `arf` (Australian rules football) | ✓ | – |
| `volley` (volleyball) | ✓ | – |
| `darts` | ✓ | – |
| `snooker` | ✓ | – |

Sports marked "–" had events present but no prices for either test account
during the captures — could be permissions or simply no liquidity. Coverage
differs per account: different accounts saw different sport sets and
different market families within the same sport (e.g. `mma` was observed
with `ml`+`dnb` on one account and with `ml`+`dnb`+`ahover`+`ahunder` on
another). Read what arrives; new codes and new families can appear without
notice.

---

## Bet-type format

A `bet_type` is a comma-separated string:

```
<direction>,<family>[,<param>...]
```

- **direction** — `for` or `against`. Both directions are published for
  the same selection on liquid markets (overround `1/p_for + 1/p_against`
  lands in roughly 1.025–1.045 — a normal margin).
- **family** — see the table below; observed list, not exhaustive.

| Family | Format | Meaning |
|---|---|---|
| `h` / `d` / `a` | `<dir>,h` / `<dir>,d` / `<dir>,a` | Home / Draw / Away on the headline market. |
| `ml` | `<dir>,ml,<side>` | Moneyline (no draw). `<side>` ∈ `h`, `a`. |
| `dnb` | `<dir>,dnb,<side>` | Draw-no-bet. `<side>` ∈ `h`, `a`. |
| `ah` | `<dir>,ah,<side>,<line>` | Asian handicap. `<side>` ∈ `h`, `a`. Line is a signed integer. |
| `ahover` / `ahunder` | `<dir>,ahover,<line>` / `<dir>,ahunder,<line>` | Asian total over / under. Line is a non-negative integer. |
| `over` / `under` | `<dir>,over,<line>` / `<dir>,under,<line>` | Total over / under with a **decimal** line, e.g. `for,over,6.5`. |
| `cs` | `<dir>,cs,<home>,<away>` | Correct score (two non-negative integers). |
| `score` | `<dir>,score,both` / `<dir>,score,both,no` | Both teams to score. `against,score,both` ≡ `for,score,both,no` (verified by price). |
| `win` | `<dir>,win,<runner_id>` | Outright winner. `<runner_id>` is an integer in the same ID space as `events.event_id`. |
| `tp` | `<dir>,tp,<period>,<inner>...` | Time-period-scoped market. Observed `<period>`: `all`. |
| `tset` | `<dir>,tset,<period>,<void_rule>,<unit>[,<inner>...]` | Tennis selection. |
| `ir` | `<dir>,ir,<home_score>,<away_score>,<inner>...` | Score-conditional. Observed only with empty `event_id`. |
| `proposition` | `<dir>,proposition,<json_array_text>` | Vendor proposition; third "token" is a JSON array embedded verbatim. Don't split on commas — parse with JSON awareness or treat as opaque. |

### Handicap line encoding

`ah`, `ahover`, `ahunder` lines are **integers**. Observed ranges per
sport:

| Family | Sport | Range | Step |
|---|---|---|---|
| `ah` | `fb` | −26 … +26 | mostly 1 |
| `ah` | `basket` | −152 … +142 | 2 |
| `ahover` / `ahunder` | `fb` | 2 … 35 | 1 |
| `ahover` / `ahunder` | `basket` | 482 … 1202 | 2–4 |

The integer is a packed representation of a half- or quarter-line
handicap; the exact scaling constant is not exposed by the feed. **For
storage and keying, use the integer verbatim — never round-trip through
a float.** If the user needs the human-readable line, get the scaling rule
from MagicMarkets directly rather than guessing.

---

## Worked example — find an event by team name and print its prices

```python
import asyncio, json, time, websockets

URL = f"wss://data.magicmarkets.com/v1/stream?token={TOKEN}"

async def drain_snapshot(ws, store, quiet_gap=2.0, max_seconds=30.0):
    """Wait until ≥1 sptmkt seen AND a quiet gap ≥quiet_gap, or max_seconds."""
    started = time.time()
    last_msg = started
    while time.time() - started < max_seconds:
        try:
            raw = await asyncio.wait_for(ws.recv(), timeout=quiet_gap)
        except asyncio.TimeoutError:
            if store["sptmkt"] and time.time() - last_msg >= quiet_gap:
                return
            continue
        last_msg = time.time()
        for op, coll, key, *rest in json.loads(raw)["data"]:
            key = tuple(key)
            if op == "upsert":   store[coll][key] = rest[0]
            elif op == "delete": store[coll].pop(key, None)

async def main():
    store = {"events": {}, "sptmkt": {}}
    async with websockets.connect(URL, max_size=2**27) as ws:
        await drain_snapshot(ws, store)

    query = "arsenal"
    for (sport, event_id), event in store["events"].items():
        if query in event["home"].lower() or query in event["away"].lower():
            print(event["home"], "vs", event["away"], event["start_ts"])
            for (s, e, bt), v in store["sptmkt"].items():
                if (s, e) == (sport, event_id):
                    print(" ", bt, v["price"])

asyncio.run(main())
```

A polished version of this pattern is in
[`examples/find_event.py`](examples/find_event.py). **Note**: a naive
"quiet gap ≥500 ms" heuristic will fire in the middle of the snapshot
and miss all prices — the gate above requires at least one `sptmkt` upsert
*and* a 2-second quiet window, which survives the events/sptmkt phase
boundary.

---

## Worked example — minimal listener

```python
import asyncio, json, websockets

URL = f"wss://data.magicmarkets.com/v1/stream?token={TOKEN}"

async def main():
    async with websockets.connect(URL, max_size=2**27) as ws:
        async for raw in ws:
            for op, coll, key, *rest in json.loads(raw)["data"]:
                print(op, coll, key, rest[0] if rest else "")

asyncio.run(main())
```

---

## Common mistakes to flag

- **Using `Authorization: Token` for the WebSocket** — gets stripped at
  the edge; the streaming backend then refuses the upgrade with HTTP 502.
  The WebSocket auth must be `?token=...` in the URL. REST is the opposite
  — header only.
- **Naive "quiet gap = snapshot done" detection** — the replay itself has
  gaps of up to 6 seconds. Use one of the robust heuristics above
  (sptmkt-seen + 2 s quiet, or `frame.ts ≈ wall-clock`, or 20 s wait).
- **Splitting `bet_type` on commas without checking for `proposition`** —
  proposition bet_types embed a JSON array as their parameter, which
  itself contains commas:
  `for,proposition,["Exact Total Runs 1st Half","0","Game Props - 1st Half"]`.
  Either treat the whole string as opaque, or parse with JSON awareness.
- **Parsing `event_id` to extract team IDs in `sptmkt`** — most rows do
  use `YYYY-MM-DD,homeID,awayID`, but ~0.2 % of `sptmkt` rows use `""`
  for outright / futures / generic markets. Check for the empty string
  before attempting to split.
- **Treating `sptmkt` as an order book** — there is no size, no depth,
  no per-update timestamp inside the value. It is a single reference
  price per selection, period.
- **Treating WebSocket query params as filters** — they are silently
  ignored. Filter on the client side after receiving the firehose.
- **Sending control messages over the WebSocket** — the server ignores
  all client frames. There is no subscribe / unsubscribe / ping protocol.
- **Trying to distinguish 'invalid token' from 'backend down' on the WS
  upgrade** — both return HTTP 502. Validate tokens with `GET /v1/config`
  on REST before opening the WebSocket.
- **Forgetting `max_size` on the WebSocket** — the largest snapshot
  frames can exceed 1 MB. `websockets.connect(..., max_size=2**27)`
  (Python) / `new WebSocket(..., { maxPayload: 1 << 27 })` (Node) avoids
  `PayloadTooBigError`.
- **Reconnecting in a tight loop** — every reconnect replays the full
  snapshot (~190 k records). Use exponential backoff.
- **Round-tripping handicap lines through a float** — the wire format is
  integer. Use the integer as a dictionary key; convert to a
  human-readable line only at the display boundary, using the scaling
  rule from MagicMarkets.
- **Calling `POST /v1/logout` accidentally** — it permanently revokes the
  token used to call it. If you have a long-lived shared token, don't
  expose `/v1/logout` from a generic API client.
- **Logging tokens** — they grant access to the feed. Store in env vars,
  redact in logs.

---

## Endpoint reference

### REST

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/v1/login` | Exchange `{username, password}` for a `token`. |
| `GET`  | `/v1/config` | Return the authenticated customer record. |
| `POST` | `/v1/logout` | Revoke the bearer token used in the call. |
| `OPTIONS` | `/v1/login` | CORS preflight (`Allow: POST, OPTIONS`). |

No other paths respond.

### WebSocket

| URL | Purpose |
|---|---|
| `wss://data.magicmarkets.com/v1/stream?token=<token>` | Full firehose. Filter args are silently ignored. Protocol detail in [`references/protocol.md`](references/protocol.md). |

---

## REST response shapes (observed)

Successful login:
```json
{
  "token": "<32-char hex>",
  "customer": {
    "id": 178,
    "username": "...",
    "active": true,
    "ccy_code": "GBP",
    "config": { "superuser": false }
  }
}
```

Common errors:
```json
{ "message": "No 'Authorization' header" }                                       // 401 missing header
{ "message": "Invalid 'Authorization' header, must be 'Token <token>'" }         // 401 wrong scheme
{ "message": "Authentication failed" }                                           // 401 revoked/invalid token
{ "message": "validation_error", "errors": { "username": ["Missing data for required field."] } }  // 400 missing field
{ "message": "The browser (or proxy) sent a request that this server could not understand." }      // 400 malformed JSON
{ "message": "Did not attempt to load JSON data because the request Content-Type was not 'application/json'." }  // 415
{ "message": "The requested URL was not found on the server. …" }                // 404
```

The WebSocket handshake returns the same JSON shape on `401`, an HTML
Cloudflare error page on `502` (the backend refused the upgrade — covers
missing-token, malformed-token, revoked-token, and backend-unavailable
cases indistinguishably).

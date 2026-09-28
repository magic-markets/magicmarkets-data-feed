# MagicMarkets Data Feed Protocol (v1)

The MagicMarkets data feed is a read-only, real-time stream of sports events
and reference prices over one WebSocket connection. Every connection receives
the full data set the account is entitled to: a snapshot, then live deltas.

Figures in this document are indicative. They vary with account entitlements
and time of day.

| | |
|--|--|
| **WebSocket** | `wss://data.magicmarkets.com/v1/stream?token=<token>` |
| **REST base** | `https://data.magicmarkets.com/v1/` |
| **Auth (REST)** | `Authorization: Token <token>` header |
| **Auth (WebSocket)** | `?token=<token>` query parameter |
| **Wire format** | UTF-8 JSON text frames, server to client only |
| **Compression** | REST: `Accept-Encoding: gzip`. WebSocket: `permessage-deflate`. |

## Contents

1. [Authentication](#1-authentication)
2. [Stream framing](#2-stream-framing)
3. [Lifecycle](#3-lifecycle)
   - [3.1 Phase ordering](#31-phase-ordering)
   - [3.2 Detecting snapshot complete](#32-detecting-snapshot-complete)
   - [3.3 Reconnect](#33-reconnect)
   - [3.4 Concurrency](#34-concurrency)
   - [3.5 Keepalive](#35-keepalive)
   - [3.6 Throughput](#36-throughput)
   - [3.7 Limits](#37-limits)
4. [Collections](#4-collections)
   - [4.1 events](#41-events)
   - [4.2 sptmkt](#42-sptmkt)
5. [Bet-type strings](#5-bet-type-strings)
   - [5.1 Families by sport](#51-families-by-sport)
   - [5.2 Family shapes](#52-family-shapes)
   - [5.3 Handicap line encoding](#53-handicap-line-encoding)
6. [Outright and generic markets](#6-outright-and-generic-markets)
7. [Sport codes](#7-sport-codes)
8. [Errors](#8-errors)
   - [8.1 REST](#81-rest)
   - [8.2 WebSocket](#82-websocket)
9. [REST endpoints](#9-rest-endpoints)
10. [WebSocket query parameters](#10-websocket-query-parameters)
11. [Reference implementation](#11-reference-implementation)
12. [Versioning and compatibility](#12-versioning-and-compatibility)

## 1. Authentication

Exchange a username and password for a token. The same token works for REST
and for the WebSocket.

```bash
curl -sX POST https://data.magicmarkets.com/v1/login \
  -H "Content-Type: application/json" \
  -d '{"username":"<username>","password":"<password>"}'
```

```json
{"token": "<token>", "customer": {}}
```

`token` is an opaque string. Treat it like a password. `customer` describes
the account. Clients must not depend on its fields.

Check a token with `GET /v1/config`. A `200` means the token is valid.

```bash
curl -s https://data.magicmarkets.com/v1/config \
  -H "Authorization: Token $MM_DATA_TOKEN"
```

Revoke a token with `POST /v1/logout`. The token stops working at once, for
every client that uses it. Do not call it on a shared token.

### REST auth rules

- The scheme is case-sensitive: `Token <token>`. `Bearer`, `token` or no
  scheme returns `401`.
- REST accepts the token only in the header, not in the query string.
- Each login returns a new token. Earlier tokens stay valid until they are
  revoked.
- Always send your own `User-Agent` on REST. The Python standard library
  default (`Python-urllib`) is rejected with `403`.

### WebSocket auth rules

- Pass the token once, URL-encoded, in the `?token=` query parameter. The
  WebSocket endpoint does not read the `Authorization` header.
- Every failed upgrade returns `502`: missing, malformed or revoked token,
  header-only auth, or the service is unavailable. To tell a bad token from
  an outage, call `GET /v1/config` first.

## 2. Stream framing

Every frame is a JSON object with two fields:

```json
{ "ts": 1778476931.285601, "data": [ [<op>, <collection>, <key>, <value?>], ... ] }
```

- `ts`: the time the server sent the frame, in Unix seconds (float).
- `data`: one or more change records.

| Position | Meaning |
|---:|---|
| `[0]` | `op`: `"upsert"` or `"delete"` |
| `[1]` | `collection`: `"events"` or `"sptmkt"` |
| `[2]` | `key`: array. The shape depends on the collection. |
| `[3]` | `value`: object. Present only for `"upsert"`. |

A frame can mix both collections. Apply the records in order.

- An upsert carries the complete value. Replace the stored value. Do not
  merge.
- A delete for a key you do not hold is safe to ignore.
- Ignore fields, collections and ops you do not recognise
  ([§12](#12-versioning-and-compatibility)).

## 3. Lifecycle

### 3.1 Phase ordering

On connect, the server replays the current state, then streams live deltas
without a break:

1. The full `events` collection, as upserts.
2. The full `sptmkt` collection, as upserts.
3. Live upserts and deletes for both collections.

For example, one account receives about 6 000 events and 160 000 prices,
delivered within about 2-4 s of connect.

### 3.2 Detecting snapshot complete

No message marks the end of the replay. The reliable signal is **key
novelty**. During the replay almost every upsert adds a key the client has
not seen. After it, almost every upsert updates a known key.

The recommended gate: the snapshot is complete when at least one `sptmkt`
upsert has arrived, and one of these is true:

- **(a)** In the most recent 1 s window that contains upserts, fewer than
  half of the upserts added a new key.
- **(b)** 2 s pass with no frames.

If neither is true after a maximum wait (30 s is a sensible default), stop
waiting, use the data you have and report that the snapshot may be
incomplete.

Two tests that look plausible do not work:

- **`frame.ts` close to wall-clock does not mean complete.** `ts` is the send
  time, and replay frames are sent in real time, so `ts` stays close to
  wall-clock during the whole replay.
- **A quiet gap alone can wait forever.** The live flow is continuous and
  rarely pauses. A short gap (for example 500 ms) can also fire between the
  `events` and `sptmkt` phases and leave you with no prices.

The simplest alternative is a fixed wait after connect, for example 20 s.

The example libraries implement the recommended gate:
`SnapshotTracker(quiet_gap=2.0, window=1.0, new_key_ratio=0.5,
max_wait=30.0)` in `examples/python/mmfeed.py`, and
`new SnapshotTracker({ quietGap: 2, window: 1, newKeyRatio: 0.5, maxWait: 30 })`
in `examples/node/mmfeed.mjs`.

### 3.3 Reconnect

Every reconnect replays the full snapshot. There is no resume cursor.
Reconnect with capped exponential backoff and jitter.

To keep serving reads during the replay, build a fresh store on the new
connection and swap it in when the snapshot gate passes. The examples take
the simpler route and clear their store on reconnect.

### 3.4 Concurrency

Several connections can use the same token at the same time, and each one
receives the full stream.

### 3.5 Keepalive

The server does not send WebSocket pings. It answers ping control frames
from the client with pong. Send your own pings (for example every 30 s with
a 10 s timeout) to detect a stalled connection.

### 3.6 Throughput

After the replay, expect on the order of 100 records/s, almost all
`sptmkt`, with bursts of a few hundred records/s. Price deletes arrive
continuously.

Frames observed so far are a few KB. A generous maximum message size (the
examples use 4 MiB, `2**22` bytes) is a harmless defensive setting.

### 3.7 Limits

There are no published rate limits. The feed accepts several connections
and several tokens per account. A token stays valid until `POST /v1/logout`.
Ask MagicMarkets about limits for production use.

## 4. Collections

### 4.1 `events`

One row per live, upcoming or recently completed event.

**Key**: `[sport, event_id]`, for example `["fb", "2026-05-11,969,1738"]`.
Treat `event_id` as an opaque key. Do not rely on it staying the same if a
fixture moves. The date part is informational.

**Value** (v1 fields):

| Field | Type | Notes |
|---|---|---|
| `competition_id` | integer | League or tournament ID. |
| `competition_name` | string | League or tournament name. |
| `competition_country` | string | Two-letter code. Countries use ISO 3166-1 alpha-2. Non-country codes are used for international competitions. |
| `home` | string | Home team or first player. |
| `away` | string | Away team or second player. |
| `start_ts` | string | Scheduled start, RFC 3339, UTC. |
| `ir` | bool | `true` while the event is in-running. |
| `score` | `[home, away]` or `null` | Current score while in-running. |
| `ir_time` | `[period, minute]` or `null` | Period token and minute counter, for example `["2h", 52]` in football. Treat unknown tokens as opaque. |

```json
["upsert", "events", ["fb", "2026-05-11,969,1738"], {
  "competition_id": 7, "competition_name": "Germany Bundesliga 2",
  "competition_country": "DE", "home": "1. FC Magdeburg", "away": "Hertha BSC",
  "start_ts": "2026-05-11T04:30:00Z", "ir": true, "score": [1, 0], "ir_time": ["2h", 52]
}]
["delete", "events", ["fb", "2026-08-15,10050631,10037275"]]
```

When an event is deleted, the feed may or may not send explicit deletes for
its prices. If you need a clean store, drop the `sptmkt` rows whose event
was deleted.

### 4.2 `sptmkt`

One row per priced selection.

**Key**: `[sport, event_id, bet_type]`. A small share of rows use
`event_id = ""` ([§6](#6-outright-and-generic-markets)). `bet_type` is
described in [§5](#5-bet-type-strings).

**Value**: `{"price": <decimal_odds>}`, for example `2.10`. One reference
price per selection: no depth, no size and no per-update timestamp. Use the
frame `ts` for ordering. A `delete` means the market was withdrawn.

```json
["upsert", "sptmkt", ["fb", "2026-05-11,969,1738", "for,h"], { "price": 2.10 }]
["delete", "sptmkt", ["fb", "2026-05-12,26000,37537", "for,ahunder,19"]]
```

## 5. Bet-type strings

The feed uses the same sport codes and `bet_type` grammar as the
MagicMarkets API v2. The canonical grammar is in the "Sports & bet types"
section of [magicmarkets.com/llms-full.txt](https://magicmarkets.com/llms-full.txt).

```
<direction>,<family>[,<param>...]
```

- **direction**: `for` prices the outcome happening, `against` prices it not
  happening. Liquid markets carry both directions.
- **family** and parameters: see below.

Do not split a `bet_type` on every comma. A `proposition` embeds a JSON array
that contains commas.

### 5.1 Families by sport

Examples only, not a closed list. Coverage varies by account and over time.
Do not fail on an unknown family.

| Sport | Example families (inner markets in brackets) |
|---|---|
| `fb` | `h`, `d`, `a`, `ah`, `ahover`, `ahunder`, `tahover`, `tahunder`, `over`, `under`, `cs`, `score`, `odd`, `even`, `gr`, `wm`, `wintonil`, `proposition`, `win`, `ir` (`ah`, `ahover`, `ahunder`) |
| `fb_ht` | as `fb`, without `win` and `ir` |
| `fb_corn` | `h`, `d`, `a`, `ah`, `ahover`, `ahunder`, `tahover`, `tahunder`, `over`, `under`, `odd`, `even`, `ir` |
| `fb_corn_ht` | `h`, `d`, `a`, `ah`, `ahover`, `ahunder` |
| `fb_book` | `proposition` |
| `basket` | `ml`, `ah`, `ahover`, `ahunder`, `proposition` |
| `basket_ht` | `ml`, `ah`, `ahover`, `ahunder` |
| `tennis` | `tset` (`ah`, `ahover`, `ahunder`, `cs`) |
| `ih` | `tp` (`ah`, `wdw`, `ahover`, `ahunder`, `ml`), `proposition` |
| `af`, `baseball` | `tp` (`ah`, `ahover`, `ahunder`, `ml`), `proposition` |
| `volley` | `tp` (`ah`, `ahover`, `ahunder`, `ml`) |
| `mma` | `ml`, `dnb`, `ahover`, `ahunder` |
| `boxing` | `h`, `d`, `a`, `dnb` |

### 5.2 Family shapes

The API docs define more families than this table lists. `<side>` is `h`
(home) or `a` (away). In tennis it is `p1` or `p2`: player 1 (home) or
player 2 (away).

| Family | Format | Meaning |
|---|---|---|
| `h` / `d` / `a` | `<dir>,h` | Home, draw or away. |
| `ml` / `dnb` | `<dir>,ml,<side>` | Moneyline or draw no bet. |
| `ah` | `<dir>,ah,<side>,<line>` | Asian handicap ([§5.3](#53-handicap-line-encoding)). |
| `ahover` / `ahunder` | `<dir>,ahover,<line>` | Asian total. `<line>` is 4 x the total. |
| `tahover` / `tahunder` | `<dir>,tahover,<side>,<line>` | Team Asian total. `<line>` is 4 x the total. |
| `over` / `under` | `<dir>,over,<line>` | Total with a decimal line, for example `for,over,6.5`. Not scaled. |
| `odd` / `even` | `<dir>,odd[,<side>]` | Total odd or even, for the match or one team. |
| `gr` | `<dir>,gr,<min>,<max>` | Total in an inclusive range. `inf` means no upper limit. |
| `wm` | `<dir>,wm,<side>,<min>,<max>` | Winning margin. |
| `cs` | `<dir>,cs,<home>,<away>` | Correct score. |
| `score` | `<dir>,score,both[,no]` | Both teams to score. The API docs also define `both,yes`. |
| `win` | `<dir>,win,<runner_id>` | Outright winner ([§6](#6-outright-and-generic-markets)). |
| `tp` | `<dir>,tp,<period>,<market>[,<args>...]` | Period-scoped market with an inner market, for example `for,tp,all,ah,a,-6`. |
| `tset` | `<dir>,tset,<period>,<void_rule>,<unit>[,<market>,<args>...]` | Tennis, for example `for,tset,all,vwhole,set,ah,p1,6`. |
| `ir` | `<dir>,ir,<home_score>,<away_score>,<market>[,<args>...]` | Price of the inner market given the current score. |
| `proposition` | `<dir>,proposition,<json_array>` | Named proposition. Split only the first two commas. Treat the JSON array as opaque and do not rely on the order of its elements. |

Other families on the feed include `wdw` (period winner, as an inner
market) and `wintonil`. For these, and for what each sport code means, see
the API docs.

### 5.3 Handicap line encoding

Asian handicap lines are integers equal to **4 x the real line**. In `ah`,
the integer is always the **home** handicap. The side picks which team the
selection backs:

| `bet_type` | Selection |
|---|---|
| `for,ah,h,4` | home +1.0 |
| `for,ah,a,4` | away -1.0 |
| `for,ah,h,-4` | home -1.0 |
| `for,ah,a,-4` | away +1.0 |

So `for,ah,h,N` and `for,ah,a,N` are the two sides of one market. For the
away side, the backed team's line is -(N/4).

Totals (`ahover`, `ahunder`, `tahover`, `tahunder`) use the same x4 scaling
and are not signed by side: `for,ahover,7` is over 1.75.

The rule applies inside any wrapper: `tp`, `tset`, `ir` and the other period
tokens in the API grammar. For example `for,tp,all,ah,a,-6` is away +1.5. It
does not apply to `over`, `under`, `cs` or `win`.

Keep the wire integer for keys and storage. Decode only for display.

## 6. Outright and generic markets

Some `sptmkt` rows have `event_id = ""`. They are not tied to a fixture in
`events`. They include outright winners and score-conditional prices, and
can also carry ordinary families:

```json
["upsert", "sptmkt", ["fb", "", "against,win,374"], { "price": 1.012 }]
["upsert", "sptmkt", ["fb", "", "for,ir,0,2,ah,a,-2"], { "price": 2.176 }]
```

The feed carries no runner names, so these rows need another source to
label them. Check for the empty string before you use an `event_id`.

## 7. Sport codes

Sport codes are shared with the MagicMarkets API v2. For what each code
means, see "Sport codes" in
[magicmarkets.com/llms-full.txt](https://magicmarkets.com/llms-full.txt).
Treat an unknown code as opaque.

Codes seen on the feed include `fb`, `fb_ht`, `fb_corn`, `fb_corn_ht`,
`fb_book`, `basket`, `basket_ht`, `tennis`, `ih`, `af`, `baseball`, `volley`,
`mma`, `boxing`, `hand`, `rl`, `ru`, `darts` and `snooker`. Coverage varies by
account and over time. A sport can appear in `events` with no prices.

## 8. Errors

### 8.1 REST

| Status | Meaning |
|---|---|
| 200 | Success. `/v1/logout` returns an empty body. |
| 400 | `/v1/login` body is not valid JSON, or fields are missing or invalid. Send both `username` and `password`. |
| 401 | Missing or invalid `Authorization` header, invalid or revoked token, or wrong credentials on `/v1/login`. |
| 403 | Blocked `User-Agent` ([§1](#1-authentication)). |
| 404 | Unknown path. |
| 415 | `/v1/login` without `Content-Type: application/json`. |

### 8.2 WebSocket

| Status at upgrade | Meaning |
|---|---|
| 101 | Connected. |
| 502 | Missing, malformed or revoked token, or the service is unavailable. |

Treat any status other than `101` as a failed upgrade. After the upgrade the
server answers ping control frames with pong and ignores data frames from
the client.

## 9. REST endpoints

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/v1/login` | Exchange `{username, password}` for a token. |
| `GET` | `/v1/config` | Check a token. |
| `POST` | `/v1/logout` | Revoke the token used in the call. |

## 10. WebSocket query parameters

`token` is the only parameter. Pass it once. The server ignores any other
parameter, for example `sport=fb`. There is no subscribe or filter protocol,
so filter on the client.

## 11. Reference implementation

The smallest correct client keeps one dictionary per collection:

```python
import asyncio, json, os, websockets
from urllib.parse import quote

URL = "wss://data.magicmarkets.com/v1/stream?token=" + quote(os.environ["MM_DATA_TOKEN"])

async def main():
    store = {"events": {}, "sptmkt": {}}
    async with websockets.connect(URL, max_size=2**22, ping_interval=30, ping_timeout=10) as ws:
        async for raw in ws:
            for record in json.loads(raw).get("data", []):
                op, coll, key = record[0], record[1], tuple(record[2])
                if coll not in store:
                    continue  # ignore unknown collections
                if op == "upsert":
                    store[coll][key] = record[3]  # full value: replace
                elif op == "delete":
                    store[coll].pop(key, None)

asyncio.run(main())
```

`examples/python/mmfeed.py` and `examples/node/mmfeed.mjs` add a `Store`, the
`SnapshotTracker` gate from [§3.2](#32-detecting-snapshot-complete), a
reconnecting stream, a proposition-aware `split_bet_type`, and `decode_line`
and `describe_line` for [§5.3](#53-handicap-line-encoding).

A production client should also:

- Wait for the snapshot gate before it serves reads.
- Back off on reconnect, and rebuild state from the new replay
  ([§3.3](#33-reconnect)).
- Send its own pings ([§3.5](#35-keepalive)).
- Redact the stream URL in logs, because it contains the token.

## 12. Versioning and compatibility

The `/v1/` path identifies this version of the feed. The feed can gain new
sport codes, bet-type families, fields, collections and ops.

Clients must ignore anything they do not recognise: unknown sport codes,
bet-type families, fields, collections and ops. Do not depend on the order
of fields in an object, or on the volumes and timings in this document.

Notable changes to this document and the examples are recorded in
`CHANGELOG.md` in the repository.

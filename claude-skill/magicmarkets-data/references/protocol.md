# MagicMarkets Data Feed — Protocol Reference

The MagicMarkets data feed publishes a real-time stream of sports events and
reference prices over a single WebSocket connection. It is read-only and
firehose-style: every client receives the full data set the account has
access to.

> **Scope of this document.** Everything below is derived from direct
> observation of the live feed using a test account (≈200 000 records and
> ≈8 minutes of continuous capture, plus targeted edge-case probes). Where
> a wire convention cannot be decoded without external information (e.g.
> the integer scaling of handicap lines), this document describes the wire
> shape only and leaves the semantic decoding to the operator.

| | |
|--|--|
| **Host** | `data.magicmarkets.com` |
| **REST base** | `https://data.magicmarkets.com/v1/` |
| **WebSocket** | `wss://data.magicmarkets.com/v1/stream?token=<token>` |
| **Auth (REST)** | `Authorization: Token <token>` header (case-sensitive scheme) |
| **Auth (WebSocket)** | `?token=<token>` query parameter |
| **Wire format** | UTF-8 JSON text frames |
| **Direction** | Server → client only; client → server frames are silently dropped |
| **Compression** | REST `Accept-Encoding: gzip` supported; WebSocket `permessage-deflate` negotiated by default |

## 1. Authentication

Exchange a username and password for a token. The same token works for both
REST and the WebSocket.

```bash
curl -sX POST https://data.magicmarkets.com/v1/login \
  -H "Content-Type: application/json" \
  -d '{"username":"<username>","password":"<password>"}'
```

```json
{
  "token": "a1b2c3d4e5f6a7b8c9d0e1f2a3b4c5d6",
  "customer": {
    "id": 178,
    "username": "devtest",
    "active": true,
    "ccy_code": "GBP",
    "config": { "superuser": false }
  }
}
```

Verify a token with `GET /v1/config` — returns the same `customer` object:

```bash
curl -s https://data.magicmarkets.com/v1/config \
  -H "Authorization: Token $TOKEN"
```

Revoke a token with `POST /v1/logout`. The token used to make the call is
invalidated immediately; subsequent uses return
`401 {"message":"Authentication failed"}`.

```bash
curl -sX POST https://data.magicmarkets.com/v1/logout \
  -H "Authorization: Token $TOKEN"
```

### Auth scheme rules

- The scheme keyword is **case-sensitive** — `Token <hex>` only. `Bearer`,
  `token` (lowercase), or omitting the scheme all return
  `401 {"message":"Invalid 'Authorization' header, must be 'Token <token>'"}`.
- The HTTP header name itself is case-insensitive (HTTP standard); both
  `Authorization:` and `authorization:` work.
- Tokens cannot be sent via the REST query string (`?token=<token>` on a REST
  request is ignored — the server still demands the header).
- A new login does **not** invalidate previously-issued tokens for the same
  account — tokens are independent and can be used in parallel. Each `POST
  /v1/login` returns a fresh token; the prior ones remain valid until
  `/v1/logout`.

### WebSocket auth rules

- Auth **must** be via the `?token=<token>` query parameter. Edge
  infrastructure strips the `Authorization` header before the streaming
  backend sees it; without `?token=` the upgrade is refused with `HTTP 502`.
- All auth-failure cases — missing token, malformed token, revoked
  token — return the same `HTTP 502` from the upgrade. There is no
  status-code-level signal that distinguishes "invalid token" from
  "backend unavailable." If you need to verify a token, hit `GET /v1/config`
  before opening the WebSocket.
- Duplicate `?token=` parameters: the **first** value wins. `?token=GOOD&token=BAD`
  connects; `?token=BAD&token=GOOD` fails. Don't rely on this — pass the
  token exactly once.

## 2. Stream framing

Every frame on the WebSocket is a JSON object with exactly two top-level
fields:

```json
{
  "ts": 1778476931.285601,
  "data": [
    [<op>, <collection>, <key>, <value?>],
    ...
  ]
}
```

- `ts` — server-side wall-clock at frame emission (Unix seconds, float).
- `data` — array of one or more **change records**. Each record is a
  fixed-position array.

Change records always start with two strings, the operation and the
collection name:

| Position | Meaning |
|---:|---|
| `[0]` | `op` — `"upsert"` or `"delete"` |
| `[1]` | `collection` — `"events"` or `"sptmkt"` |
| `[2]` | `key` — array, shape depends on the collection |
| `[3]` | `value` — object, present only for `"upsert"` |

A single frame may pack many records from both collections; iterate over
`data` and apply each record in order. Verified across ~250 000 records:
**no frames with extra top-level fields, no records of unexpected shape.**

## 3. Lifecycle

On connect, the server immediately begins replaying the current state as a
sequence of `upsert` records, then transitions seamlessly into streaming
deltas. The replay is not delimited by any "snapshot complete" sentinel.

### 3.1 Phase ordering

The replay is **two-phase**, in this fixed order:

1. **`events` first** — the full `events` collection is upserted (~45 000
   records).
2. **`sptmkt` second** — the full `sptmkt` collection is upserted (~145 000
   records).
3. **Steady-state deltas** — both collections continue with upserts/deletes
   intermixed.

Observed timings (test account):

| Marker | Typical t (s from connect) |
|---|---|
| First `events` upsert | 1.4 – 5.1 |
| First `sptmkt` upsert | 3.3 – 8.2 |
| Last `events` upsert in replay burst | typically before first `sptmkt` |
| Snapshot "settled" (frame.ts ≈ wall-clock) | 10 – 20 |

### 3.2 Detecting "snapshot complete"

There is no explicit sentinel. **Do not use "quiet gap ≥500 ms" as the
heuristic** — the replay itself contains many internal gaps (70 gaps of
>500 ms across an 8-minute capture, the largest being 6 seconds *within*
the events phase). A naive quiet-gap detector will fire in the middle of
the snapshot and leave you with zero `sptmkt` rows.

Robust heuristics, in increasing order of safety:

1. **Wait until you have seen at least one `sptmkt` upsert AND then a quiet
   gap ≥2 s.** Survives the events/sptmkt phase boundary and the small
   intra-phase gaps.
2. **Wait until `frame.ts` is within 1 s of wall-clock.** When the server
   catches up to real-time, the snapshot has drained. Most robust.
3. **Wait a fixed 20 s after connect** before serving reads — simplest, but
   blocks a few seconds longer than necessary.

For interactive tools, option 1 is usually the right choice.

### 3.3 Reconnect

Every reconnect replays the full snapshot. There is no resume cursor and no
catch-up window. Plan reconnect intervals with this in mind — back-to-back
reconnects of 10 connections completed successfully in our tests with no
rate-limit observed, but each replay carries ~190 000 records.

### 3.4 Concurrency

Multiple simultaneous WebSocket connections under the same token are
permitted. We verified three concurrent connections all receiving the full
stream. Multiple tokens for the same account are also permitted.

### 3.5 Keepalive

The server **does not send WebSocket ping/pong control frames**. A
60-second idle test on the client side (with `ping_interval=None`) showed
the stream continuing to flow with no protocol-level keepalive activity. In
practice the high data rate means the TCP connection is constantly active.

Production clients should still configure their own ping/pong on the client
side (e.g. 30 s interval, 10 s timeout) to detect a wedged connection.

### 3.6 Throughput

Observed rates on the test account:

| Phase | Approximate rate |
|---|---|
| Initial replay (events) | ~15 000–25 000 records/sec for 2–5 s |
| Initial replay (sptmkt) | ~10 000–30 000 records/sec for 4–10 s |
| Steady-state, all sports | ~70 records/sec averaged (12 events + 58 sptmkt) |
| Steady-state, peak bursts | several hundred/sec during heavy market activity |

`delete` records appear continuously at steady state, not only at
end-of-life — observed ~12 `events` deletes/sec and ~2 `sptmkt`
deletes/sec on the test account.

## 4. Collections

### 4.1 `events`

One row per known event (live, upcoming, or recently completed).

**Key** — `[sport, event_id]`

In every observed `events` record (45 326 records sampled), `event_id` had
the form `YYYY-MM-DD,<home_id>,<away_id>` — date plus two integer entity
IDs. The IDs are stable across time (a given team has the same `home_id`
every match). Example: `["fb", "2026-05-09,969,1738"]`.

**Value** — object with exactly these nine fields, every time:

| Field | Type | Notes |
|---|---|---|
| `competition_id` | integer | Stable identifier for the league / tournament. |
| `competition_name` | string | Human-readable league name. |
| `competition_country` | string | Two-letter code (ISO-3166 alpha-2 where applicable). Observed values include `"XX"` and `"EU"` for international / regional competitions. |
| `home` | string | Home team / player 1 name. |
| `away` | string | Away team / player 2 name. |
| `start_ts` | string | Scheduled start time, RFC 3339, UTC (`...Z`). |
| `ir` | bool | `true` while the event is in-running. |
| `score` | `[home, away]` \| null | Current score (integers) while in-running. |
| `ir_time` | `[period_token, minute]` \| null | Period token + minute (e.g. `["1h", 60]`). Only the `"1h"` token was observed during the capture window; other tokens almost certainly exist for other periods and sports. |

**Upsert**:

```json
["upsert", "events", ["fb", "2026-05-09,969,1738"], {
  "competition_id": 7,
  "competition_name": "Germany Bundesliga 2",
  "competition_country": "DE",
  "home": "1. FC Magdeburg",
  "away": "Hertha BSC",
  "start_ts": "2026-05-09T11:00:00Z",
  "ir": true,
  "score": [0, 0],
  "ir_time": ["1h", 60]
}]
```

**Delete** — occurs continuously during normal operation (≈12/sec
observed on the test account), not only at event end-of-life:

```json
["delete", "events", ["fb", "2026-08-15,10050631,10037275"]]
```

### 4.2 `sptmkt`

One row per priced market on an event.

**Key** — `[sport, event_id, bet_type]`

- `sport`, `event_id` — same shape as `events` for the overwhelming majority
  of records (~99.8 % observed).
- A small fraction of records (~0.2 % observed) use `event_id = ""` for
  outright / futures / generic markets where the runner is encoded in
  `bet_type` itself — e.g. `["fb", "", "against,win,374"]`. See §6.
- `bet_type` — comma-separated string identifying the selection. See §5.

**Value** — `{"price": <decimal_odds>}` — and nothing else. Verified across
all observed `upsert` records (no extra fields seen in any of ~190 000
records).

Prices are decimal-odds floats (e.g. `1.91`, `30.0`). This feed publishes one
reference price per selection; it does **not** carry order-book depth, stake
size, or per-update timestamps inside the value object. Use the timestamp on
the enclosing frame (`ts`) for ordering.

**Upsert**:

```json
["upsert", "sptmkt", ["fb", "2026-05-09,969,1738", "for,h"], { "price": 1.91 }]
```

**Delete** — the market has been withdrawn (line retired, event closed,
etc.):

```json
["delete", "sptmkt", ["fb", "2026-05-12,26000,37537", "for,ahunder,19"]]
```

## 5. Bet-type strings — observed format

A `bet_type` is a comma-separated string. The first token is the direction;
the second token is the market family; subsequent tokens are family-specific
parameters.

```
<direction>,<family>[,<param>...]
```

- **direction** — observed values: `for`, `against`. Both directions are
  broadcast for the same selection on liquid markets (a 100-event football
  sample showed 12 387 home-side AH lines with both `for,ah,h,L` and
  `against,ah,h,L` present, with `1/p_for + 1/p_against` in the 1.025–1.045
  range — consistent with a single market quoted both ways).
- **family** — see §5.1 below.

### 5.1 Families observed per sport

The table below lists the families that appeared on the live feed during our
capture, grouped by sport. The list is **not** a closed enum: new families
may be added by MagicMarkets without notice. Read whatever you get; don't
hard-fail on unknown families.

| Sport (events) | Sport (sptmkt) | Families observed | Sample `bet_type` |
|---|---|---|---|
| `fb` | yes | `h`, `d`, `a`, `ah`, `ahover`, `ahunder`, `over`, `under`, `cs`, `score`, `ir`, `proposition`, `win` | `for,ah,a,-1` |
| `fb_ht` | yes | `h`, `d`, `a`, `ah`, `ahover`, `ahunder`, `over`, `under`, `cs`, `score`, `ir`, `proposition` | `for,cs,0,0` |
| `fb_corn` | yes | `h`, `d`, `a`, `ah`, `ahover`, `ahunder`, `over`, `under`, `ir` | `for,ahover,32` |
| `fb_corn_ht` | yes | `h`, `d`, `a`, `ah`, `ahover`, `ahunder`, `ir` | `for,ah,a,-2` |
| `tennis` | yes | `tset` | `for,tset,all,vwhole,set,ah,p1,6` |
| `basket` | yes | `ml`, `ah`, `ahover`, `ahunder`, `proposition`, `win` | `for,ahover,644` |
| `baseball` | yes | `tp`, `proposition` | `against,tp,all,ah,a,22` |
| `ih` | yes | `tp`, `proposition` | `for,tp,all,ah,a,-6` |
| `mma` | yes | `ml`, `dnb` | `for,dnb,a` |
| `boxing` | yes | `ml`, `dnb` | `against,ml,a` |
| `af` | yes (very rare) | `tp` | `against,tp,all,ml,a` |
| `basket_ht`, `hand`, `rl`, `ru`, `arf`, `volley`, `fb_book`, `darts` | no | – | events present, no prices observed for this account |

### 5.2 Family shapes

Format strings derived from observed examples. Square brackets denote
required positional parameters; sport-specific parameters can vary.

| Family | Format | Meaning (observed) |
|---|---|---|
| `h` / `d` / `a` | `<dir>,h` / `<dir>,d` / `<dir>,a` | Home / Draw / Away on the headline 1×2 (or two-way) market. |
| `ml` | `<dir>,ml,<side>` | Moneyline (no draw). `<side>` ∈ `h`, `a`. |
| `dnb` | `<dir>,dnb,<side>` | Draw-no-bet. `<side>` ∈ `h`, `a`. |
| `ah` | `<dir>,ah,<side>,<line>` | Asian handicap. `<side>` ∈ `h`, `a`. `<line>` is a signed integer (see §5.3). |
| `ahover` / `ahunder` | `<dir>,ahover,<line>` / `<dir>,ahunder,<line>` | Asian total over / under (match). `<line>` is a non-negative integer (see §5.3). |
| `over` / `under` | `<dir>,over,<line>` / `<dir>,under,<line>` | Total over / under with a **decimal** line, e.g. `for,over,6.5`. |
| `cs` | `<dir>,cs,<home>,<away>` | Correct score, two non-negative integers. |
| `score` | `<dir>,score,both` and `<dir>,score,both,no` | Both teams to score. `against,score,both` and `for,score,both,no` reference the same outcome (verified by price symmetry). |
| `win` | `<dir>,win,<runner_id>` | Outright winner. `<runner_id>` is an integer in the same ID space as `events.event_id`. |
| `tp` | `<dir>,tp,<period>,<inner>...` | Time-period scoped market. `<period>` observed: `all`. `<inner>` is a nested family — e.g. `for,tp,all,ml,a`, `for,tp,all,ah,a,-6`, `against,tp,all,ah,a,22`. |
| `tset` | `<dir>,tset,<period>,<void_rule>,<unit>[,<inner>...]` | Tennis selection. `<period>`: `all` observed. `<void_rule>`: `vwhole` observed. `<unit>`: `set` observed. With an inner market: `for,tset,all,vwhole,set,ah,p1,6` (set-based AH on player 1). |
| `ir` | `<dir>,ir,<home_score>,<away_score>,<inner>...` | Score-conditional market — pricing for an inner market given the current score is `<home_score>:<away_score>`. Observed only with empty `event_id` (see §6). |
| `proposition` | `<dir>,proposition,<json_array_as_text>` | Vendor-tagged proposition. The third token is a JSON array embedded verbatim in the bet_type string — e.g. `for,proposition,["Exact Total Runs 1st Half","0","Game Props - 1st Half"]`. Don't try to parse the array out of the bet_type by splitting on commas; treat the whole bet_type as opaque or parse with awareness of the embedded JSON. |

### 5.3 Handicap line encoding (advisory)

`ah`, `ahover` and `ahunder` use **signed (or unsigned) integers** as the
line parameter. Observed line ranges and step sizes by sport:

| Family | Sport | Range observed | Step |
|---|---|---|---|
| `ah` | `fb` | `-26 … +26` | mostly 1 (gaps at extremes) |
| `ah` | `fb_ht` | `-10 … +8` | 1 |
| `ah` | `fb_corn` | `-24 … +18` | 1–2 |
| `ah` | `fb_corn_ht` | `-12 … +10` | 1–2 |
| `ah` | `basket` | `-152 … +142` | 2 |
| `ahover` / `ahunder` | `fb` | `2 … 35` | 1 |
| `ahover` / `ahunder` | `fb_ht` | `2 … 15` | 1 |
| `ahover` / `ahunder` | `fb_corn` | `30 … 46` | 2 |
| `ahover` / `ahunder` | `fb_corn_ht` | `14 … 22` | 2 |
| `ahover` / `ahunder` | `basket` | `482 … 1202` | 2–4 |

The integer is a packed representation of a half- or quarter-line handicap;
the exact scaling constant is not exposed by the feed. **For storage, key
lookup and order-flow accounting, use the integer verbatim — never
round-trip through a float.** Consult MagicMarkets for the canonical scaling
rule if you need to display lines to end users.

## 6. Outright / generic markets (`event_id = ""`)

A small subset of `sptmkt` records have an empty string in the `event_id`
position. Examples observed:

```json
["upsert", "sptmkt", ["fb", "",     "against,win,374"],         { "price": 1.012 }]
["upsert", "sptmkt", ["basket", "", "for,win,40897"],           { "price": 1.584 }]
["upsert", "sptmkt", ["fb", "",     "for,ir,0,2,ah,a,-2"],      { "price": 2.176 }]
["upsert", "sptmkt", ["fb_ht", "",  "for,a"],                   { "price": ... }]
["upsert", "sptmkt", ["mma", "",    "for,dnb,a"],               { "price": ... }]
```

These represent markets that aren't tied to a specific scheduled fixture:
outright winners (`win,<runner_id>`), score-conditional pricing
(`ir,<h>,<a>,<inner>`) and similar. They are real upserts and update over
time; treat them as a separate "namespace" alongside fixture-keyed markets.

Outright winner records (`win`) carry an integer runner ID inside the
`bet_type` (e.g. `for,win,374`). That ID matches the team / runner IDs that
appear in `events.event_id` for fixtures involving that team.

## 7. Sport codes observed

The feed broadcasts events and markets under short sport codes. The list is
open-ended; treat any unknown code as opaque rather than rejecting the
record.

Codes observed in `events` (test account, 8-minute capture, 19 distinct):

`fb`, `fb_ht`, `fb_corn`, `fb_corn_ht`, `fb_book`, `basket`, `basket_ht`,
`baseball`, `af`, `ih`, `tennis`, `hand`, `rl`, `ru`, `arf`, `volley`,
`mma`, `boxing`, `darts`.

Codes observed in `sptmkt` (subset of the above, 11 distinct):

`fb`, `fb_ht`, `fb_corn`, `fb_corn_ht`, `basket`, `baseball`, `af`,
`ih`, `tennis`, `mma`, `boxing`.

Sports present in `events` but with no prices for this account during the
capture: `basket_ht`, `hand`, `rl`, `ru`, `arf`, `volley`, `fb_book`,
`darts`. This may be a permissions effect (the test account has limited
market access) or simply the absence of liquidity for those sports during
the capture window.

## 8. Errors and edge cases

### 8.1 REST

| HTTP | Body | When |
|---|---|---|
| 200 | `{...customer object}` | `/v1/config` with valid token |
| 200 | `{token, customer}` | `/v1/login` with valid creds |
| 200 | *(empty body)* | `/v1/logout` succeeds |
| 200 | – | `OPTIONS` preflight (`Allow: POST, OPTIONS` for `/v1/login`) |
| 400 | `{"message":"validation_error","errors":{...}}` | Required field missing on `/v1/login` (e.g. `username`) |
| 400 | `{"message":"The browser (or proxy) sent a request that this server could not understand."}` | Malformed JSON or empty body on `/v1/login` |
| 401 | `{"message":"No 'Authorization' header"}` | Header omitted |
| 401 | `{"message":"Invalid 'Authorization' header, must be 'Token <token>'"}` | Wrong scheme (`Bearer`, lowercase `token`, no scheme) |
| 401 | `{"message":"Authentication failed"}` | Token is invalid / revoked |
| 401 | `{"message":"The server could not verify that you are authorized to access the URL requested. ..."}` | `/v1/login` with wrong credentials |
| 404 | `{"message":"The requested URL was not found on the server. ..."}` | Any path other than `/v1/login`, `/v1/config`, `/v1/logout` |
| 415 | `{"message":"Did not attempt to load JSON data because the request Content-Type was not 'application/json'."}` | `/v1/login` without `Content-Type: application/json` |
| 500 | `{"message":"Internal Server Error"}` | `/v1/login` with `username` but no `password` field (server-side handling gap — supply both fields) |

REST responses support gzip when the client sends `Accept-Encoding: gzip`
(`content-encoding: gzip` in the response). `HEAD` requests are honored and
return no body.

### 8.2 WebSocket

| HTTP at upgrade | When |
|---|---|
| 101 | Auth OK, connection upgraded |
| 401 | `?token=` missing entirely from the URL **and** request reached the auth layer (rare — usually you get 502 instead) |
| 502 | Any failure path: missing/malformed/revoked token, or backend unavailable. **The 502 does not distinguish "bad token" from "backend down."** |

After the upgrade, the server **never closes the connection** for a
client-side protocol violation: invalid JSON, unknown commands, 100 KB of
random bytes, and binary frames were all silently discarded while the
stream continued. The server simply ignores all inbound frames.

## 9. REST endpoints

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/v1/login` | Exchange `{username, password}` for a `token`. |
| `GET`  | `/v1/config` | Return the authenticated customer record. |
| `POST` | `/v1/logout` | Revoke the bearer token used in the call. |
| `OPTIONS` | `/v1/login` | CORS preflight (`Allow: POST, OPTIONS`). |

No other paths responded. The feed is intentionally narrow.

## 10. WebSocket query parameters

| Parameter | Effect |
|---|---|
| `token=<token>` | Authentication. Required. First value wins if duplicated. |
| any other | Silently ignored. We tested `sport=fb`, `collection=events`, `foo=bar` — all three returned the full firehose with identical record counts and sport distributions. |

There is **no subscribe / filter protocol**. Every connection receives the
complete data set the account has access to.

## 11. Reference implementation

```python
import asyncio, json, time, websockets

URL = "wss://data.magicmarkets.com/v1/stream?token=" + TOKEN

async def main():
    store = {"events": {}, "sptmkt": {}}
    async with websockets.connect(URL, max_size=2**27) as ws:
        async for raw in ws:
            for op, coll, key, *rest in json.loads(raw)["data"]:
                key = tuple(key)
                if op == "upsert":
                    store[coll][key] = rest[0]
                elif op == "delete":
                    store[coll].pop(key, None)
            # `store` is now a live mirror of the feed.

asyncio.run(main())
```

For a snapshot-aware client (one that only begins serving reads once the
initial replay is complete), use one of the heuristics in §3.2 — for
example, "wait until you have seen ≥1 `sptmkt` upsert AND then a quiet
gap ≥2 s," or "wait until `frame.ts` is within 1 s of wall-clock."

Production clients should additionally:

- Configure WebSocket `max_size` large enough for the snapshot bursts
  (recommend `2**27` / 128 MB ceiling).
- Reconnect with exponential backoff on disconnect; expect each reconnect
  to re-stream the full ~190 000-record snapshot.
- Configure their own ping/pong keepalive (server does not send pings).
- Treat unknown sport codes and unknown bet-type families as opaque rather
  than rejecting records.

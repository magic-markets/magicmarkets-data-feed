# MagicMarkets Data Feed

A real-time, read-only stream of sports events and reference prices from
MagicMarkets. One WebSocket connection delivers every event, every score
change, and the latest reference price for every priced market the account
has access to.

```
                ┌──────────────────────────────┐
                │  data.magicmarkets.com       │
                │                              │
   wss://…  ──▶ │  /v1/stream   (firehose WS)  │
   POST    ──▶ │  /v1/login    (REST auth)    │
   GET     ──▶ │  /v1/config   (REST verify)  │
                └──────────────────────────────┘
```

## What the feed delivers

- A live `events` collection — fixtures (past, live, upcoming) keyed by
  `(sport, "YYYY-MM-DD,homeID,awayID")`, with competition metadata, scheduled
  start, in-running flag, current score and clock.
- A live `sptmkt` collection — one reference price per priced selection on
  each event, keyed by `(sport, event_id, bet_type)`.
- Both as a snapshot on every connect, then ongoing deltas.

The feed is **read-only**. It does not expose order books, individual trades,
or stake size — only the latest reference price per selection.

## Repository layout

```
.
├── README.md              ← this file
├── PROTOCOL.md            ← formal wire-format spec
├── LICENSE
├── examples/
│   ├── python/
│   │   ├── requirements.txt
│   │   ├── listen.py        minimal listener
│   │   ├── store.py         in-memory mirror with progress reporting
│   │   └── find_event.py    look up an event by team name, print its prices
│   └── node/
│       ├── package.json
│       └── listen.mjs       minimal Node listener
└── claude-skill/
    └── magicmarkets-data/   drop-in skill for Claude Code / Desktop
```

## Quick start

```bash
# 1. Install the example's one dependency
pip install -r examples/python/requirements.txt

# 2. Get a token
TOKEN=$(curl -sX POST https://data.magicmarkets.com/v1/login \
  -H 'Content-Type: application/json' \
  -d '{"username":"...","password":"..."}' \
  | python3 -c 'import sys,json; print(json.load(sys.stdin)["token"])')

# 3. Open the firehose
python3 examples/python/listen.py "$TOKEN"
```

Node equivalent:

```bash
cd examples/node && npm install
node listen.mjs "$TOKEN"
```

Minimal client:

```python
# examples/python/listen.py
import asyncio, json, sys, websockets

URL = f"wss://data.magicmarkets.com/v1/stream?token={sys.argv[1]}"

async def main():
    async with websockets.connect(URL, max_size=2**27) as ws:
        async for raw in ws:
            for op, coll, key, *rest in json.loads(raw)["data"]:
                print(op, coll, key, rest[0] if rest else "")

asyncio.run(main())
```

## Examples

| File | What it does |
|---|---|
| [`examples/python/listen.py`](examples/python/listen.py) | Minimal listener — prints every record. |
| [`examples/python/store.py`](examples/python/store.py) | Maintains a local in-memory mirror keyed by `(collection, key)`. |
| [`examples/python/find_event.py`](examples/python/find_event.py) | Look up an event by team-name fragment and print its current prices. |
| [`examples/node/listen.mjs`](examples/node/listen.mjs) | Minimal Node listener (uses `ws`). |

## Wire format at a glance

```json
{ "ts": 1778476931.285601, "data": [
  ["upsert", "events", ["fb", "2026-05-09,969,1738"], { ... }],
  ["upsert", "sptmkt", ["fb", "2026-05-09,969,1738", "for,h"], { "price": 1.91 }],
  ["delete", "sptmkt", ["fb", "2026-05-12,26000,37537", "for,ahunder,19"]]
]}
```

- `op` is `"upsert"` or `"delete"`.
- `collection` is `"events"` or `"sptmkt"`.
- `value` is present only for `"upsert"` and has a fixed shape per collection
  (`events`: nine fields; `sptmkt`: just `{price}`).

Full specification with key shapes, value shapes, bet-type families,
handicap-line encoding, and edge cases: [**PROTOCOL.md**](PROTOCOL.md).

## Observed throughput

Single connection, all sports the account has access to:

| Phase | Records | Wall-clock |
|---|---|---|
| Initial replay (`events` first, then `sptmkt`) | ~190 000 records | 4–15 s, with internal gaps |
| Steady-state deltas | ~70 records/sec average (≈12 events + ≈58 sptmkt) | continuous |
| Steady-state bursts | several hundred records/sec | during heavy market activity |

The replay is two-phase: the full `events` collection is upserted first,
then the full `sptmkt` collection, then steady-state deltas begin. There is
**no "snapshot complete" sentinel**, and the replay itself contains many
internal gaps (up to several seconds *within* the events phase). Don't
detect snapshot completion with a naive short-quiet-gap heuristic — use
something like "at least one `sptmkt` upsert seen, then a quiet gap ≥2 s,"
or "frame.ts within 1 s of wall-clock." See [PROTOCOL.md §3](PROTOCOL.md)
for details.

WebSocket frames are compressed with `permessage-deflate` by default; REST
responses support `Accept-Encoding: gzip`. Plan for a JSON parser fast
enough to keep up with the snapshot burst (`orjson` / `simdjson` recommended
for high-volume bots), and treat the local store as a flat dictionary keyed
by `(collection, key_tuple)`.

The server does not send WebSocket ping frames — a 60-second idle test with
the client also silent did not produce any disconnect — but production
clients should still configure a ping/pong timeout on their own side.

## Authentication

Send `{"username", "password"}` to `POST /v1/login` and reuse the returned
`token` for both REST and the WebSocket:

| Channel | How |
|---|---|
| REST | `Authorization: Token <token>` header |
| WebSocket | `?token=<token>` query parameter — **not** a header |

Sending the WebSocket connection request with only the header (no query
parameter) returns `HTTP 502` — the streaming backend refuses the upgrade.

Treat tokens like passwords: load from an environment variable
(`export MM_DATA_TOKEN=...`), don't commit them, don't log them.

## Using the feed with an LLM

This repo ships a ready-to-install **Claude skill** that teaches Claude how
to connect to the feed, build a local cache, answer questions like "what's
the current price on team X tonight," and translate between team names and
event IDs.

```
claude-skill/magicmarkets-data/
├── SKILL.md
├── references/protocol.md
└── examples/
```

Install it by copying `claude-skill/magicmarkets-data/` into one of:

- **Claude Code** — `~/.claude/skills/` (user-wide) or `.claude/skills/`
  (per-project).
- **Claude Desktop** — drop the folder into the skills directory shown by
  Settings → Skills.

Then ask Claude something like *"using the magicmarkets data feed, what's
the current home/draw/away price on tonight's PSG vs Lyon match?"* — the
skill will trigger automatically.

## Getting credentials

Contact MagicMarkets to request data-feed credentials for production use.

## License

MIT — see [LICENSE](LICENSE).

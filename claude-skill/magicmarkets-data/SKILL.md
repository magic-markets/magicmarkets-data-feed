---
name: magicmarkets-data
description: >-
  Connect to and query the MagicMarkets data feed, a read-only real-time
  WebSocket stream of sports events, live scores and decimal reference prices
  at data.magicmarkets.com (wss://data.magicmarkets.com/v1/stream, REST
  /v1/login, /v1/config, /v1/logout). Use when the user mentions the
  MagicMarkets data feed, data.magicmarkets.com, the MagicMarkets firehose,
  MM_DATA_TOKEN, the events or sptmkt collections, wants the current
  MagicMarkets price or live score for a match or team, wants to build a
  local mirror of MagicMarkets prices, or needs to decode a MagicMarkets
  bet_type or handicap line from the feed. Not for placing orders: trading
  uses the MagicMarkets API v2 at magicmarkets.com/v2, a different product.
allowed-tools: Bash(python3:*), Bash(pip:*), Bash(pip3:*), Bash(node:*), Bash(npm:*), Bash(curl:*), Read
---

# MagicMarkets data feed

The feed is a read-only firehose. One WebSocket connection receives every
event and reference price the account is entitled to: a full snapshot, then
live deltas. There is no filter protocol and no order placement.

This skill is for Claude Code on a machine that can reach
`data.magicmarkets.com`, with `MM_DATA_TOKEN` set in the environment. It is
not supported on claude.ai for now.

Detail: [references/protocol.md](references/protocol.md). Working code:
[examples/](examples/). Run or adapt the examples rather than writing a
client from scratch.

## Rules

- **Never print, echo or log the token.** Read it from `MM_DATA_TOKEN`. The
  stream URL contains the token, so redact URLs (`redact()` in
  `examples/mmfeed.py`).
- **Never ask for the user's password in chat.** If `MM_DATA_TOKEN` is not
  set, give the user the login snippet below to run in their own shell.
- **Never call `POST /v1/logout`** unless the user asks to revoke a token and
  confirms it is not shared. Logout revokes it for every client.
- **Answer with sourced prices only.** Quote the price as the feed gave it,
  name the event and the `bet_type`, and say when you read it. If the feed has
  no price, say so. Do not estimate.
- **No trading advice and no predictions.** Do not recommend a side, a stake
  or a trade, and do not forecast results. Converting a price to an implied
  probability (`1/price`) is fine as arithmetic.

## Endpoints and auth

| | |
|---|---|
| WebSocket | `wss://data.magicmarkets.com/v1/stream?token=<token>` (token URL-encoded, once) |
| REST | `POST /v1/login`, `GET /v1/config`, `POST /v1/logout` on `https://data.magicmarkets.com` |
| REST auth | Header `Authorization: Token <token>` (case-sensitive scheme) |

Login, for the user to run in their own bash or zsh shell. It prompts for
the username and password, keeps the password out of the command line and
history, and prints `login failed` on bad credentials:

```bash
printf 'Username: '; read -r MM_USER
printf 'Password: '; stty -echo; IFS= read -r MM_PASS; stty echo; printf '\n'
export MM_USER MM_PASS
MM_DATA_TOKEN=$(python3 -c 'import json,os; print(json.dumps({"username": os.environ["MM_USER"], "password": os.environ["MM_PASS"]}))' \
  | curl -sf -X POST https://data.magicmarkets.com/v1/login -H 'Content-Type: application/json' --data-binary @- \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["token"])' 2>/dev/null) \
  && export MM_DATA_TOKEN || echo "login failed" >&2
unset MM_PASS
```

Check a token without printing it:

```bash
curl -s -o /dev/null -w '%{http_code}\n' https://data.magicmarkets.com/v1/config \
  -H "Authorization: Token $MM_DATA_TOKEN"
```

`200` means valid, `401` means missing, malformed or revoked. Check this
first: every failed WebSocket upgrade returns `502`, whatever the cause.

## Frames and collections

```json
{"ts": 1778476931.285601, "data": [
  ["upsert", "sptmkt", ["fb", "2026-05-11,969,1738", "for,h"], {"price": 2.10}],
  ["delete", "sptmkt", ["fb", "2026-05-12,26000,37537", "for,ahunder,19"]]
]}
```

Records are `[op, collection, key, value?]`. Apply them in order. An upsert
carries the full value: replace, do not merge. Ignore unknown collections,
ops, fields, sport codes and families.

| Collection | Key | Value |
|---|---|---|
| `events` | `[sport, event_id]` | `competition_id`, `competition_name`, `competition_country`, `home`, `away`, `start_ts`, `ir`, `score`, `ir_time` |
| `sptmkt` | `[sport, event_id, bet_type]` | `{"price": <decimal odds>}` |

Treat `event_id` as an opaque key. Rows with `event_id = ""` are not tied to
a fixture, and the feed has no runner names for them.

## Snapshot gate

No message marks the end of the replay. Use the key-novelty gate that
`SnapshotTracker` in `examples/mmfeed.py` implements: complete once at least
one `sptmkt` upsert has arrived and either fewer than half the upserts in the
latest 1 s window added a new key, or 2 s pass with no frames. Give up after
30 s and say the data may be incomplete. The replay usually finishes within a
few seconds.

Do not use `frame.ts` near wall-clock as the signal: replay frames are sent
in real time. Do not wait for a quiet gap alone: the live flow rarely pauses.
Every reconnect replays the full snapshot.

## Bet types

Sport codes and `bet_type` grammar are shared with the MagicMarkets API v2.
The canonical lists are in the "Sports & bet types" section of
https://magicmarkets.com/llms-full.txt. Fetch it for any code or family you
cannot interpret.

`<direction>,<family>[,<param>...]`. `for` prices the outcome happening,
`against` prices it not happening.

| Example | Meaning |
|---|---|
| `for,h` / `for,d` / `for,a` | Home, draw, away |
| `for,ah,h,-4` | Asian handicap, home -1.0 |
| `for,ah,a,-4` | Asian handicap, away +1.0 (the integer is the home handicap) |
| `for,ahover,7` | Asian total over 1.75 |
| `for,tahover,h,2` | Home team Asian total over 0.5 |
| `for,over,2.5` | Total over 2.5 (decimal line, not scaled) |
| `for,cs,2,1` | Correct score 2-1 |
| `for,tp,all,ah,a,-6` | Period-scoped market: away +1.5 |
| `for,tset,all,vwhole,set,ah,p1,6` | Tennis set handicap on player 1 (home) |
| `for,ir,0,2,ah,a,-2` | Inner market price given a score of 0:2 |
| `for,proposition,[...]` | Named proposition. Treat the JSON array as opaque. |

**Handicap rule.** Lines in `ah`, `ahover`, `ahunder`, `tahover` and
`tahunder`, in any wrapper (`tp`, `tset`, `ir` and other period tokens), are
4 x the real line. In `ah` the integer is the home handicap and the side
picks the team: `ah,h,4` is home +1.0 and `ah,a,4` is away -1.0. Totals are
not signed by side. `describe_line()` returns the display form, for example
`away -1.0` or `over 2.5`. Keep the integer for keys. Split bet types with
`split_bet_type()`, not `split(",")`.

## Examples to use

The Python examples need Python 3.10+ and `websockets>=13`. Install with
`pip install "websockets>=13"`. If the system pip refuses, create a virtual
environment first: `python3 -m venv ~/.venvs/mmfeed`, then
`~/.venvs/mmfeed/bin/pip install "websockets>=13"`, and run the examples with
`~/.venvs/mmfeed/bin/python`.

The examples read the token from `MM_DATA_TOKEN` only. Call them by absolute
path so they work from any directory:

| Command | Use it for |
|---|---|
| `python3 "<this skill's folder>/examples/find_event.py" Manchester United` | **"What is the price on X?"** |
| `python3 "<this skill's folder>/examples/store.py"` | A live local mirror with a status line each second |
| `python3 "<this skill's folder>/examples/listen.py"` | Raw records, for debugging |

`examples/mmfeed.py` has the helpers for new code: `stream_frames`
(reconnecting; it yields `RECONNECTED` before the replay that follows each
reconnect), `Store`, `SnapshotTracker`, `split_bet_type`, `decode_line`,
`describe_line`, `parse_event_id` and `redact`.

`find_event.py` joins its arguments into one query, waits for the snapshot
gate, then prints matching events with their prices and decoded lines, for
example `line=away -1.0`. If the gate times out it warns on stderr and
prints what it has. Use part of one team name, not both teams. If several
events match, show them and ask which one the user means. Report prices
plainly, for example "Arsenal to win (`for,h`): 2.10".

## Common mistakes

- Sending the token to the WebSocket in a header. Use `?token=`. REST is the
  opposite: header only.
- Calling REST without your own `User-Agent`. The Python standard library
  default (`Python-urllib`) is rejected with `403`. Set a `User-Agent`, or
  use curl.
- Treating a quiet gap or `frame.ts` as the end of the snapshot.
- Splitting a `bet_type` on every comma.
- Showing a handicap wire integer as the line, or ignoring the side. Use
  `describe_line()`.
- Using an empty `event_id` as if it were a fixture.
- Expecting query parameters or client messages to filter the stream. The
  server ignores both.
- Treating `sptmkt` as an order book. It is one reference price per
  selection.
- Reconnecting in a tight loop. Each reconnect replays the whole snapshot.
- Printing a stream URL. It contains the token.

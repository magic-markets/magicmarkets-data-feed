# MagicMarkets Data Feed

[![CI](https://github.com/magicmarkets/magicmarkets-data-feed/actions/workflows/ci.yml/badge.svg)](https://github.com/magicmarkets/magicmarkets-data-feed/actions/workflows/ci.yml)

A read-only, real-time WebSocket stream of sports events, live scores and
reference prices from MagicMarkets. This repository holds the protocol
reference, example clients in Python and Node, and a Claude skill.

```
  your client                                 data.magicmarkets.com
  -----------                                 ---------------------
  POST /v1/login   {username, password}  -->  {token, customer}
  GET  /v1/config  Authorization: Token  -->  200 if the token is valid
  WSS  /v1/stream?token=<token>          -->  snapshot, then live deltas
```

## Quick start

You need Python 3.10+ or Node 20+. The code blocks below work when pasted
into bash or zsh.

**1. Get the code.**

```bash
git clone https://github.com/magicmarkets/magicmarkets-data-feed.git
cd magicmarkets-data-feed
python3 -m venv .venv && . .venv/bin/activate
pip install -r examples/python/requirements.txt
```

**2. Get a token.** This prompts for your feed username and password, sends
them on stdin (the password never appears in the command line or shell
history) and exports `MM_DATA_TOKEN`. It prints `login failed` if the
credentials are wrong.

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

**3. Run the examples.** They read the token from `MM_DATA_TOKEN` only.
`listen.py` prints every record. `find_event.py` prints current prices for
events that match a team name.

```bash
python examples/python/listen.py
python examples/python/find_event.py Manchester United
```

Node:

```bash
cd examples/node && npm install
node listen.mjs
node find_event.mjs arsenal
```

Treat the token like a password. Do not commit it or paste it into issues.

### Try it without credentials

`make mock` serves a recorded sample session on
`ws://127.0.0.1:8765/v1/stream`. Run it in one terminal, then point the
examples at it from another with `MM_DATA_URL`:

```bash
make mock
```

```bash
MM_DATA_URL=ws://127.0.0.1:8765/v1/stream MM_DATA_TOKEN=demo python examples/python/find_event.py arsenal
```

`MM_DATA_URL` overrides the stream URL in every example. The default is
`wss://data.magicmarkets.com/v1/stream`.

## What you get

| Collection | Key | Value |
|---|---|---|
| `events` | `[sport, event_id]` | Competition, teams, start time, in-running flag, score, clock |
| `sptmkt` | `[sport, event_id, bet_type]` | `{"price": <decimal_odds>}` |

Each frame is `{"ts": ..., "data": [[op, collection, key, value?], ...]}`
with `op` either `upsert` or `delete`. Before you build a client, read these
points in [PROTOCOL.md](PROTOCOL.md):

- No message marks the end of the snapshot. Use the key-novelty gate in
  [§3.2](PROTOCOL.md#32-detecting-snapshot-complete).
- Every reconnect replays the full snapshot.
- Asian handicap lines are 4 x the home handicap: `for,ah,h,-4` is home -1.0
  and `for,ah,a,-4` is away +1.0
  ([§5.3](PROTOCOL.md#53-handicap-line-encoding)).
- Sport codes and `bet_type` grammar are shared with the MagicMarkets API v2
  ([magicmarkets.com/llms-full.txt](https://magicmarkets.com/llms-full.txt)).
  Ignore anything you do not recognise.
- REST calls need a `User-Agent`. Python's `urllib` default is rejected.

## Examples

| File | What it does |
|---|---|
| [`examples/python/mmfeed.py`](examples/python/mmfeed.py) | Helper library: reconnecting stream, `Store`, `SnapshotTracker`, `split_bet_type`, `decode_line`, `describe_line`, token redaction |
| [`examples/python/listen.py`](examples/python/listen.py) | Prints every record |
| [`examples/python/store.py`](examples/python/store.py) | Live in-memory mirror with a status line each second |
| [`examples/python/find_event.py`](examples/python/find_event.py) | Prices for events that match a team name, with decoded lines such as `line=away -1.0` |
| [`examples/node/mmfeed.mjs`](examples/node/mmfeed.mjs) | The same helpers for Node |
| [`examples/node/listen.mjs`](examples/node/listen.mjs) | Prints every record |
| [`examples/node/find_event.mjs`](examples/node/find_event.mjs) | Prices for events that match a team name, with decoded lines |

## Tests

| Command | What it does |
|---|---|
| `make install` | Installs dev dependencies. Run it inside the virtual environment. |
| `make check` | Lint, tests against a local mock server, skill sync check, copy and link checks. CI runs the same. |
| `make test-live` | Opt-in smoke test against the live feed. Needs `MM_DATA_TOKEN`. CI never runs it. |

## Claude skill

[`claude-skill/magicmarkets-data/`](claude-skill/magicmarkets-data/) teaches
Claude Code to connect to the feed, wait for the snapshot, decode bet types
and answer price questions with sourced prices. Copy the folder to
`~/.claude/skills/` (all projects) or `.claude/skills/` (one project). The
skill needs a machine that can reach `data.magicmarkets.com` and
`MM_DATA_TOKEN` set in the environment.

## Access and terms

Feed credentials come from MagicMarkets. Contact us through
[magicmarkets.com](https://magicmarkets.com).

The MIT licence covers the code in this repository. Use of the feed data is
governed by your agreement with MagicMarkets.

## More

- [PROTOCOL.md](PROTOCOL.md): the full protocol reference
- [CHANGELOG.md](CHANGELOG.md), [CONTRIBUTING.md](CONTRIBUTING.md),
  [SECURITY.md](SECURITY.md), [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md)

## License

MIT. See [LICENSE](LICENSE).

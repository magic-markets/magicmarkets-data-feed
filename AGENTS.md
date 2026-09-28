# Agent guidelines

Guidance for coding agents that work in this repository.

## What this repository is

Documentation, example clients and a Claude skill for the MagicMarkets data
feed: a read-only WebSocket stream at `wss://data.magicmarkets.com/v1/stream`.
There is no package to publish and no server code.

## Layout

| Path | Role |
|---|---|
| `PROTOCOL.md` | The v1 protocol reference. Source of truth for wire behaviour. |
| `README.md` | Front page and quick start. |
| `examples/python/mmfeed.py` | Helper library (stdlib and `websockets` only). The other Python examples build on it. |
| `examples/python/{listen,store,find_event}.py` | Runnable examples. |
| `examples/node/` | Node 20+ equivalents (`ws` only), with tests in `examples/node/test/`. |
| `tests/` | Python tests and the local mock server. |
| `scripts/sync_skill.py` | Copies `PROTOCOL.md` and the Python examples into the skill. |
| `scripts/check_copy.py`, `scripts/check_links.py` | Copy, brand and relative link checks. |
| `claude-skill/magicmarkets-data/SKILL.md` | The skill. Hand-written. |
| `claude-skill/magicmarkets-data/references/protocol.md` | Generated from `PROTOCOL.md`. Never edit by hand. |
| `claude-skill/magicmarkets-data/examples/` | Generated from `examples/python/`. Never edit by hand. |

## Commands

| Command | When |
|---|---|
| `make install` | Once, to install dev dependencies |
| `make check` | Before you finish: lint, tests, sync check, copy and link checks |
| `make sync-skill` | After any change to `PROTOCOL.md` or `examples/python/` |
| `make mock` | Serve the fixture session on `ws://127.0.0.1:8765/v1/stream` for manual runs |

## Rules

- **Do not call the live feed from tests or CI.** Tests use the local mock
  server through `MM_DATA_URL`. The live smoke test runs only with
  `pytest -m live` and a developer's own `MM_DATA_TOKEN`.
- **Examples read the token from `MM_DATA_TOKEN` only.** No positional
  token and no token flag.
- **Never print, log or commit a token.** The token is part of the stream
  URL, so pass URLs through `redact()` before you log them.
- **Never call `POST /v1/logout`** in code or tests. It revokes the token for
  every client that shares it.
- **Do not invent protocol facts.** If behaviour is not in `PROTOCOL.md`, do
  not document or depend on it. Sport codes and `bet_type` grammar come from
  the MagicMarkets API v2 docs at `https://magicmarkets.com/llms-full.txt`.
- **Ignore unknowns.** Client code must not fail on unknown fields, sport
  codes, families, collections or ops.
- **Handicap lines** in `ah`, `ahover`, `ahunder`, `tahover` and `tahunder`
  are 4 x the real line, in any wrapper. In `ah` the integer is the home
  handicap and the side picks the team (`ah,a,4` is away -1.0). Keep the
  wire integer for keys. Use `describe_line()` for display.
- **Snapshot gate is key novelty.** Complete after at least one `sptmkt`
  upsert when fewer than half the upserts in the latest 1 s window add a new
  key, or after a 2 s quiet gap, with a 30 s maximum wait. Never treat
  `frame.ts` near wall-clock as completion.
- **Set a `User-Agent`** on any REST call made with Python `urllib`. The
  default one gets `403`.
- **Split bet types with `split_bet_type()`.** A `proposition` bet type
  embeds a JSON array that contains commas.
- **Keep the Python and Node helpers in step.** A behaviour change in one
  needs the same change and a test in the other.
- **Keep `PROTOCOL.md` section numbers stable** and do not use relative file
  links in it: it is copied into the skill.
- **Follow the copy rules** in [CONTRIBUTING.md](CONTRIBUTING.md#copy-rules):
  no em or en dashes, no emojis, `MagicMarkets` as one word, placeholder
  account values only. Public docs describe only what a client needs: no
  server internals, no measurement narration and no service commitments.

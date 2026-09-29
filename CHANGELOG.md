# Changelog

Notable changes to this repository. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and releases use
[Semantic Versioning](https://semver.org/spec/v2.0.0.html). The feed protocol
itself is identified by its URL path (`/v1/`).

## [Unreleased]

### Changed

- The repository moved to the `magicmarkets` GitHub organisation. All links
  now point to github.com/magicmarkets/magicmarkets-data-feed. The old URL
  redirects.

## [1.0.0] - 2026-09-28

### Breaking

- The examples read the token from `MM_DATA_TOKEN` only. They no longer
  accept a token as a command-line argument.
- The Node examples need Node 20 or later.

### Added

- Handicap sign convention and decoding rule (§5.3): the integer is 4 x the
  home handicap, and the side picks the team.
- Links to the canonical sport codes and `bet_type` grammar, which the feed
  shares with the MagicMarkets API v2.
- `User-Agent` requirement for REST requests (§1), and a Limits section
  (§3.7).
- `examples/python/mmfeed.py` and `examples/node/mmfeed.mjs`: helper
  libraries with a reconnecting stream, `Store`, `SnapshotTracker`,
  `split_bet_type`, `decode_line`, `describe_line` and token redaction.
- `examples/node/find_event.mjs`.
- `MM_DATA_URL` to override the stream URL, and `make mock` to try the
  examples against a recorded sample session without credentials.
- Python and Node tests against a local mock server, an opt-in live smoke
  test, a `Makefile` and GitHub Actions CI.
- `CONTRIBUTING.md`, `SECURITY.md`, `CODE_OF_CONDUCT.md`, `AGENTS.md`, issue
  templates and a pull request template.

### Changed

- Snapshot detection (§3.2) now uses a key-novelty gate. The earlier
  `frame.ts` and quiet-gap heuristics are not reliable and are no longer
  recommended.
- `PROTOCOL.md` reorganised and shortened. Volumes, timings and frame sizes
  are stated as indicative.
- Family and sport-code lists are examples, not closed lists.
- `listen.py`, `store.py` and `find_event.py` use `mmfeed.py`, reconnect with
  backoff and redact the token. `find_event.py` joins its arguments into one
  query and prints side-aware lines such as `line=away -1.0`.
- `SKILL.md` rewritten for Claude Code, with rules on token handling,
  `/v1/logout` and sourced answers.
- README rewritten.

## [0.1.0] - 2026-05-11

### Added

- Initial release: protocol reference, Python examples, a Node listener and
  the `magicmarkets-data` Claude skill.

[1.0.0]: https://github.com/magicmarkets/magicmarkets-data-feed/releases/tag/v1.0.0
[0.1.0]: https://github.com/magicmarkets/magicmarkets-data-feed/tree/aa5ab33

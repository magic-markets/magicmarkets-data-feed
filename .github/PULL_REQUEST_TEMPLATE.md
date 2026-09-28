## What does this change do, and why?

<!-- Explain the reason. Reviewers can read the diff. -->

## How did you verify it?

<!--
Say what you ran. Tests use a local mock server. If you ran the opt-in live
smoke test (pytest -m live) or an example against the live feed, say so.
Never paste a token or a stream URL.
-->

- [ ] `make check` passes
- [ ] I added or updated tests for any behaviour change in `mmfeed.py` or `mmfeed.mjs`, and kept the Python and Node helpers in step
- [ ] I ran `make sync-skill` after changing `PROTOCOL.md` or `examples/python/`
- [ ] Protocol changes describe confirmed behaviour only, and `PROTOCOL.md` cross references still resolve
- [ ] No tokens, passwords, stream URLs or real account data in code, docs, fixtures or logs

## Anything reviewers should look at closely?

<!-- Deliberate scope cuts, follow-up work, open questions. -->

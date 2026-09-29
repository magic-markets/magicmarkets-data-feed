# Security policy

## Supported versions

Only the latest release of this repository receives fixes.

## Reporting a vulnerability

**Do not open a public issue for a security problem.**

Report it privately with GitHub
[private vulnerability reporting](https://github.com/magicmarkets/magicmarkets-data-feed/security/advisories/new)
(the "Report a vulnerability" button on the repository's Security tab). This
lets you share details and proof-of-concept code with the maintainers before
anything is public.

Examples of what to report:

- Code in this repository that could expose a token, for example by logging
  a stream URL without redaction.
- Examples or skill instructions that could lead an agent to leak a token or
  revoke a shared token.
- A crafted feed message that makes an example execute code or write
  outside its working directory.

We acknowledge reports as soon as we can and follow up once we confirm the
issue.

## Tokens

- Never paste a token, a password or a stream URL (it contains the token)
  into an issue, pull request, discussion or log.
- If you expose a token by mistake, revoke it with `POST /v1/logout` and log
  in again for a new one. Logout revokes the token for every client that
  uses it.

## Scope

This policy covers the code and documentation in this repository. For
problems with the data feed service itself, contact MagicMarkets through
[magicmarkets.com](https://magicmarkets.com).

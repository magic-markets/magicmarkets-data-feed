"""Small helper library for the MagicMarkets data feed.

Single file, standard library plus ``websockets`` only. Copy it next to your
own code or import it from the examples in this folder.

What it gives you:

- ``get_token`` / ``stream_url`` / ``redact``: read the token from
  ``MM_DATA_TOKEN``, build the stream URL, and keep the token out of logs.
- ``split_bet_type`` / ``decode_line`` / ``handicap_line`` / ``describe_line``:
  read bet_type strings, including proposition JSON and packed handicap lines.
- ``Store``: an in-memory mirror of the ``events`` and ``sptmkt`` collections.
- ``SnapshotTracker``: decides when the initial snapshot replay has finished.
- ``stream_frames``: an async generator of parsed frames that reconnects with
  capped exponential backoff and jitter.

The protocol is described in PROTOCOL.md (references/protocol.md in the skill).
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import re
import ssl
import sys
import time
from collections import deque
from collections.abc import AsyncIterator, Callable
from typing import Any
from urllib.parse import parse_qsl, quote, urlencode, urlsplit, urlunsplit

try:
    from websockets.asyncio.client import connect
    from websockets.exceptions import ConnectionClosedOK, InvalidStatus, InvalidURI, WebSocketException
except ImportError:  # pragma: no cover - depends on the environment
    sys.exit('mmfeed needs the websockets package: pip install "websockets>=13"')

DEFAULT_URL = "wss://data.magicmarkets.com/v1/stream"

# Frames are a few KB; this is a generous ceiling (4 MiB).
MAX_SIZE = 2**22

# Families whose line parameter is an integer equal to 4 x the real line.
HANDICAP_FAMILIES = frozenset({"ah", "ahover", "ahunder", "tahover", "tahunder"})

COLLECTIONS = ("events", "sptmkt")

log = logging.getLogger("mmfeed")


class _RedactFilter(logging.Filter):
    """Redact tokens from every record, including tracebacks and websockets debug output."""

    def filter(self, record: logging.LogRecord) -> bool:
        record.msg, record.args = redact(record.getMessage()), ()
        if record.exc_info and not record.exc_text:
            record.exc_text = logging.Formatter().formatException(record.exc_info)
        if record.exc_text:
            record.exc_text = redact(record.exc_text)
        return True


# websockets logs the request line (with the query string) at DEBUG level.
# Route its logs through a filtered logger so the token never reaches a handler.
_ws_log = logging.getLogger("mmfeed.websockets")
for _logger in (log, _ws_log):
    _logger.addFilter(_RedactFilter())


# --------------------------------------------------------------------------
# Token and URL handling
# --------------------------------------------------------------------------


def get_token() -> str:
    """Return the token from the ``MM_DATA_TOKEN`` environment variable.

    The examples never take the token as an argument: arguments are visible
    in ``ps`` output and in shell history. Exits with a message if it is unset.
    """
    token = os.environ.get("MM_DATA_TOKEN", "").strip()
    if not token:
        sys.exit("error: set MM_DATA_TOKEN (see README)")
    return token


def stream_url(token: str, base: str | None = None) -> str:
    """Build the WebSocket URL with the token URL-encoded and passed once.

    ``base`` defaults to the ``MM_DATA_URL`` env var, then ``DEFAULT_URL``.
    Any ``token`` parameter already in ``base`` is dropped (the server uses
    the first ``token`` value, so a stale one would win). Other query
    parameters are kept, although the server ignores them. Raises
    ``ValueError`` unless the URL starts with ``ws://`` or ``wss://``.
    """
    base = base or os.environ.get("MM_DATA_URL") or DEFAULT_URL
    parts = urlsplit(base)
    if parts.scheme not in ("ws", "wss") or not parts.netloc:
        raise ValueError(f"stream URL must start with ws:// or wss:// (got {redact(base, token)!r})")
    query = [(k, v) for k, v in parse_qsl(parts.query, keep_blank_values=True) if k != "token"]
    query.append(("token", token))
    return urlunsplit(parts._replace(query=urlencode(query, quote_via=quote)))


_TOKEN_PARAM = re.compile(r"(?i)(\btoken=)[^&\s#'\"]+")
_TOKEN_HEADER = re.compile(r"(\bToken\s+)[A-Za-z0-9._~%+/=-]{16,}")


def redact(text: object, token: str | None = None) -> str:
    """Hide token values in text that is about to be logged or printed.

    Replaces ``token=<value>`` query parameters and ``Token <value>``
    header values. If ``token`` is given, every raw or URL-encoded copy of
    it is replaced as well.
    """
    out = str(text)
    if token:
        for form in {token, quote(token, safe="")}:
            out = out.replace(form, "***")
    out = _TOKEN_PARAM.sub(r"\1***", out)
    return _TOKEN_HEADER.sub(r"\1***", out)


# --------------------------------------------------------------------------
# bet_type helpers
# --------------------------------------------------------------------------

_json = json.JSONDecoder()
_INT = re.compile(r"-?[0-9]+")
_NUMBER = re.compile(r"-?[0-9]+(?:\.[0-9]+)?")


def split_bet_type(bet_type: str) -> list[str]:
    """Split a bet_type on commas, keeping an embedded JSON array as one token.

    >>> split_bet_type("for,ah,h,-4")
    ['for', 'ah', 'h', '-4']
    >>> split_bet_type('for,proposition,["Total, 1st Half","0"]')
    ['for', 'proposition', '["Total, 1st Half","0"]']

    If embedded JSON does not parse, the rest of the string is returned as a
    single opaque token rather than being split on its commas.
    """
    if not bet_type:
        return []
    tokens: list[str] = []
    i, n = 0, len(bet_type)
    while True:
        if i < n and bet_type[i] in "[{":
            try:
                _, end = _json.raw_decode(bet_type, i)
            except ValueError:
                tokens.append(bet_type[i:])
                return tokens
            if end < n and bet_type[end] != ",":
                tokens.append(bet_type[i:])
                return tokens
        else:
            end = bet_type.find(",", i)
            end = n if end == -1 else end
        tokens.append(bet_type[i:end])
        if end >= n:
            return tokens
        i = end + 1


def decode_line(n: int) -> float:
    """Convert a handicap wire integer to the real line: 2 -> 0.5, -21 -> -5.25.

    Only for the handicap families (ah, ahover, ahunder, tahover, tahunder).
    Keep the integer for storage and keys; decode only for display.
    """
    return int(n) / 4


def _find_handicap(tokens: list[str]) -> tuple[str, str | None, int] | None:
    """(family, side token or None, wire integer) of the first handicap family."""
    for i, token in enumerate(tokens[1:], start=1):
        if token in HANDICAP_FAMILIES:
            params = tokens[i + 1 : i + 3]
            for j, param in enumerate(params):
                if _INT.fullmatch(param):
                    side = params[0] if j == 1 else None
                    return token, side, int(param)
            return None
    return None


def handicap_line(bet_type: str) -> float | None:
    """Return the raw decoded line of a bet_type (wire integer / 4), or None.

    For ``ah`` this is the HOME handicap, whichever side the bet_type names:
    ``for,ah,a,-4`` gives -1.0 (home -1.0, so away +1.0). Use
    ``describe_line`` for a side-aware display string. Finds the first
    handicap family, also inside ``tp``, ``tset`` and ``ir`` wrappers.
    Decimal lines such as ``for,over,2.5`` and correct scores are not decoded.
    """
    found = _find_handicap(split_bet_type(bet_type))
    return decode_line(found[2]) if found else None


_SIDE_NAMES = {"h": "home", "a": "away"}
_AWAY_SIDES = {"a", "p2"}


def _signed(value: float) -> str:
    return "0.0" if value == 0 else f"{value:+}"


def describe_line(bet_type: str) -> str | None:
    """Return the line as a person would read it, or None if there is none.

    >>> describe_line("for,ah,h,4"), describe_line("for,ah,a,4")
    ('home +1.0', 'away -1.0')
    >>> describe_line("for,ahover,10"), describe_line("for,tset,all,vwhole,set,ah,p1,6")
    ('over 2.5', 'p1 +1.5')

    The ``ah`` integer is the home handicap x 4, so the away side's line is
    its negation. Totals (``ahover``, ``ahunder``, ``tahover``, ``tahunder``)
    are not signed by side; team totals name the team: ``home over 0.5``.
    """
    found = _find_handicap(split_bet_type(bet_type))
    if found is None:
        return None
    family, side, wire = found
    line = decode_line(wire)
    if family != "ah":
        total = f"{'over' if family.endswith('over') else 'under'} {line}"
        return f"{_SIDE_NAMES.get(side, side)} {total}" if side else total
    if side in _AWAY_SIDES:
        line = -line
    return f"{_SIDE_NAMES.get(side, side or 'home')} {_signed(line)}"


def bet_type_sort_key(bet_type: str) -> tuple:
    """Natural order for display: family, side, numeric line, then direction.

    Numbers compare as numbers (``ah,h,-8`` before ``ah,h,-4`` before
    ``ah,h,2``), and ``for`` comes before ``against`` on the same selection.
    """
    tokens = split_bet_type(bet_type) or [""]
    parts = []
    for token in tokens[1:]:
        if _NUMBER.fullmatch(token):
            parts.append((0, float(token), ""))
        else:
            parts.append((1, 0.0, token))
    direction = {"for": 0.0, "against": 1.0}.get(tokens[0], 2.0)
    return (*parts, (-1, direction, tokens[0]))


def parse_event_id(event_id: str) -> tuple[str, int, int] | None:
    """Split ``"YYYY-MM-DD,<home_id>,<away_id>"`` into its parts.

    Returns None for the empty event_id used by outright and generic
    markets, and for any other shape. Never split an empty event_id.
    Treat event_id as an opaque key; the date part is informational.
    """
    parts = event_id.split(",") if event_id else []
    if len(parts) != 3 or not all(re.fullmatch(r"[0-9]+", p) for p in parts[1:]):
        return None
    return parts[0], int(parts[1]), int(parts[2])


# --------------------------------------------------------------------------
# Store: in-memory mirror
# --------------------------------------------------------------------------


class Store:
    """In-memory mirror of the feed.

    ``events`` is keyed by ``(sport, event_id)`` and ``sptmkt`` by
    ``(sport, event_id, bet_type)``. An upsert carries the complete value,
    so it replaces the stored one. Records with an unknown collection, an
    unknown op or a malformed shape are counted in ``skipped`` and ignored.
    An index by ``(sport, event_id)`` keeps ``prices_for`` fast.
    Not thread-safe: use it from one task, or add a lock.
    """

    def __init__(self) -> None:
        self.reset()

    def reset(self) -> None:
        """Forget everything. Call this on reconnect: the server replays the full snapshot."""
        self.events: dict[tuple, dict] = {}
        self.sptmkt: dict[tuple, dict] = {}
        self._prices: dict[tuple, dict[str, dict]] = {}  # (sport, event_id) -> {bet_type: value}
        self.last_ts: float = 0.0
        self.applied: int = 0
        self.skipped: int = 0

    def apply_frame(self, frame: Any) -> int:
        """Apply every record in a frame. Returns the number of records applied."""
        if not isinstance(frame, dict):
            self.skipped += 1
            return 0
        ts = frame.get("ts")
        if isinstance(ts, (int, float)) and not isinstance(ts, bool):
            self.last_ts = float(ts)
        records = frame.get("data")
        if not isinstance(records, list):
            return 0
        count = sum(1 for record in records if self.apply_record(record))
        self.applied += count
        return count

    def apply_record(self, record: Any) -> bool:
        """Apply one ``[op, collection, key, value?]`` record. Returns True if applied."""
        if not isinstance(record, list) or len(record) < 3:
            self.skipped += 1
            return False
        op, collection, key = record[0], record[1], record[2]
        if collection == "events":
            bucket = self.events
        elif collection == "sptmkt":
            bucket = self.sptmkt
        else:
            bucket = None  # unknown collection: ignore, do not fail
        if bucket is None or not isinstance(key, list):
            self.skipped += 1
            return False
        try:
            k = tuple(key)
            if op == "upsert" and len(record) > 3 and isinstance(record[3], dict):
                bucket[k] = record[3]
                if bucket is self.sptmkt and len(k) == 3:
                    self._prices.setdefault(k[:2], {})[k[2]] = record[3]
            elif op == "delete":
                bucket.pop(k, None)
                if bucket is self.sptmkt and len(k) == 3:
                    prices = self._prices.get(k[:2])
                    if prices is not None:
                        prices.pop(k[2], None)
                        if not prices:
                            del self._prices[k[:2]]
            else:
                self.skipped += 1
                return False
        except TypeError:  # key holds something unhashable
            self.skipped += 1
            return False
        return True

    def in_running(self) -> list[tuple]:
        """Keys of events that are in-running now."""
        return [k for k, v in self.events.items() if v.get("ir")]

    def prices_for(self, sport: str, event_id: str) -> list[tuple[str, float]]:
        """``(bet_type, price)`` pairs for one event, sorted by bet_type."""
        prices = self._prices.get((sport, event_id), {})
        return sorted((bet_type, value.get("price")) for bet_type, value in prices.items())


# --------------------------------------------------------------------------
# Snapshot completion
# --------------------------------------------------------------------------


class SnapshotTracker:
    """Decides when the initial snapshot replay is complete.

    There is no "snapshot complete" message, and two obvious signals fail on
    the live feed: frame ``ts`` stays close to the local clock during the
    replay, and the steady flow after the replay rarely pauses. What works is
    key novelty: during the replay almost every upsert introduces a key not
    seen before; afterwards almost none do.

    The snapshot counts as complete once at least one ``sptmkt`` upsert has
    been seen AND either:

    (a) among the upserts of the most recent ``window`` seconds, the share
        that introduced a new key is below ``new_key_ratio``, or
    (b) no frame has arrived for ``quiet_gap`` seconds (a silent feed).

    ``timed_out()`` turns true when ``max_wait`` seconds have passed since
    the first call without completion; the caller decides what to do then.
    Once complete, ``observe`` does nothing until ``reset()``, so the set of
    seen keys stops growing.

    ``now`` defaults to ``time.monotonic()``. Pass your own values in tests,
    and use the same clock for every call.
    """

    def __init__(
        self,
        quiet_gap: float = 2.0,
        window: float = 1.0,
        new_key_ratio: float = 0.5,
        max_wait: float = 30.0,
    ) -> None:
        self.quiet_gap = quiet_gap
        self.window = window
        self.new_key_ratio = new_key_ratio
        self.max_wait = max_wait
        self.started_at: float | None = None
        self.reset()

    def reset(self) -> None:
        """Start over after a reconnect. The ``max_wait`` clock keeps running."""
        self.seen: set[tuple] = set()  # (collection, *key) of every upsert so far
        self.sptmkt_seen = False
        self.last_frame_at: float | None = None
        self.recent: deque[tuple[float, int, int]] = deque()  # (time, upserts, new keys)
        self.recent_upserts = 0
        self.recent_new = 0
        self.reason: str | None = None  # "new keys" or "quiet gap" once complete

    def _now(self, now: float | None) -> float:
        now = time.monotonic() if now is None else now
        if self.started_at is None:
            self.started_at = now
        return now

    def observe(self, frame: Any, now: float | None = None) -> None:
        now = self._now(now)
        if self.reason is not None:
            return  # complete: stop tracking keys until reset()
        self.last_frame_at = now
        records = frame.get("data") if isinstance(frame, dict) else None
        upserts = new = 0
        for record in records if isinstance(records, list) else []:
            if not (isinstance(record, list) and len(record) > 3 and record[0] == "upsert"):
                continue
            if record[1] not in COLLECTIONS or not isinstance(record[2], list):
                continue
            try:
                key = (record[1], *record[2])
                is_new = key not in self.seen
            except TypeError:  # unhashable key parts
                continue
            upserts += 1
            if is_new:
                self.seen.add(key)
                new += 1
            if record[1] == "sptmkt":
                self.sptmkt_seen = True
        if not upserts:
            return

        # Keep running totals for the upserts inside the window.
        self.recent.append((now, upserts, new))
        self.recent_upserts += upserts
        self.recent_new += new
        while self.recent[0][0] < now - self.window:
            _, old_upserts, old_new = self.recent.popleft()
            self.recent_upserts -= old_upserts
            self.recent_new -= old_new
        mostly_known = self.recent_new < self.new_key_ratio * self.recent_upserts
        if self.sptmkt_seen and mostly_known:
            self.reason = "new keys"

    def is_complete(self, now: float | None = None) -> bool:
        now = self._now(now)
        if (
            self.reason is None
            and self.sptmkt_seen
            and self.last_frame_at is not None
            and now - self.last_frame_at >= self.quiet_gap
        ):
            self.reason = "quiet gap"
        return self.reason is not None

    def timed_out(self, now: float | None = None) -> bool:
        now = self._now(now)
        return not self.is_complete(now) and now - self.started_at >= self.max_wait


# --------------------------------------------------------------------------
# Streaming with reconnect
# --------------------------------------------------------------------------

# Yielded by stream_frames after a reconnect. The server is about to replay
# the full snapshot, so reset your Store and SnapshotTracker when you see it.
RECONNECTED = object()


def backoff_delay(
    attempt: int,
    initial: float = 1.0,
    maximum: float = 60.0,
    rng: Callable[[], float] = random.random,
) -> float:
    """Delay before reconnect attempt ``attempt`` (0-based).

    Capped exponential backoff with "equal jitter": the result lies between
    half the capped delay and the capped delay, so clients spread out
    without ever reconnecting in a tight loop.
    """
    capped = min(maximum, initial * 2 ** min(attempt, 32))
    return capped / 2 + rng() * capped / 2


def _is_config_error(exc: BaseException) -> bool:
    """Errors that retrying cannot fix: bad URL, TLS verification, 4xx on upgrade."""
    if isinstance(exc, (InvalidURI, ssl.SSLCertVerificationError)):
        return True
    if isinstance(exc, InvalidStatus):
        status = exc.response.status_code
        return 400 <= status < 500 and status not in (408, 429)
    return False


async def stream_frames(
    token: str,
    *,
    url: str | None = None,
    reconnect: bool = True,
    backoff_initial: float = 1,
    backoff_max: float = 60,
    stable_after: float = 30,
    ping_interval: float | None = 30,
    ping_timeout: float | None = 10,
    open_timeout: float | None = 10,
    close_timeout: float | None = 2,
    sleep: Callable[[float], Any] = asyncio.sleep,
) -> AsyncIterator[Any]:
    """Yield parsed frames forever, reconnecting on any disconnect.

    ``url`` is the stream endpoint without the token (default: ``MM_DATA_URL``
    or ``DEFAULT_URL``). After a reconnect the generator yields
    ``RECONNECTED`` before the new snapshot replay. The simple response is
    to reset your Store. A production client can instead fill a new Store
    and swap it in when its SnapshotTracker completes, so reads never see a
    half-empty mirror.

    Retries: network errors, HTTP 502 and dropped connections are retried
    with capped exponential backoff. The backoff restarts only after a
    connection has stayed up for ``stable_after`` seconds. Configuration
    errors (bad URL, TLS verification failure, other 4xx answers) raise
    ``ValueError`` at once, with the token redacted. After three 502 answers
    in a row it logs a hint to check the token.

    With ``reconnect=False`` a failed connect raises, and a dropped
    connection ends the generator (or raises if it closed with an error).

    The client sends its own pings (the server sends none), so a wedged
    connection is detected within about ``ping_interval + ping_timeout +
    close_timeout`` seconds. ``sleep`` is injectable for tests.
    """
    target = stream_url(token, url)
    attempt = 0
    bad_gateways = 0
    connected_before = False
    while True:
        log.info("connecting to %s", redact(target, token))
        opened_at: float | None = None
        try:
            async with connect(
                target,
                max_size=MAX_SIZE,
                ping_interval=ping_interval,
                ping_timeout=ping_timeout,
                open_timeout=open_timeout,
                close_timeout=close_timeout,
                logger=_ws_log,
            ) as ws:
                opened_at = time.monotonic()
                bad_gateways = 0
                log.info("connected")
                if connected_before:
                    yield RECONNECTED
                connected_before = True
                async for raw in ws:
                    try:
                        frame = json.loads(raw)
                    except ValueError:
                        log.warning("skipping a frame that is not valid JSON")
                        continue
                    yield frame
            reason = "server closed the connection"
        except (OSError, asyncio.TimeoutError, WebSocketException) as exc:
            if _is_config_error(exc):
                raise ValueError(redact(f"cannot connect: {type(exc).__name__}: {exc}", token)) from None
            if not reconnect:
                if isinstance(exc, ConnectionClosedOK):
                    return
                raise
            reason = f"{type(exc).__name__}: {exc}"
            if isinstance(exc, InvalidStatus) and exc.response.status_code == 502:
                bad_gateways += 1
                if bad_gateways == 3:
                    log.warning(
                        "three HTTP 502 answers in a row: the token may be invalid or revoked, "
                        "or the feed may be unavailable. Check the token with GET /v1/config."
                    )
        if not reconnect:
            return
        if opened_at is not None and time.monotonic() - opened_at >= stable_after:
            attempt = 0  # the connection was stable, so start the backoff again
        delay = backoff_delay(attempt, backoff_initial, backoff_max)
        attempt += 1
        log.warning("%s; reconnecting in %.1f s", redact(reason, token), delay)
        await sleep(delay)


def configure_logging() -> None:
    """Log mmfeed messages to stderr, so stdout stays clean for data."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def run(main: Any) -> None:
    """Run an async main and exit with its return value if it is an int.

    Ctrl-C exits with status 130 and no traceback. A configuration error
    (for example a bad ``MM_DATA_URL``) prints one line and exits with 2.
    """
    try:
        result = asyncio.run(main)
    except KeyboardInterrupt:
        sys.exit(130)
    except ValueError as exc:
        print(f"error: {exc}", file=sys.stderr)
        sys.exit(2)
    except BrokenPipeError:  # stdout was closed early, e.g. piped into `head`
        os.dup2(os.open(os.devnull, os.O_WRONLY), sys.stdout.fileno())
        sys.exit(0)
    if isinstance(result, int):
        sys.exit(result)

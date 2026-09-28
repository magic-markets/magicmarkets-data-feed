"""Integration tests against a local mock feed server.

The example scripts run as real subprocesses with MM_DATA_URL pointing at
the mock server. The stream_frames tests run in-process with short timing
constants so nothing waits for real backoff or keepalive intervals.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import re
import subprocess
import sys
import time
from urllib.parse import quote

import pytest
from websockets.exceptions import InvalidStatus

import mmfeed
from conftest import EXAMPLES, FAKE_TOKEN, LiveProcess, example_env, run_example, send_all, session_script

ENCODED_TOKEN = quote(FAKE_TOKEN, safe="")


def assert_token_hidden(*outputs: str) -> None:
    for text in outputs:
        assert FAKE_TOKEN not in text
        assert ENCODED_TOKEN not in text


def client_log(caplog) -> str:
    """Log text from the client side only (the mock server logs raw request lines)."""
    return "\n".join(r.getMessage() for r in caplog.records if r.name.startswith("mmfeed"))


def assert_token_sent_once(mock_feed) -> None:
    assert mock_feed.paths, "no connection reached the server"
    for path, tokens in zip(mock_feed.paths, mock_feed.tokens_seen(), strict=True):
        assert tokens == [FAKE_TOKEN]
        assert path.count("token=") == 1


GHOST_KEY = ["fb", "2026-05-01,555,556"]
GHOST_FRAME = {
    "ts": 1778476931.9,
    "data": [
        ["upsert", "events", GHOST_KEY, {"home": "Gone FC", "away": "Old Town", "ir": False}],
        ["upsert", "sptmkt", [*GHOST_KEY, "for,h"], {"price": 2.5}],
    ],
}


def ghost_then_full(session) -> list[list[tuple]]:
    """First connection: the events phase and a row the second replay does not have, then a drop."""
    return [
        [*send_all(session["events_phase"]), ("send_live", GHOST_FRAME), ("close",)],
        session_script(session),
    ]


def run_without_token(*args: str, url: str):
    env = example_env(url)
    del env["MM_DATA_TOKEN"]
    return subprocess.run(
        [sys.executable, str(EXAMPLES / "find_event.py"), *args],
        env=env,
        capture_output=True,
        text=True,
        timeout=10,
    )


# --------------------------------------------------------------------------
# listen.py
# --------------------------------------------------------------------------


def test_listen_prints_every_record(mock_feed, session):
    mock_feed.scripts = [session_script(session)]
    expected = session["expected"]["final"]["printed_lines"]
    proc = LiveProcess("listen.py", url=mock_feed.url)
    try:
        lines = proc.read_until(lambda out: len(out) >= expected + 3)  # plus some steady flow
        deadline = time.monotonic() + 5
        while "snapshot complete" not in proc.stderr and time.monotonic() < deadline:
            time.sleep(0.05)
    finally:
        status = proc.interrupt()
    assert status == 130
    assert "Traceback" not in proc.stderr
    steady = lines[expected:]
    lines = lines[:expected]  # the replay and the deltas, in order

    assert lines[0].startswith('upsert events ["fb","2026-05-11,969,1738"] {"competition_id":7,')
    assert 'upsert sptmkt ["fb","2026-05-11,19,42","for,ah,h,-4"] {"price":1.91}' in lines
    assert 'upsert sptmkt ["fb","","against,win,374"] {"price":1.012}' in lines
    assert 'delete sptmkt ["fb","2026-05-11,19,42","against,h"]' in lines
    assert 'upsert widgets ["x"] {"a":1}' in lines  # printed as-is, no crash
    assert lines[-1] == 'delete sptmkt ["fb","2026-05-09,555,556","for,h"]'
    assert steady[0] == 'upsert sptmkt ["fb","2026-05-11,19,42","for,d"] {"price":3.35}'
    prop = next(line for line in lines if "proposition" in line and "Batter" in line)
    key = json.loads(prop.split(" ", 2)[2].rsplit(" ", 1)[0])
    assert key[2] == 'for,proposition,["Player Hits, Over","1.5","Props \\"Batter\\""]'

    assert re.search(r"snapshot complete \(new keys\) [\d.]+ s after connect", proc.stderr)
    assert_token_hidden("\n".join(lines), proc.stderr)
    assert "token=***" in proc.stderr
    assert_token_sent_once(mock_feed)


def test_listen_skips_frames_that_are_not_objects(mock_feed, session):
    mock_feed.scripts = [
        [("raw", "[1, 2]"), ("raw", '"text"'), ("raw", "null"), *send_all(session["events_phase"])]
    ]
    proc = LiveProcess("listen.py", url=mock_feed.url)
    try:
        lines = proc.read_until(lambda out: len(out) >= 7)
    finally:
        status = proc.interrupt()
    assert status == 130
    assert "Traceback" not in proc.stderr
    assert all(line.startswith("upsert events ") for line in lines)


# --------------------------------------------------------------------------
# store.py
# --------------------------------------------------------------------------

STATUS = re.compile(
    r"^t=\s*\d+\.\ds events=\s*(\d+) sptmkt=\s*(\d+) applied=\s*(\d+) "
    r"in_running=\s*(\d+) feed_lag=(\S+) snapshot=(complete|loading|timeout)$"
)


def test_store_status_line_and_reset_on_reconnect(mock_feed, session):
    mock_feed.scripts = ghost_then_full(session)
    final = session["expected"]["final"]
    want = (final["events"], final["sptmkt"], final["in_running"], "complete")

    def done(out):
        for line in out:
            m = STATUS.match(line)
            if m and (int(m[1]), int(m[2]), int(m[4]), m[6]) == want:
                return True
        return False

    proc = LiveProcess("store.py", url=mock_feed.url)
    try:
        lines = proc.read_until(done, timeout=10)  # never matches if the ghost rows survive
    finally:
        status = proc.interrupt()
    assert status == 130
    assert mock_feed.connections == 2
    assert "Traceback" not in proc.stderr
    for line in lines:
        assert STATUS.match(line), line
    last = STATUS.match(lines[-1])
    assert int(last[3]) >= final["applied"]  # the steady flow keeps adding updates
    assert re.fullmatch(r"[+-]\d+\.\d\ds", last[5])
    assert_token_hidden("\n".join(lines), proc.stderr)


def test_store_status_shows_timeout(mock_feed, session):
    mock_feed.scripts = [send_all(session["events_phase"])]  # no sptmkt ever
    import store

    tracker = mmfeed.SnapshotTracker(max_wait=0.2)
    s = mmfeed.Store()
    for f in session["events_phase"]:
        s.apply_frame(f)
        tracker.observe(f)
    assert store.snapshot_state(tracker) == "loading"
    time.sleep(0.25)
    assert store.snapshot_state(tracker) == "timeout"
    assert STATUS.match(store.status_line(s, tracker, time.time()))


# --------------------------------------------------------------------------
# find_event.py
# --------------------------------------------------------------------------


def test_find_event_prints_a_fixture_with_side_aware_lines(mock_feed, session):
    mock_feed.scripts = [session_script(session)]
    result = run_example("find_event.py", "arsenal", url=mock_feed.url)
    assert result.returncode == 0, result.stderr
    out = result.stdout

    # one fixture; the sport codes with prices are listed under it, the others on one line
    assert out.count("Arsenal vs Chelsea") == 1
    assert "event_id=2026-05-11,19,42  competition=England Premier League  start=2026-05-11T19:00:00Z" in out
    assert "in_running=no  score=n/a  ir_time=null" in out
    assert out.index("  sport=fb\n") < out.index("  sport=fb_ht\n")
    assert "  no prices: fb_corn\n" in out
    # the deltas arrived before the snapshot was declared complete
    assert re.search(r"^    for,h\s+2\.05$", out, re.M)

    assert re.search(r"^    for,ah,h,-4\s+1\.91  line=home -1\.0$", out, re.M)
    assert re.search(r"^    against,ah,h,-4\s+2\.05  line=home -1\.0$", out, re.M)
    assert re.search(r"^    for,ah,a,-4\s+2\.02  line=away \+1\.0$", out, re.M)
    assert re.search(r"^    for,ahover,10\s+1\.95  line=over 2\.5$", out, re.M)
    assert re.search(r"^    for,ah,h,-2\s+2\.05  line=home -0\.5$", out, re.M)  # fb_ht
    # decimal lines, correct scores and unknown families are not decoded
    assert re.search(r"^    for,over,2\.5\s+1\.87$", out, re.M)
    assert re.search(r"^    for,cs,2,1\s+8\.5$", out, re.M)
    assert re.search(r"^    for,newfamily,1,2\s+1\.5$", out, re.M)
    # natural order: family, side, numeric line, then direction
    rows = [line.split()[0] for line in out.splitlines() if line.startswith("    ")]
    assert rows[:6] == [
        "for,a",
        "for,ah,a,-4",
        "for,ah,h,-4",
        "against,ah,h,-4",
        "for,ahover,10",
        "for,ahunder,10",
    ]
    # no outright section, and no other fixture's prices
    assert "win," not in out and "outright" not in out
    assert "for,ah,a,-1" not in out

    assert "(new keys): events=6 sptmkt=24" in result.stderr  # no quiet gap in a steady flow
    assert re.search(
        r"timing: first frame [\d.]+ s, first sptmkt upsert [\d.]+ s, gate [\d.]+ s", result.stderr
    )
    assert_token_hidden(out, result.stderr)
    assert_token_sent_once(mock_feed)


def test_find_event_orders_in_running_first_and_joins_the_query(mock_feed, session):
    mock_feed.scripts = [session_script(session)]
    result = run_example("find_event.py", "e", url=mock_feed.url)
    assert result.returncode == 0, result.stderr
    titles = [line for line in result.stdout.splitlines() if " vs " in line]
    # Magdeburg is in-running; the rest started in the past, most recent first
    assert titles == [
        "1. FC Magdeburg vs Hertha BSC",
        "Boston Celtics vs New York Knicks",
        "Arsenal vs Chelsea",
        "New York Yankees vs Boston Red Sox",
    ]
    joined = run_example("find_event.py", "New", "York", url=mock_feed.url)
    assert joined.returncode == 0, joined.stderr
    assert [line for line in joined.stdout.splitlines() if " vs " in line] == [
        "Boston Celtics vs New York Knicks",
        "New York Yankees vs Boston Red Sox",
    ]


def test_find_event_resets_on_reconnect(mock_feed, session):
    mock_feed.scripts = ghost_then_full(session)
    result = run_example("find_event.py", "Gone", url=mock_feed.url)
    assert mock_feed.connections == 2
    assert result.returncode == 1, result.stdout  # the ghost fixture was dropped with the old store
    assert "no events match 'Gone'" in result.stderr


def test_find_event_completes_on_quiet_gap_when_feed_goes_silent(mock_feed, session):
    mock_feed.scripts = [session_script(session, deltas=False, steady=False)]
    started = time.monotonic()
    result = run_example("find_event.py", "celtics", url=mock_feed.url)
    elapsed = time.monotonic() - started
    assert result.returncode == 0, result.stderr
    assert "Boston Celtics vs New York Knicks" in result.stdout
    assert re.search(r"for,ahover,644\s+1\.9  line=over 161\.0$", result.stdout, re.M)
    assert "(quiet gap)" in result.stderr
    assert elapsed >= 2.0  # waited for the 2 s quiet gap


def test_find_event_limit_flag(mock_feed, session):
    mock_feed.scripts = [session_script(session)]
    result = run_example("find_event.py", "Arsenal", "--limit", "3", url=mock_feed.url)
    assert result.returncode == 0, result.stderr
    assert "and 9 more (use --limit 0 to show all)" in result.stdout  # 12 fb prices after deltas


@pytest.mark.parametrize(
    "args, message",
    [
        (["--timeout", "0", "x"], "--timeout must be greater than 0"),
        (["--limit", "-1", "x"], "--limit must be 0 or more"),
        ([], "the following arguments are required: query"),
    ],
)
def test_find_event_argument_errors(mock_feed, args, message):
    result = run_example("find_event.py", *args, url=mock_feed.url)
    assert result.returncode == 2
    assert message in result.stderr
    assert mock_feed.paths == []


def test_find_event_help(mock_feed):
    result = run_example("find_event.py", "--help", url=mock_feed.url)
    assert result.returncode == 0
    assert "MM_DATA_TOKEN" in result.stdout and "--timeout SECONDS" in result.stdout


def test_find_event_proposition_prices_print_intact(mock_feed, session):
    mock_feed.scripts = [session_script(session)]
    result = run_example("find_event.py", "yankees", url=mock_feed.url)
    assert result.returncode == 0, result.stderr
    assert 'for,proposition,["Exact Total Runs 1st Half","0","Game Props - 1st Half"]' in result.stdout
    assert "line=" not in result.stdout


def test_find_event_no_match(mock_feed, session):
    mock_feed.scripts = [session_script(session)]
    result = run_example("find_event.py", "Nobody", "United", url=mock_feed.url)
    assert result.returncode == 1
    assert "no events match 'Nobody United'" in result.stderr


def test_find_event_warns_and_continues_when_the_gate_times_out(mock_feed, session):
    # events phase only, then silence: no sptmkt, so the snapshot never completes
    mock_feed.scripts = [send_all(session["events_phase"])]
    result = run_example("find_event.py", "arsenal", "--timeout", "1", url=mock_feed.url)
    assert result.returncode == 0, result.stderr
    assert "warning: snapshot not confirmed complete after 1 s (events=7 sptmkt=0)" in result.stderr
    assert result.stdout.count("Arsenal vs Chelsea") == 1
    assert "  no prices: fb, fb_corn, fb_ht" in result.stdout


def test_find_event_no_data_at_all(mock_feed):
    mock_feed.scripts = [[]]  # connects, then silence
    result = run_example("find_event.py", "arsenal", "--timeout", "1", url=mock_feed.url)
    assert result.returncode == 3
    assert "error: no data received within 1 s" in result.stderr
    assert result.stdout == ""


def test_find_event_needs_the_token_in_the_environment(mock_feed):
    result = run_without_token("arsenal", url=mock_feed.url)
    assert result.returncode == 1
    assert "error: set MM_DATA_TOKEN (see README)" in result.stderr
    assert mock_feed.paths == []


def test_find_event_bad_url_fails_fast_without_leaking_the_token(mock_feed):
    result = run_example("find_event.py", "arsenal", url=f"http://127.0.0.1:{mock_feed.port}/v1/stream")
    assert result.returncode == 2
    assert "error: stream URL must start with ws:// or wss://" in result.stderr
    assert_token_hidden(result.stdout, result.stderr)
    assert mock_feed.paths == []


# --------------------------------------------------------------------------
# stream_frames: reconnect, reset, keepalive, errors
# --------------------------------------------------------------------------

FAST = dict(backoff_initial=0.01, backoff_max=0.05, ping_interval=None, open_timeout=2)


async def collect(token, store, *, stop, timeout=10, **kwargs):
    """Feed frames into ``store`` until ``stop(store, reconnects)``; return the reconnect count."""
    reconnects = 0

    async def run():
        nonlocal reconnects
        async with contextlib.aclosing(mmfeed.stream_frames(token, **kwargs)) as frames:
            async for frame in frames:
                if frame is mmfeed.RECONNECTED:
                    reconnects += 1
                    store.reset()
                    continue
                store.apply_frame(frame)
                if stop(store, reconnects):
                    return

    await asyncio.wait_for(run(), timeout)
    return reconnects


async def test_reconnect_resets_store_and_redacts_logs(mock_feed, session, caplog):
    caplog.set_level(logging.DEBUG)  # includes websockets debug output
    final = session["expected"]["final"]
    mock_feed.scripts = [
        [*send_all(session["events_phase"]), ("send", GHOST_FRAME), ("close",)],
        session_script(session),
    ]
    store = mmfeed.Store()
    reconnects = await collect(
        FAKE_TOKEN,
        store,
        url=mock_feed.url,
        stop=lambda s, r: r >= 1 and s.applied == final["applied"],
        **FAST,
    )
    assert reconnects == 1
    assert mock_feed.connections == 2
    assert len(store.events) == final["events"]
    assert len(store.sptmkt) == final["sptmkt"]
    assert tuple(GHOST_KEY) not in store.events  # stale row from the first connection is gone
    assert (*GHOST_KEY, "for,h") not in store.sptmkt

    assert_token_sent_once(mock_feed)
    log = client_log(caplog)
    assert "GET /v1/stream?token=*** HTTP/1.1" in log  # websockets debug line, redacted
    assert_token_hidden(log)
    assert "reconnecting in" in log


async def test_reconnect_after_http_502(mock_feed, session, caplog):
    caplog.set_level(logging.INFO)
    mock_feed.reject_first = 4
    mock_feed.scripts = [session_script(session)]
    store = mmfeed.Store()
    reconnects = await collect(
        FAKE_TOKEN,
        store,
        url=mock_feed.url,
        stop=lambda s, r: s.applied == session["expected"]["final"]["applied"],
        **FAST,
    )
    assert reconnects == 0  # nothing was yielded before, so nothing to reset
    assert len(mock_feed.paths) == 5
    assert "HTTP 502" in caplog.text
    assert client_log(caplog).count("Check the token with GET /v1/config") == 1  # after the third 502
    assert_token_hidden(client_log(caplog))


async def test_config_errors_fail_fast_even_with_reconnect(mock_feed):
    mock_feed.reject_first = 1
    mock_feed.reject_status = 404
    mock_feed.scripts = [[("close",)]]  # a retry would connect, then keep reconnecting

    async def drain():
        async for _ in mmfeed.stream_frames(FAKE_TOKEN, url=mock_feed.url, **FAST):
            pass

    with pytest.raises(ValueError) as exc:
        await asyncio.wait_for(drain(), 5)
    assert "404" in str(exc.value)
    assert len(mock_feed.paths) == 1  # no retry
    assert_token_hidden(str(exc.value))


@pytest.mark.parametrize("reconnect", [True, False])
async def test_bad_url_raises_redacted_value_error(reconnect):
    url = "http://127.0.0.1:1/v1/stream?token=" + ENCODED_TOKEN

    async def drain():
        async for _ in mmfeed.stream_frames(FAKE_TOKEN, url=url, reconnect=reconnect, **FAST):
            pass

    with pytest.raises(ValueError) as exc:
        await asyncio.wait_for(drain(), 5)
    assert "ws:// or wss://" in str(exc.value)
    assert_token_hidden(str(exc.value), repr(exc.value))


async def test_exception_text_with_the_raw_token_is_redacted(mock_feed, session, caplog, monkeypatch):
    caplog.set_level(logging.INFO)
    real_connect = mmfeed.connect
    calls = []

    def flaky_connect(*args, **kwargs):
        calls.append(1)
        if len(calls) == 1:
            raise OSError(f"connection failed for {FAKE_TOKEN}")
        return real_connect(*args, **kwargs)

    monkeypatch.setattr(mmfeed, "connect", flaky_connect)
    mock_feed.scripts = [send_all(session["events_phase"])]
    store = mmfeed.Store()
    await collect(FAKE_TOKEN, store, url=mock_feed.url, stop=lambda s, r: len(s.events) == 7, **FAST)
    log = client_log(caplog)
    assert "connection failed for ***" in log
    assert_token_hidden(log)


async def test_backoff_restarts_only_after_a_stable_connection(mock_feed, session):
    """502, 502, a short-lived connection, 502, then data: the backoff keeps growing."""

    async def run(stable_after: float) -> list[float]:
        feed_paths_before = len(mock_feed.paths)
        mock_feed.reject = {feed_paths_before + n for n in (0, 1, 3)}
        mock_feed.connections = 0
        mock_feed.scripts = [
            [*send_all(session["events_phase"]), ("close",)],
            send_all(session["events_phase"]),
        ]
        delays: list[float] = []

        async def fake_sleep(seconds: float) -> None:
            delays.append(seconds)

        store = mmfeed.Store()
        await collect(
            FAKE_TOKEN,
            store,
            url=mock_feed.url,
            stop=lambda s, r: r >= 1 and len(s.events) == 7,
            ping_interval=None,
            stable_after=stable_after,
            sleep=fake_sleep,
        )
        return delays

    def attempts(delays: list[float]) -> list[int]:
        # attempt n sleeps between 2**n / 2 and 2**n seconds (1 s initial, no overlap)
        return [next(n for n in range(8) if 2**n / 2 <= d <= 2**n) for d in delays]

    assert attempts(await run(stable_after=30)) == [0, 1, 2, 3]
    assert attempts(await run(stable_after=0)) == [0, 1, 0, 1]


async def test_no_reconnect_raises_on_502(mock_feed):
    mock_feed.reject_first = 1
    with pytest.raises(InvalidStatus) as exc:
        async for _ in mmfeed.stream_frames(FAKE_TOKEN, url=mock_feed.url, reconnect=False):
            pass
    assert exc.value.response.status_code == 502
    assert_token_hidden(str(exc.value))


async def test_no_reconnect_ends_when_server_closes(mock_feed, session):
    mock_feed.scripts = [[*send_all(session["events_phase"]), ("close",)]]
    frames = [f async for f in mmfeed.stream_frames(FAKE_TOKEN, url=mock_feed.url, reconnect=False)]
    assert len(frames) == len(session["events_phase"])
    assert mock_feed.connections == 1


async def test_keepalive_detects_wedged_connection(mock_feed, session):
    # The first server stops reading (so it never answers pings) and sends nothing.
    mock_feed.scripts = [[*send_all(session["events_phase"]), ("wedge", 2)], session_script(session)]
    store = mmfeed.Store()
    started = time.monotonic()
    reconnects = await collect(
        FAKE_TOKEN,
        store,
        url=mock_feed.url,
        stop=lambda s, r: r >= 1 and len(s.sptmkt) > 0,
        backoff_initial=0.01,
        backoff_max=0.05,
        ping_interval=0.2,
        ping_timeout=0.2,
        close_timeout=0.1,
        timeout=5,
    )
    assert reconnects == 1
    assert time.monotonic() - started < 1.5  # detected by the ping, not by the server ending


async def test_invalid_json_frame_is_skipped(mock_feed, session, caplog):
    mock_feed.scripts = [[("raw", "{not json"), ("raw", "[1, 2"), *send_all(session["events_phase"])]]
    store = mmfeed.Store()
    await collect(FAKE_TOKEN, store, url=mock_feed.url, stop=lambda s, r: len(s.events) == 7, **FAST)
    assert caplog.text.count("not valid JSON") == 2
    assert mock_feed.connections == 1


async def test_large_snapshot_frame_is_accepted(mock_feed):
    # Observed frames are a few KB. This checks the defensive max_size=2**27 setting
    # (the websockets default of 1 MiB would reject this ~3 MB frame).
    records = [
        ["upsert", "sptmkt", ["fb", f"2026-05-10,{i},{i + 1}", f"for,ah,h,{i % 50 - 25}"], {"price": 1.91}]
        for i in range(40_000)
    ]
    mock_feed.scripts = [[("send", {"ts": 1778476936.5, "data": records})]]
    store = mmfeed.Store()
    await collect(FAKE_TOKEN, store, url=mock_feed.url, stop=lambda s, r: s.applied > 0, **FAST)
    assert len(store.sptmkt) == 40_000
    assert mock_feed.connections == 1


async def watch_tracker(mock_feed, tracker, store, timeout=5.0):
    """Stream into store and tracker; sample the state every 20 ms until complete."""
    samples: list[tuple[bool, int, int]] = []

    async def consume():
        async for frame in mmfeed.stream_frames(FAKE_TOKEN, url=mock_feed.url, **FAST):
            store.apply_frame(frame)
            tracker.observe(frame)

    task = asyncio.create_task(consume())
    try:
        deadline = time.monotonic() + timeout
        while not tracker.is_complete():
            assert time.monotonic() < deadline, "snapshot never completed"
            samples.append((tracker.is_complete(), len(store.events), len(store.sptmkt)))
            await asyncio.sleep(0.02)
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
    return samples


async def test_snapshot_tracker_completes_in_a_continuous_flow(mock_feed, session):
    """An events-only gap longer than quiet_gap must not complete; the steady flow then must."""
    mock_feed.scripts = [session_script(session, gap=0.8)]
    store = mmfeed.Store()
    tracker = mmfeed.SnapshotTracker(quiet_gap=0.3, window=0.2)
    samples = await watch_tracker(mock_feed, tracker, store)
    assert tracker.reason == "new keys"  # steady frames every 50 ms: never a quiet gap
    assert len(store.sptmkt) == session["expected"]["final"]["sptmkt"]
    events_only = [s for s in samples if s[1] == 7 and s[2] == 0]
    assert len(events_only) >= 10  # sat through the 0.8 s events-only gap
    assert not any(done for done, _, _ in samples)


async def test_snapshot_tracker_completes_when_the_feed_goes_silent(mock_feed, session):
    mock_feed.scripts = [session_script(session, deltas=False, steady=False)]
    store = mmfeed.Store()
    tracker = mmfeed.SnapshotTracker(quiet_gap=0.3, window=0.2)
    await watch_tracker(mock_feed, tracker, store)
    assert tracker.reason == "quiet gap"
    assert len(store.sptmkt) == session["expected"]["after_snapshot"]["sptmkt"]


async def test_backoff_schedule_in_the_reconnect_loop(mock_feed, session):
    """Real 1 s / 60 s defaults, with sleep() recorded instead of awaited."""
    mock_feed.reject_first = 4
    mock_feed.scripts = [send_all(session["events_phase"])]
    delays: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        delays.append(seconds)

    store = mmfeed.Store()
    await collect(
        FAKE_TOKEN,
        store,
        url=mock_feed.url,
        stop=lambda s, r: len(s.events) == 7,
        ping_interval=None,
        sleep=fake_sleep,
    )
    assert len(delays) == 4
    for attempt, delay in enumerate(delays):
        capped = min(60, 2**attempt)
        assert capped / 2 <= delay <= capped

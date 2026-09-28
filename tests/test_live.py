"""Opt-in smoke test against the real feed.

Skipped unless MM_DATA_TOKEN is set, and excluded from the default run.
Run it locally (about 10-40 s) with:

    MM_DATA_TOKEN=<token> pytest -m live

CI never runs it, because it needs a real token. The token is never printed.
"""

from __future__ import annotations

import asyncio
import json
import os
import time
import urllib.request
from urllib.parse import urlsplit

import pytest
from websockets.exceptions import InvalidStatus

import mmfeed

pytestmark = [
    pytest.mark.live,
    pytest.mark.skipif(not os.environ.get("MM_DATA_TOKEN"), reason="MM_DATA_TOKEN is not set"),
]

# The REST endpoints reject the default Python-urllib User-Agent with 403.
USER_AGENT = "magicmarkets-data-feed-examples/1.0"
EVENT_FIELDS = {
    "competition_id",
    "competition_name",
    "competition_country",
    "home",
    "away",
    "start_ts",
    "ir",
    "score",
    "ir_time",
}


def token() -> str:
    return os.environ["MM_DATA_TOKEN"].strip()


def rest_base() -> str:
    """https://<stream host>/v1/, derived from MM_DATA_URL or the default stream URL."""
    parts = urlsplit(os.environ.get("MM_DATA_URL") or mmfeed.DEFAULT_URL)
    scheme = "https" if parts.scheme == "wss" else "http"
    return f"{scheme}://{parts.netloc}/v1/"


def test_live_rest_config_with_user_agent():
    request = urllib.request.Request(
        rest_base() + "config",
        headers={"Authorization": f"Token {token()}", "User-Agent": USER_AGENT},
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            status, body = response.status, json.load(response)
    except OSError as exc:  # keep the token out of the failure message
        pytest.fail(mmfeed.redact(f"{type(exc).__name__}: {exc}", token()), pytrace=False)
    assert status == 200
    assert isinstance(body, dict)


async def test_live_bad_token_upgrade_returns_502():
    with pytest.raises(InvalidStatus) as exc:
        async for _ in mmfeed.stream_frames("not-a-valid-token", reconnect=False):
            pass
    assert exc.value.response.status_code == 502


async def test_live_stream_shape_order_and_snapshot_gate():
    store = mmfeed.Store()
    tracker = mmfeed.SnapshotTracker(max_wait=30.0)
    first_upsert: dict[str, int] = {}  # collection -> frame number of its first upsert
    frames = 0

    async def read():
        nonlocal frames
        async for frame in mmfeed.stream_frames(token(), reconnect=False):
            frames += 1
            assert set(frame) == {"ts", "data"}, "frames carry exactly ts and data"
            assert isinstance(frame["ts"], float)
            for record in frame["data"]:
                op, collection, key = record[0], record[1], record[2]
                assert op in ("upsert", "delete")
                assert collection in ("events", "sptmkt")
                assert len(key) == (2 if collection == "events" else 3)
                if op == "upsert":
                    first_upsert.setdefault(collection, frames)
                    fields = set(record[3])
                    assert fields == (EVENT_FIELDS if collection == "events" else {"price"})
            store.apply_frame(frame)
            tracker.observe(frame)
            if tracker.is_complete() or tracker.timed_out():
                return

    started = time.monotonic()
    try:
        await asyncio.wait_for(read(), timeout=40)
    except AssertionError:
        raise
    except Exception as exc:  # keep the token out of the failure message
        pytest.fail(mmfeed.redact(f"{type(exc).__name__}: {exc}", token()), pytrace=False)

    assert store.skipped == 0
    assert first_upsert["events"] < first_upsert["sptmkt"], "the events replay comes before sptmkt"
    assert tracker.is_complete(), f"snapshot gate did not complete in 30 s ({len(store.sptmkt)} sptmkt)"
    assert time.monotonic() - started < 30
    assert store.events and store.sptmkt

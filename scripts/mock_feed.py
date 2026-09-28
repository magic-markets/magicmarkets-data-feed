#!/usr/bin/env python3
"""A local mock of the MagicMarkets data feed, for trying the examples without credentials.

    python3 scripts/mock_feed.py                 serve on ws://127.0.0.1:8765/v1/stream
    python3 scripts/mock_feed.py --port 9000

Then, in another terminal:

    MM_DATA_URL=ws://127.0.0.1:8765/v1/stream MM_DATA_TOKEN=demo \\
        python3 examples/python/find_event.py arsenal

Every connection gets the sample session in tests/fixtures/feed_session.json:
the events replay, the sptmkt replay, some deltas, then a steady flow of
price updates without pauses, as the live feed sends. Any token is accepted.
Needs only the websockets package. The tests use the same MockFeed class.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import itertools
import json
import sys
import threading
import time
from http import HTTPStatus
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

try:
    from websockets.asyncio.server import serve
    from websockets.exceptions import ConnectionClosed
except ImportError:  # pragma: no cover - depends on the environment
    sys.exit('mock_feed needs the websockets package: pip install "websockets>=13"')

ROOT = Path(__file__).resolve().parent.parent
SESSION = ROOT / "tests" / "fixtures" / "feed_session.json"


def load_session() -> dict:
    return json.loads(SESSION.read_text(encoding="utf-8"))


# --------------------------------------------------------------------------
# Scripts: lists of steps, one script per connection
# --------------------------------------------------------------------------
# ("send", frame)             send the frame as-is
# ("send_live", frame)        send with ts set to the current time, as the live feed does
# ("steady", frames, s)       loop over frames every s seconds with a current ts, until the client leaves
# ("raw", text)               send a text message as-is
# ("sleep", seconds)
# ("close",)                  drop the connection (normal close)
# ("wedge", seconds)          stop reading (so no pong replies) for a while, then end
# A script that runs out of steps holds the connection open and silent.


def send_all(frames: list[dict], live: bool = True) -> list[tuple]:
    return [("send_live" if live else "send", f) for f in frames]


def session_script(fx: dict, *, gap: float = 0.05, deltas: bool = True, steady: bool = True) -> list[tuple]:
    """Events phase, internal gap, sptmkt phase, deltas, then a continuous steady flow.

    Every frame carries a current ts, as on the live feed (ts is the send
    time, even during the replay). With ``steady`` the feed never pauses
    after the replay, so SnapshotTracker must complete through its new-key
    rule. Without it the feed goes silent and the quiet-gap rule applies.
    """
    steps = send_all(fx["events_phase"])
    steps.append(("sleep", gap))
    steps += send_all(fx["sptmkt_phase"])
    if deltas:
        steps.append(("sleep", 0.05))
        steps += send_all(fx["deltas"])
    if steady:
        steps.append(("steady", fx["steady"], 0.05))
    return steps


class MockFeed:
    """WebSocket server that plays ``scripts[i]`` on connection ``i`` (the last one repeats).

    ``start()`` runs it in a background thread (tests); ``run_forever()``
    blocks (the command line). ``reject_first`` answers that many upgrades
    with ``reject_status`` (HTTP 502 by default); ``reject`` names more
    upgrade attempts (0-based) to answer that way. ``paths`` records every
    request path, query included.
    """

    def __init__(self, host: str = "127.0.0.1", port: int = 0) -> None:
        self.host = host
        self.port = port
        self.scripts: list[list[tuple]] = [[]]
        self.paths: list[str] = []
        self.reject_first = 0
        self.reject: set[int] = set()
        self.reject_status = HTTPStatus.BAD_GATEWAY
        self.connections = 0
        self._ready = threading.Event()

    @property
    def url(self) -> str:
        return f"ws://{self.host}:{self.port}/v1/stream"

    def tokens_seen(self) -> list[list[str]]:
        return [parse_qs(urlsplit(p).query).get("token", []) for p in self.paths]

    def _process_request(self, connection, request):
        attempt = len(self.paths)
        self.paths.append(request.path)
        if self.reject_first > 0 or attempt in self.reject:
            self.reject_first = max(0, self.reject_first - 1)
            status = HTTPStatus(self.reject_status)
            return connection.respond(status, f"{status.phrase}\n")
        return None

    async def _handler(self, ws) -> None:
        index = self.connections
        self.connections += 1
        script = self.scripts[min(index, len(self.scripts) - 1)]
        try:
            for step in script:
                kind = step[0]
                if kind == "send":
                    await ws.send(json.dumps(step[1]))
                elif kind == "send_live":
                    await ws.send(json.dumps(dict(step[1], ts=time.time())))
                elif kind == "steady":
                    frames, interval = step[1], step[2]
                    for i in itertools.count():
                        await ws.send(json.dumps(dict(frames[i % len(frames)], ts=time.time())))
                        await asyncio.sleep(interval)
                elif kind == "raw":
                    await ws.send(step[1])
                elif kind == "sleep":
                    await asyncio.sleep(step[1])
                elif kind == "close":
                    await ws.close()
                    return
                elif kind == "wedge":
                    ws.transport.pause_reading()
                    await asyncio.sleep(step[1])
                    return
            await ws.wait_closed()
        except ConnectionClosed:
            pass

    async def serve(self) -> None:
        """Serve until ``stop()`` is called."""
        self._stopping = asyncio.Event()
        self._loop = asyncio.get_running_loop()
        server = await serve(
            self._handler,
            self.host,
            self.port,
            process_request=self._process_request,
            max_size=None,
            close_timeout=0.5,
        )
        self.port = server.sockets[0].getsockname()[1]
        self._ready.set()
        await self._stopping.wait()
        server.close()
        await server.wait_closed()

    def start(self) -> MockFeed:
        self._thread = threading.Thread(target=lambda: asyncio.run(self.serve()), daemon=True)
        self._thread.start()
        if not self._ready.wait(5):
            raise RuntimeError("mock server did not start")
        return self

    def stop(self) -> None:
        self._loop.call_soon_threadsafe(self._stopping.set)
        self._thread.join(10)

    def run_forever(self) -> None:
        with contextlib.suppress(KeyboardInterrupt):
            asyncio.run(self.serve())


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Serve the sample feed session on a local port.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    args = parser.parse_args(argv)
    feed = MockFeed(args.host, args.port)
    feed.scripts = [session_script(load_session())]
    print(f"mock feed on {feed.url} (any token works; Ctrl-C to stop)", file=sys.stderr, flush=True)
    feed.run_forever()
    return 0


if __name__ == "__main__":
    sys.exit(main())

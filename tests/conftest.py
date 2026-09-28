"""Shared test helpers: fixture frames, the local mock feed server, and a process runner.

MockFeed lives in scripts/mock_feed.py (also used by `make mock`). It runs in
a background thread with its own event loop, so both synchronous subprocess
tests and async in-process tests can talk to it. Each connection plays one
"script": a list of steps such as sending a frame, sleeping, or dropping the
connection.
"""

from __future__ import annotations

import os
import queue
import signal
import subprocess
import sys
import threading
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
EXAMPLES = ROOT / "examples" / "python"
sys.path.insert(0, str(ROOT / "scripts"))

from mock_feed import MockFeed, load_session, send_all, session_script  # noqa: E402

__all__ = ["FAKE_TOKEN", "MockFeed", "load_session", "send_all", "session_script"]

# A placeholder token with characters that must be URL-encoded. No real tokens anywhere.
FAKE_TOKEN = "EXAMPLE-token/with+odd&chars=1 %20#?\u00e9"


@pytest.fixture
def session() -> dict:
    return load_session()


@pytest.fixture
def mock_feed():
    server = MockFeed().start()
    yield server
    server.stop()


# --------------------------------------------------------------------------
# Running the example scripts
# --------------------------------------------------------------------------


def example_env(url: str, token: str = FAKE_TOKEN) -> dict:
    env = {k: v for k, v in os.environ.items() if not k.startswith("MM_")}
    env.update(MM_DATA_URL=url, MM_DATA_TOKEN=token, PYTHONUNBUFFERED="1", PYTHONUTF8="1")
    return env


def run_example(script: str, *args: str, url: str, timeout: float = 15) -> subprocess.CompletedProcess:
    """Run an example that exits on its own (find_event.py)."""
    return subprocess.run(
        [sys.executable, str(EXAMPLES / script), *args],
        env=example_env(url),
        capture_output=True,
        text=True,
        timeout=timeout,
    )


# A shell that starts pytest in the background makes SIGINT ignored, and children
# inherit that. This wrapper restores Python's Ctrl-C handler, then runs the example
# as __main__ from its own folder, so Ctrl-C can be tested anywhere.
RESTORE_SIGINT = (
    "import os, runpy, signal, sys; "
    "signal.signal(signal.SIGINT, signal.default_int_handler); "
    "sys.argv = sys.argv[1:]; sys.path.insert(0, os.path.dirname(sys.argv[0])); "
    "runpy.run_path(sys.argv[0], run_name='__main__')"
)


class LiveProcess:
    """Run a long-lived example (listen.py, store.py) and read its stdout line by line."""

    def __init__(self, script: str, *args: str, url: str) -> None:
        self.proc = subprocess.Popen(
            [sys.executable, "-c", RESTORE_SIGINT, str(EXAMPLES / script), *args],
            env=example_env(url),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            start_new_session=True,
        )
        self.lines: queue.Queue[str] = queue.Queue()
        self._err: list[str] = []
        self._pumps = [
            threading.Thread(target=self._pump, args=(self.proc.stdout, self.lines.put), daemon=True),
            threading.Thread(target=self._pump, args=(self.proc.stderr, self._err.append), daemon=True),
        ]
        for t in self._pumps:
            t.start()
        self.stdout: list[str] = []

    @staticmethod
    def _pump(stream, sink) -> None:
        for line in stream:
            sink(line.rstrip("\n"))

    @property
    def stderr(self) -> str:
        return "\n".join(self._err)

    def read_until(self, predicate, timeout: float = 10) -> list[str]:
        """Collect stdout lines until ``predicate(lines)`` is true."""
        deadline = time.monotonic() + timeout
        while not predicate(self.stdout):
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise AssertionError(f"timed out; last stdout lines: {self.stdout[-5:]}")
            try:
                self.stdout.append(self.lines.get(timeout=remaining))
            except queue.Empty:
                continue
        return self.stdout

    def interrupt(self, timeout: float = 10) -> int:
        """Send Ctrl-C, wait for exit, and return the exit status."""
        self.proc.send_signal(signal.SIGINT)
        try:
            self.proc.wait(timeout=timeout)
        finally:
            self.kill()
        for t in self._pumps:
            t.join(5)
        while not self.lines.empty():
            self.stdout.append(self.lines.get())
        return self.proc.returncode

    def kill(self) -> None:
        if self.proc.poll() is None:
            self.proc.kill()
            self.proc.wait()

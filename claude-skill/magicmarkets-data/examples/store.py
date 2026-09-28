"""Keep a live in-memory mirror of the MagicMarkets data feed.

Usage:
    MM_DATA_TOKEN=<token> python3 store.py

Prints one status line per second on stdout:

    t=   8.0s events=  6100 sptmkt= 163000 applied=  169800 in_running= 41 feed_lag=+0.21s snapshot=complete

- ``feed_lag`` is local wall-clock minus the ``ts`` of the last frame.
- ``snapshot`` is ``loading`` until SnapshotTracker decides the replay is in,
  then ``complete``. It shows ``timeout`` if the gate has not passed after
  30 s.
- Counts vary by account and time of day; the line above is only an example.

On reconnect the Store is reset, because the server replays the full
snapshot. Stop it with Ctrl-C.
"""

import asyncio
import time

import mmfeed


def snapshot_state(tracker: mmfeed.SnapshotTracker) -> str:
    if tracker.is_complete():
        return "complete"
    return "timeout" if tracker.timed_out() else "loading"


def status_line(store: mmfeed.Store, tracker: mmfeed.SnapshotTracker, started: float) -> str:
    now = time.time()
    lag = f"{now - store.last_ts:+.2f}s" if store.last_ts else "n/a"
    return (
        f"t={now - started:6.1f}s "
        f"events={len(store.events):>6} "
        f"sptmkt={len(store.sptmkt):>7} "
        f"applied={store.applied:>8} "
        f"in_running={len(store.in_running()):>3} "
        f"feed_lag={lag} "
        f"snapshot={snapshot_state(tracker)}"
    )


async def report(store, tracker, started, interval=1.0):
    while True:
        await asyncio.sleep(interval)
        print(status_line(store, tracker, started), flush=True)


async def main() -> None:
    token = mmfeed.get_token()
    store = mmfeed.Store()
    tracker = mmfeed.SnapshotTracker()
    reporter = asyncio.create_task(report(store, tracker, time.time()))
    try:
        async for frame in mmfeed.stream_frames(token):
            if frame is mmfeed.RECONNECTED:
                store.reset()
                tracker.reset()
                continue
            store.apply_frame(frame)
            tracker.is_complete()  # lets a quiet gap before this frame count
            tracker.observe(frame)
    finally:
        reporter.cancel()


if __name__ == "__main__":
    mmfeed.configure_logging()
    mmfeed.run(main())

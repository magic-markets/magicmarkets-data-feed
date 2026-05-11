"""In-memory mirror of the MagicMarkets data feed.

Builds a `Store` keyed by `(collection, key_tuple)` and prints a one-line
summary every second so you can see the snapshot fill up and then the steady
flow of deltas.

Usage:
    python3 store.py <token>
"""
import asyncio
import json
import os
import sys
import time
import websockets


class Store:
    """Thread-unsafe in-memory mirror. Wrap with a lock if you read from
    another task."""

    def __init__(self) -> None:
        self.events: dict[tuple, dict] = {}
        self.sptmkt: dict[tuple, dict] = {}
        self.last_ts: float = 0.0
        self.applied: int = 0

    def apply_frame(self, frame: dict) -> None:
        self.last_ts = frame["ts"]
        for record in frame["data"]:
            op, collection, key, *rest = record
            key = tuple(key)
            bucket = self.events if collection == "events" else self.sptmkt
            if op == "upsert":
                bucket[key] = rest[0]
            elif op == "delete":
                bucket.pop(key, None)
            self.applied += 1

    def in_running(self) -> list[tuple]:
        return [k for k, v in self.events.items() if v.get("ir")]


async def main() -> None:
    token = sys.argv[1] if len(sys.argv) > 1 else os.environ.get("MM_DATA_TOKEN")
    if not token:
        sys.exit("usage: store.py <token>")

    url = f"wss://data.magicmarkets.com/v1/stream?token={token}"
    store = Store()
    started = time.time()
    last_report = 0.0

    async with websockets.connect(url, max_size=2**27) as ws:
        async for raw in ws:
            store.apply_frame(json.loads(raw))
            now = time.time()
            if now - last_report >= 1.0:
                lag = now - store.last_ts
                print(
                    f"t={now - started:6.1f}s "
                    f"events={len(store.events):>6} "
                    f"sptmkt={len(store.sptmkt):>7} "
                    f"applied={store.applied:>8} "
                    f"in_running={len(store.in_running()):>3} "
                    f"feed_lag={lag:+.2f}s"
                )
                last_report = now


if __name__ == "__main__":
    asyncio.run(main())

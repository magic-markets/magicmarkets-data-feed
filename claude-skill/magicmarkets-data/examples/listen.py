"""Print every record from the MagicMarkets data feed.

Usage:
    MM_DATA_TOKEN=<token> python3 listen.py

Each record prints as one line on stdout:

    upsert sptmkt ["fb","2026-05-09,969,1738","for,h"] {"price":1.91}
    delete events ["fb","2026-08-15,10050631,10037275"]

Connection messages go to stderr, with one line when the snapshot replay is
complete. The script reconnects on its own; after a reconnect the server
replays the full snapshot, so expect a burst of upserts. Stop it with Ctrl-C.
"""

import json
import sys
import time

import mmfeed


def compact(value: object) -> str:
    return json.dumps(value, separators=(",", ":"), ensure_ascii=False)


async def main() -> None:
    token = mmfeed.get_token()
    tracker = mmfeed.SnapshotTracker()
    connected_at = time.monotonic()
    announced = False
    async for frame in mmfeed.stream_frames(token):
        if frame is mmfeed.RECONNECTED:
            print("reconnected: full snapshot replay follows", file=sys.stderr)
            tracker.reset()
            connected_at, announced = time.monotonic(), False
            continue
        tracker.is_complete()  # lets a quiet gap before this frame count
        tracker.observe(frame)
        if not announced and tracker.is_complete():
            waited = time.monotonic() - connected_at
            print(f"snapshot complete ({tracker.reason}) {waited:.1f} s after connect", file=sys.stderr)
            announced = True
        records = frame.get("data") if isinstance(frame, dict) else None
        for record in records if isinstance(records, list) else []:
            if not isinstance(record, list) or len(record) < 3:
                continue  # malformed record: skip it
            op, collection, key, *rest = record
            line = f"{op} {collection} {compact(key)}"
            if rest:
                line += f" {compact(rest[0])}"
            print(line, flush=True)


if __name__ == "__main__":
    mmfeed.configure_logging()
    mmfeed.run(main())

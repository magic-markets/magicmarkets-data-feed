"""Find an event by team name and print the prices currently published for it.

Usage:
    python3 find_event.py <token> "<query>"
    python3 find_event.py "$MM_DATA_TOKEN" "Arsenal"

Strategy:
    1. Connect to the feed.
    2. Drain frames until the snapshot has clearly completed — we require
       BOTH at least one `sptmkt` upsert AND a quiet gap of ≥2 s. (The
       replay sends all events first, then all prices, with a multi-second
       gap between phases — a shorter quiet-gap threshold would fire mid-
       snapshot and miss every price.)
    3. Match `query` (case-insensitive substring) against `home` / `away`.
    4. For each matching event, print its priced markets sorted by bet_type.
"""
import asyncio
import json
import os
import sys
import time
import websockets


async def drain_snapshot(ws, store, quiet_gap: float = 2.0, max_seconds: float = 30.0) -> None:
    """Drain until the snapshot is in.

    The replay is two-phase (events first, then sptmkt) with internal gaps
    of up to several seconds. The safe completion gate is:
      ≥1 sptmkt upsert seen, AND a quiet window ≥quiet_gap seconds.
    """
    started = time.time()
    last_msg = time.time()
    while time.time() - started < max_seconds:
        try:
            raw = await asyncio.wait_for(ws.recv(), timeout=quiet_gap)
        except asyncio.TimeoutError:
            if store["sptmkt"] and time.time() - last_msg >= quiet_gap:
                return
            continue
        last_msg = time.time()
        frame = json.loads(raw)
        for op, collection, key, *rest in frame["data"]:
            key = tuple(key)
            bucket = store[collection]
            if op == "upsert":
                bucket[key] = rest[0]
            elif op == "delete":
                bucket.pop(key, None)
            store["applied"] += 1


async def main() -> None:
    if len(sys.argv) < 3:
        sys.exit('usage: find_event.py <token> "<team name fragment>"')
    token, query = sys.argv[1], sys.argv[2].lower()

    url = f"wss://data.magicmarkets.com/v1/stream?token={token}"
    store = {"events": {}, "sptmkt": {}, "applied": 0}

    async with websockets.connect(url, max_size=2**27) as ws:
        print("draining snapshot…", file=sys.stderr)
        await drain_snapshot(ws, store)
        print(
            f"  events={len(store['events'])} sptmkt={len(store['sptmkt'])}",
            file=sys.stderr,
        )

    matches = [
        (key, value)
        for key, value in store["events"].items()
        if query in value["home"].lower() or query in value["away"].lower()
    ]
    if not matches:
        sys.exit(f"no events match {query!r}")

    matches.sort(key=lambda kv: kv[1]["start_ts"])
    for (sport, event_id), event in matches[:10]:
        print(f"\n{event['home']} vs {event['away']}")
        print(
            f"  sport={sport}  event_id={event_id}  "
            f"comp={event['competition_name']}  start={event['start_ts']}  "
            f"ir={event['ir']}  score={event['score']}"
        )
        prices = [
            (key[2], value["price"])
            for key, value in store["sptmkt"].items()
            if key[0] == sport and key[1] == event_id
        ]
        prices.sort()
        if not prices:
            print("  (no prices published)")
            continue
        for bet_type, price in prices[:25]:
            print(f"    {bet_type:40} {price}")
        if len(prices) > 25:
            print(f"    … and {len(prices) - 25} more")


if __name__ == "__main__":
    asyncio.run(main())

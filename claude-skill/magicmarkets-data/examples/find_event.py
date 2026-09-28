"""Find fixtures by team name and print the prices currently published for them.

Usage:
    MM_DATA_TOKEN=<token> python3 find_event.py Manchester United

Options:
    --timeout SECONDS   longest wait for the snapshot, then go on with what arrived (default 30)
    --limit N           prices to print per sport code, 0 for all (default 25)

Steps:
    1. Connect and apply frames to a Store.
    2. Wait until SnapshotTracker says the replay is complete: at least one
       sptmkt upsert, then either the upserts of the last second are mostly
       for keys already seen, or a 2 s quiet gap. If that takes longer than
       --timeout, print a warning and go on with what arrived.
    3. Match the query (all arguments joined, case-insensitive) against home
       and away names, and group the matches by fixture (event_id).
    4. Print fixtures in-running first, then upcoming, then past. Under each
       fixture, print every sport code that has prices, in natural bet_type
       order, with side-aware handicap lines such as ``line=away -1.0``.
       Sport codes without prices are listed on one line.

Timing and progress go to stderr.
Exit status: 0 found, 1 no match (or no token), 2 usage error, 3 no data received.
"""

import argparse
import asyncio
import contextlib
import json
import sys
import time
from datetime import datetime

import mmfeed

MAX_FIXTURES = 10


def show(value: object) -> str:
    """Missing values print as n/a; numbers print as floats (9 -> 9.0), as find_event.mjs does."""
    if value is None:
        return "n/a"
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return str(float(value))
    return str(value)


def has_sptmkt_upsert(frame: object) -> bool:
    records = frame.get("data") if isinstance(frame, dict) else None
    return any(isinstance(r, list) and r[:2] == ["upsert", "sptmkt"] for r in records or [])


async def load_snapshot(token: str, max_wait: float, timing: dict[str, float]):
    """Apply frames until the tracker says the snapshot is complete or times out."""
    store, tracker = mmfeed.Store(), mmfeed.SnapshotTracker(max_wait=max_wait)
    started = time.monotonic()

    async def consume() -> None:
        async for frame in mmfeed.stream_frames(token):
            if frame is mmfeed.RECONNECTED:
                store.reset()
                tracker.reset()
                continue
            timing.setdefault("first frame", time.monotonic() - started)
            if "first sptmkt upsert" not in timing and has_sptmkt_upsert(frame):
                timing["first sptmkt upsert"] = time.monotonic() - started
            store.apply_frame(frame)
            tracker.observe(frame)

    task = asyncio.create_task(consume())
    try:
        while not tracker.is_complete() and not tracker.timed_out() and not task.done():
            await asyncio.sleep(0.05)
        if tracker.is_complete():
            timing["gate"] = time.monotonic() - started
    finally:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task  # re-raises whatever stopped the stream, if it failed
    return store, tracker


def start_epoch(event: dict) -> float:
    try:
        return datetime.fromisoformat(str(event.get("start_ts")).replace("Z", "+00:00")).timestamp()
    except ValueError:
        return 0.0


def fixture_order(variants: list, now: float) -> tuple:
    """In-running first, then upcoming by start time, then past (most recent first)."""
    start = min(start_epoch(event) for _, event in variants)
    if any(event.get("ir") for _, event in variants):
        return (0, start)
    return (1, start) if start >= now else (2, -start)


def find_fixtures(store: mmfeed.Store, query: str, now: float) -> list[tuple[str, list]]:
    """Matching fixtures as ``(event_id, [(sport, event), ...])``, in display order."""
    needle = query.casefold()
    fixtures: dict[str, list] = {}
    for (sport, event_id), event in store.events.items():
        names = f"{event.get('home', '')}\n{event.get('away', '')}".casefold()
        if needle in names:
            fixtures.setdefault(event_id, []).append((sport, event))
    for variants in fixtures.values():
        variants.sort(key=lambda v: v[0])
    return sorted(fixtures.items(), key=lambda kv: (fixture_order(kv[1], now), kv[0]))


def print_fixture(store: mmfeed.Store, event_id: str, variants: list, limit: int) -> None:
    event = variants[0][1]
    score = event.get("score")
    score = f"{score[0]}-{score[1]}" if isinstance(score, list) and len(score) == 2 else "n/a"
    print(f"\n{show(event.get('home'))} vs {show(event.get('away'))}")
    print(
        f"  event_id={event_id}  competition={show(event.get('competition_name'))}"
        f"  start={show(event.get('start_ts'))}"
    )
    print(
        f"  in_running={'yes' if event.get('ir') else 'no'}  score={score}"
        f"  ir_time={json.dumps(event.get('ir_time'), separators=(',', ':'))}"
    )
    no_prices = []
    for sport, _ in variants:
        prices = sorted(store.prices_for(sport, event_id), key=lambda p: mmfeed.bet_type_sort_key(p[0]))
        if not prices:
            no_prices.append(sport)
            continue
        print(f"  sport={sport}")
        shown = prices if limit == 0 else prices[:limit]
        for bet_type, price in shown:
            line = mmfeed.describe_line(bet_type)
            suffix = f"  line={line}" if line else ""
            print(f"    {bet_type:44} {show(price):>8}{suffix}")
        if len(prices) > len(shown):
            print(f"    and {len(prices) - len(shown)} more (use --limit 0 to show all)")
    if no_prices:
        print(f"  no prices: {', '.join(no_prices)}")


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="find_event.py",
        description="Print current prices for fixtures whose team names match the query. "
        "Reads the token from MM_DATA_TOKEN.",
    )
    parser.add_argument("query", nargs="+", help="part of a team name, for example: Manchester United")
    parser.add_argument(
        "--timeout", type=float, default=30.0, metavar="SECONDS", help="longest wait for the snapshot"
    )
    parser.add_argument("--limit", type=int, default=25, metavar="N", help="prices per sport code, 0 for all")
    args = parser.parse_args(argv[1:])
    if not args.timeout > 0:
        parser.error("--timeout must be greater than 0")
    if args.limit < 0:
        parser.error("--limit must be 0 or more")
    args.query = " ".join(args.query)
    return args


async def main() -> int:
    args = parse_args(sys.argv)
    token = mmfeed.get_token()
    timing: dict[str, float] = {}
    print("waiting for the snapshot...", file=sys.stderr)
    store, tracker = await load_snapshot(token, args.timeout, timing)
    summary = f"events={len(store.events)} sptmkt={len(store.sptmkt)}"
    if timing:
        print("timing: " + ", ".join(f"{k} {v:.2f} s" for k, v in timing.items()), file=sys.stderr)
    if not store.events and not store.sptmkt:
        print(f"error: no data received within {args.timeout:g} s", file=sys.stderr)
        return 3
    if tracker.is_complete():
        print(f"snapshot complete in {timing['gate']:.1f} s ({tracker.reason}): {summary}", file=sys.stderr)
    else:
        print(
            f"warning: snapshot not confirmed complete after {args.timeout:g} s ({summary}); "
            "results may be partial",
            file=sys.stderr,
        )

    fixtures = find_fixtures(store, args.query, time.time())
    if not fixtures:
        print(f"no events match {args.query!r}", file=sys.stderr)
        return 1
    for event_id, variants in fixtures[:MAX_FIXTURES]:
        print_fixture(store, event_id, variants, args.limit)
    if len(fixtures) > MAX_FIXTURES:
        print(f"\nand {len(fixtures) - MAX_FIXTURES} more fixtures")
    return 0


if __name__ == "__main__":
    mmfeed.configure_logging()
    mmfeed.run(main())

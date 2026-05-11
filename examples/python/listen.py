"""Minimal MagicMarkets data-feed listener.

Usage:
    python3 listen.py <token>
    MM_DATA_TOKEN=... python3 listen.py
"""
import asyncio
import json
import os
import sys
import websockets


def get_token() -> str:
    if len(sys.argv) > 1:
        return sys.argv[1]
    token = os.environ.get("MM_DATA_TOKEN")
    if not token:
        sys.exit("usage: listen.py <token>   (or set MM_DATA_TOKEN)")
    return token


async def main() -> None:
    url = f"wss://data.magicmarkets.com/v1/stream?token={get_token()}"
    async with websockets.connect(url, max_size=2**27) as ws:
        async for raw in ws:
            frame = json.loads(raw)
            for record in frame["data"]:
                op, collection, key, *rest = record
                value = rest[0] if rest else None
                print(op, collection, key, value)


if __name__ == "__main__":
    asyncio.run(main())

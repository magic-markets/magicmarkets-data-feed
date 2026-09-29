#!/usr/bin/env python3
"""Copy the protocol reference and the Python examples into the Claude skill.

    python3 scripts/sync_skill.py           write the copies
    python3 scripts/sync_skill.py --check   exit 1 if any copy is missing or out of date

Sources and destinations:

    PROTOCOL.md                         -> claude-skill/magicmarkets-data/references/protocol.md
    examples/python/{mmfeed,listen,store,find_event}.py
                                        -> claude-skill/magicmarkets-data/examples/

The example copies are byte-identical. The protocol copy is adapted to the
skill layout: examples/python/X.py becomes examples/X.py, Node paths are
marked as living in the source repository, and other relative links point
to the repository on GitHub. Edit the sources, never the copies.
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SKILL = ROOT / "claude-skill" / "magicmarkets-data"
EXAMPLES = ("mmfeed.py", "listen.py", "store.py", "find_event.py")
REPO_URL = "https://github.com/magicmarkets/magicmarkets-data-feed"

_LINK = re.compile(r"\]\((?!https?:|mailto:|#)([^)\s]+)\)")


def skill_protocol(text: str) -> str:
    """Adapt PROTOCOL.md to the skill, where it lives at references/protocol.md."""

    def link(match: re.Match) -> str:
        target = match[1]
        name = target.removeprefix("examples/python/")
        if name != target and name.split("#")[0] in EXAMPLES:
            return f"](../examples/{name})"
        return f"]({REPO_URL}/blob/main/{target})"

    text = _LINK.sub(link, text)
    text = re.sub(r"examples/python/([\w-]+\.py)", r"examples/\1", text)
    return re.sub(r"`examples/node/([\w./-]+)`", r"the Node example `\1` in the source repository", text)


def pairs(root: Path = ROOT) -> list[tuple[Path, Path]]:
    skill = root / "claude-skill" / "magicmarkets-data"
    out = [(root / "PROTOCOL.md", skill / "references" / "protocol.md")]
    out += [(root / "examples" / "python" / name, skill / "examples" / name) for name in EXAMPLES]
    return out


def main(argv: list[str] | None = None, root: Path = ROOT) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--check", action="store_true", help="report drift instead of writing")
    args = parser.parse_args(argv)

    problems = []
    for src, dst in pairs(root):
        rel = dst.relative_to(root)
        if not src.is_file():
            problems.append(f"missing source: {src.relative_to(root)}")
            continue
        data = src.read_bytes()
        if src.name == "PROTOCOL.md":
            data = skill_protocol(data.decode("utf-8")).encode("utf-8")
        if dst.is_file() and dst.read_bytes() == data:
            continue
        if args.check:
            problems.append(f"out of date: {rel} (run: python3 scripts/sync_skill.py)")
        else:
            dst.parent.mkdir(parents=True, exist_ok=True)
            dst.write_bytes(data)
            print(f"updated {rel}")

    examples_dir = root / "claude-skill" / "magicmarkets-data" / "examples"
    if examples_dir.is_dir():
        for extra in sorted(p for p in examples_dir.iterdir() if p.is_file() and p.name not in EXAMPLES):
            problems.append(f"unexpected file (remove it by hand): {extra.relative_to(root)}")

    for problem in problems:
        print(problem, file=sys.stderr)
    if not problems and args.check:
        print("skill copies are up to date")
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())

"""Unit tests for the helpers in examples/python/find_event.py. No network."""

from __future__ import annotations

from datetime import datetime, timezone

import find_event
import mmfeed

NOW = datetime(2026, 5, 11, 12, 0, tzinfo=timezone.utc).timestamp()


def event(home: str, start: str, ir: bool = False) -> dict:
    return {"home": home, "away": "Visitors", "start_ts": start, "ir": ir, "competition_name": "Test League"}


def store_with(*rows: tuple[str, str, dict]) -> mmfeed.Store:
    s = mmfeed.Store()
    s.apply_frame({"ts": NOW, "data": [["upsert", "events", [sport, eid], ev] for sport, eid, ev in rows]})
    return s


def test_fixture_order_in_running_then_upcoming_then_past():
    s = store_with(
        ("fb", "past-old", event("Past Old", "2026-05-01T10:00:00Z")),
        ("fb", "later", event("Later", "2026-05-12T10:00:00Z")),
        ("fb", "live", event("Live", "2026-05-11T11:00:00Z", ir=True)),
        ("fb", "past-new", event("Past New", "2026-05-10T10:00:00Z")),
        ("fb", "soon", event("Soon", "2026-05-11T13:00:00Z")),
        ("fb_ht", "past-old", event("Past Old", "2026-05-01T10:00:00Z")),
    )
    order = [event_id for event_id, _ in find_event.find_fixtures(s, "", NOW)]
    assert order == ["live", "soon", "later", "past-new", "past-old"]


def test_find_fixtures_groups_sport_codes_and_matches_case_insensitively():
    s = store_with(
        ("fb_ht", "x", event("Manchester United", "2026-05-11T13:00:00Z")),
        ("fb", "x", event("Manchester United", "2026-05-11T13:00:00Z")),
        ("fb_corn", "x", event("Manchester United", "2026-05-11T13:00:00Z")),
        ("fb", "y", event("Leeds", "2026-05-11T13:00:00Z")),
    )
    fixtures = find_event.find_fixtures(s, "MANCHESTER united", NOW)
    assert [(eid, [sport for sport, _ in variants]) for eid, variants in fixtures] == [
        ("x", ["fb", "fb_corn", "fb_ht"])
    ]


def test_in_running_on_any_sport_code_counts():
    variants = [
        ("fb", event("A", "2026-05-01T10:00:00Z")),
        ("fb_ht", event("A", "2026-05-01T10:00:00Z", ir=True)),
    ]
    assert find_event.fixture_order(variants, NOW)[0] == 0


def test_unparseable_start_sorts_as_past():
    assert find_event.fixture_order([("fb", {"start_ts": None})], NOW)[0] == 2


def test_print_fixture_skips_sport_codes_without_prices(capsys):
    s = store_with(
        ("fb", "x", event("Arsenal", "2026-05-11T13:00:00Z")),
        ("fb_corn", "x", event("Arsenal", "2026-05-11T13:00:00Z")),
    )
    s.apply_frame({"data": [["upsert", "sptmkt", ["fb", "x", "for,ah,a,4"], {"price": 1.9}]]})
    event_id, variants = find_event.find_fixtures(s, "arsenal", NOW)[0]
    find_event.print_fixture(s, event_id, variants, limit=25)
    out = capsys.readouterr().out
    assert "  sport=fb\n" in out and "sport=fb_corn" not in out
    assert "  no prices: fb_corn\n" in out
    assert "for,ah,a,4" in out and out.rstrip().endswith("no prices: fb_corn")
    assert "line=away -1.0" in out
    assert "competition=Test League" in out and "score=n/a" in out

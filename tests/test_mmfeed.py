"""Unit tests for examples/python/mmfeed.py. No network."""

from __future__ import annotations

import logging
from urllib.parse import parse_qs, quote, urlsplit

import pytest

import mmfeed
from conftest import FAKE_TOKEN

# --------------------------------------------------------------------------
# get_token
# --------------------------------------------------------------------------


def test_get_token_reads_env(monkeypatch):
    monkeypatch.setenv("MM_DATA_TOKEN", "  EXAMPLE-token-EXAMPLE-token-0000\n")
    assert mmfeed.get_token() == "EXAMPLE-token-EXAMPLE-token-0000"


@pytest.mark.parametrize("value", [None, "", "   "])
def test_get_token_missing_exits_with_message(monkeypatch, value):
    if value is None:
        monkeypatch.delenv("MM_DATA_TOKEN", raising=False)
    else:
        monkeypatch.setenv("MM_DATA_TOKEN", value)
    with pytest.raises(SystemExit) as exc:
        mmfeed.get_token()
    assert exc.value.code == "error: set MM_DATA_TOKEN (see README)"


# --------------------------------------------------------------------------
# stream_url
# --------------------------------------------------------------------------


def query_of(url: str) -> dict[str, list[str]]:
    return parse_qs(urlsplit(url).query, keep_blank_values=True)


def test_stream_url_default(monkeypatch):
    monkeypatch.delenv("MM_DATA_URL", raising=False)
    url = mmfeed.stream_url("abc123")
    assert url == "wss://data.magicmarkets.com/v1/stream?token=abc123"


@pytest.mark.parametrize(
    "token",
    [FAKE_TOKEN, "a&token=evil", "sp ace", "per%cent", "sl/ash", "plus+", "hash#frag", "\u00fc"],
)
def test_stream_url_encodes_token_once(monkeypatch, token):
    monkeypatch.delenv("MM_DATA_URL", raising=False)
    url = mmfeed.stream_url(token)
    assert query_of(url) == {"token": [token]}
    assert url.count("token=") == 1
    assert "#" not in url and " " not in url
    assert url.endswith("?token=" + quote(token, safe=""))


def test_stream_url_env_override(monkeypatch):
    monkeypatch.setenv("MM_DATA_URL", "ws://127.0.0.1:9999/v1/stream")
    assert mmfeed.stream_url("t") == "ws://127.0.0.1:9999/v1/stream?token=t"


def test_stream_url_explicit_base_beats_env(monkeypatch):
    monkeypatch.setenv("MM_DATA_URL", "ws://127.0.0.1:9999/v1/stream")
    assert mmfeed.stream_url("t", "ws://localhost:1/x") == "ws://localhost:1/x?token=t"


@pytest.mark.parametrize(
    "base",
    ["http://127.0.0.1:1/v1/stream", "https://data.magicmarkets.com/v1/stream", "ws:///nohost", "stream"],
)
def test_stream_url_rejects_non_websocket_urls_without_leaking_the_token(base):
    with pytest.raises(ValueError) as exc:
        mmfeed.stream_url(FAKE_TOKEN, base + "?token=" + FAKE_TOKEN)
    assert "ws:// or wss://" in str(exc.value)
    assert FAKE_TOKEN not in str(exc.value) and quote(FAKE_TOKEN, safe="") not in str(exc.value)


def test_stream_url_drops_existing_token_and_keeps_other_params(monkeypatch):
    monkeypatch.delenv("MM_DATA_URL", raising=False)
    url = mmfeed.stream_url("new", "ws://h/v1/stream?foo=bar&token=old&token=older")
    assert query_of(url) == {"foo": ["bar"], "token": ["new"]}
    assert url.index("foo=") < url.index("token=")


# --------------------------------------------------------------------------
# redact
# --------------------------------------------------------------------------


def test_redact_query_param():
    out = mmfeed.redact("connecting to wss://h/v1/stream?foo=1&token=abc123&x=2")
    assert out == "connecting to wss://h/v1/stream?foo=1&token=***&x=2"


def test_redact_header_value():
    out = mmfeed.redact("Authorization: Token EXAMPLE-token-EXAMPLE-token-0000")
    assert out == "Authorization: Token ***"


def test_redact_leaves_ordinary_text_alone():
    text = "Token expired; the token parameter is required"
    assert mmfeed.redact(text) == text


def test_redact_known_token_raw_and_encoded():
    encoded = quote(FAKE_TOKEN, safe="")
    out = mmfeed.redact(f"raw={FAKE_TOKEN} enc={encoded}", FAKE_TOKEN)
    assert FAKE_TOKEN not in out and encoded not in out


def test_redact_stream_url_hides_encoded_token():
    url = mmfeed.stream_url(FAKE_TOKEN, "ws://h/v1/stream")
    out = mmfeed.redact(url)
    assert out == "ws://h/v1/stream?token=***"


def test_redact_filter_cleans_websockets_debug_records(caplog):
    caplog.set_level(logging.DEBUG, logger="mmfeed")
    logging.getLogger("mmfeed.websockets").debug("> GET %s HTTP/1.1", "/v1/stream?token=s3cret")
    assert "s3cret" not in caplog.text
    assert "token=***" in caplog.text


def test_redact_filter_cleans_tracebacks(caplog):
    caplog.set_level(logging.INFO, logger="mmfeed")
    try:
        raise RuntimeError(f"failed on wss://h/v1/stream?token={quote(FAKE_TOKEN, safe='')}")
    except RuntimeError:
        logging.getLogger("mmfeed").exception("stream failed")
    record = caplog.records[-1]
    assert "token=***" in record.exc_text
    assert quote(FAKE_TOKEN, safe="") not in record.exc_text


# --------------------------------------------------------------------------
# split_bet_type
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    "bet_type",
    [
        "for,h",
        "against,ah,h,-4",
        "for,cs,2,1",
        "for,over,2.5",
        "for,tset,all,vwhole,set,ah,p1,6",
        "for,",
        ",for",
        "for,,x",
        "single",
    ],
)
def test_split_plain_matches_str_split(bet_type):
    assert mmfeed.split_bet_type(bet_type) == bet_type.split(",")


def test_split_empty_string():
    assert mmfeed.split_bet_type("") == []


def test_split_proposition_keeps_json_as_one_token():
    bt = 'for,proposition,["Exact Total Runs 1st Half","0","Game Props - 1st Half"]'
    assert mmfeed.split_bet_type(bt) == [
        "for",
        "proposition",
        '["Exact Total Runs 1st Half","0","Game Props - 1st Half"]',
    ]


def test_split_proposition_with_commas_quotes_and_brackets_inside():
    payload = '["Player Hits, Over","1.5","Props \\"Batter\\", [late]"]'
    parts = mmfeed.split_bet_type("against,proposition," + payload)
    assert parts == ["against", "proposition", payload]


def test_split_json_followed_by_more_tokens():
    assert mmfeed.split_bet_type('for,proposition,["a,b"],x,y') == [
        "for",
        "proposition",
        '["a,b"]',
        "x",
        "y",
    ]


def test_split_json_object_token():
    assert mmfeed.split_bet_type('for,x,{"k":"v,w"}') == ["for", "x", '{"k":"v,w"}']


@pytest.mark.parametrize(
    "bet_type, opaque",
    [
        ('for,proposition,["unterminated, list', '["unterminated, list'),
        ('for,proposition,["a"]junk,b', '["a"]junk,b'),
    ],
)
def test_split_bad_json_keeps_rest_opaque(bet_type, opaque):
    assert mmfeed.split_bet_type(bet_type) == ["for", "proposition", opaque]


# --------------------------------------------------------------------------
# decode_line / handicap_line
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    "wire, line",
    [
        (0, 0.0),
        (1, 0.25),
        (2, 0.5),
        (3, 0.75),
        (7, 1.75),
        (8, 2.0),
        (-2, -0.5),
        (-4, -1.0),
        (-21, -5.25),
        (-26, -6.5),
        (35, 8.75),
        (644, 161.0),
        (1202, 300.5),
    ],
)
def test_decode_line(wire, line):
    assert mmfeed.decode_line(wire) == line


@pytest.mark.parametrize(
    "bet_type, line",
    [
        ("for,ah,h,-4", -1.0),
        ("against,ah,a,7", 1.75),
        ("for,ahover,10", 2.5),
        ("for,ahunder,2", 0.5),
        ("for,tahover,h,2", 0.5),
        ("for,tahunder,a,-21", -5.25),
        ("for,tp,all,ah,a,-6", -1.5),
        ("for,tp,all,ahunder,16", 4.0),
        ("for,tset,all,vwhole,game,ahover,62", 15.5),
        ("for,tset,all,vwhole,set,ah,p1,6", 1.5),
        ("for,ir,0,2,ah,a,-2", -0.5),
        ("for,ahover,644", 161.0),
    ],
)
def test_handicap_line_decodes_handicap_families(bet_type, line):
    assert mmfeed.handicap_line(bet_type) == line


@pytest.mark.parametrize(
    "bet_type",
    [
        "for,h",
        "for,over,2.5",  # decimal line, already real
        "for,under,6.5",
        "for,cs,2,1",  # scores are not lines
        "for,ml,a",
        "for,win,374",
        "for,ah,h",  # no line parameter
        "for,ahover,2.5",  # not an integer, so not the packed encoding
        'for,proposition,["ah","4"]',  # "ah" inside proposition JSON is text
        "for,newfamily,ah",
        "",
    ],
)
def test_handicap_line_ignores_everything_else(bet_type):
    assert mmfeed.handicap_line(bet_type) is None


def test_parse_event_id():
    assert mmfeed.parse_event_id("2026-05-09,969,1738") == ("2026-05-09", 969, 1738)
    assert mmfeed.parse_event_id("") is None  # outright rows: never split
    assert mmfeed.parse_event_id("2026-05-09,969") is None
    assert mmfeed.parse_event_id("2026-05-09,a,b") is None


@pytest.mark.parametrize(
    "bad", ["2026-05-09,-1,2", "2026-05-09,+1,2", "2026-05-09, 1,2", "2026-05-09,1,\u0663"]
)
def test_parse_event_id_accepts_ascii_digits_only(bad):
    assert mmfeed.parse_event_id(bad) is None


# The verified sign convention: the ah integer is the HOME handicap x 4.
@pytest.mark.parametrize(
    "bet_type, text",
    [
        ("for,ah,h,4", "home +1.0"),
        ("for,ah,a,4", "away -1.0"),
        ("for,ah,h,-4", "home -1.0"),
        ("for,ah,a,-4", "away +1.0"),
        ("against,ah,a,-4", "away +1.0"),  # the direction does not change the line
        ("for,ah,h,0", "home 0.0"),
        ("for,ah,a,0", "away 0.0"),
        ("for,ah,h,-21", "home -5.25"),
        ("for,ah,a,7", "away -1.75"),
        ("for,ahover,10", "over 2.5"),
        ("for,ahunder,7", "under 1.75"),
        ("for,tahover,h,2", "home over 0.5"),
        ("for,tahunder,a,6", "away under 1.5"),
        ("for,tp,all,ah,a,22", "away -5.5"),
        ("for,tp,all,ah,h,-6", "home -1.5"),
        ("for,tp,all,ahunder,16", "under 4.0"),
        ("for,tset,all,vwhole,set,ah,p1,6", "p1 +1.5"),
        ("for,tset,all,vwhole,set,ah,p2,6", "p2 -1.5"),
        ("for,tset,all,vwhole,game,ahover,62", "over 15.5"),
        ("for,ir,0,2,ah,a,-2", "away +0.5"),
        ("for,ahover,644", "over 161.0"),
    ],
)
def test_describe_line(bet_type, text):
    assert mmfeed.describe_line(bet_type) == text


@pytest.mark.parametrize(
    "bet_type", ["for,h", "for,over,2.5", "for,cs,2,1", 'for,proposition,["ah","4"]', ""]
)
def test_describe_line_none_without_a_handicap(bet_type):
    assert mmfeed.describe_line(bet_type) is None


def test_handicap_line_stays_home_perspective():
    assert mmfeed.handicap_line("for,ah,a,-4") == -1.0
    assert mmfeed.handicap_line("for,ah,h,-4") == -1.0


def test_bet_type_sort_key_natural_order():
    shuffled = [
        "for,score,both",
        "against,ah,h,-4",
        "for,ah,h,2",
        "for,ah,h,-8",
        "for,h",
        "for,ah,a,-4",
        "for,ah,h,-4",
        "for,a",
        "for,ahover,10",
        "for,ahover,2",
        'for,proposition,["x","1"]',
        "for,score,both,no",
    ]
    assert sorted(shuffled, key=mmfeed.bet_type_sort_key) == [
        "for,a",
        "for,ah,a,-4",
        "for,ah,h,-8",
        "for,ah,h,-4",
        "against,ah,h,-4",
        "for,ah,h,2",
        "for,ahover,2",
        "for,ahover,10",
        "for,h",
        'for,proposition,["x","1"]',
        "for,score,both",
        "for,score,both,no",
    ]


# --------------------------------------------------------------------------
# Store
# --------------------------------------------------------------------------

EVENT_KEY = ["fb", "2026-05-09,969,1738"]
EVENT = {"home": "1. FC Magdeburg", "away": "Hertha BSC", "ir": True}


def frame(*records, ts=1778476931.285601):
    return {"ts": ts, "data": list(records)}


def test_store_upsert_then_delete():
    s = mmfeed.Store()
    assert s.apply_frame(frame(["upsert", "events", EVENT_KEY, EVENT])) == 1
    assert s.events == {("fb", "2026-05-09,969,1738"): EVENT}
    assert s.last_ts == 1778476931.285601
    assert s.apply_frame(frame(["delete", "events", EVENT_KEY], ts=1778476932.0)) == 1
    assert s.events == {}
    assert s.applied == 2 and s.skipped == 0
    assert s.last_ts == 1778476932.0


def test_store_upsert_replaces_value():
    s = mmfeed.Store()
    key = ["fb", "2026-05-09,969,1738", "for,h"]
    s.apply_frame(
        frame(["upsert", "sptmkt", key, {"price": 1.91}], ["upsert", "sptmkt", key, {"price": 2.0}])
    )
    assert s.sptmkt == {tuple(key): {"price": 2.0}}


def test_store_delete_of_unknown_key_is_harmless():
    s = mmfeed.Store()
    assert s.apply_frame(frame(["delete", "sptmkt", ["fb", "x", "for,h"]])) == 1
    assert s.sptmkt == {}


def test_store_empty_event_id_rows_are_kept_whole():
    s = mmfeed.Store()
    s.apply_frame(frame(["upsert", "sptmkt", ["fb", "", "against,win,374"], {"price": 1.012}]))
    assert s.sptmkt == {("fb", "", "against,win,374"): {"price": 1.012}}
    assert s.prices_for("fb", "") == [("against,win,374", 1.012)]


@pytest.mark.parametrize(
    "record",
    [
        ["upsert", "widgets", ["x"], {"a": 1}],  # unknown collection
        ["replace", "events", EVENT_KEY, EVENT],  # unknown op
        ["upsert", "events", EVENT_KEY],  # upsert without a value
        ["upsert", "events", EVENT_KEY, "not-an-object"],
        ["upsert", "events", "not-a-list", EVENT],
        ["upsert", "events", [["unhashable"]], EVENT],
        ["upsert", ["events"], EVENT_KEY, EVENT],  # unhashable collection
        ["upsert"],
        [],
        "not-a-record",
        None,
        {"op": "upsert"},
    ],
)
def test_store_skips_bad_records_without_raising(record):
    s = mmfeed.Store()
    good = ["upsert", "events", EVENT_KEY, EVENT]
    assert s.apply_frame(frame(record, good)) == 1
    assert s.skipped == 1
    assert list(s.events) == [tuple(EVENT_KEY)]


@pytest.mark.parametrize("bad", [None, [], "text", {"ts": 1.0}, {"ts": 1.0, "data": "x"}])
def test_store_tolerates_bad_frames(bad):
    s = mmfeed.Store()
    assert s.apply_frame(bad) == 0
    assert s.events == {} and s.sptmkt == {}


def test_store_ignores_non_numeric_ts():
    s = mmfeed.Store()
    s.apply_frame({"ts": "soon", "data": []})
    s.apply_frame({"ts": True, "data": []})
    assert s.last_ts == 0.0


def test_store_in_running_and_prices_for():
    s = mmfeed.Store()
    s.apply_frame(
        frame(
            ["upsert", "events", EVENT_KEY, EVENT],
            ["upsert", "events", ["fb", "2026-05-10,19,42"], {"ir": False}],
            ["upsert", "sptmkt", [*EVENT_KEY, "for,h"], {"price": 1.5}],
            ["upsert", "sptmkt", [*EVENT_KEY, "for,ah,a,-1"], {"price": 1.72}],
            ["upsert", "sptmkt", ["fb_ht", EVENT_KEY[1], "for,h"], {"price": 2.2}],
            ["upsert", "sptmkt", ["fb", "", "for,win,969"], {"price": 4.0}],
        )
    )
    assert s.in_running() == [tuple(EVENT_KEY)]
    assert s.prices_for(*EVENT_KEY) == [("for,ah,a,-1", 1.72), ("for,h", 1.5)]
    assert s.prices_for("fb", "") == [("for,win,969", 4.0)]
    assert s.prices_for("fb", "missing") == []


def test_store_price_index_follows_upserts_deletes_and_reset():
    s = mmfeed.Store()
    k1, k2 = [*EVENT_KEY, "for,h"], [*EVENT_KEY, "for,d"]
    s.apply_frame(frame(["upsert", "sptmkt", k1, {"price": 1.5}], ["upsert", "sptmkt", k2, {"price": 3.4}]))
    s.apply_frame(frame(["upsert", "sptmkt", k1, {"price": 1.6}]))  # replace, do not merge
    assert s.prices_for(*EVENT_KEY) == [("for,d", 3.4), ("for,h", 1.6)]
    s.apply_frame(frame(["delete", "sptmkt", k2], ["delete", "sptmkt", k2]))
    assert s.prices_for(*EVENT_KEY) == [("for,h", 1.6)]
    s.apply_frame(frame(["delete", "sptmkt", k1]))
    assert s.prices_for(*EVENT_KEY) == [] and s._prices == {}
    s.apply_frame(frame(["upsert", "sptmkt", ["fb", "odd-key"], {"price": 2.0}]))  # not 3 parts
    assert s.sptmkt == {("fb", "odd-key"): {"price": 2.0}} and s._prices == {}
    s.apply_frame(frame(["upsert", "sptmkt", k1, {"price": 1.5}]))
    s.reset()
    assert s.prices_for(*EVENT_KEY) == []


def test_store_prices_for_matches_a_full_scan(session):
    s = mmfeed.Store()
    for phase in ("events_phase", "sptmkt_phase", "deltas", "steady"):
        for f in session[phase]:
            s.apply_frame(f)
    for sport, event_id in {k[:2] for k in s.sptmkt}:
        scan = sorted((k[2], v["price"]) for k, v in s.sptmkt.items() if k[:2] == (sport, event_id))
        assert s.prices_for(sport, event_id) == scan


def test_store_reset():
    s = mmfeed.Store()
    s.apply_frame(frame(["upsert", "events", EVENT_KEY, EVENT], ["bogus"]))
    s.reset()
    assert (s.events, s.sptmkt, s.applied, s.skipped, s.last_ts) == ({}, {}, 0, 0, 0.0)


def test_store_replays_whole_fixture_session(session):
    s = mmfeed.Store()
    for phase in ("events_phase", "sptmkt_phase"):
        for f in session[phase]:
            s.apply_frame(f)
    snap = session["expected"]["after_snapshot"]
    assert (len(s.events), len(s.sptmkt)) == (snap["events"], snap["sptmkt"])
    for f in session["deltas"]:
        s.apply_frame(f)
    final = session["expected"]["final"]
    assert len(s.events) == final["events"]
    assert len(s.sptmkt) == final["sptmkt"]
    assert len(s.in_running()) == final["in_running"]
    assert s.applied == final["applied"]
    assert s.skipped == final["skipped"]
    assert s.sptmkt[("fb", "2026-05-11,19,42", "for,h")] == {"price": 2.05}
    assert ("fb", "", "against,win,374") not in s.sptmkt
    assert ("fb", "", "for,win,19") in s.sptmkt


# --------------------------------------------------------------------------
# SnapshotTracker (fake clock: the numbers are seconds)
# --------------------------------------------------------------------------


def events_frame(i: int) -> dict:
    return frame(["upsert", "events", ["fb", f"2026-05-10,{i},{i + 1}"], EVENT])


def sptmkt_frame(i: int, price: float = 1.91) -> dict:
    return frame(["upsert", "sptmkt", ["fb", f"2026-05-10,{i},{i + 1}", "for,h"], {"price": price}])


def replay(tracker, start: float, events: int = 200, prices: int = 400, step: float = 0.01) -> float:
    """Feed an all-new-keys replay, events first; return the time of the last frame."""
    t = start
    for i in range(events):
        tracker.observe(events_frame(i), now=t)
        assert not tracker.is_complete(t)
        t += step
    for i in range(prices):
        tracker.observe(sptmkt_frame(i), now=t)
        assert not tracker.is_complete(t), f"completed early at {t}"
        t += step
    return t - step


def test_tracker_events_phase_with_6s_gap_is_not_complete():
    t = mmfeed.SnapshotTracker()
    t.observe(events_frame(1), now=1000.0)
    t.observe(events_frame(2), now=1000.2)
    assert not t.is_complete(1006.2)  # a long gap inside the events phase
    t.observe(events_frame(3), now=1006.2)
    t.observe(events_frame(1), now=1006.3)  # a repeat key: still no sptmkt, so no completion
    assert not t.is_complete(1006.4)
    assert not t.is_complete(1020.0)


def test_tracker_replay_with_current_ts_does_not_complete_early():
    # ts equals the local clock during the replay on the live feed: it must not matter
    t = mmfeed.SnapshotTracker()
    clock = 1000.0
    for i in range(300):
        f = events_frame(i) if i < 100 else sptmkt_frame(i)
        f["ts"] = clock
        t.observe(f, now=clock)
        assert not t.is_complete(clock)
        clock += 0.01


def test_tracker_steady_flow_without_gaps_completes_by_new_key_rule():
    t = mmfeed.SnapshotTracker()
    last = replay(t, 1000.0)
    clock = last
    completed_at = None
    for n in range(40):  # known keys every 0.1 s: never a quiet gap
        clock = round(last + 0.1 * (n + 1), 3)
        t.observe(sptmkt_frame(n % 50, price=2.0 + n / 100), now=clock)
        if t.is_complete(clock):
            completed_at = clock
            break
    assert completed_at is not None, "steady flow never completed the snapshot"
    assert t.reason == "new keys"
    # the replay's burst of new keys has to leave the 1 s window first
    assert last + 0.5 <= completed_at <= last + 1.2


def test_tracker_silent_feed_after_replay_completes_by_quiet_gap():
    t = mmfeed.SnapshotTracker()
    last = replay(t, 1000.0)
    assert not t.is_complete(last + 1.99)
    assert t.is_complete(last + 2.0)
    assert t.reason == "quiet gap"


def test_tracker_new_market_burst_below_ratio_still_completes():
    t = mmfeed.SnapshotTracker(window=1.0)
    last = replay(t, 1000.0, events=10, prices=20)
    # 95 new keys among 212 upserts inside one window: 45 percent, below 0.5
    known = [
        ["upsert", "sptmkt", ["fb", f"2026-05-10,{i},{i + 1}", "for,h"], {"price": 2.0}] for i in range(20)
    ]
    fresh = [["upsert", "sptmkt", ["fb", "new", f"for,cs,{i},0"], {"price": 9.0}] for i in range(95)]
    t.observe(frame(*(known * 5 + known[:17])), now=last + 1.5)
    assert t.is_complete(last + 1.5)  # the replay has left the window
    t2 = mmfeed.SnapshotTracker(window=1.0)
    last = replay(t2, 1000.0, events=10, prices=20)
    t2.observe(frame(*(known * 5 + known[:17] + fresh)), now=last + 1.5)
    assert t2.is_complete(last + 1.5)
    assert t2.reason == "new keys"


def test_tracker_mostly_new_keys_blocks_rule_a_but_not_quiet_gap():
    t = mmfeed.SnapshotTracker()
    t.observe(sptmkt_frame(1), now=1000.0)
    t.observe(
        frame(*[["upsert", "sptmkt", ["fb", "x", f"for,cs,{i},0"], {"price": 9.0}] for i in range(10)]),
        now=1000.5,
    )
    assert not t.is_complete(1000.5)
    assert t.is_complete(1002.5)
    assert t.reason == "quiet gap"


def test_tracker_counts_only_upserts_on_known_collections():
    t = mmfeed.SnapshotTracker()
    t.observe(sptmkt_frame(1), now=1000.0)
    junk = frame(
        ["delete", "sptmkt", ["fb", "2026-05-10,1,2", "for,h"]],
        ["upsert", "widgets", ["x"], {"a": 1}],
        ["upsert", "sptmkt", [["unhashable"]], {"price": 1.0}],
        ["upsert", "sptmkt", ["fb", "y", "for,h"]],  # no value
        None,
    )
    for n in range(20):
        t.observe(junk, now=1000.1 + n * 0.1)
    assert not t.is_complete(1002.0)  # only junk since the one new key
    assert t.recent_upserts == 1 and t.recent_new == 1


def test_tracker_timed_out():
    t = mmfeed.SnapshotTracker(max_wait=30.0)
    assert not t.timed_out(500.0)  # the clock starts at the first call
    t.observe(events_frame(1), now=501.0)
    assert not t.timed_out(529.9)
    assert t.timed_out(530.0)
    assert not t.is_complete(530.0)


def test_tracker_not_timed_out_once_complete_and_reset_keeps_clock():
    t = mmfeed.SnapshotTracker(max_wait=5.0)
    t.observe(sptmkt_frame(1), now=100.0)
    assert t.is_complete(102.0)
    assert not t.timed_out(200.0)
    t.reset()  # reconnect: replay progress is forgotten, the wait clock is not
    assert not t.is_complete(200.0)
    assert t.timed_out(200.0)


def test_tracker_custom_thresholds():
    t = mmfeed.SnapshotTracker(quiet_gap=0.3, window=0.2, new_key_ratio=0.1, max_wait=1.0)
    t.observe(sptmkt_frame(1), now=10.0)
    assert not t.is_complete(10.25)
    assert t.is_complete(10.35)


def test_tracker_events_only_repeated_known_keys_do_not_complete():
    # Updates to known event keys look like steady state, but no sptmkt yet: not complete.
    t = mmfeed.SnapshotTracker()
    for i in range(10):
        t.observe(events_frame(i), now=1000.0 + i * 0.01)
    clock = 1000.1
    while clock < 1001.6:
        t.observe(events_frame(int(clock * 100) % 10), now=clock)
        assert not t.is_complete(clock)
        clock = round(clock + 0.05, 3)


def test_tracker_exactly_half_new_keys_is_not_below_the_ratio():
    t = mmfeed.SnapshotTracker(window=1.0, new_key_ratio=0.5)
    t.observe(sptmkt_frame(1), now=1000.0)  # 1 new
    t.observe(sptmkt_frame(1, price=2.0), now=1000.1)  # 1 known: 1 of 2 new, exactly 0.5
    assert not t.is_complete(1000.1)
    t.observe(sptmkt_frame(1, price=2.1), now=1000.2)  # 1 of 3 new
    assert t.is_complete(1000.2)
    assert t.reason == "new keys"


def test_tracker_stops_tracking_once_complete_and_reset_rearms():
    t = mmfeed.SnapshotTracker()
    last = replay(t, 1000.0, events=5, prices=10)
    assert t.is_complete(last + 2.0)
    size = len(t.seen)
    for i in range(100, 200):
        t.observe(sptmkt_frame(i), now=last + 3.0)
    assert len(t.seen) == size  # no growth after completion
    assert t.is_complete(last + 3.0)
    t.reset()
    assert t.seen == set() and not t.is_complete(last + 3.0)
    # the replay after a reconnect sends the same keys again: they must count as new
    second = replay(t, last + 4.0, events=5, prices=10)
    assert not t.is_complete(second)


def test_tracker_default_clock_is_monotonic():
    t = mmfeed.SnapshotTracker()
    t.observe(sptmkt_frame(1))
    assert not t.is_complete()
    assert not t.timed_out()


# --------------------------------------------------------------------------
# backoff_delay
# --------------------------------------------------------------------------


@pytest.mark.parametrize("attempt", range(0, 12))
def test_backoff_bounds(attempt):
    capped = min(60.0, 1.0 * 2**attempt)
    low = mmfeed.backoff_delay(attempt, 1.0, 60.0, rng=lambda: 0.0)
    high = mmfeed.backoff_delay(attempt, 1.0, 60.0, rng=lambda: 0.999999)
    assert low == capped / 2
    assert capped / 2 <= high <= capped
    assert high <= 60.0


def test_backoff_schedule_grows_then_caps():
    delays = [mmfeed.backoff_delay(a, 1.0, 60.0, rng=lambda: 1.0) for a in range(10)]
    assert delays == [1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 60.0, 60.0, 60.0, 60.0]


def test_backoff_huge_attempt_does_not_overflow():
    assert mmfeed.backoff_delay(10_000, 1.0, 60.0, rng=lambda: 1.0) == 60.0


def test_backoff_jitter_spreads_clients():
    import random

    rng = random.Random(7)
    samples = {round(mmfeed.backoff_delay(3, 1.0, 60.0, rng=rng.random), 3) for _ in range(50)}
    assert len(samples) > 40
    assert all(4.0 <= d <= 8.0 for d in samples)

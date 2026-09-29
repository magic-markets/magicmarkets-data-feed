"""Tests for the repository scripts: check_copy, sync_skill, check_links."""

from __future__ import annotations

from pathlib import Path

import pytest

import check_copy
import check_links
import sync_skill

# Banned strings are built from pieces so this file passes check_copy itself.
EM, EN = "\u2014", "\u2013"
TWO_WORD_BRAND = "Magic" + " Markets"
SLUG = "magic" + "-markets"
ORG = "magicmarkets"


# --------------------------------------------------------------------------
# check_copy
# --------------------------------------------------------------------------


@pytest.mark.parametrize(
    "line, label",
    [
        (f"a {EM} b", "em dash"),
        ("a &" + "mdash; b", "em dash"),
        ("pages 4" + EN + "15", "en dash"),
        ("a &" + "ndash; b", "en dash"),
        ("ship it \U0001f680", "emoji"),
        ("done \u2705", "emoji"),
        ("yes \u2713", "emoji"),
        ("\u2b50 star", "emoji"),
        (f"welcome to {TWO_WORD_BRAND}", "brand"),
        ("MAGIC" + "-" + "MARKETS rocks", "brand"),
        (SLUG.title() + " is the name", "brand"),
        (f"the {SLUG} team", "brand"),
        (f"see https://example.com/{SLUG}/repo", "brand"),
        (f"https://github.com/{SLUG}/x and {TWO_WORD_BRAND}", "brand"),
    ],
)
def test_check_copy_flags_each_rule(tmp_path, capsys, line, label):
    f = tmp_path / "doc.md"
    f.write_text(f"fine first line\n{line}\n", encoding="utf-8")
    assert check_copy.main([str(f)]) == 1
    out = capsys.readouterr().out
    assert f"{f}:2:" in out
    assert label in out


def test_check_copy_allows_clean_text_and_arrows(tmp_path, capsys):
    f = tmp_path / "ok.py"
    f.write_text(
        '"""MagicMarkets prices \u2191 and \u2193, ranges 4-15 s, x \u00d7 y, \u2265 2."""\n',
        encoding="utf-8",
    )
    assert check_copy.main([str(f)]) == 0
    assert capsys.readouterr().out == ""


@pytest.mark.parametrize(
    "line",
    [
        f"https://github.com/{SLUG}/magicmarkets-data-feed",
        f"[![CI](https://github.com/{SLUG}/magicmarkets-data-feed/actions/workflows/ci.yml/badge.svg)]",
        f"git clone https://github.com/{SLUG}/magicmarkets-data-feed.git",
        f"git clone git@github.com:{SLUG}/magicmarkets-data-feed.git",
        f"url: https://GitHub.com/{SLUG}/magicmarkets-data-feed/security",
    ],
)
def test_check_copy_allows_the_org_slug_in_github_urls(tmp_path, capsys, line):
    f = tmp_path / "README.md"
    f.write_text(line + "\n", encoding="utf-8")
    assert check_copy.main([str(f)]) == 0, capsys.readouterr().out


@pytest.mark.parametrize("name", ["Makefile", "requirements.txt", "pyproject.toml"])
def test_check_copy_scans_config_files(tmp_path, capsys, name):
    (tmp_path / name).write_text(f"# {TWO_WORD_BRAND}\n", encoding="utf-8")
    assert check_copy.main([str(tmp_path)]) == 1
    assert f"{name}:1:" in capsys.readouterr().out


def test_check_copy_walks_directories_and_skips_vendor_dirs(tmp_path, capsys):
    (tmp_path / "node_modules").mkdir()
    (tmp_path / "node_modules" / "dep.json").write_text(f'"{EM}"', encoding="utf-8")
    (tmp_path / "notes.rst").write_text(EM, encoding="utf-8")  # not a scanned suffix
    (tmp_path / "sub").mkdir()
    (tmp_path / "sub" / "bad.yml").write_text(f"name: {TWO_WORD_BRAND}\n", encoding="utf-8")
    assert check_copy.main([str(tmp_path)]) == 1
    out = capsys.readouterr().out
    assert "bad.yml:1:" in out
    assert "node_modules" not in out and "notes.rst" not in out


def test_check_copy_missing_path_fails(tmp_path):
    assert check_copy.main([str(tmp_path / "nope.md")]) == 1


# --------------------------------------------------------------------------
# sync_skill
# --------------------------------------------------------------------------


def make_repo(root: Path) -> None:
    (root / "PROTOCOL.md").write_text("# Protocol\n", encoding="utf-8")
    py = root / "examples" / "python"
    py.mkdir(parents=True)
    for name in sync_skill.EXAMPLES:
        (py / name).write_text(f"# {name}\n", encoding="utf-8")


def test_sync_skill_check_then_write(tmp_path, capsys):
    make_repo(tmp_path)
    assert sync_skill.main(["--check"], root=tmp_path) == 1
    assert "out of date" in capsys.readouterr().err

    assert sync_skill.main([], root=tmp_path) == 0
    skill = tmp_path / "claude-skill" / "magicmarkets-data"
    assert (skill / "references" / "protocol.md").read_text() == "# Protocol\n"
    for name in sync_skill.EXAMPLES:
        assert (skill / "examples" / name).read_bytes() == (
            tmp_path / "examples" / "python" / name
        ).read_bytes()
    assert sync_skill.main(["--check"], root=tmp_path) == 0

    (tmp_path / "PROTOCOL.md").write_text("# Protocol v2\n", encoding="utf-8")
    assert sync_skill.main(["--check"], root=tmp_path) == 1


def test_sync_skill_reports_unexpected_files(tmp_path, capsys):
    make_repo(tmp_path)
    sync_skill.main([], root=tmp_path)
    stray = tmp_path / "claude-skill" / "magicmarkets-data" / "examples" / "old.py"
    stray.write_text("", encoding="utf-8")
    assert sync_skill.main(["--check"], root=tmp_path) == 1
    assert "unexpected file" in capsys.readouterr().err
    assert stray.exists()  # never deleted automatically


# --------------------------------------------------------------------------
# check_links
# --------------------------------------------------------------------------


def test_check_links(tmp_path, capsys):
    (tmp_path / "docs").mkdir()
    (tmp_path / "docs" / "guide.md").write_text(
        "# Guide\n\n## 3.2 Detecting `snapshot` complete\n\n## Setup\n\n## Setup\n", encoding="utf-8"
    )
    readme = tmp_path / "README.md"
    readme.write_text(
        "\n".join(
            [
                "[ok](docs/guide.md)",
                "[ok anchor](docs/guide.md#32-detecting-snapshot-complete)",
                "[dup anchor](docs/guide.md#setup-1)",
                "[self](#title)",
                "[web](https://example.com/missing.md)",
                "[mail](mailto:someone@example.com)",
                "`[code](nope.md)`",
                "```",
                "[fenced](nope.md)",
                "```",
                "[dir](docs/)",
                "[broken](docs/missing.md)",
                "[bad anchor](docs/guide.md#nowhere)",
                "[ref]: docs/also-missing.md",
                "# Title",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    assert check_links.main([str(readme)]) == 1
    out = capsys.readouterr().out.splitlines()
    assert out == [
        f"{readme}:12: broken link: docs/missing.md",
        f"{readme}:13: missing anchor: docs/guide.md#nowhere",
        f"{readme}:14: broken link: docs/also-missing.md",
    ]


def test_slugify_matches_github():
    assert check_links.slugify('3.2 Detecting "snapshot complete"') == "32-detecting-snapshot-complete"
    assert check_links.slugify("`events` first") == "events-first"
    assert check_links.slugify("Reconnect, concurrency, keepalive") == "reconnect-concurrency-keepalive"


def test_check_links_flags_this_repository_under_another_org(tmp_path, capsys):
    doc = tmp_path / "README.md"
    doc.write_text(
        "\n".join(
            [
                "[ok](https://github.com/magicmarkets/magicmarkets-data-feed/actions)",
                "git clone https://github.com/MagicMarkets/magicmarkets-data-feed.git",
                "[other repo](https://github.com/actions/checkout)",
                "[stale](https://github.com/magic" + "-markets/magicmarkets-data-feed/issues)",
                "git clone git@github.com:someone/magicmarkets-data-feed.git",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    assert check_links.main([str(doc)], expected=f"{ORG}/magicmarkets-data-feed") == 1
    out = capsys.readouterr().out.splitlines()
    assert [line.split(": ", 2)[1] for line in out] == ["wrong repository URL", "wrong repository URL"]
    assert f"{doc}:4:" in out[0] and f"{doc}:5:" in out[1]


def test_expected_repo_comes_from_the_origin_remote():
    assert check_links.expected_repo() == f"{ORG}/magicmarkets-data-feed" == check_links.EXPECTED_REPO


def test_sync_skill_adapts_protocol_paths(tmp_path):
    make_repo(tmp_path)
    (tmp_path / "PROTOCOL.md").write_text(
        "See `examples/python/mmfeed.py` and `examples/node/mmfeed.mjs`.\n"
        "[helper](examples/python/find_event.py) [readme](README.md) [anchor](#top) [web](https://x.test/a)\n",
        encoding="utf-8",
    )
    assert sync_skill.main([], root=tmp_path) == 0
    copy = (tmp_path / "claude-skill" / "magicmarkets-data" / "references" / "protocol.md").read_text()
    assert copy == (
        "See `examples/mmfeed.py` and the Node example `mmfeed.mjs` in the source repository.\n"
        "[helper](../examples/find_event.py) "
        "[readme](https://github.com/magicmarkets/magicmarkets-data-feed/blob/main/README.md) "
        "[anchor](#top) [web](https://x.test/a)\n"
    )
    assert sync_skill.main(["--check"], root=tmp_path) == 0

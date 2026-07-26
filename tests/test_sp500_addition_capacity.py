from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

import sp500_addition_capacity as capacity
import sp500_addition_capacity_inspection as inspection


LISTING = b"""
<div class="wd_search_count">Your search returned 2 results</div>
<div class="wd_date">Oct 1, 2020</div>
<div class="wd_title"><a href="https://press.spglobal.com/2020-10-01-Pool-Set-to-Join-S-P-500">Pool Set to Join S&amp;P 500</a></div>
<div class="wd_date">Nov 16, 2020</div>
<div class="wd_title"><a href="https://press.spglobal.com/2020-11-16-Tesla-Set-to-Join-S-P-500">Tesla Set to Join S&amp;P 500</a></div>
"""


RELEASE = b"""
<html><body>
<table>
<tr><th>Effective Date</th><th>Index Name</th><th>Action</th><th>Company Name</th><th>Ticker</th><th>GICS Sector</th></tr>
<tr><td>October 7, 2020</td><td>S&amp;P 500</td><td>Addition</td><td>Pool Corp.</td><td>POOL</td><td>Consumer Discretionary</td></tr>
<tr><td>October 7, 2020</td><td>S&amp;P MidCap 400</td><td>Deletion</td><td>Pool Corp.</td><td>POOL</td><td>Consumer Discretionary</td></tr>
</table>
<!-- ITEMDATE: 2020-10-01 19:06:00 EDT -->
</body></html>
"""

RELEASE_WITH_INHERITED_DATE = b"""
<html><body>
<table>
<tr><th>Effective Date</th><th>Index Name</th><th>Action</th><th>Company Name</th><th>Ticker</th><th>GICS Sector</th></tr>
<tr><td>Mar 22, 2021</td><td>S&amp;P 500</td><td>Addition</td><td>NXP Semiconductors</td><td>NXPI</td><td>Information Technology</td></tr>
<tr><td></td><td>S&amp;P 500</td><td>Addition</td><td>Penn National Gaming</td><td>PENN</td><td>Consumer Discretionary</td></tr>
</table>
<!-- ITEMDATE: 2021-03-12 19:10:00 EST -->
</body></html>
"""

LEGACY_RELEASE = b"""
<html><body>
<p>Hilton Worldwide Holdings Inc. (NYSE: HLT) will replace Yahoo.</p>
<p>Align Technology Inc. (NASD: ALGN) and ANSYS Inc. (NASD: ANSS)
will replace other constituents.</p>
<table>
<tr><th>S&amp;P 500 INDEX - June 19, 2017</th></tr>
<tr><th></th><th>COMPANY</th><th>GICS ECONOMIC SECTOR</th></tr>
<tr><td>ADDED</td><td>Hilton Worldwide</td><td>Consumer Discretionary</td></tr>
<tr><td></td><td>Align Technology</td><td>Health Care</td></tr>
<tr><td></td><td>ANSYS</td><td>Information Technology</td></tr>
<tr><td>DELETED</td><td>Yahoo!</td><td>Information Technology</td></tr>
</table>
<!-- ITEMDATE: 2017-06-09 18:16:00 EDT -->
</body></html>
"""


def _write_artifact(path: Path, value: dict) -> Path:
    value["artifact_sha256"] = capacity._self_hash(value)
    hashed_path = path.with_name(
        f"{path.stem}-{value['artifact_sha256']}{path.suffix}"
    )
    hashed_path.parent.mkdir(parents=True, exist_ok=True)
    hashed_path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
    return hashed_path


def test_archive_tasks_are_complete_and_deterministic() -> None:
    tasks = capacity.archive_tasks()
    assert len(tasks) == 16
    assert tasks[0]["year"] == 2010
    assert tasks[-1]["year"] == 2025
    assert len({row["task_id"] for row in tasks}) == 16
    assert all("titles_only=1" in row["url"] for row in tasks)


def test_parse_archive_index_retains_exact_denominator() -> None:
    count, rows = capacity.parse_archive_index(LISTING, expected_year=2020)
    assert count == 2
    assert [row["listed_date"] for row in rows] == [
        "2020-10-01",
        "2020-11-16",
    ]
    assert rows[0]["title"] == "Pool Set to Join S&P 500"
    assert rows[0]["url_sha256"] == hashlib.sha256(
        rows[0]["url"].encode()
    ).hexdigest()


def test_parse_archive_index_accepts_explicit_zero_result_page() -> None:
    raw = b'<div class="wd_search_count">Your search returned no results</div>'
    assert capacity.parse_archive_index(raw, expected_year=2010) == (0, [])


def test_archive_parser_rejects_truncated_query() -> None:
    raw = LISTING.replace(b"2 results", b"101 results")
    with pytest.raises(capacity.Sp500AdditionCapacityError, match="truncate"):
        capacity.parse_archive_index(raw, expected_year=2020)


def test_content_addressed_cache_round_trip_is_stable(tmp_path: Path) -> None:
    cache = tmp_path / "release.html.gz"
    capacity._cache_bytes(cache, RELEASE)
    first = cache.read_bytes()
    capacity._cache_bytes(cache, RELEASE)
    assert cache.read_bytes() == first
    assert capacity._read_cached(cache) == RELEASE


def test_release_parser_extracts_only_structured_sp500_additions() -> None:
    parsed = capacity.parse_release(
        RELEASE,
        source_url="https://press.spglobal.com/2020-10-01-example",
        listed_date="2020-10-01",
    )
    assert parsed["terminal_reason"] == "ELIGIBLE_SP500_ADDITION"
    assert parsed["eligible_events"] == [
        {
            "announcement_at": "2020-10-01T19:06:00-04:00",
            "announcement_date": "2020-10-01",
            "effective_date": "2020-10-07",
            "index_name": "S&P 500",
            "action": "Addition",
            "company_name": "Pool Corp.",
            "ticker": "POOL",
            "source_url": "https://press.spglobal.com/2020-10-01-example",
        }
    ]


def test_release_parser_inherits_blank_effective_date_within_table() -> None:
    parsed = capacity.parse_release(
        RELEASE_WITH_INHERITED_DATE,
        source_url="https://press.spglobal.com/2021-03-12-example",
        listed_date="2021-03-12",
    )
    assert [
        (row["ticker"], row["effective_date"])
        for row in parsed["eligible_events"]
    ] == [("NXPI", "2021-03-22"), ("PENN", "2021-03-22")]


def test_release_parser_links_legacy_summary_rows_to_inline_tickers() -> None:
    parsed = capacity.parse_release(
        LEGACY_RELEASE,
        source_url="https://press.spglobal.com/2017-06-09-example",
        listed_date="2017-06-09",
    )
    assert [
        (row["company_name"], row["ticker"], row["effective_date"])
        for row in parsed["eligible_events"]
    ] == [
        ("Align Technology", "ALGN", "2017-06-19"),
        ("ANSYS", "ANSS", "2017-06-19"),
        ("Hilton Worldwide", "HLT", "2017-06-19"),
    ]


@pytest.mark.parametrize(
    ("raw_date", "expected"),
    [
        ("23-Sep-24", "2024-09-23"),
        ("Sept 30, 2024", "2024-09-30"),
        ("Sept. 22, 2025", "2025-09-22"),
        ("Friday, November 1, 2024", "2024-11-01"),
    ],
)
def test_release_parser_normalizes_official_date_spellings(
    raw_date: str, expected: str
) -> None:
    raw = RELEASE.replace(
        b"October 7, 2020", raw_date.encode()
    ).replace(
        b"2020-10-01 19:06:00 EDT",
        b"2020-01-01 19:06:00 EST",
    )
    parsed = capacity.parse_release(
        raw,
        source_url="https://press.spglobal.com/2020-01-01-example",
        listed_date="2020-01-01",
    )
    assert parsed["eligible_events"][0]["effective_date"] == expected


def test_release_parser_retains_tba_effective_date_as_ineligible() -> None:
    raw = RELEASE.replace(b"October 7, 2020", b"TBA")
    parsed = capacity.parse_release(
        raw,
        source_url="https://press.spglobal.com/2020-10-01-example",
        listed_date="2020-10-01",
    )
    assert parsed["eligible_events"] == []
    assert parsed["terminal_reason"] == "MISSING_EFFECTIVE_DATE"
    assert parsed["ineligible_structured_rows"] == [
        {
            "ticker": "POOL",
            "raw_effective_date": "TBA",
            "terminal_reason": "MISSING_EFFECTIVE_DATE",
        }
    ]


def test_release_parser_retains_unlinked_legacy_company_as_ineligible() -> None:
    raw = LEGACY_RELEASE.replace(
        b"<td>Hilton Worldwide</td>", b"<td>Renamed Company</td>"
    )
    parsed = capacity.parse_release(
        raw,
        source_url="https://press.spglobal.com/2017-06-09-example",
        listed_date="2017-06-09",
    )
    assert any(
        row["company_name"] == "Renamed Company"
        and row["terminal_reason"] == "UNRESOLVED_TICKER_IDENTITY"
        for row in parsed["ineligible_structured_rows"]
    )


def test_release_parser_preserves_irrelevant_page() -> None:
    raw = (
        b"<html><body><p>S&amp;P 500 buybacks</p>"
        b"<!-- ITEMDATE: 2020-10-01 07:00:00 EDT --></body></html>"
    )
    parsed = capacity.parse_release(
        raw,
        source_url="https://press.spglobal.com/2020-10-01-buybacks",
        listed_date="2020-10-01",
    )
    assert parsed["eligible_events"] == []
    assert parsed["terminal_reason"] == "NO_STRUCTURED_SP500_ADDITION"


def test_release_parser_rejects_same_day_effective_date() -> None:
    raw = RELEASE.replace(b"October 7, 2020", b"October 1, 2020")
    with pytest.raises(capacity.Sp500AdditionCapacityError, match="not later"):
        capacity.parse_release(
            raw,
            source_url="https://press.spglobal.com/2020-10-01-example",
            listed_date="2020-10-01",
        )


def test_duplicate_resolution_keeps_earliest_publication() -> None:
    releases = [
        {
            "eligible_events": [
                {
                    "announcement_at": "2020-12-11T20:00:00-05:00",
                    "ticker": "TSLA",
                    "effective_date": "2020-12-21",
                    "source_url": "https://example.test/later",
                }
            ]
        },
        {
            "eligible_events": [
                {
                    "announcement_at": "2020-11-16T17:00:00-05:00",
                    "ticker": "TSLA",
                    "effective_date": "2020-12-21",
                    "source_url": "https://example.test/earlier",
                }
            ]
        },
    ]
    retained, duplicates = inspection._deduplicate_events(releases)
    assert duplicates == 1
    assert retained[0]["source_url"].endswith("/earlier")


def test_collect_index_requires_inspected_predecessors(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    contract_path = _write_artifact(
        tmp_path / "contract.json",
        {
            "schema_version": 1,
            "artifact_kind": "sp500-addition-index-query-contract",
            "state": "INDEX_QUERY_CONTRACT_FROZEN",
            "tasks": [],
        },
    )
    predecessor_path = _write_artifact(
        tmp_path / "inspection.json",
        {
            "schema_version": 1,
            "artifact_kind": "sp500-addition-index-contract-inspection",
            "state": "WRONG",
            "contract_sha256": json.loads(contract_path.read_text())[
                "artifact_sha256"
            ],
        },
    )
    monkeypatch.setattr(capacity, "_require_committed", lambda _paths: None)
    with pytest.raises(
        capacity.Sp500AdditionCapacityError,
        match="predecessor chain",
    ):
        capacity.collect_index(
            contract_path,
            predecessor_path,
            collected_at="2026-07-26T00:00:00Z",
            root=tmp_path,
        )

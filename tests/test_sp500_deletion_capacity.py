from __future__ import annotations

import sp500_deletion_capacity as capacity


STRUCTURED_RELEASE = b"""
<html><body>
<table>
<tr><th>Effective Date</th><th>Index Name</th><th>Action</th><th>Company Name</th><th>Ticker</th><th>GICS Sector</th></tr>
<tr><td>October 7, 2020</td><td>S&amp;P 500</td><td>Addition</td><td>Pool Corp.</td><td>POOL</td><td>Consumer Discretionary</td></tr>
<tr><td></td><td>S&amp;P 500</td><td>Deletion</td><td>Kohl's</td><td>KSS</td><td>Consumer Discretionary</td></tr>
<tr><td></td><td>S&amp;P MidCap 400</td><td>Deletion</td><td>Other</td><td>OTHR</td><td>Industrials</td></tr>
</table>
<!-- ITEMDATE: 2020-10-01 19:06:00 EDT -->
</body></html>
"""


LEGACY_RELEASE = b"""
<html><body>
<p>Hilton Worldwide Holdings Inc. (NYSE: HLT) will replace Yahoo Inc.
(NASDAQ: YHOO).</p>
<table>
<tr><th>S&amp;P 500 INDEX - June 19, 2017</th></tr>
<tr><th></th><th>COMPANY</th><th>GICS ECONOMIC SECTOR</th></tr>
<tr><td>ADDED</td><td>Hilton Worldwide</td><td>Consumer Discretionary</td></tr>
<tr><td>DELETED</td><td>Yahoo</td><td>Information Technology</td></tr>
</table>
<!-- ITEMDATE: 2017-06-09 18:16:00 EDT -->
</body></html>
"""


DUAL_CLASS_RELEASE = b"""
<html><body>
<table>
<tr><th>Effective Date</th><th>Index Name</th><th>Action</th><th>Company Name</th><th>Ticker</th><th>GICS Sector</th></tr>
<tr><td>June 21, 2022</td><td>S&amp;P 500</td><td>Deletion</td><td>Under Armour</td><td>UA/UAA</td><td>Consumer Discretionary</td></tr>
</table>
<!-- ITEMDATE: 2022-06-03 17:15:00 EDT -->
</body></html>
"""


def test_parser_extracts_only_structured_sp500_deletions():
    parsed = capacity.parse_release(
        STRUCTURED_RELEASE,
        source_url="https://press.spglobal.com/2020-10-01-example",
        listed_date="2020-10-01",
    )

    assert parsed["terminal_reason"] == "ELIGIBLE_SP500_DELETION"
    assert parsed["eligible_events"] == [
        {
            "announcement_at": "2020-10-01T19:06:00-04:00",
            "announcement_date": "2020-10-01",
            "effective_date": "2020-10-07",
            "index_name": "S&P 500",
            "action": "Deletion",
            "company_name": "Kohl's",
            "ticker": "KSS",
            "source_url": "https://press.spglobal.com/2020-10-01-example",
        }
    ]


def test_parser_expands_explicit_dual_class_deletion_tickers():
    parsed = capacity.parse_release(
        DUAL_CLASS_RELEASE,
        source_url="https://press.spglobal.com/2022-06-03-example",
        listed_date="2022-06-03",
    )

    assert [
        (row["company_name"], row["ticker"], row["effective_date"])
        for row in parsed["eligible_events"]
    ] == [
        ("Under Armour", "UA", "2022-06-21"),
        ("Under Armour", "UAA", "2022-06-21"),
    ]


def test_parser_links_legacy_deleted_company_to_inline_ticker():
    parsed = capacity.parse_release(
        LEGACY_RELEASE,
        source_url="https://press.spglobal.com/2017-06-09-example",
        listed_date="2017-06-09",
    )

    assert [
        (row["company_name"], row["ticker"], row["effective_date"])
        for row in parsed["eligible_events"]
    ] == [("Yahoo", "YHOO", "2017-06-19")]


def test_parser_preserves_missing_timestamp_as_zero_capacity():
    parsed = capacity.parse_release(
        b"<html><body>S&amp;P 500 deletion</body></html>",
        source_url="https://press.spglobal.com/missing",
        listed_date="2020-10-01",
    )

    assert parsed["eligible_events"] == []
    assert parsed["terminal_reason"] == "MISSING_PUBLICATION_TIMESTAMP"

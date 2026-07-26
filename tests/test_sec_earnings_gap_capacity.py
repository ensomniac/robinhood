from datetime import datetime
from zoneinfo import ZoneInfo

import sec_earnings_gap_capacity as subject


EASTERN = ZoneInfo("America/New_York")


def test_point_in_time_cik_mapping_fails_closed():
    inventory = {
        "content_sha256": "source",
        "partitions": {
            "development": ["2024-01-03"],
            "embargo": [],
            "confirmation": ["2024-02-02"],
        },
        "candidates_by_date": {
            "2024-01-03": [
                {"symbol": "AAA", "instrument_id": "FIGI-COMPOSITE:ONE"},
                {"symbol": "MISS", "instrument_id": "FIGI-COMPOSITE:TWO"},
            ],
            "2024-02-02": [
                {"symbol": "AAA", "instrument_id": "FIGI-COMPOSITE:ONE"},
            ],
        },
    }
    security = [
        {
            "symbol": "AAA",
            "instrument_id": "FIGI-COMPOSITE:ONE",
            "observed_dates": ["2024-01-03"],
            "source": {"cik": "123"},
        }
    ]
    plan = subject.build_request_plan(inventory, security)
    assert plan["mapped_pairs"] == [
        {
            "date": "2024-01-03",
            "partition": "development",
            "symbol": "AAA",
            "instrument_id": "FIGI-COMPOSITE:ONE",
            "cik": "0000000123",
        }
    ]
    assert len(plan["unmatched_pairs"]) == 2
    assert plan["unique_ciks"] == ["0000000123"]


def _filing(
    accepted: datetime,
    *,
    form: str = "8-K",
    items: str = "2.02,9.01",
):
    return {
        "form": form,
        "acceptanceDateTime": accepted.isoformat(),
        "items": items,
        "accessionNumber": "0001-24-000001",
        "primaryDocument": "earnings.htm",
    }


def test_filing_window_is_after_prior_close_through_0925():
    rows = [
        _filing(datetime(2024, 1, 2, 16, 0, tzinfo=EASTERN)),
        _filing(datetime(2024, 1, 2, 16, 1, tzinfo=EASTERN)),
        _filing(datetime(2024, 1, 3, 9, 25, tzinfo=EASTERN)),
        _filing(datetime(2024, 1, 3, 9, 26, tzinfo=EASTERN)),
        _filing(
            datetime(2024, 1, 2, 17, 0, tzinfo=EASTERN),
            items="2.02,3.02,9.01",
        ),
        _filing(
            datetime(2024, 1, 2, 17, 0, tzinfo=EASTERN),
            form="8-K/A",
        ),
    ]
    eligible = subject.eligible_filings(
        rows,
        previous_session="2024-01-02",
        signal_date="2024-01-03",
    )
    assert [row["accepted_at"] for row in eligible] == [
        datetime(2024, 1, 2, 16, 1, tzinfo=EASTERN).isoformat(),
        datetime(2024, 1, 3, 9, 25, tzinfo=EASTERN).isoformat(),
    ]


def test_capacity_counts_distinct_signal_days():
    plan = {
        "mapped_pairs": [
            {
                "date": "2024-01-03",
                "partition": "development",
                "symbol": "AAA",
                "instrument_id": "ONE",
                "cik": "0000000001",
            },
            {
                "date": "2024-01-03",
                "partition": "development",
                "symbol": "BBB",
                "instrument_id": "TWO",
                "cik": "0000000002",
            },
        ]
    }
    accepted = datetime(2024, 1, 2, 17, 0, tzinfo=EASTERN)
    rows = {
        "0000000001": [_filing(accepted)],
        "0000000002": [_filing(accepted)],
    }
    result = subject.build_eligibility(
        plan,
        rows,
        ["2024-01-02", "2024-01-03"],
    )
    assert len(result["eligible_pairs"]) == 2
    assert result["signal_days"]["development"] == ["2024-01-03"]


def test_history_file_overlap_is_exact():
    payload = {
        "filings": {
            "files": [
                {
                    "name": "old.json",
                    "filingFrom": "2010-01-01",
                    "filingTo": "2012-12-31",
                },
                {
                    "name": "needed.json",
                    "filingFrom": "2022-10-01",
                    "filingTo": "2024-02-01",
                },
                {
                    "name": "future.json",
                    "filingFrom": "2025-01-01",
                    "filingTo": "2026-01-01",
                },
            ]
        }
    }
    assert subject.relevant_history_files(payload) == ["needed.json"]

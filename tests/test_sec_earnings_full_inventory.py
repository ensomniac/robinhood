from datetime import datetime
from zoneinfo import ZoneInfo

import sec_earnings_full_inventory as subject


EASTERN = ZoneInfo("America/New_York")


def test_signal_window_accepts_only_after_close_or_premarket():
    ends, windows = subject._signal_windows(
        ["2023-01-03", "2023-01-04", "2023-01-05"]
    )
    assert subject.signal_for_acceptance(
        datetime(2023, 1, 3, 16, 1, tzinfo=EASTERN),
        ends=ends,
        windows=windows,
    ) == ("2023-01-04", "development")
    assert subject.signal_for_acceptance(
        datetime(2023, 1, 4, 9, 25, tzinfo=EASTERN),
        ends=ends,
        windows=windows,
    ) == ("2023-01-04", "development")
    assert (
        subject.signal_for_acceptance(
            datetime(2023, 1, 4, 10, 0, tzinfo=EASTERN),
            ends=ends,
            windows=windows,
        )
        is None
    )


def test_point_in_time_listings_require_composite_interval():
    rows = [
        {
            "instrument_id": "FIGI-COMPOSITE:ONE",
            "symbol": "AAA",
            "primary_exchange": "XNAS",
            "valid_from": "2023-01-01",
            "valid_to": "2023-06-30",
        },
        {
            "instrument_id": "FIGI-COMPOSITE:TWO",
            "symbol": "BBB",
            "primary_exchange": "XNYS",
            "valid_from": "2023-07-01",
            "valid_to": "2024-12-31",
        },
    ]
    assert subject._listings(rows, "2023-03-01") == [
        {
            "instrument_id": "FIGI-COMPOSITE:ONE",
            "symbol": "AAA",
            "primary_exchange": "XNAS",
        }
    ]
    assert subject._listings(rows, "2023-06-31") == []


def test_item_202_event_deduplicates_accession_and_excludes_dilution():
    ends, windows = subject._signal_windows(
        ["2023-01-03", "2023-01-04", "2023-01-05"]
    )
    base = {
        "form": "8-K",
        "acceptanceDateTime": "2023-01-03T17:00:00-05:00",
        "items": "2.02,9.01",
        "primaryDocument": "earnings.htm",
    }
    rows = [
        {**base, "accessionNumber": "one"},
        {**base, "accessionNumber": "duplicate"},
        {**base, "accessionNumber": "duplicate"},
        {
            **base,
            "accessionNumber": "dilution",
            "items": "2.02,3.02,9.01",
        },
        {**base, "accessionNumber": "amendment", "form": "8-K/A"},
    ]
    events = subject._filing_events(rows, ends=ends, windows=windows)
    assert [event["accession"] for event in events] == ["one"]


def test_security_master_groups_only_common_composite_ciks():
    rows = [
        {
            "security_type": "COMMON",
            "instrument_id": "FIGI-COMPOSITE:ONE",
            "source": {"cik": "123"},
        },
        {
            "security_type": "ETF",
            "instrument_id": "FIGI-COMPOSITE:TWO",
            "source": {"cik": "456"},
        },
        {
            "security_type": "COMMON",
            "instrument_id": "MASSIVE-FALLBACK:THREE",
            "source": {"cik": "789"},
        },
    ]
    grouped = subject._security_by_cik(rows)
    assert list(grouped) == ["0000000123"]

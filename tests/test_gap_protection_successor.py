from __future__ import annotations

from datetime import date, timedelta

import gap_protection_successor as successor
import outcome_exposure


def _detail(count: int = 100) -> dict:
    start = date(2023, 1, 1)
    dates = {}
    for index in range(count):
        day = (start + timedelta(days=index)).isoformat()
        dates[day] = {
            "evaluations": [
                {
                    "symbol": f"S{index:03d}",
                    "instrument_id": f"instrument-{index}",
                    "primary_exchange": "XNYS",
                    "open_price": 104.0,
                    "prior_close": 100.0,
                }
            ]
        }
    return {"dates": dates}


def test_successor_grid_is_complete_and_bounded():
    assert successor._trial_count() == 32
    assert successor.PARAMETER_GRID["maximum_structural_stop_fraction"] == [
        0.03,
        0.04,
    ]
    assert successor.PARAMETER_GRID["opening_range_minutes"] == [5]


def test_allocation_is_chronological_and_preserves_five_session_embargo():
    allocation = successor._allocation(_detail(), exposure_records=[])

    partitions = allocation["partitions"]
    assert len(partitions["development"]) == 57
    assert len(partitions["embargo"]) == 5
    assert len(partitions["confirmation"]) == 38
    assert (
        partitions["development"]
        + partitions["embargo"]
        + partitions["confirmation"]
        == allocation["eligible_dates"]
    )


def test_prior_exact_exposure_discards_whole_date_before_allocation():
    detail = _detail(101)
    exposed_day = sorted(detail["dates"])[25]
    record = outcome_exposure.build_record(
        exposure_id="gap-successor-test-exposure",
        campaign_id="test",
        lane="development",
        recorded_at="2026-07-24T12:00:00Z",
        source_path="tests/test_gap_protection_successor.py",
        source_sha256="a" * 64,
        scope={"dates": [exposed_day], "symbols": ["S025"]},
    )

    allocation = successor._allocation(detail, exposure_records=[record])

    assert exposed_day not in allocation["eligible_dates"]
    assert {
        "date": exposed_day,
        "reason": "prior_outcome_exposure",
        "overlap_count": 1,
    } in allocation["excluded_dates"]

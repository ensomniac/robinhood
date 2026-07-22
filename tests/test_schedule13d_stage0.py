from __future__ import annotations

from datetime import date, timedelta
from unittest import mock

import schedule13d_stage0 as stage0


def _events(count: int, sessions: list[str]) -> list[dict[str, object]]:
    result = []
    for index in range(count):
        accepted = date.fromisoformat(sessions[index * 6]) - timedelta(days=1)
        result.append(
            {
                "event_ordinal": index,
                "event_accession": f"accession-{index:03d}",
                "accepted_at_eastern": f"{accepted.isoformat()}T12:00:00",
                "accepted_date": accepted.isoformat(),
                "subject_cik": str(1000 + index),
                "symbol": f"S{index}",
                "security_class": "Common Stock",
                "control_category": "board_representation",
                "symbol_source": "event_filing",
            }
        )
    return result


def test_xnys_calendar_observes_holidays_and_carter_closure():
    sessions = set(stage0._xnys_sessions(date(2024, 12, 20), date(2025, 1, 15)))
    assert "2024-12-25" not in sessions
    assert "2025-01-01" not in sessions
    assert "2025-01-09" not in sessions
    assert "2025-01-10" in sessions


def test_partition_is_chronological_disjoint_and_embargoed():
    sessions = stage0._xnys_sessions(date(2021, 1, 1), date(2025, 1, 1))
    value = stage0._partition(_events(100, sessions[20:]), sessions)
    assert value["stage0_signal_count"] == 40
    assert value["confirmation_signal_count"] == 20
    assert value["development_signal_count"] >= 30
    phases = value["partitions"]
    ordinals = [
        row["event_ordinal"]
        for phase in ("stage0", "development", "embargo_excluded", "confirmation")
        for row in phases[phase]
    ]
    assert len(ordinals) == len(set(ordinals))


def test_request_contract_freezes_15_atr_rows_and_five_outcome_rows():
    sessions = stage0._xnys_sessions(date(2022, 1, 1), date(2023, 1, 1))
    entry = sessions[30]
    request = stage0._request_contract(
        {
            "event_ordinal": 1,
            "event_accession": "a",
            "symbol": "ABC",
            "accepted_at_eastern": f"{sessions[29]}T17:00:00",
            "entry_session": entry,
        },
        sessions,
    )
    assert len(request["atr_sessions"]) == 15
    assert len(request["outcome_sessions"]) == 5
    assert request["outcome_sessions"][0] == entry


def test_outcome_uses_gap_fill_and_adverse_costs():
    atr = [
        {"date": f"2025-01-{index + 1:02d}", "o": 100, "h": 102, "l": 99, "c": 101}
        for index in range(15)
    ]
    outcome = [
        {"date": "2025-02-03", "o": 100, "h": 101, "l": 99, "c": 100},
        {"date": "2025-02-04", "o": 94, "h": 96, "l": 93, "c": 95},
        {"date": "2025-02-05", "o": 95, "h": 96, "l": 94, "c": 95},
        {"date": "2025-02-06", "o": 95, "h": 96, "l": 94, "c": 95},
        {"date": "2025-02-07", "o": 95, "h": 96, "l": 94, "c": 95},
    ]
    result = stage0._outcome(atr, outcome, 20)
    assert result["exit_reason"] == "stop_gap"
    assert result["exit_date"] == "2025-02-04"
    assert result["net_r"] < -1


def test_contract_remains_zero_result_and_freezes_40_requests():
    sessions = stage0._xnys_sessions()
    partition = stage0._partition(_events(100, sessions[20:]), sessions)
    capacity_result = {
        "result_sha256": stage0.CAPACITY_RESULT_SHA256,
        "verified_event_count": 120,
    }
    capacity_inspection = {"inspection_sha256": stage0.CAPACITY_INSPECTION_SHA256}
    with (
        mock.patch.object(stage0, "_load_capacity_evidence", return_value=(capacity_result, capacity_inspection)),
        mock.patch.object(stage0, "_verified_events", return_value=_events(100, sessions[20:])),
        mock.patch.object(stage0, "_partition", return_value=partition),
        mock.patch.object(stage0, "sha256_file", return_value="a" * 64),
    ):
        contract = stage0.build_contract(require_published=False)
    assert len(contract["stage0_input_requests"]) == 40
    assert contract["market_outcomes_accessed"] is False
    assert contract["returns_computed"] == 0
    assert contract["access_contract"]["return_evaluation_before_input_inspection_permitted"] is False

from __future__ import annotations

import json
import tempfile
from datetime import date, timedelta
from pathlib import Path

import continuous_strategy_discovery as continuous
import continuous_strategy_discovery_inspection as inspection
import dense_strategy_plugin
import strategy_discovery


class _CalendarResponse:
    status_code = 200

    def __init__(self, rows: list[dict[str, str]]):
        self._rows = rows

    def json(self) -> list[dict[str, str]]:
        return self._rows


def _weekdays() -> list[dict[str, str]]:
    current = date.fromisoformat(continuous.CALENDAR_START)
    end = date.fromisoformat(continuous.CALENDAR_END)
    rows: list[dict[str, str]] = []
    while current <= end:
        if current.weekday() < 5:
            rows.append(
                {
                    "date": current.isoformat(),
                    "open": "09:30",
                    "close": "16:00",
                }
            )
        current += timedelta(days=1)
    return rows


def test_existing_family_successor_freezes_without_waiting_or_reusing_v1(
    monkeypatch,
):
    monkeypatch.setattr(
        continuous.outcome_exposure,
        "read_index",
        lambda: [],
    )
    monkeypatch.setattr(
        continuous.outcome_exposure,
        "audit",
        lambda: {"index_sha256": "a" * 64},
    )
    with tempfile.TemporaryDirectory(dir=continuous.PROJECT_ROOT) as directory:
        work = Path(directory)
        root = work / "continuous"
        calendar_root = root / continuous.SUCCESSOR_ID / "calendar"
        calendar_path = work / "session-calendar.json"
        source_path = work / "session-calendar-source.json"
        env_path = work / ".env"
        env_path.write_text(
            "ALPACA_KEY=test-key\nALPACA_SECRET=test-secret\n",
            encoding="utf-8",
        )

        contract_path, contract = continuous.freeze_calendar_contract(
            created_at="2026-07-23T12:00:00-04:00",
            root=calendar_root,
            calendar_path=calendar_path,
            source_path=source_path,
            enforce_commit=False,
        )
        assert contract["new_mechanism_family_slot_consumed"] is False
        assert contract["target_outcomes_accessed"] is False

        inspection.inspect_contract(
            contract_path,
            inspected_at="2026-07-23T12:00:01-04:00",
            root=calendar_root,
            enforce_commit=False,
        )
        status_path, status = continuous.collect_calendar(
            contract_path,
            collected_at="2026-07-23T12:00:02-04:00",
            root=calendar_root,
            env_path=env_path,
            getter=lambda *args, **kwargs: _CalendarResponse(_weekdays()),
            enforce_commit=False,
        )
        assert status["provider_requests"] == 1
        assert status["market_prices_accessed"] is False
        inspection.inspect_calendar(
            status_path,
            inspected_at="2026-07-23T12:00:03-04:00",
            root=calendar_root,
            enforce_commit=False,
        )

        original_calendar_root = continuous.CALENDAR_ROOT
        continuous.CALENDAR_ROOT = calendar_root
        try:
            family_path, family, capacity_path = (
                continuous.freeze_successor_contract(
                    created_at="2026-07-23T12:00:04-04:00",
                    root=root,
                    calendar_path=calendar_path,
                    enforce_commit=False,
                )
            )
        finally:
            continuous.CALENDAR_ROOT = original_calendar_root

        validated = strategy_discovery._validate_family_contract(family)
        continuous.validate_existing_successor_contract(
            validated, enforce_commit=False
        )
        assert family_path.is_file()
        assert capacity_path.is_file()
        assert family["mechanism_family"] == "broad-etf-trend-pullback"
        assert family["research_generation"] == "existing_family_successor"
        assert family["historical_data_contract"] == {
            "daily_provider": "alpaca",
            "daily_endpoint": "/v2/stocks/{symbol}/bars",
            "daily_feed": "sip",
            "daily_adjustment": "raw",
            "split_provider": "massive",
            "provider_substitutions_allowed": False,
        }
        assert family["new_mechanism_family_slot_consumed"] is False
        assert family["predecessor"]["promotion_evidence_reused"] is False
        assert len(validated["trial_family"]) == 32
        assert len(family["development_dates"]) == 1_000
        assert len(family["confirmation_dates"]) == 500
        assert max(family["confirmation_dates"]) < "2023-01-01"
        assert set(family["development_dates"]).isdisjoint(
            family["confirmation_dates"]
        )
        monkeypatch.setattr(dense_strategy_plugin, "_require_committed", lambda path: None)
        _preflight_path, preflight = strategy_discovery.run_preflight(
            family_path,
            root=work / "discovery",
            enforce_commit=False,
        )
        assert preflight["state"] == "CAPACITY_READY"
        assert preflight["outcomes_accessed"] is False
        assert preflight["preflight_details"]["external_dataset_opened"] is False


def test_status_keeps_rolling_batch_separate_from_continuous_lane(tmp_path: Path):
    original_calendar_root = continuous.CALENDAR_ROOT
    continuous.CALENDAR_ROOT = tmp_path / "calendar"
    try:
        status = continuous.build_status(root=tmp_path)
    finally:
        continuous.CALENDAR_ROOT = original_calendar_root
    assert status["state"] == "READY_TO_FREEZE_SUCCESSOR"
    assert status["successor"]["family_id"] == (
        "liquid-equity-market-residual-reversal"
    )
    assert status["successor"]["calendar_wait_required"] is False
    assert status["successor"]["new_mechanism_family_slot_consumed"] is False
    assert status["new_family_batch"]["state"] == (
        "READY_FOR_DISJOINT_EVIDENCE_FREEZE"
    )
    assert status["new_family_batch"]["activation_permitted"] is True


def test_status_turns_rejected_successor_into_non_waiting_readiness_work():
    status = continuous.build_status()

    assert status["state"] == "EXISTING_FAMILY_QUEUE_EXHAUSTED"
    assert status["successor"]["development_disposition"] == "REJECTED"
    assert status["successor"]["calendar_wait_required"] is False
    assert status["successor"]["outcome_access_wait_required"] is False
    assert status["successor"]["outcome_access_prerequisites_remaining"] is True
    readiness = status["preactivation_readiness"]
    assert readiness["state"] in {
        "PREACTIVATION_BLOCKED",
        "ALLOCATION_CONTRACT_READY",
    }
    assert readiness["allocation_capacity"]["state"] == (
        "ALLOCATION_CAPACITY_READY"
    )
    assert readiness["allocation_capacity"]["ready"] is True
    assert all(
        "contiguous untouched target-evidence run" not in blocker
        for blocker in readiness["blockers"]
    )
    assert readiness["preactivation_work_complete"] is not bool(
        readiness["blockers"]
    )
    assert readiness["credentials_ready"] is True
    assert readiness["provider_access_permitted"] is False
    assert readiness["target_outcome_access_permitted"] is False
    assert readiness["broker_actions_permitted"] is False
    assert status["new_family_batch"]["state"] == (
        "READY_FOR_DISJOINT_EVIDENCE_FREEZE"
    )


def test_calendar_inspection_resolution_preserves_superseded_calendar(
    tmp_path: Path,
    monkeypatch,
):
    monkeypatch.setattr(continuous, "PROJECT_ROOT", tmp_path)
    root = tmp_path / "calendar-artifacts"
    old_calendar = tmp_path / "old-calendar.json"
    current_calendar = tmp_path / "current-calendar.json"
    old_calendar.write_text("[]\n", encoding="utf-8")
    current_calendar.write_text(
        json.dumps([{"date": "2022-01-03"}]) + "\n",
        encoding="utf-8",
    )
    for calendar in (old_calendar, current_calendar):
        strategy_discovery._write_artifact(
            {
                "schema_version": 1,
                "artifact_kind": continuous.CALENDAR_DATA_INSPECTION_KIND,
                "state": "CALENDAR_INSPECTED_READY",
                "calendar_path": continuous._repo_path(calendar),
                "calendar_sha256": strategy_discovery._file_hash(calendar),
            },
            root / "data-inspection",
            "calendar-inspection",
        )

    path, selected = continuous._data_inspection_for_calendar(
        current_calendar,
        root=root,
        enforce_commit=False,
    )

    assert path.is_file()
    assert selected["calendar_path"] == "current-calendar.json"

from __future__ import annotations

import tempfile
from datetime import date, timedelta
from pathlib import Path

import continuous_strategy_discovery as continuous
import continuous_strategy_discovery_inspection as inspection
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


def test_existing_family_successor_freezes_without_waiting_or_reusing_v1():
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
        assert family["new_mechanism_family_slot_consumed"] is False
        assert family["predecessor"]["promotion_evidence_reused"] is False
        assert len(validated["trial_family"]) == 32
        assert len(family["development_dates"]) == 1_000
        assert len(family["confirmation_dates"]) == 500
        assert max(family["confirmation_dates"]) < "2022-01-01"
        assert set(family["development_dates"]).isdisjoint(
            family["confirmation_dates"]
        )


def test_status_keeps_new_family_wait_separate_from_continuous_lane(tmp_path: Path):
    status = continuous.build_status(root=tmp_path)
    assert status["state"] == "READY_TO_FREEZE_CALENDAR"
    assert status["successor"]["calendar_wait_required"] is False
    assert status["successor"]["new_mechanism_family_slot_consumed"] is False
    assert status["new_family_batch"]["state"] == "WAITING_ISO_WEEK_RESET"

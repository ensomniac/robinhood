from __future__ import annotations

from datetime import date, timedelta

import pytest

import dense_session_calendar as calendar
import dense_session_calendar_inspection as inspection
import strategy_discovery


def _rows(count=1_600):
    current = date(2020, 1, 2)
    rows = []
    while len(rows) < count:
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


class FakeResponse:
    status_code = 200

    def __init__(self, rows):
        self.rows = rows

    def json(self):
        return self.rows


def _isolated_module(tmp_path, monkeypatch):
    implementation = tmp_path / "dense_session_calendar.py"
    inspector = tmp_path / "dense_session_calendar_inspection.py"
    implementation.write_text("calendar implementation\n", encoding="utf-8")
    inspector.write_text("calendar inspection\n", encoding="utf-8")
    monkeypatch.setattr(calendar, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(calendar, "INSPECTOR_PATH", inspector)
    monkeypatch.setattr(calendar, "__file__", str(implementation))
    monkeypatch.setattr(inspection, "PROJECT_ROOT", tmp_path)


def test_calendar_provider_access_fails_before_week_reset(tmp_path):
    with pytest.raises(calendar.DenseSessionCalendarError, match="closed until"):
        calendar.collect(
            tmp_path / "missing.json",
            as_of=date(2026, 7, 22),
            collected_at="2026-07-22T12:00:00-04:00",
            enforce_commit=False,
        )
    with pytest.raises(
        calendar.DenseSessionCalendarError,
        match="cannot be future-dated",
    ):
        calendar.collect(
            tmp_path / "missing.json",
            as_of=date(2026, 7, 27),
            actual_today=date(2026, 7, 22),
            collected_at="2026-07-27T12:00:00-04:00",
            enforce_commit=False,
        )


def test_extended_calendar_contract_collection_and_independent_capacity_inspection(
    tmp_path, monkeypatch
):
    _isolated_module(tmp_path, monkeypatch)
    root = tmp_path / "public"
    calendar_path = tmp_path / "historical/calendar.json"
    source_path = tmp_path / "historical/source.json"
    contract_path, contract = calendar.freeze_contract(
        created_at="2026-07-22T12:00:00-04:00",
        root=root,
        calendar_path=calendar_path,
        source_path=source_path,
        enforce_commit=False,
    )
    assert contract["provider_requests"] == 0
    _inspection_path, contract_inspection = calendar.inspect_contract(
        contract_path,
        inspected_at="2026-07-22T12:01:00-04:00",
        root=root,
        enforce_commit=False,
    )
    assert contract_inspection["state"] == "CALENDAR_CONTRACT_INSPECTED_READY"
    strategy_discovery._write_artifact(
        {
            "schema_version": 1,
            "artifact_kind": calendar.CONTRACT_INSPECTION_KIND,
            "campaign_id": calendar.batch.CAMPAIGN_ID,
            "state": "CALENDAR_CONTRACT_INSPECTED_READY",
            "contract_sha256": "f" * 64,
        },
        root / "contract-inspection",
        "superseded-calendar-contract-inspection",
    )
    env = tmp_path / ".env"
    env.write_text("ALPACA_KEY=test\nALPACA_SECRET=test\n", encoding="utf-8")
    collection_path, collected = calendar.collect(
        contract_path,
        as_of=date(2026, 7, 27),
        actual_today=date(2026, 7, 27),
        collected_at="2026-07-27T08:00:00-04:00",
        env_path=env,
        root=root,
        getter=lambda *args, **kwargs: FakeResponse(_rows()),
        enforce_commit=False,
    )
    assert collected["provider_requests"] == 1
    assert collected["target_outcomes_accessed"] is False
    index = tmp_path / "exposure.jsonl"
    _path, inspected = inspection.inspect(
        collection_path,
        inspected_at="2026-07-27T08:01:00-04:00",
        root=root,
        index_path=index,
        enforce_commit=False,
    )
    assert inspected["state"] == "CALENDAR_INSPECTED_READY"
    assert inspected["untouched_dense_allocation_sessions"] == 940
    assert inspected["target_outcomes_accessed"] is False

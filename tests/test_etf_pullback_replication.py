from __future__ import annotations

from datetime import date, timedelta
from types import SimpleNamespace

import dense_data_collection as collection
import etf_pullback_replication as replication


def _weekdays(count: int) -> list[dict[str, str]]:
    rows = []
    current = date(2008, 1, 2)
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


def test_replication_capacity_is_dense_and_pre2016():
    assert replication.TOTAL_SESSIONS == 1_905
    assert replication.DEVELOPMENT_SESSIONS == 1_200
    assert replication.CONFIRMATION_SESSIONS == 500
    assert len(replication.SYMBOLS) == 19
    assert replication.CALENDAR_END == "2015-12-31"


def test_calendar_normalization_retains_exact_full_sessions():
    rows = replication.normalize_calendar_rows(_weekdays(2_001))

    assert len(rows) == 2_001
    assert rows[0]["open_et"] == "09:30"
    assert rows[-1]["close_et"] == "16:00"
    assert rows[-1]["date"] <= replication.CALENDAR_END


def test_dense_collection_dispatches_declared_successor_validator(monkeypatch):
    calls = []
    module = SimpleNamespace(
        validate=lambda contract, *, enforce_commit: calls.append(
            (contract["successor_id"], enforce_commit)
        )
    )
    monkeypatch.setattr(
        collection.importlib,
        "import_module",
        lambda name: module if name == "replication_test_module" else None,
    )
    contract = {
        "research_generation": "existing_family_successor",
        "successor_id": "replication-test",
        "existing_successor_validator": {
            "module": "replication_test_module",
            "function": "validate",
        },
    }

    assert collection._existing_successor_authorized(
        contract, enforce_commit=True
    )
    assert calls == [("replication-test", True)]

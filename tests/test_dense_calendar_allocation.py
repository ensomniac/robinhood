from __future__ import annotations

import json
from datetime import date, timedelta
from pathlib import Path

import dense_calendar_allocation as allocation
import dense_capacity_inventory
import dense_session_calendar
import strategy_discovery


def _calendar_rows(count: int = 1_000) -> list[dict[str, str]]:
    start = date(2020, 1, 2)
    return [
        {
            "date": (start + timedelta(days=index)).isoformat(),
            "open_et": "09:30",
            "close_et": "16:00",
        }
        for index in range(count)
    ]


def _collection(tmp_path: Path) -> tuple[Path, Path]:
    calendar_path = tmp_path / "calendar.json"
    source_path = tmp_path / "source.json"
    rows = _calendar_rows()
    calendar_path.write_text(json.dumps(rows) + "\n", encoding="utf-8")
    calendar_sha256 = strategy_discovery._file_hash(calendar_path)
    source_path.write_text(
        json.dumps(
            {
                "calendar_sha256": calendar_sha256,
                "target_outcomes_accessed": False,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    payload = {
        "schema_version": 1,
        "artifact_kind": dense_session_calendar.COLLECTION_KIND,
        "campaign_id": allocation.batch.CAMPAIGN_ID,
        "state": "CALENDAR_COLLECTED_UNINSPECTED",
        "calendar_path": str(calendar_path),
        "calendar_sha256": calendar_sha256,
        "source_path": str(source_path),
        "contract_sha256": "a" * 64,
        "provider_requests": 1,
        "market_prices_accessed": False,
        "target_outcomes_accessed": False,
        "broker_actions": 0,
    }
    path, _artifact = strategy_discovery._write_artifact(
        payload,
        tmp_path / "collection",
        "dense-session-calendar-collection",
    )
    return path, calendar_path


def test_allocation_contract_separates_target_evidence_from_causal_warmup(
    tmp_path: Path, monkeypatch
):
    collection_path, calendar_path = _collection(tmp_path)
    monkeypatch.setattr(allocation, "PROJECT_ROOT", Path("/"))
    index_path = tmp_path / "exposure.jsonl"
    contract_path, contract = allocation.freeze_contract(
        collection_path,
        frozen_at="2026-07-23T19:20:00-04:00",
        root=tmp_path / "allocation",
        index_path=index_path,
        enforce_commit=False,
    )
    inspection_path, inspected = allocation.inspect(
        contract_path,
        inspected_at="2026-07-23T19:21:00-04:00",
        root=tmp_path / "allocation",
        index_path=index_path,
        enforce_commit=False,
    )

    assert contract["allocation_semantics"][
        "warmup_target_outcomes_eligible"
    ] is False
    assert contract["allocation_semantics"][
        "development_may_use_contaminated_training_history"
    ] is True
    assert contract["allocation_semantics"][
        "confirmation_signal_pairs_untouched"
    ] is True
    assert inspection_path.is_file()
    assert inspected["state"] == "CALENDAR_ALLOCATION_INSPECTED_READY"
    assert inspected["untouched_confirmation_signal_sessions"] == 105
    assert inspected["development_warmup_observations"] == 460
    assert inspected["confirmation_warmup_observations"] == 460
    assert all(inspected["checks"].values())
    assert inspected["target_outcomes_accessed"] is False
    assert strategy_discovery._file_hash(calendar_path) == contract[
        "calendar_sha256"
    ]


def test_allocator_uses_disjoint_targets_but_allows_causal_warmup_overlap():
    calendar = [
        (date(2020, 1, 2) + timedelta(days=index)).isoformat()
        for index in range(1_000)
    ]
    allocations = dense_capacity_inventory._allocate(calendar, [])
    targets = [
        day
        for item in allocations
        for day in item["evidence"]
    ]
    development_warmups = [
        day
        for item in allocations
        for day in item["development_warmup"]
    ]
    confirmation_signals = [
        day
        for item in allocations
        for day in item["confirmation_signals"]
    ]

    assert len(targets) == len(set(targets)) == 480
    assert len(development_warmups) == 460
    assert len(set(development_warmups)) < len(development_warmups)
    assert len(confirmation_signals) == len(set(confirmation_signals)) == 105
    assert all(
        max(item["development_warmup"]) < min(item["development"])
        and item["confirmation_warmup"][-1] == item["embargo"][-1]
        and set(item["confirmation_signals"]).issubset(item["confirmation"])
        for item in allocations
    )

from __future__ import annotations

import json
import sys
from datetime import date, timedelta

import pytest

import dense_capacity_inventory as capacity
import next_week_discovery_batch as batch
import outcome_exposure


def _calendar(tmp_path, count: int = 1_100):
    start = date(2020, 1, 1)
    rows = [
        {
            "date": (start + timedelta(days=index)).isoformat(),
            "open_et": "09:30",
            "close_et": "16:00",
        }
        for index in range(count)
    ]
    path = tmp_path / "calendar.json"
    path.write_text(json.dumps(rows) + "\n", encoding="utf-8")
    return path


def test_capacity_allocation_fails_before_rolling_authorization(tmp_path):
    with pytest.raises(
        capacity.DenseCapacityInventoryError,
        match="not authorized before",
    ):
        capacity.build_inventory(
            as_of=date(2026, 7, 22),
            created_at="2026-07-22T08:00:00-04:00",
            calendar_path=_calendar(tmp_path),
            index_path=tmp_path / "exposure.jsonl",
            output_root=tmp_path / "output",
        )
    with pytest.raises(
        capacity.DenseCapacityInventoryError,
        match="cannot be future-dated",
    ):
        capacity.build_inventory(
            as_of=date(2026, 7, 27),
            actual_today=date(2026, 7, 22),
            created_at="2026-07-27T08:00:00-04:00",
            calendar_path=_calendar(tmp_path),
            index_path=tmp_path / "exposure.jsonl",
            output_root=tmp_path / "future-output",
        )


def test_capacity_cli_reports_fail_closed_json(monkeypatch, capsys):
    def fail(**_kwargs):
        raise capacity.DenseCapacityInventoryError("synthetic capacity blocker")

    monkeypatch.setattr(capacity, "build_inventory", fail)
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "dense_capacity_inventory.py",
            "--as-of",
            "2026-07-27",
            "--created-at",
            "2026-07-27T08:00:00-04:00",
        ],
    )

    assert capacity.main() == 1
    assert json.loads(capsys.readouterr().out) == {
        "error": "synthetic capacity blocker",
        "error_type": "DenseCapacityInventoryError",
    }


def test_capacity_allocation_freezes_three_contiguous_disjoint_blocks(
    tmp_path, monkeypatch
):
    monkeypatch.setattr(capacity, "PROJECT_ROOT", tmp_path)
    family_contracts = __import__("dense_family_contracts")
    monkeypatch.setattr(family_contracts, "PROJECT_ROOT", tmp_path)
    calendar = _calendar(tmp_path)
    index = tmp_path / "exposure.jsonl"
    path, inventory = capacity.build_inventory(
        as_of=date(2026, 7, 23),
        actual_today=date(2026, 7, 23),
        created_at="2026-07-23T08:00:00-04:00",
        calendar_path=calendar,
        index_path=index,
        output_root=tmp_path / "output",
    )

    assert path.is_file()
    assert len(inventory["families"]) == 3
    assert inventory["provider_requests"] == 0
    assert inventory["outcomes_accessed"] is False
    all_dates = []
    all_collection_dates = []
    for family in inventory["families"]:
        expected_warmup = capacity.FAMILY_WARMUP_SESSIONS[family["family_id"]]
        assert len(family["development_warmup_dates"]) == expected_warmup
        assert len(family["confirmation_warmup_dates"]) == expected_warmup
        assert len(family["development_dates"]) == capacity.DEVELOPMENT_SESSIONS
        assert len(family["embargo_dates"]) == capacity.EMBARGO_SESSIONS
        assert len(family["confirmation_dates"]) == capacity.CONFIRMATION_SESSIONS
        all_dates.extend(
            family["development_dates"]
            + family["embargo_dates"]
            + family["confirmation_dates"]
        )
        all_collection_dates.extend(
            family["development_warmup_dates"]
            + family["development_dates"]
            + family["embargo_dates"]
            + family["confirmation_dates"]
        )
    assert len(all_dates) == len(set(all_dates)) == capacity.SESSIONS_PER_FAMILY * 3
    assert len(all_collection_dates) == len(set(all_collection_dates)) == 940
    contracts, status = family_contracts.freeze_batch(
        path,
        as_of=date(2026, 7, 23),
        actual_today=date(2026, 7, 23),
        index_path=index,
        output_root=tmp_path / "contracts",
        status_path=tmp_path / "status.json",
        enforce_commit=False,
    )
    assert len(contracts) == 3
    assert status["state"] == "THREE_FAMILY_CONTRACTS_FROZEN"


def test_known_outcome_date_splits_runs_and_can_make_capacity_insufficient(tmp_path):
    calendar = _calendar(tmp_path, count=1_000)
    index = tmp_path / "exposure.jsonl"
    rows = json.loads(calendar.read_text(encoding="utf-8"))
    outcome_exposure.append_record(
        outcome_exposure.build_record(
            exposure_id="middle",
            campaign_id="legacy",
            lane="legacy",
            recorded_at="2026-07-22T19:00:00-04:00",
            source_path="test",
            source_sha256="a" * 64,
            scope={"dates": [rows[500]["date"]], "symbols": ["*"]},
        ),
        index,
    )

    with pytest.raises(capacity.DenseCapacityInventoryError, match="contiguous"):
        capacity.build_inventory(
            as_of=batch.ACTIVATION_NOT_BEFORE,
            actual_today=batch.ACTIVATION_NOT_BEFORE,
            created_at="2026-07-23T08:00:00-04:00",
            calendar_path=calendar,
            index_path=index,
            output_root=tmp_path / "output",
        )

from __future__ import annotations

import json
from pathlib import Path

import oversold_replication_source as source


def test_selection_is_continuous_completed_2026_reserve() -> None:
    value = source.build_selection()
    dates = value["selected_dates"]
    assert dates[0] == source.TARGET_START
    assert dates[-1] == source.TARGET_END
    assert dates == sorted(dates)
    assert len(dates) > source.FRONT_EMBARGO_SESSIONS + 20
    assert value["substitution_allowed"] is False
    assert value["target_prices_accessed"] is False
    assert value["target_outcomes_observed_or_derived"] is False
    assert value["broker_actions"] == 0


def test_inspection_rebuilds_committed_selection(
    tmp_path: Path, monkeypatch
) -> None:
    selection = tmp_path / "selection.json"
    inspection = tmp_path / "inspection.json"
    monkeypatch.setattr(source, "SELECTION_PATH", selection)
    monkeypatch.setattr(source, "SELECTION_INSPECTION_PATH", inspection)
    monkeypatch.setattr(source, "_require_committed", lambda _path: None)
    monkeypatch.setattr(source, "_repo_path", lambda path: str(path))

    source.freeze_selection()
    result = source.inspect_selection()

    assert result["state"] == "SELECTION_INSPECTED_READY"
    assert all(result["checks"].values())
    assert result["provider_requests"] == 0
    assert result["broker_actions"] == 0
    assert json.loads(inspection.read_text()) == result


def test_scanner_contract_inspection_fails_closed_on_nonzero_state(
    tmp_path: Path, monkeypatch
) -> None:
    manifest_root = tmp_path / "manifests"
    manifest_root.mkdir()
    manifest_path = manifest_root / "manifest.json"
    manifest_path.write_text("{}")
    selection_inspection = tmp_path / "selection-inspection.json"
    selection_inspection.write_text(
        json.dumps({"state": "SELECTION_INSPECTED_READY"})
    )
    selection = tmp_path / "selection.json"
    selection.write_text("{}")
    monkeypatch.setattr(source, "SCANNER_MANIFEST_ROOT", manifest_root)
    monkeypatch.setattr(source, "SELECTION_PATH", selection)
    monkeypatch.setattr(
        source, "SELECTION_INSPECTION_PATH", selection_inspection
    )
    monkeypatch.setattr(source, "_require_committed", lambda _path: None)
    monkeypatch.setattr(
        source.alpaca,
        "load_contract",
        lambda _path: {
            "dataset_id": source.DATASET_ID,
            "requested_dates": source._selected_dates(),
            "manifest_sha256": "a" * 64,
            "selection": {"file_sha256": source._file_hash(source.SELECTION_PATH)},
            "collection_contract": {
                "session_calendar_sha256": source._file_hash(
                    source.CALENDAR_PATH
                ),
                "substitutions_allowed": False,
                "required_session_count": 40,
            },
        },
    )
    monkeypatch.setattr(
        source.HistoricalDayStore, "from_env", lambda _path: object()
    )
    monkeypatch.setattr(
        source.alpaca,
        "collection_status",
        lambda _manifest, store: {
            "session_files": {"ready": 1},
            "provider_requests": 0,
            "provider_retries": 0,
        },
    )

    try:
        source.inspect_scanner_contract(tmp_path / ".env")
    except source.OversoldReplicationSourceError as exc:
        assert "inspection failed" in str(exc)
    else:
        raise AssertionError("nonzero scanner state must fail closed")

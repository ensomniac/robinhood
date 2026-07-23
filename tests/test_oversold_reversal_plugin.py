from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import oversold_reversal_discovery as discovery
import oversold_reversal_plugin as plugin
from learning_data import freeze_dataset_contract


def _days(count: int) -> list[str]:
    start = date(2025, 1, 2)
    return [(start + timedelta(days=index)).isoformat() for index in range(count)]


def test_preflight_uses_only_committed_capacity_metadata(tmp_path, monkeypatch):
    dates = _days(120)
    evidence = Path("tests/test_oversold_reversal_plugin.py")
    manifest, _ = freeze_dataset_contract(
        {
            "schema_version": 1,
            "dataset_id": "dataset-oversold-preflight",
            "registered_at": "2026-07-23T17:10:00Z",
            "requested_dates": dates,
            "dataset_payload": {
                "lane": "development",
                "claim_scope": "DEVELOPMENT_ONLY",
                "evidence_paths": [str(evidence)],
                "inspected": True,
                "point_in_time_evidence": True,
                "oversold_capacity": {
                    "family_id": plugin.runtime.OVERSOLD_REVERSAL_FAMILY,
                    "formal_capacity": 4_833,
                    "development_training_contaminated": True,
                    "confirmation_access_permitted": False,
                },
            },
        },
        tmp_path / "manifests",
    )
    checked: list[Path] = []
    monkeypatch.setattr(
        plugin, "_require_committed", lambda path: checked.append(path)
    )

    result = plugin.preflight(
        {
            "capacity_manifest": str(manifest),
            "development_dates": dates,
        }
    )

    assert checked == [manifest, plugin.PROJECT_ROOT / evidence]
    assert result["verified_capacity"] == 4_833
    assert result["point_in_time_complete"] is True
    assert result["external_dataset_opened"] is False
    assert result["provider_telemetry"]["dataset_loads"] == 0


def test_status_never_waits_for_a_calendar_reset(tmp_path):
    result = discovery.status(root=tmp_path)

    assert result["state"] == "READY_TO_FREEZE"
    assert result["calendar_wait_required"] is False
    assert result["new_mechanism_family_slot_consumed"] is False
    assert result["confirmation_outcomes_accessed"] is False

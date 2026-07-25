from __future__ import annotations

import json
from pathlib import Path

import pytest

import strategy_dataset_rebind as rebind
import strategy_discovery
from learning_data import freeze_dataset_contract, load_frozen_dataset_contract


def _write_contract(path: Path, contract: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(contract, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _fixture(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(strategy_discovery, "PROJECT_ROOT", tmp_path)
    monkeypatch.setattr(rebind, "PROJECT_ROOT", tmp_path)
    root = tmp_path / "discovery"
    family_id = "dataset-rebind-family"
    implementation = tmp_path / "plugin.py"
    implementation.write_text("VALUE = 1\n", encoding="utf-8")
    old_contract = {
        "family_id": family_id,
        "development_dates": ["2024-01-02", "2024-01-03"],
        "implementation_files": ["plugin.py"],
        "implementation_hashes": {"plugin.py": "1" * 64},
    }
    new_contract = {
        **old_contract,
        "implementation_hashes": {
            "plugin.py": strategy_discovery._file_hash(implementation)
        },
    }
    old_search_path, old_search = strategy_discovery._write_artifact(
        {
            "artifact_kind": "frozen-development-search",
            "state": "SEARCH_FROZEN",
            "family_contract": old_contract,
        },
        root / family_id / "search",
        "old-search",
    )
    refreshed_contract_path = tmp_path / "contracts" / "contract.json"
    _write_contract(refreshed_contract_path, new_contract)
    refresh_path, refresh = strategy_discovery._write_artifact(
        {
            "artifact_kind": "implementation-refresh-inspection",
            "state": "IMPLEMENTATION_REFRESH_INSPECTED",
            "inspection": {"valid": True},
            "only_implementation_hashes_changed": True,
            "source_development_evaluations": 0,
            "strategy_outcomes_accessed": False,
            "confirmation_access_permitted": False,
            "source_search_path": strategy_discovery._relative(
                old_search_path
            ),
            "source_search_sha256": old_search["artifact_sha256"],
            "refreshed_contract_path": strategy_discovery._relative(
                refreshed_contract_path
            ),
            "refreshed_implementation_hashes": new_contract[
                "implementation_hashes"
            ],
        },
        root / family_id / "implementation-refresh",
        "refresh",
    )
    new_search_path, new_search = strategy_discovery._write_artifact(
        {
            "artifact_kind": "frozen-development-search",
            "state": "SEARCH_FROZEN",
            "family_contract": new_contract,
        },
        root / family_id / "search",
        "new-search",
    )
    inspection_path, inspection = strategy_discovery._write_artifact(
        {
            "artifact_kind": "dense-data-collection-inspection",
            "state": "DATASET_INSPECTED_READY",
            "family_id": family_id,
            "lane": "development",
            "dataset_sha256": "a" * 64,
            "external_file_sha256": "b" * 64,
            "evaluation_dates": old_contract["development_dates"],
            "checks": {"dataset_rebuilt": True},
        },
        root / family_id / "development-collection-inspection",
        "inspection",
    )
    source_manifest_path, source_manifest = freeze_dataset_contract(
        {
            "schema_version": 1,
            "dataset_id": f"dataset-{family_id}-development-old",
            "registered_at": "2026-07-25T12:00:00Z",
            "requested_dates": old_contract["development_dates"],
            "dataset_payload": {
                "lane": "development",
                "claim_scope": "DEVELOPMENT_ONLY",
                "evidence_paths": [
                    strategy_discovery._relative(inspection_path)
                ],
                "inspected": True,
                "point_in_time_evidence": True,
                "development_search_sha256": old_search[
                    "artifact_sha256"
                ],
                "dense_runtime": {
                    "family_id": family_id,
                    "dataset_sha256": "a" * 64,
                    "external_file_sha256": "b" * 64,
                    "external_relative_path": "private/data.json.gz",
                    "formal_capacity": 2,
                    "format": "json.gz",
                },
            },
        },
        root / family_id / "development-dataset",
    )
    return {
        "root": root,
        "refresh_path": refresh_path,
        "refresh": refresh,
        "new_search_path": new_search_path,
        "new_search": new_search,
        "source_manifest_path": source_manifest_path,
        "source_manifest": source_manifest,
        "inspection": inspection,
    }


def test_rebind_preserves_private_dataset_and_changes_only_binding(
    tmp_path,
    monkeypatch,
):
    fixture = _fixture(tmp_path, monkeypatch)

    inspection_path, inspection = rebind.rebind_development_dataset(
        fixture["refresh_path"],
        fixture["new_search_path"],
        root=fixture["root"],
        registered_at="2026-07-25T13:00:00Z",
        enforce_commit=False,
    )

    manifest = load_frozen_dataset_contract(
        tmp_path / inspection["refreshed_manifest_path"]
    )
    binding = manifest["dataset_payload"]["dataset_binding_refresh"]
    assert inspection_path.is_file()
    assert inspection["state"] == rebind.INSPECTION_STATE
    assert inspection["strategy_outcomes_accessed"] is False
    assert inspection["provider_telemetry"]["requests"] == 0
    assert manifest["requested_dates"] == fixture["source_manifest"][
        "requested_dates"
    ]
    assert manifest["dataset_payload"]["dense_runtime"] == fixture[
        "source_manifest"
    ]["dataset_payload"]["dense_runtime"]
    assert (
        manifest["dataset_payload"]["development_search_sha256"]
        == fixture["new_search"]["artifact_sha256"]
    )
    assert binding["source_manifest_sha256"] == fixture[
        "source_manifest"
    ]["manifest_sha256"]
    assert binding["external_dataset_opened"] is False


def test_rebind_rejects_semantic_search_change(tmp_path, monkeypatch):
    fixture = _fixture(tmp_path, monkeypatch)
    search = strategy_discovery.load_artifact(
        fixture["new_search_path"],
        expected_kind="frozen-development-search",
    )
    changed_contract = {
        **search["family_contract"],
        "development_dates": ["2024-01-02"],
    }
    changed_path, _changed = strategy_discovery._write_artifact(
        {
            "artifact_kind": "frozen-development-search",
            "state": "SEARCH_FROZEN",
            "family_contract": changed_contract,
        },
        fixture["root"] / "dataset-rebind-family" / "search",
        "changed-search",
    )

    with pytest.raises(
        rebind.StrategyDatasetRebindError,
        match="not semantically identical",
    ):
        rebind.rebind_development_dataset(
            fixture["refresh_path"],
            changed_path,
            root=fixture["root"],
            enforce_commit=False,
        )

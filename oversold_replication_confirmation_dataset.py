"""Publish and inspect the exact winner-bound confirmation dataset manifest."""

from __future__ import annotations

import argparse
import json
from collections.abc import Sequence
from datetime import datetime
from pathlib import Path
from typing import Any

import dense_strategy_runtime as runtime
import oversold_replication_confirmation as inventory_controller
import oversold_replication_confirmation_collection as collection
import oversold_replication_development as development
import oversold_replication_plugin as plugin
import strategy_discovery
from historical_store import (
    HistoricalDayStore,
    canonical_sha256,
    sha256_file,
)
from learning_data import (
    LearningDataError,
    freeze_dataset_contract,
    load_frozen_dataset_contract,
    validate_dataset_contract,
)


PROJECT_ROOT = Path(__file__).resolve().parent
SCHEMA_VERSION = 1
OUTPUT_ROOT = (
    PROJECT_ROOT
    / "strategy_tournament/v2/discovery"
    / runtime.OVERSOLD_REVERSAL_FAMILY
    / "confirmation-dataset"
)
INSPECTION_ROOT = (
    PROJECT_ROOT
    / "strategy_tournament/v2/discovery"
    / runtime.OVERSOLD_REVERSAL_FAMILY
    / "confirmation-dataset-inspection"
)


class OversoldReplicationConfirmationDatasetError(RuntimeError):
    """The confirmation dataset manifest is incomplete or has drifted."""


def _read(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise OversoldReplicationConfirmationDatasetError(
            f"cannot read {path}: {exc}"
        ) from exc
    if not isinstance(value, dict):
        raise OversoldReplicationConfirmationDatasetError(
            f"{path} must contain an object"
        )
    return value


def _repo_path(path: Path) -> str:
    try:
        return path.resolve().relative_to(PROJECT_ROOT.resolve()).as_posix()
    except ValueError as exc:
        raise OversoldReplicationConfirmationDatasetError(
            f"path escaped repository: {path}"
        ) from exc


def _store_path(store: HistoricalDayStore, path: Path) -> str:
    try:
        return path.resolve().relative_to(store.root.resolve()).as_posix()
    except ValueError as exc:
        raise OversoldReplicationConfirmationDatasetError(
            f"private path escaped historical store: {path}"
        ) from exc


def _timestamp(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise OversoldReplicationConfirmationDatasetError(
            "registered_at is invalid"
        ) from exc
    if parsed.tzinfo is None:
        raise OversoldReplicationConfirmationDatasetError(
            "registered_at needs a timezone"
        )
    return parsed


def _one(root: Path) -> Path:
    paths = sorted(root.glob("*.json"))
    if len(paths) != 1:
        raise OversoldReplicationConfirmationDatasetError(
            f"expected exactly one artifact under {root}"
        )
    return paths[0]


def _public_binding(path: Path) -> dict[str, str]:
    return {
        "path": _repo_path(path),
        "file_sha256": sha256_file(path),
    }


def _inputs(
    store: HistoricalDayStore,
    *,
    enforce_commit: bool,
) -> tuple[
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
    list[Path],
]:
    inventory_contract_path = _one(
        inventory_controller.CONTRACT_ROOT
    )
    inventory_inspection_path = _one(
        inventory_controller.INSPECTION_ROOT
    )
    contract_path = _one(collection.CONTRACT_ROOT)
    contract_inspection_path = _one(
        collection.CONTRACT_INSPECTION_ROOT
    )
    status_path = _one(collection.STATUS_ROOT)
    data_inspection_path = _one(collection.DATA_INSPECTION_ROOT)
    public_paths = [
        inventory_contract_path,
        inventory_inspection_path,
        contract_path,
        contract_inspection_path,
        status_path,
        data_inspection_path,
    ]
    if enforce_commit:
        for path in public_paths:
            strategy_discovery.require_committed(path)
    contract, contract_inspection, winner = (
        collection._load_contract_chain(
            contract_path,
            contract_inspection_path,
        )
    )
    status = collection._load_hashed(
        status_path,
        identity_field="status_sha256",
        expected_kind=(
            "oversold_replication_confirmation_collection_status"
        ),
    )
    data_inspection = collection._load_hashed(
        data_inspection_path,
        identity_field="inspection_sha256",
        expected_kind=(
            "oversold_replication_confirmation_data_inspection"
        ),
    )
    (
        _inventory_contract_path,
        _inventory_contract,
        _inventory_inspection_path,
        _inventory_inspection,
        inventory,
    ) = collection._load_inventory(
        store,
        require_committed=enforce_commit,
    )
    input_index = development._read_gzip(
        collection._input_index_path(store)
    )
    if not (
        status.get("state")
        == "CONFIRMATION_DATA_COLLECTED_AWAITING_INSPECTION"
        and status.get("winner_sha256") == winner["artifact_sha256"]
        and status.get("rules_hash") == winner["rules_hash"]
        and status.get("unresolved_symbol_sessions") == 0
        and status.get("capture_after_preregistration_attested") is True
        and data_inspection.get("state")
        == "CONFIRMATION_DATA_INSPECTED_READY"
        and data_inspection.get("valid") is True
        and data_inspection.get("winner_sha256")
        == winner["artifact_sha256"]
        and data_inspection.get("rules_hash") == winner["rules_hash"]
        and data_inspection.get(
            "private_input_index_content_sha256"
        )
        == canonical_sha256(input_index)
        and inventory.get("lane") == "confirmation"
        and input_index.get("lane") == "confirmation"
        and input_index.get("winner_sha256")
        == winner["artifact_sha256"]
        and input_index.get("rules_hash") == winner["rules_hash"]
        and contract_inspection.get("valid") is True
    ):
        raise OversoldReplicationConfirmationDatasetError(
            "confirmation dataset inputs are not inspected and exact"
        )
    return winner, inventory, input_index, status, public_paths


def build_contract(
    *,
    registered_at: str,
    store: HistoricalDayStore | None = None,
    enforce_commit: bool = True,
) -> dict[str, Any]:
    registered = _timestamp(registered_at)
    source = store or HistoricalDayStore.from_env()
    winner, inventory, input_index, status, public_paths = _inputs(
        source,
        enforce_commit=enforce_commit,
    )
    winner_recorded = _timestamp(str(winner["recorded_at"]))
    collection_started = _timestamp(
        str(status["collection_started_at"])
    )
    collection_completed = _timestamp(
        str(status["collection_completed_at"])
    )
    if not (
        winner_recorded < collection_started
        <= collection_completed
        < registered
    ):
        raise OversoldReplicationConfirmationDatasetError(
            "winner, collection, and manifest chronology is invalid"
        )
    inventory_path = inventory_controller._inventory_path(source)
    input_index_path = collection._input_index_path(source)
    contract = {
        "schema_version": SCHEMA_VERSION,
        "dataset_id": (
            f"dataset-{winner['strategy_version']}-confirmation"
        ),
        "registered_at": registered_at,
        "requested_dates": list(winner["confirmation_dates"]),
        "dataset_payload": {
            "lane": "confirmation",
            "claim_scope": "EXACT_PREREGISTERED_CONTRACT_ONLY",
            "evidence_paths": [
                _repo_path(path) for path in public_paths
            ],
            "inspected": True,
            "point_in_time_evidence": True,
            "preregistration_sha256": winner["rules_hash"],
            "preregistered_at": winner["recorded_at"],
            "capture_after_preregistration_attested": True,
            "oversold_replication_runtime": {
                "family_id": runtime.OVERSOLD_REVERSAL_FAMILY,
                "sample_phase": "confirmation",
                "private_inventory_path": _store_path(
                    source,
                    inventory_path,
                ),
                "private_inventory_file_sha256": sha256_file(
                    inventory_path
                ),
                "private_inventory_content_sha256": inventory[
                    "content_sha256"
                ],
                "private_input_index_path": _store_path(
                    source,
                    input_index_path,
                ),
                "private_input_index_file_sha256": sha256_file(
                    input_index_path
                ),
                "private_input_index_content_sha256": canonical_sha256(
                    input_index
                ),
                "public_bindings": [
                    _public_binding(path) for path in public_paths
                ],
                "dataset_loads_per_evaluation": 1,
                "provider_requests": 0,
            },
            "collection_chronology": {
                "winner_recorded_at": winner["recorded_at"],
                "collection_started_at": status[
                    "collection_started_at"
                ],
                "collection_completed_at": status[
                    "collection_completed_at"
                ],
                "registered_at": registered_at,
            },
            "implementation_binding": {
                "publisher_path": _repo_path(
                    Path(__file__).resolve()
                ),
                "publisher_sha256": sha256_file(
                    Path(__file__).resolve()
                ),
                "plugin_path": _repo_path(
                    Path(plugin.__file__).resolve()
                ),
                "plugin_sha256": sha256_file(
                    Path(plugin.__file__).resolve()
                ),
                "runtime_path": _repo_path(
                    Path(runtime.__file__).resolve()
                ),
                "runtime_sha256": sha256_file(
                    Path(runtime.__file__).resolve()
                ),
            },
        },
    }
    return validate_dataset_contract(contract)


def publish_confirmation(
    *,
    registered_at: str,
    store: HistoricalDayStore | None = None,
) -> tuple[Path, dict[str, Any]]:
    for path in (
        Path(__file__).resolve(),
        Path(plugin.__file__).resolve(),
        Path(runtime.__file__).resolve(),
    ):
        strategy_discovery.require_committed(path)
    contract = build_contract(
        registered_at=registered_at,
        store=store,
    )
    return freeze_dataset_contract(contract, OUTPUT_ROOT)


def inspect_confirmation(
    manifest_path: Path,
    *,
    store: HistoricalDayStore | None = None,
) -> tuple[Path, dict[str, Any]]:
    strategy_discovery.require_committed(manifest_path)
    frozen = load_frozen_dataset_contract(manifest_path)
    expected = build_contract(
        registered_at=str(frozen["registered_at"]),
        store=store,
    )
    payload = frozen["dataset_payload"]
    runtime_binding = payload["oversold_replication_runtime"]
    checks = {
        "exact_rebuild": {
            key: value
            for key, value in frozen.items()
            if key != "manifest_sha256"
        }
        == expected,
        "exact_preregistration": payload["claim_scope"]
        == "EXACT_PREREGISTERED_CONTRACT_ONLY",
        "winner_bound": bool(payload["preregistration_sha256"]),
        "capture_after_winner": payload[
            "capture_after_preregistration_attested"
        ]
        is True,
        "dates_exact": frozen["requested_dates"]
        == expected["requested_dates"],
        "one_dataset_load": runtime_binding[
            "dataset_loads_per_evaluation"
        ]
        == 1,
        "zero_provider_requests": runtime_binding[
            "provider_requests"
        ]
        == 0,
    }
    if not all(checks.values()):
        raise OversoldReplicationConfirmationDatasetError(
            "confirmation dataset manifest inspection failed"
        )
    content = {
        "schema_version": SCHEMA_VERSION,
        "artifact_kind": (
            "oversold_replication_confirmation_dataset_inspection"
        ),
        "campaign_id": development.CAMPAIGN_ID,
        "family_id": runtime.OVERSOLD_REVERSAL_FAMILY,
        "successor_id": development.SUCCESSOR_ID,
        "dataset_id": frozen["dataset_id"],
        "state": "CONFIRMATION_DATASET_INSPECTED_READY",
        "manifest_path": _repo_path(manifest_path),
        "manifest_file_sha256": sha256_file(manifest_path),
        "manifest_sha256": frozen["manifest_sha256"],
        "rules_hash": payload["preregistration_sha256"],
        "checks": checks,
        "evaluation_dates": len(frozen["requested_dates"]),
        "provider_requests": 0,
        "parameter_alternatives_evaluated": 0,
        "broker_actions": 0,
        "valid": True,
    }
    return development._publish(
        INSPECTION_ROOT,
        "oversold-replication-confirmation-dataset-inspection",
        content,
        "inspection_sha256",
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    publish = sub.add_parser("publish")
    publish.add_argument("--registered-at", required=True)
    inspect = sub.add_parser("inspect")
    inspect.add_argument("manifest", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "publish":
            path, value = publish_confirmation(
                registered_at=args.registered_at
            )
            state = "CONFIRMATION_DATASET_FROZEN"
        else:
            path, value = inspect_confirmation(args.manifest)
            state = value["state"]
        print(
            json.dumps(
                {
                    "path": _repo_path(path),
                    "state": state,
                    "rules_hash": (
                        value["dataset_payload"][
                            "preregistration_sha256"
                        ]
                        if args.command == "publish"
                        else value["rules_hash"]
                    ),
                    "provider_requests": 0,
                    "parameter_alternatives_evaluated": 0,
                    "broker_actions": 0,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    except (
        KeyError,
        OSError,
        LearningDataError,
        OversoldReplicationConfirmationDatasetError,
        collection.OversoldReplicationConfirmationCollectionError,
        strategy_discovery.StrategyDiscoveryError,
        ValueError,
    ) as exc:
        print(
            json.dumps(
                {
                    "state": "BLOCKED",
                    "error": str(exc),
                    "provider_requests": 0,
                    "parameter_alternatives_evaluated": 0,
                    "broker_actions": 0,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())

"""Publish and inspect the broad oversold development dataset manifest."""

from __future__ import annotations

import argparse
import json
from collections.abc import Sequence
from datetime import datetime
from pathlib import Path
from typing import Any

import dense_strategy_runtime as runtime
import oversold_replication_development as development
import oversold_replication_development_collection as collection
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
DATASET_ID = (
    "dataset-short-horizon-oversold-reversal-v4-broad-development"
)
OUTPUT_ROOT = (
    PROJECT_ROOT
    / "strategy_tournament/v2/continuous"
    / development.SUCCESSOR_ID
    / "capacity"
)
INSPECTION_ROOT = (
    PROJECT_ROOT
    / "strategy_tournament/v2/continuous"
    / development.SUCCESSOR_ID
    / "capacity-inspection"
)
DATA_INSPECTION = (
    collection.DATA_INSPECTION_ROOT
    / "oversold-replication-development-data-inspection-"
    "481db5955bc073bda5d7e83aefea98a7d65e41b5037962bf3cbb28264b98ae21.json"
)
COLLECTION_STATUS = (
    collection.STATUS_ROOT
    / "oversold-replication-development-collection-status-"
    "08c8efed99460f899f7a602ead79cff9c7ad791478937dd2c8a75730f6d06967.json"
)


class OversoldReplicationDatasetError(RuntimeError):
    """The development dataset manifest is incomplete or has drifted."""


def _read(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise OversoldReplicationDatasetError(
            f"cannot read {path}: {exc}"
        ) from exc
    if not isinstance(value, dict):
        raise OversoldReplicationDatasetError(
            f"{path} must contain an object"
        )
    return value


def _repo_path(path: Path) -> str:
    try:
        return path.resolve().relative_to(PROJECT_ROOT.resolve()).as_posix()
    except ValueError as exc:
        raise OversoldReplicationDatasetError(
            f"path escaped repository: {path}"
        ) from exc


def _store_path(store: HistoricalDayStore, path: Path) -> str:
    try:
        return path.resolve().relative_to(store.root.resolve()).as_posix()
    except ValueError as exc:
        raise OversoldReplicationDatasetError(
            f"private path escaped historical store: {path}"
        ) from exc


def _timestamp(value: str) -> None:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise OversoldReplicationDatasetError(
            "registered_at is invalid"
        ) from exc
    if parsed.tzinfo is None:
        raise OversoldReplicationDatasetError(
            "registered_at needs a timezone"
        )


def _public_binding(path: Path) -> dict[str, str]:
    return {
        "path": _repo_path(path),
        "file_sha256": sha256_file(path),
    }


def _inputs(
    store: HistoricalDayStore,
) -> tuple[
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
]:
    for path in (
        collection.INVENTORY_CONTRACT,
        collection.INVENTORY_INSPECTION,
        collection.CONTRACT_ROOT
        / "oversold-replication-development-collection-contract-"
        "0b036084cbc205eac67f55401114b9ee152cf378beb409574d0e11b3e11c72b9.json",
        collection.CONTRACT_INSPECTION_ROOT
        / "oversold-replication-development-collection-contract-inspection-"
        "3b5e5bd5f61a61e25d850183cb2efcc36daa7ac123207b44d00bce2e07cf3772.json",
        collection.AUTHORIZATION_ROOT
        / "oversold-replication-development-exposure-authorization-"
        "5305172cc8212b8dd75fdfc8945b429f962b1e72b3e52b4ade0f485230500916.json",
        COLLECTION_STATUS,
        DATA_INSPECTION,
    ):
        strategy_discovery.require_committed(path)
    inventory_contract, inventory_inspection, inventory = (
        collection._load_inventory(
            store,
            require_committed=True,
        )
    )
    data_inspection = collection._load_hashed(
        DATA_INSPECTION,
        identity_field="inspection_sha256",
        expected_kind=(
            "oversold_replication_development_data_inspection"
        ),
    )
    status = collection._load_hashed(
        COLLECTION_STATUS,
        identity_field="status_sha256",
        expected_kind=(
            "oversold_replication_development_collection_status"
        ),
    )
    input_index = development._read_gzip(
        collection._input_index_path(store)
    )
    if not (
        data_inspection.get("state")
        == "DEVELOPMENT_DATA_INSPECTED_READY"
        and data_inspection.get("valid") is True
        and data_inspection.get("private_input_index_content_sha256")
        == canonical_sha256(input_index)
        and data_inspection.get("candidate_symbol_sessions")
        == 2_334
        and data_inspection.get("evaluation_dates") == 399
        and data_inspection.get("zero_signal_dates") == 299
        and status.get("state")
        == "DEVELOPMENT_DATA_COLLECTED_AWAITING_INSPECTION"
        and status.get("unresolved_symbol_sessions") == 0
        and inventory_inspection.get("valid") is True
        and inventory_contract["inventory"]["private_content_sha256"]
        == inventory["content_sha256"]
    ):
        raise OversoldReplicationDatasetError(
            "development dataset inputs are not inspected and complete"
        )
    return inventory, input_index, data_inspection


def build_contract(
    *,
    registered_at: str,
    store: HistoricalDayStore | None = None,
) -> dict[str, Any]:
    _timestamp(registered_at)
    source = store or HistoricalDayStore.from_env()
    inventory, input_index, data_inspection = _inputs(source)
    inventory_path = development._inventory_path(source)
    input_index_path = collection._input_index_path(source)
    public_paths = [
        collection.INVENTORY_CONTRACT,
        collection.INVENTORY_INSPECTION,
        collection.CONTRACT_ROOT
        / "oversold-replication-development-collection-contract-"
        "0b036084cbc205eac67f55401114b9ee152cf378beb409574d0e11b3e11c72b9.json",
        collection.CONTRACT_INSPECTION_ROOT
        / "oversold-replication-development-collection-contract-inspection-"
        "3b5e5bd5f61a61e25d850183cb2efcc36daa7ac123207b44d00bce2e07cf3772.json",
        collection.AUTHORIZATION_ROOT
        / "oversold-replication-development-exposure-authorization-"
        "5305172cc8212b8dd75fdfc8945b429f962b1e72b3e52b4ade0f485230500916.json",
        COLLECTION_STATUS,
        DATA_INSPECTION,
    ]
    contract = {
        "schema_version": SCHEMA_VERSION,
        "dataset_id": DATASET_ID,
        "registered_at": registered_at,
        "requested_dates": list(inventory["evaluation_dates"]),
        "dataset_payload": {
            "lane": "development",
            "claim_scope": "DEVELOPMENT_ONLY",
            "evidence_paths": [
                _repo_path(path) for path in public_paths
            ],
            "inspected": True,
            "point_in_time_evidence": True,
            "oversold_replication_capacity": {
                "family_id": runtime.OVERSOLD_REVERSAL_FAMILY,
                "mechanism_family": (
                    "short-horizon-oversold-reversal"
                ),
                "formal_capacity": 100,
                "capacity_unit": (
                    "point-in-time signal-capable sessions"
                ),
                "evaluation_sessions": 399,
                "signal_capable_sessions": 100,
                "zero_signal_days": 299,
                "candidate_symbol_sessions": 2_334,
                "exact_390_contiguous_symbol_sessions": (
                    data_inspection[
                        "exact_390_contiguous_symbol_sessions"
                    ]
                ),
                "sparse_symbol_sessions": data_inspection[
                    "sparse_symbol_sessions_retained_as_no_signal"
                ],
                "development_training_contaminated": True,
                "confirmation_access_permitted": False,
                "provider_requests": 0,
            },
            "oversold_replication_runtime": {
                "family_id": runtime.OVERSOLD_REVERSAL_FAMILY,
                "sample_phase": "development",
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
            "implementation_binding": {
                "publisher_path": _repo_path(Path(__file__).resolve()),
                "publisher_sha256": sha256_file(
                    Path(__file__).resolve()
                ),
                "plugin_path": _repo_path(Path(plugin.__file__).resolve()),
                "plugin_sha256": sha256_file(
                    Path(plugin.__file__).resolve()
                ),
                "runtime_path": _repo_path(Path(runtime.__file__).resolve()),
                "runtime_sha256": sha256_file(
                    Path(runtime.__file__).resolve()
                ),
            },
        },
    }
    return validate_dataset_contract(contract)


def publish_development(
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


def inspect_development(
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
    checks = {
        "exact_rebuild": {
            key: value
            for key, value in frozen.items()
            if key != "manifest_sha256"
        }
        == expected,
        "chronological_dates": frozen["requested_dates"]
        == sorted(frozen["requested_dates"]),
        "complete_evaluation_path": len(frozen["requested_dates"])
        == 399,
        "capacity_floor": frozen["dataset_payload"][
            "oversold_replication_capacity"
        ]["formal_capacity"]
        == 100,
        "zero_days_retained": frozen["dataset_payload"][
            "oversold_replication_capacity"
        ]["zero_signal_days"]
        == 299,
        "one_dataset_load": frozen["dataset_payload"][
            "oversold_replication_runtime"
        ]["dataset_loads_per_evaluation"]
        == 1,
        "development_contaminated": frozen["dataset_payload"][
            "oversold_replication_capacity"
        ]["development_training_contaminated"]
        is True,
        "confirmation_locked": frozen["dataset_payload"][
            "oversold_replication_capacity"
        ]["confirmation_access_permitted"]
        is False,
    }
    if not all(checks.values()):
        raise OversoldReplicationDatasetError(
            "development dataset manifest inspection failed"
        )
    content = {
        "schema_version": SCHEMA_VERSION,
        "artifact_kind": (
            "oversold_replication_development_dataset_inspection"
        ),
        "campaign_id": development.CAMPAIGN_ID,
        "family_id": runtime.OVERSOLD_REVERSAL_FAMILY,
        "successor_id": development.SUCCESSOR_ID,
        "dataset_id": DATASET_ID,
        "state": "DEVELOPMENT_DATASET_INSPECTED_READY",
        "manifest_path": _repo_path(manifest_path),
        "manifest_file_sha256": sha256_file(manifest_path),
        "manifest_sha256": frozen["manifest_sha256"],
        "checks": checks,
        "evaluation_dates": 399,
        "signal_capable_sessions": 100,
        "candidate_symbol_sessions": 2_334,
        "provider_requests": 0,
        "return_metrics_computed": 0,
        "confirmation_accessed": False,
        "broker_actions": 0,
        "valid": True,
    }
    return development._publish(
        INSPECTION_ROOT,
        "oversold-replication-development-dataset-inspection",
        content,
        "inspection_sha256",
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    publish = sub.add_parser("publish-development")
    publish.add_argument("--registered-at", required=True)
    inspect = sub.add_parser("inspect-development")
    inspect.add_argument("manifest", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "publish-development":
            path, value = publish_development(
                registered_at=args.registered_at,
            )
        else:
            path, value = inspect_development(args.manifest)
        print(
            json.dumps(
                {
                    "path": _repo_path(path),
                    "state": (
                        "DEVELOPMENT_DATASET_FROZEN_AWAITING_INSPECTION"
                        if "manifest_sha256" in value
                        and "state" not in value
                        else value["state"]
                    ),
                    "evaluation_dates": len(
                        value.get("requested_dates", [])
                    )
                    or value.get("evaluation_dates"),
                    "provider_requests": 0,
                    "return_metrics_computed": 0,
                    "confirmation_accessed": False,
                    "broker_actions": 0,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    except (
        KeyError,
        LearningDataError,
        OSError,
        OversoldReplicationDatasetError,
        strategy_discovery.StrategyDiscoveryError,
    ) as exc:
        print(
            json.dumps(
                {
                    "state": "BLOCKED",
                    "error": str(exc),
                    "provider_requests": 0,
                    "confirmation_accessed": False,
                    "broker_actions": 0,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())

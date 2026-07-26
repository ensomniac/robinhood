"""Independently rebuild dense collection output and freeze its runtime manifest."""

from __future__ import annotations

import argparse
import gzip
import json
from collections.abc import Mapping
from datetime import datetime
from pathlib import Path
from typing import Any

import dense_data_collection as collection
import dense_strategy_runtime as runtime
import strategy_discovery
from historical_store import (
    DEFAULT_ENV_PATH,
    HistoricalStoreConfig,
    HistoricalStoreError,
    canonical_sha256,
    sha256_file,
)
from learning_data import LearningDataError, freeze_dataset_contract


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_PUBLIC_ROOT = collection.DEFAULT_PUBLIC_ROOT


class DenseDataInspectionError(RuntimeError):
    """The committed collection cannot be independently reproduced."""


def _load_external(path: Path) -> dict[str, Any]:
    try:
        with gzip.open(path, "rt", encoding="utf-8") as source:
            value = json.load(source)
    except (OSError, json.JSONDecodeError) as exc:
        raise DenseDataInspectionError(f"cannot read external dataset: {exc}") from exc
    if not isinstance(value, dict):
        raise DenseDataInspectionError("external dataset must be an object")
    return value


def _formal_capacity(dataset: Mapping[str, Any]) -> int:
    dates = dataset["evaluation_dates"]
    if dataset["family_id"] == runtime.EQUITY_RESIDUAL_FAMILY:
        return sum(len(dataset["universe_by_date"][day]) for day in dates)
    return len(dates) * len(dataset["symbols"])


def inspect(
    status_path: Path,
    *,
    inspected_at: str,
    store_config: HistoricalStoreConfig | None = None,
    public_root: Path = DEFAULT_PUBLIC_ROOT,
    enforce_commit: bool = True,
) -> tuple[Path, dict[str, Any], Path, dict[str, Any]]:
    try:
        timestamp = datetime.fromisoformat(inspected_at.replace("Z", "+00:00"))
    except ValueError as exc:
        raise DenseDataInspectionError("inspected_at is invalid") from exc
    if timestamp.tzinfo is None:
        raise DenseDataInspectionError("inspected_at must include a timezone")
    if enforce_commit:
        strategy_discovery.require_committed(status_path)
    status = strategy_discovery.load_artifact(
        status_path, expected_kind=collection.STATUS_KIND
    )
    if not (
        status.get("state") == "COLLECTED_UNINSPECTED"
        and status.get("completed_tasks") == status.get("task_count")
        and status.get("substitutions") == 0
        and status.get("broker_actions") == 0
    ):
        raise DenseDataInspectionError("collection status is incomplete or unsafe")
    try:
        collection_started = datetime.fromisoformat(
            str(status["collection_started_at"]).replace("Z", "+00:00")
        )
        collection_completed = datetime.fromisoformat(
            str(status["collection_completed_at"]).replace("Z", "+00:00")
        )
    except (KeyError, ValueError) as exc:
        raise DenseDataInspectionError(
            "collection chronology is missing or invalid"
        ) from exc
    if (
        collection_started.tzinfo is None
        or collection_completed.tzinfo is None
        or collection_completed < collection_started
        or timestamp <= collection_completed
    ):
        raise DenseDataInspectionError(
            "inspection must follow a valid collection chronology"
        )
    plan_path = PROJECT_ROOT / str(status["plan_path"])
    plan = collection._validate_plan(plan_path, enforce_commit=enforce_commit)
    if plan["lane"] == "confirmation":
        preregistered = datetime.fromisoformat(
            str(plan["preregistered_at"]).replace("Z", "+00:00")
        )
        if collection_started <= preregistered:
            raise DenseDataInspectionError(
                "confirmation collection did not follow winner preregistration"
            )
    if status.get("plan_sha256") != plan["artifact_sha256"]:
        raise DenseDataInspectionError("collection plan binding drifted")
    config = store_config or HistoricalStoreConfig.from_env(DEFAULT_ENV_PATH)
    external_path = (config.root / str(status["external_relative_path"])).resolve()
    if config.root.resolve() not in external_path.parents:
        raise DenseDataInspectionError("external dataset escaped the historical store")
    if not external_path.is_file() or sha256_file(external_path) != status.get(
        "external_file_sha256"
    ):
        raise DenseDataInspectionError("external dataset is missing or drifted")
    observed = _load_external(external_path)
    checkpoint_root = external_path.parent
    rebuilt = collection.build_dataset(checkpoint_root, plan)
    if canonical_sha256(rebuilt) != status.get("dataset_sha256") or observed != rebuilt:
        raise DenseDataInspectionError("independent dataset reconstruction differs")
    runtime.prepare_dataset(rebuilt)
    if plan.get("sparse_confirmation_outcome_access") is True:
        observed_bar_dates = {
            str(row["date"])
            for rows in rebuilt["daily_bars"].values()
            for row in rows
        }
        if observed_bar_dates != set(plan["signal_dates"]):
            raise DenseDataInspectionError(
                "sparse confirmation accessed non-signal outcome bars"
            )
    capacity = _formal_capacity(rebuilt)
    if capacity < 100:
        raise DenseDataInspectionError("formal runtime capacity is below 100")
    inspection_payload = {
        "schema_version": 1,
        "artifact_kind": "dense-data-collection-inspection",
        "campaign_id": status["campaign_id"],
        "state": "DATASET_INSPECTED_READY",
        "family_id": status["family_id"],
        "lane": status["lane"],
        "status_path": collection._repo_path(status_path),
        "status_sha256": status["artifact_sha256"],
        "plan_sha256": plan["artifact_sha256"],
        "dataset_sha256": status["dataset_sha256"],
        "external_file_sha256": status["external_file_sha256"],
        "formal_capacity": capacity,
        "evaluation_dates": plan["evaluation_dates"],
        "checks": {
            "all_tasks_rebuilt": True,
            "external_file_rehashed": True,
            "dataset_canonical_hash_rebuilt": True,
            "runtime_schema_revalidated": True,
            "frozen_scope_exact": True,
            "substitutions_zero": True,
            "sparse_confirmation_scope_exact": (
                plan.get("sparse_confirmation_outcome_access") is not True
                or observed_bar_dates == set(plan["signal_dates"])
            ),
        },
        "provider_telemetry": status["provider_telemetry"],
        "collection_started_at": status["collection_started_at"],
        "collection_completed_at": status["collection_completed_at"],
        "broker_actions": 0,
        "inspected_at": inspected_at,
    }
    inspection_path, inspection = strategy_discovery._write_artifact(
        inspection_payload,
        public_root / str(plan["family_id"]) / f"{plan['lane']}-collection-inspection",
        f"{plan['family_id']}-{plan['lane']}-collection-inspection",
    )
    binding: dict[str, Any] = {
        "family_id": plan["family_id"],
        "external_relative_path": status["external_relative_path"],
        "external_file_sha256": status["external_file_sha256"],
        "dataset_sha256": status["dataset_sha256"],
        "format": "json.gz",
        "formal_capacity": capacity,
    }
    payload: dict[str, Any] = {
        "lane": plan["lane"],
        "claim_scope": (
            "DEVELOPMENT_ONLY"
            if plan["lane"] == "development"
            else "EXACT_PREREGISTERED_CONTRACT_ONLY"
        ),
        "evidence_paths": [
            collection._repo_path(plan_path),
            collection._repo_path(status_path),
            collection._repo_path(inspection_path),
            "STRATEGY_DISCOVERY_V2.md",
        ],
        "inspected": True,
        "point_in_time_evidence": True,
        "dense_runtime": binding,
        "provider_telemetry": status["provider_telemetry"],
        "collection_started_at": status["collection_started_at"],
        "collection_completed_at": status["collection_completed_at"],
    }
    if plan["lane"] == "development":
        payload["development_search_sha256"] = plan["binding_sha256"]
    else:
        payload["preregistration_sha256"] = plan["binding_sha256"]
        payload["preregistered_at"] = plan["preregistered_at"]
        payload["capture_after_preregistration_attested"] = True
    manifest_path, manifest = freeze_dataset_contract(
        {
            "schema_version": 1,
            "dataset_id": (
                f"dataset-{plan['family_id']}-{plan['lane']}-"
                f"{plan['binding_sha256'][:16]}"
            ),
            "registered_at": inspected_at,
            "requested_dates": list(plan["evaluation_dates"]),
            "dataset_payload": payload,
        },
        public_root / str(plan["family_id"]) / f"{plan['lane']}-dataset",
    )
    return inspection_path, inspection, manifest_path, manifest


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("status", type=Path)
    parser.add_argument("--inspected-at", required=True)
    parser.add_argument("--public-root", type=Path, default=DEFAULT_PUBLIC_ROOT)
    return parser


def main() -> int:
    args = _parser().parse_args()
    try:
        inspection_path, inspection, manifest_path, manifest = inspect(
            args.status,
            inspected_at=args.inspected_at,
            public_root=args.public_root,
        )
        print(
            json.dumps(
                {
                    "inspection": collection._repo_path(inspection_path),
                    "inspection_sha256": inspection["artifact_sha256"],
                    "manifest": collection._repo_path(manifest_path),
                    "manifest_sha256": manifest["manifest_sha256"],
                    "state": inspection["state"],
                    "broker_actions": 0,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    except (
        DenseDataInspectionError,
        collection.DenseDataCollectionError,
        HistoricalStoreError,
        LearningDataError,
        strategy_discovery.StrategyDiscoveryError,
        OSError,
        ValueError,
    ) as exc:
        print(json.dumps({"error": str(exc), "error_type": type(exc).__name__}, indent=2))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

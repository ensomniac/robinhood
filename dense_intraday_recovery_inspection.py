"""Independently rebuild retained intraday evidence and freeze its data manifest."""

from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Any

import dense_data_collection as collection
import dense_data_collection_inspection as data_inspection
import dense_intraday_recovery as recovery
import dense_strategy_runtime as runtime
import strategy_discovery
from historical_store import (
    DEFAULT_ENV_PATH,
    HistoricalStoreConfig,
    canonical_sha256,
    sha256_file,
)
from learning_data import LearningDataError, freeze_dataset_contract


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_PUBLIC_ROOT = collection.DEFAULT_PUBLIC_ROOT
INSPECTION_KIND = "dense-intraday-retained-recovery-inspection"


class DenseIntradayRecoveryInspectionError(RuntimeError):
    """Retained intraday recovery cannot be independently reproduced."""


def _repo_path(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(PROJECT_ROOT.resolve()))
    except ValueError as exc:
        raise DenseIntradayRecoveryInspectionError(
            f"path is outside repository: {path}"
        ) from exc


def inspect(
    status_path: Path,
    *,
    inspected_at: str,
    store_config: HistoricalStoreConfig | None = None,
    public_root: Path = DEFAULT_PUBLIC_ROOT,
    enforce_commit: bool = True,
) -> tuple[Path, dict[str, Any], Path, dict[str, Any]]:
    try:
        timestamp = datetime.fromisoformat(
            inspected_at.replace("Z", "+00:00")
        )
    except ValueError as exc:
        raise DenseIntradayRecoveryInspectionError(
            "inspected_at is invalid"
        ) from exc
    if timestamp.tzinfo is None:
        raise DenseIntradayRecoveryInspectionError(
            "inspected_at must include a timezone"
        )
    if enforce_commit:
        strategy_discovery.require_committed(status_path)
    status = strategy_discovery.load_artifact(
        status_path, expected_kind=recovery.STATUS_KIND
    )
    if not (
        status.get("state")
        == "RETAINED_DATASET_COLLECTED_UNINSPECTED"
        and status.get("family_id") == runtime.INTRADAY_ETF_FAMILY
        and status.get("lane") == "development"
        and status.get("completed_tasks") == status.get("task_count")
        and status.get("provider_telemetry", {}).get("requests") == 0
        and status.get("substitutions") == 0
        and status.get("interpolated_minutes") == 0
        and status.get("broker_actions") == 0
    ):
        raise DenseIntradayRecoveryInspectionError(
            "retained collection status is incomplete or unsafe"
        )
    try:
        collected = datetime.fromisoformat(
            str(status["collected_at"]).replace("Z", "+00:00")
        )
    except ValueError as exc:
        raise DenseIntradayRecoveryInspectionError(
            "retained collection chronology is invalid"
        ) from exc
    if (
        collected.tzinfo is None
        or timestamp <= collected
    ):
        raise DenseIntradayRecoveryInspectionError(
            "inspection must follow retained collection"
        )
    plan_path = PROJECT_ROOT / str(status["plan_path"])
    plan = recovery.validate_plan(
        plan_path, enforce_commit=enforce_commit
    )
    if not (
        plan["artifact_sha256"] == status["plan_sha256"]
        and plan["binding_sha256"] == status["binding_sha256"]
        and plan["missing_data_policy"]["missed_dates"]
        == status["missed_data_dates"]
        and plan["missing_data_policy"]["missed_evaluation_dates"]
        == status["missed_evaluation_dates"]
    ):
        raise DenseIntradayRecoveryInspectionError(
            "retained collection plan binding drifted"
        )
    config = store_config or HistoricalStoreConfig.from_env(DEFAULT_ENV_PATH)
    external_path = (
        config.root / str(status["external_relative_path"])
    ).resolve()
    if (
        config.root.resolve() not in external_path.parents
        or not external_path.is_file()
        or sha256_file(external_path) != status["external_file_sha256"]
    ):
        raise DenseIntradayRecoveryInspectionError(
            "retained external dataset is missing or drifted"
        )
    observed = data_inspection._load_external(external_path)
    source_root = recovery._source_root(config, plan)
    rebuilt = recovery.build_dataset(source_root, plan)
    if not (
        observed == rebuilt
        and canonical_sha256(rebuilt) == status["dataset_sha256"]
    ):
        raise DenseIntradayRecoveryInspectionError(
            "retained dataset reconstruction differs"
        )
    runtime.prepare_dataset(rebuilt)
    formal_capacity = len(plan["evaluation_dates"]) * len(plan["symbols"])
    if formal_capacity < 100:
        raise DenseIntradayRecoveryInspectionError(
            "retained formal runtime capacity is below 100"
        )
    inspection_payload = {
        "schema_version": 1,
        "artifact_kind": INSPECTION_KIND,
        "campaign_id": status["campaign_id"],
        "state": "RETAINED_DATASET_INSPECTED_READY",
        "family_id": status["family_id"],
        "lane": status["lane"],
        "status_path": _repo_path(status_path),
        "status_sha256": status["artifact_sha256"],
        "plan_sha256": plan["artifact_sha256"],
        "dataset_sha256": status["dataset_sha256"],
        "external_file_sha256": status["external_file_sha256"],
        "formal_capacity": formal_capacity,
        "missed_data_dates": status["missed_data_dates"],
        "missed_evaluation_dates": status["missed_evaluation_dates"],
        "checks": {
            "all_1440_source_tasks_rehashed": (
                status["task_count"] == 1440
            ),
            "external_file_rehashed": True,
            "dataset_canonical_hash_rebuilt": True,
            "runtime_schema_revalidated": True,
            "fixed_universe_missing_dates_explicit": True,
            "zero_provider_requests": True,
            "substitutions_zero": True,
            "interpolated_minutes_zero": True,
        },
        "provider_telemetry": status["provider_telemetry"],
        "broker_actions": 0,
        "collected_at": status["collected_at"],
        "inspected_at": inspected_at,
    }
    inspection_path, inspection = strategy_discovery._write_artifact(
        inspection_payload,
        public_root
        / runtime.INTRADAY_ETF_FAMILY
        / "development-retained-recovery-inspection",
        f"{runtime.INTRADAY_ETF_FAMILY}-development-retained-recovery-inspection",
    )
    manifest_path, manifest = freeze_dataset_contract(
        {
            "schema_version": 1,
            "dataset_id": (
                f"dataset-{runtime.INTRADAY_ETF_FAMILY}-development-"
                f"{plan['binding_sha256'][:16]}"
            ),
            "registered_at": inspected_at,
            "requested_dates": list(plan["evaluation_dates"]),
            "dataset_payload": {
                "lane": "development",
                "claim_scope": "DEVELOPMENT_ONLY",
                "evidence_paths": [
                    _repo_path(plan_path),
                    _repo_path(status_path),
                    _repo_path(inspection_path),
                    "STRATEGY_DISCOVERY_V2.md",
                ],
                "inspected": True,
                "point_in_time_evidence": True,
                "development_search_sha256": plan["binding_sha256"],
                "dense_runtime": {
                    "family_id": runtime.INTRADAY_ETF_FAMILY,
                    "external_relative_path": status[
                        "external_relative_path"
                    ],
                    "external_file_sha256": status[
                        "external_file_sha256"
                    ],
                    "dataset_sha256": status["dataset_sha256"],
                    "format": "json.gz",
                    "formal_capacity": formal_capacity,
                },
                "missing_data_policy": plan["missing_data_policy"],
                "provider_telemetry": status["provider_telemetry"],
                "collected_at": status["collected_at"],
            },
        },
        public_root
        / runtime.INTRADAY_ETF_FAMILY
        / "development-dataset",
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
                    "inspection": _repo_path(inspection_path),
                    "inspection_sha256": inspection["artifact_sha256"],
                    "manifest": _repo_path(manifest_path),
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
        DenseIntradayRecoveryInspectionError,
        recovery.DenseIntradayRecoveryError,
        collection.DenseDataCollectionError,
        LearningDataError,
        strategy_discovery.StrategyDiscoveryError,
        OSError,
        ValueError,
    ) as exc:
        print(
            json.dumps(
                {"error": str(exc), "error_type": type(exc).__name__},
                indent=2,
            )
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

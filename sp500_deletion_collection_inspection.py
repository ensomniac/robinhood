"""Independently inspect S&P deletion plans and collected datasets."""

from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path
from typing import Any

import outcome_exposure
import sp500_addition_collection as shared
import sp500_deletion_collection as collection
import sp500_deletion_discovery as discovery
import strategy_discovery
from historical_store import (
    DEFAULT_ENV_PATH,
    HistoricalStoreConfig,
    canonical_sha256,
    sha256_file,
)
from learning_data import freeze_dataset_contract


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_PUBLIC_ROOT = strategy_discovery.DEFAULT_ROOT


class Sp500DeletionInspectionError(RuntimeError):
    """Deletion collection evidence failed independent reconstruction."""


def _timestamp(value: str, field: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise Sp500DeletionInspectionError(
            f"{field} is invalid"
        ) from exc
    if parsed.tzinfo is None:
        raise Sp500DeletionInspectionError(
            f"{field} needs a timezone"
        )
    return parsed


def inspect_plan(
    plan_path: Path,
    *,
    inspected_at: str,
    public_root: Path = DEFAULT_PUBLIC_ROOT,
    enforce_commit: bool = True,
) -> tuple[Path, dict[str, Any]]:
    _timestamp(inspected_at, "inspected_at")
    if enforce_commit:
        strategy_discovery.require_committed(plan_path)
    plan = strategy_discovery.load_artifact(
        plan_path, expected_kind=collection.PLAN_KIND
    )
    if not (
        plan.get("state") == "COLLECTION_PLAN_FROZEN"
        and plan.get("family_id") == discovery.FAMILY_ID
        and plan.get("lane") in {"development", "confirmation"}
        and plan.get("provider_requests_before_plan_freeze") == 0
        and plan.get("market_outcomes_accessed") is False
        and plan.get("substitutions_allowed") is False
        and plan.get("broker_actions") == 0
    ):
        raise Sp500DeletionInspectionError(
            "deletion plan is not outcome-blind and frozen"
        )
    lane = str(plan["lane"])
    authority_path = PROJECT_ROOT / str(plan["authority_path"])
    authority, contract, binding, preregistered = (
        collection._authority(
            authority_path,
            lane=lane,
            enforce_commit=enforce_commit,
        )
    )
    scope_path, scope, events, tasks, event_task_ids = (
        collection._plan_components(
            contract,
            lane=lane,
            enforce_commit=enforce_commit,
        )
    )
    if not (
        plan["authority_sha256"] == authority["artifact_sha256"]
        and plan["binding_sha256"] == binding
        and plan["event_scope_sha256"] == scope["artifact_sha256"]
        and plan["event_scope_path"] == collection._repo_path(scope_path)
        and plan["evaluation_dates"] == contract[f"{lane}_dates"]
        == scope[f"{lane}_dates"]
        and plan["events_sha256"] == canonical_sha256(events)
        and plan["event_count"] == len(events)
        and plan["signal_date_capacity"]
        == len(scope[f"{lane}_signal_dates"])
        and plan["tasks"] == tasks
        and plan["task_count"] == len(tasks)
        and plan["event_task_ids"] == event_task_ids
        and plan["source_scope"] == scope[f"{lane}_scope"]
        and (
            lane != "confirmation"
            or plan["preregistered_at"] == preregistered
        )
    ):
        raise Sp500DeletionInspectionError(
            "deletion task graph or authority binding drifted"
        )
    payload = {
        "schema_version": 1,
        "artifact_kind": collection.PLAN_INSPECTION_KIND,
        "campaign_id": discovery.CAMPAIGN_ID,
        "family_id": discovery.FAMILY_ID,
        "lane": lane,
        "state": "COLLECTION_PLAN_INSPECTED",
        "plan_path": collection._repo_path(plan_path),
        "plan_sha256": plan["artifact_sha256"],
        "authority_sha256": plan["authority_sha256"],
        "event_scope_sha256": scope["artifact_sha256"],
        "event_count": len(events),
        "signal_date_capacity": plan["signal_date_capacity"],
        "task_count": len(tasks),
        "provider_requests": 0,
        "market_outcomes_accessed": False,
        "confirmation_accessed": False,
        "broker_actions": 0,
        "inspected_at": inspected_at,
        "checks": {
            "authority_hash_rebuilt": True,
            "event_scope_hash_rebuilt": True,
            "complete_observation_windows_rebuilt": True,
            "task_hashes_rebuilt": True,
            "event_task_mapping_rebuilt": True,
            "source_scope_rebuilt": True,
            "controller_hashes_rebuilt": plan[
                "controller_hashes"
            ]
            == {
                "sp500_deletion_collection.py": sha256_file(
                    PROJECT_ROOT / "sp500_deletion_collection.py"
                ),
                "sp500_deletion_discovery.py": sha256_file(
                    PROJECT_ROOT / "sp500_deletion_discovery.py"
                ),
            },
            "zero_outcome_boundary_rebuilt": True,
            "substitutions_forbidden": True,
        },
    }
    if not all(payload["checks"].values()):
        raise Sp500DeletionInspectionError(
            "deletion controller binding drifted"
        )
    return strategy_discovery._write_artifact(
        payload,
        public_root
        / discovery.FAMILY_ID
        / f"{lane}-collection-plan-inspection",
        f"{discovery.FAMILY_ID}-{lane}-collection-plan-inspection",
    )


def inspect_collection(
    status_path: Path,
    *,
    inspected_at: str,
    store_config: HistoricalStoreConfig | None = None,
    public_root: Path = DEFAULT_PUBLIC_ROOT,
    enforce_commit: bool = True,
) -> tuple[Path, dict[str, Any], Path, dict[str, Any]]:
    if enforce_commit:
        strategy_discovery.require_committed(Path(__file__).resolve())
    observed_at = _timestamp(inspected_at, "inspected_at")
    if enforce_commit:
        strategy_discovery.require_committed(status_path)
    status = strategy_discovery.load_artifact(
        status_path, expected_kind=collection.STATUS_KIND
    )
    if not (
        status.get("state") == "COLLECTED_UNINSPECTED"
        and status.get("family_id") == discovery.FAMILY_ID
        and status.get("completed_tasks") == status.get("task_count")
        and status.get("substitutions") == 0
        and status.get("market_outcomes_accessed") is True
        and status.get("broker_actions") == 0
    ):
        raise Sp500DeletionInspectionError(
            "deletion collection status is incomplete or unsafe"
        )
    started = _timestamp(
        str(status["collection_started_at"]),
        "collection_started_at",
    )
    completed = _timestamp(
        str(status["collection_completed_at"]),
        "collection_completed_at",
    )
    if completed < started or observed_at <= completed:
        raise Sp500DeletionInspectionError(
            "deletion collection inspection chronology is invalid"
        )
    plan_path = PROJECT_ROOT / str(status["plan_path"])
    plan = collection._load_plan(
        plan_path, enforce_commit=enforce_commit
    )
    inspection_path = (
        PROJECT_ROOT / str(status["plan_inspection_path"])
    )
    plan_inspection = collection._plan_inspection(
        plan,
        inspection_path,
        enforce_commit=enforce_commit,
    )
    if not (
        status["plan_sha256"] == plan["artifact_sha256"]
        and status["plan_inspection_sha256"]
        == sha256_file(inspection_path)
        and plan_inspection["plan_sha256"]
        == plan["artifact_sha256"]
    ):
        raise Sp500DeletionInspectionError(
            "deletion collection predecessor binding drifted"
        )
    if plan["lane"] == "confirmation":
        preregistered = _timestamp(
            str(plan["preregistered_at"]), "preregistered_at"
        )
        if started <= preregistered:
            raise Sp500DeletionInspectionError(
                "confirmation outcomes preceded winner freeze"
            )
    config = store_config or HistoricalStoreConfig.from_env(
        DEFAULT_ENV_PATH
    )
    external_path = (
        config.root / str(status["external_relative_path"])
    ).resolve()
    if (
        config.root.resolve() not in external_path.parents
        or not external_path.is_file()
        or sha256_file(external_path)
        != status["external_file_sha256"]
    ):
        raise Sp500DeletionInspectionError(
            "external deletion dataset is missing or drifted"
        )
    observed = shared._read_gzip(external_path)
    rebuilt = collection.build_dataset(external_path.parent, plan)
    missing_ids = sorted(
        str(task["task_id"])
        for task in plan["tasks"]
        if shared._read_gzip(
            shared._checkpoint_path(external_path.parent, task)
        ).get("collection_disposition")
        in collection.PERMANENT_MISSING_DISPOSITIONS
    )
    if not (
        observed == rebuilt
        and canonical_sha256(rebuilt) == status["dataset_sha256"]
        and rebuilt["evaluation_dates"] == plan["evaluation_dates"]
        and rebuilt["family_id"] == discovery.FAMILY_ID
        and status["permanent_missing_task_count"]
        == len(missing_ids)
        and status["permanent_missing_task_ids"] == missing_ids
        and status["permanent_missing_semantics"] == "missed_trade"
    ):
        raise Sp500DeletionInspectionError(
            "independent deletion dataset reconstruction differs"
        )
    populated_dates = sorted(
        day
        for day, rows in rebuilt[
            "event_metadata_by_entry_date"
        ].items()
        if rows
    )
    if len(populated_dates) != plan["signal_date_capacity"]:
        raise Sp500DeletionInspectionError(
            "runtime deletion signal capacity drifted"
        )
    inspection_payload = {
        "schema_version": 1,
        "artifact_kind": "sp500-deletion-data-collection-inspection",
        "campaign_id": discovery.CAMPAIGN_ID,
        "family_id": discovery.FAMILY_ID,
        "lane": plan["lane"],
        "state": "DATASET_INSPECTED_READY",
        "status_path": collection._repo_path(status_path),
        "status_sha256": status["artifact_sha256"],
        "plan_sha256": plan["artifact_sha256"],
        "plan_inspection_sha256": plan_inspection[
            "artifact_sha256"
        ],
        "dataset_sha256": status["dataset_sha256"],
        "external_file_sha256": status["external_file_sha256"],
        "formal_capacity": plan["signal_date_capacity"],
        "event_count": plan["event_count"],
        "evaluation_dates": plan["evaluation_dates"],
        "provider_telemetry": status["provider_telemetry"],
        "permanent_missing_task_count": len(missing_ids),
        "permanent_missing_task_ids_sha256": canonical_sha256(
            missing_ids
        ),
        "collection_started_at": status["collection_started_at"],
        "collection_completed_at": status[
            "collection_completed_at"
        ],
        "inspected_at": inspected_at,
        "inspection_controller_sha256": sha256_file(
            Path(__file__).resolve()
        ),
        "broker_actions": 0,
        "checks": {
            "all_tasks_rebuilt": True,
            "external_file_rehashed": True,
            "dataset_canonical_hash_rebuilt": True,
            "runtime_schema_revalidated": True,
            "complete_observation_scope_rebuilt": True,
            "missing_rows_retained_without_substitution": True,
            "permanent_missing_tasks_rebuilt": True,
            "confirmation_after_preregistration": (
                plan["lane"] != "confirmation"
                or started
                > _timestamp(
                    str(plan["preregistered_at"]),
                    "preregistered_at",
                )
            ),
        },
    }
    output_path, inspection = strategy_discovery._write_artifact(
        inspection_payload,
        public_root
        / discovery.FAMILY_ID
        / f"{plan['lane']}-collection-inspection",
        f"{discovery.FAMILY_ID}-{plan['lane']}-collection-inspection",
    )
    binding = {
        "family_id": discovery.FAMILY_ID,
        "external_relative_path": status["external_relative_path"],
        "external_file_sha256": status["external_file_sha256"],
        "dataset_sha256": status["dataset_sha256"],
        "format": "json.gz",
        "formal_capacity": plan["signal_date_capacity"],
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
            collection._repo_path(inspection_path),
            collection._repo_path(status_path),
            collection._repo_path(output_path),
            str(plan["event_scope_path"]),
        ],
        "inspected": True,
        "point_in_time_evidence": True,
        "dense_runtime": binding,
        "provider_telemetry": status["provider_telemetry"],
        "collection_started_at": status["collection_started_at"],
        "collection_completed_at": status[
            "collection_completed_at"
        ],
    }
    if plan["lane"] == "development":
        payload["development_search_sha256"] = plan[
            "binding_sha256"
        ]
    else:
        payload["preregistration_sha256"] = plan["binding_sha256"]
        payload["preregistered_at"] = plan["preregistered_at"]
        payload["capture_after_preregistration_attested"] = True
    manifest_path, manifest = freeze_dataset_contract(
        {
            "schema_version": 1,
            "dataset_id": (
                f"dataset-{discovery.FAMILY_ID}-{plan['lane']}-"
                f"{plan['binding_sha256'][:16]}"
            ),
            "registered_at": inspected_at,
            "requested_dates": list(plan["evaluation_dates"]),
            "dataset_payload": payload,
        },
        public_root
        / discovery.FAMILY_ID
        / f"{plan['lane']}-dataset",
    )
    return output_path, inspection, manifest_path, manifest


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--public-root", type=Path, default=DEFAULT_PUBLIC_ROOT
    )
    sub = parser.add_subparsers(dest="command", required=True)
    plan = sub.add_parser("inspect-plan")
    plan.add_argument("plan", type=Path)
    plan.add_argument("--inspected-at", required=True)
    dataset = sub.add_parser("inspect-collection")
    dataset.add_argument("status", type=Path)
    dataset.add_argument("--inspected-at", required=True)
    return parser


def main() -> int:
    args = _parser().parse_args()
    try:
        if args.command == "inspect-plan":
            path, artifact = inspect_plan(
                args.plan,
                inspected_at=args.inspected_at,
                public_root=args.public_root,
            )
            result = {
                "written": collection._repo_path(path),
                "artifact_sha256": artifact["artifact_sha256"],
                "state": artifact["state"],
                "provider_requests": 0,
            }
        else:
            path, artifact, manifest_path, manifest = (
                inspect_collection(
                    args.status,
                    inspected_at=args.inspected_at,
                    public_root=args.public_root,
                )
            )
            result = {
                "written": collection._repo_path(path),
                "artifact_sha256": artifact["artifact_sha256"],
                "state": artifact["state"],
                "dataset_manifest": collection._repo_path(
                    manifest_path
                ),
                "dataset_manifest_sha256": manifest[
                    "manifest_sha256"
                ],
            }
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    except (
        Sp500DeletionInspectionError,
        collection.Sp500DeletionCollectionError,
        outcome_exposure.OutcomeExposureError,
        strategy_discovery.StrategyDiscoveryError,
        OSError,
        ValueError,
    ) as exc:
        print(
            json.dumps(
                {"error": str(exc), "error_type": type(exc).__name__},
                indent=2,
                sort_keys=True,
            )
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

"""Independently rebuild one dense collection failure disposition."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import dense_collection_recovery as recovery
import dense_data_collection as collection
import strategy_discovery
from historical_store import DEFAULT_ENV_PATH, HistoricalStoreConfig


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_PUBLIC_ROOT = collection.DEFAULT_PUBLIC_ROOT
INSPECTION_KIND = "dense-data-collection-failure-inspection"
INSPECTION_STATE = "COLLECTION_FAILURE_INSPECTED"


class DenseCollectionRecoveryInspectionError(RuntimeError):
    """A collection failure cannot be independently reproduced."""


def inspect(
    failure_path: Path,
    *,
    inspected_at: str,
    store_config: HistoricalStoreConfig | None = None,
    public_root: Path = DEFAULT_PUBLIC_ROOT,
    enforce_commit: bool = True,
) -> tuple[Path, dict[str, Any]]:
    observed_at = recovery._timestamp(inspected_at, "inspected_at")
    failure = recovery._load_failure(
        failure_path,
        enforce_commit=enforce_commit,
    )
    recorded_at = recovery._timestamp(
        str(failure["recorded_at"]),
        "recorded_at",
    )
    if observed_at <= recorded_at:
        raise DenseCollectionRecoveryInspectionError(
            "failure inspection must follow the failure record"
        )
    plan_path = PROJECT_ROOT / str(failure["plan_path"])
    plan = collection._validate_plan(
        plan_path,
        enforce_commit=enforce_commit,
    )
    if not (
        plan["artifact_sha256"] == failure["plan_sha256"]
        and plan["authority_sha256"] == failure["authority_sha256"]
        and plan["binding_sha256"] == failure["binding_sha256"]
        and plan["family_id"] == failure["family_id"]
        and plan["lane"] == failure["lane"]
    ):
        raise DenseCollectionRecoveryInspectionError(
            "failure predecessor binding drifted"
        )
    config = store_config or HistoricalStoreConfig.from_env(DEFAULT_ENV_PATH)
    root = recovery._private_root(config, plan)
    telemetry_state = collection._telemetry_state(
        root / "collection-telemetry.json",
        str(plan["artifact_sha256"]),
    )
    facts = recovery._failure_facts(
        plan,
        root,
        telemetry_state["provider_telemetry"],
    )
    expected = {
        "failure_code": failure["failure_code"],
        "completed_tasks": failure["completed_tasks"],
        "market_price_rows_accessed": failure["market_price_rows_accessed"],
        "evaluation_tasks_completed": failure["evaluation_tasks_completed"],
        "data_outcomes_accessed": failure["data_outcomes_accessed"],
        "exposure_scope": failure["exposure_scope"],
        "failure_details": failure["failure_details"],
    }
    if facts != expected:
        raise DenseCollectionRecoveryInspectionError(
            "independent failure reconstruction differs"
        )
    if (
        telemetry_state["collection_started_at"]
        != failure["collection_started_at"]
        or telemetry_state["provider_telemetry"]
        != failure["provider_telemetry"]
    ):
        raise DenseCollectionRecoveryInspectionError(
            "failure provider telemetry drifted"
        )
    if collection._existing_status(public_root, plan) is not None:
        raise DenseCollectionRecoveryInspectionError(
            "failed plan also has a completed collection status"
        )
    payload = {
        "schema_version": 1,
        "artifact_kind": INSPECTION_KIND,
        "campaign_id": failure["campaign_id"],
        "state": INSPECTION_STATE,
        "family_id": failure["family_id"],
        "lane": failure["lane"],
        "failure_path": collection._repo_path(failure_path),
        "failure_sha256": failure["artifact_sha256"],
        "plan_sha256": plan["artifact_sha256"],
        "failure_code": failure["failure_code"],
        "data_outcomes_accessed": failure["data_outcomes_accessed"],
        "exposure_scope": failure["exposure_scope"],
        "strategy_metrics_accessed": False,
        "confirmation_outcomes_accessed": False,
        "commit_enforcement_requested": enforce_commit,
        "checks": {
            "failure_reopened": True,
            "plan_reopened": True,
            "checkpoint_counts_rebuilt": True,
            "market_price_row_access_rebuilt": True,
            "outcome_exposure_scope_rebuilt": True,
            "provider_telemetry_rebuilt": True,
            "completed_status_absent": True,
            "strategy_metrics_absent": True,
            "confirmation_outcomes_absent": True,
            "substitutions_zero": failure["substitutions"] == 0,
            "broker_actions_zero": failure["broker_actions"] == 0,
        },
        "inspected_at": inspected_at,
    }
    return strategy_discovery._write_artifact(
        payload,
        public_root
        / str(failure["family_id"])
        / f"{failure['lane']}-collection-failure-inspection",
        f"{failure['family_id']}-{failure['lane']}-collection-failure-inspection",
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("failure", type=Path)
    parser.add_argument("--inspected-at", required=True)
    parser.add_argument("--public-root", type=Path, default=DEFAULT_PUBLIC_ROOT)
    return parser


def main() -> int:
    args = _parser().parse_args()
    try:
        path, artifact = inspect(
            args.failure,
            inspected_at=args.inspected_at,
            public_root=args.public_root,
        )
        print(
            json.dumps(
                {
                    "written": collection._repo_path(path),
                    "artifact_sha256": artifact["artifact_sha256"],
                    "state": artifact["state"],
                    "failure_code": artifact["failure_code"],
                    "data_outcomes_accessed": artifact[
                        "data_outcomes_accessed"
                    ],
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    except (
        DenseCollectionRecoveryInspectionError,
        recovery.DenseCollectionRecoveryError,
        collection.DenseDataCollectionError,
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

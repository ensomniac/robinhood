"""Independently inspect S&P event-window plans and collected datasets."""

from __future__ import annotations

import argparse
import gzip
import json
from collections.abc import Mapping
from datetime import datetime
from pathlib import Path
from typing import Any

import outcome_exposure
import sp500_addition_collection as collection
import sp500_addition_discovery as discovery
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


class Sp500AdditionInspectionError(RuntimeError):
    """A committed plan or external event dataset cannot be rebuilt."""


def _timestamp(value: str, field: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise Sp500AdditionInspectionError(
            f"{field} is invalid"
        ) from exc
    if parsed.tzinfo is None:
        raise Sp500AdditionInspectionError(
            f"{field} needs a timezone"
        )
    return parsed


def _task(value: Mapping[str, Any]) -> dict[str, Any]:
    result = dict(value)
    result["task_id"] = canonical_sha256(result)
    return result


def _authority_contract(
    plan: Mapping[str, Any], *, enforce_commit: bool
) -> tuple[dict[str, Any], dict[str, Any]]:
    authority_path = PROJECT_ROOT / str(plan["authority_path"])
    if enforce_commit:
        strategy_discovery.require_committed(authority_path)
    if plan["lane"] == "development":
        authority = strategy_discovery.load_artifact(
            authority_path,
            expected_kind="frozen-development-search",
        )
        contract = dict(authority["family_contract"])
        expected_binding = authority["artifact_sha256"]
    else:
        authority = strategy_discovery.load_artifact(
            authority_path,
            expected_kind="frozen-strategy-winner",
        )
        contract = {
            **dict(authority),
            "event_scope_path": authority["exact_rules"]["universe"][
                "event_scope_path"
            ],
            "event_scope_sha256": authority["exact_rules"]["universe"][
                "event_scope_sha256"
            ],
        }
        expected_binding = authority["rules_hash"]
        outcome_exposure.assert_untouched(
            authority["confirmation_scope"],
            outcome_exposure.read_index(),
        )
    if not (
        authority["artifact_sha256"] == plan["authority_sha256"]
        and expected_binding == plan["binding_sha256"]
        and contract["family_id"] == discovery.FAMILY_ID
    ):
        raise Sp500AdditionInspectionError(
            "plan authority binding drifted"
        )
    return authority, contract


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
        raise Sp500AdditionInspectionError(
            "collection plan is not outcome-blind and frozen"
        )
    _authority, contract = _authority_contract(
        plan, enforce_commit=enforce_commit
    )
    scope_path = PROJECT_ROOT / str(plan["event_scope_path"])
    if enforce_commit:
        strategy_discovery.require_committed(scope_path)
    scope = strategy_discovery.load_artifact(
        scope_path, expected_kind="sp500-addition-event-scope"
    )
    lane = str(plan["lane"])
    events = [dict(row) for row in scope[f"{lane}_events"]]
    if not (
        scope["artifact_sha256"] == plan["event_scope_sha256"]
        == contract["event_scope_sha256"]
        and str(scope_path.relative_to(PROJECT_ROOT))
        == contract["event_scope_path"]
        and plan["evaluation_dates"] == contract[f"{lane}_dates"]
        == scope[f"{lane}_dates"]
        and plan["source_scope"] == scope[f"{lane}_scope"]
        and plan["events_sha256"] == canonical_sha256(events)
        and plan["event_count"] == len(events)
        and plan["signal_date_capacity"]
        == len(scope[f"{lane}_signal_dates"])
    ):
        raise Sp500AdditionInspectionError(
            "plan event scope or capacity drifted"
        )
    required_dates = sorted(
        {
            day
            for event in events
            for day in [
                str(event["reference_date"]),
                *map(str, event["holding_dates"]),
            ]
        }
    )
    expected_split = _task(
        {
            "kind": "split_actions",
            "start": required_dates[0],
            "date": required_dates[-1],
        }
    )
    windows: dict[tuple[str, str, str], dict[str, Any]] = {}
    for event in events:
        key = (
            str(event["ticker"]),
            str(event["reference_date"]),
            str(event["holding_dates"][-1]),
        )
        windows.setdefault(
            key,
            _task(
                {
                    "kind": "yahoo_daily_symbol_bars",
                    "symbol": key[0],
                    "start": key[1],
                    "date": key[2],
                }
            ),
        )
    expected_tasks = [
        expected_split,
        *sorted(
            windows.values(),
            key=lambda row: (
                str(row["start"]),
                str(row["date"]),
                str(row["symbol"]),
            ),
        ),
    ]
    task_ids = {
        (
            str(task["symbol"]),
            str(task["start"]),
            str(task["date"]),
        ): str(task["task_id"])
        for task in expected_tasks
        if task["kind"] == "yahoo_daily_symbol_bars"
    }
    expected_event_task_ids = {
        str(event["event_id"]): task_ids[
            (
                str(event["ticker"]),
                str(event["reference_date"]),
                str(event["holding_dates"][-1]),
            )
        ]
        for event in events
    }
    if not (
        plan["tasks"] == expected_tasks
        and plan["task_count"] == len(expected_tasks)
        and plan["event_task_ids"] == expected_event_task_ids
        and plan["permanent_missing_symbol_response"]
        == "missed_trade"
        and len({row["task_id"] for row in expected_tasks})
        == len(expected_tasks)
    ):
        raise Sp500AdditionInspectionError(
            "exact provider task graph drifted"
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
        "task_count": len(expected_tasks),
        "provider_requests": 0,
        "market_outcomes_accessed": False,
        "confirmation_accessed": False,
        "broker_actions": 0,
        "inspected_at": inspected_at,
        "checks": {
            "authority_hash_rebuilt": True,
            "event_scope_hash_rebuilt": True,
            "event_window_dates_rebuilt": True,
            "task_hashes_rebuilt": True,
            "event_task_mapping_rebuilt": True,
            "source_scope_rebuilt": True,
            "zero_outcome_boundary_rebuilt": True,
            "substitutions_forbidden": True,
        },
    }
    return strategy_discovery._write_artifact(
        payload,
        public_root
        / discovery.FAMILY_ID
        / f"{lane}-collection-plan-inspection",
        f"{discovery.FAMILY_ID}-{lane}-collection-plan-inspection",
    )


def _load_external(path: Path) -> dict[str, Any]:
    try:
        with gzip.open(path, "rt", encoding="utf-8") as source:
            value = json.load(source)
    except (OSError, json.JSONDecodeError) as exc:
        raise Sp500AdditionInspectionError(
            f"external dataset is unreadable: {exc}"
        ) from exc
    if not isinstance(value, dict):
        raise Sp500AdditionInspectionError(
            "external dataset must be an object"
        )
    return value


def inspect_collection(
    status_path: Path,
    *,
    inspected_at: str,
    store_config: HistoricalStoreConfig | None = None,
    public_root: Path = DEFAULT_PUBLIC_ROOT,
    enforce_commit: bool = True,
) -> tuple[Path, dict[str, Any], Path, dict[str, Any]]:
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
        raise Sp500AdditionInspectionError(
            "collection status is incomplete or unsafe"
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
        raise Sp500AdditionInspectionError(
            "collection inspection chronology is invalid"
        )
    plan_path = PROJECT_ROOT / str(status["plan_path"])
    plan = collection._load_plan(
        plan_path, enforce_commit=enforce_commit
    )
    plan_inspection_path = (
        PROJECT_ROOT / str(status["plan_inspection_path"])
    )
    plan_inspection = collection._plan_inspection(
        plan,
        plan_inspection_path,
        enforce_commit=enforce_commit,
    )
    if not (
        status["plan_sha256"] == plan["artifact_sha256"]
        and status["plan_inspection_sha256"]
        == sha256_file(plan_inspection_path)
        and plan_inspection["plan_sha256"] == plan["artifact_sha256"]
    ):
        raise Sp500AdditionInspectionError(
            "collection predecessor binding drifted"
        )
    if plan["lane"] == "confirmation":
        preregistered = _timestamp(
            str(plan["preregistered_at"]), "preregistered_at"
        )
        if started <= preregistered:
            raise Sp500AdditionInspectionError(
                "confirmation outcomes preceded the frozen winner"
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
        raise Sp500AdditionInspectionError(
            "external dataset is missing, unsafe, or drifted"
        )
    observed = _load_external(external_path)
    rebuilt = collection.build_dataset(external_path.parent, plan)
    permanent_missing_task_ids = sorted(
        str(task["task_id"])
        for task in plan["tasks"]
        if collection._read_gzip(
            collection._checkpoint_path(external_path.parent, task)
        ).get("collection_disposition")
        in collection.PERMANENT_MISSING_DISPOSITIONS
    )
    if not (
        observed == rebuilt
        and canonical_sha256(rebuilt) == status["dataset_sha256"]
        and rebuilt["evaluation_dates"] == plan["evaluation_dates"]
        and rebuilt["family_id"] == discovery.FAMILY_ID
        and status["permanent_missing_task_count"]
        == len(permanent_missing_task_ids)
        and status["permanent_missing_task_ids"]
        == permanent_missing_task_ids
        and status["permanent_missing_semantics"] == "missed_trade"
    ):
        raise Sp500AdditionInspectionError(
            "independent dataset reconstruction differs"
        )
    populated_dates = sorted(
        day
        for day, rows in rebuilt["event_metadata_by_entry_date"].items()
        if rows
    )
    if populated_dates != sorted(
        {
            row["entry_date"]
            for rows in rebuilt["event_metadata_by_entry_date"].values()
            for row in rows
        }
    ) or len(populated_dates) != plan["signal_date_capacity"]:
        raise Sp500AdditionInspectionError(
            "runtime signal-date capacity drifted"
        )
    inspection_payload = {
        "schema_version": 1,
        "artifact_kind": "sp500-addition-data-collection-inspection",
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
        "permanent_missing_task_count": len(
            permanent_missing_task_ids
        ),
        "permanent_missing_task_ids_sha256": canonical_sha256(
            permanent_missing_task_ids
        ),
        "collection_started_at": status["collection_started_at"],
        "collection_completed_at": status[
            "collection_completed_at"
        ],
        "inspected_at": inspected_at,
        "broker_actions": 0,
        "checks": {
            "all_tasks_rebuilt": True,
            "external_file_rehashed": True,
            "dataset_canonical_hash_rebuilt": True,
            "runtime_schema_revalidated": True,
            "event_scope_exact": True,
            "missing_rows_retained_without_substitution": True,
            "permanent_missing_tasks_rebuilt": True,
            "confirmation_after_preregistration": (
                plan["lane"] != "confirmation" or started
                > _timestamp(
                    str(plan["preregistered_at"]),
                    "preregistered_at",
                )
            ),
        },
    }
    inspection_path, inspection = strategy_discovery._write_artifact(
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
            collection._repo_path(plan_inspection_path),
            collection._repo_path(status_path),
            collection._repo_path(inspection_path),
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
    return inspection_path, inspection, manifest_path, manifest


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--public-root", type=Path, default=DEFAULT_PUBLIC_ROOT
    )
    sub = parser.add_subparsers(dest="command", required=True)
    plan = sub.add_parser("inspect-plan")
    plan.add_argument("artifact", type=Path)
    plan.add_argument("--inspected-at", required=True)
    status = sub.add_parser("inspect-collection")
    status.add_argument("artifact", type=Path)
    status.add_argument("--inspected-at", required=True)
    return parser


def main() -> int:
    args = _parser().parse_args()
    try:
        if args.command == "inspect-plan":
            path, artifact = inspect_plan(
                args.artifact,
                inspected_at=args.inspected_at,
                public_root=args.public_root,
            )
            result = {
                "inspection": collection._repo_path(path),
                "inspection_sha256": artifact["artifact_sha256"],
                "state": artifact["state"],
                "broker_actions": 0,
            }
        else:
            path, artifact, manifest_path, manifest = (
                inspect_collection(
                    args.artifact,
                    inspected_at=args.inspected_at,
                    public_root=args.public_root,
                )
            )
            result = {
                "inspection": collection._repo_path(path),
                "inspection_sha256": artifact["artifact_sha256"],
                "manifest": collection._repo_path(manifest_path),
                "manifest_sha256": manifest["manifest_sha256"],
                "state": artifact["state"],
                "broker_actions": 0,
            }
    except (
        Sp500AdditionInspectionError,
        collection.Sp500AdditionCollectionError,
        HistoricalStoreError,
        LearningDataError,
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
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

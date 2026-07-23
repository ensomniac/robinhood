"""Record outcome-safe collection failures and freeze bounded recovery plans."""

from __future__ import annotations

import argparse
import json
from collections.abc import Mapping
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Any

import dense_data_collection as collection
import dense_strategy_runtime as runtime
import outcome_exposure
import strategy_discovery
from historical_store import (
    DEFAULT_ENV_PATH,
    HistoricalStoreConfig,
    canonical_sha256,
)


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_PUBLIC_ROOT = collection.DEFAULT_PUBLIC_ROOT
FAILURE_KIND = "dense-data-collection-failure"
FAILURE_STATE = "COLLECTION_FAILED_NO_STRATEGY_METRICS"
GROUPED_DAILY_FAILURE = "GROUPED_DAILY_TASK_FAILED_BEFORE_PRICE_ACCESS"
INCOMPLETE_INTRADAY = "INCOMPLETE_SIP_REGULAR_SESSION"
RECOVERY_IMPLEMENTATION_FILES = (
    "dense_collection_recovery.py",
    "dense_data_collection.py",
)


class DenseCollectionRecoveryError(RuntimeError):
    """A failure record or recovery plan cannot be proven safe."""


def _implementation_hashes(*, enforce_commit: bool) -> dict[str, str]:
    result: dict[str, str] = {}
    for name in RECOVERY_IMPLEMENTATION_FILES:
        path = PROJECT_ROOT / name
        if enforce_commit:
            strategy_discovery.require_committed(path)
        result[name] = strategy_discovery._file_hash(path)
    return result


def _timestamp(value: str, field: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise DenseCollectionRecoveryError(f"{field} is invalid") from exc
    if parsed.tzinfo is None:
        raise DenseCollectionRecoveryError(f"{field} needs a timezone")
    return parsed


def _private_root(
    config: HistoricalStoreConfig, plan: Mapping[str, Any]
) -> Path:
    return (
        config.root
        / "dense-v2"
        / str(plan["family_id"])
        / str(plan["lane"])
        / str(plan["artifact_sha256"])
    )


def _checkpoint_rows(
    root: Path, task: Mapping[str, Any]
) -> list[dict[str, Any]] | None:
    path = collection._checkpoint_path(root, task)
    if not path.exists():
        return None
    return collection._load_checkpoint(path, task)


def _regular_session_complete(rows: list[dict[str, Any]], day: str) -> bool:
    observed: list[datetime] = []
    for row in rows:
        raw = row.get("time_et")
        if not isinstance(raw, str):
            return False
        try:
            timestamp = datetime.fromisoformat(raw)
        except ValueError:
            return False
        if timestamp.tzinfo is None or timestamp.date().isoformat() != day:
            return False
        observed.append(timestamp)
    return (
        len(observed) == 390
        and observed == sorted(observed)
        and len(observed) == len(set(observed))
        and observed[0].timetz().replace(tzinfo=None) == time(9, 30)
        and observed[-1].timetz().replace(tzinfo=None) == time(15, 59)
        and all(
            right - left == timedelta(minutes=1)
            for left, right in zip(observed, observed[1:])
        )
    )


def _failure_facts(
    plan: Mapping[str, Any],
    root: Path,
    telemetry: Mapping[str, Any],
) -> dict[str, Any]:
    completed = 0
    rows_accessed = 0
    corporate_action_rows_accessed = 0
    market_price_tasks_completed = 0
    evaluation_tasks_completed = 0
    intraday_gaps: list[dict[str, Any]] = []
    evaluation_dates = set(map(str, plan["evaluation_dates"]))
    for task in plan["tasks"]:
        rows = _checkpoint_rows(root, task)
        if rows is None:
            continue
        completed += 1
        if task["kind"] == "split_actions":
            corporate_action_rows_accessed += len(rows)
        elif task["kind"] in {
            "grouped_daily_bars",
            "daily_symbol_bars",
            "massive_daily_symbol_bars",
            "sip_minute_bars",
        }:
            market_price_tasks_completed += 1
            rows_accessed += len(rows)
        if str(task["date"]) in evaluation_dates:
            evaluation_tasks_completed += 1
        if (
            task["kind"] == "sip_minute_bars"
            and not _regular_session_complete(rows, str(task["date"]))
        ):
            intraday_gaps.append(
                {
                    "date": str(task["date"]),
                    "symbol": str(task["symbol"]),
                    "observed_minutes": len(rows),
                    "expected_minutes": 390,
                }
            )
    failures = int(telemetry.get("failures", 0))
    if (
        str(plan["tasks"][0]["kind"]) == "split_actions"
        and completed == 1
        and market_price_tasks_completed == 0
        and failures >= 1
    ):
        return {
            "failure_code": GROUPED_DAILY_FAILURE,
            "completed_tasks": 1,
            "market_price_rows_accessed": 0,
            "evaluation_tasks_completed": 0,
            "data_outcomes_accessed": False,
            "exposure_scope": None,
            "failure_details": {
                "completed_metadata_task_kind": "split_actions",
                "corporate_action_rows_accessed": (
                    corporate_action_rows_accessed
                ),
                "failed_task_kind": "grouped_daily_bars",
                "price_tasks_started": 0,
                "provider_failures": failures,
            },
        }
    if (
        plan["family_id"] == runtime.INTRADAY_ETF_FAMILY
        and completed == int(plan["task_count"])
        and intraday_gaps
    ):
        return {
            "failure_code": INCOMPLETE_INTRADAY,
            "completed_tasks": completed,
            "market_price_rows_accessed": rows_accessed,
            "evaluation_tasks_completed": evaluation_tasks_completed,
            "data_outcomes_accessed": evaluation_tasks_completed > 0,
            "exposure_scope": {
                "dates": list(plan["evaluation_dates"]),
                "symbols": sorted(map(str, plan["symbols"])),
            },
            "failure_details": {
                "incomplete_sessions": intraday_gaps,
                "substituted_sessions": 0,
                "interpolated_minutes": 0,
            },
        }
    raise DenseCollectionRecoveryError(
        "private checkpoints do not prove a supported collection failure"
    )


def record_failure(
    plan_path: Path,
    *,
    recorded_at: str,
    store_config: HistoricalStoreConfig | None = None,
    public_root: Path = DEFAULT_PUBLIC_ROOT,
    enforce_commit: bool = True,
) -> tuple[Path, dict[str, Any]]:
    observed_at = _timestamp(recorded_at, "recorded_at")
    plan = collection._validate_plan(plan_path, enforce_commit=enforce_commit)
    config = store_config or HistoricalStoreConfig.from_env(DEFAULT_ENV_PATH)
    root = _private_root(config, plan)
    telemetry_state = collection._telemetry_state(
        root / "collection-telemetry.json",
        str(plan["artifact_sha256"]),
    )
    started_raw = telemetry_state.get("collection_started_at")
    if not isinstance(started_raw, str):
        raise DenseCollectionRecoveryError(
            "collection has no persisted provider-attempt chronology"
        )
    started_at = _timestamp(started_raw, "collection_started_at")
    if observed_at <= started_at:
        raise DenseCollectionRecoveryError(
            "failure record must follow the provider attempt"
        )
    facts = _failure_facts(
        plan,
        root,
        telemetry_state["provider_telemetry"],
    )
    payload = {
        "schema_version": 1,
        "artifact_kind": FAILURE_KIND,
        "campaign_id": plan["campaign_id"],
        "state": FAILURE_STATE,
        "family_id": plan["family_id"],
        "lane": plan["lane"],
        "plan_path": collection._repo_path(plan_path),
        "plan_sha256": plan["artifact_sha256"],
        "authority_sha256": plan["authority_sha256"],
        "binding_sha256": plan["binding_sha256"],
        "failure_code": facts["failure_code"],
        "completed_tasks": facts["completed_tasks"],
        "task_count": plan["task_count"],
        "market_price_rows_accessed": facts["market_price_rows_accessed"],
        "evaluation_tasks_completed": facts["evaluation_tasks_completed"],
        "data_outcomes_accessed": facts["data_outcomes_accessed"],
        "exposure_scope": facts["exposure_scope"],
        "failure_details": facts["failure_details"],
        "strategy_metrics_accessed": False,
        "confirmation_outcomes_accessed": False,
        "substitutions": 0,
        "broker_actions": 0,
        "provider_telemetry": telemetry_state["provider_telemetry"],
        "collection_started_at": started_raw,
        "recorded_at": recorded_at,
    }
    return strategy_discovery._write_artifact(
        payload,
        public_root / str(plan["family_id"]) / f"{plan['lane']}-collection-failure",
        f"{plan['family_id']}-{plan['lane']}-collection-failure",
    )


def _load_failure(
    path: Path, *, enforce_commit: bool
) -> dict[str, Any]:
    if enforce_commit:
        strategy_discovery.require_committed(path)
    failure = strategy_discovery.load_artifact(path, expected_kind=FAILURE_KIND)
    if not (
        failure.get("state") == FAILURE_STATE
        and failure.get("strategy_metrics_accessed") is False
        and failure.get("confirmation_outcomes_accessed") is False
        and failure.get("substitutions") == 0
        and failure.get("broker_actions") == 0
    ):
        raise DenseCollectionRecoveryError("collection failure artifact is unsafe")
    return failure


def freeze_pullback_recovery(
    failure_path: Path,
    *,
    as_of: date | None = None,
    actual_today: date | None = None,
    public_root: Path = DEFAULT_PUBLIC_ROOT,
    enforce_commit: bool = True,
) -> tuple[Path, dict[str, Any]]:
    failure = _load_failure(failure_path, enforce_commit=enforce_commit)
    if not (
        failure.get("family_id") == runtime.ETF_PULLBACK_FAMILY
        and failure.get("lane") == "development"
        and failure.get("failure_code") == GROUPED_DAILY_FAILURE
        and failure.get("data_outcomes_accessed") is False
        and failure.get("completed_tasks") == 1
    ):
        raise DenseCollectionRecoveryError(
            "only the outcome-blind ETF pullback grouped-daily failure is recoverable"
        )
    original_path = PROJECT_ROOT / str(failure["plan_path"])
    plan = collection._validate_plan(
        original_path,
        enforce_commit=enforce_commit,
    )
    if plan["artifact_sha256"] != failure["plan_sha256"]:
        raise DenseCollectionRecoveryError("failed plan binding drifted")
    today = actual_today or date.today()
    current = as_of or today
    if current > today:
        raise DenseCollectionRecoveryError("recovery as_of cannot be future-dated")
    symbols = sorted(map(str, plan["symbols"]))
    if not symbols:
        raise DenseCollectionRecoveryError("pullback recovery lacks frozen symbols")
    tasks: list[dict[str, Any]] = [dict(plan["tasks"][0])]
    for symbol in symbols:
        task = {
            "kind": "daily_symbol_bars",
            "date": plan["required_dates"][-1],
            "start": plan["required_dates"][0],
            "symbol": symbol,
        }
        task["task_id"] = canonical_sha256(task)
        tasks.append(task)
    payload = {
        key: value
        for key, value in plan.items()
        if key
        not in {
            "artifact_sha256",
            "as_of",
            "providers",
            "tasks",
            "task_count",
        }
    }
    payload.update(
        {
            "state": "COLLECTION_PLAN_FROZEN",
            "as_of": current.isoformat(),
            "tasks": tasks,
            "task_count": len(tasks),
            "providers": [
                "Alpaca SIP raw-adjustment daily bars by frozen symbol range",
                "Massive point-in-time split actions through the final frozen session",
            ],
            "adjustment_semantics": collection.RECOVERY_ADJUSTMENT,
            "recovery_failure_path": collection._repo_path(failure_path),
            "recovery_failure_sha256": failure["artifact_sha256"],
            "supersedes_plan_sha256": plan["artifact_sha256"],
            "recovery_implementation_hashes": _implementation_hashes(
                enforce_commit=enforce_commit
            ),
            "provider_requests_before_plan_freeze": 0,
            "market_outcomes_accessed": False,
            "substitutions_allowed": False,
            "broker_actions": 0,
        }
    )
    return strategy_discovery._write_artifact(
        payload,
        public_root
        / runtime.ETF_PULLBACK_FAMILY
        / "development-collection-plan",
        f"{runtime.ETF_PULLBACK_FAMILY}-development-collection-plan",
    )


def index_failure_exposure(
    failure_path: Path,
    *,
    index_path: Path = outcome_exposure.DEFAULT_INDEX,
    enforce_commit: bool = True,
) -> tuple[dict[str, Any], bool]:
    failure = _load_failure(failure_path, enforce_commit=enforce_commit)
    if failure.get("data_outcomes_accessed") is not True:
        raise DenseCollectionRecoveryError(
            "outcome-blind failures must not enter the exposure index"
        )
    scope = failure.get("exposure_scope")
    if not isinstance(scope, Mapping):
        raise DenseCollectionRecoveryError("failure exposure scope is missing")
    record = outcome_exposure.build_record(
        exposure_id=(
            f"dense-collection-failure-{failure['artifact_sha256'][:20]}"
        ),
        campaign_id=str(failure["campaign_id"]),
        lane=str(failure["lane"]),
        recorded_at=str(failure["recorded_at"]),
        source_path=collection._repo_path(failure_path),
        source_sha256=str(failure["artifact_sha256"]),
        scope=scope,
    )
    appended = outcome_exposure.ensure_record(record, index_path)
    return record, appended


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--public-root", type=Path, default=DEFAULT_PUBLIC_ROOT)
    subparsers = parser.add_subparsers(dest="command", required=True)
    failure = subparsers.add_parser("record-failure")
    failure.add_argument("artifact", type=Path)
    failure.add_argument("--recorded-at", required=True)
    recovery = subparsers.add_parser("freeze-pullback-recovery")
    recovery.add_argument("artifact", type=Path)
    recovery.add_argument("--as-of", type=date.fromisoformat)
    exposure = subparsers.add_parser("index-exposure")
    exposure.add_argument("artifact", type=Path)
    exposure.add_argument(
        "--index",
        type=Path,
        default=outcome_exposure.DEFAULT_INDEX,
    )
    return parser


def main() -> int:
    args = _parser().parse_args()
    try:
        if args.command == "record-failure":
            path, artifact = record_failure(
                args.artifact,
                recorded_at=args.recorded_at,
                public_root=args.public_root,
            )
            result = {
                "written": collection._repo_path(path),
                "artifact_sha256": artifact["artifact_sha256"],
                "state": artifact["state"],
                "failure_code": artifact["failure_code"],
                "data_outcomes_accessed": artifact["data_outcomes_accessed"],
            }
        elif args.command == "freeze-pullback-recovery":
            path, artifact = freeze_pullback_recovery(
                args.artifact,
                as_of=args.as_of,
                public_root=args.public_root,
            )
            result = {
                "written": collection._repo_path(path),
                "artifact_sha256": artifact["artifact_sha256"],
                "state": artifact["state"],
                "task_count": artifact["task_count"],
            }
        else:
            record, appended = index_failure_exposure(
                args.artifact,
                index_path=args.index,
            )
            result = {
                "exposure_id": record["exposure_id"],
                "record_sha256": record["record_sha256"],
                "appended": appended,
            }
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    except (
        DenseCollectionRecoveryError,
        collection.DenseDataCollectionError,
        outcome_exposure.OutcomeExposureError,
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

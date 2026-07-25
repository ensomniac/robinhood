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
MASSIVE_DAILY_SYMBOL_FAILURE = (
    "MASSIVE_DAILY_SYMBOL_TASK_FAILED_BEFORE_PRICE_ACCESS"
)
INCOMPLETE_INTRADAY = "INCOMPLETE_SIP_REGULAR_SESSION"
INCOMPLETE_INTRADAY_RANGE = "INCOMPLETE_SIP_RANGE_REGULAR_SESSION"
EMPTY_MISSED_DATE_REPRESENTATION = (
    "EMPTY_INTRADAY_MISSED_DATE_REPRESENTATION_GAP"
)
INCOMPLETE_FIXED_DAILY_RANGE = "INCOMPLETE_FIXED_DAILY_SYMBOL_RANGE"
RECOVERY_IMPLEMENTATION_FILES = (
    "dense_collection_recovery.py",
    "dense_collection_recovery_inspection.py",
    "dense_data_collection.py",
)
INTRADAY_RECOVERY_IMPLEMENTATION_FILES = (
    *RECOVERY_IMPLEMENTATION_FILES,
    "dense_strategy_runtime.py",
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


def _intraday_recovery_implementation_hashes(
    *, enforce_commit: bool
) -> dict[str, str]:
    result: dict[str, str] = {}
    for name in INTRADAY_RECOVERY_IMPLEMENTATION_FILES:
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
    intraday_range_summaries: list[dict[str, Any]] = []
    intraday_range_extra_dates: set[str] = set()
    daily_range_summaries: list[dict[str, Any]] = []
    evaluation_dates = set(map(str, plan["evaluation_dates"]))
    required_dates = set(map(str, plan.get("required_dates", [])))
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
            "sip_minute_symbol_range",
        }:
            market_price_tasks_completed += 1
            rows_accessed += len(rows)
        if task["kind"] == "sip_minute_symbol_range":
            grouped: dict[str, list[dict[str, Any]]] = {}
            for row in rows:
                day = str(row.get("date_et"))
                grouped.setdefault(day, []).append(row)
            complete_required = 0
            complete_evaluation = 0
            for day in sorted(required_dates):
                day_rows = grouped.get(day, [])
                if _regular_session_complete(day_rows, day):
                    complete_required += 1
                    if day in evaluation_dates:
                        complete_evaluation += 1
            represented_evaluation = evaluation_dates & set(grouped)
            if represented_evaluation:
                evaluation_tasks_completed += 1
            intraday_range_extra_dates.update(set(grouped) - required_dates)
            intraday_range_summaries.append(
                {
                    "symbol": str(task["symbol"]),
                    "required_sessions": len(required_dates),
                    "complete_required_sessions": complete_required,
                    "incomplete_required_sessions": (
                        len(required_dates) - complete_required
                    ),
                    "evaluation_sessions": len(evaluation_dates),
                    "complete_evaluation_sessions": complete_evaluation,
                    "incomplete_evaluation_sessions": (
                        len(evaluation_dates) - complete_evaluation
                    ),
                }
            )
        elif str(task["date"]) in evaluation_dates:
            evaluation_tasks_completed += 1
        if task["kind"] in {
            "daily_symbol_bars",
            "massive_daily_symbol_bars",
        }:
            observed_daily_dates = {
                str(row.get("date")) for row in rows
            }
            complete_evaluation = evaluation_dates & observed_daily_dates
            daily_range_summaries.append(
                {
                    "symbol": str(task["symbol"]),
                    "required_sessions": len(required_dates),
                    "complete_required_sessions": len(
                        required_dates & observed_daily_dates
                    ),
                    "missing_required_sessions": sorted(
                        required_dates - observed_daily_dates
                    ),
                    "evaluation_sessions": len(evaluation_dates),
                    "complete_evaluation_sessions": len(
                        complete_evaluation
                    ),
                    "missing_evaluation_sessions": sorted(
                        evaluation_dates - observed_daily_dates
                    ),
                }
            )
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
    failed_task = next(
        (
            task
            for task in plan["tasks"]
            if _checkpoint_rows(root, task) is None
        ),
        None,
    )
    failed_kind = (
        str(failed_task.get("kind"))
        if isinstance(failed_task, Mapping)
        else None
    )
    if (
        str(plan["tasks"][0]["kind"]) == "split_actions"
        and completed == 1
        and market_price_tasks_completed == 0
        and failures >= 1
        and failed_kind in {
            "grouped_daily_bars",
            "massive_daily_symbol_bars",
        }
    ):
        failure_code = (
            GROUPED_DAILY_FAILURE
            if failed_kind == "grouped_daily_bars"
            else MASSIVE_DAILY_SYMBOL_FAILURE
        )
        return {
            "failure_code": failure_code,
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
                "failed_task_kind": failed_kind,
                "price_tasks_completed": 0,
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
    if (
        plan["family_id"] in runtime.INTRADAY_ETF_FAMILIES
        and completed == int(plan["task_count"])
        and intraday_range_summaries
        and any(
            item["incomplete_required_sessions"] > 0
            for item in intraday_range_summaries
        )
    ):
        intraday_range_summaries.sort(key=lambda item: item["symbol"])
        complete_evaluation_sets: list[set[str]] = []
        for task in plan["tasks"]:
            rows = _checkpoint_rows(root, task)
            if rows is None or task["kind"] != "sip_minute_symbol_range":
                continue
            grouped: dict[str, list[dict[str, Any]]] = {}
            for row in rows:
                grouped.setdefault(str(row.get("date_et")), []).append(row)
            complete_evaluation_sets.append(
                {
                    day
                    for day in evaluation_dates
                    if _regular_session_complete(grouped.get(day, []), day)
                }
            )
        fully_complete_evaluation_dates = (
            set.intersection(*complete_evaluation_sets)
            if complete_evaluation_sets
            else set()
        )
        represented_by_any_complete_session = (
            set.union(*complete_evaluation_sets)
            if complete_evaluation_sets
            else set()
        )
        fully_incomplete_evaluation_dates = (
            evaluation_dates - represented_by_any_complete_session
        )
        if (
            plan.get("intraday_missing_session_policy")
            == collection.INTRADAY_FIXED_UNIVERSE_MISS_POLICY
            and fully_incomplete_evaluation_dates
            and fully_complete_evaluation_dates
        ):
            return {
                "failure_code": EMPTY_MISSED_DATE_REPRESENTATION,
                "completed_tasks": completed,
                "market_price_rows_accessed": rows_accessed,
                "evaluation_tasks_completed": evaluation_tasks_completed,
                "data_outcomes_accessed": True,
                "exposure_scope": {
                    "dates": list(plan["evaluation_dates"]),
                    "symbols": sorted(map(str, plan["symbols"])),
                },
                "failure_details": {
                    "expected_regular_session_minutes": 390,
                    "required_symbol_sessions": (
                        len(required_dates) * len(plan["symbols"])
                    ),
                    "incomplete_required_symbol_sessions": sum(
                        int(item["incomplete_required_sessions"])
                        for item in intraday_range_summaries
                    ),
                    "fully_complete_evaluation_dates": len(
                        fully_complete_evaluation_dates
                    ),
                    "fully_incomplete_evaluation_dates": sorted(
                        fully_incomplete_evaluation_dates
                    ),
                    "per_symbol": intraday_range_summaries,
                    "provider_extra_session_dates": sorted(
                        intraday_range_extra_dates
                    ),
                    "substituted_sessions": 0,
                    "interpolated_minutes": 0,
                },
            }
        return {
            "failure_code": INCOMPLETE_INTRADAY_RANGE,
            "completed_tasks": completed,
            "market_price_rows_accessed": rows_accessed,
            "evaluation_tasks_completed": evaluation_tasks_completed,
            "data_outcomes_accessed": evaluation_tasks_completed > 0,
            "exposure_scope": {
                "dates": list(plan["evaluation_dates"]),
                "symbols": sorted(map(str, plan["symbols"])),
            },
            "failure_details": {
                "expected_regular_session_minutes": 390,
                "required_symbol_sessions": (
                    len(required_dates) * len(plan["symbols"])
                ),
                "incomplete_required_symbol_sessions": sum(
                    int(item["incomplete_required_sessions"])
                    for item in intraday_range_summaries
                ),
                "fully_complete_evaluation_dates": len(
                    fully_complete_evaluation_dates
                ),
                "per_symbol": intraday_range_summaries,
                "provider_extra_session_dates": sorted(
                    intraday_range_extra_dates
                ),
                "substituted_sessions": 0,
                "interpolated_minutes": 0,
            },
        }
    if (
        completed == int(plan["task_count"])
        and daily_range_summaries
        and any(
            item["missing_required_sessions"]
            for item in daily_range_summaries
        )
    ):
        daily_range_summaries.sort(key=lambda item: item["symbol"])
        evaluation_rows_accessed = sum(
            int(item["complete_evaluation_sessions"])
            for item in daily_range_summaries
        )
        return {
            "failure_code": INCOMPLETE_FIXED_DAILY_RANGE,
            "completed_tasks": completed,
            "market_price_rows_accessed": rows_accessed,
            "evaluation_tasks_completed": evaluation_tasks_completed,
            "data_outcomes_accessed": evaluation_rows_accessed > 0,
            "exposure_scope": {
                "dates": list(plan["evaluation_dates"]),
                "symbols": sorted(map(str, plan["symbols"])),
            },
            "failure_details": {
                "required_symbol_sessions": (
                    len(required_dates) * len(plan["symbols"])
                ),
                "missing_required_symbol_sessions": sum(
                    len(item["missing_required_sessions"])
                    for item in daily_range_summaries
                ),
                "complete_evaluation_symbol_sessions": (
                    evaluation_rows_accessed
                ),
                "per_symbol": daily_range_summaries,
                "substituted_sessions": 0,
                "interpolated_sessions": 0,
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


def _load_inspected_failure(
    inspection_path: Path,
    *,
    enforce_commit: bool,
) -> tuple[Path, dict[str, Any], dict[str, Any]]:
    if enforce_commit:
        strategy_discovery.require_committed(inspection_path)
    inspection = strategy_discovery.load_artifact(
        inspection_path,
        expected_kind="dense-data-collection-failure-inspection",
    )
    checks = inspection.get("checks")
    if not (
        inspection.get("state") == "COLLECTION_FAILURE_INSPECTED"
        and isinstance(checks, Mapping)
        and checks
        and all(checks.values())
        and inspection.get("strategy_metrics_accessed") is False
        and inspection.get("confirmation_outcomes_accessed") is False
    ):
        raise DenseCollectionRecoveryError(
            "collection failure inspection is incomplete"
        )
    failure_path = PROJECT_ROOT / str(inspection["failure_path"])
    failure = _load_failure(
        failure_path,
        enforce_commit=enforce_commit,
    )
    if not (
        inspection.get("failure_sha256") == failure["artifact_sha256"]
        and inspection.get("plan_sha256") == failure["plan_sha256"]
        and inspection.get("family_id") == failure["family_id"]
        and inspection.get("lane") == failure["lane"]
        and inspection.get("failure_code") == failure["failure_code"]
        and inspection.get("data_outcomes_accessed")
        == failure["data_outcomes_accessed"]
        and inspection.get("exposure_scope") == failure["exposure_scope"]
    ):
        raise DenseCollectionRecoveryError(
            "collection failure inspection binding drifted"
        )
    return failure_path, failure, inspection


def freeze_pullback_recovery(
    failure_inspection_path: Path,
    *,
    search_path: Path | None = None,
    as_of: date | None = None,
    actual_today: date | None = None,
    public_root: Path = DEFAULT_PUBLIC_ROOT,
    enforce_commit: bool = True,
) -> tuple[Path, dict[str, Any]]:
    failure_path, failure, failure_inspection = _load_inspected_failure(
        failure_inspection_path,
        enforce_commit=enforce_commit,
    )
    recoverable_family = str(failure.get("family_id"))
    recoverable_source_failure = (
        (
            recoverable_family == runtime.ETF_PULLBACK_FAMILY
            and failure.get("failure_code") == GROUPED_DAILY_FAILURE
        )
        or (
            recoverable_family
            == runtime.CROSS_STYLE_BREADTH_CONTINUATION_FAMILY
            and failure.get("failure_code")
            == MASSIVE_DAILY_SYMBOL_FAILURE
        )
    )
    if not (
        recoverable_source_failure
        and failure.get("lane") == "development"
        and failure.get("data_outcomes_accessed") is False
        and failure.get("completed_tasks") == 1
        and failure.get("market_price_rows_accessed") == 0
    ):
        raise DenseCollectionRecoveryError(
            "only an approved outcome-blind fixed-ETF source failure is recoverable"
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
    search_refresh: dict[str, Any] | None = None
    if search_path is not None:
        if enforce_commit:
            strategy_discovery.require_committed(search_path)
        refreshed_search = strategy_discovery.load_artifact(
            search_path,
            expected_kind="frozen-development-search",
        )
        original_search_path = PROJECT_ROOT / str(plan["authority_path"])
        if enforce_commit:
            strategy_discovery.require_committed(original_search_path)
        original_search = strategy_discovery.load_artifact(
            original_search_path,
            expected_kind="frozen-development-search",
        )
        original_contract = dict(original_search["family_contract"])
        refreshed_contract = dict(refreshed_search["family_contract"])
        original_contract.pop("implementation_hashes", None)
        refreshed_contract.pop("implementation_hashes", None)
        if not (
            refreshed_search.get("state") == "SEARCH_FROZEN"
            and original_contract == refreshed_contract
            and refreshed_search["family_contract"]["family_id"]
            == recoverable_family
        ):
            raise DenseCollectionRecoveryError(
                "refreshed search changed frozen strategy semantics"
            )
        strategy_discovery._assert_implementation_current(
            refreshed_search["family_contract"],
            enforce_commit=enforce_commit,
        )
        search_refresh = {
            "original_search_sha256": original_search["artifact_sha256"],
            "refreshed_search_path": collection._repo_path(search_path),
            "refreshed_search_sha256": refreshed_search["artifact_sha256"],
            "semantic_contract_sha256": canonical_sha256(
                refreshed_contract
            ),
            "only_implementation_hashes_changed": True,
        }
        plan = {
            **plan,
            "authority_path": collection._repo_path(search_path),
            "authority_sha256": refreshed_search["artifact_sha256"],
            "binding_sha256": refreshed_search["artifact_sha256"],
        }
    symbols = sorted(map(str, plan["symbols"]))
    if not symbols:
        raise DenseCollectionRecoveryError(
            "fixed-ETF recovery lacks frozen symbols"
        )
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
            "recovery_failure_inspection_path": collection._repo_path(
                failure_inspection_path
            ),
            "recovery_failure_inspection_sha256": failure_inspection[
                "artifact_sha256"
            ],
            "supersedes_plan_sha256": plan["artifact_sha256"],
            "recovery_implementation_hashes": _implementation_hashes(
                enforce_commit=enforce_commit
            ),
            "provider_requests_before_plan_freeze": 0,
            "market_outcomes_accessed": False,
            "substitutions_allowed": False,
            "broker_actions": 0,
            **(
                {"recovery_search_refresh": search_refresh}
                if search_refresh is not None
                else {}
            ),
        }
    )
    return strategy_discovery._write_artifact(
        payload,
        public_root
        / recoverable_family
        / "development-collection-plan",
        f"{recoverable_family}-development-collection-plan",
    )


def freeze_intraday_representation_recovery(
    failure_inspection_path: Path,
    *,
    search_path: Path,
    as_of: date | None = None,
    actual_today: date | None = None,
    public_root: Path = DEFAULT_PUBLIC_ROOT,
    enforce_commit: bool = True,
) -> tuple[Path, dict[str, Any]]:
    """Rebind cached range checkpoints after an implementation-only repair."""

    failure_path, failure, failure_inspection = _load_inspected_failure(
        failure_inspection_path,
        enforce_commit=enforce_commit,
    )
    if not (
        failure.get("family_id")
        == runtime.LIQUID_INDEX_ETF_OPENING_REVERSAL_POST2016_FAMILY
        and failure.get("lane") == "development"
        and failure.get("failure_code")
        == EMPTY_MISSED_DATE_REPRESENTATION
        and failure.get("data_outcomes_accessed") is True
        and failure.get("strategy_metrics_accessed") is False
        and failure.get("completed_tasks") == failure.get("task_count")
    ):
        raise DenseCollectionRecoveryError(
            "failure is not the bounded empty-missed-date representation gap"
        )
    original_plan_path = PROJECT_ROOT / str(failure["plan_path"])
    original_plan = collection._validate_plan(
        original_plan_path,
        enforce_commit=enforce_commit,
    )
    if enforce_commit:
        strategy_discovery.require_committed(search_path)
    refreshed_search = strategy_discovery.load_artifact(
        search_path,
        expected_kind="frozen-development-search",
    )
    original_search_path = (
        PROJECT_ROOT / str(original_plan["authority_path"])
    )
    if enforce_commit:
        strategy_discovery.require_committed(original_search_path)
    original_search = strategy_discovery.load_artifact(
        original_search_path,
        expected_kind="frozen-development-search",
    )
    original_contract = dict(original_search["family_contract"])
    refreshed_contract = dict(refreshed_search["family_contract"])
    original_contract.pop("implementation_hashes", None)
    refreshed_contract.pop("implementation_hashes", None)
    if not (
        original_plan["artifact_sha256"] == failure["plan_sha256"]
        and original_contract == refreshed_contract
        and refreshed_search.get("state") == "SEARCH_FROZEN"
        and refreshed_search["family_contract"]["family_id"]
        == failure["family_id"]
    ):
        raise DenseCollectionRecoveryError(
            "refreshed intraday search changed frozen strategy semantics"
        )
    strategy_discovery._assert_implementation_current(
        refreshed_search["family_contract"],
        enforce_commit=enforce_commit,
    )
    today = actual_today or date.today()
    current = as_of or today
    if current > today:
        raise DenseCollectionRecoveryError(
            "intraday recovery as_of cannot be future-dated"
        )
    payload = {
        key: value
        for key, value in original_plan.items()
        if key not in {"artifact_sha256", "as_of"}
    }
    refreshed_contract_hash = canonical_sha256(refreshed_contract)
    payload.update(
        {
            "authority_path": collection._repo_path(search_path),
            "authority_sha256": refreshed_search["artifact_sha256"],
            "binding_sha256": refreshed_search["artifact_sha256"],
            "as_of": current.isoformat(),
            "recovery_kind": collection.INTRADAY_CHECKPOINT_REUSE_RECOVERY,
            "recovery_failure_path": collection._repo_path(failure_path),
            "recovery_failure_sha256": failure["artifact_sha256"],
            "recovery_failure_inspection_path": collection._repo_path(
                failure_inspection_path
            ),
            "recovery_failure_inspection_sha256": failure_inspection[
                "artifact_sha256"
            ],
            "supersedes_plan_sha256": original_plan["artifact_sha256"],
            "checkpoint_source_plan_path": collection._repo_path(
                original_plan_path
            ),
            "checkpoint_source_plan_sha256": original_plan[
                "artifact_sha256"
            ],
            "recovery_search_refresh": {
                "original_search_sha256": original_search[
                    "artifact_sha256"
                ],
                "refreshed_search_path": collection._repo_path(search_path),
                "refreshed_search_sha256": refreshed_search[
                    "artifact_sha256"
                ],
                "semantic_contract_sha256": refreshed_contract_hash,
                "only_implementation_hashes_changed": True,
            },
            "recovery_implementation_hashes": (
                _intraday_recovery_implementation_hashes(
                    enforce_commit=enforce_commit
                )
            ),
            "provider_requests_before_plan_freeze": 0,
            "provider_requests_already_completed": int(
                failure["provider_telemetry"]["requests"]
            ),
            "additional_provider_requests_authorized": 0,
            "market_outcomes_accessed": True,
            "strategy_metrics_accessed_before_recovery": False,
            "substitutions_allowed": False,
            "broker_actions": 0,
        }
    )
    return strategy_discovery._write_artifact(
        payload,
        public_root
        / str(failure["family_id"])
        / "development-collection-plan",
        f"{failure['family_id']}-development-collection-plan",
    )


def index_failure_exposure(
    failure_inspection_path: Path,
    *,
    index_path: Path = outcome_exposure.DEFAULT_INDEX,
    enforce_commit: bool = True,
) -> tuple[dict[str, Any], bool]:
    failure_path, failure, _inspection = _load_inspected_failure(
        failure_inspection_path,
        enforce_commit=enforce_commit,
    )
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
    recovery.add_argument("--search", type=Path)
    fixed_etf_recovery = subparsers.add_parser(
        "freeze-fixed-etf-source-recovery"
    )
    fixed_etf_recovery.add_argument("artifact", type=Path)
    fixed_etf_recovery.add_argument("--as-of", type=date.fromisoformat)
    fixed_etf_recovery.add_argument("--search", type=Path)
    intraday = subparsers.add_parser(
        "freeze-intraday-representation-recovery"
    )
    intraday.add_argument("artifact", type=Path)
    intraday.add_argument("--as-of", type=date.fromisoformat)
    intraday.add_argument("--search", type=Path, required=True)
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
        elif args.command in {
            "freeze-pullback-recovery",
            "freeze-fixed-etf-source-recovery",
        }:
            path, artifact = freeze_pullback_recovery(
                args.artifact,
                search_path=args.search,
                as_of=args.as_of,
                public_root=args.public_root,
            )
            result = {
                "written": collection._repo_path(path),
                "artifact_sha256": artifact["artifact_sha256"],
                "state": artifact["state"],
                "task_count": artifact["task_count"],
            }
        elif args.command == "freeze-intraday-representation-recovery":
            path, artifact = freeze_intraday_representation_recovery(
                args.artifact,
                search_path=args.search,
                as_of=args.as_of,
                public_root=args.public_root,
            )
            result = {
                "written": collection._repo_path(path),
                "artifact_sha256": artifact["artifact_sha256"],
                "state": artifact["state"],
                "task_count": artifact["task_count"],
                "additional_provider_requests_authorized": artifact[
                    "additional_provider_requests_authorized"
                ],
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

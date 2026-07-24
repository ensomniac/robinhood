"""Freeze and materialize an outcome-safe intraday dataset from retained checkpoints."""

from __future__ import annotations

import argparse
import json
from collections.abc import Mapping
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import dense_collection_recovery as recovery
import dense_data_collection as collection
import dense_strategy_runtime as runtime
import outcome_exposure
import strategy_discovery
from historical_store import (
    DEFAULT_ENV_PATH,
    HistoricalStoreConfig,
    canonical_sha256,
    sha256_file,
)


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_PUBLIC_ROOT = collection.DEFAULT_PUBLIC_ROOT
PLAN_KIND = "dense-intraday-retained-recovery-plan"
STATUS_KIND = "dense-intraday-retained-recovery-status"
POLICY_NAME = "miss-entire-fixed-universe-entry-date"
IMPLEMENTATION_FILES = (
    "dense_intraday_recovery.py",
    "dense_intraday_recovery_inspection.py",
    "dense_strategy_runtime.py",
)


class DenseIntradayRecoveryError(RuntimeError):
    """Retained intraday evidence cannot be used without changing frozen semantics."""


def _repo_path(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(PROJECT_ROOT.resolve()))
    except ValueError as exc:
        raise DenseIntradayRecoveryError(
            f"path is outside repository: {path}"
        ) from exc


def _implementation_hashes(*, enforce_commit: bool) -> dict[str, str]:
    hashes: dict[str, str] = {}
    for name in IMPLEMENTATION_FILES:
        path = PROJECT_ROOT / name
        if enforce_commit:
            strategy_discovery.require_committed(path)
        hashes[name] = strategy_discovery._file_hash(path)
    return hashes


def _same_semantic_search(
    original_path: Path,
    refreshed_path: Path,
    *,
    enforce_commit: bool,
) -> tuple[dict[str, Any], dict[str, Any], str]:
    if enforce_commit:
        strategy_discovery.require_committed(original_path)
        strategy_discovery.require_committed(refreshed_path)
    original = strategy_discovery.load_artifact(
        original_path, expected_kind="frozen-development-search"
    )
    refreshed = strategy_discovery.load_artifact(
        refreshed_path, expected_kind="frozen-development-search"
    )
    original_contract = dict(original["family_contract"])
    refreshed_contract = dict(refreshed["family_contract"])
    original_contract.pop("implementation_hashes", None)
    refreshed_contract.pop("implementation_hashes", None)
    if not (
        original.get("state") == "SEARCH_FROZEN"
        and refreshed.get("state") == "SEARCH_FROZEN"
        and original_contract == refreshed_contract
        and refreshed_contract.get("family_id") == runtime.INTRADAY_ETF_FAMILY
    ):
        raise DenseIntradayRecoveryError(
            "refreshed search changed frozen intraday strategy semantics"
        )
    strategy_discovery._assert_implementation_current(
        refreshed["family_contract"],
        enforce_commit=enforce_commit,
    )
    return original, refreshed, canonical_sha256(refreshed_contract)


def _exposure_is_indexed(failure: Mapping[str, Any]) -> bool:
    expected_id = (
        f"dense-collection-failure-{failure['artifact_sha256'][:20]}"
    )
    return any(
        record.get("exposure_id") == expected_id
        and record.get("source_sha256") == failure["artifact_sha256"]
        and record.get("scope") == failure["exposure_scope"]
        and record.get("lane") == "development"
        for record in outcome_exposure.read_index()
    )


def _incomplete_sessions(
    failure: Mapping[str, Any],
) -> list[dict[str, Any]]:
    details = failure.get("failure_details")
    rows = (
        details.get("incomplete_sessions")
        if isinstance(details, Mapping)
        else None
    )
    if not isinstance(rows, list) or not rows:
        raise DenseIntradayRecoveryError(
            "intraday failure lacks incomplete-session evidence"
        )
    normalized: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, Mapping):
            raise DenseIntradayRecoveryError(
                "intraday incomplete-session evidence is malformed"
            )
        item = {
            "date": str(row.get("date")),
            "symbol": str(row.get("symbol")),
            "observed_minutes": row.get("observed_minutes"),
            "expected_minutes": row.get("expected_minutes"),
        }
        if (
            item["expected_minutes"] != 390
            or isinstance(item["observed_minutes"], bool)
            or not isinstance(item["observed_minutes"], int)
            or not 0 <= item["observed_minutes"] < 390
        ):
            raise DenseIntradayRecoveryError(
                "intraday incomplete-session counts are invalid"
            )
        normalized.append(item)
    normalized.sort(key=lambda item: (item["date"], item["symbol"]))
    if normalized != rows:
        raise DenseIntradayRecoveryError(
            "intraday incomplete sessions must be canonically ordered"
        )
    return normalized


def freeze_plan(
    failure_inspection_path: Path,
    refreshed_search_path: Path,
    *,
    as_of: date | None = None,
    actual_today: date | None = None,
    public_root: Path = DEFAULT_PUBLIC_ROOT,
    enforce_commit: bool = True,
) -> tuple[Path, dict[str, Any]]:
    failure_path, failure, inspection = recovery._load_inspected_failure(
        failure_inspection_path,
        enforce_commit=enforce_commit,
    )
    if not (
        failure.get("family_id") == runtime.INTRADAY_ETF_FAMILY
        and failure.get("lane") == "development"
        and failure.get("failure_code") == recovery.INCOMPLETE_INTRADAY
        and failure.get("data_outcomes_accessed") is True
        and failure.get("strategy_metrics_accessed") is False
        and failure.get("confirmation_outcomes_accessed") is False
        and failure.get("completed_tasks") == failure.get("task_count")
        and _exposure_is_indexed(failure)
    ):
        raise DenseIntradayRecoveryError(
            "intraday retained recovery lacks inspected, indexed development authority"
        )
    original_plan_path = PROJECT_ROOT / str(failure["plan_path"])
    original_plan = collection._validate_plan(
        original_plan_path,
        enforce_commit=enforce_commit,
    )
    if original_plan["artifact_sha256"] != failure["plan_sha256"]:
        raise DenseIntradayRecoveryError("intraday source plan binding drifted")
    original_search_path = PROJECT_ROOT / str(original_plan["authority_path"])
    original_search, refreshed_search, semantic_hash = _same_semantic_search(
        original_search_path,
        refreshed_search_path,
        enforce_commit=enforce_commit,
    )
    today = actual_today or date.today()
    current = as_of or today
    if current > today:
        raise DenseIntradayRecoveryError(
            "intraday recovery as_of cannot be future-dated"
        )
    incomplete = _incomplete_sessions(failure)
    missed_dates = sorted({item["date"] for item in incomplete})
    required_dates = list(map(str, original_plan["required_dates"]))
    evaluation_dates = list(map(str, original_plan["evaluation_dates"]))
    if (
        not set(missed_dates).issubset(required_dates)
        or not set(evaluation_dates).issubset(required_dates)
    ):
        raise DenseIntradayRecoveryError(
            "intraday missing-data dates escaped the frozen calendar"
        )
    payload = {
        "schema_version": 1,
        "artifact_kind": PLAN_KIND,
        "campaign_id": failure["campaign_id"],
        "state": "RETAINED_RECOVERY_PLAN_FROZEN",
        "family_id": runtime.INTRADAY_ETF_FAMILY,
        "lane": "development",
        "as_of": current.isoformat(),
        "authority_path": _repo_path(refreshed_search_path),
        "authority_sha256": refreshed_search["artifact_sha256"],
        "binding_sha256": refreshed_search["artifact_sha256"],
        "evaluation_dates": evaluation_dates,
        "required_dates": required_dates,
        "warmup_sessions": original_plan["warmup_sessions"],
        "symbols": list(map(str, original_plan["symbols"])),
        "tasks": [dict(task) for task in original_plan["tasks"]],
        "task_count": original_plan["task_count"],
        "providers": ["retained hash-validated Alpaca SIP checkpoints only"],
        "source_plan_path": _repo_path(original_plan_path),
        "source_plan_sha256": original_plan["artifact_sha256"],
        "source_search_sha256": original_search["artifact_sha256"],
        "recovery_failure_path": _repo_path(failure_path),
        "recovery_failure_sha256": failure["artifact_sha256"],
        "recovery_failure_inspection_path": _repo_path(
            failure_inspection_path
        ),
        "recovery_failure_inspection_sha256": inspection["artifact_sha256"],
        "search_refresh": {
            "refreshed_search_path": _repo_path(refreshed_search_path),
            "refreshed_search_sha256": refreshed_search["artifact_sha256"],
            "semantic_contract_sha256": semantic_hash,
            "only_implementation_hashes_changed": True,
        },
        "missing_data_policy": {
            "name": POLICY_NAME,
            "missed_dates": missed_dates,
            "missed_evaluation_dates": sorted(
                set(missed_dates) & set(evaluation_dates)
            ),
            "incomplete_sessions": incomplete,
            "complete_symbol_sessions_retained_for_future_history_only": True,
            "entries_on_missed_dates": 0,
            "account_return_on_missed_dates": 0.0,
            "interpolated_minutes": 0,
            "substituted_sessions": 0,
        },
        "implementation_hashes": _implementation_hashes(
            enforce_commit=enforce_commit
        ),
        "provider_requests_before_plan_freeze": 0,
        "provider_requests_allowed": 0,
        "market_outcomes_already_exposed": True,
        "strategy_metrics_accessed_before_plan_freeze": False,
        "confirmation_outcomes_accessed": False,
        "substitutions_allowed": False,
        "broker_actions": 0,
    }
    return strategy_discovery._write_artifact(
        payload,
        public_root
        / runtime.INTRADAY_ETF_FAMILY
        / "development-retained-recovery-plan",
        f"{runtime.INTRADAY_ETF_FAMILY}-development-retained-recovery-plan",
    )


def validate_plan(
    path: Path, *, enforce_commit: bool = True
) -> dict[str, Any]:
    if enforce_commit:
        strategy_discovery.require_committed(path)
    plan = strategy_discovery.load_artifact(path, expected_kind=PLAN_KIND)
    policy = plan.get("missing_data_policy")
    if not (
        plan.get("state") == "RETAINED_RECOVERY_PLAN_FROZEN"
        and plan.get("family_id") == runtime.INTRADAY_ETF_FAMILY
        and plan.get("lane") == "development"
        and plan.get("task_count") == len(plan.get("tasks", []))
        and plan.get("task_count", 0) > 0
        and plan.get("provider_requests_before_plan_freeze") == 0
        and plan.get("provider_requests_allowed") == 0
        and plan.get("market_outcomes_already_exposed") is True
        and plan.get("strategy_metrics_accessed_before_plan_freeze") is False
        and plan.get("confirmation_outcomes_accessed") is False
        and plan.get("substitutions_allowed") is False
        and plan.get("broker_actions") == 0
        and isinstance(policy, Mapping)
        and policy.get("name") == POLICY_NAME
        and policy.get("entries_on_missed_dates") == 0
        and policy.get("account_return_on_missed_dates") == 0.0
        and policy.get("interpolated_minutes") == 0
        and policy.get("substituted_sessions") == 0
    ):
        raise DenseIntradayRecoveryError(
            "intraday retained recovery plan authority drifted"
        )
    source_path = PROJECT_ROOT / str(plan["source_plan_path"])
    source = collection._validate_plan(
        source_path, enforce_commit=enforce_commit
    )
    if not (
        source["artifact_sha256"] == plan["source_plan_sha256"]
        and source["tasks"] == plan["tasks"]
        and source["evaluation_dates"] == plan["evaluation_dates"]
        and source["required_dates"] == plan["required_dates"]
        and source["symbols"] == plan["symbols"]
    ):
        raise DenseIntradayRecoveryError(
            "retained checkpoint source topology drifted"
        )
    failure_inspection_path = (
        PROJECT_ROOT / str(plan["recovery_failure_inspection_path"])
    )
    _failure_path, failure, inspection = recovery._load_inspected_failure(
        failure_inspection_path,
        enforce_commit=enforce_commit,
    )
    if not (
        failure["artifact_sha256"] == plan["recovery_failure_sha256"]
        and inspection["artifact_sha256"]
        == plan["recovery_failure_inspection_sha256"]
        and _exposure_is_indexed(failure)
        and _incomplete_sessions(failure) == policy["incomplete_sessions"]
        and sorted(
            {item["date"] for item in policy["incomplete_sessions"]}
        )
        == policy["missed_dates"]
        and sorted(
            set(policy["missed_dates"]) & set(plan["evaluation_dates"])
        )
        == policy["missed_evaluation_dates"]
    ):
        raise DenseIntradayRecoveryError(
            "retained missing-data evidence drifted"
        )
    refreshed_path = PROJECT_ROOT / str(plan["authority_path"])
    _original, refreshed, semantic_hash = _same_semantic_search(
        PROJECT_ROOT / str(source["authority_path"]),
        refreshed_path,
        enforce_commit=enforce_commit,
    )
    if not (
        refreshed["artifact_sha256"] == plan["authority_sha256"]
        == plan["binding_sha256"]
        and plan["search_refresh"]["refreshed_search_sha256"]
        == refreshed["artifact_sha256"]
        and plan["search_refresh"]["semantic_contract_sha256"]
        == semantic_hash
        and plan["search_refresh"]["only_implementation_hashes_changed"]
        is True
    ):
        raise DenseIntradayRecoveryError(
            "retained recovery search binding drifted"
        )
    if plan.get("implementation_hashes") != _implementation_hashes(
        enforce_commit=enforce_commit
    ):
        raise DenseIntradayRecoveryError(
            "intraday recovery implementation drifted"
        )
    return plan


def _source_root(
    config: HistoricalStoreConfig, plan: Mapping[str, Any]
) -> Path:
    return (
        config.root
        / "dense-v2"
        / runtime.INTRADAY_ETF_FAMILY
        / "development"
        / str(plan["source_plan_sha256"])
    )


def build_dataset(
    source_root: Path, plan: Mapping[str, Any]
) -> dict[str, Any]:
    policy = plan["missing_data_policy"]
    expected_incomplete = list(policy["incomplete_sessions"])
    incomplete: list[dict[str, Any]] = []
    minute: dict[str, dict[str, list[dict[str, Any]]]] = {}
    for task in plan["tasks"]:
        rows = collection._load_checkpoint(
            collection._checkpoint_path(source_root, task), task
        )
        day = str(task["date"])
        symbol = str(task["symbol"])
        if not recovery._regular_session_complete(rows, day):
            incomplete.append(
                {
                    "date": day,
                    "symbol": symbol,
                    "observed_minutes": len(rows),
                    "expected_minutes": 390,
                }
            )
            continue
        converted = [
            {
                "timestamp": row["time_et"],
                "open": row["open"],
                "high": row["high"],
                "low": row["low"],
                "close": row["close"],
                "volume": row["volume"],
                "vwap_numerator": float(row["wap"]) * float(row["volume"]),
                "vwap_denominator": row["volume"],
            }
            for row in rows
        ]
        minute.setdefault(day, {})[symbol] = converted
    incomplete.sort(key=lambda item: (item["date"], item["symbol"]))
    if incomplete != expected_incomplete:
        raise DenseIntradayRecoveryError(
            "retained incomplete-session inventory drifted"
        )
    expected_symbols = set(map(str, plan["symbols"]))
    missed_dates = set(map(str, policy["missed_dates"]))
    if set(minute) != set(map(str, plan["required_dates"])):
        raise DenseIntradayRecoveryError(
            "retained intraday dates are incomplete"
        )
    for day, symbols in minute.items():
        observed = set(symbols)
        if day in missed_dates:
            if not observed or not observed < expected_symbols:
                raise DenseIntradayRecoveryError(
                    "retained missed date does not omit a strict universe subset"
                )
        elif observed != expected_symbols:
            raise DenseIntradayRecoveryError(
                "retained complete date lacks the frozen universe"
            )
    return {
        "schema_version": 1,
        "family_id": runtime.INTRADAY_ETF_FAMILY,
        "evaluation_dates": list(plan["evaluation_dates"]),
        "symbols": list(plan["symbols"]),
        "regular_session_minutes_by_date": {
            day: 390 for day in plan["required_dates"]
        },
        "missed_data_dates": list(policy["missed_dates"]),
        "missing_session_evidence": incomplete,
        "minute_bars": minute,
        "source_semantics": {
            "feed": "retained Alpaca SIP raw-adjustment minute bars",
            "missing_data_policy": POLICY_NAME,
            "interpolation": "forbidden",
            "substitution": "forbidden",
        },
    }


def _existing_status(
    public_root: Path, plan: Mapping[str, Any]
) -> tuple[Path, dict[str, Any]] | None:
    directory = (
        public_root
        / runtime.INTRADAY_ETF_FAMILY
        / "development-retained-recovery"
    )
    matches = []
    for path in directory.glob("*.json"):
        try:
            status = strategy_discovery.load_artifact(
                path, expected_kind=STATUS_KIND
            )
        except strategy_discovery.StrategyDiscoveryError:
            continue
        if status.get("plan_sha256") == plan["artifact_sha256"]:
            matches.append((path, status))
    if len(matches) > 1:
        raise DenseIntradayRecoveryError(
            "multiple retained statuses bind one plan"
        )
    return matches[0] if matches else None


def collect(
    plan_path: Path,
    *,
    collected_at: str | None = None,
    store_config: HistoricalStoreConfig | None = None,
    public_root: Path = DEFAULT_PUBLIC_ROOT,
    enforce_commit: bool = True,
) -> tuple[Path, dict[str, Any]]:
    plan = validate_plan(plan_path, enforce_commit=enforce_commit)
    existing = _existing_status(public_root, plan)
    if existing is not None:
        return existing
    observed = (
        datetime.fromisoformat(collected_at.replace("Z", "+00:00"))
        if collected_at is not None
        else datetime.now(timezone.utc)
    )
    if observed.tzinfo is None:
        raise DenseIntradayRecoveryError(
            "collected_at must include a timezone"
        )
    config = store_config or HistoricalStoreConfig.from_env(DEFAULT_ENV_PATH)
    source_root = _source_root(config, plan)
    target_root = (
        config.root
        / "dense-v2"
        / runtime.INTRADAY_ETF_FAMILY
        / "development"
        / str(plan["artifact_sha256"])
    )
    dataset = build_dataset(source_root, plan)
    runtime.prepare_dataset(dataset)
    dataset_path = target_root / "dataset.json.gz"
    collection._write_external(dataset_path, dataset, config)
    relative = str(dataset_path.resolve().relative_to(config.root.resolve()))
    timestamp = observed.astimezone(timezone.utc).isoformat().replace(
        "+00:00", "Z"
    )
    payload = {
        "schema_version": 1,
        "artifact_kind": STATUS_KIND,
        "campaign_id": plan["campaign_id"],
        "state": "RETAINED_DATASET_COLLECTED_UNINSPECTED",
        "family_id": plan["family_id"],
        "lane": plan["lane"],
        "plan_path": _repo_path(plan_path),
        "plan_sha256": plan["artifact_sha256"],
        "binding_sha256": plan["binding_sha256"],
        "evaluation_dates": plan["evaluation_dates"],
        "task_count": plan["task_count"],
        "completed_tasks": plan["task_count"],
        "missed_data_dates": plan["missing_data_policy"]["missed_dates"],
        "missed_evaluation_dates": plan["missing_data_policy"][
            "missed_evaluation_dates"
        ],
        "external_relative_path": relative,
        "external_file_sha256": sha256_file(dataset_path),
        "dataset_sha256": canonical_sha256(dataset),
        "provider_telemetry": {
            "requests": 0,
            "request_seconds": 0.0,
            "pacing_wait_seconds": 0.0,
            "cache_hits": plan["task_count"],
            "failures": 0,
        },
        "substitutions": 0,
        "interpolated_minutes": 0,
        "broker_actions": 0,
        "collected_at": timestamp,
    }
    return strategy_discovery._write_artifact(
        payload,
        public_root
        / runtime.INTRADAY_ETF_FAMILY
        / "development-retained-recovery",
        f"{runtime.INTRADAY_ETF_FAMILY}-development-retained-recovery",
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--public-root", type=Path, default=DEFAULT_PUBLIC_ROOT)
    subparsers = parser.add_subparsers(dest="command", required=True)
    freeze = subparsers.add_parser("freeze")
    freeze.add_argument("failure_inspection", type=Path)
    freeze.add_argument("refreshed_search", type=Path)
    freeze.add_argument("--as-of", type=date.fromisoformat)
    collect_parser = subparsers.add_parser("collect")
    collect_parser.add_argument("plan", type=Path)
    collect_parser.add_argument("--collected-at")
    return parser


def main() -> int:
    args = _parser().parse_args()
    try:
        if args.command == "freeze":
            path, artifact = freeze_plan(
                args.failure_inspection,
                args.refreshed_search,
                as_of=args.as_of,
                public_root=args.public_root,
            )
        else:
            path, artifact = collect(
                args.plan,
                collected_at=args.collected_at,
                public_root=args.public_root,
            )
        print(
            json.dumps(
                {
                    "written": _repo_path(path),
                    "artifact_sha256": artifact["artifact_sha256"],
                    "state": artifact["state"],
                    "provider_telemetry": artifact.get(
                        "provider_telemetry", {}
                    ),
                    "broker_actions": 0,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    except (
        DenseIntradayRecoveryError,
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

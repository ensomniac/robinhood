"""Independently rebuild a frozen dense market-data collection plan."""

from __future__ import annotations

import argparse
import json
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import dense_data_collection
import dense_strategy_runtime as runtime
import outcome_exposure
import strategy_discovery
from historical_store import sha256_file


PROJECT_ROOT = Path(__file__).resolve().parent
ARTIFACT_KIND = "dense-data-collection-plan-inspection"
READY_STATE = "COLLECTION_PLAN_INSPECTED_READY"
DEFAULT_ROOT = PROJECT_ROOT / "strategy_tournament/v2/discovery"


class DenseCollectionPlanInspectionError(ValueError):
    """The committed request graph cannot be independently reproduced."""


def _repo_path(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(PROJECT_ROOT.resolve()))
    except ValueError as exc:
        raise DenseCollectionPlanInspectionError(
            "inspection path is outside the repository"
        ) from exc


def _authority(
    plan: Mapping[str, Any], *, enforce_commit: bool
) -> tuple[Path, dict[str, Any], dict[str, Any]]:
    authority_path = PROJECT_ROOT / str(plan["authority_path"])
    if enforce_commit:
        strategy_discovery.require_committed(authority_path)
    authority = strategy_discovery.load_artifact(
        authority_path, expected_kind="frozen-development-search"
    )
    contract = authority.get("family_contract")
    if not (
        isinstance(contract, Mapping)
        and authority.get("artifact_sha256") == plan.get("authority_sha256")
        and contract.get("family_id") == plan.get("family_id")
        and contract.get("research_generation")
        == plan.get("research_generation")
    ):
        raise DenseCollectionPlanInspectionError(
            "plan search or family binding drifted"
        )
    return authority_path, authority, dict(contract)


def _expected_tasks(
    plan: Mapping[str, Any], contract: Mapping[str, Any]
) -> list[dict[str, Any]]:
    required_dates = list(plan["required_dates"])
    symbols = sorted(map(str, contract["universe"]["symbols"]))
    historical = contract.get("historical_data_contract")
    if not (
        isinstance(historical, Mapping)
        and historical.get("daily_provider") == "massive"
        and historical.get("daily_adjusted") is False
        and historical.get("daily_request_mode") == "symbol_range"
        and historical.get("split_provider") == "massive"
        and historical.get("provider_substitutions_allowed") is False
    ):
        raise DenseCollectionPlanInspectionError(
            "plan is not the frozen Massive symbol-range contract"
        )
    split_task = dense_data_collection._task(
        "split_actions", required_dates[-1]
    )
    split_task["start"] = required_dates[0]
    split_task["task_id"] = dense_data_collection.canonical_sha256(
        {
            key: value
            for key, value in split_task.items()
            if key != "task_id"
        }
    )
    tasks = [split_task]
    recovery = (
        plan.get("adjustment_semantics")
        == dense_data_collection.RECOVERY_ADJUSTMENT
    )
    for symbol in symbols:
        task = {
            "kind": (
                "daily_symbol_bars"
                if recovery
                else "massive_daily_symbol_bars"
            ),
            "date": required_dates[-1],
            "start": required_dates[0],
            "symbol": symbol,
        }
        task["task_id"] = dense_data_collection.canonical_sha256(task)
        tasks.append(task)
    return tasks


def inspect_plan(
    plan_path: Path,
    *,
    root: Path = DEFAULT_ROOT,
    enforce_commit: bool = True,
) -> tuple[Path, dict[str, Any]]:
    if enforce_commit:
        strategy_discovery.require_committed(Path(__file__).resolve())
    plan = dense_data_collection._validate_plan(
        plan_path, enforce_commit=enforce_commit
    )
    if plan.get("family_id") != (
        runtime.CROSS_STYLE_BREADTH_CONTINUATION_FAMILY
    ):
        raise DenseCollectionPlanInspectionError(
            "unsupported plan family for this independent inspector"
        )
    authority_path, authority, contract = _authority(
        plan, enforce_commit=enforce_commit
    )
    calendar_path = PROJECT_ROOT / str(plan["calendar_path"])
    if enforce_commit:
        strategy_discovery.require_committed(calendar_path)
    calendar = dense_data_collection._calendar(calendar_path)
    lane = str(plan["lane"])
    evaluation_dates = list(contract[f"{lane}_dates"])
    warmup_dates = list(contract[f"{lane}_warmup_dates"])
    required_dates = dense_data_collection._required_dates(
        evaluation_dates,
        calendar,
        dense_data_collection.DAILY_WARMUP_SESSIONS,
    )
    expected_tasks = _expected_tasks(plan, contract)
    recovery = (
        plan.get("adjustment_semantics")
        == dense_data_collection.RECOVERY_ADJUSTMENT
    )
    records = outcome_exposure.read_index()
    outcome_exposure.assert_untouched(
        contract["development_scope"], records
    )
    outcome_exposure.assert_untouched(
        contract["confirmation_scope"], records
    )
    checks = {
        "plan_hash_valid": (
            strategy_discovery.load_artifact(
                plan_path,
                expected_kind=dense_data_collection.PLAN_KIND,
            )["artifact_sha256"]
            == plan["artifact_sha256"]
        ),
        "committed_search_bound": (
            plan["authority_path"] == _repo_path(authority_path)
            and plan["authority_sha256"] == authority["artifact_sha256"]
        ),
        "calendar_hash_rebuilt": (
            plan["calendar_sha256"] == sha256_file(calendar_path)
        ),
        "evaluation_partition_rebuilt": (
            plan["evaluation_dates"] == evaluation_dates
        ),
        "warmup_rebuilt": (
            len(warmup_dates)
            == dense_data_collection.DAILY_WARMUP_SESSIONS
            and required_dates == [*warmup_dates, *evaluation_dates]
            and plan["required_dates"] == required_dates
        ),
        "symbols_rebuilt": (
            plan["symbols"]
            == sorted(map(str, contract["universe"]["symbols"]))
            == list(runtime.CROSS_STYLE_BREADTH_SYMBOLS)
        ),
        "request_graph_rebuilt": (
            plan["tasks"] == expected_tasks
            and plan["task_count"] == len(expected_tasks) == 9
        ),
        "provider_semantics_rebuilt": (
            plan["daily_provider"] == "massive"
            and plan["daily_request_mode"] == "symbol_range"
            and plan["providers"]
            == (
                [
                    (
                        "Alpaca SIP raw-adjustment daily bars by frozen "
                        "symbol range"
                    ),
                    (
                        "Massive point-in-time split actions through the "
                        "final frozen session"
                    ),
                ]
                if recovery
                else [
                    (
                        "Massive SIP unadjusted daily bars by frozen "
                        "symbol range"
                    ),
                    (
                        "Massive point-in-time split actions through the "
                        "final frozen session"
                    ),
                ]
            )
            and plan["substitutions_allowed"] is False
        ),
        "recovery_lineage_rebuilt": (
            (
                isinstance(plan.get("recovery_failure_path"), str)
                and isinstance(
                    plan.get("recovery_failure_inspection_path"), str
                )
                and plan.get("supersedes_plan_sha256")
                == "7f762395047610635bd897ca0b7a908ccf42ec603edee879cb73264d51773f43"
            )
            if recovery
            else plan.get("recovery_failure_path") is None
        ),
        "zero_access_boundary_rebuilt": (
            plan["provider_requests_before_plan_freeze"] == 0
            and plan["market_outcomes_accessed"] is False
            and plan["broker_actions"] == 0
        ),
        "development_scope_untouched": True,
        "confirmation_scope_untouched": True,
    }
    if not all(checks.values()):
        failed = sorted(key for key, value in checks.items() if not value)
        raise DenseCollectionPlanInspectionError(
            f"collection plan inspection failed: {failed}"
        )
    payload = {
        "schema_version": 1,
        "artifact_kind": ARTIFACT_KIND,
        "campaign_id": plan["campaign_id"],
        "state": READY_STATE,
        "family_id": plan["family_id"],
        "lane": lane,
        "plan_path": _repo_path(plan_path),
        "plan_file_sha256": sha256_file(plan_path),
        "plan_sha256": plan["artifact_sha256"],
        "authority_path": _repo_path(authority_path),
        "authority_sha256": authority["artifact_sha256"],
        "calendar_path": _repo_path(calendar_path),
        "calendar_sha256": sha256_file(calendar_path),
        "task_count": len(expected_tasks),
        "provider_requests": 0,
        "market_outcomes_accessed": False,
        "confirmation_outcomes_accessed": False,
        "broker_actions": 0,
        "outcome_exposure_index_sha256": outcome_exposure.audit()[
            "index_sha256"
        ],
        "inspector_path": _repo_path(Path(__file__).resolve()),
        "inspector_sha256": sha256_file(Path(__file__).resolve()),
        "checks": checks,
        "valid": True,
    }
    return strategy_discovery._write_artifact(
        payload,
        root
        / str(plan["family_id"])
        / f"{lane}-collection-plan-inspection",
        f"{plan['family_id']}-{lane}-collection-plan-inspection",
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("inspect",))
    parser.add_argument("plan", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        path, artifact = inspect_plan(args.plan)
    except (
        DenseCollectionPlanInspectionError,
        OSError,
        ValueError,
        outcome_exposure.OutcomeExposureError,
        strategy_discovery.StrategyDiscoveryError,
    ) as exc:
        print(
            json.dumps(
                {"error": str(exc), "error_type": type(exc).__name__},
                indent=2,
                sort_keys=True,
            )
        )
        return 1
    print(
        json.dumps(
            {
                "written": _repo_path(path),
                "state": artifact["state"],
                "plan_sha256": artifact["plan_sha256"],
                "task_count": artifact["task_count"],
                "provider_requests": artifact["provider_requests"],
                "market_outcomes_accessed": artifact[
                    "market_outcomes_accessed"
                ],
                "broker_actions": artifact["broker_actions"],
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

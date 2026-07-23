"""Freeze and inspect causal warmup plus disjoint target-date allocation."""

from __future__ import annotations

import argparse
import json
from collections.abc import Sequence
from datetime import datetime
from pathlib import Path
from typing import Any

import dense_capacity_inventory
import dense_session_calendar
import next_week_discovery_batch as batch
import outcome_exposure
import strategy_discovery


PROJECT_ROOT = Path(__file__).resolve().parent
SCHEMA_VERSION = 1
CONTRACT_KIND = "dense-calendar-allocation-contract"
INSPECTION_KIND = "dense-calendar-allocation-inspection"
DEFAULT_ROOT = PROJECT_ROOT / "strategy_tournament/v2/calendar/allocation"


class DenseCalendarAllocationError(RuntimeError):
    """The allocation contract, calendar, or evidence topology differs."""


def _repo_path(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(PROJECT_ROOT.resolve()))
    except ValueError as exc:
        raise DenseCalendarAllocationError(
            f"path escaped the repository: {path}"
        ) from exc


def _timestamp(value: str, field: str) -> str:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise DenseCalendarAllocationError(f"{field} is invalid") from exc
    if parsed.tzinfo is None:
        raise DenseCalendarAllocationError(f"{field} must be timezone-aware")
    return value


def _implementation_hashes() -> dict[str, str]:
    paths = (Path(__file__).resolve(), Path(dense_capacity_inventory.__file__).resolve())
    return {
        _repo_path(path): strategy_discovery._file_hash(path)
        for path in paths
    }


def _collection(path: Path, *, enforce_commit: bool) -> dict[str, Any]:
    if enforce_commit:
        strategy_discovery.require_committed(path)
    status = strategy_discovery.load_artifact(
        path,
        expected_kind=dense_session_calendar.COLLECTION_KIND,
    )
    if not (
        status.get("state") == "CALENDAR_COLLECTED_UNINSPECTED"
        and status.get("provider_requests") == 1
        and status.get("market_prices_accessed") is False
        and status.get("target_outcomes_accessed") is False
        and status.get("broker_actions") == 0
    ):
        raise DenseCalendarAllocationError("calendar collection is not safe")
    return status


def freeze_contract(
    collection_path: Path,
    *,
    frozen_at: str,
    root: Path = DEFAULT_ROOT,
    index_path: Path = outcome_exposure.DEFAULT_INDEX,
    enforce_commit: bool = True,
) -> tuple[Path, dict[str, Any]]:
    _timestamp(frozen_at, "frozen_at")
    status = _collection(collection_path, enforce_commit=enforce_commit)
    if enforce_commit:
        for path in (Path(__file__), Path(dense_capacity_inventory.__file__)):
            strategy_discovery.require_committed(path)
    calendar_path = PROJECT_ROOT / str(status["calendar_path"])
    if not calendar_path.is_file():
        raise DenseCalendarAllocationError("collected calendar rows are missing")
    calendar_sha256 = strategy_discovery._file_hash(calendar_path)
    if calendar_sha256 != status["calendar_sha256"]:
        raise DenseCalendarAllocationError("collected calendar hash drifted")
    plan = batch.build_plan()
    exposure = outcome_exposure.audit(index_path)
    payload: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "artifact_kind": CONTRACT_KIND,
        "campaign_id": batch.CAMPAIGN_ID,
        "state": "CALENDAR_ALLOCATION_CONTRACT_FROZEN",
        "frozen_at": frozen_at,
        "collection_path": _repo_path(collection_path),
        "collection_sha256": status["artifact_sha256"],
        "calendar_path": status["calendar_path"],
        "calendar_sha256": calendar_sha256,
        "plan_sha256": plan["plan_sha256"],
        "rolling_authorization_sha256": plan[
            "rolling_authorization_sha256"
        ],
        "outcome_exposure_index_sha256": exposure["index_sha256"],
        "family_order": [item["family_id"] for item in plan["families"]],
        "allocation_semantics": {
            "target_sessions_per_family": (
                dense_capacity_inventory.SESSIONS_PER_FAMILY
            ),
            "target_blocks_mutually_disjoint": True,
            "target_dates_globally_untouched": True,
            "target_blocks_from_one_latest_contiguous_untouched_run": True,
            "warmup_immediately_precedes_each_target_block": True,
            "warmup_point_in_time_features_only": True,
            "warmup_target_outcomes_eligible": False,
            "warmup_prior_exposure_allowed": True,
            "warmup_cross_family_overlap_allowed": True,
            "date_substitution_after_inspection_allowed": False,
        },
        "predecessor_failure": {
            "state": "INSUFFICIENT_SINGLE_RUN_FOR_DISJOINT_WARMUPS",
            "required_sessions": 940,
            "largest_untouched_run": 567,
            "target_outcomes_accessed": False,
            "strategy_metrics_accessed": False,
            "repair_scope": (
                "Allocation topology only; no family rule, parameter, cost, "
                "target date, or selection gate changed."
            ),
        },
        "implementation_hashes": _implementation_hashes(),
        "provider_requests_added": 0,
        "market_prices_accessed": False,
        "target_outcomes_accessed": False,
        "broker_actions": 0,
    }
    return strategy_discovery._write_artifact(
        payload,
        root / "contract",
        "dense-calendar-allocation-contract",
    )


def _load_contract(
    path: Path,
    *,
    enforce_commit: bool,
    index_path: Path,
) -> dict[str, Any]:
    if enforce_commit:
        strategy_discovery.require_committed(path)
        for implementation in (
            Path(__file__),
            Path(dense_capacity_inventory.__file__),
        ):
            strategy_discovery.require_committed(implementation)
    contract = strategy_discovery.load_artifact(path, expected_kind=CONTRACT_KIND)
    plan = batch.build_plan()
    if not (
        contract.get("state") == "CALENDAR_ALLOCATION_CONTRACT_FROZEN"
        and contract.get("implementation_hashes") == _implementation_hashes()
        and contract.get("plan_sha256") == plan["plan_sha256"]
        and contract.get("rolling_authorization_sha256")
        == plan["rolling_authorization_sha256"]
        and contract.get("outcome_exposure_index_sha256")
        == outcome_exposure.audit(index_path)["index_sha256"]
        and contract.get("provider_requests_added") == 0
        and contract.get("market_prices_accessed") is False
        and contract.get("target_outcomes_accessed") is False
        and contract.get("broker_actions") == 0
    ):
        raise DenseCalendarAllocationError("allocation contract drifted")
    return contract


def inspect(
    contract_path: Path,
    *,
    inspected_at: str,
    root: Path = DEFAULT_ROOT,
    index_path: Path = outcome_exposure.DEFAULT_INDEX,
    enforce_commit: bool = True,
) -> tuple[Path, dict[str, Any]]:
    _timestamp(inspected_at, "inspected_at")
    contract = _load_contract(
        contract_path,
        enforce_commit=enforce_commit,
        index_path=index_path,
    )
    collection_path = PROJECT_ROOT / str(contract["collection_path"])
    status = _collection(collection_path, enforce_commit=enforce_commit)
    calendar_path = PROJECT_ROOT / str(contract["calendar_path"])
    if strategy_discovery._file_hash(calendar_path) != contract["calendar_sha256"]:
        raise DenseCalendarAllocationError("calendar bytes drifted")
    calendar = dense_capacity_inventory._calendar(calendar_path)
    records = outcome_exposure.read_index(index_path)
    exposed = dense_capacity_inventory._globally_exposed_dates(records)
    allocations = dense_capacity_inventory._allocate(calendar, exposed)
    plan = batch.build_plan()
    if len(allocations) != len(plan["families"]) or len(allocations) != 3:
        raise DenseCalendarAllocationError("allocation family count differs")
    target_dates: list[str] = []
    warmup_dates: list[str] = []
    family_counts: dict[str, dict[str, int]] = {}
    for family, allocation in zip(plan["families"], allocations, strict=True):
        family_id = str(family["family_id"])
        warmup = allocation["warmup"]
        evidence = allocation["evidence"]
        expected_warmup = dense_capacity_inventory.FAMILY_WARMUP_SESSIONS[
            family_id
        ]
        if not (
            len(warmup) == expected_warmup
            and len(evidence) == dense_capacity_inventory.SESSIONS_PER_FAMILY
            and max(warmup) < min(evidence)
            and not set(evidence).intersection(exposed)
        ):
            raise DenseCalendarAllocationError(
                f"causal allocation differs for {family_id}"
            )
        warmup_dates.extend(warmup)
        target_dates.extend(evidence)
        family_counts[family_id] = {
            "warmup_sessions": len(warmup),
            "target_sessions": len(evidence),
        }
    if len(target_dates) != len(set(target_dates)):
        raise DenseCalendarAllocationError(
            "family target evidence dates overlap"
        )
    payload: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "artifact_kind": INSPECTION_KIND,
        "campaign_id": batch.CAMPAIGN_ID,
        "state": "CALENDAR_ALLOCATION_INSPECTED_READY",
        "contract_path": _repo_path(contract_path),
        "contract_sha256": contract["artifact_sha256"],
        "collection_path": _repo_path(collection_path),
        "collection_sha256": status["artifact_sha256"],
        "calendar_path": contract["calendar_path"],
        "calendar_sha256": contract["calendar_sha256"],
        "plan_sha256": contract["plan_sha256"],
        "rolling_authorization_sha256": contract[
            "rolling_authorization_sha256"
        ],
        "outcome_exposure_index_sha256": contract[
            "outcome_exposure_index_sha256"
        ],
        "family_counts": family_counts,
        "untouched_target_sessions": len(target_dates),
        "warmup_observations": len(warmup_dates),
        "unique_warmup_sessions": len(set(warmup_dates)),
        "checks": {
            "calendar_hash_rebuilt": True,
            "three_target_blocks_rebuilt": True,
            "target_blocks_mutually_disjoint": True,
            "target_dates_globally_untouched": True,
            "warmups_complete_and_causal": True,
            "warmups_excluded_from_target_evidence": True,
            "implementation_hashes_rebuilt": True,
            "target_outcomes_absent": True,
        },
        "provider_requests_added": 0,
        "market_prices_accessed": False,
        "target_outcomes_accessed": False,
        "broker_actions": 0,
        "inspected_at": inspected_at,
    }
    return strategy_discovery._write_artifact(
        payload,
        root / "inspection",
        "dense-calendar-allocation-inspection",
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    freeze = subparsers.add_parser("freeze")
    freeze.add_argument("collection", type=Path)
    freeze.add_argument("--frozen-at", required=True)
    inspect_parser = subparsers.add_parser("inspect")
    inspect_parser.add_argument("contract", type=Path)
    inspect_parser.add_argument("--inspected-at", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "freeze":
            path, artifact = freeze_contract(
                args.collection,
                frozen_at=args.frozen_at,
            )
        else:
            path, artifact = inspect(
                args.contract,
                inspected_at=args.inspected_at,
            )
        result: dict[str, Any] = {
            "written": _repo_path(path),
            "artifact_sha256": artifact["artifact_sha256"],
            "state": artifact["state"],
            "target_outcomes_accessed": False,
            "broker_actions": 0,
        }
    except (
        DenseCalendarAllocationError,
        dense_capacity_inventory.DenseCapacityInventoryError,
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
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""Report the exact outcome-blind handoff for the authorized dense batch."""

from __future__ import annotations

import argparse
import json
import subprocess
from datetime import date
from pathlib import Path
from typing import Any

import dense_calendar_allocation
import dense_capacity_inventory
import dense_data_collection
import dense_family_contracts
import dense_session_calendar
import dense_session_calendar_inspection
import dense_strategy_plugin
import dense_strategy_runtime
import next_week_discovery_batch as batch
import outcome_exposure
import portfolio_execution
import strategy_discovery


PROJECT_ROOT = Path(__file__).resolve().parent
IMPLEMENTATION_PATHS = (
    Path(__file__).resolve(),
    Path(batch.__file__).resolve(),
    Path(dense_calendar_allocation.__file__).resolve(),
    Path(dense_session_calendar.__file__).resolve(),
    Path(dense_session_calendar_inspection.__file__).resolve(),
    Path(dense_capacity_inventory.__file__).resolve(),
    Path(dense_family_contracts.__file__).resolve(),
    Path(dense_data_collection.__file__).resolve(),
    Path(dense_strategy_plugin.__file__).resolve(),
    Path(dense_strategy_runtime.__file__).resolve(),
    Path(portfolio_execution.__file__).resolve(),
    Path(strategy_discovery.__file__).resolve(),
)


class DenseBatchReadinessError(RuntimeError):
    """The preactivation implementation or outcome-blind boundary is incomplete."""


def _repo_path(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(PROJECT_ROOT.resolve()))
    except ValueError as exc:
        raise DenseBatchReadinessError(
            f"path escaped the repository: {path}"
        ) from exc


def _committed_clean(path: Path) -> bool:
    relative = _repo_path(path)
    tracked = subprocess.run(
        ["git", "ls-files", "--error-unmatch", "--", relative],
        cwd=PROJECT_ROOT,
        capture_output=True,
    )
    clean = subprocess.run(
        ["git", "diff", "--quiet", "HEAD", "--", relative],
        cwd=PROJECT_ROOT,
    )
    return tracked.returncode == 0 and clean.returncode == 0


def _plan_path(plan_sha256: str) -> Path:
    matches: list[Path] = []
    for path in sorted(batch.DEFAULT_ROOT.glob("*.json")):
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if isinstance(value, dict) and value.get("plan_sha256") == plan_sha256:
            matches.append(path)
    if len(matches) != 1:
        raise DenseBatchReadinessError(
            "expected one exact committed dense-batch plan"
        )
    return matches[0]


def _calendar_boundary(
    *, require_committed: bool
) -> tuple[Path, dict[str, Any], Path, dict[str, Any]]:
    current_hashes = dense_session_calendar._implementation_hashes()
    contracts: list[tuple[Path, dict[str, Any]]] = []
    for path in sorted(
        (dense_session_calendar.DEFAULT_ROOT / "contract").glob("*.json")
    ):
        try:
            contract = strategy_discovery.load_artifact(
                path,
                expected_kind=dense_session_calendar.CONTRACT_KIND,
            )
        except strategy_discovery.StrategyDiscoveryError:
            continue
        if (
            contract.get("state") == "CALENDAR_CONTRACT_FROZEN"
            and contract.get("research_batch_id") == batch.TARGET_BATCH_ID
            and contract.get("activation_policy")
            == plan_activation_policy()
            and contract.get("rolling_authorization_sha256")
            == plan_authorization_sha256()
            and contract.get("implementation_hashes") == current_hashes
            and contract.get("query")
            == {
                "start": dense_session_calendar.CALENDAR_START,
                "end": dense_session_calendar.CALENDAR_END,
            }
            and contract.get("provider_requests") == 0
            and contract.get("market_prices_accessed") is False
            and contract.get("target_outcomes_accessed") is False
            and contract.get("broker_actions") == 0
        ):
            contracts.append((path, contract))
    if len(contracts) != 1:
        raise DenseBatchReadinessError(
            "expected one current dense-session calendar contract"
        )
    contract_path, contract = contracts[0]
    inspections: list[tuple[Path, dict[str, Any]]] = []
    for path in sorted(
        (
            dense_session_calendar.DEFAULT_ROOT / "contract-inspection"
        ).glob("*.json")
    ):
        try:
            inspection = strategy_discovery.load_artifact(
                path,
                expected_kind=dense_session_calendar.CONTRACT_INSPECTION_KIND,
            )
        except strategy_discovery.StrategyDiscoveryError:
            continue
        if (
            inspection.get("contract_sha256") == contract["artifact_sha256"]
            and inspection.get("state")
            == "CALENDAR_CONTRACT_INSPECTED_READY"
            and all(inspection.get("checks", {}).values())
            and inspection.get("provider_requests") == 0
            and inspection.get("market_prices_accessed") is False
            and inspection.get("target_outcomes_accessed") is False
            and inspection.get("broker_actions") == 0
        ):
            inspections.append((path, inspection))
    if len(inspections) != 1:
        raise DenseBatchReadinessError(
            "expected one exact inspection for the current calendar contract"
        )
    inspection_path, inspection = inspections[0]
    if require_committed:
        for path in (contract_path, inspection_path):
            strategy_discovery.require_committed(path)
    return contract_path, contract, inspection_path, inspection


def _calendar_collection(
    contract: dict[str, Any],
) -> tuple[Path, dict[str, Any]] | None:
    matches: list[tuple[Path, dict[str, Any]]] = []
    for path in sorted(
        (dense_session_calendar.DEFAULT_ROOT / "collection").glob("*.json")
    ):
        try:
            status = strategy_discovery.load_artifact(
                path,
                expected_kind=dense_session_calendar.COLLECTION_KIND,
            )
        except strategy_discovery.StrategyDiscoveryError:
            continue
        if (
            status.get("contract_sha256") == contract["artifact_sha256"]
            and status.get("state") == "CALENDAR_COLLECTED_UNINSPECTED"
            and status.get("provider_requests") == 1
            and status.get("market_prices_accessed") is False
            and status.get("target_outcomes_accessed") is False
            and status.get("broker_actions") == 0
        ):
            matches.append((path, status))
    if len(matches) > 1:
        raise DenseBatchReadinessError(
            "multiple collections bind the current calendar contract"
        )
    return matches[0] if matches else None


def plan_activation_policy() -> str:
    return str(batch.build_plan()["activation_policy"])


def plan_authorization_sha256() -> str:
    return str(batch.build_plan()["rolling_authorization_sha256"])


def _allocation_capacity(
    calendar_path: Path,
    *,
    index_path: Path = outcome_exposure.DEFAULT_INDEX,
) -> dict[str, Any]:
    calendar = dense_capacity_inventory._calendar(calendar_path)
    records = outcome_exposure.read_index(index_path)
    exposed_dates = dense_capacity_inventory._globally_exposed_dates(
        records
    )
    runs = dense_capacity_inventory._untouched_runs(
        calendar,
        exposed_dates,
    )
    required = (
        dense_capacity_inventory.SESSIONS_PER_FAMILY
        * len(batch.build_plan()["families"])
    )
    try:
        allocations = dense_capacity_inventory._allocate(
            calendar,
            exposed_dates,
        )
    except dense_capacity_inventory.DenseCapacityInventoryError as exc:
        return {
            "state": "INSUFFICIENT_GLOBAL_UNTOUCHED_CAPACITY",
            "ready": False,
            "required_contiguous_target_sessions": required,
            "largest_contiguous_untouched_run": max(
                (len(run) for run in runs),
                default=0,
            ),
            "total_untouched_sessions": sum(
                len(run) for run in runs
            ),
            "blocker": str(exc),
        }
    return {
        "state": "ALLOCATION_CAPACITY_READY",
        "ready": len(allocations) == len(batch.build_plan()["families"]),
        "required_contiguous_target_sessions": required,
        "largest_contiguous_untouched_run": max(
            (len(run) for run in runs),
            default=0,
        ),
        "total_untouched_sessions": sum(len(run) for run in runs),
        "blocker": None,
    }


def build_status(
    *,
    as_of: date | None = None,
    require_committed: bool = True,
    require_credentials: bool = True,
) -> dict[str, Any]:
    current = as_of or date.today()
    plan = batch.build_plan()
    plan_path = _plan_path(plan["plan_sha256"])
    contract_path, contract, inspection_path, inspection = _calendar_boundary(
        require_committed=require_committed
    )
    collection = _calendar_collection(contract)
    if collection is not None and require_committed:
        strategy_discovery.require_committed(collection[0])
    implementation_hashes = {
        _repo_path(path): strategy_discovery._file_hash(path)
        for path in IMPLEMENTATION_PATHS
    }
    uncommitted = (
        [
            relative
            for path, relative in (
                (path, _repo_path(path)) for path in IMPLEMENTATION_PATHS
            )
            if not _committed_clean(path)
        ]
        if require_committed
        else []
    )
    if require_committed:
        for path in (plan_path, batch.DEFAULT_STATUS):
            strategy_discovery.require_committed(path)
    credentials_ready = (
        dense_session_calendar.AlpacaConfig.optional_from_env(
            dense_session_calendar.DEFAULT_ENV_PATH
        )
        is not None
    )
    blockers: list[str] = []
    if uncommitted:
        blockers.append(
            "dense implementation is not committed and clean: "
            + ", ".join(uncommitted)
        )
    if require_credentials and not credentials_ready:
        blockers.append("Alpaca calendar credentials are unavailable")
    calendar_path = PROJECT_ROOT / str(contract["calendar_path"])
    source_path = PROJECT_ROOT / str(contract["source_path"])
    allocation_capacity = (
        _allocation_capacity(calendar_path)
        if collection is not None and calendar_path.is_file()
        else {
            "state": "AWAITING_CALENDAR_COLLECTION",
            "ready": False,
            "blocker": None,
        }
    )
    if (
        collection is not None
        and allocation_capacity["ready"] is not True
    ):
        blockers.append(str(allocation_capacity["blocker"]))
    output_state = (
        "COLLECTED_READY_FOR_ALLOCATION_CONTRACT"
        if collection is not None
        else (
            "ABSENT_READY_FOR_SINGLE_COLLECTION"
            if not calendar_path.exists() and not source_path.exists()
            else "UNBOUND_COLLECTION_OUTPUT_PRESENT"
        )
    )
    activation = batch.activation_status(today=current)
    activation_permitted = activation.get("activation_permitted") is True
    if not activation_permitted:
        blockers.extend(str(item) for item in activation.get("blockers", []))
    if blockers:
        state = "PREACTIVATION_BLOCKED"
    elif collection is not None:
        state = "ALLOCATION_CONTRACT_READY"
    elif activation_permitted:
        state = "ACTIVATION_READY"
    else:
        state = "PREACTIVATION_READY"
    if collection is None:
        first_commands = [
            (
                "python3 dense_session_calendar.py collect "
                f"{_repo_path(contract_path)} --as-of "
                f"{batch.ACTIVATION_NOT_BEFORE.isoformat()} --collected-at "
                "<actual-current-ISO8601-timestamp>"
            )
        ]
    elif allocation_capacity["ready"] is not True:
        first_commands = [
            "python3 oversold_replication_discovery.py status"
        ]
    else:
        first_commands = [
            (
                "python3 dense_calendar_allocation.py freeze "
                f"{_repo_path(collection[0])} --frozen-at "
                "<actual-current-ISO8601-timestamp>"
            ),
            (
                "python3 dense_calendar_allocation.py inspect "
                "<committed-allocation-contract> --inspected-at "
                "<actual-current-ISO8601-timestamp>"
            ),
        ]
    return {
        "schema_version": 1,
        "campaign_id": batch.CAMPAIGN_ID,
        "state": state,
        "as_of": current.isoformat(),
        "activation_not_before": batch.ACTIVATION_NOT_BEFORE.isoformat(),
        "activation_permitted": activation_permitted,
        "preactivation_work_complete": not blockers,
        "blockers": blockers,
        "plan": {
            "path": _repo_path(plan_path),
            "sha256": plan["plan_sha256"],
            "families": [
                {
                    "family_id": family["family_id"],
                    "trial_count": family["trial_count"],
                }
                for family in plan["families"]
            ],
        },
        "calendar_boundary": {
            "contract_path": _repo_path(contract_path),
            "contract_sha256": contract["artifact_sha256"],
            "inspection_path": _repo_path(inspection_path),
            "inspection_sha256": inspection["artifact_sha256"],
            "output_state": output_state,
            "provider_requests_so_far": (
                int(collection[1]["provider_requests"])
                if collection is not None
                else 0
            ),
            "market_prices_accessed": False,
            "target_outcomes_accessed": False,
            "collection_path": (
                _repo_path(collection[0]) if collection is not None else None
            ),
            "collection_sha256": (
                collection[1]["artifact_sha256"]
                if collection is not None
                else None
            ),
        },
        "allocation_capacity": allocation_capacity,
        "implementation_hashes": implementation_hashes,
        "credentials_ready": credentials_ready,
        "outcome_exposure_index_sha256": outcome_exposure.audit()[
            "index_sha256"
        ],
        "permitted_now": [
            "local implementation and synthetic-path tests",
            "input and hash-boundary reconstruction",
            "performance and production-evaluator verification",
            "repository and sensitive-data audits",
        ],
        "remaining_transition_order": (
            [
                "collect the outcome-blind session calendar once",
                "freeze and inspect its causal allocation contract",
                "freeze disjoint family evidence and exact family contracts",
                "collect development inputs only from committed exact contracts",
            ]
            if collection is None
            else [
                "freeze and inspect the causal allocation contract",
                "freeze disjoint family evidence and exact family contracts",
                "collect development inputs only from committed exact contracts",
            ]
            if allocation_capacity["ready"] is True
            else [
                "preserve the insufficient-capacity disposition without target outcomes",
                "continue already-authorized existing-family replication",
            ]
        ),
        "next_commands": (
            first_commands
            if (
                collection is not None
                and allocation_capacity["ready"] is not True
            )
            else [
                *first_commands,
                (
                    "python3 dense_capacity_inventory.py --as-of "
                    f"{batch.ACTIVATION_NOT_BEFORE.isoformat()} --created-at "
                    "<actual-current-ISO8601-timestamp>"
                ),
                (
                    "python3 dense_family_contracts.py "
                    "<committed-capacity-inventory> --as-of "
                    f"{batch.ACTIVATION_NOT_BEFORE.isoformat()}"
                ),
                (
                    "python3 dense_data_collection.py freeze-development "
                    "<committed-family-contract>"
                ),
            ]
        ),
        "provider_access_permitted": (
            activation_permitted and not blockers and collection is None
        ),
        "target_outcome_access_permitted": False,
        "broker_actions_permitted": False,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("status",))
    args = parser.parse_args()
    try:
        if args.command == "status":
            value = build_status()
        else:  # pragma: no cover - argparse enforces the command.
            raise DenseBatchReadinessError("unsupported command")
        print(json.dumps(value, indent=2, sort_keys=True))
        return 0
    except (
        DenseBatchReadinessError,
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

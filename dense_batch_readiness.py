"""Report the exact outcome-blind handoff for the authorized dense batch."""

from __future__ import annotations

import argparse
import json
import subprocess
from datetime import date
from pathlib import Path
from typing import Any

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
    output_state = (
        "ABSENT_READY_FOR_SINGLE_COLLECTION"
        if not calendar_path.exists() and not source_path.exists()
        else "COLLECTION_OUTPUT_PRESENT"
    )
    activation_permitted = current >= batch.ACTIVATION_NOT_BEFORE
    if blockers:
        state = "PREACTIVATION_BLOCKED"
    elif activation_permitted:
        state = "ACTIVATION_READY"
    else:
        state = "PREACTIVATION_READY"
    collection_command = (
        "python3 dense_session_calendar.py collect "
        f"{_repo_path(contract_path)} --as-of "
        f"{batch.ACTIVATION_NOT_BEFORE.isoformat()} --collected-at "
        "<actual-current-ISO8601-timestamp>"
    )
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
            "provider_requests_so_far": 0,
            "market_prices_accessed": False,
            "target_outcomes_accessed": False,
        },
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
        "gated_until_activation": [
            "calendar provider collection",
            "new-family capacity allocation and contract freeze",
            "development market-outcome collection",
        ],
        "next_commands": [
            collection_command,
            (
                "python3 dense_session_calendar_inspection.py "
                "<calendar-collection-artifact> --inspected-at "
                "<actual-current-ISO8601-timestamp>"
            ),
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
        ],
        "provider_access_permitted": activation_permitted and not blockers,
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

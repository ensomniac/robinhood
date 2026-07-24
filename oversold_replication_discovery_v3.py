"""Supersede oversold v5 after its empty-session runtime boundary failed closed.

The successor changes no evidence, parameter, cost, execution, selection, or
confirmation rule.  It binds the runtime correction that retains an inspected
empty candidate universe as an explicit no-signal account day.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from collections.abc import Sequence
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import dense_strategy_runtime as runtime
import oversold_replication_discovery_v2 as v2
import strategy_discovery
from historical_store import HistoricalDayStore, sha256_file


PROJECT_ROOT = Path(__file__).resolve().parent
SUCCESSOR_ID = (
    "short-horizon-oversold-reversal-v6-empty-session-runtime-fix"
)
EXPERIMENT_ID = f"experiment-{SUCCESSOR_ID}"
V5_SUCCESSOR_ID = v2.SUCCESSOR_ID
V5_EXPERIMENT_ID = v2.EXPERIMENT_ID
V5_SEARCH = (
    PROJECT_ROOT
    / "strategy_tournament/v2/discovery/"
    "gap-universe-oversold-reversal/search/"
    "gap-universe-oversold-reversal-search-"
    "a33a63bbc81cd5f4e1fda0da3f546ffe44c2ba0ee8d5c55dafc1eed4eede8f9f.json"
)
FAILURE_ROOT = (
    PROJECT_ROOT
    / "strategy_tournament/v2/discovery/"
    "gap-universe-oversold-reversal/development-failures"
)
DEFAULT_ROOT = PROJECT_ROOT / "strategy_tournament/v2/continuous"


class OversoldReplicationDiscoveryV3Error(RuntimeError):
    """The runtime-only successor boundary is incomplete or drifted."""


def _v5_search() -> dict[str, Any]:
    strategy_discovery.require_committed(V5_SEARCH)
    search = strategy_discovery.load_artifact(
        V5_SEARCH,
        expected_kind="frozen-development-search",
    )
    contract = search.get("family_contract")
    if not isinstance(contract, dict) or not (
        search.get("state") == "SEARCH_FROZEN"
        and search.get("trial_count") == 32
        and contract.get("experiment_id") == V5_EXPERIMENT_ID
        and contract.get("successor_id") == V5_SUCCESSOR_ID
        and contract.get("selection_mode") == "development_search"
        and search.get("outcomes_accessed") is False
        and search.get("confirmation_access_permitted") is False
    ):
        raise OversoldReplicationDiscoveryV3Error(
            "v5 frozen search binding is invalid"
        )
    return search


def _failure_payload(*, recorded_at: str) -> dict[str, Any]:
    v2.base._timestamp(recorded_at)
    search = _v5_search()
    before = search["family_contract"]["implementation_hashes"].get(
        "dense_strategy_runtime.py"
    )
    after = sha256_file(Path(runtime.__file__).resolve())
    if not (
        isinstance(before, str)
        and len(before) == 64
        and before != after
    ):
        raise OversoldReplicationDiscoveryV3Error(
            "runtime correction is not distinct from the v5 binding"
        )
    return {
        "schema_version": 1,
        "artifact_kind": "development-evaluation-failure",
        "state": "FAILED_EMPTY_CANDIDATE_UNIVERSE_BOUNDARY",
        "campaign_id": search["campaign_id"],
        "family_id": search["family_contract"]["family_id"],
        "successor_id": V5_SUCCESSOR_ID,
        "recorded_at": recorded_at,
        "search_path": v2.base._repo_path(V5_SEARCH),
        "search_sha256": search["artifact_sha256"],
        "error": (
            "frozen candidate universe is invalid for 2023-01-27"
        ),
        "failure_boundary": (
            "the inspected development dataset loaded, then shared "
            "normalization rejected the first intentionally empty frozen "
            "candidate list before candidate construction, account-path "
            "evaluation, trial metrics, or winner selection"
        ),
        "permitted_recovery": (
            "retire v5; a new exact successor may change only the runtime "
            "validation so an empty inspected candidate list remains an "
            "explicit no-signal zero-return day; the dataset, dates, grid, "
            "costs, execution rules, selection rule, and confirmation "
            "reserve must remain unchanged"
        ),
        "development_training_already_contaminated": True,
        "external_development_file_opened": True,
        "dataset_loads": 1,
        "provider_requests": 0,
        "trials_returned": 0,
        "trial_metrics_surfaced": False,
        "selection_executed": False,
        "result_artifact_written": False,
        "confirmation_accessed": False,
        "broker_actions": 0,
        "strategy_grid_dates_rules_costs_changed_after_failure": False,
        "runtime_before_sha256": before,
        "runtime_after_sha256": after,
    }


def record_v5_failure(
    *,
    recorded_at: str,
    root: Path = FAILURE_ROOT,
) -> tuple[Path, dict[str, Any]]:
    return strategy_discovery._write_artifact(
        _failure_payload(recorded_at=recorded_at),
        root,
        "gap-universe-oversold-reversal-development-failure",
    )


def _failure_chain() -> tuple[Path, dict[str, Any]]:
    paths = sorted(FAILURE_ROOT.glob("*.json"))
    matches: list[tuple[Path, dict[str, Any]]] = []
    for path in paths:
        value = strategy_discovery.load_artifact(
            path,
            expected_kind="development-evaluation-failure",
        )
        if value.get("search_sha256") == _v5_search()["artifact_sha256"]:
            matches.append((path, value))
    if len(matches) != 1:
        raise OversoldReplicationDiscoveryV3Error(
            "expected one v5 empty-session failure artifact"
        )
    path, failure = matches[0]
    strategy_discovery.require_committed(path)
    if not (
        failure.get("state")
        == "FAILED_EMPTY_CANDIDATE_UNIVERSE_BOUNDARY"
        and failure.get("trial_metrics_surfaced") is False
        and failure.get("selection_executed") is False
        and failure.get("result_artifact_written") is False
        and failure.get("confirmation_accessed") is False
        and failure.get(
            "strategy_grid_dates_rules_costs_changed_after_failure"
        )
        is False
    ):
        raise OversoldReplicationDiscoveryV3Error(
            "v5 failure boundary is invalid"
        )
    return path, failure


@contextmanager
def configured():
    original = (v2.SUCCESSOR_ID, v2.EXPERIMENT_ID)
    try:
        v2.SUCCESSOR_ID = SUCCESSOR_ID
        v2.EXPERIMENT_ID = EXPERIMENT_ID
        yield
    finally:
        v2.SUCCESSOR_ID, v2.EXPERIMENT_ID = original


def build_family_contract(
    *,
    created_at: str,
    store: HistoricalDayStore | None = None,
) -> dict[str, Any]:
    failure_path, failure = _failure_chain()
    with configured():
        contract = v2.build_family_contract(
            created_at=created_at,
            store=store,
        )
    contract["parent_experiment_id"] = V5_EXPERIMENT_ID
    contract["successor_id"] = SUCCESSOR_ID
    contract["experiment_id"] = EXPERIMENT_ID
    contract["prior_family_attempt_count"] = 5
    contract["material_difference_rationale"] = (
        "This exact successor changes only shared input validation so an "
        "inspected empty candidate list is retained as an explicit "
        "no-signal zero-return account day. The complete v5 evidence, "
        "dates, 32-trial grid, costs, execution rules, selection rule, "
        "embargo, and sealed confirmation reserve are unchanged."
    )
    contract["predecessor_runtime_failure"] = {
        "path": v2.base._repo_path(failure_path),
        "artifact_sha256": failure["artifact_sha256"],
        "state": failure["state"],
        "trial_metrics_surfaced": False,
        "confirmation_accessed": False,
    }
    contract["evidence_paths"] = [
        *contract["evidence_paths"],
        v2.base._repo_path(failure_path),
    ]
    contract["evidence_bindings"] = {
        **contract["evidence_bindings"],
        "v5_runtime_failure_sha256": failure["artifact_sha256"],
    }
    contract["implementation_files"] = [
        "oversold_replication_discovery_v3.py",
        *contract["implementation_files"],
    ]
    strategy_discovery._validate_family_contract(contract)
    return contract


def freeze_family(
    *,
    created_at: str,
    store: HistoricalDayStore | None = None,
) -> tuple[Path, dict[str, Any]]:
    for path in (
        Path(__file__).resolve(),
        Path(v2.__file__).resolve(),
        Path(v2.base.plugin.__file__).resolve(),
        PROJECT_ROOT / "oversold_reversal_plugin.py",
        PROJECT_ROOT / "oversold_replication_development_collection.py",
        PROJECT_ROOT / "oversold_replication_confirmation_v2.py",
        PROJECT_ROOT / "oversold_replication_confirmation_collection.py",
        PROJECT_ROOT / "oversold_replication_confirmation_dataset.py",
        PROJECT_ROOT / "oversold_scanner_builder_v2.py",
        PROJECT_ROOT / "oversold_scanner_target_audit.py",
        Path(runtime.__file__).resolve(),
        PROJECT_ROOT / "learning_statistics.py",
        PROJECT_ROOT / "learning_experiment.py",
        PROJECT_ROOT / "strategy_discovery.py",
        PROJECT_ROOT / "outcome_exposure.py",
        PROJECT_ROOT / "portfolio_maturity.py",
        PROJECT_ROOT / "portfolio_config.toml",
    ):
        strategy_discovery.require_committed(path)
    contract = build_family_contract(
        created_at=created_at,
        store=store,
    )
    digest = hashlib.sha256(v2.base._canonical(contract)).hexdigest()
    path = (
        DEFAULT_ROOT
        / SUCCESSOR_ID
        / "family-contract"
        / f"contract-{digest}.json"
    )
    v2.base._write(path, contract)
    return path, contract


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "command",
        choices=("record-v5-failure", "freeze-family", "status"),
    )
    parser.add_argument("--recorded-at")
    parser.add_argument("--created-at")
    args = parser.parse_args(argv)
    try:
        if args.command == "record-v5-failure":
            if not args.recorded_at:
                raise OversoldReplicationDiscoveryV3Error(
                    "--recorded-at is required"
                )
            path, value = record_v5_failure(
                recorded_at=args.recorded_at
            )
            result = {
                "state": value["state"],
                "path": v2.base._repo_path(path),
                "trial_metrics_surfaced": False,
                "confirmation_accessed": False,
            }
        elif args.command == "freeze-family":
            if not args.created_at:
                raise OversoldReplicationDiscoveryV3Error(
                    "--created-at is required"
                )
            path, contract = freeze_family(
                created_at=args.created_at
            )
            result = {
                "state": "FAMILY_FROZEN",
                "path": v2.base._repo_path(path),
                "successor_id": SUCCESSOR_ID,
                "trials": v2.base.development._trial_count(),
                "development_signal_dates": len(
                    contract["development_signal_dates"]
                ),
                "confirmation_signal_capacity": contract[
                    "confirmation_signal_capacity"
                ],
            }
        else:
            failures = [
                path
                for path in FAILURE_ROOT.glob("*.json")
                if strategy_discovery.load_artifact(path).get(
                    "search_sha256"
                )
                == _v5_search()["artifact_sha256"]
            ]
            contracts = sorted(
                (
                    DEFAULT_ROOT
                    / SUCCESSOR_ID
                    / "family-contract"
                ).glob("contract-*.json")
            )
            result = {
                "state": (
                    "FAMILY_FROZEN"
                    if len(contracts) == 1
                    else (
                        "READY_TO_FREEZE"
                        if len(failures) == 1
                        else "READY_TO_RECORD_FAILURE"
                    )
                ),
                "v5_failure_artifacts": len(failures),
                "v6_contracts": len(contracts),
                "successor_id": SUCCESSOR_ID,
                "calendar_wait_required": False,
            }
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    except (
        KeyError,
        OSError,
        OversoldReplicationDiscoveryV3Error,
        v2.OversoldReplicationDiscoveryV2Error,
        v2.base.OversoldReplicationDiscoveryError,
        ValueError,
    ) as exc:
        print(
            json.dumps(
                {"state": "BLOCKED", "error": str(exc)},
                indent=2,
                sort_keys=True,
            )
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())

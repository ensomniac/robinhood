"""Freeze the complete exact-grid oversold-replication family contract."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from collections.abc import Mapping, Sequence
from datetime import datetime
from pathlib import Path
from typing import Any

import dense_strategy_runtime as runtime
import outcome_exposure
import oversold_replication_confirmation as confirmation
import oversold_replication_development as development
import oversold_replication_plugin as plugin
import portfolio_maturity
import strategy_discovery
from historical_store import HistoricalDayStore
from learning_data import load_frozen_dataset_contract
from learning_experiment import DEVELOPMENT_SEARCH_RULE


PROJECT_ROOT = Path(__file__).resolve().parent
CAMPAIGN_ID = portfolio_maturity.V2_CAMPAIGN_ID
FAMILY_ID = runtime.OVERSOLD_REVERSAL_FAMILY
MECHANISM_FAMILY = "short-horizon-oversold-reversal"
STRATEGY_ID = MECHANISM_FAMILY
SUCCESSOR_ID = development.SUCCESSOR_ID
EXPERIMENT_ID = f"experiment-{SUCCESSOR_ID}"
DEFAULT_ROOT = PROJECT_ROOT / "strategy_tournament/v2/continuous"
DEVELOPMENT_MANIFEST = (
    PROJECT_ROOT
    / "strategy_tournament/v2/continuous/"
    "short-horizon-oversold-reversal-v4-broad-replication/capacity/"
    "dataset-short-horizon-oversold-reversal-v4-broad-development-"
    "77eb3cdfadd61b99dee987771aff80753bb2074ac2b9b577d5c7f8dad4ddd6a0.json"
)
DEVELOPMENT_MANIFEST_INSPECTION = (
    PROJECT_ROOT
    / "strategy_tournament/v2/continuous/"
    "short-horizon-oversold-reversal-v4-broad-replication/"
    "capacity-inspection/"
    "oversold-replication-development-dataset-inspection-"
    "449dba3f8abf800771df5a3529576a722435367dbfa6b8170d359500152cddf3.json"
)


class OversoldReplicationDiscoveryError(RuntimeError):
    """The complete family evidence boundary is incomplete or drifted."""


def _canonical(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")


def _read(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise OversoldReplicationDiscoveryError(
            f"cannot read {path}: {exc}"
        ) from exc
    if not isinstance(value, dict):
        raise OversoldReplicationDiscoveryError(
            f"{path} must contain an object"
        )
    return value


def _write(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    rendered = json.dumps(value, indent=2, sort_keys=True) + "\n"
    if path.exists() and path.read_text(encoding="utf-8") == rendered:
        return
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_text(rendered, encoding="utf-8")
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _repo_path(path: Path) -> str:
    try:
        return path.resolve().relative_to(PROJECT_ROOT.resolve()).as_posix()
    except ValueError as exc:
        raise OversoldReplicationDiscoveryError(
            f"path escaped repository: {path}"
        ) from exc


def _timestamp(value: str) -> None:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise OversoldReplicationDiscoveryError(
            "created_at is invalid"
        ) from exc
    if parsed.tzinfo is None:
        raise OversoldReplicationDiscoveryError(
            "created_at needs a timezone"
        )


def _scope(
    dates: Sequence[str],
    candidates_by_date: Mapping[str, Sequence[Mapping[str, Any]]],
) -> dict[str, Any]:
    signal_dates = [
        day for day in dates if candidates_by_date[day]
    ]
    return {
        "dates": signal_dates,
        "symbols_by_date": {
            day: sorted(
                str(row["symbol"])
                for row in candidates_by_date[day]
            )
            for day in signal_dates
        },
    }


def _development_chain(
    store: HistoricalDayStore,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    for path in (
        DEVELOPMENT_MANIFEST,
        DEVELOPMENT_MANIFEST_INSPECTION,
    ):
        strategy_discovery.require_committed(path)
    manifest = load_frozen_dataset_contract(DEVELOPMENT_MANIFEST)
    inspection = development._read(
        DEVELOPMENT_MANIFEST_INSPECTION
    )
    inventory = development._read_gzip(
        development._inventory_path(store)
    )
    if not (
        inspection.get("state")
        == "DEVELOPMENT_DATASET_INSPECTED_READY"
        and inspection.get("valid") is True
        and inspection.get("manifest_sha256")
        == manifest["manifest_sha256"]
        and manifest["requested_dates"]
        == inventory["evaluation_dates"]
        and inventory.get("lane") == "development"
        and inventory.get("target_outcomes_observed_or_derived")
        is False
    ):
        raise OversoldReplicationDiscoveryError(
            "development dataset chain is invalid"
        )
    return manifest, inspection, inventory


def _confirmation_chain(
    store: HistoricalDayStore,
) -> tuple[Path, dict[str, Any], Path, dict[str, Any], dict[str, Any]]:
    contract_path = confirmation._one(confirmation.CONTRACT_ROOT)
    inspection_path = confirmation._one(confirmation.INSPECTION_ROOT)
    for path in (contract_path, inspection_path):
        strategy_discovery.require_committed(path)
    contract = confirmation._load_contract(contract_path)
    inspection = confirmation._load_hashed(
        inspection_path,
        identity_field="inspection_sha256",
        expected_kind=(
            "oversold_replication_confirmation_inventory_inspection"
        ),
    )
    inventory = development._read_gzip(
        confirmation._inventory_path(store)
    )
    if not (
        inspection.get("state")
        == "CONFIRMATION_INVENTORY_INSPECTED_READY"
        and inspection.get("valid") is True
        and inspection.get("contract_sha256")
        == contract["contract_sha256"]
        and inspection.get("private_inventory_content_sha256")
        == inventory.get("content_sha256")
        and inventory.get("lane") == "confirmation"
        and inventory.get("target_outcomes_observed_or_derived")
        is False
    ):
        raise OversoldReplicationDiscoveryError(
            "confirmation inventory chain is invalid"
        )
    outcome_exposure.assert_untouched(
        inventory["outcome_scope"],
        outcome_exposure.read_index(),
    )
    return (
        contract_path,
        contract,
        inspection_path,
        inspection,
        inventory,
    )


def build_family_contract(
    *,
    created_at: str,
    store: HistoricalDayStore | None = None,
) -> dict[str, Any]:
    _timestamp(created_at)
    source = store or HistoricalDayStore.from_env()
    development_manifest, development_inspection, development_inventory = (
        _development_chain(source)
    )
    (
        confirmation_contract_path,
        confirmation_contract,
        confirmation_inspection_path,
        confirmation_inspection,
        confirmation_inventory,
    ) = _confirmation_chain(source)
    development_dates = list(
        development_inventory["evaluation_dates"]
    )
    embargo_dates = list(confirmation_inventory["embargo_dates"])
    confirmation_dates = list(
        confirmation_inventory["evaluation_dates"]
    )
    development_scope = _scope(
        development_dates,
        development_inventory["candidates_by_date"],
    )
    confirmation_scope = _scope(
        confirmation_dates,
        confirmation_inventory["candidates_by_date"],
    )
    if not (
        development_dates[-1] < embargo_dates[0]
        and embargo_dates[-1] < confirmation_dates[0]
        and len(embargo_dates) == 5
    ):
        raise OversoldReplicationDiscoveryError(
            "development-confirmation chronology drifted"
        )
    if not outcome_exposure.find_overlaps(
        development_scope,
        outcome_exposure.read_index(),
    ):
        raise OversoldReplicationDiscoveryError(
            "development is not explicitly contaminated"
        )
    outcome_exposure.assert_untouched(
        confirmation_scope,
        outcome_exposure.read_index(),
    )
    outcome_exposure.assert_disjoint(
        [development_scope, confirmation_scope]
    )
    evidence_paths = [
        _repo_path(DEVELOPMENT_MANIFEST),
        _repo_path(DEVELOPMENT_MANIFEST_INSPECTION),
        _repo_path(confirmation_contract_path),
        _repo_path(confirmation_inspection_path),
    ]
    contract: dict[str, Any] = {
        "schema_version": 1,
        "campaign_id": CAMPAIGN_ID,
        "experiment_id": EXPERIMENT_ID,
        "family_id": FAMILY_ID,
        "mechanism_family": MECHANISM_FAMILY,
        "strategy_id": STRATEGY_ID,
        "parent_experiment_id": (
            "experiment-short-horizon-oversold-reversal-v3-gap-universe"
        ),
        "created_at": created_at,
        "status": "INVENTED",
        "research_generation": "existing_family_replication",
        "successor_id": SUCCESSOR_ID,
        "new_mechanism_family_slot_consumed": False,
        "prior_family_attempt_count": 3,
        "mechanism": (
            "Intraday mean reversion after a completed short-horizon "
            "oversold selloff inside a point-in-time liquid opening-gap "
            "common-stock universe."
        ),
        "expected_holding_behavior": (
            "Long only, next-minute entry, and flat by 15:50 ET on the "
            "signal day."
        ),
        "dataset_lane": "development",
        "universe_requirements": {
            "security_type": (
                "point-in-time active U.S. common stocks"
            ),
            "opening_price_minimum": 5.0,
            "opening_gap_fraction": [0.02, 0.08],
            "selection_time_et": "09:35:00",
            "complete_candidate_denominator": True,
        },
        "entry_rule": (
            "After completed lookback selloff, simple RSI, bullish "
            "prior-high and session-VWAP reclaim gates, enter the ranked "
            "symbol at the next observed one-minute open."
        ),
        "stop_rule": (
            "Use the lowest completed session low through the trigger bar; "
            "a nonpositive structural stop produces a missed trade."
        ),
        "exit_rule": (
            "Resolve the frozen raw-R target or stop with stop-first "
            "same-minute ambiguity and otherwise force flat at the 15:50 "
            "bar open."
        ),
        "ranking_rule": (
            "Earliest next-minute entry, then deepest selloff, lowest RSI, "
            "and lexical symbol."
        ),
        "selection_rule": (
            "At most one new family entry per day under the authoritative "
            "portfolio risk, notional, entry, and capital-contention caps."
        ),
        "parameter_grid": development.PARAMETER_GRID,
        "selection_mode": "development_search",
        "winner_selection": DEVELOPMENT_SEARCH_RULE,
        "primary_outcome": (
            "Selection-adjusted chronological account log growth after "
            "costs."
        ),
        "execution_assumptions": {
            "next_observable_fill": True,
            "ambiguity": "stop_first",
            "maximum_hold_sessions": 1,
            "missing_data": "retained_denominator_no_signal",
            "minimum_gross_to_primary_round_trip_cost": 5.0,
        },
        "falsification_criteria": {
            "minimum_20bps_log_growth": 0.0,
            "minimum_stressed_profit_factor": 1.2,
            "maximum_drawdown_r": 6.0,
            "minimum_deflated_sharpe_probability": 0.9,
            "maximum_pbo_probability": 0.5,
        },
        "minimum_evidence": {
            "configured_floor": 50,
            "confirmation_floor": 20,
            "power": 0.8,
            "alpha": 0.1,
        },
        "contamination_risks": [
            "Every 2023-2024 full-session input is explicitly development-only.",
            "Prior oversold-family results are adverse history and cannot satisfy this version.",
            "The 2026 reserve remains limited to information observable at 09:35 ET until one winner is immutable.",
        ],
        "production_compatibility_risks": [
            "Live completeness of the 09:35 gap universe, fresh quote, spread, depth, halt, tradability, timing, protection, and reconciliation remain mandatory."
        ],
        "material_difference_rationale": (
            "This exact replication preserves the predecessor's complete "
            "32-trial rule grid while expanding temporal development "
            "coverage across 399 sessions and reserving a later continuous "
            "2026 confirmation inventory."
        ),
        "development_dates": development_dates,
        "development_signal_dates": list(
            development_inventory["signal_dates"]
        ),
        "embargo_dates": embargo_dates,
        "confirmation_dates": confirmation_dates,
        "confirmation_signal_dates": list(
            confirmation_inventory["signal_dates"]
        ),
        "confirmation_signal_capacity": len(
            confirmation_inventory["signal_dates"]
        ),
        "development_scope": development_scope,
        "confirmation_scope": confirmation_scope,
        "outcome_exposure_index_sha256": outcome_exposure.audit()[
            "index_sha256"
        ],
        "universe": {
            "identity": (
                "complete point-in-time active U.S. common-stock 2-8% "
                "opening-gap candidates at 09:35 ET"
            ),
            "development_candidate_symbol_sessions": sum(
                len(rows)
                for rows in development_inventory[
                    "candidates_by_date"
                ].values()
            ),
            "confirmation_candidate_symbol_sessions": sum(
                len(rows)
                for rows in confirmation_inventory[
                    "candidates_by_date"
                ].values()
            ),
            "point_in_time": True,
        },
        "costs_bps_per_side": [5, 10, 20],
        "partitions": {
            "rolling_origin": True,
            "confirmation_untouched": True,
            "development_training_contaminated": True,
            "five_session_embargo": True,
        },
        "falsifiers": [
            "nonpositive stressed log growth",
            "unstable parameter neighbors",
            "selection-aware statistical rejection",
            "incomplete execution or trial accounting",
            "insufficient frozen confirmation power capacity",
        ],
        "implementation_files": [
            "oversold_replication_discovery.py",
            "oversold_replication_plugin.py",
            "oversold_reversal_plugin.py",
            "oversold_replication_development_collection.py",
            "oversold_replication_confirmation.py",
            "dense_strategy_runtime.py",
            "learning_statistics.py",
            "learning_experiment.py",
            "strategy_discovery.py",
            "outcome_exposure.py",
            "portfolio_maturity.py",
            "portfolio_config.toml",
        ],
        "plugin": {
            "module": "oversold_replication_plugin",
            "preflight": "preflight",
            "evaluate_development": "evaluate_development",
            "evaluate_confirmation": "evaluate_confirmation",
            "evaluate_production": "evaluate_production",
        },
        "capacity_policy": {
            "retire_below": 50,
            "fast_lane_at": 100,
        },
        "capacity_manifest": _repo_path(DEVELOPMENT_MANIFEST),
        "dataset_manifest": _repo_path(DEVELOPMENT_MANIFEST),
        "evidence_paths": evidence_paths,
        "evidence_bindings": {
            "development_manifest_sha256": development_manifest[
                "manifest_sha256"
            ],
            "development_inspection_sha256": development_inspection[
                "inspection_sha256"
            ],
            "confirmation_contract_sha256": confirmation_contract[
                "contract_sha256"
            ],
            "confirmation_inspection_sha256": confirmation_inspection[
                "inspection_sha256"
            ],
        },
    }
    strategy_discovery._validate_family_contract(contract)
    return contract


def freeze_family(
    *,
    created_at: str,
    root: Path = DEFAULT_ROOT,
    store: HistoricalDayStore | None = None,
) -> tuple[Path, dict[str, Any]]:
    for path in (
        Path(__file__).resolve(),
        Path(plugin.__file__).resolve(),
        PROJECT_ROOT / "oversold_reversal_plugin.py",
        PROJECT_ROOT / "oversold_replication_development_collection.py",
        PROJECT_ROOT / "oversold_replication_confirmation.py",
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
    digest = hashlib.sha256(_canonical(contract)).hexdigest()
    path = (
        root
        / SUCCESSOR_ID
        / "family-contract"
        / f"contract-{digest}.json"
    )
    _write(path, contract)
    return path, contract


def status(*, root: Path = DEFAULT_ROOT) -> dict[str, Any]:
    contracts = sorted(
        (root / SUCCESSOR_ID / "family-contract").glob("contract-*.json")
    )
    confirmation_inspections = sorted(
        confirmation.INSPECTION_ROOT.glob("*.json")
    )
    return {
        "schema_version": 1,
        "campaign_id": CAMPAIGN_ID,
        "successor_id": SUCCESSOR_ID,
        "family_id": FAMILY_ID,
        "mechanism_family": MECHANISM_FAMILY,
        "calendar_wait_required": False,
        "new_mechanism_family_slot_consumed": False,
        "development_dataset_ready": DEVELOPMENT_MANIFEST_INSPECTION.is_file(),
        "confirmation_inventory_ready": len(confirmation_inspections) == 1,
        "family_contracts": len(contracts),
        "state": (
            "AWAITING_CONFIRMATION_INVENTORY"
            if not confirmation_inspections
            else "READY_TO_FREEZE"
            if not contracts
            else "CONTRACT_FROZEN"
        ),
        "broker_actions_permitted": False,
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("status")
    freeze = sub.add_parser("freeze")
    freeze.add_argument("--created-at", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "status":
            value = status()
        else:
            path, contract = freeze_family(
                created_at=args.created_at
            )
            value = {
                "path": _repo_path(path),
                "state": contract["status"],
                "trial_count": 32,
                "development_dates": len(
                    contract["development_dates"]
                ),
                "confirmation_dates": len(
                    contract["confirmation_dates"]
                ),
                "confirmation_signal_capacity": contract[
                    "confirmation_signal_capacity"
                ],
                "calendar_wait_required": False,
                "broker_actions_permitted": False,
            }
        print(json.dumps(value, indent=2, sort_keys=True))
        return 0
    except (
        KeyError,
        OSError,
        OversoldReplicationDiscoveryError,
        outcome_exposure.OutcomeExposureError,
        strategy_discovery.StrategyDiscoveryError,
        ValueError,
    ) as exc:
        print(
            json.dumps(
                {
                    "state": "BLOCKED",
                    "error": str(exc),
                    "calendar_wait_required": False,
                    "broker_actions_permitted": False,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())

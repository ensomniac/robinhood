"""Freeze the fixed-rule SPY RSI(2) successor for generic discovery."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any

import outcome_exposure
import portfolio_maturity
import spy_rsi2_data as source
import strategy_discovery
from historical_store import sha256_file
from learning_data import load_frozen_dataset_contract
from learning_experiment import DEVELOPMENT_SEARCH_RULE


PROJECT_ROOT = Path(__file__).resolve().parent
CAMPAIGN_ID = portfolio_maturity.V2_CAMPAIGN_ID
FAMILY_ID = source.FAMILY_ID
MECHANISM_FAMILY = "broad-etf-trend-pullback"
STRATEGY_ID = "spy-rsi2-trend-pullback"
SUCCESSOR_ID = source.SUCCESSOR_ID
DEFAULT_ROOT = source.DEFAULT_ROOT


class SpyRsi2DiscoveryError(RuntimeError):
    """The fixed SPY RSI(2) evidence or family binding drifted."""


def freeze_family_contract(
    development_inspection_path: Path,
    *,
    created_at: str,
    root: Path = DEFAULT_ROOT,
) -> tuple[Path, dict[str, Any]]:
    for path in (
        Path(__file__).resolve(),
        development_inspection_path,
    ):
        strategy_discovery.require_committed(path)
    source._timestamp(created_at, "created_at")
    development_inspection = source._read(development_inspection_path)
    collection_path = (
        PROJECT_ROOT / str(development_inspection["collection_path"])
    )
    strategy_discovery.require_committed(collection_path)
    collection = source._read(collection_path)
    contract_path = PROJECT_ROOT / str(collection["contract_path"])
    source_inspection_path = PROJECT_ROOT / str(collection["inspection_path"])
    manifest_path = (
        PROJECT_ROOT / str(development_inspection["dataset_manifest_path"])
    )
    for path in (contract_path, source_inspection_path, manifest_path):
        strategy_discovery.require_committed(path)
    source_contract = source._read(contract_path)
    source_inspection = source._read(source_inspection_path)
    manifest = load_frozen_dataset_contract(manifest_path)
    if not (
        development_inspection.get("state")
        == "DEVELOPMENT_DATASET_INSPECTED_READY"
        and development_inspection.get("valid") is True
        and source_inspection.get("state") == "SOURCE_CONTRACT_INSPECTED_READY"
        and source_inspection.get("valid") is True
        and collection.get("contract_sha256")
        == source_contract.get("contract_sha256")
        and manifest.get("manifest_sha256")
        == development_inspection.get("dataset_manifest_sha256")
        and manifest["dataset_payload"].get("dense_runtime", {}).get("family_id")
        == FAMILY_ID
    ):
        raise SpyRsi2DiscoveryError("SPY RSI(2) development evidence graph is invalid")
    development = list(source_contract["development_dates"])
    embargo = list(source_contract["embargo_dates"])
    confirmation = list(source_contract["confirmation_dates"])
    index = outcome_exposure.read_index()
    if not outcome_exposure.find_overlaps(
        source_contract["development_scope"],
        index,
    ):
        raise SpyRsi2DiscoveryError(
            "development source access is not globally indexed"
        )
    outcome_exposure.assert_untouched(
        source_contract["confirmation_scope"],
        index,
    )
    outcome_exposure.assert_disjoint(
        [
            source_contract["development_scope"],
            source_contract["confirmation_scope"],
        ]
    )
    all_before_confirmation = [
        *source_contract["warmup_dates"],
        *development,
        *embargo,
    ]
    value: dict[str, Any] = {
        "schema_version": 1,
        "campaign_id": CAMPAIGN_ID,
        "experiment_id": f"experiment-{SUCCESSOR_ID}",
        "family_id": FAMILY_ID,
        "mechanism_family": MECHANISM_FAMILY,
        "strategy_id": STRATEGY_ID,
        "parent_experiment_id": "broad-etf-trend-pullback-v1",
        "created_at": source._timestamp(created_at, "created_at"),
        "status": "INVENTED",
        "research_generation": "existing_family_fixed_rule_successor",
        "new_mechanism_family_slot_consumed": False,
        "mechanism": (
            "A sharp two-session momentum exhaustion while SPY remains above "
            "its completed SMA200 can mean-revert toward the completed SMA5."
        ),
        "expected_holding_behavior": (
            "Long SPY only from the next observable open and flat on a completed "
            "SMA5 reclaim, structural stop, or within five sessions."
        ),
        "dataset_lane": "development",
        "universe_requirements": {
            "symbols": ["SPY"],
            "complete_adjusted_xnys_daily_history": True,
            "point_in_time": True,
        },
        "entry_rule": (
            "After a completed close above SMA200 with Wilder RSI(2) at or below "
            "10, enter SPY at the next XNYS open only when completed SMA5 divided "
            "by that observable open minus one is at least 0.005."
        ),
        "stop_rule": (
            "Use a structural stop 1.5 completed ATR14 below entry; missing or "
            "nonpositive stops reject the entry."
        ),
        "exit_rule": (
            "Resolve a stop first, otherwise exit at the first completed close "
            "at or above that session's SMA5, or at the fifth-session close."
        ),
        "ranking_rule": (
            "SPY is the sole frozen instrument and therefore receives rank one."
        ),
        "selection_rule": (
            "Evaluate the sole preregistered trial through rolling-origin "
            "out-of-fold account simulation and every selection-aware gate."
        ),
        "parameter_grid": {
            key: [value] for key, value in source.PARAMETERS.items()
        },
        "selection_mode": "development_search",
        "winner_selection": DEVELOPMENT_SEARCH_RULE,
        "primary_outcome": (
            "Selection-adjusted chronological account log growth after costs."
        ),
        "execution_assumptions": {
            "next_observable_fill": True,
            "ambiguity": "stop_first",
            "maximum_hold_sessions": 5,
            "missing_data": "missed_trade_no_substitute",
            "minimum_gross_to_primary_round_trip_cost": 5.0,
            "overnight_protection": "gtc_required",
        },
        "falsification_criteria": {
            "minimum_20bps_log_growth": 0.0,
            "minimum_stressed_profit_factor": 1.2,
            "maximum_drawdown_r": 6.0,
            "minimum_deflated_sharpe_probability": 0.9,
            "maximum_pbo_probability": 0.5,
            "all_rolling_folds_positive": True,
        },
        "minimum_evidence": {
            "configured_floor": 50,
            "confirmation_floor": 20,
            "power": 0.8,
            "alpha": 0.1,
        },
        "contamination_risks": [
            "Development bars were opened only after the fixed source contract was committed and independently inspected.",
            "The rejected 32-trial ETF pullback evidence is adverse related history and supplies no promotion evidence.",
            "Confirmation prices remain inaccessible until an exact winner is frozen.",
        ],
        "production_compatibility_risks": [
            "A fresh next-open ask can erase the SMA5 cost floor.",
            "Every overnight hold requires confirmed GTC protection and before-open reconciliation.",
        ],
        "material_difference_rationale": (
            "Unlike the rejected broad ETF grid, this exact fixed successor uses "
            "only SPY, removes the mandatory three-session-decline parameter, "
            "and exits dynamically on a completed SMA5 reclaim; it reuses no "
            "development or confirmation outcome."
        ),
        "related_adverse_history": source_contract["related_adverse_history"],
        "development_dates": development,
        "development_warmup_dates": list(source_contract["warmup_dates"]),
        "embargo_dates": embargo,
        "confirmation_dates": confirmation,
        "confirmation_warmup_dates": all_before_confirmation[-200:],
        "development_scope": dict(source_contract["development_scope"]),
        "confirmation_scope": dict(source_contract["confirmation_scope"]),
        "confirmation_signal_capacity": len(confirmation),
        "outcome_exposure_index_sha256": outcome_exposure.audit()["index_sha256"],
        "universe": {"symbols": ["SPY"], "point_in_time": True},
        "costs_bps_per_side": [5, 10, 20],
        "partitions": {
            "rolling_origin": True,
            "confirmation_untouched": True,
            "development_training_contaminated": True,
            "five_session_embargo": True,
        },
        "falsifiers": [
            "nonpositive 20-bps log growth",
            "stressed profit factor below 1.20",
            "drawdown above 6R",
            "any nonpositive rolling fold",
            "selection-aware statistical rejection",
            "incomplete execution or zero-day accounting",
            "insufficient frozen confirmation power capacity",
        ],
        "implementation_files": [
            "spy_rsi2_discovery.py",
            "spy_rsi2_data.py",
            "spy_rsi2_data_inspection.py",
            "dense_strategy_plugin.py",
            "dense_strategy_runtime.py",
            "learning_statistics.py",
            "learning_experiment.py",
            "strategy_discovery.py",
            "outcome_exposure.py",
            "portfolio_maturity.py",
            "portfolio_config.toml",
        ],
        "plugin": {
            "module": "dense_strategy_plugin",
            "preflight": "preflight",
            "evaluate_development": "evaluate_development",
            "evaluate_confirmation": "evaluate_confirmation",
            "evaluate_production": "evaluate_production",
        },
        "capacity_policy": {"retire_below": 50, "fast_lane_at": 100},
        "capacity_manifest": source._repo_path(manifest_path),
        "dataset_manifest": source._repo_path(manifest_path),
        "source_graph": {
            "source_contract_path": source._repo_path(contract_path),
            "source_contract_file_sha256": sha256_file(contract_path),
            "source_inspection_path": source._repo_path(source_inspection_path),
            "source_inspection_file_sha256": sha256_file(
                source_inspection_path
            ),
            "collection_path": source._repo_path(collection_path),
            "collection_file_sha256": sha256_file(collection_path),
            "development_inspection_path": source._repo_path(
                development_inspection_path
            ),
            "development_inspection_file_sha256": sha256_file(
                development_inspection_path
            ),
            "dataset_manifest_path": source._repo_path(manifest_path),
            "dataset_manifest_file_sha256": sha256_file(manifest_path),
        },
    }
    validated = strategy_discovery._validate_family_contract(value)
    digest = hashlib.sha256(source.canonical_bytes(validated)).hexdigest()
    path = root / "family-contract" / f"contract-{digest}.json"
    source._write(path, validated)
    return path, validated


def status(*, root: Path = DEFAULT_ROOT) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "campaign_id": CAMPAIGN_ID,
        "successor_id": SUCCESSOR_ID,
        "family_id": FAMILY_ID,
        "mechanism_family": MECHANISM_FAMILY,
        "calendar_wait_required": False,
        "new_mechanism_family_slot_consumed": False,
        "source_contracts": len(list((root / "source-contract").glob("contract-*.json"))),
        "development_collections": len(
            list((root / "development-collection").glob("collection-*.json"))
        ),
        "family_contracts": len(
            list((root / "family-contract").glob("contract-*.json"))
        ),
        "confirmation_prices_accessed": False,
        "broker_actions": 0,
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    freeze = subparsers.add_parser("freeze-family")
    freeze.add_argument("development_inspection", type=Path)
    freeze.add_argument("--created-at", required=True)
    subparsers.add_parser("status")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "status":
            result = status()
        else:
            path, contract = freeze_family_contract(
                args.development_inspection,
                created_at=args.created_at,
            )
            result = {
                "path": str(path),
                "family_id": contract["family_id"],
                "trial_count": len(contract["trial_family"]),
            }
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    except (
        SpyRsi2DiscoveryError,
        source.SpyRsi2DataError,
        OSError,
        outcome_exposure.OutcomeExposureError,
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

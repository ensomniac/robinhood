"""Freeze an existing-family SPY turn-of-month seasonality successor."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any

import dense_strategy_runtime as runtime
import etf_cross_sectional_momentum_discovery as source
import outcome_exposure
import portfolio_maturity
import strategy_discovery
from historical_store import sha256_file
from learning_data import freeze_dataset_contract, load_frozen_dataset_contract
from learning_experiment import DEVELOPMENT_SEARCH_RULE


PROJECT_ROOT = Path(__file__).resolve().parent
CAMPAIGN_ID = portfolio_maturity.V2_CAMPAIGN_ID
FAMILY_ID = runtime.ETF_TURN_OF_MONTH_FAMILY
MECHANISM_FAMILY = "turn-of-month-etf-seasonality"
STRATEGY_ID = MECHANISM_FAMILY
SUCCESSOR_ID = "turn-of-month-etf-seasonality-v2-liquid-spy"
RESEARCH_GENERATION = "existing_family_successor"
DEFAULT_ROOT = PROJECT_ROOT / "strategy_tournament/v2/continuous"

PREDECESSOR_VARIANT_ID = "turn-of-month-etf-seasonality-v1"
PREDECESSOR_RESULT = (
    PROJECT_ROOT
    / "research_results/2026-07-21-turn-of-month-etf-seasonality-stage0-"
    "392a840bb533fd9ace45075774f6318f04acdd69d1a1b9bfacc41bccb310abf6.json"
)
PREDECESSOR_INSPECTION = (
    PROJECT_ROOT
    / "strategy_tournament/second_wave/inspections/"
    "turn-of-month-etf-seasonality-v1-result-"
    "8a4eba64ecfaa2322ae20fcc77eb0bd50758e8f43534b294916b11e255cb7298.json"
)
SOURCE_SEARCH = source.SOURCE_SEARCH
SOURCE_RESULT = source.SOURCE_RESULT
SOURCE_INSPECTION = source.SOURCE_INSPECTION
SOURCE_DATASET_MANIFEST = source.SOURCE_DATASET_MANIFEST


class EtfTurnOfMonthDiscoveryError(RuntimeError):
    """The successor source graph or evidence boundary is invalid."""


def _source_graph(
    *, enforce_commit: bool
) -> tuple[dict[str, Any], dict[str, Any]]:
    paths = (
        PREDECESSOR_RESULT,
        PREDECESSOR_INSPECTION,
        SOURCE_SEARCH,
        SOURCE_RESULT,
        SOURCE_INSPECTION,
        SOURCE_DATASET_MANIFEST,
    )
    if enforce_commit:
        for path in paths:
            strategy_discovery.require_committed(path)
    predecessor = source._read(PREDECESSOR_RESULT)
    predecessor_inspection = source._read(PREDECESSOR_INSPECTION)
    search = strategy_discovery.load_artifact(
        SOURCE_SEARCH, expected_kind="frozen-development-search"
    )
    result = strategy_discovery.load_artifact(
        SOURCE_RESULT, expected_kind="development-search-result"
    )
    inspection = strategy_discovery.load_artifact(
        SOURCE_INSPECTION, expected_kind="development-search-inspection"
    )
    load_frozen_dataset_contract(SOURCE_DATASET_MANIFEST)
    if not (
        predecessor.get("variant_id") == PREDECESSOR_VARIANT_ID
        and predecessor.get("mechanism_family") == MECHANISM_FAMILY
        and predecessor.get("stage0_survived") is False
        and predecessor.get("development_evidence_eligible") is False
        and predecessor.get("confirmation_evidence_eligible") is False
        and len(predecessor.get("records", [])) == 36
        and predecessor_inspection.get("result_sha256")
        == predecessor.get("result_sha256")
        and predecessor_inspection.get("valid") is True
        and search.get("artifact_sha256") == result.get("search_sha256")
        and result.get("artifact_sha256") == inspection.get("result_sha256")
        and inspection.get("state") == "REJECTED"
        and inspection.get("inspection", {}).get("valid") is True
        and result["evaluation"].get("dataset_manifest")
        == str(SOURCE_DATASET_MANIFEST)
    ):
        raise EtfTurnOfMonthDiscoveryError(
            "bound predecessor or source development graph is invalid"
        )
    source_contract = search["family_contract"]
    if not (
        source_contract.get("family_id") == runtime.ETF_PULLBACK_FAMILY
        and source_contract.get("universe", {}).get("symbols")
        == ["SPY", "QQQ", "IWM", "DIA"]
        and len(source_contract.get("development_dates", [])) == 1_000
        and len(source_contract.get("development_warmup_dates", [])) == 200
        and len(source_contract.get("embargo_dates", [])) == 5
        and len(source_contract.get("confirmation_dates", [])) == 500
    ):
        raise EtfTurnOfMonthDiscoveryError(
            "source ETF partitions or universe drifted"
        )
    return predecessor, search


def freeze_successor_contract(
    *,
    created_at: str,
    root: Path = DEFAULT_ROOT,
    enforce_commit: bool = True,
) -> tuple[Path, dict[str, Any], Path]:
    """Freeze all 32 trials without opening external daily price rows."""

    source._timestamp(created_at)
    predecessor, source_search = _source_graph(
        enforce_commit=enforce_commit
    )
    source_contract = source_search["family_contract"]
    source_manifest = load_frozen_dataset_contract(SOURCE_DATASET_MANIFEST)
    source_binding = source_manifest["dataset_payload"]["dense_runtime"]
    development = list(source_contract["development_dates"])
    warmup = list(source_contract["development_warmup_dates"])
    embargo = list(source_contract["embargo_dates"])
    confirmation = list(source_contract["confirmation_dates"])
    development_scope = {
        "dates": list(source_contract["development_scope"]["dates"]),
        "symbols": ["SPY"],
    }
    confirmation_scope = {
        "dates": list(source_contract["confirmation_scope"]["dates"]),
        "symbols": ["SPY"],
    }
    predecessor_dates = {
        str(row["signal_date"]) for row in predecessor["records"]
    }
    if (
        predecessor_dates & set(development_scope["dates"])
        or predecessor_dates & set(confirmation_scope["dates"])
    ):
        raise EtfTurnOfMonthDiscoveryError(
            "predecessor outcomes overlap successor evidence"
        )
    index = outcome_exposure.read_index()
    if not outcome_exposure.find_overlaps(development_scope, index):
        raise EtfTurnOfMonthDiscoveryError(
            "development is not explicitly contaminated"
        )
    outcome_exposure.assert_untouched(confirmation_scope, index)
    outcome_exposure.assert_disjoint(
        [development_scope, confirmation_scope]
    )
    evidence_paths = [
        source._repo_path(PREDECESSOR_RESULT),
        source._repo_path(PREDECESSOR_INSPECTION),
        source._repo_path(SOURCE_SEARCH),
        source._repo_path(SOURCE_RESULT),
        source._repo_path(SOURCE_INSPECTION),
        source._repo_path(SOURCE_DATASET_MANIFEST),
        "strategy_tournament/v2/OUTCOME_EXPOSURE_INDEX.jsonl",
    ]
    capacity_path, _capacity = freeze_dataset_contract(
        {
            "schema_version": 1,
            "dataset_id": f"dataset-{SUCCESSOR_ID}-development",
            "registered_at": created_at,
            "requested_dates": development,
            "dataset_payload": {
                "lane": "development",
                "claim_scope": "DEVELOPMENT_ONLY",
                "evidence_paths": evidence_paths,
                "inspected": True,
                "point_in_time_evidence": True,
                "development_training_contaminated": True,
                "confirmation_access_permitted": False,
                "etf_turn_of_month_source": {
                    "source_family_id": runtime.ETF_PULLBACK_FAMILY,
                    "target_family_id": FAMILY_ID,
                    "source_manifest_path": source._repo_path(
                        SOURCE_DATASET_MANIFEST
                    ),
                    "source_manifest_file_sha256": sha256_file(
                        SOURCE_DATASET_MANIFEST
                    ),
                    "external_relative_path": source_binding[
                        "external_relative_path"
                    ],
                    "external_file_sha256": source_binding[
                        "external_file_sha256"
                    ],
                    "dataset_sha256": source_binding["dataset_sha256"],
                    "format": source_binding["format"],
                    "formal_capacity": len(development),
                    "provider_requests": 0,
                },
            },
        },
        root / SUCCESSOR_ID / "capacity",
    )
    contract: dict[str, Any] = {
        "schema_version": 1,
        "campaign_id": CAMPAIGN_ID,
        "experiment_id": f"experiment-{SUCCESSOR_ID}",
        "family_id": FAMILY_ID,
        "mechanism_family": MECHANISM_FAMILY,
        "strategy_id": STRATEGY_ID,
        "parent_experiment_id": PREDECESSOR_VARIANT_ID,
        "created_at": created_at,
        "status": "INVENTED",
        "research_generation": RESEARCH_GENERATION,
        "successor_id": SUCCESSOR_ID,
        "new_mechanism_family_slot_consumed": False,
        "prior_family_attempt_count": 1,
        "predecessor": {
            "variant_id": PREDECESSOR_VARIANT_ID,
            "result_path": source._repo_path(PREDECESSOR_RESULT),
            "result_file_sha256": sha256_file(PREDECESSOR_RESULT),
            "result_sha256": predecessor["result_sha256"],
            "inspection_path": source._repo_path(PREDECESSOR_INSPECTION),
            "inspection_file_sha256": sha256_file(PREDECESSOR_INSPECTION),
            "promotion_evidence_reused": False,
            "adverse_finding": (
                "The exact second-to-last-session SPY v1 rule lost 4.65R "
                "at primary cost across 36 monthly entries."
            ),
        },
        "source_training": {
            "search_path": source._repo_path(SOURCE_SEARCH),
            "search_sha256": source_search["artifact_sha256"],
            "result_path": source._repo_path(SOURCE_RESULT),
            "inspection_path": source._repo_path(SOURCE_INSPECTION),
            "dataset_manifest_path": source._repo_path(
                SOURCE_DATASET_MANIFEST
            ),
            "development_outcomes_exposed": True,
            "promotion_evidence_reused": False,
            "predecessor_date_overlap": 0,
        },
        "mechanism": (
            "Own SPY during a prospectively frozen set of sessions surrounding "
            "the month boundary when the completed long-term trend is positive "
            "and ordinary movement is large enough to clear modeled costs."
        ),
        "expected_holding_behavior": (
            "Long only, next-session-open entry, and flat within four sessions."
        ),
        "dataset_lane": "development",
        "universe_requirements": {
            "symbols": ["SPY"],
            "complete_frozen_daily_history": True,
            "exchange_calendar": "XNYS",
        },
        "entry_rule": (
            "At each completed SPY close in the frozen one- or three-session "
            "window before month end or after month start, require close above "
            "the frozen completed SMA and ATR14/close at least five times "
            "primary round-trip cost; enter at the next session open."
        ),
        "stop_rule": (
            "Place the exact 1.0 or 1.5 ATR14 stop below the next open; invalid "
            "or missing stops and bars produce rejected or missed trades."
        ),
        "exit_rule": (
            "Resolve the stop first on daily ambiguity and otherwise exit at "
            "the completed close after two or four sessions."
        ),
        "ranking_rule": (
            "SPY is the sole frozen instrument; at most one family entry occurs "
            "per session and portfolio contention remains authoritative."
        ),
        "selection_rule": (
            "Portfolio risk, concurrent-position, aggregate-risk, daily-entry, "
            "gross-notional, and capital-contention caps remain authoritative."
        ),
        "parameter_grid": {
            "sessions_before_month_end": [1, 3],
            "sessions_after_month_start": [1, 3],
            "market_trend_sma": [100, 200],
            "stop_atr14": [1.0, 1.5],
            "maximum_hold_sessions": [2, 4],
        },
        "selection_mode": "development_search",
        "winner_selection": DEVELOPMENT_SEARCH_RULE,
        "primary_outcome": (
            "Selection-adjusted chronological account log growth after costs."
        ),
        "execution_assumptions": {
            "next_observable_fill": True,
            "ambiguity": "stop_first",
            "maximum_hold_sessions": 4,
            "missing_data": "missed_trade_no_substitute",
            "minimum_gross_to_primary_round_trip_cost": 5.0,
            "expected_gross_proxy": "completed ATR14 divided by close",
            "exchange_calendar": "XNYS",
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
            "The 2016-2020 partition is already outcome exposed and is training only.",
            "The adverse 2023-2025 predecessor cannot promote this version.",
            "No 2021-2022 confirmation row may enter selection.",
        ],
        "production_compatibility_risks": [
            "The current XNYS calendar, complete SPY history, next-open quote, spread, depth, halt, tradability, timestamp, protection, and reconciliation remain mandatory."
        ],
        "material_difference_rationale": (
            "This version preserves the calendar-seasonality mechanism while "
            "replacing one low-capacity monthly timing choice with a frozen "
            "family of month-boundary windows, trend gates, stops, and holds "
            "on wholly disjoint dates under family-wise controls."
        ),
        "development_dates": development,
        "development_warmup_dates": warmup,
        "confirmation_warmup_dates": list(
            source_contract["confirmation_warmup_dates"]
        ),
        "embargo_dates": embargo,
        "confirmation_dates": confirmation,
        "development_scope": development_scope,
        "confirmation_scope": confirmation_scope,
        "outcome_exposure_index_sha256": outcome_exposure.audit()[
            "index_sha256"
        ],
        "universe": {"symbols": ["SPY"], "point_in_time": True},
        "costs_bps_per_side": [5, 10, 20],
        "partitions": {
            "rolling_origin": True,
            "confirmation_untouched": True,
            "development_training_contaminated": True,
        },
        "falsifiers": [
            "nonpositive stressed log growth",
            "unstable parameter neighbors",
            "selection-aware statistical rejection",
            "incomplete execution or trial accounting",
            "insufficient frozen confirmation power capacity",
        ],
        "implementation_files": [
            "etf_turn_of_month_discovery.py",
            "etf_turn_of_month_plugin.py",
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
            "module": "etf_turn_of_month_plugin",
            "preflight": "preflight",
            "evaluate_development": "evaluate_development",
            "evaluate_confirmation": "evaluate_confirmation",
            "evaluate_production": "evaluate_production",
        },
        "capacity_policy": {"retire_below": 50, "fast_lane_at": 100},
        "capacity_manifest": source._repo_path(capacity_path),
        "dataset_manifest": source._repo_path(capacity_path),
    }
    validated = strategy_discovery._validate_family_contract(contract)
    digest = hashlib.sha256(source._canonical(validated)).hexdigest()
    path = (
        root
        / SUCCESSOR_ID
        / "family-contract"
        / f"contract-{digest}.json"
    )
    source._write_json(path, validated)
    return path, validated, capacity_path


def status(*, root: Path = DEFAULT_ROOT) -> dict[str, Any]:
    contracts = sorted(
        (root / SUCCESSOR_ID / "family-contract").glob("contract-*.json")
    )
    discovery_root = strategy_discovery.DEFAULT_ROOT / FAMILY_ID
    return {
        "schema_version": 1,
        "campaign_id": CAMPAIGN_ID,
        "successor_id": SUCCESSOR_ID,
        "family_id": FAMILY_ID,
        "mechanism_family": MECHANISM_FAMILY,
        "calendar_wait_required": False,
        "new_mechanism_family_slot_consumed": False,
        "development_training_contaminated": True,
        "confirmation_outcomes_accessed": False,
        "family_contracts": len(contracts),
        "state": (
            "READY_TO_FREEZE"
            if not contracts
            else "DISCOVERY_ACTIVE"
            if discovery_root.exists()
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


def main() -> int:
    args = _parser().parse_args()
    try:
        if args.command == "status":
            result = status()
        else:
            path, contract, capacity = freeze_successor_contract(
                created_at=args.created_at
            )
            result = {
                "path": source._repo_path(path),
                "capacity_manifest": source._repo_path(capacity),
                "state": contract["status"],
                "trial_count": len(contract["trial_family"]),
                "calendar_wait_required": False,
            }
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    except (
        OSError,
        EtfTurnOfMonthDiscoveryError,
        outcome_exposure.OutcomeExposureError,
        strategy_discovery.StrategyDiscoveryError,
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

"""Freeze a selection-aware close-to-open ETF momentum successor."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any

import close_to_open_etf_momentum_stage0 as source
import dense_strategy_runtime as runtime
import etf_cross_sectional_momentum_discovery as shared
import etf_or_momentum_stage0 as source_hash
import outcome_exposure
import portfolio_maturity
import strategy_discovery
from historical_store import HistoricalDayStore, sha256_file
from learning_data import freeze_dataset_contract
from learning_experiment import DEVELOPMENT_SEARCH_RULE


PROJECT_ROOT = Path(__file__).resolve().parent
CAMPAIGN_ID = portfolio_maturity.V2_CAMPAIGN_ID
FAMILY_ID = runtime.ETF_CLOSE_TO_OPEN_FAMILY
MECHANISM_FAMILY = "close-to-open-etf-momentum"
STRATEGY_ID = MECHANISM_FAMILY
SUCCESSOR_ID = "close-to-open-etf-momentum-v2-liquid-index-etf"
RESEARCH_GENERATION = "existing_family_successor"
DEFAULT_ROOT = PROJECT_ROOT / "strategy_tournament/v2/continuous"

PREDECESSOR_VARIANT_ID = source.VARIANT_ID
SOURCE_ACTIVATION = (
    PROJECT_ROOT
    / "strategy_tournament/second_wave/activations/"
    "close-to-open-etf-momentum-v1-"
    "1e7934f023b26213e085277cb18bb343ddc6f4655aa9cdc8a9bc0096cc95a9c6.json"
)
SOURCE_INPUT_INSPECTION = (
    PROJECT_ROOT
    / "strategy_tournament/second_wave/inspections/"
    "close-to-open-etf-momentum-v1-input-"
    "650887a1b75eb05a9588928452c9a1e687d384c66089184ff2273eca60b83b07.json"
)
PREDECESSOR_RESULT = (
    PROJECT_ROOT
    / "research_results/2026-07-21-close-to-open-etf-momentum-stage0-"
    "a4c68fb9d99fcb2775caeaf0598fabb2999c9b5f0d6ce6ad275ff21174470ae8.json"
)
PREDECESSOR_INSPECTION = (
    PROJECT_ROOT
    / "strategy_tournament/second_wave/inspections/"
    "close-to-open-etf-momentum-v1-result-"
    "60ebceee9881f934ea1921d0d3d445a0910209e3b4653c340b6a9af9b61d9ed3.json"
)


class CloseToOpenEtfDiscoveryError(RuntimeError):
    """The source graph, partition, or successor contract is invalid."""


def _source_graph(
    *, enforce_commit: bool
) -> tuple[dict[str, Any], dict[str, Any]]:
    paths = (
        SOURCE_ACTIVATION,
        SOURCE_INPUT_INSPECTION,
        PREDECESSOR_RESULT,
        PREDECESSOR_INSPECTION,
    )
    if enforce_commit:
        for path in paths:
            strategy_discovery.require_committed(path)
    activation = shared._read(SOURCE_ACTIVATION)
    input_inspection = shared._read(SOURCE_INPUT_INSPECTION)
    result = shared._read(PREDECESSOR_RESULT)
    result_inspection = shared._read(PREDECESSOR_INSPECTION)
    file_sha256 = sha256_file(source.PRIVATE_INPUT_PATH)
    if not (
        activation.get("manifest_sha256")
        == source_hash._self_hash(activation, "manifest_sha256")
        and activation.get("variant_id") == PREDECESSOR_VARIANT_ID
        and activation.get("mechanism_family") == MECHANISM_FAMILY
        and activation.get("claim_scope") == "FALSIFICATION_ONLY"
        and activation.get("development_evidence_eligible") is False
        and activation.get("confirmation_evidence_eligible") is False
        and activation.get("source_selection", {}).get(
            "private_input_file_sha256"
        )
        == file_sha256
        and input_inspection.get("inspection_sha256")
        == source_hash._self_hash(
            input_inspection, "inspection_sha256"
        )
        and input_inspection.get("manifest_sha256")
        == activation["manifest_sha256"]
        and input_inspection.get("input_sha256")
        == activation["source_selection"]["input_sha256"]
        and input_inspection.get("returns_computed") == 0
        and input_inspection.get("valid") is True
        and result.get("result_sha256")
        == source_hash._self_hash(result, "result_sha256")
        and result.get("variant_id") == PREDECESSOR_VARIANT_ID
        and result.get("stage0_survived") is False
        and result.get("development_evidence_eligible") is False
        and result.get("confirmation_evidence_eligible") is False
        and result_inspection.get("inspection_sha256")
        == source_hash._self_hash(
            result_inspection, "inspection_sha256"
        )
        and result_inspection.get("result_sha256")
        == result["result_sha256"]
        and result_inspection.get("valid") is True
        and result_inspection.get("stage0_survived") is False
    ):
        raise CloseToOpenEtfDiscoveryError(
            "bound close-to-open source graph is invalid"
        )
    return activation, result


def _outcome_blind_dates() -> list[str]:
    store = HistoricalDayStore.from_env()
    common = set(store.dates(source.SYMBOLS[0]))
    for symbol in source.SYMBOLS[1:]:
        common &= set(store.dates(symbol))
    dates = [
        day
        for day in sorted(common)
        if "2022-01-03" <= day <= "2022-12-30"
    ]
    if (
        len(dates) != 251
        or dates[0] != "2022-01-03"
        or dates[-1] != "2022-12-30"
    ):
        raise CloseToOpenEtfDiscoveryError(
            "outcome-blind 2022 ETF calendar capacity drifted"
        )
    return dates


def freeze_successor_contract(
    *,
    created_at: str,
    root: Path = DEFAULT_ROOT,
    enforce_commit: bool = True,
) -> tuple[Path, dict[str, Any], Path]:
    """Freeze 32 trials and exact disjoint evidence without opening price rows."""

    shared._timestamp(created_at)
    activation, predecessor = _source_graph(
        enforce_commit=enforce_commit
    )
    dates = _outcome_blind_dates()
    warmup = dates[:60]
    development = dates[60:180]
    embargo = dates[180:185]
    confirmation = dates[185:]
    confirmation_warmup = dates[125:185]
    if not (
        len(warmup) == 60
        and len(development) == 120
        and len(embargo) == 5
        and len(confirmation) == 66
        and development[0] == "2022-03-30"
        and development[-1] == "2022-09-20"
        and confirmation[0] == "2022-09-28"
    ):
        raise CloseToOpenEtfDiscoveryError(
            "close-to-open evidence partition drifted"
        )
    scope_symbols = sorted(source.SYMBOLS)
    development_scope = {
        "dates": development,
        "symbols": scope_symbols,
    }
    confirmation_scope = {
        "dates": confirmation,
        "symbols": scope_symbols,
    }
    index = outcome_exposure.read_index()
    outcome_exposure.assert_untouched(development_scope, index)
    outcome_exposure.assert_untouched(confirmation_scope, index)
    outcome_exposure.assert_disjoint(
        [development_scope, confirmation_scope]
    )
    predecessor_dates = {
        str(row["signal_date"])
        for row in predecessor.get("records", [])
    }
    if predecessor_dates & set(development + confirmation):
        raise CloseToOpenEtfDiscoveryError(
            "predecessor outcomes overlap successor evidence"
        )
    evidence_paths = [
        shared._repo_path(SOURCE_ACTIVATION),
        shared._repo_path(SOURCE_INPUT_INSPECTION),
        shared._repo_path(PREDECESSOR_RESULT),
        shared._repo_path(PREDECESSOR_INSPECTION),
        "strategy_tournament/v2/OUTCOME_EXPOSURE_INDEX.jsonl",
    ]
    external_relative = str(
        source.PRIVATE_INPUT_PATH.relative_to(source.DATA_ROOT)
    )
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
                "development_training_contaminated": False,
                "confirmation_access_permitted": False,
                "close_to_open_runtime": {
                    "source_variant_id": PREDECESSOR_VARIANT_ID,
                    "target_family_id": FAMILY_ID,
                    "external_relative_path": external_relative,
                    "external_file_sha256": sha256_file(
                        source.PRIVATE_INPUT_PATH
                    ),
                    "input_sha256": activation["source_selection"][
                        "input_sha256"
                    ],
                    "format": "json.gz",
                    "sample_phase": "development",
                    "symbols": list(source.SYMBOLS),
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
            "result_path": shared._repo_path(PREDECESSOR_RESULT),
            "result_file_sha256": sha256_file(PREDECESSOR_RESULT),
            "result_sha256": predecessor["result_sha256"],
            "inspection_path": shared._repo_path(PREDECESSOR_INSPECTION),
            "inspection_file_sha256": sha256_file(
                PREDECESSOR_INSPECTION
            ),
            "promotion_evidence_reused": False,
            "adverse_finding": (
                "The exact 2023-2025 v1 rule had -0.0185R primary "
                "expectancy, 0.892 profit factor, and negative stress."
            ),
        },
        "source_training": {
            "activation_path": shared._repo_path(SOURCE_ACTIVATION),
            "input_inspection_path": shared._repo_path(
                SOURCE_INPUT_INSPECTION
            ),
            "external_input_file_sha256": sha256_file(
                source.PRIVATE_INPUT_PATH
            ),
            "input_sha256": activation["source_selection"]["input_sha256"],
            "development_outcomes_exposed": False,
            "promotion_evidence_reused": False,
            "predecessor_date_overlap": 0,
        },
        "mechanism": (
            "Own the strongest liquid index ETF late-session continuation "
            "from a completed afternoon bar into the next opening window."
        ),
        "expected_holding_behavior": (
            "Long only, next-observable 15-minute-open entry, and flat by "
            "the next session's 09:45 close."
        ),
        "dataset_lane": "development",
        "universe_requirements": {
            "symbols": list(source.SYMBOLS),
            "complete_frozen_daily_history": True,
            "complete_regular_session_15_minute_bars": 26,
            "point_in_time": True,
            "security_type": "ETF",
        },
        "entry_rule": (
            "At the frozen completed 15:15 or 15:30 bar, require the ETF's "
            "regular-session return to clear the frozen floor and five-times-"
            "cost gate and its close to exceed the prior completed SMA; rank "
            "all four ETFs and enter the leader at the next 15-minute open."
        ),
        "stop_rule": (
            "Place the exact 0.5 or 1.0 prior-session ATR14 stop below entry; "
            "invalid stops or missing bars produce rejected or missed trades."
        ),
        "exit_rule": (
            "Resolve the stop first through the entry session and overnight "
            "gap, then exit at the next open or next 09:45 close."
        ),
        "ranking_rule": (
            "Rank by highest completed session return, tie-break lexically, "
            "and admit no more than one new family entry per session."
        ),
        "selection_rule": (
            "Portfolio risk, concurrent-position, aggregate-risk, daily-entry, "
            "gross-notional, and capital-contention caps remain authoritative."
        ),
        "parameter_grid": {
            "decision_bar_time": ["15:15", "15:30"],
            "minimum_session_return_fraction": [0.005, 0.01],
            "prior_trend_sma": [20, 60],
            "stop_atr14": [0.5, 1.0],
            "exit_timing": ["next_open", "next_0945_close"],
        },
        "selection_mode": "development_search",
        "winner_selection": DEVELOPMENT_SEARCH_RULE,
        "primary_outcome": (
            "Selection-adjusted chronological account log growth after costs."
        ),
        "execution_assumptions": {
            "next_observable_fill": True,
            "entry_bar_minutes": 15,
            "ambiguity": "stop_first",
            "maximum_hold_sessions": 1,
            "overnight_protection": "GTC",
            "missing_data": "missed_trade_no_substitute",
            "minimum_gross_to_primary_round_trip_cost": 5.0,
            "expected_gross_proxy": "completed regular-session return",
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
            "The 2023-2025 predecessor outcomes are adverse history only.",
            "The 2022 development partition becomes training after evaluation.",
            "No 2022 confirmation row may enter trial selection.",
        ],
        "production_compatibility_risks": [
            "Complete current daily and 15-minute history, fresh quotes, spread, depth, halt, tradability, GTC protection, timestamps, and reconciliation remain mandatory."
        ],
        "material_difference_rationale": (
            "This successor preserves late-session-to-open continuation while "
            "prospectively testing frozen decision bars, return floors, trend "
            "gates, stops, and opening exits on dates disjoint from v1."
        ),
        "development_dates": development,
        "development_warmup_dates": warmup,
        "confirmation_warmup_dates": confirmation_warmup,
        "embargo_dates": embargo,
        "confirmation_dates": confirmation,
        "development_scope": development_scope,
        "confirmation_scope": confirmation_scope,
        "outcome_exposure_index_sha256": outcome_exposure.audit()[
            "index_sha256"
        ],
        "universe": {
            "symbols": list(source.SYMBOLS),
            "point_in_time": True,
        },
        "costs_bps_per_side": [5, 10, 20],
        "partitions": {
            "rolling_origin": True,
            "confirmation_untouched": True,
            "development_training_contaminated": False,
        },
        "falsifiers": [
            "nonpositive stressed log growth",
            "stressed profit-factor or drawdown failure",
            "unstable parameter neighbors",
            "selection-aware statistical rejection",
            "incomplete execution or trial accounting",
            "insufficient frozen confirmation power capacity",
        ],
        "implementation_files": [
            "close_to_open_etf_discovery.py",
            "close_to_open_etf_plugin.py",
            "close_to_open_etf_momentum_stage0.py",
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
            "module": "close_to_open_etf_plugin",
            "preflight": "preflight",
            "evaluate_development": "evaluate_development",
            "evaluate_confirmation": "evaluate_confirmation",
            "evaluate_production": "evaluate_production",
        },
        "capacity_policy": {"retire_below": 50, "fast_lane_at": 100},
        "capacity_manifest": shared._repo_path(capacity_path),
        "dataset_manifest": shared._repo_path(capacity_path),
    }
    validated = strategy_discovery._validate_family_contract(contract)
    digest = hashlib.sha256(shared._canonical(validated)).hexdigest()
    path = (
        root
        / SUCCESSOR_ID
        / "family-contract"
        / f"contract-{digest}.json"
    )
    shared._write_json(path, validated)
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
                "path": shared._repo_path(path),
                "capacity_manifest": shared._repo_path(capacity),
                "state": contract["status"],
                "trial_count": len(contract["trial_family"]),
                "calendar_wait_required": False,
            }
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    except (
        OSError,
        CloseToOpenEtfDiscoveryError,
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

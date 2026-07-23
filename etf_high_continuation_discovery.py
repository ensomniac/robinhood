"""Freeze an existing-family liquid-ETF 52-week-high continuation successor."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any

import dense_strategy_runtime as runtime
import etf_cross_sectional_reversal_discovery as shared
import outcome_exposure
import portfolio_maturity
import strategy_discovery
from historical_store import sha256_file
from learning_data import freeze_dataset_contract, load_frozen_dataset_contract
from learning_experiment import DEVELOPMENT_SEARCH_RULE


PROJECT_ROOT = Path(__file__).resolve().parent
CAMPAIGN_ID = portfolio_maturity.V2_CAMPAIGN_ID
FAMILY_ID = runtime.ETF_HIGH_CONTINUATION_FAMILY
MECHANISM_FAMILY = "five-day-52-week-high-continuation"
STRATEGY_ID = MECHANISM_FAMILY
SUCCESSOR_ID = "five-day-52-week-high-continuation-v2-liquid-index-etf"
RESEARCH_GENERATION = "existing_family_successor"
DEFAULT_ROOT = PROJECT_ROOT / "strategy_tournament/v2/continuous"

PREDECESSOR_VARIANT_ID = "five-day-52-week-high-continuation-v1"
PREDECESSOR_RESULT = (
    PROJECT_ROOT
    / "research_results/2026-07-21-five-day-52-week-high-continuation-stage0-"
    "2b44cf24c18baba5a388a6dd8bb8ed62f67fcdfe8f012965586429a13ad86fbc.json"
)
PREDECESSOR_INSPECTION = (
    PROJECT_ROOT
    / "strategy_tournament/second_wave/inspections/"
    "five-day-52-week-high-continuation-v1-result-"
    "7083067c990c221d01d47782882e8eac5a8d652f228e94894d3e5c75cc2967d2.json"
)
SOURCE_SEARCH = shared.SOURCE_SEARCH
SOURCE_RESULT = shared.SOURCE_RESULT
SOURCE_INSPECTION = shared.SOURCE_INSPECTION
SOURCE_DATASET_MANIFEST = shared.SOURCE_DATASET_MANIFEST


class EtfHighContinuationDiscoveryError(RuntimeError):
    """The successor source graph or evidence boundary is invalid."""


def _source_graph(*, enforce_commit: bool) -> tuple[dict[str, Any], dict[str, Any]]:
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
    predecessor = shared._read(PREDECESSOR_RESULT)
    predecessor_inspection = shared._read(PREDECESSOR_INSPECTION)
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
        and predecessor.get("market_outcomes_accessed") is False
        and predecessor.get("returns_computed") == 0
        and predecessor.get("structural_falsification", {}).get(
            "stage0_capacity_possible"
        )
        is False
        and predecessor_inspection.get("result_sha256")
        == predecessor.get("result_sha256")
        and predecessor_inspection.get("valid") is True
        and predecessor_inspection.get("market_outcomes_accessed") is False
        and search.get("artifact_sha256") == result.get("search_sha256")
        and result.get("artifact_sha256") == inspection.get("result_sha256")
        and inspection.get("state") == "REJECTED"
        and inspection.get("inspection", {}).get("valid") is True
        and result["evaluation"].get("dataset_manifest")
        == str(SOURCE_DATASET_MANIFEST)
    ):
        raise EtfHighContinuationDiscoveryError(
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
        raise EtfHighContinuationDiscoveryError(
            "source ETF partitions or universe drifted"
        )
    return predecessor, search


def freeze_successor_contract(
    *,
    created_at: str,
    root: Path = DEFAULT_ROOT,
    enforce_commit: bool = True,
) -> tuple[Path, dict[str, Any], Path]:
    """Freeze the full 32-trial family without opening external price rows."""

    shared._timestamp(created_at)
    predecessor, source_search = _source_graph(enforce_commit=enforce_commit)
    source_contract = source_search["family_contract"]
    source_manifest = load_frozen_dataset_contract(SOURCE_DATASET_MANIFEST)
    source_binding = source_manifest["dataset_payload"]["dense_runtime"]
    development = list(source_contract["development_dates"])
    warmup = list(source_contract["development_warmup_dates"])
    embargo = list(source_contract["embargo_dates"])
    confirmation = list(source_contract["confirmation_dates"])
    development_scope = dict(source_contract["development_scope"])
    confirmation_scope = dict(source_contract["confirmation_scope"])
    index = outcome_exposure.read_index()
    if not outcome_exposure.find_overlaps(development_scope, index):
        raise EtfHighContinuationDiscoveryError(
            "development is not explicitly contaminated"
        )
    outcome_exposure.assert_untouched(confirmation_scope, index)
    outcome_exposure.assert_disjoint([development_scope, confirmation_scope])
    evidence_paths = [
        shared._repo_path(PREDECESSOR_RESULT),
        shared._repo_path(PREDECESSOR_INSPECTION),
        shared._repo_path(SOURCE_SEARCH),
        shared._repo_path(SOURCE_RESULT),
        shared._repo_path(SOURCE_INSPECTION),
        shared._repo_path(SOURCE_DATASET_MANIFEST),
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
                "etf_high_continuation_source": {
                    "source_family_id": runtime.ETF_PULLBACK_FAMILY,
                    "target_family_id": FAMILY_ID,
                    "source_manifest_path": shared._repo_path(
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
                    "formal_capacity": len(development) * 4,
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
            "inspection_file_sha256": sha256_file(PREDECESSOR_INSPECTION),
            "promotion_evidence_reused": False,
            "adverse_finding": (
                "capacity-only retirement: 24 possible signals versus a "
                "30-signal Stage 0 floor; no market outcomes were accessed"
            ),
        },
        "source_training": {
            "search_path": shared._repo_path(SOURCE_SEARCH),
            "search_sha256": source_search["artifact_sha256"],
            "result_path": shared._repo_path(SOURCE_RESULT),
            "inspection_path": shared._repo_path(SOURCE_INSPECTION),
            "dataset_manifest_path": shared._repo_path(SOURCE_DATASET_MANIFEST),
            "development_outcomes_exposed": True,
            "promotion_evidence_reused": False,
        },
        "mechanism": (
            "Buy the strongest liquid index ETF after a cost-clearing five-session "
            "advance at or near its prior 252-session high while SPY remains in a "
            "completed long-term uptrend."
        ),
        "expected_holding_behavior": (
            "Long only, next-session-open entry, and flat within five sessions."
        ),
        "dataset_lane": "development",
        "universe_requirements": {
            "symbols": ["DIA", "IWM", "QQQ", "SPY"],
            "complete_frozen_daily_history": True,
        },
        "entry_rule": (
            "At each completed close, require a 2% or 4% five-session advance, "
            "a close at least 98% or 100% of the prior 252-session high, and the "
            "frozen SPY trend regime; rank by highest advance and enter at the "
            "next open."
        ),
        "stop_rule": (
            "Place the exact 1.0 or 1.5 ATR14 structural stop below entry; invalid "
            "or missing stops produce missed trades."
        ),
        "exit_rule": (
            "Resolve stop first on daily ambiguity and otherwise exit at the "
            "completed close after three or five sessions."
        ),
        "ranking_rule": (
            "Highest completed five-session return, then lexical symbol; at most "
            "one new family entry per session."
        ),
        "selection_rule": (
            "Portfolio risk, concurrent-position, aggregate-risk, daily-entry, "
            "gross-notional, and capital-contention caps remain authoritative."
        ),
        "parameter_grid": {
            "minimum_five_session_return_fraction": [0.02, 0.04],
            "minimum_close_to_prior_high_fraction": [0.98, 1.0],
            "market_trend_sma": [100, 200],
            "stop_atr14": [1.0, 1.5],
            "maximum_hold_sessions": [3, 5],
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
            "shared_signal_contention": (
                "the 20-bps path freezes the conservative filled-signal set "
                "used unchanged by all cost scenarios"
            ),
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
            "The 2016-2020 ETF partition is already outcome exposed and is training only.",
            "The predecessor was capacity-falsified without market outcomes.",
            "No confirmation date or symbol may enter selection.",
        ],
        "production_compatibility_risks": [
            "Live quote, spread, depth, halt, tradability, timestamp, protection, and reconciliation gates remain required."
        ],
        "material_difference_rationale": (
            "This exact version retains the authorized five-day 52-week-high "
            "continuation mechanism while replacing a 24-date capacity-defective "
            "equity screen with a complete liquid index-ETF denominator and "
            "prospectively frozen selection-aware grid on wholly disjoint dates."
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
        "outcome_exposure_index_sha256": outcome_exposure.audit()["index_sha256"],
        "universe": {
            "symbols": ["DIA", "IWM", "QQQ", "SPY"],
            "point_in_time": True,
        },
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
            "etf_high_continuation_discovery.py",
            "etf_high_continuation_plugin.py",
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
            "module": "etf_high_continuation_plugin",
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
    path = root / SUCCESSOR_ID / "family-contract" / f"contract-{digest}.json"
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
        "contracts": len(contracts),
        "discovery_started": discovery_root.exists(),
        "confirmation_outcomes_accessed": False,
        "broker_actions_permitted": False,
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("status")
    freeze = sub.add_parser("freeze")
    freeze.add_argument("--created-at", required=True)
    return parser


def main() -> int:
    args = _parser().parse_args()
    try:
        if args.command == "status":
            result = status(root=args.root)
        else:
            path, artifact, capacity = freeze_successor_contract(
                created_at=args.created_at,
                root=args.root,
            )
            result = {
                "path": shared._repo_path(path),
                "capacity_manifest": shared._repo_path(capacity),
                "state": artifact["status"],
                "trial_count": len(artifact["trial_family"]),
                "calendar_wait_required": False,
            }
    except (
        EtfHighContinuationDiscoveryError,
        strategy_discovery.StrategyDiscoveryError,
    ) as exc:
        print(json.dumps({"error": str(exc)}, indent=2, sort_keys=True))
        return 1
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

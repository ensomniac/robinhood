"""Freeze one dense daily style-ETF closing-breakout continuation rule."""

from __future__ import annotations

import argparse
import hashlib
import json
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import cross_style_breadth_discovery as shared
import dense_strategy_runtime as runtime
import outcome_exposure
import portfolio_maturity
import sector_etf_gap_drift as artifact_support
import strategy_discovery
from historical_store import sha256_file
from learning_data import freeze_dataset_contract
from learning_experiment import DEVELOPMENT_SEARCH_RULE


PROJECT_ROOT = Path(__file__).resolve().parent
CAMPAIGN_ID = portfolio_maturity.V2_CAMPAIGN_ID
FAMILY_ID = runtime.STYLE_ETF_BREAKOUT_CONTINUATION_FAMILY
MECHANISM_FAMILY = "style-etf-short-horizon-breakout-continuation"
STRATEGY_ID = "style-etf-breakout-continuation"
SUCCESSOR_ID = "style-etf-20-day-breakout-continuation-v1"
RESEARCH_GENERATION = "new_mechanism_family"
DEFAULT_ROOT = PROJECT_ROOT / "strategy_tournament/v2/continuous"
CALENDAR_PATH = shared.CALENDAR_PATH
CALENDAR_INSPECTION = shared.CALENDAR_INSPECTION
SYMBOLS = list(runtime.CROSS_STYLE_BREADTH_SYMBOLS)
WARMUP_SESSIONS = 200
EMBARGO_SESSIONS = 5
MAXIMUM_HOLD_SESSIONS = 5
PREDECESSOR_INSPECTION = (
    PROJECT_ROOT
    / "strategy_tournament/v2/discovery/"
    "cross-style-etf-breadth-continuation/development-inspection/"
    "cross-style-etf-breadth-continuation-development-inspection-"
    "2a845545f2c0b7797af2065220607e51c0434dd51814040adfebecf69c6ede24.json"
)


class StyleEtfBreakoutDiscoveryError(ValueError):
    """The fixed breakout family or its untouched boundary drifted."""


def _repo_path(path: Path) -> str:
    return artifact_support._repo_path(path)


def _partitions() -> tuple[
    list[str],
    list[str],
    list[str],
    list[str],
    list[str],
    list[str],
    list[str],
]:
    full = shared._full_sessions()
    development = [
        day for day in full if "2019-01-02" <= day <= "2019-12-31"
    ]
    year_2020 = [
        day for day in full if "2020-01-02" <= day <= "2020-12-31"
    ]
    if len(development) != 249 or len(year_2020) != 251:
        raise StyleEtfBreakoutDiscoveryError(
            "frozen 2019-2020 partition capacity drifted"
        )
    development_start = full.index(development[0])
    warmup = full[
        development_start - WARMUP_SESSIONS : development_start
    ]
    embargo = year_2020[:EMBARGO_SESSIONS]
    confirmation = year_2020[EMBARGO_SESSIONS:]
    confirmation_start = full.index(confirmation[0])
    confirmation_warmup = full[
        confirmation_start - WARMUP_SESSIONS : confirmation_start
    ]
    return (
        warmup,
        development,
        development[:-MAXIMUM_HOLD_SESSIONS],
        embargo,
        confirmation_warmup,
        confirmation,
        confirmation[:-MAXIMUM_HOLD_SESSIONS],
    )


def _scope(dates: Sequence[str]) -> dict[str, Any]:
    return {"dates": list(dates), "symbols": list(SYMBOLS)}


def _predecessor_disposition(*, enforce_commit: bool) -> dict[str, Any]:
    if enforce_commit:
        strategy_discovery.require_committed(PREDECESSOR_INSPECTION)
    inspection = strategy_discovery.load_artifact(
        PREDECESSOR_INSPECTION,
        expected_kind="development-search-inspection",
    )
    if not (
        inspection.get("family_id")
        == runtime.CROSS_STYLE_BREADTH_CONTINUATION_FAMILY
        and inspection.get("state") == "REJECTED"
        and inspection.get("confirmation_access_permitted") is False
        and inspection.get("selection", {}).get("selected_trial_id") is None
        and inspection.get("inspection", {}).get("valid") is True
    ):
        raise StyleEtfBreakoutDiscoveryError(
            "predecessor rolling slot is not terminal"
        )
    return {
        "inspection_path": _repo_path(PREDECESSOR_INSPECTION),
        "inspection_file_sha256": sha256_file(PREDECESSOR_INSPECTION),
        "inspection_sha256": inspection["artifact_sha256"],
        "state": inspection["state"],
        "slot_released": True,
        "promotion_evidence_reused": False,
    }


def _validate_exposure_state(contract: Mapping[str, Any]) -> None:
    records = outcome_exposure.read_index()
    outcome_exposure.assert_untouched(
        contract["development_scope"], records
    )
    outcome_exposure.assert_untouched(
        contract["confirmation_scope"], records
    )
    outcome_exposure.assert_disjoint(
        [contract["development_scope"], contract["confirmation_scope"]]
    )


def freeze_contract(
    *, created_at: str, enforce_commit: bool = True
) -> tuple[Path, dict[str, Any], Path]:
    shared._timestamp(created_at, "created_at")
    if enforce_commit:
        strategy_discovery.require_committed(Path(__file__).resolve())
    rolling_slot = shared._rolling_slot_authority(
        enforce_commit=enforce_commit
    )
    predecessor = _predecessor_disposition(
        enforce_commit=enforce_commit
    )
    calendar_inspection = shared._calendar_authority(
        enforce_commit=enforce_commit
    )
    (
        warmup,
        development,
        development_signals,
        embargo,
        confirmation_warmup,
        confirmation,
        confirmation_signals,
    ) = _partitions()
    development_scope = _scope(development)
    confirmation_scope = _scope(confirmation)
    _validate_exposure_state(
        {
            "development_scope": development_scope,
            "confirmation_scope": confirmation_scope,
        }
    )
    evidence_paths = [
        _repo_path(CALENDAR_PATH),
        _repo_path(CALENDAR_INSPECTION),
        _repo_path(PREDECESSOR_INSPECTION),
        "strategy_tournament/v2/OUTCOME_EXPOSURE_INDEX.jsonl",
        "PORTFOLIO_VALIDATION_V2.md",
        "PORTFOLIO_THESIS_V2.md",
    ]
    capacity_path, _capacity = freeze_dataset_contract(
        {
            "schema_version": 1,
            "dataset_id": f"dataset-{SUCCESSOR_ID}-capacity",
            "registered_at": created_at,
            "requested_dates": sorted(
                {
                    *warmup,
                    *development,
                    *embargo,
                    *confirmation_warmup,
                    *confirmation,
                }
            ),
            "dataset_payload": {
                "lane": "development",
                "claim_scope": "DEVELOPMENT_ONLY",
                "evidence_paths": evidence_paths,
                "inspected": True,
                "point_in_time_evidence": True,
                "dense_capacity": {
                    "family_id": FAMILY_ID,
                    "mechanism_family": MECHANISM_FAMILY,
                    "formal_capacity": len(development_signals),
                    "capacity_unit": (
                        "frozen daily development decision dates"
                    ),
                    "development_sessions": len(development),
                    "development_signal_dates": len(development_signals),
                    "embargo_sessions": len(embargo),
                    "confirmation_sessions": len(confirmation),
                    "confirmation_signal_dates": len(
                        confirmation_signals
                    ),
                    "calendar_sha256": sha256_file(CALENDAR_PATH),
                    "provider_requests": 0,
                    "market_prices_accessed": False,
                    "outcomes_accessed": False,
                },
            },
        },
        DEFAULT_ROOT / SUCCESSOR_ID / "capacity",
    )
    contract: dict[str, Any] = {
        "schema_version": 1,
        "campaign_id": CAMPAIGN_ID,
        "experiment_id": f"experiment-{SUCCESSOR_ID}",
        "family_id": FAMILY_ID,
        "mechanism_family": MECHANISM_FAMILY,
        "strategy_id": STRATEGY_ID,
        "created_at": created_at,
        "status": "INVENTED",
        "research_generation": RESEARCH_GENERATION,
        "successor_id": SUCCESSOR_ID,
        "new_mechanism_family_slot_consumed": True,
        "rolling_active_family_slot": 1,
        "rolling_slot_authority": rolling_slot,
        "predecessor_terminal_disposition": predecessor,
        "prior_family_attempt_count": 0,
        "mechanism": (
            "A completed 20-session closing breakout can persist for several "
            "sessions when at least six of eight growth, value, large-cap, "
            "and small-cap style ETFs remain above completed SMA100."
        ),
        "expected_holding_behavior": (
            "Long only, rank one cross-style leader per day, enter at the "
            "next observable session open, and exit within five sessions."
        ),
        "dataset_lane": "development",
        "universe_requirements": {
            "symbols": list(SYMBOLS),
            "complete_frozen_daily_history": True,
            "selection_basis": (
                "The same eight liquid style ETFs provide comparable "
                "point-in-time trend and breakout features."
            ),
        },
        "entry_rule": (
            "After a completed session with at least six of eight ETFs above "
            "SMA100, rank ETFs that close strictly above both SMA200 and every "
            "prior 20-session close by trailing 20-session return; enter only "
            "canonical rank one at the next session open."
        ),
        "stop_rule": (
            "Use 1.5 completed ATR14 below entry; missing data or an invalid "
            "structural stop produces a missed trade without substitution."
        ),
        "exit_rule": (
            "Resolve stop first on daily ambiguity and otherwise exit at the "
            "fifth completed holding-session close."
        ),
        "ranking_rule": (
            "Highest completed 20-session return wins; canonical symbol "
            "breaks an exact tie."
        ),
        "selection_rule": (
            "One frozen rule and at most one new family entry per session "
            "under all portfolio risk and capital-contention caps."
        ),
        "parameter_grid": {
            "breadth_sma": [100],
            "minimum_breadth_count": [6],
            "breakout_lookback_sessions": [20],
            "trend_sma": [200],
            "stop_atr14": [1.5],
            "maximum_hold_sessions": [5],
        },
        "selection_mode": "development_search",
        "winner_selection": DEVELOPMENT_SEARCH_RULE,
        "primary_outcome": (
            "Selection-adjusted chronological account log growth after costs."
        ),
        "execution_assumptions": {
            "long_only": True,
            "next_observable_fill": "next_session_open",
            "ambiguity": "stop_first",
            "maximum_hold_sessions": MAXIMUM_HOLD_SESSIONS,
            "missing_data": "missed_trade_no_substitute",
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
            "All 2019-2020 target date-symbol pairs are globally untouched at freeze.",
            "The 2018 warmup is explicitly feature-only and supplies no promotion return.",
            "The basket, sole rule, ranking, and partitions freeze before provider access.",
            "Confirmation remains inaccessible until an inspected development survivor is frozen.",
        ],
        "production_compatibility_risks": [
            "Fresh quote, spread, depth, halt, tradability, timestamp, GTC protection, and before-open reconciliation remain mandatory."
        ],
        "material_difference_rationale": (
            "The rejected predecessor bought fixed SCHG only on weekly breadth. "
            "This new mechanism requires a daily closing-price breakout and "
            "selects the strongest eligible style leader; no predecessor "
            "performance evidence is reused."
        ),
        "related_adverse_history": {
            "family_id": (
                runtime.CROSS_STYLE_BREADTH_CONTINUATION_FAMILY
            ),
            "inspection_sha256": predecessor["inspection_sha256"],
            "promotion_evidence_reused": False,
        },
        "development_dates": development,
        "development_signal_dates": development_signals,
        "development_warmup_dates": warmup,
        "embargo_dates": embargo,
        "confirmation_warmup_dates": confirmation_warmup,
        "confirmation_dates": confirmation,
        "confirmation_signal_dates": confirmation_signals,
        "confirmation_signal_capacity": len(confirmation_signals),
        "development_scope": development_scope,
        "confirmation_scope": confirmation_scope,
        "outcome_exposure_index_sha256": outcome_exposure.audit()[
            "index_sha256"
        ],
        "calendar_path": _repo_path(CALENDAR_PATH),
        "calendar_inspection_path": _repo_path(CALENDAR_INSPECTION),
        "calendar_inspection_sha256": calendar_inspection[
            "artifact_sha256"
        ],
        "universe": {
            "symbols": list(SYMBOLS),
            "point_in_time": True,
        },
        "historical_data_contract": {
            "daily_provider": "alpaca",
            "daily_endpoint": "/v2/stocks/{symbol}/bars",
            "daily_feed": "sip",
            "daily_adjustment": "raw",
            "daily_request_mode": "symbol_range",
            "split_provider": "massive",
            "provider_substitutions_allowed": False,
        },
        "costs_bps_per_side": [5, 10, 20],
        "partitions": {
            "rolling_origin": True,
            "confirmation_untouched": True,
            "five_session_embargo": True,
            "warmup_feature_only": True,
        },
        "falsifiers": [
            "nonpositive stressed log growth",
            "selection-aware statistical rejection",
            "incomplete execution or trial accounting",
            "insufficient frozen confirmation power capacity",
            "performance concentration in the best five trades",
            "daily breakout leadership fails to persist after costs",
        ],
        "implementation_files": [
            "style_etf_breakout_discovery.py",
            "dense_collection_plan_inspection.py",
            "dense_data_collection.py",
            "dense_data_collection_inspection.py",
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
        "capacity_manifest": _repo_path(capacity_path),
    }
    contract = strategy_discovery._validate_family_contract(contract)
    validate_contract(contract, enforce_commit=enforce_commit)
    digest = hashlib.sha256(
        artifact_support._canonical(contract)
    ).hexdigest()
    path = (
        DEFAULT_ROOT
        / SUCCESSOR_ID
        / "family-contract"
        / f"contract-{digest}.json"
    )
    artifact_support._write_json(path, contract)
    return path, contract, capacity_path


def validate_contract(
    contract: Mapping[str, Any], *, enforce_commit: bool = True
) -> None:
    (
        warmup,
        development,
        development_signals,
        embargo,
        confirmation_warmup,
        confirmation,
        confirmation_signals,
    ) = _partitions()
    if not (
        contract.get("campaign_id") == CAMPAIGN_ID
        and contract.get("family_id") == FAMILY_ID
        and contract.get("mechanism_family") == MECHANISM_FAMILY
        and contract.get("strategy_id") == STRATEGY_ID
        and contract.get("successor_id") == SUCCESSOR_ID
        and contract.get("research_generation") == RESEARCH_GENERATION
        and contract.get("new_mechanism_family_slot_consumed") is True
        and contract.get("rolling_active_family_slot") == 1
        and len(contract.get("trial_family", [])) == 1
        and contract.get("development_warmup_dates") == warmup
        and contract.get("development_dates") == development
        and contract.get("development_signal_dates")
        == development_signals
        and contract.get("embargo_dates") == embargo
        and contract.get("confirmation_warmup_dates")
        == confirmation_warmup
        and contract.get("confirmation_dates") == confirmation
        and contract.get("confirmation_signal_dates")
        == confirmation_signals
        and contract.get("confirmation_signal_capacity")
        == len(confirmation_signals)
        and contract.get("development_scope") == _scope(development)
        and contract.get("confirmation_scope") == _scope(confirmation)
        and contract.get("universe", {}).get("symbols") == SYMBOLS
        and contract.get("calendar_path") == _repo_path(CALENDAR_PATH)
        and contract.get("historical_data_contract", {}).get(
            "daily_provider"
        )
        == "alpaca"
        and contract.get("historical_data_contract", {}).get(
            "daily_request_mode"
        )
        == "symbol_range"
    ):
        raise StyleEtfBreakoutDiscoveryError(
            "style ETF breakout contract drifted"
        )
    if enforce_commit:
        strategy_discovery.require_committed(Path(__file__).resolve())
    shared._calendar_authority(enforce_commit=enforce_commit)
    _predecessor_disposition(enforce_commit=enforce_commit)
    _validate_exposure_state(contract)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("freeze",))
    parser.add_argument("--created-at", required=True)
    return parser


def main() -> int:
    args = _parser().parse_args()
    try:
        path, contract, capacity = freeze_contract(
            created_at=args.created_at
        )
        print(
            json.dumps(
                {
                    "path": _repo_path(path),
                    "state": contract["status"],
                    "trial_count": len(contract["trial_family"]),
                    "development_sessions": len(
                        contract["development_dates"]
                    ),
                    "development_signal_dates": len(
                        contract["development_signal_dates"]
                    ),
                    "confirmation_sessions": len(
                        contract["confirmation_dates"]
                    ),
                    "confirmation_signal_capacity": contract[
                        "confirmation_signal_capacity"
                    ],
                    "capacity_manifest": _repo_path(capacity),
                    "confirmation_access_permitted": False,
                    "broker_actions": 0,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    except (
        StyleEtfBreakoutDiscoveryError,
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


if __name__ == "__main__":
    raise SystemExit(main())

"""Freeze a new liquid equity-ETF internal-bar-strength reversal family."""

from __future__ import annotations

import argparse
import hashlib
import json
from collections.abc import Mapping
from datetime import date, datetime
from pathlib import Path
from typing import Any

import dense_data_collection
import dense_strategy_runtime as runtime
import etf_pullback_replication as calendar_support
import outcome_exposure
import portfolio_maturity
import rolling_discovery_authorization
import strategy_discovery
from historical_store import sha256_file
from learning_data import freeze_dataset_contract
from learning_experiment import DEVELOPMENT_SEARCH_RULE


PROJECT_ROOT = Path(__file__).resolve().parent
CAMPAIGN_ID = portfolio_maturity.V2_CAMPAIGN_ID
FAMILY_ID = runtime.ETF_IBS_REVERSAL_FAMILY
MECHANISM_FAMILY = "internal-bar-strength-liquidity-reversal"
STRATEGY_ID = "liquid-equity-etf-ibs-reversal"
SUCCESSOR_ID = "liquid-equity-etf-ibs-reversal-v1"
RESEARCH_GENERATION = "new_mechanism_family"
DEFAULT_ROOT = PROJECT_ROOT / "strategy_tournament/v2/continuous"
CALENDAR_PATH = calendar_support.CALENDAR_PATH
SYMBOLS = [
    "EEM",
    "EFA",
    "MDY",
    "SPYG",
    "SPYV",
    "VNQ",
    "VTI",
    "VTV",
    "VUG",
]
WARMUP_SESSIONS = 200
DEVELOPMENT_SESSIONS = 1_000
EMBARGO_SESSIONS = 5
CONFIRMATION_SESSIONS = 500
MAXIMUM_HOLD_SESSIONS = 2
TOTAL_SESSIONS = (
    WARMUP_SESSIONS
    + DEVELOPMENT_SESSIONS
    + EMBARGO_SESSIONS
    + CONFIRMATION_SESSIONS
)


class EtfIbsReversalError(ValueError):
    """The IBS family authority, scope, or exact rules drifted."""


def _repo_path(path: Path) -> str:
    try:
        return path.resolve().relative_to(PROJECT_ROOT).as_posix()
    except ValueError as exc:
        raise EtfIbsReversalError(
            f"path escaped repository: {path}"
        ) from exc


def _timestamp(value: str, field: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise EtfIbsReversalError(f"{field} is invalid") from exc
    if parsed.tzinfo is None:
        raise EtfIbsReversalError(f"{field} needs a timezone")
    if parsed.date() > date.today():
        raise EtfIbsReversalError(f"{field} cannot be future-dated")
    return parsed


def _rolling_slot_authority(
    *, enforce_commit: bool
) -> dict[str, Any]:
    status_path = rolling_discovery_authorization.DEFAULT_STATUS
    status = rolling_discovery_authorization.load_ready_status(
        status_path
    )
    authorization_path = Path(str(status["authorization_path"]))
    if not authorization_path.is_absolute():
        authorization_path = PROJECT_ROOT / authorization_path
    authorization = (
        rolling_discovery_authorization.load_authorization(
            authorization_path
        )
    )
    if enforce_commit:
        strategy_discovery.require_committed(status_path)
        strategy_discovery.require_committed(authorization_path)
    if not (
        status.get("state") == "ROLLING_DISCOVERY_AUTHORIZED"
        and status.get("activation_policy")
        == rolling_discovery_authorization.POLICY
        and status.get("active_family_count") == 0
        and status.get("available_slot_count") == 3
        and status.get("provider_access_permitted") is True
        and status.get("target_outcome_access_permitted") is False
        and status.get("broker_actions_permitted") is False
        and authorization.get(
            "maximum_concurrent_active_mechanism_families"
        )
        == 3
        and authorization.get("selection_accounting", {}).get(
            "all_prior_trials_retained"
        )
        is True
        and authorization.get("selection_accounting", {}).get(
            "all_prior_dispositions_retained"
        )
        is True
    ):
        raise EtfIbsReversalError(
            "rolling terminal-replacement authority is not ready"
        )
    return {
        "policy": rolling_discovery_authorization.POLICY,
        "active_family_count_before_freeze": 0,
        "available_slot_count_before_freeze": 3,
        "consumed_active_slot": 1,
        "authorization_path": _repo_path(authorization_path),
        "authorization_sha256": authorization[
            "authorization_sha256"
        ],
        "status_path": _repo_path(status_path),
        "status_file_sha256": sha256_file(status_path),
        "all_prior_trials_retained": True,
        "all_prior_dispositions_retained": True,
    }


def _partitions() -> tuple[
    list[str],
    list[str],
    list[str],
    list[str],
    list[str],
    list[str],
    list[str],
]:
    dates = calendar_support._calendar_dates()[:TOTAL_SESSIONS]
    if len(dates) != TOTAL_SESSIONS:
        raise EtfIbsReversalError(
            "2008-2014 full-session capacity drifted"
        )
    warmup = dates[:WARMUP_SESSIONS]
    development = dates[
        WARMUP_SESSIONS : WARMUP_SESSIONS + DEVELOPMENT_SESSIONS
    ]
    embargo_start = WARMUP_SESSIONS + DEVELOPMENT_SESSIONS
    embargo = dates[
        embargo_start : embargo_start + EMBARGO_SESSIONS
    ]
    confirmation = dates[-CONFIRMATION_SESSIONS:]
    confirmation_start = dates.index(confirmation[0])
    confirmation_warmup = dates[
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


def _scope(dates: list[str]) -> dict[str, Any]:
    return {"dates": list(dates), "symbols": list(SYMBOLS)}


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


def _write_json(path: Path, value: Mapping[str, Any]) -> None:
    rendered = json.dumps(value, indent=2, sort_keys=True) + "\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and path.read_text(encoding="utf-8") != rendered:
        raise EtfIbsReversalError(
            "content-addressed contract differs"
        )
    if not path.exists():
        temporary = path.with_suffix(".json.tmp")
        temporary.write_text(rendered, encoding="utf-8")
        temporary.replace(path)


def freeze_contract(
    *, created_at: str, enforce_commit: bool = True
) -> tuple[Path, dict[str, Any], Path]:
    _timestamp(created_at, "created_at")
    rolling_slot = _rolling_slot_authority(
        enforce_commit=enforce_commit
    )
    if enforce_commit:
        strategy_discovery.require_committed(Path(__file__).resolve())
    calendar_inspection_path, calendar_inspection = (
        calendar_support._calendar_data_inspection(
            enforce_commit=enforce_commit
        )
    )
    if (
        calendar_inspection.get("calendar_sha256")
        != sha256_file(CALENDAR_PATH)
    ):
        raise EtfIbsReversalError("calendar inspection drifted")
    (
        warmup,
        development,
        development_signals,
        embargo,
        confirmation_warmup,
        confirmation,
        confirmation_signals,
    ) = _partitions()
    development_scope = _scope([*warmup, *development])
    confirmation_scope = _scope(confirmation)
    records = outcome_exposure.read_index()
    outcome_exposure.assert_untouched(development_scope, records)
    outcome_exposure.assert_untouched(confirmation_scope, records)
    outcome_exposure.assert_disjoint(
        [development_scope, confirmation_scope]
    )
    evidence_paths = [
        _repo_path(CALENDAR_PATH),
        _repo_path(calendar_inspection_path),
        rolling_slot["authorization_path"],
        rolling_slot["status_path"],
        "strategy_tournament/v2/OUTCOME_EXPOSURE_INDEX.jsonl",
    ]
    capacity_path, _capacity = freeze_dataset_contract(
        {
            "schema_version": 1,
            "dataset_id": f"dataset-{SUCCESSOR_ID}-capacity",
            "registered_at": created_at,
            "requested_dates": [
                *warmup,
                *development,
                *embargo,
                *confirmation,
            ],
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
                        "frozen max-one-entry development decision dates"
                    ),
                    "development_sessions": len(development),
                    "development_signal_dates": len(
                        development_signals
                    ),
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
        "successor_id": SUCCESSOR_ID,
        "research_generation": RESEARCH_GENERATION,
        "created_at": created_at,
        "status": "INVENTED",
        "dataset_lane": "development",
        "selection_mode": "development_search",
        "new_mechanism_family_slot_consumed": True,
        "rolling_slot_authority": rolling_slot,
        "mechanism": (
            "Completed-session closing pressure inside liquid equity ETFs "
            "can reverse after constrained end-of-day liquidity demand clears."
        ),
        "expected_holding_behavior": (
            "Long only, next-session-open entry, and flat within two sessions."
        ),
        "entry_rule": (
            "Require completed internal bar strength at or below the frozen "
            "threshold, a completed one-session decline at least the frozen "
            "cost-clearing floor, and close above the frozen SMA; rank the "
            "lowest internal bar strength and enter at the next session open."
        ),
        "stop_rule": (
            "Use one or one-and-a-half completed ATR14 below entry; missing "
            "or structurally invalid protection is a missed trade."
        ),
        "exit_rule": (
            "Resolve stop first on daily ambiguity and otherwise exit at the "
            "completed close after one or two sessions."
        ),
        "ranking_rule": (
            "Lowest completed internal bar strength, then most negative "
            "one-session return, then canonical symbol."
        ),
        "selection_rule": (
            "At most one new family entry per day under all portfolio risk, "
            "notional, daily-entry, and capital-contention caps."
        ),
        "primary_outcome": (
            "Selection-adjusted chronological account log growth after costs."
        ),
        "material_difference_rationale": (
            "Internal bar strength measures where the close lands inside the "
            "completed daily range, a liquidity-pressure mechanism distinct "
            "from RSI, residual-return, trend-pullback, gap, or cross-asset "
            "signals evaluated by prior families."
        ),
        "parameter_grid": {
            "internal_bar_strength_maximum": [0.1, 0.2],
            "maximum_hold_sessions": [1, 2],
            "one_session_decline_fraction": [0.005, 0.01],
            "stop_atr14": [1.0, 1.5],
            "trend_sma": [100, 200],
        },
        "winner_selection": DEVELOPMENT_SEARCH_RULE,
        "universe": {
            "symbols": list(SYMBOLS),
            "point_in_time": True,
        },
        "universe_requirements": {
            "complete_frozen_daily_history": True,
            "security_type": "liquid long-only equity ETFs",
            "selection_basis": (
                "Nine fixed U.S.-listed equity and style ETFs with pre-2008 "
                "history and globally untouched 2008-2014 target pairs."
            ),
        },
        "execution_assumptions": {
            "long_only": True,
            "next_observable_fill": "next_session_open",
            "maximum_hold_sessions": MAXIMUM_HOLD_SESSIONS,
            "ambiguity": "stop_first",
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
            "alpha": 0.1,
            "power": 0.8,
        },
        "contamination_risks": [
            "Every frozen warmup, development, and confirmation date-symbol pair was absent from the global exposure index before freeze.",
            "No prior RSI or oversold-family outcome selected an IBS threshold.",
            "The complete 32-trial grid is frozen before any price access.",
            "Warmup is feature-only and is included in development exposure.",
        ],
        "production_compatibility_risks": [
            "Fresh quote, spread, depth, halt, tradability, timestamp, GTC protection, and before-open reconciliation remain mandatory.",
        ],
        "development_warmup_dates": warmup,
        "development_dates": development,
        "development_signal_dates": development_signals,
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
        "calendar_sha256": sha256_file(CALENDAR_PATH),
        "calendar_inspection_path": _repo_path(
            calendar_inspection_path
        ),
        "calendar_inspection_sha256": calendar_inspection[
            "artifact_sha256"
        ],
        "historical_data_contract": {
            "daily_provider": "yahoo",
            "daily_endpoint": dense_data_collection.YAHOO_CHART_ENDPOINT,
            "daily_request_mode": "symbol_range",
            "daily_adjustment": (
                dense_data_collection.YAHOO_SOURCE_RECOVERY_ADJUSTMENT
            ),
            "split_provider": "massive",
            "provider_substitutions_allowed": False,
            "no_purchase_required": True,
            "retries_permitted": 0,
        },
        "costs_bps_per_side": [5, 10, 20],
        "partitions": {
            "rolling_origin": True,
            "confirmation_untouched": True,
            "predecessor_corpora_disjoint": True,
        },
        "falsifiers": [
            "nonpositive stressed log growth",
            "unstable parameter neighbors",
            "selection-aware statistical rejection",
            "incomplete execution or trial accounting",
            "insufficient frozen confirmation power capacity",
            "closing-location effect fails outside concentrated crisis trades",
        ],
        "implementation_files": [
            "etf_ibs_reversal.py",
            "etf_pullback_replication.py",
            "dense_data_collection.py",
            "dense_data_collection_inspection.py",
            "dense_collection_plan_inspection.py",
            "dense_strategy_plugin.py",
            "dense_strategy_runtime.py",
            "learning_statistics.py",
            "learning_experiment.py",
            "strategy_discovery.py",
            "outcome_exposure.py",
            "rolling_discovery_authorization.py",
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
        "capacity_policy": {
            "retire_below": 50,
            "fast_lane_at": 100,
        },
        "capacity_manifest": _repo_path(capacity_path),
    }
    contract = strategy_discovery._validate_family_contract(
        contract
    )
    validate_contract(contract, enforce_commit=enforce_commit)
    digest = hashlib.sha256(
        json.dumps(
            contract,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode()
    ).hexdigest()
    path = (
        DEFAULT_ROOT
        / SUCCESSOR_ID
        / "family-contract"
        / f"contract-{digest}.json"
    )
    _write_json(path, contract)
    return path, contract, capacity_path


def validate_contract(
    contract: Mapping[str, Any], *, enforce_commit: bool = True
) -> None:
    rolling_slot = _rolling_slot_authority(
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
    expected_data_contract = {
        "daily_provider": "yahoo",
        "daily_endpoint": dense_data_collection.YAHOO_CHART_ENDPOINT,
        "daily_request_mode": "symbol_range",
        "daily_adjustment": (
            dense_data_collection.YAHOO_SOURCE_RECOVERY_ADJUSTMENT
        ),
        "split_provider": "massive",
        "provider_substitutions_allowed": False,
        "no_purchase_required": True,
        "retries_permitted": 0,
    }
    if not (
        contract.get("campaign_id") == CAMPAIGN_ID
        and contract.get("family_id") == FAMILY_ID
        and contract.get("mechanism_family") == MECHANISM_FAMILY
        and contract.get("successor_id") == SUCCESSOR_ID
        and contract.get("research_generation") == RESEARCH_GENERATION
        and contract.get("new_mechanism_family_slot_consumed") is True
        and contract.get("rolling_slot_authority") == rolling_slot
        and len(contract.get("trial_family", [])) == 32
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
        and contract.get("development_scope")
        == _scope([*warmup, *development])
        and contract.get("confirmation_scope") == _scope(confirmation)
        and contract.get("universe", {}).get("symbols") == SYMBOLS
        and contract.get("historical_data_contract")
        == expected_data_contract
        and contract.get("calendar_sha256")
        == sha256_file(CALENDAR_PATH)
    ):
        raise EtfIbsReversalError("IBS family contract drifted")
    if enforce_commit:
        strategy_discovery.require_committed(Path(__file__).resolve())
    calendar_path, calendar_inspection = (
        calendar_support._calendar_data_inspection(
            enforce_commit=enforce_commit
        )
    )
    if not (
        contract.get("calendar_inspection_path")
        == _repo_path(calendar_path)
        and contract.get("calendar_inspection_sha256")
        == calendar_inspection["artifact_sha256"]
    ):
        raise EtfIbsReversalError("IBS calendar binding drifted")
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
                    "confirmation_sessions": len(
                        contract["confirmation_dates"]
                    ),
                    "capacity_manifest": _repo_path(capacity),
                    "new_mechanism_family_slot_consumed": True,
                    "confirmation_access_permitted": False,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    except (
        EtfIbsReversalError,
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

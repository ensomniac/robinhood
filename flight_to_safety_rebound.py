"""Freeze an outcome-clean cross-asset flight-to-safety rebound family."""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
from collections.abc import Mapping, Sequence
from datetime import date, datetime
from pathlib import Path
from typing import Any

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
FAMILY_ID = runtime.FLIGHT_TO_SAFETY_REBOUND_FAMILY
MECHANISM_FAMILY = "cross-asset-flight-to-safety-rebound"
STRATEGY_ID = "flight-to-safety-equity-rebound"
SUCCESSOR_ID = "flight-to-safety-equity-rebound-v1"
RESEARCH_GENERATION = "new_mechanism_family"
DEFAULT_ROOT = PROJECT_ROOT / "strategy_tournament/v2/continuous"
CALENDAR_PATH = (
    PROJECT_ROOT
    / "historical_batches/dense_v2/"
    "session-calendar-2020-01-through-2026-07.json"
)
TARGET_SYMBOLS = list(runtime.FLIGHT_TO_SAFETY_TARGET_SYMBOLS)
FEATURE_SYMBOLS = [runtime.FLIGHT_TO_SAFETY_FEATURE_SYMBOL]
SYMBOLS = [*TARGET_SYMBOLS, *FEATURE_SYMBOLS]
DEVELOPMENT_WARMUP_SESSIONS = 200
EMBARGO_SESSIONS = 5
MAXIMUM_HOLD_SESSIONS = 5


class FlightToSafetyReboundError(ValueError):
    """The family contract or its outcome boundary drifted."""


def _repo_path(path: Path) -> str:
    return artifact_support._repo_path(path)


def _timestamp(value: str, field: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise FlightToSafetyReboundError(f"{field} is invalid") from exc
    if parsed.tzinfo is None:
        raise FlightToSafetyReboundError(f"{field} needs a timezone")
    if parsed.date() > date.today():
        raise FlightToSafetyReboundError(f"{field} cannot be future-dated")
    return parsed


def _full_sessions() -> list[str]:
    try:
        rows = json.loads(CALENDAR_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise FlightToSafetyReboundError(
            "frozen exchange calendar cannot be loaded"
        ) from exc
    if not isinstance(rows, list):
        raise FlightToSafetyReboundError("frozen exchange calendar is malformed")
    dates = [
        str(row["date"])
        for row in rows
        if isinstance(row, Mapping)
        and row.get("open_et") == "09:30"
        and row.get("close_et") == "16:00"
    ]
    if dates != sorted(dates) or len(dates) != len(set(dates)):
        raise FlightToSafetyReboundError(
            "frozen full-session calendar is not chronological"
        )
    return dates


def _partitions() -> tuple[
    list[str],
    list[str],
    list[str],
    list[str],
    list[str],
    list[str],
    list[str],
]:
    full = _full_sessions()
    development = [day for day in full if day.startswith("2021-")]
    year_2022 = [day for day in full if day.startswith("2022-")]
    if len(development) != 251 or len(year_2022) != 250:
        raise FlightToSafetyReboundError(
            "frozen 2021-2022 full-session partition drifted"
        )
    development_start = full.index(development[0])
    if development_start < DEVELOPMENT_WARMUP_SESSIONS:
        raise FlightToSafetyReboundError("development warmup capacity is missing")
    development_warmup = full[
        development_start - DEVELOPMENT_WARMUP_SESSIONS : development_start
    ]
    embargo = year_2022[:EMBARGO_SESSIONS]
    confirmation = year_2022[EMBARGO_SESSIONS:]
    confirmation_start = full.index(confirmation[0])
    confirmation_warmup = full[
        confirmation_start - DEVELOPMENT_WARMUP_SESSIONS : confirmation_start
    ]
    development_signals = development[:-MAXIMUM_HOLD_SESSIONS]
    confirmation_signals = confirmation[:-MAXIMUM_HOLD_SESSIONS]
    return (
        development_warmup,
        development,
        development_signals,
        embargo,
        confirmation_warmup,
        confirmation,
        confirmation_signals,
    )


def _scope(dates: Sequence[str]) -> dict[str, Any]:
    return {"dates": list(dates), "symbols": list(TARGET_SYMBOLS)}


def _weekly_slots(
    created_at: str, *, enforce_commit: bool
) -> list[dict[str, str]]:
    created = _timestamp(created_at, "created_at")
    target_week = created.date().isocalendar()[:2]
    if enforce_commit:
        command = ["git", "ls-files", "*.json"]
        result = subprocess.run(
            command,
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
            check=True,
        )
        paths = [PROJECT_ROOT / item for item in result.stdout.splitlines()]
    else:
        paths = list(
            (PROJECT_ROOT / "strategy_tournament/v2").rglob("*.json")
        )
    slots: list[dict[str, str]] = []
    for path in paths:
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if not (
            isinstance(value, Mapping)
            and value.get("new_mechanism_family_slot_consumed") is True
            and value.get("family_id") != FAMILY_ID
            and isinstance(value.get("created_at"), str)
        ):
            continue
        try:
            observed = datetime.fromisoformat(
                str(value["created_at"]).replace("Z", "+00:00")
            )
        except ValueError:
            continue
        if observed.date().isocalendar()[:2] != target_week:
            continue
        slots.append(
            {
                "family_id": str(value["family_id"]),
                "created_at": str(value["created_at"]),
                "path": _repo_path(path),
                "file_sha256": sha256_file(path),
            }
        )
    slots.sort(key=lambda item: (item["created_at"], item["family_id"]))
    if len(slots) >= 3:
        raise FlightToSafetyReboundError(
            "the authorized three-new-family ISO-week budget is exhausted"
        )
    return slots


def _validate_exposure_state(contract: Mapping[str, Any]) -> None:
    records = outcome_exposure.read_index()
    outcome_exposure.assert_untouched(contract["development_scope"], records)
    outcome_exposure.assert_untouched(contract["confirmation_scope"], records)
    outcome_exposure.assert_disjoint(
        [contract["development_scope"], contract["confirmation_scope"]]
    )


def freeze_contract(
    *, created_at: str, enforce_commit: bool = True
) -> tuple[Path, dict[str, Any], Path]:
    slots = _weekly_slots(created_at, enforce_commit=enforce_commit)
    if enforce_commit:
        strategy_discovery.require_committed(Path(__file__).resolve())
        strategy_discovery.require_committed(CALENDAR_PATH)
    (
        development_warmup,
        development,
        development_signals,
        embargo,
        confirmation_warmup,
        confirmation,
        confirmation_signals,
    ) = _partitions()
    development_scope = _scope(development)
    confirmation_scope = _scope(confirmation)
    records = outcome_exposure.read_index()
    outcome_exposure.assert_untouched(development_scope, records)
    outcome_exposure.assert_untouched(confirmation_scope, records)
    outcome_exposure.assert_disjoint(
        [development_scope, confirmation_scope]
    )
    evidence_paths = [
        _repo_path(CALENDAR_PATH),
        "strategy_tournament/v2/OUTCOME_EXPOSURE_INDEX.jsonl",
        "PORTFOLIO_VALIDATION_V2.md",
        "PORTFOLIO_THESIS_V2.md",
    ]
    requested_dates = sorted(
        {
            *development_warmup,
            *development,
            *embargo,
            *confirmation_warmup,
            *confirmation,
        }
    )
    capacity_path, _capacity = freeze_dataset_contract(
        {
            "schema_version": 1,
            "dataset_id": f"dataset-{SUCCESSOR_ID}-capacity",
            "registered_at": created_at,
            "requested_dates": requested_dates,
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
        "weekly_new_family_slot": len(slots) + 1,
        "weekly_new_family_predecessors": slots,
        "prior_family_attempt_count": 0,
        "mechanism": (
            "Treat a completed broad-equity ETF liquidation accompanied by "
            "a same-session long-Treasury bid as a cross-asset risk-off "
            "dislocation; buy the strongest qualified equity rebound at the "
            "next session open."
        ),
        "expected_holding_behavior": (
            "Long only, next-session-open entry, and flat within five sessions."
        ),
        "dataset_lane": "development",
        "universe_requirements": {
            "target_symbols": list(TARGET_SYMBOLS),
            "feature_symbols": list(FEATURE_SYMBOLS),
            "complete_frozen_daily_history": True,
            "selection_basis": (
                "Three liquid broad-equity ETFs plus TLT as a feature-only "
                "cross-asset risk-off confirmation, all frozen before prices."
            ),
        },
        "entry_rule": (
            "After TLT meets the frozen completed one-session return floor, "
            "require a target ETF to close above its completed 100- or "
            "200-session SMA while falling by the frozen amount; rank the "
            "most negative target return first and enter next session open."
        ),
        "stop_rule": (
            "Use one or one-and-a-half completed ATR14 below entry; missing or "
            "structurally invalid protection is a missed trade."
        ),
        "exit_rule": (
            "Resolve stop first on daily ambiguity and otherwise exit at the "
            "completed close after two or five sessions."
        ),
        "ranking_rule": (
            "Most negative completed target return, then canonical symbol."
        ),
        "selection_rule": (
            "At most one new family entry per day under all portfolio risk, "
            "notional, daily-entry, and capital-contention caps."
        ),
        "parameter_grid": {
            "minimum_equity_decline_fraction": [0.0075, 0.0125],
            "minimum_tlt_return_fraction": [0.0, 0.0025],
            "trend_sma": [100, 200],
            "stop_atr14": [1.0, 1.5],
            "maximum_hold_sessions": [2, 5],
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
            "The exact 2021-2022 target date-symbol pairs are globally untouched at freeze.",
            "Warmup prices are feature-only contaminated training inputs and cannot count as promotion evidence.",
            "TLT is a causal feature only and can never become a traded candidate.",
            "Confirmation remains inaccessible until one exact inspected winner is frozen.",
        ],
        "production_compatibility_risks": [
            "Fresh quote, spread, depth, halt, tradability, timestamp, GTC protection, and before-open reconciliation remain mandatory."
        ],
        "material_difference_rationale": (
            "This is a distinct cross-asset liquidity-dislocation mechanism: "
            "Treasury demand confirms risk-off liquidation before a broad-equity "
            "rebound, rather than reusing price-only oversold or gap rules."
        ),
        "development_dates": development,
        "development_signal_dates": development_signals,
        "development_warmup_dates": development_warmup,
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
        "universe": {
            "symbols": list(SYMBOLS),
            "target_symbols": list(TARGET_SYMBOLS),
            "feature_symbols": list(FEATURE_SYMBOLS),
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
        "calendar_path": _repo_path(CALENDAR_PATH),
        "costs_bps_per_side": [5, 10, 20],
        "partitions": {
            "rolling_origin": True,
            "confirmation_untouched": True,
            "five_session_embargo": True,
            "warmup_feature_only": True,
        },
        "falsifiers": [
            "nonpositive stressed log growth",
            "unstable parameter neighbors",
            "selection-aware statistical rejection",
            "incomplete execution or trial accounting",
            "insufficient frozen confirmation power capacity",
            "Treasury confirmation does not isolate rebound expectancy",
        ],
        "implementation_files": [
            "flight_to_safety_rebound.py",
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
        development_warmup,
        development,
        development_signals,
        embargo,
        confirmation_warmup,
        confirmation,
        confirmation_signals,
    ) = _partitions()
    expected_history = {
        "daily_provider": "alpaca",
        "daily_endpoint": "/v2/stocks/{symbol}/bars",
        "daily_feed": "sip",
        "daily_adjustment": "raw",
        "daily_request_mode": "symbol_range",
        "split_provider": "massive",
        "provider_substitutions_allowed": False,
    }
    if not (
        contract.get("campaign_id") == CAMPAIGN_ID
        and contract.get("family_id") == FAMILY_ID
        and contract.get("mechanism_family") == MECHANISM_FAMILY
        and contract.get("strategy_id") == STRATEGY_ID
        and contract.get("successor_id") == SUCCESSOR_ID
        and contract.get("research_generation") == RESEARCH_GENERATION
        and contract.get("new_mechanism_family_slot_consumed") is True
        and contract.get("weekly_new_family_slot") in {1, 2, 3}
        and len(contract.get("trial_family", [])) == 32
        and contract.get("development_warmup_dates")
        == development_warmup
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
        and contract.get("universe", {}).get("target_symbols")
        == TARGET_SYMBOLS
        and contract.get("universe", {}).get("feature_symbols")
        == FEATURE_SYMBOLS
        and contract.get("historical_data_contract") == expected_history
        and contract.get("calendar_path") == _repo_path(CALENDAR_PATH)
    ):
        raise FlightToSafetyReboundError(
            "flight-to-safety family contract drifted"
        )
    if enforce_commit:
        strategy_discovery.require_committed(Path(__file__).resolve())
        strategy_discovery.require_committed(CALENDAR_PATH)
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
                    "weekly_new_family_slot": contract[
                        "weekly_new_family_slot"
                    ],
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
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    except (
        FlightToSafetyReboundError,
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

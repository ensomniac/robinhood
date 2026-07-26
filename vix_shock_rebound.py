"""Freeze the fixed-rule VIX-shock SPLV rebound family before price access."""

from __future__ import annotations

import argparse
import hashlib
import json
from collections.abc import Mapping, Sequence
from datetime import datetime
from pathlib import Path
from typing import Any

import continuous_strategy_discovery as calendar_support
import dense_data_collection
import dense_strategy_runtime as runtime
import etf_close_strength_continuation as rolling_support
import outcome_exposure
import portfolio_maturity
import strategy_discovery
from historical_store import sha256_file
from learning_data import freeze_dataset_contract
from learning_experiment import DEVELOPMENT_SEARCH_RULE


PROJECT_ROOT = Path(__file__).resolve().parent
CAMPAIGN_ID = portfolio_maturity.V2_CAMPAIGN_ID
FAMILY_ID = runtime.VIX_SHOCK_REBOUND_FAMILY
MECHANISM_FAMILY = "volatility-shock-low-volatility-equity-rebound"
STRATEGY_ID = FAMILY_ID
SUCCESSOR_ID = f"{FAMILY_ID}-v1"
RESEARCH_GENERATION = "new_mechanism_family"
DEFAULT_ROOT = PROJECT_ROOT / "strategy_tournament/v2/continuous"
CALENDAR_PATH = calendar_support.CALENDAR_PATH
CALENDAR_INSPECTION_ROOT = (
    calendar_support.CALENDAR_ROOT / "data-inspection"
)
SYMBOLS = [
    runtime.VIX_SHOCK_REBOUND_TARGET_SYMBOL,
    runtime.VIX_SHOCK_REBOUND_FEATURE_SYMBOL,
]
WARMUP_SESSIONS = 200
DEVELOPMENT_SESSIONS = 1_000
EMBARGO_SESSIONS = 5
CONFIRMATION_SESSIONS = 500
MAXIMUM_HOLD_SESSIONS = 5
TOTAL_SESSIONS = (
    WARMUP_SESSIONS
    + DEVELOPMENT_SESSIONS
    + EMBARGO_SESSIONS
    + CONFIRMATION_SESSIONS
)
PARAMETERS = {
    "vix_close_minimum": 25.0,
    "minimum_three_session_decline_fraction": 0.015,
    "mean_reversion_sma": 5,
    "stop_atr14": 1.5,
    "maximum_hold_sessions": 5,
    "cooldown_sessions": 5,
}


class VixShockReboundError(ValueError):
    """The rolling authority, calendar, scope, or exact fixed rule drifted."""


def _repo_path(path: Path) -> str:
    try:
        return path.resolve().relative_to(PROJECT_ROOT.resolve()).as_posix()
    except ValueError as exc:
        raise VixShockReboundError(
            f"path escaped repository: {path}"
        ) from exc


def _timestamp(value: str, field: str) -> str:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise VixShockReboundError(f"{field} is invalid") from exc
    if parsed.tzinfo is None:
        raise VixShockReboundError(
            f"{field} must include a timezone"
        )
    return value


def _write_json(path: Path, value: Mapping[str, Any]) -> None:
    rendered = json.dumps(value, indent=2, sort_keys=True) + "\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and path.read_text(encoding="utf-8") != rendered:
        raise VixShockReboundError(
            f"content-addressed artifact differs: {path}"
        )
    if not path.exists():
        temporary = path.with_suffix(".json.tmp")
        temporary.write_text(rendered, encoding="utf-8")
        temporary.replace(path)


def _calendar_dates() -> list[str]:
    raw = json.loads(CALENDAR_PATH.read_text(encoding="utf-8"))
    if not isinstance(raw, list):
        raise VixShockReboundError("calendar must be an array")
    dates = [
        str(row["date"])
        for row in raw
        if isinstance(row, Mapping)
        and row.get("open_et") == "09:30"
        and row.get("close_et") == "16:00"
    ]
    if (
        len(dates) < TOTAL_SESSIONS
        or dates != sorted(set(dates))
    ):
        raise VixShockReboundError(
            "calendar lacks chronological full-session capacity"
        )
    return dates


def partitions() -> dict[str, list[str]]:
    dates = _calendar_dates()[:TOTAL_SESSIONS]
    warmup = dates[:WARMUP_SESSIONS]
    development_start = WARMUP_SESSIONS
    development_end = development_start + DEVELOPMENT_SESSIONS
    development = dates[development_start:development_end]
    embargo_end = development_end + EMBARGO_SESSIONS
    embargo = dates[development_end:embargo_end]
    confirmation = dates[embargo_end:]
    confirmation_warmup = dates[
        embargo_end - WARMUP_SESSIONS : embargo_end
    ]
    if not (
        len(warmup) == WARMUP_SESSIONS
        and len(development) == DEVELOPMENT_SESSIONS
        and len(embargo) == EMBARGO_SESSIONS
        and len(confirmation) == CONFIRMATION_SESSIONS
        and len(confirmation_warmup) == WARMUP_SESSIONS
        and warmup[-1] < development[0]
        and development[-1] < embargo[0]
        and embargo[-1] < confirmation[0]
    ):
        raise VixShockReboundError("fixed evidence partitions drifted")
    return {
        "development_warmup_dates": warmup,
        "development_dates": development,
        "development_signal_dates": development[
            1:-MAXIMUM_HOLD_SESSIONS
        ],
        "embargo_dates": embargo,
        "confirmation_warmup_dates": confirmation_warmup,
        "confirmation_dates": confirmation,
        "confirmation_signal_dates": confirmation[
            1:-MAXIMUM_HOLD_SESSIONS
        ],
    }


def _scope(dates: Sequence[str]) -> dict[str, Any]:
    return outcome_exposure.validate_scope(
        {"dates": list(dates), "symbols": list(SYMBOLS)}
    )


def _calendar_inspection(
    *, enforce_commit: bool
) -> tuple[Path, dict[str, Any]]:
    expected_sha256 = sha256_file(CALENDAR_PATH)
    matches: list[tuple[Path, dict[str, Any]]] = []
    for path in sorted(CALENDAR_INSPECTION_ROOT.glob("*.json")):
        try:
            artifact = strategy_discovery.load_artifact(
                path,
                expected_kind=(
                    calendar_support.CALENDAR_DATA_INSPECTION_KIND
                ),
            )
        except strategy_discovery.StrategyDiscoveryError:
            continue
        if (
            artifact.get("state") == "CALENDAR_INSPECTED_READY"
            and artifact.get("calendar_path") == _repo_path(CALENDAR_PATH)
            and artifact.get("calendar_sha256") == expected_sha256
            and artifact.get("market_prices_accessed") is False
            and artifact.get("target_outcomes_accessed") is False
            and artifact.get("broker_actions") == 0
        ):
            matches.append((path, artifact))
    if len(matches) != 1:
        raise VixShockReboundError(
            "expected one inspected 2014-2022 session calendar"
        )
    if enforce_commit:
        strategy_discovery.require_committed(matches[0][0])
        strategy_discovery.require_committed(CALENDAR_PATH)
    return matches[0]


def _rolling_authority(*, enforce_commit: bool) -> dict[str, Any]:
    return rolling_support._rolling_slot_authority(
        enforce_commit=enforce_commit
    )


def validate_contract(
    contract: Mapping[str, Any],
    *,
    enforce_commit: bool = True,
) -> None:
    split = partitions()
    rolling = _rolling_authority(enforce_commit=enforce_commit)
    inspection_path, inspection = _calendar_inspection(
        enforce_commit=enforce_commit
    )
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
        and contract.get("rolling_slot_authority") == rolling
        and len(contract.get("trial_family", [])) == 1
        and contract.get("trial_family", [{}])[0].get("parameters")
        == PARAMETERS
        and all(
            contract.get(field) == values
            for field, values in split.items()
        )
        and contract.get("development_scope")
        == _scope(
            [
                *split["development_warmup_dates"],
                *split["development_dates"],
            ]
        )
        and contract.get("confirmation_scope")
        == _scope(split["confirmation_signal_dates"])
        and contract.get("universe", {}).get("symbols") == SYMBOLS
        and contract.get("historical_data_contract")
        == expected_data_contract
        and contract.get("calendar_path") == _repo_path(CALENDAR_PATH)
        and contract.get("calendar_sha256")
        == sha256_file(CALENDAR_PATH)
        and contract.get("calendar_inspection_path")
        == _repo_path(inspection_path)
        and contract.get("calendar_inspection_sha256")
        == inspection["artifact_sha256"]
    ):
        raise VixShockReboundError("VIX-shock family contract drifted")
    records = outcome_exposure.read_index()
    outcome_exposure.assert_untouched(
        contract["development_scope"], records
    )
    outcome_exposure.assert_untouched(
        contract["confirmation_scope"], records
    )
    outcome_exposure.assert_disjoint(
        [
            contract["development_scope"],
            contract["confirmation_scope"],
        ]
    )
    if enforce_commit:
        strategy_discovery.require_committed(Path(__file__).resolve())


def freeze_contract(
    *,
    created_at: str,
    enforce_commit: bool = True,
) -> tuple[Path, dict[str, Any], Path]:
    _timestamp(created_at, "created_at")
    if enforce_commit:
        strategy_discovery.require_committed(Path(__file__).resolve())
    rolling = _rolling_authority(enforce_commit=enforce_commit)
    inspection_path, inspection = _calendar_inspection(
        enforce_commit=enforce_commit
    )
    split = partitions()
    records = outcome_exposure.read_index()
    development_scope = _scope(
        [
            *split["development_warmup_dates"],
            *split["development_dates"],
        ]
    )
    confirmation_scope = _scope(
        split["confirmation_signal_dates"]
    )
    outcome_exposure.assert_untouched(development_scope, records)
    outcome_exposure.assert_untouched(confirmation_scope, records)
    outcome_exposure.assert_disjoint(
        [development_scope, confirmation_scope]
    )
    evidence_paths = [
        _repo_path(CALENDAR_PATH),
        _repo_path(inspection_path),
        rolling["authorization_path"],
        rolling["status_path"],
        "strategy_tournament/v2/OUTCOME_EXPOSURE_INDEX.jsonl",
    ]
    capacity_path, _capacity = freeze_dataset_contract(
        {
            "schema_version": 1,
            "dataset_id": f"dataset-{SUCCESSOR_ID}-capacity",
            "registered_at": created_at,
            "requested_dates": [
                *split["development_warmup_dates"],
                *split["development_dates"],
                *split["embargo_dates"],
                *split["confirmation_dates"],
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
                    "formal_capacity": len(
                        split["development_signal_dates"]
                    ),
                    "capacity_unit": (
                        "preregistered maximum-one-entry decision dates"
                    ),
                    "development_sessions": len(
                        split["development_dates"]
                    ),
                    "development_signal_dates": len(
                        split["development_signal_dates"]
                    ),
                    "embargo_sessions": len(
                        split["embargo_dates"]
                    ),
                    "confirmation_sessions": len(
                        split["confirmation_dates"]
                    ),
                    "confirmation_signal_dates": len(
                        split["confirmation_signal_dates"]
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
        "new_mechanism_family_slot_consumed": True,
        "rolling_slot_authority": rolling,
        "created_at": created_at,
        "status": "INVENTED",
        "dataset_lane": "development",
        "selection_mode": "development_search",
        "mechanism": (
            "A completed VIX shock paired with a sharp decline in the "
            "low-volatility equity basket can create a short-lived liquidity "
            "overshoot that mean-reverts after the next observable open."
        ),
        "expected_holding_behavior": (
            "Long SPLV only from the next session open and flat on the "
            "structural stop, completed SMA5 recovery, or fifth close."
        ),
        "entry_rule": (
            "After a completed VIX close of at least 25 and completed SPLV "
            "three-session return at or below -1.5%, enter SPLV at the next "
            "session open only when completed SMA5 recovery room clears the "
            "five-times-primary-round-trip-cost floor."
        ),
        "stop_rule": (
            "Freeze a long stop 1.5 completed ATR14 below entry; missing or "
            "nonpositive protection is a missed trade."
        ),
        "exit_rule": (
            "Resolve a stop first, otherwise exit on the first completed "
            "SPLV close at or above its completed SMA5 or at the fifth close."
        ),
        "ranking_rule": (
            "SPLV is the sole tradable instrument and therefore rank one; "
            "^VIX is feature-only and never traded."
        ),
        "selection_rule": (
            "Evaluate the sole preregistered rule through rolling-origin OOF "
            "account simulation; after an eligible entry, suppress additional "
            "family entries for five sessions."
        ),
        "primary_outcome": (
            "Selection-aware chronological account log growth after costs."
        ),
        "material_difference_rationale": (
            "This is a volatility-state liquidity-overshoot mechanism, not a "
            "price-only RSI, breadth, gap, cross-sectional, residual-return, "
            "earnings, insider, buyback, or index-reconstitution rule."
        ),
        "parameter_grid": {
            key: [value] for key, value in PARAMETERS.items()
        },
        "winner_selection": DEVELOPMENT_SEARCH_RULE,
        "universe": {
            "symbols": list(SYMBOLS),
            "tradable_symbols": [
                runtime.VIX_SHOCK_REBOUND_TARGET_SYMBOL
            ],
            "feature_only_symbols": [
                runtime.VIX_SHOCK_REBOUND_FEATURE_SYMBOL
            ],
            "point_in_time": True,
        },
        "universe_requirements": {
            "complete_frozen_daily_history": True,
            "security_type": "long-only unlevered U.S. equity ETF",
            "selection_basis": (
                "SPLV and ^VIX have complete 2014-2020 causal history and "
                "were absent from the global outcome-exposure index at freeze."
            ),
        },
        "execution_assumptions": {
            "long_only": True,
            "next_observable_fill": "next_session_open",
            "maximum_hold_sessions": MAXIMUM_HOLD_SESSIONS,
            "ambiguity": "stop_first",
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
            "alpha": 0.1,
            "power": 0.8,
        },
        "contamination_risks": [
            "The sole rule and complete evidence partitions are frozen before either SPLV or ^VIX price access.",
            "Both development and confirmation target/source pairs were absent from the global outcome-exposure index at freeze.",
            "No parameter alternative exists and confirmation remains inaccessible until an exact inspected winner is frozen.",
        ],
        "production_compatibility_risks": [
            "A next-open gap can erase the recovery cost floor.",
            "Every overnight hold requires confirmed GTC protection and before-open reconciliation.",
            "The feature-only VIX close must be complete, fresh, and timestamp-valid.",
        ],
        **split,
        "confirmation_signal_capacity": len(
            split["confirmation_signal_dates"]
        ),
        "development_scope": development_scope,
        "confirmation_scope": confirmation_scope,
        "outcome_exposure_index_sha256": outcome_exposure.audit()[
            "index_sha256"
        ],
        "calendar_path": _repo_path(CALENDAR_PATH),
        "calendar_sha256": sha256_file(CALENDAR_PATH),
        "calendar_inspection_path": _repo_path(inspection_path),
        "calendar_inspection_sha256": inspection["artifact_sha256"],
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
            "development_training_contaminated": False,
            "five_session_embargo": True,
        },
        "falsifiers": [
            "nonpositive 20-bps log growth",
            "20-bps profit factor below 1.20",
            "drawdown above 6R",
            "any nonpositive rolling fold",
            "selection-aware statistical rejection",
            "incomplete rule or account-path capture",
            "insufficient dynamically powered confirmation inventory",
        ],
        "implementation_files": [
            "vix_shock_rebound.py",
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
    validated = strategy_discovery._validate_family_contract(
        contract
    )
    validate_contract(validated, enforce_commit=enforce_commit)
    digest = hashlib.sha256(
        json.dumps(
            validated,
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
    _write_json(path, validated)
    return path, validated, capacity_path


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("freeze", "status"))
    parser.add_argument("--created-at")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "status":
            result = {
                "schema_version": 1,
                "family_id": FAMILY_ID,
                "successor_id": SUCCESSOR_ID,
                "calendar_wait_required": False,
                "new_mechanism_family_slot_consumed": True,
                "family_contracts": len(
                    list(
                        (
                            DEFAULT_ROOT
                            / SUCCESSOR_ID
                            / "family-contract"
                        ).glob("*.json")
                    )
                ),
                "confirmation_prices_accessed": False,
                "broker_actions": 0,
            }
        else:
            if not args.created_at:
                raise VixShockReboundError(
                    "freeze requires --created-at"
                )
            path, contract, capacity = freeze_contract(
                created_at=args.created_at
            )
            result = {
                "path": _repo_path(path),
                "capacity_manifest": _repo_path(capacity),
                "family_id": contract["family_id"],
                "trial_count": len(contract["trial_family"]),
                "development_sessions": len(
                    contract["development_dates"]
                ),
                "confirmation_signal_capacity": contract[
                    "confirmation_signal_capacity"
                ],
                "provider_requests": 0,
                "confirmation_access_permitted": False,
                "broker_actions": 0,
            }
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    except (
        OSError,
        json.JSONDecodeError,
        VixShockReboundError,
        outcome_exposure.OutcomeExposureError,
        strategy_discovery.StrategyDiscoveryError,
    ) as exc:
        print(
            json.dumps(
                {
                    "error": str(exc),
                    "error_type": type(exc).__name__,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

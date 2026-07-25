"""Freeze the exact 32-trial SEC earnings reaction search before prices."""

from __future__ import annotations

import argparse
import hashlib
import json
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import dense_strategy_runtime as runtime
import earnings_sec_eps_capacity as v5
import earnings_sec_market_data as metadata
import earnings_sec_yahoo_data as yahoo
import earnings_sec_yahoo_failure as failure
import outcome_exposure
import portfolio_maturity
import strategy_discovery
from historical_store import HistoricalDayStore, canonical_sha256
from learning_data import freeze_dataset_contract
from learning_experiment import DEVELOPMENT_SEARCH_RULE


PROJECT_ROOT = Path(__file__).resolve().parent
CAMPAIGN_ID = portfolio_maturity.V2_CAMPAIGN_ID
FAMILY_ID = runtime.EARNINGS_SEC_REACTION_FAMILY
SUCCESSOR_ID = "earnings-positive-surprise-drift-v10-sec-reaction-search"
STRATEGY_ID = "sec-yoy-eps-reaction-drift"
MECHANISM_FAMILY = "earnings-gap-continuation"
DEFAULT_ROOT = (
    PROJECT_ROOT / "strategy_tournament/v2/continuous" / SUCCESSOR_ID
)
V9_TERMINAL_INSPECTION = (
    yahoo.DEFAULT_ROOT
    / "yahoo-pre-search-failure-inspection"
    / "inspection-8052fecc443b24d25ac2b170369f502c49c7639585bab8edd2a1671b374635c2.json"
)


class EarningsSecReactionSearchError(RuntimeError):
    """The exact pre-outcome SEC reaction search boundary drifted."""


def _event_row(event: Mapping[str, Any], reaction: str) -> dict[str, Any]:
    return {
        "adsh": str(event["adsh"]),
        "symbol": str(event["ticker"]),
        "accepted": str(event["accepted"]),
        "accepted_date": str(event["accepted_date"]),
        "report_period": str(event["period"]),
        "reaction_date": reaction,
        "current_eps": float(event["current_eps"]),
        "prior_eps": float(event["prior_year_eps"]),
        "eps_change": float(event["eps_yoy_change"]),
        "eps_change_ratio": float(event["eps_yoy_change_ratio"]),
        "security_identity_state": str(event["security_identity_state"]),
    }


def selection(
    store: HistoricalDayStore | None = None,
) -> dict[str, Any]:
    strategy_discovery.require_committed(V9_TERMINAL_INSPECTION)
    terminal = metadata._read(V9_TERMINAL_INSPECTION)
    if not (
        terminal.get("state") == "YAHOO_PRE_SEARCH_FAILURE_INSPECTED_TERMINAL"
        and terminal.get("valid") is True
        and terminal.get("v9_promotion_eligible") is False
        and terminal.get("successor_search_freeze_required_before_access")
        is True
        and terminal.get("exposed_symbols") == failure.EXPOSED_SYMBOLS
    ):
        raise EarningsSecReactionSearchError(
            "v9 pre-search exposure is not independently quarantined"
        )
    historical_store = store or HistoricalDayStore.from_env()
    base = metadata._development_selection(historical_store)
    development_dates = [
        day for day in base["development_dates"] if day <= v5.DEVELOPMENT_END
    ]
    development_metadata = {
        day: [
            row
            for row in base["event_metadata_by_date"][day]
            if row["symbol"] not in failure.EXPOSED_SYMBOLS
        ]
        for day in development_dates
    }
    development_symbols = sorted(
        {
            row["symbol"]
            for rows in development_metadata.values()
            for row in rows
        }
    )
    full_calendar = metadata._sessions(
        metadata.WARMUP_START, metadata.CONFIRMATION_END
    )
    confirmation_dates = base["confirmation_dates"]
    confirmation_metadata = {day: [] for day in confirmation_dates}
    _inspection, events = metadata._cover_lineage(historical_store)
    for event in events:
        accepted_date = str(event["accepted_date"])
        symbol = str(event["ticker"])
        if (
            not v5.CONFIRMATION_START
            <= accepted_date
            <= v5.CONFIRMATION_END
            or symbol in failure.EXPOSED_SYMBOLS
        ):
            continue
        reaction = metadata._reaction_date(
            str(event["accepted"]), full_calendar
        )
        if reaction in confirmation_metadata:
            confirmation_metadata[reaction].append(
                _event_row(event, reaction)
            )
    for rows_by_date in (development_metadata, confirmation_metadata):
        for day, rows in rows_by_date.items():
            rows_by_date[day] = sorted(
                rows,
                key=lambda row: (
                    -float(row["eps_change_ratio"]),
                    str(row["symbol"]),
                    str(row["adsh"]),
                ),
            )
    confirmation_symbols = sorted(
        {
            row["symbol"]
            for rows in confirmation_metadata.values()
            for row in rows
        }
    )
    development_signal_dates = [
        day for day, rows in development_metadata.items() if rows
    ]
    confirmation_signal_dates = [
        day for day, rows in confirmation_metadata.items() if rows
    ]
    yahoo_contract = metadata._read(
        yahoo.DEFAULT_ROOT
        / "yahoo-contract"
        / (
            "contract-"
            "a67bc66bb8065c392e53475a839a81c12e5dfefa6d958f673f156c6067f6bdd8"
            ".json"
        )
    )
    requests_ = [
        request
        for request in yahoo_contract["requests"]
        if request["symbol"] in development_symbols
    ]
    if not (
        sum(len(rows) for rows in development_metadata.values()) == 148
        and len(development_signal_dates) == 82
        and len(development_symbols) == 109
        and len(requests_) == 109
        and len(confirmation_signal_dates) >= 100
        and len(confirmation_symbols) >= 150
    ):
        raise EarningsSecReactionSearchError(
            "v10 untouched capacity or request graph differs"
        )
    opened_dates = [
        *base["warmup_dates"],
        *base["development_dates"],
        *base["settlement_dates"],
    ]
    embargo_dates = metadata._sessions(
        metadata.DEVELOPMENT_END.replace(day=20),
        metadata.CONFIRMATION_START.replace(day=7),
    )
    return {
        "schema_version": 1,
        "family_id": FAMILY_ID,
        "successor_id": SUCCESSOR_ID,
        "development_dates": development_dates,
        "development_signal_dates": development_signal_dates,
        "development_event_count": sum(
            len(rows) for rows in development_metadata.values()
        ),
        "development_symbols": development_symbols,
        "development_metadata_by_date": development_metadata,
        "development_opened_dates": opened_dates,
        "embargo_dates": embargo_dates,
        "confirmation_dates": confirmation_dates,
        "confirmation_signal_dates": confirmation_signal_dates,
        "confirmation_event_count": sum(
            len(rows) for rows in confirmation_metadata.values()
        ),
        "confirmation_symbols": confirmation_symbols,
        "development_requests": requests_,
        "excluded_pre_search_symbols": failure.EXPOSED_SYMBOLS,
        "confirmation_prices_accessed": False,
        "strategy_metrics_computed": 0,
        "broker_actions": 0,
    }


def _scope(dates: Sequence[str], symbols: Sequence[str]) -> dict[str, Any]:
    return {"dates": list(dates), "symbols": list(symbols)}


def build_contract(
    *,
    created_at: str,
    capacity_manifest: Path,
    store: HistoricalDayStore | None = None,
) -> dict[str, Any]:
    selected = selection(store)
    development_scope = _scope(
        selected["development_opened_dates"],
        selected["development_symbols"],
    )
    confirmation_scope = _scope(
        selected["confirmation_dates"], selected["confirmation_symbols"]
    )
    index = outcome_exposure.read_index()
    outcome_exposure.assert_untouched(development_scope, index)
    outcome_exposure.assert_untouched(confirmation_scope, index)
    outcome_exposure.assert_disjoint(
        [development_scope, confirmation_scope]
    )
    contract: dict[str, Any] = {
        "schema_version": 1,
        "campaign_id": CAMPAIGN_ID,
        "experiment_id": f"experiment-{SUCCESSOR_ID}",
        "family_id": FAMILY_ID,
        "mechanism_family": MECHANISM_FAMILY,
        "strategy_id": STRATEGY_ID,
        "parent_experiment_id": (
            "experiment-earnings-positive-surprise-drift-v9-"
            "sec-yahoo-development"
        ),
        "created_at": metadata._timestamp(created_at, "created_at"),
        "status": "INVENTED",
        "dataset_lane": "development",
        "mechanism": (
            "Positive year-over-year quarterly EPS change can continue to "
            "reprice after a completed bullish reaction session because "
            "institutional interpretation and position building are gradual."
        ),
        "expected_holding_behavior": (
            "Long at the session open after a completed bullish reaction, "
            "protected by completed ATR14, for at most five sessions."
        ),
        "entry_rule": (
            "After an as-filed verified positive year-over-year EPS event, "
            "require the frozen reaction gap and close confirmation; rank at "
            "the completed reaction close and enter the next session open."
        ),
        "stop_rule": (
            "Place the frozen ATR14 multiple below entry; gap-through exits "
            "at the observed open and same-interval ambiguity is stop-first."
        ),
        "exit_rule": (
            "Exit at the structural stop or the close of the frozen second "
            "or fifth holding session, whichever occurs first."
        ),
        "ranking_rule": (
            "Highest EPS change ratio, then reaction-session dollar volume, "
            "then lexical symbol; maximum one family entry per day."
        ),
        "selection_rule": (
            "The complete 32-trial family uses the frozen selection-aware "
            "DSR, Holm, PBO, neighbor-stability, stress, and account gates."
        ),
        "primary_outcome": (
            "Selection-adjusted chronological account log growth after costs."
        ),
        "material_difference_rationale": (
            "Unlike the terminal pre-search source, this successor excludes "
            "all five exposed symbols and freezes the evaluator, grid, "
            "request graph, selection rule, and production semantics before "
            "any remaining development price is accessed."
        ),
        "universe_requirements": {
            "security_type": "SEC same-accession verified common equity",
            "prior_close_minimum": 10.0,
            "prior_20_session_median_dollar_volume_minimum": 50_000_000.0,
            "excluded_pre_search_symbols": failure.EXPOSED_SYMBOLS,
        },
        "execution_assumptions": {
            "next_observable_open": True,
            "maximum_hold_sessions": 5,
            "same_interval_ambiguity": "stop_first",
            "missing_or_invalid_stop": "missed_or_rejected",
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
            "Five v9 symbols are globally exposed and permanently excluded.",
            "The remaining 109 symbols are untouched before this freeze.",
            "Confirmation prices remain globally untouched and inaccessible.",
        ],
        "production_compatibility_risks": [
            "Live filings require exact acceptance and same-accession identity.",
            "Fresh quote, spread, depth, halt, tradability, news, protection, and account reconciliation remain mandatory.",
        ],
        "parameter_grid": {
            "minimum_yoy_eps_change_ratio": [0.25, 0.50],
            "minimum_reaction_opening_gap_fraction": [0.0, 0.01],
            "reaction_confirmation": ["close>open", "close>prior_close"],
            "stop_atr14": [1.0, 1.5],
            "maximum_hold_sessions": [2, 5],
        },
        "selection_mode": "development_search",
        "winner_selection": DEVELOPMENT_SEARCH_RULE,
        "development_dates": selected["development_dates"],
        "development_signal_dates": selected["development_signal_dates"],
        "embargo_dates": selected["embargo_dates"],
        "confirmation_dates": selected["confirmation_dates"],
        "confirmation_signal_dates": selected["confirmation_signal_dates"],
        "confirmation_signal_capacity": len(
            selected["confirmation_signal_dates"]
        ),
        "development_scope": development_scope,
        "confirmation_scope": confirmation_scope,
        "outcome_exposure_index_sha256": outcome_exposure.audit()[
            "index_sha256"
        ],
        "universe": {
            "point_in_time": True,
            "security_type": "SEC same-accession verified common equity",
            "excluded_symbols": failure.EXPOSED_SYMBOLS,
            "symbols": selected["development_symbols"],
            "selection_sha256": canonical_sha256(selected),
            "development_event_count": selected["development_event_count"],
            "confirmation_event_count": selected["confirmation_event_count"],
        },
        "development_data_requests": selected["development_requests"],
        "development_data_policy": {
            "source": "Yahoo Finance historical chart JSON",
            "authorized_requests": 109,
            "pacing_seconds": yahoo.PACE_SECONDS,
            "retries": 0,
            "substitutions": 0,
            "identity_schema_mismatch": "permanent_missing_zero_credit",
            "confirmation_requests": 0,
        },
        "costs_bps_per_side": [5, 10, 20],
        "partitions": {
            "rolling_origin": True,
            "confirmation_untouched": True,
            "development_training_contaminated": False,
        },
        "falsifiers": [
            "nonpositive stressed log growth",
            "unstable parameter neighbors",
            "selection-aware statistical rejection",
            "incomplete account or candidate accounting",
            "insufficient frozen confirmation power capacity",
            "any use of the five pre-search exposed symbols",
        ],
        "implementation_files": [
            "earnings_sec_reaction_search.py",
            "earnings_sec_reaction_search_inspection.py",
            "earnings_sec_reaction_collection.py",
            "earnings_sec_reaction_collection_inspection.py",
            "dense_strategy_runtime.py",
            "dense_strategy_plugin.py",
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
        "capacity_manifest": metadata._repo_path(capacity_manifest),
    }
    return strategy_discovery._validate_family_contract(contract)


def freeze_family(
    *,
    created_at: str,
    store: HistoricalDayStore | None = None,
    root: Path = DEFAULT_ROOT,
) -> tuple[Path, dict[str, Any], Path]:
    for raw in (
        "earnings_sec_reaction_search.py",
        "earnings_sec_reaction_search_inspection.py",
        "earnings_sec_reaction_collection.py",
        "earnings_sec_reaction_collection_inspection.py",
        "dense_strategy_runtime.py",
        "dense_strategy_plugin.py",
    ):
        strategy_discovery.require_committed(PROJECT_ROOT / raw)
    selected = selection(store)
    capacity_path, _manifest = freeze_dataset_contract(
        {
            "schema_version": 1,
            "dataset_id": f"dataset-{SUCCESSOR_ID}-capacity",
            "registered_at": metadata._timestamp(created_at, "created_at"),
            "requested_dates": selected["development_dates"],
            "dataset_payload": {
                "lane": "development",
                "claim_scope": "DEVELOPMENT_ONLY",
                "evidence_paths": [
                    metadata._repo_path(V9_TERMINAL_INSPECTION),
                    "strategy_tournament/v2/OUTCOME_EXPOSURE_INDEX.jsonl",
                ],
                "inspected": True,
                "point_in_time_evidence": True,
                "development_training_contaminated": False,
                "confirmation_access_permitted": False,
                "dense_capacity": {
                    "family_id": FAMILY_ID,
                    "formal_capacity": selected["development_event_count"],
                    "signal_dates": len(
                        selected["development_signal_dates"]
                    ),
                    "confirmation_signal_dates": len(
                        selected["confirmation_signal_dates"]
                    ),
                    "external_dataset_opened": False,
                },
            },
        },
        root / "capacity",
    )
    contract = build_contract(
        created_at=created_at,
        capacity_manifest=capacity_path,
        store=store,
    )
    digest = hashlib.sha256(v5.canonical_bytes(contract)).hexdigest()
    path = root / "family-contract" / f"contract-{digest}.json"
    metadata._write(path, contract)
    return path, contract, capacity_path


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("status", "freeze-family"))
    parser.add_argument("--created-at")
    args = parser.parse_args(argv)
    if args.command == "status":
        value = {
            "state": "READY_TO_FREEZE",
            "successor_id": SUCCESSOR_ID,
            "calendar_wait_required": False,
            "provider_requests_permitted": 0,
        }
    else:
        if not args.created_at:
            raise EarningsSecReactionSearchError("--created-at is required")
        path, contract, capacity = freeze_family(
            created_at=args.created_at
        )
        value = {
            "state": "FAMILY_FROZEN",
            "path": metadata._repo_path(path),
            "capacity_path": metadata._repo_path(capacity),
            "trial_count": len(contract["trial_family"]),
            "development_events": contract["universe"][
                "development_event_count"
            ],
            "provider_requests_permitted": 0,
        }
    print(json.dumps(value, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

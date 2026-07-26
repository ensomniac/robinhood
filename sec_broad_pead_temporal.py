"""Freeze the exact temporal replication of broad SEC earnings drift.

The successor evaluates one previously selected rule on still-unopened 2015
prices.  Its separately inspected 2018-2019 SEC metadata reserve remains
price-blind until a development winner is frozen.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import math
import statistics
from collections.abc import Mapping, Sequence
from datetime import date
from pathlib import Path
from statistics import NormalDist
from typing import Any

import dense_strategy_runtime as runtime
import earnings_sec_corrected_expansion as capacity
import earnings_sec_market_data as market
import earnings_sec_reaction_v13_search as v13
import outcome_exposure
import sec_broad_pead
import sec_broad_pead_temporal_metadata as metadata
import strategy_discovery
from historical_store import (
    HistoricalDayStore,
    canonical_sha256,
    sha256_file,
)
from learning_data import freeze_dataset_contract
from learning_experiment import DEVELOPMENT_SEARCH_RULE
from learning_statistics import annualized_sharpe


PROJECT_ROOT = Path(__file__).resolve().parent
CAMPAIGN_ID = sec_broad_pead.CAMPAIGN_ID
FAMILY_ID = runtime.SEC_BROAD_PEAD_TEMPORAL_FAMILY
SUCCESSOR_ID = "sec-yoy-eps-improvement-broad-drift-temporal-replication-v1"
DEFAULT_ROOT = (
    PROJECT_ROOT / "strategy_tournament/v2/continuous" / SUCCESSOR_ID
)
DISCOVERY_ROOT = PROJECT_ROOT / "strategy_tournament/v2/discovery"
PRIVATE_NAMESPACE = "_derived/sec_broad_pead_temporal"
CURRENT_RESULT = (
    DISCOVERY_ROOT
    / sec_broad_pead.FAMILY_ID
    / "development"
    / (
        f"{sec_broad_pead.FAMILY_ID}-development-"
        "7d4bf4f4d6f76ae5adb31528d3fb082c0dbba2a1387590ce598a659ca239cc87"
        ".json"
    )
)
CURRENT_INSPECTION = (
    DISCOVERY_ROOT
    / sec_broad_pead.FAMILY_ID
    / "development-inspection"
    / (
        f"{sec_broad_pead.FAMILY_ID}-development-inspection-"
        "0a02505e68a88dfdde79f6d3bbbbb26fe525dea99a4f08e78c8c6667c5850bde"
        ".json"
    )
)
METADATA_INSPECTION = (
    metadata.DEFAULT_ROOT
    / "metadata-collection-inspection"
    / (
        "inspection-"
        "599c2b60ab550e453e05b110b1a741454862d6f12606f7682b63e5012fe1f2f1"
        ".json"
    )
)
SELECTED_PRIOR_TRIAL_ID = "trial-fd3c1a04ec4ffb49"
EXACT_PARAMETERS = dict(metadata.EXACT_PARAMETERS)
EXPECTED_PRIOR_TRIALS = 49
EXPECTED_DEVELOPMENT_EVENTS = 197
EXPECTED_DEVELOPMENT_SIGNAL_DATES = 118
EXPECTED_DEVELOPMENT_SYMBOLS = 174
EXPECTED_CONFIRMATION_EVENTS = 441
EXPECTED_CONFIRMATION_SIGNAL_DATES = 279
EXPECTED_CONFIRMATION_SYMBOLS = 319


class SecBroadPeadTemporalError(RuntimeError):
    """The temporal replication evidence or exact rule drifted."""


def _repo_path(path: Path) -> str:
    return str(path.resolve().relative_to(PROJECT_ROOT.resolve()))


def _read_private(path: Path) -> dict[str, Any]:
    try:
        with gzip.open(path, "rt", encoding="utf-8") as stream:
            value = json.load(stream)
    except (OSError, json.JSONDecodeError) as exc:
        raise SecBroadPeadTemporalError(
            f"private temporal selection is unreadable: {path}"
        ) from exc
    if not isinstance(value, dict):
        raise SecBroadPeadTemporalError(
            "private temporal selection must be an object"
        )
    return value


def _confirmation_selection(
    store: HistoricalDayStore,
) -> dict[str, Any]:
    strategy_discovery.require_committed(METADATA_INSPECTION)
    inspection = capacity._read(METADATA_INSPECTION)
    binding = inspection.get("selection", {})
    relative = Path(str(binding.get("cache_relative_path", "")))
    path = (store.root / relative).resolve()
    if (
        inspection.get("state")
        != "TEMPORAL_CONFIRMATION_METADATA_CAPACITY_READY"
        or inspection.get("valid") is not True
        or relative.is_absolute()
        or ".." in relative.parts
        or store.root.resolve() not in path.parents
        or not path.is_file()
        or sha256_file(path) != binding.get("file_sha256")
    ):
        raise SecBroadPeadTemporalError(
            "temporal confirmation metadata inspection is invalid"
        )
    selected = _read_private(path)
    if not (
        selected.get("content_sha256")
        == capacity.self_hash(selected, "content_sha256")
        == binding.get("content_sha256")
        and selected.get("market_prices_accessed") is False
        and selected.get("confirmation_outcomes_accessed") is False
        and len(selected.get("events", [])) == EXPECTED_CONFIRMATION_EVENTS
        and len(selected.get("signal_dates", []))
        == EXPECTED_CONFIRMATION_SIGNAL_DATES
        and len(selected.get("symbols", []))
        == EXPECTED_CONFIRMATION_SYMBOLS
    ):
        raise SecBroadPeadTemporalError(
            "temporal confirmation metadata selection drifted"
        )
    return selected


def _runtime_event(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "adsh": str(row["adsh"]),
        "symbol": str(row["ticker"]),
        "accepted": str(row["accepted"]),
        "accepted_date": str(row["accepted"])[:10],
        "report_period": str(row["period"]),
        "reaction_date": str(row["reaction_date"]),
        "current_eps": float(row["current_eps"]),
        "prior_eps": float(row["prior_year_eps"]),
        "eps_change": float(row["eps_yoy_change"]),
        "eps_change_ratio": float(row["eps_yoy_change_ratio"]),
        "security_identity_state": str(row["security_identity_state"]),
    }


def selection(
    store: HistoricalDayStore | None = None,
) -> dict[str, Any]:
    """Build both partitions without reading any price outcomes."""

    historical_store = store or HistoricalDayStore.from_env()
    source = sec_broad_pead.selection(historical_store)
    reserve = _confirmation_selection(historical_store)
    confirmation_dates = market._sessions(
        date.fromisoformat(metadata.RESERVE_START),
        metadata.RESERVE_SETTLEMENT_END,
    )
    confirmation_metadata = {day: [] for day in confirmation_dates}
    for row in reserve["events"]:
        confirmation_metadata[str(row["reaction_date"])].append(
            _runtime_event(row)
        )
    for day in confirmation_metadata:
        confirmation_metadata[day] = sorted(
            confirmation_metadata[day],
            key=lambda row: (
                -float(row["eps_change_ratio"]),
                str(row["symbol"]),
                str(row["adsh"]),
            ),
        )
    embargo_dates = market._sessions(
        date(2016, 1, 11), date(2016, 1, 15)
    )
    selected: dict[str, Any] = {
        "schema_version": 1,
        "family_id": FAMILY_ID,
        "successor_id": SUCCESSOR_ID,
        "development_dates": list(source["confirmation_dates"]),
        "development_signal_dates": list(
            source["confirmation_signal_dates"]
        ),
        "development_event_count": int(
            source["confirmation_event_count"]
        ),
        "development_symbols": list(source["confirmation_symbols"]),
        "development_opened_dates": list(
            source["confirmation_opened_dates"]
        ),
        "development_metadata_by_date": dict(
            source["confirmation_metadata_by_date"]
        ),
        "development_requests": list(source["confirmation_requests"]),
        "embargo_dates": embargo_dates,
        "confirmation_dates": confirmation_dates,
        "confirmation_signal_dates": list(reserve["signal_dates"]),
        "confirmation_event_count": len(reserve["events"]),
        "confirmation_symbols": list(reserve["symbols"]),
        "confirmation_opened_dates": list(reserve["price_scope_dates"]),
        "confirmation_metadata_by_date": confirmation_metadata,
        "confirmation_requests": [
            v13._request(
                symbol,
                start=str(reserve["price_scope_dates"][0]),
                end=str(reserve["price_scope_dates"][-1]),
            )
            for symbol in reserve["symbols"]
        ],
        "metadata_inspection_path": _repo_path(METADATA_INSPECTION),
        "metadata_inspection_sha256": inspection_hash(),
        "development_prices_accessed": False,
        "confirmation_prices_accessed": False,
        "strategy_metrics_computed": 0,
        "provider_requests": 0,
        "broker_actions": 0,
    }
    if not (
        len(embargo_dates) == 5
        and selected["development_event_count"]
        == EXPECTED_DEVELOPMENT_EVENTS
        and len(selected["development_signal_dates"])
        == EXPECTED_DEVELOPMENT_SIGNAL_DATES
        and len(selected["development_symbols"])
        == EXPECTED_DEVELOPMENT_SYMBOLS
        and selected["confirmation_event_count"]
        == EXPECTED_CONFIRMATION_EVENTS
        and len(selected["confirmation_signal_dates"])
        == EXPECTED_CONFIRMATION_SIGNAL_DATES
        and len(selected["confirmation_symbols"])
        == EXPECTED_CONFIRMATION_SYMBOLS
        and not set(selected["development_symbols"]).intersection(
            selected["confirmation_symbols"]
        )
        and max(selected["development_dates"]) < min(embargo_dates)
        and max(embargo_dates) < min(selected["confirmation_dates"])
    ):
        raise SecBroadPeadTemporalError(
            "temporal replication partitions changed"
        )
    development_scope = {
        "dates": selected["development_opened_dates"],
        "symbols": selected["development_symbols"],
    }
    confirmation_scope = {
        "dates": selected["confirmation_opened_dates"],
        "symbols": selected["confirmation_symbols"],
    }
    outcome_exposure.assert_untouched(
        development_scope, outcome_exposure.read_index()
    )
    outcome_exposure.assert_untouched(
        confirmation_scope, outcome_exposure.read_index()
    )
    outcome_exposure.assert_disjoint(
        [development_scope, confirmation_scope]
    )
    return selected


def inspection_hash() -> str:
    return str(capacity._read(METADATA_INSPECTION)["inspection_sha256"])


def prior_statistics() -> dict[str, Any]:
    prior = sec_broad_pead.prior_statistics()
    strategy_discovery.require_committed(CURRENT_RESULT)
    strategy_discovery.require_committed(CURRENT_INSPECTION)
    result = strategy_discovery.load_artifact(
        CURRENT_RESULT, expected_kind="development-search-result"
    )
    inspection = strategy_discovery.load_artifact(
        CURRENT_INSPECTION,
        expected_kind="development-search-inspection",
    )
    if not (
        inspection.get("state") == "REJECTED"
        and inspection.get("result_sha256") == result["artifact_sha256"]
    ):
        raise SecBroadPeadTemporalError(
            "current broad SEC development lineage is not terminal"
        )
    evaluation = strategy_discovery._load_development_evaluation(
        result, root=strategy_discovery.DEFAULT_ROOT
    )
    current_trials = evaluation.get("trials", [])
    sharpes = list(prior["trial_sharpes"])
    p_values = list(prior["trial_p_values"])
    for trial in current_trials:
        values = [
            float(value)
            for value in trial["metrics"]["oof_daily_account_returns"]
        ]
        mean = statistics.fmean(values)
        deviation = statistics.stdev(values)
        statistic = (
            mean / (deviation / math.sqrt(len(values)))
            if deviation
            else 0.0
        )
        sharpes.append(float(annualized_sharpe(values) or 0.0))
        p_values.append(1 - NormalDist().cdf(statistic))
    exact = next(
        row
        for row in inspection["selection"]["trial_classifications"]
        if row["trial_id"] == SELECTED_PRIOR_TRIAL_ID
    )
    pbo = float(exact["rebuilt_metrics"]["pbo_probability"])
    if not (
        len(current_trials) == 16
        and len(sharpes) == len(p_values) == EXPECTED_PRIOR_TRIALS
        and exact["rebuilt_metrics"]["stress_20bps_total_log_growth"]
        == 0.04590529360374653
        and 0 <= pbo <= 0.5
    ):
        raise SecBroadPeadTemporalError(
            "cumulative temporal-selection accounting changed"
        )
    return {
        "trial_count": EXPECTED_PRIOR_TRIALS,
        "trial_sharpes": sharpes,
        "trial_p_values": p_values,
        "pbo_probability": pbo,
        "lineage": [
            *prior["lineage"],
            {
                "result_path": _repo_path(CURRENT_RESULT),
                "result_file_sha256": sha256_file(CURRENT_RESULT),
                "result_sha256": result["artifact_sha256"],
                "inspection_path": _repo_path(CURRENT_INSPECTION),
                "inspection_file_sha256": sha256_file(CURRENT_INSPECTION),
                "inspection_sha256": inspection["artifact_sha256"],
                "trial_count": len(current_trials),
                "selected_prior_trial_id": SELECTED_PRIOR_TRIAL_ID,
            },
        ],
    }


def build_contract(
    *,
    created_at: str,
    capacity_manifest: Path,
    selected: Mapping[str, Any],
    prior: Mapping[str, Any],
    selection_sha256: str,
) -> dict[str, Any]:
    contract: dict[str, Any] = {
        "schema_version": 1,
        "campaign_id": CAMPAIGN_ID,
        "experiment_id": f"experiment-{SUCCESSOR_ID}",
        "family_id": FAMILY_ID,
        "mechanism_family": "earnings-gap-continuation",
        "strategy_id": FAMILY_ID,
        "parent_experiment_id": (
            "experiment-sec-yoy-eps-improvement-broad-drift-v1"
        ),
        "created_at": market._timestamp(created_at, "created_at"),
        "status": "INVENTED",
        "dataset_lane": "development",
        "mechanism": (
            "Exact temporal replication of first-observable-open drift after "
            "point-in-time SEC evidence of positive year-over-year EPS change."
        ),
        "expected_holding_behavior": (
            "Long the first observable open and exit at a 1.5 ATR14 stop or "
            "after five completed sessions."
        ),
        "entry_rule": (
            "Enter the first observable open after a qualifying SEC 10-Q when "
            "the opening gap is at least minus two percent, prior close is "
            "above SMA200, and prior price, liquidity, ATR, and cost gates pass."
        ),
        "stop_rule": (
            "Set the structural stop 1.5 completed ATR14 below entry with "
            "gap-through and same-session ambiguity resolved stop-first."
        ),
        "exit_rule": (
            "Exit on the structural stop or fifth holding-session close."
        ),
        "ranking_rule": (
            "Largest year-over-year EPS change ratio, greatest prior median "
            "dollar volume, then lexical symbol."
        ),
        "selection_rule": (
            "Evaluate exactly one preregistered rule while carrying all 49 "
            "prior broad-PEAD attempts into DSR and Holm correction."
        ),
        "primary_outcome": (
            "Selection-adjusted chronological account log growth after costs."
        ),
        "material_difference_rationale": (
            "This exact successor makes no parameter repair: it tests the one "
            "2012-2014 near-survivor on unopened, symbol-disjoint 2015 evidence "
            "and seals a later historical confirmation reserve in advance."
        ),
        "universe_requirements": {
            "security_type": "SEC same-accession verified common equity",
            "positive_year_over_year_eps_improvement": True,
            "prior_close_minimum": 10.0,
            "prior_20_session_median_dollar_volume_minimum": 50_000_000.0,
            "confirmation_symbol_disjoint_from_development": True,
        },
        "execution_assumptions": {
            "first_observable_open": True,
            "same_day_sec_cutoff_eastern": "09:25:00",
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
            "The exact rule was selected after a 49-attempt contaminated development family.",
            "Every one of those prior attempts remains in DSR and Holm correction.",
            "No 2015 price or forward return was opened before this singleton freeze.",
            "Confirmation symbols are disjoint and their prices remain inaccessible before winner freeze.",
        ],
        "production_compatibility_risks": [
            "Live SEC timestamp, identity, quote, spread, depth, halt, tradability, news, GTC protection, and reconciliation must all be complete."
        ],
        "parameter_grid": {
            key: [value] for key, value in EXACT_PARAMETERS.items()
        },
        "selection_mode": "development_search",
        "winner_selection": DEVELOPMENT_SEARCH_RULE,
        "prior_trial_sharpes": list(prior["trial_sharpes"]),
        "prior_trial_p_values": list(prior["trial_p_values"]),
        "prior_pbo_probability": float(prior["pbo_probability"]),
        "prior_selection_trial_count": int(prior["trial_count"]),
        "prior_selected_trial_id": SELECTED_PRIOR_TRIAL_ID,
        "prior_selection_lineage": list(prior["lineage"]),
        "selection_accounting": {
            "current_trial_count": 1,
            "prior_evaluated_trial_count": int(prior["trial_count"]),
            "cumulative_trial_count": 1 + int(prior["trial_count"]),
            "deflated_sharpe_trial_count": 1 + int(prior["trial_count"]),
            "holm_p_value_count": 1 + int(prior["trial_count"]),
            "pbo_probability_carried_from_prior_family": float(
                prior["pbo_probability"]
            ),
        },
        "development_dates": list(selected["development_dates"]),
        "development_signal_dates": list(
            selected["development_signal_dates"]
        ),
        "embargo_dates": list(selected["embargo_dates"]),
        "confirmation_dates": list(selected["confirmation_dates"]),
        "confirmation_signal_dates": list(
            selected["confirmation_signal_dates"]
        ),
        "confirmation_signal_capacity": len(
            selected["confirmation_signal_dates"]
        ),
        "development_scope": {
            "dates": list(selected["development_opened_dates"]),
            "symbols": list(selected["development_symbols"]),
        },
        "confirmation_scope": {
            "dates": list(selected["confirmation_opened_dates"]),
            "symbols": list(selected["confirmation_symbols"]),
        },
        "outcome_exposure_index_sha256": outcome_exposure.audit()[
            "index_sha256"
        ],
        "universe": {
            "point_in_time": True,
            "security_type": "SEC same-accession verified common equity",
            "excluded_symbols": [],
            "selection_sha256": selection_sha256,
            "development_symbols": len(selected["development_symbols"]),
            "confirmation_symbols": len(selected["confirmation_symbols"]),
            "development_event_count": selected[
                "development_event_count"
            ],
            "confirmation_event_count": selected[
                "confirmation_event_count"
            ],
        },
        "development_data_policy": {
            "provider": "Yahoo Finance historical chart JSON",
            "request_count": len(selected["development_requests"]),
            "request_graph_sha256": canonical_sha256(
                selected["development_requests"]
            ),
            "invalid_ohlcv": "whole_symbol_permanent_missing_zero_credit",
            "substitutions": 0,
            "collection_only_after_search_and_price_contract_inspection": True,
        },
        "development_data_requests": list(
            selected["development_requests"]
        ),
        "confirmation_data_reserve": {
            "metadata_inspection_path": selected[
                "metadata_inspection_path"
            ],
            "metadata_inspection_sha256": selected[
                "metadata_inspection_sha256"
            ],
            "request_count": len(selected["confirmation_requests"]),
            "request_graph_sha256": canonical_sha256(
                selected["confirmation_requests"]
            ),
            "authorized_only_after_frozen_winner": True,
            "substitutions": 0,
        },
        "costs_bps_per_side": [5, 10, 20],
        "partitions": {
            "rolling_origin": True,
            "confirmation_untouched": True,
            "development_training_contaminated": False,
        },
        "falsifiers": [
            "nonpositive stressed log growth",
            "stressed profit factor below 1.20 or drawdown above 6R",
            "selection-aware DSR, Holm, or PBO rejection",
            "rolling-fold or chronological-half instability",
            "incomplete account or signal accounting",
            "insufficient frozen confirmation power capacity",
            "any access to substituted development or confirmation evidence",
        ],
        "implementation_files": [
            "sec_broad_pead_temporal.py",
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
        "capacity_manifest": _repo_path(capacity_manifest),
        "provider_requests_permitted": 0,
        "confirmation_outcomes_accessed": False,
        "broker_actions": 0,
    }
    frozen = strategy_discovery._validate_family_contract(contract)
    if not (
        len(frozen["trial_family"]) == 1
        and frozen["trial_family"][0]["parameters"] == EXACT_PARAMETERS
        and len(frozen["trial_family"]) + int(prior["trial_count"]) == 50
    ):
        raise SecBroadPeadTemporalError(
            "temporal singleton trial accounting escaped the frozen bounds"
        )
    return frozen


def freeze_family(
    *,
    created_at: str,
    root: Path = DEFAULT_ROOT,
    store: HistoricalDayStore | None = None,
    enforce_commit: bool = True,
) -> tuple[Path, dict[str, Any], Path]:
    if enforce_commit:
        for path in (
            Path(__file__).resolve(),
            PROJECT_ROOT / "dense_strategy_runtime.py",
            PROJECT_ROOT / "dense_strategy_plugin.py",
            PROJECT_ROOT / "learning_experiment.py",
            PROJECT_ROOT / "strategy_discovery.py",
            PROJECT_ROOT / "outcome_exposure.py",
            METADATA_INSPECTION,
            CURRENT_RESULT,
            CURRENT_INSPECTION,
        ):
            strategy_discovery.require_committed(path)
    historical_store = store or HistoricalDayStore.from_env()
    selected = selection(historical_store)
    prior = prior_statistics()
    selection_sha256 = canonical_sha256(selected)
    private_path = (
        historical_store.root
        / PRIVATE_NAMESPACE
        / selection_sha256
        / "selection.json.gz"
    )
    market._write_private(private_path, selected)
    capacity_path, _ = freeze_dataset_contract(
        {
            "schema_version": 1,
            "dataset_id": f"dataset-{SUCCESSOR_ID}-capacity",
            "registered_at": market._timestamp(
                created_at, "created_at"
            ),
            "requested_dates": selected["development_dates"],
            "dataset_payload": {
                "lane": "development",
                "claim_scope": "DEVELOPMENT_ONLY",
                "inspected": True,
                "point_in_time_evidence": True,
                "confirmation_access_permitted": False,
                "evidence_paths": [
                    _repo_path(CURRENT_RESULT),
                    _repo_path(CURRENT_INSPECTION),
                    _repo_path(METADATA_INSPECTION),
                    "strategy_tournament/v2/OUTCOME_EXPOSURE_INDEX.jsonl",
                ],
                "dense_capacity": {
                    "family_id": FAMILY_ID,
                    "formal_capacity": selected[
                        "development_event_count"
                    ],
                    "signal_dates": len(
                        selected["development_signal_dates"]
                    ),
                    "confirmation_events": selected[
                        "confirmation_event_count"
                    ],
                    "confirmation_signal_dates": len(
                        selected["confirmation_signal_dates"]
                    ),
                    "external_dataset_opened": False,
                    "provider_requests": 0,
                },
                "temporal_selection": {
                    "external_relative_path": str(
                        private_path.relative_to(historical_store.root)
                    ),
                    "external_file_sha256": sha256_file(private_path),
                    "selection_sha256": selection_sha256,
                },
            },
        },
        root / "capacity",
    )
    contract = build_contract(
        created_at=created_at,
        capacity_manifest=capacity_path,
        selected=selected,
        prior=prior,
        selection_sha256=selection_sha256,
    )
    digest = hashlib.sha256(
        capacity.canonical_bytes(contract)
    ).hexdigest()
    path = root / "family-contract" / f"contract-{digest}.json"
    capacity._write(path, contract)
    return path, contract, capacity_path


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    freeze = subparsers.add_parser("freeze-family")
    freeze.add_argument("--created-at", required=True)
    args = parser.parse_args(argv)
    path, contract, capacity_path = freeze_family(
        created_at=args.created_at
    )
    print(
        json.dumps(
            {
                "state": "TEMPORAL_REPLICATION_FAMILY_FROZEN",
                "path": _repo_path(path),
                "capacity_path": _repo_path(capacity_path),
                "current_trials": len(contract["trial_family"]),
                "prior_selection_trials": contract[
                    "prior_selection_trial_count"
                ],
                "development_events": contract["universe"][
                    "development_event_count"
                ],
                "development_signal_dates": len(
                    contract["development_signal_dates"]
                ),
                "confirmation_events": contract["universe"][
                    "confirmation_event_count"
                ],
                "confirmation_signal_dates": len(
                    contract["confirmation_signal_dates"]
                ),
                "provider_requests": 0,
                "confirmation_outcomes_accessed": False,
                "broker_actions": 0,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

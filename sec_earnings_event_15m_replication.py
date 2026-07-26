"""Disjoint, data-complete temporal replication of the SEC earnings-gap family.

The rejected 2023 version remains immutable.  This successor uses only the
untouched 2024 reserve, splits it prospectively around a five-session embargo,
freezes exact input availability before outcomes, and carries all 32 adverse
prior trial paths into the 64-trial selection correction.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import math
import statistics
from collections import defaultdict
from collections.abc import Mapping, Sequence
from pathlib import Path
from statistics import NormalDist
from typing import Any

import dense_strategy_runtime as runtime
import outcome_exposure
import portfolio_maturity
import sec_earnings_event_15m as predecessor
import strategy_discovery
from gap_protection_successor import CALENDAR_PATH
from historical_store import HistoricalDayStore, canonical_sha256, sha256_file
from learning_data import freeze_dataset_contract
from learning_experiment import (
    DEVELOPMENT_SEARCH_RULE,
    enumerate_trials,
)
from learning_statistics import annualized_sharpe
from scanner_replay import load_calendar


PROJECT_ROOT = Path(__file__).resolve().parent
CAMPAIGN_ID = portfolio_maturity.V2_CAMPAIGN_ID
FAMILY_ID = runtime.SEC_EARNINGS_GAP_15M_REPLICATION_FAMILY
MECHANISM_FAMILY = predecessor.MECHANISM_FAMILY
STRATEGY_ID = FAMILY_ID
SUCCESSOR_ID = FAMILY_ID
DEFAULT_ROOT = PROJECT_ROOT / "strategy_tournament/v2/discovery"
PREDECESSOR_SCOPE = (
    PROJECT_ROOT
    / "strategy_tournament/v2/discovery/"
    "sec-filed-earnings-gap-continuation-event-first-15m/event-scope/"
    "sec-earnings-event-15m-scope-"
    "dd9a25621d38afb3f8831b1daf449f06910a3b133aa6ccaab7780373400d46d4"
    ".json"
)
PREDECESSOR_RESULT = (
    PROJECT_ROOT
    / "strategy_tournament/v2/discovery/"
    "sec-filed-earnings-gap-continuation-event-first-15m/development/"
    "sec-filed-earnings-gap-continuation-event-first-15m-development-"
    "e6402c2355b0ebbec74b607f2edf96750531449bd31b4b5dc8dbaf9a2d356c01"
    ".json"
)
PREDECESSOR_INSPECTION = (
    PROJECT_ROOT
    / "strategy_tournament/v2/discovery/"
    "sec-filed-earnings-gap-continuation-event-first-15m/"
    "development-inspection/"
    "sec-filed-earnings-gap-continuation-event-first-15m-development-inspection-"
    "5583757f06df9342c98fa2d03f2113845beb9f8978c138fbf2d0a327da12abd1"
    ".json"
)
DEVELOPMENT_START = "2024-01-09"
DEVELOPMENT_END = "2024-07-12"
EMBARGO_DATES = [
    "2024-07-15",
    "2024-07-16",
    "2024-07-17",
    "2024-07-18",
    "2024-07-19",
]
CONFIRMATION_START = "2024-07-22"
CONFIRMATION_END = "2024-12-31"
PARAMETER_GRID = predecessor.PARAMETER_GRID


class SecEarningsReplicationError(RuntimeError):
    """The disjoint replication evidence graph is incomplete or drifted."""


def _repo_path(path: Path) -> str:
    return predecessor._repo_path(path)


def _private_scope_path(store: HistoricalDayStore) -> Path:
    return (
        store.root
        / "_derived"
        / "sec_earnings_event_15m"
        / FAMILY_ID
        / "event-scope.json.gz"
    )


def _load_predecessor(
    store: HistoricalDayStore,
    *,
    enforce_commit: bool,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    for path in (
        PREDECESSOR_SCOPE,
        PREDECESSOR_RESULT,
        PREDECESSOR_INSPECTION,
    ):
        if enforce_commit:
            strategy_discovery.require_committed(path)
    public_scope = strategy_discovery.load_artifact(
        PREDECESSOR_SCOPE,
        expected_kind="sec-earnings-event-15m-scope",
    )
    result = strategy_discovery.load_artifact(
        PREDECESSOR_RESULT,
        expected_kind="development-search-result",
    )
    inspection = strategy_discovery.load_artifact(
        PREDECESSOR_INSPECTION,
        expected_kind="development-search-inspection",
    )
    private_scope = predecessor._load_private_scope(store, public_scope)
    if not (
        inspection.get("state") == "REJECTED"
        and inspection.get("confirmation_access_permitted") is False
        and inspection.get("result_sha256") == result["artifact_sha256"]
        and private_scope.get("confirmation_events")
        and result.get("confirmation_access_permitted") is False
    ):
        raise SecEarningsReplicationError(
            "predecessor is not an exact rejected, confirmation-locked source"
        )
    return public_scope, private_scope, result


def _dataset_available(
    document: Mapping[str, Any] | None,
    *,
    current: bool,
) -> bool:
    intraday = predecessor._select(
        document,
        kind="bars",
        channel="trades",
        timeframe="15m",
    )
    if intraday is None or len(intraday.get("rows", [])) != 26:
        return False
    if current:
        return True
    daily = predecessor._select(
        document,
        kind="derived",
        channel="minute_aggregate_regular",
        timeframe="1d",
    )
    return daily is not None and len(daily.get("rows", [])) == 1


def _complete_events(
    store: HistoricalDayStore,
    events: Sequence[Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, str]], int]:
    cache: dict[tuple[str, str, bool], bool] = {}
    retained: list[dict[str, Any]] = []
    unavailable: list[dict[str, str]] = []
    cache_hits = 0
    for raw in events:
        event = dict(raw)
        symbol = str(event["symbol"])
        dates = list(event["observation_dates"])
        if len(dates) != predecessor.LOOKBACK_SESSIONS + 1:
            unavailable.append(
                {
                    "date": str(event["signal_date"]),
                    "symbol": symbol,
                    "reason": "incomplete_frozen_calendar_lookback",
                }
            )
            continue
        complete = True
        for day in dates:
            current = day == event["signal_date"]
            key = (symbol, str(day), current)
            if key in cache:
                cache_hits += 1
            else:
                cache[key] = _dataset_available(
                    store.load(symbol, str(day)),
                    current=current,
                )
            if not cache[key]:
                complete = False
                break
        if complete:
            retained.append(event)
        else:
            unavailable.append(
                {
                    "date": str(event["signal_date"]),
                    "symbol": symbol,
                    "reason": "incomplete_exact_sip_input",
                }
            )
    return retained, unavailable, cache_hits


def _target_scope(events: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    symbols_by_date: dict[str, set[str]] = defaultdict(set)
    for event in events:
        symbols_by_date[str(event["signal_date"])].add(str(event["symbol"]))
    dates = sorted(symbols_by_date)
    return {
        "dates": dates,
        "symbols_by_date": {
            day: sorted(symbols_by_date[day]) for day in dates
        },
    }


def _input_scope(events: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """Cover every date/symbol input that can influence an event outcome."""

    symbols_by_date: dict[str, set[str]] = defaultdict(set)
    for event in events:
        symbol = str(event["symbol"])
        for day in event["observation_dates"]:
            symbols_by_date[str(day)].add(symbol)
    dates = sorted(symbols_by_date)
    return {
        "dates": dates,
        "symbols_by_date": {
            day: sorted(symbols_by_date[day]) for day in dates
        },
    }


def build_capacity_scope(
    *,
    store: HistoricalDayStore | None = None,
    enforce_commit: bool = True,
) -> dict[str, Any]:
    """Freeze data readiness only; never compute prices, gaps, or returns."""

    source = store or HistoricalDayStore.from_env()
    predecessor_scope, private, _result = _load_predecessor(
        source,
        enforce_commit=enforce_commit,
    )
    calendar = load_calendar(CALENDAR_PATH)
    development_dates = predecessor._calendar_slice(
        calendar, DEVELOPMENT_START, DEVELOPMENT_END
    )
    confirmation_dates = predecessor._calendar_slice(
        calendar, CONFIRMATION_START, CONFIRMATION_END
    )
    events = list(private["confirmation_events"])
    development_raw = [
        row
        for row in events
        if DEVELOPMENT_START <= str(row["signal_date"]) <= DEVELOPMENT_END
    ]
    confirmation_raw = [
        row
        for row in events
        if CONFIRMATION_START <= str(row["signal_date"]) <= CONFIRMATION_END
    ]
    development, development_missing, development_cache_hits = _complete_events(
        source, development_raw
    )
    confirmation, confirmation_missing, confirmation_cache_hits = _complete_events(
        source, confirmation_raw
    )
    development_signal_dates = sorted(
        {str(row["signal_date"]) for row in development}
    )
    confirmation_signal_dates = sorted(
        {str(row["signal_date"]) for row in confirmation}
    )
    capacity_ready = (
        len(development_signal_dates) >= 50
        and len(confirmation_signal_dates) >= 20
    )
    development_scope = _input_scope(development)
    confirmation_scope = _input_scope(confirmation)
    outcome_exposure.assert_untouched(
        confirmation_scope,
        outcome_exposure.read_index(),
    )
    private_scope = {
        "schema_version": 1,
        "family_id": FAMILY_ID,
        "development_events": development,
        "confirmation_events": confirmation,
    }
    private_scope["content_sha256"] = canonical_sha256(private_scope)
    path = _private_scope_path(source)
    predecessor._write_gzip(path, private_scope)
    return {
        "schema_version": 1,
        "artifact_kind": "sec-earnings-event-15m-replication-scope",
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "state": (
            "DATA_COMPLETE_CAPACITY_READY"
            if capacity_ready
            else "RETIRED_INSUFFICIENT_CAPACITY"
        ),
        "development_dates": development_dates,
        "development_signal_dates": development_signal_dates,
        "development_event_pairs": len(development),
        "development_unavailable_event_pairs": len(development_missing),
        "development_events_sha256": canonical_sha256(development),
        "development_target_scope": _target_scope(development),
        "development_scope": development_scope,
        "embargo_dates": EMBARGO_DATES,
        "confirmation_dates": confirmation_dates,
        "confirmation_signal_dates": confirmation_signal_dates,
        "confirmation_event_pairs": len(confirmation),
        "confirmation_unavailable_event_pairs": len(confirmation_missing),
        "confirmation_events_sha256": canonical_sha256(confirmation),
        "confirmation_target_scope": _target_scope(confirmation),
        "confirmation_scope": confirmation_scope,
        "private_scope": {
            "format": "json.gz",
            "external_relative_path": str(
                path.resolve().relative_to(source.root.resolve())
            ),
            "external_file_sha256": sha256_file(path),
            "content_sha256": private_scope["content_sha256"],
        },
        "predecessor_scope_path": _repo_path(PREDECESSOR_SCOPE),
        "predecessor_scope_sha256": predecessor_scope["artifact_sha256"],
        "predecessor_inspection_path": _repo_path(PREDECESSOR_INSPECTION),
        "predecessor_inspection_sha256": strategy_discovery.load_artifact(
            PREDECESSOR_INSPECTION,
            expected_kind="development-search-inspection",
        )["artifact_sha256"],
        "availability_cache_hits": (
            development_cache_hits + confirmation_cache_hits
        ),
        "market_price_values_computed": False,
        "strategy_metrics_computed": 0,
        "provider_requests": 0,
        "confirmation_outcomes_accessed": False,
        "broker_actions": 0,
        "outcome_exposure_index_sha256": outcome_exposure.audit()[
            "index_sha256"
        ],
    }


def freeze_capacity(
    *,
    root: Path = DEFAULT_ROOT,
    store: HistoricalDayStore | None = None,
    enforce_commit: bool = True,
) -> tuple[Path, dict[str, Any]]:
    source = store or HistoricalDayStore.from_env()
    payload = build_capacity_scope(
        store=source,
        enforce_commit=enforce_commit,
    )
    return strategy_discovery._write_artifact(
        payload,
        root / SUCCESSOR_ID / "event-scope",
        "sec-earnings-event-15m-replication-scope",
    )


def _load_private_scope(
    store: HistoricalDayStore,
    scope: Mapping[str, Any],
) -> dict[str, Any]:
    binding = scope["private_scope"]
    relative = Path(str(binding["external_relative_path"]))
    if relative.is_absolute() or ".." in relative.parts:
        raise SecEarningsReplicationError(
            "private replication scope path is unsafe"
        )
    path = (store.root / relative).resolve()
    if store.root.resolve() not in path.parents:
        raise SecEarningsReplicationError(
            "private replication scope escaped the historical store"
        )
    value = predecessor._read_gzip(path)
    content = dict(value)
    supplied = content.pop("content_sha256", None)
    if not (
        sha256_file(path) == binding["external_file_sha256"]
        and supplied == binding["content_sha256"]
        and supplied == canonical_sha256(content)
        and canonical_sha256(value["development_events"])
        == scope["development_events_sha256"]
        and canonical_sha256(value["confirmation_events"])
        == scope["confirmation_events_sha256"]
    ):
        raise SecEarningsReplicationError("private replication scope drifted")
    return value


def _prior_statistics(
    store: HistoricalDayStore,
) -> dict[str, Any]:
    result = strategy_discovery.load_artifact(
        PREDECESSOR_RESULT,
        expected_kind="development-search-result",
    )
    binding = result["evaluation_binding"]
    relative = Path(str(binding["relative_path"]))
    if relative.is_absolute() or ".." in relative.parts:
        raise SecEarningsReplicationError("prior evaluation path is unsafe")
    path = (store.root / relative).resolve()
    if store.root.resolve() not in path.parents:
        raise SecEarningsReplicationError(
            "prior evaluation escaped the historical store"
        )
    if sha256_file(path) != binding["file_sha256"]:
        raise SecEarningsReplicationError("prior evaluation file drifted")
    with gzip.open(path, "rt", encoding="utf-8") as source:
        evaluation = json.load(source)
    if canonical_sha256(evaluation) != binding["content_sha256"]:
        raise SecEarningsReplicationError("prior evaluation content drifted")
    current_ids = {
        row["trial_id"] for row in enumerate_trials(PARAMETER_GRID)
    }
    prior = sorted(evaluation["trials"], key=lambda row: row["trial_id"])
    if {row["trial_id"] for row in prior} != current_ids or len(prior) != 32:
        raise SecEarningsReplicationError("prior 32-trial family drifted")
    sharpes: list[float] = []
    p_values: list[float] = []
    paths: dict[str, list[float]] = {}
    for row in prior:
        values = [
            float(value)
            for value in row["metrics"]["oof_daily_account_returns"]
        ]
        paths[str(row["trial_id"])] = values
        sharpes.append(annualized_sharpe(values) or 0.0)
        mean = statistics.fmean(values)
        deviation = statistics.stdev(values) if len(values) > 1 else 0.0
        statistic = (
            mean / (deviation / math.sqrt(len(values)))
            if deviation
            else 0.0
        )
        p_values.append(1 - NormalDist().cdf(statistic))
    return {
        "prior_trial_sharpes": sharpes,
        "prior_trial_p_values": p_values,
        "prior_trial_daily_returns_by_id": paths,
        "prior_pbo_probability": 0.0,
        "prior_selection_trial_count": 32,
    }


def freeze_family(
    *,
    created_at: str,
    root: Path = DEFAULT_ROOT,
    store: HistoricalDayStore | None = None,
    enforce_commit: bool = True,
) -> tuple[Path, dict[str, Any], Path, Path]:
    created = predecessor._timestamp(created_at, "created_at")
    source = store or HistoricalDayStore.from_env()
    implementation_files = [
        "sec_earnings_event_15m_replication.py",
        "sec_earnings_event_15m.py",
        "dense_strategy_plugin.py",
        "dense_strategy_runtime.py",
        "learning_statistics.py",
        "learning_experiment.py",
        "strategy_discovery.py",
        "outcome_exposure.py",
        "portfolio_maturity.py",
        "portfolio_config.toml",
    ]
    if enforce_commit:
        for relative in implementation_files:
            strategy_discovery.require_committed(PROJECT_ROOT / relative)
    scope_path, scope = freeze_capacity(
        root=root,
        store=source,
        enforce_commit=enforce_commit,
    )
    if scope["state"] != "DATA_COMPLETE_CAPACITY_READY":
        raise SecEarningsReplicationError(
            "replication retired with "
            f"{len(scope['development_signal_dates'])} development and "
            f"{len(scope['confirmation_signal_dates'])} confirmation "
            "data-complete signal dates"
        )
    capacity_path, _capacity = freeze_dataset_contract(
        {
            "schema_version": 1,
            "dataset_id": f"dataset-{SUCCESSOR_ID}-capacity",
            "registered_at": created,
            "requested_dates": scope["development_dates"],
            "dataset_payload": {
                "lane": "development",
                "claim_scope": "DEVELOPMENT_ONLY",
                "evidence_phase": "OUTCOME_BLIND_DATA_COMPLETE_CAPACITY",
                "inspected": True,
                "point_in_time_evidence": True,
                "evidence_paths": [
                    _repo_path(PREDECESSOR_SCOPE),
                    _repo_path(PREDECESSOR_RESULT),
                    _repo_path(PREDECESSOR_INSPECTION),
                    _repo_path(scope_path),
                    _repo_path(CALENDAR_PATH),
                    "strategy_tournament/v2/OUTCOME_EXPOSURE_INDEX.jsonl",
                ],
                "dense_capacity": {
                    "family_id": FAMILY_ID,
                    "formal_capacity": len(
                        scope["development_signal_dates"]
                    ),
                    "development_event_pairs": scope[
                        "development_event_pairs"
                    ],
                    "confirmation_signal_capacity": len(
                        scope["confirmation_signal_dates"]
                    ),
                    "confirmation_event_pairs": scope[
                        "confirmation_event_pairs"
                    ],
                    "provider_requests": 0,
                    "market_price_values_computed": False,
                },
            },
        },
        root / SUCCESSOR_ID / "capacity",
    )
    prior = _prior_statistics(source)
    contract: dict[str, Any] = {
        "schema_version": 1,
        "campaign_id": CAMPAIGN_ID,
        "experiment_id": f"experiment-{SUCCESSOR_ID}",
        "family_id": FAMILY_ID,
        "mechanism_family": MECHANISM_FAMILY,
        "strategy_id": STRATEGY_ID,
        "created_at": created,
        "status": "INVENTED",
        "dataset_lane": "development",
        "research_generation": "disjoint_temporal_replication",
        "new_mechanism_family_slot_consumed": False,
        "mechanism": (
            "Point-in-time SEC Item 2.02 earnings filings can create "
            "continuing price discovery when a liquid common stock gaps "
            "higher and the first completed 15-minute interval confirms demand."
        ),
        "expected_holding_behavior": (
            "Long only from the 09:45 ET next observable 15-minute open "
            "until the structural stop, frozen R target, or 15:45 ET cutoff."
        ),
        "entry_rule": (
            "Inside the prospectively frozen data-complete event universe, "
            "require prior close at least $10, prior 20-session median dollar "
            "volume at least $50 million, and the frozen positive-gap, "
            "bullish close-location, and opening-volume-ratio gates."
        ),
        "stop_rule": (
            "Use the completed 09:30-09:45 low; reject a nonpositive, "
            "nonprotective, or cap-exceeding structural stop."
        ),
        "exit_rule": (
            "Resolve gaps, stop, then target on each later 15-minute interval "
            "with stop-first ambiguity; otherwise exit at the 15:45 bar open."
        ),
        "ranking_rule": (
            "Largest positive gap, opening-volume ratio, prior median dollar "
            "volume, symbol, then canonical event ID."
        ),
        "selection_rule": (
            "At most one entry per day. Input completeness is frozen before "
            "outcomes; any later drift is a missed trade with no substitution."
        ),
        "primary_outcome": (
            "Prior-trial-adjusted chronological account log growth after "
            "5/10/20-bps-per-side costs."
        ),
        "material_difference_rationale": (
            "The rejected 2023 version blocked nearly every day when any "
            "source event lacked inputs. This temporal replication freezes "
            "data-complete event membership before outcomes on disjoint 2024 "
            "evidence and carries all 32 adverse prior trial paths."
        ),
        "universe_requirements": {
            "security_type": "point-in-time U.S. common stock",
            "sec_form": "8-K",
            "required_item": "2.02",
            "acceptance_cutoff_et": "09:25:00",
            "exact_current_and_prior_20_session_sip_inputs_required": True,
            "availability_frozen_before_outcomes": True,
        },
        "execution_assumptions": {
            "next_observable_fill": "09:45:00_ET_open",
            "same_interval_ambiguity": "stop_first",
            "maximum_hold_sessions": 1,
            "postfreeze_missing_data": "missed_trade_no_substitute",
            "minimum_gross_to_primary_round_trip_cost": 5.0,
            "force_flat_et": "15:45:00",
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
            "All 32 rejected 2023 paths are carried into selection correction.",
            "Development and confirmation use disjoint untouched 2024 dates.",
            "Input availability is frozen without computing price features or returns.",
        ],
        "production_compatibility_risks": [
            "Live SEC capture, exact event ranking, fresh market gates, protection, and reconciliation remain mandatory."
        ],
        "parameter_grid": PARAMETER_GRID,
        "selection_mode": "development_search",
        "winner_selection": DEVELOPMENT_SEARCH_RULE,
        **prior,
        "development_dates": scope["development_dates"],
        "development_signal_dates": scope["development_signal_dates"],
        "embargo_dates": scope["embargo_dates"],
        "confirmation_dates": scope["confirmation_dates"],
        "confirmation_signal_dates": scope["confirmation_signal_dates"],
        "confirmation_signal_capacity": len(
            scope["confirmation_signal_dates"]
        ),
        "development_scope": scope["development_scope"],
        "confirmation_scope": scope["confirmation_scope"],
        "outcome_exposure_index_sha256": scope[
            "outcome_exposure_index_sha256"
        ],
        "universe": {
            "identity": (
                "data-complete point-in-time SEC Item 2.02 common-stock events"
            ),
            "event_scope_path": _repo_path(scope_path),
            "event_scope_sha256": scope["artifact_sha256"],
            "point_in_time": True,
        },
        "costs_bps_per_side": [5, 10, 20],
        "partitions": {
            "rolling_origin": True,
            "confirmation_untouched": True,
            "disjoint_temporal_replication": True,
            "confirmation_embargo_sessions": 5,
        },
        "falsifiers": [
            "nonpositive stressed log growth",
            "stressed profit-factor or drawdown failure",
            "unstable one-step parameter neighbors",
            "prior-trial-adjusted DSR, Holm, or PBO rejection",
            "incomplete execution or trial accounting",
            "insufficient frozen confirmation power capacity",
        ],
        "historical_data_contract": {
            "provider": "alpaca",
            "feed": "sip",
            "adjustment": "raw",
            "bar_timeframe": "15m",
            "substitutions_allowed": False,
            "market_price_values_before_search_freeze": False,
            "confirmation_access_before_winner_freeze": False,
        },
        "implementation_files": implementation_files,
        "plugin": {
            "module": "dense_strategy_plugin",
            "preflight": "preflight",
            "evaluate_development": "evaluate_development",
            "evaluate_confirmation": "evaluate_confirmation",
            "evaluate_production": "evaluate_production",
        },
        "capacity_policy": {"retire_below": 50, "fast_lane_at": 100},
        "capacity_manifest": _repo_path(capacity_path),
        "event_scope_path": _repo_path(scope_path),
        "event_scope_sha256": scope["artifact_sha256"],
        "predecessor_result_sha256": strategy_discovery.load_artifact(
            PREDECESSOR_RESULT,
            expected_kind="development-search-result",
        )["artifact_sha256"],
        "predecessor_inspection_sha256": strategy_discovery.load_artifact(
            PREDECESSOR_INSPECTION,
            expected_kind="development-search-inspection",
        )["artifact_sha256"],
    }
    validated = strategy_discovery._validate_family_contract(contract)
    digest = hashlib.sha256(
        json.dumps(validated, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    path = (
        root
        / SUCCESSOR_ID
        / "family-contract"
        / f"contract-{digest}.json"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(validated, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return path, validated, capacity_path, scope_path


def build_development_dataset(
    *,
    search_path: Path,
    store: HistoricalDayStore | None = None,
    enforce_commit: bool = True,
) -> tuple[dict[str, Any], dict[str, Any]]:
    source = store or HistoricalDayStore.from_env()
    if enforce_commit:
        strategy_discovery.require_committed(search_path)
    search = strategy_discovery.load_artifact(
        search_path,
        expected_kind="frozen-development-search",
    )
    contract = search.get("family_contract")
    if not (
        isinstance(contract, Mapping)
        and contract.get("family_id") == FAMILY_ID
    ):
        raise SecEarningsReplicationError("search family binding drifted")
    scope_path = PROJECT_ROOT / str(contract["event_scope_path"])
    if enforce_commit:
        strategy_discovery.require_committed(scope_path)
    scope = strategy_discovery.load_artifact(
        scope_path,
        expected_kind="sec-earnings-event-15m-replication-scope",
    )
    private = _load_private_scope(source, scope)
    events = predecessor._deduplicate_events(private["development_events"])
    calendar = list(contract["development_dates"])
    full_calendar = load_calendar(CALENDAR_PATH)
    positions = {day: index for index, day in enumerate(full_calendar)}
    feature_cache: dict[
        tuple[str, str], tuple[dict[str, Any], float] | None
    ] = {}
    metadata: dict[str, list[dict[str, Any]]] = {day: [] for day in calendar}
    bars_by_date: dict[str, dict[str, list[dict[str, Any]]]] = {}
    unavailable: list[dict[str, str]] = []
    for day in contract["development_signal_dates"]:
        for event in events.get(day, []):
            symbol = str(event["symbol"])
            index = positions[day]
            prior_dates = full_calendar[
                index - predecessor.LOOKBACK_SESSIONS : index
            ]
            prior: list[tuple[dict[str, Any], float]] = []
            for prior_day in prior_dates:
                key = (symbol, prior_day)
                if key not in feature_cache:
                    feature_cache[key] = predecessor._daily_and_opening(
                        source, symbol, prior_day
                    )
                value = feature_cache[key]
                if value is None:
                    unavailable.append(
                        {
                            "date": day,
                            "symbol": symbol,
                            "reason": "postfreeze_input_drift",
                        }
                    )
                    break
                prior.append(value)
            else:
                bars = predecessor._current_bars(source, symbol, day)
                if bars is None:
                    unavailable.append(
                        {
                            "date": day,
                            "symbol": symbol,
                            "reason": "postfreeze_input_drift",
                        }
                    )
                    continue
                prior_close = float(prior[-1][0]["close"])
                median_dollar_volume = statistics.median(
                    float(daily["close"]) * float(daily["volume"])
                    for daily, _opening in prior
                )
                median_opening_volume = statistics.median(
                    opening for _daily, opening in prior
                )
                opening = bars[0]
                opening_range = float(opening["high"]) - float(opening["low"])
                metadata[day].append(
                    {
                        "accepted_at": str(event["accepted_at"]),
                        "event_id": str(event["event_id"]),
                        "gap_fraction": (
                            float(opening["open"]) / prior_close - 1
                        ),
                        "instrument_id": str(event["instrument_id"]),
                        "opening_bullish": (
                            float(opening["close"]) > float(opening["open"])
                        ),
                        "opening_close_location": (
                            (
                                float(opening["close"])
                                - float(opening["low"])
                            )
                            / opening_range
                            if opening_range > 0
                            else 0.0
                        ),
                        "opening_volume_ratio": (
                            float(opening["volume"])
                            / median_opening_volume
                            if median_opening_volume > 0
                            else 0.0
                        ),
                        "prior_close": prior_close,
                        "prior_median_dollar_volume": median_dollar_volume,
                        "symbol": symbol,
                    }
                )
                bars_by_date.setdefault(day, {})[symbol] = bars
        metadata[day].sort(key=lambda row: str(row["symbol"]))
    blocked_dates = sorted({row["date"] for row in unavailable})
    dataset = {
        "schema_version": 1,
        "family_id": FAMILY_ID,
        "evaluation_dates": calendar,
        "signal_dates": list(contract["development_signal_dates"]),
        "event_metadata_by_date": metadata,
        "fifteen_minute_bars": bars_by_date,
        "blocked_dates": blocked_dates,
        "source_semantics": {
            "provider": "alpaca",
            "feed": "sip",
            "adjustment": "raw",
            "timeframe": "15m",
            "availability_frozen_before_outcomes": True,
        },
    }
    runtime.prepare_dataset(dataset)
    return dataset, {
        "frozen_event_pairs": sum(len(rows) for rows in events.values()),
        "materialized_event_pairs": sum(len(rows) for rows in metadata.values()),
        "postfreeze_unavailable_event_pairs": len(unavailable),
        "blocked_dates": blocked_dates,
        "provider_requests": 0,
        "confirmation_files_opened": 0,
    }


def publish_development(
    *,
    search_path: Path,
    registered_at: str,
    root: Path = DEFAULT_ROOT,
    store: HistoricalDayStore | None = None,
) -> tuple[Path, dict[str, Any], dict[str, Any]]:
    registered = predecessor._timestamp(registered_at, "registered_at")
    source = store or HistoricalDayStore.from_env()
    dataset, accounting = build_development_dataset(
        search_path=search_path,
        store=source,
    )
    search = strategy_discovery.load_artifact(
        search_path,
        expected_kind="frozen-development-search",
    )
    external = (
        source.root
        / "_derived"
        / "sec_earnings_event_15m"
        / FAMILY_ID
        / f"development-{search['artifact_sha256']}.json.gz"
    )
    predecessor._write_gzip(external, dataset)
    manifest_path, manifest = freeze_dataset_contract(
        {
            "schema_version": 1,
            "dataset_id": (
                f"dataset-{FAMILY_ID}-development-"
                f"{search['artifact_sha256'][:16]}"
            ),
            "registered_at": registered,
            "requested_dates": dataset["evaluation_dates"],
            "dataset_payload": {
                "lane": "development",
                "claim_scope": "DEVELOPMENT_ONLY",
                "inspected": True,
                "point_in_time_evidence": True,
                "development_search_sha256": search["artifact_sha256"],
                "evidence_paths": [
                    _repo_path(search_path),
                    str(search["preflight_path"]),
                    str(search["family_contract"]["event_scope_path"]),
                    _repo_path(PREDECESSOR_INSPECTION),
                    _repo_path(CALENDAR_PATH),
                ],
                "dense_runtime": {
                    "family_id": FAMILY_ID,
                    "format": "json.gz",
                    "external_relative_path": str(
                        external.resolve().relative_to(source.root.resolve())
                    ),
                    "external_file_sha256": sha256_file(external),
                    "dataset_sha256": canonical_sha256(dataset),
                    "formal_capacity": len(dataset["signal_dates"]),
                    "provider_requests": 0,
                    "confirmation_files_opened": 0,
                },
                "collection_accounting": accounting,
            },
        },
        root / FAMILY_ID / "development-dataset",
    )
    return manifest_path, manifest, accounting


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("freeze-capacity")
    freeze = sub.add_parser("freeze-family")
    freeze.add_argument("--created-at", required=True)
    publish = sub.add_parser("publish-development")
    publish.add_argument("--search", type=Path, required=True)
    publish.add_argument("--registered-at", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "freeze-capacity":
            path, scope = freeze_capacity()
            result: Mapping[str, Any] = {
                "path": _repo_path(path),
                "state": scope["state"],
                "development_event_pairs": scope[
                    "development_event_pairs"
                ],
                "development_signal_dates": len(
                    scope["development_signal_dates"]
                ),
                "confirmation_event_pairs": scope[
                    "confirmation_event_pairs"
                ],
                "confirmation_signal_dates": len(
                    scope["confirmation_signal_dates"]
                ),
                "market_price_values_computed": scope[
                    "market_price_values_computed"
                ],
            }
        elif args.command == "freeze-family":
            path, contract, capacity, scope = freeze_family(
                created_at=args.created_at
            )
            result = {
                "path": _repo_path(path),
                "capacity_manifest": _repo_path(capacity),
                "event_scope": _repo_path(scope),
                "trial_count": len(contract["trial_family"]),
                "prior_trial_count": contract[
                    "prior_selection_trial_count"
                ],
                "development_signal_dates": len(
                    contract["development_signal_dates"]
                ),
                "confirmation_signal_capacity": contract[
                    "confirmation_signal_capacity"
                ],
            }
        else:
            path, manifest, accounting = publish_development(
                search_path=args.search,
                registered_at=args.registered_at,
            )
            result = {
                "path": _repo_path(path),
                "manifest_sha256": manifest["manifest_sha256"],
                "accounting": accounting,
            }
    except (
        SecEarningsReplicationError,
        predecessor.SecEarningsEvent15mError,
        strategy_discovery.StrategyDiscoveryError,
        outcome_exposure.OutcomeExposureError,
        OSError,
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
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

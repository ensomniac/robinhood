"""Freeze the v13 SEC reaction search on the inspected 2012-2015 inventory."""

from __future__ import annotations

import argparse
import copy
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
from urllib.parse import quote

import dense_strategy_runtime as runtime
import earnings_sec_corrected_expansion as capacity
import earnings_sec_market_data as market
import earnings_sec_reaction_v11_search as v11
import earnings_sec_yahoo_data as yahoo
import outcome_exposure
import strategy_discovery
from historical_store import (
    HistoricalDayStore,
    canonical_sha256,
    sha256_file,
)
from learning_data import freeze_dataset_contract
from learning_statistics import (
    annualized_sharpe,
    probability_of_backtest_overfitting,
)


PROJECT_ROOT = Path(__file__).resolve().parent
CAMPAIGN_ID = capacity.CAMPAIGN_ID
FAMILY_ID = runtime.EARNINGS_SEC_REACTION_FAMILY
SUCCESSOR_ID = capacity.SUCCESSOR_ID
STRATEGY_ID = v11.STRATEGY_ID
MECHANISM_FAMILY = v11.MECHANISM_FAMILY
DEFAULT_ROOT = capacity.DEFAULT_ROOT
CAPACITY_INSPECTION = (
    capacity.DEFAULT_ROOT
    / "metadata-collection-inspection"
    / "inspection-"
    "4eb73e802bd925e6a67f55e2096c5dea3b82d2670d4870e5c5b98e1aea11fb5c"
    ".json"
)
PRIOR_SEARCH = (
    PROJECT_ROOT
    / "strategy_tournament/v2/discovery"
    / FAMILY_ID
    / "search"
    / (
        f"{FAMILY_ID}-search-"
        "cd2e32ff3a9af0f46e601644710594982863a33eeb3a9c77fb87179251722671"
        ".json"
    )
)
PRIOR_RESULT = (
    PROJECT_ROOT
    / "strategy_tournament/v2/discovery"
    / FAMILY_ID
    / "development"
    / (
        f"{FAMILY_ID}-development-"
        "40b4414cf53b173980c7f54840816d6a5514972c555c6ae1e7da2f8b143d2a4f"
        ".json"
    )
)
PRIOR_INSPECTION = (
    PROJECT_ROOT
    / "strategy_tournament/v2/discovery"
    / FAMILY_ID
    / "development-inspection"
    / (
        f"{FAMILY_ID}-development-inspection-"
        "01b19f2081cedfe25f4fd30ed0c556b340a2bae742ab64c229aaac0a22c6f6df"
        ".json"
    )
)
WARMUP_START = date(2011, 11, 1)
DEVELOPMENT_START = date(2012, 1, 3)
DEVELOPMENT_END = date(2014, 12, 30)
EMBARGO_START = date(2014, 12, 31)
EMBARGO_END = date(2015, 1, 9)
CONFIRMATION_WARMUP_START = date(2014, 11, 3)
CONFIRMATION_START = date(2015, 1, 12)
CONFIRMATION_END = date(2016, 1, 8)
EXPECTED_DEVELOPMENT_EVENTS = 1063
EXPECTED_DEVELOPMENT_SIGNAL_DATES = 507
EXPECTED_DEVELOPMENT_SYMBOLS = 635
EXPECTED_CONFIRMATION_EVENTS = 201
EXPECTED_CONFIRMATION_SIGNAL_DATES = 118
EXPECTED_CONFIRMATION_SYMBOLS = 178


class EarningsSecReactionV13SearchError(RuntimeError):
    """The v13 SEC reaction search boundary drifted."""


def _event_row(event: Mapping[str, Any], reaction: str) -> dict[str, Any]:
    return {
        "adsh": str(event["adsh"]),
        "symbol": str(event["ticker"]),
        "accepted": str(event["accepted"]),
        "accepted_date": str(event["accepted"])[:10],
        "report_period": str(event["period"]),
        "reaction_date": reaction,
        "current_eps": float(event["current_eps"]),
        "prior_eps": float(event["prior_year_eps"]),
        "eps_change": float(event["eps_yoy_change"]),
        "eps_change_ratio": float(event["eps_yoy_change_ratio"]),
        "security_identity_state": str(event["security_identity_state"]),
    }


def _request(symbol: str, *, start: str, end: str) -> dict[str, Any]:
    value: dict[str, Any] = {
        "method": "GET",
        "endpoint": yahoo.ENDPOINT_TEMPLATE.format(
            symbol=quote(symbol, safe="")
        ),
        "parameters": {
            "period1": yahoo._period(start),
            "period2": yahoo._exclusive_period(end),
            "interval": "1d",
            "events": "history",
            "includeAdjustedClose": "true",
        },
        "symbol": symbol,
        "start": start,
        "end": end,
    }
    value["request_sha256"] = capacity.self_hash(
        value, "request_sha256"
    )
    return value


def _capacity_events(
    store: HistoricalDayStore,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    strategy_discovery.require_committed(CAPACITY_INSPECTION)
    inspection = capacity._read(CAPACITY_INSPECTION)
    collection_path = PROJECT_ROOT / str(inspection["collection_path"])
    strategy_discovery.require_committed(collection_path)
    collection = capacity._read(collection_path)
    if not (
        inspection.get("inspection_sha256")
        == capacity.self_hash(inspection, "inspection_sha256")
        and inspection.get("state")
        == "SEC_CORRECTED_EXPANSION_CAPACITY_READY"
        and inspection.get("valid") is True
        and inspection.get("development_events")
        == EXPECTED_DEVELOPMENT_EVENTS
        and inspection.get("development_signal_dates")
        == EXPECTED_DEVELOPMENT_SIGNAL_DATES
        and inspection.get("confirmation_events") == 369
        and inspection.get("confirmation_signal_dates") == 176
        and inspection.get("development_market_price_access_authorized")
        is True
        and inspection.get("confirmation_market_price_access_authorized")
        is False
        and inspection.get("collection_sha256")
        == collection.get("collection_sha256")
        and collection.get("collection_sha256")
        == capacity.self_hash(collection, "collection_sha256")
    ):
        raise EarningsSecReactionV13SearchError(
            "v13 capacity lineage is invalid"
        )
    info = collection["private_artifact"]
    path = store.root / str(info["cache_relative_path"])
    raw = path.read_bytes()
    if hashlib.sha256(raw).hexdigest() != info["file_sha256"]:
        raise EarningsSecReactionV13SearchError(
            "v13 private metadata artifact hash differs"
        )
    value = json.loads(gzip.decompress(raw))
    if not (
        isinstance(value, dict)
        and value.get("content_sha256") == info["content_sha256"]
        and value.get("content_sha256")
        == capacity.self_hash(value, "content_sha256")
        and len(value.get("events", [])) == 1444
    ):
        raise EarningsSecReactionV13SearchError(
            "v13 private metadata content differs"
        )
    return inspection, [dict(row) for row in value["events"]]


def selection(
    store: HistoricalDayStore | None = None,
) -> dict[str, Any]:
    historical_store = store or HistoricalDayStore.from_env()
    capacity_inspection, events = _capacity_events(historical_store)
    full_calendar = market._sessions(
        WARMUP_START, CONFIRMATION_END
    )
    development_dates = market._sessions(
        DEVELOPMENT_START, DEVELOPMENT_END
    )
    development_opened_dates = market._sessions(
        WARMUP_START, DEVELOPMENT_END
    )
    embargo_dates = market._sessions(EMBARGO_START, EMBARGO_END)
    confirmation_dates = market._sessions(
        CONFIRMATION_START, CONFIRMATION_END
    )
    confirmation_opened_dates = market._sessions(
        CONFIRMATION_WARMUP_START, CONFIRMATION_END
    )
    development_metadata = {day: [] for day in development_dates}
    development_events = [
        event
        for event in events
        if capacity.DEVELOPMENT_START
        <= str(event["accepted"])[:10]
        <= capacity.DEVELOPMENT_END
    ]
    development_symbols = sorted(
        {str(event["ticker"]) for event in development_events}
    )
    for event in development_events:
        reaction = market._reaction_date(
            str(event["accepted"]), full_calendar
        )
        if reaction in development_metadata:
            development_metadata[reaction].append(
                _event_row(event, reaction)
            )
    confirmation_metadata = {day: [] for day in confirmation_dates}
    confirmation_events = [
        event
        for event in events
        if capacity.CONFIRMATION_START
        <= str(event["accepted"])[:10]
        <= capacity.CONFIRMATION_END
        and str(event["ticker"]) not in set(development_symbols)
    ]
    for event in confirmation_events:
        reaction = market._reaction_date(
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
    development_signal_dates = [
        day for day, rows in development_metadata.items() if rows
    ]
    confirmation_signal_dates = [
        day for day, rows in confirmation_metadata.items() if rows
    ]
    confirmation_symbols = sorted(
        {
            str(row["symbol"])
            for rows in confirmation_metadata.values()
            for row in rows
        }
    )
    development_requests = [
        _request(
            symbol,
            start=development_opened_dates[0],
            end=development_opened_dates[-1],
        )
        for symbol in development_symbols
    ]
    confirmation_requests = [
        _request(
            symbol,
            start=confirmation_opened_dates[0],
            end=confirmation_opened_dates[-1],
        )
        for symbol in confirmation_symbols
    ]
    if not (
        len(development_events) == EXPECTED_DEVELOPMENT_EVENTS
        and len(development_signal_dates)
        == EXPECTED_DEVELOPMENT_SIGNAL_DATES
        and len(development_symbols) == EXPECTED_DEVELOPMENT_SYMBOLS
        and len(development_requests) == EXPECTED_DEVELOPMENT_SYMBOLS
        and len(confirmation_events) == EXPECTED_CONFIRMATION_EVENTS
        and len(confirmation_signal_dates)
        == EXPECTED_CONFIRMATION_SIGNAL_DATES
        and len(confirmation_symbols) == EXPECTED_CONFIRMATION_SYMBOLS
        and len(confirmation_requests) == EXPECTED_CONFIRMATION_SYMBOLS
        and not set(development_symbols).intersection(confirmation_symbols)
        and len(embargo_dates) >= 5
    ):
        raise EarningsSecReactionV13SearchError(
            "v13 event partitions or request graph differ"
        )
    return {
        "schema_version": 1,
        "family_id": FAMILY_ID,
        "successor_id": SUCCESSOR_ID,
        "capacity_inspection_sha256": capacity_inspection[
            "inspection_sha256"
        ],
        "development_dates": development_dates,
        "development_signal_dates": development_signal_dates,
        "development_event_count": len(development_events),
        "development_symbols": development_symbols,
        "development_metadata_by_date": development_metadata,
        "development_opened_dates": development_opened_dates,
        "development_requests": development_requests,
        "embargo_dates": embargo_dates,
        "confirmation_dates": confirmation_dates,
        "confirmation_signal_dates": confirmation_signal_dates,
        "confirmation_event_count": len(confirmation_events),
        "confirmation_symbols": confirmation_symbols,
        "confirmation_metadata_by_date": confirmation_metadata,
        "confirmation_opened_dates": confirmation_opened_dates,
        "confirmation_requests": confirmation_requests,
        "confirmation_symbol_disjoint_from_development": True,
        "market_prices_accessed": False,
        "strategy_metrics_computed": 0,
        "confirmation_prices_accessed": False,
        "broker_actions": 0,
    }


def prior_statistics() -> dict[str, Any]:
    for path in (PRIOR_SEARCH, PRIOR_RESULT, PRIOR_INSPECTION):
        strategy_discovery.require_committed(path)
    prior_search = strategy_discovery.load_artifact(
        PRIOR_SEARCH, expected_kind="frozen-development-search"
    )
    result = strategy_discovery.load_artifact(
        PRIOR_RESULT, expected_kind="development-search-result"
    )
    inspection = strategy_discovery.load_artifact(
        PRIOR_INSPECTION, expected_kind="development-search-inspection"
    )
    if not (
        prior_search.get("artifact_sha256")
        == result.get("search_sha256")
        and result.get("artifact_sha256") == inspection.get("result_sha256")
        and inspection.get("state") == "REJECTED"
        and inspection.get("selection", {}).get("status") == "REJECTED"
        and prior_search.get("trial_count") == 32
    ):
        raise EarningsSecReactionV13SearchError(
            "prior 32-trial selection lineage is invalid"
        )
    evaluation = strategy_discovery._load_development_evaluation(
        result, root=strategy_discovery.DEFAULT_ROOT
    )
    trials = sorted(evaluation["trials"], key=lambda row: row["trial_id"])
    daily_returns_by_id: dict[str, list[float]] = {}
    sharpes: list[float] = []
    p_values: list[float] = []
    trial_ids: list[str] = []
    for trial in trials:
        trial_id = str(trial["trial_id"])
        values = [
            float(value)
            for value in trial["metrics"]["oof_daily_account_returns"]
        ]
        mean = statistics.fmean(values)
        deviation = statistics.stdev(values) if len(values) > 1 else 0.0
        statistic = (
            mean / (deviation / math.sqrt(len(values)))
            if deviation
            else 0.0
        )
        trial_ids.append(trial_id)
        daily_returns_by_id[trial_id] = values
        sharpes.append(float(annualized_sharpe(values) or 0.0))
        p_values.append(1 - NormalDist().cdf(statistic))
    standalone_pbo = probability_of_backtest_overfitting(
        daily_returns_by_id
    )["probability"]
    if not (
        len(trial_ids) == 32
        and len(set(trial_ids)) == 32
        and set(trial_ids)
        == {
            str(row["trial_id"])
            for row in prior_search["family_contract"]["trial_family"]
        }
        and standalone_pbo is not None
    ):
        raise EarningsSecReactionV13SearchError(
            "prior selection statistics are incomplete"
        )
    return {
        "prior_search_path": market._repo_path(PRIOR_SEARCH),
        "prior_search_file_sha256": sha256_file(PRIOR_SEARCH),
        "prior_search_sha256": prior_search["artifact_sha256"],
        "prior_result_path": market._repo_path(PRIOR_RESULT),
        "prior_result_file_sha256": sha256_file(PRIOR_RESULT),
        "prior_result_sha256": result["artifact_sha256"],
        "prior_inspection_path": market._repo_path(PRIOR_INSPECTION),
        "prior_inspection_file_sha256": sha256_file(PRIOR_INSPECTION),
        "prior_inspection_sha256": inspection["artifact_sha256"],
        "prior_evaluation_binding": dict(result["evaluation_binding"]),
        "prior_trial_ids": trial_ids,
        "prior_trial_sharpes": sharpes,
        "prior_trial_p_values": p_values,
        "prior_trial_daily_returns_by_id": daily_returns_by_id,
        "prior_standalone_pbo_probability": float(standalone_pbo),
        "cumulative_pbo_method": (
            "concatenate matching parameter OOF paths across disjoint corpora"
        ),
    }


def _scope(
    dates: Sequence[str], symbols: Sequence[str]
) -> dict[str, list[str]]:
    return {"dates": list(dates), "symbols": list(symbols)}


def build_contract(
    *,
    created_at: str,
    capacity_manifest: Path,
    store: HistoricalDayStore | None = None,
    selected_override: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    selected = (
        dict(selected_override)
        if selected_override is not None
        else selection(store)
    )
    prior = prior_statistics()
    strategy_discovery.require_committed(PRIOR_SEARCH)
    frozen_prior = strategy_discovery.load_artifact(
        PRIOR_SEARCH, expected_kind="frozen-development-search"
    )
    contract = copy.deepcopy(frozen_prior["family_contract"])
    for field in (
        "implementation_hashes",
        "trial_family",
        "primary_trial_id",
        "rolling_origin_plan",
        "prior_trial_sharpes",
        "prior_trial_p_values",
        "prior_trial_daily_returns_by_id",
    ):
        contract.pop(field, None)
    development_scope = _scope(
        selected["development_opened_dates"],
        selected["development_symbols"],
    )
    confirmation_scope = _scope(
        selected["confirmation_opened_dates"],
        selected["confirmation_symbols"],
    )
    index = outcome_exposure.read_index()
    outcome_exposure.assert_untouched(development_scope, index)
    outcome_exposure.assert_untouched(confirmation_scope, index)
    outcome_exposure.assert_disjoint(
        [development_scope, confirmation_scope]
    )
    contract.update(
        {
            "experiment_id": f"experiment-{SUCCESSOR_ID}",
            "parent_experiment_id": frozen_prior["family_contract"][
                "experiment_id"
            ],
            "created_at": market._timestamp(created_at, "created_at"),
            "status": "INVENTED",
            "material_difference_rationale": (
                "The prior exact search was independently underpowered with "
                "at most seven OOF fills per trial. V13 preserves its exact "
                "reaction-confirmed 32-trial grid on 1,063 point-in-time, "
                "date-symbol-disjoint 2012-2014 events and carries all prior "
                "trial statistics into cumulative selection correction."
            ),
            "selection_rule": (
                "The complete new 32-trial family uses frozen DSR and Holm "
                "corrections across 64 current-plus-prior trials, cumulative "
                "PBO from matching parameter paths concatenated across the "
                "disjoint OOF corpora, plus neighbor, stress, and account gates."
            ),
            "contamination_risks": [
                "The prior 32 evaluated trials are adverse history and enter every cumulative correction.",
                "Development uses only globally untouched 2011-2014 date-symbol price scope.",
                "Confirmation symbols are absent from development so their 2014 warmup remains untouched.",
                "Confirmation prices remain inaccessible before an exact winner freeze.",
            ],
            "development_dates": selected["development_dates"],
            "development_signal_dates": selected[
                "development_signal_dates"
            ],
            "embargo_dates": selected["embargo_dates"],
            "confirmation_dates": selected["confirmation_dates"],
            "confirmation_signal_dates": selected[
                "confirmation_signal_dates"
            ],
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
                "excluded_symbols": [],
                "symbols": selected["development_symbols"],
                "selection_sha256": canonical_sha256(selected),
                "development_event_count": selected[
                    "development_event_count"
                ],
                "confirmation_event_count": selected[
                    "confirmation_event_count"
                ],
                "confirmation_symbols": selected[
                    "confirmation_symbols"
                ],
                "confirmation_symbol_disjoint_from_development": True,
            },
            "development_data_requests": selected[
                "development_requests"
            ],
            "development_data_policy": {
                "source": "Yahoo Finance historical chart JSON",
                "authorized_requests": len(
                    selected["development_requests"]
                ),
                "pacing_seconds": yahoo.PACE_SECONDS,
                "retries": 0,
                "substitutions": 0,
                "http_400": "permanent_missing_zero_credit",
                "http_404": "permanent_missing_zero_credit",
                "identity_schema_mismatch": (
                    "permanent_missing_zero_credit"
                ),
                "other_http_or_schema_error": "fail_closed",
                "confirmation_requests": 0,
            },
            "confirmation_data_reserve": {
                "source": "Yahoo Finance historical chart JSON",
                "authorized_only_after_frozen_winner": True,
                "request_count": len(selected["confirmation_requests"]),
                "request_graph_sha256": canonical_sha256(
                    selected["confirmation_requests"]
                ),
                "symbol_disjoint_from_development": True,
            },
            "selection_accounting": {
                "current_trial_count": 32,
                "prior_evaluated_trial_count": 32,
                "cumulative_trial_count": 64,
                "deflated_sharpe_trial_count": 64,
                "holm_p_value_count": 64,
                "cumulative_pbo_method": prior[
                    "cumulative_pbo_method"
                ],
                "prior_standalone_pbo_is_not_irreversible_veto": True,
            },
            "prior_selection_lineage": {
                key: value
                for key, value in prior.items()
                if key
                not in {
                    "prior_trial_sharpes",
                    "prior_trial_p_values",
                    "prior_trial_daily_returns_by_id",
                }
            },
            "prior_trial_sharpes": prior["prior_trial_sharpes"],
            "prior_trial_p_values": prior["prior_trial_p_values"],
            "prior_trial_daily_returns_by_id": prior[
                "prior_trial_daily_returns_by_id"
            ],
            "prior_pbo_probability": 0.0,
            "falsifiers": [
                "nonpositive stressed log growth",
                "unstable parameter neighbors",
                "selection-aware statistical rejection across all 64 attempts",
                "incomplete account or candidate accounting",
                "insufficient frozen confirmation power capacity",
                "any alteration or omission of a prior selection path",
                "any development-confirmation symbol overlap",
            ],
            "implementation_files": [
                "earnings_sec_reaction_v13_search.py",
                "earnings_sec_reaction_v13_search_inspection.py",
                "earnings_sec_reaction_v13_collection.py",
                "earnings_sec_reaction_v13_collection_inspection.py",
                "earnings_sec_corrected_expansion.py",
                "earnings_sec_corrected_expansion_inspection.py",
                "dense_strategy_runtime.py",
                "dense_strategy_plugin.py",
                "learning_statistics.py",
                "learning_experiment.py",
                "strategy_discovery.py",
                "outcome_exposure.py",
                "portfolio_maturity.py",
                "portfolio_config.toml",
            ],
            "capacity_manifest": market._repo_path(
                capacity_manifest
            ),
        }
    )
    contract["universe_requirements"] = {
        **contract["universe_requirements"],
        "confirmation_symbol_disjoint_from_development": True,
    }
    return strategy_discovery._validate_family_contract(contract)


def freeze_family(
    *,
    created_at: str,
    store: HistoricalDayStore | None = None,
    root: Path = DEFAULT_ROOT,
) -> tuple[Path, dict[str, Any], Path]:
    for raw in (
        "earnings_sec_reaction_v13_search.py",
        "earnings_sec_reaction_v13_search_inspection.py",
        "earnings_sec_reaction_v13_collection.py",
        "earnings_sec_reaction_v13_collection_inspection.py",
        "dense_strategy_runtime.py",
        "dense_strategy_plugin.py",
        "learning_experiment.py",
    ):
        strategy_discovery.require_committed(PROJECT_ROOT / raw)
    selected = selection(store)
    capacity_path, _manifest = freeze_dataset_contract(
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
                "evidence_paths": [
                    market._repo_path(CAPACITY_INSPECTION),
                    market._repo_path(PRIOR_RESULT),
                    market._repo_path(PRIOR_INSPECTION),
                    "strategy_tournament/v2/OUTCOME_EXPOSURE_INDEX.jsonl",
                ],
                "inspected": True,
                "point_in_time_evidence": True,
                "development_training_contaminated": False,
                "confirmation_access_permitted": False,
                "dense_capacity": {
                    "family_id": FAMILY_ID,
                    "formal_capacity": selected[
                        "development_event_count"
                    ],
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
    digest = hashlib.sha256(
        capacity.canonical_bytes(contract)
    ).hexdigest()
    path = root / "family-contract" / f"contract-{digest}.json"
    market._write(path, contract)
    return path, contract, capacity_path


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("status", "freeze-family"))
    parser.add_argument("--created-at")
    args = parser.parse_args(argv)
    if args.command == "status":
        selected = selection()
        value = {
            "state": "READY_TO_FREEZE",
            "successor_id": SUCCESSOR_ID,
            "calendar_wait_required": False,
            "development_events": selected["development_event_count"],
            "confirmation_events": selected["confirmation_event_count"],
            "provider_requests_permitted": 0,
        }
    else:
        if not args.created_at:
            raise EarningsSecReactionV13SearchError(
                "--created-at is required"
            )
        path, contract, capacity_path = freeze_family(
            created_at=args.created_at
        )
        value = {
            "state": "FAMILY_FROZEN",
            "path": market._repo_path(path),
            "capacity_path": market._repo_path(capacity_path),
            "trial_count": len(contract["trial_family"]),
            "cumulative_trial_count": contract[
                "selection_accounting"
            ]["cumulative_trial_count"],
            "development_events": contract["universe"][
                "development_event_count"
            ],
            "provider_requests_permitted": 0,
        }
    print(json.dumps(value, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())

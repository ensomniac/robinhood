from __future__ import annotations

from pathlib import Path

import dense_strategy_runtime as runtime
import outcome_exposure
import pytest
import sp500_deletion_collection as collection
import sp500_deletion_discovery as discovery
import sp500_deletion_plugin as plugin
from historical_store import (
    HistoricalStoreConfig,
    canonical_sha256,
)


EVALUATION_DATES = [
    "2020-01-08",
    "2020-01-09",
    "2020-01-10",
    "2020-01-13",
    "2020-01-14",
]
OBSERVATION_DATES = [
    "2020-01-02",
    "2020-01-03",
    "2020-01-06",
    "2020-01-07",
    *EVALUATION_DATES,
]


def _event(*, ticker: str = "AAA") -> dict:
    value = {
        "action": "Deletion",
        "announcement_at": "2020-01-02T18:00:00-05:00",
        "announcement_date": "2020-01-02",
        "company_name": f"{ticker} Company",
        "effective_date": "2020-01-08",
        "entry_date": "2020-01-08",
        "flow_start_date": "2020-01-03",
        "holding_dates": EVALUATION_DATES,
        "index_name": "S&P 500",
        "observation_dates": OBSERVATION_DATES,
        "pre_effective_date": "2020-01-07",
        "reference_date": "2020-01-02",
        "sessions_to_effective": 3,
        "source_url": "https://press.spglobal.com/example",
        "ticker": ticker,
    }
    value["event_id"] = canonical_sha256(value)
    return value


def _bars(ticker: str = "AAA") -> list[dict]:
    closes = {
        "2020-01-02": 100.0,
        "2020-01-03": 98.0,
        "2020-01-06": 96.0,
        "2020-01-07": 94.0,
        "2020-01-08": 96.0,
        "2020-01-09": 97.0,
        "2020-01-10": 98.0,
        "2020-01-13": 99.0,
        "2020-01-14": 100.0,
    }
    rows = []
    for day in OBSERVATION_DATES:
        close = closes[day]
        opening = 95.0 if day == "2020-01-08" else close
        rows.append(
            {
                "date": day,
                "open": opening,
                "high": max(opening, close) + 1,
                "low": min(opening, close) - 0.5,
                "close": close,
                "volume": 1_000_000,
            }
        )
    return rows


def _dataset() -> dict:
    event = _event()
    metadata = {day: [] for day in EVALUATION_DATES}
    metadata[EVALUATION_DATES[0]] = [event]
    return {
        "schema_version": 1,
        "family_id": runtime.SP500_DELETION_FORCED_SELLING_FAMILY,
        "evaluation_dates": EVALUATION_DATES,
        "event_metadata_by_entry_date": metadata,
        "daily_bars": {"AAA": _bars()},
        "split_execution_dates_by_symbol": {"AAA": []},
        "source_semantics": {
            "feed": "Yahoo Finance unadjusted daily event windows",
            "adjustment": (
                "raw bars adjusted only by frozen point-in-time split "
                "actions through the dataset end"
            ),
            "permanent_missing_symbol_response": "missed_trade",
            "invalid_daily_response": "missed_trade",
            "substitution": "forbidden",
        },
    }


def _parameters() -> dict:
    return {
        "maximum_hold_sessions": 2,
        "maximum_positive_effective_gap_fraction": 0.02,
        "minimum_pre_effective_decline_fraction": 0.05,
        "minimum_sessions_to_effective": 2,
        "stop_buffer_below_pre_effective_close": 0.01,
    }


def test_partition_uses_complete_observation_scope_and_clean_reserve():
    scope, capacity = discovery._partition(enforce_commit=False)

    assert capacity["state"] == "CAPACITY_READY_FAST_LANE"
    assert len(scope["development_events"]) == 114
    assert len(scope["development_signal_dates"]) == 85
    assert len(scope["confirmation_events"]) == 44
    assert len(scope["confirmation_signal_dates"]) == 34
    assert len(scope["causal_ineligible_events"]) == 5
    assert len(scope["embargo_dates"]) == 5
    assert all(
        set(row["observation_dates"]).issubset(
            scope["development_scope"]["dates"]
        )
        for row in scope["development_events"]
    )
    outcome_exposure.assert_untouched(
        scope["confirmation_scope"], outcome_exposure.read_index()
    )
    assert scope["market_outcomes_accessed"] is False
    assert scope["provider_requests"] == 0


def test_family_freezes_exact_32_trial_search_without_outcomes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
):
    original_repo_path = discovery._repo_path

    def test_repo_path(path: Path) -> str:
        try:
            return original_repo_path(path)
        except ValueError:
            return f"tests/generated/{path.name}"

    monkeypatch.setattr(discovery, "_repo_path", test_repo_path)
    path, contract, capacity, scope = discovery.freeze_family(
        created_at="2026-07-26T08:30:00Z",
        root=tmp_path,
        enforce_commit=False,
    )

    assert path.exists()
    assert capacity.exists()
    assert scope.exists()
    assert len(contract["trial_family"]) == 32
    assert contract["selection_mode"] == "development_search"
    assert contract["confirmation_signal_capacity"] == 34
    assert contract["historical_data_contract"][
        "market_price_access_before_search_freeze"
    ] is False


def test_runtime_enters_after_rebalance_and_exits_at_frozen_hold():
    prepared = runtime.prepare_dataset(_dataset())
    candidates = runtime.build_candidates(
        prepared,
        runtime.SP500_DELETION_FORCED_SELLING_FAMILY,
        _parameters(),
    )

    assert len(candidates) == 1
    candidate = candidates[0]
    assert candidate["outcome"] == "eligible"
    assert candidate["rank"] == 1
    assert candidate["entry_price"] == 95.0
    assert candidate["stop_price"] == 93.06
    assert candidate[
        "forced_selling_decline_fraction"
    ] == pytest.approx(0.06)
    assert candidate["exit_date"] == "2020-01-09"
    assert candidate["exit_price"] == 97.0


def test_collection_retains_complete_observation_window(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
):
    event = _event()
    split_task = collection._task(
        {
            "kind": "split_actions",
            "start": OBSERVATION_DATES[0],
            "date": OBSERVATION_DATES[-1],
        }
    )
    daily_task = collection._task(
        {
            "kind": "yahoo_daily_symbol_bars",
            "symbol": "AAA",
            "start": OBSERVATION_DATES[0],
            "date": OBSERVATION_DATES[-1],
        }
    )
    plan = {
        "lane": "development",
        "event_scope_path": "event-scope.json",
        "events_sha256": canonical_sha256([event]),
        "event_task_ids": {
            event["event_id"]: daily_task["task_id"]
        },
        "tasks": [split_task, daily_task],
        "evaluation_dates": EVALUATION_DATES,
    }
    monkeypatch.setattr(
        collection.strategy_discovery,
        "load_artifact",
        lambda *_args, **_kwargs: {
            "development_events": [event]
        },
    )
    config = HistoricalStoreConfig(root=tmp_path, min_free_bytes=0)
    collection.shared._write_gzip(
        collection.shared._checkpoint_path(tmp_path, split_task),
        {
            "schema_version": 1,
            "task": split_task,
            "rows": [],
            "rows_sha256": canonical_sha256([]),
            "collection_disposition": "provider_rows",
        },
        config,
    )
    raw_rows = [{"symbol": "AAA", **row} for row in _bars()]
    collection.shared._write_gzip(
        collection.shared._checkpoint_path(tmp_path, daily_task),
        {
            "schema_version": 1,
            "task": daily_task,
            "rows": raw_rows,
            "rows_sha256": canonical_sha256(raw_rows),
            "collection_disposition": "provider_rows",
        },
        config,
    )

    dataset = collection.build_dataset(tmp_path, plan)

    assert len(dataset["daily_bars"]["AAA"]) == len(
        OBSERVATION_DATES
    )
    assert dataset["event_metadata_by_entry_date"][
        EVALUATION_DATES[0]
    ] == [event]
    candidate = runtime.build_candidates(
        runtime.prepare_dataset(dataset),
        runtime.SP500_DELETION_FORCED_SELLING_FAMILY,
        _parameters(),
    )[0]
    assert candidate["outcome"] == "eligible"


def test_production_evaluator_matches_historical_ranking_and_stop():
    event = _event()
    winner = {
        "strategy_id": discovery.STRATEGY_ID,
        "strategy_version": "sp500-deletion-test",
        "rules_hash": "a" * 64,
        "exact_rules": {
            "selected_trial_id": "trial-001",
            "parameters": _parameters(),
        },
    }
    operational = {
        "before_open_account_reconciled": True,
        "before_open_news_reconciled": True,
        "before_open_orders_reconciled": True,
        "before_open_protection_reconciled": True,
        "before_open_tradability_reconciled": True,
        "monitoring_ready": True,
        "protection_failure_safe_cutoff": "15:45:00-05:00",
        "protective_order_route_ready": True,
    }
    result = plugin.evaluate_production(
        winner,
        {
            "selected_trial_id": "trial-001",
            "parameters": _parameters(),
            "decision_data": {
                "decision_date": "2020-01-07",
                "events": [event],
                "family_id": (
                    runtime.SP500_DELETION_FORCED_SELLING_FAMILY
                ),
                "next_session_date": "2020-01-08",
                "pre_effective_closes": {"AAA": 94.0},
                "reference_closes": {"AAA": 100.0},
            },
            "quotes": [
                {
                    "ask": 95.0,
                    "bid": 94.9,
                    "depth_gate_passed": True,
                    "executable_ask_depth": 10_000,
                    "halted": False,
                    "news_gate_passed": True,
                    "observed_at": "2020-01-08T09:30:15-05:00",
                    "recent_real_minute_volume": 25_000,
                    "spread_gate_passed": True,
                    "symbol": "AAA",
                    "tradable": True,
                }
            ],
            "operational": operational,
        },
    )

    assert result["symbol"] == "AAA"
    assert result["rank"] == 1
    assert result["entry_limit"] == 95.0
    assert result["stop_price"] == 93.06
    assert result["holding_trading_days"] == 2
    assert result["protection_time_in_force"] == "gtc"

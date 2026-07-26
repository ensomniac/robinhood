from __future__ import annotations

from pathlib import Path

import pytest

import dense_strategy_runtime as runtime
import sp500_addition_collection as collection
import sp500_addition_discovery as discovery
import sp500_addition_plugin as plugin
from historical_store import HistoricalStoreConfig, canonical_sha256


CALENDAR = [
    "2020-01-03",
    "2020-01-06",
    "2020-01-07",
    "2020-01-08",
    "2020-01-09",
    "2020-01-10",
]
REFERENCE_DATE = "2020-01-02"


def _event(
    *,
    ticker: str = "AAA",
    sessions_to_effective: int = 3,
    pre_effective_date: str = "2020-01-07",
) -> dict:
    value = {
        "action": "Addition",
        "announcement_at": "2020-01-02T18:00:00-05:00",
        "announcement_date": "2020-01-02",
        "company_name": f"{ticker} Company",
        "effective_date": "2020-01-08",
        "entry_date": CALENDAR[0],
        "holding_dates": CALENDAR[:5],
        "index_name": "S&P 500",
        "pre_effective_date": pre_effective_date,
        "reference_date": REFERENCE_DATE,
        "sessions_to_effective": sessions_to_effective,
        "source_url": "https://press.spglobal.com/example",
        "ticker": ticker,
    }
    value["event_id"] = canonical_sha256(value)
    return value


def _bars(ticker: str = "AAA", entry_open: float = 102.0) -> list[dict]:
    rows = [
        {
            "date": REFERENCE_DATE,
            "open": 99.0,
            "high": 101.0,
            "low": 98.0,
            "close": 100.0,
            "volume": 1_000_000,
        }
    ]
    for index, day in enumerate(CALENDAR[:5]):
        opening = entry_open if index == 0 else 102.0 + index
        rows.append(
            {
                "date": day,
                "open": opening,
                "high": max(opening, 104.0 + index),
                "low": 100.0,
                "close": 103.0 + index,
                "volume": 1_000_000,
            }
        )
    return rows


def _dataset(
    *,
    event: dict | None = None,
    entry_open: float = 102.0,
) -> dict:
    selected = event or _event()
    metadata = {day: [] for day in CALENDAR}
    metadata[CALENDAR[0]] = [selected]
    return {
        "schema_version": 1,
        "family_id": runtime.SP500_ADDITION_FORCED_DEMAND_FAMILY,
        "evaluation_dates": CALENDAR,
        "event_metadata_by_entry_date": metadata,
        "daily_bars": {
            selected["ticker"]: _bars(
                str(selected["ticker"]), entry_open
            )
        },
        "split_execution_dates_by_symbol": {
            selected["ticker"]: []
        },
        "source_semantics": {
            "feed": "Massive SIP unadjusted daily event windows",
            "adjustment": (
                "raw bars adjusted only by frozen point-in-time split "
                "actions through the dataset end"
            ),
            "substitution": "forbidden",
        },
    }


def _parameters(**overrides) -> dict:
    value = {
        "exit_mode": "pre_effective_or_maximum_hold",
        "maximum_hold_sessions": 5,
        "maximum_positive_announcement_gap_fraction": 0.04,
        "minimum_sessions_to_effective": 2,
        "stop_buffer_below_reference_close": 0.01,
    }
    value.update(overrides)
    return value


def test_outcome_blind_partition_uses_all_clean_post_2018_capacity() -> None:
    scope, capacity = discovery._partition(enforce_commit=False)
    assert capacity["market_outcomes_accessed"] is False
    assert len(scope["development_events"]) == 144
    assert len(scope["development_signal_dates"]) == 100
    assert len(scope["causal_ineligible_events"]) == 5
    assert len(scope["embargo_dates"]) == 5
    assert len(scope["confirmation_events"]) == 45
    assert len(scope["confirmation_signal_dates"]) == 31
    assert scope["development_dates"][-1] == "2019-01-04"
    assert scope["embargo_dates"] == [
        "2019-01-07",
        "2019-01-08",
        "2019-01-09",
        "2019-01-10",
        "2019-01-11",
    ]
    assert scope["confirmation_dates"][0] == "2019-01-14"
    assert scope["market_outcomes_accessed"] is False
    assert scope["provider_requests"] == 0


def test_runtime_exits_at_causal_pre_effective_close() -> None:
    dataset = runtime.prepare_dataset(_dataset())
    candidates = runtime.build_candidates(
        dataset,
        runtime.SP500_ADDITION_FORCED_DEMAND_FAMILY,
        _parameters(),
    )
    assert len(candidates) == 1
    candidate = candidates[0]
    assert candidate["outcome"] == "eligible"
    assert candidate["rank"] == 1
    assert candidate["entry_price"] == 102.0
    assert candidate["stop_price"] == 99.0
    assert candidate["exit_date"] == "2020-01-07"
    assert candidate["exit_price"] == 105.0
    assert list(candidate["marks"]) == CALENDAR[:3]


def test_runtime_retains_gap_rejection_and_invalid_stop_as_miss() -> None:
    gap = runtime.prepare_dataset(_dataset(entry_open=105.0))
    rejected = runtime.build_candidates(
        gap,
        runtime.SP500_ADDITION_FORCED_DEMAND_FAMILY,
        _parameters(
            maximum_positive_announcement_gap_fraction=0.02
        ),
    )[0]
    assert rejected["outcome"] == "rejected"
    assert (
        rejected["rejection_reason"]
        == "announcement_gap_above_maximum"
    )

    invalid = runtime.prepare_dataset(_dataset(entry_open=100.0))
    missed = runtime.build_candidates(
        invalid,
        runtime.SP500_ADDITION_FORCED_DEMAND_FAMILY,
        _parameters(stop_buffer_below_reference_close=0.0),
    )[0]
    assert missed["outcome"] == "missed_fill"
    assert missed["rejection_reason"] == "invalid_structural_stop"


def test_runtime_ranking_is_lead_then_ticker_and_accounts_every_event() -> None:
    first = _event(ticker="BBB", sessions_to_effective=5)
    second = _event(ticker="AAA", sessions_to_effective=3)
    metadata = {day: [] for day in CALENDAR}
    metadata[CALENDAR[0]] = [first, second]
    dataset = {
        **_dataset(event=first),
        "event_metadata_by_entry_date": metadata,
        "daily_bars": {
            "AAA": _bars("AAA"),
            "BBB": _bars("BBB"),
        },
        "split_execution_dates_by_symbol": {"AAA": [], "BBB": []},
    }
    prepared = runtime.prepare_dataset(dataset)
    candidates = runtime.build_candidates(
        prepared,
        runtime.SP500_ADDITION_FORCED_DEMAND_FAMILY,
        _parameters(),
    )
    assert [
        (row["rank"], row["symbol"], row["outcome"])
        for row in candidates
    ] == [(1, "BBB", "eligible"), (2, "AAA", "eligible")]


def test_collection_build_retains_exact_event_window(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    event = _event()
    split_task = collection._task(
        {
            "kind": "split_actions",
            "start": REFERENCE_DATE,
            "date": CALENDAR[4],
        }
    )
    daily_task = collection._task(
        {
            "kind": "massive_daily_symbol_bars",
            "symbol": "AAA",
            "start": REFERENCE_DATE,
            "date": CALENDAR[4],
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
        "evaluation_dates": CALENDAR,
    }
    scope = {"development_events": [event]}
    monkeypatch.setattr(
        collection.strategy_discovery,
        "load_artifact",
        lambda *_args, **_kwargs: scope,
    )
    config = HistoricalStoreConfig(root=tmp_path, min_free_bytes=0)
    collection._write_gzip(
        collection._checkpoint_path(tmp_path, split_task),
        {
            "schema_version": 1,
            "task": split_task,
            "rows": [],
            "rows_sha256": canonical_sha256([]),
        },
        config,
    )
    raw_rows = [
        {"symbol": "AAA", **row} for row in _bars()
    ]
    collection._write_gzip(
        collection._checkpoint_path(tmp_path, daily_task),
        {
            "schema_version": 1,
            "task": daily_task,
            "rows": raw_rows,
            "rows_sha256": canonical_sha256(raw_rows),
        },
        config,
    )
    dataset = collection.build_dataset(tmp_path, plan)
    assert list(dataset["daily_bars"]) == ["AAA"]
    assert len(dataset["daily_bars"]["AAA"]) == 6
    assert dataset["event_metadata_by_entry_date"][CALENDAR[0]] == [
        event
    ]


def test_production_uses_frozen_rank_gap_stop_and_next_open() -> None:
    event = _event()
    parameters = _parameters()
    winner = {
        "strategy_id": "sp500-index-addition-forced-demand",
        "strategy_version": "sp500-index-addition-v1",
        "rules_hash": "a" * 64,
        "exact_rules": {
            "selected_trial_id": "trial-a",
            "parameters": parameters,
        },
    }
    quote = {
        "symbol": "AAA",
        "observed_at": "2020-01-03T09:30:15-05:00",
        "halted": False,
        "tradable": True,
        "bid": 101.99,
        "ask": 102.0,
        "spread_gate_passed": True,
        "depth_gate_passed": True,
        "news_gate_passed": True,
        "executable_ask_depth": 10_000,
        "recent_real_minute_volume": 25_000,
    }
    operational = {
        "before_open_account_reconciled": True,
        "before_open_orders_reconciled": True,
        "before_open_protection_reconciled": True,
        "before_open_tradability_reconciled": True,
        "before_open_news_reconciled": True,
        "protective_order_route_ready": True,
        "monitoring_ready": True,
        "protection_failure_safe_cutoff": "15:45 America/New_York",
    }
    result = plugin.evaluate_production(
        winner,
        {
            "selected_trial_id": "trial-a",
            "parameters": parameters,
            "decision_data": {
                "family_id": (
                    runtime.SP500_ADDITION_FORCED_DEMAND_FAMILY
                ),
                "decision_date": "2020-01-02",
                "next_session_date": "2020-01-03",
                "events": [event],
                "reference_closes": {"AAA": 100.0},
            },
            "quotes": [quote],
            "operational": operational,
        },
    )
    assert result["symbol"] == "AAA"
    assert result["entry_limit"] == 102.0
    assert result["stop_price"] == 99.0
    assert result["protection_time_in_force"] == "gtc"
    assert result["exit_plan"]["pre_effective_date"] == "2020-01-07"

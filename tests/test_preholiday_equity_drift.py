from __future__ import annotations

import copy
import json
from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

import dense_collection_plan_inspection as plan_inspection
import dense_data_collection as collection
import dense_strategy_plugin as plugin
import dense_strategy_runtime as runtime
import preholiday_equity_drift as family


PARAMETERS = dict(family.PARAMETERS)


def _dataset() -> dict:
    dates = ["2024-07-02", "2024-07-03", "2024-07-05"]
    return {
        "family_id": runtime.PREHOLIDAY_EQUITY_DRIFT_FAMILY,
        "evaluation_dates": dates,
        "signal_dates": ["2024-07-03"],
        "symbols": [runtime.PREHOLIDAY_EQUITY_DRIFT_SYMBOL],
        "daily_bars": {
            runtime.PREHOLIDAY_EQUITY_DRIFT_SYMBOL: [
                {
                    "date": dates[0],
                    "open": 320.0,
                    "high": 322.0,
                    "low": 319.0,
                    "close": 321.0,
                    "volume": 200_000,
                },
                {
                    "date": dates[1],
                    "open": 321.0,
                    "high": 324.0,
                    "low": 320.0,
                    "close": 323.0,
                    "volume": 180_000,
                },
                {
                    "date": dates[2],
                    "open": 323.5,
                    "high": 324.0,
                    "low": 322.0,
                    "close": 323.0,
                    "volume": 210_000,
                },
            ]
        },
    }


def _candidate(dataset: dict) -> dict:
    prepared = runtime.prepare_dataset(dataset)
    return runtime.build_candidates(
        prepared,
        runtime.PREHOLIDAY_EQUITY_DRIFT_FAMILY,
        PARAMETERS,
    )[0]


def test_preholiday_inventory_is_dense_scheduled_and_outcome_clean():
    split = family.partitions()
    events = family.preholiday_events()

    assert len(events) == 145
    assert len(split["development_signal_dates"]) == 80
    assert len(split["confirmation_inventory_signal_dates"]) == 65
    assert len(split["confirmation_signal_dates"]) == 48
    assert len(split["confirmation_excluded_exposed_signal_dates"]) == 17
    assert len(split["embargo_dates"]) == 5
    assert split["development_dates"][-1] < split["embargo_dates"][0]
    assert split["embargo_dates"][-1] < split["confirmation_dates"][0]
    assert any(item["scheduled_close_et"] == "13:00" for item in events)
    assert {
        "2012-10-26",
        "2018-12-04",
        "2025-01-08",
    }.isdisjoint(item["signal_date"] for item in events)
    assert family.SYMBOLS == ["IWV"]


def test_preholiday_collection_calendar_retains_early_closes(tmp_path):
    calendar_path = tmp_path / "calendar.json"
    calendar_path.write_text(
        json.dumps(
            [
                {
                    "date": "2024-07-02",
                    "open_et": "09:30",
                    "close_et": "16:00",
                },
                {
                    "date": "2024-07-03",
                    "open_et": "09:30",
                    "close_et": "13:00",
                },
                {
                    "date": "2024-07-05",
                    "open_et": "09:30",
                    "close_et": "16:00",
                },
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    assert collection._calendar(calendar_path) == [
        "2024-07-02",
        "2024-07-05",
    ]
    assert collection._calendar(
        calendar_path, include_early_closes=True
    ) == [
        "2024-07-02",
        "2024-07-03",
        "2024-07-05",
    ]


def test_preholiday_runtime_enters_open_and_exits_same_session_close():
    candidate = _candidate(_dataset())

    assert candidate["outcome"] == "eligible"
    assert candidate["signal_date"] == "2024-07-03"
    assert candidate["decision_date"] == "2024-07-02"
    assert candidate["entry_price"] == 321.0
    assert candidate["exit_date"] == "2024-07-03"
    assert candidate["exit_price"] == 323.0
    assert candidate["stop_executed"] is False
    assert candidate["expected_gross_move_fraction"] == 0.005


def test_preholiday_runtime_resolves_intraday_stop_conservatively():
    dataset = _dataset()
    expected_stop = 321.0 * 0.985
    dataset["daily_bars"]["IWV"][1]["low"] = expected_stop - 0.01

    candidate = _candidate(dataset)

    assert candidate["stop_executed"] is True
    assert candidate["exit_price"] == pytest.approx(expected_stop)
    assert candidate["marks"] == {
        "2024-07-03": pytest.approx(expected_stop)
    }


def test_preholiday_runtime_rejects_parameter_drift():
    prepared = runtime.prepare_dataset(_dataset())

    with pytest.raises(
        runtime.DenseStrategyRuntimeError,
        match="parameters drifted",
    ):
        runtime.build_candidates(
            prepared,
            runtime.PREHOLIDAY_EQUITY_DRIFT_FAMILY,
            {**PARAMETERS, "stop_fraction": 0.02},
        )


def test_preholiday_confirmation_can_keep_zero_days_without_outcome_bars():
    dataset = _dataset()
    dataset["daily_bars"]["IWV"] = [
        dataset["daily_bars"]["IWV"][1]
    ]

    prepared = runtime.prepare_dataset(dataset)
    candidate = runtime.build_candidates(
        prepared,
        runtime.PREHOLIDAY_EQUITY_DRIFT_FAMILY,
        PARAMETERS,
    )[0]

    assert candidate["outcome"] == "eligible"
    assert len(prepared["evaluation_dates"]) == 3
    assert len(prepared["daily_bars"]["IWV"]) == 1


def test_preholiday_confirmation_plan_requests_only_exact_signal_dates():
    required_dates = [
        "2024-07-01",
        "2024-07-02",
        "2024-07-03",
        "2024-07-05",
    ]
    signal_dates = ["2024-07-03"]
    plan = {
        "family_id": runtime.PREHOLIDAY_EQUITY_DRIFT_FAMILY,
        "lane": "confirmation",
        "required_dates": required_dates,
        "signal_dates": signal_dates,
        "sparse_confirmation_outcome_access": True,
    }
    contract = {
        "universe": {"symbols": ["IWV"]},
        "historical_data_contract": {
            "daily_provider": "yahoo",
            "daily_request_mode": "symbol_range",
            "confirmation_request_mode": "exact_signal_dates_only",
            "daily_adjustment": (
                collection.YAHOO_SOURCE_RECOVERY_ADJUSTMENT
            ),
            "split_provider": "massive",
            "provider_substitutions_allowed": False,
            "no_purchase_required": True,
            "retries_permitted": 0,
        },
    }

    tasks = plan_inspection._expected_tasks(plan, contract)

    assert len(tasks) == 2
    assert tasks[0]["kind"] == "split_actions"
    assert tasks[0]["start"] == required_dates[0]
    assert tasks[0]["date"] == required_dates[-1]
    assert tasks[1] == {
        "kind": "yahoo_daily_symbol_bars",
        "date": "2024-07-03",
        "start": "2024-07-03",
        "symbol": "IWV",
        "task_id": collection.canonical_sha256(
            {
                "kind": "yahoo_daily_symbol_bars",
                "date": "2024-07-03",
                "start": "2024-07-03",
                "symbol": "IWV",
            }
        ),
    }


def _production_data() -> dict:
    return {
        "family_id": runtime.PREHOLIDAY_EQUITY_DRIFT_FAMILY,
        "decision_date": "2024-07-02",
        "next_session_date": "2024-07-03",
        "scheduled_preholiday_session_date": "2024-07-03",
        "scheduled_close_et": "13:00",
        "calendar_dates": ["2024-07-02"],
        "daily_history_complete": True,
        "symbols": ["IWV"],
        "daily_bars": {
            "IWV": [_dataset()["daily_bars"]["IWV"][0]]
        },
    }


def _winner() -> dict:
    return {
        "family_id": runtime.PREHOLIDAY_EQUITY_DRIFT_FAMILY,
        "strategy_id": runtime.PREHOLIDAY_EQUITY_DRIFT_FAMILY,
        "strategy_version": "preholiday-equity-drift-v1",
        "rules_hash": "b" * 64,
        "exact_rules": {
            "selected_trial_id": "preholiday-fixed-rule",
            "parameters": PARAMETERS,
            "universe": {
                "symbols": ["IWV"],
                "tradable_symbols": ["IWV"],
                "feature_only_symbols": [],
                "point_in_time": True,
            },
        },
    }


def test_preholiday_production_path_requires_exact_schedule():
    signal = runtime.evaluate_production_signal(
        _production_data(),
        family_id=runtime.PREHOLIDAY_EQUITY_DRIFT_FAMILY,
        parameters=PARAMETERS,
        frozen_universe=_winner()["exact_rules"]["universe"],
    )

    assert signal["symbol"] == "IWV"
    assert signal["entry_timing"] == "scheduled_preholiday_session_open"
    assert signal["scheduled_preholiday_session_date"] == "2024-07-03"
    assert signal["scheduled_close_et"] == "13:00"
    assert signal["stop_fraction"] == 0.015
    assert signal["overnight_hold"] is False
    assert signal["exit_plan"]["scheduled_exit_time_et"] == "13:00"

    drifted = copy.deepcopy(_production_data())
    drifted["scheduled_preholiday_session_date"] = "2024-07-05"
    with pytest.raises(
        runtime.DenseStrategyRuntimeError,
        match="schedule or exact rules drifted",
    ):
        runtime.evaluate_production_signal(
            drifted,
            family_id=runtime.PREHOLIDAY_EQUITY_DRIFT_FAMILY,
            parameters=PARAMETERS,
            frozen_universe=_winner()["exact_rules"]["universe"],
        )


def test_preholiday_production_evaluator_uses_open_quote_and_day_stop():
    observed = datetime(
        2024,
        7,
        3,
        9,
        30,
        20,
        tzinfo=ZoneInfo("America/New_York"),
    )
    market_facts = {
        "selected_trial_id": "preholiday-fixed-rule",
        "parameters": PARAMETERS,
        "decision_data": _production_data(),
        "quote": {
            "symbol": "IWV",
            "observed_at": observed.isoformat(),
            "halted": False,
            "tradable": True,
            "bid": 320.98,
            "ask": 321.00,
            "executable_ask_depth": 8_000,
            "recent_real_minute_volume": 12_000,
        },
        "operational": {
            "before_open_account_reconciled": True,
            "before_open_orders_reconciled": True,
            "before_open_protection_reconciled": True,
            "before_open_tradability_reconciled": True,
            "before_open_news_reconciled": True,
            "protective_order_route_ready": True,
            "monitoring_ready": True,
            "protection_failure_safe_cutoff": "12:45 ET",
        },
    }

    result = plugin.evaluate_production(_winner(), market_facts)

    assert result["entry_limit"] == 321.00
    assert result["stop_price"] == pytest.approx(321.00 * 0.985)
    assert result["protection_time_in_force"] == "day"
    assert result["exit_plan"]["scheduled_exit_date"] == "2024-07-03"
    assert result["exit_plan"]["scheduled_exit_time_et"] == "13:00"
    assert result["historical_semantics_sha256"] == _winner()["rules_hash"]

    market_facts["quote"]["observed_at"] = observed.replace(
        hour=9,
        minute=31,
    ).isoformat()
    with pytest.raises(
        plugin.DenseStrategyPluginError,
        match="opening interval",
    ):
        plugin.evaluate_production(_winner(), market_facts)

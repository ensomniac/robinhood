from __future__ import annotations

import copy
from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

import dense_strategy_plugin as plugin
import dense_strategy_runtime as runtime
import fomc_preannouncement as family


PARAMETERS = dict(family.PARAMETERS)


def _dataset() -> dict:
    dates = ["2024-03-19", "2024-03-20", "2024-03-21"]
    return {
        "family_id": runtime.FOMC_PREANNOUNCEMENT_FAMILY,
        "evaluation_dates": dates,
        "signal_dates": ["2024-03-19"],
        "symbols": [runtime.FOMC_PREANNOUNCEMENT_SYMBOL],
        "daily_bars": {
            runtime.FOMC_PREANNOUNCEMENT_SYMBOL: [
                {
                    "date": dates[0],
                    "open": 60.0,
                    "high": 60.8,
                    "low": 59.8,
                    "close": 60.5,
                    "volume": 1_000_000,
                },
                {
                    "date": dates[1],
                    "open": 60.6,
                    "high": 61.4,
                    "low": 60.2,
                    "close": 61.1,
                    "volume": 1_200_000,
                },
                {
                    "date": dates[2],
                    "open": 61.1,
                    "high": 61.2,
                    "low": 60.9,
                    "close": 61.0,
                    "volume": 900_000,
                },
            ]
        },
    }


def _candidate(dataset: dict) -> dict:
    prepared = runtime.prepare_dataset(dataset)
    return runtime.build_candidates(
        prepared,
        runtime.FOMC_PREANNOUNCEMENT_FAMILY,
        PARAMETERS,
    )[0]


def test_fomc_inventory_is_fixed_dense_and_partitioned_before_outcomes():
    split = family.partitions()

    assert len(family.decision_dates()) == 119
    assert len(split["development_signal_dates"]) == 64
    assert len(split["confirmation_inventory_signal_dates"]) == 55
    assert len(split["confirmation_signal_dates"]) == 35
    assert len(split["confirmation_excluded_exposed_signal_dates"]) == 20
    assert len(split["embargo_dates"]) == 5
    assert split["development_dates"][-1] < split["embargo_dates"][0]
    assert split["embargo_dates"][-1] < split["confirmation_dates"][0]
    assert len(family.PARAMETERS) == 5
    assert family.SYMBOLS == ["SCHB"]


def test_fomc_runtime_enters_prior_close_and_exits_decision_close():
    candidate = _candidate(_dataset())

    assert candidate["outcome"] == "eligible"
    assert candidate["signal_date"] == "2024-03-19"
    assert candidate["fomc_decision_date"] == "2024-03-20"
    assert candidate["entry_price"] == 60.5
    assert candidate["exit_date"] == "2024-03-20"
    assert candidate["exit_price"] == 61.1
    assert candidate["stop_executed"] is False
    assert candidate["expected_gross_move_fraction"] == 0.005


def test_fomc_runtime_resolves_gap_and_intraday_stops_conservatively():
    gap = _dataset()
    expected_stop = 60.5 * 0.985
    gap["daily_bars"]["SCHB"][1]["open"] = expected_stop - 0.25
    gap["daily_bars"]["SCHB"][1]["low"] = expected_stop - 0.40
    gap_candidate = _candidate(gap)

    intraday = _dataset()
    intraday["daily_bars"]["SCHB"][1]["low"] = expected_stop - 0.01
    intraday_candidate = _candidate(intraday)

    assert gap_candidate["stop_executed"] is True
    assert gap_candidate["exit_price"] == pytest.approx(expected_stop - 0.25)
    assert intraday_candidate["stop_executed"] is True
    assert intraday_candidate["exit_price"] == pytest.approx(expected_stop)


def test_fomc_runtime_rejects_parameter_drift():
    prepared = runtime.prepare_dataset(_dataset())

    with pytest.raises(runtime.DenseStrategyRuntimeError, match="parameters drifted"):
        runtime.build_candidates(
            prepared,
            runtime.FOMC_PREANNOUNCEMENT_FAMILY,
            {**PARAMETERS, "stop_fraction": 0.02},
        )


def _production_data() -> dict:
    dataset = _dataset()
    return {
        "family_id": runtime.FOMC_PREANNOUNCEMENT_FAMILY,
        "decision_date": "2024-03-19",
        "next_session_date": "2024-03-20",
        "scheduled_fomc_decision_date": "2024-03-20",
        "calendar_dates": ["2024-03-19"],
        "daily_history_complete": True,
        "symbols": ["SCHB"],
        "daily_bars": {"SCHB": [dataset["daily_bars"]["SCHB"][0]]},
    }


def test_fomc_production_path_requires_exact_next_session_schedule():
    signal = runtime.evaluate_production_signal(
        _production_data(),
        family_id=runtime.FOMC_PREANNOUNCEMENT_FAMILY,
        parameters=PARAMETERS,
        frozen_universe={
            "symbols": ["SCHB"],
            "tradable_symbols": ["SCHB"],
            "feature_only_symbols": [],
            "point_in_time": True,
        },
    )

    assert signal["symbol"] == "SCHB"
    assert signal["entry_timing"] == "prior_session_close"
    assert signal["scheduled_fomc_decision_date"] == "2024-03-20"
    assert signal["stop_fraction"] == 0.015
    assert signal["overnight_hold"] is True

    drifted = copy.deepcopy(_production_data())
    drifted["scheduled_fomc_decision_date"] = "2024-03-21"
    with pytest.raises(
        runtime.DenseStrategyRuntimeError,
        match="schedule or exact rules drifted",
    ):
        runtime.evaluate_production_signal(
            drifted,
            family_id=runtime.FOMC_PREANNOUNCEMENT_FAMILY,
            parameters=PARAMETERS,
            frozen_universe={
                "symbols": ["SCHB"],
                "tradable_symbols": ["SCHB"],
                "feature_only_symbols": [],
                "point_in_time": True,
            },
        )


def test_fomc_production_evaluator_requires_closing_quote_and_percentage_stop():
    observed = datetime(
        2024,
        3,
        19,
        15,
        59,
        30,
        tzinfo=ZoneInfo("America/New_York"),
    )
    winner = {
        "family_id": runtime.FOMC_PREANNOUNCEMENT_FAMILY,
        "strategy_id": runtime.FOMC_PREANNOUNCEMENT_FAMILY,
        "strategy_version": "fomc-preannouncement-v1",
        "rules_hash": "a" * 64,
        "exact_rules": {
            "selected_trial_id": "fomc-fixed-rule",
            "parameters": PARAMETERS,
            "universe": {
                "symbols": ["SCHB"],
                "tradable_symbols": ["SCHB"],
                "feature_only_symbols": [],
                "point_in_time": True,
            },
        },
    }
    market_facts = {
        "selected_trial_id": "fomc-fixed-rule",
        "parameters": PARAMETERS,
        "decision_data": _production_data(),
        "quote": {
            "symbol": "SCHB",
            "observed_at": observed.isoformat(),
            "halted": False,
            "tradable": True,
            "bid": 60.49,
            "ask": 60.50,
            "executable_ask_depth": 20_000,
            "recent_real_minute_volume": 30_000,
        },
        "operational": {
            "before_open_account_reconciled": True,
            "before_open_orders_reconciled": True,
            "before_open_protection_reconciled": True,
            "before_open_tradability_reconciled": True,
            "before_open_news_reconciled": True,
            "protective_order_route_ready": True,
            "monitoring_ready": True,
            "protection_failure_safe_cutoff": "15:45 ET",
        },
    }

    result = plugin.evaluate_production(winner, market_facts)

    assert result["entry_limit"] == 60.50
    assert result["stop_price"] == pytest.approx(60.50 * 0.985)
    assert result["protection_time_in_force"] == "gtc"
    assert result["exit_plan"]["scheduled_exit_date"] == "2024-03-20"
    assert result["historical_semantics_sha256"] == winner["rules_hash"]

    market_facts["quote"]["observed_at"] = observed.replace(
        hour=15,
        minute=58,
    ).isoformat()
    with pytest.raises(
        plugin.DenseStrategyPluginError,
        match="frozen closing interval",
    ):
        plugin.evaluate_production(winner, market_facts)

from __future__ import annotations

from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

import pytest

import dense_strategy_plugin as plugin
import dense_strategy_runtime as runtime


PARAMETERS = {
    "trend_sma": 200,
    "rsi2_maximum": 10.0,
    "mean_reversion_sma": 5,
    "stop_atr14": 1.5,
    "maximum_hold_sessions": 5,
}


def _business_days(count: int) -> list[str]:
    result: list[str] = []
    current = date(2021, 1, 4)
    while len(result) < count:
        if current.weekday() < 5:
            result.append(current.isoformat())
        current += timedelta(days=1)
    return result


def _dataset() -> dict:
    days = _business_days(235)
    closes = [100.0 + 0.20 * index for index in range(len(days))]
    closes[205] -= 3.0
    closes[206] -= 5.5
    closes[207] -= 6.5
    closes[208] -= 1.0
    bars = []
    for index, (day, close) in enumerate(zip(days, closes, strict=True)):
        opening = close - 0.05
        if index == 208:
            opening = close - 4.0
        bars.append(
            {
                "date": day,
                "open": opening,
                "high": max(opening, close) + 0.40,
                "low": min(opening, close) - 0.40,
                "close": close,
                "volume": 10_000_000,
            }
        )
    return {
        "family_id": runtime.SPY_RSI2_PULLBACK_FAMILY,
        "evaluation_dates": days[200:230],
        "symbols": ["SPY"],
        "daily_bars": {"SPY": bars},
    }


def _eligible(dataset: dict) -> list[dict]:
    return [
        candidate
        for candidate in runtime.build_candidates(
            dataset,
            runtime.SPY_RSI2_PULLBACK_FAMILY,
            PARAMETERS,
        )
        if candidate["outcome"] == "eligible"
    ]


def test_fixed_spy_rsi2_rule_enters_next_open_and_exits_on_sma5_reclaim():
    dataset = _dataset()

    candidate = next(
        item
        for item in _eligible(dataset)
        if item["decision_date"] == dataset["evaluation_dates"][7]
    )

    assert candidate["signal_date"] == dataset["evaluation_dates"][8]
    assert candidate["entry_price"] == dataset["daily_bars"]["SPY"][208]["open"]
    assert candidate["expected_gross_move_fraction"] >= 0.005
    assert candidate["exit_date"] == candidate["signal_date"]
    assert candidate["stop_executed"] is False


def test_fixed_spy_rsi2_rule_resolves_stop_before_same_day_sma5_exit():
    dataset = _dataset()
    initial = next(
        item
        for item in _eligible(dataset)
        if item["decision_date"] == dataset["evaluation_dates"][7]
    )
    entry_index = 208
    dataset["daily_bars"]["SPY"][entry_index]["low"] = initial["stop_price"] - 0.01

    candidate = next(
        item
        for item in _eligible(dataset)
        if item["decision_date"] == dataset["evaluation_dates"][7]
    )

    assert candidate["exit_date"] == candidate["signal_date"]
    assert candidate["exit_price"] == pytest.approx(candidate["stop_price"])
    assert candidate["stop_executed"] is True


def _winner() -> dict:
    return {
        "strategy_id": "spy-rsi2-trend-pullback",
        "strategy_version": "v1",
        "family_id": runtime.SPY_RSI2_PULLBACK_FAMILY,
        "rules_hash": "a" * 64,
        "exact_rules": {
            "selected_trial_id": "trial-001",
            "parameters": PARAMETERS,
            "universe": {"symbols": ["SPY"]},
        },
    }


def _market_facts(dataset: dict, *, ask: float) -> dict:
    decision_date = dataset["evaluation_dates"][7]
    next_session_date = dataset["evaluation_dates"][8]
    daily = [
        bar
        for bar in dataset["daily_bars"]["SPY"]
        if bar["date"] <= decision_date
    ]
    observed = datetime.combine(
        date.fromisoformat(next_session_date),
        time(9, 30),
        tzinfo=ZoneInfo("America/New_York"),
    )
    return {
        "selected_trial_id": "trial-001",
        "parameters": PARAMETERS,
        "decision_data": {
            "family_id": runtime.SPY_RSI2_PULLBACK_FAMILY,
            "decision_date": decision_date,
            "next_session_date": next_session_date,
            "calendar_dates": [bar["date"] for bar in daily],
            "daily_history_complete": True,
            "daily_bars": {"SPY": daily},
            "symbols": ["SPY"],
        },
        "quote": {
            "symbol": "SPY",
            "observed_at": observed.isoformat(),
            "halted": False,
            "tradable": True,
            "bid": ask - 0.01,
            "ask": ask,
            "executable_ask_depth": 10_000,
            "recent_real_minute_volume": 500_000,
        },
        "operational": {
            "before_open_account_reconciled": True,
            "before_open_orders_reconciled": True,
            "before_open_protection_reconciled": True,
            "before_open_tradability_reconciled": True,
            "before_open_news_reconciled": True,
            "protective_order_route_ready": True,
            "monitoring_ready": True,
            "protection_failure_safe_cutoff": "15:45:00-04:00",
        },
    }


def test_production_rebuilds_rule_and_recomputes_cost_floor_from_opening_ask():
    dataset = _dataset()
    candidate = next(
        item
        for item in _eligible(dataset)
        if item["decision_date"] == dataset["evaluation_dates"][7]
    )

    result = plugin.evaluate_production(
        _winner(),
        _market_facts(dataset, ask=candidate["entry_price"]),
    )

    assert result["symbol"] == "SPY"
    assert result["holding_trading_days"] == 5
    assert result["protection_time_in_force"] == "gtc"
    assert result["expected_gross_move_fraction"] == pytest.approx(
        candidate["expected_gross_move_fraction"]
    )
    with pytest.raises(
        plugin.DenseStrategyPluginError,
        match="below the frozen cost floor",
    ):
        plugin.evaluate_production(
            _winner(),
            _market_facts(
                dataset,
                ask=candidate["mean_reversion_reference_price"],
            ),
        )

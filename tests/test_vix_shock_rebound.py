from __future__ import annotations

from datetime import date, timedelta

import pytest

import dense_strategy_runtime as runtime


PARAMETERS = {
    "vix_close_minimum": 25.0,
    "minimum_three_session_decline_fraction": 0.015,
    "mean_reversion_sma": 5,
    "stop_atr14": 1.5,
    "maximum_hold_sessions": 5,
    "cooldown_sessions": 5,
}


def _business_days(count: int) -> list[str]:
    result: list[str] = []
    current = date(2020, 1, 2)
    while len(result) < count:
        if current.weekday() < 5:
            result.append(current.isoformat())
        current += timedelta(days=1)
    return result


def _dataset() -> dict:
    days = _business_days(240)
    closes = [100.0 + 0.03 * index for index in range(len(days))]
    closes[205] = closes[202] * 0.99
    closes[206] = closes[202] * 0.98
    closes[207] = closes[202] * 0.97
    closes[208] = closes[207] - 0.70
    closes[209] = closes[207] + 0.80
    splv = []
    vix = []
    for index, (day, close) in enumerate(zip(days, closes, strict=True)):
        opening = close - 0.05
        if index == 208:
            opening = close - 0.50
        splv.append(
            {
                "date": day,
                "open": opening,
                "high": max(opening, close) + 0.40,
                "low": min(opening, close) - 0.40,
                "close": close,
                "volume": 5_000_000,
            }
        )
        vix_close = 30.0 if 207 <= index <= 211 else 20.0
        vix.append(
            {
                "date": day,
                "open": vix_close,
                "high": vix_close + 1.0,
                "low": vix_close - 1.0,
                "close": vix_close,
                "volume": 0,
            }
        )
    return {
        "family_id": runtime.VIX_SHOCK_REBOUND_FAMILY,
        "evaluation_dates": days[200:230],
        "signal_dates": days[200:225],
        "symbols": [
            runtime.VIX_SHOCK_REBOUND_TARGET_SYMBOL,
            runtime.VIX_SHOCK_REBOUND_FEATURE_SYMBOL,
        ],
        "daily_bars": {
            runtime.VIX_SHOCK_REBOUND_TARGET_SYMBOL: splv,
            runtime.VIX_SHOCK_REBOUND_FEATURE_SYMBOL: vix,
        },
    }


def _eligible(dataset: dict) -> list[dict]:
    prepared = runtime.prepare_dataset(dataset)
    return [
        row
        for row in runtime.build_candidates(
            prepared,
            runtime.VIX_SHOCK_REBOUND_FAMILY,
            PARAMETERS,
        )
        if row["outcome"] == "eligible"
    ]


def test_vix_shock_rule_enters_next_open_and_recovers_to_sma5():
    dataset = _dataset()

    candidate = _eligible(dataset)[0]

    assert candidate["decision_date"] == dataset["evaluation_dates"][7]
    assert candidate["signal_date"] == dataset["evaluation_dates"][8]
    assert candidate["symbol"] == "SPLV"
    assert candidate["vix_close"] == 30.0
    assert candidate["three_session_return_fraction"] <= -0.015
    assert candidate["expected_gross_move_fraction"] >= 0.005
    assert candidate["exit_date"] == dataset["evaluation_dates"][9]
    assert candidate["stop_executed"] is False


def test_vix_shock_rule_resolves_daily_stop_first():
    dataset = _dataset()
    initial = _eligible(dataset)[0]
    entry_day = initial["signal_date"]
    entry_bar = next(
        row
        for row in dataset["daily_bars"]["SPLV"]
        if row["date"] == entry_day
    )
    entry_bar["low"] = initial["stop_price"] - 0.01

    candidate = _eligible(dataset)[0]

    assert candidate["exit_date"] == entry_day
    assert candidate["exit_price"] == pytest.approx(
        candidate["stop_price"]
    )
    assert candidate["stop_executed"] is True


def test_vix_shock_rule_enforces_one_maximum_hold_cooldown():
    dataset = _dataset()

    candidates = _eligible(dataset)

    assert len(candidates) == 1


def test_vix_shock_rule_rejects_parameter_drift():
    dataset = runtime.prepare_dataset(_dataset())
    changed = {**PARAMETERS, "vix_close_minimum": 24.0}

    with pytest.raises(
        runtime.DenseStrategyRuntimeError,
        match="parameters drifted",
    ):
        runtime.build_candidates(
            dataset,
            runtime.VIX_SHOCK_REBOUND_FAMILY,
            changed,
        )

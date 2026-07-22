from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone

import pytest

import dense_strategy_runtime as runtime


def _days(count: int) -> list[str]:
    start = date(2024, 1, 1)
    return [(start + timedelta(days=index)).isoformat() for index in range(count)]


def _daily_bar(day: str, close: float, *, low: float | None = None) -> dict:
    opening = close - 0.1
    return {
        "date": day,
        "open": opening,
        "high": max(opening, close) + 0.5,
        "low": low if low is not None else min(opening, close) - 0.5,
        "close": close,
        "volume": 2_000_000,
    }


def test_pullback_uses_only_completed_decision_bar_and_enters_next_open():
    days = _days(230)
    closes = [100 + 0.2 * index for index in range(230)]
    closes[217:221] = [143.4, 142.0, 140.0, 138.0]
    bars = [_daily_bar(day, close) for day, close in zip(days, closes, strict=True)]
    evaluation_dates = days[220:229]
    dataset = {
        "family_id": runtime.ETF_PULLBACK_FAMILY,
        "evaluation_dates": evaluation_dates,
        "daily_bars": {"SPY": bars},
    }
    parameters = {
        "trend_sma": 100,
        "rsi2_maximum": 10,
        "three_session_decline_fraction": 0.02,
        "stop_atr14": 1.0,
        "maximum_hold_sessions": 3,
    }

    candidates = runtime.build_candidates(
        dataset, runtime.ETF_PULLBACK_FAMILY, parameters
    )
    candidate = next(
        item for item in candidates if item["decision_date"] == evaluation_dates[0]
    )

    assert candidate["signal_date"] == evaluation_dates[1]
    assert candidate["entry_price"] == pytest.approx(bars[221]["open"])
    assert candidate["entry_price"] != pytest.approx(bars[220]["close"])
    assert candidate["exit_date"] <= evaluation_dates[3]


def _minute_bar(
    day: str,
    minute: int,
    *,
    opening: float,
    close: float,
    low: float | None = None,
    high: float | None = None,
) -> dict:
    volume = 1_000.0
    session_open = datetime.combine(
        date.fromisoformat(day), time(9, 30), tzinfo=timezone(timedelta(hours=-4))
    )
    return {
        "timestamp": (session_open + timedelta(minutes=minute)).isoformat(),
        "open": opening,
        "high": high if high is not None else max(opening, close) + 0.05,
        "low": low if low is not None else min(opening, close) - 0.05,
        "close": close,
        "volume": volume,
        "vwap_numerator": close * volume,
        "vwap_denominator": volume,
    }


def _minute_session(day: str, opening_return: float, *, stop_and_target: bool = False):
    opening = 100.0
    window_close = opening * (1 + opening_return)
    bars = []
    previous = opening
    for minute in range(15):
        close = opening + (window_close - opening) * (minute + 1) / 15
        bars.append(
            _minute_bar(day, minute, opening=previous, close=close)
        )
        previous = close
    reclaim_close = max(100.5, window_close + 0.5)
    bars.append(
        _minute_bar(day, 15, opening=previous, close=reclaim_close)
    )
    entry_open = reclaim_close + 0.1
    bars.append(
        _minute_bar(
            day,
            16,
            opening=entry_open,
            close=entry_open,
            low=entry_open - 5.0 if stop_and_target else entry_open - 0.05,
            high=entry_open + 5.0 if stop_and_target else entry_open + 0.05,
        )
    )
    bars.append(
        _minute_bar(day, 17, opening=entry_open, close=entry_open + 0.1)
    )
    return bars


def _intraday_dataset() -> dict:
    days = _days(61)
    minute_bars = {}
    for index, day in enumerate(days[:-1]):
        opening_return = ((index % 7) - 3) * 0.0005
        minute_bars[day] = {"SPY": _minute_session(day, opening_return)}
    minute_bars[days[-1]] = {
        "SPY": _minute_session(days[-1], -0.05, stop_and_target=True)
    }
    return {
        "family_id": runtime.INTRADAY_ETF_FAMILY,
        "evaluation_dates": [days[-1]],
        "symbols": ["SPY"],
        "regular_session_minutes_by_date": {day: 18 for day in days},
        "minute_bars": minute_bars,
    }


def test_intraday_reclaim_enters_next_bar_and_resolves_ambiguity_stop_first():
    dataset = _intraday_dataset()
    parameters = {
        "opening_window_minutes": 15,
        "downside_z_threshold": -1.5,
        "vwap_reclaim_completed_bars": 1,
        "stop_intraday_atr": 1.0,
        "target_r": 1.0,
    }

    candidates = runtime.build_candidates(
        dataset, runtime.INTRADAY_ETF_FAMILY, parameters
    )

    assert len(candidates) == 1
    candidate = candidates[0]
    assert candidate["entry_price"] == pytest.approx(
        dataset["minute_bars"][dataset["evaluation_dates"][0]]["SPY"][16]["open"]
    )
    assert candidate["stop_executed"] is True
    assert candidate["exit_price"] == pytest.approx(candidate["stop_price"])


def test_trial_compounds_account_and_cost_stress_is_monotonic():
    result = runtime.evaluate_trial(
        _intraday_dataset(),
        family_id=runtime.INTRADAY_ETF_FAMILY,
        trial_id="trial-cost-stress",
        parameters={
            "opening_window_minutes": 15,
            "downside_z_threshold": -1.5,
            "vwap_reclaim_completed_bars": 1,
            "stop_intraday_atr": 1.0,
            "target_r": 1.0,
        },
        account_policy={
            "starting_equity": 100_000.0,
            "risk_fraction": 0.005,
            "maximum_concurrent_positions": 3,
            "maximum_aggregate_risk_fraction": 0.0125,
            "maximum_gross_notional_fraction": 1.0,
        },
    )

    growth = [
        result["scenarios"][f"{cost}bps"]["total_log_growth"]
        for cost in (5, 10, 20)
    ]
    assert growth[0] > growth[1] > growth[2]
    assert len(result["metrics"]["oof_daily_account_returns"]) == 1
    assert len(result["metrics"]["oof_filled_account_returns"]) == 1
    assert len(result["maturity_rows"][0]["signals"]) == 1
    assert result["maturity_rows"][0]["signals"][0]["stop_executed"] is True


def test_prepared_dataset_reuses_normalized_rows_across_trials():
    source = _intraday_dataset()
    prepared = runtime.prepare_dataset(source)

    first = runtime.build_candidates(
        prepared,
        runtime.INTRADAY_ETF_FAMILY,
        {
            "opening_window_minutes": 15,
            "downside_z_threshold": -1.5,
            "vwap_reclaim_completed_bars": 1,
            "stop_intraday_atr": 1.0,
            "target_r": 1.0,
        },
    )
    second = runtime.build_candidates(
        prepared,
        runtime.INTRADAY_ETF_FAMILY,
        {
            "opening_window_minutes": 15,
            "downside_z_threshold": -1.5,
            "vwap_reclaim_completed_bars": 1,
            "stop_intraday_atr": 1.5,
            "target_r": 1.5,
        },
    )

    assert "_prepared_minute_bars" in prepared
    assert first[0]["signal_id"] == second[0]["signal_id"]

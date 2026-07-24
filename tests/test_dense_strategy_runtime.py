from __future__ import annotations

import itertools
import math
import time as wall_time
from datetime import date, datetime, time, timedelta, timezone

import pytest

import dense_strategy_runtime as runtime
from learning_experiment import build_rolling_origin_plan


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


def test_standardization_uses_only_latest_sixty_completed_observations():
    recent = [float(index) for index in range(60)]

    assert runtime._z_score(70.0, [-10_000.0, *recent]) == pytest.approx(
        runtime._z_score(70.0, recent)
    )


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


def test_cost_scenarios_share_the_stressed_contention_selection():
    calendar = _days(2)
    candidates = [
        {
            "signal_id": "cost-sensitive-entry",
            "signal_date": calendar[0],
            "decision_date": calendar[0],
            "symbol": "SPY",
            "outcome": "eligible",
            "rank": 1,
            "entry_price": 99.9,
            "stop_price": 50.0,
            "exit_date": calendar[1],
            "exit_price": 99.9,
            "marks": {calendar[0]: 99.9, calendar[1]: 99.9},
            "stop_executed": False,
            "planned_stop_distance": 49.9,
        }
    ]
    policy = {
        "starting_equity": 100.0,
        "risk_fraction": 0.5,
        "maximum_concurrent_positions": 1,
        "maximum_aggregate_risk_fraction": 0.5,
        "maximum_gross_notional_fraction": 1.0,
    }

    scenarios = runtime._shared_cost_scenarios(
        calendar,
        candidates,
        policy,
        rolling_origin_plan=None,
    )

    assert all(not scenario["closed_trades"] for scenario in scenarios.values())
    assert scenarios[5]["trial_accounting"][0] == {
        "date": calendar[0],
        "signal_id": "cost-sensitive-entry",
        "outcome": "capital_blocked",
        "reasons": ["shared_stress_cost_contention"],
    }


def test_rolling_cost_scenarios_intersect_shared_selection_by_fold():
    calendar = _days(4)
    candidates = [
        {
            "signal_id": f"signal-{index}",
            "signal_date": calendar[index],
            "decision_date": calendar[index],
            "symbol": "SPY",
            "outcome": "eligible",
            "rank": 1,
            "entry_price": 100.0,
            "stop_price": 99.0,
            "exit_date": calendar[index + 1],
            "exit_price": 101.0,
            "marks": {
                calendar[index]: 100.0,
                calendar[index + 1]: 101.0,
            },
            "stop_executed": False,
            "planned_stop_distance": 1.0,
        }
        for index in (0, 2)
    ]
    policy = {
        "starting_equity": 100_000.0,
        "risk_fraction": 0.005,
        "maximum_concurrent_positions": 1,
        "maximum_aggregate_risk_fraction": 0.005,
        "maximum_gross_notional_fraction": 1.0,
    }
    plan = [
        {
            "fold": 1,
            "entry_dates": [calendar[0]],
            "settlement_only_dates": [calendar[1]],
            "test_dates": calendar[:2],
        },
        {
            "fold": 2,
            "entry_dates": [calendar[2]],
            "settlement_only_dates": [calendar[3]],
            "test_dates": calendar[2:],
        },
    ]

    scenarios = runtime._shared_cost_scenarios(
        plan,
        candidates,
        policy,
        rolling_origin_plan=plan,
    )

    assert {
        trade["signal_id"] for trade in scenarios[5]["closed_trades"]
    } == {"signal-0", "signal-2"}


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


def test_intraday_selects_the_first_observable_reclaim_without_future_ranking():
    dataset = _intraday_dataset()
    current_day = dataset["evaluation_dates"][0]
    for index, (day, symbols) in enumerate(dataset["minute_bars"].items()):
        opening_return = ((index % 7) - 3) * 0.0005
        symbols["QQQ"] = _minute_session(
            day,
            -0.04 if day == current_day else opening_return,
            stop_and_target=day == current_day,
        )
    dataset["symbols"] = ["SPY", "QQQ"]
    spy = dataset["minute_bars"][current_day]["SPY"]
    spy[15] = _minute_bar(
        current_day,
        15,
        opening=float(spy[14]["close"]),
        close=float(spy[14]["close"]) + 0.05,
    )

    candidates = runtime.build_candidates(
        dataset,
        runtime.INTRADAY_ETF_FAMILY,
        {
            "opening_window_minutes": 15,
            "downside_z_threshold": -1.5,
            "vwap_reclaim_completed_bars": 1,
            "stop_intraday_atr": 1.0,
            "target_r": 1.0,
        },
    )

    assert len(candidates) == 1
    assert candidates[0]["symbol"] == "QQQ"
    assert candidates[0]["entry_price"] == pytest.approx(
        dataset["minute_bars"][current_day]["QQQ"][16]["open"]
    )


def test_intraday_missing_fixed_universe_date_is_an_explicit_zero_return_day():
    days = _days(62)
    missed_day, valid_day = days[-2:]
    minute_bars = {}
    for index, day in enumerate(days):
        opening_return = ((index % 7) - 3) * 0.0005
        minute_bars[day] = {
            "SPY": _minute_session(
                day,
                -0.05 if day == valid_day else opening_return,
                stop_and_target=day == valid_day,
            ),
            "QQQ": _minute_session(
                day,
                -0.05 if day == missed_day else opening_return,
                stop_and_target=day == missed_day,
            ),
        }
    del minute_bars[missed_day]["SPY"]
    dataset = runtime.prepare_dataset(
        {
            "family_id": runtime.INTRADAY_ETF_FAMILY,
            "evaluation_dates": [missed_day, valid_day],
            "symbols": ["SPY", "QQQ"],
            "regular_session_minutes_by_date": {
                day: 18 for day in days
            },
            "missed_data_dates": [missed_day],
            "minute_bars": minute_bars,
        }
    )

    result = runtime.evaluate_trial(
        dataset,
        family_id=runtime.INTRADAY_ETF_FAMILY,
        trial_id="trial-missed-data",
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

    missed = result["trial_accounting"][0]
    assert missed["date"] == missed_day
    assert missed["outcome"] == "zero_return_day"
    assert missed["zero_return_reason"] == "missed_data"
    assert missed["session_outcome"] == "missed_data"
    assert missed["new_entries"] == 0
    assert result["maturity_rows"][0]["session_outcome"] == "missed_data"
    assert all(
        candidate["signal_date"] != missed_day
        for candidate in result["candidate_accounting"]
    )


def test_intraday_partial_universe_requires_a_frozen_missed_data_date():
    dataset = _intraday_dataset()
    dataset["symbols"] = ["SPY", "QQQ"]

    with pytest.raises(
        runtime.DenseStrategyRuntimeError,
        match="complete frozen universe",
    ):
        runtime.prepare_dataset(dataset)


def _oversold_session(day: str) -> list[dict]:
    bars: list[dict] = []
    previous = 100.0
    for minute in range(30):
        close = 100.0 - 4.0 * (minute + 1) / 30
        bars.append(
            _minute_bar(
                day,
                minute,
                opening=previous,
                close=close,
                low=close - 0.2,
                high=max(previous, close) + 0.02,
            )
        )
        previous = close
    bars.append(
        _minute_bar(
            day,
            30,
            opening=96.0,
            close=100.5,
            low=95.9,
            high=100.6,
        )
    )
    bars.append(
        _minute_bar(
            day,
            31,
            opening=100.6,
            close=100.6,
            low=95.0,
            high=108.0,
        )
    )
    for minute in range(32, 390):
        bars.append(
            _minute_bar(
                day,
                minute,
                opening=100.6,
                close=100.6,
            )
        )
    return bars


def test_oversold_reversal_enters_next_bar_and_resolves_stop_first():
    day = _days(1)[0]
    dataset = runtime.prepare_dataset(
        {
            "family_id": runtime.OVERSOLD_REVERSAL_FAMILY,
            "evaluation_dates": [day],
            "candidate_symbols_by_date": {day: ["AAA"]},
            "regular_session_minutes_by_date": {day: 390},
            "minute_bars": {day: {"AAA": _oversold_session(day)}},
        }
    )

    candidates = runtime.build_candidates(
        dataset,
        runtime.OVERSOLD_REVERSAL_FAMILY,
        {
            "lookback_minutes": 30,
            "selloff_threshold": -0.03,
            "rsi_period": 5,
            "rsi_maximum": 20.0,
            "target_r": 1.5,
        },
    )

    assert len(candidates) == 1
    candidate = candidates[0]
    assert candidate["trigger_index"] == 30
    assert candidate["entry_price"] == pytest.approx(100.6)
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
    signal = result["maturity_rows"][0]["signals"][0]
    assert signal["stop_executed"] is True
    assert signal["net_account_log_growth"] == pytest.approx(
        math.log1p(signal["primary_account_return_fraction"])
    )
    assert signal["stress_20bps_account_log_growth"] == pytest.approx(
        math.log1p(signal["stress_20bps_account_return_fraction"])
    )


def test_trial_serializes_infinite_profit_factor_without_nonfinite_json(
    monkeypatch,
):
    monkeypatch.setattr(runtime, "profit_factor", lambda _values: math.inf)

    result = runtime.evaluate_trial(
        _intraday_dataset(),
        family_id=runtime.INTRADAY_ETF_FAMILY,
        trial_id="trial-infinite-profit-factor",
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

    metrics = result["metrics"]
    assert metrics["stress_20bps_profit_factor"] == 0.0
    assert metrics["stress_20bps_profit_factor_is_infinite"] is True
    assert math.isfinite(metrics["stress_20bps_profit_factor"])


def test_rolling_origin_path_excludes_training_and_finishes_each_fold_flat():
    days = _days(80)
    plan = build_rolling_origin_plan(days)
    candidates = []
    for fold in plan:
        entry = fold["entry_dates"][-1]
        exit_day = fold["test_dates"][-1]
        first = fold["test_dates"].index(entry)
        marks = {day: 101.0 for day in fold["test_dates"][first:]}
        candidates.append(
            {
                "signal_id": f"{entry}-fold-{fold['fold']}",
                "signal_date": entry,
                "outcome": "eligible",
                "rank": 1,
                "entry_price": 100.0,
                "stop_price": 99.0,
                "exit_date": exit_day,
                "exit_price": 101.0,
                "marks": marks,
            }
        )
    scenario = runtime._rolling_origin_scenario(
        plan,
        candidates,
        {
            "starting_equity": 100_000.0,
            "risk_fraction": 0.005,
            "maximum_concurrent_positions": 3,
            "maximum_aggregate_risk_fraction": 0.0125,
            "maximum_gross_notional_fraction": 1.0,
        },
        20,
    )

    observed_dates = [row["date"] for row in scenario["account_path"]]
    assert observed_dates == [
        day for fold in plan for day in fold["test_dates"]
    ]
    assert not set(observed_dates) & set(plan[0]["train_dates"])
    for fold in plan:
        final = next(
            row
            for row in scenario["account_path"]
            if row["date"] == fold["test_dates"][-1]
        )
        assert final["open_positions"] == 0
    assert len(scenario["closed_trades"]) == len(plan)


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


def test_production_pullback_rebuilds_the_historical_rank_from_completed_bars():
    days = _days(230)
    closes = [100 + 0.2 * index for index in range(230)]
    closes[217:221] = [143.4, 142.0, 140.0, 138.0]
    bars = [_daily_bar(day, close) for day, close in zip(days, closes, strict=True)]
    parameters = {
        "trend_sma": 100,
        "rsi2_maximum": 10,
        "three_session_decline_fraction": 0.02,
        "stop_atr14": 1.0,
        "maximum_hold_sessions": 3,
    }
    decision_date = days[220]
    historical = runtime.build_candidates(
        {
            "family_id": runtime.ETF_PULLBACK_FAMILY,
            "evaluation_dates": days[220:229],
            "daily_bars": {"SPY": bars},
        },
        runtime.ETF_PULLBACK_FAMILY,
        parameters,
    )[0]
    production = runtime.evaluate_production_signal(
        {
            "family_id": runtime.ETF_PULLBACK_FAMILY,
            "decision_date": decision_date,
            "next_session_date": days[221],
            "calendar_dates": days[:221],
            "daily_history_complete": True,
            "symbols": ["SPY"],
            "daily_bars": {
                "SPY": [bar for bar in bars if bar["date"] <= decision_date]
            },
        },
        family_id=runtime.ETF_PULLBACK_FAMILY,
        parameters=parameters,
        frozen_universe={"symbols": ["SPY"]},
    )

    assert production["symbol"] == historical["symbol"] == "SPY"
    assert production["rank"] == historical["rank"] == 1
    assert production["atr"] == pytest.approx(
        historical["entry_price"] - historical["stop_price"]
    )
    assert production["holding_trading_days"] == 3

    with pytest.raises(
        runtime.DenseStrategyRuntimeError,
        match="not chronologically adjacent",
    ):
        runtime.evaluate_production_signal(
            {
                "family_id": runtime.ETF_PULLBACK_FAMILY,
                "decision_date": decision_date,
                "next_session_date": days[225],
                "calendar_dates": days[:221],
                "daily_history_complete": True,
                "symbols": ["SPY"],
                "daily_bars": {
                    "SPY": [
                        bar for bar in bars if bar["date"] <= decision_date
                    ]
                },
            },
            family_id=runtime.ETF_PULLBACK_FAMILY,
            parameters=parameters,
            frozen_universe={"symbols": ["SPY"]},
        )


def test_production_equity_rank_rebuilds_top_250_and_residual_signal():
    days = _days(205)
    spy_closes = [100 + 0.1 * index for index in range(len(days))]
    daily_bars = {
        "SPY": [
            _daily_bar(day, close)
            for day, close in zip(days, spy_closes, strict=True)
        ]
    }
    references = {}
    for ordinal in range(500):
        symbol = f"S{ordinal:03d}"
        closes = [
            (50 + 0.05 * index) * (1 + 0.002 * ((index % 7) - 3))
            for index in range(len(days))
        ]
        if ordinal == 0:
            closes[-1] *= 0.94
        daily_bars[symbol] = [
            _daily_bar(day, close)
            for day, close in zip(days, closes, strict=True)
        ]
        if ordinal == 0:
            for bar in daily_bars[symbol]:
                bar["volume"] = 4_000_000
        references[symbol] = {
            "active": True,
            "type": "CS",
            "listing_identity": f"listing-{ordinal:03d}",
        }
    signal = runtime.evaluate_production_signal(
        {
            "family_id": runtime.EQUITY_RESIDUAL_FAMILY,
            "decision_date": days[-1],
            "next_session_date": "2024-07-24",
            "calendar_dates": days,
            "daily_history_complete": True,
            "daily_bars": daily_bars,
            "reference_snapshot": references,
            "reference_snapshot_complete": True,
            "reference_source_total": len(references),
        },
        family_id=runtime.EQUITY_RESIDUAL_FAMILY,
        parameters={
            "prior_return_sessions": 1,
            "residual_z_threshold": -1.5,
            "market_trend_gate": "SPY>SMA100",
            "stop_atr14": 1.0,
            "hold_sessions": 2,
        },
        frozen_universe={"point_in_time": "top-250"},
    )

    assert signal["symbol"] == "S000"
    assert signal["rank"] == 1
    assert signal["expected_gross_move_fraction"] >= 0.005


def _full_minute_session(day: str, opening_return: float) -> list[dict]:
    bars = _minute_session(day, opening_return)
    previous = float(bars[-1]["close"])
    for minute in range(len(bars), 390):
        bars.append(_minute_bar(day, minute, opening=previous, close=previous))
    return bars


def test_production_intraday_signal_requires_current_first_reclaim_bar():
    days = _days(61)
    minute_bars = {
        day: {
            "SPY": _full_minute_session(
                day, ((index % 7) - 3) * 0.0005
            )
        }
        for index, day in enumerate(days[:-1])
    }
    current = _minute_session(days[-1], -0.05)[:16]
    minute_bars[days[-1]] = {"SPY": current}
    parameters = {
        "opening_window_minutes": 15,
        "downside_z_threshold": -1.5,
        "vwap_reclaim_completed_bars": 1,
        "stop_intraday_atr": 1.0,
        "target_r": 1.0,
    }
    signal = runtime.evaluate_production_signal(
        {
            "family_id": runtime.INTRADAY_ETF_FAMILY,
            "session_date": days[-1],
            "calendar_sessions": days,
            "minute_history_complete": True,
            "symbols": ["SPY"],
            "minute_bars": minute_bars,
        },
        family_id=runtime.INTRADAY_ETF_FAMILY,
        parameters=parameters,
        frozen_universe={"symbols": ["SPY"]},
    )
    assert signal["symbol"] == "SPY"
    assert signal["trigger_bar_timestamp"] == current[-1]["timestamp"]

    later = dict(minute_bars)
    later[days[-1]] = {
        "SPY": [
            *current,
            _minute_bar(
                days[-1],
                16,
                opening=float(current[-1]["close"]),
                close=float(current[-1]["close"]),
            ),
        ]
    }
    with pytest.raises(runtime.DenseStrategyRuntimeError, match="earlier bar"):
        runtime.evaluate_production_signal(
            {
                "family_id": runtime.INTRADAY_ETF_FAMILY,
                "session_date": days[-1],
                "calendar_sessions": days,
                "minute_history_complete": True,
                "symbols": ["SPY"],
                "minute_bars": later,
            },
            family_id=runtime.INTRADAY_ETF_FAMILY,
            parameters=parameters,
            frozen_universe={"symbols": ["SPY"]},
        )


def test_real_48_trial_equity_runtime_reuses_features_under_sixty_seconds():
    days = _days(325)
    evaluation_dates = days[-120:]
    spy_closes = [100 + 0.04 * index for index in range(len(days))]
    daily_bars = {
        "SPY": [
            _daily_bar(day, close)
            for day, close in zip(days, spy_closes, strict=True)
        ]
    }
    symbols = [f"S{ordinal:03d}" for ordinal in range(250)]
    universe_symbols = [*symbols, "MISSING"]
    for ordinal, symbol in enumerate(symbols):
        closes = [
            (40 + ordinal * 0.02 + 0.016 * index)
            * (1 + 0.002 * (((index + ordinal) % 11) - 5))
            for index in range(len(days))
        ]
        daily_bars[symbol] = [
            _daily_bar(day, close)
            for day, close in zip(days, closes, strict=True)
        ]
    dataset = runtime.prepare_dataset(
        {
            "family_id": runtime.EQUITY_RESIDUAL_FAMILY,
            "evaluation_dates": evaluation_dates,
            "daily_bars": daily_bars,
            "universe_by_date": {
                day: universe_symbols for day in evaluation_dates[::2]
            },
            "universe_identity_by_date": {
                day: {
                    symbol: f"listing-{ordinal:03d}"
                    for ordinal, symbol in enumerate(universe_symbols)
                }
                for day in evaluation_dates[::2]
            },
        }
    )
    policy = {
        "starting_equity": 100_000.0,
        "risk_fraction": 0.005,
        "maximum_concurrent_positions": 3,
        "maximum_aggregate_risk_fraction": 0.0125,
        "maximum_gross_notional_fraction": 1.0,
    }
    trials = itertools.product(
        (1, 3),
        (-1.5, -2.0, -2.5),
        ("SPY>SMA100", "SPY>SMA200"),
        (1.0, 1.5),
        (2, 5),
    )
    started = wall_time.monotonic()
    results = [
        runtime.evaluate_trial(
            dataset,
            family_id=runtime.EQUITY_RESIDUAL_FAMILY,
            trial_id=f"trial-{index:02d}",
            parameters={
                "prior_return_sessions": window,
                "residual_z_threshold": threshold,
                "market_trend_gate": trend,
                "stop_atr14": stop,
                "hold_sessions": hold,
            },
            account_policy=policy,
        )
        for index, (window, threshold, trend, stop, hold) in enumerate(trials)
    ]
    elapsed = wall_time.monotonic() - started
    uncached = runtime.evaluate_trial(
        {key: value for key, value in dataset.items() if not key.startswith("_")},
        family_id=runtime.EQUITY_RESIDUAL_FAMILY,
        trial_id="trial-00",
        parameters={
            "prior_return_sessions": 1,
            "residual_z_threshold": -1.5,
            "market_trend_gate": "SPY>SMA100",
            "stop_atr14": 1.0,
            "hold_sessions": 2,
        },
        account_policy=policy,
    )
    uncached_strict = runtime.evaluate_trial(
        {key: value for key, value in dataset.items() if not key.startswith("_")},
        family_id=runtime.EQUITY_RESIDUAL_FAMILY,
        trial_id="trial-08",
        parameters={
            "prior_return_sessions": 1,
            "residual_z_threshold": -2.0,
            "market_trend_gate": "SPY>SMA100",
            "stop_atr14": 1.0,
            "hold_sessions": 2,
        },
        account_policy=policy,
    )

    assert len(results) == 48
    assert set(dataset["_equity_residual_feature_cache"]) == {1, 3}
    assert len(dataset["_equity_residual_candidate_cache"]) == 16
    assert results[0]["candidate_accounting"] == uncached["candidate_accounting"]
    assert (
        results[8]["candidate_accounting"]
        == uncached_strict["candidate_accounting"]
    )
    assert elapsed <= 60

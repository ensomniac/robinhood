from __future__ import annotations

from datetime import date, timedelta

import multi_asset_etf_tsmom_capacity as contract
import multi_asset_etf_tsmom_capacity_run as capacity_run


def _weekdays(start: date, count: int) -> list[str]:
    values: list[str] = []
    current = start
    while len(values) < count:
        if current.weekday() < 5:
            values.append(current.isoformat())
        current += timedelta(days=1)
    return values


def _graph(*, positive: bool) -> dict:
    dates = _weekdays(date(2021, 12, 1), 1065)
    schedule = capacity_run._weekly_schedule(dates)
    closes = []
    for index, day in enumerate(dates):
        close = 100.0 + index if positive else 2000.0 - index
        closes.append({"t": f"{day}T00:00:00-05:00", "c": close})
    return {
        "contract_sha256": "contract",
        "dates": dates,
        "closes_by_symbol": {
            symbol: list(closes) for symbol, _asset_class in contract.UNIVERSE
        },
        "schedule_without_signals": schedule,
    }


def test_schedule_is_fixed_weekly_round_robin_with_exact_252_session_lookback():
    graph = _graph(positive=True)
    schedule = graph["schedule_without_signals"]
    assert len(schedule) == 157
    assert all(row["eligible"] for row in schedule)
    assert [row["scheduled_symbol"] for row in schedule[:8]] == [
        "SPY",
        "EFA",
        "EEM",
        "IEF",
        "GLD",
        "DBC",
        "UUP",
        "SPY",
    ]
    positions = {day: index for index, day in enumerate(graph["dates"])}
    assert all(
        positions[row["decision_session"]] - positions[row["lookback_session"]]
        == 252
        for row in schedule
    )


def test_positive_own_price_history_passes_capacity_without_returns():
    state = capacity_run._capacity_state(_graph(positive=True))
    assert state["scheduled_weeks"] == 157
    assert state["eligible_causal_evaluations"] == 157
    assert state["causal_long_signals"] == 157
    assert state["capacity_passed"] is True
    assert state["entry_fills_accessed"] is False
    assert state["exit_or_stop_values_accessed"] is False
    assert state["forward_returns_computed"] == 0
    assert state["market_outcomes_accessed"] is False


def test_negative_own_price_history_fails_capacity_without_parameter_repair():
    state = capacity_run._capacity_state(_graph(positive=False))
    assert state["causal_long_signals"] == 0
    assert state["minimum_required_signals"] == 50
    assert state["signal_shortfall"] == 50
    assert state["capacity_passed"] is False
    assert state["forward_returns_computed"] == 0


def test_capacity_state_never_publishes_prices_or_performance_metrics():
    state = capacity_run._capacity_state(_graph(positive=True))
    assert all("c" not in row for row in state["records"])
    assert all("return" not in row for row in state["records"])
    assert "profit_factor" not in state
    assert "expectancy" not in state
    assert state["broker_actions"] == 0

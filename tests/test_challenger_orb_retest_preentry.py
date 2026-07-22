from __future__ import annotations

from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

import challenger_orb_retest_preentry as preentry


EASTERN = ZoneInfo("America/New_York")


def _at(hour: int, minute: int, second: int = 0) -> datetime:
    return datetime.combine(date(2026, 1, 2), time(hour, minute, second), EASTERN)


def _trade(observed: datetime, price: float) -> dict:
    return {
        "source_timestamp": observed.isoformat(),
        "price": price,
        "size": 100,
        "conditions": ["@"],
        "tape": "C",
        "trade_id": observed.isoformat(),
    }


def _bar(observed: datetime, low: float, high: float, close: float) -> dict:
    return {
        "source_timestamp": observed.isoformat(),
        "open": close,
        "high": high,
        "low": low,
        "close": close,
        "volume": 1000,
        "wap": close,
        "interpolated": False,
    }


def test_terminal_boundary_stops_at_decision_for_valid_rebreak() -> None:
    trades = [_trade(_at(9, 36, 1), 10.01), _trade(_at(9, 38, 5), 10.06)]
    bars = [_bar(_at(9, 37), 9.99, 10.05, 10.02)]
    observed = preentry._terminal_boundary(
        opening_high=10.0,
        search_start=_at(9, 35),
        cutoff=_at(10, 30),
        trades=trades,
        bars=bars,
    )
    assert observed == _at(9, 38, 15)


def test_terminal_boundary_stops_at_failed_touch_bar() -> None:
    trades = [_trade(_at(9, 36, 1), 10.01)]
    bars = [_bar(_at(9, 37), 9.98, 10.01, 9.99)]
    observed = preentry._terminal_boundary(
        opening_high=10.0,
        search_start=_at(9, 35),
        cutoff=_at(10, 30),
        trades=trades,
        bars=bars,
    )
    assert observed == _at(9, 37) + timedelta(minutes=1)

from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import turn_of_month_etf_seasonality_stage0 as stage0


EASTERN = ZoneInfo("America/New_York")


def _rows(count: int = 30, *, start: float = 100.0):
    rows = []
    observed = datetime(2025, 1, 2, tzinfo=EASTERN)
    current = start
    for _ in range(count):
        while observed.weekday() >= 5:
            observed += timedelta(days=1)
        close = current * 1.001
        rows.append(
            {
                "t": observed.isoformat(),
                "o": current,
                "h": close * 1.002,
                "l": current * 0.998,
                "c": close,
                "v": 1_000_000,
                "i": False,
            }
        )
        current = close
        observed += timedelta(days=1)
    return rows


def test_frozen_variant_is_final_second_wave_candidate():
    variant = stage0._variant()
    assert variant["variant_id"] == stage0.VARIANT_ID
    assert variant["mechanism_family"] == "turn-of-month-etf-seasonality"
    assert variant["universe"] == {"symbols": ["SPY"]}
    assert variant["maximum_holding_trading_days"] == 4
    assert variant["maturity_effect"] == "NONE"


def test_stop_precedes_time_exit_and_gap_fill_is_conservative():
    rows = _rows()
    signal_index = 14
    exit_index = 18
    ordinary = stage0._outcome(rows, signal_index, exit_index, 5)
    assert ordinary["exit_reason"] == "time_exit"
    stopped = [dict(row) for row in rows]
    stop = float(stopped[signal_index + 1]["o"]) - 1.5 * stage0.daily._atr(
        stopped, signal_index
    )
    stopped[signal_index + 1]["l"] = stop - 0.01
    stopped_outcome = stage0._outcome(stopped, signal_index, exit_index, 5)
    assert stopped_outcome["exit_reason"] == "stop"
    gapped = [dict(row) for row in rows]
    gapped[signal_index + 2]["o"] = stop - 1
    gapped[signal_index + 2]["l"] = stop - 2
    gap_outcome = stage0._outcome(gapped, signal_index, exit_index, 5)
    assert gap_outcome["exit_reason"] == "stop_gap"
    assert gap_outcome["net_r"] < stopped_outcome["net_r"]


def test_monthly_event_mapping_uses_final_and_third_next_month_sessions():
    dates = []
    observed = datetime(2022, 1, 3, tzinfo=EASTERN)
    while observed.date().isoformat() <= "2026-01-08":
        if observed.weekday() < 5:
            dates.append(observed.date().isoformat())
        observed += timedelta(days=1)
    events = stage0._monthly_events(dates)
    assert len(events) == 36
    assert events[0]["month"] == "2023-01"
    assert events[0]["entry_date"] == "2023-01-31"
    assert events[0]["exit_date"] == "2023-02-03"
    assert events[-1]["month"] == "2025-12"
    assert events[-1]["entry_date"] == "2025-12-31"
    assert events[-1]["exit_date"] == "2026-01-05"

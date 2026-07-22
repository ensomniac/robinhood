from __future__ import annotations

from datetime import datetime, timedelta
import json
from pathlib import Path
from zoneinfo import ZoneInfo

import broad_etf_trend_pullback_stage0 as stage0


EASTERN = ZoneInfo("America/New_York")
ROOT = Path(__file__).resolve().parents[1]


def _rows(count: int = 220, *, start: float = 100.0, daily_gain: float = 0.001):
    rows = []
    current = start
    observed = datetime(2024, 1, 2, tzinfo=EASTERN)
    for _ in range(count):
        while observed.weekday() >= 5:
            observed += timedelta(days=1)
        close = current * (1 + daily_gain)
        rows.append(
            {
                "t": observed.isoformat(),
                "o": current,
                "h": max(current, close) * 1.002,
                "l": min(current, close) * 0.998,
                "c": close,
                "v": 1_000_000,
                "i": False,
            }
        )
        current = close
        observed += timedelta(days=1)
    return rows


def test_wilder_rsi_uses_only_present_and_past_closes():
    closes = [10.0, 11.0, 10.0, 9.0, 10.0, 8.0]
    rsi = stage0._rsi_wilder(closes)
    assert rsi[:2] == [None, None]
    assert rsi[2] == 50.0
    assert 0 < rsi[-1] < 50
    changed = [*closes, 1_000_000.0]
    assert stage0._rsi_wilder(changed)[:-1] == rsi


def test_stop_precedes_recovery_and_gap_fill_is_conservative():
    rows = _rows()
    signal_index = 200
    recovered = [dict(row) for row in rows]
    recovered[signal_index + 1]["c"] = float(recovered[signal_index + 1]["o"]) * 1.02
    recovered[signal_index + 1]["h"] = recovered[signal_index + 1]["c"]
    ordinary = stage0._outcome(recovered, signal_index, 5)
    assert ordinary["exit_reason"] == "recovery_exit"
    stopped = [dict(row) for row in recovered]
    stop = float(stopped[signal_index + 1]["o"]) - 1.5 * stage0.daily._atr(
        stopped, signal_index
    )
    stopped[signal_index + 1]["l"] = stop - 0.01
    stopped_outcome = stage0._outcome(stopped, signal_index, 5)
    assert stopped_outcome["exit_reason"] == "stop"
    gapped = [dict(row) for row in rows]
    first_hold = gapped[signal_index + 1]
    first_hold["c"] = float(first_hold["o"]) * 0.996
    first_hold["l"] = float(first_hold["c"])
    second_hold = gapped[signal_index + 2]
    second_hold["o"] = stop - 1
    second_hold["c"] = stop - 1
    second_hold["h"] = stop - 0.9
    second_hold["l"] = stop - 2
    gap_outcome = stage0._outcome(gapped, signal_index, 5)
    assert gap_outcome["exit_reason"] == "stop_gap"
    assert gap_outcome["net_r"] < stopped_outcome["net_r"]


def test_frozen_variant_is_second_in_inspected_second_wave():
    variant = stage0._variant()
    assert variant["variant_id"] == stage0.VARIANT_ID
    assert variant["mechanism_family"] == "broad-etf-trend-pullback"
    assert variant["maximum_holding_trading_days"] == 5
    assert variant["maturity_effect"] == "NONE"


def test_published_activation_binds_complete_whole_provider_input_graph():
    matches = sorted(
        (ROOT / "strategy_tournament/second_wave/activations").glob(
            "broad-etf-trend-pullback-v1-*.json"
        )
    )
    assert len(matches) == 1
    activation = json.loads(matches[0].read_text(encoding="utf-8"))
    assert activation == stage0.build_activation()
    assert activation["manifest_sha256"] == stage0.common._self_hash(
        activation, "manifest_sha256"
    )
    assert activation["variant_ordinal"] == 2
    assert activation["source_selection"]["whole_provider"] == "ibkr"
    assert activation["source_selection"]["provider_substitution"] is False
    assert activation["source_selection"]["common_calendar_dates"] == 1008
    assert activation["source_selection"]["evaluation_dates"] == 752
    assert activation["denominator"]["input_symbol_dates"] == 4032
    assert activation["return_evaluation_authorized_before_inspection"] is False
    assert activation["maturity_effect"] == "NONE"


def test_input_inspection_is_absent_before_independent_review():
    matches = sorted(
        (ROOT / "strategy_tournament/second_wave/inspections").glob(
            "broad-etf-trend-pullback-v1-input-*.json"
        )
    )
    assert matches == []

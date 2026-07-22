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


def test_published_input_inspection_authorizes_one_frozen_evaluation():
    matches = sorted(
        (ROOT / "strategy_tournament/second_wave/inspections").glob(
            "broad-etf-trend-pullback-v1-input-*.json"
        )
    )
    assert len(matches) == 1
    inspection = json.loads(matches[0].read_text(encoding="utf-8"))
    assert inspection["inspection_sha256"] == stage0.common._self_hash(
        inspection, "inspection_sha256"
    )
    assert inspection["manifest_sha256"] == (
        "a4f2348809b00bf70994d3c7868a1bce34b8cec578a5270dc32d80afa1f39860"
    )
    assert inspection["input_symbol_dates"] == 4032
    assert inspection["evaluation_dates"] == 752
    assert inspection["provider_requests"] == 0
    assert inspection["returns_computed"] == 0
    assert inspection["maturity_effect"] == "NONE"
    assert inspection["return_evaluation_authorized"] is True
    assert inspection["valid"] is True


def test_published_result_fails_only_twenty_bps_cost_stress():
    matches = sorted(
        (ROOT / "research_results").glob(
            "2026-07-21-broad-etf-trend-pullback-stage0-*.json"
        )
    )
    assert len(matches) == 1
    result = json.loads(matches[0].read_text(encoding="utf-8"))
    assert result["result_sha256"] == stage0.common._self_hash(
        result, "result_sha256"
    )
    denominator = result["denominator"]
    assert denominator["decision_dates"] == 752
    assert denominator["closed_signals"] == 77
    assert (
        denominator["closed_signals"]
        + denominator["no_trade_dates"]
        + denominator["position_occupied_dates"]
        == denominator["decision_dates"]
    )
    assert result["primary_5bps"]["expectancy_r"] > 0
    assert result["primary_5bps"]["profit_factor"] >= 1.10
    assert result["primary_5bps"]["maximum_drawdown_r"] <= 8
    assert result["stress"]["20"]["total_r"] < 0
    assert result["stage0_blockers"] == [
        "20 bps-per-side total R is not positive"
    ]
    assert result["stage0_survived"] is False
    assert result["maturity_effect"] == "NONE"


def test_published_result_inspection_rebuilds_failed_disposition():
    matches = sorted(
        (ROOT / "strategy_tournament/second_wave/inspections").glob(
            "broad-etf-trend-pullback-v1-result-*.json"
        )
    )
    assert len(matches) == 1
    inspection = json.loads(matches[0].read_text(encoding="utf-8"))
    assert inspection["inspection_sha256"] == stage0.common._self_hash(
        inspection, "inspection_sha256"
    )
    assert inspection["result_sha256"] == (
        "2aa31af9eb16c6a268b19df57b5bfc3b9d6950b69fa00cddcbfcb12386a41209"
    )
    assert inspection["closed_signals"] == 77
    assert inspection["stage0_survived"] is False
    assert inspection["provider_requests"] == 0
    assert inspection["broker_actions"] == 0
    assert inspection["maturity_effect"] == "NONE"
    assert inspection["valid"] is True

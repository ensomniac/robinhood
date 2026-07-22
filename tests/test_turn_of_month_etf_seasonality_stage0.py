from __future__ import annotations

from datetime import datetime, timedelta
import json
from pathlib import Path
from zoneinfo import ZoneInfo

import turn_of_month_etf_seasonality_stage0 as stage0


EASTERN = ZoneInfo("America/New_York")
ROOT = Path(__file__).resolve().parents[1]


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


def test_published_activation_binds_all_frozen_months():
    matches = sorted(
        (ROOT / "strategy_tournament/second_wave/activations").glob(
            "turn-of-month-etf-seasonality-v1-*.json"
        )
    )
    assert len(matches) == 1
    activation = json.loads(matches[0].read_text(encoding="utf-8"))
    assert activation == stage0.build_activation()
    assert activation["manifest_sha256"] == stage0.common._self_hash(
        activation, "manifest_sha256"
    )
    assert activation["variant_ordinal"] == 6
    assert activation["base_rules_hash"] == (
        "c206cd4f7bd0a6aa094f85a0eafc75f5dffae3df7c9ae74141284178c85212e2"
    )
    assert activation["source_selection"]["common_calendar_dates"] == 1008
    assert activation["source_selection"]["evaluation_months"] == 36
    assert activation["denominator"]["input_symbol_dates"] == 1008
    assert activation["return_evaluation_authorized_before_inspection"] is False
    assert activation["maturity_effect"] == "NONE"


def test_published_input_inspection_authorizes_one_evaluation():
    activation_path = next(
        (ROOT / "strategy_tournament/second_wave/activations").glob(
            "turn-of-month-etf-seasonality-v1-*.json"
        )
    )
    matches = sorted(
        (ROOT / "strategy_tournament/second_wave/inspections").glob(
            "turn-of-month-etf-seasonality-v1-input-*.json"
        )
    )
    assert len(matches) == 1
    inspection = json.loads(matches[0].read_text(encoding="utf-8"))
    assert inspection == stage0.inspect_activation(activation_path)
    assert inspection["inspection_sha256"] == stage0.common._self_hash(
        inspection, "inspection_sha256"
    )
    assert inspection["input_symbol_dates"] == 1008
    assert inspection["evaluation_months"] == 36
    assert inspection["provider_requests"] == 0
    assert inspection["returns_computed"] == 0
    assert inspection["return_evaluation_authorized"] is True
    assert inspection["maturity_effect"] == "NONE"
    assert inspection["valid"] is True


def test_published_result_fails_four_stage0_gates():
    matches = sorted(
        (ROOT / "research_results").glob(
            "2026-07-21-turn-of-month-etf-seasonality-stage0-*.json"
        )
    )
    assert len(matches) == 1
    result = json.loads(matches[0].read_text(encoding="utf-8"))
    assert result["result_sha256"] == stage0.common._self_hash(result, "result_sha256")
    assert result["denominator"] == {
        "decision_months": 36,
        "closed_signals": 36,
        "no_trade_months": 0,
        "rule_violations": 0,
    }
    assert result["primary_5bps"]["expectancy_r"] < 0
    assert result["primary_5bps"]["profit_factor"] < 1.10
    assert result["primary_5bps"]["maximum_drawdown_r"] > 8
    assert result["stress"]["20"]["total_r"] < 0
    assert result["stage0_blockers"] == [
        "primary expectancy is not positive",
        "primary profit factor is below the Stage 0 minimum",
        "primary drawdown exceeds the Stage 0 maximum",
        "20 bps-per-side total R is not positive",
    ]
    assert result["stage0_survived"] is False
    assert result["maturity_effect"] == "NONE"


def test_published_result_inspection_rebuilds_final_retirement():
    matches = sorted(
        (ROOT / "strategy_tournament/second_wave/inspections").glob(
            "turn-of-month-etf-seasonality-v1-result-*.json"
        )
    )
    assert len(matches) == 1
    inspection = json.loads(matches[0].read_text(encoding="utf-8"))
    assert inspection["inspection_sha256"] == stage0.common._self_hash(
        inspection, "inspection_sha256"
    )
    assert inspection["result_sha256"] == (
        "392a840bb533fd9ace45075774f6318f04acdd69d1a1b9bfacc41bccb310abf6"
    )
    assert inspection["closed_signals"] == 36
    assert inspection["stage0_survived"] is False
    assert inspection["provider_requests"] == 0
    assert inspection["broker_actions"] == 0
    assert inspection["maturity_effect"] == "NONE"
    assert inspection["valid"] is True

from __future__ import annotations

from datetime import datetime, timedelta
import json
from pathlib import Path
from zoneinfo import ZoneInfo

import sector_etf_rotation_stage0 as stage0


EASTERN = ZoneInfo("America/New_York")
ROOT = Path(__file__).resolve().parents[1]


def _rows(count: int = 70, *, start: float = 100.0, daily_gain: float = 0.001):
    rows = []
    current = start
    observed = datetime(2025, 1, 2, tzinfo=EASTERN)
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


def test_daily_row_validation_rejects_bad_ohlc_and_interpolation():
    row = _rows(1)[0]
    day = datetime.fromisoformat(row["t"]).date().isoformat()
    assert stage0._validate_daily_row(row, "XLK", day) == row
    bad = dict(row, l=float(row["h"]) + 1)
    try:
        stage0._validate_daily_row(bad, "XLK", day)
    except stage0.SectorEtfRotationError as exc:
        assert "OHLC" in str(exc)
    else:
        raise AssertionError("bad OHLC must fail closed")
    interpolated = dict(row, i=True)
    try:
        stage0._validate_daily_row(interpolated, "XLK", day)
    except stage0.SectorEtfRotationError as exc:
        assert "unusable volume" in str(exc)
    else:
        raise AssertionError("interpolation must fail closed")


def test_stop_first_daily_outcome_and_gap_fill_are_conservative():
    rows = _rows()
    signal_index = 50
    ordinary = stage0._outcome(rows, signal_index, 5)
    assert ordinary["exit_reason"] == "time_exit"
    stopped = _rows()
    atr = stage0._atr(stopped, signal_index)
    stop = float(stopped[signal_index + 1]["o"]) - 1.5 * atr
    stopped[signal_index + 2]["l"] = stop - 0.01
    stop_outcome = stage0._outcome(stopped, signal_index, 5)
    assert stop_outcome["exit_reason"] == "stop"
    assert stop_outcome["stop_executed"] is True
    gapped = _rows()
    gapped[signal_index + 2]["o"] = stop - 1
    gapped[signal_index + 2]["l"] = stop - 2
    gap_outcome = stage0._outcome(gapped, signal_index, 5)
    assert gap_outcome["exit_reason"] == "stop_gap"
    assert gap_outcome["net_r"] < stop_outcome["net_r"]


def test_atr_and_sma_use_only_completed_signal_and_prior_rows():
    rows = _rows()
    closes = [float(row["c"]) for row in rows]
    assert stage0._sma(closes, 49, 50) == sum(closes[:50]) / 50
    atr = stage0._atr(rows, 50)
    assert atr > 0
    changed_future = [dict(row) for row in rows]
    changed_future[51].update(h=1_000_000, l=0.01, c=500_000)
    assert stage0._atr(changed_future, 50) == atr


def test_frozen_variant_is_first_in_inspected_second_wave():
    variant = stage0._variant()
    assert variant["variant_id"] == stage0.VARIANT_ID
    assert variant["mechanism_family"] == "sector-etf-rotation"
    assert variant["maximum_holding_trading_days"] == 5
    assert variant["maturity_effect"] == "NONE"


def test_published_activation_binds_complete_whole_provider_input_graph():
    matches = sorted(
        (ROOT / "strategy_tournament/second_wave/activations").glob(
            "sector-etf-rotation-v1-*.json"
        )
    )
    assert len(matches) == 1
    activation = json.loads(matches[0].read_text(encoding="utf-8"))
    assert activation == stage0.build_activation()
    assert activation["manifest_sha256"] == stage0.common._self_hash(
        activation, "manifest_sha256"
    )
    assert activation["source_selection"]["whole_provider"] == "ibkr"
    assert activation["source_selection"]["provider_substitution"] is False
    assert activation["source_selection"]["common_calendar_dates"] == 1008
    assert activation["source_selection"]["evaluation_dates"] == 752
    assert activation["denominator"]["input_symbol_dates"] == 12_096
    assert activation["return_evaluation_authorized_before_inspection"] is False
    assert activation["maturity_effect"] == "NONE"


def test_published_input_inspection_authorizes_one_frozen_evaluation():
    matches = sorted(
        (ROOT / "strategy_tournament/second_wave/inspections").glob(
            "sector-etf-rotation-v1-input-*.json"
        )
    )
    assert len(matches) == 1
    inspection = json.loads(matches[0].read_text(encoding="utf-8"))
    assert inspection["inspection_sha256"] == stage0.common._self_hash(
        inspection, "inspection_sha256"
    )
    assert inspection["manifest_sha256"] == (
        "5360bfd6c12065169975ec828a813f7e6f5042d87979131542b39659ca5df632"
    )
    assert inspection["input_symbol_dates"] == 12_096
    assert inspection["evaluation_dates"] == 752
    assert inspection["provider_requests"] == 0
    assert inspection["returns_computed"] == 0
    assert inspection["maturity_effect"] == "NONE"
    assert inspection["return_evaluation_authorized"] is True
    assert inspection["valid"] is True

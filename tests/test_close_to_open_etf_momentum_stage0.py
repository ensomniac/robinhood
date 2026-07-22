from __future__ import annotations

from datetime import datetime, timedelta
import json
from pathlib import Path
from zoneinfo import ZoneInfo

import close_to_open_etf_momentum_stage0 as stage0


EASTERN = ZoneInfo("America/New_York")
ROOT = Path(__file__).resolve().parents[1]


def _session(day: datetime, *, start: float = 100.0, gain: float = 0.01):
    rows = []
    for index in range(26):
        observed = day.replace(hour=9, minute=30) + timedelta(minutes=15 * index)
        open_price = start * (1 + gain * index / 26)
        close = start * (1 + gain * (index + 1) / 26)
        rows.append(
            {
                "t": observed.isoformat(),
                "o": open_price,
                "h": max(open_price, close) * 1.0005,
                "l": min(open_price, close) * 0.9995,
                "c": close,
                "v": 1_000_000,
                "i": False,
            }
        )
    return rows


def test_intraday_validation_requires_ordered_regular_session_rows():
    day = datetime(2025, 1, 2, tzinfo=EASTERN)
    rows = _session(day)
    assert stage0._validate_intraday_rows(rows, "SPY", "2025-01-02") == rows
    reversed_rows = list(reversed(rows))
    try:
        stage0._validate_intraday_rows(reversed_rows, "SPY", "2025-01-02")
    except stage0.CloseToOpenEtfMomentumError as exc:
        assert "unordered" in str(exc)
    else:
        raise AssertionError("unordered bars must fail closed")


def test_entry_bar_stop_precedes_next_open_and_gap_is_conservative():
    day = datetime(2025, 1, 2, tzinfo=EASTERN)
    signal = _session(day)
    next_rows = _session(day + timedelta(days=1), start=102.0)
    ordinary = stage0._outcome(signal, next_rows, 1.0, 5)
    assert ordinary["exit_reason"] == "next_open"
    stopped = [dict(row) for row in signal]
    stop = float(stopped[25]["o"]) - 1.0
    stopped[25]["l"] = stop - 0.01
    stopped_outcome = stage0._outcome(stopped, next_rows, 1.0, 5)
    assert stopped_outcome["exit_reason"] == "stop"
    gapped_next = [dict(row) for row in next_rows]
    gapped_next[0]["o"] = stop - 1
    gap_outcome = stage0._outcome(signal, gapped_next, 1.0, 5)
    assert gap_outcome["exit_reason"] == "stop_gap"
    assert gap_outcome["net_r"] < stopped_outcome["net_r"]


def test_frozen_variant_is_third_in_inspected_second_wave():
    variant = stage0._variant()
    assert variant["variant_id"] == stage0.VARIANT_ID
    assert variant["mechanism_family"] == "close-to-open-etf-momentum"
    assert variant["maximum_holding_trading_days"] == 1
    assert variant["data_contract"]["required_complete_regular_session_bars"] == 26
    assert variant["maturity_effect"] == "NONE"


def test_published_activation_binds_daily_and_intraday_input_graph():
    matches = sorted(
        (ROOT / "strategy_tournament/second_wave/activations").glob(
            "close-to-open-etf-momentum-v1-*.json"
        )
    )
    assert len(matches) == 1
    activation = json.loads(matches[0].read_text(encoding="utf-8"))
    assert activation == stage0.build_activation()
    assert activation["manifest_sha256"] == stage0.common._self_hash(
        activation, "manifest_sha256"
    )
    assert activation["variant_ordinal"] == 3
    source = activation["source_selection"]
    assert source["whole_provider"] == "ibkr"
    assert source["provider_substitution"] is False
    assert source["common_calendar_dates"] == 1008
    assert source["evaluation_dates"] == 744
    assert source["daily_input_symbol_dates"] == 4032
    assert source["intraday_input_symbol_dates"] == 4032
    assert activation["return_evaluation_authorized_before_inspection"] is False
    assert activation["maturity_effect"] == "NONE"


def test_published_input_inspection_authorizes_one_frozen_evaluation():
    matches = sorted(
        (ROOT / "strategy_tournament/second_wave/inspections").glob(
            "close-to-open-etf-momentum-v1-input-*.json"
        )
    )
    assert len(matches) == 1
    inspection = json.loads(matches[0].read_text(encoding="utf-8"))
    assert inspection["inspection_sha256"] == stage0.common._self_hash(
        inspection, "inspection_sha256"
    )
    assert inspection["manifest_sha256"] == (
        "1e7934f023b26213e085277cb18bb343ddc6f4655aa9cdc8a9bc0096cc95a9c6"
    )
    assert inspection["daily_input_symbol_dates"] == 4032
    assert inspection["intraday_input_symbol_dates"] == 4032
    assert inspection["evaluation_dates"] == 744
    assert inspection["provider_requests"] == 0
    assert inspection["returns_computed"] == 0
    assert inspection["maturity_effect"] == "NONE"
    assert inspection["return_evaluation_authorized"] is True
    assert inspection["valid"] is True

from __future__ import annotations

from datetime import datetime, timedelta

import pytest

import dense_strategy_runtime as runtime
import sec_earnings_event_15m as family


DAY = "2024-02-01"


def _bars(*, both_touched: bool = False) -> list[dict]:
    start = datetime.fromisoformat(f"{DAY}T09:30:00-05:00")
    rows = []
    for index in range(26):
        opening = 101.0
        high = 101.5
        low = 100.5
        close = 101.0
        volume = 1_000_000.0
        if index == 0:
            opening, high, low, close, volume = (
                100.0,
                102.0,
                99.0,
                101.75,
                3_000_000.0,
            )
        elif index == 2 and both_touched:
            high, low = 105.0, 98.0
        rows.append(
            {
                "timestamp": (start + timedelta(minutes=15 * index)).isoformat(),
                "open": opening,
                "high": high,
                "low": low,
                "close": close,
                "volume": volume,
            }
        )
    return rows


def _event(symbol: str = "AAA", *, gap: float = 0.03) -> dict:
    return {
        "accepted_at": f"{DAY}T08:00:00-05:00",
        "event_id": symbol.lower() * 32,
        "gap_fraction": gap,
        "instrument_id": f"figi-{symbol}",
        "opening_bullish": True,
        "opening_close_location": 0.9166666667,
        "opening_volume_ratio": 3.0,
        "prior_close": 97.08737864,
        "prior_median_dollar_volume": 100_000_000.0,
        "symbol": symbol,
    }


def _dataset(*, both_touched: bool = False) -> dict:
    return {
        "schema_version": 1,
        "family_id": runtime.SEC_EARNINGS_GAP_15M_FAMILY,
        "evaluation_dates": [DAY],
        "signal_dates": [DAY],
        "event_metadata_by_date": {DAY: [_event()]},
        "fifteen_minute_bars": {DAY: {"AAA": _bars(both_touched=both_touched)}},
        "blocked_dates": [],
    }


def _parameters() -> dict:
    return {
        "maximum_structural_stop_fraction": 0.03,
        "minimum_gap_fraction": 0.02,
        "minimum_opening_close_location": 0.5,
        "minimum_opening_volume_ratio": 1.5,
        "target_r": 1.5,
    }


def test_family_grid_is_exactly_32_trials():
    count = 1
    for values in family.PARAMETER_GRID.values():
        count *= len(values)

    assert count == 32


def test_sec_earnings_entry_uses_next_bar_and_stop_first_ambiguity():
    dataset = runtime.prepare_dataset(_dataset(both_touched=True))

    candidates = runtime.build_candidates(
        dataset,
        runtime.SEC_EARNINGS_GAP_15M_FAMILY,
        _parameters(),
    )

    assert len(candidates) == 1
    candidate = candidates[0]
    assert candidate["outcome"] == "eligible"
    assert candidate["entry_price"] == 101.0
    assert candidate["stop_price"] == 99.0
    assert candidate["target_price"] == 104.0
    assert candidate["exit_price"] == 99.0
    assert candidate["stop_executed"] is True


def test_sec_earnings_ranking_is_deterministic():
    dataset = _dataset()
    dataset["event_metadata_by_date"][DAY].append(_event("BBB", gap=0.04))
    dataset["fifteen_minute_bars"][DAY]["BBB"] = _bars()
    prepared = runtime.prepare_dataset(dataset)

    candidates = runtime.build_candidates(
        prepared,
        runtime.SEC_EARNINGS_GAP_15M_FAMILY,
        _parameters(),
    )

    assert len(candidates) == 1
    assert candidates[0]["symbol"] == "BBB"


def test_sec_earnings_blocked_date_never_substitutes_available_symbol():
    dataset = _dataset()
    dataset["blocked_dates"] = [DAY]
    prepared = runtime.prepare_dataset(dataset)

    candidates = runtime.build_candidates(
        prepared,
        runtime.SEC_EARNINGS_GAP_15M_FAMILY,
        _parameters(),
    )

    assert candidates == [
        {
            "signal_id": (
                f"{DAY}-{runtime.SEC_EARNINGS_GAP_15M_FAMILY}-DATA"
            ),
            "signal_date": DAY,
            "decision_date": DAY,
            "symbol": "DATA",
            "outcome": "missed_fill",
            "rank": 1,
            "rejection_reason": "incomplete_frozen_event_denominator",
        }
    ]


def test_sec_earnings_invalid_bullish_flag_fails_closed():
    dataset = _dataset()
    dataset["event_metadata_by_date"][DAY][0]["opening_bullish"] = "true"

    with pytest.raises(
        runtime.DenseStrategyRuntimeError,
        match="bullish flag",
    ):
        runtime.prepare_dataset(dataset)

from __future__ import annotations

import tempfile
from datetime import date, timedelta
from pathlib import Path

import dense_strategy_runtime as runtime
import liquid_equity_momentum_discovery as discovery
import liquid_equity_momentum_plugin as plugin


def _days(count: int) -> list[str]:
    start = date(2024, 1, 2)
    return [(start + timedelta(days=index)).isoformat() for index in range(count)]


def _bars(days: list[str], daily_gain: float, volume: int) -> list[dict]:
    return [
        {
            "date": day,
            "open": 100.0 + daily_gain * index - 0.05,
            "high": 100.0 + daily_gain * index + 0.25,
            "low": 100.0 + daily_gain * index - 0.25,
            "close": 100.0 + daily_gain * index,
            "volume": volume,
        }
        for index, day in enumerate(days)
    ]


def _parameters() -> dict:
    return {
        "return_lookback_sessions": 20,
        "trend_sma_sessions": 50,
        "minimum_excess_return_fraction": 0.01,
        "stop_atr14": 1.5,
        "maximum_hold_sessions": 3,
    }


def _dataset(symbol_count: int = 260) -> dict:
    days = _days(125)
    decision = days[115]
    symbols = [f"S{index:03d}" for index in range(symbol_count)]
    return {
        "family_id": runtime.LIQUID_EQUITY_MOMENTUM_FAMILY,
        "evaluation_dates": days[110:125],
        "session_dates": days,
        "universe_by_date": {decision: symbols},
        "universe_identity_by_date": {
            decision: {symbol: f"ID-{symbol}" for symbol in symbols}
        },
        "split_execution_dates_by_symbol": {},
        "daily_bars": {
            symbol: _bars(
                days,
                0.02 + index / 1_000,
                2_000_000 + (symbol_count - index) * 1_000,
            )
            for index, symbol in enumerate(symbols)
        },
    }


def test_historical_runtime_uses_point_in_time_top_250_and_next_open():
    dataset = runtime.prepare_dataset(_dataset())
    candidates = runtime.build_candidates(
        dataset,
        runtime.LIQUID_EQUITY_MOMENTUM_FAMILY,
        _parameters(),
    )
    first = candidates[0]

    assert first["decision_date"] == _days(125)[115]
    assert first["signal_date"] == _days(125)[116]
    assert first["rank"] == 1
    assert first["symbol"] == "S259"
    assert first["entry_price"] == dataset["daily_bars"]["S259"][116]["open"]


def test_split_affected_symbol_is_excluded_before_liquidity_ranking():
    raw = _dataset()
    raw["split_execution_dates_by_symbol"] = {"S259": [_days(125)[114]]}
    candidates = runtime.build_candidates(
        runtime.prepare_dataset(raw),
        runtime.LIQUID_EQUITY_MOMENTUM_FAMILY,
        _parameters(),
    )

    assert candidates[0]["symbol"] == "S258"


def test_empty_source_series_remains_membership_only():
    rows = {
        "EMPTY": [],
        "READY": [{"date": "2025-01-02", "close": 10.0}],
    }

    filtered = plugin._nonempty_daily_bars(rows)

    assert set(rows) == {"EMPTY", "READY"}
    assert set(filtered) == {"READY"}


def test_production_rebuilds_the_same_dynamic_universe_and_ranking():
    dataset = _dataset(symbol_count=500)
    decision = _days(125)[115]
    daily = {
        symbol: [bar for bar in bars if bar["date"] <= decision]
        for symbol, bars in dataset["daily_bars"].items()
    }
    references = {
        symbol: {
            "active": True,
            "type": "CS",
            "listing_identity": f"ID-{symbol}",
        }
        for symbol in daily
    }

    signal = runtime.evaluate_production_signal(
        {
            "family_id": runtime.LIQUID_EQUITY_MOMENTUM_FAMILY,
            "decision_date": decision,
            "next_session_date": _days(125)[116],
            "calendar_dates": _days(125)[:116],
            "daily_history_complete": True,
            "daily_bars": daily,
            "reference_snapshot": references,
            "reference_snapshot_complete": True,
            "reference_source_total": 500,
            "corporate_actions_complete": True,
            "recent_split_symbols": [],
        },
        family_id=runtime.LIQUID_EQUITY_MOMENTUM_FAMILY,
        parameters=_parameters(),
        frozen_universe={
            "point_in_time": True,
            "security_type": "CS",
            "minimum_prior_close": 10.0,
            "minimum_median_20_session_dollar_volume": 50_000_000.0,
            "liquidity_ranking_sessions": 60,
            "maximum_names": 250,
            "split_affected_windows": "excluded",
        },
    )

    assert signal["symbol"] == "S499"
    assert signal["rank"] == 1
    assert signal["holding_trading_days"] == 3
    assert signal["expected_gross_move_fraction"] >= 0.01


def test_real_source_partitions_have_no_calendar_wait_and_twenty_confirmations():
    (
        development,
        development_signals,
        embargo,
        confirmation,
        confirmation_signals,
    ) = discovery._partitions(enforce_commit=False)

    assert len(development_signals) == 80
    assert len(embargo) == 5
    assert len(confirmation_signals) >= 20
    assert max(development) < min(embargo) < min(confirmation)
    assert set(development_signals).issubset(development)
    assert set(confirmation_signals).issubset(confirmation)


def test_real_contract_freezes_without_opening_the_price_file():
    with tempfile.TemporaryDirectory(dir=discovery.PROJECT_ROOT) as directory:
        path, contract, capacity = discovery.freeze_successor_contract(
            created_at="2026-07-23T21:24:00Z",
            root=Path(directory),
            enforce_commit=False,
        )

        assert path.is_file()
        assert capacity.is_file()
        assert len(contract["trial_family"]) == 32
        assert contract["confirmation_signal_capacity"] == 25
        assert len(contract["development_signal_dates"]) == 80
        assert contract["plugin"]["module"] == "liquid_equity_momentum_plugin"

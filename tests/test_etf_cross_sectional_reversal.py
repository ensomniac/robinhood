from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import dense_strategy_runtime as runtime
import etf_cross_sectional_reversal_discovery as discovery
import etf_cross_sectional_reversal_plugin as plugin
from learning_data import freeze_dataset_contract


def _days(count: int) -> list[str]:
    start = date(2024, 1, 2)
    return [(start + timedelta(days=index)).isoformat() for index in range(count)]


def _bars(days: list[str], daily_gain: float, *, reversal_laggard: bool) -> list[dict]:
    rows = []
    for index, day in enumerate(days):
        close = 100.0 + daily_gain * index
        if reversal_laggard and index in {219, 220}:
            close -= 1.5 * (index - 218)
        rows.append(
            {
                "date": day,
                "open": close - 0.05,
                "high": close + 0.25,
                "low": close - 0.25,
                "close": close,
                "volume": 2_000_000,
            }
        )
    return rows


def _dataset() -> dict:
    days = _days(230)
    return {
        "family_id": runtime.ETF_CROSS_SECTIONAL_REVERSAL_FAMILY,
        "evaluation_dates": days[220:229],
        "symbols": ["DIA", "IWM", "QQQ", "SPY"],
        "daily_bars": {
            "DIA": _bars(days, 0.15, reversal_laggard=False),
            "IWM": _bars(days, 0.20, reversal_laggard=True),
            "QQQ": _bars(days, 0.40, reversal_laggard=False),
            "SPY": _bars(days, 0.20, reversal_laggard=False),
        },
    }


def _parameters() -> dict:
    return {
        "return_lookback_sessions": 2,
        "market_trend_sma": 100,
        "minimum_lag_fraction": 0.005,
        "stop_atr14": 1.0,
        "maximum_hold_sessions": 2,
    }


def test_runtime_buys_cost_clearing_laggard_at_next_open():
    dataset = _dataset()
    candidates = runtime.build_candidates(
        dataset,
        runtime.ETF_CROSS_SECTIONAL_REVERSAL_FAMILY,
        _parameters(),
    )
    first = next(
        row
        for row in candidates
        if row["decision_date"] == dataset["evaluation_dates"][0]
    )

    assert first["symbol"] == "IWM"
    assert first["rank"] == 1
    assert first["signal_date"] == dataset["evaluation_dates"][1]
    assert first["entry_price"] == dataset["daily_bars"]["IWM"][221]["open"]
    assert first["expected_gross_move_fraction"] >= 0.005
    assert first["exit_date"] <= dataset["evaluation_dates"][2]


def test_production_uses_identical_reversal_ranking():
    dataset = _dataset()
    decision_date = dataset["evaluation_dates"][0]
    daily_bars = {
        symbol: [bar for bar in bars if bar["date"] <= decision_date]
        for symbol, bars in dataset["daily_bars"].items()
    }
    calendar_dates = [bar["date"] for bar in daily_bars["SPY"]]

    signal = runtime.evaluate_production_signal(
        {
            "family_id": runtime.ETF_CROSS_SECTIONAL_REVERSAL_FAMILY,
            "decision_date": decision_date,
            "next_session_date": dataset["evaluation_dates"][1],
            "calendar_dates": calendar_dates,
            "daily_history_complete": True,
            "daily_bars": daily_bars,
            "symbols": ["DIA", "IWM", "QQQ", "SPY"],
        },
        family_id=runtime.ETF_CROSS_SECTIONAL_REVERSAL_FAMILY,
        parameters=_parameters(),
        frozen_universe={"symbols": ["DIA", "IWM", "QQQ", "SPY"]},
    )

    assert signal["symbol"] == "IWM"
    assert signal["rank"] == 1
    assert signal["holding_trading_days"] == 2
    assert signal["expected_gross_move_fraction"] >= 0.005


def test_preflight_is_outcome_blind(tmp_path, monkeypatch):
    dates = _days(50)
    manifest, _ = freeze_dataset_contract(
        {
            "schema_version": 1,
            "dataset_id": "dataset-etf-cross-sectional-reversal-preflight",
            "registered_at": "2026-07-23T19:00:00Z",
            "requested_dates": dates,
            "dataset_payload": {
                "lane": "development",
                "claim_scope": "DEVELOPMENT_ONLY",
                "evidence_paths": ["tests/test_etf_cross_sectional_reversal.py"],
                "inspected": True,
                "point_in_time_evidence": True,
                "development_training_contaminated": True,
                "confirmation_access_permitted": False,
                "etf_cross_sectional_reversal_source": {
                    "source_family_id": runtime.ETF_PULLBACK_FAMILY,
                    "target_family_id": runtime.ETF_CROSS_SECTIONAL_REVERSAL_FAMILY,
                    "external_relative_path": "dense/example.json.gz",
                    "external_file_sha256": "a" * 64,
                    "dataset_sha256": "b" * 64,
                    "format": "json.gz",
                    "formal_capacity": 200,
                },
            },
        },
        tmp_path,
    )
    checked: list[Path] = []
    monkeypatch.setattr(
        plugin.shared, "_require_committed", lambda path: checked.append(path)
    )

    result = plugin.preflight(
        {
            "capacity_manifest": str(manifest),
            "family_id": runtime.ETF_CROSS_SECTIONAL_REVERSAL_FAMILY,
            "development_dates": dates,
        }
    )

    assert checked == [manifest]
    assert result["verified_capacity"] == 200
    assert result["point_in_time_complete"] is True
    assert result["external_dataset_opened"] is False
    assert result["provider_telemetry"]["dataset_loads"] == 0


def test_status_never_waits_for_new_family_reset(tmp_path):
    result = discovery.status(root=tmp_path)

    assert result["calendar_wait_required"] is False
    assert result["new_mechanism_family_slot_consumed"] is False
    assert result["confirmation_outcomes_accessed"] is False

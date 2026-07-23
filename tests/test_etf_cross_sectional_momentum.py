from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import dense_strategy_runtime as runtime
import etf_cross_sectional_momentum_discovery as discovery
import etf_cross_sectional_momentum_plugin as plugin
from learning_data import freeze_dataset_contract


def _days(count: int) -> list[str]:
    start = date(2024, 1, 2)
    return [(start + timedelta(days=index)).isoformat() for index in range(count)]


def _bars(days: list[str], daily_gain: float) -> list[dict]:
    return [
        {
            "date": day,
            "open": 100.0 + daily_gain * index - 0.05,
            "high": 100.0 + daily_gain * index + 0.25,
            "low": 100.0 + daily_gain * index - 0.25,
            "close": 100.0 + daily_gain * index,
            "volume": 2_000_000,
        }
        for index, day in enumerate(days)
    ]


def _dataset() -> dict:
    days = _days(230)
    return {
        "family_id": runtime.ETF_CROSS_SECTIONAL_MOMENTUM_FAMILY,
        "evaluation_dates": days[220:229],
        "symbols": ["DIA", "IWM", "QQQ", "SPY"],
        "daily_bars": {
            "DIA": _bars(days, 0.15),
            "IWM": _bars(days, 0.10),
            "QQQ": _bars(days, 0.40),
            "SPY": _bars(days, 0.20),
        },
    }


def _parameters() -> dict:
    return {
        "return_lookback_sessions": 20,
        "market_trend_sma": 100,
        "minimum_excess_return_fraction": 0.01,
        "stop_atr14": 1.5,
        "maximum_hold_sessions": 3,
    }


def test_runtime_ranks_complete_etf_universe_and_enters_next_open():
    dataset = _dataset()
    candidates = runtime.build_candidates(
        dataset,
        runtime.ETF_CROSS_SECTIONAL_MOMENTUM_FAMILY,
        _parameters(),
    )
    first = next(
        row
        for row in candidates
        if row["decision_date"] == dataset["evaluation_dates"][0]
    )

    assert first["symbol"] == "QQQ"
    assert first["rank"] == 1
    assert first["signal_date"] == dataset["evaluation_dates"][1]
    assert first["entry_price"] == dataset["daily_bars"]["QQQ"][221]["open"]
    assert first["exit_date"] <= dataset["evaluation_dates"][3]


def test_production_uses_the_same_cross_sectional_ranking():
    dataset = _dataset()
    decision_date = dataset["evaluation_dates"][0]
    daily_bars = {
        symbol: [bar for bar in bars if bar["date"] <= decision_date]
        for symbol, bars in dataset["daily_bars"].items()
    }
    calendar_dates = [bar["date"] for bar in daily_bars["SPY"]]

    signal = runtime.evaluate_production_signal(
        {
            "family_id": runtime.ETF_CROSS_SECTIONAL_MOMENTUM_FAMILY,
            "decision_date": decision_date,
            "next_session_date": dataset["evaluation_dates"][1],
            "calendar_dates": calendar_dates,
            "daily_history_complete": True,
            "daily_bars": daily_bars,
            "symbols": ["DIA", "IWM", "QQQ", "SPY"],
        },
        family_id=runtime.ETF_CROSS_SECTIONAL_MOMENTUM_FAMILY,
        parameters=_parameters(),
        frozen_universe={"symbols": ["DIA", "IWM", "QQQ", "SPY"]},
    )

    assert signal["symbol"] == "QQQ"
    assert signal["rank"] == 1
    assert signal["holding_trading_days"] == 3
    assert signal["expected_gross_move_fraction"] >= 0.01


def test_preflight_opens_only_committed_metadata(tmp_path, monkeypatch):
    dates = _days(50)
    manifest, _ = freeze_dataset_contract(
        {
            "schema_version": 1,
            "dataset_id": "dataset-etf-cross-sectional-preflight",
            "registered_at": "2026-07-23T17:40:00Z",
            "requested_dates": dates,
            "dataset_payload": {
                "lane": "development",
                "claim_scope": "DEVELOPMENT_ONLY",
                "evidence_paths": ["tests/test_etf_cross_sectional_momentum.py"],
                "inspected": True,
                "point_in_time_evidence": True,
                "development_training_contaminated": True,
                "confirmation_access_permitted": False,
                "etf_cross_sectional_momentum_source": {
                    "source_family_id": runtime.ETF_PULLBACK_FAMILY,
                    "target_family_id": (
                        runtime.ETF_CROSS_SECTIONAL_MOMENTUM_FAMILY
                    ),
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
        plugin, "_require_committed", lambda path: checked.append(path)
    )

    result = plugin.preflight(
        {
            "capacity_manifest": str(manifest),
            "family_id": runtime.ETF_CROSS_SECTIONAL_MOMENTUM_FAMILY,
            "development_dates": dates,
        }
    )

    assert checked == [manifest]
    assert result["verified_capacity"] == 200
    assert result["point_in_time_complete"] is True
    assert result["external_dataset_opened"] is False
    assert result["provider_telemetry"]["dataset_loads"] == 0


def test_development_reports_contract_canonical_manifest_path(monkeypatch):
    canonical = "strategy_tournament/v2/continuous/example/dataset.json"
    monkeypatch.setattr(plugin, "_load_dataset", lambda *args, **kwargs: {})
    monkeypatch.setattr(
        plugin.runtime,
        "evaluate_trial",
        lambda *args, **kwargs: {"trial_id": kwargs["trial_id"]},
    )
    monkeypatch.setattr(plugin, "_account_policy", lambda: {})

    result = plugin.evaluate_development(
        {
            "dataset_manifest": canonical,
            "development_dates": ["2020-01-02"],
            "rolling_origin_plan": [],
        },
        [{"trial_id": "trial-a", "parameters": {}}],
    )

    assert result["dataset_manifest"] == canonical


def test_status_never_waits_for_new_family_reset(tmp_path):
    result = discovery.status(root=tmp_path)

    assert result["calendar_wait_required"] is False
    assert result["new_mechanism_family_slot_consumed"] is False
    assert result["confirmation_outcomes_accessed"] is False

from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import dense_strategy_runtime as runtime
import etf_turn_of_month_discovery as discovery
import etf_turn_of_month_plugin as plugin
from learning_data import freeze_dataset_contract


def _weekdays(count: int) -> list[str]:
    result: list[str] = []
    current = date(2023, 1, 2)
    while len(result) < count:
        if current.weekday() < 5:
            result.append(current.isoformat())
        current += timedelta(days=1)
    return result


def _bars(days: list[str]) -> list[dict]:
    rows: list[dict] = []
    for index, day in enumerate(days):
        close = 100.0 + 0.1 * index
        rows.append(
            {
                "date": day,
                "open": close - 0.05,
                "high": close + 1.0,
                "low": close - 1.0,
                "close": close,
                "volume": 2_000_000,
            }
        )
    return rows


def _dataset() -> dict:
    days = _weekdays(320)
    return {
        "family_id": runtime.ETF_TURN_OF_MONTH_FAMILY,
        "evaluation_dates": days[220:315],
        "symbols": ["SPY"],
        "daily_bars": {"SPY": _bars(days)},
    }


def _parameters() -> dict:
    return {
        "sessions_before_month_end": 1,
        "sessions_after_month_start": 1,
        "market_trend_sma": 100,
        "stop_atr14": 1.0,
        "maximum_hold_sessions": 2,
    }


def test_runtime_enters_only_exact_month_boundary_at_next_open():
    dataset = runtime.prepare_dataset(_dataset())

    candidates = runtime.build_candidates(
        dataset,
        runtime.ETF_TURN_OF_MONTH_FAMILY,
        _parameters(),
    )

    assert candidates
    for candidate in candidates:
        decision = candidate["decision_date"]
        all_days = [
            bar["date"] for bar in dataset["daily_bars"]["SPY"]
        ]
        assert runtime._turn_of_month_membership(
            decision,
            all_days,
            before_sessions=1,
            after_sessions=1,
        )
        assert candidate["symbol"] == "SPY"
        assert candidate["rank"] == 1
        assert candidate["signal_date"] > decision
        assert candidate["expected_gross_move_fraction"] >= 0.005
        assert candidate["exit_date"] >= candidate["signal_date"]


def test_production_rebuilds_same_exchange_calendar_signal():
    dataset = _dataset()
    prepared = runtime.prepare_dataset(dataset)
    historical = runtime.build_candidates(
        prepared,
        runtime.ETF_TURN_OF_MONTH_FAMILY,
        _parameters(),
    )[0]
    decision_date = historical["decision_date"]
    all_days = [
        bar["date"] for bar in dataset["daily_bars"]["SPY"]
    ]
    decision_index = all_days.index(decision_date)
    history = dataset["daily_bars"]["SPY"][: decision_index + 1]

    signal = runtime.evaluate_production_signal(
        {
            "family_id": runtime.ETF_TURN_OF_MONTH_FAMILY,
            "decision_date": decision_date,
            "next_session_date": historical["signal_date"],
            "calendar_dates": [bar["date"] for bar in history],
            "exchange_calendar_dates": all_days,
            "daily_history_complete": True,
            "symbols": ["SPY"],
            "daily_bars": {"SPY": history},
        },
        family_id=runtime.ETF_TURN_OF_MONTH_FAMILY,
        parameters=_parameters(),
        frozen_universe={"symbols": ["SPY"]},
    )

    assert signal["symbol"] == "SPY"
    assert signal["rank"] == 1
    assert signal["next_session_date"] == historical["signal_date"]
    assert signal["holding_trading_days"] == 2
    assert signal["expected_gross_move_fraction"] >= 0.005


def test_preflight_is_outcome_blind(tmp_path, monkeypatch):
    dates = _weekdays(120)
    manifest, _ = freeze_dataset_contract(
        {
            "schema_version": 1,
            "dataset_id": "dataset-etf-turn-of-month-preflight",
            "registered_at": "2026-07-23T20:00:00Z",
            "requested_dates": dates,
            "dataset_payload": {
                "lane": "development",
                "claim_scope": "DEVELOPMENT_ONLY",
                "evidence_paths": ["tests/test_etf_turn_of_month.py"],
                "inspected": True,
                "point_in_time_evidence": True,
                "development_training_contaminated": True,
                "confirmation_access_permitted": False,
                "etf_turn_of_month_source": {
                    "source_family_id": runtime.ETF_PULLBACK_FAMILY,
                    "target_family_id": runtime.ETF_TURN_OF_MONTH_FAMILY,
                    "external_relative_path": "dense/example.json.gz",
                    "external_file_sha256": "a" * 64,
                    "dataset_sha256": "b" * 64,
                    "format": "json.gz",
                    "formal_capacity": 1_000,
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
            "family_id": runtime.ETF_TURN_OF_MONTH_FAMILY,
            "development_dates": dates,
        }
    )

    assert checked == [manifest]
    assert result["verified_capacity"] == 1_000
    assert result["point_in_time_complete"] is True
    assert result["external_dataset_opened"] is False
    assert result["provider_telemetry"]["dataset_loads"] == 0


def test_status_never_waits_for_new_family_reset(tmp_path):
    result = discovery.status(root=tmp_path)

    assert result["calendar_wait_required"] is False
    assert result["new_mechanism_family_slot_consumed"] is False
    assert result["confirmation_outcomes_accessed"] is False

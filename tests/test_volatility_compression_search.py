from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path

import dense_strategy_runtime as runtime
import volatility_compression_discovery as discovery
import volatility_compression_plugin as plugin
from learning_data import freeze_dataset_contract


DAY = "2025-01-02"
EASTERN = timezone(timedelta(hours=-5))


def _bar(
    minute: int,
    *,
    opening: float = 100.0,
    close: float = 100.0,
    high: float | None = None,
    low: float | None = None,
    volume: float = 1_000.0,
) -> dict:
    observed = datetime.combine(
        date.fromisoformat(DAY),
        time(9, 30),
        tzinfo=EASTERN,
    ) + timedelta(minutes=minute)
    high_value = high if high is not None else max(opening, close) + 0.05
    low_value = low if low is not None else min(opening, close) - 0.05
    typical = (high_value + low_value + close) / 3.0
    return {
        "timestamp": observed.isoformat(),
        "open": opening,
        "high": high_value,
        "low": low_value,
        "close": close,
        "volume": volume,
        "vwap_numerator": typical * volume,
        "vwap_denominator": volume,
    }


def _session(*, compression_half_width: float) -> list[dict]:
    bars = [_bar(minute) for minute in range(390)]
    bars[0] = _bar(0, high=101.0, low=99.0)
    for minute in range(35, 45):
        bars[minute] = _bar(
            minute,
            high=100.0 + compression_half_width,
            low=100.0 - compression_half_width,
        )
    bars[45] = _bar(
        45,
        opening=100.0,
        close=100.3,
        high=100.35,
        low=99.95,
        volume=3_000.0,
    )
    bars[46] = _bar(
        46,
        opening=100.4,
        close=100.4,
        high=102.0,
        low=99.0,
    )
    return bars


def _dataset() -> dict:
    return {
        "family_id": runtime.VOLATILITY_COMPRESSION_FAMILY,
        "evaluation_dates": [DAY],
        "candidate_symbols_by_date": {DAY: ["AAA", "BBB"]},
        "regular_session_minutes_by_date": {DAY: 390},
        "minute_bars": {
            DAY: {
                "AAA": _session(compression_half_width=0.15),
                "BBB": _session(compression_half_width=0.10),
            }
        },
    }


def _parameters() -> dict:
    return {
        "compression_bars": 10,
        "maximum_compression_ratio": 0.4,
        "breakout_volume_multiple": 2.5,
        "signal_cutoff_minutes": 120,
        "target_r": 2.0,
    }


def test_runtime_ranks_tighter_compression_and_resolves_stop_first():
    prepared = runtime.prepare_dataset(_dataset())

    candidates = runtime.build_candidates(
        prepared,
        runtime.VOLATILITY_COMPRESSION_FAMILY,
        _parameters(),
    )

    assert len(candidates) == 1
    selected = candidates[0]
    assert selected["symbol"] == "BBB"
    assert selected["trigger_index"] == 45
    assert selected["entry_price"] == 100.4
    assert selected["compression_ratio"] < 0.11
    assert selected["stop_executed"] is True
    assert selected["exit_price"] == selected["stop_price"]


def test_production_rebuilds_same_current_ranked_trigger():
    dataset = _dataset()
    bars = {
        symbol: rows[:46]
        for symbol, rows in dataset["minute_bars"][DAY].items()
    }
    quote_time = datetime.fromisoformat(
        bars["BBB"][-1]["timestamp"]
    ) + timedelta(minutes=1, seconds=5)
    winner = {
        "strategy_id": "volatility-compression-breakout",
        "strategy_version": "compression-test-v1",
        "rules_hash": "a" * 64,
        "exact_rules": {
            "selected_trial_id": "trial-compression",
            "parameters": _parameters(),
        },
    }

    result = plugin.evaluate_production(
        winner,
        {
            "selected_trial_id": "trial-compression",
            "parameters": _parameters(),
            "session_date": DAY,
            "candidate_symbols": ["AAA", "BBB"],
            "selection_complete": True,
            "bars_by_symbol": bars,
            "quote": {
                "symbol": "BBB",
                "observed_at": quote_time.isoformat(),
                "halted": False,
                "tradable": True,
                "bid": 100.38,
                "ask": 100.4,
                "executable_ask_depth": 10_000,
                "recent_real_minute_volume": 3_000,
            },
            "operational": {
                "account_reconciled": True,
                "orders_reconciled": True,
                "protection_reconciled": True,
                "tradability_reconciled": True,
                "news_reconciled": True,
                "protective_order_route_ready": True,
                "monitoring_ready": True,
                "safe_cutoff": "15:45 ET",
            },
        },
    )

    assert result["symbol"] == "BBB"
    assert result["rank"] == 1
    assert result["entry_limit"] == 100.4
    assert result["stop_price"] < result["entry_limit"]
    assert result["expected_gross_move_fraction"] >= 0.005
    assert result["holding_trading_days"] == 1


def test_preflight_is_outcome_blind(tmp_path, monkeypatch):
    dates = [
        (date(2025, 1, 2) + timedelta(days=index)).isoformat()
        for index in range(120)
    ]
    evidence = Path("tests/test_volatility_compression_search.py")
    manifest, _ = freeze_dataset_contract(
        {
            "schema_version": 1,
            "dataset_id": "dataset-compression-preflight",
            "registered_at": "2026-07-23T20:00:00Z",
            "requested_dates": dates,
            "dataset_payload": {
                "lane": "development",
                "claim_scope": "DEVELOPMENT_ONLY",
                "evidence_paths": [str(evidence)],
                "inspected": True,
                "point_in_time_evidence": True,
                "volatility_compression_capacity": {
                    "family_id": runtime.VOLATILITY_COMPRESSION_FAMILY,
                    "formal_capacity": 4_833,
                    "development_training_contaminated": True,
                    "confirmation_access_permitted": False,
                },
            },
        },
        tmp_path / "manifests",
    )
    checked: list[Path] = []
    monkeypatch.setattr(
        plugin, "_require_committed", lambda path: checked.append(path)
    )

    result = plugin.preflight(
        {
            "capacity_manifest": str(manifest),
            "development_dates": dates,
        }
    )

    assert checked == [manifest, plugin.PROJECT_ROOT / evidence]
    assert result["verified_capacity"] == 4_833
    assert result["point_in_time_complete"] is True
    assert result["external_dataset_opened"] is False
    assert result["provider_telemetry"]["dataset_loads"] == 0


def test_status_never_waits_for_a_calendar_reset(tmp_path):
    result = discovery.status(root=tmp_path)

    assert result["state"] == "READY_TO_FREEZE"
    assert result["calendar_wait_required"] is False
    assert result["new_mechanism_family_slot_consumed"] is False
    assert result["confirmation_outcomes_accessed"] is False

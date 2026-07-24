from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path

import pytest

import dense_strategy_runtime as runtime
import equity_gap_continuation_discovery as discovery
import equity_gap_continuation_plugin as plugin
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


def _session(*, trigger_volume: float) -> list[dict]:
    bars = [_bar(minute) for minute in range(390)]
    bars[15] = _bar(
        15,
        opening=100.0,
        close=100.5,
        high=100.55,
        low=99.95,
        volume=trigger_volume,
    )
    bars[16] = _bar(
        16,
        opening=101.0,
        close=101.0,
        high=104.0,
        low=99.0,
    )
    return bars


def _dataset() -> dict:
    return {
        "family_id": runtime.EQUITY_GAP_CONTINUATION_FAMILY,
        "evaluation_dates": [DAY],
        "candidate_symbols_by_date": {DAY: ["AAA", "BBB"]},
        "candidate_metadata_by_date": {
            DAY: {
                "AAA": {"symbol": "AAA", "gap_fraction": 0.04},
                "BBB": {"symbol": "BBB", "gap_fraction": 0.03},
            }
        },
        "regular_session_minutes_by_date": {DAY: 390},
        "minute_bars": {
            DAY: {
                "AAA": _session(trigger_volume=2_000.0),
                "BBB": _session(trigger_volume=3_000.0),
            }
        },
    }


def _parameters() -> dict:
    return {
        "minimum_gap_fraction": 0.02,
        "opening_range_minutes": 15,
        "breakout_volume_multiple": 1.5,
        "signal_cutoff_minutes": 60,
        "target_r": 2.0,
    }


def _wide_stop_dataset() -> dict:
    dataset = _dataset()
    for symbol in dataset["minute_bars"][DAY]:
        dataset["minute_bars"][DAY][symbol][0]["low"] = 90.0
    return dataset


def test_runtime_ranks_volume_then_gap_and_resolves_stop_first():
    prepared = runtime.prepare_dataset(_dataset())

    candidates = runtime.build_candidates(
        prepared,
        runtime.EQUITY_GAP_CONTINUATION_FAMILY,
        _parameters(),
    )

    assert len(candidates) == 1
    selected = candidates[0]
    assert selected["symbol"] == "BBB"
    assert selected["trigger_index"] == 15
    assert selected["entry_price"] == 101.0
    assert selected["stop_executed"] is True
    assert selected["exit_price"] == selected["stop_price"]
    assert selected["target_price"] > selected["entry_price"]


def test_production_rebuilds_the_same_current_ranked_trigger():
    dataset = _dataset()
    bars = {
        symbol: rows[:16]
        for symbol, rows in dataset["minute_bars"][DAY].items()
    }
    quote_time = datetime.fromisoformat(
        bars["BBB"][-1]["timestamp"]
    ) + timedelta(minutes=1, seconds=5)
    winner = {
        "strategy_id": "equity-gap-continuation",
        "strategy_version": "gap-test-v1",
        "rules_hash": "a" * 64,
        "exact_rules": {
            "selected_trial_id": "trial-gap",
            "parameters": _parameters(),
        },
    }

    result = plugin.evaluate_production(
        winner,
        {
            "selected_trial_id": "trial-gap",
            "parameters": _parameters(),
            "session_date": DAY,
            "candidate_symbols": ["AAA", "BBB"],
            "candidate_metadata": dataset["candidate_metadata_by_date"][DAY],
            "selection_complete": True,
            "bars_by_symbol": bars,
            "quote": {
                "symbol": "BBB",
                "observed_at": quote_time.isoformat(),
                "halted": False,
                "tradable": True,
                "bid": 100.98,
                "ask": 101.0,
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
    assert result["entry_limit"] == 101.0
    assert result["stop_price"] < result["entry_limit"]
    assert result["expected_gross_move_fraction"] >= 0.005
    assert result["holding_trading_days"] == 1


def test_historical_protection_cap_rejects_without_substituting():
    parameters = {
        **_parameters(),
        "maximum_structural_stop_fraction": 0.03,
    }

    candidates = runtime.build_candidates(
        runtime.prepare_dataset(_wide_stop_dataset()),
        runtime.EQUITY_GAP_CONTINUATION_FAMILY,
        parameters,
    )

    assert len(candidates) == 1
    assert candidates[0]["symbol"] == "BBB"
    assert candidates[0]["outcome"] == "rejected"
    assert (
        candidates[0]["rejection_reason"]
        == "structural_stop_exceeds_protection_cap"
    )


def test_production_enforces_the_same_frozen_protection_cap():
    dataset = _wide_stop_dataset()
    parameters = {
        **_parameters(),
        "maximum_structural_stop_fraction": 0.03,
    }
    bars = {
        symbol: rows[:16]
        for symbol, rows in dataset["minute_bars"][DAY].items()
    }
    quote_time = datetime.fromisoformat(
        bars["BBB"][-1]["timestamp"]
    ) + timedelta(minutes=1, seconds=5)
    winner = {
        "strategy_id": "equity-gap-continuation",
        "strategy_version": "gap-test-v2",
        "rules_hash": "b" * 64,
        "exact_rules": {
            "selected_trial_id": "trial-gap-cap",
            "parameters": parameters,
        },
    }

    with pytest.raises(
        plugin.EquityGapContinuationPluginError,
        match="exceeds the frozen protection cap",
    ):
        plugin.evaluate_production(
            winner,
            {
                "selected_trial_id": "trial-gap-cap",
                "parameters": parameters,
                "session_date": DAY,
                "candidate_symbols": ["AAA", "BBB"],
                "candidate_metadata": dataset["candidate_metadata_by_date"][DAY],
                "selection_complete": True,
                "bars_by_symbol": bars,
                "quote": {
                    "symbol": "BBB",
                    "observed_at": quote_time.isoformat(),
                    "halted": False,
                    "tradable": True,
                    "bid": 100.98,
                    "ask": 101.0,
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


def test_preflight_uses_only_committed_capacity_metadata(
    tmp_path,
    monkeypatch,
):
    dates = [
        (date(2025, 1, 2) + timedelta(days=index)).isoformat()
        for index in range(120)
    ]
    evidence = Path("tests/test_equity_gap_continuation_search.py")
    manifest, _ = freeze_dataset_contract(
        {
            "schema_version": 1,
            "dataset_id": "dataset-gap-continuation-preflight",
            "registered_at": "2026-07-23T18:30:00Z",
            "requested_dates": dates,
            "dataset_payload": {
                "lane": "development",
                "claim_scope": "DEVELOPMENT_ONLY",
                "evidence_paths": [str(evidence)],
                "inspected": True,
                "point_in_time_evidence": True,
                "gap_continuation_capacity": {
                    "family_id": runtime.EQUITY_GAP_CONTINUATION_FAMILY,
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


def test_development_reports_contract_canonical_manifest_path(monkeypatch):
    canonical = (
        "strategy_tournament/v2/continuous/example/"
        "dataset-gap-development.json"
    )
    monkeypatch.setattr(plugin, "_load_bound_dataset", lambda *args, **kwargs: {})
    monkeypatch.setattr(plugin.runtime, "prepare_dataset", lambda value: value)
    monkeypatch.setattr(
        plugin.runtime,
        "evaluate_trial",
        lambda *args, **kwargs: {"trial_id": kwargs["trial_id"]},
    )
    monkeypatch.setattr(plugin, "_account_policy", lambda: {})

    result = plugin.evaluate_development(
        {
            "dataset_manifest": canonical,
            "development_dates": [DAY],
            "rolling_origin_plan": [],
        },
        [{"trial_id": "trial-a", "parameters": {}}],
    )

    assert result["dataset_manifest"] == canonical


def test_status_never_waits_for_a_calendar_reset(tmp_path):
    result = discovery.status(root=tmp_path)

    assert result["state"] == "READY_TO_FREEZE"
    assert result["calendar_wait_required"] is False
    assert result["new_mechanism_family_slot_consumed"] is False
    assert result["confirmation_outcomes_accessed"] is False

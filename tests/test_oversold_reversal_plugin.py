from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest

import oversold_reversal_discovery as discovery
import oversold_reversal_plugin as plugin
import portfolio_execution
import portfolio_maturity
from learning_data import freeze_dataset_contract


def _days(count: int) -> list[str]:
    start = date(2025, 1, 2)
    return [(start + timedelta(days=index)).isoformat() for index in range(count)]


def test_preflight_uses_only_committed_capacity_metadata(tmp_path, monkeypatch):
    dates = _days(120)
    evidence = Path("tests/test_oversold_reversal_plugin.py")
    manifest, _ = freeze_dataset_contract(
        {
            "schema_version": 1,
            "dataset_id": "dataset-oversold-preflight",
            "registered_at": "2026-07-23T17:10:00Z",
            "requested_dates": dates,
            "dataset_payload": {
                "lane": "development",
                "claim_scope": "DEVELOPMENT_ONLY",
                "evidence_paths": [str(evidence)],
                "inspected": True,
                "point_in_time_evidence": True,
                "oversold_capacity": {
                    "family_id": plugin.runtime.OVERSOLD_REVERSAL_FAMILY,
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


def test_development_reports_the_contract_canonical_manifest_path(monkeypatch):
    canonical = (
        "strategy_tournament/v2/continuous/example/"
        "dataset-example-development.json"
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
            "development_dates": ["2025-01-02"],
            "rolling_origin_plan": [],
        },
        [{"trial_id": "trial-a", "parameters": {}}],
    )

    assert result["dataset_manifest"] == canonical


def _live_bars() -> list[dict[str, object]]:
    start = datetime(2026, 7, 24, 13, 30, tzinfo=UTC)
    rows: list[dict[str, object]] = []
    closes = [
        *([100.0] * 16),
        99.8,
        99.6,
        99.4,
        99.2,
        99.0,
        98.8,
        98.6,
        98.4,
        98.2,
        98.0,
        97.7,
        97.4,
        97.1,
        96.8,
    ]
    for index, close in enumerate(closes):
        rows.append(
            {
                "timestamp": (
                    start + timedelta(minutes=index)
                ).isoformat(),
                "open": close + 0.1,
                "high": close + 0.2,
                "low": close - 0.2,
                "close": close,
                "volume": 0,
            }
        )
    rows.append(
        {
            "timestamp": (
                start + timedelta(minutes=len(rows))
            ).isoformat(),
            "open": 96.7,
            "high": 98.0,
            "low": 96.5,
            "close": 97.8,
            "volume": 10_000,
        }
    )
    return rows


def _winner() -> dict[str, object]:
    parameters = {
        "lookback_minutes": 15,
        "rsi_period": 3,
        "rsi_maximum": 15,
        "selloff_threshold": -0.02,
        "target_r": 1.0,
    }
    return {
        "strategy_id": "short-horizon-oversold-reversal",
        "strategy_version": "oversold-exact-v1",
        "rules_hash": "a" * 64,
        "exact_rules": {
            "selected_trial_id": "trial-a",
            "parameters": parameters,
        },
    }


def _market_facts() -> dict[str, object]:
    bars = _live_bars()
    observed_at = (
        datetime.fromisoformat(str(bars[-1]["timestamp"]))
        + timedelta(minutes=1, seconds=1)
    )
    winner = _winner()
    return {
        "selected_trial_id": "trial-a",
        "parameters": winner["exact_rules"]["parameters"],
        "session_date": "2026-07-24",
        "candidate_symbols": ["XYZ"],
        "selection_complete": True,
        "bars_by_symbol": {"XYZ": bars},
        "quote": {
            "symbol": "XYZ",
            "observed_at": observed_at.isoformat(),
            "halted": False,
            "tradable": True,
            "bid": 97.89,
            "ask": 97.90,
            "executable_ask_depth": 10_000,
            "recent_real_minute_volume": 10_000,
        },
        "operational": {
            "before_open_account_reconciled": True,
            "before_open_orders_reconciled": True,
            "before_open_protection_reconciled": True,
            "before_open_tradability_reconciled": True,
            "before_open_news_reconciled": True,
            "protective_order_route_ready": True,
            "monitoring_ready": True,
            "protection_failure_safe_cutoff": "15:45 ET",
        },
    }


def test_exact_oversold_production_path_reaches_order_sizing() -> None:
    winner = _winner()
    facts = _market_facts()
    candidate = plugin.evaluate_production(winner, facts)
    now = datetime.fromisoformat(
        str(facts["quote"]["observed_at"])
    ) + timedelta(seconds=1)
    result = portfolio_execution.evaluate_production_candidate(
        winner,
        candidate,
        {"equity": 100_000.0, "buying_power": 100_000.0},
        portfolio_maturity.load_config(),
        now=now,
    )

    assert result["status"] == "PRODUCTION_EVALUATION_READY"
    assert result["symbol"] == "XYZ"
    assert result["order"]["quantity"] > 0
    assert result["protection"]["failure_safe_cutoff"] == "15:45 ET"


def test_live_bars_reject_a_missing_completed_minute() -> None:
    facts = _market_facts()
    facts["bars_by_symbol"]["XYZ"].pop(10)
    with pytest.raises(
        plugin.OversoldReversalPluginError,
        match="minute",
    ):
        plugin.evaluate_production(_winner(), facts)

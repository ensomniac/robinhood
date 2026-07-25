from __future__ import annotations

import gzip
import hashlib
import json
from datetime import date, datetime, time, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

import dense_strategy_plugin as plugin
import dense_strategy_runtime as runtime
import portfolio_execution
import portfolio_maturity
from historical_store import canonical_sha256, sha256_file
from learning_data import freeze_dataset_contract
from learning_experiment import build_rolling_origin_plan


def _days(count: int) -> list[str]:
    start = date(2024, 1, 1)
    return [(start + timedelta(days=index)).isoformat() for index in range(count)]


def _dataset() -> dict:
    days = _days(230)
    closes = [100 + 0.2 * index for index in range(230)]
    closes[217:221] = [143.4, 142.0, 140.0, 138.0]
    bars = [
        {
            "date": day,
            "open": close - 0.1,
            "high": close + 0.5,
            "low": close - 0.6,
            "close": close,
            "volume": 2_000_000,
        }
        for day, close in zip(days, closes, strict=True)
    ]
    return {
        "schema_version": 1,
        "family_id": runtime.ETF_PULLBACK_FAMILY,
        "evaluation_dates": days[220:229],
        "symbols": ["SPY"],
        "daily_bars": {"SPY": bars},
    }


def _manifest(
    tmp_path: Path,
    dataset: dict,
    *,
    external_exists: bool = True,
    lane: str = "development",
    development_search_sha256: str | None = None,
    manifest_root: Path | None = None,
) -> Path:
    store = tmp_path / "historical-store"
    external = store / "_derived/dense/pullback.json.gz"
    if external_exists:
        external.parent.mkdir(parents=True, exist_ok=True)
        with gzip.open(external, "wt", encoding="utf-8") as destination:
            json.dump(dataset, destination, sort_keys=True, separators=(",", ":"))
        file_hash = sha256_file(external)
    else:
        file_hash = "a" * 64
    path, _ = freeze_dataset_contract(
        {
            "schema_version": 1,
            "dataset_id": (
                f"dataset-dense-pullback-{lane}-"
                f"{(development_search_sha256 or 'explicit')[:8]}"
            ),
            "registered_at": "2026-07-22T19:00:00-04:00",
            "requested_dates": dataset["evaluation_dates"],
            "dataset_payload": {
                "lane": lane,
                "claim_scope": (
                    "DEVELOPMENT_ONLY"
                    if lane == "development"
                    else "EXACT_PREREGISTERED_CONTRACT_ONLY"
                ),
                "evidence_paths": ["tests/test_dense_strategy_plugin.py"],
                "inspected": True,
                "point_in_time_evidence": True,
                "dense_runtime": {
                    "family_id": runtime.ETF_PULLBACK_FAMILY,
                    "external_relative_path": "_derived/dense/pullback.json.gz",
                    "external_file_sha256": file_hash,
                    "dataset_sha256": canonical_sha256(dataset),
                    "format": "json.gz",
                    "formal_capacity": 120,
                },
                **(
                    {
                        "preregistration_sha256": "a" * 64,
                        "preregistered_at": "2026-07-22T18:00:00-04:00",
                        "capture_after_preregistration_attested": True,
                    }
                    if lane == "confirmation"
                    else {}
                ),
                **(
                    {
                        "development_search_sha256": (
                            development_search_sha256
                        )
                    }
                    if development_search_sha256 is not None
                    else {}
                ),
            },
        },
        manifest_root or (tmp_path / "manifests"),
    )
    return path


def _contract(manifest: Path, dates: list[str]) -> dict:
    return {
        "family_id": runtime.ETF_PULLBACK_FAMILY,
        "development_dates": dates,
        "dataset_manifest": str(manifest),
    }


def test_preflight_reads_only_committed_frozen_metadata(tmp_path, monkeypatch):
    dataset = _dataset()
    manifest = _manifest(tmp_path, dataset, external_exists=False)
    checked = []
    monkeypatch.setattr(
        plugin,
        "_require_committed",
        lambda path: checked.append(path.resolve()),
    )

    result = plugin.preflight(
        _contract(manifest, dataset["evaluation_dates"])
    )

    assert checked == [manifest.resolve()]
    assert result["verified_capacity"] == 120
    assert result["point_in_time_complete"] is True
    assert result["external_dataset_opened"] is False
    assert result["provider_telemetry"]["dataset_loads"] == 0


def test_preflight_rejects_uncommitted_capacity_manifest(tmp_path, monkeypatch):
    dataset = _dataset()
    manifest = _manifest(tmp_path, dataset, external_exists=False)

    def reject(_path):
        raise plugin.DenseStrategyPluginError(
            "dense dataset manifest must be committed and unchanged"
        )

    monkeypatch.setattr(plugin, "_require_committed", reject)
    with pytest.raises(
        plugin.DenseStrategyPluginError,
        match="committed and unchanged",
    ):
        plugin.preflight(_contract(manifest, dataset["evaluation_dates"]))


def test_development_loads_dataset_once_and_runs_all_declared_trials(
    tmp_path, monkeypatch
):
    dataset = _dataset()
    manifest = _manifest(tmp_path, dataset)
    monkeypatch.setenv(
        "LOCAL_HISTORICAL_DATA_ROOT", str(tmp_path / "historical-store")
    )
    monkeypatch.setenv("LOCAL_HISTORICAL_MIN_FREE_GIB", "1")
    trials = [
        {
            "trial_id": "trial-a",
            "parameters": {
                "trend_sma": 100,
                "rsi2_maximum": 10,
                "three_session_decline_fraction": 0.02,
                "stop_atr14": 1.0,
                "maximum_hold_sessions": 3,
            },
        },
        {
            "trial_id": "trial-b",
            "parameters": {
                "trend_sma": 100,
                "rsi2_maximum": 10,
                "three_session_decline_fraction": 0.03,
                "stop_atr14": 1.5,
                "maximum_hold_sessions": 3,
            },
        },
    ]

    result = plugin.evaluate_development(
        _contract(manifest, dataset["evaluation_dates"]), trials
    )

    assert result["dataset_manifest"] == str(manifest)
    assert [item["trial_id"] for item in result["trials"]] == [
        "trial-a",
        "trial-b",
    ]
    assert result["provider_telemetry"]["requests"] == 0
    assert result["provider_telemetry"]["dataset_loads"] == 1


def test_development_preserves_explicit_relative_manifest_binding(
    tmp_path, monkeypatch
):
    dataset = _dataset()
    manifest = _manifest(tmp_path, dataset)
    relative_manifest = manifest.relative_to(tmp_path)
    monkeypatch.setattr(plugin, "PROJECT_ROOT", tmp_path)
    monkeypatch.setenv(
        "LOCAL_HISTORICAL_DATA_ROOT", str(tmp_path / "historical-store")
    )
    monkeypatch.setenv("LOCAL_HISTORICAL_MIN_FREE_GIB", "1")

    result = plugin.evaluate_development(
        {
            "family_id": runtime.ETF_PULLBACK_FAMILY,
            "development_dates": dataset["evaluation_dates"],
            "dataset_manifest": str(relative_manifest),
        },
        [
            {
                "trial_id": "trial-relative-manifest",
                "parameters": {
                    "trend_sma": 100,
                    "rsi2_maximum": 10,
                    "three_session_decline_fraction": 0.02,
                    "stop_atr14": 1.0,
                    "maximum_hold_sessions": 3,
                },
            }
        ],
    )

    assert result["dataset_manifest"] == str(relative_manifest)


def test_development_manifest_selection_uses_exact_frozen_search_binding(
    tmp_path,
    monkeypatch,
):
    dataset = _dataset()
    manifest_root = (
        tmp_path
        / runtime.ETF_PULLBACK_FAMILY
        / "development-dataset"
    )
    stale = _manifest(
        tmp_path,
        dataset,
        development_search_sha256="a" * 64,
        manifest_root=manifest_root,
    )
    current = _manifest(
        tmp_path,
        dataset,
        development_search_sha256="b" * 64,
        manifest_root=manifest_root,
    )
    monkeypatch.setattr(plugin, "CONFIRMATION_MANIFEST_ROOT", tmp_path)

    selected = plugin._development_manifest_path(
        {
            "family_id": runtime.ETF_PULLBACK_FAMILY,
            "development_dates": dataset["evaluation_dates"],
            "development_search_sha256": "b" * 64,
        }
    )

    assert selected == current
    assert selected != stale


def test_development_plugin_emits_only_frozen_rolling_origin_test_sessions(
    tmp_path, monkeypatch
):
    dataset = _dataset()
    dataset["evaluation_dates"] = [
        bar["date"] for bar in dataset["daily_bars"]["SPY"][-120:]
    ]
    manifest = _manifest(tmp_path, dataset)
    monkeypatch.setenv(
        "LOCAL_HISTORICAL_DATA_ROOT", str(tmp_path / "historical-store")
    )
    monkeypatch.setenv("LOCAL_HISTORICAL_MIN_FREE_GIB", "1")
    plan = build_rolling_origin_plan(dataset["evaluation_dates"])
    result = plugin.evaluate_development(
        {
            **_contract(manifest, dataset["evaluation_dates"]),
            "rolling_origin_plan": plan,
        },
        [
            {
                "trial_id": "trial-oof",
                "parameters": {
                    "trend_sma": 100,
                    "rsi2_maximum": 10,
                    "three_session_decline_fraction": 0.02,
                    "stop_atr14": 1.0,
                    "maximum_hold_sessions": 5,
                },
            }
        ],
    )

    trial = result["trials"][0]
    expected_dates = [day for fold in plan for day in fold["test_dates"]]
    assert [row["date"] for row in trial["maturity_rows"]] == expected_dates
    assert len(trial["metrics"]["oof_daily_account_returns"]) == 72
    assert {item["signal_date"] for item in trial["candidate_accounting"]} <= {
        day for fold in plan for day in fold["entry_dates"]
    }


def test_confirmation_manifest_runs_only_the_exact_frozen_winner(
    tmp_path, monkeypatch
):
    dataset = _dataset()
    manifest = _manifest(tmp_path, dataset, lane="confirmation")
    monkeypatch.setattr(plugin, "_require_committed", lambda _path: None)
    monkeypatch.setenv(
        "LOCAL_HISTORICAL_DATA_ROOT", str(tmp_path / "historical-store")
    )
    monkeypatch.setenv("LOCAL_HISTORICAL_MIN_FREE_GIB", "1")
    winner = {
        "family_id": runtime.ETF_PULLBACK_FAMILY,
        "rules_hash": "a" * 64,
        "confirmation_dates": dataset["evaluation_dates"],
        "confirmation_dataset_manifest": str(manifest),
        "exact_rules": {
            "selected_trial_id": "pullback-trial",
            "parameters": {
                "trend_sma": 100,
                "rsi2_maximum": 10,
                "three_session_decline_fraction": 0.02,
                "stop_atr14": 1.0,
                "maximum_hold_sessions": 3,
            },
        },
    }

    result = plugin.evaluate_confirmation(winner)

    assert result["rules_hash"] == winner["rules_hash"]
    assert result["parameter_alternatives"] == 0
    assert result["observed_dates"] == dataset["evaluation_dates"]
    assert result["dataset_manifest"] == str(manifest)


def test_exact_pullback_winner_rebuilds_live_rank_stop_exit_and_sizing():
    dataset = _dataset()
    parameters = {
        "trend_sma": 100,
        "rsi2_maximum": 10,
        "three_session_decline_fraction": 0.02,
        "stop_atr14": 1.0,
        "maximum_hold_sessions": 3,
    }
    decision_date = dataset["evaluation_dates"][0]
    historical = runtime.build_candidates(
        dataset, runtime.ETF_PULLBACK_FAMILY, parameters
    )[0]
    observed = datetime.combine(
        date.fromisoformat(historical["signal_date"]),
        time(hour=9, minute=30, second=30),
        tzinfo=ZoneInfo("America/New_York"),
    )
    implementation_paths = [
        Path("dense_strategy_plugin.py"),
        Path("dense_strategy_runtime.py"),
        Path("learning_statistics.py"),
    ]
    winner = {
        "family_id": runtime.ETF_PULLBACK_FAMILY,
        "strategy_id": runtime.ETF_PULLBACK_FAMILY,
        "strategy_version": "pullback-production-v1",
        "rules_hash": "a" * 64,
        "exact_rules": {
            "selected_trial_id": "pullback-trial",
            "parameters": parameters,
            "universe": {"symbols": ["SPY"]},
        },
        "plugin": {
            "module": "dense_strategy_plugin",
            "evaluate_production": "evaluate_production",
        },
        "implementation_hashes": {
            str(path): hashlib.sha256(path.read_bytes()).hexdigest()
            for path in implementation_paths
        },
    }
    market_facts = {
        "selected_trial_id": "pullback-trial",
        "parameters": parameters,
        "decision_data": {
            "family_id": runtime.ETF_PULLBACK_FAMILY,
            "decision_date": decision_date,
            "next_session_date": historical["signal_date"],
            "calendar_dates": [
                bar["date"]
                for bar in dataset["daily_bars"]["SPY"]
                if bar["date"] <= decision_date
            ],
            "daily_history_complete": True,
            "symbols": ["SPY"],
            "daily_bars": {
                "SPY": [
                    bar
                    for bar in dataset["daily_bars"]["SPY"]
                    if bar["date"] <= decision_date
                ]
            },
        },
        "quote": {
            "symbol": "SPY",
            "observed_at": observed.isoformat(),
            "halted": False,
            "tradable": True,
            "bid": historical["entry_price"] - 0.02,
            "ask": historical["entry_price"],
            "executable_ask_depth": 20_000,
            "recent_real_minute_volume": 30_000,
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

    result = portfolio_execution.evaluate_frozen_winner(
        winner,
        market_facts,
        {"equity": 100_000, "buying_power": 100_000},
        portfolio_maturity.load_config(),
        now=observed,
    )

    assert result["status"] == "PRODUCTION_EVALUATION_READY"
    assert result["symbol"] == historical["symbol"] == "SPY"
    assert result["protection"]["stop_price"] == historical["stop_price"]
    assert result["protection"]["time_in_force"] == "gtc"
    assert result["exit"]["maximum_hold_sessions"] == 3
    assert result["broker_actions_performed"] == 0

    market_facts["quote"]["observed_at"] = (
        observed + timedelta(minutes=1)
    ).isoformat()
    with pytest.raises(
        portfolio_execution.PortfolioExecutionError,
        match="next-session opening interval",
    ):
        portfolio_execution.evaluate_frozen_winner(
            winner,
            market_facts,
            {"equity": 100_000, "buying_power": 100_000},
            portfolio_maturity.load_config(),
            now=observed + timedelta(minutes=1),
        )
    market_facts["quote"]["observed_at"] = observed.isoformat()

    market_facts["decision_data"]["symbols"] = ["QQQ"]
    with pytest.raises(
        portfolio_execution.PortfolioExecutionError,
        match="universe drifted",
    ):
        portfolio_execution.evaluate_frozen_winner(
            winner,
            market_facts,
            {"equity": 100_000, "buying_power": 100_000},
            portfolio_maturity.load_config(),
            now=observed,
        )


def test_intraday_production_quote_is_limited_to_exact_next_minute(monkeypatch):
    trigger = datetime(
        2026,
        7,
        22,
        9,
        45,
        tzinfo=ZoneInfo("America/New_York"),
    )
    signal = {
        "symbol": "SPY",
        "rank": 1,
        "score": -2.0,
        "expected_gross_move_fraction": 0.01,
        "atr": 1.0,
        "stop_atr_multiple": 1.0,
        "holding_trading_days": 1,
        "trigger_bar_timestamp": trigger.isoformat(),
        "target_r": 1.0,
        "exit_plan": {
            "type": "stop_target_or_session_close",
            "same_interval_ambiguity": "stop_first",
        },
    }
    monkeypatch.setattr(
        runtime,
        "evaluate_production_signal",
        lambda *_args, **_kwargs: signal,
    )
    parameters = {
        "opening_window_minutes": 15,
        "downside_z_threshold": -1.5,
        "vwap_reclaim_completed_bars": 1,
        "stop_intraday_atr": 1.0,
        "target_r": 1.0,
    }
    winner = {
        "family_id": runtime.INTRADAY_ETF_FAMILY,
        "strategy_id": runtime.INTRADAY_ETF_FAMILY,
        "strategy_version": "intraday-production-v1",
        "rules_hash": "b" * 64,
        "exact_rules": {
            "selected_trial_id": "intraday-trial",
            "parameters": parameters,
            "universe": {"symbols": ["SPY"]},
        },
    }
    market_facts = {
        "selected_trial_id": "intraday-trial",
        "parameters": parameters,
        "decision_data": {},
        "quote": {
            "symbol": "SPY",
            "observed_at": (trigger + timedelta(minutes=1, seconds=20)).isoformat(),
            "halted": False,
            "tradable": True,
            "bid": 99.99,
            "ask": 100.0,
            "executable_ask_depth": 20_000,
            "recent_real_minute_volume": 30_000,
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

    assert plugin.evaluate_production(winner, market_facts)["symbol"] == "SPY"

    market_facts["quote"]["observed_at"] = (
        trigger + timedelta(minutes=2)
    ).isoformat()
    with pytest.raises(
        plugin.DenseStrategyPluginError,
        match="next observable intraday interval",
    ):
        plugin.evaluate_production(winner, market_facts)

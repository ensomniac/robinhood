from __future__ import annotations

from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path

import close_to_open_etf_discovery as discovery
import close_to_open_etf_momentum_stage0 as source
import close_to_open_etf_plugin as plugin
import dense_strategy_runtime as runtime
from historical_store import sha256_file
from learning_data import freeze_dataset_contract


EASTERN = timezone(timedelta(hours=-5))


def _days(count: int) -> list[str]:
    start = date(2024, 1, 2)
    return [
        (start + timedelta(days=index)).isoformat()
        for index in range(count)
    ]


def _daily_bars(days: list[str], daily_gain: float) -> list[dict]:
    result = []
    for index, day in enumerate(days):
        close = 100.0 + daily_gain * index
        result.append(
            {
                "date": day,
                "open": close - 0.05,
                "high": close + 0.75,
                "low": close - 0.75,
                "close": close,
                "volume": 2_000_000,
            }
        )
    return result


def _session(day: str, session_return: float) -> list[dict]:
    start = datetime.combine(
        date.fromisoformat(day), time(9, 30), tzinfo=EASTERN
    )
    opening = 100.0
    result = []
    for index in range(26):
        fraction = session_return * index / 24
        close = opening * (1 + fraction)
        result.append(
            {
                "timestamp": (
                    start + timedelta(minutes=15 * index)
                ).isoformat(),
                "open": close - 0.01,
                "high": close + 0.1,
                "low": close - 0.1,
                "close": close,
                "volume": 100_000,
            }
        )
    return result


def _dataset() -> dict:
    days = _days(95)
    symbols = list(source.SYMBOLS)
    return {
        "family_id": runtime.ETF_CLOSE_TO_OPEN_FAMILY,
        "evaluation_dates": days[70:94],
        "symbols": symbols,
        "daily_bars": {
            symbol: _daily_bars(days, 0.01)
            for symbol in symbols
        },
        "fifteen_minute_bars": {
            day: {
                symbol: _session(
                    day, 0.015 if symbol == "QQQ" else 0.002
                )
                for symbol in symbols
            }
            for day in days[70:94]
        },
    }


def _parameters() -> dict:
    return {
        "decision_bar_time": "15:30",
        "minimum_session_return_fraction": 0.01,
        "prior_trend_sma": 20,
        "stop_atr14": 1.0,
        "exit_timing": "next_0945_close",
    }


def _nyse_2022_dates() -> list[str]:
    holidays = {
        "2022-01-17",
        "2022-02-21",
        "2022-04-15",
        "2022-05-30",
        "2022-06-20",
        "2022-07-04",
        "2022-09-05",
        "2022-11-24",
        "2022-12-26",
    }
    current = date(2022, 1, 3)
    end = date(2022, 12, 30)
    result = []
    while current <= end:
        rendered = current.isoformat()
        if current.weekday() < 5 and rendered not in holidays:
            result.append(rendered)
        current += timedelta(days=1)
    assert len(result) == 251
    return result


def test_runtime_ranks_strongest_etf_and_exits_next_opening_window():
    dataset = _dataset()
    candidates = runtime.build_candidates(
        runtime.prepare_dataset(dataset),
        runtime.ETF_CLOSE_TO_OPEN_FAMILY,
        _parameters(),
    )
    first = next(
        row
        for row in candidates
        if row["decision_date"] == dataset["evaluation_dates"][0]
        and row["rank"] == 1
    )

    assert first["symbol"] == "QQQ"
    assert first["signal_date"] == dataset["evaluation_dates"][0]
    assert first["entry_price"] == dataset["fifteen_minute_bars"][
        dataset["evaluation_dates"][0]
    ]["QQQ"][25]["open"]
    assert first["exit_date"] == dataset["evaluation_dates"][1]
    assert first["expected_gross_move_fraction"] >= 0.01


def test_production_rebuilds_rank_and_requires_gtc_protection():
    dataset = _dataset()
    decision_date = dataset["evaluation_dates"][0]
    decision_rows = {
        symbol: rows[:25]
        for symbol, rows in dataset["fifteen_minute_bars"][
            decision_date
        ].items()
    }
    prior_daily = {
        symbol: [
            bar for bar in bars if bar["date"] < decision_date
        ]
        for symbol, bars in dataset["daily_bars"].items()
    }
    winner = {
        "family_id": runtime.ETF_CLOSE_TO_OPEN_FAMILY,
        "strategy_id": "close-to-open-etf-momentum",
        "strategy_version": "close-to-open-test-v1",
        "rules_hash": "a" * 64,
        "exact_rules": {
            "selected_trial_id": "trial-close-to-open",
            "parameters": _parameters(),
            "universe": {
                "symbols": list(source.SYMBOLS),
                "point_in_time": True,
            },
        },
    }
    quote_time = datetime.fromisoformat(
        decision_rows["QQQ"][-1]["timestamp"]
    ) + timedelta(minutes=15, seconds=5)

    result = plugin.evaluate_production(
        winner,
        {
            "selected_trial_id": "trial-close-to-open",
            "parameters": _parameters(),
            "decision_data": {
                "family_id": runtime.ETF_CLOSE_TO_OPEN_FAMILY,
                "decision_date": decision_date,
                "next_session_date": dataset["evaluation_dates"][1],
                "daily_history_complete": True,
                "daily_bars": prior_daily,
                "symbols": list(source.SYMBOLS),
                "decision_bars_complete": True,
                "fifteen_minute_bars": decision_rows,
            },
            "quote": {
                "symbol": "QQQ",
                "observed_at": quote_time.isoformat(),
                "halted": False,
                "tradable": True,
                "bid": 101.48,
                "ask": 101.5,
                "executable_ask_depth": 100_000,
                "recent_real_minute_volume": 25_000,
            },
            "operational": {
                "before_open_account_reconciled": True,
                "before_open_orders_reconciled": True,
                "before_open_protection_reconciled": True,
                "before_open_tradability_reconciled": True,
                "before_open_news_reconciled": True,
                "protective_order_route_ready": True,
                "monitoring_ready": True,
                "protection_failure_safe_cutoff": "15:55 ET",
            },
        },
    )

    assert result["symbol"] == "QQQ"
    assert result["rank"] == 1
    assert result["holding_trading_days"] == 1
    assert result["protection_time_in_force"] == "gtc"
    assert result["stop_price"] < result["entry_limit"]
    assert result["expected_gross_move_fraction"] >= 0.01


def test_preflight_opens_only_committed_metadata(tmp_path, monkeypatch):
    dates = _days(120)
    manifest, _ = freeze_dataset_contract(
        {
            "schema_version": 1,
            "dataset_id": "dataset-close-to-open-preflight",
            "registered_at": "2026-07-23T20:30:00Z",
            "requested_dates": dates,
            "dataset_payload": {
                "lane": "development",
                "claim_scope": "DEVELOPMENT_ONLY",
                "evidence_paths": [
                    "tests/test_close_to_open_etf_search.py"
                ],
                "inspected": True,
                "point_in_time_evidence": True,
                "development_training_contaminated": False,
                "confirmation_access_permitted": False,
                "close_to_open_runtime": {
                    "source_variant_id": source.VARIANT_ID,
                    "target_family_id": runtime.ETF_CLOSE_TO_OPEN_FAMILY,
                    "external_relative_path": "dense/example.json.gz",
                    "external_file_sha256": "a" * 64,
                    "input_sha256": "b" * 64,
                    "format": "json.gz",
                    "sample_phase": "development",
                    "symbols": list(source.SYMBOLS),
                    "formal_capacity": 120,
                    "provider_requests": 0,
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
            "family_id": runtime.ETF_CLOSE_TO_OPEN_FAMILY,
            "development_dates": dates,
        }
    )

    assert checked == [manifest]
    assert result["verified_capacity"] == 120
    assert result["point_in_time_complete"] is True
    assert result["external_dataset_opened"] is False
    assert result["provider_telemetry"]["dataset_loads"] == 0


def test_freeze_builds_complete_disjoint_32_trial_contract(
    tmp_path, monkeypatch
):
    private_root = tmp_path / "historical"
    private_path = private_root / "_derived/close/frozen-inputs.json.gz"
    private_path.parent.mkdir(parents=True)
    private_path.write_bytes(b"outcome rows remain unopened")
    monkeypatch.setattr(source, "DATA_ROOT", private_root)
    monkeypatch.setattr(source, "PRIVATE_INPUT_PATH", private_path)
    monkeypatch.setattr(
        discovery,
        "_source_graph",
        lambda **_kwargs: (
            {
                "source_selection": {
                    "input_sha256": "c" * 64,
                    "private_input_file_sha256": sha256_file(
                        private_path
                    ),
                }
            },
            {"result_sha256": "d" * 64, "records": []},
        ),
    )
    monkeypatch.setattr(
        discovery, "_outcome_blind_dates", _nyse_2022_dates
    )
    monkeypatch.setattr(
        discovery.shared, "_repo_path", lambda path: Path(path).name
    )
    monkeypatch.setattr(
        discovery.outcome_exposure, "read_index", lambda: []
    )
    monkeypatch.setattr(
        discovery.outcome_exposure,
        "audit",
        lambda: {"index_sha256": "e" * 64},
    )

    _path, contract, capacity = discovery.freeze_successor_contract(
        created_at="2026-07-23T20:30:00Z",
        root=tmp_path / "continuous",
        enforce_commit=False,
    )

    assert len(contract["trial_family"]) == 32
    assert len(contract["development_warmup_dates"]) == 60
    assert len(contract["development_dates"]) == 120
    assert len(contract["embargo_dates"]) == 5
    assert len(contract["confirmation_dates"]) == 66
    assert contract["new_mechanism_family_slot_consumed"] is False
    assert capacity.is_file()


def test_status_never_waits_for_new_family_reset(tmp_path):
    result = discovery.status(root=tmp_path)

    assert result["state"] == "READY_TO_FREEZE"
    assert result["calendar_wait_required"] is False
    assert result["new_mechanism_family_slot_consumed"] is False
    assert result["confirmation_outcomes_accessed"] is False

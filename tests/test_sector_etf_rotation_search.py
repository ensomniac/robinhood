from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import dense_strategy_runtime as runtime
import sector_etf_rotation_discovery as discovery
import sector_etf_rotation_plugin as plugin
import sector_etf_rotation_stage0 as source
from historical_store import sha256_file
from learning_data import freeze_dataset_contract


def _days(count: int) -> list[str]:
    start = date(2024, 1, 2)
    return [
        (start + timedelta(days=index)).isoformat()
        for index in range(count)
    ]


def _bars(days: list[str], daily_gain: float) -> list[dict]:
    result = []
    for index, day in enumerate(days):
        close = 100.0 + daily_gain * index
        result.append(
            {
                "date": day,
                "open": close - 0.05,
                "high": close + 0.5,
                "low": close - 0.5,
                "close": close,
                "volume": 2_000_000,
            }
        )
    return result


def _dataset() -> dict:
    days = _days(95)
    symbols = list(source.SYMBOLS)
    return {
        "family_id": runtime.SECTOR_ETF_ROTATION_FAMILY,
        "evaluation_dates": days[70:94],
        "symbols": symbols,
        "daily_bars": {
            symbol: _bars(
                days,
                0.6 if symbol == "XLK" else 0.1,
            )
            for symbol in symbols
        },
    }


def _parameters() -> dict:
    return {
        "return_lookback_sessions": 20,
        "minimum_excess_return_fraction": 0.01,
        "market_trend_sma": 20,
        "stop_atr14": 1.5,
        "maximum_hold_sessions": 3,
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


def test_runtime_ranks_strongest_sector_and_enters_next_open():
    dataset = _dataset()
    candidates = runtime.build_candidates(
        runtime.prepare_dataset(dataset),
        runtime.SECTOR_ETF_ROTATION_FAMILY,
        _parameters(),
    )
    first = next(
        row
        for row in candidates
        if row["decision_date"] == dataset["evaluation_dates"][0]
        and row["rank"] == 1
    )

    assert first["symbol"] == "XLK"
    assert first["signal_date"] == dataset["evaluation_dates"][1]
    assert first["entry_price"] == dataset["daily_bars"]["XLK"][71]["open"]
    assert first["expected_gross_move_fraction"] >= 0.01
    assert first["exit_date"] <= dataset["evaluation_dates"][3]


def test_production_rebuilds_the_same_sector_rank():
    dataset = _dataset()
    decision_date = dataset["evaluation_dates"][0]
    daily_bars = {
        symbol: [bar for bar in bars if bar["date"] <= decision_date]
        for symbol, bars in dataset["daily_bars"].items()
    }
    calendar_dates = [
        bar["date"] for bar in daily_bars["SPY"]
    ]

    signal = runtime.evaluate_production_signal(
        {
            "family_id": runtime.SECTOR_ETF_ROTATION_FAMILY,
            "decision_date": decision_date,
            "next_session_date": dataset["evaluation_dates"][1],
            "calendar_dates": calendar_dates,
            "daily_history_complete": True,
            "daily_bars": daily_bars,
            "symbols": list(source.SYMBOLS),
        },
        family_id=runtime.SECTOR_ETF_ROTATION_FAMILY,
        parameters=_parameters(),
        frozen_universe={"symbols": list(source.SYMBOLS)},
    )

    assert signal["symbol"] == "XLK"
    assert signal["rank"] == 1
    assert signal["holding_trading_days"] == 3
    assert signal["expected_gross_move_fraction"] >= 0.01


def test_preflight_opens_only_committed_metadata(tmp_path, monkeypatch):
    dates = _days(120)
    manifest, _ = freeze_dataset_contract(
        {
            "schema_version": 1,
            "dataset_id": "dataset-sector-rotation-preflight",
            "registered_at": "2026-07-23T20:00:00Z",
            "requested_dates": dates,
            "dataset_payload": {
                "lane": "development",
                "claim_scope": "DEVELOPMENT_ONLY",
                "evidence_paths": [
                    "tests/test_sector_etf_rotation_search.py"
                ],
                "inspected": True,
                "point_in_time_evidence": True,
                "development_training_contaminated": False,
                "confirmation_access_permitted": False,
                "sector_rotation_runtime": {
                    "source_variant_id": source.VARIANT_ID,
                    "target_family_id": (
                        runtime.SECTOR_ETF_ROTATION_FAMILY
                    ),
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
            "family_id": runtime.SECTOR_ETF_ROTATION_FAMILY,
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
    private_path = (
        private_root
        / "_derived/sector/frozen-inputs.json.gz"
    )
    private_path.parent.mkdir(parents=True)
    private_path.write_bytes(b"outcome rows remain unopened")
    input_sha256 = "c" * 64
    result_sha256 = "d" * 64
    monkeypatch.setattr(source, "DATA_ROOT", private_root)
    monkeypatch.setattr(source, "PRIVATE_INPUT_PATH", private_path)
    monkeypatch.setattr(
        discovery,
        "_source_graph",
        lambda **_kwargs: (
            {
                "source_selection": {
                    "input_sha256": input_sha256,
                    "private_input_file_sha256": sha256_file(
                        private_path
                    ),
                }
            },
            {"result_sha256": result_sha256, "records": []},
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
        created_at="2026-07-23T20:00:00Z",
        root=tmp_path / "continuous",
        enforce_commit=False,
    )

    assert len(contract["trial_family"]) == 32
    assert len(contract["development_warmup_dates"]) == 60
    assert len(contract["development_dates"]) == 120
    assert len(contract["embargo_dates"]) == 5
    assert len(contract["confirmation_dates"]) == 66
    assert contract["development_scope"]["symbols"] == sorted(
        source.SYMBOLS
    )
    assert set(contract["development_dates"]).isdisjoint(
        contract["confirmation_dates"]
    )
    assert contract["new_mechanism_family_slot_consumed"] is False
    assert capacity.is_file()


def test_status_never_waits_for_new_family_reset(tmp_path):
    result = discovery.status(root=tmp_path)

    assert result["state"] == "READY_TO_FREEZE"
    assert result["calendar_wait_required"] is False
    assert result["new_mechanism_family_slot_consumed"] is False
    assert result["confirmation_outcomes_accessed"] is False

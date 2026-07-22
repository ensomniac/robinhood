from __future__ import annotations

import gzip
import json
from datetime import date, timedelta
from pathlib import Path

import dense_strategy_plugin as plugin
import dense_strategy_runtime as runtime
from historical_store import canonical_sha256, sha256_file
from learning_data import freeze_dataset_contract


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


def _manifest(tmp_path: Path, dataset: dict, *, external_exists: bool = True) -> Path:
    store = tmp_path / "historical-store"
    external = store / "_derived/dense/pullback.json.gz"
    if external_exists:
        external.parent.mkdir(parents=True)
        with gzip.open(external, "wt", encoding="utf-8") as destination:
            json.dump(dataset, destination, sort_keys=True, separators=(",", ":"))
        file_hash = sha256_file(external)
    else:
        file_hash = "a" * 64
    path, _ = freeze_dataset_contract(
        {
            "schema_version": 1,
            "dataset_id": "dataset-dense-pullback-development",
            "registered_at": "2026-07-22T19:00:00-04:00",
            "requested_dates": dataset["evaluation_dates"],
            "dataset_payload": {
                "lane": "development",
                "claim_scope": "DEVELOPMENT_ONLY",
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
            },
        },
        tmp_path / "manifests",
    )
    return path


def _contract(manifest: Path, dates: list[str]) -> dict:
    return {
        "family_id": runtime.ETF_PULLBACK_FAMILY,
        "development_dates": dates,
        "dataset_manifest": str(manifest),
    }


def test_preflight_reads_only_frozen_metadata(tmp_path):
    dataset = _dataset()
    manifest = _manifest(tmp_path, dataset, external_exists=False)

    result = plugin.preflight(
        _contract(manifest, dataset["evaluation_dates"])
    )

    assert result["verified_capacity"] == 120
    assert result["point_in_time_complete"] is True
    assert result["external_outcomes_opened"] is False
    assert result["provider_telemetry"]["dataset_loads"] == 0


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

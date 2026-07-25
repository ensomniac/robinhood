from __future__ import annotations

from pathlib import Path

import spy_rsi2_data as shared
import spy_rsi2_yahoo_data as source
from historical_store import HistoricalStoreConfig


def _failure_inspection(tmp_path: Path) -> Path:
    path = tmp_path / "massive-failure-inspection.json"
    shared._write(
        path,
        {
            "state": "DEVELOPMENT_SOURCE_FAILURE_INSPECTED",
            "inspection_sha256": "a" * 64,
            "source_retry_authorized": False,
            "provider_purchase_authorized": False,
            "valid": True,
        },
    )
    return path


def test_yahoo_fallback_freezes_same_rule_and_partitions_without_outcomes(
    tmp_path: Path,
    monkeypatch,
):
    monkeypatch.setattr(
        source.strategy_discovery,
        "require_committed",
        lambda _path: None,
    )
    monkeypatch.setattr(source.outcome_exposure, "read_index", lambda: [])
    monkeypatch.setattr(shared, "_repo_path", lambda path: str(path))

    contract = source.build_source_contract(
        created_at="2026-07-25T01:40:00Z",
        massive_failure_inspection_path=_failure_inspection(tmp_path),
    )

    assert contract["source_id"] == source.SOURCE_ID
    assert contract["frozen_rule"]["parameters"] == shared.PARAMETERS
    assert contract["warmup_dates"] == shared.partitions()["warmup_dates"]
    assert contract["development_dates"] == shared.partitions()["development_dates"]
    assert contract["confirmation_dates"] == shared.partitions()["confirmation_dates"]
    assert contract["request_policy"]["authorized_provider_requests"] == 1
    assert contract["request_policy"]["no_purchase_required"] is True
    assert contract["request_policy"]["retries_permitted"] == 0
    assert contract["request_policy"]["confirmation_request_permitted"] is False
    assert contract["development_request"]["response_semantics"][
        "raw_close_adjustment"
    ] == "splits_only"
    assert contract["market_prices_accessed"] is False


def _bar(day: str, index: int) -> dict:
    close = 100.0 + index / 100
    return {
        "date": day,
        "open": close - 0.05,
        "high": close + 0.25,
        "low": close - 0.25,
        "close": close,
        "volume": 1_000_000,
    }


def test_yahoo_collection_retains_no_confirmation_prices(
    tmp_path: Path,
    monkeypatch,
):
    monkeypatch.setattr(
        source.strategy_discovery,
        "require_committed",
        lambda _path: None,
    )
    monkeypatch.setattr(source.outcome_exposure, "read_index", lambda: [])
    monkeypatch.setattr(shared, "_repo_path", lambda path: str(path))
    monkeypatch.setattr(
        source.outcome_exposure,
        "ensure_record",
        lambda _record: True,
    )
    contract = source.build_source_contract(
        created_at="2026-07-25T01:40:00Z",
        massive_failure_inspection_path=_failure_inspection(tmp_path),
    )
    contract_path = tmp_path / "contract.json"
    shared._write(contract_path, contract)
    inspection_path = tmp_path / "inspection.json"
    shared._write(
        inspection_path,
        {
            "state": "SOURCE_CONTRACT_INSPECTED_READY",
            "contract_sha256": contract["contract_sha256"],
            "valid": True,
        },
    )
    expected_dates = [
        *contract["warmup_dates"],
        *contract["development_dates"],
    ]

    path, collection = source.collect_development(
        contract_path,
        inspection_path,
        collected_at="2026-07-25T01:41:00Z",
        root=tmp_path / "public",
        store_config=HistoricalStoreConfig(tmp_path / "private", 0),
        fetcher=lambda _contract: (
            [_bar(day, index) for index, day in enumerate(expected_dates)],
            {
                "requests": 1,
                "request_seconds": 0.1,
                "pacing_wait_seconds": 0.0,
                "cache_hits": 0,
                "failures": 0,
            },
        ),
    )

    assert path.is_file()
    assert collection["row_count"] == len(expected_dates)
    assert collection["provider_requests"] == 1
    assert collection["confirmation_prices_accessed"] is False
    assert collection["strategy_metrics_computed"] == 0

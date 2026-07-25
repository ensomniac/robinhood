from __future__ import annotations

from pathlib import Path

import outcome_exposure
import spy_rsi2_data as source
from historical_store import HistoricalStoreConfig


def test_frozen_xnys_calendar_covers_regular_and_extraordinary_closures():
    split = source.partitions()
    all_sessions = {
        day
        for dates in split.values()
        for day in dates
    }

    assert len(split["warmup_dates"]) == 253
    assert len(split["development_dates"]) == 2_012
    assert len(split["confirmation_dates"]) == 2_008
    assert split["embargo_dates"] == [
        "2006-01-03",
        "2006-01-04",
        "2006-01-05",
        "2006-01-06",
        "2006-01-09",
    ]
    assert "1998-01-01" not in all_sessions
    assert "1998-01-02" in all_sessions
    assert "2001-09-10" in all_sessions
    assert all(
        day not in all_sessions
        for day in (
            "2001-09-11",
            "2001-09-12",
            "2001-09-13",
            "2001-09-14",
            "2004-06-11",
            "2007-01-02",
            "2012-10-29",
            "2012-10-30",
        )
    )
    assert "2001-09-17" in all_sessions
    assert "2012-10-31" in all_sessions


def test_source_contract_freezes_one_rule_request_and_locked_confirmation(
    monkeypatch,
):
    monkeypatch.setattr(
        source.strategy_discovery,
        "require_committed",
        lambda _path: None,
    )

    contract = source.build_source_contract(
        created_at="2026-07-25T01:30:00Z"
    )

    assert contract["contract_sha256"] == source.self_hash(
        contract,
        "contract_sha256",
    )
    assert contract["frozen_rule"]["parameters"] == source.PARAMETERS
    assert contract["frozen_rule"]["trial_count"] == 1
    assert contract["request_policy"]["authorized_provider_requests"] == 1
    assert contract["request_policy"]["retries_permitted"] == 0
    assert contract["request_policy"]["confirmation_request_permitted"] is False
    assert contract["market_prices_accessed"] is False
    assert contract["confirmation_prices_accessed"] is False


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


def test_development_collection_retains_only_frozen_development_prices(
    tmp_path: Path,
    monkeypatch,
):
    monkeypatch.setattr(
        source.strategy_discovery,
        "require_committed",
        lambda _path: None,
    )
    monkeypatch.setattr(
        source.outcome_exposure,
        "ensure_record",
        lambda _record: True,
    )
    monkeypatch.setattr(source, "_repo_path", lambda path: str(path))
    contract = source.build_source_contract(
        created_at="2026-07-25T01:30:00Z"
    )
    contract_path = tmp_path / "contract.json"
    source._write(contract_path, contract)
    inspection = {
        "state": "SOURCE_CONTRACT_INSPECTED_READY",
        "contract_sha256": contract["contract_sha256"],
        "valid": True,
    }
    inspection_path = tmp_path / "inspection.json"
    source._write(inspection_path, inspection)
    expected_dates = [
        *contract["warmup_dates"],
        *contract["development_dates"],
    ]

    path, collected = source.collect_development(
        contract_path,
        inspection_path,
        collected_at="2026-07-25T01:31:00Z",
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
    assert collected["row_count"] == len(expected_dates)
    assert collected["provider_requests"] == 1
    assert collected["confirmation_prices_accessed"] is False
    assert (
        max(expected_dates)
        < min(contract["embargo_dates"])
        < min(contract["confirmation_dates"])
    )

    second_path, second = source.collect_development(
        contract_path,
        inspection_path,
        collected_at="2026-07-25T01:32:00Z",
        root=tmp_path / "public",
        store_config=HistoricalStoreConfig(tmp_path / "private", 0),
        fetcher=lambda _contract: (_ for _ in ()).throw(
            AssertionError("provider must not be called on resume")
        ),
    )
    assert second_path == path
    assert second == collected


def test_pre2014_confirmation_scope_is_globally_untouched():
    split = source.partitions()

    outcome_exposure.assert_untouched(
        {"dates": split["confirmation_dates"], "symbols": ["SPY"]},
        outcome_exposure.read_index(),
    )


def test_permission_failure_records_one_request_and_zero_outcomes(
    tmp_path: Path,
    monkeypatch,
):
    monkeypatch.setattr(
        source.strategy_discovery,
        "require_committed",
        lambda _path: None,
    )
    monkeypatch.setattr(source, "_repo_path", lambda path: str(path))
    contract = source.build_source_contract(
        created_at="2026-07-25T01:30:00Z"
    )
    contract_path = tmp_path / "contract.json"
    source._write(contract_path, contract)
    inspection_path = tmp_path / "inspection.json"
    source._write(
        inspection_path,
        {
            "state": "SOURCE_CONTRACT_INSPECTED_READY",
            "contract_sha256": contract["contract_sha256"],
            "valid": True,
        },
    )

    _path, failure = source.record_collection_failure(
        contract_path,
        inspection_path,
        failed_at="2026-07-25T01:31:13Z",
        http_status=403,
        root=tmp_path / "public",
    )

    assert failure["failure_category"] == "permanent_permission"
    assert failure["provider_telemetry"]["requests"] == 1
    assert failure["retained_rows"] == 0
    assert failure["retries"] == 0
    assert failure["confirmation_prices_accessed"] is False

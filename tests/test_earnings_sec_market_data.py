from pathlib import Path

import earnings_sec_market_data as source
from historical_store import HistoricalDayStore


def test_reaction_date_uses_first_fully_observable_session():
    sessions = ["2010-04-20", "2010-04-21", "2010-04-22"]

    assert (
        source._reaction_date("2010-04-20 08:15:00", sessions)
        == "2010-04-20"
    )
    assert (
        source._reaction_date("2010-04-20 16:15:00", sessions)
        == "2010-04-21"
    )


def test_collection_resumes_hash_valid_tasks_without_duplicate_requests(
    tmp_path: Path,
    monkeypatch,
):
    monkeypatch.setattr(
        source.strategy_discovery, "require_committed", lambda _path: None
    )
    monkeypatch.setattr(
        source.outcome_exposure, "ensure_record", lambda _record: True
    )
    monkeypatch.setattr(source, "_repo_path", lambda path: str(path))
    selection = {
        "development_dates": ["2010-01-04"],
        "event_metadata_by_date": {"2010-01-04": []},
    }
    monkeypatch.setattr(
        source, "_development_selection", lambda _store: selection
    )
    request = {
        "request_sha256": "a" * 64,
        "symbol": "EDGE",
        "start": "2009-11-16",
        "end": "2010-01-08",
    }
    contract = {
        "contract_sha256": "b" * 64,
        "selection_sha256": source.canonical_sha256(selection),
        "requests": [request],
        "development_scope": {
            "dates": ["2009-11-16", "2010-01-04"],
            "symbols": ["EDGE"],
        },
    }
    contract["contract_sha256"] = source.v5.self_hash(
        contract, "contract_sha256"
    )
    contract_path = tmp_path / "contract.json"
    source._write(contract_path, contract)
    inspection_path = tmp_path / "inspection.json"
    source._write(
        inspection_path,
        {
            "contract_sha256": contract["contract_sha256"],
            "inspection_sha256": "c" * 64,
            "state": "MARKET_DATA_CONTRACT_INSPECTED_READY",
            "development_provider_access_authorized": True,
            "valid": True,
        },
    )
    store = HistoricalDayStore(tmp_path / "store", min_free_bytes=0)
    calls = 0

    def fetcher(_request):
        nonlocal calls
        calls += 1
        return {
            "schema_version": 1,
            "request_sha256": request["request_sha256"],
            "symbol": "EDGE",
            "status": "COMPLETE",
            "missing_reason": None,
            "rows": [
                {
                    "symbol": "EDGE",
                    "date": "2010-01-04",
                    "open": 10,
                    "high": 11,
                    "low": 9,
                    "close": 10.5,
                    "volume": 1000,
                }
            ],
        }

    _path, first = source.collect_development(
        contract_path,
        inspection_path,
        collected_at="2026-07-25T04:00:00Z",
        store=store,
        root=tmp_path / "public",
        fetcher=fetcher,
    )
    _path, second = source.collect_development(
        contract_path,
        inspection_path,
        collected_at="2026-07-25T04:00:00Z",
        store=store,
        root=tmp_path / "public",
        fetcher=fetcher,
    )

    assert calls == 1
    assert first["provider_telemetry"]["requests"] == 1
    assert second["provider_telemetry"]["cache_hits"] == 1
    assert second["provider_telemetry"]["requests"] == 0
    assert second["confirmation_prices_accessed"] is False

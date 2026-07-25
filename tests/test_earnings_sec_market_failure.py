import earnings_sec_market_failure as source
from historical_store import HistoricalDayStore


def test_permission_failure_retains_no_outcomes(tmp_path, monkeypatch):
    monkeypatch.setattr(
        source.strategy_discovery, "require_committed", lambda _path: None
    )
    monkeypatch.setattr(source.outcome_exposure, "read_index", lambda: [])
    monkeypatch.setattr(source.source, "_repo_path", lambda path: str(path))
    monkeypatch.setattr(
        source, "sha256_file", lambda _path: "d" * 64
    )
    monkeypatch.setattr(
        source.source,
        "sha256_file",
        lambda _path: "d" * 64,
    )
    contract = {
        "contract_sha256": "a" * 64,
        "implementation_hashes": {
            "earnings_sec_market_data.py": "d" * 64
        },
        "development_scope": {
            "dates": ["2010-01-04"],
            "symbols": ["AAPL"],
        },
        "requests": [
            {
                "request_sha256": "b" * 64,
                "symbol": "AAPL",
                "path": "/frozen",
            }
        ],
    }
    contract["contract_sha256"] = source.v5.self_hash(
        contract, "contract_sha256"
    )
    contract_path = tmp_path / "contract.json"
    source.source._write(contract_path, contract)
    inspection_path = tmp_path / "inspection.json"
    source.source._write(
        inspection_path,
        {
            "contract_sha256": contract["contract_sha256"],
            "inspection_sha256": "c" * 64,
            "state": "MARKET_DATA_CONTRACT_INSPECTED_READY",
            "valid": True,
        },
    )

    failure = source.build_failure(
        contract_path,
        inspection_path,
        observed_at="2026-07-25T03:32:18Z",
        store=HistoricalDayStore(tmp_path / "store", min_free_bytes=0),
    )

    assert failure["error"]["http_status"] == 403
    assert failure["failure_boundary"]["rows_returned"] == 0
    assert failure["failure_boundary"]["strategy_metrics_computed"] == 0
    assert failure["disposition"]["same_source_retry_permitted"] is False
    assert (
        failure["disposition"]["exact_no_purchase_source_fallback_permitted"]
        is True
    )

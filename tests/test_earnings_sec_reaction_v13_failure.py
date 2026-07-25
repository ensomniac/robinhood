from datetime import date, timedelta
from pathlib import Path

import earnings_sec_reaction_v13_failure as source


def test_v13_failure_constants_bind_exact_partial_prefix():
    assert source.RETAINED_TASK_COUNT == 26
    assert source.FAILED_ORDINAL == 26
    assert source.FAILED_SYMBOL == "AMCF"
    assert source.FAILURE_CODE == "YAHOO_INVALID_OHLCV"


def test_v13_failure_records_zero_metric_boundary(monkeypatch, tmp_path: Path):
    requests = [
        {
            "symbol": f"S{index:03d}" if index != 26 else "AMCF",
            "request_sha256": f"{index:064x}",
        }
        for index in range(635)
    ]
    contract = {
        "development_data_requests": requests,
        "development_scope": {
            "dates": [
                (date(2012, 1, 1) + timedelta(days=day)).isoformat()
                for day in range(840)
            ],
            "symbols": [row["symbol"] for row in requests],
        },
    }
    search = {
        "artifact_sha256": (
            "36a9738a580d04c53bd94b5a683b341febdc115e1666396f58d5d1ac22852da7"
        ),
        "family_contract": contract,
    }
    inspection = {
        "inspection_sha256": (
            "52bbde4c0d4136aaf7c7e6d1a486cee745064afdc1efc0ab719679ebc3ab64cd"
        ),
        "state": "REACTION_V13_SEARCH_INSPECTED_READY_FOR_COLLECTION",
        "valid": True,
        "search_sha256": search["artifact_sha256"],
    }
    monkeypatch.setattr(
        source.strategy_discovery,
        "require_committed",
        lambda _path: None,
    )
    monkeypatch.setattr(
        source.strategy_discovery,
        "load_artifact",
        lambda *_args, **_kwargs: search,
    )
    monkeypatch.setattr(
        source.capacity,
        "_read",
        lambda _path: inspection,
    )
    monkeypatch.setattr(
        source.capacity,
        "_repo_path",
        lambda path: str(path),
    )
    monkeypatch.setattr(source, "sha256_file", lambda _path: "a" * 64)
    monkeypatch.setattr(
        source.outcome_exposure, "read_index", lambda: []
    )
    monkeypatch.setattr(
        source.outcome_exposure,
        "find_overlaps",
        lambda _scope, _records: [],
    )
    store = type("Store", (), {"root": tmp_path})()
    for request in requests[:26]:
        path = source.collection._task_path(
            store, search["artifact_sha256"], request["request_sha256"]
        )
        source.market._write_private(
            path,
            {
                "request_sha256": request["request_sha256"],
                "task_sha256": "task",
                "symbol": request["symbol"],
                "status": "COMPLETE",
                "rows": [],
            },
        )
    monkeypatch.setattr(
        source.capacity,
        "self_hash",
        lambda value, field: value.get(field, "task"),
    )

    failure = source.build_failure(
        failed_at="2026-07-25T09:02:30Z", store=store
    )

    assert failure["state"] == "REACTION_V13_INVALID_OHLCV_TERMINAL"
    assert failure["provider_telemetry"]["attempted_requests"] == 27
    assert failure["provider_telemetry"]["retained_tasks"] == 26
    assert failure["failure"]["failed_request"]["symbol"] == "AMCF"
    assert len(failure["opened_scope"]["symbols"]) == 27
    assert failure["strategy_metrics_computed"] == 0
    assert failure["confirmation_prices_accessed"] is False
    assert failure["successor_authority"]["remaining_untouched_symbols"] == 608

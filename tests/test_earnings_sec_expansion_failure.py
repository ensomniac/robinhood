from pathlib import Path

import earnings_sec_expansion_capacity as capacity
import earnings_sec_expansion_failure as source
import earnings_sec_expansion_failure_inspection as inspection


def _commit_bypass(monkeypatch):
    monkeypatch.setattr(
        source.strategy_discovery, "require_committed", lambda _path: None
    )
    monkeypatch.setattr(
        inspection.strategy_discovery, "require_committed", lambda _path: None
    )


def _lineage_bypass(monkeypatch, tmp_path: Path):
    first = capacity.requests()[0]
    plan = {
        "created_at": "2026-07-25T07:35:00Z",
        "plan_sha256": "b17f1cd4982ee5e7c043dedda85e4bacbcbbe4679e91c12cc51c74c40b434b43",
        "requests": capacity.requests(),
    }
    inspection_row = {
        "inspection_sha256": (
            "59705455815bd3b1f87c2f2d4418024fbdb78d88028335ff0f4bbafd2885c336"
        ),
        "plan_sha256": plan["plan_sha256"],
        "state": "SEC_EXPANSION_COLLECTION_PLAN_INSPECTED_READY",
        "valid": True,
    }
    monkeypatch.setattr(
        capacity,
        "_read",
        lambda path: (
            plan
            if path == source.PLAN
            else inspection_row
        ),
    )
    monkeypatch.setattr(
        source.collection,
        "build_plan",
        lambda created_at: plan,
    )
    monkeypatch.setattr(
        capacity,
        "self_hash",
        lambda value, field: value.get(field, "hash"),
    )
    monkeypatch.setattr(source, "sha256_file", lambda _path: "a" * 64)
    monkeypatch.setattr(capacity, "_repo_path", lambda path: str(path))
    store = type("Store", (), {"root": tmp_path})()
    destination = source.collection._archive_path(
        store, plan["plan_sha256"], first["request_sha256"]
    )
    assert not destination.exists()
    return store


def test_failure_records_exact_zero_row_404_boundary(monkeypatch, tmp_path):
    _commit_bypass(monkeypatch)
    store = _lineage_bypass(monkeypatch, tmp_path)

    failure = source.build_failure(
        failed_at="2026-07-25T07:40:00Z", store=store
    )

    assert failure["failed_request"]["quarter"] == "2012q1"
    assert failure["provider_response"]["http_status"] == 404
    assert failure["provider_telemetry"]["requests"] == 1
    assert failure["provider_telemetry"]["retained_archives"] == 0
    assert failure["metadata_rows_accessed"] == 0
    assert failure["market_prices_accessed"] is False
    assert failure["broker_actions"] == 0
    assert (
        failure["root_path_disposition"]["official_index_root"]
        == "https://www.sec.gov/files/dera/data/"
        "financial-statement-notes-data-sets"
    )


def test_failure_forbids_same_version_retry(monkeypatch, tmp_path):
    _commit_bypass(monkeypatch)
    store = _lineage_bypass(monkeypatch, tmp_path)

    failure = source.build_failure(
        failed_at="2026-07-25T07:40:00Z", store=store
    )

    assert (
        failure["root_path_disposition"]["same_version_retry_permitted"]
        is False
    )
    assert (
        failure["successor_authority"][
            "source_root_correction_permitted_after_inspection"
        ]
        is True
    )
    assert (
        failure["successor_authority"]["market_price_access_permitted"]
        is False
    )

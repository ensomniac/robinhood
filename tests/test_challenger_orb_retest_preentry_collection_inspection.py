from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pytest

import challenger_orb_retest_preentry as preentry
import challenger_orb_retest_preentry_collection_inspection as inspection


def _fixture(tmp_path: Path) -> dict:
    request = {
        "request_sha256": "request-a",
        "kind": "candidate_bars",
        "symbol": "TEST",
        "date": "2026-01-02",
    }
    rows = [{"source_timestamp": "2026-01-02T09:30:00-05:00", "close": 10.0}]
    wrapper = {
        "schema_version": 1,
        "dataset_id": preentry.DATASET_ID,
        "manifest_sha256": "manifest-a",
        "request_sha256": "request-a",
        "status": "SUCCESS",
        "origin": "LOCAL_CACHE",
        "row_count": 1,
        "rows_sha256": preentry._sha256_json(rows),
        "error_category": None,
        "target_outcomes_observed_or_derived": False,
    }
    record = {
        "request_sha256": "request-a",
        "wrapper_sha256": preentry._sha256_json(wrapper),
        "status": "SUCCESS",
        "kind": "candidate_bars",
        "origin": "LOCAL_CACHE",
        "row_count": 1,
        "rows_sha256": wrapper["rows_sha256"],
        "error_category": None,
    }
    counts = {
        "benchmark_bars_requests": 0,
        "candidate_bars_requests": 1,
        "candidate_trades_requests": 0,
        "expected_requests": 1,
        "failed_requests": 0,
        "LOCAL_CACHE_requests": 1,
        "pending_requests": 0,
        "rows": 1,
        "successful_requests": 1,
        "terminal_requests": 1,
    }
    index = {
        "schema_version": 1,
        "dataset_id": preentry.DATASET_ID,
        "manifest_sha256": "manifest-a",
        "status": "COLLECTION_COMPLETE",
        "counts": counts,
        "records": [record],
        "request_graph_sha256": "graph-a",
        "post_entry_data_accessed": False,
        "target_outcomes_observed_or_derived": False,
    }
    index_path = tmp_path / "collection-index.json.gz"
    wrapper_path = tmp_path / "wrapper.json.gz"
    status_path = tmp_path / "status.json"
    result_path = tmp_path / "result.json"
    preentry._write_gzip(index_path, index)
    preentry._write_gzip(wrapper_path, wrapper)
    preentry._write_json(
        status_path,
        {
            "dataset_id": preentry.DATASET_ID,
            "manifest_sha256": "manifest-a",
            "status": "COLLECTION_COMPLETE",
            "counts": counts,
            "private_collection_sha256": preentry._sha256_file(index_path),
            "post_entry_data_accessed": False,
            "target_outcomes_observed_or_derived": False,
        },
    )
    return {
        "manifest": {"manifest_sha256": "manifest-a"},
        "config": SimpleNamespace(root=tmp_path),
        "selection": {"requests": [request], "request_graph_sha256": "graph-a"},
        "index": index,
        "index_path": index_path,
        "wrapper_path": wrapper_path,
        "status_path": status_path,
        "result_path": result_path,
        "rows": rows,
    }


def _patches(value: dict):
    return (
        patch.object(
            preentry,
            "_load_contract",
            return_value=(value["manifest"], value["config"], value["selection"]),
        ),
        patch.object(preentry, "_index_path", return_value=value["index_path"]),
        patch.object(preentry, "_wrapper_path", return_value=value["wrapper_path"]),
        patch.object(preentry, "_trigger_path", return_value=value["config"].root / "trigger.json.gz"),
        patch.object(preentry, "_build_index", return_value=value["index"]),
        patch.object(preentry, "_load_rows", return_value=value["rows"]),
        patch.object(inspection, "HistoricalDayStore"),
        patch.object(inspection, "LocalHistoricalClient"),
    )


def test_inspection_reconciles_all_wrappers_and_canonical_rows(tmp_path: Path) -> None:
    value = _fixture(tmp_path)
    patches = _patches(value)
    with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patches[6], patches[7]:
        result = inspection.inspect_collection(
            manifest_path=tmp_path / "manifest.json",
            env_path=tmp_path / ".env",
            status_path=value["status_path"],
            result_path=value["result_path"],
        )

    assert result["status"] == "COLLECTION_INSPECTED"
    assert result["counts"]["successful_requests"] == 1
    assert result["inspection"]["all_canonical_rows_reloaded_and_rehashed"] is True
    assert result["target_outcomes_observed_or_derived"] is False


def test_inspection_rejects_existing_trigger_artifact(tmp_path: Path) -> None:
    value = _fixture(tmp_path)
    (tmp_path / "trigger.json.gz").write_bytes(b"forbidden")
    patches = _patches(value)
    with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5], patches[6], patches[7], pytest.raises(
        inspection.ChallengerPreentryCollectionInspectionError,
        match="pre-trigger boundary",
    ):
        inspection.inspect_collection(
            manifest_path=tmp_path / "manifest.json",
            env_path=tmp_path / ".env",
            status_path=value["status_path"],
            result_path=value["result_path"],
        )

from __future__ import annotations

import json
from pathlib import Path

import asr_capacity_recovery as recovery
import asr_capacity_recovery_collection as collection
import asr_capacity_recovery_failure as failure


def _write_page(
    root: Path,
    offset: int,
    identifiers: list[str],
    *,
    total: int,
) -> None:
    path = collection._cache_path(
        root,
        failure.V2_CONTRACT_SHA256,
        failure.FAILED_PHRASE,
        failure.FAILED_WINDOW_START,
        failure.FAILED_WINDOW_END,
        offset,
        100,
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "hits": {
                    "total": {"value": total, "relation": "eq"},
                    "hits": [{"_id": identifier} for identifier in identifiers],
                }
            }
        ),
        encoding="utf-8",
    )


def test_failure_inspection_retires_duplicate_pagination(
    tmp_path: Path, monkeypatch
):
    monkeypatch.setattr(
        failure,
        "_authority",
        lambda: (
            {"source_contract": {"page_size": 100}},
            {"inspection_sha256": "a" * 64},
        ),
    )
    _write_page(tmp_path, 0, [f"hit-{index}" for index in range(100)], total=101)
    _write_page(tmp_path, 100, ["hit-99"], total=101)
    result = failure.build_disposition(store_root=tmp_path)
    assert result["disposition"] == "RETIRED_UNSTABLE_EFTS_PAGINATION"
    assert result["reported_exact_total"] == 101
    assert result["returned_hit_rows"] == 101
    assert result["unique_hit_ids"] == 100
    assert result["duplicated_hit_id_count"] == 1
    assert result["duplicate_excess_rows"] == 1
    assert result["same_contract_resume_or_repair_permitted"] is False
    assert result["separately_frozen_no_pagination_version_permitted"] is True
    assert result["matched_document_access_performed"] is False
    assert result["verified_event_count"] is None
    assert result["market_outcomes_accessed"] is False
    assert result["broker_actions"] == 0


def test_disposition_is_content_addressed(tmp_path: Path, monkeypatch):
    value = {
        "schema_version": 1,
        "disposition_sha256": "a" * 64,
    }
    monkeypatch.setattr(
        failure, "build_disposition", lambda store_root=None: value
    )
    path, written = failure.write_disposition(output_root=tmp_path)
    assert path.name == (
        f"{recovery.CANDIDATE_ID}-{'a' * 64}.json"
    )
    assert recovery.read_object(path) == written

from __future__ import annotations

import json
from pathlib import Path

import asr_capacity as capacity
import asr_capacity_collection as collection
import asr_capacity_collection_inspection as inspection


class FakeResponse:
    def __init__(self, value):
        self.content = json.dumps(value).encode()

    def raise_for_status(self):
        return None


class FakeSession:
    def __init__(self, relation: str):
        self.headers = {}
        self.relation = relation
        self.calls = []

    def get(self, url, timeout):
        self.calls.append((url, timeout))
        return FakeResponse(
            {
                "hits": {
                    "total": {"value": 10_000, "relation": self.relation},
                    "hits": [{"_id": f"hit-{index}"} for index in range(100)],
                }
            }
        )


def test_inexact_total_blocks_before_pagination_or_documents(
    tmp_path: Path, monkeypatch
):
    monkeypatch.setattr(collection, "_load_authority", lambda: (
        capacity.build_contract(),
        {"inspection_sha256": "a" * 64},
    ))
    session = FakeSession("gte")
    result = collection.collect_search_denominator(
        status_path=tmp_path / "status.json",
        require_published=False,
        session=session,
        store_root=tmp_path / "private",
    )
    assert result["state"] == "BLOCKED_INEXACT_EFTS_DENOMINATOR"
    assert result["query_count"] == 4
    assert len(session.calls) == 4
    assert result["pagination_complete"] is False
    assert result["filing_hit_count"] is None
    assert result["matched_document_access_performed"] is False
    assert result["verified_event_count"] is None
    assert result["market_outcomes_accessed"] is False


def test_inspection_rebuilds_inexact_provider_blocker(tmp_path: Path, monkeypatch):
    monkeypatch.setattr(collection, "_load_authority", lambda: (
        capacity.build_contract(),
        {"inspection_sha256": "a" * 64},
    ))
    status_path = tmp_path / "status.json"
    store_root = tmp_path / "private"
    collection.collect_search_denominator(
        status_path=status_path,
        require_published=False,
        session=FakeSession("gte"),
        store_root=store_root,
    )
    path, result = inspection.inspect_collection(
        status_path, output_root=tmp_path / "inspections", store_root=store_root
    )
    assert path.is_file()
    assert result["state"] == "BLOCKED_INEXACT_EFTS_DENOMINATOR"
    assert result["all_reported_totals_exact"] is False
    assert result["verified_event_count"] is None
    assert result["market_outcomes_accessed"] is False


def test_request_scope_keeps_exact_dates_and_phrase():
    phrase = capacity.SEARCH_PHRASES[0]
    params = collection._query_params(phrase, 0)
    assert params == {
        "q": phrase,
        "dateRange": "custom",
        "startdt": "2010-01-01",
        "enddt": "2025-12-31",
        "from": 0,
        "size": 100,
    }

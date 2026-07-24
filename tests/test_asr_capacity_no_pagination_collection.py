from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import parse_qs, urlparse

import requests

import asr_capacity_no_pagination as capacity
import asr_capacity_no_pagination_collection as collection
import asr_capacity_no_pagination_collection_inspection as inspection


class FakeResponse:
    def __init__(self, value, *, status_code=200):
        self.content = json.dumps(value).encode()
        self.status_code = status_code

    def raise_for_status(self):
        if self.status_code >= 400:
            response = requests.Response()
            response.status_code = self.status_code
            raise requests.HTTPError(
                f"{self.status_code} error",
                response=response,
            )


class SinglePageSession:
    def __init__(self, *, fail_once=False):
        self.headers = {}
        self.calls = []
        self.fail_once = fail_once

    def get(self, url, timeout):
        query = parse_qs(urlparse(url).query)
        phrase = query["q"][0]
        start = query["startdt"][0]
        end = query["enddt"][0]
        offset = int(query["from"][0])
        self.calls.append((phrase, start, end, offset, timeout))
        assert offset == 0
        if self.fail_once:
            self.fail_once = False
            return FakeResponse({}, status_code=500)
        if (
            phrase == capacity.v1.SEARCH_PHRASES[0]
            and start == capacity.COLLECTION_START
            and end == capacity.COLLECTION_END
        ):
            total = {"value": 101, "relation": "eq"}
            count = 100
        else:
            total = {"value": 1, "relation": "eq"}
            count = 1
        hits = [
            {
                "_id": f"{phrase}-{start}-{end}-{index}",
                "_source": {
                    "file_date": start,
                    "ciks": ["1"],
                },
                "highlight": {"root_forms": ["query-dependent"]},
            }
            for index in range(count)
        ]
        return FakeResponse({"hits": {"total": total, "hits": hits}})


def _prepare(monkeypatch):
    contract = capacity.build_contract()
    monkeypatch.setattr(
        collection,
        "_load_authority",
        lambda: (
            Path("contract.json"),
            contract,
            {"inspection_sha256": "a" * 64},
        ),
    )
    monkeypatch.setattr(
        collection.SecConfig,
        "from_env",
        lambda *args, **kwargs: SimpleNamespace(user_agent="tests test@example.com"),
    )
    monkeypatch.setattr(collection.time, "sleep", lambda _seconds: None)
    return contract


def test_exact_over_page_splits_without_offset_and_inspection_rebuilds(
    tmp_path: Path, monkeypatch
):
    _prepare(monkeypatch)
    status_path = tmp_path / "status.json"
    store_root = tmp_path / "private"
    session = SinglePageSession()
    result = collection.collect_search_denominator(
        status_path=status_path,
        require_published=False,
        session=session,
        store_root=store_root,
    )
    assert result["state"] == "SEARCH_DENOMINATOR_COMPLETE"
    assert result["split_parent_window_count"] == 1
    assert result["terminal_leaf_window_count"] == 5
    assert result["offset_pagination_requests"] == 0
    assert result["filing_hit_count"] == 5
    assert result["unique_hit_count"] == 5
    assert all(call[3] == 0 for call in session.calls)
    assert result["matched_document_access_performed"] is False
    assert result["market_outcomes_accessed"] is False
    output, rebuilt = inspection.inspect_collection(
        status_path,
        output_root=tmp_path / "inspections",
        store_root=store_root,
    )
    assert output.is_file()
    assert rebuilt["denominator_complete"] is True
    assert rebuilt["offset_pagination_requests_rebuilt"] == 0
    assert rebuilt["private_hit_index_rehashed"] is True
    assert rebuilt["matched_document_manifest_freeze_permitted"] is True
    assert rebuilt["matched_document_access_permitted"] is False


def test_retryable_500_is_bounded_and_preserves_exact_scope(
    tmp_path: Path, monkeypatch
):
    _prepare(monkeypatch)
    session = SinglePageSession(fail_once=True)
    result = collection.collect_search_denominator(
        status_path=tmp_path / "status.json",
        require_published=False,
        session=session,
        store_root=tmp_path / "private",
    )
    assert result["state"] == "SEARCH_DENOMINATOR_COMPLETE"
    assert result["provider_telemetry"]["retry_attempts"] == 1
    assert result["provider_telemetry"]["retry_wait_seconds"] == 1.0
    assert result["provider_telemetry"]["failures"] == 1
    assert session.calls[0][:4] == session.calls[1][:4]
    assert result["offset_pagination_requests"] == 0


def test_canonical_hit_excludes_query_dependent_highlights():
    first = collection._canonical_hit(
        {
            "_id": "filing",
            "_source": {"file_date": "2025-01-01"},
            "highlight": {"root_forms": ["first"]},
        }
    )
    second = collection._canonical_hit(
        {
            "_id": "filing",
            "_source": {"file_date": "2025-01-01"},
            "highlight": {"root_forms": ["second"]},
        }
    )
    assert first == second
    assert "highlight" not in first

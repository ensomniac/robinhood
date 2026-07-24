from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from urllib.parse import parse_qs, urlparse

import pytest

import asr_capacity_recovery as recovery
import asr_capacity_recovery_collection as collection
import asr_capacity_recovery_collection_inspection as inspection


class FakeResponse:
    def __init__(self, value):
        self.content = json.dumps(value).encode()

    def raise_for_status(self):
        return None


class RecursiveFakeSession:
    def __init__(self):
        self.headers = {}
        self.calls = []

    def get(self, url, timeout):
        query = parse_qs(urlparse(url).query)
        phrase = query["q"][0]
        start = query["startdt"][0]
        end = query["enddt"][0]
        offset = int(query["from"][0])
        self.calls.append((phrase, start, end, offset, timeout))
        if (
            phrase == recovery.v1.SEARCH_PHRASES[0]
            and start == recovery.COLLECTION_START
            and end == recovery.COLLECTION_END
        ):
            total = {"value": 10_000, "relation": "gte"}
            hits = [{"_id": "ignored-lower-bound"}]
        else:
            total = {"value": 1, "relation": "eq"}
            hits = [{"_id": f"{phrase}-{start}-{end}"}]
        return FakeResponse({"hits": {"total": total, "hits": hits}})


class SingleDateInexactSession:
    def __init__(self):
        self.headers = {}

    def get(self, url, timeout):
        del url, timeout
        return FakeResponse(
            {
                "hits": {
                    "total": {"value": 10_000, "relation": "gte"},
                    "hits": [{"_id": "lower-bound"}],
                }
            }
        )


def _prepare(monkeypatch):
    contract = recovery.build_contract()
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
    return contract


def test_recursive_bisection_completes_and_inspection_rebuilds(
    tmp_path: Path, monkeypatch
):
    _prepare(monkeypatch)
    status_path = tmp_path / "status.json"
    store_root = tmp_path / "private"
    session = RecursiveFakeSession()
    result = collection.collect_search_denominator(
        status_path=status_path,
        require_published=False,
        session=session,
        store_root=store_root,
    )
    assert result["state"] == "SEARCH_DENOMINATOR_COMPLETE"
    assert result["inexact_parent_window_count"] == 1
    assert result["exact_leaf_window_count"] == 5
    assert result["filing_hit_count"] == 5
    assert result["unique_hit_count"] == 5
    assert result["matched_document_access_performed"] is False
    assert result["verified_event_count"] is None
    assert result["market_outcomes_accessed"] is False
    output, rebuilt = inspection.inspect_collection(
        status_path,
        output_root=tmp_path / "inspections",
        store_root=store_root,
    )
    assert output.is_file()
    assert rebuilt["denominator_complete"] is True
    assert rebuilt["private_hit_index_rehashed"] is True
    assert rebuilt["matched_document_manifest_freeze_permitted"] is True
    assert rebuilt["matched_document_access_permitted"] is False


def test_single_date_inexact_fails_closed_without_private_index(
    tmp_path: Path, monkeypatch
):
    contract = _prepare(monkeypatch)
    contract["source_contract"]["collection_start"] = "2025-01-01"
    contract["source_contract"]["collection_end_inclusive"] = "2025-01-01"
    monkeypatch.setattr(recovery, "COLLECTION_START", "2025-01-01")
    monkeypatch.setattr(recovery, "COLLECTION_END", "2025-01-01")
    result = collection.collect_search_denominator(
        status_path=tmp_path / "status.json",
        require_published=False,
        session=SingleDateInexactSession(),
        store_root=tmp_path / "private",
    )
    assert result["state"] == "BLOCKED_INEXACT_SINGLE_DATE"
    assert result["blocked_single_date"]["date"] == "2025-01-01"
    assert result["pagination_complete"] is False
    assert result["filing_hit_count"] is None
    assert result["private_hit_index"] is None
    assert result["matched_document_access_performed"] is False


def test_split_window_is_inclusive_oldest_first_and_rejects_one_day():
    assert collection._split_window("2025-01-01", "2025-01-04") == (
        ("2025-01-01", "2025-01-02"),
        ("2025-01-03", "2025-01-04"),
    )
    with pytest.raises(collection.AsrCapacityRecoveryCollectionError):
        collection._split_window("2025-01-01", "2025-01-01")

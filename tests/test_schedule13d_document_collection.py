from __future__ import annotations

import json
from pathlib import Path

import pytest

import schedule13d_document_collection as collection
import schedule13d_document_collection_inspection as inspection


def test_document_graph_freezes_all_indexed_accessions_without_outcomes():
    graph = collection.build_request_graph()
    requests = graph["request_contract"]["requests"]
    assert len(requests) == 6852
    assert [row["ordinal"] for row in requests] == list(range(6852))
    assert len({row["url"] for row in requests}) == 6852
    assert all(row["url"].endswith(".txt") for row in requests)
    assert graph["filing_count"] == 6852
    assert graph["verified_event_count"] is None
    assert graph["access_contract"]["filing_semantic_classification_permitted"] is False
    assert graph["access_contract"]["issuer_symbol_supplemental_access_permitted"] is False
    assert graph["access_contract"]["market_price_access_permitted"] is False
    assert graph["access_contract"]["stage0_outcome_access_permitted"] is False
    assert graph["returns_computed"] == 0
    assert graph["market_outcomes_accessed"] is False


def test_document_graph_inspection_opens_only_exact_complete_submissions(
    tmp_path: Path,
):
    path, graph = collection.freeze_request_graph(
        root=tmp_path / "manifests", status_path=tmp_path / "status.json"
    )
    result = inspection.inspect_request_graph(
        path, status_path=tmp_path / "status.json"
    )
    assert result["request_graph_sha256"] == graph["request_graph_sha256"]
    assert result["provider_access_permitted"] is True
    assert result["request_count"] == 6852
    assert result["filing_semantic_classification_permitted"] is False
    assert result["verified_event_count"] is None
    assert result["market_price_access_permitted"] is False
    assert result["outcome_access_permitted"] is False


def test_document_graph_inspection_rejects_tampering(tmp_path: Path):
    path, graph = collection.freeze_request_graph(
        root=tmp_path / "manifests", status_path=tmp_path / "status.json"
    )
    graph["request_contract"]["requests"].pop()
    path.write_text(json.dumps(graph, sort_keys=True), encoding="utf-8")
    with pytest.raises(collection.Schedule13dDocumentCollectionError):
        inspection.inspect_request_graph(path, status_path=tmp_path / "status.json")


def test_document_request_rejects_non_accession_path():
    with pytest.raises(collection.Schedule13dDocumentCollectionError):
        collection._request(
            {
                "year": 2022,
                "quarter": 1,
                "cik": "1",
                "filed_on": "2022-01-03",
                "filename": "../outside.txt",
            },
            0,
        )

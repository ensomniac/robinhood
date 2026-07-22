from __future__ import annotations

import json
from pathlib import Path

import pytest

import schedule13d_index_collection as collection
import schedule13d_index_collection_inspection as inspection


def test_request_graph_freezes_exact_16_quarters_without_counts_or_outcomes():
    graph = collection.build_request_graph()
    requests = graph["request_contract"]["requests"]
    assert [(row["year"], row["quarter"]) for row in requests] == [
        (year, quarter) for year in range(2022, 2026) for quarter in range(1, 5)
    ]
    assert len({row["url"] for row in requests}) == 16
    assert requests[0]["url"].endswith("/2022/QTR1/master.idx")
    assert requests[-1]["url"].endswith("/2025/QTR4/master.idx")
    assert graph["filing_count"] is None
    assert graph["verified_event_count"] is None
    assert graph["access_contract"]["accession_document_access_permitted"] is False
    assert graph["access_contract"]["market_price_access_permitted"] is False
    assert graph["access_contract"]["stage0_outcome_access_permitted"] is False
    assert graph["returns_computed"] == 0
    assert graph["market_outcomes_accessed"] is False


def test_request_graph_inspection_opens_only_the_exact_quarterly_indexes(
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
    assert result["quarter_request_count"] == 16
    assert result["accession_document_access_permitted"] is False
    assert result["filing_count"] is None
    assert result["market_price_access_permitted"] is False
    assert result["outcome_access_permitted"] is False


def test_request_graph_inspection_rejects_tampering(tmp_path: Path):
    path, graph = collection.freeze_request_graph(
        root=tmp_path / "manifests", status_path=tmp_path / "status.json"
    )
    graph["request_contract"]["requests"].pop()
    path.write_text(json.dumps(graph, sort_keys=True), encoding="utf-8")
    with pytest.raises(collection.Schedule13dIndexCollectionError):
        inspection.inspect_request_graph(path, status_path=tmp_path / "status.json")


def test_master_index_parser_keeps_exact_initial_13d_and_boundary_rows():
    request = collection._request(2022, 1)
    text = """Description: Master Index\nCIK|Company Name|Form Type|Date Filed|Filename\n--------------------------------------------------------\n1|Issuer One|SC 13D|2022-01-02|edgar/data/1/one.txt\n2|Issuer Two|SC 13D/A|2022-01-03|edgar/data/2/two.txt\n3|Issuer Three|SC 13D|2022-01-03|edgar/data/3/three.txt\n4|Issuer Four|SC 13G|2022-01-04|edgar/data/4/four.txt\n"""
    parsed = collection._parse_master_index(text, request)
    assert parsed["all_index_rows"] == 4
    assert [row["filename"] for row in parsed["initial_sc13d_rows"]] == [
        "edgar/data/1/one.txt",
        "edgar/data/3/three.txt",
    ]
    assert all(
        row["classification_status"] == "PENDING_ACCESSION_DOCUMENT"
        and row["terminal_reason"] is None
        for row in parsed["initial_sc13d_rows"]
    )


def test_private_index_preserves_complete_sc13d_denominator():
    graph = collection.build_request_graph()
    request = graph["request_contract"]["requests"][0]
    row = {
        "year": 2022,
        "quarter": 1,
        "cik": "1",
        "company": "Issuer One",
        "form": "SC 13D",
        "filed_on": "2022-01-03",
        "filename": "edgar/data/1/one.txt",
        "complete_submission_url": (
            "https://www.sec.gov/Archives/edgar/data/1/one.txt"
        ),
        "classification_status": "PENDING_ACCESSION_DOCUMENT",
        "terminal_reason": None,
    }
    private = collection._build_private_index(
        graph=graph,
        quarter_results=[
            {
                "year": 2022,
                "quarter": 1,
                "request_sha256": request["request_sha256"],
                "source_origin": "SEC_DOWNLOAD",
                "source_bytes": 100,
                "source_sha256": "a" * 64,
                "all_index_rows": 10,
                "initial_sc13d_rows": [row],
            }
        ],
    )
    assert private["indexed_initial_sc13d_count"] == 1
    assert private["filed_date_window_provisional_count"] == 1
    assert private["acceptance_time_classified_count"] == 0
    assert private["terminal_reason_count"] == 0
    assert private["market_price_values_accessed"] == 0
    assert private["returns_computed"] == 0
    assert private["market_outcomes_accessed"] is False

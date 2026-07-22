from __future__ import annotations

import json
from pathlib import Path

import pytest

import schedule13d_symbol_submissions as submissions
import schedule13d_symbol_submissions_inspection as inspection


def test_submissions_graph_freezes_all_pending_subject_ciks_without_outcomes():
    graph = submissions.build_request_graph()
    requests = graph["request_contract"]["requests"]
    assert len(requests) == 312
    assert sum(row["event_count"] for row in requests) == 317
    assert [row["ordinal"] for row in requests] == list(range(312))
    assert len({row["subject_cik"] for row in requests}) == 312
    assert all(row["url"].startswith("https://data.sec.gov/submissions/CIK") for row in requests)
    assert graph["verified_event_count"] == 1
    assert graph["capacity_passed"] is None
    assert graph["access_contract"]["supplemental_submission_file_access_permitted"] is False
    assert graph["access_contract"]["issuer_primary_document_access_permitted"] is False
    assert graph["access_contract"]["market_price_access_permitted"] is False
    assert graph["market_outcomes_accessed"] is False


def test_submissions_graph_inspection_opens_only_exact_main_metadata(tmp_path: Path):
    path, graph = submissions.freeze_request_graph(
        root=tmp_path / "manifests", status_path=tmp_path / "status.json"
    )
    result = inspection.inspect_request_graph(
        path, status_path=tmp_path / "status.json"
    )
    assert result["request_graph_sha256"] == graph["request_graph_sha256"]
    assert result["provider_access_permitted"] is True
    assert result["request_count"] == 312
    assert result["pending_event_count"] == 317
    assert result["supplemental_submission_file_access_permitted"] is False
    assert result["issuer_primary_document_access_permitted"] is False
    assert result["capacity_passed"] is None
    assert result["outcome_access_permitted"] is False


def test_submissions_graph_inspection_rejects_tampering(tmp_path: Path):
    path, graph = submissions.freeze_request_graph(
        root=tmp_path / "manifests", status_path=tmp_path / "status.json"
    )
    graph["request_contract"]["requests"].pop()
    path.write_text(json.dumps(graph, sort_keys=True), encoding="utf-8")
    with pytest.raises(submissions.Schedule13dSymbolSubmissionsError):
        inspection.inspect_request_graph(path, status_path=tmp_path / "status.json")

from __future__ import annotations

import json
from pathlib import Path

import pytest

import schedule13d_symbol_supplemental as supplemental
import schedule13d_symbol_supplemental_inspection as inspection


def test_supplemental_graph_freezes_only_required_historical_metadata():
    graph = supplemental.build_request_graph()
    request = graph["request_contract"]
    assert request["pending_event_count"] == 317
    assert request["events_with_prior_in_main_metadata"] == 279
    assert request["events_without_prior_and_without_historical_descriptor"] == 36
    assert request["events_requiring_historical_metadata"] == 2
    assert request["request_count"] == 3
    assert len(request["requests"]) == 3
    assert graph["verified_event_count"] == 1
    assert graph["capacity_passed"] is None
    assert graph["access_contract"]["issuer_primary_document_access_permitted"] is False
    assert graph["access_contract"]["market_price_access_permitted"] is False
    assert graph["market_outcomes_accessed"] is False


def test_supplemental_graph_inspection_opens_only_three_exact_files(tmp_path: Path):
    path, graph = supplemental.freeze_request_graph(
        root=tmp_path / "manifests", status_path=tmp_path / "status.json"
    )
    result = inspection.inspect_graph(path, status_path=tmp_path / "status.json")
    assert result["request_graph_sha256"] == graph["request_graph_sha256"]
    assert result["provider_access_permitted"] is True
    assert result["request_count"] == 3
    assert result["issuer_primary_document_access_permitted"] is False
    assert result["capacity_passed"] is None
    assert result["outcome_access_permitted"] is False


def test_supplemental_graph_inspection_rejects_tampering(tmp_path: Path):
    path, graph = supplemental.freeze_request_graph(
        root=tmp_path / "manifests", status_path=tmp_path / "status.json"
    )
    graph["request_contract"]["requests"].pop()
    path.write_text(json.dumps(graph, sort_keys=True), encoding="utf-8")
    with pytest.raises(supplemental.Schedule13dSymbolSupplementalError):
        inspection.inspect_graph(path, status_path=tmp_path / "status.json")

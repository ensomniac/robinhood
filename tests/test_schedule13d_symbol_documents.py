from __future__ import annotations

import json
from pathlib import Path

import pytest

import schedule13d_symbol_documents as documents
import schedule13d_symbol_documents_inspection as inspection


def test_document_graph_freezes_corrected_latest_prior_filings_without_symbols():
    graph = documents.build_request_graph()
    request = graph["request_contract"]
    assert request["pending_event_count"] == 317
    assert request["causal_prior_filing_count"] == 281
    assert request["no_causal_prior_filing_count"] == 36
    assert request["request_count"] == 281
    assert request["form_counts"] == {
        "10-K": 7,
        "10-Q": 33,
        "20-F": 6,
        "6-K": 26,
        "8-K": 209,
    }
    assert graph["timing_audit_sha256"] == documents.TIMING_AUDIT_SHA256
    assert graph["verified_event_count"] == 1
    assert graph["capacity_passed"] is None
    assert graph["access_contract"]["market_price_access_permitted"] is False
    assert graph["market_outcomes_accessed"] is False


def test_document_graph_inspection_opens_only_exact_issuer_documents(tmp_path: Path):
    path, graph = documents.freeze_request_graph(
        root=tmp_path / "manifests", status_path=tmp_path / "status.json"
    )
    result = inspection.inspect_graph(path, status_path=tmp_path / "status.json")
    assert result["request_graph_sha256"] == graph["request_graph_sha256"]
    assert result["provider_access_permitted"] is True
    assert result["request_count"] == 281
    assert result["symbol_resolution_permitted"] is False
    assert result["capacity_passed"] is None
    assert result["outcome_access_permitted"] is False


def test_document_graph_inspection_rejects_tampering(tmp_path: Path):
    path, graph = documents.freeze_request_graph(
        root=tmp_path / "manifests", status_path=tmp_path / "status.json"
    )
    graph["request_contract"]["requests"].pop()
    path.write_text(json.dumps(graph, sort_keys=True), encoding="utf-8")
    with pytest.raises(documents.Schedule13dSymbolDocumentsError):
        inspection.inspect_graph(path, status_path=tmp_path / "status.json")


def test_dei_symbol_parser_requires_one_unique_normalized_symbol():
    one = b'<ix:nonNumeric name="dei:TradingSymbol"> tst </ix:nonNumeric>'
    duplicate = one + b'<dei:TradingSymbol>TST</dei:TradingSymbol>'
    multiple = one + b'<dei:TradingSymbol>ALT</dei:TradingSymbol>'
    assert documents._normalized_symbols(one) == ["TST"]
    assert documents._normalized_symbols(duplicate) == ["TST"]
    assert documents._normalized_symbols(multiple) == ["ALT", "TST"]

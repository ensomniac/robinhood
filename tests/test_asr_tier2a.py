from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

import asr_tier2a as tier2a


QUALIFIED_DOCUMENT = b"""
On January 2, 2025, we entered into an accelerated share repurchase
agreement to repurchase $500 million of our common stock. The bank made
an initial delivery of shares and final settlement will occur after the
valuation period based on the average price.
"""


def test_private_graph_rebuilds_exact_item_101_tier_without_outcomes():
    graph, path = tier2a.build_private_graph()
    assert graph["selected_hit_count"] == 1_823
    assert graph["selected_accession_count"] == 1_273
    assert graph["candidate_url_count"] == 1_830
    assert len(graph["requests"]) == 1_823
    assert graph["market_outcomes_accessed"] is False
    assert graph["broker_actions"] == 0
    assert path.name == f"{graph['private_graph_sha256']}.json.gz"


def test_contract_stages_documents_before_complete_submissions_and_prices():
    contract, graph, _path = tier2a.build_contract()
    assert graph["selected_hit_count"] == 1_823
    assert contract["selection_contract"]["required_item"] == "1.01"
    assert contract["staged_contract"]["collect_exact_matched_documents_first"] is True
    assert (
        contract["staged_contract"][
            "complete_submission_requests_before_document_semantics"
        ]
        is False
    )
    assert contract["staged_contract"][
        "remaining_items_if_combined_candidates_below_100"
    ] == ["7.01", "8.01"]
    assert contract["access_contract"]["complete_submission_access_permitted"] is False
    assert contract["access_contract"]["market_price_access_permitted"] is False
    assert contract["verified_event_count"] is None
    assert contract["market_outcomes_accessed"] is False


def test_matched_document_url_rejects_path_substitution():
    with pytest.raises(tier2a.AsrTier2aError):
        tier2a._document_url(
            "1234", "0000001234-25-000001", "../different-document.htm"
        )


def test_semantic_rebuild_reclassifies_sources_and_preserves_failure(
    tmp_path: Path,
):
    source = tmp_path / "qualified.htm"
    source.write_bytes(QUALIFIED_DOCUMENT)
    private = {
        "records": [
            {
                "ordinal": 0,
                "hit_id": "0000001234-25-000001:qualified.htm",
                "accession": "0000001234-25-000001",
                "file_date": "2025-01-03",
                "selected_cik": "1234",
                "source_cache_relative_path": "qualified.htm",
                "source_sha256": hashlib.sha256(QUALIFIED_DOCUMENT).hexdigest(),
                "source_bytes": len(QUALIFIED_DOCUMENT),
            }
        ],
        "failures": [
            {
                "ordinal": 1,
                "hit_id": "0000001234-25-000002:missing.htm",
            }
        ],
    }
    result = tier2a.rebuild_semantic_result(private, tmp_path)
    assert result["document_semantic_candidate_count"] == 1
    assert result["qualified_accession_count"] == 1
    assert result["terminal_counts"] == {
        "QUALIFIED_EVENT_CANDIDATE": 1,
        "SOURCE_FAILURE_UNRESOLVED_ZERO_CREDIT": 1,
    }
    assert result["verified_event_count"] is None
    assert result["market_outcomes_accessed"] is False

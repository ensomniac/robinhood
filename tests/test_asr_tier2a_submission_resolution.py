from __future__ import annotations

import hashlib
from pathlib import Path

import asr_tier2a_submission_resolution as resolution


QUALIFIED_SUBMISSION = b"""
<SEC-DOCUMENT>
<ACCEPTANCE-DATETIME>20250103120000
On January 2, 2025, we entered into an accelerated share repurchase
agreement to repurchase $500 million of our common stock. The bank made
an initial delivery of shares and final settlement will occur after the
valuation period based on the average price.
"""


def test_graph_binds_only_inspected_qualified_accessions():
    graph, path = resolution.build_private_graph()
    assert graph["request_count"] == 252
    assert graph["candidate_url_count"] == 252
    assert len(graph["requests"]) == 252
    assert all(len(row["candidates"]) == 1 for row in graph["requests"])
    assert graph["market_outcomes_accessed"] is False
    assert graph["broker_actions"] == 0
    assert path.name == f"{graph['private_graph_sha256']}.json.gz"


def test_contract_preserves_identity_and_market_locks():
    contract, graph, _path = resolution.build_contract()
    assert graph["request_count"] == 252
    assert contract["source_lineage"]["document_semantic_candidate_count"] == 508
    assert (
        contract["classification_contract"][
            "acceptance_datetime_from_complete_submission"
        ]
        is True
    )
    assert (
        contract["classification_contract"][
            "security_identity_still_separately_required"
        ]
        is True
    )
    assert contract["access_contract"]["security_identity_access_permitted"] is False
    assert contract["access_contract"]["market_price_access_permitted"] is False
    assert contract["verified_event_count"] is None
    assert contract["market_outcomes_accessed"] is False


def test_precise_rebuild_uses_exact_acceptance_and_preserves_failure(
    tmp_path: Path,
):
    source = tmp_path / "submission.txt"
    source.write_bytes(QUALIFIED_SUBMISSION)
    private = {
        "records": [
            {
                "ordinal": 0,
                "accession": "0000001234-25-000001",
                "selected_cik": "1234",
                "acceptance_datetime_raw": "20250103120000",
                "source_cache_relative_path": "submission.txt",
                "source_sha256": hashlib.sha256(QUALIFIED_SUBMISSION).hexdigest(),
                "source_bytes": len(QUALIFIED_SUBMISSION),
            }
        ],
        "failures": [
            {
                "ordinal": 1,
                "accession": "0000001234-25-000002",
            }
        ],
    }
    result = resolution.rebuild_result(private, tmp_path)
    assert result["precise_semantic_event_count"] == 1
    assert result["events"][0]["acceptance_datetime_raw"] == "20250103120000"
    assert result["terminal_counts"] == {
        "QUALIFIED_EVENT_CANDIDATE": 1,
        "SOURCE_FAILURE_UNRESOLVED_ZERO_CREDIT": 1,
    }
    assert result["security_identity_resolution_complete"] is False
    assert result["verified_event_count"] is None
    assert result["market_outcomes_accessed"] is False

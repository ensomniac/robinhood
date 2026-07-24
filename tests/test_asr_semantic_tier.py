from __future__ import annotations

import asr_semantic_tier as semantic


QUALIFIED = b"""
<SEC-DOCUMENT>
<ACCEPTANCE-DATETIME>20250103120000
<p>On January 2, 2025, we entered into an accelerated share repurchase
agreement to repurchase $500 million of our common stock. The bank made
an initial delivery of shares and final settlement will occur after the
valuation period based on the average price.</p>
"""


def test_same_window_classifier_extracts_and_deduplicates_event():
    events, terminal = semantic.classify_submission(
        QUALIFIED,
        acceptance_datetime="20250103120000",
        cik="1234",
        accession="0000001234-25-000001",
    )
    assert terminal == "QUALIFIED_EVENT_CANDIDATE"
    assert events == [
        {
            "issuer_cik": "1234",
            "agreement_date": "2025-01-02",
            "committed_notional_dollars": 500_000_000,
            "acceptance_datetime_raw": "20250103120000",
            "accession": "0000001234-25-000001",
            "event_key_sha256": events[0]["event_key_sha256"],
        }
    ]


def test_missing_continuing_mechanics_is_zero_credit():
    raw = b"""
    <SEC-DOCUMENT>
    On January 2, 2025, we entered into an accelerated share repurchase
    agreement to repurchase $500 million of common stock.
    """
    events, terminal = semantic.classify_submission(
        raw,
        acceptance_datetime="20250103120000",
        cik="1234",
        accession="0000001234-25-000001",
    )
    assert events == []
    assert terminal == "NO_CONTINUING_MECHANICS_SAME_WINDOW"


def test_future_agreement_date_is_ineligible():
    raw = QUALIFIED.replace(b"January 2, 2025", b"January 4, 2025")
    events, terminal = semantic.classify_submission(
        raw,
        acceptance_datetime="20250103120000",
        cik="1234",
        accession="0000001234-25-000001",
    )
    assert events == []
    assert terminal == "AGREEMENT_DATE_AFTER_ACCEPTANCE"


def test_contract_preserves_failures_and_market_lock():
    contract = semantic.build_contract()
    assert contract["source_lineage"]["inspected_success_count"] == 196
    assert contract["source_lineage"]["source_failure_count"] == 5
    assert contract["source_lineage"]["unselected_unique_hit_count"] == 17_278
    assert contract["classification_contract"]["all_positive_groups_same_window"] is True
    assert contract["capacity_contract"][
        "formal_verified_event_count_requires_security_identity"
    ] is True
    assert contract["access_contract"]["new_provider_access_permitted"] is False
    assert contract["access_contract"]["market_price_access_permitted"] is False
    assert contract["verified_event_count"] is None
    assert contract["market_outcomes_accessed"] is False

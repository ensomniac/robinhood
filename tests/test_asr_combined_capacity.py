from __future__ import annotations

import asr_combined_capacity as combined


def test_combined_capacity_counts_independent_disclosures_not_agreements():
    result = combined.build_result()
    assert result["source_tiers"]["accession_sets_disjoint"] is True
    assert result["verified_agreement_count"] == 185
    assert result["independent_disclosure_signal_count"] == 89
    assert result["capacity_counting_rule"].startswith(
        "one accession, ticker, and exact acceptance timestamp"
    )
    assert result["capacity_disposition"] == "PRESERVED_LATER_SINGLE_RULE_RESEARCH"
    assert result["development_search_contract_freeze_permitted"] is False
    assert result["market_price_access_permitted"] is False
    assert result["market_outcomes_accessed"] is False

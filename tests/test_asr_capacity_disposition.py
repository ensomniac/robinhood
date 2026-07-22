from __future__ import annotations

import asr_capacity_disposition as disposition


def test_inexact_denominator_retires_only_the_exact_source_contract():
    result = disposition.build_disposition()
    assert result["disposition"] == "RETIRED_INSUFFICIENT_SOURCE_COMPLETENESS"
    assert result["count_based_capacity_disposition"] is None
    assert result["verified_event_count"] is None
    assert result["first_pilot_fast_lane_eligible"] is False
    assert result["same_contract_query_or_partition_repair_permitted"] is False
    assert result["same_contract_matched_document_access_permitted"] is False
    assert result["future_new_version_requires_new_preoutcome_authorization"] is True
    assert result["market_outcomes_accessed"] is False
    assert result["broker_actions"] == 0
    assert result["maturity_effect"] == "RETIRED_NOT_PILOT_READY"

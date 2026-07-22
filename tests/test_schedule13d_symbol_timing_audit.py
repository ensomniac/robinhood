from __future__ import annotations

from datetime import UTC

import schedule13d_symbol_timing_audit as audit


def test_corrected_acceptance_partition_preserves_exact_source_url_set():
    value = audit.build_audit()
    assert value["superseded_partition"] == {
        "events_with_prior_in_main_metadata": 279,
        "events_without_prior_and_without_historical_descriptor": 36,
        "events_requiring_historical_metadata": 2,
        "valid": False,
    }
    assert value["corrected_partition"] == {
        "events_with_prior_in_main_metadata": 280,
        "events_without_prior_and_without_historical_descriptor": 35,
        "events_requiring_historical_metadata": 2,
        "lexical_comparison_mismatches": 1,
        "valid": True,
    }
    assert value["source_scope"]["url_sets_identical"] is True
    assert value["source_scope"]["new_provider_requests_required"] == 0
    assert value["corrected_primary_document_freeze_permitted"] is True
    assert value["market_outcomes_accessed"] is False


def test_complete_submission_acceptance_is_converted_from_eastern_to_utc():
    winter = audit._cutoff_utc("2022-01-03T16:17:04")
    summer = audit._cutoff_utc("2022-07-01T16:17:04")
    assert winter.tzinfo == UTC
    assert winter.hour == 21
    assert summer.hour == 20

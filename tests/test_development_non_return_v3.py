from __future__ import annotations

import copy
from unittest.mock import patch

import pytest

import development_non_return_v3 as non_return


def _reviewed_inputs() -> tuple[dict, dict]:
    prior = {
        "dataset_id": non_return.accession_recovery.SOURCE_REVIEW_DATASET_ID,
        "status": "REVIEW_COMPLETE",
        "verified_positive_pairs": 2,
        "pair_dispositions": {
            "pair-a": "VERIFIED_POSITIVE_PRIMARY",
            "pair-b": "VERIFIED_POSITIVE_PRIMARY",
            "pair-c": "DOCUMENT_SEMANTICS_UNRESOLVED",
            "pair-d": "DOCUMENT_SEMANTICS_UNRESOLVED",
            "pair-e": "VERIFIED_CONFLICT",
        },
        "target_outcomes_observed_or_derived": False,
    }
    recovered = {
        "dataset_id": non_return.accession_recovery.DATASET_ID,
        "status": "REVIEW_COMPLETE",
        "recovered_verified_positive_pairs": 1,
        "combined_verified_positive_pairs": 3,
        "pair_dispositions": {
            "pair-c": "VERIFIED_POSITIVE_PRIMARY",
            "pair-d": "VERIFIED_NEGATIVE_PRIMARY",
        },
        "target_outcomes_observed_or_derived": False,
    }
    return prior, recovered


@patch.multiple(
    non_return,
    EXPECTED_SOURCE_PAIRS=5,
    EXPECTED_PRIOR_POSITIVES=2,
    EXPECTED_RECOVERY_PAIRS=2,
    EXPECTED_RECOVERED_POSITIVES=1,
    EXPECTED_COMBINED_POSITIVES=3,
)
def test_combined_positive_decisions_are_exact_and_disjoint() -> None:
    prior, recovered = _reviewed_inputs()
    combined = non_return.build_combined_pair_dispositions(prior, recovered)
    assert {
        key for key, value in combined.items()
        if value == "VERIFIED_POSITIVE_PRIMARY"
    } == {"pair-a", "pair-b", "pair-c"}


@patch.multiple(
    non_return,
    EXPECTED_SOURCE_PAIRS=5,
    EXPECTED_PRIOR_POSITIVES=2,
    EXPECTED_RECOVERY_PAIRS=2,
    EXPECTED_RECOVERED_POSITIVES=1,
    EXPECTED_COMBINED_POSITIVES=3,
)
def test_recovery_cannot_overwrite_prior_resolved_pair() -> None:
    prior, recovered = _reviewed_inputs()
    recovered["pair_dispositions"] = {
        "pair-a": "VERIFIED_POSITIVE_PRIMARY",
        "pair-d": "VERIFIED_NEGATIVE_PRIMARY",
    }
    with pytest.raises(
        non_return.DevelopmentNonReturnV3Error,
        match="overwrite",
    ):
        non_return.build_combined_pair_dispositions(prior, recovered)


@patch.multiple(
    non_return,
    EXPECTED_SOURCE_PAIRS=5,
    EXPECTED_PRIOR_POSITIVES=2,
    EXPECTED_RECOVERY_PAIRS=2,
    EXPECTED_RECOVERED_POSITIVES=1,
    EXPECTED_COMBINED_POSITIVES=3,
)
def test_recovery_pair_must_stay_in_source_denominator() -> None:
    prior, recovered = _reviewed_inputs()
    recovered["pair_dispositions"] = {
        "pair-c": "VERIFIED_POSITIVE_PRIMARY",
        "pair-z": "VERIFIED_NEGATIVE_PRIMARY",
    }
    with pytest.raises(
        non_return.DevelopmentNonReturnV3Error,
        match="outside",
    ):
        non_return.build_combined_pair_dispositions(prior, recovered)


def test_request_graph_covers_full_second_tranche_calendar() -> None:
    selection = {
        "positive_pairs": [
            {
                "date": "2025-12-31",
                "symbol": "TEST",
                "instrument_id": "instrument-test",
            }
        ]
    }
    value = non_return.build_request_graph(copy.deepcopy(selection))
    assert value["graph"]["calendar_query"]["end"] == "2025-12-31"
    assert value["graph"]["split_action_query"]["execution_date_lte"] == "2025-12-31"
    assert value["request_graph_sha256"] == non_return.base._sha256_json(
        value["graph"]
    )


def test_v3_private_namespace_isolated_from_completed_v2() -> None:
    root = non_return.Path("/historical")
    assert non_return._private_root(root) == (
        root
        / "_derived/development_non_return"
        / "dataset-development-non-return-qualification-2026-07-20-tranche-v3-v1"
    )


def test_v3_private_namespace_cannot_alias_superseded_contract() -> None:
    root = non_return.Path("/historical")
    old = (
        root
        / "_derived/development_non_return"
        / "dataset-development-non-return-qualification-2026-07-20-v3"
    )
    assert non_return._private_root(root) != old

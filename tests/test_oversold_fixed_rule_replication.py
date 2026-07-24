import oversold_fixed_rule_replication as replication
from historical_store import HistoricalDayStore


def test_prior_selection_is_unique_and_carries_complete_correction() -> None:
    prior = replication._prior_statistics(
        HistoricalDayStore.from_env(), enforce_commit=False
    )

    assert prior["trial_count"] == 32
    assert prior["selected_trial_id"] == replication.SELECTED_PRIOR_TRIAL_ID
    assert prior["selected_parameters"] == replication.SELECTED_PARAMETERS
    assert len(prior["trial_sharpes"]) == 32
    assert len(prior["trial_p_values"]) == 32
    assert (
        prior["selected_combined_metrics"][
            "passes_replication_candidate_gates"
        ]
        is True
    )


def test_boundary_is_outcome_blind_and_temporally_disjoint() -> None:
    boundary, development, confirmation = replication.build_boundary(
        created_at="2026-07-24T22:15:00Z",
        enforce_commit=False,
    )

    assert len(development["evaluation_dates"]) == 118
    assert len(confirmation["evaluation_dates"]) == 66
    assert len(boundary["embargo_dates"]) == 5
    assert (
        max(boundary["prior_used_account_dates"])
        < boundary["embargo_dates"][0]
        < min(confirmation["evaluation_dates"])
    )
    assert (
        set(development["evaluation_dates"])
        .isdisjoint(confirmation["evaluation_dates"])
    )
    assert boundary["new_development_outcomes_accessed"] is False
    assert boundary["confirmation_outcomes_accessed"] is False
    assert boundary["provider_requests"] == 0
    assert boundary["broker_actions"] == 0

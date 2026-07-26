from __future__ import annotations

import outcome_exposure
import sec_broad_pead as source


def test_selection_excludes_exposed_reserve_without_substitution() -> None:
    selected = source.selection()

    assert selected["development_event_count"] == 1019
    assert len(selected["development_signal_dates"]) == 499
    assert selected["confirmation_event_count"] == 197
    assert len(selected["confirmation_signal_dates"]) == 118
    assert len(selected["confirmation_symbols"]) == 174
    assert selected["globally_exposed_confirmation_symbols"] == [
        "AGN",
        "BEAM",
        "EQIX",
        "WPX",
    ]
    assert selected["confirmation_prices_accessed"] is False
    assert selected["provider_requests"] == 0
    assert selected["broker_actions"] == 0


def test_contract_accounts_for_all_prior_trials_and_sealed_reserve() -> None:
    selected = source.selection()
    prior = source.prior_statistics()

    contract = source.build_contract(
        created_at="2026-07-26T01:00:00-04:00",
        capacity_manifest=source.SOURCE_DATASET_MANIFEST,
        selected=selected,
        prior=prior,
    )

    assert len(contract["trial_family"]) == 16
    assert contract["prior_selection_trial_count"] == 33
    assert contract["selection_accounting"]["cumulative_trial_count"] == 49
    assert contract["parameter_grid"]["minimum_yoy_eps_change_ratio"] == [
        0.0
    ]
    assert contract["parameter_grid"]["security_trend_gate"] == [
        "price>SMA100",
        "price>SMA200",
    ]
    assert contract["partitions"]["development_training_contaminated"] is True
    assert contract["confirmation_outcomes_accessed"] is False
    outcome_exposure.assert_untouched(
        contract["confirmation_scope"],
        outcome_exposure.read_index(),
    )

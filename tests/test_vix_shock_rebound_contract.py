from __future__ import annotations

import vix_shock_rebound as family


def test_vix_shock_partitions_are_dense_chronological_and_immediate():
    split = family.partitions()

    assert len(split["development_warmup_dates"]) == 200
    assert len(split["development_dates"]) == 1_000
    assert len(split["development_signal_dates"]) == 994
    assert len(split["embargo_dates"]) == 5
    assert len(split["confirmation_warmup_dates"]) == 200
    assert len(split["confirmation_dates"]) == 500
    assert len(split["confirmation_signal_dates"]) == 494
    assert (
        split["development_warmup_dates"][-1]
        < split["development_dates"][0]
        < split["embargo_dates"][0]
        < split["confirmation_dates"][0]
    )


def test_vix_shock_rule_has_one_exact_trial_and_fresh_symbols():
    grid = {key: [value] for key, value in family.PARAMETERS.items()}

    assert all(len(values) == 1 for values in grid.values())
    assert family.SYMBOLS == ["SPLV", "^VIX"]
    assert family.RESEARCH_GENERATION == "new_mechanism_family"
    assert family.MAXIMUM_HOLD_SESSIONS == 5


def test_vix_shock_frozen_scopes_are_currently_untouched():
    split = family.partitions()
    development = family._scope(
        [
            *split["development_warmup_dates"],
            *split["development_dates"],
        ]
    )
    confirmation = family._scope(
        split["confirmation_signal_dates"]
    )

    assert development["symbols"] == ["SPLV", "^VIX"]
    assert confirmation["symbols"] == ["SPLV", "^VIX"]

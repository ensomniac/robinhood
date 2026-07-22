from datetime import date, timedelta

import pytest

import challenger_orb_retest_tranche as prior
import challenger_orb_retest_tranche2 as tranche


def _calendar(count=500):
    start = date.fromisoformat("2023-01-03")
    return [(start + timedelta(days=index)).isoformat() for index in range(count)]


def test_second_selection_is_exact_disjoint_and_deterministic(monkeypatch):
    monkeypatch.setattr(tranche, "ELIGIBLE_END", "2026-12-31")
    calendar = _calendar()
    excluded = calendar[30:130]
    snapshot = {
        "excluded_dates": excluded,
        "excluded_dates_sha256": tranche._sha256_json(excluded),
    }

    first, required = tranche.build_selection(
        calendar_dates=calendar, exclusion_snapshot=snapshot
    )
    second, _ = tranche.build_selection(
        calendar_dates=calendar, exclusion_snapshot=snapshot
    )

    assert first == second
    assert first["dataset_id"] == tranche.DATASET_ID
    assert first["hypothesis_sha256"] == tranche.retest.HYPOTHESIS_SHA256
    assert len(first["selected_dates"]) == tranche.TARGET_COUNT
    assert not set(first["selected_dates"]) & set(excluded)
    assert len(required) == first["required_session_count"]
    assert first["substitution_allowed"] is False
    assert first["target_outcomes_observed_or_derived"] is False


def test_second_selection_explicitly_includes_first_challenger_tranche():
    assert prior.DEFAULT_SELECTION in tranche.PRIOR_SELECTIONS
    assert len(tranche.PRIOR_SELECTIONS) == len(prior.PRIOR_SELECTIONS) + 1


def test_second_selection_fails_when_disjoint_pool_is_too_small(monkeypatch):
    monkeypatch.setattr(tranche, "ELIGIBLE_END", "2026-12-31")
    calendar = _calendar(140)
    excluded = calendar[20:100]

    with pytest.raises(tranche.ChallengerTranche2Error, match="needs 100"):
        tranche.build_selection(
            calendar_dates=calendar,
            exclusion_snapshot={
                "excluded_dates": excluded,
                "excluded_dates_sha256": tranche._sha256_json(excluded),
            },
        )

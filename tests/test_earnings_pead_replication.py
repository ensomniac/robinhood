import earnings_pead_replication as replication
from historical_store import HistoricalDayStore


def test_replication_freezes_prior_trial_correction_and_capacity() -> None:
    store = HistoricalDayStore.from_env()
    prior = replication._prior_statistics(
        store, enforce_commit=False
    )
    selection = replication._selection(
        store, enforce_commit=False
    )

    assert prior["trial_count"] == 32
    assert (
        prior["selected_trial_id"]
        == replication.SELECTED_PRIOR_TRIAL_ID
    )
    assert len(prior["trial_sharpes"]) == 32
    assert len(prior["trial_p_values"]) == 32
    assert len(selection["development_signal_dates"]) == 123
    assert len(selection["confirmation_signal_dates"]) == 31
    assert selection["confirmation_outcomes_accessed"] is False

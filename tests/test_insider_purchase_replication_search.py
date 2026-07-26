import insider_purchase_replication_search as replication
import strategy_discovery


def test_replication_partition_is_dense_disjoint_and_immediate():
    selected = replication.selection()

    assert len(selected["development_events"]) == 424
    assert len(selected["development_signal_dates"]) == 94
    assert len(selected["confirmation_events"]) == 209
    assert len(selected["confirmation_signal_dates"]) == 46
    assert len(selected["embargo_dates"]) == 5
    assert selected["development_dates"][-1] < selected["embargo_dates"][0]
    assert selected["embargo_dates"][-1] < selected["confirmation_dates"][0]
    assert selected["contaminated_events_removed_without_replacement"] == 0


def test_replication_retains_every_prior_trial_path():
    prior = replication.prior_statistics()

    assert len(prior["prior_trial_sharpes"]) == 32
    assert len(prior["prior_trial_p_values"]) == 32
    assert len(prior["prior_trial_daily_returns_by_id"]) == 32
    assert 0 <= prior["prior_standalone_pbo_probability"] <= 1


def test_replication_contract_preserves_grid_and_cumulative_correction():
    selected = replication.selection()
    contract = replication.build_contract(
        created_at="2026-07-26T11:15:00Z",
        capacity_manifest=replication.PRIOR_CAPACITY,
        selected=selected,
    )
    parent = strategy_discovery.load_artifact(
        replication.PRIOR_SEARCH,
        expected_kind="frozen-development-search",
    )["family_contract"]

    assert contract["parameter_grid"] == parent["parameter_grid"]
    assert {
        row["trial_id"] for row in contract["trial_family"]
    } == {row["trial_id"] for row in parent["trial_family"]}
    assert contract["selection_accounting"]["cumulative_trial_count"] == 64
    assert len(contract["prior_trial_daily_returns_by_id"]) == 32
    assert contract["new_mechanism_family_slot_consumed"] is False
    assert contract["confirmation_signal_capacity"] == 46

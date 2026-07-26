from __future__ import annotations

from datetime import date

import dense_batch_readiness as readiness


def test_rolling_readiness_resolves_exact_committed_boundary():
    status = readiness.build_status(
        as_of=date(2026, 7, 23),
        require_committed=False,
        require_credentials=False,
    )

    assert status["state"] == "ALLOCATION_CONTRACT_READY"
    assert status["activation_permitted"] is True
    assert status["preactivation_work_complete"] is True
    assert status["blockers"] == []
    allocation = status["allocation_capacity"]
    assert allocation["state"] == "ALLOCATION_CAPACITY_READY"
    assert allocation["ready"] is True
    assert allocation["development_account_sessions_per_family"] == 120
    assert allocation["required_confirmation_signal_sessions_per_family"] == 35
    assert allocation["blocker"] is None
    assert allocation["allocation_semantics"] == (
        "contiguous account calendars with pair-clean confirmation "
        "signal reserves"
    )
    assert set(allocation["confirmation_account_sessions"]) == {
        "intraday-index-etf-opening-reversal",
        "liquid-equity-market-residual-reversal",
        "liquid-etf-trend-pullback-cost-floor",
    }
    assert all(
        sessions >= 35
        for sessions in allocation["confirmation_account_sessions"].values()
    )
    assert status["calendar_boundary"]["output_state"] == (
        "COLLECTED_READY_FOR_ALLOCATION_CONTRACT"
    )
    assert status["calendar_boundary"]["provider_requests_so_far"] == 1
    assert status["calendar_boundary"]["target_outcomes_accessed"] is False
    assert [item["trial_count"] for item in status["plan"]["families"]] == [
        48,
        32,
        32,
    ]
    assert status["provider_access_permitted"] is False
    assert status["target_outcome_access_permitted"] is False
    assert status["broker_actions_permitted"] is False


def test_readiness_advances_to_allocation_contract_after_collection():
    status = readiness.build_status(
        as_of=date(2026, 7, 23),
        require_committed=False,
        require_credentials=False,
    )

    assert status["state"] == "ALLOCATION_CONTRACT_READY"
    assert status["activation_permitted"] is True
    assert status["target_outcome_access_permitted"] is False
    assert status["remaining_transition_order"] == [
        "freeze and inspect the causal allocation contract",
        "freeze disjoint family evidence and exact family contracts",
        "collect development inputs only from committed exact contracts",
    ]
    assert status["next_commands"][0].startswith(
        "python3 dense_calendar_allocation.py freeze "
    )

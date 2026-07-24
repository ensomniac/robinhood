from __future__ import annotations

from datetime import date

import dense_batch_readiness as readiness


def test_rolling_readiness_resolves_exact_committed_boundary():
    status = readiness.build_status(
        as_of=date(2026, 7, 23),
        require_committed=False,
        require_credentials=False,
    )

    assert status["state"] == "PREACTIVATION_BLOCKED"
    assert status["activation_permitted"] is True
    assert status["preactivation_work_complete"] is False
    assert status["blockers"] == [
        "no contiguous untouched target-evidence run has 480 sessions"
    ]
    assert status["allocation_capacity"] == {
        "blocker": (
            "no contiguous untouched target-evidence run has 480 sessions"
        ),
        "largest_contiguous_untouched_run": 28,
        "ready": False,
        "required_contiguous_target_sessions": 480,
        "state": "INSUFFICIENT_GLOBAL_UNTOUCHED_CAPACITY",
        "total_untouched_sessions": 285,
    }
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

    assert status["state"] == "PREACTIVATION_BLOCKED"
    assert status["activation_permitted"] is True
    assert status["target_outcome_access_permitted"] is False
    assert status["remaining_transition_order"] == [
        "preserve the insufficient-capacity disposition without target outcomes",
        "continue already-authorized existing-family replication",
    ]
    assert status["next_commands"] == [
        "python3 oversold_replication_discovery.py status"
    ]

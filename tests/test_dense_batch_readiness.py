from __future__ import annotations

from datetime import date

import dense_batch_readiness as readiness


def test_rolling_readiness_resolves_exact_committed_boundary():
    status = readiness.build_status(
        as_of=date(2026, 7, 23),
        require_committed=False,
        require_credentials=False,
    )

    assert status["state"] == "ACTIVATION_READY"
    assert status["activation_permitted"] is True
    assert status["preactivation_work_complete"] is True
    assert status["blockers"] == []
    assert status["calendar_boundary"]["output_state"] == (
        "ABSENT_READY_FOR_SINGLE_COLLECTION"
    )
    assert status["calendar_boundary"]["provider_requests_so_far"] == 0
    assert status["calendar_boundary"]["target_outcomes_accessed"] is False
    assert [item["trial_count"] for item in status["plan"]["families"]] == [
        48,
        32,
        32,
    ]
    assert status["provider_access_permitted"] is True
    assert status["target_outcome_access_permitted"] is False
    assert status["broker_actions_permitted"] is False


def test_readiness_opens_only_calendar_provider_step_after_authorization():
    status = readiness.build_status(
        as_of=date(2026, 7, 23),
        require_committed=False,
        require_credentials=False,
    )

    assert status["state"] == "ACTIVATION_READY"
    assert status["activation_permitted"] is True
    assert status["target_outcome_access_permitted"] is False
    assert status["next_commands"][0].startswith(
        "python3 dense_session_calendar.py collect "
    )

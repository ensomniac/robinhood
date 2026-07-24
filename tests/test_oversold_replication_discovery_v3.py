from __future__ import annotations

import oversold_replication_discovery_v3 as discovery


def test_configured_restores_v2_identity() -> None:
    before = (discovery.v2.SUCCESSOR_ID, discovery.v2.EXPERIMENT_ID)
    with discovery.configured():
        assert discovery.v2.SUCCESSOR_ID == discovery.SUCCESSOR_ID
        assert discovery.v2.EXPERIMENT_ID == discovery.EXPERIMENT_ID
    assert (discovery.v2.SUCCESSOR_ID, discovery.v2.EXPERIMENT_ID) == before


def test_v5_failure_records_pre_result_boundary(tmp_path) -> None:
    path, failure = discovery.record_v5_failure(
        recorded_at="2026-07-24T19:25:00Z",
        root=tmp_path,
    )

    assert path.exists()
    assert failure["state"] == "FAILED_EMPTY_CANDIDATE_UNIVERSE_BOUNDARY"
    assert failure["trials_returned"] == 0
    assert failure["trial_metrics_surfaced"] is False
    assert failure["selection_executed"] is False
    assert failure["confirmation_accessed"] is False
    assert failure["provider_requests"] == 0
    assert failure["runtime_before_sha256"] != failure["runtime_after_sha256"]

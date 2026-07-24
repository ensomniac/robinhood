from __future__ import annotations

import earnings_gap_continuation_v2 as continuation


def test_event_contract_freezes_completed_metadata_only_windows() -> None:
    contract = continuation.build_event_contract(
        created_at="2026-07-24T19:45:00Z",
        enforce_commit=False,
    )

    assert contract["event_end"] == "2026-07-23"
    assert contract["requests"] == continuation._requests()
    assert contract["logical_provider_requests"] == 7
    assert contract["market_prices_accessed"] is False
    assert contract["forward_returns_accessed"] is False
    assert contract["strategy_metrics_computed"] == 0
    assert contract["broker_actions"] == 0


def test_base_event_scope_restores_normalizer_boundary() -> None:
    before = (
        continuation.base.EVENT_START,
        continuation.base.EVENT_END,
        continuation.base.PRIOR_DISCARDED_PROVIDER_REQUESTS,
    )
    with continuation._base_event_scope():
        assert continuation.base.EVENT_START == continuation.EVENT_START
        assert continuation.base.EVENT_END == continuation.EVENT_END
        assert continuation.base.PRIOR_DISCARDED_PROVIDER_REQUESTS == 0
    assert (
        continuation.base.EVENT_START,
        continuation.base.EVENT_END,
        continuation.base.PRIOR_DISCARDED_PROVIDER_REQUESTS,
    ) == before


def test_transport_failure_records_no_artifact_boundary(tmp_path) -> None:
    path, failure = continuation.record_transport_failure(
        recorded_at="2026-07-24T19:50:00Z",
        root=tmp_path,
    )

    assert path.exists()
    assert (
        failure["state"]
        == "EVENT_METADATA_TRANSPORT_FAILED_NO_ARTIFACT"
    )
    assert failure["provider_requests"] == 7
    assert failure["responses_retained"] == 0
    assert failure["private_artifact_written"] is False
    assert failure["public_collection_artifact_written"] is False
    assert failure["market_prices_accessed"] is False
    assert failure["forward_returns_accessed"] is False
    assert failure["strategy_metrics_computed"] == 0
    assert failure["broker_actions"] == 0


def test_artifact_sha_reports_the_artifacts_own_hash() -> None:
    assert (
        continuation._artifact_sha(
            {
                "artifact_kind": "earnings-gap-v2-event-retry-contract",
                "contract_sha256": "contract",
                "failure_sha256": "embedded-failure",
            }
        )
        == "contract"
    )


def test_retry_failure_exhausts_provider_access(tmp_path) -> None:
    path, failure = continuation.record_retry_failure(
        recorded_at="2026-07-24T20:20:00Z",
        root=tmp_path,
    )

    assert path.exists()
    assert (
        failure["state"]
        == "EVENT_METADATA_RETRY_FAILED_DUPLICATES_NO_ARTIFACT"
    )
    assert failure["provider_requests_total"] == 14
    assert failure["provider_retry_permitted"] is False
    assert failure["responses_retained"] == 0
    assert failure["market_prices_accessed"] is False
    assert failure["forward_returns_accessed"] is False
    assert failure["broker_actions"] == 0

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

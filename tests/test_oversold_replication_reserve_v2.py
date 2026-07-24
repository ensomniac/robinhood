from __future__ import annotations

import oversold_replication_reserve_v2 as reserve


def test_completed_reserve_is_historical_and_globally_untouched(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        reserve.strategy_discovery,
        "require_committed",
        lambda _path: None,
    )
    value = reserve._selection_content(
        created_at="2026-07-24T11:27:00-04:00"
    )

    assert value["development_last_date"] == "2024-12-24"
    assert value["embargo_dates"] == [
        "2024-12-26",
        "2024-12-27",
        "2024-12-30",
        "2024-12-31",
        "2025-01-02",
    ]
    assert value["confirmation_dates"] == [
        "2025-12-11",
        "2025-12-16",
        "2025-12-17",
        "2025-12-18",
        "2025-12-19",
        "2025-12-22",
        "2025-12-23",
        "2025-12-26",
        "2025-12-29",
        "2025-12-30",
        "2025-12-31",
        "2026-01-02",
        "2026-01-05",
        "2026-01-06",
        "2026-01-07",
        "2026-01-12",
        "2026-01-20",
        "2026-02-18",
        "2026-02-26",
        "2026-03-16",
        "2026-03-27",
        "2026-04-02",
        "2026-04-10",
        "2026-04-13",
        "2026-04-14",
        "2026-04-17",
        "2026-04-20",
        "2026-05-19",
        "2026-05-26",
        "2026-05-27",
        "2026-06-09",
        "2026-06-16",
        "2026-06-17",
        "2026-06-18",
        "2026-06-23",
        "2026-06-29",
        "2026-07-13",
        "2026-07-15",
        "2026-07-16",
        "2026-07-17",
    ]
    assert value["selection_cutoff"] == "2026-07-17"
    assert value["predecessor_failure"]["overlap_count"] > 0
    assert value["predecessor_failure"][
        "full_session_prices_accessed"
    ] is False
    assert not (
        set(value["confirmation_dates"])
        & reserve.dense_capacity_inventory._globally_exposed_dates(
            reserve.outcome_exposure.read_index()
        )
    )


def test_hashed_selection_rejects_drift(tmp_path) -> None:
    content = {
        "artifact_kind": (
            "oversold_replication_completed_reserve_selection"
        ),
        "state": "COMPLETED_RESERVE_SELECTED_AWAITING_INSPECTION",
    }
    identity = reserve._hash(content)
    path = tmp_path / f"selection-{identity}.json"
    reserve._write(
        path,
        {**content, "selection_sha256": identity},
    )
    assert reserve._load_hashed(
        path,
        identity_field="selection_sha256",
        expected_kind=content["artifact_kind"],
    )["selection_sha256"] == identity

    reserve._write(
        path,
        {
            **content,
            "state": "DRIFTED",
            "selection_sha256": identity,
        },
    )
    try:
        reserve._load_hashed(
            path,
            identity_field="selection_sha256",
            expected_kind=content["artifact_kind"],
        )
    except reserve.OversoldReplicationReserveError as exc:
        assert "invalid" in str(exc)
    else:
        raise AssertionError("selection drift must fail closed")

from __future__ import annotations

import etf_macro_pullback_replication as replication


def test_macro_replication_capacity_and_grid_are_dense():
    assert replication.TOTAL_SESSIONS == 1_705
    assert replication.DEVELOPMENT_SESSIONS == 1_000
    assert replication.CONFIRMATION_SESSIONS == 500
    assert replication.SYMBOLS == [
        "DBC",
        "EEM",
        "EFA",
        "GLD",
        "IEF",
        "TLT",
    ]


def test_macro_replication_partitions_are_chronological():
    dates = replication._full_sessions()[-replication.TOTAL_SESSIONS:]
    warmup = dates[:replication.DEVELOPMENT_WARMUP_SESSIONS]
    development = dates[
        replication.DEVELOPMENT_WARMUP_SESSIONS:
        replication.DEVELOPMENT_WARMUP_SESSIONS
        + replication.DEVELOPMENT_SESSIONS
    ]
    embargo_start = (
        replication.DEVELOPMENT_WARMUP_SESSIONS
        + replication.DEVELOPMENT_SESSIONS
    )
    embargo = dates[
        embargo_start:
        embargo_start + replication.EMBARGO_SESSIONS
    ]
    confirmation = dates[-replication.CONFIRMATION_SESSIONS:]

    assert len(warmup) == 200
    assert len(development) == 1_000
    assert len(embargo) == 5
    assert len(confirmation) == 500
    assert warmup[-1] < development[0]
    assert development[-1] < embargo[0]
    assert embargo[-1] < confirmation[0]
    assert confirmation[-1] == "2022-12-30"

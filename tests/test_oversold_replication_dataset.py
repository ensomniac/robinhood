from __future__ import annotations

from pathlib import Path

import oversold_replication_dataset as dataset


def test_store_path_is_relative_and_fail_closed(tmp_path: Path) -> None:
    class Store:
        root = tmp_path

    assert dataset._store_path(
        Store(),
        tmp_path / "inputs/data.json.gz",
    ) == "inputs/data.json.gz"
    try:
        dataset._store_path(
            Store(),
            tmp_path.parent / "escaped.json.gz",
        )
    except dataset.OversoldReplicationDatasetError as exc:
        assert "escaped" in str(exc)
    else:
        raise AssertionError("private path escape must fail")


def test_timestamp_requires_timezone() -> None:
    dataset._timestamp("2026-07-24T10:29:00-04:00")
    try:
        dataset._timestamp("2026-07-24T10:29:00")
    except dataset.OversoldReplicationDatasetError as exc:
        assert "timezone" in str(exc)
    else:
        raise AssertionError("naive registration time must fail")

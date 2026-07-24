from __future__ import annotations

import pytest

import oversold_replication_confirmation_dataset as dataset


def test_timestamp_requires_timezone() -> None:
    with pytest.raises(
        dataset.OversoldReplicationConfirmationDatasetError,
        match="timezone",
    ):
        dataset._timestamp("2026-07-24T10:00:00")


def test_one_rejects_missing_artifact(tmp_path) -> None:
    with pytest.raises(
        dataset.OversoldReplicationConfirmationDatasetError,
        match="exactly one",
    ):
        dataset._one(tmp_path)

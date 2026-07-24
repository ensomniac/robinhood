from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest

import oversold_replication_confirmation_collection as collection


def test_timestamp_requires_timezone() -> None:
    with pytest.raises(
        collection.OversoldReplicationConfirmationCollectionError,
        match="timezone",
    ):
        collection._timestamp("2026-07-24T10:00:00")


def test_scope_preserves_frozen_candidate_denominator() -> None:
    inventory = {
        "signal_dates": ["2026-01-12", "2026-01-13"],
        "candidates_by_date": {
            "2026-01-12": [{"symbol": "B"}, {"symbol": "A"}],
            "2026-01-13": [{"symbol": "C"}],
        },
    }
    assert collection._scope(inventory) == {
        "dates": ["2026-01-12", "2026-01-13"],
        "symbols_by_date": {
            "2026-01-12": ["B", "A"],
            "2026-01-13": ["C"],
        },
    }


def test_collection_chronology_is_strict() -> None:
    winner = datetime.now(UTC)
    assert winner + timedelta(seconds=1) > winner

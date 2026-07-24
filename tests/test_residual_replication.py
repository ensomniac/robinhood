from __future__ import annotations

from datetime import date, timedelta

import residual_replication_data as data
import residual_replication_inspection as inspection
import residual_replication_plugin as plugin
from historical_store import canonical_sha256


def _days(count: int) -> list[str]:
    start = date(2023, 1, 3)
    return [(start + timedelta(days=index)).isoformat() for index in range(count)]


def test_independent_artifact_hash_excludes_only_its_digest():
    payload = {"artifact_kind": "example", "state": "FROZEN"}
    artifact = {
        **payload,
        "artifact_sha256": canonical_sha256(payload),
    }

    assert inspection._artifact_hash(artifact) == artifact["artifact_sha256"]


def test_frozen_source_graph_is_disjoint_and_power_capable():
    source_dates, identities, bindings = data._source_graph()
    development, development_signals, embargo, confirmation, confirmation_signals = (
        data._partitions(source_dates)
    )

    assert len(source_dates) == len(set(source_dates)) == 300
    assert len(bindings) == 3
    assert len(development_signals) == 200
    assert len(embargo) == 5
    assert len(confirmation_signals) == 93
    assert set(development_signals).isdisjoint(confirmation_signals)
    assert max(development) < min(embargo) < min(confirmation)
    assert all(len(identities[day]) >= 500 for day in source_dates)


def test_replication_liquidity_selection_is_deterministic_top_250():
    days = _days(61)
    decision = days[-1]
    symbols = [f"S{index:03d}" for index in range(251)]
    bars = {
        symbol: [
            {
                "date": day,
                "open": 20.0,
                "high": 20.1,
                "low": 19.9,
                "close": 20.0,
                "volume": 3_000_000 + index,
            }
            for day in days
        ]
        for index, symbol in enumerate(symbols)
    }
    bars["SPY"] = [
        {
            "date": day,
            "open": 100.0,
            "high": 100.1,
            "low": 99.9,
            "close": 100.0,
            "volume": 100_000_000,
        }
        for day in days
    ]
    dataset = {
        "family_id": data.FAMILY_ID,
        "evaluation_dates": days,
        "decision_dates": [decision],
        "reference_identities_by_date": {
            decision: {
                symbol: f"listing-{symbol}" for symbol in symbols
            }
        },
        "split_execution_dates_by_symbol": {},
        "daily_bars": bars,
    }

    prepared = plugin._select_universes(dataset)
    selected = prepared["universe_by_date"][decision]

    assert len(selected) == 250
    assert "S000" not in selected
    assert selected == list(reversed(symbols[1:]))
    assert canonical_sha256(selected) == canonical_sha256(
        prepared["universe_by_date"][decision]
    )

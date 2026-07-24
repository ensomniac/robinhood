from __future__ import annotations

import json

import dense_strategy_runtime as runtime
import residual_temporal_data as data
import residual_temporal_plugin as plugin


def test_temporal_partitions_add_200_fresh_before_v6_training():
    v6_contract = json.loads(data.reference.V6_CONTRACT.read_text())

    development, signals, embargo, confirmation, confirmation_signals = (
        data._partitions(data.reference.selected_dates(), v6_contract)
    )

    assert len(signals) == 400
    assert signals[:200] == data.reference.selected_dates()
    assert signals[200:] == v6_contract["development_signal_dates"]
    assert max(development) < min(embargo) < min(confirmation)
    assert len(embargo) == 5
    assert len(confirmation_signals) == 93


def test_temporal_family_uses_equity_residual_runtime():
    assert data.FAMILY_ID in runtime.EQUITY_RESIDUAL_FAMILIES
    assert data.FAMILY_ID in runtime.RESIDUAL_FAMILIES


def test_temporal_empty_series_normalization_changes_no_nonempty_rows():
    dataset = {
        "daily_bars": {
            "SPY": [{"date": "2021-01-04", "close": 100.0}],
            "EMPTY": [],
            "FULL": [{"date": "2021-01-04", "close": 10.0}],
        },
        "reference_identities_by_date": {
            "2021-01-04": {"EMPTY": "one", "FULL": "two"}
        },
    }
    before = data.canonical_sha256(dataset["daily_bars"]["FULL"])
    identity_hash = data.canonical_sha256(
        dataset["reference_identities_by_date"]
    )

    normalized, removed = plugin._drop_empty_series(dataset)

    assert removed == ["EMPTY"]
    assert set(normalized["daily_bars"]) == {"SPY", "FULL"}
    assert (
        data.canonical_sha256(normalized["daily_bars"]["FULL"]) == before
    )
    assert (
        data.canonical_sha256(
            normalized["reference_identities_by_date"]
        )
        == identity_hash
    )

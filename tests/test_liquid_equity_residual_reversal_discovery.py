from __future__ import annotations

import tempfile
from pathlib import Path

import dense_strategy_runtime as runtime
import liquid_equity_momentum_plugin as plugin
import liquid_equity_residual_reversal_discovery as discovery
import pytest


def test_residual_successor_partitions_do_not_wait_for_calendar_reset():
    (
        development,
        development_signals,
        embargo,
        confirmation,
        confirmation_signals,
    ) = discovery._partitions(enforce_commit=False)

    assert len(development_signals) == 80
    assert len(embargo) == 5
    assert len(confirmation_signals) == 25
    assert max(development) < min(embargo) < min(confirmation)
    assert set(development_signals).issubset(development)
    assert set(confirmation_signals).issubset(confirmation)


def test_residual_contract_freezes_all_48_trials_without_outcome_derivation():
    with tempfile.TemporaryDirectory(dir=discovery.PROJECT_ROOT) as directory:
        path, contract, capacity = discovery.freeze_successor_contract(
            created_at="2026-07-23T22:00:00Z",
            root=Path(directory),
            enforce_commit=False,
        )

        assert path.is_file()
        assert capacity.is_file()
        assert contract["family_id"] == runtime.EQUITY_RESIDUAL_FAMILY
        assert contract["mechanism_family"] == (
            "two-to-three-day-cross-sectional-reversal"
        )
        assert len(contract["trial_family"]) == 48
        assert contract["confirmation_signal_capacity"] == 25
        assert contract["new_mechanism_family_slot_consumed"] is False
        assert contract["plugin"]["module"] == "liquid_equity_momentum_plugin"
        assert contract["successor_id"] == (
            "two-to-three-day-cross-sectional-reversal-v5-"
            "liquid-common-stock-residual-spy"
        )
        assert contract["supersedes_diagnostic_transition"][
            "promotion_eligible"
        ] is False
        manifest = discovery._read(capacity)
        spy = manifest["dataset_payload"]["spy_reference_source"]
        assert spy["reference_start"] == "2024-01-02"
        assert spy["reference_end"] == contract["development_dates"][-1]
        assert spy["reference_sessions"] == len(spy["rows"])
        assert spy["reference_sessions"] >= 300
        assert spy["prices_or_returns_derived"] is False


def test_shared_daily_adapter_binds_exact_supported_family():
    manifest = {
        "dataset_payload": {
            "liquid_equity_daily_source": {
                "family_id": runtime.EQUITY_RESIDUAL_FAMILY,
                "external_relative_path": plugin.SOURCE_RELATIVE_PATH,
                "external_file_sha256": "a" * 64,
                "format": "json.gz",
                "signal_dates": ["2025-01-02"],
                "formal_capacity": 250,
            }
        }
    }

    binding = plugin._binding(
        manifest,
        expected_family_id=runtime.EQUITY_RESIDUAL_FAMILY,
    )

    assert binding["family_id"] == runtime.EQUITY_RESIDUAL_FAMILY
    with pytest.raises(
        plugin.LiquidEquityMomentumPluginError,
        match="source identity drifted",
    ):
        plugin._binding(
            manifest,
            expected_family_id=runtime.LIQUID_EQUITY_MOMENTUM_FAMILY,
        )

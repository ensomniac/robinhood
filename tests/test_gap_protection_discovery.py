from __future__ import annotations

import gap_protection_discovery as discovery
import gap_protection_plugin as plugin
import gap_protection_successor as successor


def test_frozen_grid_has_all_32_protection_trials():
    count = 1
    for values in successor.PARAMETER_GRID.values():
        count *= len(values)
    assert count == 32
    assert successor.PARAMETER_GRID["opening_range_minutes"] == [5]


def test_plugin_telemetry_is_warm_and_provider_free():
    assert plugin._telemetry(dataset_loads=1) == {
        "requests": 0,
        "request_seconds": 0.0,
        "pacing_wait_seconds": 0.0,
        "cache_hits": 1,
        "failures": 0,
        "dataset_loads": 1,
    }


def test_discovery_identity_is_existing_family_successor():
    assert (
        discovery.SUCCESSOR_ID
        == "equity-gap-protection-continuation-v3-development-search"
    )


def test_confirmation_consumer_runs_only_the_frozen_winner(monkeypatch):
    winner = {
        "rules_hash": "f" * 64,
        "confirmation_dates": ["2025-01-02"],
        "exact_rules": {
            "selected_trial_id": "trial-017",
            "parameters": {
                "maximum_structural_stop_fraction": 0.04,
            },
        },
    }
    manifest = plugin.PROJECT_ROOT / "strategy_tournament/confirmation-dataset.json"
    exact = {
        "scenarios": {
            "5bps": {"daily_account_returns": [0.001]},
            "10bps": {"daily_account_returns": [0.0009]},
            "20bps": {"daily_account_returns": [0.0007]},
        },
        "maturity_rows": [{"date": "2025-01-02"}],
    }
    calls = []
    monkeypatch.setattr(plugin, "_confirmation_manifest", lambda value: manifest)
    monkeypatch.setattr(
        plugin,
        "_load_bound_dataset",
        lambda path, **kwargs: calls.append((path, kwargs)) or {"raw": True},
    )
    monkeypatch.setattr(plugin.runtime, "prepare_dataset", lambda value: value)
    monkeypatch.setattr(
        plugin.runtime,
        "evaluate_trial",
        lambda dataset, **kwargs: calls.append((dataset, kwargs)) or exact,
    )

    result = plugin.evaluate_confirmation(winner)

    assert result["rules_hash"] == winner["rules_hash"]
    assert result["parameter_alternatives"] == 0
    assert result["observed_dates"] == winner["confirmation_dates"]
    assert result["outcome_access_before_winner_freeze"] is False
    assert calls[0][1]["lane"] == "confirmation"
    assert calls[0][1]["preregistration_sha256"] == winner["rules_hash"]
    assert calls[1][1]["trial_id"] == "trial-017"

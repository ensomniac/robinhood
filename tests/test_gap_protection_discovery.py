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
        == "equity-gap-protection-continuation-v1-development-search"
    )

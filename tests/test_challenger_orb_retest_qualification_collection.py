from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

from historical_store import HistoricalDayStore

import challenger_orb_retest_qualification_collection as collection


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_trigger_selection_rebuilds_aggregate_only() -> None:
    store = HistoricalDayStore.from_env(PROJECT_ROOT / ".env")
    value = collection.build_trigger_selection(store.root)
    assert value["positive_pair_count"] == 60
    assert value["distinct_trigger_dates"] == 52
    assert len(value["request_graph"]) == 60
    assert value["post_entry_data_accessed"] is False
    assert value["target_outcomes_observed_or_derived"] is False


def test_adapter_source_state_binds_capacity() -> None:
    _manifest, result, private, _store = collection._load_base(
        PROJECT_ROOT / ".env"
    )
    assert result["minimum_development_signal_capacity_passed"] is True
    assert len(private["selection"]["positive_pairs"]) == 60
    assert private["target_outcomes_observed_or_derived"] is False


def test_initial_state_pins_retest_rebreak() -> None:
    store = HistoricalDayStore.from_env(PROJECT_ROOT / ".env")
    pair = collection.build_trigger_selection(store.root)["positive_pairs"][0]
    state = collection._initial_pair_state(pair, "a" * 64)
    assert state["clean_cross"]["observed_at_et"] == pair["trigger"]["rebreak_at_et"]
    assert state["clean_cross"]["price"] == pair["trigger"]["rebreak_price"]
    assert state["search_windows_complete"] == 0
    assert state["target_outcome_observed_or_derived"] is False


def test_qualification_collection_clis_are_available() -> None:
    for path in (
        "challenger_orb_retest_qualification_collection.py",
        "challenger_orb_retest_qualification_collection_inspection.py",
    ):
        completed = subprocess.run(
            [sys.executable, path, "--help"],
            cwd=PROJECT_ROOT,
            check=False,
            capture_output=True,
            text=True,
        )
        assert completed.returncode == 0, json.dumps(
            {"path": path, "stderr": completed.stderr}
        )

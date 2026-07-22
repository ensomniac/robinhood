from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

import challenger_orb_retest_cumulative_capacity as capacity
from historical_store import HistoricalStoreConfig


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_cumulative_capacity_rebuilds_without_outcomes() -> None:
    config = HistoricalStoreConfig.from_env(PROJECT_ROOT / ".env")
    summary, trigger_sets, corpus_sets = capacity.build_capacity_state(config.root)
    assert summary["source_trigger_session_counts"] == [22, 22, 8]
    assert summary["source_trigger_pair_counts"] == [29, 23, 8]
    assert summary["pairwise_source_session_intersections"] == [0, 0, 0]
    assert summary["pairwise_trigger_session_intersections"] == [0, 0, 0]
    assert summary["cumulative_distinct_trigger_sessions"] == 52
    assert summary["minimum_required_development_signals"] == 50
    assert summary["minimum_development_signal_capacity_passed"] is True
    assert summary["post_entry_data_accessed"] is False
    assert summary["target_outcomes_observed_or_derived"] is False
    assert capacity._pairwise_counts(trigger_sets) == [0, 0, 0]
    assert capacity._pairwise_counts(corpus_sets) == [0, 0, 0]


def test_capacity_summary_does_not_publish_exact_identities() -> None:
    config = HistoricalStoreConfig.from_env(PROJECT_ROOT / ".env")
    summary, _trigger_sets, _corpus_sets = capacity.build_capacity_state(config.root)
    rendered = json.dumps(summary, sort_keys=True)
    assert '"records"' not in rendered
    assert '"dates"' not in rendered
    assert '"symbols"' not in rendered
    assert '"instrument_ids"' not in rendered
    assert summary["exact_dates_symbols_and_instrument_ids_public"] is False


def test_capacity_manifest_detects_mutation(tmp_path: Path) -> None:
    value = {
        "schema_version": 1,
        "dataset_id": capacity.DATASET_ID,
        "registered_at": "2026-07-22T15:30:00+00:00",
    }
    path, manifest = capacity._write_manifest(value, tmp_path)
    assert capacity.load_manifest(path) == manifest
    changed = dict(manifest)
    changed["registered_at"] = "2026-07-22T15:31:00+00:00"
    capacity._write_json(path, changed)
    with pytest.raises(capacity.ChallengerCumulativeCapacityError):
        capacity.load_manifest(path)


def test_cumulative_capacity_clis_are_available() -> None:
    for path in (
        "challenger_orb_retest_cumulative_capacity.py",
        "challenger_orb_retest_cumulative_capacity_inspection.py",
    ):
        completed = subprocess.run(
            [sys.executable, path, "--help"],
            cwd=PROJECT_ROOT,
            check=False,
            capture_output=True,
            text=True,
        )
        assert completed.returncode == 0

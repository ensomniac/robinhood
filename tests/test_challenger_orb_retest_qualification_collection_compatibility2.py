from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

import challenger_orb_retest_qualification_collection_compatibility2 as v2


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_collector_local_time_preserves_instant_and_uses_eastern() -> None:
    value = v2.collector_local_time("2023-01-03T14:35:10+00:00")
    assert value.isoformat() == "2023-01-03T09:35:10-05:00"


def test_quote_call_snapshot_rebuilds_all_stored_snapshots() -> None:
    with pytest.raises(v2.v1.ChallengerQualificationCompatibilityError):
        v2.build_quote_call_snapshot(PROJECT_ROOT / ".env")
    matches = sorted(v2.DEFAULT_OUTPUT_ROOT.glob(f"{v2.DATASET_ID}-*.json"))
    assert len(matches) == 1
    value = v2.load_manifest(matches[0])["incident_snapshot"]
    assert value["source_pairs"] == 60
    assert value["stored_collector_local_snapshot_rebuilds"] == 60
    assert value["source_utc_representation_snapshot_mismatches"] == 56
    assert value["empty_snapshot_sets_unaffected"] == 4
    assert value["maximum_absolute_timestamp_delta_seconds"] == 0
    assert value["target_outcomes_observed_or_derived"] is False


def test_v2_manifest_detects_mutation(tmp_path: Path) -> None:
    value = {
        "schema_version": 1,
        "dataset_id": v2.DATASET_ID,
        "registered_at": "2026-07-22T16:10:00+00:00",
    }
    path, manifest = v2._write_manifest(value, tmp_path)
    assert v2.load_manifest(path) == manifest
    changed = dict(manifest)
    changed["registered_at"] = "2026-07-22T16:11:00+00:00"
    v2.v1._write_json(path, changed)
    with pytest.raises(v2.ChallengerQualificationCompatibility2Error):
        v2.load_manifest(path)


def test_v2_snapshot_does_not_publish_identities() -> None:
    rendered = json.dumps(v2.v1._read_json(v2.DEFAULT_RESULT))
    for forbidden in ('"dates"', '"symbols"', '"instrument_ids"', '"requests"'):
        assert forbidden not in rendered


def test_v2_compatibility_clis_are_available() -> None:
    for path in (
        "challenger_orb_retest_qualification_collection_compatibility2.py",
        "challenger_orb_retest_qualification_collection_compatibility_inspection2.py",
    ):
        completed = subprocess.run(
            [sys.executable, path, "--help"],
            cwd=PROJECT_ROOT,
            check=False,
            capture_output=True,
            text=True,
        )
        assert completed.returncode == 0

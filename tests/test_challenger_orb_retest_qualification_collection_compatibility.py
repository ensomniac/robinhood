from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

import challenger_orb_retest_qualification_collection_compatibility as compat


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_same_instant_requires_aware_exact_equality() -> None:
    assert compat.same_instant(
        "2023-01-03T14:35:10+00:00", "2023-01-03T09:35:10-05:00"
    )
    assert not compat.same_instant(
        "2023-01-03T14:35:11+00:00", "2023-01-03T09:35:10-05:00"
    )
    assert not compat.same_instant(
        "2023-01-03T14:35:10", "2023-01-03T09:35:10-05:00"
    )


def test_compatibility_snapshot_is_outcome_blind_and_complete() -> None:
    value = compat.build_compatibility_snapshot(PROJECT_ROOT / ".env")
    assert value["source_pairs"] == 60
    assert value["source_distinct_trigger_sessions"] == 52
    assert value["observed_at_semantic_matches"] == 60
    assert value["decision_at_semantic_matches"] == 60
    assert value["observed_at_representation_differences"] == 60
    assert value["decision_at_representation_differences"] == 60
    assert value["maximum_absolute_timestamp_delta_seconds"] == 0
    assert value["target_outcomes_observed_or_derived"] is False


def test_compatibility_manifest_detects_mutation(tmp_path: Path) -> None:
    value = {
        "schema_version": 1,
        "dataset_id": compat.DATASET_ID,
        "registered_at": "2026-07-22T16:00:00+00:00",
    }
    path, manifest = compat._write_manifest(value, tmp_path)
    assert compat.load_manifest(path) == manifest
    changed = dict(manifest)
    changed["registered_at"] = "2026-07-22T16:01:00+00:00"
    compat._write_json(path, changed)
    with pytest.raises(compat.ChallengerQualificationCompatibilityError):
        compat.load_manifest(path)


def test_compatibility_snapshot_does_not_publish_identities() -> None:
    rendered = json.dumps(
        compat.build_compatibility_snapshot(PROJECT_ROOT / ".env"), sort_keys=True
    )
    for forbidden in ('"dates"', '"symbols"', '"instrument_ids"', '"requests"'):
        assert forbidden not in rendered


def test_compatibility_clis_are_available() -> None:
    for path in (
        "challenger_orb_retest_qualification_collection_compatibility.py",
        "challenger_orb_retest_qualification_collection_compatibility_inspection.py",
    ):
        completed = subprocess.run(
            [sys.executable, path, "--help"],
            cwd=PROJECT_ROOT,
            check=False,
            capture_output=True,
            text=True,
        )
        assert completed.returncode == 0

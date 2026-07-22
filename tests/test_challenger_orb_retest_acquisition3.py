from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

import challenger_orb_retest_acquisition3 as acquisition


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_third_acquisition_configuration_is_isolated() -> None:
    code = """
import json
import challenger_orb_retest_acquisition3 as value
with value.configured() as acquisition:
    print(json.dumps({
        'dataset_id': acquisition.DATASET_ID,
        'scanner_dataset_id': acquisition.SCANNER_DATASET_ID,
        'selection_dataset_id': acquisition.tranche.DATASET_ID,
        'implementation': acquisition.__file__,
        'reference_root': str(acquisition.REFERENCE_ROOT),
    }))
"""
    completed = subprocess.run(
        [sys.executable, "-c", code],
        cwd=PROJECT_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    value = json.loads(completed.stdout)
    assert value["dataset_id"] == (
        "dataset-challenger-orb-retest-acquisition-2026-07-22-tranche3-v1"
    )
    assert value["scanner_dataset_id"].endswith(
        "challenger-orb-retest-tranche3-v1"
    )
    assert value["selection_dataset_id"].endswith("2026-07-22-v3")
    assert value["implementation"].endswith(
        "challenger_orb_retest_acquisition3.py"
    )
    assert "challenger_orb_retest_v1_tranche3" in value["reference_root"]
    assert value["reference_root"].endswith("reference-canonical")


def test_third_acquisition_reference_normalization_excludes_collisions() -> None:
    rows = [
        {"ticker": "ABC", "name": "regular"},
        {"ticker": "SPINw", "name": "when issued"},
        {"ticker": "KW", "name": "regular collision"},
        {"ticker": "Kw", "name": "when issued collision"},
    ]
    canonical, status = acquisition._canonicalize_reference_rows(rows)
    assert canonical == [
        {"ticker": "ABC", "name": "regular"},
        {"ticker": "SPINW", "name": "when issued"},
    ]
    assert status == {
        "source_rows": 4,
        "canonical_rows": 2,
        "case_normalized_rows": 1,
        "collision_groups_excluded": 1,
        "collision_rows_excluded": 2,
    }


def test_third_acquisition_reference_normalization_rejects_exact_duplicates() -> None:
    with pytest.raises(
        acquisition.ChallengerAcquisition3Error,
        match="duplicate case-sensitive ticker",
    ):
        acquisition._canonicalize_reference_rows(
            [{"ticker": "ABC"}, {"ticker": "ABC"}]
        )


def test_third_acquisition_help_is_available() -> None:
    completed = subprocess.run(
        [sys.executable, "challenger_orb_retest_acquisition3.py", "--help"],
        cwd=PROJECT_ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0

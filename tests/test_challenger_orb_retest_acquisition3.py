from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


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


def test_third_acquisition_reference_zero_state() -> None:
    completed = subprocess.run(
        [
            sys.executable,
            "challenger_orb_retest_acquisition3_inspection.py",
            "--reference-only",
        ],
        cwd=PROJECT_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    value = json.loads(completed.stdout)
    assert value["requested"] == 100
    assert value["ready"] == 0
    assert value["missing"] == 100
    assert value["target_market_data_accessed"] is False
    assert value["target_outcomes_observed_or_derived"] is False


def test_third_acquisition_help_is_available() -> None:
    completed = subprocess.run(
        [sys.executable, "challenger_orb_retest_acquisition3.py", "--help"],
        cwd=PROJECT_ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0

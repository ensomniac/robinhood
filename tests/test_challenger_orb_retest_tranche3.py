from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_third_challenger_preflight_is_disjoint_and_outcome_blind() -> None:
    completed = subprocess.run(
        [sys.executable, "challenger_orb_retest_tranche3.py", "preflight"],
        cwd=PROJECT_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    value = json.loads(completed.stdout)
    assert value["selected_dates"] == 100
    assert value["eligible_dates"] >= 100
    assert value["excluded_dates"] >= 200
    assert value["required_sessions"] >= 100
    assert value["target_outcomes_observed_or_derived"] is False


def test_third_challenger_inspector_help_is_available() -> None:
    completed = subprocess.run(
        [sys.executable, "challenger_orb_retest_tranche3_inspection.py", "--help"],
        cwd=PROJECT_ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0

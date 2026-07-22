from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_challenger_recovery_adapter_isolated_configuration() -> None:
    code = """
import json
import challenger_sec_accession_chain_recovery as value
value.configure_base()
print(json.dumps({
    'dataset_id': value.base.DATASET_ID,
    'pairs': value.base.EXPECTED_UNRESOLVED_PAIRS,
    'joins': value.base.EXPECTED_PRIOR_JOINS,
    'accessions': value.base.EXPECTED_UNIQUE_ACCESSIONS,
    'implementation': value.base.__file__,
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
    assert value["dataset_id"].endswith("challenger-orb-retest-v1")
    assert value["pairs"] == 372
    assert value["joins"] == 433
    assert value["accessions"] == 417
    assert value["implementation"].endswith(
        "challenger_sec_accession_chain_recovery.py"
    )


def test_challenger_recovery_help_is_available() -> None:
    completed = subprocess.run(
        [sys.executable, "challenger_sec_accession_chain_recovery.py", "--help"],
        cwd=PROJECT_ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0
    assert "freeze" in completed.stdout


def test_challenger_collection_inspection_help_is_available() -> None:
    completed = subprocess.run(
        [
            sys.executable,
            "challenger_sec_accession_chain_collection_inspection.py",
            "--help",
        ],
        cwd=PROJECT_ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0
    assert "manifest" in completed.stdout

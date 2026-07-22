from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_second_challenger_recovery_adapter_isolated_configuration() -> None:
    code = """
import json
import challenger_sec_accession_chain_recovery2 as value
value.configure_base()
print(json.dumps({
    'dataset_id': value.base.DATASET_ID,
    'pairs': value.base.EXPECTED_UNRESOLVED_PAIRS,
    'joins': value.base.EXPECTED_PRIOR_JOINS,
    'accessions': value.base.EXPECTED_UNIQUE_ACCESSIONS,
    'prior_positives': value.base.EXPECTED_PRIOR_POSITIVES,
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
    assert value["dataset_id"].endswith("challenger-orb-retest-tranche2-v1")
    assert value["pairs"] == 365
    assert value["joins"] == 434
    assert value["accessions"] == 425
    assert value["prior_positives"] == 18
    assert value["implementation"].endswith(
        "challenger_sec_accession_chain_recovery2.py"
    )


def test_second_challenger_recovery_help_is_available() -> None:
    completed = subprocess.run(
        [sys.executable, "challenger_sec_accession_chain_recovery2.py", "--help"],
        cwd=PROJECT_ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0
    assert "freeze" in completed.stdout


def test_second_challenger_inspector_help_is_available() -> None:
    for path in (
        "challenger_sec_accession_chain_inspection2.py",
        "challenger_sec_accession_chain_collection_inspection2.py",
    ):
        completed = subprocess.run(
            [sys.executable, path, "--help"],
            cwd=PROJECT_ROOT,
            check=False,
            capture_output=True,
            text=True,
        )
        assert completed.returncode == 0
        assert "manifest" in completed.stdout

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_second_trigger_runner_isolated_configuration() -> None:
    code = """
import json
import challenger_orb_retest_trigger_runner2 as value
value.configure_base()
print(json.dumps({
    'dataset_id': value.base.DATASET_ID,
    'implementation': value.base.__file__,
    'preentry_dataset_id': value.base.preentry.DATASET_ID,
    'requests': value.EXPECTED_REQUESTS,
    'rows': value.EXPECTED_ROWS,
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
        "dataset-challenger-orb-retest-trigger-review-2026-07-22-tranche2-v1"
    )
    assert value["implementation"].endswith(
        "challenger_orb_retest_trigger_runner2.py"
    )
    assert value["preentry_dataset_id"].endswith("2026-07-22-tranche2-v1")
    assert value["requests"] == 336
    assert value["rows"] == 4_325_297


def test_second_trigger_runner_source_state_rebuilds() -> None:
    code = """
import json
from pathlib import Path
import challenger_orb_retest_trigger_runner2 as value
value.configure_base()
manifest, config, selection = value._source_state(Path('.env'))
print(json.dumps({
    'manifest': manifest['manifest_sha256'],
    'counts': selection['counts'],
    'trigger_exists': value.preentry2.base._trigger_path(config.root).exists(),
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
    assert value["manifest"] == (
        "5a6ac82d0c1205ae915e91d629d921b309d4862525b979ade877dbc3f7ec9108"
    )
    assert value["counts"]["total_requests"] == 336
    assert value["trigger_exists"] is False


def test_second_trigger_runner_clis_are_available() -> None:
    for path in (
        "challenger_orb_retest_trigger_runner2.py",
        "challenger_orb_retest_trigger_runner_inspection2.py",
    ):
        completed = subprocess.run(
            [sys.executable, path, "--help"],
            cwd=PROJECT_ROOT,
            check=False,
            capture_output=True,
            text=True,
        )
        assert completed.returncode == 0

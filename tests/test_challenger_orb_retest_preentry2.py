from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_second_preentry_adapter_isolated_configuration() -> None:
    code = """
import json
import challenger_orb_retest_preentry2 as value
value.configure_base()
print(json.dumps({
    'dataset_id': value.base.DATASET_ID,
    'pairs': value.base.EXPECTED_POSITIVE_PAIRS,
    'dates': value.base.EXPECTED_POSITIVE_DATES,
    'implementation': value.base.__file__,
    'selected_pair_module': value.base.selected_pairs.__name__,
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
        "dataset-challenger-orb-retest-preentry-collection-2026-07-22-"
        "tranche2-v1"
    )
    assert value["pairs"] == 113
    assert value["dates"] == 55
    assert value["implementation"].endswith(
        "challenger_orb_retest_preentry2.py"
    )
    assert value["selected_pair_module"] == "challenger_orb_retest_selected_pairs2"


def test_second_preentry_private_graph_rebuilds_without_outcomes() -> None:
    code = """
import json
from pathlib import Path
from historical_store import HistoricalStoreConfig
import challenger_orb_retest_preentry2 as value
value.configure_base()
config = HistoricalStoreConfig.from_env(Path('.env'))
selection = value.build_selection(config.root)
print(json.dumps({
    'counts': selection['counts'],
    'post_entry': selection['post_entry_data_accessed'],
    'outcomes': selection['target_outcomes_observed_or_derived'],
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
    assert value["counts"] == {
        "benchmark_bar_requests": 110,
        "candidate_bar_requests": 113,
        "candidate_trade_requests": 113,
        "maximum_daily_signals": 55,
        "total_requests": 336,
        "verified_positive_dates": 55,
        "verified_positive_pairs": 113,
    }
    assert value["post_entry"] is False
    assert value["outcomes"] is False


def test_second_preentry_clis_are_available() -> None:
    for path in (
        "challenger_orb_retest_preentry2.py",
        "challenger_orb_retest_preentry_inspection2.py",
        "challenger_orb_retest_preentry_collection_inspection2.py",
    ):
        completed = subprocess.run(
            [sys.executable, path, "--help"],
            cwd=PROJECT_ROOT,
            check=False,
            capture_output=True,
            text=True,
        )
        assert completed.returncode == 0

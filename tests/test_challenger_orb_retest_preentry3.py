from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_third_preentry_adapter_isolated_configuration() -> None:
    code = """
import json
import challenger_orb_retest_preentry3 as value
value.configure_base()
print(json.dumps({
    'dataset_id': value.base.DATASET_ID,
    'pairs': value.base.EXPECTED_POSITIVE_PAIRS,
    'dates': value.base.EXPECTED_POSITIVE_DATES,
    'implementation': value.base.__file__,
    'selected_pair_module': value.base.selected_pairs.__name__,
    'direct_no_recovery': value.ACCESSION_RESULT == value.SOURCE_SEMANTICS_RESULT,
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
        "tranche3-v2"
    )
    assert value["pairs"] == 23
    assert value["dates"] == 20
    assert value["implementation"].endswith("challenger_orb_retest_preentry3.py")
    assert value["selected_pair_module"] == "challenger_orb_retest_selected_pairs3_v2"
    assert value["direct_no_recovery"] is True


def test_third_preentry_private_graph_rebuilds_without_outcomes() -> None:
    code = """
import json
from pathlib import Path
from historical_store import HistoricalStoreConfig
import challenger_orb_retest_preentry3 as value
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
        "benchmark_bar_requests": 40,
        "candidate_bar_requests": 23,
        "candidate_trade_requests": 23,
        "maximum_daily_signals": 20,
        "total_requests": 86,
        "verified_positive_dates": 20,
        "verified_positive_pairs": 23,
    }
    assert value["post_entry"] is False
    assert value["outcomes"] is False


def test_third_preentry_clis_are_available() -> None:
    for path in (
        "challenger_orb_retest_preentry3.py",
        "challenger_orb_retest_preentry_inspection3.py",
        "challenger_orb_retest_preentry_collection_inspection3.py",
    ):
        completed = subprocess.run(
            [sys.executable, path, "--help"],
            cwd=PROJECT_ROOT,
            check=False,
            capture_output=True,
            text=True,
        )
        assert completed.returncode == 0

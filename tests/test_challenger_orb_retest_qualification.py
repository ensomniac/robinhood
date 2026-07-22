from __future__ import annotations

import subprocess
import sys
from pathlib import Path

import pytest

import challenger_orb_retest_qualification as qualification


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_retest_stop_uses_noise_and_atr_without_compression() -> None:
    value = qualification.derive_retest_stop(
        entry_limit=10.10,
        retest_bar_low=10.04,
        average_close_increment=0.02,
        median_spread_dollars=0.01,
        daily_atr_14=0.50,
        minimum_observed_bid=10.08,
    )
    assert value["normal_noise_dollars"] == pytest.approx(0.02)
    assert value["technical_invalidation"] == pytest.approx(10.02)
    assert value["stop_distance"] == pytest.approx(0.08)
    assert value["planned_stop"] == pytest.approx(10.02)
    assert value["stop_outside_noise"] is True
    assert value["maximum_stop_fraction_pass"] is True
    assert value["planned_stop_below_every_observed_bid"] is True


def test_retest_stop_reports_wide_structure_instead_of_compressing() -> None:
    value = qualification.derive_retest_stop(
        entry_limit=10.10,
        retest_bar_low=9.90,
        average_close_increment=0.02,
        median_spread_dollars=0.01,
        daily_atr_14=0.50,
        minimum_observed_bid=10.08,
    )
    assert value["stop_fraction"] > 0.008
    assert value["maximum_stop_fraction_pass"] is False


def test_daily_ranking_selects_one_by_frozen_precedence() -> None:
    records = [
        {
            "pair_key": "b",
            "date": "2025-01-02",
            "terminal_reason": "RAW_SURVIVOR",
            "raw_survivor": True,
            "survivor": False,
            "selected": False,
            "ranking": {
                "opening_relative_volume": 3.0,
                "direct_catalyst_quality": 25,
                "median_spread_fraction": 0.0005,
                "private_pair_key": "b",
            },
        },
        {
            "pair_key": "a",
            "date": "2025-01-02",
            "terminal_reason": "RAW_SURVIVOR",
            "raw_survivor": True,
            "survivor": False,
            "selected": False,
            "ranking": {
                "opening_relative_volume": 4.0,
                "direct_catalyst_quality": 25,
                "median_spread_fraction": 0.0007,
                "private_pair_key": "a",
            },
        },
    ]
    ranked = qualification.apply_daily_ranking(records)
    assert [row["terminal_reason"] for row in ranked] == [
        "DAILY_RANK_NOT_SELECTED",
        "SURVIVOR",
    ]
    assert sum(row["selected"] for row in ranked) == 1


def test_source_preflight_rebuilds_complete_private_denominator() -> None:
    _store, index, pairs, states = qualification._load_source(PROJECT_ROOT / ".env")
    assert len(index["pair_files"]) == 60
    assert len(pairs) == 60
    assert len(states) == 60
    assert len({row["date"] for row in pairs}) == 52
    assert all(row["target_outcome_observed_or_derived"] is False for row in states)


def test_qualification_manifest_detects_mutation(tmp_path: Path) -> None:
    value = {
        "schema_version": 1,
        "dataset_id": qualification.DATASET_ID,
        "registered_at": "2026-07-22T16:30:00+00:00",
    }
    path, manifest = qualification._write_manifest(value, tmp_path)
    assert qualification.load_manifest(path) == manifest
    changed = dict(manifest)
    changed["registered_at"] = "2026-07-22T16:31:00+00:00"
    qualification.base._write_json(path, changed)
    with pytest.raises(qualification.ChallengerRetestQualificationError):
        qualification.load_manifest(path)


def test_qualification_clis_are_available() -> None:
    for path in (
        "challenger_orb_retest_qualification.py",
        "challenger_orb_retest_qualification_inspection.py",
    ):
        completed = subprocess.run(
            [sys.executable, path, "--help"],
            cwd=PROJECT_ROOT,
            check=False,
            capture_output=True,
            text=True,
        )
        assert completed.returncode == 0

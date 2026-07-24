from __future__ import annotations

from datetime import datetime

import sector_etf_gap_drift as gap_drift


def test_gap_drift_capacity_and_grid_are_dense():
    assert gap_drift.TOTAL_SESSIONS == 1_455
    assert gap_drift.DEVELOPMENT_SESSIONS == 1_000
    assert gap_drift.CONFIRMATION_SESSIONS == 250
    assert gap_drift.SYMBOLS == [
        "XLB",
        "XLE",
        "XLF",
        "XLI",
        "XLK",
        "XLP",
        "XLU",
        "XLV",
        "XLY",
    ]


def test_gap_drift_partitions_are_chronological_and_outcome_clean():
    dates = gap_drift._full_sessions()[-gap_drift.TOTAL_SESSIONS:]
    warmup = dates[:gap_drift.DEVELOPMENT_WARMUP_SESSIONS]
    development = dates[
        gap_drift.DEVELOPMENT_WARMUP_SESSIONS:
        gap_drift.DEVELOPMENT_WARMUP_SESSIONS
        + gap_drift.DEVELOPMENT_SESSIONS
    ]
    embargo_start = (
        gap_drift.DEVELOPMENT_WARMUP_SESSIONS
        + gap_drift.DEVELOPMENT_SESSIONS
    )
    embargo = dates[
        embargo_start:
        embargo_start + gap_drift.EMBARGO_SESSIONS
    ]
    confirmation = dates[-gap_drift.CONFIRMATION_SESSIONS:]

    assert warmup[0] == "2016-03-08"
    assert development[0] == "2016-12-21"
    assert development[-1] == "2020-12-23"
    assert embargo == [
        "2020-12-28",
        "2020-12-29",
        "2020-12-30",
        "2020-12-31",
        "2021-01-04",
    ]
    assert confirmation[0] == "2021-01-05"
    assert confirmation[-1] == "2021-12-31"


def test_gap_drift_contract_freezes_complete_selection_family(
    tmp_path, monkeypatch
):
    original_repo_path = gap_drift._repo_path

    def repo_path(path):
        try:
            return original_repo_path(path)
        except gap_drift.SectorEtfGapDriftError:
            return f"strategy_tournament/v2/test/{path.name}"

    monkeypatch.setattr(gap_drift, "DEFAULT_ROOT", tmp_path)
    monkeypatch.setattr(gap_drift, "_repo_path", repo_path)
    monkeypatch.setattr(
        gap_drift.outcome_exposure,
        "read_index",
        lambda *_args, **_kwargs: [],
    )
    monkeypatch.setattr(
        gap_drift.outcome_exposure,
        "audit",
        lambda *_args, **_kwargs: {"index_sha256": "0" * 64},
    )

    _path, contract, capacity_path = gap_drift.freeze_contract(
        created_at=datetime.now().astimezone().isoformat(),
        enforce_commit=False,
    )

    assert capacity_path.is_file()
    assert len(contract["trial_family"]) == 32
    assert contract["selection_mode"] == "development_search"
    assert contract["new_mechanism_family_slot_consumed"] is False
    assert contract["confirmation_signal_capacity"] == 250

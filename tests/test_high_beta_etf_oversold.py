from __future__ import annotations

from datetime import datetime

import high_beta_etf_oversold as oversold


def test_high_beta_oversold_capacity_and_grid_are_dense():
    assert oversold.TOTAL_SESSIONS == 1_705
    assert oversold.DEVELOPMENT_SESSIONS == 1_000
    assert oversold.CONFIRMATION_SESSIONS == 500
    assert oversold.SYMBOLS == [
        "ARKK",
        "EWJ",
        "EWZ",
        "FXI",
        "GDX",
        "KRE",
        "SMH",
        "XBI",
        "XRT",
    ]


def test_high_beta_oversold_partitions_are_chronological():
    dates = oversold._full_sessions()[-oversold.TOTAL_SESSIONS:]
    warmup = dates[:oversold.DEVELOPMENT_WARMUP_SESSIONS]
    development = dates[
        oversold.DEVELOPMENT_WARMUP_SESSIONS:
        oversold.DEVELOPMENT_WARMUP_SESSIONS
        + oversold.DEVELOPMENT_SESSIONS
    ]
    embargo_start = (
        oversold.DEVELOPMENT_WARMUP_SESSIONS
        + oversold.DEVELOPMENT_SESSIONS
    )
    embargo = dates[
        embargo_start:
        embargo_start + oversold.EMBARGO_SESSIONS
    ]
    confirmation = dates[-oversold.CONFIRMATION_SESSIONS:]

    assert warmup[-1] < development[0]
    assert development[-1] < embargo[0]
    assert embargo[-1] < confirmation[0]
    assert confirmation[-1] == "2022-12-30"


def test_high_beta_oversold_contract_freezes_complete_family(
    tmp_path, monkeypatch
):
    original_repo_path = oversold._repo_path

    def repo_path(path):
        try:
            return original_repo_path(path)
        except ValueError:
            return f"strategy_tournament/v2/test/{path.name}"

    monkeypatch.setattr(oversold, "DEFAULT_ROOT", tmp_path)
    monkeypatch.setattr(oversold, "_repo_path", repo_path)
    monkeypatch.setattr(
        oversold.outcome_exposure,
        "read_index",
        lambda *_args, **_kwargs: [],
    )
    monkeypatch.setattr(
        oversold.outcome_exposure,
        "audit",
        lambda *_args, **_kwargs: {"index_sha256": "0" * 64},
    )

    _path, contract, capacity_path = oversold.freeze_contract(
        created_at=datetime.now().astimezone().isoformat(),
        enforce_commit=False,
    )

    assert capacity_path.is_file()
    assert len(contract["trial_family"]) == 32
    assert contract["selection_mode"] == "development_search"
    assert contract["new_mechanism_family_slot_consumed"] is False
    assert contract["confirmation_signal_capacity"] == 500

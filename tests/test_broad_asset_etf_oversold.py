from __future__ import annotations

from datetime import datetime

import broad_asset_etf_oversold as oversold


def test_broad_asset_replication_preserves_dense_grid_and_clean_universe():
    assert oversold.TOTAL_SESSIONS == 1_705
    assert oversold.DEVELOPMENT_SESSIONS == 1_000
    assert oversold.CONFIRMATION_SESSIONS == 500
    assert oversold.SYMBOLS == [
        "AGG",
        "HYG",
        "LQD",
        "MDY",
        "USO",
        "UUP",
        "VNQ",
        "VOO",
        "VTI",
    ]


def test_broad_asset_replication_contract_freezes_unchanged_family(
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

    _path, contract, capacity_path = oversold.freeze_contract(
        created_at=datetime.now().astimezone().isoformat(),
        enforce_commit=False,
    )

    predecessor_grid = oversold._read_plain(
        oversold.BASE_CONTRACT
    )["parameter_grid"]
    assert capacity_path.is_file()
    assert len(contract["trial_family"]) == 32
    assert contract["parameter_grid"] == predecessor_grid
    assert contract["new_mechanism_family_slot_consumed"] is False
    assert contract["development_scope"]["symbols"] == oversold.SYMBOLS
    assert contract["confirmation_signal_capacity"] == 500

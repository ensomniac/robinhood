from __future__ import annotations

from datetime import datetime

import dense_data_collection as collection
import liquid_index_etf_opening_reversal as liquid


def test_liquid_index_replication_uses_clean_long_partitions():
    warmup, development, embargo, confirmation_warmup, confirmation = (
        liquid._partitions()
    )

    assert len(warmup) == 60
    assert len(development) == 483
    assert len(embargo) == 5
    assert len(confirmation_warmup) == 60
    assert len(confirmation) == 315
    assert development[0] == "2014-03-31"
    assert development[-1] == "2016-03-07"
    assert confirmation[0] == "2020-12-28"
    assert confirmation[-1] == "2022-03-29"
    assert liquid.SYMBOLS == ["DIA", "IWM", "QQQ", "SPY"]


def test_liquid_index_replication_freezes_unchanged_grid_and_miss_policy(
    tmp_path, monkeypatch
):
    original_repo_path = liquid._repo_path

    def repo_path(path):
        try:
            return original_repo_path(path)
        except ValueError:
            return f"strategy_tournament/v2/test/{path.name}"

    monkeypatch.setattr(liquid, "DEFAULT_ROOT", tmp_path)
    monkeypatch.setattr(liquid, "_repo_path", repo_path)

    _path, contract, capacity_path = liquid.freeze_contract(
        created_at=datetime.now().astimezone().isoformat(),
        enforce_commit=False,
    )

    previous = liquid._read_plain(liquid.BASE_CONTRACT)
    assert capacity_path.is_file()
    assert len(contract["trial_family"]) == 32
    assert contract["parameter_grid"] == previous["parameter_grid"]
    assert contract["selection_rule"] == previous["selection_rule"]
    assert contract["winner_selection"] == previous["winner_selection"]
    assert contract["development_scope"] == {
        "dates": contract["development_dates"],
        "symbols": liquid.SYMBOLS,
    }
    assert contract["confirmation_scope"] == {
        "dates": contract["confirmation_dates"],
        "symbols": liquid.SYMBOLS,
    }
    assert contract["historical_data_contract"][
        "minute_request_mode"
    ] == "symbol_range"
    assert contract["historical_data_contract"][
        "minute_missing_session_policy"
    ] == collection.INTRADAY_FIXED_UNIVERSE_MISS_POLICY
    assert contract["new_mechanism_family_slot_consumed"] is False
    assert contract["confirmation_signal_capacity"] == 315

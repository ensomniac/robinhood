from __future__ import annotations

from datetime import datetime

import dense_data_collection as collection
import liquid_index_etf_opening_reversal as liquid
import pytest


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
    monkeypatch.setattr(
        liquid.outcome_exposure,
        "read_index",
        lambda *_args, **_kwargs: [],
    )
    monkeypatch.setattr(
        liquid.outcome_exposure,
        "audit",
        lambda *_args, **_kwargs: {"index_sha256": "0" * 64},
    )

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


def test_post2016_replication_uses_contaminated_training_and_clean_reserve():
    warmup, development, embargo, confirmation_warmup, confirmation = (
        liquid._post2016_partitions()
    )

    assert len(warmup) == 60
    assert len(development) == 1_000
    assert development[0] == "2016-12-21"
    assert development[-1] == "2020-12-23"
    assert embargo == [
        "2020-12-28",
        "2020-12-29",
        "2020-12-30",
        "2020-12-31",
        "2021-01-04",
    ]
    assert len(confirmation_warmup) == 60
    assert len(confirmation) == 310
    assert confirmation[0] == "2021-01-05"
    assert confirmation[-1] == "2022-03-29"


def test_post2016_replication_freezes_explicit_contamination_boundary(
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
    development = liquid._post2016_partitions()[1]
    synthetic_exposure = liquid.outcome_exposure.build_record(
        exposure_id="test-post2016-opening-reversal-development",
        campaign_id="multi-strategy-portfolio-validation-v2",
        lane="development",
        recorded_at=datetime.now().astimezone().isoformat(),
        source_path="tests/test_liquid_index_etf_opening_reversal.py",
        source_sha256="0" * 64,
        scope={"dates": development, "symbols": liquid.SYMBOLS},
    )
    monkeypatch.setattr(
        liquid.outcome_exposure,
        "read_index",
        lambda *_args, **_kwargs: [synthetic_exposure],
    )
    monkeypatch.setattr(
        liquid.outcome_exposure,
        "audit",
        lambda *_args, **_kwargs: {"index_sha256": "0" * 64},
    )

    _path, contract, capacity_path = (
        liquid.freeze_post2016_contract(
            created_at=datetime.now().astimezone().isoformat(),
            enforce_commit=False,
        )
    )

    previous = liquid._read_plain(liquid.V1_CONTRACT)
    assert capacity_path.is_file()
    assert len(contract["trial_family"]) == 32
    assert contract["parameter_grid"] == previous["parameter_grid"]
    assert contract["winner_selection"] == previous["winner_selection"]
    assert (
        contract["development_evidence_classification"]
        == "CONTAMINATED_TRAINING_ONLY"
    )
    assert contract["development_outcomes_previously_exposed"] is True
    assert (
        contract["development_outcomes_eligible_for_confirmation"] is False
    )
    assert contract["partitions"]["confirmation_untouched"] is True
    assert contract["confirmation_signal_capacity"] == 310
    assert (
        contract["family_id"]
        == liquid.POST2016_FAMILY_ID
    )

    monkeypatch.setattr(
        liquid.outcome_exposure,
        "read_index",
        lambda *_args, **_kwargs: [],
    )
    with pytest.raises(
        liquid.LiquidIndexEtfOpeningReversalError,
        match="must be fully labeled",
    ):
        liquid.validate_post2016_successor_contract(
            contract, enforce_commit=False
        )

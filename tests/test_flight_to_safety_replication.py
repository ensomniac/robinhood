from __future__ import annotations

from datetime import date, datetime, timedelta

import dense_strategy_runtime as runtime
import flight_to_safety_replication as replication
import flight_to_safety_replication2 as replication2
import flight_to_safety_replication3 as replication3


def _business_dates(count: int) -> list[str]:
    result: list[str] = []
    current = date(2023, 1, 3)
    while len(result) < count:
        if current.weekday() < 5:
            result.append(current.isoformat())
        current += timedelta(days=1)
    return result


def _bars(
    dates: list[str], *, shock_date: str, shock_return: float
) -> list[dict[str, float | str]]:
    rows: list[dict[str, float | str]] = []
    previous = 100.0
    for day in dates:
        close = (
            previous * (1 + shock_return)
            if day == shock_date
            else previous * 1.001
        )
        rows.append(
            {
                "date": day,
                "open": previous,
                "high": max(previous, close) + 0.5,
                "low": min(previous, close) - 0.5,
                "close": close,
                "volume": 1_000_000,
            }
        )
        previous = close
    return rows


def _parameters() -> dict[str, float | int]:
    return {
        "minimum_equity_decline_fraction": 0.0075,
        "minimum_tlt_return_fraction": 0.0025,
        "trend_sma": 100,
        "stop_atr14": 1.0,
        "maximum_hold_sessions": 2,
    }


def test_replication_preserves_rules_on_different_targets():
    dates = _business_dates(230)
    evaluation = dates[-12:]
    decision = evaluation[3]
    shocks = {
        "IWB": -0.01,
        "SCHX": -0.02,
        "SPTM": -0.005,
        "TLT": 0.01,
    }
    raw = {
        "schema_version": 1,
        "family_id": runtime.FLIGHT_TO_SAFETY_REPLICATION_FAMILY,
        "evaluation_dates": evaluation,
        "symbols": ["IWB", "SCHX", "SPTM", "TLT"],
        "daily_bars": {
            symbol: _bars(
                dates, shock_date=decision, shock_return=shock
            )
            for symbol, shock in shocks.items()
        },
    }
    candidates = runtime.build_candidates(
        runtime.prepare_dataset(raw),
        runtime.FLIGHT_TO_SAFETY_REPLICATION_FAMILY,
        _parameters(),
    )
    observed = [
        item for item in candidates if item["decision_date"] == decision
    ]
    assert [item["symbol"] for item in observed] == ["SCHX", "IWB"]

    decision_index = dates.index(decision)
    production = runtime.evaluate_production_signal(
        {
            "family_id": runtime.FLIGHT_TO_SAFETY_REPLICATION_FAMILY,
            "decision_date": decision,
            "next_session_date": dates[decision_index + 1],
            "calendar_dates": dates[: decision_index + 1],
            "daily_history_complete": True,
            "symbols": ["IWB", "SCHX", "SPTM", "TLT"],
            "daily_bars": {
                symbol: bars[: decision_index + 1]
                for symbol, bars in raw["daily_bars"].items()
            },
        },
        family_id=runtime.FLIGHT_TO_SAFETY_REPLICATION_FAMILY,
        parameters=_parameters(),
        frozen_universe={
            "symbols": ["IWB", "SCHX", "SPTM", "TLT"],
            "target_symbols": ["IWB", "SCHX", "SPTM"],
            "feature_symbols": ["TLT"],
            "point_in_time": True,
        },
    )
    assert production["symbol"] == "SCHX"


def test_replication_contract_keeps_exact_grid_and_long_embargo(
    tmp_path, monkeypatch
):
    original_repo_path = replication._repo_path

    def repo_path(path):
        try:
            return original_repo_path(path)
        except ValueError:
            return f"strategy_tournament/v2/test/{path.name}"

    monkeypatch.setattr(replication, "DEFAULT_ROOT", tmp_path)
    monkeypatch.setattr(replication, "_repo_path", repo_path)
    monkeypatch.setattr(
        replication.outcome_exposure,
        "read_index",
        lambda *_args, **_kwargs: [],
    )
    monkeypatch.setattr(
        replication.outcome_exposure,
        "audit",
        lambda *_args, **_kwargs: {"index_sha256": "0" * 64},
    )

    _path, contract, capacity = replication.freeze_contract(
        created_at=datetime.now().astimezone().isoformat(),
        enforce_commit=False,
    )
    predecessor = replication._predecessor_graph(
        enforce_commit=False
    )["contract"]

    assert capacity.is_file()
    assert contract["parameter_grid"] == predecessor["parameter_grid"]
    assert contract["new_mechanism_family_slot_consumed"] is False
    assert len(contract["trial_family"]) == 32
    assert len(contract["development_dates"]) == 746
    assert len(contract["development_signal_dates"]) == 741
    assert len(contract["embargo_dates"]) == 251
    assert len(contract["confirmation_dates"]) == 251
    assert contract["confirmation_signal_capacity"] == 246


def test_identity_safe_replication_uses_new_targets_with_same_grid(
    tmp_path, monkeypatch
):
    original_repo_path = replication2._repo_path

    def repo_path(path):
        try:
            return original_repo_path(path)
        except ValueError:
            return f"strategy_tournament/v2/test/{path.name}"

    monkeypatch.setattr(replication2, "DEFAULT_ROOT", tmp_path)
    monkeypatch.setattr(replication2, "_repo_path", repo_path)
    monkeypatch.setattr(
        replication2.outcome_exposure,
        "read_index",
        lambda *_args, **_kwargs: [],
    )
    monkeypatch.setattr(
        replication2.outcome_exposure,
        "audit",
        lambda *_args, **_kwargs: {"index_sha256": "0" * 64},
    )

    _path, contract, capacity = replication2.freeze_contract(
        created_at=datetime.now().astimezone().isoformat(),
        enforce_commit=False,
    )
    base = replication2._read_contract(replication2.V1_CONTRACT)

    assert capacity.is_file()
    assert contract["parameter_grid"] == base["parameter_grid"]
    assert contract["universe"]["target_symbols"] == [
        "ITOT",
        "RSP",
        "VV",
    ]
    assert contract["prior_family_attempt_count"] == 2
    assert contract["predecessor"]["strategy_metrics_accessed"] is False
    assert contract["new_mechanism_family_slot_consumed"] is False


def test_identity_safe_runtime_targets_itot_rsp_and_vv():
    dates = _business_dates(230)
    evaluation = dates[-12:]
    decision = evaluation[3]
    shocks = {
        "ITOT": -0.01,
        "RSP": -0.005,
        "VV": -0.02,
        "TLT": 0.01,
    }
    raw = {
        "schema_version": 1,
        "family_id": runtime.FLIGHT_TO_SAFETY_REPLICATION_V2_FAMILY,
        "evaluation_dates": evaluation,
        "symbols": ["ITOT", "RSP", "VV", "TLT"],
        "daily_bars": {
            symbol: _bars(
                dates, shock_date=decision, shock_return=shock
            )
            for symbol, shock in shocks.items()
        },
    }
    candidates = runtime.build_candidates(
        runtime.prepare_dataset(raw),
        runtime.FLIGHT_TO_SAFETY_REPLICATION_V2_FAMILY,
        _parameters(),
    )
    observed = [
        item for item in candidates if item["decision_date"] == decision
    ]
    assert [item["symbol"] for item in observed] == ["VV", "ITOT"]


def test_long_history_replication_freezes_exact_grid_and_disjoint_dates(
    tmp_path, monkeypatch
):
    original_repo_path = replication3._repo_path

    def repo_path(path):
        try:
            return original_repo_path(path)
        except ValueError:
            return f"strategy_tournament/v2/test/{path.name}"

    monkeypatch.setattr(replication3, "DEFAULT_ROOT", tmp_path)
    monkeypatch.setattr(replication3, "_repo_path", repo_path)
    monkeypatch.setattr(
        replication3.outcome_exposure,
        "read_index",
        lambda *_args, **_kwargs: [],
    )
    monkeypatch.setattr(
        replication3.outcome_exposure,
        "audit",
        lambda *_args, **_kwargs: {"index_sha256": "0" * 64},
    )

    _path, contract, capacity = replication3.freeze_contract(
        created_at=datetime.now().astimezone().isoformat(),
        enforce_commit=False,
    )
    base = replication3._read_contract(replication3.V2_CONTRACT)

    assert capacity.is_file()
    assert contract["parameter_grid"] == base["parameter_grid"]
    assert contract["universe"]["target_symbols"] == ["DIA", "IWM", "QQQ"]
    assert contract["development_dates"][0] == "2008-10-17"
    assert contract["development_dates"][-1] == "2012-10-15"
    assert contract["embargo_dates"] == [
        "2012-10-16",
        "2012-10-17",
        "2012-10-18",
        "2012-10-19",
        "2012-10-22",
    ]
    assert contract["confirmation_dates"][0] == "2012-10-23"
    assert contract["confirmation_dates"][-1] == "2013-12-31"
    assert contract["confirmation_signal_capacity"] == 288
    assert contract["historical_data_contract"]["daily_provider"] == "yahoo"
    assert contract["new_mechanism_family_slot_consumed"] is False
    assert contract["prior_family_attempt_count"] == 3


def test_long_history_runtime_targets_dia_iwm_and_qqq():
    dates = _business_dates(230)
    evaluation = dates[-12:]
    decision = evaluation[3]
    shocks = {
        "DIA": -0.01,
        "IWM": -0.005,
        "QQQ": -0.02,
        "TLT": 0.01,
    }
    raw = {
        "schema_version": 1,
        "family_id": runtime.FLIGHT_TO_SAFETY_REPLICATION_V3_FAMILY,
        "evaluation_dates": evaluation,
        "symbols": ["DIA", "IWM", "QQQ", "TLT"],
        "daily_bars": {
            symbol: _bars(
                dates, shock_date=decision, shock_return=shock
            )
            for symbol, shock in shocks.items()
        },
    }
    prepared = runtime.prepare_dataset(raw)
    candidates = runtime.build_candidates(
        prepared,
        runtime.FLIGHT_TO_SAFETY_REPLICATION_V3_FAMILY,
        _parameters(),
    )
    observed = [
        item for item in candidates if item["decision_date"] == decision
    ]

    assert [item["symbol"] for item in observed] == ["QQQ", "DIA"]

    decision_index = dates.index(decision)
    production = runtime.evaluate_production_signal(
        {
            "family_id": runtime.FLIGHT_TO_SAFETY_REPLICATION_V3_FAMILY,
            "decision_date": decision,
            "next_session_date": dates[decision_index + 1],
            "calendar_dates": dates[: decision_index + 1],
            "daily_history_complete": True,
            "symbols": ["DIA", "IWM", "QQQ", "TLT"],
            "daily_bars": {
                symbol: bars[: decision_index + 1]
                for symbol, bars in raw["daily_bars"].items()
            },
        },
        family_id=runtime.FLIGHT_TO_SAFETY_REPLICATION_V3_FAMILY,
        parameters=_parameters(),
        frozen_universe={
            "symbols": ["DIA", "IWM", "QQQ", "TLT"],
            "target_symbols": ["DIA", "IWM", "QQQ"],
            "feature_symbols": ["TLT"],
            "point_in_time": True,
        },
    )

    assert production["symbol"] == "QQQ"
    assert production["overnight_hold"] is True

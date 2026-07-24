from __future__ import annotations

from datetime import date, datetime, timedelta

import pytest

import dense_strategy_runtime as runtime
import flight_to_safety_rebound as rebound


def _business_dates(count: int) -> list[str]:
    result: list[str] = []
    current = date(2023, 1, 3)
    while len(result) < count:
        if current.weekday() < 5:
            result.append(current.isoformat())
        current += timedelta(days=1)
    return result


def _bars(
    dates: list[str],
    *,
    shock_date: str | None = None,
    shock_return: float = 0.0,
) -> list[dict[str, float | str]]:
    rows: list[dict[str, float | str]] = []
    previous = 100.0
    for day in dates:
        close = (
            previous * (1 + shock_return)
            if day == shock_date
            else previous * 1.001
        )
        opening = previous
        rows.append(
            {
                "date": day,
                "open": opening,
                "high": max(opening, close) + 0.5,
                "low": min(opening, close) - 0.5,
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


def _dataset() -> tuple[dict, list[str], str]:
    dates = _business_dates(230)
    evaluation_dates = dates[-12:]
    decision_date = evaluation_dates[3]
    daily_bars = {
        "MDY": _bars(
            dates, shock_date=decision_date, shock_return=-0.01
        ),
        "VOO": _bars(
            dates, shock_date=decision_date, shock_return=-0.02
        ),
        "VTI": _bars(
            dates, shock_date=decision_date, shock_return=-0.005
        ),
        "TLT": _bars(
            dates, shock_date=decision_date, shock_return=0.01
        ),
    }
    dataset = {
        "schema_version": 1,
        "family_id": runtime.FLIGHT_TO_SAFETY_REBOUND_FAMILY,
        "evaluation_dates": evaluation_dates,
        "symbols": ["MDY", "VOO", "VTI", "TLT"],
        "daily_bars": daily_bars,
    }
    return dataset, dates, decision_date


def test_cross_asset_historical_and_production_rank_same_target():
    raw, dates, decision_date = _dataset()
    prepared = runtime.prepare_dataset(raw)
    candidates = runtime.build_candidates(
        prepared,
        runtime.FLIGHT_TO_SAFETY_REBOUND_FAMILY,
        _parameters(),
    )
    observed = [
        item
        for item in candidates
        if item["decision_date"] == decision_date
    ]
    assert [item["symbol"] for item in observed] == ["VOO", "MDY"]
    assert [item["rank"] for item in observed] == [1, 2]
    assert all(item["outcome"] == "eligible" for item in observed)

    decision_index = dates.index(decision_date)
    production = runtime.evaluate_production_signal(
        {
            "family_id": runtime.FLIGHT_TO_SAFETY_REBOUND_FAMILY,
            "decision_date": decision_date,
            "next_session_date": dates[decision_index + 1],
            "calendar_dates": dates[: decision_index + 1],
            "daily_history_complete": True,
            "symbols": ["MDY", "VOO", "VTI", "TLT"],
            "daily_bars": {
                symbol: bars[: decision_index + 1]
                for symbol, bars in raw["daily_bars"].items()
            },
        },
        family_id=runtime.FLIGHT_TO_SAFETY_REBOUND_FAMILY,
        parameters=_parameters(),
        frozen_universe={
            "symbols": ["MDY", "VOO", "VTI", "TLT"],
            "target_symbols": ["MDY", "VOO", "VTI"],
            "feature_symbols": ["TLT"],
            "point_in_time": True,
        },
    )
    assert production["symbol"] == "VOO"
    assert production["expected_gross_move_fraction"] == pytest.approx(
        0.02
    )
    assert production["overnight_hold"] is True


def test_treasury_feature_is_required_and_never_traded():
    raw, _dates, decision_date = _dataset()
    for bar in raw["daily_bars"]["TLT"]:
        if bar["date"] == decision_date:
            bar["close"] = float(bar["open"]) * 0.99
            bar["low"] = min(float(bar["low"]), float(bar["close"]) - 0.5)
    candidates = runtime.build_candidates(
        runtime.prepare_dataset(raw),
        runtime.FLIGHT_TO_SAFETY_REBOUND_FAMILY,
        _parameters(),
    )
    assert not [
        item
        for item in candidates
        if item["decision_date"] == decision_date
    ]
    assert all(item["symbol"] != "TLT" for item in candidates)


def test_contract_freezes_current_week_slot_and_untouched_partitions(
    tmp_path, monkeypatch
):
    original_repo_path = rebound._repo_path

    def repo_path(path):
        try:
            return original_repo_path(path)
        except ValueError:
            return f"strategy_tournament/v2/test/{path.name}"

    monkeypatch.setattr(rebound, "DEFAULT_ROOT", tmp_path)
    monkeypatch.setattr(rebound, "_repo_path", repo_path)
    monkeypatch.setattr(
        rebound.outcome_exposure,
        "read_index",
        lambda *_args, **_kwargs: [],
    )
    monkeypatch.setattr(
        rebound.outcome_exposure,
        "audit",
        lambda *_args, **_kwargs: {"index_sha256": "0" * 64},
    )
    monkeypatch.setattr(
        rebound,
        "_weekly_slots",
        lambda *_args, **_kwargs: [
            {
                "family_id": "existing-new-family",
                "created_at": datetime.now().astimezone().isoformat(),
                "path": "existing.json",
                "file_sha256": "1" * 64,
            }
        ],
    )

    _path, contract, capacity = rebound.freeze_contract(
        created_at=datetime.now().astimezone().isoformat(),
        enforce_commit=False,
    )

    assert capacity.is_file()
    assert contract["new_mechanism_family_slot_consumed"] is True
    assert contract["weekly_new_family_slot"] == 2
    assert len(contract["trial_family"]) == 32
    assert len(contract["development_dates"]) == 251
    assert len(contract["development_signal_dates"]) == 246
    assert contract["embargo_dates"] == [
        "2022-01-03",
        "2022-01-04",
        "2022-01-05",
        "2022-01-06",
        "2022-01-07",
    ]
    assert len(contract["confirmation_dates"]) == 245
    assert contract["confirmation_signal_capacity"] == 240
    assert contract["universe"]["feature_symbols"] == ["TLT"]

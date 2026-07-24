from __future__ import annotations

from datetime import date, datetime, timedelta

import pytest

import breadth_capitulation_rebound as rebound
import dense_strategy_runtime as runtime


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
        "minimum_declining_symbols": 4,
        "minimum_median_decline_fraction": 0.005,
        "minimum_target_decline_fraction": 0.01,
        "stop_atr14": 1.0,
        "maximum_hold_sessions": 2,
    }


def _dataset() -> tuple[dict, list[str], str]:
    dates = _business_dates(230)
    evaluation = dates[-12:]
    decision = evaluation[3]
    shocks = {
        "ITOT": -0.012,
        "IWB": -0.011,
        "RSP": -0.016,
        "SCHX": -0.009,
        "SPTM": -0.013,
        "VV": -0.02,
    }
    dataset = {
        "schema_version": 1,
        "family_id": runtime.BREADTH_CAPITULATION_REBOUND_FAMILY,
        "evaluation_dates": evaluation,
        "symbols": list(runtime.BREADTH_CAPITULATION_SYMBOLS),
        "daily_bars": {
            symbol: _bars(
                dates, shock_date=decision, shock_return=shock
            )
            for symbol, shock in shocks.items()
        },
    }
    return dataset, dates, decision


def test_historical_and_production_rank_same_breadth_rebound():
    raw, dates, decision = _dataset()
    candidates = runtime.build_candidates(
        runtime.prepare_dataset(raw),
        runtime.BREADTH_CAPITULATION_REBOUND_FAMILY,
        _parameters(),
    )
    observed = [
        item for item in candidates if item["decision_date"] == decision
    ]
    assert [item["symbol"] for item in observed] == [
        "VV",
        "RSP",
        "SPTM",
        "ITOT",
        "IWB",
    ]
    assert [item["rank"] for item in observed] == [1, 2, 3, 4, 5]

    decision_index = dates.index(decision)
    production = runtime.evaluate_production_signal(
        {
            "family_id": runtime.BREADTH_CAPITULATION_REBOUND_FAMILY,
            "decision_date": decision,
            "next_session_date": dates[decision_index + 1],
            "calendar_dates": dates[: decision_index + 1],
            "daily_history_complete": True,
            "symbols": list(runtime.BREADTH_CAPITULATION_SYMBOLS),
            "daily_bars": {
                symbol: bars[: decision_index + 1]
                for symbol, bars in raw["daily_bars"].items()
            },
        },
        family_id=runtime.BREADTH_CAPITULATION_REBOUND_FAMILY,
        parameters=_parameters(),
        frozen_universe={
            "symbols": list(runtime.BREADTH_CAPITULATION_SYMBOLS),
            "point_in_time": True,
        },
    )
    assert production["symbol"] == "VV"
    assert production["expected_gross_move_fraction"] == pytest.approx(
        0.02
    )
    assert production["overnight_hold"] is True


def test_breadth_gate_rejects_an_isolated_selloff():
    raw, _dates, decision = _dataset()
    for symbol in ("ITOT", "IWB", "RSP", "SCHX", "SPTM"):
        for bar in raw["daily_bars"][symbol]:
            if bar["date"] == decision:
                bar["close"] = float(bar["open"]) * 1.001
                bar["high"] = max(
                    float(bar["high"]), float(bar["close"]) + 0.5
                )
    candidates = runtime.build_candidates(
        runtime.prepare_dataset(raw),
        runtime.BREADTH_CAPITULATION_REBOUND_FAMILY,
        _parameters(),
    )
    assert not [
        item for item in candidates if item["decision_date"] == decision
    ]


def test_contract_consumes_final_weekly_slot_on_untouched_evidence(
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
                "family_id": f"existing-{index}",
                "created_at": datetime.now().astimezone().isoformat(),
                "path": f"existing-{index}.json",
                "file_sha256": str(index) * 64,
            }
            for index in (1, 2)
        ],
    )

    _path, contract, capacity = rebound.freeze_contract(
        created_at=datetime.now().astimezone().isoformat(),
        enforce_commit=False,
    )

    assert capacity.is_file()
    assert contract["weekly_new_family_slot"] == 3
    assert contract["new_mechanism_family_slot_consumed"] is True
    assert len(contract["trial_family"]) == 32
    assert len(contract["development_dates"]) == 251
    assert len(contract["development_signal_dates"]) == 246
    assert contract["embargo_dates"] == [
        "2021-01-04",
        "2021-01-05",
        "2021-01-06",
        "2021-01-07",
        "2021-01-08",
    ]
    assert len(contract["confirmation_dates"]) == 246
    assert contract["confirmation_signal_capacity"] == 241

from __future__ import annotations

from datetime import date, datetime, timedelta

import dense_strategy_runtime as runtime
import etf_ibs_reversal as ibs


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
    decision_date: str,
    decline: float,
    internal_bar_strength: float,
) -> list[dict[str, float | str]]:
    rows: list[dict[str, float | str]] = []
    previous = 100.0
    for day in dates:
        close = (
            previous * (1 + decline)
            if day == decision_date
            else previous * 1.002
        )
        if day == decision_date:
            low = close - 1.0
            high = low + 1.0 / internal_bar_strength
        else:
            low = min(previous, close) - 0.25
            high = max(previous, close) + 0.25
        rows.append(
            {
                "date": day,
                "open": previous,
                "high": high,
                "low": low,
                "close": close,
                "volume": 1_000_000,
            }
        )
        previous = close
    return rows


def _parameters() -> dict[str, float | int]:
    return {
        "internal_bar_strength_maximum": 0.2,
        "maximum_hold_sessions": 2,
        "one_session_decline_fraction": 0.005,
        "stop_atr14": 1.0,
        "trend_sma": 100,
    }


def test_ibs_runtime_ranks_lowest_completed_closing_location():
    dates = _business_dates(230)
    evaluation = dates[-12:]
    decision = evaluation[3]
    bars = {
        symbol: _bars(
            dates,
            decision_date=decision,
            decline=(-0.012 if symbol in {"EEM", "VTI"} else 0.001),
            internal_bar_strength=(
                0.08
                if symbol == "VTI"
                else 0.16
                if symbol == "EEM"
                else 0.5
            ),
        )
        for symbol in ibs.SYMBOLS
    }
    raw = {
        "schema_version": 1,
        "family_id": runtime.ETF_IBS_REVERSAL_FAMILY,
        "evaluation_dates": evaluation,
        "symbols": list(ibs.SYMBOLS),
        "daily_bars": bars,
    }
    candidates = runtime.build_candidates(
        runtime.prepare_dataset(raw),
        runtime.ETF_IBS_REVERSAL_FAMILY,
        _parameters(),
    )
    observed = [
        item for item in candidates if item["decision_date"] == decision
    ]
    assert [item["symbol"] for item in observed] == ["VTI", "EEM"]
    assert observed[0]["internal_bar_strength"] < observed[1][
        "internal_bar_strength"
    ]


def test_ibs_production_rebuilds_the_historical_rank():
    dates = _business_dates(230)
    decision = dates[-4]
    bars = {
        symbol: _bars(
            dates,
            decision_date=decision,
            decline=(-0.012 if symbol in {"EEM", "VTI"} else 0.001),
            internal_bar_strength=(
                0.08
                if symbol == "VTI"
                else 0.16
                if symbol == "EEM"
                else 0.5
            ),
        )
        for symbol in ibs.SYMBOLS
    }
    decision_index = dates.index(decision)
    production = runtime.evaluate_production_signal(
        {
            "family_id": runtime.ETF_IBS_REVERSAL_FAMILY,
            "decision_date": decision,
            "next_session_date": dates[decision_index + 1],
            "calendar_dates": dates[: decision_index + 1],
            "daily_history_complete": True,
            "symbols": list(ibs.SYMBOLS),
            "daily_bars": {
                symbol: rows[: decision_index + 1]
                for symbol, rows in bars.items()
            },
        },
        family_id=runtime.ETF_IBS_REVERSAL_FAMILY,
        parameters=_parameters(),
        frozen_universe={
            "symbols": list(ibs.SYMBOLS),
            "point_in_time": True,
        },
    )
    assert production["symbol"] == "VTI"
    assert production["holding_trading_days"] == 2


def test_ibs_contract_freezes_disjoint_completed_history(
    tmp_path, monkeypatch
):
    original_repo_path = ibs._repo_path

    def repo_path(path):
        try:
            return original_repo_path(path)
        except ValueError:
            return f"strategy_tournament/v2/test/{path.name}"

    monkeypatch.setattr(ibs, "DEFAULT_ROOT", tmp_path)
    monkeypatch.setattr(ibs, "_repo_path", repo_path)
    monkeypatch.setattr(
        ibs.outcome_exposure,
        "read_index",
        lambda *_args, **_kwargs: [],
    )
    monkeypatch.setattr(
        ibs.outcome_exposure,
        "audit",
        lambda *_args, **_kwargs: {"index_sha256": "0" * 64},
    )

    _path, contract, capacity = ibs.freeze_contract(
        created_at=datetime.now().astimezone().isoformat(),
        enforce_commit=False,
    )

    assert capacity.is_file()
    assert len(contract["trial_family"]) == 32
    assert contract["new_mechanism_family_slot_consumed"] is True
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
    assert contract["confirmation_dates"][-1] == "2014-10-28"
    assert contract["confirmation_signal_capacity"] == 498
    assert contract["historical_data_contract"]["daily_provider"] == "yahoo"

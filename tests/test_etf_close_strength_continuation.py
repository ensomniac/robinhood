from __future__ import annotations

from datetime import date, datetime, timedelta

import dense_strategy_runtime as runtime
import etf_close_strength_continuation as close_strength


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
    advance: float,
    close_location: float,
) -> list[dict[str, float | str]]:
    rows: list[dict[str, float | str]] = []
    previous = 100.0
    for day in dates:
        close = (
            previous * (1 + advance)
            if day == decision_date
            else previous * 1.002
        )
        if day == decision_date:
            low = previous - 0.25
            high = low + (close - low) / close_location
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
        "close_location_minimum": 0.8,
        "maximum_hold_sessions": 2,
        "one_session_advance_fraction": 0.005,
        "stop_atr14": 1.0,
        "trend_sma": 100,
    }


def test_close_strength_runtime_ranks_highest_completed_close():
    dates = _business_dates(230)
    evaluation = dates[-12:]
    decision = evaluation[3]
    bars = {
        symbol: _bars(
            dates,
            decision_date=decision,
            advance=(0.012 if symbol in {"EWC", "EWJ"} else 0.001),
            close_location=(
                0.94
                if symbol == "EWJ"
                else 0.86
                if symbol == "EWC"
                else 0.5
            ),
        )
        for symbol in close_strength.SYMBOLS
    }
    raw = {
        "schema_version": 1,
        "family_id": (
            runtime.ETF_CLOSE_STRENGTH_CONTINUATION_FAMILY
        ),
        "evaluation_dates": evaluation,
        "symbols": list(close_strength.SYMBOLS),
        "daily_bars": bars,
    }
    candidates = runtime.build_candidates(
        runtime.prepare_dataset(raw),
        runtime.ETF_CLOSE_STRENGTH_CONTINUATION_FAMILY,
        _parameters(),
    )
    observed = [
        item
        for item in candidates
        if item["decision_date"] == decision
    ]
    assert [item["symbol"] for item in observed] == ["EWJ", "EWC"]
    assert observed[0]["close_location"] > observed[1][
        "close_location"
    ]
    assert all(
        item["signal_date"] > item["decision_date"]
        for item in observed
    )


def test_close_strength_production_rebuilds_historical_rank():
    dates = _business_dates(230)
    decision = dates[-4]
    bars = {
        symbol: _bars(
            dates,
            decision_date=decision,
            advance=(0.012 if symbol in {"EWC", "EWJ"} else 0.001),
            close_location=(
                0.94
                if symbol == "EWJ"
                else 0.86
                if symbol == "EWC"
                else 0.5
            ),
        )
        for symbol in close_strength.SYMBOLS
    }
    decision_index = dates.index(decision)
    production = runtime.evaluate_production_signal(
        {
            "family_id": (
                runtime.ETF_CLOSE_STRENGTH_CONTINUATION_FAMILY
            ),
            "decision_date": decision,
            "next_session_date": dates[decision_index + 1],
            "calendar_dates": dates[: decision_index + 1],
            "daily_history_complete": True,
            "symbols": list(close_strength.SYMBOLS),
            "daily_bars": {
                symbol: rows[: decision_index + 1]
                for symbol, rows in bars.items()
            },
        },
        family_id=runtime.ETF_CLOSE_STRENGTH_CONTINUATION_FAMILY,
        parameters=_parameters(),
        frozen_universe={
            "symbols": list(close_strength.SYMBOLS),
            "point_in_time": True,
        },
    )
    assert production["symbol"] == "EWJ"
    assert production["holding_trading_days"] == 2
    assert production["overnight_hold"] is True


def test_close_strength_contract_freezes_untouched_history(
    tmp_path, monkeypatch
):
    original_repo_path = close_strength._repo_path

    def repo_path(path):
        try:
            return original_repo_path(path)
        except ValueError:
            return f"strategy_tournament/v2/test/{path.name}"

    monkeypatch.setattr(close_strength, "DEFAULT_ROOT", tmp_path)
    monkeypatch.setattr(close_strength, "_repo_path", repo_path)
    monkeypatch.setattr(
        close_strength.outcome_exposure,
        "read_index",
        lambda *_args, **_kwargs: [],
    )
    monkeypatch.setattr(
        close_strength.outcome_exposure,
        "audit",
        lambda *_args, **_kwargs: {
            "index_sha256": "0" * 64
        },
    )

    _path, contract, capacity = close_strength.freeze_contract(
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
    assert contract["historical_data_contract"][
        "daily_provider"
    ] == "yahoo"

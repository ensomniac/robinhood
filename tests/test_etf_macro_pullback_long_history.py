from __future__ import annotations

from datetime import date, datetime, timedelta

import dense_strategy_runtime as runtime
import etf_macro_pullback_long_history as replication


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
    pullback_start: int | None = None,
) -> list[dict[str, float | int | str]]:
    result: list[dict[str, float | int | str]] = []
    close = 100.0
    for index, day in enumerate(dates):
        change = (
            -0.012
            if pullback_start is not None
            and pullback_start <= index < pullback_start + 3
            else 0.002
        )
        opening = close
        close *= 1 + change
        result.append(
            {
                "date": day,
                "open": opening,
                "high": max(opening, close) + 0.5,
                "low": min(opening, close) - 0.5,
                "close": close,
                "volume": 2_000_000,
            }
        )
    return result


def _parameters() -> dict[str, float | int]:
    return {
        "trend_sma": 100,
        "rsi2_maximum": 10,
        "three_session_decline_fraction": 0.03,
        "stop_atr14": 1.5,
        "maximum_hold_sessions": 3,
    }


def _dataset() -> dict:
    dates = _business_dates(230)
    decision_index = 220
    symbols = list(runtime.ETF_PULLBACK_REPLICATION_V6_SYMBOLS)
    return {
        "schema_version": 1,
        "family_id": runtime.ETF_PULLBACK_REPLICATION_V6_FAMILY,
        "evaluation_dates": dates[decision_index : decision_index + 8],
        "symbols": symbols,
        "daily_bars": {
            symbol: _bars(
                dates,
                pullback_start=(
                    decision_index - 2 if symbol == "SLV" else None
                ),
            )
            for symbol in symbols
        },
    }


def test_v6_runtime_preserves_exact_pullback_and_production_rules():
    dataset = _dataset()
    decision = dataset["evaluation_dates"][0]
    candidates = runtime.build_candidates(
        runtime.prepare_dataset(dataset),
        runtime.ETF_PULLBACK_REPLICATION_V6_FAMILY,
        _parameters(),
    )
    first = next(
        row
        for row in candidates
        if row["decision_date"] == decision and row["rank"] == 1
    )
    assert first["symbol"] == "SLV"
    assert first["signal_date"] == dataset["evaluation_dates"][1]

    all_dates = [
        row["date"] for row in dataset["daily_bars"]["AGG"]
    ]
    decision_index = all_dates.index(decision)
    production = runtime.evaluate_production_signal(
        {
            "family_id": runtime.ETF_PULLBACK_REPLICATION_V6_FAMILY,
            "decision_date": decision,
            "next_session_date": dataset["evaluation_dates"][1],
            "calendar_dates": all_dates[: decision_index + 1],
            "daily_history_complete": True,
            "symbols": dataset["symbols"],
            "daily_bars": {
                symbol: bars[: decision_index + 1]
                for symbol, bars in dataset["daily_bars"].items()
            },
        },
        family_id=runtime.ETF_PULLBACK_REPLICATION_V6_FAMILY,
        parameters=_parameters(),
        frozen_universe={
            "symbols": dataset["symbols"],
            "point_in_time": True,
        },
    )
    assert production["symbol"] == "SLV"
    assert production["holding_trading_days"] == 3
    assert production["overnight_hold"] is True


def test_v6_contract_keeps_grid_and_disjoint_long_history(
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
    base = replication._read_contract(
        replication.PREDECESSOR_CONTRACT
    )

    assert capacity.is_file()
    assert contract["parameter_grid"] == base["parameter_grid"]
    assert len(contract["trial_family"]) == 32
    assert contract["new_mechanism_family_slot_consumed"] is False
    assert contract["prior_family_attempt_count"] == 6
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
    assert contract["confirmation_signal_capacity"] == 495
    assert contract["universe"]["symbols"] == list(
        runtime.ETF_PULLBACK_REPLICATION_V6_SYMBOLS
    )
    assert contract["predecessor"]["parameter_grid_changed"] is False
    assert contract["historical_data_contract"]["daily_provider"] == (
        "yahoo"
    )

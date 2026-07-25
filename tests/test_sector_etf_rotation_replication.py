from __future__ import annotations

from datetime import date, datetime, timedelta

import dense_strategy_runtime as runtime
import sector_etf_rotation_replication as replication


def _business_dates(count: int) -> list[str]:
    result: list[str] = []
    current = date(2023, 1, 3)
    while len(result) < count:
        if current.weekday() < 5:
            result.append(current.isoformat())
        current += timedelta(days=1)
    return result


def _bars(
    dates: list[str], daily_return: float
) -> list[dict[str, float | int | str]]:
    result: list[dict[str, float | int | str]] = []
    previous = 100.0
    for day in dates:
        close = previous * (1 + daily_return)
        result.append(
            {
                "date": day,
                "open": previous,
                "high": max(previous, close) + 0.5,
                "low": min(previous, close) - 0.5,
                "close": close,
                "volume": 2_000_000,
            }
        )
        previous = close
    return result


def _parameters() -> dict[str, float | int]:
    return {
        "return_lookback_sessions": 20,
        "minimum_excess_return_fraction": 0.01,
        "market_trend_sma": 20,
        "stop_atr14": 1.5,
        "maximum_hold_sessions": 3,
    }


def _dataset() -> dict:
    dates = _business_dates(230)
    symbols = [
        runtime.SECTOR_ETF_ROTATION_REPLICATION_BENCHMARK,
        *runtime.SECTOR_ETF_ROTATION_REPLICATION_TARGET_SYMBOLS,
    ]
    return {
        "schema_version": 1,
        "family_id": runtime.SECTOR_ETF_ROTATION_REPLICATION_FAMILY,
        "evaluation_dates": dates[-12:],
        "symbols": symbols,
        "daily_bars": {
            symbol: _bars(
                dates,
                0.003
                if symbol == "VGT"
                else 0.001
                if symbol != "IWB"
                else 0.0005,
            )
            for symbol in symbols
        },
    }


def test_long_history_replication_preserves_sector_rotation_rules():
    dataset = _dataset()
    candidates = runtime.build_candidates(
        runtime.prepare_dataset(dataset),
        runtime.SECTOR_ETF_ROTATION_REPLICATION_FAMILY,
        _parameters(),
    )
    decision = dataset["evaluation_dates"][0]
    first = next(
        row
        for row in candidates
        if row["decision_date"] == decision and row["rank"] == 1
    )
    assert first["symbol"] == "VGT"
    assert first["signal_date"] == dataset["evaluation_dates"][1]
    assert first["expected_gross_move_fraction"] >= 0.01

    decision_index = [
        row["date"] for row in dataset["daily_bars"]["IWB"]
    ].index(decision)
    production = runtime.evaluate_production_signal(
        {
            "family_id": (
                runtime.SECTOR_ETF_ROTATION_REPLICATION_FAMILY
            ),
            "decision_date": decision,
            "next_session_date": dataset["evaluation_dates"][1],
            "calendar_dates": [
                row["date"]
                for row in dataset["daily_bars"]["IWB"][
                    : decision_index + 1
                ]
            ],
            "daily_history_complete": True,
            "symbols": dataset["symbols"],
            "daily_bars": {
                symbol: bars[: decision_index + 1]
                for symbol, bars in dataset["daily_bars"].items()
            },
        },
        family_id=runtime.SECTOR_ETF_ROTATION_REPLICATION_FAMILY,
        parameters=_parameters(),
        frozen_universe={
            "symbols": dataset["symbols"],
            "target_symbols": list(
                runtime.SECTOR_ETF_ROTATION_REPLICATION_TARGET_SYMBOLS
            ),
            "feature_symbols": ["IWB"],
            "point_in_time": True,
        },
    )
    assert production["symbol"] == "VGT"
    assert production["holding_trading_days"] == 3
    assert production["overnight_hold"] is True


def test_contract_keeps_exact_grid_and_uses_untouched_long_history(
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
    assert contract["confirmation_signal_capacity"] == 497
    assert contract["universe"]["feature_symbols"] == ["IWB"]
    assert contract["universe"]["target_symbols"] == list(
        runtime.SECTOR_ETF_ROTATION_REPLICATION_TARGET_SYMBOLS
    )
    assert contract["predecessor"]["parameter_grid_changed"] is False
    assert contract["historical_data_contract"]["daily_provider"] == (
        "yahoo"
    )

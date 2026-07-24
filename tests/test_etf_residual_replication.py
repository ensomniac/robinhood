from __future__ import annotations

import math
from datetime import date, datetime, timedelta

import dense_strategy_runtime as runtime
import etf_residual_replication as replication


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
    phase: float,
    shock_index: int | None = None,
    shock_return: float = 0.0,
) -> list[dict[str, float | str]]:
    rows: list[dict[str, float | str]] = []
    previous = 100.0
    for index, day in enumerate(dates):
        daily_return = 0.001 + math.sin(index / 7 + phase) * 0.0005
        if index == shock_index:
            daily_return = shock_return
        close = previous * (1 + daily_return)
        rows.append(
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
    return rows


def _parameters() -> dict[str, float | int | str]:
    return {
        "prior_return_sessions": 1,
        "residual_z_threshold": -1.5,
        "market_trend_gate": "SPY>SMA100",
        "stop_atr14": 1.0,
        "hold_sessions": 2,
    }


def test_fixed_etf_residual_runtime_uses_only_frozen_targets():
    dates = _business_dates(230)
    evaluation = dates[-12:]
    decision_index = dates.index(evaluation[3])
    bars = {
        "SPY": _bars(dates, phase=0.0),
        **{
            symbol: _bars(
                dates,
                phase=(index + 1) / 3,
                shock_index=(
                    decision_index if symbol == "IWN" else None
                ),
                shock_return=-0.04,
            )
            for index, symbol in enumerate(
                runtime.ETF_RESIDUAL_REPLICATION_TARGET_SYMBOLS
            )
        },
    }
    dataset = runtime.prepare_dataset(
        {
            "schema_version": 1,
            "family_id": runtime.ETF_RESIDUAL_REPLICATION_FAMILY,
            "evaluation_dates": evaluation,
            "symbols": [
                *runtime.ETF_RESIDUAL_REPLICATION_TARGET_SYMBOLS,
                runtime.ETF_RESIDUAL_REPLICATION_FEATURE_SYMBOL,
            ],
            "daily_bars": bars,
        }
    )

    candidates = runtime.build_candidates(
        dataset,
        runtime.ETF_RESIDUAL_REPLICATION_FAMILY,
        _parameters(),
    )
    observed = [
        item
        for item in candidates
        if item["decision_date"] == evaluation[3]
    ]

    assert observed
    assert observed[0]["symbol"] == "IWN"
    assert all(
        item["symbol"]
        in runtime.ETF_RESIDUAL_REPLICATION_TARGET_SYMBOLS
        for item in candidates
    )
    assert len(dataset["_equity_residual_feature_cache"]) == 1

    production = runtime.evaluate_production_signal(
        {
            "family_id": runtime.ETF_RESIDUAL_REPLICATION_FAMILY,
            "decision_date": evaluation[3],
            "next_session_date": dates[decision_index + 1],
            "calendar_dates": dates[: decision_index + 1],
            "daily_history_complete": True,
            "symbols": [
                *runtime.ETF_RESIDUAL_REPLICATION_TARGET_SYMBOLS,
                runtime.ETF_RESIDUAL_REPLICATION_FEATURE_SYMBOL,
            ],
            "daily_bars": {
                symbol: rows[: decision_index + 1]
                for symbol, rows in bars.items()
            },
        },
        family_id=runtime.ETF_RESIDUAL_REPLICATION_FAMILY,
        parameters=_parameters(),
        frozen_universe={
            "symbols": [
                *runtime.ETF_RESIDUAL_REPLICATION_TARGET_SYMBOLS,
                runtime.ETF_RESIDUAL_REPLICATION_FEATURE_SYMBOL,
            ],
            "target_symbols": list(
                runtime.ETF_RESIDUAL_REPLICATION_TARGET_SYMBOLS
            ),
            "feature_symbols": [
                runtime.ETF_RESIDUAL_REPLICATION_FEATURE_SYMBOL
            ],
            "point_in_time": True,
        },
    )
    assert production["symbol"] == "IWN"
    assert production["overnight_hold"] is True


def test_fixed_etf_residual_contract_preserves_grid_and_disjoint_dates(
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
    base, _inspection = replication._predecessor_graph(
        enforce_commit=False
    )

    assert capacity.is_file()
    assert contract["parameter_grid"] == base["parameter_grid"]
    assert len(contract["trial_family"]) == 48
    assert len(contract["development_dates"]) == 746
    assert len(contract["development_signal_dates"]) == 741
    assert len(contract["embargo_dates"]) == 251
    assert len(contract["confirmation_dates"]) == 251
    assert contract["confirmation_signal_capacity"] == 246
    assert contract["new_mechanism_family_slot_consumed"] is False
    assert contract["development_scope"]["symbols"] == list(
        runtime.ETF_RESIDUAL_REPLICATION_TARGET_SYMBOLS
    )
    assert "SPY" not in contract["development_scope"]["symbols"]

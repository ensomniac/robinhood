from __future__ import annotations

from datetime import date, datetime, timedelta

import dense_strategy_runtime as runtime
import style_etf_breakout_discovery as discovery


def _business_dates(count: int) -> list[str]:
    result: list[str] = []
    current = date(2022, 1, 3)
    while len(result) < count:
        if current.weekday() < 5:
            result.append(current.isoformat())
        current += timedelta(days=1)
    return result


def _bars(
    dates: list[str], *, daily_change: float
) -> list[dict[str, float | str]]:
    rows: list[dict[str, float | str]] = []
    close = 100.0
    for day in dates:
        opening = close
        close += daily_change
        rows.append(
            {
                "date": day,
                "open": opening,
                "high": close + 0.8,
                "low": opening - 0.8,
                "close": close,
                "volume": 1_000_000,
            }
        )
    return rows


def _parameters() -> dict[str, float | int]:
    return {
        "breadth_sma": 100,
        "minimum_breadth_count": 6,
        "breakout_lookback_sessions": 20,
        "trend_sma": 200,
        "stop_atr14": 1.5,
        "maximum_hold_sessions": 5,
    }


def _dataset() -> tuple[dict, list[str], str]:
    dates = _business_dates(235)
    evaluation = dates[-20:]
    symbols = list(runtime.CROSS_STYLE_BREADTH_SYMBOLS)
    daily = {
        symbol: _bars(
            dates,
            daily_change=0.3 if symbol == "SCHG" else 0.2,
        )
        for symbol in symbols
    }
    return (
        {
            "schema_version": 1,
            "family_id": runtime.STYLE_ETF_BREAKOUT_CONTINUATION_FAMILY,
            "evaluation_dates": evaluation,
            "symbols": symbols,
            "daily_bars": daily,
        },
        dates,
        evaluation[-6],
    )


def test_historical_and_production_share_exact_breakout_ranking():
    raw, dates, decision = _dataset()
    candidates = runtime.build_candidates(
        runtime.prepare_dataset(raw),
        runtime.STYLE_ETF_BREAKOUT_CONTINUATION_FAMILY,
        _parameters(),
    )
    observed = [
        item for item in candidates if item["decision_date"] == decision
    ]
    assert len(observed) == 1
    assert observed[0]["symbol"] == "SCHG"
    assert observed[0]["breadth_count"] == 8

    decision_index = dates.index(decision)
    production = runtime.evaluate_production_signal(
        {
            "family_id": runtime.STYLE_ETF_BREAKOUT_CONTINUATION_FAMILY,
            "decision_date": decision,
            "next_session_date": dates[decision_index + 1],
            "calendar_dates": dates[: decision_index + 1],
            "daily_history_complete": True,
            "symbols": list(runtime.CROSS_STYLE_BREADTH_SYMBOLS),
            "daily_bars": {
                symbol: bars[: decision_index + 1]
                for symbol, bars in raw["daily_bars"].items()
            },
        },
        family_id=runtime.STYLE_ETF_BREAKOUT_CONTINUATION_FAMILY,
        parameters=_parameters(),
        frozen_universe={
            "symbols": list(runtime.CROSS_STYLE_BREADTH_SYMBOLS),
            "point_in_time": True,
        },
    )
    assert production["symbol"] == observed[0]["symbol"]
    assert production["breadth_count"] == observed[0]["breadth_count"]
    assert production["holding_trading_days"] == 5
    assert production["overnight_hold"] is True


def test_contract_freezes_daily_2019_development_and_2020_confirmation(
    tmp_path, monkeypatch
):
    original_repo_path = discovery._repo_path

    def repo_path(path):
        try:
            return original_repo_path(path)
        except ValueError:
            return f"strategy_tournament/v2/test/{path.name}"

    monkeypatch.setattr(discovery, "DEFAULT_ROOT", tmp_path)
    monkeypatch.setattr(discovery, "_repo_path", repo_path)
    monkeypatch.setattr(
        discovery.outcome_exposure,
        "read_index",
        lambda *_args, **_kwargs: [],
    )
    monkeypatch.setattr(
        discovery.outcome_exposure,
        "audit",
        lambda *_args, **_kwargs: {"index_sha256": "0" * 64},
    )
    monkeypatch.setattr(
        discovery.shared,
        "_rolling_slot_authority",
        lambda *_args, **_kwargs: {
            "policy": "ROLLING_TERMINAL_REPLACEMENT",
            "active_family_count_before_freeze": 0,
            "available_slot_count_before_freeze": 3,
            "consumed_active_slot": 1,
            "authorization_path": "rolling.json",
            "authorization_sha256": "1" * 64,
            "status_path": "status.json",
            "status_file_sha256": "2" * 64,
            "all_prior_trials_retained": True,
            "all_prior_dispositions_retained": True,
        },
    )
    monkeypatch.setattr(
        discovery,
        "_predecessor_disposition",
        lambda *_args, **_kwargs: {
            "inspection_path": "predecessor.json",
            "inspection_file_sha256": "3" * 64,
            "inspection_sha256": "4" * 64,
            "state": "REJECTED",
            "slot_released": True,
            "promotion_evidence_reused": False,
        },
    )

    _path, contract, capacity = discovery.freeze_contract(
        created_at=datetime.now().astimezone().isoformat(),
        enforce_commit=False,
    )

    assert capacity.is_file()
    assert contract["rolling_active_family_slot"] == 1
    assert contract["new_mechanism_family_slot_consumed"] is True
    assert len(contract["trial_family"]) == 1
    assert len(contract["development_dates"]) == 249
    assert len(contract["development_signal_dates"]) == 244
    assert contract["embargo_dates"] == [
        "2020-01-02",
        "2020-01-03",
        "2020-01-06",
        "2020-01-07",
        "2020-01-08",
    ]
    assert len(contract["confirmation_dates"]) == 246
    assert contract["confirmation_signal_capacity"] == 241
    assert contract["historical_data_contract"]["daily_provider"] == "alpaca"

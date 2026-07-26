from datetime import date, timedelta

import dense_strategy_runtime as runtime
import flight_to_safety_replication4 as replication


def _business_dates(count):
    result = []
    current = date(2020, 1, 2)
    while len(result) < count:
        if current.weekday() < 5:
            result.append(current.isoformat())
        current += timedelta(days=1)
    return result


def _bars(dates, *, shock_date=None, shock_return=0.0):
    value = 100.0
    rows = []
    for day in dates:
        prior = value
        value = prior * (1 + (shock_return if day == shock_date else 0.001))
        rows.append(
            {
                "date": day,
                "open": prior,
                "high": max(prior, value) * 1.003,
                "low": min(prior, value) * 0.997,
                "close": value,
                "volume": 1_000_000,
            }
        )
    return rows


def _parameters():
    return {
        "minimum_equity_decline_fraction": 0.0075,
        "minimum_tlt_return_fraction": 0.0025,
        "trend_sma": 100,
        "stop_atr14": 1.0,
        "maximum_hold_sessions": 2,
    }


def test_country_replication_partitions_are_disjoint_and_complete():
    (
        warmup,
        development,
        development_signals,
        embargo,
        confirmation_warmup,
        confirmation,
        confirmation_signals,
    ) = replication._partitions()

    assert len(warmup) == 200
    assert len(development) == 1_000
    assert len(development_signals) == 995
    assert len(embargo) == 5
    assert len(confirmation_warmup) == 200
    assert len(confirmation) == 500
    assert len(confirmation_signals) == 495
    assert development[-1] < embargo[0] < confirmation[0]


def test_country_replication_historical_and_production_paths_match():
    dates = _business_dates(230)
    evaluation = dates[-12:]
    decision = evaluation[3]
    symbols = [
        *runtime.FLIGHT_TO_SAFETY_REPLICATION_V4_TARGET_SYMBOLS,
        runtime.FLIGHT_TO_SAFETY_FEATURE_SYMBOL,
    ]
    shocks = {symbol: -0.005 for symbol in symbols}
    shocks["EWA"] = -0.015
    shocks["EWH"] = -0.010
    shocks[runtime.FLIGHT_TO_SAFETY_FEATURE_SYMBOL] = 0.01
    raw = {
        "schema_version": 1,
        "family_id": runtime.FLIGHT_TO_SAFETY_REPLICATION_V4_FAMILY,
        "evaluation_dates": evaluation,
        "symbols": symbols,
        "daily_bars": {
            symbol: _bars(
                dates,
                shock_date=decision,
                shock_return=shocks[symbol],
            )
            for symbol in symbols
        },
    }
    prepared = runtime.prepare_dataset(raw)
    candidates = runtime.build_candidates(
        prepared,
        runtime.FLIGHT_TO_SAFETY_REPLICATION_V4_FAMILY,
        _parameters(),
    )
    observed = [
        item for item in candidates if item["decision_date"] == decision
    ]
    assert [item["symbol"] for item in observed] == ["EWA", "EWH"]

    decision_index = dates.index(decision)
    production = runtime.evaluate_production_signal(
        {
            "family_id": runtime.FLIGHT_TO_SAFETY_REPLICATION_V4_FAMILY,
            "decision_date": decision,
            "next_session_date": dates[decision_index + 1],
            "calendar_dates": dates[: decision_index + 1],
            "daily_history_complete": True,
            "symbols": symbols,
            "daily_bars": {
                symbol: bars[: decision_index + 1]
                for symbol, bars in raw["daily_bars"].items()
            },
        },
        family_id=runtime.FLIGHT_TO_SAFETY_REPLICATION_V4_FAMILY,
        parameters=_parameters(),
        frozen_universe={
            "symbols": symbols,
            "target_symbols": list(
                runtime.FLIGHT_TO_SAFETY_REPLICATION_V4_TARGET_SYMBOLS
            ),
            "feature_symbols": [runtime.FLIGHT_TO_SAFETY_FEATURE_SYMBOL],
            "point_in_time": True,
        },
    )

    assert production["symbol"] == "EWA"
    assert production["overnight_hold"] is True

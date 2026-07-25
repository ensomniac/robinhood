from __future__ import annotations

from datetime import date, datetime, timedelta

import dense_strategy_runtime as runtime
import etf_abnormal_volume_continuation as volume_shock


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
    volume_multiple: float,
) -> list[dict[str, float | str]]:
    rows: list[dict[str, float | str]] = []
    previous = 100.0
    for day in dates:
        close = (
            previous * (1 + advance)
            if day == decision_date
            else previous * 1.002
        )
        rows.append(
            {
                "date": day,
                "open": previous,
                "high": max(previous, close) + 0.25,
                "low": min(previous, close) - 0.25,
                "close": close,
                "volume": (
                    1_000_000 * volume_multiple
                    if day == decision_date
                    else 1_000_000
                ),
            }
        )
        previous = close
    return rows


def _parameters() -> dict[str, float | int]:
    return {
        "maximum_hold_sessions": 2,
        "minimum_advance_fraction": 0.01,
        "minimum_volume_multiple": 1.5,
        "stop_atr14": 1.0,
        "volume_lookback_sessions": 20,
    }


def test_volume_shock_runtime_ranks_highest_completed_multiple():
    dates = _business_dates(230)
    evaluation = dates[-12:]
    decision = evaluation[3]
    bars = {
        symbol: _bars(
            dates,
            decision_date=decision,
            advance=(0.03 if symbol in {"IBB", "SMH"} else 0.001),
            volume_multiple=(
                4.0
                if symbol == "SMH"
                else 3.0
                if symbol == "IBB"
                else 1.0
            ),
        )
        for symbol in volume_shock.SYMBOLS
    }
    raw = {
        "schema_version": 1,
        "family_id": (
            runtime.ETF_ABNORMAL_VOLUME_CONTINUATION_FAMILY
        ),
        "evaluation_dates": evaluation,
        "symbols": list(volume_shock.SYMBOLS),
        "daily_bars": bars,
    }
    candidates = runtime.build_candidates(
        runtime.prepare_dataset(raw),
        runtime.ETF_ABNORMAL_VOLUME_CONTINUATION_FAMILY,
        _parameters(),
    )
    observed = [
        item
        for item in candidates
        if item["decision_date"] == decision
    ]
    assert [item["symbol"] for item in observed] == ["SMH", "IBB"]
    assert observed[0]["volume_multiple"] > observed[1][
        "volume_multiple"
    ]
    assert all(
        item["signal_date"] > item["decision_date"]
        for item in observed
    )


def test_volume_shock_production_rebuilds_historical_rank():
    dates = _business_dates(230)
    decision = dates[-7]
    bars = {
        symbol: _bars(
            dates,
            decision_date=decision,
            advance=(0.03 if symbol in {"IBB", "SMH"} else 0.001),
            volume_multiple=(
                4.0
                if symbol == "SMH"
                else 3.0
                if symbol == "IBB"
                else 1.0
            ),
        )
        for symbol in volume_shock.SYMBOLS
    }
    decision_index = dates.index(decision)
    production = runtime.evaluate_production_signal(
        {
            "family_id": (
                runtime.ETF_ABNORMAL_VOLUME_CONTINUATION_FAMILY
            ),
            "decision_date": decision,
            "next_session_date": dates[decision_index + 1],
            "calendar_dates": dates[: decision_index + 1],
            "daily_history_complete": True,
            "symbols": list(volume_shock.SYMBOLS),
            "daily_bars": {
                symbol: rows[: decision_index + 1]
                for symbol, rows in bars.items()
            },
        },
        family_id=runtime.ETF_ABNORMAL_VOLUME_CONTINUATION_FAMILY,
        parameters=_parameters(),
        frozen_universe={
            "symbols": list(volume_shock.SYMBOLS),
            "point_in_time": True,
        },
    )
    assert production["symbol"] == "SMH"
    assert production["holding_trading_days"] == 2
    assert production["overnight_hold"] is True


def test_volume_shock_contract_freezes_untouched_history(
    tmp_path, monkeypatch
):
    original_repo_path = volume_shock._repo_path

    def repo_path(path):
        try:
            return original_repo_path(path)
        except ValueError:
            return f"strategy_tournament/v2/test/{path.name}"

    monkeypatch.setattr(volume_shock, "DEFAULT_ROOT", tmp_path)
    monkeypatch.setattr(volume_shock, "_repo_path", repo_path)
    monkeypatch.setattr(
        volume_shock.outcome_exposure,
        "read_index",
        lambda *_args, **_kwargs: [],
    )
    monkeypatch.setattr(
        volume_shock.outcome_exposure,
        "audit",
        lambda *_args, **_kwargs: {
            "index_sha256": "0" * 64
        },
    )

    _path, contract, capacity = volume_shock.freeze_contract(
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
    assert contract["confirmation_signal_capacity"] == 495
    assert contract["historical_data_contract"][
        "daily_provider"
    ] == "yahoo"

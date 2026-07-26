from __future__ import annotations

from datetime import date, timedelta

import pytest

import dense_strategy_runtime as runtime


def _days(count: int) -> list[str]:
    start = date(2024, 1, 2)
    return [
        (start + timedelta(days=index)).isoformat()
        for index in range(count)
    ]


def _bar(day: str, close: float) -> dict[str, float | int | str]:
    return {
        "date": day,
        "open": close,
        "high": close + 1.0,
        "low": close - 1.0,
        "close": close,
        "volume": 2_000_000,
    }


def _event(day: str, *, accepted: str) -> dict[str, float | str]:
    return {
        "adsh": "0000000001-24-000001",
        "symbol": "EDGE",
        "accepted": accepted,
        "accepted_date": accepted[:10],
        "report_period": "20240930",
        "reaction_date": day,
        "current_eps": 1.5,
        "prior_eps": 1.0,
        "eps_change": 0.5,
        "eps_change_ratio": 0.5,
        "security_identity_state": "VERIFIED_COMMON_EQUITY_COVER_FACT",
    }


def _parameters() -> dict[str, float | int]:
    return {
        "minimum_yoy_eps_change_ratio": 0.25,
        "minimum_opening_gap_fraction": 0.0,
        "stop_atr14": 1.0,
        "maximum_hold_sessions": 2,
    }


def test_broad_sec_pead_enters_first_observable_open() -> None:
    days = _days(212)
    entry_index = 205
    bars = [
        _bar(day, 100.0 + index * 0.05)
        for index, day in enumerate(days)
    ]
    entry_open = float(bars[entry_index - 1]["close"]) * 1.01
    bars[entry_index]["open"] = entry_open
    bars[entry_index]["high"] = entry_open + 1.0
    evaluation = days[entry_index : entry_index + 5]
    metadata = {day: [] for day in evaluation}
    metadata[evaluation[0]] = [
        _event(
            evaluation[0],
            accepted=f"{evaluation[0]} 08:00:00",
        )
    ]
    dataset = runtime.prepare_dataset(
        {
            "family_id": runtime.SEC_BROAD_PEAD_FAMILY,
            "evaluation_dates": evaluation,
            "event_metadata_by_date": metadata,
            "daily_bars": {"EDGE": bars},
        }
    )

    candidates = runtime.build_candidates(
        dataset,
        runtime.SEC_BROAD_PEAD_FAMILY,
        _parameters(),
    )

    assert len(candidates) == 1
    assert candidates[0]["outcome"] == "eligible"
    assert candidates[0]["decision_date"] == evaluation[0]
    assert candidates[0]["signal_date"] == evaluation[0]
    assert candidates[0]["entry_price"] == pytest.approx(
        bars[entry_index]["open"]
    )
    assert candidates[0]["exit_date"] <= evaluation[1]


def test_broad_sec_pead_rejects_after_cutoff_same_day_filing() -> None:
    days = _days(212)
    entry_index = 205
    bars = [
        _bar(day, 100.0 + index * 0.05)
        for index, day in enumerate(days)
    ]
    evaluation = days[entry_index : entry_index + 5]
    metadata = {day: [] for day in evaluation}
    metadata[evaluation[0]] = [
        _event(
            evaluation[0],
            accepted=f"{evaluation[0]} 09:25:00",
        )
    ]
    dataset = {
        "family_id": runtime.SEC_BROAD_PEAD_FAMILY,
        "evaluation_dates": evaluation,
        "event_metadata_by_date": metadata,
        "daily_bars": {"EDGE": bars},
    }

    with pytest.raises(
        runtime.DenseStrategyRuntimeError,
        match="identity or observation date drifted",
    ):
        runtime.build_candidates(
            dataset,
            runtime.SEC_BROAD_PEAD_FAMILY,
            _parameters(),
        )


def test_broad_sec_pead_production_rebuilds_same_rank() -> None:
    days = _days(205)
    bars = [
        _bar(day, 100.0 + index * 0.05)
        for index, day in enumerate(days)
    ]
    previous = days[-1]
    decision = (date.fromisoformat(previous) + timedelta(days=1)).isoformat()
    event = {
        **_event(
            decision,
            accepted=f"{decision} 08:00:00",
        ),
        "opening_price": float(bars[-1]["close"]) * 1.01,
    }
    facts = {
        "family_id": runtime.SEC_BROAD_PEAD_FAMILY,
        "decision_date": decision,
        "previous_session_date": previous,
        "calendar_dates": days,
        "daily_history_complete": True,
        "event_inventory_complete": True,
        "daily_bars": {"EDGE": bars},
        "events": [event],
    }
    universe = {
        "point_in_time": True,
        "security_type": "SEC same-accession verified common equity",
        "excluded_symbols": ["AGN", "EQIX"],
    }

    signal = runtime.evaluate_production_signal(
        facts,
        family_id=runtime.SEC_BROAD_PEAD_FAMILY,
        parameters=_parameters(),
        frozen_universe=universe,
    )

    assert signal["symbol"] == "EDGE"
    assert signal["next_session_date"] == decision
    assert signal["holding_trading_days"] == 2
    assert signal["expected_gross_move_fraction"] > 0

    facts["daily_bars"]["EDGE"] = [
        *bars,
        _bar(decision, 120.0),
    ]
    with pytest.raises(
        runtime.DenseStrategyRuntimeError,
        match="current/future bars",
    ):
        runtime.evaluate_production_signal(
            facts,
            family_id=runtime.SEC_BROAD_PEAD_FAMILY,
            parameters=_parameters(),
            frozen_universe=universe,
        )

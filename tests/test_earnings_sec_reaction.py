from __future__ import annotations

from datetime import date, timedelta

import pytest

import dense_strategy_runtime as runtime
import earnings_sec_reaction_collection as collection
import earnings_sec_reaction_search as search
import earnings_sec_yahoo_data as yahoo


def _days(count: int) -> list[str]:
    start = date(2010, 1, 1)
    return [
        (start + timedelta(days=index)).isoformat()
        for index in range(count)
    ]


def _bar(day: str, close: float) -> dict:
    return {
        "date": day,
        "open": close - 0.10,
        "high": close + 1.0,
        "low": close - 1.0,
        "close": close,
        "volume": 2_000_000,
    }


def _runtime_fixture() -> tuple[dict, dict, int]:
    days = _days(30)
    bars = [_bar(day, 100.0 + index * 0.10) for index, day in enumerate(days)]
    reaction_index = 22
    prior_close = float(bars[reaction_index - 1]["close"])
    bars[reaction_index].update(
        {
            "open": prior_close * 1.02,
            "high": prior_close * 1.05,
            "low": prior_close * 1.01,
            "close": prior_close * 1.04,
            "volume": 3_000_000,
        }
    )
    entry_open = prior_close * 1.045
    bars[reaction_index + 1].update(
        {
            "open": entry_open,
            "high": max(
                entry_open, float(bars[reaction_index + 1]["close"])
            )
            + 1.0,
            "low": min(
                entry_open, float(bars[reaction_index + 1]["close"])
            )
            - 1.0,
        }
    )
    evaluation = days[reaction_index : reaction_index + 6]
    metadata = {day: [] for day in evaluation}
    metadata[evaluation[0]] = [
        {
            "adsh": "0000000001-10-000001",
            "symbol": "EDGE",
            "accepted": f"{evaluation[0]} 08:00:00",
            "accepted_date": evaluation[0],
            "report_period": "20091231",
            "reaction_date": evaluation[0],
            "current_eps": 1.5,
            "prior_eps": 1.0,
            "eps_change": 0.5,
            "eps_change_ratio": 0.5,
            "security_identity_state": (
                "VERIFIED_COMMON_EQUITY_COVER_FACT"
            ),
        }
    ]
    return (
        {
            "family_id": runtime.EARNINGS_SEC_REACTION_FAMILY,
            "evaluation_dates": evaluation,
            "event_metadata_by_date": metadata,
            "daily_bars": {"EDGE": bars},
        },
        {
            "minimum_yoy_eps_change_ratio": 0.25,
            "minimum_reaction_opening_gap_fraction": 0.01,
            "reaction_confirmation": "close>open",
            "stop_atr14": 1.0,
            "maximum_hold_sessions": 2,
        },
        reaction_index,
    )


def test_sec_reaction_enters_next_open_after_completed_reaction() -> None:
    dataset, parameters, reaction_index = _runtime_fixture()

    candidates = runtime.build_candidates(
        dataset,
        runtime.EARNINGS_SEC_REACTION_FAMILY,
        parameters,
    )

    assert len(candidates) == 1
    candidate = candidates[0]
    bars = dataset["daily_bars"]["EDGE"]
    assert candidate["decision_date"] == dataset["evaluation_dates"][0]
    assert candidate["signal_date"] == dataset["evaluation_dates"][1]
    assert candidate["entry_price"] == pytest.approx(
        bars[reaction_index + 1]["open"]
    )
    assert candidate["entry_price"] != pytest.approx(
        bars[reaction_index]["close"]
    )
    assert candidate["exit_date"] <= dataset["evaluation_dates"][2]


def test_sec_reaction_production_rebuilds_rank_and_fails_on_future_data() -> None:
    dataset, parameters, reaction_index = _runtime_fixture()
    decision_date = dataset["evaluation_dates"][0]
    next_session = dataset["evaluation_dates"][1]
    event = dataset["event_metadata_by_date"][decision_date][0]
    decision = {
        "family_id": runtime.EARNINGS_SEC_REACTION_FAMILY,
        "decision_date": decision_date,
        "next_session_date": next_session,
        "calendar_dates": [
            row["date"]
            for row in dataset["daily_bars"]["EDGE"][: reaction_index + 1]
        ],
        "daily_history_complete": True,
        "daily_bars": {
            "EDGE": dataset["daily_bars"]["EDGE"][: reaction_index + 1]
        },
        "events": [event],
    }
    universe = {
        "point_in_time": True,
        "security_type": "SEC same-accession verified common equity",
        "excluded_symbols": ["AAPL", "ADSK", "ALGN", "AMZN", "ANN"],
    }

    signal = runtime.evaluate_production_signal(
        decision,
        family_id=runtime.EARNINGS_SEC_REACTION_FAMILY,
        parameters=parameters,
        frozen_universe=universe,
    )

    assert signal["symbol"] == "EDGE"
    assert signal["decision_date"] == decision_date
    assert signal["next_session_date"] == next_session
    assert signal["holding_trading_days"] == 2

    decision["daily_bars"]["EDGE"] = dataset["daily_bars"]["EDGE"][
        : reaction_index + 2
    ]
    with pytest.raises(
        runtime.DenseStrategyRuntimeError,
        match="post-decision bars",
    ):
        runtime.evaluate_production_signal(
            decision,
            family_id=runtime.EARNINGS_SEC_REACTION_FAMILY,
            parameters=parameters,
            frozen_universe=universe,
        )


def test_successor_selection_excludes_every_pre_search_symbol() -> None:
    selected = search.selection()

    assert selected["development_event_count"] == 148
    assert len(selected["development_signal_dates"]) == 82
    assert len(selected["development_symbols"]) == 109
    assert len(selected["development_requests"]) == 109
    assert selected["excluded_pre_search_symbols"] == [
        "AAPL",
        "ADSK",
        "ALGN",
        "AMZN",
        "ANN",
    ]
    assert not set(selected["excluded_pre_search_symbols"]).intersection(
        selected["development_symbols"]
    )
    assert selected["confirmation_event_count"] == 229
    assert len(selected["confirmation_signal_dates"]) == 116
    assert len(selected["confirmation_symbols"]) == 184


def test_identity_envelope_failure_is_permanent_missing(
    monkeypatch,
) -> None:
    request = {
        "request_sha256": "a" * 64,
        "symbol": "EDGE",
    }

    def fail(*_args, **_kwargs):
        raise yahoo.EarningsSecYahooDataError(
            "Yahoo chart identity, timezone, or quote arrays drifted"
        )

    monkeypatch.setattr(collection.yahoo, "_fetch", fail)
    telemetry = {
        "requests": 1,
        "request_seconds": 0.0,
        "pacing_wait_seconds": 0.0,
        "cache_hits": 0,
        "failures": 0,
    }

    task = collection._fetch(request, object(), telemetry)

    assert task["status"] == "PERMANENT_MISSING"
    assert task["rows"] == []
    assert task["symbol"] == "EDGE"
    assert task["task_sha256"] == collection.v5.self_hash(
        task, "task_sha256"
    )

from datetime import date, timedelta

import dense_strategy_runtime as runtime
import earnings_pead_discovery as discovery
import earnings_pead_plugin as plugin
from historical_store import HistoricalDayStore


def _bar(day: str, close: float) -> dict[str, float | str]:
    return {
        "date": day,
        "open": close,
        "high": close + 1.0,
        "low": close - 1.0,
        "close": close,
        "volume": 2_000_000,
    }


def test_earnings_pead_candidate_is_next_open_and_five_day_bounded() -> None:
    days = [
        (date(2025, 1, 1) + timedelta(days=index)).isoformat()
        for index in range(230)
    ]
    reaction = days[220]
    evaluation = days[220:225]
    spy = [_bar(day, 100.0 + index) for index, day in enumerate(days)]
    symbol = [_bar(day, 50.0 + index * 0.05) for index, day in enumerate(days[195:226])]
    symbol[25]["open"] = 52.0
    symbol[25]["high"] = 53.0
    symbol[25]["low"] = 51.5
    symbol[25]["close"] = 52.5
    metadata = {day: [] for day in evaluation}
    metadata[reaction] = [
        {
            "symbol": "EDGE",
            "report_date": days[219],
            "reaction_date": reaction,
            "timing": "pm",
            "actual_eps": 1.25,
            "estimated_eps": 1.0,
            "identity_first_observed": days[100],
        }
    ]
    dataset = runtime.prepare_dataset(
        {
            "family_id": runtime.EARNINGS_PEAD_FAMILY,
            "evaluation_dates": evaluation,
            "event_metadata_by_date": metadata,
            "daily_bars": {"SPY": spy, "EDGE": symbol},
        }
    )

    candidates = runtime.build_candidates(
        dataset,
        runtime.EARNINGS_PEAD_FAMILY,
        {
            "minimum_surprise_ratio": 0.25,
            "minimum_opening_gap_fraction": 0.0,
            "market_trend_gate": "SPY>SMA200",
            "stop_atr14": 1.5,
            "maximum_hold_sessions": 5,
        },
    )

    assert len(candidates) == 1
    assert candidates[0]["entry_price"] == 52.0
    assert candidates[0]["signal_date"] == reaction
    assert candidates[0]["exit_date"] <= evaluation[-1]
    assert candidates[0]["planned_stop_distance"] > 0


def test_pead_status_never_requires_calendar_wait() -> None:
    status = discovery.status()
    assert status["calendar_wait_required"] is False
    assert status["provider_requests_permitted"] == 0
    assert status["confirmation_outcomes_accessed"] is False


def test_pead_production_evaluator_fails_closed() -> None:
    winner = {
        "strategy_id": "earnings-positive-surprise-drift",
        "strategy_version": "v1",
        "rules_hash": "a" * 64,
    }
    decision = {
        "as_of": "2026-07-24T20:00:00Z",
        "event_verified": True,
        "event_timing": "am",
        "event_timestamp_fresh": True,
        "identity_point_in_time": True,
        "daily_history_complete": True,
        "quote_fresh": False,
        "spread_fraction": 0.0005,
        "depth_sufficient": True,
        "halted": False,
        "tradable": True,
        "news_reconciled": True,
        "protection_plan_complete": True,
        "account_reconciled": True,
    }

    result = plugin.evaluate_production(winner, decision)

    assert result["entry_ready"] is False
    assert result["fail_closed"] is True
    assert result["broker_actions"] == 0


def test_complete_raw_daily_fallback_handles_prior_spy_history() -> None:
    bar = plugin._daily_bar(
        HistoricalDayStore.from_env(), "SPY", "2024-01-02"
    )

    assert bar["date"] == "2024-01-02"
    assert bar["open"] > 0
    assert bar["volume"] >= 0


def test_v1_development_failure_has_no_trial_or_confirmation_metrics(
    tmp_path,
) -> None:
    _path, failure = discovery.record_v1_failure(
        recorded_at="2026-07-24T20:45:00Z",
        root=tmp_path,
    )

    assert (
        failure["state"]
        == "FAILED_INCOMPLETE_PREFERRED_DAILY_HISTORY"
    )
    assert failure["trials_returned"] == 0
    assert failure["trial_metrics_surfaced"] is False
    assert failure["confirmation_accessed"] is False

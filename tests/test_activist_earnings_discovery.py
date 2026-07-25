from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import activist_earnings_discovery as discovery
import activist_earnings_data as data
import dense_strategy_runtime as runtime
from learning_experiment import enumerate_trials


def _weekdays(start: date, count: int) -> list[str]:
    result: list[str] = []
    current = start
    while len(result) < count:
        if current.weekday() < 5:
            result.append(current.isoformat())
        current += timedelta(days=1)
    return result


def test_sec_utc_timestamp_is_converted_to_eastern() -> None:
    observed = discovery._parse_timestamp("2024-07-29T20:10:17.000Z")
    assert observed.astimezone(discovery.NEW_YORK).strftime("%H:%M:%S") == "16:10:17"


def test_active_identity_uses_latest_observable_symbol_interval() -> None:
    intervals = [
        {
            "accepted_at": "2022-01-13T15:07:06",
            "symbol": "WAVD",
        },
        {
            "accepted_at": "2024-08-19T16:38:10",
            "symbol": "AIFF",
        },
    ]
    assert (
        discovery._active_identity(intervals, "2024-07-29T20:10:17.000Z")[
            "symbol"
        ]
        == "WAVD"
    )
    assert (
        discovery._active_identity(intervals, "2024-10-29T20:10:17.000Z")[
            "symbol"
        ]
        == "AIFF"
    )


def test_frozen_grid_has_exactly_32_trials() -> None:
    trials = enumerate_trials(
        {
            "maximum_hold_sessions": [2, 5],
            "minimum_close_location": [0.50, 0.75],
            "minimum_reaction_opening_gap_fraction": [0.02, 0.04],
            "reaction_confirmation": ["close>open", "close>prior_close"],
            "stop_atr14": [1.0, 1.5],
        }
    )
    assert len(trials) == 32
    assert len({trial["trial_id"] for trial in trials}) == 32


def test_runtime_enters_only_after_completed_reaction() -> None:
    calendar = _weekdays(date(2024, 1, 2), 35)
    reaction_date = calendar[25]
    bars = []
    for index, day in enumerate(calendar):
        opening = 100.0
        high = 102.0
        low = 99.0
        close = 101.0
        if index == 25:
            opening, high, low, close = 104.0, 109.0, 103.0, 108.0
        elif index > 25:
            opening, high, low, close = 109.0, 112.0, 107.0, 111.0
        bars.append(
            {
                "date": day,
                "open": opening,
                "high": high,
                "low": low,
                "close": close,
                "volume": 1_000_000,
            }
        )
    metadata = {day: [] for day in calendar}
    metadata[reaction_date] = [
        {
            "symbol": "ABC",
            "security_identity_state": "VERIFIED_ACTIVIST_COMMON_EQUITY",
            "reaction_date": reaction_date,
            "sec_form": "8-K",
            "sec_item": "2.02",
            "timing": "after_market",
            "accepted_at": "2024-01-01T21:00:00Z",
            "accession": "0000000000-24-000001",
        }
    ]
    dataset = {
        "family_id": runtime.ACTIVIST_EARNINGS_REACTION_FAMILY,
        "evaluation_dates": calendar,
        "event_metadata_by_date": metadata,
        "daily_bars": {"ABC": bars},
    }
    candidates = runtime.build_candidates(
        dataset,
        runtime.ACTIVIST_EARNINGS_REACTION_FAMILY,
        {
            "maximum_hold_sessions": 2,
            "minimum_close_location": 0.50,
            "minimum_reaction_opening_gap_fraction": 0.02,
            "reaction_confirmation": "close>open",
            "stop_atr14": 1.0,
        },
    )
    assert len(candidates) == 1
    assert candidates[0]["decision_date"] == reaction_date
    assert candidates[0]["signal_date"] == calendar[26]
    assert candidates[0]["outcome"] == "eligible"


def test_yahoo_response_parser_preserves_raw_daily_ohlcv() -> None:
    request = data._request("ABC")
    timestamp = int(
        datetime(2023, 1, 3, 14, 30, tzinfo=timezone.utc).timestamp()
    )
    task = data._parse_response(
        request,
        {
            "chart": {
                "error": None,
                "result": [
                    {
                        "meta": {
                            "symbol": "ABC",
                            "exchangeTimezoneName": "America/New_York",
                        },
                        "timestamp": [timestamp],
                        "indicators": {
                            "quote": [
                                {
                                    "open": [10.0],
                                    "high": [11.0],
                                    "low": [9.5],
                                    "close": [10.5],
                                    "volume": [1000],
                                }
                            ]
                        },
                    }
                ],
            }
        },
    )
    assert task["status"] == "COMPLETE"
    assert task["rows"] == [
        {
            "date": "2023-01-03",
            "open": 10.0,
            "high": 11.0,
            "low": 9.5,
            "close": 10.5,
            "volume": 1000,
        }
    ]
    assert task["task_sha256"] == data._hash(
        {key: value for key, value in task.items() if key != "task_sha256"}
    )


def test_partial_scope_keeps_only_accessed_symbols() -> None:
    assert data._filtered_scope(
        {
            "dates": ["2023-01-03", "2023-01-04"],
            "symbols_by_date": {
                "2023-01-03": ["ABC", "XYZ"],
                "2023-01-04": ["XYZ"],
            },
        },
        {"ABC"},
    ) == {
        "dates": ["2023-01-03"],
        "symbols_by_date": {"2023-01-03": ["ABC"]},
    }


def test_prospectively_registered_http_400_is_permanent_missing() -> None:
    class Response:
        status_code = 400

    class Session:
        def get(self, *_args, **_kwargs):
            return Response()

    telemetry = {
        "requests": 0,
        "request_seconds": 0.0,
        "failures": 0,
        "permanent_missing_responses": 0,
    }
    task = data._fetch(data._request("GRTX"), Session(), telemetry)
    assert task["status"] == "PERMANENT_MISSING"
    assert task["rows"] == []
    assert telemetry["requests"] == 1
    assert telemetry["permanent_missing_responses"] == 1

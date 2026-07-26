from __future__ import annotations

from datetime import datetime, timedelta

import dense_strategy_runtime as runtime
import sec_earnings_event_15m_replication as replication


DAY = "2024-03-01"


def _bars() -> list[dict]:
    start = datetime.fromisoformat(f"{DAY}T09:30:00-05:00")
    rows = []
    for index in range(26):
        rows.append(
            {
                "timestamp": (
                    start + timedelta(minutes=15 * index)
                ).isoformat(),
                "open": 101.0,
                "high": 101.5,
                "low": 100.5,
                "close": 101.0,
                "volume": 1_000_000.0,
            }
        )
    rows[0].update(
        {
            "open": 100.0,
            "high": 102.0,
            "low": 99.0,
            "close": 101.75,
            "volume": 3_000_000.0,
        }
    )
    return rows


def _parameters() -> dict:
    return {
        "maximum_structural_stop_fraction": 0.03,
        "minimum_gap_fraction": 0.02,
        "minimum_opening_close_location": 0.5,
        "minimum_opening_volume_ratio": 1.5,
        "target_r": 1.5,
    }


def test_replication_runtime_preserves_exact_semantics_and_identity():
    dataset = runtime.prepare_dataset(
        {
            "schema_version": 1,
            "family_id": replication.FAMILY_ID,
            "evaluation_dates": [DAY],
            "signal_dates": [DAY],
            "event_metadata_by_date": {
                DAY: [
                    {
                        "accepted_at": f"{DAY}T08:00:00-05:00",
                        "event_id": "a" * 64,
                        "gap_fraction": 0.03,
                        "instrument_id": "figi-AAA",
                        "opening_bullish": True,
                        "opening_close_location": 0.9166666667,
                        "opening_volume_ratio": 3.0,
                        "prior_close": 97.08737864,
                        "prior_median_dollar_volume": 100_000_000.0,
                        "symbol": "AAA",
                    }
                ]
            },
            "fifteen_minute_bars": {DAY: {"AAA": _bars()}},
            "blocked_dates": [],
        }
    )

    candidates = runtime.build_candidates(
        dataset,
        replication.FAMILY_ID,
        _parameters(),
    )

    assert replication.FAMILY_ID in runtime.SEC_EARNINGS_GAP_15M_FAMILIES
    assert replication.FAMILY_ID in runtime.SUPPORTED_FAMILIES
    assert len(candidates) == 1
    assert candidates[0]["signal_id"].startswith(
        f"{DAY}-{replication.FAMILY_ID}-AAA-"
    )
    assert candidates[0]["entry_price"] == 101.0
    assert candidates[0]["stop_price"] == 99.0


def test_replication_outcome_scope_covers_lookback_inputs():
    event = {
        "signal_date": DAY,
        "symbol": "AAA",
        "observation_dates": ["2024-02-28", "2024-02-29", DAY],
    }

    target = replication._target_scope([event])
    inputs = replication._input_scope([event])

    assert target == {
        "dates": [DAY],
        "symbols_by_date": {DAY: ["AAA"]},
    }
    assert inputs == {
        "dates": ["2024-02-28", "2024-02-29", DAY],
        "symbols_by_date": {
            "2024-02-28": ["AAA"],
            "2024-02-29": ["AAA"],
            DAY: ["AAA"],
        },
    }


def test_capacity_check_uses_only_structural_input_presence(monkeypatch):
    calls: list[tuple[str, str, str]] = []

    def select(_document, *, kind, channel, timeframe):
        calls.append((kind, channel, timeframe))
        if timeframe == "15m":
            return {"rows": [object()] * 26}
        return {"rows": [object()]}

    monkeypatch.setattr(replication.predecessor, "_select", select)

    assert replication._dataset_available({}, current=True) is True
    assert replication._dataset_available({}, current=False) is True
    assert calls == [
        ("bars", "trades", "15m"),
        ("bars", "trades", "15m"),
        ("derived", "minute_aggregate_regular", "1d"),
    ]

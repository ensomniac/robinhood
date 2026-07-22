from __future__ import annotations

from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pytest

from challenger_orb_retest_preentry_reader import FrozenCausalWindowClient
from historical_providers import HistoricalProviderError
from historical_store import (
    HistoricalDayStore,
    build_dataset,
    compact_bar,
    compact_trade,
)


EASTERN = ZoneInfo("America/New_York")


def _request(start: datetime, end: datetime, *, bars: bool) -> dict:
    value = {
        "start": start.astimezone(ZoneInfo("UTC")).isoformat(),
        "end": end.astimezone(ZoneInfo("UTC")).isoformat(),
        "use_rth": True,
    }
    if bars:
        value.update({"bar_size": "1 min", "what": "TRADES"})
    return value


def _client(tmp_path: Path) -> tuple[FrozenCausalWindowClient, datetime, datetime]:
    store = HistoricalDayStore(tmp_path, min_free_bytes=0)
    start = datetime(2026, 1, 2, 9, 30, tzinfo=EASTERN)
    end = datetime(2026, 1, 2, 10, 30, tzinfo=EASTERN)
    bar = {
        "time_et": start.isoformat(),
        "open": 10.0,
        "high": 10.1,
        "low": 9.9,
        "close": 10.05,
        "volume": 100,
    }
    trade = {
        "time_et": (start.replace(minute=35)).isoformat(),
        "source_timestamp": "2026-01-02T14:35:00.000000900Z",
        "price": 10.01,
        "size": 25,
        "trade_id": "1",
    }
    earlier_source_trade = {
        **trade,
        "source_timestamp": "2026-01-02T14:35:00.000000100Z",
        "trade_id": "2",
    }
    def provenance(bars: bool) -> dict:
        return {
            "captured_at": "2026-07-21T22:44:00+00:00",
            "request": _request(
                start if bars else start.replace(minute=35), end, bars=bars
            ),
        }
    store.merge(
        "TEST",
        "2026-01-02",
        datasets=[
            build_dataset(
                kind="bars",
                provider="alpaca",
                rows=[compact_bar(bar)],
                channel="trades",
                timeframe="1m",
                feed="sip",
                adjustment="raw",
                session="regular",
                scope="observed_window",
                quality={"complete": False, "requested_window_complete": True},
                provenance=provenance(True),
            ),
            build_dataset(
                kind="trades",
                provider="alpaca",
                rows=[compact_trade(trade), compact_trade(earlier_source_trade)],
                channel="sale",
                feed="sip",
                adjustment="raw",
                session="regular",
                scope="observed_window",
                quality={"complete": True},
                provenance=provenance(False),
            ),
        ],
    )
    return (
        FrozenCausalWindowClient(store, "alpaca", feed="sip", adjustment="raw"),
        start,
        end,
    )


def test_reads_exact_bounded_regular_session_bar_and_trade_windows(
    tmp_path: Path,
) -> None:
    client, start, end = _client(tmp_path)

    bars = client.fetch_bars("TEST", start, end)
    trades = client.fetch_trades("TEST", start.replace(minute=35), end)

    assert len(bars) == 1
    assert bars[0]["close"] == 10.05
    assert len(trades) == 2
    assert [row["trade_id"] for row in trades] == ["2", "1"]


def test_rejects_a_window_not_bound_by_exact_request_provenance(tmp_path: Path) -> None:
    client, start, end = _client(tmp_path)

    with pytest.raises(HistoricalProviderError, match="missing or ambiguous"):
        client.fetch_bars("TEST", start.replace(minute=31), end)

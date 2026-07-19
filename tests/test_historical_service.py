import tempfile
import unittest
from argparse import Namespace
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch

from historical_data_cli import _fetch
from historical_providers import HistoricalProviderError
from historical_service import (
    LocalHistoricalClient,
    RecordingHistoricalClient,
    collect_with_fallback,
)
from historical_store import (
    EASTERN,
    HistoricalDayStore,
    build_dataset,
    compact_bar,
    compact_quote,
)


def minute_row(observed, index=0):
    return {
        "epoch": int(observed.timestamp()),
        "time_et": observed.isoformat(),
        "date_et": observed.date().isoformat(),
        "open": 10 + index / 100,
        "high": 10.1 + index / 100,
        "low": 9.9 + index / 100,
        "close": 10.05 + index / 100,
        "volume": 100,
        "count": 2,
        "wap": 10.02 + index / 100,
        "interpolated": False,
    }


class FakeProvider:
    provider_name = "Alpaca Market Data API"
    cache_namespace = "alpaca"
    feed = "sip"
    adjustment = "raw"

    def __init__(self, rows):
        self.rows = rows

    def fetch_bars(self, *args, **kwargs):
        return list(self.rows)

    def fetch_bid_ask_ticks(self, *args, **kwargs):
        return []


class HistoricalServiceTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.store = HistoricalDayStore(Path(self.temporary.name) / "history")

    def tearDown(self):
        self.temporary.cleanup()

    def test_successful_live_response_is_recorded(self):
        start = datetime(2026, 3, 3, 9, 30, tzinfo=EASTERN)
        client = RecordingHistoricalClient(
            FakeProvider([minute_row(start)]), self.store
        )

        rows = client.fetch_bars("AAPL", start, start + timedelta(minutes=1))

        self.assertEqual(len(rows), 1)
        document = self.store.load("AAPL", "2026-03-03")
        self.assertEqual(document["datasets"][0]["provider"], "alpaca")
        self.assertEqual(document["datasets"][0]["feed"], "sip")
        self.assertFalse(document["datasets"][0]["quality"]["complete"])
        self.assertEqual(document["datasets"][0]["scope"], "observed_window")

    def test_full_requested_window_is_complete_without_390_populated_buckets(self):
        start = datetime(2026, 3, 3, 9, 30, tzinfo=EASTERN)
        rows = [
            minute_row(start + timedelta(minutes=index), index) for index in range(210)
        ]
        client = RecordingHistoricalClient(FakeProvider(rows), self.store)

        client.fetch_bars("AAPL", start, start.replace(hour=16, minute=0))

        dataset = self.store.load("AAPL", "2026-03-03")["datasets"][0]
        self.assertEqual(dataset["quality"]["row_count"], 210)
        self.assertTrue(dataset["quality"]["complete"])
        self.assertTrue(dataset["quality"]["requested_window_complete"])
        self.assertTrue(dataset["quality"]["sparse_intervals_allowed"])
        self.assertEqual(dataset["scope"], "full_session")

    def test_non_rth_window_is_recorded_and_reused_by_exact_request_coverage(self):
        start = datetime(2026, 3, 3, 4, 0, tzinfo=EASTERN)
        rows = [
            minute_row(start + timedelta(minutes=index), index) for index in range(330)
        ]
        recording = RecordingHistoricalClient(FakeProvider(rows), self.store)

        recording.fetch_bars(
            "AAPL",
            start,
            start.replace(hour=9, minute=30),
            use_rth=False,
        )

        dataset = self.store.load("AAPL", "2026-03-03")["datasets"][0]
        self.assertEqual(dataset["session"], "all")
        self.assertEqual(dataset["scope"], "observed_window")
        self.assertFalse(dataset["quality"]["complete"])
        self.assertTrue(dataset["quality"]["requested_window_complete"])

        local = LocalHistoricalClient(
            self.store, "alpaca", feed="sip", adjustment="raw"
        )
        cached = local.fetch_bars(
            "AAPL",
            start.replace(hour=8),
            start.replace(hour=9, minute=30),
            use_rth=False,
        )
        self.assertEqual(len(cached), 90)
        self.assertEqual(cached[0]["time_et"], start.replace(hour=8).isoformat())

    def test_non_rth_cache_rejects_a_query_outside_captured_window(self):
        start = datetime(2026, 3, 3, 8, 0, tzinfo=EASTERN)
        rows = [
            minute_row(start + timedelta(minutes=index), index) for index in range(90)
        ]
        recording = RecordingHistoricalClient(FakeProvider(rows), self.store)
        recording.fetch_bars(
            "AAPL", start, start.replace(hour=9, minute=30), use_rth=False
        )
        local = LocalHistoricalClient(
            self.store, "alpaca", feed="sip", adjustment="raw"
        )

        with self.assertRaises(HistoricalProviderError) as caught:
            local.fetch_bars(
                "AAPL",
                start.replace(hour=7),
                start.replace(hour=9, minute=30),
                use_rth=False,
            )

        self.assertEqual(caught.exception.category, "local_cache_miss")

    def test_local_cache_aggregates_complete_minutes(self):
        start = datetime(2026, 3, 3, 9, 30, tzinfo=EASTERN)
        rows = [
            minute_row(start + timedelta(minutes=index), index) for index in range(390)
        ]
        self.store.merge(
            "AAPL",
            "2026-03-03",
            datasets=[
                build_dataset(
                    kind="bars",
                    provider="ibkr",
                    rows=[compact_bar(row) for row in rows],
                    channel="trades",
                    timeframe="1m",
                    feed="smart",
                    adjustment="provider_adjusted_unknown_basis",
                    quality={"complete": True},
                )
            ],
        )
        client = LocalHistoricalClient(self.store, "ibkr")

        bars = client.fetch_bars(
            "AAPL", start, start + timedelta(minutes=10), bar_size="5 mins"
        )

        self.assertEqual(len(bars), 2)
        self.assertEqual(bars[0]["volume"], 500)

    def test_local_quotes_union_shards_without_mixing_feeds(self):
        start = datetime(2026, 3, 3, 9, 35, tzinfo=EASTERN)
        datasets = []
        for index in range(2):
            observed = start + timedelta(minutes=index)
            datasets.append(
                build_dataset(
                    kind="quotes",
                    provider="alpaca",
                    rows=[
                        compact_quote(
                            {
                                "time_et": observed.isoformat(),
                                "bid": 10 + index / 100,
                                "ask": 10.01 + index / 100,
                                "bid_size": 100,
                                "ask_size": 200,
                            }
                        )
                    ],
                    channel="top_of_book",
                    feed="sip",
                    adjustment="raw",
                )
            )
        datasets.append(
            build_dataset(
                kind="quotes",
                provider="alpaca",
                rows=[
                    compact_quote(
                        {
                            "time_et": (start + timedelta(seconds=30)).isoformat(),
                            "bid": 9,
                            "ask": 9.01,
                            "bid_size": 1,
                            "ask_size": 1,
                        }
                    )
                ],
                channel="top_of_book",
                feed="iex",
                adjustment="raw",
            )
        )
        self.store.merge("AAPL", "2026-03-03", datasets=datasets)
        client = LocalHistoricalClient(
            self.store, "alpaca", feed="sip", adjustment="raw"
        )

        quotes = client.fetch_bid_ask_ticks("AAPL", start, start + timedelta(minutes=2))

        self.assertEqual(len(quotes), 2)
        self.assertEqual([row["bid"] for row in quotes], [10.0, 10.01])

    def test_local_quotes_apply_subsecond_source_timestamp_boundaries(self):
        start = datetime(2026, 3, 3, 9, 35, tzinfo=EASTERN)
        rows = []
        for offset, bid in ((-0.5, 9.99), (0.5, 10.0)):
            observed = start + timedelta(seconds=offset)
            rows.append(
                compact_quote(
                    {
                        "time_et": observed.isoformat(),
                        "source_timestamp": observed.astimezone().isoformat(),
                        "bid": bid,
                        "ask": bid + 0.01,
                        "bid_size": 100,
                        "ask_size": 200,
                    }
                )
            )
        self.store.merge(
            "AAPL",
            "2026-03-03",
            datasets=[
                build_dataset(
                    kind="quotes",
                    provider="alpaca",
                    rows=rows,
                    channel="top_of_book",
                    feed="sip",
                    adjustment="raw",
                )
            ],
        )
        client = LocalHistoricalClient(
            self.store, "alpaca", feed="sip", adjustment="raw"
        )

        quotes = client.fetch_bid_ask_ticks("AAPL", start, start + timedelta(seconds=1))

        self.assertEqual([row["bid"] for row in quotes], [10.0])

    def test_fetch_cache_hit_opens_no_live_provider(self):
        start = datetime(2026, 3, 3, 9, 30, tzinfo=EASTERN)
        rows = [
            minute_row(start + timedelta(minutes=index), index) for index in range(390)
        ]
        self.store.merge(
            "AAPL",
            "2026-03-03",
            datasets=[
                build_dataset(
                    kind="bars",
                    provider="ibkr",
                    rows=[compact_bar(row) for row in rows],
                    channel="trades",
                    timeframe="1m",
                    feed="smart",
                    adjustment="provider_adjusted_unknown_basis",
                    quality={"complete": True},
                )
            ],
        )
        args = Namespace(
            symbol="AAPL",
            date="2026-03-03",
            env_file=Path(self.temporary.name) / "missing.env",
        )

        with patch(
            "historical_data_cli.open_provider_set",
            side_effect=AssertionError("cache hit must not open providers"),
        ):
            result = _fetch(args, self.store)

        self.assertTrue(result["valid"])
        self.assertEqual(result["rows"], 390)
        self.assertEqual(result["attempts"][0]["lane"], "cache")

    def test_fetch_accepts_complete_early_close_cache(self):
        start = datetime(2026, 3, 3, 9, 30, tzinfo=EASTERN)
        rows = [
            minute_row(start + timedelta(minutes=index), index) for index in range(210)
        ]
        self.store.merge(
            "AAPL",
            "2026-03-03",
            datasets=[
                build_dataset(
                    kind="bars",
                    provider="ibkr",
                    rows=[compact_bar(row) for row in rows],
                    channel="trades",
                    timeframe="1m",
                    feed="smart",
                    adjustment="provider_adjusted_unknown_basis",
                    scope="full_session",
                    quality={
                        "complete": True,
                        "requested_window_complete": True,
                        "sparse_intervals_allowed": True,
                    },
                )
            ],
        )
        args = Namespace(
            symbol="AAPL",
            date="2026-03-03",
            env_file=Path(self.temporary.name) / "missing.env",
        )

        with patch(
            "historical_data_cli.open_provider_set",
            side_effect=AssertionError("complete cache hit must not open providers"),
        ):
            result = _fetch(args, self.store)

        self.assertTrue(result["valid"])
        self.assertEqual(result["rows"], 210)
        self.assertEqual(result["attempts"][0]["lane"], "cache")

    def test_fallback_advances_after_retryable_failure(self):
        class Failure:
            provider_name = "IBKR"

        class Success:
            provider_name = "Massive"

        clients = [Failure(), Success()]

        def operation(client):
            if isinstance(client, Failure):
                raise HistoricalProviderError(
                    "timed out", category="retryable_transport"
                )
            return ["ok"]

        rows, used, attempts = collect_with_fallback(clients, operation)

        self.assertEqual(rows, ["ok"])
        self.assertIsInstance(used, Success)
        self.assertEqual(
            [attempt.status for attempt in attempts], ["failed", "success"]
        )


if __name__ == "__main__":
    unittest.main()

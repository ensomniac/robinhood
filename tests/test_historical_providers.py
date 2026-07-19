import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path

from historical_providers import (
    AlpacaConfig,
    AlpacaHistoricalClient,
    EASTERN,
    HistoricalProviderError,
    MassiveConfig,
    MassiveHistoricalClient,
)


class FakeResponse:
    def __init__(self, payload, status_code=200):
        self.payload = payload
        self.status_code = status_code

    def json(self):
        return self.payload


class FakeSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def get(self, url, *, params, timeout, headers=None):
        self.calls.append((url, dict(params), timeout, dict(headers or {})))
        return self.responses.pop(0)


def aggregate(stamp, *, price=10.0, volume=100):
    return {
        "t": int(stamp.timestamp() * 1000),
        "o": price,
        "h": price + 0.1,
        "l": price - 0.1,
        "c": price + 0.05,
        "v": volume,
        "n": 3,
        "vw": price,
    }


class MassiveConfigTests(unittest.TestCase):
    def test_optional_config_does_not_expose_api_key(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / ".env"
            path.write_text(
                "MASSIVE_API_KEY=secret-value\nMASSIVE_TIMEOUT_SECONDS=12\n",
                encoding="utf-8",
            )
            config = MassiveConfig.optional_from_env(path)

        self.assertIsNotNone(config)
        self.assertNotIn("secret-value", str(config.public_dict()))
        self.assertTrue(config.public_dict()["api_key_configured"])
        self.assertEqual(config.timeout_seconds, 12)

    def test_missing_key_disables_fallback(self):
        with tempfile.TemporaryDirectory() as directory:
            config = MassiveConfig.optional_from_env(Path(directory) / "missing")

        self.assertIsNone(config)


class AlpacaConfigTests(unittest.TestCase):
    def test_project_key_aliases_use_official_data_host_without_exposing_secrets(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / ".env"
            path.write_text(
                "ALPACA_KEY=key-value\n"
                "ALPACA_SECRET=secret-value\n"
                "ALPACA_ENDPOINT=https://paper-api.alpaca.markets\n",
                encoding="utf-8",
            )
            config = AlpacaConfig.optional_from_env(path)

        self.assertEqual(config.base_url, "https://data.alpaca.markets")
        self.assertEqual(config.feed, "sip")
        self.assertEqual(config.adjustment, "raw")
        self.assertEqual(config.minimum_interval_seconds, 0.35)
        self.assertNotIn("key-value", str(config.public_dict()))
        self.assertNotIn("secret-value", str(config.public_dict()))

    def test_configurable_request_pacing_is_public_and_nonnegative(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / ".env"
            path.write_text(
                "ALPACA_KEY=key-value\n"
                "ALPACA_SECRET=secret-value\n"
                "ALPACA_MINIMUM_INTERVAL_SECONDS=0.5\n",
                encoding="utf-8",
            )
            config = AlpacaConfig.optional_from_env(path)

        self.assertEqual(config.minimum_interval_seconds, 0.5)
        self.assertEqual(config.public_dict()["minimum_interval_seconds"], 0.5)

    def test_partial_credentials_are_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / ".env"
            path.write_text("ALPACA_KEY=key-value\n", encoding="utf-8")

            with self.assertRaisesRegex(HistoricalProviderError, "both key and secret"):
                AlpacaConfig.optional_from_env(path)


class MassiveNormalizationTests(unittest.TestCase):
    def test_normalizes_adjusted_minute_aggregates_and_resamples(self):
        start = datetime(2026, 3, 3, 9, 30, tzinfo=EASTERN)
        raw = [aggregate(start + timedelta(minutes=index)) for index in range(5)]
        session = FakeSession([FakeResponse({"status": "OK", "results": raw})])
        client = MassiveHistoricalClient(
            MassiveConfig(api_key="secret"), session=session
        )

        bars = client.fetch_bars(
            "AAPL", start, start + timedelta(minutes=5), bar_size="5 mins"
        )

        self.assertEqual(len(bars), 1)
        self.assertEqual(bars[0]["volume"], 500)
        self.assertEqual(bars[0]["time_et"], start.isoformat())
        self.assertFalse(bars[0]["interpolated"])
        self.assertEqual(session.calls[0][1]["adjusted"], "true")
        self.assertEqual(session.calls[0][1]["apiKey"], "secret")

    def test_quotes_follow_safe_pagination_and_preserve_sip_time(self):
        evaluation = datetime(2026, 3, 3, 9, 40, tzinfo=EASTERN)
        first_ns = int((evaluation - timedelta(seconds=2)).timestamp() * 1e9)
        second_ns = int((evaluation - timedelta(seconds=1)).timestamp() * 1e9)
        session = FakeSession(
            [
                FakeResponse({"status": "OK", "results": []}),
                FakeResponse(
                    {
                        "status": "OK",
                        "results": [
                            {
                                "sip_timestamp": first_ns,
                                "participant_timestamp": first_ns - 10,
                                "bid_price": 10.0,
                                "ask_price": 10.01,
                                "bid_size": 100,
                                "ask_size": 200,
                            }
                        ],
                        "next_url": "https://api.massive.com/v3/quotes/AAPL?cursor=abc",
                    }
                ),
                FakeResponse(
                    {
                        "status": "OK",
                        "results": [
                            {
                                "sip_timestamp": second_ns,
                                "bid_price": 10.01,
                                "ask_price": 10.02,
                                "bid_size": 110,
                                "ask_size": 210,
                            }
                        ],
                    }
                ),
            ]
        )
        client = MassiveHistoricalClient(
            MassiveConfig(api_key="secret"), session=session
        )

        ticks = client.fetch_bid_ask_ticks(
            "AAPL", evaluation - timedelta(seconds=5), evaluation
        )

        self.assertEqual(len(ticks), 2)
        self.assertEqual(ticks[0]["sip_timestamp_ns"], first_ns)
        self.assertEqual(ticks[-1]["ask_size"], 210)
        self.assertEqual(session.calls[2][1], {"apiKey": "secret"})

    def test_quotes_are_adjusted_to_the_aggregate_split_basis(self):
        evaluation = datetime(2020, 8, 28, 9, 40, tzinfo=EASTERN)
        observed_ns = int((evaluation - timedelta(seconds=1)).timestamp() * 1e9)
        session = FakeSession(
            [
                FakeResponse(
                    {
                        "status": "OK",
                        "results": [
                            {
                                "execution_date": "2020-08-31",
                                "historical_adjustment_factor": 0.25,
                            }
                        ],
                    }
                ),
                FakeResponse(
                    {
                        "status": "OK",
                        "results": [
                            {
                                "sip_timestamp": observed_ns,
                                "bid_price": 400.0,
                                "ask_price": 404.0,
                                "bid_size": 100,
                                "ask_size": 200,
                            }
                        ],
                    }
                ),
            ]
        )
        client = MassiveHistoricalClient(
            MassiveConfig(api_key="secret"), session=session
        )

        ticks = client.fetch_bid_ask_ticks(
            "AAPL", evaluation - timedelta(seconds=5), evaluation
        )

        self.assertEqual(ticks[0]["bid"], 100.0)
        self.assertEqual(ticks[0]["ask"], 101.0)
        self.assertEqual(ticks[0]["bid_size"], 400)
        self.assertEqual(ticks[0]["ask_size"], 800)
        self.assertEqual(ticks[0]["split_adjustment_factor"], 0.25)

    def test_rejects_cross_origin_pagination_before_forwarding_key(self):
        session = FakeSession(
            [
                FakeResponse(
                    {
                        "status": "OK",
                        "results": [],
                        "next_url": "https://attacker.invalid/steal",
                    }
                )
            ]
        )
        client = MassiveHistoricalClient(
            MassiveConfig(api_key="secret"), session=session
        )
        start = datetime(2026, 3, 3, 9, 30, tzinfo=EASTERN)

        with self.assertRaisesRegex(HistoricalProviderError, "unsafe pagination"):
            client.fetch_bars("AAPL", start, start + timedelta(minutes=1))

        self.assertEqual(len(session.calls), 1)

    def test_rate_limit_is_explicitly_retryable(self):
        session = FakeSession([FakeResponse({}, status_code=429)])
        client = MassiveHistoricalClient(
            MassiveConfig(api_key="secret"), session=session
        )
        start = datetime(2026, 3, 3, 9, 30, tzinfo=EASTERN)

        with self.assertRaises(HistoricalProviderError) as raised:
            client.fetch_bars("AAPL", start, start + timedelta(minutes=1))

        self.assertTrue(raised.exception.retryable)
        self.assertEqual(raised.exception.category, "retryable_provider")


class AlpacaNormalizationTests(unittest.TestCase):
    def test_null_observation_collection_is_an_empty_success(self):
        start = datetime(2026, 3, 3, 9, 35, tzinfo=EASTERN)
        session = FakeSession([FakeResponse({"quotes": None})])
        client = AlpacaHistoricalClient(
            AlpacaConfig(api_key="key", api_secret="secret"), session=session
        )

        quotes = client.fetch_bid_ask_ticks(
            "AAPL", start, start + timedelta(seconds=10)
        )

        self.assertEqual(quotes, [])

    def test_bars_paginate_with_sip_feed_raw_adjustment_and_auth_headers(self):
        start = datetime(2026, 3, 3, 9, 30, tzinfo=EASTERN)
        raw = {
            "t": start.isoformat(),
            "o": 10,
            "h": 10.1,
            "l": 9.9,
            "c": 10.05,
            "v": 100,
            "n": 2,
            "vw": 10.02,
        }
        session = FakeSession(
            [
                FakeResponse({"bars": [raw], "next_page_token": "next"}),
                FakeResponse(
                    {"bars": [{**raw, "t": (start + timedelta(minutes=1)).isoformat()}]}
                ),
            ]
        )
        client = AlpacaHistoricalClient(
            AlpacaConfig(
                api_key="key", api_secret="secret", minimum_interval_seconds=0
            ),
            session=session,
        )

        bars = client.fetch_bars("AAPL", start, start + timedelta(minutes=2))

        self.assertEqual(len(bars), 2)
        self.assertEqual(session.calls[0][1]["feed"], "sip")
        self.assertEqual(session.calls[0][1]["adjustment"], "raw")
        self.assertEqual(session.calls[0][1]["asof"], "-")
        self.assertEqual(session.calls[1][1]["page_token"], "next")
        self.assertEqual(session.calls[0][3]["APCA-API-KEY-ID"], "key")
        self.assertEqual(session.calls[0][3]["APCA-API-SECRET-KEY"], "secret")

    def test_pagination_respects_configured_minimum_request_interval(self):
        start = datetime(2026, 3, 3, 9, 30, tzinfo=EASTERN)
        raw = {
            "t": start.isoformat(),
            "o": 10,
            "h": 10.1,
            "l": 9.9,
            "c": 10.05,
            "v": 100,
            "n": 3,
            "vw": 10,
        }
        session = FakeSession(
            [
                FakeResponse({"bars": [raw], "next_page_token": "next"}),
                FakeResponse({"bars": []}),
            ]
        )
        clock = [10.0]
        sleeps = []

        def sleeper(seconds):
            sleeps.append(seconds)
            clock[0] += seconds

        client = AlpacaHistoricalClient(
            AlpacaConfig(
                api_key="key", api_secret="secret", minimum_interval_seconds=0.5
            ),
            session=session,
            sleeper=sleeper,
            monotonic=lambda: clock[0],
        )

        client.fetch_bars("AAPL", start, start + timedelta(minutes=2), bar_size="1 min")

        self.assertEqual(sleeps, [0.5])

    def test_fifteen_minute_bars_use_the_native_alpaca_timeframe(self):
        start = datetime(2026, 3, 3, 9, 30, tzinfo=EASTERN)
        session = FakeSession([FakeResponse({"bars": []})])
        client = AlpacaHistoricalClient(
            AlpacaConfig(api_key="key", api_secret="secret"), session=session
        )

        bars = client.fetch_bars(
            "AAPL",
            start,
            start + timedelta(minutes=15),
            bar_size="15 mins",
        )

        self.assertEqual(bars, [])
        self.assertEqual(session.calls[0][1]["timeframe"], "15Min")

    def test_quotes_preserve_exchange_condition_and_tape_context(self):
        start = datetime(2026, 3, 3, 9, 35, tzinfo=EASTERN)
        session = FakeSession(
            [
                FakeResponse(
                    {
                        "quotes": [
                            {
                                "t": start.isoformat(),
                                "bp": 10,
                                "ap": 10.01,
                                "bs": 100,
                                "as": 200,
                                "bx": "V",
                                "ax": "Q",
                                "c": ["R"],
                                "z": "C",
                            }
                        ]
                    }
                )
            ]
        )
        client = AlpacaHistoricalClient(
            AlpacaConfig(api_key="key", api_secret="secret"), session=session
        )

        quotes = client.fetch_bid_ask_ticks(
            "AAPL", start - timedelta(seconds=1), start + timedelta(seconds=1)
        )

        self.assertEqual(quotes[0]["bid_exchange"], "V")
        self.assertEqual(quotes[0]["conditions"], ["R"])
        self.assertEqual(quotes[0]["tape"], "C")

    def test_rate_limit_is_explicitly_retryable(self):
        session = FakeSession([FakeResponse({}, status_code=429)])
        client = AlpacaHistoricalClient(
            AlpacaConfig(api_key="key", api_secret="secret"), session=session
        )
        start = datetime(2026, 3, 3, 9, 30, tzinfo=EASTERN)

        with self.assertRaises(HistoricalProviderError) as raised:
            client.fetch_bars("AAPL", start, start + timedelta(minutes=1))

        self.assertTrue(raised.exception.retryable)
        self.assertEqual(raised.exception.category, "retryable_provider")

    def test_raw_trades_preserve_sip_ordering_and_conditions(self):
        start = datetime(2026, 3, 3, 9, 35, tzinfo=EASTERN)
        session = FakeSession(
            [
                FakeResponse(
                    {
                        "trades": [
                            {
                                "t": start.isoformat(),
                                "p": 10.01,
                                "s": 100,
                                "x": "Q",
                                "c": ["@"],
                                "i": 17,
                                "z": "C",
                            }
                        ]
                    }
                )
            ]
        )
        client = AlpacaHistoricalClient(
            AlpacaConfig(api_key="key", api_secret="secret"), session=session
        )

        trades = client.fetch_trades("AAPL", start, start + timedelta(minutes=1))

        self.assertEqual(trades[0]["price"], 10.01)
        self.assertEqual(trades[0]["conditions"], ["@"])
        self.assertEqual(trades[0]["trade_id"], 17)
        self.assertEqual(session.calls[0][1]["asof"], "-")

    def test_news_is_time_bounded_and_marked_with_source(self):
        start = datetime(2026, 3, 2, 0, 0, tzinfo=EASTERN)
        created = datetime(2026, 3, 3, 8, 0, tzinfo=EASTERN)
        session = FakeSession(
            [
                FakeResponse(
                    {
                        "news": [
                            {
                                "id": 9,
                                "created_at": created.isoformat(),
                                "updated_at": created.isoformat(),
                                "headline": "Issuer reports results",
                                "summary": "Summary",
                                "source": "benzinga",
                                "url": "https://example.test/article",
                                "symbols": ["AAPL"],
                                "content": "not requested",
                            }
                        ]
                    }
                )
            ]
        )
        client = AlpacaHistoricalClient(
            AlpacaConfig(api_key="key", api_secret="secret"), session=session
        )

        news = client.fetch_news(["AAPL"], start, created + timedelta(hours=2))

        self.assertEqual(news[0]["source"], "benzinga")
        self.assertNotIn("content", news[0])
        self.assertEqual(session.calls[0][1]["include_content"], "false")


if __name__ == "__main__":
    unittest.main()

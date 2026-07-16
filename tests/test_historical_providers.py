import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path

from historical_providers import (
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

    def get(self, url, *, params, timeout):
        self.calls.append((url, dict(params), timeout))
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


if __name__ == "__main__":
    unittest.main()

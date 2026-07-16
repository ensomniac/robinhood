import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

from ibkr_historical import (
    IBKRConfig,
    IBKRConfigurationError,
    IBKRHistoricalClient,
    IBKRRequestError,
    collect_quote_evidence,
    ensure_tws_socket,
    historical_error_category,
    is_retryable_historical_error,
    probe_historical_candidate,
    probe_historical_symbol,
    select_quote_snapshots,
)


EASTERN = ZoneInfo("America/New_York")


class ConfigTests(unittest.TestCase):
    def test_loads_optional_connection_settings_without_account_or_secret(self):
        with tempfile.TemporaryDirectory() as directory:
            env_path = Path(directory) / ".env"
            env_path.write_text(
                "IBKR_HOST=127.0.0.1\n"
                "IBKR_PORT=7497\n"
                "IBKR_CLIENT_ID=88\n"
                "IBKR_AUTO_START_TWS=true\n",
                encoding="utf-8",
            )
            config = IBKRConfig.from_env(env_path)

        self.assertEqual(config.port, 7497)
        self.assertEqual(config.client_id, 88)
        self.assertTrue(config.auto_start_tws)
        self.assertNotIn("account", config.public_dict())
        self.assertNotIn("key", config.public_dict())

    def test_closed_socket_fails_with_actionable_tws_message(self):
        config = IBKRConfig(host="127.0.0.1", port=65534, auto_start_tws=False)

        with self.assertRaisesRegex(IBKRConfigurationError, "Start and log in"):
            ensure_tws_socket(config)

    def test_adapter_defines_no_account_or_order_methods(self):
        forbidden = (
            "placeOrder",
            "cancelOrder",
            "reqAccountUpdates",
            "reqPositions",
            "reqOpenOrders",
            "reqExecutions",
        )

        for method_name in forbidden:
            self.assertFalse(hasattr(IBKRHistoricalClient, method_name))


class NormalizationTests(unittest.TestCase):
    def test_daily_and_epoch_bar_timestamps_are_normalized_to_eastern(self):
        daily = IBKRHistoricalClient._bar_timestamp("20260512")
        epoch = IBKRHistoricalClient._bar_timestamp("1778592600")

        self.assertEqual(daily.astimezone(EASTERN).date().isoformat(), "2026-05-12")
        self.assertIsNotNone(epoch)
        self.assertEqual(epoch.tzinfo, timezone.utc)


class SymbolProbeTests(unittest.TestCase):
    def test_retired_symbol_is_a_skippable_preflight_result(self):
        class FakeClient:
            def fetch_contract_details(self, symbol):
                raise IBKRRequestError("missing", error_code=200)

        result = probe_historical_symbol(FakeClient(), "semr")

        self.assertFalse(result["viable"])
        self.assertEqual(result["symbol"], "SEMR")
        self.assertEqual(result["reason"], "unresolvable_security_definition")

    def test_provider_wide_failure_is_not_downgraded_to_a_symbol_skip(self):
        class FakeClient:
            def fetch_contract_details(self, symbol):
                raise IBKRRequestError("permission", error_code=10187)

        with self.assertRaisesRegex(IBKRRequestError, "permission"):
            probe_historical_symbol(FakeClient(), "ALK")

    def test_resolved_us_stock_is_viable(self):
        class FakeClient:
            def fetch_contract_details(self, symbol):
                return [
                    {
                        "symbol": symbol,
                        "security_type": "STK",
                        "currency": "USD",
                    }
                ]

        result = probe_historical_symbol(FakeClient(), "ALK")

        self.assertTrue(result["viable"])
        self.assertEqual(result["reason"], "contract_resolved")

    def test_pre_session_probe_requires_strategy_compatible_history(self):
        day = datetime(2026, 3, 3, tzinfo=EASTERN).date()

        class FakeClient:
            def __init__(self):
                self.ends = []

            def fetch_contract_details(self, symbol):
                return [
                    {
                        "symbol": symbol,
                        "security_type": "STK",
                        "currency": "USD",
                    }
                ]

            def fetch_bars(self, symbol, start, end, *, bar_size, what):
                self.ends.append(end)
                count = 14 if bar_size == "5 mins" else 15
                rows = []
                for offset in range(count, 0, -1):
                    stamp = datetime.combine(
                        day - timedelta(days=offset),
                        datetime.min.time().replace(hour=9, minute=30),
                        tzinfo=EASTERN,
                    )
                    rows.append(
                        {
                            "epoch": int(stamp.timestamp()),
                            "date_et": stamp.date().isoformat(),
                            "volume": 1000,
                        }
                    )
                return rows

        client = FakeClient()
        result = probe_historical_candidate(client, "ALK", day)

        self.assertTrue(result["viable"])
        self.assertEqual(result["reason"], "pre_session_history_available")
        self.assertFalse(result["target_session_prices_observed"])
        self.assertTrue(all(end.date() == day for end in client.ends))
        self.assertTrue(all(end.time() == datetime.min.time() for end in client.ends))

    def test_pre_session_probe_rejects_zero_opening_volume(self):
        day = datetime(2026, 3, 3, tzinfo=EASTERN).date()

        class FakeClient:
            def fetch_contract_details(self, symbol):
                return [
                    {
                        "symbol": symbol,
                        "security_type": "STK",
                        "currency": "USD",
                    }
                ]

            def fetch_bars(self, symbol, start, end, *, bar_size, what):
                rows = []
                for offset in range(14, 0, -1):
                    stamp = datetime.combine(
                        day - timedelta(days=offset),
                        datetime.min.time().replace(hour=9, minute=30),
                        tzinfo=EASTERN,
                    )
                    rows.append(
                        {
                            "epoch": int(stamp.timestamp()),
                            "date_et": stamp.date().isoformat(),
                            "volume": 0 if offset == 7 else 1000,
                        }
                    )
                return rows

        result = probe_historical_candidate(FakeClient(), "THO", day)

        self.assertFalse(result["viable"])
        self.assertEqual(result["reason"], "nonpositive_prior_opening_volume")


class ErrorClassificationTests(unittest.TestCase):
    def test_connection_and_timeout_are_retryable(self):
        disconnected = IBKRRequestError("closed", error_code=507)
        timed_out = IBKRRequestError("historical-bars request timed out")

        self.assertEqual(historical_error_category(disconnected), "retryable_transport")
        self.assertTrue(is_retryable_historical_error(disconnected))
        self.assertTrue(is_retryable_historical_error(timed_out))

    def test_missing_historical_fact_is_permanent_fidelity(self):
        missing = IBKRRequestError("no historical bid/ask quote evidence")

        self.assertEqual(historical_error_category(missing), "permanent_fidelity")
        self.assertFalse(is_retryable_historical_error(missing))


class QuoteSnapshotTests(unittest.TestCase):
    def _tick(self, observed: datetime, bid: float, ask: float):
        return {
            "epoch": int(observed.timestamp()),
            "bid": bid,
            "ask": ask,
            "bid_size": 4000,
            "ask_size": 5000,
        }

    def test_builds_three_point_in_time_strategy_snapshots(self):
        evaluation = datetime(2026, 5, 12, 9, 40, tzinfo=EASTERN)
        ticks = [
            self._tick(evaluation - timedelta(seconds=11), 99.98, 100.00),
            self._tick(evaluation - timedelta(seconds=6), 99.99, 100.01),
            self._tick(evaluation - timedelta(seconds=1), 100.00, 100.02),
        ]
        minute_bars = [
            {
                "epoch": int((evaluation - timedelta(minutes=1)).timestamp()),
                "volume": 12000,
            }
        ]

        snapshots = select_quote_snapshots(ticks, evaluation, minute_bars)

        self.assertEqual(len(snapshots), 3)
        self.assertEqual([row["age_seconds"] for row in snapshots], [1.0, 1.0, 1.0])
        self.assertEqual(snapshots[-1]["ask_depth"], 5000)
        self.assertEqual(snapshots[-1]["recent_real_1m_volume"], 12000)
        self.assertEqual(snapshots[-1]["depth_scope"], "historical top-of-book size")

    def test_rejects_a_snapshot_without_prior_quote_state(self):
        evaluation = datetime(2026, 5, 12, 9, 40, tzinfo=EASTERN)

        with self.assertRaisesRegex(IBKRRequestError, "no historical bid/ask"):
            select_quote_snapshots([], evaluation, [])

    def test_same_session_fallback_preserves_stale_quote_evidence(self):
        evaluation = datetime(2026, 5, 12, 9, 40, tzinfo=EASTERN)
        stale_tick = self._tick(evaluation - timedelta(seconds=60), 99.98, 100.00)

        class FakeClient:
            def __init__(self):
                self.calls = []

            def fetch_bid_ask_ticks(self, symbol, start, end, *, use_rth):
                self.calls.append((symbol, start, end, use_rth))
                return [] if len(self.calls) == 1 else [stale_tick]

        client = FakeClient()
        ticks, snapshots = collect_quote_evidence(
            client,
            "TEST",
            evaluation.replace(hour=9, minute=30),
            evaluation,
            [
                {
                    "epoch": int((evaluation - timedelta(minutes=2)).timestamp()),
                    "volume": 12_000,
                }
            ],
        )

        self.assertEqual(ticks, [stale_tick])
        self.assertEqual(len(client.calls), 2)
        self.assertEqual([row["age_seconds"] for row in snapshots], [50.0, 55.0, 60.0])


if __name__ == "__main__":
    unittest.main()

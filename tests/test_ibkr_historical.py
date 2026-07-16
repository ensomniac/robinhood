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
    ensure_tws_socket,
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


if __name__ == "__main__":
    unittest.main()

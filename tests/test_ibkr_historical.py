import json
import tempfile
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from datetime import timezone
from pathlib import Path
from unittest.mock import patch
from zoneinfo import ZoneInfo

from ibkr_historical import (
    BLOCKED_TWS_METHODS,
    IBKRConfig,
    IBKRConfigurationError,
    IBKRHistoricalClient,
    IBKRRequestError,
    _IBKRHistoricalConnection,
    ensure_tws_socket,
    historical_error_category,
    probe_historical_symbol,
)


EASTERN = ZoneInfo("America/New_York")


class ConfigTests(unittest.TestCase):
    def test_loads_historical_settings_without_account_or_secret(self):
        with tempfile.TemporaryDirectory() as directory:
            env_path = Path(directory) / ".env"
            env_path.write_text(
                "IBKR_HOST=127.0.0.1\n"
                "IBKR_PORT=7497\n"
                "IBKR_CLIENT_ID=88\n"
                "IBKR_MAX_CONCURRENT_REQUESTS=7\n"
                "IBKR_AUTO_START_TWS=true\n",
                encoding="utf-8",
            )
            config = IBKRConfig.from_env(env_path)

        self.assertEqual(config.port, 7497)
        self.assertEqual(config.client_id, 88)
        self.assertEqual(config.max_concurrent_requests, 7)
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

    def test_private_tws_connection_blocks_nonhistorical_surfaces(self):
        connection = _IBKRHistoricalConnection(IBKRConfig())

        for method_name in BLOCKED_TWS_METHODS:
            with self.assertRaisesRegex(
                IBKRConfigurationError, "historical-data-only"
            ):
                getattr(connection, method_name)


class NormalizationAndPacingTests(unittest.TestCase):
    def test_daily_and_epoch_bar_timestamps_are_normalized(self):
        daily = IBKRHistoricalClient._bar_timestamp("20260512")
        epoch = IBKRHistoricalClient._bar_timestamp("1778592600")

        self.assertEqual(daily.astimezone(EASTERN).date().isoformat(), "2026-05-12")
        self.assertIsNotNone(epoch)
        self.assertEqual(epoch.tzinfo, timezone.utc)

    def test_threads_reserve_distinct_paced_send_times(self):
        spacing = 0.02
        connection = _IBKRHistoricalConnection(
            IBKRConfig(
                minimum_request_spacing_seconds=spacing,
                max_concurrent_requests=3,
            )
        )

        with ThreadPoolExecutor(max_workers=3) as executor:
            completed = list(
                executor.map(
                    lambda _: (connection._pacing_wait(), time.monotonic()), range(3)
                )
            )

        observed = sorted(stamp for _, stamp in completed)
        gaps = [right - left for left, right in zip(observed, observed[1:])]
        self.assertTrue(all(gap >= spacing * 0.75 for gap in gaps), gaps)

    def test_message_rate_error_is_retryable_provider_failure(self):
        error = IBKRRequestError("maximum message rate exceeded", error_code=100)

        self.assertEqual(historical_error_category(error), "retryable_provider")


class SymbolResolutionTests(unittest.TestCase):
    DETAILS = [
        {
            "symbol": "ALK",
            "security_type": "STK",
            "stock_type": "COMMON",
            "currency": "USD",
        }
    ]

    def test_contract_details_cache_reuses_symbol_proof(self):
        with tempfile.TemporaryDirectory() as directory:
            cache_root = Path(directory)
            first = IBKRHistoricalClient(IBKRConfig(), contract_cache_root=cache_root)
            with patch.object(
                first._connection, "fetch_contract_details", return_value=self.DETAILS
            ) as provider:
                self.assertEqual(first.fetch_contract_details("alk"), self.DETAILS)
                self.assertEqual(first.fetch_contract_details("ALK"), self.DETAILS)
            provider.assert_called_once_with("ALK")

            second = IBKRHistoricalClient(IBKRConfig(), contract_cache_root=cache_root)
            with patch.object(
                second._connection,
                "fetch_contract_details",
                side_effect=AssertionError("provider should not be called"),
            ):
                self.assertEqual(second.fetch_contract_details("ALK"), self.DETAILS)
            self.assertEqual(second.request_telemetry()["contract_cache"]["hits"], 1)

    def test_unresolvable_symbol_is_cached_briefly_without_private_root(self):
        with tempfile.TemporaryDirectory() as directory:
            cache_root = Path(directory)
            first = IBKRHistoricalClient(IBKRConfig(), contract_cache_root=cache_root)
            with patch.object(
                first._connection,
                "fetch_contract_details",
                side_effect=IBKRRequestError("missing", error_code=200),
            ):
                result = probe_historical_symbol(first, "SEMR")
            self.assertFalse(result["viable"])

            cache_path = next(cache_root.rglob("SEMR.json"))
            payload = json.loads(cache_path.read_text(encoding="utf-8"))
            self.assertNotIn(str(cache_root), json.dumps(payload))

    def test_provider_wide_failure_is_not_downgraded(self):
        class FakeClient:
            def fetch_contract_details(self, symbol):
                raise IBKRRequestError("permission", error_code=10187)

        with self.assertRaisesRegex(IBKRRequestError, "permission"):
            probe_historical_symbol(FakeClient(), "ALK")

    def test_resolved_us_common_stock_is_available(self):
        class FakeClient:
            def fetch_contract_details(self, symbol):
                return [
                    {
                        "symbol": symbol,
                        "security_type": "STK",
                        "stock_type": "COMMON",
                        "currency": "USD",
                    }
                ]

        result = probe_historical_symbol(FakeClient(), "ALK")

        self.assertTrue(result["viable"])
        self.assertEqual(result["reason"], "contract_resolved")


if __name__ == "__main__":
    unittest.main()

import json
import tempfile
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch
from zoneinfo import ZoneInfo

from ibkr_historical import (
    IBKRConfig,
    IBKRConfigurationError,
    IBKRHistoricalClient,
    IBKRRequestError,
    PRE_SESSION_CACHE_VERSION,
    _IBKRHistoricalConnection,
    collect_candidate_history,
    collect_quote_evidence,
    ensure_tws_socket,
    historical_error_category,
    is_retryable_historical_error,
    probe_historical_candidate,
    probe_historical_candidate_with_history,
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


class NormalizationTests(unittest.TestCase):
    def test_daily_and_epoch_bar_timestamps_are_normalized_to_eastern(self):
        daily = IBKRHistoricalClient._bar_timestamp("20260512")
        epoch = IBKRHistoricalClient._bar_timestamp("1778592600")

        self.assertEqual(daily.astimezone(EASTERN).date().isoformat(), "2026-05-12")
        self.assertIsNotNone(epoch)
        self.assertEqual(epoch.tzinfo, timezone.utc)


class ConcurrentRequestTests(unittest.TestCase):
    def test_threads_reserve_distinct_paced_send_times(self):
        spacing = 0.02
        connection = _IBKRHistoricalConnection(
            IBKRConfig(
                minimum_request_spacing_seconds=spacing,
                max_concurrent_requests=3,
            )
        )

        with ThreadPoolExecutor(max_workers=3) as executor:
            completed = sorted(
                executor.map(
                    lambda _: (connection._pacing_wait(), time.monotonic()), range(3)
                )
            )

        observed = sorted(stamp for _, stamp in completed)
        gaps = [right - left for left, right in zip(observed, observed[1:])]
        self.assertTrue(all(gap >= spacing * 0.75 for gap in gaps), gaps)

    def test_message_rate_error_is_retryable_provider_failure(self):
        error = IBKRRequestError(
            "maximum allowed message rate exceeded",
            error_code=100,
        )

        self.assertEqual(historical_error_category(error), "retryable_provider")


class SymbolProbeTests(unittest.TestCase):
    def test_contract_details_cache_reuses_symbol_proof_across_clients(self):
        details = [
            {
                "symbol": "ALK",
                "security_type": "STK",
                "stock_type": "COMMON",
                "currency": "USD",
            }
        ]
        with tempfile.TemporaryDirectory() as directory:
            cache_root = Path(directory)
            first = IBKRHistoricalClient(IBKRConfig(), contract_cache_root=cache_root)
            with patch.object(
                first._connection, "fetch_contract_details", return_value=details
            ) as provider:
                self.assertEqual(first.fetch_contract_details("alk"), details)
                self.assertEqual(first.fetch_contract_details("ALK"), details)
            provider.assert_called_once_with("ALK")

            second = IBKRHistoricalClient(IBKRConfig(), contract_cache_root=cache_root)
            with patch.object(
                second._connection,
                "fetch_contract_details",
                side_effect=AssertionError("provider should not be called"),
            ):
                self.assertEqual(second.fetch_contract_details("ALK"), details)
            self.assertEqual(second.request_telemetry()["contract_cache"]["hits"], 1)

    def test_contract_cache_keeps_unresolvable_symbols_short_lived_and_local(self):
        with tempfile.TemporaryDirectory() as directory:
            cache_root = Path(directory)
            first = IBKRHistoricalClient(IBKRConfig(), contract_cache_root=cache_root)
            with patch.object(
                first._connection,
                "fetch_contract_details",
                side_effect=IBKRRequestError("missing", error_code=200),
            ) as provider:
                result = probe_historical_symbol(first, "SEMR")
            provider.assert_called_once_with("SEMR")
            self.assertFalse(result["viable"])

            second = IBKRHistoricalClient(IBKRConfig(), contract_cache_root=cache_root)
            with patch.object(
                second._connection,
                "fetch_contract_details",
                side_effect=AssertionError("provider should not be called"),
            ):
                cached = probe_historical_symbol(second, "SEMR")
            self.assertEqual(cached["reason"], "unresolvable_security_definition")
            cache_path = next(cache_root.rglob("SEMR.json"))
            payload = json.loads(cache_path.read_text(encoding="utf-8"))
            captured = datetime.fromisoformat(payload["captured_at"])
            expires = datetime.fromisoformat(payload["expires_at"])
            self.assertEqual((expires - captured).total_seconds(), 24 * 60 * 60)
            self.assertNotIn("root", second.request_telemetry()["contract_cache"])

    def test_contract_cache_single_flights_concurrent_symbol_requests(self):
        details = [
            {
                "symbol": "ALK",
                "security_type": "STK",
                "stock_type": "COMMON",
                "currency": "USD",
            }
        ]
        with tempfile.TemporaryDirectory() as directory:
            client = IBKRHistoricalClient(
                IBKRConfig(), contract_cache_root=Path(directory)
            )

            def slow_provider(symbol):
                time.sleep(0.01)
                return details

            with patch.object(
                client._connection,
                "fetch_contract_details",
                side_effect=slow_provider,
            ) as provider:
                with ThreadPoolExecutor(max_workers=4) as executor:
                    results = list(
                        executor.map(client.fetch_contract_details, ["ALK"] * 4)
                    )

            self.assertEqual(results, [details] * 4)
            provider.assert_called_once_with("ALK")

    def test_contract_cache_never_stores_provider_wide_failure(self):
        details = [
            {
                "symbol": "ALK",
                "security_type": "STK",
                "stock_type": "COMMON",
                "currency": "USD",
            }
        ]
        with tempfile.TemporaryDirectory() as directory:
            cache_root = Path(directory)
            client = IBKRHistoricalClient(IBKRConfig(), contract_cache_root=cache_root)
            with patch.object(
                client._connection,
                "fetch_contract_details",
                side_effect=[
                    IBKRRequestError("permission", error_code=10187),
                    details,
                ],
            ) as provider:
                with self.assertRaisesRegex(IBKRRequestError, "permission"):
                    client.fetch_contract_details("ALK")
                self.assertEqual(list(cache_root.rglob("*.json")), [])
                self.assertEqual(client.fetch_contract_details("ALK"), details)
            self.assertEqual(provider.call_count, 2)

    def test_tampered_contract_cache_fails_closed_and_refreshes(self):
        details = [
            {
                "symbol": "ALK",
                "security_type": "STK",
                "stock_type": "COMMON",
                "currency": "USD",
            }
        ]
        with tempfile.TemporaryDirectory() as directory:
            cache_root = Path(directory)
            first = IBKRHistoricalClient(IBKRConfig(), contract_cache_root=cache_root)
            with patch.object(
                first._connection, "fetch_contract_details", return_value=details
            ):
                first.fetch_contract_details("ALK")
            cache_path = next(cache_root.rglob("ALK.json"))
            payload = json.loads(cache_path.read_text(encoding="utf-8"))
            payload["outcome"]["details"][0]["stock_type"] = "ADR"
            cache_path.write_text(json.dumps(payload), encoding="utf-8")

            second = IBKRHistoricalClient(IBKRConfig(), contract_cache_root=cache_root)
            with patch.object(
                second._connection, "fetch_contract_details", return_value=details
            ) as provider:
                self.assertEqual(second.fetch_contract_details("ALK"), details)
            provider.assert_called_once_with("ALK")
            stats = second.request_telemetry()["contract_cache"]
            self.assertEqual(stats["invalid"], 1)
            self.assertEqual(stats["writes"], 1)

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
                        "stock_type": "COMMON",
                        "currency": "USD",
                    }
                ]

        result = probe_historical_symbol(FakeClient(), "ALK")

        self.assertTrue(result["viable"])
        self.assertEqual(result["reason"], "contract_resolved")

    def test_non_common_stock_contract_is_not_viable(self):
        class FakeClient:
            def fetch_contract_details(self, symbol):
                return [
                    {
                        "symbol": symbol,
                        "security_type": "STK",
                        "stock_type": "ADR",
                        "currency": "USD",
                    }
                ]

        result = probe_historical_symbol(FakeClient(), "TEST")

        self.assertFalse(result["viable"])
        self.assertEqual(result["reason"], "no_matching_us_common_stock_contract")

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
                        "stock_type": "COMMON",
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
                        "stock_type": "COMMON",
                        "currency": "USD",
                    }
                ]

            def fetch_bars(self, symbol, start, end, *, bar_size, what):
                rows = []
                count = 15 if bar_size == "1 day" else 14
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
                            "volume": (
                                0 if bar_size == "5 mins" and offset == 7 else 1000
                            ),
                        }
                    )
                return rows

        result = probe_historical_candidate(FakeClient(), "THO", day)

        self.assertFalse(result["viable"])
        self.assertEqual(result["reason"], "nonpositive_prior_opening_volume")

    def test_symbol_scoped_hmds_no_data_is_a_buffered_skip(self):
        day = datetime(2026, 6, 8, tzinfo=EASTERN).date()

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

            def fetch_bars(self, symbol, start, end, *, bar_size, what):
                if bar_size == "1 day":
                    return [
                        {
                            "epoch": int(
                                datetime.combine(
                                    day - timedelta(days=offset),
                                    datetime.min.time(),
                                    tzinfo=EASTERN,
                                ).timestamp()
                            ),
                            "date_et": (day - timedelta(days=offset)).isoformat(),
                            "volume": 1000,
                        }
                        for offset in range(15, 0, -1)
                    ]
                raise IBKRRequestError(
                    "HMDS query returned no data: SPTX@SMART Trades",
                    error_code=162,
                )

        result, history = probe_historical_candidate_with_history(
            FakeClient(), "SPTX", "2026-06-08"
        )

        self.assertFalse(result["viable"])
        self.assertEqual(result["reason"], "missing_prior_opening_history")
        self.assertIsNone(history)

    def test_daily_volume_gate_skips_opening_history_request(self):
        day = datetime(2026, 3, 3, tzinfo=EASTERN).date()

        class FakeClient:
            def __init__(self):
                self.bar_sizes = []
                self.contract_calls = 0

            def fetch_contract_details(self, symbol):
                self.contract_calls += 1
                return [
                    {
                        "symbol": symbol,
                        "security_type": "STK",
                        "stock_type": "COMMON",
                        "currency": "USD",
                    }
                ]

            def fetch_bars(self, symbol, start, end, *, bar_size, what):
                self.bar_sizes.append(bar_size)
                return [
                    {
                        "epoch": int(
                            datetime.combine(
                                day - timedelta(days=offset),
                                datetime.min.time(),
                                tzinfo=EASTERN,
                            ).timestamp()
                        ),
                        "date_et": (day - timedelta(days=offset)).isoformat(),
                        "open": 10.0,
                        "high": 11.0,
                        "low": 9.0,
                        "close": 10.0,
                        "volume": 100_000,
                    }
                    for offset in range(15, 0, -1)
                ]

        client = FakeClient()
        result, history = probe_historical_candidate_with_history(
            client,
            "TEST",
            day,
            minimum_average_daily_volume_14=1_000_000,
            minimum_daily_atr_14=0.5,
        )

        self.assertFalse(result["viable"])
        self.assertEqual(
            result["reason"], "average_daily_volume_below_strategy_minimum"
        )
        self.assertEqual(client.bar_sizes, ["1 day"])
        self.assertEqual(client.contract_calls, 0)
        self.assertIsNone(history)

    def test_daily_atr_gate_skips_opening_history_request(self):
        day = datetime(2026, 3, 3, tzinfo=EASTERN).date()

        class FakeClient:
            def __init__(self):
                self.bar_sizes = []

            def fetch_contract_details(self, symbol):
                return [
                    {
                        "symbol": symbol,
                        "security_type": "STK",
                        "stock_type": "COMMON",
                        "currency": "USD",
                    }
                ]

            def fetch_bars(self, symbol, start, end, *, bar_size, what):
                self.bar_sizes.append(bar_size)
                return [
                    {
                        "epoch": int(
                            datetime.combine(
                                day - timedelta(days=offset),
                                datetime.min.time(),
                                tzinfo=EASTERN,
                            ).timestamp()
                        ),
                        "date_et": (day - timedelta(days=offset)).isoformat(),
                        "open": 10.0,
                        "high": 10.1,
                        "low": 9.9,
                        "close": 10.0,
                        "volume": 2_000_000,
                    }
                    for offset in range(15, 0, -1)
                ]

        client = FakeClient()
        result, history = probe_historical_candidate_with_history(
            client,
            "TEST",
            day,
            minimum_average_daily_volume_14=1_000_000,
            minimum_daily_atr_14=0.5,
        )

        self.assertFalse(result["viable"])
        self.assertEqual(result["reason"], "daily_atr_below_strategy_minimum")
        self.assertEqual(client.bar_sizes, ["1 day"])
        self.assertIsNone(history)

    def test_strategy_qualified_probe_fetches_daily_before_one_opening_window(self):
        day = datetime(2026, 3, 3, tzinfo=EASTERN).date()

        class FakeClient:
            def __init__(self):
                self.requests = []

            def fetch_contract_details(self, symbol):
                return [
                    {
                        "symbol": symbol,
                        "security_type": "STK",
                        "stock_type": "COMMON",
                        "currency": "USD",
                    }
                ]

            def fetch_bars(self, symbol, start, end, *, bar_size, what):
                self.requests.append((start, end, bar_size))
                count = 15 if bar_size == "1 day" else 14
                hour = 0 if bar_size == "1 day" else 9
                minute = 0 if bar_size == "1 day" else 30
                return [
                    {
                        "epoch": int(
                            datetime.combine(
                                day - timedelta(days=offset),
                                datetime.min.time().replace(hour=hour, minute=minute),
                                tzinfo=EASTERN,
                            ).timestamp()
                        ),
                        "date_et": (day - timedelta(days=offset)).isoformat(),
                        "open": 10.0,
                        "high": 11.0,
                        "low": 9.0,
                        "close": 10.0,
                        "volume": 2_000_000,
                    }
                    for offset in range(count, 0, -1)
                ]

        client = FakeClient()
        result, history = probe_historical_candidate_with_history(
            client,
            "TEST",
            day,
            minimum_average_daily_volume_14=1_000_000,
            minimum_daily_atr_14=0.5,
        )

        self.assertTrue(result["viable"])
        self.assertEqual([row[2] for row in client.requests], ["1 day", "5 mins"])
        self.assertEqual((day - client.requests[1][0].date()).days, 28)
        self.assertEqual(result["average_daily_volume_14"], 2_000_000)
        self.assertEqual(result["daily_atr_14"], 2.0)
        self.assertIsNotNone(history)

    def test_five_minute_28_day_window_uses_one_provider_request(self):
        connection = _IBKRHistoricalConnection(IBKRConfig())
        end = datetime(2026, 3, 3, tzinfo=EASTERN)
        start = end - timedelta(days=28)

        with patch.object(connection, "_request_bars", return_value=[]) as request:
            rows = connection.fetch_bars(
                "TEST", start, end, bar_size="5 mins", what="TRADES"
            )

        self.assertEqual(rows, [])
        request.assert_called_once()
        self.assertEqual(request.call_args.args[2], "28 D")

    def test_pacing_failure_remains_a_batch_blocker(self):
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

            def fetch_bars(self, *args, **kwargs):
                raise IBKRRequestError("pacing violation", error_code=162)

        with self.assertRaisesRegex(IBKRRequestError, "pacing violation"):
            probe_historical_candidate_with_history(FakeClient(), "SPTX", "2026-06-08")

    def test_candidate_collection_reuses_pre_session_history(self):
        day = datetime(2026, 3, 3, tzinfo=EASTERN).date()
        opening_start = datetime(2026, 3, 3, 9, 30, tzinfo=EASTERN)
        minutes = [
            {
                "epoch": int((opening_start + timedelta(minutes=index)).timestamp()),
                "open": 10.0 + index / 100,
                "high": 10.1 + index / 100,
                "low": 9.9 + index / 100,
                "close": 10.05 + index / 100,
                "volume": 1000 + index,
            }
            for index in range(5)
        ]
        prior_opening = []
        daily = []
        for offset in range(20, 0, -1):
            stamp = datetime.combine(
                day - timedelta(days=offset),
                datetime.min.time().replace(hour=9, minute=30),
                tzinfo=EASTERN,
            )
            daily_stamp = stamp.replace(hour=0, minute=0)
            if len(prior_opening) < 14:
                prior_opening.append(
                    {
                        "epoch": int(stamp.timestamp()),
                        "date_et": stamp.date().isoformat(),
                        "volume": 5000,
                    }
                )
            daily.append(
                {
                    "epoch": int(daily_stamp.timestamp()),
                    "date_et": daily_stamp.date().isoformat(),
                    "volume": 1_000_000,
                }
            )
        history = {
            "schema_version": PRE_SESSION_CACHE_VERSION,
            "symbol": "TEST",
            "session_date": day.isoformat(),
            "target_session_prices_observed": False,
            "prior_opening_bars": prior_opening,
            "daily_bars": daily,
        }

        class NoHistoryClient:
            def fetch_bars(self, *args, **kwargs):
                raise AssertionError("pre-session history should come from cache")

        with patch("ibkr_historical.collect_quote_evidence", return_value=([], [])):
            result = collect_candidate_history(
                NoHistoryClient(),
                "TEST",
                day,
                "09:35:00",
                session_bars=minutes,
                pre_session_history=history,
            )

        self.assertEqual(result["opening_bar"]["volume"], 5010)
        self.assertEqual(result["prior_opening_volumes"], [5000] * 14)
        self.assertEqual(len(result["daily_bars"]), 20)


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

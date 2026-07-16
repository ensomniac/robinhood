import json
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch
from zoneinfo import ZoneInfo

from historical_bundle_builder import (
    HistoricalBundleBuildError,
    _build_candidate,
    _canonical_hash,
    _collect_candidate_with_preflight_fallback,
    _failure_row,
    _load_or_collect_candidate,
    _load_pre_session_history,
    _selection_metadata,
    _atr14,
    _market_metrics,
    build_bundle,
    collect_manifest,
    determine_evaluation,
)
from ibkr_historical import IBKRConfigurationError
from historical_learning import validate_bundle
from historical_providers import HistoricalProviderError, MassiveConfig
from ibkr_historical import IBKRRequestError


EASTERN = ZoneInfo("America/New_York")


def session_bars(*, break_index=10):
    start = datetime(2026, 3, 3, 9, 30, tzinfo=EASTERN)
    rows = []
    for index in range(390):
        opening = index < 5
        high = 10.0 if opening else 9.99
        if break_index is not None and index == break_index:
            high = 10.01
        timestamp = start + timedelta(minutes=index)
        rows.append(
            {
                "epoch": int(timestamp.timestamp()),
                "time_et": timestamp.isoformat(),
                "open": 9.8,
                "high": high,
                "low": 9.7,
                "close": 9.9,
                "volume": 1000 + index,
                "wap": 9.85 + index / 100000,
                "interpolated": False,
            }
        )
    return rows


def raw_candidate(symbol, day="2026-03-03"):
    bars = session_bars()
    daily_start = datetime(2026, 1, 1, tzinfo=EASTERN)
    return {
        "provider": "Massive SIP REST API",
        "provenance": {
            "session_bars": "Massive SIP REST API",
            "opening_volume_history": "Massive SIP REST API",
            "daily_bars": "Massive SIP REST API",
            "historical_quotes_and_depth": "Massive SIP REST API",
        },
        "request": {
            "symbol": symbol,
            "date": day,
            "evaluation_time_et": "09:42:00",
        },
        "session_bars": bars,
        "opening_bar": {"volume": 6000},
        "prior_opening_volumes": [1000] * 14,
        "daily_bars": [
            {
                "epoch": int((daily_start + timedelta(days=index)).timestamp()),
                "open": 9.8,
                "high": 10.8,
                "low": 9.6,
                "close": 10.0,
                "volume": 2_000_000,
            }
            for index in range(20)
        ],
        "quote_snapshots": [
            {
                "observed_at_et": f"{day}T{observed}-05:00",
                "age_seconds": 0,
                "bid": 10,
                "ask": 10.01,
                "ask_depth": 10_000,
                "recent_real_1m_volume": 20_000,
            }
            for observed in ("09:41:50", "09:41:55", "09:42:00")
        ],
    }


class EvaluationTimeTests(unittest.TestCase):
    def test_selects_first_opening_range_break(self):
        evaluation, clean_break, opening = determine_evaluation(
            session_bars(break_index=10)
        )

        self.assertEqual(evaluation, "09:42:00")
        self.assertTrue(clean_break)
        self.assertEqual(opening["high"], 10.0)
        self.assertEqual(opening["volume"], 5010.0)

    def test_no_break_uses_cutoff(self):
        evaluation, clean_break, _ = determine_evaluation(
            session_bars(break_index=None)
        )

        self.assertEqual(evaluation, "10:30:00")
        self.assertFalse(clean_break)

    def test_last_eligible_crossing_bar_evaluates_at_cutoff(self):
        evaluation, clean_break, _ = determine_evaluation(
            session_bars(break_index=58)
        )

        self.assertEqual(evaluation, "10:30:00")
        self.assertTrue(clean_break)

    def test_crossing_at_cutoff_is_not_an_entry_signal(self):
        evaluation, clean_break, _ = determine_evaluation(
            session_bars(break_index=59)
        )

        self.assertEqual(evaluation, "10:30:00")
        self.assertFalse(clean_break)


class DerivedMetricTests(unittest.TestCase):
    def test_atr_uses_prior_close_gap(self):
        start = datetime(2026, 1, 1, tzinfo=EASTERN)
        rows = []
        for index in range(15):
            close = 10.0 + index
            timestamp = start + timedelta(days=index)
            rows.append(
                {
                    "epoch": int(timestamp.timestamp()),
                    "high": close + 0.5,
                    "low": close - 0.5,
                    "close": close,
                    "volume": 1000,
                }
            )

        self.assertAlmostEqual(_atr14(rows), 1.5)

    def test_market_metrics_use_only_completed_bars(self):
        metrics = _market_metrics(session_bars(break_index=10), "09:40:00")

        self.assertGreater(metrics["vwap"], 0)
        self.assertTrue(metrics["vwap_flat_or_rising"])
        self.assertAlmostEqual(metrics["last"], 9.9)

    def test_zero_volume_early_window_marks_vwap_trend_unavailable(self):
        day = "2026-03-03"
        raw = raw_candidate("UGI", day)
        raw["session_bars"] = session_bars(break_index=5)
        for row in raw["session_bars"][:5]:
            row["volume"] = 0
        raw["request"]["evaluation_time_et"] = "09:37:00"

        candidate = _build_candidate(
            day,
            {
                "symbol": "UGI",
                "is_common_stock": True,
                "catalyst": {"point_in_time": True},
                "discovery": {"source": "test"},
            },
            raw,
            {
                "SPY": session_bars(break_index=5),
                "QQQ": session_bars(break_index=5),
            },
            1,
            10,
            25_000,
        )

        self.assertFalse(
            candidate["evaluation_payload"]["candidate"]["vwap_flat_or_rising"]
        )


class BundleAssemblyTests(unittest.TestCase):
    def test_builds_a_validator_safe_bundle(self):
        day = "2026-03-03"
        bars = session_bars(break_index=10)
        daily_start = datetime(2026, 1, 1, tzinfo=EASTERN)
        daily = [
            {
                "epoch": int((daily_start + timedelta(days=index)).timestamp()),
                "open": 9.8,
                "high": 10.8,
                "low": 9.6,
                "close": 10.0,
                "volume": 2_000_000,
            }
            for index in range(20)
        ]
        evidence = []
        raw_by_symbol = {}
        for index in range(10):
            symbol = f"T{index:02d}"
            evidence.append(
                {
                    "symbol": symbol,
                    "surprise_rank": index + 1,
                    "report_date": "2026-03-02",
                    "report_timing": "pm",
                    "eps_estimate": 0.1,
                    "eps_actual": 0.2,
                    "is_common_stock": True,
                    "catalyst": {
                        "source_url": f"https://example.com/{symbol}",
                        "published_at": "2026-03-02T21:00:00+00:00",
                        "point_in_time": True,
                    },
                }
            )
            raw_by_symbol[symbol] = {
                "provider": "test historical feed",
                "request": {
                    "symbol": symbol,
                    "date": day,
                    "evaluation_time_et": "09:42:00",
                },
                "session_bars": bars,
                "session_bar_quality": {"complete": True},
                "opening_bar": {"volume": 6000},
                "prior_opening_volumes": [1000] * 14,
                "daily_bars": daily,
                "quote_snapshots": [
                    {
                        "observed_at_et": f"{day}T{observed}-05:00",
                        "age_seconds": 0.0,
                        "bid": 10.0,
                        "ask": 10.01,
                        "ask_depth": 10_000,
                        "recent_real_1m_volume": 20_000,
                    }
                    for observed in ("09:41:50", "09:41:55", "09:42:00")
                ],
            }

        evidence[0] = {
            "symbol": "T00",
            "is_common_stock": True,
            "catalyst": evidence[0]["catalyst"],
            "discovery": {
                "source": "SEC EDGAR daily filing index",
                "form": "8-K",
                "filing_items": "8.01,9.01",
                "accepted_at": "2026-03-02T21:00:00+00:00",
            },
        }

        bundle = build_bundle(
            day,
            evidence,
            raw_by_symbol,
            {"SPY": bars, "QQQ": bars},
            synthetic_equity=25_000,
            scanner={"universe_capture_complete": True},
        )

        validate_bundle(bundle)
        self.assertEqual(len(bundle["candidates"]), 10)
        self.assertEqual(bundle["schema_version"], 2)
        self.assertEqual(bundle["candidates"][0]["evaluation_time_et"], "09:42:00")
        self.assertEqual(
            bundle["candidates"][0]["evaluation_basis"],
            "next_minute_after_completed_breakout_bar",
        )
        self.assertEqual(bundle["candidates"][0]["discovery"]["form"], "8-K")

    def test_non_break_below_opening_range_is_ineligible_not_malformed(self):
        day = "2026-03-03"
        raw = raw_candidate("T00", day)
        raw["session_bars"] = session_bars(break_index=None)
        raw["request"]["evaluation_time_et"] = "10:30:00"
        for quote in raw["quote_snapshots"]:
            quote["bid"] = 9.49
            quote["ask"] = 9.50

        candidate = _build_candidate(
            day,
            {
                "symbol": "T00",
                "is_common_stock": True,
                "catalyst": {"point_in_time": True},
                "discovery": {"source": "test"},
            },
            raw,
            {"SPY": session_bars(), "QQQ": session_bars()},
            1,
            10,
            25_000,
        )

        payload = candidate["evaluation_payload"]["candidate"]
        self.assertLess(payload["technical_invalidation"], payload["entry_limit"])
        self.assertFalse(payload["stop_outside_noise"])


class CollectionControlTests(unittest.TestCase):
    def test_incompatible_candidate_cache_is_refreshed_once(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            stale = raw_candidate("T00")
            stale["request"]["evaluation_time_et"] = "09:40:00"
            path = root / "2026-03-03-T00.json"
            path.write_text(json.dumps(stale), encoding="utf-8")
            fresh = raw_candidate("T00")
            with (
                patch(
                    "historical_bundle_builder._load_pre_session_history",
                    return_value={"schema_version": 2},
                ),
                patch(
                    "historical_bundle_builder."
                    "_collect_candidate_with_preflight_fallback",
                    return_value=(fresh, True),
                ) as collect,
            ):
                raw, cache_hit, reused = _load_or_collect_candidate(
                    object(),
                    root,
                    {"cache": {}},
                    "2026-03-03",
                    {"symbol": "T00"},
                )
            persisted = json.loads(path.read_text(encoding="utf-8"))

        self.assertEqual(collect.call_count, 1)
        self.assertFalse(cache_hit)
        self.assertTrue(reused)
        self.assertEqual(raw["request"]["evaluation_time_et"], "09:42:00")
        self.assertEqual(persisted["request"]["evaluation_time_et"], "09:42:00")

    def test_loads_matching_reusable_preflight_history(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            cache_file = root / "2026-03-03" / "T00.json"
            cache_file.parent.mkdir(parents=True)
            history = {
                "schema_version": 2,
                "symbol": "T00",
                "session_date": "2026-03-03",
                "target_session_prices_observed": False,
                "prior_opening_bars": [],
                "daily_bars": [],
            }
            cache_file.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "probe_contract_version": 2,
                        "symbol": "T00",
                        "session_date": "2026-03-03",
                        "result": {"symbol": "T00", "viable": True},
                        "pre_session_history": history,
                    }
                ),
                encoding="utf-8",
            )

            loaded = _load_pre_session_history(
                {
                    "cache": {
                        "root": str(root),
                        "schema_version": 1,
                        "reusable_pre_session_history": True,
                    }
                },
                "2026-03-03",
                "T00",
            )

        self.assertEqual(loaded, history)

    def test_v2_preflight_history_is_bound_to_manifest_hash(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            cache_file = root / "2026-03-03" / "T00.json"
            cache_file.parent.mkdir(parents=True)
            history = {
                "schema_version": 2,
                "symbol": "T00",
                "session_date": "2026-03-03",
                "target_session_prices_observed": False,
                "prior_opening_bars": [],
                "daily_bars": [],
            }
            history_hash = _canonical_hash(history)
            cache_file.write_text(
                json.dumps(
                    {
                        "schema_version": 2,
                        "probe_contract_version": 2,
                        "qualification_sha256": "rules-hash",
                        "symbol": "T00",
                        "session_date": "2026-03-03",
                        "result": {
                            "symbol": "T00",
                            "viable": True,
                            "pre_session_history_sha256": history_hash,
                        },
                        "pre_session_history": history,
                    }
                ),
                encoding="utf-8",
            )
            preflight = {
                "schema_version": 2,
                "dates": {
                    "2026-03-03": {
                        "accepted": [
                            {
                                "symbol": "T00",
                                "pre_session_history_sha256": history_hash,
                            }
                        ]
                    }
                },
                "cache": {
                    "root": str(root),
                    "schema_version": 2,
                    "qualification_sha256": "rules-hash",
                    "reusable_pre_session_history": True,
                },
            }

            self.assertEqual(
                _load_pre_session_history(preflight, "2026-03-03", "T00"),
                history,
            )
            preflight["cache"]["schema_version"] = 3
            self.assertIsNone(
                _load_pre_session_history(preflight, "2026-03-03", "T00")
            )
            preflight["cache"]["schema_version"] = 2
            preflight["dates"]["2026-03-03"]["accepted"][0][
                "pre_session_history_sha256"
            ] = "0" * 64
            self.assertIsNone(
                _load_pre_session_history(preflight, "2026-03-03", "T00")
            )

    def test_incompatible_preflight_history_falls_back_to_normal_collection(self):
        calls = []

        def fake_collect(client, symbol, day, *, pre_session_history=None):
            calls.append(pre_session_history)
            if pre_session_history is not None:
                raise IBKRConfigurationError("stale cache")
            return {"symbol": symbol, "session_date": day}

        with patch(
            "historical_bundle_builder._collect_candidate_raw",
            side_effect=fake_collect,
        ):
            raw, reused = _collect_candidate_with_preflight_fallback(
                object(),
                "T00",
                "2026-03-03",
                {"schema_version": 0},
            )

        self.assertEqual(raw["symbol"], "T00")
        self.assertFalse(reused)
        self.assertEqual(calls, [{"schema_version": 0}, None])

    def test_linked_selection_is_authoritative_for_large_integer_seed(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            selection = root / "selection.json"
            selection.write_text(
                json.dumps(
                    {
                        "seed": 367306429345783127,
                        "selected_dates": ["2026-03-03"],
                    }
                ),
                encoding="utf-8",
            )
            dates, seed, integrity = _selection_metadata(
                root / "evidence.json",
                {
                    "selection_file": str(selection),
                    "selection_seed": 367306429345783100,
                },
                {"2026-03-03": []},
            )

        self.assertEqual(dates, ["2026-03-03"])
        self.assertEqual(seed, 367306429345783127)
        self.assertFalse(integrity["copied_seed_matches"])

    def test_cached_provider_shape_failure_is_permanent_fidelity(self):
        failure = _failure_row(
            "2026-03-03",
            "THO",
            "candidate",
            HistoricalBundleBuildError("nonpositive opening volume"),
        )

        self.assertEqual(failure["category"], "permanent_fidelity")
        self.assertFalse(failure["retryable"])

    def _manifest(self, root, symbols=("T00", "T01")):
        path = root / "manifest.json"
        path.write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "selection_seed": 17,
                    "scanner": {"universe_capture_complete": True},
                    "candidates_by_date": {
                        "2026-03-03": [
                            {"symbol": symbol, "catalyst": {"point_in_time": True}}
                            for symbol in symbols
                        ]
                    },
                }
            ),
            encoding="utf-8",
        )
        return path

    def test_transport_error_aborts_immediately_and_persists_interruption(self):
        class FakeClient:
            def __enter__(self):
                return self

            def __exit__(self, *args):
                return None

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest = self._manifest(root)
            status = root / "status.json"
            error = IBKRRequestError("TWS connection closed", error_code=507)
            with (
                patch(
                    "historical_bundle_builder.IBKRConfig.from_env",
                    return_value=object(),
                ),
                patch(
                    "historical_bundle_builder.IBKRHistoricalClient",
                    return_value=FakeClient(),
                ),
                patch(
                    "historical_bundle_builder._benchmark_history", side_effect=error
                ) as benchmark,
            ):
                with self.assertRaisesRegex(IBKRRequestError, "connection closed"):
                    collect_manifest(
                        manifest,
                        data_root=root / "data",
                        status_path=status,
                        transport_retries=0,
                        max_workers=1,
                    )
            persisted = json.loads(status.read_text(encoding="utf-8"))

        self.assertEqual(benchmark.call_count, 1)
        self.assertTrue(persisted["interrupted"])
        failure = persisted["blocked_dates"][0]["failures"][0]
        self.assertEqual(failure["category"], "retryable_transport")
        self.assertTrue(failure["retryable"])

    def test_transport_error_reconnects_once_then_resumes_cached_batch(self):
        class FakeClient:
            def __enter__(self):
                return self

            def __exit__(self, *args):
                return None

        calls = 0

        def benchmark(client, day, symbol):
            nonlocal calls
            calls += 1
            if calls == 1:
                raise IBKRRequestError("TWS connection closed", error_code=507)
            return {"provider": "fake", "session_bars": session_bars()}

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest = self._manifest(root)
            with (
                patch(
                    "historical_bundle_builder.IBKRConfig.from_env",
                    return_value=object(),
                ),
                patch(
                    "historical_bundle_builder.IBKRHistoricalClient",
                    side_effect=[FakeClient(), FakeClient()],
                ) as client_factory,
                patch(
                    "historical_bundle_builder._benchmark_history",
                    side_effect=benchmark,
                ),
                patch(
                    "historical_bundle_builder._collect_candidate_raw",
                    side_effect=IBKRRequestError("permanent missing quote"),
                ),
            ):
                result = collect_manifest(
                    manifest,
                    data_root=root / "data",
                    env_file=root / "missing.env",
                    status_path=root / "status.json",
                )

        self.assertEqual(client_factory.call_count, 2)
        self.assertEqual(calls, 3)
        self.assertFalse(result["valid"])
        self.assertEqual(result["failures"][0]["category"], "retryable_transport")
        self.assertEqual(result["failures"][-1]["category"], "permanent_fidelity")

    def test_permanent_candidate_failure_stops_that_date_without_cascade(self):
        class FakeClient:
            def __enter__(self):
                return self

            def __exit__(self, *args):
                return None

            def fetch_bars(self, *args, **kwargs):
                return session_bars()

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest = self._manifest(root)
            status = root / "status.json"
            error = IBKRRequestError("no historical bid/ask evidence")
            with (
                patch(
                    "historical_bundle_builder.IBKRConfig.from_env",
                    return_value=object(),
                ),
                patch(
                    "historical_bundle_builder.IBKRHistoricalClient",
                    return_value=FakeClient(),
                ),
                patch(
                    "historical_bundle_builder._benchmark_history",
                    side_effect=lambda client, day, symbol: {
                        "provider": "fake",
                        "session_bars": session_bars(),
                    },
                ),
                patch(
                    "historical_bundle_builder.collect_candidate_history",
                    side_effect=error,
                ) as collect,
            ):
                result = collect_manifest(
                    manifest,
                    data_root=root / "data",
                    status_path=status,
                    max_workers=1,
                )
            persisted = json.loads(status.read_text(encoding="utf-8"))

        self.assertEqual(collect.call_count, 1)
        self.assertFalse(result["valid"])
        self.assertFalse(persisted["interrupted"])
        failure = result["failures"][0]
        self.assertEqual(failure["category"], "permanent_fidelity")
        self.assertFalse(failure["retryable"])

    def test_permanent_primary_gap_uses_configured_fallback(self):
        class PrimaryClient:
            provider_name = "Interactive Brokers TWS API"

            def __enter__(self):
                return self

            def __exit__(self, *args):
                return None

        class FallbackClient:
            provider_name = "Massive SIP REST API"

            def close(self):
                return None

        fallback = FallbackClient()
        primary_error = IBKRRequestError("no historical quote evidence")

        def collect(client, symbol, day):
            if isinstance(client, PrimaryClient):
                raise primary_error
            if symbol == "T00":
                return raw_candidate(symbol, day)
            raise HistoricalProviderError(
                "fallback has no data", category="permanent_fidelity"
            )

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest = self._manifest(root)
            with (
                patch(
                    "historical_bundle_builder.IBKRConfig.from_env",
                    return_value=object(),
                ),
                patch(
                    "historical_bundle_builder.IBKRHistoricalClient",
                    return_value=PrimaryClient(),
                ),
                patch(
                    "historical_bundle_builder.MassiveConfig.optional_from_env",
                    return_value=MassiveConfig(api_key="configured"),
                ),
                patch(
                    "historical_bundle_builder.MassiveHistoricalClient",
                    return_value=fallback,
                ),
                patch(
                    "historical_bundle_builder._benchmark_history",
                    side_effect=lambda client, day, symbol: {
                        "provider": getattr(client, "provider_name"),
                        "session_bars": session_bars(),
                    },
                ),
                patch(
                    "historical_bundle_builder._collect_candidate_raw",
                    side_effect=collect,
                ),
            ):
                result = collect_manifest(
                    manifest,
                    data_root=root / "data",
                    env_file=root / "missing.env",
                    status_path=root / "status.json",
                )

        self.assertEqual(len(result["fallback_recoveries"]), 1)
        recovery = result["fallback_recoveries"][0]
        self.assertEqual(recovery["symbol"], "T00")
        self.assertEqual(recovery["fallback_provider"], "Massive SIP REST API")
        self.assertEqual(result["failures"][0]["stage"], "candidate_fallback")

    def test_retryable_optional_fallback_does_not_reconnect_primary(self):
        class PrimaryClient:
            provider_name = "Interactive Brokers TWS API"

            def __enter__(self):
                return self

            def __exit__(self, *args):
                return None

        class FallbackClient:
            provider_name = "Massive SIP REST API"

            def close(self):
                return None

        def collect(client, symbol, day):
            if isinstance(client, PrimaryClient):
                raise IBKRRequestError("no historical quote evidence")
            raise HistoricalProviderError(
                "Massive HTTP 429", category="retryable_provider"
            )

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest = self._manifest(root)
            status = root / "status.json"
            with (
                patch(
                    "historical_bundle_builder.IBKRConfig.from_env",
                    return_value=object(),
                ),
                patch(
                    "historical_bundle_builder.IBKRHistoricalClient",
                    return_value=PrimaryClient(),
                ) as primary_factory,
                patch(
                    "historical_bundle_builder.MassiveConfig.optional_from_env",
                    return_value=MassiveConfig(api_key="configured"),
                ),
                patch(
                    "historical_bundle_builder.MassiveHistoricalClient",
                    return_value=FallbackClient(),
                ),
                patch(
                    "historical_bundle_builder._benchmark_history",
                    side_effect=lambda client, day, symbol: {
                        "provider": client.provider_name,
                        "session_bars": session_bars(),
                    },
                ),
                patch(
                    "historical_bundle_builder._collect_candidate_raw",
                    side_effect=collect,
                ),
            ):
                result = collect_manifest(
                    manifest,
                    data_root=root / "data",
                    status_path=status,
                    transport_retries=0,
                )
            persisted = json.loads(status.read_text(encoding="utf-8"))

        self.assertEqual(primary_factory.call_count, 1)
        self.assertFalse(result["valid"])
        self.assertEqual(result["failures"][0]["stage"], "candidate_fallback")
        self.assertEqual(result["failures"][0]["category"], "retryable_provider")
        self.assertFalse(persisted["interrupted"])
        self.assertIn("performance", persisted)


if __name__ == "__main__":
    unittest.main()

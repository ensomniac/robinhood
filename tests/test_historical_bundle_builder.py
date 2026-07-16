import unittest
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from historical_bundle_builder import (
    _atr14,
    _market_metrics,
    build_bundle,
    determine_evaluation,
)
from historical_learning import validate_bundle


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


class EvaluationTimeTests(unittest.TestCase):
    def test_selects_first_opening_range_break(self):
        evaluation, clean_break, opening = determine_evaluation(
            session_bars(break_index=10)
        )

        self.assertEqual(evaluation, "09:40:00")
        self.assertTrue(clean_break)
        self.assertEqual(opening["high"], 10.0)
        self.assertEqual(opening["volume"], 5010.0)

    def test_no_break_uses_cutoff(self):
        evaluation, clean_break, _ = determine_evaluation(
            session_bars(break_index=None)
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
                "request": {
                    "symbol": symbol,
                    "date": day,
                    "evaluation_time_et": "09:40:00",
                },
                "session_bars": bars,
                "session_bar_quality": {"complete": True},
                "opening_bar": {"volume": 6000},
                "prior_opening_volumes": [1000] * 14,
                "daily_bars": daily,
                "quote_snapshots": [
                    {
                        "observed_at_et": f"{day}T09:39:{second:02d}-05:00",
                        "age_seconds": 0.0,
                        "bid": 10.0,
                        "ask": 10.01,
                        "ask_depth": 10_000,
                        "recent_real_1m_volume": 20_000,
                    }
                    for second in (50, 55, 59)
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
        self.assertEqual(bundle["candidates"][0]["evaluation_time_et"], "09:40:00")
        self.assertEqual(bundle["candidates"][0]["discovery"]["form"], "8-K")


if __name__ == "__main__":
    unittest.main()

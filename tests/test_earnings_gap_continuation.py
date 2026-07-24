from __future__ import annotations

import unittest

import earnings_gap_continuation as subject


class EarningsGapContinuationTests(unittest.TestCase):
    def test_event_windows_cover_each_2025_month_once(self):
        windows = subject._event_windows()
        self.assertEqual(len(windows), 12)
        self.assertEqual(windows[0]["start_date"], "2025-01-01")
        self.assertEqual(windows[0]["days"], 31)
        self.assertEqual(windows[1]["days"], 28)
        self.assertEqual(windows[-1]["start_date"], "2025-12-01")
        self.assertEqual(windows[-1]["days"], 31)
        self.assertEqual(subject.PRIOR_DISCARDED_PROVIDER_REQUESTS, 12)

    def test_normalize_calendar_row_accepts_nested_verified_result(self):
        row = subject._normalize_calendar_row(
            {
                "symbol": "AAPL",
                "report": {
                    "date": "2025-01-30",
                    "timing": "pm",
                    "verified": True,
                },
                "eps": {"actual": "2.40", "estimate": "2.35"},
            }
        )
        self.assertEqual(
            row,
            {
                "symbol": "AAPL",
                "report_date": "2025-01-30",
                "timing": "pm",
                "verified": True,
                "actual_eps": 2.4,
                "estimated_eps": 2.35,
            },
        )

    def test_normalize_calendar_row_rejects_bad_timing(self):
        self.assertIsNone(
            subject._normalize_calendar_row(
                {
                    "symbol": "AAPL",
                    "report": {
                        "date": "2025-01-30",
                        "timing": "unknown",
                        "verified": True,
                    },
                    "eps": {"actual": 2.4, "estimate": 2.35},
                }
            )
        )

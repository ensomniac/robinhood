from __future__ import annotations

import json
import unittest
from datetime import date, datetime, time, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import post_earnings_drift_stage0 as stage0


EASTERN = ZoneInfo("America/New_York")


class PostEarningsDriftStage0Tests(unittest.TestCase):
    def test_published_activation_inspection_is_return_blind(self):
        root = Path(__file__).resolve().parents[1]
        path = (
            root
            / "strategy_tournament"
            / "inspections"
            / "post-earnings-drift-v1-activation-fad0b77a77d8ae2d2d68197e00a1c25dd7bc189969c3b8db05a514784cd573e5.json"
        )
        inspection = json.loads(path.read_text(encoding="utf-8"))
        self.assertEqual(
            inspection["inspection_sha256"],
            stage0.common._self_hash(inspection, "inspection_sha256"),
        )
        self.assertEqual(inspection["candidate_pairs"], 102)
        self.assertEqual(inspection["provider_requests"], 0)
        self.assertEqual(inspection["returns_computed"], 0)
        self.assertTrue(inspection["collection_authorized"])
        self.assertFalse(inspection["return_evaluation_authorized"])

    def test_manifest_freezes_complete_source_verified_denominator(self):
        manifest = stage0.build_manifest()
        stage0._validate_manifest(manifest)
        self.assertEqual(manifest["denominator"]["candidate_pairs"], 102)
        self.assertEqual(manifest["denominator"]["candidate_dates"], 52)
        self.assertEqual(manifest["denominator"]["unique_symbols"], 84)
        self.assertEqual(manifest["declared_prior_policy_trials"], 15)
        self.assertFalse(manifest["provider_requests_authorized_before_inspection"])
        self.assertFalse(
            manifest["return_evaluation_authorized_before_input_inspection"]
        )

    def test_normalize_earnings_retains_verified_numeric_inputs(self):
        response = {
            "data": {
                "results": [
                    {
                        "symbol": "TEST",
                        "year": 2025,
                        "quarter": 2,
                        "eps": {"actual": "1.25", "estimate": "1.10"},
                        "report": {
                            "date": "2025-05-01",
                            "timing": "pm",
                            "verified": True,
                        },
                    }
                ]
            }
        }
        normalized = stage0._normalize_earnings("TEST", response)
        self.assertEqual(normalized["status"], "ok")
        self.assertEqual(normalized["results"][0]["actual_eps"], 1.25)
        self.assertTrue(normalized["results"][0]["report"]["verified"])

    def test_reaction_session_uses_next_session_for_pm_report(self):
        sessions = ["2025-05-01", "2025-05-02", "2025-05-05"]
        self.assertEqual(
            stage0._reaction_day(
                {"date": "2025-05-01", "timing": "pm"}, sessions
            ),
            "2025-05-02",
        )
        self.assertEqual(
            stage0._reaction_day(
                {"date": "2025-05-01", "timing": "am"}, sessions
            ),
            "2025-05-01",
        )

    def test_complete_minute_session_rejects_a_missing_bar(self):
        day = date.fromisoformat("2025-05-01")
        rows = [
            {
                "time_et": (
                    datetime.combine(day, time(9, 30), tzinfo=EASTERN)
                    + timedelta(minutes=offset)
                ).isoformat()
            }
            for offset in range(390)
        ]
        self.assertTrue(stage0._complete_minute_session(rows, day.isoformat()))
        self.assertFalse(stage0._complete_minute_session(rows[:-1], day.isoformat()))

    def test_stop_first_and_cost_stress_are_conservative(self):
        day = date.fromisoformat("2025-05-01")
        minutes = [
            {
                "time_et": (
                    datetime.combine(day, time(9, 30), tzinfo=EASTERN)
                    + timedelta(minutes=offset)
                ).isoformat(),
                "open": 100.0,
                "high": 102.0,
                "low": 99.0,
                "close": 101.0,
                "volume": 100,
                "wap": 100.5,
            }
            for offset in range(390)
        ]
        minutes[377]["low"] = 98.0
        holding = ["2025-05-01", "2025-05-02", "2025-05-05", "2025-05-06", "2025-05-07"]
        daily = {
            value: {"open": 100.0, "high": 102.0, "low": 99.0, "close": 101.0}
            for value in holding
        }
        primary = stage0._trade_outcome(
            minute_rows=minutes,
            holding_dates=holding,
            daily_by_date=daily,
            stop=98.5,
            cost_bps=5,
        )
        stressed = stage0._trade_outcome(
            minute_rows=minutes,
            holding_dates=holding,
            daily_by_date=daily,
            stop=98.5,
            cost_bps=20,
        )
        self.assertEqual(primary["exit_reason"], "stop")
        self.assertLessEqual(primary["net_r"], 0)
        self.assertLessEqual(stressed["net_r"], primary["net_r"])


if __name__ == "__main__":
    unittest.main()

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

    def test_superseding_inspection_counts_discarded_provider_calls(self):
        root = Path(__file__).resolve().parents[1]
        path = (
            root
            / "strategy_tournament"
            / "inspections"
            / "post-earnings-drift-v1-activation-1f0bc5f5a0416ffe09d832be2f1dac2f72d03236ab775e85c4152dd13ebae58e.json"
        )
        inspection = json.loads(path.read_text(encoding="utf-8"))
        self.assertEqual(
            inspection["inspection_sha256"],
            stage0.common._self_hash(inspection, "inspection_sha256"),
        )
        self.assertEqual(inspection["provider_requests_before_this_activation"], 84)
        self.assertEqual(inspection["provider_requests"], 0)
        self.assertEqual(inspection["returns_computed"], 0)
        self.assertTrue(inspection["collection_authorized"])

    def test_corrected_stream_inspection_counts_both_failed_attempts(self):
        root = Path(__file__).resolve().parents[1]
        path = (
            root
            / "strategy_tournament"
            / "inspections"
            / "post-earnings-drift-v1-activation-e728d442e0e32c86ccb4447595f4245a195276fd2b9084c85b2e488fe24b3c0e.json"
        )
        inspection = json.loads(path.read_text(encoding="utf-8"))
        self.assertEqual(
            inspection["inspection_sha256"],
            stage0.common._self_hash(inspection, "inspection_sha256"),
        )
        self.assertEqual(inspection["provider_requests_before_this_activation"], 168)
        self.assertEqual(inspection["provider_requests"], 0)
        self.assertEqual(inspection["returns_computed"], 0)
        self.assertTrue(inspection["collection_authorized"])

    def test_market_boundary_inspection_counts_all_prior_provider_calls(self):
        root = Path(__file__).resolve().parents[1]
        path = (
            root
            / "strategy_tournament"
            / "inspections"
            / "post-earnings-drift-v1-activation-0159e738fe1e50c3248ea1496d560b264419f80077fc906b91d4cb6373c5d5f4.json"
        )
        inspection = json.loads(path.read_text(encoding="utf-8"))
        self.assertEqual(
            inspection["inspection_sha256"],
            stage0.common._self_hash(inspection, "inspection_sha256"),
        )
        self.assertEqual(inspection["provider_requests_before_this_activation"], 255)
        self.assertEqual(inspection["provider_requests"], 0)
        self.assertEqual(inspection["returns_computed"], 0)
        self.assertTrue(inspection["collection_authorized"])

    def test_published_earnings_collection_counts_every_provider_call(self):
        root = Path(__file__).resolve().parents[1]
        path = (
            root
            / "strategy_tournament"
            / "post_earnings_drift"
            / "earnings-status.json"
        )
        status = json.loads(path.read_text(encoding="utf-8"))
        self.assertEqual(
            status["status_sha256"],
            stage0.common._self_hash(status, "status_sha256"),
        )
        self.assertEqual(status["requested_symbols"], 84)
        self.assertEqual(status["effective_provider_requests"], 84)
        self.assertEqual(status["discarded_ingestion_transport_requests"], 168)
        self.assertEqual(status["provider_requests"], 252)
        self.assertEqual(status["earnings_rows"], 648)
        self.assertEqual(status["returns_computed"], 0)
        self.assertEqual(status["broker_actions"], 0)
        self.assertEqual(status["status"], "READY")

    def test_published_market_collection_counts_boundary_and_discarded_calls(self):
        root = Path(__file__).resolve().parents[1]
        path = (
            root
            / "strategy_tournament"
            / "post_earnings_drift"
            / "market-status.json"
        )
        status = json.loads(path.read_text(encoding="utf-8"))
        self.assertEqual(
            status["status_sha256"],
            stage0.common._self_hash(status, "status_sha256"),
        )
        self.assertEqual(status["requested_symbols"], 84)
        self.assertEqual(status["requested_pairs"], 102)
        self.assertEqual(status["symbols_with_daily_rows"], 84)
        self.assertEqual(status["pairs_with_minute_rows"], 102)
        self.assertEqual(status["effective_provider_requests"], 55)
        self.assertEqual(status["discarded_provider_requests"], 3)
        self.assertEqual(status["provider_requests"], 58)
        self.assertEqual(status["ignored_non_regular_session_rows"], 102)
        self.assertEqual(status["returns_computed"], 0)
        self.assertEqual(status["broker_actions"], 0)
        self.assertEqual(status["status"], "READY")

    def test_published_input_inspection_keeps_complete_denominator(self):
        root = Path(__file__).resolve().parents[1]
        path = (
            root
            / "strategy_tournament"
            / "inspections"
            / "post-earnings-drift-v1-input-dfbf41c267099c34fc2e97f88c801b9d6f2b4731c9598cba107c7faef5cbf9cf.json"
        )
        inspection = json.loads(path.read_text(encoding="utf-8"))
        self.assertEqual(
            inspection["inspection_sha256"],
            stage0.common._self_hash(inspection, "inspection_sha256"),
        )
        self.assertEqual(inspection["candidate_pairs"], 102)
        self.assertEqual(inspection["complete_pairs"], 87)
        self.assertEqual(
            inspection["status_counts"],
            {"complete": 87, "incomplete_minute_session": 15},
        )
        self.assertEqual(inspection["provider_requests_during_inspection"], 0)
        self.assertEqual(inspection["returns_computed"], 0)
        self.assertTrue(inspection["return_evaluation_authorized"])

    def test_manifest_freezes_complete_source_verified_denominator(self):
        manifest = stage0.build_manifest()
        stage0._validate_manifest(manifest)
        self.assertEqual(manifest["denominator"]["candidate_pairs"], 102)
        self.assertEqual(manifest["denominator"]["candidate_dates"], 52)
        self.assertEqual(manifest["denominator"]["unique_symbols"], 84)
        self.assertEqual(manifest["declared_prior_policy_trials"], 15)
        self.assertEqual(
            manifest["collection_transport_incident"][
                "discarded_earnings_provider_requests"
            ],
            168,
        )
        self.assertEqual(
            manifest["collection_transport_incident"]["failed_ingestion_attempts"],
            2,
        )
        self.assertEqual(
            manifest["collection_contract"]["logical_earnings_requests"], 84
        )
        self.assertEqual(
            manifest["collection_transport_incident"][
                "discarded_market_provider_requests"
            ],
            3,
        )
        self.assertTrue(
            manifest["collection_transport_incident"]["market_outcomes_accessed"]
        )
        self.assertEqual(
            manifest["activation_rules_hash"],
            "177a304fe28a4923b373975cb025a7b157fa5b8c476e0bca5a6d35c1b2dc2af9",
        )
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

    def test_normalize_minutes_ignores_provider_end_boundary(self):
        rows = []
        day = date.fromisoformat("2025-05-01")
        for offset in range(391):
            rows.append(
                {
                    "t": (
                        datetime.combine(day, time(9, 30), tzinfo=EASTERN)
                        + timedelta(minutes=offset)
                    ).isoformat(),
                    "o": 100.0,
                    "h": 101.0,
                    "l": 99.0,
                    "c": 100.5,
                    "v": 100,
                    "vw": 100.25,
                }
            )
        normalized = stage0._normalize_minutes("TEST", day.isoformat(), rows)
        self.assertEqual(len(normalized), 390)
        self.assertTrue(stage0._complete_minute_session(normalized, day.isoformat()))

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

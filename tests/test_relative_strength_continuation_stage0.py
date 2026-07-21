from __future__ import annotations

import json
import unittest
from datetime import date, datetime, time, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import relative_strength_continuation_stage0 as stage0


EASTERN = ZoneInfo("America/New_York")


def _session(day: str) -> list[dict[str, object]]:
    session_day = date.fromisoformat(day)
    return [
        {
            "time_et": (
                datetime.combine(session_day, time(9, 30), tzinfo=EASTERN)
                + timedelta(minutes=offset)
            ).isoformat(),
            "open": 100.0,
            "high": 100.0,
            "low": 99.0,
            "close": 100.0,
            "volume": 100,
            "wap": 100.0,
        }
        for offset in range(390)
    ]


class RelativeStrengthContinuationStage0Tests(unittest.TestCase):
    def test_published_activation_inspection_keeps_target_outcomes_locked(self):
        root = Path(__file__).resolve().parents[1]
        path = (
            root
            / "strategy_tournament"
            / "inspections"
            / "relative-strength-continuation-v1-activation-3d386ed05e5b616f014169f6a6dfb0088ba146c82579140c66b71463a55586d2.json"
        )
        inspection = json.loads(path.read_text(encoding="utf-8"))
        self.assertEqual(
            inspection["inspection_sha256"],
            stage0.common._self_hash(inspection, "inspection_sha256"),
        )
        self.assertEqual(inspection["target_dates"], 80)
        self.assertEqual(inspection["member_symbol_sessions"], 395_316)
        self.assertEqual(inspection["provider_requests"], 0)
        self.assertEqual(inspection["returns_computed"], 0)
        self.assertTrue(inspection["prefix_collection_authorized"])
        self.assertFalse(inspection["target_outcome_collection_authorized"])

    def test_published_prefix_collection_is_selection_only(self):
        root = Path(__file__).resolve().parents[1]
        path = (
            root
            / "strategy_tournament"
            / "relative_strength_continuation"
            / "prefix-status.json"
        )
        status = json.loads(path.read_text(encoding="utf-8"))
        self.assertEqual(
            status["status_sha256"],
            stage0.common._self_hash(status, "status_sha256"),
        )
        self.assertEqual(status["target_dates"], 80)
        self.assertEqual(status["member_symbol_sessions"], 395_316)
        self.assertEqual(status["provider_requests"], 831)
        self.assertEqual(status["minute_rows"], 6_592_844)
        self.assertEqual(status["ignored_outside_window_rows"], 236_485)
        self.assertEqual(status["returns_computed"], 0)
        self.assertEqual(status["broker_actions"], 0)
        self.assertEqual(status["status"], "READY")

    def test_benchmark_retry_inspection_preserves_outcome_lock(self):
        root = Path(__file__).resolve().parents[1]
        path = (
            root
            / "strategy_tournament"
            / "inspections"
            / "relative-strength-continuation-v1-activation-7bfe5fddcbaaba26be35eba3cf35dea00d617a6654464998cc499f62f161f992.json"
        )
        inspection = json.loads(path.read_text(encoding="utf-8"))
        self.assertEqual(
            inspection["inspection_sha256"],
            stage0.common._self_hash(inspection, "inspection_sha256"),
        )
        self.assertEqual(inspection["provider_requests_before_this_activation"], 831)
        self.assertEqual(inspection["provider_requests"], 0)
        self.assertEqual(inspection["returns_computed"], 0)
        self.assertTrue(inspection["benchmark_prior_close_collection_authorized"])
        self.assertFalse(inspection["target_outcome_collection_authorized"])

    def test_benchmark_collection_excludes_target_date_bars(self):
        root = Path(__file__).resolve().parents[1]
        path = (
            root
            / "strategy_tournament"
            / "relative_strength_continuation"
            / "benchmark-prior-close-status.json"
        )
        status = json.loads(path.read_text(encoding="utf-8"))
        self.assertEqual(
            status["status_sha256"],
            stage0.common._self_hash(status, "status_sha256"),
        )
        self.assertEqual(status["requested_prior_sessions"], 80)
        self.assertEqual(status["prior_sessions_with_rows"], 80)
        self.assertEqual(status["provider_requests"], 80)
        self.assertEqual(status["target_date_bars_requested"], 0)
        self.assertEqual(status["returns_computed"], 0)
        self.assertEqual(status["broker_actions"], 0)
        self.assertEqual(status["status"], "READY")

    def test_prefix_inspection_freezes_leaders_before_outcomes(self):
        root = Path(__file__).resolve().parents[1]
        path = (
            root
            / "strategy_tournament"
            / "inspections"
            / "relative-strength-continuation-v1-prefix-554a3e4678175f32140642ad98ea9959a5bd87ab65aca5636ba92119a8fe6e9e.json"
        )
        inspection = json.loads(path.read_text(encoding="utf-8"))
        self.assertEqual(
            inspection["inspection_sha256"],
            stage0.common._self_hash(inspection, "inspection_sha256"),
        )
        self.assertEqual(inspection["target_dates"], 80)
        self.assertEqual(inspection["member_symbol_sessions"], 395_316)
        self.assertEqual(inspection["qualifying_leader_symbol_sessions"], 8_729)
        self.assertEqual(inspection["dates_without_leaders"], 0)
        self.assertEqual(inspection["provider_requests_during_inspection"], 0)
        self.assertEqual(inspection["returns_computed"], 0)
        self.assertTrue(inspection["target_outcome_collection_authorized"])

    def test_manifest_freezes_new_full_cross_sectional_dates(self):
        manifest = stage0.build_manifest()
        stage0._validate_manifest(manifest)
        prior_dates = set(stage0._load_json(stage0.CROSS_ACTIVATION)["target_dates"])
        self.assertEqual(manifest["denominator"]["target_dates"], 80)
        self.assertGreater(manifest["denominator"]["member_symbol_sessions"], 300_000)
        self.assertGreater(manifest["denominator"]["unique_symbols"], 5_000)
        self.assertTrue(set(manifest["target_dates"]).isdisjoint(prior_dates))
        self.assertFalse(manifest["provider_requests_authorized_before_inspection"])
        self.assertFalse(
            manifest["target_outcome_requests_authorized_before_input_inspection"]
        )
        self.assertFalse(manifest["development_evidence_eligible"])
        self.assertFalse(manifest["confirmation_evidence_eligible"])
        self.assertEqual(
            manifest["activation_rules_hash"],
            "0aa48c15a997b5182cf7223d0c2f6597e2fd9cbc5ce3bf9b2d97753cdf03e6f8",
        )
        self.assertEqual(manifest["collection_lineage"]["prefix_provider_requests"], 831)
        self.assertFalse(manifest["collection_lineage"]["prefix_ranks_computed"])
        self.assertFalse(manifest["collection_lineage"]["target_outcomes_accessed"])

    def test_normalizer_ignores_half_open_end_boundary(self):
        day = "2025-05-01"
        raw = []
        session_day = date.fromisoformat(day)
        for offset in range(31):
            raw.append(
                {
                    "t": (
                        datetime.combine(session_day, time(9, 30), tzinfo=EASTERN)
                        + timedelta(minutes=offset)
                    ).isoformat(),
                    "o": 100,
                    "h": 101,
                    "l": 99,
                    "c": 100.5,
                    "v": 100,
                    "vw": 100.25,
                }
            )
        rows = stage0._normalize_minutes(
            "TEST", day, raw, start_time=time(9, 30), end_time=time(10)
        )
        self.assertEqual(len(rows), 30)
        self.assertTrue(stage0._complete_session(rows, day, 30))

    def test_trigger_uses_only_completed_history_and_next_open(self):
        rows = _session("2025-05-01")
        rows[30].update(
            {"open": 100.0, "high": 101.0, "low": 99.5, "close": 101.0, "volume": 200, "wap": 100.5}
        )
        rows[31]["open"] = 100.5
        self.assertEqual(stage0._trigger(rows), (30, 99.0))

    def test_stop_first_ambiguity_is_conservative(self):
        rows = _session("2025-05-01")
        rows[31].update({"open": 100.0, "high": 103.0, "low": 98.0, "close": 101.0})
        outcome = stage0._trade_outcome(rows, 30, 99.0, 5)
        self.assertEqual(outcome["exit_reason"], "stop")
        self.assertTrue(outcome["stop_executed"])
        self.assertLessEqual(outcome["net_r"], 0)


if __name__ == "__main__":
    unittest.main()

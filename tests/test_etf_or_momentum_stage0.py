from __future__ import annotations

import unittest
from datetime import date, datetime, time, timedelta
import json
from pathlib import Path
from zoneinfo import ZoneInfo

import etf_or_momentum_stage0 as stage0


EASTERN = ZoneInfo("America/New_York")


def bars(
    *,
    day: str = "2025-01-02",
    bullish: bool = True,
    trigger_index: int | None = 5,
    entry_open: float = 100.11,
    stop_and_target_index: int | None = None,
) -> list[dict]:
    start = datetime.combine(date.fromisoformat(day), time(9, 30), EASTERN)
    result = []
    for index in range(390):
        value = 100.0
        row = {
            "t": (start + timedelta(minutes=index)).isoformat(),
            "o": value,
            "h": 100.10,
            "l": 99.90,
            "c": value,
            "v": 1000,
            "vw": value,
            "i": False,
        }
        result.append(row)
    for row in result[5:]:
        row["l"] = 99.95
    result[4]["c"] = 100.05 if bullish else 99.95
    if trigger_index is not None:
        result[trigger_index]["c"] = 100.11
        result[trigger_index]["h"] = 100.12
        result[trigger_index + 1]["o"] = entry_open
        result[trigger_index + 1]["h"] = max(100.12, entry_open)
        result[trigger_index + 1]["l"] = min(99.95, entry_open)
    if stop_and_target_index is not None:
        result[stop_and_target_index]["h"] = 100.60
        result[stop_and_target_index]["l"] = 99.80
    result[380]["o"] = 100.20
    result[380]["h"] = 100.25
    result[380]["l"] = 100.15
    result[380]["c"] = 100.20
    return result


class EtfOrbStage0Tests(unittest.TestCase):
    def test_published_activation_is_locked_and_claim_bounded(self):
        path = (
            Path(__file__).resolve().parents[1]
            / "strategy_tournament"
            / "activations"
            / "etf-or-momentum-v1-fbea206058e0530a38f89b4b19ccdb71949329960fc6ebbef63ae56966961b03.json"
        )
        manifest = json.loads(path.read_text(encoding="utf-8"))
        stage0._validate_manifest_identity(manifest)
        self.assertEqual(manifest["denominator"]["included_dates"], 164)
        self.assertEqual(manifest["denominator"]["included_symbol_sessions"], 328)
        self.assertEqual(manifest["denominator"]["excluded_dates"], 81)
        self.assertIs(manifest["development_evidence_eligible"], False)
        self.assertIs(manifest["confirmation_evidence_eligible"], False)
        self.assertIs(manifest["provider_requests_authorized"], False)
        self.assertIs(manifest["broker_actions_authorized"], False)
        self.assertIs(
            manifest["return_evaluation_authorized_before_inspection"], False
        )

    def test_published_input_inspection_authorizes_only_stage0_evaluation(self):
        path = (
            Path(__file__).resolve().parents[1]
            / "strategy_tournament"
            / "inspections"
            / "etf-or-momentum-v1-input-6736b37133fd8ba9d46c86e76ef3d7e21a2a09541f1ad631d4ec8b94ff78b1c8.json"
        )
        inspection = json.loads(path.read_text(encoding="utf-8"))
        self.assertEqual(
            inspection["inspection_sha256"],
            stage0._self_hash(inspection, "inspection_sha256"),
        )
        self.assertTrue(inspection["valid"])
        self.assertTrue(inspection["return_evaluation_authorized"])
        self.assertEqual(inspection["returns_computed"], 0)
        self.assertEqual(inspection["provider_requests"], 0)
        self.assertEqual(inspection["broker_actions"], 0)


    def test_candidate_uses_only_completed_trigger_then_next_open(self):
        rows = bars(trigger_index=7, entry_open=100.12)
        candidate = stage0._candidate(day="2025-01-02", symbol="SPY", rows=rows)
        self.assertEqual(candidate["status"], "executable")
        self.assertEqual(candidate["trigger_index"], 7)
        self.assertEqual(candidate["entry_index"], 8)
        self.assertTrue(candidate["trigger_time_et"].endswith("09:37:00-05:00"))
        self.assertTrue(candidate["entry_time_et"].endswith("09:38:00-05:00"))

    def test_red_opening_and_chased_entry_are_preserved_rejections(self):
        red = stage0._candidate(
            day="2025-01-02", symbol="SPY", rows=bars(bullish=False)
        )
        chased = stage0._candidate(
            day="2025-01-02", symbol="SPY", rows=bars(entry_open=100.30)
        )
        self.assertEqual(red["status"], "opening_not_bullish")
        self.assertEqual(chased["status"], "missed_chase_cap")

    def test_same_bar_stop_target_ambiguity_is_stop_first(self):
        rows = bars(trigger_index=5, stop_and_target_index=7)
        candidate = stage0._candidate(day="2025-01-02", symbol="SPY", rows=rows)
        outcome = stage0._trade_outcome(candidate, rows, 5)
        self.assertEqual(outcome["exit_reason"], "stop_first")
        self.assertTrue(outcome["stop_executed"])
        self.assertLess(outcome["net_r"], 0)

    def test_force_flat_uses_1550_open(self):
        rows = bars(trigger_index=5)
        candidate = stage0._candidate(day="2025-01-02", symbol="SPY", rows=rows)
        outcome = stage0._trade_outcome(candidate, rows, 5)
        self.assertEqual(outcome["exit_reason"], "force_flat")
        self.assertTrue(outcome["exit_time_et"].endswith("15:50:00-05:00"))

    def test_cost_stress_is_adverse(self):
        rows = bars(trigger_index=5)
        candidate = stage0._candidate(day="2025-01-02", symbol="SPY", rows=rows)
        primary = stage0._trade_outcome(candidate, rows, 5)
        stressed = stage0._trade_outcome(candidate, rows, 20)
        self.assertLess(stressed["net_r"], primary["net_r"])

    def test_manifest_hash_rejects_tampering(self):
        value = {
            "manifest_sha256": "bad",
            "variant_id": stage0.VARIANT_ID,
            "claim_scope": "FALSIFICATION_ONLY",
            "development_evidence_eligible": False,
            "confirmation_evidence_eligible": False,
            "provider_requests_authorized": False,
            "broker_actions_authorized": False,
        }
        with self.assertRaisesRegex(stage0.EtfOrbStage0Error, "content hash"):
            stage0._validate_manifest_identity(value)

    def test_profit_factor_and_drawdown_use_all_ordered_signals(self):
        metrics = stage0._metrics([1.0, -0.5, 0.25, -0.25])
        self.assertAlmostEqual(metrics["profit_factor"], 1.25 / 0.75)
        self.assertFalse(metrics["profit_factor_infinite"])
        self.assertAlmostEqual(metrics["maximum_drawdown_r"], 0.5)
        self.assertEqual(metrics["signals"], 4)

    def test_all_winners_report_infinite_profit_factor_without_nonfinite_json(self):
        metrics = stage0._metrics([0.2, 0.4])
        self.assertIsNone(metrics["profit_factor"])
        self.assertTrue(metrics["profit_factor_infinite"])


if __name__ == "__main__":
    unittest.main()

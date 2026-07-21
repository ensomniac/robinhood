from __future__ import annotations

import unittest
from datetime import date, datetime, time, timedelta
import json
from pathlib import Path
from zoneinfo import ZoneInfo

import etf_vwap_mean_reversion_stage0 as stage0


EASTERN = ZoneInfo("America/New_York")


def bars(*, signal: bool = True) -> list[dict]:
    start = datetime.combine(date(2025, 1, 2), time(9, 30), EASTERN)
    rows = []
    price = 100.0
    for index in range(390):
        if signal and 25 <= index <= 31:
            price -= 0.18
        elif signal and index == 32:
            price += 0.28
        row = {
            "t": (start + timedelta(minutes=index)).isoformat(),
            "o": price - 0.02,
            "h": price + 0.04,
            "l": price - 0.04,
            "c": price,
            "v": 1000,
            "vw": 100.0,
            "i": False,
        }
        rows.append(row)
    if signal:
        rows[31]["h"] = rows[31]["c"] + 0.02
        rows[32]["o"] = rows[32]["c"] - 0.10
        rows[32]["h"] = rows[32]["c"] + 0.04
        rows[33]["o"] = rows[32]["c"] + 0.02
        rows[33]["h"] = rows[33]["o"] + 0.04
        rows[33]["l"] = rows[33]["o"] - 0.04
        rows[33]["c"] = rows[33]["o"]
    rows[380]["o"] = 99.5
    rows[380]["h"] = 99.55
    rows[380]["l"] = 99.45
    rows[380]["c"] = 99.5
    return rows


class EtfVwapStage0Tests(unittest.TestCase):
    def test_published_activation_is_falsification_only_and_return_locked(self):
        path = (
            Path(__file__).resolve().parents[1]
            / "strategy_tournament"
            / "activations"
            / "etf-vwap-mean-reversion-v1-ecaff3785f2dc9fe3f14332be9e40e056e08aacf55f86487cb2f25df62688a47.json"
        )
        manifest = json.loads(path.read_text(encoding="utf-8"))
        stage0._validate_manifest(manifest)
        self.assertEqual(manifest["denominator"]["included_dates"], 164)
        self.assertEqual(manifest["denominator"]["included_symbol_sessions"], 328)
        self.assertFalse(manifest["development_evidence_eligible"])
        self.assertFalse(manifest["confirmation_evidence_eligible"])
        self.assertFalse(manifest["return_evaluation_authorized_before_inspection"])

    def test_published_input_inspection_computed_no_returns(self):
        path = (
            Path(__file__).resolve().parents[1]
            / "strategy_tournament"
            / "inspections"
            / "etf-vwap-mean-reversion-v1-input-deae004f10d53c1753435155385fc0557b6c39249f43896725bf1237dd32eaa0.json"
        )
        inspection = json.loads(path.read_text(encoding="utf-8"))
        self.assertEqual(
            inspection["inspection_sha256"],
            stage0.common._self_hash(inspection, "inspection_sha256"),
        )
        self.assertTrue(inspection["valid"])
        self.assertTrue(inspection["return_evaluation_authorized"])
        self.assertEqual(inspection["returns_computed"], 0)

    def test_published_result_is_an_inspected_retirement(self):
        root = Path(__file__).resolve().parents[1]
        result = json.loads(
            (
                root
                / "research_results"
                / "2026-07-21-etf-vwap-mean-reversion-stage0-6b3cb4374d858c293176a622e3dc23300d1e09e9a7207a2f487bae700281a5bd.json"
            ).read_text(encoding="utf-8")
        )
        inspection = json.loads(
            (
                root
                / "strategy_tournament"
                / "inspections"
                / "etf-vwap-mean-reversion-v1-result-faef3c6c57e36f8604179e596c959bd3f7c982848552ad224ef371da7c963176.json"
            ).read_text(encoding="utf-8")
        )
        self.assertEqual(
            result["result_sha256"],
            stage0.common._self_hash(result, "result_sha256"),
        )
        self.assertFalse(result["stage0_survived"])
        self.assertEqual(result["maturity_effect"], "NONE")
        self.assertEqual(result["denominator"]["closed_signals"], 29)
        self.assertEqual(result["denominator"]["rule_violations"], 0)
        self.assertLess(result["primary_5bps"]["total_r"], 0)
        self.assertTrue(inspection["valid"])
        self.assertFalse(inspection["stage0_survived"])



    def test_simple_rsi_uses_exactly_five_completed_changes(self):
        rows = bars()
        self.assertIsNone(stage0._rsi_five(rows, 4))
        self.assertLessEqual(stage0._rsi_five(rows, 30), 25)

    def test_washout_must_precede_bullish_reversal_and_next_bar_entry(self):
        rows = bars()
        candidate = stage0._candidate(day="2025-01-02", symbol="SPY", rows=rows)
        self.assertEqual(candidate["status"], "executable")
        self.assertTrue(candidate["armed_time_et"].endswith("10:01:00-05:00"))
        self.assertTrue(candidate["trigger_time_et"].endswith("10:02:00-05:00"))
        self.assertTrue(candidate["entry_time_et"].endswith("10:03:00-05:00"))

    def test_absent_washout_is_preserved_no_signal(self):
        candidate = stage0._candidate(
            day="2025-01-02", symbol="QQQ", rows=bars(signal=False)
        )
        self.assertEqual(candidate["status"], "no_washout")

    def test_stop_target_ambiguity_resolves_stop_first(self):
        rows = bars()
        candidate = stage0._candidate(day="2025-01-02", symbol="SPY", rows=rows)
        index = int(candidate["entry_index"])
        rows[index]["l"] = float(candidate["stop"]) - 0.01
        rows[index]["h"] = float(candidate["target"]) + 0.01
        outcome = stage0._trade_outcome(candidate, rows, 5)
        self.assertEqual(outcome["exit_reason"], "stop_first")
        self.assertLess(outcome["net_r"], 0)

    def test_cost_stress_reduces_net_r(self):
        rows = bars()
        candidate = stage0._candidate(day="2025-01-02", symbol="SPY", rows=rows)
        primary = stage0._trade_outcome(candidate, rows, 5)
        stress = stage0._trade_outcome(candidate, rows, 20)
        self.assertLess(stress["net_r"], primary["net_r"])

    def test_manifest_identity_rejects_tampering(self):
        with self.assertRaisesRegex(stage0.EtfVwapStage0Error, "content hash"):
            stage0._validate_manifest({"manifest_sha256": "bad"})


if __name__ == "__main__":
    unittest.main()

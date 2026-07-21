from __future__ import annotations

import json
import unittest
from datetime import datetime, timedelta
from pathlib import Path

import equity_gap_continuation_stage0 as stage0


def bars(*, volume_multiple: float = 2.0) -> list[dict]:
    start = datetime.fromisoformat("2025-01-02T09:30:00-05:00")
    result = []
    for index in range(390):
        row = {
            "time_et": (start + timedelta(minutes=index)).time().isoformat(),
            "open": 103.0,
            "high": 103.1,
            "low": 102.9,
            "close": 103.0,
            "volume": 1000,
            "interpolated": False,
        }
        result.append(row)
    result[0]["low"] = 102.5
    result[14]["high"] = 103.2
    result[15].update(
        {
            "open": 103.1,
            "high": 103.4,
            "low": 103.0,
            "close": 103.3,
            "volume": int(1000 * volume_multiple),
        }
    )
    result[16].update(
        {
            "open": 103.35,
            "high": 103.4,
            "low": 103.2,
            "close": 103.3,
        }
    )
    result[380].update(
        {
            "open": 103.6,
            "high": 103.7,
            "low": 103.5,
            "close": 103.6,
        }
    )
    return result


def raw_candidate(*, rows: list[dict] | None = None, common_stock: bool = True):
    return {
        "symbol": "TEST",
        "bars": rows or bars(),
        "evaluation_payload": {"candidate": {"is_common_stock": common_stock}},
    }


class EquityGapContinuationStage0Tests(unittest.TestCase):
    def test_published_activation_is_exact_and_return_locked(self):
        path = (
            Path(__file__).resolve().parents[1]
            / "strategy_tournament"
            / "activations"
            / "equity-gap-continuation-v1-3bc6f70c2a331e70b00a1dda076b258ce46f1175d062ecee0a641e4c4ab06f3f.json"
        )
        manifest = json.loads(path.read_text(encoding="utf-8"))
        stage0._validate_manifest(manifest)
        self.assertEqual(manifest["denominator"]["requested_dates"], 100)
        self.assertEqual(manifest["denominator"]["included_dates"], 95)
        self.assertEqual(manifest["denominator"]["excluded_dates"], 5)
        self.assertEqual(manifest["denominator"]["included_symbol_sessions"], 950)
        self.assertEqual(
            manifest["denominator"]["prior_close_available_symbol_sessions"],
            950,
        )
        self.assertFalse(manifest["return_evaluation_authorized_before_inspection"])
        self.assertFalse(manifest["development_evidence_eligible"])
        self.assertFalse(manifest["confirmation_evidence_eligible"])

    def test_published_input_inspection_computed_zero_returns(self):
        path = (
            Path(__file__).resolve().parents[1]
            / "strategy_tournament"
            / "inspections"
            / "equity-gap-continuation-v1-input-fe842aebc174196ceaa34a529d6c3a8eb75ab4df5bc6db110221f7f3dea4c881.json"
        )
        inspection = json.loads(path.read_text(encoding="utf-8"))
        self.assertEqual(
            inspection["inspection_sha256"],
            stage0.common._self_hash(inspection, "inspection_sha256"),
        )
        self.assertTrue(inspection["return_evaluation_authorized"])
        self.assertEqual(inspection["returns_computed"], 0)
        self.assertEqual(inspection["selected_dates"], 95)
        self.assertEqual(inspection["selected_symbol_sessions"], 950)
        self.assertEqual(inspection["selected_bars"], 370500)

    def test_published_result_is_an_inspected_stage0_survivor(self):
        root = Path(__file__).resolve().parents[1]
        result = json.loads(
            (
                root
                / "research_results"
                / "2026-07-21-equity-gap-continuation-stage0-1f447fb4e066b041463e62e26d7e752a12e1b90da7c13b6881ea42e10dda9e94.json"
            ).read_text(encoding="utf-8")
        )
        inspection = json.loads(
            (
                root
                / "strategy_tournament"
                / "inspections"
                / "equity-gap-continuation-v1-result-66ede02688e090973e482ecc9c491349238d14e2af63fd02c336b99a8564c7be.json"
            ).read_text(encoding="utf-8")
        )
        self.assertEqual(
            result["result_sha256"],
            stage0.common._self_hash(result, "result_sha256"),
        )
        self.assertTrue(result["stage0_survived"])
        self.assertEqual(result["stage0_blockers"], [])
        self.assertEqual(result["maturity_effect"], "NONE")
        self.assertEqual(result["denominator"]["closed_signals"], 37)
        self.assertGreater(result["stress"]["20"]["total_r"], 0)
        self.assertTrue(inspection["valid"])
        self.assertTrue(inspection["stage0_survived"])

    def test_completed_breakout_uses_next_bar_open(self):
        candidate = stage0._candidate(
            day="2025-01-02", raw=raw_candidate(), prior_close=100.0
        )
        self.assertEqual(candidate["status"], "executable")
        self.assertEqual(candidate["trigger_index"], 15)
        self.assertEqual(candidate["entry_index"], 16)
        self.assertEqual(candidate["entry_open"], 103.35)

    def test_common_stock_price_and_gap_gates_are_fail_closed(self):
        not_common = stage0._candidate(
            day="2025-01-02",
            raw=raw_candidate(common_stock=False),
            prior_close=100.0,
        )
        low_price_rows = bars()
        for row in low_price_rows:
            for field in ("open", "high", "low", "close"):
                row[field] /= 25
        low_price = stage0._candidate(
            day="2025-01-02",
            raw=raw_candidate(rows=low_price_rows),
            prior_close=4.0,
        )
        no_gap = stage0._candidate(
            day="2025-01-02", raw=raw_candidate(), prior_close=102.5
        )
        self.assertEqual(not_common["status"], "not_common_stock")
        self.assertEqual(low_price["status"], "opening_price_not_above_5")
        self.assertEqual(no_gap["status"], "gap_outside_range")

    def test_missing_prior_close_remains_a_no_signal_denominator(self):
        candidate = stage0._candidate(
            day="2025-01-02", raw=raw_candidate(), prior_close=None
        )
        self.assertEqual(candidate["status"], "prior_close_missing")

    def test_breakout_volume_gate_uses_prior_fifteen_bars(self):
        candidate = stage0._candidate(
            day="2025-01-02",
            raw=raw_candidate(rows=bars(volume_multiple=1.49)),
            prior_close=100.0,
        )
        self.assertEqual(candidate["status"], "no_breakout_trigger")

    def test_same_bar_stop_target_ambiguity_is_stop_first(self):
        rows = bars()
        candidate = stage0._candidate(
            day="2025-01-02", raw=raw_candidate(rows=rows), prior_close=100.0
        )
        index = int(candidate["entry_index"])
        target = float(candidate["entry_open"]) + 2 * (
            float(candidate["entry_open"]) - float(candidate["stop"])
        )
        rows[index]["low"] = float(candidate["stop"]) - 0.01
        rows[index]["high"] = target + 0.01
        outcome = stage0._trade_outcome(candidate, rows, 5)
        self.assertEqual(outcome["exit_reason"], "stop_first")
        self.assertLess(outcome["net_r"], 0)

    def test_force_flat_and_cost_stress_are_adverse(self):
        rows = bars()
        candidate = stage0._candidate(
            day="2025-01-02", raw=raw_candidate(rows=rows), prior_close=100.0
        )
        primary = stage0._trade_outcome(candidate, rows, 5)
        stressed = stage0._trade_outcome(candidate, rows, 20)
        self.assertEqual(primary["exit_reason"], "force_flat")
        self.assertEqual(primary["exit_time_et"], "15:50:00")
        self.assertLess(stressed["net_r"], primary["net_r"])

    def test_manifest_identity_rejects_tampering(self):
        with self.assertRaisesRegex(stage0.EquityGapStage0Error, "content hash"):
            stage0._validate_manifest({"manifest_sha256": "bad"})


if __name__ == "__main__":
    unittest.main()

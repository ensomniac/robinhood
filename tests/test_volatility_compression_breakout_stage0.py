from __future__ import annotations

import json
import unittest
from datetime import datetime, timedelta
from pathlib import Path

import volatility_compression_breakout_stage0 as stage0


def bars(*, volume_multiple: float = 2.0) -> list[dict]:
    start = datetime.fromisoformat("2025-01-02T09:30:00-05:00")
    result = []
    for index in range(390):
        row = {
            "time_et": (start + timedelta(minutes=index)).time().isoformat(),
            "open": 100.0,
            "high": 100.2,
            "low": 99.8 if index < 30 else 99.9,
            "close": 100.0,
            "volume": 1000,
            "interpolated": False,
        }
        result.append(row)
    result[0].update({"high": 101.0, "low": 99.0})
    result[45].update(
        {
            "open": 100.1,
            "high": 100.7,
            "low": 100.0,
            "close": 100.6,
            "volume": int(1000 * volume_multiple),
        }
    )
    result[46].update(
        {
            "open": 100.65,
            "high": 100.7,
            "low": 100.5,
            "close": 100.6,
        }
    )
    result[380].update({"open": 100.8, "high": 100.9, "low": 100.7, "close": 100.8})
    return result


def raw_candidate(*, rows: list[dict] | None = None, common_stock: bool = True):
    return {
        "symbol": "TEST",
        "bars": rows or bars(),
        "evaluation_payload": {"candidate": {"is_common_stock": common_stock}},
    }


class VolatilityCompressionBreakoutStage0Tests(unittest.TestCase):
    def test_published_activation_is_exact_and_return_locked(self):
        root = Path(__file__).resolve().parents[1]
        path = (
            root
            / "strategy_tournament"
            / "activations"
            / "volatility-compression-breakout-v1-3376ff9d2b6af10247e9eee3704f4e3bf612283a42c37a2c0f1b8f680a76d4c0.json"
        )
        manifest = json.loads(path.read_text(encoding="utf-8"))
        stage0._validate_manifest(manifest)
        self.assertEqual(manifest["denominator"]["requested_dates"], 100)
        self.assertEqual(manifest["denominator"]["included_dates"], 95)
        self.assertEqual(manifest["denominator"]["excluded_dates"], 5)
        self.assertEqual(manifest["denominator"]["included_symbol_sessions"], 950)
        self.assertFalse(manifest["return_evaluation_authorized_before_inspection"])

    def test_published_input_inspection_computed_zero_returns(self):
        root = Path(__file__).resolve().parents[1]
        path = (
            root
            / "strategy_tournament"
            / "inspections"
            / "volatility-compression-breakout-v1-input-600ae694412202299505870eb48a14a9819a00f6c88867c3370a27e89c7d8fce.json"
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

    def test_published_result_is_an_inspected_stage0_retirement(self):
        root = Path(__file__).resolve().parents[1]
        result = json.loads(
            (
                root
                / "research_results"
                / "2026-07-21-volatility-compression-breakout-stage0-5959e922a6cdaf117521a2a70877984497077f3c79acd5aedf2ac87eea0bbdfe.json"
            ).read_text(encoding="utf-8")
        )
        inspection = json.loads(
            (
                root
                / "strategy_tournament"
                / "inspections"
                / "volatility-compression-breakout-v1-result-67792983095a62d5811046d8d15a095faa75da7566d7957492d6c52213824f53.json"
            ).read_text(encoding="utf-8")
        )
        self.assertEqual(
            result["result_sha256"],
            stage0.common._self_hash(result, "result_sha256"),
        )
        self.assertFalse(result["stage0_survived"])
        self.assertEqual(result["maturity_effect"], "NONE")
        self.assertEqual(result["denominator"]["closed_signals"], 95)
        self.assertLess(result["primary_5bps"]["expectancy_r"], 0)
        self.assertLess(result["stress"]["20"]["total_r"], 0)
        self.assertTrue(inspection["valid"])
        self.assertFalse(inspection["stage0_survived"])

    def test_completed_breakout_uses_prior_twenty_bars_and_next_open(self):
        candidate = stage0._candidate(day="2025-01-02", raw=raw_candidate())
        self.assertEqual(candidate["status"], "executable")
        self.assertEqual(candidate["trigger_index"], 45)
        self.assertEqual(candidate["entry_index"], 46)
        self.assertEqual(candidate["entry_open"], 100.65)
        self.assertEqual(candidate["stop"], 99.8)
        self.assertAlmostEqual(candidate["target"], 102.35)

    def test_signal_bar_is_not_part_of_compression_window(self):
        rows = bars()
        rows[45]["high"] = 102.0
        rows[45]["close"] = 101.5
        candidate = stage0._candidate(day="2025-01-02", raw=raw_candidate(rows=rows))
        self.assertEqual(candidate["status"], "executable")
        self.assertEqual(candidate["trigger_index"], 45)

    def test_compression_ratio_gate_uses_first_thirty_minute_range(self):
        rows = bars()
        rows[30].update({"high": 100.8, "low": 99.2})
        candidate = stage0._candidate(day="2025-01-02", raw=raw_candidate(rows=rows))
        self.assertEqual(candidate["status"], "no_compression_breakout")

    def test_breakout_volume_gate_uses_prior_twenty_bars(self):
        candidate = stage0._candidate(
            day="2025-01-02", raw=raw_candidate(rows=bars(volume_multiple=1.49))
        )
        self.assertEqual(candidate["status"], "no_compression_breakout")

    def test_price_and_asset_gates_fail_closed(self):
        not_common = stage0._candidate(
            day="2025-01-02", raw=raw_candidate(common_stock=False)
        )
        low_price_rows = bars()
        for row in low_price_rows:
            for field in ("open", "high", "low", "close"):
                row[field] /= 25
        low_price = stage0._candidate(
            day="2025-01-02", raw=raw_candidate(rows=low_price_rows)
        )
        self.assertEqual(not_common["status"], "not_common_stock")
        self.assertEqual(low_price["status"], "opening_price_not_above_5")

    def test_same_bar_stop_target_ambiguity_is_stop_first(self):
        rows = bars()
        candidate = stage0._candidate(day="2025-01-02", raw=raw_candidate(rows=rows))
        index = int(candidate["entry_index"])
        rows[index]["low"] = float(candidate["stop"]) - 0.01
        rows[index]["high"] = float(candidate["target"]) + 0.01
        outcome = stage0._trade_outcome(candidate, rows, 5)
        self.assertEqual(outcome["exit_reason"], "stop_first")
        self.assertLess(outcome["net_r"], 0)

    def test_force_flat_and_cost_stress_are_adverse(self):
        rows = bars()
        candidate = stage0._candidate(day="2025-01-02", raw=raw_candidate(rows=rows))
        primary = stage0._trade_outcome(candidate, rows, 5)
        stressed = stage0._trade_outcome(candidate, rows, 20)
        self.assertEqual(primary["exit_reason"], "force_flat")
        self.assertEqual(primary["exit_time_et"], "15:50:00")
        self.assertLess(stressed["net_r"], primary["net_r"])


if __name__ == "__main__":
    unittest.main()

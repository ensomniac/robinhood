from __future__ import annotations

import json
import unittest
from datetime import datetime, timedelta
from pathlib import Path

import short_horizon_oversold_reversal_stage0 as stage0


def bars() -> list[dict]:
    start = datetime.fromisoformat("2025-01-02T09:30:00-05:00")
    result = []
    for index in range(390):
        close = 100.0 - 0.13 * min(index, 29)
        result.append(
            {
                "time_et": (start + timedelta(minutes=index)).time().isoformat(),
                "open": close + 0.03,
                "high": close + 0.08,
                "low": close - 0.08,
                "close": close,
                "volume": 1000,
                "interpolated": False,
            }
        )
    result[30].update(
        {
            "open": 96.20,
            "high": 100.40,
            "low": 96.10,
            "close": 100.30,
        }
    )
    result[31].update(
        {
            "open": 100.35,
            "high": 100.4,
            "low": 100.2,
            "close": 100.3,
        }
    )
    for row in result[32:380]:
        row.update({"open": 100.4, "high": 100.5, "low": 100.2, "close": 100.4})
    result[380].update({"open": 100.8, "high": 100.9, "low": 100.7, "close": 100.8})
    return result


def raw_candidate(*, rows: list[dict] | None = None, common_stock: bool = True):
    return {
        "symbol": "TEST",
        "bars": rows or bars(),
        "evaluation_payload": {"candidate": {"is_common_stock": common_stock}},
    }


class ShortHorizonOversoldReversalStage0Tests(unittest.TestCase):
    def test_published_activation_is_exact_and_return_locked(self):
        root = Path(__file__).resolve().parents[1]
        path = (
            root
            / "strategy_tournament"
            / "activations"
            / "short-horizon-oversold-reversal-v1-b65565475be90a9ad981e4e5dd1c598edbd21d5ee21ba9cad3333ca7da8d9d51.json"
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
            / "short-horizon-oversold-reversal-v1-input-0e8796722e06b0643aeb9f6e3001e9e9320730d617619b50884293c006dd46dd.json"
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

    def test_published_result_is_retired_only_for_insufficient_signals(self):
        root = Path(__file__).resolve().parents[1]
        result = json.loads(
            (
                root
                / "research_results"
                / "2026-07-21-short-horizon-oversold-reversal-stage0-bb2152f3f18aef92bac5c7d5dcffc15bdbe2967525e63fa6678c6c553aeb75c6.json"
            ).read_text(encoding="utf-8")
        )
        inspection = json.loads(
            (
                root
                / "strategy_tournament"
                / "inspections"
                / "short-horizon-oversold-reversal-v1-result-be56bcf7bf8fe2fd16e223f510a8236135fbc39f1a27e7531af9a4649a706e25.json"
            ).read_text(encoding="utf-8")
        )
        self.assertEqual(
            result["result_sha256"],
            stage0.common._self_hash(result, "result_sha256"),
        )
        self.assertFalse(result["stage0_survived"])
        self.assertEqual(
            result["stage0_blockers"],
            ["closed signals are below the Stage 0 minimum"],
        )
        self.assertEqual(result["denominator"]["closed_signals"], 10)
        self.assertGreater(result["primary_5bps"]["expectancy_r"], 0)
        self.assertGreater(result["stress"]["20"]["total_r"], 0)
        self.assertTrue(inspection["valid"])
        self.assertFalse(inspection["stage0_survived"])

    def test_simple_rsi_uses_five_completed_changes(self):
        rows = bars()
        self.assertEqual(stage0._simple_rsi(rows, 29), 0.0)
        self.assertGreater(stage0._simple_rsi(rows, 30), 20.0)

    def test_completed_reversal_uses_pretrigger_lookback_and_next_open(self):
        candidate = stage0._candidate(day="2025-01-02", raw=raw_candidate())
        self.assertEqual(candidate["status"], "executable")
        self.assertEqual(candidate["trigger_index"], 30)
        self.assertEqual(candidate["entry_index"], 31)
        self.assertEqual(candidate["entry_open"], 100.35)
        self.assertEqual(candidate["rsi"], 0.0)

    def test_selloff_and_rsi_do_not_use_trigger_close(self):
        rows = bars()
        candidate = stage0._candidate(day="2025-01-02", raw=raw_candidate(rows=rows))
        self.assertLessEqual(candidate["selloff_return"], -0.03)
        self.assertEqual(candidate["rsi"], stage0._simple_rsi(rows, 29))

    def test_selloff_gate_rejects_a_shallow_decline(self):
        rows = bars()
        for index, row in enumerate(rows[:30]):
            close = 100.0 - 0.05 * index
            row.update(
                {
                    "open": close + 0.03,
                    "high": close + 0.08,
                    "low": close - 0.08,
                    "close": close,
                }
            )
        candidate = stage0._candidate(day="2025-01-02", raw=raw_candidate(rows=rows))
        self.assertEqual(candidate["status"], "no_oversold_reversal")

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

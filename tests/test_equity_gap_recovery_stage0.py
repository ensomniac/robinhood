from __future__ import annotations

import unittest
from datetime import datetime, timedelta

import equity_gap_recovery_stage0 as stage0


def bars() -> list[dict]:
    start = datetime.fromisoformat("2025-01-02T09:30:00-05:00")
    result = []
    for index in range(390):
        result.append(
            {
                "time_et": (start + timedelta(minutes=index)).time().isoformat(),
                "open": 95.0,
                "high": 95.1,
                "low": 94.9,
                "close": 95.0,
                "volume": 1000,
                "interpolated": False,
            }
        )
    result[0]["low"] = 94.0
    result[15].update(
        {
            "open": 95.0,
            "high": 95.7,
            "low": 94.9,
            "close": 95.6,
        }
    )
    result[16].update(
        {
            "open": 95.55,
            "high": 95.7,
            "low": 95.4,
            "close": 95.6,
        }
    )
    result[380].update(
        {
            "open": 96.0,
            "high": 96.1,
            "low": 95.9,
            "close": 96.0,
        }
    )
    return result


def raw_candidate(*, rows: list[dict] | None = None, common_stock: bool = True):
    return {
        "symbol": "TEST",
        "bars": rows or bars(),
        "evaluation_payload": {"candidate": {"is_common_stock": common_stock}},
    }


class EquityGapRecoveryStage0Tests(unittest.TestCase):
    def test_gap_boundaries_are_inclusive_and_outside_values_fail(self):
        self.assertTrue(stage0._gap_in_range(-0.08))
        self.assertTrue(stage0._gap_in_range(-0.02))
        self.assertFalse(stage0._gap_in_range(-0.080001))
        self.assertFalse(stage0._gap_in_range(-0.019999))

    def test_completed_recovery_trigger_uses_next_bar_open(self):
        candidate = stage0._candidate(
            day="2025-01-02", raw=raw_candidate(), prior_close=100.0
        )
        self.assertEqual(candidate["status"], "executable")
        self.assertEqual(candidate["trigger_index"], 15)
        self.assertEqual(candidate["entry_index"], 16)
        self.assertEqual(candidate["entry_open"], 95.55)

    def test_new_low_inside_stabilization_window_delays_trigger(self):
        rows = bars()
        rows[10]["low"] = 93.9
        rows[15].update({"open": 95.0, "high": 95.1, "low": 94.9, "close": 95.0})
        rows[20].update({"high": 95.8, "close": 95.7})
        rows[21]["open"] = 95.65
        candidate = stage0._candidate(
            day="2025-01-02", raw=raw_candidate(rows=rows), prior_close=100.0
        )
        self.assertEqual(candidate["status"], "executable")
        self.assertEqual(candidate["trigger_index"], 20)

    def test_reclaim_must_be_strictly_above_open_and_vwap(self):
        rows = bars()
        for row in rows[15:151]:
            row.update({"open": 95.0, "high": 95.1, "low": 94.9, "close": 95.0})
        candidate = stage0._candidate(
            day="2025-01-02", raw=raw_candidate(rows=rows), prior_close=100.0
        )
        self.assertEqual(candidate["status"], "no_recovery_trigger")

    def test_common_stock_price_prior_close_and_gap_gates_fail_closed(self):
        not_common = stage0._candidate(
            day="2025-01-02",
            raw=raw_candidate(common_stock=False),
            prior_close=100.0,
        )
        low_price_rows = bars()
        for row in low_price_rows:
            for field in ("open", "high", "low", "close"):
                row[field] /= 20
        low_price = stage0._candidate(
            day="2025-01-02",
            raw=raw_candidate(rows=low_price_rows),
            prior_close=5.0,
        )
        missing_prior = stage0._candidate(
            day="2025-01-02", raw=raw_candidate(), prior_close=None
        )
        no_gap = stage0._candidate(
            day="2025-01-02", raw=raw_candidate(), prior_close=96.0
        )
        self.assertEqual(not_common["status"], "not_common_stock")
        self.assertEqual(low_price["status"], "opening_price_not_above_5")
        self.assertEqual(missing_prior["status"], "prior_close_missing")
        self.assertEqual(no_gap["status"], "gap_outside_range")

    def test_target_is_nearer_of_prior_close_and_raw_two_r(self):
        candidate = stage0._candidate(
            day="2025-01-02", raw=raw_candidate(), prior_close=100.0
        )
        self.assertAlmostEqual(candidate["target"], 98.65)
        wide_stop_rows = bars()
        wide_stop_rows[0]["low"] = 90.0
        prior_close_target = stage0._candidate(
            day="2025-01-02",
            raw=raw_candidate(rows=wide_stop_rows),
            prior_close=100.0,
        )
        self.assertEqual(prior_close_target["target"], 100.0)

    def test_same_bar_stop_target_ambiguity_is_stop_first(self):
        rows = bars()
        candidate = stage0._candidate(
            day="2025-01-02", raw=raw_candidate(rows=rows), prior_close=100.0
        )
        index = int(candidate["entry_index"])
        rows[index]["low"] = float(candidate["stop"]) - 0.01
        rows[index]["high"] = float(candidate["target"]) + 0.01
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


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import unittest
from datetime import date
import json
from pathlib import Path

import cross_sectional_momentum_stage0 as stage0


class CrossSectionalMomentumStage0Tests(unittest.TestCase):
    def test_published_activation_freezes_point_in_time_denominator(self):
        root = Path(__file__).resolve().parents[1]
        path = (
            root
            / "strategy_tournament"
            / "activations"
            / "cross-sectional-momentum-v1-a8970ad2bfb0045f54404da985178686ec7cd0068c3a6a23f59165c7e415520a.json"
        )
        manifest = json.loads(path.read_text(encoding="utf-8"))
        stage0._validate_manifest(manifest)
        self.assertEqual(manifest["denominator"]["target_dates"], 24)
        self.assertEqual(manifest["denominator"]["member_symbol_sessions"], 118636)
        self.assertEqual(manifest["denominator"]["unique_symbols"], 5457)
        self.assertEqual(manifest["denominator"]["maximum_selected_signals"], 72)
        self.assertFalse(manifest["provider_requests_authorized_before_inspection"])
        self.assertFalse(
            manifest["return_evaluation_authorized_before_input_inspection"]
        )

    def test_published_activation_inspection_computed_zero_returns(self):
        root = Path(__file__).resolve().parents[1]
        path = (
            root
            / "strategy_tournament"
            / "inspections"
            / "cross-sectional-momentum-v1-activation-a0f893cb02e31fb445febfacd4e5535a6e5ec602f18d630c06827efbdc84c31f.json"
        )
        inspection = json.loads(path.read_text(encoding="utf-8"))
        self.assertEqual(
            inspection["inspection_sha256"],
            stage0.common._self_hash(inspection, "inspection_sha256"),
        )
        self.assertEqual(inspection["target_dates"], 24)
        self.assertEqual(inspection["member_symbol_sessions"], 118636)
        self.assertEqual(inspection["unique_symbols"], 5457)
        self.assertEqual(inspection["returns_computed"], 0)
        self.assertTrue(inspection["collection_authorized"])
        self.assertFalse(inspection["return_evaluation_authorized"])

    def test_published_collection_status_is_complete_and_return_blind(self):
        root = Path(__file__).resolve().parents[1]
        path = (
            root
            / "strategy_tournament"
            / "cross_sectional_momentum"
            / "collection-status.json"
        )
        status = json.loads(path.read_text(encoding="utf-8"))
        self.assertEqual(
            status["status_sha256"],
            stage0.common._self_hash(status, "status_sha256"),
        )
        self.assertEqual(status["status"], "READY")
        self.assertEqual(status["requested_symbols"], 5457)
        self.assertEqual(status["symbols_with_rows"], 5453)
        self.assertEqual(status["daily_rows"], 1279526)
        self.assertEqual(status["provider_requests"], 131)
        self.assertEqual(status["returns_computed"], 0)
        self.assertEqual(status["broker_actions"], 0)

    def test_published_input_inspection_keeps_complete_denominator(self):
        root = Path(__file__).resolve().parents[1]
        path = (
            root
            / "strategy_tournament"
            / "inspections"
            / "cross-sectional-momentum-v1-input-198957534e2d6ec63519d9e6c96b598ff96f8490f2eece45af282ffc5b9358de.json"
        )
        inspection = json.loads(path.read_text(encoding="utf-8"))
        self.assertEqual(
            inspection["inspection_sha256"],
            stage0.common._self_hash(inspection, "inspection_sha256"),
        )
        self.assertEqual(inspection["member_symbol_sessions"], 118636)
        self.assertEqual(inspection["complete_symbol_sessions"], 77639)
        self.assertEqual(inspection["missing_daily_window_symbol_sessions"], 3030)
        self.assertEqual(inspection["missing_ranking_prefix_symbol_sessions"], 37967)
        self.assertEqual(inspection["returns_computed"], 0)
        self.assertEqual(inspection["provider_requests_during_inspection"], 0)
        self.assertTrue(inspection["return_evaluation_authorized"])

    def test_target_dates_are_fixed_spaced_and_have_complete_windows(self):
        source_dates, _, _ = stage0.universe_source._source_graph()
        sessions = stage0._calendar()
        targets = stage0._target_dates(source_dates, sessions)
        positions = {day: index for index, day in enumerate(sessions)}
        self.assertEqual(len(targets), 24)
        self.assertEqual(targets[0], "2025-03-03")
        self.assertEqual(targets[-1], "2025-12-15")
        self.assertTrue(
            all(
                positions[right] - positions[left] >= 6
                for left, right in zip(targets, targets[1:])
            )
        )
        self.assertGreaterEqual(positions[targets[0]], stage0.LOOKBACK_SESSIONS)
        self.assertLess(positions[targets[-1]] + 5, len(sessions))

    def test_provider_daily_rows_are_validated_and_sorted(self):
        rows = [
            {
                "t": "2025-01-03T05:00:00Z",
                "o": 11,
                "h": 12,
                "l": 10,
                "c": 11.5,
                "v": 200,
            },
            {
                "t": "2025-01-02T05:00:00Z",
                "o": 10,
                "h": 11,
                "l": 9,
                "c": 10.5,
                "v": 100,
            },
        ]
        normalized = stage0._normalize_daily_rows("TEST", rows)
        self.assertEqual(
            [row["date"] for row in normalized], ["2025-01-02", "2025-01-03"]
        )
        with self.assertRaisesRegex(stage0.CrossSectionalMomentumError, "duplicate"):
            stage0._normalize_daily_rows("TEST", [rows[0], rows[0]])

    def test_target_basis_split_adjustment_changes_price_and_volume(self):
        splits = {
            "TEST": [
                {
                    "execution_date": date.fromisoformat("2025-01-03"),
                    "split_from": 1.0,
                    "split_to": 2.0,
                }
            ]
        }
        adjusted = stage0._adjusted_daily(
            "TEST",
            "2025-01-02",
            "2025-01-03",
            {"open": 100, "high": 110, "low": 90, "close": 100, "volume": 1000},
            splits,
        )
        self.assertEqual(adjusted["close"], 50.0)
        self.assertEqual(adjusted["volume"], 2000.0)

    def test_trade_holds_to_fifth_close_when_stop_is_not_hit(self):
        dates = [f"2025-01-0{day}" for day in range(2, 7)]
        daily = {
            day: {"open": 100.0, "high": 102.0, "low": 99.0, "close": 101.0}
            for day in dates
        }
        candidate = {"atr14": 2.0}
        primary = stage0._trade_outcome(candidate, dates, daily, 5)
        stressed = stage0._trade_outcome(candidate, dates, daily, 20)
        self.assertEqual(primary["exit_reason"], "maximum_hold_close")
        self.assertEqual(primary["exit_date"], dates[-1])
        self.assertLess(stressed["net_r"], primary["net_r"])

    def test_stop_gap_uses_worse_open(self):
        dates = [f"2025-01-0{day}" for day in range(2, 7)]
        daily = {
            day: {"open": 100.0, "high": 102.0, "low": 99.0, "close": 101.0}
            for day in dates
        }
        daily[dates[1]].update({"open": 95.0, "high": 96.0, "low": 94.0, "close": 95.0})
        outcome = stage0._trade_outcome({"atr14": 2.0}, dates, daily, 5)
        self.assertEqual(outcome["exit_reason"], "stop_gap")
        self.assertEqual(outcome["exit_date"], dates[1])
        self.assertLess(outcome["net_r"], -1)


if __name__ == "__main__":
    unittest.main()

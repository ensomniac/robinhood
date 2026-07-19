import unittest
from datetime import datetime, timedelta

from preentry_structure import (
    EASTERN,
    PreentryStructureError,
    derive_preentry_structure,
    derive_resistance_structure,
    derive_stop_structure,
)


def minute_bar(observed, close=10.0, high=None, low=None):
    return {
        "time_et": observed.isoformat(),
        "open": close,
        "high": high if high is not None else close + 0.02,
        "low": low if low is not None else close - 0.02,
        "close": close,
    }


def daily_bars(count, *, base=9.0, overhead=None):
    start = datetime(2025, 1, 1)
    rows = []
    for index in range(count):
        high = base + index / 10000
        rows.append(
            {
                "date_et": (start + timedelta(days=index)).date().isoformat(),
                "high": high,
            }
        )
    if overhead is not None:
        rows[-1]["high"] = overhead
    return rows


class StopStructureTests(unittest.TestCase):
    def setUp(self):
        self.start = datetime(2026, 3, 3, 9, 30, tzinfo=EASTERN)
        self.observation = self.start + timedelta(minutes=6)
        closes = (10.00, 10.02, 10.01, 10.03, 10.05, 10.06)
        self.bars = [
            minute_bar(
                self.start + timedelta(minutes=index),
                close=close,
                high=10.10 if index == 4 else close + 0.02,
            )
            for index, close in enumerate(closes)
        ]

    def test_stop_uses_opening_high_minus_observed_noise_zone(self):
        result = derive_stop_structure(
            observation_at=self.observation,
            entry_limit=10.11,
            daily_atr_14=0.60,
            median_spread_dollars=0.01,
            completed_regular_bars=self.bars,
        )

        self.assertAlmostEqual(result.opening_range_high, 10.10)
        self.assertAlmostEqual(result.average_close_increment, 0.016)
        self.assertAlmostEqual(result.normal_noise_dollars, 0.016)
        self.assertAlmostEqual(result.technical_invalidation, 10.084)
        self.assertAlmostEqual(result.stop_distance, 0.06)
        self.assertTrue(result.stop_outside_noise)
        self.assertTrue(result.maximum_stop_fraction_pass)

    def test_spread_can_be_the_binding_noise_observation(self):
        result = derive_stop_structure(
            observation_at=self.observation,
            entry_limit=10.11,
            daily_atr_14=0.50,
            median_spread_dollars=0.04,
            completed_regular_bars=self.bars,
        )
        self.assertAlmostEqual(result.normal_noise_dollars, 0.04)
        self.assertAlmostEqual(result.technical_invalidation, 10.06)

    def test_incomplete_opening_minutes_fail_closed(self):
        with self.assertRaises(PreentryStructureError):
            derive_stop_structure(
                observation_at=self.observation,
                entry_limit=10.11,
                daily_atr_14=0.50,
                median_spread_dollars=0.01,
                completed_regular_bars=self.bars[1:],
            )

    def test_bar_finishing_after_observation_is_lookahead(self):
        with self.assertRaises(PreentryStructureError):
            derive_stop_structure(
                observation_at=self.observation,
                entry_limit=10.11,
                daily_atr_14=0.50,
                median_spread_dollars=0.01,
                completed_regular_bars=[
                    *self.bars,
                    minute_bar(self.observation, 10.08),
                ],
            )

    def test_missing_completed_minute_fails_closed(self):
        with self.assertRaises(PreentryStructureError):
            derive_stop_structure(
                observation_at=self.observation + timedelta(minutes=1),
                entry_limit=10.11,
                daily_atr_14=0.50,
                median_spread_dollars=0.01,
                completed_regular_bars=[
                    *self.bars[:5],
                    minute_bar(self.start + timedelta(minutes=6), 10.08),
                ],
            )


class ResistanceStructureTests(unittest.TestCase):
    def setUp(self):
        self.observation = datetime(2026, 3, 3, 9, 36, tzinfo=EASTERN)
        self.premarket = [
            minute_bar(datetime(2026, 3, 3, 8, 0, tzinfo=EASTERN), 10.1, high=10.25)
        ]

    def test_nearest_overhead_level_is_binding_when_inputs_are_complete(self):
        result = derive_resistance_structure(
            observation_at=self.observation,
            entry_limit=10.0,
            premarket_bars=self.premarket,
            premarket_window_complete=True,
            target_adjusted_daily_bars=daily_bars(252, overhead=10.5),
            daily_history_complete=True,
            daily_split_basis_verified=True,
        )
        self.assertEqual(result.status, "RESOLVED_OVERHEAD")
        self.assertEqual(result.resistance_source, "target_premarket_high")
        self.assertAlmostEqual(result.resistance_room_fraction, 0.025)
        self.assertTrue(result.minimum_room_pass)

    def test_known_near_overhead_blocks_even_when_long_history_is_missing(self):
        result = derive_resistance_structure(
            observation_at=self.observation,
            entry_limit=10.1,
            premarket_bars=self.premarket,
            premarket_window_complete=True,
            target_adjusted_daily_bars=daily_bars(14),
            daily_history_complete=False,
            daily_split_basis_verified=True,
        )
        self.assertEqual(result.status, "BLOCKED_KNOWN_OVERHEAD")
        self.assertFalse(result.minimum_room_pass)

    def test_missing_long_history_cannot_create_favorable_clear_sky(self):
        result = derive_resistance_structure(
            observation_at=self.observation,
            entry_limit=11.0,
            premarket_bars=self.premarket,
            premarket_window_complete=True,
            target_adjusted_daily_bars=daily_bars(14),
            daily_history_complete=False,
            daily_split_basis_verified=True,
        )
        self.assertEqual(result.status, "UNRESOLVED_INPUT")
        self.assertIsNone(result.minimum_room_pass)

    def test_split_adjusted_partial_history_can_only_add_adverse_evidence(self):
        result = derive_resistance_structure(
            observation_at=self.observation,
            entry_limit=10.0,
            premarket_bars=[],
            premarket_window_complete=True,
            target_adjusted_daily_bars=daily_bars(14, overhead=10.1),
            daily_history_complete=False,
            daily_split_basis_verified=True,
        )

        self.assertEqual(result.status, "BLOCKED_KNOWN_OVERHEAD")
        self.assertEqual(result.resistance_source, "observed_partial_history_high")
        self.assertFalse(result.minimum_room_pass)

    def test_complete_252_session_breakout_can_resolve_price_discovery(self):
        result = derive_resistance_structure(
            observation_at=self.observation,
            entry_limit=11.0,
            premarket_bars=self.premarket,
            premarket_window_complete=True,
            target_adjusted_daily_bars=daily_bars(252),
            daily_history_complete=True,
            daily_split_basis_verified=True,
        )
        self.assertEqual(result.status, "RESOLVED_PRICE_DISCOVERY")
        self.assertTrue(result.minimum_room_pass)
        self.assertIsNone(result.resistance_price)

    def test_incomplete_premarket_window_stays_unresolved(self):
        result = derive_resistance_structure(
            observation_at=self.observation,
            entry_limit=11.0,
            premarket_bars=self.premarket,
            premarket_window_complete=False,
            target_adjusted_daily_bars=daily_bars(252),
            daily_history_complete=True,
            daily_split_basis_verified=True,
        )
        self.assertEqual(result.status, "UNRESOLVED_INPUT")
        self.assertEqual(result.premarket_observation_count, 0)

    def test_target_date_daily_bar_is_lookahead(self):
        rows = daily_bars(252)
        rows[-1]["date_et"] = self.observation.date().isoformat()
        with self.assertRaises(PreentryStructureError):
            derive_resistance_structure(
                observation_at=self.observation,
                entry_limit=11.0,
                premarket_bars=self.premarket,
                premarket_window_complete=True,
                target_adjusted_daily_bars=rows,
                daily_history_complete=True,
                daily_split_basis_verified=True,
            )

    def test_false_complete_attestation_is_rejected(self):
        with self.assertRaises(PreentryStructureError):
            derive_resistance_structure(
                observation_at=self.observation,
                entry_limit=11.0,
                premarket_bars=self.premarket,
                premarket_window_complete=True,
                target_adjusted_daily_bars=daily_bars(251),
                daily_history_complete=True,
                daily_split_basis_verified=True,
            )


class CombinedStructureTests(unittest.TestCase):
    def test_combined_result_retains_contract_identity(self):
        start = datetime(2026, 3, 3, 9, 30, tzinfo=EASTERN)
        result = derive_preentry_structure(
            observation_at=start + timedelta(minutes=6),
            entry_limit=10.11,
            daily_atr_14=0.60,
            median_spread_dollars=0.01,
            completed_regular_bars=[
                minute_bar(start + timedelta(minutes=index), 10 + index / 100)
                for index in range(6)
            ],
            premarket_bars=[],
            premarket_window_complete=True,
            target_adjusted_daily_bars=daily_bars(252),
            daily_history_complete=True,
            daily_split_basis_verified=True,
        )
        self.assertEqual(result.contract_version, "preentry-structure-v1")
        self.assertTrue(result.stop.stop_outside_noise)
        self.assertEqual(result.resistance.status, "RESOLVED_PRICE_DISCOVERY")


if __name__ == "__main__":
    unittest.main()

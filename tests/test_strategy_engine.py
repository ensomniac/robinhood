import copy
import tempfile
import unittest
from pathlib import Path

from strategy_engine import (
    StrategyInputError,
    compute_opening_relative_volume,
    evaluate_candidate,
    load_config,
)


def qualifying_payload():
    return {
        "session": {
            "time_et": "09:40:00",
            "mode": "live",
            "maturity": "PROVISIONAL",
            "agentic_allowed": True,
            "account_identified": True,
            "encryption_ready": True,
            "monitoring_available": True,
            "protective_stop_workflow_ready": True,
            "broker_review_available": True,
            "open_positions": 0,
            "unresolved_orders": 0,
            "filled_entries_today": 0,
            "circuit_breaker_active": False,
            "account_equity": 25000,
            "buying_power": 25000,
        },
        "candidate": {
            "symbol": "XYZ",
            "is_common_stock": True,
            "average_daily_volume_14": 5000000,
            "daily_atr_14": 3.0,
            "opening_bar": {
                "open": 49.5,
                "high": 50.0,
                "low": 49.4,
                "close": 49.9,
                "volume": 600000,
            },
            "prior_opening_volumes": [100000] * 14,
            "opening_rvol_rank": 3,
            "ranking_scope": "full_eligible_universe",
            "ranking_scope_count": 25,
            "verified_catalyst": True,
            "catalyst_score": 25,
            "dilution_conflict": False,
            "halt_risk": False,
            "tradable": True,
            "clean_break": True,
            "above_vwap": True,
            "vwap_flat_or_rising": True,
            "benchmark_supportive_or_independent_strength": True,
            "sector_relative_strength": True,
            "stop_outside_noise": True,
            "entry_limit": 50.05,
            "technical_invalidation": 49.75,
            "resistance_price": 51.55,
            "observed_stop_slippage_p95_fraction": 0.001,
        },
        "quotes": [
            {
                "age_seconds": 1.0,
                "bid": 50.00,
                "ask": 50.04,
                "ask_depth": 10000,
                "recent_real_1m_volume": 20000,
            },
            {
                "age_seconds": 1.5,
                "bid": 50.01,
                "ask": 50.05,
                "ask_depth": 10000,
                "recent_real_1m_volume": 20000,
            },
            {
                "age_seconds": 2.0,
                "bid": 50.01,
                "ask": 50.05,
                "ask_depth": 10000,
                "recent_real_1m_volume": 20000,
            },
        ],
    }


class OpeningRelativeVolumeTests(unittest.TestCase):
    def test_uses_time_matched_fourteen_session_mean(self):
        self.assertEqual(compute_opening_relative_volume(300, [100] * 14), 3.0)

    def test_requires_exactly_fourteen_completed_sessions(self):
        with self.assertRaisesRegex(StrategyInputError, "exactly 14"):
            compute_opening_relative_volume(300, [100] * 13)


class EvaluationTests(unittest.TestCase):
    def test_qualifying_candidate_computes_score_and_sizing(self):
        result = evaluate_candidate(qualifying_payload(), load_config())

        self.assertTrue(result.eligible)
        self.assertEqual(result.score, 100)
        self.assertEqual(result.classification, "A+")
        self.assertEqual(result.opening_relative_volume, 6.0)
        self.assertEqual(result.sizing.quantity, 357)
        self.assertEqual(result.sizing.binding_caps, ("risk",))
        self.assertTrue(result.sizing.allocation_target_met)
        self.assertAlmostEqual(result.sizing.buying_power_fraction, 0.714714)
        self.assertGreaterEqual(result.sizing.reward_risk, 2.5)

    def test_stale_snapshot_is_a_hard_reject(self):
        payload = qualifying_payload()
        payload["quotes"][1]["age_seconds"] = 5.1

        result = evaluate_candidate(payload)

        self.assertFalse(result.eligible)
        self.assertIn("snapshot 2 is stale", result.hard_rejects)

    def test_a_plus_spread_is_enforced_for_unvalidated_live_pilot(self):
        payload = qualifying_payload()
        payload["session"]["maturity"] = "UNVALIDATED"
        for snapshot in payload["quotes"]:
            snapshot["bid"] = 50.0
            snapshot["ask"] = 50.045

        result = evaluate_candidate(payload)

        self.assertEqual(result.score, 100)
        self.assertFalse(result.eligible)
        self.assertEqual(result.classification, "rejected")
        self.assertIn(
            "A+ median spread exceeds the 0.08% limit", result.hard_rejects
        )

        payload["session"]["maturity"] = "PROVISIONAL"
        provisional = evaluate_candidate(payload)
        self.assertTrue(provisional.eligible)
        self.assertEqual(provisional.classification, "qualified")
        self.assertIn(
            "median spread is too wide for A+ classification", provisional.warnings
        )

    def test_score_is_computed_instead_of_trusted_from_input(self):
        payload = qualifying_payload()
        payload["candidate"]["sector_relative_strength"] = False
        payload["candidate"]["claimed_score"] = 100

        result = evaluate_candidate(payload)

        self.assertEqual(result.score, 95)
        self.assertFalse(result.eligible)
        self.assertIn(
            "sector or candidate relative-strength gate failed", result.hard_rejects
        )

    def test_chase_limit_is_enforced(self):
        payload = qualifying_payload()
        payload["candidate"]["entry_limit"] = 50.08
        payload["quotes"][-1]["ask"] = 50.08

        result = evaluate_candidate(payload)

        self.assertFalse(result.eligible)
        self.assertIn(
            "entry would chase too far above the opening-range high",
            result.hard_rejects,
        )

    def test_stop_inside_observed_market_is_a_hard_reject(self):
        payload = qualifying_payload()
        payload["candidate"]["daily_atr_14"] = 0.5
        payload["candidate"]["technical_invalidation"] = 50.04

        result = evaluate_candidate(payload)

        self.assertFalse(result.eligible)
        self.assertAlmostEqual(result.sizing.planned_stop, 50.0)
        self.assertIn(
            "planned stop is not below the observed bid", result.hard_rejects
        )

    def test_low_allocation_is_disclosed_but_not_a_hard_reject(self):
        payload = qualifying_payload()
        payload["candidate"]["technical_invalidation"] = 49.65
        payload["candidate"]["resistance_price"] = 51.75

        result = evaluate_candidate(payload)

        self.assertTrue(result.eligible)
        self.assertLess(result.sizing.buying_power_fraction, 0.70)
        self.assertFalse(result.sizing.allocation_target_met)
        self.assertEqual(result.sizing.binding_caps, ("risk",))
        self.assertIn(
            "risk- or liquidity-sized notional is below the aggressive allocation target",
            result.warnings,
        )

    def test_liquidity_can_safely_reduce_quantity_without_discarding_edge(self):
        payload = qualifying_payload()
        for snapshot in payload["quotes"]:
            snapshot["ask_depth"] = 4000
            snapshot["recent_real_1m_volume"] = 10000

        result = evaluate_candidate(payload)

        self.assertTrue(result.eligible)
        self.assertEqual(result.sizing.quantity, 200)
        self.assertEqual(result.sizing.binding_caps, ("liquidity",))
        self.assertIn("liquidity cap reduced the risk-sized quantity", result.warnings)

    def test_existing_position_blocks_entry(self):
        payload = copy.deepcopy(qualifying_payload())
        payload["session"]["open_positions"] = 1

        result = evaluate_candidate(payload)

        self.assertFalse(result.eligible)
        self.assertIn("an equity position already exists", result.hard_rejects)

    def test_scanner_only_rank_is_explicitly_disclosed(self):
        payload = qualifying_payload()
        payload["candidate"]["ranking_scope"] = "scanner_results"

        result = evaluate_candidate(payload)

        self.assertTrue(result.eligible)
        self.assertEqual(result.ranking_scope, "scanner_results")
        self.assertIn(
            "opening RVOL rank is approximate within scanner results", result.warnings
        )

    def test_inconsistent_opening_ohlc_is_rejected_as_input_error(self):
        payload = qualifying_payload()
        payload["candidate"]["opening_bar"]["high"] = 49.0

        with self.assertRaisesRegex(StrategyInputError, "inconsistent OHLC"):
            evaluate_candidate(payload)

    def test_open_price_gate_uses_validated_opening_bar_not_duplicate_field(self):
        payload = qualifying_payload()
        payload["candidate"]["opening_price"] = 500.0
        payload["candidate"]["opening_bar"].update(
            {"open": 4.99, "high": 50.0, "low": 4.9, "close": 49.9}
        )

        result = evaluate_candidate(payload)

        self.assertFalse(result.eligible)
        self.assertIn("opening price is below the universe minimum", result.hard_rejects)

    def test_flat_opening_bar_is_valid_input_but_not_bullish(self):
        payload = qualifying_payload()
        payload["candidate"]["opening_bar"].update(
            {"open": 49.5, "high": 49.5, "low": 49.5, "close": 49.5}
        )

        result = evaluate_candidate(payload)

        self.assertFalse(result.eligible)
        self.assertIn("first five-minute candle is not bullish", result.hard_rejects)

    def test_zero_opening_volume_is_valid_input_but_fails_rvol_gate(self):
        payload = qualifying_payload()
        payload["candidate"]["opening_bar"]["volume"] = 0

        result = evaluate_candidate(payload)

        self.assertFalse(result.eligible)
        self.assertEqual(result.opening_relative_volume, 0.0)
        self.assertIn("opening relative volume is below 1.0", result.hard_rejects)


class ConfigValidationTests(unittest.TestCase):
    def _invalid_config(self, old: str, new: str) -> Path:
        self.directory = tempfile.TemporaryDirectory()
        path = Path(self.directory.name) / "strategy.toml"
        source = Path("strategy_config.toml").read_text(encoding="utf-8")
        self.assertIn(old, source)
        path.write_text(source.replace(old, new), encoding="utf-8")
        return path

    def tearDown(self):
        directory = getattr(self, "directory", None)
        if directory is not None:
            directory.cleanup()

    def test_rejects_inverted_spread_limits(self):
        path = self._invalid_config(
            "maximum_a_plus_median_spread_fraction = 0.0008",
            "maximum_a_plus_median_spread_fraction = 0.0011",
        )
        with self.assertRaisesRegex(StrategyInputError, "spread limits"):
            load_config(path)

    def test_rejects_inverted_session_times(self):
        path = self._invalid_config(
            'entry_cutoff_et = "10:30:00"', 'entry_cutoff_et = "09:34:00"'
        )
        with self.assertRaisesRegex(StrategyInputError, "entry_start"):
            load_config(path)

    def test_rejects_malformed_toml_without_traceback_contract(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "strategy.toml"
            path.write_text("[strategy\n", encoding="utf-8")
            with self.assertRaisesRegex(StrategyInputError, "valid UTF-8 TOML"):
                load_config(path)

    def test_rejects_missing_promotion_metric_before_maturity_assessment(self):
        path = self._invalid_config(
            "maximum_entry_slippage_p95_bps = 15.0",
            "maximum_entry_slippage_p95_bps_missing = 15.0",
        )
        with self.assertRaisesRegex(
            StrategyInputError, "maximum_entry_slippage_p95_bps must be numeric"
        ):
            load_config(path)

    def test_rejects_validated_gate_weaker_than_provisional(self):
        path = self._invalid_config(
            "minimum_profit_factor = 1.30",
            "minimum_profit_factor = 1.10",
        )
        with self.assertRaisesRegex(StrategyInputError, "cannot weaken provisional"):
            load_config(path)


if __name__ == "__main__":
    unittest.main()

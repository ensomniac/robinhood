from __future__ import annotations

import math
import tempfile
import unittest
from pathlib import Path

import equity_gap_continuation_validation as validation
import tests.test_equity_gap_continuation_stage0 as stage0_fixture
import portfolio_maturity as maturity
from historical_store import HistoricalDayStore


class EquityGapContinuationValidationTests(unittest.TestCase):
    def test_published_development_collection_is_complete_and_return_free(self):
        status = validation._load_json(validation.PUBLIC_STATUS)
        self.assertEqual(status["status"], "READY")
        self.assertEqual(status["sample_phase"], "development")
        self.assertEqual(status["candidate_symbol_sessions"], 4833)
        self.assertEqual(status["provider_symbols_received"], 4833)
        self.assertEqual(status["unresolved_symbol_sessions"], 0)
        self.assertEqual(status["target_returns_computed"], 0)
        self.assertEqual(status["broker_actions"], 0)

    def test_published_development_input_inspection_reconciles_full_denominator(self):
        path = (
            Path(__file__).resolve().parents[1]
            / "strategy_validation/equity_gap_continuation/inspections"
            / "equity-gap-continuation-v1-development-inputs-36ef7b4db655573630d0e407e35d3a1e9ed43a44c91da6bf9646a5a991066761.json"
        )
        inspection = validation._load_json(path)
        self.assertEqual(
            inspection["inspection_sha256"],
            validation.common._self_hash(inspection, "inspection_sha256"),
        )
        self.assertEqual(inspection["candidate_symbol_sessions"], 4833)
        self.assertEqual(inspection["minute_bars"], 1860990)
        self.assertEqual(inspection["exact_390_contiguous_symbol_sessions"], 3148)
        self.assertEqual(inspection["sparse_symbol_sessions_retained_as_no_signal"], 1685)
        self.assertEqual(inspection["returns_computed"], 0)
        self.assertTrue(inspection["return_evaluation_authorized"])

    def test_published_development_result_is_an_independently_rebuilt_failure(self):
        root = Path(__file__).resolve().parents[1]
        result = validation._load_json(
            root
            / "research_results/2026-07-21-equity-gap-continuation-development-3add7a7bdbd3a47282663a0daa41e17d1b6d3e6e780f08d959cf81ca4f2dba92.json"
        )
        inspection = validation._load_json(
            root
            / "strategy_validation/equity_gap_continuation/inspections/equity-gap-continuation-v1-development-result-bf1050d3d88b2402890befd5c583f2b623b14fa1b33f2edc9f89692e130db8c6.json"
        )
        self.assertEqual(
            result["result_sha256"],
            validation.common._self_hash(result, "result_sha256"),
        )
        self.assertEqual(result["denominator"]["closed_signals"], 111)
        self.assertFalse(result["development_passed"])
        self.assertLess(result["robustness"]["expectancy_r"], 0)
        self.assertLess(result["robustness"]["profit_factor"], 1.0)
        self.assertGreater(result["robustness"]["maximum_drawdown_r"], 6.0)
        self.assertTrue(inspection["valid"])
        self.assertFalse(inspection["phase_passed"])
        self.assertEqual(inspection["result_sha256"], result["result_sha256"])



    def test_published_freeze_locks_both_samples_before_outcomes(self):
        path = (
            Path(__file__).resolve().parents[1]
            / "strategy_validation/equity_gap_continuation/manifests"
            / "equity-gap-continuation-v1-0145f77948ff8d7398c69ec7ae2229b72cfb7a07bab055c3dcfb15762d5cea43.json"
        )
        manifest = validation._load_json(path)
        validation._validate_manifest(manifest)
        self.assertEqual(manifest["denominator"]["development_dates"], 120)
        self.assertEqual(
            manifest["denominator"]["development_candidate_symbol_sessions"],
            4833,
        )
        self.assertEqual(manifest["denominator"]["embargo_dates"], 5)
        self.assertEqual(manifest["denominator"]["confirmation_dates"], 75)
        self.assertEqual(
            manifest["denominator"]["confirmation_candidate_symbol_sessions"],
            3192,
        )
        self.assertFalse(
            manifest["access_contract"]["confirmation_collection_before_development_pass"]
        )
        self.assertFalse(
            manifest["access_contract"]["confirmation_outcomes_observed_or_derived"]
        )
        self.assertEqual(
            manifest["supersedes_freeze"]["manifest_sha256"],
            "fc4674e3384b107043d80bf97a883ed297db3bc36b6f78f223e5cee297cc3809",
        )
        self.assertEqual(
            manifest["supersedes_freeze"]["prior_return_results_computed"], 0
        )

    def test_published_freeze_inspection_computes_no_returns(self):
        path = (
            Path(__file__).resolve().parents[1]
            / "strategy_validation/equity_gap_continuation/inspections"
            / "equity-gap-continuation-v1-freeze-1aa8fad408438bf19824ae1c01ede6ea35c446238d75113666e337e0fdecec67.json"
        )
        inspection = validation._load_json(path)
        self.assertEqual(
            inspection["inspection_sha256"],
            validation.common._self_hash(inspection, "inspection_sha256"),
        )
        self.assertTrue(inspection["valid"])
        self.assertEqual(inspection["returns_computed"], 0)
        self.assertEqual(inspection["provider_requests"], 0)
        self.assertTrue(inspection["development_collection_authorized"])
        self.assertFalse(inspection["confirmation_collection_authorized"])

    def test_preopen_candidate_gate_is_inclusive_and_outcome_blind(self):
        rows = [
            {
                "symbol": "LOW",
                "instrument_id": "one",
                "primary_exchange": "XNYS",
                "open_price": 102.0,
                "prior_close": 100.0,
                "disposition": "below_minimum_opening_rvol",
            },
            {
                "symbol": "HIGH",
                "instrument_id": "two",
                "primary_exchange": "XNAS",
                "open_price": 108.0,
                "prior_close": 100.0,
                "disposition": "non_bullish_opening_candle",
            },
            {
                "symbol": "OUT",
                "instrument_id": "three",
                "primary_exchange": "XNAS",
                "open_price": 108.01,
                "prior_close": 100.0,
                "disposition": "eligible",
            },
            {"symbol": "MISSING", "disposition": "incomplete_target_opening_bar"},
        ]
        selected = validation._candidates("2025-01-02", {"evaluations": rows})
        self.assertEqual([row["symbol"] for row in selected], ["HIGH", "LOW"])
        self.assertAlmostEqual(selected[0]["gap_fraction"], 0.08)
        self.assertAlmostEqual(selected[1]["gap_fraction"], 0.02)

    def test_freeze_uses_120_dates_five_embargo_and_75_confirmation(self):
        with tempfile.TemporaryDirectory() as directory:
            store = HistoricalDayStore(Path(directory), min_free_bytes=0)
            manifest, private = validation.build_freeze(store)
        self.assertEqual(manifest["denominator"]["source_dates"], 200)
        self.assertEqual(manifest["denominator"]["development_dates"], 120)
        self.assertEqual(manifest["denominator"]["embargo_dates"], 5)
        self.assertEqual(manifest["denominator"]["confirmation_dates"], 75)
        self.assertEqual(len(private["phases"]["development"]["dates"]), 120)
        self.assertEqual(len(private["embargo_dates"]), 5)
        self.assertEqual(len(private["phases"]["confirmation"]["dates"]), 75)
        self.assertLess(
            max(private["phases"]["development"]["dates"]),
            min(private["embargo_dates"]),
        )
        self.assertLess(
            max(private["embargo_dates"]),
            min(private["phases"]["confirmation"]["dates"]),
        )
        self.assertFalse(private["target_outcomes_observed_or_derived"])

    def test_confirmation_stays_locked_without_both_published_development_artifacts(self):
        with self.assertRaisesRegex(
            validation.GapValidationError,
            "development result and independent inspection",
        ):
            validation._confirmation_access_allowed(None, None)

    def test_infinite_profit_factor_is_serialized_without_nonstandard_json(self):
        metrics = maturity.RobustnessMetrics(
            signals=30,
            expectancy_r=0.2,
            profit_factor=math.inf,
            maximum_drawdown_r=0.0,
            bootstrap_lower_expectancy_r=0.1,
            first_half_total_r=3.0,
            second_half_total_r=3.0,
            without_five_best_total_r=1.0,
            stress_10_total_r=4.0,
            stress_10_profit_factor=math.inf,
            stress_10_drawdown_r=0.0,
            stress_20_total_r=2.0,
            stress_20_profit_factor=math.inf,
            stress_20_drawdown_r=0.0,
            rule_violations=0,
            incomplete_capture_records=0,
        )
        rendered = validation._serializable_metrics(metrics)
        self.assertIsNone(rendered["profit_factor"])
        self.assertTrue(rendered["profit_factor_infinite"])
        self.assertTrue(rendered["stress_10_profit_factor_infinite"])
        self.assertTrue(rendered["stress_20_profit_factor_infinite"])

    def test_production_evaluator_preserves_next_bar_fill_and_inclusive_gap(self):
        candidate = validation._evaluate_candidate(
            day="2025-01-02",
            symbol="TEST",
            bars=stage0_fixture.bars(),
            prior_close=103.0 / 1.08,
        )
        self.assertEqual(candidate["status"], "executable")
        self.assertEqual(candidate["entry_index"], 16)
        self.assertEqual(candidate["entry_open"], 103.35)


if __name__ == "__main__":
    unittest.main()

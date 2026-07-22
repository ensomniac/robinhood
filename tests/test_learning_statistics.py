import math
import unittest

from learning_statistics import (
    LearningStatisticsError,
    analyze_account_path,
    deflated_sharpe_probability,
    holm_family_decisions,
    materialize_account_path,
    maximum_drawdown_fraction,
    power_sample_target,
    probability_of_backtest_overfitting,
    simulate_portfolio_account,
    stationary_bootstrap_summary,
)


def day(day_text, outcome, gross=0.0, cost=0.0):
    active = outcome in {"filled", "missed"}
    return {
        "date": day_text,
        "signal_id": f"signal-{day_text}" if active else None,
        "signal_intent": active or outcome == "rejected",
        "detection_status": "detected" if active else "none",
        "order_intent": active,
        "outcome": outcome,
        "protection_status": "protected" if outcome == "filled" else "not_applicable",
        "gross_return_fraction": gross,
        "cost_fraction": cost,
    }


class AccountPathTests(unittest.TestCase):
    def test_misses_and_no_signal_days_are_explicit_zero_returns(self):
        path = materialize_account_path(
            [
                day("2026-07-14", "filled", 0.02, 0.002),
                day("2026-07-15", "missed"),
                day("2026-07-16", "no_signal"),
            ],
            starting_equity=100_000,
        )

        self.assertAlmostEqual(path[0]["net_return_fraction"], 0.018)
        self.assertEqual([item["net_return_fraction"] for item in path[1:]], [0.0, 0.0])
        self.assertAlmostEqual(path[-1]["ending_equity"], 101_800)

    def test_higher_cost_cannot_improve_or_remove_the_trade(self):
        low = materialize_account_path(
            [day("2026-07-14", "filled", 0.02, 0.001)], starting_equity=100_000
        )
        high = materialize_account_path(
            [day("2026-07-14", "filled", 0.02, 0.010)], starting_equity=100_000
        )

        self.assertEqual(len(low), len(high))
        self.assertEqual(low[0]["outcome"], high[0]["outcome"])
        self.assertLess(high[0]["ending_equity"], low[0]["ending_equity"])

    def test_nonfilled_outcome_cannot_hide_return_or_cost(self):
        with self.assertRaisesRegex(LearningStatisticsError, "explicit zero returns"):
            materialize_account_path(
                [day("2026-07-14", "missed", 0.01, 0.0)], starting_equity=100_000
            )

    def test_dates_are_unique_and_chronological(self):
        with self.assertRaisesRegex(
            LearningStatisticsError, "unique and chronological"
        ):
            materialize_account_path(
                [day("2026-07-15", "no_signal"), day("2026-07-14", "no_signal")],
                starting_equity=100_000,
            )

    def test_compounded_drawdown_uses_account_equity(self):
        self.assertAlmostEqual(maximum_drawdown_fraction([0.10, -0.10]), 0.10)

    def test_portfolio_simulator_compounds_overlap_and_records_contention(self):
        dates = [
            "2026-07-13",
            "2026-07-14",
            "2026-07-15",
            "2026-07-16",
            "2026-07-17",
        ]
        candidates = [
            {
                "signal_id": "first",
                "signal_date": dates[0],
                "outcome": "eligible",
                "entry_price": 100.0,
                "stop_price": 99.0,
                "exit_date": dates[2],
                "exit_price": 104.0,
                "marks": {dates[0]: 100.0, dates[1]: 101.0, dates[2]: 104.0},
                "rank": 1,
            },
            {
                "signal_id": "second",
                "signal_date": dates[1],
                "outcome": "eligible",
                "entry_price": 50.0,
                "stop_price": 49.5,
                "exit_date": dates[3],
                "exit_price": 51.0,
                "marks": {dates[1]: 50.0, dates[2]: 50.5, dates[3]: 51.0},
                "rank": 1,
            },
            {
                "signal_id": "third",
                "signal_date": dates[1],
                "outcome": "eligible",
                "entry_price": 25.0,
                "stop_price": 24.5,
                "exit_date": dates[2],
                "exit_price": 26.0,
                "marks": {dates[1]: 25.0, dates[2]: 26.0},
                "rank": 2,
            },
        ]
        result = simulate_portfolio_account(
            dates,
            candidates,
            starting_equity=100_000,
            risk_fraction=0.005,
            maximum_concurrent_positions=2,
            maximum_new_entries_per_day=5,
            maximum_aggregate_risk_fraction=0.0125,
            maximum_gross_notional_fraction=1.0,
            cost_bps_per_side=5,
        )

        self.assertGreater(result["ending_equity"], result["starting_equity"])
        self.assertEqual(len(result["closed_trades"]), 2)
        blocked = [
            item
            for item in result["trial_accounting"]
            if item["outcome"] == "capital_blocked"
        ]
        self.assertEqual([item["signal_id"] for item in blocked], ["third"])
        self.assertEqual(result["account_path"][-1]["session_outcome"], "no_signal")
        self.assertEqual(len(result["account_path"]), len(dates))

    def test_portfolio_simulator_primary_and_stress_costs_are_monotone(self):
        dates = ["2026-07-13", "2026-07-14"]
        candidate = {
            "signal_id": "one",
            "signal_date": dates[0],
            "outcome": "eligible",
            "entry_price": 100.0,
            "stop_price": 99.0,
            "exit_date": dates[1],
            "exit_price": 101.0,
            "marks": {dates[0]: 100.0, dates[1]: 101.0},
        }
        results = [
            simulate_portfolio_account(
                dates,
                [candidate],
                starting_equity=100_000,
                risk_fraction=0.005,
                maximum_concurrent_positions=3,
                maximum_new_entries_per_day=5,
                maximum_aggregate_risk_fraction=0.0125,
                maximum_gross_notional_fraction=1.0,
                cost_bps_per_side=bps,
            )
            for bps in (5, 10, 20)
        ]
        self.assertGreater(
            results[0]["compounded_return_fraction"],
            results[1]["compounded_return_fraction"],
        )
        self.assertGreater(
            results[1]["compounded_return_fraction"],
            results[2]["compounded_return_fraction"],
        )


class StatisticalControlTests(unittest.TestCase):
    def test_stationary_bootstrap_is_deterministic_and_block_aware(self):
        values = [0.01, 0.01, -0.01, -0.01] * 5
        first = stationary_bootstrap_summary(values, samples=200, seed=7)
        second = stationary_bootstrap_summary(values, samples=200, seed=7)

        self.assertEqual(first, second)
        self.assertEqual(first["mean_block_length"], 5)

    def test_holm_stops_after_first_failed_ordered_hypothesis(self):
        result = holm_family_decisions({"a": 0.01, "b": 0.04, "c": 0.2}, alpha=0.10)

        self.assertTrue(result["a"]["reject_null"])
        self.assertTrue(result["b"]["reject_null"])
        self.assertFalse(result["c"]["reject_null"])

    def test_deflated_sharpe_penalizes_larger_search_family(self):
        returns = [0.01, -0.002, 0.008, -0.001] * 20
        small = deflated_sharpe_probability(returns, [0.0, 0.1])
        large = deflated_sharpe_probability(
            returns, [value / 10 for value in range(20)]
        )

        self.assertIsNotNone(small["probability"])
        self.assertLess(large["probability"], small["probability"])

    def test_pbo_detects_in_sample_winner_reversal(self):
        first = [0.02] * 8 + [-0.02] * 8
        second = [-0.02] * 8 + [0.02] * 8
        result = probability_of_backtest_overfitting(
            {"first": first, "second": second}, slices=8
        )

        self.assertEqual(result["combinations"], 35)
        self.assertGreater(result["probability"], 0)

    def test_power_target_respects_configured_floor(self):
        self.assertEqual(
            power_sample_target(1.0, 0.1, configured_floor=50),
            50,
        )

    def test_full_analysis_counts_zero_days_and_all_trials(self):
        path = materialize_account_path(
            [
                day("2026-07-14", "filled", 0.01, 0.001),
                day("2026-07-15", "missed"),
                day("2026-07-16", "filled", -0.005, 0.001),
                day("2026-07-17", "no_signal"),
            ],
            starting_equity=100_000,
        )
        metrics = analyze_account_path(
            path,
            trial_sharpes=[0.1, 0.2, -0.1],
            family_p_values={"one": 0.03, "two": 0.2},
            strategy_daily_returns={
                "one": [0.009, 0, -0.006, 0] * 4,
                "two": [-0.001, 0.002, 0, 0] * 4,
            },
            bootstrap_samples=200,
        )

        self.assertEqual(metrics["requested_days"], 4)
        self.assertEqual(metrics["zero_return_days"], 2)
        self.assertEqual(metrics["deflated_sharpe"]["trials"], 3)
        self.assertTrue(math.isfinite(metrics["compounded_return"]))


if __name__ == "__main__":
    unittest.main()

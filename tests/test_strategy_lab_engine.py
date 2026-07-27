from __future__ import annotations

import tempfile
import unittest
from datetime import date
from pathlib import Path

from strategy_lab.contracts import StrategySpec
from strategy_lab.data import HistoricalCatalog
from strategy_lab.database import LabDatabase
from strategy_lab.engine import BacktestEngine
from strategy_lab.statistics import family_adjustment, one_sided_wilson_lower

from tests.strategy_lab_helpers import make_test_config, populate_observations


def daily_spec(*, strategy_id: str, stop: float = 0.01, hold: int = 1) -> StrategySpec:
    return StrategySpec(
        strategy_id=strategy_id,
        family_id="family-engine-contract-test",
        idea_id="idea-engine-contract-test",
        horizon="daily",
        signal={
            "op": "gte",
            "left": {"feature": "return_1d"},
            "right": {"value": -1.0},
        },
        rank_by="return_1d",
        rank_direction="desc",
        entry="next_open",
        stop_loss_pct=stop,
        target_pct=0.04,
        maximum_hold_sessions=hold,
        round_trip_bps=10,
        causal_thesis="Synthetic data isolates the execution and portfolio boundary.",
        falsifier="Any optimistic gap fill or risk-cap violation fails the contract.",
        parameters={"threshold": -1.0, "stop": stop, "target": 0.04},
    )


class StrategyLabEngineTests(unittest.TestCase):
    def test_stop_first_and_exact_replay_use_the_shared_engine(self):
        with tempfile.TemporaryDirectory() as directory:
            config = make_test_config(Path(directory), sessions=24)
            with LabDatabase(config) as database:
                populate_observations(
                    database,
                    sessions=24,
                    both_stop_and_target=True,
                )
                HistoricalCatalog(config, database).build_feature_mart()
                spec = StrategySpec(
                    strategy_id="strategy-stop-first-test",
                    family_id="family-stop-first-test",
                    idea_id="idea-stop-first-test",
                    horizon="daily",
                    signal={
                        "op": "gte",
                        "left": {"feature": "return_1d"},
                        "right": {"value": -1.0},
                    },
                    rank_by="return_1d",
                    rank_direction="desc",
                    entry="next_open",
                    stop_loss_pct=0.01,
                    target_pct=0.01,
                    maximum_hold_sessions=1,
                    round_trip_bps=10,
                    causal_thesis="A synthetic always-on signal tests deterministic execution.",
                    falsifier="Any non-stop result when stop and target coexist is a failure.",
                    parameters={"threshold": -1.0, "stop": 0.01, "target": 0.01},
                )
                database.register_spec(spec)
                result = BacktestEngine(config, database).evaluate(
                    spec,
                    phase="development",
                    cumulative_trial_count=1,
                )
                self.assertGreater(len(result.trades), 0)
                self.assertTrue(
                    all(trade.exit_reason == "stop" for trade in result.trades)
                )
                self.assertTrue(
                    all(trade.net_return < -0.01 for trade in result.trades)
                )
                rebuilt = BacktestEngine(config, database).evaluate(
                    spec,
                    phase="development",
                    cumulative_trial_count=1,
                )
                self.assertEqual(
                    result.metrics["evaluation_sha256"],
                    rebuilt.metrics["evaluation_sha256"],
                )

    def test_selection_adjustment_and_win_bound_are_fail_closed(self):
        adjustment = family_adjustment(
            [
                {
                    "strategy_id": "strategy-a",
                    "pvalue": 0.01,
                    "stressed_log_growth": 1.0,
                    "fold_log_growth": [1, 1, 1, 1, 1],
                },
                {
                    "strategy_id": "strategy-b",
                    "pvalue": 0.20,
                    "stressed_log_growth": -1.0,
                    "fold_log_growth": [-1, -1, -1, -1, -1],
                },
            ]
        )
        self.assertTrue(adjustment.holm_pass["strategy-a"])
        self.assertFalse(adjustment.holm_pass["strategy-b"])
        self.assertGreater(one_sided_wilson_lower(6, 10), 0)
        self.assertLess(one_sided_wilson_lower(6, 10), 0.5)

    def test_daily_gap_through_stop_fills_at_the_open(self):
        with tempfile.TemporaryDirectory() as directory:
            config = make_test_config(Path(directory), sessions=24)
            with LabDatabase(config) as database:
                engine = BacktestEngine(config, database)
                spec = daily_spec(
                    strategy_id="strategy-gap-stop-test",
                    stop=0.02,
                    hold=2,
                )
                row = {
                    "symbol": "AAA",
                    "session_date": date(2024, 1, 2),
                    "future_date_1": date(2024, 1, 3),
                    "future_open_1": 100.0,
                    "future_high_1": 101.0,
                    "future_low_1": 99.0,
                    "future_close_1": 100.0,
                    "future_date_2": date(2024, 1, 4),
                    "future_open_2": 95.0,
                    "future_high_2": 99.0,
                    "future_low_2": 94.0,
                    "future_close_2": 96.0,
                    "rank_value": 1.0,
                }
                trade = engine._raw_trade(spec, row, stress_bps=20)
                self.assertIsNotNone(trade)
                self.assertEqual(trade["exit_reason"], "gap_stop")
                self.assertEqual(trade["exit_price"], 95.0)

    def test_aggregate_planned_open_loss_caps_third_position(self):
        with tempfile.TemporaryDirectory() as directory:
            config = make_test_config(Path(directory), sessions=24)
            with LabDatabase(config) as database:
                engine = BacktestEngine(config, database)
                spec = daily_spec(
                    strategy_id="strategy-aggregate-risk-test",
                    stop=0.02,
                    hold=2,
                )
                raw = [
                    {
                        "signal_date": date(2024, 1, 2),
                        "entry_date": date(2024, 1, 3),
                        "exit_date": date(2024, 1, 4),
                        "symbol": symbol,
                        "entry_price": 100.0,
                        "exit_price": 101.0,
                        "exit_reason": "maximum_hold",
                        "gross_return": 0.01,
                        "net_return": 0.009,
                        "stressed_net_return": 0.008,
                        "rank_value": float(rank),
                    }
                    for rank, symbol in enumerate(("AAA", "BBB", "CCC"), 1)
                ]
                trades = engine._allocate(spec, raw)
                planned_loss = sum(
                    trade.notional_fraction * spec.stop_loss_pct for trade in trades
                )
                self.assertEqual(len(trades), 3)
                self.assertLessEqual(planned_loss, 0.0125 + 1e-12)
                self.assertAlmostEqual(
                    min(trade.notional_fraction for trade in trades),
                    0.125,
                )


if __name__ == "__main__":
    unittest.main()

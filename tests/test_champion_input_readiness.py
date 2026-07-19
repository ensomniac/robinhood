import json
import unittest
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from champion_input_readiness import (
    benchmark_alignment,
    completed_bar_state,
    quote_readiness,
)


EASTERN = ZoneInfo("America/New_York")


class ChampionInputReadinessTests(unittest.TestCase):
    def test_quote_contract_recomputes_spread_chase_and_visible_size(self):
        rules = {
            "maximum_quote_age_seconds": 5.0,
            "maximum_median_spread_fraction": 0.001,
            "maximum_single_spread_fraction": 0.0015,
            "maximum_entry_chase_fraction": 0.0015,
        }
        snapshots = [
            {"bid": 10.00, "ask": 10.01, "age_seconds": 1, "ask_size": 500},
            {"bid": 10.01, "ask": 10.02, "age_seconds": 2, "ask_size": 400},
            {"bid": 10.01, "ask": 10.02, "age_seconds": 3, "ask_size": 300},
        ]
        result = quote_readiness(snapshots, 10.01, rules)
        self.assertTrue(result["basic_fresh_uncrossed"])
        self.assertTrue(result["spread_pass"])
        self.assertTrue(result["chase_pass"])
        self.assertEqual(result["minimum_visible_ask_depth_shares"], 300)

    def test_completed_bars_never_use_an_incomplete_interval(self):
        start = datetime(2026, 1, 2, 9, 30, tzinfo=EASTERN)
        rows = [
            {
                "time_et": (start + timedelta(minutes=index)).isoformat(),
                "open": 10 + index,
                "high": 11 + index,
                "low": 9 + index,
                "close": 10.5 + index,
                "volume": 100,
                "wap": 10.25 + index,
            }
            for index in range(3)
        ]
        state = completed_bar_state(rows, start + timedelta(minutes=2, seconds=30))
        self.assertIsNotNone(state)
        self.assertEqual(state["completed_bar_count"], 2)
        self.assertEqual(state["last_close"], 11.5)

    def test_market_contract_does_not_grant_an_undefined_exception(self):
        base = {
            "last_close": 99,
            "vwap": 100,
            "vwap_flat_or_rising": False,
            "last_bar_makes_five_bar_low": True,
            "return_from_open": -0.01,
        }
        result = benchmark_alignment(10, 10.5, {"SPY": base, "QQQ": base})
        self.assertFalse(result["benchmark_supportive"])
        self.assertTrue(result["candidate_outperforms_both"])
        self.assertTrue(result["both_benchmarks_unsupportive"])

    def test_public_result_is_aggregate_only_when_generated(self):
        path = (
            Path(__file__).resolve().parents[1]
            / "research_results"
            / "2026-07-19-champion-input-readiness.json"
        )
        if not path.exists():
            self.skipTest("readiness artifact is generated after freezing")
        value = json.loads(path.read_text(encoding="utf-8"))
        self.assertEqual(value["status"], "READY")
        self.assertFalse(
            value["findings"]["unchanged_champion_outcome_evaluation_allowed"]
        )
        rendered = path.read_text(encoding="utf-8")
        for forbidden in ('"symbol"', '"records"', '"cik"', '"accession"'):
            self.assertNotIn(forbidden, rendered)


if __name__ == "__main__":
    unittest.main()

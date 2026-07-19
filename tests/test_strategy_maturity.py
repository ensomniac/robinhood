import unittest

from strategy_engine import load_config
from strategy_maturity import assess_maturity


def record(index, net_r, *, live=False, confirmation=False, stopped=False):
    config = load_config()
    return {
        "signal_id": f"2026-07-{index + 1:02d}-XYZ-1",
        "date": f"2026-07-{index + 1:02d}",
        "strategy_version": config.version,
        "rules_hash": config.rules_hash,
        "closed": True,
        "triggered": True,
        "session_capture_complete": True,
        "sample_phase": "confirmation" if confirmation else "pilot",
        "mode": "live" if live else "shadow",
        "net_r": net_r,
        "entry_slippage_bps": 8.0 if live else None,
        "unprotected_seconds": 5.0 if live else None,
        "stop_executed": stopped,
        "stop_slippage_bps": 8.0 if stopped else None,
        "stop_reserve_bps": 10.0 if stopped else None,
        "rule_violations": [],
    }


class MaturityTests(unittest.TestCase):
    def test_empty_sample_remains_unvalidated(self):
        result = assess_maturity([])

        self.assertEqual(result.earned_maturity, "UNVALIDATED")
        self.assertIn(
            "closed signals 0 is below required 20", result.provisional_blockers
        )

    def test_twenty_positive_signals_with_live_execution_earn_provisional(self):
        records = [
            record(index, 0.5 if index % 2 == 0 else -0.2, live=index < 5)
            for index in range(20)
        ]

        result = assess_maturity(records)

        self.assertEqual(result.earned_maturity, "PROVISIONAL")
        self.assertEqual(result.provisional_blockers, ())

    def test_strong_fifty_signal_sample_earns_validated(self):
        records = [
            record(
                index,
                0.8 if index % 5 else -0.4,
                live=index < 10,
                confirmation=index >= 30,
                stopped=index < 5,
            )
            for index in range(50)
        ]

        result = assess_maturity(records)

        self.assertEqual(result.earned_maturity, "VALIDATED")
        self.assertEqual(result.validated_blockers, ())
        self.assertGreater(result.metrics.bootstrap_lower_expectancy_r, 0)

    def test_one_large_outlier_does_not_pass_bootstrap_gate(self):
        records = [
            record(
                index,
                10.0 if index == 0 else -0.1,
                live=index < 10,
                confirmation=index >= 30,
                stopped=index < 5,
            )
            for index in range(50)
        ]

        result = assess_maturity(records)

        self.assertNotEqual(result.earned_maturity, "VALIDATED")
        self.assertIn(
            "bootstrap lower expectancy R is not above 0", result.validated_blockers
        )

    def test_mixed_rules_hash_blocks_promotion(self):
        records = [
            record(index, 0.5 if index % 2 == 0 else -0.2, live=index < 5)
            for index in range(20)
        ]
        records[0]["rules_hash"] = "old-rules"

        result = assess_maturity(records)

        self.assertEqual(result.earned_maturity, "UNVALIDATED")
        self.assertIn(
            "one or more records use a different rules hash",
            result.provisional_blockers,
        )

    def test_old_rule_rejection_does_not_poison_new_performance_sample(self):
        records = [
            record(index, 0.5 if index % 2 == 0 else -0.2, live=index < 5)
            for index in range(20)
        ]
        rejected = record(20, -0.1)
        rejected.update(
            {
                "rules_hash": "old-rules",
                "closed": True,
                "triggered": False,
                "net_r": None,
            }
        )
        records.append(rejected)

        result = assess_maturity(records)

        self.assertEqual(result.earned_maturity, "PROVISIONAL")
        self.assertEqual(result.metrics.mismatched_rule_records, 0)

    def test_incomplete_universe_capture_blocks_promotion(self):
        records = [
            record(index, 0.5 if index % 2 == 0 else -0.2, live=index < 5)
            for index in range(20)
        ]
        records[0]["session_capture_complete"] = False

        result = assess_maturity(records)

        self.assertEqual(result.earned_maturity, "UNVALIDATED")
        self.assertIn(
            "one or more closed signals lack complete-universe capture",
            result.provisional_blockers,
        )

    def test_incomplete_live_execution_evidence_blocks_promotion(self):
        records = [
            record(index, 0.5 if index % 2 == 0 else -0.2, live=index < 5)
            for index in range(20)
        ]
        records[0]["entry_slippage_bps"] = None

        result = assess_maturity(records)

        self.assertEqual(result.earned_maturity, "UNVALIDATED")
        self.assertIn(
            "one or more live execution records are incomplete",
            result.provisional_blockers,
        )


if __name__ == "__main__":
    unittest.main()

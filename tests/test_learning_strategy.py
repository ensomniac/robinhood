import unittest

from learning_strategy import (
    LearningStrategyError,
    anytime_hoeffding_confidence_sequence,
    audit_strategy_evidence,
    build_strategy_evidence_report,
    compare_champion_challenger,
    monitor_strategy_health,
    strategy_readiness,
    validate_axis_transition,
)


class StrategyAxisTests(unittest.TestCase):
    def test_readiness_requires_all_three_axes(self):
        base = {
            "alpha_state": "CONFIRMED",
            "execution_state": "SHADOW_VERIFIED",
            "operations_state": "READY",
            "production_role": "RESEARCH_CHALLENGER",
        }

        self.assertEqual(strategy_readiness(base), "SHADOW_QUALIFIED")
        self.assertEqual(
            strategy_readiness({**base, "alpha_state": "DEVELOPMENT"}),
            "RESEARCH_ONLY",
        )
        self.assertEqual(
            strategy_readiness({**base, "operations_state": "PAUSED"}), "PAUSED"
        )
        self.assertEqual(
            strategy_readiness({**base, "alpha_state": "RETIRED"}), "RETIRED"
        )

    def test_invalid_axis_shortcut_is_rejected(self):
        with self.assertRaisesRegex(LearningStrategyError, "not allowed"):
            validate_axis_transition("execution", "UNVERIFIED", "LIVE_CALIBRATED")
        with self.assertRaisesRegex(LearningStrategyError, "not allowed"):
            validate_axis_transition("alpha", "RETIRED", "DEVELOPMENT")

    def test_current_registry_has_one_champion_and_reports_real_maturity(self):
        audit = audit_strategy_evidence()
        report = build_strategy_evidence_report()

        self.assertEqual(audit["champion"], "strategy-2026-07-15-orb-v3")
        self.assertEqual(report["production_maturity"], "UNVALIDATED")
        self.assertEqual(
            report["strategies"]["strategy-2026-07-15-orb-v3"]["readiness"],
            "RESEARCH_ONLY",
        )
        self.assertEqual(
            report["strategies"]["strategy-early-item-2.02-reversal-v1"]["readiness"],
            "RETIRED",
        )


class ChampionChallengerTests(unittest.TestCase):
    def test_paired_comparison_preserves_zero_return_days(self):
        result = compare_champion_challenger(
            [
                {
                    "date": "2026-07-14",
                    "champion_return": 0.0,
                    "challenger_return": 0.01,
                },
                {
                    "date": "2026-07-15",
                    "champion_return": 0.0,
                    "challenger_return": 0.0,
                },
                {
                    "date": "2026-07-16",
                    "champion_return": -0.005,
                    "challenger_return": 0.0,
                },
            ]
        )

        self.assertEqual(result["requested_days"], 3)
        self.assertEqual(result["champion_zero_days"], 2)
        self.assertEqual(result["challenger_zero_days"], 2)
        self.assertFalse(result["automatic_promotion"])

    def test_time_uniform_evidence_can_identify_superior_challenger(self):
        days = [
            {
                "date": f"{2020 + index // 365:04d}-{1 + (index % 365) // 30:02d}-{1 + index % 28:02d}",
                "champion_return": 0.0,
                "challenger_return": 0.02,
            }
            for index in range(2500)
        ]
        # Use stable strictly increasing synthetic labels; comparison only needs ordering.
        for index, item in enumerate(days):
            item["date"] = f"day-{index:04d}"
        result = compare_champion_challenger(days)

        self.assertEqual(result["decision"], "CHALLENGER_SUPERIOR")
        self.assertGreater(result["confidence_sequence"][-1]["lower"], 0)

    def test_confidence_sequence_rejects_returns_outside_frozen_bound(self):
        with self.assertRaisesRegex(LearningStrategyError, "exceeds"):
            anytime_hoeffding_confidence_sequence([0.2])


class HealthMonitoringTests(unittest.TestCase):
    def test_integrity_or_protection_failure_pauses_immediately(self):
        result = monitor_strategy_health(
            [0.01],
            rule_violations=0,
            protection_failures=1,
            evidence_hash_matches=True,
        )

        self.assertEqual(result["status"], "PAUSE_IMMEDIATELY")

    def test_sustained_negative_growth_marks_degraded(self):
        result = monitor_strategy_health(
            [-0.02] * 2500,
            rule_violations=0,
            protection_failures=0,
            evidence_hash_matches=True,
        )

        self.assertEqual(result["status"], "DEGRADED")
        self.assertFalse(result["automatic_strategy_change"])


if __name__ == "__main__":
    unittest.main()

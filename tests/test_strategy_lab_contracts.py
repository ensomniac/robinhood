from __future__ import annotations

import unittest

from strategy_lab.config import ConfigError, load_config, validate_config
from strategy_lab.contracts import ContractError, StrategySpec
from strategy_lab.generator import expand_specs, fallback_ideas


def spec(identifier: str, provenance: dict[str, str]) -> StrategySpec:
    return StrategySpec(
        strategy_id=identifier,
        family_id="family-test-v1",
        idea_id="idea-test-v1",
        horizon="daily",
        signal={
            "op": "gte",
            "left": {"feature": "return_5d"},
            "right": {"value": 0.05},
        },
        rank_by="return_5d",
        rank_direction="desc",
        entry="next_open",
        stop_loss_pct=0.01,
        target_pct=0.02,
        maximum_hold_sessions=2,
        round_trip_bps=10,
        causal_thesis="Persistent information diffuses into prices over several sessions.",
        falsifier="The exact rule fails net growth and stability gates after costs.",
        parameters={"momentum": 0.05, "stop": 0.01, "target": 0.02},
        provenance=provenance,
    )


class StrategyLabContractTests(unittest.TestCase):
    def test_shipped_config_relationships_are_valid(self):
        loaded = load_config()
        self.assertEqual(loaded.raw["daily_run"]["target_configurations"], 250)
        self.assertEqual(
            loaded.raw["execution"]["same_interval_ambiguity"], "stop_first"
        )

    def test_config_rejects_relaxed_risk_or_daily_ceiling(self):
        raw = load_config().raw
        raw["pilot_risk"]["maximum_planned_loss_fraction_per_position"] = 0.006
        with self.assertRaisesRegex(ConfigError, "0.5%"):
            validate_config(raw)

    def test_semantic_hash_ignores_labels_and_provenance(self):
        left = spec("strategy-left", {"run": "a"})
        right = spec("strategy-right", {"run": "b"})
        self.assertEqual(left.rules_sha256, right.rules_sha256)

    def test_unknown_features_and_rule_drift_fail_closed(self):
        payload = spec("strategy-safe", {}).to_dict()
        payload["signal"]["left"]["feature"] = "future_return"
        with self.assertRaisesRegex(ContractError, "unsupported feature"):
            StrategySpec.from_dict(payload)
        payload = spec("strategy-safe", {}).to_dict()
        payload["target_pct"] = 0.03
        with self.assertRaisesRegex(ContractError, "rules_sha256"):
            StrategySpec.from_dict(payload)

    def test_expansion_produces_exactly_250_semantic_uniques(self):
        specs = expand_specs(
            fallback_ideas(),
            target=250,
            round_trip_bps=10,
            provenance={"test": True},
        )
        self.assertEqual(len(specs), 250)
        self.assertEqual(len({item.rules_sha256 for item in specs}), 250)
        self.assertGreaterEqual(len({item.family_id for item in specs}), 8)

    def test_expansion_can_supply_consecutive_fresh_500_configuration_batches(self):
        first = expand_specs(
            fallback_ideas(),
            target=500,
            round_trip_bps=10,
            provenance={"run": "first"},
        )
        second = expand_specs(
            fallback_ideas(),
            target=500,
            round_trip_bps=10,
            provenance={"run": "second"},
            existing_rules={item.rules_sha256 for item in first},
        )
        self.assertEqual(len(first), 500)
        self.assertEqual(len(second), 500)
        self.assertFalse(
            {item.rules_sha256 for item in first}
            & {item.rules_sha256 for item in second}
        )


if __name__ == "__main__":
    unittest.main()

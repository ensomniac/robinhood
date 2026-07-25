import json
import tempfile
import unittest
from pathlib import Path

import asr_single_rule_preflight as preflight
import asr_single_rule_preflight_inspection as inspection


class AsrSingleRulePreflightTests(unittest.TestCase):
    def test_inventory_rebuilds_without_market_outcomes(self):
        inventory = preflight.build_inventory(enforce_commit=False)

        self.assertEqual(inventory["verified_agreement_count"], 185)
        self.assertEqual(inventory["independent_disclosure_count"], 89)
        self.assertEqual(inventory["daily_ranked_signal_capacity"], 86)
        self.assertEqual(inventory["development_candidate_count"], 50)
        self.assertEqual(inventory["confirmation_candidate_count"], 35)
        self.assertEqual(
            inventory["untouched_confirmation_signal_capacity"], 3
        )
        self.assertEqual(len(inventory["embargo_dates"]), 5)

    def test_exact_rule_fails_closed_before_price_access(self):
        contract = preflight.build_contract(
            created_at="2026-07-25T23:41:03Z",
            enforce_commit=False,
        )

        self.assertEqual(contract["selection_mode"], "preselected_primary")
        self.assertEqual(contract["trial_count"], 1)
        self.assertEqual(
            contract["capacity_disposition"],
            "INSUFFICIENT_POWER_CAPACITY",
        )
        self.assertFalse(contract["provider_access_permitted"])
        self.assertFalse(contract["market_price_access_permitted"])
        self.assertEqual(contract["market_price_values_accessed"], 0)
        self.assertEqual(contract["returns_computed"], 0)

    def test_independent_inspection_rebuilds_terminal_capacity(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path, _contract = preflight.freeze(
                created_at="2026-07-25T23:41:03Z",
                output_root=root / "contracts",
                enforce_commit=False,
            )
            inspection_path, result = inspection.inspect(
                path,
                inspected_at="2026-07-25T23:42:00Z",
                output_root=root / "inspections",
                enforce_commit=False,
            )

            self.assertTrue(inspection_path.is_file())
            self.assertEqual(result["state"], "INSUFFICIENT_POWER_CAPACITY")
            self.assertEqual(result["untouched_confirmation_signal_capacity"], 3)
            self.assertFalse(result["single_rule_price_access_permitted"])
            self.assertTrue(all(result["checks"].values()))

    def test_hash_addressed_contract_rejects_mutation(self):
        with tempfile.TemporaryDirectory() as directory:
            path, _contract = preflight.freeze(
                created_at="2026-07-25T23:41:03Z",
                output_root=Path(directory),
                enforce_commit=False,
            )
            changed = json.loads(path.read_text(encoding="utf-8"))
            changed["trial_count"] = 2
            path.write_text(json.dumps(changed), encoding="utf-8")

            with self.assertRaisesRegex(
                preflight.AsrSingleRulePreflightError,
                "mutated",
            ):
                preflight.load_contract(path)


if __name__ == "__main__":
    unittest.main()

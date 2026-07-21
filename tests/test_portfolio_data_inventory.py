from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import portfolio_data_inventory as inventory


class PortfolioDataInventoryTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.rebuilt = inventory.build_inventory()

    def test_published_inventory_rebuilds_exactly(self):
        recorded = json.loads(
            inventory.DEFAULT_INVENTORY_PATH.read_text(encoding="utf-8")
        )
        self.assertEqual(recorded, self.rebuilt)
        self.assertEqual(inventory.inspect_inventory()["valid"], True)

    def test_legacy_corpus_is_screening_not_confirmation(self):
        corpus = self.rebuilt["legacy_bundle_corpus"]
        self.assertEqual(corpus["available_dates"], 95)
        self.assertEqual(corpus["candidate_rows"], 950)
        self.assertEqual(corpus["minute_bars"], 370_500)
        self.assertEqual(corpus["interpolated_minute_bars"], 0)
        self.assertEqual(corpus["claim_scope"], "FALSIFICATION_ONLY")
        self.assertTrue(corpus["outcomes_previously_accessed"])
        self.assertFalse(corpus["untouched_confirmation_eligible"])

    def test_inventory_did_not_contact_provider_or_broker(self):
        self.assertEqual(self.rebuilt["provider_requests"], 0)
        self.assertEqual(self.rebuilt["broker_actions"], 0)
        self.assertEqual(self.rebuilt["outcome_calculations"], 0)

    def test_exact_ten_initial_family_routes_are_accounted(self):
        routes = self.rebuilt["strategy_data_routes"]
        self.assertEqual(len(routes), 10)
        self.assertEqual(len({item["strategy_family"] for item in routes}), 10)

    def test_etf_cache_sample_supports_manifest_freeze_not_validation(self):
        samples = {
            item["symbol"]: item
            for item in self.rebuilt["canonical_store_bounded_sample"]
        }
        for symbol in ("SPY", "QQQ"):
            self.assertGreaterEqual(samples[symbol]["dates"], 245)
            self.assertTrue(
                all(
                    any(
                        dataset["timeframe"] == "1m"
                        and dataset["row_count"] == 390
                        and dataset["complete"]
                        for dataset in sample["datasets"]
                    )
                    for sample in samples[symbol]["samples"]
                )
            )
        self.assertTrue(
            self.rebuilt["store_contract"][
                "bounded_inventory_is_not_full_store_validation"
            ]
        )

    def test_retired_confirmation_is_never_relabelled(self):
        prior = self.rebuilt["prior_research"]["retired_confirmation"]
        self.assertEqual(prior["next_stage"], "stop_without_threshold_tuning")
        self.assertLess(prior["total_r"], 0)
        self.assertIn("never untouched confirmation", prior["reuse_boundary"])

    def test_content_hash_tampering_is_rejected(self):
        altered = dict(self.rebuilt)
        altered["provider_requests"] = 1
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "inventory.json"
            path.write_text(json.dumps(altered), encoding="utf-8")
            with self.assertRaisesRegex(
                inventory.PortfolioDataInventoryError, "content hash"
            ):
                inventory.inspect_inventory(path)


if __name__ == "__main__":
    unittest.main()

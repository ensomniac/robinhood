from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import portfolio_maturity as maturity
import portfolio_tournament as tournament


class PortfolioTournamentTests(unittest.TestCase):
    def setUp(self):
        manifests = sorted(
            tournament.MANIFEST_ROOT.glob("portfolio-stage0-slate-*.json")
        )
        self.assertEqual(len(manifests), 1)
        self.path = manifests[0]

    def test_published_manifest_exactly_rebuilds_and_inspects(self):
        recorded = json.loads(self.path.read_text(encoding="utf-8"))
        self.assertEqual(recorded, tournament.build_manifest())
        inspection = tournament.inspect_manifest(self.path)
        self.assertTrue(inspection["valid"])
        self.assertEqual(inspection["mechanism_families"], 10)
        self.assertEqual(inspection["variants"], 10)

    def test_slate_has_unique_ordered_families_and_rule_hashes(self):
        manifest = tournament.build_manifest()
        variants = manifest["variants"]
        self.assertEqual(
            [item["variant_ordinal"] for item in variants], list(range(1, 11))
        )
        self.assertEqual(
            len({item["mechanism_family"] for item in variants}), len(variants)
        )
        self.assertEqual(len({item["rules_hash"] for item in variants}), len(variants))
        for variant in variants:
            payload = {
                key: value for key, value in variant.items() if key != "rules_hash"
            }
            self.assertEqual(variant["rules_hash"], tournament._hash(payload))

    def test_slate_preserves_capacity_trials_costs_and_holding_limit(self):
        manifest = tournament.build_manifest()
        self.assertEqual(manifest["prior_policy_trials_to_retain"], 15)
        self.assertEqual(manifest["maximum_initial_variants"], 20)
        self.assertEqual(manifest["unused_initial_variant_capacity"], 10)
        for variant in manifest["variants"]:
            self.assertLessEqual(variant["maximum_holding_trading_days"], 5)
            self.assertEqual(variant["execution"]["primary_cost_bps_per_side"], 5)
            self.assertEqual(
                variant["execution"]["stress_cost_bps_per_side"], [10, 20]
            )
            self.assertEqual(
                variant["execution"]["same_interval_ambiguity"], "stop_first"
            )

    def test_slate_does_not_authorize_external_or_outcome_actions(self):
        manifest = tournament.build_manifest()
        self.assertIs(manifest["outcome_access_authorized"], False)
        self.assertIs(manifest["provider_requests_authorized"], False)
        self.assertIs(manifest["broker_actions_authorized"], False)
        self.assertIn(
            "never PILOT_READY", manifest["stage0_falsification"]["effect"]
        )

    def test_stage0_result_cannot_enter_the_maturity_ledger(self):
        record = {
            "schema_version": 1,
            "record_type": "signal",
            "recorded_at": "2026-07-21T12:00:00+00:00",
            "strategy_id": "etf-or-momentum-v1",
            "strategy_version": "0.1.0-stage0",
            "mechanism_family": "etf-opening-range-momentum",
            "rules_hash": "a" * 64,
            "date": "2026-01-02",
            "sample_phase": "stage0",
            "mode": "historical",
            "signal_id": "2026-01-02-etf-or-momentum-v1-stage0",
            "closed": True,
            "eligible": True,
            "net_r": 1.0,
            "stress_10bps_r": 0.9,
            "stress_20bps_r": 0.8,
            "stop_executed": False,
            "session_capture_complete": True,
            "rule_violations": [],
        }
        with self.assertRaisesRegex(maturity.PortfolioMaturityError, "sample phase"):
            maturity.validate_record(record)

    def test_content_tampering_is_detected(self):
        manifest = tournament.build_manifest()
        manifest["variants"][0]["hypothesis"] = "outcome-shaped replacement"
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "tampered.json"
            path.write_text(json.dumps(manifest), encoding="utf-8")
            with self.assertRaisesRegex(
                tournament.PortfolioTournamentError, "content hash"
            ):
                tournament.inspect_manifest(path)

    def test_hash_is_stable_across_whole_float_json_roundtrip(self):
        value = {"target_r": 2.0, "drawdown_r": 8.0, "cost_bps": 5}
        round_tripped = json.loads(json.dumps(value))
        self.assertEqual(tournament._hash(value), tournament._hash(round_tripped))


if __name__ == "__main__":
    unittest.main()

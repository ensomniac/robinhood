import tempfile
import unittest
from datetime import date
from pathlib import Path

from learning_data import (
    LearningDataError,
    append_security_record,
    audit_dataset_claims,
    audit_learning_data,
    freeze_dataset_contract,
    load_frozen_dataset_contract,
    resolve_security,
    security_master_sha256,
    validate_dataset_payload,
)
from learning_registry import append_event


def security_record(
    record_id,
    instrument_id,
    symbol,
    valid_from,
    valid_to,
    status="ACTIVE",
):
    return {
        "schema_version": 1,
        "record_id": record_id,
        "instrument_id": instrument_id,
        "symbol": symbol,
        "primary_exchange": "NASDAQ",
        "security_type": "COMMON",
        "valid_from": valid_from,
        "valid_to": valid_to,
        "status": status,
        "recorded_at": "2026-07-18T18:00:00-04:00",
        "provenance_paths": ["historical_batches/test.json"],
    }


class SecurityMasterTests(unittest.TestCase):
    def test_symbol_history_resolves_without_current_symbol_substitution(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "SECURITY_MASTER.jsonl"
            append_security_record(
                security_record(
                    "instrument-one-old",
                    "instrument-one",
                    "OLD",
                    "2020-01-01",
                    "2021-12-31",
                    "RENAMED",
                ),
                path,
            )
            append_security_record(
                security_record(
                    "instrument-one-new", "instrument-one", "NEW", "2022-01-01", None
                ),
                path,
            )

            self.assertEqual(
                resolve_security("OLD", date(2021, 6, 1), path=path)["instrument_id"],
                "instrument-one",
            )
            self.assertEqual(
                resolve_security("NEW", date(2023, 6, 1), path=path)["instrument_id"],
                "instrument-one",
            )
            with self.assertRaisesRegex(LearningDataError, "exactly one"):
                resolve_security("OLD", date(2023, 6, 1), path=path)

    def test_overlapping_instrument_or_listing_interval_fails(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "SECURITY_MASTER.jsonl"
            append_security_record(
                security_record(
                    "one", "instrument-one", "AAA", "2020-01-01", "2022-01-01"
                ),
                path,
            )
            with self.assertRaisesRegex(LearningDataError, "intervals overlap"):
                append_security_record(
                    security_record("two", "instrument-one", "BBB", "2021-01-01", None),
                    path,
                )

    def test_empty_master_has_deterministic_identity(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "missing.jsonl"
            self.assertEqual(len(security_master_sha256(path)), 64)

    def test_sampled_observation_dates_do_not_invent_continuity(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "SECURITY_MASTER.jsonl"
            record = security_record(
                "sampled", "instrument-one", "AAA", "2026-01-05", "2026-03-16"
            )
            record["observed_dates"] = ["2026-01-05", "2026-03-16"]
            append_security_record(record, path)

            self.assertEqual(
                resolve_security("AAA", date(2026, 1, 5), path=path)["record_id"],
                "sampled",
            )
            with self.assertRaisesRegex(LearningDataError, "exactly one"):
                resolve_security("AAA", date(2026, 2, 2), path=path)


class DatasetLaneTests(unittest.TestCase):
    def test_catalyst_lane_cannot_claim_production_replay(self):
        with self.assertRaisesRegex(LearningDataError, "claim_scope"):
            validate_dataset_payload(
                "dataset-test",
                {
                    "lane": "catalyst_falsification",
                    "claim_scope": "PRODUCTION_POLICY_REPLAY",
                    "evidence_paths": ["evidence.json"],
                    "inspected": True,
                    "point_in_time_evidence": True,
                },
            )

    def test_production_replay_requires_dynamic_0935_universe(self):
        with self.assertRaisesRegex(LearningDataError, "selection_time_et"):
            validate_dataset_payload(
                "dataset-test",
                {
                    "lane": "production_scanner_replay",
                    "claim_scope": "PRODUCTION_POLICY_REPLAY",
                    "evidence_paths": ["evidence.json"],
                    "inspected": False,
                    "universe_contract": {
                        "selection_time_et": "09:30:00",
                        "information_cutoff": "TARGET_SESSION_09:35_ET",
                        "selection_is_dynamic": True,
                        "complete_universe": True,
                        "scanner_rules_sha256": "a" * 64,
                        "security_master_sha256": "b" * 64,
                        "security_master_path": "learning/security-master.jsonl.gz",
                        "security_master_attestation_path": "historical_batches/security-master.json",
                    },
                },
            )

    def test_valid_production_replay_contract_passes(self):
        result = validate_dataset_payload(
            "dataset-test",
            {
                "lane": "production_scanner_replay",
                "claim_scope": "PRODUCTION_POLICY_REPLAY",
                "evidence_paths": ["evidence.json"],
                "inspected": False,
                "universe_contract": {
                    "selection_time_et": "09:35:00",
                    "information_cutoff": "TARGET_SESSION_09:35_ET",
                    "selection_is_dynamic": True,
                    "complete_universe": True,
                    "scanner_rules_sha256": "a" * 64,
                    "security_master_sha256": "b" * 64,
                    "security_master_path": "learning/security-master.jsonl.gz",
                    "security_master_attestation_path": "historical_batches/security-master.json",
                },
            },
        )

        self.assertEqual(result["claim_scope"], "PRODUCTION_POLICY_REPLAY")

    def test_current_public_dataset_claims_audit(self):
        result = audit_learning_data()

        self.assertTrue(result["valid"])
        self.assertEqual(result["datasets"]["datasets"], 3)

    def test_public_audit_uses_attestation_when_licensed_master_is_absent(self):
        with tempfile.TemporaryDirectory() as directory:
            project = Path(directory)
            registry_root = project / "learning"
            attestation = project / "historical_batches" / "security-master.json"
            attestation.parent.mkdir(parents=True)
            attestation.write_text(
                '{"security_master":{"sha256":"' + "b" * 64 + '"}}\n',
                encoding="utf-8",
            )
            append_event(
                "datasets",
                {
                    "schema_version": 1,
                    "event_id": "dataset-scanner-test-registered",
                    "entity_id": "dataset-scanner-test",
                    "event_type": "registered",
                    "recorded_at": "2026-07-18T18:00:00-04:00",
                    "payload": {
                        "lane": "production_scanner_replay",
                        "claim_scope": "PRODUCTION_POLICY_REPLAY",
                        "status": "READY",
                        "evidence_paths": ["historical_batches/security-master.json"],
                        "inspected": True,
                        "universe_contract": {
                            "selection_time_et": "09:35:00",
                            "information_cutoff": "TARGET_SESSION_09:35_ET",
                            "selection_is_dynamic": True,
                            "complete_universe": True,
                            "scanner_rules_sha256": "a" * 64,
                            "security_master_sha256": "b" * 64,
                            "security_master_path": "learning/private.jsonl.gz",
                            "security_master_attestation_path": "historical_batches/security-master.json",
                        },
                    },
                },
                registry_root,
            )

            result = audit_dataset_claims(registry_root)

            self.assertTrue(result["valid"])

    def test_hash_addressed_contract_rejects_results_and_mutation(self):
        contract = {
            "schema_version": 1,
            "dataset_id": "dataset-scanner-test",
            "registered_at": "2026-07-18T18:00:00-04:00",
            "requested_dates": ["2026-07-14", "2026-07-15"],
            "dataset_payload": {
                "lane": "catalyst_falsification",
                "claim_scope": "FALSIFICATION_ONLY",
                "evidence_paths": ["historical_batches/test.json"],
                "inspected": False,
                "point_in_time_evidence": True,
            },
        }
        with tempfile.TemporaryDirectory() as directory:
            path, frozen = freeze_dataset_contract(contract, Path(directory))
            self.assertEqual(load_frozen_dataset_contract(path), frozen)
            path.write_text(
                path.read_text(encoding="utf-8").replace("2026-07-14", "2026-07-13"),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(LearningDataError, "mutated"):
                load_frozen_dataset_contract(path)
        contract["outcomes"] = [1.0]
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(LearningDataError, "target-session results"):
                freeze_dataset_contract(contract, Path(directory))


if __name__ == "__main__":
    unittest.main()

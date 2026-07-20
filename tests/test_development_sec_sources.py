import gzip
import json
import tempfile
import unittest
from pathlib import Path

from development_catalyst_contract import _rebuild_selection
from development_sec_sources import (
    PROJECT_ROOT,
    STRATEGY_SOURCE,
    DevelopmentSecSourceError,
    _private_identity_path,
    _resolve_pairs,
    _sha256_json,
    _target_response_root,
    freeze_contract,
    inspect_contract,
)
from learning_data import freeze_dataset_contract, security_master_sha256


class DevelopmentSecSourceTests(unittest.TestCase):
    def _fixture(self, root: Path, external: Path) -> dict[str, Path]:
        store = external / "history"
        env = root / ".env"
        env.write_text(
            f"LOCAL_HISTORICAL_DATA_ROOT={store}\n"
            "LOCAL_HISTORICAL_MIN_FREE_GIB=20\n",
            encoding="utf-8",
        )
        selected_id = "dataset-selected-candidate-contract-sec-fixture"
        pairs = [
            {
                "date": "2025-01-02",
                "symbol": "AAA",
                "instrument_id": "FIGI:AAA:LISTING:ONE",
                "primary_exchange": "XNAS",
                "rank": 1,
                "scanner_fields": {
                    "opening_relative_volume": 3.0,
                    "opening_return": 0.03,
                },
            },
            {
                "date": "2025-01-02",
                "symbol": "BBB",
                "instrument_id": "FIGI:BBB",
                "primary_exchange": "XNYS",
                "rank": 2,
                "scanner_fields": {
                    "opening_relative_volume": 2.0,
                    "opening_return": 0.02,
                },
            },
        ]
        private = {
            "schema_version": 1,
            "dataset_id": selected_id,
            "source_dataset_id": "dataset-scanner-fixture",
            "source_summary_sha256": "s" * 64,
            "source_detail_sha256": "d" * 64,
            "selection_time_et": "09:35:00",
            "information_cutoff": "TARGET_SESSION_09:35_ET",
            "selected_pair_count": len(pairs),
            "selected_pairs": pairs,
        }
        private_path = (
            store
            / "_derived/scanner_selected_pairs"
            / selected_id
            / "selected-pairs.json.gz"
        )
        private_path.parent.mkdir(parents=True)
        with gzip.open(private_path, "wt", encoding="utf-8") as target:
            json.dump(private, target)
        shortlist = [
            {
                "symbol": row["symbol"],
                "instrument_id": row["instrument_id"],
                "opening_relative_volume": row["scanner_fields"][
                    "opening_relative_volume"
                ],
                "opening_return": row["scanner_fields"]["opening_return"],
                "rank": row["rank"],
            }
            for row in pairs
        ]
        selected_path, selected = freeze_dataset_contract(
            {
                "schema_version": 1,
                "dataset_id": selected_id,
                "registered_at": "2026-07-19T00:00:00+00:00",
                "requested_dates": ["2025-01-02"],
                "dataset_payload": {
                    "lane": "development",
                    "claim_scope": "DEVELOPMENT_ONLY",
                    "status": "COLLECTING",
                    "evidence_paths": ["DEVELOPMENT_SELECTED_PAIRS.md"],
                    "inspected": False,
                },
                "selection_contract": {
                    "requested_dates": ["2025-01-02"],
                    "selected_pair_count": len(pairs),
                    "daily_shortlists": [
                        {
                            "date": "2025-01-02",
                            "shortlist_count": len(shortlist),
                            "shortlist_sha256": _sha256_json(shortlist),
                        }
                    ],
                    "private_selection_content_sha256": _sha256_json(private),
                },
                "downstream_contract": {
                    "source_outcomes_observed_or_derived": False,
                    "substitutions_allowed": False,
                },
            },
            root / "selected_manifests",
        )
        rebuilt = _rebuild_selection(selected, store)
        source_path, _source = freeze_dataset_contract(
            {
                "schema_version": 1,
                "dataset_id": "dataset-primary-source-semantics-sec-fixture",
                "registered_at": "2026-07-19T00:01:00+00:00",
                "requested_dates": ["2025-01-02"],
                "dataset_payload": {
                    "lane": "development",
                    "claim_scope": "DEVELOPMENT_ONLY",
                    "status": "COLLECTING",
                    "evidence_paths": ["DEVELOPMENT_CATALYST_CONTRACT.md"],
                    "inspected": False,
                },
                "selection_contract": rebuilt,
                "source_rules": {
                    "primary_evidence_only": True,
                    "selection_or_source_substitution_allowed": False,
                },
                "acquisition_contract": {
                    "primary_source_replacement_with_secondary_news_allowed": False
                },
                "outcome_lock": {
                    "post_entry_data_access_allowed": False,
                    "target_outcomes_observed_or_derived": False,
                },
            },
            root / "source_manifests",
        )
        master = root / "master.jsonl"
        records = [
            {
                "schema_version": 1,
                "record_id": "record-aaa",
                "instrument_id": "FIGI:AAA:LISTING:ONE",
                "symbol": "AAA",
                "primary_exchange": "XNAS",
                "security_type": "COMMON",
                "status": "ACTIVE",
                "valid_from": "2025-01-02",
                "valid_to": "2025-01-02",
                "observed_dates": ["2025-01-02"],
                "recorded_at": "2026-07-19T00:00:00+00:00",
                "provenance_paths": ["DEVELOPMENT_TRANCHE.md"],
                "source": {
                    "identity_source": "composite_figi_listing_scoped",
                    "cik": "1234",
                },
            },
            {
                "schema_version": 1,
                "record_id": "record-bbb",
                "instrument_id": "FIGI:BBB",
                "symbol": "BBB",
                "primary_exchange": "XNYS",
                "security_type": "COMMON",
                "status": "ACTIVE",
                "valid_from": "2025-01-02",
                "valid_to": "2025-01-02",
                "observed_dates": ["2025-01-02"],
                "recorded_at": "2026-07-19T00:00:00+00:00",
                "provenance_paths": ["DEVELOPMENT_TRANCHE.md"],
                "source": {"identity_source": "fallback", "cik": None},
            },
        ]
        master.write_text(
            "".join(
                json.dumps(row, sort_keys=True, separators=(",", ":")) + "\n"
                for row in records
            ),
            encoding="utf-8",
        )
        master_source = root / "master-source.json"
        master_source.write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "requested_dates": ["2025-01-02"],
                    "security_master": {
                        "path": str(master.relative_to(PROJECT_ROOT)),
                        "records": 2,
                        "instruments": 2,
                        "sha256": security_master_sha256(master),
                    },
                }
            )
            + "\n",
            encoding="utf-8",
        )
        return {
            "env": env,
            "store": store,
            "selected": selected_path,
            "source": source_path,
            "master": master,
            "master_source": master_source,
            "output": root / "sec_manifests",
            "status": root / "status.json",
        }

    def _freeze(self, paths: dict[str, Path]):
        return freeze_contract(
            source_contract_path=paths["source"],
            selected_manifest_path=paths["selected"],
            master_path=paths["master"],
            master_source_path=paths["master_source"],
            strategy_source_path=STRATEGY_SOURCE,
            env_path=paths["env"],
            output_root=paths["output"],
        )

    def test_freeze_and_inspect_are_idempotent_and_public_is_aggregate_only(self):
        with tempfile.TemporaryDirectory(
            dir=PROJECT_ROOT
        ) as directory, tempfile.TemporaryDirectory() as external:
            paths = self._fixture(Path(directory), Path(external))
            first_path, first = self._freeze(paths)
            second_path, second = self._freeze(paths)
            self.assertEqual(first_path, second_path)
            self.assertEqual(first, second)
            status = inspect_contract(
                manifest_path=first_path,
                source_contract_path=paths["source"],
                selected_manifest_path=paths["selected"],
                master_path=paths["master"],
                master_source_path=paths["master_source"],
                strategy_source_path=STRATEGY_SOURCE,
                env_path=paths["env"],
                status_path=paths["status"],
            )
            self.assertEqual(status["status"], "FROZEN_READY")
            self.assertEqual(status["cik_missing_pair_count"], 1)
            rendered = json.dumps(first, sort_keys=True)
            self.assertNotIn("AAA", rendered)
            self.assertNotIn("0000001234", rendered)
            self.assertFalse(first["outcome_lock"]["post_entry_data_access_allowed"])

    def test_listing_scoped_identity_is_exact_and_missing_cik_is_retained(self):
        with tempfile.TemporaryDirectory(
            dir=PROJECT_ROOT
        ) as directory, tempfile.TemporaryDirectory() as external:
            paths = self._fixture(Path(directory), Path(external))
            _manifest_path, manifest = self._freeze(paths)
            private = _private_identity_path(paths["store"])
            with gzip.open(private, "rt", encoding="utf-8") as source:
                mapping = json.load(source)
            self.assertEqual(mapping["selected_pair_count"], 2)
            self.assertEqual(mapping["listing_scoped_pair_count"], 1)
            self.assertEqual(mapping["cik_missing_pair_count"], 1)
            self.assertEqual(
                [row["identity_disposition"] for row in mapping["pairs"]],
                ["SEC_SUBMISSIONS_READY", "CIK_MISSING"],
            )
            self.assertEqual(len(mapping["submission_requests"]), 1)
            self.assertEqual(manifest["identity_contract"]["mapped_pair_count"], 2)

    def test_ambiguous_point_in_time_identity_fails_closed(self):
        pair = {
            "date": "2025-01-02",
            "instrument_id": "FIGI:AAA",
            "symbol": "AAA",
            "primary_exchange": "XNAS",
            "rank": 1,
        }
        record = {
            "record_id": "one",
            "instrument_id": "FIGI:AAA",
            "symbol": "AAA",
            "primary_exchange": "XNAS",
            "valid_from": "2025-01-02",
            "valid_to": "2025-01-02",
            "observed_dates": ["2025-01-02"],
            "source": {"identity_source": "composite_figi", "cik": "1"},
        }
        with self.assertRaisesRegex(DevelopmentSecSourceError, "exactly once"):
            _resolve_pairs([pair], [record, {**record, "record_id": "two"}])

    def test_preexisting_target_response_blocks_freeze(self):
        with tempfile.TemporaryDirectory(
            dir=PROJECT_ROOT
        ) as directory, tempfile.TemporaryDirectory() as external:
            paths = self._fixture(Path(directory), Path(external))
            response = _target_response_root(paths["store"]) / "submissions/one.json"
            response.parent.mkdir(parents=True)
            response.write_text("{}", encoding="utf-8")
            with self.assertRaisesRegex(
                DevelopmentSecSourceError, "responses exist before"
            ):
                self._freeze(paths)


if __name__ == "__main__":
    unittest.main()

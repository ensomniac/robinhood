import gzip
import json
import tempfile
import unittest
from pathlib import Path

from development_catalyst_contract import (
    DATASET_ID,
    DevelopmentCatalystContractError,
    PROJECT_ROOT,
    _rebuild_selection,
    _sha256_json,
    _source_rules,
    _target_source_root,
    freeze_contract,
    inspect_contract,
)
from learning_data import freeze_dataset_contract


class DevelopmentCatalystContractTests(unittest.TestCase):
    def _fixture(self, root: Path, store_root: Path) -> tuple[Path, Path, Path]:
        store = store_root / "history"
        env = root / ".env"
        env.write_text(
            f"LOCAL_HISTORICAL_DATA_ROOT={store}\n"
            "LOCAL_HISTORICAL_MIN_FREE_GIB=1\n",
            encoding="utf-8",
        )
        source_id = "dataset-selected-candidate-contract-fixture"
        pairs = [
            {
                "date": "2025-01-02",
                "symbol": "AAA",
                "instrument_id": "FIGI:AAA",
                "primary_exchange": "XNAS",
                "rank": 1,
                "scanner_fields": {
                    "opening_relative_volume": 2.0,
                    "opening_return": 0.01,
                },
            }
        ]
        private = {
            "schema_version": 1,
            "dataset_id": source_id,
            "source_dataset_id": "dataset-production-scanner-replay-fixture",
            "source_summary_sha256": "s" * 64,
            "source_detail_sha256": "d" * 64,
            "selection_time_et": "09:35:00",
            "information_cutoff": "TARGET_SESSION_09:35_ET",
            "selected_pair_count": 1,
            "selected_pairs": pairs,
        }
        private_path = (
            store
            / "_derived/scanner_selected_pairs"
            / source_id
            / "selected-pairs.json.gz"
        )
        private_path.parent.mkdir(parents=True)
        with gzip.open(private_path, "wt", encoding="utf-8") as target:
            json.dump(private, target)
        shortlist = [
            {
                "symbol": "AAA",
                "instrument_id": "FIGI:AAA",
                "opening_relative_volume": 2.0,
                "opening_return": 0.01,
                "rank": 1,
            }
        ]
        source_contract = {
            "schema_version": 1,
            "dataset_id": source_id,
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
                "selected_pair_count": 1,
                "daily_shortlists": [
                    {
                        "date": "2025-01-02",
                        "shortlist_count": 1,
                        "shortlist_sha256": _sha256_json(shortlist),
                    }
                ],
                "private_selection_content_sha256": _sha256_json(private),
            },
            "downstream_contract": {
                "source_outcomes_observed_or_derived": False,
                "substitutions_allowed": False,
            },
        }
        source_path, _manifest = freeze_dataset_contract(
            source_contract, root / "source_manifests"
        )
        return env, source_path, store

    def test_rebuild_is_exact_and_public_aggregate_has_no_symbol(self):
        with tempfile.TemporaryDirectory(
            dir=PROJECT_ROOT
        ) as directory, tempfile.TemporaryDirectory() as store_directory:
            root = Path(directory)
            _env, source_path, store = self._fixture(root, Path(store_directory))
            source = json.loads(source_path.read_text(encoding="utf-8"))
            rebuilt = _rebuild_selection(source, store)
            self.assertEqual(rebuilt["selected_pair_count"], 1)
            self.assertEqual(rebuilt["requested_date_count"], 1)
            self.assertNotIn("AAA", json.dumps(rebuilt))

    def test_rules_keep_primary_semantics_and_outcome_independence(self):
        rules = _source_rules()
        self.assertTrue(rules["primary_evidence_only"])
        self.assertTrue(rules["same_day_date_only_fails"])
        self.assertFalse(rules["selection_or_source_substitution_allowed"])
        self.assertEqual(
            rules["terminal_precedence"],
            list(__import__("catalyst_source_semantics").TERMINAL_PRECEDENCE),
        )

    def test_freeze_and_inspect_are_idempotent_and_outcome_locked(self):
        with tempfile.TemporaryDirectory(
            dir=PROJECT_ROOT
        ) as directory, tempfile.TemporaryDirectory() as store_directory:
            root = Path(directory)
            env, source_path, _store = self._fixture(root, Path(store_directory))
            output = root / "manifests"
            first_path, first = freeze_contract(
                source_manifest_path=source_path,
                env_path=env,
                output_root=output,
            )
            second_path, second = freeze_contract(
                source_manifest_path=source_path,
                env_path=env,
                output_root=output,
            )
            self.assertEqual(first_path, second_path)
            self.assertEqual(first, second)
            self.assertFalse(first["outcome_lock"]["post_entry_data_access_allowed"])
            status = inspect_contract(
                manifest_path=first_path,
                source_manifest_path=source_path,
                env_path=env,
                status_path=root / "status.json",
            )
            self.assertEqual(status["status"], "FROZEN_READY")
            self.assertFalse(status["target_outcomes_observed_or_derived"])

    def test_private_selection_drift_fails_closed(self):
        with tempfile.TemporaryDirectory(
            dir=PROJECT_ROOT
        ) as directory, tempfile.TemporaryDirectory() as store_directory:
            root = Path(directory)
            _env, source_path, store = self._fixture(root, Path(store_directory))
            source = json.loads(source_path.read_text(encoding="utf-8"))
            path = (
                store
                / "_derived/scanner_selected_pairs"
                / source["dataset_id"]
                / "selected-pairs.json.gz"
            )
            with gzip.open(path, "rt", encoding="utf-8") as current:
                value = json.load(current)
            value["selected_pairs"][0]["rank"] = 2
            with gzip.open(path, "wt", encoding="utf-8") as target:
                json.dump(value, target)
            with self.assertRaises(DevelopmentCatalystContractError):
                _rebuild_selection(source, store)

    def test_preexisting_target_source_artifact_blocks_freeze(self):
        with tempfile.TemporaryDirectory(
            dir=PROJECT_ROOT
        ) as directory, tempfile.TemporaryDirectory() as store_directory:
            root = Path(directory)
            env, source_path, store = self._fixture(root, Path(store_directory))
            artifact = _target_source_root(store, DATASET_ID) / "row.json"
            artifact.parent.mkdir(parents=True)
            artifact.write_text("{}", encoding="utf-8")
            with self.assertRaises(DevelopmentCatalystContractError):
                freeze_contract(
                    source_manifest_path=source_path,
                    env_path=env,
                    output_root=root / "manifests",
                )


if __name__ == "__main__":
    unittest.main()

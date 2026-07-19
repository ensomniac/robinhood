import gzip
import json
import tempfile
import unittest
from pathlib import Path

import selected_candidate_join as legacy_join
from learning_data import freeze_dataset_contract, load_frozen_dataset_contract
from selected_candidate_join_expansion import (
    _activate_legacy,
    _sha256_json,
    freeze_expansion_join,
)


class SelectedCandidateJoinExpansionTests(unittest.TestCase):
    def _source(self, root: Path, store: Path) -> Path:
        source_id = "dataset-selected-candidate-contract-fixture"
        private = {
            "schema_version": 1,
            "dataset_id": source_id,
            "source_dataset_id": "dataset-production-scanner-replay-fixture",
            "source_summary_sha256": "s" * 64,
            "source_detail_sha256": "d" * 64,
            "selection_time_et": "09:35:00",
            "information_cutoff": "TARGET_SESSION_09:35_ET",
            "selected_pair_count": 1,
            "selected_pairs": [
                {
                    "date": "2026-03-03",
                    "symbol": "AAA",
                    "instrument_id": "FIGI-COMPOSITE:AAA",
                    "primary_exchange": "XNAS",
                    "rank": 1,
                    "scanner_fields": {"opening_high": 10.0},
                }
            ],
        }
        private_path = (
            store
            / "_derived"
            / "scanner_selected_pairs"
            / source_id
            / "selected-pairs.json.gz"
        )
        private_path.parent.mkdir(parents=True)
        with gzip.open(private_path, "wt", encoding="utf-8") as target:
            json.dump(private, target)
        contract = {
            "schema_version": 1,
            "dataset_id": source_id,
            "registered_at": "2026-07-19T00:00:00+00:00",
            "requested_dates": ["2026-03-03"],
            "dataset_payload": {
                "lane": "development",
                "claim_scope": "DEVELOPMENT_ONLY",
                "status": "COLLECTING",
                "evidence_paths": ["SCANNER_SELECTED_PAIRS.md"],
                "inspected": False,
            },
            "selection_contract": {
                "source_dataset_id": private["source_dataset_id"],
                "source_manifest_sha256": "m" * 64,
                "source_summary_sha256": private["source_summary_sha256"],
                "source_detail_sha256": private["source_detail_sha256"],
                "requested_dates": ["2026-03-03"],
                "selected_pair_count": 1,
                "daily_shortlists": [
                    {
                        "date": "2026-03-03",
                        "shortlist_count": 1,
                        "shortlist_sha256": "h" * 64,
                    }
                ],
                "private_selection_content_sha256": _sha256_json(private),
            },
            "downstream_contract": {"selection_only": True},
        }
        path, _manifest = freeze_dataset_contract(contract, root / "source-manifests")
        return path

    def test_freeze_adapts_private_selection_without_public_symbols(self):
        with tempfile.TemporaryDirectory(
            dir=Path(__file__).resolve().parents[1]
        ) as directory, tempfile.TemporaryDirectory() as store_directory:
            root = Path(directory)
            store = Path(store_directory) / "history"
            env = root / ".env"
            env.write_text(f"LOCAL_HISTORICAL_DATA_ROOT={store}\n", encoding="utf-8")
            source = self._source(root, store)
            output = root / "join-manifests"

            path, manifest = freeze_expansion_join(
                source_manifest_path=source,
                env_path=env,
                output_root=output,
                dataset_id="dataset-selected-candidate-join-fixture",
            )
            second_path, second = freeze_expansion_join(
                source_manifest_path=source,
                env_path=env,
                output_root=output,
                dataset_id="dataset-selected-candidate-join-fixture",
            )

            self.assertEqual(load_frozen_dataset_contract(path), manifest)
            self.assertEqual(second_path, path)
            self.assertEqual(second, manifest)
            self.assertNotIn("AAA", path.read_text(encoding="utf-8"))
            self.assertFalse(
                manifest["collection_contract"]["target_outcomes_observed_or_derived"]
            )
            destination = (
                store
                / "_derived"
                / "selected_candidate_join"
                / "dataset-selected-candidate-join-fixture"
                / "selected-pairs.json.gz"
            )
            with gzip.open(destination, "rt", encoding="utf-8") as source_file:
                private = json.load(source_file)
            self.assertEqual(private["selected_pair_count"], 1)
            self.assertEqual(private["dataset_id"], manifest["dataset_id"])

    def test_activation_binds_adapter_and_unchanged_collector(self):
        with tempfile.TemporaryDirectory(
            dir=Path(__file__).resolve().parents[1]
        ) as directory, tempfile.TemporaryDirectory() as store_directory:
            root = Path(directory)
            store = Path(store_directory) / "history"
            env = root / ".env"
            env.write_text(f"LOCAL_HISTORICAL_DATA_ROOT={store}\n", encoding="utf-8")
            source = self._source(root, store)
            path, manifest = freeze_expansion_join(
                source_manifest_path=source,
                env_path=env,
                output_root=root / "join-manifests",
                dataset_id="dataset-selected-candidate-join-fixture",
            )

            activated = _activate_legacy(path)

            self.assertEqual(activated, manifest)
            self.assertEqual(legacy_join.DATASET_ID, manifest["dataset_id"])


if __name__ == "__main__":
    unittest.main()

import json
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path

from selected_candidate_join import (
    DATASET_ID,
    DEFAULT_SOURCE_SUMMARY,
    _load_private_selection,
    _load_scanner_selection,
    _select_quote_snapshots,
    freeze_join,
)
from selected_candidate_join import EASTERN
from historical_store import HistoricalStoreConfig
from learning_data import load_frozen_dataset_contract


PROJECT_ROOT = Path(__file__).resolve().parents[1]
FROZEN_MANIFEST = (
    PROJECT_ROOT
    / "historical_batches"
    / "selected_candidate_join"
    / "manifests"
    / "dataset-selected-candidate-join-2026-07-19-v1-a46ac360d3d7578757dd7ae420b6a6abe3d820d7d3c4895b083cd576ca05fba4.json"
)


class SelectedCandidateJoinTests(unittest.TestCase):
    def test_inspected_scanner_selection_is_exact_and_public_contract_is_private(self):
        private, public = _load_scanner_selection(DEFAULT_SOURCE_SUMMARY)

        self.assertEqual(private["selected_pair_count"], 389)
        self.assertEqual(public["selected_pair_count"], 389)
        self.assertEqual(len(public["daily_shortlists"]), 20)
        self.assertNotIn("selected_pairs", public)
        self.assertNotIn("symbol", json.dumps(public))

    def test_freeze_writes_private_exact_pairs_and_hash_only_public_contract(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store_root = root / "history"
            env_path = root / ".env"
            env_path.write_text(
                f"LOCAL_HISTORICAL_DATA_ROOT={store_root}\n", encoding="utf-8"
            )

            path, frozen = freeze_join(
                summary_path=DEFAULT_SOURCE_SUMMARY,
                env_path=env_path,
                output_root=root / "manifests",
            )

            self.assertEqual(load_frozen_dataset_contract(path), frozen)
            self.assertEqual(frozen["dataset_id"], DATASET_ID)
            self.assertEqual(frozen["dataset_payload"]["lane"], "development")
            self.assertFalse(
                frozen["collection_contract"]["alpha_or_confirmation_claim_allowed"]
            )
            rendered = path.read_text(encoding="utf-8")
            self.assertNotIn("selected_pairs", rendered)
            self.assertNotIn('"symbols_by_date"', rendered)
            config = HistoricalStoreConfig.from_env(env_path)
            private = _load_private_selection(frozen, config.root)
            self.assertEqual(len(private["selected_pairs"]), 389)

    def test_quote_snapshots_use_only_information_available_at_each_target(self):
        trigger = datetime(2026, 3, 3, 9, 35, tzinfo=EASTERN)
        quotes = [
            {
                "time_et": (trigger + timedelta(seconds=offset)).isoformat(),
                "source_timestamp": (
                    trigger + timedelta(seconds=offset)
                ).astimezone().isoformat(),
                "bid": 10 + offset / 1000,
                "ask": 10.01 + offset / 1000,
                "bid_size": 100,
                "ask_size": 200,
            }
            for offset in (-1, 4, 9, 11)
        ]

        snapshots = _select_quote_snapshots(quotes, trigger)

        self.assertEqual(len(snapshots), 3)
        self.assertEqual([row["age_seconds"] for row in snapshots], [1, 1, 1])
        self.assertLessEqual(
            datetime.fromisoformat(snapshots[-1]["observed_at_et"]),
            datetime.fromisoformat(snapshots[-1]["target_at_et"]),
        )

    def test_public_ready_artifacts_are_hash_bound_and_aggregate_only(self):
        manifest = load_frozen_dataset_contract(FROZEN_MANIFEST)
        result = json.loads(
            (
                PROJECT_ROOT
                / "research_results"
                / "2026-07-19-selected-candidate-join.json"
            ).read_text(encoding="utf-8")
        )

        self.assertEqual(manifest["manifest_sha256"], result["manifest_sha256"])
        self.assertEqual(result["status"], "READY")
        self.assertTrue(result["inspected"])
        self.assertEqual(result["counts"]["selected_pairs"], 389)
        self.assertNotIn("records", result)
        self.assertNotIn("selected_pairs_by_date", result)


if __name__ == "__main__":
    unittest.main()

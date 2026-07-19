import tempfile
import unittest
from datetime import datetime
from pathlib import Path

from selected_candidate_fidelity import (
    DATASET_ID,
    EASTERN,
    REFERENCE_ROOT,
    _filing_candidates,
    _load_source_selection,
    _point_in_time_cik_map,
    freeze_fidelity,
)
from historical_store import HistoricalStoreConfig
from learning_data import load_frozen_dataset_contract


PROJECT_ROOT = Path(__file__).resolve().parents[1]


class SelectedCandidateFidelityTests(unittest.TestCase):
    def test_point_in_time_snapshots_map_every_frozen_pair_to_one_cik(self):
        root = HistoricalStoreConfig.from_env(PROJECT_ROOT / ".env").root
        _manifest, selection = _load_source_selection(root)

        mapped, sources = _point_in_time_cik_map(
            selection["selected_pairs"], REFERENCE_ROOT
        )

        self.assertEqual(len(mapped), 389)
        self.assertEqual(len(sources), 20)
        self.assertEqual(len({row["cik"] for row in mapped}), 283)
        self.assertTrue(all(row["cik"] for row in mapped))
        self.assertTrue(
            all(
                row["cik_match_basis"]
                in {"ticker_and_share_class_figi", "unique_ticker_in_dated_snapshot"}
                for row in mapped
            )
        )

    def test_sec_candidates_are_strictly_bounded_by_information_cutoff(self):
        payload = {
            "filings": {
                "recent": {
                    "accessionNumber": ["a", "b", "c", "d"],
                    "form": ["8-K", "8-K", "10-Q", "6-K"],
                    "acceptanceDateTime": [
                        "2026-01-05T09:34:59-05:00",
                        "2026-01-05T09:35:01-05:00",
                        "2026-01-05T09:00:00-05:00",
                        "2025-12-31T23:59:59-05:00",
                    ],
                    "primaryDocument": ["a.htm", "b.htm", "c.htm", "d.htm"],
                    "items": ["2.02", "8.01", "", ""],
                }
            }
        }
        start = datetime(2026, 1, 1, 0, 0, tzinfo=EASTERN)
        cutoff = datetime(2026, 1, 5, 9, 35, tzinfo=EASTERN)

        rows = _filing_candidates(payload, start, cutoff)

        self.assertEqual([row["accession"] for row in rows], ["a"])

    def test_freeze_keeps_exact_pairs_and_ciks_out_of_public_manifest(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store = root / "history"
            env_path = root / ".env"
            env_path.write_text(
                f"LOCAL_HISTORICAL_DATA_ROOT={store}\n", encoding="utf-8"
            )
            # Seed the source private selection from the real ignored store.
            real_root = HistoricalStoreConfig.from_env(PROJECT_ROOT / ".env").root
            source = (
                real_root
                / "_derived"
                / "selected_candidate_join"
                / "dataset-selected-candidate-join-2026-07-19-v1"
                / "selected-pairs.json.gz"
            )
            destination = (
                store
                / "_derived"
                / "selected_candidate_join"
                / "dataset-selected-candidate-join-2026-07-19-v1"
                / "selected-pairs.json.gz"
            )
            destination.parent.mkdir(parents=True)
            destination.write_bytes(source.read_bytes())

            path, frozen = freeze_fidelity(
                env_path=env_path, output_root=root / "manifests"
            )

            self.assertEqual(load_frozen_dataset_contract(path), frozen)
            self.assertEqual(frozen["dataset_id"], DATASET_ID)
            rendered = path.read_text(encoding="utf-8")
            self.assertNotIn('"pairs"', rendered)
            self.assertNotIn('"cik"', rendered)
            self.assertFalse(
                frozen["collection_contract"]["alpha_or_confirmation_claim_allowed"]
            )


if __name__ == "__main__":
    unittest.main()

import gzip
import json
import tempfile
import unittest
from pathlib import Path

from champion_input_fidelity import (
    DATASET_ID,
    _load_calendar,
    _prior_close,
    freeze_inputs,
)
from historical_store import HistoricalStoreConfig
from learning_data import load_frozen_dataset_contract


PROJECT_ROOT = Path(__file__).resolve().parents[1]


class ChampionInputFidelityTests(unittest.TestCase):
    def test_prior_close_uses_frozen_trading_calendar(self):
        calendar = _load_calendar()
        prior = _prior_close("2026-01-05", calendar)
        self.assertEqual(prior.isoformat(), "2026-01-02T16:00:00-05:00")

    def test_freeze_is_private_and_dependency_bound(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store = root / "history"
            env_path = root / ".env"
            env_path.write_text(
                f"LOCAL_HISTORICAL_DATA_ROOT={store}\n", encoding="utf-8"
            )
            real_root = HistoricalStoreConfig.from_env(PROJECT_ROOT / ".env").root
            source = (
                real_root
                / "_derived"
                / "selected_candidate_fidelity"
                / "dataset-selected-candidate-fidelity-2026-07-19-v1"
            )
            target = (
                store
                / "_derived"
                / "selected_candidate_fidelity"
                / "dataset-selected-candidate-fidelity-2026-07-19-v1"
            )
            target.mkdir(parents=True)
            for name in (
                "selection-and-cik-map.json.gz",
                "primary-catalyst-index.json.gz",
                "clean-trigger-index.json.gz",
            ):
                (target / name).write_bytes((source / name).read_bytes())

            path, frozen = freeze_inputs(
                env_path=env_path, output_root=root / "manifests"
            )

            self.assertEqual(load_frozen_dataset_contract(path), frozen)
            self.assertEqual(frozen["dataset_id"], DATASET_ID)
            self.assertIn("classifier_sha256", frozen["collection_contract"])
            self.assertIn("halt_client_sha256", frozen["collection_contract"])
            rendered = path.read_text(encoding="utf-8")
            self.assertNotIn('"pairs"', rendered)
            self.assertNotIn('"symbol"', rendered)
            private_path = (
                store
                / "_derived"
                / "champion_input_fidelity"
                / DATASET_ID
                / "selection.json.gz"
            )
            with gzip.open(private_path, "rt", encoding="utf-8") as stream:
                private = json.load(stream)
            self.assertEqual(private["selected_pair_count"], 389)


if __name__ == "__main__":
    unittest.main()

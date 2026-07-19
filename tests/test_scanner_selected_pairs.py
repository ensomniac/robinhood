import json
import tempfile
import unittest
from pathlib import Path

from learning_data import load_frozen_dataset_contract
from scanner_replay import _sha256_json
from scanner_selected_pairs import (
    PROJECT_ROOT,
    freeze_selection_contract,
    load_scanner_selection,
)


class ScannerSelectedPairTests(unittest.TestCase):
    def _fixture(self, root: Path) -> Path:
        day = "2026-03-03"
        row = {
            "symbol": "AAA",
            "instrument_id": "FIGI-COMPOSITE:AAA",
            "primary_exchange": "XNAS",
            "disposition": "eligible",
            "opening_rvol_rank": 1,
            "open_price": 10.0,
            "opening_high": 10.3,
            "opening_low": 9.9,
            "opening_close": 10.2,
            "opening_volume": 200,
            "opening_relative_volume": 2.0,
            "opening_return": 0.02,
            "average_daily_volume_14": 2_000_000,
            "daily_atr_14": 1.0,
            "prior_close": 10.0,
        }
        shortlist = [
            {
                "symbol": "AAA",
                "instrument_id": "FIGI-COMPOSITE:AAA",
                "opening_relative_volume": 2.0,
                "opening_return": 0.02,
                "rank": 1,
            }
        ]
        detail = {
            "dataset_id": "dataset-production-scanner-replay-fixture",
            "scanner_rules_sha256": "r" * 64,
            "security_master_sha256": "s" * 64,
            "split_actions_sha256": "p" * 64,
            "dates": {day: {"selected_symbols": ["AAA"], "evaluations": [row]}},
        }
        detail_path = root / "detail.json"
        detail_path.write_text(json.dumps(detail), encoding="utf-8")
        import hashlib

        detail_hash = hashlib.sha256(detail_path.read_bytes()).hexdigest()
        summary = {
            "dataset_id": detail["dataset_id"],
            "status": "READY",
            "selection_time_et": "09:35:00",
            "information_cutoff": "TARGET_SESSION_09:35_ET",
            "complete_universe": True,
            "selection_is_dynamic": True,
            "scanner_rules_sha256": detail["scanner_rules_sha256"],
            "security_master_sha256": detail["security_master_sha256"],
            "split_actions_sha256": detail["split_actions_sha256"],
            "requested_dates": [day],
            "dates": [
                {
                    "date": day,
                    "shortlist_count": 1,
                    "shortlist_sha256": _sha256_json(shortlist),
                }
            ],
            "detailed_artifact": {
                "local_path": str(detail_path.relative_to(PROJECT_ROOT)),
                "public": False,
                "sha256": detail_hash,
            },
            "source": {"contract_sha256": "m" * 64},
        }
        summary_path = root / "summary.json"
        summary_path.write_text(json.dumps(summary), encoding="utf-8")
        return summary_path

    def test_private_selection_is_exact_and_public_contract_has_no_symbol(self):
        with tempfile.TemporaryDirectory(dir=PROJECT_ROOT) as directory:
            summary = self._fixture(Path(directory))
            private, public = load_scanner_selection(
                summary, dataset_id="dataset-selected-candidate-contract-test"
            )

            self.assertEqual(private["selected_pair_count"], 1)
            self.assertEqual(public["selected_pair_count"], 1)
            self.assertNotIn("AAA", json.dumps(public))
            self.assertNotIn("selected_pairs", public)

    def test_freeze_keeps_pairs_external_and_manifest_hash_bound(self):
        with tempfile.TemporaryDirectory(
            dir=PROJECT_ROOT
        ) as directory, tempfile.TemporaryDirectory() as store_directory:
            root = Path(directory)
            summary = self._fixture(root)
            env = root / ".env"
            store = Path(store_directory) / "history"
            env.write_text(f"LOCAL_HISTORICAL_DATA_ROOT={store}\n", encoding="utf-8")
            path, frozen = freeze_selection_contract(
                dataset_id="dataset-selected-candidate-contract-test",
                summary_path=summary,
                env_path=env,
                output_root=root / "manifests",
            )
            second_path, second = freeze_selection_contract(
                dataset_id="dataset-selected-candidate-contract-test",
                summary_path=summary,
                env_path=env,
                output_root=root / "manifests",
            )

            self.assertEqual(load_frozen_dataset_contract(path), frozen)
            self.assertEqual(second_path, path)
            self.assertEqual(second, frozen)
            self.assertEqual(frozen["selection_contract"]["selected_pair_count"], 1)
            self.assertFalse(frozen["downstream_contract"]["substitutions_allowed"])
            self.assertNotIn("AAA", path.read_text(encoding="utf-8"))
            self.assertTrue(
                (
                    store
                    / "_derived/scanner_selected_pairs"
                    / "dataset-selected-candidate-contract-test"
                    / "selected-pairs.json.gz"
                ).exists()
            )


if __name__ == "__main__":
    unittest.main()

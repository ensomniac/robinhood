import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from development_sec_sources import MINIMUM_RESERVE_BYTES
from development_sec_supplemental import (
    SOURCE_MANIFEST,
    DevelopmentSecSupplementalError,
    _private_contract_path,
    _select_requests,
    _target_response_root,
    freeze_contract,
    inspect_contract,
)
from historical_store import HistoricalStoreConfig


def _pair(cik, symbol, start="2024-12-29T00:00:00-05:00"):
    return {
        "date": "2025-01-02",
        "instrument_id": f"FIGI:{symbol}",
        "primary_exchange": "XNAS",
        "rank": 1,
        "symbol": symbol,
        "cik": cik,
        "window_start_et": start,
    }


def _descriptor(cik, name, start, end):
    return {
        "cik": cik,
        "name": name,
        "filing_from": start,
        "filing_to": end,
        "url": f"https://data.sec.gov/submissions/{name}",
        "shared_cache_relative_path": f"submissions/files/{name}",
    }


class DevelopmentSecSupplementalTests(unittest.TestCase):
    def _source_state(self, root):
        cik = "0000000001"
        descriptors = [
            _descriptor(cik, "overlap.json", "2024-01-01", "2024-12-31"),
            _descriptor(cik, "outside.json", "2020-01-01", "2020-12-31"),
            _descriptor(cik, "unknown.json", None, None),
        ]
        source_manifest = {
            "manifest_sha256": "m" * 64,
            "request_contract": {
                "user_agent_sha256": "u" * 64,
                "workers": 4,
                "global_minimum_spacing_seconds": 0.15,
                "timeout_seconds": 30.0,
                "maximum_attempts": 4,
                "retry_backoff_seconds": [0.5, 1.0, 2.0],
            },
            "lineage_contract": {
                "strategy": {
                    "strategy_version": "2026-07-15-orb-v3",
                    "rules_hash": "r" * 64,
                }
            },
        }
        config = HistoricalStoreConfig(root=root, min_free_bytes=MINIMUM_RESERVE_BYTES)
        private = {"pairs": [_pair(cik, "AAA")]}
        collection = {
            "supplemental_submission_requests": descriptors,
            "supplemental_request_graph_sha256": "s" * 64,
        }
        return source_manifest, config, private, collection

    def test_selects_only_overlap_and_conservatively_keeps_invalid_range(self):
        cik = "0000000001"
        result = _select_requests(
            descriptors=[
                _descriptor(cik, "overlap.json", "2024-12-01", "2024-12-31"),
                _descriptor(cik, "outside.json", "2020-01-01", "2020-12-31"),
                _descriptor(cik, "unknown.json", None, None),
            ],
            pairs=[_pair(cik, "AAA")],
        )
        self.assertEqual(result["descriptor_count"], 3)
        self.assertEqual(result["selected_request_count"], 2)
        self.assertEqual(result["excluded_request_count"], 1)
        self.assertEqual(
            result["disposition_counts"],
            {
                "EXCLUDED_OUTSIDE_WINDOWS": 1,
                "SELECTED_CONSERVATIVE_INVALID_RANGE": 1,
                "SELECTED_WINDOW_OVERLAP": 1,
            },
        )

    def test_freeze_and_inspect_are_idempotent_and_public_is_aggregate_only(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            state = self._source_state(root)
            output = root / "manifests"
            status_path = root / "status.json"
            with patch(
                "development_sec_supplemental._load_source_state",
                return_value=state,
            ):
                first_path, first = freeze_contract(
                    manifest_path=SOURCE_MANIFEST,
                    output_root=output,
                    require_published_implementation=False,
                )
                second_path, second = freeze_contract(
                    manifest_path=SOURCE_MANIFEST,
                    output_root=output,
                    require_published_implementation=False,
                )
                self.assertEqual(first_path, second_path)
                self.assertEqual(first, second)
                status = inspect_contract(
                    manifest_path=first_path,
                    source_manifest_path=SOURCE_MANIFEST,
                    status_path=status_path,
                    require_published_implementation=False,
                )
            self.assertEqual(status["status"], "FROZEN_READY")
            self.assertEqual(status["descriptor_count"], 3)
            self.assertEqual(status["selected_request_count"], 2)
            self.assertTrue(_private_contract_path(root).exists())
            rendered = json.dumps(first, sort_keys=True)
            self.assertNotIn("AAA", rendered)
            self.assertNotIn("0000000001", rendered)
            self.assertFalse(first["outcome_lock"]["primary_documents_requested"])

    def test_preexisting_target_response_blocks_freeze(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            response = _target_response_root(root) / "one.json"
            response.parent.mkdir(parents=True)
            response.write_text("{}", encoding="utf-8")
            state = self._source_state(root)
            with patch(
                "development_sec_supplemental._load_source_state",
                return_value=state,
            ), self.assertRaisesRegex(
                DevelopmentSecSupplementalError, "responses exist before"
            ):
                freeze_contract(
                    manifest_path=SOURCE_MANIFEST,
                    output_root=root / "manifests",
                    require_published_implementation=False,
                )


if __name__ == "__main__":
    unittest.main()

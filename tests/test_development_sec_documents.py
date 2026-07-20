import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from development_sec_documents import (
    MAIN_MANIFEST,
    SUPPLEMENTAL_MANIFEST,
    DevelopmentSecDocumentsError,
    _build_request_graph,
    _private_contract_path,
    _target_response_root,
    freeze_contract,
    inspect_contract,
)
from development_sec_sources import MINIMUM_RESERVE_BYTES
from historical_store import HistoricalStoreConfig


def _document(cik, accession, primary="event.htm"):
    return {
        "cik": cik,
        "form": "8-K",
        "accession": accession,
        "accepted_at": "2025-01-02T09:00:00-05:00",
        "filing_date": "2025-01-02",
        "report_date": "2025-01-01",
        "items": ["2.02"],
        "primary_document": primary,
        "source_url": (
            "https://www.sec.gov/Archives/edgar/data/"
            f"{int(cik)}/{accession.replace('-', '')}/{primary}"
        ),
    }


def _join(document, symbol, rank=1):
    return {
        "date": "2025-01-02",
        "instrument_id": f"FIGI:{symbol}",
        "primary_exchange": "XNAS",
        "rank": rank,
        "symbol": symbol,
        "filing": {key: value for key, value in document.items() if key != "cik"},
    }


def _index(documents, joins, graph_hash):
    return {
        "candidate_document_requests": documents,
        "candidate_document_graph_sha256": graph_hash,
        "pair_filing_joins": joins,
    }


class DevelopmentSecDocumentsTests(unittest.TestCase):
    def _source_state(self, root):
        first = _document("0000000001", "0000000001-25-000001")
        second = _document("0000000002", "0000000002-25-000001")
        main = _index([first], [_join(first, "AAA")], "a" * 64)
        supplemental = _index(
            [first, second],
            [_join(first, "AAA"), _join(second, "BBB", rank=2)],
            "b" * 64,
        )
        main_manifest = {
            "manifest_sha256": "m" * 64,
            "requested_dates": ["2025-01-02"],
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
        supplemental_manifest = {
            "manifest_sha256": "s" * 64,
            "lineage_contract": {
                "strategy": main_manifest["lineage_contract"]["strategy"]
            },
        }
        config = HistoricalStoreConfig(root=root, min_free_bytes=MINIMUM_RESERVE_BYTES)
        return main_manifest, supplemental_manifest, config, main, supplemental

    def test_deduplicates_identical_documents_and_preserves_every_source_join(self):
        state = self._source_state(Path("/tmp"))
        graph = _build_request_graph(main_index=state[3], supplemental_index=state[4])
        self.assertEqual(graph["counts"]["candidate_document_observations"], 3)
        self.assertEqual(graph["counts"]["unique_document_requests"], 2)
        self.assertEqual(graph["counts"]["duplicate_document_observations"], 1)
        self.assertEqual(graph["counts"]["pair_document_joins"], 3)
        self.assertEqual(
            graph["document_requests"][0]["source_origins"],
            ["MAIN_SUBMISSIONS", "SUPPLEMENTAL_SUBMISSIONS"],
        )

    def test_conflicting_duplicate_document_fails_closed(self):
        state = self._source_state(Path("/tmp"))
        conflict = dict(state[4]["candidate_document_requests"][0])
        conflict["report_date"] = "2025-01-02"
        state[4]["candidate_document_requests"][0] = conflict
        with self.assertRaisesRegex(
            DevelopmentSecDocumentsError, "conflicting metadata"
        ):
            _build_request_graph(main_index=state[3], supplemental_index=state[4])

    def test_freeze_and_inspect_are_idempotent_and_private(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            state = self._source_state(root)
            output = root / "manifests"
            status_path = root / "status.json"
            with (
                patch(
                    "development_sec_documents._load_source_state", return_value=state
                ),
                patch(
                    "development_sec_documents.load_frozen_dataset_contract",
                    side_effect=lambda path: (
                        json.loads(path.read_text())
                        if path.is_relative_to(output)
                        else {"requested_dates": ["2025-01-02"]}
                    ),
                ),
            ):
                first_path, first = freeze_contract(
                    main_manifest_path=MAIN_MANIFEST,
                    supplemental_manifest_path=SUPPLEMENTAL_MANIFEST,
                    output_root=output,
                    require_published_implementation=False,
                )
                second_path, second = freeze_contract(
                    main_manifest_path=MAIN_MANIFEST,
                    supplemental_manifest_path=SUPPLEMENTAL_MANIFEST,
                    output_root=output,
                    require_published_implementation=False,
                )
                status = inspect_contract(
                    manifest_path=first_path,
                    main_manifest_path=MAIN_MANIFEST,
                    supplemental_manifest_path=SUPPLEMENTAL_MANIFEST,
                    status_path=status_path,
                    require_published_implementation=False,
                )
            self.assertEqual(first_path, second_path)
            self.assertEqual(first, second)
            self.assertEqual(status["status"], "FROZEN_READY")
            self.assertEqual(status["counts"]["unique_document_requests"], 2)
            self.assertTrue(_private_contract_path(root).exists())
            rendered = json.dumps(first, sort_keys=True)
            self.assertNotIn("AAA", rendered)
            self.assertNotIn("0000000001", rendered)
            self.assertFalse(first["outcome_lock"]["primary_documents_requested"])

    def test_preexisting_document_response_blocks_freeze(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            response = _target_response_root(root) / "one.json.gz"
            response.parent.mkdir(parents=True)
            response.write_bytes(b"response")
            state = self._source_state(root)
            with (
                patch(
                    "development_sec_documents._load_source_state", return_value=state
                ),
                self.assertRaisesRegex(
                    DevelopmentSecDocumentsError, "responses exist before"
                ),
            ):
                freeze_contract(
                    main_manifest_path=MAIN_MANIFEST,
                    supplemental_manifest_path=SUPPLEMENTAL_MANIFEST,
                    output_root=root / "manifests",
                    require_published_implementation=False,
                )

    def test_explicit_datasets_scope_private_contract_and_response_preflight(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            state = self._source_state(root)
            dataset_id = "dataset-development-sec-primary-documents-test-v3"
            source_dataset_id = "dataset-development-sec-primary-sources-test-v3"
            supplemental_dataset_id = "dataset-development-sec-supplemental-test-v3"
            output = root / "manifests"
            status_path = root / "status.json"
            with (
                patch(
                    "development_sec_documents._load_source_state", return_value=state
                ),
                patch(
                    "development_sec_documents.load_frozen_dataset_contract",
                    side_effect=lambda path: (
                        json.loads(path.read_text())
                        if path.is_relative_to(output)
                        else {"requested_dates": ["2025-01-02"]}
                    ),
                ),
            ):
                manifest_path, manifest = freeze_contract(
                    dataset_id=dataset_id,
                    source_dataset_id=source_dataset_id,
                    supplemental_dataset_id=supplemental_dataset_id,
                    main_manifest_path=MAIN_MANIFEST,
                    supplemental_manifest_path=SUPPLEMENTAL_MANIFEST,
                    output_root=output,
                    require_published_implementation=False,
                )
                status = inspect_contract(
                    manifest_path=manifest_path,
                    dataset_id=dataset_id,
                    source_dataset_id=source_dataset_id,
                    supplemental_dataset_id=supplemental_dataset_id,
                    main_manifest_path=MAIN_MANIFEST,
                    supplemental_manifest_path=SUPPLEMENTAL_MANIFEST,
                    status_path=status_path,
                    require_published_implementation=False,
                )
            self.assertEqual(manifest["dataset_id"], dataset_id)
            self.assertEqual(status["dataset_id"], dataset_id)
            self.assertTrue(
                _private_contract_path(root, source_dataset_id, dataset_id).is_file()
            )
            self.assertFalse(_private_contract_path(root).exists())
            self.assertFalse(_target_response_root(root, source_dataset_id).exists())

    def test_unsafe_source_dataset_is_rejected_before_private_path_use(self):
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(
                DevelopmentSecDocumentsError, "dataset ID is unsafe"
            ):
                _private_contract_path(
                    Path(directory), "../cross-tranche", "dataset-safe"
                )


if __name__ == "__main__":
    unittest.main()

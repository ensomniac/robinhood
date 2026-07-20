import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from development_sec_sources import MINIMUM_RESERVE_BYTES
from development_sec_supplemental_collection import (
    _derive_response,
    _index_path,
    _sha256_json,
    _wrapper_path,
    collect,
    inspect,
)
from historical_discovery import HistoricalDiscoveryError
from historical_store import HistoricalStoreConfig


def _request(cik, name, symbol):
    return {
        "cik": cik,
        "name": name,
        "filing_from": "2024-01-01",
        "filing_to": "2025-01-02",
        "url": f"https://data.sec.gov/submissions/{name}",
        "shared_cache_relative_path": f"submissions/files/{name}",
        "target_response_relative_path": f"supplemental/{name}",
        "selection_disposition": "SELECTED_WINDOW_OVERLAP",
        "matching_windows": [
            {
                "date": "2025-01-02",
                "instrument_id": f"FIGI:{symbol}",
                "primary_exchange": "XNAS",
                "rank": 1,
                "symbol": symbol,
                "start": "2024-12-29",
                "end": "2025-01-02",
            }
        ],
    }


class FakeClient:
    def __init__(self, responses):
        self.responses = dict(responses)
        self.calls = []

    def json(self, url, path):
        self.calls.append(url)
        value = self.responses[url]
        if isinstance(value, Exception):
            raise value
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(value, sort_keys=True), encoding="utf-8")
        return value


class DevelopmentSecSupplementalCollectionTests(unittest.TestCase):
    def _payload(self):
        return {
            "accessionNumber": [
                "0000000001-25-000001",
                "0000000001-25-000002",
                "0000000001-25-000003",
            ],
            "acceptanceDateTime": [
                "2025-01-02T09:00:00-05:00",
                "2025-01-02T10:00:00-05:00",
                None,
            ],
            "filingDate": ["2025-01-02", "2025-01-02", "2025-01-02"],
            "form": ["8-K", "8-K", "6-K"],
            "items": ["2.02", "8.01", ""],
            "primaryDocument": ["good.htm", "late.htm", "date-only.htm"],
            "reportDate": ["2025-01-01", "2025-01-02", "2025-01-02"],
        }

    def _surface(self, root):
        requests = [
            _request("0000000001", "one.json", "AAA"),
            _request("0000000002", "two.json", "BBB"),
        ]
        manifest = {
            "manifest_sha256": "m" * 64,
            "request_contract": {"workers": 2},
        }
        private = {
            "selected_requests": requests,
            "selected_request_graph_sha256": _sha256_json(requests),
        }
        config = HistoricalStoreConfig(root=root, min_free_bytes=MINIMUM_RESERVE_BYTES)
        publication = {"collector": {"commit": "collector-commit"}}
        return manifest, config, private, publication

    def test_top_level_columnar_derivation_enforces_precise_cutoff(self):
        request = _request("0000000001", "one.json", "AAA")
        derived = _derive_response(request=request, payload=self._payload())
        self.assertEqual(derived["source_row_count"], 3)
        self.assertEqual(len(derived["candidate_filings"]), 1)
        self.assertEqual(len(derived["pair_filing_joins"]), 1)
        self.assertEqual(
            derived["diagnostics"]["relevant_form_missing_precise_acceptance"], 1
        )
        self.assertNotIn("late.htm", json.dumps(derived))

    def test_collection_is_failure_isolated_resumable_and_inspectable(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest, config, private, publication = self._surface(root)
            first, second = private["selected_requests"]
            client = FakeClient(
                {
                    first["url"]: self._payload(),
                    second["url"]: HistoricalDiscoveryError("provider failure"),
                }
            )
            status_path = root / "status.json"
            inspection_path = root / "inspection.json"
            with patch(
                "development_sec_supplemental_collection._load_contract",
                return_value=(manifest, config, private, publication),
            ):
                status = collect(
                    status_path=status_path,
                    client=client,
                    require_published=False,
                )
                self.assertEqual(status["status"], "SUPPLEMENTAL_COLLECTION_COMPLETE")
                self.assertEqual(status["counts"]["successful_requests"], 1)
                self.assertEqual(status["counts"]["failed_requests"], 1)
                self.assertEqual(status["unique_candidate_document_count"], 1)
                self.assertTrue(_wrapper_path(root, first).exists())
                self.assertTrue(_wrapper_path(root, second).exists())
                self.assertTrue(_index_path(root).exists())

                no_calls = FakeClient({})
                resumed = collect(
                    status_path=status_path,
                    client=no_calls,
                    require_published=False,
                )
                self.assertEqual(resumed, status)
                self.assertEqual(no_calls.calls, [])
                inspection = inspect(
                    status_path=status_path,
                    output_path=inspection_path,
                    require_published=False,
                )
            self.assertTrue(inspection["valid"])
            self.assertEqual(inspection["source_wrappers_rebuilt"], 2)
            self.assertEqual(inspection["failed_sources_reconciled"], 1)
            self.assertFalse(inspection["primary_documents_requested"])
            self.assertFalse(inspection["target_outcomes_observed_or_derived"])
            self.assertNotIn("AAA", json.dumps(status))
            self.assertNotIn("0000000001", json.dumps(status))

    def test_malformed_payload_failure_does_not_abort_other_request(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest, config, private, publication = self._surface(root)
            first, second = private["selected_requests"]
            client = FakeClient(
                {
                    first["url"]: {"form": ["8-K"], "acceptanceDateTime": ["bad"]},
                    second["url"]: {},
                }
            )
            with patch(
                "development_sec_supplemental_collection._load_contract",
                return_value=(manifest, config, private, publication),
            ):
                status = collect(
                    status_path=root / "status.json",
                    client=client,
                    require_published=False,
                )
            self.assertEqual(status["counts"]["terminal_requests"], 2)
            self.assertEqual(status["counts"]["successful_requests"], 2)
            self.assertEqual(status["counts"]["candidate_filings"], 0)


if __name__ == "__main__":
    unittest.main()

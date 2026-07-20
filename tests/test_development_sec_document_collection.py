import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import development_sec_sources as source_contract
from development_sec_document_collection import (
    DevelopmentSecDocumentCollectionError,
    _index_path,
    _sha256_json,
    _transport_integrity,
    _wrapper_path,
    collect,
    inspect,
)
from development_sec_sources import MINIMUM_RESERVE_BYTES
from historical_discovery import HistoricalDiscoveryError
from historical_store import HistoricalStoreConfig


def _request(cik, accession, symbol):
    url = (
        "https://www.sec.gov/Archives/edgar/data/"
        f"{int(cik)}/{accession.replace('-', '')}/event.htm"
    )
    digest = hashlib.sha256(url.encode()).hexdigest()
    return {
        "cik": cik,
        "form": "8-K",
        "accession": accession,
        "accepted_at": "2025-01-02T09:00:00-05:00",
        "filing_date": "2025-01-02",
        "report_date": "2025-01-01",
        "items": ["2.02"],
        "primary_document": "event.htm",
        "source_url": url,
        "source_origins": ["MAIN_SUBMISSIONS"],
        "shared_cache_relative_path": f"documents/{digest}.source",
        "target_response_relative_path": f"documents/{digest}.json.gz",
        "test_symbol": symbol,
    }


class FakeClient:
    def __init__(self, responses):
        self.responses = dict(responses)
        self.calls = []

    def text(self, url, path):
        self.calls.append(url)
        value = self.responses[url]
        if isinstance(value, Exception):
            raise value
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(value)
        return value.decode("utf-8", errors="replace")


class DevelopmentSecDocumentCollectionTests(unittest.TestCase):
    def _surface(self, root):
        requests = [
            _request("0000000001", "0000000001-25-000001", "AAA"),
            _request("0000000002", "0000000002-25-000001", "BBB"),
        ]
        joins = [{"join": 1}, {"join": 2}]
        manifest = {
            "manifest_sha256": "m" * 64,
            "request_contract": {"workers": 2},
        }
        private = {
            "document_requests": requests,
            "document_request_graph_sha256": _sha256_json(requests),
            "pair_document_joins": joins,
            "pair_document_join_graph_sha256": _sha256_json(joins),
        }
        config = HistoricalStoreConfig(root=root, min_free_bytes=MINIMUM_RESERVE_BYTES)
        publication = {"collector": {"commit": "collector-commit"}}
        return manifest, config, private, publication

    def test_transport_integrity_rejects_empty_and_sec_denial_pages(self):
        with self.assertRaisesRegex(
            DevelopmentSecDocumentCollectionError, "empty"
        ):
            _transport_integrity(b"  \n")
        with self.assertRaisesRegex(
            DevelopmentSecDocumentCollectionError, "automated-access denial"
        ):
            _transport_integrity(
                b"Your Request Originates from an Undeclared Automated Tool"
            )

    def test_collection_is_failure_isolated_resumable_and_inspectable(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest, config, private, publication = self._surface(root)
            first, second = private["document_requests"]
            client = FakeClient(
                {
                    first["source_url"]: b"<html>issuer filing</html>",
                    second["source_url"]: HistoricalDiscoveryError(
                        "provider failure"
                    ),
                }
            )
            status_path = root / "status.json"
            inspection_path = root / "inspection.json"
            with patch(
                "development_sec_document_collection._load_contract",
                return_value=(manifest, config, private, publication),
            ):
                status = collect(
                    status_path=status_path,
                    client=client,
                    require_published=False,
                )
                self.assertEqual(status["status"], "DOCUMENT_COLLECTION_COMPLETE")
                self.assertEqual(status["counts"]["successful_requests"], 1)
                self.assertEqual(status["counts"]["failed_requests"], 1)
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
            self.assertFalse(inspection["source_semantics_observed_or_derived"])
            self.assertFalse(inspection["target_outcomes_observed_or_derived"])
            self.assertNotIn("AAA", json.dumps(status))
            self.assertNotIn("0000000001", json.dumps(status))
            self.assertNotIn("issuer filing", json.dumps(status))

    def test_tampered_raw_source_fails_independent_rehash(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest, config, private, publication = self._surface(root)
            responses = {
                request["source_url"]: b"<html>source</html>"
                for request in private["document_requests"]
            }
            with patch(
                "development_sec_document_collection._load_contract",
                return_value=(manifest, config, private, publication),
            ):
                collect(
                    status_path=root / "status.json",
                    client=FakeClient(responses),
                    require_published=False,
                )
                first = private["document_requests"][0]
                shared = (
                    root
                    / source_contract.SHARED_SEC_CACHE_NAMESPACE
                    / first["shared_cache_relative_path"]
                )
                shared.write_bytes(b"tampered")
                with self.assertRaisesRegex(
                    DevelopmentSecDocumentCollectionError, "raw bytes drifted"
                ):
                    inspect(
                        status_path=root / "status.json",
                        output_path=root / "inspection.json",
                        require_published=False,
                    )


if __name__ == "__main__":
    unittest.main()

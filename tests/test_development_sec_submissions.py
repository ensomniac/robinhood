import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from development_sec_sources import MINIMUM_RESERVE_BYTES
from development_sec_submissions import (
    DevelopmentSecSubmissionsError,
    _collection_index_path,
    _derive_response,
    _load_contract,
    _sha256_json,
    _submissions_root,
    _wrapper_path,
    collect_submissions,
    inspect_submissions,
)
from historical_discovery import HistoricalDiscoveryError
from historical_store import HistoricalStoreConfig


def _recent(rows):
    fields = {
        "accessionNumber",
        "acceptanceDateTime",
        "filingDate",
        "form",
        "items",
        "primaryDocument",
        "reportDate",
    }
    return {field: [row.get(field) for row in rows] for field in fields}


def _pair(cik, symbol, rank):
    return {
        "date": "2025-01-02",
        "instrument_id": f"FIGI:{symbol}",
        "primary_exchange": "XNAS",
        "rank": rank,
        "symbol": symbol,
        "cik": cik,
        "identity_disposition": "SEC_SUBMISSIONS_READY" if cik else "CIK_MISSING",
        "window_start_et": "2024-12-29T00:00:00-05:00",
        "window_cutoff_et": "2025-01-02T09:35:00-05:00",
    }


class FakeSecClient:
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


class DevelopmentSecSubmissionTests(unittest.TestCase):
    def _surface(self, root):
        cik_one = "0000000001"
        cik_two = "0000000002"
        requests = [
            {
                "cik": cik,
                "url": f"https://data.sec.gov/submissions/CIK{cik}.json",
                "shared_cache_relative_path": f"submissions/CIK{cik}.json",
                "target_response_relative_path": f"submissions/CIK{cik}.json",
            }
            for cik in (cik_one, cik_two)
        ]
        private = {
            "schema_version": 1,
            "dataset_id": "dataset-development-sec-primary-sources-2026-07-19-v2",
            "selected_pair_count": 3,
            "mapped_pair_count": 3,
            "cik_present_pair_count": 2,
            "cik_missing_pair_count": 1,
            "unique_cik_count": 2,
            "listing_scoped_pair_count": 0,
            "pairs": [
                _pair(cik_one, "AAA", 1),
                _pair(cik_two, "BBB", 2),
                _pair(None, "CCC", 3),
            ],
            "submission_requests": requests,
            "submission_request_graph_sha256": _sha256_json(requests),
            "daily_identity_aggregates": [],
            "daily_identity_aggregates_sha256": _sha256_json([]),
        }
        manifest = {
            "manifest_sha256": "m" * 64,
            "request_contract": {"workers": 2},
        }
        config = HistoricalStoreConfig(root=root, min_free_bytes=MINIMUM_RESERVE_BYTES)
        publication = {"collector": {"commit": "commit-one"}}
        return manifest, config, private, publication

    def _payload(self):
        return {
            "cik": "1",
            "filings": {
                "recent": _recent(
                    [
                        {
                            "accessionNumber": "0000000001-25-000001",
                            "acceptanceDateTime": "2025-01-02T09:00:00-05:00",
                            "filingDate": "2025-01-02",
                            "form": "8-K",
                            "items": "2.02",
                            "primaryDocument": "good.htm",
                            "reportDate": "2025-01-01",
                        },
                        {
                            "accessionNumber": "0000000001-25-000002",
                            "acceptanceDateTime": "2025-01-02T10:00:00-05:00",
                            "filingDate": "2025-01-02",
                            "form": "8-K",
                            "items": "8.01",
                            "primaryDocument": "late.htm",
                            "reportDate": "2025-01-02",
                        },
                        {
                            "accessionNumber": "0000000001-25-000003",
                            "acceptanceDateTime": None,
                            "filingDate": "2025-01-02",
                            "form": "6-K",
                            "items": "",
                            "primaryDocument": "date-only.htm",
                            "reportDate": "2025-01-02",
                        },
                    ]
                ),
                "files": [
                    {
                        "name": "CIK0000000001-submissions-001.json",
                        "filingFrom": "2020-01-01",
                        "filingTo": "2024-12-31",
                    }
                ],
            },
        }

    def test_derivation_enforces_precise_cutoff_and_only_derives_requests(self):
        derived = _derive_response(
            cik="0000000001",
            payload=self._payload(),
            pairs=[_pair("0000000001", "AAA", 1)],
        )
        self.assertEqual(len(derived["candidate_filings"]), 1)
        self.assertEqual(len(derived["pair_filing_joins"]), 1)
        self.assertEqual(len(derived["supplemental_submission_requests"]), 1)
        self.assertEqual(
            derived["diagnostics"]["relevant_form_missing_precise_acceptance"], 1
        )
        self.assertNotIn("late.htm", json.dumps(derived))

    def test_collection_captures_failures_resumes_and_independently_inspects(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest, config, private, publication = self._surface(root)
            first_url = private["submission_requests"][0]["url"]
            second_url = private["submission_requests"][1]["url"]
            client = FakeSecClient(
                {
                    first_url: self._payload(),
                    second_url: HistoricalDiscoveryError("provider failure"),
                }
            )
            status_path = root / "status.json"
            inspection_path = root / "inspection.json"
            with patch(
                "development_sec_submissions._load_contract",
                return_value=(manifest, config, private, publication),
            ):
                status = collect_submissions(
                    public_status_path=status_path,
                    client=client,
                    require_published=False,
                )
                self.assertEqual(status["status"], "SUBMISSIONS_COLLECTION_COMPLETE")
                self.assertEqual(status["counts"]["successful_requests"], 1)
                self.assertEqual(status["counts"]["failed_requests"], 1)
                self.assertEqual(status["counts"]["missing_cik_pairs"], 1)
                self.assertEqual(status["unique_candidate_document_count"], 1)
                self.assertEqual(status["unique_supplemental_request_count"], 1)
                self.assertTrue(_wrapper_path(root, "0000000001").exists())
                self.assertTrue(_wrapper_path(root, "0000000002").exists())
                self.assertTrue(_collection_index_path(root).exists())

                no_calls = FakeSecClient({})
                resumed = collect_submissions(
                    public_status_path=status_path,
                    client=no_calls,
                    require_published=False,
                )
                self.assertEqual(resumed, status)
                self.assertEqual(no_calls.calls, [])
                inspection = inspect_submissions(
                    public_status_path=status_path,
                    inspection_path=inspection_path,
                    require_published=False,
                )
                self.assertTrue(inspection["valid"])
                self.assertEqual(inspection["source_wrappers_rebuilt"], 2)
                self.assertEqual(inspection["failed_sources_reconciled"], 1)
                self.assertFalse(inspection["primary_documents_requested"])
                self.assertFalse(inspection["target_outcomes_observed_or_derived"])
                self.assertNotIn("AAA", json.dumps(status))
                self.assertNotIn("0000000001", json.dumps(status))

    def test_payload_identity_failure_is_terminal_and_does_not_abort_other_ciks(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest, config, private, publication = self._surface(root)
            bad = self._payload()
            bad["cik"] = "999"
            second = {"cik": "2", "filings": {"recent": {}, "files": []}}
            client = FakeSecClient(
                {
                    private["submission_requests"][0]["url"]: bad,
                    private["submission_requests"][1]["url"]: second,
                }
            )
            with patch(
                "development_sec_submissions._load_contract",
                return_value=(manifest, config, private, publication),
            ):
                status = collect_submissions(
                    public_status_path=root / "status.json",
                    client=client,
                    require_published=False,
                )
            self.assertEqual(status["counts"]["failed_requests"], 1)
            self.assertEqual(status["counts"]["successful_requests"], 1)
            self.assertEqual(status["counts"]["terminal_requests"], 2)

    def test_explicit_dataset_scopes_every_private_artifact(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            manifest, config, private, publication = self._surface(root)
            dataset_id = "dataset-development-sec-primary-sources-test-v3"
            private["dataset_id"] = dataset_id
            client = FakeSecClient(
                {
                    private["submission_requests"][0]["url"]: self._payload(),
                    private["submission_requests"][1]["url"]: {
                        "cik": "2",
                        "filings": {"recent": {}, "files": []},
                    },
                }
            )
            status_path = root / "status.json"
            inspection_path = root / "inspection.json"
            with patch(
                "development_sec_submissions._load_contract",
                return_value=(manifest, config, private, publication),
            ):
                status = collect_submissions(
                    dataset_id=dataset_id,
                    public_status_path=status_path,
                    client=client,
                    require_published=False,
                )
                inspection = inspect_submissions(
                    dataset_id=dataset_id,
                    public_status_path=status_path,
                    inspection_path=inspection_path,
                    require_published=False,
                )
            self.assertEqual(status["dataset_id"], dataset_id)
            self.assertEqual(inspection["dataset_id"], dataset_id)
            self.assertTrue(_collection_index_path(root, dataset_id).is_file())
            self.assertTrue(_wrapper_path(root, "0000000001", dataset_id).is_file())
            self.assertFalse(_collection_index_path(root).exists())

    def test_unsafe_dataset_id_is_rejected_before_private_path_use(self):
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(
                DevelopmentSecSubmissionsError, "dataset ID is unsafe"
            ):
                _submissions_root(Path(directory), "../cross-tranche")

    def test_manifest_dataset_must_match_explicit_dataset(self):
        with patch(
            "development_sec_submissions.load_frozen_dataset_contract",
            return_value={"dataset_id": "dataset-other"},
        ):
            with self.assertRaisesRegex(
                DevelopmentSecSubmissionsError, "unexpected SEC source dataset"
            ):
                _load_contract(
                    dataset_id="dataset-expected",
                    manifest_path=Path("unused.json"),
                    env_path=Path("unused.env"),
                    require_published=False,
                )


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import catalyst_source_recovery as csr


class AccessionRecoveryUrlTests(unittest.TestCase):
    def test_index_becomes_complete_submission_text(self) -> None:
        value = csr.accession_recovery_url(
            "https://www.sec.gov/Archives/edgar/data/1067983/"
            "000119312526054580/0001193125-26-054580-index.html"
        )
        self.assertEqual(value["accession"], "000119312526054580")
        self.assertEqual(value["filing_cik"], "1067983")
        self.assertEqual(
            value["recovery_url"],
            "https://www.sec.gov/Archives/edgar/data/1067983/"
            "000119312526054580/0001193125-26-054580.txt",
        )

    def test_inline_xbrl_wrapper_becomes_direct_archive_document(self) -> None:
        value = csr.accession_recovery_url(
            "https://www.sec.gov/ix?doc=/Archives/edgar/data/0001045810/"
            "000104581026000052/nvda-20260426.htm"
        )
        self.assertEqual(
            value["recovery_url"],
            "https://www.sec.gov/Archives/edgar/data/1045810/"
            "000104581026000052/nvda-20260426.htm",
        )

    def test_xsl_document_path_is_retained(self) -> None:
        value = csr.accession_recovery_url(
            "https://www.sec.gov/Archives/edgar/data/886982/"
            "000088698226000274/xslForm13F_X02/InfoTable.xml"
        )
        self.assertTrue(
            str(value["recovery_url"]).endswith("/xslForm13F_X02/InfoTable.xml")
        )

    def test_repeated_malformed_archive_path_uses_last_complete_path(self) -> None:
        value = csr.accession_recovery_url(
            "https://www.sec.gov/Archives/edgar/data/1835632/"
            "000162828026044905/Archives/edgar/data/1835632/"
            "000162828026044905/0001628280-26-044905-index.htm"
        )
        self.assertEqual(
            value["recovery_url"],
            "https://www.sec.gov/Archives/edgar/data/1835632/"
            "000162828026044905/0001628280-26-044905.txt",
        )

    def test_generic_browse_page_has_no_recovery_endpoint(self) -> None:
        value = csr.accession_recovery_url(
            "https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=SPCE"
        )
        self.assertIsNone(value["accession"])
        self.assertIsNone(value["recovery_url"])

    def test_non_sec_target_fails(self) -> None:
        with self.assertRaises(csr.CatalystSourceRecoveryError):
            csr.accession_recovery_url(
                "https://example.com/Archives/edgar/data/1/000000000000000001/a.htm"
            )

    def test_conflicting_accessions_fail(self) -> None:
        with self.assertRaises(csr.CatalystSourceRecoveryError):
            csr.accession_from_url(
                "https://www.sec.gov/Archives/edgar/data/1/"
                "000000000000000001/000000000000000002/a.htm"
            )


class CollectionContractTests(unittest.TestCase):
    def test_public_summary_contains_no_private_rows(self) -> None:
        manifest = {"manifest_sha256": "a" * 64}
        selection = {
            "counts": {
                "sec_403_urls": 26,
                "accession_bound_urls": 24,
                "non_accession_urls": 2,
                "pair_source_joins": 37,
                "pairs": 31,
            }
        }
        index = {
            "status": "COLLECTION_COMPLETE",
            "records": {
                "private": {"status": "NO_ACCESSION"},
                "private2": {
                    "status": "RESPONSE_CAPTURED",
                    "http_status": 200,
                    "body_bytes": 5,
                },
            },
        }
        value = csr._public_summary(manifest, selection, index)
        encoded = json.dumps(value)
        self.assertNotIn("private2", encoded)
        self.assertFalse(value["target_outcomes_observed_or_derived"])

    def test_validate_target_rejects_noncanonical_sec_host(self) -> None:
        with self.assertRaises(csr.CatalystSourceRecoveryError):
            csr._validate_sec_target(
                "https://data.sec.gov/Archives/edgar/data/1/000000000000000001/a.htm"
            )

    def test_read_response_enforces_byte_limit(self) -> None:
        response = mock.Mock()
        response.headers = {"Content-Length": str(csr.MAX_RESPONSE_BYTES + 1)}
        with self.assertRaises(csr.CatalystSourceRecoveryError):
            csr._read_response_bytes(response)

    def test_inspect_rejects_incomplete_checkpoint(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            index_path = root / "capture-index.json"
            index_path.write_text(
                json.dumps(
                    {
                        "manifest_sha256": "b" * 64,
                        "status": "COLLECTING",
                        "records": {},
                        "target_outcomes_observed_or_derived": False,
                    }
                )
            )
            store = mock.Mock(root=root)
            with (
                mock.patch.object(
                    csr.HistoricalDayStore, "from_env", return_value=store
                ),
                mock.patch.object(
                    csr,
                    "_verify_contract",
                    return_value=(
                        {"manifest_sha256": "b" * 64},
                        {"records": [], "counts": {}},
                    ),
                ),
                mock.patch.object(csr, "_index_path", return_value=index_path),
            ):
                with self.assertRaises(csr.CatalystSourceRecoveryError):
                    csr.inspect(
                        manifest_path=root / "manifest.json",
                        env_path=root / ".env",
                        public_result_path=root / "result.json",
                    )

    def test_collection_user_agent_is_declared_and_paced(self) -> None:
        self.assertIn("ryan@ensomniac.com", csr.USER_AGENT)
        self.assertGreaterEqual(csr.MINIMUM_INTERVAL_SECONDS, 0.1)
        self.assertEqual(csr.EXPECTED_ACCESSION_URLS, 24)


if __name__ == "__main__":
    unittest.main()

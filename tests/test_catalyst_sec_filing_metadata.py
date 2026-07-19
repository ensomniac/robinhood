from __future__ import annotations

import unittest

import catalyst_sec_filing_metadata as metadata


class FilingMetadataTests(unittest.TestCase):
    def test_filing_detail_url_is_accession_bound(self) -> None:
        value = metadata.filing_detail_url("1045810", "000104581026000052")
        self.assertEqual(
            value,
            "https://www.sec.gov/Archives/edgar/data/1045810/"
            "000104581026000052/0001045810-26-000052-index.html",
        )

    def test_invalid_accession_fails(self) -> None:
        with self.assertRaises(metadata.SecFilingMetadataError):
            metadata.filing_detail_url("1045810", "bad")

    def test_extracts_visible_accepted_datetime_and_cik(self) -> None:
        body = b"""
        <html><body><div class='infoHead'>Accepted</div>
        <div class='info'>2026-05-20 16:31:02</div>
        <span class='companyName'>Example (Filer) CIK: 0001045810</span>
        </body></html>
        """
        value = metadata.extract_filing_metadata(body)
        self.assertEqual(
            value["accepted_datetime_candidates_et"], ["2026-05-20T16:31:02"]
        )
        self.assertEqual(value["cik_candidates"], ["1045810"])

    def test_metadata_extraction_does_not_invent_missing_fields(self) -> None:
        value = metadata.extract_filing_metadata(b"<html><body>none</body></html>")
        self.assertEqual(value["accepted_datetime_candidates_et"], [])
        self.assertEqual(value["cik_candidates"], [])

    def test_selection_excludes_non_accession_rows(self) -> None:
        source = {
            "records": [
                {
                    "source_sha256": str(index),
                    "accession": f"{index:018d}" if index < 24 else None,
                    "filing_cik": "1" if index < 24 else None,
                    "pairs": [
                        {
                            "date": f"2026-01-{(index % 28) + 1:02d}",
                            "symbol": f"T{index:02d}",
                        }
                    ],
                }
                for index in range(26)
            ]
        }
        with self.assertRaises(metadata.SecFilingMetadataError):
            metadata.build_selection(source)


if __name__ == "__main__":
    unittest.main()

import hashlib
import json
import tempfile
import unittest
from pathlib import Path

import development_catalyst_source_semantics as semantics


def _pair(index):
    return {
        "date": f"2025-01-{index + 2:02d}",
        "instrument_id": f"FIGI:{index}",
        "primary_exchange": "XNAS",
        "rank": index + 1,
        "symbol": f"T{index}",
        "cik": f"{index + 1:010d}",
    }


def _request(index):
    pair = _pair(index)
    accession = f"{index + 1:010d}-25-000001"
    url = (
        "https://www.sec.gov/Archives/edgar/data/"
        f"{index + 1}/{accession.replace('-', '')}/event.htm"
    )
    digest = hashlib.sha256(url.encode()).hexdigest()
    return {
        "cik": pair["cik"],
        "form": "8-K",
        "accession": accession,
        "accepted_at": f"{pair['date']}T09:00:00-05:00",
        "filing_date": pair["date"],
        "report_date": pair["date"],
        "items": ["2.02"],
        "primary_document": "event.htm",
        "source_url": url,
        "source_origins": ["MAIN_SUBMISSIONS"],
        "shared_cache_relative_path": f"documents/{digest}.source",
        "target_response_relative_path": f"documents/{digest}.json.gz",
    }


def _join(pair, request):
    return {
        **{field: pair[field] for field in semantics.PAIR_FIELDS},
        "source_origin": "MAIN_SUBMISSIONS",
        "filing": {
            key: request[key]
            for key in (
                "form",
                "accession",
                "accepted_at",
                "filing_date",
                "report_date",
                "items",
                "primary_document",
                "source_url",
            )
        },
    }


def _selection_surface():
    pairs = [_pair(index) for index in range(3)]
    requests = [_request(index) for index in range(2)]
    joins = [_join(pairs[index], requests[index]) for index in range(2)]
    records = []
    for request in requests:
        records.append(
            {
                "request_sha256": semantics._sha256_json(request),
                "status": "SUCCESS",
                "source_bytes": 100,
                "source_sha256": f"{len(records) + 1:064d}",
            }
        )
    source_manifest = {"selection_contract": {"selected_pair_count": 3}}
    document_private = {
        "counts": {
            "pair_document_joins": 2,
            "unique_pairs_with_document": 2,
        },
        "document_requests": requests,
        "pair_document_joins": joins,
    }
    document_index = {
        "counts": {"successful_requests": 2},
        "request_records": records,
    }
    identities = {"pairs": pairs}
    return source_manifest, document_private, document_index, identities


def _extraction_row(text, *, row_id="row-1", date="2025-01-02"):
    return {
        "row_id": row_id,
        "date": date,
        "instrument_id": "FIGI:1",
        "primary_exchange": "XNAS",
        "rank": 1,
        "symbol": "TEST",
        "cik": "0000000001",
        "source_type": "SEC",
        "accepted_at": f"{date}T09:00:00-05:00",
        "extraction": {
            "capture_status": "RESPONSE_CAPTURED",
            "http_status": 200,
            "text": text,
            "financing_or_dilution_terms_present": bool(
                semantics.shared_semantics.FINANCING_PATTERN.search(text)
            ),
            "diagnostic_failures": [],
        },
    }


class DevelopmentCatalystSourceSemanticsTests(unittest.TestCase):
    def test_selection_preserves_complete_pair_denominator_and_private_rows(self):
        values = _selection_surface()
        result = semantics.build_selection(
            source_manifest=values[0],
            document_private=values[1],
            document_index=values[2],
            identities=values[3],
        )
        self.assertEqual(
            result["counts"],
            {
                "selected_pairs": 3,
                "source_documents": 2,
                "pair_source_joins": 2,
                "pairs_with_source": 2,
                "pairs_without_source": 1,
            },
        )
        self.assertEqual(len(result["rows"]), 2)
        self.assertEqual(len(result["no_source_pairs"]), 1)
        self.assertFalse(result["target_outcomes_observed_or_derived"])

    def test_selection_rejects_cik_or_join_drift(self):
        values = list(_selection_surface())
        values[1]["document_requests"][0]["cik"] = "9999999999"
        with self.assertRaisesRegex(
            semantics.DevelopmentCatalystSourceSemanticsError,
            "request identity|source identity differs",
        ):
            semantics.build_selection(
                source_manifest=values[0],
                document_private=values[1],
                document_index=values[2],
                identities=values[3],
            )

    def test_html_extraction_ignores_script_and_detects_financing(self):
        raw = (
            b"<html><head><title>Issuer Update</title>"
            b"<style>.hidden{color:red}</style></head>"
            b"<body>Public offering announced<script>future_price=99</script>"
            b"</body></html>"
        )
        result = semantics._extract_document(raw)
        self.assertEqual(result["title"], "Issuer Update")
        self.assertIn("Public offering announced", result["text"])
        self.assertNotIn("future_price", result["text"])
        self.assertNotIn("color:red", result["text"])
        self.assertTrue(result["financing_or_dilution_terms_present"])

    def test_financing_precedes_positive_and_positive_negative_becomes_mixed(self):
        financing = _extraction_row(
            "Record revenue increased 25%. The company announced a public offering."
        )
        decision = semantics._automatic_decision(financing)
        self.assertEqual(decision["semantics"], "FINANCING_OR_DILUTION_CONFLICT")
        self.assertTrue(decision["financing_or_dilution_conflict"])

        mixed = _extraction_row(
            "Record revenue increased 25%, but the company lowered full-year guidance."
        )
        decision = semantics._automatic_decision(mixed)
        self.assertEqual(decision["semantics"], "MIXED_OR_CONTRADICTORY")

    def test_official_sec_acceptance_is_validated_against_cutoff(self):
        row = _extraction_row("The company was awarded a material contract.")
        decision = semantics._automatic_decision(row)
        validated, timestamp = (
            semantics.shared_semantics.validate_review_decision(decision, row=row)
        )
        terminal, _ = semantics.shared_semantics.terminal_disposition(
            row, validated, timestamp
        )
        self.assertEqual(timestamp["status"], "ACCEPTED")
        self.assertEqual(terminal, "VERIFIED_POSITIVE_PRIMARY")

    def test_review_reconciles_all_pairs_and_conflict_overrides_positive(self):
        positive = _extraction_row(
            "The company was awarded a material contract.", row_id="row-positive"
        )
        conflict = _extraction_row(
            "The company announced a public offering.", row_id="row-conflict"
        )
        conflict["instrument_id"] = positive["instrument_id"]
        extraction = {
            "manifest_sha256": "m" * 64,
            "counts": {
                "selected_pairs": 2,
                "source_documents": 2,
                "pair_source_joins": 2,
                "pairs_with_source": 1,
                "pairs_without_source": 1,
            },
            "rows": [positive, conflict],
            "no_source_pairs": [
                {
                    "date": "2025-01-03",
                    "instrument_id": "FIGI:2",
                    "symbol": "NONE",
                }
            ],
        }
        result = semantics.build_review(extraction)
        self.assertEqual(sum(result["terminal_join_counts"].values()), 2)
        self.assertEqual(sum(result["terminal_pair_counts"].values()), 2)
        self.assertEqual(result["terminal_pair_counts"]["VERIFIED_CONFLICT"], 1)
        self.assertEqual(
            result["terminal_pair_counts"]["SOURCE_OWNERSHIP_UNRESOLVED"], 1
        )
        self.assertEqual(result["verified_positive_pairs"], 0)
        self.assertEqual(result["next_phase"], "SOURCE_RECOVERY")
        self.assertFalse(result["outcome_contract_permitted"])

    def test_public_result_is_aggregate_only(self):
        reviewed = {
            "counts": {
                "selected_pairs": 3,
                "source_documents": 2,
                "pair_source_joins": 2,
                "pairs_with_source": 2,
                "pairs_without_source": 1,
            },
            "terminal_join_counts": {
                key: 0 for key in semantics.shared_semantics.TERMINAL_PRECEDENCE
            },
            "terminal_pair_counts": {
                key: 0 for key in semantics.shared_semantics.TERMINAL_PRECEDENCE
            },
            "verified_positive_pairs": 0,
            "next_phase": "SOURCE_RECOVERY",
        }
        reviewed["terminal_join_counts"]["DOCUMENT_SEMANTICS_UNRESOLVED"] = 2
        reviewed["terminal_pair_counts"]["DOCUMENT_SEMANTICS_UNRESOLVED"] = 2
        reviewed["terminal_pair_counts"]["SOURCE_OWNERSHIP_UNRESOLVED"] = 1
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "private.gz"
            path.write_bytes(b"private")
            result = semantics._public_result(
                manifest={"manifest_sha256": "m" * 64},
                reviewed=reviewed,
                private_path=path,
            )
        rendered = json.dumps(result).lower()
        self.assertNotIn("https://", rendered)
        self.assertNotIn("test", rendered)
        self.assertTrue(result["inspection"]["complete_pair_denominator_reconciled"])
        self.assertFalse(result["outcome_contract_permitted"])


if __name__ == "__main__":
    unittest.main()

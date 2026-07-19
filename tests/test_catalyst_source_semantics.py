from __future__ import annotations

import tempfile
import unittest
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import catalyst_primary_source_capture as capture
import catalyst_source_semantics as semantics


class CatalystSourceSemanticsTests(unittest.TestCase):
    def _row(self, **overrides):
        value = {
            "row_id": "join-test",
            "date": "2026-01-15",
            "symbol": "TEST",
            "instrument_id": "instrument-test",
            "primary_exchange": "NASDAQ",
            "cik": "123456",
            "issuer_name": "Test Corporation",
            "identity_match_basis": "exact_instrument_ticker_exchange",
            "source_hash": "body-hash",
            "url_sha256": "url-hash",
            "url": "https://ir.test.example/release",
            "final_url": "https://ir.test.example/release",
            "source_type": "ISSUER_HOST",
            "route_category": "POTENTIAL_ISSUER_HOST",
            "decision_cutoff_et": "2026-01-15T09:35:00-05:00",
            "extraction": {
                "capture_status": "RESPONSE_CAPTURED",
                "http_status": 200,
                "diagnostic_failures": [],
                "financing_or_dilution_terms_present": False,
            },
        }
        value.update(overrides)
        return value

    def _decision(self, **overrides):
        value = {
            "row_id": "join-test",
            "source_ownership": "VERIFIED",
            "ownership_evidence": ["issuer-controlled investor relations page"],
            "issuer_binding": "VERIFIED",
            "issuer_binding_methods": [
                "legal_company_name",
                "point_in_time_ticker",
            ],
            "relevance": "RELEVANT",
            "published_at_utc": "2026-01-15T12:00:00+00:00",
            "published_date": None,
            "source_timezone": "America/New_York",
            "timestamp_precision": "DATETIME",
            "timestamp_evidence_kind": "SOURCE_CONTROLLED_EXPLICIT_DATETIME",
            "timestamp_conflict": False,
            "semantics": "DIRECT_POSITIVE",
            "semantic_evidence": ["material issuer contract award"],
            "financing_or_dilution_conflict": False,
            "notes": "",
        }
        value.update(overrides)
        return value

    def _selection_inputs(self):
        pair_records = []
        selection_records = []
        capture_records = {}
        profile_records = {}
        identity_rows = []
        pdf_records = {}
        for index in range(33):
            source_hash = f"source-{index:02d}"
            routes = [source_hash]
            if index < 5:
                routes.append(f"source-{index + 5:02d}")
            pair_records.append(
                {
                    "date": f"2026-01-{index % 28 + 1:02d}",
                    "symbol": f"T{index:02d}",
                    "route_url_hashes": routes,
                }
            )
            selection_records.append(
                {
                    "url_sha256": source_hash,
                    "url": f"https://issuer{index}.example/release",
                    "category": "POTENTIAL_ISSUER_HOST",
                }
            )
            capture_records[source_hash] = {
                "status": "RESPONSE_CAPTURED",
                "http_status": 200,
                "body_sha256": f"body-{index:02d}",
                "final_url": f"https://issuer{index}.example/release",
                "category": "POTENTIAL_ISSUER_HOST",
                "headers": {"content-type": "text/html"},
            }
            profile_records[source_hash] = {
                "html": {
                    "meta_timestamp_candidates": [
                        {"key": "datepublished", "value": "2026-01-01T07:00:00-05:00"}
                    ]
                }
            }
            identity_rows.append(
                {
                    "date": f"2026-01-{index % 28 + 1:02d}",
                    "symbol": f"T{index:02d}",
                    "instrument_id": f"instrument-{index}",
                    "primary_exchange": "NASDAQ",
                    "cik": f"{index + 1}",
                    "issuer_name": f"Test Issuer {index}",
                    "cik_match_basis": "exact_instrument_ticker_exchange",
                }
            )
        capture_records["source-00"]["headers"]["content-type"] = "application/pdf"
        profile_records["source-00"] = {"html": None}
        pdf_records["source-00"] = {"text_present": True}
        return (
            {"pair_records": pair_records},
            {"records": selection_records},
            {"records": capture_records},
            {"records": profile_records},
            {"records": pdf_records},
            {"pairs": identity_rows},
        )

    def test_exact_selection_freezes_33_pairs_documents_and_38_joins(self):
        result = semantics.build_selection(*self._selection_inputs())
        self.assertEqual(
            result["counts"],
            {"pairs": 33, "documents": 33, "pair_source_joins": 38},
        )
        self.assertFalse(result["target_outcomes_observed_or_derived"])
        self.assertNotIn("outcomes", result)

    def test_selection_rejects_identity_or_join_drift(self):
        inputs = list(self._selection_inputs())
        inputs[-1]["pairs"].pop()
        with self.assertRaisesRegex(semantics.CatalystSourceSemanticsError, "identity"):
            semantics.build_selection(*inputs)

    def test_winter_and_summer_cutoffs_use_new_york_timezone(self):
        self.assertTrue(
            semantics._decision_cutoff_text("2026-01-15").endswith("-05:00")
        )
        self.assertTrue(
            semantics._decision_cutoff_text("2026-07-15").endswith("-04:00")
        )

    def test_same_day_date_only_is_rejected(self):
        decision = self._decision(
            published_at_utc=None,
            published_date="2026-01-15",
            timestamp_precision="DATE",
        )
        result = semantics._review_timestamp(decision, "2026-01-15")
        self.assertEqual(result["status"], "SAME_DAY_TIME_UNRESOLVED")
        self.assertIsNone(result["accepted_utc"])

    def test_prior_date_is_conservatively_normalized_and_accepted(self):
        decision = self._decision(
            published_at_utc=None,
            published_date="2026-01-14",
            timestamp_precision="DATE",
        )
        result = semantics._review_timestamp(decision, "2026-01-15")
        self.assertEqual(result["status"], "ACCEPTED")
        self.assertEqual(
            datetime.fromisoformat(result["accepted_utc"]).tzinfo,
            UTC,
        )

    def test_timestamp_conflict_precedes_same_day_resolution(self):
        decision = self._decision(
            published_at_utc=None,
            published_date="2026-01-15",
            timestamp_precision="DATE",
            timestamp_conflict=True,
        )
        result = semantics._review_timestamp(decision, "2026-01-15")
        self.assertEqual(result["status"], "CONFLICT")

    def test_issuer_host_needs_two_binding_methods(self):
        decision = self._decision(issuer_binding_methods=["legal_company_name"])
        with self.assertRaisesRegex(
            semantics.CatalystSourceSemanticsError, "needs 2 methods"
        ):
            semantics.validate_review_decision(decision, row=self._row())

    def test_sec_source_requires_cik_binding(self):
        row = self._row(source_type="SEC")
        decision = self._decision(issuer_binding_methods=["legal_company_name"])
        with self.assertRaisesRegex(
            semantics.CatalystSourceSemanticsError, "exact CIK"
        ):
            semantics.validate_review_decision(decision, row=row)

    def test_pdf_extraction_keeps_metadata_and_date_as_candidates_only(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = capture._response_path(root, "url-hash")
            path.parent.mkdir(parents=True)
            path.write_bytes(b"pdf-bytes")
            record = {
                "status": "RESPONSE_CAPTURED",
                "http_status": 200,
                "body_sha256": capture._sha256_file(path),
                "headers": {"content-type": "application/pdf"},
                "final_url": "https://issuer.example/release.pdf",
            }
            page = SimpleNamespace(
                extract_text=lambda: "PRESS RELEASE January 14, 2026 Test Corporation"
            )
            reader = SimpleNamespace(
                pages=[page], metadata={"/CreationDate": "D:20260114"}
            )
            with patch.object(semantics, "PdfReader", return_value=reader):
                result = semantics._document_extraction(
                    SimpleNamespace(root=root), self._row(), record
                )
        self.assertTrue(result["pdf_metadata"]["creation_date_present"])
        self.assertIn("January 14, 2026", result["pdf_front_text_date_candidates"])
        self.assertNotIn("accepted_timestamp", result)

    def test_terminal_precedence_is_fail_closed(self):
        row = self._row()
        decision = self._decision(
            source_ownership="UNRESOLVED",
            ownership_evidence=[],
            issuer_binding="UNRESOLVED",
            issuer_binding_methods=[],
            relevance="IRRELEVANT",
            published_at_utc=None,
            timestamp_precision="UNRESOLVED",
            timestamp_evidence_kind="UNRESOLVED",
            semantics="UNRESOLVED",
            semantic_evidence=[],
        )
        terminal, _ = semantics.terminal_disposition(
            row, decision, {"status": "MISSING", "accepted_utc": None}
        )
        self.assertEqual(terminal, "SOURCE_OWNERSHIP_UNRESOLVED")

    def test_timestamp_failure_precedes_unresolved_semantics(self):
        decision = self._decision(
            relevance="UNRESOLVED",
            published_at_utc=None,
            timestamp_precision="UNRESOLVED",
            timestamp_evidence_kind="UNRESOLVED",
            semantics="UNRESOLVED",
            semantic_evidence=[],
        )
        terminal, _ = semantics.terminal_disposition(
            self._row(), decision, {"status": "MISSING", "accepted_utc": None}
        )
        self.assertEqual(terminal, "TIMESTAMP_MISSING")

    def test_financing_conflict_precedes_positive_classification(self):
        decision = self._decision(financing_or_dilution_conflict=True)
        terminal, _ = semantics.terminal_disposition(
            self._row(), decision, {"status": "ACCEPTED", "accepted_utc": "x"}
        )
        self.assertEqual(terminal, "VERIFIED_CONFLICT")

    def test_resolved_semantics_require_evidence(self):
        decision = self._decision(semantic_evidence=[])
        with self.assertRaisesRegex(
            semantics.CatalystSourceSemanticsError, "need evidence"
        ):
            semantics.validate_review_decision(decision, row=self._row())

    def test_completed_review_cannot_contain_outcomes(self):
        extraction = {
            "manifest_sha256": "manifest",
            "rows": [self._row()],
        }
        review = {
            "schema_version": 1,
            "dataset_id": semantics.DATASET_ID,
            "manifest_sha256": "manifest",
            "review_completed": True,
            "decisions": [self._decision()],
            "target_outcomes_observed_or_derived": True,
        }
        with self.assertRaisesRegex(
            semantics.CatalystSourceSemanticsError, "outcome lock"
        ):
            semantics._validate_review_input(review, extraction)

    def test_aggregate_rebuild_reconciles_one_terminal_reason_per_join(self):
        rows = []
        decisions = []
        for index in range(38):
            pair_index = index if index < 33 else index - 33
            row = self._row(
                row_id=f"join-{index}",
                date=f"2026-01-{pair_index % 28 + 1:02d}",
                symbol=f"T{pair_index:02d}",
            )
            decision = self._decision(
                row_id=f"join-{index}",
                source_ownership="UNRESOLVED",
                ownership_evidence=[],
                issuer_binding="UNRESOLVED",
                issuer_binding_methods=[],
                relevance="UNRESOLVED",
                published_at_utc=None,
                source_timezone=None,
                timestamp_precision="UNRESOLVED",
                timestamp_evidence_kind="UNRESOLVED",
                semantics="UNRESOLVED",
                semantic_evidence=[],
            )
            rows.append(row)
            decisions.append(decision)
        extraction = {
            "manifest_sha256": "manifest",
            "counts": {"pairs": 33, "documents": 33, "pair_source_joins": 38},
            "rows": rows,
        }
        review = {
            "schema_version": 1,
            "dataset_id": semantics.DATASET_ID,
            "manifest_sha256": "manifest",
            "review_completed": True,
            "decisions": decisions,
            "target_outcomes_observed_or_derived": False,
        }
        result = semantics.apply_review(extraction, review)
        self.assertEqual(sum(result["terminal_join_counts"].values()), 38)
        self.assertEqual(sum(result["terminal_pair_counts"].values()), 33)
        self.assertEqual(result["verified_positive_pairs"], 0)
        self.assertEqual(result["next_phase"], "SOURCE_RECOVERY")

    def test_public_result_is_aggregate_only(self):
        reviewed = {
            "counts": {"pairs": 33, "documents": 33, "pair_source_joins": 38},
            "terminal_join_counts": {key: 0 for key in semantics.TERMINAL_PRECEDENCE},
            "terminal_pair_counts": {key: 0 for key in semantics.TERMINAL_PRECEDENCE},
            "verified_positive_pairs": 0,
            "next_phase": "SOURCE_RECOVERY",
        }
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "private.gz"
            path.write_bytes(b"private")
            result = semantics._public_result(
                {"manifest_sha256": "manifest"}, reviewed, path
            )
        rendered = str(result).lower()
        self.assertNotIn("https://", rendered)
        self.assertNotIn("test corporation", rendered)
        self.assertNotIn("rows", result)
        self.assertFalse(
            result["source_text_urls_symbols_dates_reviews_and_rows_public"]
        )
        self.assertFalse(result["target_outcomes_observed_or_derived"])

    def test_implementation_hash_mismatch_fails_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            paths = {
                name: root / f"{name}.bin" for name in semantics._input_paths(root)
            }
            for path in paths.values():
                path.write_bytes(b"input")
            selection_path = root / "selection.json.gz"
            capture._write_gzip(
                selection_path,
                {"target_outcomes_observed_or_derived": False},
            )
            manifest = {
                "dataset_id": semantics.DATASET_ID,
                "selection_contract": {
                    **{f"{name}_sha256": "hash" for name in paths},
                    "frozen_private_selection_sha256": "hash",
                },
                "derivation_contract": {
                    "implementation_sha256": "expected",
                    "python_version": semantics.sys.version.split()[0],
                    "pypdf_version": semantics.pypdf.__version__,
                    "target_outcomes_observed_or_derived": False,
                },
            }

            def fake_hash(path):
                return "actual" if Path(path) == Path(semantics.__file__) else "hash"

            with (
                patch.object(
                    semantics, "load_frozen_dataset_contract", return_value=manifest
                ),
                patch.object(semantics, "_input_paths", return_value=paths),
                patch.object(semantics, "_selection_path", return_value=selection_path),
                patch.object(semantics.capture, "_sha256_file", side_effect=fake_hash),
            ):
                with self.assertRaisesRegex(
                    semantics.CatalystSourceSemanticsError, "implementation differs"
                ):
                    semantics._verify_contract(
                        root / "manifest.json", SimpleNamespace(root=root)
                    )


if __name__ == "__main__":
    unittest.main()

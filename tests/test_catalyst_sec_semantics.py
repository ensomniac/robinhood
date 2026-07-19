from __future__ import annotations

import unittest

import catalyst_sec_semantics as sec


def row(**updates):
    value = {
        "row_id": "row",
        "accession": "0" * 18,
        "recovery_status": "RESPONSE_CAPTURED",
        "recovery_http_status": 200,
        "metadata_status": "RESPONSE_CAPTURED",
        "metadata_http_status": 200,
        "source_ownership": "VERIFIED",
        "directory_cik_match": True,
        "target_cik": "1234",
        "metadata_cik_candidates": ["1234"],
        "accepted_datetime_candidates_et": ["2026-01-02T08:00:00"],
        "date": "2026-01-02",
    }
    value.update(updates)
    return value


def decision(**updates):
    value = {
        "row_id": "row",
        "issuer_binding": "VERIFIED",
        "issuer_binding_methods": ["SEC_DIRECTORY_CIK", "SEC_DOCUMENT_CIK"],
        "issuer_binding_evidence": ["exact SEC target CIK and issuer identity"],
        "relevance": "RELEVANT",
        "semantics": "DIRECT_POSITIVE",
        "semantic_evidence": ["material positive issuer event"],
        "financing_or_dilution_conflict": False,
        "notes": "",
    }
    value.update(updates)
    return value


class SecTerminalTests(unittest.TestCase):
    def test_no_accession_precedes_review(self) -> None:
        terminal, _ = sec.terminal_disposition(
            row(accession=None, directory_cik_match=False),
            decision(issuer_binding="UNRESOLVED", issuer_binding_evidence=[]),
        )
        self.assertEqual(terminal, "NO_ACCESSION")

    def test_cik_mismatch_precedes_review(self) -> None:
        terminal, _ = sec.terminal_disposition(
            row(directory_cik_match=False),
            decision(issuer_binding="UNRESOLVED", issuer_binding_evidence=[]),
        )
        self.assertEqual(terminal, "DOCUMENT_CIK_MISMATCH")

    def test_document_metadata_cik_must_match_target(self) -> None:
        terminal, _ = sec.terminal_disposition(
            row(metadata_cik_candidates=["9999"]),
            decision(issuer_binding="UNRESOLVED", issuer_binding_evidence=[]),
        )
        self.assertEqual(terminal, "DOCUMENT_CIK_MISMATCH")

    def test_after_cutoff_fails(self) -> None:
        terminal, timestamp = sec.terminal_disposition(
            row(accepted_datetime_candidates_et=["2026-01-02T09:36:00"]),
            decision(),
        )
        self.assertEqual(terminal, "ACCEPTED_AFTER_0935")
        self.assertIsNotNone(timestamp["accepted_utc"])

    def test_prior_day_acceptance_passes(self) -> None:
        terminal, _ = sec.terminal_disposition(
            row(accepted_datetime_candidates_et=["2026-01-01T23:59:59"]),
            decision(),
        )
        self.assertEqual(terminal, "VERIFIED_POSITIVE_PRIMARY")

    def test_conflicting_acceptance_times_fail(self) -> None:
        terminal, _ = sec.terminal_disposition(
            row(
                accepted_datetime_candidates_et=[
                    "2026-01-01T08:00:00",
                    "2026-01-01T08:01:00",
                ]
            ),
            decision(),
        )
        self.assertEqual(terminal, "ACCEPTANCE_TIME_CONFLICT")

    def test_financing_conflict_precedes_positive(self) -> None:
        terminal, _ = sec.terminal_disposition(
            row(), decision(financing_or_dilution_conflict=True)
        )
        self.assertEqual(terminal, "VERIFIED_CONFLICT")

    def test_negative_primary_is_retained(self) -> None:
        terminal, _ = sec.terminal_disposition(
            row(),
            decision(
                semantics="DIRECT_NEGATIVE",
                semantic_evidence=["material adverse issuer event"],
            ),
        )
        self.assertEqual(terminal, "VERIFIED_NEGATIVE_PRIMARY")

    def test_resolved_semantics_requires_evidence(self) -> None:
        with self.assertRaises(sec.SecSemanticsError):
            sec._validate_decision(decision(semantic_evidence=[]), row())

    def test_completed_review_cannot_hide_outcome_fields(self) -> None:
        extraction = {
            "manifest_sha256": "manifest",
            "rows": [row(row_id=str(index)) for index in range(37)],
        }
        decisions = [decision(row_id=str(index)) for index in range(37)]
        review = {
            "schema_version": 1,
            "dataset_id": sec.DATASET_ID,
            "manifest_sha256": "manifest",
            "review_completed": True,
            "target_outcomes_observed_or_derived": False,
            "decisions": decisions,
            "context": [{"target_return": 1.0}],
        }
        with self.assertRaisesRegex(sec.SecSemanticsError, "outcome-shaped"):
            sec.apply_review(extraction, review)


if __name__ == "__main__":
    unittest.main()

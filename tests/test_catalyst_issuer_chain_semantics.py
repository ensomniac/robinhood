from __future__ import annotations

import copy
import hashlib
import unittest

import catalyst_issuer_chain_semantics as semantics


def source_inputs() -> tuple[dict, dict]:
    chains = []
    urls = []
    captured = {}
    predecessor = 0
    for chain_index in range(6):
        predecessor_count = 8 if chain_index == 0 else 1
        predecessors = [
            f"{predecessor + offset:064x}" for offset in range(predecessor_count)
        ]
        predecessor += predecessor_count
        chain_urls = [
            f"https://issuer{chain_index}.example/news",
            f"https://issuer{chain_index}.example/news/document",
        ]
        chain_id = f"chain-{chain_index}"
        chains.append(
            {
                "chain_id": chain_id,
                "date": f"2026-01-{chain_index + 2:02d}",
                "symbol": f"T{chain_index}",
                "instrument_id": f"instrument-{chain_index}",
                "primary_exchange": "XNYS",
                "target_issuer_name": f"Distinctive {chain_index} Corporation",
                "target_cik": str(1000 + chain_index),
                "source_predecessor_sha256": predecessors,
                "issuer_domain": f"issuer{chain_index}.example",
                "chain_urls": chain_urls,
                "selection_basis": "CANONICAL_ISSUER_NEWSROOM_CHAIN",
                "notes": "official chain",
            }
        )
        for position, url in enumerate(chain_urls):
            url_hash = hashlib.sha256(url.encode()).hexdigest()
            urls.append(
                {
                    "url_sha256": url_hash,
                    "url": url,
                    "issuer_domain": f"issuer{chain_index}.example",
                    "chain_ids": [chain_id],
                    "positions": [position],
                }
            )
            captured[url_hash] = {
                "status": "RESPONSE_CAPTURED",
                "http_status": 200,
                "body_sha256": f"{position + chain_index:064x}",
            }
    selection = {
        "dataset_id": semantics.recovery.DATASET_ID,
        "counts": {
            "chains": 6,
            "source_predecessors": 13,
            "chain_urls": 12,
            "unique_urls": 12,
        },
        "records": chains,
        "urls": urls,
        "target_outcomes_observed_or_derived": False,
    }
    index = {
        "dataset_id": semantics.recovery.DATASET_ID,
        "status": "COLLECTION_COMPLETE",
        "records": captured,
        "target_outcomes_observed_or_derived": False,
    }
    return selection, index


def extracted_row() -> dict:
    return {
        "row_id": "row-1",
        "pair_hash": "pair-1",
        "date": "2026-01-05",
        "symbol": "TEST",
        "issuer_name": "Distinctive Corporation",
        "cik": "1000",
        "source_type": "ISSUER_HOST",
        "extraction": {
            "capture_status": "RESPONSE_CAPTURED",
            "http_status": 200,
            "diagnostic_failures": [],
            "financing_or_dilution_terms_present": False,
        },
    }


def decision() -> dict:
    return {
        "row_id": "row-1",
        "source_ownership": "VERIFIED",
        "ownership_evidence": ["official footer and controlled host"],
        "source_issuer_name": "Distinctive Corporation",
        "source_issuer_cik": None,
        "issuer_binding": "VERIFIED",
        "issuer_binding_methods": [
            "issuer_owned_canonical_domain",
            "legal_company_name",
        ],
        "relevance": "RELEVANT",
        "published_at_utc": "2026-01-05T13:00:00+00:00",
        "published_date": None,
        "source_timezone": "America/New_York",
        "timestamp_precision": "DATETIME",
        "timestamp_evidence_kind": "JSON_LD_AND_VISIBLE_DATETIME",
        "timestamp_conflict": False,
        "semantics": "DIRECT_POSITIVE",
        "semantic_evidence": ["material positive issuer announcement"],
        "financing_or_dilution_conflict": False,
        "notes": "",
    }


class IssuerChainSelectionTests(unittest.TestCase):
    def test_exact_private_surface_rebuilds(self) -> None:
        selection, index = source_inputs()
        value = semantics.build_selection(selection, index)
        self.assertEqual(
            value["counts"],
            {"chains": 6, "sources": 12, "source_predecessors": 13},
        )
        self.assertFalse(value["target_outcomes_observed_or_derived"])

    def test_missing_terminal_source_is_rejected(self) -> None:
        selection, index = source_inputs()
        index["records"].pop(next(iter(index["records"])))
        with self.assertRaisesRegex(
            semantics.IssuerChainSemanticsError, "source set differs"
        ):
            semantics.build_selection(selection, index)

    def test_chain_relation_cannot_change(self) -> None:
        selection, index = source_inputs()
        selection["urls"][0]["positions"] = [1]
        with self.assertRaisesRegex(
            semantics.IssuerChainSemanticsError, "URL relation differs"
        ):
            semantics.build_selection(selection, index)


class IssuerChainDecisionTests(unittest.TestCase):
    def test_cross_issuer_name_cannot_verify_target_binding(self) -> None:
        value = decision()
        value["source_issuer_name"] = "Unrelated Holdings Inc."
        with self.assertRaisesRegex(
            semantics.IssuerChainSemanticsError,
            "does not match point-in-time target",
        ):
            semantics._validate_review_decision(value, row=extracted_row())

    def test_explicit_mismatched_source_cik_cannot_fall_back_to_name(self) -> None:
        value = decision()
        value["source_issuer_cik"] = "9999"
        with self.assertRaisesRegex(
            semantics.IssuerChainSemanticsError,
            "does not match point-in-time target",
        ):
            semantics._validate_review_decision(value, row=extracted_row())

    def test_exact_cik_binding_requires_exact_cik_method(self) -> None:
        value = decision()
        value["source_issuer_cik"] = "1000"
        with self.assertRaisesRegex(
            semantics.IssuerChainSemanticsError, "exact-CIK method"
        ):
            semantics._validate_review_decision(value, row=extracted_row())

    def test_same_day_date_only_is_terminally_unresolved(self) -> None:
        value = decision()
        value.update(
            {
                "published_at_utc": None,
                "published_date": "2026-01-05",
                "timestamp_precision": "DATE",
            }
        )
        validated, timestamp = semantics._validate_review_decision(
            value, row=extracted_row()
        )
        terminal, _ = semantics.source_semantics.terminal_disposition(
            extracted_row(), validated, timestamp
        )
        self.assertEqual(terminal, "SAME_DAY_TIME_UNRESOLVED")

    def test_financing_conflict_precedes_positive(self) -> None:
        value = decision()
        value["financing_or_dilution_conflict"] = True
        validated, timestamp = semantics._validate_review_decision(
            value, row=extracted_row()
        )
        terminal, _ = semantics.source_semantics.terminal_disposition(
            extracted_row(), validated, timestamp
        )
        self.assertEqual(terminal, "VERIFIED_CONFLICT")

    def test_binding_cannot_be_verified_without_ownership(self) -> None:
        value = decision()
        value["source_ownership"] = "UNRESOLVED"
        with self.assertRaisesRegex(
            semantics.IssuerChainSemanticsError, "requires verified ownership"
        ):
            semantics._validate_review_decision(value, row=extracted_row())


class IssuerChainReviewTests(unittest.TestCase):
    @staticmethod
    def _extraction() -> dict:
        rows = []
        for pair_index in range(6):
            for source_index in range(2):
                row = copy.deepcopy(extracted_row())
                row["row_id"] = f"row-{pair_index}-{source_index}"
                row["pair_hash"] = f"pair-{pair_index}"
                row["issuer_name"] = f"Distinctive {pair_index} Corporation"
                rows.append(row)
        return {
            "manifest_sha256": "manifest",
            "counts": {"chains": 6, "sources": 12, "source_predecessors": 13},
            "rows": rows,
        }

    def test_outcome_shaped_review_field_is_rejected(self) -> None:
        extraction = self._extraction()
        review_input = {
            "schema_version": 1,
            "dataset_id": semantics.DATASET_ID,
            "manifest_sha256": "manifest",
            "review_completed": True,
            "decisions": [],
            "future_price": 10,
            "target_outcomes_observed_or_derived": False,
        }
        with self.assertRaisesRegex(
            semantics.IssuerChainSemanticsError, "outcome-shaped"
        ):
            semantics._validate_review_input(review_input, extraction)

    def test_pair_conflict_precedes_positive(self) -> None:
        rows = [
            {"terminal_disposition": "VERIFIED_POSITIVE_PRIMARY"},
            {"terminal_disposition": "VERIFIED_CONFLICT"},
        ]
        self.assertEqual(semantics._pair_disposition(rows), "VERIFIED_CONFLICT")

    def test_positive_pair_hashes_are_exactly_deduplicated(self) -> None:
        reviewed = {
            "pair_dispositions": {
                "pair-a": "VERIFIED_POSITIVE_PRIMARY",
                "pair-b": "VERIFIED_CONFLICT",
                "pair-c": "VERIFIED_POSITIVE_PRIMARY",
            }
        }
        self.assertEqual(
            semantics._positive_pair_hashes(reviewed), {"pair-a", "pair-c"}
        )


if __name__ == "__main__":
    unittest.main()

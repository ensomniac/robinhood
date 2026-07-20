from __future__ import annotations

import hashlib
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

import development_sec_accession_chain_recovery as recovery
import development_catalyst_source_semantics as upstream


def _upstream_row(
    *,
    index: int,
    pair_index: int,
    accession: str,
) -> dict:
    return {
        "row_id": f"prior-row-{index}",
        "date": "2025-01-02",
        "instrument_id": f"instrument-{pair_index}",
        "primary_exchange": "XNYS",
        "rank": pair_index + 1,
        "symbol": f"T{pair_index}",
        "cik": str(1000 + index),
        "form": "8-K",
        "accession": accession,
        "accepted_at": "2025-01-02T13:00:00+00:00",
    }


def _reviewed_fixture() -> dict:
    rows = [
        _upstream_row(
            index=0,
            pair_index=0,
            accession="0000001000-25-000001",
        ),
        _upstream_row(
            index=1,
            pair_index=0,
            accession="0000001001-25-000002",
        ),
        _upstream_row(
            index=2,
            pair_index=1,
            accession="0000001002-25-000003",
        ),
    ]
    pairs = {
        upstream._sha256_json(("2025-01-02", "instrument-0")):
        "DOCUMENT_SEMANTICS_UNRESOLVED",
        upstream._sha256_json(("2025-01-02", "instrument-1")):
        "DOCUMENT_SEMANTICS_UNRESOLVED",
    }
    return {
        "schema_version": 1,
        "dataset_id": recovery.SOURCE_REVIEW_DATASET_ID,
        "status": "REVIEW_COMPLETE",
        "verified_positive_pairs": recovery.EXPECTED_PRIOR_POSITIVES,
        "pair_dispositions": pairs,
        "rows": rows,
        "target_outcomes_observed_or_derived": False,
    }


def _one_pair_selection() -> dict:
    reviewed = _reviewed_fixture()
    first = reviewed["rows"][0]
    pair_hash = upstream._sha256_json((first["date"], first["instrument_id"]))
    reviewed["rows"] = [first]
    reviewed["pair_dispositions"] = {
        pair_hash: "DOCUMENT_SEMANTICS_UNRESOLVED"
    }
    return recovery.build_selection(
        reviewed,
        expected_pairs=1,
        expected_joins=1,
        expected_accessions=1,
    )


def _complete_submission(*documents: tuple[str, str]) -> bytes:
    blocks = []
    for index, (document_type, text) in enumerate(documents):
        blocks.append(
            "<DOCUMENT>\n"
            f"<TYPE>{document_type}\n"
            f"<FILENAME>document-{index}.htm\n"
            "<DESCRIPTION>Issuer release\n"
            f"<TEXT><html><body>{text}</body></html></TEXT>\n"
            "</DOCUMENT>"
        )
    return "\n".join(blocks).encode()


def _index_and_source(
    root: Path, selection: dict, raw: bytes, *, status: str = "SUCCESS"
) -> dict:
    request = selection["requests"][0]
    source = recovery._shared_path(root, request)
    source.parent.mkdir(parents=True, exist_ok=True)
    source.write_bytes(raw)
    record = {
        "request_sha256": request["request_sha256"],
        "wrapper_sha256": "wrapper",
        "status": status,
        "source_origin": "SEC_DOWNLOAD" if status == "SUCCESS" else None,
        "source_bytes": len(raw) if status == "SUCCESS" else 0,
        "source_sha256": hashlib.sha256(raw).hexdigest()
        if status == "SUCCESS"
        else None,
        "error": None if status == "SUCCESS" else {"type": "failure"},
    }
    return {
        "schema_version": 1,
        "dataset_id": recovery.DATASET_ID,
        "manifest_sha256": "manifest",
        "status": "COLLECTION_COMPLETE",
        "counts": {
            "expected_requests": 1,
            "terminal_requests": 1,
            "successful_requests": int(status == "SUCCESS"),
            "failed_requests": int(status != "SUCCESS"),
            "pending_requests": 0,
        },
        "records": [record],
        "target_outcomes_observed_or_derived": False,
    }


def test_complete_unresolved_accession_surface_rebuilds() -> None:
    selection = recovery.build_selection(
        _reviewed_fixture(),
        expected_pairs=2,
        expected_joins=3,
        expected_accessions=3,
    )
    assert selection["counts"] == {
        "candidate_pairs": 2,
        "prior_pair_source_joins": 3,
        "unique_accessions": 3,
    }
    assert selection["target_outcomes_observed_or_derived"] is False


def test_non_unresolved_pair_cannot_enter_selection() -> None:
    reviewed = _reviewed_fixture()
    pair_hash = next(iter(reviewed["pair_dispositions"]))
    reviewed["pair_dispositions"][pair_hash] = "VERIFIED_NEGATIVE_PRIMARY"
    with pytest.raises(
        recovery.DevelopmentSecAccessionChainError,
        match="surface differs",
    ):
        recovery.build_selection(
            reviewed,
            expected_pairs=2,
            expected_joins=3,
            expected_accessions=3,
        )


def test_accession_request_is_exact_and_rejects_malformed_identity() -> None:
    request = recovery._submission_request("0000001000", "0000001000-25-000001")
    assert request["source_url"].endswith(
        "/1000/000000100025000001/0000001000-25-000001.txt"
    )
    with pytest.raises(
        recovery.DevelopmentSecAccessionChainError,
        match="malformed",
    ):
        recovery._submission_request("1000", "bad-accession")


def test_parser_selects_only_ex99_documents() -> None:
    raw = _complete_submission(
        ("8-K", "filing stub"),
        ("EX-99.1", "issuer release"),
        ("EX-101.INS", "xbrl"),
    )
    documents = recovery.parse_ex99_documents(raw)
    assert len(documents) == 1
    assert documents[0]["document_type"] == "EX-99.1"
    assert b"issuer release" in documents[0]["body"]


def test_positive_exhibit_opens_source_capacity_without_outcomes() -> None:
    selection = _one_pair_selection()
    raw = _complete_submission(
        ("8-K", "Item 9.01"),
        (
            "EX-99.1",
            "Revenue increased by 25% during the quarter, reflecting sustained demand.",
        ),
    )
    with TemporaryDirectory() as directory:
        root = Path(directory)
        index = _index_and_source(root, selection, raw)
        reviewed = recovery.build_review(
            selection=selection,
            index=index,
            store_root=root,
        )
    assert reviewed["recovered_verified_positive_pairs"] == 1
    assert reviewed["combined_verified_positive_pairs"] == 20
    assert reviewed["next_phase"] == "DEVELOPMENT_ACQUISITION"
    assert reviewed["outcome_contract_permitted"] is False


def test_financing_conflict_precedes_positive_exhibit() -> None:
    selection = _one_pair_selection()
    raw = _complete_submission(
        (
            "EX-99.1",
            "Revenue increased by 25%. The company also announced a public offering.",
        ),
    )
    with TemporaryDirectory() as directory:
        root = Path(directory)
        index = _index_and_source(root, selection, raw)
        reviewed = recovery.build_review(
            selection=selection,
            index=index,
            store_root=root,
        )
    assert reviewed["recovered_verified_positive_pairs"] == 0
    assert reviewed["terminal_pair_counts"]["VERIFIED_CONFLICT"] == 1


def test_missing_ex99_is_semantically_unresolved() -> None:
    selection = _one_pair_selection()
    raw = _complete_submission(("8-K", "Item 9.01 filing stub"))
    with TemporaryDirectory() as directory:
        root = Path(directory)
        index = _index_and_source(root, selection, raw)
        reviewed = recovery.build_review(
            selection=selection,
            index=index,
            store_root=root,
        )
    assert reviewed["terminal_pair_counts"]["DOCUMENT_SEMANTICS_UNRESOLVED"] == 1
    assert reviewed["recovered_verified_positive_pairs"] == 0


def test_failed_capture_remains_in_pair_denominator() -> None:
    selection = _one_pair_selection()
    with TemporaryDirectory() as directory:
        root = Path(directory)
        index = _index_and_source(root, selection, b"failure", status="FAILED")
        reviewed = recovery.build_review(
            selection=selection,
            index=index,
            store_root=root,
        )
    assert reviewed["terminal_pair_counts"]["CAPTURE_TRANSPORT_ERROR"] == 1
    assert sum(reviewed["terminal_pair_counts"].values()) == 1


def test_pair_conflict_overrides_a_second_positive_exhibit() -> None:
    selection = _one_pair_selection()
    raw = _complete_submission(
        ("EX-99.1", "Revenue increased by 25%."),
        ("EX-99.2", "The company announced a registered direct offering."),
    )
    with TemporaryDirectory() as directory:
        root = Path(directory)
        index = _index_and_source(root, selection, raw)
        reviewed = recovery.build_review(
            selection=selection,
            index=index,
            store_root=root,
        )
    assert reviewed["terminal_pair_counts"]["VERIFIED_CONFLICT"] == 1
    assert reviewed["recovered_verified_positive_pairs"] == 0

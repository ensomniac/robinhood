"""Profile captured catalyst PDFs without accepting dates or evidence."""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
from collections import Counter
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import pypdf
from pypdf import PdfReader

import catalyst_primary_source_capture as capture
from historical_store import HistoricalDayStore
from learning_data import (
    LearningDataError,
    freeze_dataset_contract,
    load_frozen_dataset_contract,
)


PROJECT_ROOT = Path(__file__).resolve().parent
DATASET_ID = "dataset-catalyst-pdf-source-profile-2026-07-19-expansion-v1"
EXPECTED_PDFS = 13
SOURCE_MANIFEST = (
    PROJECT_ROOT
    / "historical_batches"
    / "catalyst_source_profile"
    / "manifests"
    / (
        "dataset-catalyst-source-profile-2026-07-19-expansion-v1-"
        "411be9d083d118d06087c73d95ad3e8de386b6a701358c29feb618800cb3151f.json"
    )
)
SOURCE_RESULT = (
    PROJECT_ROOT / "research_results" / "2026-07-19-catalyst-source-profile.json"
)
DEFAULT_OUTPUT_ROOT = (
    PROJECT_ROOT / "historical_batches" / "catalyst_pdf_source_profile" / "manifests"
)
DEFAULT_PUBLIC_STATUS = (
    PROJECT_ROOT
    / "historical_batches"
    / "catalyst_pdf_source_profile"
    / "collection-status.json"
)
DEFAULT_PUBLIC_RESULT = (
    PROJECT_ROOT / "research_results" / "2026-07-19-catalyst-pdf-source-profile.json"
)
MAX_PROFILE_PAGES = 3
DATE_PATTERNS = {
    "iso": re.compile(r"\b20\d{2}-[01]\d-[0-3]\d\b"),
    "month_name": re.compile(
        r"\b(?:jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|"
        r"jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:tember)?|oct(?:ober)?|"
        r"nov(?:ember)?|dec(?:ember)?)\s+\d{1,2},?\s+20\d{2}\b",
        re.IGNORECASE,
    ),
    "numeric_month_day": re.compile(
        r"\b(?:0?[1-9]|1[0-2])[/.-](?:0?[1-9]|[12]\d|3[01])[/.-]20\d{2}\b"
    ),
}
DOCUMENT_MARKERS = {
    "annual_report": re.compile(r"\b(?:annual report|form 10-k)\b", re.IGNORECASE),
    "earnings_or_results": re.compile(
        r"\b(?:earnings|financial results|quarter results|quarterly results)\b",
        re.IGNORECASE,
    ),
    "press_release": re.compile(r"\bpress release\b", re.IGNORECASE),
    "court_document": re.compile(
        r"\b(?:court|plaintiff|defendant|memorandum opinion)\b", re.IGNORECASE
    ),
    "government_document": re.compile(
        r"\b(?:united states|u\.s\. government|senate|commission)\b",
        re.IGNORECASE,
    ),
}


class CatalystPdfSourceProfileError(RuntimeError):
    """The frozen PDF profile contract or inputs are invalid."""


def _write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_text(
            json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _repo_path(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(PROJECT_ROOT))
    except ValueError as exc:
        raise CatalystPdfSourceProfileError(
            f"public path must be repository-relative: {path}"
        ) from exc


def _private_path(store_root: Path) -> Path:
    return (
        store_root
        / "_derived"
        / "catalyst_pdf_source_profile"
        / DATASET_ID
        / "pdf-profile.json.gz"
    )


def _capture_index_path(store_root: Path) -> Path:
    return capture._index_path(store_root)


def _pdf_hashes(index: Mapping[str, Any]) -> list[str]:
    return sorted(
        str(key)
        for key, row in index.get("records", {}).items()
        if row.get("http_status") == 200
        and "pdf" in str(row.get("headers", {}).get("content-type") or "").lower()
    )


def _pdf_bundle_hash(store_root: Path, hashes: list[str]) -> str:
    rows = [
        (
            source_hash,
            capture._sha256_file(capture._response_path(store_root, source_hash)),
        )
        for source_hash in hashes
    ]
    return capture._sha256_json(rows)


def profile_text(text: str) -> dict[str, Any]:
    date_candidates = {
        name: sorted(set(pattern.findall(text)))
        for name, pattern in DATE_PATTERNS.items()
    }
    return {
        "text_present": bool(text.strip()),
        "date_candidates": date_candidates,
        "document_markers": {
            name: bool(pattern.search(text))
            for name, pattern in DOCUMENT_MARKERS.items()
        },
    }


def build_pdf_profile(
    store: HistoricalDayStore, index: Mapping[str, Any]
) -> dict[str, Any]:
    records: dict[str, dict[str, Any]] = {}
    counts: Counter[str] = Counter()
    page_counts: list[int] = []
    for source_hash in _pdf_hashes(index):
        path = capture._response_path(store.root, source_hash)
        if capture._sha256_file(path) != index["records"][source_hash].get(
            "body_sha256"
        ):
            raise CatalystPdfSourceProfileError("captured PDF hash mismatch")
        try:
            reader = PdfReader(path)
            metadata = reader.metadata or {}
            pages_profiled = min(len(reader.pages), MAX_PROFILE_PAGES)
            text = "\n".join(
                (reader.pages[number].extract_text() or "")
                for number in range(pages_profiled)
            )
        except Exception as exc:
            raise CatalystPdfSourceProfileError(
                f"captured PDF cannot be profiled: {source_hash}"
            ) from exc
        text_profile = profile_text(text)
        record = {
            "page_count": len(reader.pages),
            "pages_profiled": pages_profiled,
            "metadata": {
                "title_present": bool(metadata.get("/Title")),
                "creation_date_present": bool(metadata.get("/CreationDate")),
                "modification_date_present": bool(metadata.get("/ModDate")),
            },
            **text_profile,
        }
        records[source_hash] = record
        page_counts.append(len(reader.pages))
        counts["pdf_documents"] += 1
        counts["text_present"] += record["text_present"]
        for name, present in record["metadata"].items():
            counts[f"metadata_{name}"] += present
        for name, candidates in record["date_candidates"].items():
            counts[f"documents_with_{name}_date_candidate"] += bool(candidates)
        counts["documents_with_any_date_candidate"] += any(
            record["date_candidates"].values()
        )
        for name, present in record["document_markers"].items():
            counts[f"documents_with_{name}_marker"] += present
    counts["pages_total"] = sum(page_counts)
    counts["pages_minimum"] = min(page_counts, default=0)
    counts["pages_maximum"] = max(page_counts, default=0)
    return {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "status": "PROFILE_COMPLETE",
        "counts": dict(sorted(counts.items())),
        "records": records,
        "metadata_dates_accepted": 0,
        "text_date_candidates_accepted": 0,
        "source_ownership_verified": False,
        "issuer_binding_verified": False,
        "causal_availability_verified": False,
        "primary_catalyst_verified": False,
        "target_outcomes_observed_or_derived": False,
    }


def freeze_inputs(*, env_path: Path, output_root: Path) -> tuple[Path, dict[str, Any]]:
    store = HistoricalDayStore.from_env(env_path)
    source_manifest = load_frozen_dataset_contract(SOURCE_MANIFEST)
    source_result = capture._read_json(SOURCE_RESULT)
    if (
        source_result.get("status") != "READY"
        or source_result.get("inspected") is not True
    ):
        raise CatalystPdfSourceProfileError("source profile is not inspected READY")
    index_path = _capture_index_path(store.root)
    index = capture._read_json(index_path)
    hashes = _pdf_hashes(index)
    if len(hashes) != EXPECTED_PDFS:
        raise CatalystPdfSourceProfileError("capture corpus is not exactly 13 PDFs")
    contract = {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "registered_at": capture._timestamp_now(),
        "requested_dates": list(source_manifest["requested_dates"]),
        "dataset_payload": {
            "lane": "development",
            "claim_scope": "DEVELOPMENT_ONLY",
            "status": "COLLECTING",
            "evidence_paths": [
                _repo_path(SOURCE_MANIFEST),
                _repo_path(SOURCE_RESULT),
                "CATALYST_SOURCE_PAIR_READINESS.md",
                "CATALYST_EVIDENCE_ACQUISITION.md",
            ],
            "inspected": False,
        },
        "selection_contract": {
            "source_profile_manifest_sha256": source_manifest["manifest_sha256"],
            "capture_index_sha256": capture._sha256_file(index_path),
            "pdf_bundle_sha256": _pdf_bundle_hash(store.root, hashes),
            "pdf_documents": EXPECTED_PDFS,
            "pdf_hashes": hashes,
            "documents_text_dates_and_rows_public": False,
        },
        "derivation_contract": {
            "implementation_sha256": capture._sha256_file(Path(__file__)),
            "pypdf_version": pypdf.__version__,
            "maximum_profile_pages": MAX_PROFILE_PAGES,
            "date_patterns": {
                name: pattern.pattern for name, pattern in DATE_PATTERNS.items()
            },
            "document_markers": {
                name: pattern.pattern for name, pattern in DOCUMENT_MARKERS.items()
            },
            "pdf_metadata_dates_are_causal_evidence": False,
            "text_date_candidates_are_causal_evidence": False,
            "document_markers_classify_catalysts": False,
            "network_access_allowed": False,
            "target_outcomes_observed_or_derived": False,
        },
    }
    return freeze_dataset_contract(contract, output_root)


def _verify_contract(
    manifest_path: Path, store: HistoricalDayStore
) -> tuple[dict[str, Any], dict[str, Any]]:
    manifest = load_frozen_dataset_contract(manifest_path)
    if manifest.get("dataset_id") != DATASET_ID:
        raise CatalystPdfSourceProfileError("unexpected PDF profile dataset")
    index_path = _capture_index_path(store.root)
    index = capture._read_json(index_path)
    hashes = _pdf_hashes(index)
    selection = manifest["selection_contract"]
    if capture._sha256_file(index_path) != selection.get("capture_index_sha256"):
        raise CatalystPdfSourceProfileError("capture index changed")
    if _pdf_bundle_hash(store.root, hashes) != selection.get("pdf_bundle_sha256"):
        raise CatalystPdfSourceProfileError("PDF bundle changed")
    derivation = manifest["derivation_contract"]
    if capture._sha256_file(Path(__file__)) != derivation.get("implementation_sha256"):
        raise CatalystPdfSourceProfileError("PDF profiler differs from frozen contract")
    if pypdf.__version__ != derivation.get("pypdf_version"):
        raise CatalystPdfSourceProfileError(
            "pypdf version differs from frozen contract"
        )
    return manifest, index


def derive(
    *, manifest_path: Path, env_path: Path, public_status_path: Path
) -> dict[str, Any]:
    store = HistoricalDayStore.from_env(env_path)
    manifest, index = _verify_contract(manifest_path, store)
    private = build_pdf_profile(store, index)
    private["manifest_sha256"] = manifest["manifest_sha256"]
    private_path = _private_path(store.root)
    capture._write_gzip(private_path, private)
    public = {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "status": "PROFILE_COMPLETE",
        "counts": private["counts"],
        "private_result_sha256": capture._sha256_file(private_path),
        "metadata_dates_accepted": 0,
        "text_date_candidates_accepted": 0,
        "documents_text_dates_and_rows_public": False,
        "primary_catalyst_verified": False,
        "target_outcomes_observed_or_derived": False,
    }
    _write_json(public_status_path, public)
    return public


def inspect(
    *, manifest_path: Path, env_path: Path, public_result_path: Path
) -> dict[str, Any]:
    store = HistoricalDayStore.from_env(env_path)
    manifest, index = _verify_contract(manifest_path, store)
    private_path = _private_path(store.root)
    private = capture._read_gzip(private_path)
    rebuilt = build_pdf_profile(store, index)
    if not (
        private.get("manifest_sha256") == manifest["manifest_sha256"]
        and private.get("counts") == rebuilt.get("counts")
        and private.get("metadata_dates_accepted") == 0
        and private.get("text_date_candidates_accepted") == 0
        and private.get("primary_catalyst_verified") is False
        and private.get("target_outcomes_observed_or_derived") is False
    ):
        raise CatalystPdfSourceProfileError("PDF profile failed inspection")
    result = {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "status": "READY",
        "inspected": True,
        "claim_scope": "DEVELOPMENT_ONLY",
        "counts": private["counts"],
        "private_result_sha256": capture._sha256_file(private_path),
        "findings": {
            "pdf_structure_profile_complete": True,
            "metadata_dates_accepted": 0,
            "text_date_candidates_accepted": 0,
            "source_ownership_verified": False,
            "issuer_binding_verified": False,
            "causal_availability_verified": False,
            "primary_catalyst_verified": False,
            "production_rule_change_earned": False,
        },
        "next_required_stage": (
            "freeze issuer/source binding and first-page date semantics before "
            "accepting any PDF as causal catalyst evidence"
        ),
        "claim_boundary": (
            "PDF readability, metadata, front-text date-pattern, and structural-marker "
            "profile only; none is accepted as publication or catalyst evidence."
        ),
        "documents_text_dates_and_rows_public": False,
        "target_outcomes_observed_or_derived": False,
    }
    _write_json(public_result_path, result)
    return result


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("freeze", "derive", "inspect"))
    parser.add_argument("--manifest", type=Path)
    parser.add_argument("--env-file", type=Path, default=PROJECT_ROOT / ".env")
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--public-status", type=Path, default=DEFAULT_PUBLIC_STATUS)
    parser.add_argument("--public-result", type=Path, default=DEFAULT_PUBLIC_RESULT)
    return parser


def main() -> int:
    args = _parser().parse_args()
    try:
        if args.command == "freeze":
            path, manifest = freeze_inputs(
                env_path=args.env_file, output_root=args.output_root
            )
            output = {"manifest": _repo_path(path), **manifest}
        elif args.manifest is None:
            raise CatalystPdfSourceProfileError("--manifest is required")
        elif args.command == "derive":
            output = derive(
                manifest_path=args.manifest,
                env_path=args.env_file,
                public_status_path=args.public_status,
            )
        else:
            output = inspect(
                manifest_path=args.manifest,
                env_path=args.env_file,
                public_result_path=args.public_result,
            )
    except (
        CatalystPdfSourceProfileError,
        LearningDataError,
        OSError,
        ValueError,
    ) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}), file=sys.stderr)
        return 2
    print(json.dumps(output, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

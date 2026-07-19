"""Freeze and inspect outcome-blind semantics for canonical issuer chains."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from pypdf import PdfReader

import catalyst_issuer_chain_recovery as recovery
import catalyst_sec_semantics as sec_semantics
import catalyst_source_semantics as source_semantics
from historical_store import HistoricalDayStore
from learning_data import (
    LearningDataError,
    freeze_dataset_contract,
    load_frozen_dataset_contract,
)


PROJECT_ROOT = Path(__file__).resolve().parent
DATASET_ID = "dataset-catalyst-issuer-chain-semantics-2026-07-19-expansion-v1"
SOURCE_MANIFEST = (
    PROJECT_ROOT
    / "historical_batches"
    / "catalyst_issuer_chain_recovery"
    / "manifests"
    / (
        "dataset-catalyst-issuer-chain-recovery-2026-07-19-expansion-v1-"
        "15f7b129be15389a1ad59159f75e37ea79f49c8fd8d1a85ae08a73c3fd1d2ebf.json"
    )
)
SOURCE_RESULT = (
    PROJECT_ROOT / "research_results" / "2026-07-19-catalyst-issuer-chain-recovery.json"
)
PRIOR_SOURCE_RESULT = source_semantics.DEFAULT_PUBLIC_RESULT
PRIOR_SEC_RESULT = sec_semantics.DEFAULT_PUBLIC_RESULT
DEFAULT_OUTPUT_ROOT = (
    PROJECT_ROOT
    / "historical_batches"
    / "catalyst_issuer_chain_semantics"
    / "manifests"
)
DEFAULT_PUBLIC_STATUS = (
    PROJECT_ROOT
    / "historical_batches"
    / "catalyst_issuer_chain_semantics"
    / "collection-status.json"
)
DEFAULT_PUBLIC_RESULT = (
    PROJECT_ROOT
    / "research_results"
    / "2026-07-19-catalyst-issuer-chain-semantics.json"
)
DEFAULT_REVIEW_TEMPLATE = (
    PROJECT_ROOT
    / "learning_runs"
    / "production_validation"
    / "issuer-chain-semantics-review.json"
)

EXPECTED_CHAINS = 6
EXPECTED_SOURCES = 12
PARSER_VERSION = "issuer-chain-semantics-parser-v1"
TERMINAL_PRECEDENCE = source_semantics.TERMINAL_PRECEDENCE
EVENT_TAXONOMY = source_semantics.EVENT_TAXONOMY
TIMESTAMP_PRECEDENCE = source_semantics.TIMESTAMP_PRECEDENCE
BASE_REVIEW_FIELDS = source_semantics.REVIEW_FIELDS
REVIEW_FIELDS = {*BASE_REVIEW_FIELDS, "source_issuer_name", "source_issuer_cik"}
COMPANY_SUFFIXES = {
    "ag",
    "ai",
    "co",
    "company",
    "corp",
    "corporation",
    "inc",
    "incorporated",
    "limited",
    "llc",
    "ltd",
    "nv",
    "plc",
    "sa",
}
GENERIC_COMPANY_TOKENS = {
    "advanced",
    "global",
    "group",
    "holding",
    "holdings",
    "international",
    "semiconductor",
    "systems",
    "technologies",
    "technology",
}


class IssuerChainSemanticsError(RuntimeError):
    """The issuer-chain semantic contract or review is unsafe."""


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode()


def _sha256_json(value: Any) -> str:
    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise IssuerChainSemanticsError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise IssuerChainSemanticsError(f"{path} must contain an object")
    return value


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
        raise IssuerChainSemanticsError(
            f"public path must be repository-relative: {path}"
        ) from exc


def _private_root(store_root: Path) -> Path:
    return store_root / "_derived" / "catalyst_issuer_chain_semantics" / DATASET_ID


def _selection_path(store_root: Path) -> Path:
    return _private_root(store_root) / "selection.json.gz"


def _extraction_path(store_root: Path) -> Path:
    return _private_root(store_root) / "extraction.json.gz"


def _reviewed_path(store_root: Path) -> Path:
    return _private_root(store_root) / "reviewed.json.gz"


def _prior_paths(store_root: Path) -> dict[str, Path]:
    return {
        "prior_source_reviewed": source_semantics._reviewed_path(store_root),
        "prior_sec_reviewed": sec_semantics._reviewed_path(store_root),
    }


def _verify_public_result(path: Path) -> dict[str, Any]:
    value = _read_json(path)
    if not (
        value.get("status") == "READY"
        and value.get("inspected") is True
        and value.get("target_outcomes_observed_or_derived") is False
    ):
        raise IssuerChainSemanticsError(f"upstream result is not READY: {path.name}")
    return value


def build_selection(
    source_selection: Mapping[str, Any], source_index: Mapping[str, Any]
) -> dict[str, Any]:
    if not (
        source_selection.get("dataset_id") == recovery.DATASET_ID
        and source_selection.get("counts")
        == {
            "chains": EXPECTED_CHAINS,
            "source_predecessors": recovery.EXPECTED_SOURCE_PREDECESSORS,
            "chain_urls": EXPECTED_SOURCES,
            "unique_urls": EXPECTED_SOURCES,
        }
        and source_selection.get("target_outcomes_observed_or_derived") is False
        and source_index.get("dataset_id") == recovery.DATASET_ID
        and source_index.get("status") == "COLLECTION_COMPLETE"
        and source_index.get("target_outcomes_observed_or_derived") is False
    ):
        raise IssuerChainSemanticsError("issuer-chain recovery inputs are incomplete")
    url_records = {
        str(row["url_sha256"]): row for row in source_selection.get("urls", [])
    }
    captured = source_index.get("records", {})
    if len(url_records) != EXPECTED_SOURCES or set(url_records) != set(captured):
        raise IssuerChainSemanticsError("issuer-chain source set differs")
    rows: list[dict[str, Any]] = []
    pair_hashes: set[str] = set()
    predecessor_hashes: set[str] = set()
    for chain in source_selection.get("records", []):
        pair_key = (str(chain["date"]), str(chain["symbol"]))
        pair_hash = _sha256_json(pair_key)
        pair_hashes.add(pair_hash)
        predecessor_hashes.update(chain["source_predecessor_sha256"])
        for position, url in enumerate(chain["chain_urls"]):
            url_hash = hashlib.sha256(str(url).encode()).hexdigest()
            selected = url_records.get(url_hash)
            capture_record = captured.get(url_hash)
            if not selected or not capture_record:
                raise IssuerChainSemanticsError("issuer-chain source is missing")
            if not (
                selected.get("url") == url
                and selected.get("issuer_domain") == chain.get("issuer_domain")
                and chain.get("chain_id") in selected.get("chain_ids", [])
                and position in selected.get("positions", [])
            ):
                raise IssuerChainSemanticsError("issuer-chain URL relation differs")
            rows.append(
                {
                    "row_id": "issuer-source-"
                    + _sha256_json([chain["chain_id"], url_hash])[:24],
                    "chain_id": chain["chain_id"],
                    "pair_hash": pair_hash,
                    "date": chain["date"],
                    "symbol": chain["symbol"],
                    "instrument_id": chain["instrument_id"],
                    "primary_exchange": chain["primary_exchange"],
                    "issuer_name": chain["target_issuer_name"],
                    "cik": str(chain["target_cik"]),
                    "source_predecessor_sha256": list(
                        chain["source_predecessor_sha256"]
                    ),
                    "issuer_domain": chain["issuer_domain"],
                    "source_position": position,
                    "url_sha256": url_hash,
                    "url": url,
                    "source_type": "ISSUER_HOST",
                    "capture_record_sha256": _sha256_json(capture_record),
                }
            )
    rows.sort(key=lambda row: str(row["row_id"]))
    if not (
        len(rows) == EXPECTED_SOURCES
        and len(pair_hashes) == EXPECTED_CHAINS
        and len(predecessor_hashes) == recovery.EXPECTED_SOURCE_PREDECESSORS
    ):
        raise IssuerChainSemanticsError("issuer-chain semantic surface differs")
    return {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "status": "FROZEN_SELECTION",
        "counts": {
            "chains": len(pair_hashes),
            "sources": len(rows),
            "source_predecessors": len(predecessor_hashes),
        },
        "rows": rows,
        "pair_identity_sha256": _sha256_json(sorted(pair_hashes)),
        "source_relation_sha256": _sha256_json(rows),
        "symbols_dates_urls_sources_reviews_and_rows_public": False,
        "target_outcomes_observed_or_derived": False,
    }


def freeze_inputs(*, env_path: Path, output_root: Path) -> tuple[Path, dict[str, Any]]:
    source_result = _verify_public_result(SOURCE_RESULT)
    prior_source_result = _verify_public_result(PRIOR_SOURCE_RESULT)
    prior_sec_result = _verify_public_result(PRIOR_SEC_RESULT)
    source_manifest = load_frozen_dataset_contract(SOURCE_MANIFEST)
    store = HistoricalDayStore.from_env(env_path)
    source_selection_path = recovery._selection_path(store.root)
    source_index_path = recovery._index_path(store.root)
    prior_paths = _prior_paths(store.root)
    source_selection = recovery._read_gzip(source_selection_path)
    source_index = recovery._read_json(source_index_path)
    prior_source_reviewed = recovery._read_gzip(prior_paths["prior_source_reviewed"])
    prior_sec_reviewed = recovery._read_gzip(prior_paths["prior_sec_reviewed"])
    if not (
        source_result.get("private_index_sha256")
        == recovery._sha256_file(source_index_path)
        and prior_source_result.get("private_result_sha256")
        == recovery._sha256_file(prior_paths["prior_source_reviewed"])
        and prior_sec_result.get("private_result_sha256")
        == recovery._sha256_file(prior_paths["prior_sec_reviewed"])
        and prior_source_reviewed.get("status") == "REVIEW_COMPLETE"
        and prior_sec_reviewed.get("status") == "REVIEW_COMPLETE"
        and prior_source_reviewed.get("target_outcomes_observed_or_derived") is False
        and prior_sec_reviewed.get("target_outcomes_observed_or_derived") is False
    ):
        raise IssuerChainSemanticsError("prior private semantic evidence differs")
    selection = build_selection(source_selection, source_index)
    private_selection_path = _selection_path(store.root)
    recovery._write_gzip(private_selection_path, selection)
    contract = {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "registered_at": recovery._timestamp_now(),
        "requested_dates": list(source_manifest["requested_dates"]),
        "dataset_payload": {
            "lane": "development",
            "claim_scope": "DEVELOPMENT_ONLY",
            "status": "COLLECTING",
            "evidence_paths": [
                _repo_path(SOURCE_MANIFEST),
                _repo_path(SOURCE_RESULT),
                _repo_path(PRIOR_SOURCE_RESULT),
                _repo_path(PRIOR_SEC_RESULT),
                "CATALYST_SOURCE_RECOVERY.md",
                "PRODUCTION_STRATEGY_VALIDATION.md",
            ],
            "inspected": False,
        },
        "selection_contract": {
            "source_manifest_sha256": source_manifest["manifest_sha256"],
            "source_result_sha256": recovery._sha256_file(SOURCE_RESULT),
            "source_selection_sha256": recovery._sha256_file(source_selection_path),
            "source_index_sha256": recovery._sha256_file(source_index_path),
            "prior_source_result_sha256": recovery._sha256_file(PRIOR_SOURCE_RESULT),
            "prior_sec_result_sha256": recovery._sha256_file(PRIOR_SEC_RESULT),
            **{
                f"{name}_sha256": recovery._sha256_file(path)
                for name, path in prior_paths.items()
            },
            "private_selection_sha256": recovery._sha256_file(private_selection_path),
            "pair_identity_sha256": selection["pair_identity_sha256"],
            "source_relation_sha256": selection["source_relation_sha256"],
            **selection["counts"],
            "symbols_dates_urls_sources_reviews_and_rows_public": False,
        },
        "derivation_contract": {
            "implementation_sha256": recovery._sha256_file(Path(__file__)),
            "issuer_recovery_implementation_sha256": recovery._sha256_file(
                Path(recovery.__file__)
            ),
            "semantic_dependency_sha256": recovery._sha256_file(
                Path(source_semantics.__file__)
            ),
            "python_version": sys.version.split()[0],
            "pypdf_version": source_semantics.pypdf.__version__,
            "parser_version": PARSER_VERSION,
            "source_ownership_rules": {
                "https_official_domain_required": True,
                "official_domain_alone_does_not_establish_target_binding": True,
                "captured_branding_or_footer_evidence_required": True,
            },
            "issuer_binding_rules": {
                "minimum_independent_methods": 2,
                "official_domain_method_required": True,
                "exact_target_cik_or_matching_distinctive_legal_name_required": True,
                "cross_issuer_chain_must_fail_target_binding": True,
            },
            "timestamp_precedence": list(TIMESTAMP_PRECEDENCE),
            "same_day_date_only_rejected": True,
            "metadata_headers_capture_url_and_pdf_creation_not_independent_proof": True,
            "event_taxonomy": list(EVENT_TAXONOMY),
            "financing_conflict_runs_before_positive": True,
            "terminal_precedence": list(TERMINAL_PRECEDENCE),
            "one_terminal_disposition_per_source": True,
            "exact_private_pair_deduplication_across_all_source_passes": True,
            "network_access_allowed": False,
            "target_outcomes_observed_or_derived": False,
            "automatic_strategy_application": False,
        },
    }
    if not (
        prior_source_result.get("verified_positive_pairs") == 3
        and prior_sec_result.get("verified_positive_pairs") == 5
        and source_result.get("terminal_urls") == EXPECTED_SOURCES
    ):
        raise IssuerChainSemanticsError("upstream aggregate counts differ")
    return freeze_dataset_contract(contract, output_root)


def _verify_contract(
    manifest_path: Path, store: HistoricalDayStore
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    manifest = load_frozen_dataset_contract(manifest_path)
    if manifest.get("dataset_id") != DATASET_ID:
        raise IssuerChainSemanticsError("unexpected issuer-chain semantics dataset")
    contract = manifest["selection_contract"]
    if load_frozen_dataset_contract(SOURCE_MANIFEST).get(
        "manifest_sha256"
    ) != contract.get("source_manifest_sha256"):
        raise IssuerChainSemanticsError("source manifest changed after freeze")
    source_selection_path = recovery._selection_path(store.root)
    source_index_path = recovery._index_path(store.root)
    paths = {
        "source_result": SOURCE_RESULT,
        "source_selection": source_selection_path,
        "source_index": source_index_path,
        "prior_source_result": PRIOR_SOURCE_RESULT,
        "prior_sec_result": PRIOR_SEC_RESULT,
        **_prior_paths(store.root),
        "private_selection": _selection_path(store.root),
    }
    for name, path in paths.items():
        if recovery._sha256_file(path) != contract.get(f"{name}_sha256"):
            raise IssuerChainSemanticsError(f"{name} changed after freeze")
    derivation = manifest["derivation_contract"]
    dependencies = {
        "implementation_sha256": Path(__file__),
        "issuer_recovery_implementation_sha256": Path(recovery.__file__),
        "semantic_dependency_sha256": Path(source_semantics.__file__),
    }
    for field, path in dependencies.items():
        if recovery._sha256_file(path) != derivation.get(field):
            raise IssuerChainSemanticsError(f"{field} differs from frozen contract")
    if not (
        sys.version.split()[0] == derivation.get("python_version")
        and source_semantics.pypdf.__version__ == derivation.get("pypdf_version")
        and derivation.get("target_outcomes_observed_or_derived") is False
    ):
        raise IssuerChainSemanticsError("frozen derivation environment differs")
    source_selection = recovery._read_gzip(source_selection_path)
    source_index = recovery._read_json(source_index_path)
    selection = recovery._read_gzip(_selection_path(store.root))
    if build_selection(source_selection, source_index) != selection:
        raise IssuerChainSemanticsError("issuer-chain semantic selection differs")
    return manifest, selection, source_index


def _content_type(record: Mapping[str, Any]) -> str:
    return str(record.get("headers", {}).get("content-type") or "").lower()


def _extract_source(
    store: HistoricalDayStore,
    row: Mapping[str, Any],
    capture_record: Mapping[str, Any],
) -> dict[str, Any]:
    url_hash = str(row["url_sha256"])
    source_path = recovery._response_path(store.root, url_hash)
    body = b""
    if capture_record.get("status") == "RESPONSE_CAPTURED":
        if not source_path.exists() or recovery._sha256_file(
            source_path
        ) != capture_record.get("body_sha256"):
            raise IssuerChainSemanticsError("issuer-chain response hash differs")
        body = source_path.read_bytes()
    content_type = _content_type(capture_record)
    is_pdf = "pdf" in content_type or body.startswith(b"%PDF")
    is_html = "html" in content_type or body.lstrip().startswith(b"<")
    text = ""
    title = ""
    canonical_urls: list[str] = []
    meta_timestamps: list[dict[str, str]] = []
    time_datetimes: list[str] = []
    json_ld_dates: list[str] = []
    pdf_dates: list[str] = []
    pdf_metadata = {
        "creation_date_present": False,
        "modification_date_present": False,
    }
    if capture_record.get("http_status") == 200 and is_html:
        parser = source_semantics._SemanticHtmlParser()
        parser.feed(body.decode("utf-8", errors="replace"))
        title = source_semantics._normalize_text(parser.title_parts, limit=2_000)
        text = source_semantics._normalize_text(parser.text_parts)
        canonical_urls = sorted(set(parser.canonical_urls))
        meta_timestamps = sorted(
            parser.meta_timestamps, key=lambda item: (item["key"], item["value"])
        )
        time_datetimes = sorted(set(parser.time_datetimes))
        json_ld_dates = source_semantics._json_ld_dates(parser.json_ld_parts)
    elif capture_record.get("http_status") == 200 and is_pdf:
        try:
            reader = PdfReader(source_path)
            metadata = reader.metadata or {}
            title = str(metadata.get("/Title") or "")[:2_000]
            text = source_semantics._normalize_text(
                [
                    reader.pages[index].extract_text() or ""
                    for index in range(min(3, len(reader.pages)))
                ]
            )
            pdf_dates = sorted(set(source_semantics.DATE_PATTERN.findall(text)))
            pdf_metadata = {
                "creation_date_present": bool(metadata.get("/CreationDate")),
                "modification_date_present": bool(metadata.get("/ModDate")),
            }
        except Exception as exc:
            raise IssuerChainSemanticsError(
                f"cannot extract issuer-chain PDF {url_hash}"
            ) from exc
    target_name = str(row["issuer_name"])
    symbol = str(row["symbol"])
    target_cik = str(row["cik"])
    lowered = f"{title} {text}".lower()
    target_tokens = [
        token.lower()
        for token in re.findall(r"[A-Za-z0-9]+", target_name)
        if len(token) >= 4
    ]
    diagnostics: list[str] = []
    if capture_record.get("status") != "RESPONSE_CAPTURED":
        diagnostics.append("capture_not_successful")
    if capture_record.get("http_status") != 200:
        diagnostics.append("http_not_successful")
    if not any((meta_timestamps, time_datetimes, json_ld_dates, pdf_dates)):
        diagnostics.append("no_supported_timestamp_candidate")
    if capture_record.get("http_status") == 200 and not (is_html or is_pdf):
        diagnostics.append("unsupported_document_type")
    return {
        "capture_status": capture_record.get("status"),
        "capture_error": capture_record.get("error"),
        "http_status": capture_record.get("http_status"),
        "content_type": capture_record.get("headers", {}).get("content-type"),
        "captured_at": capture_record.get("captured_at"),
        "initial_url": row["url"],
        "final_url": capture_record.get("final_url"),
        "redirect_history": capture_record.get("redirect_history", []),
        "host": urlparse(str(capture_record.get("final_url") or "")).hostname,
        "title": title,
        "canonical_urls": canonical_urls,
        "meta_timestamp_candidates": meta_timestamps,
        "time_datetime_candidates": time_datetimes,
        "json_ld_date_published_candidates": json_ld_dates,
        "pdf_front_text_date_candidates": pdf_dates,
        "pdf_metadata": pdf_metadata,
        "text": text,
        "identity_candidate_matches": {
            "target_issuer_name_token_matches": sorted(
                token for token in set(target_tokens) if token in lowered
            ),
            "point_in_time_symbol_visible": bool(
                re.search(
                    rf"(?<![A-Z0-9]){re.escape(symbol)}(?![A-Z0-9])", title + " " + text
                )
            ),
            "target_cik_visible": target_cik in text.replace("-", "").replace(" ", ""),
        },
        "financing_or_dilution_terms_present": bool(
            source_semantics.FINANCING_PATTERN.search(text)
        ),
        "diagnostic_failures": diagnostics,
        "target_outcomes_observed_or_derived": False,
    }


def build_extraction(
    store: HistoricalDayStore,
    selection: Mapping[str, Any],
    source_index: Mapping[str, Any],
) -> dict[str, Any]:
    captured = source_index.get("records", {})
    rows: list[dict[str, Any]] = []
    for selected in selection.get("rows", []):
        url_hash = str(selected["url_sha256"])
        if url_hash not in captured:
            raise IssuerChainSemanticsError("selected issuer source is missing")
        rows.append(
            {
                **dict(selected),
                "decision_cutoff_et": source_semantics._decision_cutoff_text(
                    str(selected["date"])
                ),
                "extraction": _extract_source(store, selected, captured[url_hash]),
            }
        )
    rows.sort(key=lambda row: str(row["row_id"]))
    if len(rows) != EXPECTED_SOURCES:
        raise IssuerChainSemanticsError("extraction source count differs")
    return {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "status": "EXTRACTION_COMPLETE",
        "counts": dict(selection["counts"]),
        "rows": rows,
        "row_sha256": _sha256_json(rows),
        "source_text_urls_symbols_dates_reviews_and_rows_public": False,
        "target_outcomes_observed_or_derived": False,
    }


def extract(
    *, manifest_path: Path, env_path: Path, public_status_path: Path
) -> dict[str, Any]:
    store = HistoricalDayStore.from_env(env_path)
    manifest, selection, source_index = _verify_contract(manifest_path, store)
    value = build_extraction(store, selection, source_index)
    value["manifest_sha256"] = manifest["manifest_sha256"]
    private_path = _extraction_path(store.root)
    recovery._write_gzip(private_path, value)
    public = {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "status": "EXTRACTION_COMPLETE",
        "counts": value["counts"],
        "private_extraction_sha256": recovery._sha256_file(private_path),
        "review_complete": False,
        "source_text_urls_symbols_dates_reviews_and_rows_public": False,
        "target_outcomes_observed_or_derived": False,
    }
    _write_json(public_status_path, public)
    return public


def build_review_template(extraction: Mapping[str, Any]) -> dict[str, Any]:
    decisions: list[dict[str, Any]] = []
    context: list[dict[str, Any]] = []
    for row in extraction.get("rows", []):
        extracted = row["extraction"]
        decisions.append(
            {
                "row_id": row["row_id"],
                "source_ownership": "UNRESOLVED",
                "ownership_evidence": [],
                "source_issuer_name": None,
                "source_issuer_cik": None,
                "issuer_binding": "UNRESOLVED",
                "issuer_binding_methods": [],
                "relevance": "UNRESOLVED",
                "published_at_utc": None,
                "published_date": None,
                "source_timezone": None,
                "timestamp_precision": "UNRESOLVED",
                "timestamp_evidence_kind": "UNRESOLVED",
                "timestamp_conflict": False,
                "semantics": "UNRESOLVED",
                "semantic_evidence": [],
                "financing_or_dilution_conflict": False,
                "notes": "",
            }
        )
        context.append(
            {
                "row_id": row["row_id"],
                "chain_id": row["chain_id"],
                "source_position": row["source_position"],
                "date": row["date"],
                "symbol": row["symbol"],
                "target_issuer_name": row["issuer_name"],
                "target_cik": row["cik"],
                "issuer_domain": row["issuer_domain"],
                "url": row["url"],
                "final_url": extracted["final_url"],
                "capture_status": extracted["capture_status"],
                "http_status": extracted["http_status"],
                "title": extracted["title"],
                "timestamp_candidates": {
                    "meta": extracted["meta_timestamp_candidates"],
                    "time": extracted["time_datetime_candidates"],
                    "json_ld": extracted["json_ld_date_published_candidates"],
                    "pdf_front_text": extracted["pdf_front_text_date_candidates"],
                },
                "identity_candidate_matches": extracted["identity_candidate_matches"],
                "financing_or_dilution_terms_present": extracted[
                    "financing_or_dilution_terms_present"
                ],
                "diagnostic_failures": extracted["diagnostic_failures"],
                "text_excerpt": extracted["text"][:12_000],
            }
        )
    return {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "manifest_sha256": extraction["manifest_sha256"],
        "review_completed": False,
        "instructions": {
            "outcomes_forbidden": True,
            "official_domain_alone_does_not_establish_target_binding": True,
            "cross_issuer_source_must_remain_unbound": True,
            "same_day_date_only_fails": True,
            "metadata_headers_capture_url_and_pdf_creation_not_independent_proof": True,
            "financing_conflict_precedes_positive": True,
            "terminal_disposition_is_derived_not_entered": True,
        },
        "decisions": decisions,
        "context": context,
        "target_outcomes_observed_or_derived": False,
    }


def _company_tokens(value: str) -> set[str]:
    return {
        token
        for token in re.findall(r"[a-z0-9]+", value.lower())
        if token not in COMPANY_SUFFIXES and token not in GENERIC_COMPANY_TOKENS
    }


def _issuer_identity_matches(
    *, target_name: str, target_cik: str, source_name: str, source_cik: str | None
) -> bool:
    if source_cik is not None:
        return source_cik.lstrip("0") == target_cik.lstrip("0")
    target_tokens = _company_tokens(target_name)
    source_tokens = _company_tokens(source_name)
    return bool(
        target_tokens
        and source_tokens
        and (target_tokens <= source_tokens or source_tokens <= target_tokens)
    )


def _validate_review_decision(
    value: Any, *, row: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any]]:
    if not isinstance(value, Mapping):
        raise IssuerChainSemanticsError("review decision must be an object")
    decision = dict(value)
    extras = sorted(set(decision) - REVIEW_FIELDS)
    missing = sorted(REVIEW_FIELDS - set(decision))
    if extras or missing:
        raise IssuerChainSemanticsError(
            f"review decision fields differ; missing={missing}, extras={extras}"
        )
    source_name = decision.get("source_issuer_name")
    source_cik = decision.get("source_issuer_cik")
    if source_name is not None and (
        not isinstance(source_name, str) or not source_name.strip()
    ):
        raise IssuerChainSemanticsError("source_issuer_name must be null or non-empty")
    if source_cik is not None and (
        not isinstance(source_cik, str) or not source_cik.isdigit()
    ):
        raise IssuerChainSemanticsError("source_issuer_cik must be null or digits")
    base_decision = {field: decision[field] for field in BASE_REVIEW_FIELDS}
    try:
        validated, timestamp = source_semantics.validate_review_decision(
            base_decision, row=row
        )
    except source_semantics.CatalystSourceSemanticsError as exc:
        raise IssuerChainSemanticsError(str(exc)) from exc
    if validated["source_ownership"] == "VERIFIED" and source_name is None:
        raise IssuerChainSemanticsError("verified ownership needs source issuer name")
    if validated["issuer_binding"] == "VERIFIED":
        if validated["source_ownership"] != "VERIFIED":
            raise IssuerChainSemanticsError(
                "verified issuer binding requires verified ownership"
            )
        methods = set(validated["issuer_binding_methods"])
        if "issuer_owned_canonical_domain" not in methods:
            raise IssuerChainSemanticsError(
                "verified issuer binding needs official-domain method"
            )
        if source_name is None or not _issuer_identity_matches(
            target_name=str(row["issuer_name"]),
            target_cik=str(row["cik"]),
            source_name=source_name,
            source_cik=source_cik,
        ):
            raise IssuerChainSemanticsError(
                "source issuer does not match point-in-time target"
            )
        if source_cik is not None and source_cik.lstrip("0") == str(row["cik"]).lstrip(
            "0"
        ):
            if "security_master_cik_to_document_cik" not in methods:
                raise IssuerChainSemanticsError(
                    "exact source CIK binding needs exact-CIK method"
                )
        elif "legal_company_name" not in methods:
            raise IssuerChainSemanticsError(
                "name-based issuer binding needs legal-company-name method"
            )
    return {
        **validated,
        "source_issuer_name": source_name,
        "source_issuer_cik": source_cik,
    }, timestamp


def _validate_review_input(
    value: Mapping[str, Any], extraction: Mapping[str, Any]
) -> dict[str, Mapping[str, Any]]:
    if not (
        value.get("schema_version") == 1
        and value.get("dataset_id") == DATASET_ID
        and value.get("manifest_sha256") == extraction.get("manifest_sha256")
        and value.get("review_completed") is True
        and value.get("target_outcomes_observed_or_derived") is False
    ):
        raise IssuerChainSemanticsError("review input contract is invalid")
    if recovery._contains_outcome_key(
        {
            key: item
            for key, item in value.items()
            if key != "target_outcomes_observed_or_derived"
        }
    ):
        raise IssuerChainSemanticsError("review input contains outcome-shaped field")
    decisions = value.get("decisions")
    if not isinstance(decisions, list) or len(decisions) != EXPECTED_SOURCES:
        raise IssuerChainSemanticsError("review input must contain 12 decisions")
    by_id: dict[str, Mapping[str, Any]] = {}
    for decision in decisions:
        if not isinstance(decision, Mapping):
            raise IssuerChainSemanticsError("review decision must be an object")
        row_id = str(decision.get("row_id"))
        if row_id in by_id:
            raise IssuerChainSemanticsError("review input has duplicate row_id")
        by_id[row_id] = decision
    if set(by_id) != {str(row["row_id"]) for row in extraction["rows"]}:
        raise IssuerChainSemanticsError("review input row set differs")
    return by_id


def _pair_disposition(rows: Sequence[Mapping[str, Any]]) -> str:
    dispositions = {str(row["terminal_disposition"]) for row in rows}
    for disposition in (
        "VERIFIED_CONFLICT",
        "VERIFIED_NEGATIVE_PRIMARY",
        "VERIFIED_POSITIVE_PRIMARY",
    ):
        if disposition in dispositions:
            return disposition
    return min(dispositions, key=TERMINAL_PRECEDENCE.index)


def apply_review(
    extraction: Mapping[str, Any], review_input: Mapping[str, Any]
) -> dict[str, Any]:
    decisions = _validate_review_input(review_input, extraction)
    reviewed_rows: list[dict[str, Any]] = []
    for row in extraction["rows"]:
        decision, timestamp = _validate_review_decision(
            decisions[str(row["row_id"])], row=row
        )
        terminal, diagnostics = source_semantics.terminal_disposition(
            row, decision, timestamp
        )
        reviewed_rows.append(
            {
                **dict(row),
                "review": decision,
                "accepted_timestamp": timestamp,
                "diagnostic_failures": diagnostics,
                "terminal_disposition": terminal,
            }
        )
    reviewed_rows.sort(key=lambda row: str(row["row_id"]))
    source_counts = Counter(str(row["terminal_disposition"]) for row in reviewed_rows)
    by_pair: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in reviewed_rows:
        by_pair[str(row["pair_hash"])].append(row)
    pair_dispositions = {
        pair_hash: _pair_disposition(rows)
        for pair_hash, rows in sorted(by_pair.items())
    }
    pair_counts = Counter(pair_dispositions.values())
    return {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "manifest_sha256": extraction["manifest_sha256"],
        "status": "REVIEW_COMPLETE",
        "counts": dict(extraction["counts"]),
        "terminal_source_counts": {
            key: source_counts.get(key, 0) for key in TERMINAL_PRECEDENCE
        },
        "terminal_pair_counts": {
            key: pair_counts.get(key, 0) for key in TERMINAL_PRECEDENCE
        },
        "pair_dispositions": pair_dispositions,
        "verified_positive_pairs": pair_counts["VERIFIED_POSITIVE_PRIMARY"],
        "rows": reviewed_rows,
        "row_sha256": _sha256_json(reviewed_rows),
        "review_input_sha256": _sha256_json(review_input),
        "source_text_urls_symbols_dates_reviews_and_rows_public": False,
        "target_outcomes_observed_or_derived": False,
    }


def review(
    *,
    manifest_path: Path,
    env_path: Path,
    review_input_path: Path | None,
    review_template_path: Path,
    public_status_path: Path,
) -> dict[str, Any]:
    store = HistoricalDayStore.from_env(env_path)
    manifest, _selection, _source_index = _verify_contract(manifest_path, store)
    extraction = recovery._read_gzip(_extraction_path(store.root))
    if not (
        extraction.get("manifest_sha256") == manifest["manifest_sha256"]
        and extraction.get("status") == "EXTRACTION_COMPLETE"
        and extraction.get("target_outcomes_observed_or_derived") is False
    ):
        raise IssuerChainSemanticsError("private extraction is incomplete or stale")
    if review_input_path is None:
        template = build_review_template(extraction)
        _write_json(review_template_path, template)
        return {
            "schema_version": 1,
            "dataset_id": DATASET_ID,
            "status": "AWAITING_REVIEW",
            "review_template": _repo_path(review_template_path),
            "sources": EXPECTED_SOURCES,
            "target_outcomes_observed_or_derived": False,
        }
    reviewed = apply_review(extraction, _read_json(review_input_path))
    private_path = _reviewed_path(store.root)
    recovery._write_gzip(private_path, reviewed)
    public = {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "status": "REVIEW_COMPLETE",
        "counts": reviewed["counts"],
        "terminal_source_counts": reviewed["terminal_source_counts"],
        "terminal_pair_counts": reviewed["terminal_pair_counts"],
        "verified_positive_pairs": reviewed["verified_positive_pairs"],
        "private_reviewed_result_sha256": recovery._sha256_file(private_path),
        "source_text_urls_symbols_dates_reviews_and_rows_public": False,
        "target_outcomes_observed_or_derived": False,
    }
    _write_json(public_status_path, public)
    return public


def _positive_pair_hashes(reviewed: Mapping[str, Any]) -> set[str]:
    return {
        str(pair_hash)
        for pair_hash, disposition in reviewed.get("pair_dispositions", {}).items()
        if disposition == "VERIFIED_POSITIVE_PRIMARY"
    }


def inspect(
    *, manifest_path: Path, env_path: Path, public_result_path: Path
) -> dict[str, Any]:
    store = HistoricalDayStore.from_env(env_path)
    manifest, selection, source_index = _verify_contract(manifest_path, store)
    extraction = recovery._read_gzip(_extraction_path(store.root))
    reviewed_path = _reviewed_path(store.root)
    reviewed = recovery._read_gzip(reviewed_path)
    rebuilt_extraction = build_extraction(store, selection, source_index)
    rebuilt_extraction["manifest_sha256"] = manifest["manifest_sha256"]
    if extraction != rebuilt_extraction:
        raise IssuerChainSemanticsError("issuer-chain extraction does not rebuild")
    replay_input = {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "review_completed": True,
        "decisions": [row["review"] for row in reviewed["rows"]],
        "target_outcomes_observed_or_derived": False,
    }
    replayed = apply_review(rebuilt_extraction, replay_input)
    for field in (
        "terminal_source_counts",
        "terminal_pair_counts",
        "pair_dispositions",
        "verified_positive_pairs",
        "rows",
        "row_sha256",
    ):
        if reviewed.get(field) != replayed.get(field):
            raise IssuerChainSemanticsError(f"review inspection mismatch: {field}")
    prior_source = recovery._read_gzip(source_semantics._reviewed_path(store.root))
    prior_sec = recovery._read_gzip(sec_semantics._reviewed_path(store.root))
    combined_positive_hashes = (
        _positive_pair_hashes(prior_source)
        | _positive_pair_hashes(prior_sec)
        | _positive_pair_hashes(reviewed)
    )
    combined_positive = len(combined_positive_hashes)
    result = {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "status": "READY",
        "inspected": True,
        "claim_scope": "DEVELOPMENT_ONLY",
        "selection_counts": dict(reviewed["counts"]),
        "terminal_source_counts": dict(reviewed["terminal_source_counts"]),
        "terminal_pair_counts": dict(reviewed["terminal_pair_counts"]),
        "issuer_chain_verified_positive_pairs": reviewed["verified_positive_pairs"],
        "combined_exact_deduplicated_verified_positive_pairs": combined_positive,
        "minimum_positive_capacity": 20,
        "capacity_gate_passed": combined_positive >= 20,
        "outcome_contract_permitted": combined_positive >= 20,
        "next_phase": "DEVELOPMENT_ACQUISITION",
        "next_action": (
            "Freeze an outcome contract only after at least 20 complete unchanged-v3 "
            "non-return survivors are independently verified."
            if combined_positive >= 20
            else "Freeze the next exact disjoint 100-session tranche before market-data "
            "or catalyst collection; source recovery is exhausted for this corpus."
        ),
        "private_result_sha256": recovery._sha256_file(reviewed_path),
        "inspection": {
            "selection_rebuilt": True,
            "extraction_rebuilt": True,
            "review_replayed": True,
            "terminal_counts_rebuilt": True,
            "accepted_timestamps_rebuilt": True,
            "prior_positive_pair_hashes_reconciled_privately": True,
            "one_terminal_reason_per_source": sum(
                int(value) for value in reviewed["terminal_source_counts"].values()
            )
            == EXPECTED_SOURCES,
        },
        "claim_boundary": (
            "Primary-source ownership, target binding, causal timing, relevance, and "
            "direction only. Returns and target-session outcomes remain inaccessible."
        ),
        "production_rule_change_earned": False,
        "source_text_urls_symbols_dates_reviews_and_rows_public": False,
        "target_outcomes_observed_or_derived": False,
    }
    _write_json(public_result_path, result)
    return result


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("freeze", "extract", "review", "inspect"))
    parser.add_argument("--manifest", type=Path)
    parser.add_argument("--env-file", type=Path, default=PROJECT_ROOT / ".env")
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--public-status", type=Path, default=DEFAULT_PUBLIC_STATUS)
    parser.add_argument("--public-result", type=Path, default=DEFAULT_PUBLIC_RESULT)
    parser.add_argument("--review-input", type=Path)
    parser.add_argument("--review-template", type=Path, default=DEFAULT_REVIEW_TEMPLATE)
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
            raise IssuerChainSemanticsError("--manifest is required")
        elif args.command == "extract":
            output = extract(
                manifest_path=args.manifest,
                env_path=args.env_file,
                public_status_path=args.public_status,
            )
        elif args.command == "review":
            output = review(
                manifest_path=args.manifest,
                env_path=args.env_file,
                review_input_path=args.review_input,
                review_template_path=args.review_template,
                public_status_path=args.public_status,
            )
        else:
            output = inspect(
                manifest_path=args.manifest,
                env_path=args.env_file,
                public_result_path=args.public_result,
            )
    except (
        IssuerChainSemanticsError,
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

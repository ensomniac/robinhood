"""Freeze, extract, review, and inspect catalyst source semantics.

The pipeline is outcome blind and network free. Exact rows, source text, URLs,
symbols, dates, identities, and review decisions stay outside Git.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime, time
from html.parser import HTMLParser
from pathlib import Path
from typing import Any
from urllib.parse import urlparse
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import pypdf
from pypdf import PdfReader

import catalyst_pdf_source_profile as pdf_profile
import catalyst_primary_source_capture as capture
import catalyst_source_pair_readiness as readiness
import catalyst_source_profile as source_profile
import selected_candidate_fidelity_expansion as fidelity
from historical_store import HistoricalDayStore
from learning_data import (
    LearningDataError,
    freeze_dataset_contract,
    load_frozen_dataset_contract,
)


PROJECT_ROOT = Path(__file__).resolve().parent
DATASET_ID = "dataset-catalyst-source-semantics-2026-07-19-expansion-v1"
EXPECTED_PAIRS = 33
EXPECTED_DOCUMENTS = 33
EXPECTED_JOINS = 38
PARSER_VERSION = "source-semantics-parser-v1"
SOURCE_PAIR_MANIFEST = (
    PROJECT_ROOT
    / "historical_batches"
    / "catalyst_source_pair_readiness"
    / "manifests"
    / (
        "dataset-catalyst-source-pair-readiness-2026-07-19-expansion-v1-"
        "7ace3b399e8e6320325c755001cb3b31217d77fb534f2ccd030d9ecd568bbd0b.json"
    )
)
SOURCE_PAIR_RESULT = (
    PROJECT_ROOT / "research_results" / "2026-07-19-catalyst-source-pair-readiness.json"
)
SOURCE_PDF_MANIFEST = (
    PROJECT_ROOT
    / "historical_batches"
    / "catalyst_pdf_source_profile"
    / "manifests"
    / (
        "dataset-catalyst-pdf-source-profile-2026-07-19-expansion-v1-"
        "b1f2bff3bbc816151676a940e6547e9526a1539398c2a4afae48664896162e4c.json"
    )
)
SOURCE_PDF_RESULT = (
    PROJECT_ROOT / "research_results" / "2026-07-19-catalyst-pdf-source-profile.json"
)
SOURCE_IDENTITY_MANIFEST = (
    PROJECT_ROOT
    / "historical_batches"
    / "selected_candidate_fidelity_expansion"
    / "manifests"
    / (
        "dataset-selected-candidate-fidelity-2026-07-19-expansion-v1-"
        "ee9ba6f3a2d48f3337ee654c545ea3c02287e23f4a4db5a3fb86adf1b0c780ce.json"
    )
)
SOURCE_IDENTITY_RESULT = (
    PROJECT_ROOT
    / "research_results"
    / "2026-07-19-selected-candidate-fidelity-expansion.json"
)
DEFAULT_OUTPUT_ROOT = (
    PROJECT_ROOT / "historical_batches" / "catalyst_source_semantics" / "manifests"
)
DEFAULT_PUBLIC_STATUS = (
    PROJECT_ROOT
    / "historical_batches"
    / "catalyst_source_semantics"
    / "collection-status.json"
)
DEFAULT_PUBLIC_RESULT = (
    PROJECT_ROOT / "research_results" / "2026-07-19-catalyst-source-semantics.json"
)
DEFAULT_REVIEW_TEMPLATE = (
    PROJECT_ROOT
    / "learning_runs"
    / "production_validation"
    / "source-semantics-review.json"
)

TIMESTAMP_PRECEDENCE = (
    "SOURCE_CONTROLLED_EXPLICIT_DATETIME",
    "JSON_LD_AND_VISIBLE_DATETIME",
    "META_AND_VISIBLE_DATETIME",
    "CONTEXTUAL_TIME_DATETIME",
    "PDF_DATELINE_AND_REFERRER_CORROBORATION",
)
EVENT_TAXONOMY = (
    "DIRECT_POSITIVE",
    "DIRECT_NEGATIVE",
    "FINANCING_OR_DILUTION_CONFLICT",
    "MIXED_OR_CONTRADICTORY",
    "NON_MATERIAL_OR_CONTEXT_ONLY",
    "UNRESOLVED",
)
TERMINAL_PRECEDENCE = (
    "CAPTURE_TRANSPORT_ERROR",
    "CAPTURE_FORBIDDEN",
    "CAPTURE_NOT_FOUND",
    "CAPTURE_HTTP_ERROR",
    "SOURCE_OWNERSHIP_UNRESOLVED",
    "ISSUER_BINDING_UNRESOLVED",
    "PRIMARY_SOURCE_IRRELEVANT",
    "TIMESTAMP_MISSING",
    "TIMESTAMP_CONFLICT",
    "SAME_DAY_TIME_UNRESOLVED",
    "PUBLISHED_AFTER_0935",
    "DOCUMENT_SEMANTICS_UNRESOLVED",
    "NON_MATERIAL_OR_CONTEXT_ONLY",
    "VERIFIED_CONFLICT",
    "VERIFIED_NEGATIVE_PRIMARY",
    "VERIFIED_POSITIVE_PRIMARY",
)
TIMESTAMP_EVIDENCE_KINDS = {*TIMESTAMP_PRECEDENCE, "UNRESOLVED"}
ISSUER_BINDING_METHODS = {
    "legal_company_name",
    "point_in_time_ticker",
    "security_master_cik_to_document_cik",
    "issuer_owned_canonical_domain",
    "issuer_controlled_footer_or_identity",
    "unique_subsidiary_or_product_binding",
    "exchange_symbol_and_listing_identity",
}
REVIEW_FIELDS = {
    "row_id",
    "source_ownership",
    "ownership_evidence",
    "issuer_binding",
    "issuer_binding_methods",
    "relevance",
    "published_at_utc",
    "published_date",
    "source_timezone",
    "timestamp_precision",
    "timestamp_evidence_kind",
    "timestamp_conflict",
    "semantics",
    "semantic_evidence",
    "financing_or_dilution_conflict",
    "notes",
}
OUTCOME_KEY_PATTERN = re.compile(
    r"(?i)(?:outcome|return|net_r|profit|loss|target_hit|stop_hit|future_price)"
)
FINANCING_PATTERN = re.compile(
    r"(?i)\b(?:at-the-market|\bATM\b|shelf registration|public offering|"
    r"registered direct|private placement|convertible note|convertible senior|"
    r"dilution|going concern|liquidity warning|warrant exercise|secondary offering)\b"
)
DATE_PATTERN = re.compile(
    r"\b(?:20\d{2}-[01]\d-[0-3]\d|(?:jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|"
    r"apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:tember)?|"
    r"oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\s+\d{1,2},?\s+20\d{2})\b",
    re.IGNORECASE,
)


class CatalystSourceSemanticsError(RuntimeError):
    """The source-semantics contract, review, or evidence is unsafe."""


class _SemanticHtmlParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.title_depth = 0
        self.script_type = ""
        self.script_parts: list[str] = []
        self.title_parts: list[str] = []
        self.text_parts: list[str] = []
        self.canonical_urls: list[str] = []
        self.meta_timestamps: list[dict[str, str]] = []
        self.time_datetimes: list[str] = []
        self.json_ld_parts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        normalized = {str(key).lower(): str(value or "") for key, value in attrs}
        lowered = tag.lower()
        if lowered == "title":
            self.title_depth += 1
        elif lowered == "link" and "canonical" in normalized.get("rel", "").lower():
            if normalized.get("href"):
                self.canonical_urls.append(normalized["href"])
        elif lowered == "time" and normalized.get("datetime"):
            self.time_datetimes.append(normalized["datetime"])
        elif lowered == "meta":
            key = (normalized.get("property") or normalized.get("name") or "").lower()
            if key in source_profile.PUBLISHED_META_KEYS and normalized.get("content"):
                self.meta_timestamps.append(
                    {"key": key, "value": normalized["content"]}
                )
        elif lowered == "script":
            self.script_type = normalized.get("type", "").lower()
            self.script_parts = []

    def handle_endtag(self, tag: str) -> None:
        lowered = tag.lower()
        if lowered == "title" and self.title_depth:
            self.title_depth -= 1
        elif lowered == "script":
            if "ld+json" in self.script_type:
                self.json_ld_parts.append("".join(self.script_parts))
            self.script_type = ""
            self.script_parts = []

    def handle_data(self, data: str) -> None:
        if self.title_depth:
            self.title_parts.append(data)
        if self.script_type:
            self.script_parts.append(data)
        else:
            self.text_parts.append(data)


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
        raise CatalystSourceSemanticsError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise CatalystSourceSemanticsError(f"{path} must contain an object")
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
        raise CatalystSourceSemanticsError(
            f"public path must be repository-relative: {path}"
        ) from exc


def _private_root(store_root: Path) -> Path:
    return store_root / "_derived" / "catalyst_source_semantics" / DATASET_ID


def _selection_path(store_root: Path) -> Path:
    return _private_root(store_root) / "frozen-selection.json.gz"


def _extraction_path(store_root: Path) -> Path:
    return _private_root(store_root) / "extraction.json.gz"


def _reviewed_path(store_root: Path) -> Path:
    return _private_root(store_root) / "reviewed-result.json.gz"


def _input_paths(store_root: Path) -> dict[str, Path]:
    return {
        "pair_readiness": readiness._private_path(store_root),
        "capture_selection": capture._selection_path(store_root),
        "capture_index": capture._index_path(store_root),
        "source_profile": source_profile._private_path(store_root),
        "pdf_profile": pdf_profile._private_path(store_root),
        "identity_map": fidelity._selection_path(store_root, fidelity.DATASET_ID),
    }


def _verify_ready_result(path: Path) -> None:
    value = _read_json(path)
    if value.get("status") != "READY" or value.get("inspected") is not True:
        raise CatalystSourceSemanticsError(f"upstream result is not READY: {path.name}")
    if value.get("target_outcomes_observed_or_derived") is not False:
        raise CatalystSourceSemanticsError(
            f"upstream result outcome lock is open: {path.name}"
        )


def _content_type(record: Mapping[str, Any]) -> str:
    return str(record.get("headers", {}).get("content-type") or "").lower()


def _has_html_timestamp(profile: Mapping[str, Any]) -> bool:
    html = profile.get("html") or {}
    return bool(
        html.get("meta_timestamp_candidates")
        or html.get("time_datetime_candidates")
        or html.get("json_ld_date_published_present")
    )


def _source_type(record: Mapping[str, Any]) -> str:
    final_host = (urlparse(str(record.get("final_url") or "")).hostname or "").lower()
    category = str(record.get("category") or "")
    if final_host == "sec.gov" or final_host.endswith(".sec.gov"):
        return "SEC"
    if category == "EXCHANGE":
        return "EXCHANGE"
    if final_host.endswith(".gov"):
        return "AUTHORITY"
    return "ISSUER_HOST"


def build_selection(
    pair_readiness: Mapping[str, Any],
    capture_selection: Mapping[str, Any],
    capture_index: Mapping[str, Any],
    profiles: Mapping[str, Any],
    pdf_profiles: Mapping[str, Any],
    identity_map: Mapping[str, Any],
) -> dict[str, Any]:
    selected_by_hash = {
        str(row["url_sha256"]): row for row in capture_selection.get("records", [])
    }
    captured = capture_index.get("records", {})
    profile_rows = profiles.get("records", {})
    pdf_rows = pdf_profiles.get("records", {})
    identities: dict[tuple[str, str], Mapping[str, Any]] = {}
    for row in identity_map.get("pairs", []):
        key = (str(row.get("date")), str(row.get("symbol")))
        if key in identities:
            raise CatalystSourceSemanticsError("identity map has duplicate pair")
        identities[key] = row
    joins: list[dict[str, Any]] = []
    pair_keys: set[tuple[str, str]] = set()
    document_hashes: set[str] = set()
    for pair in pair_readiness.get("pair_records", []):
        qualifying: list[str] = []
        for source_hash in pair.get("route_url_hashes", []):
            response = captured.get(source_hash, {})
            profile = profile_rows.get(source_hash, {})
            is_pdf = response.get("http_status") == 200 and "pdf" in _content_type(
                response
            )
            is_html_timestamp = (
                response.get("http_status") == 200
                and "html" in _content_type(response)
                and _has_html_timestamp(profile)
            )
            if is_pdf or is_html_timestamp:
                if is_pdf and source_hash not in pdf_rows:
                    raise CatalystSourceSemanticsError(
                        "selected PDF lacks frozen profile"
                    )
                qualifying.append(str(source_hash))
        if not qualifying:
            continue
        pair_key = (str(pair.get("date")), str(pair.get("symbol")))
        identity = identities.get(pair_key)
        if identity is None:
            raise CatalystSourceSemanticsError(
                "selected pair lacks point-in-time identity"
            )
        pair_keys.add(pair_key)
        for source_hash in sorted(qualifying):
            response = captured[source_hash]
            selected = selected_by_hash[source_hash]
            document_hash = str(response.get("body_sha256") or source_hash)
            document_hashes.add(document_hash)
            row_id = (
                "join-" + _sha256_json([pair_key[0], pair_key[1], source_hash])[:24]
            )
            joins.append(
                {
                    "row_id": row_id,
                    "date": pair_key[0],
                    "symbol": pair_key[1],
                    "instrument_id": str(identity.get("instrument_id")),
                    "primary_exchange": str(identity.get("primary_exchange")),
                    "cik": str(identity.get("cik")),
                    "issuer_name": str(identity.get("issuer_name")),
                    "identity_match_basis": str(identity.get("cik_match_basis")),
                    "source_hash": document_hash,
                    "url_sha256": source_hash,
                    "url": str(selected.get("url")),
                    "final_url": str(response.get("final_url") or ""),
                    "source_type": _source_type(response),
                    "route_category": str(selected.get("category")),
                }
            )
    joins.sort(key=lambda row: str(row["row_id"]))
    if not (
        len(pair_keys) == EXPECTED_PAIRS
        and len(document_hashes) == EXPECTED_DOCUMENTS
        and len(joins) == EXPECTED_JOINS
    ):
        raise CatalystSourceSemanticsError(
            "source semantics selection is not exactly 33 pairs, 33 documents, and 38 joins"
        )
    return {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "status": "FROZEN_SELECTION",
        "counts": {
            "pairs": len(pair_keys),
            "documents": len(document_hashes),
            "pair_source_joins": len(joins),
        },
        "joins": joins,
        "pair_identity_sha256": _sha256_json(
            sorted(
                (
                    row["date"],
                    row["symbol"],
                    row["instrument_id"],
                    row["primary_exchange"],
                    row["cik"],
                )
                for row in joins
            )
        ),
        "source_document_sha256": _sha256_json(sorted(document_hashes)),
        "pair_source_join_sha256": _sha256_json(joins),
        "symbols_urls_sources_and_rows_public": False,
        "target_outcomes_observed_or_derived": False,
    }


def freeze_inputs(*, env_path: Path, output_root: Path) -> tuple[Path, dict[str, Any]]:
    for path in (SOURCE_PAIR_RESULT, SOURCE_PDF_RESULT, SOURCE_IDENTITY_RESULT):
        _verify_ready_result(path)
    pair_manifest = load_frozen_dataset_contract(SOURCE_PAIR_MANIFEST)
    pdf_manifest = load_frozen_dataset_contract(SOURCE_PDF_MANIFEST)
    identity_manifest = load_frozen_dataset_contract(SOURCE_IDENTITY_MANIFEST)
    store = HistoricalDayStore.from_env(env_path)
    paths = _input_paths(store.root)
    selection = build_selection(
        capture._read_gzip(paths["pair_readiness"]),
        capture._read_gzip(paths["capture_selection"]),
        capture._read_json(paths["capture_index"]),
        capture._read_gzip(paths["source_profile"]),
        capture._read_gzip(paths["pdf_profile"]),
        fidelity._read_gzip(paths["identity_map"]),
    )
    private_selection_path = _selection_path(store.root)
    capture._write_gzip(private_selection_path, selection)
    contract = {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "registered_at": capture._timestamp_now(),
        "requested_dates": list(pair_manifest["requested_dates"]),
        "dataset_payload": {
            "lane": "development",
            "claim_scope": "DEVELOPMENT_ONLY",
            "status": "COLLECTING",
            "evidence_paths": [
                _repo_path(SOURCE_PAIR_MANIFEST),
                _repo_path(SOURCE_PAIR_RESULT),
                _repo_path(SOURCE_PDF_MANIFEST),
                _repo_path(SOURCE_PDF_RESULT),
                _repo_path(SOURCE_IDENTITY_MANIFEST),
                _repo_path(SOURCE_IDENTITY_RESULT),
                "CATALYST_SOURCE_SEMANTICS_PLAN.md",
                "PRODUCTION_STRATEGY_VALIDATION.md",
            ],
            "inspected": False,
        },
        "selection_contract": {
            "pair_readiness_manifest_sha256": pair_manifest["manifest_sha256"],
            "pdf_profile_manifest_sha256": pdf_manifest["manifest_sha256"],
            "identity_manifest_sha256": identity_manifest["manifest_sha256"],
            **{
                f"{name}_sha256": capture._sha256_file(path)
                for name, path in paths.items()
            },
            "frozen_private_selection_sha256": capture._sha256_file(
                private_selection_path
            ),
            "pair_identity_sha256": selection["pair_identity_sha256"],
            "source_document_sha256": selection["source_document_sha256"],
            "pair_source_join_sha256": selection["pair_source_join_sha256"],
            "pairs": EXPECTED_PAIRS,
            "source_documents": EXPECTED_DOCUMENTS,
            "pair_source_joins": EXPECTED_JOINS,
            "symbols_urls_sources_and_rows_public": False,
        },
        "derivation_contract": {
            "implementation_sha256": capture._sha256_file(Path(__file__)),
            "python_version": sys.version.split()[0],
            "pypdf_version": pypdf.__version__,
            "parser_version": PARSER_VERSION,
            "source_ownership_rules": {
                "sec_requires_https_sec_host_and_accession_document_identity": True,
                "authority_host_does_not_establish_issuer_binding": True,
                "exchange_requires_exact_listing_identity": True,
                "issuer_host_requires_two_binding_methods": True,
                "generic_infrastructure_host_is_not_ownership": True,
            },
            "timestamp_precedence": list(TIMESTAMP_PRECEDENCE),
            "same_day_date_only_rejected": True,
            "metadata_headers_capture_url_and_pdf_creation_not_independent_proof": True,
            "event_taxonomy": list(EVENT_TAXONOMY),
            "financing_conflict_runs_before_positive": True,
            "terminal_precedence": list(TERMINAL_PRECEDENCE),
            "one_terminal_disposition_per_join": True,
            "network_access_allowed": False,
            "target_outcomes_observed_or_derived": False,
            "automatic_strategy_application": False,
        },
    }
    return freeze_dataset_contract(contract, output_root)


def _verify_contract(
    manifest_path: Path, store: HistoricalDayStore
) -> tuple[dict[str, Any], dict[str, Path], dict[str, Any]]:
    manifest = load_frozen_dataset_contract(manifest_path)
    if manifest.get("dataset_id") != DATASET_ID:
        raise CatalystSourceSemanticsError("unexpected source-semantics dataset")
    paths = _input_paths(store.root)
    selection_contract = manifest["selection_contract"]
    for name, path in paths.items():
        if capture._sha256_file(path) != selection_contract.get(f"{name}_sha256"):
            raise CatalystSourceSemanticsError(f"{name} changed after freeze")
    private_selection_path = _selection_path(store.root)
    if capture._sha256_file(private_selection_path) != selection_contract.get(
        "frozen_private_selection_sha256"
    ):
        raise CatalystSourceSemanticsError(
            "private source selection changed after freeze"
        )
    derivation = manifest["derivation_contract"]
    if capture._sha256_file(Path(__file__)) != derivation.get("implementation_sha256"):
        raise CatalystSourceSemanticsError(
            "implementation differs from frozen contract"
        )
    if sys.version.split()[0] != derivation.get("python_version"):
        raise CatalystSourceSemanticsError(
            "Python version differs from frozen contract"
        )
    if pypdf.__version__ != derivation.get("pypdf_version"):
        raise CatalystSourceSemanticsError("pypdf version differs from frozen contract")
    if derivation.get("target_outcomes_observed_or_derived") is not False:
        raise CatalystSourceSemanticsError("frozen outcome lock is not closed")
    selection = capture._read_gzip(private_selection_path)
    if selection.get("target_outcomes_observed_or_derived") is not False:
        raise CatalystSourceSemanticsError(
            "private selection outcome lock is not closed"
        )
    return manifest, paths, selection


def _normalize_text(parts: Sequence[str], *, limit: int = 100_000) -> str:
    value = re.sub(r"\s+", " ", " ".join(parts)).strip()
    return value[:limit]


def _decision_cutoff_text(trading_day: str) -> str:
    cutoff = datetime.combine(
        date.fromisoformat(trading_day),
        time(9, 35),
        tzinfo=ZoneInfo("America/New_York"),
    )
    return cutoff.isoformat()


def _json_ld_dates(parts: Sequence[str]) -> list[str]:
    values: set[str] = set()

    def visit(value: Any) -> None:
        if isinstance(value, Mapping):
            for key, child in value.items():
                if str(key).lower() == "datepublished" and isinstance(child, str):
                    values.add(child.strip())
                visit(child)
        elif isinstance(value, list):
            for child in value:
                visit(child)

    for raw in parts:
        try:
            visit(json.loads(raw))
        except json.JSONDecodeError:
            continue
    return sorted(value for value in values if value)


def _document_extraction(
    store: HistoricalDayStore,
    join: Mapping[str, Any],
    capture_record: Mapping[str, Any],
) -> dict[str, Any]:
    source_hash = str(join["url_sha256"])
    source_path = capture._response_path(store.root, source_hash)
    if capture_record.get("status") == "RESPONSE_CAPTURED":
        if capture._sha256_file(source_path) != capture_record.get("body_sha256"):
            raise CatalystSourceSemanticsError("captured source body hash mismatch")
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
    if (
        capture_record.get("status") == "RESPONSE_CAPTURED"
        and capture_record.get("http_status") == 200
    ):
        if "html" in _content_type(capture_record):
            parser = _SemanticHtmlParser()
            parser.feed(source_path.read_bytes().decode("utf-8", errors="replace"))
            title = _normalize_text(parser.title_parts, limit=2_000)
            text = _normalize_text(parser.text_parts)
            canonical_urls = sorted(set(parser.canonical_urls))
            meta_timestamps = sorted(
                parser.meta_timestamps, key=lambda row: (row["key"], row["value"])
            )
            time_datetimes = sorted(set(parser.time_datetimes))
            json_ld_dates = _json_ld_dates(parser.json_ld_parts)
        elif "pdf" in _content_type(capture_record):
            try:
                reader = PdfReader(source_path)
                metadata = reader.metadata or {}
                title = str(metadata.get("/Title") or "")[:2_000]
                text = _normalize_text(
                    [
                        reader.pages[index].extract_text() or ""
                        for index in range(min(3, len(reader.pages)))
                    ]
                )
                pdf_dates = sorted(set(DATE_PATTERN.findall(text)))
                pdf_metadata = {
                    "creation_date_present": bool(metadata.get("/CreationDate")),
                    "modification_date_present": bool(metadata.get("/ModDate")),
                }
            except Exception as exc:
                raise CatalystSourceSemanticsError(
                    f"cannot extract selected PDF {source_hash}"
                ) from exc
    issuer_name = str(join["issuer_name"])
    symbol = str(join["symbol"])
    cik = str(join["cik"])
    lowered = f"{title} {text}".lower()
    issuer_tokens = [
        token.lower()
        for token in re.findall(r"[A-Za-z0-9]+", issuer_name)
        if len(token) >= 4
    ]
    return {
        "capture_status": capture_record.get("status"),
        "http_status": capture_record.get("http_status"),
        "content_type": capture_record.get("headers", {}).get("content-type"),
        "captured_at": capture_record.get("captured_at"),
        "initial_url": join["url"],
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
            "issuer_name_token_matches": sorted(
                token for token in set(issuer_tokens) if token in lowered
            ),
            "point_in_time_symbol_visible": bool(
                re.search(
                    rf"(?<![A-Z0-9]){re.escape(symbol)}(?![A-Z0-9])", title + " " + text
                )
            ),
            "cik_visible": cik in text.replace("-", "").replace(" ", ""),
        },
        "financing_or_dilution_terms_present": bool(FINANCING_PATTERN.search(text)),
        "diagnostic_failures": [],
        "target_outcomes_observed_or_derived": False,
    }


def build_extraction(
    store: HistoricalDayStore,
    selection: Mapping[str, Any],
    capture_index: Mapping[str, Any],
) -> dict[str, Any]:
    captured = capture_index.get("records", {})
    document_cache: dict[str, dict[str, Any]] = {}
    rows: list[dict[str, Any]] = []
    for join in selection.get("joins", []):
        source_hash = str(join["url_sha256"])
        if source_hash not in captured:
            raise CatalystSourceSemanticsError("frozen join source is missing")
        if source_hash not in document_cache:
            document_cache[source_hash] = _document_extraction(
                store, join, captured[source_hash]
            )
        extraction = document_cache[source_hash]
        diagnostics: list[str] = []
        if extraction["capture_status"] != "RESPONSE_CAPTURED":
            diagnostics.append("capture_not_successful")
        if extraction["http_status"] != 200:
            diagnostics.append("http_not_successful")
        if not any(
            (
                extraction["meta_timestamp_candidates"],
                extraction["time_datetime_candidates"],
                extraction["json_ld_date_published_candidates"],
                extraction["pdf_front_text_date_candidates"],
            )
        ):
            diagnostics.append("no_supported_timestamp_candidate")
        rows.append(
            {
                **dict(join),
                "decision_cutoff_et": _decision_cutoff_text(str(join["date"])),
                "extraction": {**extraction, "diagnostic_failures": diagnostics},
            }
        )
    rows.sort(key=lambda row: str(row["row_id"]))
    if len(rows) != EXPECTED_JOINS:
        raise CatalystSourceSemanticsError("extraction does not contain 38 joins")
    return {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "status": "EXTRACTION_COMPLETE",
        "counts": dict(selection["counts"]),
        "rows": rows,
        "row_sha256": _sha256_json(rows),
        "source_text_urls_symbols_dates_and_rows_public": False,
        "target_outcomes_observed_or_derived": False,
    }


def extract(
    *, manifest_path: Path, env_path: Path, public_status_path: Path
) -> dict[str, Any]:
    store = HistoricalDayStore.from_env(env_path)
    manifest, paths, selection = _verify_contract(manifest_path, store)
    extraction = build_extraction(
        store, selection, capture._read_json(paths["capture_index"])
    )
    extraction["manifest_sha256"] = manifest["manifest_sha256"]
    private_path = _extraction_path(store.root)
    capture._write_gzip(private_path, extraction)
    public = {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "status": "EXTRACTION_COMPLETE",
        "counts": extraction["counts"],
        "private_extraction_sha256": capture._sha256_file(private_path),
        "review_complete": False,
        "source_text_urls_symbols_dates_and_rows_public": False,
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
                "date": row["date"],
                "symbol": row["symbol"],
                "issuer_name": row["issuer_name"],
                "cik": row["cik"],
                "source_type": row["source_type"],
                "url": row["url"],
                "final_url": row["final_url"],
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
            "same_day_date_only_fails": True,
            "issuer_host_needs_two_binding_methods": True,
            "financing_conflict_precedes_positive": True,
            "terminal_disposition_is_derived_not_entered": True,
        },
        "decisions": decisions,
        "context": context,
        "target_outcomes_observed_or_derived": False,
    }


def _strings(value: Any, field: str) -> list[str]:
    if not isinstance(value, list) or any(
        not isinstance(item, str) or not item.strip() for item in value
    ):
        raise CatalystSourceSemanticsError(
            f"{field} must be an array of non-empty strings"
        )
    return [str(item).strip() for item in value]


def _review_timestamp(decision: Mapping[str, Any], trading_day: str) -> dict[str, Any]:
    precision = decision.get("timestamp_precision")
    conflict = decision.get("timestamp_conflict")
    if not isinstance(conflict, bool):
        raise CatalystSourceSemanticsError("timestamp_conflict must be boolean")
    if conflict:
        return {"status": "CONFLICT", "accepted_utc": None}
    evidence_kind = decision.get("timestamp_evidence_kind")
    if evidence_kind not in TIMESTAMP_EVIDENCE_KINDS:
        raise CatalystSourceSemanticsError("invalid timestamp_evidence_kind")
    if precision == "UNRESOLVED":
        if (
            decision.get("published_at_utc") is not None
            or decision.get("published_date") is not None
        ):
            raise CatalystSourceSemanticsError(
                "unresolved timestamp cannot retain an accepted value"
            )
        return {"status": "MISSING", "accepted_utc": None}
    if evidence_kind == "UNRESOLVED":
        raise CatalystSourceSemanticsError(
            "accepted timestamp needs a supported evidence kind"
        )
    timezone_name = decision.get("source_timezone")
    if not isinstance(timezone_name, str) or not timezone_name.strip():
        raise CatalystSourceSemanticsError("accepted timestamp needs source_timezone")
    try:
        source_zone = ZoneInfo(timezone_name)
    except ZoneInfoNotFoundError as exc:
        raise CatalystSourceSemanticsError("unknown source_timezone") from exc
    target_day = date.fromisoformat(trading_day)
    cutoff = datetime.combine(
        target_day, time(9, 35), tzinfo=ZoneInfo("America/New_York")
    ).astimezone(UTC)
    if precision == "DATETIME":
        raw = decision.get("published_at_utc")
        if not isinstance(raw, str):
            raise CatalystSourceSemanticsError("DATETIME needs published_at_utc")
        try:
            accepted = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError as exc:
            raise CatalystSourceSemanticsError("published_at_utc is invalid") from exc
        if accepted.tzinfo is None or accepted.utcoffset() != UTC.utcoffset(accepted):
            raise CatalystSourceSemanticsError(
                "published_at_utc must be explicitly UTC"
            )
        accepted = accepted.astimezone(UTC)
    elif precision == "DATE":
        raw_date = decision.get("published_date")
        if not isinstance(raw_date, str):
            raise CatalystSourceSemanticsError("DATE needs published_date")
        published_day = date.fromisoformat(raw_date)
        if published_day == target_day:
            return {"status": "SAME_DAY_TIME_UNRESOLVED", "accepted_utc": None}
        accepted = datetime.combine(
            published_day, time(23, 59, 59, 999999), tzinfo=source_zone
        ).astimezone(UTC)
    else:
        raise CatalystSourceSemanticsError("invalid timestamp_precision")
    return {
        "status": "ACCEPTED" if accepted <= cutoff else "AFTER_CUTOFF",
        "accepted_utc": accepted.isoformat(),
    }


def validate_review_decision(
    value: Any, *, row: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any]]:
    if not isinstance(value, Mapping):
        raise CatalystSourceSemanticsError("review decision must be an object")
    decision = dict(value)
    extras = sorted(set(decision) - REVIEW_FIELDS)
    missing = sorted(REVIEW_FIELDS - set(decision))
    if extras or missing:
        raise CatalystSourceSemanticsError(
            f"review decision fields differ; missing={missing}, extras={extras}"
        )
    if decision.get("row_id") != row.get("row_id"):
        raise CatalystSourceSemanticsError("review row_id differs from extraction")
    if decision.get("source_ownership") not in {"VERIFIED", "UNRESOLVED"}:
        raise CatalystSourceSemanticsError("invalid source_ownership")
    ownership_evidence = _strings(decision["ownership_evidence"], "ownership_evidence")
    if decision["source_ownership"] == "VERIFIED" and not ownership_evidence:
        raise CatalystSourceSemanticsError("verified ownership needs evidence")
    if decision.get("issuer_binding") not in {"VERIFIED", "UNRESOLVED"}:
        raise CatalystSourceSemanticsError("invalid issuer_binding")
    methods = _strings(decision["issuer_binding_methods"], "issuer_binding_methods")
    unknown_methods = sorted(set(methods) - ISSUER_BINDING_METHODS)
    if unknown_methods:
        raise CatalystSourceSemanticsError(
            f"unknown issuer binding methods: {unknown_methods}"
        )
    if decision["issuer_binding"] == "VERIFIED":
        minimum = 2 if row.get("source_type") == "ISSUER_HOST" else 1
        if len(set(methods)) < minimum:
            raise CatalystSourceSemanticsError(
                f"{row.get('source_type')} verified binding needs {minimum} methods"
            )
        if row.get("source_type") == "SEC" and (
            "security_master_cik_to_document_cik" not in methods
        ):
            raise CatalystSourceSemanticsError("SEC binding requires exact CIK method")
    if decision.get("relevance") not in {"RELEVANT", "IRRELEVANT", "UNRESOLVED"}:
        raise CatalystSourceSemanticsError("invalid relevance")
    if decision.get("semantics") not in EVENT_TAXONOMY:
        raise CatalystSourceSemanticsError("invalid event semantics")
    semantic_evidence = _strings(decision["semantic_evidence"], "semantic_evidence")
    if decision["semantics"] != "UNRESOLVED" and not semantic_evidence:
        raise CatalystSourceSemanticsError("resolved semantics need evidence")
    if not isinstance(decision.get("financing_or_dilution_conflict"), bool):
        raise CatalystSourceSemanticsError("financing conflict flag must be boolean")
    if not isinstance(decision.get("notes"), str):
        raise CatalystSourceSemanticsError("review notes must be a string")
    timestamp = _review_timestamp(decision, str(row["date"]))
    return decision, timestamp


def terminal_disposition(
    row: Mapping[str, Any], decision: Mapping[str, Any], timestamp: Mapping[str, Any]
) -> tuple[str, list[str]]:
    extracted = row["extraction"]
    diagnostics = list(extracted.get("diagnostic_failures", []))
    if extracted.get("capture_status") != "RESPONSE_CAPTURED":
        return "CAPTURE_TRANSPORT_ERROR", diagnostics
    status = extracted.get("http_status")
    if status == 403:
        return "CAPTURE_FORBIDDEN", diagnostics
    if status == 404:
        return "CAPTURE_NOT_FOUND", diagnostics
    if status != 200:
        return "CAPTURE_HTTP_ERROR", diagnostics
    if decision["source_ownership"] != "VERIFIED":
        return "SOURCE_OWNERSHIP_UNRESOLVED", diagnostics
    if decision["issuer_binding"] != "VERIFIED":
        return "ISSUER_BINDING_UNRESOLVED", diagnostics
    if decision["relevance"] == "IRRELEVANT":
        return "PRIMARY_SOURCE_IRRELEVANT", diagnostics
    timestamp_status = timestamp["status"]
    if timestamp_status == "MISSING":
        return "TIMESTAMP_MISSING", diagnostics
    if timestamp_status == "CONFLICT":
        return "TIMESTAMP_CONFLICT", diagnostics
    if timestamp_status == "SAME_DAY_TIME_UNRESOLVED":
        return "SAME_DAY_TIME_UNRESOLVED", diagnostics
    if timestamp_status == "AFTER_CUTOFF":
        return "PUBLISHED_AFTER_0935", diagnostics
    if decision["relevance"] == "UNRESOLVED":
        return "DOCUMENT_SEMANTICS_UNRESOLVED", diagnostics
    semantics = decision["semantics"]
    if semantics == "UNRESOLVED":
        return "DOCUMENT_SEMANTICS_UNRESOLVED", diagnostics
    if semantics == "NON_MATERIAL_OR_CONTEXT_ONLY":
        return "NON_MATERIAL_OR_CONTEXT_ONLY", diagnostics
    if (
        decision["financing_or_dilution_conflict"]
        or extracted.get("financing_or_dilution_terms_present")
        and semantics == "FINANCING_OR_DILUTION_CONFLICT"
        or semantics in {"FINANCING_OR_DILUTION_CONFLICT", "MIXED_OR_CONTRADICTORY"}
    ):
        return "VERIFIED_CONFLICT", diagnostics
    if semantics == "DIRECT_NEGATIVE":
        return "VERIFIED_NEGATIVE_PRIMARY", diagnostics
    if semantics == "DIRECT_POSITIVE":
        return "VERIFIED_POSITIVE_PRIMARY", diagnostics
    raise CatalystSourceSemanticsError("resolved review has no terminal disposition")


def _validate_review_input(
    value: Mapping[str, Any], extraction: Mapping[str, Any]
) -> dict[str, Mapping[str, Any]]:
    if value.get("schema_version") != 1 or value.get("dataset_id") != DATASET_ID:
        raise CatalystSourceSemanticsError("review input identity is invalid")
    if value.get("manifest_sha256") != extraction.get("manifest_sha256"):
        raise CatalystSourceSemanticsError("review input manifest differs")
    if value.get("review_completed") is not True:
        raise CatalystSourceSemanticsError("review_completed must be true")
    if value.get("target_outcomes_observed_or_derived") is not False:
        raise CatalystSourceSemanticsError(
            "review input outcome lock must remain closed"
        )
    if any(
        OUTCOME_KEY_PATTERN.search(str(key))
        for key in value
        if key not in {"target_outcomes_observed_or_derived"}
    ):
        raise CatalystSourceSemanticsError(
            "review input contains an outcome-shaped field"
        )
    decisions = value.get("decisions")
    if not isinstance(decisions, list) or len(decisions) != EXPECTED_JOINS:
        raise CatalystSourceSemanticsError(
            "review input must contain exactly 38 decisions"
        )
    by_id: dict[str, Mapping[str, Any]] = {}
    for decision in decisions:
        if not isinstance(decision, Mapping):
            raise CatalystSourceSemanticsError("review decision must be an object")
        row_id = str(decision.get("row_id"))
        if row_id in by_id:
            raise CatalystSourceSemanticsError("review input has duplicate row_id")
        by_id[row_id] = decision
    expected_ids = {str(row["row_id"]) for row in extraction["rows"]}
    if set(by_id) != expected_ids:
        raise CatalystSourceSemanticsError(
            "review input row set differs from extraction"
        )
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
        decision, timestamp = validate_review_decision(
            decisions[str(row["row_id"])], row=row
        )
        terminal, diagnostics = terminal_disposition(row, decision, timestamp)
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
    join_counts = Counter(str(row["terminal_disposition"]) for row in reviewed_rows)
    by_pair: dict[tuple[str, str], list[Mapping[str, Any]]] = defaultdict(list)
    for row in reviewed_rows:
        by_pair[(str(row["date"]), str(row["symbol"]))].append(row)
    pair_dispositions = {
        _sha256_json(key): _pair_disposition(rows)
        for key, rows in sorted(by_pair.items())
    }
    pair_counts = Counter(pair_dispositions.values())
    verified_positive_pairs = pair_counts["VERIFIED_POSITIVE_PRIMARY"]
    return {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "manifest_sha256": extraction["manifest_sha256"],
        "status": "REVIEW_COMPLETE",
        "counts": dict(extraction["counts"]),
        "terminal_join_counts": {
            key: join_counts.get(key, 0) for key in TERMINAL_PRECEDENCE
        },
        "terminal_pair_counts": {
            key: pair_counts.get(key, 0) for key in TERMINAL_PRECEDENCE
        },
        "pair_dispositions": pair_dispositions,
        "verified_positive_pairs": verified_positive_pairs,
        "rows": reviewed_rows,
        "row_sha256": _sha256_json(reviewed_rows),
        "review_input_sha256": _sha256_json(review_input),
        "next_phase": (
            "SOURCE_RECOVERY"
            if verified_positive_pairs < 20
            else "DEVELOPMENT_ACQUISITION"
        ),
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
    manifest, _paths, _selection = _verify_contract(manifest_path, store)
    extraction = capture._read_gzip(_extraction_path(store.root))
    if not (
        extraction.get("manifest_sha256") == manifest["manifest_sha256"]
        and extraction.get("status") == "EXTRACTION_COMPLETE"
        and extraction.get("target_outcomes_observed_or_derived") is False
    ):
        raise CatalystSourceSemanticsError("private extraction is incomplete or stale")
    if review_input_path is None:
        template = build_review_template(extraction)
        _write_json(review_template_path, template)
        return {
            "schema_version": 1,
            "dataset_id": DATASET_ID,
            "status": "AWAITING_REVIEW",
            "review_template": _repo_path(review_template_path),
            "rows": EXPECTED_JOINS,
            "target_outcomes_observed_or_derived": False,
        }
    review_input = _read_json(review_input_path)
    result = apply_review(extraction, review_input)
    private_path = _reviewed_path(store.root)
    capture._write_gzip(private_path, result)
    public = {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "status": "REVIEW_COMPLETE",
        "counts": result["counts"],
        "terminal_join_counts": result["terminal_join_counts"],
        "terminal_pair_counts": result["terminal_pair_counts"],
        "verified_positive_pairs": result["verified_positive_pairs"],
        "private_reviewed_result_sha256": capture._sha256_file(private_path),
        "next_phase": result["next_phase"],
        "source_text_urls_symbols_dates_reviews_and_rows_public": False,
        "target_outcomes_observed_or_derived": False,
    }
    _write_json(public_status_path, public)
    return public


def _public_result(
    manifest: Mapping[str, Any], reviewed: Mapping[str, Any], private_path: Path
) -> dict[str, Any]:
    positive = int(reviewed["verified_positive_pairs"])
    return {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "status": "READY",
        "inspected": True,
        "claim_scope": "DEVELOPMENT_ONLY",
        "selection_counts": dict(reviewed["counts"]),
        "terminal_join_counts": dict(reviewed["terminal_join_counts"]),
        "terminal_pair_counts": dict(reviewed["terminal_pair_counts"]),
        "verified_positive_pairs": positive,
        "minimum_positive_capacity": 20,
        "capacity_gate_passed": positive >= 20,
        "next_phase": reviewed["next_phase"],
        "private_result_sha256": capture._sha256_file(private_path),
        "inspection": {
            "joins_rebuilt": True,
            "terminal_counts_rebuilt": True,
            "accepted_timestamps_rebuilt": True,
            "private_hashes_verified": True,
            "one_terminal_reason_per_join": sum(
                int(value) for value in reviewed["terminal_join_counts"].values()
            )
            == EXPECTED_JOINS,
        },
        "claim_boundary": (
            "Primary-source ownership, issuer binding, causal timing, relevance, and "
            "direction only. Returns and target-session outcomes remain inaccessible."
        ),
        "outcome_contract_permitted": positive >= 20,
        "production_rule_change_earned": False,
        "source_text_urls_symbols_dates_reviews_and_rows_public": False,
        "target_outcomes_observed_or_derived": False,
    }


def inspect(
    *, manifest_path: Path, env_path: Path, public_result_path: Path
) -> dict[str, Any]:
    store = HistoricalDayStore.from_env(env_path)
    manifest, paths, selection = _verify_contract(manifest_path, store)
    extraction_path = _extraction_path(store.root)
    reviewed_path = _reviewed_path(store.root)
    extraction = capture._read_gzip(extraction_path)
    reviewed = capture._read_gzip(reviewed_path)
    rebuilt_extraction = build_extraction(
        store, selection, capture._read_json(paths["capture_index"])
    )
    rebuilt_extraction["manifest_sha256"] = manifest["manifest_sha256"]
    if not (
        extraction.get("row_sha256") == rebuilt_extraction.get("row_sha256")
        and extraction.get("rows") == rebuilt_extraction.get("rows")
        and extraction.get("target_outcomes_observed_or_derived") is False
    ):
        raise CatalystSourceSemanticsError("independent extraction rebuild differs")
    review_input = {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "review_completed": True,
        "decisions": [row["review"] for row in reviewed["rows"]],
        "target_outcomes_observed_or_derived": False,
    }
    rebuilt_review = apply_review(rebuilt_extraction, review_input)
    for field in (
        "terminal_join_counts",
        "terminal_pair_counts",
        "pair_dispositions",
        "verified_positive_pairs",
        "rows",
        "row_sha256",
        "next_phase",
    ):
        if reviewed.get(field) != rebuilt_review.get(field):
            raise CatalystSourceSemanticsError(f"review inspection mismatch: {field}")
    if reviewed.get("target_outcomes_observed_or_derived") is not False:
        raise CatalystSourceSemanticsError("reviewed result outcome lock is open")
    result = _public_result(manifest, reviewed, reviewed_path)
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


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "freeze":
            path, manifest = freeze_inputs(
                env_path=args.env_file, output_root=args.output_root
            )
            output = {"manifest": _repo_path(path), **manifest}
        elif args.manifest is None:
            raise CatalystSourceSemanticsError("--manifest is required")
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
        print(json.dumps(output, indent=2, sort_keys=True))
        return 0
    except (
        CatalystSourceSemanticsError,
        LearningDataError,
        OSError,
        ValueError,
    ) as exc:
        print(
            json.dumps(
                {"status": "error", "error": str(exc), "error_type": type(exc).__name__}
            ),
            file=sys.stderr,
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())

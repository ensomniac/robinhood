"""Review the frozen development SEC corpus under the frozen source rules."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import importlib.metadata
import io
import json
import os
import re
import shutil
import sys
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from html.parser import HTMLParser
from pathlib import Path
from typing import Any

import catalyst_source_semantics as shared_semantics
import development_catalyst_contract as source_semantics_contract
import development_sec_document_collection as document_collection
import development_sec_documents as document_contract
import development_sec_sources as sec_source_contract
from historical_store import HistoricalStoreConfig, HistoricalStoreError
from learning_data import (
    LearningDataError,
    freeze_dataset_contract,
    load_frozen_dataset_contract,
)


PROJECT_ROOT = Path(__file__).resolve().parent
DATASET_ID = "dataset-development-sec-source-semantics-2026-07-19-v2"
SOURCE_SEMANTICS_MANIFEST = sec_source_contract.SOURCE_CONTRACT
DOCUMENT_MANIFEST = document_collection.MANIFEST
DOCUMENT_STATUS = document_collection.DEFAULT_PUBLIC_STATUS
DOCUMENT_INSPECTION = document_collection.DEFAULT_PUBLIC_INSPECTION
DEFAULT_OUTPUT_ROOT = (
    PROJECT_ROOT / "historical_batches/development_tranche_v2/sec_semantics_manifests"
)
DEFAULT_PUBLIC_STATUS = (
    PROJECT_ROOT / "historical_batches/development_tranche_v2/sec-semantics-status.json"
)
DEFAULT_PUBLIC_RESULT = (
    PROJECT_ROOT
    / "research_results/2026-07-19-development-sec-source-semantics-inspection.json"
)
PRIVATE_NAMESPACE = "_derived/development_catalyst_sources"
PARSER_VERSION = "development-sec-source-semantics-v1"
TEXT_LIMIT = 250_000

POSITIVE_PATTERNS = (
    (
        "record_operating_result",
        r"\brecord(?:\s+(?:quarterly|annual|full[- ]year))?\s+"
        r"(?:revenue|sales|bookings|earnings|adjusted ebitda)\b",
    ),
    (
        "revenue_or_sales_growth",
        r"\b(?:revenue|sales|bookings)\s+(?:grew|increased|rose)\s+"
        r"(?:by\s+)?(?:approximately\s+)?\d+(?:\.\d+)?\s*%",
    ),
    (
        "raised_guidance",
        r"\b(?:raise[sd]?|increase[sd]?)\s+(?:its\s+)?"
        r"(?:full[- ]year\s+)?(?:financial\s+)?guidance\b",
    ),
    (
        "expectations_exceeded",
        r"\b(?:exceeded|surpassed|beat)\s+(?:consensus\s+|analyst\s+)?"
        r"(?:expectations|estimates)\b",
    ),
    (
        "contract_award",
        r"\b(?:awarded|received|secured|wins?|won)\b.{0,100}"
        r"\b(?:contract|award|order)\b",
    ),
    (
        "regulatory_approval",
        r"\b(?:fda|food and drug administration|ema|european commission)\b"
        r".{0,100}\b(?:approv(?:al|ed)|clearance|authorization)\b",
    ),
    (
        "positive_trial",
        r"\b(?:positive|favorable)\s+(?:topline\s+)?(?:clinical\s+)?"
        r"(?:trial|study|data|results)\b",
    ),
    (
        "endpoint_met",
        r"\b(?:met|achieved)\s+(?:its\s+|the\s+)?"
        r"(?:primary|main|key)\s+endpoint\b",
    ),
    (
        "premium_transaction",
        r"\bdefinitive\s+(?:merger|acquisition)\s+agreement\b.{0,180}"
        r"\bpremium\b",
    ),
    (
        "capital_return",
        r"\b(?:special\s+dividend|increased\s+(?:quarterly\s+)?dividend|"
        r"share\s+repurchase\s+(?:program|authorization))\b",
    ),
)
NEGATIVE_PATTERNS = (
    ("bankruptcy", r"\b(?:chapter\s+11|bankruptcy|bankrupt)\b"),
    (
        "going_concern",
        r"\bsubstantial\s+doubt\b.{0,100}\bcontinue\s+as\s+a\s+going\s+concern\b",
    ),
    (
        "lowered_guidance",
        r"\b(?:lower(?:ed|s)?|reduce[sd]?)\s+(?:its\s+)?"
        r"(?:full[- ]year\s+)?(?:financial\s+)?guidance\b",
    ),
    (
        "missed_expectations",
        r"\b(?:missed|below)\s+(?:consensus\s+|analyst\s+)?"
        r"(?:expectations|estimates)\b",
    ),
    (
        "failed_endpoint",
        r"\b(?:did\s+not\s+meet|failed\s+to\s+meet)\s+(?:its\s+|the\s+)?"
        r"(?:primary|main|key)\s+endpoint\b",
    ),
    ("clinical_hold", r"\b(?:clinical\s+hold|trial\s+suspension)\b"),
    (
        "delisting",
        r"\b(?:delisting|delist|noncompliance\s+with\s+(?:the\s+)?"
        r"(?:nasdaq|nyse)\s+listing)\b",
    ),
    ("material_weakness", r"\bmaterial\s+weakness(?:es)?\b"),
    (
        "revenue_decline",
        r"\b(?:revenue|sales)\s+(?:declined|decreased|fell)\s+"
        r"(?:by\s+)?(?:approximately\s+)?\d+(?:\.\d+)?\s*%",
    ),
)
NON_MATERIAL_PATTERNS = (
    (
        "governance_only",
        r"\b(?:appointment|election|departure)\s+of\s+"
        r"(?:directors?|certain\s+officers?)\b",
    ),
    (
        "bylaw_only",
        r"\bamendments?\s+to\s+(?:articles\s+of\s+incorporation|bylaws?)\b",
    ),
)
PAIR_FIELDS = ("date", "instrument_id", "primary_exchange", "rank", "symbol")


class DevelopmentCatalystSourceSemanticsError(RuntimeError):
    """The frozen development source-semantics evidence is unsafe or drifted."""


class _VisibleTextParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.hidden_depth = 0
        self.title_depth = 0
        self.title_parts: list[str] = []
        self.text_parts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        del attrs
        lowered = tag.lower()
        if lowered in {"script", "style", "noscript"}:
            self.hidden_depth += 1
        elif lowered == "title":
            self.title_depth += 1

    def handle_endtag(self, tag: str) -> None:
        lowered = tag.lower()
        if lowered in {"script", "style", "noscript"} and self.hidden_depth:
            self.hidden_depth -= 1
        elif lowered == "title" and self.title_depth:
            self.title_depth -= 1

    def handle_data(self, data: str) -> None:
        if self.hidden_depth:
            return
        if self.title_depth:
            self.title_parts.append(data)
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


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_object(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise DevelopmentCatalystSourceSemanticsError(
            f"cannot read {path}: {exc}"
        ) from exc
    if not isinstance(value, dict):
        raise DevelopmentCatalystSourceSemanticsError(
            f"{path} must contain an object"
        )
    return value


def _read_gzip_object(path: Path) -> dict[str, Any]:
    try:
        with gzip.open(path, "rt", encoding="utf-8") as source:
            value = json.load(source)
    except (OSError, json.JSONDecodeError) as exc:
        raise DevelopmentCatalystSourceSemanticsError(
            f"cannot read {path}: {exc}"
        ) from exc
    if not isinstance(value, dict):
        raise DevelopmentCatalystSourceSemanticsError(
            f"{path} must contain an object"
        )
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


def _write_gzip_json(path: Path, value: Any) -> None:
    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode="wb", compresslevel=6, mtime=0) as stream:
        stream.write(json.dumps(value, indent=2, sort_keys=True).encode())
        stream.write(b"\n")
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_bytes(buffer.getvalue())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _repo_path(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(PROJECT_ROOT))
    except ValueError as exc:
        raise DevelopmentCatalystSourceSemanticsError(
            f"path must be repository relative: {path}"
        ) from exc


def _private_root(store_root: Path) -> Path:
    return (
        store_root
        / PRIVATE_NAMESPACE
        / source_semantics_contract.DATASET_ID
        / "semantics_runs"
        / DATASET_ID
    )


def _selection_path(store_root: Path) -> Path:
    return _private_root(store_root) / "frozen-selection.json.gz"


def _extraction_path(store_root: Path) -> Path:
    return _private_root(store_root) / "extraction.json.gz"


def _reviewed_path(store_root: Path) -> Path:
    return _private_root(store_root) / "reviewed-result.json.gz"


def _target_artifact_count(store_root: Path) -> int:
    return sum(
        path.exists()
        for path in (_extraction_path(store_root), _reviewed_path(store_root))
    )


def _resolve_public_path(value: Any) -> Path:
    if not isinstance(value, str) or not value:
        raise DevelopmentCatalystSourceSemanticsError("bound path is missing")
    path = Path(value)
    if path.is_absolute() or ".." in path.parts:
        raise DevelopmentCatalystSourceSemanticsError("bound path is unsafe")
    return PROJECT_ROOT / path


def _verify_binding(value: Mapping[str, Any]) -> None:
    path = _resolve_public_path(value.get("path"))
    if not path.is_file() or _sha256_file(path) != value.get("sha256"):
        raise DevelopmentCatalystSourceSemanticsError(
            f"bound source semantics input drifted: {path}"
        )


def _expected_source_rules() -> dict[str, Any]:
    return {
        "primary_evidence_only": True,
        "secondary_news_may_route_but_cannot_verify": True,
        "source_ownership_required": True,
        "issuer_binding_required": True,
        "issuer_binding_methods": sorted(shared_semantics.ISSUER_BINDING_METHODS),
        "timestamp_precedence": list(shared_semantics.TIMESTAMP_PRECEDENCE),
        "same_day_date_only_fails": True,
        "metadata_headers_capture_url_and_pdf_dates_cannot_independently_prove_publication": True,
        "event_taxonomy": list(shared_semantics.EVENT_TAXONOMY),
        "financing_or_dilution_conflicts_classified_before_positive": True,
        "terminal_precedence": list(shared_semantics.TERMINAL_PRECEDENCE),
        "one_terminal_disposition_per_pair_source_join": True,
        "selection_or_source_substitution_allowed": False,
    }


def _verify_source_semantics_manifest(manifest: Mapping[str, Any]) -> None:
    if manifest.get("dataset_id") != source_semantics_contract.DATASET_ID:
        raise DevelopmentCatalystSourceSemanticsError(
            "unexpected frozen source-semantics contract"
        )
    if manifest.get("source_rules") != _expected_source_rules():
        raise DevelopmentCatalystSourceSemanticsError(
            "frozen source-semantics rules differ"
        )
    outcome = manifest.get("outcome_lock")
    implementation = manifest.get("implementation_contract")
    upstream = manifest.get("upstream_contract")
    selection = manifest.get("selection_contract")
    if not all(
        isinstance(value, Mapping)
        for value in (outcome, implementation, upstream, selection)
    ):
        raise DevelopmentCatalystSourceSemanticsError(
            "frozen source-semantics contract is incomplete"
        )
    if (
        outcome.get("post_entry_data_access_allowed") is not False
        or outcome.get("return_fields_allowed") is not False
        or outcome.get("target_outcomes_observed_or_derived") is not False
        or int(outcome.get("minimum_verified_positive_pairs_before_any_outcome_contract", -1))
        != 20
        or int(
            outcome.get(
                "minimum_complete_unchanged_v3_non_return_survivors_before_outcomes",
                -1,
            )
        )
        != 20
    ):
        raise DevelopmentCatalystSourceSemanticsError(
            "frozen source-semantics outcome lock differs"
        )
    for value in upstream.values():
        if not isinstance(value, Mapping):
            raise DevelopmentCatalystSourceSemanticsError(
                "source-semantics upstream binding is malformed"
            )
        _verify_binding(value)
    files = implementation.get("files")
    if not isinstance(files, Mapping):
        raise DevelopmentCatalystSourceSemanticsError(
            "source-semantics implementation binding is missing"
        )
    for value in files.values():
        if not isinstance(value, Mapping):
            raise DevelopmentCatalystSourceSemanticsError(
                "source-semantics implementation binding is malformed"
            )
        _verify_binding(value)


def _load_inputs(
    *,
    source_semantics_manifest_path: Path,
    document_manifest_path: Path,
    env_path: Path,
) -> tuple[
    dict[str, Any],
    dict[str, Any],
    HistoricalStoreConfig,
    dict[str, Any],
    dict[str, Any],
    dict[str, Any],
]:
    source_manifest = load_frozen_dataset_contract(source_semantics_manifest_path)
    _verify_source_semantics_manifest(source_manifest)
    document_manifest, config, document_private, _ = (
        document_collection._load_contract(
            manifest_path=document_manifest_path,
            env_path=env_path,
            require_published=False,
        )
    )
    document_index = document_collection._build_index(
        manifest=document_manifest,
        private=document_private,
        store_root=config.root,
    )
    if document_index != _read_gzip_object(document_collection._index_path(config.root)):
        raise DevelopmentCatalystSourceSemanticsError(
            "primary-document collection index drifted"
        )
    recorded_status = _read_object(DOCUMENT_STATUS)
    publication = {
        "collector": {"commit": recorded_status.get("collector_commit")}
    }
    if recorded_status != document_collection._public(document_index, publication):
        raise DevelopmentCatalystSourceSemanticsError(
            "primary-document public status drifted"
        )
    inspection = _read_object(DOCUMENT_INSPECTION)
    if (
        inspection.get("status") != "DOCUMENTS_INSPECTED"
        or inspection.get("valid") is not True
        or inspection.get("private_collection_content_sha256")
        != _sha256_json(document_index)
        or int(document_index["counts"].get("failed_requests", -1)) != 0
        or int(document_index["counts"].get("pending_requests", -1)) != 0
    ):
        raise DevelopmentCatalystSourceSemanticsError(
            "primary-document inspection is incomplete"
        )
    identities = sec_source_contract._read_gzip_object(
        sec_source_contract._private_identity_path(config.root)
    )
    return (
        source_manifest,
        document_manifest,
        config,
        document_private,
        document_index,
        identities,
    )


def _request_record_map(index: Mapping[str, Any]) -> dict[str, Mapping[str, Any]]:
    values = index.get("request_records")
    if not isinstance(values, list):
        raise DevelopmentCatalystSourceSemanticsError(
            "document request records are malformed"
        )
    result: dict[str, Mapping[str, Any]] = {}
    for value in values:
        if not isinstance(value, Mapping):
            raise DevelopmentCatalystSourceSemanticsError(
                "document request record is malformed"
            )
        key = str(value.get("request_sha256") or "")
        if not key or key in result:
            raise DevelopmentCatalystSourceSemanticsError(
                "document request record identity is missing or duplicated"
            )
        if value.get("status") != "SUCCESS":
            raise DevelopmentCatalystSourceSemanticsError(
                "document source is not successful"
            )
        result[key] = value
    return result


def build_selection(
    *,
    source_manifest: Mapping[str, Any],
    document_private: Mapping[str, Any],
    document_index: Mapping[str, Any],
    identities: Mapping[str, Any],
) -> dict[str, Any]:
    pair_values = identities.get("pairs")
    request_values = document_private.get("document_requests")
    join_values = document_private.get("pair_document_joins")
    if not all(isinstance(value, list) for value in (pair_values, request_values, join_values)):
        raise DevelopmentCatalystSourceSemanticsError(
            "private pair or document graph is malformed"
        )
    expected_pair_count = int(
        source_manifest["selection_contract"].get("selected_pair_count", -1)
    )
    pairs: dict[tuple[str, str], dict[str, Any]] = {}
    for value in pair_values:
        if not isinstance(value, Mapping):
            raise DevelopmentCatalystSourceSemanticsError("pair identity is malformed")
        pair = {field: value.get(field) for field in (*PAIR_FIELDS, "cik")}
        key = (str(pair["date"]), str(pair["instrument_id"]))
        if not all(str(pair[field] or "") for field in PAIR_FIELDS) or key in pairs:
            raise DevelopmentCatalystSourceSemanticsError(
                "pair identity is incomplete or duplicated"
            )
        pairs[key] = pair
    if len(pairs) != expected_pair_count:
        raise DevelopmentCatalystSourceSemanticsError(
            "selected-pair universe differs from source contract"
        )
    records = _request_record_map(document_index)
    requests: dict[str, Mapping[str, Any]] = {}
    for value in request_values:
        if not isinstance(value, Mapping):
            raise DevelopmentCatalystSourceSemanticsError(
                "document request is malformed"
            )
        url = str(value.get("source_url") or "")
        request_sha256 = _sha256_json(value)
        record = records.get(request_sha256)
        if not url or url in requests or record is None:
            raise DevelopmentCatalystSourceSemanticsError(
                "document request identity is missing or duplicated"
            )
        requests[url] = {**dict(value), "collection_record": dict(record)}
    rows: list[dict[str, Any]] = []
    joined_pairs: set[tuple[str, str]] = set()
    row_ids: set[str] = set()
    for value in join_values:
        if not isinstance(value, Mapping) or not isinstance(value.get("filing"), Mapping):
            raise DevelopmentCatalystSourceSemanticsError(
                "pair/document join is malformed"
            )
        filing = value["filing"]
        url = str(filing.get("source_url") or "")
        request = requests.get(url)
        pair_key = (str(value.get("date") or ""), str(value.get("instrument_id") or ""))
        pair = pairs.get(pair_key)
        if request is None or pair is None:
            raise DevelopmentCatalystSourceSemanticsError(
                "pair/document join references unknown frozen input"
            )
        for field in PAIR_FIELDS:
            if value.get(field) != pair.get(field):
                raise DevelopmentCatalystSourceSemanticsError(
                    "pair/document join identity differs"
                )
        if (
            str(request.get("cik")) != str(pair.get("cik"))
            or any(
                request.get(field) != filing.get(field)
                for field in (
                    "form",
                    "accession",
                    "accepted_at",
                    "filing_date",
                    "report_date",
                    "items",
                    "primary_document",
                    "source_url",
                )
            )
        ):
            raise DevelopmentCatalystSourceSemanticsError(
                "pair/document join source identity differs"
            )
        row_id = "sec-join-" + _sha256_json(
            [
                *pair_key,
                url,
                value.get("source_origin"),
            ]
        )[:24]
        if row_id in row_ids:
            raise DevelopmentCatalystSourceSemanticsError(
                "pair/document row identity repeats"
            )
        row_ids.add(row_id)
        joined_pairs.add(pair_key)
        record = request["collection_record"]
        rows.append(
            {
                "row_id": row_id,
                **pair,
                "source_type": "SEC",
                "source_origin": value.get("source_origin"),
                "source_url": url,
                "form": request["form"],
                "accession": request["accession"],
                "accepted_at": request["accepted_at"],
                "filing_date": request["filing_date"],
                "report_date": request["report_date"],
                "items": request["items"],
                "primary_document": request["primary_document"],
                "shared_cache_relative_path": request[
                    "shared_cache_relative_path"
                ],
                "source_bytes": record["source_bytes"],
                "source_sha256": record["source_sha256"],
            }
        )
    rows.sort(key=lambda row: str(row["row_id"]))
    no_source_pairs = [
        pair
        for key, pair in sorted(pairs.items())
        if key not in joined_pairs
    ]
    counts = {
        "selected_pairs": len(pairs),
        "source_documents": len(requests),
        "pair_source_joins": len(rows),
        "pairs_with_source": len(joined_pairs),
        "pairs_without_source": len(no_source_pairs),
    }
    if (
        counts["source_documents"]
        != int(document_index["counts"]["successful_requests"])
        or counts["pair_source_joins"]
        != int(document_private["counts"]["pair_document_joins"])
        or counts["pairs_with_source"]
        != int(document_private["counts"]["unique_pairs_with_document"])
    ):
        raise DevelopmentCatalystSourceSemanticsError(
            "source semantics selection counts differ"
        )
    return {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "status": "FROZEN_SELECTION",
        "counts": counts,
        "rows": rows,
        "no_source_pairs": no_source_pairs,
        "pair_identity_sha256": _sha256_json(
            [pairs[key] for key in sorted(pairs)]
        ),
        "source_document_sha256": _sha256_json(
            sorted(
                (
                    request["source_url"],
                    request["collection_record"]["source_sha256"],
                    request["collection_record"]["source_bytes"],
                )
                for request in requests.values()
            )
        ),
        "pair_source_join_sha256": _sha256_json(rows),
        "no_source_pair_sha256": _sha256_json(no_source_pairs),
        "symbols_ciks_accessions_urls_sources_text_and_rows_public": False,
        "target_outcomes_observed_or_derived": False,
    }


def _implementation_contract() -> dict[str, Any]:
    paths = {
        "development_semantics": Path(__file__),
        "frozen_shared_semantics": Path(shared_semantics.__file__),
        "document_collector": Path(document_collection.__file__),
        "document_contract": Path(document_contract.__file__),
    }
    return {
        "files": {
            name: {"path": _repo_path(path), "sha256": _sha256_file(path)}
            for name, path in paths.items()
        },
        "parser_version": PARSER_VERSION,
        "dependencies": {
            "python": sys.version.split()[0],
            "pypdf": importlib.metadata.version("pypdf"),
        },
    }


def _classification_contract() -> dict[str, Any]:
    return {
        "positive_patterns": [list(value) for value in POSITIVE_PATTERNS],
        "negative_patterns": [list(value) for value in NEGATIVE_PATTERNS],
        "non_material_patterns": [list(value) for value in NON_MATERIAL_PATTERNS],
        "financing_pattern": shared_semantics.FINANCING_PATTERN.pattern,
        "financing_or_dilution_conflict_precedes_all_positive_patterns": True,
        "positive_and_negative_match_is_mixed_or_contradictory": True,
        "no_supported_direction_match_is_unresolved": True,
        "sec_ownership_evidence": "SEC-operated accession-bound archive endpoint",
        "sec_issuer_binding_method": "security_master_cik_to_document_cik",
        "accepted_at_evidence_kind": "SOURCE_CONTROLLED_EXPLICIT_DATETIME",
        "accepted_at_is_official_sec_filing_acceptance_not_generic_metadata": True,
        "same_day_date_only_accepted": False,
        "pair_precedence": [
            "VERIFIED_CONFLICT",
            "VERIFIED_NEGATIVE_PRIMARY",
            "VERIFIED_POSITIVE_PRIMARY",
            *[
                value
                for value in shared_semantics.TERMINAL_PRECEDENCE
                if value
                not in {
                    "VERIFIED_CONFLICT",
                    "VERIFIED_NEGATIVE_PRIMARY",
                    "VERIFIED_POSITIVE_PRIMARY",
                }
            ],
        ],
    }


def _stable_contract(
    *,
    source_semantics_manifest_path: Path,
    document_manifest_path: Path,
    env_path: Path,
    require_published_implementation: bool,
) -> tuple[dict[str, Any], HistoricalStoreConfig, dict[str, Any]]:
    if require_published_implementation:
        document_collection._published_artifact(Path(__file__))
    (
        source_manifest,
        document_manifest,
        config,
        document_private,
        document_index,
        identities,
    ) = _load_inputs(
        source_semantics_manifest_path=source_semantics_manifest_path,
        document_manifest_path=document_manifest_path,
        env_path=env_path,
    )
    selection = build_selection(
        source_manifest=source_manifest,
        document_private=document_private,
        document_index=document_index,
        identities=identities,
    )
    stable = {
        "lineage_contract": {
            "source_semantics_manifest": {
                "path": _repo_path(source_semantics_manifest_path),
                "sha256": _sha256_file(source_semantics_manifest_path),
                "manifest_sha256": source_manifest["manifest_sha256"],
            },
            "document_manifest": {
                "path": _repo_path(document_manifest_path),
                "sha256": _sha256_file(document_manifest_path),
                "manifest_sha256": document_manifest["manifest_sha256"],
            },
            "document_status": {
                "path": _repo_path(DOCUMENT_STATUS),
                "sha256": _sha256_file(DOCUMENT_STATUS),
            },
            "document_inspection": {
                "path": _repo_path(DOCUMENT_INSPECTION),
                "sha256": _sha256_file(DOCUMENT_INSPECTION),
            },
            "private_document_collection_sha256": _sha256_json(document_index),
            "strategy": source_manifest["upstream_contract"]["strategy_source"],
        },
        "selection_contract": {
            **selection["counts"],
            "pair_identity_sha256": selection["pair_identity_sha256"],
            "source_document_sha256": selection["source_document_sha256"],
            "pair_source_join_sha256": selection["pair_source_join_sha256"],
            "no_source_pair_sha256": selection["no_source_pair_sha256"],
            "private_selection_content_sha256": _sha256_json(selection),
            "private_selection_path": (
                "LOCAL_HISTORICAL_DATA_ROOT/"
                f"{PRIVATE_NAMESPACE}/{source_semantics_contract.DATASET_ID}/"
                f"semantics_runs/{DATASET_ID}/frozen-selection.json.gz"
            ),
            "symbols_ciks_accessions_urls_sources_text_and_rows_public": False,
        },
        "source_rules": source_manifest["source_rules"],
        "classification_contract": _classification_contract(),
        "private_record_contract": source_manifest["private_record_contract"],
        "outcome_lock": {
            **source_manifest["outcome_lock"],
            "outcome_contract_permitted_at_source_semantics_stage": False,
            "complete_unchanged_v3_non_return_survivors_known": 0,
        },
        "implementation_contract": _implementation_contract(),
        "pre_freeze_target_artifact_count": 0,
    }
    return stable, config, selection


def _verify_or_write_selection(
    *, config: HistoricalStoreConfig, selection: Mapping[str, Any], write: bool
) -> None:
    path = _selection_path(config.root)
    expected = _sha256_json(selection)
    if path.exists():
        if _sha256_json(_read_gzip_object(path)) != expected:
            raise DevelopmentCatalystSourceSemanticsError(
                "private source-semantics selection drifted"
            )
    elif write:
        _write_gzip_json(path, selection)
    else:
        raise DevelopmentCatalystSourceSemanticsError(
            "private source-semantics selection is missing"
        )


def freeze_contract(
    *,
    source_semantics_manifest_path: Path = SOURCE_SEMANTICS_MANIFEST,
    document_manifest_path: Path = DOCUMENT_MANIFEST,
    env_path: Path = PROJECT_ROOT / ".env",
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    require_published_implementation: bool = True,
) -> tuple[Path, dict[str, Any]]:
    stable, config, selection = _stable_contract(
        source_semantics_manifest_path=source_semantics_manifest_path,
        document_manifest_path=document_manifest_path,
        env_path=env_path,
        require_published_implementation=require_published_implementation,
    )
    matches = sorted(output_root.glob(f"{DATASET_ID}-*.json"))
    if not matches and _target_artifact_count(config.root):
        raise DevelopmentCatalystSourceSemanticsError(
            "target semantics artifacts exist before contract freeze"
        )
    _verify_or_write_selection(config=config, selection=selection, write=True)
    if len(matches) > 1:
        raise DevelopmentCatalystSourceSemanticsError(
            "source-semantics contract has multiple manifests"
        )
    if matches:
        existing = load_frozen_dataset_contract(matches[0])
        if any(existing.get(key) != value for key, value in stable.items()):
            raise DevelopmentCatalystSourceSemanticsError(
                "existing source-semantics contract drifted"
            )
        return matches[0], existing
    usage = shutil.disk_usage(config.root)
    if usage.free < config.min_free_bytes:
        raise DevelopmentCatalystSourceSemanticsError(
            "historical-store reserve is unavailable"
        )
    contract = {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "registered_at": datetime.now(UTC).isoformat(),
        "requested_dates": load_frozen_dataset_contract(
            source_semantics_manifest_path
        )["requested_dates"],
        "dataset_payload": {
            "lane": "development",
            "claim_scope": "DEVELOPMENT_ONLY",
            "status": "COLLECTING",
            "evidence_paths": [
                "DEVELOPMENT_CATALYST_CONTRACT.md",
                _repo_path(source_semantics_manifest_path),
                _repo_path(document_manifest_path),
                _repo_path(DOCUMENT_INSPECTION),
                "PRODUCTION_STRATEGY_VALIDATION.md",
            ],
            "inspected": False,
        },
        **stable,
        "capacity_contract": {
            "historical_store_outside_repository": not config.root.resolve().is_relative_to(
                PROJECT_ROOT.resolve()
            ),
            "free_bytes_at_freeze": usage.free,
            "reserve_bytes": config.min_free_bytes,
            "minimum_required_reserve_bytes": sec_source_contract.MINIMUM_RESERVE_BYTES,
            "capacity_ready": True,
            "historical_deletion_allowed": False,
        },
    }
    return freeze_dataset_contract(contract, output_root)


def _verify_contract(
    *,
    manifest_path: Path,
    source_semantics_manifest_path: Path,
    document_manifest_path: Path,
    env_path: Path,
) -> tuple[dict[str, Any], HistoricalStoreConfig, dict[str, Any]]:
    manifest = load_frozen_dataset_contract(manifest_path)
    if manifest.get("dataset_id") != DATASET_ID:
        raise DevelopmentCatalystSourceSemanticsError(
            "unexpected development source-semantics dataset"
        )
    stable, config, selection = _stable_contract(
        source_semantics_manifest_path=source_semantics_manifest_path,
        document_manifest_path=document_manifest_path,
        env_path=env_path,
        require_published_implementation=True,
    )
    for key, value in stable.items():
        if manifest.get(key) != value:
            raise DevelopmentCatalystSourceSemanticsError(
                f"source-semantics contract {key} drifted"
            )
    _verify_or_write_selection(config=config, selection=selection, write=False)
    capacity = manifest.get("capacity_contract")
    if not isinstance(capacity, Mapping) or any(
        (
            capacity.get("capacity_ready") is not True,
            capacity.get("historical_store_outside_repository") is not True,
            capacity.get("historical_deletion_allowed") is not False,
            int(capacity.get("reserve_bytes", -1)) != config.min_free_bytes,
        )
    ):
        raise DevelopmentCatalystSourceSemanticsError(
            "source-semantics capacity contract is invalid"
        )
    return manifest, config, selection


def inspect_contract(
    *,
    manifest_path: Path,
    source_semantics_manifest_path: Path = SOURCE_SEMANTICS_MANIFEST,
    document_manifest_path: Path = DOCUMENT_MANIFEST,
    env_path: Path = PROJECT_ROOT / ".env",
    status_path: Path = DEFAULT_PUBLIC_STATUS,
) -> dict[str, Any]:
    manifest, _config, selection = _verify_contract(
        manifest_path=manifest_path,
        source_semantics_manifest_path=source_semantics_manifest_path,
        document_manifest_path=document_manifest_path,
        env_path=env_path,
    )
    status = {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "status": "FROZEN_READY",
        "counts": selection["counts"],
        "pair_identity_sha256": selection["pair_identity_sha256"],
        "source_document_sha256": selection["source_document_sha256"],
        "pair_source_join_sha256": selection["pair_source_join_sha256"],
        "no_source_pair_sha256": selection["no_source_pair_sha256"],
        "private_selection_content_sha256": _sha256_json(selection),
        "pre_freeze_target_artifact_count": 0,
        "source_text_or_semantics_observed_or_derived": False,
        "verified_positive_pairs": 0,
        "outcome_contract_permitted": False,
        "target_outcomes_observed_or_derived": False,
        "symbols_ciks_accessions_urls_sources_text_and_rows_public": False,
        "valid": True,
    }
    _write_json(status_path, status)
    return status


def _normalize_text(parts: Sequence[str]) -> str:
    return re.sub(r"\s+", " ", " ".join(parts)).strip()[:TEXT_LIMIT]


def _extract_document(raw: bytes) -> dict[str, Any]:
    decoded = raw.decode("utf-8", errors="replace")
    parser = _VisibleTextParser()
    parser.feed(decoded)
    text = _normalize_text(parser.text_parts)
    title = _normalize_text(parser.title_parts)[:2_000]
    if not text and decoded.strip():
        text = re.sub(r"\s+", " ", decoded).strip()[:TEXT_LIMIT]
    return {
        "title": title,
        "text": text,
        "text_sha256": hashlib.sha256(text.encode()).hexdigest(),
        "text_characters": len(text),
        "financing_or_dilution_terms_present": bool(
            shared_semantics.FINANCING_PATTERN.search(text)
        ),
        "diagnostic_failures": (
            [] if len(text) >= 50 else ["insufficient_extracted_source_text"]
        ),
        "target_outcomes_observed_or_derived": False,
    }


def build_extraction(
    *, selection: Mapping[str, Any], store_root: Path
) -> dict[str, Any]:
    document_cache: dict[str, dict[str, Any]] = {}
    rows: list[dict[str, Any]] = []
    for row in selection["rows"]:
        relative = Path(str(row["shared_cache_relative_path"]))
        if relative.is_absolute() or ".." in relative.parts:
            raise DevelopmentCatalystSourceSemanticsError(
                "frozen source path is unsafe"
            )
        source_path = (
            store_root / sec_source_contract.SHARED_SEC_CACHE_NAMESPACE / relative
        )
        raw = source_path.read_bytes()
        if (
            len(raw) != int(row["source_bytes"])
            or hashlib.sha256(raw).hexdigest() != row["source_sha256"]
        ):
            raise DevelopmentCatalystSourceSemanticsError(
                "raw source bytes differ from inspected collection"
            )
        source_sha256 = str(row["source_sha256"])
        if source_sha256 not in document_cache:
            document_cache[source_sha256] = _extract_document(raw)
        extracted = document_cache[source_sha256]
        accepted = datetime.fromisoformat(str(row["accepted_at"]))
        if accepted.tzinfo is None:
            raise DevelopmentCatalystSourceSemanticsError(
                "SEC acceptance timestamp lacks timezone"
            )
        rows.append(
            {
                **dict(row),
                "decision_cutoff_et": shared_semantics._decision_cutoff_text(
                    str(row["date"])
                ),
                "extraction": {
                    "capture_status": "RESPONSE_CAPTURED",
                    "http_status": 200,
                    "source_ownership_candidate": "SEC_OPERATED_ACCESSION_ENDPOINT",
                    "issuer_binding_candidate": (
                        "SECURITY_MASTER_CIK_EQUALS_DOCUMENT_CIK"
                    ),
                    "timestamp_candidates": [
                        {
                            "kind": "SOURCE_CONTROLLED_EXPLICIT_DATETIME",
                            "value": accepted.isoformat(),
                            "source": "SEC acceptanceDateTime",
                        }
                    ],
                    **extracted,
                },
            }
        )
    rows.sort(key=lambda value: str(value["row_id"]))
    if len(rows) != int(selection["counts"]["pair_source_joins"]):
        raise DevelopmentCatalystSourceSemanticsError(
            "source extraction join count differs"
        )
    return {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "status": "EXTRACTION_COMPLETE",
        "counts": dict(selection["counts"]),
        "rows": rows,
        "no_source_pairs": selection["no_source_pairs"],
        "row_sha256": _sha256_json(rows),
        "source_text_urls_symbols_dates_and_rows_public": False,
        "target_outcomes_observed_or_derived": False,
    }


def extract(
    *,
    manifest_path: Path,
    source_semantics_manifest_path: Path = SOURCE_SEMANTICS_MANIFEST,
    document_manifest_path: Path = DOCUMENT_MANIFEST,
    env_path: Path = PROJECT_ROOT / ".env",
    public_status_path: Path = DEFAULT_PUBLIC_STATUS,
) -> dict[str, Any]:
    manifest, config, selection = _verify_contract(
        manifest_path=manifest_path,
        source_semantics_manifest_path=source_semantics_manifest_path,
        document_manifest_path=document_manifest_path,
        env_path=env_path,
    )
    extraction = build_extraction(selection=selection, store_root=config.root)
    extraction["manifest_sha256"] = manifest["manifest_sha256"]
    private_path = _extraction_path(config.root)
    _write_gzip_json(private_path, extraction)
    public = {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "status": "EXTRACTION_COMPLETE",
        "counts": extraction["counts"],
        "private_extraction_sha256": _sha256_file(private_path),
        "review_complete": False,
        "source_text_urls_symbols_dates_and_rows_public": False,
        "target_outcomes_observed_or_derived": False,
    }
    _write_json(public_status_path, public)
    return public


def _pattern_matches(
    text: str, patterns: Sequence[tuple[str, str]]
) -> list[dict[str, str]]:
    values: list[dict[str, str]] = []
    for name, pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE | re.DOTALL)
        if match is None:
            continue
        start = max(0, match.start() - 80)
        end = min(len(text), match.end() + 80)
        values.append(
            {
                "rule": name,
                "excerpt": re.sub(r"\s+", " ", text[start:end]).strip(),
            }
        )
    return values


def _automatic_decision(row: Mapping[str, Any]) -> dict[str, Any]:
    extracted = row["extraction"]
    text = str(extracted.get("text") or "")
    accepted = datetime.fromisoformat(str(row["accepted_at"])).astimezone(UTC)
    positive = _pattern_matches(text, POSITIVE_PATTERNS)
    negative = _pattern_matches(text, NEGATIVE_PATTERNS)
    non_material = _pattern_matches(text, NON_MATERIAL_PATTERNS)
    financing = bool(extracted.get("financing_or_dilution_terms_present"))
    diagnostic_failures = extracted.get("diagnostic_failures") or []
    if diagnostic_failures:
        relevance = "UNRESOLVED"
        semantics = "UNRESOLVED"
        evidence: list[str] = []
    elif financing:
        relevance = "RELEVANT"
        semantics = "FINANCING_OR_DILUTION_CONFLICT"
        evidence = ["frozen financing or dilution pattern matched"]
    elif positive and negative:
        relevance = "RELEVANT"
        semantics = "MIXED_OR_CONTRADICTORY"
        evidence = [
            f"positive:{value['rule']}:{value['excerpt']}" for value in positive
        ] + [f"negative:{value['rule']}:{value['excerpt']}" for value in negative]
    elif positive:
        relevance = "RELEVANT"
        semantics = "DIRECT_POSITIVE"
        evidence = [
            f"positive:{value['rule']}:{value['excerpt']}" for value in positive
        ]
    elif negative:
        relevance = "RELEVANT"
        semantics = "DIRECT_NEGATIVE"
        evidence = [
            f"negative:{value['rule']}:{value['excerpt']}" for value in negative
        ]
    elif non_material:
        relevance = "RELEVANT"
        semantics = "NON_MATERIAL_OR_CONTEXT_ONLY"
        evidence = [
            f"non_material:{value['rule']}:{value['excerpt']}"
            for value in non_material
        ]
    else:
        relevance = "RELEVANT"
        semantics = "UNRESOLVED"
        evidence = []
    return {
        "row_id": row["row_id"],
        "source_ownership": "VERIFIED",
        "ownership_evidence": ["SEC-operated accession-bound archive endpoint"],
        "issuer_binding": "VERIFIED",
        "issuer_binding_methods": [
            "security_master_cik_to_document_cik"
        ],
        "relevance": relevance,
        "published_at_utc": accepted.isoformat(),
        "published_date": None,
        "source_timezone": "America/New_York",
        "timestamp_precision": "DATETIME",
        "timestamp_evidence_kind": "SOURCE_CONTROLLED_EXPLICIT_DATETIME",
        "timestamp_conflict": False,
        "semantics": semantics,
        "semantic_evidence": evidence,
        "financing_or_dilution_conflict": financing,
        "notes": (
            "Frozen deterministic SEC source review; unresolved means no supported "
            "direction pattern matched."
        ),
    }


def build_review(extraction: Mapping[str, Any]) -> dict[str, Any]:
    reviewed_rows: list[dict[str, Any]] = []
    for row in extraction["rows"]:
        decision = _automatic_decision(row)
        validated, timestamp = shared_semantics.validate_review_decision(
            decision, row=row
        )
        terminal, diagnostics = shared_semantics.terminal_disposition(
            row, validated, timestamp
        )
        reviewed_rows.append(
            {
                **dict(row),
                "review": validated,
                "accepted_timestamp": timestamp,
                "diagnostic_failures": diagnostics,
                "terminal_disposition": terminal,
            }
        )
    reviewed_rows.sort(key=lambda value: str(value["row_id"]))
    join_counts = Counter(
        str(row["terminal_disposition"]) for row in reviewed_rows
    )
    by_pair: dict[tuple[str, str], list[Mapping[str, Any]]] = defaultdict(list)
    for row in reviewed_rows:
        by_pair[(str(row["date"]), str(row["instrument_id"]))].append(row)
    pair_dispositions = {
        _sha256_json(key): shared_semantics._pair_disposition(rows)
        for key, rows in sorted(by_pair.items())
    }
    for pair in extraction["no_source_pairs"]:
        key = (str(pair["date"]), str(pair["instrument_id"]))
        pair_dispositions[_sha256_json(key)] = "SOURCE_OWNERSHIP_UNRESOLVED"
    if len(pair_dispositions) != int(extraction["counts"]["selected_pairs"]):
        raise DevelopmentCatalystSourceSemanticsError(
            "reviewed pair denominator differs"
        )
    pair_counts = Counter(pair_dispositions.values())
    verified_positive_pairs = pair_counts["VERIFIED_POSITIVE_PRIMARY"]
    return {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "manifest_sha256": extraction["manifest_sha256"],
        "status": "REVIEW_COMPLETE",
        "counts": dict(extraction["counts"]),
        "terminal_join_counts": {
            key: join_counts.get(key, 0)
            for key in shared_semantics.TERMINAL_PRECEDENCE
        },
        "terminal_pair_counts": {
            key: pair_counts.get(key, 0)
            for key in shared_semantics.TERMINAL_PRECEDENCE
        },
        "pair_dispositions": pair_dispositions,
        "verified_positive_pairs": verified_positive_pairs,
        "complete_unchanged_v3_non_return_survivors": 0,
        "rows": reviewed_rows,
        "row_sha256": _sha256_json(reviewed_rows),
        "next_phase": (
            "SOURCE_RECOVERY"
            if verified_positive_pairs < 20
            else "DEVELOPMENT_ACQUISITION"
        ),
        "outcome_contract_permitted": False,
        "source_text_urls_symbols_dates_reviews_and_rows_public": False,
        "target_outcomes_observed_or_derived": False,
    }


def review(
    *,
    manifest_path: Path,
    source_semantics_manifest_path: Path = SOURCE_SEMANTICS_MANIFEST,
    document_manifest_path: Path = DOCUMENT_MANIFEST,
    env_path: Path = PROJECT_ROOT / ".env",
    public_status_path: Path = DEFAULT_PUBLIC_STATUS,
) -> dict[str, Any]:
    manifest, config, _selection = _verify_contract(
        manifest_path=manifest_path,
        source_semantics_manifest_path=source_semantics_manifest_path,
        document_manifest_path=document_manifest_path,
        env_path=env_path,
    )
    extraction = _read_gzip_object(_extraction_path(config.root))
    if (
        extraction.get("manifest_sha256") != manifest["manifest_sha256"]
        or extraction.get("status") != "EXTRACTION_COMPLETE"
        or extraction.get("target_outcomes_observed_or_derived") is not False
    ):
        raise DevelopmentCatalystSourceSemanticsError(
            "private source extraction is incomplete or stale"
        )
    reviewed = build_review(extraction)
    private_path = _reviewed_path(config.root)
    _write_gzip_json(private_path, reviewed)
    public = {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "status": "REVIEW_COMPLETE",
        "counts": reviewed["counts"],
        "terminal_join_counts": reviewed["terminal_join_counts"],
        "terminal_pair_counts": reviewed["terminal_pair_counts"],
        "verified_positive_pairs": reviewed["verified_positive_pairs"],
        "complete_unchanged_v3_non_return_survivors": 0,
        "private_reviewed_result_sha256": _sha256_file(private_path),
        "next_phase": reviewed["next_phase"],
        "outcome_contract_permitted": False,
        "source_text_urls_symbols_dates_reviews_and_rows_public": False,
        "target_outcomes_observed_or_derived": False,
    }
    _write_json(public_status_path, public)
    return public


def _public_result(
    *, manifest: Mapping[str, Any], reviewed: Mapping[str, Any], private_path: Path
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
        "positive_capacity_gate_passed": positive >= 20,
        "complete_unchanged_v3_non_return_survivors": 0,
        "minimum_non_return_survivors_before_outcomes": 20,
        "next_phase": reviewed["next_phase"],
        "private_result_sha256": _sha256_file(private_path),
        "inspection": {
            "selection_rebuilt": True,
            "source_bytes_rehashed": True,
            "extraction_rebuilt": True,
            "terminal_counts_rebuilt": True,
            "accepted_timestamps_rebuilt": True,
            "one_terminal_reason_per_join": sum(
                int(value)
                for value in reviewed["terminal_join_counts"].values()
            )
            == int(reviewed["counts"]["pair_source_joins"]),
            "complete_pair_denominator_reconciled": sum(
                int(value)
                for value in reviewed["terminal_pair_counts"].values()
            )
            == int(reviewed["counts"]["selected_pairs"]),
        },
        "claim_boundary": (
            "Primary SEC source ownership, exact CIK issuer binding, official "
            "acceptance time, deterministic direction, conflicts, materiality, and "
            "terminal disposition only. Returns and target-session outcomes remain "
            "inaccessible."
        ),
        "outcome_contract_permitted": False,
        "production_rule_change_earned": False,
        "source_text_urls_symbols_dates_reviews_and_rows_public": False,
        "target_outcomes_observed_or_derived": False,
        "valid": True,
    }


def inspect(
    *,
    manifest_path: Path,
    source_semantics_manifest_path: Path = SOURCE_SEMANTICS_MANIFEST,
    document_manifest_path: Path = DOCUMENT_MANIFEST,
    env_path: Path = PROJECT_ROOT / ".env",
    public_result_path: Path = DEFAULT_PUBLIC_RESULT,
) -> dict[str, Any]:
    manifest, config, selection = _verify_contract(
        manifest_path=manifest_path,
        source_semantics_manifest_path=source_semantics_manifest_path,
        document_manifest_path=document_manifest_path,
        env_path=env_path,
    )
    extraction = _read_gzip_object(_extraction_path(config.root))
    reviewed_path = _reviewed_path(config.root)
    reviewed = _read_gzip_object(reviewed_path)
    rebuilt_extraction = build_extraction(
        selection=selection, store_root=config.root
    )
    rebuilt_extraction["manifest_sha256"] = manifest["manifest_sha256"]
    if extraction != rebuilt_extraction:
        raise DevelopmentCatalystSourceSemanticsError(
            "independent source extraction rebuild differs"
        )
    rebuilt_review = build_review(rebuilt_extraction)
    if reviewed != rebuilt_review:
        raise DevelopmentCatalystSourceSemanticsError(
            "independent source review rebuild differs"
        )
    if reviewed.get("target_outcomes_observed_or_derived") is not False:
        raise DevelopmentCatalystSourceSemanticsError(
            "reviewed source result outcome lock is open"
        )
    result = _public_result(
        manifest=manifest, reviewed=reviewed, private_path=reviewed_path
    )
    _write_json(public_result_path, result)
    return result


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "command", choices=("freeze", "extract", "review", "inspect", "inspect-contract")
    )
    parser.add_argument("--manifest", type=Path)
    parser.add_argument(
        "--source-semantics-manifest",
        type=Path,
        default=SOURCE_SEMANTICS_MANIFEST,
    )
    parser.add_argument(
        "--document-manifest", type=Path, default=DOCUMENT_MANIFEST
    )
    parser.add_argument("--env-file", type=Path, default=PROJECT_ROOT / ".env")
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--public-status", type=Path, default=DEFAULT_PUBLIC_STATUS)
    parser.add_argument("--public-result", type=Path, default=DEFAULT_PUBLIC_RESULT)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        if args.command == "freeze":
            path, manifest = freeze_contract(
                source_semantics_manifest_path=args.source_semantics_manifest,
                document_manifest_path=args.document_manifest,
                env_path=args.env_file,
                output_root=args.output_root,
            )
            output = {
                "dataset_id": DATASET_ID,
                "manifest_sha256": manifest["manifest_sha256"],
                "path": str(path),
                "counts": {
                    key: manifest["selection_contract"][key]
                    for key in (
                        "selected_pairs",
                        "source_documents",
                        "pair_source_joins",
                        "pairs_with_source",
                        "pairs_without_source",
                    )
                },
            }
        elif args.manifest is None:
            raise DevelopmentCatalystSourceSemanticsError(
                "--manifest is required"
            )
        elif args.command == "inspect-contract":
            output = inspect_contract(
                manifest_path=args.manifest,
                source_semantics_manifest_path=args.source_semantics_manifest,
                document_manifest_path=args.document_manifest,
                env_path=args.env_file,
                status_path=args.public_status,
            )
        elif args.command == "extract":
            output = extract(
                manifest_path=args.manifest,
                source_semantics_manifest_path=args.source_semantics_manifest,
                document_manifest_path=args.document_manifest,
                env_path=args.env_file,
                public_status_path=args.public_status,
            )
        elif args.command == "review":
            output = review(
                manifest_path=args.manifest,
                source_semantics_manifest_path=args.source_semantics_manifest,
                document_manifest_path=args.document_manifest,
                env_path=args.env_file,
                public_status_path=args.public_status,
            )
        else:
            output = inspect(
                manifest_path=args.manifest,
                source_semantics_manifest_path=args.source_semantics_manifest,
                document_manifest_path=args.document_manifest,
                env_path=args.env_file,
                public_result_path=args.public_result,
            )
        print(json.dumps(output, indent=2, sort_keys=True))
        return 0
    except (
        DevelopmentCatalystSourceSemanticsError,
        HistoricalStoreError,
        LearningDataError,
        OSError,
        ValueError,
    ) as exc:
        print(
            json.dumps(
                {"error": str(exc), "error_type": type(exc).__name__},
                indent=2,
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

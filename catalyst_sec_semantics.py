"""Freeze and inspect outcome-blind semantics for recovered SEC source joins."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import io
import json
import os
import re
import sys
from collections import Counter, defaultdict
from collections.abc import Mapping
from datetime import UTC, datetime, time
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import catalyst_sec_filing_metadata as metadata
import catalyst_source_recovery as recovery
import catalyst_source_semantics as source_semantics
from historical_store import HistoricalDayStore
from learning_data import (
    LearningDataError,
    freeze_dataset_contract,
    load_frozen_dataset_contract,
)


PROJECT_ROOT = Path(__file__).resolve().parent
DATASET_ID = "dataset-catalyst-sec-semantics-2026-07-19-expansion-v1"
SOURCE_RECOVERY_DATASET_ID = (
    "dataset-catalyst-source-recovery-sec-2026-07-19-expansion-v1"
)
SOURCE_METADATA_DATASET_ID = (
    "dataset-catalyst-sec-filing-metadata-2026-07-19-expansion-v1"
)
SOURCE_RECOVERY_MANIFEST = (
    PROJECT_ROOT
    / "historical_batches"
    / "catalyst_source_recovery"
    / "manifests"
    / (
        "dataset-catalyst-source-recovery-sec-2026-07-19-expansion-v1-"
        "2f4258b23e2aac76529754f51e5df6618db3e655d01491bb7cb01e14a5b0d374.json"
    )
)
SOURCE_RECOVERY_RESULT = (
    PROJECT_ROOT / "research_results" / "2026-07-19-catalyst-source-recovery-sec.json"
)
SOURCE_METADATA_MANIFEST = (
    PROJECT_ROOT
    / "historical_batches"
    / "catalyst_sec_filing_metadata"
    / "manifests"
    / (
        "dataset-catalyst-sec-filing-metadata-2026-07-19-expansion-v1-"
        "46864d4285439be3e61d44dc98779b288a8bdd2b6f6f436be22ba812fe385bd2.json"
    )
)
SOURCE_METADATA_RESULT = (
    PROJECT_ROOT / "research_results" / "2026-07-19-catalyst-sec-filing-metadata.json"
)
BASE_SEMANTICS_RESULT = (
    PROJECT_ROOT / "research_results" / "2026-07-19-catalyst-source-semantics.json"
)
DEFAULT_OUTPUT_ROOT = (
    PROJECT_ROOT / "historical_batches" / "catalyst_sec_semantics" / "manifests"
)
DEFAULT_PUBLIC_STATUS = (
    PROJECT_ROOT
    / "historical_batches"
    / "catalyst_sec_semantics"
    / "collection-status.json"
)
DEFAULT_PUBLIC_RESULT = (
    PROJECT_ROOT / "research_results" / "2026-07-19-catalyst-sec-semantics.json"
)
DEFAULT_REVIEW_TEMPLATE = (
    PROJECT_ROOT
    / "learning_runs"
    / "production_validation"
    / "sec-semantics-review.json"
)

EXPECTED_SOURCES = 26
EXPECTED_JOINS = 37
EXPECTED_PAIRS = 31
EXPECTED_ACCESSION_JOINS = 34
EXPECTED_DIRECTORY_CIK_MATCH_JOINS = 9
MINIMUM_POSITIVE_CAPACITY = 20
TERMINAL_PRECEDENCE = (
    "NO_ACCESSION",
    "RECOVERY_CAPTURE_ERROR",
    "RECOVERY_HTTP_ERROR",
    "FILING_METADATA_ERROR",
    "SOURCE_OWNERSHIP_UNRESOLVED",
    "DOCUMENT_CIK_MISMATCH",
    "ISSUER_BINDING_UNRESOLVED",
    "PRIMARY_SOURCE_IRRELEVANT",
    "ACCEPTANCE_TIME_MISSING",
    "ACCEPTANCE_TIME_CONFLICT",
    "ACCEPTED_AFTER_0935",
    "DOCUMENT_SEMANTICS_UNRESOLVED",
    "NON_MATERIAL_OR_CONTEXT_ONLY",
    "VERIFIED_CONFLICT",
    "VERIFIED_NEGATIVE_PRIMARY",
    "VERIFIED_POSITIVE_PRIMARY",
)
REVIEW_FIELDS = {
    "row_id",
    "issuer_binding",
    "issuer_binding_methods",
    "issuer_binding_evidence",
    "relevance",
    "semantics",
    "semantic_evidence",
    "financing_or_dilution_conflict",
    "notes",
}
OUTCOME_KEY_PATTERN = re.compile(
    r"(?i)(?:outcome|return|net_r|profit|loss|target_hit|stop_hit|future_price)"
)


class SecSemanticsError(RuntimeError):
    """The frozen SEC semantics contract cannot be honored."""


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


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SecSemanticsError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise SecSemanticsError(f"{path} must contain an object")
    return value


def _read_gzip(path: Path) -> dict[str, Any]:
    try:
        with gzip.open(path, "rt", encoding="utf-8") as source:
            value = json.load(source)
    except (OSError, json.JSONDecodeError) as exc:
        raise SecSemanticsError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise SecSemanticsError(f"{path} must contain an object")
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


def _write_gzip(path: Path, value: Any) -> None:
    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode="wb", compresslevel=6, mtime=0) as target:
        target.write(json.dumps(value, indent=2, sort_keys=True).encode())
        target.write(b"\n")
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
        raise SecSemanticsError(
            f"public path must be repository-relative: {path}"
        ) from exc


def _timestamp_now() -> str:
    return datetime.now(UTC).isoformat()


def _recovery_root(store_root: Path) -> Path:
    return (
        store_root
        / "_derived"
        / "catalyst_source_recovery"
        / SOURCE_RECOVERY_DATASET_ID
    )


def _metadata_root(store_root: Path) -> Path:
    return (
        store_root
        / "_derived"
        / "catalyst_sec_filing_metadata"
        / SOURCE_METADATA_DATASET_ID
    )


def _private_root(store_root: Path) -> Path:
    return store_root / "_derived" / "catalyst_sec_semantics" / DATASET_ID


def _selection_path(store_root: Path) -> Path:
    return _private_root(store_root) / "selection.json.gz"


def _extraction_path(store_root: Path) -> Path:
    return _private_root(store_root) / "extraction.json.gz"


def _reviewed_path(store_root: Path) -> Path:
    return _private_root(store_root) / "reviewed.json.gz"


def _source_response_path(store_root: Path, source_hash: str) -> Path:
    return _recovery_root(store_root) / "responses" / f"{source_hash}.bin"


def _metadata_response_path(store_root: Path, source_hash: str) -> Path:
    return _metadata_root(store_root) / "responses" / f"{source_hash}.bin"


def build_selection(source: Mapping[str, Any]) -> dict[str, Any]:
    records = []
    pairs: set[tuple[str, str]] = set()
    match_joins = 0
    accession_joins = 0
    for source_row in source.get("records", []):
        for pair in source_row.get("pairs", []):
            source_hash = str(source_row["source_sha256"])
            date_value = str(pair["date"])
            symbol = str(pair["symbol"])
            target_cik = str(int(pair["cik"]))
            filing_cik = (
                str(int(source_row["filing_cik"]))
                if source_row.get("filing_cik") is not None
                else None
            )
            directory_cik_match = filing_cik == target_cik if filing_cik else False
            accession_joins += source_row.get("accession") is not None
            match_joins += directory_cik_match
            row_id = (
                "sec-join-"
                + hashlib.sha256(
                    f"{source_hash}|{date_value}|{symbol}".encode()
                ).hexdigest()[:24]
            )
            records.append(
                {
                    "row_id": row_id,
                    "source_sha256": source_hash,
                    "accession": source_row.get("accession"),
                    "filing_cik": filing_cik,
                    "directory_cik_match": directory_cik_match,
                    "date": date_value,
                    "symbol": symbol,
                    "instrument_id": pair["instrument_id"],
                    "primary_exchange": pair["primary_exchange"],
                    "issuer_name": pair["issuer_name"],
                    "target_cik": target_cik,
                    "decision_cutoff_et": datetime.combine(
                        datetime.fromisoformat(date_value).date(),
                        time(9, 35),
                        tzinfo=ZoneInfo("America/New_York"),
                    ).isoformat(),
                }
            )
            pairs.add((date_value, symbol))
    records.sort(key=lambda row: row["row_id"])
    if (
        len(source.get("records", [])) != EXPECTED_SOURCES
        or len(records) != EXPECTED_JOINS
        or len(pairs) != EXPECTED_PAIRS
        or accession_joins != EXPECTED_ACCESSION_JOINS
        or match_joins != EXPECTED_DIRECTORY_CIK_MATCH_JOINS
    ):
        raise SecSemanticsError("SEC semantic selection counts differ")
    return {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "status": "FROZEN",
        "records": records,
        "counts": {
            "sources": EXPECTED_SOURCES,
            "pair_source_joins": len(records),
            "pairs": len(pairs),
            "accession_joins": accession_joins,
            "directory_cik_match_joins": match_joins,
        },
        "symbols_dates_accessions_sources_reviews_and_rows_public": False,
        "target_outcomes_observed_or_derived": False,
    }


def freeze_inputs(*, env_path: Path, output_root: Path) -> tuple[Path, dict[str, Any]]:
    store = HistoricalDayStore.from_env(env_path)
    recovery_manifest = load_frozen_dataset_contract(SOURCE_RECOVERY_MANIFEST)
    metadata_manifest = load_frozen_dataset_contract(SOURCE_METADATA_MANIFEST)
    recovery_result = _read_json(SOURCE_RECOVERY_RESULT)
    metadata_result = _read_json(SOURCE_METADATA_RESULT)
    base_result = _read_json(BASE_SEMANTICS_RESULT)
    if not all(
        result.get("status") == "READY" and result.get("inspected")
        for result in (recovery_result, metadata_result, base_result)
    ):
        raise SecSemanticsError("required source evidence is not inspected READY")
    source_selection_path = _recovery_root(store.root) / "selection.json.gz"
    source_recovery_index = _recovery_root(store.root) / "capture-index.json"
    source_metadata_index = _metadata_root(store.root) / "capture-index.json"
    selection = build_selection(_read_gzip(source_selection_path))
    selection_path = _selection_path(store.root)
    _write_gzip(selection_path, selection)
    contract = {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "registered_at": _timestamp_now(),
        "requested_dates": list(recovery_manifest["requested_dates"]),
        "dataset_payload": {
            "lane": "development",
            "claim_scope": "DEVELOPMENT_ONLY",
            "status": "COLLECTING",
            "evidence_paths": [
                _repo_path(SOURCE_RECOVERY_MANIFEST),
                _repo_path(SOURCE_RECOVERY_RESULT),
                _repo_path(SOURCE_METADATA_MANIFEST),
                _repo_path(SOURCE_METADATA_RESULT),
                _repo_path(BASE_SEMANTICS_RESULT),
                "CATALYST_SOURCE_RECOVERY.md",
            ],
            "inspected": False,
        },
        "selection_contract": {
            "recovery_manifest_sha256": recovery_manifest["manifest_sha256"],
            "metadata_manifest_sha256": metadata_manifest["manifest_sha256"],
            "recovery_result_sha256": _sha256_file(SOURCE_RECOVERY_RESULT),
            "metadata_result_sha256": _sha256_file(SOURCE_METADATA_RESULT),
            "base_semantics_result_sha256": _sha256_file(BASE_SEMANTICS_RESULT),
            "source_private_selection_sha256": _sha256_file(source_selection_path),
            "source_recovery_index_sha256": _sha256_file(source_recovery_index),
            "source_metadata_index_sha256": _sha256_file(source_metadata_index),
            "private_selection_sha256": _sha256_file(selection_path),
            **selection["counts"],
            "symbols_dates_accessions_sources_reviews_and_rows_public": False,
        },
        "derivation_contract": {
            "implementation_sha256": _sha256_file(Path(__file__)),
            "recovery_dependency_sha256": _sha256_file(Path(recovery.__file__)),
            "metadata_dependency_sha256": _sha256_file(Path(metadata.__file__)),
            "semantics_dependency_sha256": _sha256_file(
                Path(source_semantics.__file__)
            ),
            "network_access_allowed": False,
            "edgar_acceptance_timezone": "America/New_York",
            "decision_cutoff": "09:35 America/New_York",
            "exact_directory_cik_and_reviewed_issuer_binding_required": True,
            "financing_conflict_runs_before_positive": True,
            "event_taxonomy": list(source_semantics.EVENT_TAXONOMY),
            "terminal_precedence": list(TERMINAL_PRECEDENCE),
            "one_terminal_disposition_per_join": True,
            "automatic_strategy_application": False,
            "target_outcomes_observed_or_derived": False,
        },
    }
    return freeze_dataset_contract(contract, output_root)


def _verify_contract(
    manifest_path: Path, store: HistoricalDayStore
) -> tuple[dict[str, Any], dict[str, Any]]:
    manifest = load_frozen_dataset_contract(manifest_path)
    if manifest.get("dataset_id") != DATASET_ID:
        raise SecSemanticsError("unexpected SEC semantics dataset")
    selection_path = _selection_path(store.root)
    selection = _read_gzip(selection_path)
    if _sha256_file(selection_path) != manifest["selection_contract"].get(
        "private_selection_sha256"
    ):
        raise SecSemanticsError("SEC semantic selection changed")
    dependency_paths = {
        "implementation_sha256": Path(__file__),
        "recovery_dependency_sha256": Path(recovery.__file__),
        "metadata_dependency_sha256": Path(metadata.__file__),
        "semantics_dependency_sha256": Path(source_semantics.__file__),
    }
    for key, path in dependency_paths.items():
        if _sha256_file(path) != manifest["derivation_contract"].get(key):
            raise SecSemanticsError(f"{key} differs from frozen contract")
    return manifest, selection


def _visible_text(body: bytes) -> str:
    raw = body.decode("utf-8", errors="replace")
    parser = metadata._VisibleTextParser()
    parser.feed(raw)
    visible = " ".join(parser.parts)
    return visible if visible.strip() else " ".join(raw.split())


def build_extraction(
    *, store_root: Path, manifest: Mapping[str, Any], selection: Mapping[str, Any]
) -> dict[str, Any]:
    recovery_index = _read_json(_recovery_root(store_root) / "capture-index.json")
    metadata_index = _read_json(_metadata_root(store_root) / "capture-index.json")
    rows = []
    for selected in selection["records"]:
        source_hash = str(selected["source_sha256"])
        recovery_record = recovery_index["records"][source_hash]
        metadata_record = metadata_index.get("records", {}).get(source_hash)
        source_text = ""
        metadata_fields = {
            "accepted_datetime_candidates_et": [],
            "cik_candidates": [],
        }
        if recovery_record.get("status") == "RESPONSE_CAPTURED":
            source_text = _visible_text(
                _source_response_path(store_root, source_hash).read_bytes()
            )
        if metadata_record and metadata_record.get("status") == "RESPONSE_CAPTURED":
            metadata_fields = metadata.extract_filing_metadata(
                _metadata_response_path(store_root, source_hash).read_bytes()
            )
        rows.append(
            {
                **selected,
                "recovery_status": recovery_record.get("status"),
                "recovery_http_status": recovery_record.get("http_status"),
                "metadata_status": (
                    metadata_record.get("status") if metadata_record else None
                ),
                "metadata_http_status": (
                    metadata_record.get("http_status") if metadata_record else None
                ),
                "source_type": "SEC",
                "source_ownership": (
                    "VERIFIED"
                    if recovery_record.get("status") == "RESPONSE_CAPTURED"
                    and recovery_record.get("http_status") == 200
                    else "UNRESOLVED"
                ),
                "source_ownership_evidence": [
                    "frozen canonical SEC accession endpoint",
                    "independently inspected SEC response chain and body hash",
                ],
                "accepted_datetime_candidates_et": metadata_fields[
                    "accepted_datetime_candidates_et"
                ],
                "metadata_cik_candidates": metadata_fields["cik_candidates"],
                "source_text_sha256": hashlib.sha256(source_text.encode()).hexdigest(),
                "source_text_excerpt": source_text[:100000],
                "financing_or_dilution_terms_present": bool(
                    source_semantics.FINANCING_PATTERN.search(source_text)
                ),
            }
        )
    return {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "status": "EXTRACTION_COMPLETE",
        "rows": rows,
        "row_sha256": _sha256_json(rows),
        "counts": selection["counts"],
        "symbols_dates_accessions_sources_reviews_text_and_rows_public": False,
        "target_outcomes_observed_or_derived": False,
    }


def extract(*, manifest_path: Path, env_path: Path) -> dict[str, Any]:
    store = HistoricalDayStore.from_env(env_path)
    manifest, selection = _verify_contract(manifest_path, store)
    value = build_extraction(
        store_root=store.root, manifest=manifest, selection=selection
    )
    _write_gzip(_extraction_path(store.root), value)
    return {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "status": "EXTRACTION_COMPLETE",
        "private_extraction_sha256": _sha256_file(_extraction_path(store.root)),
        "counts": selection["counts"],
        "target_outcomes_observed_or_derived": False,
    }


def _review_template(extraction: Mapping[str, Any]) -> dict[str, Any]:
    decisions = []
    context = []
    for row in extraction["rows"]:
        decisions.append(
            {
                "row_id": row["row_id"],
                "issuer_binding": "UNRESOLVED",
                "issuer_binding_methods": [],
                "issuer_binding_evidence": [],
                "relevance": "UNRESOLVED",
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
                "target_cik": row["target_cik"],
                "filing_cik": row["filing_cik"],
                "directory_cik_match": row["directory_cik_match"],
                "accession": row["accession"],
                "accepted_datetime_candidates_et": row[
                    "accepted_datetime_candidates_et"
                ],
                "metadata_cik_candidates": row["metadata_cik_candidates"],
                "financing_or_dilution_terms_present": row[
                    "financing_or_dilution_terms_present"
                ],
                "source_text_excerpt": row["source_text_excerpt"][:6000],
            }
        )
    return {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "manifest_sha256": extraction["manifest_sha256"],
        "review_completed": False,
        "target_outcomes_observed_or_derived": False,
        "instructions": {
            "only_exact_cik_pre_cutoff_rows_need_semantic_judgment": True,
            "financing_conflict_precedes_positive": True,
            "terminal_disposition_is_derived_not_entered": True,
            "outcomes_forbidden": True,
        },
        "decisions": decisions,
        "context": context,
    }


def _strings(value: Any, field: str) -> list[str]:
    if not isinstance(value, list) or any(
        not isinstance(item, str) or not item.strip() for item in value
    ):
        raise SecSemanticsError(f"{field} must be an array of non-empty strings")
    return [item.strip() for item in value]


def _contains_outcome_key(value: Any) -> bool:
    if isinstance(value, Mapping):
        return any(
            OUTCOME_KEY_PATTERN.search(str(key)) or _contains_outcome_key(item)
            for key, item in value.items()
        )
    if isinstance(value, list):
        return any(_contains_outcome_key(item) for item in value)
    return False


def _accepted_timestamp(row: Mapping[str, Any]) -> dict[str, Any]:
    candidates = row["accepted_datetime_candidates_et"]
    if not candidates:
        return {"status": "MISSING", "accepted_utc": None}
    if len(set(candidates)) != 1:
        return {"status": "CONFLICT", "accepted_utc": None}
    accepted = datetime.fromisoformat(candidates[0]).replace(
        tzinfo=ZoneInfo("America/New_York")
    )
    target_day = datetime.fromisoformat(str(row["date"])).date()
    cutoff = datetime.combine(
        target_day, time(9, 35), tzinfo=ZoneInfo("America/New_York")
    )
    return {
        "status": "ACCEPTED" if accepted <= cutoff else "AFTER_CUTOFF",
        "accepted_utc": accepted.astimezone(UTC).isoformat(),
    }


def _validate_decision(value: Any, row: Mapping[str, Any]) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise SecSemanticsError("review decision must be an object")
    decision = dict(value)
    if set(decision) != REVIEW_FIELDS or decision.get("row_id") != row["row_id"]:
        raise SecSemanticsError("review decision fields or row identity differ")
    if decision["issuer_binding"] not in {"VERIFIED", "UNRESOLVED"}:
        raise SecSemanticsError("invalid issuer_binding")
    methods = _strings(decision["issuer_binding_methods"], "issuer_binding_methods")
    allowed_methods = {"SEC_DIRECTORY_CIK", "SEC_DOCUMENT_CIK"}
    if not set(methods) <= allowed_methods:
        raise SecSemanticsError("invalid issuer_binding_methods")
    binding = _strings(decision["issuer_binding_evidence"], "issuer_binding_evidence")
    if decision["issuer_binding"] == "VERIFIED" and (
        set(methods) != allowed_methods or not binding
    ):
        raise SecSemanticsError(
            "verified issuer binding needs both SEC CIK methods and evidence"
        )
    if decision["relevance"] not in {"RELEVANT", "IRRELEVANT", "UNRESOLVED"}:
        raise SecSemanticsError("invalid relevance")
    if decision["semantics"] not in source_semantics.EVENT_TAXONOMY:
        raise SecSemanticsError("invalid semantics")
    evidence = _strings(decision["semantic_evidence"], "semantic_evidence")
    if decision["semantics"] != "UNRESOLVED" and not evidence:
        raise SecSemanticsError("resolved semantics need evidence")
    if not isinstance(decision["financing_or_dilution_conflict"], bool):
        raise SecSemanticsError("financing conflict flag must be boolean")
    if not isinstance(decision["notes"], str):
        raise SecSemanticsError("notes must be a string")
    return decision


def terminal_disposition(
    row: Mapping[str, Any], decision: Mapping[str, Any]
) -> tuple[str, dict[str, Any]]:
    if row["accession"] is None:
        return "NO_ACCESSION", {"status": "MISSING", "accepted_utc": None}
    if row["recovery_status"] != "RESPONSE_CAPTURED":
        return "RECOVERY_CAPTURE_ERROR", {"status": "MISSING", "accepted_utc": None}
    if row["recovery_http_status"] != 200:
        return "RECOVERY_HTTP_ERROR", {"status": "MISSING", "accepted_utc": None}
    if (
        row["metadata_status"] != "RESPONSE_CAPTURED"
        or row["metadata_http_status"] != 200
    ):
        return "FILING_METADATA_ERROR", {"status": "MISSING", "accepted_utc": None}
    if row["source_ownership"] != "VERIFIED":
        return "SOURCE_OWNERSHIP_UNRESOLVED", {
            "status": "MISSING",
            "accepted_utc": None,
        }
    if (
        not row["directory_cik_match"]
        or row["target_cik"] not in row["metadata_cik_candidates"]
    ):
        return "DOCUMENT_CIK_MISMATCH", {"status": "MISSING", "accepted_utc": None}
    if decision["issuer_binding"] != "VERIFIED":
        return "ISSUER_BINDING_UNRESOLVED", {"status": "MISSING", "accepted_utc": None}
    if decision["relevance"] == "IRRELEVANT":
        return "PRIMARY_SOURCE_IRRELEVANT", {
            "status": "MISSING",
            "accepted_utc": None,
        }
    timestamp = _accepted_timestamp(row)
    if timestamp["status"] == "MISSING":
        return "ACCEPTANCE_TIME_MISSING", timestamp
    if timestamp["status"] == "CONFLICT":
        return "ACCEPTANCE_TIME_CONFLICT", timestamp
    if timestamp["status"] == "AFTER_CUTOFF":
        return "ACCEPTED_AFTER_0935", timestamp
    if decision["relevance"] != "RELEVANT" or decision["semantics"] == "UNRESOLVED":
        return "DOCUMENT_SEMANTICS_UNRESOLVED", timestamp
    semantics = decision["semantics"]
    if semantics == "NON_MATERIAL_OR_CONTEXT_ONLY":
        return "NON_MATERIAL_OR_CONTEXT_ONLY", timestamp
    if decision["financing_or_dilution_conflict"] or semantics in {
        "FINANCING_OR_DILUTION_CONFLICT",
        "MIXED_OR_CONTRADICTORY",
    }:
        return "VERIFIED_CONFLICT", timestamp
    if semantics == "DIRECT_NEGATIVE":
        return "VERIFIED_NEGATIVE_PRIMARY", timestamp
    if semantics == "DIRECT_POSITIVE":
        return "VERIFIED_POSITIVE_PRIMARY", timestamp
    raise SecSemanticsError("resolved decision has no terminal disposition")


def apply_review(
    extraction: Mapping[str, Any], review_input: Mapping[str, Any]
) -> dict[str, Any]:
    if not (
        review_input.get("schema_version") == 1
        and review_input.get("dataset_id") == DATASET_ID
        and review_input.get("manifest_sha256") == extraction["manifest_sha256"]
        and review_input.get("review_completed") is True
        and review_input.get("target_outcomes_observed_or_derived") is False
    ):
        raise SecSemanticsError("review input contract is invalid")
    review_without_lock = {
        key: value
        for key, value in review_input.items()
        if key != "target_outcomes_observed_or_derived"
    }
    if _contains_outcome_key(review_without_lock):
        raise SecSemanticsError("review input contains outcome-shaped field")
    decisions = review_input.get("decisions")
    if not isinstance(decisions, list) or len(decisions) != EXPECTED_JOINS:
        raise SecSemanticsError("review input must contain exactly 37 decisions")
    by_id = {
        str(value.get("row_id")): value
        for value in decisions
        if isinstance(value, Mapping)
    }
    if len(by_id) != EXPECTED_JOINS:
        raise SecSemanticsError("review input has duplicate or invalid row IDs")
    rows = []
    for row in extraction["rows"]:
        if row["row_id"] not in by_id:
            raise SecSemanticsError("review input row set differs")
        decision = _validate_decision(by_id[row["row_id"]], row)
        terminal, timestamp = terminal_disposition(row, decision)
        rows.append(
            {
                "row_id": row["row_id"],
                "source_sha256": row["source_sha256"],
                "date": row["date"],
                "symbol": row["symbol"],
                "terminal_disposition": terminal,
                "accepted_utc": timestamp["accepted_utc"],
                "decision": decision,
            }
        )
    join_counts = Counter(row["terminal_disposition"] for row in rows)
    by_pair: dict[tuple[str, str], list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        by_pair[(row["date"], row["symbol"])].append(row)
    pair_counts = Counter()
    for pair_rows in by_pair.values():
        dispositions = {str(row["terminal_disposition"]) for row in pair_rows}
        selected = next(
            (
                value
                for value in (
                    "VERIFIED_CONFLICT",
                    "VERIFIED_NEGATIVE_PRIMARY",
                    "VERIFIED_POSITIVE_PRIMARY",
                )
                if value in dispositions
            ),
            min(dispositions, key=TERMINAL_PRECEDENCE.index),
        )
        pair_counts[selected] += 1
    return {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "manifest_sha256": extraction["manifest_sha256"],
        "status": "REVIEW_COMPLETE",
        "rows": rows,
        "terminal_join_counts": {
            key: int(join_counts.get(key, 0)) for key in TERMINAL_PRECEDENCE
        },
        "terminal_pair_counts": {
            key: int(pair_counts.get(key, 0)) for key in TERMINAL_PRECEDENCE
        },
        "verified_positive_pairs": int(pair_counts["VERIFIED_POSITIVE_PRIMARY"]),
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
    manifest, _ = _verify_contract(manifest_path, store)
    extraction = _read_gzip(_extraction_path(store.root))
    if extraction.get("manifest_sha256") != manifest["manifest_sha256"]:
        raise SecSemanticsError("extraction belongs to another manifest")
    if review_input_path is None:
        template = _review_template(extraction)
        _write_json(review_template_path, template)
        return {
            "schema_version": 1,
            "dataset_id": DATASET_ID,
            "status": "AWAITING_REVIEW",
            "review_template": _repo_path(review_template_path),
            "rows": EXPECTED_JOINS,
            "target_outcomes_observed_or_derived": False,
        }
    reviewed = apply_review(extraction, _read_json(review_input_path))
    _write_gzip(_reviewed_path(store.root), reviewed)
    public = {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "status": "REVIEW_COMPLETE",
        "selection_counts": extraction["counts"],
        "terminal_join_counts": reviewed["terminal_join_counts"],
        "terminal_pair_counts": reviewed["terminal_pair_counts"],
        "verified_positive_pairs": reviewed["verified_positive_pairs"],
        "private_reviewed_result_sha256": _sha256_file(_reviewed_path(store.root)),
        "symbols_dates_accessions_sources_reviews_text_and_rows_public": False,
        "target_outcomes_observed_or_derived": False,
    }
    _write_json(public_status_path, public)
    return public


def inspect(
    *, manifest_path: Path, env_path: Path, public_result_path: Path
) -> dict[str, Any]:
    store = HistoricalDayStore.from_env(env_path)
    manifest, selection = _verify_contract(manifest_path, store)
    extraction = _read_gzip(_extraction_path(store.root))
    rebuilt = build_extraction(
        store_root=store.root, manifest=manifest, selection=selection
    )
    if extraction != rebuilt:
        raise SecSemanticsError("SEC semantics extraction does not rebuild")
    reviewed = _read_gzip(_reviewed_path(store.root))
    decisions = [row["decision"] for row in reviewed["rows"]]
    replayed = apply_review(
        extraction,
        {
            "schema_version": 1,
            "dataset_id": DATASET_ID,
            "manifest_sha256": manifest["manifest_sha256"],
            "review_completed": True,
            "target_outcomes_observed_or_derived": False,
            "decisions": decisions,
        },
    )
    if reviewed != replayed:
        raise SecSemanticsError("SEC semantics review does not rebuild")
    base_positive = int(_read_json(BASE_SEMANTICS_RESULT)["verified_positive_pairs"])
    sec_positive = int(reviewed["verified_positive_pairs"])
    maximum_combined_positive = base_positive + sec_positive
    result = {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "status": "READY",
        "inspected": True,
        "claim_scope": "DEVELOPMENT_ONLY",
        "selection_counts": selection["counts"],
        "terminal_join_counts": reviewed["terminal_join_counts"],
        "terminal_pair_counts": reviewed["terminal_pair_counts"],
        "verified_positive_pairs": sec_positive,
        "base_verified_positive_pairs": base_positive,
        "maximum_combined_verified_positive_pairs": maximum_combined_positive,
        "minimum_positive_capacity": MINIMUM_POSITIVE_CAPACITY,
        "capacity_gate_passed": maximum_combined_positive >= MINIMUM_POSITIVE_CAPACITY,
        "outcome_contract_permitted": False,
        "next_phase": "SOURCE_RECOVERY",
        "private_result_sha256": _sha256_file(_reviewed_path(store.root)),
        "inspection": {
            "selection_rebuilt": True,
            "extraction_rebuilt": True,
            "accepted_timestamps_rebuilt": True,
            "terminal_counts_rebuilt": True,
            "one_terminal_reason_per_join": sum(
                reviewed["terminal_join_counts"].values()
            )
            == EXPECTED_JOINS,
            "private_hashes_verified": True,
        },
        "production_rule_change_earned": False,
        "symbols_dates_accessions_sources_reviews_text_and_rows_public": False,
        "target_outcomes_observed_or_derived": False,
        "claim_boundary": (
            "Recovered SEC source ownership, target-CIK binding, EDGAR acceptance "
            "time, relevance, and event direction only; no outcomes or alpha."
        ),
    }
    _write_json(public_result_path, result)
    return result


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("freeze", "extract", "review", "inspect"))
    parser.add_argument("--manifest", type=Path)
    parser.add_argument("--env-file", type=Path, default=PROJECT_ROOT / ".env")
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--review-input", type=Path)
    parser.add_argument("--review-template", type=Path, default=DEFAULT_REVIEW_TEMPLATE)
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
            raise SecSemanticsError("--manifest is required")
        elif args.command == "extract":
            output = extract(manifest_path=args.manifest, env_path=args.env_file)
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
    except (SecSemanticsError, LearningDataError, OSError, ValueError) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}), file=sys.stderr)
        return 2
    print(json.dumps(output, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

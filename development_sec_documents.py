"""Freeze and inspect the exact accession-bound SEC primary-document graph."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import io
import json
import os
import re
import shutil
import sys
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import development_sec_sources as source_contract
import development_sec_submissions as main_collection
import development_sec_supplemental_collection as supplemental_collection
from historical_store import HistoricalStoreConfig, HistoricalStoreError
from learning_data import (
    LearningDataError,
    freeze_dataset_contract,
    load_frozen_dataset_contract,
)


PROJECT_ROOT = Path(__file__).resolve().parent
DATASET_ID = "dataset-development-sec-primary-documents-2026-07-19-v2"
MAIN_MANIFEST = main_collection.MANIFEST
MAIN_STATUS = main_collection.DEFAULT_PUBLIC_STATUS
MAIN_INSPECTION = main_collection.DEFAULT_PUBLIC_INSPECTION
SUPPLEMENTAL_MANIFEST = supplemental_collection.MANIFEST
SUPPLEMENTAL_STATUS = supplemental_collection.DEFAULT_PUBLIC_STATUS
SUPPLEMENTAL_INSPECTION = supplemental_collection.DEFAULT_PUBLIC_INSPECTION
DEFAULT_OUTPUT_ROOT = (
    PROJECT_ROOT / "historical_batches/development_tranche_v2/sec_document_manifests"
)
DEFAULT_PUBLIC_STATUS = (
    PROJECT_ROOT
    / "historical_batches/development_tranche_v2/sec-documents-contract-status.json"
)
DEFAULT_SOURCE_CONTRACT_DOC = PROJECT_ROOT / "DEVELOPMENT_SEC_SOURCES.md"
PRIVATE_NAMESPACE = "_derived/development_sec_sources"
PRIVATE_CONTRACT_FILE = "primary-document-request-map.json.gz"
TARGET_RESPONSE_NAMESPACE = "responses/documents"
ACCESSION_PATTERN = re.compile(r"^[0-9]{10}-[0-9]{2}-[0-9]{6}$")
DOCUMENT_FIELDS = (
    "cik",
    "form",
    "accession",
    "accepted_at",
    "filing_date",
    "report_date",
    "items",
    "primary_document",
    "source_url",
)
PAIR_FIELDS = (
    "date",
    "instrument_id",
    "primary_exchange",
    "rank",
    "symbol",
)


class DevelopmentSecDocumentsError(RuntimeError):
    """The primary-document request graph is unsafe, incomplete, or drifted."""


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
        raise DevelopmentSecDocumentsError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise DevelopmentSecDocumentsError(f"{path} must contain an object")
    return value


def _read_gzip_object(path: Path) -> dict[str, Any]:
    try:
        with gzip.open(path, "rt", encoding="utf-8") as source:
            value = json.load(source)
    except (OSError, json.JSONDecodeError) as exc:
        raise DevelopmentSecDocumentsError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise DevelopmentSecDocumentsError(f"{path} must contain an object")
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
        raise DevelopmentSecDocumentsError(
            f"public path must be repository relative: {path}"
        ) from exc


def _validated_dataset_id(dataset_id: str) -> str:
    if (
        not dataset_id
        or Path(dataset_id).name != dataset_id
        or dataset_id in {".", ".."}
    ):
        raise DevelopmentSecDocumentsError("dataset ID is unsafe")
    return dataset_id


def _private_contract_path(
    store_root: Path,
    source_dataset_id: str = source_contract.DATASET_ID,
    dataset_id: str = DATASET_ID,
) -> Path:
    return (
        store_root
        / PRIVATE_NAMESPACE
        / _validated_dataset_id(source_dataset_id)
        / "document_contracts"
        / _validated_dataset_id(dataset_id)
        / PRIVATE_CONTRACT_FILE
    )


def _target_response_root(
    store_root: Path, source_dataset_id: str = source_contract.DATASET_ID
) -> Path:
    return (
        store_root
        / PRIVATE_NAMESPACE
        / _validated_dataset_id(source_dataset_id)
        / TARGET_RESPONSE_NAMESPACE
    )


def _target_response_count(
    store_root: Path, source_dataset_id: str = source_contract.DATASET_ID
) -> int:
    root = _target_response_root(store_root, source_dataset_id)
    return sum(1 for path in root.rglob("*") if path.is_file()) if root.exists() else 0


def _recorded_public(
    *,
    path: Path,
    rebuilt: Mapping[str, Any],
    render: Any,
    expected_status: str,
    inspection_path: Path,
) -> dict[str, Any]:
    recorded = _read_object(path)
    publication = {"collector": {"commit": recorded.get("collector_commit")}}
    if recorded != render(rebuilt, publication):
        raise DevelopmentSecDocumentsError(f"source public status drifted: {path}")
    inspection = _read_object(inspection_path)
    if (
        inspection.get("status") != expected_status
        or inspection.get("valid") is not True
        or inspection.get("private_collection_content_sha256") != _sha256_json(rebuilt)
        or int(rebuilt.get("counts", {}).get("pending_requests", -1)) != 0
        or int(rebuilt.get("counts", {}).get("failed_requests", -1)) != 0
        or rebuilt.get("primary_documents_requested") is not False
        or rebuilt.get("target_outcomes_observed_or_derived") is not False
    ):
        raise DevelopmentSecDocumentsError(
            f"source inspection is incomplete or failed: {inspection_path}"
        )
    return inspection


def _load_source_state(
    *,
    source_dataset_id: str,
    supplemental_dataset_id: str,
    main_manifest_path: Path,
    main_status_path: Path,
    main_inspection_path: Path,
    supplemental_manifest_path: Path,
    supplemental_status_path: Path,
    supplemental_inspection_path: Path,
    env_path: Path,
) -> tuple[
    dict[str, Any],
    dict[str, Any],
    HistoricalStoreConfig,
    dict[str, Any],
    dict[str, Any],
]:
    source_dataset_id = _validated_dataset_id(source_dataset_id)
    supplemental_dataset_id = _validated_dataset_id(supplemental_dataset_id)
    for path in (
        Path(main_collection.__file__),
        main_manifest_path,
        main_status_path,
        main_inspection_path,
        Path(supplemental_collection.__file__),
        supplemental_manifest_path,
        supplemental_status_path,
        supplemental_inspection_path,
    ):
        main_collection._published_source(path)
    main_manifest, main_config, main_private, _ = main_collection._load_contract(
        dataset_id=source_dataset_id,
        manifest_path=main_manifest_path,
        env_path=env_path,
        require_published=False,
    )
    main_index = main_collection._build_collection_index(
        dataset_id=source_dataset_id,
        manifest=main_manifest,
        private=main_private,
        store_root=main_config.root,
    )
    if main_index != _read_gzip_object(
        main_collection._collection_index_path(main_config.root, source_dataset_id)
    ):
        raise DevelopmentSecDocumentsError("main submissions private index drifted")
    _recorded_public(
        path=main_status_path,
        rebuilt=main_index,
        render=lambda index, publication: main_collection._public_status(
            dataset_id=source_dataset_id,
            index=index,
            publication=publication,
        ),
        expected_status="SUBMISSIONS_INSPECTED",
        inspection_path=main_inspection_path,
    )

    supplemental_manifest, supplemental_config, supplemental_private, _ = (
        supplemental_collection._load_contract(
            dataset_id=supplemental_dataset_id,
            source_dataset_id=source_dataset_id,
            manifest_path=supplemental_manifest_path,
            env_path=env_path,
            require_published=False,
        )
    )
    if supplemental_config.root.resolve() != main_config.root.resolve():
        raise DevelopmentSecDocumentsError("source collections use different stores")
    supplemental_index = supplemental_collection._build_index(
        dataset_id=supplemental_dataset_id,
        source_dataset_id=source_dataset_id,
        manifest=supplemental_manifest,
        private=supplemental_private,
        store_root=supplemental_config.root,
    )
    if supplemental_index != _read_gzip_object(
        supplemental_collection._index_path(supplemental_config.root, source_dataset_id)
    ):
        raise DevelopmentSecDocumentsError("supplemental private index drifted")
    _recorded_public(
        path=supplemental_status_path,
        rebuilt=supplemental_index,
        render=lambda index, publication: supplemental_collection._public(
            supplemental_dataset_id, index, publication
        ),
        expected_status="SUPPLEMENTAL_INSPECTED",
        inspection_path=supplemental_inspection_path,
    )
    return (
        main_manifest,
        supplemental_manifest,
        main_config,
        main_index,
        supplemental_index,
    )


def _document_url(cik: str, accession: str, primary_document: str) -> str:
    return (
        "https://www.sec.gov/Archives/edgar/data/"
        f"{int(cik)}/{accession.replace('-', '')}/{primary_document}"
    )


def _normalize_document(value: Mapping[str, Any]) -> dict[str, Any]:
    if any(field not in value for field in DOCUMENT_FIELDS):
        raise DevelopmentSecDocumentsError("candidate document fields are incomplete")
    document = {field: value[field] for field in DOCUMENT_FIELDS}
    cik = str(document["cik"])
    accession = str(document["accession"])
    primary = str(document["primary_document"])
    source_url = str(document["source_url"])
    form = str(document["form"])
    try:
        accepted = datetime.fromisoformat(str(document["accepted_at"]))
    except ValueError as exc:
        raise DevelopmentSecDocumentsError(
            "candidate document lacks a precise acceptance timestamp"
        ) from exc
    if (
        not cik.isdigit()
        or len(cik) != 10
        or ACCESSION_PATTERN.fullmatch(accession) is None
        or not primary
        or Path(primary).name != primary
        or form not in source_contract.SEC_FORMS
        or accepted.tzinfo is None
        or source_url != _document_url(cik, accession, primary)
    ):
        raise DevelopmentSecDocumentsError(
            "candidate document identity or SEC endpoint is invalid"
        )
    items = document["items"]
    if not isinstance(items, list) or not all(isinstance(item, str) for item in items):
        raise DevelopmentSecDocumentsError("candidate document items are malformed")
    return document


def _normalize_join(
    value: Mapping[str, Any], *, candidate: Mapping[str, Any], origin: str
) -> dict[str, Any]:
    filing = value.get("filing")
    if not isinstance(filing, Mapping) or any(
        field not in value for field in PAIR_FIELDS
    ):
        raise DevelopmentSecDocumentsError("pair/document join is incomplete")
    joined_document = _normalize_document({"cik": candidate["cik"], **filing})
    if joined_document != candidate:
        raise DevelopmentSecDocumentsError("pair/document join metadata differs")
    return {
        **{field: value[field] for field in PAIR_FIELDS},
        "source_origin": origin,
        "filing": {
            field: candidate[field] for field in DOCUMENT_FIELDS if field != "cik"
        },
    }


def _build_request_graph(
    *,
    main_index: Mapping[str, Any],
    supplemental_index: Mapping[str, Any],
    dataset_id: str = DATASET_ID,
) -> dict[str, Any]:
    dataset_id = _validated_dataset_id(dataset_id)
    sources = (
        ("MAIN_SUBMISSIONS", main_index),
        ("SUPPLEMENTAL_SUBMISSIONS", supplemental_index),
    )
    documents: dict[str, dict[str, Any]] = {}
    origins: dict[str, set[str]] = {}
    source_candidates: dict[str, int] = {}
    source_joins: dict[str, int] = {}
    joins: list[dict[str, Any]] = []
    join_identities: set[str] = set()
    for origin, index in sources:
        candidate_values = index.get("candidate_document_requests")
        join_values = index.get("pair_filing_joins")
        if not isinstance(candidate_values, list) or not isinstance(join_values, list):
            raise DevelopmentSecDocumentsError("source document graph is malformed")
        source_candidates[origin] = len(candidate_values)
        source_joins[origin] = len(join_values)
        origin_documents: dict[str, dict[str, Any]] = {}
        for value in candidate_values:
            if not isinstance(value, Mapping):
                raise DevelopmentSecDocumentsError("candidate document is malformed")
            document = _normalize_document(value)
            url = str(document["source_url"])
            if url in origin_documents:
                raise DevelopmentSecDocumentsError(
                    "source candidate graph contains a duplicate URL"
                )
            origin_documents[url] = document
            if url in documents and documents[url] != document:
                raise DevelopmentSecDocumentsError(
                    "duplicate document URL has conflicting metadata"
                )
            documents[url] = document
            origins.setdefault(url, set()).add(origin)
        for value in join_values:
            if not isinstance(value, Mapping) or not isinstance(
                value.get("filing"), Mapping
            ):
                raise DevelopmentSecDocumentsError("pair/document join is malformed")
            url = str(value["filing"].get("source_url") or "")
            candidate = origin_documents.get(url)
            if candidate is None:
                raise DevelopmentSecDocumentsError(
                    "pair/document join references an unknown source candidate"
                )
            join = _normalize_join(value, candidate=candidate, origin=origin)
            identity = _sha256_json(join)
            if identity in join_identities:
                raise DevelopmentSecDocumentsError(
                    "source pair/document graph contains a duplicate join"
                )
            join_identities.add(identity)
            joins.append(join)
    requests: list[dict[str, Any]] = []
    for url, document in sorted(documents.items()):
        digest = hashlib.sha256(url.encode()).hexdigest()
        requests.append(
            {
                **document,
                "source_origins": sorted(origins[url]),
                "shared_cache_relative_path": f"documents/{digest}.source",
                "target_response_relative_path": f"documents/{digest}.json.gz",
            }
        )
    joins.sort(
        key=lambda row: (
            str(row["date"]),
            int(row["rank"]),
            str(row["filing"]["accepted_at"]),
            str(row["source_origin"]),
        )
    )
    counts = {
        "main_candidate_documents": source_candidates["MAIN_SUBMISSIONS"],
        "supplemental_candidate_documents": source_candidates[
            "SUPPLEMENTAL_SUBMISSIONS"
        ],
        "candidate_document_observations": sum(source_candidates.values()),
        "unique_document_requests": len(requests),
        "duplicate_document_observations": sum(source_candidates.values())
        - len(requests),
        "main_pair_document_joins": source_joins["MAIN_SUBMISSIONS"],
        "supplemental_pair_document_joins": source_joins["SUPPLEMENTAL_SUBMISSIONS"],
        "pair_document_joins": len(joins),
        "unique_pairs_with_document": len(
            {(str(row["date"]), str(row["instrument_id"])) for row in joins}
        ),
    }
    return {
        "schema_version": 1,
        "dataset_id": dataset_id,
        "counts": counts,
        "document_requests": requests,
        "document_request_graph_sha256": _sha256_json(requests),
        "pair_document_joins": joins,
        "pair_document_join_graph_sha256": _sha256_json(joins),
        "source_candidate_graph_sha256": {
            "MAIN_SUBMISSIONS": main_index.get("candidate_document_graph_sha256"),
            "SUPPLEMENTAL_SUBMISSIONS": supplemental_index.get(
                "candidate_document_graph_sha256"
            ),
        },
        "primary_document_responses_present": False,
        "source_semantics_observed_or_derived": False,
        "target_outcomes_observed_or_derived": False,
    }


def _implementation_contract() -> dict[str, Any]:
    paths = {
        "document_contract": Path(__file__),
        "main_submissions_collector": Path(main_collection.__file__),
        "supplemental_collector": Path(supplemental_collection.__file__),
        "sec_source_contract": Path(source_contract.__file__),
    }
    return {
        "files": {
            name: {"path": _repo_path(path), "sha256": _sha256_file(path)}
            for name, path in paths.items()
        },
        "python": sys.version.split()[0],
    }


def _stable_contract(
    *,
    dataset_id: str,
    source_dataset_id: str,
    supplemental_dataset_id: str,
    main_manifest_path: Path,
    main_status_path: Path,
    main_inspection_path: Path,
    supplemental_manifest_path: Path,
    supplemental_status_path: Path,
    supplemental_inspection_path: Path,
    env_path: Path,
    require_published_implementation: bool,
) -> tuple[dict[str, Any], HistoricalStoreConfig, dict[str, Any]]:
    dataset_id = _validated_dataset_id(dataset_id)
    source_dataset_id = _validated_dataset_id(source_dataset_id)
    supplemental_dataset_id = _validated_dataset_id(supplemental_dataset_id)
    if require_published_implementation:
        main_collection._published_source(Path(__file__))
    (
        main_manifest,
        supplemental_manifest,
        config,
        main_index,
        supplemental_index,
    ) = _load_source_state(
        source_dataset_id=source_dataset_id,
        supplemental_dataset_id=supplemental_dataset_id,
        main_manifest_path=main_manifest_path,
        main_status_path=main_status_path,
        main_inspection_path=main_inspection_path,
        supplemental_manifest_path=supplemental_manifest_path,
        supplemental_status_path=supplemental_status_path,
        supplemental_inspection_path=supplemental_inspection_path,
        env_path=env_path,
    )
    graph = _build_request_graph(
        main_index=main_index,
        supplemental_index=supplemental_index,
        dataset_id=dataset_id,
    )
    response_count = _target_response_count(config.root, source_dataset_id)
    if response_count:
        raise DevelopmentSecDocumentsError(
            "target primary-document responses exist before contract freeze"
        )
    main_request = main_manifest["request_contract"]
    main_strategy = main_manifest["lineage_contract"]["strategy"]
    supplemental_strategy = supplemental_manifest["lineage_contract"]["strategy"]
    if main_strategy != supplemental_strategy:
        raise DevelopmentSecDocumentsError("source strategy lineage differs")
    stable = {
        "lineage_contract": {
            "main_submissions_manifest": {
                "path": _repo_path(main_manifest_path),
                "sha256": _sha256_file(main_manifest_path),
                "manifest_sha256": main_manifest["manifest_sha256"],
            },
            "main_submissions_status": {
                "path": _repo_path(main_status_path),
                "sha256": _sha256_file(main_status_path),
            },
            "main_submissions_inspection": {
                "path": _repo_path(main_inspection_path),
                "sha256": _sha256_file(main_inspection_path),
            },
            "main_private_collection_content_sha256": _sha256_json(main_index),
            "supplemental_manifest": {
                "path": _repo_path(supplemental_manifest_path),
                "sha256": _sha256_file(supplemental_manifest_path),
                "manifest_sha256": supplemental_manifest["manifest_sha256"],
            },
            "supplemental_status": {
                "path": _repo_path(supplemental_status_path),
                "sha256": _sha256_file(supplemental_status_path),
            },
            "supplemental_inspection": {
                "path": _repo_path(supplemental_inspection_path),
                "sha256": _sha256_file(supplemental_inspection_path),
            },
            "supplemental_private_collection_content_sha256": _sha256_json(
                supplemental_index
            ),
            "strategy": main_strategy,
        },
        "selection_contract": {
            **graph["counts"],
            "document_request_graph_sha256": graph["document_request_graph_sha256"],
            "pair_document_join_graph_sha256": graph["pair_document_join_graph_sha256"],
            "source_candidate_graph_sha256": graph["source_candidate_graph_sha256"],
            "private_contract_content_sha256": _sha256_json(graph),
            "private_contract_path": (
                "LOCAL_HISTORICAL_DATA_ROOT/"
                f"{PRIVATE_NAMESPACE}/{source_dataset_id}/"
                f"document_contracts/{dataset_id}/{PRIVATE_CONTRACT_FILE}"
            ),
            "symbols_ciks_accessions_urls_and_joins_public": False,
            "deduplication_key": "exact SEC accession-bound source URL",
            "conflicting_duplicate_policy": "fail closed",
            "every_source_pair_join_preserved": True,
        },
        "request_contract": {
            "stage": "SEC_PRIMARY_DOCUMENTS_ONLY",
            "provider": "SEC_EDGAR",
            "provider_operated_accession_bound_endpoints_only": True,
            "request_count": graph["counts"]["unique_document_requests"],
            "private_request_graph_sha256": graph["document_request_graph_sha256"],
            "user_agent_sha256": main_request["user_agent_sha256"],
            "workers": main_request["workers"],
            "global_minimum_spacing_seconds": main_request[
                "global_minimum_spacing_seconds"
            ],
            "timeout_seconds": main_request["timeout_seconds"],
            "maximum_attempts": main_request["maximum_attempts"],
            "retry_backoff_seconds": main_request["retry_backoff_seconds"],
            "shared_cache_first": True,
            "raw_source_bytes_retained_outside_git": True,
            "per_request_terminal_result_required": True,
            "one_failure_cannot_abort_unrelated_requests": True,
            "substitution_allowed": False,
            "source_semantic_classification_allowed": False,
        },
        "outcome_lock": {
            "primary_documents_requested": False,
            "source_semantics_observed_or_derived": False,
            "target_outcomes_observed_or_derived": False,
            "post_entry_data_access_allowed": False,
            "substitutions_allowed": False,
            "separate_semantics_review_required": True,
        },
        "pre_freeze_target_response_artifact_count": response_count,
        "implementation_contract": _implementation_contract(),
    }
    return stable, config, graph


def _verify_or_write_private(
    *,
    dataset_id: str,
    source_dataset_id: str,
    config: HistoricalStoreConfig,
    graph: Mapping[str, Any],
    write: bool,
) -> None:
    path = _private_contract_path(config.root, source_dataset_id, dataset_id)
    expected = _sha256_json(graph)
    if path.exists():
        if _sha256_json(_read_gzip_object(path)) != expected:
            raise DevelopmentSecDocumentsError("private primary-document graph drifted")
    elif write:
        _write_gzip_json(path, graph)
    else:
        raise DevelopmentSecDocumentsError("private primary-document graph is missing")


def freeze_contract(
    *,
    dataset_id: str = DATASET_ID,
    source_dataset_id: str = source_contract.DATASET_ID,
    supplemental_dataset_id: str = supplemental_collection.supplemental_contract.DATASET_ID,
    main_manifest_path: Path = MAIN_MANIFEST,
    main_status_path: Path = MAIN_STATUS,
    main_inspection_path: Path = MAIN_INSPECTION,
    supplemental_manifest_path: Path = SUPPLEMENTAL_MANIFEST,
    supplemental_status_path: Path = SUPPLEMENTAL_STATUS,
    supplemental_inspection_path: Path = SUPPLEMENTAL_INSPECTION,
    source_contract_doc: Path = DEFAULT_SOURCE_CONTRACT_DOC,
    env_path: Path = PROJECT_ROOT / ".env",
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    require_published_implementation: bool = True,
) -> tuple[Path, dict[str, Any]]:
    stable, config, graph = _stable_contract(
        dataset_id=dataset_id,
        source_dataset_id=source_dataset_id,
        supplemental_dataset_id=supplemental_dataset_id,
        main_manifest_path=main_manifest_path,
        main_status_path=main_status_path,
        main_inspection_path=main_inspection_path,
        supplemental_manifest_path=supplemental_manifest_path,
        supplemental_status_path=supplemental_status_path,
        supplemental_inspection_path=supplemental_inspection_path,
        env_path=env_path,
        require_published_implementation=require_published_implementation,
    )
    _verify_or_write_private(
        dataset_id=dataset_id,
        source_dataset_id=source_dataset_id,
        config=config,
        graph=graph,
        write=True,
    )
    matches = sorted(output_root.glob(f"{dataset_id}-*.json"))
    if len(matches) > 1:
        raise DevelopmentSecDocumentsError(
            "primary-document contract has multiple manifests"
        )
    if matches:
        existing = load_frozen_dataset_contract(matches[0])
        if any(existing.get(key) != value for key, value in stable.items()):
            raise DevelopmentSecDocumentsError(
                "existing primary-document contract drifted"
            )
        return matches[0], existing
    usage = shutil.disk_usage(config.root)
    if usage.free < config.min_free_bytes:
        raise DevelopmentSecDocumentsError("historical-store reserve is unavailable")
    contract = {
        "schema_version": 1,
        "dataset_id": dataset_id,
        "registered_at": datetime.now(UTC).isoformat(),
        "requested_dates": load_frozen_dataset_contract(main_manifest_path)[
            "requested_dates"
        ],
        "dataset_payload": {
            "lane": "development",
            "claim_scope": "DEVELOPMENT_ONLY",
            "status": "COLLECTING",
            "evidence_paths": [
                _repo_path(source_contract_doc),
                _repo_path(main_manifest_path),
                _repo_path(main_status_path),
                _repo_path(main_inspection_path),
                _repo_path(supplemental_manifest_path),
                _repo_path(supplemental_status_path),
                _repo_path(supplemental_inspection_path),
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
            "minimum_required_reserve_bytes": source_contract.MINIMUM_RESERVE_BYTES,
            "capacity_ready": True,
            "historical_deletion_allowed": False,
        },
    }
    return freeze_dataset_contract(contract, output_root)


def inspect_contract(
    *,
    manifest_path: Path,
    dataset_id: str = DATASET_ID,
    source_dataset_id: str = source_contract.DATASET_ID,
    supplemental_dataset_id: str = supplemental_collection.supplemental_contract.DATASET_ID,
    main_manifest_path: Path = MAIN_MANIFEST,
    main_status_path: Path = MAIN_STATUS,
    main_inspection_path: Path = MAIN_INSPECTION,
    supplemental_manifest_path: Path = SUPPLEMENTAL_MANIFEST,
    supplemental_status_path: Path = SUPPLEMENTAL_STATUS,
    supplemental_inspection_path: Path = SUPPLEMENTAL_INSPECTION,
    env_path: Path = PROJECT_ROOT / ".env",
    status_path: Path = DEFAULT_PUBLIC_STATUS,
    require_published_implementation: bool = True,
) -> dict[str, Any]:
    dataset_id = _validated_dataset_id(dataset_id)
    manifest = load_frozen_dataset_contract(manifest_path)
    if manifest.get("dataset_id") != dataset_id:
        raise DevelopmentSecDocumentsError("unexpected primary-document dataset")
    stable, config, graph = _stable_contract(
        dataset_id=dataset_id,
        source_dataset_id=source_dataset_id,
        supplemental_dataset_id=supplemental_dataset_id,
        main_manifest_path=main_manifest_path,
        main_status_path=main_status_path,
        main_inspection_path=main_inspection_path,
        supplemental_manifest_path=supplemental_manifest_path,
        supplemental_status_path=supplemental_status_path,
        supplemental_inspection_path=supplemental_inspection_path,
        env_path=env_path,
        require_published_implementation=require_published_implementation,
    )
    for key, value in stable.items():
        if manifest.get(key) != value:
            raise DevelopmentSecDocumentsError(
                f"primary-document contract {key} drifted"
            )
    _verify_or_write_private(
        dataset_id=dataset_id,
        source_dataset_id=source_dataset_id,
        config=config,
        graph=graph,
        write=False,
    )
    capacity = manifest.get("capacity_contract")
    if not isinstance(capacity, Mapping) or any(
        (
            capacity.get("capacity_ready") is not True,
            capacity.get("historical_store_outside_repository") is not True,
            capacity.get("historical_deletion_allowed") is not False,
            int(capacity.get("reserve_bytes", -1)) != config.min_free_bytes,
        )
    ):
        raise DevelopmentSecDocumentsError(
            "primary-document capacity contract is invalid"
        )
    selection = stable["selection_contract"]
    status = {
        "schema_version": 1,
        "dataset_id": dataset_id,
        "status": "FROZEN_READY",
        "manifest_sha256": manifest["manifest_sha256"],
        "counts": {
            key: selection[key]
            for key in (
                "main_candidate_documents",
                "supplemental_candidate_documents",
                "candidate_document_observations",
                "unique_document_requests",
                "duplicate_document_observations",
                "main_pair_document_joins",
                "supplemental_pair_document_joins",
                "pair_document_joins",
                "unique_pairs_with_document",
            )
        },
        "document_request_graph_sha256": selection["document_request_graph_sha256"],
        "pair_document_join_graph_sha256": selection["pair_document_join_graph_sha256"],
        "private_contract_content_sha256": selection["private_contract_content_sha256"],
        "pre_freeze_target_response_artifact_count": 0,
        "primary_documents_requested": False,
        "source_semantics_observed_or_derived": False,
        "verified_positive_catalyst_count": 0,
        "target_outcomes_observed_or_derived": False,
        "substitutions_allowed": False,
        "symbols_ciks_accessions_urls_and_joins_public": False,
        "valid": True,
    }
    _write_json(status_path, status)
    return status


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env", type=Path, default=PROJECT_ROOT / ".env")
    parser.add_argument("--dataset-id", default=DATASET_ID)
    parser.add_argument("--source-dataset-id", default=source_contract.DATASET_ID)
    parser.add_argument(
        "--supplemental-dataset-id",
        default=supplemental_collection.supplemental_contract.DATASET_ID,
    )
    parser.add_argument("--main-manifest", type=Path, default=MAIN_MANIFEST)
    parser.add_argument("--main-status", type=Path, default=MAIN_STATUS)
    parser.add_argument("--main-inspection", type=Path, default=MAIN_INSPECTION)
    parser.add_argument(
        "--supplemental-manifest", type=Path, default=SUPPLEMENTAL_MANIFEST
    )
    parser.add_argument("--supplemental-status", type=Path, default=SUPPLEMENTAL_STATUS)
    parser.add_argument(
        "--supplemental-inspection", type=Path, default=SUPPLEMENTAL_INSPECTION
    )
    parser.add_argument(
        "--source-contract-doc", type=Path, default=DEFAULT_SOURCE_CONTRACT_DOC
    )
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("freeze")
    inspect = subparsers.add_parser("inspect")
    inspect.add_argument("manifest", type=Path)
    inspect.add_argument("--status", type=Path, default=DEFAULT_PUBLIC_STATUS)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        if args.command == "freeze":
            path, manifest = freeze_contract(
                dataset_id=args.dataset_id,
                source_dataset_id=args.source_dataset_id,
                supplemental_dataset_id=args.supplemental_dataset_id,
                main_manifest_path=args.main_manifest,
                main_status_path=args.main_status,
                main_inspection_path=args.main_inspection,
                supplemental_manifest_path=args.supplemental_manifest,
                supplemental_status_path=args.supplemental_status,
                supplemental_inspection_path=args.supplemental_inspection,
                source_contract_doc=args.source_contract_doc,
                env_path=args.env,
                output_root=args.output_root,
            )
            value = {
                "dataset_id": args.dataset_id,
                "manifest_sha256": manifest["manifest_sha256"],
                "path": str(path),
                "unique_document_requests": manifest["selection_contract"][
                    "unique_document_requests"
                ],
                "pair_document_joins": manifest["selection_contract"][
                    "pair_document_joins"
                ],
            }
        else:
            value = inspect_contract(
                manifest_path=args.manifest,
                dataset_id=args.dataset_id,
                source_dataset_id=args.source_dataset_id,
                supplemental_dataset_id=args.supplemental_dataset_id,
                main_manifest_path=args.main_manifest,
                main_status_path=args.main_status,
                main_inspection_path=args.main_inspection,
                supplemental_manifest_path=args.supplemental_manifest,
                supplemental_status_path=args.supplemental_status,
                supplemental_inspection_path=args.supplemental_inspection,
                env_path=args.env,
                status_path=args.status,
            )
        print(json.dumps(value, indent=2, sort_keys=True))
        return 0
    except (
        DevelopmentSecDocumentsError,
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

"""Recover and inspect issuer-filed exhibits from frozen SEC accessions.

The production-v3 source review left a complete, outcome-blind set of pairs
whose accession-bound primary filing document was causal and correctly bound,
but semantically unresolved.  This module freezes the complete submission-text
request for every accession in that set before provider access, retains the raw
responses outside Git, and applies the already-frozen v3 event rules only to
issuer-filed EX-99 exhibits.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import io
import json
import os
import re
import shutil
import subprocess
import sys
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import catalyst_source_semantics as shared_semantics
import development_catalyst_source_semantics as upstream_semantics
import development_sec_sources as source_contract
from historical_concurrency import ordered_bounded_results
from historical_discovery import HistoricalDiscoveryError, SecClient, SecConfig
from historical_store import HistoricalStoreConfig, HistoricalStoreError
from learning_data import (
    LearningDataError,
    freeze_dataset_contract,
    load_frozen_dataset_contract,
)


PROJECT_ROOT = Path(__file__).resolve().parent
DATASET_ID = "dataset-development-sec-accession-chain-recovery-2026-07-20-v3"
SOURCE_DATASET_ID = "dataset-development-sec-primary-sources-2026-07-20-v3"
SOURCE_SEMANTICS_DATASET_ID = (
    "dataset-primary-source-semantics-contract-2026-07-20-development-v3"
)
SOURCE_REVIEW_DATASET_ID = "dataset-development-sec-source-semantics-2026-07-20-v3"
SOURCE_SEMANTICS_MANIFEST = (
    PROJECT_ROOT
    / "historical_batches/development_tranche_v3/sec_semantics_manifests"
    / (
        "dataset-development-sec-source-semantics-2026-07-20-v3-"
        "b1241f337aef38adab82924273e59d5a9a1533bc5f6e16096bc8daabce710531.json"
    )
)
SOURCE_SEMANTICS_RESULT = (
    PROJECT_ROOT
    / "research_results/2026-07-20-development-sec-source-semantics-inspection.json"
)
DEFAULT_OUTPUT_ROOT = (
    PROJECT_ROOT
    / "historical_batches/development_tranche_v3/sec_accession_chain_manifests"
)
DEFAULT_PUBLIC_STATUS = (
    PROJECT_ROOT
    / "historical_batches/development_tranche_v3/sec-accession-chain-status.json"
)
DEFAULT_PUBLIC_RESULT = (
    PROJECT_ROOT
    / "research_results/2026-07-20-development-sec-accession-chain-inspection.json"
)

PRIVATE_NAMESPACE = "_derived/development_sec_accession_chain_recovery"
SHARED_CACHE_NAMESPACE = source_contract.SHARED_SEC_CACHE_NAMESPACE
EXPECTED_PRIOR_POSITIVES = 19
EXPECTED_UNRESOLVED_PAIRS = 377
EXPECTED_PRIOR_JOINS = 436
EXPECTED_UNIQUE_ACCESSIONS = 417
WORKERS = 4
MAX_RESPONSE_BYTES = 100 * 1024 * 1024
MINIMUM_RESERVE_BYTES = 20 * 1024**3
COLLECTOR_SCHEMA_VERSION = 1
PARSER_VERSION = "sec-complete-submission-ex99-v1"
SEC_DENIAL_MARKERS = (
    b"your request originates from an undeclared automated tool",
    b"request rate threshold exceeded",
)
DOCUMENT_PATTERN = re.compile(r"(?is)<DOCUMENT>(.*?)</DOCUMENT>")
TYPE_PATTERN = re.compile(r"(?im)^\s*<TYPE>\s*([^\r\n<]+)")
FILENAME_PATTERN = re.compile(r"(?im)^\s*<FILENAME>\s*([^\r\n<]+)")
DESCRIPTION_PATTERN = re.compile(r"(?im)^\s*<DESCRIPTION>\s*([^\r\n<]+)")
TEXT_PATTERN = re.compile(r"(?is)<TEXT>(.*?)(?:</TEXT>|\Z)")


class DevelopmentSecAccessionChainError(RuntimeError):
    """The frozen accession-chain recovery cannot be honored safely."""


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
        raise DevelopmentSecAccessionChainError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise DevelopmentSecAccessionChainError(f"{path} must contain an object")
    return value


def _read_gzip(path: Path) -> dict[str, Any]:
    try:
        with gzip.open(path, "rt", encoding="utf-8") as source:
            value = json.load(source)
    except (OSError, json.JSONDecodeError) as exc:
        raise DevelopmentSecAccessionChainError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise DevelopmentSecAccessionChainError(f"{path} must contain an object")
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
        raise DevelopmentSecAccessionChainError(
            f"public path must be repository relative: {path}"
        ) from exc


def _private_root(store_root: Path) -> Path:
    return store_root / PRIVATE_NAMESPACE / DATASET_ID


def _selection_path(store_root: Path) -> Path:
    return _private_root(store_root) / "frozen-selection.json.gz"


def _wrapper_path(store_root: Path, request_sha256: str) -> Path:
    return _private_root(store_root) / "wrappers" / f"{request_sha256}.json.gz"


def _index_path(store_root: Path) -> Path:
    return _private_root(store_root) / "collection-index.json.gz"


def _reviewed_path(store_root: Path) -> Path:
    return _private_root(store_root) / "reviewed-result.json.gz"


def _upstream_reviewed_path(store_root: Path) -> Path:
    return upstream_semantics._reviewed_path(
        store_root, SOURCE_SEMANTICS_DATASET_ID, SOURCE_REVIEW_DATASET_ID
    )


def _shared_path(store_root: Path, request: Mapping[str, Any]) -> Path:
    relative = Path(str(request["shared_cache_relative_path"]))
    if relative.is_absolute() or ".." in relative.parts:
        raise DevelopmentSecAccessionChainError("shared cache path is unsafe")
    return store_root / SHARED_CACHE_NAMESPACE / relative


def _target_artifact_count(store_root: Path) -> int:
    root = _private_root(store_root)
    return sum(1 for path in root.rglob("*") if path.is_file()) if root.exists() else 0


def _published_artifact(path: Path) -> dict[str, str]:
    relative = _repo_path(path)
    dirty = subprocess.run(
        ["git", "status", "--porcelain", "--", relative],
        cwd=PROJECT_ROOT,
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    if dirty.strip():
        raise DevelopmentSecAccessionChainError(
            f"provider input is not committed: {relative}"
        )
    head = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=PROJECT_ROOT,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    upstream = subprocess.run(
        ["git", "rev-parse", "@{upstream}"],
        cwd=PROJECT_ROOT,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    if head != upstream:
        raise DevelopmentSecAccessionChainError(
            "provider access requires HEAD to equal its pushed upstream"
        )
    committed = subprocess.run(
        ["git", "show", f"HEAD:{relative}"],
        cwd=PROJECT_ROOT,
        check=True,
        capture_output=True,
    ).stdout
    if hashlib.sha256(committed).hexdigest() != _sha256_file(path):
        raise DevelopmentSecAccessionChainError(
            f"committed bytes differ: {relative}"
        )
    return {"commit": head, "path": relative, "sha256": _sha256_file(path)}


def _submission_request(cik: str, accession: str) -> dict[str, Any]:
    normalized_cik = str(int(cik))
    compact = accession.replace("-", "")
    if not (
        cik.isdigit()
        and re.fullmatch(r"\d{10}-\d{2}-\d{6}", accession)
        and len(compact) == 18
    ):
        raise DevelopmentSecAccessionChainError("accession identity is malformed")
    return {
        "cik": cik,
        "accession": accession,
        "source_url": (
            "https://www.sec.gov/Archives/edgar/data/"
            f"{normalized_cik}/{compact}/{accession}.txt"
        ),
        "shared_cache_relative_path": (
            f"archives/edgar/data/{normalized_cik}/{compact}/{accession}.txt"
        ),
    }


def build_selection(
    reviewed: Mapping[str, Any],
    *,
    expected_pairs: int = EXPECTED_UNRESOLVED_PAIRS,
    expected_joins: int = EXPECTED_PRIOR_JOINS,
    expected_accessions: int = EXPECTED_UNIQUE_ACCESSIONS,
) -> dict[str, Any]:
    if not (
        reviewed.get("dataset_id") == SOURCE_REVIEW_DATASET_ID
        and reviewed.get("status") == "REVIEW_COMPLETE"
        and reviewed.get("verified_positive_pairs") == EXPECTED_PRIOR_POSITIVES
        and reviewed.get("target_outcomes_observed_or_derived") is False
    ):
        raise DevelopmentSecAccessionChainError(
            "upstream private source review is incomplete"
        )
    pair_dispositions = reviewed.get("pair_dispositions")
    rows = reviewed.get("rows")
    if not isinstance(pair_dispositions, Mapping) or not isinstance(rows, list):
        raise DevelopmentSecAccessionChainError("upstream source rows are malformed")
    unresolved = {
        str(pair_hash)
        for pair_hash, disposition in pair_dispositions.items()
        if disposition == "DOCUMENT_SEMANTICS_UNRESOLVED"
    }
    requests: dict[tuple[str, str], dict[str, Any]] = {}
    joins: list[dict[str, Any]] = []
    pair_hashes: set[str] = set()
    prior_row_ids: set[str] = set()
    for value in rows:
        if not isinstance(value, Mapping):
            raise DevelopmentSecAccessionChainError("upstream source row is malformed")
        pair_key = (str(value.get("date") or ""), str(value.get("instrument_id") or ""))
        pair_hash = upstream_semantics._sha256_json(pair_key)
        if pair_hash not in unresolved:
            continue
        cik = str(value.get("cik") or "")
        accession = str(value.get("accession") or "")
        request = _submission_request(cik, accession)
        request_sha256 = _sha256_json(request)
        current = requests.setdefault((cik, accession), request)
        if current != request:
            raise DevelopmentSecAccessionChainError("accession request conflicts")
        prior_row_id = str(value.get("row_id") or "")
        if not prior_row_id or prior_row_id in prior_row_ids:
            raise DevelopmentSecAccessionChainError("upstream row identity repeats")
        prior_row_ids.add(prior_row_id)
        pair_hashes.add(pair_hash)
        joins.append(
            {
                "pair_hash": pair_hash,
                "date": pair_key[0],
                "instrument_id": pair_key[1],
                "primary_exchange": value.get("primary_exchange"),
                "rank": value.get("rank"),
                "symbol": value.get("symbol"),
                "cik": cik,
                "form": value.get("form"),
                "accession": accession,
                "accepted_at": value.get("accepted_at"),
                "prior_row_id": prior_row_id,
                "request_sha256": request_sha256,
            }
        )
    request_rows = []
    for request in requests.values():
        request_rows.append({**request, "request_sha256": _sha256_json(request)})
    request_rows.sort(key=lambda row: str(row["request_sha256"]))
    joins.sort(key=lambda row: (str(row["pair_hash"]), str(row["prior_row_id"])))
    counts = {
        "candidate_pairs": len(pair_hashes),
        "prior_pair_source_joins": len(joins),
        "unique_accessions": len(request_rows),
    }
    if counts != {
        "candidate_pairs": expected_pairs,
        "prior_pair_source_joins": expected_joins,
        "unique_accessions": expected_accessions,
    } or unresolved != pair_hashes:
        raise DevelopmentSecAccessionChainError(
            "unresolved accession-chain surface differs"
        )
    return {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "status": "FROZEN_SELECTION",
        "counts": counts,
        "requests": request_rows,
        "joins": joins,
        "pair_identity_sha256": _sha256_json(sorted(pair_hashes)),
        "request_graph_sha256": _sha256_json(request_rows),
        "join_graph_sha256": _sha256_json(joins),
        "symbols_ciks_accessions_urls_sources_text_and_rows_public": False,
        "target_outcomes_observed_or_derived": False,
    }


def freeze_inputs(
    *, env_path: Path, output_root: Path, status_path: Path
) -> tuple[Path, dict[str, Any]]:
    source_manifest = load_frozen_dataset_contract(SOURCE_SEMANTICS_MANIFEST)
    source_result = _read_json(SOURCE_SEMANTICS_RESULT)
    config = HistoricalStoreConfig.from_env(env_path)
    if (
        config.min_free_bytes < MINIMUM_RESERVE_BYTES
        or shutil.disk_usage(config.root).free < config.min_free_bytes
        or config.root.resolve().is_relative_to(PROJECT_ROOT.resolve())
    ):
        raise DevelopmentSecAccessionChainError("historical store capacity is unsafe")
    prior_path = _upstream_reviewed_path(config.root)
    reviewed = _read_gzip(prior_path)
    if not (
        source_result.get("status") == "READY"
        and source_result.get("inspected") is True
        and source_result.get("private_result_sha256") == _sha256_file(prior_path)
        and source_result.get("verified_positive_pairs") == EXPECTED_PRIOR_POSITIVES
        and source_result.get("target_outcomes_observed_or_derived") is False
        and _target_artifact_count(config.root) == 0
    ):
        raise DevelopmentSecAccessionChainError(
            "source semantics or pre-freeze artifact boundary differs"
        )
    selection = build_selection(reviewed)
    private_selection_path = _selection_path(config.root)
    _write_gzip(private_selection_path, selection)
    sec_config = SecConfig.from_env(
        env_path, config.root / SHARED_CACHE_NAMESPACE, workers=WORKERS
    )
    contract = {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "registered_at": datetime.now(UTC).isoformat(),
        "requested_dates": sorted({str(row["date"]) for row in selection["joins"]}),
        "dataset_payload": {
            "lane": "development",
            "claim_scope": "DEVELOPMENT_ONLY",
            "status": "COLLECTING",
            "evidence_paths": [
                _repo_path(SOURCE_SEMANTICS_MANIFEST),
                _repo_path(SOURCE_SEMANTICS_RESULT),
                "PRODUCTION_STRATEGY_VALIDATION.md",
            ],
            "inspected": False,
        },
        "selection_contract": {
            "source_semantics_manifest_sha256": source_manifest["manifest_sha256"],
            "source_semantics_result_sha256": _sha256_file(SOURCE_SEMANTICS_RESULT),
            "source_semantics_private_result_sha256": _sha256_file(prior_path),
            "private_selection_sha256": _sha256_file(private_selection_path),
            **selection["counts"],
            "prior_verified_positive_pairs": EXPECTED_PRIOR_POSITIVES,
            "required_combined_positive_pairs": 20,
            "pair_identity_sha256": selection["pair_identity_sha256"],
            "request_graph_sha256": selection["request_graph_sha256"],
            "join_graph_sha256": selection["join_graph_sha256"],
            "complete_unresolved_pair_surface_selected": True,
            "post_outcome_selection_allowed": False,
            "symbols_ciks_accessions_urls_sources_text_and_rows_public": False,
        },
        "request_contract": {
            "provider": "SEC_EDGAR",
            "endpoint": "ACCESSION_COMPLETE_SUBMISSION_TEXT",
            "request_count": selection["counts"]["unique_accessions"],
            "user_agent_sha256": hashlib.sha256(sec_config.user_agent.encode()).hexdigest(),
            "workers": WORKERS,
            "global_minimum_spacing_seconds": sec_config.minimum_spacing_seconds,
            "timeout_seconds": sec_config.timeout_seconds,
            "maximum_attempts": 4,
            "maximum_response_bytes": MAX_RESPONSE_BYTES,
            "shared_cache_first": True,
            "provider_substitution_allowed": False,
            "secondary_news_substitution_allowed": False,
            "historical_store_reserve_enforced": True,
            "minimum_required_reserve_bytes": MINIMUM_RESERVE_BYTES,
            "checkpoint_unit": "accession",
        },
        "review_contract": {
            "implementation_sha256": _sha256_file(Path(__file__)),
            "upstream_classifier_sha256": _sha256_file(
                Path(upstream_semantics.__file__)
            ),
            "shared_semantics_sha256": _sha256_file(Path(shared_semantics.__file__)),
            "parser_version": PARSER_VERSION,
            "document_type_prefix": "EX-99",
            "positive_patterns": [list(value) for value in upstream_semantics.POSITIVE_PATTERNS],
            "negative_patterns": [list(value) for value in upstream_semantics.NEGATIVE_PATTERNS],
            "non_material_patterns": [
                list(value) for value in upstream_semantics.NON_MATERIAL_PATTERNS
            ],
            "financing_pattern": shared_semantics.FINANCING_PATTERN.pattern,
            "financing_or_dilution_precedes_positive": True,
            "terminal_precedence": list(shared_semantics.TERMINAL_PRECEDENCE),
            "same_accession_acceptance_timestamp_applies_to_attached_exhibits": True,
            "same_interval_or_return_data_allowed": False,
            "network_access_allowed_during_review": False,
            "target_outcomes_observed_or_derived": False,
        },
        "outcome_lock": {
            "post_entry_data_access_allowed": False,
            "target_outcomes_observed_or_derived": False,
            "production_rule_change_allowed": False,
        },
    }
    path, manifest = freeze_dataset_contract(contract, output_root)
    public = {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "status": "FROZEN_READY",
        "counts": selection["counts"],
        "pair_identity_sha256": selection["pair_identity_sha256"],
        "request_graph_sha256": selection["request_graph_sha256"],
        "join_graph_sha256": selection["join_graph_sha256"],
        "pre_freeze_target_artifact_count": 0,
        "source_semantics_classified": False,
        "outcome_contract_permitted": False,
        "target_outcomes_observed_or_derived": False,
        "valid": True,
    }
    _write_json(status_path, public)
    return path, manifest


def _load_contract(
    *, manifest_path: Path, env_path: Path, require_published: bool
) -> tuple[dict[str, Any], HistoricalStoreConfig, dict[str, Any], dict[str, Any]]:
    manifest = load_frozen_dataset_contract(manifest_path)
    if manifest.get("dataset_id") != DATASET_ID:
        raise DevelopmentSecAccessionChainError("unexpected accession-chain dataset")
    config = HistoricalStoreConfig.from_env(env_path)
    selection_path = _selection_path(config.root)
    selection = _read_gzip(selection_path)
    contract = manifest.get("selection_contract", {})
    if not (
        _sha256_file(selection_path) == contract.get("private_selection_sha256")
        and selection.get("request_graph_sha256") == contract.get("request_graph_sha256")
        and selection.get("join_graph_sha256") == contract.get("join_graph_sha256")
        and selection.get("target_outcomes_observed_or_derived") is False
    ):
        raise DevelopmentSecAccessionChainError("frozen selection changed")
    prior_path = _upstream_reviewed_path(config.root)
    if load_frozen_dataset_contract(SOURCE_SEMANTICS_MANIFEST).get(
        "manifest_sha256"
    ) != contract.get("source_semantics_manifest_sha256"):
        raise DevelopmentSecAccessionChainError(
            "source semantics manifest changed after freeze"
        )
    paths = {
        "source_semantics_result_sha256": SOURCE_SEMANTICS_RESULT,
        "source_semantics_private_result_sha256": prior_path,
    }
    for name, path in paths.items():
        if _sha256_file(path) != contract.get(name):
            raise DevelopmentSecAccessionChainError(f"upstream binding changed: {name}")
    review_contract = manifest.get("review_contract", {})
    dependencies = {
        "implementation_sha256": Path(__file__),
        "upstream_classifier_sha256": Path(upstream_semantics.__file__),
        "shared_semantics_sha256": Path(shared_semantics.__file__),
    }
    for name, path in dependencies.items():
        if _sha256_file(path) != review_contract.get(name):
            raise DevelopmentSecAccessionChainError(f"frozen dependency changed: {name}")
    request_contract = manifest.get("request_contract", {})
    outcome_lock = manifest.get("outcome_lock", {})
    if not (
        request_contract.get("provider") == "SEC_EDGAR"
        and request_contract.get("endpoint")
        == "ACCESSION_COMPLETE_SUBMISSION_TEXT"
        and int(request_contract.get("request_count", -1))
        == int(selection["counts"]["unique_accessions"])
        and int(request_contract.get("maximum_attempts", -1)) == 4
        and int(request_contract.get("maximum_response_bytes", -1))
        == MAX_RESPONSE_BYTES
        and request_contract.get("provider_substitution_allowed") is False
        and request_contract.get("secondary_news_substitution_allowed") is False
        and request_contract.get("historical_store_reserve_enforced") is True
        and int(request_contract.get("minimum_required_reserve_bytes", -1))
        == MINIMUM_RESERVE_BYTES
        and review_contract.get("parser_version") == PARSER_VERSION
        and review_contract.get("document_type_prefix") == "EX-99"
        and review_contract.get("financing_or_dilution_precedes_positive") is True
        and review_contract.get("network_access_allowed_during_review") is False
        and review_contract.get("target_outcomes_observed_or_derived") is False
        and outcome_lock.get("post_entry_data_access_allowed") is False
        and outcome_lock.get("target_outcomes_observed_or_derived") is False
        and outcome_lock.get("production_rule_change_allowed") is False
    ):
        raise DevelopmentSecAccessionChainError(
            "frozen request, review, or outcome rules differ"
        )
    if build_selection(_read_gzip(prior_path)) != selection:
        raise DevelopmentSecAccessionChainError(
            "accession-chain selection does not independently rebuild"
        )
    sec_config = SecConfig.from_env(
        env_path,
        config.root / SHARED_CACHE_NAMESPACE,
        workers=int(request_contract.get("workers", 0)),
    )
    if not (
        hashlib.sha256(sec_config.user_agent.encode()).hexdigest()
        == request_contract.get("user_agent_sha256")
        and sec_config.minimum_spacing_seconds
        == request_contract.get("global_minimum_spacing_seconds")
        and sec_config.timeout_seconds == request_contract.get("timeout_seconds")
        and shutil.disk_usage(config.root).free >= config.min_free_bytes
    ):
        raise DevelopmentSecAccessionChainError("SEC client or capacity differs")
    publication: dict[str, Any] = {}
    if require_published:
        publication = {
            "collector": _published_artifact(Path(__file__)),
            "manifest": _published_artifact(manifest_path),
        }
    return manifest, config, selection, publication


def _transport_integrity(raw: bytes) -> None:
    if not raw.strip():
        raise DevelopmentSecAccessionChainError("complete submission is empty")
    if len(raw) > MAX_RESPONSE_BYTES:
        raise DevelopmentSecAccessionChainError("complete submission exceeds byte limit")
    lowered = raw[: 256 * 1024].lower()
    if any(marker in lowered for marker in SEC_DENIAL_MARKERS):
        raise DevelopmentSecAccessionChainError(
            "SEC returned an automated-access denial document"
        )


def _success_wrapper(
    *, request: Mapping[str, Any], manifest_sha256: str, raw_path: Path, cache_hit: bool
) -> dict[str, Any]:
    raw = raw_path.read_bytes()
    _transport_integrity(raw)
    return {
        "schema_version": COLLECTOR_SCHEMA_VERSION,
        "dataset_id": DATASET_ID,
        "manifest_sha256": manifest_sha256,
        "collector_sha256": _sha256_file(Path(__file__)),
        "request_sha256": request["request_sha256"],
        "captured_at": datetime.now(UTC).isoformat(),
        "status": "SUCCESS",
        "source_origin": "SHARED_CACHE" if cache_hit else "SEC_DOWNLOAD",
        "source_bytes": len(raw),
        "source_sha256": hashlib.sha256(raw).hexdigest(),
        "raw_source_retained": True,
        "semantic_classification_performed": False,
        "target_outcomes_observed_or_derived": False,
    }


def _failure_wrapper(
    *, request: Mapping[str, Any], manifest_sha256: str, error: Exception
) -> dict[str, Any]:
    return {
        "schema_version": COLLECTOR_SCHEMA_VERSION,
        "dataset_id": DATASET_ID,
        "manifest_sha256": manifest_sha256,
        "collector_sha256": _sha256_file(Path(__file__)),
        "request_sha256": request["request_sha256"],
        "captured_at": datetime.now(UTC).isoformat(),
        "status": "FAILED",
        "source_origin": None,
        "source_bytes": 0,
        "source_sha256": None,
        "raw_source_retained": False,
        "semantic_classification_performed": False,
        "error": {"type": type(error).__name__, "message": str(error)},
        "target_outcomes_observed_or_derived": False,
    }


def _validate_wrapper(
    wrapper: Mapping[str, Any], request: Mapping[str, Any], manifest_sha256: str
) -> None:
    if not (
        wrapper.get("schema_version") == COLLECTOR_SCHEMA_VERSION
        and wrapper.get("dataset_id") == DATASET_ID
        and wrapper.get("manifest_sha256") == manifest_sha256
        and wrapper.get("collector_sha256") == _sha256_file(Path(__file__))
        and wrapper.get("request_sha256") == request.get("request_sha256")
        and wrapper.get("status") in {"SUCCESS", "FAILED"}
        and wrapper.get("target_outcomes_observed_or_derived") is False
    ):
        raise DevelopmentSecAccessionChainError("accession wrapper differs")


def _build_index(
    *, manifest: Mapping[str, Any], selection: Mapping[str, Any], store_root: Path
) -> dict[str, Any]:
    records: list[dict[str, Any]] = []
    counts: Counter[str] = Counter()
    for request in selection["requests"]:
        wrapper_path = _wrapper_path(store_root, str(request["request_sha256"]))
        if not wrapper_path.exists():
            continue
        wrapper = _read_gzip(wrapper_path)
        _validate_wrapper(wrapper, request, str(manifest["manifest_sha256"]))
        if wrapper["status"] == "SUCCESS":
            raw_path = _shared_path(store_root, request)
            raw = raw_path.read_bytes()
            _transport_integrity(raw)
            if (
                len(raw) != int(wrapper["source_bytes"])
                or hashlib.sha256(raw).hexdigest() != wrapper["source_sha256"]
            ):
                raise DevelopmentSecAccessionChainError("raw accession bytes drifted")
        counts["terminal_requests"] += 1
        counts["successful_requests"] += wrapper["status"] == "SUCCESS"
        counts["failed_requests"] += wrapper["status"] == "FAILED"
        counts["cache_hits"] += wrapper.get("source_origin") == "SHARED_CACHE"
        counts["downloads"] += wrapper.get("source_origin") == "SEC_DOWNLOAD"
        counts["source_bytes"] += int(wrapper.get("source_bytes") or 0)
        records.append(
            {
                "request_sha256": wrapper["request_sha256"],
                "wrapper_sha256": _sha256_json(wrapper),
                "status": wrapper["status"],
                "source_origin": wrapper.get("source_origin"),
                "source_bytes": wrapper.get("source_bytes"),
                "source_sha256": wrapper.get("source_sha256"),
                "error": wrapper.get("error"),
            }
        )
    records.sort(key=lambda row: str(row["request_sha256"]))
    counts["expected_requests"] = len(selection["requests"])
    counts["pending_requests"] = counts["expected_requests"] - counts["terminal_requests"]
    return {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "collector_sha256": _sha256_file(Path(__file__)),
        "status": (
            "COLLECTION_COMPLETE"
            if counts["pending_requests"] == 0
            else "COLLECTION_PARTIAL"
        ),
        "counts": dict(sorted(counts.items())),
        "records": records,
        "request_graph_sha256": selection["request_graph_sha256"],
        "target_outcomes_observed_or_derived": False,
    }


def _public_collection(
    index: Mapping[str, Any], publication: Mapping[str, Any]
) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "manifest_sha256": index["manifest_sha256"],
        "status": index["status"],
        "counts": dict(index["counts"]),
        "request_graph_sha256": index["request_graph_sha256"],
        "private_collection_content_sha256": _sha256_json(index),
        "collector_sha256": index["collector_sha256"],
        "collector_commit": publication.get("collector", {}).get("commit"),
        "source_semantics_classified": False,
        "outcome_contract_permitted": False,
        "symbols_ciks_accessions_urls_sources_text_and_rows_public": False,
        "target_outcomes_observed_or_derived": False,
    }


def collect(
    *,
    manifest_path: Path,
    env_path: Path,
    status_path: Path,
    client: Any | None = None,
    require_published: bool = True,
) -> dict[str, Any]:
    manifest, config, selection, publication = _load_contract(
        manifest_path=manifest_path,
        env_path=env_path,
        require_published=require_published,
    )
    request_contract = manifest["request_contract"]
    sec_config = SecConfig.from_env(
        env_path,
        config.root / SHARED_CACHE_NAMESPACE,
        workers=int(request_contract["workers"]),
    )
    sec_client = client or SecClient(sec_config)
    pending = [
        request
        for request in selection["requests"]
        if not _wrapper_path(config.root, str(request["request_sha256"])).exists()
    ]

    def load(request: Mapping[str, Any]) -> tuple[Mapping[str, Any], Path, bool]:
        reserve_margin = int(request_contract["maximum_response_bytes"]) * int(
            request_contract["workers"]
        )
        if shutil.disk_usage(config.root).free - reserve_margin < config.min_free_bytes:
            raise DevelopmentSecAccessionChainError(
                "accession request would breach historical store reserve"
            )
        path = _shared_path(config.root, request)
        cache_hit = path.exists()
        sec_client.text(str(request["source_url"]), path)
        return request, path, cache_hit

    for result in ordered_bounded_results(
        pending, load, max_workers=int(request_contract["workers"])
    ):
        request = result.item
        if result.error is None:
            returned, raw_path, cache_hit = result.unwrap()
            try:
                wrapper = _success_wrapper(
                    request=returned,
                    manifest_sha256=str(manifest["manifest_sha256"]),
                    raw_path=raw_path,
                    cache_hit=cache_hit,
                )
            except Exception as exc:
                wrapper = _failure_wrapper(
                    request=request,
                    manifest_sha256=str(manifest["manifest_sha256"]),
                    error=exc,
                )
        else:
            wrapper = _failure_wrapper(
                request=request,
                manifest_sha256=str(manifest["manifest_sha256"]),
                error=result.error,
            )
        _write_gzip(
            _wrapper_path(config.root, str(request["request_sha256"])), wrapper
        )
    index = _build_index(manifest=manifest, selection=selection, store_root=config.root)
    _write_gzip(_index_path(config.root), index)
    public = _public_collection(index, publication)
    _write_json(status_path, public)
    return public


def _field(pattern: re.Pattern[str], block: str) -> str:
    match = pattern.search(block)
    return match.group(1).strip() if match else ""


def parse_ex99_documents(raw: bytes) -> list[dict[str, Any]]:
    decoded = raw.decode("utf-8", errors="replace")
    values: list[dict[str, Any]] = []
    for position, match in enumerate(DOCUMENT_PATTERN.finditer(decoded)):
        block = match.group(1)
        document_type = _field(TYPE_PATTERN, block).upper()
        if not document_type.startswith("EX-99"):
            continue
        text_match = TEXT_PATTERN.search(block)
        text = text_match.group(1) if text_match else ""
        body = text.encode("utf-8")
        values.append(
            {
                "document_position": position,
                "document_type": document_type,
                "filename": _field(FILENAME_PATTERN, block),
                "description": _field(DESCRIPTION_PATTERN, block),
                "body": body,
                "body_sha256": hashlib.sha256(body).hexdigest(),
            }
        )
    return values


def _capture_extraction(
    *,
    record: Mapping[str, Any],
    document: Mapping[str, Any] | None,
    raw_present: bool,
) -> dict[str, Any]:
    if record.get("status") != "SUCCESS" or not raw_present:
        return {
            "capture_status": "CAPTURE_ERROR",
            "http_status": None,
            "diagnostic_failures": ["accession_capture_failed"],
            "text": "",
            "financing_or_dilution_terms_present": False,
            "target_outcomes_observed_or_derived": False,
        }
    if document is None:
        return {
            "capture_status": "RESPONSE_CAPTURED",
            "http_status": 200,
            "diagnostic_failures": ["no_ex99_exhibit_in_complete_submission"],
            "text": "",
            "financing_or_dilution_terms_present": False,
            "target_outcomes_observed_or_derived": False,
        }
    return {
        "capture_status": "RESPONSE_CAPTURED",
        "http_status": 200,
        "source_ownership_candidate": "SEC_OPERATED_ACCESSION_ENDPOINT",
        "issuer_binding_candidate": "SECURITY_MASTER_CIK_EQUALS_DOCUMENT_CIK",
        **upstream_semantics._extract_document(document["body"]),
    }


def build_review(
    *, selection: Mapping[str, Any], index: Mapping[str, Any], store_root: Path
) -> dict[str, Any]:
    records = {
        str(row["request_sha256"]): row for row in index.get("records", [])
    }
    requests = {
        str(row["request_sha256"]): row for row in selection.get("requests", [])
    }
    if set(records) != set(requests):
        raise DevelopmentSecAccessionChainError("accession capture set differs")
    documents: dict[str, list[dict[str, Any]]] = {}
    for request_sha256, request in requests.items():
        record = records[request_sha256]
        if record.get("status") != "SUCCESS":
            documents[request_sha256] = []
            continue
        raw = _shared_path(store_root, request).read_bytes()
        _transport_integrity(raw)
        if (
            len(raw) != int(record.get("source_bytes") or 0)
            or hashlib.sha256(raw).hexdigest() != record.get("source_sha256")
        ):
            raise DevelopmentSecAccessionChainError("review source bytes differ")
        documents[request_sha256] = parse_ex99_documents(raw)
    reviewed_rows: list[dict[str, Any]] = []
    by_pair: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for join in selection["joins"]:
        request_sha256 = str(join["request_sha256"])
        record = records[request_sha256]
        selected_documents: Sequence[Mapping[str, Any] | None] = (
            documents[request_sha256]
            if documents[request_sha256]
            else [None]
        )
        for ordinal, document in enumerate(selected_documents):
            document_identity = (
                "NO_EX99_OR_CAPTURE_FAILURE"
                if document is None
                else str(document["body_sha256"])
            )
            row = {
                "row_id": "sec-accession-exhibit-"
                + _sha256_json(
                    [join["prior_row_id"], request_sha256, ordinal, document_identity]
                )[:24],
                **dict(join),
                "source_type": "SEC",
                "source_origin": record.get("source_origin"),
                "document_position": (
                    None if document is None else document["document_position"]
                ),
                "document_type": (
                    None if document is None else document["document_type"]
                ),
                "document_filename_sha256": (
                    None
                    if document is None
                    else hashlib.sha256(str(document["filename"]).encode()).hexdigest()
                ),
                "document_description_sha256": (
                    None
                    if document is None
                    else hashlib.sha256(str(document["description"]).encode()).hexdigest()
                ),
                "extraction": _capture_extraction(
                    record=record,
                    document=document,
                    raw_present=record.get("status") == "SUCCESS",
                ),
            }
            decision = upstream_semantics._automatic_decision(row)
            validated, timestamp = shared_semantics.validate_review_decision(
                decision, row=row
            )
            terminal, diagnostics = shared_semantics.terminal_disposition(
                row, validated, timestamp
            )
            reviewed = {
                **row,
                "review": validated,
                "accepted_timestamp": timestamp,
                "diagnostic_failures": diagnostics,
                "terminal_disposition": terminal,
            }
            reviewed_rows.append(reviewed)
            by_pair[str(join["pair_hash"])].append(reviewed)
    expected_pairs = {
        str(join["pair_hash"]) for join in selection["joins"]
    }
    if set(by_pair) != expected_pairs:
        raise DevelopmentSecAccessionChainError("reviewed pair denominator differs")
    pair_dispositions = {
        pair_hash: shared_semantics._pair_disposition(rows)
        for pair_hash, rows in sorted(by_pair.items())
    }
    terminal_source_counts = Counter(
        str(row["terminal_disposition"]) for row in reviewed_rows
    )
    terminal_pair_counts = Counter(pair_dispositions.values())
    recovered_positive_pairs = terminal_pair_counts["VERIFIED_POSITIVE_PRIMARY"]
    combined_positive_pairs = EXPECTED_PRIOR_POSITIVES + recovered_positive_pairs
    return {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "manifest_sha256": index["manifest_sha256"],
        "status": "REVIEW_COMPLETE",
        "selection_counts": dict(selection["counts"]),
        "collection_counts": dict(index["counts"]),
        "recovered_source_rows": len(reviewed_rows),
        "terminal_source_counts": {
            key: terminal_source_counts.get(key, 0)
            for key in shared_semantics.TERMINAL_PRECEDENCE
        },
        "terminal_pair_counts": {
            key: terminal_pair_counts.get(key, 0)
            for key in shared_semantics.TERMINAL_PRECEDENCE
        },
        "pair_dispositions": pair_dispositions,
        "recovered_verified_positive_pairs": recovered_positive_pairs,
        "prior_verified_positive_pairs": EXPECTED_PRIOR_POSITIVES,
        "combined_verified_positive_pairs": combined_positive_pairs,
        "rows": sorted(reviewed_rows, key=lambda row: str(row["row_id"])),
        "next_phase": (
            "DEVELOPMENT_ACQUISITION"
            if combined_positive_pairs >= 20
            else "SOURCE_RECOVERY"
        ),
        "outcome_contract_permitted": False,
        "source_text_urls_symbols_dates_reviews_and_rows_public": False,
        "target_outcomes_observed_or_derived": False,
    }


def review(
    *, manifest_path: Path, env_path: Path, status_path: Path
) -> dict[str, Any]:
    manifest, config, selection, _publication = _load_contract(
        manifest_path=manifest_path, env_path=env_path, require_published=False
    )
    index = _read_gzip(_index_path(config.root))
    if not (
        index.get("manifest_sha256") == manifest["manifest_sha256"]
        and index.get("status") == "COLLECTION_COMPLETE"
        and int(index.get("counts", {}).get("pending_requests", -1)) == 0
        and index.get("target_outcomes_observed_or_derived") is False
    ):
        raise DevelopmentSecAccessionChainError("accession collection is incomplete")
    reviewed = build_review(
        selection=selection, index=index, store_root=config.root
    )
    private_path = _reviewed_path(config.root)
    _write_gzip(private_path, reviewed)
    public = {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "status": "REVIEW_COMPLETE",
        "selection_counts": reviewed["selection_counts"],
        "collection_counts": reviewed["collection_counts"],
        "recovered_source_rows": reviewed["recovered_source_rows"],
        "terminal_source_counts": reviewed["terminal_source_counts"],
        "terminal_pair_counts": reviewed["terminal_pair_counts"],
        "recovered_verified_positive_pairs": reviewed[
            "recovered_verified_positive_pairs"
        ],
        "combined_verified_positive_pairs": reviewed[
            "combined_verified_positive_pairs"
        ],
        "private_reviewed_result_sha256": _sha256_file(private_path),
        "next_phase": reviewed["next_phase"],
        "outcome_contract_permitted": False,
        "source_text_urls_symbols_dates_reviews_and_rows_public": False,
        "target_outcomes_observed_or_derived": False,
    }
    _write_json(status_path, public)
    return public


def inspect(
    *, manifest_path: Path, env_path: Path, result_path: Path
) -> dict[str, Any]:
    manifest, config, selection, _publication = _load_contract(
        manifest_path=manifest_path, env_path=env_path, require_published=False
    )
    index = _read_gzip(_index_path(config.root))
    rebuilt_index = _build_index(
        manifest=manifest, selection=selection, store_root=config.root
    )
    if rebuilt_index != index or index.get("status") != "COLLECTION_COMPLETE":
        raise DevelopmentSecAccessionChainError("collection index does not rebuild")
    private_path = _reviewed_path(config.root)
    reviewed = _read_gzip(private_path)
    rebuilt_reviewed = build_review(
        selection=selection, index=index, store_root=config.root
    )
    if rebuilt_reviewed != reviewed:
        raise DevelopmentSecAccessionChainError("semantic review does not rebuild")
    source_total = sum(int(value) for value in reviewed["terminal_source_counts"].values())
    pair_total = sum(int(value) for value in reviewed["terminal_pair_counts"].values())
    combined = int(reviewed["combined_verified_positive_pairs"])
    result = {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "status": "READY",
        "inspected": True,
        "claim_scope": "DEVELOPMENT_ONLY",
        "selection_counts": reviewed["selection_counts"],
        "collection_counts": reviewed["collection_counts"],
        "recovered_source_rows": reviewed["recovered_source_rows"],
        "terminal_source_counts": reviewed["terminal_source_counts"],
        "terminal_pair_counts": reviewed["terminal_pair_counts"],
        "prior_verified_positive_pairs": EXPECTED_PRIOR_POSITIVES,
        "recovered_verified_positive_pairs": reviewed[
            "recovered_verified_positive_pairs"
        ],
        "combined_verified_positive_pairs": combined,
        "minimum_positive_capacity": 20,
        "positive_capacity_gate_passed": combined >= 20,
        "next_phase": reviewed["next_phase"],
        "private_result_sha256": _sha256_file(private_path),
        "inspection": {
            "selection_rebuilt": True,
            "collection_index_rebuilt": True,
            "raw_source_bytes_rehashed": True,
            "ex99_document_graph_rebuilt": True,
            "terminal_counts_rebuilt": True,
            "one_terminal_reason_per_source": source_total
            == int(reviewed["recovered_source_rows"]),
            "complete_pair_denominator_reconciled": pair_total
            == int(selection["counts"]["candidate_pairs"]),
            "prior_and_recovered_positive_counts_deduplicated": True,
        },
        "claim_boundary": (
            "Issuer-filed EX-99 exhibits attached to exact causal SEC accessions, "
            "reviewed under the unchanged v3 direction and conflict rules. Returns "
            "and target-session outcomes remain inaccessible."
        ),
        "outcome_contract_permitted": False,
        "production_rule_change_earned": False,
        "source_text_urls_symbols_dates_reviews_and_rows_public": False,
        "target_outcomes_observed_or_derived": False,
        "valid": True,
    }
    _write_json(result_path, result)
    return result


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("freeze", "collect", "review", "inspect"))
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
                env_path=args.env_file,
                output_root=args.output_root,
                status_path=args.public_status,
            )
            value = {"manifest": _repo_path(path), **manifest}
        elif args.manifest is None:
            raise DevelopmentSecAccessionChainError("--manifest is required")
        elif args.command == "collect":
            value = collect(
                manifest_path=args.manifest,
                env_path=args.env_file,
                status_path=args.public_status,
            )
        elif args.command == "review":
            value = review(
                manifest_path=args.manifest,
                env_path=args.env_file,
                status_path=args.public_status,
            )
        else:
            value = inspect(
                manifest_path=args.manifest,
                env_path=args.env_file,
                result_path=args.public_result,
            )
    except (
        DevelopmentSecAccessionChainError,
        HistoricalDiscoveryError,
        HistoricalStoreError,
        LearningDataError,
        OSError,
        subprocess.CalledProcessError,
        ValueError,
    ) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}), file=sys.stderr)
        return 2
    print(json.dumps(value, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

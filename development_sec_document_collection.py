"""Collect and inspect the frozen SEC primary-document request graph."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import io
import json
import os
import shutil
import subprocess
import sys
from collections import Counter
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import development_sec_documents as document_contract
import development_sec_sources as source_contract
import development_sec_submissions as publication_gate
from historical_concurrency import ordered_bounded_results
from historical_discovery import HistoricalDiscoveryError, SecClient, SecConfig
from historical_store import HistoricalStoreConfig, HistoricalStoreError
from learning_data import LearningDataError, load_frozen_dataset_contract


PROJECT_ROOT = Path(__file__).resolve().parent
MANIFEST = (
    PROJECT_ROOT
    / "historical_batches/development_tranche_v2/sec_document_manifests"
    / (
        "dataset-development-sec-primary-documents-2026-07-19-v2-"
        "73d7223add12925324b585f226214095cf32909eeda08bc856a0e8c759ab8538.json"
    )
)
DEFAULT_PUBLIC_STATUS = (
    PROJECT_ROOT / "historical_batches/development_tranche_v2/sec-documents-status.json"
)
DEFAULT_PUBLIC_INSPECTION = (
    PROJECT_ROOT
    / "research_results/2026-07-19-development-sec-documents-inspection.json"
)
COLLECTION_INDEX_FILE = "collection-index.json.gz"
COLLECTOR_SCHEMA_VERSION = 1
SEC_DENIAL_MARKERS = (
    b"your request originates from an undeclared automated tool",
    b"request rate threshold exceeded",
)


class DevelopmentSecDocumentCollectionError(RuntimeError):
    """The frozen SEC primary-document collection is invalid or incomplete."""


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
        raise DevelopmentSecDocumentCollectionError(
            f"cannot read {path}: {exc}"
        ) from exc
    if not isinstance(value, dict):
        raise DevelopmentSecDocumentCollectionError(
            f"{path} must contain an object"
        )
    return value


def _read_gzip_object(path: Path) -> dict[str, Any]:
    try:
        with gzip.open(path, "rt", encoding="utf-8") as source:
            value = json.load(source)
    except (OSError, json.JSONDecodeError) as exc:
        raise DevelopmentSecDocumentCollectionError(
            f"cannot read {path}: {exc}"
        ) from exc
    if not isinstance(value, dict):
        raise DevelopmentSecDocumentCollectionError(
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
        raise DevelopmentSecDocumentCollectionError(
            f"path must be repository relative: {path}"
        ) from exc


def _response_root(store_root: Path) -> Path:
    return document_contract._target_response_root(store_root)


def _wrapper_path(store_root: Path, request: Mapping[str, Any]) -> Path:
    relative = Path(str(request["target_response_relative_path"]))
    if (
        relative.is_absolute()
        or ".." in relative.parts
        or len(relative.parts) != 2
        or relative.parts[0] != "documents"
        or relative.suffixes != [".json", ".gz"]
    ):
        raise DevelopmentSecDocumentCollectionError(
            "target response path is unsafe"
        )
    return _response_root(store_root) / relative.name


def _index_path(store_root: Path) -> Path:
    return _response_root(store_root) / COLLECTION_INDEX_FILE


def _shared_path(store_root: Path, request: Mapping[str, Any]) -> Path:
    relative = Path(str(request["shared_cache_relative_path"]))
    if (
        relative.is_absolute()
        or ".." in relative.parts
        or len(relative.parts) != 2
        or relative.parts[0] != "documents"
    ):
        raise DevelopmentSecDocumentCollectionError("shared path is unsafe")
    digest = hashlib.sha256(str(request["source_url"]).encode()).hexdigest()
    if relative.name != f"{digest}.source":
        raise DevelopmentSecDocumentCollectionError(
            "shared path differs from the exact URL hash"
        )
    return store_root / source_contract.SHARED_SEC_CACHE_NAMESPACE / relative


def _resolve_public_path(value: Any) -> Path:
    if not isinstance(value, str) or not value:
        raise DevelopmentSecDocumentCollectionError("bound path is missing")
    path = Path(value)
    if path.is_absolute() or ".." in path.parts:
        raise DevelopmentSecDocumentCollectionError("bound path is unsafe")
    return PROJECT_ROOT / path


def _verify_binding(value: Mapping[str, Any]) -> None:
    path = _resolve_public_path(value.get("path"))
    if not path.is_file() or _sha256_file(path) != value.get("sha256"):
        raise DevelopmentSecDocumentCollectionError(f"bound input drifted: {path}")


def _published_artifact(path: Path) -> dict[str, Any]:
    proof = publication_gate._published_source(path)
    relative = _repo_path(path)
    commit = subprocess.run(
        ["git", "log", "-1", "--format=%H", "--", relative],
        cwd=PROJECT_ROOT,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    if not commit:
        raise DevelopmentSecDocumentCollectionError(
            f"published source commit is missing: {relative}"
        )
    committed = subprocess.run(
        ["git", "show", f"{commit}:{relative}"],
        cwd=PROJECT_ROOT,
        check=True,
        capture_output=True,
    ).stdout
    digest = _sha256_file(path)
    if hashlib.sha256(committed).hexdigest() != digest:
        raise DevelopmentSecDocumentCollectionError(
            f"published source bytes differ: {relative}"
        )
    return {**proof, "commit": commit, "sha256": digest}


def _load_contract(
    *, manifest_path: Path, env_path: Path, require_published: bool
) -> tuple[dict[str, Any], HistoricalStoreConfig, dict[str, Any], dict[str, Any]]:
    manifest = load_frozen_dataset_contract(manifest_path)
    if manifest.get("dataset_id") != document_contract.DATASET_ID:
        raise DevelopmentSecDocumentCollectionError(
            "unexpected primary-document dataset"
        )
    selection = manifest.get("selection_contract")
    request = manifest.get("request_contract")
    outcome = manifest.get("outcome_lock")
    capacity = manifest.get("capacity_contract")
    if not all(
        isinstance(value, Mapping) for value in (selection, request, outcome, capacity)
    ):
        raise DevelopmentSecDocumentCollectionError(
            "primary-document manifest is incomplete"
        )
    if (
        request.get("stage") != "SEC_PRIMARY_DOCUMENTS_ONLY"
        or request.get("provider") != "SEC_EDGAR"
        or request.get("provider_operated_accession_bound_endpoints_only") is not True
        or request.get("request_count") != selection.get("unique_document_requests")
        or request.get("private_request_graph_sha256")
        != selection.get("document_request_graph_sha256")
        or request.get("raw_source_bytes_retained_outside_git") is not True
        or request.get("per_request_terminal_result_required") is not True
        or request.get("one_failure_cannot_abort_unrelated_requests") is not True
        or request.get("substitution_allowed") is not False
        or request.get("source_semantic_classification_allowed") is not False
        or outcome.get("primary_documents_requested") is not False
        or outcome.get("source_semantics_observed_or_derived") is not False
        or outcome.get("target_outcomes_observed_or_derived") is not False
        or outcome.get("post_entry_data_access_allowed") is not False
        or outcome.get("substitutions_allowed") is not False
        or outcome.get("separate_semantics_review_required") is not True
        or capacity.get("capacity_ready") is not True
        or capacity.get("historical_deletion_allowed") is not False
    ):
        raise DevelopmentSecDocumentCollectionError(
            "primary-document request rules drifted"
        )
    lineage = manifest.get("lineage_contract")
    implementation = manifest.get("implementation_contract")
    if not isinstance(lineage, Mapping) or not isinstance(implementation, Mapping):
        raise DevelopmentSecDocumentCollectionError(
            "primary-document lineage is incomplete"
        )
    for name in (
        "main_submissions_manifest",
        "main_submissions_status",
        "main_submissions_inspection",
        "supplemental_manifest",
        "supplemental_status",
        "supplemental_inspection",
    ):
        value = lineage.get(name)
        if not isinstance(value, Mapping):
            raise DevelopmentSecDocumentCollectionError(f"missing lineage {name}")
        _verify_binding(value)
    files = implementation.get("files")
    if not isinstance(files, Mapping):
        raise DevelopmentSecDocumentCollectionError(
            "primary-document implementation binding is missing"
        )
    for value in files.values():
        if not isinstance(value, Mapping):
            raise DevelopmentSecDocumentCollectionError(
                "primary-document implementation binding is malformed"
            )
        _verify_binding(value)
    config = HistoricalStoreConfig.from_env(env_path)
    if (
        config.min_free_bytes < source_contract.MINIMUM_RESERVE_BYTES
        or shutil.disk_usage(config.root).free < config.min_free_bytes
        or config.root.resolve().is_relative_to(PROJECT_ROOT.resolve())
    ):
        raise DevelopmentSecDocumentCollectionError(
            "historical-store capacity is unsafe"
        )
    private = _read_gzip_object(document_contract._private_contract_path(config.root))
    document_requests = private.get("document_requests")
    pair_joins = private.get("pair_document_joins")
    if (
        _sha256_json(private) != selection.get("private_contract_content_sha256")
        or not isinstance(document_requests, list)
        or not isinstance(pair_joins, list)
        or len(document_requests) != int(selection.get("unique_document_requests", -1))
        or len(pair_joins) != int(selection.get("pair_document_joins", -1))
        or _sha256_json(document_requests)
        != selection.get("document_request_graph_sha256")
        or _sha256_json(pair_joins)
        != selection.get("pair_document_join_graph_sha256")
        or private.get("primary_document_responses_present") is not False
        or private.get("source_semantics_observed_or_derived") is not False
        or private.get("target_outcomes_observed_or_derived") is not False
    ):
        raise DevelopmentSecDocumentCollectionError(
            "private primary-document request graph drifted"
        )
    sec_config = SecConfig.from_env(
        env_path,
        config.root / source_contract.SHARED_SEC_CACHE_NAMESPACE,
        workers=int(request["workers"]),
    )
    if (
        hashlib.sha256(sec_config.user_agent.encode()).hexdigest()
        != request.get("user_agent_sha256")
        or sec_config.minimum_spacing_seconds
        != request.get("global_minimum_spacing_seconds")
        or sec_config.timeout_seconds != request.get("timeout_seconds")
    ):
        raise DevelopmentSecDocumentCollectionError(
            "SEC client differs from the frozen request contract"
        )
    publication: dict[str, Any] = {}
    if require_published:
        publication = {
            "collector": _published_artifact(Path(__file__)),
            "manifest": _published_artifact(manifest_path),
        }
    return manifest, config, private, publication


def _transport_integrity(raw: bytes) -> None:
    if not raw.strip():
        raise DevelopmentSecDocumentCollectionError(
            "SEC primary document is empty"
        )
    lowered = raw[: 256 * 1024].lower()
    if any(marker in lowered for marker in SEC_DENIAL_MARKERS):
        raise DevelopmentSecDocumentCollectionError(
            "SEC returned an automated-access denial document"
        )


def _validate_wrapper(
    wrapper: Mapping[str, Any], *, request: Mapping[str, Any], manifest_sha256: str
) -> None:
    if (
        wrapper.get("schema_version") != COLLECTOR_SCHEMA_VERSION
        or wrapper.get("dataset_id") != document_contract.DATASET_ID
        or wrapper.get("manifest_sha256") != manifest_sha256
        or wrapper.get("request_sha256") != _sha256_json(request)
        or wrapper.get("collector_sha256") != _sha256_file(Path(__file__))
        or wrapper.get("status") not in {"SUCCESS", "FAILED"}
    ):
        raise DevelopmentSecDocumentCollectionError(
            "primary-document wrapper differs from contract"
        )
    if wrapper.get("status") == "SUCCESS" and any(
        (
            wrapper.get("source_sha256") is None,
            int(wrapper.get("source_bytes", 0)) < 1,
            wrapper.get("raw_source_retained") is not True,
            wrapper.get("semantic_classification_performed") is not False,
        )
    ):
        raise DevelopmentSecDocumentCollectionError(
            "successful primary-document wrapper is incomplete"
        )
    if wrapper.get("status") == "FAILED" and not isinstance(
        wrapper.get("error"), Mapping
    ):
        raise DevelopmentSecDocumentCollectionError(
            "failed primary-document wrapper lacks terminal error"
        )


def _success_wrapper(
    *,
    request: Mapping[str, Any],
    manifest_sha256: str,
    raw_path: Path,
    cache_hit: bool,
) -> dict[str, Any]:
    raw = raw_path.read_bytes()
    _transport_integrity(raw)
    return {
        "schema_version": COLLECTOR_SCHEMA_VERSION,
        "dataset_id": document_contract.DATASET_ID,
        "manifest_sha256": manifest_sha256,
        "collector_sha256": _sha256_file(Path(__file__)),
        "request_sha256": _sha256_json(request),
        "captured_at": datetime.now(UTC).isoformat(),
        "status": "SUCCESS",
        "source_origin": "SHARED_CACHE" if cache_hit else "SEC_DOWNLOAD",
        "source_bytes": len(raw),
        "source_sha256": hashlib.sha256(raw).hexdigest(),
        "raw_source_retained": True,
        "semantic_classification_performed": False,
        "derived": None,
    }


def _failure_wrapper(
    *, request: Mapping[str, Any], manifest_sha256: str, error: Exception
) -> dict[str, Any]:
    return {
        "schema_version": COLLECTOR_SCHEMA_VERSION,
        "dataset_id": document_contract.DATASET_ID,
        "manifest_sha256": manifest_sha256,
        "collector_sha256": _sha256_file(Path(__file__)),
        "request_sha256": _sha256_json(request),
        "captured_at": datetime.now(UTC).isoformat(),
        "status": "FAILED",
        "source_origin": None,
        "source_bytes": 0,
        "source_sha256": None,
        "raw_source_retained": False,
        "semantic_classification_performed": False,
        "derived": None,
        "error": {"type": type(error).__name__, "message": str(error)},
    }


def _load_rehashed(
    *,
    path: Path,
    request: Mapping[str, Any],
    manifest_sha256: str,
    store_root: Path,
) -> dict[str, Any]:
    wrapper = _read_gzip_object(path)
    _validate_wrapper(wrapper, request=request, manifest_sha256=manifest_sha256)
    if wrapper["status"] == "SUCCESS":
        source = _shared_path(store_root, request)
        try:
            raw = source.read_bytes()
        except OSError as exc:
            raise DevelopmentSecDocumentCollectionError(
                "successful primary-document source cannot be read"
            ) from exc
        _transport_integrity(raw)
        if (
            len(raw) != wrapper.get("source_bytes")
            or hashlib.sha256(raw).hexdigest() != wrapper.get("source_sha256")
        ):
            raise DevelopmentSecDocumentCollectionError(
                "primary-document raw bytes drifted"
            )
    return wrapper


def _build_index(
    *, manifest: Mapping[str, Any], private: Mapping[str, Any], store_root: Path
) -> dict[str, Any]:
    wrappers: list[dict[str, Any]] = []
    records: list[dict[str, Any]] = []
    counts: Counter[str] = Counter()
    for request in private["document_requests"]:
        path = _wrapper_path(store_root, request)
        if not path.exists():
            continue
        wrapper = _load_rehashed(
            path=path,
            request=request,
            manifest_sha256=str(manifest["manifest_sha256"]),
            store_root=store_root,
        )
        wrapper_sha256 = _sha256_json(wrapper)
        wrappers.append(
            {
                "request_sha256": wrapper["request_sha256"],
                "status": wrapper["status"],
                "wrapper_sha256": wrapper_sha256,
            }
        )
        counts["terminal_requests"] += 1
        counts["successful_requests"] += wrapper["status"] == "SUCCESS"
        counts["failed_requests"] += wrapper["status"] == "FAILED"
        counts["cache_hits"] += wrapper.get("source_origin") == "SHARED_CACHE"
        counts["downloads"] += wrapper.get("source_origin") == "SEC_DOWNLOAD"
        counts["source_bytes"] += int(wrapper.get("source_bytes") or 0)
        records.append(
            {
                "request_sha256": wrapper["request_sha256"],
                "wrapper_sha256": wrapper_sha256,
                "status": wrapper["status"],
                "source_origin": wrapper.get("source_origin"),
                "source_bytes": wrapper.get("source_bytes"),
                "source_sha256": wrapper.get("source_sha256"),
                "error": wrapper.get("error"),
            }
        )
    counts["expected_requests"] = len(private["document_requests"])
    counts["pending_requests"] = counts["expected_requests"] - counts[
        "terminal_requests"
    ]
    return {
        "schema_version": COLLECTOR_SCHEMA_VERSION,
        "dataset_id": document_contract.DATASET_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "collector_sha256": _sha256_file(Path(__file__)),
        "status": (
            "DOCUMENT_COLLECTION_COMPLETE"
            if counts["pending_requests"] == 0
            else "DOCUMENT_COLLECTION_PARTIAL"
        ),
        "counts": dict(sorted(counts.items())),
        "wrappers": wrappers,
        "request_records": records,
        "source_document_request_graph_sha256": private[
            "document_request_graph_sha256"
        ],
        "source_pair_document_join_graph_sha256": private[
            "pair_document_join_graph_sha256"
        ],
        "raw_provider_response_bodies_in_git": False,
        "source_semantics_observed_or_derived": False,
        "verified_positive_catalyst_count": 0,
        "target_outcomes_observed_or_derived": False,
        "substitutions_used": 0,
    }


def _public(index: Mapping[str, Any], publication: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "dataset_id": document_contract.DATASET_ID,
        "manifest_sha256": index["manifest_sha256"],
        "status": index["status"],
        "counts": index["counts"],
        "source_document_request_graph_sha256": index[
            "source_document_request_graph_sha256"
        ],
        "source_pair_document_join_graph_sha256": index[
            "source_pair_document_join_graph_sha256"
        ],
        "private_collection_content_sha256": _sha256_json(index),
        "collector_sha256": index["collector_sha256"],
        "collector_commit": publication.get("collector", {}).get("commit"),
        "symbols_ciks_accessions_urls_joins_and_bodies_public": False,
        "primary_documents_requested": bool(index["counts"].get("terminal_requests")),
        "raw_source_bytes_retained_outside_git": True,
        "source_semantics_observed_or_derived": False,
        "verified_positive_catalyst_count": 0,
        "target_outcomes_observed_or_derived": False,
        "substitutions_used": 0,
        "classification_boundary": (
            "Raw accession-bound SEC documents are transport evidence only; "
            "ownership, issuer binding, timing, direction, materiality, conflicts, "
            "and terminal semantics remain unclassified."
        ),
    }


def collect(
    *,
    manifest_path: Path = MANIFEST,
    env_path: Path = PROJECT_ROOT / ".env",
    status_path: Path = DEFAULT_PUBLIC_STATUS,
    client: Any | None = None,
    require_published: bool = True,
) -> dict[str, Any]:
    manifest, config, private, publication = _load_contract(
        manifest_path=manifest_path,
        env_path=env_path,
        require_published=require_published,
    )
    request_contract = manifest["request_contract"]
    sec_config = SecConfig.from_env(
        env_path,
        config.root / source_contract.SHARED_SEC_CACHE_NAMESPACE,
        workers=int(request_contract["workers"]),
    )
    sec_client = client or SecClient(sec_config)
    pending = [
        request
        for request in private["document_requests"]
        if not _wrapper_path(config.root, request).exists()
    ]

    def load(
        request: Mapping[str, Any],
    ) -> tuple[Mapping[str, Any], Path, bool]:
        path = _shared_path(config.root, request)
        cache_hit = path.exists()
        sec_client.text(str(request["source_url"]), path)
        return request, path, cache_hit

    for result in ordered_bounded_results(
        pending,
        load,
        max_workers=int(request_contract["workers"]),
    ):
        request = result.item
        if result.error is None:
            returned, path, cache_hit = result.unwrap()
            try:
                wrapper = _success_wrapper(
                    request=returned,
                    manifest_sha256=str(manifest["manifest_sha256"]),
                    raw_path=path,
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
        _write_gzip_json(_wrapper_path(config.root, request), wrapper)
    index = _build_index(manifest=manifest, private=private, store_root=config.root)
    _write_gzip_json(_index_path(config.root), index)
    public = _public(index, publication)
    _write_json(status_path, public)
    return public


def inspect(
    *,
    manifest_path: Path = MANIFEST,
    env_path: Path = PROJECT_ROOT / ".env",
    status_path: Path = DEFAULT_PUBLIC_STATUS,
    output_path: Path = DEFAULT_PUBLIC_INSPECTION,
    require_published: bool = True,
) -> dict[str, Any]:
    manifest, config, private, publication = _load_contract(
        manifest_path=manifest_path,
        env_path=env_path,
        require_published=require_published,
    )
    rebuilt = _build_index(manifest=manifest, private=private, store_root=config.root)
    if rebuilt != _read_gzip_object(_index_path(config.root)):
        raise DevelopmentSecDocumentCollectionError(
            "private primary-document collection index drifted"
        )
    public = _public(rebuilt, publication)
    if public != _read_object(status_path):
        raise DevelopmentSecDocumentCollectionError(
            "public primary-document status drifted"
        )
    if rebuilt["counts"]["pending_requests"]:
        raise DevelopmentSecDocumentCollectionError(
            "primary-document collection is incomplete"
        )
    inspection = {
        **public,
        "status": "DOCUMENTS_INSPECTED",
        "source_wrappers_rebuilt": rebuilt["counts"]["terminal_requests"],
        "successful_sources_rehashed": rebuilt["counts"]["successful_requests"],
        "failed_sources_reconciled": rebuilt["counts"]["failed_requests"],
        "all_expected_requests_terminal": True,
        "private_index_matches": True,
        "public_status_matches": True,
        "valid": True,
    }
    _write_json(output_path, inspection)
    return inspection


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env", type=Path, default=PROJECT_ROOT / ".env")
    parser.add_argument("--manifest", type=Path, default=MANIFEST)
    subparsers = parser.add_subparsers(dest="command", required=True)
    collect_parser = subparsers.add_parser("collect")
    collect_parser.add_argument("--status", type=Path, default=DEFAULT_PUBLIC_STATUS)
    inspect_parser = subparsers.add_parser("inspect")
    inspect_parser.add_argument("--status", type=Path, default=DEFAULT_PUBLIC_STATUS)
    inspect_parser.add_argument("--output", type=Path, default=DEFAULT_PUBLIC_INSPECTION)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        if args.command == "collect":
            value = collect(
                manifest_path=args.manifest,
                env_path=args.env,
                status_path=args.status,
            )
        else:
            value = inspect(
                manifest_path=args.manifest,
                env_path=args.env,
                status_path=args.status,
                output_path=args.output,
            )
        print(json.dumps(value, indent=2, sort_keys=True))
        return 0
    except (
        DevelopmentSecDocumentCollectionError,
        HistoricalDiscoveryError,
        HistoricalStoreError,
        LearningDataError,
        OSError,
        subprocess.CalledProcessError,
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

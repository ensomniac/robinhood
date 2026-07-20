"""Collect and inspect the frozen target-window SEC supplemental files."""

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
from datetime import UTC, date, datetime, time as wall_time
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import development_sec_sources as source_contract
import development_sec_submissions as main_collection
import development_sec_supplemental as supplemental_contract
from historical_concurrency import ordered_bounded_results
from historical_discovery import (
    HistoricalDiscoveryError,
    SecClient,
    SecConfig,
    _accepted_at,
    _filing_items,
)
from historical_store import HistoricalStoreConfig, HistoricalStoreError
from learning_data import LearningDataError, load_frozen_dataset_contract


PROJECT_ROOT = Path(__file__).resolve().parent
MANIFEST = (
    PROJECT_ROOT
    / "historical_batches/development_tranche_v2/sec_supplemental_manifests"
    / (
        "dataset-development-sec-supplemental-2026-07-19-v2-"
        "1e2900aedffd20e0c465e0c9e4b8236aa6693711806abd3351c34e4044bc558a.json"
    )
)
DEFAULT_PUBLIC_STATUS = (
    PROJECT_ROOT
    / "historical_batches/development_tranche_v2/sec-supplemental-status.json"
)
DEFAULT_PUBLIC_INSPECTION = (
    PROJECT_ROOT
    / "research_results/2026-07-19-development-sec-supplemental-inspection.json"
)
COLLECTION_INDEX_FILE = "collection-index.json.gz"
COLLECTOR_SCHEMA_VERSION = 1
EASTERN = ZoneInfo("America/New_York")


class DevelopmentSecSupplementalCollectionError(RuntimeError):
    """The frozen supplemental collection is invalid or incomplete."""


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
        raise DevelopmentSecSupplementalCollectionError(
            f"cannot read {path}: {exc}"
        ) from exc
    if not isinstance(value, dict):
        raise DevelopmentSecSupplementalCollectionError(
            f"{path} must contain an object"
        )
    return value


def _read_gzip_object(path: Path) -> dict[str, Any]:
    try:
        with gzip.open(path, "rt", encoding="utf-8") as source:
            value = json.load(source)
    except (OSError, json.JSONDecodeError) as exc:
        raise DevelopmentSecSupplementalCollectionError(
            f"cannot read {path}: {exc}"
        ) from exc
    if not isinstance(value, dict):
        raise DevelopmentSecSupplementalCollectionError(
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
        raise DevelopmentSecSupplementalCollectionError(
            f"path must be repository relative: {path}"
        ) from exc


def _response_root(store_root: Path) -> Path:
    return supplemental_contract._target_response_root(store_root)


def _wrapper_path(store_root: Path, request: Mapping[str, Any]) -> Path:
    digest = hashlib.sha256(str(request["url"]).encode()).hexdigest()
    return _response_root(store_root) / f"{digest}.json.gz"


def _index_path(store_root: Path) -> Path:
    return _response_root(store_root) / COLLECTION_INDEX_FILE


def _shared_path(store_root: Path, request: Mapping[str, Any]) -> Path:
    relative = Path(str(request["shared_cache_relative_path"]))
    if relative.is_absolute() or ".." in relative.parts:
        raise DevelopmentSecSupplementalCollectionError("shared path is unsafe")
    return store_root / source_contract.SHARED_SEC_CACHE_NAMESPACE / relative


def _resolve_public_path(value: Any) -> Path:
    if not isinstance(value, str) or not value:
        raise DevelopmentSecSupplementalCollectionError("bound path is missing")
    path = Path(value)
    if path.is_absolute() or ".." in path.parts:
        raise DevelopmentSecSupplementalCollectionError("bound path is unsafe")
    return PROJECT_ROOT / path


def _verify_binding(value: Mapping[str, Any]) -> None:
    path = _resolve_public_path(value.get("path"))
    if not path.is_file() or _sha256_file(path) != value.get("sha256"):
        raise DevelopmentSecSupplementalCollectionError(f"bound input drifted: {path}")


def _load_contract(
    *, manifest_path: Path, env_path: Path, require_published: bool
) -> tuple[dict[str, Any], HistoricalStoreConfig, dict[str, Any], dict[str, Any]]:
    manifest = load_frozen_dataset_contract(manifest_path)
    if manifest.get("dataset_id") != supplemental_contract.DATASET_ID:
        raise DevelopmentSecSupplementalCollectionError(
            "unexpected supplemental dataset"
        )
    selection = manifest.get("selection_contract")
    request = manifest.get("request_contract")
    outcome = manifest.get("outcome_lock")
    capacity = manifest.get("capacity_contract")
    if not all(isinstance(value, Mapping) for value in (selection, request, outcome, capacity)):
        raise DevelopmentSecSupplementalCollectionError(
            "supplemental manifest is incomplete"
        )
    if (
        request.get("stage") != "SEC_SUPPLEMENTAL_SUBMISSIONS_ONLY"
        or request.get("provider") != "SEC_EDGAR"
        or request.get("provider_operated_endpoints_only") is not True
        or request.get("request_count") != selection.get("selected_request_count")
        or request.get("private_request_graph_sha256")
        != selection.get("selected_request_graph_sha256")
        or request.get("substitution_allowed") is not False
        or request.get("primary_document_access_allowed") is not False
        or outcome.get("primary_documents_requested") is not False
        or outcome.get("target_outcomes_observed_or_derived") is not False
        or outcome.get("substitutions_allowed") is not False
        or capacity.get("capacity_ready") is not True
        or capacity.get("historical_deletion_allowed") is not False
    ):
        raise DevelopmentSecSupplementalCollectionError(
            "supplemental request rules drifted"
        )
    lineage = manifest.get("lineage_contract")
    implementation = manifest.get("implementation_contract")
    if not isinstance(lineage, Mapping) or not isinstance(implementation, Mapping):
        raise DevelopmentSecSupplementalCollectionError(
            "supplemental lineage is incomplete"
        )
    for name in ("sec_source_manifest", "submissions_status", "submissions_inspection"):
        value = lineage.get(name)
        if not isinstance(value, Mapping):
            raise DevelopmentSecSupplementalCollectionError(f"missing lineage {name}")
        _verify_binding(value)
    files = implementation.get("files")
    if not isinstance(files, Mapping):
        raise DevelopmentSecSupplementalCollectionError(
            "supplemental implementation binding is missing"
        )
    for value in files.values():
        if not isinstance(value, Mapping):
            raise DevelopmentSecSupplementalCollectionError(
                "supplemental implementation binding is malformed"
            )
        _verify_binding(value)
    config = HistoricalStoreConfig.from_env(env_path)
    if (
        config.min_free_bytes < source_contract.MINIMUM_RESERVE_BYTES
        or shutil.disk_usage(config.root).free < config.min_free_bytes
        or config.root.resolve().is_relative_to(PROJECT_ROOT.resolve())
    ):
        raise DevelopmentSecSupplementalCollectionError(
            "historical-store capacity is unsafe"
        )
    private = _read_gzip_object(supplemental_contract._private_contract_path(config.root))
    if (
        _sha256_json(private) != selection.get("private_contract_content_sha256")
        or private.get("selected_request_graph_sha256")
        != selection.get("selected_request_graph_sha256")
        or len(private.get("selected_requests", []))
        != int(selection.get("selected_request_count", -1))
    ):
        raise DevelopmentSecSupplementalCollectionError(
            "private supplemental request graph drifted"
        )
    sec_config = SecConfig.from_env(
        env_path,
        config.root / source_contract.SHARED_SEC_CACHE_NAMESPACE,
        workers=int(request["workers"]),
    )
    if hashlib.sha256(sec_config.user_agent.encode()).hexdigest() != request.get(
        "user_agent_sha256"
    ):
        raise DevelopmentSecSupplementalCollectionError(
            "SEC user agent differs from manifest"
        )
    publication: dict[str, Any] = {}
    if require_published:
        publication = {
            "collector": main_collection._published_source(Path(__file__)),
            "manifest": main_collection._published_source(manifest_path),
        }
    return manifest, config, private, publication


def _columnar_rows(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    fields = {key: value for key, value in payload.items() if isinstance(value, list)}
    count = max((len(value) for value in fields.values()), default=0)
    return [
        {
            key: values[index] if index < len(values) else None
            for key, values in fields.items()
        }
        for index in range(count)
    ]


def _document_url(cik: str, accession: str, primary: str) -> str:
    return (
        "https://www.sec.gov/Archives/edgar/data/"
        f"{int(cik)}/{accession.replace('-', '')}/{primary}"
    )


def _derive_response(
    *, request: Mapping[str, Any], payload: Mapping[str, Any]
) -> dict[str, Any]:
    rows = _columnar_rows(payload)
    candidates: dict[str, dict[str, Any]] = {}
    joins: list[dict[str, Any]] = []
    diagnostics: Counter[str] = Counter()
    for row in rows:
        form = str(row.get("form") or "")
        if form not in source_contract.SEC_FORMS:
            continue
        accepted = _accepted_at(row.get("acceptanceDateTime"))
        accession = str(row.get("accessionNumber") or "").strip()
        primary = str(row.get("primaryDocument") or "").strip()
        if accepted is None:
            diagnostics["relevant_form_missing_precise_acceptance"] += 1
            continue
        if not accession:
            diagnostics["relevant_form_missing_accession"] += 1
            continue
        if not primary or Path(primary).name != primary:
            diagnostics["relevant_form_missing_or_unsafe_primary_document"] += 1
            continue
        filing = {
            "form": form,
            "accession": accession,
            "accepted_at": accepted.isoformat(),
            "filing_date": row.get("filingDate"),
            "report_date": row.get("reportDate"),
            "items": _filing_items(row.get("items")),
            "primary_document": primary,
            "source_url": _document_url(str(request["cik"]), accession, primary),
        }
        for window in request["matching_windows"]:
            start = datetime.combine(
                date.fromisoformat(str(window["start"])),
                wall_time(0),
                tzinfo=EASTERN,
            )
            cutoff = datetime.combine(
                date.fromisoformat(str(window["date"])),
                wall_time(9, 35),
                tzinfo=EASTERN,
            )
            if start <= accepted <= cutoff:
                candidates[filing["source_url"]] = filing
                joins.append(
                    {
                        "date": window["date"],
                        "instrument_id": window["instrument_id"],
                        "primary_exchange": window["primary_exchange"],
                        "rank": window["rank"],
                        "symbol": window["symbol"],
                        "filing": filing,
                    }
                )
    return {
        "source_row_count": len(rows),
        "candidate_filings": sorted(
            candidates.values(),
            key=lambda row: (row["accepted_at"], row["accession"]),
        ),
        "pair_filing_joins": sorted(
            joins,
            key=lambda row: (
                str(row["date"]),
                int(row["rank"]),
                str(row["filing"]["accepted_at"]),
            ),
        ),
        "diagnostics": dict(sorted(diagnostics.items())),
    }


def _validate_wrapper(
    wrapper: Mapping[str, Any], *, request: Mapping[str, Any], manifest_sha256: str
) -> None:
    if (
        wrapper.get("schema_version") != COLLECTOR_SCHEMA_VERSION
        or wrapper.get("dataset_id") != supplemental_contract.DATASET_ID
        or wrapper.get("manifest_sha256") != manifest_sha256
        or wrapper.get("request_sha256") != _sha256_json(request)
        or wrapper.get("collector_sha256") != _sha256_file(Path(__file__))
        or wrapper.get("status") not in {"SUCCESS", "FAILED"}
    ):
        raise DevelopmentSecSupplementalCollectionError(
            "supplemental wrapper differs from contract"
        )


def _success_wrapper(
    *,
    request: Mapping[str, Any],
    manifest_sha256: str,
    payload: Mapping[str, Any],
    raw_path: Path,
    cache_hit: bool,
) -> dict[str, Any]:
    raw = raw_path.read_bytes()
    return {
        "schema_version": COLLECTOR_SCHEMA_VERSION,
        "dataset_id": supplemental_contract.DATASET_ID,
        "manifest_sha256": manifest_sha256,
        "collector_sha256": _sha256_file(Path(__file__)),
        "request_sha256": _sha256_json(request),
        "captured_at": datetime.now(UTC).isoformat(),
        "status": "SUCCESS",
        "source_origin": "SHARED_CACHE" if cache_hit else "SEC_DOWNLOAD",
        "source_bytes": len(raw),
        "source_sha256": hashlib.sha256(raw).hexdigest(),
        "derived": _derive_response(request=request, payload=payload),
    }


def _failure_wrapper(
    *, request: Mapping[str, Any], manifest_sha256: str, error: Exception
) -> dict[str, Any]:
    return {
        "schema_version": COLLECTOR_SCHEMA_VERSION,
        "dataset_id": supplemental_contract.DATASET_ID,
        "manifest_sha256": manifest_sha256,
        "collector_sha256": _sha256_file(Path(__file__)),
        "request_sha256": _sha256_json(request),
        "captured_at": datetime.now(UTC).isoformat(),
        "status": "FAILED",
        "source_origin": None,
        "source_bytes": 0,
        "source_sha256": None,
        "derived": None,
        "error": {"type": type(error).__name__, "message": str(error)},
    }


def _load_rederived(
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
            payload = json.loads(raw)
        except (OSError, json.JSONDecodeError) as exc:
            raise DevelopmentSecSupplementalCollectionError(
                "successful supplemental source cannot be read"
            ) from exc
        if (
            not isinstance(payload, Mapping)
            or len(raw) != wrapper.get("source_bytes")
            or hashlib.sha256(raw).hexdigest() != wrapper.get("source_sha256")
            or _derive_response(request=request, payload=payload)
            != wrapper.get("derived")
        ):
            raise DevelopmentSecSupplementalCollectionError(
                "supplemental derivation drifted"
            )
    return wrapper


def _build_index(
    *, manifest: Mapping[str, Any], private: Mapping[str, Any], store_root: Path
) -> dict[str, Any]:
    wrappers: list[dict[str, Any]] = []
    records: list[dict[str, Any]] = []
    documents: dict[str, dict[str, Any]] = {}
    joins: list[dict[str, Any]] = []
    counts: Counter[str] = Counter()
    for request in private["selected_requests"]:
        path = _wrapper_path(store_root, request)
        if not path.exists():
            continue
        wrapper = _load_rederived(
            path=path,
            request=request,
            manifest_sha256=str(manifest["manifest_sha256"]),
            store_root=store_root,
        )
        wrappers.append(
            {
                "request_sha256": wrapper["request_sha256"],
                "status": wrapper["status"],
                "wrapper_sha256": _sha256_json(wrapper),
            }
        )
        counts["terminal_requests"] += 1
        counts["successful_requests"] += wrapper["status"] == "SUCCESS"
        counts["failed_requests"] += wrapper["status"] == "FAILED"
        counts["cache_hits"] += wrapper.get("source_origin") == "SHARED_CACHE"
        counts["downloads"] += wrapper.get("source_origin") == "SEC_DOWNLOAD"
        counts["source_bytes"] += int(wrapper.get("source_bytes") or 0)
        record = {
            "request": request,
            "status": wrapper["status"],
            "wrapper_sha256": _sha256_json(wrapper),
            "error": wrapper.get("error"),
        }
        if wrapper["status"] == "SUCCESS":
            derived = wrapper["derived"]
            record["derived"] = derived
            counts["candidate_filings"] += len(derived["candidate_filings"])
            counts["pair_filing_joins"] += len(derived["pair_filing_joins"])
            joins.extend(derived["pair_filing_joins"])
            for filing in derived["candidate_filings"]:
                documents[filing["source_url"]] = {
                    "cik": request["cik"],
                    **filing,
                }
        records.append(record)
    counts["expected_requests"] = len(private["selected_requests"])
    counts["pending_requests"] = counts["expected_requests"] - counts[
        "terminal_requests"
    ]
    counts["pairs_with_candidate_filing"] = len(
        {(str(row["date"]), str(row["instrument_id"])) for row in joins}
    )
    document_rows = sorted(documents.values(), key=lambda row: row["source_url"])
    return {
        "schema_version": 1,
        "dataset_id": supplemental_contract.DATASET_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "collector_sha256": _sha256_file(Path(__file__)),
        "status": (
            "SUPPLEMENTAL_COLLECTION_COMPLETE"
            if counts["pending_requests"] == 0
            else "SUPPLEMENTAL_COLLECTION_PARTIAL"
        ),
        "counts": dict(sorted(counts.items())),
        "wrappers": wrappers,
        "request_records": records,
        "candidate_document_requests": document_rows,
        "candidate_document_graph_sha256": _sha256_json(document_rows),
        "pair_filing_joins": sorted(
            joins,
            key=lambda row: (
                str(row["date"]),
                int(row["rank"]),
                str(row["filing"]["accepted_at"]),
            ),
        ),
        "primary_documents_requested": False,
        "target_outcomes_observed_or_derived": False,
        "substitutions_used": 0,
    }


def _public(index: Mapping[str, Any], publication: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "dataset_id": supplemental_contract.DATASET_ID,
        "manifest_sha256": index["manifest_sha256"],
        "status": index["status"],
        "counts": index["counts"],
        "unique_candidate_document_count": len(index["candidate_document_requests"]),
        "candidate_document_graph_sha256": index[
            "candidate_document_graph_sha256"
        ],
        "private_collection_content_sha256": _sha256_json(index),
        "collector_sha256": index["collector_sha256"],
        "collector_commit": publication.get("collector", {}).get("commit"),
        "symbols_ciks_accessions_and_urls_public": False,
        "primary_documents_requested": False,
        "verified_positive_catalyst_count": 0,
        "target_outcomes_observed_or_derived": False,
        "substitutions_used": 0,
        "classification_boundary": (
            "Supplemental submissions metadata can add time-valid filing candidates; "
            "documents and semantics remain unobserved."
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
        for request in private["selected_requests"]
        if not _wrapper_path(config.root, request).exists()
    ]

    def load(request: Mapping[str, Any]) -> tuple[Mapping[str, Any], Mapping[str, Any], Path, bool]:
        path = _shared_path(config.root, request)
        cache_hit = path.exists()
        payload = sec_client.json(str(request["url"]), path)
        return request, payload, path, cache_hit

    for outcome in ordered_bounded_results(
        pending,
        load,
        max_workers=int(request_contract["workers"]),
    ):
        request = outcome.item
        if outcome.error is None:
            returned, payload, path, cache_hit = outcome.unwrap()
            try:
                wrapper = _success_wrapper(
                    request=returned,
                    manifest_sha256=str(manifest["manifest_sha256"]),
                    payload=payload,
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
                error=outcome.error,
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
        raise DevelopmentSecSupplementalCollectionError(
            "private supplemental index drifted"
        )
    public = _public(rebuilt, publication)
    if public != _read_object(status_path):
        raise DevelopmentSecSupplementalCollectionError(
            "public supplemental status drifted"
        )
    if rebuilt["counts"]["pending_requests"]:
        raise DevelopmentSecSupplementalCollectionError(
            "supplemental collection is incomplete"
        )
    inspection = {
        **public,
        "status": "SUPPLEMENTAL_INSPECTED",
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
        DevelopmentSecSupplementalCollectionError,
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

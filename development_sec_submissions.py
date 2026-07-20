"""Collect and inspect a frozen SEC submissions graph without document access."""

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
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import development_sec_sources as contract_source
from historical_concurrency import ordered_bounded_results
from historical_discovery import (
    SEC_SUBMISSIONS_ROOT,
    HistoricalDiscoveryError,
    SecClient,
    SecConfig,
    _accepted_at,
    _filing_items,
    _submission_recent,
)
from historical_store import HistoricalStoreConfig, HistoricalStoreError
from learning_data import LearningDataError, load_frozen_dataset_contract


PROJECT_ROOT = Path(__file__).resolve().parent
MANIFEST = (
    PROJECT_ROOT
    / "historical_batches/development_tranche_v2/sec_manifests"
    / (
        "dataset-development-sec-primary-sources-2026-07-19-v2-"
        "ecabc373f600864beb3642a67417a2262b5aad58f68ce1e091aab115e683d264.json"
    )
)
DEFAULT_PUBLIC_STATUS = (
    PROJECT_ROOT
    / "historical_batches/development_tranche_v2/sec-submissions-status.json"
)
DEFAULT_PUBLIC_INSPECTION = (
    PROJECT_ROOT
    / "research_results/2026-07-19-development-sec-submissions-inspection.json"
)
COLLECTION_INDEX_FILE = "collection-index.json.gz"
COLLECTOR_SCHEMA_VERSION = 1


class DevelopmentSecSubmissionsError(RuntimeError):
    """The frozen SEC submissions acquisition is invalid or incomplete."""


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
        raise DevelopmentSecSubmissionsError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise DevelopmentSecSubmissionsError(f"{path} must contain an object")
    return value


def _read_gzip_object(path: Path) -> dict[str, Any]:
    try:
        with gzip.open(path, "rt", encoding="utf-8") as source:
            value = json.load(source)
    except (OSError, json.JSONDecodeError) as exc:
        raise DevelopmentSecSubmissionsError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise DevelopmentSecSubmissionsError(f"{path} must contain an object")
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
        raise DevelopmentSecSubmissionsError(
            f"path must be repository relative: {path}"
        ) from exc


def _private_identity_path(store_root: Path) -> Path:
    return contract_source._private_identity_path(store_root)


def _submissions_root(store_root: Path) -> Path:
    return contract_source._target_response_root(store_root) / "submissions"


def _wrapper_path(store_root: Path, cik: str) -> Path:
    return _submissions_root(store_root) / f"CIK{cik}.json.gz"


def _collection_index_path(store_root: Path) -> Path:
    return _submissions_root(store_root) / COLLECTION_INDEX_FILE


def _shared_cache_path(store_root: Path, request: Mapping[str, Any]) -> Path:
    relative = Path(str(request["shared_cache_relative_path"]))
    if relative.is_absolute() or ".." in relative.parts:
        raise DevelopmentSecSubmissionsError("shared cache path is unsafe")
    return store_root / contract_source.SHARED_SEC_CACHE_NAMESPACE / relative


def _resolve_public_path(value: Any) -> Path:
    if not isinstance(value, str) or not value:
        raise DevelopmentSecSubmissionsError("manifest path is missing")
    relative = Path(value)
    if relative.is_absolute() or ".." in relative.parts:
        raise DevelopmentSecSubmissionsError("manifest path is unsafe")
    return PROJECT_ROOT / relative


def _verify_file_binding(value: Mapping[str, Any]) -> None:
    path = _resolve_public_path(value.get("path"))
    if not path.is_file() or _sha256_file(path) != value.get("sha256"):
        raise DevelopmentSecSubmissionsError(f"bound input drifted: {path}")


def _published_source(path: Path) -> dict[str, str]:
    relative = _repo_path(path)
    status = subprocess.run(
        ["git", "status", "--porcelain", "--", relative],
        cwd=PROJECT_ROOT,
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    if status.strip():
        raise DevelopmentSecSubmissionsError(f"provider input is not committed: {relative}")
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
        raise DevelopmentSecSubmissionsError(
            "provider access requires HEAD to equal its pushed upstream"
        )
    committed = subprocess.run(
        ["git", "show", f"HEAD:{relative}"],
        cwd=PROJECT_ROOT,
        check=True,
        capture_output=True,
    ).stdout
    if hashlib.sha256(committed).hexdigest() != _sha256_file(path):
        raise DevelopmentSecSubmissionsError(f"committed bytes differ: {relative}")
    return {"commit": head, "path": relative, "sha256": _sha256_file(path)}


def _load_contract(
    *, manifest_path: Path, env_path: Path, require_published: bool
) -> tuple[dict[str, Any], HistoricalStoreConfig, dict[str, Any], dict[str, Any]]:
    manifest = load_frozen_dataset_contract(manifest_path)
    if manifest.get("dataset_id") != contract_source.DATASET_ID:
        raise DevelopmentSecSubmissionsError("unexpected SEC source dataset")
    identity = manifest.get("identity_contract")
    request = manifest.get("request_contract")
    outcome = manifest.get("outcome_lock")
    capacity = manifest.get("capacity_contract")
    if not all(isinstance(value, Mapping) for value in (identity, request, outcome, capacity)):
        raise DevelopmentSecSubmissionsError("SEC source manifest is incomplete")
    if (
        request.get("stage") != "SEC_SUBMISSIONS_ONLY"
        or request.get("provider") != "SEC_EDGAR"
        or request.get("forms") != list(contract_source.SEC_FORMS)
        or request.get("provider_operated_endpoints_only") is not True
        or request.get("global_minimum_spacing_seconds")
        != contract_source.SEC_MINIMUM_SPACING_SECONDS
        or request.get("timeout_seconds") != contract_source.SEC_TIMEOUT_SECONDS
        or request.get("maximum_attempts") != contract_source.SEC_MAX_ATTEMPTS
        or request.get("retry_backoff_seconds")
        != list(contract_source.SEC_RETRY_BACKOFF_SECONDS)
        or request.get("failure_policy", {}).get("substitution_allowed") is not False
        or request.get("staging", {}).get(
            "accession_bound_document_manifest_required_before_request"
        )
        is not True
        or outcome.get("post_entry_data_access_allowed") is not False
        or outcome.get("target_outcomes_observed_or_derived") is not False
        or outcome.get("substitutions_allowed") is not False
        or capacity.get("capacity_ready") is not True
        or capacity.get("historical_deletion_allowed") is not False
        or int(capacity.get("minimum_required_reserve_bytes", -1))
        != contract_source.MINIMUM_RESERVE_BYTES
    ):
        raise DevelopmentSecSubmissionsError("SEC source manifest rules drifted")

    lineage = manifest.get("lineage_contract")
    implementation = manifest.get("implementation_contract")
    if not isinstance(lineage, Mapping) or not isinstance(implementation, Mapping):
        raise DevelopmentSecSubmissionsError("SEC source lineage is incomplete")
    for name in ("source_semantics_manifest", "selected_pair_manifest"):
        value = lineage.get(name)
        if not isinstance(value, Mapping):
            raise DevelopmentSecSubmissionsError(f"missing lineage {name}")
        _verify_file_binding(value)
    master = lineage.get("security_master")
    strategy = lineage.get("strategy")
    if not isinstance(master, Mapping) or not isinstance(strategy, Mapping):
        raise DevelopmentSecSubmissionsError("master or strategy lineage is missing")
    for path_key, hash_key in (
        ("path", "file_sha256"),
        ("attestation_path", "attestation_sha256"),
    ):
        path = _resolve_public_path(master.get(path_key))
        if _sha256_file(path) != master.get(hash_key):
            raise DevelopmentSecSubmissionsError("security-master lineage drifted")
    for path_key, hash_key in (
        ("config_path", "config_sha256"),
        ("attestation_path", "attestation_sha256"),
    ):
        path = _resolve_public_path(strategy.get(path_key))
        if _sha256_file(path) != strategy.get(hash_key):
            raise DevelopmentSecSubmissionsError("strategy lineage drifted")
    files = implementation.get("files")
    if not isinstance(files, Mapping):
        raise DevelopmentSecSubmissionsError("implementation lineage is missing")
    for value in files.values():
        if not isinstance(value, Mapping):
            raise DevelopmentSecSubmissionsError("implementation binding is malformed")
        _verify_file_binding(value)

    config = HistoricalStoreConfig.from_env(env_path)
    if (
        config.min_free_bytes < contract_source.MINIMUM_RESERVE_BYTES
        or shutil.disk_usage(config.root).free < config.min_free_bytes
        or config.root.resolve().is_relative_to(PROJECT_ROOT.resolve())
    ):
        raise DevelopmentSecSubmissionsError("historical-store capacity is unsafe")
    private = _read_gzip_object(_private_identity_path(config.root))
    if (
        _sha256_json(private) != identity.get("private_identity_content_sha256")
        or private.get("submission_request_graph_sha256")
        != identity.get("submission_request_graph_sha256")
        or len(private.get("submission_requests", []))
        != int(identity.get("unique_cik_count", -1))
        or int(request.get("request_count", -1))
        != int(identity.get("unique_cik_count", -1))
    ):
        raise DevelopmentSecSubmissionsError("private SEC request graph drifted")
    sec_config = SecConfig.from_env(
        env_path,
        config.root / contract_source.SHARED_SEC_CACHE_NAMESPACE,
        workers=int(request["workers"]),
    )
    if hashlib.sha256(sec_config.user_agent.encode()).hexdigest() != request.get(
        "user_agent_sha256"
    ):
        raise DevelopmentSecSubmissionsError("SEC user agent differs from manifest")
    publication: dict[str, Any] = {}
    if require_published:
        publication = {
            "collector": _published_source(Path(__file__)),
            "manifest": _published_source(manifest_path),
        }
    return manifest, config, private, publication


def _supplemental_rows(payload: Mapping[str, Any]) -> tuple[list[dict[str, Any]], int]:
    filings = payload.get("filings")
    values = filings.get("files") if isinstance(filings, Mapping) else None
    if not isinstance(values, list):
        return [], 0
    rows: list[dict[str, Any]] = []
    malformed = 0
    for value in values:
        if not isinstance(value, Mapping):
            malformed += 1
            continue
        name = str(value.get("name") or "").strip()
        if not name or Path(name).name != name or not name.endswith(".json"):
            malformed += 1
            continue
        rows.append(
            {
                "name": name,
                "filing_from": value.get("filingFrom"),
                "filing_to": value.get("filingTo"),
                "url": f"{SEC_SUBMISSIONS_ROOT}/{name}",
                "shared_cache_relative_path": f"submissions/files/{name}",
            }
        )
    return sorted(rows, key=lambda row: row["name"]), malformed


def _document_url(cik: str, accession: str, primary_document: str) -> str:
    return (
        "https://www.sec.gov/Archives/edgar/data/"
        f"{int(cik)}/{accession.replace('-', '')}/{primary_document}"
    )


def _derive_response(
    *, cik: str, payload: Mapping[str, Any], pairs: Sequence[Mapping[str, Any]]
) -> dict[str, Any]:
    payload_cik = str(payload.get("cik") or "").strip()
    if not payload_cik.isdigit() or payload_cik.zfill(10) != cik:
        raise DevelopmentSecSubmissionsError("SEC submissions payload CIK differs")
    recent = list(_submission_recent(payload).values())
    candidates: list[dict[str, Any]] = []
    diagnostics: Counter[str] = Counter()
    pair_joins: list[dict[str, Any]] = []
    for row in recent:
        form = str(row.get("form") or "")
        if form not in contract_source.SEC_FORMS:
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
        candidate = {
            "form": form,
            "accession": accession,
            "accepted_at": accepted.isoformat(),
            "filing_date": row.get("filingDate"),
            "report_date": row.get("reportDate"),
            "items": _filing_items(row.get("items")),
            "primary_document": primary,
            "source_url": _document_url(cik, accession, primary),
        }
        matching_pairs: list[dict[str, Any]] = []
        for pair in pairs:
            start = datetime.fromisoformat(str(pair["window_start_et"]))
            cutoff = datetime.fromisoformat(str(pair["window_cutoff_et"]))
            if start <= accepted <= cutoff:
                matching_pairs.append(
                    {
                        "date": pair["date"],
                        "instrument_id": pair["instrument_id"],
                        "primary_exchange": pair["primary_exchange"],
                        "rank": pair["rank"],
                        "symbol": pair["symbol"],
                    }
                )
        if not matching_pairs:
            continue
        candidates.append(candidate)
        for pair in matching_pairs:
            pair_joins.append({**pair, "filing": candidate})
    supplemental, malformed_supplemental = _supplemental_rows(payload)
    diagnostics["malformed_supplemental_descriptor"] += malformed_supplemental
    candidates = sorted(
        {row["source_url"]: row for row in candidates}.values(),
        key=lambda row: (row["accepted_at"], row["accession"]),
    )
    return {
        "recent_filing_row_count": len(recent),
        "candidate_filings": candidates,
        "pair_filing_joins": sorted(
            pair_joins,
            key=lambda row: (
                str(row["date"]),
                int(row["rank"]),
                str(row["filing"]["accepted_at"]),
            ),
        ),
        "supplemental_submission_requests": supplemental,
        "diagnostics": dict(sorted(diagnostics.items())),
    }


def _validate_wrapper(
    wrapper: Mapping[str, Any], *, request: Mapping[str, Any], manifest_sha256: str
) -> None:
    if (
        wrapper.get("schema_version") != COLLECTOR_SCHEMA_VERSION
        or wrapper.get("dataset_id") != contract_source.DATASET_ID
        or wrapper.get("manifest_sha256") != manifest_sha256
        or wrapper.get("request_sha256") != _sha256_json(request)
        or wrapper.get("collector_sha256") != _sha256_file(Path(__file__))
        or wrapper.get("status") not in {"SUCCESS", "FAILED"}
    ):
        raise DevelopmentSecSubmissionsError("submission wrapper differs from contract")
    if wrapper.get("status") == "SUCCESS" and not isinstance(
        wrapper.get("derived"), Mapping
    ):
        raise DevelopmentSecSubmissionsError("successful wrapper lacks derived data")
    if wrapper.get("status") == "FAILED" and not isinstance(wrapper.get("error"), Mapping):
        raise DevelopmentSecSubmissionsError("failed wrapper lacks terminal error")


def _successful_wrapper(
    *,
    request: Mapping[str, Any],
    manifest_sha256: str,
    payload: Mapping[str, Any],
    raw_path: Path,
    cache_hit: bool,
    pairs: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    raw = raw_path.read_bytes()
    return {
        "schema_version": COLLECTOR_SCHEMA_VERSION,
        "dataset_id": contract_source.DATASET_ID,
        "manifest_sha256": manifest_sha256,
        "collector_sha256": _sha256_file(Path(__file__)),
        "request_sha256": _sha256_json(request),
        "captured_at": datetime.now(UTC).isoformat(),
        "status": "SUCCESS",
        "source_origin": "SHARED_CACHE" if cache_hit else "SEC_DOWNLOAD",
        "source_bytes": len(raw),
        "source_sha256": hashlib.sha256(raw).hexdigest(),
        "derived": _derive_response(
            cik=str(request["cik"]), payload=payload, pairs=pairs
        ),
    }


def _failed_wrapper(
    *, request: Mapping[str, Any], manifest_sha256: str, error: Exception
) -> dict[str, Any]:
    return {
        "schema_version": COLLECTOR_SCHEMA_VERSION,
        "dataset_id": contract_source.DATASET_ID,
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


def _load_and_rederive_wrapper(
    *,
    wrapper_path: Path,
    request: Mapping[str, Any],
    manifest_sha256: str,
    store_root: Path,
    pairs: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    wrapper = _read_gzip_object(wrapper_path)
    _validate_wrapper(wrapper, request=request, manifest_sha256=manifest_sha256)
    if wrapper["status"] == "SUCCESS":
        raw_path = _shared_cache_path(store_root, request)
        try:
            raw = raw_path.read_bytes()
            payload = json.loads(raw)
        except (OSError, json.JSONDecodeError) as exc:
            raise DevelopmentSecSubmissionsError(
                "successful submission source cannot be independently read"
            ) from exc
        if (
            not isinstance(payload, Mapping)
            or len(raw) != wrapper.get("source_bytes")
            or hashlib.sha256(raw).hexdigest() != wrapper.get("source_sha256")
            or _derive_response(cik=str(request["cik"]), payload=payload, pairs=pairs)
            != wrapper.get("derived")
        ):
            raise DevelopmentSecSubmissionsError("submission response derivation drifted")
    return wrapper


def _build_collection_index(
    *, manifest: Mapping[str, Any], private: Mapping[str, Any], store_root: Path
) -> dict[str, Any]:
    pairs_by_cik: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    missing_pairs: list[dict[str, Any]] = []
    for pair in private["pairs"]:
        if pair.get("cik"):
            pairs_by_cik[str(pair["cik"])].append(pair)
        else:
            missing_pairs.append(dict(pair))
    wrappers: list[dict[str, Any]] = []
    request_records: list[dict[str, Any]] = []
    supplemental: dict[str, dict[str, Any]] = {}
    documents: dict[str, dict[str, Any]] = {}
    pair_joins: list[dict[str, Any]] = []
    counts: Counter[str] = Counter()
    for request in private["submission_requests"]:
        cik = str(request["cik"])
        path = _wrapper_path(store_root, cik)
        if not path.exists():
            continue
        wrapper = _load_and_rederive_wrapper(
            wrapper_path=path,
            request=request,
            manifest_sha256=str(manifest["manifest_sha256"]),
            store_root=store_root,
            pairs=pairs_by_cik[cik],
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
            "cik": cik,
            "request": request,
            "wrapper_status": wrapper["status"],
            "wrapper_sha256": _sha256_json(wrapper),
            "error": wrapper.get("error"),
        }
        if wrapper["status"] == "SUCCESS":
            derived = wrapper["derived"]
            record["derived"] = derived
            counts["candidate_filings"] += len(derived["candidate_filings"])
            counts["pair_filing_joins"] += len(derived["pair_filing_joins"])
            pair_joins.extend(derived["pair_filing_joins"])
            for row in derived["supplemental_submission_requests"]:
                supplemental[row["url"]] = {"cik": cik, **row}
            for row in derived["candidate_filings"]:
                documents[row["source_url"]] = {"cik": cik, **row}
        request_records.append(record)
    counts["expected_requests"] = len(private["submission_requests"])
    counts["pending_requests"] = counts["expected_requests"] - counts["terminal_requests"]
    counts["missing_cik_pairs"] = len(missing_pairs)
    counts["selected_pairs"] = len(private["pairs"])
    counts["pairs_with_candidate_filing"] = len(
        {(str(row["date"]), str(row["instrument_id"])) for row in pair_joins}
    )
    supplemental_rows = sorted(supplemental.values(), key=lambda row: row["url"])
    document_rows = sorted(documents.values(), key=lambda row: row["source_url"])
    return {
        "schema_version": COLLECTOR_SCHEMA_VERSION,
        "dataset_id": contract_source.DATASET_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "collector_sha256": _sha256_file(Path(__file__)),
        "status": (
            "SUBMISSIONS_COLLECTION_COMPLETE"
            if counts["pending_requests"] == 0
            else "SUBMISSIONS_COLLECTION_PARTIAL"
        ),
        "counts": dict(sorted(counts.items())),
        "wrappers": wrappers,
        "request_records": request_records,
        "missing_cik_pairs": missing_pairs,
        "supplemental_submission_requests": supplemental_rows,
        "supplemental_request_graph_sha256": _sha256_json(supplemental_rows),
        "candidate_document_requests": document_rows,
        "candidate_document_graph_sha256": _sha256_json(document_rows),
        "pair_filing_joins": sorted(
            pair_joins,
            key=lambda row: (
                str(row["date"]),
                int(row["rank"]),
                str(row["filing"]["accepted_at"]),
            ),
        ),
        "provider_response_bodies_in_git": False,
        "primary_documents_requested": False,
        "supplemental_submission_files_requested": False,
        "target_outcomes_observed_or_derived": False,
        "substitutions_used": 0,
    }


def _public_status(
    *, index: Mapping[str, Any], publication: Mapping[str, Any]
) -> dict[str, Any]:
    counts = index["counts"]
    return {
        "schema_version": 1,
        "dataset_id": contract_source.DATASET_ID,
        "manifest_sha256": index["manifest_sha256"],
        "status": index["status"],
        "counts": counts,
        "unique_supplemental_request_count": len(
            index["supplemental_submission_requests"]
        ),
        "unique_candidate_document_count": len(index["candidate_document_requests"]),
        "supplemental_request_graph_sha256": index[
            "supplemental_request_graph_sha256"
        ],
        "candidate_document_graph_sha256": index[
            "candidate_document_graph_sha256"
        ],
        "private_collection_content_sha256": _sha256_json(index),
        "collector_sha256": index["collector_sha256"],
        "collector_commit": publication.get("collector", {}).get("commit"),
        "symbols_ciks_accessions_and_urls_public": False,
        "primary_documents_requested": False,
        "supplemental_submission_files_requested": False,
        "verified_positive_catalyst_count": 0,
        "classification_boundary": (
            "Submissions metadata can discover time-valid filing candidates; "
            "it does not verify source semantics, positive direction, or materiality."
        ),
        "target_outcomes_observed_or_derived": False,
        "substitutions_used": 0,
    }


def collect_submissions(
    *,
    manifest_path: Path = MANIFEST,
    env_path: Path = PROJECT_ROOT / ".env",
    public_status_path: Path = DEFAULT_PUBLIC_STATUS,
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
        config.root / contract_source.SHARED_SEC_CACHE_NAMESPACE,
        workers=int(request_contract["workers"]),
    )
    sec_client = client or SecClient(sec_config)
    pairs_by_cik: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for pair in private["pairs"]:
        if pair.get("cik"):
            pairs_by_cik[str(pair["cik"])].append(pair)
    pending = [
        request
        for request in private["submission_requests"]
        if not _wrapper_path(config.root, str(request["cik"])).exists()
    ]

    def load(request: Mapping[str, Any]) -> tuple[Mapping[str, Any], Mapping[str, Any], Path, bool]:
        raw_path = _shared_cache_path(config.root, request)
        cache_hit = raw_path.exists()
        payload = sec_client.json(str(request["url"]), raw_path)
        return request, payload, raw_path, cache_hit

    completed = len(private["submission_requests"]) - len(pending)
    for outcome in ordered_bounded_results(
        pending,
        load,
        max_workers=int(request_contract["workers"]),
    ):
        request = outcome.item
        if outcome.error is None:
            returned_request, payload, raw_path, cache_hit = outcome.unwrap()
            try:
                wrapper = _successful_wrapper(
                    request=returned_request,
                    manifest_sha256=str(manifest["manifest_sha256"]),
                    payload=payload,
                    raw_path=raw_path,
                    cache_hit=cache_hit,
                    pairs=pairs_by_cik[str(request["cik"])],
                )
            except Exception as exc:
                wrapper = _failed_wrapper(
                    request=request,
                    manifest_sha256=str(manifest["manifest_sha256"]),
                    error=exc,
                )
        else:
            wrapper = _failed_wrapper(
                request=request,
                manifest_sha256=str(manifest["manifest_sha256"]),
                error=outcome.error,
            )
        _write_gzip_json(_wrapper_path(config.root, str(request["cik"])), wrapper)
        completed += 1
        if completed % 25 == 0 or completed == len(private["submission_requests"]):
            print(
                json.dumps(
                    {
                        "completed_requests": completed,
                        "expected_requests": len(private["submission_requests"]),
                    },
                    sort_keys=True,
                ),
                flush=True,
            )
    index = _build_collection_index(
        manifest=manifest, private=private, store_root=config.root
    )
    _write_gzip_json(_collection_index_path(config.root), index)
    public = _public_status(index=index, publication=publication)
    _write_json(public_status_path, public)
    return public


def inspect_submissions(
    *,
    manifest_path: Path = MANIFEST,
    env_path: Path = PROJECT_ROOT / ".env",
    public_status_path: Path = DEFAULT_PUBLIC_STATUS,
    inspection_path: Path = DEFAULT_PUBLIC_INSPECTION,
    require_published: bool = True,
) -> dict[str, Any]:
    manifest, config, private, publication = _load_contract(
        manifest_path=manifest_path,
        env_path=env_path,
        require_published=require_published,
    )
    rebuilt = _build_collection_index(
        manifest=manifest, private=private, store_root=config.root
    )
    stored = _read_gzip_object(_collection_index_path(config.root))
    if rebuilt != stored:
        raise DevelopmentSecSubmissionsError("private submissions index drifted")
    public = _public_status(index=rebuilt, publication=publication)
    if _read_object(public_status_path) != public:
        raise DevelopmentSecSubmissionsError("public submissions status drifted")
    if rebuilt["counts"]["pending_requests"]:
        raise DevelopmentSecSubmissionsError("submissions collection is incomplete")
    inspection = {
        **public,
        "status": "SUBMISSIONS_INSPECTED",
        "source_wrappers_rebuilt": rebuilt["counts"]["terminal_requests"],
        "successful_sources_rehashed": rebuilt["counts"]["successful_requests"],
        "failed_sources_reconciled": rebuilt["counts"]["failed_requests"],
        "all_expected_requests_terminal": True,
        "private_index_matches": True,
        "public_status_matches": True,
        "valid": True,
    }
    _write_json(inspection_path, inspection)
    return inspection


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env", type=Path, default=PROJECT_ROOT / ".env")
    parser.add_argument("--manifest", type=Path, default=MANIFEST)
    subparsers = parser.add_subparsers(dest="command", required=True)
    collect = subparsers.add_parser("collect")
    collect.add_argument("--status", type=Path, default=DEFAULT_PUBLIC_STATUS)
    inspect = subparsers.add_parser("inspect")
    inspect.add_argument("--status", type=Path, default=DEFAULT_PUBLIC_STATUS)
    inspect.add_argument("--output", type=Path, default=DEFAULT_PUBLIC_INSPECTION)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        if args.command == "collect":
            value = collect_submissions(
                manifest_path=args.manifest,
                env_path=args.env,
                public_status_path=args.status,
            )
        else:
            value = inspect_submissions(
                manifest_path=args.manifest,
                env_path=args.env,
                public_status_path=args.status,
                inspection_path=args.output,
            )
        print(json.dumps(value, indent=2, sort_keys=True))
        return 0
    except (
        DevelopmentSecSubmissionsError,
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

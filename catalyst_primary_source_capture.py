"""Capture frozen primary-source URL leads without classifying catalysts."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import io
import ipaddress
import json
import os
import socket
import sys
import time
from collections import Counter
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse

import requests

from catalyst_source_leads import PRIMARY_LEAD_CATEGORIES
from historical_store import HistoricalDayStore
from learning_data import (
    LearningDataError,
    freeze_dataset_contract,
    load_frozen_dataset_contract,
)


PROJECT_ROOT = Path(__file__).resolve().parent
DATASET_ID = "dataset-catalyst-primary-source-capture-2026-07-19-expansion-v1"
SOURCE_DATASET_ID = "dataset-catalyst-source-leads-2026-07-19-expansion-v1"
SOURCE_MANIFEST = (
    PROJECT_ROOT
    / "historical_batches"
    / "catalyst_source_leads"
    / "manifests"
    / (
        "dataset-catalyst-source-leads-2026-07-19-expansion-v1-"
        "14107372085ed528dd001e74814024a9093d308ac4ba7bbf50fab7617cde495a.json"
    )
)
SOURCE_RESULT = (
    PROJECT_ROOT / "research_results" / "2026-07-19-catalyst-source-leads.json"
)
DEFAULT_OUTPUT_ROOT = (
    PROJECT_ROOT / "historical_batches" / "catalyst_primary_source_capture" / "manifests"
)
DEFAULT_PUBLIC_STATUS = (
    PROJECT_ROOT
    / "historical_batches"
    / "catalyst_primary_source_capture"
    / "collection-status.json"
)
DEFAULT_PUBLIC_RESULT = (
    PROJECT_ROOT / "research_results" / "2026-07-19-catalyst-primary-source-capture.json"
)
EXPECTED_URLS = 134
MAX_REDIRECTS = 5
MAX_RESPONSE_BYTES = 10 * 1024 * 1024
REQUEST_TIMEOUT_SECONDS = 20.0
MINIMUM_INTERVAL_SECONDS = 0.25
USER_AGENT = "RobinhoodCodexResearch/1.0 source-fidelity-capture"


class PrimarySourceCaptureError(RuntimeError):
    """The frozen source-capture contract cannot be honored."""


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
        raise PrimarySourceCaptureError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise PrimarySourceCaptureError(f"{path} must contain an object")
    return value


def _read_gzip(path: Path) -> dict[str, Any]:
    try:
        with gzip.open(path, "rt", encoding="utf-8") as source:
            value = json.load(source)
    except (OSError, json.JSONDecodeError) as exc:
        raise PrimarySourceCaptureError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise PrimarySourceCaptureError(f"{path} must contain an object")
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


def _write_bytes(path: Path, value: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_bytes(value)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _timestamp_now() -> str:
    return datetime.now(UTC).isoformat()


def _repo_path(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(PROJECT_ROOT))
    except ValueError as exc:
        raise PrimarySourceCaptureError(
            f"public path must be repository-relative: {path}"
        ) from exc


def _source_private_path(store_root: Path) -> Path:
    return (
        store_root
        / "_derived"
        / "catalyst_source_leads"
        / SOURCE_DATASET_ID
        / "source-lead-index.json.gz"
    )


def _private_root(store_root: Path) -> Path:
    return store_root / "_derived" / "catalyst_primary_source_capture" / DATASET_ID


def _selection_path(store_root: Path) -> Path:
    return _private_root(store_root) / "capture-selection.json.gz"


def _index_path(store_root: Path) -> Path:
    return _private_root(store_root) / "capture-index.json"


def _response_path(store_root: Path, url_sha256: str) -> Path:
    return _private_root(store_root) / "responses" / f"{url_sha256}.bin"


def _is_public_address(raw: str) -> bool:
    try:
        address = ipaddress.ip_address(raw)
    except ValueError:
        return False
    return address.is_global


def _validate_network_target(url: str) -> list[str]:
    parsed = urlparse(url)
    if parsed.scheme.lower() not in {"http", "https"}:
        raise PrimarySourceCaptureError("capture URL must be HTTP or HTTPS")
    host = (parsed.hostname or "").lower()
    if not host or host in {"localhost", "localhost.localdomain"}:
        raise PrimarySourceCaptureError("capture URL lacks a public hostname")
    try:
        resolved = sorted(
            {
                item[4][0]
                for item in socket.getaddrinfo(
                    host,
                    parsed.port or (443 if parsed.scheme.lower() == "https" else 80),
                    type=socket.SOCK_STREAM,
                )
            }
        )
    except OSError as exc:
        raise PrimarySourceCaptureError(f"DNS resolution failed for {host}") from exc
    if not resolved or not all(_is_public_address(address) for address in resolved):
        raise PrimarySourceCaptureError(f"non-public address blocked for {host}")
    return resolved


def build_capture_selection(source: Mapping[str, Any]) -> dict[str, Any]:
    by_url: dict[str, dict[str, Any]] = {}
    for article_key, record in source.get("article_records", {}).items():
        for lead in record.get("leads", []):
            if lead.get("category") not in PRIMARY_LEAD_CATEGORIES:
                continue
            url = str(lead.get("url") or "")
            current = by_url.setdefault(
                url,
                {
                    "url": url,
                    "url_sha256": hashlib.sha256(url.encode()).hexdigest(),
                    "category": str(lead["category"]),
                    "article_keys": [],
                },
            )
            if current["category"] != lead["category"]:
                raise PrimarySourceCaptureError("URL has conflicting routing categories")
            current["article_keys"].append(str(article_key))
    records = sorted(by_url.values(), key=lambda row: row["url_sha256"])
    for record in records:
        record["article_keys"] = sorted(set(record["article_keys"]))
    if len(records) != EXPECTED_URLS:
        raise PrimarySourceCaptureError("primary URL selection is not exactly 134")
    return {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "source_dataset_id": SOURCE_DATASET_ID,
        "selected_url_count": len(records),
        "records": records,
        "primary_catalyst_verified": False,
        "target_outcomes_observed_or_derived": False,
    }


def freeze_inputs(*, env_path: Path, output_root: Path) -> tuple[Path, dict[str, Any]]:
    store = HistoricalDayStore.from_env(env_path)
    source_manifest = load_frozen_dataset_contract(SOURCE_MANIFEST)
    source_result = _read_json(SOURCE_RESULT)
    if source_manifest.get("dataset_id") != SOURCE_DATASET_ID:
        raise PrimarySourceCaptureError("unexpected source-lead dataset")
    if source_result.get("status") != "READY" or source_result.get("inspected") is not True:
        raise PrimarySourceCaptureError("source-lead dataset is not inspected READY")
    source_path = _source_private_path(store.root)
    selection = build_capture_selection(_read_gzip(source_path))
    selection_path = _selection_path(store.root)
    _write_gzip(selection_path, selection)
    contract = {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "registered_at": _timestamp_now(),
        "requested_dates": list(source_manifest["requested_dates"]),
        "dataset_payload": {
            "lane": "development",
            "claim_scope": "DEVELOPMENT_ONLY",
            "status": "COLLECTING",
            "evidence_paths": [
                _repo_path(SOURCE_MANIFEST),
                _repo_path(SOURCE_RESULT),
                "CATALYST_SOURCE_LEADS.md",
                "CATALYST_EVIDENCE_ACQUISITION.md",
            ],
            "inspected": False,
        },
        "selection_contract": {
            "source_dataset_id": SOURCE_DATASET_ID,
            "source_manifest_sha256": source_manifest["manifest_sha256"],
            "source_result_sha256": _sha256_file(SOURCE_RESULT),
            "source_private_sha256": _sha256_file(source_path),
            "capture_selection_sha256": _sha256_file(selection_path),
            "selected_url_count": EXPECTED_URLS,
            "categories": sorted(PRIMARY_LEAD_CATEGORIES),
            "symbols_articles_urls_and_responses_public": False,
        },
        "collection_contract": {
            "collector_sha256": _sha256_file(Path(__file__)),
            "user_agent": USER_AGENT,
            "timeout_seconds": REQUEST_TIMEOUT_SECONDS,
            "minimum_interval_seconds": MINIMUM_INTERVAL_SECONDS,
            "maximum_redirects": MAX_REDIRECTS,
            "maximum_response_bytes": MAX_RESPONSE_BYTES,
            "private_or_non_global_addresses_blocked": True,
            "every_redirect_revalidated": True,
            "checkpoint_unit": "URL",
            "terminal_errors_retained": True,
            "provider_substitution_allowed": False,
            "source_ownership_verified_by_capture": False,
            "causal_availability_verified_by_capture": False,
            "primary_catalyst_classification_allowed": False,
            "target_outcomes_observed_or_derived": False,
        },
    }
    return freeze_dataset_contract(contract, output_root)


def _verify_contract(
    manifest_path: Path, store: HistoricalDayStore
) -> tuple[dict[str, Any], dict[str, Any]]:
    manifest = load_frozen_dataset_contract(manifest_path)
    if manifest.get("dataset_id") != DATASET_ID:
        raise PrimarySourceCaptureError("unexpected capture dataset")
    selection_path = _selection_path(store.root)
    selection = _read_gzip(selection_path)
    contract = manifest["selection_contract"]
    if _sha256_file(selection_path) != contract.get("capture_selection_sha256"):
        raise PrimarySourceCaptureError("capture selection changed")
    if _sha256_file(_source_private_path(store.root)) != contract.get(
        "source_private_sha256"
    ):
        raise PrimarySourceCaptureError("source lead index changed")
    if _sha256_file(Path(__file__)) != manifest["collection_contract"].get(
        "collector_sha256"
    ):
        raise PrimarySourceCaptureError("collector differs from frozen contract")
    return manifest, selection


def _read_response_bytes(response: requests.Response) -> bytes:
    content_length = response.headers.get("Content-Length")
    if content_length and int(content_length) > MAX_RESPONSE_BYTES:
        raise PrimarySourceCaptureError("response exceeds frozen byte limit")
    buffer = bytearray()
    for chunk in response.iter_content(chunk_size=64 * 1024):
        buffer.extend(chunk)
        if len(buffer) > MAX_RESPONSE_BYTES:
            raise PrimarySourceCaptureError("response exceeded frozen byte limit")
    return bytes(buffer)


def _request_once(session: requests.Session, initial_url: str) -> dict[str, Any]:
    current = initial_url
    history: list[dict[str, Any]] = []
    for redirect_count in range(MAX_REDIRECTS + 1):
        resolved = _validate_network_target(current)
        try:
            response = session.get(
                current,
                headers={"User-Agent": USER_AGENT, "Accept": "*/*"},
                timeout=REQUEST_TIMEOUT_SECONDS,
                allow_redirects=False,
                stream=True,
            )
        except requests.RequestException as exc:
            raise PrimarySourceCaptureError("source request transport failure") from exc
        status = int(response.status_code)
        location = response.headers.get("Location")
        history.append(
            {
                "url": current,
                "status": status,
                "resolved_addresses": resolved,
                "location": location,
            }
        )
        if status in {301, 302, 303, 307, 308} and location:
            response.close()
            if redirect_count >= MAX_REDIRECTS:
                raise PrimarySourceCaptureError("source redirect limit exceeded")
            current = urljoin(current, location)
            continue
        body = _read_response_bytes(response)
        headers = {
            key.lower(): value
            for key, value in response.headers.items()
            if key.lower()
            in {"content-type", "content-length", "etag", "last-modified", "date"}
        }
        response.close()
        return {
            "status": "RESPONSE_CAPTURED",
            "final_url": current,
            "http_status": status,
            "headers": headers,
            "redirect_history": history,
            "body": body,
        }
    raise AssertionError("redirect loop must return or raise")


def _capture_with_retry(session: requests.Session, url: str) -> dict[str, Any]:
    last_error = ""
    for attempt in range(3):
        try:
            result = _request_once(session, url)
            if result["http_status"] == 429 or result["http_status"] >= 500:
                last_error = f"HTTP {result['http_status']}"
                if attempt < 2:
                    time.sleep(2.0**attempt)
                    continue
            return result
        except PrimarySourceCaptureError as exc:
            last_error = str(exc)
            if attempt < 2 and "transport" in last_error:
                time.sleep(2.0**attempt)
                continue
            break
    return {"status": "CAPTURE_ERROR", "error": last_error}


def _public_summary(
    manifest: Mapping[str, Any], index: Mapping[str, Any], private_root: Path
) -> dict[str, Any]:
    records = index.get("records", {})
    statuses = Counter(str(row.get("status")) for row in records.values())
    http_statuses = Counter(
        str(row.get("http_status"))
        for row in records.values()
        if row.get("http_status") is not None
    )
    content_types = Counter(
        str(row.get("headers", {}).get("content-type") or "<missing>").split(";")[0]
        for row in records.values()
        if row.get("status") == "RESPONSE_CAPTURED"
    )
    response_bytes = sum(int(row.get("body_bytes") or 0) for row in records.values())
    return {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "status": index["status"],
        "selected_urls": EXPECTED_URLS,
        "terminal_urls": len(records),
        "capture_status_counts": dict(sorted(statuses.items())),
        "http_status_counts": dict(sorted(http_statuses.items())),
        "content_type_counts": dict(sorted(content_types.items())),
        "response_bytes": response_bytes,
        "private_index_sha256": (
            _sha256_file(_index_path(private_root.parents[2]))
            if _index_path(private_root.parents[2]).exists()
            else None
        ),
        "symbols_articles_urls_and_responses_public": False,
        "source_ownership_verified": False,
        "causal_availability_verified": False,
        "primary_catalyst_verified": False,
        "target_outcomes_observed_or_derived": False,
    }


def collect(
    *, manifest_path: Path, env_path: Path, public_status_path: Path
) -> dict[str, Any]:
    store = HistoricalDayStore.from_env(env_path)
    manifest, selection = _verify_contract(manifest_path, store)
    index_path = _index_path(store.root)
    index = (
        _read_json(index_path)
        if index_path.exists()
        else {
            "schema_version": 1,
            "dataset_id": DATASET_ID,
            "manifest_sha256": manifest["manifest_sha256"],
            "status": "COLLECTING",
            "records": {},
            "target_outcomes_observed_or_derived": False,
            "primary_catalyst_verified": False,
        }
    )
    if index.get("manifest_sha256") != manifest["manifest_sha256"]:
        raise PrimarySourceCaptureError("capture checkpoint belongs to another manifest")
    session = requests.Session()
    last_started = 0.0
    try:
        for selected in selection["records"]:
            url_hash = str(selected["url_sha256"])
            if url_hash in index["records"]:
                continue
            remaining = MINIMUM_INTERVAL_SECONDS - (time.monotonic() - last_started)
            if remaining > 0:
                time.sleep(remaining)
            last_started = time.monotonic()
            result = _capture_with_retry(session, str(selected["url"]))
            body = result.pop("body", None)
            if body is not None:
                path = _response_path(store.root, url_hash)
                _write_bytes(path, body)
                result["body_sha256"] = _sha256_file(path)
                result["body_bytes"] = len(body)
            result.update(
                {
                    "url_sha256": url_hash,
                    "category": selected["category"],
                    "captured_at": _timestamp_now(),
                }
            )
            index["records"][url_hash] = result
            _write_json(index_path, index)
            _write_json(
                public_status_path,
                _public_summary(manifest, index, _private_root(store.root)),
            )
    finally:
        session.close()
    index["status"] = "COLLECTION_COMPLETE"
    _write_json(index_path, index)
    public = _public_summary(manifest, index, _private_root(store.root))
    _write_json(public_status_path, public)
    return public


def inspect(
    *, manifest_path: Path, env_path: Path, public_result_path: Path
) -> dict[str, Any]:
    store = HistoricalDayStore.from_env(env_path)
    manifest, selection = _verify_contract(manifest_path, store)
    index_path = _index_path(store.root)
    index = _read_json(index_path)
    if not (
        index.get("manifest_sha256") == manifest["manifest_sha256"]
        and index.get("status") == "COLLECTION_COMPLETE"
        and len(index.get("records", {})) == len(selection["records"]) == EXPECTED_URLS
        and index.get("target_outcomes_observed_or_derived") is False
        and index.get("primary_catalyst_verified") is False
    ):
        raise PrimarySourceCaptureError("source capture is incomplete")
    for url_hash, record in index["records"].items():
        if record.get("status") == "RESPONSE_CAPTURED":
            path = _response_path(store.root, url_hash)
            if not path.exists() or _sha256_file(path) != record.get("body_sha256"):
                raise PrimarySourceCaptureError("captured response hash mismatch")
    summary = _public_summary(manifest, index, _private_root(store.root))
    result = {
        **summary,
        "status": "READY",
        "inspected": True,
        "claim_scope": "DEVELOPMENT_ONLY",
        "private_index_sha256": _sha256_file(index_path),
        "findings": {
            "terminal_capture_disposition_complete": True,
            "response_hashes_verified": True,
            "source_ownership_verified": False,
            "issuer_binding_verified": False,
            "causal_availability_verified": False,
            "primary_catalyst_verified": False,
            "production_rule_change_earned": False,
        },
        "next_required_stage": (
            "parse captured sources under a frozen contract and verify ownership, "
            "issuer binding, historical publication time, direction, and conflicts"
        ),
        "claim_boundary": (
            "Bounded response capture on frozen source-routing leads; not proof of "
            "source identity, causality, event direction, alpha, or a strategy change."
        ),
    }
    _write_json(public_result_path, result)
    return result


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("freeze", "collect", "inspect"))
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
            raise PrimarySourceCaptureError("--manifest is required")
        elif args.command == "collect":
            output = collect(
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
    except (PrimarySourceCaptureError, LearningDataError, OSError, ValueError) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}), file=sys.stderr)
        return 2
    print(json.dumps(output, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

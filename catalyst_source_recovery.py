"""Recover frozen SEC 403 source leads through accession-bound SEC endpoints."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import io
import ipaddress
import json
import os
import re
import shutil
import socket
import sys
import time
from collections import Counter, defaultdict
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urljoin, urlparse

import requests

from historical_store import HistoricalDayStore
from learning_data import (
    LearningDataError,
    freeze_dataset_contract,
    load_frozen_dataset_contract,
)


PROJECT_ROOT = Path(__file__).resolve().parent
DATASET_ID = "dataset-catalyst-source-recovery-sec-2026-07-19-expansion-v1"
SOURCE_CAPTURE_DATASET_ID = (
    "dataset-catalyst-primary-source-capture-2026-07-19-expansion-v1"
)
SOURCE_CAPTURE_MANIFEST = (
    PROJECT_ROOT
    / "historical_batches"
    / "catalyst_primary_source_capture"
    / "manifests"
    / (
        "dataset-catalyst-primary-source-capture-2026-07-19-expansion-v1-"
        "409a807f1cae100ced8b9018aeb99612a2efc9d5f496e5981c26654762b41ef4.json"
    )
)
SOURCE_CAPTURE_RESULT = (
    PROJECT_ROOT
    / "research_results"
    / "2026-07-19-catalyst-primary-source-capture.json"
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
SOURCE_SEMANTICS_RESULT = (
    PROJECT_ROOT / "research_results" / "2026-07-19-catalyst-source-semantics.json"
)
DEFAULT_OUTPUT_ROOT = (
    PROJECT_ROOT / "historical_batches" / "catalyst_source_recovery" / "manifests"
)
DEFAULT_PUBLIC_STATUS = (
    PROJECT_ROOT
    / "historical_batches"
    / "catalyst_source_recovery"
    / "collection-status.json"
)
DEFAULT_PUBLIC_RESULT = (
    PROJECT_ROOT / "research_results" / "2026-07-19-catalyst-source-recovery-sec.json"
)

EXPECTED_SEC_URLS = 26
EXPECTED_ACCESSION_URLS = 24
EXPECTED_PAIR_SOURCE_JOINS = 37
EXPECTED_PAIRS = 31
MINIMUM_INTERVAL_SECONDS = 0.5
REQUEST_TIMEOUT_SECONDS = 20.0
MAX_REDIRECTS = 3
MAX_RESPONSE_BYTES = 20 * 1024 * 1024
USER_AGENT = "Ensomniac RobinhoodCodexResearch ryan@ensomniac.com"
ACCESSION_PATTERN = re.compile(r"(?<!\d)(\d{18})(?!\d)")
ARCHIVE_PATTERN = re.compile(
    r"/Archives/edgar/data/0*(\d+)/(\d{18})/(.*)", re.IGNORECASE
)
INDEX_PATTERN = re.compile(r"(?:-index\.html?|index\.html?)$", re.IGNORECASE)


class CatalystSourceRecoveryError(RuntimeError):
    """The frozen source-recovery contract cannot be honored."""


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
        raise CatalystSourceRecoveryError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise CatalystSourceRecoveryError(f"{path} must contain an object")
    return value


def _read_gzip(path: Path) -> dict[str, Any]:
    try:
        with gzip.open(path, "rt", encoding="utf-8") as source:
            value = json.load(source)
    except (OSError, json.JSONDecodeError) as exc:
        raise CatalystSourceRecoveryError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise CatalystSourceRecoveryError(f"{path} must contain an object")
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


def _repo_path(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(PROJECT_ROOT))
    except ValueError as exc:
        raise CatalystSourceRecoveryError(
            f"public path must be repository-relative: {path}"
        ) from exc


def _timestamp_now() -> str:
    return datetime.now(UTC).isoformat()


def _capture_root(store_root: Path) -> Path:
    return (
        store_root
        / "_derived"
        / "catalyst_primary_source_capture"
        / SOURCE_CAPTURE_DATASET_ID
    )


def _private_root(store_root: Path) -> Path:
    return store_root / "_derived" / "catalyst_source_recovery" / DATASET_ID


def _selection_path(store_root: Path) -> Path:
    return _private_root(store_root) / "selection.json.gz"


def _index_path(store_root: Path) -> Path:
    return _private_root(store_root) / "capture-index.json"


def _response_path(store_root: Path, source_sha256: str) -> Path:
    return _private_root(store_root) / "responses" / f"{source_sha256}.bin"


def _discovery_path(store_root: Path) -> Path:
    return (
        store_root
        / "_derived"
        / "catalyst_news_enrichment"
        / "dataset-catalyst-news-enrichment-2026-07-19-expansion-v1"
        / "discovery-index.json.gz"
    )


def _identity_path(store_root: Path) -> Path:
    return (
        store_root
        / "_derived"
        / "selected_candidate_fidelity"
        / "dataset-selected-candidate-fidelity-2026-07-19-expansion-v1"
        / "selection-and-cik-map.json.gz"
    )


def _is_sec_host(host: str) -> bool:
    value = host.lower().rstrip(".")
    return value == "sec.gov" or value.endswith(".sec.gov")


def accession_from_url(url: str) -> str | None:
    values = sorted(set(ACCESSION_PATTERN.findall(url)))
    if not values:
        return None
    if len(values) != 1:
        raise CatalystSourceRecoveryError("SEC URL has conflicting accessions")
    return values[0]


def accession_recovery_url(url: str) -> dict[str, str | None]:
    parsed = urlparse(url)
    if parsed.scheme != "https" or not _is_sec_host(parsed.hostname or ""):
        raise CatalystSourceRecoveryError("recovery source is not HTTPS SEC")
    accession = accession_from_url(url)
    if accession is None:
        return {"accession": None, "filing_cik": None, "recovery_url": None}
    path = parse_qs(parsed.query).get("doc", [parsed.path])[0]
    matches = list(ARCHIVE_PATTERN.finditer(path))
    if not matches:
        raise CatalystSourceRecoveryError("accession URL lacks an EDGAR archive path")
    match = matches[-1]
    filing_cik = str(int(match.group(1)))
    if match.group(2) != accession:
        raise CatalystSourceRecoveryError("archive path accession differs")
    document = match.group(3).lstrip("/")
    if not document:
        raise CatalystSourceRecoveryError("accession URL lacks a document")
    if INDEX_PATTERN.search(document):
        dashed = f"{accession[:10]}-{accession[10:12]}-{accession[12:]}"
        document = f"{dashed}.txt"
    recovery_url = (
        f"https://www.sec.gov/Archives/edgar/data/{filing_cik}/{accession}/{document}"
    )
    return {
        "accession": accession,
        "filing_cik": filing_cik,
        "recovery_url": recovery_url,
    }


def build_selection(
    *,
    capture_selection: Mapping[str, Any],
    capture_index: Mapping[str, Any],
    discovery: Mapping[str, Any],
    identities: Mapping[str, Any],
) -> dict[str, Any]:
    pairs_by_article: dict[str, list[tuple[str, str]]] = defaultdict(list)
    for pair in discovery.get("pair_records", []):
        key = (str(pair["date"]), str(pair["symbol"]))
        for article_key in pair.get("article_keys", []):
            pairs_by_article[str(article_key)].append(key)
    identity_by_pair = {
        (str(row["date"]), str(row["symbol"])): row
        for row in identities.get("pairs", [])
    }
    records: list[dict[str, Any]] = []
    unique_pairs: set[tuple[str, str]] = set()
    joins = 0
    for source in capture_selection.get("records", []):
        url = str(source["url"])
        host = urlparse(url).hostname or ""
        original = capture_index.get("records", {}).get(source["url_sha256"], {})
        if not _is_sec_host(host) or original.get("http_status") != 403:
            continue
        pairs = sorted(
            {
                pair
                for article_key in source.get("article_keys", [])
                for pair in pairs_by_article.get(str(article_key), [])
            }
        )
        bound_pairs = []
        for pair in pairs:
            identity = identity_by_pair.get(pair)
            if identity is None:
                raise CatalystSourceRecoveryError(
                    "SEC source pair lacks frozen identity"
                )
            bound_pairs.append(
                {
                    "date": pair[0],
                    "symbol": pair[1],
                    "instrument_id": identity["instrument_id"],
                    "primary_exchange": identity["primary_exchange"],
                    "issuer_name": identity["issuer_name"],
                    "cik": identity["cik"],
                }
            )
            unique_pairs.add(pair)
        if not bound_pairs:
            raise CatalystSourceRecoveryError("SEC source has no selected-pair join")
        normalized = accession_recovery_url(url)
        records.append(
            {
                "source_sha256": str(source["url_sha256"]),
                "source_url": url,
                "article_keys": sorted(set(source.get("article_keys", []))),
                "pairs": bound_pairs,
                **normalized,
            }
        )
        joins += len(bound_pairs)
    records.sort(key=lambda row: row["source_sha256"])
    accession_count = sum(row["accession"] is not None for row in records)
    if (
        len(records) != EXPECTED_SEC_URLS
        or accession_count != EXPECTED_ACCESSION_URLS
        or joins != EXPECTED_PAIR_SOURCE_JOINS
        or len(unique_pairs) != EXPECTED_PAIRS
    ):
        raise CatalystSourceRecoveryError("SEC recovery selection counts differ")
    return {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "status": "FROZEN",
        "records": records,
        "counts": {
            "sec_403_urls": len(records),
            "accession_bound_urls": accession_count,
            "non_accession_urls": len(records) - accession_count,
            "pair_source_joins": joins,
            "pairs": len(unique_pairs),
        },
        "symbols_dates_urls_articles_and_rows_public": False,
        "target_outcomes_observed_or_derived": False,
    }


def freeze_inputs(*, env_path: Path, output_root: Path) -> tuple[Path, dict[str, Any]]:
    store = HistoricalDayStore.from_env(env_path)
    if shutil.disk_usage(store.root).free < store.min_free_bytes:
        raise CatalystSourceRecoveryError("historical store disk reserve is not ready")
    capture_manifest = load_frozen_dataset_contract(SOURCE_CAPTURE_MANIFEST)
    identity_manifest = load_frozen_dataset_contract(SOURCE_IDENTITY_MANIFEST)
    capture_result = _read_json(SOURCE_CAPTURE_RESULT)
    semantics_result = _read_json(SOURCE_SEMANTICS_RESULT)
    if capture_result.get("status") != "READY" or not capture_result.get("inspected"):
        raise CatalystSourceRecoveryError("source capture is not inspected READY")
    if not (
        semantics_result.get("status") == "READY"
        and semantics_result.get("next_phase") == "SOURCE_RECOVERY"
        and semantics_result.get("target_outcomes_observed_or_derived") is False
    ):
        raise CatalystSourceRecoveryError("source semantics did not earn recovery")
    capture_root = _capture_root(store.root)
    inputs = {
        "capture_selection": capture_root / "capture-selection.json.gz",
        "capture_index": capture_root / "capture-index.json",
        "discovery": _discovery_path(store.root),
        "identities": _identity_path(store.root),
    }
    selection = build_selection(
        capture_selection=_read_gzip(inputs["capture_selection"]),
        capture_index=_read_json(inputs["capture_index"]),
        discovery=_read_gzip(inputs["discovery"]),
        identities=_read_gzip(inputs["identities"]),
    )
    selection_path = _selection_path(store.root)
    _write_gzip(selection_path, selection)
    contract = {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "registered_at": _timestamp_now(),
        "requested_dates": list(capture_manifest["requested_dates"]),
        "dataset_payload": {
            "lane": "development",
            "claim_scope": "DEVELOPMENT_ONLY",
            "status": "COLLECTING",
            "evidence_paths": [
                _repo_path(SOURCE_CAPTURE_MANIFEST),
                _repo_path(SOURCE_CAPTURE_RESULT),
                _repo_path(SOURCE_IDENTITY_MANIFEST),
                _repo_path(SOURCE_SEMANTICS_RESULT),
                "CATALYST_SOURCE_RECOVERY.md",
                "PRODUCTION_STRATEGY_VALIDATION.md",
            ],
            "inspected": False,
        },
        "selection_contract": {
            "source_capture_manifest_sha256": capture_manifest["manifest_sha256"],
            "source_capture_result_sha256": _sha256_file(SOURCE_CAPTURE_RESULT),
            "source_identity_manifest_sha256": identity_manifest["manifest_sha256"],
            "source_semantics_result_sha256": _sha256_file(SOURCE_SEMANTICS_RESULT),
            "private_input_sha256": {
                name: _sha256_file(path) for name, path in sorted(inputs.items())
            },
            "private_selection_sha256": _sha256_file(selection_path),
            **selection["counts"],
            "symbols_dates_urls_articles_and_rows_public": False,
        },
        "collection_contract": {
            "collector_sha256": _sha256_file(Path(__file__)),
            "sec_operated_https_only": True,
            "accession_bound_only": True,
            "non_accession_rows_requested": False,
            "user_agent": USER_AGENT,
            "accept_encoding": "gzip, deflate",
            "minimum_interval_seconds": MINIMUM_INTERVAL_SECONDS,
            "timeout_seconds": REQUEST_TIMEOUT_SECONDS,
            "maximum_redirects": MAX_REDIRECTS,
            "maximum_response_bytes": MAX_RESPONSE_BYTES,
            "maximum_attempts": 3,
            "terminal_errors_retained": True,
            "checkpoint_unit": "source URL",
            "provider_substitution_allowed": False,
            "secondary_source_substitution_allowed": False,
            "source_semantics_classification_allowed": False,
            "target_outcomes_observed_or_derived": False,
        },
    }
    return freeze_dataset_contract(contract, output_root)


def _verify_contract(
    manifest_path: Path, store: HistoricalDayStore
) -> tuple[dict[str, Any], dict[str, Any]]:
    manifest = load_frozen_dataset_contract(manifest_path)
    if manifest.get("dataset_id") != DATASET_ID:
        raise CatalystSourceRecoveryError("unexpected source-recovery dataset")
    selection_path = _selection_path(store.root)
    selection = _read_gzip(selection_path)
    if _sha256_file(selection_path) != manifest["selection_contract"].get(
        "private_selection_sha256"
    ):
        raise CatalystSourceRecoveryError("source-recovery selection changed")
    if _sha256_file(Path(__file__)) != manifest["collection_contract"].get(
        "collector_sha256"
    ):
        raise CatalystSourceRecoveryError("collector differs from frozen contract")
    return manifest, selection


def _is_public_address(raw: str) -> bool:
    try:
        return ipaddress.ip_address(raw).is_global
    except ValueError:
        return False


def _validate_sec_target(url: str) -> list[str]:
    parsed = urlparse(url)
    if parsed.scheme != "https" or parsed.hostname != "www.sec.gov":
        raise CatalystSourceRecoveryError("request target is not canonical SEC HTTPS")
    if not ARCHIVE_PATTERN.match(parsed.path):
        raise CatalystSourceRecoveryError("request target is not accession-bound")
    try:
        resolved = sorted(
            {
                item[4][0]
                for item in socket.getaddrinfo(
                    parsed.hostname, 443, type=socket.SOCK_STREAM
                )
            }
        )
    except OSError as exc:
        raise CatalystSourceRecoveryError("SEC DNS resolution failed") from exc
    if not resolved or not all(_is_public_address(address) for address in resolved):
        raise CatalystSourceRecoveryError("SEC target resolved to non-public address")
    return resolved


def _read_response_bytes(response: requests.Response) -> bytes:
    content_length = response.headers.get("Content-Length")
    if content_length and int(content_length) > MAX_RESPONSE_BYTES:
        raise CatalystSourceRecoveryError("response exceeds frozen byte limit")
    buffer = bytearray()
    for chunk in response.iter_content(chunk_size=64 * 1024):
        buffer.extend(chunk)
        if len(buffer) > MAX_RESPONSE_BYTES:
            raise CatalystSourceRecoveryError("response exceeded frozen byte limit")
    return bytes(buffer)


def _request_once(session: requests.Session, initial_url: str) -> dict[str, Any]:
    current = initial_url
    history: list[dict[str, Any]] = []
    for redirect_count in range(MAX_REDIRECTS + 1):
        resolved = _validate_sec_target(current)
        try:
            response = session.get(
                current,
                headers={
                    "User-Agent": USER_AGENT,
                    "Accept": "*/*",
                    "Accept-Encoding": "gzip, deflate",
                },
                timeout=REQUEST_TIMEOUT_SECONDS,
                allow_redirects=False,
                stream=True,
            )
        except requests.RequestException as exc:
            raise CatalystSourceRecoveryError("SEC request transport failure") from exc
        status = int(response.status_code)
        location = response.headers.get("Location")
        history.append(
            {
                "url_sha256": hashlib.sha256(current.encode()).hexdigest(),
                "status": status,
                "resolved_addresses": resolved,
                "location_sha256": (
                    hashlib.sha256(location.encode()).hexdigest() if location else None
                ),
            }
        )
        if status in {301, 302, 303, 307, 308} and location:
            response.close()
            if redirect_count >= MAX_REDIRECTS:
                raise CatalystSourceRecoveryError("SEC redirect limit exceeded")
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
            "final_url_sha256": hashlib.sha256(current.encode()).hexdigest(),
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
        except CatalystSourceRecoveryError as exc:
            last_error = str(exc)
            if attempt < 2 and "transport" in last_error:
                time.sleep(2.0**attempt)
                continue
            break
    return {"status": "CAPTURE_ERROR", "error": last_error}


def _public_summary(
    manifest: Mapping[str, Any], selection: Mapping[str, Any], index: Mapping[str, Any]
) -> dict[str, Any]:
    records = index.get("records", {})
    statuses = Counter(str(row.get("status")) for row in records.values())
    http_statuses = Counter(
        str(row.get("http_status"))
        for row in records.values()
        if row.get("http_status") is not None
    )
    return {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "status": index["status"],
        "selection_counts": selection["counts"],
        "terminal_sources": len(records),
        "capture_status_counts": dict(sorted(statuses.items())),
        "http_status_counts": dict(sorted(http_statuses.items())),
        "response_bytes": sum(
            int(row.get("body_bytes") or 0) for row in records.values()
        ),
        "symbols_dates_urls_articles_responses_and_rows_public": False,
        "source_semantics_classified": False,
        "target_outcomes_observed_or_derived": False,
    }


def collect(
    *, manifest_path: Path, env_path: Path, public_status_path: Path
) -> dict[str, Any]:
    store = HistoricalDayStore.from_env(env_path)
    manifest, selection = _verify_contract(manifest_path, store)
    if shutil.disk_usage(store.root).free < store.min_free_bytes:
        raise CatalystSourceRecoveryError("historical store disk reserve is not ready")
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
        }
    )
    if index.get("manifest_sha256") != manifest["manifest_sha256"]:
        raise CatalystSourceRecoveryError("checkpoint belongs to another manifest")
    session = requests.Session()
    last_started = 0.0
    try:
        for selected in selection["records"]:
            source_hash = str(selected["source_sha256"])
            if source_hash in index["records"]:
                continue
            if selected["accession"] is None:
                result: dict[str, Any] = {"status": "NO_ACCESSION"}
            else:
                remaining = MINIMUM_INTERVAL_SECONDS - (time.monotonic() - last_started)
                if remaining > 0:
                    time.sleep(remaining)
                last_started = time.monotonic()
                result = _capture_with_retry(session, str(selected["recovery_url"]))
                body = result.pop("body", None)
                if body is not None:
                    path = _response_path(store.root, source_hash)
                    _write_bytes(path, body)
                    result["body_sha256"] = _sha256_file(path)
                    result["body_bytes"] = len(body)
            result.update(
                {
                    "source_sha256": source_hash,
                    "accession_sha256": (
                        hashlib.sha256(str(selected["accession"]).encode()).hexdigest()
                        if selected["accession"]
                        else None
                    ),
                    "captured_at": _timestamp_now(),
                }
            )
            index["records"][source_hash] = result
            _write_json(index_path, index)
            _write_json(public_status_path, _public_summary(manifest, selection, index))
    finally:
        session.close()
    index["status"] = "COLLECTION_COMPLETE"
    _write_json(index_path, index)
    public = _public_summary(manifest, selection, index)
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
        and len(index.get("records", {})) == EXPECTED_SEC_URLS
        and index.get("target_outcomes_observed_or_derived") is False
    ):
        raise CatalystSourceRecoveryError("SEC recovery capture is incomplete")
    expected_hashes = {row["source_sha256"] for row in selection["records"]}
    if set(index["records"]) != expected_hashes:
        raise CatalystSourceRecoveryError("SEC recovery row set differs")
    response_hashes_verified = True
    for source_hash, record in index["records"].items():
        if record["status"] != "RESPONSE_CAPTURED":
            continue
        path = _response_path(store.root, source_hash)
        if not path.exists() or _sha256_file(path) != record.get("body_sha256"):
            response_hashes_verified = False
            break
    if not response_hashes_verified:
        raise CatalystSourceRecoveryError("recovered SEC response hash mismatch")
    summary = _public_summary(manifest, selection, index)
    result = {
        **summary,
        "status": "READY",
        "inspected": True,
        "claim_scope": "DEVELOPMENT_ONLY",
        "private_index_sha256": _sha256_file(index_path),
        "inspection": {
            "selection_rebuilt": True,
            "terminal_counts_rebuilt": True,
            "response_hashes_verified": True,
            "non_accession_rows_not_requested": True,
        },
        "production_rule_change_earned": False,
        "next_required_stage": (
            "Freeze outcome-blind SEC document extraction, exact document-CIK and "
            "target-CIK binding, acceptance timestamp, event semantics, and conflict review."
        ),
        "claim_boundary": (
            "Accession-bound SEC-operated response recovery only; not issuer binding, "
            "catalyst verification, alpha, outcomes, or strategy promotion."
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
            raise CatalystSourceRecoveryError("--manifest is required")
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
    except (
        CatalystSourceRecoveryError,
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

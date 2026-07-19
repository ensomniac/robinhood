"""Freeze and collect canonical issuer-controlled page and document chains."""

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
from collections import Counter
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse

import requests

from historical_store import HistoricalDayStore
from learning_data import (
    LearningDataError,
    freeze_dataset_contract,
    load_frozen_dataset_contract,
)


PROJECT_ROOT = Path(__file__).resolve().parent
DATASET_ID = "dataset-catalyst-issuer-chain-recovery-2026-07-19-expansion-v1"
SOURCE_DATASET_ID = "dataset-catalyst-transport-recovery-2026-07-19-expansion-v1"
SOURCE_MANIFEST = (
    PROJECT_ROOT
    / "historical_batches"
    / "catalyst_transport_recovery"
    / "manifests"
    / (
        "dataset-catalyst-transport-recovery-2026-07-19-expansion-v1-"
        "7837dd652471f90c578a829d2d59c677142ad15d75527e5af4171bb8b46261d9.json"
    )
)
SOURCE_RESULT = (
    PROJECT_ROOT / "research_results" / "2026-07-19-catalyst-transport-recovery.json"
)
DEFAULT_PLAN = (
    PROJECT_ROOT / "learning_runs" / "production_validation" / "issuer-chain-plan.json"
)
DEFAULT_OUTPUT_ROOT = (
    PROJECT_ROOT / "historical_batches" / "catalyst_issuer_chain_recovery" / "manifests"
)
DEFAULT_PUBLIC_STATUS = (
    PROJECT_ROOT
    / "historical_batches"
    / "catalyst_issuer_chain_recovery"
    / "collection-status.json"
)
DEFAULT_PUBLIC_RESULT = (
    PROJECT_ROOT / "research_results" / "2026-07-19-catalyst-issuer-chain-recovery.json"
)

EXPECTED_CHAINS = 6
EXPECTED_SOURCE_PREDECESSORS = 13
EXPECTED_CHAIN_URLS = 12
MINIMUM_INTERVAL_SECONDS = 0.5
REQUEST_TIMEOUT_SECONDS = 20.0
MAX_REDIRECTS = 5
MAX_RESPONSE_BYTES = 20 * 1024 * 1024
MAXIMUM_ATTEMPTS = 3
USER_AGENT = "Ensomniac RobinhoodCodexResearch ryan@ensomniac.com"
ALLOWED_SELECTION_BASES = {
    "CANONICAL_ISSUER_NEWSROOM_CHAIN",
    "CANONICAL_ISSUER_INVESTOR_CHAIN",
    "CANONICAL_ISSUER_FINANCIAL_RESULTS_CHAIN",
}
OUTCOME_KEY_PATTERN = re.compile(
    r"(?i)(?:outcome|return|net_r|profit|loss|target_hit|stop_hit|future_price)"
)


class IssuerChainRecoveryError(RuntimeError):
    """The frozen issuer-chain recovery contract cannot be honored."""


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
        raise IssuerChainRecoveryError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise IssuerChainRecoveryError(f"{path} must contain an object")
    return value


def _read_gzip(path: Path) -> dict[str, Any]:
    try:
        with gzip.open(path, "rt", encoding="utf-8") as source:
            value = json.load(source)
    except (OSError, json.JSONDecodeError) as exc:
        raise IssuerChainRecoveryError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise IssuerChainRecoveryError(f"{path} must contain an object")
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
        raise IssuerChainRecoveryError(
            f"public path must be repository-relative: {path}"
        ) from exc


def _timestamp_now() -> str:
    return datetime.now(UTC).isoformat()


def _source_root(store_root: Path) -> Path:
    return store_root / "_derived" / "catalyst_transport_recovery" / SOURCE_DATASET_ID


def _private_root(store_root: Path) -> Path:
    return store_root / "_derived" / "catalyst_issuer_chain_recovery" / DATASET_ID


def _selection_path(store_root: Path) -> Path:
    return _private_root(store_root) / "selection.json.gz"


def _plan_path(store_root: Path) -> Path:
    return _private_root(store_root) / "frozen-plan.json.gz"


def _index_path(store_root: Path) -> Path:
    return _private_root(store_root) / "capture-index.json"


def _response_path(store_root: Path, url_hash: str) -> Path:
    return _private_root(store_root) / "responses" / f"{url_hash}.bin"


def _contains_outcome_key(value: Any) -> bool:
    if isinstance(value, Mapping):
        return any(
            OUTCOME_KEY_PATTERN.search(str(key)) or _contains_outcome_key(item)
            for key, item in value.items()
            if key != "target_outcomes_observed_or_derived"
        )
    if isinstance(value, list):
        return any(_contains_outcome_key(item) for item in value)
    return False


def _host_within_domain(host: str, issuer_domain: str) -> bool:
    normalized_host = host.lower().rstrip(".")
    normalized_domain = issuer_domain.lower().rstrip(".")
    return normalized_host == normalized_domain or normalized_host.endswith(
        "." + normalized_domain
    )


def _validate_chain_url(url: str, issuer_domain: str) -> None:
    parsed = urlparse(url)
    if parsed.scheme != "https" or not _host_within_domain(
        parsed.hostname or "", issuer_domain
    ):
        raise IssuerChainRecoveryError(
            "chain URL must be HTTPS under its frozen issuer domain"
        )
    if parsed.username or parsed.password or parsed.fragment:
        raise IssuerChainRecoveryError("chain URL contains forbidden URL components")


def plan_template(source_selection: Mapping[str, Any]) -> dict[str, Any]:
    by_pair: dict[tuple[str, str], dict[str, Any]] = {}
    predecessor_hashes = {
        str(source["source_sha256"]) for source in source_selection.get("records", [])
    }
    for source in source_selection.get("records", []):
        for pair in source.get("pairs", []):
            key = (str(pair["date"]), str(pair["symbol"]))
            current = by_pair.setdefault(
                key,
                {
                    "chain_id": "issuer-chain-"
                    + hashlib.sha256("|".join(key).encode()).hexdigest()[:24],
                    "date": key[0],
                    "symbol": key[1],
                    "instrument_id": pair["instrument_id"],
                    "primary_exchange": pair["primary_exchange"],
                    "target_issuer_name": pair["issuer_name"],
                    "target_cik": str(pair["cik"]),
                    "source_predecessor_sha256": [],
                    "issuer_domain": "",
                    "chain_urls": [],
                    "selection_basis": "",
                    "notes": "",
                },
            )
            current["source_predecessor_sha256"].append(source["source_sha256"])
    records = sorted(by_pair.values(), key=lambda row: row["chain_id"])
    for record in records:
        record["source_predecessor_sha256"] = sorted(
            set(record["source_predecessor_sha256"])
        )
    if (
        len(records) != EXPECTED_CHAINS
        or len(predecessor_hashes) != EXPECTED_SOURCE_PREDECESSORS
    ):
        raise IssuerChainRecoveryError(
            "issuer-chain template must have six pairs and 13 sources"
        )
    return {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "review_completed": False,
        "target_outcomes_observed_or_derived": False,
        "records": records,
    }


def build_selection(
    source_selection: Mapping[str, Any], plan: Mapping[str, Any]
) -> dict[str, Any]:
    if not (
        plan.get("schema_version") == 1
        and plan.get("dataset_id") == DATASET_ID
        and plan.get("review_completed") is True
        and plan.get("target_outcomes_observed_or_derived") is False
        and not _contains_outcome_key(plan)
    ):
        raise IssuerChainRecoveryError("issuer-chain plan contract is invalid")
    expected_template = plan_template(source_selection)
    expected = {row["chain_id"]: row for row in expected_template["records"]}
    planned = plan.get("records")
    if not isinstance(planned, list) or len(planned) != EXPECTED_CHAINS:
        raise IssuerChainRecoveryError("issuer-chain plan must contain six chains")
    records = []
    predecessor_hashes: set[str] = set()
    url_records: dict[str, dict[str, Any]] = {}
    for value in planned:
        if not isinstance(value, Mapping):
            raise IssuerChainRecoveryError("issuer-chain record must be an object")
        row = dict(value)
        chain_id = str(row.get("chain_id") or "")
        template = expected.get(chain_id)
        if template is None:
            raise IssuerChainRecoveryError("issuer-chain plan has unknown chain")
        immutable_fields = {
            "date",
            "symbol",
            "instrument_id",
            "primary_exchange",
            "target_issuer_name",
            "target_cik",
            "source_predecessor_sha256",
        }
        if any(row.get(field) != template[field] for field in immutable_fields):
            raise IssuerChainRecoveryError(
                "issuer-chain identity or source set differs"
            )
        issuer_domain = str(row.get("issuer_domain") or "").lower().rstrip(".")
        urls = row.get("chain_urls")
        if (
            not issuer_domain
            or not isinstance(urls, list)
            or len(urls) != 2
            or len(set(urls)) != 2
        ):
            raise IssuerChainRecoveryError(
                "each issuer chain needs one canonical page and one document URL"
            )
        if row.get("selection_basis") not in ALLOWED_SELECTION_BASES:
            raise IssuerChainRecoveryError("issuer-chain selection basis is invalid")
        if not isinstance(row.get("notes"), str):
            raise IssuerChainRecoveryError("issuer-chain notes must be a string")
        for position, url in enumerate(urls):
            if not isinstance(url, str):
                raise IssuerChainRecoveryError("issuer-chain URL must be a string")
            _validate_chain_url(url, issuer_domain)
            url_hash = hashlib.sha256(url.encode()).hexdigest()
            current = url_records.setdefault(
                url_hash,
                {
                    "url_sha256": url_hash,
                    "url": url,
                    "issuer_domain": issuer_domain,
                    "chain_ids": [],
                    "positions": [],
                },
            )
            if current["url"] != url or current["issuer_domain"] != issuer_domain:
                raise IssuerChainRecoveryError("issuer-chain URL identity conflicts")
            current["chain_ids"].append(chain_id)
            current["positions"].append(position)
        predecessor_hashes.update(row["source_predecessor_sha256"])
        records.append(row)
    expected_predecessors = {
        str(row["source_sha256"]) for row in source_selection.get("records", [])
    }
    if predecessor_hashes != expected_predecessors:
        raise IssuerChainRecoveryError("issuer-chain predecessor coverage differs")
    urls = sorted(url_records.values(), key=lambda row: row["url_sha256"])
    for row in urls:
        row["chain_ids"] = sorted(set(row["chain_ids"]))
        row["positions"] = sorted(set(row["positions"]))
    records.sort(key=lambda row: row["chain_id"])
    if len(urls) != EXPECTED_CHAIN_URLS:
        raise IssuerChainRecoveryError("issuer-chain URL count differs")
    return {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "status": "FROZEN",
        "records": records,
        "urls": urls,
        "counts": {
            "chains": len(records),
            "source_predecessors": len(predecessor_hashes),
            "chain_urls": sum(len(row["chain_urls"]) for row in records),
            "unique_urls": len(urls),
        },
        "symbols_dates_urls_sources_responses_and_rows_public": False,
        "target_outcomes_observed_or_derived": False,
    }


def create_plan(*, env_path: Path, plan_path: Path) -> dict[str, Any]:
    store = HistoricalDayStore.from_env(env_path)
    value = plan_template(_read_gzip(_source_root(store.root) / "selection.json.gz"))
    _write_json(plan_path, value)
    return {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "status": "AWAITING_REVIEW",
        "chains": EXPECTED_CHAINS,
        "plan_path": _repo_path(plan_path),
        "target_outcomes_observed_or_derived": False,
    }


def freeze_inputs(
    *, env_path: Path, plan_path: Path, output_root: Path
) -> tuple[Path, dict[str, Any]]:
    store = HistoricalDayStore.from_env(env_path)
    if shutil.disk_usage(store.root).free < store.min_free_bytes:
        raise IssuerChainRecoveryError("historical store disk reserve is not ready")
    source_manifest = load_frozen_dataset_contract(SOURCE_MANIFEST)
    source_result = _read_json(SOURCE_RESULT)
    if not (
        source_result.get("status") == "READY"
        and source_result.get("inspected") is True
        and source_result.get("capture_status_counts") == {"CAPTURE_ERROR": 13}
        and source_result.get("target_outcomes_observed_or_derived") is False
    ):
        raise IssuerChainRecoveryError("transport recovery is not inspected READY")
    source_selection_path = _source_root(store.root) / "selection.json.gz"
    source_index_path = _source_root(store.root) / "capture-index.json"
    source_selection = _read_gzip(source_selection_path)
    plan_value = _read_json(plan_path)
    selection = build_selection(source_selection, plan_value)
    frozen_plan_path = _plan_path(store.root)
    _write_gzip(frozen_plan_path, plan_value)
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
                "CATALYST_SOURCE_RECOVERY.md",
                "PRODUCTION_STRATEGY_VALIDATION.md",
            ],
            "inspected": False,
        },
        "selection_contract": {
            "source_manifest_sha256": source_manifest["manifest_sha256"],
            "source_result_sha256": _sha256_file(SOURCE_RESULT),
            "source_private_selection_sha256": _sha256_file(source_selection_path),
            "source_private_index_sha256": _sha256_file(source_index_path),
            "operator_plan_sha256": _sha256_file(plan_path),
            "private_plan_sha256": _sha256_file(frozen_plan_path),
            "private_selection_sha256": _sha256_file(selection_path),
            **selection["counts"],
            "two_urls_per_chain": True,
            "all_urls_https_under_frozen_issuer_domain": True,
            "symbols_dates_urls_sources_responses_and_rows_public": False,
        },
        "collection_contract": {
            "collector_sha256": _sha256_file(Path(__file__)),
            "user_agent": USER_AGENT,
            "minimum_interval_seconds": MINIMUM_INTERVAL_SECONDS,
            "timeout_seconds": REQUEST_TIMEOUT_SECONDS,
            "maximum_redirects": MAX_REDIRECTS,
            "maximum_response_bytes": MAX_RESPONSE_BYTES,
            "maximum_attempts": MAXIMUM_ATTEMPTS,
            "redirects_must_remain_under_frozen_issuer_domain": True,
            "private_or_non_global_addresses_blocked": True,
            "historical_store_reserve_enforced": True,
            "checkpoint_unit": "canonical URL",
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
        raise IssuerChainRecoveryError("unexpected issuer-chain dataset")
    selection_path = _selection_path(store.root)
    selection = _read_gzip(selection_path)
    if _sha256_file(selection_path) != manifest["selection_contract"].get(
        "private_selection_sha256"
    ):
        raise IssuerChainRecoveryError("issuer-chain selection changed")
    if _sha256_file(Path(__file__)) != manifest["collection_contract"].get(
        "collector_sha256"
    ):
        raise IssuerChainRecoveryError("issuer-chain collector changed")
    source_selection_path = _source_root(store.root) / "selection.json.gz"
    frozen_plan_path = _plan_path(store.root)
    if _sha256_file(source_selection_path) != manifest["selection_contract"].get(
        "source_private_selection_sha256"
    ):
        raise IssuerChainRecoveryError("transport source selection changed")
    if _sha256_file(frozen_plan_path) != manifest["selection_contract"].get(
        "private_plan_sha256"
    ):
        raise IssuerChainRecoveryError("frozen issuer-chain plan changed")
    rebuilt = build_selection(
        _read_gzip(source_selection_path), _read_gzip(frozen_plan_path)
    )
    if rebuilt != selection:
        raise IssuerChainRecoveryError("issuer-chain selection does not rebuild")
    return manifest, selection


def _is_public_address(raw: str) -> bool:
    try:
        return ipaddress.ip_address(raw).is_global
    except ValueError:
        return False


def _validate_network_target(url: str, issuer_domain: str) -> list[str]:
    _validate_chain_url(url, issuer_domain)
    parsed = urlparse(url)
    host = parsed.hostname or ""
    try:
        resolved = sorted(
            {
                item[4][0]
                for item in socket.getaddrinfo(host, 443, type=socket.SOCK_STREAM)
            }
        )
    except OSError as exc:
        raise IssuerChainRecoveryError("issuer-chain DNS resolution failed") from exc
    if not resolved or not all(_is_public_address(address) for address in resolved):
        raise IssuerChainRecoveryError("issuer chain resolved to non-public address")
    return resolved


def _read_response_bytes(response: requests.Response) -> bytes:
    content_length = response.headers.get("Content-Length")
    if content_length and int(content_length) > MAX_RESPONSE_BYTES:
        raise IssuerChainRecoveryError("response exceeds frozen byte limit")
    buffer = bytearray()
    for chunk in response.iter_content(chunk_size=64 * 1024):
        buffer.extend(chunk)
        if len(buffer) > MAX_RESPONSE_BYTES:
            raise IssuerChainRecoveryError("response exceeded frozen byte limit")
    return bytes(buffer)


def _request_once(
    session: requests.Session, initial_url: str, issuer_domain: str
) -> dict[str, Any]:
    current = initial_url
    history = []
    for redirect_count in range(MAX_REDIRECTS + 1):
        resolved = _validate_network_target(current, issuer_domain)
        try:
            response = session.get(
                current,
                headers={"User-Agent": USER_AGENT, "Accept": "*/*"},
                timeout=REQUEST_TIMEOUT_SECONDS,
                allow_redirects=False,
                stream=True,
            )
        except requests.RequestException as exc:
            raise IssuerChainRecoveryError("issuer-chain transport failure") from exc
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
                raise IssuerChainRecoveryError("issuer-chain redirect limit exceeded")
            target = urljoin(current, location)
            _validate_chain_url(target, issuer_domain)
            current = target
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


def _capture_with_retry(
    session: requests.Session, url: str, issuer_domain: str
) -> dict[str, Any]:
    last_error = ""
    for attempt in range(MAXIMUM_ATTEMPTS):
        try:
            result = _request_once(session, url, issuer_domain)
            status = int(result["http_status"])
            if (status == 429 or status >= 500) and attempt + 1 < MAXIMUM_ATTEMPTS:
                last_error = f"HTTP {status}"
                time.sleep(2.0**attempt)
                continue
            return result
        except IssuerChainRecoveryError as exc:
            last_error = str(exc)
            if "transport" in last_error and attempt + 1 < MAXIMUM_ATTEMPTS:
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
        "terminal_urls": len(records),
        "capture_status_counts": dict(sorted(statuses.items())),
        "http_status_counts": dict(sorted(http_statuses.items())),
        "response_bytes": sum(
            int(row.get("body_bytes") or 0) for row in records.values()
        ),
        "symbols_dates_urls_sources_responses_and_rows_public": False,
        "source_semantics_classified": False,
        "target_outcomes_observed_or_derived": False,
    }


def collect(
    *, manifest_path: Path, env_path: Path, public_status_path: Path
) -> dict[str, Any]:
    store = HistoricalDayStore.from_env(env_path)
    manifest, selection = _verify_contract(manifest_path, store)
    if shutil.disk_usage(store.root).free < store.min_free_bytes:
        raise IssuerChainRecoveryError("historical store disk reserve is not ready")
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
        raise IssuerChainRecoveryError("issuer-chain checkpoint differs")
    session = requests.Session()
    last_started = 0.0
    try:
        for selected in selection["urls"]:
            url_hash = str(selected["url_sha256"])
            if url_hash in index["records"]:
                continue
            if shutil.disk_usage(store.root).free < store.min_free_bytes:
                raise IssuerChainRecoveryError(
                    "historical store disk reserve is not ready"
                )
            remaining = MINIMUM_INTERVAL_SECONDS - (time.monotonic() - last_started)
            if remaining > 0:
                time.sleep(remaining)
            last_started = time.monotonic()
            result = _capture_with_retry(
                session, str(selected["url"]), str(selected["issuer_domain"])
            )
            body = result.pop("body", None)
            if body is not None:
                if (
                    shutil.disk_usage(store.root).free - len(body)
                    < store.min_free_bytes
                ):
                    raise IssuerChainRecoveryError(
                        "response would breach historical store disk reserve"
                    )
                path = _response_path(store.root, url_hash)
                _write_bytes(path, body)
                result["body_sha256"] = _sha256_file(path)
                result["body_bytes"] = len(body)
            result.update(
                {
                    "url_sha256": url_hash,
                    "issuer_domain_sha256": hashlib.sha256(
                        str(selected["issuer_domain"]).encode()
                    ).hexdigest(),
                    "captured_at": _timestamp_now(),
                }
            )
            index["records"][url_hash] = result
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
        and len(index.get("records", {})) == EXPECTED_CHAIN_URLS
        and index.get("target_outcomes_observed_or_derived") is False
    ):
        raise IssuerChainRecoveryError("issuer-chain recovery is incomplete")
    expected = {row["url_sha256"] for row in selection["urls"]}
    if set(index["records"]) != expected:
        raise IssuerChainRecoveryError("issuer-chain URL set differs")
    for url_hash, record in index["records"].items():
        if record.get("status") != "RESPONSE_CAPTURED":
            continue
        path = _response_path(store.root, url_hash)
        if not path.exists() or _sha256_file(path) != record.get("body_sha256"):
            raise IssuerChainRecoveryError("issuer-chain response hash mismatch")
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
            "issuer_domain_redirect_boundaries_verified": True,
            "no_substitution_verified": True,
        },
        "next_required_stage": (
            "freeze issuer ownership, target binding, causal timestamp, relevance, "
            "financing conflict, and event-direction semantics before review"
        ),
        "claim_boundary": (
            "Canonical issuer-controlled chain capture only; not proof that the "
            "document issuer equals the target security, that publication was causal, "
            "or that event direction, alpha, or a strategy change is established."
        ),
    }
    _write_json(public_result_path, result)
    return result


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("plan", "freeze", "collect", "inspect"))
    parser.add_argument("--manifest", type=Path)
    parser.add_argument("--env-file", type=Path, default=PROJECT_ROOT / ".env")
    parser.add_argument("--plan", type=Path, default=DEFAULT_PLAN)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--public-status", type=Path, default=DEFAULT_PUBLIC_STATUS)
    parser.add_argument("--public-result", type=Path, default=DEFAULT_PUBLIC_RESULT)
    return parser


def main() -> int:
    args = _parser().parse_args()
    try:
        if args.command == "plan":
            output = create_plan(env_path=args.env_file, plan_path=args.plan)
        elif args.command == "freeze":
            path, manifest = freeze_inputs(
                env_path=args.env_file,
                plan_path=args.plan,
                output_root=args.output_root,
            )
            output = {"manifest": _repo_path(path), **manifest}
        elif args.manifest is None:
            raise IssuerChainRecoveryError("--manifest is required")
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
        IssuerChainRecoveryError,
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

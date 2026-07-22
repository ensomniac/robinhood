"""Freeze and collect accession-bound Schedule 13D complete submissions."""

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
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import schedule13d_capacity as capacity
import schedule13d_index_collection as indexes
from historical_concurrency import ordered_bounded_results
from historical_discovery import HistoricalDiscoveryError, SecClient, SecConfig
from historical_store import DEFAULT_MIN_FREE_BYTES, HistoricalStoreConfig, sha256_file


PROJECT_ROOT = Path(__file__).resolve().parent
SCHEMA_VERSION = 1
DATASET_ID = indexes.DATASET_ID
INDEX_COLLECTION_INSPECTION_SHA256 = (
    "5c2a7edba261104df5e3d2a52e1095c5b8bcc22cd7e0f03789177bfa66de5f77"
)
INDEX_COLLECTION_INSPECTION_PATH = (
    PROJECT_ROOT
    / "strategy_tournament/v2/schedule13d/indexes/inspections/"
    f"{capacity.CANDIDATE_ID}-index-collection-"
    f"{INDEX_COLLECTION_INSPECTION_SHA256}.json"
)
GRAPH_ROOT = PROJECT_ROOT / "strategy_tournament/v2/schedule13d/documents/manifests"
GRAPH_STATUS_PATH = (
    PROJECT_ROOT / "strategy_tournament/v2/schedule13d/documents/request-status.json"
)
COLLECTION_STATUS_PATH = (
    PROJECT_ROOT
    / "strategy_tournament/v2/schedule13d/documents/collection-status.json"
)
COLLECTION_INSPECTION_ROOT = (
    PROJECT_ROOT / "strategy_tournament/v2/schedule13d/documents/inspections"
)
INSPECTOR_PATH = PROJECT_ROOT / "schedule13d_document_collection_inspection.py"
ENV_PATH = PROJECT_ROOT / ".env"
SHARED_CACHE_NAMESPACE = "_sources/sec/schedule13d-complete-submissions-v1"
PRIVATE_NAMESPACE = f"_derived/schedule13d_capacity/{DATASET_ID}/documents"
PRIVATE_COLLECTION_NAME = "complete-submission-collection.json.gz"
WORKERS = 4
MINIMUM_SPACING_SECONDS = 0.15
TIMEOUT_SECONDS = 30.0


class Schedule13dDocumentCollectionError(RuntimeError):
    """The complete-submission graph or collection is invalid."""


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


def _read_object(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise Schedule13dDocumentCollectionError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise Schedule13dDocumentCollectionError(f"{path} must contain an object")
    return value


def _read_gzip_object(path: Path) -> dict[str, Any]:
    try:
        with gzip.open(path, "rt", encoding="utf-8") as source:
            value = json.load(source)
    except (OSError, json.JSONDecodeError) as exc:
        raise Schedule13dDocumentCollectionError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise Schedule13dDocumentCollectionError(f"{path} must contain an object")
    return value


def _write_json(value: Mapping[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_text(
            json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _write_gzip_json(value: Mapping[str, Any], path: Path) -> None:
    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode="wb", compresslevel=6, mtime=0) as stream:
        stream.write(_canonical_bytes(value))
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_bytes(buffer.getvalue())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _store_config() -> HistoricalStoreConfig:
    config = HistoricalStoreConfig.from_env(ENV_PATH)
    minimum = max(DEFAULT_MIN_FREE_BYTES, config.min_free_bytes)
    if (
        config.root.resolve().is_relative_to(PROJECT_ROOT.resolve())
        or shutil.disk_usage(config.root).free < minimum
    ):
        raise Schedule13dDocumentCollectionError(
            "private historical storage is unsafe or lacks its frozen reserve"
        )
    return config


def _sec_config(store_root: Path) -> SecConfig:
    value = SecConfig.from_env(
        ENV_PATH, store_root / SHARED_CACHE_NAMESPACE, workers=WORKERS
    )
    if not (
        value.minimum_spacing_seconds == MINIMUM_SPACING_SECONDS
        and value.timeout_seconds == TIMEOUT_SECONDS
    ):
        raise Schedule13dDocumentCollectionError("SEC client pacing contract drifted")
    return value


def _load_index_lineage() -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    collection = _read_object(indexes.COLLECTION_STATUS_PATH)
    inspection = _read_object(INDEX_COLLECTION_INSPECTION_PATH)
    store = _store_config()
    private_path = indexes._private_index_path(store.root)
    private = indexes._read_gzip_object(private_path)
    if not (
        collection.get("collection_sha256")
        == capacity.successor._self_hash(collection, "collection_sha256")
        and collection.get("valid") is True
        and collection.get("indexed_initial_sc13d_count") == 6852
        and collection.get("market_outcomes_accessed") is False
        and collection.get("broker_actions") == 0
        and inspection.get("inspection_sha256")
        == INDEX_COLLECTION_INSPECTION_SHA256
        and inspection.get("inspection_sha256")
        == capacity.successor._self_hash(inspection, "inspection_sha256")
        and inspection.get("collection_sha256") == collection["collection_sha256"]
        and inspection.get("indexed_initial_sc13d_count") == 6852
        and inspection.get("accession_document_request_freeze_required") is True
        and inspection.get("accession_document_access_permitted") is False
        and inspection.get("market_outcomes_accessed") is False
        and inspection.get("valid") is True
        and private.get("private_index_sha256")
        == collection.get("private_index_sha256")
        and sha256_file(private_path) == collection.get("private_index_file_sha256")
        and private.get("indexed_initial_sc13d_count") == 6852
        and private.get("market_outcomes_accessed") is False
        and private.get("broker_actions") == 0
    ):
        raise Schedule13dDocumentCollectionError(
            "inspected quarterly-index lineage is invalid"
        )
    return collection, inspection, private


def _request(row: Mapping[str, Any], ordinal: int) -> dict[str, Any]:
    filename = str(row["filename"])
    relative = Path(filename)
    if (
        relative.is_absolute()
        or ".." in relative.parts
        or len(relative.parts) != 4
        or relative.parts[0:2] != ("edgar", "data")
        or relative.suffix.lower() != ".txt"
    ):
        raise Schedule13dDocumentCollectionError(
            f"unsafe complete-submission filename: {filename}"
        )
    url = "https://www.sec.gov/Archives/" + filename
    value = {
        "ordinal": ordinal,
        "year": int(row["year"]),
        "quarter": int(row["quarter"]),
        "cik": str(row["cik"]),
        "filed_on": str(row["filed_on"]),
        "filename": filename,
        "url": url,
        "shared_cache_relative_path": filename,
    }
    value["request_sha256"] = _sha256_json(value)
    return value


def build_request_graph() -> dict[str, Any]:
    """Build all exact accession requests without reading filing documents."""

    collection, inspection, private = _load_index_lineage()
    store = _store_config()
    sec = _sec_config(store.root)
    requests = [
        _request(row, ordinal)
        for ordinal, row in enumerate(private["indexed_initial_sc13d_rows"])
    ]
    if len({row["url"] for row in requests}) != len(requests):
        raise Schedule13dDocumentCollectionError(
            "complete-submission request URLs are not unique"
        )
    graph: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "artifact_kind": "outcome-blind-sec-complete-submission-request-graph",
        "campaign_id": capacity.CAMPAIGN_ID,
        "candidate_id": capacity.CANDIDATE_ID,
        "strategy_version": capacity.STRATEGY_VERSION,
        "dataset_id": DATASET_ID,
        "contract_sha256": indexes.CONTRACT_SHA256,
        "index_collection_sha256": collection["collection_sha256"],
        "index_collection_inspection_sha256": inspection["inspection_sha256"],
        "private_index_sha256": private["private_index_sha256"],
        "private_index_file_sha256": collection["private_index_file_sha256"],
        "request_contract": {
            "provider": "SEC_EDGAR",
            "endpoint_kind": "accession-bound complete submission text",
            "request_count": len(requests),
            "requests": requests,
            "workers": WORKERS,
            "global_minimum_spacing_seconds": MINIMUM_SPACING_SECONDS,
            "maximum_requests_per_second": 1 / MINIMUM_SPACING_SECONDS,
            "timeout_seconds": TIMEOUT_SECONDS,
            "maximum_attempts": 4,
            "provider_substitution_permitted": False,
            "user_agent_sha256": hashlib.sha256(sec.user_agent.encode()).hexdigest(),
        },
        "storage_contract": {
            "shared_cache_namespace": SHARED_CACHE_NAMESPACE,
            "private_derived_namespace": PRIVATE_NAMESPACE,
            "minimum_free_bytes_after_reserve": max(
                DEFAULT_MIN_FREE_BYTES, store.min_free_bytes
            ),
            "raw_source_retention_required": True,
            "historical_deletion_permitted": False,
            "repository_storage_permitted": False,
        },
        "classification_handoff": {
            "acceptance_datetime_source": "ACCEPTANCE-DATETIME in complete submission",
            "primary_document_rule": (
                "first accession-bound document whose normalized TYPE is SC 13D"
            ),
            "classification_before_collection_inspection_permitted": False,
            "terminal_reason_required_for_every_request": True,
        },
        "access_contract": {
            "provider_access_before_graph_inspection_permitted": False,
            "provider_access_after_graph_inspection_permitted": True,
            "filing_semantic_classification_permitted": False,
            "issuer_symbol_supplemental_access_permitted": False,
            "market_price_access_permitted": False,
            "entry_fill_access_permitted": False,
            "exit_or_stop_access_permitted": False,
            "forward_return_computation_permitted": False,
            "stage0_outcome_access_permitted": False,
            "broker_actions_permitted": False,
            "maturity_effect": "NONE",
        },
        "implementation_hashes": {
            str(path.relative_to(PROJECT_ROOT)): sha256_file(path)
            for path in (Path(__file__).resolve(), INSPECTOR_PATH)
        },
        "filing_count": len(requests),
        "verified_event_count": None,
        "returns_computed": 0,
        "market_outcomes_accessed": False,
        "claim_limit": (
            "This graph freezes only accession-bound SEC complete-submission "
            "requests. It contains no filing content, eligibility result, price, "
            "fill, return, or maturity evidence."
        ),
    }
    graph["request_graph_sha256"] = capacity.successor._self_hash(
        graph, "request_graph_sha256"
    )
    return graph


def graph_path(graph: Mapping[str, Any], root: Path = GRAPH_ROOT) -> Path:
    return root / f"{capacity.CANDIDATE_ID}-{graph['request_graph_sha256']}.json"


def load_request_graph(path: Path) -> dict[str, Any]:
    value = _read_object(path)
    digest = value.get("request_graph_sha256")
    if not (
        isinstance(digest, str)
        and digest == capacity.successor._self_hash(value, "request_graph_sha256")
        and path.name == f"{capacity.CANDIDATE_ID}-{digest}.json"
    ):
        raise Schedule13dDocumentCollectionError(
            "complete-submission request graph was mutated or renamed"
        )
    return value


def freeze_request_graph(
    *, root: Path = GRAPH_ROOT, status_path: Path = GRAPH_STATUS_PATH
) -> tuple[Path, dict[str, Any]]:
    value = build_request_graph()
    path = graph_path(value, root)
    if path.exists() and _read_object(path) != value:
        raise Schedule13dDocumentCollectionError(
            "content-addressed document graph has other content"
        )
    _write_json(value, path)
    _write_json(
        {
            "schema_version": SCHEMA_VERSION,
            "candidate_id": capacity.CANDIDATE_ID,
            "dataset_id": DATASET_ID,
            "contract_sha256": indexes.CONTRACT_SHA256,
            "request_graph_sha256": value["request_graph_sha256"],
            "request_count": value["request_contract"]["request_count"],
            "status": "DOCUMENT_REQUEST_GRAPH_PENDING_INSPECTION",
            "provider_access_permitted": False,
            "filing_semantic_classification_permitted": False,
            "verified_event_count": None,
            "market_price_access_permitted": False,
            "outcome_access_permitted": False,
            "broker_actions_permitted": False,
            "valid": False,
        },
        status_path,
    )
    return path, value


def _repo_relative(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(PROJECT_ROOT))
    except ValueError as exc:
        raise Schedule13dDocumentCollectionError(
            f"publication source is outside repository: {path}"
        ) from exc


def _published_source(path: Path) -> dict[str, str]:
    relative = _repo_relative(path)
    status = subprocess.run(
        ["git", "status", "--porcelain", "--", relative],
        cwd=PROJECT_ROOT,
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    if status.strip():
        raise Schedule13dDocumentCollectionError(
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
        raise Schedule13dDocumentCollectionError(
            "provider access requires HEAD to equal its pushed upstream"
        )
    committed = subprocess.run(
        ["git", "show", f"HEAD:{relative}"],
        cwd=PROJECT_ROOT,
        check=True,
        capture_output=True,
    ).stdout
    digest = sha256_file(path)
    if hashlib.sha256(committed).hexdigest() != digest:
        raise Schedule13dDocumentCollectionError(
            f"committed bytes differ: {relative}"
        )
    return {"commit": head, "path": relative, "sha256": digest}


def _resolve_cache_path(store_root: Path, request: Mapping[str, Any]) -> Path:
    relative = Path(str(request["shared_cache_relative_path"]))
    if (
        relative.is_absolute()
        or ".." in relative.parts
        or relative.parts[0:2] != ("edgar", "data")
    ):
        raise Schedule13dDocumentCollectionError("document cache path is unsafe")
    return store_root / SHARED_CACHE_NAMESPACE / relative


def _private_collection_path(store_root: Path) -> Path:
    return store_root / PRIVATE_NAMESPACE / PRIVATE_COLLECTION_NAME


def _load_graph_for_collection(path: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    graph = load_request_graph(path)
    expected = build_request_graph()
    status = _read_object(GRAPH_STATUS_PATH)
    for relative, digest in graph["implementation_hashes"].items():
        if sha256_file(PROJECT_ROOT / relative) != digest:
            raise Schedule13dDocumentCollectionError(
                f"document-graph implementation drifted: {relative}"
            )
    if not (
        graph == expected
        and status.get("status") == "DOCUMENT_REQUEST_GRAPH_INSPECTED"
        and status.get("request_graph_sha256") == graph["request_graph_sha256"]
        and status.get("inspection_sha256")
        == capacity.successor._self_hash(status, "inspection_sha256")
        and status.get("provider_access_permitted") is True
        and status.get("filing_semantic_classification_permitted") is False
        and status.get("market_price_access_permitted") is False
        and status.get("outcome_access_permitted") is False
        and status.get("broker_actions_permitted") is False
        and status.get("valid") is True
    ):
        raise Schedule13dDocumentCollectionError(
            "document request graph is not independently inspected"
        )
    return graph, status


def _collect_one(
    client: SecClient, store_root: Path, request: Mapping[str, Any]
) -> dict[str, Any]:
    path = _resolve_cache_path(store_root, request)
    existed = path.is_file()
    text = client.text(str(request["url"]), path)
    raw = path.read_bytes()
    if not raw or "<SEC-DOCUMENT" not in text[:4096].upper():
        raise Schedule13dDocumentCollectionError(
            f"SEC complete submission is malformed: {request['url']}"
        )
    return {
        "ordinal": request["ordinal"],
        "request_sha256": request["request_sha256"],
        "source_origin": "SHARED_CACHE" if existed else "SEC_DOWNLOAD",
        "source_bytes": len(raw),
        "source_sha256": hashlib.sha256(raw).hexdigest(),
        "status": "SUCCESS",
    }


def collect_documents(path: Path) -> dict[str, Any]:
    """Collect every frozen complete submission without semantic classification."""

    graph, graph_inspection = _load_graph_for_collection(path)
    publication = {
        "collector": _published_source(Path(__file__).resolve()),
        "request_graph": _published_source(path),
        "request_graph_inspection": _published_source(GRAPH_STATUS_PATH),
    }
    store = _store_config()
    sec_config = _sec_config(store.root)
    if hashlib.sha256(sec_config.user_agent.encode()).hexdigest() != graph[
        "request_contract"
    ]["user_agent_sha256"]:
        raise Schedule13dDocumentCollectionError(
            "SEC private user-agent identity differs from the frozen graph"
        )
    client = SecClient(sec_config)

    def collect(request: Mapping[str, Any]) -> dict[str, Any]:
        return _collect_one(client, store.root, request)

    records: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    for outcome in ordered_bounded_results(
        list(graph["request_contract"]["requests"]),
        collect,
        max_workers=WORKERS,
    ):
        request = outcome.item
        if outcome.error is not None:
            failures.append(
                {
                    "ordinal": request["ordinal"],
                    "request_sha256": request["request_sha256"],
                    "error_type": type(outcome.error).__name__,
                    "error": str(outcome.error),
                }
            )
        elif isinstance(outcome.value, dict):
            records.append(outcome.value)
    records.sort(key=lambda row: int(row["ordinal"]))
    failures.sort(key=lambda row: int(row["ordinal"]))
    private: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "dataset_id": DATASET_ID,
        "candidate_id": capacity.CANDIDATE_ID,
        "contract_sha256": indexes.CONTRACT_SHA256,
        "request_graph_sha256": graph["request_graph_sha256"],
        "collector_sha256": sha256_file(Path(__file__).resolve()),
        "captured_at": datetime.now(UTC).isoformat(),
        "request_count": len(graph["request_contract"]["requests"]),
        "success_count": len(records),
        "failure_count": len(failures),
        "records": records,
        "failures": failures,
        "acceptance_time_classified_count": 0,
        "filing_semantic_classified_count": 0,
        "terminal_reason_count": 0,
        "verified_event_count": None,
        "market_price_values_accessed": 0,
        "returns_computed": 0,
        "market_outcomes_accessed": False,
        "broker_actions": 0,
    }
    private["private_collection_sha256"] = capacity.successor._self_hash(
        private, "private_collection_sha256"
    )
    private_path = _private_collection_path(store.root)
    _write_gzip_json(private, private_path)
    valid = not failures and len(records) == len(graph["request_contract"]["requests"])
    result: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "collection_kind": "outcome-blind-sec-complete-submission-collection",
        "campaign_id": capacity.CAMPAIGN_ID,
        "candidate_id": capacity.CANDIDATE_ID,
        "dataset_id": DATASET_ID,
        "contract_sha256": indexes.CONTRACT_SHA256,
        "request_graph_sha256": graph["request_graph_sha256"],
        "request_graph_inspection_sha256": graph_inspection["inspection_sha256"],
        "collector_sha256": sha256_file(Path(__file__).resolve()),
        "publication": publication,
        "request_count": private["request_count"],
        "success_count": private["success_count"],
        "failure_count": private["failure_count"],
        "source_bytes": sum(int(row["source_bytes"]) for row in records),
        "private_collection_sha256": private["private_collection_sha256"],
        "private_collection_file_sha256": sha256_file(private_path),
        "provider_telemetry": client.stats(),
        "acceptance_time_classified_count": 0,
        "filing_semantic_classified_count": 0,
        "terminal_reason_count": 0,
        "filing_semantic_classification_permitted": False,
        "capacity_classification_complete": False,
        "verified_event_count": None,
        "market_price_values_accessed": 0,
        "returns_computed": 0,
        "market_outcomes_accessed": False,
        "broker_actions": 0,
        "maturity_effect": "NONE",
        "valid": valid,
    }
    result["collection_sha256"] = capacity.successor._self_hash(
        result, "collection_sha256"
    )
    _write_json(result, COLLECTION_STATUS_PATH)
    if not valid:
        raise Schedule13dDocumentCollectionError(
            f"complete-submission collection has {len(failures)} failures"
        )
    return result


def _one(pattern: str, description: str) -> Path:
    matches = sorted(PROJECT_ROOT.glob(pattern))
    if len(matches) != 1:
        raise Schedule13dDocumentCollectionError(
            f"expected one {description}; found {len(matches)}"
        )
    return matches[0]


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("freeze", "collect", "status"))
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "freeze":
            path, value = freeze_request_graph()
            result: dict[str, Any] = {
                "written": _repo_relative(path),
                "request_graph_sha256": value["request_graph_sha256"],
                "request_count": value["request_contract"]["request_count"],
                "verified_event_count": None,
                "provider_access_permitted": False,
                "market_outcomes_accessed": False,
            }
        elif args.command == "collect":
            path = _one(
                "strategy_tournament/v2/schedule13d/documents/manifests/"
                f"{capacity.CANDIDATE_ID}-*.json",
                "complete-submission request graph",
            )
            result = collect_documents(path)
        else:
            result = _read_object(COLLECTION_STATUS_PATH)
    except (
        Schedule13dDocumentCollectionError,
        HistoricalDiscoveryError,
        OSError,
        ValueError,
        subprocess.CalledProcessError,
    ) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}), file=sys.stderr)
        return 2
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""Freeze and collect causal-symbol SEC issuer-submissions metadata."""

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
from collections import defaultdict
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import schedule13d_capacity as capacity
import schedule13d_semantic_capacity as semantic
from historical_concurrency import ordered_bounded_results
from historical_discovery import HistoricalDiscoveryError, SecClient, SecConfig
from historical_store import DEFAULT_MIN_FREE_BYTES, HistoricalStoreConfig, sha256_file


PROJECT_ROOT = Path(__file__).resolve().parent
SCHEMA_VERSION = 1
SEMANTIC_INSPECTION_SHA256 = (
    "9973ee5a49484f2ec041e439c8310f622f8eedb7be431bfff35039e7be1f7522"
)
SEMANTIC_INSPECTION_PATH = (
    PROJECT_ROOT
    / "strategy_tournament/v2/schedule13d/semantic/inspections/"
    f"{capacity.CANDIDATE_ID}-semantic-{SEMANTIC_INSPECTION_SHA256}.json"
)
SEMANTIC_RESULT_PATH = (
    PROJECT_ROOT
    / "strategy_tournament/v2/schedule13d/semantic/results/"
    f"{capacity.CANDIDATE_ID}-"
    "3bf6253e91ecc3cf8b15788ff047cf1d66868bfb2ec204fd72cc54081b055f13.json"
)
GRAPH_ROOT = (
    PROJECT_ROOT / "strategy_tournament/v2/schedule13d/symbols/submissions/manifests"
)
GRAPH_STATUS_PATH = (
    PROJECT_ROOT
    / "strategy_tournament/v2/schedule13d/symbols/submissions/request-status.json"
)
COLLECTION_STATUS_PATH = (
    PROJECT_ROOT
    / "strategy_tournament/v2/schedule13d/symbols/submissions/collection-status.json"
)
COLLECTION_INSPECTION_ROOT = (
    PROJECT_ROOT
    / "strategy_tournament/v2/schedule13d/symbols/submissions/inspections"
)
INSPECTOR_PATH = PROJECT_ROOT / "schedule13d_symbol_submissions_inspection.py"
ENV_PATH = PROJECT_ROOT / ".env"
SHARED_CACHE_NAMESPACE = "_sources/sec/schedule13d-issuer-submissions-v1"
PRIVATE_NAMESPACE = (
    f"_derived/schedule13d_capacity/{semantic.documents.DATASET_ID}/symbols/submissions"
)
PRIVATE_COLLECTION_NAME = "issuer-submissions-collection.json.gz"
WORKERS = 4
MINIMUM_SPACING_SECONDS = 0.15
TIMEOUT_SECONDS = 30.0
ALLOWED_FORMS = ("10-K", "10-Q", "8-K", "20-F", "6-K")


class Schedule13dSymbolSubmissionsError(RuntimeError):
    """The issuer-submissions graph or collection is invalid."""


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
        raise Schedule13dSymbolSubmissionsError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise Schedule13dSymbolSubmissionsError(f"{path} must contain an object")
    return value


def _read_gzip_object(path: Path) -> dict[str, Any]:
    try:
        with gzip.open(path, "rt", encoding="utf-8") as source:
            value = json.load(source)
    except (OSError, json.JSONDecodeError) as exc:
        raise Schedule13dSymbolSubmissionsError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise Schedule13dSymbolSubmissionsError(f"{path} must contain an object")
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
        raise Schedule13dSymbolSubmissionsError(
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
        raise Schedule13dSymbolSubmissionsError("SEC client pacing contract drifted")
    return value


def _load_semantic_lineage() -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    result = _read_object(SEMANTIC_RESULT_PATH)
    inspection = _read_object(SEMANTIC_INSPECTION_PATH)
    private_path = semantic._private_result_path()
    private = semantic._read_gzip_object(private_path)
    if not (
        result.get("result_sha256")
        == capacity.successor._self_hash(result, "result_sha256")
        and result.get("pending_causal_symbol_supplemental") == 317
        and result.get("capacity_passed") is None
        and result.get("market_outcomes_accessed") is False
        and inspection.get("inspection_sha256") == SEMANTIC_INSPECTION_SHA256
        and inspection.get("inspection_sha256")
        == capacity.successor._self_hash(inspection, "inspection_sha256")
        and inspection.get("result_sha256") == result["result_sha256"]
        and inspection.get("pending_causal_symbol_supplemental") == 317
        and inspection.get("issuer_symbol_supplemental_access_permitted") is True
        and inspection.get("market_outcomes_accessed") is False
        and inspection.get("valid") is True
        and private.get("classification_sha256")
        == result.get("classification_sha256")
        and sha256_file(private_path) == result.get("private_result_file_sha256")
        and private.get("pending_causal_symbol_supplemental") == 317
        and private.get("market_outcomes_accessed") is False
    ):
        raise Schedule13dSymbolSubmissionsError(
            "inspected semantic supplemental lineage is invalid"
        )
    return result, inspection, private


def _request(cik: str, events: Sequence[Mapping[str, Any]], ordinal: int) -> dict[str, Any]:
    padded = str(int(cik)).zfill(10)
    value = {
        "ordinal": ordinal,
        "subject_cik": str(int(cik)),
        "event_count": len(events),
        "earliest_event_accepted_at": min(str(row["accepted_at"]) for row in events),
        "latest_event_accepted_at": max(str(row["accepted_at"]) for row in events),
        "url": f"https://data.sec.gov/submissions/CIK{padded}.json",
        "shared_cache_relative_path": f"submissions/CIK{padded}.json",
    }
    value["request_sha256"] = _sha256_json(value)
    return value


def build_request_graph() -> dict[str, Any]:
    """Freeze subject-CIK submissions requests without reading provider metadata."""

    result, inspection, private = _load_semantic_lineage()
    events_by_cik: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in private["records"]:
        if row.get("status") == "PENDING_CAUSAL_SYMBOL_SUPPLEMENTAL":
            events_by_cik[str(row["subject_cik"])].append(row)
    if sum(len(rows) for rows in events_by_cik.values()) != 317:
        raise Schedule13dSymbolSubmissionsError("pending event denominator drifted")
    requests = [
        _request(cik, events_by_cik[cik], ordinal)
        for ordinal, cik in enumerate(sorted(events_by_cik, key=int))
    ]
    if len(requests) != 312 or len({row["url"] for row in requests}) != 312:
        raise Schedule13dSymbolSubmissionsError("subject-CIK request graph differs")
    store = _store_config()
    sec = _sec_config(store.root)
    graph: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "artifact_kind": "outcome-blind-sec-issuer-submissions-request-graph",
        "campaign_id": capacity.CAMPAIGN_ID,
        "candidate_id": capacity.CANDIDATE_ID,
        "strategy_version": capacity.STRATEGY_VERSION,
        "dataset_id": semantic.documents.DATASET_ID,
        "contract_sha256": semantic.documents.indexes.CONTRACT_SHA256,
        "semantic_result_sha256": result["result_sha256"],
        "semantic_inspection_sha256": inspection["inspection_sha256"],
        "classification_sha256": private["classification_sha256"],
        "request_contract": {
            "provider": "SEC_EDGAR",
            "endpoint_kind": "issuer submissions metadata",
            "pending_event_count": 317,
            "unique_subject_cik_count": len(requests),
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
        "source_selection_contract": {
            "allowed_forms": list(ALLOWED_FORMS),
            "accepted_no_later_than_event_required": True,
            "latest_eligible_prior_filing_required": True,
            "supplemental_submission_files_must_be_frozen_before_access": True,
            "primary_documents_must_be_frozen_before_access": True,
            "dei_trading_symbol_only": True,
            "current_or_future_symbol_mapping_permitted": False,
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
        "access_contract": {
            "provider_access_before_graph_inspection_permitted": False,
            "provider_access_after_graph_inspection_permitted": True,
            "supplemental_submission_file_access_permitted": False,
            "issuer_primary_document_access_permitted": False,
            "market_price_access_permitted": False,
            "forward_return_computation_permitted": False,
            "stage0_outcome_access_permitted": False,
            "broker_actions_permitted": False,
            "maturity_effect": "NONE",
        },
        "implementation_hashes": {
            str(path.relative_to(PROJECT_ROOT)): sha256_file(path)
            for path in (Path(__file__).resolve(), INSPECTOR_PATH)
        },
        "pending_event_count": 317,
        "verified_event_count": 1,
        "capacity_passed": None,
        "returns_computed": 0,
        "market_outcomes_accessed": False,
        "claim_limit": (
            "This graph freezes only subject-CIK submissions metadata requests. "
            "It contains no issuer filing selection, recovered symbol, price, "
            "return, or maturity evidence."
        ),
    }
    graph["request_graph_sha256"] = capacity.successor._self_hash(
        graph, "request_graph_sha256"
    )
    return graph


def graph_path(value: Mapping[str, Any], root: Path = GRAPH_ROOT) -> Path:
    return root / f"{capacity.CANDIDATE_ID}-{value['request_graph_sha256']}.json"


def load_request_graph(path: Path) -> dict[str, Any]:
    value = _read_object(path)
    digest = value.get("request_graph_sha256")
    if not (
        isinstance(digest, str)
        and digest == capacity.successor._self_hash(value, "request_graph_sha256")
        and path.name == f"{capacity.CANDIDATE_ID}-{digest}.json"
    ):
        raise Schedule13dSymbolSubmissionsError(
            "issuer-submissions graph was mutated or renamed"
        )
    return value


def freeze_request_graph(
    *, root: Path = GRAPH_ROOT, status_path: Path = GRAPH_STATUS_PATH
) -> tuple[Path, dict[str, Any]]:
    value = build_request_graph()
    path = graph_path(value, root)
    if path.exists() and _read_object(path) != value:
        raise Schedule13dSymbolSubmissionsError(
            "content-addressed submissions graph has other content"
        )
    _write_json(value, path)
    _write_json(
        {
            "schema_version": SCHEMA_VERSION,
            "candidate_id": capacity.CANDIDATE_ID,
            "dataset_id": semantic.documents.DATASET_ID,
            "contract_sha256": semantic.documents.indexes.CONTRACT_SHA256,
            "request_graph_sha256": value["request_graph_sha256"],
            "request_count": 312,
            "pending_event_count": 317,
            "status": "SYMBOL_SUBMISSIONS_GRAPH_PENDING_INSPECTION",
            "provider_access_permitted": False,
            "supplemental_submission_file_access_permitted": False,
            "issuer_primary_document_access_permitted": False,
            "capacity_passed": None,
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
        raise Schedule13dSymbolSubmissionsError(
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
    if status.strip() or head != upstream:
        raise Schedule13dSymbolSubmissionsError(
            f"provider input is not committed and pushed: {relative}"
        )
    committed = subprocess.run(
        ["git", "show", f"HEAD:{relative}"],
        cwd=PROJECT_ROOT,
        check=True,
        capture_output=True,
    ).stdout
    digest = sha256_file(path)
    if hashlib.sha256(committed).hexdigest() != digest:
        raise Schedule13dSymbolSubmissionsError(f"committed bytes differ: {relative}")
    return {"commit": head, "path": relative, "sha256": digest}


def _resolve_cache_path(store_root: Path, request: Mapping[str, Any]) -> Path:
    relative = Path(str(request["shared_cache_relative_path"]))
    if relative.is_absolute() or ".." in relative.parts:
        raise Schedule13dSymbolSubmissionsError("submissions cache path is unsafe")
    return store_root / SHARED_CACHE_NAMESPACE / relative


def _private_collection_path(store_root: Path) -> Path:
    return store_root / PRIVATE_NAMESPACE / PRIVATE_COLLECTION_NAME


def _load_graph_for_collection(path: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    graph = load_request_graph(path)
    status = _read_object(GRAPH_STATUS_PATH)
    if not (
        graph == build_request_graph()
        and status.get("status") == "SYMBOL_SUBMISSIONS_GRAPH_INSPECTED"
        and status.get("request_graph_sha256") == graph["request_graph_sha256"]
        and status.get("inspection_sha256")
        == capacity.successor._self_hash(status, "inspection_sha256")
        and status.get("provider_access_permitted") is True
        and status.get("supplemental_submission_file_access_permitted") is False
        and status.get("issuer_primary_document_access_permitted") is False
        and status.get("market_price_access_permitted") is False
        and status.get("outcome_access_permitted") is False
        and status.get("valid") is True
    ):
        raise Schedule13dSymbolSubmissionsError(
            "issuer-submissions graph is not independently inspected"
        )
    return graph, status


def _collect_one(
    client: SecClient, store_root: Path, request: Mapping[str, Any]
) -> dict[str, Any]:
    path = _resolve_cache_path(store_root, request)
    existed = path.is_file()
    payload = client.json(str(request["url"]), path)
    raw = path.read_bytes()
    payload_cik = str(payload.get("cik") or "").lstrip("0") or "0"
    if payload_cik != request["subject_cik"]:
        raise Schedule13dSymbolSubmissionsError(
            f"issuer submissions CIK differs: {request['subject_cik']}"
        )
    return {
        "ordinal": request["ordinal"],
        "subject_cik": request["subject_cik"],
        "request_sha256": request["request_sha256"],
        "source_origin": "SHARED_CACHE" if existed else "SEC_DOWNLOAD",
        "source_bytes": len(raw),
        "source_sha256": hashlib.sha256(raw).hexdigest(),
        "status": "SUCCESS",
    }


def collect_submissions(path: Path) -> dict[str, Any]:
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
        raise Schedule13dSymbolSubmissionsError(
            "SEC private user-agent identity differs from the frozen graph"
        )
    client = SecClient(sec_config)

    def collect(request: Mapping[str, Any]) -> dict[str, Any]:
        return _collect_one(client, store.root, request)

    records: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    for outcome in ordered_bounded_results(
        list(graph["request_contract"]["requests"]), collect, max_workers=WORKERS
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
        "dataset_id": semantic.documents.DATASET_ID,
        "candidate_id": capacity.CANDIDATE_ID,
        "contract_sha256": semantic.documents.indexes.CONTRACT_SHA256,
        "request_graph_sha256": graph["request_graph_sha256"],
        "collector_sha256": sha256_file(Path(__file__).resolve()),
        "captured_at": datetime.now(UTC).isoformat(),
        "request_count": 312,
        "success_count": len(records),
        "failure_count": len(failures),
        "records": records,
        "failures": failures,
        "supplemental_submission_file_count": None,
        "issuer_primary_document_count": None,
        "recovered_symbol_count": 0,
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
    valid = not failures and len(records) == 312
    result: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "collection_kind": "outcome-blind-sec-issuer-submissions-collection",
        "campaign_id": capacity.CAMPAIGN_ID,
        "candidate_id": capacity.CANDIDATE_ID,
        "dataset_id": semantic.documents.DATASET_ID,
        "contract_sha256": semantic.documents.indexes.CONTRACT_SHA256,
        "request_graph_sha256": graph["request_graph_sha256"],
        "request_graph_inspection_sha256": graph_inspection["inspection_sha256"],
        "collector_sha256": sha256_file(Path(__file__).resolve()),
        "publication": publication,
        "pending_event_count": 317,
        "request_count": 312,
        "success_count": len(records),
        "failure_count": len(failures),
        "source_bytes": sum(int(row["source_bytes"]) for row in records),
        "private_collection_sha256": private["private_collection_sha256"],
        "private_collection_file_sha256": sha256_file(private_path),
        "provider_telemetry": client.stats(),
        "supplemental_submission_file_count": None,
        "issuer_primary_document_count": None,
        "recovered_symbol_count": 0,
        "supplemental_submission_file_access_permitted": False,
        "issuer_primary_document_access_permitted": False,
        "capacity_passed": None,
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
        raise Schedule13dSymbolSubmissionsError(
            f"issuer-submissions collection has {len(failures)} failures"
        )
    return result


def _one(pattern: str, description: str) -> Path:
    matches = sorted(PROJECT_ROOT.glob(pattern))
    if len(matches) != 1:
        raise Schedule13dSymbolSubmissionsError(
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
                "request_count": 312,
                "pending_event_count": 317,
                "provider_access_permitted": False,
                "market_outcomes_accessed": False,
            }
        elif args.command == "collect":
            path = _one(
                "strategy_tournament/v2/schedule13d/symbols/submissions/manifests/"
                f"{capacity.CANDIDATE_ID}-*.json",
                "issuer-submissions request graph",
            )
            result = collect_submissions(path)
        else:
            result = _read_object(COLLECTION_STATUS_PATH)
    except (
        Schedule13dSymbolSubmissionsError,
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

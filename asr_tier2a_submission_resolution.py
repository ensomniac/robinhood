"""Resolve exact acceptance times for inspected ASR tier-2A candidates."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import subprocess
import sys
import time
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import requests

import asr_semantic_tier as semantic
import asr_submission_collection as submissions
import asr_submission_tier as tier1
import asr_tier2a as tier2a
from historical_discovery import SecConfig


PROJECT_ROOT = Path(__file__).resolve().parent
RESOLUTION_ID = "tier2a-qualified-complete-submission-resolution"
EXPECTED_ACCESSIONS = 252
EXPECTED_CANDIDATES = 252
TIER2A_RESULT_SHA256 = (
    "1b47b5871612e84dff60953c824ec9f7c026c2b7be411a5dcd1eebb353155fe5"
)
TIER2A_RESULT_PATH = tier2a.DEFAULT_RESULT_ROOT / (
    f"{tier2a.capacity.CANDIDATE_ID}-tier2a-{TIER2A_RESULT_SHA256}.json"
)
TIER2A_INSPECTION_SHA256 = (
    "3b2e4c6ab06442a81ae4a44690a5e8ec71e413866f2101140b6729204698ea5e"
)
TIER2A_INSPECTION_PATH = (
    tier2a.DEFAULT_RESULT_ROOT
    / "inspections"
    / f"tier2a-{TIER2A_INSPECTION_SHA256}.json"
)
DEFAULT_ROOT = PROJECT_ROOT / "strategy_tournament/v2/asr/tier2a/submissions"
DEFAULT_CONTRACT_ROOT = DEFAULT_ROOT / "contracts"
DEFAULT_CONTRACT_STATUS = DEFAULT_ROOT / "contract-status.json"
DEFAULT_COLLECTION_STATUS = DEFAULT_ROOT / "collection-status.json"
DEFAULT_COLLECTION_INSPECTION = DEFAULT_ROOT / "collection-inspection.json"
DEFAULT_RESULT_ROOT = DEFAULT_ROOT / "results"
PRIVATE_GRAPH_NAMESPACE = "_derived/asr-tier2a-qualified-submission-graph"
PRIVATE_COLLECTION_NAMESPACE = "_derived/asr-tier2a-qualified-submissions"
PRIVATE_RESULT_NAMESPACE = "_derived/asr-tier2a-qualified-result"
RAW_NAMESPACE = "_sources/sec/asr-tier2a-qualified-submissions"


class AsrTier2aSubmissionResolutionError(RuntimeError):
    """The tier-2A submission resolution lane is invalid."""


def canonical_bytes(value: Any) -> bytes:
    return tier2a.canonical_bytes(value)


def self_hash(value: Mapping[str, Any], field: str) -> str:
    return tier2a.self_hash(value, field)


def read_object(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise AsrTier2aSubmissionResolutionError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise AsrTier2aSubmissionResolutionError(f"{path} must contain an object")
    return value


def write_object(value: Mapping[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )


def _write_gzip(value: Mapping[str, Any], path: Path) -> bytes:
    raw = gzip.compress(canonical_bytes(value), mtime=0)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_bytes(raw)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)
    return raw


def _read_gzip(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(gzip.decompress(path.read_bytes()))
    except (OSError, gzip.BadGzipFile, json.JSONDecodeError) as exc:
        raise AsrTier2aSubmissionResolutionError(
            f"cannot read private artifact {path}: {exc}"
        ) from exc
    if not isinstance(value, dict):
        raise AsrTier2aSubmissionResolutionError(
            "private artifact must contain an object"
        )
    return value


def file_hash(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _published(path: Path) -> dict[str, str]:
    return tier1.shared._published(path)


def _lineage(root: Path) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    result = read_object(TIER2A_RESULT_PATH)
    inspection = read_object(TIER2A_INSPECTION_PATH)
    collection_status, collection = tier2a._load_private_collection(root)
    info = result["private_result"]
    private_path = root / str(info["cache_relative_path"])
    private_raw = private_path.read_bytes() if private_path.is_file() else b""
    private = _read_gzip(private_path)
    if not (
        result.get("result_sha256") == TIER2A_RESULT_SHA256
        and result.get("result_sha256") == self_hash(result, "result_sha256")
        and result.get("document_semantic_candidate_count") == 508
        and result.get("qualified_accession_count") == EXPECTED_ACCESSIONS
        and result.get("market_outcomes_accessed") is False
        and inspection.get("inspection_sha256") == TIER2A_INSPECTION_SHA256
        and inspection.get("inspection_sha256")
        == self_hash(inspection, "inspection_sha256")
        and inspection.get("qualified_submission_manifest_freeze_permitted") is True
        and inspection.get("complete_submission_access_permitted") is False
        and inspection.get("market_outcomes_accessed") is False
        and hashlib.sha256(private_raw).hexdigest() == info["file_sha256"]
        and len(private_raw) == info["bytes"]
        and private.get("private_result_sha256") == info["private_result_sha256"]
        and private.get("private_result_sha256")
        == self_hash(private, "private_result_sha256")
        and collection_status.get("collection_sha256") == result["collection_sha256"]
        and collection.get("market_outcomes_accessed") is False
    ):
        raise AsrTier2aSubmissionResolutionError("inspected tier-2A lineage differs")
    return result, private, collection


def build_private_graph(
    *, store_root: Path | None = None
) -> tuple[dict[str, Any], Path]:
    root = store_root or tier1.shared._store().root
    result, private_result, collection = _lineage(root)
    qualified = set(private_result["qualified_accessions"])
    by_accession: dict[str, set[str]] = defaultdict(set)
    file_dates: dict[str, set[str]] = defaultdict(set)
    for record in collection["records"]:
        accession = str(record["accession"])
        if accession in qualified:
            by_accession[accession].add(str(record["selected_cik"]))
            file_dates[accession].add(str(record["file_date"]))
    if (
        set(by_accession) != qualified
        or any(len(values) != 1 for values in by_accession.values())
        or any(len(values) != 1 for values in file_dates.values())
    ):
        raise AsrTier2aSubmissionResolutionError(
            "qualified accession identity is ambiguous"
        )
    requests_: list[dict[str, Any]] = []
    for ordinal, accession in enumerate(sorted(qualified)):
        cik = next(iter(by_accession[accession]))
        row: dict[str, Any] = {
            "ordinal": ordinal,
            "accession": accession,
            "file_date": next(iter(file_dates[accession])),
            "candidates": [
                {
                    "candidate_ordinal": 0,
                    "cik": cik,
                    "url": tier1._submission_url(cik, accession),
                }
            ],
        }
        row["request_sha256"] = hashlib.sha256(canonical_bytes(row)).hexdigest()
        requests_.append(row)
    if not (
        len(requests_) == EXPECTED_ACCESSIONS
        and sum(len(row["candidates"]) for row in requests_) == EXPECTED_CANDIDATES
    ):
        raise AsrTier2aSubmissionResolutionError(
            "qualified submission graph counts differ"
        )
    graph: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": "private-asr-tier2a-qualified-submission-graph",
        "campaign_id": tier2a.capacity.CAMPAIGN_ID,
        "candidate_id": tier2a.capacity.CANDIDATE_ID,
        "resolution_id": RESOLUTION_ID,
        "tier2a_result_sha256": result["result_sha256"],
        "tier2a_inspection_sha256": TIER2A_INSPECTION_SHA256,
        "request_count": len(requests_),
        "candidate_url_count": sum(len(row["candidates"]) for row in requests_),
        "requests": requests_,
        "market_price_values_accessed": 0,
        "returns_computed": 0,
        "market_outcomes_accessed": False,
        "broker_actions": 0,
    }
    graph["private_graph_sha256"] = self_hash(graph, "private_graph_sha256")
    path = root / PRIVATE_GRAPH_NAMESPACE / f"{graph['private_graph_sha256']}.json.gz"
    return graph, path


def build_contract(
    *, store_root: Path | None = None
) -> tuple[dict[str, Any], dict[str, Any], Path]:
    root = store_root or tier1.shared._store().root
    graph, graph_path = build_private_graph(store_root=root)
    contract: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": "outcome-blind-asr-tier2a-submission-resolution-contract",
        "campaign_id": tier2a.capacity.CAMPAIGN_ID,
        "candidate_id": tier2a.capacity.CANDIDATE_ID,
        "strategy_version": tier2a.capacity.STRATEGY_VERSION,
        "resolution_id": RESOLUTION_ID,
        "source_lineage": {
            "tier2a_result_sha256": TIER2A_RESULT_SHA256,
            "tier2a_inspection_sha256": TIER2A_INSPECTION_SHA256,
            "document_semantic_candidate_count": 508,
            "qualified_accession_count": EXPECTED_ACCESSIONS,
        },
        "request_contract": {
            "provider": "SEC_EDGAR_ARCHIVES",
            "endpoint_kind": "complete accession submission",
            "candidate_order": "inspected matched-document selected CIK",
            "request_count": EXPECTED_ACCESSIONS,
            "candidate_url_count": EXPECTED_CANDIDATES,
            "maximum_attempts_per_candidate": 3,
            "retryable_statuses": [429, 500, 502, 503, 504],
            "minimum_spacing_seconds": 0.15,
            "timeout_seconds": 30.0,
            "maximum_source_bytes": 50_000_000,
            "source_or_identity_substitution_permitted": False,
            "failure_credit": 0,
        },
        "classification_contract": {
            "semantic_contract_sha256": semantic.load_contract(
                semantic._contract_path()
            )["contract_sha256"],
            "same_window_classifier_reused_without_change": True,
            "acceptance_datetime_from_complete_submission": True,
            "issuer_cik_from_frozen_request_identity": True,
            "security_identity_still_separately_required": True,
            "formal_verified_event_count_after_this_stage": None,
        },
        "private_graph": {
            "cache_relative_path": str(graph_path.relative_to(root)),
            "private_graph_sha256": graph["private_graph_sha256"],
            "file_sha256": None,
            "bytes": None,
        },
        "access_contract": {
            "provider_access_before_independent_inspection_permitted": False,
            "provider_access_after_independent_inspection_permitted": True,
            "local_semantics_before_collection_inspection_permitted": False,
            "security_identity_access_permitted": False,
            "market_price_access_permitted": False,
            "forward_return_access_permitted": False,
            "broker_actions_permitted": False,
            "maturity_effect": "NONE",
        },
        "implementation_hashes": {
            relative: file_hash(PROJECT_ROOT / relative)
            for relative in (
                "asr_tier2a_submission_resolution.py",
                "asr_tier2a_submission_resolution_inspection.py",
            )
        },
        "precise_semantic_event_count": None,
        "verified_event_count": None,
        "market_price_values_accessed": 0,
        "returns_computed": 0,
        "market_outcomes_accessed": False,
        "broker_actions": 0,
    }
    contract["contract_sha256"] = self_hash(contract, "contract_sha256")
    return contract, graph, graph_path


def _contract_path() -> Path:
    matches = sorted(DEFAULT_CONTRACT_ROOT.glob("*.json"))
    if len(matches) != 1:
        raise AsrTier2aSubmissionResolutionError(
            "expected exactly one resolution contract"
        )
    return matches[0]


def load_contract(path: Path) -> dict[str, Any]:
    value = read_object(path)
    digest = value.get("contract_sha256")
    if not (
        isinstance(digest, str)
        and digest == self_hash(value, "contract_sha256")
        and path.name == f"{tier2a.capacity.CANDIDATE_ID}-{digest}.json"
    ):
        raise AsrTier2aSubmissionResolutionError(
            "resolution contract was mutated or renamed"
        )
    return value


def freeze(
    *,
    output_root: Path = DEFAULT_CONTRACT_ROOT,
    status_path: Path = DEFAULT_CONTRACT_STATUS,
    store_root: Path | None = None,
) -> tuple[Path, dict[str, Any]]:
    root = store_root or tier1.shared._store().root
    contract, graph, graph_path = build_contract(store_root=root)
    raw = _write_gzip(graph, graph_path)
    contract["private_graph"]["file_sha256"] = hashlib.sha256(raw).hexdigest()
    contract["private_graph"]["bytes"] = len(raw)
    contract["contract_sha256"] = self_hash(contract, "contract_sha256")
    path = output_root / (
        f"{tier2a.capacity.CANDIDATE_ID}-{contract['contract_sha256']}.json"
    )
    write_object(contract, path)
    write_object(
        {
            "schema_version": 1,
            "campaign_id": tier2a.capacity.CAMPAIGN_ID,
            "candidate_id": tier2a.capacity.CANDIDATE_ID,
            "resolution_id": RESOLUTION_ID,
            "contract_sha256": contract["contract_sha256"],
            "status": "RESOLUTION_CONTRACT_PENDING_INSPECTION",
            "provider_access_permitted": False,
            "local_semantics_permitted": False,
            "security_identity_access_permitted": False,
            "market_price_access_permitted": False,
            "outcome_access_permitted": False,
            "broker_actions_permitted": False,
            "valid": False,
        },
        status_path,
    )
    return path, contract


def _private_graph(root: Path, contract: Mapping[str, Any]) -> dict[str, Any]:
    info = contract["private_graph"]
    path = root / str(info["cache_relative_path"])
    raw = path.read_bytes() if path.is_file() else b""
    if (
        hashlib.sha256(raw).hexdigest() != info["file_sha256"]
        or len(raw) != info["bytes"]
    ):
        raise AsrTier2aSubmissionResolutionError("private resolution graph drifted")
    graph = _read_gzip(path)
    if graph.get("private_graph_sha256") != info["private_graph_sha256"]:
        raise AsrTier2aSubmissionResolutionError(
            "private resolution graph hash differs"
        )
    return graph


def _authority(
    root: Path,
) -> tuple[Path, dict[str, Any], dict[str, Any], dict[str, Any]]:
    path = _contract_path()
    contract = load_contract(path)
    status = read_object(DEFAULT_CONTRACT_STATUS)
    graph = _private_graph(root, contract)
    if not (
        status.get("status") == "RESOLUTION_CONTRACT_INSPECTED"
        and status.get("contract_sha256") == contract["contract_sha256"]
        and status.get("inspection_sha256") == self_hash(status, "inspection_sha256")
        and status.get("provider_access_permitted") is True
        and status.get("local_semantics_permitted") is False
        and status.get("security_identity_access_permitted") is False
        and status.get("market_price_access_permitted") is False
        and status.get("outcome_access_permitted") is False
        and status.get("broker_actions_permitted") is False
        and status.get("valid") is True
        and graph.get("request_count") == EXPECTED_ACCESSIONS
    ):
        raise AsrTier2aSubmissionResolutionError(
            "resolution contract is not independently inspected"
        )
    return path, contract, status, graph


def _cache_path(root: Path, url: str) -> Path:
    return root / RAW_NAMESPACE / f"{hashlib.sha256(url.encode()).hexdigest()}.txt"


def collect(
    *,
    status_path: Path = DEFAULT_COLLECTION_STATUS,
    require_published: bool = True,
    session: requests.Session | None = None,
    store_root: Path | None = None,
) -> dict[str, Any]:
    root = store_root or tier1.shared._store().root
    contract_path, contract, inspection, graph = _authority(root)
    publication = (
        {
            "collector": _published(Path(__file__).resolve()),
            "contract": _published(contract_path),
            "contract_inspection": _published(DEFAULT_CONTRACT_STATUS),
        }
        if require_published
        else {}
    )
    config = SecConfig.from_env(submissions.ENV_PATH, root / RAW_NAMESPACE, workers=1)
    client = session or requests.Session()
    client.headers.update(
        {"User-Agent": config.user_agent, "Accept-Encoding": "gzip, deflate"}
    )
    telemetry: dict[str, Any] = {
        "requests": 0,
        "request_seconds": 0.0,
        "pacing_wait_seconds": 0.0,
        "cache_hits": 0,
        "downloads": 0,
        "download_bytes": 0,
        "transport_failures": 0,
        "retryable_http_failures": 0,
        "terminal_http_candidates": 0,
        "retry_attempts": 0,
        "retry_wait_seconds": 0.0,
    }
    next_at = 0.0

    def pace() -> None:
        nonlocal next_at
        now = time.monotonic()
        delay = max(0.0, next_at - now)
        if delay:
            time.sleep(delay)
            telemetry["pacing_wait_seconds"] += delay
        next_at = max(now, next_at) + float(
            contract["request_contract"]["minimum_spacing_seconds"]
        )

    records: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    attempted = 0
    for request in graph["requests"]:
        attempts: list[dict[str, Any]] = []
        selected: dict[str, Any] | None = None
        for candidate in request["candidates"]:
            attempted += 1
            url = str(candidate["url"])
            path = _cache_path(root, url)
            state, raw, status = submissions._request_candidate(
                client,
                url,
                path,
                contract=contract,
                pace=pace,
                telemetry=telemetry,
            )
            attempts.append(
                {
                    "candidate_ordinal": candidate["candidate_ordinal"],
                    "url_sha256": hashlib.sha256(url.encode()).hexdigest(),
                    "state": state,
                    "http_status": status,
                }
            )
            if state == "SUCCESS" and raw is not None:
                match = submissions.ACCEPTANCE_PATTERN.search(raw)
                if match is None:
                    raise AsrTier2aSubmissionResolutionError(
                        "accepted source lacks exact acceptance datetime"
                    )
                selected = {
                    "ordinal": request["ordinal"],
                    "request_sha256": request["request_sha256"],
                    "accession": request["accession"],
                    "file_date": request["file_date"],
                    "selected_cik": candidate["cik"],
                    "acceptance_datetime_raw": match.group(1).decode(),
                    "source_cache_relative_path": str(path.relative_to(root)),
                    "source_sha256": hashlib.sha256(raw).hexdigest(),
                    "source_bytes": len(raw),
                    "candidate_attempts": attempts,
                }
                break
        if selected is None:
            failures.append(
                {
                    "ordinal": request["ordinal"],
                    "request_sha256": request["request_sha256"],
                    "accession": request["accession"],
                    "candidate_attempts": attempts,
                    "terminal_reason": "NO_VALID_COMPLETE_SUBMISSION_CANDIDATE",
                }
            )
        else:
            records.append(selected)
    private: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": "private-asr-tier2a-submission-collection",
        "contract_sha256": contract["contract_sha256"],
        "private_graph_sha256": graph["private_graph_sha256"],
        "request_count": len(graph["requests"]),
        "success_count": len(records),
        "failure_count": len(failures),
        "attempted_candidate_count": attempted,
        "records": records,
        "failures": failures,
        "semantic_classified_count": 0,
        "verified_event_count": None,
        "market_outcomes_accessed": False,
        "broker_actions": 0,
    }
    private["private_collection_sha256"] = self_hash(
        private, "private_collection_sha256"
    )
    private_path = (
        root / PRIVATE_COLLECTION_NAMESPACE / f"{contract['contract_sha256']}.json.gz"
    )
    private_raw = _write_gzip(private, private_path)
    result: dict[str, Any] = {
        "schema_version": 1,
        "collection_kind": "outcome-blind-asr-tier2a-complete-submissions",
        "campaign_id": tier2a.capacity.CAMPAIGN_ID,
        "candidate_id": tier2a.capacity.CANDIDATE_ID,
        "resolution_id": RESOLUTION_ID,
        "contract_sha256": contract["contract_sha256"],
        "contract_inspection_sha256": inspection["inspection_sha256"],
        "publication": publication,
        "request_count": len(graph["requests"]),
        "success_count": len(records),
        "failure_count": len(failures),
        "attempted_candidate_count": attempted,
        "source_bytes": sum(int(row["source_bytes"]) for row in records),
        "private_collection": {
            "cache_relative_path": str(private_path.relative_to(root)),
            "private_collection_sha256": private["private_collection_sha256"],
            "file_sha256": hashlib.sha256(private_raw).hexdigest(),
            "bytes": len(private_raw),
        },
        "provider_telemetry": telemetry,
        "state": "RESOLUTION_SUBMISSIONS_PENDING_INSPECTION",
        "local_semantics_permitted": False,
        "security_identity_access_permitted": False,
        "market_price_access_permitted": False,
        "verified_event_count": None,
        "market_outcomes_accessed": False,
        "broker_actions": 0,
        "maturity_effect": "NONE",
        "valid": True,
    }
    result["collection_sha256"] = self_hash(result, "collection_sha256")
    write_object(result, status_path)
    return result


def load_private_collection(
    root: Path,
) -> tuple[dict[str, Any], dict[str, Any]]:
    status = read_object(DEFAULT_COLLECTION_STATUS)
    info = status["private_collection"]
    path = root / str(info["cache_relative_path"])
    raw = path.read_bytes() if path.is_file() else b""
    if (
        hashlib.sha256(raw).hexdigest() != info["file_sha256"]
        or len(raw) != info["bytes"]
    ):
        raise AsrTier2aSubmissionResolutionError(
            "private submission collection drifted"
        )
    private = _read_gzip(path)
    if private.get("private_collection_sha256") != info["private_collection_sha256"]:
        raise AsrTier2aSubmissionResolutionError(
            "private submission collection hash differs"
        )
    return status, private


def rebuild_result(private: Mapping[str, Any], root: Path) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    events: list[dict[str, Any]] = []
    for record in private["records"]:
        raw = (root / str(record["source_cache_relative_path"])).read_bytes()
        if (
            hashlib.sha256(raw).hexdigest() != record["source_sha256"]
            or len(raw) != record["source_bytes"]
        ):
            raise AsrTier2aSubmissionResolutionError(
                "complete submission drifted during classification"
            )
        candidates, terminal = semantic.classify_submission(
            raw,
            acceptance_datetime=str(record["acceptance_datetime_raw"]),
            cik=str(record["selected_cik"]),
            accession=str(record["accession"]),
        )
        rows.append(
            {
                "ordinal": record["ordinal"],
                "accession": record["accession"],
                "acceptance_datetime_raw": record["acceptance_datetime_raw"],
                "terminal_reason": terminal,
                "candidate_key_count": len(candidates),
            }
        )
        events.extend(candidates)
    for failure in private["failures"]:
        rows.append(
            {
                "ordinal": failure["ordinal"],
                "accession": failure["accession"],
                "acceptance_datetime_raw": None,
                "terminal_reason": "SOURCE_FAILURE_UNRESOLVED_ZERO_CREDIT",
                "candidate_key_count": 0,
            }
        )
    deduplicated: dict[tuple[str, str, int], dict[str, Any]] = {}
    for event in events:
        key = (
            str(event["issuer_cik"]),
            str(event["agreement_date"]),
            int(event["committed_notional_dollars"]),
        )
        if key not in deduplicated or (
            str(event["acceptance_datetime_raw"]),
            str(event["accession"]),
        ) < (
            str(deduplicated[key]["acceptance_datetime_raw"]),
            str(deduplicated[key]["accession"]),
        ):
            deduplicated[key] = event
    terminals = Counter(row["terminal_reason"] for row in rows)
    result: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": "private-asr-tier2a-precise-semantic-result",
        "rows": sorted(rows, key=lambda row: int(row["ordinal"])),
        "events": [deduplicated[key] for key in sorted(deduplicated)],
        "terminal_counts": dict(sorted(terminals.items())),
        "precise_semantic_event_count": len(deduplicated),
        "security_identity_resolution_complete": False,
        "verified_event_count": None,
        "market_outcomes_accessed": False,
        "broker_actions": 0,
    }
    result["private_result_sha256"] = self_hash(result, "private_result_sha256")
    return result


def evaluate(
    *,
    result_root: Path = DEFAULT_RESULT_ROOT,
    store_root: Path | None = None,
) -> tuple[Path, dict[str, Any]]:
    root = store_root or tier1.shared._store().root
    collection_status, private = load_private_collection(root)
    inspection = read_object(DEFAULT_COLLECTION_INSPECTION)
    if not (
        inspection.get("collection_sha256") == collection_status["collection_sha256"]
        and inspection.get("local_semantics_permitted") is True
        and inspection.get("security_identity_access_permitted") is False
        and inspection.get("market_price_access_permitted") is False
        and inspection.get("market_outcomes_accessed") is False
    ):
        raise AsrTier2aSubmissionResolutionError(
            "submission collection is not independently inspected"
        )
    private_result = rebuild_result(private, root)
    private_path = (
        root
        / PRIVATE_RESULT_NAMESPACE
        / f"{private_result['private_result_sha256']}.json.gz"
    )
    private_raw = _write_gzip(private_result, private_path)
    result: dict[str, Any] = {
        "schema_version": 1,
        "result_kind": "outcome-blind-asr-tier2a-precise-semantic-result",
        "campaign_id": tier2a.capacity.CAMPAIGN_ID,
        "candidate_id": tier2a.capacity.CANDIDATE_ID,
        "resolution_id": RESOLUTION_ID,
        "collection_sha256": collection_status["collection_sha256"],
        "terminal_counts": private_result["terminal_counts"],
        "precise_semantic_event_count": private_result["precise_semantic_event_count"],
        "security_identity_resolution_required": True,
        "security_identity_resolution_complete": False,
        "private_result": {
            "cache_relative_path": str(private_path.relative_to(root)),
            "private_result_sha256": private_result["private_result_sha256"],
            "file_sha256": hashlib.sha256(private_raw).hexdigest(),
            "bytes": len(private_raw),
        },
        "verified_event_count": None,
        "market_price_values_accessed": 0,
        "returns_computed": 0,
        "market_outcomes_accessed": False,
        "broker_actions": 0,
        "maturity_effect": "NONE",
        "state": "PRECISE_SEMANTIC_RESULT_PENDING_INSPECTION",
        "valid": True,
    }
    result["result_sha256"] = self_hash(result, "result_sha256")
    path = result_root / (
        f"{tier2a.capacity.CANDIDATE_ID}-{result['result_sha256']}.json"
    )
    write_object(result, path)
    return path, result


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("freeze", "collect", "evaluate", "status"))
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "freeze":
            path, value = freeze()
            result: dict[str, Any] = {
                "written": str(path.relative_to(PROJECT_ROOT)),
                "contract_sha256": value["contract_sha256"],
                "request_count": EXPECTED_ACCESSIONS,
                "provider_access_permitted": False,
                "market_outcomes_accessed": False,
            }
        elif args.command == "collect":
            result = collect()
        elif args.command == "evaluate":
            path, value = evaluate()
            result = {**value, "written": str(path.relative_to(PROJECT_ROOT))}
        else:
            result = read_object(DEFAULT_COLLECTION_STATUS)
    except (
        AsrTier2aSubmissionResolutionError,
        OSError,
        requests.RequestException,
        subprocess.CalledProcessError,
        ValueError,
    ) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}), file=sys.stderr)
        return 2
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""Run the ASR tier-2A Item 1.01 matched-document capacity lane."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import subprocess
import sys
import time
from collections import Counter
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import requests

import asr_capacity_no_pagination as capacity
import asr_capacity_no_pagination_collection as denominator
import asr_semantic_tier as semantic
import asr_submission_collection as submissions
import asr_submission_tier as tier1
from historical_discovery import SecConfig


PROJECT_ROOT = Path(__file__).resolve().parent
SCHEMA_VERSION = 1
TIER_ID = "item-1-01-current-report-matched-documents-tier-2a"
EXPECTED_HITS = 1_823
EXPECTED_ACCESSIONS = 1_273
EXPECTED_CANDIDATE_URLS = 1_830
TIER1_SEMANTIC_INSPECTION_SHA256 = (
    "ff6be084bd7a0f541c11b29a5fb45fcf733a76d237275630b4360c659244a86d"
)
TIER1_SEMANTIC_INSPECTION_PATH = (
    PROJECT_ROOT / "strategy_tournament/v2/asr/semantics/tier1/results/inspections/"
    f"{capacity.CANDIDATE_ID}-semantic-tier1-"
    f"{TIER1_SEMANTIC_INSPECTION_SHA256}.json"
)
DEFAULT_CONTRACT_ROOT = PROJECT_ROOT / "strategy_tournament/v2/asr/tier2a/contracts"
DEFAULT_CONTRACT_STATUS = (
    PROJECT_ROOT / "strategy_tournament/v2/asr/tier2a/contract-status.json"
)
DEFAULT_COLLECTION_STATUS = (
    PROJECT_ROOT / "strategy_tournament/v2/asr/tier2a/collection-status.json"
)
DEFAULT_RESULT_ROOT = PROJECT_ROOT / "strategy_tournament/v2/asr/tier2a/results"
PRIVATE_GRAPH_NAMESPACE = "_derived/asr-tier2a-graph"
PRIVATE_COLLECTION_NAMESPACE = "_derived/asr-tier2a-collection"
PRIVATE_RESULT_NAMESPACE = "_derived/asr-tier2a-result"
RAW_NAMESPACE = "_sources/sec/asr-tier2a-matched-documents"
MAXIMUM_SOURCE_BYTES = 25_000_000
SEC_DENIAL_MARKERS = (
    b"Your Request Originates from an Undeclared Automated Tool",
    b"Request Rate Threshold Exceeded",
)


class AsrTier2aError(RuntimeError):
    """The tier-2A source, contract, or result is invalid."""


def canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode()


def self_hash(value: Mapping[str, Any], field: str) -> str:
    return hashlib.sha256(
        canonical_bytes({key: item for key, item in value.items() if key != field})
    ).hexdigest()


def read_object(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise AsrTier2aError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise AsrTier2aError(f"{path} must contain an object")
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
        raise AsrTier2aError(f"cannot read private artifact {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise AsrTier2aError("private artifact must contain an object")
    return value


def _lineage() -> tuple[dict[str, Any], dict[str, Any]]:
    inspected = read_object(TIER1_SEMANTIC_INSPECTION_PATH)
    collection = capacity.read_object(denominator.STATUS_PATH)
    if not (
        inspected.get("inspection_sha256") == TIER1_SEMANTIC_INSPECTION_SHA256
        and inspected.get("inspection_sha256")
        == semantic.self_hash(inspected, "inspection_sha256")
        and inspected.get("semantically_qualified_unique_event_count") == 59
        and inspected.get("tier_2_open") is True
        and inspected.get("market_outcomes_accessed") is False
        and collection.get("collection_sha256")
        == capacity.self_hash(collection, "collection_sha256")
        and collection.get("state") == "SEARCH_DENOMINATOR_COMPLETE"
        and collection.get("unique_hit_count") == 17_482
        and collection.get("market_outcomes_accessed") is False
    ):
        raise AsrTier2aError("tier-1 or denominator lineage differs")
    return inspected, collection


def _document_url(cik: str, accession: str, filename: str) -> str:
    if (
        not cik.isdigit()
        or not tier1.ACCESSION_PATTERN.fullmatch(accession)
        or not filename
        or "/" in filename
        or "\\" in filename
        or filename in {".", ".."}
    ):
        raise AsrTier2aError("matched-document identity is malformed")
    return (
        "https://www.sec.gov/Archives/edgar/data/"
        f"{int(cik)}/{accession.replace('-', '')}/{filename}"
    )


def build_private_graph(
    *, store_root: Path | None = None
) -> tuple[dict[str, Any], Path]:
    tier1_inspection, collection = _lineage()
    root = store_root or tier1.shared._store().root
    index_path = root / str(collection["private_hit_index"]["cache_relative_path"])
    index = _read_gzip(index_path)
    tier1_graph, _ = tier1.build_private_graph(store_root=root)
    excluded_accessions = {str(row["accession"]) for row in tier1_graph["requests"]}
    selected: list[dict[str, Any]] = []
    for hit in index["hits"]:
        source = hit["_source"]
        root_forms = set(source.get("root_forms", []))
        items = set(source.get("items", []))
        accession = str(source.get("adsh") or "")
        if (
            not root_forms.intersection({"8-K", "6-K"})
            or "1.01" not in items
            or accession in excluded_accessions
        ):
            continue
        identifier = str(hit["_id"])
        if ":" not in identifier:
            raise AsrTier2aError("selected hit lacks matched-document filename")
        hit_accession, filename = identifier.split(":", 1)
        if hit_accession != accession:
            raise AsrTier2aError("selected hit accession differs")
        ciks = source.get("ciks")
        if not isinstance(ciks, list) or not ciks:
            raise AsrTier2aError("selected hit lacks CIK candidates")
        candidates = [
            {
                "candidate_ordinal": ordinal,
                "cik": str(cik),
                "url": _document_url(str(cik), accession, filename),
            }
            for ordinal, cik in enumerate(ciks)
        ]
        row = {
            "ordinal": len(selected),
            "hit_id": identifier,
            "accession": accession,
            "filename": filename,
            "file_date": str(source.get("file_date") or ""),
            "form": str(source.get("form") or ""),
            "file_type": str(source.get("file_type") or ""),
            "candidates": candidates,
        }
        row["request_sha256"] = hashlib.sha256(canonical_bytes(row)).hexdigest()
        selected.append(row)
    selected.sort(key=lambda row: str(row["hit_id"]))
    for ordinal, row in enumerate(selected):
        row["ordinal"] = ordinal
        row.pop("request_sha256")
        row["request_sha256"] = hashlib.sha256(canonical_bytes(row)).hexdigest()
    if not (
        len(selected) == EXPECTED_HITS
        and len({row["accession"] for row in selected}) == EXPECTED_ACCESSIONS
        and sum(len(row["candidates"]) for row in selected) == EXPECTED_CANDIDATE_URLS
    ):
        raise AsrTier2aError("tier-2A selection counts differ")
    graph: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": "private-asr-tier2a-matched-document-graph",
        "campaign_id": capacity.CAMPAIGN_ID,
        "candidate_id": capacity.CANDIDATE_ID,
        "tier_id": TIER_ID,
        "tier1_semantic_inspection_sha256": tier1_inspection["inspection_sha256"],
        "denominator_collection_sha256": collection["collection_sha256"],
        "excluded_tier1_accession_count": len(excluded_accessions),
        "selected_hit_count": len(selected),
        "selected_accession_count": len({row["accession"] for row in selected}),
        "candidate_url_count": sum(len(row["candidates"]) for row in selected),
        "requests": selected,
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
    implementation_paths = ("asr_tier2a.py", "asr_tier2a_inspection.py")
    contract: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": "outcome-blind-asr-tier2a-contract",
        "campaign_id": capacity.CAMPAIGN_ID,
        "candidate_id": capacity.CANDIDATE_ID,
        "strategy_version": capacity.STRATEGY_VERSION,
        "tier_id": TIER_ID,
        "selection_contract": {
            "root_forms": ["6-K", "8-K"],
            "required_item": "1.01",
            "exclude_all_tier1_accessions": True,
            "selected_hit_count": EXPECTED_HITS,
            "selected_accession_count": EXPECTED_ACCESSIONS,
            "candidate_url_count": EXPECTED_CANDIDATE_URLS,
            "selection_uses_market_outcomes": False,
            "source_date_or_issuer_substitution_permitted": False,
        },
        "staged_contract": {
            "collect_exact_matched_documents_first": True,
            "complete_submission_requests_before_document_semantics": False,
            "qualified_document_requires_later_complete_submission": True,
            "remaining_items_if_combined_candidates_below_100": ["7.01", "8.01"],
        },
        "request_contract": {
            "provider": "SEC_EDGAR_ARCHIVES",
            "endpoint_kind": "exact EFTS-matched filing document",
            "candidate_order": "EFTS source CIK order",
            "maximum_attempts_per_candidate": 3,
            "retryable_statuses": [429, 500, 502, 503, 504],
            "minimum_spacing_seconds": 0.15,
            "timeout_seconds": 30.0,
            "maximum_source_bytes": MAXIMUM_SOURCE_BYTES,
            "provider_substitution_permitted": False,
        },
        "semantic_contract": {
            "classifier_contract_sha256": semantic.load_contract(
                semantic._contract_path()
            )["contract_sha256"],
            "same_window_classifier_reused_without_change": True,
            "file_date_end_of_day_used_only_for_document_prefilter": True,
            "document_candidate_is_not_verified_event": True,
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
            "local_document_semantics_before_collection_inspection_permitted": False,
            "complete_submission_access_permitted": False,
            "market_price_access_permitted": False,
            "forward_return_access_permitted": False,
            "broker_actions_permitted": False,
            "maturity_effect": "NONE",
        },
        "implementation_hashes": {
            path: tier1.file_hash(PROJECT_ROOT / path) for path in implementation_paths
        },
        "document_semantic_candidate_count": None,
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
        raise AsrTier2aError("expected exactly one tier-2A contract")
    return matches[0]


def load_contract(path: Path) -> dict[str, Any]:
    value = read_object(path)
    digest = value.get("contract_sha256")
    if not (
        isinstance(digest, str)
        and digest == self_hash(value, "contract_sha256")
        and path.name == f"{capacity.CANDIDATE_ID}-tier2a-{digest}.json"
    ):
        raise AsrTier2aError("tier-2A contract was mutated or renamed")
    return value


def freeze_contract(
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
        f"{capacity.CANDIDATE_ID}-tier2a-{contract['contract_sha256']}.json"
    )
    if path.exists() and read_object(path) != contract:
        raise AsrTier2aError("content-addressed tier-2A contract differs")
    write_object(contract, path)
    write_object(
        {
            "schema_version": 1,
            "campaign_id": capacity.CAMPAIGN_ID,
            "candidate_id": capacity.CANDIDATE_ID,
            "tier_id": TIER_ID,
            "contract_sha256": contract["contract_sha256"],
            "status": "TIER2A_CONTRACT_PENDING_INSPECTION",
            "provider_access_permitted": False,
            "local_document_semantics_permitted": False,
            "complete_submission_access_permitted": False,
            "market_price_access_permitted": False,
            "outcome_access_permitted": False,
            "broker_actions_permitted": False,
            "valid": False,
        },
        status_path,
    )
    return path, contract


def _private_graph(root: Path, contract: Mapping[str, Any]) -> dict[str, Any]:
    path = root / str(contract["private_graph"]["cache_relative_path"])
    raw = path.read_bytes() if path.is_file() else b""
    if (
        hashlib.sha256(raw).hexdigest() != contract["private_graph"]["file_sha256"]
        or len(raw) != contract["private_graph"]["bytes"]
    ):
        raise AsrTier2aError("private tier-2A graph drifted")
    graph = _read_gzip(path)
    if (
        graph.get("private_graph_sha256")
        != contract["private_graph"]["private_graph_sha256"]
    ):
        raise AsrTier2aError("private tier-2A graph hash differs")
    return graph


def _load_collection_authority(
    root: Path,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    contract = load_contract(_contract_path())
    status = read_object(DEFAULT_CONTRACT_STATUS)
    graph = _private_graph(root, contract)
    if not (
        status.get("status") == "TIER2A_CONTRACT_INSPECTED"
        and status.get("contract_sha256") == contract["contract_sha256"]
        and status.get("inspection_sha256") == self_hash(status, "inspection_sha256")
        and status.get("provider_access_permitted") is True
        and status.get("local_document_semantics_permitted") is False
        and status.get("complete_submission_access_permitted") is False
        and status.get("market_price_access_permitted") is False
        and status.get("outcome_access_permitted") is False
        and status.get("broker_actions_permitted") is False
        and status.get("valid") is True
    ):
        raise AsrTier2aError("tier-2A contract is not independently inspected")
    return contract, status, graph


def _cache_path(root: Path, url: str) -> Path:
    return root / RAW_NAMESPACE / f"{hashlib.sha256(url.encode()).hexdigest()}.bin"


def collect(
    *,
    status_path: Path = DEFAULT_COLLECTION_STATUS,
    require_published: bool = True,
    session: requests.Session | None = None,
    store_root: Path | None = None,
) -> dict[str, Any]:
    root = store_root or tier1.shared._store().root
    contract, inspection, graph = _load_collection_authority(root)
    publication = (
        {
            "collector": tier1.shared._published(Path(__file__).resolve()),
            "contract": tier1.shared._published(_contract_path()),
            "contract_inspection": tier1.shared._published(DEFAULT_CONTRACT_STATUS),
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
        "cache_hits": 0,
        "downloads": 0,
        "download_bytes": 0,
        "request_seconds": 0.0,
        "pacing_wait_seconds": 0.0,
        "retry_attempts": 0,
        "failures": 0,
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
    attempts = 0
    retryable = set(contract["request_contract"]["retryable_statuses"])
    for request in graph["requests"]:
        candidate_states: list[dict[str, Any]] = []
        selected: dict[str, Any] | None = None
        for candidate in request["candidates"]:
            attempts += 1
            url = str(candidate["url"])
            path = _cache_path(root, url)
            raw: bytes | None = None
            state = "UNKNOWN"
            status_code: int | None = None
            if path.is_file():
                telemetry["cache_hits"] += 1
                raw = path.read_bytes()
                state = "SUCCESS"
                status_code = 200
            else:
                maximum_attempts = int(
                    contract["request_contract"]["maximum_attempts_per_candidate"]
                )
                for attempt in range(maximum_attempts):
                    pace()
                    started = time.monotonic()
                    try:
                        response = client.get(
                            url,
                            timeout=float(
                                contract["request_contract"]["timeout_seconds"]
                            ),
                        )
                        telemetry["requests"] += 1
                        telemetry["request_seconds"] += time.monotonic() - started
                        status_code = int(response.status_code)
                    except requests.RequestException:
                        status_code = None
                        if attempt + 1 == maximum_attempts:
                            state = "TRANSPORT_FAILURE"
                            break
                        telemetry["retry_attempts"] += 1
                        time.sleep(float(2**attempt))
                        continue
                    if status_code == 200:
                        raw = response.content
                        if (
                            not raw
                            or len(raw) > MAXIMUM_SOURCE_BYTES
                            or any(marker in raw for marker in SEC_DENIAL_MARKERS)
                        ):
                            raw = None
                            state = "MALFORMED_RESPONSE"
                        else:
                            path.parent.mkdir(parents=True, exist_ok=True)
                            temporary = path.with_name(
                                f".{path.name}.{os.getpid()}.tmp"
                            )
                            try:
                                temporary.write_bytes(raw)
                                os.replace(temporary, path)
                            finally:
                                temporary.unlink(missing_ok=True)
                            telemetry["downloads"] += 1
                            telemetry["download_bytes"] += len(raw)
                            state = "SUCCESS"
                        break
                    if status_code in retryable and attempt + 1 < maximum_attempts:
                        telemetry["retry_attempts"] += 1
                        time.sleep(float(2**attempt))
                        continue
                    state = f"HTTP_{status_code}"
                    break
            candidate_states.append(
                {
                    "candidate_ordinal": candidate["candidate_ordinal"],
                    "url_sha256": hashlib.sha256(url.encode()).hexdigest(),
                    "state": state,
                    "http_status": status_code,
                }
            )
            if state == "SUCCESS" and raw is not None:
                selected = {
                    "ordinal": request["ordinal"],
                    "request_sha256": request["request_sha256"],
                    "hit_id": request["hit_id"],
                    "accession": request["accession"],
                    "file_date": request["file_date"],
                    "selected_cik": candidate["cik"],
                    "source_cache_relative_path": str(path.relative_to(root)),
                    "source_sha256": hashlib.sha256(raw).hexdigest(),
                    "source_bytes": len(raw),
                    "candidate_states": candidate_states,
                }
                break
        if selected is None:
            failures.append(
                {
                    "ordinal": request["ordinal"],
                    "request_sha256": request["request_sha256"],
                    "hit_id": request["hit_id"],
                    "accession": request["accession"],
                    "candidate_states": candidate_states,
                    "terminal_reason": "NO_VALID_MATCHED_DOCUMENT_CANDIDATE",
                }
            )
            telemetry["failures"] += 1
        else:
            records.append(selected)
    private: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": "private-asr-tier2a-document-collection",
        "contract_sha256": contract["contract_sha256"],
        "private_graph_sha256": graph["private_graph_sha256"],
        "request_count": len(graph["requests"]),
        "success_count": len(records),
        "failure_count": len(failures),
        "attempted_candidate_count": attempts,
        "records": records,
        "failures": failures,
        "document_semantic_classified_count": 0,
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
        "collection_kind": "outcome-blind-asr-tier2a-matched-documents",
        "campaign_id": capacity.CAMPAIGN_ID,
        "candidate_id": capacity.CANDIDATE_ID,
        "tier_id": TIER_ID,
        "contract_sha256": contract["contract_sha256"],
        "contract_inspection_sha256": inspection["inspection_sha256"],
        "publication": publication,
        "request_count": len(graph["requests"]),
        "success_count": len(records),
        "failure_count": len(failures),
        "attempted_candidate_count": attempts,
        "source_bytes": sum(int(row["source_bytes"]) for row in records),
        "private_collection": {
            "cache_relative_path": str(private_path.relative_to(root)),
            "private_collection_sha256": private["private_collection_sha256"],
            "file_sha256": hashlib.sha256(private_raw).hexdigest(),
            "bytes": len(private_raw),
        },
        "provider_telemetry": telemetry,
        "state": "TIER2A_DOCUMENTS_PENDING_INSPECTION",
        "local_document_semantics_permitted": False,
        "complete_submission_access_permitted": False,
        "document_semantic_candidate_count": None,
        "verified_event_count": None,
        "market_price_values_accessed": 0,
        "returns_computed": 0,
        "market_outcomes_accessed": False,
        "broker_actions": 0,
        "maturity_effect": "NONE",
        "valid": True,
    }
    result["collection_sha256"] = self_hash(result, "collection_sha256")
    write_object(result, status_path)
    return result


def _load_private_collection(root: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    status = read_object(DEFAULT_COLLECTION_STATUS)
    info = status["private_collection"]
    path = root / str(info["cache_relative_path"])
    raw = path.read_bytes() if path.is_file() else b""
    if (
        hashlib.sha256(raw).hexdigest() != info["file_sha256"]
        or len(raw) != info["bytes"]
    ):
        raise AsrTier2aError("private tier-2A collection drifted")
    private = _read_gzip(path)
    if private.get("private_collection_sha256") != info["private_collection_sha256"]:
        raise AsrTier2aError("private tier-2A collection hash differs")
    return status, private


def rebuild_semantic_result(private: Mapping[str, Any], root: Path) -> dict[str, Any]:
    """Reclassify every retained document and every frozen failure."""
    rows: list[dict[str, Any]] = []
    events: list[dict[str, Any]] = []
    for record in private["records"]:
        raw = (root / record["source_cache_relative_path"]).read_bytes()
        if (
            hashlib.sha256(raw).hexdigest() != record["source_sha256"]
            or len(raw) != record["source_bytes"]
        ):
            raise AsrTier2aError("tier-2A source drifted during classification")
        acceptance_proxy = str(record["file_date"]).replace("-", "") + "235959"
        candidates, terminal = semantic.classify_submission(
            raw,
            acceptance_datetime=acceptance_proxy,
            cik=str(record["selected_cik"]),
            accession=str(record["accession"]),
        )
        rows.append(
            {
                "ordinal": record["ordinal"],
                "hit_id": record["hit_id"],
                "terminal_reason": terminal,
                "candidate_key_count": len(candidates),
            }
        )
        events.extend(candidates)
    for failure in private["failures"]:
        rows.append(
            {
                "ordinal": failure["ordinal"],
                "hit_id": failure["hit_id"],
                "terminal_reason": "SOURCE_FAILURE_UNRESOLVED_ZERO_CREDIT",
                "candidate_key_count": 0,
            }
        )
    unique_keys = {
        (
            str(event["issuer_cik"]),
            str(event["agreement_date"]),
            int(event["committed_notional_dollars"]),
        )
        for event in events
    }
    qualified_accessions = {str(event["accession"]) for event in events}
    terminal_counts = Counter(row["terminal_reason"] for row in rows)
    result: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": "private-asr-tier2a-document-semantic-result",
        "rows": sorted(rows, key=lambda row: row["ordinal"]),
        "candidate_event_keys": [list(key) for key in sorted(unique_keys)],
        "qualified_accessions": sorted(qualified_accessions),
        "terminal_counts": dict(sorted(terminal_counts.items())),
        "document_semantic_candidate_count": len(unique_keys),
        "qualified_accession_count": len(qualified_accessions),
        "precise_acceptance_resolution_complete": False,
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
    contract_status = read_object(DEFAULT_CONTRACT_STATUS)
    collection_status, private = _load_private_collection(root)
    inspection_path = result_root / "collection-inspection.json"
    collection_inspection = read_object(inspection_path)
    if not (
        collection_inspection.get("collection_sha256")
        == collection_status["collection_sha256"]
        and collection_inspection.get("local_document_semantics_permitted") is True
        and collection_inspection.get("complete_submission_access_permitted") is False
        and collection_inspection.get("market_outcomes_accessed") is False
        and contract_status.get("market_price_access_permitted") is False
    ):
        raise AsrTier2aError("tier-2A collection is not independently inspected")
    private_result = rebuild_semantic_result(private, root)
    private_path = (
        root
        / PRIVATE_RESULT_NAMESPACE
        / f"{private_result['private_result_sha256']}.json.gz"
    )
    raw = _write_gzip(private_result, private_path)
    result: dict[str, Any] = {
        "schema_version": 1,
        "result_kind": "outcome-blind-asr-tier2a-document-semantic-result",
        "campaign_id": capacity.CAMPAIGN_ID,
        "candidate_id": capacity.CANDIDATE_ID,
        "tier_id": TIER_ID,
        "collection_sha256": collection_status["collection_sha256"],
        "terminal_counts": private_result["terminal_counts"],
        "document_semantic_candidate_count": private_result[
            "document_semantic_candidate_count"
        ],
        "qualified_accession_count": private_result["qualified_accession_count"],
        "combined_tier1_plus_tier2a_candidate_ceiling": (
            59 + private_result["document_semantic_candidate_count"]
        ),
        "precise_acceptance_resolution_required": bool(
            private_result["qualified_accession_count"]
        ),
        "precise_acceptance_resolution_complete": False,
        "private_result": {
            "cache_relative_path": str(private_path.relative_to(root)),
            "private_result_sha256": private_result["private_result_sha256"],
            "file_sha256": hashlib.sha256(raw).hexdigest(),
            "bytes": len(raw),
        },
        "verified_event_count": None,
        "market_price_values_accessed": 0,
        "returns_computed": 0,
        "market_outcomes_accessed": False,
        "broker_actions": 0,
        "maturity_effect": "NONE",
        "state": "TIER2A_SEMANTIC_RESULT_PENDING_INSPECTION",
        "valid": True,
    }
    result["result_sha256"] = self_hash(result, "result_sha256")
    output = result_root / (
        f"{capacity.CANDIDATE_ID}-tier2a-{result['result_sha256']}.json"
    )
    write_object(result, output)
    return output, result


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("freeze", "collect", "evaluate", "status"))
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "freeze":
            path, value = freeze_contract()
            result: dict[str, Any] = {
                "written": str(path.relative_to(PROJECT_ROOT)),
                "contract_sha256": value["contract_sha256"],
                "selected_hit_count": EXPECTED_HITS,
                "provider_access_permitted": False,
                "market_outcomes_accessed": False,
            }
        elif args.command == "collect":
            result = collect()
        elif args.command == "evaluate":
            path, value = evaluate()
            result = {
                **value,
                "written": str(path.relative_to(PROJECT_ROOT)),
            }
        else:
            result = read_object(DEFAULT_COLLECTION_STATUS)
    except (
        AsrTier2aError,
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

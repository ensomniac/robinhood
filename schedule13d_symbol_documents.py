"""Freeze, collect, and resolve causal issuer trading-symbol documents."""

from __future__ import annotations

import argparse
import hashlib
import html
import json
import os
import re
import shutil
import subprocess
import sys
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from datetime import UTC
from pathlib import Path
from typing import Any

import schedule13d_capacity as capacity
import schedule13d_semantic_capacity as semantic
import schedule13d_symbol_submissions as submissions
import schedule13d_symbol_supplemental as supplemental
import schedule13d_symbol_timing_audit as timing
from historical_concurrency import ordered_bounded_results
from historical_discovery import (
    HistoricalDiscoveryError,
    SecClient,
    SecConfig,
    _accepted_at,
    _columnar_rows,
    _submission_recent,
)
from historical_store import DEFAULT_MIN_FREE_BYTES, HistoricalStoreConfig, sha256_file


PROJECT_ROOT = Path(__file__).resolve().parent
SCHEMA_VERSION = 1
TIMING_AUDIT_SHA256 = (
    "14ba4bb795bc36ba6f9d4a1942423023b92790c44b967a5ada9018e73017a15f"
)
TIMING_AUDIT_PATH = (
    PROJECT_ROOT
    / "strategy_tournament/v2/schedule13d/symbols/corrections/"
    f"symbol-time-correction-{TIMING_AUDIT_SHA256}.json"
)
SUPPLEMENTAL_INSPECTION_SHA256 = (
    "7095a7782ee037cb3d3ea1c86fae7f132e06963020d6c80037e7aa7c155324ca"
)
SUPPLEMENTAL_INSPECTION_PATH = (
    PROJECT_ROOT
    / "strategy_tournament/v2/schedule13d/symbols/supplemental/inspections/"
    f"{capacity.CANDIDATE_ID}-supplemental-{SUPPLEMENTAL_INSPECTION_SHA256}.json"
)
GRAPH_ROOT = (
    PROJECT_ROOT / "strategy_tournament/v2/schedule13d/symbols/documents/manifests"
)
GRAPH_STATUS_PATH = (
    PROJECT_ROOT
    / "strategy_tournament/v2/schedule13d/symbols/documents/request-status.json"
)
COLLECTION_STATUS_PATH = (
    PROJECT_ROOT
    / "strategy_tournament/v2/schedule13d/symbols/documents/collection-status.json"
)
COLLECTION_INSPECTION_ROOT = (
    PROJECT_ROOT / "strategy_tournament/v2/schedule13d/symbols/documents/inspections"
)
RESULT_ROOT = PROJECT_ROOT / "strategy_tournament/v2/schedule13d/symbols/results"
RESULT_INSPECTION_ROOT = (
    PROJECT_ROOT / "strategy_tournament/v2/schedule13d/symbols/inspections"
)
INSPECTOR_PATH = PROJECT_ROOT / "schedule13d_symbol_documents_inspection.py"
ENV_PATH = PROJECT_ROOT / ".env"
SHARED_CACHE_NAMESPACE = "_sources/sec/schedule13d-issuer-primary-documents-v1"
PRIVATE_NAMESPACE = (
    f"_derived/schedule13d_capacity/{semantic.documents.DATASET_ID}/symbols/documents"
)
PRIVATE_COLLECTION_NAME = "issuer-document-collection.json"
PRIVATE_RESULT_NAME = "symbol-resolution.json"
WORKERS = 4
MINIMUM_SPACING_SECONDS = 0.15
TIMEOUT_SECONDS = 30.0
IXBRL_SYMBOL_PATTERN = re.compile(
    r"<ix:nonNumeric\b[^>]*\bname=[\"']dei:TradingSymbol[\"'][^>]*>"
    r"(.*?)</ix:nonNumeric>",
    re.IGNORECASE | re.DOTALL,
)
XML_SYMBOL_PATTERN = re.compile(
    r"<dei:TradingSymbol\b[^>]*>(.*?)</dei:TradingSymbol>",
    re.IGNORECASE | re.DOTALL,
)
TAG_PATTERN = re.compile(r"<[^>]+>")


class Schedule13dSymbolDocumentsError(RuntimeError):
    """The issuer-document graph, collection, or symbol result is invalid."""


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode()


def _read_object(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise Schedule13dSymbolDocumentsError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise Schedule13dSymbolDocumentsError(f"{path} must contain an object")
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


def _store_config() -> HistoricalStoreConfig:
    config = HistoricalStoreConfig.from_env(ENV_PATH)
    minimum = max(DEFAULT_MIN_FREE_BYTES, config.min_free_bytes)
    if (
        config.root.resolve().is_relative_to(PROJECT_ROOT.resolve())
        or shutil.disk_usage(config.root).free < minimum
    ):
        raise Schedule13dSymbolDocumentsError(
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
        raise Schedule13dSymbolDocumentsError("SEC client pacing contract drifted")
    return value


def _private_collection_path(store_root: Path) -> Path:
    return store_root / PRIVATE_NAMESPACE / PRIVATE_COLLECTION_NAME


def _private_result_path(store_root: Path) -> Path:
    return store_root / PRIVATE_NAMESPACE / PRIVATE_RESULT_NAME


def _load_lineage() -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    correction = _read_object(TIMING_AUDIT_PATH)
    supplemental_inspection = _read_object(SUPPLEMENTAL_INSPECTION_PATH)
    state = semantic._read_gzip_object(semantic._private_result_path())
    if not (
        correction.get("audit_sha256") == TIMING_AUDIT_SHA256
        and correction.get("audit_sha256")
        == capacity.successor._self_hash(correction, "audit_sha256")
        and correction.get("corrected_partition", {}).get(
            "events_with_prior_in_main_metadata"
        )
        == 280
        and correction.get("source_scope", {}).get("url_sets_identical") is True
        and correction.get("corrected_primary_document_freeze_permitted") is True
        and correction.get("valid") is True
        and supplemental_inspection.get("inspection_sha256")
        == SUPPLEMENTAL_INSPECTION_SHA256
        and supplemental_inspection.get("inspection_sha256")
        == capacity.successor._self_hash(
            supplemental_inspection, "inspection_sha256"
        )
        and supplemental_inspection.get(
            "issuer_primary_document_request_freeze_permitted"
        )
        is True
        and supplemental_inspection.get("valid") is True
        and state.get("pending_causal_symbol_supplemental") == 317
        and state.get("market_outcomes_accessed") is False
    ):
        raise Schedule13dSymbolDocumentsError(
            "corrected symbol-source lineage is invalid"
        )
    return correction, supplemental_inspection, state


def _one_graph(pattern: str, description: str) -> tuple[Path, dict[str, Any]]:
    matches = sorted(PROJECT_ROOT.glob(pattern))
    if len(matches) != 1:
        raise Schedule13dSymbolDocumentsError(
            f"expected one {description}; found {len(matches)}"
        )
    return matches[0], _read_object(matches[0])


def _metadata_rows() -> dict[str, list[dict[str, Any]]]:
    store = _store_config()
    _path, main = _one_graph(
        "strategy_tournament/v2/schedule13d/symbols/submissions/manifests/*.json",
        "main submissions graph",
    )
    _path, older = _one_graph(
        "strategy_tournament/v2/schedule13d/symbols/supplemental/manifests/*.json",
        "historical submissions graph",
    )
    rows: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for request in main["request_contract"]["requests"]:
        payload = json.loads(
            submissions._resolve_cache_path(store.root, request).read_text(
                encoding="utf-8"
            )
        )
        rows[str(request["subject_cik"])].extend(
            dict(row) for row in _submission_recent(payload).values()
        )
    for request in older["request_contract"]["requests"]:
        payload = json.loads(
            supplemental._resolve_cache_path(store.root, request).read_text(
                encoding="utf-8"
            )
        )
        rows[str(request["subject_cik"])].extend(_columnar_rows(payload))
    return rows


def _selected_filing(
    event: Mapping[str, Any], rows: Sequence[Mapping[str, Any]]
) -> dict[str, Any] | None:
    cutoff = timing._cutoff_utc(str(event["accepted_at"]))
    candidates: list[dict[str, Any]] = []
    for row in rows:
        form = str(row.get("form") or "")
        accepted = _accepted_at(row.get("acceptanceDateTime"))
        accession = str(row.get("accessionNumber") or "").strip()
        primary = str(row.get("primaryDocument") or "").strip()
        if (
            form not in submissions.ALLOWED_FORMS
            or accepted is None
            or accepted.astimezone(UTC) > cutoff
            or not accession
            or not primary
            or Path(primary).name != primary
        ):
            continue
        candidates.append(
            {
                "form": form,
                "accepted_at_utc": accepted.astimezone(UTC).isoformat(),
                "accession": accession,
                "primary_document": primary,
            }
        )
    if not candidates:
        return None
    latest = max(str(row["accepted_at_utc"]) for row in candidates)
    return min(
        (row for row in candidates if row["accepted_at_utc"] == latest),
        key=lambda row: str(row["accession"]),
    )


def build_request_graph() -> dict[str, Any]:
    """Freeze one latest causal issuer document per recoverable pending event."""

    correction, supplemental_inspection, state = _load_lineage()
    rows = _metadata_rows()
    requests: list[dict[str, Any]] = []
    terminal_events: list[dict[str, Any]] = []
    for event in state["records"]:
        if event.get("status") != "PENDING_CAUSAL_SYMBOL_SUPPLEMENTAL":
            continue
        cik = str(event["subject_cik"])
        selected = _selected_filing(event, rows[cik])
        event_identity = {
            "event_ordinal": int(event["ordinal"]),
            "event_accession": event["accession"],
            "event_accepted_at_eastern": event["accepted_at"],
            "subject_cik": cik,
        }
        if selected is None:
            terminal_events.append(
                {**event_identity, "terminal_reason": "NO_CAUSAL_PRIOR_ISSUER_FILING"}
            )
            continue
        url = (
            "https://www.sec.gov/Archives/edgar/data/"
            f"{int(cik)}/{selected['accession'].replace('-', '')}/"
            f"{selected['primary_document']}"
        )
        request = {
            "ordinal": len(requests),
            **event_identity,
            **selected,
            "url": url,
            "shared_cache_relative_path": (
                f"{int(cik)}/{selected['accession'].replace('-', '')}/"
                f"{selected['primary_document']}"
            ),
        }
        request["request_sha256"] = hashlib.sha256(
            _canonical_bytes(request)
        ).hexdigest()
        requests.append(request)
    form_counts = Counter(str(row["form"]) for row in requests)
    if not (
        len(requests) == 281
        and len(terminal_events) == 36
        and len({row["url"] for row in requests}) == 281
        and dict(sorted(form_counts.items()))
        == {"10-K": 7, "10-Q": 33, "20-F": 6, "6-K": 26, "8-K": 209}
    ):
        raise Schedule13dSymbolDocumentsError(
            "corrected issuer-document selection denominator differs"
        )
    store = _store_config()
    sec = _sec_config(store.root)
    value: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "artifact_kind": "outcome-blind-causal-issuer-document-request-graph",
        "campaign_id": capacity.CAMPAIGN_ID,
        "candidate_id": capacity.CANDIDATE_ID,
        "strategy_version": capacity.STRATEGY_VERSION,
        "dataset_id": semantic.documents.DATASET_ID,
        "contract_sha256": semantic.documents.indexes.CONTRACT_SHA256,
        "timing_audit_sha256": correction["audit_sha256"],
        "supplemental_inspection_sha256": supplemental_inspection[
            "inspection_sha256"
        ],
        "request_contract": {
            "pending_event_count": 317,
            "causal_prior_filing_count": len(requests),
            "no_causal_prior_filing_count": len(terminal_events),
            "request_count": len(requests),
            "form_counts": dict(sorted(form_counts.items())),
            "requests": requests,
            "terminal_events": terminal_events,
            "selection": (
                "latest allowed-form acceptance at or before the event cutoff after "
                "Eastern-to-UTC conversion; lexical accession tie-break"
            ),
            "workers": WORKERS,
            "global_minimum_spacing_seconds": MINIMUM_SPACING_SECONDS,
            "maximum_requests_per_second": 1 / MINIMUM_SPACING_SECONDS,
            "timeout_seconds": TIMEOUT_SECONDS,
            "maximum_attempts": 4,
            "provider_substitution_permitted": False,
            "user_agent_sha256": hashlib.sha256(sec.user_agent.encode()).hexdigest(),
        },
        "symbol_contract": {
            "permitted_fact": "dei:TradingSymbol in the exact selected primary document",
            "unique_normalized_symbol_required": True,
            "multiple_distinct_symbols": "terminal_ambiguous",
            "missing_symbol": "terminal_missing",
            "current_or_future_symbol_mapping_permitted": False,
            "issuer_name_inference_permitted": False,
        },
        "access_contract": {
            "provider_access_before_graph_inspection_permitted": False,
            "provider_access_after_graph_inspection_permitted": True,
            "symbol_resolution_before_collection_inspection_permitted": False,
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
        "verified_event_count": 1,
        "capacity_passed": None,
        "returns_computed": 0,
        "market_outcomes_accessed": False,
    }
    value["request_graph_sha256"] = capacity.successor._self_hash(
        value, "request_graph_sha256"
    )
    return value


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
        raise Schedule13dSymbolDocumentsError(
            "issuer-document graph was mutated or renamed"
        )
    return value


def freeze_request_graph(
    *, root: Path = GRAPH_ROOT, status_path: Path = GRAPH_STATUS_PATH
) -> tuple[Path, dict[str, Any]]:
    value = build_request_graph()
    path = graph_path(value, root)
    if path.exists() and _read_object(path) != value:
        raise Schedule13dSymbolDocumentsError(
            "content-addressed issuer-document graph has other content"
        )
    _write_json(value, path)
    _write_json(
        {
            "schema_version": SCHEMA_VERSION,
            "candidate_id": capacity.CANDIDATE_ID,
            "request_graph_sha256": value["request_graph_sha256"],
            "request_count": 281,
            "status": "SYMBOL_DOCUMENT_GRAPH_PENDING_INSPECTION",
            "provider_access_permitted": False,
            "symbol_resolution_permitted": False,
            "verified_event_count": 1,
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
    return str(path.resolve().relative_to(PROJECT_ROOT))


def _published(path: Path) -> None:
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
        raise Schedule13dSymbolDocumentsError(
            f"provider or resolution input is not committed and pushed: {relative}"
        )


def _resolve_cache_path(store_root: Path, request: Mapping[str, Any]) -> Path:
    relative = Path(str(request["shared_cache_relative_path"]))
    if relative.is_absolute() or ".." in relative.parts:
        raise Schedule13dSymbolDocumentsError("issuer-document cache path is unsafe")
    return store_root / SHARED_CACHE_NAMESPACE / relative


def _load_graph_for_collection(path: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    graph = load_request_graph(path)
    status = _read_object(GRAPH_STATUS_PATH)
    if not (
        graph == build_request_graph()
        and status.get("status") == "SYMBOL_DOCUMENT_GRAPH_INSPECTED"
        and status.get("request_graph_sha256") == graph["request_graph_sha256"]
        and status.get("inspection_sha256")
        == capacity.successor._self_hash(status, "inspection_sha256")
        and status.get("provider_access_permitted") is True
        and status.get("symbol_resolution_permitted") is False
        and status.get("market_price_access_permitted") is False
        and status.get("outcome_access_permitted") is False
        and status.get("valid") is True
    ):
        raise Schedule13dSymbolDocumentsError(
            "issuer-document graph is not independently inspected"
        )
    return graph, status


def collect_documents(path: Path) -> dict[str, Any]:
    graph, inspection = _load_graph_for_collection(path)
    for published in (Path(__file__).resolve(), path, GRAPH_STATUS_PATH):
        _published(published)
    store = _store_config()
    sec_config = _sec_config(store.root)
    if hashlib.sha256(sec_config.user_agent.encode()).hexdigest() != graph[
        "request_contract"
    ]["user_agent_sha256"]:
        raise Schedule13dSymbolDocumentsError("SEC user-agent identity drifted")
    client = SecClient(sec_config)

    def collect(request: Mapping[str, Any]) -> dict[str, Any]:
        cache = _resolve_cache_path(store.root, request)
        existed = cache.is_file()
        text = client.text(str(request["url"]), cache)
        raw = cache.read_bytes()
        if not text.strip():
            raise Schedule13dSymbolDocumentsError(
                f"issuer primary document is empty: {request['url']}"
            )
        return {
            "ordinal": request["ordinal"],
            "request_sha256": request["request_sha256"],
            "source_origin": "SHARED_CACHE" if existed else "SEC_DOWNLOAD",
            "source_bytes": len(raw),
            "source_sha256": hashlib.sha256(raw).hexdigest(),
            "status": "SUCCESS",
        }

    records: list[dict[str, Any]] = []
    failures: list[dict[str, Any]] = []
    for outcome in ordered_bounded_results(
        list(graph["request_contract"]["requests"]), collect, max_workers=WORKERS
    ):
        if outcome.error is not None:
            failures.append(
                {
                    "ordinal": outcome.item["ordinal"],
                    "request_sha256": outcome.item["request_sha256"],
                    "error_type": type(outcome.error).__name__,
                    "error": str(outcome.error),
                }
            )
        elif isinstance(outcome.value, dict):
            records.append(outcome.value)
    records.sort(key=lambda row: int(row["ordinal"]))
    private: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "request_graph_sha256": graph["request_graph_sha256"],
        "collector_sha256": sha256_file(Path(__file__).resolve()),
        "request_count": 281,
        "success_count": len(records),
        "failure_count": len(failures),
        "records": records,
        "failures": failures,
        "symbols_read": 0,
        "market_outcomes_accessed": False,
        "broker_actions": 0,
    }
    private["private_collection_sha256"] = capacity.successor._self_hash(
        private, "private_collection_sha256"
    )
    private_path = _private_collection_path(store.root)
    _write_json(private, private_path)
    valid = not failures and len(records) == 281
    result: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "collection_kind": "outcome-blind-causal-issuer-document-collection",
        "campaign_id": capacity.CAMPAIGN_ID,
        "candidate_id": capacity.CANDIDATE_ID,
        "request_graph_sha256": graph["request_graph_sha256"],
        "request_graph_inspection_sha256": inspection["inspection_sha256"],
        "request_count": 281,
        "success_count": len(records),
        "failure_count": len(failures),
        "source_bytes": sum(int(row["source_bytes"]) for row in records),
        "private_collection_sha256": private["private_collection_sha256"],
        "private_collection_file_sha256": sha256_file(private_path),
        "provider_telemetry": client.stats(),
        "symbol_resolution_permitted": False,
        "symbols_read": 0,
        "verified_event_count": 1,
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
        raise Schedule13dSymbolDocumentsError(
            f"issuer-document collection has {len(failures)} failures"
        )
    return result


def _normalized_symbols(raw: bytes) -> list[str]:
    text = raw.decode("utf-8", errors="replace")
    values = [
        *IXBRL_SYMBOL_PATTERN.findall(text),
        *XML_SYMBOL_PATTERN.findall(text),
    ]
    symbols: set[str] = set()
    for value in values:
        normalized = html.unescape(TAG_PATTERN.sub("", value)).strip().upper()
        normalized = re.sub(r"\s+", "", normalized)
        if semantic.SYMBOL_PATTERN.fullmatch(normalized) and normalized not in {
            "NONE",
            "N/A",
        }:
            symbols.add(normalized)
    return sorted(symbols)


def _collection_inspection() -> tuple[Path, dict[str, Any]]:
    matches = sorted(
        COLLECTION_INSPECTION_ROOT.glob(
            f"{capacity.CANDIDATE_ID}-documents-*.json"
        )
    )
    if len(matches) != 1:
        raise Schedule13dSymbolDocumentsError(
            f"expected one issuer-document inspection; found {len(matches)}"
        )
    return matches[0], _read_object(matches[0])


def build_resolution(
    graph_path_value: Path, *, require_published: bool = True, write_private: bool = False
) -> dict[str, Any]:
    graph = load_request_graph(graph_path_value)
    inspection_path, inspection = _collection_inspection()
    collection = _read_object(COLLECTION_STATUS_PATH)
    if not (
        inspection.get("inspection_sha256")
        == capacity.successor._self_hash(inspection, "inspection_sha256")
        and inspection.get("collection_sha256") == collection.get("collection_sha256")
        and inspection.get("symbol_resolution_permitted") is True
        and inspection.get("market_outcomes_accessed") is False
        and inspection.get("valid") is True
    ):
        raise Schedule13dSymbolDocumentsError(
            "issuer-document collection is not inspected for symbol resolution"
        )
    if require_published:
        for path in (
            Path(__file__).resolve(),
            graph_path_value,
            COLLECTION_STATUS_PATH,
            inspection_path,
        ):
            _published(path)
    store = _store_config()
    records: list[dict[str, Any]] = []
    for request in graph["request_contract"]["requests"]:
        symbols = _normalized_symbols(_resolve_cache_path(store.root, request).read_bytes())
        status = (
            "RECOVERED_CAUSAL_DEI_TRADING_SYMBOL"
            if len(symbols) == 1
            else (
                "MISSING_DEI_TRADING_SYMBOL"
                if not symbols
                else "AMBIGUOUS_MULTIPLE_DEI_TRADING_SYMBOLS"
            )
        )
        records.append(
            {
                "event_ordinal": request["event_ordinal"],
                "event_accession": request["event_accession"],
                "subject_cik": request["subject_cik"],
                "issuer_filing_accession": request["accession"],
                "issuer_filing_form": request["form"],
                "symbols": symbols,
                "status": status,
                "verified_event": len(symbols) == 1,
            }
        )
    counts = Counter(str(row["status"]) for row in records)
    recovered = counts["RECOVERED_CAUSAL_DEI_TRADING_SYMBOL"]
    verified = 1 + recovered
    state: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "request_graph_sha256": graph["request_graph_sha256"],
        "collection_inspection_sha256": inspection["inspection_sha256"],
        "records": records,
        "resolution_counts": dict(sorted(counts.items())),
        "direct_event_filing_symbol_count": 1,
        "recovered_causal_symbol_count": recovered,
        "no_causal_prior_issuer_filing_count": 36,
        "verified_event_count": verified,
        "minimum_required_verified_events": capacity.MINIMUM_VERIFIED_EVENTS,
        "capacity_passed": verified >= capacity.MINIMUM_VERIFIED_EVENTS,
        "market_price_values_accessed": 0,
        "returns_computed": 0,
        "market_outcomes_accessed": False,
        "broker_actions": 0,
    }
    state["resolution_sha256"] = capacity.successor._self_hash(
        state, "resolution_sha256"
    )
    private_path = _private_result_path(store.root)
    if write_private:
        _write_json(state, private_path)
    if not private_path.is_file() or _read_object(private_path) != state:
        raise Schedule13dSymbolDocumentsError(
            "private symbol resolution is missing or differs"
        )
    passed = bool(state["capacity_passed"])
    result: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "result_kind": "outcome-blind-schedule13d-capacity-result",
        "campaign_id": capacity.CAMPAIGN_ID,
        "candidate_id": capacity.CANDIDATE_ID,
        "strategy_version": capacity.STRATEGY_VERSION,
        "contract_sha256": semantic.documents.indexes.CONTRACT_SHA256,
        "request_graph_sha256": graph["request_graph_sha256"],
        "collection_inspection_sha256": inspection["inspection_sha256"],
        "resolution_sha256": state["resolution_sha256"],
        "private_result_file_sha256": sha256_file(private_path),
        "denominator": 6852,
        "semantic_base_candidates_after_dedup_and_cooldown": 318,
        "direct_event_filing_symbol_count": 1,
        "issuer_document_request_count": 281,
        "no_causal_prior_issuer_filing_count": 36,
        "resolution_counts": state["resolution_counts"],
        "verified_event_count": verified,
        "minimum_required_verified_events": capacity.MINIMUM_VERIFIED_EVENTS,
        "capacity_passed": passed,
        "candidate_disposition": (
            "CAPACITY_SURVIVOR_STAGE0_FREEZE_REQUIRED"
            if passed
            else "RETIRED_CAPACITY_NO_PARAMETER_REPAIR"
        ),
        "next_action": (
            "freeze exact Stage 0 rules, dates, fills, exits, costs, and falsification gates"
            if passed
            else "advance to the next authorized mechanism family"
        ),
        "market_price_values_accessed": 0,
        "returns_computed": 0,
        "market_outcomes_accessed": False,
        "broker_actions": 0,
        "development_evidence_eligible": False,
        "confirmation_evidence_eligible": False,
        "maturity_effect": "NONE",
    }
    result["result_sha256"] = capacity.successor._self_hash(
        result, "result_sha256"
    )
    return result


def result_path(value: Mapping[str, Any]) -> Path:
    return RESULT_ROOT / f"{capacity.CANDIDATE_ID}-{value['result_sha256']}.json"


def _one(pattern: str, description: str) -> Path:
    matches = sorted(PROJECT_ROOT.glob(pattern))
    if len(matches) != 1:
        raise Schedule13dSymbolDocumentsError(
            f"expected one {description}; found {len(matches)}"
        )
    return matches[0]


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("freeze", "collect", "resolve", "status"))
    args = parser.parse_args(argv)
    try:
        if args.command == "freeze":
            path, value = freeze_request_graph()
            result: dict[str, Any] = {
                "written": _repo_relative(path),
                "request_graph_sha256": value["request_graph_sha256"],
                "request_count": 281,
                "provider_access_permitted": False,
                "capacity_passed": None,
            }
        elif args.command == "collect":
            graph = _one(
                "strategy_tournament/v2/schedule13d/symbols/documents/manifests/*.json",
                "issuer-document graph",
            )
            result = collect_documents(graph)
        elif args.command == "resolve":
            graph = _one(
                "strategy_tournament/v2/schedule13d/symbols/documents/manifests/*.json",
                "issuer-document graph",
            )
            value = build_resolution(graph, write_private=True)
            path = result_path(value)
            _write_json(value, path)
            result = {**value, "written": _repo_relative(path)}
        else:
            result = _read_object(COLLECTION_STATUS_PATH)
    except (
        Schedule13dSymbolDocumentsError,
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

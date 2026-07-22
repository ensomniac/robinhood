"""Inspect causal issuer-document graph, collection, and capacity result."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import schedule13d_capacity as capacity
import schedule13d_symbol_documents as documents
from historical_store import sha256_file


class Schedule13dSymbolDocumentsInspectionError(RuntimeError):
    """The issuer-document artifact does not independently rebuild."""


def inspect_graph(
    path: Path, *, status_path: Path = documents.GRAPH_STATUS_PATH
) -> dict[str, Any]:
    recorded = documents.load_request_graph(path)
    status = documents._read_object(status_path)
    pending = status.get("status") == "SYMBOL_DOCUMENT_GRAPH_PENDING_INSPECTION"
    inspected = (
        status.get("status") == "SYMBOL_DOCUMENT_GRAPH_INSPECTED"
        and status.get("inspection_sha256")
        == capacity.successor._self_hash(status, "inspection_sha256")
    )
    requests = recorded["request_contract"]["requests"]
    if not (
        recorded == documents.build_request_graph()
        and (pending or inspected)
        and status.get("request_graph_sha256") == recorded["request_graph_sha256"]
        and len(requests) == 281
        and len(recorded["request_contract"]["terminal_events"]) == 36
        and len({row["url"] for row in requests}) == 281
        and all(
            row["request_sha256"]
            == hashlib.sha256(
                documents._canonical_bytes(
                    {key: value for key, value in row.items() if key != "request_sha256"}
                )
            ).hexdigest()
            for row in requests
        )
        and recorded["timing_audit_sha256"] == documents.TIMING_AUDIT_SHA256
        and recorded["access_contract"][
            "symbol_resolution_before_collection_inspection_permitted"
        ]
        is False
        and recorded["access_contract"]["market_price_access_permitted"] is False
        and recorded["capacity_passed"] is None
        and recorded["market_outcomes_accessed"] is False
    ):
        raise Schedule13dSymbolDocumentsInspectionError(
            "issuer-document graph or zero-outcome boundary differs"
        )
    result: dict[str, Any] = {
        "schema_version": documents.SCHEMA_VERSION,
        "inspection_kind": "outcome-blind-causal-issuer-document-request-inspection",
        "campaign_id": capacity.CAMPAIGN_ID,
        "candidate_id": capacity.CANDIDATE_ID,
        "request_graph_sha256": recorded["request_graph_sha256"],
        "request_graph_file_sha256": sha256_file(path),
        "status": "SYMBOL_DOCUMENT_GRAPH_INSPECTED",
        "request_count": 281,
        "no_causal_prior_filing_count": 36,
        "exact_latest_prior_selection_rebuilt": True,
        "timing_correction_bound": True,
        "provider_access_permitted": True,
        "provider_access_scope": "the 281 exact issuer primary-document URLs only",
        "symbol_resolution_permitted": False,
        "verified_event_count": 1,
        "capacity_passed": None,
        "market_price_access_permitted": False,
        "outcome_access_permitted": False,
        "broker_actions_permitted": False,
        "returns_computed": 0,
        "market_outcomes_accessed": False,
        "maturity_effect": "NONE",
        "valid": True,
    }
    result["inspection_sha256"] = capacity.successor._self_hash(
        result, "inspection_sha256"
    )
    documents._write_json(result, status_path)
    return result


def inspect_collection(
    graph_path: Path,
    *, collection_path: Path = documents.COLLECTION_STATUS_PATH,
) -> dict[str, Any]:
    graph, graph_inspection = documents._load_graph_for_collection(graph_path)
    public = documents._read_object(collection_path)
    store = documents._store_config()
    private_path = documents._private_collection_path(store.root)
    private = documents._read_object(private_path)
    records = {int(row["ordinal"]): row for row in private["records"]}
    for request in graph["request_contract"]["requests"]:
        raw = documents._resolve_cache_path(store.root, request).read_bytes()
        record = records.get(int(request["ordinal"]))
        if not (
            record is not None
            and record.get("request_sha256") == request["request_sha256"]
            and record.get("source_bytes") == len(raw)
            and record.get("source_sha256") == hashlib.sha256(raw).hexdigest()
            and record.get("status") == "SUCCESS"
            and raw
        ):
            raise Schedule13dSymbolDocumentsInspectionError(
                f"issuer primary document drifted: {request['url']}"
            )
    if not (
        public.get("collection_sha256")
        == capacity.successor._self_hash(public, "collection_sha256")
        and private.get("private_collection_sha256")
        == capacity.successor._self_hash(private, "private_collection_sha256")
        and sha256_file(private_path) == public.get("private_collection_file_sha256")
        and public.get("request_count") == 281
        and public.get("success_count") == 281
        and public.get("failure_count") == 0
        and public.get("symbols_read") == 0
        and public.get("symbol_resolution_permitted") is False
        and public.get("capacity_passed") is None
        and public.get("market_outcomes_accessed") is False
        and public.get("valid") is True
    ):
        raise Schedule13dSymbolDocumentsInspectionError(
            "issuer-document collection does not independently rebuild"
        )
    result: dict[str, Any] = {
        "schema_version": documents.SCHEMA_VERSION,
        "inspection_kind": "outcome-blind-causal-issuer-document-collection-inspection",
        "campaign_id": capacity.CAMPAIGN_ID,
        "candidate_id": capacity.CANDIDATE_ID,
        "request_graph_sha256": graph["request_graph_sha256"],
        "request_graph_inspection_sha256": graph_inspection["inspection_sha256"],
        "collection_sha256": public["collection_sha256"],
        "collection_file_sha256": sha256_file(collection_path),
        "private_collection_sha256": private["private_collection_sha256"],
        "private_collection_file_sha256": sha256_file(private_path),
        "request_count": 281,
        "success_count": 281,
        "failure_count": 0,
        "raw_sources_rehashed": True,
        "symbol_resolution_permitted": True,
        "verified_event_count": 1,
        "capacity_passed": None,
        "market_price_values_accessed": 0,
        "returns_computed": 0,
        "market_outcomes_accessed": False,
        "broker_actions": 0,
        "maturity_effect": "NONE",
        "valid": True,
    }
    result["inspection_sha256"] = capacity.successor._self_hash(
        result, "inspection_sha256"
    )
    return result


def inspect_result(graph_path: Path, result_path: Path) -> dict[str, Any]:
    recorded = documents._read_object(result_path)
    if recorded.get("result_sha256") != capacity.successor._self_hash(
        recorded, "result_sha256"
    ):
        raise Schedule13dSymbolDocumentsInspectionError("capacity result hash invalid")
    expected = documents.build_resolution(
        graph_path, require_published=False, write_private=False
    )
    if recorded != expected:
        raise Schedule13dSymbolDocumentsInspectionError(
            "capacity result does not independently rebuild"
        )
    result: dict[str, Any] = {
        "schema_version": documents.SCHEMA_VERSION,
        "inspection_kind": "outcome-blind-schedule13d-capacity-result-inspection",
        "campaign_id": capacity.CAMPAIGN_ID,
        "candidate_id": capacity.CANDIDATE_ID,
        "contract_sha256": recorded["contract_sha256"],
        "request_graph_sha256": recorded["request_graph_sha256"],
        "collection_inspection_sha256": recorded["collection_inspection_sha256"],
        "result_sha256": recorded["result_sha256"],
        "result_file_sha256": sha256_file(result_path),
        "resolution_sha256": recorded["resolution_sha256"],
        "denominator": recorded["denominator"],
        "resolution_counts": recorded["resolution_counts"],
        "verified_event_count": recorded["verified_event_count"],
        "minimum_required_verified_events": recorded[
            "minimum_required_verified_events"
        ],
        "capacity_passed": recorded["capacity_passed"],
        "candidate_disposition": recorded["candidate_disposition"],
        "stage0_outcome_access_permitted": False,
        "market_price_values_accessed": 0,
        "returns_computed": 0,
        "market_outcomes_accessed": False,
        "broker_actions": 0,
        "maturity_effect": "NONE",
        "valid": True,
    }
    result["inspection_sha256"] = capacity.successor._self_hash(
        result, "inspection_sha256"
    )
    return result


def _one(pattern: str, description: str) -> Path:
    matches = sorted(documents.PROJECT_ROOT.glob(pattern))
    if len(matches) != 1:
        raise Schedule13dSymbolDocumentsInspectionError(
            f"expected one {description}; found {len(matches)}"
        )
    return matches[0]


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("inspect-graph", "inspect-collection", "inspect-result"))
    args = parser.parse_args(argv)
    try:
        graph = _one(
            "strategy_tournament/v2/schedule13d/symbols/documents/manifests/*.json",
            "issuer-document graph",
        )
        if args.command == "inspect-graph":
            result = inspect_graph(graph)
        elif args.command == "inspect-collection":
            result = inspect_collection(graph)
            path = documents.COLLECTION_INSPECTION_ROOT / (
                f"{capacity.CANDIDATE_ID}-documents-{result['inspection_sha256']}.json"
            )
            documents._write_json(result, path)
            result = {**result, "written": documents._repo_relative(path)}
        else:
            result_path = _one(
                "strategy_tournament/v2/schedule13d/symbols/results/*.json",
                "symbol capacity result",
            )
            result = inspect_result(graph, result_path)
            path = documents.RESULT_INSPECTION_ROOT / (
                f"{capacity.CANDIDATE_ID}-capacity-{result['inspection_sha256']}.json"
            )
            documents._write_json(result, path)
            result = {**result, "written": documents._repo_relative(path)}
    except (
        Schedule13dSymbolDocumentsInspectionError,
        documents.Schedule13dSymbolDocumentsError,
        OSError,
        ValueError,
    ) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}), file=sys.stderr)
        return 2
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

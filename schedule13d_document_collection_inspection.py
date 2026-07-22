"""Inspect Schedule 13D complete-submission request graph and collection."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import schedule13d_capacity as capacity
import schedule13d_document_collection as documents
from historical_store import sha256_file


class Schedule13dDocumentInspectionError(RuntimeError):
    """The complete-submission graph or collection does not rebuild."""


def inspect_request_graph(
    path: Path, *, status_path: Path = documents.GRAPH_STATUS_PATH
) -> dict[str, Any]:
    recorded = documents.load_request_graph(path)
    expected = documents.build_request_graph()
    status = documents._read_object(status_path)
    requests = recorded["request_contract"]["requests"]
    pending = status.get("status") == "DOCUMENT_REQUEST_GRAPH_PENDING_INSPECTION"
    inspected = (
        status.get("status") == "DOCUMENT_REQUEST_GRAPH_INSPECTED"
        and status.get("inspection_sha256")
        == capacity.successor._self_hash(status, "inspection_sha256")
    )
    for relative, digest in recorded["implementation_hashes"].items():
        if sha256_file(documents.PROJECT_ROOT / relative) != digest:
            raise Schedule13dDocumentInspectionError(
                f"document implementation drifted: {relative}"
            )
    if not (
        recorded == expected
        and (pending or inspected)
        and status.get("request_graph_sha256") == recorded["request_graph_sha256"]
        and len(requests) == 6852
        and [row["ordinal"] for row in requests] == list(range(6852))
        and len({row["url"] for row in requests}) == 6852
        and all(
            row["url"] == "https://www.sec.gov/Archives/" + row["filename"]
            for row in requests
        )
        and all(
            row["request_sha256"]
            == documents._sha256_json(
                {key: value for key, value in row.items() if key != "request_sha256"}
            )
            for row in requests
        )
        and recorded["filing_count"] == 6852
        and recorded["verified_event_count"] is None
        and recorded["access_contract"][
            "provider_access_before_graph_inspection_permitted"
        ]
        is False
        and recorded["access_contract"]["filing_semantic_classification_permitted"]
        is False
        and recorded["access_contract"]["issuer_symbol_supplemental_access_permitted"]
        is False
        and recorded["access_contract"]["market_price_access_permitted"] is False
        and recorded["access_contract"]["stage0_outcome_access_permitted"] is False
        and recorded["access_contract"]["broker_actions_permitted"] is False
        and recorded["returns_computed"] == 0
        and recorded["market_outcomes_accessed"] is False
    ):
        raise Schedule13dDocumentInspectionError(
            "document request graph, denominator, or zero-outcome boundary differs"
        )
    result: dict[str, Any] = {
        "schema_version": documents.SCHEMA_VERSION,
        "inspection_kind": "outcome-blind-sec-complete-submission-request-inspection",
        "campaign_id": capacity.CAMPAIGN_ID,
        "candidate_id": capacity.CANDIDATE_ID,
        "dataset_id": documents.DATASET_ID,
        "contract_sha256": documents.indexes.CONTRACT_SHA256,
        "index_collection_inspection_sha256": (
            documents.INDEX_COLLECTION_INSPECTION_SHA256
        ),
        "request_graph_sha256": recorded["request_graph_sha256"],
        "request_graph_file_sha256": sha256_file(path),
        "status": "DOCUMENT_REQUEST_GRAPH_INSPECTED",
        "request_count": 6852,
        "exact_accession_urls_rebuilt": True,
        "request_hashes_rebuilt": True,
        "implementation_hashes_rebuilt": True,
        "complete_denominator_rebuilt": True,
        "zero_outcome_boundary_rebuilt": True,
        "provider_access_permitted": True,
        "provider_access_scope": "the 6,852 exact complete-submission URLs only",
        "filing_semantic_classification_permitted": False,
        "issuer_symbol_supplemental_access_permitted": False,
        "verified_event_count": None,
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


def _rebuild_private_collection(
    graph: Mapping[str, Any], recorded: Mapping[str, Any]
) -> dict[str, Any]:
    store = documents._store_config()
    public_records = {
        int(row["ordinal"]): row for row in recorded.get("records", [])
    }
    records: list[dict[str, Any]] = []
    for request in graph["request_contract"]["requests"]:
        ordinal = int(request["ordinal"])
        public = public_records.get(ordinal)
        if public is None:
            raise Schedule13dDocumentInspectionError(
                f"private collection lacks ordinal {ordinal}"
            )
        path = documents._resolve_cache_path(store.root, request)
        raw = path.read_bytes()
        if not (
            public.get("request_sha256") == request["request_sha256"]
            and public.get("source_bytes") == len(raw)
            and public.get("source_sha256") == hashlib.sha256(raw).hexdigest()
            and public.get("status") == "SUCCESS"
            and raw
            and b"<SEC-DOCUMENT" in raw[:4096].upper()
        ):
            raise Schedule13dDocumentInspectionError(
                f"complete-submission source drifted at ordinal {ordinal}"
            )
        records.append(dict(public))
    result = dict(recorded)
    result["records"] = records
    return result


def inspect_collection(
    graph_path: Path,
    *, collection_path: Path = documents.COLLECTION_STATUS_PATH,
) -> dict[str, Any]:
    graph, graph_inspection = documents._load_graph_for_collection(graph_path)
    public = documents._read_object(collection_path)
    store = documents._store_config()
    private_path = documents._private_collection_path(store.root)
    private = documents._read_gzip_object(private_path)
    rebuilt = _rebuild_private_collection(graph, private)
    if not (
        public.get("collection_sha256")
        == capacity.successor._self_hash(public, "collection_sha256")
        and private == rebuilt
        and private.get("private_collection_sha256")
        == capacity.successor._self_hash(private, "private_collection_sha256")
        and sha256_file(private_path) == public.get("private_collection_file_sha256")
        and private.get("private_collection_sha256")
        == public.get("private_collection_sha256")
        and public.get("request_graph_sha256") == graph["request_graph_sha256"]
        and public.get("request_graph_inspection_sha256")
        == graph_inspection["inspection_sha256"]
        and public.get("request_count") == 6852
        and public.get("success_count") == 6852
        and public.get("failure_count") == 0
        and private.get("request_count") == 6852
        and private.get("success_count") == 6852
        and private.get("failure_count") == 0
        and public.get("source_bytes")
        == sum(int(row["source_bytes"]) for row in private["records"])
        and public.get("filing_semantic_classification_permitted") is False
        and public.get("capacity_classification_complete") is False
        and public.get("verified_event_count") is None
        and public.get("market_price_values_accessed") == 0
        and public.get("returns_computed") == 0
        and public.get("market_outcomes_accessed") is False
        and public.get("broker_actions") == 0
        and public.get("maturity_effect") == "NONE"
        and public.get("valid") is True
    ):
        raise Schedule13dDocumentInspectionError(
            "complete-submission collection does not independently rebuild"
        )
    result: dict[str, Any] = {
        "schema_version": documents.SCHEMA_VERSION,
        "inspection_kind": "outcome-blind-sec-complete-submission-collection-inspection",
        "campaign_id": capacity.CAMPAIGN_ID,
        "candidate_id": capacity.CANDIDATE_ID,
        "dataset_id": documents.DATASET_ID,
        "contract_sha256": documents.indexes.CONTRACT_SHA256,
        "request_graph_sha256": graph["request_graph_sha256"],
        "request_graph_inspection_sha256": graph_inspection["inspection_sha256"],
        "collection_sha256": public["collection_sha256"],
        "collection_file_sha256": sha256_file(collection_path),
        "private_collection_sha256": private["private_collection_sha256"],
        "private_collection_file_sha256": sha256_file(private_path),
        "request_count": 6852,
        "success_count": 6852,
        "failure_count": 0,
        "source_bytes": public["source_bytes"],
        "raw_sources_rehashed": True,
        "complete_denominator_rebuilt": True,
        "filing_semantic_classification_permitted": True,
        "issuer_symbol_supplemental_access_permitted": False,
        "capacity_classification_complete": False,
        "verified_event_count": None,
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


def collection_inspection_path(value: Mapping[str, Any]) -> Path:
    return documents.COLLECTION_INSPECTION_ROOT / (
        f"{capacity.CANDIDATE_ID}-document-collection-"
        f"{value['inspection_sha256']}.json"
    )


def _one(pattern: str, description: str) -> Path:
    matches = sorted(documents.PROJECT_ROOT.glob(pattern))
    if len(matches) != 1:
        raise Schedule13dDocumentInspectionError(
            f"expected one {description}; found {len(matches)}"
        )
    return matches[0]


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("inspect-graph", "inspect-collection"))
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        graph = _one(
            "strategy_tournament/v2/schedule13d/documents/manifests/"
            f"{capacity.CANDIDATE_ID}-*.json",
            "complete-submission request graph",
        )
        if args.command == "inspect-graph":
            result = inspect_request_graph(graph)
        else:
            result = inspect_collection(graph)
            path = collection_inspection_path(result)
            documents._write_json(result, path)
            result = {**result, "written": documents._repo_relative(path)}
    except (
        Schedule13dDocumentInspectionError,
        documents.Schedule13dDocumentCollectionError,
        OSError,
        ValueError,
    ) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}), file=sys.stderr)
        return 2
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

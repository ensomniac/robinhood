"""Inspect causal-symbol SEC issuer-submissions graph and collection."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import schedule13d_capacity as capacity
import schedule13d_symbol_submissions as submissions
from historical_store import sha256_file


class Schedule13dSymbolSubmissionsInspectionError(RuntimeError):
    """The issuer-submissions graph or collection does not rebuild."""


def inspect_request_graph(
    path: Path, *, status_path: Path = submissions.GRAPH_STATUS_PATH
) -> dict[str, Any]:
    recorded = submissions.load_request_graph(path)
    expected = submissions.build_request_graph()
    status = submissions._read_object(status_path)
    pending = status.get("status") == "SYMBOL_SUBMISSIONS_GRAPH_PENDING_INSPECTION"
    inspected = (
        status.get("status") == "SYMBOL_SUBMISSIONS_GRAPH_INSPECTED"
        and status.get("inspection_sha256")
        == capacity.successor._self_hash(status, "inspection_sha256")
    )
    requests = recorded["request_contract"]["requests"]
    for relative, digest in recorded["implementation_hashes"].items():
        if sha256_file(submissions.PROJECT_ROOT / relative) != digest:
            raise Schedule13dSymbolSubmissionsInspectionError(
                f"submissions implementation drifted: {relative}"
            )
    if not (
        recorded == expected
        and (pending or inspected)
        and status.get("request_graph_sha256") == recorded["request_graph_sha256"]
        and len(requests) == 312
        and sum(int(row["event_count"]) for row in requests) == 317
        and [row["ordinal"] for row in requests] == list(range(312))
        and len({row["subject_cik"] for row in requests}) == 312
        and len({row["url"] for row in requests}) == 312
        and all(
            row["url"]
            == "https://data.sec.gov/submissions/"
            f"CIK{int(row['subject_cik']):010d}.json"
            for row in requests
        )
        and all(
            row["request_sha256"]
            == submissions._sha256_json(
                {key: value for key, value in row.items() if key != "request_sha256"}
            )
            for row in requests
        )
        and recorded["verified_event_count"] == 1
        and recorded["capacity_passed"] is None
        and recorded["access_contract"][
            "supplemental_submission_file_access_permitted"
        ]
        is False
        and recorded["access_contract"]["issuer_primary_document_access_permitted"]
        is False
        and recorded["access_contract"]["market_price_access_permitted"] is False
        and recorded["market_outcomes_accessed"] is False
    ):
        raise Schedule13dSymbolSubmissionsInspectionError(
            "issuer-submissions graph or zero-outcome boundary differs"
        )
    result: dict[str, Any] = {
        "schema_version": submissions.SCHEMA_VERSION,
        "inspection_kind": "outcome-blind-sec-issuer-submissions-request-inspection",
        "campaign_id": capacity.CAMPAIGN_ID,
        "candidate_id": capacity.CANDIDATE_ID,
        "dataset_id": submissions.semantic.documents.DATASET_ID,
        "contract_sha256": submissions.semantic.documents.indexes.CONTRACT_SHA256,
        "semantic_inspection_sha256": submissions.SEMANTIC_INSPECTION_SHA256,
        "request_graph_sha256": recorded["request_graph_sha256"],
        "request_graph_file_sha256": sha256_file(path),
        "status": "SYMBOL_SUBMISSIONS_GRAPH_INSPECTED",
        "pending_event_count": 317,
        "request_count": 312,
        "exact_subject_cik_urls_rebuilt": True,
        "request_hashes_rebuilt": True,
        "implementation_hashes_rebuilt": True,
        "provider_access_permitted": True,
        "provider_access_scope": "the 312 exact issuer submissions URLs only",
        "supplemental_submission_file_access_permitted": False,
        "issuer_primary_document_access_permitted": False,
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
    submissions._write_json(result, status_path)
    return result


def inspect_collection(
    graph_path: Path,
    *, collection_path: Path = submissions.COLLECTION_STATUS_PATH,
) -> dict[str, Any]:
    graph, graph_inspection = submissions._load_graph_for_collection(graph_path)
    public = submissions._read_object(collection_path)
    store = submissions._store_config()
    private_path = submissions._private_collection_path(store.root)
    private = submissions._read_gzip_object(private_path)
    records_by_ordinal = {int(row["ordinal"]): row for row in private["records"]}
    for request in graph["request_contract"]["requests"]:
        ordinal = int(request["ordinal"])
        record = records_by_ordinal.get(ordinal)
        path = submissions._resolve_cache_path(store.root, request)
        raw = path.read_bytes()
        payload = json.loads(raw)
        if not (
            isinstance(payload, Mapping)
            and str(payload.get("cik") or "").lstrip("0")
            == request["subject_cik"]
            and record is not None
            and record.get("request_sha256") == request["request_sha256"]
            and record.get("source_bytes") == len(raw)
            and record.get("source_sha256") == hashlib.sha256(raw).hexdigest()
            and record.get("status") == "SUCCESS"
        ):
            raise Schedule13dSymbolSubmissionsInspectionError(
                f"issuer submissions source drifted at ordinal {ordinal}"
            )
    if not (
        public.get("collection_sha256")
        == capacity.successor._self_hash(public, "collection_sha256")
        and private.get("private_collection_sha256")
        == capacity.successor._self_hash(private, "private_collection_sha256")
        and sha256_file(private_path) == public.get("private_collection_file_sha256")
        and private.get("private_collection_sha256")
        == public.get("private_collection_sha256")
        and public.get("request_graph_sha256") == graph["request_graph_sha256"]
        and public.get("request_graph_inspection_sha256")
        == graph_inspection["inspection_sha256"]
        and public.get("pending_event_count") == 317
        and public.get("request_count") == 312
        and public.get("success_count") == 312
        and public.get("failure_count") == 0
        and public.get("supplemental_submission_file_count") is None
        and public.get("issuer_primary_document_count") is None
        and public.get("recovered_symbol_count") == 0
        and public.get("supplemental_submission_file_access_permitted") is False
        and public.get("issuer_primary_document_access_permitted") is False
        and public.get("capacity_passed") is None
        and public.get("market_price_values_accessed") == 0
        and public.get("market_outcomes_accessed") is False
        and public.get("valid") is True
    ):
        raise Schedule13dSymbolSubmissionsInspectionError(
            "issuer-submissions collection does not independently rebuild"
        )
    result: dict[str, Any] = {
        "schema_version": submissions.SCHEMA_VERSION,
        "inspection_kind": "outcome-blind-sec-issuer-submissions-collection-inspection",
        "campaign_id": capacity.CAMPAIGN_ID,
        "candidate_id": capacity.CANDIDATE_ID,
        "dataset_id": submissions.semantic.documents.DATASET_ID,
        "contract_sha256": submissions.semantic.documents.indexes.CONTRACT_SHA256,
        "request_graph_sha256": graph["request_graph_sha256"],
        "request_graph_inspection_sha256": graph_inspection["inspection_sha256"],
        "collection_sha256": public["collection_sha256"],
        "collection_file_sha256": sha256_file(collection_path),
        "private_collection_sha256": private["private_collection_sha256"],
        "private_collection_file_sha256": sha256_file(private_path),
        "pending_event_count": 317,
        "request_count": 312,
        "success_count": 312,
        "failure_count": 0,
        "source_bytes": public["source_bytes"],
        "raw_sources_rehashed": True,
        "supplemental_submission_request_freeze_permitted": True,
        "supplemental_submission_file_access_permitted": False,
        "issuer_primary_document_access_permitted": False,
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


def collection_inspection_path(value: Mapping[str, Any]) -> Path:
    return submissions.COLLECTION_INSPECTION_ROOT / (
        f"{capacity.CANDIDATE_ID}-submissions-{value['inspection_sha256']}.json"
    )


def _one(pattern: str, description: str) -> Path:
    matches = sorted(submissions.PROJECT_ROOT.glob(pattern))
    if len(matches) != 1:
        raise Schedule13dSymbolSubmissionsInspectionError(
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
            "strategy_tournament/v2/schedule13d/symbols/submissions/manifests/"
            f"{capacity.CANDIDATE_ID}-*.json",
            "issuer-submissions request graph",
        )
        if args.command == "inspect-graph":
            result = inspect_request_graph(graph)
        else:
            result = inspect_collection(graph)
            path = collection_inspection_path(result)
            submissions._write_json(result, path)
            result = {**result, "written": submissions._repo_relative(path)}
    except (
        Schedule13dSymbolSubmissionsInspectionError,
        submissions.Schedule13dSymbolSubmissionsError,
        OSError,
        ValueError,
    ) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}), file=sys.stderr)
        return 2
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

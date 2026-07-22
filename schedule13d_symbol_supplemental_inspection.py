"""Inspect historical SEC submissions request graph and collection."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import schedule13d_capacity as capacity
import schedule13d_symbol_supplemental as supplemental
from historical_store import sha256_file


class Schedule13dSymbolSupplementalInspectionError(RuntimeError):
    """The historical submissions graph or collection does not rebuild."""


def inspect_graph(
    path: Path, *, status_path: Path = supplemental.GRAPH_STATUS_PATH
) -> dict[str, Any]:
    recorded = supplemental.load_request_graph(path)
    status = supplemental._read_object(status_path)
    pending = status.get("status") == "SYMBOL_SUPPLEMENTAL_GRAPH_PENDING_INSPECTION"
    inspected = (
        status.get("status") == "SYMBOL_SUPPLEMENTAL_GRAPH_INSPECTED"
        and status.get("inspection_sha256")
        == capacity.successor._self_hash(status, "inspection_sha256")
    )
    requests = recorded["request_contract"]["requests"]
    if not (
        recorded == supplemental.build_request_graph()
        and (pending or inspected)
        and status.get("request_graph_sha256") == recorded["request_graph_sha256"]
        and len(requests) == 3
        and len({row["subject_cik"] for row in requests}) == 2
        and recorded["request_contract"]["events_with_prior_in_main_metadata"] == 279
        and recorded["request_contract"][
            "events_without_prior_and_without_historical_descriptor"
        ]
        == 36
        and all(
            row["request_sha256"]
            == supplemental._sha256_json(
                {key: value for key, value in row.items() if key != "request_sha256"}
            )
            for row in requests
        )
        and recorded["access_contract"]["issuer_primary_document_access_permitted"]
        is False
        and recorded["access_contract"]["market_price_access_permitted"] is False
        and recorded["capacity_passed"] is None
        and recorded["market_outcomes_accessed"] is False
    ):
        raise Schedule13dSymbolSupplementalInspectionError(
            "historical submissions graph or access boundary differs"
        )
    result: dict[str, Any] = {
        "schema_version": supplemental.SCHEMA_VERSION,
        "inspection_kind": "outcome-blind-sec-historical-submissions-request-inspection",
        "campaign_id": capacity.CAMPAIGN_ID,
        "candidate_id": capacity.CANDIDATE_ID,
        "request_graph_sha256": recorded["request_graph_sha256"],
        "request_graph_file_sha256": sha256_file(path),
        "status": "SYMBOL_SUPPLEMENTAL_GRAPH_INSPECTED",
        "request_count": 3,
        "events_requiring_historical_metadata": 2,
        "exact_descriptor_urls_rebuilt": True,
        "provider_access_permitted": True,
        "provider_access_scope": "the three exact historical submissions URLs only",
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
    supplemental._write_json(result, status_path)
    return result


def inspect_collection(
    graph_path: Path,
    *, collection_path: Path = supplemental.COLLECTION_STATUS_PATH,
) -> dict[str, Any]:
    graph, graph_inspection = supplemental._load_graph_for_collection(graph_path)
    public = supplemental._read_object(collection_path)
    store = supplemental._store_config()
    private_path = supplemental._private_collection_path(store.root)
    private = supplemental._read_object(private_path)
    records = {int(row["ordinal"]): row for row in private["records"]}
    for request in graph["request_contract"]["requests"]:
        raw = supplemental._resolve_cache_path(store.root, request).read_bytes()
        record = records.get(int(request["ordinal"]))
        if not (
            record is not None
            and record.get("request_sha256") == request["request_sha256"]
            and record.get("source_bytes") == len(raw)
            and record.get("source_sha256") == hashlib.sha256(raw).hexdigest()
            and record.get("status") == "SUCCESS"
            and isinstance(json.loads(raw), Mapping)
        ):
            raise Schedule13dSymbolSupplementalInspectionError(
                f"historical submissions source drifted: {request['url']}"
            )
    if not (
        public.get("collection_sha256")
        == capacity.successor._self_hash(public, "collection_sha256")
        and private.get("private_collection_sha256")
        == capacity.successor._self_hash(private, "private_collection_sha256")
        and sha256_file(private_path) == public.get("private_collection_file_sha256")
        and public.get("request_count") == 3
        and public.get("success_count") == 3
        and public.get("failure_count") == 0
        and public.get("issuer_primary_document_request_freeze_permitted") is True
        and public.get("issuer_primary_document_access_permitted") is False
        and public.get("capacity_passed") is None
        and public.get("market_outcomes_accessed") is False
        and public.get("valid") is True
    ):
        raise Schedule13dSymbolSupplementalInspectionError(
            "historical submissions collection does not rebuild"
        )
    result: dict[str, Any] = {
        "schema_version": supplemental.SCHEMA_VERSION,
        "inspection_kind": "outcome-blind-sec-historical-submissions-collection-inspection",
        "campaign_id": capacity.CAMPAIGN_ID,
        "candidate_id": capacity.CANDIDATE_ID,
        "request_graph_sha256": graph["request_graph_sha256"],
        "request_graph_inspection_sha256": graph_inspection["inspection_sha256"],
        "collection_sha256": public["collection_sha256"],
        "collection_file_sha256": sha256_file(collection_path),
        "private_collection_sha256": private["private_collection_sha256"],
        "private_collection_file_sha256": sha256_file(private_path),
        "request_count": 3,
        "success_count": 3,
        "failure_count": 0,
        "raw_sources_rehashed": True,
        "issuer_primary_document_request_freeze_permitted": True,
        "issuer_primary_document_access_permitted": False,
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


def inspection_path(value: Mapping[str, Any]) -> Path:
    return supplemental.COLLECTION_INSPECTION_ROOT / (
        f"{capacity.CANDIDATE_ID}-supplemental-{value['inspection_sha256']}.json"
    )


def _one(pattern: str, description: str) -> Path:
    matches = sorted(supplemental.PROJECT_ROOT.glob(pattern))
    if len(matches) != 1:
        raise Schedule13dSymbolSupplementalInspectionError(
            f"expected one {description}; found {len(matches)}"
        )
    return matches[0]


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("inspect-graph", "inspect-collection"))
    args = parser.parse_args(argv)
    try:
        graph = _one(
            "strategy_tournament/v2/schedule13d/symbols/supplemental/manifests/"
            f"{capacity.CANDIDATE_ID}-*.json",
            "historical submissions graph",
        )
        if args.command == "inspect-graph":
            result = inspect_graph(graph)
        else:
            result = inspect_collection(graph)
            path = inspection_path(result)
            supplemental._write_json(result, path)
            result = {**result, "written": supplemental._repo_relative(path)}
    except (
        Schedule13dSymbolSupplementalInspectionError,
        supplemental.Schedule13dSymbolSupplementalError,
        OSError,
        ValueError,
    ) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}), file=sys.stderr)
        return 2
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

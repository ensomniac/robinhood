"""Independently inspect Schedule 13D quarterly-index graph and collection."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import schedule13d_capacity as capacity
import schedule13d_index_collection as indexes
from historical_store import sha256_file


class Schedule13dIndexInspectionError(RuntimeError):
    """The Schedule 13D index graph or collection does not independently rebuild."""


def inspect_request_graph(
    graph_path: Path, *, status_path: Path = indexes.GRAPH_STATUS_PATH
) -> dict[str, Any]:
    recorded = indexes.load_request_graph(graph_path)
    expected = indexes.build_request_graph()
    pending = indexes._read_object(status_path)
    for relative, digest in recorded["implementation_hashes"].items():
        if sha256_file(indexes.PROJECT_ROOT / relative) != digest:
            raise Schedule13dIndexInspectionError(
                f"request-graph implementation drifted: {relative}"
            )
    requests = recorded["request_contract"]["requests"]
    expected_pairs = [
        (year, quarter)
        for year in range(indexes.START_YEAR, indexes.END_YEAR + 1)
        for quarter in range(1, 5)
    ]
    actual_pairs = [(row["year"], row["quarter"]) for row in requests]
    pending_state = pending.get("status") == "INDEX_REQUEST_GRAPH_PENDING_INSPECTION"
    already_inspected = (
        pending.get("status") == "INDEX_REQUEST_GRAPH_INSPECTED"
        and pending.get("inspection_sha256")
        == capacity.successor._self_hash(pending, "inspection_sha256")
    )
    if not (
        recorded == expected
        and (pending_state or already_inspected)
        and pending.get("request_graph_sha256") == recorded["request_graph_sha256"]
        and len(requests) == 16
        and actual_pairs == expected_pairs
        and len({row["url"] for row in requests}) == 16
        and all(
            row["url"]
            == "https://www.sec.gov/Archives/edgar/full-index/"
            f"{row['year']}/QTR{row['quarter']}/master.idx"
            for row in requests
        )
        and all(
            row["request_sha256"]
            == indexes._sha256_json(
                {key: value for key, value in row.items() if key != "request_sha256"}
            )
            for row in requests
        )
        and recorded["denominator_contract"]["filing_count"] is None
        and recorded["filing_count"] is None
        and recorded["verified_event_count"] is None
        and recorded["access_contract"]["provider_access_before_graph_inspection_permitted"]
        is False
        and recorded["access_contract"]["accession_document_access_permitted"]
        is False
        and recorded["access_contract"]["market_price_access_permitted"] is False
        and recorded["access_contract"]["stage0_outcome_access_permitted"] is False
        and recorded["access_contract"]["broker_actions_permitted"] is False
        and recorded["returns_computed"] == 0
        and recorded["market_outcomes_accessed"] is False
    ):
        raise Schedule13dIndexInspectionError(
            "quarterly-index graph, zero-count lock, or access boundary differs"
        )
    result: dict[str, Any] = {
        "schema_version": indexes.SCHEMA_VERSION,
        "inspection_kind": "outcome-blind-sec-quarterly-index-request-inspection",
        "campaign_id": capacity.CAMPAIGN_ID,
        "candidate_id": capacity.CANDIDATE_ID,
        "dataset_id": indexes.DATASET_ID,
        "contract_sha256": indexes.CONTRACT_SHA256,
        "request_graph_sha256": recorded["request_graph_sha256"],
        "request_graph_file_sha256": sha256_file(graph_path),
        "status": "INDEX_REQUEST_GRAPH_INSPECTED",
        "quarter_request_count": 16,
        "exact_quarter_pairs_rebuilt": True,
        "exact_urls_rebuilt": True,
        "request_hashes_rebuilt": True,
        "implementation_hashes_rebuilt": True,
        "zero_count_boundary_rebuilt": True,
        "provider_access_permitted": True,
        "provider_access_scope": "the 16 exact quarterly master-index URLs only",
        "accession_document_access_permitted": False,
        "filing_count": None,
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
    indexes._write_json(result, status_path)
    return result


def _rebuild_private_index(
    graph: Mapping[str, Any],
    collection: Mapping[str, Any],
    *,
    captured_at: str,
) -> dict[str, Any]:
    store = indexes._store_config()
    request_by_pair = {
        (int(row["year"]), int(row["quarter"])): row
        for row in graph["request_contract"]["requests"]
    }
    quarter_results: list[dict[str, Any]] = []
    for public in collection["quarter_sources"]:
        pair = (int(public["year"]), int(public["quarter"]))
        request = request_by_pair.get(pair)
        if request is None:
            raise Schedule13dIndexInspectionError(
                f"collection contains an unfrozen quarter: {pair}"
            )
        path = indexes._resolve_cache_path(store.root, request)
        raw = path.read_bytes()
        if not (
            len(raw) == public["source_bytes"]
            and hashlib.sha256(raw).hexdigest() == public["source_sha256"]
            and request["request_sha256"] == public["request_sha256"]
        ):
            raise Schedule13dIndexInspectionError(
                f"retained quarterly source drifted: {pair}"
            )
        parsed = indexes._parse_master_index(
            raw.decode("utf-8", errors="replace"), request
        )
        if parsed["all_index_rows"] != public["all_index_rows"]:
            raise Schedule13dIndexInspectionError(
                f"quarter denominator drifted: {pair}"
            )
        quarter_results.append(
            {
                **dict(public),
                "initial_sc13d_rows": parsed["initial_sc13d_rows"],
            }
        )
    quarter_results.sort(key=lambda row: (int(row["year"]), int(row["quarter"])))
    return indexes._build_private_index(
        graph=graph, quarter_results=quarter_results, captured_at=captured_at
    )


def inspect_collection(
    graph_path: Path,
    *, collection_path: Path = indexes.COLLECTION_STATUS_PATH,
) -> dict[str, Any]:
    graph, graph_inspection = indexes._load_graph_for_collection(graph_path)
    recorded = indexes._read_object(collection_path)
    if recorded.get("collection_sha256") != capacity.successor._self_hash(
        recorded, "collection_sha256"
    ):
        raise Schedule13dIndexInspectionError("collection hash is invalid")
    store = indexes._store_config()
    private_path = indexes._private_index_path(store.root)
    private = indexes._read_gzip_object(private_path)
    rebuilt = _rebuild_private_index(
        graph, recorded, captured_at=str(private.get("captured_at") or "")
    )
    if not (
        private == rebuilt
        and private.get("private_index_sha256")
        == capacity.successor._self_hash(private, "private_index_sha256")
        and sha256_file(private_path) == recorded["private_index_file_sha256"]
        and recorded["private_index_sha256"] == private["private_index_sha256"]
        and recorded["request_graph_sha256"] == graph["request_graph_sha256"]
        and recorded["request_graph_inspection_sha256"]
        == graph_inspection["inspection_sha256"]
        and recorded["collector_sha256"] == sha256_file(indexes.Path(indexes.__file__))
        and recorded["quarter_request_count"] == 16
        and recorded["quarter_success_count"] == 16
        and recorded["quarter_failure_count"] == 0
        and recorded["all_index_rows"] == private["all_index_rows"]
        and recorded["indexed_initial_sc13d_count"]
        == private["indexed_initial_sc13d_count"]
        and recorded["filed_date_window_provisional_count"]
        == private["filed_date_window_provisional_count"]
        and recorded["acceptance_time_classified_count"] == 0
        and recorded["terminal_reason_count"] == 0
        and recorded["accession_document_access_permitted"] is False
        and recorded["capacity_classification_complete"] is False
        and recorded["verified_event_count"] is None
        and recorded["market_price_values_accessed"] == 0
        and recorded["returns_computed"] == 0
        and recorded["market_outcomes_accessed"] is False
        and recorded["broker_actions"] == 0
        and recorded["maturity_effect"] == "NONE"
        and recorded["valid"] is True
    ):
        raise Schedule13dIndexInspectionError(
            "quarterly-index collection does not independently rebuild"
        )
    result: dict[str, Any] = {
        "schema_version": indexes.SCHEMA_VERSION,
        "inspection_kind": "outcome-blind-sec-quarterly-index-collection-inspection",
        "campaign_id": capacity.CAMPAIGN_ID,
        "candidate_id": capacity.CANDIDATE_ID,
        "dataset_id": indexes.DATASET_ID,
        "contract_sha256": indexes.CONTRACT_SHA256,
        "request_graph_sha256": graph["request_graph_sha256"],
        "request_graph_inspection_sha256": graph_inspection["inspection_sha256"],
        "collection_sha256": recorded["collection_sha256"],
        "collection_file_sha256": sha256_file(collection_path),
        "private_index_sha256": private["private_index_sha256"],
        "private_index_file_sha256": sha256_file(private_path),
        "quarter_request_count": 16,
        "quarter_success_count": 16,
        "quarter_failure_count": 0,
        "all_index_rows": private["all_index_rows"],
        "indexed_initial_sc13d_count": private["indexed_initial_sc13d_count"],
        "filed_date_window_provisional_count": private[
            "filed_date_window_provisional_count"
        ],
        "raw_sources_rehashed": True,
        "denominator_reparsed": True,
        "accession_document_request_freeze_required": True,
        "accession_document_access_permitted": False,
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
    return indexes.COLLECTION_INSPECTION_ROOT / (
        f"{capacity.CANDIDATE_ID}-index-collection-{value['inspection_sha256']}.json"
    )


def _one(pattern: str, description: str) -> Path:
    matches = sorted(indexes.PROJECT_ROOT.glob(pattern))
    if len(matches) != 1:
        raise Schedule13dIndexInspectionError(
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
            "strategy_tournament/v2/schedule13d/indexes/manifests/"
            f"{capacity.CANDIDATE_ID}-*.json",
            "quarterly-index request graph",
        )
        if args.command == "inspect-graph":
            result = inspect_request_graph(graph)
        else:
            result = inspect_collection(graph)
            path = collection_inspection_path(result)
            indexes._write_json(result, path)
            result = {**result, "written": indexes._repo_relative(path)}
    except (
        Schedule13dIndexInspectionError,
        indexes.Schedule13dIndexCollectionError,
        OSError,
        ValueError,
    ) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}), file=sys.stderr)
        return 2
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""Independently inspect S&P 500 addition source contracts and collections."""

from __future__ import annotations

import argparse
import hashlib
import json
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import outcome_exposure
import strategy_discovery

import sp500_addition_capacity as capacity


class Sp500AdditionCapacityInspectionError(RuntimeError):
    """A source contract, cache, or capacity result did not rebuild."""


def _write_artifact(
    *,
    root: Path,
    lane: str,
    prefix: str,
    value: dict[str, Any],
) -> tuple[Path, dict[str, Any]]:
    value["artifact_sha256"] = capacity._self_hash(value)
    path = root / lane / f"{prefix}-{value['artifact_sha256']}.json"
    capacity._write(path, value)
    return path, value


def inspect_index_contract(
    contract_path: Path,
    *,
    inspected_at: str,
    root: Path = capacity.PUBLIC_ROOT,
) -> tuple[Path, dict[str, Any]]:
    """Rebuild the zero-access archive-index contract."""

    capacity._timestamp(inspected_at)
    strategy_discovery.require_committed(contract_path)
    recorded = capacity._load_artifact(
        contract_path, "sp500-addition-index-query-contract"
    )
    expected_tasks = capacity.archive_tasks()
    actual_hashes = capacity._implementation_hashes()
    authority = capacity._read(capacity.ROLLING_AUTHORIZATION_PATH)
    status = capacity._read(capacity.ROLLING_STATUS_PATH)
    if not (
        recorded.get("state") == "INDEX_QUERY_CONTRACT_FROZEN"
        and recorded.get("tasks") == expected_tasks
        and recorded.get("task_count") == 16
        and recorded.get("implementation_hashes") == actual_hashes
        and recorded.get("provider_requests_before_contract_freeze") == 0
        and recorded.get("release_page_access_permitted") is False
        and recorded.get("market_price_access_permitted") is False
        and recorded.get("target_return_access_permitted") is False
        and recorded.get("confirmation_access_permitted") is False
        and recorded.get("broker_actions_permitted") is False
        and recorded.get("market_outcomes_accessed") is False
        and authority.get("authorization_sha256")
        == capacity.ROLLING_AUTHORIZATION_SHA256
        and authority.get("maximum_concurrent_active_mechanism_families")
        == 3
        and status.get("state") == "ROLLING_DISCOVERY_AUTHORIZED"
        and status.get("available_slot_count", 0) >= 1
        and status.get("valid") is True
    ):
        raise Sp500AdditionCapacityInspectionError(
            "archive-index contract does not independently rebuild"
        )
    inspection = {
        "schema_version": 1,
        "artifact_kind": "sp500-addition-index-contract-inspection",
        "campaign_id": capacity.CAMPAIGN_ID,
        "family_id": capacity.FAMILY_ID,
        "successor_id": capacity.SUCCESSOR_ID,
        "inspected_at": inspected_at,
        "state": "INDEX_QUERY_CONTRACT_INSPECTED_READY",
        "contract_path": capacity._repo_path(contract_path),
        "contract_sha256": recorded["artifact_sha256"],
        "inspection": {
            "rolling_slot_authority_rebuilt": True,
            "sixteen_year_queries_rebuilt": True,
            "single_page_capacity_rebuilt": True,
            "provider_semantics_rebuilt": True,
            "complete_denominator_rebuilt": True,
            "implementation_hashes_rebuilt": True,
            "zero_outcome_boundary_rebuilt": True,
            "valid": True,
        },
        "archive_index_access_permitted": True,
        "release_page_access_permitted": False,
        "market_price_access_permitted": False,
        "target_return_access_permitted": False,
        "confirmation_access_permitted": False,
        "broker_actions_permitted": False,
        "provider_requests": 0,
        "market_outcomes_accessed": False,
    }
    return _write_artifact(
        root=root,
        lane="index-contract-inspection",
        prefix="sp500-addition-index-contract-inspection",
        value=inspection,
    )


def inspect_index_collection(
    collection_path: Path,
    *,
    inspected_at: str,
    root: Path = capacity.PUBLIC_ROOT,
) -> tuple[Path, dict[str, Any]]:
    """Rehash every cached archive page and rebuild every listed URL."""

    capacity._timestamp(inspected_at)
    strategy_discovery.require_committed(collection_path)
    collection = capacity._load_artifact(
        collection_path, "sp500-addition-index-collection"
    )
    contract_path = capacity.PROJECT_ROOT / collection["contract_path"]
    inspection_path = capacity.PROJECT_ROOT / collection["inspection_path"]
    strategy_discovery.require_committed(contract_path)
    strategy_discovery.require_committed(inspection_path)
    contract = capacity._load_artifact(
        contract_path, "sp500-addition-index-query-contract"
    )
    predecessor = capacity._load_artifact(
        inspection_path, "sp500-addition-index-contract-inspection"
    )
    if not (
        collection.get("state") == "INDEX_QUERY_COLLECTION_UNINSPECTED"
        and collection.get("contract_sha256") == contract["artifact_sha256"]
        and collection.get("inspection_sha256")
        == predecessor["artifact_sha256"]
        and collection.get("task_count") == contract["task_count"]
        and collection.get("release_page_requests") == 0
        and collection.get("market_price_requests") == 0
        and collection.get("market_outcomes_accessed") is False
        and collection.get("confirmation_accessed") is False
        and collection.get("broker_actions") == 0
        and predecessor.get("archive_index_access_permitted") is True
    ):
        raise Sp500AdditionCapacityInspectionError(
            "archive collection predecessor chain drifted"
        )
    entries: list[dict[str, Any]] = []
    total_bytes = 0
    for task, frozen in zip(
        contract["tasks"], collection["tasks"], strict=True
    ):
        if any(frozen.get(key) != task.get(key) for key in task):
            raise Sp500AdditionCapacityInspectionError(
                "archive collection task identity drifted"
            )
        cache = capacity._source_path(frozen["cache_relative_path"])
        raw = capacity._read_cached(cache)
        if (
            hashlib.sha256(raw).hexdigest() != frozen["raw_sha256"]
            or len(raw) != frozen["raw_bytes"]
        ):
            raise Sp500AdditionCapacityInspectionError(
                f"archive cache drifted: {task['year']}"
            )
        count, rows = capacity.parse_archive_index(
            raw, expected_year=task["year"]
        )
        if count != frozen["result_count"]:
            raise Sp500AdditionCapacityInspectionError(
                "archive declared count drifted"
            )
        total_bytes += len(raw)
        entries.extend({**row, "listing_year": task["year"]} for row in rows)
    entries.sort(key=lambda row: (row["listed_date"], row["url"]))
    if len({row["url"] for row in entries}) != len(entries):
        raise Sp500AdditionCapacityInspectionError(
            "archive result URLs overlap across yearly queries"
        )
    if len(entries) != collection["declared_result_count"]:
        raise Sp500AdditionCapacityInspectionError(
            "archive result denominator is incomplete"
        )
    inspection = {
        "schema_version": 1,
        "artifact_kind": "sp500-addition-index-collection-inspection",
        "campaign_id": capacity.CAMPAIGN_ID,
        "family_id": capacity.FAMILY_ID,
        "successor_id": capacity.SUCCESSOR_ID,
        "inspected_at": inspected_at,
        "state": "INDEX_QUERY_COLLECTION_INSPECTED_READY",
        "collection_path": capacity._repo_path(collection_path),
        "collection_sha256": collection["artifact_sha256"],
        "contract_sha256": contract["artifact_sha256"],
        "entries": entries,
        "entry_count": len(entries),
        "archive_query_count": len(collection["tasks"]),
        "cached_bytes": total_bytes,
        "inspection": {
            "all_query_tasks_rebuilt": True,
            "all_cache_hashes_rebuilt": True,
            "all_declared_counts_rebuilt": True,
            "complete_url_denominator_rebuilt": True,
            "cross_year_url_uniqueness_rebuilt": True,
            "zero_release_page_access_rebuilt": True,
            "zero_market_outcome_boundary_rebuilt": True,
            "valid": True,
        },
        "release_page_contract_permitted": True,
        "release_page_requests": 0,
        "market_price_access_permitted": False,
        "target_return_access_permitted": False,
        "confirmation_access_permitted": False,
        "broker_actions_permitted": False,
        "market_outcomes_accessed": False,
    }
    return _write_artifact(
        root=root,
        lane="index-collection-inspection",
        prefix="sp500-addition-index-collection-inspection",
        value=inspection,
    )


def inspect_page_contract(
    contract_path: Path,
    *,
    inspected_at: str,
    root: Path = capacity.PUBLIC_ROOT,
) -> tuple[Path, dict[str, Any]]:
    """Rebuild the exact release-page request graph."""

    capacity._timestamp(inspected_at)
    strategy_discovery.require_committed(contract_path)
    contract = capacity._load_artifact(
        contract_path, "sp500-addition-release-page-contract"
    )
    listing_path = capacity.PROJECT_ROOT / contract["listing_inspection_path"]
    strategy_discovery.require_committed(listing_path)
    listing = capacity._load_artifact(
        listing_path, "sp500-addition-index-collection-inspection"
    )
    expected_tasks = [
        {
            "ordinal": ordinal,
            "listed_date": entry["listed_date"],
            "title": entry["title"],
            "url": entry["url"],
            "url_sha256": entry["url_sha256"],
            "task_id": hashlib.sha256(
                f"release-page|{entry['url']}".encode()
            ).hexdigest(),
        }
        for ordinal, entry in enumerate(listing["entries"], start=1)
    ]
    if not (
        contract.get("state") == "RELEASE_PAGE_CONTRACT_FROZEN"
        and contract.get("listing_inspection_sha256")
        == listing["artifact_sha256"]
        and contract.get("tasks") == expected_tasks
        and contract.get("task_count") == len(expected_tasks)
        and contract.get("implementation_hashes")
        == capacity._implementation_hashes()
        and contract.get("provider_requests_before_contract_freeze") == 0
        and contract.get("market_price_access_permitted") is False
        and contract.get("target_return_access_permitted") is False
        and contract.get("confirmation_access_permitted") is False
        and contract.get("broker_actions_permitted") is False
        and contract.get("market_outcomes_accessed") is False
    ):
        raise Sp500AdditionCapacityInspectionError(
            "release-page contract does not independently rebuild"
        )
    inspection = {
        "schema_version": 1,
        "artifact_kind": "sp500-addition-page-contract-inspection",
        "campaign_id": capacity.CAMPAIGN_ID,
        "family_id": capacity.FAMILY_ID,
        "successor_id": capacity.SUCCESSOR_ID,
        "inspected_at": inspected_at,
        "state": "RELEASE_PAGE_CONTRACT_INSPECTED_READY",
        "contract_path": capacity._repo_path(contract_path),
        "contract_sha256": contract["artifact_sha256"],
        "task_count": len(expected_tasks),
        "inspection": {
            "listing_lineage_rebuilt": True,
            "exact_url_graph_rebuilt": True,
            "publication_and_table_semantics_rebuilt": True,
            "deduplication_rebuilt": True,
            "capacity_thresholds_rebuilt": True,
            "implementation_hashes_rebuilt": True,
            "zero_outcome_boundary_rebuilt": True,
            "valid": True,
        },
        "release_page_access_permitted": True,
        "market_price_access_permitted": False,
        "target_return_access_permitted": False,
        "confirmation_access_permitted": False,
        "broker_actions_permitted": False,
        "provider_requests": 0,
        "market_outcomes_accessed": False,
    }
    return _write_artifact(
        root=root,
        lane="page-contract-inspection",
        prefix="sp500-addition-page-contract-inspection",
        value=inspection,
    )


def _deduplicate_events(
    parsed_releases: Sequence[Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], int]:
    by_identity: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for release in parsed_releases:
        for event in release["eligible_events"]:
            by_identity.setdefault(
                (event["ticker"], event["effective_date"]), []
            ).append(dict(event))
    retained: list[dict[str, Any]] = []
    duplicates = 0
    for identity in sorted(by_identity):
        candidates = sorted(
            by_identity[identity],
            key=lambda row: (row["announcement_at"], row["source_url"]),
        )
        retained.append(candidates[0])
        duplicates += len(candidates) - 1
    retained.sort(
        key=lambda row: (
            row["announcement_at"],
            row["ticker"],
            row["effective_date"],
        )
    )
    return retained, duplicates


def inspect_page_collection(
    collection_path: Path,
    *,
    inspected_at: str,
    root: Path = capacity.PUBLIC_ROOT,
) -> tuple[Path, dict[str, Any]]:
    """Rebuild every release and classify formal signal-date capacity."""

    capacity._timestamp(inspected_at)
    strategy_discovery.require_committed(collection_path)
    collection = capacity._load_artifact(
        collection_path, "sp500-addition-release-page-collection"
    )
    contract_path = capacity.PROJECT_ROOT / collection["contract_path"]
    inspection_path = capacity.PROJECT_ROOT / collection["inspection_path"]
    strategy_discovery.require_committed(contract_path)
    strategy_discovery.require_committed(inspection_path)
    contract = capacity._load_artifact(
        contract_path, "sp500-addition-release-page-contract"
    )
    predecessor = capacity._load_artifact(
        inspection_path, "sp500-addition-page-contract-inspection"
    )
    if not (
        collection.get("state") == "RELEASE_PAGES_COLLECTED_UNINSPECTED"
        and collection.get("contract_sha256") == contract["artifact_sha256"]
        and collection.get("inspection_sha256")
        == predecessor["artifact_sha256"]
        and collection.get("task_count") == contract["task_count"]
        and collection.get("market_price_requests") == 0
        and collection.get("market_outcomes_accessed") is False
        and collection.get("confirmation_accessed") is False
        and collection.get("broker_actions") == 0
        and predecessor.get("release_page_access_permitted") is True
    ):
        raise Sp500AdditionCapacityInspectionError(
            "release-page collection predecessor chain drifted"
        )
    parsed_releases: list[dict[str, Any]] = []
    denominator: list[dict[str, Any]] = []
    total_bytes = 0
    for task, frozen in zip(
        contract["tasks"], collection["tasks"], strict=True
    ):
        if any(frozen.get(key) != task.get(key) for key in task):
            raise Sp500AdditionCapacityInspectionError(
                "release-page task identity drifted"
            )
        cache = capacity._source_path(frozen["cache_relative_path"])
        raw = capacity._read_cached(cache)
        if (
            hashlib.sha256(raw).hexdigest() != frozen["raw_sha256"]
            or len(raw) != frozen["raw_bytes"]
        ):
            raise Sp500AdditionCapacityInspectionError(
                f"release cache drifted: {task['url']}"
            )
        parsed = capacity.parse_release(
            raw,
            source_url=task["url"],
            listed_date=task["listed_date"],
        )
        parsed_releases.append(parsed)
        denominator.append(
            {
                "source_url": task["url"],
                "listed_date": task["listed_date"],
                "title": task["title"],
                "terminal_reason": parsed["terminal_reason"],
                "eligible_event_count": len(parsed["eligible_events"]),
            }
        )
        total_bytes += len(raw)
    events, duplicate_count = _deduplicate_events(parsed_releases)
    development = [
        event
        for event in events
        if event["announcement_date"] <= capacity.DEVELOPMENT_END
    ]
    confirmation = [
        event
        for event in events
        if capacity.CONFIRMATION_START
        <= event["announcement_date"]
        <= capacity.CONFIRMATION_END
    ]
    development_dates = sorted(
        {event["announcement_date"] for event in development}
    )
    confirmation_dates = sorted(
        {event["announcement_date"] for event in confirmation}
    )
    total_dates = sorted(
        {event["announcement_date"] for event in events}
    )
    if (
        len(total_dates) >= capacity.FAST_LANE_TOTAL_SIGNAL_DATES
        and len(development_dates)
        >= capacity.MINIMUM_DEVELOPMENT_SIGNAL_DATES
        and len(confirmation_dates)
        >= capacity.MINIMUM_CONFIRMATION_SIGNAL_DATES
    ):
        state = "CAPACITY_READY_FAST_LANE"
    elif (
        len(total_dates) >= capacity.MINIMUM_TOTAL_SIGNAL_DATES
        and len(development_dates)
        >= capacity.MINIMUM_DEVELOPMENT_SIGNAL_DATES
        and len(confirmation_dates)
        >= capacity.MINIMUM_CONFIRMATION_SIGNAL_DATES
    ):
        state = "CAPACITY_READY_LATER_SINGLE_RULE"
    else:
        state = "RETIRED_INSUFFICIENT_FORMAL_CAPACITY"
    terminal_reason_counts: dict[str, int] = {}
    for row in denominator:
        terminal_reason_counts[row["terminal_reason"]] = (
            terminal_reason_counts.get(row["terminal_reason"], 0) + 1
        )
    # The global outcome index is read only to bind the later pair-level
    # eligibility check.  No price, bar, or return is opened here.
    exposure_audit = outcome_exposure.audit()
    result = {
        "schema_version": 1,
        "artifact_kind": "sp500-addition-capacity-inspection",
        "campaign_id": capacity.CAMPAIGN_ID,
        "family_id": capacity.FAMILY_ID,
        "successor_id": capacity.SUCCESSOR_ID,
        "mechanism_family": capacity.MECHANISM_FAMILY,
        "inspected_at": inspected_at,
        "state": state,
        "collection_path": capacity._repo_path(collection_path),
        "collection_sha256": collection["artifact_sha256"],
        "contract_sha256": contract["artifact_sha256"],
        "release_count": len(denominator),
        "eligible_event_count": len(events),
        "duplicate_event_count": duplicate_count,
        "total_signal_date_capacity": len(total_dates),
        "development_event_count": len(development),
        "development_signal_date_capacity": len(development_dates),
        "confirmation_event_count": len(confirmation),
        "confirmation_signal_date_capacity": len(confirmation_dates),
        "development_signal_dates": development_dates,
        "confirmation_signal_dates": confirmation_dates,
        "events": events,
        "denominator": denominator,
        "terminal_reason_counts": dict(sorted(terminal_reason_counts.items())),
        "cached_bytes": total_bytes,
        "outcome_exposure_index_sha256": exposure_audit["index_sha256"],
        "exact_pair_and_warmup_exposure_check_pending_family_freeze": True,
        "inspection": {
            "all_release_tasks_rebuilt": True,
            "all_cache_hashes_rebuilt": True,
            "publication_timestamps_rebuilt": True,
            "structured_addition_rows_rebuilt": True,
            "duplicate_resolution_rebuilt": True,
            "one_entry_per_announcement_date_rebuilt": True,
            "development_confirmation_counts_rebuilt": True,
            "complete_denominator_rebuilt": True,
            "zero_price_and_return_boundary_rebuilt": True,
            "valid": True,
        },
        "market_price_requests": 0,
        "market_price_access_permitted": False,
        "target_return_access_permitted": False,
        "confirmation_access_permitted": False,
        "broker_actions_permitted": False,
        "market_outcomes_accessed": False,
        "maturity_effect": "NONE",
    }
    return _write_artifact(
        root=root,
        lane="capacity-inspection",
        prefix="sp500-addition-capacity-inspection",
        value=result,
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    for command in (
        "inspect-index-contract",
        "inspect-index-collection",
        "inspect-page-contract",
        "inspect-page-collection",
    ):
        child = sub.add_parser(command)
        child.add_argument("artifact", type=Path)
        child.add_argument("--inspected-at", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    functions = {
        "inspect-index-contract": inspect_index_contract,
        "inspect-index-collection": inspect_index_collection,
        "inspect-page-contract": inspect_page_contract,
        "inspect-page-collection": inspect_page_collection,
    }
    try:
        path, artifact = functions[args.command](
            args.artifact,
            inspected_at=args.inspected_at,
        )
    except (
        Sp500AdditionCapacityInspectionError,
        capacity.Sp500AdditionCapacityError,
        strategy_discovery.StrategyDiscoveryError,
        outcome_exposure.OutcomeExposureError,
        OSError,
        ValueError,
    ) as exc:
        print(
            json.dumps(
                {"error": str(exc), "error_type": type(exc).__name__},
                indent=2,
                sort_keys=True,
            )
        )
        return 1
    print(
        json.dumps(
            {
                "artifact_sha256": artifact["artifact_sha256"],
                "state": artifact["state"],
                "written": capacity._repo_path(path),
                "market_outcomes_accessed": artifact.get(
                    "market_outcomes_accessed", False
                ),
                "broker_actions_permitted": artifact.get(
                    "broker_actions_permitted", False
                ),
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

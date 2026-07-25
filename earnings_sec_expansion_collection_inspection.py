"""Independently inspect SEC expansion plans and metadata capacity results."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
from collections.abc import Mapping, Sequence
from datetime import date
from pathlib import Path
from typing import Any

import earnings_sec_expansion_capacity as capacity
import earnings_sec_expansion_collection as source
import earnings_sec_market_data as market
import outcome_exposure
import strategy_discovery
from historical_store import HistoricalDayStore, sha256_file


class EarningsSecExpansionCollectionInspectionError(RuntimeError):
    """A SEC expansion plan or capacity result failed reconstruction."""


def inspect_plan(
    plan_path: Path,
    *,
    inspected_at: str,
    root: Path = source.DEFAULT_ROOT,
) -> tuple[Path, dict[str, Any]]:
    strategy_discovery.require_committed(Path(__file__).resolve())
    strategy_discovery.require_committed(plan_path)
    plan = capacity._read(plan_path)
    rebuilt = source.build_plan(created_at=str(plan["created_at"]))
    checks = {
        "plan_hash_valid": plan.get("plan_sha256")
        == capacity.self_hash(plan, "plan_sha256"),
        "exact_plan_rebuild": plan == rebuilt,
        "contract_lineage_valid": plan.get("contract_sha256")
        == "214e97345b01a2fe67dfee3858b9cc8a9678be97cecb057a8a969c3e482e15fe",
        "contract_inspection_lineage_valid": plan.get(
            "contract_inspection_sha256"
        )
        == "ae3509f40206f7e79326b400665a11d8961a6b0bf2db8eea4f5b31e6ad178d08",
        "exact_request_graph": plan.get("requests") == capacity.requests()
        and plan.get("request_count") == 32,
        "request_hashes_valid": all(
            row.get("request_sha256")
            == hashlib.sha256(
                capacity.canonical_bytes(
                    {
                        key: value
                        for key, value in row.items()
                        if key != "request_sha256"
                    }
                )
            ).hexdigest()
            for row in plan.get("requests", [])
        ),
        "zero_retry_or_substitution": plan.get("transport", {}).get(
            "retries_permitted"
        )
        == 0
        and plan.get("transport", {}).get("substitutions_permitted") == 0,
        "point_in_time_derivation": plan.get("derivation", {}).get(
            "exposed_ranked_event_substitution_permitted"
        )
        is False
        and plan.get("derivation", {}).get(
            "global_outcome_exposure_filter_required"
        )
        is True,
        "zero_provider_access": plan.get("provider_requests_executed") == 0
        and plan.get("metadata_rows_accessed") == 0,
        "zero_outcome_or_broker": plan.get("market_prices_accessed") is False
        and plan.get("forward_returns_accessed") is False
        and plan.get("strategy_metrics_computed") == 0
        and plan.get("confirmation_outcomes_accessed") is False
        and plan.get("broker_actions") == 0,
    }
    if not all(checks.values()):
        raise EarningsSecExpansionCollectionInspectionError(
            "SEC expansion collection plan inspection failed"
        )
    value: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": (
            "earnings-sec-expansion-metadata-collection-plan-inspection"
        ),
        "campaign_id": capacity.CAMPAIGN_ID,
        "family_id": capacity.FAMILY_ID,
        "successor_id": capacity.SUCCESSOR_ID,
        "state": "SEC_EXPANSION_COLLECTION_PLAN_INSPECTED_READY",
        "inspected_at": capacity._timestamp(inspected_at, "inspected_at"),
        "plan_path": capacity._repo_path(plan_path),
        "plan_file_sha256": sha256_file(plan_path),
        "plan_sha256": plan["plan_sha256"],
        "checks": checks,
        "metadata_collection_authorized": True,
        "authorized_provider_requests": 32,
        "market_price_access_authorized": False,
        "confirmation_access_authorized": False,
        "broker_actions": 0,
        "valid": True,
    }
    value["inspection_sha256"] = capacity.self_hash(
        value, "inspection_sha256"
    )
    output = (
        root
        / "metadata-collection-plan-inspection"
        / f"inspection-{value['inspection_sha256']}.json"
    )
    capacity._write(output, value)
    return output, value


def _read_private(
    collection: Mapping[str, Any], store: HistoricalDayStore
) -> dict[str, Any]:
    info = collection["private_artifact"]
    path = store.root / str(info["cache_relative_path"])
    raw = path.read_bytes()
    if hashlib.sha256(raw).hexdigest() != info["file_sha256"]:
        raise EarningsSecExpansionCollectionInspectionError(
            "private SEC expansion artifact hash differs"
        )
    value = json.loads(gzip.decompress(raw))
    if not isinstance(value, dict):
        raise EarningsSecExpansionCollectionInspectionError(
            "private SEC expansion artifact must be an object"
        )
    if (
        value.get("content_sha256")
        != capacity.self_hash(value, "content_sha256")
        or value["content_sha256"] != info["content_sha256"]
    ):
        raise EarningsSecExpansionCollectionInspectionError(
            "private SEC expansion content hash differs"
        )
    return value


def _scope_for_events(
    events: Sequence[Mapping[str, Any]],
    sessions: Sequence[str],
) -> tuple[list[dict[str, Any]], int]:
    records = outcome_exposure.read_index()
    admitted: list[dict[str, Any]] = []
    exposed = 0
    for raw in events:
        event = dict(raw)
        reaction_date = market._reaction_date(
            str(event["accepted"]), sessions
        )
        if reaction_date is None:
            continue
        scope = {"dates": [reaction_date], "symbols": [str(event["ticker"])]}
        if outcome_exposure.find_overlaps(scope, records):
            exposed += 1
            continue
        admitted.append({**event, "reaction_date": reaction_date})
    return admitted, exposed


def inspect_collection(
    collection_path: Path,
    *,
    inspected_at: str,
    store: HistoricalDayStore | None = None,
    root: Path = source.DEFAULT_ROOT,
) -> tuple[Path, dict[str, Any]]:
    strategy_discovery.require_committed(Path(__file__).resolve())
    strategy_discovery.require_committed(collection_path)
    collection = capacity._read(collection_path)
    plan_path = capacity.PROJECT_ROOT / collection["plan_path"]
    inspection_path = capacity.PROJECT_ROOT / collection["inspection_path"]
    strategy_discovery.require_committed(plan_path)
    strategy_discovery.require_committed(inspection_path)
    plan = capacity._read(plan_path)
    plan_inspection = capacity._read(inspection_path)
    historical_store = store or HistoricalDayStore.from_env()
    private = _read_private(collection, historical_store)
    archive_paths: list[Path] = []
    archives_valid = True
    for request, info in zip(
        plan["requests"], collection["archives"], strict=True
    ):
        path = historical_store.root / str(info["cache_relative_path"])
        if not (
            info["request_sha256"] == request["request_sha256"]
            and path.is_file()
            and sha256_file(path) == info["file_sha256"]
            and path.stat().st_size == info["bytes"]
        ):
            archives_valid = False
        archive_paths.append(path)
    events, archive_counts, summary = source._rank_and_cover(archive_paths)
    sessions = market._sessions(
        date(2011, 12, 1), date(2020, 1, 15)
    )
    development_raw = [
        row
        for row in events
        if capacity.DEVELOPMENT_START
        <= str(row["accepted"])[:10]
        <= capacity.DEVELOPMENT_END
    ]
    confirmation_raw = [
        row
        for row in events
        if capacity.CONFIRMATION_START
        <= str(row["accepted"])[:10]
        <= capacity.CONFIRMATION_END
    ]
    development, development_exposed = _scope_for_events(
        development_raw, sessions
    )
    confirmation, confirmation_exposed = _scope_for_events(
        confirmation_raw, sessions
    )
    development_dates = sorted(
        {str(row["reaction_date"]) for row in development}
    )
    confirmation_dates = sorted(
        {str(row["reaction_date"]) for row in confirmation}
    )
    total_events = len(development) + len(confirmation)
    capacity_ready = (
        total_events >= capacity.MINIMUM_UNIQUE_EVENTS
        and len(development_dates) >= capacity.MINIMUM_DEVELOPMENT_DATES
        and len(confirmation_dates) >= capacity.MINIMUM_CONFIRMATION_DATES
    )
    checks = {
        "collection_hash_valid": collection.get("collection_sha256")
        == capacity.self_hash(collection, "collection_sha256"),
        "plan_chain_valid": collection.get("plan_sha256")
        == plan.get("plan_sha256")
        == plan_inspection.get("plan_sha256"),
        "plan_inspected": plan_inspection.get("state")
        == "SEC_EXPANSION_COLLECTION_PLAN_INSPECTED_READY"
        and plan_inspection.get("valid") is True,
        "archive_graph_complete": archives_valid
        and len(archive_paths) == len(capacity.ARCHIVES),
        "request_accounting_complete": collection.get(
            "provider_telemetry", {}
        ).get("requests", 0)
        + collection.get("provider_telemetry", {}).get("cache_hits", 0)
        == len(capacity.ARCHIVES),
        "private_derivation_exact": private.get("events") == events
        and private.get("archive_counts") == archive_counts
        and private.get("derivation_summary") == summary
        and collection.get("derivation_summary") == summary,
        "zero_retry_or_substitution": collection.get("retries") == 0
        and collection.get("substitutions") == 0,
        "zero_market_outcomes_or_broker": collection.get(
            "market_prices_accessed"
        )
        is False
        and collection.get("forward_returns_accessed") is False
        and collection.get("strategy_metrics_computed") == 0
        and collection.get("confirmation_outcomes_accessed") is False
        and collection.get("broker_actions") == 0,
    }
    if not all(checks.values()):
        raise EarningsSecExpansionCollectionInspectionError(
            "SEC expansion metadata collection inspection failed"
        )
    value: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": "earnings-sec-expansion-capacity-inspection",
        "campaign_id": capacity.CAMPAIGN_ID,
        "family_id": capacity.FAMILY_ID,
        "successor_id": capacity.SUCCESSOR_ID,
        "state": (
            "SEC_EXPANSION_CAPACITY_READY"
            if capacity_ready
            else "INSUFFICIENT_SEC_EXPANSION_CAPACITY"
        ),
        "inspected_at": capacity._timestamp(inspected_at, "inspected_at"),
        "collection_path": capacity._repo_path(collection_path),
        "collection_file_sha256": sha256_file(collection_path),
        "collection_sha256": collection["collection_sha256"],
        "checks": checks,
        "verified_common_equity_events_total": len(events),
        "development_events_before_exposure_filter": len(development_raw),
        "development_events": len(development),
        "development_signal_dates": len(development_dates),
        "development_exposed_events_zero_credit": development_exposed,
        "confirmation_events_before_exposure_filter": len(confirmation_raw),
        "confirmation_events": len(confirmation),
        "confirmation_signal_dates": len(confirmation_dates),
        "confirmation_exposed_events_zero_credit": confirmation_exposed,
        "minimums": {
            "unique_events": capacity.MINIMUM_UNIQUE_EVENTS,
            "development_signal_dates": capacity.MINIMUM_DEVELOPMENT_DATES,
            "confirmation_signal_dates": capacity.MINIMUM_CONFIRMATION_DATES,
        },
        "development_market_price_access_authorized": capacity_ready,
        "development_search_freeze_required_before_price_access": True,
        "confirmation_market_price_access_authorized": False,
        "market_prices_accessed": False,
        "forward_returns_accessed": False,
        "strategy_metrics_computed": 0,
        "broker_actions": 0,
        "valid": True,
    }
    value["inspection_sha256"] = capacity.self_hash(
        value, "inspection_sha256"
    )
    output = (
        root
        / "metadata-collection-inspection"
        / f"inspection-{value['inspection_sha256']}.json"
    )
    capacity._write(output, value)
    return output, value


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    plan_parser = subparsers.add_parser("inspect-plan")
    plan_parser.add_argument("plan", type=Path)
    plan_parser.add_argument("--inspected-at", required=True)
    result_parser = subparsers.add_parser("inspect-collection")
    result_parser.add_argument("collection", type=Path)
    result_parser.add_argument("--inspected-at", required=True)
    args = parser.parse_args(argv)
    if args.command == "inspect-plan":
        path, value = inspect_plan(
            args.plan, inspected_at=args.inspected_at
        )
    else:
        path, value = inspect_collection(
            args.collection, inspected_at=args.inspected_at
        )
    print(
        json.dumps(
            {
                "path": capacity._repo_path(path),
                "inspection_sha256": value["inspection_sha256"],
                "state": value["state"],
                "valid": value["valid"],
                "market_prices_accessed": False,
                "broker_actions": 0,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())

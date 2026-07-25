"""Independently inspect corrected-source SEC PEAD capacity artifacts."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
from collections.abc import Mapping, Sequence
from datetime import date
from pathlib import Path
from typing import Any

import earnings_sec_corrected_expansion as source
import earnings_sec_expansion_collection as v12_collection
import earnings_sec_market_data as market
import outcome_exposure
import strategy_discovery
from historical_store import HistoricalDayStore, sha256_file


class EarningsSecCorrectedExpansionInspectionError(RuntimeError):
    """A corrected-source SEC PEAD artifact failed reconstruction."""


def _embargo_is_sufficient() -> bool:
    sessions = market._sessions(date(2014, 12, 1), date(2015, 1, 15))
    later = [day for day in sessions if day > source.DEVELOPMENT_END]
    if len(later) < 11:
        return False
    reaction = later[0]
    entry_index = sessions.index(reaction) + 1
    final_hold = sessions[entry_index + 4]
    post_hold = [day for day in sessions if day > final_hold]
    return (
        len(post_hold) >= 6
        and post_hold[4] <= source.EMBARGO_END
        and source.CONFIRMATION_START > post_hold[4]
    )


def inspect_contract(
    contract_path: Path,
    *,
    inspected_at: str,
    root: Path = source.DEFAULT_ROOT,
) -> tuple[Path, dict[str, Any]]:
    strategy_discovery.require_committed(Path(__file__).resolve())
    strategy_discovery.require_committed(contract_path)
    contract = source._read(contract_path)
    rebuilt = source.build_contract(created_at=str(contract["created_at"]))
    request_rows = contract.get("requests", [])
    checks = {
        "contract_hash_valid": contract.get("contract_sha256")
        == source.self_hash(contract, "contract_sha256"),
        "exact_contract_rebuild": contract == rebuilt,
        "v12_terminal_lineage": contract.get("predecessor", {}).get("state")
        == "SEC_EXPANSION_ARCHIVE_ROOT_404_INSPECTED_TERMINAL"
        and contract.get("predecessor", {}).get("v12_resume_permitted")
        is False,
        "corrected_official_root_exact": contract.get("archive_root")
        == source.ARCHIVE_ROOT
        and "-and-" not in str(contract.get("archive_root")),
        "exact_16_archive_prefix": request_rows == source.requests_graph()
        and len(request_rows) == 16
        and request_rows[0].get("quarter") == "2012q1"
        and request_rows[-1].get("quarter") == "2015q4",
        "request_hashes_valid": all(
            row.get("request_sha256")
            == hashlib.sha256(
                source.canonical_bytes(
                    {
                        key: item
                        for key, item in row.items()
                        if key != "request_sha256"
                    }
                )
            ).hexdigest()
            for row in request_rows
        ),
        "scope_reduction_outcome_blind": contract.get(
            "scope_reduction", {}
        ).get("prices_or_returns_used")
        is False
        and contract.get("scope_reduction", {}).get(
            "successor_archive_count"
        )
        == 16,
        "calendar_wait_absent": contract.get("rolling_authority", {}).get(
            "calendar_wait_required"
        )
        is False,
        "same_family_slot_preserved": contract.get(
            "rolling_authority", {}
        ).get("new_mechanism_family_slot_consumed")
        is False,
        "event_semantics_unchanged": contract.get(
            "event_semantics", {}
        ).get("identical_to_v12")
        is True
        and contract.get("event_semantics", {}).get(
            "external_or_current_ticker_mapping_permitted"
        )
        is False
        and contract.get("event_semantics", {}).get(
            "same_accession_common_stock_shares_cover_fact_required"
        )
        is True,
        "partitions_exact": contract.get("partitions", {}).get("development")
        == [source.DEVELOPMENT_START, source.DEVELOPMENT_END]
        and contract.get("partitions", {}).get("embargo")
        == [source.EMBARGO_START, source.EMBARGO_END]
        and contract.get("partitions", {}).get("confirmation")
        == [source.CONFIRMATION_START, source.CONFIRMATION_END],
        "hold_plus_embargo_sufficient": _embargo_is_sufficient(),
        "cumulative_selection_accounting": contract.get(
            "selection_accounting", {}
        ).get("prior_evaluated_trials_same_mechanism")
        == 32
        and contract.get("selection_accounting", {}).get(
            "cumulative_trial_count_if_full_search"
        )
        == 64,
        "outcome_index_current": contract.get(
            "outcome_exposure_index_sha256"
        )
        == outcome_exposure.audit()["index_sha256"],
        "zero_provider_access": contract.get("provider_requests_executed") == 0
        and contract.get("metadata_rows_accessed") == 0,
        "zero_outcome_or_broker": contract.get("market_prices_accessed")
        is False
        and contract.get("forward_returns_accessed") is False
        and contract.get("strategy_metrics_computed") == 0
        and contract.get("confirmation_outcomes_accessed") is False
        and contract.get("broker_actions") == 0,
    }
    if not all(checks.values()):
        raise EarningsSecCorrectedExpansionInspectionError(
            "corrected-source SEC contract inspection failed"
        )
    value: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": (
            "earnings-sec-corrected-expansion-capacity-contract-inspection"
        ),
        "campaign_id": source.CAMPAIGN_ID,
        "family_id": source.FAMILY_ID,
        "successor_id": source.SUCCESSOR_ID,
        "state": (
            "SEC_CORRECTED_EXPANSION_CAPACITY_CONTRACT_INSPECTED_READY"
        ),
        "inspected_at": source._timestamp(inspected_at, "inspected_at"),
        "contract_path": source._repo_path(contract_path),
        "contract_file_sha256": sha256_file(contract_path),
        "contract_sha256": contract["contract_sha256"],
        "checks": checks,
        "metadata_collection_authorized": True,
        "authorized_provider_requests": len(source.ARCHIVES),
        "market_price_access_authorized": False,
        "confirmation_access_authorized": False,
        "broker_actions": 0,
        "valid": True,
    }
    value["inspection_sha256"] = source.self_hash(
        value, "inspection_sha256"
    )
    output = (
        root
        / "metadata-contract-inspection"
        / f"inspection-{value['inspection_sha256']}.json"
    )
    source._write(output, value)
    return output, value


def inspect_plan(
    plan_path: Path,
    *,
    inspected_at: str,
    root: Path = source.DEFAULT_ROOT,
) -> tuple[Path, dict[str, Any]]:
    strategy_discovery.require_committed(Path(__file__).resolve())
    strategy_discovery.require_committed(plan_path)
    plan = source._read(plan_path)
    contract_path = source.PROJECT_ROOT / str(plan["contract_path"])
    contract_inspection_path = source.PROJECT_ROOT / str(
        plan["contract_inspection_path"]
    )
    rebuilt = source.build_plan(
        contract_path,
        contract_inspection_path,
        created_at=str(plan["created_at"]),
    )
    checks = {
        "plan_hash_valid": plan.get("plan_sha256")
        == source.self_hash(plan, "plan_sha256"),
        "exact_plan_rebuild": plan == rebuilt,
        "exact_request_graph": plan.get("requests")
        == source.requests_graph()
        and plan.get("request_count") == 16,
        "zero_retry_or_substitution": plan.get("transport", {}).get(
            "retries_permitted"
        )
        == 0
        and plan.get("transport", {}).get("substitutions_permitted") == 0,
        "hash_valid_resume_only": plan.get("transport", {}).get(
            "hash_valid_cache_resume_permitted"
        )
        is True,
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
        raise EarningsSecCorrectedExpansionInspectionError(
            "corrected-source SEC collection plan inspection failed"
        )
    value: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": (
            "earnings-sec-corrected-expansion-collection-plan-inspection"
        ),
        "campaign_id": source.CAMPAIGN_ID,
        "family_id": source.FAMILY_ID,
        "successor_id": source.SUCCESSOR_ID,
        "state": (
            "SEC_CORRECTED_EXPANSION_COLLECTION_PLAN_INSPECTED_READY"
        ),
        "inspected_at": source._timestamp(inspected_at, "inspected_at"),
        "plan_path": source._repo_path(plan_path),
        "plan_file_sha256": sha256_file(plan_path),
        "plan_sha256": plan["plan_sha256"],
        "checks": checks,
        "metadata_collection_authorized": True,
        "authorized_provider_requests": len(source.ARCHIVES),
        "market_price_access_authorized": False,
        "confirmation_access_authorized": False,
        "broker_actions": 0,
        "valid": True,
    }
    value["inspection_sha256"] = source.self_hash(
        value, "inspection_sha256"
    )
    output = (
        root
        / "metadata-collection-plan-inspection"
        / f"inspection-{value['inspection_sha256']}.json"
    )
    source._write(output, value)
    return output, value


def _read_private(
    collection: Mapping[str, Any], store: HistoricalDayStore
) -> dict[str, Any]:
    info = collection["private_artifact"]
    path = store.root / str(info["cache_relative_path"])
    raw = path.read_bytes()
    if hashlib.sha256(raw).hexdigest() != info["file_sha256"]:
        raise EarningsSecCorrectedExpansionInspectionError(
            "private corrected SEC artifact hash differs"
        )
    value = json.loads(gzip.decompress(raw))
    if not isinstance(value, dict):
        raise EarningsSecCorrectedExpansionInspectionError(
            "private corrected SEC artifact must be an object"
        )
    if (
        value.get("content_sha256")
        != source.self_hash(value, "content_sha256")
        or value["content_sha256"] != info["content_sha256"]
    ):
        raise EarningsSecCorrectedExpansionInspectionError(
            "private corrected SEC content hash differs"
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
        scope = {
            "dates": [reaction_date],
            "symbols": [str(event["ticker"])],
        }
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
    collection = source._read(collection_path)
    plan_path = source.PROJECT_ROOT / str(collection["plan_path"])
    inspection_path = source.PROJECT_ROOT / str(collection["inspection_path"])
    strategy_discovery.require_committed(plan_path)
    strategy_discovery.require_committed(inspection_path)
    plan = source._read(plan_path)
    plan_inspection = source._read(inspection_path)
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
    events, archive_counts, summary = v12_collection._rank_and_cover(
        archive_paths
    )
    sessions = market._sessions(date(2011, 12, 1), date(2016, 1, 15))
    development_raw = [
        row
        for row in events
        if source.DEVELOPMENT_START
        <= str(row["accepted"])[:10]
        <= source.DEVELOPMENT_END
    ]
    confirmation_raw = [
        row
        for row in events
        if source.CONFIRMATION_START
        <= str(row["accepted"])[:10]
        <= source.CONFIRMATION_END
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
        total_events >= source.MINIMUM_UNIQUE_EVENTS
        and len(development_dates) >= source.MINIMUM_DEVELOPMENT_DATES
        and len(confirmation_dates) >= source.MINIMUM_CONFIRMATION_DATES
    )
    checks = {
        "collection_hash_valid": collection.get("collection_sha256")
        == source.self_hash(collection, "collection_sha256"),
        "plan_chain_valid": collection.get("plan_sha256")
        == plan.get("plan_sha256")
        == plan_inspection.get("plan_sha256"),
        "plan_inspected": plan_inspection.get("state")
        == "SEC_CORRECTED_EXPANSION_COLLECTION_PLAN_INSPECTED_READY"
        and plan_inspection.get("valid") is True,
        "archive_graph_complete": archives_valid
        and len(archive_paths) == len(source.ARCHIVES),
        "request_accounting_complete": collection.get(
            "provider_telemetry", {}
        ).get("requests", 0)
        + collection.get("provider_telemetry", {}).get("cache_hits", 0)
        == len(source.ARCHIVES),
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
        raise EarningsSecCorrectedExpansionInspectionError(
            "corrected-source SEC metadata collection inspection failed"
        )
    value: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": (
            "earnings-sec-corrected-expansion-capacity-inspection"
        ),
        "campaign_id": source.CAMPAIGN_ID,
        "family_id": source.FAMILY_ID,
        "successor_id": source.SUCCESSOR_ID,
        "state": (
            "SEC_CORRECTED_EXPANSION_CAPACITY_READY"
            if capacity_ready
            else "INSUFFICIENT_SEC_CORRECTED_EXPANSION_CAPACITY"
        ),
        "inspected_at": source._timestamp(inspected_at, "inspected_at"),
        "collection_path": source._repo_path(collection_path),
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
            "unique_events": source.MINIMUM_UNIQUE_EVENTS,
            "development_signal_dates": source.MINIMUM_DEVELOPMENT_DATES,
            "confirmation_signal_dates": source.MINIMUM_CONFIRMATION_DATES,
        },
        "selection_accounting": {
            "prior_evaluated_trials_same_mechanism": (
                source.PRIOR_EVALUATED_TRIALS
            ),
            "future_search_trial_cap": source.FUTURE_SEARCH_TRIAL_CAP,
            "cumulative_trial_count_if_full_search": (
                source.PRIOR_EVALUATED_TRIALS
                + source.FUTURE_SEARCH_TRIAL_CAP
            ),
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
    value["inspection_sha256"] = source.self_hash(
        value, "inspection_sha256"
    )
    output = (
        root
        / "metadata-collection-inspection"
        / f"inspection-{value['inspection_sha256']}.json"
    )
    source._write(output, value)
    return output, value


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    contract = subparsers.add_parser("inspect-contract")
    contract.add_argument("contract", type=Path)
    contract.add_argument("--inspected-at", required=True)
    plan = subparsers.add_parser("inspect-plan")
    plan.add_argument("plan", type=Path)
    plan.add_argument("--inspected-at", required=True)
    collection = subparsers.add_parser("inspect-collection")
    collection.add_argument("collection", type=Path)
    collection.add_argument("--inspected-at", required=True)
    args = parser.parse_args(argv)
    if args.command == "inspect-contract":
        path, value = inspect_contract(
            args.contract, inspected_at=args.inspected_at
        )
    elif args.command == "inspect-plan":
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
                "path": source._repo_path(path),
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

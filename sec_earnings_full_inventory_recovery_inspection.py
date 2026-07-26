"""Inspect the event-first SEC inventory performance recovery."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import outcome_exposure
import sec_earnings_full_inventory as v1
import sec_earnings_full_inventory_recovery as source
import sec_earnings_gap_capacity as narrow
import strategy_discovery
from historical_store import HistoricalDayStore, sha256_file
from scanner_replay import load_calendar


class SecEarningsFullRecoveryInspectionError(RuntimeError):
    """The performance recovery failed independent reconstruction."""


def _write(
    value: dict[str, Any],
    *,
    directory: str,
    root: Path,
) -> tuple[Path, dict[str, Any]]:
    value["inspection_sha256"] = v1.self_hash(
        value, "inspection_sha256"
    )
    path = (
        root
        / directory
        / f"inspection-{value['inspection_sha256']}.json"
    )
    v1._write_json(path, value)
    return path, value


def inspect_failure(
    failure_path: Path,
    *,
    inspected_at: str,
    root: Path = source.DEFAULT_ROOT,
    store: HistoricalDayStore | None = None,
    enforce_committed: bool = True,
) -> tuple[Path, dict[str, Any]]:
    historical = store or HistoricalDayStore.from_env()
    if enforce_committed:
        strategy_discovery.require_committed(Path(__file__).resolve())
        strategy_discovery.require_committed(failure_path)
    failure = v1._read_json(failure_path)
    archive = v1._validate_archive(v1._archive_path(historical))
    audit = outcome_exposure.audit()
    checks = {
        "failure_hash_valid": failure.get("failure_sha256")
        == v1.self_hash(failure, "failure_sha256"),
        "failure_state_exact": failure.get("state")
        == "OVERLAP_EXPANSION_PERFORMANCE_FAILURE",
        "v1_lineage_exact": failure.get("contract_path")
        == v1._repo_path(source.V1_CONTRACT),
        "archive_retained_exact": failure.get("retained_archive") == archive,
        "exposure_scale_rebuilt": failure.get(
            "outcome_exposure_audit", {}
        ).get("exposed_pairs")
        == audit["exposed_pairs"]
        and audit["exposed_pairs"] > 6_000_000,
        "no_partial_collection": (
            not v1._private_inventory_path(historical).exists()
            and not list((v1.DEFAULT_ROOT / "capacity-collection").glob("*.json"))
        ),
        "recovery_change_bounded": failure.get(
            "successor_authority", {}
        ).get("only_permitted_change")
        == "direct membership checks against normalized exposure records",
        "zero_additional_provider_authority": failure.get(
            "successor_authority", {}
        ).get("additional_provider_requests_permitted")
        == 0,
        "zero_market_or_outcome_access": (
            failure.get("market_prices_accessed") is False
            and failure.get("forward_returns_accessed") is False
            and failure.get("confirmation_outcomes_accessed") is False
            and failure.get("strategy_metrics_computed") == 0
        ),
        "zero_broker_actions": failure.get("broker_actions") == 0,
    }
    if not all(checks.values()):
        raise SecEarningsFullRecoveryInspectionError(
            "failure inspection failed: "
            f"{[key for key, valid in checks.items() if not valid]}"
        )
    value: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": (
            "sec-earnings-full-inventory-performance-failure-inspection"
        ),
        "campaign_id": source.CAMPAIGN_ID,
        "family_id": source.FAMILY_ID,
        "dataset_id": source.DATASET_ID,
        "state": "OVERLAP_EXPANSION_FAILURE_INSPECTED_RECOVERY_READY",
        "inspected_at": v1._timestamp(inspected_at, "inspected_at"),
        "failure_path": v1._repo_path(failure_path),
        "failure_file_sha256": sha256_file(failure_path),
        "failure_sha256": failure["failure_sha256"],
        "checks": checks,
        "cache_reuse_permitted": True,
        "additional_provider_requests_permitted": 0,
        "semantic_changes_permitted": False,
        "market_prices_accessed": False,
        "forward_returns_accessed": False,
        "confirmation_outcomes_accessed": False,
        "broker_actions": 0,
        "valid": True,
    }
    return _write(
        value,
        directory="collection-failure-inspection",
        root=root,
    )


def inspect_recovery_contract(
    contract_path: Path,
    *,
    inspected_at: str,
    root: Path = source.DEFAULT_ROOT,
    store: HistoricalDayStore | None = None,
    enforce_committed: bool = True,
) -> tuple[Path, dict[str, Any]]:
    if enforce_committed:
        strategy_discovery.require_committed(Path(__file__).resolve())
        strategy_discovery.require_committed(contract_path)
    contract = source._load_recovery_contract(contract_path)
    rebuilt = source.build_recovery_contract(
        created_at=str(contract["created_at"]),
        store=store,
        enforce_committed=enforce_committed,
    )
    checks = {
        "exact_contract_rebuild": contract == rebuilt,
        "archive_reuse_exact": contract["retained_archive"]
        == v1._validate_archive(
            v1._archive_path(store or HistoricalDayStore.from_env())
        ),
        "event_semantics_unchanged": contract["event_semantics"]
        == v1._load_contract(source.V1_CONTRACT)["event_semantics"],
        "partitions_unchanged": contract["partitions"]
        == v1._load_contract(source.V1_CONTRACT)["partitions"],
        "capacity_thresholds_unchanged": contract["capacity_thresholds"]
        == v1._load_contract(source.V1_CONTRACT)["capacity_thresholds"],
        "selection_accounting_unchanged": contract["future_search"]
        == v1._load_contract(source.V1_CONTRACT)["future_search"],
        "direct_membership_frozen": contract["recovery_implementation"][
            "scope_pair_materialization_per_event"
        ]
        is False,
        "zero_provider_requests": contract[
            "additional_provider_requests_permitted"
        ]
        == 0,
        "zero_market_or_outcome_access": (
            contract["market_prices_accessed"] is False
            and contract["forward_returns_accessed"] is False
            and contract["confirmation_outcomes_accessed"] is False
            and contract["strategy_metrics_computed"] == 0
        ),
        "zero_broker_actions": contract["broker_actions"] == 0,
    }
    if not all(checks.values()):
        raise SecEarningsFullRecoveryInspectionError(
            "recovery contract inspection failed: "
            f"{[key for key, valid in checks.items() if not valid]}"
        )
    value: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": "sec-earnings-full-inventory-recovery-inspection",
        "campaign_id": source.CAMPAIGN_ID,
        "family_id": source.FAMILY_ID,
        "dataset_id": source.DATASET_ID,
        "state": "OVERLAP_RECOVERY_CONTRACT_INSPECTED_READY",
        "inspected_at": v1._timestamp(inspected_at, "inspected_at"),
        "contract_path": v1._repo_path(contract_path),
        "contract_file_sha256": sha256_file(contract_path),
        "contract_sha256": contract["contract_sha256"],
        "checks": checks,
        "cache_reuse_authorized": True,
        "provider_requests_authorized": 0,
        "market_price_access_authorized": False,
        "confirmation_outcome_access_authorized": False,
        "valid": True,
        "market_prices_accessed": False,
        "forward_returns_accessed": False,
        "confirmation_outcomes_accessed": False,
        "broker_actions": 0,
    }
    return _write(
        value,
        directory="recovery-contract-inspection",
        root=root,
    )


def inspect_collection(
    contract_path: Path,
    contract_inspection_path: Path,
    collection_path: Path,
    *,
    inspected_at: str,
    root: Path = source.DEFAULT_ROOT,
    store: HistoricalDayStore | None = None,
    enforce_committed: bool = True,
) -> tuple[Path, dict[str, Any]]:
    historical = store or HistoricalDayStore.from_env()
    if enforce_committed:
        for path in (
            Path(__file__).resolve(),
            contract_path,
            contract_inspection_path,
            collection_path,
        ):
            strategy_discovery.require_committed(path)
    contract = source._load_recovery_contract(contract_path)
    inspection = source._load_recovery_inspection(
        contract_inspection_path, contract
    )
    collection = v1._read_json(collection_path)
    if collection.get("collection_sha256") != v1.self_hash(
        collection, "collection_sha256"
    ):
        raise SecEarningsFullRecoveryInspectionError(
            "recovery collection hash is invalid"
        )
    archive_path = v1._archive_path(historical)
    archive = v1._validate_archive(archive_path)
    private_path = source._private_inventory_path(historical)
    private = v1._read_gzip(private_path)
    rebuilt = source.derive_inventory_fast(
        archive_path,
        security_rows=narrow._security_rows(v1.SECURITY_MASTER),
        calendar=load_calendar(v1.CALENDAR_PATH),
        exposure_records=outcome_exposure.read_index(),
    )
    rebuilt["contract_sha256"] = contract["contract_sha256"]
    rebuilt["contract_inspection_sha256"] = inspection["inspection_sha256"]
    rebuilt["archive_sha256"] = archive["sha256"]
    rebuilt["content_sha256"] = v1.content_hash(
        {
            key: item
            for key, item in rebuilt.items()
            if key != "content_sha256"
        }
    )
    days = rebuilt["signal_days"]
    counts = {
        "development_signal_days": len(days["development"]),
        "embargo_signal_days": len(days["embargo"]),
        "confirmation_signal_days": len(days["confirmation"]),
        "total_creditable_signal_days": (
            len(days["development"]) + len(days["confirmation"])
        ),
        "retained_event_pairs": len(rebuilt["events"]),
    }
    thresholds = contract["capacity_thresholds"]
    adequate = (
        counts["total_creditable_signal_days"]
        >= thresholds["minimum_total_signal_days"]
        and counts["development_signal_days"]
        >= thresholds["minimum_development_signal_days"]
        and counts["confirmation_signal_days"]
        >= thresholds["minimum_confirmation_signal_days"]
    )
    checks = {
        "contract_inspection_valid": inspection["valid"] is True,
        "archive_exact": collection["archive"] == archive,
        "private_file_hash_bound": sha256_file(private_path)
        == collection["private_inventory_file_sha256"],
        "private_content_hash_bound": private.get("content_sha256")
        == collection["private_inventory_content_sha256"],
        "private_inventory_rebuilt": private == rebuilt,
        "public_counts_rebuilt": all(
            collection[key] == value for key, value in counts.items()
        ),
        "source_file_count_rebuilt": collection["source_file_count"]
        == len(rebuilt["source_files"]),
        "reason_counts_rebuilt": collection["reason_counts"]
        == rebuilt["reason_counts"],
        "zero_provider_requests": collection["provider_telemetry"][
            "requests"
        ]
        == 0,
        "zero_market_or_outcome_access": (
            collection["market_prices_accessed"] is False
            and collection["forward_returns_accessed"] is False
            and collection["confirmation_outcomes_accessed"] is False
            and collection["strategy_metrics_computed"] == 0
        ),
        "zero_broker_actions": collection["broker_actions"] == 0,
    }
    if not all(checks.values()):
        raise SecEarningsFullRecoveryInspectionError(
            "recovery collection inspection failed: "
            f"{[key for key, valid in checks.items() if not valid]}"
        )
    state = (
        "EVENT_FIRST_CAPACITY_INSPECTED_READY"
        if adequate
        else "RETIRED_INSUFFICIENT_CAPACITY"
    )
    value: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": (
            "sec-earnings-full-inventory-recovery-collection-inspection"
        ),
        "campaign_id": source.CAMPAIGN_ID,
        "family_id": source.FAMILY_ID,
        "dataset_id": source.DATASET_ID,
        "state": state,
        "inspected_at": v1._timestamp(inspected_at, "inspected_at"),
        "contract_path": v1._repo_path(contract_path),
        "contract_sha256": contract["contract_sha256"],
        "contract_inspection_path": v1._repo_path(
            contract_inspection_path
        ),
        "contract_inspection_sha256": inspection["inspection_sha256"],
        "collection_path": v1._repo_path(collection_path),
        "collection_sha256": collection["collection_sha256"],
        "checks": checks,
        "capacity_thresholds": thresholds,
        **counts,
        "capacity_adequate": adequate,
        "development_market_data_contract_freeze_permitted": adequate,
        "confirmation_market_data_access_permitted": False,
        "valid": True,
        "market_prices_accessed": False,
        "forward_returns_accessed": False,
        "confirmation_outcomes_accessed": False,
        "broker_actions": 0,
    }
    return _write(
        value,
        directory="capacity-inspection",
        root=root,
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    failure = subparsers.add_parser("inspect-failure")
    failure.add_argument("failure", type=Path)
    failure.add_argument("--inspected-at", required=True)
    contract = subparsers.add_parser("inspect-recovery")
    contract.add_argument("contract", type=Path)
    contract.add_argument("--inspected-at", required=True)
    collection = subparsers.add_parser("inspect-collection")
    collection.add_argument("contract", type=Path)
    collection.add_argument("contract_inspection", type=Path)
    collection.add_argument("collection", type=Path)
    collection.add_argument("--inspected-at", required=True)
    args = parser.parse_args(argv)
    try:
        if args.command == "inspect-failure":
            path, value = inspect_failure(
                args.failure,
                inspected_at=args.inspected_at,
            )
        elif args.command == "inspect-recovery":
            path, value = inspect_recovery_contract(
                args.contract,
                inspected_at=args.inspected_at,
            )
        else:
            path, value = inspect_collection(
                args.contract,
                args.contract_inspection,
                args.collection,
                inspected_at=args.inspected_at,
            )
        print(
            json.dumps(
                {
                    "path": v1._repo_path(path),
                    "state": value["state"],
                    "sha256": value["inspection_sha256"],
                    "valid": value["valid"],
                    "market_prices_accessed": False,
                    "broker_actions": 0,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    except (
        SecEarningsFullRecoveryInspectionError,
        source.SecEarningsFullRecoveryError,
        v1.SecEarningsFullInventoryError,
        strategy_discovery.StrategyDiscoveryError,
        OSError,
        KeyError,
        ValueError,
        json.JSONDecodeError,
    ) as exc:
        print(
            json.dumps(
                {"error": str(exc), "error_type": type(exc).__name__},
                indent=2,
                sort_keys=True,
            )
        )
        return 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())

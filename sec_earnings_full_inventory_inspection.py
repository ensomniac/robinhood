"""Independently inspect the event-first SEC earnings inventory."""

from __future__ import annotations

import argparse
import json
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import outcome_exposure
import sec_earnings_full_inventory as source
import sec_earnings_gap_capacity as narrow
import strategy_discovery
from historical_store import HistoricalDayStore, sha256_file
from scanner_replay import load_calendar


class SecEarningsFullInventoryInspectionError(RuntimeError):
    """The event-first SEC inventory failed independent reconstruction."""


def _write(
    value: dict[str, Any],
    *,
    directory: str,
    root: Path,
) -> tuple[Path, dict[str, Any]]:
    value["inspection_sha256"] = source.self_hash(
        value, "inspection_sha256"
    )
    path = (
        root
        / directory
        / f"inspection-{value['inspection_sha256']}.json"
    )
    source._write_json(path, value)
    return path, value


def inspect_contract(
    contract_path: Path,
    *,
    inspected_at: str,
    root: Path = source.DEFAULT_ROOT,
    enforce_committed: bool = True,
) -> tuple[Path, dict[str, Any]]:
    if enforce_committed:
        strategy_discovery.require_committed(Path(__file__).resolve())
        strategy_discovery.require_committed(contract_path)
    contract = source._load_contract(contract_path)
    rebuilt = source.build_contract(
        created_at=str(contract["created_at"]),
        enforce_committed=enforce_committed,
    )
    checks = {
        "exact_contract_rebuild": contract == rebuilt,
        "predecessor_terminal_without_outcomes": (
            contract["predecessor"]["state"]
            == "RETIRED_INSUFFICIENT_CAPACITY"
            and contract["predecessor"]["predecessor_outcomes_accessed"]
            is False
        ),
        "official_bulk_request_exact": (
            contract["provider_request"]["url"] == source.BULK_URL
            and contract["provider_request"]["method"] == "GET"
            and contract["provider_request"]["substitutions_permitted"] == 0
        ),
        "event_rule_exact": (
            contract["event_semantics"]["forms"] == ["8-K"]
            and contract["event_semantics"]["required_items"] == ["2.02"]
            and contract["event_semantics"]["excluded_items"] == ["3.02"]
            and contract["event_semantics"]["amendments_permitted"] is False
        ),
        "point_in_time_no_fallback": (
            contract["point_in_time_universe"][
                "fallback_identity_permitted"
            ]
            is False
            and contract["point_in_time_universe"][
                "current_ticker_fallback_permitted"
            ]
            is False
        ),
        "five_session_embargo_exact": (
            contract["partitions"]["embargo_dates"]
            == source.EMBARGO_DATES
            and len(source.EMBARGO_DATES) == 5
        ),
        "confirmation_global_overlap_zero_credit": (
            contract["partitions"][
                "confirmation_pair_overlap_with_global_index"
            ]
            == "zero_credit"
            and contract["partitions"]["outcome_exposure_index_sha256"]
            == outcome_exposure.audit()["index_sha256"]
        ),
        "selection_accounting_conservative": (
            contract["future_search"]["current_trial_count"] == 32
            and contract["future_search"][
                "cumulative_selection_trial_count"
            ]
            == 128
        ),
        "zero_provider_access": contract["provider_requests_executed"] == 0,
        "zero_market_or_outcome_access": (
            contract["market_prices_accessed"] is False
            and contract["forward_returns_accessed"] is False
            and contract["confirmation_outcomes_accessed"] is False
            and contract["strategy_metrics_computed"] == 0
        ),
        "zero_broker_actions": contract["broker_actions"] == 0,
    }
    if not all(checks.values()):
        raise SecEarningsFullInventoryInspectionError(
            "contract inspection failed: "
            f"{[key for key, valid in checks.items() if not valid]}"
        )
    value: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": "sec-earnings-full-inventory-contract-inspection",
        "campaign_id": source.CAMPAIGN_ID,
        "family_id": source.FAMILY_ID,
        "dataset_id": source.DATASET_ID,
        "state": "EVENT_FIRST_CONTRACT_INSPECTED_READY",
        "inspected_at": source._timestamp(inspected_at, "inspected_at"),
        "contract_path": source._repo_path(contract_path),
        "contract_file_sha256": sha256_file(contract_path),
        "contract_sha256": contract["contract_sha256"],
        "checks": checks,
        "provider_access_authorized": True,
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
        directory="capacity-contract-inspection",
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
    contract = source._load_contract(contract_path)
    contract_inspection = source._load_contract_inspection(
        contract_inspection_path, contract
    )
    collection = source._read_json(collection_path)
    if collection.get("collection_sha256") != source.self_hash(
        collection, "collection_sha256"
    ):
        raise SecEarningsFullInventoryInspectionError(
            "collection hash is invalid"
        )
    archive_path = source._archive_path(historical)
    archive_info = source._validate_archive(archive_path)
    private_path = source._private_inventory_path(historical)
    private = source._read_gzip(private_path)
    rebuilt = source.derive_inventory(
        archive_path,
        security_rows=narrow._security_rows(source.SECURITY_MASTER),
        calendar=load_calendar(source.CALENDAR_PATH),
        exposure_records=outcome_exposure.read_index(),
    )
    rebuilt["contract_sha256"] = contract["contract_sha256"]
    rebuilt["contract_inspection_sha256"] = contract_inspection[
        "inspection_sha256"
    ]
    rebuilt["archive_sha256"] = archive_info["sha256"]
    rebuilt["content_sha256"] = source.content_hash(
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
        "contract_inspection_valid": contract_inspection["valid"] is True,
        "archive_hash_bound": collection["archive"]["sha256"]
        == archive_info["sha256"],
        "archive_size_bound": collection["archive"]["bytes"]
        == archive_info["bytes"],
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
        "filing_documents_absent": (
            collection["filing_documents_accessed"] is False
        ),
        "zero_market_or_outcome_access": (
            collection["market_prices_accessed"] is False
            and collection["forward_returns_accessed"] is False
            and collection["confirmation_outcomes_accessed"] is False
            and collection["strategy_metrics_computed"] == 0
        ),
        "zero_broker_actions": collection["broker_actions"] == 0,
    }
    if not all(checks.values()):
        raise SecEarningsFullInventoryInspectionError(
            "collection inspection failed: "
            f"{[key for key, valid in checks.items() if not valid]}"
        )
    state = (
        "EVENT_FIRST_CAPACITY_INSPECTED_READY"
        if adequate
        else "RETIRED_INSUFFICIENT_CAPACITY"
    )
    value: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": "sec-earnings-full-inventory-inspection",
        "campaign_id": source.CAMPAIGN_ID,
        "family_id": source.FAMILY_ID,
        "dataset_id": source.DATASET_ID,
        "state": state,
        "inspected_at": source._timestamp(inspected_at, "inspected_at"),
        "contract_path": source._repo_path(contract_path),
        "contract_sha256": contract["contract_sha256"],
        "contract_inspection_path": source._repo_path(
            contract_inspection_path
        ),
        "contract_inspection_sha256": contract_inspection[
            "inspection_sha256"
        ],
        "collection_path": source._repo_path(collection_path),
        "collection_sha256": collection["collection_sha256"],
        "checks": checks,
        "capacity_thresholds": thresholds,
        **counts,
        "capacity_adequate": adequate,
        "development_market_data_contract_freeze_permitted": adequate,
        "confirmation_market_data_access_permitted": False,
        "market_prices_accessed": False,
        "forward_returns_accessed": False,
        "confirmation_outcomes_accessed": False,
        "broker_actions": 0,
        "valid": True,
    }
    return _write(
        value,
        directory="capacity-inspection",
        root=root,
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    contract = subparsers.add_parser("inspect-contract")
    contract.add_argument("contract", type=Path)
    contract.add_argument("--inspected-at", required=True)
    collection = subparsers.add_parser("inspect-collection")
    collection.add_argument("contract", type=Path)
    collection.add_argument("contract_inspection", type=Path)
    collection.add_argument("collection", type=Path)
    collection.add_argument("--inspected-at", required=True)
    args = parser.parse_args(argv)
    try:
        if args.command == "inspect-contract":
            path, value = inspect_contract(
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
                    "path": source._repo_path(path),
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
        SecEarningsFullInventoryInspectionError,
        source.SecEarningsFullInventoryError,
        narrow.SecEarningsGapCapacityError,
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

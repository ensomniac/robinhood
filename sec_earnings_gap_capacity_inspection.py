"""Independently inspect SEC earnings-gap capacity artifacts."""

from __future__ import annotations

import argparse
import json
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import sec_earnings_gap_capacity as source
import strategy_discovery
from historical_store import HistoricalDayStore, sha256_file
from scanner_replay import load_calendar


class SecEarningsGapInspectionError(RuntimeError):
    """The SEC earnings-gap evidence failed independent reconstruction."""


def _write_artifact(
    value: dict[str, Any],
    *,
    field: str,
    directory: str,
    root: Path,
) -> tuple[Path, dict[str, Any]]:
    value[field] = source.self_hash(value, field)
    path = root / directory / f"inspection-{value[field]}.json"
    source._write_json(path, value)
    return path, value


def inspect_contract(
    contract_path: Path,
    *,
    inspected_at: str,
    root: Path = source.DEFAULT_ROOT,
    store: HistoricalDayStore | None = None,
    enforce_committed: bool = True,
) -> tuple[Path, dict[str, Any]]:
    historical = store or HistoricalDayStore.from_env()
    if enforce_committed:
        strategy_discovery.require_committed(Path(__file__).resolve())
        strategy_discovery.require_committed(contract_path)
    contract = source._load_contract(contract_path)
    rebuilt, plan = source.build_contract(
        created_at=str(contract["created_at"]),
        store=historical,
        enforce_committed=enforce_committed,
    )
    private_path = source._private_plan_path(historical)
    observed_plan = source._read_gzip(private_path)
    counts = source._partition_counts(plan)
    checks = {
        "exact_contract_rebuild": contract == rebuilt,
        "exact_private_plan_rebuild": observed_plan == plan,
        "private_plan_content_hash_valid": (
            observed_plan.get("content_sha256") == source.content_hash(
                {
                    key: item
                    for key, item in observed_plan.items()
                    if key != "content_sha256"
                }
            )
        ),
        "private_plan_file_hash_bound": (
            sha256_file(private_path)
            == contract["point_in_time_identity"]["private_plan_file_sha256"]
        ),
        "point_in_time_mapping_complete_enough": (
            len(plan["mapped_pairs"]) == 2320
            and len(plan["unmatched_pairs"]) == 14
            and len(plan["unique_ciks"]) == 568
        ),
        "partition_mapping_rebuilt": counts
        == {
            "development": 1474,
            "embargo": 68,
            "confirmation": 778,
        },
        "exact_sec_semantics": (
            contract["sec_event_semantics"]["forms"] == ["8-K"]
            and contract["sec_event_semantics"]["required_items"] == ["2.02"]
            and contract["sec_event_semantics"]["excluded_items"] == ["3.02"]
            and contract["sec_event_semantics"]["amendments_permitted"] is False
            and contract["sec_event_semantics"]["filing_documents_accessed"]
            is False
        ),
        "one_entry_signal_unit": (
            contract["capacity_thresholds"]["signal_unit"]
            == "distinct date with at least one eligible candidate"
        ),
        "complete_32_trial_grid": (
            contract["future_search_contract"]["trial_count"] == 32
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
        raise SecEarningsGapInspectionError(
            "capacity contract inspection failed: "
            f"{[key for key, valid in checks.items() if not valid]}"
        )
    value: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": "sec-earnings-gap-capacity-contract-inspection",
        "campaign_id": source.CAMPAIGN_ID,
        "family_id": source.FAMILY_ID,
        "dataset_id": source.DATASET_ID,
        "state": "CAPACITY_CONTRACT_INSPECTED_READY",
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
    return _write_artifact(
        value,
        field="inspection_sha256",
        directory="capacity-contract-inspection",
        root=root,
    )


def _cached_rows(
    private: Mapping[str, Any],
    *,
    store: HistoricalDayStore,
) -> dict[str, list[dict[str, Any]]]:
    cache_root = store.root / "_sources"
    rows_by_cik: dict[str, list[dict[str, Any]]] = {}
    for source_file in private["source_files"]:
        path = cache_root / source_file["cache_path"]
        if not path.is_file() or sha256_file(path) != source_file["sha256"]:
            raise SecEarningsGapInspectionError(
                "SEC source cache is missing or hash-invalid"
            )
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, Mapping):
            raise SecEarningsGapInspectionError(
                "SEC source cache must contain an object"
            )
        cik = source_file["cik"]
        if source_file["kind"] == "main":
            rows_by_cik[cik] = source.submission_rows(payload)
        else:
            rows_by_cik.setdefault(cik, []).extend(
                source._columnar_rows(payload)
            )
    return rows_by_cik


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
    contract_inspection = source._load_inspection(
        contract_inspection_path, contract
    )
    collection = source._read_json(collection_path)
    if collection.get("collection_sha256") != source.self_hash(
        collection, "collection_sha256"
    ):
        raise SecEarningsGapInspectionError("collection hash is invalid")
    private_path = source._private_collection_path(historical)
    private = source._read_gzip(private_path)
    plan = source._read_gzip(source._private_plan_path(historical))
    rows_by_cik = _cached_rows(private, store=historical)
    rebuilt = source.build_eligibility(
        plan,
        rows_by_cik,
        load_calendar(source.CALENDAR_PATH),
    )
    expected_private = {
        "schema_version": 1,
        "dataset_id": source.DATASET_ID,
        "contract_sha256": contract["contract_sha256"],
        "inspection_sha256": contract_inspection["inspection_sha256"],
        "plan_content_sha256": plan["content_sha256"],
        "source_files": private["source_files"],
        **rebuilt,
        "market_prices_accessed": False,
        "forward_returns_accessed": False,
        "confirmation_outcomes_accessed": False,
    }
    expected_private["content_sha256"] = source.content_hash(expected_private)
    signal_days = rebuilt["signal_days"]
    counts = {
        "development_signal_days": len(signal_days["development"]),
        "embargo_signal_days": len(signal_days["embargo"]),
        "confirmation_signal_days": len(signal_days["confirmation"]),
        "total_creditable_signal_days": (
            len(signal_days["development"])
            + len(signal_days["confirmation"])
        ),
        "eligible_candidate_pairs": len(rebuilt["eligible_pairs"]),
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
        "collection_hash_valid": collection.get("collection_sha256")
        == source.self_hash(collection, "collection_sha256"),
        "private_file_hash_bound": sha256_file(private_path)
        == collection["private_collection_file_sha256"],
        "private_content_hash_bound": private.get("content_sha256")
        == collection["private_collection_content_sha256"],
        "private_collection_rebuilt": private == expected_private,
        "eligible_pair_count_rebuilt": collection["eligible_candidate_pairs"]
        == counts["eligible_candidate_pairs"],
        "signal_day_counts_rebuilt": all(
            collection[key] == value
            for key, value in counts.items()
            if key != "eligible_candidate_pairs"
        ),
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
        raise SecEarningsGapInspectionError(
            "capacity collection inspection failed: "
            f"{[key for key, valid in checks.items() if not valid]}"
        )
    state = (
        "CAPACITY_INSPECTED_READY"
        if adequate
        else "RETIRED_INSUFFICIENT_CAPACITY"
    )
    value: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": "sec-earnings-gap-capacity-inspection",
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
        "development_outcome_access_permitted": adequate,
        "confirmation_outcome_access_permitted": False,
        "market_prices_accessed": False,
        "forward_returns_accessed": False,
        "confirmation_outcomes_accessed": False,
        "broker_actions": 0,
        "valid": True,
    }
    return _write_artifact(
        value,
        field="inspection_sha256",
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
        SecEarningsGapInspectionError,
        source.SecEarningsGapCapacityError,
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

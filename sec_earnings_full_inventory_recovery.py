"""Recover the event-first SEC inventory from a repeated-expansion bottleneck.

The v1 provider archive is retained exactly.  This successor changes only the
global outcome-exposure membership implementation: it checks each normalized
record directly rather than materializing and sorting all 6.27 million exposed
pairs once per candidate event.
"""

from __future__ import annotations

import argparse
import json
import time
import zipfile
from collections import Counter
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import outcome_exposure
import sec_earnings_full_inventory as v1
import sec_earnings_gap_capacity as narrow
import strategy_discovery
from historical_store import HistoricalDayStore, sha256_file
from scanner_replay import load_calendar


PROJECT_ROOT = Path(__file__).resolve().parent
CAMPAIGN_ID = v1.CAMPAIGN_ID
FAMILY_ID = v1.FAMILY_ID
DATASET_ID = "dataset-sec-filed-earnings-event-first-2023-2024-v2"
DEFAULT_ROOT = (
    PROJECT_ROOT
    / "strategy_tournament/v2/continuous"
    / FAMILY_ID
    / DATASET_ID
)
V1_CONTRACT = (
    v1.DEFAULT_ROOT
    / "capacity-contract/"
    "contract-319a78ec1c2fac291a9463bed424bde7e9137fe157119d56c487205822bb13ea"
    ".json"
)
V1_CONTRACT_INSPECTION = (
    v1.DEFAULT_ROOT
    / "capacity-contract-inspection/"
    "inspection-84c9500e28d46077d41c54a3127dfdd7945c77a01c14436e54e00415d8f644b5"
    ".json"
)


class SecEarningsFullRecoveryError(RuntimeError):
    """The event-first capacity recovery lineage is invalid."""


def _private_root(store: HistoricalDayStore) -> Path:
    return store.root / "_derived" / "sec_earnings_full_inventory" / DATASET_ID


def _private_inventory_path(store: HistoricalDayStore) -> Path:
    return _private_root(store) / "event-inventory.json.gz"


def _failure_artifacts() -> list[Path]:
    return sorted((DEFAULT_ROOT / "collection-failure").glob("*.json"))


def _failure_inspections() -> list[Path]:
    return sorted(
        (DEFAULT_ROOT / "collection-failure-inspection").glob("*.json")
    )


def _contract_artifacts() -> list[Path]:
    return sorted((DEFAULT_ROOT / "recovery-contract").glob("*.json"))


def _contract_inspections() -> list[Path]:
    return sorted(
        (DEFAULT_ROOT / "recovery-contract-inspection").glob("*.json")
    )


def _load_v1_lineage() -> tuple[dict[str, Any], dict[str, Any]]:
    contract = v1._load_contract(V1_CONTRACT)
    inspection = v1._load_contract_inspection(
        V1_CONTRACT_INSPECTION, contract
    )
    return contract, inspection


def record_failure(
    *,
    failed_at: str,
    store: HistoricalDayStore | None = None,
    root: Path = DEFAULT_ROOT,
) -> tuple[Path, dict[str, Any]]:
    historical = store or HistoricalDayStore.from_env()
    for path in (V1_CONTRACT, V1_CONTRACT_INSPECTION):
        strategy_discovery.require_committed(path)
    contract, inspection = _load_v1_lineage()
    archive_path = v1._archive_path(historical)
    archive = v1._validate_archive(archive_path)
    if v1._private_inventory_path(historical).exists() or list(
        (v1.DEFAULT_ROOT / "capacity-collection").glob("*.json")
    ):
        raise SecEarningsFullRecoveryError(
            "v1 produced a collection artifact and is not failure-only"
        )
    value: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": "sec-earnings-full-inventory-performance-failure",
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "dataset_id": v1.DATASET_ID,
        "state": "OVERLAP_EXPANSION_PERFORMANCE_FAILURE",
        "failed_at": v1._timestamp(failed_at, "failed_at"),
        "contract_path": v1._repo_path(V1_CONTRACT),
        "contract_sha256": contract["contract_sha256"],
        "contract_inspection_path": v1._repo_path(V1_CONTRACT_INSPECTION),
        "contract_inspection_sha256": inspection["inspection_sha256"],
        "failure": {
            "stage": "local_event_derivation",
            "implementation": (
                "outcome_exposure.find_overlaps called once per proposed event"
            ),
            "root_cause": (
                "each call re-expanded and sorted the complete global exposure "
                "scope, producing redundant work proportional to event count "
                "times 6.27 million exposed pairs"
            ),
            "provider_transport_complete": True,
            "private_inventory_written": False,
            "public_collection_written": False,
        },
        "retained_archive": archive,
        "outcome_exposure_audit": outcome_exposure.audit(),
        "successor_authority": {
            "same_archive_bytes_required": True,
            "same_event_semantics_required": True,
            "same_partitions_required": True,
            "same_capacity_thresholds_required": True,
            "same_selection_accounting_required": True,
            "additional_provider_requests_permitted": 0,
            "only_permitted_change": (
                "direct membership checks against normalized exposure records"
            ),
        },
        "provider_requests_executed": 1,
        "provider_metadata_archive_accessed": True,
        "filing_documents_accessed": False,
        "market_prices_accessed": False,
        "forward_returns_accessed": False,
        "strategy_metrics_computed": 0,
        "confirmation_outcomes_accessed": False,
        "broker_actions": 0,
    }
    value["failure_sha256"] = v1.self_hash(value, "failure_sha256")
    path = (
        root
        / "collection-failure"
        / f"failure-{value['failure_sha256']}.json"
    )
    v1._write_json(path, value)
    return path, value


def _load_failure_inspection() -> tuple[Path, dict[str, Any]]:
    matches = _failure_inspections()
    if len(matches) != 1:
        raise SecEarningsFullRecoveryError(
            "expected one exact inspected performance failure"
        )
    path = matches[0]
    value = v1._read_json(path)
    if not (
        value.get("inspection_sha256")
        == v1.self_hash(value, "inspection_sha256")
        and value.get("state")
        == "OVERLAP_EXPANSION_FAILURE_INSPECTED_RECOVERY_READY"
        and value.get("valid") is True
    ):
        raise SecEarningsFullRecoveryError(
            "performance failure inspection is invalid"
        )
    return path, value


def build_recovery_contract(
    *,
    created_at: str,
    store: HistoricalDayStore | None = None,
    enforce_committed: bool = True,
) -> dict[str, Any]:
    historical = store or HistoricalDayStore.from_env()
    failure_inspection_path, failure_inspection = _load_failure_inspection()
    if enforce_committed:
        for path in (
            Path(__file__).resolve(),
            PROJECT_ROOT / "sec_earnings_full_inventory_recovery_inspection.py",
            failure_inspection_path,
            V1_CONTRACT,
            V1_CONTRACT_INSPECTION,
        ):
            strategy_discovery.require_committed(path)
    contract, inspection = _load_v1_lineage()
    archive = v1._validate_archive(v1._archive_path(historical))
    value: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": "sec-earnings-full-inventory-recovery-contract",
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "dataset_id": DATASET_ID,
        "state": "OVERLAP_RECOVERY_CONTRACT_FROZEN",
        "created_at": v1._timestamp(created_at, "created_at"),
        "v1_lineage": {
            "contract_path": v1._repo_path(V1_CONTRACT),
            "contract_file_sha256": sha256_file(V1_CONTRACT),
            "contract_sha256": contract["contract_sha256"],
            "contract_inspection_path": v1._repo_path(
                V1_CONTRACT_INSPECTION
            ),
            "contract_inspection_file_sha256": sha256_file(
                V1_CONTRACT_INSPECTION
            ),
            "contract_inspection_sha256": inspection["inspection_sha256"],
            "failure_inspection_path": v1._repo_path(
                failure_inspection_path
            ),
            "failure_inspection_file_sha256": sha256_file(
                failure_inspection_path
            ),
            "failure_inspection_sha256": failure_inspection[
                "inspection_sha256"
            ],
        },
        "retained_archive": archive,
        "provider_request": contract["provider_request"],
        "event_semantics": contract["event_semantics"],
        "point_in_time_universe": contract["point_in_time_universe"],
        "partitions": contract["partitions"],
        "capacity_thresholds": contract["capacity_thresholds"],
        "future_search": contract["future_search"],
        "recovery_implementation": {
            "normalized_records_validated_once": True,
            "symbols_by_date_membership": (
                "lookup exact signal date then exact symbol or wildcard"
            ),
            "cartesian_scope_membership": (
                "require exact date then exact symbol or wildcard"
            ),
            "scope_pair_materialization_per_event": False,
            "semantic_change_permitted": False,
        },
        "implementation_hashes": {
            "sec_earnings_full_inventory.py": sha256_file(
                PROJECT_ROOT / "sec_earnings_full_inventory.py"
            ),
            "sec_earnings_full_inventory_recovery.py": sha256_file(
                Path(__file__).resolve()
            ),
            "sec_earnings_full_inventory_recovery_inspection.py": sha256_file(
                PROJECT_ROOT
                / "sec_earnings_full_inventory_recovery_inspection.py"
            ),
            "outcome_exposure.py": sha256_file(
                PROJECT_ROOT / "outcome_exposure.py"
            ),
        },
        "additional_provider_requests_permitted": 0,
        "market_prices_accessed": False,
        "forward_returns_accessed": False,
        "strategy_metrics_computed": 0,
        "confirmation_outcomes_accessed": False,
        "broker_actions": 0,
    }
    value["contract_sha256"] = v1.self_hash(value, "contract_sha256")
    return value


def freeze_recovery_contract(
    *,
    created_at: str,
    store: HistoricalDayStore | None = None,
    root: Path = DEFAULT_ROOT,
) -> tuple[Path, dict[str, Any]]:
    value = build_recovery_contract(
        created_at=created_at,
        store=store,
    )
    path = (
        root
        / "recovery-contract"
        / f"contract-{value['contract_sha256']}.json"
    )
    v1._write_json(path, value)
    return path, value


def _load_recovery_contract(path: Path) -> dict[str, Any]:
    value = v1._read_json(path)
    if value.get("contract_sha256") != v1.self_hash(
        value, "contract_sha256"
    ):
        raise SecEarningsFullRecoveryError(
            "recovery contract hash is invalid"
        )
    return value


def _load_recovery_inspection(
    path: Path,
    contract: Mapping[str, Any],
) -> dict[str, Any]:
    value = v1._read_json(path)
    if not (
        value.get("inspection_sha256")
        == v1.self_hash(value, "inspection_sha256")
        and value.get("state") == "OVERLAP_RECOVERY_CONTRACT_INSPECTED_READY"
        and value.get("contract_sha256") == contract["contract_sha256"]
        and value.get("cache_reuse_authorized") is True
        and value.get("valid") is True
    ):
        raise SecEarningsFullRecoveryError(
            "recovery contract inspection is invalid"
        )
    return value


def _pair_exposed_fast(
    day: str,
    symbol: str,
    records: Sequence[Mapping[str, Any]],
) -> bool:
    for record in records:
        scope = record["scope"]
        if "symbols_by_date" in scope:
            symbols = scope["symbols_by_date"].get(day, [])
        elif day in scope["dates"]:
            symbols = scope["symbols"]
        else:
            continue
        if "*" in symbols or symbol in symbols:
            return True
    return False


def derive_inventory_fast(
    archive_path: Path,
    *,
    security_rows: Sequence[Mapping[str, Any]],
    calendar: Sequence[str],
    exposure_records: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    normalized_records = [
        outcome_exposure.validate_record(record) for record in exposure_records
    ]
    by_cik = v1._security_by_cik(security_rows)
    ends, windows = v1._signal_windows(calendar)
    events: list[dict[str, Any]] = []
    source_files: list[dict[str, Any]] = []
    reasons: Counter[str] = Counter()
    with zipfile.ZipFile(archive_path) as archive:
        members = v1._member_index(archive)
        for cik in sorted(by_cik):
            primary_name = f"CIK{cik}.json"
            member = members.get(primary_name)
            if member is None:
                reasons["MISSING_PRIMARY_CIK_FILE"] += 1
                continue
            primary, digest, size = v1._read_member(archive, member)
            rows = v1._submission_rows(primary)
            source_files.append(
                {
                    "cik": cik,
                    "name": primary_name,
                    "sha256": digest,
                    "bytes": size,
                }
            )
            missing_history = False
            for history_name in v1._history_files(primary):
                history_member = members.get(history_name)
                if history_member is None:
                    reasons["MISSING_REFERENCED_HISTORY_FILE"] += 1
                    missing_history = True
                    break
                history, history_digest, history_size = v1._read_member(
                    archive, history_member
                )
                rows.extend(narrow._columnar_rows(history))
                source_files.append(
                    {
                        "cik": cik,
                        "name": history_name,
                        "sha256": history_digest,
                        "bytes": history_size,
                    }
                )
            if missing_history:
                continue
            filings = v1._filing_events(rows, ends=ends, windows=windows)
            for filing in filings:
                listings = v1._listings(by_cik[cik], filing["signal_date"])
                if not listings:
                    reasons["NO_POINT_IN_TIME_COMMON_LISTING"] += 1
                    continue
                for listing in listings:
                    exposed = _pair_exposed_fast(
                        filing["signal_date"],
                        listing["symbol"],
                        normalized_records,
                    )
                    if filing["partition"] == "confirmation" and exposed:
                        reasons["CONFIRMATION_PAIR_EXPOSED_ZERO_CREDIT"] += 1
                        continue
                    events.append(
                        {
                            **filing,
                            **listing,
                            "cik": cik,
                            "development_pair_previously_exposed": (
                                exposed
                                if filing["partition"] == "development"
                                else False
                            ),
                        }
                    )
                    reasons[f"{filing['partition'].upper()}_PAIR_RETAINED"] += 1
    events.sort(
        key=lambda row: (
            row["signal_date"],
            row["symbol"],
            row["accession"],
            row["instrument_id"],
        )
    )
    source_files.sort(key=lambda row: (row["cik"], row["name"]))
    signal_days = {
        partition: sorted(
            {
                row["signal_date"]
                for row in events
                if row["partition"] == partition
            }
        )
        for partition in ("development", "embargo", "confirmation")
    }
    value = {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "events": events,
        "signal_days": signal_days,
        "source_files": source_files,
        "reason_counts": dict(sorted(reasons.items())),
        "outcome_exposure_index_sha256": outcome_exposure.audit()[
            "index_sha256"
        ],
        "market_prices_accessed": False,
        "forward_returns_accessed": False,
        "confirmation_outcomes_accessed": False,
    }
    value["content_sha256"] = v1.content_hash(value)
    return value


def collect_recovery(
    contract_path: Path,
    inspection_path: Path,
    *,
    collected_at: str,
    store: HistoricalDayStore | None = None,
    root: Path = DEFAULT_ROOT,
) -> tuple[Path, dict[str, Any]]:
    historical = store or HistoricalDayStore.from_env()
    for path in (contract_path, inspection_path):
        strategy_discovery.require_committed(path)
    contract = _load_recovery_contract(contract_path)
    inspection = _load_recovery_inspection(inspection_path, contract)
    if contract["partitions"][
        "outcome_exposure_index_sha256"
    ] != outcome_exposure.audit()["index_sha256"]:
        raise SecEarningsFullRecoveryError(
            "outcome exposure index drifted after recovery freeze"
        )
    archive_path = v1._archive_path(historical)
    archive = v1._validate_archive(archive_path)
    if archive != contract["retained_archive"]:
        raise SecEarningsFullRecoveryError(
            "retained SEC archive differs from recovery contract"
        )
    started = time.monotonic()
    private = derive_inventory_fast(
        archive_path,
        security_rows=narrow._security_rows(v1.SECURITY_MASTER),
        calendar=load_calendar(v1.CALENDAR_PATH),
        exposure_records=outcome_exposure.read_index(),
    )
    private["contract_sha256"] = contract["contract_sha256"]
    private["contract_inspection_sha256"] = inspection["inspection_sha256"]
    private["archive_sha256"] = archive["sha256"]
    private["content_sha256"] = v1.content_hash(
        {
            key: item
            for key, item in private.items()
            if key != "content_sha256"
        }
    )
    private_path = _private_inventory_path(historical)
    v1._write_gzip(private_path, private)
    days = private["signal_days"]
    value: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": "sec-earnings-full-inventory-recovery-collection",
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "dataset_id": DATASET_ID,
        "state": "EVENT_FIRST_CAPACITY_RECOVERED_AWAITING_INSPECTION",
        "collected_at": v1._timestamp(collected_at, "collected_at"),
        "contract_path": v1._repo_path(contract_path),
        "contract_sha256": contract["contract_sha256"],
        "contract_inspection_path": v1._repo_path(inspection_path),
        "contract_inspection_sha256": inspection["inspection_sha256"],
        "archive": archive,
        "provider_telemetry": {
            "requests": 0,
            "cache_hits": 1,
            "failures": 0,
            "request_seconds": 0.0,
            "local_derivation_seconds": time.monotonic() - started,
        },
        "source_cik_count": len(
            v1._security_by_cik(narrow._security_rows())
        ),
        "source_file_count": len(private["source_files"]),
        "retained_event_pairs": len(private["events"]),
        "development_signal_days": len(days["development"]),
        "embargo_signal_days": len(days["embargo"]),
        "confirmation_signal_days": len(days["confirmation"]),
        "total_creditable_signal_days": (
            len(days["development"]) + len(days["confirmation"])
        ),
        "reason_counts": private["reason_counts"],
        "private_inventory_content_sha256": private["content_sha256"],
        "private_inventory_file_sha256": sha256_file(private_path),
        "filing_documents_accessed": False,
        "market_prices_accessed": False,
        "forward_returns_accessed": False,
        "strategy_metrics_computed": 0,
        "confirmation_outcomes_accessed": False,
        "broker_actions": 0,
    }
    value["collection_sha256"] = v1.self_hash(value, "collection_sha256")
    path = (
        root
        / "capacity-collection"
        / f"collection-{value['collection_sha256']}.json"
    )
    v1._write_json(path, value)
    return path, value


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    failure = subparsers.add_parser("record-failure")
    failure.add_argument("--failed-at", required=True)
    freeze = subparsers.add_parser("freeze-recovery")
    freeze.add_argument("--created-at", required=True)
    collect = subparsers.add_parser("collect")
    collect.add_argument("contract", type=Path)
    collect.add_argument("inspection", type=Path)
    collect.add_argument("--collected-at", required=True)
    args = parser.parse_args(argv)
    try:
        if args.command == "record-failure":
            path, value = record_failure(failed_at=args.failed_at)
        elif args.command == "freeze-recovery":
            path, value = freeze_recovery_contract(
                created_at=args.created_at
            )
        else:
            path, value = collect_recovery(
                args.contract,
                args.inspection,
                collected_at=args.collected_at,
            )
        print(
            json.dumps(
                {
                    "path": v1._repo_path(path),
                    "state": value["state"],
                    "sha256": value.get("collection_sha256")
                    or value.get("failure_sha256")
                    or value.get("contract_sha256"),
                    "market_prices_accessed": False,
                    "broker_actions": 0,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    except (
        SecEarningsFullRecoveryError,
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

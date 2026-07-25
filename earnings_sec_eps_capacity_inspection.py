"""Independently inspect SEC quarterly EPS capacity artifacts."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
from collections import Counter
from collections.abc import Mapping, Sequence
from datetime import datetime
from pathlib import Path
from typing import Any

import earnings_sec_eps_capacity as source
import outcome_exposure
import strategy_discovery
from historical_store import HistoricalDayStore, sha256_file


class EarningsSecEpsInspectionError(RuntimeError):
    """A SEC EPS source artifact failed independent reconstruction."""


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
    checks = {
        "artifact_hash_valid": contract.get("contract_sha256")
        == source.self_hash(contract, "contract_sha256"),
        "exact_contract_rebuild": contract == rebuilt,
        "eight_exact_archives": contract.get("requests") == source._requests(),
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
            for row in contract.get("requests", [])
        ),
        "point_in_time_identity": contract.get("event_semantics", {}).get(
            "external_or_current_ticker_mapping_permitted"
        )
        is False,
        "next_session_boundary": contract.get("event_semantics", {}).get(
            "entry_observation_boundary"
        )
        == "first regular-session open strictly after SEC acceptance",
        "five_session_embargo": contract.get("partitions", {}).get("embargo")
        == [source.EMBARGO_START, source.EMBARGO_END],
        "confirmation_outcomes_sealed": contract.get("partitions", {}).get(
            "confirmation_market_outcomes_remain_untouched"
        )
        is True,
        "outcome_index_current": contract.get("outcome_exposure_index_sha256")
        == outcome_exposure.audit()["index_sha256"],
        "recovery_inspection_bound": contract.get("recovery_lineage", {}).get(
            "failure_inspection_sha256"
        )
        == "228596d0026fdd2e4202e2b8c8ad65dc967a5d39c8c68d3107761f02bdacd81b",
        "zero_request_recovery": contract.get("recovery_lineage", {}).get(
            "additional_provider_requests_permitted"
        )
        == 0
        and len(
            contract.get("recovery_lineage", {}).get(
                "required_cached_archives", []
            )
        )
        == len(source.ARCHIVES),
        "market_prices_absent": contract.get("market_prices_accessed") is False,
        "forward_returns_absent": contract.get("forward_returns_accessed")
        is False,
        "strategy_metrics_absent": contract.get("strategy_metrics_computed") == 0,
        "broker_actions_zero": contract.get("broker_actions") == 0,
    }
    if not all(checks.values()):
        raise EarningsSecEpsInspectionError(
            "SEC quarterly EPS capacity contract inspection failed"
        )
    value: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": "earnings-sec-fsnds-eps-capacity-contract-inspection",
        "campaign_id": source.CAMPAIGN_ID,
        "family_id": source.FAMILY_ID,
        "successor_id": source.SUCCESSOR_ID,
        "state": "SEC_EPS_CONTRACT_INSPECTED_READY",
        "inspected_at": source._timestamp(inspected_at, "inspected_at"),
        "contract_path": source._repo_path(contract_path),
        "contract_file_sha256": sha256_file(contract_path),
        "contract_sha256": contract["contract_sha256"],
        "checks": checks,
        "provider_access_authorized": True,
        "authorized_provider_requests": len(source.ARCHIVES),
        "market_prices_accessed": False,
        "forward_returns_accessed": False,
        "strategy_metrics_computed": 0,
        "confirmation_outcomes_accessed": False,
        "broker_actions": 0,
        "valid": True,
    }
    value["inspection_sha256"] = source.self_hash(value, "inspection_sha256")
    path = (
        root
        / "metadata-contract-inspection"
        / f"inspection-{value['inspection_sha256']}.json"
    )
    source._write(path, value)
    return path, value


def _read_private(
    collection: Mapping[str, Any], store: HistoricalDayStore
) -> tuple[dict[str, Any], bytes]:
    info = collection["private_event_artifact"]
    raw = (store.root / str(info["cache_relative_path"])).read_bytes()
    if hashlib.sha256(raw).hexdigest() != info["file_sha256"]:
        raise EarningsSecEpsInspectionError(
            "private SEC EPS event artifact hash differs"
        )
    try:
        value = json.loads(gzip.decompress(raw))
    except (gzip.BadGzipFile, json.JSONDecodeError) as exc:
        raise EarningsSecEpsInspectionError(
            "private SEC EPS event artifact is unreadable"
        ) from exc
    if not isinstance(value, dict):
        raise EarningsSecEpsInspectionError(
            "private SEC EPS event artifact must contain an object"
        )
    if not (
        value.get("content_sha256")
        == source.self_hash(value, "content_sha256")
        == info["content_sha256"]
    ):
        raise EarningsSecEpsInspectionError(
            "private SEC EPS event content hash differs"
        )
    return value, raw


def _partition(accepted: str) -> str | None:
    observed = datetime.fromisoformat(accepted).date().isoformat()
    if source.DEVELOPMENT_START <= observed <= source.DEVELOPMENT_END:
        return "development"
    if source.EMBARGO_START <= observed <= source.EMBARGO_END:
        return "embargo"
    if source.CONFIRMATION_START <= observed <= source.CONFIRMATION_END:
        return "confirmation"
    return None


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
    contract_path = source.PROJECT_ROOT / str(collection["contract_path"])
    inspection_path = source.PROJECT_ROOT / str(collection["inspection_path"])
    strategy_discovery.require_committed(contract_path)
    strategy_discovery.require_committed(inspection_path)
    contract = source._read(contract_path)
    contract_inspection = source._read(inspection_path)
    historical_store = store or HistoricalDayStore.from_env()
    private, raw = _read_private(collection, historical_store)
    rebuilt_events: list[dict[str, Any]] = []
    rebuilt_counts: list[dict[str, Any]] = []
    for request, info in zip(
        contract["requests"], collection["archive_artifacts"], strict=True
    ):
        archive_path = historical_store.root / str(info["cache_relative_path"])
        if (
            info.get("request_sha256") != request["request_sha256"]
            or sha256_file(archive_path) != info.get("file_sha256")
            or archive_path.stat().st_size != info.get("bytes")
        ):
            raise EarningsSecEpsInspectionError(
                f"archive binding differs for {request['quarter']}"
            )
        events, counts = source.derive_archive_events(archive_path)
        rebuilt_events.extend(events)
        rebuilt_counts.append(
            {
                "quarter": request["quarter"],
                "request_sha256": request["request_sha256"],
                **counts,
            }
        )
    event_keys = Counter(
        (event["accepted"], event["ticker"], event["adsh"])
        for event in rebuilt_events
    )
    unique_events = [
        event
        for event in rebuilt_events
        if event_keys[(event["accepted"], event["ticker"], event["adsh"])] == 1
    ]
    unique_events.sort(key=lambda item: (item["accepted"], item["ticker"], item["adsh"]))
    records = outcome_exposure.read_index()
    partitions: dict[str, list[dict[str, Any]]] = {
        "development": [],
        "embargo": [],
        "confirmation": [],
    }
    contaminated: list[dict[str, Any]] = []
    for event in unique_events:
        partition = _partition(str(event["accepted"]))
        if partition is None:
            continue
        event_date = datetime.fromisoformat(str(event["accepted"])).date().isoformat()
        overlaps = outcome_exposure.find_overlaps(
            {"dates": [event_date], "symbols": [event["ticker"]]},
            records,
        )
        if overlaps:
            contaminated.append(event)
        else:
            partitions[partition].append(event)
    date_counts = {
        key: len(
            {
                datetime.fromisoformat(str(event["accepted"])).date().isoformat()
                for event in events
            }
        )
        for key, events in partitions.items()
    }
    capacity_ready = (
        len(partitions["development"]) + len(partitions["confirmation"])
        >= source.MINIMUM_UNIQUE_EVENTS
        and date_counts["development"] >= source.MINIMUM_DEVELOPMENT_DATES
        and date_counts["confirmation"] >= source.MINIMUM_CONFIRMATION_DATES
    )
    checks = {
        "collection_hash_valid": collection.get("collection_sha256")
        == source.self_hash(collection, "collection_sha256"),
        "contract_binding_valid": private.get("contract_sha256")
        == collection.get("contract_sha256")
        == contract.get("contract_sha256"),
        "contract_inspection_valid": contract_inspection.get("state")
        == "SEC_EPS_CONTRACT_INSPECTED_READY"
        and contract_inspection.get("valid") is True,
        "all_frozen_archives_present": len(collection["archive_artifacts"])
        == len(contract["requests"])
        == len(source.ARCHIVES),
        "archive_order_matches": [
            item["request_sha256"] for item in collection["archive_artifacts"]
        ]
        == [item["request_sha256"] for item in contract["requests"]],
        "archive_counts_rebuilt": rebuilt_counts == private["archive_counts"],
        "events_rebuilt": unique_events == private["events"],
        "event_count_rebuilt": len(unique_events)
        == collection["eligible_event_count"],
        "private_file_hash_valid": hashlib.sha256(raw).hexdigest()
        == collection["private_event_artifact"]["file_sha256"],
        "provider_accounting_complete": (
            collection["provider_telemetry"]["request_count"]
            + collection["provider_telemetry"]["cache_hits"]
            == len(source.ARCHIVES)
        ),
        "provider_failures_zero": collection["provider_telemetry"]["failures"] == 0,
        "no_retry_or_substitution": (
            collection["provider_telemetry"]["retries"] == 0
            and collection["provider_telemetry"]["substitutions"] == 0
        ),
        "market_prices_absent": collection["market_prices_accessed"] is False
        and private["market_prices_accessed"] is False,
        "forward_returns_absent": collection["forward_returns_accessed"] is False
        and private["forward_returns_accessed"] is False,
        "strategy_metrics_absent": collection["strategy_metrics_computed"] == 0
        and private["strategy_metrics_computed"] == 0,
        "confirmation_outcomes_absent": (
            collection["confirmation_outcomes_accessed"] is False
            and private["confirmation_outcomes_accessed"] is False
        ),
        "broker_actions_zero": collection["broker_actions"] == 0
        and private["broker_actions"] == 0,
    }
    if not all(checks.values()):
        raise EarningsSecEpsInspectionError(
            "SEC quarterly EPS collection inspection failed"
        )
    value: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": "earnings-sec-fsnds-eps-capacity-collection-inspection",
        "campaign_id": source.CAMPAIGN_ID,
        "family_id": source.FAMILY_ID,
        "successor_id": source.SUCCESSOR_ID,
        "state": (
            "SEC_EPS_METADATA_CAPACITY_READY"
            if capacity_ready
            else "INSUFFICIENT_SEC_EPS_METADATA_CAPACITY"
        ),
        "inspected_at": source._timestamp(inspected_at, "inspected_at"),
        "collection_path": source._repo_path(collection_path),
        "collection_file_sha256": sha256_file(collection_path),
        "collection_sha256": collection["collection_sha256"],
        "contract_sha256": contract["contract_sha256"],
        "checks": checks,
        "total_unique_events": len(unique_events),
        "duplicate_event_rows_zero_credit": (
            len(rebuilt_events) - len(unique_events)
        ),
        "globally_contaminated_event_count": len(contaminated),
        "untouched_partition_counts": {
            key: {
                "events": len(events),
                "event_dates": date_counts[key],
                "symbols": len({event["ticker"] for event in events}),
            }
            for key, events in partitions.items()
        },
        "capacity_thresholds": contract["capacity_thresholds"],
        "development_price_access_permitted": capacity_ready,
        "confirmation_price_access_permitted": False,
        "provider_requests": 0,
        "market_prices_accessed": False,
        "forward_returns_accessed": False,
        "strategy_metrics_computed": 0,
        "confirmation_outcomes_accessed": False,
        "broker_actions": 0,
        "valid": True,
    }
    value["inspection_sha256"] = source.self_hash(value, "inspection_sha256")
    path = (
        root
        / "metadata-collection-inspection"
        / f"inspection-{value['inspection_sha256']}.json"
    )
    source._write(path, value)
    return path, value


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    contract = subparsers.add_parser("inspect-contract")
    contract.add_argument("artifact", type=Path)
    contract.add_argument("--inspected-at", required=True)
    collection = subparsers.add_parser("inspect-collection")
    collection.add_argument("artifact", type=Path)
    collection.add_argument("--inspected-at", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.command == "inspect-contract":
        path, value = inspect_contract(
            args.artifact, inspected_at=args.inspected_at
        )
    else:
        path, value = inspect_collection(
            args.artifact, inspected_at=args.inspected_at
        )
    print(
        json.dumps(
            {
                "path": source._repo_path(path),
                "sha256": value["inspection_sha256"],
                "state": value["state"],
                "provider_requests": value.get("provider_requests", 0),
                "partition_counts": value.get("untouched_partition_counts", {}),
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

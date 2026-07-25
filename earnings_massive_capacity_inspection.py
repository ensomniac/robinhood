"""Independently inspect Massive earnings metadata contracts and collections."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
from collections import Counter
from collections.abc import Mapping, Sequence
from datetime import date
from pathlib import Path
from typing import Any

import earnings_massive_capacity as source
import strategy_discovery
from historical_store import HistoricalDayStore, sha256_file


class EarningsMassiveInspectionError(RuntimeError):
    """An earnings metadata artifact failed independent reconstruction."""


def inspect_contract(
    contract_path: Path,
    *,
    inspected_at: str,
    root: Path = source.DEFAULT_ROOT,
) -> tuple[Path, dict[str, Any]]:
    strategy_discovery.require_committed(Path(__file__).resolve())
    strategy_discovery.require_committed(contract_path)
    contract = source._read(contract_path)
    rebuilt = source.build_contract(created_at=contract["created_at"])
    checks = {
        "artifact_hash_valid": contract.get("contract_sha256")
        == source.self_hash(contract, "contract_sha256"),
        "exact_contract_rebuild": contract == rebuilt,
        "annual_request_count": len(contract.get("requests", [])) == 15,
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
        "full_history_bound": contract.get("history_start") == source.FIRST_DATE
        and contract.get("history_end") == source.LAST_DATE,
        "nonpaginated_fail_closed": contract.get("request_policy", {}).get(
            "unexpected_next_url_fails_closed"
        )
        is True,
        "existing_family_unchanged": contract.get(
            "existing_family_rule_policy", {}
        ).get("mechanism_parameters_may_not_change")
        is True,
        "prior_trials_preserved": contract.get(
            "existing_family_rule_policy", {}
        ).get("all_prior_trials_enter_selection_correction")
        is True,
        "market_prices_absent": contract.get("market_prices_accessed") is False,
        "forward_returns_absent": contract.get("forward_returns_accessed")
        is False,
        "strategy_metrics_absent": contract.get("strategy_metrics_computed") == 0,
        "broker_actions_zero": contract.get("broker_actions") == 0,
    }
    if not all(checks.values()):
        raise EarningsMassiveInspectionError(
            "Massive earnings metadata contract inspection failed"
        )
    inspected = source._timestamp(inspected_at, "inspected_at")
    value: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": "earnings-massive-metadata-contract-inspection",
        "campaign_id": source.CAMPAIGN_ID,
        "family_id": source.FAMILY_ID,
        "successor_id": source.SUCCESSOR_ID,
        "state": "METADATA_CONTRACT_INSPECTED_READY",
        "inspected_at": inspected,
        "contract_path": source._repo_path(contract_path),
        "contract_file_sha256": sha256_file(contract_path),
        "contract_sha256": contract["contract_sha256"],
        "checks": checks,
        "provider_access_authorized": True,
        "authorized_provider_requests": 15,
        "market_prices_accessed": False,
        "forward_returns_accessed": False,
        "strategy_metrics_computed": 0,
        "broker_actions": 0,
        "valid": True,
    }
    value["inspection_sha256"] = source.self_hash(
        value, "inspection_sha256"
    )
    path = (
        root
        / "metadata-contract-inspection"
        / f"inspection-{value['inspection_sha256']}.json"
    )
    source._write(path, value)
    return path, value


def _private(
    collection: Mapping[str, Any],
    *,
    store: HistoricalDayStore,
) -> tuple[dict[str, Any], bytes]:
    info = collection["private_artifact"]
    path = store.root / str(info["cache_relative_path"])
    raw = path.read_bytes()
    if hashlib.sha256(raw).hexdigest() != info["file_sha256"]:
        raise EarningsMassiveInspectionError(
            "private earnings metadata file hash differs"
        )
    try:
        value = json.loads(gzip.decompress(raw))
    except (gzip.BadGzipFile, json.JSONDecodeError) as exc:
        raise EarningsMassiveInspectionError(
            "private earnings metadata is unreadable"
        ) from exc
    if not isinstance(value, dict):
        raise EarningsMassiveInspectionError(
            "private earnings metadata must be an object"
        )
    if not (
        value.get("content_sha256")
        == source.self_hash(value, "content_sha256")
        == info["content_sha256"]
    ):
        raise EarningsMassiveInspectionError(
            "private earnings metadata content hash differs"
        )
    return value, raw


def _event_key(row: Mapping[str, Any]) -> tuple[str, str, str, str, str]:
    return (
        str(row.get("ticker") or "").strip().upper(),
        str(row.get("date") or ""),
        str(row.get("time") or ""),
        str(row.get("fiscal_year") or ""),
        str(row.get("fiscal_period") or ""),
    )


def _eligible(row: Mapping[str, Any]) -> bool:
    try:
        actual = float(row["actual_eps"])
        estimate = float(row["estimated_eps"])
    except (KeyError, TypeError, ValueError):
        return False
    ticker, event_date, event_time, _, _ = _event_key(row)
    try:
        parsed = date.fromisoformat(event_date)
    except ValueError:
        return False
    return bool(
        ticker
        and event_time
        and source.FIRST_DATE <= parsed.isoformat() <= source.LAST_DATE
        and str(row.get("date_status") or "").lower() == "confirmed"
        and actual > estimate
    )


def inspect_collection(
    collection_path: Path,
    *,
    inspected_at: str,
    store: HistoricalDayStore | None = None,
    root: Path = source.DEFAULT_ROOT,
) -> tuple[Path, dict[str, Any]]:
    strategy_discovery.require_committed(collection_path)
    collection = source._read(collection_path)
    contract_path = source.PROJECT_ROOT / str(collection["contract_path"])
    inspection_path = source.PROJECT_ROOT / str(collection["inspection_path"])
    strategy_discovery.require_committed(contract_path)
    strategy_discovery.require_committed(inspection_path)
    contract = source._read(contract_path)
    contract_inspection = source._read(inspection_path)
    private, raw = _private(
        collection, store=store or HistoricalDayStore.from_env()
    )
    pages = private.get("pages", [])
    rows = [
        row
        for page in pages
        for row in page.get("rows", [])
        if isinstance(row, Mapping)
    ]
    counts = Counter(_event_key(row) for row in rows if _eligible(row))
    unique = sorted(key for key, count in counts.items() if count == 1)
    duplicate_count = sum(count for count in counts.values() if count > 1)
    unique_dates = {key[1] for key in unique}
    unique_symbols = {key[0] for key in unique}
    checks = {
        "collection_hash_valid": collection.get("collection_sha256")
        == source.self_hash(collection, "collection_sha256"),
        "contract_binding_valid": private.get("contract_sha256")
        == collection.get("contract_sha256")
        == contract.get("contract_sha256"),
        "contract_inspection_valid": contract_inspection.get("state")
        == "METADATA_CONTRACT_INSPECTED_READY"
        and contract_inspection.get("valid") is True,
        "all_frozen_requests_present": len(pages)
        == len(contract.get("requests", []))
        == 15,
        "request_order_and_hashes_match": [
            page.get("request_sha256") for page in pages
        ]
        == [row.get("request_sha256") for row in contract["requests"]],
        "row_count_rebuilt": len(rows) == collection.get("row_count"),
        "private_file_hash_valid": hashlib.sha256(raw).hexdigest()
        == collection["private_artifact"]["file_sha256"],
        "provider_requests_complete": collection.get(
            "provider_telemetry", {}
        ).get("request_count")
        == 15,
        "provider_failures_zero": collection.get(
            "provider_telemetry", {}
        ).get("failures")
        == 0,
        "market_prices_absent": collection.get("market_prices_accessed")
        is False
        and private.get("market_prices_accessed") is False,
        "forward_returns_absent": collection.get("forward_returns_accessed")
        is False
        and private.get("forward_returns_accessed") is False,
        "strategy_metrics_absent": collection.get("strategy_metrics_computed")
        == 0
        and private.get("strategy_metrics_computed") == 0,
        "broker_actions_zero": collection.get("broker_actions") == 0
        and private.get("broker_actions") == 0,
    }
    if not all(checks.values()):
        raise EarningsMassiveInspectionError(
            "Massive earnings metadata collection inspection failed"
        )
    inspected = source._timestamp(inspected_at, "inspected_at")
    capacity_ready = len(unique) >= 100 and len(unique_dates) >= 50
    value: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": "earnings-massive-metadata-collection-inspection",
        "campaign_id": source.CAMPAIGN_ID,
        "family_id": source.FAMILY_ID,
        "successor_id": source.SUCCESSOR_ID,
        "state": (
            "METADATA_CAPACITY_READY"
            if capacity_ready
            else "INSUFFICIENT_METADATA_CAPACITY"
        ),
        "inspected_at": inspected,
        "collection_path": source._repo_path(collection_path),
        "collection_file_sha256": sha256_file(collection_path),
        "collection_sha256": collection["collection_sha256"],
        "contract_sha256": contract["contract_sha256"],
        "checks": checks,
        "row_count": len(rows),
        "formally_eligible_unique_event_count": len(unique),
        "duplicate_eligible_row_count_zero_credit": duplicate_count,
        "eligible_event_date_count": len(unique_dates),
        "eligible_symbol_count": len(unique_symbols),
        "capacity_thresholds": {
            "minimum_unique_events": 100,
            "minimum_event_dates": 50,
        },
        "development_or_confirmation_partition_frozen": False,
        "provider_requests": 0,
        "market_prices_accessed": False,
        "forward_returns_accessed": False,
        "strategy_metrics_computed": 0,
        "confirmation_outcomes_accessed": False,
        "broker_actions": 0,
        "valid": True,
    }
    value["inspection_sha256"] = source.self_hash(
        value, "inspection_sha256"
    )
    path = (
        root
        / "metadata-collection-inspection"
        / f"inspection-{value['inspection_sha256']}.json"
    )
    source._write(path, value)
    return path, value


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    contract = subparsers.add_parser("inspect-contract")
    contract.add_argument("artifact", type=Path)
    contract.add_argument("--inspected-at", required=True)
    collection = subparsers.add_parser("inspect-collection")
    collection.add_argument("artifact", type=Path)
    collection.add_argument("--inspected-at", required=True)
    args = parser.parse_args(argv)
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
                "eligible_events": value.get(
                    "formally_eligible_unique_event_count", 0
                ),
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

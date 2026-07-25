"""Independently inspect v6 legacy SEC provisional capacity."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import earnings_sec_eps_capacity as v5
import earnings_sec_legacy_capacity as source
import outcome_exposure
import strategy_discovery
from historical_store import HistoricalDayStore, sha256_file


class EarningsSecLegacyInspectionError(RuntimeError):
    """The v6 provisional contract or result failed reconstruction."""


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
        "hash_valid": contract.get("contract_sha256")
        == v5.self_hash(contract, "contract_sha256"),
        "exact_rebuild": contract == rebuilt,
        "v5_terminal_bound": contract["source_lineage"][
            "v5_terminal_inspection_sha256"
        ]
        == "88198d4ece6d14267852d4170547c2662c6bd5f6b7b1d31db65b9375c4f6a147",
        "eight_cached_archives_bound": len(
            contract["source_lineage"]["archive_artifacts"]
        )
        == len(v5.ARCHIVES),
        "zero_provider_requests": contract["provider_request_contract"][
            "additional_provider_requests_permitted"
        ]
        == 0,
        "legacy_encoding_exact": contract["observed_legacy_encoding"][
            "provisional_identity_iprx"
        ]
        == 0
        and contract["observed_legacy_encoding"]["eps_iprx"] == 0
        and contract["observed_legacy_encoding"]["eps_uom"] == "USD",
        "provisional_only": contract["observed_legacy_encoding"][
            "security_type_claimed_before_cover_verification"
        ]
        is False,
        "cover_required_before_prices": contract["next_transition"][
            "filing_cover_contract_required"
        ]
        is True
        and contract["next_transition"]["market_price_access_permitted"] is False,
        "outcome_index_current": contract["outcome_exposure_index_sha256"]
        == outcome_exposure.audit()["index_sha256"],
        "outcomes_absent": contract["market_prices_accessed"] is False
        and contract["forward_returns_accessed"] is False
        and contract["strategy_metrics_computed"] == 0
        and contract["confirmation_outcomes_accessed"] is False,
        "broker_actions_zero": contract["broker_actions"] == 0,
    }
    if not all(checks.values()):
        raise EarningsSecLegacyInspectionError(
            "v6 legacy provisional contract inspection failed"
        )
    value: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": "earnings-sec-legacy-provisional-contract-inspection",
        "campaign_id": source.CAMPAIGN_ID,
        "family_id": source.FAMILY_ID,
        "successor_id": source.SUCCESSOR_ID,
        "state": "SEC_LEGACY_CONTRACT_INSPECTED_READY",
        "inspected_at": v5._timestamp(inspected_at, "inspected_at"),
        "contract_path": source._repo_path(contract_path),
        "contract_file_sha256": sha256_file(contract_path),
        "contract_sha256": contract["contract_sha256"],
        "checks": checks,
        "cached_derivation_permitted": True,
        "filing_cover_provider_access_permitted": False,
        "market_price_access_permitted": False,
        "broker_actions": 0,
        "valid": True,
    }
    value["inspection_sha256"] = v5.self_hash(value, "inspection_sha256")
    path = (
        root
        / "provisional-contract-inspection"
        / f"inspection-{value['inspection_sha256']}.json"
    )
    source._write(path, value)
    return path, value


def _private(
    result: dict[str, Any], store: HistoricalDayStore
) -> tuple[dict[str, Any], bytes]:
    info = result["private_artifact"]
    raw = (store.root / info["cache_relative_path"]).read_bytes()
    if hashlib.sha256(raw).hexdigest() != info["file_sha256"]:
        raise EarningsSecLegacyInspectionError("private result hash differs")
    value = json.loads(gzip.decompress(raw))
    if value.get("content_sha256") != v5.self_hash(value, "content_sha256"):
        raise EarningsSecLegacyInspectionError("private content hash differs")
    return value, raw


def inspect_result(
    result_path: Path,
    *,
    inspected_at: str,
    store: HistoricalDayStore | None = None,
    root: Path = source.DEFAULT_ROOT,
) -> tuple[Path, dict[str, Any]]:
    strategy_discovery.require_committed(Path(__file__).resolve())
    strategy_discovery.require_committed(result_path)
    result = source._read(result_path)
    contract_path = source.PROJECT_ROOT / result["contract_path"]
    inspection_path = source.PROJECT_ROOT / result["inspection_path"]
    strategy_discovery.require_committed(contract_path)
    strategy_discovery.require_committed(inspection_path)
    contract = source._read(contract_path)
    inspection = source._read(inspection_path)
    historical_store = store or HistoricalDayStore.from_env()
    private, raw = _private(result, historical_store)
    rebuilt_events: list[dict[str, Any]] = []
    rebuilt_counts: list[dict[str, Any]] = []
    archive_map = {
        item["request_sha256"]: item
        for item in contract["source_lineage"]["archive_artifacts"]
    }
    for request in v5._requests():
        info = archive_map[request["request_sha256"]]
        path = (
            historical_store.root
            / v5.PRIVATE_NAMESPACE
            / "archives"
            / f"{request['request_sha256']}.zip"
        )
        if sha256_file(path) != info["file_sha256"]:
            raise EarningsSecLegacyInspectionError("archive hash differs")
        events, counts = source.derive_archive(path)
        rebuilt_events.extend(events)
        rebuilt_counts.append({"quarter": request["quarter"], **counts})
    keys: dict[tuple[str, str, str], int] = {}
    for event in rebuilt_events:
        key = (event["accepted"], event["ticker"], event["adsh"])
        keys[key] = keys.get(key, 0) + 1
    unique = [
        event
        for event in rebuilt_events
        if keys[(event["accepted"], event["ticker"], event["adsh"])] == 1
    ]
    ranked = source._rank(unique)
    records = outcome_exposure.read_index()
    partitions: dict[str, list[dict[str, Any]]] = {
        "development": [],
        "embargo": [],
        "confirmation": [],
    }
    contaminated = 0
    for event in ranked:
        accepted_date = event["accepted_date"]
        if v5.DEVELOPMENT_START <= accepted_date <= v5.DEVELOPMENT_END:
            partition = "development"
        elif v5.EMBARGO_START <= accepted_date <= v5.EMBARGO_END:
            partition = "embargo"
        elif v5.CONFIRMATION_START <= accepted_date <= v5.CONFIRMATION_END:
            partition = "confirmation"
        else:
            continue
        if outcome_exposure.find_overlaps(
            {"dates": [accepted_date], "symbols": [event["ticker"]]}, records
        ):
            contaminated += 1
        else:
            partitions[partition].append(event)
    counts = {
        key: {
            "events": len(events),
            "event_dates": len({event["accepted_date"] for event in events}),
            "symbols": len({event["ticker"] for event in events}),
        }
        for key, events in partitions.items()
    }
    capacity_ready = (
        counts["development"]["event_dates"] >= v5.MINIMUM_DEVELOPMENT_DATES
        and counts["confirmation"]["event_dates"]
        >= v5.MINIMUM_CONFIRMATION_DATES
        and counts["development"]["events"] + counts["confirmation"]["events"]
        >= v5.MINIMUM_UNIQUE_EVENTS
    )
    checks = {
        "result_hash_valid": result["result_sha256"]
        == v5.self_hash(result, "result_sha256"),
        "contract_and_inspection_valid": inspection["contract_sha256"]
        == result["contract_sha256"]
        == contract["contract_sha256"]
        and inspection["state"] == "SEC_LEGACY_CONTRACT_INSPECTED_READY",
        "archive_counts_rebuilt": rebuilt_counts == private["archive_counts"],
        "pre_rank_count_rebuilt": len(unique)
        == result["pre_rank_unique_events"]
        == private["pre_rank_unique_events"],
        "ranked_events_rebuilt": ranked == private["events"],
        "retained_count_rebuilt": len(ranked)
        == result["retained_provisional_events"],
        "private_hash_valid": hashlib.sha256(raw).hexdigest()
        == result["private_artifact"]["file_sha256"],
        "zero_provider_requests": result["provider_telemetry"]["request_count"] == 0
        and result["provider_telemetry"]["cache_hits"] == len(v5.ARCHIVES),
        "outcomes_absent": result["market_prices_accessed"] is False
        and result["forward_returns_accessed"] is False
        and result["strategy_metrics_computed"] == 0
        and result["confirmation_outcomes_accessed"] is False,
        "broker_actions_zero": result["broker_actions"] == 0,
    }
    if not all(checks.values()):
        raise EarningsSecLegacyInspectionError(
            "v6 legacy provisional result inspection failed"
        )
    value: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": "earnings-sec-legacy-provisional-result-inspection",
        "campaign_id": source.CAMPAIGN_ID,
        "family_id": source.FAMILY_ID,
        "successor_id": source.SUCCESSOR_ID,
        "state": (
            "SEC_LEGACY_PROVISIONAL_CAPACITY_READY"
            if capacity_ready
            else "INSUFFICIENT_SEC_LEGACY_PROVISIONAL_CAPACITY"
        ),
        "inspected_at": v5._timestamp(inspected_at, "inspected_at"),
        "result_path": source._repo_path(result_path),
        "result_file_sha256": sha256_file(result_path),
        "result_sha256": result["result_sha256"],
        "contract_sha256": contract["contract_sha256"],
        "checks": checks,
        "pre_rank_unique_events": len(unique),
        "retained_provisional_events": len(ranked),
        "globally_contaminated_events": contaminated,
        "untouched_partition_counts": counts,
        "filing_cover_contract_freeze_permitted": capacity_ready,
        "filing_cover_provider_access_permitted": False,
        "market_price_access_permitted": False,
        "provider_requests": 0,
        "market_prices_accessed": False,
        "forward_returns_accessed": False,
        "strategy_metrics_computed": 0,
        "confirmation_outcomes_accessed": False,
        "broker_actions": 0,
        "valid": True,
    }
    value["inspection_sha256"] = v5.self_hash(value, "inspection_sha256")
    path = (
        root
        / "provisional-result-inspection"
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
    result = subparsers.add_parser("inspect-result")
    result.add_argument("artifact", type=Path)
    result.add_argument("--inspected-at", required=True)
    args = parser.parse_args(argv)
    if args.command == "inspect-contract":
        path, value = inspect_contract(
            args.artifact, inspected_at=args.inspected_at
        )
    else:
        path, value = inspect_result(
            args.artifact, inspected_at=args.inspected_at
        )
    print(
        json.dumps(
            {
                "path": source._repo_path(path),
                "sha256": value["inspection_sha256"],
                "state": value["state"],
                "provider_requests": 0,
                "partition_counts": value.get("untouched_partition_counts", {}),
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

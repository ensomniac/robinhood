"""Independently inspect external S&P 400/600 addition capacity."""

from __future__ import annotations

import argparse
import hashlib
import json
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import outcome_exposure
import sp500_addition_capacity as source
import sp_mid_small_addition_capacity as capacity
import strategy_discovery


class SpMidSmallAdditionCapacityInspectionError(RuntimeError):
    """The frozen cache graph or event classification did not rebuild."""


def _write_artifact(
    *,
    root: Path,
    lane: str,
    prefix: str,
    value: dict[str, Any],
) -> tuple[Path, dict[str, Any]]:
    value["artifact_sha256"] = capacity._self_hash(value)
    path = root / lane / f"{prefix}-{value['artifact_sha256']}.json"
    source._write(path, value)
    return path, value


def inspect_contract(
    contract_path: Path,
    *,
    inspected_at: str,
    root: Path = capacity.PUBLIC_ROOT,
) -> tuple[Path, dict[str, Any]]:
    """Rebuild the zero-row, zero-request source-reuse contract."""

    source._timestamp(inspected_at)
    strategy_discovery.require_committed(contract_path)
    contract = capacity._load(
        contract_path,
        "sp-mid-small-external-addition-capacity-contract",
    )
    expected_tasks = capacity.source_tasks()
    exposure_audit = outcome_exposure.audit()
    if not (
        contract.get("state") == "SOURCE_REUSE_CONTRACT_FROZEN"
        and contract.get("tasks") == expected_tasks
        and contract.get("task_count") == 312
        and contract.get("implementation_hashes")
        == capacity._implementation_hashes()
        and contract.get("outcome_exposure_index_sha256")
        == exposure_audit["index_sha256"]
        and contract.get("provider_requests_before_contract_freeze") == 0
        and contract.get("cache_rows_accessed_before_contract_freeze") == 0
        and contract.get("market_price_access_permitted") is False
        and contract.get("target_return_access_permitted") is False
        and contract.get("confirmation_access_permitted") is False
        and contract.get("broker_actions_permitted") is False
        and contract.get("market_outcomes_accessed") is False
    ):
        raise SpMidSmallAdditionCapacityInspectionError(
            "source-reuse contract does not independently rebuild"
        )
    result = {
        "schema_version": 1,
        "artifact_kind": (
            "sp-mid-small-external-addition-contract-inspection"
        ),
        "campaign_id": capacity.CAMPAIGN_ID,
        "family_id": capacity.FAMILY_ID,
        "successor_id": capacity.SUCCESSOR_ID,
        "inspected_at": inspected_at,
        "state": "SOURCE_REUSE_CONTRACT_INSPECTED_READY",
        "contract_path": source._repo_path(contract_path),
        "contract_sha256": contract["artifact_sha256"],
        "task_count": len(expected_tasks),
        "inspection": {
            "committed_source_lineage_rebuilt": True,
            "all_cache_identities_rebuilt": True,
            "external_addition_semantics_rebuilt": True,
            "cross_index_transfer_exclusion_rebuilt": True,
            "partitions_rebuilt": True,
            "capacity_thresholds_rebuilt": True,
            "implementation_hashes_rebuilt": True,
            "outcome_exposure_binding_rebuilt": True,
            "zero_access_boundary_rebuilt": True,
            "valid": True,
        },
        "cache_page_access_permitted": True,
        "provider_requests": 0,
        "market_price_access_permitted": False,
        "target_return_access_permitted": False,
        "confirmation_access_permitted": False,
        "broker_actions_permitted": False,
        "market_outcomes_accessed": False,
    }
    return _write_artifact(
        root=root,
        lane="source-reuse-contract-inspection",
        prefix="sp-mid-small-addition-contract-inspection",
        value=result,
    )


def _deduplicate_events(
    releases: Sequence[Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], int]:
    by_identity: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    for release in releases:
        for event in release["eligible_events"]:
            identity = (
                event["ticker"],
                event["effective_date"],
                event["index_name"],
            )
            by_identity.setdefault(identity, []).append(dict(event))
    retained: list[dict[str, Any]] = []
    duplicate_count = 0
    for identity in sorted(by_identity):
        choices = sorted(
            by_identity[identity],
            key=lambda row: (row["announcement_at"], row["source_url"]),
        )
        retained.append(choices[0])
        duplicate_count += len(choices) - 1
    retained.sort(
        key=lambda row: (
            row["announcement_at"],
            row["ticker"],
            row["effective_date"],
            row["index_name"],
        )
    )
    return retained, duplicate_count


def inspect_capacity(
    contract_path: Path,
    inspection_path: Path,
    *,
    inspected_at: str,
    root: Path = capacity.PUBLIC_ROOT,
) -> tuple[Path, dict[str, Any]]:
    """Rehash every page and rebuild the complete event denominator."""

    source._timestamp(inspected_at)
    strategy_discovery.require_committed(contract_path)
    strategy_discovery.require_committed(inspection_path)
    contract = capacity._load(
        contract_path,
        "sp-mid-small-external-addition-capacity-contract",
    )
    predecessor = capacity._load(
        inspection_path,
        "sp-mid-small-external-addition-contract-inspection",
    )
    if not (
        predecessor.get("state")
        == "SOURCE_REUSE_CONTRACT_INSPECTED_READY"
        and predecessor.get("contract_sha256")
        == contract["artifact_sha256"]
        and predecessor.get("cache_page_access_permitted") is True
        and predecessor.get("provider_requests") == 0
        and predecessor.get("market_outcomes_accessed") is False
        and predecessor.get("broker_actions_permitted") is False
    ):
        raise SpMidSmallAdditionCapacityInspectionError(
            "capacity predecessor chain is not ready"
        )
    releases: list[dict[str, Any]] = []
    denominator: list[dict[str, Any]] = []
    total_bytes = 0
    for task in contract["tasks"]:
        cache_path = source._source_path(task["cache_relative_path"])
        raw = source._read_cached(cache_path)
        if (
            hashlib.sha256(raw).hexdigest() != task["raw_sha256"]
            or len(raw) != task["raw_bytes"]
        ):
            raise SpMidSmallAdditionCapacityInspectionError(
                f"source cache drifted: {task['url']}"
            )
        parsed = capacity.parse_release(
            raw,
            source_url=task["url"],
            listed_date=task["listed_date"],
        )
        releases.append(parsed)
        reason_counts: dict[str, int] = {}
        for row in parsed["ineligible_rows"]:
            reason = row["terminal_reason"]
            reason_counts[reason] = reason_counts.get(reason, 0) + 1
        denominator.append(
            {
                "source_url": task["url"],
                "listed_date": task["listed_date"],
                "title": task["title"],
                "terminal_reason": parsed["terminal_reason"],
                "eligible_event_count": len(parsed["eligible_events"]),
                "all_index_action_count": len(parsed["all_index_actions"]),
                "ineligible_reason_counts": dict(sorted(reason_counts.items())),
            }
        )
        total_bytes += len(raw)
    events, duplicate_count = _deduplicate_events(releases)
    development = [
        row
        for row in events
        if row["announcement_date"] <= capacity.DEVELOPMENT_END
    ]
    confirmation = [
        row
        for row in events
        if capacity.CONFIRMATION_START
        <= row["announcement_date"]
        <= capacity.CONFIRMATION_END
    ]
    total_dates = sorted({row["announcement_date"] for row in events})
    development_dates = sorted(
        {row["announcement_date"] for row in development}
    )
    confirmation_dates = sorted(
        {row["announcement_date"] for row in confirmation}
    )
    state = capacity.capacity_state(
        total_dates=len(total_dates),
        development_dates=len(development_dates),
        confirmation_dates=len(confirmation_dates),
    )
    terminal_reason_counts: dict[str, int] = {}
    ineligible_reason_counts: dict[str, int] = {}
    for row in denominator:
        reason = row["terminal_reason"]
        terminal_reason_counts[reason] = (
            terminal_reason_counts.get(reason, 0) + 1
        )
        for ineligible_reason, count in row[
            "ineligible_reason_counts"
        ].items():
            ineligible_reason_counts[ineligible_reason] = (
                ineligible_reason_counts.get(ineligible_reason, 0) + count
            )
    exposure_audit = outcome_exposure.audit()
    result = {
        "schema_version": 1,
        "artifact_kind": (
            "sp-mid-small-external-addition-capacity-inspection"
        ),
        "campaign_id": capacity.CAMPAIGN_ID,
        "family_id": capacity.FAMILY_ID,
        "successor_id": capacity.SUCCESSOR_ID,
        "mechanism_family": capacity.MECHANISM_FAMILY,
        "inspected_at": inspected_at,
        "state": state,
        "contract_path": source._repo_path(contract_path),
        "contract_sha256": contract["artifact_sha256"],
        "contract_inspection_path": source._repo_path(inspection_path),
        "contract_inspection_sha256": predecessor["artifact_sha256"],
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
        "terminal_reason_counts": dict(
            sorted(terminal_reason_counts.items())
        ),
        "ineligible_reason_counts": dict(
            sorted(ineligible_reason_counts.items())
        ),
        "cached_bytes": total_bytes,
        "cache_hits": len(contract["tasks"]),
        "provider_requests": 0,
        "outcome_exposure_index_sha256": exposure_audit["index_sha256"],
        "exact_pair_and_warmup_exposure_check_pending_family_freeze": True,
        "inspection": {
            "all_release_tasks_rebuilt": True,
            "all_cache_hashes_rebuilt": True,
            "publication_timestamps_rebuilt": True,
            "all_composite_index_actions_rebuilt": True,
            "same_security_transfer_exclusions_rebuilt": True,
            "external_addition_deduplication_rebuilt": True,
            "one_entry_per_announcement_date_rebuilt": True,
            "development_confirmation_counts_rebuilt": True,
            "complete_denominator_rebuilt": True,
            "zero_provider_price_return_boundary_rebuilt": True,
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
        prefix="sp-mid-small-addition-capacity-inspection",
        value=result,
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    inspect_contract_parser = sub.add_parser("inspect-contract")
    inspect_contract_parser.add_argument("contract", type=Path)
    inspect_contract_parser.add_argument("--inspected-at", required=True)
    inspect_capacity_parser = sub.add_parser("inspect-capacity")
    inspect_capacity_parser.add_argument("contract", type=Path)
    inspect_capacity_parser.add_argument("inspection", type=Path)
    inspect_capacity_parser.add_argument("--inspected-at", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "inspect-contract":
            path, artifact = inspect_contract(
                args.contract,
                inspected_at=args.inspected_at,
            )
        else:
            path, artifact = inspect_capacity(
                args.contract,
                args.inspection,
                inspected_at=args.inspected_at,
            )
    except (
        SpMidSmallAdditionCapacityInspectionError,
        capacity.SpMidSmallAdditionCapacityError,
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
                "written": source._repo_path(path),
                "provider_requests": artifact.get("provider_requests", 0),
                "market_outcomes_accessed": False,
                "broker_actions_permitted": False,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""Inspect and freeze the completed oversold confirmation inventory."""

from __future__ import annotations

import argparse
import gzip
import io
import json
import os
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import equity_gap_continuation_validation as gap
import outcome_exposure
import oversold_replication_reserve_v2 as reserve
import oversold_replication_source_v3 as scanner_source
import oversold_scanner_builder_v2 as builder
import strategy_discovery
from historical_store import HistoricalDayStore, canonical_sha256, sha256_file


PROJECT_ROOT = Path(__file__).resolve().parent
CAMPAIGN_ID = reserve.CAMPAIGN_ID
FAMILY_ID = "gap-universe-oversold-reversal"
SUCCESSOR_ID = reserve.SUCCESSOR_ID
DATASET_ID = (
    "dataset-short-horizon-oversold-reversal-v5-confirmation-"
    "inventory-2026-07-24-v1"
)
SCANNER_INSPECTION_ROOT = (
    scanner_source.ROOT / "scanner-v2-data-inspection"
)
OUTPUT_ROOT = (
    PROJECT_ROOT
    / "strategy_tournament/v2/oversold_replication"
    / DATASET_ID
)
CONTRACT_ROOT = OUTPUT_ROOT / "confirmation-inventory-contract"
INSPECTION_ROOT = OUTPUT_ROOT / "confirmation-inventory-inspection"


class OversoldReplicationConfirmationV2Error(RuntimeError):
    """The completed confirmation inventory is incomplete or drifted."""


def _read(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise OversoldReplicationConfirmationV2Error(
            f"cannot read {path}: {exc}"
        ) from exc
    if not isinstance(value, dict):
        raise OversoldReplicationConfirmationV2Error(
            f"{path} must contain an object"
        )
    return value


def _private_path(store: HistoricalDayStore) -> Path:
    return (
        store.root
        / "_derived/oversold_replication_confirmation"
        / DATASET_ID
        / "confirmation-inventory.json.gz"
    )


def _write_gzip(path: Path, value: Any) -> None:
    buffer = io.BytesIO()
    with gzip.GzipFile(
        fileobj=buffer, mode="wb", compresslevel=6, mtime=0
    ) as stream:
        stream.write(reserve._canonical(value) + b"\n")
    encoded = buffer.getvalue()
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and path.read_bytes() == encoded:
        return
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_bytes(encoded)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _read_gzip(path: Path) -> dict[str, Any]:
    try:
        with gzip.open(path, "rt", encoding="utf-8") as stream:
            value = json.load(stream)
    except (OSError, json.JSONDecodeError) as exc:
        raise OversoldReplicationConfirmationV2Error(
            f"cannot read private inventory: {exc}"
        ) from exc
    if not isinstance(value, dict):
        raise OversoldReplicationConfirmationV2Error(
            "private inventory must be an object"
        )
    return value


def _selection() -> tuple[Path, dict[str, Any]]:
    path = reserve._one(reserve.SELECTION_ROOT)
    strategy_discovery.require_committed(path)
    value = reserve._load_hashed(
        path,
        identity_field="selection_sha256",
        expected_kind=(
            "oversold_replication_completed_reserve_selection"
        ),
    )
    reserve._assert_dates_globally_untouched(
        value["confirmation_dates"],
        outcome_exposure.read_index(),
    )
    return path, value


def _candidate_graph(
    detail: Mapping[str, Any],
    dates: Sequence[str],
) -> dict[str, list[dict[str, Any]]]:
    raw_dates = detail.get("dates")
    if not isinstance(raw_dates, Mapping) or set(raw_dates) != set(dates):
        raise OversoldReplicationConfirmationV2Error(
            "scanner detail does not cover exact reserve dates"
        )
    return {
        day: gap._candidates(day, raw_dates[day])
        for day in dates
    }


def inspect_scanner_data() -> tuple[Path, dict[str, Any]]:
    build_path = reserve._one(builder.BUILD_STATUS_ROOT)
    target_path = reserve._one(
        __import__(
            "oversold_scanner_target_audit"
        ).COLLECTION_INSPECTION_ROOT
    )
    selection_path, selection = _selection()
    for path in (
        Path(__file__).resolve(),
        build_path,
        target_path,
        selection_path,
    ):
        strategy_discovery.require_committed(path)
    build_status = reserve._load_hashed(
        build_path,
        identity_field="status_sha256",
        expected_kind="oversold_replication_scanner_build_status",
    )
    detail = _read(scanner_source.DETAIL_PATH)
    summary = _read(scanner_source.SUMMARY_PATH)
    dates = list(selection["confirmation_dates"])
    candidates = _candidate_graph(detail, dates)
    checks = {
        "detail_hash_bound": sha256_file(scanner_source.DETAIL_PATH)
        == build_status["detail_file_sha256"],
        "summary_hash_bound": sha256_file(scanner_source.SUMMARY_PATH)
        == build_status["summary_file_sha256"],
        "exact_dates": list(detail["dates"]) == dates,
        "selection_time": detail.get("selection_time_et") == "09:35:00",
        "information_cutoff": detail.get("information_cutoff")
        == "TARGET_SESSION_09:35_ET",
        "summary_complete": summary.get("completed_dates") == 40,
        "all_dates_signal_capable": all(candidates[day] for day in dates),
        "no_outcomes": build_status[
            "target_outcomes_observed_or_derived"
        ]
        is False,
    }
    if not all(checks.values()):
        raise OversoldReplicationConfirmationV2Error(
            "scanner data inspection failed"
        )
    content = {
        "schema_version": 1,
        "artifact_kind": "oversold_replication_scanner_data_inspection",
        "campaign_id": CAMPAIGN_ID,
        "successor_id": SUCCESSOR_ID,
        "state": "SCANNER_DATA_INSPECTED_CONFIRMATION_INVENTORY_READY",
        "build_status_path": scanner_source.base._repo_path(build_path),
        "build_status_file_sha256": sha256_file(build_path),
        "build_status_sha256": build_status["status_sha256"],
        "detail_file_sha256": sha256_file(scanner_source.DETAIL_PATH),
        "summary_file_sha256": sha256_file(scanner_source.SUMMARY_PATH),
        "dates": len(dates),
        "signal_capable_sessions": sum(
            bool(candidates[day]) for day in dates
        ),
        "candidate_symbol_sessions": sum(
            len(candidates[day]) for day in dates
        ),
        "minimum_candidates": min(
            len(candidates[day]) for day in dates
        ),
        "maximum_candidates": max(
            len(candidates[day]) for day in dates
        ),
        "checks": checks,
        "provider_requests": 0,
        "full_session_target_prices_accessed": False,
        "target_outcomes_observed_or_derived": False,
        "broker_actions": 0,
        "valid": True,
    }
    return reserve._publish(
        SCANNER_INSPECTION_ROOT,
        "oversold-replication-scanner-data-inspection",
        content,
        "inspection_sha256",
    )


def _inventory(
    detail: Mapping[str, Any],
    selection: Mapping[str, Any],
) -> dict[str, Any]:
    dates = list(selection["confirmation_dates"])
    candidates = _candidate_graph(detail, dates)
    scope = {
        "dates": dates,
        "symbols_by_date": {
            day: [str(row["symbol"]) for row in candidates[day]]
            for day in dates
        },
    }
    value = {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "lane": "confirmation",
        "embargo_dates": list(selection["embargo_dates"]),
        "evaluation_dates": dates,
        "signal_dates": [
            day for day in dates if candidates[day]
        ],
        "zero_signal_dates": [
            day for day in dates if not candidates[day]
        ],
        "candidates_by_date": candidates,
        "outcome_scope": scope,
        "source_detail_sha256": sha256_file(
            scanner_source.DETAIL_PATH
        ),
        "target_outcomes_observed_or_derived": False,
        "confirmation_access_permitted_before_winner_freeze": False,
        "broker_actions": 0,
    }
    value["content_sha256"] = reserve._hash(value)
    return value


def freeze_inventory(
    *,
    store: HistoricalDayStore | None = None,
) -> tuple[Path, dict[str, Any]]:
    scanner_inspection_path = reserve._one(SCANNER_INSPECTION_ROOT)
    selection_path, selection = _selection()
    for path in (
        Path(__file__).resolve(),
        scanner_inspection_path,
        selection_path,
    ):
        strategy_discovery.require_committed(path)
    scanner_inspection = reserve._load_hashed(
        scanner_inspection_path,
        identity_field="inspection_sha256",
        expected_kind="oversold_replication_scanner_data_inspection",
    )
    if scanner_inspection["valid"] is not True:
        raise OversoldReplicationConfirmationV2Error(
            "scanner inspection is not ready"
        )
    source = store or HistoricalDayStore.from_env()
    inventory = _inventory(
        _read(scanner_source.DETAIL_PATH),
        selection,
    )
    reserve._assert_dates_globally_untouched(
        inventory["evaluation_dates"],
        outcome_exposure.read_index(),
    )
    _write_gzip(_private_path(source), inventory)
    content = {
        "schema_version": 1,
        "artifact_kind": (
            "oversold_replication_confirmation_inventory_contract"
        ),
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "successor_id": SUCCESSOR_ID,
        "dataset_id": DATASET_ID,
        "state": "CONFIRMATION_INVENTORY_FROZEN_AWAITING_INSPECTION",
        "scanner_inspection_path": scanner_source.base._repo_path(
            scanner_inspection_path
        ),
        "scanner_inspection_file_sha256": sha256_file(
            scanner_inspection_path
        ),
        "scanner_inspection_sha256": scanner_inspection[
            "inspection_sha256"
        ],
        "private_inventory_file_sha256": sha256_file(
            _private_path(source)
        ),
        "private_inventory_content_sha256": inventory[
            "content_sha256"
        ],
        "embargo_dates": len(inventory["embargo_dates"]),
        "evaluation_dates": len(inventory["evaluation_dates"]),
        "signal_capable_sessions": len(inventory["signal_dates"]),
        "candidate_symbol_sessions": sum(
            len(rows) for rows in inventory["candidates_by_date"].values()
        ),
        "outcome_scope_sha256": canonical_sha256(
            inventory["outcome_scope"]
        ),
        "full_session_prices_accessed": False,
        "target_outcomes_observed_or_derived": False,
        "confirmation_access_permitted_before_winner_freeze": False,
        "substitutions_allowed": False,
        "broker_actions": 0,
    }
    return reserve._publish(
        CONTRACT_ROOT,
        "oversold-replication-confirmation-inventory-contract",
        content,
        "contract_sha256",
    )


def inspect_inventory(
    contract_path: Path,
    *,
    store: HistoricalDayStore | None = None,
) -> tuple[Path, dict[str, Any]]:
    strategy_discovery.require_committed(contract_path)
    contract = reserve._load_hashed(
        contract_path,
        identity_field="contract_sha256",
        expected_kind=(
            "oversold_replication_confirmation_inventory_contract"
        ),
    )
    _selection_path, selection = _selection()
    rebuilt = _inventory(
        _read(scanner_source.DETAIL_PATH),
        selection,
    )
    source = store or HistoricalDayStore.from_env()
    recorded = _read_gzip(_private_path(source))
    checks = {
        "private_exact_rebuild": recorded == rebuilt,
        "content_hash": rebuilt["content_sha256"]
        == contract["private_inventory_content_sha256"],
        "file_hash": sha256_file(_private_path(source))
        == contract["private_inventory_file_sha256"],
        "five_session_embargo": len(rebuilt["embargo_dates"]) == 5,
        "forty_signal_sessions": len(rebuilt["signal_dates"]) == 40,
        "chronological": rebuilt["evaluation_dates"]
        == sorted(rebuilt["evaluation_dates"]),
        "globally_untouched": not (
            set(rebuilt["evaluation_dates"])
            & reserve.dense_capacity_inventory._globally_exposed_dates(
                outcome_exposure.read_index()
            )
        ),
        "winner_required": contract[
            "confirmation_access_permitted_before_winner_freeze"
        ]
        is False,
        "no_full_session_access": contract[
            "full_session_prices_accessed"
        ]
        is False,
        "no_broker_actions": contract["broker_actions"] == 0,
    }
    if not all(checks.values()):
        raise OversoldReplicationConfirmationV2Error(
            "confirmation inventory inspection failed"
        )
    content = {
        "schema_version": 1,
        "artifact_kind": (
            "oversold_replication_confirmation_inventory_inspection"
        ),
        "campaign_id": CAMPAIGN_ID,
        "successor_id": SUCCESSOR_ID,
        "state": "CONFIRMATION_INVENTORY_INSPECTED_READY",
        "contract_path": scanner_source.base._repo_path(contract_path),
        "contract_file_sha256": sha256_file(contract_path),
        "contract_sha256": contract["contract_sha256"],
        "checks": checks,
        "embargo_dates": 5,
        "evaluation_dates": 40,
        "signal_capable_sessions": 40,
        "candidate_symbol_sessions": contract[
            "candidate_symbol_sessions"
        ],
        "provider_requests": 0,
        "full_session_prices_accessed": False,
        "target_outcomes_observed_or_derived": False,
        "broker_actions": 0,
        "valid": True,
    }
    return reserve._publish(
        INSPECTION_ROOT,
        "oversold-replication-confirmation-inventory-inspection",
        content,
        "inspection_sha256",
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("inspect-scanner-data")
    sub.add_parser("freeze-inventory")
    inspect = sub.add_parser("inspect-inventory")
    inspect.add_argument("contract", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "inspect-scanner-data":
            path, value = inspect_scanner_data()
        elif args.command == "freeze-inventory":
            path, value = freeze_inventory()
        else:
            path, value = inspect_inventory(args.contract)
        print(
            json.dumps(
                {
                    "path": scanner_source.base._repo_path(path),
                    **value,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    except (
        KeyError,
        OSError,
        OversoldReplicationConfirmationV2Error,
        reserve.OversoldReplicationReserveError,
        ValueError,
    ) as exc:
        print(
            json.dumps(
                {"state": "BLOCKED", "error": str(exc)},
                indent=2,
                sort_keys=True,
            )
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())

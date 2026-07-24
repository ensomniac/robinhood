"""Inspect and freeze the untouched 2026 oversold confirmation inventory.

The scanner source is limited to information observable at 09:35 ET.  This
controller independently validates that source, preserves the first five
sessions as an embargo, derives the unchanged 2-8% opening-gap candidate graph
for the remaining sessions, and proves it outcome-clean.  It never opens
full-session confirmation bars or computes returns.
"""

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
import oversold_replication_development as development
import oversold_replication_source_v2 as scanner_source
import strategy_discovery
from historical_store import (
    HistoricalDayStore,
    canonical_sha256,
    sha256_file,
)
from scanner_replay_alpaca import load_contract


PROJECT_ROOT = Path(__file__).resolve().parent
SCHEMA_VERSION = 1
CAMPAIGN_ID = development.CAMPAIGN_ID
FAMILY_ID = "gap-universe-oversold-reversal"
SUCCESSOR_ID = development.SUCCESSOR_ID
DATASET_ID = (
    "dataset-short-horizon-oversold-reversal-v4-confirmation-"
    "inventory-2026-07-24-v1"
)
SELECTION_PATH = scanner_source.base.SELECTION_PATH
SELECTION_INSPECTION_PATH = scanner_source.base.SELECTION_INSPECTION_PATH
FRONT_EMBARGO_SESSIONS = scanner_source.base.FRONT_EMBARGO_SESSIONS
SCANNER_DATA_INSPECTION_ROOT = (
    scanner_source.ROOT / "scanner-v2-data-inspection"
)
OUTPUT_ROOT = (
    PROJECT_ROOT
    / "strategy_tournament/v2/oversold_replication"
    / DATASET_ID
)
CONTRACT_ROOT = OUTPUT_ROOT / "confirmation-inventory-contract"
INSPECTION_ROOT = OUTPUT_ROOT / "confirmation-inventory-inspection"


class OversoldReplicationConfirmationError(RuntimeError):
    """The 2026 confirmation inventory is incomplete or contaminated."""


def _read(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise OversoldReplicationConfirmationError(
            f"cannot read {path}: {exc}"
        ) from exc
    if not isinstance(value, dict):
        raise OversoldReplicationConfirmationError(
            f"{path} must contain an object"
        )
    return value


def _write(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    rendered = json.dumps(value, indent=2, sort_keys=True) + "\n"
    if path.exists() and path.read_text(encoding="utf-8") == rendered:
        return
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_text(rendered, encoding="utf-8")
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _gzip_bytes(value: Any) -> bytes:
    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode="wb", compresslevel=6, mtime=0) as stream:
        stream.write(development._canonical(value) + b"\n")
    return buffer.getvalue()


def _write_gzip(path: Path, value: Any) -> None:
    encoded = _gzip_bytes(value)
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and path.read_bytes() == encoded:
        return
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_bytes(encoded)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _repo_path(path: Path) -> str:
    try:
        return path.resolve().relative_to(PROJECT_ROOT.resolve()).as_posix()
    except ValueError as exc:
        raise OversoldReplicationConfirmationError(
            f"path escaped repository: {path}"
        ) from exc


def _one(root: Path, pattern: str = "*.json") -> Path:
    paths = sorted(root.glob(pattern))
    if len(paths) != 1:
        raise OversoldReplicationConfirmationError(
            f"expected exactly one artifact under {root}"
        )
    return paths[0]


def _publish(
    root: Path,
    prefix: str,
    content: Mapping[str, Any],
    identity_field: str,
) -> tuple[Path, dict[str, Any]]:
    value = dict(content)
    identity = development._hash(value)
    value[identity_field] = identity
    path = root / f"{prefix}-{identity}.json"
    if path.exists() and _read(path) != value:
        raise OversoldReplicationConfirmationError(
            f"hash-addressed artifact drifted: {path}"
        )
    if not path.exists():
        _write(path, value)
    return path, value


def _load_hashed(
    path: Path,
    *,
    identity_field: str,
    expected_kind: str,
) -> dict[str, Any]:
    value = _read(path)
    supplied = value.pop(identity_field, None)
    expected = development._hash(value)
    value[identity_field] = supplied
    if (
        supplied != expected
        or not path.name.endswith(f"-{expected}.json")
        or value.get("artifact_kind") != expected_kind
    ):
        raise OversoldReplicationConfirmationError(
            f"invalid {expected_kind}: {path}"
        )
    return value


def _private_root(store: HistoricalDayStore) -> Path:
    return (
        store.root
        / "_derived/oversold_replication_confirmation"
        / DATASET_ID
    )


def _inventory_path(store: HistoricalDayStore) -> Path:
    return _private_root(store) / "confirmation-inventory.json.gz"


def inspect_scanner_data() -> tuple[Path, dict[str, Any]]:
    manifest_path = scanner_source._scanner_manifest_path()
    required = (
        manifest_path,
        scanner_source.CONTROLLER_BINDING_PATH,
        scanner_source.SCANNER_CONTRACT_INSPECTION_PATH,
        scanner_source.SCANNER_STATUS_PATH,
        scanner_source.DETAIL_PATH,
        scanner_source.SUMMARY_PATH,
        SELECTION_PATH,
        SELECTION_INSPECTION_PATH,
    )
    for path in required[:4] + required[6:]:
        strategy_discovery.require_committed(path)
    scanner_source.validate_identity_recovery()
    scanner_source.validate_controller_binding()
    manifest = load_contract(manifest_path)
    contract_inspection = _read(
        scanner_source.SCANNER_CONTRACT_INSPECTION_PATH
    )
    status = _read(scanner_source.SCANNER_STATUS_PATH)
    selection = _read(SELECTION_PATH)
    detail = _read(scanner_source.DETAIL_PATH)
    summary = _read(scanner_source.SUMMARY_PATH)
    dates = list(selection["selected_dates"])
    evaluated = detail.get("dates")
    if not isinstance(evaluated, Mapping):
        raise OversoldReplicationConfirmationError(
            "scanner detail dates are malformed"
        )
    symbol_sessions = 0
    candidate_sessions = 0
    for day in dates:
        raw = evaluated.get(day)
        if not isinstance(raw, Mapping):
            raise OversoldReplicationConfirmationError(
                f"scanner detail is missing {day}"
            )
        rows = raw.get("evaluations")
        if not isinstance(rows, list):
            raise OversoldReplicationConfirmationError(
                f"scanner evaluations are malformed on {day}"
            )
        symbols = [str(row.get("symbol") or "") for row in rows]
        if any(not symbol for symbol in symbols) or len(symbols) != len(
            set(symbols)
        ):
            raise OversoldReplicationConfirmationError(
                f"scanner symbols are ambiguous on {day}"
            )
        symbol_sessions += len(rows)
        candidate_sessions += len(gap._candidates(day, raw))
    checks = {
        "manifest_bound": manifest["manifest_sha256"]
        == contract_inspection["manifest_sha256"],
        "collection_complete": status.get("state")
        == "SCANNER_INPUTS_COLLECTED",
        "zero_substitutions": status.get("substitutions") == 0,
        "exact_dates": set(evaluated) == set(dates)
        and len(dates) == 135,
        "selection_time": detail.get("selection_time_et")
        == "09:35:00",
        "information_cutoff": detail.get("information_cutoff")
        == "TARGET_SESSION_09:35_ET",
        "summary_complete": summary.get("completed_dates") == 135,
        "detail_hash_bound": summary.get(
            "detailed_artifact",
            {},
        ).get("sha256")
        == sha256_file(scanner_source.DETAIL_PATH),
        "no_target_outcomes": status.get(
            "target_outcomes_observed_or_derived"
        )
        is False,
        "no_broker_actions": status.get("broker_actions") == 0,
    }
    if not all(checks.values()):
        raise OversoldReplicationConfirmationError(
            "2026 scanner data inspection failed"
        )
    content = {
        "schema_version": SCHEMA_VERSION,
        "artifact_kind": "oversold_replication_scanner_data_inspection",
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "successor_id": SUCCESSOR_ID,
        "state": "SCANNER_DATA_INSPECTED_CONFIRMATION_INVENTORY_READY",
        "manifest_path": _repo_path(manifest_path),
        "manifest_file_sha256": sha256_file(manifest_path),
        "manifest_sha256": manifest["manifest_sha256"],
        "contract_inspection_path": _repo_path(
            scanner_source.SCANNER_CONTRACT_INSPECTION_PATH
        ),
        "contract_inspection_file_sha256": sha256_file(
            scanner_source.SCANNER_CONTRACT_INSPECTION_PATH
        ),
        "collection_status_path": _repo_path(
            scanner_source.SCANNER_STATUS_PATH
        ),
        "collection_status_file_sha256": sha256_file(
            scanner_source.SCANNER_STATUS_PATH
        ),
        "detail_path": _repo_path(scanner_source.DETAIL_PATH),
        "detail_file_sha256": sha256_file(scanner_source.DETAIL_PATH),
        "summary_path": _repo_path(scanner_source.SUMMARY_PATH),
        "summary_file_sha256": sha256_file(scanner_source.SUMMARY_PATH),
        "dates": len(dates),
        "evaluated_symbol_sessions": symbol_sessions,
        "candidate_symbol_sessions": candidate_sessions,
        "checks": checks,
        "provider_requests": 0,
        "target_outcomes_observed_or_derived": False,
        "broker_actions": 0,
        "valid": True,
    }
    return _publish(
        SCANNER_DATA_INSPECTION_ROOT,
        "oversold-replication-scanner-data-inspection",
        content,
        "inspection_sha256",
    )


def _build_inventory(
    detail: Mapping[str, Any],
    selected_dates: Sequence[str],
) -> dict[str, Any]:
    dates = list(map(str, selected_dates))
    if (
        len(dates) != 135
        or dates != sorted(dates)
        or len(set(dates)) != 135
    ):
        raise OversoldReplicationConfirmationError(
            "confirmation selection dates drifted"
        )
    embargo_dates = dates[:FRONT_EMBARGO_SESSIONS]
    confirmation_dates = dates[FRONT_EMBARGO_SESSIONS:]
    raw_dates = detail.get("dates")
    if not isinstance(raw_dates, Mapping) or set(raw_dates) != set(dates):
        raise OversoldReplicationConfirmationError(
            "scanner detail does not cover the selection"
        )
    candidates_by_date = {
        day: gap._candidates(day, raw_dates[day])
        for day in confirmation_dates
    }
    signal_dates = [
        day for day in confirmation_dates if candidates_by_date[day]
    ]
    scope = {
        "dates": signal_dates,
        "symbols_by_date": {
            day: [
                str(row["symbol"])
                for row in candidates_by_date[day]
            ]
            for day in signal_dates
        },
    }
    inventory = {
        "schema_version": SCHEMA_VERSION,
        "dataset_id": DATASET_ID,
        "lane": "confirmation",
        "embargo_dates": embargo_dates,
        "evaluation_dates": confirmation_dates,
        "signal_dates": signal_dates,
        "zero_signal_dates": [
            day
            for day in confirmation_dates
            if not candidates_by_date[day]
        ],
        "candidates_by_date": candidates_by_date,
        "outcome_scope": scope,
        "source_detail_sha256": sha256_file(
            scanner_source.DETAIL_PATH
        ),
        "target_outcomes_observed_or_derived": False,
        "confirmation_access_permitted_before_winner_freeze": False,
        "broker_actions": 0,
    }
    inventory["content_sha256"] = development._hash(inventory)
    return inventory


def _contract_content(
    *,
    inventory: Mapping[str, Any],
    store: HistoricalDayStore,
) -> dict[str, Any]:
    scanner_inspection_path = _one(SCANNER_DATA_INSPECTION_ROOT)
    strategy_discovery.require_committed(scanner_inspection_path)
    scanner_inspection = _load_hashed(
        scanner_inspection_path,
        identity_field="inspection_sha256",
        expected_kind="oversold_replication_scanner_data_inspection",
    )
    outcome_exposure.assert_untouched(
        inventory["outcome_scope"],
        outcome_exposure.read_index(),
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "artifact_kind": (
            "oversold_replication_confirmation_inventory_contract"
        ),
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "successor_id": SUCCESSOR_ID,
        "dataset_id": DATASET_ID,
        "state": "CONFIRMATION_INVENTORY_FROZEN_AWAITING_INSPECTION",
        "scanner_binding": {
            "inspection_path": _repo_path(scanner_inspection_path),
            "inspection_file_sha256": sha256_file(
                scanner_inspection_path
            ),
            "inspection_sha256": scanner_inspection[
                "inspection_sha256"
            ],
            "detail_file_sha256": scanner_inspection[
                "detail_file_sha256"
            ],
        },
        "inventory": {
            "private_content_sha256": inventory["content_sha256"],
            "private_file_sha256": sha256_file(
                _inventory_path(store)
            ),
            "embargo_dates": len(inventory["embargo_dates"]),
            "evaluation_dates": len(inventory["evaluation_dates"]),
            "signal_capable_sessions": len(inventory["signal_dates"]),
            "zero_signal_dates": len(inventory["zero_signal_dates"]),
            "candidate_symbol_sessions": sum(
                len(rows)
                for rows in inventory["candidates_by_date"].values()
            ),
            "first_date": inventory["evaluation_dates"][0],
            "last_date": inventory["evaluation_dates"][-1],
        },
        "outcome_boundary": {
            "global_outcome_index_sha256": outcome_exposure.audit()[
                "index_sha256"
            ],
            "outcome_scope_sha256": canonical_sha256(
                inventory["outcome_scope"]
            ),
            "outcome_clean": True,
            "full_session_prices_accessed": False,
            "target_outcomes_observed_or_derived": False,
            "confirmation_access_permitted_before_winner_freeze": False,
            "substitutions_allowed": False,
            "broker_actions": 0,
        },
        "implementation_binding": {
            "controller_path": _repo_path(Path(__file__).resolve()),
            "controller_sha256": sha256_file(Path(__file__).resolve()),
            "candidate_selector_path": _repo_path(
                Path(gap.__file__).resolve()
            ),
            "candidate_selector_sha256": sha256_file(
                Path(gap.__file__).resolve()
            ),
        },
    }


def freeze_inventory(
    *,
    store: HistoricalDayStore | None = None,
) -> tuple[Path, dict[str, Any]]:
    for path in (
        Path(__file__).resolve(),
        Path(gap.__file__).resolve(),
        SELECTION_PATH,
    ):
        strategy_discovery.require_committed(path)
    source = store or HistoricalDayStore.from_env()
    selection = _read(SELECTION_PATH)
    detail = _read(scanner_source.DETAIL_PATH)
    inventory = _build_inventory(
        detail,
        selection["selected_dates"],
    )
    _write_gzip(_inventory_path(source), inventory)
    content = _contract_content(
        inventory=inventory,
        store=source,
    )
    return _publish(
        CONTRACT_ROOT,
        "oversold-replication-confirmation-inventory-contract",
        content,
        "contract_sha256",
    )


def _load_contract(path: Path) -> dict[str, Any]:
    return _load_hashed(
        path,
        identity_field="contract_sha256",
        expected_kind=(
            "oversold_replication_confirmation_inventory_contract"
        ),
    )


def inspect_inventory(
    contract_path: Path,
    *,
    store: HistoricalDayStore | None = None,
) -> tuple[Path, dict[str, Any]]:
    strategy_discovery.require_committed(contract_path)
    contract = _load_contract(contract_path)
    for path_field, hash_field in (
        ("controller_path", "controller_sha256"),
        ("candidate_selector_path", "candidate_selector_sha256"),
    ):
        path = PROJECT_ROOT / contract["implementation_binding"][
            path_field
        ]
        if sha256_file(path) != contract["implementation_binding"][
            hash_field
        ]:
            raise OversoldReplicationConfirmationError(
                f"confirmation implementation drifted: {path_field}"
            )
    source = store or HistoricalDayStore.from_env()
    selection = _read(SELECTION_PATH)
    detail = _read(scanner_source.DETAIL_PATH)
    rebuilt = _build_inventory(
        detail,
        selection["selected_dates"],
    )
    recorded = development._read_gzip(_inventory_path(source))
    expected_contract = _contract_content(
        inventory=rebuilt,
        store=source,
    )
    checks = {
        "contract_exact_rebuild": {
            key: value
            for key, value in contract.items()
            if key != "contract_sha256"
        }
        == expected_contract,
        "private_inventory_exact_rebuild": recorded == rebuilt,
        "private_inventory_semantic_hash": canonical_sha256(recorded)
        == canonical_sha256(rebuilt),
        "five_session_embargo": len(rebuilt["embargo_dates"]) == 5,
        "chronological_confirmation": rebuilt["evaluation_dates"]
        == sorted(rebuilt["evaluation_dates"]),
        "outcome_clean": contract["outcome_boundary"][
            "outcome_clean"
        ]
        is True,
        "no_full_session_access": contract["outcome_boundary"][
            "full_session_prices_accessed"
        ]
        is False,
        "winner_required_for_access": contract["outcome_boundary"][
            "confirmation_access_permitted_before_winner_freeze"
        ]
        is False,
        "no_substitutions": contract["outcome_boundary"][
            "substitutions_allowed"
        ]
        is False,
        "no_broker_actions": contract["outcome_boundary"][
            "broker_actions"
        ]
        == 0,
    }
    if not all(checks.values()):
        raise OversoldReplicationConfirmationError(
            "confirmation inventory inspection failed"
        )
    content = {
        "schema_version": SCHEMA_VERSION,
        "artifact_kind": (
            "oversold_replication_confirmation_inventory_inspection"
        ),
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "successor_id": SUCCESSOR_ID,
        "dataset_id": DATASET_ID,
        "state": "CONFIRMATION_INVENTORY_INSPECTED_READY",
        "contract_path": _repo_path(contract_path),
        "contract_file_sha256": sha256_file(contract_path),
        "contract_sha256": contract["contract_sha256"],
        "private_inventory_content_sha256": rebuilt[
            "content_sha256"
        ],
        "checks": checks,
        "embargo_dates": len(rebuilt["embargo_dates"]),
        "evaluation_dates": len(rebuilt["evaluation_dates"]),
        "signal_capable_sessions": len(rebuilt["signal_dates"]),
        "zero_signal_dates": len(rebuilt["zero_signal_dates"]),
        "candidate_symbol_sessions": sum(
            len(rows)
            for rows in rebuilt["candidates_by_date"].values()
        ),
        "provider_requests": 0,
        "full_session_prices_accessed": False,
        "return_metrics_computed": 0,
        "confirmation_accessed": False,
        "broker_actions": 0,
        "valid": True,
    }
    return _publish(
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
                    "path": _repo_path(path),
                    "state": value["state"],
                    "dates": value.get(
                        "dates",
                        value.get("evaluation_dates"),
                    ),
                    "candidate_symbol_sessions": value.get(
                        "candidate_symbol_sessions"
                    ),
                    "provider_requests": 0,
                    "full_session_prices_accessed": False,
                    "return_metrics_computed": 0,
                    "confirmation_accessed": False,
                    "broker_actions": 0,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    except (
        KeyError,
        OSError,
        OversoldReplicationConfirmationError,
        outcome_exposure.OutcomeExposureError,
        strategy_discovery.StrategyDiscoveryError,
        ValueError,
    ) as exc:
        print(
            json.dumps(
                {
                    "state": "BLOCKED",
                    "error": str(exc),
                    "full_session_prices_accessed": False,
                    "confirmation_accessed": False,
                    "broker_actions": 0,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())

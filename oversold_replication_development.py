"""Freeze the broad contaminated-development graph for oversold replication.

The source is an already inspected, 09:35-ET information-wall scanner replay
covering 399 completed 2023-2024 sessions.  This controller admits every
point-in-time 2-8% opening-gap common-stock candidate to development, retains
the 299 empty sessions as explicit no-signal days, and freezes the predecessor's
unchanged 32-trial grid.  It does not open full-session bars or compute returns.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import io
import json
import os
from collections.abc import Mapping, Sequence
from datetime import datetime
from pathlib import Path
from typing import Any

import equity_gap_continuation_validation as gap
import gap_protection_successor as source
import outcome_exposure
import portfolio_maturity
import strategy_discovery
from historical_store import (
    HistoricalDayStore,
    canonical_sha256,
    sha256_file,
)


PROJECT_ROOT = Path(__file__).resolve().parent
SCHEMA_VERSION = 1
CAMPAIGN_ID = portfolio_maturity.V2_CAMPAIGN_ID
FAMILY_ID = "short-horizon-oversold-reversal"
SUCCESSOR_ID = "short-horizon-oversold-reversal-v4-broad-replication"
DATASET_ID = (
    "dataset-short-horizon-oversold-reversal-v4-broad-development-"
    "2026-07-24-v1"
)
SOURCE_DETAIL = (
    PROJECT_ROOT
    / "learning_runs/gap_protection_successor/"
    "dataset-equity-gap-protection-continuation-2026-07-24-v2/"
    "scanner-replay-detail.json"
)
SOURCE_CONTRACT = (
    PROJECT_ROOT
    / "strategy_tournament/v2/gap_protection_successor/"
    "dataset-equity-gap-protection-continuation-2026-07-24-v2/"
    "preentry-contract/"
    "gap-protection-preentry-contract-"
    "b2c223bcfe81f92a96882b7974774d424b96f45c32bd89e0e02dae44305ad6d1.json"
)
SOURCE_SUMMARY = (
    PROJECT_ROOT
    / "strategy_tournament/v2/gap_protection_successor/"
    "dataset-equity-gap-protection-continuation-2026-07-24-v2/"
    "preentry-summary/"
    "gap-protection-preentry-summary-"
    "f22b4392e65ea0c3983982481c9b08a59927180b83b51326f4cb5e14794c1218.json"
)
SOURCE_INSPECTION = (
    PROJECT_ROOT
    / "strategy_tournament/v2/gap_protection_successor/"
    "dataset-equity-gap-protection-continuation-2026-07-24-v2/"
    "preentry-inspection/"
    "gap-protection-preentry-inspection-"
    "b82ec51a28d5852538fc1e518d5136dd7ea0060222529f60d0e4a1352e7d6a7c.json"
)
OUTPUT_ROOT = (
    PROJECT_ROOT
    / "strategy_tournament/v2/oversold_replication"
    / DATASET_ID
)
CONTRACT_ROOT = OUTPUT_ROOT / "development-inventory-contract"
INSPECTION_ROOT = OUTPUT_ROOT / "development-inventory-inspection"

PARAMETER_GRID = {
    "lookback_minutes": [15, 30],
    "rsi_maximum": [15.0, 20.0],
    "rsi_period": [3, 5],
    "selloff_threshold": [-0.02, -0.03],
    "target_r": [1.0, 1.5],
}


class OversoldReplicationDevelopmentError(RuntimeError):
    """The broad development evidence is incomplete or has drifted."""


def _canonical(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")


def _hash(value: Any) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _read(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise OversoldReplicationDevelopmentError(
            f"cannot read {path}: {exc}"
        ) from exc
    if not isinstance(value, dict):
        raise OversoldReplicationDevelopmentError(
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
        stream.write(_canonical(value) + b"\n")
    return buffer.getvalue()


def _write_gzip(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = _gzip_bytes(value)
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
        raise OversoldReplicationDevelopmentError(
            f"cannot read {path}: {exc}"
        ) from exc
    if not isinstance(value, dict):
        raise OversoldReplicationDevelopmentError(
            f"{path} must contain an object"
        )
    return value


def _repo_path(path: Path) -> str:
    try:
        return path.resolve().relative_to(PROJECT_ROOT.resolve()).as_posix()
    except ValueError as exc:
        raise OversoldReplicationDevelopmentError(
            f"path escaped repository: {path}"
        ) from exc


def _private_root(store: HistoricalDayStore) -> Path:
    return (
        store.root
        / "_derived/oversold_replication"
        / DATASET_ID
    )


def _inventory_path(store: HistoricalDayStore) -> Path:
    return _private_root(store) / "development-inventory.json.gz"


def _trial_count() -> int:
    result = 1
    for values in PARAMETER_GRID.values():
        result *= len(values)
    return result


def _validate_timestamp(value: str) -> None:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise OversoldReplicationDevelopmentError(
            "created_at is invalid"
        ) from exc
    if parsed.tzinfo is None:
        raise OversoldReplicationDevelopmentError(
            "created_at needs a timezone"
        )


def _source_detail() -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    contract = source._load_contract(SOURCE_CONTRACT)
    summary = _read(SOURCE_SUMMARY)
    inspection = _read(SOURCE_INSPECTION)
    summary_identity = summary.pop("summary_sha256", None)
    inspection_identity = inspection.pop("inspection_sha256", None)
    summary["summary_sha256"] = summary_identity
    inspection["inspection_sha256"] = inspection_identity
    if not (
        summary_identity
        == source._hash(
            {
                key: value
                for key, value in summary.items()
                if key != "summary_sha256"
            }
        )
        and inspection_identity
        == source._hash(
            {
                key: value
                for key, value in inspection.items()
                if key != "inspection_sha256"
            }
        )
        and contract.get("contract_sha256")
        == "b2c223bcfe81f92a96882b7974774d424b96f45c32bd89e0e02dae44305ad6d1"
        and summary_identity
        == "f22b4392e65ea0c3983982481c9b08a59927180b83b51326f4cb5e14794c1218"
        and inspection_identity
        == "b82ec51a28d5852538fc1e518d5136dd7ea0060222529f60d0e4a1352e7d6a7c"
        and inspection.get("state") == "PREENTRY_INSPECTED_READY"
        and inspection.get("valid") is True
        and summary.get("target_outcomes_observed_or_derived") is False
        and summary.get("scanner_summary", {}).get("information_cutoff")
        == "TARGET_SESSION_09:35_ET"
        and summary.get("scanner_summary", {}).get("detail_sha256")
        == sha256_file(SOURCE_DETAIL)
    ):
        raise OversoldReplicationDevelopmentError(
            "inspected 09:35 source binding is invalid"
        )
    detail = _read(SOURCE_DETAIL)
    if not (
        detail.get("information_cutoff") == "TARGET_SESSION_09:35_ET"
        and detail.get("selection_time_et") == "09:35:00"
        and isinstance(detail.get("dates"), Mapping)
    ):
        raise OversoldReplicationDevelopmentError(
            "scanner detail escaped the 09:35 information wall"
        )
    return detail, summary, inspection


def _build_inventory(detail: Mapping[str, Any]) -> dict[str, Any]:
    raw_dates = detail.get("dates")
    if not isinstance(raw_dates, Mapping):
        raise OversoldReplicationDevelopmentError(
            "scanner detail dates are malformed"
        )
    dates = sorted(map(str, raw_dates))
    candidates_by_date = {
        day: gap._candidates(day, raw_dates[day])
        for day in dates
    }
    signal_dates = [
        day for day in dates if candidates_by_date[day]
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
        "lane": "development",
        "evaluation_dates": dates,
        "signal_dates": signal_dates,
        "zero_signal_dates": [
            day for day in dates if not candidates_by_date[day]
        ],
        "candidates_by_date": candidates_by_date,
        "outcome_scope": scope,
        "source_detail_sha256": sha256_file(SOURCE_DETAIL),
        "target_outcomes_observed_or_derived": False,
        "confirmation_access_permitted": False,
        "broker_actions": 0,
    }
    inventory["content_sha256"] = _hash(inventory)
    if not (
        len(dates) == 399
        and dates[0] == "2023-01-26"
        and dates[-1] == "2024-12-24"
        and len(signal_dates) == 100
        and len(inventory["zero_signal_dates"]) == 299
        and sum(len(rows) for rows in candidates_by_date.values()) == 2_334
    ):
        raise OversoldReplicationDevelopmentError(
            "broad development inventory capacity drifted"
        )
    return inventory


def _exposure_accounting(inventory: Mapping[str, Any]) -> dict[str, Any]:
    records = outcome_exposure.read_index()
    overlaps = outcome_exposure.find_overlaps(
        inventory["outcome_scope"],
        records,
    )
    covered = {
        (str(row["date"]), str(row["symbol"]))
        for row in overlaps
    }
    all_pairs = {
        (day, symbol)
        for day, symbols in inventory["outcome_scope"][
            "symbols_by_date"
        ].items()
        for symbol in symbols
    }
    previously_exposed = all_pairs & covered
    return {
        "global_outcome_index_sha256": outcome_exposure.audit()[
            "index_sha256"
        ],
        "candidate_symbol_sessions": len(all_pairs),
        "previously_exposed_symbol_sessions": len(previously_exposed),
        "new_development_symbol_sessions": len(
            all_pairs - previously_exposed
        ),
        "all_assigned_to_development_before_full_session_access": True,
    }


def _contract_content(
    *,
    created_at: str,
    inventory: Mapping[str, Any],
    inventory_path: Path,
) -> dict[str, Any]:
    detail, summary, inspection = _source_detail()
    del detail
    exposure = _exposure_accounting(inventory)
    return {
        "schema_version": SCHEMA_VERSION,
        "artifact_kind": "oversold_replication_development_inventory_contract",
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "successor_id": SUCCESSOR_ID,
        "dataset_id": DATASET_ID,
        "created_at": created_at,
        "state": "DEVELOPMENT_INVENTORY_FROZEN_AWAITING_INSPECTION",
        "source_binding": {
            "contract_path": _repo_path(SOURCE_CONTRACT),
            "contract_file_sha256": sha256_file(SOURCE_CONTRACT),
            "contract_sha256": summary["contract_sha256"],
            "summary_path": _repo_path(SOURCE_SUMMARY),
            "summary_file_sha256": sha256_file(SOURCE_SUMMARY),
            "summary_sha256": summary["summary_sha256"],
            "inspection_path": _repo_path(SOURCE_INSPECTION),
            "inspection_file_sha256": sha256_file(SOURCE_INSPECTION),
            "inspection_sha256": inspection["inspection_sha256"],
            "detail_path": _repo_path(SOURCE_DETAIL),
            "detail_file_sha256": sha256_file(SOURCE_DETAIL),
            "information_cutoff": "TARGET_SESSION_09:35_ET",
        },
        "inventory": {
            "private_content_sha256": inventory["content_sha256"],
            "private_file_sha256": sha256_file(inventory_path),
            "evaluation_dates": len(inventory["evaluation_dates"]),
            "signal_dates": len(inventory["signal_dates"]),
            "zero_signal_dates": len(inventory["zero_signal_dates"]),
            "candidate_symbol_sessions": sum(
                len(rows)
                for rows in inventory["candidates_by_date"].values()
            ),
            "first_date": inventory["evaluation_dates"][0],
            "last_date": inventory["evaluation_dates"][-1],
        },
        "outcome_boundary": {
            **exposure,
            "development_training_contaminated": True,
            "confirmation_dates_or_symbols_included": False,
            "full_session_prices_accessed": False,
            "target_outcomes_observed_or_derived": False,
            "broker_actions": 0,
        },
        "search_contract": {
            "selection_mode": "development_search",
            "parameter_grid": PARAMETER_GRID,
            "trial_count": _trial_count(),
            "grid_changed_from_v3": False,
            "winner_selection": (
                "PORTFOLIO_VALIDATION_V2_SELECTION_AWARE_V1"
            ),
            "maximum_hold_sessions": 1,
            "primary_cost_bps_per_side": 5,
            "stress_cost_bps_per_side": [10, 20],
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


def _publish(
    root: Path,
    prefix: str,
    content: Mapping[str, Any],
    identity_field: str,
) -> tuple[Path, dict[str, Any]]:
    value = dict(content)
    identity = _hash(value)
    value[identity_field] = identity
    path = root / f"{prefix}-{identity}.json"
    if path.exists() and _read(path) != value:
        raise OversoldReplicationDevelopmentError(
            f"hash-addressed artifact drifted: {path}"
        )
    if not path.exists():
        _write(path, value)
    return path, value


def _load_contract(path: Path) -> dict[str, Any]:
    value = _read(path)
    supplied = value.pop("contract_sha256", None)
    expected = _hash(value)
    value["contract_sha256"] = supplied
    if supplied != expected or not path.name.endswith(f"-{expected}.json"):
        raise OversoldReplicationDevelopmentError(
            "development inventory contract hash is invalid"
        )
    return value


def freeze_inventory(
    *,
    created_at: str,
    store: HistoricalDayStore | None = None,
) -> tuple[Path, dict[str, Any]]:
    _validate_timestamp(created_at)
    for path in (
        Path(__file__).resolve(),
        Path(gap.__file__).resolve(),
        SOURCE_CONTRACT,
        SOURCE_SUMMARY,
        SOURCE_INSPECTION,
    ):
        strategy_discovery.require_committed(path)
    source_store = store or HistoricalDayStore.from_env()
    detail, _summary, _inspection = _source_detail()
    inventory = _build_inventory(detail)
    _write_gzip(_inventory_path(source_store), inventory)
    content = _contract_content(
        created_at=created_at,
        inventory=inventory,
        inventory_path=_inventory_path(source_store),
    )
    return _publish(
        CONTRACT_ROOT,
        "oversold-replication-development-inventory-contract",
        content,
        "contract_sha256",
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
            raise OversoldReplicationDevelopmentError(
                f"development implementation drifted: {path_field}"
            )
    source_store = store or HistoricalDayStore.from_env()
    detail, _summary, _inspection = _source_detail()
    rebuilt = _build_inventory(detail)
    recorded = _read_gzip(_inventory_path(source_store))
    expected_contract = _contract_content(
        created_at=str(contract["created_at"]),
        inventory=rebuilt,
        inventory_path=_inventory_path(source_store),
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
        "source_information_wall": contract["source_binding"][
            "information_cutoff"
        ]
        == "TARGET_SESSION_09:35_ET",
        "complete_trial_grid": contract["search_contract"]["trial_count"]
        == 32,
        "explicit_zero_days": contract["inventory"]["zero_signal_dates"]
        == 299,
        "development_only": contract["outcome_boundary"][
            "confirmation_dates_or_symbols_included"
        ]
        is False,
        "no_full_session_access": contract["outcome_boundary"][
            "full_session_prices_accessed"
        ]
        is False,
        "no_outcomes": contract["outcome_boundary"][
            "target_outcomes_observed_or_derived"
        ]
        is False,
        "no_broker_actions": contract["outcome_boundary"]["broker_actions"]
        == 0,
    }
    if not all(checks.values()):
        raise OversoldReplicationDevelopmentError(
            "development inventory inspection failed"
        )
    content = {
        "schema_version": SCHEMA_VERSION,
        "artifact_kind": (
            "oversold_replication_development_inventory_inspection"
        ),
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "successor_id": SUCCESSOR_ID,
        "dataset_id": DATASET_ID,
        "state": "DEVELOPMENT_INVENTORY_INSPECTED_READY",
        "contract_path": _repo_path(contract_path),
        "contract_file_sha256": sha256_file(contract_path),
        "contract_sha256": contract["contract_sha256"],
        "private_inventory_content_sha256": rebuilt["content_sha256"],
        "checks": checks,
        "evaluation_dates": len(rebuilt["evaluation_dates"]),
        "signal_dates": len(rebuilt["signal_dates"]),
        "zero_signal_dates": len(rebuilt["zero_signal_dates"]),
        "candidate_symbol_sessions": sum(
            len(rows)
            for rows in rebuilt["candidates_by_date"].values()
        ),
        "provider_requests": 0,
        "return_metrics_computed": 0,
        "confirmation_accessed": False,
        "broker_actions": 0,
        "valid": True,
    }
    return _publish(
        INSPECTION_ROOT,
        "oversold-replication-development-inventory-inspection",
        content,
        "inspection_sha256",
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    freeze = sub.add_parser("freeze-inventory")
    freeze.add_argument("--created-at", required=True)
    inspect = sub.add_parser("inspect-inventory")
    inspect.add_argument("contract", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "freeze-inventory":
            path, value = freeze_inventory(created_at=args.created_at)
        else:
            path, value = inspect_inventory(args.contract)
        print(
            json.dumps(
                {
                    "path": _repo_path(path),
                    "state": value["state"],
                    "evaluation_dates": value["inventory"]["evaluation_dates"]
                    if "inventory" in value
                    else value["evaluation_dates"],
                    "candidate_symbol_sessions": (
                        value["inventory"]["candidate_symbol_sessions"]
                        if "inventory" in value
                        else value["candidate_symbol_sessions"]
                    ),
                    "provider_requests": 0,
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
        OSError,
        OversoldReplicationDevelopmentError,
        outcome_exposure.OutcomeExposureError,
        strategy_discovery.StrategyDiscoveryError,
    ) as exc:
        print(
            json.dumps(
                {
                    "state": "BLOCKED",
                    "error": str(exc),
                    "provider_requests": 0,
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

"""Freeze and collect a completed, globally untouched oversold reserve.

The predecessor 2026-wide reserve was selected before reconciling the legacy
wildcard outcome index.  This controller retires that reserve before any
full-session target prices are opened, preserves the existing 100-signal
development corpus, freezes five subsequent full sessions as embargo, and
selects the latest 40 already-completed full sessions that are globally
untouched.  Only dated common-stock reference metadata is collected here.
"""

from __future__ import annotations

import argparse
import gzip
import json
import math
import os
from collections.abc import Mapping, Sequence
from datetime import datetime
from pathlib import Path
from typing import Any

import dense_capacity_inventory
import outcome_exposure
import oversold_replication_development as development
import oversold_replication_reference as predecessor
import scanner_replay
import strategy_discovery
from historical_store import sha256_file
from learning_data import (
    load_frozen_dataset_contract,
    load_security_master,
    security_master_sha256,
    security_record_covers,
)


PROJECT_ROOT = Path(__file__).resolve().parent
SCHEMA_VERSION = 1
CAMPAIGN_ID = development.CAMPAIGN_ID
SUCCESSOR_ID = "short-horizon-oversold-reversal-v5-completed-reserve"
DATASET_ID = (
    "dataset-short-horizon-oversold-reversal-v5-completed-reserve-"
    "2026-07-24-v1"
)
ROOT = PROJECT_ROOT / "historical_batches/oversold_replication_v5"
PREDECESSOR_FAILURE_ROOT = ROOT / "predecessor-reserve-failure"
SELECTION_ROOT = ROOT / "selection"
SELECTION_INSPECTION_ROOT = ROOT / "selection-inspection"
REFERENCE_CONTRACT_ROOT = ROOT / "reference-contract"
REFERENCE_CONTRACT_INSPECTION_ROOT = ROOT / "reference-contract-inspection"
REFERENCE_STATUS_ROOT = ROOT / "reference-status"
REFERENCE_DATA_INSPECTION_ROOT = ROOT / "reference-data-inspection"
SECURITY_MASTER = ROOT / "security-master.jsonl"
SECURITY_SOURCE = ROOT / "security-master-source.json"
SECURITY_INSPECTION_ROOT = ROOT / "security-master-inspection"
PRIVATE_REFERENCE_ROOT = predecessor.PRIVATE_REFERENCE_ROOT
CONFIRMATION_SESSIONS = 40
EMBARGO_SESSIONS = 5
SELECTION_CUTOFF = "2026-07-17"
DEVELOPMENT_MANIFEST = (
    PROJECT_ROOT
    / "strategy_tournament/v2/continuous/"
    "short-horizon-oversold-reversal-v4-broad-replication/capacity/"
    "dataset-short-horizon-oversold-reversal-v4-broad-development-"
    "77eb3cdfadd61b99dee987771aff80753bb2074ac2b9b577d5c7f8dad4ddd6a0.json"
)
PREDECESSOR_SELECTION = (
    PROJECT_ROOT
    / "historical_batches/oversold_replication_v4/"
    "selection-2026-oversold-replication.json"
)
PREDECESSOR_REFERENCE_CONTRACT = (
    predecessor.REFERENCE_CONTRACT_ROOT
    / "oversold-replication-reference-contract-"
    "3c6ec8fe5aa239f5ec6817fbf88c04f7643d7f1da1a5d5f0c5d01b2e1afb4d40.json"
)
PREDECESSOR_REFERENCE_INSPECTION = (
    predecessor.REFERENCE_CONTRACT_INSPECTION_ROOT
    / "oversold-replication-reference-contract-inspection-"
    "58079996343774fa19c1bc26746972b0a6d1ec8d2acb499a72dbcce7d15399df.json"
)


class OversoldReplicationReserveError(RuntimeError):
    """The completed reserve is contaminated, incomplete, or drifted."""


def _canonical(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")


def _hash(value: Any) -> str:
    import hashlib

    return hashlib.sha256(_canonical(value)).hexdigest()


def _read(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise OversoldReplicationReserveError(
            f"cannot read {path}: {exc}"
        ) from exc
    if not isinstance(value, dict):
        raise OversoldReplicationReserveError(
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


def _repo_path(path: Path) -> str:
    try:
        return path.resolve().relative_to(PROJECT_ROOT.resolve()).as_posix()
    except ValueError as exc:
        raise OversoldReplicationReserveError(
            f"path escaped repository: {path}"
        ) from exc


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
        raise OversoldReplicationReserveError(
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
    expected = _hash(value)
    value[identity_field] = supplied
    if (
        supplied != expected
        or not path.name.endswith(f"-{expected}.json")
        or value.get("artifact_kind") != expected_kind
    ):
        raise OversoldReplicationReserveError(
            f"invalid {expected_kind}: {path}"
        )
    return value


def _one(root: Path) -> Path:
    paths = sorted(root.glob("*.json"))
    if len(paths) != 1:
        raise OversoldReplicationReserveError(
            f"expected exactly one artifact under {root}"
        )
    return paths[0]


def _timestamp(value: str) -> None:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise OversoldReplicationReserveError(
            "timestamp is invalid"
        ) from exc
    if parsed.tzinfo is None:
        raise OversoldReplicationReserveError(
            "timestamp needs a timezone"
        )


def _calendar() -> list[str]:
    return dense_capacity_inventory._calendar(
        dense_capacity_inventory.DEFAULT_CALENDAR
    )


def _assert_dates_globally_untouched(
    dates: Sequence[str],
    records: Sequence[Mapping[str, Any]],
) -> None:
    exposed_dates = dense_capacity_inventory._globally_exposed_dates(
        records
    )
    overlaps = sorted(set(dates) & exposed_dates)
    if overlaps:
        raise OversoldReplicationReserveError(
            "completed reserve has prior global outcome exposure on "
            f"{len(overlaps)} dates"
        )


def _selection_content(*, created_at: str) -> dict[str, Any]:
    _timestamp(created_at)
    for path in (
        DEVELOPMENT_MANIFEST,
        PREDECESSOR_SELECTION,
        PREDECESSOR_REFERENCE_CONTRACT,
        PREDECESSOR_REFERENCE_INSPECTION,
        outcome_exposure.DEFAULT_INDEX,
    ):
        strategy_discovery.require_committed(path)
    development_manifest = load_frozen_dataset_contract(
        DEVELOPMENT_MANIFEST
    )
    development_dates = list(
        development_manifest["requested_dates"]
    )
    development_last = development_dates[-1]
    calendar = _calendar()
    later = [day for day in calendar if day > development_last]
    embargo_dates = later[:EMBARGO_SESSIONS]
    exposure_records = outcome_exposure.read_index()
    exposed_dates = dense_capacity_inventory._globally_exposed_dates(
        exposure_records
    )
    clean = [
        day
        for day in later[EMBARGO_SESSIONS:]
        if day <= SELECTION_CUTOFF and day not in exposed_dates
    ]
    confirmation_dates = clean[-CONFIRMATION_SESSIONS:]
    if not (
        len(embargo_dates) == EMBARGO_SESSIONS
        and len(confirmation_dates) == CONFIRMATION_SESSIONS
        and development_last < embargo_dates[0]
        and embargo_dates[-1] < confirmation_dates[0]
    ):
        raise OversoldReplicationReserveError(
            "completed untouched reserve capacity is insufficient"
        )
    scope = {"dates": confirmation_dates, "symbols": ["*"]}
    _assert_dates_globally_untouched(
        confirmation_dates,
        exposure_records,
    )
    predecessor_selection = _read(PREDECESSOR_SELECTION)
    overlaps = outcome_exposure.find_overlaps(
        {
            "dates": list(predecessor_selection["selected_dates"]),
            "symbols": ["*"],
        },
        outcome_exposure.read_index(),
    )
    if not overlaps:
        raise OversoldReplicationReserveError(
            "predecessor contamination was not reproduced"
        )
    return {
        "schema_version": SCHEMA_VERSION,
        "artifact_kind": "oversold_replication_completed_reserve_selection",
        "campaign_id": CAMPAIGN_ID,
        "successor_id": SUCCESSOR_ID,
        "dataset_id": DATASET_ID,
        "created_at": created_at,
        "state": "COMPLETED_RESERVE_SELECTED_AWAITING_INSPECTION",
        "development_manifest_path": _repo_path(
            DEVELOPMENT_MANIFEST
        ),
        "development_manifest_sha256": development_manifest[
            "manifest_sha256"
        ],
        "development_last_date": development_last,
        "embargo_dates": embargo_dates,
        "confirmation_dates": confirmation_dates,
        "confirmation_scope": scope,
        "selection_cutoff": SELECTION_CUTOFF,
        "selection_algorithm": (
            "latest 40 completed full regular sessions after the five-session "
            "post-development embargo that have no outcome exposure for any "
            "symbol as of the frozen global index"
        ),
        "outcome_exposure_index_sha256": outcome_exposure.audit()[
            "index_sha256"
        ],
        "predecessor_failure": {
            "state": "RETIRED_CONFIRMATION_CONTAMINATED_BEFORE_PRICE_ACCESS",
            "selection_path": _repo_path(PREDECESSOR_SELECTION),
            "selection_file_sha256": sha256_file(
                PREDECESSOR_SELECTION
            ),
            "reference_contract_path": _repo_path(
                PREDECESSOR_REFERENCE_CONTRACT
            ),
            "reference_contract_file_sha256": sha256_file(
                PREDECESSOR_REFERENCE_CONTRACT
            ),
            "reference_inspection_path": _repo_path(
                PREDECESSOR_REFERENCE_INSPECTION
            ),
            "reference_inspection_file_sha256": sha256_file(
                PREDECESSOR_REFERENCE_INSPECTION
            ),
            "overlap_count": len(overlaps),
            "overlap_dates": sorted(
                {row["date"] for row in overlaps}
            ),
            "full_session_prices_accessed": False,
            "target_outcomes_observed_or_derived": False,
            "broker_actions": 0,
        },
        "calendar_path": _repo_path(
            dense_capacity_inventory.DEFAULT_CALENDAR
        ),
        "calendar_file_sha256": sha256_file(
            dense_capacity_inventory.DEFAULT_CALENDAR
        ),
        "implementation_path": _repo_path(
            Path(__file__).resolve()
        ),
        "implementation_sha256": sha256_file(
            Path(__file__).resolve()
        ),
        "substitutions_allowed": False,
        "market_prices_accessed": False,
        "target_outcomes_observed_or_derived": False,
        "broker_actions": 0,
    }


def freeze_selection(
    *, created_at: str
) -> tuple[Path, dict[str, Any]]:
    for path in (
        Path(__file__).resolve(),
        Path(scanner_replay.__file__).resolve(),
    ):
        strategy_discovery.require_committed(path)
    return _publish(
        SELECTION_ROOT,
        "oversold-replication-completed-reserve-selection",
        _selection_content(created_at=created_at),
        "selection_sha256",
    )


def inspect_selection(
    selection_path: Path,
) -> tuple[Path, dict[str, Any]]:
    strategy_discovery.require_committed(selection_path)
    selection = _load_hashed(
        selection_path,
        identity_field="selection_sha256",
        expected_kind=(
            "oversold_replication_completed_reserve_selection"
        ),
    )
    expected = _selection_content(
        created_at=str(selection["created_at"])
    )
    checks = {
        "exact_rebuild": {
            key: value
            for key, value in selection.items()
            if key != "selection_sha256"
        }
        == expected,
        "five_session_embargo": len(selection["embargo_dates"])
        == EMBARGO_SESSIONS,
        "forty_completed_confirmation_sessions": len(
            selection["confirmation_dates"]
        )
        == CONFIRMATION_SESSIONS,
        "confirmation_globally_untouched": not (
            set(selection["confirmation_dates"])
            & dense_capacity_inventory._globally_exposed_dates(
                outcome_exposure.read_index()
            )
        ),
        "predecessor_retired_before_prices": selection[
            "predecessor_failure"
        ]["full_session_prices_accessed"]
        is False,
        "no_substitutions": selection["substitutions_allowed"] is False,
        "no_outcomes": selection[
            "target_outcomes_observed_or_derived"
        ]
        is False,
        "no_broker_actions": selection["broker_actions"] == 0,
    }
    if not all(checks.values()):
        raise OversoldReplicationReserveError(
            "completed reserve selection inspection failed"
        )
    content = {
        "schema_version": SCHEMA_VERSION,
        "artifact_kind": (
            "oversold_replication_completed_reserve_selection_inspection"
        ),
        "campaign_id": CAMPAIGN_ID,
        "successor_id": SUCCESSOR_ID,
        "state": "COMPLETED_RESERVE_SELECTION_INSPECTED_READY",
        "selection_path": _repo_path(selection_path),
        "selection_file_sha256": sha256_file(selection_path),
        "selection_sha256": selection["selection_sha256"],
        "development_last_date": selection[
            "development_last_date"
        ],
        "embargo_dates": len(selection["embargo_dates"]),
        "confirmation_dates": len(selection["confirmation_dates"]),
        "checks": checks,
        "provider_requests": 0,
        "market_prices_accessed": False,
        "target_outcomes_observed_or_derived": False,
        "broker_actions": 0,
        "valid": True,
    }
    return _publish(
        SELECTION_INSPECTION_ROOT,
        "oversold-replication-completed-reserve-selection-inspection",
        content,
        "inspection_sha256",
    )


def _selection_chain() -> tuple[Path, dict[str, Any], Path, dict[str, Any]]:
    selection_path = _one(SELECTION_ROOT)
    inspection_path = _one(SELECTION_INSPECTION_ROOT)
    for path in (selection_path, inspection_path):
        strategy_discovery.require_committed(path)
    selection = _load_hashed(
        selection_path,
        identity_field="selection_sha256",
        expected_kind=(
            "oversold_replication_completed_reserve_selection"
        ),
    )
    inspection = _load_hashed(
        inspection_path,
        identity_field="inspection_sha256",
        expected_kind=(
            "oversold_replication_completed_reserve_selection_inspection"
        ),
    )
    if not (
        inspection.get("state")
        == "COMPLETED_RESERVE_SELECTION_INSPECTED_READY"
        and inspection.get("valid") is True
        and inspection.get("selection_sha256")
        == selection["selection_sha256"]
    ):
        raise OversoldReplicationReserveError(
            "completed reserve selection chain is invalid"
        )
    _assert_dates_globally_untouched(
        selection["confirmation_dates"],
        outcome_exposure.read_index(),
    )
    return selection_path, selection, inspection_path, inspection


def freeze_reference_contract() -> tuple[Path, dict[str, Any]]:
    selection_path, selection, inspection_path, inspection = (
        _selection_chain()
    )
    for path in (
        Path(__file__).resolve(),
        Path(scanner_replay.__file__).resolve(),
    ):
        strategy_discovery.require_committed(path)
    content = {
        "schema_version": SCHEMA_VERSION,
        "artifact_kind": (
            "oversold_replication_completed_reserve_reference_contract"
        ),
        "campaign_id": CAMPAIGN_ID,
        "successor_id": SUCCESSOR_ID,
        "dataset_id": DATASET_ID,
        "state": "REFERENCE_CONTRACT_FROZEN_AWAITING_INSPECTION",
        "selection_path": _repo_path(selection_path),
        "selection_file_sha256": sha256_file(selection_path),
        "selection_sha256": selection["selection_sha256"],
        "selection_inspection_path": _repo_path(inspection_path),
        "selection_inspection_file_sha256": sha256_file(
            inspection_path
        ),
        "selection_inspection_sha256": inspection[
            "inspection_sha256"
        ],
        "dates": list(selection["confirmation_dates"]),
        "provider": "Massive dated ticker reference",
        "endpoint": scanner_replay.MASSIVE_REFERENCE_URL,
        "query": {
            "market": "stocks",
            "locale": "us",
            "type": "CS",
            "active": "true",
            "sort": "ticker",
            "order": "asc",
            "limit": 1000,
        },
        "private_output_root": _repo_path(
            PRIVATE_REFERENCE_ROOT
        ),
        "implementation_hashes": {
            _repo_path(Path(__file__).resolve()): sha256_file(
                Path(__file__).resolve()
            ),
            _repo_path(Path(scanner_replay.__file__).resolve()): (
                sha256_file(Path(scanner_replay.__file__).resolve())
            ),
        },
        "substitutions_allowed": False,
        "provider_requests": 0,
        "market_prices_accessed": False,
        "target_outcomes_observed_or_derived": False,
        "broker_actions": 0,
    }
    return _publish(
        REFERENCE_CONTRACT_ROOT,
        "oversold-replication-completed-reserve-reference-contract",
        content,
        "contract_sha256",
    )


def inspect_reference_contract(
    contract_path: Path,
) -> tuple[Path, dict[str, Any]]:
    strategy_discovery.require_committed(contract_path)
    contract = _load_hashed(
        contract_path,
        identity_field="contract_sha256",
        expected_kind=(
            "oversold_replication_completed_reserve_reference_contract"
        ),
    )
    _selection_path, selection, _inspection_path, inspection = (
        _selection_chain()
    )
    checks = {
        "selection_bound": contract["selection_sha256"]
        == selection["selection_sha256"],
        "selection_inspection_bound": contract[
            "selection_inspection_sha256"
        ]
        == inspection["inspection_sha256"],
        "exact_dates": contract["dates"]
        == selection["confirmation_dates"],
        "reference_only": contract["endpoint"]
        == scanner_replay.MASSIVE_REFERENCE_URL,
        "substitutions_forbidden": contract[
            "substitutions_allowed"
        ]
        is False,
        "implementation_bound": contract["implementation_hashes"]
        == {
            _repo_path(Path(__file__).resolve()): sha256_file(
                Path(__file__).resolve()
            ),
            _repo_path(Path(scanner_replay.__file__).resolve()): (
                sha256_file(Path(scanner_replay.__file__).resolve())
            ),
        },
        "no_market_prices": contract["market_prices_accessed"] is False,
        "no_outcomes": contract[
            "target_outcomes_observed_or_derived"
        ]
        is False,
        "no_broker_actions": contract["broker_actions"] == 0,
    }
    if not all(checks.values()):
        raise OversoldReplicationReserveError(
            "completed reserve reference contract inspection failed"
        )
    content = {
        "schema_version": SCHEMA_VERSION,
        "artifact_kind": (
            "oversold_replication_completed_reserve_reference_contract_inspection"
        ),
        "campaign_id": CAMPAIGN_ID,
        "successor_id": SUCCESSOR_ID,
        "state": "REFERENCE_CONTRACT_INSPECTED_PROVIDER_READY",
        "contract_path": _repo_path(contract_path),
        "contract_file_sha256": sha256_file(contract_path),
        "contract_sha256": contract["contract_sha256"],
        "dates": len(contract["dates"]),
        "checks": checks,
        "provider_access_permitted": True,
        "market_prices_accessed": False,
        "target_outcomes_observed_or_derived": False,
        "broker_actions": 0,
        "valid": True,
    }
    return _publish(
        REFERENCE_CONTRACT_INSPECTION_ROOT,
        "oversold-replication-completed-reserve-reference-contract-inspection",
        content,
        "inspection_sha256",
    )


def _reference_chain() -> tuple[Path, dict[str, Any], Path, dict[str, Any]]:
    contract_path = _one(REFERENCE_CONTRACT_ROOT)
    inspection_path = _one(REFERENCE_CONTRACT_INSPECTION_ROOT)
    for path in (contract_path, inspection_path):
        strategy_discovery.require_committed(path)
    contract = _load_hashed(
        contract_path,
        identity_field="contract_sha256",
        expected_kind=(
            "oversold_replication_completed_reserve_reference_contract"
        ),
    )
    inspection = _load_hashed(
        inspection_path,
        identity_field="inspection_sha256",
        expected_kind=(
            "oversold_replication_completed_reserve_reference_contract_inspection"
        ),
    )
    if not (
        inspection.get("state")
        == "REFERENCE_CONTRACT_INSPECTED_PROVIDER_READY"
        and inspection.get("valid") is True
        and inspection.get("contract_sha256")
        == contract["contract_sha256"]
    ):
        raise OversoldReplicationReserveError(
            "completed reserve reference chain is invalid"
        )
    _selection_chain()
    return contract_path, contract, inspection_path, inspection


def collect_reference(
    env_path: Path,
) -> tuple[Path, dict[str, Any]]:
    contract_path, contract, inspection_path, inspection = (
        _reference_chain()
    )
    result = scanner_replay.collect_reference_snapshots(
        contract["dates"],
        config=scanner_replay.MassiveReferenceConfig.from_env(
            env_path
        ),
        output_root=PRIVATE_REFERENCE_ROOT,
    )
    snapshots = [
        {
            "date": row["date"],
            "rows": row["rows"],
            "sha256": row["sha256"],
            "disposition": row["disposition"],
            "successful_pages": math.ceil(row["rows"] / 1000),
        }
        for row in result["snapshots"]
    ]
    collected = [
        row for row in snapshots if row["disposition"] == "collected"
    ]
    cached = [
        row for row in snapshots if row["disposition"] == "cached"
    ]
    content = {
        "schema_version": SCHEMA_VERSION,
        "artifact_kind": (
            "oversold_replication_completed_reserve_reference_status"
        ),
        "campaign_id": CAMPAIGN_ID,
        "successor_id": SUCCESSOR_ID,
        "state": "REFERENCE_COLLECTED_AWAITING_INSPECTION",
        "contract_path": _repo_path(contract_path),
        "contract_file_sha256": sha256_file(contract_path),
        "contract_sha256": contract["contract_sha256"],
        "contract_inspection_path": _repo_path(inspection_path),
        "contract_inspection_file_sha256": sha256_file(
            inspection_path
        ),
        "contract_inspection_sha256": inspection[
            "inspection_sha256"
        ],
        "dates": len(snapshots),
        "snapshots": snapshots,
        "provider_telemetry": {
            "date_requests": len(collected),
            "successful_page_requests": sum(
                row["successful_pages"] for row in collected
            ),
            "cache_hits": len(cached),
            "failures": 0,
            "minimum_pacing_wait_seconds": max(
                0,
                sum(row["successful_pages"] for row in collected) - 1,
            )
            * scanner_replay.MassiveReferenceConfig.from_env(
                env_path
            ).minimum_interval_seconds,
        },
        "substitutions": 0,
        "market_prices_accessed": False,
        "target_outcomes_observed_or_derived": False,
        "broker_actions": 0,
    }
    return _publish(
        REFERENCE_STATUS_ROOT,
        "oversold-replication-completed-reserve-reference-status",
        content,
        "status_sha256",
    )


def inspect_reference_data(
    status_path: Path,
) -> tuple[Path, dict[str, Any]]:
    strategy_discovery.require_committed(status_path)
    _contract_path, contract, _inspection_path, _inspection = (
        _reference_chain()
    )
    status = _load_hashed(
        status_path,
        identity_field="status_sha256",
        expected_kind=(
            "oversold_replication_completed_reserve_reference_status"
        ),
    )
    rebuilt: list[dict[str, Any]] = []
    for day in contract["dates"]:
        path = PRIVATE_REFERENCE_ROOT / f"{day}.json.gz"
        if not path.is_file():
            raise OversoldReplicationReserveError(
                f"reference snapshot is missing: {day}"
            )
        with gzip.open(path, "rt", encoding="utf-8") as stream:
            rows = json.load(stream)
        if not isinstance(rows, list) or not rows:
            raise OversoldReplicationReserveError(
                f"reference snapshot is empty: {day}"
            )
        tickers = [str(row.get("ticker") or "") for row in rows]
        if (
            any(not ticker for ticker in tickers)
            or len(tickers) != len(set(tickers))
        ):
            raise OversoldReplicationReserveError(
                f"reference identities are ambiguous: {day}"
            )
        rebuilt.append(
            {
                "date": day,
                "rows": len(rows),
                "sha256": sha256_file(path),
                "successful_pages": math.ceil(len(rows) / 1000),
            }
        )
    observed = [
        {
            key: row[key]
            for key in ("date", "rows", "sha256", "successful_pages")
        }
        for row in status["snapshots"]
    ]
    checks = {
        "exact_dates": [row["date"] for row in rebuilt]
        == contract["dates"],
        "status_rebuilt": rebuilt == observed,
        "all_nonempty": all(row["rows"] > 0 for row in rebuilt),
        "zero_failures": status["provider_telemetry"]["failures"] == 0,
        "no_substitutions": status["substitutions"] == 0,
        "reference_only": status["market_prices_accessed"] is False,
        "no_outcomes": status[
            "target_outcomes_observed_or_derived"
        ]
        is False,
        "no_broker_actions": status["broker_actions"] == 0,
    }
    if not all(checks.values()):
        raise OversoldReplicationReserveError(
            "completed reserve reference data inspection failed"
        )
    content = {
        "schema_version": SCHEMA_VERSION,
        "artifact_kind": (
            "oversold_replication_completed_reserve_reference_data_inspection"
        ),
        "campaign_id": CAMPAIGN_ID,
        "successor_id": SUCCESSOR_ID,
        "state": "REFERENCE_DATA_INSPECTED_READY",
        "contract_sha256": contract["contract_sha256"],
        "status_path": _repo_path(status_path),
        "status_file_sha256": sha256_file(status_path),
        "status_sha256": status["status_sha256"],
        "dates": len(rebuilt),
        "minimum_rows": min(row["rows"] for row in rebuilt),
        "maximum_rows": max(row["rows"] for row in rebuilt),
        "snapshot_graph_sha256": _hash(rebuilt),
        "checks": checks,
        "provider_telemetry": status["provider_telemetry"],
        "market_prices_accessed": False,
        "target_outcomes_observed_or_derived": False,
        "broker_actions": 0,
        "valid": True,
    }
    return _publish(
        REFERENCE_DATA_INSPECTION_ROOT,
        "oversold-replication-completed-reserve-reference-data-inspection",
        content,
        "inspection_sha256",
    )


def build_master() -> dict[str, Any]:
    _selection_path, selection, _selection_inspection_path, _ = (
        _selection_chain()
    )
    status_path = _one(REFERENCE_STATUS_ROOT)
    inspection_path = _one(REFERENCE_DATA_INSPECTION_ROOT)
    for path in (status_path, inspection_path):
        strategy_discovery.require_committed(path)
    status = _load_hashed(
        status_path,
        identity_field="status_sha256",
        expected_kind=(
            "oversold_replication_completed_reserve_reference_status"
        ),
    )
    inspection = _load_hashed(
        inspection_path,
        identity_field="inspection_sha256",
        expected_kind=(
            "oversold_replication_completed_reserve_reference_data_inspection"
        ),
    )
    if not (
        inspection.get("state") == "REFERENCE_DATA_INSPECTED_READY"
        and inspection.get("valid") is True
        and inspection.get("status_sha256") == status["status_sha256"]
    ):
        raise OversoldReplicationReserveError(
            "security master lacks inspected reference data"
        )
    manifest = scanner_replay.build_security_master(
        selection["confirmation_dates"],
        snapshots_root=PRIVATE_REFERENCE_ROOT,
        output=SECURITY_MASTER,
        source_manifest=SECURITY_SOURCE,
    )
    return {
        "state": "SECURITY_MASTER_BUILT_AWAITING_INSPECTION",
        "security_master": manifest["security_master"],
        "source_path": _repo_path(SECURITY_SOURCE),
        "source_sha256": sha256_file(SECURITY_SOURCE),
        "provider_requests": 0,
        "market_prices_accessed": False,
        "target_outcomes_observed_or_derived": False,
        "broker_actions": 0,
    }


def inspect_master() -> tuple[Path, dict[str, Any]]:
    for path in (SECURITY_MASTER, SECURITY_SOURCE):
        strategy_discovery.require_committed(path)
    _selection_path, selection, _inspection_path, _inspection = (
        _selection_chain()
    )
    source = _read(SECURITY_SOURCE)
    records = load_security_master(SECURITY_MASTER)
    coverage: list[dict[str, Any]] = []
    from datetime import date

    for day in selection["confirmation_dates"]:
        observed = date.fromisoformat(day)
        symbols = {
            str(record["symbol"])
            for record in records
            if record.get("security_type") == "COMMON"
            and security_record_covers(record, observed)
        }
        if not symbols:
            raise OversoldReplicationReserveError(
                f"security master lacks exact-date common stocks: {day}"
            )
        coverage.append({"date": day, "symbols": len(symbols)})
    checks = {
        "source_dates_bound": source["requested_dates"]
        == selection["confirmation_dates"],
        "semantic_hash": source["security_master"]["sha256"]
        == security_master_sha256(SECURITY_MASTER),
        "exact_date_coverage": len(coverage)
        == CONFIRMATION_SESSIONS,
        "all_dates_nonempty": all(row["symbols"] > 0 for row in coverage),
        "reference_only": source["source"]["endpoint"]
        == scanner_replay.MASSIVE_REFERENCE_URL,
    }
    if not all(checks.values()):
        raise OversoldReplicationReserveError(
            "security master inspection failed"
        )
    content = {
        "schema_version": SCHEMA_VERSION,
        "artifact_kind": (
            "oversold_replication_completed_reserve_security_master_inspection"
        ),
        "campaign_id": CAMPAIGN_ID,
        "successor_id": SUCCESSOR_ID,
        "state": "SECURITY_MASTER_INSPECTED_READY",
        "security_master_path": _repo_path(SECURITY_MASTER),
        "security_master_file_sha256": sha256_file(SECURITY_MASTER),
        "security_master_sha256": security_master_sha256(
            SECURITY_MASTER
        ),
        "source_path": _repo_path(SECURITY_SOURCE),
        "source_file_sha256": sha256_file(SECURITY_SOURCE),
        "dates": len(coverage),
        "minimum_common_stocks": min(
            row["symbols"] for row in coverage
        ),
        "maximum_common_stocks": max(
            row["symbols"] for row in coverage
        ),
        "coverage_sha256": _hash(coverage),
        "checks": checks,
        "provider_requests": 0,
        "market_prices_accessed": False,
        "target_outcomes_observed_or_derived": False,
        "broker_actions": 0,
        "valid": True,
    }
    return _publish(
        SECURITY_INSPECTION_ROOT,
        "oversold-replication-completed-reserve-security-master-inspection",
        content,
        "inspection_sha256",
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env", type=Path, default=PROJECT_ROOT / ".env")
    sub = parser.add_subparsers(dest="command", required=True)
    freeze = sub.add_parser("freeze-selection")
    freeze.add_argument("--created-at", required=True)
    inspect = sub.add_parser("inspect-selection")
    inspect.add_argument("selection", type=Path)
    sub.add_parser("freeze-reference")
    reference_inspection = sub.add_parser(
        "inspect-reference-contract"
    )
    reference_inspection.add_argument("contract", type=Path)
    sub.add_parser("collect-reference")
    data_inspection = sub.add_parser("inspect-reference-data")
    data_inspection.add_argument("status", type=Path)
    sub.add_parser("build-master")
    sub.add_parser("inspect-master")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "freeze-selection":
            path, result = freeze_selection(
                created_at=args.created_at
            )
        elif args.command == "inspect-selection":
            path, result = inspect_selection(args.selection)
        elif args.command == "freeze-reference":
            path, result = freeze_reference_contract()
        elif args.command == "inspect-reference-contract":
            path, result = inspect_reference_contract(args.contract)
        elif args.command == "collect-reference":
            path, result = collect_reference(args.env)
        elif args.command == "inspect-reference-data":
            path, result = inspect_reference_data(args.status)
        elif args.command == "build-master":
            result = build_master()
            path = SECURITY_SOURCE
        else:
            path, result = inspect_master()
        print(
            json.dumps(
                {
                    "path": _repo_path(path),
                    **result,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    except (
        KeyError,
        OSError,
        OversoldReplicationReserveError,
        outcome_exposure.OutcomeExposureError,
        scanner_replay.ScannerReplayError,
        strategy_discovery.StrategyDiscoveryError,
        ValueError,
    ) as exc:
        print(
            json.dumps(
                {
                    "state": "BLOCKED",
                    "error": str(exc),
                    "market_prices_accessed": False,
                    "target_outcomes_observed_or_derived": False,
                    "broker_actions": 0,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())

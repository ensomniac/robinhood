"""Collect and inspect exact winner-bound oversold confirmation minutes.

The frozen confirmation inventory contains only information observable at
09:35 ET.  This controller may open or collect full-session prices only after
one exact development winner is committed.  It retains every frozen candidate,
never substitutes dates or symbols, computes no returns, and performs no broker
actions.
"""

from __future__ import annotations

import argparse
import json
import math
import os
from collections import Counter
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime, time
from pathlib import Path
from typing import Any

import outcome_exposure
import oversold_replication_confirmation as inventory_controller
import oversold_replication_development as development
import oversold_replication_development_collection as development_collection
import strategy_discovery
from historical_store import (
    HistoricalDayStore,
    build_dataset,
    canonical_sha256,
    compact_bar,
    sha256_file,
)
from scanner_replay_alpaca import (
    ALPACA_BARS_URL,
    AlpacaBulkBarsClient,
    AlpacaBulkConfig,
    _parse_provider_bar,
)


PROJECT_ROOT = Path(__file__).resolve().parent
EASTERN = development_collection.EASTERN
SCHEMA_VERSION = 1
CAMPAIGN_ID = development.CAMPAIGN_ID
FAMILY_ID = inventory_controller.FAMILY_ID
SUCCESSOR_ID = development.SUCCESSOR_ID
DATASET_ID = (
    "dataset-short-horizon-oversold-reversal-v4-confirmation-"
    "minutes-2026-07-24-v1"
)
OUTPUT_ROOT = (
    PROJECT_ROOT
    / "strategy_tournament/v2/oversold_replication"
    / DATASET_ID
)
CONTRACT_ROOT = OUTPUT_ROOT / "collection-contract"
CONTRACT_INSPECTION_ROOT = OUTPUT_ROOT / "collection-contract-inspection"
STATUS_ROOT = OUTPUT_ROOT / "collection-status"
DATA_INSPECTION_ROOT = OUTPUT_ROOT / "data-inspection"


class OversoldReplicationConfirmationCollectionError(RuntimeError):
    """The exact confirmation collection authority or data has drifted."""


def _read(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise OversoldReplicationConfirmationCollectionError(
            f"cannot read {path}: {exc}"
        ) from exc
    if not isinstance(value, dict):
        raise OversoldReplicationConfirmationCollectionError(
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
        raise OversoldReplicationConfirmationCollectionError(
            f"path escaped repository: {path}"
        ) from exc


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
        raise OversoldReplicationConfirmationCollectionError(
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
        raise OversoldReplicationConfirmationCollectionError(
            f"invalid {expected_kind}: {path}"
        )
    return value


def _timestamp(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise OversoldReplicationConfirmationCollectionError(
            "timestamp is invalid"
        ) from exc
    if parsed.tzinfo is None:
        raise OversoldReplicationConfirmationCollectionError(
            "timestamp needs a timezone"
        )
    return parsed


def _private_root(store: HistoricalDayStore) -> Path:
    return (
        store.root
        / "_derived/oversold_replication_confirmation_collection"
        / DATASET_ID
    )


def _state_path(store: HistoricalDayStore) -> Path:
    return _private_root(store) / "collection-state.json"


def _input_index_path(store: HistoricalDayStore) -> Path:
    return _private_root(store) / "confirmation-input-index.json.gz"


def _load_inventory(
    store: HistoricalDayStore,
    *,
    require_committed: bool,
) -> tuple[
    Path,
    dict[str, Any],
    Path,
    dict[str, Any],
    dict[str, Any],
]:
    contract_path = inventory_controller._one(
        inventory_controller.CONTRACT_ROOT
    )
    inspection_path = inventory_controller._one(
        inventory_controller.INSPECTION_ROOT
    )
    if require_committed:
        strategy_discovery.require_committed(contract_path)
        strategy_discovery.require_committed(inspection_path)
    contract = inventory_controller._load_contract(contract_path)
    inspection = inventory_controller._load_hashed(
        inspection_path,
        identity_field="inspection_sha256",
        expected_kind=(
            "oversold_replication_confirmation_inventory_inspection"
        ),
    )
    inventory = development._read_gzip(
        inventory_controller._inventory_path(store)
    )
    if not (
        inspection.get("state")
        == "CONFIRMATION_INVENTORY_INSPECTED_READY"
        and inspection.get("valid") is True
        and inspection.get("contract_sha256")
        == contract["contract_sha256"]
        and inspection.get("private_inventory_content_sha256")
        == inventory.get("content_sha256")
        and contract["inventory"]["private_content_sha256"]
        == inventory.get("content_sha256")
        and inventory.get("lane") == "confirmation"
        and inventory.get("target_outcomes_observed_or_derived")
        is False
    ):
        raise OversoldReplicationConfirmationCollectionError(
            "inspected confirmation inventory binding is invalid"
        )
    return (
        contract_path,
        contract,
        inspection_path,
        inspection,
        inventory,
    )


def _load_winner(
    winner_path: Path,
    *,
    enforce_commit: bool,
) -> dict[str, Any]:
    if enforce_commit:
        strategy_discovery.require_committed(winner_path)
    winner = strategy_discovery.load_artifact(
        winner_path,
        expected_kind="frozen-strategy-winner",
    )
    if not (
        winner.get("campaign_id") == CAMPAIGN_ID
        and winner.get("family_id") == FAMILY_ID
        and winner.get("state") == "WINNER_FROZEN"
        and winner.get("confirmation_access_permitted") is True
        and winner.get("broker_actions_permitted") is False
    ):
        raise OversoldReplicationConfirmationCollectionError(
            "winner does not authorize exact confirmation"
        )
    return winner


def _scope(inventory: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "dates": list(inventory["signal_dates"]),
        "symbols_by_date": {
            day: [
                str(row["symbol"])
                for row in inventory["candidates_by_date"][day]
            ]
            for day in inventory["signal_dates"]
        },
    }


def _contract_content(
    *,
    winner_path: Path,
    created_at: str,
    store: HistoricalDayStore,
) -> dict[str, Any]:
    winner = _load_winner(winner_path, enforce_commit=True)
    created = _timestamp(created_at)
    winner_recorded = _timestamp(str(winner["recorded_at"]))
    if created <= winner_recorded:
        raise OversoldReplicationConfirmationCollectionError(
            "confirmation contract must follow winner preregistration"
        )
    (
        inventory_contract_path,
        inventory_contract,
        inventory_inspection_path,
        inventory_inspection,
        inventory,
    ) = _load_inventory(store, require_committed=True)
    scope = _scope(inventory)
    if not (
        winner["confirmation_dates"] == inventory["evaluation_dates"]
        and winner["confirmation_signal_dates"] == inventory["signal_dates"]
        and winner["confirmation_signal_capacity"]
        == len(inventory["signal_dates"])
        and winner["confirmation_scope"] == inventory["outcome_scope"]
        and scope == inventory["outcome_scope"]
    ):
        raise OversoldReplicationConfirmationCollectionError(
            "winner and confirmation inventory scope differ"
        )
    outcome_exposure.assert_untouched(
        scope,
        outcome_exposure.read_index(),
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "artifact_kind": (
            "oversold_replication_confirmation_collection_contract"
        ),
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "successor_id": SUCCESSOR_ID,
        "dataset_id": DATASET_ID,
        "created_at": created_at,
        "state": "CONFIRMATION_COLLECTION_FROZEN_AWAITING_INSPECTION",
        "winner_binding": {
            "path": _repo_path(winner_path),
            "file_sha256": sha256_file(winner_path),
            "artifact_sha256": winner["artifact_sha256"],
            "strategy_id": winner["strategy_id"],
            "strategy_version": winner["strategy_version"],
            "rules_hash": winner["rules_hash"],
            "recorded_at": winner["recorded_at"],
        },
        "inventory_binding": {
            "contract_path": _repo_path(inventory_contract_path),
            "contract_file_sha256": sha256_file(
                inventory_contract_path
            ),
            "contract_sha256": inventory_contract["contract_sha256"],
            "inspection_path": _repo_path(
                inventory_inspection_path
            ),
            "inspection_file_sha256": sha256_file(
                inventory_inspection_path
            ),
            "inspection_sha256": inventory_inspection[
                "inspection_sha256"
            ],
            "private_inventory_content_sha256": inventory[
                "content_sha256"
            ],
            "private_inventory_file_sha256": sha256_file(
                inventory_controller._inventory_path(store)
            ),
        },
        "evaluation_dates": list(inventory["evaluation_dates"]),
        "zero_signal_dates": list(inventory["zero_signal_dates"]),
        "scope": scope,
        "date_count": len(inventory["evaluation_dates"]),
        "signal_date_count": len(scope["dates"]),
        "candidate_symbol_sessions": sum(
            len(symbols)
            for symbols in scope["symbols_by_date"].values()
        ),
        "collection": {
            "provider": "Alpaca",
            "endpoint": ALPACA_BARS_URL,
            "feed": "sip",
            "adjustment": "raw",
            "timeframe": "1Min",
            "session": "09:30:00-15:59:59.999999_ET",
            "batching": "multi_symbol_by_frozen_date",
            "substitutions_allowed": False,
            "interpolation_allowed": False,
            "missing_rows": "retain candidate denominator as no-signal",
            "resume": "canonical store, no duplicate request",
        },
        "outcome_boundary": {
            "lane": "confirmation",
            "winner_must_be_committed_before_access": True,
            "exact_rules_only": True,
            "parameter_alternatives": 0,
            "return_metrics_permitted_during_collection": False,
            "broker_actions_permitted": False,
        },
        "implementation_binding": {
            "controller_path": _repo_path(Path(__file__).resolve()),
            "controller_sha256": sha256_file(
                Path(__file__).resolve()
            ),
            "provider_adapter_path": "scanner_replay_alpaca.py",
            "provider_adapter_sha256": sha256_file(
                PROJECT_ROOT / "scanner_replay_alpaca.py"
            ),
            "historical_store_path": "historical_store.py",
            "historical_store_sha256": sha256_file(
                PROJECT_ROOT / "historical_store.py"
            ),
        },
        "provider_requests": 0,
        "return_metrics_computed": 0,
        "parameter_alternatives_evaluated": 0,
        "broker_actions": 0,
    }


def freeze_contract(
    winner_path: Path,
    *,
    created_at: str,
    store: HistoricalDayStore | None = None,
) -> tuple[Path, dict[str, Any]]:
    for path in (
        Path(__file__).resolve(),
        PROJECT_ROOT / "scanner_replay_alpaca.py",
        PROJECT_ROOT / "historical_store.py",
    ):
        strategy_discovery.require_committed(path)
    source = store or HistoricalDayStore.from_env()
    return _publish(
        CONTRACT_ROOT,
        "oversold-replication-confirmation-collection-contract",
        _contract_content(
            winner_path=winner_path,
            created_at=created_at,
            store=source,
        ),
        "contract_sha256",
    )


def _load_contract(path: Path) -> dict[str, Any]:
    return _load_hashed(
        path,
        identity_field="contract_sha256",
        expected_kind=(
            "oversold_replication_confirmation_collection_contract"
        ),
    )


def inspect_contract(
    contract_path: Path,
    *,
    store: HistoricalDayStore | None = None,
) -> tuple[Path, dict[str, Any]]:
    strategy_discovery.require_committed(contract_path)
    contract = _load_contract(contract_path)
    winner_path = PROJECT_ROOT / contract["winner_binding"]["path"]
    source = store or HistoricalDayStore.from_env()
    expected = _contract_content(
        winner_path=winner_path,
        created_at=str(contract["created_at"]),
        store=source,
    )
    checks = {
        "exact_rebuild": {
            key: value
            for key, value in contract.items()
            if key != "contract_sha256"
        }
        == expected,
        "winner_preregistered": _timestamp(
            contract["created_at"]
        )
        > _timestamp(contract["winner_binding"]["recorded_at"]),
        "exact_rules_bound": bool(
            contract["winner_binding"]["rules_hash"]
        ),
        "substitutions_forbidden": contract["collection"][
            "substitutions_allowed"
        ]
        is False,
        "winner_precedes_access": contract["outcome_boundary"][
            "winner_must_be_committed_before_access"
        ]
        is True,
        "zero_alternatives": contract["outcome_boundary"][
            "parameter_alternatives"
        ]
        == 0,
        "no_provider_access": contract["provider_requests"] == 0,
        "no_returns": contract["return_metrics_computed"] == 0,
        "no_broker_actions": contract["broker_actions"] == 0,
    }
    if not all(checks.values()):
        raise OversoldReplicationConfirmationCollectionError(
            "confirmation collection contract inspection failed"
        )
    content = {
        "schema_version": SCHEMA_VERSION,
        "artifact_kind": (
            "oversold_replication_confirmation_collection_contract_inspection"
        ),
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "successor_id": SUCCESSOR_ID,
        "dataset_id": DATASET_ID,
        "state": "CONFIRMATION_COLLECTION_CONTRACT_INSPECTED_READY",
        "contract_path": _repo_path(contract_path),
        "contract_file_sha256": sha256_file(contract_path),
        "contract_sha256": contract["contract_sha256"],
        "winner_sha256": contract["winner_binding"][
            "artifact_sha256"
        ],
        "rules_hash": contract["winner_binding"]["rules_hash"],
        "checks": checks,
        "evaluation_dates": contract["date_count"],
        "signal_dates": contract["signal_date_count"],
        "candidate_symbol_sessions": contract[
            "candidate_symbol_sessions"
        ],
        "provider_access_permitted": True,
        "return_metrics_permitted": False,
        "broker_actions_permitted": False,
        "valid": True,
    }
    return _publish(
        CONTRACT_INSPECTION_ROOT,
        "oversold-replication-confirmation-collection-contract-inspection",
        content,
        "inspection_sha256",
    )


def _load_contract_chain(
    contract_path: Path,
    inspection_path: Path,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    strategy_discovery.require_committed(contract_path)
    strategy_discovery.require_committed(inspection_path)
    contract = _load_contract(contract_path)
    inspection = _load_hashed(
        inspection_path,
        identity_field="inspection_sha256",
        expected_kind=(
            "oversold_replication_confirmation_collection_contract_inspection"
        ),
    )
    winner = _load_winner(
        PROJECT_ROOT / contract["winner_binding"]["path"],
        enforce_commit=True,
    )
    if not (
        inspection.get("state")
        == "CONFIRMATION_COLLECTION_CONTRACT_INSPECTED_READY"
        and inspection.get("valid") is True
        and inspection.get("contract_sha256")
        == contract["contract_sha256"]
        and inspection.get("winner_sha256")
        == winner["artifact_sha256"]
        and inspection.get("rules_hash") == winner["rules_hash"]
        and contract["winner_binding"]["file_sha256"]
        == sha256_file(
            PROJECT_ROOT / contract["winner_binding"]["path"]
        )
    ):
        raise OversoldReplicationConfirmationCollectionError(
            "confirmation collection contract chain is not ready"
        )
    for path_field, hash_field in (
        ("controller_path", "controller_sha256"),
        ("provider_adapter_path", "provider_adapter_sha256"),
        ("historical_store_path", "historical_store_sha256"),
    ):
        path = PROJECT_ROOT / contract["implementation_binding"][
            path_field
        ]
        if sha256_file(path) != contract["implementation_binding"][
            hash_field
        ]:
            raise OversoldReplicationConfirmationCollectionError(
                f"confirmation implementation drifted: {path_field}"
            )
    outcome_exposure.assert_untouched(
        contract["scope"],
        outcome_exposure.read_index(),
    )
    return contract, inspection, winner


def _full_dataset(
    store: HistoricalDayStore,
    symbol: str,
    day: str,
) -> dict[str, Any] | None:
    return development_collection._full_dataset(store, symbol, day)


def collect(
    contract_path: Path,
    inspection_path: Path,
    *,
    env_path: Path = PROJECT_ROOT / ".env",
    max_dates: int | None = None,
    store: HistoricalDayStore | None = None,
) -> tuple[Path, dict[str, Any]]:
    source = store or HistoricalDayStore.from_env(env_path)
    contract, inspection, winner = _load_contract_chain(
        contract_path,
        inspection_path,
    )
    config = AlpacaBulkConfig.from_env(env_path)
    requested = cached = received = rows_received = 0
    unresolved: list[dict[str, str]] = []
    started = datetime.now(UTC)
    winner_recorded = _timestamp(str(winner["recorded_at"]))
    if started <= winner_recorded:
        raise OversoldReplicationConfirmationCollectionError(
            "confirmation source access must follow winner preregistration"
        )
    processed_dates = 0
    with AlpacaBulkBarsClient(config) as client:
        for day in contract["scope"]["dates"]:
            symbols = list(contract["scope"]["symbols_by_date"][day])
            missing = [
                symbol
                for symbol in symbols
                if _full_dataset(source, symbol, day) is None
            ]
            if not missing:
                requested += len(symbols)
                cached += len(symbols)
                continue
            if max_dates is not None and processed_dates >= max_dates:
                continue
            processed_dates += 1
            requested += len(symbols)
            cached += len(symbols) - len(missing)
            session_day = date.fromisoformat(day)
            session_start = datetime.combine(
                session_day,
                time(9, 30),
                tzinfo=EASTERN,
            )
            session_end = datetime.combine(
                session_day,
                time(15, 59, 59, 999999),
                tzinfo=EASTERN,
            )
            for offset in range(0, len(missing), config.batch_size):
                batch = missing[offset : offset + config.batch_size]
                rows_by_symbol, _ = client.fetch(
                    batch,
                    timeframe="1Min",
                    start=session_start,
                    end=session_end,
                )
                captured_at = datetime.now(UTC).isoformat()
                for symbol in batch:
                    raw_rows = rows_by_symbol.get(symbol, [])
                    if not raw_rows:
                        unresolved.append(
                            {
                                "date": day,
                                "symbol": symbol,
                                "reason": "provider_returned_no_rows",
                            }
                        )
                        continue
                    normalized = sorted(
                        (
                            _parse_provider_bar(
                                row,
                                day=day,
                                window="regular",
                            )
                            for row in raw_rows
                        ),
                        key=lambda row: str(row["time_et"]),
                    )
                    timestamps = [
                        str(row["time_et"]) for row in normalized
                    ]
                    if (
                        len(timestamps) != len(set(timestamps))
                        or len(normalized) > 390
                    ):
                        raise OversoldReplicationConfirmationCollectionError(
                            f"{day} {symbol}: provider minutes are ambiguous"
                        )
                    dataset = build_dataset(
                        kind="bars",
                        provider="alpaca",
                        rows=[
                            compact_bar(row, day=day)
                            for row in normalized
                        ],
                        channel="trades",
                        timeframe="1m",
                        feed="sip",
                        adjustment="raw",
                        session="regular",
                        scope="full_session",
                        quality={
                            "complete": True,
                            "requested_window_complete": True,
                            "sparse_intervals_allowed": True,
                        },
                        provenance={
                            "source_type": (
                                "alpaca_multi_symbol_oversold_replication_confirmation"
                            ),
                            "endpoint": ALPACA_BARS_URL,
                            "dataset_id": DATASET_ID,
                            "contract_sha256": contract[
                                "contract_sha256"
                            ],
                            "winner_sha256": winner["artifact_sha256"],
                            "rules_hash": winner["rules_hash"],
                            "lane": "confirmation",
                            "session_date": day,
                            "feed": "sip",
                            "adjustment": "raw",
                            "asof": "-",
                            "captured_at": captured_at,
                        },
                    )
                    source.merge(symbol, day, datasets=[dataset])
                    received += 1
                    rows_received += len(normalized)
        requests = client.request_count
        retries = client.retry_count
        request_seconds = client.request_seconds
        pacing_wait_seconds = client.pacing_wait_seconds
    remaining: list[dict[str, str]] = []
    for day in contract["scope"]["dates"]:
        for symbol in contract["scope"]["symbols_by_date"][day]:
            if _full_dataset(source, symbol, day) is None:
                matching = next(
                    (
                        row
                        for row in unresolved
                        if row["date"] == day
                        and row["symbol"] == symbol
                    ),
                    None,
                )
                remaining.append(
                    matching
                    or {
                        "date": day,
                        "symbol": symbol,
                        "reason": (
                            "not_processed_in_bounded_resume"
                            if max_dates is not None
                            else "canonical_dataset_missing"
                        ),
                    }
                )
    completed = datetime.now(UTC)
    state = {
        "schema_version": SCHEMA_VERSION,
        "dataset_id": DATASET_ID,
        "contract_sha256": contract["contract_sha256"],
        "inspection_sha256": inspection["inspection_sha256"],
        "winner_sha256": winner["artifact_sha256"],
        "rules_hash": winner["rules_hash"],
        "requested_symbol_sessions": requested,
        "cached_before_collection": cached,
        "provider_symbol_sessions_received": received,
        "provider_rows_received": rows_received,
        "provider_requests": requests,
        "provider_retries": retries,
        "provider_request_seconds": request_seconds,
        "pacing_wait_seconds": pacing_wait_seconds,
        "unresolved": remaining,
        "started_at": started.isoformat(),
        "completed_at": completed.isoformat(),
    }
    _write(_state_path(source), state)
    content = {
        "schema_version": SCHEMA_VERSION,
        "artifact_kind": (
            "oversold_replication_confirmation_collection_status"
        ),
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "successor_id": SUCCESSOR_ID,
        "dataset_id": DATASET_ID,
        "state": (
            "CONFIRMATION_DATA_COLLECTED_AWAITING_INSPECTION"
            if not remaining
            else "CONFIRMATION_DATA_PARTIAL"
        ),
        "contract_path": _repo_path(contract_path),
        "contract_sha256": contract["contract_sha256"],
        "contract_inspection_path": _repo_path(inspection_path),
        "contract_inspection_sha256": inspection[
            "inspection_sha256"
        ],
        "winner_path": contract["winner_binding"]["path"],
        "winner_sha256": winner["artifact_sha256"],
        "rules_hash": winner["rules_hash"],
        "evaluation_dates": contract["date_count"],
        "signal_dates": contract["signal_date_count"],
        "candidate_symbol_sessions": contract[
            "candidate_symbol_sessions"
        ],
        "cached_before_collection": cached,
        "provider_symbol_sessions_received": received,
        "provider_rows_received": rows_received,
        "provider_telemetry": {
            "requests": requests,
            "retries": retries,
            "request_seconds": request_seconds,
            "pacing_wait_seconds": pacing_wait_seconds,
            "cache_hits": cached,
            "failures": len(remaining),
        },
        "unresolved_symbol_sessions": len(remaining),
        "unresolved_reason_counts": dict(
            sorted(
                Counter(
                    row["reason"] for row in remaining
                ).items()
            )
        ),
        "collection_started_at": started.isoformat(),
        "collection_completed_at": completed.isoformat(),
        "capture_after_preregistration_attested": (
            started > winner_recorded
        ),
        "return_metrics_computed": 0,
        "parameter_alternatives_evaluated": 0,
        "broker_actions": 0,
    }
    return _publish(
        STATUS_ROOT,
        "oversold-replication-confirmation-collection-status",
        content,
        "status_sha256",
    )


def inspect_data(
    contract_path: Path,
    contract_inspection_path: Path,
    status_path: Path,
    *,
    store: HistoricalDayStore | None = None,
) -> tuple[Path, dict[str, Any]]:
    source = store or HistoricalDayStore.from_env()
    contract, contract_inspection, winner = _load_contract_chain(
        contract_path,
        contract_inspection_path,
    )
    strategy_discovery.require_committed(status_path)
    status = _load_hashed(
        status_path,
        identity_field="status_sha256",
        expected_kind=(
            "oversold_replication_confirmation_collection_status"
        ),
    )
    if not (
        status.get("state")
        == "CONFIRMATION_DATA_COLLECTED_AWAITING_INSPECTION"
        and status.get("contract_sha256")
        == contract["contract_sha256"]
        and status.get("winner_sha256")
        == winner["artifact_sha256"]
        and status.get("rules_hash") == winner["rules_hash"]
        and status.get("unresolved_symbol_sessions") == 0
        and status.get("capture_after_preregistration_attested") is True
        and status.get("return_metrics_computed") == 0
        and status.get("parameter_alternatives_evaluated") == 0
    ):
        raise OversoldReplicationConfirmationCollectionError(
            "confirmation collection is not complete and committed"
        )
    (
        _inventory_contract_path,
        _inventory_contract,
        _inventory_inspection_path,
        _inventory_inspection,
        inventory,
    ) = _load_inventory(source, require_committed=True)
    candidate_metadata = {
        day: {
            str(row["symbol"]): row
            for row in inventory["candidates_by_date"][day]
        }
        for day in inventory["signal_dates"]
    }
    index_rows: list[dict[str, Any]] = []
    exact_sessions = sparse_sessions = minute_rows = 0
    for day in contract["scope"]["dates"]:
        for symbol in contract["scope"]["symbols_by_date"][day]:
            dataset = _full_dataset(source, symbol, day)
            if dataset is None:
                raise OversoldReplicationConfirmationCollectionError(
                    f"{day} {symbol}: inspected dataset is missing"
                )
            rows, exact = development_collection._expanded_rows(
                dataset,
                day=day,
                symbol=symbol,
            )
            if not rows:
                raise OversoldReplicationConfirmationCollectionError(
                    f"{day} {symbol}: inspected dataset is empty"
                )
            if not math.isclose(
                float(rows[0]["open"]),
                float(
                    candidate_metadata[day][symbol]["open_price"]
                ),
                rel_tol=0,
                abs_tol=1e-10,
            ):
                raise OversoldReplicationConfirmationCollectionError(
                    f"{day} {symbol}: 09:30 open drifted"
                )
            minute_rows += len(rows)
            exact_sessions += int(exact)
            sparse_sessions += int(not exact)
            index_rows.append(
                {
                    "date": day,
                    "symbol": symbol,
                    "dataset_id": dataset["id"],
                    "dataset_sha256": dataset["content_sha256"],
                    "rows": len(rows),
                    "exact_390_contiguous": exact,
                }
            )
    private_index = {
        "schema_version": SCHEMA_VERSION,
        "dataset_id": DATASET_ID,
        "contract_sha256": contract["contract_sha256"],
        "winner_sha256": winner["artifact_sha256"],
        "rules_hash": winner["rules_hash"],
        "lane": "confirmation",
        "evaluation_dates": list(contract["evaluation_dates"]),
        "zero_signal_dates": list(contract["zero_signal_dates"]),
        "input_rows": index_rows,
    }
    development._write_gzip(
        _input_index_path(source),
        private_index,
    )
    content = {
        "schema_version": SCHEMA_VERSION,
        "artifact_kind": (
            "oversold_replication_confirmation_data_inspection"
        ),
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "successor_id": SUCCESSOR_ID,
        "dataset_id": DATASET_ID,
        "state": "CONFIRMATION_DATA_INSPECTED_READY",
        "contract_path": _repo_path(contract_path),
        "contract_sha256": contract["contract_sha256"],
        "contract_inspection_path": _repo_path(
            contract_inspection_path
        ),
        "contract_inspection_sha256": contract_inspection[
            "inspection_sha256"
        ],
        "status_path": _repo_path(status_path),
        "status_sha256": status["status_sha256"],
        "winner_path": contract["winner_binding"]["path"],
        "winner_sha256": winner["artifact_sha256"],
        "rules_hash": winner["rules_hash"],
        "evaluation_dates": contract["date_count"],
        "signal_dates": contract["signal_date_count"],
        "zero_signal_dates": len(contract["zero_signal_dates"]),
        "candidate_symbol_sessions": len(index_rows),
        "minute_rows": minute_rows,
        "exact_390_contiguous_symbol_sessions": exact_sessions,
        "sparse_symbol_sessions_retained_as_no_signal": sparse_sessions,
        "private_input_index_content_sha256": canonical_sha256(
            private_index
        ),
        "capture_after_preregistration_attested": True,
        "return_metrics_computed": 0,
        "parameter_alternatives_evaluated": 0,
        "provider_requests": 0,
        "broker_actions": 0,
        "valid": True,
    }
    return _publish(
        DATA_INSPECTION_ROOT,
        "oversold-replication-confirmation-data-inspection",
        content,
        "inspection_sha256",
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env", type=Path, default=PROJECT_ROOT / ".env")
    sub = parser.add_subparsers(dest="command", required=True)
    freeze = sub.add_parser("freeze-contract")
    freeze.add_argument("winner", type=Path)
    freeze.add_argument("--created-at", required=True)
    inspect = sub.add_parser("inspect-contract")
    inspect.add_argument("contract", type=Path)
    collect_parser = sub.add_parser("collect")
    collect_parser.add_argument("contract", type=Path)
    collect_parser.add_argument("inspection", type=Path)
    collect_parser.add_argument("--max-dates", type=int)
    data = sub.add_parser("inspect-data")
    data.add_argument("contract", type=Path)
    data.add_argument("inspection", type=Path)
    data.add_argument("status", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "freeze-contract":
            path, value = freeze_contract(
                args.winner,
                created_at=args.created_at,
            )
        elif args.command == "inspect-contract":
            path, value = inspect_contract(args.contract)
        elif args.command == "collect":
            path, value = collect(
                args.contract,
                args.inspection,
                env_path=args.env,
                max_dates=args.max_dates,
            )
        else:
            path, value = inspect_data(
                args.contract,
                args.inspection,
                args.status,
            )
        print(
            json.dumps(
                {
                    "path": _repo_path(path),
                    "state": value["state"],
                    "candidate_symbol_sessions": value[
                        "candidate_symbol_sessions"
                    ],
                    "provider_telemetry": value.get(
                        "provider_telemetry",
                        {"requests": value.get("provider_requests", 0)},
                    ),
                    "return_metrics_computed": 0,
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
        OversoldReplicationConfirmationCollectionError,
        outcome_exposure.OutcomeExposureError,
        strategy_discovery.StrategyDiscoveryError,
        ValueError,
    ) as exc:
        print(
            json.dumps(
                {
                    "state": "BLOCKED",
                    "error": str(exc),
                    "return_metrics_computed": 0,
                    "broker_actions": 0,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())

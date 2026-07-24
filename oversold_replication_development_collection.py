"""Collect and inspect the frozen broad oversold development minutes.

The exact candidate graph must already be committed and independently
inspected.  A separate committed authorization assigns every candidate pair to
contaminated development before this controller opens cached full-session bars
or contacts Alpaca.  Confirmation data and broker actions are never permitted.
"""

from __future__ import annotations

import argparse
import gzip
import io
import json
import math
import os
from collections import Counter
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime, time
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import outcome_exposure
import oversold_replication_development as development
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
EASTERN = ZoneInfo("America/New_York")
SCHEMA_VERSION = 1
CAMPAIGN_ID = development.CAMPAIGN_ID
FAMILY_ID = development.FAMILY_ID
SUCCESSOR_ID = development.SUCCESSOR_ID
DATASET_ID = (
    "dataset-short-horizon-oversold-reversal-v4-broad-development-"
    "minutes-2026-07-24-v1"
)
EXPOSURE_ID = (
    "short-horizon-oversold-reversal-v4-broad-development-2026-07-24-v1"
)
INVENTORY_CONTRACT = (
    development.CONTRACT_ROOT
    / "oversold-replication-development-inventory-contract-"
    "459bc4b963eb812f6302ee12a326be14dabcb5b3fc03f0a382f3aec2389b7d59.json"
)
INVENTORY_INSPECTION = (
    development.INSPECTION_ROOT
    / "oversold-replication-development-inventory-inspection-"
    "49103e8025cb7ff0712aaff00ac6785d3f2e485082b870bcd56f72033b9b5700.json"
)
OUTPUT_ROOT = (
    PROJECT_ROOT
    / "strategy_tournament/v2/oversold_replication"
    / DATASET_ID
)
CONTRACT_ROOT = OUTPUT_ROOT / "collection-contract"
CONTRACT_INSPECTION_ROOT = OUTPUT_ROOT / "collection-contract-inspection"
AUTHORIZATION_ROOT = OUTPUT_ROOT / "development-exposure-authorization"
STATUS_ROOT = OUTPUT_ROOT / "collection-status"
DATA_INSPECTION_ROOT = OUTPUT_ROOT / "data-inspection"


class OversoldReplicationCollectionError(RuntimeError):
    """The development collection authority or data has drifted."""


def _read(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise OversoldReplicationCollectionError(
            f"cannot read {path}: {exc}"
        ) from exc
    if not isinstance(value, dict):
        raise OversoldReplicationCollectionError(
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
        raise OversoldReplicationCollectionError(
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
        raise OversoldReplicationCollectionError(
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
        raise OversoldReplicationCollectionError(
            f"invalid {expected_kind}: {path}"
        )
    return value


def _private_root(store: HistoricalDayStore) -> Path:
    return (
        store.root
        / "_derived/oversold_replication_collection"
        / DATASET_ID
    )


def _state_path(store: HistoricalDayStore) -> Path:
    return _private_root(store) / "collection-state.json"


def _input_index_path(store: HistoricalDayStore) -> Path:
    return _private_root(store) / "development-input-index.json.gz"


def _load_inventory(
    store: HistoricalDayStore,
    *,
    require_committed: bool,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    if require_committed:
        for path in (INVENTORY_CONTRACT, INVENTORY_INSPECTION):
            strategy_discovery.require_committed(path)
    contract = development._load_contract(INVENTORY_CONTRACT)
    inspection = _load_hashed(
        INVENTORY_INSPECTION,
        identity_field="inspection_sha256",
        expected_kind=(
            "oversold_replication_development_inventory_inspection"
        ),
    )
    inventory = development._read_gzip(
        development._inventory_path(store)
    )
    if not (
        inspection.get("state")
        == "DEVELOPMENT_INVENTORY_INSPECTED_READY"
        and inspection.get("valid") is True
        and inspection.get("contract_sha256")
        == contract["contract_sha256"]
        and inspection.get("private_inventory_content_sha256")
        == inventory.get("content_sha256")
        and contract["inventory"]["private_content_sha256"]
        == inventory.get("content_sha256")
        and contract["inventory"]["private_file_sha256"]
        == sha256_file(development._inventory_path(store))
        and inventory.get("target_outcomes_observed_or_derived") is False
    ):
        raise OversoldReplicationCollectionError(
            "inspected development inventory binding is invalid"
        )
    return contract, inspection, inventory


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
    created_at: str,
    store: HistoricalDayStore,
) -> dict[str, Any]:
    inventory_contract, inspection, inventory = _load_inventory(
        store,
        require_committed=True,
    )
    scope = _scope(inventory)
    return {
        "schema_version": SCHEMA_VERSION,
        "artifact_kind": (
            "oversold_replication_development_collection_contract"
        ),
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "successor_id": SUCCESSOR_ID,
        "dataset_id": DATASET_ID,
        "created_at": created_at,
        "state": "DEVELOPMENT_COLLECTION_FROZEN_AWAITING_INSPECTION",
        "inventory_binding": {
            "contract_path": _repo_path(INVENTORY_CONTRACT),
            "contract_file_sha256": sha256_file(INVENTORY_CONTRACT),
            "contract_sha256": inventory_contract["contract_sha256"],
            "inspection_path": _repo_path(INVENTORY_INSPECTION),
            "inspection_file_sha256": sha256_file(INVENTORY_INSPECTION),
            "inspection_sha256": inspection["inspection_sha256"],
            "private_inventory_content_sha256": inventory[
                "content_sha256"
            ],
            "private_inventory_file_sha256": sha256_file(
                development._inventory_path(store)
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
            "resume": (
                "content-addressed canonical store, no duplicate request"
            ),
        },
        "outcome_boundary": {
            "lane": "development",
            "exposure_id": EXPOSURE_ID,
            "authorization_must_be_committed_before_cache_or_provider_access": True,
            "confirmation_access_permitted": False,
            "broker_actions_permitted": False,
        },
        "implementation_binding": {
            "controller_path": _repo_path(Path(__file__).resolve()),
            "controller_sha256": sha256_file(Path(__file__).resolve()),
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
        "confirmation_accessed": False,
        "broker_actions": 0,
    }


def freeze_contract(
    *,
    created_at: str,
    store: HistoricalDayStore | None = None,
) -> tuple[Path, dict[str, Any]]:
    development._validate_timestamp(created_at)
    for path in (
        Path(__file__).resolve(),
        PROJECT_ROOT / "scanner_replay_alpaca.py",
        PROJECT_ROOT / "historical_store.py",
    ):
        strategy_discovery.require_committed(path)
    source = store or HistoricalDayStore.from_env()
    return _publish(
        CONTRACT_ROOT,
        "oversold-replication-development-collection-contract",
        _contract_content(created_at=created_at, store=source),
        "contract_sha256",
    )


def _load_contract(path: Path) -> dict[str, Any]:
    return _load_hashed(
        path,
        identity_field="contract_sha256",
        expected_kind=(
            "oversold_replication_development_collection_contract"
        ),
    )


def inspect_contract(
    contract_path: Path,
    *,
    store: HistoricalDayStore | None = None,
) -> tuple[Path, dict[str, Any]]:
    strategy_discovery.require_committed(contract_path)
    contract = _load_contract(contract_path)
    source = store or HistoricalDayStore.from_env()
    expected = _contract_content(
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
        "inventory_inspected": contract["inventory_binding"][
            "inspection_sha256"
        ]
        == "49103e8025cb7ff0712aaff00ac6785d3f2e485082b870bcd56f72033b9b5700",
        "complete_scope": contract["candidate_symbol_sessions"]
        == 2_334,
        "zero_days_retained": len(contract["zero_signal_dates"]) == 299,
        "substitutions_forbidden": contract["collection"][
            "substitutions_allowed"
        ]
        is False,
        "authorization_precedes_access": contract["outcome_boundary"][
            "authorization_must_be_committed_before_cache_or_provider_access"
        ]
        is True,
        "confirmation_locked": contract["outcome_boundary"][
            "confirmation_access_permitted"
        ]
        is False,
        "no_provider_access": contract["provider_requests"] == 0,
        "no_outcomes": contract["return_metrics_computed"] == 0,
        "no_broker_actions": contract["broker_actions"] == 0,
    }
    if not all(checks.values()):
        raise OversoldReplicationCollectionError(
            "development collection contract inspection failed"
        )
    content = {
        "schema_version": SCHEMA_VERSION,
        "artifact_kind": (
            "oversold_replication_development_collection_contract_inspection"
        ),
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "successor_id": SUCCESSOR_ID,
        "dataset_id": DATASET_ID,
        "state": "DEVELOPMENT_COLLECTION_CONTRACT_INSPECTED_READY",
        "contract_path": _repo_path(contract_path),
        "contract_file_sha256": sha256_file(contract_path),
        "contract_sha256": contract["contract_sha256"],
        "checks": checks,
        "evaluation_dates": contract["date_count"],
        "signal_dates": contract["signal_date_count"],
        "candidate_symbol_sessions": contract[
            "candidate_symbol_sessions"
        ],
        "provider_access_permitted_after_committed_authorization": True,
        "confirmation_access_permitted": False,
        "broker_actions_permitted": False,
        "valid": True,
    }
    return _publish(
        CONTRACT_INSPECTION_ROOT,
        "oversold-replication-development-collection-contract-inspection",
        content,
        "inspection_sha256",
    )


def _load_contract_chain(
    contract_path: Path,
    inspection_path: Path,
) -> tuple[dict[str, Any], dict[str, Any]]:
    strategy_discovery.require_committed(contract_path)
    strategy_discovery.require_committed(inspection_path)
    contract = _load_contract(contract_path)
    inspection = _load_hashed(
        inspection_path,
        identity_field="inspection_sha256",
        expected_kind=(
            "oversold_replication_development_collection_contract_inspection"
        ),
    )
    if not (
        inspection.get("state")
        == "DEVELOPMENT_COLLECTION_CONTRACT_INSPECTED_READY"
        and inspection.get("valid") is True
        and inspection.get("contract_sha256")
        == contract["contract_sha256"]
        and inspection.get(
            "provider_access_permitted_after_committed_authorization"
        )
        is True
    ):
        raise OversoldReplicationCollectionError(
            "collection contract chain is not independently ready"
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
            raise OversoldReplicationCollectionError(
                f"collection implementation drifted: {path_field}"
            )
    return contract, inspection


def authorize_development_exposure(
    contract_path: Path,
    inspection_path: Path,
) -> tuple[Path, dict[str, Any]]:
    contract, inspection = _load_contract_chain(
        contract_path,
        inspection_path,
    )
    record = outcome_exposure.build_record(
        exposure_id=EXPOSURE_ID,
        campaign_id=CAMPAIGN_ID,
        lane="development",
        recorded_at=str(contract["created_at"]),
        source_path=_repo_path(contract_path),
        source_sha256=sha256_file(contract_path),
        scope=contract["scope"],
    )
    outcome_exposure.ensure_record(record)
    matches = [
        row
        for row in outcome_exposure.read_index()
        if row.get("exposure_id") == EXPOSURE_ID
    ]
    if len(matches) != 1 or matches[0] != record:
        raise OversoldReplicationCollectionError(
            "development exposure record failed exact persistence"
        )
    content = {
        "schema_version": SCHEMA_VERSION,
        "artifact_kind": (
            "oversold_replication_development_exposure_authorization"
        ),
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "successor_id": SUCCESSOR_ID,
        "dataset_id": DATASET_ID,
        "state": "DEVELOPMENT_EXPOSURE_AUTHORIZED",
        "contract_path": _repo_path(contract_path),
        "contract_file_sha256": sha256_file(contract_path),
        "contract_sha256": contract["contract_sha256"],
        "contract_inspection_path": _repo_path(inspection_path),
        "contract_inspection_file_sha256": sha256_file(inspection_path),
        "contract_inspection_sha256": inspection["inspection_sha256"],
        "exposure_id": EXPOSURE_ID,
        "exposure_record_sha256": record["record_sha256"],
        "outcome_index_sha256_after_authorization": (
            outcome_exposure.audit()["index_sha256"]
        ),
        "candidate_symbol_sessions": contract[
            "candidate_symbol_sessions"
        ],
        "provider_requests": 0,
        "cached_full_session_inputs_opened": 0,
        "return_metrics_computed": 0,
        "confirmation_accessed": False,
        "broker_actions": 0,
    }
    return _publish(
        AUTHORIZATION_ROOT,
        "oversold-replication-development-exposure-authorization",
        content,
        "authorization_sha256",
    )


def _load_authorization(
    authorization_path: Path,
    contract: Mapping[str, Any],
) -> dict[str, Any]:
    strategy_discovery.require_committed(authorization_path)
    strategy_discovery.require_committed(
        PROJECT_ROOT / "strategy_tournament/v2/OUTCOME_EXPOSURE_INDEX.jsonl"
    )
    authorization = _load_hashed(
        authorization_path,
        identity_field="authorization_sha256",
        expected_kind=(
            "oversold_replication_development_exposure_authorization"
        ),
    )
    matches = [
        row
        for row in outcome_exposure.read_index()
        if row.get("exposure_id") == EXPOSURE_ID
    ]
    if not (
        authorization.get("state")
        == "DEVELOPMENT_EXPOSURE_AUTHORIZED"
        and authorization.get("contract_sha256")
        == contract["contract_sha256"]
        and authorization.get("candidate_symbol_sessions")
        == contract["candidate_symbol_sessions"]
        and len(matches) == 1
        and matches[0]["record_sha256"]
        == authorization.get("exposure_record_sha256")
    ):
        raise OversoldReplicationCollectionError(
            "committed development exposure authorization is invalid"
        )
    return authorization


def _full_dataset(
    store: HistoricalDayStore,
    symbol: str,
    day: str,
) -> dict[str, Any] | None:
    return store.select_dataset(
        symbol,
        day,
        kind="bars",
        channel="trades",
        timeframe="1m",
        providers=("alpaca",),
        require_complete=True,
        feed="sip",
        adjustment="raw",
    )


def collect(
    contract_path: Path,
    inspection_path: Path,
    authorization_path: Path,
    *,
    env_path: Path = PROJECT_ROOT / ".env",
    max_dates: int | None = None,
    store: HistoricalDayStore | None = None,
) -> tuple[Path, dict[str, Any]]:
    source = store or HistoricalDayStore.from_env(env_path)
    contract, inspection = _load_contract_chain(
        contract_path,
        inspection_path,
    )
    authorization = _load_authorization(
        authorization_path,
        contract,
    )
    config = AlpacaBulkConfig.from_env(env_path)
    requested = cached = received = rows_received = 0
    unresolved: list[dict[str, str]] = []
    started = datetime.now(UTC)
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
                        raise OversoldReplicationCollectionError(
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
                                "alpaca_multi_symbol_oversold_replication_development"
                            ),
                            "endpoint": ALPACA_BARS_URL,
                            "dataset_id": DATASET_ID,
                            "contract_sha256": contract[
                                "contract_sha256"
                            ],
                            "lane": "development",
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
        "authorization_sha256": authorization[
            "authorization_sha256"
        ],
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
            "oversold_replication_development_collection_status"
        ),
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "successor_id": SUCCESSOR_ID,
        "dataset_id": DATASET_ID,
        "state": (
            "DEVELOPMENT_DATA_COLLECTED_AWAITING_INSPECTION"
            if not remaining
            else "DEVELOPMENT_DATA_PARTIAL"
        ),
        "contract_path": _repo_path(contract_path),
        "contract_sha256": contract["contract_sha256"],
        "contract_inspection_path": _repo_path(inspection_path),
        "contract_inspection_sha256": inspection[
            "inspection_sha256"
        ],
        "authorization_path": _repo_path(authorization_path),
        "authorization_sha256": authorization[
            "authorization_sha256"
        ],
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
        "development_scope_indexed_as_exposed": True,
        "confirmation_accessed": False,
        "broker_actions": 0,
    }
    return _publish(
        STATUS_ROOT,
        "oversold-replication-development-collection-status",
        content,
        "status_sha256",
    )


def _expanded_rows(
    dataset: Mapping[str, Any],
    *,
    day: str,
    symbol: str,
) -> tuple[list[dict[str, Any]], bool]:
    from historical_store import expand_bar

    raw_rows = dataset.get("rows")
    if not isinstance(raw_rows, list):
        raise OversoldReplicationCollectionError(
            f"{day} {symbol}: rows are malformed"
        )
    rows = sorted(
        (expand_bar(row) for row in raw_rows),
        key=lambda row: str(row["time_et"]),
    )
    timestamps = [str(row["time_et"]) for row in rows]
    if len(timestamps) != len(set(timestamps)) or len(rows) > 390:
        raise OversoldReplicationCollectionError(
            f"{day} {symbol}: timestamps are ambiguous"
        )
    exact = len(rows) == 390
    for index, row in enumerate(rows):
        observed = datetime.fromisoformat(
            str(row["time_et"])
        ).astimezone(EASTERN)
        if (
            observed.date().isoformat() != day
            or not time(9, 30) <= observed.time() < time(16, 0)
            or row.get("interpolated") is not False
        ):
            raise OversoldReplicationCollectionError(
                f"{day} {symbol}: minute row escaped the contract"
            )
        if exact:
            expected = (
                datetime.combine(
                    date.fromisoformat(day),
                    time(9, 30),
                    tzinfo=EASTERN,
                ).timestamp()
                + 60 * index
            )
            exact = math.isclose(
                observed.timestamp(),
                expected,
                abs_tol=0.1,
            )
    return rows, exact


def inspect_data(
    contract_path: Path,
    contract_inspection_path: Path,
    authorization_path: Path,
    status_path: Path,
    *,
    store: HistoricalDayStore | None = None,
) -> tuple[Path, dict[str, Any]]:
    source = store or HistoricalDayStore.from_env()
    contract, contract_inspection = _load_contract_chain(
        contract_path,
        contract_inspection_path,
    )
    authorization = _load_authorization(
        authorization_path,
        contract,
    )
    strategy_discovery.require_committed(status_path)
    status = _load_hashed(
        status_path,
        identity_field="status_sha256",
        expected_kind=(
            "oversold_replication_development_collection_status"
        ),
    )
    if not (
        status.get("state")
        == "DEVELOPMENT_DATA_COLLECTED_AWAITING_INSPECTION"
        and status.get("contract_sha256")
        == contract["contract_sha256"]
        and status.get("authorization_sha256")
        == authorization["authorization_sha256"]
        and status.get("unresolved_symbol_sessions") == 0
    ):
        raise OversoldReplicationCollectionError(
            "development collection is not complete and committed"
        )
    _inventory_contract, _inspection, inventory = _load_inventory(
        source,
        require_committed=True,
    )
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
                raise OversoldReplicationCollectionError(
                    f"{day} {symbol}: inspected dataset is missing"
                )
            rows, exact = _expanded_rows(
                dataset,
                day=day,
                symbol=symbol,
            )
            if not rows:
                raise OversoldReplicationCollectionError(
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
                raise OversoldReplicationCollectionError(
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
        "lane": "development",
        "evaluation_dates": list(contract["evaluation_dates"]),
        "zero_signal_dates": list(contract["zero_signal_dates"]),
        "input_rows": index_rows,
    }
    _write_gzip(_input_index_path(source), private_index)
    content = {
        "schema_version": SCHEMA_VERSION,
        "artifact_kind": (
            "oversold_replication_development_data_inspection"
        ),
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "successor_id": SUCCESSOR_ID,
        "dataset_id": DATASET_ID,
        "state": "DEVELOPMENT_DATA_INSPECTED_READY",
        "contract_path": _repo_path(contract_path),
        "contract_sha256": contract["contract_sha256"],
        "contract_inspection_path": _repo_path(
            contract_inspection_path
        ),
        "contract_inspection_sha256": contract_inspection[
            "inspection_sha256"
        ],
        "authorization_path": _repo_path(authorization_path),
        "authorization_sha256": authorization[
            "authorization_sha256"
        ],
        "status_path": _repo_path(status_path),
        "status_sha256": status["status_sha256"],
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
        "development_scope_indexed_as_exposed": True,
        "confirmation_accessed": False,
        "return_metrics_computed": 0,
        "provider_requests": 0,
        "broker_actions": 0,
        "valid": True,
    }
    return _publish(
        DATA_INSPECTION_ROOT,
        "oversold-replication-development-data-inspection",
        content,
        "inspection_sha256",
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env", type=Path, default=PROJECT_ROOT / ".env")
    sub = parser.add_subparsers(dest="command", required=True)
    freeze = sub.add_parser("freeze-contract")
    freeze.add_argument("--created-at", required=True)
    inspect = sub.add_parser("inspect-contract")
    inspect.add_argument("contract", type=Path)
    authorize = sub.add_parser("authorize-development")
    authorize.add_argument("contract", type=Path)
    authorize.add_argument("inspection", type=Path)
    collect_parser = sub.add_parser("collect")
    collect_parser.add_argument("contract", type=Path)
    collect_parser.add_argument("inspection", type=Path)
    collect_parser.add_argument("authorization", type=Path)
    collect_parser.add_argument("--max-dates", type=int)
    data = sub.add_parser("inspect-data")
    data.add_argument("contract", type=Path)
    data.add_argument("inspection", type=Path)
    data.add_argument("authorization", type=Path)
    data.add_argument("status", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "freeze-contract":
            path, value = freeze_contract(
                created_at=args.created_at,
            )
        elif args.command == "inspect-contract":
            path, value = inspect_contract(args.contract)
        elif args.command == "authorize-development":
            path, value = authorize_development_exposure(
                args.contract,
                args.inspection,
            )
        elif args.command == "collect":
            path, value = collect(
                args.contract,
                args.inspection,
                args.authorization,
                env_path=args.env,
                max_dates=args.max_dates,
            )
        else:
            path, value = inspect_data(
                args.contract,
                args.inspection,
                args.authorization,
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
                        {
                            "requests": value.get(
                                "provider_requests",
                                0,
                            )
                        },
                    ),
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
        OversoldReplicationCollectionError,
        outcome_exposure.OutcomeExposureError,
        strategy_discovery.StrategyDiscoveryError,
        ValueError,
    ) as exc:
        print(
            json.dumps(
                {
                    "state": "BLOCKED",
                    "error": str(exc),
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

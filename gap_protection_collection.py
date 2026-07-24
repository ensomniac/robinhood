"""Freeze, collect, and inspect development minutes for the gap successor."""

from __future__ import annotations

import argparse
import gzip
import hashlib
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

import gap_protection_successor as successor
import outcome_exposure
import strategy_discovery
from historical_store import (
    HistoricalDayStore,
    build_dataset,
    compact_bar,
    expand_bar,
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
DATASET_ID = "dataset-equity-gap-protection-development-minutes-2026-07-24-v1"
EXPOSURE_ID = "gap-protection-development-minutes-2026-07-24-v1"
PREENTRY_INSPECTION = (
    PROJECT_ROOT
    / "strategy_tournament/v2/gap_protection_successor/"
    "dataset-equity-gap-protection-continuation-2026-07-24-v2/"
    "preentry-inspection/"
    "gap-protection-preentry-inspection-"
    "b82ec51a28d5852538fc1e518d5136dd7ea0060222529f60d0e4a1352e7d6a7c.json"
)
OUTPUT_ROOT = (
    PROJECT_ROOT
    / "strategy_tournament/v2/gap_protection_successor"
    / DATASET_ID
)
CONTRACT_ROOT = OUTPUT_ROOT / "collection-contract"
CONTRACT_INSPECTION_ROOT = OUTPUT_ROOT / "collection-contract-inspection"
STATUS_ROOT = OUTPUT_ROOT / "collection-status"
DATA_INSPECTION_ROOT = OUTPUT_ROOT / "data-inspection"


class GapProtectionCollectionError(RuntimeError):
    """The frozen development collection is incomplete or has drifted."""


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


def _load_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise GapProtectionCollectionError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise GapProtectionCollectionError(f"{path} must contain an object")
    return value


def _write_json(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_text(
            json.dumps(value, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()


def _write_gzip(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode="wb", compresslevel=6, mtime=0) as stream:
        stream.write(json.dumps(value, sort_keys=True).encode("utf-8"))
        stream.write(b"\n")
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_bytes(buffer.getvalue())
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()


def _load_gzip(path: Path) -> dict[str, Any]:
    try:
        with gzip.open(path, "rt", encoding="utf-8") as stream:
            value = json.load(stream)
    except (OSError, json.JSONDecodeError) as exc:
        raise GapProtectionCollectionError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise GapProtectionCollectionError(f"{path} must contain an object")
    return value


def _repo_path(path: Path) -> str:
    try:
        return path.resolve().relative_to(PROJECT_ROOT).as_posix()
    except ValueError as exc:
        raise GapProtectionCollectionError(f"path escaped repository: {path}") from exc


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
    if path.exists() and _load_json(path) != value:
        raise GapProtectionCollectionError(f"artifact drifted: {path}")
    if not path.exists():
        _write_json(path, value)
    return path, value


def _private_root(store: HistoricalDayStore) -> Path:
    return store.root / "_derived" / "gap_protection_collection" / DATASET_ID


def _state_path(store: HistoricalDayStore) -> Path:
    return _private_root(store) / "collection-state.json"


def _input_index_path(store: HistoricalDayStore) -> Path:
    return _private_root(store) / "development-input-index.json.gz"


def _load_preentry(
    store: HistoricalDayStore,
) -> tuple[dict[str, Any], dict[str, Any]]:
    inspection = _load_json(PREENTRY_INSPECTION)
    supplied = inspection.pop("inspection_sha256", None)
    if supplied != _hash(inspection) or inspection.get("valid") is not True:
        raise GapProtectionCollectionError("preentry inspection hash is invalid")
    inspection["inspection_sha256"] = supplied
    inventory = _load_gzip(successor._inventory_path(store))
    if (
        inventory.get("content_sha256")
        != inspection["inventory"]["content_sha256"]
        or sha256_file(successor._inventory_path(store))
        != inspection["inventory"]["file_sha256"]
        or inventory.get("target_outcomes_observed_or_derived") is not False
    ):
        raise GapProtectionCollectionError("inspected preentry inventory drifted")
    return inspection, inventory


def _development_scope(inventory: Mapping[str, Any]) -> dict[str, Any]:
    dates = list(inventory["partitions"]["development"])
    return {
        "dates": dates,
        "symbols_by_date": {
            day: sorted(
                str(row["symbol"])
                for row in inventory["candidates_by_date"][day]
            )
            for day in dates
        },
    }


def freeze_contract(
    *,
    created_at: str,
    store: HistoricalDayStore | None = None,
) -> tuple[Path, dict[str, Any]]:
    source = store or HistoricalDayStore.from_env()
    try:
        observed = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
    except ValueError as exc:
        raise GapProtectionCollectionError("created_at is invalid") from exc
    if observed.tzinfo is None:
        raise GapProtectionCollectionError("created_at needs a timezone")
    strategy_discovery.require_committed(PREENTRY_INSPECTION)
    inspection, inventory = _load_preentry(source)
    scope = _development_scope(inventory)
    dates = scope["dates"]
    candidate_pairs = sum(len(rows) for rows in scope["symbols_by_date"].values())
    content = {
        "schema_version": SCHEMA_VERSION,
        "artifact_kind": "gap_protection_development_collection_contract",
        "campaign_id": successor.CAMPAIGN_ID,
        "family_id": successor.FAMILY_ID,
        "dataset_id": DATASET_ID,
        "created_at": created_at,
        "state": "DEVELOPMENT_COLLECTION_FROZEN",
        "preentry_binding": {
            "inspection_path": _repo_path(PREENTRY_INSPECTION),
            "inspection_file_sha256": sha256_file(PREENTRY_INSPECTION),
            "inspection_sha256": inspection["inspection_sha256"],
            "inventory_content_sha256": inventory["content_sha256"],
            "inventory_file_sha256": sha256_file(
                successor._inventory_path(source)
            ),
        },
        "scope": scope,
        "date_count": len(dates),
        "candidate_symbol_sessions": candidate_pairs,
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
            "resume": "content-addressed canonical store, no duplicate request",
        },
        "outcome_boundary": {
            "lane": "development",
            "exposure_id": EXPOSURE_ID,
            "expose_full_scope_before_first_provider_request": True,
            "confirmation_access_permitted": False,
            "broker_actions_permitted": False,
        },
        "implementation_binding": {
            "collector_path": _repo_path(Path(__file__)),
            "collector_sha256": sha256_file(Path(__file__)),
            "provider_adapter_path": "scanner_replay_alpaca.py",
            "provider_adapter_sha256": sha256_file(
                PROJECT_ROOT / "scanner_replay_alpaca.py"
            ),
            "historical_store_path": "historical_store.py",
            "historical_store_sha256": sha256_file(
                PROJECT_ROOT / "historical_store.py"
            ),
        },
    }
    return _publish(
        CONTRACT_ROOT,
        "gap-protection-development-collection-contract",
        content,
        "contract_sha256",
    )


def _load_contract(path: Path) -> dict[str, Any]:
    value = _load_json(path)
    supplied = value.pop("contract_sha256", None)
    expected = _hash(value)
    value["contract_sha256"] = supplied
    if supplied != expected or not path.name.endswith(f"-{expected}.json"):
        raise GapProtectionCollectionError("collection contract hash is invalid")
    return value


def inspect_contract(
    contract_path: Path,
    store: HistoricalDayStore | None = None,
) -> tuple[Path, dict[str, Any]]:
    source = store or HistoricalDayStore.from_env()
    strategy_discovery.require_committed(contract_path)
    contract = _load_contract(contract_path)
    inspection, inventory = _load_preentry(source)
    scope = _development_scope(inventory)
    checks = {
        "committed_preentry": sha256_file(PREENTRY_INSPECTION)
        == contract["preentry_binding"]["inspection_file_sha256"],
        "semantic_preentry": inspection["inspection_sha256"]
        == contract["preentry_binding"]["inspection_sha256"],
        "private_inventory": inventory["content_sha256"]
        == contract["preentry_binding"]["inventory_content_sha256"],
        "scope_exact": scope == contract["scope"],
        "development_only": contract["outcome_boundary"]["lane"]
        == "development",
        "confirmation_locked": contract["outcome_boundary"][
            "confirmation_access_permitted"
        ]
        is False,
        "substitutions_forbidden": contract["collection"][
            "substitutions_allowed"
        ]
        is False,
        "trial_outcomes_not_accessed": inventory[
            "target_outcomes_observed_or_derived"
        ]
        is False,
    }
    if not all(checks.values()):
        raise GapProtectionCollectionError("collection contract inspection failed")
    content = {
        "schema_version": SCHEMA_VERSION,
        "artifact_kind": "gap_protection_development_collection_contract_inspection",
        "campaign_id": successor.CAMPAIGN_ID,
        "family_id": successor.FAMILY_ID,
        "dataset_id": DATASET_ID,
        "state": "DEVELOPMENT_COLLECTION_CONTRACT_INSPECTED_READY",
        "contract_path": _repo_path(contract_path),
        "contract_file_sha256": sha256_file(contract_path),
        "contract_sha256": contract["contract_sha256"],
        "checks": checks,
        "date_count": len(scope["dates"]),
        "candidate_symbol_sessions": sum(
            len(rows) for rows in scope["symbols_by_date"].values()
        ),
        "provider_access_permitted": True,
        "confirmation_access_permitted": False,
        "broker_actions_permitted": False,
        "valid": True,
    }
    return _publish(
        CONTRACT_INSPECTION_ROOT,
        "gap-protection-development-collection-contract-inspection",
        content,
        "inspection_sha256",
    )


def _load_contract_inspection(
    contract_path: Path,
    inspection_path: Path,
) -> tuple[dict[str, Any], dict[str, Any]]:
    strategy_discovery.require_committed(contract_path)
    strategy_discovery.require_committed(inspection_path)
    contract = _load_contract(contract_path)
    inspection = _load_json(inspection_path)
    supplied = inspection.pop("inspection_sha256", None)
    expected = _hash(inspection)
    inspection["inspection_sha256"] = supplied
    if (
        supplied != expected
        or inspection.get("valid") is not True
        or inspection.get("contract_sha256") != contract["contract_sha256"]
        or inspection.get("provider_access_permitted") is not True
    ):
        raise GapProtectionCollectionError(
            "collection requires a valid committed contract inspection"
        )
    for path_field, hash_field in (
        ("collector_path", "collector_sha256"),
        ("provider_adapter_path", "provider_adapter_sha256"),
        ("historical_store_path", "historical_store_sha256"),
    ):
        path = PROJECT_ROOT / contract["implementation_binding"][path_field]
        if sha256_file(path) != contract["implementation_binding"][hash_field]:
            raise GapProtectionCollectionError(
                f"collection implementation drifted: {path_field}"
            )
    return contract, inspection


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


def _expose_development(contract_path: Path, contract: Mapping[str, Any]) -> None:
    record = outcome_exposure.build_record(
        exposure_id=EXPOSURE_ID,
        campaign_id=successor.CAMPAIGN_ID,
        lane="development",
        recorded_at=str(contract["created_at"]),
        source_path=_repo_path(contract_path),
        source_sha256=sha256_file(contract_path),
        scope=contract["scope"],
    )
    outcome_exposure.ensure_record(record)


def collect(
    contract_path: Path,
    inspection_path: Path,
    *,
    env_path: Path = PROJECT_ROOT / ".env",
    store: HistoricalDayStore | None = None,
) -> tuple[Path, dict[str, Any]]:
    source = store or HistoricalDayStore.from_env(env_path)
    contract, inspection = _load_contract_inspection(
        contract_path,
        inspection_path,
    )
    _expose_development(contract_path, contract)
    config = AlpacaBulkConfig.from_env(env_path)
    requested = cached = received = rows_received = 0
    unresolved: list[dict[str, str]] = []
    started = datetime.now(UTC)
    with AlpacaBulkBarsClient(config) as client:
        for day in contract["scope"]["dates"]:
            symbols = list(contract["scope"]["symbols_by_date"][day])
            missing = [
                symbol
                for symbol in symbols
                if _full_dataset(source, symbol, day) is None
            ]
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
                    timestamps = [str(row["time_et"]) for row in normalized]
                    if (
                        len(timestamps) != len(set(timestamps))
                        or len(normalized) > 390
                    ):
                        raise GapProtectionCollectionError(
                            f"{day} {symbol}: provider minute bars are ambiguous"
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
                                "alpaca_multi_symbol_gap_protection_development"
                            ),
                            "endpoint": ALPACA_BARS_URL,
                            "dataset_id": DATASET_ID,
                            "contract_sha256": contract["contract_sha256"],
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
    for day in contract["scope"]["dates"]:
        for symbol in contract["scope"]["symbols_by_date"][day]:
            if _full_dataset(source, symbol, day) is None and not any(
                row["date"] == day and row["symbol"] == symbol
                for row in unresolved
            ):
                unresolved.append(
                    {
                        "date": day,
                        "symbol": symbol,
                        "reason": "canonical_dataset_missing",
                    }
                )
    completed = datetime.now(UTC)
    state = {
        "schema_version": SCHEMA_VERSION,
        "dataset_id": DATASET_ID,
        "contract_sha256": contract["contract_sha256"],
        "inspection_sha256": inspection["inspection_sha256"],
        "requested_symbol_sessions": requested,
        "cached_before_collection": cached,
        "provider_symbol_sessions_received": received,
        "provider_rows_received": rows_received,
        "provider_requests": requests,
        "provider_retries": retries,
        "provider_request_seconds": request_seconds,
        "pacing_wait_seconds": pacing_wait_seconds,
        "unresolved": unresolved,
        "started_at": started.isoformat(),
        "completed_at": completed.isoformat(),
    }
    _write_json(_state_path(source), state)
    content = {
        "schema_version": SCHEMA_VERSION,
        "artifact_kind": "gap_protection_development_collection_status",
        "campaign_id": successor.CAMPAIGN_ID,
        "family_id": successor.FAMILY_ID,
        "dataset_id": DATASET_ID,
        "state": (
            "DEVELOPMENT_DATA_COLLECTED_AWAITING_INSPECTION"
            if not unresolved
            else "DEVELOPMENT_DATA_INCOMPLETE"
        ),
        "contract_path": _repo_path(contract_path),
        "contract_sha256": contract["contract_sha256"],
        "contract_inspection_path": _repo_path(inspection_path),
        "contract_inspection_sha256": inspection["inspection_sha256"],
        "date_count": contract["date_count"],
        "candidate_symbol_sessions": requested,
        "cached_before_collection": cached,
        "provider_symbol_sessions_received": received,
        "provider_rows_received": rows_received,
        "provider_telemetry": {
            "requests": requests,
            "retries": retries,
            "request_seconds": request_seconds,
            "pacing_wait_seconds": pacing_wait_seconds,
            "cache_hits": cached,
            "failures": len(unresolved),
        },
        "unresolved_symbol_sessions": len(unresolved),
        "unresolved_reason_counts": dict(
            sorted(Counter(row["reason"] for row in unresolved).items())
        ),
        "development_scope_indexed_as_exposed": True,
        "confirmation_accessed": False,
        "broker_actions": 0,
    }
    return _publish(
        STATUS_ROOT,
        "gap-protection-development-collection-status",
        content,
        "status_sha256",
    )


def _expanded_rows(
    dataset: Mapping[str, Any],
    *,
    day: str,
    symbol: str,
) -> tuple[list[dict[str, Any]], bool]:
    raw_rows = dataset.get("rows")
    if not isinstance(raw_rows, list):
        raise GapProtectionCollectionError(f"{day} {symbol}: rows are malformed")
    rows = sorted(
        (expand_bar(row) for row in raw_rows),
        key=lambda row: str(row["time_et"]),
    )
    timestamps = [str(row["time_et"]) for row in rows]
    if len(timestamps) != len(set(timestamps)) or len(rows) > 390:
        raise GapProtectionCollectionError(f"{day} {symbol}: timestamps are ambiguous")
    exact = len(rows) == 390
    for index, row in enumerate(rows):
        observed = datetime.fromisoformat(str(row["time_et"])).astimezone(EASTERN)
        if (
            observed.date().isoformat() != day
            or not time(9, 30) <= observed.time() < time(16, 0)
            or row.get("interpolated") is not False
        ):
            raise GapProtectionCollectionError(
                f"{day} {symbol}: minute row escaped the contract"
            )
        if exact:
            expected = datetime.combine(
                date.fromisoformat(day),
                time(9, 30),
                tzinfo=EASTERN,
            ).timestamp() + 60 * index
            exact = math.isclose(observed.timestamp(), expected, abs_tol=0.1)
    return rows, exact


def inspect_data(
    contract_path: Path,
    contract_inspection_path: Path,
    status_path: Path,
    *,
    store: HistoricalDayStore | None = None,
) -> tuple[Path, dict[str, Any]]:
    source = store or HistoricalDayStore.from_env()
    contract, contract_inspection = _load_contract_inspection(
        contract_path,
        contract_inspection_path,
    )
    strategy_discovery.require_committed(status_path)
    status = _load_json(status_path)
    supplied = status.pop("status_sha256", None)
    expected = _hash(status)
    status["status_sha256"] = supplied
    if (
        supplied != expected
        or status.get("state")
        != "DEVELOPMENT_DATA_COLLECTED_AWAITING_INSPECTION"
        or status.get("contract_sha256") != contract["contract_sha256"]
        or status.get("unresolved_symbol_sessions") != 0
    ):
        raise GapProtectionCollectionError(
            "development collection is not complete and committed"
        )
    _inspection, inventory = _load_preentry(source)
    candidate_metadata = {
        day: {
            str(row["symbol"]): row
            for row in inventory["candidates_by_date"][day]
        }
        for day in contract["scope"]["dates"]
    }
    index_rows: list[dict[str, Any]] = []
    exact_sessions = sparse_sessions = minute_rows = 0
    for day in contract["scope"]["dates"]:
        for symbol in contract["scope"]["symbols_by_date"][day]:
            dataset = _full_dataset(source, symbol, day)
            if dataset is None:
                raise GapProtectionCollectionError(
                    f"{day} {symbol}: inspected dataset is missing"
                )
            rows, exact = _expanded_rows(dataset, day=day, symbol=symbol)
            if not rows:
                raise GapProtectionCollectionError(
                    f"{day} {symbol}: inspected dataset is empty"
                )
            if not math.isclose(
                float(rows[0]["open"]),
                float(candidate_metadata[day][symbol]["open_price"]),
                rel_tol=0,
                abs_tol=1e-10,
            ):
                raise GapProtectionCollectionError(
                    f"{day} {symbol}: 09:30 open drifted from preentry"
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
        "input_rows": index_rows,
    }
    _write_gzip(_input_index_path(source), private_index)
    content = {
        "schema_version": SCHEMA_VERSION,
        "artifact_kind": "gap_protection_development_data_inspection",
        "campaign_id": successor.CAMPAIGN_ID,
        "family_id": successor.FAMILY_ID,
        "dataset_id": DATASET_ID,
        "state": "DEVELOPMENT_DATA_INSPECTED_READY",
        "contract_path": _repo_path(contract_path),
        "contract_sha256": contract["contract_sha256"],
        "contract_inspection_path": _repo_path(contract_inspection_path),
        "contract_inspection_sha256": contract_inspection["inspection_sha256"],
        "status_path": _repo_path(status_path),
        "status_sha256": status["status_sha256"],
        "date_count": contract["date_count"],
        "candidate_symbol_sessions": len(index_rows),
        "minute_rows": minute_rows,
        "exact_390_contiguous_symbol_sessions": exact_sessions,
        "sparse_symbol_sessions_retained_as_no_signal": sparse_sessions,
        "private_input_index_content_sha256": _hash(private_index),
        "development_scope_indexed_as_exposed": True,
        "confirmation_accessed": False,
        "return_metrics_computed": 0,
        "provider_requests": 0,
        "broker_actions": 0,
        "valid": True,
    }
    return _publish(
        DATA_INSPECTION_ROOT,
        "gap-protection-development-data-inspection",
        content,
        "inspection_sha256",
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    freeze = subparsers.add_parser("freeze-contract")
    freeze.add_argument("--created-at", required=True)
    inspect = subparsers.add_parser("inspect-contract")
    inspect.add_argument("contract", type=Path)
    collect_parser = subparsers.add_parser("collect")
    collect_parser.add_argument("contract", type=Path)
    collect_parser.add_argument("inspection", type=Path)
    collect_parser.add_argument("--env", type=Path, default=PROJECT_ROOT / ".env")
    data = subparsers.add_parser("inspect-data")
    data.add_argument("contract", type=Path)
    data.add_argument("contract_inspection", type=Path)
    data.add_argument("status", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "freeze-contract":
            path, value = freeze_contract(created_at=args.created_at)
        elif args.command == "inspect-contract":
            path, value = inspect_contract(args.contract)
        elif args.command == "collect":
            path, value = collect(
                args.contract,
                args.inspection,
                env_path=args.env,
            )
        else:
            path, value = inspect_data(
                args.contract,
                args.contract_inspection,
                args.status,
            )
        identity_field = next(
            field
            for field in (
                "inspection_sha256",
                "status_sha256",
                "contract_sha256",
            )
            if field in value
        )
        print(
            json.dumps(
                {
                    "path": str(path),
                    "state": value["state"],
                    "sha256": value[identity_field],
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    except (
        GapProtectionCollectionError,
        KeyError,
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


if __name__ == "__main__":
    raise SystemExit(main())

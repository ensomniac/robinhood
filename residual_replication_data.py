"""Freeze and collect the disjoint long-history residual-reversal inputs.

Only security-master metadata is opened while the collection contract is
frozen.  Price bars are opened only after that contract and its independent
inspection are committed.  Confirmation prices remain unopened until an exact
winner is preregistered.
"""

from __future__ import annotations

import argparse
import gzip
import io
import json
import os
import time
from collections.abc import Mapping, Sequence
from datetime import date, datetime, time as wall_time, timedelta, timezone
from pathlib import Path
from typing import Any

import outcome_exposure
import strategy_discovery
from historical_store import (
    DEFAULT_ENV_PATH,
    EASTERN,
    HistoricalStoreConfig,
    canonical_json_bytes,
    canonical_sha256,
    sha256_file,
)
from scanner_replay import ScannerReplayError
from scanner_replay_alpaca import AlpacaBulkBarsClient, AlpacaBulkConfig


PROJECT_ROOT = Path(__file__).resolve().parent
CAMPAIGN_ID = "multi-strategy-portfolio-validation-v2"
FAMILY_ID = "liquid-equity-market-residual-reversal-replication"
MECHANISM_FAMILY = "two-to-three-day-cross-sectional-reversal"
SUCCESSOR_ID = (
    "two-to-three-day-cross-sectional-reversal-v6-disjoint-long-history"
)
CONTRACT_KIND = "residual-replication-data-contract"
CONTRACT_STATE = "DATA_CONTRACT_FROZEN"
CONTRACT_INSPECTION_KIND = "residual-replication-data-contract-inspection"
CONTRACT_INSPECTION_STATE = "DATA_CONTRACT_INSPECTED"
COLLECTION_KIND = "residual-replication-development-collection"
COLLECTION_STATE = "DEVELOPMENT_COLLECTED_UNINSPECTED"
COLLECTION_INSPECTION_KIND = "residual-replication-development-inspection"
COLLECTION_INSPECTION_STATE = "DEVELOPMENT_DATA_INSPECTED"
DEVELOPMENT_SIGNAL_COUNT = 200
MAXIMUM_HOLD_SESSIONS = 5
MINIMUM_CONFIRMATION_SIGNAL_CAPACITY = 20
ROOT = (
    PROJECT_ROOT
    / "strategy_tournament/v2/continuous"
    / SUCCESSOR_ID
)

CALENDAR_PATH = (
    PROJECT_ROOT
    / "historical_batches/challenger_orb_retest_v1/"
    "session-calendar-2023-01-through-2026-07.json"
)
SPLIT_SOURCE_PATH = (
    PROJECT_ROOT
    / "historical_batches/challenger_orb_retest_v1_tranche2/"
    "split-actions-source.json"
)

SOURCE_TRANCHES = (
    {
        "name": "challenger-v1",
        "selection": PROJECT_ROOT
        / "historical_batches/challenger_orb_retest_v1/"
        "selection-2026-07-20-100-days.json",
        "status": PROJECT_ROOT
        / "historical_batches/challenger_orb_retest_v1/"
        "scanner-contract-status.json",
        "detail": PROJECT_ROOT
        / "learning_runs/challenger_orb_retest_v1/scanner_replay_v2/"
        "scanner-replay-detail.json",
    },
    {
        "name": "challenger-v2",
        "selection": PROJECT_ROOT
        / "historical_batches/challenger_orb_retest_v1_tranche2/"
        "selection-2026-07-21-100-days-v2.json",
        "status": PROJECT_ROOT
        / "historical_batches/challenger_orb_retest_v1_tranche2/"
        "scanner-contract-status.json",
        "detail": PROJECT_ROOT
        / "learning_runs/challenger_orb_retest_v1_tranche2/scanner_replay/"
        "scanner-replay-detail.json",
    },
    {
        "name": "challenger-v3",
        "selection": PROJECT_ROOT
        / "historical_batches/challenger_orb_retest_v1_tranche3/"
        "selection-2026-07-22-100-days-v3.json",
        "status": PROJECT_ROOT
        / "historical_batches/challenger_orb_retest_v1_tranche3/"
        "scanner-contract-status.json",
        "detail": PROJECT_ROOT
        / "learning_runs/challenger_orb_retest_v1_tranche3/scanner_replay/"
        "scanner-replay-detail.json",
    },
)


class ResidualReplicationDataError(RuntimeError):
    """A frozen source, evidence boundary, or collection result is unsafe."""


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ResidualReplicationDataError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise ResidualReplicationDataError(f"{path} must contain an object")
    return value


def _read_gzip(path: Path) -> Any:
    try:
        with gzip.open(path, "rt", encoding="utf-8") as source:
            return json.load(source)
    except (OSError, json.JSONDecodeError) as exc:
        raise ResidualReplicationDataError(f"cannot read {path}: {exc}") from exc


def _repo_path(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(PROJECT_ROOT.resolve()))
    except ValueError as exc:
        raise ResidualReplicationDataError(
            f"path escaped repository: {path}"
        ) from exc


def _timestamp(value: str, field: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ResidualReplicationDataError(f"{field} is invalid") from exc
    if parsed.tzinfo is None:
        raise ResidualReplicationDataError(f"{field} needs a timezone")
    return parsed.astimezone(timezone.utc)


def _calendar() -> list[str]:
    try:
        raw = json.loads(CALENDAR_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ResidualReplicationDataError(
            "frozen exchange calendar is unavailable"
        ) from exc
    if not isinstance(raw, list):
        raise ResidualReplicationDataError("frozen exchange calendar is malformed")
    dates = [
        str(row.get("date"))
        for row in raw
        if isinstance(row, Mapping) and isinstance(row.get("date"), str)
    ]
    if dates != sorted(set(dates)):
        raise ResidualReplicationDataError(
            "frozen exchange calendar is not chronological"
        )
    return dates


def _source_graph() -> tuple[list[str], dict[str, dict[str, str]], list[dict[str, Any]]]:
    dates: list[str] = []
    identities: dict[str, dict[str, str]] = {}
    bindings: list[dict[str, Any]] = []
    for source in SOURCE_TRANCHES:
        selection = _read_json(source["selection"])
        status = _read_json(source["status"])
        detail = _read_json(source["detail"])
        selected = selection.get("selected_dates")
        if not (
            isinstance(selected, list)
            and len(selected) == 100
            and len(set(map(str, selected))) == 100
            and selection.get("target_outcomes_observed_or_derived") is False
            and status.get("status") == "FROZEN_READY"
            and status.get("target_outcomes_observed_or_derived") is False
            and status.get("substitutions_allowed") is False
            and set(detail.get("dates", {})) == set(selected)
        ):
            raise ResidualReplicationDataError(
                f"{source['name']}: source tranche is not outcome locked"
            )
        overlap = set(dates) & set(map(str, selected))
        if overlap:
            raise ResidualReplicationDataError(
                f"source tranches overlap on {min(overlap)}"
            )
        for day in selected:
            raw = detail["dates"][day]
            evaluations = raw.get("evaluations") if isinstance(raw, Mapping) else None
            if not isinstance(evaluations, list):
                raise ResidualReplicationDataError(
                    f"{source['name']} {day}: security master is malformed"
                )
            by_symbol: dict[str, str] = {}
            for item in evaluations:
                if not isinstance(item, Mapping):
                    raise ResidualReplicationDataError(
                        f"{source['name']} {day}: security row is malformed"
                    )
                symbol = str(item.get("symbol") or "").strip().upper()
                exchange = str(item.get("primary_exchange") or "")
                identity = str(item.get("instrument_id") or "")
                if exchange not in {"XNAS", "XNYS"}:
                    continue
                if not symbol or not identity or symbol in by_symbol:
                    raise ResidualReplicationDataError(
                        f"{source['name']} {day}: identity is incomplete"
                    )
                by_symbol[symbol] = identity
            if len(by_symbol) < 500 or len(set(by_symbol.values())) != len(by_symbol):
                raise ResidualReplicationDataError(
                    f"{source['name']} {day}: common-stock denominator is incomplete"
                )
            identities[str(day)] = by_symbol
        dates.extend(map(str, selected))
        manifest_path = PROJECT_ROOT / str(status["manifest_path"])
        manifest = _read_json(manifest_path)
        if (
            not manifest_path.is_file()
            or status.get("manifest_sha256")
            != manifest.get("manifest_sha256")
        ):
            raise ResidualReplicationDataError(
                f"{source['name']}: scanner manifest binding drifted"
            )
        bindings.append(
            {
                "name": source["name"],
                "selection_path": _repo_path(source["selection"]),
                "selection_sha256": sha256_file(source["selection"]),
                "status_path": _repo_path(source["status"]),
                "status_sha256": sha256_file(source["status"]),
                "manifest_path": _repo_path(manifest_path),
                "manifest_sha256": sha256_file(manifest_path),
                "detail_path": _repo_path(source["detail"]),
                "detail_sha256": sha256_file(source["detail"]),
                "selected_dates": len(selected),
            }
        )
    ordered = sorted(dates)
    if len(ordered) != 300 or len(set(ordered)) != 300:
        raise ResidualReplicationDataError("source graph needs 300 disjoint dates")
    return ordered, identities, bindings


def _partitions(
    source_dates: Sequence[str],
) -> tuple[list[str], list[str], list[str], list[str], list[str]]:
    calendar = _calendar()
    positions = {day: index for index, day in enumerate(calendar)}
    eligible = [
        day
        for day in source_dates
        if day in positions and positions[day] + MAXIMUM_HOLD_SESSIONS < len(calendar)
    ]
    if len(eligible) < (
        DEVELOPMENT_SIGNAL_COUNT + MINIMUM_CONFIRMATION_SIGNAL_CAPACITY
    ):
        raise ResidualReplicationDataError("source graph has insufficient capacity")
    development_signals = eligible[:DEVELOPMENT_SIGNAL_COUNT]
    final_development_index = (
        positions[development_signals[-1]] + MAXIMUM_HOLD_SESSIONS
    )
    development_dates = calendar[: final_development_index + 1]
    embargo = calendar[
        final_development_index + 1 : final_development_index + 6
    ]
    if len(embargo) != 5:
        raise ResidualReplicationDataError("five-session embargo is unavailable")
    confirmation_signals = [
        day for day in eligible[DEVELOPMENT_SIGNAL_COUNT:] if day > embargo[-1]
    ]
    if len(confirmation_signals) < MINIMUM_CONFIRMATION_SIGNAL_CAPACITY:
        raise ResidualReplicationDataError(
            "INSUFFICIENT_POWER_CAPACITY: confirmation reserve is below 20"
        )
    final_confirmation_index = (
        positions[confirmation_signals[-1]] + MAXIMUM_HOLD_SESSIONS
    )
    confirmation_dates = calendar[
        final_development_index + 6 : final_confirmation_index + 1
    ]
    return (
        development_dates,
        development_signals,
        embargo,
        confirmation_dates,
        confirmation_signals,
    )


def _split_binding() -> dict[str, Any]:
    source = _read_json(SPLIT_SOURCE_PATH)
    artifact = source.get("artifact")
    if not isinstance(artifact, Mapping):
        raise ResidualReplicationDataError("split source artifact is missing")
    data_path = PROJECT_ROOT / str(artifact.get("local_ignored_path", ""))
    if (
        source.get("source", {}).get("provider") != "Massive"
        or source.get("source", {}).get("query_range", {}).get(
            "execution_date_gte"
        )
        != "2023-01-03"
        or source.get("source", {}).get("query_range", {}).get(
            "execution_date_lte"
        )
        != "2026-07-17"
        or not data_path.is_file()
        or artifact.get("sha256") != sha256_file(data_path)
    ):
        raise ResidualReplicationDataError("split source binding drifted")
    return {
        "source_path": _repo_path(SPLIT_SOURCE_PATH),
        "source_sha256": sha256_file(SPLIT_SOURCE_PATH),
        "data_path": _repo_path(data_path),
        "data_sha256": sha256_file(data_path),
        "events": int(artifact["events"]),
        "start": "2023-01-03",
        "end": "2026-07-17",
    }


def freeze_contract(
    *,
    created_at: str,
    root: Path = ROOT,
) -> tuple[Path, dict[str, Any]]:
    _timestamp(created_at, "created_at")
    source_dates, identities, bindings = _source_graph()
    (
        development_dates,
        development_signals,
        embargo,
        confirmation_dates,
        confirmation_signals,
    ) = _partitions(source_dates)
    development_scope = {"dates": development_signals, "symbols": ["*"]}
    confirmation_scope = {"dates": confirmation_signals, "symbols": ["*"]}
    index = outcome_exposure.read_index()
    if outcome_exposure.find_overlaps(development_scope, index):
        raise ResidualReplicationDataError(
            "disjoint development source is already outcome exposed"
        )
    outcome_exposure.assert_untouched(confirmation_scope, index)
    outcome_exposure.assert_disjoint([development_scope, confirmation_scope])
    symbols = sorted(
        {"SPY", *{symbol for day in development_signals for symbol in identities[day]}}
    )
    implementation_files = (
        "residual_replication_data.py",
        "residual_replication_inspection.py",
        "residual_replication_discovery.py",
        "residual_replication_plugin.py",
        "dense_strategy_runtime.py",
        "strategy_discovery.py",
        "learning_statistics.py",
    )
    implementation_hashes = {
        name: sha256_file(PROJECT_ROOT / name)
        for name in implementation_files
        if (PROJECT_ROOT / name).is_file()
    }
    payload = {
        "schema_version": 1,
        "artifact_kind": CONTRACT_KIND,
        "state": CONTRACT_STATE,
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "mechanism_family": MECHANISM_FAMILY,
        "successor_id": SUCCESSOR_ID,
        "created_at": created_at,
        "source_tranches": bindings,
        "source_date_count": len(source_dates),
        "source_dates_sha256": canonical_sha256(source_dates),
        "calendar_path": _repo_path(CALENDAR_PATH),
        "calendar_sha256": sha256_file(CALENDAR_PATH),
        "development_dates": development_dates,
        "development_signal_dates": development_signals,
        "embargo_dates": embargo,
        "confirmation_dates": confirmation_dates,
        "confirmation_signal_dates": confirmation_signals,
        "development_scope": development_scope,
        "confirmation_scope": confirmation_scope,
        "development_symbol_count": len(symbols),
        "development_symbols_sha256": canonical_sha256(symbols),
        "split_source": _split_binding(),
        "collection_contract": {
            "provider": "Alpaca historical SIP",
            "endpoint": "https://data.alpaca.markets/v2/stocks/bars",
            "feed": "sip",
            "adjustment": "raw",
            "asof": "-",
            "timeframe": "1Day",
            "development_start_inclusive": development_dates[0],
            "development_end_inclusive": development_dates[-1],
            "confirmation_prices_locked": True,
            "provider_substitution_allowed": False,
            "interpolation_allowed": False,
            "resumable_symbol_batches": True,
        },
        "universe_contract": {
            "security_type": "point-in-time XNAS/XNYS common stock",
            "minimum_prior_close": 10.0,
            "minimum_prior_20_session_median_dollar_volume": 50_000_000.0,
            "ranking": "top 250 by prior 60-session median close-times-volume",
            "listing_identity_required": True,
            "split_affected_feature_and_hold_windows": "excluded",
        },
        "implementation_hashes": implementation_hashes,
        "outcome_exposure_index_sha256": outcome_exposure.audit()[
            "index_sha256"
        ],
        "market_prices_accessed_before_freeze": False,
        "strategy_metrics_accessed_before_freeze": False,
        "confirmation_prices_accessed": False,
        "substitutions": 0,
        "broker_actions": 0,
    }
    return strategy_discovery._write_artifact(
        payload,
        root / "data-contract",
        "residual-replication-data-contract",
    )


def _load_contract(path: Path, *, enforce_commit: bool) -> dict[str, Any]:
    if enforce_commit:
        strategy_discovery.require_committed(path)
    contract = strategy_discovery.load_artifact(
        path, expected_kind=CONTRACT_KIND
    )
    if not (
        contract.get("state") == CONTRACT_STATE
        and contract.get("confirmation_prices_accessed") is False
        and contract.get("strategy_metrics_accessed_before_freeze") is False
        and contract.get("substitutions") == 0
        and contract.get("broker_actions") == 0
    ):
        raise ResidualReplicationDataError("data contract is not collection-ready")
    for name, expected in contract.get("implementation_hashes", {}).items():
        implementation = PROJECT_ROOT / str(name)
        if not implementation.is_file() or sha256_file(implementation) != expected:
            raise ResidualReplicationDataError(
                f"frozen implementation drifted: {name}"
            )
        if enforce_commit:
            strategy_discovery.require_committed(implementation)
    return contract


def _gzip_bytes(value: Any) -> bytes:
    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode="wb", mtime=0) as target:
        target.write(canonical_json_bytes(value) + b"\n")
    return buffer.getvalue()


def _write_external(
    path: Path, value: Any, config: HistoricalStoreConfig
) -> None:
    encoded = _gzip_bytes(value)
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if path.read_bytes() != encoded:
            raise ResidualReplicationDataError(
                f"immutable private artifact drifted: {path}"
            )
        return
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_bytes(encoded)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _normalize_daily_rows(
    symbol: str, rows: Sequence[Mapping[str, Any]]
) -> list[dict[str, Any]]:
    output: list[dict[str, Any]] = []
    seen: set[str] = set()
    for raw in rows:
        try:
            observed = datetime.fromisoformat(
                str(raw["t"]).replace("Z", "+00:00")
            )
            if observed.tzinfo is None:
                observed = observed.replace(tzinfo=timezone.utc)
            day = observed.astimezone(EASTERN).date().isoformat()
            opened = float(raw["o"])
            high = float(raw["h"])
            low = float(raw["l"])
            close = float(raw["c"])
            volume = int(float(raw["v"]))
        except (KeyError, TypeError, ValueError) as exc:
            raise ResidualReplicationDataError(
                f"{symbol}: provider daily bar is malformed"
            ) from exc
        if (
            day in seen
            or min(opened, high, low, close) <= 0
            or not low <= min(opened, close) <= max(opened, close) <= high
            or volume < 0
        ):
            raise ResidualReplicationDataError(
                f"{symbol} {day}: provider daily bar is invalid"
            )
        seen.add(day)
        output.append(
            {
                "date": day,
                "open": opened,
                "high": high,
                "low": low,
                "close": close,
                "volume": volume,
            }
        )
    return sorted(output, key=lambda item: item["date"])


def _development_identities(
    contract: Mapping[str, Any],
) -> dict[str, dict[str, str]]:
    source_dates, identities, _bindings = _source_graph()
    if canonical_sha256(source_dates) != contract["source_dates_sha256"]:
        raise ResidualReplicationDataError("source date graph drifted")
    selected = {
        day: identities[day] for day in contract["development_signal_dates"]
    }
    symbols = sorted(
        {"SPY", *{symbol for values in selected.values() for symbol in values}}
    )
    if (
        len(symbols) != contract["development_symbol_count"]
        or canonical_sha256(symbols) != contract["development_symbols_sha256"]
    ):
        raise ResidualReplicationDataError("development symbol union drifted")
    return selected


def _load_split_dates(
    contract: Mapping[str, Any],
) -> dict[str, list[str]]:
    binding = contract["split_source"]
    path = PROJECT_ROOT / str(binding["data_path"])
    if not path.is_file() or sha256_file(path) != binding["data_sha256"]:
        raise ResidualReplicationDataError("split data drifted")
    rows = _read_gzip(path)
    if not isinstance(rows, list) or len(rows) != binding["events"]:
        raise ResidualReplicationDataError("split rows are incomplete")
    result: dict[str, set[str]] = {}
    for row in rows:
        if not isinstance(row, Mapping):
            raise ResidualReplicationDataError("split row is malformed")
        symbol = str(row.get("ticker") or "").strip().upper()
        day = str(row.get("execution_date") or "")
        if not symbol or not day:
            raise ResidualReplicationDataError("split identity is malformed")
        result.setdefault(symbol, set()).add(day)
    return {symbol: sorted(days) for symbol, days in result.items()}


def _inspection(path: Path, *, enforce_commit: bool) -> dict[str, Any]:
    if enforce_commit:
        strategy_discovery.require_committed(path)
    inspection = strategy_discovery.load_artifact(
        path, expected_kind=CONTRACT_INSPECTION_KIND
    )
    contract_path = PROJECT_ROOT / str(inspection.get("contract_path", ""))
    contract = _load_contract(contract_path, enforce_commit=enforce_commit)
    checks = inspection.get("checks")
    if not (
        inspection.get("state") == CONTRACT_INSPECTION_STATE
        and inspection.get("contract_sha256") == contract["artifact_sha256"]
        and isinstance(checks, Mapping)
        and checks
        and all(checks.values())
    ):
        raise ResidualReplicationDataError(
            "data contract inspection is incomplete"
        )
    return contract


def collect_development(
    inspection_path: Path,
    *,
    store_config: HistoricalStoreConfig | None = None,
    root: Path = ROOT,
    enforce_commit: bool = True,
) -> tuple[Path, dict[str, Any]]:
    contract = _inspection(inspection_path, enforce_commit=enforce_commit)
    config = store_config or HistoricalStoreConfig.from_env(DEFAULT_ENV_PATH)
    private_root = (
        config.root
        / "_derived/residual_reversal_replication"
        / str(contract["artifact_sha256"])
        / "development"
    )
    identities = _development_identities(contract)
    symbols = sorted(
        {"SPY", *{symbol for values in identities.values() for symbol in values}}
    )
    provider_config = AlpacaBulkConfig.from_env(DEFAULT_ENV_PATH)
    batches = [
        symbols[index : index + provider_config.batch_size]
        for index in range(0, len(symbols), provider_config.batch_size)
    ]
    start = datetime.combine(
        date.fromisoformat(contract["development_dates"][0]),
        wall_time(0, 0),
        tzinfo=EASTERN,
    )
    end = datetime.combine(
        date.fromisoformat(contract["development_dates"][-1]) + timedelta(days=1),
        wall_time(0, 0),
        tzinfo=EASTERN,
    )
    telemetry = {
        "requests": 0,
        "request_seconds": 0.0,
        "pacing_wait_seconds": 0.0,
        "cache_hits": 0,
        "failures": 0,
    }
    started = datetime.now(timezone.utc)
    rows_by_symbol: dict[str, list[dict[str, Any]]] = {}
    with AlpacaBulkBarsClient(provider_config) as client:
        for index, symbols_batch in enumerate(batches, 1):
            batch_id = canonical_sha256(
                {
                    "symbols": symbols_batch,
                    "start": contract["development_dates"][0],
                    "end": contract["development_dates"][-1],
                    "feed": "sip",
                    "adjustment": "raw",
                    "timeframe": "1Day",
                }
            )
            checkpoint = private_root / "batches" / f"{batch_id}.json.gz"
            if checkpoint.exists():
                payload = _read_gzip(checkpoint)
                if not (
                    isinstance(payload, Mapping)
                    and payload.get("symbols") == symbols_batch
                    and isinstance(payload.get("rows_by_symbol"), Mapping)
                ):
                    raise ResidualReplicationDataError(
                        "development checkpoint drifted"
                    )
                batch_rows = dict(payload["rows_by_symbol"])
                telemetry["cache_hits"] += 1
            else:
                request_started = time.monotonic()
                try:
                    raw, pages = client.fetch(
                        symbols_batch,
                        timeframe="1Day",
                        start=start,
                        end=end,
                    )
                except ScannerReplayError:
                    telemetry["failures"] += 1
                    raise
                telemetry["requests"] += pages
                telemetry["request_seconds"] += (
                    time.monotonic() - request_started
                )
                batch_rows = {
                    symbol: _normalize_daily_rows(symbol, raw.get(symbol, []))
                    for symbol in symbols_batch
                }
                _write_external(
                    checkpoint,
                    {
                        "schema_version": 1,
                        "symbols": symbols_batch,
                        "rows_by_symbol": batch_rows,
                    },
                    config,
                )
            rows_by_symbol.update(batch_rows)
            print(
                f"daily batch {index}/{len(batches)}: "
                f"{len(symbols_batch)} symbols",
                flush=True,
            )
        telemetry["pacing_wait_seconds"] = float(client.pacing_wait_seconds)
        telemetry["request_seconds"] = float(client.request_seconds)
    dataset = {
        "schema_version": 1,
        "family_id": FAMILY_ID,
        "evaluation_dates": list(contract["development_dates"]),
        "decision_dates": list(contract["development_signal_dates"]),
        "reference_identities_by_date": identities,
        "split_execution_dates_by_symbol": _load_split_dates(contract),
        "daily_bars": rows_by_symbol,
        "source_semantics": {
            "provider": "Alpaca historical SIP",
            "feed": "sip",
            "adjustment": "raw",
            "timeframe": "1Day",
            "interpolation": "forbidden",
            "substitution": "forbidden",
        },
    }
    dataset_path = private_root / "dataset.json.gz"
    _write_external(dataset_path, dataset, config)
    completed = datetime.now(timezone.utc)
    relative = str(dataset_path.resolve().relative_to(config.root.resolve()))
    payload = {
        "schema_version": 1,
        "artifact_kind": COLLECTION_KIND,
        "state": COLLECTION_STATE,
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "contract_path": _repo_path(
            PROJECT_ROOT / str(
                strategy_discovery.load_artifact(
                    inspection_path, expected_kind=CONTRACT_INSPECTION_KIND
                )["contract_path"]
            )
        ),
        "contract_sha256": contract["artifact_sha256"],
        "contract_inspection_path": _repo_path(inspection_path),
        "contract_inspection_sha256": strategy_discovery.load_artifact(
            inspection_path, expected_kind=CONTRACT_INSPECTION_KIND
        )["artifact_sha256"],
        "external_relative_path": relative,
        "external_file_sha256": sha256_file(dataset_path),
        "dataset_sha256": canonical_sha256(dataset),
        "evaluation_dates": list(contract["development_dates"]),
        "decision_dates": list(contract["development_signal_dates"]),
        "symbols_requested": len(symbols),
        "symbols_with_rows": sum(bool(rows_by_symbol[symbol]) for symbol in symbols),
        "daily_rows": sum(len(rows) for rows in rows_by_symbol.values()),
        "provider_telemetry": telemetry,
        "collection_started_at": started.isoformat().replace("+00:00", "Z"),
        "collection_completed_at": completed.isoformat().replace("+00:00", "Z"),
        "strategy_metrics_computed": False,
        "confirmation_prices_accessed": False,
        "substitutions": 0,
        "broker_actions": 0,
    }
    return strategy_discovery._write_artifact(
        payload,
        root / "development-collection",
        "residual-replication-development-collection",
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT)
    subparsers = parser.add_subparsers(dest="command", required=True)
    freeze = subparsers.add_parser("freeze-contract")
    freeze.add_argument("--created-at", required=True)
    collect = subparsers.add_parser("collect-development")
    collect.add_argument("inspection", type=Path)
    return parser


def main() -> int:
    args = _parser().parse_args()
    try:
        if args.command == "freeze-contract":
            path, artifact = freeze_contract(
                created_at=args.created_at, root=args.root
            )
        else:
            path, artifact = collect_development(
                args.inspection, root=args.root
            )
        print(
            json.dumps(
                {
                    "written": _repo_path(path),
                    "artifact_sha256": artifact["artifact_sha256"],
                    "state": artifact["state"],
                    "provider_telemetry": artifact.get(
                        "provider_telemetry", {}
                    ),
                    "broker_actions": 0,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    except (
        ResidualReplicationDataError,
        ScannerReplayError,
        strategy_discovery.StrategyDiscoveryError,
        OSError,
        ValueError,
    ) as exc:
        print(
            json.dumps(
                {"error": str(exc), "error_type": type(exc).__name__},
                indent=2,
            )
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

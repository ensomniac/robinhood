"""Freeze, collect, and evaluate cross-sectional momentum at Stage 0."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import io
import json
import math
import os
import shutil
import statistics
import sys
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import equity_gap_continuation_validation as universe_source
import etf_or_momentum_stage0 as common
from historical_store import HistoricalDayStore, sha256_file
from portfolio_tournament import inspect_manifest as inspect_slate
from scanner_replay import (
    ScannerReplayError,
    load_split_actions,
    split_adjustment_factor,
)
from scanner_replay_alpaca import AlpacaBulkBarsClient, AlpacaBulkConfig


PROJECT_ROOT = Path(__file__).resolve().parent
EASTERN = ZoneInfo("America/New_York")
SLATE_PATH = common.SLATE_PATH
CALENDAR_PATH = (
    PROJECT_ROOT
    / "historical_batches"
    / "preentry_structure"
    / "session-calendar-2024-12-through-2026-06.json"
)
SPLIT_PATHS = (
    PROJECT_ROOT / "learning_runs" / "scanner_expansion_v2" / "splits.json.gz",
    PROJECT_ROOT
    / "learning_runs"
    / "development_tranche_v3"
    / "scanner_replay"
    / "splits.json.gz",
)
VARIANT_ID = "cross-sectional-momentum-v1"
VARIANT_ORDINAL = 7
SCHEMA_VERSION = 1
DATASET_ID = "dataset-cross-sectional-momentum-stage0-2026-07-21-v1"
PRIMARY_COST_BPS = 5
STRESS_COST_BPS = (10, 20)
TARGET_DATE_COUNT = 24
TARGET_START = "2025-03-03"
TARGET_END = "2025-12-15"
MINIMUM_TARGET_SPACING = 6
LOOKBACK_SESSIONS = 50
RETURN_LOOKBACK = 20
ATR_PERIOD = 14
MAXIMUM_NAMES = 3
MINIMUM_AVERAGE_DOLLAR_VOLUME = 20_000_000.0
MINIMUM_FREE_BYTES = 20 * 1024**3
OUTPUT_ROOT = PROJECT_ROOT / "strategy_tournament" / "cross_sectional_momentum"
ACTIVATION_ROOT = PROJECT_ROOT / "strategy_tournament" / "activations"
INSPECTION_ROOT = PROJECT_ROOT / "strategy_tournament" / "inspections"
RESULT_ROOT = PROJECT_ROOT / "research_results"
PUBLIC_STATUS = OUTPUT_ROOT / "collection-status.json"


class CrossSectionalMomentumError(RuntimeError):
    """Cross-sectional Stage 0 evidence is incomplete or invalid."""


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")


def _hash(value: Any) -> str:
    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def _load_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise CrossSectionalMomentumError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise CrossSectionalMomentumError(f"{path} must contain an object")
    return value


def _gzip_bytes(value: Any) -> bytes:
    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode="wb", compresslevel=6, mtime=0) as stream:
        stream.write(json.dumps(value, indent=2, sort_keys=True).encode("utf-8"))
        stream.write(b"\n")
    return buffer.getvalue()


def _write_gzip(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_bytes(_gzip_bytes(value))
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()


def _load_gzip(path: Path) -> Any:
    try:
        with gzip.open(path, "rt", encoding="utf-8") as source:
            return json.load(source)
    except (OSError, json.JSONDecodeError) as exc:
        raise CrossSectionalMomentumError(f"cannot read {path}: {exc}") from exc


def _write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()


def _private_root(store: HistoricalDayStore) -> Path:
    return store.root / "_derived" / "cross_sectional_momentum_stage0" / DATASET_ID


def _membership_path(store: HistoricalDayStore) -> Path:
    return _private_root(store) / "frozen-membership.json.gz"


def _daily_path(store: HistoricalDayStore) -> Path:
    return _private_root(store) / "daily-bars.json.gz"


def _input_index_path(store: HistoricalDayStore) -> Path:
    return _private_root(store) / "input-index.json.gz"


def _variant() -> dict[str, Any]:
    inspect_slate(SLATE_PATH)
    matches = [
        row
        for row in _load_json(SLATE_PATH)["variants"]
        if row.get("variant_id") == VARIANT_ID
        and row.get("variant_ordinal") == VARIANT_ORDINAL
    ]
    if len(matches) != 1:
        raise CrossSectionalMomentumError("frozen slate variant is unavailable")
    return dict(matches[0])


def _calendar() -> list[str]:
    try:
        raw = json.loads(CALENDAR_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise CrossSectionalMomentumError("session calendar is unavailable") from exc
    if not isinstance(raw, list):
        raise CrossSectionalMomentumError("session calendar is malformed")
    sessions = [str(row.get("date")) for row in raw if isinstance(row, Mapping)]
    if len(sessions) != len(set(sessions)) or sessions != sorted(sessions):
        raise CrossSectionalMomentumError("session calendar is not unique and ordered")
    return sessions


def _target_dates(source_dates: Sequence[str], sessions: Sequence[str]) -> list[str]:
    positions = {day: index for index, day in enumerate(sessions)}
    pool = [
        day
        for day in sorted(source_dates)
        if TARGET_START <= day <= TARGET_END
        and day in positions
        and positions[day] >= LOOKBACK_SESSIONS
        and positions[day] + 5 < len(sessions)
    ]
    if len(pool) < TARGET_DATE_COUNT:
        raise CrossSectionalMomentumError("source has too few eligible target dates")
    low = positions[pool[0]]
    high = positions[pool[-1]]
    selected: list[str] = []
    for offset in range(TARGET_DATE_COUNT):
        anchor = round(low + offset * (high - low) / (TARGET_DATE_COUNT - 1))
        choices = [
            day
            for day in pool
            if day not in selected
            and positions[day] >= anchor
            and (
                not selected
                or positions[day] - positions[selected[-1]] >= MINIMUM_TARGET_SPACING
            )
        ]
        if not choices:
            raise CrossSectionalMomentumError(
                "target-date spacing cannot satisfy the frozen sample"
            )
        selected.append(min(choices, key=positions.__getitem__))
    return selected


def _membership_graph(store: HistoricalDayStore) -> dict[str, Any]:
    source_dates, details, source_bindings = universe_source._source_graph()
    sessions = _calendar()
    targets = _target_dates(source_dates, sessions)
    position = {day: index for index, day in enumerate(sessions)}
    members_by_date: dict[str, list[dict[str, str]]] = {}
    for day in targets:
        members = []
        seen: set[str] = set()
        for raw in details[day]["evaluations"]:
            if not isinstance(raw, Mapping):
                raise CrossSectionalMomentumError(f"{day}: malformed universe row")
            symbol = str(raw.get("symbol", "")).strip().upper()
            exchange = str(raw.get("primary_exchange", ""))
            instrument_id = str(raw.get("instrument_id", ""))
            if exchange not in {"XNAS", "XNYS"} or not symbol:
                continue
            if symbol in seen:
                raise CrossSectionalMomentumError(f"{day}: duplicate member {symbol}")
            seen.add(symbol)
            members.append(
                {
                    "symbol": symbol,
                    "instrument_id": instrument_id,
                    "primary_exchange": exchange,
                }
            )
        members.sort(key=lambda row: row["symbol"])
        if len(members) < 100:
            raise CrossSectionalMomentumError(
                f"{day}: point-in-time universe is too small"
            )
        members_by_date[day] = members
    windows = {}
    for day in targets:
        index = position[day]
        windows[day] = {
            "prior_sessions": sessions[index - LOOKBACK_SESSIONS : index],
            "entry_and_hold_sessions": sessions[index + 1 : index + 6],
        }
    graph = {
        "schema_version": SCHEMA_VERSION,
        "dataset_id": DATASET_ID,
        "selection_information_cutoff": "DATED_SECURITY_MASTER_BEFORE_TARGET",
        "target_outcomes_observed_or_derived": False,
        "target_dates": targets,
        "windows": windows,
        "members_by_date": members_by_date,
        "source_bindings": source_bindings,
    }
    path = _membership_path(store)
    existing = _load_gzip(path) if path.exists() else None
    if existing is not None and existing != graph:
        raise CrossSectionalMomentumError("private membership graph drifted")
    if existing is None:
        _write_gzip(path, graph)
    return graph


def build_manifest(store: HistoricalDayStore | None = None) -> dict[str, Any]:
    source = store or HistoricalDayStore.from_env()
    graph = _membership_graph(source)
    variant = _variant()
    all_members = {
        row["symbol"] for rows in graph["members_by_date"].values() for row in rows
    }
    member_sessions = sum(len(rows) for rows in graph["members_by_date"].values())
    free_bytes = shutil.disk_usage(source.root).free
    if free_bytes < MINIMUM_FREE_BYTES:
        raise CrossSectionalMomentumError("historical store is below its reserve")
    selection_contract = {
        "universe": "complete dated XNAS/XNYS common-stock membership from the two inspected 2025 scanner-replay security masters",
        "ranking_time_et": "15:45:00 using the close of the completed 15:30-15:44 SIP bar",
        "price_gate": "ranking-time price strictly above 5 dollars",
        "liquidity_gate": "mean split-adjusted close times split-adjusted volume across the prior 20 completed sessions is at least 20 million dollars",
        "return": "ranking-time price divided by split-adjusted close exactly 20 trading sessions earlier minus one",
        "trend_gate": "ranking-time price strictly above the simple mean of 50 prior split-adjusted closes",
        "ranking": "descending 20-session return, then lexical symbol; retain the top ceiling(eligible names times 0.10), then at most the first three",
        "corporate_actions": "raw bars adjusted to target-date share basis using only frozen splits with execution after the observation and on or before target; a split during the five-session hold makes the symbol ineligible",
        "missing_policy": "every frozen member remains in the denominator; incomplete history, ranking prefix, next open, or five-session outcome cannot signal",
    }
    outcome_contract = {
        "entry": "next regular-session open",
        "stop": "entry minus 1.5 times prior-day ATR14; ATR uses the final 14 prior completed target-basis daily true ranges",
        "target": "none",
        "maximum_hold_trading_days": 5,
        "exit": "stop first on daily ambiguity; gap below stop fills at the worse open; otherwise fifth holding-session close",
        "entry_exit_cost_bps_per_side": [PRIMARY_COST_BPS, *STRESS_COST_BPS],
        "planned_risk_r": "cost-adjusted entry minus cost-adjusted planned stop",
    }
    manifest: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "contract_kind": "stage0-multisession-activation",
        "campaign_id": "multi-strategy-portfolio-validation-v1",
        "tournament_wave": 1,
        "slate_manifest_sha256": sha256_file(SLATE_PATH),
        "variant_ordinal": VARIANT_ORDINAL,
        "variant_id": VARIANT_ID,
        "strategy_version": variant["version"],
        "mechanism_family": variant["mechanism_family"],
        "base_rules_hash": variant["rules_hash"],
        "implementation_sha256": sha256_file(Path(__file__).resolve()),
        "claim_scope": "FALSIFICATION_ONLY",
        "outcomes_previously_accessed": False,
        "development_evidence_eligible": False,
        "confirmation_evidence_eligible": False,
        "broker_actions_authorized": False,
        "provider_requests_authorized_before_inspection": False,
        "return_evaluation_authorized_before_input_inspection": False,
        "selection_contract": selection_contract,
        "outcome_contract": outcome_contract,
        "stage0_gate": {
            "minimum_closed_signals": 30,
            "minimum_expectancy_r_exclusive": 0.0,
            "minimum_profit_factor": 1.10,
            "maximum_drawdown_r": 8.0,
            "stress_20bps_total_r_exclusive": 0.0,
            "maximum_rule_violations": 0,
        },
        "denominator": {
            "target_dates": len(graph["target_dates"]),
            "member_symbol_sessions": member_sessions,
            "unique_symbols": len(all_members),
            "maximum_selected_signals": len(graph["target_dates"]) * MAXIMUM_NAMES,
            "minimum_target_spacing_sessions": MINIMUM_TARGET_SPACING,
        },
        "target_dates": graph["target_dates"],
        "private_membership": {
            "location": "LOCAL_HISTORICAL_DATA_ROOT/_derived/cross_sectional_momentum_stage0/"
            f"{DATASET_ID}/frozen-membership.json.gz",
            "content_sha256": _hash(graph),
            "contains_target_returns": False,
        },
        "source_bindings": graph["source_bindings"],
        "calendar": {
            "path": CALENDAR_PATH.relative_to(PROJECT_ROOT).as_posix(),
            "file_sha256": sha256_file(CALENDAR_PATH),
        },
        "split_sources": [
            {
                "path": path.relative_to(PROJECT_ROOT).as_posix(),
                "file_sha256": sha256_file(path),
            }
            for path in SPLIT_PATHS
        ],
        "collection_contract": {
            "provider": "Alpaca historical SIP",
            "endpoint": "https://data.alpaca.markets/v2/stocks/bars",
            "feed": "sip",
            "adjustment": "raw with frozen local target-date split adjustment",
            "asof": "-",
            "timeframe": "1Day",
            "start_session": min(
                row
                for item in graph["windows"].values()
                for row in item["prior_sessions"]
            ),
            "end_session_inclusive": max(
                row
                for item in graph["windows"].values()
                for row in item["entry_and_hold_sessions"]
            ),
            "provider_switching_allowed": False,
            "minimum_free_bytes": MINIMUM_FREE_BYTES,
        },
    }
    manifest["activation_rules_hash"] = _hash(
        {
            "base_rules_hash": manifest["base_rules_hash"],
            "selection_contract": selection_contract,
            "outcome_contract": outcome_contract,
            "stage0_gate": manifest["stage0_gate"],
        }
    )
    manifest["manifest_sha256"] = common._self_hash(manifest, "manifest_sha256")
    return manifest


def _validate_manifest(manifest: Mapping[str, Any]) -> None:
    if manifest.get("manifest_sha256") != common._self_hash(
        manifest, "manifest_sha256"
    ):
        raise CrossSectionalMomentumError("activation manifest content hash is invalid")
    if (
        manifest.get("variant_id") != VARIANT_ID
        or manifest.get("variant_ordinal") != VARIANT_ORDINAL
        or manifest.get("claim_scope") != "FALSIFICATION_ONLY"
    ):
        raise CrossSectionalMomentumError("activation manifest identity is invalid")
    if manifest.get("implementation_sha256") != sha256_file(Path(__file__).resolve()):
        raise CrossSectionalMomentumError("activation implementation drifted")
    if (
        manifest.get("provider_requests_authorized_before_inspection") is not False
        or manifest.get("return_evaluation_authorized_before_input_inspection")
        is not False
        or manifest.get("broker_actions_authorized") is not False
    ):
        raise CrossSectionalMomentumError("activation access gates are not closed")
    denominator = manifest.get("denominator", {})
    if (
        denominator.get("target_dates") != TARGET_DATE_COUNT
        or denominator.get("maximum_selected_signals") != 72
        or denominator.get("unique_symbols", 0) < 100
    ):
        raise CrossSectionalMomentumError("activation denominator is invalid")


def inspect_activation(
    manifest_path: Path, store: HistoricalDayStore | None = None
) -> dict[str, Any]:
    source = store or HistoricalDayStore.from_env()
    recorded = _load_json(manifest_path)
    _validate_manifest(recorded)
    if recorded != build_manifest(source):
        raise CrossSectionalMomentumError("activation manifest does not rebuild")
    graph = _load_gzip(_membership_path(source))
    if _hash(graph) != recorded["private_membership"]["content_sha256"]:
        raise CrossSectionalMomentumError("private membership hash drifted")
    result: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "inspection_kind": "stage0-multisession-activation-inspection",
        "variant_id": VARIANT_ID,
        "manifest_sha256": recorded["manifest_sha256"],
        "manifest_file_sha256": sha256_file(manifest_path),
        "implementation_sha256": recorded["implementation_sha256"],
        "target_dates": len(graph["target_dates"]),
        "member_symbol_sessions": sum(
            len(rows) for rows in graph["members_by_date"].values()
        ),
        "unique_symbols": len(
            {
                row["symbol"]
                for rows in graph["members_by_date"].values()
                for row in rows
            }
        ),
        "minimum_target_spacing_sessions": min(
            _calendar().index(right) - _calendar().index(left)
            for left, right in zip(graph["target_dates"], graph["target_dates"][1:])
        ),
        "provider_requests": 0,
        "broker_actions": 0,
        "returns_computed": 0,
        "collection_authorized": True,
        "return_evaluation_authorized": False,
        "valid": True,
    }
    result["inspection_sha256"] = common._self_hash(result, "inspection_sha256")
    return result


def _validate_activation_inspection(
    manifest_path: Path,
    inspection_path: Path,
    store: HistoricalDayStore,
) -> tuple[dict[str, Any], dict[str, Any]]:
    manifest = _load_json(manifest_path)
    _validate_manifest(manifest)
    recorded = _load_json(inspection_path)
    if recorded.get("inspection_sha256") != common._self_hash(
        recorded, "inspection_sha256"
    ):
        raise CrossSectionalMomentumError("activation inspection hash is invalid")
    if recorded != inspect_activation(manifest_path, store):
        raise CrossSectionalMomentumError("activation inspection does not rebuild")
    if recorded.get("collection_authorized") is not True:
        raise CrossSectionalMomentumError(
            "activation inspection did not authorize collection"
        )
    return manifest, recorded


def _provider_day(timestamp: Any) -> str:
    try:
        return (
            datetime.fromisoformat(str(timestamp).replace("Z", "+00:00"))
            .astimezone(EASTERN)
            .date()
            .isoformat()
        )
    except ValueError as exc:
        raise CrossSectionalMomentumError(
            "provider daily timestamp is invalid"
        ) from exc


def _normalize_daily_rows(
    symbol: str, rows: Sequence[Mapping[str, Any]]
) -> list[dict[str, Any]]:
    output = []
    seen: set[str] = set()
    for raw in rows:
        day = _provider_day(raw.get("t"))
        if day in seen:
            raise CrossSectionalMomentumError(f"{symbol}: duplicate provider day {day}")
        seen.add(day)
        try:
            opened = float(raw["o"])
            high = float(raw["h"])
            low = float(raw["l"])
            close = float(raw["c"])
            volume = int(raw["v"])
        except (KeyError, TypeError, ValueError) as exc:
            raise CrossSectionalMomentumError(
                f"{symbol} {day}: malformed provider daily bar"
            ) from exc
        if (
            min(opened, high, low, close) <= 0
            or low > min(opened, close)
            or high < max(opened, close)
            or volume < 0
        ):
            raise CrossSectionalMomentumError(
                f"{symbol} {day}: invalid provider daily OHLCV"
            )
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
    output.sort(key=lambda row: row["date"])
    return output


def collect_inputs(
    manifest_path: Path,
    activation_inspection_path: Path,
    *,
    env_path: Path,
    store: HistoricalDayStore | None = None,
) -> dict[str, Any]:
    source = store or HistoricalDayStore.from_env(env_path)
    manifest, inspection = _validate_activation_inspection(
        manifest_path, activation_inspection_path, source
    )
    common._require_published((manifest_path, activation_inspection_path))
    graph = _load_gzip(_membership_path(source))
    symbols = sorted(
        {
            row["symbol"]
            for members in graph["members_by_date"].values()
            for row in members
        }
    )
    daily_path = _daily_path(source)
    if daily_path.exists():
        payload = _load_gzip(daily_path)
        if (
            not isinstance(payload, Mapping)
            or payload.get("manifest_sha256") != manifest["manifest_sha256"]
            or payload.get("symbols") != symbols
        ):
            raise CrossSectionalMomentumError("existing daily input payload drifted")
        provider_requests = int(payload.get("provider_requests", 0))
    else:
        free_bytes = shutil.disk_usage(source.root).free
        if free_bytes < MINIMUM_FREE_BYTES:
            raise CrossSectionalMomentumError("historical store is below its reserve")
        config = AlpacaBulkConfig.from_env(env_path)
        start_day = date.fromisoformat(manifest["collection_contract"]["start_session"])
        end_day = date.fromisoformat(
            manifest["collection_contract"]["end_session_inclusive"]
        )
        start = datetime.combine(start_day, time(0, 0), tzinfo=EASTERN)
        end = datetime.combine(
            date.fromordinal(end_day.toordinal() + 1), time(0, 0), tzinfo=EASTERN
        )
        rows_by_symbol: dict[str, list[dict[str, Any]]] = {}
        provider_requests = 0
        with AlpacaBulkBarsClient(config) as client:
            for offset in range(0, len(symbols), config.batch_size):
                batch = symbols[offset : offset + config.batch_size]
                fetched, pages = client.fetch(
                    batch,
                    timeframe="1Day",
                    start=start,
                    end=end,
                )
                provider_requests += pages
                for symbol in batch:
                    rows_by_symbol[symbol] = _normalize_daily_rows(
                        symbol, fetched.get(symbol, [])
                    )
        payload = {
            "schema_version": SCHEMA_VERSION,
            "dataset_id": DATASET_ID,
            "manifest_sha256": manifest["manifest_sha256"],
            "activation_inspection_sha256": inspection["inspection_sha256"],
            "provider": "Alpaca historical SIP",
            "feed": "sip",
            "adjustment": "raw",
            "asof": "-",
            "timeframe": "1Day",
            "symbols": symbols,
            "provider_requests": provider_requests,
            "broker_actions": 0,
            "rows_by_symbol": rows_by_symbol,
        }
        _write_gzip(daily_path, payload)
    rows_by_symbol = payload.get("rows_by_symbol", {})
    if not isinstance(rows_by_symbol, Mapping):
        raise CrossSectionalMomentumError("daily input payload rows are malformed")
    status: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "status_kind": "stage0-multisession-collection",
        "dataset_id": DATASET_ID,
        "variant_id": VARIANT_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "activation_inspection_sha256": inspection["inspection_sha256"],
        "requested_symbols": len(symbols),
        "symbols_with_rows": sum(
            bool(rows_by_symbol.get(symbol)) for symbol in symbols
        ),
        "daily_rows": sum(len(rows_by_symbol.get(symbol, [])) for symbol in symbols),
        "provider_requests": provider_requests,
        "private_payload_present": True,
        "broker_actions": 0,
        "returns_computed": 0,
        "private_daily_file_sha256": sha256_file(daily_path),
        "status": "READY",
    }
    status["status_sha256"] = common._self_hash(status, "status_sha256")
    _write_json(PUBLIC_STATUS, status)
    return status


def _validate_collection(
    manifest: Mapping[str, Any], store: HistoricalDayStore
) -> tuple[dict[str, Any], dict[str, Any]]:
    status = _load_json(PUBLIC_STATUS)
    if status.get("status_sha256") != common._self_hash(status, "status_sha256"):
        raise CrossSectionalMomentumError("collection status hash is invalid")
    daily_path = _daily_path(store)
    if (
        status.get("status") != "READY"
        or status.get("manifest_sha256") != manifest.get("manifest_sha256")
        or not daily_path.is_file()
        or status.get("private_daily_file_sha256") != sha256_file(daily_path)
        or status.get("returns_computed") != 0
        or status.get("broker_actions") != 0
    ):
        raise CrossSectionalMomentumError("collection status is not usable")
    payload = _load_gzip(daily_path)
    if not isinstance(payload, dict) or payload.get("manifest_sha256") != manifest.get(
        "manifest_sha256"
    ):
        raise CrossSectionalMomentumError("private daily payload is invalid")
    return status, payload


def _ranking_prefix(
    store: HistoricalDayStore, symbol: str, day: str
) -> dict[str, Any] | None:
    dataset = store.select_dataset(
        symbol,
        day,
        kind="bars",
        channel="trades",
        timeframe="15m",
        providers=("alpaca",),
        require_complete=True,
        feed="sip",
        adjustment="raw",
    )
    if dataset is None:
        return None
    rows = dataset.get("rows")
    if not isinstance(rows, list) or len(rows) < 25:
        return None
    expected = [
        datetime.combine(date.fromisoformat(day), time(9, 30), tzinfo=EASTERN)
        + timedelta(minutes=15 * index)
        for index in range(25)
    ]
    observed = []
    for row in rows[:25]:
        try:
            stamp = datetime.fromisoformat(str(row["t"]))
            opened = float(row["o"])
            high = float(row["h"])
            low = float(row["l"])
            close = float(row["c"])
            volume = int(row["v"])
        except (KeyError, TypeError, ValueError):
            return None
        if (
            min(opened, high, low, close) <= 0
            or low > min(opened, close)
            or high < max(opened, close)
            or volume < 0
            or row.get("i") is not False
        ):
            return None
        observed.append(stamp)
    if observed != expected:
        return None
    return {
        "dataset_id": dataset["id"],
        "dataset_sha256": dataset["content_sha256"],
        "ranking_close": float(rows[24]["c"]),
    }


def _build_input_index(
    manifest: Mapping[str, Any],
    store: HistoricalDayStore,
    daily_payload: Mapping[str, Any],
) -> dict[str, Any]:
    graph = _load_gzip(_membership_path(store))
    if _hash(graph) != manifest["private_membership"]["content_sha256"]:
        raise CrossSectionalMomentumError("membership graph drifted before inspection")
    raw_by_symbol = daily_payload.get("rows_by_symbol")
    if not isinstance(raw_by_symbol, Mapping):
        raise CrossSectionalMomentumError("daily payload rows are malformed")
    daily_by_symbol = {
        str(symbol): {
            str(row["date"]): dict(row)
            for row in rows
            if isinstance(row, Mapping) and row.get("date")
        }
        for symbol, rows in raw_by_symbol.items()
        if isinstance(rows, list)
    }
    records_by_date: dict[str, list[dict[str, Any]]] = {}
    counts: Counter[str] = Counter()
    for day in graph["target_dates"]:
        required = [
            *graph["windows"][day]["prior_sessions"],
            *graph["windows"][day]["entry_and_hold_sessions"],
        ]
        records = []
        for member in graph["members_by_date"][day]:
            symbol = member["symbol"]
            rows = daily_by_symbol.get(symbol, {})
            missing_dates = [
                required_day for required_day in required if required_day not in rows
            ]
            prefix = _ranking_prefix(store, symbol, day)
            if missing_dates:
                status = "missing_daily_window"
                record: dict[str, Any] = {"symbol": symbol, "status": status}
            elif prefix is None:
                status = "missing_ranking_prefix"
                record = {"symbol": symbol, "status": status}
            else:
                status = "complete"
                record = {"symbol": symbol, "status": status, **prefix}
            counts[status] += 1
            records.append(record)
        records_by_date[day] = records
    return {
        "schema_version": SCHEMA_VERSION,
        "dataset_id": DATASET_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "private_membership_sha256": manifest["private_membership"]["content_sha256"],
        "private_daily_file_sha256": sha256_file(_daily_path(store)),
        "records_by_date": records_by_date,
        "status_counts": dict(sorted(counts.items())),
        "returns_computed": 0,
    }


def inspect_inputs(
    manifest_path: Path,
    activation_inspection_path: Path,
    store: HistoricalDayStore | None = None,
) -> dict[str, Any]:
    source = store or HistoricalDayStore.from_env()
    manifest, activation_inspection = _validate_activation_inspection(
        manifest_path, activation_inspection_path, source
    )
    common._require_published(
        (manifest_path, activation_inspection_path, PUBLIC_STATUS)
    )
    collection, daily_payload = _validate_collection(manifest, source)
    index = _build_input_index(manifest, source, daily_payload)
    index_path = _input_index_path(source)
    if index_path.exists():
        if _load_gzip(index_path) != index:
            raise CrossSectionalMomentumError("private input index drifted")
    else:
        _write_gzip(index_path, index)
    status_counts = index["status_counts"]
    result: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "inspection_kind": "stage0-multisession-input-inspection",
        "variant_id": VARIANT_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "manifest_file_sha256": sha256_file(manifest_path),
        "activation_inspection_sha256": activation_inspection["inspection_sha256"],
        "collection_status_sha256": collection["status_sha256"],
        "collection_status_file_sha256": sha256_file(PUBLIC_STATUS),
        "private_daily_file_sha256": sha256_file(_daily_path(source)),
        "private_input_index_sha256": _hash(index),
        "target_dates": manifest["denominator"]["target_dates"],
        "member_symbol_sessions": manifest["denominator"]["member_symbol_sessions"],
        "complete_symbol_sessions": int(status_counts.get("complete", 0)),
        "missing_daily_window_symbol_sessions": int(
            status_counts.get("missing_daily_window", 0)
        ),
        "missing_ranking_prefix_symbol_sessions": int(
            status_counts.get("missing_ranking_prefix", 0)
        ),
        "provider_requests_during_inspection": 0,
        "broker_actions": 0,
        "returns_computed": 0,
        "return_evaluation_authorized": True,
        "valid": True,
    }
    result["inspection_sha256"] = common._self_hash(result, "inspection_sha256")
    return result


def _validate_input_inspection(
    manifest_path: Path,
    activation_inspection_path: Path,
    input_inspection_path: Path,
    store: HistoricalDayStore,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], dict[str, Any]]:
    manifest = _load_json(manifest_path)
    _validate_manifest(manifest)
    recorded = _load_json(input_inspection_path)
    if recorded.get("inspection_sha256") != common._self_hash(
        recorded, "inspection_sha256"
    ):
        raise CrossSectionalMomentumError("input inspection hash is invalid")
    if recorded != inspect_inputs(manifest_path, activation_inspection_path, store):
        raise CrossSectionalMomentumError("input inspection does not rebuild")
    if recorded.get("return_evaluation_authorized") is not True:
        raise CrossSectionalMomentumError("input inspection did not authorize returns")
    _, daily_payload = _validate_collection(manifest, store)
    index = _load_gzip(_input_index_path(store))
    if _hash(index) != recorded["private_input_index_sha256"]:
        raise CrossSectionalMomentumError("private input index hash drifted")
    graph = _load_gzip(_membership_path(store))
    return manifest, recorded, daily_payload, {"graph": graph, "index": index}


def _merged_split_actions() -> dict[str, list[dict[str, Any]]]:
    merged: dict[str, list[dict[str, Any]]] = defaultdict(list)
    seen: set[tuple[Any, ...]] = set()
    for path in SPLIT_PATHS:
        for symbol, rows in load_split_actions(path).items():
            for row in rows:
                identity = (
                    symbol,
                    row["execution_date"],
                    float(row["split_from"]),
                    float(row["split_to"]),
                    row.get("id"),
                )
                if identity in seen:
                    continue
                seen.add(identity)
                merged[symbol].append(dict(row))
    for rows in merged.values():
        rows.sort(key=lambda row: row["execution_date"])
    return dict(merged)


def _adjusted_daily(
    symbol: str,
    observed_day: str,
    target_day: str,
    row: Mapping[str, Any],
    splits: Mapping[str, Sequence[Mapping[str, Any]]],
) -> dict[str, float]:
    factor = split_adjustment_factor(symbol, observed_day, target_day, splits)
    return {
        "open": float(row["open"]) * factor,
        "high": float(row["high"]) * factor,
        "low": float(row["low"]) * factor,
        "close": float(row["close"]) * factor,
        "volume": float(row["volume"]) / factor,
    }


def _ranking_candidate(
    *,
    day: str,
    member: Mapping[str, Any],
    input_record: Mapping[str, Any],
    window: Mapping[str, Any],
    daily_by_symbol: Mapping[str, Mapping[str, Mapping[str, Any]]],
    splits: Mapping[str, Sequence[Mapping[str, Any]]],
) -> dict[str, Any]:
    symbol = str(member["symbol"])
    if input_record.get("status") != "complete":
        return {"symbol": symbol, "status": str(input_record.get("status"))}
    ranking_close = float(input_record["ranking_close"])
    if ranking_close <= 5:
        return {"symbol": symbol, "status": "ranking_price_not_above_5"}
    prior_dates = list(window["prior_sessions"])
    hold_dates = list(window["entry_and_hold_sessions"])
    for event in splits.get(symbol, []):
        if event["execution_date"].isoformat() in hold_dates:
            return {"symbol": symbol, "status": "split_during_hold"}
    raw_rows = daily_by_symbol[symbol]
    adjusted = {
        prior_day: _adjusted_daily(symbol, prior_day, day, raw_rows[prior_day], splits)
        for prior_day in prior_dates
    }
    average_dollar_volume = statistics.fmean(
        adjusted[prior_day]["close"] * adjusted[prior_day]["volume"]
        for prior_day in prior_dates[-RETURN_LOOKBACK:]
    )
    if average_dollar_volume < MINIMUM_AVERAGE_DOLLAR_VOLUME:
        return {"symbol": symbol, "status": "below_liquidity_gate"}
    sma50 = statistics.fmean(adjusted[prior_day]["close"] for prior_day in prior_dates)
    if ranking_close <= sma50:
        return {"symbol": symbol, "status": "not_above_sma50"}
    return_fraction = (
        ranking_close / adjusted[prior_dates[-RETURN_LOOKBACK]]["close"] - 1
    )
    atr_ranges = []
    for offset in range(len(prior_dates) - ATR_PERIOD, len(prior_dates)):
        prior_day = prior_dates[offset]
        previous_day = prior_dates[offset - 1]
        row = adjusted[prior_day]
        previous_close = adjusted[previous_day]["close"]
        atr_ranges.append(
            max(
                row["high"] - row["low"],
                abs(row["high"] - previous_close),
                abs(row["low"] - previous_close),
            )
        )
    atr14 = statistics.fmean(atr_ranges)
    if atr14 <= 0:
        return {"symbol": symbol, "status": "nonpositive_atr14"}
    return {
        "symbol": symbol,
        "status": "eligible",
        "return_fraction": return_fraction,
        "average_dollar_volume": average_dollar_volume,
        "sma50": sma50,
        "atr14": atr14,
        "ranking_close": ranking_close,
    }


def _trade_outcome(
    candidate: Mapping[str, Any],
    hold_dates: Sequence[str],
    daily_rows: Mapping[str, Mapping[str, Any]],
    cost_bps: int,
) -> dict[str, Any]:
    entry_day = hold_dates[0]
    raw_entry = float(daily_rows[entry_day]["open"])
    stop = raw_entry - 1.5 * float(candidate["atr14"])
    if stop <= 0 or raw_entry <= stop:
        raise CrossSectionalMomentumError(
            "planned stop is not positive and below entry"
        )
    exit_price: float | None = None
    exit_reason = ""
    exit_day = ""
    for offset, day in enumerate(hold_dates):
        row = daily_rows[day]
        opened = float(row["open"])
        if opened <= stop:
            exit_price = opened
            exit_reason = "stop_gap" if offset > 0 else "stop"
        elif float(row["low"]) <= stop:
            exit_price = stop
            exit_reason = "stop"
        elif offset == len(hold_dates) - 1:
            exit_price = float(row["close"])
            exit_reason = "maximum_hold_close"
        if exit_price is not None:
            exit_day = day
            break
    if exit_price is None:
        raise CrossSectionalMomentumError("trade did not resolve within five sessions")
    cost = cost_bps / 10_000
    entry_fill = raw_entry * (1 + cost)
    exit_fill = exit_price * (1 - cost)
    stop_fill = stop * (1 - cost)
    planned_risk = entry_fill - stop_fill
    if planned_risk <= 0:
        raise CrossSectionalMomentumError("cost-adjusted planned risk is not positive")
    return {
        "entry_date": entry_day,
        "exit_date": exit_day,
        "exit_reason": exit_reason,
        "stop_executed": exit_reason.startswith("stop"),
        "net_r": (exit_fill - entry_fill) / planned_risk,
    }


def build_result(
    manifest_path: Path,
    activation_inspection_path: Path,
    input_inspection_path: Path,
    store: HistoricalDayStore | None = None,
    *,
    require_published: bool = True,
) -> dict[str, Any]:
    source = store or HistoricalDayStore.from_env()
    manifest, input_inspection, daily_payload, private = _validate_input_inspection(
        manifest_path,
        activation_inspection_path,
        input_inspection_path,
        source,
    )
    if require_published:
        common._require_published(
            (manifest_path, activation_inspection_path, input_inspection_path)
        )
    graph = private["graph"]
    index = private["index"]
    raw_by_symbol = daily_payload["rows_by_symbol"]
    daily_by_symbol = {
        str(symbol): {str(row["date"]): row for row in rows}
        for symbol, rows in raw_by_symbol.items()
    }
    splits = _merged_split_actions()
    dispositions: Counter[str] = Counter()
    records: list[dict[str, Any]] = []
    no_trade_dates = 0
    selected_by_date: dict[str, list[str]] = {}
    for day in graph["target_dates"]:
        members = graph["members_by_date"][day]
        input_records = {row["symbol"]: row for row in index["records_by_date"][day]}
        ranked = [
            _ranking_candidate(
                day=day,
                member=member,
                input_record=input_records[member["symbol"]],
                window=graph["windows"][day],
                daily_by_symbol=daily_by_symbol,
                splits=splits,
            )
            for member in members
        ]
        dispositions.update(row["status"] for row in ranked)
        eligible = [row for row in ranked if row["status"] == "eligible"]
        eligible.sort(key=lambda row: (-float(row["return_fraction"]), row["symbol"]))
        top_decile_count = math.ceil(len(eligible) * 0.10)
        top_decile = eligible[:top_decile_count]
        selected = top_decile[:MAXIMUM_NAMES]
        dispositions["not_selected_outside_top_decile"] += len(eligible) - len(
            top_decile
        )
        dispositions["not_selected_maximum_names"] += len(top_decile) - len(selected)
        dispositions["selected"] += len(selected)
        selected_by_date[day] = [str(row["symbol"]) for row in selected]
        if not selected:
            no_trade_dates += 1
            continue
        hold_dates = graph["windows"][day]["entry_and_hold_sessions"]
        for candidate in selected:
            symbol = str(candidate["symbol"])
            outcomes = {
                str(cost): _trade_outcome(
                    candidate,
                    hold_dates,
                    daily_by_symbol[symbol],
                    cost,
                )
                for cost in (PRIMARY_COST_BPS, *STRESS_COST_BPS)
            }
            primary = outcomes[str(PRIMARY_COST_BPS)]
            records.append(
                {
                    "ranking_date": day,
                    "symbol": symbol,
                    "rank": selected.index(candidate) + 1,
                    "return_fraction": candidate["return_fraction"],
                    "entry_date": primary["entry_date"],
                    "exit_date": primary["exit_date"],
                    "exit_reason": primary["exit_reason"],
                    "stop_executed": primary["stop_executed"],
                    "net_r": primary["net_r"],
                    "stress_10bps_r": outcomes["10"]["net_r"],
                    "stress_20bps_r": outcomes["20"]["net_r"],
                }
            )
    values = [float(row["net_r"]) for row in records]
    stress_10 = [float(row["stress_10bps_r"]) for row in records]
    stress_20 = [float(row["stress_20bps_r"]) for row in records]
    primary_metrics = common._metrics(values)
    stress_metrics = {
        "10": common._metrics(stress_10),
        "20": common._metrics(stress_20),
    }
    gate = manifest["stage0_gate"]
    blockers = []
    if len(records) < int(gate["minimum_closed_signals"]):
        blockers.append("closed signals are below the Stage 0 minimum")
    if primary_metrics["expectancy_r"] is None or primary_metrics["expectancy_r"] <= 0:
        blockers.append("primary expectancy is not positive")
    if not primary_metrics["profit_factor_infinite"] and (
        primary_metrics["profit_factor"] is None
        or primary_metrics["profit_factor"] < float(gate["minimum_profit_factor"])
    ):
        blockers.append("primary profit factor is below the Stage 0 minimum")
    if primary_metrics["maximum_drawdown_r"] > float(gate["maximum_drawdown_r"]):
        blockers.append("primary drawdown exceeds the Stage 0 maximum")
    if stress_metrics["20"]["total_r"] <= 0:
        blockers.append("20 bps-per-side total R is not positive")
    result: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "result_kind": "stage0-falsification",
        "variant_id": VARIANT_ID,
        "strategy_version": manifest["strategy_version"],
        "mechanism_family": manifest["mechanism_family"],
        "base_rules_hash": manifest["base_rules_hash"],
        "activation_rules_hash": manifest["activation_rules_hash"],
        "manifest_sha256": manifest["manifest_sha256"],
        "input_inspection_sha256": input_inspection["inspection_sha256"],
        "implementation_sha256": manifest["implementation_sha256"],
        "claim_scope": "FALSIFICATION_ONLY",
        "development_evidence_eligible": False,
        "confirmation_evidence_eligible": False,
        "provider_requests_during_evaluation": 0,
        "broker_actions": 0,
        "denominator": {
            "target_dates": manifest["denominator"]["target_dates"],
            "member_symbol_sessions": manifest["denominator"]["member_symbol_sessions"],
            "complete_symbol_sessions": input_inspection["complete_symbol_sessions"],
            "closed_signals": len(records),
            "no_trade_dates": no_trade_dates,
            "rule_violations": 0,
        },
        "selected_by_date": selected_by_date,
        "disposition_counts": dict(sorted(dispositions.items())),
        "primary_5bps": primary_metrics,
        "stress": stress_metrics,
        "stage0_survived": not blockers,
        "stage0_blockers": blockers,
        "next_action": (
            "freeze a representative development corpus without changing rules"
            if not blockers
            else "retire this exact variant and advance to the next frozen mechanism"
        ),
        "maturity_effect": "NONE",
        "records": records,
    }
    result["result_sha256"] = common._self_hash(result, "result_sha256")
    return result


def inspect_result(
    manifest_path: Path,
    activation_inspection_path: Path,
    input_inspection_path: Path,
    result_path: Path,
    store: HistoricalDayStore | None = None,
) -> dict[str, Any]:
    recorded = _load_json(result_path)
    if recorded.get("result_sha256") != common._self_hash(recorded, "result_sha256"):
        raise CrossSectionalMomentumError("Stage 0 result hash is invalid")
    rebuilt = build_result(
        manifest_path,
        activation_inspection_path,
        input_inspection_path,
        store,
        require_published=False,
    )
    if recorded != rebuilt:
        raise CrossSectionalMomentumError("Stage 0 result does not rebuild")
    result: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "inspection_kind": "stage0-result-inspection",
        "variant_id": VARIANT_ID,
        "manifest_sha256": recorded["manifest_sha256"],
        "input_inspection_sha256": recorded["input_inspection_sha256"],
        "result_sha256": recorded["result_sha256"],
        "result_file_sha256": sha256_file(result_path),
        "closed_signals": recorded["denominator"]["closed_signals"],
        "stage0_survived": recorded["stage0_survived"],
        "maturity_effect": "NONE",
        "provider_requests": 0,
        "broker_actions": 0,
        "valid": True,
    }
    result["inspection_sha256"] = common._self_hash(result, "inspection_sha256")
    return result


def _publish(value: Mapping[str, Any], path: Path) -> None:
    if "inspection_kind" in value:
        identity = str(value.get("inspection_sha256", ""))
    elif "result_kind" in value:
        identity = str(value.get("result_sha256", ""))
    else:
        identity = str(value.get("manifest_sha256", ""))
    if not identity or not path.name.endswith(f"-{identity}.json"):
        raise CrossSectionalMomentumError("output filename must end with content hash")
    resolved = path.resolve()
    if resolved.parent not in {
        ACTIVATION_ROOT.resolve(),
        INSPECTION_ROOT.resolve(),
        RESULT_ROOT.resolve(),
    }:
        raise CrossSectionalMomentumError("output path is outside evidence roots")
    _write_json(resolved, value)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    freeze = commands.add_parser("freeze")
    freeze.add_argument("--output", type=Path)
    inspect = commands.add_parser("inspect")
    inspect.add_argument("manifest", type=Path)
    inspect.add_argument("--output", type=Path)
    collect = commands.add_parser("collect")
    collect.add_argument("manifest", type=Path)
    collect.add_argument("activation_inspection", type=Path)
    collect.add_argument("--env-file", type=Path, default=PROJECT_ROOT / ".env")
    inspect_data = commands.add_parser("inspect-inputs")
    inspect_data.add_argument("manifest", type=Path)
    inspect_data.add_argument("activation_inspection", type=Path)
    inspect_data.add_argument("--output", type=Path)
    evaluate = commands.add_parser("evaluate")
    evaluate.add_argument("manifest", type=Path)
    evaluate.add_argument("activation_inspection", type=Path)
    evaluate.add_argument("input_inspection", type=Path)
    evaluate.add_argument("--output", type=Path)
    inspect_outcome = commands.add_parser("inspect-result")
    inspect_outcome.add_argument("manifest", type=Path)
    inspect_outcome.add_argument("activation_inspection", type=Path)
    inspect_outcome.add_argument("input_inspection", type=Path)
    inspect_outcome.add_argument("result", type=Path)
    inspect_outcome.add_argument("--output", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "freeze":
            value = build_manifest()
        elif args.command == "inspect":
            value = inspect_activation(args.manifest)
        elif args.command == "collect":
            value = collect_inputs(
                args.manifest,
                args.activation_inspection,
                env_path=args.env_file,
            )
        elif args.command == "inspect-inputs":
            value = inspect_inputs(args.manifest, args.activation_inspection)
        elif args.command == "evaluate":
            value = build_result(
                args.manifest,
                args.activation_inspection,
                args.input_inspection,
            )
        else:
            value = inspect_result(
                args.manifest,
                args.activation_inspection,
                args.input_inspection,
                args.result,
            )
        if args.command != "collect" and args.output:
            _publish(value, args.output)
    except (
        CrossSectionalMomentumError,
        common.EtfOrbStage0Error,
        ScannerReplayError,
        OSError,
    ) as exc:
        print(json.dumps({"status": "BLOCKED", "error": str(exc)}), file=sys.stderr)
        return 2
    print(json.dumps(value, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

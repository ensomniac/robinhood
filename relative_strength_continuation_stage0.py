"""Freeze and falsify intraday relative-strength continuation at Stage 0."""

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

import cross_sectional_momentum_stage0 as cross
import equity_gap_continuation_validation as universe_source
import etf_or_momentum_stage0 as common
from historical_store import HistoricalDayStore, sha256_file
from portfolio_tournament import inspect_manifest as inspect_slate
from scanner_replay import load_split_actions
from scanner_replay_alpaca import AlpacaBulkBarsClient, AlpacaBulkConfig


PROJECT_ROOT = Path(__file__).resolve().parent
EASTERN = ZoneInfo("America/New_York")
SLATE_PATH = common.SLATE_PATH
VARIANT_ID = "relative-strength-continuation-v1"
VARIANT_ORDINAL = 5
SCHEMA_VERSION = 1
DATASET_ID = "dataset-relative-strength-continuation-stage0-2026-07-21-v1"
TARGET_DATE_COUNT = 80
PRIMARY_COST_BPS = 5
STRESS_COST_BPS = (10, 20)
MINIMUM_FREE_BYTES = 20 * 1024**3
CROSS_STATUS = (
    PROJECT_ROOT
    / "strategy_tournament"
    / "cross_sectional_momentum"
    / "collection-status.json"
)
CROSS_PRIVATE_RELATIVE = (
    "_derived/cross_sectional_momentum_stage0/"
    "dataset-cross-sectional-momentum-stage0-2026-07-21-v1/daily-bars.json.gz"
)
CROSS_ACTIVATION = (
    PROJECT_ROOT
    / "strategy_tournament"
    / "activations"
    / "cross-sectional-momentum-v1-a8970ad2bfb0045f54404da985178686ec7cd0068c3a6a23f59165c7e415520a.json"
)
SPLIT_PATHS = cross.SPLIT_PATHS
ACTIVATION_ROOT = PROJECT_ROOT / "strategy_tournament" / "activations"
INSPECTION_ROOT = PROJECT_ROOT / "strategy_tournament" / "inspections"
RESULT_ROOT = PROJECT_ROOT / "research_results"
OUTPUT_ROOT = PROJECT_ROOT / "strategy_tournament" / "relative_strength_continuation"
PREFIX_STATUS = OUTPUT_ROOT / "prefix-status.json"
BENCHMARK_STATUS = OUTPUT_ROOT / "benchmark-prior-close-status.json"
OUTCOME_STATUS = OUTPUT_ROOT / "outcome-status.json"
PREFIX_SOURCE_MANIFEST_SHA256 = (
    "a22173e0b7ee21a9a5f37e4b7bc84c8863e9733d6c6d670313662a6e2b0a55cb"
)
PREFIX_STATUS_SHA256 = (
    "54d610efbb744c5054bd41c5bef4ff781424e83941cf4847941e694594457570"
)
PREFIX_PROVIDER_REQUESTS = 831


class RelativeStrengthError(RuntimeError):
    """The relative-strength Stage 0 evidence graph is invalid or incomplete."""


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode()


def _hash(value: Any) -> str:
    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def _load_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RelativeStrengthError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise RelativeStrengthError(f"{path} must contain an object")
    return value


def _gzip_bytes(value: Any) -> bytes:
    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode="wb", compresslevel=6, mtime=0) as stream:
        stream.write(json.dumps(value, indent=2, sort_keys=True).encode())
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
        raise RelativeStrengthError(f"cannot read {path}: {exc}") from exc


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
    return store.root / "_derived" / "relative_strength_continuation_stage0" / DATASET_ID


def _membership_path(store: HistoricalDayStore) -> Path:
    return _private_root(store) / "frozen-membership.json.gz"


def _prefix_path(store: HistoricalDayStore, day: str) -> Path:
    return _private_root(store) / "prefix" / f"{day}.json.gz"


def _candidate_path(store: HistoricalDayStore) -> Path:
    return _private_root(store) / "frozen-ranked-candidates.json.gz"


def _benchmark_path(store: HistoricalDayStore) -> Path:
    return _private_root(store) / "spy-prior-closes.json.gz"


def _outcome_path(store: HistoricalDayStore, day: str) -> Path:
    return _private_root(store) / "outcomes" / f"{day}.json.gz"


def _variant() -> dict[str, Any]:
    inspect_slate(SLATE_PATH)
    rows = [
        row
        for row in _load_json(SLATE_PATH)["variants"]
        if row.get("variant_id") == VARIANT_ID
        and row.get("variant_ordinal") == VARIANT_ORDINAL
    ]
    if len(rows) != 1:
        raise RelativeStrengthError("frozen slate variant is unavailable")
    return dict(rows[0])


def _target_dates(source_dates: Sequence[str]) -> list[str]:
    excluded = set(_load_json(CROSS_ACTIVATION)["target_dates"])
    pool = [
        day
        for day in sorted(source_dates)
        if "2025-01-15" <= day <= "2025-12-15" and day not in excluded
    ]
    if len(pool) < TARGET_DATE_COUNT:
        raise RelativeStrengthError("not enough outcome-distinct source dates")
    selected = [
        pool[round(index * (len(pool) - 1) / (TARGET_DATE_COUNT - 1))]
        for index in range(TARGET_DATE_COUNT)
    ]
    if len(set(selected)) != TARGET_DATE_COUNT:
        raise RelativeStrengthError("target-date interpolation produced duplicates")
    return selected


def _membership_graph(store: HistoricalDayStore) -> dict[str, Any]:
    source_dates, details, bindings = universe_source._source_graph()
    targets = _target_dates(source_dates)
    sessions = cross._calendar()
    positions = {day: index for index, day in enumerate(sessions)}
    members_by_date: dict[str, list[dict[str, str]]] = {}
    prior_sessions: dict[str, str] = {}
    for day in targets:
        if day not in positions or positions[day] == 0:
            raise RelativeStrengthError(f"{day}: prior exchange session unavailable")
        prior_sessions[day] = sessions[positions[day] - 1]
        members = []
        seen: set[str] = set()
        for raw in details[day]["evaluations"]:
            symbol = str(raw.get("symbol", "")).strip().upper()
            exchange = str(raw.get("primary_exchange", ""))
            instrument_id = str(raw.get("instrument_id", ""))
            if exchange not in {"XNAS", "XNYS"} or not symbol:
                continue
            if symbol in seen:
                raise RelativeStrengthError(f"{day}: duplicate member {symbol}")
            seen.add(symbol)
            members.append(
                {
                    "symbol": symbol,
                    "instrument_id": instrument_id,
                    "primary_exchange": exchange,
                }
            )
        members.sort(key=lambda row: row["symbol"])
        if len(members) < 4_000:
            raise RelativeStrengthError(f"{day}: point-in-time universe is too small")
        members_by_date[day] = members
    graph = {
        "schema_version": SCHEMA_VERSION,
        "dataset_id": DATASET_ID,
        "selection_information_cutoff": "09:59:59_ET_ON_TARGET_DATE",
        "target_outcomes_observed_or_derived": False,
        "target_dates": targets,
        "prior_sessions": prior_sessions,
        "members_by_date": members_by_date,
        "source_bindings": bindings,
        "excluded_prior_variant_dates": sorted(
            set(_load_json(CROSS_ACTIVATION)["target_dates"])
        ),
    }
    path = _membership_path(store)
    if path.exists() and _load_gzip(path) != graph:
        raise RelativeStrengthError("private membership graph drifted")
    if not path.exists():
        _write_gzip(path, graph)
    return graph


def _daily_source(store: HistoricalDayStore) -> tuple[dict[str, Any], dict[str, Any]]:
    status = _load_json(CROSS_STATUS)
    path = store.root / CROSS_PRIVATE_RELATIVE
    if (
        status.get("status_sha256") != common._self_hash(status, "status_sha256")
        or status.get("status") != "READY"
        or status.get("private_daily_file_sha256") != sha256_file(path)
        or status.get("returns_computed") != 0
    ):
        raise RelativeStrengthError("bound prior-close source is unusable")
    payload = _load_gzip(path)
    if payload.get("manifest_sha256") != status.get("manifest_sha256"):
        raise RelativeStrengthError("bound prior-close payload identity drifted")
    return status, payload


def build_manifest(store: HistoricalDayStore | None = None) -> dict[str, Any]:
    source = store or HistoricalDayStore.from_env()
    if shutil.disk_usage(source.root).free < MINIMUM_FREE_BYTES:
        raise RelativeStrengthError("historical store is below its reserve")
    graph = _membership_graph(source)
    daily_status, _ = _daily_source(source)
    variant = _variant()
    member_sessions = sum(len(rows) for rows in graph["members_by_date"].values())
    symbols = {
        row["symbol"]
        for rows in graph["members_by_date"].values()
        for row in rows
    }
    selection_contract = {
        "universe": "complete dated XNAS/XNYS common-stock membership from the two inspected 2025 scanner-replay security masters",
        "target_dates": "80 deterministic evenly spaced source dates excluding every cross-sectional-momentum-v1 Stage 0 date",
        "ranking_time_et": "09:59 close using exactly 30 completed regular-session one-minute SIP bars",
        "benchmark": "SPY 09:59 close divided by its prior-session raw close minus one",
        "price_gate": "09:59 close strictly above 5 dollars",
        "relative_strength": "security 09:59 return from prior-session raw close minus SPY return, minimum 0.02",
        "ranking": "descending relative-strength fraction, then lexical symbol; retain every qualifying leader",
        "corporate_actions": "a frozen split executing on the target session makes that symbol ineligible",
        "missing_policy": "every dated member remains in the denominator; missing prior close or any 09:30-09:59 bar cannot qualify",
    }
    signal_contract = {
        "window_et": "completed bars timestamped 10:00 through 14:30 inclusive",
        "trigger": "completed close strictly above the prior session high-of-day and completed session VWAP",
        "volume": "trigger-minute volume at least 1.5 times the mean of the previous 20 completed one-minute bars",
        "priority": "earliest executable trigger, then better frozen 09:59 relative-strength rank, then lexical symbol",
        "maximum_entries_per_strategy_per_day": 1,
        "missed_entry": "if the next minute bar is absent or its open is not strictly above the frozen stop, skip that trigger and consider the next ordered trigger",
    }
    outcome_contract = {
        "entry": "next observed one-minute open plus adverse per-side cost",
        "stop": "minimum low across the trigger bar and four immediately preceding completed bars",
        "target": "two times raw planned risk above raw entry",
        "same_interval_ambiguity": "stop_first",
        "force_flat": "15:50 one-minute open after evaluating bars only through 15:49",
        "entry_exit_cost_bps_per_side": [PRIMARY_COST_BPS, *STRESS_COST_BPS],
        "maximum_holding_trading_days": 1,
    }
    value: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "contract_kind": "stage0-intraday-activation",
        "campaign_id": "multi-strategy-portfolio-validation-v1",
        "tournament_wave": 1,
        "slate_manifest_sha256": sha256_file(SLATE_PATH),
        "variant_id": VARIANT_ID,
        "variant_ordinal": VARIANT_ORDINAL,
        "strategy_version": variant["version"],
        "mechanism_family": variant["mechanism_family"],
        "base_rules_hash": variant["rules_hash"],
        "implementation_sha256": sha256_file(Path(__file__).resolve()),
        "claim_scope": "FALSIFICATION_ONLY",
        "development_evidence_eligible": False,
        "confirmation_evidence_eligible": False,
        "broker_actions_authorized": False,
        "provider_requests_authorized_before_inspection": False,
        "target_outcome_requests_authorized_before_input_inspection": False,
        "supersedes_manifest_sha256": PREFIX_SOURCE_MANIFEST_SHA256,
        "collection_lineage": {
            "prefix_source_manifest_sha256": PREFIX_SOURCE_MANIFEST_SHA256,
            "prefix_status_sha256": PREFIX_STATUS_SHA256,
            "prefix_provider_requests": PREFIX_PROVIDER_REQUESTS,
            "prefix_selection_inputs_accessed": True,
            "prefix_ranks_computed": False,
            "target_outcomes_accessed": False,
            "failure": "the bound common-stock daily corpus omitted SPY, so prefix inspection failed before ranking",
        },
        "related_prior_trial": {
            "variant_id": "cross-sectional-momentum-v1",
            "activation_path": CROSS_ACTIVATION.relative_to(PROJECT_ROOT).as_posix(),
            "activation_file_sha256": sha256_file(CROSS_ACTIVATION),
            "all_target_dates_excluded": True,
        },
        "selection_contract": selection_contract,
        "signal_contract": signal_contract,
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
            "unique_symbols": len(symbols),
            "maximum_selected_signals": len(graph["target_dates"]),
        },
        "target_dates": graph["target_dates"],
        "private_membership": {
            "location": "LOCAL_HISTORICAL_DATA_ROOT/_derived/relative_strength_continuation_stage0/"
            f"{DATASET_ID}/frozen-membership.json.gz",
            "content_sha256": _hash(graph),
            "contains_target_returns": False,
        },
        "source_bindings": graph["source_bindings"],
        "prior_close_source": {
            "status_path": CROSS_STATUS.relative_to(PROJECT_ROOT).as_posix(),
            "status_file_sha256": sha256_file(CROSS_STATUS),
            "status_sha256": daily_status["status_sha256"],
            "private_file_sha256": daily_status["private_daily_file_sha256"],
            "usage": "common-stock prior-session close only",
        },
        "benchmark_prior_close_collection": {
            "symbol": "SPY",
            "timeframe": "1Day",
            "one_exact_prior-session half-open request per target date": True,
            "target_date_bars_authorized": False,
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
            "feed": "sip",
            "adjustment": "raw",
            "selection_prefix": "09:30 inclusive through 10:00 exclusive for every frozen member and SPY",
            "target_outcomes": "complete regular session only for every frozen 09:59 qualifying leader",
            "provider_boundary_policy": "ignore same-date bars outside each explicitly requested half-open regular-session window",
            "provider_switching_allowed": False,
            "minimum_free_bytes": MINIMUM_FREE_BYTES,
        },
    }
    value["activation_rules_hash"] = _hash(
        {
            "base_rules_hash": value["base_rules_hash"],
            "selection_contract": selection_contract,
            "signal_contract": signal_contract,
            "outcome_contract": outcome_contract,
            "stage0_gate": value["stage0_gate"],
        }
    )
    value["manifest_sha256"] = common._self_hash(value, "manifest_sha256")
    return value


def _validate_manifest(manifest: Mapping[str, Any]) -> None:
    if manifest.get("manifest_sha256") != common._self_hash(
        manifest, "manifest_sha256"
    ):
        raise RelativeStrengthError("activation manifest hash is invalid")
    if (
        manifest.get("variant_id") != VARIANT_ID
        or manifest.get("variant_ordinal") != VARIANT_ORDINAL
        or manifest.get("implementation_sha256")
        != sha256_file(Path(__file__).resolve())
        or manifest.get("claim_scope") != "FALSIFICATION_ONLY"
    ):
        raise RelativeStrengthError("activation identity or implementation drifted")
    if (
        manifest.get("provider_requests_authorized_before_inspection") is not False
        or manifest.get("target_outcome_requests_authorized_before_input_inspection")
        is not False
        or manifest.get("broker_actions_authorized") is not False
    ):
        raise RelativeStrengthError("activation access gates are not closed")
    denominator = manifest.get("denominator", {})
    if (
        denominator.get("target_dates") != TARGET_DATE_COUNT
        or denominator.get("maximum_selected_signals") != TARGET_DATE_COUNT
    ):
        raise RelativeStrengthError("activation denominator drifted")


def inspect_activation(
    manifest_path: Path, store: HistoricalDayStore | None = None
) -> dict[str, Any]:
    source = store or HistoricalDayStore.from_env()
    common._require_published((manifest_path,))
    recorded = _load_json(manifest_path)
    _validate_manifest(recorded)
    if recorded != build_manifest(source):
        raise RelativeStrengthError("activation does not rebuild")
    graph = _load_gzip(_membership_path(source))
    value: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "inspection_kind": "stage0-intraday-activation-inspection",
        "variant_id": VARIANT_ID,
        "manifest_sha256": recorded["manifest_sha256"],
        "manifest_file_sha256": sha256_file(manifest_path),
        "implementation_sha256": recorded["implementation_sha256"],
        "activation_rules_hash": recorded["activation_rules_hash"],
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
        "provider_requests": 0,
        "provider_requests_before_this_activation": PREFIX_PROVIDER_REQUESTS,
        "broker_actions": 0,
        "returns_computed": 0,
        "prefix_collection_authorized": True,
        "benchmark_prior_close_collection_authorized": True,
        "target_outcome_collection_authorized": False,
        "valid": True,
    }
    value["inspection_sha256"] = common._self_hash(value, "inspection_sha256")
    return value


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
        raise RelativeStrengthError("activation inspection hash is invalid")
    if recorded != inspect_activation(manifest_path, store):
        raise RelativeStrengthError("activation inspection does not rebuild")
    if recorded.get("prefix_collection_authorized") is not True:
        raise RelativeStrengthError("prefix collection is not authorized")
    return manifest, recorded


def _normalize_minutes(
    symbol: str,
    day: str,
    rows: Sequence[Mapping[str, Any]],
    *,
    start_time: time,
    end_time: time,
) -> list[dict[str, Any]]:
    output = []
    seen: set[str] = set()
    for raw in rows:
        try:
            observed = datetime.fromisoformat(str(raw["t"]).replace("Z", "+00:00"))
            observed = observed.astimezone(EASTERN)
        except (KeyError, TypeError, ValueError) as exc:
            raise RelativeStrengthError(f"{symbol} {day}: malformed timestamp") from exc
        if observed.date().isoformat() != day:
            raise RelativeStrengthError(f"{symbol} {day}: wrong-date minute bar")
        if not start_time <= observed.time() < end_time:
            continue
        try:
            opened = float(raw["o"])
            high = float(raw["h"])
            low = float(raw["l"])
            close = float(raw["c"])
            volume = int(float(raw["v"]))
            wap = float(raw.get("vw") or 0)
        except (KeyError, TypeError, ValueError) as exc:
            raise RelativeStrengthError(f"{symbol} {day}: malformed OHLCV") from exc
        stamp = observed.isoformat()
        if (
            stamp in seen
            or min(opened, high, low, close) <= 0
            or low > min(opened, close)
            or high < max(opened, close)
            or volume < 0
            or wap < 0
        ):
            raise RelativeStrengthError(f"{symbol} {day}: invalid minute bar")
        seen.add(stamp)
        output.append(
            {
                "time_et": stamp,
                "open": opened,
                "high": high,
                "low": low,
                "close": close,
                "volume": volume,
                "wap": wap,
            }
        )
    output.sort(key=lambda row: row["time_et"])
    return output


def _complete_session(rows: Sequence[Mapping[str, Any]], day: str, count: int) -> bool:
    expected = [
        (
            datetime.combine(date.fromisoformat(day), time(9, 30), tzinfo=EASTERN)
            + timedelta(minutes=offset)
        ).isoformat()
        for offset in range(count)
    ]
    return len(rows) == count and [row.get("time_et") for row in rows] == expected


def collect_prefix(
    manifest_path: Path,
    inspection_path: Path,
    *,
    env_path: Path,
    store: HistoricalDayStore | None = None,
) -> dict[str, Any]:
    source = store or HistoricalDayStore.from_env(env_path)
    manifest, inspection = _validate_activation_inspection(
        manifest_path, inspection_path, source
    )
    common._require_published((manifest_path, inspection_path))
    graph = _load_gzip(_membership_path(source))
    if shutil.disk_usage(source.root).free < MINIMUM_FREE_BYTES:
        raise RelativeStrengthError("historical store is below its reserve")
    config = AlpacaBulkConfig.from_env(env_path)
    provider_requests = 0
    date_hashes: dict[str, str] = {}
    symbol_rows = 0
    minute_rows = 0
    ignored_rows = 0
    with AlpacaBulkBarsClient(config) as client:
        for day in graph["target_dates"]:
            path = _prefix_path(source, day)
            symbols = sorted(
                {row["symbol"] for row in graph["members_by_date"][day]} | {"SPY"}
            )
            if path.exists():
                payload = _load_gzip(path)
                if (
                    payload.get("manifest_sha256") != manifest["manifest_sha256"]
                    or payload.get("symbols") != symbols
                ):
                    raise RelativeStrengthError(f"{day}: prefix payload drifted")
            else:
                session_day = date.fromisoformat(day)
                rows_by_symbol: dict[str, list[dict[str, Any]]] = {}
                calls = 0
                raw_count = 0
                for offset in range(0, len(symbols), config.batch_size):
                    batch = symbols[offset : offset + config.batch_size]
                    fetched, pages = client.fetch(
                        batch,
                        timeframe="1Min",
                        start=datetime.combine(
                            session_day, time(9, 30), tzinfo=EASTERN
                        ),
                        end=datetime.combine(session_day, time(10), tzinfo=EASTERN),
                    )
                    calls += pages
                    for symbol in batch:
                        raw = fetched.get(symbol, [])
                        raw_count += len(raw)
                        rows_by_symbol[symbol] = _normalize_minutes(
                            symbol,
                            day,
                            raw,
                            start_time=time(9, 30),
                            end_time=time(10),
                        )
                payload = {
                    "schema_version": SCHEMA_VERSION,
                    "dataset_id": DATASET_ID,
                    "manifest_sha256": manifest["manifest_sha256"],
                    "activation_inspection_sha256": inspection["inspection_sha256"],
                    "day": day,
                    "symbols": symbols,
                    "provider_requests": calls,
                    "provider_rows": raw_count,
                    "broker_actions": 0,
                    "returns_computed": 0,
                    "rows_by_symbol": rows_by_symbol,
                }
                _write_gzip(path, payload)
            provider_requests += int(payload["provider_requests"])
            date_hashes[day] = sha256_file(path)
            rows_by_symbol = payload["rows_by_symbol"]
            symbol_rows += sum(bool(rows_by_symbol.get(symbol)) for symbol in symbols)
            retained = sum(len(rows_by_symbol.get(symbol, [])) for symbol in symbols)
            minute_rows += retained
            ignored_rows += int(payload["provider_rows"]) - retained
    status: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "status_kind": "stage0-relative-strength-prefix-collection",
        "dataset_id": DATASET_ID,
        "variant_id": VARIANT_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "activation_inspection_sha256": inspection["inspection_sha256"],
        "target_dates": len(graph["target_dates"]),
        "member_symbol_sessions": manifest["denominator"]["member_symbol_sessions"],
        "symbol_date_payloads_with_rows": symbol_rows,
        "minute_rows": minute_rows,
        "ignored_outside_window_rows": ignored_rows,
        "provider_requests": provider_requests,
        "private_date_file_sha256": date_hashes,
        "broker_actions": 0,
        "returns_computed": 0,
        "status": "READY",
    }
    status["status_sha256"] = common._self_hash(status, "status_sha256")
    _write_json(PREFIX_STATUS, status)
    return status


def _validate_prefix(
    manifest: Mapping[str, Any], store: HistoricalDayStore
) -> dict[str, Any]:
    status = _load_json(PREFIX_STATUS)
    graph = _load_gzip(_membership_path(store))
    expected_manifest = manifest.get("collection_lineage", {}).get(
        "prefix_source_manifest_sha256", manifest.get("manifest_sha256")
    )
    if (
        status.get("status_sha256") != common._self_hash(status, "status_sha256")
        or status.get("status") != "READY"
        or status.get("manifest_sha256") != expected_manifest
        or status.get("status_sha256")
        != manifest.get("collection_lineage", {}).get(
            "prefix_status_sha256", status.get("status_sha256")
        )
        or status.get("returns_computed") != 0
        or status.get("broker_actions") != 0
    ):
        raise RelativeStrengthError("prefix status is unusable")
    expected = {
        day: sha256_file(_prefix_path(store, day)) for day in graph["target_dates"]
    }
    if status.get("private_date_file_sha256") != expected:
        raise RelativeStrengthError("private prefix payload drifted")
    return status


def collect_benchmark(
    manifest_path: Path,
    inspection_path: Path,
    *,
    env_path: Path,
    store: HistoricalDayStore | None = None,
) -> dict[str, Any]:
    source = store or HistoricalDayStore.from_env(env_path)
    manifest, inspection = _validate_activation_inspection(
        manifest_path, inspection_path, source
    )
    common._require_published((manifest_path, inspection_path, PREFIX_STATUS))
    _validate_prefix(manifest, source)
    graph = _load_gzip(_membership_path(source))
    path = _benchmark_path(source)
    if path.exists():
        payload = _load_gzip(path)
        if payload.get("manifest_sha256") != manifest["manifest_sha256"]:
            raise RelativeStrengthError("benchmark prior-close payload drifted")
    else:
        config = AlpacaBulkConfig.from_env(env_path)
        rows_by_target_date: dict[str, dict[str, Any] | None] = {}
        provider_requests = 0
        with AlpacaBulkBarsClient(config) as client:
            for target_day in graph["target_dates"]:
                prior_day = date.fromisoformat(graph["prior_sessions"][target_day])
                fetched, pages = client.fetch(
                    ["SPY"],
                    timeframe="1Day",
                    start=datetime.combine(prior_day, time(0), tzinfo=EASTERN),
                    end=datetime.combine(
                        prior_day + timedelta(days=1), time(0), tzinfo=EASTERN
                    ),
                )
                provider_requests += pages
                normalized = cross._normalize_daily_rows("SPY", fetched.get("SPY", []))
                matches = [
                    row for row in normalized if row["date"] == prior_day.isoformat()
                ]
                if len(matches) > 1:
                    raise RelativeStrengthError(
                        f"{target_day}: duplicate SPY prior close"
                    )
                rows_by_target_date[target_day] = matches[0] if matches else None
        payload = {
            "schema_version": SCHEMA_VERSION,
            "dataset_id": DATASET_ID,
            "manifest_sha256": manifest["manifest_sha256"],
            "activation_inspection_sha256": inspection["inspection_sha256"],
            "symbol": "SPY",
            "provider_requests": provider_requests,
            "rows_by_target_date": rows_by_target_date,
            "target_date_bars_requested": 0,
            "broker_actions": 0,
            "returns_computed": 0,
        }
        _write_gzip(path, payload)
    rows = payload["rows_by_target_date"]
    status: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "status_kind": "stage0-relative-strength-benchmark-prior-close-collection",
        "dataset_id": DATASET_ID,
        "variant_id": VARIANT_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "activation_inspection_sha256": inspection["inspection_sha256"],
        "requested_prior_sessions": len(graph["target_dates"]),
        "prior_sessions_with_rows": sum(row is not None for row in rows.values()),
        "provider_requests": int(payload["provider_requests"]),
        "target_date_bars_requested": 0,
        "private_payload_sha256": sha256_file(path),
        "broker_actions": 0,
        "returns_computed": 0,
        "status": "READY",
    }
    status["status_sha256"] = common._self_hash(status, "status_sha256")
    _write_json(BENCHMARK_STATUS, status)
    return status


def _validate_benchmark(
    manifest: Mapping[str, Any], store: HistoricalDayStore
) -> tuple[dict[str, Any], dict[str, Any]]:
    status = _load_json(BENCHMARK_STATUS)
    path = _benchmark_path(store)
    if (
        status.get("status_sha256") != common._self_hash(status, "status_sha256")
        or status.get("status") != "READY"
        or status.get("manifest_sha256") != manifest.get("manifest_sha256")
        or status.get("private_payload_sha256") != sha256_file(path)
        or status.get("target_date_bars_requested") != 0
        or status.get("returns_computed") != 0
        or status.get("broker_actions") != 0
    ):
        raise RelativeStrengthError("benchmark prior-close status is unusable")
    payload = _load_gzip(path)
    if payload.get("manifest_sha256") != manifest.get("manifest_sha256"):
        raise RelativeStrengthError("benchmark prior-close identity drifted")
    return status, payload


def _split_days() -> dict[str, set[str]]:
    output: dict[str, set[str]] = defaultdict(set)
    for path in SPLIT_PATHS:
        for symbol, rows in load_split_actions(path).items():
            for row in rows:
                output[symbol].add(row["execution_date"].isoformat())
    return output


def inspect_prefix(
    manifest_path: Path,
    activation_inspection_path: Path,
    store: HistoricalDayStore | None = None,
    *,
    require_published: bool = True,
) -> dict[str, Any]:
    source = store or HistoricalDayStore.from_env()
    manifest, activation = _validate_activation_inspection(
        manifest_path, activation_inspection_path, source
    )
    if require_published:
        common._require_published(
            (
                manifest_path,
                activation_inspection_path,
                PREFIX_STATUS,
                BENCHMARK_STATUS,
            )
        )
    prefix_status = _validate_prefix(manifest, source)
    benchmark_status, benchmark = _validate_benchmark(manifest, source)
    daily_status, daily_payload = _daily_source(source)
    graph = _load_gzip(_membership_path(source))
    daily = {
        symbol: {row["date"]: row for row in rows}
        for symbol, rows in daily_payload["rows_by_symbol"].items()
    }
    split_days = _split_days()
    records_by_date: dict[str, list[dict[str, Any]]] = {}
    counts: Counter[str] = Counter()
    leaders = 0
    no_leader_dates = 0
    for day in graph["target_dates"]:
        payload = _load_gzip(_prefix_path(source, day))
        rows_by_symbol = payload["rows_by_symbol"]
        prior_day = graph["prior_sessions"][day]
        spy_rows = rows_by_symbol.get("SPY", [])
        spy_daily = benchmark["rows_by_target_date"].get(day)
        if not _complete_session(spy_rows, day, 30) or not spy_daily:
            raise RelativeStrengthError(f"{day}: complete SPY prefix is required")
        spy_return = float(spy_rows[-1]["close"]) / float(spy_daily["close"]) - 1
        records = []
        for member in graph["members_by_date"][day]:
            symbol = member["symbol"]
            rows = rows_by_symbol.get(symbol, [])
            prior = daily.get(symbol, {}).get(prior_day)
            if not _complete_session(rows, day, 30):
                record = {"symbol": symbol, "status": "incomplete_prefix"}
            elif not prior:
                record = {"symbol": symbol, "status": "missing_prior_close"}
            elif day in split_days.get(symbol, set()):
                record = {"symbol": symbol, "status": "split_on_target_date"}
            else:
                ranking_close = float(rows[-1]["close"])
                relative = ranking_close / float(prior["close"]) - 1 - spy_return
                if ranking_close <= 5:
                    record = {"symbol": symbol, "status": "price_not_above_5"}
                elif relative < 0.02:
                    record = {"symbol": symbol, "status": "below_relative_strength"}
                else:
                    record = {
                        "symbol": symbol,
                        "status": "leader",
                        "relative_strength_fraction": relative,
                    }
            counts[record["status"]] += 1
            records.append(record)
        ranked = sorted(
            (row for row in records if row["status"] == "leader"),
            key=lambda row: (-float(row["relative_strength_fraction"]), row["symbol"]),
        )
        for rank, row in enumerate(ranked, start=1):
            row["rank"] = rank
        leaders += len(ranked)
        no_leader_dates += not ranked
        records_by_date[day] = records
    index = {
        "schema_version": SCHEMA_VERSION,
        "dataset_id": DATASET_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "prefix_status_sha256": prefix_status["status_sha256"],
        "prior_close_status_sha256": daily_status["status_sha256"],
        "benchmark_status_sha256": benchmark_status["status_sha256"],
        "records_by_date": records_by_date,
        "status_counts": dict(sorted(counts.items())),
        "returns_computed": 0,
    }
    path = _candidate_path(source)
    if path.exists() and _load_gzip(path) != index:
        raise RelativeStrengthError("private ranked-candidate index drifted")
    if not path.exists():
        _write_gzip(path, index)
    value: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "inspection_kind": "stage0-relative-strength-prefix-inspection",
        "variant_id": VARIANT_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "manifest_file_sha256": sha256_file(manifest_path),
        "activation_inspection_sha256": activation["inspection_sha256"],
        "prefix_status_sha256": prefix_status["status_sha256"],
        "prior_close_status_sha256": daily_status["status_sha256"],
        "target_dates": len(graph["target_dates"]),
        "member_symbol_sessions": manifest["denominator"]["member_symbol_sessions"],
        "qualifying_leader_symbol_sessions": leaders,
        "dates_without_leaders": no_leader_dates,
        "status_counts": index["status_counts"],
        "private_candidate_index_sha256": _hash(index),
        "provider_requests_during_inspection": 0,
        "broker_actions": 0,
        "returns_computed": 0,
        "target_outcome_collection_authorized": True,
        "valid": True,
    }
    value["inspection_sha256"] = common._self_hash(value, "inspection_sha256")
    return value


def _validate_prefix_inspection(
    manifest_path: Path,
    activation_inspection_path: Path,
    prefix_inspection_path: Path,
    store: HistoricalDayStore,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    manifest = _load_json(manifest_path)
    _validate_manifest(manifest)
    recorded = _load_json(prefix_inspection_path)
    if recorded.get("inspection_sha256") != common._self_hash(
        recorded, "inspection_sha256"
    ):
        raise RelativeStrengthError("prefix inspection hash is invalid")
    rebuilt = inspect_prefix(
        manifest_path,
        activation_inspection_path,
        store,
        require_published=False,
    )
    if recorded != rebuilt or recorded.get("target_outcome_collection_authorized") is not True:
        raise RelativeStrengthError("prefix inspection does not rebuild")
    index = _load_gzip(_candidate_path(store))
    if _hash(index) != recorded["private_candidate_index_sha256"]:
        raise RelativeStrengthError("private candidate index drifted")
    return manifest, recorded, index


def collect_outcomes(
    manifest_path: Path,
    activation_inspection_path: Path,
    prefix_inspection_path: Path,
    *,
    env_path: Path,
    store: HistoricalDayStore | None = None,
) -> dict[str, Any]:
    source = store or HistoricalDayStore.from_env(env_path)
    manifest, inspection, index = _validate_prefix_inspection(
        manifest_path, activation_inspection_path, prefix_inspection_path, source
    )
    common._require_published(
        (manifest_path, activation_inspection_path, prefix_inspection_path)
    )
    if shutil.disk_usage(source.root).free < MINIMUM_FREE_BYTES:
        raise RelativeStrengthError("historical store is below its reserve")
    config = AlpacaBulkConfig.from_env(env_path)
    provider_requests = 0
    date_hashes: dict[str, str] = {}
    requested_symbol_sessions = 0
    complete_symbol_sessions = 0
    minute_rows = 0
    ignored_rows = 0
    with AlpacaBulkBarsClient(config) as client:
        for day, records in index["records_by_date"].items():
            symbols = sorted(row["symbol"] for row in records if row["status"] == "leader")
            path = _outcome_path(source, day)
            if path.exists():
                payload = _load_gzip(path)
                if (
                    payload.get("manifest_sha256") != manifest["manifest_sha256"]
                    or payload.get("symbols") != symbols
                ):
                    raise RelativeStrengthError(f"{day}: outcome payload drifted")
            else:
                session_day = date.fromisoformat(day)
                rows_by_symbol: dict[str, list[dict[str, Any]]] = {}
                calls = 0
                raw_count = 0
                for offset in range(0, len(symbols), config.batch_size):
                    batch = symbols[offset : offset + config.batch_size]
                    fetched, pages = client.fetch(
                        batch,
                        timeframe="1Min",
                        start=datetime.combine(session_day, time(9, 30), tzinfo=EASTERN),
                        end=datetime.combine(session_day, time(16), tzinfo=EASTERN),
                    )
                    calls += pages
                    for symbol in batch:
                        raw = fetched.get(symbol, [])
                        raw_count += len(raw)
                        rows_by_symbol[symbol] = _normalize_minutes(
                            symbol,
                            day,
                            raw,
                            start_time=time(9, 30),
                            end_time=time(16),
                        )
                payload = {
                    "schema_version": SCHEMA_VERSION,
                    "dataset_id": DATASET_ID,
                    "manifest_sha256": manifest["manifest_sha256"],
                    "prefix_inspection_sha256": inspection["inspection_sha256"],
                    "day": day,
                    "symbols": symbols,
                    "provider_requests": calls,
                    "provider_rows": raw_count,
                    "broker_actions": 0,
                    "returns_computed": 0,
                    "rows_by_symbol": rows_by_symbol,
                }
                _write_gzip(path, payload)
            provider_requests += int(payload["provider_requests"])
            date_hashes[day] = sha256_file(path)
            requested_symbol_sessions += len(symbols)
            complete_symbol_sessions += sum(
                _complete_session(payload["rows_by_symbol"].get(symbol, []), day, 390)
                for symbol in symbols
            )
            retained = sum(len(rows) for rows in payload["rows_by_symbol"].values())
            minute_rows += retained
            ignored_rows += int(payload["provider_rows"]) - retained
    status: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "status_kind": "stage0-relative-strength-outcome-collection",
        "dataset_id": DATASET_ID,
        "variant_id": VARIANT_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "prefix_inspection_sha256": inspection["inspection_sha256"],
        "target_dates": manifest["denominator"]["target_dates"],
        "requested_leader_symbol_sessions": requested_symbol_sessions,
        "complete_leader_symbol_sessions": complete_symbol_sessions,
        "minute_rows": minute_rows,
        "ignored_outside_session_rows": ignored_rows,
        "provider_requests": provider_requests,
        "private_date_file_sha256": date_hashes,
        "broker_actions": 0,
        "returns_computed": 0,
        "status": "READY",
    }
    status["status_sha256"] = common._self_hash(status, "status_sha256")
    _write_json(OUTCOME_STATUS, status)
    return status


def _validate_outcomes(
    manifest: Mapping[str, Any], store: HistoricalDayStore
) -> dict[str, Any]:
    status = _load_json(OUTCOME_STATUS)
    if (
        status.get("status_sha256") != common._self_hash(status, "status_sha256")
        or status.get("status") != "READY"
        or status.get("manifest_sha256") != manifest.get("manifest_sha256")
        or status.get("returns_computed") != 0
        or status.get("broker_actions") != 0
    ):
        raise RelativeStrengthError("outcome status is unusable")
    expected = {
        day: sha256_file(_outcome_path(store, day)) for day in manifest["target_dates"]
    }
    if status.get("private_date_file_sha256") != expected:
        raise RelativeStrengthError("private outcome payload drifted")
    return status


def inspect_inputs(
    manifest_path: Path,
    activation_inspection_path: Path,
    prefix_inspection_path: Path,
    store: HistoricalDayStore | None = None,
    *,
    require_published: bool = True,
) -> dict[str, Any]:
    source = store or HistoricalDayStore.from_env()
    manifest, prefix_inspection, _ = _validate_prefix_inspection(
        manifest_path, activation_inspection_path, prefix_inspection_path, source
    )
    if require_published:
        common._require_published(
            (
                manifest_path,
                activation_inspection_path,
                prefix_inspection_path,
                OUTCOME_STATUS,
            )
        )
    outcome_status = _validate_outcomes(manifest, source)
    value: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "inspection_kind": "stage0-intraday-input-inspection",
        "variant_id": VARIANT_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "manifest_file_sha256": sha256_file(manifest_path),
        "prefix_inspection_sha256": prefix_inspection["inspection_sha256"],
        "outcome_status_sha256": outcome_status["status_sha256"],
        "target_dates": manifest["denominator"]["target_dates"],
        "qualifying_leader_symbol_sessions": prefix_inspection[
            "qualifying_leader_symbol_sessions"
        ],
        "complete_leader_symbol_sessions": outcome_status[
            "complete_leader_symbol_sessions"
        ],
        "provider_requests_during_inspection": 0,
        "broker_actions": 0,
        "returns_computed": 0,
        "return_evaluation_authorized": True,
        "valid": True,
    }
    value["inspection_sha256"] = common._self_hash(value, "inspection_sha256")
    return value


def _validate_input_inspection(
    manifest_path: Path,
    activation_inspection_path: Path,
    prefix_inspection_path: Path,
    input_inspection_path: Path,
    store: HistoricalDayStore,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    manifest = _load_json(manifest_path)
    _validate_manifest(manifest)
    recorded = _load_json(input_inspection_path)
    if recorded.get("inspection_sha256") != common._self_hash(
        recorded, "inspection_sha256"
    ):
        raise RelativeStrengthError("input inspection hash is invalid")
    rebuilt = inspect_inputs(
        manifest_path,
        activation_inspection_path,
        prefix_inspection_path,
        store,
        require_published=False,
    )
    if recorded != rebuilt or recorded.get("return_evaluation_authorized") is not True:
        raise RelativeStrengthError("input inspection does not rebuild")
    return manifest, recorded, _load_gzip(_candidate_path(store))


def _trigger(rows: Sequence[Mapping[str, Any]]) -> tuple[int, float] | None:
    if len(rows) != 390:
        return None
    cumulative_volume = 0
    cumulative_value = 0.0
    for index, row in enumerate(rows):
        volume = int(row["volume"])
        cumulative_volume += volume
        cumulative_value += float(row["wap"]) * volume
        if not 30 <= index <= 300:
            continue
        prior_high = max(float(item["high"]) for item in rows[:index])
        prior_mean_volume = statistics.fmean(
            int(item["volume"]) for item in rows[index - 20 : index]
        )
        session_vwap = (
            cumulative_value / cumulative_volume if cumulative_volume > 0 else math.inf
        )
        if (
            float(row["close"]) > prior_high
            and float(row["close"]) > session_vwap
            and prior_mean_volume > 0
            and volume >= 1.5 * prior_mean_volume
        ):
            stop = min(float(item["low"]) for item in rows[index - 4 : index + 1])
            if index + 1 < len(rows) and float(rows[index + 1]["open"]) > stop > 0:
                return index, stop
    return None


def _trade_outcome(
    rows: Sequence[Mapping[str, Any]], trigger_index: int, stop: float, cost_bps: int
) -> dict[str, Any]:
    entry_index = trigger_index + 1
    raw_entry = float(rows[entry_index]["open"])
    raw_risk = raw_entry - stop
    if raw_risk <= 0:
        raise RelativeStrengthError("planned stop is not below entry")
    target = raw_entry + 2 * raw_risk
    raw_exit = float(rows[380]["open"])
    exit_reason = "force_flat"
    exit_index = 380
    for index in range(entry_index, 380):
        row = rows[index]
        if float(row["low"]) <= stop:
            raw_exit = stop
            exit_reason = "stop"
            exit_index = index
            break
        if float(row["high"]) >= target:
            raw_exit = target
            exit_reason = "target"
            exit_index = index
            break
    cost = cost_bps / 10_000
    entry_fill = raw_entry * (1 + cost)
    exit_fill = raw_exit * (1 - cost)
    stop_fill = stop * (1 - cost)
    planned_risk = entry_fill - stop_fill
    if planned_risk <= 0:
        raise RelativeStrengthError("cost-adjusted planned risk is not positive")
    return {
        "entry_time_et": rows[entry_index]["time_et"],
        "exit_time_et": rows[exit_index]["time_et"],
        "exit_reason": exit_reason,
        "stop_executed": exit_reason == "stop",
        "net_r": (exit_fill - entry_fill) / planned_risk,
    }


def build_result(
    manifest_path: Path,
    activation_inspection_path: Path,
    prefix_inspection_path: Path,
    input_inspection_path: Path,
    store: HistoricalDayStore | None = None,
    *,
    require_published: bool = True,
) -> dict[str, Any]:
    source = store or HistoricalDayStore.from_env()
    manifest, inspection, index = _validate_input_inspection(
        manifest_path,
        activation_inspection_path,
        prefix_inspection_path,
        input_inspection_path,
        source,
    )
    if require_published:
        common._require_published(
            (
                manifest_path,
                activation_inspection_path,
                prefix_inspection_path,
                input_inspection_path,
            )
        )
    records = []
    dispositions: Counter[str] = Counter()
    for day in manifest["target_dates"]:
        payload = _load_gzip(_outcome_path(source, day))
        rows_by_symbol = payload["rows_by_symbol"]
        candidates = sorted(
            (row for row in index["records_by_date"][day] if row["status"] == "leader"),
            key=lambda row: int(row["rank"]),
        )
        triggers = []
        for candidate in candidates:
            rows = rows_by_symbol.get(candidate["symbol"], [])
            if not _complete_session(rows, day, 390):
                dispositions["incomplete_outcome_session"] += 1
                continue
            found = _trigger(rows)
            if found is None:
                dispositions["no_executable_trigger"] += 1
                continue
            triggers.append((found[0], int(candidate["rank"]), candidate, found[1], rows))
        if not triggers:
            dispositions["no_trade_date"] += 1
            continue
        trigger_index, rank, candidate, stop, rows = min(
            triggers, key=lambda item: (item[0], item[1], item[2]["symbol"])
        )
        dispositions["selected"] += 1
        dispositions["later_trigger_not_selected"] += len(triggers) - 1
        outcomes = {
            str(cost): _trade_outcome(rows, trigger_index, stop, cost)
            for cost in (PRIMARY_COST_BPS, *STRESS_COST_BPS)
        }
        primary = outcomes[str(PRIMARY_COST_BPS)]
        records.append(
            {
                "signal_date": day,
                "symbol": candidate["symbol"],
                "relative_strength_rank": rank,
                "relative_strength_fraction": candidate[
                    "relative_strength_fraction"
                ],
                "trigger_time_et": rows[trigger_index]["time_et"],
                "entry_time_et": primary["entry_time_et"],
                "exit_time_et": primary["exit_time_et"],
                "exit_reason": primary["exit_reason"],
                "stop_executed": primary["stop_executed"],
                "net_r": primary["net_r"],
                "stress_10bps_r": outcomes["10"]["net_r"],
                "stress_20bps_r": outcomes["20"]["net_r"],
            }
        )
    primary = common._metrics([float(row["net_r"]) for row in records])
    stress = {
        "10": common._metrics([float(row["stress_10bps_r"]) for row in records]),
        "20": common._metrics([float(row["stress_20bps_r"]) for row in records]),
    }
    gate = manifest["stage0_gate"]
    blockers = []
    if len(records) < gate["minimum_closed_signals"]:
        blockers.append("closed signals are below the Stage 0 minimum")
    if primary["expectancy_r"] is None or primary["expectancy_r"] <= 0:
        blockers.append("primary expectancy is not positive")
    if not primary["profit_factor_infinite"] and (
        primary["profit_factor"] is None
        or primary["profit_factor"] < gate["minimum_profit_factor"]
    ):
        blockers.append("primary profit factor is below the Stage 0 minimum")
    if primary["maximum_drawdown_r"] > gate["maximum_drawdown_r"]:
        blockers.append("primary drawdown exceeds the Stage 0 maximum")
    if stress["20"]["total_r"] <= 0:
        blockers.append("20 bps-per-side total R is not positive")
    value: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "result_kind": "stage0-falsification",
        "variant_id": VARIANT_ID,
        "strategy_version": manifest["strategy_version"],
        "mechanism_family": manifest["mechanism_family"],
        "base_rules_hash": manifest["base_rules_hash"],
        "activation_rules_hash": manifest["activation_rules_hash"],
        "manifest_sha256": manifest["manifest_sha256"],
        "input_inspection_sha256": inspection["inspection_sha256"],
        "implementation_sha256": manifest["implementation_sha256"],
        "claim_scope": "FALSIFICATION_ONLY",
        "development_evidence_eligible": False,
        "confirmation_evidence_eligible": False,
        "provider_requests_during_evaluation": 0,
        "broker_actions": 0,
        "denominator": {
            "target_dates": manifest["denominator"]["target_dates"],
            "member_symbol_sessions": manifest["denominator"]["member_symbol_sessions"],
            "qualifying_leader_symbol_sessions": inspection[
                "qualifying_leader_symbol_sessions"
            ],
            "complete_leader_symbol_sessions": inspection[
                "complete_leader_symbol_sessions"
            ],
            "closed_signals": len(records),
            "no_trade_dates": manifest["denominator"]["target_dates"] - len(records),
            "rule_violations": 0,
        },
        "disposition_counts": dict(sorted(dispositions.items())),
        "primary_5bps": primary,
        "stress": stress,
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
    value["result_sha256"] = common._self_hash(value, "result_sha256")
    return value


def inspect_result(
    manifest_path: Path,
    activation_inspection_path: Path,
    prefix_inspection_path: Path,
    input_inspection_path: Path,
    result_path: Path,
    store: HistoricalDayStore | None = None,
) -> dict[str, Any]:
    recorded = _load_json(result_path)
    if recorded.get("result_sha256") != common._self_hash(recorded, "result_sha256"):
        raise RelativeStrengthError("result hash is invalid")
    rebuilt = build_result(
        manifest_path,
        activation_inspection_path,
        prefix_inspection_path,
        input_inspection_path,
        store,
        require_published=False,
    )
    if recorded != rebuilt:
        raise RelativeStrengthError("result does not rebuild")
    value: dict[str, Any] = {
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
    value["inspection_sha256"] = common._self_hash(value, "inspection_sha256")
    return value


def _publish(value: Mapping[str, Any], path: Path) -> None:
    if "inspection_kind" in value:
        identity = str(value.get("inspection_sha256", ""))
    elif "result_kind" in value:
        identity = str(value.get("result_sha256", ""))
    else:
        identity = str(value.get("manifest_sha256", ""))
    if not identity or not path.name.endswith(f"-{identity}.json"):
        raise RelativeStrengthError("output filename must end with content hash")
    if path.resolve().parent not in {
        ACTIVATION_ROOT.resolve(),
        INSPECTION_ROOT.resolve(),
        RESULT_ROOT.resolve(),
    }:
        raise RelativeStrengthError("output path is outside evidence roots")
    _write_json(path.resolve(), value)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    freeze = commands.add_parser("freeze")
    freeze.add_argument("--output", type=Path)
    inspect = commands.add_parser("inspect")
    inspect.add_argument("manifest", type=Path)
    inspect.add_argument("--output", type=Path)
    prefix = commands.add_parser("collect-prefix")
    prefix.add_argument("manifest", type=Path)
    prefix.add_argument("activation_inspection", type=Path)
    prefix.add_argument("--env-file", type=Path, default=PROJECT_ROOT / ".env")
    benchmark = commands.add_parser("collect-benchmark")
    benchmark.add_argument("manifest", type=Path)
    benchmark.add_argument("activation_inspection", type=Path)
    benchmark.add_argument("--env-file", type=Path, default=PROJECT_ROOT / ".env")
    inspect_selection = commands.add_parser("inspect-prefix")
    inspect_selection.add_argument("manifest", type=Path)
    inspect_selection.add_argument("activation_inspection", type=Path)
    inspect_selection.add_argument("--output", type=Path)
    outcomes = commands.add_parser("collect-outcomes")
    outcomes.add_argument("manifest", type=Path)
    outcomes.add_argument("activation_inspection", type=Path)
    outcomes.add_argument("prefix_inspection", type=Path)
    outcomes.add_argument("--env-file", type=Path, default=PROJECT_ROOT / ".env")
    inspect_data = commands.add_parser("inspect-inputs")
    inspect_data.add_argument("manifest", type=Path)
    inspect_data.add_argument("activation_inspection", type=Path)
    inspect_data.add_argument("prefix_inspection", type=Path)
    inspect_data.add_argument("--output", type=Path)
    evaluate = commands.add_parser("evaluate")
    evaluate.add_argument("manifest", type=Path)
    evaluate.add_argument("activation_inspection", type=Path)
    evaluate.add_argument("prefix_inspection", type=Path)
    evaluate.add_argument("input_inspection", type=Path)
    evaluate.add_argument("--output", type=Path)
    inspect_outcome = commands.add_parser("inspect-result")
    inspect_outcome.add_argument("manifest", type=Path)
    inspect_outcome.add_argument("activation_inspection", type=Path)
    inspect_outcome.add_argument("prefix_inspection", type=Path)
    inspect_outcome.add_argument("input_inspection", type=Path)
    inspect_outcome.add_argument("result", type=Path)
    inspect_outcome.add_argument("--output", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "freeze":
            value: Any = build_manifest()
        elif args.command == "inspect":
            value = inspect_activation(args.manifest)
        elif args.command == "collect-prefix":
            value = collect_prefix(
                args.manifest,
                args.activation_inspection,
                env_path=args.env_file,
            )
        elif args.command == "collect-benchmark":
            value = collect_benchmark(
                args.manifest,
                args.activation_inspection,
                env_path=args.env_file,
            )
        elif args.command == "inspect-prefix":
            value = inspect_prefix(args.manifest, args.activation_inspection)
        elif args.command == "collect-outcomes":
            value = collect_outcomes(
                args.manifest,
                args.activation_inspection,
                args.prefix_inspection,
                env_path=args.env_file,
            )
        elif args.command == "inspect-inputs":
            value = inspect_inputs(
                args.manifest,
                args.activation_inspection,
                args.prefix_inspection,
            )
        elif args.command == "evaluate":
            value = build_result(
                args.manifest,
                args.activation_inspection,
                args.prefix_inspection,
                args.input_inspection,
            )
        else:
            value = inspect_result(
                args.manifest,
                args.activation_inspection,
                args.prefix_inspection,
                args.input_inspection,
                args.result,
            )
        if args.command not in {
            "collect-prefix",
            "collect-benchmark",
            "collect-outcomes",
        } and args.output:
            _publish(value, args.output)
    except Exception as exc:
        print(
            json.dumps({"status": "BLOCKED", "error": str(exc)}, sort_keys=True),
            file=sys.stderr,
        )
        return 1
    print(json.dumps(value, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

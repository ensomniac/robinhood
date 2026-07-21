"""Freeze, inspect, evaluate, and audit ETF opening-range Stage 0 evidence."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import statistics
import subprocess
import sys
from collections import Counter
from collections.abc import Mapping, Sequence
from datetime import datetime, time
from pathlib import Path
from typing import Any

from historical_store import HistoricalDayStore, sha256_file
from portfolio_tournament import inspect_manifest as inspect_slate


PROJECT_ROOT = Path(__file__).resolve().parent
SLATE_PATH = (
    PROJECT_ROOT
    / "strategy_tournament"
    / "manifests"
    / "portfolio-stage0-slate-1c4f1cd20c490ea56e36fc0b81c30f1c1eb588d9a9dfb310501028de95b3c50f.json"
)
STORE_METADATA_KEY = "_store.json"
SYMBOLS = ("SPY", "QQQ")
VARIANT_ID = "etf-or-momentum-v1"
VARIANT_ORDINAL = 1
SCHEMA_VERSION = 1
PRIMARY_COST_BPS = 5
STRESS_COST_BPS = (10, 20)
TRIGGER_START = time(9, 35)
TRIGGER_END = time(11, 0)
FORCE_FLAT = time(15, 50)
DATASET_IDENTITY = {
    "kind": "bars",
    "provider": "ibkr",
    "channel": "trades",
    "timeframe": "1m",
    "feed": "smart",
    "adjustment": "provider_adjusted_unknown_basis",
    "session": "regular",
    "scope": "full_session",
    "complete": True,
    "row_count": 390,
}


class EtfOrbStage0Error(RuntimeError):
    """The ETF Stage 0 contract or evidence is invalid."""


def _normalize_numbers(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _normalize_numbers(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_normalize_numbers(item) for item in value]
    if isinstance(value, float) and value.is_integer():
        return int(value)
    return value


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        _normalize_numbers(value),
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
        raise EtfOrbStage0Error(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise EtfOrbStage0Error(f"{path} must contain an object")
    return value


def _self_hash(value: Mapping[str, Any], field: str) -> str:
    return _hash({key: item for key, item in value.items() if key != field})


def _variant() -> dict[str, Any]:
    inspect_slate(SLATE_PATH)
    slate = _load_json(SLATE_PATH)
    matches = [
        item
        for item in slate["variants"]
        if item.get("variant_id") == VARIANT_ID
        and item.get("variant_ordinal") == VARIANT_ORDINAL
    ]
    if len(matches) != 1:
        raise EtfOrbStage0Error("frozen slate variant identity is unavailable")
    return dict(matches[0])


def _dataset_matches(dataset: Mapping[str, Any]) -> bool:
    quality = dataset.get("quality")
    if not isinstance(quality, Mapping):
        return False
    observed = {
        "kind": dataset.get("kind"),
        "provider": dataset.get("provider"),
        "channel": dataset.get("channel"),
        "timeframe": dataset.get("timeframe"),
        "feed": dataset.get("feed"),
        "adjustment": dataset.get("adjustment"),
        "session": dataset.get("session"),
        "scope": dataset.get("scope"),
        "complete": quality.get("complete"),
        "row_count": quality.get("row_count"),
    }
    return observed == DATASET_IDENTITY


def _selected_dataset(
    store: HistoricalDayStore, symbol: str, day: str
) -> tuple[Path, dict[str, Any]] | None:
    document = store.load(symbol, day)
    if document is None:
        return None
    matches = [item for item in document["datasets"] if _dataset_matches(item)]
    if len(matches) > 1:
        raise EtfOrbStage0Error(f"multiple frozen datasets for {symbol} {day}")
    if not matches:
        return None
    return store.path_for(symbol, day), dict(matches[0])


def _store_key(store: HistoricalDayStore, path: Path) -> str:
    try:
        return path.relative_to(store.root).as_posix()
    except ValueError as exc:
        raise EtfOrbStage0Error("historical input escaped the configured store") from exc


def build_manifest(store: HistoricalDayStore | None = None) -> dict[str, Any]:
    source = store or HistoricalDayStore.from_env()
    variant = _variant()
    common_dates = sorted(set(source.dates("SPY")) & set(source.dates("QQQ")))
    if not common_dates:
        raise EtfOrbStage0Error("no common SPY/QQQ cached dates are available")
    included: list[dict[str, Any]] = []
    excluded: list[dict[str, str]] = []
    for day in common_dates:
        selected: list[dict[str, Any]] = []
        for symbol in SYMBOLS:
            match = _selected_dataset(source, symbol, day)
            if match is None:
                selected = []
                break
            path, dataset = match
            selected.append(
                {
                    "symbol": symbol,
                    "store_key": _store_key(source, path),
                    "source_document_sha256": sha256_file(path),
                    "selected_dataset_id": dataset["id"],
                    "selected_dataset_sha256": dataset["content_sha256"],
                }
            )
        if len(selected) != len(SYMBOLS):
            excluded.append(
                {"date": day, "reason": "missing_exact_frozen_ibkr_dataset"}
            )
        else:
            included.append({"date": day, "symbols": selected})
    if len(included) < 30:
        raise EtfOrbStage0Error("fewer than 30 exact common ETF sessions are available")
    metadata_path = source.root / STORE_METADATA_KEY
    if not metadata_path.is_file():
        raise EtfOrbStage0Error("historical store metadata is missing")
    manifest: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "contract_kind": "stage0-activation",
        "campaign_id": "multi-strategy-portfolio-validation-v1",
        "tournament_wave": 1,
        "variant_ordinal": VARIANT_ORDINAL,
        "variant_id": VARIANT_ID,
        "strategy_version": variant["version"],
        "mechanism_family": variant["mechanism_family"],
        "base_rules_hash": variant["rules_hash"],
        "slate_manifest_sha256": _load_json(SLATE_PATH)["manifest_sha256"],
        "implementation_sha256": sha256_file(Path(__file__).resolve()),
        "store_metadata_sha256": sha256_file(metadata_path),
        "claim_scope": "FALSIFICATION_ONLY",
        "outcomes_previously_accessed": True,
        "development_evidence_eligible": False,
        "confirmation_evidence_eligible": False,
        "provider_requests_authorized": False,
        "broker_actions_authorized": False,
        "return_evaluation_authorized_before_inspection": False,
        "source_selection": {
            "symbols": list(SYMBOLS),
            "whole_provider": "ibkr",
            "dataset_identity": dict(DATASET_IDENTITY),
            "date_rule": "all locally cached dates with the exact dataset for both symbols",
            "substitutions": 0,
        },
        "denominator": {
            "common_cached_dates_inspected": len(common_dates),
            "included_dates": len(included),
            "included_symbol_sessions": len(included) * len(SYMBOLS),
            "excluded_dates": len(excluded),
            "preserve_no_trade_dates": True,
            "maximum_strategy_entries_per_day": 1,
        },
        "selection_contract": {
            "opening_range_bar_starts_et": "09:30:00-09:34:00",
            "trigger_bar_starts_et": "09:35:00-11:00:00",
            "opening_candle": "fifth bar close strictly above first bar open",
            "session_vwap": "cumulative sum(bar WAP times volume) divided by cumulative volume through decision bar",
            "trigger": "first completed close strictly above opening-range high and cumulative session VWAP",
            "entry": "next observed one-minute open",
            "maximum_entry_open_above_range_high_fraction": 0.0015,
            "maximum_stop_fraction": 0.012,
            "daily_selection": "earliest executable entry; ties use larger close/range-high breakout then lexical symbol",
            "miss_policy": "a missed or rejected symbol remains in the denominator; a later executable symbol may be selected",
        },
        "outcome_contract": {
            "stop": "opening-range low",
            "target": "two times raw entry-to-stop distance above raw entry",
            "entry_exit_cost_bps_per_side": [
                PRIMARY_COST_BPS,
                *STRESS_COST_BPS,
            ],
            "same_interval_ambiguity": "stop_first",
            "stop_gap_fill": "worse of stop and observed bar open",
            "target_gap_fill": "target price without favorable gap improvement",
            "force_flat_et": "15:50:00 at observed bar open",
            "planned_risk_r": "cost-adjusted entry minus cost-adjusted planned stop",
        },
        "stage0_gate": {
            "minimum_closed_signals": 30,
            "minimum_expectancy_r_exclusive": 0,
            "minimum_profit_factor": 1.10,
            "maximum_drawdown_r": 8,
            "require_positive_20bps_total_r": True,
            "maximum_rule_violations": 0,
            "effect": "SURVIVE_TO_REPRESENTATIVE_DEVELOPMENT only; never PILOT_READY",
        },
        "excluded_dates": excluded,
        "inputs": included,
    }
    manifest["activation_rules_hash"] = _hash(
        {
            "base_rules_hash": manifest["base_rules_hash"],
            "source_selection": manifest["source_selection"],
            "selection_contract": manifest["selection_contract"],
            "outcome_contract": manifest["outcome_contract"],
            "stage0_gate": manifest["stage0_gate"],
        }
    )
    manifest["manifest_sha256"] = _self_hash(manifest, "manifest_sha256")
    return manifest


def default_manifest_path(manifest: Mapping[str, Any]) -> Path:
    return (
        PROJECT_ROOT
        / "strategy_tournament"
        / "activations"
        / f"{VARIANT_ID}-{manifest['manifest_sha256']}.json"
    )


def _validate_manifest_identity(recorded: Mapping[str, Any]) -> None:
    if recorded.get("manifest_sha256") != _self_hash(recorded, "manifest_sha256"):
        raise EtfOrbStage0Error("activation manifest content hash is invalid")
    if recorded.get("variant_id") != VARIANT_ID:
        raise EtfOrbStage0Error("activation manifest variant identity is invalid")
    if recorded.get("claim_scope") != "FALSIFICATION_ONLY":
        raise EtfOrbStage0Error("activation manifest claim scope is invalid")
    for field in ("development_evidence_eligible", "confirmation_evidence_eligible"):
        if recorded.get(field) is not False:
            raise EtfOrbStage0Error(f"activation manifest {field} must be false")
    for field in ("provider_requests_authorized", "broker_actions_authorized"):
        if recorded.get(field) is not False:
            raise EtfOrbStage0Error(f"activation manifest {field} must be false")


def _row_time(row: Mapping[str, Any]) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(row["t"]))
    except (KeyError, ValueError) as exc:
        raise EtfOrbStage0Error("stored bar has an invalid timestamp") from exc
    if parsed.tzinfo is None:
        raise EtfOrbStage0Error("stored bar timestamp is timezone-naive")
    return parsed


def _validate_bars(rows: Any, symbol: str, day: str) -> list[dict[str, Any]]:
    if not isinstance(rows, list) or len(rows) != 390:
        raise EtfOrbStage0Error(f"{symbol} {day} does not contain 390 bars")
    result: list[dict[str, Any]] = []
    previous: datetime | None = None
    for index, raw in enumerate(rows):
        if not isinstance(raw, Mapping):
            raise EtfOrbStage0Error(f"{symbol} {day} contains a malformed bar")
        row = dict(raw)
        observed = _row_time(row)
        expected = time(9, 30) if index == 0 else None
        if expected is not None and observed.time().replace(tzinfo=None) != expected:
            raise EtfOrbStage0Error(f"{symbol} {day} does not begin at 09:30 ET")
        if observed.date().isoformat() != day:
            raise EtfOrbStage0Error(f"{symbol} {day} contains a wrong-date bar")
        if previous is not None and (observed - previous).total_seconds() != 60:
            raise EtfOrbStage0Error(f"{symbol} {day} bars are not one minute apart")
        previous = observed
        try:
            open_price = float(row["o"])
            high = float(row["h"])
            low = float(row["l"])
            close = float(row["c"])
            volume = int(row["v"])
            wap = float(row["vw"])
        except (KeyError, TypeError, ValueError) as exc:
            raise EtfOrbStage0Error(f"{symbol} {day} bar fields are invalid") from exc
        values = (open_price, high, low, close, wap)
        if not all(math.isfinite(value) and value > 0 for value in values):
            raise EtfOrbStage0Error(f"{symbol} {day} contains nonpositive prices")
        if low > min(open_price, close) or high < max(open_price, close) or low > high:
            raise EtfOrbStage0Error(f"{symbol} {day} contains invalid OHLC ordering")
        if volume < 0 or row.get("i") is not False:
            raise EtfOrbStage0Error(f"{symbol} {day} contains unusable volume or interpolation")
        result.append(row)
    if _row_time(result[-1]).time().replace(tzinfo=None) != time(15, 59):
        raise EtfOrbStage0Error(f"{symbol} {day} does not end at 15:59 ET")
    return result


def _load_manifest_bars(
    manifest: Mapping[str, Any], store: HistoricalDayStore
) -> dict[tuple[str, str], list[dict[str, Any]]]:
    loaded: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for session in manifest["inputs"]:
        day = str(session["date"])
        for source in session["symbols"]:
            symbol = str(source["symbol"])
            path = store.root / str(source["store_key"])
            if not path.is_file() or sha256_file(path) != source["source_document_sha256"]:
                raise EtfOrbStage0Error(f"source document drifted: {symbol} {day}")
            selected = _selected_dataset(store, symbol, day)
            if selected is None:
                raise EtfOrbStage0Error(f"selected dataset disappeared: {symbol} {day}")
            selected_path, dataset = selected
            if selected_path != path:
                raise EtfOrbStage0Error(f"selected source path drifted: {symbol} {day}")
            if (
                dataset.get("id") != source["selected_dataset_id"]
                or dataset.get("content_sha256")
                != source["selected_dataset_sha256"]
            ):
                raise EtfOrbStage0Error(f"selected dataset drifted: {symbol} {day}")
            loaded[(day, symbol)] = _validate_bars(dataset.get("rows"), symbol, day)
    return loaded


def inspect_activation(
    path: Path, store: HistoricalDayStore | None = None
) -> dict[str, Any]:
    recorded = _load_json(path)
    _validate_manifest_identity(recorded)
    rebuilt = build_manifest(store)
    if recorded != rebuilt:
        raise EtfOrbStage0Error("activation manifest does not exactly rebuild")
    source = store or HistoricalDayStore.from_env()
    loaded = _load_manifest_bars(recorded, source)
    result: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "inspection_kind": "stage0-activation-input-inspection",
        "variant_id": VARIANT_ID,
        "manifest_sha256": recorded["manifest_sha256"],
        "manifest_file_sha256": sha256_file(path),
        "implementation_sha256": recorded["implementation_sha256"],
        "selected_dates": recorded["denominator"]["included_dates"],
        "selected_symbol_sessions": len(loaded),
        "selected_bars": sum(len(rows) for rows in loaded.values()),
        "excluded_dates": recorded["denominator"]["excluded_dates"],
        "provider_requests": 0,
        "broker_actions": 0,
        "returns_computed": 0,
        "return_evaluation_authorized": True,
        "claim_scope": "FALSIFICATION_ONLY",
        "valid": True,
    }
    result["inspection_sha256"] = _self_hash(result, "inspection_sha256")
    return result


def _validate_inspection(
    manifest_path: Path, inspection_path: Path, store: HistoricalDayStore
) -> dict[str, Any]:
    recorded = _load_json(inspection_path)
    if recorded.get("inspection_sha256") != _self_hash(
        recorded, "inspection_sha256"
    ):
        raise EtfOrbStage0Error("activation inspection content hash is invalid")
    rebuilt = inspect_activation(manifest_path, store)
    if recorded != rebuilt or recorded.get("return_evaluation_authorized") is not True:
        raise EtfOrbStage0Error("activation inspection does not authorize evaluation")
    return recorded


def _git(*args: str) -> str:
    completed = subprocess.run(
        ["git", *args],
        cwd=PROJECT_ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode:
        raise EtfOrbStage0Error(completed.stderr.strip() or "Git command failed")
    return completed.stdout.strip()


def _require_published(paths: Sequence[Path]) -> None:
    if _git("status", "--porcelain", "--untracked-files=all"):
        raise EtfOrbStage0Error("evaluation requires a clean worktree")
    upstream = _git("rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{u}")
    if _git("rev-parse", "HEAD") != _git("rev-parse", upstream):
        raise EtfOrbStage0Error("evaluation requires HEAD to equal its upstream")
    for path in paths:
        try:
            relative = path.resolve().relative_to(PROJECT_ROOT).as_posix()
        except ValueError as exc:
            raise EtfOrbStage0Error("publication path escaped the repository") from exc
        _git("ls-files", "--error-unmatch", relative)


def _cumulative_vwap(rows: Sequence[Mapping[str, Any]]) -> list[float]:
    numerator = 0.0
    denominator = 0
    result: list[float] = []
    for row in rows:
        volume = int(row["v"])
        numerator += float(row["vw"]) * volume
        denominator += volume
        result.append(numerator / denominator if denominator else float(row["c"]))
    return result


def _candidate(
    *, day: str, symbol: str, rows: Sequence[Mapping[str, Any]]
) -> dict[str, Any]:
    opening = rows[:5]
    if float(opening[-1]["c"]) <= float(opening[0]["o"]):
        return {"date": day, "symbol": symbol, "status": "opening_not_bullish"}
    range_high = max(float(row["h"]) for row in opening)
    range_low = min(float(row["l"]) for row in opening)
    cumulative_vwap = _cumulative_vwap(rows)
    trigger_index: int | None = None
    for index in range(5, len(rows) - 1):
        observed = _row_time(rows[index]).time().replace(tzinfo=None)
        if observed < TRIGGER_START:
            continue
        if observed > TRIGGER_END:
            break
        if (
            float(rows[index]["c"]) > range_high
            and float(rows[index]["c"]) > cumulative_vwap[index]
        ):
            trigger_index = index
            break
    if trigger_index is None:
        return {"date": day, "symbol": symbol, "status": "no_trigger"}
    entry_index = trigger_index + 1
    entry_open = float(rows[entry_index]["o"])
    if entry_open > range_high * 1.0015:
        return {
            "date": day,
            "symbol": symbol,
            "status": "missed_chase_cap",
            "trigger_time_et": rows[trigger_index]["t"],
        }
    stop_fraction = (entry_open - range_low) / entry_open
    if stop_fraction <= 0:
        return {
            "date": day,
            "symbol": symbol,
            "status": "nonpositive_stop_distance",
            "trigger_time_et": rows[trigger_index]["t"],
        }
    if stop_fraction > 0.012:
        return {
            "date": day,
            "symbol": symbol,
            "status": "stop_too_wide",
            "trigger_time_et": rows[trigger_index]["t"],
        }
    return {
        "date": day,
        "symbol": symbol,
        "status": "executable",
        "trigger_index": trigger_index,
        "entry_index": entry_index,
        "trigger_time_et": rows[trigger_index]["t"],
        "entry_time_et": rows[entry_index]["t"],
        "breakout_fraction": float(rows[trigger_index]["c"]) / range_high - 1,
        "entry_open": entry_open,
        "stop": range_low,
    }


def _trade_outcome(
    candidate: Mapping[str, Any],
    rows: Sequence[Mapping[str, Any]],
    cost_bps: int,
) -> dict[str, Any]:
    entry_index = int(candidate["entry_index"])
    raw_entry = float(candidate["entry_open"])
    stop = float(candidate["stop"])
    target = raw_entry + 2 * (raw_entry - stop)
    exit_price: float | None = None
    exit_reason = ""
    exit_time = ""
    for index in range(entry_index, len(rows)):
        row = rows[index]
        observed_time = _row_time(row).time().replace(tzinfo=None)
        open_price = float(row["o"])
        if open_price <= stop:
            exit_price = open_price
            exit_reason = "stop_gap" if index > entry_index else "stop"
        elif observed_time >= FORCE_FLAT:
            exit_price = open_price
            exit_reason = "force_flat"
        else:
            stop_hit = float(row["l"]) <= stop
            target_hit = float(row["h"]) >= target
            if stop_hit:
                exit_price = stop
                exit_reason = "stop_first" if target_hit else "stop"
            elif target_hit:
                exit_price = target
                exit_reason = "target"
        if exit_price is not None:
            exit_time = str(row["t"])
            break
    if exit_price is None:
        raise EtfOrbStage0Error("trade did not resolve by the force-flat boundary")
    cost = cost_bps / 10_000
    entry_fill = raw_entry * (1 + cost)
    exit_fill = exit_price * (1 - cost)
    planned_stop_fill = stop * (1 - cost)
    planned_risk = entry_fill - planned_stop_fill
    if planned_risk <= 0:
        raise EtfOrbStage0Error("cost-adjusted planned risk is not positive")
    return {
        "cost_bps_per_side": cost_bps,
        "net_r": (exit_fill - entry_fill) / planned_risk,
        "exit_reason": exit_reason,
        "exit_time_et": exit_time,
        "stop_executed": exit_reason.startswith("stop"),
    }


def _profit_factor(values: Sequence[float]) -> float | None:
    gains = sum(value for value in values if value > 0)
    losses = abs(sum(value for value in values if value < 0))
    if not values or (not losses and not gains):
        return None
    if not losses:
        return None
    return gains / losses


def _drawdown(values: Sequence[float]) -> float:
    equity = peak = maximum = 0.0
    for value in values:
        equity += value
        peak = max(peak, equity)
        maximum = max(maximum, peak - equity)
    return maximum


def _metrics(values: Sequence[float]) -> dict[str, Any]:
    gains = sum(value for value in values if value > 0)
    losses = abs(sum(value for value in values if value < 0))
    return {
        "signals": len(values),
        "total_r": sum(values),
        "expectancy_r": statistics.fmean(values) if values else None,
        "profit_factor": _profit_factor(values),
        "profit_factor_infinite": bool(values and gains > 0 and losses == 0),
        "maximum_drawdown_r": _drawdown(values),
        "win_rate": (
            sum(value > 0 for value in values) / len(values) if values else None
        ),
    }


def build_result(
    manifest_path: Path,
    inspection_path: Path,
    store: HistoricalDayStore | None = None,
    *,
    require_published: bool = True,
) -> dict[str, Any]:
    source = store or HistoricalDayStore.from_env()
    manifest = _load_json(manifest_path)
    _validate_manifest_identity(manifest)
    inspection = _validate_inspection(manifest_path, inspection_path, source)
    if require_published:
        _require_published((manifest_path, inspection_path))
    bars = _load_manifest_bars(manifest, source)
    disposition_counts: Counter[str] = Counter()
    records: list[dict[str, Any]] = []
    no_trade_dates = 0
    for session in manifest["inputs"]:
        day = str(session["date"])
        candidates = [
            _candidate(day=day, symbol=symbol, rows=bars[(day, symbol)])
            for symbol in SYMBOLS
        ]
        disposition_counts.update(item["status"] for item in candidates)
        executable = [item for item in candidates if item["status"] == "executable"]
        executable.sort(
            key=lambda item: (
                str(item["entry_time_et"]),
                -float(item["breakout_fraction"]),
                str(item["symbol"]),
            )
        )
        if not executable:
            no_trade_dates += 1
            continue
        selected = executable[0]
        disposition_counts["selected"] += 1
        disposition_counts["not_selected_daily_cap"] += len(executable) - 1
        outcomes = {
            str(cost): _trade_outcome(
                selected,
                bars[(day, str(selected["symbol"]))],
                cost,
            )
            for cost in (PRIMARY_COST_BPS, *STRESS_COST_BPS)
        }
        primary = outcomes[str(PRIMARY_COST_BPS)]
        records.append(
            {
                "date": day,
                "symbol": selected["symbol"],
                "trigger_time_et": selected["trigger_time_et"],
                "entry_time_et": selected["entry_time_et"],
                "exit_time_et": primary["exit_time_et"],
                "exit_reason": primary["exit_reason"],
                "stop_executed": primary["stop_executed"],
                "net_r": primary["net_r"],
                "stress_10bps_r": outcomes["10"]["net_r"],
                "stress_20bps_r": outcomes["20"]["net_r"],
            }
        )
    records.sort(key=lambda item: (item["date"], item["symbol"]))
    primary_values = [float(item["net_r"]) for item in records]
    stress_10 = [float(item["stress_10bps_r"]) for item in records]
    stress_20 = [float(item["stress_20bps_r"]) for item in records]
    primary_metrics = _metrics(primary_values)
    stress_metrics = {"10": _metrics(stress_10), "20": _metrics(stress_20)}
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
        "inspection_sha256": inspection["inspection_sha256"],
        "implementation_sha256": manifest["implementation_sha256"],
        "claim_scope": "FALSIFICATION_ONLY",
        "development_evidence_eligible": False,
        "confirmation_evidence_eligible": False,
        "provider_requests": 0,
        "broker_actions": 0,
        "denominator": {
            "dates": manifest["denominator"]["included_dates"],
            "symbol_sessions": manifest["denominator"]["included_symbol_sessions"],
            "closed_signals": len(records),
            "no_trade_dates": no_trade_dates,
            "rule_violations": 0,
        },
        "disposition_counts": dict(sorted(disposition_counts.items())),
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
    result["result_sha256"] = _self_hash(result, "result_sha256")
    return result


def inspect_result(
    manifest_path: Path,
    inspection_path: Path,
    result_path: Path,
    store: HistoricalDayStore | None = None,
) -> dict[str, Any]:
    recorded = _load_json(result_path)
    if recorded.get("result_sha256") != _self_hash(recorded, "result_sha256"):
        raise EtfOrbStage0Error("Stage 0 result content hash is invalid")
    rebuilt = build_result(
        manifest_path,
        inspection_path,
        store,
        require_published=False,
    )
    if recorded != rebuilt:
        raise EtfOrbStage0Error("Stage 0 result does not independently rebuild")
    audit: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "inspection_kind": "stage0-result-inspection",
        "variant_id": VARIANT_ID,
        "manifest_sha256": recorded["manifest_sha256"],
        "input_inspection_sha256": recorded["inspection_sha256"],
        "result_sha256": recorded["result_sha256"],
        "result_file_sha256": sha256_file(result_path),
        "closed_signals": recorded["denominator"]["closed_signals"],
        "stage0_survived": recorded["stage0_survived"],
        "maturity_effect": "NONE",
        "provider_requests": 0,
        "broker_actions": 0,
        "valid": True,
    }
    audit["inspection_sha256"] = _self_hash(audit, "inspection_sha256")
    return audit


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("freeze", help="build the exact activation manifest")
    inspect = commands.add_parser("inspect", help="inspect inputs without returns")
    inspect.add_argument("manifest", type=Path)
    evaluate = commands.add_parser("evaluate", help="evaluate the inspected Stage 0")
    evaluate.add_argument("manifest", type=Path)
    evaluate.add_argument("inspection", type=Path)
    audit = commands.add_parser("inspect-result", help="independently rebuild a result")
    audit.add_argument("manifest", type=Path)
    audit.add_argument("inspection", type=Path)
    audit.add_argument("result", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "freeze":
            result = build_manifest()
        elif args.command == "inspect":
            result = inspect_activation(args.manifest)
        elif args.command == "evaluate":
            result = build_result(args.manifest, args.inspection)
        else:
            result = inspect_result(args.manifest, args.inspection, args.result)
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    except (EtfOrbStage0Error, OSError, ValueError) as exc:
        print(
            json.dumps({"error": str(exc), "error_type": type(exc).__name__}, indent=2),
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

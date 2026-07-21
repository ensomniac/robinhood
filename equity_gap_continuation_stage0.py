"""Freeze and evaluate the preregistered equity gap-continuation Stage 0 trial."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import statistics
import sys
from collections import Counter
from collections.abc import Mapping, Sequence
from datetime import time
from pathlib import Path
from typing import Any

import etf_or_momentum_stage0 as common
from historical_research import load_dataset
from historical_store import HistoricalDayStore, sha256_file
from portfolio_tournament import inspect_manifest as inspect_slate


PROJECT_ROOT = Path(__file__).resolve().parent
SLATE_PATH = common.SLATE_PATH
EVIDENCE_PATH = (
    PROJECT_ROOT / "historical_batches" / "evidence-2026-07-16-one-hundred-days.json"
)
COLLECTION_PATH = (
    PROJECT_ROOT
    / "historical_batches"
    / "collection-2026-07-16-one-hundred-days.json"
)
BUNDLE_ROOT = PROJECT_ROOT / "historical_data"
VARIANT_ID = "equity-gap-continuation-v1"
VARIANT_ORDINAL = 3
SCHEMA_VERSION = 1
PRIMARY_COST_BPS = 5
STRESS_COST_BPS = (10, 20)
SIGNAL_START = time(9, 45)
SIGNAL_END = time(11, 30)
FORCE_FLAT = time(15, 50)
OPENING_RANGE_BARS = 15
VOLUME_LOOKBACK_BARS = 15


class EquityGapStage0Error(RuntimeError):
    """The equity gap-continuation Stage 0 evidence is invalid."""


def _load_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise EquityGapStage0Error(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise EquityGapStage0Error(f"{path} must contain an object")
    return value


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode()


def _variant() -> dict[str, Any]:
    inspect_slate(SLATE_PATH)
    matches = [
        item
        for item in _load_json(SLATE_PATH)["variants"]
        if item.get("variant_id") == VARIANT_ID
        and item.get("variant_ordinal") == VARIANT_ORDINAL
    ]
    if len(matches) != 1:
        raise EquityGapStage0Error("frozen slate variant identity is unavailable")
    return dict(matches[0])


def _expected_frozen_hashes() -> dict[str, str]:
    evidence = _load_json(EVIDENCE_PATH)
    scanner = evidence.get("scanner")
    candidates_by_date = evidence.get("candidates_by_date")
    if not isinstance(scanner, Mapping) or not isinstance(candidates_by_date, Mapping):
        raise EquityGapStage0Error("legacy evidence manifest is malformed")
    result: dict[str, str] = {}
    for day, candidates in candidates_by_date.items():
        if not isinstance(day, str) or not isinstance(candidates, list) or not candidates:
            raise EquityGapStage0Error("legacy candidate denominator is malformed")
        result[day] = hashlib.sha256(
            _canonical_bytes(
                {
                    "date": day,
                    "candidates": candidates,
                    "scanner": dict(scanner),
                }
            )
        ).hexdigest()
    return result


def _previous_session(bundle: Mapping[str, Any], day: str) -> str:
    source = bundle.get("source")
    scanner = source.get("scanner_definition") if isinstance(source, Mapping) else None
    dates = scanner.get("dates") if isinstance(scanner, Mapping) else None
    row = dates.get(day) if isinstance(dates, Mapping) else None
    sessions = row.get("source_sessions") if isinstance(row, Mapping) else None
    if (
        not isinstance(sessions, list)
        or len(sessions) < 2
        or sessions[-1] != day
        or not isinstance(sessions[-2], str)
    ):
        raise EquityGapStage0Error(f"{day}: prior session identity is unavailable")
    return str(sessions[-2])


def _prior_close_input(
    store: HistoricalDayStore, symbol: str, prior_day: str
) -> dict[str, Any]:
    path = store.path_for(symbol, prior_day)
    dataset = store.select_dataset(
        symbol,
        prior_day,
        kind="bars",
        channel="trades",
        timeframe="1d",
        providers=("ibkr",),
        require_complete=True,
        adjustment="provider_adjusted_unknown_basis",
    )
    if dataset is None or not path.is_file():
        return {"status": "missing", "prior_date": prior_day}
    rows = dataset.get("rows")
    if not isinstance(rows, list) or len(rows) != 1:
        return {"status": "missing", "prior_date": prior_day}
    close = rows[0].get("c")
    if isinstance(close, bool) or not isinstance(close, (int, float)) or close <= 0:
        return {"status": "missing", "prior_date": prior_day}
    return {
        "status": "available",
        "prior_date": prior_day,
        "store_key": path.relative_to(store.root).as_posix(),
        "document_sha256": sha256_file(path),
        "dataset_id": dataset["id"],
        "dataset_sha256": dataset["content_sha256"],
        "prior_close": float(close),
    }


def _load_bundle(path: Path, day: str) -> dict[str, Any]:
    bundle = _load_json(path)
    if bundle.get("date") != day or bundle.get("session_capture_complete") is not True:
        raise EquityGapStage0Error(f"{day}: bundle identity or capture is invalid")
    source = bundle.get("source")
    required = (
        "point_in_time",
        "regular_hours_only",
        "split_adjusted",
        "universe_capture_complete",
    )
    if not isinstance(source, Mapping) or any(
        source.get(field) is not True for field in required
    ):
        raise EquityGapStage0Error(f"{day}: bundle source attestations are incomplete")
    return bundle


def build_manifest(store: HistoricalDayStore | None = None) -> dict[str, Any]:
    source = store or HistoricalDayStore.from_env()
    dataset = load_dataset(EVIDENCE_PATH, BUNDLE_ROOT)
    collection = _load_json(COLLECTION_PATH)
    variant = _variant()
    expected_hashes = _expected_frozen_hashes()
    blocked = {
        str(item["date"]): {
            "date": str(item["date"]),
            "reason_code": str(item["reason_code"]),
            "detail": str(item["detail"]),
        }
        for item in collection.get("blocked_dates", [])
        if isinstance(item, Mapping)
    }
    missing = {str(item["date"]) for item in dataset["bundles"] if item["status"] == "missing"}
    if missing != set(blocked) or len(dataset["dates"]) != 100:
        raise EquityGapStage0Error("legacy 95-ready/5-blocked denominator drifted")
    inputs: list[dict[str, Any]] = []
    prior_available = 0
    for item in dataset["bundles"]:
        if item["status"] != "available":
            continue
        day = str(item["date"])
        path = Path(str(item["path"]))
        bundle = _load_bundle(path, day)
        candidates = bundle.get("candidates")
        if not isinstance(candidates, list) or len(candidates) != 10:
            raise EquityGapStage0Error(f"{day}: candidate denominator is not exactly ten")
        symbols = [str(candidate.get("symbol", "")) for candidate in candidates]
        if symbols != item["expected_symbols"]:
            raise EquityGapStage0Error(f"{day}: candidate order drifted")
        frozen_hash = bundle["source"].get("frozen_evidence_sha256")
        if frozen_hash != expected_hashes.get(day):
            raise EquityGapStage0Error(f"{day}: frozen evidence identity drifted")
        prior_day = _previous_session(bundle, day)
        candidate_inputs = []
        for symbol in symbols:
            prior = _prior_close_input(source, symbol, prior_day)
            prior_available += prior["status"] == "available"
            candidate_inputs.append({"symbol": symbol, "prior_close_input": prior})
        inputs.append(
            {
                "date": day,
                "bundle_path": path.relative_to(PROJECT_ROOT).as_posix(),
                "bundle_sha256": str(item["sha256"]),
                "frozen_evidence_sha256": frozen_hash,
                "ordered_symbols": symbols,
                "candidate_inputs": candidate_inputs,
            }
        )
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
        "legacy_evidence_sha256": sha256_file(EVIDENCE_PATH),
        "legacy_collection_sha256": sha256_file(COLLECTION_PATH),
        "legacy_dataset_hash": dataset["dataset_hash"],
        "claim_scope": "FALSIFICATION_ONLY",
        "outcomes_previously_accessed": True,
        "development_evidence_eligible": False,
        "confirmation_evidence_eligible": False,
        "provider_requests_authorized": False,
        "broker_actions_authorized": False,
        "return_evaluation_authorized_before_inspection": False,
        "source_selection": {
            "selection_boundary": "exact catalyst-selected ten-candidate daily universe",
            "date_rule": "all 100 frozen dates with no substitutions",
            "candidate_order_rule": "exact evidence order",
            "prior_close_rule": "IBKR complete daily summary for the prior exchange session named by the frozen bundle",
            "substitutions": 0,
        },
        "denominator": {
            "requested_dates": 100,
            "included_dates": len(inputs),
            "excluded_dates": len(blocked),
            "included_symbol_sessions": len(inputs) * 10,
            "prior_close_available_symbol_sessions": prior_available,
            "blocked_dates": [blocked[day] for day in dataset["dates"] if day in blocked],
        },
        "selection_contract": {
            "asset_gate": "frozen candidate is attested common stock with 09:30 open strictly above 5 dollars",
            "gap_gate": "09:30 open divided by prior completed regular-session close minus one is within 0.02 through 0.08 inclusive",
            "opening_range": "high and low of completed 09:30 through 09:44 one-minute bars",
            "session_vwap": "cumulative volume-weighted typical price (high plus low plus close divided by three) through the completed signal bar",
            "breakout_volume_multiple": "signal-bar volume divided by mean volume of the prior 15 completed one-minute bars; minimum 1.5",
            "trigger": "completed close strictly above first-15-minute high and session VWAP",
            "signal_bar_starts_et": "09:45:00-11:30:00 inclusive",
            "entry": "next observed one-minute open",
            "daily_selection": "earliest entry, then highest breakout-volume multiple, then largest gap, then lexical symbol",
            "miss_policy": "missing prior close, invalid bars, or nonpositive stop distance remains in denominator and cannot signal",
        },
        "outcome_contract": {
            "stop": "first-15-minute low",
            "target": "two times raw entry-to-stop distance above raw entry",
            "entry_exit_cost_bps_per_side": [PRIMARY_COST_BPS, *STRESS_COST_BPS],
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
        "inputs": inputs,
    }
    manifest["activation_rules_hash"] = common._hash(
        {
            "base_rules_hash": manifest["base_rules_hash"],
            "source_selection": manifest["source_selection"],
            "selection_contract": manifest["selection_contract"],
            "outcome_contract": manifest["outcome_contract"],
            "stage0_gate": manifest["stage0_gate"],
        }
    )
    manifest["manifest_sha256"] = common._self_hash(manifest, "manifest_sha256")
    return manifest


def _validate_manifest(recorded: Mapping[str, Any]) -> None:
    if recorded.get("manifest_sha256") != common._self_hash(
        recorded, "manifest_sha256"
    ):
        raise EquityGapStage0Error("activation manifest content hash is invalid")
    if (
        recorded.get("variant_id") != VARIANT_ID
        or recorded.get("variant_ordinal") != VARIANT_ORDINAL
        or recorded.get("claim_scope") != "FALSIFICATION_ONLY"
    ):
        raise EquityGapStage0Error("activation manifest identity is invalid")
    for field in (
        "development_evidence_eligible",
        "confirmation_evidence_eligible",
        "provider_requests_authorized",
        "broker_actions_authorized",
        "return_evaluation_authorized_before_inspection",
    ):
        if recorded.get(field) is not False:
            raise EquityGapStage0Error(f"activation manifest {field} must be false")
    denominator = recorded.get("denominator", {})
    if (
        denominator.get("requested_dates") != 100
        or denominator.get("included_dates") != 95
        or denominator.get("excluded_dates") != 5
        or denominator.get("included_symbol_sessions") != 950
    ):
        raise EquityGapStage0Error("activation denominator is not exactly 100/95/5/950")


def _bar_time(row: Mapping[str, Any]) -> time:
    try:
        return time.fromisoformat(str(row["time_et"]))
    except (KeyError, ValueError) as exc:
        raise EquityGapStage0Error("bar timestamp is invalid") from exc


def _validate_bars(value: Any, symbol: str, day: str) -> list[dict[str, Any]]:
    if not isinstance(value, list) or len(value) != 390:
        raise EquityGapStage0Error(f"{day} {symbol}: expected 390 one-minute bars")
    bars: list[dict[str, Any]] = []
    for index, raw in enumerate(value):
        if not isinstance(raw, Mapping):
            raise EquityGapStage0Error(f"{day} {symbol}: malformed bar")
        expected = 9 * 60 + 30 + index
        observed = _bar_time(raw)
        if observed.hour * 60 + observed.minute != expected or observed.second != 0:
            raise EquityGapStage0Error(f"{day} {symbol}: noncontiguous bars")
        if raw.get("interpolated") is not False:
            raise EquityGapStage0Error(f"{day} {symbol}: interpolated bar is forbidden")
        try:
            o, h, low, close = (
                float(raw[field]) for field in ("open", "high", "low", "close")
            )
            volume = int(raw["volume"])
        except (KeyError, TypeError, ValueError) as exc:
            raise EquityGapStage0Error(f"{day} {symbol}: malformed OHLCV") from exc
        if min(o, h, low, close) <= 0 or low > min(o, close) or h < max(o, close):
            raise EquityGapStage0Error(f"{day} {symbol}: invalid OHLC")
        if volume < 0:
            raise EquityGapStage0Error(f"{day} {symbol}: negative volume")
        bars.append(dict(raw))
    return bars


def _verify_prior(
    store: HistoricalDayStore, symbol: str, value: Mapping[str, Any]
) -> float | None:
    if value.get("status") == "missing":
        return None
    prior_day = str(value.get("prior_date"))
    path = store.path_for(symbol, prior_day)
    if (
        value.get("store_key") != path.relative_to(store.root).as_posix()
        or not path.is_file()
        or value.get("document_sha256") != sha256_file(path)
    ):
        raise EquityGapStage0Error(f"{prior_day} {symbol}: prior document drifted")
    dataset = store.select_dataset(
        symbol,
        prior_day,
        kind="bars",
        channel="trades",
        timeframe="1d",
        providers=("ibkr",),
        require_complete=True,
        adjustment="provider_adjusted_unknown_basis",
    )
    if dataset is None or (
        value.get("dataset_id") != dataset.get("id")
        or value.get("dataset_sha256") != dataset.get("content_sha256")
    ):
        raise EquityGapStage0Error(f"{prior_day} {symbol}: prior dataset drifted")
    rows = dataset.get("rows")
    if not isinstance(rows, list) or len(rows) != 1:
        raise EquityGapStage0Error(f"{prior_day} {symbol}: prior close is malformed")
    close = float(rows[0]["c"])
    if float(value.get("prior_close")) != close:
        raise EquityGapStage0Error(f"{prior_day} {symbol}: prior close drifted")
    return close


def _load_inputs(
    manifest: Mapping[str, Any], store: HistoricalDayStore
) -> list[dict[str, Any]]:
    sessions: list[dict[str, Any]] = []
    for item in manifest["inputs"]:
        day = str(item["date"])
        path = PROJECT_ROOT / str(item["bundle_path"])
        if not path.is_file() or sha256_file(path) != item["bundle_sha256"]:
            raise EquityGapStage0Error(f"{day}: frozen bundle drifted")
        bundle = _load_bundle(path, day)
        candidates = bundle.get("candidates")
        if not isinstance(candidates, list):
            raise EquityGapStage0Error(f"{day}: candidates are malformed")
        symbols = [str(candidate.get("symbol", "")) for candidate in candidates]
        if symbols != item["ordered_symbols"]:
            raise EquityGapStage0Error(f"{day}: frozen candidate order drifted")
        inputs = item.get("candidate_inputs")
        if not isinstance(inputs, list) or len(inputs) != len(candidates):
            raise EquityGapStage0Error(f"{day}: prior-close inputs are incomplete")
        loaded = []
        for raw, source_input in zip(candidates, inputs, strict=True):
            symbol = str(raw["symbol"])
            if source_input.get("symbol") != symbol:
                raise EquityGapStage0Error(f"{day}: prior-close symbol order drifted")
            prior = source_input.get("prior_close_input")
            if not isinstance(prior, Mapping):
                raise EquityGapStage0Error(f"{day} {symbol}: prior-close input is malformed")
            loaded.append(
                {
                    "raw": raw,
                    "symbol": symbol,
                    "bars": _validate_bars(raw.get("bars"), symbol, day),
                    "prior_close": _verify_prior(store, symbol, prior),
                }
            )
        sessions.append({"date": day, "candidates": loaded})
    return sessions


def inspect_activation(
    path: Path, store: HistoricalDayStore | None = None
) -> dict[str, Any]:
    recorded = _load_json(path)
    _validate_manifest(recorded)
    source = store or HistoricalDayStore.from_env()
    if recorded != build_manifest(source):
        raise EquityGapStage0Error("activation manifest does not exactly rebuild")
    sessions = _load_inputs(recorded, source)
    result: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "inspection_kind": "stage0-activation-input-inspection",
        "variant_id": VARIANT_ID,
        "manifest_sha256": recorded["manifest_sha256"],
        "manifest_file_sha256": sha256_file(path),
        "implementation_sha256": recorded["implementation_sha256"],
        "requested_dates": recorded["denominator"]["requested_dates"],
        "selected_dates": len(sessions),
        "selected_symbol_sessions": sum(len(item["candidates"]) for item in sessions),
        "selected_bars": sum(
            len(candidate["bars"])
            for item in sessions
            for candidate in item["candidates"]
        ),
        "excluded_dates": recorded["denominator"]["excluded_dates"],
        "prior_close_available_symbol_sessions": sum(
            candidate["prior_close"] is not None
            for item in sessions
            for candidate in item["candidates"]
        ),
        "provider_requests": 0,
        "broker_actions": 0,
        "returns_computed": 0,
        "return_evaluation_authorized": True,
        "claim_scope": "FALSIFICATION_ONLY",
        "valid": True,
    }
    result["inspection_sha256"] = common._self_hash(result, "inspection_sha256")
    return result


def _validate_inspection(
    manifest_path: Path, inspection_path: Path, store: HistoricalDayStore
) -> dict[str, Any]:
    recorded = _load_json(inspection_path)
    if recorded.get("inspection_sha256") != common._self_hash(
        recorded, "inspection_sha256"
    ):
        raise EquityGapStage0Error("activation inspection content hash is invalid")
    if recorded != inspect_activation(manifest_path, store):
        raise EquityGapStage0Error("activation inspection does not exactly rebuild")
    if recorded.get("return_evaluation_authorized") is not True:
        raise EquityGapStage0Error("activation inspection does not authorize evaluation")
    return recorded


def _cumulative_vwap(rows: Sequence[Mapping[str, Any]]) -> list[float]:
    numerator = 0.0
    denominator = 0
    result = []
    for row in rows:
        volume = int(row["volume"])
        typical = (
            float(row["high"]) + float(row["low"]) + float(row["close"])
        ) / 3
        numerator += typical * volume
        denominator += volume
        result.append(numerator / denominator if denominator else float(row["close"]))
    return result


def _candidate(
    *, day: str, raw: Mapping[str, Any], prior_close: float | None
) -> dict[str, Any]:
    symbol = str(raw.get("symbol", ""))
    bars = raw.get("bars")
    if not isinstance(bars, list):
        raise EquityGapStage0Error(f"{day} {symbol}: bars are malformed")
    payload = raw.get("evaluation_payload")
    candidate = payload.get("candidate") if isinstance(payload, Mapping) else None
    if not isinstance(candidate, Mapping) or candidate.get("is_common_stock") is not True:
        return {"date": day, "symbol": symbol, "status": "not_common_stock"}
    opening_price = float(bars[0]["open"])
    if opening_price <= 5:
        return {"date": day, "symbol": symbol, "status": "opening_price_not_above_5"}
    if prior_close is None:
        return {"date": day, "symbol": symbol, "status": "prior_close_missing"}
    gap_fraction = opening_price / prior_close - 1
    if gap_fraction < 0.02 or gap_fraction > 0.08:
        return {"date": day, "symbol": symbol, "status": "gap_outside_range"}
    opening = bars[:OPENING_RANGE_BARS]
    range_high = max(float(row["high"]) for row in opening)
    range_low = min(float(row["low"]) for row in opening)
    vwap = _cumulative_vwap(bars)
    for index in range(OPENING_RANGE_BARS, len(bars) - 1):
        observed = _bar_time(bars[index])
        if observed < SIGNAL_START:
            continue
        if observed > SIGNAL_END:
            break
        prior_volumes = [
            int(row["volume"])
            for row in bars[index - VOLUME_LOOKBACK_BARS : index]
        ]
        mean_volume = statistics.fmean(prior_volumes)
        volume_multiple = (
            int(bars[index]["volume"]) / mean_volume if mean_volume > 0 else 0.0
        )
        if (
            float(bars[index]["close"]) <= range_high
            or float(bars[index]["close"]) <= vwap[index]
            or volume_multiple < 1.5
        ):
            continue
        entry_index = index + 1
        entry_open = float(bars[entry_index]["open"])
        if entry_open <= range_low:
            return {
                "date": day,
                "symbol": symbol,
                "status": "nonpositive_stop_distance",
                "trigger_time_et": bars[index]["time_et"],
            }
        return {
            "date": day,
            "symbol": symbol,
            "status": "executable",
            "gap_fraction": gap_fraction,
            "volume_multiple": volume_multiple,
            "trigger_index": index,
            "entry_index": entry_index,
            "trigger_time_et": bars[index]["time_et"],
            "entry_time_et": bars[entry_index]["time_et"],
            "entry_open": entry_open,
            "stop": range_low,
        }
    return {"date": day, "symbol": symbol, "status": "no_breakout_trigger"}


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
        observed = _bar_time(row)
        open_price = float(row["open"])
        if open_price <= stop:
            exit_price = open_price
            exit_reason = "stop_gap" if index > entry_index else "stop"
        elif observed >= FORCE_FLAT:
            exit_price = open_price
            exit_reason = "force_flat"
        else:
            stop_hit = float(row["low"]) <= stop
            target_hit = float(row["high"]) >= target
            if stop_hit:
                exit_price = stop
                exit_reason = "stop_first" if target_hit else "stop"
            elif target_hit:
                exit_price = target
                exit_reason = "target"
        if exit_price is not None:
            exit_time = str(row["time_et"])
            break
    if exit_price is None:
        raise EquityGapStage0Error("trade did not resolve by force-flat")
    cost = cost_bps / 10_000
    entry_fill = raw_entry * (1 + cost)
    exit_fill = exit_price * (1 - cost)
    stop_fill = stop * (1 - cost)
    planned_risk = entry_fill - stop_fill
    if planned_risk <= 0:
        raise EquityGapStage0Error("cost-adjusted planned risk is not positive")
    return {
        "cost_bps_per_side": cost_bps,
        "net_r": (exit_fill - entry_fill) / planned_risk,
        "exit_reason": exit_reason,
        "exit_time_et": exit_time,
        "stop_executed": exit_reason.startswith("stop"),
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
    _validate_manifest(manifest)
    inspection = _validate_inspection(manifest_path, inspection_path, source)
    if require_published:
        common._require_published((manifest_path, inspection_path))
    sessions = _load_inputs(manifest, source)
    dispositions: Counter[str] = Counter()
    records: list[dict[str, Any]] = []
    no_trade_dates = 0
    for session in sessions:
        day = str(session["date"])
        candidates = [
            _candidate(
                day=day,
                raw=item["raw"],
                prior_close=item["prior_close"],
            )
            for item in session["candidates"]
        ]
        dispositions.update(item["status"] for item in candidates)
        executable = [item for item in candidates if item["status"] == "executable"]
        executable.sort(
            key=lambda item: (
                str(item["entry_time_et"]),
                -float(item["volume_multiple"]),
                -float(item["gap_fraction"]),
                str(item["symbol"]),
            )
        )
        if not executable:
            no_trade_dates += 1
            continue
        selected = executable[0]
        dispositions["selected"] += 1
        dispositions["not_selected_daily_cap"] += len(executable) - 1
        raw = next(
            item["raw"]
            for item in session["candidates"]
            if item["symbol"] == selected["symbol"]
        )
        outcomes = {
            str(cost): _trade_outcome(selected, raw["bars"], cost)
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
    values = [float(item["net_r"]) for item in records]
    stress_10 = [float(item["stress_10bps_r"]) for item in records]
    stress_20 = [float(item["stress_20bps_r"]) for item in records]
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
        "inspection_sha256": inspection["inspection_sha256"],
        "implementation_sha256": manifest["implementation_sha256"],
        "claim_scope": "FALSIFICATION_ONLY",
        "development_evidence_eligible": False,
        "confirmation_evidence_eligible": False,
        "provider_requests": 0,
        "broker_actions": 0,
        "denominator": {
            "requested_dates": manifest["denominator"]["requested_dates"],
            "dates": manifest["denominator"]["included_dates"],
            "excluded_dates": manifest["denominator"]["excluded_dates"],
            "symbol_sessions": manifest["denominator"]["included_symbol_sessions"],
            "closed_signals": len(records),
            "no_trade_dates": no_trade_dates,
            "rule_violations": 0,
        },
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
    inspection_path: Path,
    result_path: Path,
    store: HistoricalDayStore | None = None,
) -> dict[str, Any]:
    recorded = _load_json(result_path)
    if recorded.get("result_sha256") != common._self_hash(recorded, "result_sha256"):
        raise EquityGapStage0Error("Stage 0 result content hash is invalid")
    rebuilt = build_result(
        manifest_path,
        inspection_path,
        store,
        require_published=False,
    )
    if recorded != rebuilt:
        raise EquityGapStage0Error("Stage 0 result does not independently rebuild")
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
    audit["inspection_sha256"] = common._self_hash(audit, "inspection_sha256")
    return audit


def _publish(value: Mapping[str, Any], path: Path) -> None:
    if "inspection_kind" in value:
        identity = str(value.get("inspection_sha256", ""))
    elif "result_kind" in value:
        identity = str(value.get("result_sha256", ""))
    else:
        identity = str(value.get("manifest_sha256", ""))
    if not identity or not path.name.endswith(f"-{identity}.json"):
        raise EquityGapStage0Error("output filename must end with its content hash")
    resolved = path.resolve()
    allowed = (
        (PROJECT_ROOT / "strategy_tournament" / "activations").resolve(),
        (PROJECT_ROOT / "strategy_tournament" / "inspections").resolve(),
        (PROJECT_ROOT / "research_results").resolve(),
    )
    if not any(parent == resolved.parent for parent in allowed):
        raise EquityGapStage0Error("output path is outside an approved evidence directory")
    resolved.parent.mkdir(parents=True, exist_ok=True)
    temporary = resolved.with_name(f".{resolved.name}.{os.getpid()}.tmp")
    try:
        temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
        os.replace(temporary, resolved)
    finally:
        temporary.unlink(missing_ok=True)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    freeze = commands.add_parser("freeze")
    freeze.add_argument("--output", type=Path)
    inspect = commands.add_parser("inspect")
    inspect.add_argument("manifest", type=Path)
    inspect.add_argument("--output", type=Path)
    evaluate = commands.add_parser("evaluate")
    evaluate.add_argument("manifest", type=Path)
    evaluate.add_argument("inspection", type=Path)
    evaluate.add_argument("--output", type=Path)
    audit = commands.add_parser("inspect-result")
    audit.add_argument("manifest", type=Path)
    audit.add_argument("inspection", type=Path)
    audit.add_argument("result", type=Path)
    audit.add_argument("--output", type=Path)
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
        if args.output is not None:
            _publish(result, args.output)
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    except (EquityGapStage0Error, common.EtfOrbStage0Error, OSError, ValueError) as exc:
        print(
            json.dumps({"error": str(exc), "error_type": type(exc).__name__}, indent=2),
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

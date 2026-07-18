"""Prospective, no-order execution qualification for the Item 2.02 reversal.

The module consumes privacy-safe captures from the normal read-only discovery,
bar, NBBO, and book surfaces.  It has no account connector and no order-action
interface.  A capture is evaluated only after the session lifecycle is complete;
missing or delayed evidence becomes a visible miss or rule violation rather than
an assumed fill.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import re
import statistics
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from historical_research import _find_signal_point_in_time
from historical_research_strategies import CandidateContext, parse_bars, load_strategy


SCHEMA_VERSION = 1
CONTRACT_ID = "early-item-2.02-reversal-shadow-v1"
PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "shadow_results"
ET = ZoneInfo("America/New_York")
PUBLIC_ALIAS = re.compile(r"^\d{4}-\d{2}-\d{2}-[A-Z][A-Z0-9.]{0,9}-\d+$")
ORDER_ACTIONS_ALLOWED = False
EXTERNAL_WRITE_CAPABILITIES: tuple[str, ...] = ()
MAX_QUOTE_AGE_SECONDS = 5.0
MAX_SNAPSHOT_WINDOW_SECONDS = 10.0
MAX_DETECTION_DELAY_SECONDS = 5.0
MAX_ENTRY_OBSERVATION_DELAY_SECONDS = 15.0
MAX_MONITORING_GAP_SECONDS = 15.0
CHASE_CAP_FRACTION = 0.0015
OPERATING_SPREAD_FRACTION = 0.001
HARD_SPREAD_FRACTION = 0.0015
RISK_FRACTION = 0.0025
STOP_RESERVE_FRACTION = 0.001
ALLOCATION_CAP_FRACTION = 0.80
ALLOCATION_OBJECTIVE_FLOOR_FRACTION = 0.70
FORCE_FLAT_TIME_ET = "15:50:00"
TARGET_R = 2.0
FORBIDDEN_PRIVATE_KEYS = {
    "account_number",
    "broker_order_id",
    "client_ref_id",
    "confirmation_id",
    "cancellation_id",
    "replacement_id",
    "password",
    "token",
    "mfa",
}


class ShadowReversalError(RuntimeError):
    """Raised when a capture cannot satisfy the prospective evidence contract."""


def _canonical_json(value: Any) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode("utf-8")


def _sha256(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _atomic_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
    os.replace(temporary, path)


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ShadowReversalError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise ShadowReversalError(f"{path} must contain a JSON object")
    return value


def _number(value: Any, label: str, *, positive: bool = False) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ShadowReversalError(f"{label} must be numeric")
    result = float(value)
    if not math.isfinite(result) or (positive and result <= 0):
        raise ShadowReversalError(
            f"{label} must be finite" + (" and positive" if positive else "")
        )
    return result


def _timestamp(value: Any, label: str) -> datetime:
    if not isinstance(value, str):
        raise ShadowReversalError(f"{label} must be an ISO timestamp")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise ShadowReversalError(f"{label} must be an ISO timestamp") from exc
    if parsed.tzinfo is None:
        raise ShadowReversalError(f"{label} must include a timezone")
    return parsed


def _et_timestamp(day: str, time_et: str) -> datetime:
    try:
        parsed_day = date.fromisoformat(day)
        parsed_time = time.fromisoformat(time_et)
    except ValueError as exc:
        raise ShadowReversalError(f"invalid ET date/time {day} {time_et}") from exc
    return datetime.combine(parsed_day, parsed_time, tzinfo=ET)


def _quantile(values: Sequence[float], fraction: float) -> float:
    if not values:
        raise ShadowReversalError("quantile requires values")
    ordered = sorted(values)
    position = (len(ordered) - 1) * fraction
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    weight = position - lower
    return ordered[lower] * (1.0 - weight) + ordered[upper] * weight


def _assert_privacy_safe(value: Any, path: str = "capture") -> None:
    if isinstance(value, Mapping):
        for key, child in value.items():
            normalized = str(key).strip().lower()
            if normalized in FORBIDDEN_PRIVATE_KEYS:
                raise ShadowReversalError(
                    f"privacy-safe shadow capture cannot contain {path}.{key}"
                )
            _assert_privacy_safe(child, f"{path}.{key}")
    elif isinstance(value, list):
        for index, child in enumerate(value):
            _assert_privacy_safe(child, f"{path}[{index}]")


def _item_202_candidate(raw: Mapping[str, Any], day: str) -> tuple[str, str]:
    symbol = str(raw.get("symbol", "")).strip().upper()
    alias = str(raw.get("public_alias", "")).strip()
    if not symbol or not PUBLIC_ALIAS.fullmatch(alias) or not alias.startswith(
        f"{day}-{symbol}-"
    ):
        raise ShadowReversalError(
            f"{day}: candidate needs a matching privacy-safe public_alias"
        )
    catalyst = raw.get("catalyst")
    if not isinstance(catalyst, Mapping) or catalyst.get("point_in_time") is not True:
        raise ShadowReversalError(f"{alias}: catalyst must be point-in-time")
    items = {value.strip() for value in str(catalyst.get("filing_items", "")).split(",")}
    if "2.02" not in items:
        raise ShadowReversalError(f"{alias}: catalyst must include SEC Item 2.02")
    published = _timestamp(catalyst.get("published_at"), f"{alias}.published_at")
    if published.astimezone(ET) >= _et_timestamp(day, "09:30:00"):
        raise ShadowReversalError(
            f"{alias}: Item 2.02 evidence was not available before the open"
        )
    if not str(catalyst.get("source_url", "")).strip():
        raise ShadowReversalError(f"{alias}: catalyst source URL is missing")
    return symbol, alias


def _quote_snapshot(raw: Mapping[str, Any], label: str) -> dict[str, Any]:
    quoted = _timestamp(raw.get("quoted_at"), f"{label}.quoted_at")
    observed = _timestamp(raw.get("observed_at"), f"{label}.observed_at")
    age = (observed - quoted).total_seconds()
    bid = _number(raw.get("bid"), f"{label}.bid", positive=True)
    ask = _number(raw.get("ask"), f"{label}.ask", positive=True)
    bid_size = _number(raw.get("bid_size"), f"{label}.bid_size", positive=True)
    ask_size = _number(raw.get("ask_size"), f"{label}.ask_size", positive=True)
    midpoint = (bid + ask) / 2.0
    spread = (ask - bid) / midpoint
    return {
        "quoted_at": quoted,
        "observed_at": observed,
        "age_seconds": age,
        "bid": bid,
        "ask": ask,
        "bid_size": bid_size,
        "ask_size": ask_size,
        "spread_fraction": spread,
        "crossed": bid >= ask,
        "stale": age < 0 or age > MAX_QUOTE_AGE_SECONDS,
    }


def _market_path_point(raw: Mapping[str, Any], label: str) -> dict[str, Any]:
    snapshot = _quote_snapshot(raw, label)
    low_bid = _number(raw.get("interval_low_bid", snapshot["bid"]), f"{label}.interval_low_bid", positive=True)
    high_bid = _number(raw.get("interval_high_bid", snapshot["bid"]), f"{label}.interval_high_bid", positive=True)
    if low_bid > high_bid:
        raise ShadowReversalError(f"{label}: interval low exceeds interval high")
    snapshot.update({"interval_low_bid": low_bid, "interval_high_bid": high_bid})
    return snapshot


def _return_stats(values: Sequence[float]) -> dict[str, Any]:
    if not values:
        return {
            "signals": 0,
            "mean_r": None,
            "total_r": 0.0,
            "profit_factor": None,
            "maximum_drawdown_r": 0.0,
        }
    wins = [value for value in values if value > 0]
    losses = [value for value in values if value < 0]
    gross_loss = -sum(losses)
    equity = peak = maximum = 0.0
    for value in values:
        equity += value
        peak = max(peak, equity)
        maximum = max(maximum, peak - equity)
    return {
        "signals": len(values),
        "mean_r": round(statistics.fmean(values), 8),
        "total_r": round(sum(values), 8),
        "profit_factor": round(sum(wins) / gross_loss, 8) if gross_loss else None,
        "maximum_drawdown_r": round(maximum, 8),
    }


def _simulate_exit(
    path: Sequence[Mapping[str, Any]],
    *,
    entry: float,
    stop: float,
    target: float,
) -> dict[str, Any]:
    points = [
        _market_path_point(value, f"market_path[{index}]")
        for index, value in enumerate(path)
    ]
    if not points:
        raise ShadowReversalError("selected signal has no subsequent market path")
    if [value["observed_at"] for value in points] != sorted(
        value["observed_at"] for value in points
    ):
        raise ShadowReversalError("market path must be chronological")
    violations: list[str] = []
    if any(value["stale"] for value in points):
        violations.append("stale_monitoring_quote")
    if any(value["crossed"] for value in points):
        violations.append("crossed_monitoring_quote")
    exit_price = None
    exit_timestamp = None
    exit_reason = None
    ambiguity = False
    exit_index = None
    for index, point in enumerate(points):
        hit_stop = point["interval_low_bid"] <= stop
        hit_target = point["interval_high_bid"] >= target
        if hit_stop and hit_target:
            ambiguity = True
            exit_price = min(point["bid"], stop)
            exit_timestamp = point["observed_at"]
            exit_reason = "stop_first_ambiguous_interval"
            exit_index = index
            break
        if hit_stop:
            exit_price = min(point["bid"], stop)
            exit_timestamp = point["observed_at"]
            exit_reason = "stop"
            exit_index = index
            break
        if hit_target:
            exit_price = max(target, point["bid"])
            exit_timestamp = point["observed_at"]
            exit_reason = "target_2r"
            exit_index = index
            break
        if point["observed_at"].astimezone(ET).time() >= time(15, 50):
            exit_price = point["bid"]
            exit_timestamp = point["observed_at"]
            exit_reason = "force_flat_15_50"
            exit_index = index
            break
    if exit_price is None:
        violations.append("forced_flat_quote_missing")
        return {
            "complete": False,
            "violations": violations,
            "same_interval_ambiguity": ambiguity,
        }
    risk = entry - stop
    stop_slippage = (
        max(0.0, (stop - exit_price) / entry * 10_000.0)
        if "stop" in exit_reason
        else None
    )
    lifecycle_points = points[: int(exit_index) + 1]
    return {
        "complete": True,
        "exit_price": round(exit_price, 8),
        "exit_at": exit_timestamp.isoformat(),
        "exit_reason": exit_reason,
        "net_r": round((exit_price - entry) / risk, 8),
        "mfe_r": round((max(value["interval_high_bid"] for value in lifecycle_points) - entry) / risk, 8),
        "mae_r": round((min(value["interval_low_bid"] for value in lifecycle_points) - entry) / risk, 8),
        "modeled_stop_slippage_bps": round(stop_slippage, 8) if stop_slippage is not None else None,
        "same_interval_ambiguity": ambiguity,
        "violations": violations,
    }


def evaluate_shadow_session(capture: Mapping[str, Any]) -> dict[str, Any]:
    """Turn one complete read-only capture into a privacy-safe lifecycle record."""

    _assert_privacy_safe(capture)
    if capture.get("schema_version") != SCHEMA_VERSION or capture.get("mode") != "shadow":
        raise ShadowReversalError("capture must use schema 1 and mode=shadow")
    day = str(capture.get("date", ""))
    try:
        date.fromisoformat(day)
    except ValueError as exc:
        raise ShadowReversalError("capture.date must be YYYY-MM-DD") from exc
    frozen_at = _timestamp(
        capture.get("premarket_shortlist_frozen_at"),
        "premarket_shortlist_frozen_at",
    )
    if frozen_at.astimezone(ET) >= _et_timestamp(day, "09:30:00"):
        raise ShadowReversalError("premarket shortlist must be frozen before 09:30 ET")
    if capture.get("shortlist_complete") is not True:
        raise ShadowReversalError("shortlist_complete must be true")
    equity = _number(
        capture.get("simulation_account_equity"),
        "simulation_account_equity",
        positive=True,
    )
    buying_power = _number(
        capture.get("simulation_buying_power"),
        "simulation_buying_power",
        positive=True,
    )
    if buying_power < equity:
        raise ShadowReversalError("simulation buying power cannot be below equity")
    raw_candidates = capture.get("candidates")
    if not isinstance(raw_candidates, list) or not raw_candidates:
        raise ShadowReversalError("capture.candidates must be a non-empty array")
    plugin = load_strategy("opening-reversal")
    signals: list[tuple[int, float, str, str, Mapping[str, Any], Any, Any]] = []
    aliases: list[str] = []
    for raw in raw_candidates:
        if not isinstance(raw, Mapping):
            raise ShadowReversalError("each candidate must be an object")
        symbol, alias = _item_202_candidate(raw, day)
        if alias in aliases:
            raise ShadowReversalError("public aliases must be unique")
        aliases.append(alias)
        bars = parse_bars(raw.get("bars"))
        context = CandidateContext(day, symbol, alias, bars)
        search = _find_signal_point_in_time(plugin, context)
        if search.decision is None:
            continue
        decision = search.decision
        signal_time = bars[decision.signal_index].time_et
        if signal_time >= "09:40:00":
            continue
        signals.append(
            (
                decision.signal_index,
                -decision.strength,
                symbol,
                alias,
                raw,
                context,
                decision,
            )
        )
    signals.sort(key=lambda value: value[:3])
    base_record: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "contract_id": CONTRACT_ID,
        "mode": "shadow",
        "date": day,
        "premarket_shortlist_frozen_at": frozen_at.isoformat(),
        "shortlist_candidate_count": len(raw_candidates),
        "qualified_signal_count": len(signals),
        "public_candidate_aliases": aliases,
        "order_actions_allowed": ORDER_ACTIONS_ALLOWED,
        "automatic_strategy_application": False,
        "production_strategy_changed": False,
        "recorded_at": datetime.now(UTC).isoformat(),
    }
    if not signals:
        record = {
            **base_record,
            "status": "no_trade",
            "reason": "no_completed_early_item_2.02_reversal_signal",
            "rule_violations": [],
            "lifecycle_complete": True,
        }
        return _seal_record(record)

    _, _, symbol, alias, raw, context, decision = signals[0]
    bar = context.bars[decision.signal_index]
    signal_completed_at = _et_timestamp(day, bar.time_et) + timedelta(minutes=1)
    detected = _timestamp(raw.get("signal_detected_at"), f"{alias}.signal_detected_at")
    detection_delay = (detected - signal_completed_at).total_seconds()
    missed_reasons: list[str] = []
    violations: list[str] = []
    if detection_delay < 0:
        violations.append("signal_evaluated_before_bar_completion")
    elif detection_delay > MAX_DETECTION_DELAY_SECONDS:
        missed_reasons.append("signal_detection_delayed")
    snapshots_raw = raw.get("quote_snapshots")
    if not isinstance(snapshots_raw, list) or len(snapshots_raw) != 3:
        missed_reasons.append("three_quote_snapshots_missing")
        snapshots: list[dict[str, Any]] = []
    else:
        snapshots = [
            _quote_snapshot(value, f"{alias}.quote_snapshots[{index}]")
            for index, value in enumerate(snapshots_raw)
        ]
        observed_times = [value["observed_at"] for value in snapshots]
        if observed_times != sorted(observed_times):
            violations.append("quote_snapshots_not_chronological")
        if observed_times and (observed_times[-1] - observed_times[0]).total_seconds() > MAX_SNAPSHOT_WINDOW_SECONDS:
            violations.append("quote_snapshot_window_exceeds_10_seconds")
        if any(value["stale"] for value in snapshots):
            violations.append("stale_entry_quote")
        if any(value["crossed"] for value in snapshots):
            violations.append("crossed_entry_quote")
        if snapshots and max(value["spread_fraction"] for value in snapshots) > HARD_SPREAD_FRACTION:
            violations.append("entry_spread_exceeds_hard_limit")
        if snapshots and statistics.median(value["spread_fraction"] for value in snapshots) > OPERATING_SPREAD_FRACTION:
            violations.append("median_entry_spread_exceeds_operating_limit")
        if snapshots and (observed_times[-1] - signal_completed_at).total_seconds() > MAX_ENTRY_OBSERVATION_DELAY_SECONDS:
            missed_reasons.append("entry_observation_delayed")
    stop_ready_value = raw.get("stop_ready_at")
    protection_value = raw.get("simulated_protection_at")
    if not isinstance(stop_ready_value, str):
        missed_reasons.append("stop_readiness_timestamp_missing")
        stop_ready = None
    else:
        stop_ready = _timestamp(stop_ready_value, f"{alias}.stop_ready_at")
    if not isinstance(protection_value, str):
        missed_reasons.append("protection_timestamp_missing")
        protection = None
    else:
        protection = _timestamp(
            protection_value, f"{alias}.simulated_protection_at"
        )
    entry = snapshots[-1]["ask"] if snapshots else None
    chase_cap = bar.close * (1.0 + CHASE_CAP_FRACTION)
    if entry is not None and entry > chase_cap:
        missed_reasons.append("fresh_ask_above_chase_cap")
    monitoring = raw.get("monitoring")
    if not isinstance(monitoring, Mapping) or monitoring.get("complete") is not True:
        violations.append("monitoring_incomplete")
    else:
        maximum_gap = _number(
            monitoring.get("maximum_gap_seconds"),
            f"{alias}.monitoring.maximum_gap_seconds",
        )
        if maximum_gap < 0 or maximum_gap > MAX_MONITORING_GAP_SECONDS:
            violations.append("monitoring_gap_exceeds_15_seconds")
    public_snapshots = [
        {
            "quoted_at": value["quoted_at"].isoformat(),
            "observed_at": value["observed_at"].isoformat(),
            "age_seconds": round(value["age_seconds"], 6),
            "bid": value["bid"],
            "ask": value["ask"],
            "bid_size": value["bid_size"],
            "ask_size": value["ask_size"],
            "spread_fraction": round(value["spread_fraction"], 8),
        }
        for value in snapshots
    ]
    if missed_reasons or violations or entry is None or stop_ready is None or protection is None:
        record = {
            **base_record,
            "status": "missed_signal",
            "reason": sorted(set(missed_reasons or ["entry_evidence_invalid"])),
            "selected_public_alias": alias,
            "symbol": symbol,
            "signal_bar_start_et": bar.time_et,
            "signal_completed_at": signal_completed_at.isoformat(),
            "signal_detected_at": detected.isoformat(),
            "detection_delay_seconds": round(detection_delay, 6),
            "quote_snapshots": public_snapshots,
            "chase_cap_price": round(chase_cap, 8),
            "structural_stop": round(decision.technical_stop, 8),
            "structural_stop_compressed": False,
            "rule_violations": sorted(set(violations)),
            "lifecycle_complete": True,
        }
        return _seal_record(record)

    fill_at = snapshots[-1]["observed_at"]
    if stop_ready < detected:
        violations.append("stop_readiness_precedes_signal_detection")
    if protection < fill_at:
        violations.append("protection_precedes_simulated_fill")
    stop = float(decision.technical_stop)
    stop_distance = entry - stop
    if stop_distance <= 0:
        violations.append("structural_stop_not_below_entry")
    reserve = entry * STOP_RESERVE_FRACTION
    risk_per_share = stop_distance + reserve
    q_risk = math.floor(equity * RISK_FRACTION / risk_per_share) if risk_per_share > 0 else 0
    q_allocation = math.floor(buying_power * ALLOCATION_CAP_FRACTION / entry)
    q_depth = math.floor(min(value["ask_size"] for value in snapshots) * 0.05)
    q_volume = math.floor(bar.volume * 0.05)
    quantity = min(q_risk, q_allocation, q_depth, q_volume)
    if quantity <= 0:
        violations.append("risk_allocation_or_liquidity_cap_produces_zero_shares")
    cap_values = {
        "risk": q_risk,
        "allocation": q_allocation,
        "visible_ask_depth": q_depth,
        "recent_signal_bar_volume": q_volume,
    }
    binding_cap = min(cap_values, key=lambda key: (cap_values[key], key))
    planned_loss_fraction = quantity * risk_per_share / equity if equity else 0.0
    if planned_loss_fraction > RISK_FRACTION + 1e-12:
        violations.append("planned_loss_exceeds_unvalidated_risk_cap")
    allocation = quantity * entry / equity
    target = entry + TARGET_R * stop_distance
    outcome = _simulate_exit(
        raw.get("market_path", ()), entry=entry, stop=stop, target=target
    )
    violations.extend(outcome.get("violations", ()))
    unprotected = (protection - fill_at).total_seconds()
    if unprotected < 0:
        violations.append("negative_unprotected_exposure")
    record = {
        **base_record,
        "status": "trade" if outcome.get("complete") and not violations else "incomplete_signal",
        "reason": "modeled_lifecycle_complete" if outcome.get("complete") and not violations else "lifecycle_or_rule_violation",
        "selected_public_alias": alias,
        "symbol": symbol,
        "signal_bar_start_et": bar.time_et,
        "signal_bar_close": round(bar.close, 8),
        "signal_completed_at": signal_completed_at.isoformat(),
        "signal_detected_at": detected.isoformat(),
        "detection_delay_seconds": round(detection_delay, 6),
        "quote_snapshots": public_snapshots,
        "hypothetical_entry_at": fill_at.isoformat(),
        "hypothetical_fill_price": round(entry, 8),
        "chase_cap_price": round(chase_cap, 8),
        "entry_slippage_bps_from_signal_close": round((entry / bar.close - 1.0) * 10_000.0, 8),
        "stop_ready_at": stop_ready.isoformat(),
        "simulated_protection_at": protection.isoformat(),
        "unprotected_exposure_seconds": round(unprotected, 6),
        "structural_stop": round(stop, 8),
        "deployed_stop": round(stop, 8),
        "structural_stop_fraction": round(stop_distance / entry, 8),
        "structural_stop_compressed": False,
        "stop_slippage_reserve_fraction": STOP_RESERVE_FRACTION,
        "target_price": round(target, 8),
        "quantity": quantity,
        "q_risk": q_risk,
        "q_allocation": q_allocation,
        "q_visible_ask_depth": q_depth,
        "q_recent_volume": q_volume,
        "binding_cap": binding_cap,
        "planned_loss_fraction": round(planned_loss_fraction, 8),
        "allocation_fraction": round(allocation, 8),
        "allocation_shortfall_fraction": round(max(0.0, ALLOCATION_OBJECTIVE_FLOOR_FRACTION - allocation), 8),
        "outcome": outcome,
        "rule_violations": sorted(set(violations)),
        "lifecycle_complete": bool(outcome.get("complete")) and not violations,
    }
    return _seal_record(record)


def _seal_record(record: Mapping[str, Any]) -> dict[str, Any]:
    body = dict(record)
    body["record_sha256"] = _sha256(_canonical_json(body))
    return body


def verify_shadow_record(record: Mapping[str, Any]) -> dict[str, Any]:
    value = dict(record)
    expected = value.pop("record_sha256", None)
    if not isinstance(expected, str) or _sha256(_canonical_json(value)) != expected:
        raise ShadowReversalError("shadow record hash mismatch")
    if value.get("schema_version") != SCHEMA_VERSION or value.get("contract_id") != CONTRACT_ID:
        raise ShadowReversalError("shadow record contract mismatch")
    if value.get("order_actions_allowed") is not False:
        raise ShadowReversalError("shadow record cannot authorize order actions")
    return dict(record)


def write_shadow_record(record: Mapping[str, Any], path: Path | None = None) -> Path:
    verified = verify_shadow_record(record)
    if path is None:
        path = DEFAULT_OUTPUT_ROOT / f"{verified['date']}-{verified['record_sha256'][:12]}.json"
    resolved = path.resolve()
    allowed = DEFAULT_OUTPUT_ROOT.resolve()
    if allowed != resolved.parent and allowed not in resolved.parents:
        raise ShadowReversalError("shadow records must stay under shadow_results/")
    if resolved.exists():
        if _read_json(resolved) != verified:
            raise ShadowReversalError("existing shadow record has different content")
    else:
        _atomic_json(resolved, verified)
    return resolved


def qualify_shadow_records(
    records: Sequence[Mapping[str, Any]],
    *,
    confirmation_result: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    verified = [verify_shadow_record(value) for value in records]
    if not verified:
        raise ShadowReversalError("qualification requires shadow records")
    dates = [date.fromisoformat(str(value["date"])) for value in verified]
    if len(set(dates)) != len(dates):
        raise ShadowReversalError("qualification requires one record per date")
    ordered = [value for _, value in sorted(zip(dates, verified), key=lambda row: row[0])]
    signal_records = [value for value in ordered if value["status"] != "no_trade"]
    returns = [
        float(value["outcome"]["net_r"]) if value["status"] == "trade" else 0.0
        for value in signal_records
    ]
    stats = _return_stats(returns)
    spreads = [
        float(snapshot["spread_fraction"])
        for value in signal_records
        for snapshot in value.get("quote_snapshots", ())
    ]
    slippages = [
        float(value["entry_slippage_bps_from_signal_close"])
        for value in signal_records
        if value["status"] == "trade"
    ]
    unprotected = [
        float(value["unprotected_exposure_seconds"])
        for value in signal_records
        if value["status"] == "trade"
    ]
    violations = [
        f"{value['date']}:{violation}"
        for value in ordered
        for violation in value.get("rule_violations", ())
    ]
    span_days = (max(dates) - min(dates)).days + 1
    historical_passed = bool(
        confirmation_result
        and confirmation_result.get("acceptance", {}).get("all_passed") is True
        and confirmation_result.get("decision", {}).get("next_stage")
        == "advance_structural_stop_risk_sized_arm_to_shadow"
    )
    pf_passed = (
        stats["profit_factor"] is None and stats["total_r"] > 0
    ) or (
        stats["profit_factor"] is not None and stats["profit_factor"] >= 1.20
    )
    gates = {
        "minimum_completed_signals": {"passed": len(signal_records) >= 20, "actual": len(signal_records), "required": ">=20"},
        "calendar_span": {"passed": span_days >= 30, "actual": span_days, "required": ">=30 days"},
        "zero_rule_violations": {"passed": not violations, "actual": violations, "required": "none"},
        "lifecycle_complete": {"passed": all(value.get("lifecycle_complete") for value in ordered), "actual": sum(bool(value.get("lifecycle_complete")) for value in ordered), "required": len(ordered)},
        "median_spread": {"passed": bool(spreads) and statistics.median(spreads) <= OPERATING_SPREAD_FRACTION, "actual": round(statistics.median(spreads), 8) if spreads else None, "required": "<=0.001"},
        "maximum_spread": {"passed": bool(spreads) and max(spreads) <= HARD_SPREAD_FRACTION, "actual": round(max(spreads), 8) if spreads else None, "required": "<=0.0015"},
        "entry_slippage_p95": {"passed": bool(slippages) and _quantile(slippages, 0.95) <= 15.0, "actual": round(_quantile(slippages, 0.95), 8) if slippages else None, "required": "<=15 bps"},
        "unprotected_exposure_p95": {"passed": bool(unprotected) and _quantile(unprotected, 0.95) <= 10.0, "actual": round(_quantile(unprotected, 0.95), 8) if unprotected else None, "required": "<=10 seconds"},
        "positive_expectancy": {"passed": stats["mean_r"] is not None and stats["mean_r"] > 0, "actual": stats["mean_r"], "required": ">0R"},
        "profit_factor": {"passed": pf_passed, "actual": stats["profit_factor"], "required": ">=1.20"},
        "maximum_drawdown": {"passed": stats["maximum_drawdown_r"] <= 6.0, "actual": stats["maximum_drawdown_r"], "required": "<=6R"},
        "no_fill_above_chase_cap": {"passed": all(value.get("hypothetical_fill_price", 0) <= value.get("chase_cap_price", math.inf) for value in signal_records if value["status"] == "trade"), "actual": True, "required": True},
        "structural_stops_preserved": {"passed": all(value.get("structural_stop_compressed") is False for value in signal_records), "actual": 0, "required": 0},
    }
    shadow_passed = all(value["passed"] for value in gates.values())
    return {
        "schema_version": SCHEMA_VERSION,
        "contract_id": CONTRACT_ID,
        "records": len(ordered),
        "first_date": min(dates).isoformat(),
        "last_date": max(dates).isoformat(),
        "calendar_span_days": span_days,
        "no_trade_days": sum(value["status"] == "no_trade" for value in ordered),
        "missed_signal_days": sum(value["status"] == "missed_signal" for value in ordered),
        "modeled_trade_days": sum(value["status"] == "trade" for value in ordered),
        "signal_statistics_including_operational_misses_as_zero_r": stats,
        "gates": gates,
        "shadow_execution_passed": shadow_passed,
        "historical_confirmation_passed": historical_passed,
        "proposal_gate_status": (
            "qualified_for_normal_cadence_review"
            if shadow_passed and historical_passed
            else "awaiting_historical_confirmation"
            if shadow_passed
            else "shadow_qualification_failed_or_incomplete"
        ),
        "automatic_strategy_application": False,
        "order_actions_allowed": False,
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    record = subparsers.add_parser("record", help="evaluate and seal one shadow session capture")
    record.add_argument("capture", type=Path)
    record.add_argument("--output", type=Path)
    qualify = subparsers.add_parser("qualify", help="aggregate prospective shadow records")
    qualify.add_argument("records", type=Path, nargs="+")
    qualify.add_argument("--confirmation-result", type=Path)
    qualify.add_argument("--output", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        if args.command == "record":
            record = evaluate_shadow_session(_read_json(args.capture))
            path = write_shadow_record(record, args.output)
            output: Mapping[str, Any] = {
                "record": str(path),
                "record_sha256": record["record_sha256"],
                "status": record["status"],
                "order_actions_allowed": False,
            }
        else:
            records = [_read_json(path) for path in args.records]
            confirmation = (
                _read_json(args.confirmation_result)
                if args.confirmation_result
                else None
            )
            output = qualify_shadow_records(records, confirmation_result=confirmation)
            if args.output:
                resolved = args.output.resolve()
                allowed = DEFAULT_OUTPUT_ROOT.resolve()
                if allowed != resolved.parent and allowed not in resolved.parents:
                    raise ShadowReversalError(
                        "shadow qualification output must stay under shadow_results/"
                    )
                _atomic_json(resolved, output)
        print(json.dumps(output, indent=2, sort_keys=True))
        return 0
    except (ShadowReversalError, OSError, ValueError) as exc:
        print(json.dumps({"error": str(exc), "error_type": type(exc).__name__}, indent=2))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

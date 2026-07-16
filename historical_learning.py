"""Deterministic, no-broker historical learning workflow for the ORB strategy.

Historical data is supplied as point-in-time replay bundles collected by the
active Codex workflow.  This module validates fidelity, evaluates candidates
through ``strategy_engine.py``, selects at most one simulated trade for the day,
replays its minute bars conservatively, records every candidate, and archives
all generated context.  It never calls broker or live-order tools.
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import math
import random
import secrets
import sys
from dataclasses import asdict, dataclass
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Any, Mapping, Sequence
from zoneinfo import ZoneInfo

from strategy_engine import (
    EvaluationResult,
    StrategyConfig,
    StrategyInputError,
    evaluate_candidate,
    load_config,
)
from strategy_ledger import (
    DEFAULT_LEDGER_PATH,
    LedgerError,
    append_records,
    audit_ledger,
    build_report,
    prepare_record,
    read_records,
)
from trade_lifecycle import (
    DEFAULT_ACTIVE_ROOT,
    DEFAULT_ARCHIVE_ROOT,
    LifecycleError,
    archive_context,
    audit_lifecycle,
)


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_DATA_ROOT = PROJECT_ROOT / "historical_data"
DEFAULT_BATCH_ROOT = PROJECT_ROOT / "historical_batches"
BUNDLE_SCHEMA_VERSION = 1
BATCH_STATUS_SCHEMA_VERSION = 1
MINIMUM_CANDIDATES = 10
REQUIRED_SOURCE_ATTESTATIONS = (
    "point_in_time",
    "regular_hours_only",
    "split_adjusted",
    "historical_quotes_and_depth",
    "catalysts_point_in_time",
    "universe_capture_complete",
)


class HistoricalLearningError(ValueError):
    """Raised when replay data or state cannot support a faithful simulation."""


def batch_acceptance(
    requested_days: int,
    completed_days: int,
    *,
    substitution_count: int = 0,
    cascade_error_count: int = 0,
) -> dict[str, Any]:
    """Return the fixed engineering gate for historical throughput quality."""
    yield_fraction = completed_days / requested_days if requested_days else 0.0
    checks = {
        "minimum_20_random_dates": requested_days >= 20,
        "minimum_80_percent_yield": yield_fraction >= 0.80,
        "zero_substitutions": substitution_count == 0,
        "zero_cascade_errors": cascade_error_count == 0,
    }
    return {
        "target": {
            "minimum_random_dates": 20,
            "minimum_yield_fraction": 0.80,
            "maximum_substitutions": 0,
            "maximum_cascade_errors": 0,
        },
        "observed": {
            "requested_days": requested_days,
            "completed_days": completed_days,
            "yield_fraction": yield_fraction,
            "substitution_count": substitution_count,
            "cascade_error_count": cascade_error_count,
        },
        "checks": checks,
        "passed": all(checks.values()),
    }


@dataclass(frozen=True)
class ReplayOutcome:
    entry_price: float
    exit_price: float
    exit_time_et: str
    exit_reason: str
    quantity: int
    net_r: float
    net_pnl_dollars: float
    paper_eod_shadow_net_r: float
    mfe_r: float
    mae_r: float
    stop_executed: bool
    runner_activated: bool


@dataclass(frozen=True)
class ReplayDayResult:
    date: str
    session_id: str
    selected_signal_id: str | None
    candidate_count: int
    eligible_count: int
    archived_paths: tuple[str, ...]
    ledger_records: int
    earned_maturity: str
    seed: int | None


def _number(value: Any, field: str, *, minimum: float | None = None) -> float:
    if isinstance(value, bool):
        raise HistoricalLearningError(f"{field} must be numeric")
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise HistoricalLearningError(f"{field} must be numeric") from exc
    if not math.isfinite(result) or (minimum is not None and result < minimum):
        raise HistoricalLearningError(f"{field} must be finite and >= {minimum}")
    return result


def _mapping(value: Any, field: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise HistoricalLearningError(f"{field} must be an object")
    return value


def _parse_time(value: Any, field: str) -> time:
    if not isinstance(value, str):
        raise HistoricalLearningError(f"{field} must be HH:MM:SS")
    try:
        parsed = time.fromisoformat(value)
    except ValueError as exc:
        raise HistoricalLearningError(f"{field} must be HH:MM:SS") from exc
    if parsed.tzinfo is not None:
        raise HistoricalLearningError(f"{field} must be an Eastern wall-clock time")
    return parsed


def _expected_bar_times() -> tuple[str, ...]:
    start = datetime.combine(date(2000, 1, 1), time(9, 30))
    return tuple(
        (start + timedelta(minutes=index)).time().isoformat() for index in range(390)
    )


EXPECTED_BAR_TIMES = _expected_bar_times()


def _validate_bar(
    bar: Mapping[str, Any],
    index: int,
    candidate_index: int,
    maximum_runner_spread: float,
) -> None:
    prefix = f"candidates[{candidate_index}].bars[{index}]"
    if bar.get("time_et") != EXPECTED_BAR_TIMES[index]:
        raise HistoricalLearningError(
            f"{prefix}.time_et must be {EXPECTED_BAR_TIMES[index]}"
        )
    open_price = _number(bar.get("open"), f"{prefix}.open", minimum=0.000001)
    high = _number(bar.get("high"), f"{prefix}.high", minimum=0.000001)
    low = _number(bar.get("low"), f"{prefix}.low", minimum=0.000001)
    close = _number(bar.get("close"), f"{prefix}.close", minimum=0.000001)
    _number(bar.get("volume"), f"{prefix}.volume", minimum=0)
    if low > min(open_price, close) or high < max(open_price, close) or low > high:
        raise HistoricalLearningError(f"{prefix} contains inconsistent OHLC values")
    if bar.get("interpolated") is not False:
        raise HistoricalLearningError(f"{prefix} must explicitly be non-interpolated")
    if "runner_eligible" in bar and not isinstance(bar["runner_eligible"], bool):
        raise HistoricalLearningError(f"{prefix}.runner_eligible must be true or false")
    if bar.get("runner_eligible") is True:
        for field in (
            "above_rising_vwap",
            "market_supportive",
            "catalyst_intact",
        ):
            if bar.get(field) is not True:
                raise HistoricalLearningError(
                    f"{prefix}.{field} must be true for a qualified runner"
                )
        spread_fraction = _number(
            bar.get("spread_fraction"),
            f"{prefix}.spread_fraction",
            minimum=0,
        )
        if spread_fraction > maximum_runner_spread:
            raise HistoricalLearningError(
                f"{prefix}.spread_fraction exceeds the runner limit"
            )
        structural_stop = _number(
            bar.get("structural_stop"), f"{prefix}.structural_stop", minimum=0.000001
        )
        if structural_stop >= close:
            raise HistoricalLearningError(
                f"{prefix}.structural_stop must be below the bar close"
            )


def validate_bundle(
    bundle: Mapping[str, Any],
    config: StrategyConfig | None = None,
    *,
    today_et: date | None = None,
) -> None:
    """Validate point-in-time completeness before any replay artifacts are written."""
    config = config or load_config()
    if bundle.get("schema_version") != BUNDLE_SCHEMA_VERSION:
        raise HistoricalLearningError(
            f"bundle schema_version must be {BUNDLE_SCHEMA_VERSION}"
        )
    day_text = bundle.get("date")
    if not isinstance(day_text, str):
        raise HistoricalLearningError("date must be an ISO calendar date")
    try:
        replay_day = date.fromisoformat(day_text)
    except ValueError as exc:
        raise HistoricalLearningError("date must be an ISO calendar date") from exc
    current_day = today_et or datetime.now(ZoneInfo("America/New_York")).date()
    if replay_day >= current_day:
        raise HistoricalLearningError(
            "historical replay date must be a completed prior day"
        )

    source = _mapping(bundle.get("source"), "source")
    provider = source.get("provider")
    if not isinstance(provider, str) or not provider.strip():
        raise HistoricalLearningError("source.provider must be a non-empty string")
    for field in REQUIRED_SOURCE_ATTESTATIONS:
        if source.get(field) is not True:
            raise HistoricalLearningError(f"source.{field} must be true")
    captured_at = source.get("captured_at")
    if not isinstance(captured_at, str):
        raise HistoricalLearningError("source.captured_at must be an ISO timestamp")
    try:
        captured = datetime.fromisoformat(captured_at)
    except ValueError as exc:
        raise HistoricalLearningError(
            "source.captured_at must be an ISO timestamp"
        ) from exc
    if captured.tzinfo is None:
        raise HistoricalLearningError("source.captured_at must include a timezone")
    if bundle.get("session_capture_complete") is not True:
        raise HistoricalLearningError("session_capture_complete must be true")
    simulated_equity = _number(
        bundle.get("simulation_account_equity"),
        "simulation_account_equity",
        minimum=0.000001,
    )
    simulated_buying_power = _number(
        bundle.get("simulation_buying_power"),
        "simulation_buying_power",
        minimum=0.000001,
    )
    if simulated_buying_power < simulated_equity:
        raise HistoricalLearningError(
            "simulation_buying_power cannot be below simulation_account_equity"
        )
    sample_phase = bundle.get("sample_phase", "pilot")
    if sample_phase not in ("pilot", "confirmation"):
        raise HistoricalLearningError("sample_phase must be pilot or confirmation")
    if sample_phase == "confirmation":
        preregistration = _mapping(bundle.get("preregistration"), "preregistration")
        if (
            not isinstance(preregistration.get("manifest_hash"), str)
            or not preregistration["manifest_hash"].strip()
        ):
            raise HistoricalLearningError(
                "confirmation replay requires a preregistration manifest_hash"
            )
        if not isinstance(preregistration.get("registered_at"), str):
            raise HistoricalLearningError(
                "confirmation replay requires preregistration.registered_at"
            )
        try:
            registered_at = datetime.fromisoformat(preregistration["registered_at"])
        except ValueError as exc:
            raise HistoricalLearningError(
                "preregistration.registered_at must be an ISO timestamp"
            ) from exc
        if registered_at.tzinfo is None:
            raise HistoricalLearningError(
                "preregistration.registered_at must include a timezone"
            )
        if registered_at > captured:
            raise HistoricalLearningError(
                "confirmation replay must be preregistered before data collection"
            )

    candidates = bundle.get("candidates")
    if not isinstance(candidates, list) or len(candidates) < MINIMUM_CANDIDATES:
        raise HistoricalLearningError(
            f"candidates must contain at least {MINIMUM_CANDIDATES} symbols"
        )
    symbols: set[str] = set()
    signal_ids: set[str] = set()
    entry_start = _parse_time(
        config.raw["strategy"]["entry_start_et"], "strategy.entry_start_et"
    )
    entry_cutoff = _parse_time(
        config.raw["strategy"]["entry_cutoff_et"], "strategy.entry_cutoff_et"
    )
    replay_date = date.fromisoformat(day_text)
    for candidate_index, item in enumerate(candidates):
        candidate = _mapping(item, f"candidates[{candidate_index}]")
        symbol = candidate.get("symbol")
        if not isinstance(symbol, str) or not symbol or symbol != symbol.upper():
            raise HistoricalLearningError(
                f"candidates[{candidate_index}].symbol must be uppercase"
            )
        if symbol in symbols:
            raise HistoricalLearningError(f"duplicate candidate symbol: {symbol}")
        symbols.add(symbol)
        signal_id = candidate.get("signal_id")
        if not isinstance(signal_id, str) or not signal_id.startswith(
            f"{day_text}-{symbol}-"
        ):
            raise HistoricalLearningError(
                f"candidates[{candidate_index}].signal_id must match date and symbol"
            )
        if signal_id in signal_ids:
            raise HistoricalLearningError(f"duplicate signal_id: {signal_id}")
        signal_ids.add(signal_id)
        evaluation_time = _parse_time(
            candidate.get("evaluation_time_et"),
            f"candidates[{candidate_index}].evaluation_time_et",
        )
        if not entry_start <= evaluation_time <= entry_cutoff:
            raise HistoricalLearningError(
                f"candidates[{candidate_index}] evaluation is outside the entry window"
            )

        catalyst = _mapping(
            candidate.get("catalyst"), f"candidates[{candidate_index}].catalyst"
        )
        if catalyst.get("point_in_time") is not True:
            raise HistoricalLearningError(
                f"candidates[{candidate_index}].catalyst.point_in_time must be true"
            )
        if (
            not isinstance(catalyst.get("source_url"), str)
            or not catalyst["source_url"].strip()
        ):
            raise HistoricalLearningError(
                f"candidates[{candidate_index}].catalyst.source_url is required"
            )
        published_at = catalyst.get("published_at")
        if not isinstance(published_at, str):
            raise HistoricalLearningError(
                f"candidates[{candidate_index}].catalyst.published_at is required"
            )
        try:
            published = datetime.fromisoformat(published_at)
        except ValueError as exc:
            raise HistoricalLearningError(
                f"candidates[{candidate_index}].catalyst.published_at is invalid"
            ) from exc
        if published.tzinfo is None:
            raise HistoricalLearningError(
                f"candidates[{candidate_index}].catalyst.published_at needs timezone"
            )
        evaluation_dt = datetime.combine(
            replay_date, evaluation_time, ZoneInfo("America/New_York")
        )
        if published > evaluation_dt:
            raise HistoricalLearningError(
                f"candidates[{candidate_index}] catalyst was published after evaluation"
            )

        payload = _mapping(
            candidate.get("evaluation_payload"),
            f"candidates[{candidate_index}].evaluation_payload",
        )
        candidate_payload = _mapping(
            payload.get("candidate"),
            f"candidates[{candidate_index}].evaluation_payload.candidate",
        )
        session_payload = _mapping(
            payload.get("session"),
            f"candidates[{candidate_index}].evaluation_payload.session",
        )
        if candidate_payload.get("symbol") != symbol:
            raise HistoricalLearningError(
                f"candidates[{candidate_index}] payload symbol does not match"
            )
        if session_payload.get("time_et") != candidate.get("evaluation_time_et"):
            raise HistoricalLearningError(
                f"candidates[{candidate_index}] payload time does not match"
            )
        quotes = payload.get("quotes")
        expected_quotes = int(config.raw["execution"]["quote_snapshot_count"])
        if not isinstance(quotes, list) or len(quotes) != expected_quotes:
            raise HistoricalLearningError(
                f"candidates[{candidate_index}] needs {expected_quotes} quote snapshots"
            )
        for quote_index, quote_value in enumerate(quotes):
            quote = _mapping(
                quote_value,
                f"candidates[{candidate_index}].evaluation_payload.quotes[{quote_index}]",
            )
            observed = _parse_time(
                quote.get("observed_at_et"),
                f"candidates[{candidate_index}].quotes[{quote_index}].observed_at_et",
            )
            if observed > evaluation_time:
                raise HistoricalLearningError(
                    f"candidates[{candidate_index}] quote snapshot is from the future"
                )

        bars = candidate.get("bars")
        if not isinstance(bars, list) or len(bars) != len(EXPECTED_BAR_TIMES):
            raise HistoricalLearningError(
                f"candidates[{candidate_index}].bars must contain all 390 regular-session minutes"
            )
        for bar_index, bar_value in enumerate(bars):
            _validate_bar(
                _mapping(bar_value, f"candidates[{candidate_index}].bars[{bar_index}]"),
                bar_index,
                candidate_index,
                float(config.raw["execution"]["maximum_median_spread_fraction"]),
            )
        if candidate_payload.get("clean_break") is True:
            opening_bar = _mapping(
                candidate_payload.get("opening_bar"),
                f"candidates[{candidate_index}].evaluation_payload.candidate.opening_bar",
            )
            opening_high = _number(
                opening_bar.get("high"),
                f"candidates[{candidate_index}].opening_bar.high",
                minimum=0.000001,
            )
            trigger_index = _bar_index(str(candidate["evaluation_time_et"]))
            if float(bars[trigger_index]["high"]) <= opening_high:
                raise HistoricalLearningError(
                    f"candidates[{candidate_index}] trigger minute did not break the opening-range high"
                )
            first_eligible_index = EXPECTED_BAR_TIMES.index("09:35:00")
            if any(
                float(bar["high"]) > opening_high
                for bar in bars[first_eligible_index:trigger_index]
            ):
                raise HistoricalLearningError(
                    f"candidates[{candidate_index}] evaluation is after the first opening-range break"
                )


def _bar_index(time_et: str) -> int:
    parsed = _parse_time(time_et, "evaluation_time_et")
    minute = parsed.replace(second=0, microsecond=0)
    key = minute.isoformat()
    try:
        return EXPECTED_BAR_TIMES.index(key)
    except ValueError as exc:
        raise HistoricalLearningError(
            "evaluation time has no regular-session bar"
        ) from exc


def _stop_fill(bar: Mapping[str, Any], stop: float, reserve: float) -> float:
    return max(0.0001, min(stop, float(bar["open"])) - reserve)


def _paper_baseline(
    bars: Sequence[Mapping[str, Any]],
    start_index: int,
    entry: float,
    planned_stop: float,
    reserve: float,
    spread: float,
    risk_per_share: float,
) -> float:
    for bar in bars[start_index:]:
        if float(bar["low"]) <= planned_stop:
            return (_stop_fill(bar, planned_stop, reserve) - entry) / risk_per_share
    eod_fill = max(0.0001, float(bars[-1]["close"]) - spread)
    return (eod_fill - entry) / risk_per_share


def replay_selected_trade(
    candidate: Mapping[str, Any], evaluation: EvaluationResult, config: StrategyConfig
) -> ReplayOutcome:
    bars = candidate["bars"]
    start_index = _bar_index(str(candidate["evaluation_time_et"]))
    sizing = evaluation.sizing
    entry = sizing.entry_limit
    entry_bar = bars[start_index]
    if float(entry_bar["high"]) < entry:
        raise HistoricalLearningError(
            f"{candidate['signal_id']} entry limit never traded in its trigger minute"
        )
    spread = evaluation.quote_summary.median_spread_dollars
    reserve = sizing.slippage_reserve_per_share
    risk_per_share = sizing.risk_per_share
    stop = sizing.planned_stop
    runner_active = False
    exit_price: float | None = None
    exit_time: str | None = None
    exit_reason: str | None = None
    stop_executed = False
    observed_bars: list[Mapping[str, Any]] = []
    force_flat = _parse_time(config.raw["strategy"]["force_flat_et"], "force_flat_et")

    for bar in bars[start_index:]:
        bar_time = _parse_time(bar["time_et"], "bar.time_et")
        observed_bars.append(bar)
        # Minute bars do not reveal intrabar order.  Stop-first is the conservative
        # outcome whenever both stop and target lie inside the same bar.
        if float(bar["low"]) <= stop:
            exit_price = _stop_fill(bar, stop, reserve)
            exit_time = str(bar["time_et"])
            exit_reason = (
                "protective_stop" if not runner_active else "runner_trailing_stop"
            )
            stop_executed = True
            break
        if not runner_active and float(bar["high"]) >= sizing.milestone_price:
            structural_stop = bar.get("structural_stop")
            if bar.get("runner_eligible") is True and structural_stop is not None:
                candidate_stop = float(structural_stop)
                one_r_floor = entry + risk_per_share
                if candidate_stop >= one_r_floor and candidate_stop < float(
                    bar["close"]
                ):
                    runner_active = True
                    stop = max(stop, candidate_stop)
                else:
                    exit_price = max(0.0001, sizing.milestone_price - spread)
                    exit_time = str(bar["time_et"])
                    exit_reason = "two_percent_milestone"
                    break
            else:
                exit_price = max(0.0001, sizing.milestone_price - spread)
                exit_time = str(bar["time_et"])
                exit_reason = "two_percent_milestone"
                break
        elif runner_active and bar.get("runner_eligible") is True:
            candidate_stop = float(bar["structural_stop"])
            if stop <= candidate_stop < float(bar["close"]):
                stop = candidate_stop
        if bar_time >= force_flat:
            exit_price = max(0.0001, float(bar["close"]) - spread)
            exit_time = str(bar["time_et"])
            exit_reason = "force_flat"
            break

    if exit_price is None or exit_time is None or exit_reason is None:
        raise HistoricalLearningError("replay did not reach a terminal project exit")
    net_r = (exit_price - entry) / risk_per_share
    paper_r = _paper_baseline(
        bars,
        start_index,
        entry,
        sizing.planned_stop,
        reserve,
        spread,
        risk_per_share,
    )
    maximum = max(float(bar["high"]) for bar in observed_bars)
    minimum = min(float(bar["low"]) for bar in observed_bars)
    return ReplayOutcome(
        entry_price=entry,
        exit_price=exit_price,
        exit_time_et=exit_time,
        exit_reason=exit_reason,
        quantity=sizing.quantity,
        net_r=net_r,
        net_pnl_dollars=(exit_price - entry) * sizing.quantity,
        paper_eod_shadow_net_r=paper_r,
        mfe_r=(maximum - entry) / risk_per_share,
        mae_r=(minimum - entry) / risk_per_share,
        stop_executed=stop_executed,
        runner_activated=runner_active,
    )


def _features(evaluation: EvaluationResult) -> dict[str, float]:
    # The evaluator intentionally preserves negative raw room/reward values so
    # the hard-reject explanation can distinguish resistance below entry. The
    # public ledger feature schema is magnitude-only and requires nonnegative
    # finite values; zero truthfully means there is no defensible upside room.
    return {
        "opening_relative_volume": evaluation.opening_relative_volume,
        "score": float(evaluation.score),
        "median_spread_bps": evaluation.quote_summary.median_spread_fraction * 10000,
        "stop_fraction": evaluation.sizing.stop_fraction,
        "resistance_room_fraction": max(
            0.0, evaluation.sizing.resistance_room_fraction
        ),
        "reward_risk": max(0.0, evaluation.sizing.reward_risk),
    }


def _candidate_context(
    day: str,
    candidate: Mapping[str, Any],
    evaluation: EvaluationResult,
    outcome: ReplayOutcome | None,
    *,
    selected: bool,
    missed: bool,
    provider: str,
) -> str:
    state = (
        "selected simulated trade"
        if selected
        else "missed after daily limit"
        if missed
        else "rejected idea"
    )
    lines = [
        f"# Historical Replay: {candidate['signal_id']}",
        "",
        "## Replay Identity",
        "",
        f"- Date (ET): {day}",
        "- Mode: shadow historical replay",
        f"- Signal ID: {candidate['signal_id']}",
        f"- Symbol: {candidate['symbol']}",
        f"- Point-in-time provider: {provider}",
        f"- Evaluation time (ET): {candidate['evaluation_time_et']}",
        f"- Replay state: {state}",
        f"- Strategy version: {evaluation.strategy_version}",
        f"- Rules hash: {evaluation.rules_hash}",
        "",
        "## Deterministic Evaluation",
        "",
        f"- Eligible: {str(evaluation.eligible).lower()}",
        f"- Score: {evaluation.score}",
        f"- Classification: {evaluation.classification}",
        f"- Opening relative volume: {evaluation.opening_relative_volume:.6f}",
        f"- Ranking scope: {evaluation.ranking_scope}",
        f"- Hard rejects: {list(evaluation.hard_rejects)}",
        f"- Warnings: {list(evaluation.warnings)}",
        f"- Quantity: {evaluation.sizing.quantity}",
        f"- Entry / stop / milestone: {evaluation.sizing.entry_limit:.6f} / {evaluation.sizing.planned_stop:.6f} / {evaluation.sizing.milestone_price:.6f}",
        "",
        "## Point-In-Time Catalyst",
        "",
        f"- Source: {candidate['catalyst']['source_url']}",
        f"- Published: {candidate['catalyst']['published_at']}",
    ]
    if outcome is not None:
        lines.extend(
            [
                "",
                "## Simulated Execution",
                "",
                f"- Exit: {outcome.exit_price:.6f} at {outcome.exit_time_et} ET",
                f"- Exit reason: {outcome.exit_reason}",
                f"- Net R: {outcome.net_r:.6f}",
                f"- Net P/L: ${outcome.net_pnl_dollars:.2f}",
                f"- Paired paper EOD net R: {outcome.paper_eod_shadow_net_r:.6f}",
                f"- MFE / MAE: {outcome.mfe_r:.6f}R / {outcome.mae_r:.6f}R",
            ]
        )
    return "\n".join(lines) + "\n"


def _session_context(
    day: str,
    session_id: str,
    candidates: Sequence[Mapping[str, Any]],
    evaluations: Sequence[EvaluationResult],
    selected_id: str | None,
    provider: str,
    config: StrategyConfig,
) -> str:
    return "\n".join(
        [
            f"# Historical Learning Session: {day}",
            "",
            f"- Public session ID: {session_id}",
            "- Mode: shadow historical replay",
            f"- Strategy version: {config.version}",
            f"- Rules hash: {config.rules_hash}",
            f"- Point-in-time provider: {provider}",
            "- Session capture complete: yes",
            f"- Candidates evaluated: {len(candidates)}",
            f"- Eligible candidates: {sum(result.eligible for result in evaluations)}",
            f"- Selected trade: {selected_id or 'none'}",
            "- Selection rule: earliest qualified trigger; simultaneous triggers rank by score then OR_RVOL",
            "- Intrabar ambiguity rule: protective stop before target",
            "- Broker actions: none",
            "",
        ]
    )


def _write_active(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with path.open("x", encoding="utf-8") as handle:
            handle.write(text)
    except FileExistsError as exc:
        raise HistoricalLearningError(f"active context already exists: {path}") from exc


def _outcome_payload(
    *,
    context_id: str,
    day: str,
    kind: str,
    result: str,
    summary: str,
    primary_reason: str,
    thesis_result: str,
    what_worked: Sequence[str],
    what_failed: Sequence[str],
    lessons: Sequence[str],
    next_time: Sequence[str],
    metrics: Mapping[str, Any],
    config: StrategyConfig,
) -> dict[str, Any]:
    return {
        "context_id": context_id,
        "date": day,
        "context_kind": kind,
        "mode": "shadow",
        "result": result,
        "summary": summary,
        "primary_reason": primary_reason,
        "thesis_result": thesis_result,
        "what_worked": list(what_worked),
        "what_failed": list(what_failed),
        "lessons": list(lessons),
        "next_time": list(next_time),
        "metrics": dict(metrics),
        "strategy_version": config.version,
        "rules_hash": config.rules_hash,
    }


def run_replay_bundle(
    bundle: Mapping[str, Any],
    *,
    ledger_path: Path = DEFAULT_LEDGER_PATH,
    active_root: Path = DEFAULT_ACTIVE_ROOT,
    archive_root: Path = DEFAULT_ARCHIVE_ROOT,
    config: StrategyConfig | None = None,
    seed: int | None = None,
) -> ReplayDayResult:
    config = config or load_config()
    validate_bundle(bundle, config)
    active_contexts = [path for path in active_root.glob("*.md") if path.is_file()]
    if active_contexts:
        raise HistoricalLearningError(
            "historical learning requires an empty active context directory"
        )
    day = str(bundle["date"])
    archive_day = archive_root / day.replace("-", "_")
    if archive_day.exists():
        raise HistoricalLearningError(
            f"date already exists in the archive and cannot be replayed: {day}"
        )
    ledger_audit = audit_ledger(ledger_path, config)
    if not ledger_audit.valid:
        raise HistoricalLearningError(
            f"signal ledger audit failed: {list(ledger_audit.violations)}"
        )
    existing_records = read_records(ledger_path)
    maturity = build_report(existing_records, config)["maturity"]["earned_maturity"]
    candidates = list(bundle["candidates"])
    evaluations: list[EvaluationResult] = []
    for candidate in candidates:
        payload = copy.deepcopy(candidate["evaluation_payload"])
        payload["session"]["mode"] = "shadow"
        payload["session"]["maturity"] = maturity
        payload["session"]["account_equity"] = bundle["simulation_account_equity"]
        payload["session"]["buying_power"] = bundle["simulation_buying_power"]
        evaluations.append(evaluate_candidate(payload, config))

    ranked = sorted(
        (
            (
                str(candidate["evaluation_time_et"]),
                -evaluation.score,
                -evaluation.opening_relative_volume,
                str(candidate["symbol"]),
                index,
            )
            for index, (candidate, evaluation) in enumerate(
                zip(candidates, evaluations)
            )
            if evaluation.eligible
        )
    )
    selected_index = ranked[0][-1] if ranked else None
    selected_id = (
        str(candidates[selected_index]["signal_id"])
        if selected_index is not None
        else None
    )
    selected_outcome = (
        replay_selected_trade(
            candidates[selected_index], evaluations[selected_index], config
        )
        if selected_index is not None
        else None
    )
    session_id = f"{day}-session"
    provider = str(bundle["source"]["provider"])
    active_entries: list[tuple[Path, dict[str, Any]]] = []
    active_context_texts: list[tuple[Path, str]] = []
    ledger_payloads: list[dict[str, Any]] = []

    for index, (candidate, evaluation) in enumerate(zip(candidates, evaluations)):
        selected = index == selected_index
        missed = evaluation.eligible and not selected
        trade_outcome = selected_outcome if selected else None
        signal_id = str(candidate["signal_id"])
        active_path = active_root / f"{signal_id}.md"
        active_context_texts.append(
            (
                active_path,
                _candidate_context(
                    day,
                    candidate,
                    evaluation,
                    trade_outcome,
                    selected=selected,
                    missed=missed,
                    provider=provider,
                ),
            )
        )
        features = _features(evaluation)
        if selected and trade_outcome is not None:
            result_name = (
                "success"
                if trade_outcome.net_r > 0
                else "failure"
                if trade_outcome.net_r < 0
                else "flat"
            )
            summary = (
                f"The simulated trade exited via {trade_outcome.exit_reason} at "
                f"{trade_outcome.net_r:.3f}R."
            )
            worked = ["The setup passed every frozen strategy gate."]
            failed = (
                []
                if trade_outcome.net_r > 0
                else [f"The trade finished at {trade_outcome.net_r:.3f}R."]
            )
            outcome_payload = _outcome_payload(
                context_id=signal_id,
                day=day,
                kind="trade",
                result=result_name,
                summary=summary,
                primary_reason=trade_outcome.exit_reason,
                thesis_result="held" if trade_outcome.net_r > 0 else "failed",
                what_worked=worked,
                what_failed=failed,
                lessons=[
                    "Treat this replay as one frozen-rule observation, not permission to change the strategy."
                ],
                next_time=["Aggregate this result at the fixed learning cadence."],
                metrics={**asdict(trade_outcome), **features},
                config=config,
            )
            ledger_payloads.append(
                {
                    "record_type": "signal",
                    "signal_id": signal_id,
                    "session_id": session_id,
                    "date": day,
                    "symbol": candidate["symbol"],
                    "mode": "shadow",
                    "sample_phase": str(bundle.get("sample_phase", "pilot")),
                    "session_capture_complete": True,
                    "triggered": True,
                    "eligible": True,
                    "decision": "shadow",
                    "closed": True,
                    "net_r": trade_outcome.net_r,
                    "net_pnl_dollars": trade_outcome.net_pnl_dollars,
                    "project_exit_net_r": trade_outcome.net_r,
                    "paper_baseline_eligible": True,
                    "paper_eod_shadow_net_r": trade_outcome.paper_eod_shadow_net_r,
                    "entry_slippage_bps": None,
                    "unprotected_seconds": None,
                    "stop_executed": trade_outcome.stop_executed,
                    "stop_slippage_bps": (
                        evaluation.sizing.slippage_reserve_per_share
                        / evaluation.sizing.entry_limit
                        * 10000
                        if trade_outcome.stop_executed
                        else None
                    ),
                    "stop_reserve_bps": (
                        evaluation.sizing.slippage_reserve_per_share
                        / evaluation.sizing.entry_limit
                        * 10000
                        if trade_outcome.stop_executed
                        else None
                    ),
                    "rule_violations": [],
                    "rejection_reasons": [],
                    "features": features,
                }
            )
        elif missed:
            reason = "daily filled-entry limit exhausted by an earlier qualified signal"
            outcome_payload = _outcome_payload(
                context_id=signal_id,
                day=day,
                kind="trade_idea",
                result="stale",
                summary="The setup qualified but was not soft-executed because the day already had its selected trade.",
                primary_reason=reason,
                thesis_result="not_applicable",
                what_worked=["The idea passed every frozen strategy gate."],
                what_failed=[
                    "The opportunity arrived after the one-trade daily allocation was committed."
                ],
                lessons=[
                    "Preserve the one-trade limit; do not count this missed idea as a closed strategy return."
                ],
                next_time=["Retain it as selection-process evidence only."],
                metrics=features,
                config=config,
            )
            ledger_payloads.append(
                {
                    "record_type": "signal",
                    "signal_id": signal_id,
                    "session_id": session_id,
                    "date": day,
                    "symbol": candidate["symbol"],
                    "mode": "shadow",
                    "sample_phase": str(bundle.get("sample_phase", "pilot")),
                    "session_capture_complete": True,
                    "triggered": True,
                    "eligible": True,
                    "decision": "missed",
                    "missed_reason": reason,
                    "closed": False,
                    "net_r": None,
                    "paper_baseline_eligible": False,
                    "entry_slippage_bps": None,
                    "unprotected_seconds": None,
                    "stop_executed": False,
                    "stop_slippage_bps": None,
                    "stop_reserve_bps": None,
                    "rule_violations": [],
                    "rejection_reasons": [],
                    "features": features,
                }
            )
        else:
            reason = (
                evaluation.hard_rejects[0]
                if evaluation.hard_rejects
                else "strategy eligibility failed"
            )
            outcome_payload = _outcome_payload(
                context_id=signal_id,
                day=day,
                kind="trade_idea",
                result="rejected",
                summary=f"The historical idea was rejected before simulated entry: {reason}.",
                primary_reason=reason,
                thesis_result="not_applicable",
                what_worked=[
                    "The candidate was captured and evaluated rather than silently omitted."
                ],
                what_failed=list(evaluation.hard_rejects),
                lessons=[
                    "A rejected idea is selection evidence, not a realized return."
                ],
                next_time=[
                    "Keep the frozen gate unless cohort evidence supports a versioned review."
                ],
                metrics=features,
                config=config,
            )
            triggered = bool(
                candidate["evaluation_payload"]["candidate"].get("clean_break")
            )
            ledger_payloads.append(
                {
                    "record_type": "signal",
                    "signal_id": signal_id,
                    "session_id": session_id,
                    "date": day,
                    "symbol": candidate["symbol"],
                    "mode": "shadow",
                    "sample_phase": str(bundle.get("sample_phase", "pilot")),
                    "session_capture_complete": True,
                    "triggered": triggered,
                    "eligible": False,
                    "decision": "rejected",
                    "closed": not triggered,
                    "net_r": None,
                    "paper_baseline_eligible": False,
                    "entry_slippage_bps": None,
                    "unprotected_seconds": None,
                    "stop_executed": False,
                    "stop_slippage_bps": None,
                    "stop_reserve_bps": None,
                    "rule_violations": [],
                    "rejection_reasons": list(evaluation.hard_rejects),
                    "features": features,
                }
            )
        active_entries.append((active_path, outcome_payload))

    session_path = active_root / f"{session_id}.md"
    active_context_texts.append(
        (
            session_path,
            _session_context(
                day, session_id, candidates, evaluations, selected_id, provider, config
            ),
        )
    )
    session_outcome = _outcome_payload(
        context_id=session_id,
        day=day,
        kind="session",
        result="completed" if selected_id else "no_trade",
        summary=(
            f"Historical replay completed with selected trade {selected_id}."
            if selected_id
            else "Historical replay completed with no candidate passing every gate."
        ),
        primary_reason=(
            "daily replay completed" if selected_id else "no eligible setup"
        ),
        thesis_result="not_applicable",
        what_worked=[
            "The full supplied universe was evaluated with the frozen engine."
        ],
        what_failed=[]
        if selected_id
        else ["No candidate passed every production gate."],
        lessons=["Review only in aggregate at the evidence cadence."],
        next_time=["Select a different unarchived day for any future replay."],
        metrics={
            "candidate_count": len(candidates),
            "eligible_count": sum(result.eligible for result in evaluations),
            "selected_signal_id": selected_id,
        },
        config=config,
    )
    active_entries.append((session_path, session_outcome))
    ledger_payloads.append(
        {
            "record_type": "session",
            "session_id": session_id,
            "date": day,
            "mode": "shadow",
            "sample_phase": str(bundle.get("sample_phase", "pilot")),
            "closed": True,
            "session_capture_complete": True,
            "trade_taken": selected_id is not None,
            "candidate_count": len(candidates),
            "triggered_signal_count": sum(
                bool(candidate["evaluation_payload"]["candidate"].get("clean_break"))
                for candidate in candidates
            ),
            "no_trade_reason": ""
            if selected_id
            else "No candidate passed every hard gate",
            "rule_violations": [],
        }
    )

    # Validate every record and collision before context moves make the day
    # terminal.  The final append still repeats these checks under the file lock.
    prepared = [prepare_record(payload, config) for payload in ledger_payloads]
    existing_ids = {
        str(record.get("signal_id") or record.get("session_id"))
        for record in existing_records
    }
    for record in prepared:
        record_id = str(record.get("signal_id") or record.get("session_id"))
        if record_id in existing_ids:
            raise HistoricalLearningError(f"ledger record already exists: {record_id}")

    written_paths: list[Path] = []
    try:
        for path, text in active_context_texts:
            _write_active(path, text)
            written_paths.append(path)
    except Exception:
        for path in written_paths:
            path.unlink(missing_ok=True)
        raise

    archived_paths = tuple(
        str(
            archive_context(
                path,
                outcome,
                active_root=active_root,
                archive_root=archive_root,
                config=config,
                allow_historical=True,
            )
        )
        for path, outcome in active_entries
    )
    append_records(ledger_payloads, ledger_path, config)
    lifecycle_audit = audit_lifecycle(
        active_root=active_root, archive_root=archive_root, config=config
    )
    if not lifecycle_audit.valid:
        raise HistoricalLearningError(
            f"post-replay lifecycle audit failed: {list(lifecycle_audit.violations)}"
        )
    return ReplayDayResult(
        date=day,
        session_id=session_id,
        selected_signal_id=selected_id,
        candidate_count=len(candidates),
        eligible_count=sum(result.eligible for result in evaluations),
        archived_paths=archived_paths,
        ledger_records=len(ledger_payloads),
        earned_maturity=maturity,
        seed=seed,
    )


def load_bundle(path: Path) -> Mapping[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise HistoricalLearningError(f"{path}: invalid JSON: {exc.msg}") from exc
    if not isinstance(value, Mapping):
        raise HistoricalLearningError(f"{path}: bundle must be an object")
    return value


def select_random_dates(
    trading_days: Sequence[str],
    count: int,
    archive_root: Path = DEFAULT_ARCHIVE_ROOT,
    *,
    seed: int | None = None,
    today_et: date | None = None,
) -> tuple[list[str], int]:
    if count < 1:
        raise HistoricalLearningError("day count must be at least 1")
    current_day = today_et or datetime.now(ZoneInfo("America/New_York")).date()
    eligible: list[str] = []
    for item in dict.fromkeys(trading_days):
        try:
            day = date.fromisoformat(item)
        except (TypeError, ValueError) as exc:
            raise HistoricalLearningError(f"invalid trading date: {item}") from exc
        if day >= current_day:
            continue
        if not (archive_root / item.replace("-", "_")).exists():
            eligible.append(item)
    if len(eligible) < count:
        raise HistoricalLearningError(
            f"only {len(eligible)} unarchived completed trading days are available"
        )
    actual_seed = seed if seed is not None else secrets.randbits(63)
    selected = random.Random(actual_seed).sample(sorted(eligible), count)
    return selected, actual_seed


def _bundles_by_date(data_root: Path) -> dict[str, Path]:
    by_date: dict[str, Path] = {}
    for path in sorted(data_root.glob("*.json")):
        bundle = load_bundle(path)
        day = bundle.get("date")
        if not isinstance(day, str):
            raise HistoricalLearningError(f"{path}: bundle date is missing")
        if day in by_date:
            raise HistoricalLearningError(
                f"multiple replay bundles claim {day}: {by_date[day]} and {path}"
            )
        by_date[day] = path
    return by_date


def _selection_hash(dates: Sequence[str], seed: int | None) -> str:
    payload = json.dumps(
        {"seed": seed, "selected_dates": list(dates)},
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _write_batch_status(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    rendered = json.dumps(value, indent=2, sort_keys=True) + "\n"
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(rendered, encoding="utf-8")
    temporary.replace(path)


def _batch_status(
    dates: Sequence[str],
    *,
    seed: int | None,
    ready_dates: Sequence[str],
    replayed_dates: Sequence[str],
    newly_replayed_dates: Sequence[str],
    already_replayed_dates: Sequence[str],
    blocked: Mapping[str, Mapping[str, str]],
    results: Sequence[ReplayDayResult],
    created_at: str | None = None,
    collection_summary: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    selected = list(dates)
    replayed = list(replayed_dates)
    newly_replayed = list(newly_replayed_dates)
    already_replayed = list(already_replayed_dates)
    blocked_rows = [
        {"date": day, **dict(blocked[day])} for day in selected if day in blocked
    ]
    completed = len(replayed)
    now = datetime.now().astimezone().isoformat()
    public_results: list[dict[str, Any]] = []
    for result in results:
        payload = json.loads(json.dumps(asdict(result), sort_keys=True))
        archive_directory = result.date.replace("-", "_")
        payload["archived_paths"] = [
            f"trades/archived/{archive_directory}/{Path(path).name}"
            for path in result.archived_paths
        ]
        public_results.append(payload)
    return {
        "schema_version": BATCH_STATUS_SCHEMA_VERSION,
        "batch_id": f"historical-{_selection_hash(selected, seed)[:16]}",
        "selection_sha256": _selection_hash(selected, seed),
        "seed": seed,
        "created_at": created_at or now,
        "updated_at": now,
        "policy": "ready_only_no_substitution",
        "requested_days": len(selected),
        "completed_days": completed,
        "newly_replayed_days": len(newly_replayed),
        "yield_fraction": completed / len(selected),
        "selected_dates": selected,
        "ready_dates": list(ready_dates),
        "replayed_dates": replayed,
        "already_replayed_dates": already_replayed,
        "blocked_dates": blocked_rows,
        "substituted_dates": [],
        "engineering_acceptance": batch_acceptance(
            len(selected), completed, substitution_count=0, cascade_error_count=0
        ),
        "collection_summary": dict(collection_summary or {}),
        "results": public_results,
    }


def run_selected_dates(
    dates: Sequence[str],
    *,
    data_root: Path = DEFAULT_DATA_ROOT,
    ledger_path: Path = DEFAULT_LEDGER_PATH,
    active_root: Path = DEFAULT_ACTIVE_ROOT,
    archive_root: Path = DEFAULT_ARCHIVE_ROOT,
    seed: int | None = None,
    ready_only: bool = False,
    status_path: Path | None = None,
) -> dict[str, Any]:
    if not dates:
        raise HistoricalLearningError("at least one selected date is required")
    if len(dates) != len(set(dates)):
        raise HistoricalLearningError("selected dates must be unique")
    by_date = _bundles_by_date(data_root)
    missing = [day for day in dates if day not in by_date]
    if missing and not ready_only:
        raise HistoricalLearningError(
            f"selected dates do not have replay bundles: {missing}"
        )
    if not ready_only:
        results = [
            run_replay_bundle(
                load_bundle(by_date[day]),
                ledger_path=ledger_path,
                active_root=active_root,
                archive_root=archive_root,
                seed=seed,
            )
            for day in dates
        ]
        return {
            "seed": seed,
            "requested_days": len(dates),
            "completed_days": len(results),
            "selected_dates": list(dates),
            "results": [asdict(result) for result in results],
        }

    config = load_config()
    ready: list[str] = []
    already_replayed: list[str] = []
    blocked: dict[str, dict[str, str]] = {
        day: {
            "reason_code": "missing_replay_bundle",
            "detail": "No validation-grade replay bundle is available.",
        }
        for day in missing
    }
    for day in dates:
        if (archive_root / day.replace("-", "_")).exists():
            already_replayed.append(day)
            blocked.pop(day, None)
            continue
        path = by_date.get(day)
        if path is None:
            continue
        try:
            validate_bundle(load_bundle(path), config)
        except (HistoricalLearningError, OSError, json.JSONDecodeError) as exc:
            blocked[day] = {
                "reason_code": "invalid_replay_bundle",
                "detail": str(exc),
            }
            continue
        ready.append(day)

    created_at: str | None = None
    collection_summary: Mapping[str, Any] | None = None
    if status_path is not None and status_path.exists():
        try:
            previous = json.loads(status_path.read_text(encoding="utf-8"))
            if isinstance(previous, Mapping):
                previous_hash = previous.get("selection_sha256")
                current_hash = _selection_hash(dates, seed)
                if previous_hash not in (None, current_hash):
                    raise HistoricalLearningError(
                        f"batch status selection mismatch: {status_path}"
                    )
                if isinstance(previous.get("created_at"), str):
                    created_at = str(previous["created_at"])
                if previous.get("mode") == "historical_collection":
                    collection_summary = json.loads(json.dumps(previous))
                elif isinstance(previous.get("collection_summary"), Mapping):
                    collection_summary = json.loads(
                        json.dumps(previous["collection_summary"])
                    )
                for row in previous.get("blocked_dates", []):
                    if not isinstance(row, Mapping):
                        continue
                    day = row.get("date")
                    if day in blocked and isinstance(row.get("reason_code"), str):
                        blocked[str(day)] = {
                            "reason_code": str(row["reason_code"]),
                            "detail": str(row.get("detail", "")),
                        }
        except json.JSONDecodeError as exc:
            raise HistoricalLearningError(
                f"cannot parse existing batch status {status_path}: {exc}"
            ) from exc

    results: list[ReplayDayResult] = []
    replayed: list[str] = []

    def persist() -> dict[str, Any]:
        status = _batch_status(
            dates,
            seed=seed,
            ready_dates=[day for day in ready if day not in replayed],
            replayed_dates=[
                day for day in dates if day in set(already_replayed).union(replayed)
            ],
            newly_replayed_dates=replayed,
            already_replayed_dates=already_replayed,
            blocked=blocked,
            results=results,
            created_at=created_at,
            collection_summary=collection_summary,
        )
        if status_path is not None:
            _write_batch_status(status_path, status)
        return status

    initial_status = persist()
    created_at = str(initial_status["created_at"])
    for day in ready:
        try:
            result = run_replay_bundle(
                load_bundle(by_date[day]),
                ledger_path=ledger_path,
                active_root=active_root,
                archive_root=archive_root,
                config=config,
                seed=seed,
            )
        except Exception as exc:
            blocked[day] = {
                "reason_code": "replay_failed",
                "detail": str(exc),
            }
            persist()
            raise
        results.append(result)
        replayed.append(day)
        persist()
    return persist()


def run_batch(
    count: int,
    *,
    data_root: Path = DEFAULT_DATA_ROOT,
    ledger_path: Path = DEFAULT_LEDGER_PATH,
    active_root: Path = DEFAULT_ACTIVE_ROOT,
    archive_root: Path = DEFAULT_ARCHIVE_ROOT,
    seed: int | None = None,
) -> dict[str, Any]:
    if count < 1:
        raise HistoricalLearningError("day count must be at least 1")
    paths = [
        path
        for day, path in _bundles_by_date(data_root).items()
        if not (archive_root / day.replace("-", "_")).exists()
    ]
    if len(paths) < count:
        raise HistoricalLearningError(
            f"only {len(paths)} unarchived replay bundles are available; collect more point-in-time data first"
        )
    actual_seed = seed if seed is not None else secrets.randbits(63)
    selected_paths = random.Random(actual_seed).sample(paths, count)
    results = [
        run_replay_bundle(
            load_bundle(path),
            ledger_path=ledger_path,
            active_root=active_root,
            archive_root=archive_root,
            seed=actual_seed,
        )
        for path in selected_paths
    ]
    return {
        "seed": actual_seed,
        "requested_days": count,
        "completed_days": len(results),
        "results": [asdict(result) for result in results],
    }


def _positive_count(prompt: str) -> int:
    try:
        value = int(input(prompt).strip())
    except (EOFError, ValueError) as exc:
        raise HistoricalLearningError("day count must be an integer") from exc
    if value < 0:
        raise HistoricalLearningError("day count cannot be negative")
    return value


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ledger", type=Path, default=DEFAULT_LEDGER_PATH)
    parser.add_argument("--active-root", type=Path, default=DEFAULT_ACTIVE_ROOT)
    parser.add_argument("--archive-root", type=Path, default=DEFAULT_ARCHIVE_ROOT)
    parser.add_argument("--data-root", type=Path, default=DEFAULT_DATA_ROOT)
    subparsers = parser.add_subparsers(dest="command", required=True)
    validate = subparsers.add_parser("validate", help="validate one replay bundle")
    validate.add_argument("bundle", type=Path)
    select = subparsers.add_parser(
        "select", help="randomly select unarchived dates from a verified calendar"
    )
    select.add_argument(
        "calendar", type=Path, help="JSON array of exchange trading dates"
    )
    select.add_argument("--days", type=int, required=True)
    select.add_argument("--seed", type=int, default=None)
    run = subparsers.add_parser("run", help="run selected or random bundles")
    run_group = run.add_mutually_exclusive_group(required=True)
    run_group.add_argument("--days", type=int)
    run_group.add_argument(
        "--selection",
        type=Path,
        help="selection JSON emitted before point-in-time data collection",
    )
    run.add_argument("--seed", type=int, default=None)
    run.add_argument(
        "--ready-only",
        action="store_true",
        help="replay valid selected dates and report blocked dates without substitution",
    )
    run.add_argument(
        "--status-output",
        type=Path,
        help="atomic machine-readable batch status (defaults under historical_batches with --ready-only)",
    )
    interactive = subparsers.add_parser(
        "interactive", help="ask how many days to replay and whether to continue"
    )
    interactive.add_argument("--seed", type=int, default=None)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        if args.command == "validate":
            validate_bundle(load_bundle(args.bundle))
            result: Any = {"valid": True, "bundle": str(args.bundle)}
        elif args.command == "select":
            calendar = json.loads(args.calendar.read_text(encoding="utf-8"))
            if not isinstance(calendar, list):
                raise HistoricalLearningError("calendar must be a JSON array")
            days, actual_seed = select_random_dates(
                calendar,
                args.days,
                args.archive_root,
                seed=args.seed,
            )
            result = {"seed": actual_seed, "selected_dates": days}
        elif args.command == "run":
            if args.selection:
                selection = json.loads(args.selection.read_text(encoding="utf-8"))
                if not isinstance(selection, Mapping) or not isinstance(
                    selection.get("selected_dates"), list
                ):
                    raise HistoricalLearningError(
                        "selection must contain a selected_dates array"
                    )
                selection_seed = selection.get("seed")
                if not isinstance(selection_seed, int):
                    raise HistoricalLearningError(
                        "selection must contain an integer seed"
                    )
                result = run_selected_dates(
                    selection["selected_dates"],
                    data_root=args.data_root,
                    ledger_path=args.ledger,
                    active_root=args.active_root,
                    archive_root=args.archive_root,
                    seed=selection_seed,
                    ready_only=args.ready_only,
                    status_path=(
                        args.status_output
                        or DEFAULT_BATCH_ROOT
                        / f"historical-{_selection_hash(selection['selected_dates'], selection_seed)[:16]}.json"
                        if args.ready_only
                        else args.status_output
                    ),
                )
            else:
                result = run_batch(
                    args.days,
                    data_root=args.data_root,
                    ledger_path=args.ledger,
                    active_root=args.active_root,
                    archive_root=args.archive_root,
                    seed=args.seed,
                )
        else:
            batches: list[dict[str, Any]] = []
            count = _positive_count("How many historical days should be simulated? ")
            next_seed = args.seed
            while count:
                batch = run_batch(
                    count,
                    data_root=args.data_root,
                    ledger_path=args.ledger,
                    active_root=args.active_root,
                    archive_root=args.archive_root,
                    seed=next_seed,
                )
                batches.append(batch)
                next_seed = None
                count = _positive_count(
                    "How many additional historical days should be simulated? (0 to stop) "
                )
            result = {
                "batches": batches,
                "completed_days": sum(item["completed_days"] for item in batches),
            }
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    except (
        OSError,
        json.JSONDecodeError,
        HistoricalLearningError,
        LedgerError,
        LifecycleError,
        StrategyInputError,
    ) as exc:
        print(
            json.dumps({"error": str(exc), "error_type": type(exc).__name__}, indent=2),
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

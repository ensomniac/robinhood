"""Outcome-blind trigger reconstruction for the preregistered ORB retest challenger."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from typing import Any

from sip_trade_conditions import classify_trade_conditions


HYPOTHESIS_SHA256 = "b766000bc84aff7ae836c8dcdc516d4b1c7dc4fb3dcb5f179bb5eb452656d105"
RULE_VERSION = "catalyst-orb-retest-v1"


class ChallengerOrbRetestError(ValueError):
    """The causal trigger surface is incomplete or malformed."""


@dataclass(frozen=True)
class RetestTrigger:
    initial_break_at_et: str
    initial_break_price: float
    retest_bar_start_et: str
    retest_bar_high: float
    retest_bar_low: float
    retest_bar_close: float
    rebreak_at_et: str
    rebreak_price: float
    decision_at_et: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _observed(row: Mapping[str, Any]) -> datetime:
    raw = str(row.get("source_timestamp") or row.get("time_et") or "")
    try:
        value = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ChallengerOrbRetestError("row timestamp is malformed") from exc
    if value.tzinfo is None:
        raise ChallengerOrbRetestError("row timestamp lacks timezone")
    return value


def _ordered_trades(
    rows: Sequence[Mapping[str, Any]], *, start: datetime, end: datetime
) -> list[Mapping[str, Any]]:
    if start.tzinfo is None or end.tzinfo is None or end <= start:
        raise ChallengerOrbRetestError("trade window is invalid")
    ordered = sorted(rows, key=lambda row: (_observed(row), str(row.get("trade_id", ""))))
    if any(not (start <= _observed(row) < end) for row in ordered):
        raise ChallengerOrbRetestError("trade row is outside the causal window")
    return ordered


def _clean_cross(
    rows: Sequence[Mapping[str, Any]], *, trigger: float
) -> Mapping[str, Any] | None:
    if trigger <= 0:
        raise ChallengerOrbRetestError("trigger must be positive")
    return next(
        (
            row
            for row in rows
            if float(row.get("price", 0)) > trigger
            and classify_trade_conditions(
                row.get("tape"), row.get("conditions")
            ).establishes_continuous_cross
        ),
        None,
    )


def _first_retest_bar(
    rows: Sequence[Mapping[str, Any]],
    *,
    initial_break_at: datetime,
    opening_high: float,
    cutoff: datetime,
) -> tuple[Mapping[str, Any] | None, bool | None]:
    initial_minute = initial_break_at.replace(second=0, microsecond=0)
    ordered = sorted(rows, key=_observed)
    for row in ordered:
        bar_start = _observed(row)
        bar_end = bar_start + timedelta(minutes=1)
        if row.get("interpolated") is True:
            raise ChallengerOrbRetestError("retest bars contain interpolation")
        if bar_start <= initial_minute or bar_end > cutoff:
            continue
        if float(row.get("low", 0)) <= opening_high:
            return row, float(row.get("close", 0)) >= opening_high
    return None, None


def evaluate_retest_trigger(
    *,
    opening_high: float,
    search_start: datetime,
    cutoff: datetime,
    captured_through: datetime,
    trades: Sequence[Mapping[str, Any]],
    completed_minute_bars: Sequence[Mapping[str, Any]],
    trade_window_complete: bool,
    bar_window_complete: bool,
) -> dict[str, Any]:
    """Rebuild the frozen first-break, first-touch retest, and rebreak sequence.

    This function consumes no quote, fill, post-entry, or outcome data. The
    caller must attest complete causal trade and completed-bar windows; missing
    windows fail closed rather than becoming a favorable no-signal result.
    """

    if (
        opening_high <= 0
        or cutoff <= search_start
        or captured_through <= search_start
        or captured_through > cutoff
    ):
        raise ChallengerOrbRetestError("opening range or session window is invalid")
    if not trade_window_complete or not bar_window_complete:
        raise ChallengerOrbRetestError("causal trigger windows are incomplete")
    ordered_trades = _ordered_trades(
        trades, start=search_start, end=captured_through
    )
    completed_rows = sorted(completed_minute_bars, key=_observed)
    if any(
        not (
            search_start <= _observed(row)
            and _observed(row) + timedelta(minutes=1) <= captured_through
        )
        for row in completed_rows
    ):
        raise ChallengerOrbRetestError("completed bar is outside the causal window")
    initial = _clean_cross(ordered_trades, trigger=opening_high)
    base = {
        "schema_version": 1,
        "rule_version": RULE_VERSION,
        "hypothesis_sha256": HYPOTHESIS_SHA256,
        "opening_high": float(opening_high),
        "target_outcome_observed_or_derived": False,
    }
    if initial is None:
        if captured_through != cutoff:
            raise ChallengerOrbRetestError("initial-break search stopped early")
        return {**base, "terminal_reason": "NO_INITIAL_BREAK", "trigger": None}

    initial_at = _observed(initial)
    retest, held = _first_retest_bar(
        completed_rows,
        initial_break_at=initial_at,
        opening_high=opening_high,
        cutoff=cutoff,
    )
    if retest is None:
        if captured_through != cutoff:
            raise ChallengerOrbRetestError("retest search stopped early")
        return {**base, "terminal_reason": "NO_RETEST_BEFORE_CUTOFF", "trigger": None}
    if held is not True:
        retest_end = _observed(retest) + timedelta(minutes=1)
        if captured_through != retest_end:
            raise ChallengerOrbRetestError("failed retest retained later data")
        return {**base, "terminal_reason": "RETEST_HOLD_FAILED", "trigger": None}

    retest_start = _observed(retest)
    retest_end = retest_start + timedelta(minutes=1)
    later = [row for row in ordered_trades if _observed(row) >= retest_end]
    rebreak = _clean_cross(later, trigger=float(retest["high"]))
    if rebreak is None:
        if captured_through != cutoff:
            raise ChallengerOrbRetestError("rebreak search stopped early")
        return {**base, "terminal_reason": "NO_REBREAK_BEFORE_CUTOFF", "trigger": None}
    rebreak_at = _observed(rebreak)
    decision_at = rebreak_at + timedelta(seconds=10)
    if decision_at > cutoff:
        if captured_through != cutoff:
            raise ChallengerOrbRetestError("late rebreak did not stop at cutoff")
        return {
            **base,
            "terminal_reason": "FINAL_DECISION_AFTER_CUTOFF",
            "trigger": None,
        }
    if captured_through != decision_at:
        raise ChallengerOrbRetestError("trigger evidence does not end at decision")
    trigger = RetestTrigger(
        initial_break_at_et=initial_at.isoformat(),
        initial_break_price=float(initial["price"]),
        retest_bar_start_et=retest_start.isoformat(),
        retest_bar_high=float(retest["high"]),
        retest_bar_low=float(retest["low"]),
        retest_bar_close=float(retest["close"]),
        rebreak_at_et=rebreak_at.isoformat(),
        rebreak_price=float(rebreak["price"]),
        decision_at_et=decision_at.isoformat(),
    )
    return {**base, "terminal_reason": "TRIGGER_FOUND", "trigger": trigger.to_dict()}

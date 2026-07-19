"""Reconstruct Alpaca SIP minute bars from raw trades and condition rules."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any


SOURCE_URL = (
    "https://docs.alpaca.markets/us/docs/market-data-faq#how-are-bars-aggregated"
)
RULE_VERSION = "alpaca-sip-minute-ohlcv-v1"
GREEN = 2
YELLOW = 1
RED = 0

# Minute-bar rules transcribed from Alpaca's official condition table. Each
# tuple is (open/close, high/low, volume). Unknown or tape-inapplicable codes
# fail closed. No minute rule is yellow in the current table, but the state
# machine supports it because Alpaca defines yellow as first-trade-only.
_RULES: dict[str, dict[str, tuple[int, int, int]]] = {
    "A": {
        " ": (GREEN, GREEN, GREEN),
        "B": (RED, RED, GREEN),
        "C": (RED, RED, GREEN),
        "E": (GREEN, GREEN, GREEN),
        "F": (GREEN, GREEN, GREEN),
        "H": (RED, RED, GREEN),
        "I": (RED, RED, GREEN),
        "K": (GREEN, GREEN, GREEN),
        "L": (GREEN, GREEN, GREEN),
        "M": (RED, RED, RED),
        "N": (RED, RED, GREEN),
        "O": (GREEN, GREEN, GREEN),
        "P": (RED, RED, GREEN),
        "Q": (RED, RED, RED),
        "R": (RED, RED, GREEN),
        "T": (GREEN, GREEN, GREEN),
        "U": (RED, RED, GREEN),
        "V": (RED, RED, GREEN),
        "X": (GREEN, GREEN, GREEN),
        "Z": (RED, RED, GREEN),
        "4": (RED, RED, GREEN),
        "5": (GREEN, GREEN, GREEN),
        "6": (GREEN, GREEN, GREEN),
        "7": (RED, RED, GREEN),
        "9": (RED, RED, RED),
    },
    "B": {
        " ": (GREEN, GREEN, GREEN),
        "B": (RED, RED, GREEN),
        "C": (RED, RED, GREEN),
        "E": (GREEN, GREEN, GREEN),
        "F": (GREEN, GREEN, GREEN),
        "H": (RED, RED, GREEN),
        "I": (RED, RED, GREEN),
        "K": (GREEN, GREEN, GREEN),
        "L": (GREEN, GREEN, GREEN),
        "M": (RED, RED, RED),
        "N": (RED, RED, GREEN),
        "O": (GREEN, GREEN, GREEN),
        "P": (RED, RED, GREEN),
        "Q": (RED, RED, RED),
        "R": (RED, RED, GREEN),
        "T": (GREEN, GREEN, GREEN),
        "U": (RED, RED, GREEN),
        "V": (RED, RED, GREEN),
        "X": (GREEN, GREEN, GREEN),
        "Z": (RED, RED, GREEN),
        "4": (RED, RED, GREEN),
        "5": (GREEN, GREEN, GREEN),
        "6": (GREEN, GREEN, GREEN),
        "7": (RED, RED, GREEN),
        "9": (RED, RED, RED),
    },
    "C": {
        "@": (GREEN, GREEN, GREEN),
        "A": (GREEN, GREEN, GREEN),
        "B": (GREEN, GREEN, GREEN),
        "C": (RED, RED, GREEN),
        "D": (GREEN, GREEN, GREEN),
        "F": (GREEN, GREEN, GREEN),
        "G": (RED, RED, GREEN),
        "H": (RED, RED, GREEN),
        "I": (RED, RED, GREEN),
        "K": (GREEN, GREEN, GREEN),
        "L": (GREEN, GREEN, GREEN),
        "M": (RED, RED, RED),
        "N": (RED, RED, GREEN),
        "O": (GREEN, GREEN, GREEN),
        "P": (RED, RED, GREEN),
        "Q": (RED, RED, RED),
        "R": (RED, RED, GREEN),
        "T": (GREEN, GREEN, GREEN),
        "U": (RED, RED, GREEN),
        "V": (RED, RED, GREEN),
        "W": (RED, RED, GREEN),
        "X": (GREEN, GREEN, GREEN),
        "Y": (GREEN, GREEN, GREEN),
        "Z": (RED, RED, GREEN),
        "4": (RED, RED, GREEN),
        "5": (GREEN, GREEN, GREEN),
        "6": (GREEN, GREEN, GREEN),
        "7": (RED, RED, GREEN),
        "9": (RED, RED, RED),
    },
}


@dataclass(frozen=True)
class MinuteUpdateDecision:
    tape: str
    conditions: tuple[str, ...]
    update_open_close: int
    update_high_low: int
    update_volume: int
    supported: bool


def classify_minute_update(tape: object, conditions: object) -> MinuteUpdateDecision:
    normalized_tape = str(tape or "").strip().upper()
    if isinstance(conditions, str):
        normalized_conditions = (conditions,)
    elif isinstance(conditions, Sequence):
        normalized_conditions = tuple(str(item) for item in conditions)
    else:
        normalized_conditions = ()
    tape_rules = _RULES.get(normalized_tape)
    if tape_rules is None or not normalized_conditions:
        return MinuteUpdateDecision(
            normalized_tape, normalized_conditions, RED, RED, RED, False
        )
    decisions = [tape_rules.get(code) for code in normalized_conditions]
    if any(decision is None for decision in decisions):
        return MinuteUpdateDecision(
            normalized_tape, normalized_conditions, RED, RED, RED, False
        )
    typed = [decision for decision in decisions if decision is not None]
    return MinuteUpdateDecision(
        normalized_tape,
        normalized_conditions,
        min(item[0] for item in typed),
        min(item[1] for item in typed),
        min(item[2] for item in typed),
        True,
    )


def _timestamp(row: Mapping[str, Any]) -> datetime:
    raw = str(row.get("source_timestamp") or row.get("time_et") or "")
    parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed


def aggregate_minute(trades: Sequence[Mapping[str, Any]]) -> dict[str, Any] | None:
    """Aggregate one chronological SIP minute with Alpaca's strictest rules."""
    if not trades:
        return None
    ordered = sorted(
        trades, key=lambda row: (_timestamp(row), str(row.get("trade_id", "")))
    )
    minute = _timestamp(ordered[0]).replace(second=0, microsecond=0)
    if any(
        _timestamp(row).replace(second=0, microsecond=0) != minute for row in ordered
    ):
        raise ValueError("aggregate_minute received trades from multiple minutes")
    opened: float | None = None
    high: float | None = None
    low: float | None = None
    closed: float | None = None
    volume = 0
    count = 0
    weighted_price = 0.0
    weighted_volume = 0
    unsupported = 0
    for row in ordered:
        price = float(row["price"])
        size = int(row["size"])
        if price <= 0 or size <= 0:
            raise ValueError("trade price and size must be positive")
        decision = classify_minute_update(row.get("tape"), row.get("conditions"))
        unsupported += int(not decision.supported)
        open_close = decision.update_open_close == GREEN or (
            decision.update_open_close == YELLOW and opened is None
        )
        high_low = decision.update_high_low == GREEN
        update_volume = decision.update_volume == GREEN
        if open_close:
            if opened is None:
                opened = price
            closed = price
        if high_low:
            high = price if high is None else max(high, price)
            low = price if low is None else min(low, price)
        if update_volume:
            volume += size
            count += 1
        if high_low and update_volume:
            weighted_price += price * size
            weighted_volume += size
    if (
        opened is None
        or high is None
        or low is None
        or closed is None
        or volume <= 0
        or weighted_volume <= 0
    ):
        return None
    return {
        "time": minute.isoformat(),
        "open": opened,
        "high": high,
        "low": low,
        "close": closed,
        "volume": volume,
        "count": count,
        "wap": weighted_price / weighted_volume,
        "vwap_eligible_volume": weighted_volume,
        "unsupported_trade_count": unsupported,
        "source_trade_count": len(ordered),
    }


def aggregate_prefix_vwap(trades: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """Compute exact eligible VWAP state across an arbitrary chronological prefix."""
    weighted_price = 0.0
    weighted_volume = 0
    volume = 0
    count = 0
    unsupported = 0
    for row in trades:
        price = float(row["price"])
        size = int(row["size"])
        decision = classify_minute_update(row.get("tape"), row.get("conditions"))
        unsupported += int(not decision.supported)
        if decision.update_volume == GREEN:
            volume += size
            count += 1
        if decision.update_high_low == GREEN and decision.update_volume == GREEN:
            weighted_price += price * size
            weighted_volume += size
    return {
        "wap": weighted_price / weighted_volume if weighted_volume else None,
        "vwap_eligible_volume": weighted_volume,
        "reported_volume": volume,
        "eligible_trade_count": count,
        "unsupported_trade_count": unsupported,
        "source_trade_count": len(trades),
    }

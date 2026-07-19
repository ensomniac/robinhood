"""Interpret consolidated-tape trade conditions for faithful ORB replay.

The bar-eligibility rules below transcribe Alpaca's published minute-bar
aggregation table, which in turn cites the CTA/UTP SIP specifications.  The
separate continuous-cross contract is deliberately narrower: a production ORB
trigger should be established by an ordinary continuous-market execution, not
merely by any print that a data vendor is allowed to include in a bar high.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass


SOURCE_URL = (
    "https://docs.alpaca.markets/us/docs/market-data-faq#how-are-bars-aggregated"
)
RULE_VERSION = "alpaca-sip-minute-v1"
CONTINUOUS_CROSS_VERSION = "continuous-regular-cross-v1"

# Conditions that update a minute bar's high/low. Conditions absent from a
# tape's set are either explicitly ineligible in the source table or invalid
# for that tape. Alpaca applies the strictest rule when multiple conditions are
# present, so every condition must be green.
_MINUTE_HIGH_LOW_GREEN = {
    "A": frozenset({" ", "E", "F", "K", "L", "O", "T", "X", "5", "6"}),
    "B": frozenset({" ", "E", "F", "K", "L", "O", "T", "X", "5", "6"}),
    "C": frozenset({"@", "A", "B", "D", "F", "K", "L", "O", "T", "X", "Y", "5", "6"}),
}

# A clean breakout is narrower than bar eligibility. It must carry the tape's
# regular-sale condition and may only add continuous-execution modifiers. An
# intermarket sweep is a real continuous execution. Automatic execution is an
# AB-tape continuous execution. Special settlements, auctions, crosses,
# extended-hours markers, late/out-of-sequence reports, odd lots, and corrected
# prints are excluded even where a vendor permits them to update a minute high.
_REGULAR_SALE = {"A": " ", "B": " ", "C": "@"}
_CONTINUOUS_MODIFIERS = {
    "A": frozenset({"E", "F"}),
    "B": frozenset({"E", "F"}),
    "C": frozenset({"F"}),
}


@dataclass(frozen=True)
class TradeConditionDecision:
    """Two explicitly different decisions for one raw SIP trade."""

    tape: str
    conditions: tuple[str, ...]
    updates_minute_high_low: bool
    establishes_continuous_cross: bool
    reason: str


def _normalize(tape: object, conditions: object) -> tuple[str, tuple[str, ...]]:
    normalized_tape = str(tape or "").strip().upper()
    if isinstance(conditions, str):
        normalized_conditions = (conditions,)
    elif isinstance(conditions, Sequence):
        normalized_conditions = tuple(str(item) for item in conditions)
    else:
        normalized_conditions = ()
    return normalized_tape, normalized_conditions


def classify_trade_conditions(
    tape: object, conditions: object
) -> TradeConditionDecision:
    """Classify minute-high eligibility and clean continuous-cross eligibility."""
    normalized_tape, normalized_conditions = _normalize(tape, conditions)
    green = _MINUTE_HIGH_LOW_GREEN.get(normalized_tape)
    if green is None:
        return TradeConditionDecision(
            normalized_tape,
            normalized_conditions,
            False,
            False,
            "unsupported_tape",
        )
    if not normalized_conditions:
        return TradeConditionDecision(
            normalized_tape,
            normalized_conditions,
            False,
            False,
            "missing_conditions",
        )
    ineligible = tuple(code for code in normalized_conditions if code not in green)
    updates = not ineligible
    regular = _REGULAR_SALE[normalized_tape]
    establishes = (
        updates
        and regular in normalized_conditions
        and all(
            code == regular or code in _CONTINUOUS_MODIFIERS[normalized_tape]
            for code in normalized_conditions
        )
    )
    if ineligible:
        reason = "minute_high_low_ineligible:" + ",".join(ineligible)
    elif not establishes:
        reason = "special_print_not_continuous_regular_sale"
    else:
        reason = "continuous_regular_sale"
    return TradeConditionDecision(
        normalized_tape,
        normalized_conditions,
        updates,
        establishes,
        reason,
    )


def updates_minute_high_low(tape: object, conditions: object) -> bool:
    """Return whether Alpaca's published strictest-rule logic updates H/L."""
    return classify_trade_conditions(tape, conditions).updates_minute_high_low


def establishes_continuous_cross(tape: object, conditions: object) -> bool:
    """Return whether the print can establish a clean continuous ORB cross."""
    return classify_trade_conditions(tape, conditions).establishes_continuous_cross

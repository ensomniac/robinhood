"""Provider-neutral metrics shared by historical preflight and bundle assembly."""

from __future__ import annotations

import math
import statistics
from collections.abc import Mapping, Sequence
from typing import Any


class HistoricalMetricError(ValueError):
    """Raised when historical bars cannot support a requested metric."""


def _number(value: Any, field: str, *, minimum: float | None = None) -> float:
    if isinstance(value, bool):
        raise HistoricalMetricError(f"{field} must be numeric")
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise HistoricalMetricError(f"{field} must be numeric") from exc
    if not math.isfinite(result) or (minimum is not None and result < minimum):
        raise HistoricalMetricError(f"{field} is outside its valid range")
    return result


def _ordered_daily_bars(
    rows: Sequence[Mapping[str, Any]], required: int
) -> list[Mapping[str, Any]]:
    if len(rows) < required:
        raise HistoricalMetricError(
            f"daily history needs at least {required} bars, got {len(rows)}"
        )
    try:
        ordered = sorted(rows, key=lambda row: int(row["epoch"]))
    except (KeyError, TypeError, ValueError) as exc:
        raise HistoricalMetricError("daily bars need integer epoch values") from exc
    epochs = [int(row["epoch"]) for row in ordered]
    if len(epochs) != len(set(epochs)):
        raise HistoricalMetricError("daily bars contain duplicate epochs")
    return ordered


def average_daily_volume(rows: Sequence[Mapping[str, Any]], periods: int = 14) -> float:
    """Return mean volume over the latest completed daily bars."""
    if periods < 1:
        raise HistoricalMetricError("volume periods must be positive")
    ordered = _ordered_daily_bars(rows, periods)[-periods:]
    volumes = [
        _number(row.get("volume"), "daily volume", minimum=0.0) for row in ordered
    ]
    return statistics.fmean(volumes)


def average_true_range(rows: Sequence[Mapping[str, Any]], periods: int = 14) -> float:
    """Return Wilder-style simple ATR from completed daily OHLC bars."""
    if periods < 1:
        raise HistoricalMetricError("ATR periods must be positive")
    ordered = _ordered_daily_bars(rows, periods + 1)[-(periods + 1) :]
    ranges: list[float] = []
    for previous, current in zip(ordered, ordered[1:]):
        high = _number(current.get("high"), "daily high", minimum=0.0)
        low = _number(current.get("low"), "daily low", minimum=0.0)
        previous_close = _number(
            previous.get("close"), "previous daily close", minimum=0.0
        )
        if min(high, low, previous_close) <= 0:
            raise HistoricalMetricError("daily OHLC prices must be positive")
        if high < low:
            raise HistoricalMetricError("daily high cannot be below daily low")
        ranges.append(
            max(high - low, abs(high - previous_close), abs(low - previous_close))
        )
    return statistics.fmean(ranges)

"""Shared, no-lookahead runtime for the three frozen dense strategy families.

The runtime is intentionally provider-agnostic.  It consumes one already-frozen,
point-in-time dataset per process, emits every attempted candidate, and delegates
portfolio sizing/contention and chronological compounding to
``simulate_portfolio_account``.
"""

from __future__ import annotations

import math
import statistics
from collections.abc import Mapping, Sequence
from datetime import date, datetime, time, timedelta
from typing import Any

from learning_statistics import (
    maximum_drawdown_fraction,
    profit_factor,
    simulate_portfolio_account,
)


EQUITY_RESIDUAL_FAMILY = "liquid-equity-market-residual-reversal"
EQUITY_RESIDUAL_REPLICATION_FAMILY = (
    "liquid-equity-market-residual-reversal-replication"
)
EQUITY_RESIDUAL_TEMPORAL_FAMILY = (
    "liquid-equity-market-residual-reversal-temporal-expansion-v7"
)
ETF_RESIDUAL_REPLICATION_FAMILY = (
    "liquid-etf-market-residual-reversal-replication-v1"
)
ETF_RESIDUAL_REPLICATION_TARGET_SYMBOLS = (
    "IJH",
    "IJR",
    "IWD",
    "IWF",
    "IWN",
    "IWO",
    "SPLG",
    "VB",
    "VO",
    "VXF",
)
ETF_RESIDUAL_REPLICATION_FEATURE_SYMBOL = "SPY"
ETF_RESIDUAL_REPLICATION_V2_FAMILY = (
    "liquid-etf-market-residual-reversal-replication-v2"
)
ETF_RESIDUAL_REPLICATION_V2_TARGET_SYMBOLS = (
    "IBB",
    "IGV",
    "ITB",
    "MTUM",
    "QUAL",
    "SOXX",
    "USMV",
    "VEA",
    "VLUE",
    "VWO",
)
ETF_RESIDUAL_REPLICATION_V3_FAMILY = (
    "liquid-etf-market-residual-reversal-replication-v3"
)
ETF_RESIDUAL_REPLICATION_V3_TARGET_SYMBOLS = (
    "IYC",
    "IYE",
    "IYF",
    "IYH",
    "IYJ",
    "IYK",
    "IYM",
    "IYR",
    "IYW",
    "IYZ",
)
EQUITY_RESIDUAL_FAMILIES = {
    EQUITY_RESIDUAL_FAMILY,
    EQUITY_RESIDUAL_REPLICATION_FAMILY,
    EQUITY_RESIDUAL_TEMPORAL_FAMILY,
}
RESIDUAL_FAMILIES = {
    *EQUITY_RESIDUAL_FAMILIES,
    ETF_RESIDUAL_REPLICATION_FAMILY,
    ETF_RESIDUAL_REPLICATION_V2_FAMILY,
    ETF_RESIDUAL_REPLICATION_V3_FAMILY,
}
INTRADAY_ETF_FAMILY = "intraday-index-etf-opening-reversal"
COUNTRY_ETF_OPENING_REVERSAL_FAMILY = "country-etf-opening-reversal"
LIQUID_INDEX_ETF_OPENING_REVERSAL_FAMILY = (
    "liquid-index-etf-opening-reversal-long-history"
)
LIQUID_INDEX_ETF_OPENING_REVERSAL_POST2016_FAMILY = (
    "liquid-index-etf-opening-reversal-post2016"
)
INDEX_ETF_OPENING_MOMENTUM_FAMILY = (
    "liquid-index-etf-opening-momentum"
)
INTRADAY_ETF_FAMILIES = {
    INTRADAY_ETF_FAMILY,
    COUNTRY_ETF_OPENING_REVERSAL_FAMILY,
    LIQUID_INDEX_ETF_OPENING_REVERSAL_FAMILY,
    LIQUID_INDEX_ETF_OPENING_REVERSAL_POST2016_FAMILY,
    INDEX_ETF_OPENING_MOMENTUM_FAMILY,
}
ETF_PULLBACK_FAMILY = "liquid-etf-trend-pullback-cost-floor"
ETF_PULLBACK_REPLICATION_FAMILY = (
    "liquid-etf-trend-pullback-cost-floor-replication-v5"
)
ETF_PULLBACK_REPLICATION_SYMBOLS = (
    "VAW",
    "VCR",
    "VDC",
    "VDE",
    "VFH",
    "VGT",
    "VHT",
    "VIS",
    "VOX",
    "VPU",
)
ETF_PULLBACK_FAMILIES = {
    ETF_PULLBACK_FAMILY,
    ETF_PULLBACK_REPLICATION_FAMILY,
}
SPY_RSI2_PULLBACK_FAMILY = "spy-rsi2-trend-pullback"
ETF_CROSS_SECTIONAL_MOMENTUM_FAMILY = "liquid-etf-cross-sectional-momentum"
LIQUID_EQUITY_MOMENTUM_FAMILY = "liquid-equity-cross-sectional-momentum"
ETF_CROSS_SECTIONAL_REVERSAL_FAMILY = "liquid-etf-cross-sectional-reversal"
ETF_HIGH_CONTINUATION_FAMILY = "liquid-etf-52-week-high-continuation"
ETF_TURN_OF_MONTH_FAMILY = "liquid-etf-turn-of-month-seasonality"
SECTOR_ETF_ROTATION_FAMILY = "liquid-sector-etf-rotation"
SECTOR_ETF_GAP_DRIFT_FAMILY = "sector-etf-gap-drift-continuation"
FLIGHT_TO_SAFETY_REBOUND_FAMILY = (
    "cross-asset-flight-to-safety-equity-rebound"
)
FLIGHT_TO_SAFETY_TARGET_SYMBOLS = ("MDY", "VOO", "VTI")
FLIGHT_TO_SAFETY_FEATURE_SYMBOL = "TLT"
FLIGHT_TO_SAFETY_REPLICATION_FAMILY = (
    "cross-asset-flight-to-safety-equity-rebound-replication"
)
FLIGHT_TO_SAFETY_REPLICATION_TARGET_SYMBOLS = ("IWB", "SCHX", "SPTM")
FLIGHT_TO_SAFETY_REPLICATION_V2_FAMILY = (
    "cross-asset-flight-to-safety-equity-rebound-replication-v2"
)
FLIGHT_TO_SAFETY_REPLICATION_V2_TARGET_SYMBOLS = ("ITOT", "RSP", "VV")
BREADTH_CAPITULATION_REBOUND_FAMILY = (
    "broad-equity-etf-breadth-capitulation-rebound"
)
BREADTH_CAPITULATION_SYMBOLS = (
    "ITOT",
    "IWB",
    "RSP",
    "SCHX",
    "SPTM",
    "VV",
)
CROSS_STYLE_BREADTH_CONTINUATION_FAMILY = (
    "cross-style-etf-breadth-continuation"
)
CROSS_STYLE_BREADTH_SYMBOLS = (
    "SCHG",
    "SCHV",
    "SPYG",
    "SPYV",
    "VONE",
    "VTV",
    "VTWO",
    "VUG",
)
CROSS_STYLE_BREADTH_TARGET_SYMBOL = "SCHG"
STYLE_ETF_BREAKOUT_CONTINUATION_FAMILY = (
    "style-etf-20-day-breakout-continuation"
)
HIGH_BETA_ETF_OVERSOLD_FAMILY = "high-beta-etf-oversold-reversal"
BROAD_ASSET_ETF_OVERSOLD_FAMILY = "broad-asset-etf-oversold-reversal"
ETF_OVERSOLD_FAMILIES = {
    HIGH_BETA_ETF_OVERSOLD_FAMILY,
    BROAD_ASSET_ETF_OVERSOLD_FAMILY,
}
ETF_CLOSE_TO_OPEN_FAMILY = "liquid-etf-close-to-open-momentum"
CLOSE_TO_OPEN_ETF_SYMBOLS = ("QQQ", "IWM", "DIA")
OVERSOLD_REVERSAL_FAMILY = "gap-universe-oversold-reversal"
EQUITY_GAP_CONTINUATION_FAMILY = "equity-gap-continuation-development-search"
EARNINGS_PEAD_FAMILY = "earnings-positive-surprise-drift"
EARNINGS_SEC_REACTION_FAMILY = "earnings-sec-yoy-eps-reaction-drift"
VOLATILITY_COMPRESSION_FAMILY = (
    "gap-universe-volatility-compression-breakout"
)
SUPPORTED_FAMILIES = {
    *EQUITY_RESIDUAL_FAMILIES,
    ETF_RESIDUAL_REPLICATION_FAMILY,
    ETF_RESIDUAL_REPLICATION_V2_FAMILY,
    ETF_RESIDUAL_REPLICATION_V3_FAMILY,
    *INTRADAY_ETF_FAMILIES,
    ETF_PULLBACK_FAMILY,
    ETF_PULLBACK_REPLICATION_FAMILY,
    SPY_RSI2_PULLBACK_FAMILY,
    ETF_CROSS_SECTIONAL_MOMENTUM_FAMILY,
    LIQUID_EQUITY_MOMENTUM_FAMILY,
    ETF_CROSS_SECTIONAL_REVERSAL_FAMILY,
    ETF_HIGH_CONTINUATION_FAMILY,
    ETF_TURN_OF_MONTH_FAMILY,
    SECTOR_ETF_ROTATION_FAMILY,
    SECTOR_ETF_GAP_DRIFT_FAMILY,
    FLIGHT_TO_SAFETY_REBOUND_FAMILY,
    FLIGHT_TO_SAFETY_REPLICATION_FAMILY,
    FLIGHT_TO_SAFETY_REPLICATION_V2_FAMILY,
    BREADTH_CAPITULATION_REBOUND_FAMILY,
    CROSS_STYLE_BREADTH_CONTINUATION_FAMILY,
    STYLE_ETF_BREAKOUT_CONTINUATION_FAMILY,
    *ETF_OVERSOLD_FAMILIES,
    ETF_CLOSE_TO_OPEN_FAMILY,
    OVERSOLD_REVERSAL_FAMILY,
    EQUITY_GAP_CONTINUATION_FAMILY,
    EARNINGS_PEAD_FAMILY,
    EARNINGS_SEC_REACTION_FAMILY,
    VOLATILITY_COMPRESSION_FAMILY,
}
PRIMARY_ROUND_TRIP_COST_FRACTION = 0.001
MINIMUM_GROSS_TO_COST_MULTIPLE = 5.0
STANDARDIZATION_LOOKBACK = 60
OVERSOLD_SIGNAL_START_INDEX = 30
OVERSOLD_SIGNAL_END_INDEX = 300
OVERSOLD_FORCE_FLAT_INDEX = 380
GAP_VOLUME_LOOKBACK_BARS = 15
GAP_FORCE_FLAT_INDEX = 380
COMPRESSION_SIGNAL_START_INDEX = 45


class DenseStrategyRuntimeError(ValueError):
    """Frozen data or exact-rule evaluation is incomplete or inconsistent."""


def _number(value: Any, field: str, *, positive: bool = False) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise DenseStrategyRuntimeError(f"{field} must be numeric")
    result = float(value)
    if not math.isfinite(result) or (positive and result <= 0):
        qualifier = "positive and finite" if positive else "finite"
        raise DenseStrategyRuntimeError(f"{field} must be {qualifier}")
    return result


def _calendar(dataset: Mapping[str, Any]) -> list[str]:
    raw = dataset.get("evaluation_dates")
    if not isinstance(raw, list) or not raw or any(
        not isinstance(item, str) or not item for item in raw
    ):
        raise DenseStrategyRuntimeError("evaluation_dates must be non-empty text")
    if raw != sorted(raw) or len(raw) != len(set(raw)):
        raise DenseStrategyRuntimeError(
            "evaluation_dates must be unique and chronological"
        )
    return list(raw)


def _daily_series(dataset: Mapping[str, Any]) -> dict[str, list[dict[str, Any]]]:
    prepared = dataset.get("_prepared_daily_bars")
    if isinstance(prepared, dict):
        return prepared
    raw = dataset.get("daily_bars")
    if not isinstance(raw, Mapping) or not raw:
        raise DenseStrategyRuntimeError("daily_bars must be a non-empty object")
    normalized: dict[str, list[dict[str, Any]]] = {}
    for raw_symbol, raw_bars in raw.items():
        symbol = str(raw_symbol)
        if not isinstance(raw_bars, list) or not raw_bars:
            raise DenseStrategyRuntimeError(f"daily_bars.{symbol} must be non-empty")
        bars: list[dict[str, Any]] = []
        dates: list[str] = []
        for index, raw_bar in enumerate(raw_bars):
            if not isinstance(raw_bar, Mapping):
                raise DenseStrategyRuntimeError(
                    f"daily_bars.{symbol}[{index}] must be an object"
                )
            day = raw_bar.get("date")
            if not isinstance(day, str) or not day:
                raise DenseStrategyRuntimeError(
                    f"daily_bars.{symbol}[{index}].date is invalid"
                )
            bar = {"date": day}
            for field in ("open", "high", "low", "close", "volume"):
                bar[field] = _number(
                    raw_bar.get(field),
                    f"daily_bars.{symbol}[{index}].{field}",
                    positive=field != "volume",
                )
            if bar["volume"] < 0 or not (
                bar["low"] <= min(bar["open"], bar["close"])
                and bar["high"] >= max(bar["open"], bar["close"])
            ):
                raise DenseStrategyRuntimeError(
                    f"daily_bars.{symbol}[{index}] has invalid OHLCV"
                )
            dates.append(day)
            bars.append(bar)
        if dates != sorted(dates) or len(dates) != len(set(dates)):
            raise DenseStrategyRuntimeError(
                f"daily_bars.{symbol} dates must be unique and chronological"
            )
        normalized[symbol] = bars
    return normalized


def _fifteen_minute_sessions(
    dataset: Mapping[str, Any],
) -> dict[str, dict[str, list[dict[str, Any]]]]:
    prepared = dataset.get("_prepared_fifteen_minute_bars")
    if isinstance(prepared, dict):
        return prepared
    raw = dataset.get("fifteen_minute_bars")
    if not isinstance(raw, Mapping) or not raw:
        raise DenseStrategyRuntimeError(
            "fifteen_minute_bars must be a non-empty object"
        )
    sessions: dict[str, dict[str, list[dict[str, Any]]]] = {}
    for raw_day, raw_symbols in raw.items():
        day = str(raw_day)
        if not isinstance(raw_symbols, Mapping) or not raw_symbols:
            raise DenseStrategyRuntimeError(
                f"fifteen_minute_bars.{day} must contain symbols"
            )
        sessions[day] = {}
        for raw_symbol, raw_bars in raw_symbols.items():
            symbol = str(raw_symbol)
            if not isinstance(raw_bars, list) or not raw_bars:
                raise DenseStrategyRuntimeError(
                    f"fifteen_minute_bars.{day}.{symbol} is empty"
                )
            bars: list[dict[str, Any]] = []
            observed_times: list[datetime] = []
            for index, raw_bar in enumerate(raw_bars):
                if not isinstance(raw_bar, Mapping):
                    raise DenseStrategyRuntimeError(
                        f"fifteen_minute_bars.{day}.{symbol}[{index}] "
                        "must be an object"
                    )
                timestamp = raw_bar.get("timestamp")
                if not isinstance(timestamp, str):
                    raise DenseStrategyRuntimeError(
                        "fifteen-minute timestamp is invalid"
                    )
                try:
                    observed = datetime.fromisoformat(timestamp)
                except ValueError as exc:
                    raise DenseStrategyRuntimeError(
                        "fifteen-minute timestamp must be ISO formatted"
                    ) from exc
                if (
                    observed.tzinfo is None
                    or observed.date().isoformat() != day
                ):
                    raise DenseStrategyRuntimeError(
                        "fifteen-minute timestamp needs a timezone and "
                        "matching session date"
                    )
                bar = {"timestamp": timestamp}
                for field in ("open", "high", "low", "close", "volume"):
                    bar[field] = _number(
                        raw_bar.get(field),
                        (
                            f"fifteen_minute_bars.{day}.{symbol}"
                            f"[{index}].{field}"
                        ),
                        positive=field != "volume",
                    )
                if bar["volume"] < 0 or not (
                    bar["low"] <= min(bar["open"], bar["close"])
                    and bar["high"] >= max(bar["open"], bar["close"])
                ):
                    raise DenseStrategyRuntimeError(
                        "fifteen-minute OHLCV data is invalid"
                    )
                observed_times.append(observed)
                bars.append(bar)
            if (
                observed_times != sorted(observed_times)
                or len(observed_times) != len(set(observed_times))
                or observed_times[0].timetz().replace(tzinfo=None)
                != time(9, 30)
                or any(
                    right - left != timedelta(minutes=15)
                    for left, right in zip(
                        observed_times, observed_times[1:]
                    )
                )
                or any(
                    not time(9, 30)
                    <= item.timetz().replace(tzinfo=None)
                    < time(16, 0)
                    for item in observed_times
                )
            ):
                raise DenseStrategyRuntimeError(
                    "fifteen-minute bars must be unique, chronological, "
                    "and regular-session aligned"
                )
            sessions[day][symbol] = bars
    return sessions


def _bar_maps(
    series: Mapping[str, Sequence[Mapping[str, Any]]],
) -> dict[str, dict[str, Mapping[str, Any]]]:
    return {
        symbol: {str(bar["date"]): bar for bar in bars}
        for symbol, bars in series.items()
    }


def _true_ranges(bars: Sequence[Mapping[str, Any]]) -> list[float]:
    ranges: list[float] = []
    for index, bar in enumerate(bars):
        previous_close = (
            float(bars[index - 1]["close"]) if index else float(bar["close"])
        )
        ranges.append(
            max(
                float(bar["high"]) - float(bar["low"]),
                abs(float(bar["high"]) - previous_close),
                abs(float(bar["low"]) - previous_close),
            )
        )
    return ranges


def _atr(bars: Sequence[Mapping[str, Any]], end_index: int, period: int = 14) -> float | None:
    if end_index < period:
        return None
    ranges = _true_ranges(bars[: end_index + 1])
    return statistics.fmean(ranges[-period:])


def _sma(
    bars: Sequence[Mapping[str, Any]], end_index: int, period: int
) -> float | None:
    if end_index + 1 < period:
        return None
    return statistics.fmean(
        float(item["close"]) for item in bars[end_index - period + 1 : end_index + 1]
    )


def _rsi_wilder(
    bars: Sequence[Mapping[str, Any]], end_index: int, period: int = 2
) -> float | None:
    if end_index < period:
        return None
    closes = [float(item["close"]) for item in bars[: end_index + 1]]
    changes = [right - left for left, right in zip(closes, closes[1:])]
    gains = [max(change, 0.0) for change in changes]
    losses = [max(-change, 0.0) for change in changes]
    average_gain = statistics.fmean(gains[:period])
    average_loss = statistics.fmean(losses[:period])
    for gain, loss in zip(gains[period:], losses[period:], strict=True):
        average_gain = ((period - 1) * average_gain + gain) / period
        average_loss = ((period - 1) * average_loss + loss) / period
    if average_loss == 0:
        return 100.0 if average_gain > 0 else 50.0
    relative_strength = average_gain / average_loss
    return 100 - 100 / (1 + relative_strength)


def _etf_pullback_feature_cache(
    daily: Mapping[str, Sequence[Mapping[str, Any]]],
) -> dict[str, dict[str, dict[str, float | None]]]:
    """Compute completed-bar pullback features once for the full trial grid."""

    cached: dict[str, dict[str, dict[str, float | None]]] = {}
    for symbol, bars in daily.items():
        true_ranges = _true_ranges(bars)
        closes = [float(item["close"]) for item in bars]
        changes = [right - left for left, right in zip(closes, closes[1:])]
        gains = [max(change, 0.0) for change in changes]
        losses = [max(-change, 0.0) for change in changes]
        average_gain: float | None = None
        average_loss: float | None = None
        symbol_cache: dict[str, dict[str, float | None]] = {}
        for index, bar in enumerate(bars):
            rsi2: float | None = None
            if index == 2:
                average_gain = statistics.fmean(gains[:2])
                average_loss = statistics.fmean(losses[:2])
            elif index > 2:
                if average_gain is None or average_loss is None:
                    raise DenseStrategyRuntimeError(
                        "pullback RSI cache initialization failed"
                    )
                average_gain = (average_gain + gains[index - 1]) / 2
                average_loss = (average_loss + losses[index - 1]) / 2
            if index >= 2:
                if average_gain is None or average_loss is None:
                    raise DenseStrategyRuntimeError(
                        "pullback RSI cache is incomplete"
                    )
                if average_loss == 0:
                    rsi2 = 100.0 if average_gain > 0 else 50.0
                else:
                    relative_strength = average_gain / average_loss
                    rsi2 = 100 - 100 / (1 + relative_strength)
            symbol_cache[str(bar["date"])] = {
                "rsi2": rsi2,
                "atr14": (
                    statistics.fmean(true_ranges[index - 13 : index + 1])
                    if index >= 14
                    else None
                ),
                "decline3": (
                    closes[index] / closes[index - 3] - 1
                    if index >= 3
                    else None
                ),
                "decline1": (
                    closes[index] / closes[index - 1] - 1
                    if index >= 1
                    else None
                ),
                "sma100": (
                    statistics.fmean(closes[index - 99 : index + 1])
                    if index >= 99
                    else None
                ),
                "sma200": (
                    statistics.fmean(closes[index - 199 : index + 1])
                    if index >= 199
                    else None
                ),
            }
        cached[symbol] = symbol_cache
    return cached


def _z_score(current: float, history: Sequence[float]) -> float | None:
    if len(history) < STANDARDIZATION_LOOKBACK:
        return None
    sample = list(history[-STANDARDIZATION_LOOKBACK:])
    deviation = statistics.stdev(sample)
    if deviation == 0:
        return None
    return (current - statistics.fmean(sample)) / deviation


def _cost_floor(expected_gross_fraction: float) -> bool:
    return (
        expected_gross_fraction
        >= MINIMUM_GROSS_TO_COST_MULTIPLE * PRIMARY_ROUND_TRIP_COST_FRACTION
    )


def _daily_exit(
    bars: Sequence[Mapping[str, Any]],
    *,
    entry_index: int,
    hold_sessions: int,
    stop_price: float,
) -> tuple[int, float, bool]:
    final_index = entry_index + hold_sessions - 1
    for index in range(entry_index, final_index + 1):
        bar = bars[index]
        if float(bar["open"]) <= stop_price:
            return index, float(bar["open"]), True
        if float(bar["low"]) <= stop_price:
            return index, stop_price, True
    return final_index, float(bars[final_index]["close"]), False


def _daily_candidate(
    *,
    family_id: str,
    symbol: str,
    decision_date: str,
    entry_date: str,
    bars: Sequence[Mapping[str, Any]],
    entry_index: int,
    stop_atr: float,
    atr14: float,
    hold_sessions: int,
    rank: int,
    score: float,
) -> dict[str, Any]:
    entry_price = float(bars[entry_index]["open"])
    stop_price = entry_price - stop_atr * atr14
    signal_id = f"{entry_date}-{family_id}-{symbol}"
    if stop_price <= 0 or stop_price >= entry_price:
        return {
            "signal_id": signal_id,
            "signal_date": entry_date,
            "decision_date": decision_date,
            "symbol": symbol,
            "outcome": "rejected",
            "rank": rank,
            "rejection_reason": "invalid_structural_stop",
        }
    exit_index, exit_price, stop_executed = _daily_exit(
        bars,
        entry_index=entry_index,
        hold_sessions=hold_sessions,
        stop_price=stop_price,
    )
    marks = {
        str(bars[index]["date"]): (
            exit_price if index == exit_index else float(bars[index]["close"])
        )
        for index in range(entry_index, exit_index + 1)
    }
    return {
        "signal_id": signal_id,
        "signal_date": entry_date,
        "decision_date": decision_date,
        "symbol": symbol,
        "outcome": "eligible",
        "rank": rank,
        "score": score,
        "entry_price": entry_price,
        "stop_price": stop_price,
        "exit_date": str(bars[exit_index]["date"]),
        "exit_price": exit_price,
        "marks": marks,
        "stop_executed": stop_executed,
        "planned_stop_distance": entry_price - stop_price,
    }


def _equity_residual_candidates(
    dataset: Mapping[str, Any], parameters: Mapping[str, Any]
) -> list[dict[str, Any]]:
    family_id = str(dataset.get("family_id"))
    if family_id not in RESIDUAL_FAMILIES:
        raise DenseStrategyRuntimeError(
            "equity residual dataset family binding is invalid"
        )
    calendar = _calendar(dataset)
    calendar_positions = {day: index for index, day in enumerate(calendar)}
    daily = _daily_series(dataset)
    universe = dataset.get("universe_by_date")
    if not isinstance(universe, Mapping):
        raise DenseStrategyRuntimeError(
            "equity residual data needs point-in-time universe_by_date"
        )
    identities = dataset.get("universe_identity_by_date")
    if not isinstance(identities, Mapping):
        raise DenseStrategyRuntimeError(
            "equity residual data needs point-in-time identity by date"
        )
    raw_split_dates = dataset.get("split_execution_dates_by_symbol")
    if (
        raw_split_dates is None
        and family_id == EQUITY_RESIDUAL_FAMILY
    ):
        # Preserve the already-frozen V5 input contract.  The disjoint
        # replication below requires an explicit complete split denominator.
        raw_split_dates = {}
    if not isinstance(raw_split_dates, Mapping):
        raise DenseStrategyRuntimeError(
            "equity residual data needs point-in-time split actions"
        )
    split_dates: dict[str, set[str]] = {}
    for raw_symbol, raw_dates in raw_split_dates.items():
        if (
            not isinstance(raw_dates, list)
            or list(map(str, raw_dates)) != sorted(set(map(str, raw_dates)))
        ):
            raise DenseStrategyRuntimeError(
                "equity residual split dates are invalid"
            )
        split_dates[str(raw_symbol)] = set(map(str, raw_dates))
    if "SPY" not in daily:
        raise DenseStrategyRuntimeError("equity residual data needs SPY market bars")
    decision_dates = sorted(map(str, universe))
    if (
        not decision_dates
        or not set(decision_dates).issubset(calendar_positions)
        or any(calendar_positions[day] >= len(calendar) - 1 for day in decision_dates)
    ):
        raise DenseStrategyRuntimeError(
            "equity residual decision dates escaped the account calendar"
        )
    prepared = isinstance(dataset, dict) and "_prepared_daily_bars" in dataset
    universe_cache: tuple[
        dict[str, set[str]], tuple[str, ...]
    ] | None = None
    if prepared:
        raw_universe_cache = dataset.get("_equity_residual_universe_cache")
        if (
            isinstance(raw_universe_cache, tuple)
            and len(raw_universe_cache) == 2
            and isinstance(raw_universe_cache[0], dict)
            and isinstance(raw_universe_cache[1], tuple)
        ):
            universe_cache = raw_universe_cache
    if universe_cache is None:
        universe_sets: dict[str, set[str]] = {}
        for decision_date in decision_dates:
            day_universe = universe.get(decision_date)
            if not isinstance(day_universe, list):
                raise DenseStrategyRuntimeError(
                    f"universe_by_date is missing frozen date {decision_date}"
                )
            day_identities = identities.get(decision_date)
            if (
                not isinstance(day_identities, Mapping)
                or set(map(str, day_universe)) != set(map(str, day_identities))
                or len(set(map(str, day_identities.values())))
                != len(day_identities)
            ):
                raise DenseStrategyRuntimeError(
                    f"point-in-time identities are incomplete on {decision_date}"
                )
            universe_sets[decision_date] = set(map(str, day_universe))
        universe_symbols = tuple(
            sorted(set().union(*universe_sets.values()))
        )
        universe_cache = (universe_sets, universe_symbols)
        if prepared:
            dataset["_equity_residual_universe_cache"] = universe_cache
    universe_sets, universe_symbols = universe_cache
    window = int(parameters["prior_return_sessions"])
    threshold = float(parameters["residual_z_threshold"])
    trend_period = 100 if parameters["market_trend_gate"] == "SPY>SMA100" else 200
    stop_atr = float(parameters["stop_atr14"])
    hold = int(parameters["hold_sessions"])
    indices: dict[str, dict[str, int]] | None = None
    if prepared:
        raw_indices = dataset.get("_equity_residual_index_cache")
        if isinstance(raw_indices, dict):
            indices = raw_indices
    if indices is None:
        relevant_symbols = {"SPY", *universe_symbols}
        indices = {
            symbol: {
                str(bar["date"]): index for index, bar in enumerate(bars)
            }
            for symbol, bars in daily.items()
            if symbol in relevant_symbols
        }
        if prepared:
            dataset["_equity_residual_index_cache"] = indices
    cache: dict[int, dict[str, list[tuple[float, str, float, float]]]] | None = None
    if prepared:
        raw_cache = dataset.setdefault("_equity_residual_feature_cache", {})
        if isinstance(raw_cache, dict):
            cache = raw_cache
    features = cache.get(window) if cache is not None else None
    if features is None:
        evaluation_set = set(decision_dates)
        spy_bars = daily["SPY"]
        spy_returns: dict[str, float] = {}
        for index, bar in enumerate(spy_bars):
            if index >= window:
                spy_returns[str(bar["date"])] = (
                    float(bar["close"])
                    / float(spy_bars[index - window]["close"])
                    - 1
                )
        features = {day: [] for day in decision_dates}
        for symbol in universe_symbols:
            if symbol == "SPY":
                continue
            bars = daily.get(symbol)
            if bars is None:
                continue
            ranges = _true_ranges(bars)
            residual_history: list[float] = []
            for symbol_index, bar in enumerate(bars):
                day = str(bar["date"])
                spy_return = spy_returns.get(day)
                if symbol_index < window or spy_return is None:
                    continue
                symbol_return = (
                    float(bar["close"])
                    / float(bars[symbol_index - window]["close"])
                    - 1
                )
                residual = symbol_return - spy_return
                history_start = str(
                    bars[max(0, symbol_index - window - STANDARDIZATION_LOOKBACK)][
                        "date"
                    ]
                )
                recent_split = any(
                    history_start <= split_day <= day
                    for split_day in split_dates.get(symbol, set())
                )
                eligible_decision = (
                    day in evaluation_set
                    and symbol in universe_sets[day]
                    and symbol_index >= 14
                    and not recent_split
                )
                z_score = (
                    _z_score(residual, residual_history)
                    if eligible_decision
                    else None
                )
                residual_history.append(residual)
                if z_score is None:
                    continue
                atr14 = statistics.fmean(
                    ranges[symbol_index - 13 : symbol_index + 1]
                )
                features[day].append((z_score, symbol, atr14, residual))
        if cache is not None:
            cache[window] = features
    candidate_cache: dict[
        tuple[int, int, float, int], dict[str, Any]
    ] | None = None
    candidate_cache_key = (window, trend_period, stop_atr, hold)
    if prepared:
        raw_candidate_cache = dataset.setdefault(
            "_equity_residual_candidate_cache", {}
        )
        if isinstance(raw_candidate_cache, dict):
            candidate_cache = raw_candidate_cache
    cached_candidates = (
        candidate_cache.get(candidate_cache_key)
        if candidate_cache is not None
        else None
    )
    if (
        isinstance(cached_candidates, Mapping)
        and isinstance(cached_candidates.get("threshold"), (int, float))
        and threshold <= float(cached_candidates["threshold"])
        and isinstance(cached_candidates.get("tagged_candidates"), list)
    ):
        return [
            candidate
            for z_score, candidate in cached_candidates["tagged_candidates"]
            if z_score <= threshold
        ]
    candidates: list[dict[str, Any]] = []
    tagged_candidates: list[tuple[float, dict[str, Any]]] = []
    for decision_date in decision_dates:
        calendar_index = calendar_positions[decision_date]
        spy_index = indices["SPY"].get(decision_date)
        if spy_index is None or spy_index < max(window, trend_period - 1):
            continue
        spy_bars = daily["SPY"]
        spy_sma = _sma(spy_bars, spy_index, trend_period)
        trend_ready = spy_sma is not None and float(
            spy_bars[spy_index]["close"]
        ) > spy_sma
        scored: list[tuple[float, str, float]] = []
        for z_score, symbol, atr14, residual in features[decision_date]:
            if (
                not trend_ready
                or z_score > threshold
                or not _cost_floor(abs(residual))
            ):
                continue
            scored.append((z_score, symbol, atr14))
        entry_date = calendar[calendar_index + 1]
        if calendar_index + 1 + hold > len(calendar):
            continue
        for rank, (z_score, symbol, atr14) in enumerate(sorted(scored), 1):
            expected_holding_dates = set(
                calendar[calendar_index + 1 : calendar_index + 1 + hold]
            )
            if expected_holding_dates & split_dates.get(symbol, set()):
                candidate = {
                    "signal_id": f"{entry_date}-{family_id}-{symbol}",
                    "signal_date": entry_date,
                    "decision_date": decision_date,
                    "symbol": symbol,
                    "outcome": "rejected",
                    "rank": rank,
                    "rejection_reason": "split_affected_window",
                }
                candidates.append(candidate)
                tagged_candidates.append((z_score, candidate))
                continue
            entry_index = indices[symbol].get(entry_date)
            if entry_index is None:
                candidate = {
                    "signal_id": (
                        f"{entry_date}-{family_id}-{symbol}"
                    ),
                    "signal_date": entry_date,
                    "decision_date": decision_date,
                    "symbol": symbol,
                    "outcome": "missed_fill",
                    "rank": rank,
                    "rejection_reason": "missing_next_open",
                }
                candidates.append(candidate)
                tagged_candidates.append((z_score, candidate))
                continue
            if entry_index + hold > len(daily[symbol]):
                continue
            exit_dates = {
                str(item["date"])
                for item in daily[symbol][entry_index : entry_index + hold]
            }
            if exit_dates != expected_holding_dates:
                candidate = {
                    "signal_id": (
                        f"{entry_date}-{family_id}-{symbol}"
                    ),
                    "signal_date": entry_date,
                    "decision_date": decision_date,
                    "symbol": symbol,
                    "outcome": "missed_fill",
                    "rank": rank,
                    "rejection_reason": "incomplete_holding_bars",
                }
                candidates.append(candidate)
                tagged_candidates.append((z_score, candidate))
                continue
            candidate = _daily_candidate(
                family_id=family_id,
                symbol=symbol,
                decision_date=decision_date,
                entry_date=entry_date,
                bars=daily[symbol],
                entry_index=entry_index,
                stop_atr=stop_atr,
                atr14=atr14,
                hold_sessions=hold,
                rank=rank,
                score=z_score,
            )
            candidates.append(candidate)
            tagged_candidates.append((z_score, candidate))
    if candidate_cache is not None:
        candidate_cache[candidate_cache_key] = {
            "threshold": threshold,
            "tagged_candidates": tagged_candidates,
        }
    return candidates


def _fixed_etf_residual_candidates(
    dataset: Mapping[str, Any], parameters: Mapping[str, Any]
) -> list[dict[str, Any]]:
    """Apply the frozen residual-reversal rules to a fixed ETF denominator."""

    family_id = str(dataset.get("family_id"))
    if family_id not in {
        ETF_RESIDUAL_REPLICATION_FAMILY,
        ETF_RESIDUAL_REPLICATION_V2_FAMILY,
        ETF_RESIDUAL_REPLICATION_V3_FAMILY,
    }:
        raise DenseStrategyRuntimeError(
            "fixed ETF residual dataset family binding is invalid"
        )
    target_symbols = {
        ETF_RESIDUAL_REPLICATION_FAMILY: (
            ETF_RESIDUAL_REPLICATION_TARGET_SYMBOLS
        ),
        ETF_RESIDUAL_REPLICATION_V2_FAMILY: (
            ETF_RESIDUAL_REPLICATION_V2_TARGET_SYMBOLS
        ),
        ETF_RESIDUAL_REPLICATION_V3_FAMILY: (
            ETF_RESIDUAL_REPLICATION_V3_TARGET_SYMBOLS
        ),
    }[family_id]
    calendar = _calendar(dataset)
    daily = _daily_series(dataset)
    expected_symbols = {
        *target_symbols,
        ETF_RESIDUAL_REPLICATION_FEATURE_SYMBOL,
    }
    if set(daily) != expected_symbols:
        raise DenseStrategyRuntimeError(
            "fixed ETF residual universe drifted from the frozen symbols"
        )
    decision_dates = calendar[:-5]
    if isinstance(dataset, dict):
        augmented = dataset
    else:
        augmented = dict(dataset)
    augmented["universe_by_date"] = {
        day: list(target_symbols)
        for day in decision_dates
    }
    augmented["universe_identity_by_date"] = {
        day: {
            symbol: f"ETF:{symbol}" for symbol in target_symbols
        }
        for day in decision_dates
    }
    augmented["split_execution_dates_by_symbol"] = {
        symbol: [] for symbol in target_symbols
    }
    return _equity_residual_candidates(augmented, parameters)


def _etf_pullback_candidates(
    dataset: Mapping[str, Any],
    parameters: Mapping[str, Any],
    *,
    family_id: str = ETF_PULLBACK_FAMILY,
) -> list[dict[str, Any]]:
    calendar = _calendar(dataset)
    daily = _daily_series(dataset)
    if family_id == ETF_PULLBACK_REPLICATION_FAMILY and (
        dataset.get("symbols") != list(ETF_PULLBACK_REPLICATION_SYMBOLS)
        or set(daily) != set(ETF_PULLBACK_REPLICATION_SYMBOLS)
    ):
        raise DenseStrategyRuntimeError(
            "ETF pullback replication universe drifted from the frozen symbols"
        )
    trend_period = int(parameters["trend_sma"])
    rsi_max = float(parameters["rsi2_maximum"])
    decline_floor = float(parameters["three_session_decline_fraction"])
    stop_atr = float(parameters["stop_atr14"])
    hold = int(parameters["maximum_hold_sessions"])
    indices = {
        symbol: {str(bar["date"]): index for index, bar in enumerate(bars)}
        for symbol, bars in daily.items()
    }
    feature_cache = dataset.get("_etf_pullback_feature_cache")
    if not isinstance(feature_cache, Mapping):
        feature_cache = _etf_pullback_feature_cache(daily)
    candidates: list[dict[str, Any]] = []
    for calendar_index, decision_date in enumerate(calendar[:-1]):
        scored: list[tuple[float, float, str, float]] = []
        for symbol, bars in daily.items():
            symbol_index = indices[symbol].get(decision_date)
            if symbol_index is None or symbol_index < max(trend_period - 1, 3):
                continue
            features = feature_cache[symbol][decision_date]
            trend = features[f"sma{trend_period}"]
            rsi2 = features["rsi2"]
            atr14 = features["atr14"]
            decline = features["decline3"]
            if (
                trend is None
                or rsi2 is None
                or atr14 is None
                or decline is None
                or float(bars[symbol_index]["close"]) <= trend
                or rsi2 > rsi_max
                or decline > -decline_floor
                or not _cost_floor(abs(decline))
            ):
                continue
            scored.append((rsi2, decline, symbol, atr14))
        entry_date = calendar[calendar_index + 1]
        if calendar_index + 1 + hold > len(calendar):
            continue
        for rank, (rsi2, decline, symbol, atr14) in enumerate(sorted(scored), 1):
            bars = daily[symbol]
            entry_index = indices[symbol].get(entry_date)
            if entry_index is None:
                candidates.append(
                    {
                        "signal_id": f"{entry_date}-{family_id}-{symbol}",
                        "signal_date": entry_date,
                        "decision_date": decision_date,
                        "symbol": symbol,
                        "outcome": "missed_fill",
                        "rank": rank,
                        "rejection_reason": "missing_next_open",
                    }
                )
                continue
            if entry_index + hold > len(bars):
                continue
            exit_dates = {
                str(item["date"])
                for item in bars[entry_index : entry_index + hold]
            }
            expected_dates = set(
                calendar[calendar_index + 1 : calendar_index + 1 + hold]
            )
            if exit_dates != expected_dates:
                candidates.append(
                    {
                        "signal_id": f"{entry_date}-{family_id}-{symbol}",
                        "signal_date": entry_date,
                        "decision_date": decision_date,
                        "symbol": symbol,
                        "outcome": "missed_fill",
                        "rank": rank,
                        "rejection_reason": "incomplete_holding_bars",
                    }
                )
                continue
            candidates.append(
                _daily_candidate(
                    family_id=family_id,
                    symbol=symbol,
                    decision_date=decision_date,
                    entry_date=entry_date,
                    bars=bars,
                    entry_index=entry_index,
                    stop_atr=stop_atr,
                    atr14=atr14,
                    hold_sessions=hold,
                    rank=rank,
                    score=rsi2 + decline,
                )
            )
    return candidates


def _spy_rsi2_pullback_candidates(
    dataset: Mapping[str, Any],
    parameters: Mapping[str, Any],
) -> list[dict[str, Any]]:
    """Evaluate the one preregistered SPY RSI(2) mean-reversion rule."""

    calendar = _calendar(dataset)
    daily = _daily_series(dataset)
    if dataset.get("symbols") != ["SPY"] or set(daily) != {"SPY"}:
        raise DenseStrategyRuntimeError(
            "SPY RSI(2) pullback data must contain only frozen symbol SPY"
        )
    expected_parameters = {
        "trend_sma": 200,
        "rsi2_maximum": 10.0,
        "mean_reversion_sma": 5,
        "stop_atr14": 1.5,
        "maximum_hold_sessions": 5,
    }
    normalized_parameters = {
        "trend_sma": int(parameters["trend_sma"]),
        "rsi2_maximum": float(parameters["rsi2_maximum"]),
        "mean_reversion_sma": int(parameters["mean_reversion_sma"]),
        "stop_atr14": float(parameters["stop_atr14"]),
        "maximum_hold_sessions": int(parameters["maximum_hold_sessions"]),
    }
    if normalized_parameters != expected_parameters:
        raise DenseStrategyRuntimeError(
            "SPY RSI(2) pullback parameters drifted from the preregistered rule"
        )
    bars = daily["SPY"]
    indices = {str(bar["date"]): index for index, bar in enumerate(bars)}
    features = dataset.get("_etf_pullback_feature_cache")
    if not isinstance(features, Mapping):
        features = _etf_pullback_feature_cache(daily)
    symbol_features = features["SPY"]
    stop_atr = normalized_parameters["stop_atr14"]
    hold = normalized_parameters["maximum_hold_sessions"]
    candidates: list[dict[str, Any]] = []
    for calendar_index, decision_date in enumerate(calendar[:-1]):
        decision_index = indices.get(decision_date)
        if decision_index is None or decision_index < 199:
            continue
        decision_features = symbol_features[decision_date]
        trend = decision_features["sma200"]
        rsi2 = decision_features["rsi2"]
        atr14 = decision_features["atr14"]
        decision_close = float(bars[decision_index]["close"])
        if (
            trend is None
            or rsi2 is None
            or atr14 is None
            or decision_close <= trend
            or rsi2 > normalized_parameters["rsi2_maximum"]
        ):
            continue
        entry_date = calendar[calendar_index + 1]
        entry_index = indices.get(entry_date)
        signal_id = f"{entry_date}-{SPY_RSI2_PULLBACK_FAMILY}-SPY"
        if entry_index is None:
            candidates.append(
                {
                    "signal_id": signal_id,
                    "signal_date": entry_date,
                    "decision_date": decision_date,
                    "symbol": "SPY",
                    "outcome": "missed_fill",
                    "rank": 1,
                    "rejection_reason": "missing_next_open",
                }
            )
            continue
        expected_dates = calendar[
            calendar_index + 1 : calendar_index + 1 + hold
        ]
        if len(expected_dates) < hold:
            continue
        observed_dates = [
            str(item["date"])
            for item in bars[entry_index : entry_index + hold]
        ]
        if observed_dates != expected_dates:
            candidates.append(
                {
                    "signal_id": signal_id,
                    "signal_date": entry_date,
                    "decision_date": decision_date,
                    "symbol": "SPY",
                    "outcome": "missed_fill",
                    "rank": 1,
                    "rejection_reason": "incomplete_holding_bars",
                }
            )
            continue
        entry_price = float(bars[entry_index]["open"])
        mean_reversion_reference = _sma(
            bars,
            decision_index,
            normalized_parameters["mean_reversion_sma"],
        )
        if mean_reversion_reference is None:
            continue
        expected_gross = mean_reversion_reference / entry_price - 1
        if not _cost_floor(expected_gross):
            candidates.append(
                {
                    "signal_id": signal_id,
                    "signal_date": entry_date,
                    "decision_date": decision_date,
                    "symbol": "SPY",
                    "outcome": "rejected",
                    "rank": 1,
                    "rejection_reason": "expected_move_below_cost_floor",
                    "expected_gross_move_fraction": expected_gross,
                }
            )
            continue
        stop_price = entry_price - stop_atr * atr14
        if stop_price <= 0 or stop_price >= entry_price:
            candidates.append(
                {
                    "signal_id": signal_id,
                    "signal_date": entry_date,
                    "decision_date": decision_date,
                    "symbol": "SPY",
                    "outcome": "rejected",
                    "rank": 1,
                    "rejection_reason": "invalid_structural_stop",
                }
            )
            continue
        exit_index = entry_index + hold - 1
        exit_price = float(bars[exit_index]["close"])
        stop_executed = False
        for index in range(entry_index, entry_index + hold):
            bar = bars[index]
            opening = float(bar["open"])
            if opening <= stop_price:
                exit_index = index
                exit_price = opening
                stop_executed = True
                break
            if float(bar["low"]) <= stop_price:
                exit_index = index
                exit_price = stop_price
                stop_executed = True
                break
            exit_sma = _sma(
                bars,
                index,
                normalized_parameters["mean_reversion_sma"],
            )
            if exit_sma is None:
                raise DenseStrategyRuntimeError(
                    "SPY RSI(2) pullback exit SMA is unavailable"
                )
            if float(bar["close"]) >= exit_sma:
                exit_index = index
                exit_price = float(bar["close"])
                break
        marks = {
            str(bars[index]["date"]): (
                exit_price if index == exit_index else float(bars[index]["close"])
            )
            for index in range(entry_index, exit_index + 1)
        }
        candidates.append(
            {
                "signal_id": signal_id,
                "signal_date": entry_date,
                "decision_date": decision_date,
                "symbol": "SPY",
                "outcome": "eligible",
                "rank": 1,
                "score": -float(rsi2),
                "entry_price": entry_price,
                "stop_price": stop_price,
                "exit_date": str(bars[exit_index]["date"]),
                "exit_price": exit_price,
                "marks": marks,
                "stop_executed": stop_executed,
                "planned_stop_distance": entry_price - stop_price,
                "expected_gross_move_fraction": expected_gross,
                "mean_reversion_reference_price": mean_reversion_reference,
            }
        )
    return candidates


def _sector_etf_gap_drift_candidates(
    dataset: Mapping[str, Any], parameters: Mapping[str, Any]
) -> list[dict[str, Any]]:
    calendar = _calendar(dataset)
    daily = _daily_series(dataset)
    minimum_gap = float(parameters["minimum_gap_fraction"])
    maximum_gap = float(parameters["maximum_gap_fraction"])
    trend_period = int(parameters["trend_sma"])
    stop_atr = float(parameters["stop_atr14"])
    hold = int(parameters["maximum_hold_sessions"])
    indices = {
        symbol: {str(bar["date"]): index for index, bar in enumerate(bars)}
        for symbol, bars in daily.items()
    }
    feature_cache = dataset.get("_etf_pullback_feature_cache")
    if not isinstance(feature_cache, Mapping):
        feature_cache = _etf_pullback_feature_cache(daily)
    candidates: list[dict[str, Any]] = []
    for calendar_index, decision_date in enumerate(calendar[:-1]):
        scored: list[tuple[float, float, str, float]] = []
        for symbol, bars in daily.items():
            symbol_index = indices[symbol].get(decision_date)
            if symbol_index is None or symbol_index < max(trend_period - 1, 14, 1):
                continue
            decision_bar = bars[symbol_index]
            prior_close = float(bars[symbol_index - 1]["close"])
            gap_fraction = float(decision_bar["open"]) / prior_close - 1
            session_return = (
                float(decision_bar["close"]) / float(decision_bar["open"]) - 1
            )
            features = feature_cache[symbol][decision_date]
            trend = features[f"sma{trend_period}"]
            atr14 = features["atr14"]
            if (
                trend is None
                or atr14 is None
                or gap_fraction < minimum_gap
                or gap_fraction > maximum_gap
                or session_return < 0
                or float(decision_bar["close"]) <= trend
                or not _cost_floor(gap_fraction)
            ):
                continue
            scored.append((-gap_fraction, -session_return, symbol, atr14))
        entry_date = calendar[calendar_index + 1]
        if calendar_index + 1 + hold > len(calendar):
            continue
        for rank, (
            negative_gap,
            negative_session_return,
            symbol,
            atr14,
        ) in enumerate(sorted(scored), 1):
            bars = daily[symbol]
            entry_index = indices[symbol].get(entry_date)
            if entry_index is None:
                candidates.append(
                    {
                        "signal_id": (
                            f"{entry_date}-{SECTOR_ETF_GAP_DRIFT_FAMILY}-{symbol}"
                        ),
                        "signal_date": entry_date,
                        "decision_date": decision_date,
                        "symbol": symbol,
                        "outcome": "missed_fill",
                        "rank": rank,
                        "rejection_reason": "missing_next_open",
                    }
                )
                continue
            if entry_index + hold > len(bars):
                continue
            exit_dates = {
                str(item["date"])
                for item in bars[entry_index : entry_index + hold]
            }
            expected_dates = set(
                calendar[calendar_index + 1 : calendar_index + 1 + hold]
            )
            if exit_dates != expected_dates:
                candidates.append(
                    {
                        "signal_id": (
                            f"{entry_date}-{SECTOR_ETF_GAP_DRIFT_FAMILY}-{symbol}"
                        ),
                        "signal_date": entry_date,
                        "decision_date": decision_date,
                        "symbol": symbol,
                        "outcome": "missed_fill",
                        "rank": rank,
                        "rejection_reason": "incomplete_holding_bars",
                    }
                )
                continue
            gap_fraction = -negative_gap
            session_return = -negative_session_return
            candidates.append(
                _daily_candidate(
                    family_id=SECTOR_ETF_GAP_DRIFT_FAMILY,
                    symbol=symbol,
                    decision_date=decision_date,
                    entry_date=entry_date,
                    bars=bars,
                    entry_index=entry_index,
                    stop_atr=stop_atr,
                    atr14=atr14,
                    hold_sessions=hold,
                    rank=rank,
                    score=gap_fraction + session_return,
                )
            )
    return candidates


def _flight_to_safety_rebound_candidates(
    dataset: Mapping[str, Any],
    parameters: Mapping[str, Any],
    *,
    family_id: str = FLIGHT_TO_SAFETY_REBOUND_FAMILY,
    target_symbols: Sequence[str] = FLIGHT_TO_SAFETY_TARGET_SYMBOLS,
) -> list[dict[str, Any]]:
    calendar = _calendar(dataset)
    daily = _daily_series(dataset)
    decline_floor = float(parameters["minimum_equity_decline_fraction"])
    treasury_return_floor = float(
        parameters["minimum_tlt_return_fraction"]
    )
    trend_period = int(parameters["trend_sma"])
    stop_atr = float(parameters["stop_atr14"])
    hold = int(parameters["maximum_hold_sessions"])
    required_symbols = {
        *target_symbols,
        FLIGHT_TO_SAFETY_FEATURE_SYMBOL,
    }
    if set(daily) != required_symbols:
        raise DenseStrategyRuntimeError(
            "flight-to-safety data does not match its frozen cross-asset universe"
        )
    indices = {
        symbol: {str(bar["date"]): index for index, bar in enumerate(bars)}
        for symbol, bars in daily.items()
    }
    feature_cache = dataset.get("_etf_pullback_feature_cache")
    if not isinstance(feature_cache, Mapping):
        feature_cache = _etf_pullback_feature_cache(daily)
    candidates: list[dict[str, Any]] = []
    for calendar_index, decision_date in enumerate(calendar[:-1]):
        treasury_bars = daily[FLIGHT_TO_SAFETY_FEATURE_SYMBOL]
        treasury_index = indices[FLIGHT_TO_SAFETY_FEATURE_SYMBOL].get(
            decision_date
        )
        if treasury_index is None or treasury_index < 1:
            continue
        treasury_return = (
            float(treasury_bars[treasury_index]["close"])
            / float(treasury_bars[treasury_index - 1]["close"])
            - 1
        )
        if treasury_return < treasury_return_floor:
            continue
        scored: list[tuple[float, str, float]] = []
        for symbol in target_symbols:
            bars = daily[symbol]
            symbol_index = indices[symbol].get(decision_date)
            if symbol_index is None or symbol_index < max(
                trend_period - 1, 14, 1
            ):
                continue
            features = feature_cache[symbol][decision_date]
            trend = features[f"sma{trend_period}"]
            atr14 = features["atr14"]
            decline = (
                float(bars[symbol_index]["close"])
                / float(bars[symbol_index - 1]["close"])
                - 1
            )
            if (
                trend is None
                or atr14 is None
                or float(bars[symbol_index]["close"]) <= trend
                or decline > -decline_floor
                or not _cost_floor(abs(decline))
            ):
                continue
            scored.append((decline, symbol, atr14))
        entry_date = calendar[calendar_index + 1]
        if calendar_index + 1 + hold > len(calendar):
            continue
        for rank, (decline, symbol, atr14) in enumerate(sorted(scored), 1):
            bars = daily[symbol]
            entry_index = indices[symbol].get(entry_date)
            signal_id = (
                f"{entry_date}-{family_id}-{symbol}"
            )
            if entry_index is None:
                candidates.append(
                    {
                        "signal_id": signal_id,
                        "signal_date": entry_date,
                        "decision_date": decision_date,
                        "symbol": symbol,
                        "outcome": "missed_fill",
                        "rank": rank,
                        "rejection_reason": "missing_next_open",
                    }
                )
                continue
            if entry_index + hold > len(bars):
                continue
            observed_dates = {
                str(item["date"])
                for item in bars[entry_index : entry_index + hold]
            }
            expected_dates = set(
                calendar[calendar_index + 1 : calendar_index + 1 + hold]
            )
            if observed_dates != expected_dates:
                candidates.append(
                    {
                        "signal_id": signal_id,
                        "signal_date": entry_date,
                        "decision_date": decision_date,
                        "symbol": symbol,
                        "outcome": "missed_fill",
                        "rank": rank,
                        "rejection_reason": "incomplete_holding_bars",
                    }
                )
                continue
            candidates.append(
                _daily_candidate(
                    family_id=family_id,
                    symbol=symbol,
                    decision_date=decision_date,
                    entry_date=entry_date,
                    bars=bars,
                    entry_index=entry_index,
                    stop_atr=stop_atr,
                    atr14=atr14,
                    hold_sessions=hold,
                    rank=rank,
                    score=-decline + treasury_return,
                )
            )
    return candidates


def _breadth_capitulation_rebound_candidates(
    dataset: Mapping[str, Any], parameters: Mapping[str, Any]
) -> list[dict[str, Any]]:
    calendar = _calendar(dataset)
    daily = _daily_series(dataset)
    required_decliners = int(parameters["minimum_declining_symbols"])
    median_decline_floor = float(
        parameters["minimum_median_decline_fraction"]
    )
    target_decline_floor = float(
        parameters["minimum_target_decline_fraction"]
    )
    stop_atr = float(parameters["stop_atr14"])
    hold = int(parameters["maximum_hold_sessions"])
    if set(daily) != set(BREADTH_CAPITULATION_SYMBOLS):
        raise DenseStrategyRuntimeError(
            "breadth-capitulation data does not match its frozen ETF basket"
        )
    indices = {
        symbol: {str(bar["date"]): index for index, bar in enumerate(bars)}
        for symbol, bars in daily.items()
    }
    candidates: list[dict[str, Any]] = []
    for calendar_index, decision_date in enumerate(calendar[:-1]):
        observed: list[tuple[float, str, float]] = []
        for symbol in BREADTH_CAPITULATION_SYMBOLS:
            bars = daily[symbol]
            symbol_index = indices[symbol].get(decision_date)
            if symbol_index is None or symbol_index < 14:
                observed = []
                break
            atr14 = _atr(bars, symbol_index)
            if atr14 is None:
                observed = []
                break
            session_return = (
                float(bars[symbol_index]["close"])
                / float(bars[symbol_index - 1]["close"])
                - 1
            )
            observed.append((session_return, symbol, atr14))
        if len(observed) != len(BREADTH_CAPITULATION_SYMBOLS):
            continue
        returns = [item[0] for item in observed]
        if (
            sum(item < 0 for item in returns) < required_decliners
            or statistics.median(returns) > -median_decline_floor
        ):
            continue
        scored = [
            item
            for item in observed
            if item[0] <= -target_decline_floor
            and _cost_floor(abs(item[0]))
        ]
        entry_date = calendar[calendar_index + 1]
        if calendar_index + 1 + hold > len(calendar):
            continue
        for rank, (session_return, symbol, atr14) in enumerate(
            sorted(scored), 1
        ):
            bars = daily[symbol]
            entry_index = indices[symbol].get(entry_date)
            signal_id = (
                f"{entry_date}-{BREADTH_CAPITULATION_REBOUND_FAMILY}-{symbol}"
            )
            if entry_index is None:
                candidates.append(
                    {
                        "signal_id": signal_id,
                        "signal_date": entry_date,
                        "decision_date": decision_date,
                        "symbol": symbol,
                        "outcome": "missed_fill",
                        "rank": rank,
                        "rejection_reason": "missing_next_open",
                    }
                )
                continue
            if entry_index + hold > len(bars):
                continue
            observed_dates = {
                str(item["date"])
                for item in bars[entry_index : entry_index + hold]
            }
            expected_dates = set(
                calendar[calendar_index + 1 : calendar_index + 1 + hold]
            )
            if observed_dates != expected_dates:
                candidates.append(
                    {
                        "signal_id": signal_id,
                        "signal_date": entry_date,
                        "decision_date": decision_date,
                        "symbol": symbol,
                        "outcome": "missed_fill",
                        "rank": rank,
                        "rejection_reason": "incomplete_holding_bars",
                    }
                )
                continue
            candidates.append(
                _daily_candidate(
                    family_id=BREADTH_CAPITULATION_REBOUND_FAMILY,
                    symbol=symbol,
                    decision_date=decision_date,
                    entry_date=entry_date,
                    bars=bars,
                    entry_index=entry_index,
                    stop_atr=stop_atr,
                    atr14=atr14,
                    hold_sessions=hold,
                    rank=rank,
                    score=-session_return,
                )
            )
    return candidates


def _cross_style_breadth_continuation_candidates(
    dataset: Mapping[str, Any], parameters: Mapping[str, Any]
) -> list[dict[str, Any]]:
    calendar = _calendar(dataset)
    daily = _daily_series(dataset)
    normalized_parameters = {
        "breadth_sma": int(parameters["breadth_sma"]),
        "minimum_breadth_count": int(parameters["minimum_breadth_count"]),
        "target_trend_sma": int(parameters["target_trend_sma"]),
        "stop_atr14": float(parameters["stop_atr14"]),
        "maximum_hold_sessions": int(parameters["maximum_hold_sessions"]),
    }
    expected_parameters = {
        "breadth_sma": 100,
        "minimum_breadth_count": 7,
        "target_trend_sma": 200,
        "stop_atr14": 1.5,
        "maximum_hold_sessions": 5,
    }
    if normalized_parameters != expected_parameters:
        raise DenseStrategyRuntimeError(
            "cross-style breadth rules drifted from the frozen exact rule"
        )
    if set(daily) != set(CROSS_STYLE_BREADTH_SYMBOLS):
        raise DenseStrategyRuntimeError(
            "cross-style breadth data does not match its frozen ETF basket"
        )
    indices = {
        symbol: {str(bar["date"]): index for index, bar in enumerate(bars)}
        for symbol, bars in daily.items()
    }
    candidates: list[dict[str, Any]] = []
    hold = expected_parameters["maximum_hold_sessions"]
    for calendar_index, decision_date in enumerate(calendar[:-1]):
        next_session = calendar[calendar_index + 1]
        if (
            date.fromisoformat(decision_date).isocalendar()[:2]
            == date.fromisoformat(next_session).isocalendar()[:2]
        ):
            continue
        breadth_count = 0
        complete = True
        for symbol in CROSS_STYLE_BREADTH_SYMBOLS:
            bars = daily[symbol]
            symbol_index = indices[symbol].get(decision_date)
            breadth = (
                _sma(bars, symbol_index, 100)
                if symbol_index is not None
                else None
            )
            if breadth is None:
                complete = False
                break
            if float(bars[symbol_index]["close"]) > breadth:
                breadth_count += 1
        if not complete or breadth_count < 7:
            continue
        target_bars = daily[CROSS_STYLE_BREADTH_TARGET_SYMBOL]
        target_index = indices[CROSS_STYLE_BREADTH_TARGET_SYMBOL].get(
            decision_date
        )
        if target_index is None:
            continue
        trend = _sma(target_bars, target_index, 200)
        atr14 = _atr(target_bars, target_index)
        decision_close = float(target_bars[target_index]["close"])
        if (
            trend is None
            or atr14 is None
            or decision_close <= trend
            or not _cost_floor(atr14 / decision_close)
        ):
            continue
        entry_date = next_session
        if calendar_index + 1 + hold > len(calendar):
            continue
        entry_index = indices[CROSS_STYLE_BREADTH_TARGET_SYMBOL].get(
            entry_date
        )
        signal_id = (
            f"{entry_date}-{CROSS_STYLE_BREADTH_CONTINUATION_FAMILY}-"
            f"{CROSS_STYLE_BREADTH_TARGET_SYMBOL}"
        )
        if entry_index is None:
            candidates.append(
                {
                    "signal_id": signal_id,
                    "signal_date": entry_date,
                    "decision_date": decision_date,
                    "symbol": CROSS_STYLE_BREADTH_TARGET_SYMBOL,
                    "outcome": "missed_fill",
                    "rank": 1,
                    "rejection_reason": "missing_next_open",
                }
            )
            continue
        observed_dates = {
            str(item["date"])
            for item in target_bars[entry_index : entry_index + hold]
        }
        expected_dates = set(
            calendar[calendar_index + 1 : calendar_index + 1 + hold]
        )
        if observed_dates != expected_dates:
            candidates.append(
                {
                    "signal_id": signal_id,
                    "signal_date": entry_date,
                    "decision_date": decision_date,
                    "symbol": CROSS_STYLE_BREADTH_TARGET_SYMBOL,
                    "outcome": "missed_fill",
                    "rank": 1,
                    "rejection_reason": "incomplete_holding_bars",
                }
            )
            continue
        candidate = _daily_candidate(
            family_id=CROSS_STYLE_BREADTH_CONTINUATION_FAMILY,
            symbol=CROSS_STYLE_BREADTH_TARGET_SYMBOL,
            decision_date=decision_date,
            entry_date=entry_date,
            bars=target_bars,
            entry_index=entry_index,
            stop_atr=1.5,
            atr14=atr14,
            hold_sessions=hold,
            rank=1,
            score=breadth_count + (decision_close / trend - 1),
        )
        candidate["expected_gross_move_fraction"] = atr14 / decision_close
        candidate["breadth_count"] = breadth_count
        candidates.append(candidate)
    return candidates


def _style_etf_breakout_continuation_candidates(
    dataset: Mapping[str, Any], parameters: Mapping[str, Any]
) -> list[dict[str, Any]]:
    """Evaluate one fixed daily cross-style closing-breakout rule."""

    calendar = _calendar(dataset)
    daily = _daily_series(dataset)
    normalized_parameters = {
        "breadth_sma": int(parameters["breadth_sma"]),
        "minimum_breadth_count": int(parameters["minimum_breadth_count"]),
        "breakout_lookback_sessions": int(
            parameters["breakout_lookback_sessions"]
        ),
        "trend_sma": int(parameters["trend_sma"]),
        "stop_atr14": float(parameters["stop_atr14"]),
        "maximum_hold_sessions": int(parameters["maximum_hold_sessions"]),
    }
    expected_parameters = {
        "breadth_sma": 100,
        "minimum_breadth_count": 6,
        "breakout_lookback_sessions": 20,
        "trend_sma": 200,
        "stop_atr14": 1.5,
        "maximum_hold_sessions": 5,
    }
    if normalized_parameters != expected_parameters:
        raise DenseStrategyRuntimeError(
            "style ETF breakout rules drifted from the frozen exact rule"
        )
    if set(daily) != set(CROSS_STYLE_BREADTH_SYMBOLS):
        raise DenseStrategyRuntimeError(
            "style ETF breakout data does not match its frozen basket"
        )
    indices = {
        symbol: {str(bar["date"]): index for index, bar in enumerate(bars)}
        for symbol, bars in daily.items()
    }
    candidates: list[dict[str, Any]] = []
    hold = expected_parameters["maximum_hold_sessions"]
    for calendar_index, decision_date in enumerate(calendar[:-1]):
        breadth_count = 0
        complete = True
        for symbol in CROSS_STYLE_BREADTH_SYMBOLS:
            bars = daily[symbol]
            symbol_index = indices[symbol].get(decision_date)
            breadth = (
                _sma(bars, symbol_index, 100)
                if symbol_index is not None
                else None
            )
            if breadth is None:
                complete = False
                break
            if float(bars[symbol_index]["close"]) > breadth:
                breadth_count += 1
        if not complete or breadth_count < 6:
            continue
        ranked: list[tuple[float, str, float, float]] = []
        for symbol in CROSS_STYLE_BREADTH_SYMBOLS:
            bars = daily[symbol]
            symbol_index = indices[symbol].get(decision_date)
            if symbol_index is None or symbol_index < 200:
                continue
            trend = _sma(bars, symbol_index, 200)
            atr14 = _atr(bars, symbol_index)
            decision_close = float(bars[symbol_index]["close"])
            prior_high = max(
                float(item["close"])
                for item in bars[symbol_index - 20 : symbol_index]
            )
            trailing_return = (
                decision_close
                / float(bars[symbol_index - 20]["close"])
                - 1
            )
            if (
                trend is None
                or atr14 is None
                or decision_close <= trend
                or decision_close <= prior_high
                or not _cost_floor(atr14 / decision_close)
            ):
                continue
            ranked.append(
                (trailing_return, symbol, atr14, decision_close)
            )
        if not ranked:
            continue
        trailing_return, symbol, atr14, decision_close = sorted(
            ranked,
            key=lambda item: (-item[0], item[1]),
        )[0]
        if calendar_index + 1 + hold > len(calendar):
            continue
        entry_date = calendar[calendar_index + 1]
        bars = daily[symbol]
        entry_index = indices[symbol].get(entry_date)
        signal_id = (
            f"{entry_date}-{STYLE_ETF_BREAKOUT_CONTINUATION_FAMILY}-"
            f"{symbol}"
        )
        if entry_index is None:
            candidates.append(
                {
                    "signal_id": signal_id,
                    "signal_date": entry_date,
                    "decision_date": decision_date,
                    "symbol": symbol,
                    "outcome": "missed_fill",
                    "rank": 1,
                    "rejection_reason": "missing_next_open",
                }
            )
            continue
        observed_dates = {
            str(item["date"])
            for item in bars[entry_index : entry_index + hold]
        }
        expected_dates = set(
            calendar[calendar_index + 1 : calendar_index + 1 + hold]
        )
        if observed_dates != expected_dates:
            candidates.append(
                {
                    "signal_id": signal_id,
                    "signal_date": entry_date,
                    "decision_date": decision_date,
                    "symbol": symbol,
                    "outcome": "missed_fill",
                    "rank": 1,
                    "rejection_reason": "incomplete_holding_bars",
                }
            )
            continue
        candidate = _daily_candidate(
            family_id=STYLE_ETF_BREAKOUT_CONTINUATION_FAMILY,
            symbol=symbol,
            decision_date=decision_date,
            entry_date=entry_date,
            bars=bars,
            entry_index=entry_index,
            stop_atr=1.5,
            atr14=atr14,
            hold_sessions=hold,
            rank=1,
            score=trailing_return,
        )
        candidate["breadth_count"] = breadth_count
        candidate["trailing_return_fraction"] = trailing_return
        candidate["breakout_reference_price"] = max(
            float(item["close"])
            for item in bars[indices[symbol][decision_date] - 20 : indices[symbol][decision_date]]
        )
        candidate["expected_gross_move_fraction"] = atr14 / decision_close
        candidates.append(candidate)
    return candidates


def _high_beta_etf_oversold_candidates(
    dataset: Mapping[str, Any],
    parameters: Mapping[str, Any],
    *,
    family_id: str = HIGH_BETA_ETF_OVERSOLD_FAMILY,
) -> list[dict[str, Any]]:
    calendar = _calendar(dataset)
    daily = _daily_series(dataset)
    trend_period = int(parameters["trend_sma"])
    rsi_max = float(parameters["rsi2_maximum"])
    decline_floor = float(parameters["one_session_decline_fraction"])
    stop_atr = float(parameters["stop_atr14"])
    hold = int(parameters["maximum_hold_sessions"])
    indices = {
        symbol: {str(bar["date"]): index for index, bar in enumerate(bars)}
        for symbol, bars in daily.items()
    }
    feature_cache = dataset.get("_etf_pullback_feature_cache")
    if not isinstance(feature_cache, Mapping):
        feature_cache = _etf_pullback_feature_cache(daily)
    candidates: list[dict[str, Any]] = []
    for calendar_index, decision_date in enumerate(calendar[:-1]):
        scored: list[tuple[float, float, str, float]] = []
        for symbol, bars in daily.items():
            symbol_index = indices[symbol].get(decision_date)
            if symbol_index is None or symbol_index < max(
                trend_period - 1, 14, 2
            ):
                continue
            features = feature_cache[symbol][decision_date]
            trend = features[f"sma{trend_period}"]
            rsi2 = features["rsi2"]
            atr14 = features["atr14"]
            decline = features["decline1"]
            if (
                trend is None
                or rsi2 is None
                or atr14 is None
                or decline is None
                or float(bars[symbol_index]["close"]) <= trend
                or rsi2 > rsi_max
                or decline > -decline_floor
                or not _cost_floor(abs(decline))
            ):
                continue
            scored.append((rsi2, decline, symbol, atr14))
        entry_date = calendar[calendar_index + 1]
        if calendar_index + 1 + hold > len(calendar):
            continue
        for rank, (rsi2, decline, symbol, atr14) in enumerate(
            sorted(scored), 1
        ):
            bars = daily[symbol]
            entry_index = indices[symbol].get(entry_date)
            if entry_index is None:
                candidates.append(
                    {
                        "signal_id": (
                            f"{entry_date}-{family_id}-"
                            f"{symbol}"
                        ),
                        "signal_date": entry_date,
                        "decision_date": decision_date,
                        "symbol": symbol,
                        "outcome": "missed_fill",
                        "rank": rank,
                        "rejection_reason": "missing_next_open",
                    }
                )
                continue
            if entry_index + hold > len(bars):
                continue
            exit_dates = {
                str(item["date"])
                for item in bars[entry_index : entry_index + hold]
            }
            expected_dates = set(
                calendar[calendar_index + 1 : calendar_index + 1 + hold]
            )
            if exit_dates != expected_dates:
                candidates.append(
                    {
                        "signal_id": (
                            f"{entry_date}-{family_id}-"
                            f"{symbol}"
                        ),
                        "signal_date": entry_date,
                        "decision_date": decision_date,
                        "symbol": symbol,
                        "outcome": "missed_fill",
                        "rank": rank,
                        "rejection_reason": "incomplete_holding_bars",
                    }
                )
                continue
            candidates.append(
                _daily_candidate(
                    family_id=family_id,
                    symbol=symbol,
                    decision_date=decision_date,
                    entry_date=entry_date,
                    bars=bars,
                    entry_index=entry_index,
                    stop_atr=stop_atr,
                    atr14=atr14,
                    hold_sessions=hold,
                    rank=rank,
                    score=rsi2 + decline,
                )
            )
    return candidates


def _etf_cross_sectional_momentum_candidates(
    dataset: Mapping[str, Any], parameters: Mapping[str, Any]
) -> list[dict[str, Any]]:
    calendar = _calendar(dataset)
    daily = _daily_series(dataset)
    lookback = int(parameters["return_lookback_sessions"])
    trend_period = int(parameters["market_trend_sma"])
    excess_floor = float(parameters["minimum_excess_return_fraction"])
    stop_atr = float(parameters["stop_atr14"])
    hold = int(parameters["maximum_hold_sessions"])
    if "SPY" not in daily:
        raise DenseStrategyRuntimeError("ETF momentum data requires SPY")
    indices = {
        symbol: {str(bar["date"]): index for index, bar in enumerate(bars)}
        for symbol, bars in daily.items()
    }
    candidates: list[dict[str, Any]] = []
    for calendar_index, decision_date in enumerate(calendar[:-1]):
        spy_index = indices["SPY"].get(decision_date)
        if spy_index is None or spy_index < max(lookback, trend_period - 1):
            continue
        spy_bars = daily["SPY"]
        market_trend = _sma(spy_bars, spy_index, trend_period)
        if (
            market_trend is None
            or float(spy_bars[spy_index]["close"]) <= market_trend
        ):
            continue
        ranked: list[tuple[float, str, float]] = []
        observed_returns: list[float] = []
        features: list[tuple[float, str, float]] = []
        for symbol, bars in daily.items():
            symbol_index = indices[symbol].get(decision_date)
            if symbol_index is None or symbol_index < lookback:
                continue
            atr14 = _atr(bars, symbol_index)
            if atr14 is None:
                continue
            trailing_return = (
                float(bars[symbol_index]["close"])
                / float(bars[symbol_index - lookback]["close"])
                - 1
            )
            observed_returns.append(trailing_return)
            features.append((trailing_return, symbol, atr14))
        if len(observed_returns) != len(daily):
            continue
        benchmark = statistics.median(observed_returns)
        for trailing_return, symbol, atr14 in features:
            excess_return = trailing_return - benchmark
            if (
                excess_return < excess_floor
                or not _cost_floor(abs(excess_return))
            ):
                continue
            ranked.append((-trailing_return, symbol, atr14))
        if not ranked:
            continue
        entry_date = calendar[calendar_index + 1]
        if calendar_index + 1 + hold > len(calendar):
            continue
        for rank, (negative_return, symbol, atr14) in enumerate(sorted(ranked), 1):
            bars = daily[symbol]
            entry_index = indices[symbol].get(entry_date)
            if entry_index is None:
                candidates.append(
                    {
                        "signal_id": (
                            f"{entry_date}-{ETF_CROSS_SECTIONAL_MOMENTUM_FAMILY}-"
                            f"{symbol}"
                        ),
                        "signal_date": entry_date,
                        "decision_date": decision_date,
                        "symbol": symbol,
                        "outcome": "missed_fill",
                        "rank": rank,
                        "rejection_reason": "missing_next_open",
                    }
                )
                continue
            if entry_index + hold > len(bars):
                continue
            exit_dates = {
                str(item["date"])
                for item in bars[entry_index : entry_index + hold]
            }
            expected_dates = set(
                calendar[calendar_index + 1 : calendar_index + 1 + hold]
            )
            if exit_dates != expected_dates:
                candidates.append(
                    {
                        "signal_id": (
                            f"{entry_date}-{ETF_CROSS_SECTIONAL_MOMENTUM_FAMILY}-"
                            f"{symbol}"
                        ),
                        "signal_date": entry_date,
                        "decision_date": decision_date,
                        "symbol": symbol,
                        "outcome": "missed_fill",
                        "rank": rank,
                        "rejection_reason": "incomplete_holding_bars",
                    }
                )
                continue
            candidates.append(
                _daily_candidate(
                    family_id=ETF_CROSS_SECTIONAL_MOMENTUM_FAMILY,
                    symbol=symbol,
                    decision_date=decision_date,
                    entry_date=entry_date,
                    bars=bars,
                    entry_index=entry_index,
                    stop_atr=stop_atr,
                    atr14=atr14,
                    hold_sessions=hold,
                    rank=rank,
                    score=-negative_return,
                )
            )
    return candidates


def _liquid_equity_momentum_candidates(
    dataset: Mapping[str, Any], parameters: Mapping[str, Any]
) -> list[dict[str, Any]]:
    """Rank a point-in-time liquid common-stock universe without lookahead."""

    calendar = _calendar(dataset)
    session_dates = dataset.get("session_dates")
    if (
        not isinstance(session_dates, list)
        or session_dates != sorted(set(map(str, session_dates)))
        or not set(calendar).issubset(session_dates)
    ):
        raise DenseStrategyRuntimeError(
            "liquid-equity momentum needs a complete frozen session calendar"
        )
    daily = _daily_series(dataset)
    universe = dataset.get("universe_by_date")
    identities = dataset.get("universe_identity_by_date")
    split_dates = dataset.get("split_execution_dates_by_symbol")
    if not (
        isinstance(universe, Mapping)
        and isinstance(identities, Mapping)
        and set(universe) == set(identities)
        and isinstance(split_dates, Mapping)
    ):
        raise DenseStrategyRuntimeError(
            "liquid-equity momentum needs point-in-time universe identities "
            "and split actions"
        )
    if not set(universe).issubset(calendar):
        raise DenseStrategyRuntimeError(
            "liquid-equity momentum decision dates escaped the account calendar"
        )
    lookback = int(parameters["return_lookback_sessions"])
    trend_period = int(parameters["trend_sma_sessions"])
    excess_floor = float(parameters["minimum_excess_return_fraction"])
    stop_atr = float(parameters["stop_atr14"])
    hold = int(parameters["maximum_hold_sessions"])
    if (
        lookback not in {20, 60}
        or trend_period not in {50, 100}
        or excess_floor not in {0.01, 0.02}
        or stop_atr not in {1.5, 2.0}
        or hold not in {3, 5}
    ):
        raise DenseStrategyRuntimeError(
            "liquid-equity momentum parameters escaped the frozen grid"
        )
    indices = {
        symbol: {str(bar["date"]): index for index, bar in enumerate(bars)}
        for symbol, bars in daily.items()
    }
    session_index = {day: index for index, day in enumerate(session_dates)}
    universe_cache: dict[int, dict[str, list[str]]] | None = None
    feature_cache: dict[
        tuple[int, int, int], dict[str, list[tuple[float, str, float]]]
    ] | None = None
    if isinstance(dataset, dict) and "_prepared_daily_bars" in dataset:
        raw_universe_cache = dataset.setdefault(
            "_liquid_equity_momentum_universe_cache", {}
        )
        raw_feature_cache = dataset.setdefault(
            "_liquid_equity_momentum_feature_cache", {}
        )
        if isinstance(raw_universe_cache, dict):
            universe_cache = raw_universe_cache
        if isinstance(raw_feature_cache, dict):
            feature_cache = raw_feature_cache

    cached_universe = (
        universe_cache.get(hold) if universe_cache is not None else None
    )
    if cached_universe is not None:
        selected_by_date = cached_universe
    else:
        selected_by_date: dict[str, list[str]] = {}
        for decision_date in sorted(universe):
            decision_index = session_index.get(decision_date)
            raw_symbols = universe[decision_date]
            raw_identities = identities[decision_date]
            if (
                decision_index is None
                or decision_index < 60
                or not isinstance(raw_symbols, list)
                or raw_symbols != sorted(set(map(str, raw_symbols)))
                or not isinstance(raw_identities, Mapping)
                or set(raw_identities) != set(raw_symbols)
            ):
                raise DenseStrategyRuntimeError(
                    f"liquid-equity universe is invalid for {decision_date}"
                )
            expected_history = session_dates[decision_index - 59 : decision_index + 1]
            ranked_liquidity: list[tuple[float, str, str]] = []
            for symbol in map(str, raw_symbols):
                identity = raw_identities.get(symbol)
                bars = daily.get(symbol)
                symbol_index = indices.get(symbol, {}).get(decision_date)
                if (
                    not isinstance(identity, str)
                    or not identity
                    or bars is None
                    or symbol_index is None
                    or symbol_index < 59
                    or [
                        str(bar["date"])
                        for bar in bars[symbol_index - 59 : symbol_index + 1]
                    ]
                    != expected_history
                ):
                    continue
                history = bars[symbol_index - 59 : symbol_index + 1]
                close = float(history[-1]["close"])
                dollar_volume = [
                    float(bar["close"]) * float(bar["volume"]) for bar in history
                ]
                if (
                    close < 10
                    or statistics.median(dollar_volume[-20:]) < 50_000_000
                ):
                    continue
                raw_splits = split_dates.get(symbol, [])
                if (
                    not isinstance(raw_splits, list)
                    or raw_splits != sorted(set(map(str, raw_splits)))
                ):
                    raise DenseStrategyRuntimeError(
                        f"split actions are invalid for {symbol}"
                    )
                action_start = expected_history[0]
                hold_end_index = decision_index + hold
                if hold_end_index >= len(session_dates):
                    continue
                action_end = session_dates[hold_end_index]
                if any(action_start <= day <= action_end for day in raw_splits):
                    continue
                ranked_liquidity.append(
                    (statistics.median(dollar_volume), symbol, identity)
                )
            selected = sorted(
                ranked_liquidity,
                key=lambda item: (-item[0], item[1]),
            )[:250]
            if len(selected) != 250:
                raise DenseStrategyRuntimeError(
                    f"{decision_date}: fewer than 250 liquid common stocks"
                )
            selected_identities = [item[2] for item in selected]
            if len(selected_identities) != len(set(selected_identities)):
                raise DenseStrategyRuntimeError(
                    f"{decision_date}: duplicate listing identity"
                )
            selected_by_date[decision_date] = [item[1] for item in selected]
        if universe_cache is not None:
            universe_cache[hold] = selected_by_date

    feature_key = (lookback, trend_period, hold)
    features_by_date = (
        feature_cache.get(feature_key) if feature_cache is not None else None
    )
    if features_by_date is None:
        features_by_date = {}
        for decision_date, symbols in selected_by_date.items():
            features: list[tuple[float, str, float]] = []
            for symbol in symbols:
                bars = daily[symbol]
                symbol_index = indices[symbol][decision_date]
                if symbol_index < max(lookback, trend_period - 1):
                    continue
                trend = _sma(bars, symbol_index, trend_period)
                atr14 = _atr(bars, symbol_index)
                if (
                    trend is None
                    or atr14 is None
                    or float(bars[symbol_index]["close"]) <= trend
                ):
                    continue
                trailing_return = (
                    float(bars[symbol_index]["close"])
                    / float(bars[symbol_index - lookback]["close"])
                    - 1
                )
                features.append((trailing_return, symbol, atr14))
            features_by_date[decision_date] = features
        if feature_cache is not None:
            feature_cache[feature_key] = features_by_date

    candidates: list[dict[str, Any]] = []
    for decision_date in sorted(features_by_date):
        features = features_by_date[decision_date]
        if not features:
            continue
        benchmark = statistics.median(item[0] for item in features)
        ranked = sorted(
            (
                (-trailing_return, symbol, atr14)
                for trailing_return, symbol, atr14 in features
                if trailing_return - benchmark >= excess_floor
                and _cost_floor(trailing_return - benchmark)
            ),
            key=lambda item: (item[0], item[1]),
        )
        decision_index = session_index[decision_date]
        if decision_index + hold >= len(session_dates):
            continue
        entry_date = session_dates[decision_index + 1]
        expected_dates = session_dates[
            decision_index + 1 : decision_index + 1 + hold
        ]
        for rank, (negative_return, symbol, atr14) in enumerate(ranked, 1):
            bars = daily[symbol]
            entry_index = indices[symbol].get(entry_date)
            signal_id = (
                f"{entry_date}-{LIQUID_EQUITY_MOMENTUM_FAMILY}-{symbol}"
            )
            if entry_index is None:
                candidates.append(
                    {
                        "signal_id": signal_id,
                        "signal_date": entry_date,
                        "decision_date": decision_date,
                        "symbol": symbol,
                        "outcome": "missed_fill",
                        "rank": rank,
                        "rejection_reason": "missing_next_open",
                    }
                )
                continue
            observed_dates = [
                str(bar["date"])
                for bar in bars[entry_index : entry_index + hold]
            ]
            if observed_dates != expected_dates:
                candidates.append(
                    {
                        "signal_id": signal_id,
                        "signal_date": entry_date,
                        "decision_date": decision_date,
                        "symbol": symbol,
                        "outcome": "missed_fill",
                        "rank": rank,
                        "rejection_reason": "incomplete_holding_bars",
                    }
                )
                continue
            candidates.append(
                _daily_candidate(
                    family_id=LIQUID_EQUITY_MOMENTUM_FAMILY,
                    symbol=symbol,
                    decision_date=decision_date,
                    entry_date=entry_date,
                    bars=bars,
                    entry_index=entry_index,
                    stop_atr=stop_atr,
                    atr14=atr14,
                    hold_sessions=hold,
                    rank=rank,
                    score=-negative_return,
                )
            )
    return candidates


def _etf_cross_sectional_reversal_candidates(
    dataset: Mapping[str, Any], parameters: Mapping[str, Any]
) -> list[dict[str, Any]]:
    calendar = _calendar(dataset)
    daily = _daily_series(dataset)
    lookback = int(parameters["return_lookback_sessions"])
    trend_period = int(parameters["market_trend_sma"])
    lag_floor = float(parameters["minimum_lag_fraction"])
    stop_atr = float(parameters["stop_atr14"])
    hold = int(parameters["maximum_hold_sessions"])
    if "SPY" not in daily:
        raise DenseStrategyRuntimeError("ETF reversal data requires SPY")
    if (
        lookback not in {2, 3}
        or trend_period not in {100, 200}
        or lag_floor not in {0.005, 0.01}
        or stop_atr not in {1.0, 1.5}
        or hold not in {2, 3}
    ):
        raise DenseStrategyRuntimeError("ETF reversal parameters escaped the grid")
    indices = {
        symbol: {str(bar["date"]): index for index, bar in enumerate(bars)}
        for symbol, bars in daily.items()
    }
    candidates: list[dict[str, Any]] = []
    for calendar_index, decision_date in enumerate(calendar[:-1]):
        spy_index = indices["SPY"].get(decision_date)
        if spy_index is None or spy_index < max(lookback, trend_period - 1):
            continue
        market_trend = _sma(daily["SPY"], spy_index, trend_period)
        if (
            market_trend is None
            or float(daily["SPY"][spy_index]["close"]) <= market_trend
        ):
            continue
        features: list[tuple[float, str, float]] = []
        for symbol, bars in daily.items():
            symbol_index = indices[symbol].get(decision_date)
            if symbol_index is None or symbol_index < lookback:
                continue
            atr14 = _atr(bars, symbol_index)
            if atr14 is None:
                continue
            trailing_return = (
                float(bars[symbol_index]["close"])
                / float(bars[symbol_index - lookback]["close"])
                - 1
            )
            features.append((trailing_return, symbol, atr14))
        if len(features) != len(daily):
            continue
        benchmark = statistics.median(item[0] for item in features)
        ranked = [
            (trailing_return, symbol, atr14, benchmark - trailing_return)
            for trailing_return, symbol, atr14 in features
            if benchmark - trailing_return >= lag_floor
            and _cost_floor(benchmark - trailing_return)
        ]
        if not ranked:
            continue
        entry_date = calendar[calendar_index + 1]
        if calendar_index + 1 + hold > len(calendar):
            continue
        for rank, (trailing_return, symbol, atr14, lag) in enumerate(
            sorted(ranked, key=lambda item: (item[0], item[1])), 1
        ):
            bars = daily[symbol]
            entry_index = indices[symbol].get(entry_date)
            signal_id = (
                f"{entry_date}-{ETF_CROSS_SECTIONAL_REVERSAL_FAMILY}-{symbol}"
            )
            if entry_index is None:
                candidates.append(
                    {
                        "signal_id": signal_id,
                        "signal_date": entry_date,
                        "decision_date": decision_date,
                        "symbol": symbol,
                        "outcome": "missed_fill",
                        "rank": rank,
                        "rejection_reason": "missing_next_open",
                    }
                )
                continue
            if entry_index + hold > len(bars):
                continue
            exit_dates = {
                str(item["date"])
                for item in bars[entry_index : entry_index + hold]
            }
            expected_dates = set(
                calendar[calendar_index + 1 : calendar_index + 1 + hold]
            )
            if exit_dates != expected_dates:
                candidates.append(
                    {
                        "signal_id": signal_id,
                        "signal_date": entry_date,
                        "decision_date": decision_date,
                        "symbol": symbol,
                        "outcome": "missed_fill",
                        "rank": rank,
                        "rejection_reason": "incomplete_holding_bars",
                    }
                )
                continue
            candidate = _daily_candidate(
                family_id=ETF_CROSS_SECTIONAL_REVERSAL_FAMILY,
                symbol=symbol,
                decision_date=decision_date,
                entry_date=entry_date,
                bars=bars,
                entry_index=entry_index,
                stop_atr=stop_atr,
                atr14=atr14,
                hold_sessions=hold,
                rank=rank,
                score=-trailing_return,
            )
            candidate["expected_gross_move_fraction"] = lag
            candidates.append(candidate)
    return candidates


def _etf_high_continuation_candidates(
    dataset: Mapping[str, Any], parameters: Mapping[str, Any]
) -> list[dict[str, Any]]:
    calendar = _calendar(dataset)
    daily = _daily_series(dataset)
    return_floor = float(parameters["minimum_five_session_return_fraction"])
    proximity = float(parameters["minimum_close_to_prior_high_fraction"])
    trend_period = int(parameters["market_trend_sma"])
    stop_atr = float(parameters["stop_atr14"])
    hold = int(parameters["maximum_hold_sessions"])
    if "SPY" not in daily:
        raise DenseStrategyRuntimeError("ETF high-continuation data requires SPY")
    if (
        return_floor not in {0.02, 0.04}
        or proximity not in {0.98, 1.0}
        or trend_period not in {100, 200}
        or stop_atr not in {1.0, 1.5}
        or hold not in {3, 5}
    ):
        raise DenseStrategyRuntimeError(
            "ETF high-continuation parameters escaped the grid"
        )
    indices = {
        symbol: {str(bar["date"]): index for index, bar in enumerate(bars)}
        for symbol, bars in daily.items()
    }
    candidates: list[dict[str, Any]] = []
    for calendar_index, decision_date in enumerate(calendar[:-1]):
        spy_index = indices["SPY"].get(decision_date)
        if spy_index is None or spy_index < max(252, trend_period - 1):
            continue
        market_trend = _sma(daily["SPY"], spy_index, trend_period)
        if (
            market_trend is None
            or float(daily["SPY"][spy_index]["close"]) <= market_trend
        ):
            continue
        ranked: list[tuple[float, str, float]] = []
        for symbol, bars in daily.items():
            symbol_index = indices[symbol].get(decision_date)
            if symbol_index is None or symbol_index < 252:
                continue
            atr14 = _atr(bars, symbol_index)
            if atr14 is None:
                continue
            five_session_return = (
                float(bars[symbol_index]["close"])
                / float(bars[symbol_index - 5]["close"])
                - 1
            )
            prior_high = max(
                float(item["high"])
                for item in bars[symbol_index - 252 : symbol_index]
            )
            if (
                five_session_return < return_floor
                or float(bars[symbol_index]["close"]) < proximity * prior_high
                or not _cost_floor(five_session_return)
            ):
                continue
            ranked.append((-five_session_return, symbol, atr14))
        if not ranked:
            continue
        entry_date = calendar[calendar_index + 1]
        if calendar_index + 1 + hold > len(calendar):
            continue
        for rank, (negative_return, symbol, atr14) in enumerate(sorted(ranked), 1):
            bars = daily[symbol]
            entry_index = indices[symbol].get(entry_date)
            signal_id = f"{entry_date}-{ETF_HIGH_CONTINUATION_FAMILY}-{symbol}"
            if entry_index is None:
                candidates.append(
                    {
                        "signal_id": signal_id,
                        "signal_date": entry_date,
                        "decision_date": decision_date,
                        "symbol": symbol,
                        "outcome": "missed_fill",
                        "rank": rank,
                        "rejection_reason": "missing_next_open",
                    }
                )
                continue
            if entry_index + hold > len(bars):
                continue
            exit_dates = {
                str(item["date"])
                for item in bars[entry_index : entry_index + hold]
            }
            expected_dates = set(
                calendar[calendar_index + 1 : calendar_index + 1 + hold]
            )
            if exit_dates != expected_dates:
                candidates.append(
                    {
                        "signal_id": signal_id,
                        "signal_date": entry_date,
                        "decision_date": decision_date,
                        "symbol": symbol,
                        "outcome": "missed_fill",
                        "rank": rank,
                        "rejection_reason": "incomplete_holding_bars",
                    }
                )
                continue
            candidate = _daily_candidate(
                family_id=ETF_HIGH_CONTINUATION_FAMILY,
                symbol=symbol,
                decision_date=decision_date,
                entry_date=entry_date,
                bars=bars,
                entry_index=entry_index,
                stop_atr=stop_atr,
                atr14=atr14,
                hold_sessions=hold,
                rank=rank,
                score=-negative_return,
            )
            candidate["expected_gross_move_fraction"] = -negative_return
            candidates.append(candidate)
    return candidates


def _turn_of_month_membership(
    decision_date: str,
    exchange_calendar: Sequence[str],
    *,
    before_sessions: int,
    after_sessions: int,
) -> bool:
    month_sessions = [
        day for day in exchange_calendar if day[:7] == decision_date[:7]
    ]
    if decision_date not in month_sessions:
        raise DenseStrategyRuntimeError(
            "turn-of-month decision is absent from the exchange calendar"
        )
    position = month_sessions.index(decision_date)
    sessions_remaining = len(month_sessions) - position - 1
    return (
        1 <= sessions_remaining <= before_sessions
        or position < after_sessions
    )


def _turn_of_month_candidates(
    dataset: Mapping[str, Any], parameters: Mapping[str, Any]
) -> list[dict[str, Any]]:
    calendar = _calendar(dataset)
    daily = _daily_series(dataset)
    before_sessions = int(parameters["sessions_before_month_end"])
    after_sessions = int(parameters["sessions_after_month_start"])
    trend_period = int(parameters["market_trend_sma"])
    stop_atr = float(parameters["stop_atr14"])
    hold = int(parameters["maximum_hold_sessions"])
    if (
        set(daily) != {"SPY"}
        or before_sessions not in {1, 3}
        or after_sessions not in {1, 3}
        or trend_period not in {100, 200}
        or stop_atr not in {1.0, 1.5}
        or hold not in {2, 4}
    ):
        raise DenseStrategyRuntimeError(
            "turn-of-month inputs or parameters escaped the grid"
        )
    bars = daily["SPY"]
    exchange_calendar = [str(bar["date"]) for bar in bars]
    indices = {str(bar["date"]): index for index, bar in enumerate(bars)}
    candidates: list[dict[str, Any]] = []
    for calendar_index, decision_date in enumerate(calendar[:-1]):
        if not _turn_of_month_membership(
            decision_date,
            exchange_calendar,
            before_sessions=before_sessions,
            after_sessions=after_sessions,
        ):
            continue
        decision_index = indices.get(decision_date)
        if decision_index is None or decision_index < trend_period - 1:
            continue
        trend = _sma(bars, decision_index, trend_period)
        atr14 = _atr(bars, decision_index)
        decision_close = float(bars[decision_index]["close"])
        if (
            trend is None
            or atr14 is None
            or decision_close <= trend
            or not _cost_floor(atr14 / decision_close)
        ):
            continue
        entry_date = calendar[calendar_index + 1]
        entry_index = indices.get(entry_date)
        signal_id = f"{entry_date}-{ETF_TURN_OF_MONTH_FAMILY}-SPY"
        if entry_index is None:
            candidates.append(
                {
                    "signal_id": signal_id,
                    "signal_date": entry_date,
                    "decision_date": decision_date,
                    "symbol": "SPY",
                    "outcome": "missed_fill",
                    "rank": 1,
                    "rejection_reason": "missing_next_open",
                }
            )
            continue
        if entry_index + hold > len(bars):
            continue
        expected_dates = set(
            calendar[calendar_index + 1 : calendar_index + 1 + hold]
        )
        exit_dates = {
            str(item["date"])
            for item in bars[entry_index : entry_index + hold]
        }
        if len(expected_dates) != hold or exit_dates != expected_dates:
            candidates.append(
                {
                    "signal_id": signal_id,
                    "signal_date": entry_date,
                    "decision_date": decision_date,
                    "symbol": "SPY",
                    "outcome": "missed_fill",
                    "rank": 1,
                    "rejection_reason": "incomplete_holding_bars",
                }
            )
            continue
        candidate = _daily_candidate(
            family_id=ETF_TURN_OF_MONTH_FAMILY,
            symbol="SPY",
            decision_date=decision_date,
            entry_date=entry_date,
            bars=bars,
            entry_index=entry_index,
            stop_atr=stop_atr,
            atr14=atr14,
            hold_sessions=hold,
            rank=1,
            score=1.0,
        )
        candidate["expected_gross_move_fraction"] = atr14 / decision_close
        candidates.append(candidate)
    return candidates


def _sector_rotation_candidates(
    dataset: Mapping[str, Any], parameters: Mapping[str, Any]
) -> list[dict[str, Any]]:
    calendar = _calendar(dataset)
    daily = _daily_series(dataset)
    lookback = int(parameters["return_lookback_sessions"])
    excess_floor = float(parameters["minimum_excess_return_fraction"])
    trend_period = int(parameters["market_trend_sma"])
    stop_atr = float(parameters["stop_atr14"])
    hold = int(parameters["maximum_hold_sessions"])
    if (
        "SPY" not in daily
        or len(daily) != 12
        or lookback not in {5, 20}
        or excess_floor not in {0.0, 0.01}
        or trend_period not in {20, 60}
        or stop_atr not in {1.0, 1.5}
        or hold not in {1, 3}
    ):
        raise DenseStrategyRuntimeError(
            "sector-rotation inputs or parameters escaped the grid"
        )
    indices = {
        symbol: {str(bar["date"]): index for index, bar in enumerate(bars)}
        for symbol, bars in daily.items()
    }
    candidates: list[dict[str, Any]] = []
    for calendar_index, decision_date in enumerate(calendar[:-1]):
        spy_index = indices["SPY"].get(decision_date)
        if spy_index is None or spy_index < max(lookback, trend_period - 1):
            continue
        trend = _sma(daily["SPY"], spy_index, trend_period)
        if (
            trend is None
            or float(daily["SPY"][spy_index]["close"]) <= trend
        ):
            continue
        spy_return = (
            float(daily["SPY"][spy_index]["close"])
            / float(daily["SPY"][spy_index - lookback]["close"])
            - 1
        )
        ranked: list[tuple[float, str, float, float]] = []
        for symbol, bars in daily.items():
            if symbol == "SPY":
                continue
            symbol_index = indices[symbol].get(decision_date)
            if symbol_index is None or symbol_index < lookback:
                continue
            atr14 = _atr(bars, symbol_index)
            if atr14 is None:
                continue
            trailing_return = (
                float(bars[symbol_index]["close"])
                / float(bars[symbol_index - lookback]["close"])
                - 1
            )
            excess = trailing_return - spy_return
            if excess + 1e-12 < excess_floor or not _cost_floor(excess):
                continue
            ranked.append((-trailing_return, symbol, atr14, excess))
        if not ranked:
            continue
        entry_date = calendar[calendar_index + 1]
        if calendar_index + 1 + hold > len(calendar):
            continue
        for rank, (negative_return, symbol, atr14, excess) in enumerate(
            sorted(ranked), 1
        ):
            bars = daily[symbol]
            entry_index = indices[symbol].get(entry_date)
            signal_id = (
                f"{entry_date}-{SECTOR_ETF_ROTATION_FAMILY}-{symbol}"
            )
            if entry_index is None:
                candidates.append(
                    {
                        "signal_id": signal_id,
                        "signal_date": entry_date,
                        "decision_date": decision_date,
                        "symbol": symbol,
                        "outcome": "missed_fill",
                        "rank": rank,
                        "rejection_reason": "missing_next_open",
                    }
                )
                continue
            if entry_index + hold > len(bars):
                continue
            expected_dates = set(
                calendar[
                    calendar_index + 1 : calendar_index + 1 + hold
                ]
            )
            exit_dates = {
                str(item["date"])
                for item in bars[entry_index : entry_index + hold]
            }
            if len(expected_dates) != hold or exit_dates != expected_dates:
                candidates.append(
                    {
                        "signal_id": signal_id,
                        "signal_date": entry_date,
                        "decision_date": decision_date,
                        "symbol": symbol,
                        "outcome": "missed_fill",
                        "rank": rank,
                        "rejection_reason": "incomplete_holding_bars",
                    }
                )
                continue
            candidate = _daily_candidate(
                family_id=SECTOR_ETF_ROTATION_FAMILY,
                symbol=symbol,
                decision_date=decision_date,
                entry_date=entry_date,
                bars=bars,
                entry_index=entry_index,
                stop_atr=stop_atr,
                atr14=atr14,
                hold_sessions=hold,
                rank=rank,
                score=-negative_return,
            )
            candidate["expected_gross_move_fraction"] = excess
            candidates.append(candidate)
    return candidates


def _close_to_open_candidates(
    dataset: Mapping[str, Any], parameters: Mapping[str, Any]
) -> list[dict[str, Any]]:
    calendar = _calendar(dataset)
    daily = _daily_series(dataset)
    sessions = _fifteen_minute_sessions(dataset)
    decision_time = str(parameters["decision_bar_time"])
    return_floor = float(parameters["minimum_session_return_fraction"])
    trend_period = int(parameters["prior_trend_sma"])
    stop_atr = float(parameters["stop_atr14"])
    exit_timing = str(parameters["exit_timing"])
    decision_indices = {"15:15": 23, "15:30": 24}
    if (
        set(daily) != set(CLOSE_TO_OPEN_ETF_SYMBOLS)
        or decision_time not in decision_indices
        or return_floor not in {0.005, 0.01}
        or trend_period not in {20, 60}
        or stop_atr not in {0.5, 1.0}
        or exit_timing not in {"next_open", "next_0945_close"}
        or set(sessions) != set(calendar)
    ):
        raise DenseStrategyRuntimeError(
            "close-to-open inputs or parameters escaped the frozen grid"
        )
    if any(
        set(day_symbols) != set(daily)
        for day_symbols in sessions.values()
    ):
        raise DenseStrategyRuntimeError(
            "close-to-open sessions do not cover the complete ETF universe"
        )
    daily_indices = {
        symbol: {str(bar["date"]): index for index, bar in enumerate(bars)}
        for symbol, bars in daily.items()
    }
    candidates: list[dict[str, Any]] = []
    decision_index = decision_indices[decision_time]
    entry_index = decision_index + 1
    for calendar_index, decision_date in enumerate(calendar[:-1]):
        next_date = calendar[calendar_index + 1]
        ranked: list[
            tuple[float, str, float, Sequence[Mapping[str, Any]]]
        ] = []
        for symbol in sorted(daily):
            bars = daily[symbol]
            day_index = daily_indices[symbol].get(decision_date)
            current_rows = sessions[decision_date][symbol]
            next_rows = sessions[next_date][symbol]
            if (
                day_index is None
                or day_index < max(trend_period, 15)
                or len(current_rows) != 26
                or len(next_rows) < 1
            ):
                continue
            signal_close = float(current_rows[decision_index]["close"])
            session_return = (
                signal_close / float(current_rows[0]["open"]) - 1
            )
            prior_trend = statistics.fmean(
                float(item["close"])
                for item in bars[day_index - trend_period : day_index]
            )
            atr14 = _atr(bars, day_index - 1)
            if (
                atr14 is None
                or session_return + 1e-12 < return_floor
                or signal_close <= prior_trend
                or not _cost_floor(session_return)
            ):
                continue
            ranked.append(
                (-session_return, symbol, atr14, current_rows)
            )
        for rank, (
            negative_return,
            symbol,
            atr14,
            current_rows,
        ) in enumerate(sorted(ranked), 1):
            next_rows = sessions[next_date][symbol]
            entry_price = float(current_rows[entry_index]["open"])
            stop_price = entry_price - stop_atr * atr14
            signal_id = (
                f"{decision_date}-{ETF_CLOSE_TO_OPEN_FAMILY}-{symbol}"
            )
            if stop_price <= 0 or stop_price >= entry_price:
                candidates.append(
                    {
                        "signal_id": signal_id,
                        "signal_date": decision_date,
                        "decision_date": decision_date,
                        "symbol": symbol,
                        "outcome": "rejected",
                        "rank": rank,
                        "rejection_reason": "invalid_structural_stop",
                    }
                )
                continue
            exit_date = next_date
            exit_price: float | None = None
            stop_executed = False
            for row in current_rows[entry_index:]:
                opening = float(row["open"])
                if opening <= stop_price:
                    exit_price = opening
                    stop_executed = True
                    exit_date = decision_date
                    break
                if float(row["low"]) <= stop_price:
                    exit_price = stop_price
                    stop_executed = True
                    exit_date = decision_date
                    break
            if exit_price is None:
                next_open = float(next_rows[0]["open"])
                if next_open <= stop_price:
                    exit_price = next_open
                    stop_executed = True
                elif (
                    exit_timing == "next_0945_close"
                    and float(next_rows[0]["low"]) <= stop_price
                ):
                    exit_price = stop_price
                    stop_executed = True
                else:
                    exit_price = (
                        next_open
                        if exit_timing == "next_open"
                        else float(next_rows[0]["close"])
                    )
            marks = (
                {decision_date: exit_price}
                if exit_date == decision_date
                else {
                    decision_date: float(current_rows[-1]["close"]),
                    next_date: exit_price,
                }
            )
            candidates.append(
                {
                    "signal_id": signal_id,
                    "signal_date": decision_date,
                    "decision_date": decision_date,
                    "symbol": symbol,
                    "outcome": "eligible",
                    "rank": rank,
                    "score": -negative_return,
                    "expected_gross_move_fraction": -negative_return,
                    "entry_price": entry_price,
                    "stop_price": stop_price,
                    "exit_date": exit_date,
                    "exit_price": exit_price,
                    "marks": marks,
                    "stop_executed": stop_executed,
                    "planned_stop_distance": entry_price - stop_price,
                }
            )
    return candidates


def _minute_sessions(
    dataset: Mapping[str, Any],
) -> dict[str, dict[str, list[dict[str, Any]]]]:
    prepared = dataset.get("_prepared_minute_bars")
    if isinstance(prepared, dict):
        return prepared
    raw = dataset.get("minute_bars")
    if not isinstance(raw, Mapping) or not raw:
        raise DenseStrategyRuntimeError("minute_bars must be a non-empty object")
    sessions: dict[str, dict[str, list[dict[str, Any]]]] = {}
    for day, raw_symbols in raw.items():
        if not isinstance(raw_symbols, Mapping):
            raise DenseStrategyRuntimeError(f"minute_bars.{day} must be an object")
        sessions[str(day)] = {}
        for raw_symbol, raw_bars in raw_symbols.items():
            symbol = str(raw_symbol)
            if not isinstance(raw_bars, list) or not raw_bars:
                raise DenseStrategyRuntimeError(
                    f"minute_bars.{day}.{symbol} must be non-empty"
                )
            bars: list[dict[str, Any]] = []
            timestamps: list[str] = []
            for index, raw_bar in enumerate(raw_bars):
                if not isinstance(raw_bar, Mapping):
                    raise DenseStrategyRuntimeError(
                        f"minute_bars.{day}.{symbol}[{index}] must be an object"
                    )
                timestamp = raw_bar.get("timestamp")
                if not isinstance(timestamp, str) or not timestamp:
                    raise DenseStrategyRuntimeError("minute timestamp is invalid")
                try:
                    observed = datetime.fromisoformat(timestamp)
                except ValueError as exc:
                    raise DenseStrategyRuntimeError(
                        "minute timestamp must be ISO formatted"
                    ) from exc
                if observed.tzinfo is None or observed.date().isoformat() != str(day):
                    raise DenseStrategyRuntimeError(
                        "minute timestamp needs a timezone and matching session date"
                    )
                bar = {"timestamp": timestamp}
                for field in (
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "vwap_numerator",
                    "vwap_denominator",
                ):
                    bar[field] = _number(
                        raw_bar.get(field),
                        f"minute_bars.{day}.{symbol}[{index}].{field}",
                        positive=field not in {"volume", "vwap_numerator", "vwap_denominator"},
                    )
                if (
                    bar["volume"] < 0
                    or bar["vwap_numerator"] < 0
                    or bar["vwap_denominator"] < 0
                    or not (
                        bar["low"] <= min(bar["open"], bar["close"])
                        and bar["high"] >= max(bar["open"], bar["close"])
                    )
                ):
                    raise DenseStrategyRuntimeError("minute OHLCV/VWAP data is invalid")
                timestamps.append(timestamp)
                bars.append(bar)
            if timestamps != sorted(timestamps) or len(timestamps) != len(
                set(timestamps)
            ):
                raise DenseStrategyRuntimeError(
                    "minute timestamps must be unique and chronological"
                )
            expected_by_date = dataset.get("regular_session_minutes_by_date", {})
            if not isinstance(expected_by_date, Mapping):
                raise DenseStrategyRuntimeError(
                    "regular_session_minutes_by_date must be an object"
                )
            expected_minutes = expected_by_date.get(str(day), 390)
            if (
                isinstance(expected_minutes, bool)
                or not isinstance(expected_minutes, int)
                or expected_minutes < 1
                or len(bars) != expected_minutes
            ):
                raise DenseStrategyRuntimeError(
                    f"minute_bars.{day}.{symbol} is not a complete regular session"
                )
            parsed_times = [datetime.fromisoformat(item) for item in timestamps]
            if parsed_times[0].timetz().replace(tzinfo=None) != time(9, 30) or any(
                right - left != timedelta(minutes=1)
                for left, right in zip(parsed_times, parsed_times[1:])
            ):
                raise DenseStrategyRuntimeError(
                    "minute bars must start at 09:30 and remain one minute apart"
                )
            sessions[str(day)][symbol] = bars
    return sessions


def _intraday_missed_data_dates(dataset: Mapping[str, Any]) -> set[str]:
    raw = dataset.get("missed_data_dates", [])
    if not isinstance(raw, list) or any(
        not isinstance(item, str) or not item for item in raw
    ):
        raise DenseStrategyRuntimeError(
            "missed_data_dates must be an array of ISO dates"
        )
    if raw != sorted(raw) or len(raw) != len(set(raw)):
        raise DenseStrategyRuntimeError(
            "missed_data_dates must be unique and chronological"
        )
    return set(raw)


def prepare_dataset(dataset: Mapping[str, Any]) -> dict[str, Any]:
    """Validate and normalize row-level bars once for every trial in a process."""

    family_id = dataset.get("family_id")
    if family_id not in SUPPORTED_FAMILIES:
        raise DenseStrategyRuntimeError("dataset family binding is unsupported")
    calendar = _calendar(dataset)
    prepared = dict(dataset)
    if family_id == ETF_CLOSE_TO_OPEN_FAMILY:
        daily = _daily_series(dataset)
        sessions = _fifteen_minute_sessions(dataset)
        symbols = dataset.get("symbols")
        if (
            symbols != list(CLOSE_TO_OPEN_ETF_SYMBOLS)
            or set(daily) != set(symbols)
            or set(sessions) != set(calendar)
            or any(set(day_symbols) != set(symbols) for day_symbols in sessions.values())
        ):
            raise DenseStrategyRuntimeError(
                "close-to-open data does not match its complete frozen universe"
            )
        prepared["_prepared_daily_bars"] = daily
        prepared["_prepared_fifteen_minute_bars"] = sessions
    elif family_id in {
        *INTRADAY_ETF_FAMILIES,
        OVERSOLD_REVERSAL_FAMILY,
        EQUITY_GAP_CONTINUATION_FAMILY,
        VOLATILITY_COMPRESSION_FAMILY,
    }:
        sessions = _minute_sessions(dataset)
        if family_id in INTRADAY_ETF_FAMILIES:
            symbols = dataset.get("symbols")
            if not isinstance(symbols, list) or not symbols:
                raise DenseStrategyRuntimeError(
                    "intraday dataset must name its complete frozen symbols"
                )
            expected_symbols = {str(symbol) for symbol in symbols}
            missed_dates = _intraday_missed_data_dates(dataset)
            if not missed_dates.issubset(sessions):
                raise DenseStrategyRuntimeError(
                    "intraday missed-data dates lack retained session evidence"
                )
            for day, day_symbols in sessions.items():
                observed_symbols = set(day_symbols)
                if day in missed_dates:
                    if not observed_symbols < expected_symbols:
                        raise DenseStrategyRuntimeError(
                            "intraday missed-data dates must contain at most "
                            "a strict subset of the frozen universe"
                        )
                elif observed_symbols != expected_symbols:
                    raise DenseStrategyRuntimeError(
                        "intraday sessions do not cover the complete frozen universe"
                    )
            prepared["_prepared_missed_data_dates"] = missed_dates
        else:
            candidates = dataset.get("candidate_symbols_by_date")
            calendar = _calendar(dataset)
            if (
                not isinstance(candidates, Mapping)
                or set(candidates) != set(calendar)
            ):
                raise DenseStrategyRuntimeError(
                    "oversold dataset must bind every frozen daily candidate universe"
                )
            for day in calendar:
                raw_symbols = candidates[day]
                if (
                    not isinstance(raw_symbols, list)
                    or raw_symbols != sorted(set(map(str, raw_symbols)))
                ):
                    raise DenseStrategyRuntimeError(
                        f"frozen candidate universe is invalid for {day}"
                    )
                if not set(sessions.get(day, {})).issubset(set(raw_symbols)):
                    raise DenseStrategyRuntimeError(
                        f"minute inputs escaped the frozen universe for {day}"
                    )
            if family_id == EQUITY_GAP_CONTINUATION_FAMILY:
                metadata = dataset.get("candidate_metadata_by_date")
                if not isinstance(metadata, Mapping) or set(metadata) != set(
                    calendar
                ):
                    raise DenseStrategyRuntimeError(
                        "gap-continuation metadata does not match the calendar"
                    )
                for day in calendar:
                    rows = metadata[day]
                    if not isinstance(rows, Mapping) or set(rows) != set(
                        map(str, candidates[day])
                    ):
                        raise DenseStrategyRuntimeError(
                            f"gap-continuation metadata is incomplete for {day}"
                        )
                    for symbol, row in rows.items():
                        if (
                            not isinstance(row, Mapping)
                            or row.get("symbol") != symbol
                            or _number(
                                row.get("gap_fraction"),
                                f"candidate_metadata_by_date.{day}.{symbol}.gap_fraction",
                            )
                            < 0
                        ):
                            raise DenseStrategyRuntimeError(
                                f"gap-continuation metadata is invalid for {day} {symbol}"
                            )
        prepared["_prepared_minute_bars"] = sessions
        if family_id == OVERSOLD_REVERSAL_FAMILY:
            prepared["_oversold_feature_cache"] = _oversold_feature_cache(
                sessions
            )
        elif family_id == EQUITY_GAP_CONTINUATION_FAMILY:
            prepared["_gap_continuation_feature_cache"] = (
                _gap_continuation_feature_cache(sessions)
            )
        elif family_id == VOLATILITY_COMPRESSION_FAMILY:
            prepared["_compression_feature_cache"] = (
                _compression_feature_cache(sessions)
            )
    else:
        daily = _daily_series(dataset)
        if family_id == LIQUID_EQUITY_MOMENTUM_FAMILY:
            universe = dataset.get("universe_by_date")
            identities = dataset.get("universe_identity_by_date")
            split_dates = dataset.get("split_execution_dates_by_symbol")
            session_dates = dataset.get("session_dates")
            if not (
                isinstance(universe, Mapping)
                and isinstance(identities, Mapping)
                and set(universe) == set(identities)
                and isinstance(split_dates, Mapping)
                and isinstance(session_dates, list)
                and session_dates == sorted(set(map(str, session_dates)))
                and set(calendar).issubset(session_dates)
            ):
                raise DenseStrategyRuntimeError(
                    "liquid-equity momentum metadata is incomplete"
                )
            for decision_date, symbols in universe.items():
                if (
                    decision_date not in calendar
                    or not isinstance(symbols, list)
                    or symbols != sorted(set(map(str, symbols)))
                    or not isinstance(identities[decision_date], Mapping)
                    or set(identities[decision_date]) != set(symbols)
                ):
                    raise DenseStrategyRuntimeError(
                        f"liquid-equity momentum universe is invalid for {decision_date}"
                    )
            prepared["_liquid_equity_momentum_universe_cache"] = {}
            prepared["_liquid_equity_momentum_feature_cache"] = {}
        if family_id in {
            *ETF_PULLBACK_FAMILIES,
            SPY_RSI2_PULLBACK_FAMILY,
            ETF_CROSS_SECTIONAL_MOMENTUM_FAMILY,
            ETF_CROSS_SECTIONAL_REVERSAL_FAMILY,
            ETF_HIGH_CONTINUATION_FAMILY,
            ETF_TURN_OF_MONTH_FAMILY,
            SECTOR_ETF_ROTATION_FAMILY,
            SECTOR_ETF_GAP_DRIFT_FAMILY,
            FLIGHT_TO_SAFETY_REBOUND_FAMILY,
            FLIGHT_TO_SAFETY_REPLICATION_FAMILY,
            FLIGHT_TO_SAFETY_REPLICATION_V2_FAMILY,
            BREADTH_CAPITULATION_REBOUND_FAMILY,
            CROSS_STYLE_BREADTH_CONTINUATION_FAMILY,
            STYLE_ETF_BREAKOUT_CONTINUATION_FAMILY,
            *ETF_OVERSOLD_FAMILIES,
        }:
            symbols = dataset.get("symbols")
            if not isinstance(symbols, list) or set(map(str, symbols)) != set(daily):
                raise DenseStrategyRuntimeError(
                    "ETF pullback data does not match its frozen symbol universe"
                )
            evaluation_dates = set(_calendar(dataset))
            if any(
                not evaluation_dates.issubset(
                    {str(bar["date"]) for bar in symbol_bars}
                )
                for symbol_bars in daily.values()
            ):
                raise DenseStrategyRuntimeError(
                    "ETF pullback data omits a frozen symbol-session"
                )
        prepared["_prepared_daily_bars"] = daily
        if family_id in {
            *ETF_PULLBACK_FAMILIES,
            SPY_RSI2_PULLBACK_FAMILY,
            SECTOR_ETF_GAP_DRIFT_FAMILY,
            FLIGHT_TO_SAFETY_REBOUND_FAMILY,
            FLIGHT_TO_SAFETY_REPLICATION_FAMILY,
            FLIGHT_TO_SAFETY_REPLICATION_V2_FAMILY,
            BREADTH_CAPITULATION_REBOUND_FAMILY,
            *ETF_OVERSOLD_FAMILIES,
        }:
            prepared["_etf_pullback_feature_cache"] = (
                _etf_pullback_feature_cache(daily)
            )
    return prepared


def _intraday_atr(bars: Sequence[Mapping[str, Any]], end_index: int) -> float | None:
    if end_index < 14:
        return None
    return statistics.fmean(_true_ranges(bars[: end_index + 1])[-14:])


def _intraday_exit(
    bars: Sequence[Mapping[str, Any]],
    *,
    entry_index: int,
    stop_price: float,
    target_price: float,
) -> tuple[float, bool]:
    for bar in bars[entry_index:]:
        opening = float(bar["open"])
        if opening <= stop_price:
            return opening, True
        if opening >= target_price:
            return opening, False
        stop_hit = float(bar["low"]) <= stop_price
        target_hit = float(bar["high"]) >= target_price
        if stop_hit:
            return stop_price, True
        if target_hit:
            return target_price, False
    return float(bars[-1]["close"]), False


def _intraday_candidates(
    dataset: Mapping[str, Any],
    parameters: Mapping[str, Any],
    *,
    family_id: str = INTRADAY_ETF_FAMILY,
) -> list[dict[str, Any]]:
    calendar = _calendar(dataset)
    sessions = _minute_sessions(dataset)
    if set(calendar) - set(sessions):
        raise DenseStrategyRuntimeError("minute_bars omit frozen evaluation dates")
    prepared_missed = dataset.get("_prepared_missed_data_dates")
    missed_dates = (
        prepared_missed
        if isinstance(prepared_missed, set)
        else _intraday_missed_data_dates(dataset)
    )
    if not missed_dates.issubset(sessions):
        raise DenseStrategyRuntimeError(
            "intraday missed-data dates escaped the retained sessions"
        )
    opening_window = int(parameters["opening_window_minutes"])
    threshold = float(parameters["downside_z_threshold"])
    reclaim_bars = int(parameters["vwap_reclaim_completed_bars"])
    stop_atr = float(parameters["stop_intraday_atr"])
    target_r = float(parameters["target_r"])
    evaluation_dates = set(calendar)
    histories: dict[str, list[float]] = {}
    candidates: list[dict[str, Any]] = []
    for day in sorted(sessions):
        qualified: dict[str, dict[str, Any]] = {}
        for symbol, bars in sessions[day].items():
            if len(bars) <= opening_window + reclaim_bars:
                continue
            opening_return = (
                float(bars[opening_window - 1]["close"]) / float(bars[0]["open"])
                - 1
            )
            history = histories.setdefault(symbol, [])
            z_score = _z_score(opening_return, history)
            history.append(opening_return)
            if day not in evaluation_dates or day in missed_dates:
                continue
            if (
                z_score is None
                or z_score > threshold
                or not _cost_floor(abs(opening_return))
            ):
                continue
            qualified[symbol] = {
                "z_score": z_score,
                "opening_return": opening_return,
                "numerator": sum(
                    float(item["vwap_numerator"])
                    for item in bars[:opening_window]
                ),
                "denominator": sum(
                    float(item["vwap_denominator"])
                    for item in bars[:opening_window]
                ),
                "above": 0,
            }
        if day not in evaluation_dates or day in missed_dates or not qualified:
            continue
        selected: tuple[float, str, int, float] | None = None
        maximum_completed_index = min(
            len(sessions[day][symbol]) - 2 for symbol in qualified
        )
        for index in range(opening_window, maximum_completed_index + 1):
            triggered: list[tuple[float, str, int, float]] = []
            for symbol, state in qualified.items():
                if state.get("triggered") is True:
                    continue
                bar = sessions[day][symbol][index]
                state["numerator"] += float(bar["vwap_numerator"])
                state["denominator"] += float(bar["vwap_denominator"])
                if state["denominator"] <= 0:
                    state["above"] = 0
                    continue
                exact_vwap = state["numerator"] / state["denominator"]
                state["above"] = (
                    state["above"] + 1
                    if float(bar["close"]) > exact_vwap
                    else 0
                )
                if state["above"] >= reclaim_bars:
                    state["triggered"] = True
                    atr = _intraday_atr(sessions[day][symbol], index)
                    if atr is not None:
                        triggered.append(
                            (float(state["z_score"]), symbol, index + 1, atr)
                        )
            if triggered:
                selected = sorted(triggered)[0]
                break
        if selected is None:
            continue
        z_score, symbol, entry_index, atr = selected
        bars = sessions[day][symbol]
        signal_id = f"{day}-{family_id}-{symbol}"
        if entry_index >= len(bars):
            candidates.append(
                {
                    "signal_id": signal_id,
                    "signal_date": day,
                    "decision_date": day,
                    "symbol": symbol,
                    "outcome": "missed_fill",
                    "rank": 1,
                    "rejection_reason": "missing_next_bar",
                }
            )
            continue
        entry_price = float(bars[entry_index]["open"])
        stop_price = entry_price - stop_atr * atr
        if stop_price <= 0 or stop_price >= entry_price:
            candidates.append(
                {
                    "signal_id": signal_id,
                    "signal_date": day,
                    "decision_date": day,
                    "symbol": symbol,
                    "outcome": "rejected",
                    "rank": 1,
                    "rejection_reason": "invalid_structural_stop",
                }
            )
            continue
        target_price = entry_price + target_r * (entry_price - stop_price)
        exit_price, stop_executed = _intraday_exit(
            bars,
            entry_index=entry_index,
            stop_price=stop_price,
            target_price=target_price,
        )
        candidates.append(
            {
                "signal_id": signal_id,
                "signal_date": day,
                "decision_date": day,
                "symbol": symbol,
                "outcome": "eligible",
                "rank": 1,
                "score": z_score,
                "entry_price": entry_price,
                "stop_price": stop_price,
                "target_price": target_price,
                "exit_date": day,
                "exit_price": exit_price,
                "marks": {day: exit_price},
                "stop_executed": stop_executed,
                "planned_stop_distance": entry_price - stop_price,
            }
        )
    return candidates


def _intraday_momentum_candidates(
    dataset: Mapping[str, Any],
    parameters: Mapping[str, Any],
) -> list[dict[str, Any]]:
    calendar = _calendar(dataset)
    sessions = _minute_sessions(dataset)
    if set(calendar) - set(sessions):
        raise DenseStrategyRuntimeError(
            "minute_bars omit frozen evaluation dates"
        )
    prepared_missed = dataset.get("_prepared_missed_data_dates")
    missed_dates = (
        prepared_missed
        if isinstance(prepared_missed, set)
        else _intraday_missed_data_dates(dataset)
    )
    opening_window = int(parameters["opening_window_minutes"])
    minimum_return = float(parameters["minimum_opening_return"])
    confirmation_bars = int(
        parameters["vwap_confirmation_completed_bars"]
    )
    stop_atr = float(parameters["stop_intraday_atr"])
    target_r = float(parameters["target_r"])
    evaluation_dates = set(calendar)
    candidates: list[dict[str, Any]] = []
    for day in sorted(sessions):
        if day not in evaluation_dates or day in missed_dates:
            continue
        qualified: dict[str, dict[str, Any]] = {}
        for symbol, bars in sessions[day].items():
            if len(bars) <= opening_window + confirmation_bars:
                continue
            opening_return = (
                float(bars[opening_window - 1]["close"])
                / float(bars[0]["open"])
                - 1
            )
            if (
                opening_return < minimum_return
                or not _cost_floor(opening_return)
            ):
                continue
            qualified[symbol] = {
                "opening_return": opening_return,
                "numerator": sum(
                    float(item["vwap_numerator"])
                    for item in bars[:opening_window]
                ),
                "denominator": sum(
                    float(item["vwap_denominator"])
                    for item in bars[:opening_window]
                ),
                "above": 0,
            }
        if not qualified:
            continue
        selected: tuple[float, str, int, float] | None = None
        maximum_completed_index = min(
            len(sessions[day][symbol]) - 2 for symbol in qualified
        )
        for index in range(
            opening_window, maximum_completed_index + 1
        ):
            triggered: list[tuple[float, str, int, float]] = []
            for symbol, state in qualified.items():
                if state.get("triggered") is True:
                    continue
                bar = sessions[day][symbol][index]
                state["numerator"] += float(bar["vwap_numerator"])
                state["denominator"] += float(bar["vwap_denominator"])
                if state["denominator"] <= 0:
                    state["above"] = 0
                    continue
                exact_vwap = state["numerator"] / state["denominator"]
                state["above"] = (
                    state["above"] + 1
                    if float(bar["close"]) > exact_vwap
                    else 0
                )
                if state["above"] >= confirmation_bars:
                    state["triggered"] = True
                    atr = _intraday_atr(
                        sessions[day][symbol], index
                    )
                    if atr is not None:
                        triggered.append(
                            (
                                -float(state["opening_return"]),
                                symbol,
                                index + 1,
                                atr,
                            )
                        )
            if triggered:
                selected = sorted(triggered)[0]
                break
        if selected is None:
            continue
        negative_return, symbol, entry_index, atr = selected
        bars = sessions[day][symbol]
        signal_id = (
            f"{day}-{INDEX_ETF_OPENING_MOMENTUM_FAMILY}-{symbol}"
        )
        if entry_index >= len(bars):
            candidates.append(
                {
                    "signal_id": signal_id,
                    "signal_date": day,
                    "decision_date": day,
                    "symbol": symbol,
                    "outcome": "missed_fill",
                    "rank": 1,
                    "rejection_reason": "missing_next_bar",
                }
            )
            continue
        entry_price = float(bars[entry_index]["open"])
        stop_price = entry_price - stop_atr * atr
        if stop_price <= 0 or stop_price >= entry_price:
            candidates.append(
                {
                    "signal_id": signal_id,
                    "signal_date": day,
                    "decision_date": day,
                    "symbol": symbol,
                    "outcome": "rejected",
                    "rank": 1,
                    "rejection_reason": "invalid_structural_stop",
                }
            )
            continue
        target_price = entry_price + target_r * (
            entry_price - stop_price
        )
        exit_price, stop_executed = _intraday_exit(
            bars,
            entry_index=entry_index,
            stop_price=stop_price,
            target_price=target_price,
        )
        candidates.append(
            {
                "signal_id": signal_id,
                "signal_date": day,
                "decision_date": day,
                "symbol": symbol,
                "outcome": "eligible",
                "rank": 1,
                "score": -negative_return,
                "opening_return": -negative_return,
                "entry_price": entry_price,
                "stop_price": stop_price,
                "target_price": target_price,
                "exit_date": day,
                "exit_price": exit_price,
                "marks": {day: exit_price},
                "stop_executed": stop_executed,
                "planned_stop_distance": entry_price - stop_price,
            }
        )
    return candidates


def _simple_rsi(
    bars: Sequence[Mapping[str, Any]], index: int, period: int
) -> float | None:
    if index < period:
        return None
    changes = [
        float(bars[offset]["close"]) - float(bars[offset - 1]["close"])
        for offset in range(index - period + 1, index + 1)
    ]
    gains = statistics.fmean(max(change, 0.0) for change in changes)
    losses = statistics.fmean(max(-change, 0.0) for change in changes)
    if gains == 0 and losses == 0:
        return 50.0
    if losses == 0:
        return 100.0
    return 100.0 - 100.0 / (1.0 + gains / losses)


def _oversold_feature_cache(
    sessions: Mapping[str, Mapping[str, Sequence[Mapping[str, Any]]]],
) -> dict[str, dict[str, list[dict[str, Any]]]]:
    """Precompute parameter-invariant trigger inputs once for all 32 trials."""

    result: dict[str, dict[str, list[dict[str, Any]]]] = {}
    for day, symbols in sessions.items():
        result[day] = {}
        for symbol, bars in symbols.items():
            if len(bars) != 390:
                raise DenseStrategyRuntimeError(
                    f"oversold input must contain 390 exact bars: {day} {symbol}"
                )
            numerator = 0.0
            denominator = 0.0
            session_low = math.inf
            features: list[dict[str, Any]] = []
            for index, bar in enumerate(bars):
                numerator += float(bar["vwap_numerator"])
                denominator += float(bar["vwap_denominator"])
                session_low = min(session_low, float(bar["low"]))
                if not (
                    OVERSOLD_SIGNAL_START_INDEX
                    <= index
                    <= OVERSOLD_SIGNAL_END_INDEX
                ):
                    continue
                if denominator <= 0:
                    continue
                close = float(bar["close"])
                if (
                    close <= float(bar["open"])
                    or close <= float(bars[index - 1]["high"])
                    or close <= numerator / denominator
                ):
                    continue
                selloffs = {
                    str(lookback): (
                        float(bars[index - 1]["close"])
                        / float(bars[index - lookback]["close"])
                        - 1.0
                    )
                    for lookback in (15, 30)
                }
                rsis = {
                    str(period): _simple_rsi(bars, index - 1, period)
                    for period in (3, 5)
                }
                features.append(
                    {
                        "trigger_index": index,
                        "entry_index": index + 1,
                        "selloff_returns": selloffs,
                        "simple_rsi": rsis,
                        "session_low": session_low,
                    }
                )
            result[day][symbol] = features
    return result


def _oversold_exit(
    bars: Sequence[Mapping[str, Any]],
    *,
    entry_index: int,
    stop_price: float,
    target_price: float,
) -> tuple[float, bool]:
    for index in range(entry_index, len(bars)):
        bar = bars[index]
        opening = float(bar["open"])
        if opening <= stop_price:
            return opening, True
        if index >= OVERSOLD_FORCE_FLAT_INDEX:
            return opening, False
        stop_hit = float(bar["low"]) <= stop_price
        target_hit = float(bar["high"]) >= target_price
        if stop_hit:
            return stop_price, True
        if target_hit:
            return target_price, False
    raise DenseStrategyRuntimeError("oversold trade did not flatten by 15:50")


def _oversold_candidates(
    dataset: Mapping[str, Any], parameters: Mapping[str, Any]
) -> list[dict[str, Any]]:
    calendar = _calendar(dataset)
    sessions = _minute_sessions(dataset)
    cache = dataset.get("_oversold_feature_cache")
    if not isinstance(cache, Mapping):
        cache = _oversold_feature_cache(sessions)
    raw_universe = dataset.get("candidate_symbols_by_date")
    if not isinstance(raw_universe, Mapping):
        raise DenseStrategyRuntimeError(
            "oversold dataset lacks its frozen candidate universe"
        )
    lookback = int(parameters["lookback_minutes"])
    selloff_threshold = float(parameters["selloff_threshold"])
    rsi_period = int(parameters["rsi_period"])
    rsi_maximum = float(parameters["rsi_maximum"])
    target_r = float(parameters["target_r"])
    if (
        lookback not in {15, 30}
        or selloff_threshold not in {-0.02, -0.03}
        or rsi_period not in {3, 5}
        or rsi_maximum not in {15.0, 20.0}
        or target_r not in {1.0, 1.5}
    ):
        raise DenseStrategyRuntimeError("oversold trial parameters escaped the grid")
    candidates: list[dict[str, Any]] = []
    for day in calendar:
        qualified: list[tuple[int, float, float, str, dict[str, Any]]] = []
        for symbol in map(str, raw_universe[day]):
            for feature in cache.get(day, {}).get(symbol, []):
                selloff = float(
                    feature["selloff_returns"][str(lookback)]
                )
                rsi = feature["simple_rsi"][str(rsi_period)]
                if (
                    rsi is None
                    or selloff > selloff_threshold + 1e-12
                    or float(rsi) > rsi_maximum + 1e-12
                ):
                    continue
                qualified.append(
                    (
                        int(feature["entry_index"]),
                        selloff,
                        float(rsi),
                        symbol,
                        dict(feature),
                    )
                )
                break
        if not qualified:
            continue
        entry_index, selloff, rsi, symbol, selected = sorted(qualified)[0]
        signal_id = f"{day}-{OVERSOLD_REVERSAL_FAMILY}-{symbol}"
        bars = sessions.get(day, {}).get(symbol)
        if bars is None or entry_index >= len(bars):
            candidates.append(
                {
                    "signal_id": signal_id,
                    "signal_date": day,
                    "decision_date": day,
                    "symbol": symbol,
                    "outcome": "missed_fill",
                    "rank": 1,
                    "rejection_reason": "missing_next_bar",
                }
            )
            continue
        entry_price = float(bars[entry_index]["open"])
        stop_price = float(selected["session_low"])
        if stop_price <= 0 or stop_price >= entry_price:
            candidates.append(
                {
                    "signal_id": signal_id,
                    "signal_date": day,
                    "decision_date": day,
                    "symbol": symbol,
                    "outcome": "rejected",
                    "rank": 1,
                    "rejection_reason": "invalid_structural_stop",
                }
            )
            continue
        target_price = entry_price + target_r * (entry_price - stop_price)
        expected_gross = (target_price - entry_price) / entry_price
        if not _cost_floor(expected_gross):
            candidates.append(
                {
                    "signal_id": signal_id,
                    "signal_date": day,
                    "decision_date": day,
                    "symbol": symbol,
                    "outcome": "rejected",
                    "rank": 1,
                    "rejection_reason": "expected_move_below_cost_floor",
                }
            )
            continue
        exit_price, stop_executed = _oversold_exit(
            bars,
            entry_index=entry_index,
            stop_price=stop_price,
            target_price=target_price,
        )
        candidates.append(
            {
                "signal_id": signal_id,
                "signal_date": day,
                "decision_date": day,
                "symbol": symbol,
                "outcome": "eligible",
                "rank": 1,
                "score": selloff + rsi / 100.0,
                "selloff_return": selloff,
                "rsi": rsi,
                "trigger_index": int(selected["trigger_index"]),
                "entry_price": entry_price,
                "stop_price": stop_price,
                "target_price": target_price,
                "exit_date": day,
                "exit_price": exit_price,
                "marks": {day: exit_price},
                "stop_executed": stop_executed,
                "planned_stop_distance": entry_price - stop_price,
            }
        )
    return candidates


def _gap_continuation_feature_cache(
    sessions: Mapping[str, Mapping[str, Sequence[Mapping[str, Any]]]],
) -> dict[str, dict[str, dict[str, list[dict[str, Any]]]]]:
    """Precompute completed breakout observations once for all 32 gap trials."""

    result: dict[str, dict[str, dict[str, list[dict[str, Any]]]]] = {}
    for day, symbols in sessions.items():
        result[day] = {}
        for symbol, bars in symbols.items():
            if len(bars) != 390:
                raise DenseStrategyRuntimeError(
                    f"gap-continuation input must contain 390 exact bars: {day} {symbol}"
                )
            cumulative_numerator = 0.0
            cumulative_denominator = 0.0
            vwap_by_index: list[float | None] = []
            for bar in bars:
                cumulative_numerator += float(bar["vwap_numerator"])
                cumulative_denominator += float(bar["vwap_denominator"])
                vwap_by_index.append(
                    cumulative_numerator / cumulative_denominator
                    if cumulative_denominator > 0
                    else None
                )
            range_features: dict[str, list[dict[str, Any]]] = {}
            for opening_range in (5, 15):
                range_high = max(
                    float(bar["high"]) for bar in bars[:opening_range]
                )
                range_low = min(
                    float(bar["low"]) for bar in bars[:opening_range]
                )
                features: list[dict[str, Any]] = []
                for index in range(
                    max(opening_range, GAP_VOLUME_LOOKBACK_BARS),
                    121,
                ):
                    vwap = vwap_by_index[index]
                    if (
                        vwap is None
                        or float(bars[index]["close"]) <= range_high
                        or float(bars[index]["close"]) <= vwap
                    ):
                        continue
                    mean_volume = statistics.fmean(
                        float(bar["volume"])
                        for bar in bars[
                            index - GAP_VOLUME_LOOKBACK_BARS : index
                        ]
                    )
                    volume_multiple = (
                        float(bars[index]["volume"]) / mean_volume
                        if mean_volume > 0
                        else 0.0
                    )
                    features.append(
                        {
                            "trigger_index": index,
                            "entry_index": index + 1,
                            "range_high": range_high,
                            "range_low": range_low,
                            "volume_multiple": volume_multiple,
                        }
                    )
                range_features[str(opening_range)] = features
            result[day][symbol] = range_features
    return result


def _gap_continuation_exit(
    bars: Sequence[Mapping[str, Any]],
    *,
    entry_index: int,
    stop_price: float,
    target_price: float,
) -> tuple[float, bool]:
    for index in range(entry_index, len(bars)):
        bar = bars[index]
        opening = float(bar["open"])
        if opening <= stop_price:
            return opening, True
        if index >= GAP_FORCE_FLAT_INDEX:
            return opening, False
        stop_hit = float(bar["low"]) <= stop_price
        target_hit = float(bar["high"]) >= target_price
        if stop_hit:
            return stop_price, True
        if target_hit:
            return target_price, False
    raise DenseStrategyRuntimeError(
        "gap-continuation trade did not flatten by 15:50"
    )


def _gap_continuation_candidates(
    dataset: Mapping[str, Any], parameters: Mapping[str, Any]
) -> list[dict[str, Any]]:
    calendar = _calendar(dataset)
    sessions = _minute_sessions(dataset)
    cache = dataset.get("_gap_continuation_feature_cache")
    if not isinstance(cache, Mapping):
        cache = _gap_continuation_feature_cache(sessions)
    raw_universe = dataset.get("candidate_symbols_by_date")
    raw_metadata = dataset.get("candidate_metadata_by_date")
    if not isinstance(raw_universe, Mapping) or not isinstance(
        raw_metadata, Mapping
    ):
        raise DenseStrategyRuntimeError(
            "gap-continuation dataset lacks its frozen candidate universe"
        )
    minimum_gap = float(parameters["minimum_gap_fraction"])
    opening_range = int(parameters["opening_range_minutes"])
    volume_threshold = float(parameters["breakout_volume_multiple"])
    signal_cutoff = int(parameters["signal_cutoff_minutes"])
    target_r = float(parameters["target_r"])
    raw_stop_cap = parameters.get("maximum_structural_stop_fraction")
    stop_cap = float(raw_stop_cap) if raw_stop_cap is not None else None
    if (
        minimum_gap not in {0.02, 0.04}
        or opening_range not in {5, 15}
        or volume_threshold not in {1.5, 2.5}
        or signal_cutoff not in {60, 120}
        or target_r not in {1.5, 2.0}
        or (stop_cap is not None and stop_cap not in {0.03, 0.04})
    ):
        raise DenseStrategyRuntimeError(
            "gap-continuation trial parameters escaped the grid"
        )
    candidates: list[dict[str, Any]] = []
    for day in calendar:
        qualified: list[
            tuple[int, float, float, str, dict[str, Any]]
        ] = []
        for symbol in map(str, raw_universe[day]):
            metadata = raw_metadata[day][symbol]
            gap_fraction = float(metadata["gap_fraction"])
            if gap_fraction + 1e-12 < minimum_gap:
                continue
            for feature in (
                cache.get(day, {})
                .get(symbol, {})
                .get(str(opening_range), [])
            ):
                if int(feature["trigger_index"]) > signal_cutoff:
                    break
                volume_multiple = float(feature["volume_multiple"])
                if volume_multiple + 1e-12 < volume_threshold:
                    continue
                qualified.append(
                    (
                        int(feature["entry_index"]),
                        -volume_multiple,
                        -gap_fraction,
                        symbol,
                        dict(feature),
                    )
                )
                break
        if not qualified:
            continue
        (
            entry_index,
            negative_volume,
            negative_gap,
            symbol,
            selected,
        ) = sorted(qualified)[0]
        volume_multiple = -negative_volume
        gap_fraction = -negative_gap
        signal_id = f"{day}-{EQUITY_GAP_CONTINUATION_FAMILY}-{symbol}"
        bars = sessions.get(day, {}).get(symbol)
        if bars is None or entry_index >= len(bars):
            candidates.append(
                {
                    "signal_id": signal_id,
                    "signal_date": day,
                    "decision_date": day,
                    "symbol": symbol,
                    "outcome": "missed_fill",
                    "rank": 1,
                    "rejection_reason": "missing_next_bar",
                }
            )
            continue
        entry_price = float(bars[entry_index]["open"])
        stop_price = float(selected["range_low"])
        if stop_price <= 0 or stop_price >= entry_price:
            candidates.append(
                {
                    "signal_id": signal_id,
                    "signal_date": day,
                    "decision_date": day,
                    "symbol": symbol,
                    "outcome": "rejected",
                    "rank": 1,
                    "rejection_reason": "invalid_structural_stop",
                }
            )
            continue
        stop_fraction = (entry_price - stop_price) / entry_price
        if stop_cap is not None and stop_fraction > stop_cap + 1e-12:
            candidates.append(
                {
                    "signal_id": signal_id,
                    "signal_date": day,
                    "decision_date": day,
                    "symbol": symbol,
                    "outcome": "rejected",
                    "rank": 1,
                    "rejection_reason": "structural_stop_exceeds_protection_cap",
                }
            )
            continue
        target_price = entry_price + target_r * (entry_price - stop_price)
        expected_gross = (target_price - entry_price) / entry_price
        if not _cost_floor(expected_gross):
            candidates.append(
                {
                    "signal_id": signal_id,
                    "signal_date": day,
                    "decision_date": day,
                    "symbol": symbol,
                    "outcome": "rejected",
                    "rank": 1,
                    "rejection_reason": "expected_move_below_cost_floor",
                }
            )
            continue
        exit_price, stop_executed = _gap_continuation_exit(
            bars,
            entry_index=entry_index,
            stop_price=stop_price,
            target_price=target_price,
        )
        candidates.append(
            {
                "signal_id": signal_id,
                "signal_date": day,
                "decision_date": day,
                "symbol": symbol,
                "outcome": "eligible",
                "rank": 1,
                "score": volume_multiple + gap_fraction,
                "gap_fraction": gap_fraction,
                "volume_multiple": volume_multiple,
                "trigger_index": int(selected["trigger_index"]),
                "entry_price": entry_price,
                "stop_price": stop_price,
                "target_price": target_price,
                "exit_date": day,
                "exit_price": exit_price,
                "marks": {day: exit_price},
                "stop_executed": stop_executed,
                "planned_stop_distance": entry_price - stop_price,
            }
        )
    return candidates


def _compression_feature_cache(
    sessions: Mapping[str, Mapping[str, Sequence[Mapping[str, Any]]]],
) -> dict[str, dict[str, dict[str, list[dict[str, Any]]]]]:
    """Precompute the union of observations usable by the frozen 32-trial grid."""

    result: dict[str, dict[str, dict[str, list[dict[str, Any]]]]] = {}
    for day, symbols in sessions.items():
        result[day] = {}
        for symbol, bars in symbols.items():
            if len(bars) != 390:
                raise DenseStrategyRuntimeError(
                    f"compression input must contain 390 exact bars: {day} {symbol}"
                )
            first_thirty_high = max(float(bar["high"]) for bar in bars[:30])
            first_thirty_low = min(float(bar["low"]) for bar in bars[:30])
            first_thirty_range = first_thirty_high - first_thirty_low
            by_window: dict[str, list[dict[str, Any]]] = {
                "10": [],
                "20": [],
            }
            if first_thirty_range <= 0:
                result[day][symbol] = by_window
                continue
            numerator = 0.0
            denominator = 0.0
            vwap_by_index: list[float | None] = []
            for bar in bars:
                numerator += float(bar["vwap_numerator"])
                denominator += float(bar["vwap_denominator"])
                vwap_by_index.append(
                    numerator / denominator if denominator > 0 else None
                )
            for compression_bars in (10, 20):
                features = by_window[str(compression_bars)]
                for index in range(
                    COMPRESSION_SIGNAL_START_INDEX,
                    301,
                ):
                    window = bars[index - compression_bars : index]
                    compression_high = max(
                        float(bar["high"]) for bar in window
                    )
                    compression_low = min(
                        float(bar["low"]) for bar in window
                    )
                    compression_ratio = (
                        compression_high - compression_low
                    ) / first_thirty_range
                    mean_volume = statistics.fmean(
                        float(bar["volume"]) for bar in window
                    )
                    volume_multiple = (
                        float(bars[index]["volume"]) / mean_volume
                        if mean_volume > 0
                        else 0.0
                    )
                    vwap = vwap_by_index[index]
                    close = float(bars[index]["close"])
                    if (
                        vwap is None
                        or compression_ratio > 0.6 + 1e-12
                        or volume_multiple + 1e-12 < 1.5
                        or close <= compression_high
                        or close <= vwap
                    ):
                        continue
                    features.append(
                        {
                            "trigger_index": index,
                            "entry_index": index + 1,
                            "compression_high": compression_high,
                            "compression_low": compression_low,
                            "compression_ratio": compression_ratio,
                            "volume_multiple": volume_multiple,
                        }
                    )
            result[day][symbol] = by_window
    return result


def _compression_candidates(
    dataset: Mapping[str, Any], parameters: Mapping[str, Any]
) -> list[dict[str, Any]]:
    calendar = _calendar(dataset)
    sessions = _minute_sessions(dataset)
    cache = dataset.get("_compression_feature_cache")
    if not isinstance(cache, Mapping):
        cache = _compression_feature_cache(sessions)
    raw_universe = dataset.get("candidate_symbols_by_date")
    if not isinstance(raw_universe, Mapping):
        raise DenseStrategyRuntimeError(
            "compression dataset lacks its frozen candidate universe"
        )
    compression_bars = int(parameters["compression_bars"])
    maximum_ratio = float(parameters["maximum_compression_ratio"])
    volume_threshold = float(parameters["breakout_volume_multiple"])
    signal_cutoff = int(parameters["signal_cutoff_minutes"])
    target_r = float(parameters["target_r"])
    if (
        compression_bars not in {10, 20}
        or maximum_ratio not in {0.4, 0.6}
        or volume_threshold not in {1.5, 2.5}
        or signal_cutoff not in {120, 300}
        or target_r not in {1.5, 2.0}
    ):
        raise DenseStrategyRuntimeError(
            "compression trial parameters escaped the grid"
        )
    candidates: list[dict[str, Any]] = []
    for day in calendar:
        qualified: list[
            tuple[int, float, float, str, dict[str, Any]]
        ] = []
        for symbol in map(str, raw_universe[day]):
            for feature in (
                cache.get(day, {})
                .get(symbol, {})
                .get(str(compression_bars), [])
            ):
                if int(feature["trigger_index"]) > signal_cutoff:
                    break
                ratio = float(feature["compression_ratio"])
                volume_multiple = float(feature["volume_multiple"])
                if (
                    ratio > maximum_ratio + 1e-12
                    or volume_multiple + 1e-12 < volume_threshold
                ):
                    continue
                qualified.append(
                    (
                        int(feature["entry_index"]),
                        ratio,
                        -volume_multiple,
                        symbol,
                        dict(feature),
                    )
                )
                break
        if not qualified:
            continue
        (
            entry_index,
            compression_ratio,
            negative_volume,
            symbol,
            selected,
        ) = sorted(qualified)[0]
        volume_multiple = -negative_volume
        signal_id = f"{day}-{VOLATILITY_COMPRESSION_FAMILY}-{symbol}"
        bars = sessions.get(day, {}).get(symbol)
        if bars is None or entry_index >= len(bars):
            candidates.append(
                {
                    "signal_id": signal_id,
                    "signal_date": day,
                    "decision_date": day,
                    "symbol": symbol,
                    "outcome": "missed_fill",
                    "rank": 1,
                    "rejection_reason": "missing_next_bar",
                }
            )
            continue
        entry_price = float(bars[entry_index]["open"])
        stop_price = float(selected["compression_low"])
        if stop_price <= 0 or stop_price >= entry_price:
            candidates.append(
                {
                    "signal_id": signal_id,
                    "signal_date": day,
                    "decision_date": day,
                    "symbol": symbol,
                    "outcome": "rejected",
                    "rank": 1,
                    "rejection_reason": "invalid_structural_stop",
                }
            )
            continue
        target_price = entry_price + target_r * (entry_price - stop_price)
        expected_gross = (target_price - entry_price) / entry_price
        if not _cost_floor(expected_gross):
            candidates.append(
                {
                    "signal_id": signal_id,
                    "signal_date": day,
                    "decision_date": day,
                    "symbol": symbol,
                    "outcome": "rejected",
                    "rank": 1,
                    "rejection_reason": "expected_move_below_cost_floor",
                }
            )
            continue
        exit_price, stop_executed = _gap_continuation_exit(
            bars,
            entry_index=entry_index,
            stop_price=stop_price,
            target_price=target_price,
        )
        candidates.append(
            {
                "signal_id": signal_id,
                "signal_date": day,
                "decision_date": day,
                "symbol": symbol,
                "outcome": "eligible",
                "rank": 1,
                "score": -compression_ratio + volume_multiple / 100.0,
                "compression_ratio": compression_ratio,
                "volume_multiple": volume_multiple,
                "trigger_index": int(selected["trigger_index"]),
                "entry_price": entry_price,
                "stop_price": stop_price,
                "target_price": target_price,
                "exit_date": day,
                "exit_price": exit_price,
                "marks": {day: exit_price},
                "stop_executed": stop_executed,
                "planned_stop_distance": entry_price - stop_price,
            }
        )
    return candidates


def _earnings_pead_exit(
    bars: Sequence[Mapping[str, Any]],
    *,
    entry_index: int,
    stop_price: float,
    hold_sessions: int,
) -> tuple[str, float, bool, dict[str, float]]:
    marks: dict[str, float] = {}
    final_index = entry_index + hold_sessions - 1
    if final_index >= len(bars):
        raise DenseStrategyRuntimeError(
            "earnings PEAD holding window is incomplete"
        )
    for index in range(entry_index, final_index + 1):
        bar = bars[index]
        day = str(bar["date"])
        if float(bar["low"]) <= stop_price:
            exit_price = min(float(bar["open"]), stop_price)
            marks[day] = exit_price
            return day, exit_price, True, marks
        marks[day] = float(bar["close"])
    final = bars[final_index]
    return (
        str(final["date"]),
        float(final["close"]),
        False,
        marks,
    )


def _earnings_pead_candidates(
    dataset: Mapping[str, Any],
    parameters: Mapping[str, Any],
) -> list[dict[str, Any]]:
    calendar = _calendar(dataset)
    raw_metadata = dataset.get("event_metadata_by_date")
    if not isinstance(raw_metadata, Mapping) or set(raw_metadata) != set(
        calendar
    ):
        raise DenseStrategyRuntimeError(
            "earnings PEAD event metadata must bind every account date"
        )
    daily = _daily_series(dataset)
    if "SPY" not in daily:
        raise DenseStrategyRuntimeError(
            "earnings PEAD data requires SPY trend history"
        )
    indices = {
        symbol: {str(bar["date"]): index for index, bar in enumerate(bars)}
        for symbol, bars in daily.items()
    }
    minimum_surprise = float(parameters["minimum_surprise_ratio"])
    minimum_gap = float(parameters["minimum_opening_gap_fraction"])
    trend_gate = str(parameters["market_trend_gate"])
    stop_atr = float(parameters["stop_atr14"])
    hold_sessions = int(parameters["maximum_hold_sessions"])
    if (
        minimum_surprise not in {0.0, 0.25}
        or minimum_gap not in {-0.02, 0.0}
        or trend_gate not in {"SPY>SMA100", "SPY>SMA200"}
        or stop_atr not in {1.0, 1.5}
        or hold_sessions not in {2, 5}
    ):
        raise DenseStrategyRuntimeError(
            "earnings PEAD trial parameters escaped the grid"
        )
    trend_sessions = 100 if trend_gate.endswith("100") else 200
    candidates: list[dict[str, Any]] = []
    spy = daily["SPY"]
    spy_indices = indices["SPY"]
    for day in calendar:
        rows = raw_metadata[day]
        if not isinstance(rows, list):
            raise DenseStrategyRuntimeError(
                f"earnings PEAD metadata is invalid for {day}"
            )
        spy_index = spy_indices.get(day)
        if spy_index is None or spy_index + 1 < trend_sessions:
            continue
        trend = _sma(spy, spy_index, trend_sessions)
        if trend is None or float(spy[spy_index]["close"]) <= trend:
            continue
        qualified: list[tuple[float, float, str, dict[str, Any]]] = []
        for raw in rows:
            if not isinstance(raw, Mapping):
                raise DenseStrategyRuntimeError(
                    f"earnings PEAD metadata row is invalid for {day}"
                )
            symbol = str(raw.get("symbol", ""))
            bars = daily.get(symbol)
            symbol_index = indices.get(symbol, {}).get(day)
            if bars is None or symbol_index is None or symbol_index < 20:
                continue
            actual = float(raw["actual_eps"])
            estimate = float(raw["estimated_eps"])
            surprise = (actual - estimate) / max(abs(estimate), 0.10)
            if surprise + 1e-12 < minimum_surprise:
                continue
            prior_close = float(bars[symbol_index - 1]["close"])
            entry_price = float(bars[symbol_index]["open"])
            gap = entry_price / prior_close - 1
            if gap + 1e-12 < minimum_gap:
                continue
            dollar_volume = statistics.median(
                float(bar["close"]) * float(bar["volume"])
                for bar in bars[symbol_index - 20 : symbol_index]
            )
            if prior_close < 10 or dollar_volume < 50_000_000:
                continue
            atr14 = _atr(bars, symbol_index - 1)
            if (
                atr14 is None
                or atr14 / entry_price
                < MINIMUM_GROSS_TO_COST_MULTIPLE
                * PRIMARY_ROUND_TRIP_COST_FRACTION
            ):
                continue
            qualified.append(
                (-surprise, -dollar_volume, symbol, dict(raw))
            )
        if not qualified:
            continue
        negative_surprise, negative_liquidity, symbol, selected = sorted(
            qualified
        )[0]
        bars = daily[symbol]
        entry_index = indices[symbol][day]
        entry_price = float(bars[entry_index]["open"])
        atr14 = _atr(bars, entry_index - 1)
        if atr14 is None:
            continue
        stop_price = entry_price - stop_atr * atr14
        signal_id = f"{day}-{EARNINGS_PEAD_FAMILY}-{symbol}"
        if stop_price <= 0:
            candidates.append(
                {
                    "signal_id": signal_id,
                    "signal_date": day,
                    "decision_date": day,
                    "symbol": symbol,
                    "outcome": "rejected",
                    "rank": 1,
                    "rejection_reason": "invalid_structural_stop",
                }
            )
            continue
        exit_date, exit_price, stop_executed, marks = (
            _earnings_pead_exit(
                bars,
                entry_index=entry_index,
                stop_price=stop_price,
                hold_sessions=hold_sessions,
            )
        )
        candidates.append(
            {
                "signal_id": signal_id,
                "signal_date": day,
                "decision_date": day,
                "symbol": symbol,
                "outcome": "eligible",
                "rank": 1,
                "score": -negative_surprise,
                "surprise_ratio": -negative_surprise,
                "prior_median_dollar_volume": -negative_liquidity,
                "report_date": selected["report_date"],
                "report_timing": selected["timing"],
                "entry_price": entry_price,
                "stop_price": stop_price,
                "exit_date": exit_date,
                "exit_price": exit_price,
                "marks": marks,
                "stop_executed": stop_executed,
                "planned_stop_distance": entry_price - stop_price,
            }
        )
    return candidates


def _earnings_sec_reaction_candidates(
    dataset: Mapping[str, Any],
    parameters: Mapping[str, Any],
) -> list[dict[str, Any]]:
    """Evaluate a next-open trade after a completed SEC reaction session."""

    calendar = _calendar(dataset)
    raw_metadata = dataset.get("event_metadata_by_date")
    if not isinstance(raw_metadata, Mapping) or set(raw_metadata) != set(
        calendar
    ):
        raise DenseStrategyRuntimeError(
            "SEC earnings metadata must bind every account date"
        )
    daily = _daily_series(dataset)
    indices = {
        symbol: {str(bar["date"]): index for index, bar in enumerate(bars)}
        for symbol, bars in daily.items()
    }
    minimum_eps_change = float(
        parameters["minimum_yoy_eps_change_ratio"]
    )
    minimum_gap = float(
        parameters["minimum_reaction_opening_gap_fraction"]
    )
    confirmation = str(parameters["reaction_confirmation"])
    stop_atr = float(parameters["stop_atr14"])
    hold_sessions = int(parameters["maximum_hold_sessions"])
    if (
        minimum_eps_change not in {0.25, 0.50}
        or minimum_gap not in {0.0, 0.01}
        or confirmation not in {"close>open", "close>prior_close"}
        or stop_atr not in {1.0, 1.5}
        or hold_sessions not in {2, 5}
    ):
        raise DenseStrategyRuntimeError(
            "SEC earnings trial parameters escaped the frozen grid"
        )
    candidates: list[dict[str, Any]] = []
    for calendar_index, decision_date in enumerate(calendar[:-1]):
        rows = raw_metadata[decision_date]
        if not isinstance(rows, list):
            raise DenseStrategyRuntimeError(
                f"SEC earnings metadata is invalid for {decision_date}"
            )
        qualified: list[
            tuple[float, float, str, dict[str, Any], float]
        ] = []
        for raw in rows:
            if not isinstance(raw, Mapping):
                raise DenseStrategyRuntimeError(
                    f"SEC earnings row is invalid for {decision_date}"
                )
            symbol = str(raw.get("symbol", ""))
            bars = daily.get(symbol)
            reaction_index = indices.get(symbol, {}).get(decision_date)
            if bars is None or reaction_index is None or reaction_index < 20:
                continue
            if (
                raw.get("security_identity_state")
                != "VERIFIED_COMMON_EQUITY_COVER_FACT"
                or raw.get("reaction_date") != decision_date
            ):
                raise DenseStrategyRuntimeError(
                    "SEC earnings identity or reaction date drifted"
                )
            eps_change = float(raw["eps_change_ratio"])
            if eps_change + 1e-12 < minimum_eps_change:
                continue
            prior_close = float(bars[reaction_index - 1]["close"])
            reaction = bars[reaction_index]
            opening = float(reaction["open"])
            close = float(reaction["close"])
            gap = opening / prior_close - 1
            if gap + 1e-12 < minimum_gap:
                continue
            if (
                confirmation == "close>open" and close <= opening
            ) or (
                confirmation == "close>prior_close"
                and close <= prior_close
            ):
                continue
            prior_dollar_volume = statistics.median(
                float(bar["close"]) * float(bar["volume"])
                for bar in bars[reaction_index - 20 : reaction_index]
            )
            if prior_close < 10 or prior_dollar_volume < 50_000_000:
                continue
            atr14 = _atr(bars, reaction_index)
            if atr14 is None:
                continue
            reaction_dollar_volume = close * float(reaction["volume"])
            qualified.append(
                (
                    -eps_change,
                    -reaction_dollar_volume,
                    symbol,
                    dict(raw),
                    atr14,
                )
            )
        if not qualified:
            continue
        (
            negative_eps_change,
            negative_reaction_liquidity,
            symbol,
            selected,
            atr14,
        ) = sorted(qualified)[0]
        bars = daily[symbol]
        entry_date = calendar[calendar_index + 1]
        entry_index = indices[symbol].get(entry_date)
        signal_id = (
            f"{entry_date}-{EARNINGS_SEC_REACTION_FAMILY}-{symbol}"
        )
        if entry_index is None:
            candidates.append(
                {
                    "signal_id": signal_id,
                    "signal_date": entry_date,
                    "decision_date": decision_date,
                    "symbol": symbol,
                    "outcome": "missed_fill",
                    "rank": 1,
                    "rejection_reason": "missing_next_open",
                }
            )
            continue
        expected_dates = calendar[
            calendar_index + 1 : calendar_index + 1 + hold_sessions
        ]
        observed_dates = [
            str(bar["date"])
            for bar in bars[entry_index : entry_index + hold_sessions]
        ]
        if (
            len(expected_dates) != hold_sessions
            or observed_dates != expected_dates
        ):
            candidates.append(
                {
                    "signal_id": signal_id,
                    "signal_date": entry_date,
                    "decision_date": decision_date,
                    "symbol": symbol,
                    "outcome": "missed_fill",
                    "rank": 1,
                    "rejection_reason": "incomplete_holding_bars",
                }
            )
            continue
        entry_price = float(bars[entry_index]["open"])
        expected_gross = atr14 / entry_price
        if not _cost_floor(expected_gross):
            candidates.append(
                {
                    "signal_id": signal_id,
                    "signal_date": entry_date,
                    "decision_date": decision_date,
                    "symbol": symbol,
                    "outcome": "rejected",
                    "rank": 1,
                    "rejection_reason": "expected_move_below_cost_floor",
                    "expected_gross_move_fraction": expected_gross,
                }
            )
            continue
        stop_price = entry_price - stop_atr * atr14
        if stop_price <= 0 or stop_price >= entry_price:
            candidates.append(
                {
                    "signal_id": signal_id,
                    "signal_date": entry_date,
                    "decision_date": decision_date,
                    "symbol": symbol,
                    "outcome": "rejected",
                    "rank": 1,
                    "rejection_reason": "invalid_structural_stop",
                }
            )
            continue
        exit_date, exit_price, stop_executed, marks = _earnings_pead_exit(
            bars,
            entry_index=entry_index,
            stop_price=stop_price,
            hold_sessions=hold_sessions,
        )
        candidates.append(
            {
                "signal_id": signal_id,
                "signal_date": entry_date,
                "decision_date": decision_date,
                "symbol": symbol,
                "outcome": "eligible",
                "rank": 1,
                "score": -negative_eps_change,
                "eps_change_ratio": -negative_eps_change,
                "reaction_dollar_volume": -negative_reaction_liquidity,
                "accepted": selected["accepted"],
                "report_period": selected["report_period"],
                "entry_price": entry_price,
                "stop_price": stop_price,
                "exit_date": exit_date,
                "exit_price": exit_price,
                "marks": marks,
                "stop_executed": stop_executed,
                "planned_stop_distance": entry_price - stop_price,
                "expected_gross_move_fraction": expected_gross,
            }
        )
    return candidates


def build_candidates(
    dataset: Mapping[str, Any],
    family_id: str,
    parameters: Mapping[str, Any],
) -> list[dict[str, Any]]:
    """Build exact-rule candidates using only information observable at decision time."""

    if dataset.get("family_id") != family_id:
        raise DenseStrategyRuntimeError("dataset family binding does not match")
    if family_id in {
        ETF_RESIDUAL_REPLICATION_FAMILY,
        ETF_RESIDUAL_REPLICATION_V2_FAMILY,
        ETF_RESIDUAL_REPLICATION_V3_FAMILY,
    }:
        return _fixed_etf_residual_candidates(dataset, parameters)
    if family_id in EQUITY_RESIDUAL_FAMILIES:
        return _equity_residual_candidates(dataset, parameters)
    if family_id == INDEX_ETF_OPENING_MOMENTUM_FAMILY:
        return _intraday_momentum_candidates(dataset, parameters)
    if family_id in INTRADAY_ETF_FAMILIES:
        return _intraday_candidates(
            dataset, parameters, family_id=family_id
        )
    if family_id in ETF_PULLBACK_FAMILIES:
        return _etf_pullback_candidates(
            dataset,
            parameters,
            family_id=family_id,
        )
    if family_id == SPY_RSI2_PULLBACK_FAMILY:
        return _spy_rsi2_pullback_candidates(dataset, parameters)
    if family_id == SECTOR_ETF_GAP_DRIFT_FAMILY:
        return _sector_etf_gap_drift_candidates(dataset, parameters)
    if family_id == FLIGHT_TO_SAFETY_REBOUND_FAMILY:
        return _flight_to_safety_rebound_candidates(dataset, parameters)
    if family_id == FLIGHT_TO_SAFETY_REPLICATION_FAMILY:
        return _flight_to_safety_rebound_candidates(
            dataset,
            parameters,
            family_id=family_id,
            target_symbols=FLIGHT_TO_SAFETY_REPLICATION_TARGET_SYMBOLS,
        )
    if family_id == FLIGHT_TO_SAFETY_REPLICATION_V2_FAMILY:
        return _flight_to_safety_rebound_candidates(
            dataset,
            parameters,
            family_id=family_id,
            target_symbols=FLIGHT_TO_SAFETY_REPLICATION_V2_TARGET_SYMBOLS,
        )
    if family_id == BREADTH_CAPITULATION_REBOUND_FAMILY:
        return _breadth_capitulation_rebound_candidates(dataset, parameters)
    if family_id == CROSS_STYLE_BREADTH_CONTINUATION_FAMILY:
        return _cross_style_breadth_continuation_candidates(
            dataset, parameters
        )
    if family_id == STYLE_ETF_BREAKOUT_CONTINUATION_FAMILY:
        return _style_etf_breakout_continuation_candidates(
            dataset, parameters
        )
    if family_id in ETF_OVERSOLD_FAMILIES:
        return _high_beta_etf_oversold_candidates(
            dataset, parameters, family_id=family_id
        )
    if family_id == ETF_CROSS_SECTIONAL_MOMENTUM_FAMILY:
        return _etf_cross_sectional_momentum_candidates(dataset, parameters)
    if family_id == LIQUID_EQUITY_MOMENTUM_FAMILY:
        return _liquid_equity_momentum_candidates(dataset, parameters)
    if family_id == ETF_CROSS_SECTIONAL_REVERSAL_FAMILY:
        return _etf_cross_sectional_reversal_candidates(dataset, parameters)
    if family_id == ETF_HIGH_CONTINUATION_FAMILY:
        return _etf_high_continuation_candidates(dataset, parameters)
    if family_id == ETF_TURN_OF_MONTH_FAMILY:
        return _turn_of_month_candidates(dataset, parameters)
    if family_id == SECTOR_ETF_ROTATION_FAMILY:
        return _sector_rotation_candidates(dataset, parameters)
    if family_id == ETF_CLOSE_TO_OPEN_FAMILY:
        return _close_to_open_candidates(dataset, parameters)
    if family_id == OVERSOLD_REVERSAL_FAMILY:
        return _oversold_candidates(dataset, parameters)
    if family_id == EQUITY_GAP_CONTINUATION_FAMILY:
        return _gap_continuation_candidates(dataset, parameters)
    if family_id == EARNINGS_PEAD_FAMILY:
        return _earnings_pead_candidates(dataset, parameters)
    if family_id == EARNINGS_SEC_REACTION_FAMILY:
        return _earnings_sec_reaction_candidates(dataset, parameters)
    if family_id == VOLATILITY_COMPRESSION_FAMILY:
        return _compression_candidates(dataset, parameters)
    raise DenseStrategyRuntimeError(f"unsupported dense family: {family_id}")


def _production_equity_universe(
    daily: Mapping[str, Sequence[Mapping[str, Any]]],
    references: Any,
    decision_date: str,
    calendar_dates: Sequence[str],
    *,
    excluded_symbols: Sequence[str] = (),
) -> list[str]:
    if not isinstance(references, Mapping):
        raise DenseStrategyRuntimeError(
            "equity production data needs a point-in-time reference snapshot"
        )
    if len(references) < 500:
        raise DenseStrategyRuntimeError(
            "point-in-time common-stock reference snapshot is implausibly small"
        )
    candidates: list[tuple[float, str, str]] = []
    excluded = set(map(str, excluded_symbols))
    for raw_symbol, raw_reference in references.items():
        symbol = str(raw_symbol)
        if (
            symbol == "SPY"
            or symbol in excluded
            or not isinstance(raw_reference, Mapping)
        ):
            continue
        if set(raw_reference) != {"active", "type", "listing_identity"}:
            raise DenseStrategyRuntimeError(
                "point-in-time reference fields are incomplete"
            )
        identity = raw_reference["listing_identity"]
        if (
            raw_reference["active"] is not True
            or raw_reference["type"] != "CS"
            or not isinstance(identity, str)
            or not identity
        ):
            continue
        bars = daily.get(symbol)
        if bars is None:
            raise DenseStrategyRuntimeError(
                f"point-in-time common stock {symbol} lacks daily history"
            )
        through_decision = [bar for bar in bars if str(bar["date"]) <= decision_date]
        if (
            len(through_decision) < 60
            or [str(bar["date"]) for bar in through_decision[-60:]]
            != list(calendar_dates[-60:])
        ):
            continue
        history = through_decision[-60:]
        dollar = [float(bar["close"]) * float(bar["volume"]) for bar in history]
        if float(history[-1]["close"]) < 10 or statistics.median(dollar[-20:]) < 50_000_000:
            continue
        candidates.append((statistics.median(dollar), symbol, identity))
    selected = sorted(candidates, key=lambda item: (-item[0], item[1]))[:250]
    if len(selected) != 250:
        raise DenseStrategyRuntimeError(
            "production equity universe does not contain 250 qualified common stocks"
        )
    identities = [identity for _liquidity, _symbol, identity in selected]
    if len(identities) != len(set(identities)):
        raise DenseStrategyRuntimeError(
            "production equity universe contains duplicate listing identities"
        )
    return [symbol for _liquidity, symbol, _identity in selected]


def _production_daily_signal(
    decision_data: Mapping[str, Any],
    family_id: str,
    parameters: Mapping[str, Any],
    frozen_universe: Mapping[str, Any],
) -> dict[str, Any]:
    expected = {
        "family_id",
        "decision_date",
        "next_session_date",
        "calendar_dates",
        "daily_history_complete",
        "daily_bars",
    }
    if family_id in {
        *EQUITY_RESIDUAL_FAMILIES,
        LIQUID_EQUITY_MOMENTUM_FAMILY,
    }:
        expected.update(
            {
                "reference_snapshot",
                "reference_snapshot_complete",
                "reference_source_total",
            }
        )
        if family_id == LIQUID_EQUITY_MOMENTUM_FAMILY:
            expected.update(
                {
                    "corporate_actions_complete",
                    "recent_split_symbols",
                }
            )
    else:
        expected.add("symbols")
    if family_id == ETF_TURN_OF_MONTH_FAMILY:
        expected.add("exchange_calendar_dates")
    if set(decision_data) != expected or decision_data.get("family_id") != family_id:
        raise DenseStrategyRuntimeError("production daily decision-data schema drifted")
    decision_date = decision_data.get("decision_date")
    if not isinstance(decision_date, str) or not decision_date:
        raise DenseStrategyRuntimeError("production decision_date is missing")
    next_session_date = decision_data.get("next_session_date")
    calendar_dates = decision_data.get("calendar_dates")
    if (
        not isinstance(next_session_date, str)
        or not isinstance(calendar_dates, list)
        or not calendar_dates
        or any(not isinstance(item, str) or not item for item in calendar_dates)
        or calendar_dates != sorted(calendar_dates)
        or len(calendar_dates) != len(set(calendar_dates))
        or calendar_dates[-1] != decision_date
        or decision_data.get("daily_history_complete") is not True
    ):
        raise DenseStrategyRuntimeError(
            "production daily calendar/history completeness is invalid"
        )
    try:
        decision_day = date.fromisoformat(decision_date)
        next_session_day = date.fromisoformat(next_session_date)
    except ValueError as exc:
        raise DenseStrategyRuntimeError(
            "production daily calendar dates are invalid"
        ) from exc
    if not 1 <= (next_session_day - decision_day).days <= 4:
        raise DenseStrategyRuntimeError(
            "production next session is not chronologically adjacent"
        )
    daily = _daily_series(decision_data)
    if any(
        str(bar["date"]) > decision_date
        for bars in daily.values()
        for bar in bars
    ):
        raise DenseStrategyRuntimeError(
            "production daily history contains post-decision bars"
        )
    indices = {
        symbol: {str(bar["date"]): index for index, bar in enumerate(bars)}
        for symbol, bars in daily.items()
    }
    if family_id == LIQUID_EQUITY_MOMENTUM_FAMILY:
        references = decision_data["reference_snapshot"]
        recent_splits = decision_data["recent_split_symbols"]
        expected_universe = {
            "point_in_time": True,
            "security_type": "CS",
            "minimum_prior_close": 10.0,
            "minimum_median_20_session_dollar_volume": 50_000_000.0,
            "liquidity_ranking_sessions": 60,
            "maximum_names": 250,
            "split_affected_windows": "excluded",
        }
        if frozen_universe != expected_universe:
            raise DenseStrategyRuntimeError(
                "production liquid-equity universe rules drifted"
            )
        if (
            not isinstance(references, Mapping)
            or decision_data.get("reference_snapshot_complete") is not True
            or isinstance(decision_data.get("reference_source_total"), bool)
            or decision_data.get("reference_source_total") != len(references)
            or decision_data.get("corporate_actions_complete") is not True
            or not isinstance(recent_splits, list)
            or recent_splits != sorted(set(map(str, recent_splits)))
        ):
            raise DenseStrategyRuntimeError(
                "production liquid-equity reference or split snapshot is incomplete"
            )
        lookback = int(parameters["return_lookback_sessions"])
        trend_period = int(parameters["trend_sma_sessions"])
        excess_floor = float(parameters["minimum_excess_return_fraction"])
        stop_atr = float(parameters["stop_atr14"])
        hold = int(parameters["maximum_hold_sessions"])
        if (
            lookback not in {20, 60}
            or trend_period not in {50, 100}
            or excess_floor not in {0.01, 0.02}
            or stop_atr not in {1.5, 2.0}
            or hold not in {3, 5}
        ):
            raise DenseStrategyRuntimeError(
                "production liquid-equity parameters escaped the frozen grid"
            )
        selected = _production_equity_universe(
            daily,
            references,
            decision_date,
            calendar_dates,
            excluded_symbols=recent_splits,
        )
        features: list[tuple[float, str, float]] = []
        for symbol in selected:
            bars = daily[symbol]
            symbol_index = indices[symbol].get(decision_date)
            if symbol_index is None or symbol_index < max(
                lookback, trend_period - 1
            ):
                raise DenseStrategyRuntimeError(
                    "production liquid-equity momentum history is incomplete"
                )
            trend = _sma(bars, symbol_index, trend_period)
            atr14 = _atr(bars, symbol_index)
            if (
                trend is None
                or atr14 is None
                or float(bars[symbol_index]["close"]) <= trend
            ):
                continue
            trailing_return = (
                float(bars[symbol_index]["close"])
                / float(bars[symbol_index - lookback]["close"])
                - 1
            )
            features.append((trailing_return, symbol, atr14))
        if not features:
            raise DenseStrategyRuntimeError(
                "production liquid-equity trend gates produced no ranks"
            )
        benchmark = statistics.median(item[0] for item in features)
        qualified = [
            (trailing_return, symbol, atr14, trailing_return - benchmark)
            for trailing_return, symbol, atr14 in features
            if trailing_return - benchmark >= excess_floor
            and _cost_floor(trailing_return - benchmark)
        ]
        if not qualified:
            raise DenseStrategyRuntimeError(
                "no exact production liquid-equity momentum signal"
            )
        trailing_return, symbol, atr14, excess_return = sorted(
            qualified,
            key=lambda item: (-item[0], item[1]),
        )[0]
        return {
            "symbol": symbol,
            "rank": 1,
            "score": trailing_return,
            "expected_gross_move_fraction": excess_return,
            "atr": atr14,
            "stop_atr_multiple": stop_atr,
            "holding_trading_days": hold,
            "decision_date": decision_date,
            "next_session_date": next_session_date,
            "exit_plan": {
                "type": "stop_or_maximum_hold_close",
                "maximum_hold_sessions": hold,
                "same_interval_ambiguity": "stop_first",
            },
        }
    if family_id in {
        ETF_RESIDUAL_REPLICATION_FAMILY,
        ETF_RESIDUAL_REPLICATION_V2_FAMILY,
        ETF_RESIDUAL_REPLICATION_V3_FAMILY,
        *ETF_PULLBACK_FAMILIES,
        SPY_RSI2_PULLBACK_FAMILY,
        ETF_CROSS_SECTIONAL_MOMENTUM_FAMILY,
        ETF_CROSS_SECTIONAL_REVERSAL_FAMILY,
        ETF_HIGH_CONTINUATION_FAMILY,
        ETF_TURN_OF_MONTH_FAMILY,
        SECTOR_ETF_ROTATION_FAMILY,
        SECTOR_ETF_GAP_DRIFT_FAMILY,
        FLIGHT_TO_SAFETY_REBOUND_FAMILY,
        FLIGHT_TO_SAFETY_REPLICATION_FAMILY,
        FLIGHT_TO_SAFETY_REPLICATION_V2_FAMILY,
        BREADTH_CAPITULATION_REBOUND_FAMILY,
        CROSS_STYLE_BREADTH_CONTINUATION_FAMILY,
        STYLE_ETF_BREAKOUT_CONTINUATION_FAMILY,
        *ETF_OVERSOLD_FAMILIES,
    }:
        frozen_symbols = frozen_universe.get("symbols")
        observed_symbols = decision_data.get("symbols")
        if (
            not isinstance(frozen_symbols, list)
            or observed_symbols != frozen_symbols
            or set(daily) != set(map(str, frozen_symbols))
        ):
            raise DenseStrategyRuntimeError(
                "production ETF universe drifted from the frozen exact rules"
            )
        if any(
            [str(bar["date"]) for bar in bars] != calendar_dates
            for bars in daily.values()
        ):
            raise DenseStrategyRuntimeError(
                "production ETF history does not cover the complete calendar"
            )
        if (
            family_id == ETF_PULLBACK_REPLICATION_FAMILY
            and frozen_symbols != list(ETF_PULLBACK_REPLICATION_SYMBOLS)
        ):
            raise DenseStrategyRuntimeError(
                "production ETF pullback replication universe escaped the frozen rules"
            )
        if family_id == CROSS_STYLE_BREADTH_CONTINUATION_FAMILY:
            normalized_parameters = {
                "breadth_sma": int(parameters["breadth_sma"]),
                "minimum_breadth_count": int(
                    parameters["minimum_breadth_count"]
                ),
                "target_trend_sma": int(parameters["target_trend_sma"]),
                "stop_atr14": float(parameters["stop_atr14"]),
                "maximum_hold_sessions": int(
                    parameters["maximum_hold_sessions"]
                ),
            }
            expected_parameters = {
                "breadth_sma": 100,
                "minimum_breadth_count": 7,
                "target_trend_sma": 200,
                "stop_atr14": 1.5,
                "maximum_hold_sessions": 5,
            }
            if (
                frozen_symbols != list(CROSS_STYLE_BREADTH_SYMBOLS)
                or normalized_parameters != expected_parameters
            ):
                raise DenseStrategyRuntimeError(
                    "production cross-style breadth rules drifted"
                )
            if (
                decision_day.isocalendar()[:2]
                == next_session_day.isocalendar()[:2]
            ):
                raise DenseStrategyRuntimeError(
                    "no exact production cross-style weekly decision"
                )
            breadth_count = 0
            for symbol in CROSS_STYLE_BREADTH_SYMBOLS:
                bars = daily[symbol]
                symbol_index = indices[symbol].get(decision_date)
                if symbol_index is None or symbol_index < 99:
                    raise DenseStrategyRuntimeError(
                        "production cross-style breadth history is incomplete"
                    )
                breadth = _sma(bars, symbol_index, 100)
                if (
                    breadth is not None
                    and float(bars[symbol_index]["close"]) > breadth
                ):
                    breadth_count += 1
            target_bars = daily[CROSS_STYLE_BREADTH_TARGET_SYMBOL]
            target_index = indices[CROSS_STYLE_BREADTH_TARGET_SYMBOL].get(
                decision_date
            )
            if target_index is None or target_index < 199:
                raise DenseStrategyRuntimeError(
                    "production cross-style target history is incomplete"
                )
            trend = _sma(target_bars, target_index, 200)
            atr14 = _atr(target_bars, target_index)
            decision_close = float(target_bars[target_index]["close"])
            if (
                breadth_count < 7
                or trend is None
                or atr14 is None
                or decision_close <= trend
                or not _cost_floor(atr14 / decision_close)
            ):
                raise DenseStrategyRuntimeError(
                    "no exact production cross-style breadth signal"
                )
            return {
                "symbol": CROSS_STYLE_BREADTH_TARGET_SYMBOL,
                "rank": 1,
                "score": breadth_count + (decision_close / trend - 1),
                "breadth_count": breadth_count,
                "expected_gross_move_fraction": atr14 / decision_close,
                "atr": atr14,
                "stop_atr_multiple": 1.5,
                "holding_trading_days": 5,
                "decision_date": decision_date,
                "next_session_date": next_session_date,
                "overnight_hold": True,
                "exit_plan": {
                    "type": "stop_or_maximum_hold_close",
                    "maximum_hold_sessions": 5,
                    "same_interval_ambiguity": "stop_first",
                },
            }
        if family_id == STYLE_ETF_BREAKOUT_CONTINUATION_FAMILY:
            expected_parameters = {
                "breadth_sma": 100,
                "minimum_breadth_count": 6,
                "breakout_lookback_sessions": 20,
                "trend_sma": 200,
                "stop_atr14": 1.5,
                "maximum_hold_sessions": 5,
            }
            normalized_parameters = {
                "breadth_sma": int(parameters["breadth_sma"]),
                "minimum_breadth_count": int(
                    parameters["minimum_breadth_count"]
                ),
                "breakout_lookback_sessions": int(
                    parameters["breakout_lookback_sessions"]
                ),
                "trend_sma": int(parameters["trend_sma"]),
                "stop_atr14": float(parameters["stop_atr14"]),
                "maximum_hold_sessions": int(
                    parameters["maximum_hold_sessions"]
                ),
            }
            if (
                frozen_symbols != list(CROSS_STYLE_BREADTH_SYMBOLS)
                or normalized_parameters != expected_parameters
            ):
                raise DenseStrategyRuntimeError(
                    "production style ETF breakout rules drifted"
                )
            breadth_count = 0
            ranked: list[tuple[float, str, float, float, float]] = []
            for symbol in CROSS_STYLE_BREADTH_SYMBOLS:
                bars = daily[symbol]
                symbol_index = indices[symbol].get(decision_date)
                if symbol_index is None or symbol_index < 200:
                    raise DenseStrategyRuntimeError(
                        "production style ETF breakout history is incomplete"
                    )
                breadth = _sma(bars, symbol_index, 100)
                if (
                    breadth is not None
                    and float(bars[symbol_index]["close"]) > breadth
                ):
                    breadth_count += 1
                trend = _sma(bars, symbol_index, 200)
                atr14 = _atr(bars, symbol_index)
                decision_close = float(bars[symbol_index]["close"])
                prior_high = max(
                    float(item["close"])
                    for item in bars[symbol_index - 20 : symbol_index]
                )
                trailing_return = (
                    decision_close
                    / float(bars[symbol_index - 20]["close"])
                    - 1
                )
                if (
                    trend is not None
                    and atr14 is not None
                    and decision_close > trend
                    and decision_close > prior_high
                    and _cost_floor(atr14 / decision_close)
                ):
                    ranked.append(
                        (
                            trailing_return,
                            symbol,
                            atr14,
                            decision_close,
                            prior_high,
                        )
                    )
            if breadth_count < 6 or not ranked:
                raise DenseStrategyRuntimeError(
                    "no exact production style ETF breakout signal"
                )
            (
                trailing_return,
                symbol,
                atr14,
                decision_close,
                prior_high,
            ) = sorted(ranked, key=lambda item: (-item[0], item[1]))[0]
            return {
                "symbol": symbol,
                "rank": 1,
                "score": trailing_return,
                "breadth_count": breadth_count,
                "trailing_return_fraction": trailing_return,
                "breakout_reference_price": prior_high,
                "expected_gross_move_fraction": atr14 / decision_close,
                "atr": atr14,
                "stop_atr_multiple": 1.5,
                "holding_trading_days": 5,
                "decision_date": decision_date,
                "next_session_date": next_session_date,
                "overnight_hold": True,
                "exit_plan": {
                    "type": "stop_or_maximum_hold_close",
                    "maximum_hold_sessions": 5,
                    "same_interval_ambiguity": "stop_first",
                },
            }
        if family_id == SPY_RSI2_PULLBACK_FAMILY:
            expected_parameters = {
                "trend_sma": 200,
                "rsi2_maximum": 10.0,
                "mean_reversion_sma": 5,
                "stop_atr14": 1.5,
                "maximum_hold_sessions": 5,
            }
            normalized_parameters = {
                "trend_sma": int(parameters["trend_sma"]),
                "rsi2_maximum": float(parameters["rsi2_maximum"]),
                "mean_reversion_sma": int(parameters["mean_reversion_sma"]),
                "stop_atr14": float(parameters["stop_atr14"]),
                "maximum_hold_sessions": int(parameters["maximum_hold_sessions"]),
            }
            if (
                frozen_symbols != ["SPY"]
                or normalized_parameters != expected_parameters
            ):
                raise DenseStrategyRuntimeError(
                    "production SPY RSI(2) pullback rules drifted"
                )
            bars = daily["SPY"]
            symbol_index = indices["SPY"].get(decision_date)
            if symbol_index is None or symbol_index < 199:
                raise DenseStrategyRuntimeError(
                    "production SPY RSI(2) history is incomplete"
                )
            trend = _sma(bars, symbol_index, 200)
            rsi2 = _rsi_wilder(bars, symbol_index, 2)
            atr14 = _atr(bars, symbol_index)
            reference = _sma(bars, symbol_index, 5)
            decision_close = float(bars[symbol_index]["close"])
            if (
                trend is None
                or rsi2 is None
                or atr14 is None
                or reference is None
                or decision_close <= trend
                or rsi2 > 10.0
            ):
                raise DenseStrategyRuntimeError(
                    "no exact production SPY RSI(2) pullback signal"
                )
            return {
                "symbol": "SPY",
                "rank": 1,
                "score": -rsi2,
                "expected_gross_move_fraction": reference / decision_close - 1,
                "mean_reversion_reference_price": reference,
                "atr": atr14,
                "stop_atr_multiple": 1.5,
                "holding_trading_days": 5,
                "decision_date": decision_date,
                "next_session_date": next_session_date,
                "overnight_hold": True,
                "exit_plan": {
                    "type": "stop_or_completed_sma5_reclaim_or_maximum_hold_close",
                    "mean_reversion_sma": 5,
                    "maximum_hold_sessions": 5,
                    "same_interval_ambiguity": "stop_first",
                },
            }
        if family_id in {
            ETF_RESIDUAL_REPLICATION_FAMILY,
            ETF_RESIDUAL_REPLICATION_V2_FAMILY,
            ETF_RESIDUAL_REPLICATION_V3_FAMILY,
        }:
            target_symbols = {
                ETF_RESIDUAL_REPLICATION_FAMILY: (
                    ETF_RESIDUAL_REPLICATION_TARGET_SYMBOLS
                ),
                ETF_RESIDUAL_REPLICATION_V2_FAMILY: (
                    ETF_RESIDUAL_REPLICATION_V2_TARGET_SYMBOLS
                ),
                ETF_RESIDUAL_REPLICATION_V3_FAMILY: (
                    ETF_RESIDUAL_REPLICATION_V3_TARGET_SYMBOLS
                ),
            }[family_id]
            if frozen_symbols != [
                *target_symbols,
                ETF_RESIDUAL_REPLICATION_FEATURE_SYMBOL,
            ]:
                raise DenseStrategyRuntimeError(
                    "production ETF residual universe escaped the frozen rules"
                )
            window = int(parameters["prior_return_sessions"])
            threshold = float(parameters["residual_z_threshold"])
            trend_period = (
                100
                if parameters["market_trend_gate"] == "SPY>SMA100"
                else 200
            )
            stop_atr = float(parameters["stop_atr14"])
            hold_sessions = int(parameters["hold_sessions"])
            if (
                window not in {1, 3}
                or threshold not in {-1.5, -2.0, -2.5}
                or parameters["market_trend_gate"]
                not in {"SPY>SMA100", "SPY>SMA200"}
                or stop_atr not in {1.0, 1.5}
                or hold_sessions not in {2, 5}
            ):
                raise DenseStrategyRuntimeError(
                    "production ETF residual rules escaped the frozen grid"
                )
            spy_bars = daily[ETF_RESIDUAL_REPLICATION_FEATURE_SYMBOL]
            spy_index = indices[
                ETF_RESIDUAL_REPLICATION_FEATURE_SYMBOL
            ].get(decision_date)
            if spy_index is None or spy_index < max(
                trend_period - 1,
                window,
            ):
                raise DenseStrategyRuntimeError(
                    "production ETF residual market history is incomplete"
                )
            market_trend = _sma(spy_bars, spy_index, trend_period)
            if (
                market_trend is None
                or float(spy_bars[spy_index]["close"]) <= market_trend
            ):
                raise DenseStrategyRuntimeError(
                    "production ETF residual market trend gate is closed"
                )
            spy_return = (
                float(spy_bars[spy_index]["close"])
                / float(spy_bars[spy_index - window]["close"])
                - 1
            )
            qualified: list[tuple[float, str, float, float]] = []
            for symbol in target_symbols:
                bars = daily[symbol]
                symbol_index = indices[symbol].get(decision_date)
                if symbol_index is None or symbol_index < max(
                    STANDARDIZATION_LOOKBACK + window,
                    14,
                ):
                    raise DenseStrategyRuntimeError(
                        "production ETF residual target history is incomplete"
                    )
                residual = (
                    float(bars[symbol_index]["close"])
                    / float(bars[symbol_index - window]["close"])
                    - 1
                    - spy_return
                )
                history: list[float] = []
                for prior_index in range(window, symbol_index):
                    prior_day = str(bars[prior_index]["date"])
                    prior_spy_index = indices[
                        ETF_RESIDUAL_REPLICATION_FEATURE_SYMBOL
                    ].get(prior_day)
                    if (
                        prior_spy_index is None
                        or prior_spy_index < window
                    ):
                        continue
                    history.append(
                        float(bars[prior_index]["close"])
                        / float(bars[prior_index - window]["close"])
                        - float(spy_bars[prior_spy_index]["close"])
                        / float(spy_bars[prior_spy_index - window]["close"])
                    )
                z_score = _z_score(residual, history)
                atr14 = _atr(bars, symbol_index)
                if (
                    z_score is not None
                    and z_score <= threshold
                    and atr14 is not None
                    and _cost_floor(abs(residual))
                ):
                    qualified.append(
                        (z_score, symbol, atr14, residual)
                    )
            if not qualified:
                raise DenseStrategyRuntimeError(
                    "no exact production ETF residual signal"
                )
            z_score, symbol, atr14, residual = sorted(qualified)[0]
            return {
                "symbol": symbol,
                "rank": 1,
                "score": z_score,
                "expected_gross_move_fraction": abs(residual),
                "atr": atr14,
                "stop_atr_multiple": stop_atr,
                "holding_trading_days": hold_sessions,
                "decision_date": decision_date,
                "next_session_date": next_session_date,
                "overnight_hold": True,
                "exit_plan": {
                    "type": "stop_or_maximum_hold_close",
                    "maximum_hold_sessions": hold_sessions,
                    "same_interval_ambiguity": "stop_first",
                },
            }
        if family_id in ETF_OVERSOLD_FAMILIES:
            trend_period = int(parameters["trend_sma"])
            rsi_max = float(parameters["rsi2_maximum"])
            decline_floor = float(
                parameters["one_session_decline_fraction"]
            )
            stop_atr = float(parameters["stop_atr14"])
            hold_sessions = int(parameters["maximum_hold_sessions"])
            if (
                trend_period not in {100, 200}
                or rsi_max not in {5.0, 10.0}
                or decline_floor not in {0.01, 0.02}
                or stop_atr not in {1.0, 1.5}
                or hold_sessions not in {2, 5}
            ):
                raise DenseStrategyRuntimeError(
                    "production high-beta oversold rules escaped the frozen grid"
                )
            qualified: list[tuple[float, float, str, float]] = []
            for symbol in map(str, frozen_symbols):
                bars = daily[symbol]
                symbol_index = indices[symbol].get(decision_date)
                if symbol_index is None or symbol_index < max(
                    trend_period - 1, 14, 2
                ):
                    raise DenseStrategyRuntimeError(
                        "production high-beta oversold history is incomplete"
                    )
                trend = _sma(bars, symbol_index, trend_period)
                rsi2 = _rsi_wilder(bars, symbol_index, 2)
                atr14 = _atr(bars, symbol_index)
                decline = (
                    float(bars[symbol_index]["close"])
                    / float(bars[symbol_index - 1]["close"])
                    - 1
                )
                if (
                    trend is not None
                    and rsi2 is not None
                    and atr14 is not None
                    and float(bars[symbol_index]["close"]) > trend
                    and rsi2 <= rsi_max
                    and decline <= -decline_floor
                    and _cost_floor(abs(decline))
                ):
                    qualified.append((rsi2, decline, symbol, atr14))
            if not qualified:
                raise DenseStrategyRuntimeError(
                    "no exact production high-beta oversold signal"
                )
            rsi2, decline, symbol, atr14 = sorted(qualified)[0]
            return {
                "symbol": symbol,
                "rank": 1,
                "score": rsi2 + decline,
                "expected_gross_move_fraction": abs(decline),
                "atr": atr14,
                "stop_atr_multiple": stop_atr,
                "holding_trading_days": hold_sessions,
                "decision_date": decision_date,
                "next_session_date": next_session_date,
                "overnight_hold": True,
                "exit_plan": {
                    "type": "stop_or_maximum_hold_close",
                    "maximum_hold_sessions": hold_sessions,
                    "same_interval_ambiguity": "stop_first",
                },
            }
        if family_id in {
            FLIGHT_TO_SAFETY_REBOUND_FAMILY,
            FLIGHT_TO_SAFETY_REPLICATION_FAMILY,
            FLIGHT_TO_SAFETY_REPLICATION_V2_FAMILY,
        }:
            decline_floor = float(
                parameters["minimum_equity_decline_fraction"]
            )
            treasury_floor = float(
                parameters["minimum_tlt_return_fraction"]
            )
            trend_period = int(parameters["trend_sma"])
            stop_atr = float(parameters["stop_atr14"])
            hold_sessions = int(parameters["maximum_hold_sessions"])
            if (
                decline_floor not in {0.0075, 0.0125}
                or treasury_floor not in {0.0, 0.0025}
                or trend_period not in {100, 200}
                or stop_atr not in {1.0, 1.5}
                or hold_sessions not in {2, 5}
            ):
                raise DenseStrategyRuntimeError(
                    "production flight-to-safety rules escaped the frozen grid"
                )
            if family_id == FLIGHT_TO_SAFETY_REBOUND_FAMILY:
                target_symbols = FLIGHT_TO_SAFETY_TARGET_SYMBOLS
            elif family_id == FLIGHT_TO_SAFETY_REPLICATION_FAMILY:
                target_symbols = FLIGHT_TO_SAFETY_REPLICATION_TARGET_SYMBOLS
            else:
                target_symbols = (
                    FLIGHT_TO_SAFETY_REPLICATION_V2_TARGET_SYMBOLS
                )
            if frozen_symbols != [
                *target_symbols,
                FLIGHT_TO_SAFETY_FEATURE_SYMBOL,
            ]:
                raise DenseStrategyRuntimeError(
                    "production flight-to-safety universe escaped the frozen rules"
                )
            treasury_bars = daily[FLIGHT_TO_SAFETY_FEATURE_SYMBOL]
            treasury_index = indices[
                FLIGHT_TO_SAFETY_FEATURE_SYMBOL
            ].get(decision_date)
            if treasury_index is None or treasury_index < 1:
                raise DenseStrategyRuntimeError(
                    "production Treasury feature history is incomplete"
                )
            treasury_return = (
                float(treasury_bars[treasury_index]["close"])
                / float(treasury_bars[treasury_index - 1]["close"])
                - 1
            )
            if treasury_return < treasury_floor:
                raise DenseStrategyRuntimeError(
                    "no exact production flight-to-safety signal"
                )
            qualified: list[tuple[float, str, float]] = []
            for symbol in target_symbols:
                bars = daily[symbol]
                symbol_index = indices[symbol].get(decision_date)
                if symbol_index is None or symbol_index < max(
                    trend_period - 1, 14, 1
                ):
                    raise DenseStrategyRuntimeError(
                        "production equity-rebound history is incomplete"
                    )
                trend = _sma(bars, symbol_index, trend_period)
                atr14 = _atr(bars, symbol_index)
                decline = (
                    float(bars[symbol_index]["close"])
                    / float(bars[symbol_index - 1]["close"])
                    - 1
                )
                if (
                    trend is not None
                    and atr14 is not None
                    and float(bars[symbol_index]["close"]) > trend
                    and decline <= -decline_floor
                    and _cost_floor(abs(decline))
                ):
                    qualified.append((decline, symbol, atr14))
            if not qualified:
                raise DenseStrategyRuntimeError(
                    "no exact production flight-to-safety signal"
                )
            decline, symbol, atr14 = sorted(qualified)[0]
            return {
                "symbol": symbol,
                "rank": 1,
                "score": -decline + treasury_return,
                "expected_gross_move_fraction": abs(decline),
                "atr": atr14,
                "stop_atr_multiple": stop_atr,
                "holding_trading_days": hold_sessions,
                "decision_date": decision_date,
                "next_session_date": next_session_date,
                "overnight_hold": True,
                "exit_plan": {
                    "type": "stop_or_maximum_hold_close",
                    "maximum_hold_sessions": hold_sessions,
                    "same_interval_ambiguity": "stop_first",
                },
            }
        if family_id == BREADTH_CAPITULATION_REBOUND_FAMILY:
            required_decliners = int(
                parameters["minimum_declining_symbols"]
            )
            median_decline_floor = float(
                parameters["minimum_median_decline_fraction"]
            )
            target_decline_floor = float(
                parameters["minimum_target_decline_fraction"]
            )
            stop_atr = float(parameters["stop_atr14"])
            hold_sessions = int(parameters["maximum_hold_sessions"])
            if (
                required_decliners not in {4, 5}
                or median_decline_floor not in {0.005, 0.01}
                or target_decline_floor not in {0.01, 0.015}
                or stop_atr not in {1.0, 1.5}
                or hold_sessions not in {2, 5}
                or frozen_symbols != list(BREADTH_CAPITULATION_SYMBOLS)
            ):
                raise DenseStrategyRuntimeError(
                    "production breadth-capitulation rules escaped the frozen grid"
                )
            observed: list[tuple[float, str, float]] = []
            for symbol in BREADTH_CAPITULATION_SYMBOLS:
                bars = daily[symbol]
                symbol_index = indices[symbol].get(decision_date)
                if symbol_index is None or symbol_index < 14:
                    raise DenseStrategyRuntimeError(
                        "production breadth-capitulation history is incomplete"
                    )
                atr14 = _atr(bars, symbol_index)
                if atr14 is None:
                    raise DenseStrategyRuntimeError(
                        "production breadth-capitulation ATR is incomplete"
                    )
                session_return = (
                    float(bars[symbol_index]["close"])
                    / float(bars[symbol_index - 1]["close"])
                    - 1
                )
                observed.append((session_return, symbol, atr14))
            returns = [item[0] for item in observed]
            if (
                sum(item < 0 for item in returns) < required_decliners
                or statistics.median(returns) > -median_decline_floor
            ):
                raise DenseStrategyRuntimeError(
                    "no exact production breadth-capitulation signal"
                )
            qualified = [
                item
                for item in observed
                if item[0] <= -target_decline_floor
                and _cost_floor(abs(item[0]))
            ]
            if not qualified:
                raise DenseStrategyRuntimeError(
                    "no exact production breadth-capitulation signal"
                )
            session_return, symbol, atr14 = sorted(qualified)[0]
            return {
                "symbol": symbol,
                "rank": 1,
                "score": -session_return,
                "expected_gross_move_fraction": abs(session_return),
                "atr": atr14,
                "stop_atr_multiple": stop_atr,
                "holding_trading_days": hold_sessions,
                "decision_date": decision_date,
                "next_session_date": next_session_date,
                "overnight_hold": True,
                "exit_plan": {
                    "type": "stop_or_maximum_hold_close",
                    "maximum_hold_sessions": hold_sessions,
                    "same_interval_ambiguity": "stop_first",
                },
            }
        if family_id == SECTOR_ETF_GAP_DRIFT_FAMILY:
            minimum_gap = float(parameters["minimum_gap_fraction"])
            maximum_gap = float(parameters["maximum_gap_fraction"])
            trend_period = int(parameters["trend_sma"])
            stop_atr = float(parameters["stop_atr14"])
            hold_sessions = int(parameters["maximum_hold_sessions"])
            if (
                minimum_gap not in {0.01, 0.02}
                or maximum_gap not in {0.04, 0.08}
                or minimum_gap >= maximum_gap
                or trend_period not in {100, 200}
                or stop_atr not in {1.0, 1.5}
                or hold_sessions not in {2, 5}
            ):
                raise DenseStrategyRuntimeError(
                    "production sector gap-drift rules escaped the frozen grid"
                )
            qualified: list[tuple[float, float, str, float]] = []
            for symbol in map(str, frozen_symbols):
                bars = daily[symbol]
                symbol_index = indices[symbol].get(decision_date)
                if symbol_index is None or symbol_index < max(
                    trend_period - 1, 14, 1
                ):
                    raise DenseStrategyRuntimeError(
                        "production sector gap-drift history is incomplete"
                    )
                decision_bar = bars[symbol_index]
                gap_fraction = (
                    float(decision_bar["open"])
                    / float(bars[symbol_index - 1]["close"])
                    - 1
                )
                session_return = (
                    float(decision_bar["close"])
                    / float(decision_bar["open"])
                    - 1
                )
                trend = _sma(bars, symbol_index, trend_period)
                atr14 = _atr(bars, symbol_index)
                if (
                    trend is not None
                    and atr14 is not None
                    and minimum_gap <= gap_fraction <= maximum_gap
                    and session_return >= 0
                    and float(decision_bar["close"]) > trend
                    and _cost_floor(gap_fraction)
                ):
                    qualified.append(
                        (gap_fraction, session_return, symbol, atr14)
                    )
            if not qualified:
                raise DenseStrategyRuntimeError(
                    "no exact production sector gap-drift signal"
                )
            gap_fraction, session_return, symbol, atr14 = sorted(
                qualified,
                key=lambda item: (-item[0], -item[1], item[2]),
            )[0]
            return {
                "symbol": symbol,
                "rank": 1,
                "score": gap_fraction + session_return,
                "expected_gross_move_fraction": gap_fraction,
                "atr": atr14,
                "stop_atr_multiple": stop_atr,
                "holding_trading_days": hold_sessions,
                "decision_date": decision_date,
                "next_session_date": next_session_date,
                "overnight_hold": True,
                "exit_plan": {
                    "type": "stop_or_maximum_hold_close",
                    "maximum_hold_sessions": hold_sessions,
                    "same_interval_ambiguity": "stop_first",
                },
            }
        if family_id in {
            ETF_CROSS_SECTIONAL_MOMENTUM_FAMILY,
            ETF_CROSS_SECTIONAL_REVERSAL_FAMILY,
        }:
            lookback = int(parameters["return_lookback_sessions"])
            trend_period = int(parameters["market_trend_sma"])
            threshold_field = (
                "minimum_excess_return_fraction"
                if family_id == ETF_CROSS_SECTIONAL_MOMENTUM_FAMILY
                else "minimum_lag_fraction"
            )
            threshold = float(parameters[threshold_field])
            spy_index = indices["SPY"].get(decision_date)
            if spy_index is None or spy_index < max(lookback, trend_period - 1):
                raise DenseStrategyRuntimeError(
                    "production ETF relative-performance history is incomplete"
                )
            market_trend = _sma(daily["SPY"], spy_index, trend_period)
            if (
                market_trend is None
                or float(daily["SPY"][spy_index]["close"]) <= market_trend
            ):
                raise DenseStrategyRuntimeError(
                    "production ETF relative-performance market trend gate is closed"
                )
            features: list[tuple[float, str, float]] = []
            for symbol in map(str, frozen_symbols):
                bars = daily[symbol]
                symbol_index = indices[symbol].get(decision_date)
                if symbol_index is None or symbol_index < lookback:
                    raise DenseStrategyRuntimeError(
                        "production ETF relative-performance universe history is incomplete"
                    )
                atr14 = _atr(bars, symbol_index)
                if atr14 is None:
                    raise DenseStrategyRuntimeError(
                        "production ETF relative-performance ATR history is incomplete"
                    )
                trailing_return = (
                    float(bars[symbol_index]["close"])
                    / float(bars[symbol_index - lookback]["close"])
                    - 1
                )
                features.append((trailing_return, symbol, atr14))
            benchmark = statistics.median(item[0] for item in features)
            direction = (
                1 if family_id == ETF_CROSS_SECTIONAL_MOMENTUM_FAMILY else -1
            )
            qualified = []
            for trailing_return, symbol, atr14 in features:
                relative_move = direction * (trailing_return - benchmark)
                if relative_move >= threshold and _cost_floor(relative_move):
                    qualified.append(
                        (trailing_return, symbol, atr14, relative_move)
                    )
            if not qualified:
                raise DenseStrategyRuntimeError(
                    "no exact production ETF relative-performance signal"
                )
            trailing_return, symbol, atr14, relative_move = sorted(
                qualified,
                key=lambda item: (
                    -direction * item[0],
                    item[1],
                ),
            )[0]
            return {
                "symbol": symbol,
                "rank": 1,
                "score": direction * trailing_return,
                "expected_gross_move_fraction": relative_move,
                "atr": atr14,
                "stop_atr_multiple": float(parameters["stop_atr14"]),
                "holding_trading_days": int(parameters["maximum_hold_sessions"]),
                "decision_date": decision_date,
                "next_session_date": next_session_date,
                "exit_plan": {
                    "type": "stop_or_maximum_hold_close",
                    "maximum_hold_sessions": int(
                        parameters["maximum_hold_sessions"]
                    ),
                    "same_interval_ambiguity": "stop_first",
                },
            }
        if family_id == SECTOR_ETF_ROTATION_FAMILY:
            lookback = int(parameters["return_lookback_sessions"])
            excess_floor = float(
                parameters["minimum_excess_return_fraction"]
            )
            trend_period = int(parameters["market_trend_sma"])
            stop_atr = float(parameters["stop_atr14"])
            hold_sessions = int(parameters["maximum_hold_sessions"])
            if (
                len(frozen_symbols) != 12
                or set(map(str, frozen_symbols)) != set(daily)
                or "SPY" not in daily
                or lookback not in {5, 20}
                or excess_floor not in {0.0, 0.01}
                or trend_period not in {20, 60}
                or stop_atr not in {1.0, 1.5}
                or hold_sessions not in {1, 3}
            ):
                raise DenseStrategyRuntimeError(
                    "production sector-rotation rules escaped the frozen grid"
                )
            spy_index = indices["SPY"].get(decision_date)
            if spy_index is None or spy_index < max(
                lookback, trend_period - 1
            ):
                raise DenseStrategyRuntimeError(
                    "production sector-rotation SPY history is incomplete"
                )
            trend = _sma(daily["SPY"], spy_index, trend_period)
            if (
                trend is None
                or float(daily["SPY"][spy_index]["close"]) <= trend
            ):
                raise DenseStrategyRuntimeError(
                    "production sector-rotation market trend gate is closed"
                )
            spy_return = (
                float(daily["SPY"][spy_index]["close"])
                / float(daily["SPY"][spy_index - lookback]["close"])
                - 1
            )
            qualified: list[tuple[float, str, float, float]] = []
            for symbol in map(str, frozen_symbols):
                if symbol == "SPY":
                    continue
                bars = daily[symbol]
                symbol_index = indices[symbol].get(decision_date)
                if symbol_index is None or symbol_index < lookback:
                    raise DenseStrategyRuntimeError(
                        "production sector-rotation universe history is incomplete"
                    )
                atr14 = _atr(bars, symbol_index)
                if atr14 is None:
                    raise DenseStrategyRuntimeError(
                        "production sector-rotation ATR history is incomplete"
                    )
                trailing_return = (
                    float(bars[symbol_index]["close"])
                    / float(bars[symbol_index - lookback]["close"])
                    - 1
                )
                excess = trailing_return - spy_return
                if excess >= excess_floor and _cost_floor(excess):
                    qualified.append(
                        (trailing_return, symbol, atr14, excess)
                    )
            if not qualified:
                raise DenseStrategyRuntimeError(
                    "no exact production sector-rotation signal"
                )
            trailing_return, symbol, atr14, excess = sorted(
                qualified, key=lambda item: (-item[0], item[1])
            )[0]
            return {
                "symbol": symbol,
                "rank": 1,
                "score": trailing_return,
                "expected_gross_move_fraction": excess,
                "atr": atr14,
                "stop_atr_multiple": stop_atr,
                "holding_trading_days": hold_sessions,
                "decision_date": decision_date,
                "next_session_date": next_session_date,
                "exit_plan": {
                    "type": "stop_or_maximum_hold_close",
                    "maximum_hold_sessions": hold_sessions,
                    "same_interval_ambiguity": "stop_first",
                },
            }
        if family_id == ETF_TURN_OF_MONTH_FAMILY:
            exchange_calendar = decision_data.get(
                "exchange_calendar_dates"
            )
            if (
                not isinstance(exchange_calendar, list)
                or exchange_calendar
                != sorted(set(map(str, exchange_calendar)))
                or decision_date not in exchange_calendar
                or next_session_date not in exchange_calendar
                or exchange_calendar.index(next_session_date)
                != exchange_calendar.index(decision_date) + 1
            ):
                raise DenseStrategyRuntimeError(
                    "production turn-of-month exchange calendar is incomplete"
                )
            before_sessions = int(
                parameters["sessions_before_month_end"]
            )
            after_sessions = int(
                parameters["sessions_after_month_start"]
            )
            if not _turn_of_month_membership(
                decision_date,
                exchange_calendar,
                before_sessions=before_sessions,
                after_sessions=after_sessions,
            ):
                raise DenseStrategyRuntimeError(
                    "production date is outside the exact month boundary"
                )
            spy_index = indices["SPY"].get(decision_date)
            trend_period = int(parameters["market_trend_sma"])
            if spy_index is None or spy_index < trend_period - 1:
                raise DenseStrategyRuntimeError(
                    "production turn-of-month history is incomplete"
                )
            trend = _sma(daily["SPY"], spy_index, trend_period)
            atr14 = _atr(daily["SPY"], spy_index)
            close = float(daily["SPY"][spy_index]["close"])
            if (
                trend is None
                or atr14 is None
                or close <= trend
                or not _cost_floor(atr14 / close)
            ):
                raise DenseStrategyRuntimeError(
                    "production turn-of-month trend or cost gate is closed"
                )
            return {
                "symbol": "SPY",
                "rank": 1,
                "score": 1.0,
                "expected_gross_move_fraction": atr14 / close,
                "atr": atr14,
                "stop_atr_multiple": float(parameters["stop_atr14"]),
                "holding_trading_days": int(
                    parameters["maximum_hold_sessions"]
                ),
                "decision_date": decision_date,
                "next_session_date": next_session_date,
                "exit_plan": {
                    "type": "stop_or_maximum_hold_close",
                    "maximum_hold_sessions": int(
                        parameters["maximum_hold_sessions"]
                    ),
                    "same_interval_ambiguity": "stop_first",
                },
            }
        if family_id == ETF_HIGH_CONTINUATION_FAMILY:
            return_floor = float(
                parameters["minimum_five_session_return_fraction"]
            )
            proximity = float(
                parameters["minimum_close_to_prior_high_fraction"]
            )
            trend_period = int(parameters["market_trend_sma"])
            spy_index = indices["SPY"].get(decision_date)
            if spy_index is None or spy_index < max(252, trend_period - 1):
                raise DenseStrategyRuntimeError(
                    "production ETF high-continuation history is incomplete"
                )
            market_trend = _sma(daily["SPY"], spy_index, trend_period)
            if (
                market_trend is None
                or float(daily["SPY"][spy_index]["close"]) <= market_trend
            ):
                raise DenseStrategyRuntimeError(
                    "production ETF high-continuation trend gate is closed"
                )
            qualified: list[tuple[float, str, float]] = []
            for symbol in map(str, frozen_symbols):
                bars = daily[symbol]
                symbol_index = indices[symbol].get(decision_date)
                if symbol_index is None or symbol_index < 252:
                    raise DenseStrategyRuntimeError(
                        "production ETF high-continuation universe history is incomplete"
                    )
                atr14 = _atr(bars, symbol_index)
                if atr14 is None:
                    raise DenseStrategyRuntimeError(
                        "production ETF high-continuation ATR history is incomplete"
                    )
                five_session_return = (
                    float(bars[symbol_index]["close"])
                    / float(bars[symbol_index - 5]["close"])
                    - 1
                )
                prior_high = max(
                    float(item["high"])
                    for item in bars[symbol_index - 252 : symbol_index]
                )
                if (
                    five_session_return >= return_floor
                    and float(bars[symbol_index]["close"])
                    >= proximity * prior_high
                    and _cost_floor(five_session_return)
                ):
                    qualified.append((five_session_return, symbol, atr14))
            if not qualified:
                raise DenseStrategyRuntimeError(
                    "no exact production ETF high-continuation signal"
                )
            five_session_return, symbol, atr14 = sorted(
                qualified, key=lambda item: (-item[0], item[1])
            )[0]
            return {
                "symbol": symbol,
                "rank": 1,
                "score": five_session_return,
                "expected_gross_move_fraction": five_session_return,
                "atr": atr14,
                "stop_atr_multiple": float(parameters["stop_atr14"]),
                "holding_trading_days": int(parameters["maximum_hold_sessions"]),
                "decision_date": decision_date,
                "next_session_date": next_session_date,
                "exit_plan": {
                    "type": "stop_or_maximum_hold_close",
                    "maximum_hold_sessions": int(
                        parameters["maximum_hold_sessions"]
                    ),
                    "same_interval_ambiguity": "stop_first",
                },
            }
        trend_period = int(parameters["trend_sma"])
        rsi_max = float(parameters["rsi2_maximum"])
        decline_floor = float(parameters["three_session_decline_fraction"])
        scored: list[tuple[float, float, str, float]] = []
        for symbol in map(str, frozen_symbols):
            bars = daily[symbol]
            symbol_index = indices[symbol].get(decision_date)
            if symbol_index is None or symbol_index < max(trend_period - 1, 3):
                continue
            trend = _sma(bars, symbol_index, trend_period)
            rsi2 = _rsi_wilder(bars, symbol_index, 2)
            atr14 = _atr(bars, symbol_index)
            decline = (
                float(bars[symbol_index]["close"])
                / float(bars[symbol_index - 3]["close"])
                - 1
            )
            if (
                trend is not None
                and rsi2 is not None
                and atr14 is not None
                and float(bars[symbol_index]["close"]) > trend
                and rsi2 <= rsi_max
                and decline <= -decline_floor
                and _cost_floor(abs(decline))
            ):
                scored.append((rsi2, decline, symbol, atr14))
        if not scored:
            raise DenseStrategyRuntimeError("no exact production pullback signal")
        rsi2, decline, symbol, atr14 = sorted(scored)[0]
        return {
            "symbol": symbol,
            "rank": 1,
            "score": rsi2 + decline,
            "expected_gross_move_fraction": abs(decline),
            "atr": atr14,
            "stop_atr_multiple": float(parameters["stop_atr14"]),
            "holding_trading_days": int(parameters["maximum_hold_sessions"]),
            "decision_date": decision_date,
            "next_session_date": next_session_date,
            "exit_plan": {
                "type": "stop_or_maximum_hold_close",
                "maximum_hold_sessions": int(parameters["maximum_hold_sessions"]),
                "same_interval_ambiguity": "stop_first",
            },
        }
    if family_id not in EQUITY_RESIDUAL_FAMILIES:
        raise DenseStrategyRuntimeError("unsupported production daily family")
    if "SPY" not in daily:
        raise DenseStrategyRuntimeError("production equity data needs SPY")
    references = decision_data["reference_snapshot"]
    if (
        not isinstance(references, Mapping)
        or decision_data.get("reference_snapshot_complete") is not True
        or isinstance(decision_data.get("reference_source_total"), bool)
        or decision_data.get("reference_source_total") != len(references)
    ):
        raise DenseStrategyRuntimeError(
            "production equity reference denominator is incomplete"
        )
    if [str(bar["date"]) for bar in daily["SPY"]] != calendar_dates:
        raise DenseStrategyRuntimeError(
            "production SPY history does not cover the complete calendar"
        )
    universe = _production_equity_universe(
        daily,
        references,
        decision_date,
        calendar_dates,
    )
    window = int(parameters["prior_return_sessions"])
    threshold = float(parameters["residual_z_threshold"])
    trend_period = 100 if parameters["market_trend_gate"] == "SPY>SMA100" else 200
    spy_bars = daily["SPY"]
    spy_index = indices["SPY"].get(decision_date)
    if spy_index is None or spy_index < max(window, trend_period - 1):
        raise DenseStrategyRuntimeError("production SPY history is incomplete")
    spy_return = (
        float(spy_bars[spy_index]["close"])
        / float(spy_bars[spy_index - window]["close"])
        - 1
    )
    spy_sma = _sma(spy_bars, spy_index, trend_period)
    if spy_sma is None or float(spy_bars[spy_index]["close"]) <= spy_sma:
        raise DenseStrategyRuntimeError("production market trend gate is closed")
    scored_equities: list[tuple[float, str, float, float]] = []
    for symbol in universe:
        bars = daily[symbol]
        symbol_index = indices[symbol].get(decision_date)
        if symbol_index is None or symbol_index < window:
            continue
        symbol_return = (
            float(bars[symbol_index]["close"])
            / float(bars[symbol_index - window]["close"])
            - 1
        )
        residual = symbol_return - spy_return
        history: list[float] = []
        for prior_bar in bars[:symbol_index]:
            prior_date = str(prior_bar["date"])
            prior_symbol_index = indices[symbol][prior_date]
            prior_spy_index = indices["SPY"].get(prior_date)
            if (
                prior_symbol_index < window
                or prior_spy_index is None
                or prior_spy_index < window
            ):
                continue
            history.append(
                float(bars[prior_symbol_index]["close"])
                / float(bars[prior_symbol_index - window]["close"])
                - float(spy_bars[prior_spy_index]["close"])
                / float(spy_bars[prior_spy_index - window]["close"])
            )
        z_score = _z_score(residual, history)
        atr14 = _atr(bars, symbol_index)
        if (
            z_score is not None
            and z_score <= threshold
            and atr14 is not None
            and _cost_floor(abs(residual))
        ):
            scored_equities.append((z_score, symbol, atr14, residual))
    if not scored_equities:
        raise DenseStrategyRuntimeError("no exact production residual-reversal signal")
    z_score, symbol, atr14, residual = sorted(scored_equities)[0]
    return {
        "symbol": symbol,
        "rank": 1,
        "score": z_score,
        "expected_gross_move_fraction": abs(residual),
        "atr": atr14,
        "stop_atr_multiple": float(parameters["stop_atr14"]),
        "holding_trading_days": int(parameters["hold_sessions"]),
        "decision_date": decision_date,
        "next_session_date": next_session_date,
        "exit_plan": {
            "type": "stop_or_maximum_hold_close",
            "maximum_hold_sessions": int(parameters["hold_sessions"]),
            "same_interval_ambiguity": "stop_first",
        },
    }


def _production_close_to_open_signal(
    decision_data: Mapping[str, Any],
    parameters: Mapping[str, Any],
    frozen_universe: Mapping[str, Any],
) -> dict[str, Any]:
    expected = {
        "family_id",
        "decision_date",
        "next_session_date",
        "daily_history_complete",
        "daily_bars",
        "symbols",
        "decision_bars_complete",
        "fifteen_minute_bars",
    }
    if (
        set(decision_data) != expected
        or decision_data.get("family_id") != ETF_CLOSE_TO_OPEN_FAMILY
    ):
        raise DenseStrategyRuntimeError(
            "production close-to-open decision-data schema drifted"
        )
    decision_date = decision_data.get("decision_date")
    next_session_date = decision_data.get("next_session_date")
    if (
        not isinstance(decision_date, str)
        or not isinstance(next_session_date, str)
        or decision_data.get("daily_history_complete") is not True
        or decision_data.get("decision_bars_complete") is not True
    ):
        raise DenseStrategyRuntimeError(
            "production close-to-open dates or completeness are invalid"
        )
    try:
        decision_day = date.fromisoformat(decision_date)
        next_day = date.fromisoformat(next_session_date)
    except ValueError as exc:
        raise DenseStrategyRuntimeError(
            "production close-to-open dates are invalid"
        ) from exc
    if not 1 <= (next_day - decision_day).days <= 4:
        raise DenseStrategyRuntimeError(
            "production close-to-open next session is not adjacent"
        )
    frozen_symbols = frozen_universe.get("symbols")
    if (
        frozen_symbols != list(CLOSE_TO_OPEN_ETF_SYMBOLS)
        or decision_data.get("symbols") != frozen_symbols
    ):
        raise DenseStrategyRuntimeError(
            "production close-to-open universe drifted"
        )
    daily = _daily_series(decision_data)
    if set(daily) != set(frozen_symbols) or any(
        not bars or str(bars[-1]["date"]) >= decision_date
        for bars in daily.values()
    ):
        raise DenseStrategyRuntimeError(
            "production close-to-open prior daily history is incomplete"
        )
    raw_fifteen = decision_data.get("fifteen_minute_bars")
    sessions = _fifteen_minute_sessions(
        {
            "fifteen_minute_bars": {
                decision_date: raw_fifteen,
            }
        }
    )
    rows_by_symbol = sessions[decision_date]
    if set(rows_by_symbol) != set(frozen_symbols):
        raise DenseStrategyRuntimeError(
            "production close-to-open intraday universe is incomplete"
        )
    decision_time = str(parameters["decision_bar_time"])
    return_floor = float(parameters["minimum_session_return_fraction"])
    trend_period = int(parameters["prior_trend_sma"])
    stop_atr = float(parameters["stop_atr14"])
    exit_timing = str(parameters["exit_timing"])
    decision_indices = {"15:15": 23, "15:30": 24}
    if (
        decision_time not in decision_indices
        or return_floor not in {0.005, 0.01}
        or trend_period not in {20, 60}
        or stop_atr not in {0.5, 1.0}
        or exit_timing not in {"next_open", "next_0945_close"}
    ):
        raise DenseStrategyRuntimeError(
            "production close-to-open rules escaped the frozen grid"
        )
    decision_index = decision_indices[decision_time]
    qualified: list[tuple[float, str, float]] = []
    for symbol in map(str, frozen_symbols):
        rows = rows_by_symbol[symbol]
        bars = daily[symbol]
        if len(rows) != decision_index + 1 or len(bars) < max(
            trend_period, 15
        ):
            raise DenseStrategyRuntimeError(
                "production close-to-open observable history is incomplete"
            )
        observed = datetime.fromisoformat(str(rows[-1]["timestamp"]))
        if observed.timetz().replace(tzinfo=None) != time.fromisoformat(
            decision_time
        ):
            raise DenseStrategyRuntimeError(
                "production close-to-open decision timestamp drifted"
            )
        signal_close = float(rows[-1]["close"])
        session_return = signal_close / float(rows[0]["open"]) - 1
        prior_trend = statistics.fmean(
            float(item["close"]) for item in bars[-trend_period:]
        )
        atr14 = _atr(bars, len(bars) - 1)
        if (
            atr14 is not None
            and session_return + 1e-12 >= return_floor
            and signal_close > prior_trend
            and _cost_floor(session_return)
        ):
            qualified.append((session_return, symbol, atr14))
    if not qualified:
        raise DenseStrategyRuntimeError(
            "no exact production close-to-open signal"
        )
    session_return, symbol, atr14 = sorted(
        qualified, key=lambda item: (-item[0], item[1])
    )[0]
    trigger = rows_by_symbol[symbol][-1]
    return {
        "symbol": symbol,
        "rank": 1,
        "score": session_return,
        "expected_gross_move_fraction": session_return,
        "atr": atr14,
        "stop_atr_multiple": stop_atr,
        "holding_trading_days": 1,
        "decision_date": decision_date,
        "next_session_date": next_session_date,
        "trigger_bar_timestamp": trigger["timestamp"],
        "entry_interval_minutes": 15,
        "overnight_hold": True,
        "exit_plan": {
            "type": (
                "stop_or_next_open"
                if exit_timing == "next_open"
                else "stop_or_next_0945_close"
            ),
            "maximum_hold_sessions": 1,
            "same_interval_ambiguity": "stop_first",
        },
    }


def _production_intraday_momentum_signal(
    decision_data: Mapping[str, Any],
    parameters: Mapping[str, Any],
    frozen_universe: Mapping[str, Any],
) -> dict[str, Any]:
    expected = {
        "family_id",
        "session_date",
        "calendar_sessions",
        "minute_history_complete",
        "symbols",
        "minute_bars",
    }
    if (
        set(decision_data) != expected
        or decision_data.get("family_id")
        != INDEX_ETF_OPENING_MOMENTUM_FAMILY
    ):
        raise DenseStrategyRuntimeError(
            "production opening-momentum decision-data schema drifted"
        )
    frozen_symbols = frozen_universe.get("symbols")
    if (
        not isinstance(frozen_symbols, list)
        or decision_data.get("symbols") != frozen_symbols
    ):
        raise DenseStrategyRuntimeError(
            "production opening-momentum universe drifted"
        )
    raw_sessions = decision_data.get("minute_bars")
    session_date = decision_data.get("session_date")
    if not isinstance(raw_sessions, Mapping) or not raw_sessions:
        raise DenseStrategyRuntimeError(
            "production opening-momentum minute data is missing"
        )
    days = sorted(map(str, raw_sessions))
    if (
        not isinstance(session_date, str)
        or days[-1] != session_date
        or decision_data.get("calendar_sessions") != days
        or decision_data.get("minute_history_complete") is not True
    ):
        raise DenseStrategyRuntimeError(
            "production opening-momentum session boundary drifted"
        )
    expected_symbols = set(map(str, frozen_symbols))
    counts: dict[str, int] = {}
    for day in days:
        raw_day = raw_sessions[day]
        if (
            not isinstance(raw_day, Mapping)
            or set(map(str, raw_day)) != expected_symbols
        ):
            raise DenseStrategyRuntimeError(
                "production opening-momentum universe is incomplete"
            )
        lengths = {
            len(raw_day[symbol]) for symbol in expected_symbols
        }
        if len(lengths) != 1:
            raise DenseStrategyRuntimeError(
                "production opening-momentum bars are not synchronized"
            )
        counts[day] = lengths.pop()
    if (
        any(counts[day] != 390 for day in days[:-1])
        or not 1 <= counts[session_date] <= 390
    ):
        raise DenseStrategyRuntimeError(
            "production opening-momentum session lengths are invalid"
        )
    sessions = _minute_sessions(
        {
            "minute_bars": raw_sessions,
            "regular_session_minutes_by_date": counts,
        }
    )
    opening_window = int(parameters["opening_window_minutes"])
    minimum_return = float(parameters["minimum_opening_return"])
    confirmation_bars = int(
        parameters["vwap_confirmation_completed_bars"]
    )
    current_length = counts[session_date]
    if current_length <= opening_window:
        raise DenseStrategyRuntimeError(
            "production opening-momentum window is incomplete"
        )
    qualified: dict[str, dict[str, Any]] = {}
    for symbol in map(str, frozen_symbols):
        bars = sessions[session_date][symbol]
        opening_return = (
            float(bars[opening_window - 1]["close"])
            / float(bars[0]["open"])
            - 1
        )
        if (
            opening_return < minimum_return
            or not _cost_floor(opening_return)
        ):
            continue
        qualified[symbol] = {
            "opening_return": opening_return,
            "numerator": sum(
                float(item["vwap_numerator"])
                for item in bars[:opening_window]
            ),
            "denominator": sum(
                float(item["vwap_denominator"])
                for item in bars[:opening_window]
            ),
            "above": 0,
        }
    if not qualified:
        raise DenseStrategyRuntimeError(
            "no exact production opening-momentum setup"
        )
    first_trigger: (
        tuple[int, list[tuple[float, str, float, float]]] | None
    ) = None
    for index in range(opening_window, current_length):
        triggered: list[tuple[float, str, float, float]] = []
        for symbol, state in qualified.items():
            if state.get("triggered") is True:
                continue
            bar = sessions[session_date][symbol][index]
            state["numerator"] += float(bar["vwap_numerator"])
            state["denominator"] += float(bar["vwap_denominator"])
            if state["denominator"] <= 0:
                state["above"] = 0
                continue
            exact_vwap = state["numerator"] / state["denominator"]
            state["above"] = (
                state["above"] + 1
                if float(bar["close"]) > exact_vwap
                else 0
            )
            if state["above"] >= confirmation_bars:
                state["triggered"] = True
                atr = _intraday_atr(
                    sessions[session_date][symbol], index
                )
                if atr is not None:
                    triggered.append(
                        (
                            -float(state["opening_return"]),
                            symbol,
                            atr,
                            float(state["opening_return"]),
                        )
                    )
        if triggered:
            first_trigger = (index, triggered)
            break
    if first_trigger is None:
        raise DenseStrategyRuntimeError(
            "no exact production opening-momentum signal"
        )
    trigger_index, triggered = first_trigger
    if trigger_index != current_length - 1:
        raise DenseStrategyRuntimeError(
            "production opening-momentum signal was already observable"
        )
    negative_return, symbol, atr, opening_return = sorted(
        triggered
    )[0]
    trigger_bar = sessions[session_date][symbol][trigger_index]
    return {
        "symbol": symbol,
        "rank": 1,
        "score": -negative_return,
        "expected_gross_move_fraction": opening_return,
        "atr": atr,
        "stop_atr_multiple": float(parameters["stop_intraday_atr"]),
        "holding_trading_days": 1,
        "decision_date": session_date,
        "trigger_bar_timestamp": trigger_bar["timestamp"],
        "target_r": float(parameters["target_r"]),
        "exit_plan": {
            "type": "stop_target_or_session_close",
            "target_r": float(parameters["target_r"]),
            "same_interval_ambiguity": "stop_first",
        },
    }


def _production_intraday_signal(
    decision_data: Mapping[str, Any],
    family_id: str,
    parameters: Mapping[str, Any],
    frozen_universe: Mapping[str, Any],
) -> dict[str, Any]:
    expected = {
        "family_id",
        "session_date",
        "calendar_sessions",
        "minute_history_complete",
        "symbols",
        "minute_bars",
    }
    if (
        set(decision_data) != expected
        or decision_data.get("family_id") != family_id
    ):
        raise DenseStrategyRuntimeError(
            "production intraday decision-data schema drifted"
        )
    frozen_symbols = frozen_universe.get("symbols")
    if not isinstance(frozen_symbols, list) or decision_data.get("symbols") != frozen_symbols:
        raise DenseStrategyRuntimeError(
            "production intraday universe drifted from the frozen exact rules"
        )
    raw_sessions = decision_data.get("minute_bars")
    if not isinstance(raw_sessions, Mapping) or not raw_sessions:
        raise DenseStrategyRuntimeError("production minute history is missing")
    session_date = decision_data.get("session_date")
    days = sorted(map(str, raw_sessions))
    if (
        not isinstance(session_date, str)
        or days[-1] != session_date
        or len(days) < 61
        or decision_data.get("calendar_sessions") != days
        or decision_data.get("minute_history_complete") is not True
    ):
        raise DenseStrategyRuntimeError(
            "production minute history needs 60 prior sessions and current session"
        )
    counts: dict[str, int] = {}
    expected_symbols = set(map(str, frozen_symbols))
    for day in days:
        raw_day = raw_sessions[day]
        if not isinstance(raw_day, Mapping) or set(map(str, raw_day)) != expected_symbols:
            raise DenseStrategyRuntimeError(
                "production minute history omits a frozen symbol-session"
            )
        lengths = {len(raw_day[symbol]) for symbol in expected_symbols}
        if len(lengths) != 1:
            raise DenseStrategyRuntimeError(
                "production symbols are not synchronized to one completed minute"
            )
        counts[day] = lengths.pop()
    if any(counts[day] != 390 for day in days[:-1]) or not 1 <= counts[session_date] <= 390:
        raise DenseStrategyRuntimeError(
            "production prior sessions must contain all 390 regular-session minutes"
        )
    sessions = _minute_sessions(
        {
            "minute_bars": raw_sessions,
            "regular_session_minutes_by_date": counts,
        }
    )
    opening_window = int(parameters["opening_window_minutes"])
    reclaim_bars = int(parameters["vwap_reclaim_completed_bars"])
    threshold = float(parameters["downside_z_threshold"])
    current_length = counts[session_date]
    if current_length <= opening_window:
        raise DenseStrategyRuntimeError("opening window is not complete")
    qualified: dict[str, dict[str, Any]] = {}
    prior_days = days[-61:-1]
    for symbol in map(str, frozen_symbols):
        history = [
            float(sessions[day][symbol][opening_window - 1]["close"])
            / float(sessions[day][symbol][0]["open"])
            - 1
            for day in prior_days
        ]
        bars = sessions[session_date][symbol]
        opening_return = (
            float(bars[opening_window - 1]["close"])
            / float(bars[0]["open"])
            - 1
        )
        z_score = _z_score(opening_return, history)
        if (
            z_score is None
            or z_score > threshold
            or not _cost_floor(abs(opening_return))
        ):
            continue
        qualified[symbol] = {
            "z_score": z_score,
            "opening_return": opening_return,
            "numerator": sum(
                float(item["vwap_numerator"])
                for item in bars[:opening_window]
            ),
            "denominator": sum(
                float(item["vwap_denominator"])
                for item in bars[:opening_window]
            ),
            "above": 0,
        }
    if not qualified:
        raise DenseStrategyRuntimeError("no exact production opening dislocation")
    first_trigger: tuple[int, list[tuple[float, str, float, float]]] | None = None
    for index in range(opening_window, current_length):
        triggered: list[tuple[float, str, float, float]] = []
        for symbol, state in qualified.items():
            if state.get("triggered") is True:
                continue
            bar = sessions[session_date][symbol][index]
            state["numerator"] += float(bar["vwap_numerator"])
            state["denominator"] += float(bar["vwap_denominator"])
            if state["denominator"] <= 0:
                state["above"] = 0
                continue
            exact_vwap = state["numerator"] / state["denominator"]
            state["above"] = (
                state["above"] + 1
                if float(bar["close"]) > exact_vwap
                else 0
            )
            if state["above"] >= reclaim_bars:
                state["triggered"] = True
                atr = _intraday_atr(sessions[session_date][symbol], index)
                if atr is not None:
                    triggered.append(
                        (
                            float(state["z_score"]),
                            symbol,
                            atr,
                            float(state["opening_return"]),
                        )
                    )
        if triggered:
            first_trigger = (index, triggered)
            break
    if first_trigger is None:
        raise DenseStrategyRuntimeError("no exact production VWAP-reclaim signal")
    trigger_index, triggered = first_trigger
    if trigger_index != current_length - 1:
        raise DenseStrategyRuntimeError(
            "production intraday signal was observable on an earlier bar"
        )
    z_score, symbol, atr, opening_return = sorted(triggered)[0]
    trigger_bar = sessions[session_date][symbol][trigger_index]
    return {
        "symbol": symbol,
        "rank": 1,
        "score": z_score,
        "expected_gross_move_fraction": abs(opening_return),
        "atr": atr,
        "stop_atr_multiple": float(parameters["stop_intraday_atr"]),
        "holding_trading_days": 1,
        "decision_date": session_date,
        "trigger_bar_timestamp": trigger_bar["timestamp"],
        "target_r": float(parameters["target_r"]),
        "exit_plan": {
            "type": "stop_target_or_session_close",
            "target_r": float(parameters["target_r"]),
            "same_interval_ambiguity": "stop_first",
        },
    }


def _production_earnings_sec_reaction_signal(
    decision_data: Mapping[str, Any],
    parameters: Mapping[str, Any],
    frozen_universe: Mapping[str, Any],
) -> dict[str, Any]:
    expected = {
        "family_id",
        "decision_date",
        "next_session_date",
        "calendar_dates",
        "daily_history_complete",
        "daily_bars",
        "events",
    }
    if (
        set(decision_data) != expected
        or decision_data.get("family_id") != EARNINGS_SEC_REACTION_FAMILY
        or decision_data.get("daily_history_complete") is not True
    ):
        raise DenseStrategyRuntimeError(
            "production SEC earnings decision-data schema drifted"
        )
    decision_date = str(decision_data["decision_date"])
    next_session_date = str(decision_data["next_session_date"])
    calendar_dates = decision_data["calendar_dates"]
    if not (
        isinstance(calendar_dates, list)
        and calendar_dates
        and calendar_dates == sorted(set(calendar_dates))
        and calendar_dates[-1] == decision_date
    ):
        raise DenseStrategyRuntimeError(
            "production SEC earnings calendar is incomplete"
        )
    try:
        decision_day = date.fromisoformat(decision_date)
        next_session_day = date.fromisoformat(next_session_date)
    except ValueError as exc:
        raise DenseStrategyRuntimeError(
            "production SEC earnings calendar dates are invalid"
        ) from exc
    if not 1 <= (next_session_day - decision_day).days <= 4:
        raise DenseStrategyRuntimeError(
            "production SEC earnings next session is not adjacent"
        )
    excluded = frozen_universe.get("excluded_symbols")
    if (
        frozen_universe.get("point_in_time") is not True
        or frozen_universe.get("security_type")
        != "SEC same-accession verified common equity"
        or not isinstance(excluded, list)
        or excluded != sorted(set(map(str, excluded)))
    ):
        raise DenseStrategyRuntimeError(
            "production SEC earnings universe drifted"
        )
    daily = _daily_series(decision_data)
    if any(
        str(bar["date"]) > decision_date
        for bars in daily.values()
        for bar in bars
    ):
        raise DenseStrategyRuntimeError(
            "production SEC earnings history contains post-decision bars"
        )
    indices = {
        symbol: {str(bar["date"]): index for index, bar in enumerate(bars)}
        for symbol, bars in daily.items()
    }
    events = decision_data["events"]
    if not isinstance(events, list):
        raise DenseStrategyRuntimeError(
            "production SEC earnings events are invalid"
        )
    minimum_eps_change = float(
        parameters["minimum_yoy_eps_change_ratio"]
    )
    minimum_gap = float(
        parameters["minimum_reaction_opening_gap_fraction"]
    )
    confirmation = str(parameters["reaction_confirmation"])
    stop_atr = float(parameters["stop_atr14"])
    hold = int(parameters["maximum_hold_sessions"])
    if (
        minimum_eps_change not in {0.25, 0.50}
        or minimum_gap not in {0.0, 0.01}
        or confirmation not in {"close>open", "close>prior_close"}
        or stop_atr not in {1.0, 1.5}
        or hold not in {2, 5}
    ):
        raise DenseStrategyRuntimeError(
            "production SEC earnings parameters escaped the grid"
        )
    qualified: list[tuple[float, float, str, float]] = []
    for event in events:
        if not isinstance(event, Mapping):
            raise DenseStrategyRuntimeError(
                "production SEC earnings event is invalid"
            )
        symbol = str(event.get("symbol", ""))
        if symbol in excluded:
            raise DenseStrategyRuntimeError(
                "production SEC earnings event uses an excluded symbol"
            )
        bars = daily.get(symbol)
        index = indices.get(symbol, {}).get(decision_date)
        if bars is None or index is None or index < 20:
            continue
        if (
            event.get("security_identity_state")
            != "VERIFIED_COMMON_EQUITY_COVER_FACT"
            or event.get("reaction_date") != decision_date
        ):
            raise DenseStrategyRuntimeError(
                "production SEC earnings identity or reaction date drifted"
            )
        eps_change = float(event["eps_change_ratio"])
        if eps_change + 1e-12 < minimum_eps_change:
            continue
        prior_close = float(bars[index - 1]["close"])
        reaction = bars[index]
        opening = float(reaction["open"])
        close = float(reaction["close"])
        gap = opening / prior_close - 1
        if gap + 1e-12 < minimum_gap:
            continue
        if (
            confirmation == "close>open" and close <= opening
        ) or (
            confirmation == "close>prior_close" and close <= prior_close
        ):
            continue
        prior_dollar_volume = statistics.median(
            float(bar["close"]) * float(bar["volume"])
            for bar in bars[index - 20 : index]
        )
        atr14 = _atr(bars, index)
        if (
            prior_close < 10
            or prior_dollar_volume < 50_000_000
            or atr14 is None
        ):
            continue
        reaction_dollar_volume = close * float(reaction["volume"])
        qualified.append(
            (-eps_change, -reaction_dollar_volume, symbol, atr14)
        )
    if not qualified:
        raise DenseStrategyRuntimeError(
            "no exact production SEC earnings reaction signal"
        )
    negative_eps, _negative_liquidity, symbol, atr14 = sorted(qualified)[0]
    reference_price = float(daily[symbol][-1]["close"])
    return {
        "symbol": symbol,
        "rank": 1,
        "score": -negative_eps,
        "expected_gross_move_fraction": atr14 / reference_price,
        "atr": atr14,
        "stop_atr_multiple": stop_atr,
        "holding_trading_days": hold,
        "decision_date": decision_date,
        "next_session_date": next_session_date,
        "overnight_hold": True,
        "exit_plan": {
            "type": "stop_or_maximum_hold_close",
            "maximum_hold_sessions": hold,
            "same_interval_ambiguity": "stop_first",
        },
    }


def evaluate_production_signal(
    decision_data: Mapping[str, Any],
    *,
    family_id: str,
    parameters: Mapping[str, Any],
    frozen_universe: Mapping[str, Any],
) -> dict[str, Any]:
    """Rebuild one current signal from the same observable indicators as history."""
    if family_id == INDEX_ETF_OPENING_MOMENTUM_FAMILY:
        return _production_intraday_momentum_signal(
            decision_data, parameters, frozen_universe
        )
    if family_id in INTRADAY_ETF_FAMILIES:
        return _production_intraday_signal(
            decision_data, family_id, parameters, frozen_universe
        )
    if family_id == ETF_CLOSE_TO_OPEN_FAMILY:
        return _production_close_to_open_signal(
            decision_data, parameters, frozen_universe
        )
    if family_id == EARNINGS_SEC_REACTION_FAMILY:
        return _production_earnings_sec_reaction_signal(
            decision_data, parameters, frozen_universe
        )
    if family_id in {
        *EQUITY_RESIDUAL_FAMILIES,
        ETF_RESIDUAL_REPLICATION_FAMILY,
        ETF_RESIDUAL_REPLICATION_V2_FAMILY,
        ETF_RESIDUAL_REPLICATION_V3_FAMILY,
        LIQUID_EQUITY_MOMENTUM_FAMILY,
        *ETF_PULLBACK_FAMILIES,
        SPY_RSI2_PULLBACK_FAMILY,
        ETF_CROSS_SECTIONAL_MOMENTUM_FAMILY,
        ETF_CROSS_SECTIONAL_REVERSAL_FAMILY,
        ETF_HIGH_CONTINUATION_FAMILY,
        ETF_TURN_OF_MONTH_FAMILY,
        SECTOR_ETF_ROTATION_FAMILY,
        SECTOR_ETF_GAP_DRIFT_FAMILY,
        FLIGHT_TO_SAFETY_REBOUND_FAMILY,
        FLIGHT_TO_SAFETY_REPLICATION_FAMILY,
        FLIGHT_TO_SAFETY_REPLICATION_V2_FAMILY,
        BREADTH_CAPITULATION_REBOUND_FAMILY,
        CROSS_STYLE_BREADTH_CONTINUATION_FAMILY,
        STYLE_ETF_BREAKOUT_CONTINUATION_FAMILY,
        *ETF_OVERSOLD_FAMILIES,
    }:
        return _production_daily_signal(
            decision_data, family_id, parameters, frozen_universe
        )
    raise DenseStrategyRuntimeError("unsupported production strategy family")


def _scenario(
    calendar: Sequence[str],
    candidates: Sequence[Mapping[str, Any]],
    policy: Mapping[str, Any],
    cost_bps_per_side: int,
    *,
    allowed_signal_ids: set[str] | None = None,
    quantity_by_signal_id: Mapping[str, int] | None = None,
) -> dict[str, Any]:
    return simulate_portfolio_account(
        calendar,
        candidates,
        starting_equity=float(policy.get("starting_equity", 100_000.0)),
        risk_fraction=float(policy["risk_fraction"]),
        maximum_concurrent_positions=int(policy["maximum_concurrent_positions"]),
        maximum_new_entries_per_day=1,
        maximum_aggregate_risk_fraction=float(
            policy["maximum_aggregate_risk_fraction"]
        ),
        maximum_gross_notional_fraction=float(
            policy["maximum_gross_notional_fraction"]
        ),
        cost_bps_per_side=cost_bps_per_side,
        allowed_signal_ids=allowed_signal_ids,
        quantity_by_signal_id=quantity_by_signal_id,
    )


def _rolling_origin_scenario(
    plan: Sequence[Mapping[str, Any]],
    candidates: Sequence[Mapping[str, Any]],
    policy: Mapping[str, Any],
    cost_bps_per_side: int,
    *,
    allowed_signal_ids: set[str] | None = None,
    quantity_by_signal_id: Mapping[str, int] | None = None,
) -> dict[str, Any]:
    starting_equity = float(policy.get("starting_equity", 100_000.0))
    current_equity = starting_equity
    account_path: list[dict[str, Any]] = []
    trial_accounting: list[dict[str, Any]] = []
    closed_trades: list[dict[str, Any]] = []
    for expected_fold, fold in enumerate(plan, 1):
        test_dates = fold.get("test_dates")
        entry_dates = fold.get("entry_dates")
        settlement_dates = fold.get("settlement_only_dates")
        if not (
            fold.get("fold") == expected_fold
            and isinstance(test_dates, list)
            and isinstance(entry_dates, list)
            and isinstance(settlement_dates, list)
            and test_dates == [*entry_dates, *settlement_dates]
            and test_dates
        ):
            raise DenseStrategyRuntimeError(
                "rolling-origin fold entry and settlement dates are invalid"
            )
        test_set = set(test_dates)
        entry_set = set(entry_dates)
        fold_candidates = [
            item for item in candidates if item.get("signal_date") in entry_set
        ]
        if any(
            item.get("outcome") == "eligible" and item.get("exit_date") not in test_set
            for item in fold_candidates
        ):
            raise DenseStrategyRuntimeError(
                "rolling-origin entry cannot settle inside its frozen test fold"
            )
        fold_allowed_signal_ids = (
            None
            if allowed_signal_ids is None
            else allowed_signal_ids
            & {str(item["signal_id"]) for item in fold_candidates}
        )
        fold_quantities = (
            None
            if quantity_by_signal_id is None
            else {
                signal_id: quantity_by_signal_id[signal_id]
                for signal_id in fold_allowed_signal_ids or set()
            }
        )
        fold_policy = {**dict(policy), "starting_equity": current_equity}
        scenario = _scenario(
            test_dates,
            fold_candidates,
            fold_policy,
            cost_bps_per_side,
            allowed_signal_ids=fold_allowed_signal_ids,
            quantity_by_signal_id=fold_quantities,
        )
        account_path.extend(scenario["account_path"])
        trial_accounting.extend(scenario["trial_accounting"])
        closed_trades.extend(scenario["closed_trades"])
        current_equity = float(scenario["ending_equity"])
    daily_returns = [
        float(item["daily_account_return_fraction"]) for item in account_path
    ]
    return {
        "cost_bps_per_side": float(cost_bps_per_side),
        "starting_equity": starting_equity,
        "ending_equity": current_equity,
        "compounded_return_fraction": current_equity / starting_equity - 1,
        "total_log_growth": sum(math.log1p(value) for value in daily_returns),
        "maximum_drawdown_fraction": maximum_drawdown_fraction(daily_returns),
        "account_path": account_path,
        "trial_accounting": trial_accounting,
        "closed_trades": closed_trades,
    }


def _shared_cost_scenarios(
    calendar: Sequence[str],
    candidates: Sequence[Mapping[str, Any]],
    policy: Mapping[str, Any],
    *,
    rolling_origin_plan: Sequence[Mapping[str, Any]] | None,
) -> dict[int, dict[str, Any]]:
    """Use the highest-cost path as the cost-blind contention decision surface."""

    evaluator = _scenario if rolling_origin_plan is None else _rolling_origin_scenario
    scope = calendar if rolling_origin_plan is None else rolling_origin_plan
    stress = evaluator(scope, candidates, policy, 20)
    selected = {
        str(item["signal_id"]) for item in stress["closed_trades"]
    }
    frozen_quantities = {
        str(item["signal_id"]): int(item["quantity"])
        for item in stress["closed_trades"]
    }
    scenarios = {
        cost: evaluator(
            scope,
            candidates,
            policy,
            cost,
            allowed_signal_ids=selected,
            quantity_by_signal_id=frozen_quantities,
        )
        for cost in (5, 10)
    }
    scenarios[20] = stress
    expected = selected
    if any(
        {str(item["signal_id"]) for item in scenario["closed_trades"]}
        != expected
        for scenario in scenarios.values()
    ):
        raise DenseStrategyRuntimeError(
            "shared stressed-cost contention did not preserve signal identity"
        )
    return scenarios


def _rolling_origin_scope(
    calendar: Sequence[str],
    plan: Sequence[Mapping[str, Any]],
) -> tuple[list[str], set[str]]:
    account_dates: list[str] = []
    entry_dates: list[str] = []
    for expected_fold, fold in enumerate(plan, 1):
        if not isinstance(fold, Mapping) or fold.get("fold") != expected_fold:
            raise DenseStrategyRuntimeError("rolling-origin fold ordering drifted")
        test = fold.get("test_dates")
        entries = fold.get("entry_dates")
        settlement = fold.get("settlement_only_dates")
        if not (
            isinstance(test, list)
            and isinstance(entries, list)
            and isinstance(settlement, list)
            and test == [*entries, *settlement]
            and entries
        ):
            raise DenseStrategyRuntimeError("rolling-origin evidence scope is invalid")
        account_dates.extend(test)
        entry_dates.extend(entries)
    if (
        account_dates != sorted(account_dates)
        or len(account_dates) != len(set(account_dates))
        or not set(account_dates).issubset(calendar)
        or entry_dates != sorted(entry_dates)
        or len(entry_dates) != len(set(entry_dates))
    ):
        raise DenseStrategyRuntimeError(
            "rolling-origin evidence dates drifted from the development calendar"
        )
    return account_dates, set(entry_dates)


def _trade_signal(
    candidate: Mapping[str, Any],
    trades: Mapping[int, Mapping[str, Any]],
    risk_fraction: float,
) -> dict[str, Any]:
    primary = trades[5]
    stress_10 = trades[10]
    stress_20 = trades[20]

    def net_r(trade: Mapping[str, Any]) -> float:
        planned = float(trade["quantity"]) * float(
            candidate["planned_stop_distance"]
        )
        return float(trade["net_pnl_dollars"]) / planned

    return {
        "date": primary["entry_date"],
        "closed_date": primary["exit_date"],
        "signal_id": primary["signal_id"],
        "primary_account_return_fraction": primary[
            "net_account_return_fraction"
        ],
        "stress_10bps_account_return_fraction": stress_10[
            "net_account_return_fraction"
        ],
        "stress_20bps_account_return_fraction": stress_20[
            "net_account_return_fraction"
        ],
        "net_account_log_growth": primary["log_growth"],
        "stress_10bps_account_log_growth": stress_10["log_growth"],
        "stress_20bps_account_log_growth": stress_20["log_growth"],
        "net_r": net_r(primary),
        "stress_10bps_r": net_r(stress_10),
        "stress_20bps_r": net_r(stress_20),
        "net_pnl_dollars": primary["net_pnl_dollars"],
        "stress_10bps_net_pnl_dollars": stress_10["net_pnl_dollars"],
        "stress_20bps_net_pnl_dollars": stress_20["net_pnl_dollars"],
        "stop_executed": bool(candidate["stop_executed"]),
        "risk_fraction": risk_fraction,
    }


def _maturity_rows(
    calendar: Sequence[str],
    candidates: Sequence[Mapping[str, Any]],
    scenarios: Mapping[int, Mapping[str, Any]],
    risk_fraction: float,
    *,
    missed_data_dates: set[str] | None = None,
) -> list[dict[str, Any]]:
    candidate_by_id = {str(item["signal_id"]): item for item in candidates}
    trade_maps = {
        cost: {str(item["signal_id"]): item for item in scenario["closed_trades"]}
        for cost, scenario in scenarios.items()
    }
    if not (set(trade_maps[5]) == set(trade_maps[10]) == set(trade_maps[20])):
        raise DenseStrategyRuntimeError(
            "cost scenarios selected different signals under frozen contention"
        )
    signals_by_exit: dict[str, list[dict[str, Any]]] = {}
    for signal_id, primary in trade_maps[5].items():
        signals_by_exit.setdefault(str(primary["exit_date"]), []).append(
            _trade_signal(
                candidate_by_id[signal_id],
                {cost: trade_maps[cost][signal_id] for cost in (5, 10, 20)},
                risk_fraction,
            )
        )
    paths = {
        cost: {str(item["date"]): item for item in scenario["account_path"]}
        for cost, scenario in scenarios.items()
    }
    return [
        {
            "date": day,
            "session_outcome": (
                "missed_data"
                if day in (missed_data_dates or set())
                else paths[5][day]["session_outcome"]
            ),
            "eligible_signal": bool(paths[5][day]["new_entries"]),
            "primary_account_return_fraction": paths[5][day][
                "daily_account_return_fraction"
            ],
            "stress_10bps_account_return_fraction": paths[10][day][
                "daily_account_return_fraction"
            ],
            "stress_20bps_account_return_fraction": paths[20][day][
                "daily_account_return_fraction"
            ],
            "signals": sorted(
                signals_by_exit.get(day, []), key=lambda item: item["signal_id"]
            ),
        }
        for day in calendar
    ]


def evaluate_trial(
    dataset: Mapping[str, Any],
    *,
    family_id: str,
    trial_id: str,
    parameters: Mapping[str, Any],
    account_policy: Mapping[str, Any],
    rolling_origin_plan: Sequence[Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    """Evaluate one trial at 5/10/20 bps with a shared chronological account path."""

    full_calendar = _calendar(dataset)
    candidates = build_candidates(dataset, family_id, parameters)
    missed_data_dates = (
        _intraday_missed_data_dates(dataset)
        if family_id in INTRADAY_ETF_FAMILIES
        else set()
    )
    if rolling_origin_plan is None:
        calendar = full_calendar
    else:
        calendar, entry_dates = _rolling_origin_scope(
            full_calendar, rolling_origin_plan
        )
        candidates = [
            item for item in candidates if item.get("signal_date") in entry_dates
        ]
    scenarios = _shared_cost_scenarios(
        calendar,
        candidates,
        account_policy,
        rolling_origin_plan=rolling_origin_plan,
    )
    stress = scenarios[20]
    daily_returns = [
        float(item["daily_account_return_fraction"])
        for item in stress["account_path"]
    ]
    filled_returns = [
        float(item["net_account_return_fraction"])
        for item in stress["closed_trades"]
    ]
    dollars = [float(item["net_pnl_dollars"]) for item in stress["closed_trades"]]
    risk_fraction = float(account_policy["risk_fraction"])
    stressed_pf = profit_factor(dollars)
    stressed_pf_is_infinite = stressed_pf == math.inf
    serialized_stressed_pf = (
        0.0
        if stressed_pf is None or stressed_pf_is_infinite
        else float(stressed_pf)
    )
    accounting = [
        {
            "date": item["date"],
            "outcome": (
                "zero_return_day"
                if float(item["daily_account_return_fraction"]) == 0
                else "account_return_day"
            ),
            "session_outcome": (
                "missed_data"
                if item["date"] in missed_data_dates
                else item["session_outcome"]
            ),
            "new_entries": item["new_entries"],
            "open_positions": item["open_positions"],
            "capital_blocked_signals": item["capital_blocked_signals"],
            "missed_fills": item["missed_fills"],
            "rejected_signals": item["rejected_signals"],
            **(
                {"zero_return_reason": "missed_data"}
                if item["date"] in missed_data_dates
                else {}
            ),
        }
        for item in stress["account_path"]
    ]
    if any(
        item["date"] in missed_data_dates
        and (
            float(item["daily_account_return_fraction"]) != 0
            or item["new_entries"]
        )
        for item in stress["account_path"]
    ):
        raise DenseStrategyRuntimeError(
            "intraday missed-data dates must remain explicit zero-return days"
        )
    return {
        "trial_id": trial_id,
        "parameters": dict(parameters),
        "metrics": {
            "stress_20bps_total_log_growth": stress["total_log_growth"],
            "stress_20bps_bootstrap_lower_mean_account_return": 0.0,
            "stress_20bps_profit_factor": serialized_stressed_pf,
            "stress_20bps_profit_factor_is_infinite": (
                stressed_pf_is_infinite
            ),
            "stress_20bps_maximum_drawdown_r": stress[
                "maximum_drawdown_fraction"
            ]
            / risk_fraction,
            "deflated_sharpe_probability": 0.0,
            "pbo_probability": 1.0,
            "holm_reject_null": False,
            "rolling_folds_positive": False,
            "rules_complete": True,
            "trial_accounting_complete": True,
            "oof_daily_account_returns": daily_returns,
            "oof_filled_account_returns": filled_returns,
            "oof_net_pnl_dollars": dollars,
            "risk_fraction": risk_fraction,
        },
        "trial_accounting": accounting,
        "maturity_rows": _maturity_rows(
            calendar,
            candidates,
            scenarios,
            risk_fraction,
            missed_data_dates=missed_data_dates,
        ),
        "candidate_accounting": [dict(item) for item in candidates],
        "scenarios": {
            f"{cost}bps": {
                "daily_account_returns": [
                    item["daily_account_return_fraction"]
                    for item in scenarios[cost]["account_path"]
                ],
                "filled_account_returns": [
                    item["net_account_return_fraction"]
                    for item in scenarios[cost]["closed_trades"]
                ],
                "net_pnl_dollars": [
                    item["net_pnl_dollars"]
                    for item in scenarios[cost]["closed_trades"]
                ],
                "total_log_growth": scenarios[cost]["total_log_growth"],
            }
            for cost in (5, 10, 20)
        },
    }

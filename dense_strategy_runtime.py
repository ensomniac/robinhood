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
from datetime import datetime, time, timedelta
from typing import Any

from learning_statistics import profit_factor, simulate_portfolio_account


EQUITY_RESIDUAL_FAMILY = "liquid-equity-market-residual-reversal"
INTRADAY_ETF_FAMILY = "intraday-index-etf-opening-reversal"
ETF_PULLBACK_FAMILY = "liquid-etf-trend-pullback-cost-floor"
SUPPORTED_FAMILIES = {
    EQUITY_RESIDUAL_FAMILY,
    INTRADAY_ETF_FAMILY,
    ETF_PULLBACK_FAMILY,
}
PRIMARY_ROUND_TRIP_COST_FRACTION = 0.001
MINIMUM_GROSS_TO_COST_MULTIPLE = 5.0
STANDARDIZATION_LOOKBACK = 60


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
    calendar = _calendar(dataset)
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
    if "SPY" not in daily:
        raise DenseStrategyRuntimeError("equity residual data needs SPY market bars")
    window = int(parameters["prior_return_sessions"])
    threshold = float(parameters["residual_z_threshold"])
    trend_period = 100 if parameters["market_trend_gate"] == "SPY>SMA100" else 200
    stop_atr = float(parameters["stop_atr14"])
    hold = int(parameters["hold_sessions"])
    indices = {
        symbol: {str(bar["date"]): index for index, bar in enumerate(bars)}
        for symbol, bars in daily.items()
    }
    cache: dict[int, dict[str, list[tuple[float, str, float, float]]]] | None = None
    if isinstance(dataset, dict) and "_prepared_daily_bars" in dataset:
        raw_cache = dataset.setdefault("_equity_residual_feature_cache", {})
        if isinstance(raw_cache, dict):
            cache = raw_cache
    features = cache.get(window) if cache is not None else None
    if features is None:
        evaluation_set = set(calendar[:-1])
        universe_sets: dict[str, set[str]] = {}
        for decision_date in calendar[:-1]:
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
        spy_bars = daily["SPY"]
        spy_returns: dict[str, float] = {}
        for index, bar in enumerate(spy_bars):
            if index >= window:
                spy_returns[str(bar["date"])] = (
                    float(bar["close"])
                    / float(spy_bars[index - window]["close"])
                    - 1
                )
        features = {day: [] for day in calendar[:-1]}
        symbols = sorted(set().union(*universe_sets.values()))
        for symbol in symbols:
            if symbol == "SPY":
                continue
            bars = daily.get(symbol)
            if bars is None:
                raise DenseStrategyRuntimeError(
                    f"point-in-time universe symbol {symbol} lacks daily history"
                )
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
                z_score = _z_score(residual, residual_history)
                residual_history.append(residual)
                if (
                    day not in evaluation_set
                    or symbol not in universe_sets[day]
                    or z_score is None
                    or symbol_index < 14
                ):
                    continue
                atr14 = statistics.fmean(
                    ranges[symbol_index - 13 : symbol_index + 1]
                )
                features[day].append((z_score, symbol, atr14, residual))
        if cache is not None:
            cache[window] = features
    candidates: list[dict[str, Any]] = []
    for calendar_index, decision_date in enumerate(calendar[:-1]):
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
            entry_index = indices[symbol].get(entry_date)
            if entry_index is None:
                candidates.append(
                    {
                        "signal_id": f"{entry_date}-{EQUITY_RESIDUAL_FAMILY}-{symbol}",
                        "signal_date": entry_date,
                        "decision_date": decision_date,
                        "symbol": symbol,
                        "outcome": "missed_fill",
                        "rank": rank,
                        "rejection_reason": "missing_next_open",
                    }
                )
                continue
            if entry_index + hold > len(daily[symbol]):
                continue
            exit_dates = {
                str(item["date"])
                for item in daily[symbol][entry_index : entry_index + hold]
            }
            expected_dates = set(
                calendar[calendar_index + 1 : calendar_index + 1 + hold]
            )
            if exit_dates != expected_dates:
                candidates.append(
                    {
                        "signal_id": f"{entry_date}-{EQUITY_RESIDUAL_FAMILY}-{symbol}",
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
                    family_id=EQUITY_RESIDUAL_FAMILY,
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
            )
    return candidates


def _etf_pullback_candidates(
    dataset: Mapping[str, Any], parameters: Mapping[str, Any]
) -> list[dict[str, Any]]:
    calendar = _calendar(dataset)
    daily = _daily_series(dataset)
    trend_period = int(parameters["trend_sma"])
    rsi_max = float(parameters["rsi2_maximum"])
    decline_floor = float(parameters["three_session_decline_fraction"])
    stop_atr = float(parameters["stop_atr14"])
    hold = int(parameters["maximum_hold_sessions"])
    indices = {
        symbol: {str(bar["date"]): index for index, bar in enumerate(bars)}
        for symbol, bars in daily.items()
    }
    candidates: list[dict[str, Any]] = []
    for calendar_index, decision_date in enumerate(calendar[:-1]):
        scored: list[tuple[float, float, str, float]] = []
        for symbol, bars in daily.items():
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
                trend is None
                or rsi2 is None
                or atr14 is None
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
                        "signal_id": f"{entry_date}-{ETF_PULLBACK_FAMILY}-{symbol}",
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
                        "signal_id": f"{entry_date}-{ETF_PULLBACK_FAMILY}-{symbol}",
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
                    family_id=ETF_PULLBACK_FAMILY,
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


def prepare_dataset(dataset: Mapping[str, Any]) -> dict[str, Any]:
    """Validate and normalize row-level bars once for every trial in a process."""

    family_id = dataset.get("family_id")
    if family_id not in SUPPORTED_FAMILIES:
        raise DenseStrategyRuntimeError("dataset family binding is unsupported")
    _calendar(dataset)
    prepared = dict(dataset)
    if family_id == INTRADAY_ETF_FAMILY:
        sessions = _minute_sessions(dataset)
        symbols = dataset.get("symbols")
        if not isinstance(symbols, list) or not symbols:
            raise DenseStrategyRuntimeError(
                "intraday dataset must name its complete frozen symbols"
            )
        expected_symbols = {str(symbol) for symbol in symbols}
        if any(set(day_symbols) != expected_symbols for day_symbols in sessions.values()):
            raise DenseStrategyRuntimeError(
                "intraday sessions do not cover the complete frozen universe"
            )
        prepared["_prepared_minute_bars"] = sessions
    else:
        daily = _daily_series(dataset)
        if family_id == ETF_PULLBACK_FAMILY:
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
    dataset: Mapping[str, Any], parameters: Mapping[str, Any]
) -> list[dict[str, Any]]:
    calendar = _calendar(dataset)
    sessions = _minute_sessions(dataset)
    if set(calendar) - set(sessions):
        raise DenseStrategyRuntimeError("minute_bars omit frozen evaluation dates")
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
            if day not in evaluation_dates:
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
        if day not in evaluation_dates or not qualified:
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
        signal_id = f"{day}-{INTRADAY_ETF_FAMILY}-{symbol}"
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


def build_candidates(
    dataset: Mapping[str, Any],
    family_id: str,
    parameters: Mapping[str, Any],
) -> list[dict[str, Any]]:
    """Build exact-rule candidates using only information observable at decision time."""

    if dataset.get("family_id") != family_id:
        raise DenseStrategyRuntimeError("dataset family binding does not match")
    if family_id == EQUITY_RESIDUAL_FAMILY:
        return _equity_residual_candidates(dataset, parameters)
    if family_id == INTRADAY_ETF_FAMILY:
        return _intraday_candidates(dataset, parameters)
    if family_id == ETF_PULLBACK_FAMILY:
        return _etf_pullback_candidates(dataset, parameters)
    raise DenseStrategyRuntimeError(f"unsupported dense family: {family_id}")


def _production_equity_universe(
    daily: Mapping[str, Sequence[Mapping[str, Any]]],
    references: Any,
    decision_date: str,
    calendar_dates: Sequence[str],
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
    for raw_symbol, raw_reference in references.items():
        symbol = str(raw_symbol)
        if symbol == "SPY" or not isinstance(raw_reference, Mapping):
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
    if family_id == EQUITY_RESIDUAL_FAMILY:
        expected.update(
            {
                "reference_snapshot",
                "reference_snapshot_complete",
                "reference_source_total",
            }
        )
    else:
        expected.add("symbols")
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
    if family_id == ETF_PULLBACK_FAMILY:
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
    if family_id != EQUITY_RESIDUAL_FAMILY:
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


def _production_intraday_signal(
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
        or decision_data.get("family_id") != INTRADAY_ETF_FAMILY
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


def evaluate_production_signal(
    decision_data: Mapping[str, Any],
    *,
    family_id: str,
    parameters: Mapping[str, Any],
    frozen_universe: Mapping[str, Any],
) -> dict[str, Any]:
    """Rebuild one current signal from the same observable indicators as history."""
    if family_id == INTRADAY_ETF_FAMILY:
        return _production_intraday_signal(
            decision_data, parameters, frozen_universe
        )
    if family_id in {EQUITY_RESIDUAL_FAMILY, ETF_PULLBACK_FAMILY}:
        return _production_daily_signal(
            decision_data, family_id, parameters, frozen_universe
        )
    raise DenseStrategyRuntimeError("unsupported production strategy family")


def _scenario(
    calendar: Sequence[str],
    candidates: Sequence[Mapping[str, Any]],
    policy: Mapping[str, Any],
    cost_bps_per_side: int,
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
    )


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
            "session_outcome": paths[5][day]["session_outcome"],
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
) -> dict[str, Any]:
    """Evaluate one trial at 5/10/20 bps with a shared chronological account path."""

    calendar = _calendar(dataset)
    candidates = build_candidates(dataset, family_id, parameters)
    scenarios = {
        cost: _scenario(calendar, candidates, account_policy, cost)
        for cost in (5, 10, 20)
    }
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
    accounting = [
        {
            "date": item["date"],
            "outcome": (
                "zero_return_day"
                if float(item["daily_account_return_fraction"]) == 0
                else "account_return_day"
            ),
            "session_outcome": item["session_outcome"],
            "new_entries": item["new_entries"],
            "open_positions": item["open_positions"],
            "capital_blocked_signals": item["capital_blocked_signals"],
            "missed_fills": item["missed_fills"],
            "rejected_signals": item["rejected_signals"],
        }
        for item in stress["account_path"]
    ]
    return {
        "trial_id": trial_id,
        "parameters": dict(parameters),
        "metrics": {
            "stress_20bps_total_log_growth": stress["total_log_growth"],
            "stress_20bps_bootstrap_lower_mean_account_return": 0.0,
            "stress_20bps_profit_factor": stressed_pf or 0.0,
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
            calendar, candidates, scenarios, risk_fraction
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

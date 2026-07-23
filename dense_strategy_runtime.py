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
    candidates: list[dict[str, Any]] = []
    for calendar_index, decision_date in enumerate(calendar[:-1]):
        spy_index = indices["SPY"].get(decision_date)
        if spy_index is None or spy_index < max(window, trend_period - 1):
            continue
        spy_bars = daily["SPY"]
        spy_return = (
            float(spy_bars[spy_index]["close"])
            / float(spy_bars[spy_index - window]["close"])
            - 1
        )
        spy_sma = _sma(spy_bars, spy_index, trend_period)
        trend_ready = spy_sma is not None and float(
            spy_bars[spy_index]["close"]
        ) > spy_sma
        day_universe = universe.get(decision_date)
        if not isinstance(day_universe, list):
            raise DenseStrategyRuntimeError(
                f"universe_by_date is missing frozen date {decision_date}"
            )
        day_identities = identities.get(decision_date)
        if (
            not isinstance(day_identities, Mapping)
            or set(map(str, day_universe)) != set(map(str, day_identities))
            or len(set(map(str, day_identities.values()))) != len(day_identities)
        ):
            raise DenseStrategyRuntimeError(
                f"point-in-time identities are incomplete on {decision_date}"
            )
        scored: list[tuple[float, str, float]] = []
        for raw_symbol in day_universe:
            symbol = str(raw_symbol)
            if symbol == "SPY":
                continue
            if symbol not in daily:
                raise DenseStrategyRuntimeError(
                    f"point-in-time universe symbol {symbol} lacks daily history"
                )
            symbol_index = indices[symbol].get(decision_date)
            if symbol_index is None or symbol_index < window:
                continue
            bars = daily[symbol]
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
                prior_symbol_return = (
                    float(bars[prior_symbol_index]["close"])
                    / float(bars[prior_symbol_index - window]["close"])
                    - 1
                )
                prior_spy_return = (
                    float(spy_bars[prior_spy_index]["close"])
                    / float(spy_bars[prior_spy_index - window]["close"])
                    - 1
                )
                history.append(prior_symbol_return - prior_spy_return)
            z_score = _z_score(residual, history)
            if (
                not trend_ready
                or z_score is None
                or z_score > threshold
                or not _cost_floor(abs(residual))
            ):
                continue
            atr14 = _atr(bars, symbol_index)
            if atr14 is not None:
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
        scored: list[tuple[float, str, int, float]] = []
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
            numerator = sum(float(item["vwap_numerator"]) for item in bars[:opening_window])
            denominator = sum(float(item["vwap_denominator"]) for item in bars[:opening_window])
            above = 0
            reclaim_index: int | None = None
            for index in range(opening_window, len(bars) - 1):
                numerator += float(bars[index]["vwap_numerator"])
                denominator += float(bars[index]["vwap_denominator"])
                if denominator <= 0:
                    above = 0
                    continue
                exact_vwap = numerator / denominator
                above = above + 1 if float(bars[index]["close"]) > exact_vwap else 0
                if above >= reclaim_bars:
                    reclaim_index = index
                    break
            if reclaim_index is None:
                continue
            atr = _intraday_atr(bars, reclaim_index)
            if atr is not None:
                scored.append((z_score, symbol, reclaim_index + 1, atr))
        for rank, (z_score, symbol, entry_index, atr) in enumerate(sorted(scored), 1):
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
                        "rank": rank,
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
                        "rank": rank,
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
                    "rank": rank,
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

"""Versioned, research-only strategy plugins for cached historical bundles.

The plugins deliberately consume immutable one-minute bar views. They do not
import production ledger, maturity, trade-archive, or broker surfaces. A signal
is formed from a completed bar and is filled at the following bar's open by
``historical_research``.
"""

from __future__ import annotations

import importlib
import hashlib
import inspect
import math
import re
from dataclasses import asdict, dataclass
from typing import Any, Mapping, Protocol, Sequence, runtime_checkable


PLUGIN_ID_PATTERN = re.compile(r"^[a-z0-9][a-z0-9-]*$")


class ResearchStrategyError(RuntimeError):
    """Raised when a strategy plugin or input violates the research contract."""


@dataclass(frozen=True, slots=True)
class Bar:
    """Immutable regular-session one-minute OHLCV bar."""

    time_et: str
    open: float
    high: float
    low: float
    close: float
    volume: float
    interpolated: bool = False


@dataclass(frozen=True, slots=True)
class CandidateContext:
    """The intentionally small, immutable view exposed to a strategy plugin."""

    date: str
    symbol: str
    signal_id: str
    bars: tuple[Bar, ...]


@dataclass(frozen=True, slots=True)
class DataRequirements:
    """Machine-readable data contract declared by every plugin."""

    regular_session_one_minute_bars: bool = True
    complete_regular_session: bool = True
    real_bars_only: bool = True
    historical_top_of_book: bool = False
    historical_depth: bool = False
    subminute_trades: bool = False
    benchmark_bars: bool = False


@dataclass(frozen=True, slots=True)
class SignalDecision:
    """A decision made after ``signal_index`` is fully known."""

    signal_index: int
    technical_stop: float
    strength: float
    reason: str
    max_entry_price: float | None = None


@dataclass(frozen=True, slots=True)
class SignalSearch:
    """First qualifying decision or a stable no-signal reason."""

    decision: SignalDecision | None
    reason: str


@runtime_checkable
class StrategyPlugin(Protocol):
    """Protocol for built-in and external point-in-time strategies."""

    strategy_id: str
    version: str
    description: str
    requirements: DataRequirements

    def find_signal(self, candidate: CandidateContext) -> SignalSearch:
        """Evaluate the progressively revealed bar prefix supplied by the runner."""


def _opening_range(bars: Sequence[Bar]) -> tuple[float, float, float, float]:
    opening = bars[:5]
    return (
        opening[0].open,
        max(bar.high for bar in opening),
        min(bar.low for bar in opening),
        opening[-1].close,
    )


def running_vwap(bars: Sequence[Bar]) -> tuple[float, ...]:
    """Return point-in-time VWAP values using only each bar and its prefix."""

    notional = 0.0
    volume = 0.0
    values: list[float] = []
    for bar in bars:
        typical = (bar.high + bar.low + bar.close) / 3.0
        if bar.volume > 0:
            notional += typical * bar.volume
            volume += bar.volume
        values.append(notional / volume if volume else bar.close)
    return tuple(values)


def _buffered_stop(reference: float) -> float:
    return reference * (1.0 - 0.0005)


def _signal_indexes(bars: Sequence[Bar], *, start: int = 5) -> range:
    # A 10:29 signal is completed at 10:30 and can enter on the 10:30 bar.
    end = next(
        (index for index, bar in enumerate(bars) if bar.time_et >= "10:30:00"),
        len(bars),
    )
    return range(start, min(end, len(bars)))


class OpeningRangeBreakout:
    strategy_id = "orb-5m-research"
    version = "1.0.0"
    description = (
        "Bullish five-minute opening range; first completed close above the "
        "range high and running VWAP, with a 0.15% next-open chase cap."
    )
    requirements = DataRequirements()

    def find_signal(self, candidate: CandidateContext) -> SignalSearch:
        bars = candidate.bars
        opening_open, opening_high, _opening_low, opening_close = _opening_range(bars)
        if opening_close <= opening_open:
            return SignalSearch(None, "opening_range_not_bullish")
        vwaps = running_vwap(bars)
        for index in _signal_indexes(bars):
            bar = bars[index]
            previous = bars[index - 1]
            if bar.close <= opening_high or previous.close > opening_high:
                continue
            if bar.close <= vwaps[index]:
                return SignalSearch(None, "first_breakout_below_vwap")
            prior = bars[max(0, index - 5) : index]
            average_volume = sum(value.volume for value in prior) / len(prior)
            volume_ratio = bar.volume / max(1.0, average_volume)
            strength = (bar.close / opening_high - 1.0) * 10_000 + volume_ratio
            return SignalSearch(
                SignalDecision(
                    signal_index=index,
                    technical_stop=_buffered_stop(min(bar.low, vwaps[index])),
                    strength=strength,
                    reason="completed_close_above_opening_range_and_vwap",
                    max_entry_price=opening_high * 1.0015,
                ),
                "signal",
            )
        return SignalSearch(None, "no_completed_opening_range_breakout")


class VwapPullback:
    strategy_id = "vwap-pullback"
    version = "1.0.0"
    description = (
        "Post-open bullish pullback that touches and reclaims running VWAP, "
        "closes above the prior high, and follows recent above-VWAP trade."
    )
    requirements = DataRequirements()

    def find_signal(self, candidate: CandidateContext) -> SignalSearch:
        bars = candidate.bars
        vwaps = running_vwap(bars)
        for index in _signal_indexes(bars, start=10):
            bar = bars[index]
            prior = bars[index - 5 : index]
            recently_above = (
                sum(
                    value.close > vwaps[index - 5 + offset]
                    for offset, value in enumerate(prior)
                )
                >= 3
            )
            touched_vwap = bar.low <= vwaps[index] * 1.001
            reclaimed = bar.close > vwaps[index]
            bullish_confirmation = (
                bar.close > bar.open and bar.close > bars[index - 1].high
            )
            if recently_above and touched_vwap and reclaimed and bullish_confirmation:
                distance = (bar.close / vwaps[index] - 1.0) * 10_000
                return SignalSearch(
                    SignalDecision(
                        signal_index=index,
                        technical_stop=_buffered_stop(
                            min(bar.low, bars[index - 1].low, vwaps[index])
                        ),
                        strength=distance,
                        reason="bullish_vwap_touch_and_reclaim",
                    ),
                    "signal",
                )
        return SignalSearch(None, "no_bullish_vwap_pullback_reclaim")


class HighOfDayContinuation:
    strategy_id = "hod-continuation"
    version = "1.0.0"
    description = (
        "Completed bullish close above the prior session high of day and running "
        "VWAP on at least 1.5x the preceding five-minute average volume."
    )
    requirements = DataRequirements()

    def find_signal(self, candidate: CandidateContext) -> SignalSearch:
        bars = candidate.bars
        vwaps = running_vwap(bars)
        for index in _signal_indexes(bars, start=10):
            bar = bars[index]
            prior_high = max(value.high for value in bars[:index])
            prior_volume = sum(value.volume for value in bars[index - 5 : index]) / 5.0
            volume_ratio = bar.volume / max(1.0, prior_volume)
            if (
                bar.close > prior_high
                and bar.close > vwaps[index]
                and bar.close > bar.open
                and volume_ratio >= 1.5
            ):
                strength = (bar.close / prior_high - 1.0) * 10_000 + volume_ratio
                return SignalSearch(
                    SignalDecision(
                        signal_index=index,
                        technical_stop=_buffered_stop(min(bar.low, vwaps[index])),
                        strength=strength,
                        reason="high_of_day_close_with_volume_confirmation",
                    ),
                    "signal",
                )
        return SignalSearch(None, "no_volume_confirmed_high_of_day_close")


class OpeningReversal:
    strategy_id = "opening-reversal"
    version = "1.0.0"
    description = (
        "Research counterfactual for a red opening range: a later bullish close "
        "reclaims both its midpoint and running VWAP."
    )
    requirements = DataRequirements()

    def find_signal(self, candidate: CandidateContext) -> SignalSearch:
        bars = candidate.bars
        opening_open, opening_high, opening_low, opening_close = _opening_range(bars)
        if opening_close >= opening_open:
            return SignalSearch(None, "opening_range_not_bearish")
        midpoint = (opening_high + opening_low) / 2.0
        vwaps = running_vwap(bars)
        for index in _signal_indexes(bars):
            bar = bars[index]
            previous = bars[index - 1]
            threshold = max(midpoint, vwaps[index])
            crossed = previous.close <= max(midpoint, vwaps[index - 1])
            if bar.close > threshold and bar.close > bar.open and crossed:
                strength = (bar.close / threshold - 1.0) * 10_000
                return SignalSearch(
                    SignalDecision(
                        signal_index=index,
                        technical_stop=_buffered_stop(
                            min(bar.low, previous.low, opening_low)
                        ),
                        strength=strength,
                        reason="bullish_reclaim_of_opening_midpoint_and_vwap",
                    ),
                    "signal",
                )
        return SignalSearch(None, "no_opening_reversal_reclaim")


BUILTIN_STRATEGIES: dict[str, StrategyPlugin] = {
    plugin.strategy_id: plugin
    for plugin in (
        OpeningRangeBreakout(),
        VwapPullback(),
        HighOfDayContinuation(),
        OpeningReversal(),
    )
}


def validate_plugin(plugin: Any) -> StrategyPlugin:
    """Validate a plugin object before it can enter a research run."""

    if not isinstance(plugin, StrategyPlugin):
        raise ResearchStrategyError(
            "strategy plugin must expose strategy_id, version, description, "
            "requirements, and find_signal(candidate)"
        )
    if not PLUGIN_ID_PATTERN.fullmatch(plugin.strategy_id):
        raise ResearchStrategyError(f"invalid strategy_id: {plugin.strategy_id!r}")
    if not isinstance(plugin.version, str) or not plugin.version.strip():
        raise ResearchStrategyError(f"{plugin.strategy_id} has no version")
    if not isinstance(plugin.description, str) or not plugin.description.strip():
        raise ResearchStrategyError(f"{plugin.strategy_id} has no description")
    if not isinstance(plugin.requirements, DataRequirements):
        raise ResearchStrategyError(
            f"{plugin.strategy_id} requirements must be DataRequirements"
        )
    return plugin


def load_strategy(spec: str) -> StrategyPlugin:
    """Load a built-in id or an external ``module:attribute`` plugin."""

    if spec in BUILTIN_STRATEGIES:
        return validate_plugin(BUILTIN_STRATEGIES[spec])
    if ":" not in spec:
        available = ", ".join(sorted(BUILTIN_STRATEGIES))
        raise ResearchStrategyError(
            f"unknown strategy {spec!r}; built-ins: {available}; external plugins "
            "use module:attribute"
        )
    module_name, attribute = spec.rsplit(":", 1)
    if not module_name or not attribute:
        raise ResearchStrategyError(f"invalid external plugin spec: {spec!r}")
    try:
        plugin = getattr(importlib.import_module(module_name), attribute)
    except (ImportError, AttributeError) as exc:
        raise ResearchStrategyError(f"cannot load strategy {spec!r}: {exc}") from exc
    if isinstance(plugin, type):
        plugin = plugin()
    return validate_plugin(plugin)


def plugin_manifest(spec: str) -> dict[str, Any]:
    plugin = load_strategy(spec)
    module = inspect.getmodule(plugin.__class__)
    source_path = inspect.getsourcefile(plugin.__class__)
    if source_path:
        with open(source_path, "rb") as stream:
            implementation = stream.read()
    else:
        implementation = inspect.getsource(plugin.__class__).encode("utf-8")
    return {
        "load_spec": spec,
        "strategy_id": plugin.strategy_id,
        "version": plugin.version,
        "description": plugin.description,
        "requirements": asdict(plugin.requirements),
        "implementation": {
            "module": module.__name__ if module else plugin.__class__.__module__,
            "sha256": hashlib.sha256(implementation).hexdigest(),
        },
    }


def parse_bars(raw_bars: Any) -> tuple[Bar, ...]:
    """Validate and freeze the complete regular-session bar payload."""

    if not isinstance(raw_bars, list) or len(raw_bars) != 390:
        raise ResearchStrategyError("expected exactly 390 regular-session minute bars")
    bars: list[Bar] = []
    expected_minutes = 9 * 60 + 30
    for index, raw in enumerate(raw_bars):
        if not isinstance(raw, Mapping):
            raise ResearchStrategyError(f"bar {index} must be an object")
        time_et = str(raw.get("time_et", ""))
        hour = expected_minutes // 60
        minute = expected_minutes % 60
        expected_time = f"{hour:02d}:{minute:02d}:00"
        if time_et != expected_time:
            raise ResearchStrategyError(
                f"bar {index} time {time_et!r} does not match {expected_time}"
            )
        expected_minutes += 1
        try:
            open_price = float(raw["open"])
            high = float(raw["high"])
            low = float(raw["low"])
            close = float(raw["close"])
            volume = float(raw["volume"])
        except (KeyError, TypeError, ValueError) as exc:
            raise ResearchStrategyError(f"bar {index} has invalid OHLCV") from exc
        values = (open_price, high, low, close, volume)
        if not all(math.isfinite(value) for value in values):
            raise ResearchStrategyError(f"bar {index} contains non-finite OHLCV")
        if min(open_price, high, low, close) <= 0 or volume < 0:
            raise ResearchStrategyError(f"bar {index} has non-positive price or volume")
        if high < max(open_price, close) or low > min(open_price, close) or high < low:
            raise ResearchStrategyError(f"bar {index} has inconsistent OHLC values")
        if bool(raw.get("interpolated", False)):
            raise ResearchStrategyError(f"bar {index} is interpolated")
        bars.append(
            Bar(
                time_et=time_et,
                open=open_price,
                high=high,
                low=low,
                close=close,
                volume=volume,
                interpolated=False,
            )
        )
    return tuple(bars)


def plugin_manifest_hash_input(specs: Sequence[str]) -> list[dict[str, Any]]:
    """Stable helper kept separate for runner and determinism tests."""

    return [plugin_manifest(spec) for spec in specs]

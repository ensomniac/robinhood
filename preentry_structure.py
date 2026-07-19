"""Outcome-blind pre-entry stop/noise and resistance definitions for ORB v3."""

from __future__ import annotations

import math
import statistics
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import date, datetime, time, timedelta
from typing import Any
from zoneinfo import ZoneInfo


EASTERN = ZoneInfo("America/New_York")
CONTRACT_VERSION = "preentry-structure-v1"
MAXIMUM_NOISE_BARS = 60
REQUIRED_OPENING_BARS = 5
REQUIRED_RECENT_DAILY_BARS = 14
REQUIRED_LONG_DAILY_BARS = 252
SOURCE_URLS = (
    "https://papers.ssrn.com/sol3/papers.cfm?abstract_id=4729284",
    "https://arxiv.org/abs/2101.07410",
    "https://papers.ssrn.com/sol3/papers.cfm?abstract_id=1572269",
)


class PreentryStructureError(ValueError):
    """A point-in-time structure input is missing, malformed, or lookahead-prone."""


@dataclass(frozen=True)
class StopStructure:
    opening_range_high: float
    average_close_increment: float
    median_spread_dollars: float
    normal_noise_dollars: float
    technical_invalidation: float
    atr_stop_distance: float
    stop_distance: float
    planned_stop: float
    stop_fraction: float
    stop_outside_noise: bool
    maximum_stop_fraction_pass: bool
    completed_noise_bars: int


@dataclass(frozen=True)
class ResistanceStructure:
    status: str
    resistance_price: float | None
    resistance_source: str | None
    resistance_room_fraction: float | None
    minimum_room_pass: bool | None
    premarket_window_complete: bool
    premarket_observation_count: int
    daily_history_count: int
    daily_history_complete: bool
    daily_split_basis_verified: bool
    known_overhead_levels: tuple[tuple[str, float], ...]


@dataclass(frozen=True)
class PreentryStructure:
    contract_version: str
    observation_at: str
    entry_limit: float
    stop: StopStructure
    resistance: ResistanceStructure

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _positive(value: Any, field: str) -> float:
    if isinstance(value, bool):
        raise PreentryStructureError(f"{field} must be positive and finite")
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise PreentryStructureError(f"{field} must be positive and finite") from exc
    if not math.isfinite(result) or result <= 0:
        raise PreentryStructureError(f"{field} must be positive and finite")
    return result


def _observed(value: Any, field: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as exc:
        raise PreentryStructureError(f"{field} must be an ISO timestamp") from exc
    if parsed.tzinfo is None:
        raise PreentryStructureError(f"{field} must include a timezone")
    return parsed.astimezone(EASTERN)


def _validated_intraday_bars(
    rows: Sequence[Mapping[str, Any]],
    observation: datetime,
    *,
    premarket: bool,
) -> list[dict[str, Any]]:
    validated: list[dict[str, Any]] = []
    seen: set[datetime] = set()
    for index, row in enumerate(rows):
        observed = _observed(row.get("time_et"), f"bars[{index}].time_et")
        if observed in seen:
            raise PreentryStructureError("intraday bars contain duplicate minutes")
        seen.add(observed)
        if observed.date() != observation.date():
            raise PreentryStructureError("intraday bar date differs from observation")
        if observed + timedelta(minutes=1) > observation:
            raise PreentryStructureError("intraday bar was not complete at observation")
        if premarket:
            if not time(4, 0) <= observed.time() < time(9, 30):
                raise PreentryStructureError("premarket bar is outside 04:00-09:30 ET")
        elif not time(9, 30) <= observed.time() < time(16, 0):
            raise PreentryStructureError("regular bar is outside regular hours")
        prices = {
            field: _positive(row.get(field), f"bars[{index}].{field}")
            for field in ("open", "high", "low", "close")
        }
        if prices["low"] > min(prices["open"], prices["close"]) or prices["high"] < max(
            prices["open"], prices["close"]
        ):
            raise PreentryStructureError("intraday bar contains inconsistent OHLC")
        validated.append({"observed": observed, **prices})
    validated.sort(key=lambda row: row["observed"])
    return validated


def _opening_and_noise(
    rows: Sequence[Mapping[str, Any]], observation: datetime
) -> tuple[float, float, int]:
    bars = _validated_intraday_bars(rows, observation, premarket=False)
    expected = [
        datetime.combine(observation.date(), time(9, 30), tzinfo=EASTERN)
        + timedelta(minutes=index)
        for index in range(REQUIRED_OPENING_BARS)
    ]
    by_time = {row["observed"]: row for row in bars}
    if any(moment not in by_time for moment in expected):
        raise PreentryStructureError("the exact five opening minutes are required")
    available = [row for row in bars if row["observed"] < observation]
    expected_available = {
        datetime.combine(observation.date(), time(9, 30), tzinfo=EASTERN)
        + timedelta(minutes=index)
        for index in range(
            int(
                (
                    observation
                    - datetime.combine(observation.date(), time(9, 30), tzinfo=EASTERN)
                ).total_seconds()
                // 60
            )
        )
    }
    if {row["observed"] for row in available} != expected_available:
        raise PreentryStructureError(
            "completed regular-minute history must be contiguous from 09:30 ET"
        )
    window = available[-MAXIMUM_NOISE_BARS:]
    if len(window) < REQUIRED_OPENING_BARS:
        raise PreentryStructureError(
            "at least five completed regular bars are required"
        )
    closes = [float(row["close"]) for row in window]
    increments = [abs(current - prior) for prior, current in zip(closes, closes[1:])]
    average_increment = statistics.fmean(increments) if increments else 0.0
    opening_high = max(float(by_time[moment]["high"]) for moment in expected)
    return opening_high, average_increment, len(window)


def derive_stop_structure(
    *,
    observation_at: datetime,
    entry_limit: Any,
    daily_atr_14: Any,
    median_spread_dollars: Any,
    completed_regular_bars: Sequence[Mapping[str, Any]],
    atr_stop_fraction: float = 0.10,
    maximum_stop_fraction: float = 0.008,
) -> StopStructure:
    observation = _observed(observation_at.isoformat(), "observation_at")
    entry = _positive(entry_limit, "entry_limit")
    atr = _positive(daily_atr_14, "daily_atr_14")
    spread = _positive(median_spread_dollars, "median_spread_dollars")
    opening_high, average_increment, bar_count = _opening_and_noise(
        completed_regular_bars, observation
    )
    if entry < opening_high:
        raise PreentryStructureError(
            "entry_limit must be at or above opening-range high"
        )
    noise = max(spread, average_increment)
    invalidation = opening_high - noise
    if invalidation <= 0:
        raise PreentryStructureError("noise zone produces a nonpositive invalidation")
    atr_distance = _positive(atr_stop_fraction, "atr_stop_fraction") * atr
    distance = max(atr_distance, entry - invalidation)
    planned = entry - distance
    if planned <= 0:
        raise PreentryStructureError("planned stop must remain positive")
    fraction = distance / entry
    return StopStructure(
        opening_range_high=opening_high,
        average_close_increment=average_increment,
        median_spread_dollars=spread,
        normal_noise_dollars=noise,
        technical_invalidation=invalidation,
        atr_stop_distance=atr_distance,
        stop_distance=distance,
        planned_stop=planned,
        stop_fraction=fraction,
        stop_outside_noise=planned <= invalidation,
        maximum_stop_fraction_pass=(
            fraction <= _positive(maximum_stop_fraction, "maximum_stop_fraction")
        ),
        completed_noise_bars=bar_count,
    )


def _daily_highs(
    rows: Sequence[Mapping[str, Any]], observation: datetime
) -> list[float]:
    dated: list[tuple[str, float]] = []
    seen: set[str] = set()
    for index, row in enumerate(rows):
        day = str(row.get("date_et") or row.get("date") or "")
        try:
            parsed = date.fromisoformat(day)
        except ValueError as exc:
            raise PreentryStructureError(
                f"daily_bars[{index}] needs an ISO date"
            ) from exc
        normalized = parsed.isoformat()
        if parsed >= observation.date():
            raise PreentryStructureError(
                "daily resistance history must end before the observation date"
            )
        if normalized in seen:
            raise PreentryStructureError("daily bars contain duplicate dates")
        seen.add(normalized)
        dated.append(
            (normalized, _positive(row.get("high"), f"daily_bars[{index}].high"))
        )
    dated.sort(key=lambda item: item[0])
    return [high for _day, high in dated]


def derive_resistance_structure(
    *,
    observation_at: datetime,
    entry_limit: Any,
    premarket_bars: Sequence[Mapping[str, Any]],
    premarket_window_complete: bool,
    target_adjusted_daily_bars: Sequence[Mapping[str, Any]],
    daily_history_complete: bool,
    daily_split_basis_verified: bool,
    minimum_room_fraction: float = 0.022,
) -> ResistanceStructure:
    observation = _observed(observation_at.isoformat(), "observation_at")
    entry = _positive(entry_limit, "entry_limit")
    if premarket_window_complete is not True:
        premarket = []
    else:
        premarket = _validated_intraday_bars(
            premarket_bars, observation, premarket=True
        )
    minimum_room = _positive(minimum_room_fraction, "minimum_room_fraction")
    highs = _daily_highs(target_adjusted_daily_bars, observation)
    if daily_history_complete is True and len(highs) != REQUIRED_LONG_DAILY_BARS:
        raise PreentryStructureError(
            "complete daily history must contain exactly 252 prior sessions"
        )
    candidates: list[tuple[str, float]] = []
    if premarket:
        candidates.append(
            ("target_premarket_high", max(row["high"] for row in premarket))
        )
    if highs and daily_split_basis_verified is True:
        if daily_history_complete is True:
            candidates.extend(
                (
                    ("prior_session_high", highs[-1]),
                    ("prior_14_session_high", max(highs[-14:])),
                    ("prior_252_session_high", max(highs)),
                )
            )
        else:
            # Partial same-symbol history cannot prove price discovery, but an
            # observed adjusted high remains valid adverse evidence.
            candidates.append(("observed_partial_history_high", max(highs)))
    overhead = tuple(
        sorted(
            ((source, price) for source, price in candidates if price > entry),
            key=lambda item: (item[1], item[0]),
        )
    )
    inputs_complete = (
        premarket_window_complete is True
        and daily_history_complete is True
        and daily_split_basis_verified is True
        and len(highs) == REQUIRED_LONG_DAILY_BARS
    )
    nearest = overhead[0] if overhead else None
    if nearest is not None:
        source, price = nearest
        room = (price - entry) / entry
        if room < minimum_room:
            status = "BLOCKED_KNOWN_OVERHEAD"
            passed: bool | None = False
        elif inputs_complete:
            status = "RESOLVED_OVERHEAD"
            passed = True
        else:
            status = "UNRESOLVED_INPUT"
            passed = None
        resistance_price: float | None = price
        resistance_source: str | None = source
    elif inputs_complete:
        status = "RESOLVED_PRICE_DISCOVERY"
        resistance_price = None
        resistance_source = None
        room = None
        passed = True
    else:
        status = "UNRESOLVED_INPUT"
        resistance_price = None
        resistance_source = None
        room = None
        passed = None
    return ResistanceStructure(
        status=status,
        resistance_price=resistance_price,
        resistance_source=resistance_source,
        resistance_room_fraction=room,
        minimum_room_pass=passed,
        premarket_window_complete=premarket_window_complete is True,
        premarket_observation_count=len(premarket),
        daily_history_count=len(highs),
        daily_history_complete=daily_history_complete is True,
        daily_split_basis_verified=daily_split_basis_verified is True,
        known_overhead_levels=overhead,
    )


def derive_preentry_structure(
    *,
    observation_at: datetime,
    entry_limit: Any,
    daily_atr_14: Any,
    median_spread_dollars: Any,
    completed_regular_bars: Sequence[Mapping[str, Any]],
    premarket_bars: Sequence[Mapping[str, Any]],
    premarket_window_complete: bool,
    target_adjusted_daily_bars: Sequence[Mapping[str, Any]],
    daily_history_complete: bool,
    daily_split_basis_verified: bool,
    atr_stop_fraction: float = 0.10,
    maximum_stop_fraction: float = 0.008,
    minimum_room_fraction: float = 0.022,
) -> PreentryStructure:
    observation = _observed(observation_at.isoformat(), "observation_at")
    entry = _positive(entry_limit, "entry_limit")
    stop = derive_stop_structure(
        observation_at=observation,
        entry_limit=entry,
        daily_atr_14=daily_atr_14,
        median_spread_dollars=median_spread_dollars,
        completed_regular_bars=completed_regular_bars,
        atr_stop_fraction=atr_stop_fraction,
        maximum_stop_fraction=maximum_stop_fraction,
    )
    resistance = derive_resistance_structure(
        observation_at=observation,
        entry_limit=entry,
        premarket_bars=premarket_bars,
        premarket_window_complete=premarket_window_complete,
        target_adjusted_daily_bars=target_adjusted_daily_bars,
        daily_history_complete=daily_history_complete,
        daily_split_basis_verified=daily_split_basis_verified,
        minimum_room_fraction=minimum_room_fraction,
    )
    return PreentryStructure(
        contract_version=CONTRACT_VERSION,
        observation_at=observation.isoformat(),
        entry_limit=entry,
        stop=stop,
        resistance=resistance,
    )

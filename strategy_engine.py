"""Deterministic evaluation for the production five-minute ORB strategy.

The broker and market-data tools remain authoritative. This module turns their
already-collected values into one reproducible decision; it never fetches data
or places an order.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import statistics
import tomllib
from dataclasses import asdict, dataclass
from datetime import time
from pathlib import Path
from typing import Any, Mapping, Sequence


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "strategy_config.toml"


class StrategyInputError(ValueError):
    """Raised when an evaluation payload cannot be evaluated safely."""


@dataclass(frozen=True)
class StrategyConfig:
    raw: Mapping[str, Any]
    rules_hash: str

    @property
    def version(self) -> str:
        return str(self.raw["strategy"]["version"])

    def maturity(self, state: str) -> Mapping[str, Any]:
        try:
            return self.raw["maturity"][state]
        except KeyError as exc:
            raise StrategyInputError(f"Unknown maturity state: {state}") from exc


@dataclass(frozen=True)
class QuoteSummary:
    median_spread_fraction: float
    maximum_spread_fraction: float
    median_spread_dollars: float
    minimum_ask_depth: int
    minimum_recent_volume: int
    snapshots_clean: bool
    failures: tuple[str, ...]


@dataclass(frozen=True)
class SizingResult:
    entry_limit: float
    stop_distance: float
    stop_fraction: float
    planned_stop: float
    slippage_reserve_per_share: float
    risk_per_share: float
    risk_budget: float
    q_risk: int
    q_allocation: int
    q_liquidity: int
    quantity: int
    binding_caps: tuple[str, ...]
    notional: float
    buying_power_fraction: float
    allocation_target_met: bool
    resistance_room_fraction: float
    reward_risk: float
    milestone_price: float


@dataclass(frozen=True)
class EvaluationResult:
    strategy_version: str
    rules_hash: str
    symbol: str
    eligible: bool
    classification: str
    score: int
    score_components: Mapping[str, int]
    opening_relative_volume: float
    opening_rvol_rank: int
    ranking_scope: str
    hard_rejects: tuple[str, ...]
    warnings: tuple[str, ...]
    quote_summary: QuoteSummary
    sizing: SizingResult

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _validate_config(raw: Mapping[str, Any]) -> None:
    try:
        strategy = raw["strategy"]
        universe = raw["universe"]
        execution = raw["execution"]
        risk = raw["risk"]
        circuit_breakers = raw["circuit_breakers"]
        maturities = raw["maturity"]
        promotion = raw["promotion"]
    except KeyError as exc:
        raise StrategyInputError(f"strategy config lacks section {exc.args[0]}") from exc
    sections = {
        "strategy": strategy,
        "universe": universe,
        "execution": execution,
        "risk": risk,
        "circuit_breakers": circuit_breakers,
        "maturity": maturities,
        "promotion": promotion,
    }
    malformed = [name for name, value in sections.items() if not isinstance(value, Mapping)]
    if malformed:
        raise StrategyInputError(f"strategy config sections are malformed: {malformed}")
    if strategy.get("engine_schema") != 1 or not str(strategy.get("version") or ""):
        raise StrategyInputError("strategy config identity or engine schema is invalid")
    entry_start = _parse_et(strategy.get("entry_start_et"), "strategy.entry_start_et")
    entry_cutoff = _parse_et(
        strategy.get("entry_cutoff_et"), "strategy.entry_cutoff_et"
    )
    force_flat = _parse_et(strategy.get("force_flat_et"), "strategy.force_flat_et")
    if not entry_start < entry_cutoff < force_flat:
        raise StrategyInputError(
            "strategy times must satisfy entry_start < entry_cutoff < force_flat"
        )
    if (
        _integer(
            execution.get("quote_snapshot_count"),
            "execution.quote_snapshot_count",
        )
        != 3
    ):
        raise StrategyInputError("execution.quote_snapshot_count must remain exactly 3")
    a_plus_spread = _number(
        execution.get("maximum_a_plus_median_spread_fraction"),
        "execution.maximum_a_plus_median_spread_fraction",
        positive=True,
    )
    operating_spread = _number(
        execution.get("maximum_median_spread_fraction"),
        "execution.maximum_median_spread_fraction",
        positive=True,
    )
    single_spread = _number(
        execution.get("maximum_single_spread_fraction"),
        "execution.maximum_single_spread_fraction",
        positive=True,
    )
    if not a_plus_spread <= operating_spread <= single_spread < 1:
        raise StrategyInputError(
            "spread limits must satisfy A+ <= operating <= single < 1"
        )
    for field in (
        "maximum_entry_chase_fraction",
        "maximum_depth_participation_fraction",
        "maximum_recent_volume_participation_fraction",
    ):
        if not 0 < _number(execution.get(field), f"execution.{field}") <= 1:
            raise StrategyInputError(f"execution.{field} must be in (0, 1]")
    if not (
        0
        < _number(risk.get("atr_stop_fraction"), "risk.atr_stop_fraction")
        <= 1
        and 0
        < _number(risk.get("maximum_stop_fraction"), "risk.maximum_stop_fraction")
        < 1
        and 0
        < _number(
            risk.get("minimum_stop_slippage_reserve_fraction"),
            "risk.minimum_stop_slippage_reserve_fraction",
        )
        < 1
        and _number(
            risk.get("minimum_resistance_room_fraction"),
            "risk.minimum_resistance_room_fraction",
            positive=True,
        )
        and _number(
            risk.get("minimum_reward_risk"),
            "risk.minimum_reward_risk",
            positive=True,
        )
    ):
        raise StrategyInputError("risk fractions or reward/risk limit are invalid")
    if _integer(
        circuit_breakers.get("maximum_consecutive_losses"),
        "circuit_breakers.maximum_consecutive_losses",
        minimum=1,
    ) < 1:
        raise StrategyInputError("maximum consecutive losses must be positive")
    for field in (
        "maximum_rolling_five_session_drawdown_fraction",
        "maximum_strategy_drawdown_fraction",
    ):
        if not 0 < _number(
            circuit_breakers.get(field), f"circuit_breakers.{field}"
        ) < 1:
            raise StrategyInputError(f"circuit_breakers.{field} must be in (0, 1)")
    for field in (
        "minimum_open_price",
        "minimum_average_daily_volume_14",
        "minimum_daily_atr_14",
        "minimum_opening_relative_volume",
    ):
        _number(universe.get(field), f"universe.{field}", positive=True)
    maturity_order = ("UNVALIDATED", "PROVISIONAL", "VALIDATED")
    risk_fractions: list[float] = []
    allocation_caps: list[float] = []
    for name in maturity_order:
        values = maturities.get(name)
        if not isinstance(values, Mapping):
            raise StrategyInputError(f"maturity.{name} is missing")
        risk_fractions.append(
            _number(values.get("risk_fraction"), f"maturity.{name}.risk_fraction")
        )
        allocation_caps.append(
            _number(
                values.get("allocation_cap_fraction"),
                f"maturity.{name}.allocation_cap_fraction",
            )
        )
        score = _integer(values.get("minimum_score"), f"maturity.{name}.minimum_score")
        if not 0 <= risk_fractions[-1] < 1 or not 0 < allocation_caps[-1] <= 1:
            raise StrategyInputError(f"maturity.{name} risk or allocation is invalid")
        if not 0 <= score <= 100:
            raise StrategyInputError(f"maturity.{name}.minimum_score must be <= 100")
    if risk_fractions != sorted(risk_fractions) or allocation_caps != sorted(
        allocation_caps
    ):
        raise StrategyInputError("maturity risk and allocation caps must not decrease")
    provisional = promotion.get("provisional")
    validated = promotion.get("validated")
    if not isinstance(provisional, Mapping) or not isinstance(validated, Mapping):
        raise StrategyInputError("promotion gates are incomplete")
    if _integer(
        validated.get("minimum_closed_signals"),
        "promotion.validated.minimum_closed_signals",
    ) < _integer(
        provisional.get("minimum_closed_signals"),
        "promotion.provisional.minimum_closed_signals",
    ):
        raise StrategyInputError("validated signal minimum cannot trail provisional")


def load_config(path: Path = DEFAULT_CONFIG_PATH) -> StrategyConfig:
    contents = path.read_bytes()
    raw = tomllib.loads(contents.decode("utf-8"))
    _validate_config(raw)
    canonical = json.dumps(raw, sort_keys=True, separators=(",", ":")).encode()
    return StrategyConfig(raw=raw, rules_hash=hashlib.sha256(canonical).hexdigest())


def _number(value: Any, name: str, *, positive: bool = False) -> float:
    if isinstance(value, bool):
        raise StrategyInputError(f"{name} must be numeric")
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise StrategyInputError(f"{name} must be numeric") from exc
    if not math.isfinite(result) or (positive and result <= 0):
        qualifier = "positive and finite" if positive else "finite"
        raise StrategyInputError(f"{name} must be {qualifier}")
    return result


def _integer(value: Any, name: str, *, minimum: int = 0) -> int:
    if isinstance(value, bool):
        raise StrategyInputError(f"{name} must be an integer")
    try:
        result = int(value)
    except (TypeError, ValueError) as exc:
        raise StrategyInputError(f"{name} must be an integer") from exc
    if result != value or result < minimum:
        raise StrategyInputError(f"{name} must be an integer >= {minimum}")
    return result


def _required(mapping: Mapping[str, Any], key: str, prefix: str = "") -> Any:
    try:
        return mapping[key]
    except KeyError as exc:
        name = f"{prefix}.{key}" if prefix else key
        raise StrategyInputError(f"Missing required field: {name}") from exc


def _true(mapping: Mapping[str, Any], key: str) -> bool:
    return _required(mapping, key) is True


def compute_opening_relative_volume(
    today_volume: Any,
    prior_opening_volumes: Sequence[Any],
    lookback: int = 14,
) -> float:
    if len(prior_opening_volumes) != lookback:
        raise StrategyInputError(
            f"prior_opening_volumes must contain exactly {lookback} completed sessions"
        )
    today = _number(today_volume, "opening_bar.volume")
    if today < 0:
        raise StrategyInputError("opening_bar.volume must be nonnegative and finite")
    prior = [
        _number(value, f"prior_opening_volumes[{index}]", positive=True)
        for index, value in enumerate(prior_opening_volumes)
    ]
    return today / statistics.fmean(prior)


def summarize_quotes(
    snapshots: Sequence[Mapping[str, Any]], config: StrategyConfig
) -> QuoteSummary:
    execution = config.raw["execution"]
    expected_count = int(execution["quote_snapshot_count"])
    failures: list[str] = []
    spreads_fraction: list[float] = []
    spreads_dollars: list[float] = []
    ask_depths: list[int] = []
    recent_volumes: list[int] = []

    if len(snapshots) != expected_count:
        raise StrategyInputError(
            f"quotes must contain exactly {expected_count} snapshots"
        )

    for index, snapshot in enumerate(snapshots):
        bid = _number(
            _required(snapshot, "bid", f"quotes[{index}]"),
            f"quotes[{index}].bid",
            positive=True,
        )
        ask = _number(
            _required(snapshot, "ask", f"quotes[{index}]"),
            f"quotes[{index}].ask",
            positive=True,
        )
        age = _number(
            _required(snapshot, "age_seconds", f"quotes[{index}]"),
            f"quotes[{index}].age_seconds",
        )
        depth = _integer(
            _required(snapshot, "ask_depth", f"quotes[{index}]"),
            f"quotes[{index}].ask_depth",
        )
        recent_volume = _integer(
            _required(snapshot, "recent_real_1m_volume", f"quotes[{index}]"),
            f"quotes[{index}].recent_real_1m_volume",
        )
        if ask < bid:
            failures.append(f"snapshot {index + 1} is crossed")
        if age < 0 or age > float(execution["maximum_quote_age_seconds"]):
            failures.append(f"snapshot {index + 1} is stale")
        spread = max(0.0, ask - bid)
        midpoint = (ask + bid) / 2
        spreads_dollars.append(spread)
        spreads_fraction.append(spread / midpoint)
        ask_depths.append(depth)
        recent_volumes.append(recent_volume)

    median_fraction = statistics.median(spreads_fraction)
    maximum_fraction = max(spreads_fraction)
    if median_fraction > float(execution["maximum_median_spread_fraction"]):
        failures.append("median spread exceeds operating limit")
    if maximum_fraction > float(execution["maximum_single_spread_fraction"]):
        failures.append("a snapshot spread exceeds the hard limit")

    return QuoteSummary(
        median_spread_fraction=median_fraction,
        maximum_spread_fraction=maximum_fraction,
        median_spread_dollars=statistics.median(spreads_dollars),
        minimum_ask_depth=min(ask_depths),
        minimum_recent_volume=min(recent_volumes),
        snapshots_clean=not failures,
        failures=tuple(failures),
    )


def _parse_et(value: Any, name: str) -> time:
    if not isinstance(value, str):
        raise StrategyInputError(f"{name} must be HH:MM:SS")
    try:
        return time.fromisoformat(value)
    except ValueError as exc:
        raise StrategyInputError(f"{name} must be HH:MM:SS") from exc


def _score_opening_rvol(rvol: float, rank: int) -> int:
    if rvol >= 5 and rank <= 5:
        return 20
    if rvol >= 3 and rank <= 10:
        return 17
    if rvol >= 1 and rank <= 20:
        return 14
    return 0


def evaluate_candidate(
    payload: Mapping[str, Any], config: StrategyConfig | None = None
) -> EvaluationResult:
    config = config or load_config()
    universe = config.raw["universe"]
    execution = config.raw["execution"]
    risk = config.raw["risk"]
    strategy = config.raw["strategy"]

    session = _required(payload, "session")
    candidate = _required(payload, "candidate")
    if not isinstance(session, Mapping) or not isinstance(candidate, Mapping):
        raise StrategyInputError("session and candidate must be objects")
    maturity_name = str(_required(session, "maturity", "session"))
    maturity = config.maturity(maturity_name)
    symbol = str(_required(candidate, "symbol", "candidate")).upper()
    rejects: list[str] = []
    warnings: list[str] = []

    now = _parse_et(_required(session, "time_et", "session"), "session.time_et")
    start = _parse_et(strategy["entry_start_et"], "strategy.entry_start_et")
    cutoff = _parse_et(strategy["entry_cutoff_et"], "strategy.entry_cutoff_et")
    if not start <= now <= cutoff:
        rejects.append("entry time is outside the production window")

    boolean_session_gates = {
        "agentic_allowed": "account is not agentic-authorized",
        "account_identified": "authorized account was not identified",
        "encryption_ready": "identifier encryption or audit is unavailable",
        "monitoring_available": "continuous monitoring is unavailable",
        "protective_stop_workflow_ready": "prompt protective-stop workflow is unavailable",
        "broker_review_available": "broker review workflow is unavailable",
    }
    for field, message in boolean_session_gates.items():
        if not _true(session, field):
            rejects.append(message)
    if _integer(
        _required(session, "open_positions", "session"), "session.open_positions"
    ):
        rejects.append("an equity position already exists")
    if _integer(
        _required(session, "unresolved_orders", "session"), "session.unresolved_orders"
    ):
        rejects.append("an unresolved equity order already exists")
    if _integer(
        _required(session, "filled_entries_today", "session"),
        "session.filled_entries_today",
    ):
        rejects.append("today's filled-entry limit is exhausted")
    if _true(session, "circuit_breaker_active"):
        rejects.append("a circuit breaker is active")
    if (
        not bool(maturity["live_allowed"])
        and str(session.get("mode", "live")) == "live"
    ):
        rejects.append(f"live entries are disabled for {maturity_name}")

    account_equity = _number(
        _required(session, "account_equity", "session"),
        "session.account_equity",
        positive=True,
    )
    buying_power = _number(
        _required(session, "buying_power", "session"),
        "session.buying_power",
        positive=True,
    )

    opening_bar = _required(candidate, "opening_bar", "candidate")
    if not isinstance(opening_bar, Mapping):
        raise StrategyInputError("candidate.opening_bar must be an object")
    opening_price = _number(
        _required(candidate, "opening_price", "candidate"),
        "candidate.opening_price",
        positive=True,
    )
    average_volume = _number(
        _required(candidate, "average_daily_volume_14", "candidate"),
        "candidate.average_daily_volume_14",
        positive=True,
    )
    atr = _number(
        _required(candidate, "daily_atr_14", "candidate"),
        "candidate.daily_atr_14",
        positive=True,
    )
    opening_open = _number(
        _required(opening_bar, "open", "candidate.opening_bar"),
        "candidate.opening_bar.open",
        positive=True,
    )
    opening_high = _number(
        _required(opening_bar, "high", "candidate.opening_bar"),
        "candidate.opening_bar.high",
        positive=True,
    )
    opening_low = _number(
        _required(opening_bar, "low", "candidate.opening_bar"),
        "candidate.opening_bar.low",
        positive=True,
    )
    opening_close = _number(
        _required(opening_bar, "close", "candidate.opening_bar"),
        "candidate.opening_bar.close",
        positive=True,
    )
    if (
        opening_low > min(opening_open, opening_close)
        or opening_high < max(opening_open, opening_close)
        or opening_low > opening_high
    ):
        raise StrategyInputError(
            "candidate.opening_bar contains inconsistent OHLC values"
        )
    rvol = compute_opening_relative_volume(
        _required(opening_bar, "volume", "candidate.opening_bar"),
        _required(candidate, "prior_opening_volumes", "candidate"),
        int(universe["opening_relative_volume_lookback"]),
    )
    rank = _integer(
        _required(candidate, "opening_rvol_rank", "candidate"),
        "candidate.opening_rvol_rank",
        minimum=1,
    )
    ranking_scope = str(_required(candidate, "ranking_scope", "candidate"))
    if ranking_scope not in ("full_eligible_universe", "scanner_results"):
        raise StrategyInputError(
            "candidate.ranking_scope must be full_eligible_universe or scanner_results"
        )
    ranking_scope_count = _integer(
        _required(candidate, "ranking_scope_count", "candidate"),
        "candidate.ranking_scope_count",
        minimum=1,
    )
    if rank > ranking_scope_count:
        raise StrategyInputError("opening RVOL rank exceeds its recorded ranking scope")
    if ranking_scope != "full_eligible_universe":
        warnings.append("opening RVOL rank is approximate within scanner results")

    if opening_price < float(universe["minimum_open_price"]):
        rejects.append("opening price is below the universe minimum")
    if average_volume < float(universe["minimum_average_daily_volume_14"]):
        rejects.append("average daily volume is below the universe minimum")
    if atr < float(universe["minimum_daily_atr_14"]):
        rejects.append("daily ATR is below the universe minimum")
    bullish = opening_close > opening_open
    if not bullish:
        rejects.append("first five-minute candle is not bullish")
    if rvol < float(universe["minimum_opening_relative_volume"]):
        rejects.append("opening relative volume is below 1.0")

    candidate_gates = {
        "is_common_stock": "security is not a U.S.-listed common stock",
        "verified_catalyst": "catalyst is not verified",
        "tradable": "symbol is not currently long-tradable",
        "clean_break": "opening-range breakout is not clean",
        "above_vwap": "price is not above session VWAP",
        "vwap_flat_or_rising": "session VWAP is falling",
        "benchmark_supportive_or_independent_strength": "market alignment gate failed",
        "sector_relative_strength": "sector or candidate relative-strength gate failed",
        "stop_outside_noise": "planned stop is inside ordinary noise",
    }
    for field, message in candidate_gates.items():
        if not _true(candidate, field):
            rejects.append(message)
    for field, message in {
        "dilution_conflict": "catalyst has a dilution or financing conflict",
        "halt_risk": "halt or unstable-liquidity risk is present",
    }.items():
        if _true(candidate, field):
            rejects.append(message)

    catalyst_score = _integer(
        _required(candidate, "catalyst_score", "candidate"), "candidate.catalyst_score"
    )
    if catalyst_score not in (20, 25):
        raise StrategyInputError("candidate.catalyst_score must be 20 or 25")

    snapshots = _required(payload, "quotes")
    if not isinstance(snapshots, Sequence) or isinstance(snapshots, (str, bytes)):
        raise StrategyInputError("quotes must be an array")
    quote_summary = summarize_quotes(snapshots, config)
    rejects.extend(quote_summary.failures)

    entry_limit = _number(
        _required(candidate, "entry_limit", "candidate"),
        "candidate.entry_limit",
        positive=True,
    )
    latest_ask = _number(
        _required(snapshots[-1], "ask", "quotes[-1]"), "quotes[-1].ask", positive=True
    )
    if entry_limit < latest_ask:
        rejects.append("entry limit is not marketable against the fresh ask")
    chase_fraction = (entry_limit - opening_high) / opening_high
    chase_ok = 0 <= chase_fraction <= float(execution["maximum_entry_chase_fraction"])
    if not chase_ok:
        rejects.append("entry would chase too far above the opening-range high")

    invalidation = _number(
        _required(candidate, "technical_invalidation", "candidate"),
        "candidate.technical_invalidation",
        positive=True,
    )
    if invalidation >= entry_limit:
        raise StrategyInputError("technical invalidation must be below entry limit")
    stop_distance = max(
        float(risk["atr_stop_fraction"]) * atr,
        entry_limit - invalidation,
    )
    stop_fraction = stop_distance / entry_limit
    planned_stop = entry_limit - stop_distance
    if stop_fraction > float(risk["maximum_stop_fraction"]):
        rejects.append("stop distance exceeds the maximum fraction")
    if planned_stop >= min(
        _number(snapshot["bid"], "quote.bid", positive=True) for snapshot in snapshots
    ):
        rejects.append("planned stop is not below the observed bid")

    observed_p95_fraction = _number(
        candidate.get("observed_stop_slippage_p95_fraction", 0.0),
        "candidate.observed_stop_slippage_p95_fraction",
    )
    if observed_p95_fraction < 0:
        raise StrategyInputError("observed stop slippage cannot be negative")
    reserve = max(
        quote_summary.median_spread_dollars,
        float(risk["minimum_stop_slippage_reserve_fraction"]) * entry_limit,
        observed_p95_fraction * entry_limit,
    )
    risk_per_share = stop_distance + reserve
    risk_budget = account_equity * float(maturity["risk_fraction"])
    q_risk = math.floor(risk_budget / risk_per_share)
    q_allocation = math.floor(
        buying_power * float(maturity["allocation_cap_fraction"]) / entry_limit
    )
    q_depth = math.floor(
        quote_summary.minimum_ask_depth
        * float(execution["maximum_depth_participation_fraction"])
    )
    q_volume = math.floor(
        quote_summary.minimum_recent_volume
        * float(execution["maximum_recent_volume_participation_fraction"])
    )
    q_liquidity = min(q_depth, q_volume)
    quantity = min(q_risk, q_allocation, q_liquidity)
    cap_values = {
        "risk": q_risk,
        "allocation": q_allocation,
        "liquidity": q_liquidity,
    }
    binding_caps = tuple(
        name
        for name, capped_quantity in cap_values.items()
        if capped_quantity == quantity
    )
    if quantity < 1:
        rejects.append("risk, allocation, or liquidity caps produce zero shares")
    notional = quantity * entry_limit
    buying_power_fraction = notional / buying_power
    target_floor = float(risk["allocation_target_floor_fraction"])
    if quantity > 0 and buying_power_fraction < target_floor:
        warnings.append(
            "risk- or liquidity-sized notional is below the aggressive allocation target"
        )

    resistance = _number(
        _required(candidate, "resistance_price", "candidate"),
        "candidate.resistance_price",
        positive=True,
    )
    resistance_room = (resistance - entry_limit) / entry_limit
    estimated_exit_cost = max(
        quote_summary.median_spread_dollars,
        float(risk["minimum_stop_slippage_reserve_fraction"]) * entry_limit,
    )
    reward_per_share = resistance - entry_limit - estimated_exit_cost
    reward_risk = reward_per_share / risk_per_share
    if resistance_room < float(risk["minimum_resistance_room_fraction"]):
        rejects.append("resistance room is below 2.2%")
    if reward_risk < float(risk["minimum_reward_risk"]):
        rejects.append("reward/risk before resistance is below 2.5")

    depth_ok = q_liquidity >= min(q_risk, q_allocation) and q_liquidity > 0
    # Liquidity caps size rather than automatically rejecting the signal. A zero
    # quantity remains a hard reject; constrained notional is disclosed above.
    if not depth_ok:
        warnings.append("liquidity cap reduced the risk-sized quantity")

    exit_plan_ok = (
        stop_fraction <= float(risk["maximum_stop_fraction"])
        and _true(candidate, "stop_outside_noise")
        and resistance_room >= float(risk["minimum_resistance_room_fraction"])
        and reward_risk >= float(risk["minimum_reward_risk"])
    )
    components = {
        "catalyst_quality": catalyst_score,
        "opening_relative_volume": _score_opening_rvol(rvol, rank),
        "orb_structure_and_timing": 5
        * sum(
            (
                bullish,
                _true(candidate, "clean_break"),
                chase_ok,
                _true(candidate, "above_vwap")
                and _true(candidate, "vwap_flat_or_rising"),
                resistance_room >= float(risk["minimum_resistance_room_fraction"]),
            )
        ),
        "liquidity_spread_depth_tradability": 5
        * sum(
            (
                quote_summary.snapshots_clean,
                q_liquidity > 0,
                _true(candidate, "tradable"),
            )
        ),
        "market_sector_alignment": 5
        * sum(
            (
                _true(candidate, "benchmark_supportive_or_independent_strength"),
                _true(candidate, "sector_relative_strength"),
            )
        ),
        "exit_plan_quality": 5 if exit_plan_ok else 0,
    }
    score = sum(components.values())
    minimum_score = int(maturity["minimum_score"])
    if score < minimum_score:
        rejects.append(
            f"score {score} is below the {minimum_score}-point maturity gate"
        )
    a_plus_spread_ok = quote_summary.median_spread_fraction <= float(
        execution["maximum_a_plus_median_spread_fraction"]
    )
    if score >= 90 and not a_plus_spread_ok:
        warnings.append("median spread is too wide for A+ classification")
        if maturity_name == "UNVALIDATED":
            rejects.append("A+ median spread exceeds the 0.08% limit")
    classification = (
        "A+"
        if score >= 90 and a_plus_spread_ok
        else "qualified"
        if score >= 85
        else "rejected"
    )

    sizing = SizingResult(
        entry_limit=entry_limit,
        stop_distance=stop_distance,
        stop_fraction=stop_fraction,
        planned_stop=planned_stop,
        slippage_reserve_per_share=reserve,
        risk_per_share=risk_per_share,
        risk_budget=risk_budget,
        q_risk=q_risk,
        q_allocation=q_allocation,
        q_liquidity=q_liquidity,
        quantity=quantity,
        binding_caps=binding_caps,
        notional=notional,
        buying_power_fraction=buying_power_fraction,
        allocation_target_met=buying_power_fraction >= target_floor,
        resistance_room_fraction=resistance_room,
        reward_risk=reward_risk,
        milestone_price=entry_limit * 1.02,
    )
    return EvaluationResult(
        strategy_version=config.version,
        rules_hash=config.rules_hash,
        symbol=symbol,
        eligible=not rejects,
        classification=classification if not rejects else "rejected",
        score=score,
        score_components=components,
        opening_relative_volume=rvol,
        opening_rvol_rank=rank,
        ranking_scope=ranking_scope,
        hard_rejects=tuple(dict.fromkeys(rejects)),
        warnings=tuple(dict.fromkeys(warnings)),
        quote_summary=quote_summary,
        sizing=sizing,
    )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", type=Path, help="JSON candidate evaluation payload")
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG_PATH)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        payload = json.loads(args.input.read_text(encoding="utf-8"))
        if not isinstance(payload, Mapping):
            raise StrategyInputError("input JSON must contain an object")
        result = evaluate_candidate(payload, load_config(args.config))
        print(json.dumps(result.to_dict(), indent=2, sort_keys=True))
        return 0 if result.eligible else 2
    except (OSError, json.JSONDecodeError, StrategyInputError) as exc:
        print(
            json.dumps({"error": str(exc), "error_type": type(exc).__name__}, indent=2),
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

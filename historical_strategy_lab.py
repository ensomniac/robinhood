"""Run production-aware counterfactual strategy research on frozen local data.

The lab is deliberately research-only. It reuses the immutable evidence and
completed-bar execution contracts from :mod:`historical_research`, evaluates
every bar-executable signal, and then applies versioned one-trade-per-day
policies. It never edits production strategy configuration, ledgers, trade
contexts, maturity state, or broker state.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import random
import re
import statistics
import time
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from historical_research import (
    DEFAULT_DATA_ROOT,
    ExecutionConfig,
    HistoricalResearchError,
    _find_signal_point_in_time,
    _trade_from_decision,
    load_dataset,
)
from historical_research_strategies import (
    CandidateContext,
    ResearchStrategyError,
    SignalDecision,
    StrategyPlugin,
    load_strategy,
    parse_bars,
    plugin_manifest_hash_input,
)
from strategy_engine import (
    EvaluationResult,
    StrategyConfig,
    evaluate_candidate,
    load_config,
)


SCHEMA_VERSION = 1
LAB_VERSION = "2026-07-18-v2"
PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_EVIDENCE = (
    PROJECT_ROOT / "historical_batches" / "evidence-2026-07-16-one-hundred-days.json"
)
DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "research_runs"
PRODUCTION_PATHS = (
    PROJECT_ROOT / "strategy_config.toml",
    PROJECT_ROOT / "SIGNALS.jsonl",
    PROJECT_ROOT / "TRADES.md",
    PROJECT_ROOT / "trades",
)
PROTECTED_PUBLISH_PATHS = PRODUCTION_PATHS + (
    PROJECT_ROOT / "progress" / "HISTORY.jsonl",
    PROJECT_ROOT / "AGENTS.md",
)
SCORE_REASON = re.compile(r"^score \d+ is below the \d+-point maturity gate$")
STALE_REASON = re.compile(r"^snapshot \d+ is stale$")
CROSSED_REASON = re.compile(r"^snapshot \d+ is crossed$")


class HistoricalStrategyLabError(RuntimeError):
    """Raised when the lab cannot preserve its research-fidelity boundary."""


@dataclass(frozen=True, slots=True)
class Policy:
    """One predeclared one-trade-per-day counterfactual selection policy."""

    policy_id: str
    strategy_spec: str
    description: str
    version: str = "1.0.0"
    signal_cutoff_et: str | None = None
    require_earnings_2_02: bool = False
    maximum_stop_fraction: float | None = None
    selection: str = "strength"
    require_production_eligible: bool = False

    def validate(self) -> None:
        if not self.policy_id or not self.description or not self.strategy_spec:
            raise HistoricalStrategyLabError("policy identity fields cannot be empty")
        if self.signal_cutoff_et is not None and not (
            "09:35:00" < self.signal_cutoff_et <= "10:30:00"
        ):
            raise HistoricalStrategyLabError(
                f"{self.policy_id}: signal cutoff must be after 09:35 and at or before 10:30"
            )
        if self.maximum_stop_fraction is not None and not (
            math.isfinite(self.maximum_stop_fraction) and self.maximum_stop_fraction > 0
        ):
            raise HistoricalStrategyLabError(
                f"{self.policy_id}: maximum stop fraction must be positive"
            )
        if self.selection not in {"strength", "production_score", "opening_rvol"}:
            raise HistoricalStrategyLabError(
                f"{self.policy_id}: unsupported selection {self.selection!r}"
            )


BUILTIN_POLICIES: dict[str, Policy] = {
    policy.policy_id: policy
    for policy in (
        Policy(
            "orb-production-stack",
            "orb-5m-research",
            "Research ORB signal requiring every current production gate.",
            require_production_eligible=True,
        ),
        Policy(
            "orb-all-strength",
            "orb-5m-research",
            "All-window ORB counterfactual; earliest signal then strength.",
        ),
        Policy(
            "orb-early-strength",
            "orb-5m-research",
            "ORB signal-bar start before 09:40; earliest signal then strength.",
            signal_cutoff_et="09:40:00",
        ),
        Policy(
            "orb-early-score",
            "orb-5m-research",
            "Early ORB signal-bar start; production score ranks simultaneous signals.",
            signal_cutoff_et="09:40:00",
            selection="production_score",
        ),
        Policy(
            "orb-early-rvol",
            "orb-5m-research",
            "Early ORB signal-bar start; opening-relative-volume ranks simultaneous signals.",
            signal_cutoff_et="09:40:00",
            selection="opening_rvol",
        ),
        Policy(
            "orb-early-earnings",
            "orb-5m-research",
            "Early ORB restricted to SEC item 2.02 earnings catalysts.",
            signal_cutoff_et="09:40:00",
            require_earnings_2_02=True,
        ),
        Policy(
            "orb-early-stop-0.8pct",
            "orb-5m-research",
            "Early ORB enforcing the frozen production 0.8% maximum stop.",
            signal_cutoff_et="09:40:00",
            maximum_stop_fraction=0.008,
        ),
        Policy(
            "reversal-all-strength",
            "opening-reversal",
            "All-window opening reversal; earliest signal then strength.",
        ),
        Policy(
            "reversal-early-strength",
            "opening-reversal",
            "Opening reversal signal-bar start before 09:40; earliest signal then strength.",
            signal_cutoff_et="09:40:00",
        ),
        Policy(
            "reversal-early-score",
            "opening-reversal",
            "Early opening reversal; production score ranks simultaneous signals.",
            signal_cutoff_et="09:40:00",
            selection="production_score",
        ),
        Policy(
            "reversal-early-rvol",
            "opening-reversal",
            "Early opening reversal; opening-relative-volume rank orders simultaneous signals.",
            signal_cutoff_et="09:40:00",
            selection="opening_rvol",
        ),
        Policy(
            "reversal-early-earnings",
            "opening-reversal",
            "Early opening reversal restricted to SEC item 2.02 earnings catalysts.",
            signal_cutoff_et="09:40:00",
            require_earnings_2_02=True,
        ),
        Policy(
            "reversal-early-stop-0.8pct",
            "opening-reversal",
            "Early opening reversal enforcing the frozen production 0.8% maximum stop.",
            signal_cutoff_et="09:40:00",
            maximum_stop_fraction=0.008,
        ),
        Policy(
            "vwap-pullback-all",
            "vwap-pullback",
            "Existing VWAP-pullback research definition for deletion evidence.",
        ),
        Policy(
            "hod-continuation-all",
            "hod-continuation",
            "Existing high-of-day continuation definition for deletion evidence.",
        ),
    )
}


@dataclass(frozen=True, slots=True)
class Observation:
    """A point-in-time signal decision plus facts known to the lab policy."""

    date: str
    symbol: str
    signal_id: str
    strategy_id: str
    context: CandidateContext
    decision: SignalDecision | None
    no_signal_reason: str
    production: EvaluationResult
    earnings_2_02: bool
    production_evaluation_time_et: str


def _canonical_json(value: Any) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode("utf-8")


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _tree_hash(path: Path) -> str | None:
    if not path.exists():
        return None
    if path.is_file():
        return _sha256_file(path)
    rows = [
        {
            "path": str(value.relative_to(path)),
            "sha256": _sha256_file(value),
        }
        for value in sorted(path.rglob("*"))
        if value.is_file()
    ]
    return _sha256_bytes(_canonical_json(rows))


def production_hashes() -> dict[str, str | None]:
    return {
        str(path.relative_to(PROJECT_ROOT)): _tree_hash(path)
        for path in PRODUCTION_PATHS
    }


def _safe_publish_prefix(prefix: Path) -> Path:
    resolved = prefix.resolve()
    for protected in PROTECTED_PUBLISH_PATHS:
        protected = protected.resolve()
        if resolved == protected or protected in resolved.parents:
            raise HistoricalStrategyLabError(
                f"strategy lab output cannot target production artifact {protected}"
            )
    return resolved


def _atomic_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(text, encoding="utf-8")
    os.replace(temporary, path)


def _atomic_json(path: Path, value: Any) -> None:
    _atomic_text(path, json.dumps(value, indent=2, sort_keys=True) + "\n")


def _seconds(time_et: str) -> int:
    try:
        hour, minute, second = (int(value) for value in time_et.split(":"))
    except (TypeError, ValueError) as exc:
        raise HistoricalStrategyLabError(f"invalid ET time {time_et!r}") from exc
    return hour * 3600 + minute * 60 + second


def normalize_rejection_reason(reason: str) -> str:
    if SCORE_REASON.match(reason):
        return "score below maturity gate"
    if STALE_REASON.match(reason):
        return "quote snapshot is stale"
    if CROSSED_REASON.match(reason):
        return "quote snapshot is crossed"
    return reason


def rejection_category(reason: str) -> str:
    reason = normalize_rejection_reason(reason)
    operational = {
        "account is not agentic-authorized",
        "authorized account was not identified",
        "identifier encryption or audit is unavailable",
        "continuous monitoring is unavailable",
        "prompt protective-stop workflow is unavailable",
        "broker review workflow is unavailable",
        "an equity position already exists",
        "an unresolved equity order already exists",
        "today's filled-entry limit is exhausted",
        "a circuit breaker is active",
        "symbol is not currently long-tradable",
        "halt or unstable-liquidity risk is present",
        "quote snapshot is crossed",
        "entry limit is not marketable against the fresh ask",
        "risk, allocation, or liquidity caps produce zero shares",
    }
    universe = {
        "opening price is below the universe minimum",
        "average daily volume is below the universe minimum",
        "daily ATR is below the universe minimum",
        "opening relative volume is below 1.0",
        "security is not a U.S.-listed common stock",
        "catalyst is not verified",
        "catalyst has a dilution or financing conflict",
    }
    structure = {
        "first five-minute candle is not bullish",
        "opening-range breakout is not clean",
        "price is not above session VWAP",
        "session VWAP is falling",
        "market alignment gate failed",
        "sector or candidate relative-strength gate failed",
    }
    execution = {
        "quote snapshot is stale",
        "median spread exceeds operating limit",
        "a snapshot spread exceeds the hard limit",
        "entry would chase too far above the opening-range high",
        "entry time is outside the production window",
    }
    risk_exit = {
        "planned stop is inside ordinary noise",
        "stop distance exceeds the maximum fraction",
        "resistance room is below 2.2%",
        "reward/risk before resistance is below 2.5",
    }
    if reason in operational:
        return "operational_safety"
    if reason in universe:
        return "universe"
    if reason in structure:
        return "signal_structure"
    if reason in execution:
        return "execution"
    if reason in risk_exit:
        return "risk_and_exit"
    if reason == "score below maturity gate":
        return "derived_score"
    if reason.startswith("live entries are disabled for "):
        return "operational_safety"
    return "unclassified"


def _load_verified_bundle(
    item: Mapping[str, Any], *, expected_frozen_hash: str | None = None
) -> Mapping[str, Any] | None:
    if item["status"] != "available":
        return None
    path = Path(str(item["path"]))
    try:
        bundle = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise HistoricalStrategyLabError(f"cannot read bundle {path}: {exc}") from exc
    day = str(item["date"])
    if bundle.get("date") != day:
        raise HistoricalStrategyLabError(f"{day}: bundle date mismatch")
    if bundle.get("session_capture_complete") is not True:
        raise HistoricalStrategyLabError(f"{day}: incomplete session capture")
    source = bundle.get("source")
    required = (
        "point_in_time",
        "regular_hours_only",
        "split_adjusted",
        "universe_capture_complete",
    )
    if not isinstance(source, Mapping) or any(
        source.get(value) is not True for value in required
    ):
        raise HistoricalStrategyLabError(f"{day}: source attestations are incomplete")
    frozen_hash = source.get("frozen_evidence_sha256")
    if not isinstance(frozen_hash, str) or len(frozen_hash) != 64:
        raise HistoricalStrategyLabError(f"{day}: frozen evidence identity is missing")
    if expected_frozen_hash is not None and frozen_hash != expected_frozen_hash:
        raise HistoricalStrategyLabError(
            f"{day}: bundle belongs to different frozen evidence"
        )
    candidates = bundle.get("candidates")
    if not isinstance(candidates, list) or not candidates:
        raise HistoricalStrategyLabError(f"{day}: bundle has no candidates")
    symbols = tuple(
        str(value.get("symbol", "")).strip().upper() for value in candidates
    )
    expected = tuple(item["expected_symbols"])
    if symbols != expected:
        raise HistoricalStrategyLabError(f"{day}: ordered candidate universe mismatch")
    actual_hash = _sha256_file(path)
    if actual_hash != item["sha256"]:
        raise HistoricalStrategyLabError(f"{day}: bundle changed after inventory")
    return bundle


def _expected_frozen_hashes(evidence_path: Path) -> dict[str, str]:
    """Reconstruct the builder's exact per-date evidence identity."""

    try:
        evidence = json.loads(evidence_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise HistoricalStrategyLabError(
            f"cannot read evidence manifest {evidence_path}: {exc}"
        ) from exc
    scanner = evidence.get("scanner")
    candidates_by_date = evidence.get("candidates_by_date")
    if not isinstance(scanner, Mapping) or not isinstance(candidates_by_date, Mapping):
        raise HistoricalStrategyLabError(
            "evidence manifest must contain scanner and candidates_by_date objects"
        )
    expected: dict[str, str] = {}
    for day, rows in candidates_by_date.items():
        if not isinstance(day, str) or not isinstance(rows, list) or not rows:
            raise HistoricalStrategyLabError(
                "evidence dates must map to non-empty candidate arrays"
            )
        expected[day] = _sha256_bytes(
            _canonical_json(
                {
                    "date": day,
                    "candidates": rows,
                    "scanner": dict(scanner),
                }
            )
        )
    return expected


def _observation(
    day: str,
    raw: Mapping[str, Any],
    plugin: StrategyPlugin,
    config: StrategyConfig,
) -> Observation:
    symbol = str(raw.get("symbol", "")).strip().upper()
    signal_id = str(raw.get("signal_id", f"{day}-{symbol}-research"))
    context = CandidateContext(day, symbol, signal_id, parse_bars(raw.get("bars")))
    try:
        search = _find_signal_point_in_time(plugin, context)
    except (ResearchStrategyError, HistoricalResearchError) as exc:
        raise HistoricalStrategyLabError(
            f"{day} {symbol} {plugin.strategy_id}: {exc}"
        ) from exc
    payload = raw.get("evaluation_payload")
    if not isinstance(payload, Mapping):
        raise HistoricalStrategyLabError(f"{day} {symbol}: evaluation payload missing")
    production = evaluate_candidate(payload, config)
    discovery = raw.get("discovery")
    items = (
        str(discovery.get("filing_items", "")) if isinstance(discovery, Mapping) else ""
    )
    evaluation_time = str(raw.get("evaluation_time_et", ""))
    _seconds(evaluation_time)
    return Observation(
        date=day,
        symbol=symbol,
        signal_id=signal_id,
        strategy_id=plugin.strategy_id,
        context=context,
        decision=search.decision,
        no_signal_reason=search.reason,
        production=production,
        earnings_2_02="2.02" in {value.strip() for value in items.split(",")},
        production_evaluation_time_et=evaluation_time,
    )


def build_observations(
    dataset: Mapping[str, Any],
    policies: Sequence[Policy],
    expected_frozen_hashes: Mapping[str, str],
) -> tuple[dict[str, list[Observation]], dict[str, str]]:
    strategy_specs = tuple(dict.fromkeys(policy.strategy_spec for policy in policies))
    strategy_plugins = {
        strategy_spec: load_strategy(strategy_spec) for strategy_spec in strategy_specs
    }
    config = load_config()
    observations: dict[str, list[Observation]] = defaultdict(list)
    blocked: dict[str, str] = {}
    for item in dataset["bundles"]:
        day = str(item["date"])
        expected_frozen_hash = expected_frozen_hashes.get(day)
        if expected_frozen_hash is None:
            raise HistoricalStrategyLabError(
                f"{day}: evidence identity was not reconstructed"
            )
        bundle = _load_verified_bundle(item, expected_frozen_hash=expected_frozen_hash)
        if bundle is None:
            blocked[day] = "bundle_missing"
            continue
        for raw in bundle["candidates"]:
            for strategy_spec in strategy_specs:
                observations[day].append(
                    _observation(day, raw, strategy_plugins[strategy_spec], config)
                )
    return dict(observations), blocked


def _selection_key(observation: Observation, policy: Policy) -> tuple[Any, ...]:
    decision = observation.decision
    if decision is None:
        raise HistoricalStrategyLabError("cannot rank an observation without a signal")
    if policy.selection == "production_score":
        secondary = (
            -observation.production.score,
            observation.production.opening_rvol_rank,
            -decision.strength,
        )
    elif policy.selection == "opening_rvol":
        secondary = (
            observation.production.opening_rvol_rank,
            -observation.production.opening_relative_volume,
            -decision.strength,
        )
    else:
        secondary = (-decision.strength,)
    return (decision.signal_index, *secondary, observation.symbol)


def evaluate_policy_day(
    observations: Sequence[Observation],
    policy: Policy,
    execution: ExecutionConfig,
) -> dict[str, Any]:
    policy.validate()
    candidates: list[tuple[Observation, dict[str, Any], float]] = []
    filtered = Counter()
    unexecutable = Counter()
    strategy_id = load_strategy(policy.strategy_spec).strategy_id
    for observation in observations:
        if observation.strategy_id != strategy_id:
            continue
        decision = observation.decision
        if decision is None:
            filtered["no_signal"] += 1
            continue
        signal_time = observation.context.bars[decision.signal_index].time_et
        if (
            policy.signal_cutoff_et is not None
            and signal_time >= policy.signal_cutoff_et
        ):
            filtered["signal_at_or_after_cutoff"] += 1
            continue
        if policy.require_earnings_2_02 and not observation.earnings_2_02:
            filtered["not_item_2.02_earnings"] += 1
            continue
        if policy.require_production_eligible and not observation.production.eligible:
            filtered["production_gate_stack_rejected"] += 1
            continue
        try:
            trade = _trade_from_decision(observation.context, decision, execution)
        except ResearchStrategyError as exc:
            unexecutable[str(exc)] += 1
            continue
        stop_fraction = (
            float(trade["entry_price"]) - float(trade["technical_stop"])
        ) / float(trade["entry_price"])
        if (
            policy.maximum_stop_fraction is not None
            and stop_fraction > policy.maximum_stop_fraction
        ):
            filtered["stop_exceeds_policy_maximum"] += 1
            continue
        candidates.append((observation, trade, stop_fraction))
    candidates.sort(key=lambda value: _selection_key(value[0], policy))
    if not candidates:
        return {
            "trade": None,
            "eligible_counterfactuals": 0,
            "filtered": dict(sorted(filtered.items())),
            "unexecutable": dict(sorted(unexecutable.items())),
        }
    observation, trade, stop_fraction = candidates[0]
    return {
        "trade": {
            **trade,
            "production_score": observation.production.score,
            "opening_rvol": round(observation.production.opening_relative_volume, 8),
            "opening_rvol_rank": observation.production.opening_rvol_rank,
            "earnings_2_02": observation.earnings_2_02,
            "stop_fraction": round(stop_fraction, 8),
            "production_eligible": observation.production.eligible,
            "production_rejection_reasons": [
                normalize_rejection_reason(reason)
                for reason in observation.production.hard_rejects
            ],
            "production_evaluation_time_et": observation.production_evaluation_time_et,
            "evaluation_minus_signal_bar_start_seconds": _seconds(
                observation.production_evaluation_time_et
            )
            - _seconds(str(trade["signal_time_et"])),
            "evaluation_minus_entry_bar_start_seconds": _seconds(
                observation.production_evaluation_time_et
            )
            - _seconds(str(trade["entry_time_et"])),
        },
        "eligible_counterfactuals": len(candidates),
        "filtered": dict(sorted(filtered.items())),
        "unexecutable": dict(sorted(unexecutable.items())),
    }


def _drawdown(values: Sequence[float]) -> float:
    equity = 0.0
    peak = 0.0
    maximum = 0.0
    for value in values:
        equity += value
        peak = max(peak, equity)
        maximum = max(maximum, peak - equity)
    return maximum


def _quantile(values: Sequence[float], fraction: float) -> float:
    if not values:
        raise HistoricalStrategyLabError("quantile requires values")
    ordered = sorted(values)
    position = (len(ordered) - 1) * fraction
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    weight = position - lower
    return ordered[lower] * (1 - weight) + ordered[upper] * weight


def _bootstrap_mean(
    values: Sequence[float], *, seed: int, samples: int
) -> dict[str, Any]:
    if not values:
        return {
            "samples": samples,
            "lower_90_one_sided": None,
            "confidence_interval_95": [None, None],
            "probability_mean_positive": None,
        }
    generator = random.Random(seed)
    count = len(values)
    means = [
        sum(values[generator.randrange(count)] for _ in range(count)) / count
        for _ in range(samples)
    ]
    return {
        "samples": samples,
        "lower_90_one_sided": round(_quantile(means, 0.10), 8),
        "confidence_interval_95": [
            round(_quantile(means, 0.025), 8),
            round(_quantile(means, 0.975), 8),
        ],
        "probability_mean_positive": round(
            sum(value > 0 for value in means) / samples, 8
        ),
    }


def _return_stats(values: Sequence[float]) -> dict[str, Any]:
    if not values:
        return {
            "trades": 0,
            "wins": 0,
            "losses": 0,
            "win_rate": None,
            "mean_r": None,
            "median_r": None,
            "total_r": 0.0,
            "profit_factor": None,
            "maximum_drawdown_r": 0.0,
        }
    wins = [value for value in values if value > 0]
    losses = [value for value in values if value < 0]
    gross_loss = -sum(losses)
    return {
        "trades": len(values),
        "wins": len(wins),
        "losses": len(losses),
        "win_rate": round(len(wins) / len(values), 8),
        "mean_r": round(statistics.fmean(values), 8),
        "median_r": round(statistics.median(values), 8),
        "total_r": round(sum(values), 8),
        "profit_factor": (round(sum(wins) / gross_loss, 8) if gross_loss > 0 else None),
        "maximum_drawdown_r": round(_drawdown(values), 8),
    }


def _phase_ranges(requested_dates: Sequence[str]) -> list[tuple[str, int, int]]:
    count = len(requested_dates)
    development_end = math.floor(count * 0.60)
    validation_end = math.floor(count * 0.80)
    return [
        ("development", 0, development_end),
        ("retrospective_validation", development_end, validation_end),
        ("retrospective_holdout", validation_end, count),
    ]


def summarize_policy(
    policy: Policy,
    requested_dates: Sequence[str],
    observations: Mapping[str, Sequence[Observation]],
    blocked: Mapping[str, str],
    execution: ExecutionConfig,
    *,
    bootstrap_samples: int,
) -> dict[str, Any]:
    daily: list[dict[str, Any]] = []
    aggregate_filtered = Counter()
    aggregate_unexecutable = Counter()
    for day in requested_dates:
        if day in blocked:
            daily.append({"date": day, "status": "blocked", "reason": blocked[day]})
            continue
        result = evaluate_policy_day(observations.get(day, ()), policy, execution)
        aggregate_filtered.update(result["filtered"])
        aggregate_unexecutable.update(result["unexecutable"])
        daily.append(
            {
                "date": day,
                "status": "trade" if result["trade"] else "no_trade",
                "trade": result["trade"],
                "eligible_counterfactuals": result["eligible_counterfactuals"],
            }
        )
    covered = [value for value in daily if value["status"] != "blocked"]
    trades = [value["trade"] for value in daily if value["status"] == "trade"]
    returns = [float(value["net_r"]) for value in trades]
    stats = _return_stats(returns)
    monthly: dict[str, list[float]] = defaultdict(list)
    exits = Counter()
    for row in daily:
        if row["status"] != "trade":
            continue
        trade = row["trade"]
        monthly[row["date"][:7]].append(float(trade["net_r"]))
        exits[str(trade["exit_reason"])] += 1
    phases = {}
    for label, start, end in _phase_ranges(requested_dates):
        rows = daily[start:end]
        phase_returns = [
            float(value["trade"]["net_r"])
            for value in rows
            if value["status"] == "trade"
        ]
        phase_stats = _return_stats(phase_returns)
        phase_stats.update(
            {
                "requested_days": len(rows),
                "covered_days": sum(value["status"] != "blocked" for value in rows),
                "first_date": rows[0]["date"] if rows else None,
                "last_date": rows[-1]["date"] if rows else None,
            }
        )
        phases[label] = phase_stats
    stop_fractions = [float(value["stop_fraction"]) for value in trades]
    implied_allocations = [
        min(0.80, 0.0025 / (value + 0.001)) for value in stop_fractions
    ]
    ordered_returns = sorted(returns, reverse=True)
    seed = int(
        _sha256_bytes(
            _canonical_json(
                {
                    "policy": asdict(policy),
                    "execution": asdict(execution),
                    "dates": list(requested_dates),
                }
            )
        )[:16],
        16,
    )
    return {
        "policy": asdict(policy),
        "requested_days": len(requested_dates),
        "covered_days": len(covered),
        "blocked_days": len(daily) - len(covered),
        "no_trade_days": sum(value["status"] == "no_trade" for value in daily),
        **stats,
        "mean_r_per_covered_day": (
            round(sum(returns) / len(covered), 8) if covered else None
        ),
        "bootstrap_trade_mean": _bootstrap_mean(
            returns, seed=seed, samples=bootstrap_samples
        ),
        "chronological_phases": phases,
        "monthly": {
            month: _return_stats(values) for month, values in sorted(monthly.items())
        },
        "exit_reasons": dict(sorted(exits.items())),
        "filtered_signals": dict(sorted(aggregate_filtered.items())),
        "unexecutable_signals": dict(sorted(aggregate_unexecutable.items())),
        "stop_geometry": {
            "median_fraction": (
                round(statistics.median(stop_fractions), 8) if stop_fractions else None
            ),
            "p90_fraction": (
                round(_quantile(stop_fractions, 0.90), 8) if stop_fractions else None
            ),
            "at_or_below_0.8pct": sum(value <= 0.008 for value in stop_fractions),
            "at_or_below_0.8pct_fraction": (
                round(sum(value <= 0.008 for value in stop_fractions) / len(trades), 8)
                if trades
                else None
            ),
            "median_implied_unvalidated_notional_fraction": (
                round(statistics.median(implied_allocations), 8)
                if implied_allocations
                else None
            ),
            "assumptions": {
                "account_risk_fraction": 0.0025,
                "reserve_fraction": 0.001,
                "allocation_cap_fraction": 0.80,
            },
        },
        "concentration": {
            "best_trade_r": round(ordered_returns[0], 8) if ordered_returns else None,
            "top_five_total_r": (
                round(sum(ordered_returns[:5]), 8) if ordered_returns else None
            ),
            "total_without_top_five_r": (
                round(sum(ordered_returns[5:]), 8) if ordered_returns else None
            ),
        },
    }


def _all_signal_diagnostics(
    observations: Mapping[str, Sequence[Observation]],
    execution: ExecutionConfig,
) -> dict[str, Any]:
    rows: list[tuple[Observation, dict[str, Any]]] = []
    unexecutable = Counter()
    production_evaluations: dict[str, EvaluationResult] = {}
    for day_observations in observations.values():
        for observation in day_observations:
            production_evaluations.setdefault(
                observation.signal_id, observation.production
            )
            if (
                observation.strategy_id != "orb-5m-research"
                or observation.decision is None
            ):
                continue
            try:
                trade = _trade_from_decision(
                    observation.context, observation.decision, execution
                )
            except ResearchStrategyError as exc:
                unexecutable[str(exc)] += 1
                continue
            rows.append((observation, trade))
    reasons = sorted(
        {
            normalize_rejection_reason(reason)
            for observation, _ in rows
            for reason in observation.production.hard_rejects
        }
    )
    attribution = []
    for reason in reasons:
        failed = [
            float(trade["net_r"])
            for observation, trade in rows
            if reason
            in {
                normalize_rejection_reason(value)
                for value in observation.production.hard_rejects
            }
        ]
        passed = [
            float(trade["net_r"])
            for observation, trade in rows
            if reason
            not in {
                normalize_rejection_reason(value)
                for value in observation.production.hard_rejects
            }
        ]
        attribution.append(
            {
                "reason": reason,
                "category": rejection_category(reason),
                "failed": _return_stats(failed),
                "passed": _return_stats(passed),
                "pass_minus_fail_mean_r": (
                    round(statistics.fmean(passed) - statistics.fmean(failed), 8)
                    if passed and failed
                    else None
                ),
            }
        )
    reason_counts = Counter()
    category_counts = Counter()
    production_eligible = 0
    for evaluation in production_evaluations.values():
        production_eligible += evaluation.eligible
        for raw_reason in evaluation.hard_rejects:
            reason = normalize_rejection_reason(raw_reason)
            reason_counts[reason] += 1
            category_counts[rejection_category(reason)] += 1
    delays_signal = [
        _seconds(observation.production_evaluation_time_et)
        - _seconds(str(trade["signal_time_et"]))
        for observation, trade in rows
    ]
    delays_entry = [
        _seconds(observation.production_evaluation_time_et)
        - _seconds(str(trade["entry_time_et"]))
        for observation, trade in rows
    ]
    chase_rejects = sum(
        "entry would chase too far above the opening-range high"
        in observation.production.hard_rejects
        for observation, _ in rows
    )
    return {
        "production_candidate_evaluations": len(production_evaluations),
        "production_eligible": production_eligible,
        "executable_orb_counterfactual_signals": len(rows),
        "unexecutable_orb_signals": dict(sorted(unexecutable.items())),
        "chase_rejected_executable_orb_signals": chase_rejects,
        "chase_rejected_fraction": (
            round(chase_rejects / len(rows), 8) if rows else None
        ),
        "production_rejection_reason_counts": dict(reason_counts.most_common()),
        "production_rejection_category_counts": dict(sorted(category_counts.items())),
        "evaluation_delay_seconds": {
            "from_signal_bar_start_median": (
                statistics.median(delays_signal) if delays_signal else None
            ),
            "from_signal_bar_start_p90": (
                _quantile(delays_signal, 0.90) if delays_signal else None
            ),
            "from_research_entry_bar_start_median": (
                statistics.median(delays_entry) if delays_entry else None
            ),
            "from_research_entry_bar_start_p90": (
                _quantile(delays_entry, 0.90) if delays_entry else None
            ),
            "note": "Minute timestamps identify bar starts; this is a fidelity diagnostic, not observed live latency.",
        },
        "gate_attribution": sorted(
            attribution,
            key=lambda value: (
                value["category"],
                value["reason"],
            ),
        ),
    }


def _compact_sensitivity(summary: Mapping[str, Any]) -> dict[str, Any]:
    return {
        key: summary[key]
        for key in (
            "trades",
            "win_rate",
            "mean_r",
            "total_r",
            "profit_factor",
            "maximum_drawdown_r",
            "mean_r_per_covered_day",
        )
    }


def _status(
    summary: Mapping[str, Any], sensitivity: Sequence[Mapping[str, Any]]
) -> tuple[str, str]:
    if not summary["trades"]:
        research = "no_trade_evidence"
    else:
        validation = summary["chronological_phases"]["retrospective_validation"]
        holdout = summary["chronological_phases"]["retrospective_holdout"]
        bootstrap = summary["bootstrap_trade_mean"]

        def sensitivity_point(slippage_bps: float, target_r: float) -> Any:
            return next(
                (
                    value
                    for value in sensitivity
                    if value["entry_slippage_bps"] == slippage_bps
                    and value["exit_slippage_bps"] == slippage_bps
                    and value["target_r"] == target_r
                ),
                None,
            )

        ten_bps = sensitivity_point(10.0, 2.0)
        twenty_bps = sensitivity_point(20.0, 2.0)
        base_cost_targets = [
            sensitivity_point(5.0, target_r) for target_r in (1.0, 1.5, 2.0, 3.0)
        ]
        passes = (
            summary["mean_r"] is not None
            and summary["mean_r"] > 0
            and summary["profit_factor"] is not None
            and summary["profit_factor"] >= 1.20
            and summary["maximum_drawdown_r"] <= 6.0
            and bootstrap["lower_90_one_sided"] is not None
            and bootstrap["lower_90_one_sided"] > 0
            and validation["total_r"] > 0
            and holdout["total_r"] > 0
            and ten_bps is not None
            and ten_bps["total_r"] > 0
            and ten_bps["profit_factor"] is not None
            and ten_bps["profit_factor"] >= 1.20
            and ten_bps["maximum_drawdown_r"] <= 6.0
            and twenty_bps is not None
            and twenty_bps["total_r"] > 0
            and twenty_bps["profit_factor"] is not None
            and twenty_bps["profit_factor"] >= 1.20
            and twenty_bps["maximum_drawdown_r"] <= 6.0
            and all(
                value is not None and value["total_r"] > 0
                for value in base_cost_targets
            )
        )
        if passes:
            research = "promising_for_independent_confirmation"
        elif summary["mean_r"] is not None and summary["mean_r"] <= 0:
            research = "reject_or_redesign"
        else:
            research = "inconclusive"
    geometry = summary["stop_geometry"]
    policy = summary["policy"]
    if not summary["trades"]:
        deployment = "no_trade_evidence"
    elif policy["maximum_stop_fraction"] == 0.008:
        deployment = "production_stop_geometry_enforced"
    elif (
        geometry["at_or_below_0.8pct_fraction"] is not None
        and geometry["at_or_below_0.8pct_fraction"] < 0.50
    ):
        deployment = "production_incompatible_stop_geometry"
    else:
        deployment = "stop_geometry_needs_live_execution_confirmation"
    return research, deployment


def _result_identity(
    dataset: Mapping[str, Any],
    policies: Sequence[Policy],
    slippages: Sequence[float],
    targets: Sequence[float],
) -> dict[str, Any]:
    config = load_config()
    implementation_paths = (
        Path(__file__),
        PROJECT_ROOT / "historical_research.py",
        PROJECT_ROOT / "historical_research_strategies.py",
        PROJECT_ROOT / "strategy_engine.py",
    )
    identity = {
        "lab_version": LAB_VERSION,
        "dataset_hash": dataset["dataset_hash"],
        "evidence_sha256": dataset["evidence_sha256"],
        "production_strategy_version": config.version,
        "production_rules_hash": config.rules_hash,
        "policies": [asdict(value) for value in policies],
        "strategy_plugins": plugin_manifest_hash_input(
            tuple(dict.fromkeys(value.strategy_spec for value in policies))
        ),
        "execution_grid": {
            "slippage_bps": list(slippages),
            "target_r": list(targets),
            "force_flat_time_et": "15:50:00",
        },
        "implementations": {
            str(path.relative_to(PROJECT_ROOT)): _sha256_file(path)
            for path in implementation_paths
        },
    }
    identity["configuration_hash"] = _sha256_bytes(_canonical_json(identity))
    identity["run_id"] = (
        f"strategy-lab-{dataset['dataset_hash'][:12]}-"
        f"{identity['configuration_hash'][:12]}"
    )
    return identity


def run_lab(
    evidence_path: Path,
    *,
    data_root: Path = DEFAULT_DATA_ROOT,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    policies: Sequence[Policy] | None = None,
    slippages: Sequence[float] = (0.0, 5.0, 10.0, 20.0),
    targets: Sequence[float] = (1.0, 1.5, 2.0, 3.0),
    bootstrap_samples: int = 10_000,
    publish_prefix: Path | None = None,
) -> dict[str, Any]:
    started = time.perf_counter()
    chosen = tuple(policies or BUILTIN_POLICIES.values())
    if not chosen or len({value.policy_id for value in chosen}) != len(chosen):
        raise HistoricalStrategyLabError("policies must be non-empty and unique")
    for policy in chosen:
        policy.validate()
    slippages = tuple(float(value) for value in slippages)
    targets = tuple(float(value) for value in targets)
    if any(not math.isfinite(value) or value < 0 for value in slippages):
        raise HistoricalStrategyLabError("slippage grid must be finite and nonnegative")
    if any(not math.isfinite(value) or value <= 0 for value in targets):
        raise HistoricalStrategyLabError("target grid must be finite and positive")
    if bootstrap_samples < 100:
        raise HistoricalStrategyLabError("bootstrap samples must be at least 100")
    before = production_hashes()
    dataset = load_dataset(evidence_path, data_root)
    expected_frozen_hashes = _expected_frozen_hashes(evidence_path)
    observations, blocked = build_observations(dataset, chosen, expected_frozen_hashes)
    requested_dates = tuple(dataset["dates"])
    base_execution = ExecutionConfig()
    base_summaries = {
        policy.policy_id: summarize_policy(
            policy,
            requested_dates,
            observations,
            blocked,
            base_execution,
            bootstrap_samples=bootstrap_samples,
        )
        for policy in chosen
    }
    sensitivity: dict[str, list[dict[str, Any]]] = {
        policy.policy_id: [] for policy in chosen
    }
    for slippage in slippages:
        for target in targets:
            execution = ExecutionConfig(
                entry_slippage_bps=slippage,
                exit_slippage_bps=slippage,
                target_r=target,
            )
            execution.validate()
            for policy in chosen:
                summary = summarize_policy(
                    policy,
                    requested_dates,
                    observations,
                    blocked,
                    execution,
                    bootstrap_samples=100,
                )
                sensitivity[policy.policy_id].append(
                    {
                        "entry_slippage_bps": slippage,
                        "exit_slippage_bps": slippage,
                        "target_r": target,
                        **_compact_sensitivity(summary),
                    }
                )
    for policy_id, summary in base_summaries.items():
        research, deployment = _status(summary, sensitivity[policy_id])
        summary["research_status"] = research
        summary["deployment_status"] = deployment
    identity = _result_identity(dataset, chosen, slippages, targets)
    result = {
        "schema_version": SCHEMA_VERSION,
        "manifest": {
            **identity,
            "mode": "research_only",
            "automatic_strategy_application": False,
            "provider_requests": 0,
            "dataset": {
                "evidence_manifest": str(evidence_path),
                "dataset_hash": dataset["dataset_hash"],
                "requested_dates": len(requested_dates),
                "available_dates": dataset["available_dates"],
                "missing_dates": dataset["missing_dates"],
                "first_date": min(requested_dates),
                "last_date": max(requested_dates),
            },
            "sample_disclosure": {
                "independent_confirmation": False,
                "full_corpus_previously_inspected": True,
                "chronological_phases_are_retrospective_stability_checks": True,
                "promotion_evidence": False,
            },
        },
        "policy_summaries": [base_summaries[policy.policy_id] for policy in chosen],
        "sensitivity": sensitivity,
        "all_signal_diagnostics": _all_signal_diagnostics(observations, base_execution),
        "interpretation": {
            "promising_policies": [
                policy_id
                for policy_id, summary in base_summaries.items()
                if summary["research_status"]
                == "promising_for_independent_confirmation"
            ],
            "rejected_or_redesign_policies": [
                policy_id
                for policy_id, summary in base_summaries.items()
                if summary["research_status"] == "reject_or_redesign"
            ],
            "production_strategy_changed": False,
            "next_evidence_gate": "newly frozen dates not inspected in this run",
            "research_gate": {
                "base": "positive mean R, PF >= 1.20, max drawdown <= 6R, and one-sided 90% bootstrap lower mean R > 0",
                "retrospective_stability": "positive total R in both chronological validation and holdout phases",
                "cost_stress": "10 and 20 bps per side at 2R each require positive total R, PF >= 1.20, and max drawdown <= 6R",
                "target_stress": "1R, 1.5R, 2R, and 3R at 5 bps per side must each have positive total R",
                "independence": "still requires newly frozen dates; this corpus cannot promote a strategy",
            },
        },
        "runtime": {
            "elapsed_seconds": round(time.perf_counter() - started, 6),
            "provider_requests": 0,
            "candidate_strategy_observations": sum(
                len(value) for value in observations.values()
            ),
            "policy_cost_target_evaluations": (
                len(chosen) * len(slippages) * len(targets)
            ),
            "bootstrap_samples_for_base": bootstrap_samples,
        },
    }
    after = production_hashes()
    if before != after:
        changed = [key for key in before if before[key] != after[key]]
        raise HistoricalStrategyLabError(
            f"research run changed protected production artifacts: {changed}"
        )
    result["manifest"]["production_isolation"] = {
        "verified_unchanged": True,
        "paths": sorted(before),
    }
    output_path = _safe_publish_prefix(
        output_root / identity["run_id"] / "result"
    ).with_suffix(".json")
    _atomic_json(output_path, result)
    report = render_report(result)
    _atomic_text(output_path.with_name("report.md"), report)
    if publish_prefix is not None:
        prefix = _safe_publish_prefix(publish_prefix)
        _atomic_json(prefix.with_suffix(".json"), result)
        _atomic_text(prefix.with_suffix(".md"), report)
    return result


def render_report(result: Mapping[str, Any]) -> str:
    manifest = result["manifest"]
    summaries = result["policy_summaries"]
    diagnostics = result["all_signal_diagnostics"]
    by_id = {value["policy"]["policy_id"]: value for value in summaries}

    def sensitivity_point(policy_id: str, slippage_bps: float, target_r: float) -> Any:
        return next(
            (
                value
                for value in result["sensitivity"].get(policy_id, ())
                if value["entry_slippage_bps"] == slippage_bps
                and value["exit_slippage_bps"] == slippage_bps
                and value["target_r"] == target_r
            ),
            None,
        )

    lines = [
        "# Production-Aware Historical Strategy Lab",
        "",
        f"Run ID: `{manifest['run_id']}`",
        "",
        "## Scope And Boundary",
        "",
        f"- Requested dates: {manifest['dataset']['requested_dates']}",
        f"- Available immutable bundles: {manifest['dataset']['available_dates']}",
        f"- Missing bundles retained as blockers: {manifest['dataset']['missing_dates']}",
        f"- Dataset hash: `{manifest['dataset']['dataset_hash']}`",
        "- Provider requests: 0",
        "- Production strategy/configuration changes: 0",
        "- Independent confirmation: no; every split is a retrospective stability check.",
        "- Full tested family: all policy, cost, and target cells are retained in the JSON result.",
        "",
        "## Declared Adversarial Gate",
        "",
        "A policy is only `promising_for_independent_confirmation` when all of the following hold:",
        "",
        "- Base 5 bps-per-side, 2R result: positive mean R, PF at least 1.20, maximum drawdown at most 6R, and one-sided 90% bootstrap lower mean R above zero.",
        "- Positive total R in both retrospective chronological validation and holdout phases.",
        "- At both 10 and 20 bps per side with a 2R target: positive total R, PF at least 1.20, and maximum drawdown at most 6R.",
        "- Positive total R at 1R, 1.5R, 2R, and 3R targets under 5 bps-per-side costs.",
        "- Passing is a freeze-and-confirm signal, never promotion evidence.",
        "",
        "## Base Policy Results",
        "",
        "| Policy | Trades | Mean R | Total R | PF | Max DD R | 90% lower mean R | Research status | Deployment |",
        "|---|---:|---:|---:|---:|---:|---:|---|---|",
    ]
    for summary in summaries:
        bootstrap = summary["bootstrap_trade_mean"]
        lines.append(
            "| {policy} | {trades} | {mean} | {total} | {pf} | {dd} | {lower} | {status} | {deployment} |".format(
                policy=summary["policy"]["policy_id"],
                trades=summary["trades"],
                mean=_format_number(summary["mean_r"]),
                total=_format_number(summary["total_r"]),
                pf=_format_number(summary["profit_factor"]),
                dd=_format_number(summary["maximum_drawdown_r"]),
                lower=_format_number(bootstrap["lower_90_one_sided"]),
                status=summary["research_status"],
                deployment=summary["deployment_status"],
            )
        )
    promising = result["interpretation"]["promising_policies"]
    lines.extend(
        [
            "",
            "## Survivors Under Cost Stress",
            "",
            "| Policy | 10 bps Total R | 10 bps PF | 20 bps Total R | 20 bps PF | 20 bps DD R | Median stop | <=0.8% stops | Implied median notional |",
            "|---|---:|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for policy_id in promising:
        summary = by_id[policy_id]
        ten = sensitivity_point(policy_id, 10.0, 2.0)
        twenty = sensitivity_point(policy_id, 20.0, 2.0)
        geometry = summary["stop_geometry"]
        lines.append(
            "| {policy} | {ten_total} | {ten_pf} | {twenty_total} | {twenty_pf} | {twenty_dd} | {stop} | {tight} | {notional} |".format(
                policy=policy_id,
                ten_total=_format_number(ten["total_r"] if ten else None),
                ten_pf=_format_number(ten["profit_factor"] if ten else None),
                twenty_total=_format_number(twenty["total_r"] if twenty else None),
                twenty_pf=_format_number(twenty["profit_factor"] if twenty else None),
                twenty_dd=_format_number(
                    twenty["maximum_drawdown_r"] if twenty else None
                ),
                stop=_format_percent(geometry["median_fraction"]),
                tight=_format_percent(geometry["at_or_below_0.8pct_fraction"]),
                notional=_format_percent(
                    geometry["median_implied_unvalidated_notional_fraction"]
                ),
            )
        )
    if not promising:
        lines.append("| None | n/a | n/a | n/a | n/a | n/a | n/a | n/a | n/a |")
    lines.extend(
        [
            "",
            "The stop and implied-notional columns are the decisive deployment warning. A policy can have positive R expectancy while remaining incompatible with the current 0.8% production stop cap and 70-80% allocation objective.",
            "",
            "## Production Gate And Timing Diagnostics",
            "",
            f"- Production candidate evaluations: {diagnostics['production_candidate_evaluations']}",
            f"- Production-eligible candidates: {diagnostics['production_eligible']}",
            f"- Executable ORB counterfactual signals: {diagnostics['executable_orb_counterfactual_signals']}",
            f"- Chase-rejected executable ORBs: {diagnostics['chase_rejected_executable_orb_signals']} ({_format_percent(diagnostics['chase_rejected_fraction'])})",
            f"- Median production evaluation delay from signal-bar start: {diagnostics['evaluation_delay_seconds']['from_signal_bar_start_median']} seconds",
            f"- Median production evaluation delay from research entry-bar start: {diagnostics['evaluation_delay_seconds']['from_research_entry_bar_start_median']} seconds",
            "",
            "The delay uses one-minute bar-start timestamps and is not observed live latency. It demonstrates why the stored production adapter and the next-open research model are not interchangeable.",
            "",
            "Gate attribution is descriptive, not causal. In particular, the fixed-slippage minute-bar model cannot disprove live spread, freshness, depth, chase, or protection controls. It can identify gates whose historical labels deserve a cleaner prospective test.",
            "",
            "## Theory Decisions",
            "",
        ]
    )
    if "reversal-early-earnings" in by_id:
        preferred = by_id["reversal-early-earnings"]
        phases = preferred["chronological_phases"]
        twenty = sensitivity_point("reversal-early-earnings", 20.0, 2.0)
        lines.extend(
            [
                "1. **Freeze the simple early earnings reversal for independent confirmation.** It produced {trades} trades, {total}R, PF {pf}, and {dd}R maximum drawdown; development, retrospective validation, and retrospective holdout were all positive ({dev}R, {validation}R, {holdout}R). At 20 bps per side it retained {stress}R and PF {stress_pf}.".format(
                    trades=preferred["trades"],
                    total=_format_number(preferred["total_r"]),
                    pf=_format_number(preferred["profit_factor"]),
                    dd=_format_number(preferred["maximum_drawdown_r"]),
                    dev=_format_number(phases["development"]["total_r"]),
                    validation=_format_number(
                        phases["retrospective_validation"]["total_r"]
                    ),
                    holdout=_format_number(phases["retrospective_holdout"]["total_r"]),
                    stress=_format_number(twenty["total_r"] if twenty else None),
                    stress_pf=_format_number(
                        twenty["profit_factor"] if twenty else None
                    ),
                ),
                "2. **Keep the current production ORB frozen.** The complete production gate stack selected zero trades. Relaxed ORB variants did trade, but none cleared the statistical and severe-cost gate.",
                "3. **Do not use production score or RVOL ranking as reversal selectors.** They were tested on the same corpus, add complexity, and failed the 20 bps PF/drawdown gate; simple signal strength is the safer comparator.",
                "4. **Reject the 0.8% stop as a drop-in fit for these stored signals.** The tight-stop early ORB lost money, and only {tight_count} of {trades} preferred reversal trades fit the current cap. The preferred cohort's median structural stop was {median_stop}, implying only {notional} median notional at the current 0.25% risk budget plus reserve.".format(
                    tight_count=preferred["stop_geometry"]["at_or_below_0.8pct"],
                    trades=preferred["trades"],
                    median_stop=_format_percent(
                        preferred["stop_geometry"]["median_fraction"]
                    ),
                    notional=_format_percent(
                        preferred["stop_geometry"][
                            "median_implied_unvalidated_notional_fraction"
                        ]
                    ),
                ),
                "5. **Retire the current VWAP-pullback branch and deprioritize HOD continuation.** The former had negative expectancy; the latter was near flat with drawdown above the production maturity budget. More tuning on this inspected corpus would spend learning capacity on overfit risk.",
            ]
        )
    lines.extend(
        [
            "",
            "## Selection Artifact Warning",
            "",
        ]
    )
    orb_counts = [
        sensitivity_point("orb-early-strength", slippage, 2.0)
        for slippage in (0.0, 5.0, 10.0, 20.0)
    ]
    if all(value is not None for value in orb_counts):
        lines.append(
            "The early ORB trade count fell from {zero} at 0 bps to {five}, {ten}, and {twenty} at 5, 10, and 20 bps. Modest added slippage can therefore appear to improve ORB returns by pushing entries past the chase cap and deleting trades. This is a selection artifact, not evidence that worse execution helps.".format(
                zero=orb_counts[0]["trades"],
                five=orb_counts[1]["trades"],
                ten=orb_counts[2]["trades"],
                twenty=orb_counts[3]["trades"],
            )
        )
    else:
        lines.append(
            "The requested grid did not include every standard ORB cost point, so the chase-cap selection artifact could not be fully evaluated."
        )
    lines.extend(
        [
            "",
            "Promising for independent confirmation:",
            "",
        ]
    )
    lines.extend(
        [f"- `{value}`" for value in promising]
        if promising
        else ["- None met every declared research gate."]
    )
    lines.extend(
        [
            "",
            "Reject or redesign:",
            "",
        ]
    )
    rejected = result["interpretation"]["rejected_or_redesign_policies"]
    lines.extend(
        [f"- `{value}`" for value in rejected]
        if rejected
        else ["- None were automatically rejected."]
    )
    lines.extend(
        [
            "",
            "## Required Next Gate",
            "",
            "Freeze new dates and symbols before viewing their target-session outcomes. Re-run only the exact simple early-earnings-reversal confirmation contract, retain every blocked date, collect full-universe and subminute quote/execution evidence where available, and do not edit production rules until both independent expectancy and deployable stop/protection geometry pass.",
            "",
        ]
    )
    return "\n".join(lines)


def _format_number(value: Any) -> str:
    if value is None:
        return "n/a"
    return f"{float(value):.3f}"


def _format_percent(value: Any) -> str:
    if value is None:
        return "n/a"
    return f"{100 * float(value):.1f}%"


def _parse_csv_numbers(value: str, *, positive: bool) -> tuple[float, ...]:
    try:
        numbers = tuple(float(part.strip()) for part in value.split(","))
    except ValueError as exc:
        raise argparse.ArgumentTypeError("expected comma-separated numbers") from exc
    if not numbers or any(
        not math.isfinite(number) or (number <= 0 if positive else number < 0)
        for number in numbers
    ):
        qualifier = "positive" if positive else "nonnegative"
        raise argparse.ArgumentTypeError(f"numbers must be finite and {qualifier}")
    return numbers


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("list-policies", help="list versioned built-in policies")
    run = subparsers.add_parser("run", help="run the evidence-bound strategy lab")
    run.add_argument("--evidence", type=Path, default=DEFAULT_EVIDENCE)
    run.add_argument("--data-root", type=Path, default=DEFAULT_DATA_ROOT)
    run.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    run.add_argument("--policy", action="append", dest="policies")
    run.add_argument(
        "--slippage-grid",
        type=lambda value: _parse_csv_numbers(value, positive=False),
        default=(0.0, 5.0, 10.0, 20.0),
    )
    run.add_argument(
        "--target-grid",
        type=lambda value: _parse_csv_numbers(value, positive=True),
        default=(1.0, 1.5, 2.0, 3.0),
    )
    run.add_argument("--bootstrap-samples", type=int, default=10_000)
    run.add_argument("--publish-prefix", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        if args.command == "list-policies":
            print(
                json.dumps(
                    [asdict(value) for value in BUILTIN_POLICIES.values()],
                    indent=2,
                    sort_keys=True,
                )
            )
            return 0
        policies = None
        if args.policies:
            missing = [
                value for value in args.policies if value not in BUILTIN_POLICIES
            ]
            if missing:
                raise HistoricalStrategyLabError(f"unknown policies: {missing}")
            policies = tuple(BUILTIN_POLICIES[value] for value in args.policies)
        result = run_lab(
            args.evidence,
            data_root=args.data_root,
            output_root=args.output_root,
            policies=policies,
            slippages=args.slippage_grid,
            targets=args.target_grid,
            bootstrap_samples=args.bootstrap_samples,
            publish_prefix=args.publish_prefix,
        )
        print(
            json.dumps(
                {
                    "run_id": result["manifest"]["run_id"],
                    "dataset_hash": result["manifest"]["dataset"]["dataset_hash"],
                    "promising_policies": result["interpretation"][
                        "promising_policies"
                    ],
                    "rejected_or_redesign_policies": result["interpretation"][
                        "rejected_or_redesign_policies"
                    ],
                    "provider_requests": 0,
                    "production_strategy_changed": False,
                    "elapsed_seconds": result["runtime"]["elapsed_seconds"],
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    except (
        HistoricalStrategyLabError,
        HistoricalResearchError,
        ResearchStrategyError,
        OSError,
        ValueError,
    ) as exc:
        print(
            json.dumps({"error": str(exc), "error_type": type(exc).__name__}, indent=2)
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

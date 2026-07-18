"""Selection-aware daily account simulation and strategy research statistics."""

from __future__ import annotations

import argparse
import hashlib
import itertools
import json
import math
import random
import statistics
from collections.abc import Mapping, Sequence
from datetime import date
from pathlib import Path
from statistics import NormalDist
from typing import Any


SCHEMA_VERSION = 1
DEFAULT_ALPHA = 0.10
DEFAULT_POWER = 0.80
TRADING_DAYS_PER_YEAR = 252
OUTCOMES = {"filled", "missed", "rejected", "no_signal"}


class LearningStatisticsError(ValueError):
    """Raised when an account path or statistical family is malformed."""


def _finite(value: Any, field: str) -> float:
    if isinstance(value, bool):
        raise LearningStatisticsError(f"{field} must be numeric")
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise LearningStatisticsError(f"{field} must be numeric") from exc
    if not math.isfinite(result):
        raise LearningStatisticsError(f"{field} must be finite")
    return result


def _fingerprint(value: Any) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def materialize_account_path(
    days: Sequence[Mapping[str, Any]], *, starting_equity: float
) -> list[dict[str, Any]]:
    """Materialize one explicit account return for every requested trading day."""
    equity = _finite(starting_equity, "starting_equity")
    if equity <= 0:
        raise LearningStatisticsError("starting_equity must be positive")
    result: list[dict[str, Any]] = []
    seen_dates: set[str] = set()
    previous_day: date | None = None
    for index, raw in enumerate(days):
        if not isinstance(raw, Mapping):
            raise LearningStatisticsError(f"days[{index}] must be an object")
        day_text = raw.get("date")
        if not isinstance(day_text, str):
            raise LearningStatisticsError(f"days[{index}].date must be ISO text")
        try:
            parsed_day = date.fromisoformat(day_text)
        except ValueError as exc:
            raise LearningStatisticsError(
                f"days[{index}].date must be an ISO date"
            ) from exc
        if day_text in seen_dates or (
            previous_day is not None and parsed_day <= previous_day
        ):
            raise LearningStatisticsError(
                "account-path dates must be unique and chronological"
            )
        seen_dates.add(day_text)
        previous_day = parsed_day
        outcome = raw.get("outcome")
        if outcome not in OUTCOMES:
            raise LearningStatisticsError(f"days[{index}].outcome is invalid")
        signal_intent = raw.get("signal_intent")
        order_intent = raw.get("order_intent")
        if not isinstance(signal_intent, bool) or not isinstance(order_intent, bool):
            raise LearningStatisticsError(
                f"days[{index}] needs boolean signal_intent and order_intent"
            )
        if outcome == "no_signal" and (signal_intent or order_intent):
            raise LearningStatisticsError(
                "no_signal cannot have signal or order intent"
            )
        if outcome == "rejected" and order_intent:
            raise LearningStatisticsError("rejected signal cannot have order intent")
        if outcome in {"filled", "missed"} and not (signal_intent and order_intent):
            raise LearningStatisticsError(
                f"{outcome} requires both signal and order intent"
            )
        gross = _finite(
            raw.get("gross_return_fraction", 0.0),
            f"days[{index}].gross_return_fraction",
        )
        cost = _finite(raw.get("cost_fraction", 0.0), f"days[{index}].cost_fraction")
        if cost < 0:
            raise LearningStatisticsError("cost_fraction cannot be negative")
        if outcome != "filled" and (gross != 0 or cost != 0):
            raise LearningStatisticsError(
                "missed, rejected, and no-signal days must remain explicit zero returns"
            )
        net = gross - cost if outcome == "filled" else 0.0
        if net <= -1:
            raise LearningStatisticsError(
                "a daily account return cannot lose 100% or more"
            )
        starting_day_equity = equity
        equity *= 1 + net
        result.append(
            {
                "date": day_text,
                "signal_id": raw.get("signal_id"),
                "signal_intent": signal_intent,
                "detection_status": raw.get("detection_status", "none"),
                "order_intent": order_intent,
                "outcome": outcome,
                "protection_status": raw.get("protection_status", "not_applicable"),
                "gross_return_fraction": gross,
                "cost_fraction": cost,
                "net_return_fraction": net,
                "starting_equity": starting_day_equity,
                "ending_equity": equity,
                "log_return": math.log1p(net),
            }
        )
    if not result:
        raise LearningStatisticsError("account path needs at least one requested day")
    return result


def maximum_drawdown_fraction(returns: Sequence[float]) -> float:
    equity = 1.0
    peak = 1.0
    maximum = 0.0
    for value in returns:
        daily = _finite(value, "daily return")
        if daily <= -1:
            raise LearningStatisticsError("daily return cannot lose 100% or more")
        equity *= 1 + daily
        peak = max(peak, equity)
        maximum = max(maximum, 1 - equity / peak)
    return maximum


def profit_factor(returns: Sequence[float]) -> float | None:
    gains = sum(value for value in returns if value > 0)
    losses = abs(sum(value for value in returns if value < 0))
    if losses == 0:
        return math.inf if gains > 0 else None
    return gains / losses


def annualized_sharpe(returns: Sequence[float]) -> float | None:
    if len(returns) < 2:
        return None
    deviation = statistics.stdev(returns)
    if deviation == 0:
        return None
    return statistics.fmean(returns) / deviation * math.sqrt(TRADING_DAYS_PER_YEAR)


def stationary_bootstrap_means(
    values: Sequence[float],
    *,
    samples: int = 10_000,
    seed: int | None = None,
    mean_block_length: int | None = None,
) -> list[float]:
    """Stationary bootstrap means that preserve short-range day dependence."""
    normalized = [_finite(value, "bootstrap value") for value in values]
    if not normalized:
        raise LearningStatisticsError("stationary bootstrap needs values")
    if samples < 100:
        raise LearningStatisticsError("stationary bootstrap needs at least 100 samples")
    block = mean_block_length or max(5, round(math.sqrt(len(normalized))))
    if block < 1:
        raise LearningStatisticsError("mean_block_length must be positive")
    restart_probability = 1 / block
    if seed is None:
        seed = int(_fingerprint([round(item, 12) for item in normalized])[:16], 16)
    rng = random.Random(seed)
    count = len(normalized)
    means: list[float] = []
    for _ in range(samples):
        index = rng.randrange(count)
        sample: list[float] = []
        for position in range(count):
            if position and rng.random() < restart_probability:
                index = rng.randrange(count)
            sample.append(normalized[index])
            index = (index + 1) % count
        means.append(statistics.fmean(sample))
    return means


def stationary_bootstrap_summary(
    values: Sequence[float],
    *,
    confidence: float = 0.90,
    samples: int = 10_000,
    seed: int | None = None,
) -> dict[str, Any]:
    if not 0.5 < confidence < 1:
        raise LearningStatisticsError("confidence must be between 0.5 and 1")
    block = max(5, round(math.sqrt(len(values))))
    means = sorted(
        stationary_bootstrap_means(
            values, samples=samples, seed=seed, mean_block_length=block
        )
    )
    lower_index = max(0, math.ceil((1 - confidence) * len(means)) - 1)
    upper_fraction = 1 - (1 - confidence) / 2
    upper_index = min(len(means) - 1, math.ceil(upper_fraction * len(means)) - 1)
    return {
        "samples": samples,
        "mean_block_length": block,
        "lower_one_sided": means[lower_index],
        "central_lower": means[
            max(0, math.ceil(((1 - confidence) / 2) * len(means)) - 1)
        ],
        "central_upper": means[upper_index],
    }


def _skewness(values: Sequence[float]) -> float:
    if len(values) < 3:
        return 0.0
    mean = statistics.fmean(values)
    second = statistics.fmean((value - mean) ** 2 for value in values)
    if second == 0:
        return 0.0
    third = statistics.fmean((value - mean) ** 3 for value in values)
    return third / second**1.5


def _kurtosis(values: Sequence[float]) -> float:
    if len(values) < 4:
        return 3.0
    mean = statistics.fmean(values)
    second = statistics.fmean((value - mean) ** 2 for value in values)
    if second == 0:
        return 3.0
    fourth = statistics.fmean((value - mean) ** 4 for value in values)
    return fourth / second**2


def deflated_sharpe_probability(
    returns: Sequence[float], trial_sharpes: Sequence[float]
) -> dict[str, Any]:
    """Return the Bailey-Lopez de Prado deflated Sharpe probability."""
    normalized = [_finite(value, "daily return") for value in returns]
    trials = [_finite(value, "trial Sharpe") for value in trial_sharpes]
    observed_annual = annualized_sharpe(normalized)
    if observed_annual is None or len(normalized) < 3:
        return {
            "probability": None,
            "observed_sharpe": observed_annual,
            "benchmark_sharpe": None,
        }
    observed = observed_annual / math.sqrt(TRADING_DAYS_PER_YEAR)
    if len(trials) < 2:
        benchmark_annual = trials[0] if trials else 0.0
    else:
        trial_deviation = statistics.stdev(trials)
        count = len(trials)
        gamma = 0.5772156649015329
        normal = NormalDist()
        expected_maximum = (1 - gamma) * normal.inv_cdf(
            1 - 1 / count
        ) + gamma * normal.inv_cdf(1 - 1 / (count * math.e))
        benchmark_annual = statistics.fmean(trials) + trial_deviation * expected_maximum
    benchmark = benchmark_annual / math.sqrt(TRADING_DAYS_PER_YEAR)
    skew = _skewness(normalized)
    kurtosis = _kurtosis(normalized)
    denominator_squared = 1 - skew * observed + ((kurtosis - 1) / 4) * observed**2
    if denominator_squared <= 0:
        probability = None
    else:
        statistic = (observed - benchmark) * math.sqrt(len(normalized) - 1)
        statistic /= math.sqrt(denominator_squared)
        probability = NormalDist().cdf(statistic)
    return {
        "probability": probability,
        "observed_sharpe": observed_annual,
        "benchmark_sharpe": benchmark_annual,
        "trials": len(trials),
        "skewness": skew,
        "kurtosis": kurtosis,
    }


def holm_family_decisions(
    p_values: Mapping[str, Any], *, alpha: float = DEFAULT_ALPHA
) -> dict[str, dict[str, Any]]:
    if not 0 < alpha < 1:
        raise LearningStatisticsError("alpha must be between zero and one")
    normalized = {
        name: _finite(value, f"p_values.{name}") for name, value in p_values.items()
    }
    if any(not 0 <= value <= 1 for value in normalized.values()):
        raise LearningStatisticsError("p-values must be between zero and one")
    ordered = sorted(normalized.items(), key=lambda item: (item[1], item[0]))
    decisions: dict[str, dict[str, Any]] = {}
    family_size = len(ordered)
    still_rejecting = True
    for index, (name, value) in enumerate(ordered):
        threshold = alpha / (family_size - index)
        rejected = still_rejecting and value <= threshold
        if not rejected:
            still_rejecting = False
        decisions[name] = {
            "p_value": value,
            "threshold": threshold,
            "reject_null": rejected,
        }
    return decisions


def power_sample_target(
    effect_mean: float,
    standard_deviation: float,
    *,
    alpha: float = DEFAULT_ALPHA,
    power: float = DEFAULT_POWER,
    configured_floor: int = 0,
) -> int:
    effect = abs(_finite(effect_mean, "effect_mean"))
    deviation = _finite(standard_deviation, "standard_deviation")
    if effect == 0 or deviation <= 0:
        raise LearningStatisticsError("effect and standard deviation must be positive")
    if not 0 < alpha < 0.5 or not 0.5 < power < 1:
        raise LearningStatisticsError("alpha or power is outside the supported range")
    normal = NormalDist()
    estimate = math.ceil(
        ((normal.inv_cdf(1 - alpha) + normal.inv_cdf(power)) * deviation / effect) ** 2
    )
    return max(configured_floor, estimate)


def probability_of_backtest_overfitting(
    strategy_daily_returns: Mapping[str, Sequence[float]], *, slices: int = 8
) -> dict[str, Any]:
    """Estimate PBO from symmetric combinatorial chronological partitions."""
    if slices < 4 or slices % 2:
        raise LearningStatisticsError(
            "PBO slices must be an even integer of at least four"
        )
    if len(strategy_daily_returns) < 2:
        return {
            "probability": None,
            "combinations": 0,
            "reason": "needs at least two strategies",
        }
    normalized = {
        name: [_finite(value, f"{name} daily return") for value in values]
        for name, values in strategy_daily_returns.items()
    }
    lengths = {len(values) for values in normalized.values()}
    if len(lengths) != 1:
        raise LearningStatisticsError(
            "PBO strategies must share the same requested days"
        )
    observations = lengths.pop()
    if observations < slices * 2:
        return {
            "probability": None,
            "combinations": 0,
            "reason": "insufficient days for PBO",
        }
    blocks: list[list[int]] = []
    for block in range(slices):
        start = round(block * observations / slices)
        end = round((block + 1) * observations / slices)
        blocks.append(list(range(start, end)))
    lambdas: list[float] = []
    half = slices // 2
    for selected in itertools.combinations(range(slices), half):
        # Complementary partitions are symmetric; keep one canonical orientation.
        if 0 not in selected:
            continue
        selected_set = set(selected)
        in_sample = [index for block in selected for index in blocks[block]]
        out_sample = [
            index
            for block in range(slices)
            if block not in selected_set
            for index in blocks[block]
        ]
        in_scores = {
            name: statistics.fmean(values[index] for index in in_sample)
            for name, values in normalized.items()
        }
        winner = max(in_scores, key=lambda name: (in_scores[name], name))
        out_scores = {
            name: statistics.fmean(values[index] for index in out_sample)
            for name, values in normalized.items()
        }
        ordered = sorted(out_scores, key=lambda name: (out_scores[name], name))
        rank = ordered.index(winner) + 1
        percentile = (rank - 0.5) / len(ordered)
        lambdas.append(math.log(percentile / (1 - percentile)))
    return {
        "probability": sum(value <= 0 for value in lambdas) / len(lambdas),
        "combinations": len(lambdas),
        "negative_logit_count": sum(value <= 0 for value in lambdas),
        "median_logit": statistics.median(lambdas),
    }


def analyze_account_path(
    path: Sequence[Mapping[str, Any]],
    *,
    trial_sharpes: Sequence[float],
    family_p_values: Mapping[str, Any],
    strategy_daily_returns: Mapping[str, Sequence[float]],
    bootstrap_samples: int = 10_000,
) -> dict[str, Any]:
    returns = [
        _finite(day["net_return_fraction"], "net_return_fraction") for day in path
    ]
    filled = sum(day["outcome"] == "filled" for day in path)
    zeros = sum(value == 0 for value in returns)
    mean = statistics.fmean(returns)
    deviation = statistics.stdev(returns) if len(returns) > 1 else 0.0
    factor = profit_factor(returns)
    return {
        "requested_days": len(path),
        "filled_days": filled,
        "zero_return_days": zeros,
        "coverage_fraction": filled / len(path),
        "mean_daily_return": mean,
        "total_log_growth": sum(math.log1p(value) for value in returns),
        "compounded_return": math.prod(1 + value for value in returns) - 1,
        "profit_factor": "Infinity"
        if factor is not None and math.isinf(factor)
        else factor,
        "maximum_drawdown_fraction": maximum_drawdown_fraction(returns),
        "annualized_sharpe": annualized_sharpe(returns),
        "stationary_bootstrap": stationary_bootstrap_summary(
            returns, samples=bootstrap_samples
        ),
        "deflated_sharpe": deflated_sharpe_probability(returns, trial_sharpes),
        "holm_family": holm_family_decisions(family_p_values),
        "pbo": probability_of_backtest_overfitting(strategy_daily_returns),
        "power_target": (
            power_sample_target(mean, deviation, configured_floor=50)
            if mean != 0 and deviation > 0
            else None
        ),
    }


def evaluate_contract(value: Mapping[str, Any]) -> dict[str, Any]:
    if value.get("schema_version") != SCHEMA_VERSION:
        raise LearningStatisticsError(f"schema_version must be {SCHEMA_VERSION}")
    for field in ("experiment_id", "family_id"):
        if not isinstance(value.get(field), str) or not value[field].strip():
            raise LearningStatisticsError(f"{field} must be non-empty")
    path = materialize_account_path(
        value.get("days", []), starting_equity=value.get("starting_equity", 0)
    )
    metrics = analyze_account_path(
        path,
        trial_sharpes=value.get("trial_sharpes", []),
        family_p_values=value.get("family_p_values", {}),
        strategy_daily_returns=value.get("strategy_daily_returns", {}),
        bootstrap_samples=int(value.get("bootstrap_samples", 10_000)),
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "experiment_id": value["experiment_id"],
        "family_id": value["family_id"],
        "contract_sha256": _fingerprint(value),
        "account_path": path,
        "metrics": metrics,
    }


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise LearningStatisticsError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise LearningStatisticsError("input must contain a JSON object")
    return value


def _write_json(path: Path, value: Mapping[str, Any]) -> None:
    rendered = json.dumps(value, indent=2, sort_keys=True) + "\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(rendered, encoding="utf-8")
    temporary.replace(path)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("input", type=Path)
    parser.add_argument("--output", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        result = evaluate_contract(_read_json(args.input))
        if args.output:
            _write_json(args.output, result)
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    except (LearningStatisticsError, OSError, ValueError) as exc:
        print(
            json.dumps({"error": str(exc), "error_type": type(exc).__name__}, indent=2)
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

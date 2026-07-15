"""Evidence-based maturity assessment for a frozen strategy version."""

from __future__ import annotations

import hashlib
import math
import random
import statistics
from dataclasses import asdict, dataclass
from typing import Any, Mapping, Sequence

from strategy_engine import StrategyConfig, StrategyInputError, load_config


@dataclass(frozen=True)
class PerformanceMetrics:
    closed_signals: int
    live_signals: int
    confirmation_signals: int
    stop_execution_signals: int
    expectancy_r: float | None
    confirmation_expectancy_r: float | None
    profit_factor: float | None
    win_rate: float | None
    average_win_r: float | None
    average_loss_r: float | None
    maximum_drawdown_r: float
    bootstrap_lower_expectancy_r: float | None
    entry_slippage_p95_bps: float | None
    unprotected_p95_seconds: float | None
    stop_slippage_excess_p95_bps: float | None
    rule_violations: int
    incomplete_execution_records: int
    incomplete_capture_records: int
    mismatched_rule_records: int


@dataclass(frozen=True)
class MaturityAssessment:
    strategy_version: str
    rules_hash: str
    earned_maturity: str
    metrics: PerformanceMetrics
    provisional_blockers: tuple[str, ...]
    validated_blockers: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        def json_safe(value: Any) -> Any:
            if isinstance(value, float) and math.isinf(value):
                return "Infinity" if value > 0 else "-Infinity"
            if isinstance(value, dict):
                return {key: json_safe(child) for key, child in value.items()}
            if isinstance(value, list):
                return [json_safe(child) for child in value]
            if isinstance(value, tuple):
                return [json_safe(child) for child in value]
            return value

        return json_safe(asdict(self))


def _finite_number(value: Any, field: str) -> float:
    if isinstance(value, bool):
        raise StrategyInputError(f"{field} must be numeric")
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise StrategyInputError(f"{field} must be numeric") from exc
    if not math.isfinite(result):
        raise StrategyInputError(f"{field} must be finite")
    return result


def _percentile(values: Sequence[float], fraction: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    rank = max(0, math.ceil(fraction * len(ordered)) - 1)
    return ordered[rank]


def _profit_factor(values: Sequence[float]) -> float | None:
    if not values:
        return None
    gains = sum(value for value in values if value > 0)
    losses = abs(sum(value for value in values if value < 0))
    if losses == 0:
        return math.inf if gains > 0 else None
    return gains / losses


def _maximum_drawdown(values: Sequence[float]) -> float:
    equity = 0.0
    peak = 0.0
    maximum = 0.0
    for value in values:
        equity += value
        peak = max(peak, equity)
        maximum = max(maximum, peak - equity)
    return maximum


def bootstrap_lower_mean(
    values: Sequence[float], confidence: float, iterations: int = 5000
) -> float | None:
    """Return a deterministic non-parametric lower confidence bound for mean R."""
    if not values:
        return None
    if not 0.5 < confidence < 1:
        raise StrategyInputError("bootstrap confidence must be between 0.5 and 1")
    seed_material = json_fingerprint([round(value, 10) for value in values])
    rng = random.Random(int(seed_material[:16], 16))
    sample_size = len(values)
    means = [
        statistics.fmean(rng.choice(values) for _ in range(sample_size))
        for _ in range(iterations)
    ]
    return _percentile(means, 1 - confidence)


def json_fingerprint(value: Any) -> str:
    import json

    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def _closed_records(
    records: Sequence[Mapping[str, Any]], config: StrategyConfig
) -> tuple[list[Mapping[str, Any]], int, int]:
    relevant: list[Mapping[str, Any]] = []
    incomplete_capture = 0
    mismatched_rules = 0
    for index, record in enumerate(records):
        if record.get("strategy_version") != config.version:
            continue
        if record.get("rules_hash") != config.rules_hash:
            mismatched_rules += 1
            continue
        if record.get("closed") is not True or record.get("triggered") is not True:
            continue
        if record.get("session_capture_complete") is not True:
            incomplete_capture += 1
        _finite_number(record.get("net_r"), f"records[{index}].net_r")
        relevant.append(record)
    relevant.sort(
        key=lambda item: (str(item.get("date", "")), str(item.get("signal_id", "")))
    )
    return relevant, incomplete_capture, mismatched_rules


def calculate_metrics(
    records: Sequence[Mapping[str, Any]], config: StrategyConfig | None = None
) -> PerformanceMetrics:
    config = config or load_config()
    closed, incomplete_capture, mismatched_rules = _closed_records(records, config)
    values = [_finite_number(record["net_r"], "net_r") for record in closed]
    confirmation = [
        _finite_number(record["net_r"], "net_r")
        for record in closed
        if record.get("sample_phase") == "confirmation"
    ]
    live = [record for record in closed if record.get("mode") == "live"]
    stopped = [record for record in live if record.get("stop_executed") is True]
    entry_slippage = [
        _finite_number(record["entry_slippage_bps"], "entry_slippage_bps")
        for record in live
        if record.get("entry_slippage_bps") is not None
    ]
    unprotected = [
        _finite_number(record["unprotected_seconds"], "unprotected_seconds")
        for record in live
        if record.get("unprotected_seconds") is not None
    ]
    stop_excess = [
        _finite_number(record["stop_slippage_bps"], "stop_slippage_bps")
        - _finite_number(record["stop_reserve_bps"], "stop_reserve_bps")
        for record in stopped
        if record.get("stop_slippage_bps") is not None
        and record.get("stop_reserve_bps") is not None
    ]
    violations = sum(
        len(record.get("rule_violations", []))
        for record in closed
        if isinstance(record.get("rule_violations", []), list)
    )
    wins = [value for value in values if value > 0]
    losses = [value for value in values if value < 0]
    incomplete_execution = sum(
        record.get("entry_slippage_bps") is None
        or record.get("unprotected_seconds") is None
        for record in live
    ) + sum(
        record.get("stop_slippage_bps") is None
        or record.get("stop_reserve_bps") is None
        for record in stopped
    )
    confidence = float(
        config.raw["promotion"]["validated"]["minimum_bootstrap_confidence"]
    )
    return PerformanceMetrics(
        closed_signals=len(values),
        live_signals=len(live),
        confirmation_signals=len(confirmation),
        stop_execution_signals=len(stopped),
        expectancy_r=statistics.fmean(values) if values else None,
        confirmation_expectancy_r=(
            statistics.fmean(confirmation) if confirmation else None
        ),
        profit_factor=_profit_factor(values),
        win_rate=len(wins) / len(values) if values else None,
        average_win_r=statistics.fmean(wins) if wins else None,
        average_loss_r=statistics.fmean(losses) if losses else None,
        maximum_drawdown_r=_maximum_drawdown(values),
        bootstrap_lower_expectancy_r=bootstrap_lower_mean(values, confidence),
        entry_slippage_p95_bps=_percentile(entry_slippage, 0.95),
        unprotected_p95_seconds=_percentile(unprotected, 0.95),
        stop_slippage_excess_p95_bps=_percentile(stop_excess, 0.95),
        rule_violations=violations,
        incomplete_execution_records=incomplete_execution,
        incomplete_capture_records=incomplete_capture,
        mismatched_rule_records=mismatched_rules,
    )


def _minimum(blockers: list[str], name: str, actual: int, required: int) -> None:
    if actual < required:
        blockers.append(f"{name} {actual} is below required {required}")


def _at_least(
    blockers: list[str], name: str, actual: float | None, required: float
) -> None:
    if actual is None or actual < required:
        blockers.append(f"{name} is below required {required:g}")


def _strictly_above(
    blockers: list[str], name: str, actual: float | None, threshold: float
) -> None:
    if actual is None or actual <= threshold:
        blockers.append(f"{name} is not above {threshold:g}")


def assess_maturity(
    records: Sequence[Mapping[str, Any]], config: StrategyConfig | None = None
) -> MaturityAssessment:
    config = config or load_config()
    metrics = calculate_metrics(records, config)
    provisional_rule = config.raw["promotion"]["provisional"]
    validated_rule = config.raw["promotion"]["validated"]
    common: list[str] = []
    if metrics.incomplete_capture_records:
        common.append("one or more closed signals lack complete-universe capture")
    if metrics.mismatched_rule_records:
        common.append("one or more records use a different rules hash")
    if metrics.incomplete_execution_records:
        common.append("one or more live execution records are incomplete")

    provisional = list(common)
    _minimum(
        provisional,
        "closed signals",
        metrics.closed_signals,
        int(provisional_rule["minimum_closed_signals"]),
    )
    _minimum(
        provisional,
        "live signals",
        metrics.live_signals,
        int(provisional_rule["minimum_live_signals"]),
    )
    _strictly_above(
        provisional,
        "expectancy R",
        metrics.expectancy_r,
        float(provisional_rule["minimum_expectancy_r"]),
    )
    _at_least(
        provisional,
        "profit factor",
        metrics.profit_factor,
        float(provisional_rule["minimum_profit_factor"]),
    )
    if metrics.maximum_drawdown_r > float(provisional_rule["maximum_drawdown_r"]):
        provisional.append("maximum drawdown exceeds the provisional limit")
    if metrics.rule_violations > int(provisional_rule["maximum_rule_violations"]):
        provisional.append("rule violations exceed the provisional limit")

    validated = list(common)
    _minimum(
        validated,
        "closed signals",
        metrics.closed_signals,
        int(validated_rule["minimum_closed_signals"]),
    )
    _minimum(
        validated,
        "confirmation signals",
        metrics.confirmation_signals,
        int(validated_rule["minimum_confirmation_signals"]),
    )
    _minimum(
        validated,
        "live signals",
        metrics.live_signals,
        int(validated_rule["minimum_live_signals"]),
    )
    _minimum(
        validated,
        "stop execution signals",
        metrics.stop_execution_signals,
        int(validated_rule["minimum_stop_execution_signals"]),
    )
    _strictly_above(
        validated,
        "expectancy R",
        metrics.expectancy_r,
        float(validated_rule["minimum_expectancy_r"]),
    )
    _strictly_above(
        validated,
        "confirmation expectancy R",
        metrics.confirmation_expectancy_r,
        float(validated_rule["minimum_confirmation_expectancy_r"]),
    )
    _at_least(
        validated,
        "profit factor",
        metrics.profit_factor,
        float(validated_rule["minimum_profit_factor"]),
    )
    _strictly_above(
        validated,
        "bootstrap lower expectancy R",
        metrics.bootstrap_lower_expectancy_r,
        0.0,
    )
    if metrics.maximum_drawdown_r > float(validated_rule["maximum_drawdown_r"]):
        validated.append("maximum drawdown exceeds the validated limit")
    if metrics.rule_violations > int(validated_rule["maximum_rule_violations"]):
        validated.append("rule violations exceed the validated limit")
    if metrics.entry_slippage_p95_bps is None or metrics.entry_slippage_p95_bps > float(
        validated_rule["maximum_entry_slippage_p95_bps"]
    ):
        validated.append("entry slippage p95 is missing or above budget")
    if (
        metrics.unprotected_p95_seconds is None
        or metrics.unprotected_p95_seconds
        > float(validated_rule["maximum_unprotected_p95_seconds"])
    ):
        validated.append("unprotected exposure p95 is missing or above budget")
    if (
        metrics.stop_slippage_excess_p95_bps is None
        or metrics.stop_slippage_excess_p95_bps > 0
    ):
        validated.append("stop slippage p95 is missing or exceeds its planned reserve")

    earned = (
        "VALIDATED"
        if not validated
        else "PROVISIONAL"
        if not provisional
        else "UNVALIDATED"
    )
    return MaturityAssessment(
        strategy_version=config.version,
        rules_hash=config.rules_hash,
        earned_maturity=earned,
        metrics=metrics,
        provisional_blockers=tuple(provisional),
        validated_blockers=tuple(validated),
    )

"""Selection-aware deterministic statistics for Strategy Lab."""

from __future__ import annotations

import math
import random
from itertools import combinations
from dataclasses import dataclass
from statistics import NormalDist, fmean, pstdev
from typing import Any, Sequence


NORMAL = NormalDist()


def _finite(values: Sequence[float]) -> list[float]:
    return [float(value) for value in values if math.isfinite(float(value))]


def profit_factor(values: Sequence[float]) -> float:
    gains = sum(value for value in values if value > 0)
    losses = -sum(value for value in values if value < 0)
    if losses == 0:
        return 999.0 if gains > 0 else 0.0
    return gains / losses


def one_sided_wilson_lower(wins: int, total: int, confidence: float = 0.90) -> float:
    if total <= 0:
        return 0.0
    z = NORMAL.inv_cdf(confidence)
    proportion = wins / total
    denominator = 1 + z * z / total
    center = proportion + z * z / (2 * total)
    margin = z * math.sqrt(
        proportion * (1 - proportion) / total + z * z / (4 * total * total)
    )
    return max(0.0, (center - margin) / denominator)


def bootstrap_lower_mean(
    values: Sequence[float],
    *,
    confidence: float,
    samples: int,
    seed: int,
) -> float:
    clean = _finite(values)
    if not clean:
        return 0.0
    if len(clean) == 1:
        return clean[0]
    generator = random.Random(seed)
    means = sorted(
        fmean(clean[generator.randrange(len(clean))] for _ in clean)
        for _ in range(samples)
    )
    index = max(0, min(len(means) - 1, math.floor((1 - confidence) * len(means))))
    return means[index]


def maximum_drawdown_r(values: Sequence[float]) -> float:
    equity = peak = 0.0
    maximum = 0.0
    for value in values:
        equity += float(value)
        peak = max(peak, equity)
        maximum = max(maximum, peak - equity)
    return maximum


def one_sided_mean_pvalue(values: Sequence[float]) -> float:
    clean = _finite(values)
    if len(clean) < 2:
        return 1.0
    standard = pstdev(clean)
    if standard == 0:
        return 0.0 if fmean(clean) > 0 else 1.0
    z = fmean(clean) / (standard / math.sqrt(len(clean)))
    return 1 - NORMAL.cdf(z)


def _moments(values: Sequence[float]) -> tuple[float, float]:
    clean = _finite(values)
    if len(clean) < 3:
        return 0.0, 3.0
    mean = fmean(clean)
    standard = pstdev(clean)
    if standard == 0:
        return 0.0, 3.0
    skew = fmean(((value - mean) / standard) ** 3 for value in clean)
    kurtosis = fmean(((value - mean) / standard) ** 4 for value in clean)
    return skew, kurtosis


def deflated_sharpe_probability(values: Sequence[float], trial_count: int) -> float:
    clean = _finite(values)
    if len(clean) < 3 or trial_count < 1:
        return 0.0
    standard = pstdev(clean)
    if standard == 0:
        return 1.0 if fmean(clean) > 0 else 0.0
    sharpe = fmean(clean) / standard
    skew, kurtosis = _moments(clean)
    trials = max(2, int(trial_count))
    gamma = 0.5772156649015329
    first = NORMAL.inv_cdf(1 - 1 / trials)
    second = NORMAL.inv_cdf(1 - 1 / (trials * math.e))
    expected_max = (1 - gamma) * first + gamma * second
    denominator = math.sqrt(
        max(
            1e-12,
            1 - skew * sharpe + ((kurtosis - 1) / 4) * sharpe * sharpe,
        )
    )
    z = (sharpe - expected_max) * math.sqrt(len(clean) - 1) / denominator
    return min(1.0, max(0.0, NORMAL.cdf(z)))


def positive_without_best_five(values: Sequence[float]) -> bool:
    clean = sorted(_finite(values), reverse=True)
    if len(clean) <= 5:
        return False
    return sum(clean[5:]) > 0


def positive_halves(values: Sequence[float]) -> bool:
    clean = _finite(values)
    midpoint = len(clean) // 2
    return midpoint > 0 and sum(clean[:midpoint]) > 0 and sum(clean[midpoint:]) > 0


@dataclass(frozen=True)
class FamilyAdjustment:
    pbo: float
    holm_pass: dict[str, bool]
    neighbor_stability: dict[str, bool]


def family_adjustment(
    rows: Sequence[dict[str, Any]],
    *,
    alpha: float = 0.10,
    minimum_positive_neighbor_fraction: float = 0.50,
) -> FamilyAdjustment:
    if not rows:
        return FamilyAdjustment(1.0, {}, {})
    ordered = sorted(
        ((str(row["strategy_id"]), float(row["pvalue"])) for row in rows),
        key=lambda item: item[1],
    )
    holm: dict[str, bool] = {strategy_id: False for strategy_id, _ in ordered}
    still_passing = True
    total = len(ordered)
    for index, (strategy_id, pvalue) in enumerate(ordered):
        threshold = alpha / (total - index)
        still_passing = still_passing and pvalue <= threshold
        holm[strategy_id] = still_passing

    fold_vectors = [
        (
            str(row["strategy_id"]),
            [float(value) for value in row.get("fold_log_growth", [])],
        )
        for row in rows
    ]
    folds = min((len(vector) for _, vector in fold_vectors), default=0)
    selections = failures = 0
    if folds >= 4 and len(fold_vectors) >= 2:
        split = max(1, folds // 2)
        train_combinations = list(combinations(range(folds), split))
        for train_indices in train_combinations:
            test_indices = [
                index for index in range(folds) if index not in train_indices
            ]
            if not test_indices:
                continue
            train_scores = {
                strategy_id: sum(vector[index] for index in train_indices)
                for strategy_id, vector in fold_vectors
            }
            winner = max(
                train_scores,
                key=lambda strategy_id: (train_scores[strategy_id], strategy_id),
            )
            test_scores = sorted(
                (
                    sum(vector[index] for index in test_indices),
                    strategy_id,
                )
                for strategy_id, vector in fold_vectors
            )
            winner_rank = next(
                index
                for index, (_, strategy_id) in enumerate(test_scores)
                if strategy_id == winner
            )
            failures += winner_rank < len(test_scores) / 2
            selections += 1
    pbo = failures / selections if selections else 1.0

    parameter_keys = sorted(
        {key for row in rows for key in dict(row.get("parameters") or {})}
    )
    ranges: dict[str, tuple[float, float]] = {}
    for key in parameter_keys:
        values = [
            float(row["parameters"][key])
            for row in rows
            if key in dict(row.get("parameters") or {})
        ]
        ranges[key] = (min(values), max(values)) if values else (0.0, 0.0)

    def distance(left: dict[str, Any], right: dict[str, Any]) -> float:
        left_parameters = dict(left.get("parameters") or {})
        right_parameters = dict(right.get("parameters") or {})
        if set(left_parameters) != set(right_parameters) or not left_parameters:
            return math.inf
        total_distance = 0.0
        for key in left_parameters:
            low, high = ranges[key]
            scale = high - low
            difference = abs(float(left_parameters[key]) - float(right_parameters[key]))
            total_distance += difference / scale if scale > 0 else difference
        return total_distance

    neighbors: dict[str, bool] = {}
    for row in rows:
        strategy_id = str(row["strategy_id"])
        nearest = sorted(
            (
                distance(row, candidate),
                str(candidate["strategy_id"]),
                float(candidate["stressed_log_growth"]),
            )
            for candidate in rows
            if str(candidate["strategy_id"]) != strategy_id
        )
        usable = [candidate for candidate in nearest if math.isfinite(candidate[0])][:4]
        positive_fraction = (
            sum(candidate[2] > 0 for candidate in usable) / len(usable)
            if usable
            else 0.0
        )
        neighbors[strategy_id] = (
            float(row["stressed_log_growth"]) > 0
            and positive_fraction >= minimum_positive_neighbor_fraction
        )
    return FamilyAdjustment(pbo=pbo, holm_pass=holm, neighbor_stability=neighbors)

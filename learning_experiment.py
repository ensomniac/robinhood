"""Preregister bounded strategy hypotheses and enforce complete experiment families."""

from __future__ import annotations

import argparse
import hashlib
import itertools
import json
import math
import statistics
from collections.abc import Mapping, Sequence
from datetime import date, datetime
from pathlib import Path
from statistics import NormalDist
from typing import Any

from learning_data import DATASET_LANES, load_frozen_dataset_contract
from learning_registry import REGISTRY_ROOT, current_entities
from learning_statistics import (
    annualized_sharpe,
    deflated_sharpe_probability,
    holm_family_decisions,
    maximum_drawdown_fraction,
    power_sample_target,
    probability_of_backtest_overfitting,
    profit_factor,
    stationary_bootstrap_summary,
)


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_HYPOTHESIS_ROOT = REGISTRY_ROOT / "hypotheses"
DEFAULT_RESEARCH_LOCK = REGISTRY_ROOT / "RESEARCH_LOCK.json"
SCHEMA_VERSION = 1
MAX_NEW_HYPOTHESES_PER_ISO_WEEK = 3
MAX_TRIALS_PER_FAMILY = 64
SELECTION_MODES = {"preselected_primary", "development_search"}
DEVELOPMENT_SEARCH_RULE = {
    "minimum_deflated_sharpe_probability": 0.90,
    "holm_alpha": 0.10,
    "maximum_pbo_probability": 0.50,
    "minimum_neighbor_positive_fraction": 0.50,
    "ranking": [
        "highest_20bps_bootstrap_lower_mean_account_return",
        "highest_20bps_total_log_growth",
        "lowest_20bps_maximum_drawdown",
        "canonical_trial_id",
    ],
}
EXPERIMENT_TRANSITIONS = {
    "INVENTED": {"PREREGISTERED", "REJECTED"},
    "PREREGISTERED": {"DATA_READY", "REJECTED"},
    "DATA_READY": {"EVALUATED", "REJECTED"},
    "EVALUATED": {"ADVERSARIALLY_REVIEWED", "REJECTED"},
    "ADVERSARIALLY_REVIEWED": {
        "REJECTED",
        "CONFIRMATION_QUEUED",
        "SHADOW_QUEUED",
    },
    "CONFIRMATION_QUEUED": {"CLOSED"},
    "SHADOW_QUEUED": {"CLOSED"},
    "REJECTED": {"CLOSED"},
    "FAILED": {"CLOSED"},
    "CLOSED": set(),
    "RETIRED": set(),
}
REQUIRED_TEXT_FIELDS = (
    "experiment_id",
    "family_id",
    "strategy_id",
    "mechanism",
    "expected_holding_behavior",
    "entry_rule",
    "stop_rule",
    "exit_rule",
    "ranking_rule",
    "selection_rule",
    "primary_outcome",
    "material_difference_rationale",
)


class LearningExperimentError(ValueError):
    """Raised when a hypothesis or experiment family is incomplete or mutable."""


def _fingerprint(value: Any) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()


def _timestamp(value: Any, field: str) -> datetime:
    if not isinstance(value, str):
        raise LearningExperimentError(f"{field} must be an ISO timestamp")
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise LearningExperimentError(f"{field} must be an ISO timestamp") from exc
    if result.tzinfo is None:
        raise LearningExperimentError(f"{field} must include a timezone")
    return result


def _text(value: Any, field: str, *, minimum: int = 1) -> str:
    if not isinstance(value, str) or len(value.strip()) < minimum:
        raise LearningExperimentError(
            f"{field} must contain at least {minimum} characters"
        )
    return value.strip()


def _text_list(value: Any, field: str) -> list[str]:
    if not isinstance(value, list) or not value:
        raise LearningExperimentError(f"{field} must be a non-empty array")
    return [_text(item, f"{field} item") for item in value]


def enumerate_trials(parameter_grid: Mapping[str, Any]) -> list[dict[str, Any]]:
    if not isinstance(parameter_grid, Mapping) or not parameter_grid:
        raise LearningExperimentError("parameter_grid must be a non-empty object")
    names = sorted(parameter_grid)
    values: list[list[Any]] = []
    for name in names:
        options = parameter_grid[name]
        if not isinstance(options, list) or not options:
            raise LearningExperimentError(f"parameter_grid.{name} must be non-empty")
        canonical = {json.dumps(item, sort_keys=True) for item in options}
        if len(canonical) != len(options):
            raise LearningExperimentError(f"parameter_grid.{name} contains duplicates")
        values.append(options)
    count = math.prod(len(options) for options in values)
    if count > MAX_TRIALS_PER_FAMILY:
        raise LearningExperimentError(
            f"parameter family has {count} trials; maximum is {MAX_TRIALS_PER_FAMILY}"
        )
    trials: list[dict[str, Any]] = []
    for combination in itertools.product(*values):
        parameters = dict(zip(names, combination, strict=True))
        trials.append(
            {
                "trial_id": f"trial-{_fingerprint(parameters)[:16]}",
                "parameters": parameters,
            }
        )
    return trials


def parameter_neighbors(
    trial: Mapping[str, Any],
    parameter_grid: Mapping[str, Any],
    trials: Sequence[Mapping[str, Any]],
) -> list[str]:
    parameters = trial.get("parameters")
    if not isinstance(parameters, Mapping):
        raise LearningExperimentError("trial parameters are missing")
    by_parameters = {
        json.dumps(item["parameters"], sort_keys=True, separators=(",", ":")): str(
            item["trial_id"]
        )
        for item in trials
    }
    neighbors: list[str] = []
    for name in sorted(parameter_grid):
        options = parameter_grid[name]
        canonical = [json.dumps(item, sort_keys=True) for item in options]
        current = json.dumps(parameters[name], sort_keys=True)
        index = canonical.index(current)
        for neighbor_index in (index - 1, index + 1):
            if not 0 <= neighbor_index < len(options):
                continue
            candidate = dict(parameters)
            candidate[name] = options[neighbor_index]
            key = json.dumps(candidate, sort_keys=True, separators=(",", ":"))
            neighbors.append(by_parameters[key])
    return sorted(neighbors)


def build_rolling_origin_plan(
    requested_dates: Sequence[str],
    *,
    minimum_train_days: int = 40,
    embargo_days: int = 1,
    maximum_hold_sessions: int = 5,
) -> list[dict[str, Any]]:
    if embargo_days < 1:
        raise LearningExperimentError(
            "rolling-origin plan needs at least one embargo day"
        )
    if (
        isinstance(maximum_hold_sessions, bool)
        or not isinstance(maximum_hold_sessions, int)
        or maximum_hold_sessions < 1
    ):
        raise LearningExperimentError(
            "rolling-origin maximum hold must be a positive integer"
        )
    try:
        parsed = [date.fromisoformat(item) for item in requested_dates]
    except (TypeError, ValueError) as exc:
        raise LearningExperimentError("rolling-origin dates must be ISO dates") from exc
    if len(parsed) != len(set(parsed)) or parsed != sorted(parsed):
        raise LearningExperimentError(
            "rolling-origin dates must be unique and chronological"
        )
    if len(parsed) < minimum_train_days + embargo_days + 10:
        raise LearningExperimentError(
            "insufficient dates for rolling-origin validation"
        )
    test_size = max(10, len(parsed) // 5)
    folds: list[dict[str, Any]] = []
    train_end = minimum_train_days
    while train_end + embargo_days < len(parsed):
        test_start = train_end + embargo_days
        test_end = min(len(parsed), test_start + test_size)
        if test_end - test_start < 5:
            break
        test_dates = [item.isoformat() for item in parsed[test_start:test_end]]
        if len(test_dates) < maximum_hold_sessions:
            break
        entry_count = len(test_dates) - maximum_hold_sessions + 1
        folds.append(
            {
                "fold": len(folds) + 1,
                "train_dates": [item.isoformat() for item in parsed[:train_end]],
                "embargo_dates": [
                    item.isoformat() for item in parsed[train_end:test_start]
                ],
                "test_dates": test_dates,
                "entry_dates": test_dates[:entry_count],
                "settlement_only_dates": test_dates[entry_count:],
            }
        )
        train_end = test_end
    if not folds:
        raise LearningExperimentError(
            "rolling-origin plan produced no validation folds"
        )
    return folds


def validate_hypothesis_contract(value: Any) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise LearningExperimentError("hypothesis contract must be an object")
    contract = dict(value)
    if contract.get("schema_version") != SCHEMA_VERSION:
        raise LearningExperimentError(f"schema_version must be {SCHEMA_VERSION}")
    for field in REQUIRED_TEXT_FIELDS:
        _text(contract.get(field), field)
    _timestamp(contract.get("created_at"), "created_at")
    if contract.get("status") != "INVENTED":
        raise LearningExperimentError("a new hypothesis contract must start INVENTED")
    if contract.get("dataset_lane") not in DATASET_LANES:
        raise LearningExperimentError("dataset_lane is unsupported")
    for field in (
        "universe_requirements",
        "execution_assumptions",
        "falsification_criteria",
        "minimum_evidence",
    ):
        if not isinstance(contract.get(field), Mapping) or not contract[field]:
            raise LearningExperimentError(f"{field} must be a non-empty object")
    _text_list(contract.get("contamination_risks"), "contamination_risks")
    _text_list(
        contract.get("production_compatibility_risks"),
        "production_compatibility_risks",
    )
    if len(contract["material_difference_rationale"].strip()) < 20:
        raise LearningExperimentError(
            "material_difference_rationale must explain the distinct mechanism"
        )
    trials = enumerate_trials(contract.get("parameter_grid", {}))
    selection_mode = contract.get("selection_mode", "preselected_primary")
    if selection_mode not in SELECTION_MODES:
        raise LearningExperimentError("selection_mode is unsupported")
    contract["selection_mode"] = selection_mode
    matching: list[dict[str, Any]] = []
    if selection_mode == "preselected_primary":
        primary = contract.get("primary_parameters")
        if not isinstance(primary, Mapping):
            raise LearningExperimentError("primary_parameters must be an object")
        matching = [trial for trial in trials if trial["parameters"] == dict(primary)]
        if len(matching) != 1:
            raise LearningExperimentError(
                "primary_parameters must select exactly one registered grid trial"
            )
    else:
        if "primary_parameters" in contract:
            raise LearningExperimentError(
                "development_search cannot preselect primary_parameters"
            )
        if contract.get("winner_selection") != DEVELOPMENT_SEARCH_RULE:
            raise LearningExperimentError(
                "development_search winner_selection must match the frozen rule"
            )
    parent = contract.get("parent_experiment_id")
    if parent is not None and (
        not isinstance(parent, str) or parent == contract["experiment_id"]
    ):
        raise LearningExperimentError("parent_experiment_id is invalid")
    contract["trial_family"] = trials
    contract["primary_trial_id"] = (
        matching[0]["trial_id"] if matching else None
    )
    return contract


def freeze_hypothesis_contract(
    value: Mapping[str, Any],
    output_root: Path = DEFAULT_HYPOTHESIS_ROOT,
    *,
    research_lock_path: Path = DEFAULT_RESEARCH_LOCK,
    registry_root: Path = REGISTRY_ROOT,
) -> tuple[Path, dict[str, Any]]:
    contract = validate_hypothesis_contract(value)
    enforce_research_lock(
        contract, lock_path=research_lock_path, registry_root=registry_root
    )
    content = dict(contract)
    content.pop("contract_sha256", None)
    fingerprint = _fingerprint(content)
    frozen = {**content, "contract_sha256": fingerprint}
    output_root.mkdir(parents=True, exist_ok=True)
    path = output_root / f"{content['experiment_id']}-{fingerprint}.json"
    rendered = json.dumps(frozen, indent=2, sort_keys=True) + "\n"
    if path.exists():
        if path.read_text(encoding="utf-8") != rendered:
            raise LearningExperimentError(
                "hash-addressed hypothesis contract has other content"
            )
    else:
        temporary = path.with_suffix(".json.tmp")
        temporary.write_text(rendered, encoding="utf-8")
        temporary.replace(path)
    return path, frozen


def load_hypothesis_contract(path: Path) -> dict[str, Any]:
    contract = _read_object(path)
    recorded = contract.get("contract_sha256")
    content = dict(contract)
    content.pop("contract_sha256", None)
    expected = _fingerprint(content)
    if (
        recorded != expected
        or path.name != f"{content.get('experiment_id')}-{expected}.json"
    ):
        raise LearningExperimentError("hypothesis contract was mutated or renamed")
    normalized = validate_hypothesis_contract(content)
    return {**normalized, "contract_sha256": recorded}


def weekly_hypothesis_count(
    created_at: datetime, *, registry_root: Path = REGISTRY_ROOT
) -> int:
    target = created_at.isocalendar()[:2]
    count = 0
    for event in current_entities("experiments", registry_root).values():
        timestamp = event["payload"].get("created_at")
        if not isinstance(timestamp, str):
            continue
        if _timestamp(timestamp, "experiment created_at").isocalendar()[:2] == target:
            count += 1
    return count


def enforce_weekly_hypothesis_budget(
    contract: Mapping[str, Any], *, registry_root: Path = REGISTRY_ROOT
) -> None:
    created_at = _timestamp(contract.get("created_at"), "created_at")
    if (
        weekly_hypothesis_count(created_at, registry_root=registry_root)
        >= MAX_NEW_HYPOTHESES_PER_ISO_WEEK
    ):
        raise LearningExperimentError(
            f"weekly hypothesis budget of {MAX_NEW_HYPOTHESES_PER_ISO_WEEK} is exhausted"
        )


def research_lock_status(
    *,
    lock_path: Path = DEFAULT_RESEARCH_LOCK,
    registry_root: Path = REGISTRY_ROOT,
) -> dict[str, Any]:
    if not lock_path.exists():
        return {"active": False, "blocks_new_hypotheses": False}
    lock = _read_object(lock_path)
    if lock.get("schema_version") != 1 or not isinstance(lock.get("active"), bool):
        raise LearningExperimentError("research lock is malformed")
    if not lock["active"]:
        return {**lock, "blocks_new_hypotheses": False}
    blocked_lane = _text(lock.get("blocked_dataset_lane"), "blocked_dataset_lane")
    if blocked_lane not in DATASET_LANES:
        raise LearningExperimentError(
            "research lock blocked_dataset_lane is unsupported"
        )
    required_id = _text(lock.get("required_dataset_id"), "required_dataset_id")
    required_status = _text(
        lock.get("required_dataset_status"), "required_dataset_status"
    )
    required_lane = _text(lock.get("required_dataset_lane"), "required_dataset_lane")
    required_evidence = _text(lock.get("evidence_path"), "evidence_path")
    evidence_path = Path(required_evidence)
    if evidence_path.is_absolute() or ".." in evidence_path.parts:
        raise LearningExperimentError("research lock evidence_path is unsafe")
    if lock.get("required_dataset_inspected") is not True:
        raise LearningExperimentError(
            "active research lock must require an inspected dataset"
        )
    dataset = current_entities("datasets", registry_root).get(required_id)
    payload = dataset["payload"] if dataset else {}
    checks = {
        "registered": dataset is not None,
        "status": payload.get("status") == required_status,
        "lane": payload.get("lane") == required_lane,
        "inspected": payload.get("inspected") is True,
        "evidence": required_evidence in (payload.get("evidence_paths") or []),
    }
    satisfied = all(checks.values())
    return {
        **lock,
        "satisfied": satisfied,
        "unsatisfied_conditions": [key for key, passed in checks.items() if not passed],
        "blocks_new_hypotheses": not satisfied,
    }


def enforce_research_lock(
    contract: Mapping[str, Any],
    *,
    lock_path: Path = DEFAULT_RESEARCH_LOCK,
    registry_root: Path = REGISTRY_ROOT,
) -> None:
    lock = research_lock_status(lock_path=lock_path, registry_root=registry_root)
    if lock.get("blocks_new_hypotheses") and contract.get("dataset_lane") == lock.get(
        "blocked_dataset_lane"
    ):
        raise LearningExperimentError(str(lock.get("reason") or "research is locked"))


def validate_transition(previous: str, current: str) -> None:
    allowed = EXPERIMENT_TRANSITIONS.get(previous)
    if allowed is None or current not in allowed:
        raise LearningExperimentError(
            f"experiment transition {previous} -> {current} is not allowed"
        )


def validate_complete_evaluation(
    contract: Mapping[str, Any], result: Mapping[str, Any]
) -> dict[str, Any]:
    frozen = validate_hypothesis_contract(
        {
            key: value
            for key, value in contract.items()
            if key not in {"contract_sha256", "trial_family", "primary_trial_id"}
        }
    )
    if result.get("experiment_id") != frozen["experiment_id"]:
        raise LearningExperimentError("evaluation experiment identity mismatch")
    dataset_manifest = result.get("dataset_manifest")
    if not isinstance(dataset_manifest, str):
        raise LearningExperimentError("evaluation needs a frozen dataset_manifest")
    dataset = load_frozen_dataset_contract(Path(dataset_manifest))
    if dataset["dataset_payload"]["lane"] != frozen["dataset_lane"]:
        raise LearningExperimentError(
            "evaluation dataset lane does not match the frozen hypothesis"
        )
    trials = result.get("trials")
    if not isinstance(trials, list):
        raise LearningExperimentError("evaluation trials must be an array")
    expected = {item["trial_id"] for item in frozen["trial_family"]}
    actual = {
        item.get("trial_id")
        for item in trials
        if isinstance(item, Mapping) and isinstance(item.get("trial_id"), str)
    }
    if actual != expected or len(trials) != len(expected):
        missing = sorted(expected - actual)
        extra = sorted(actual - expected)
        raise LearningExperimentError(
            f"evaluation must disclose the complete family; missing={missing}, extra={extra}"
        )
    for item in trials:
        metrics = item.get("metrics")
        if not isinstance(metrics, Mapping):
            raise LearningExperimentError("every trial needs metrics")
        numeric_fields = (
            (
                "total_log_growth",
                "bootstrap_lower_mean",
                "profit_factor",
                "maximum_drawdown",
                "deflated_sharpe_probability",
                "pbo_probability",
            )
            if frozen["selection_mode"] == "preselected_primary"
            else (
                "stress_20bps_total_log_growth",
                "stress_20bps_bootstrap_lower_mean_account_return",
                "stress_20bps_profit_factor",
                "stress_20bps_maximum_drawdown_r",
                "deflated_sharpe_probability",
                "pbo_probability",
            )
        )
        for field in numeric_fields:
            value = metrics.get(field)
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(value)
            ):
                raise LearningExperimentError(f"trial metric {field} must be finite")
        boolean_fields = ["holm_reject_null", "rolling_folds_positive"]
        if frozen["selection_mode"] == "development_search":
            boolean_fields.extend(("rules_complete", "trial_accounting_complete"))
            filled_returns = metrics.get("oof_filled_account_returns")
            daily_returns = metrics.get("oof_daily_account_returns")
            if not isinstance(filled_returns, list):
                raise LearningExperimentError(
                    "development-search trial needs oof_filled_account_returns"
                )
            if not isinstance(daily_returns, list) or not daily_returns:
                raise LearningExperimentError(
                    "development-search trial needs oof_daily_account_returns"
                )
            for value in [*filled_returns, *daily_returns]:
                if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
                    raise LearningExperimentError(
                        "development account returns must be finite numbers"
                    )
            accounting = item.get("trial_accounting")
            if not isinstance(accounting, list) or not accounting:
                raise LearningExperimentError(
                    "development-search trial needs complete trial_accounting"
                )
            if len(accounting) != len(daily_returns):
                raise LearningExperimentError(
                    "trial accounting must align with the complete daily account path"
                )
            for index, (row, daily_return) in enumerate(
                zip(accounting, daily_returns, strict=True)
            ):
                if not isinstance(row, Mapping):
                    raise LearningExperimentError(
                        "trial accounting rows must be objects"
                    )
                if daily_return == 0 and row.get("outcome") != "zero_return_day":
                    raise LearningExperimentError(
                        f"trial accounting row {index} must retain its zero-return day"
                    )
                if daily_return != 0 and row.get("outcome") == "zero_return_day":
                    raise LearningExperimentError(
                        f"trial accounting row {index} mislabels a nonzero account day"
                    )
        for field in boolean_fields:
            if not isinstance(metrics.get(field), bool):
                raise LearningExperimentError(f"trial metric {field} must be boolean")
    implementation = result.get("implementation_sha256")
    if not isinstance(implementation, str) or len(implementation) != 64:
        raise LearningExperimentError("evaluation needs implementation_sha256")
    return dict(result)


def _rebuild_development_statistics(
    trials: Sequence[Mapping[str, Any]],
    *,
    development_dates: Sequence[str] | None = None,
    rolling_origin_plan: Sequence[Mapping[str, Any]] | None = None,
    prior_trial_sharpes: Sequence[float] = (),
    prior_trial_p_values: Sequence[float] = (),
    prior_pbo_probability: float = 0.0,
) -> dict[str, dict[str, Any]]:
    daily_returns_by_id = {
        str(item["trial_id"]): [
            float(value) for value in item["metrics"]["oof_daily_account_returns"]
        ]
        for item in trials
    }
    filled_returns_by_id = {
        str(item["trial_id"]): [
            float(value) for value in item["metrics"]["oof_filled_account_returns"]
        ]
        for item in trials
    }
    lengths = {len(values) for values in daily_returns_by_id.values()}
    if len(lengths) != 1:
        raise LearningExperimentError(
            "development trials must share the complete OOF calendar"
        )
    fold_indices: list[list[int]] | None = None
    if development_dates is not None or rolling_origin_plan is not None:
        if development_dates is None or rolling_origin_plan is None:
            raise LearningExperimentError(
                "rolling-origin dates and plan must be supplied together"
            )
        dates = list(development_dates)
        expected_plan = build_rolling_origin_plan(dates)
        account_dates = [
            day for fold in expected_plan for day in fold["test_dates"]
        ]
        if len(account_dates) not in lengths:
            raise LearningExperimentError(
                "rolling-origin test dates must align with every trial account path"
            )
        if [dict(item) for item in rolling_origin_plan] != expected_plan:
            raise LearningExperimentError("rolling-origin plan drifted")
        positions = {day: index for index, day in enumerate(account_dates)}
        fold_indices = [
            [positions[day] for day in fold["test_dates"]]
            for fold in expected_plan
        ]
    sharpes = {
        trial_id: annualized_sharpe(values) or 0.0
        for trial_id, values in daily_returns_by_id.items()
    }
    p_values: dict[str, float] = {}
    for trial_id, values in daily_returns_by_id.items():
        mean = statistics.fmean(values)
        deviation = statistics.stdev(values) if len(values) > 1 else 0.0
        statistic = mean / (deviation / math.sqrt(len(values))) if deviation else 0.0
        p_values[trial_id] = 1 - NormalDist().cdf(statistic)
    if (
        len(prior_trial_sharpes) != len(prior_trial_p_values)
        or len(trials) + len(prior_trial_sharpes) > MAX_TRIALS_PER_FAMILY
        or any(
            not math.isfinite(float(value))
            for value in prior_trial_sharpes
        )
        or any(
            not 0 <= float(value) <= 1
            for value in prior_trial_p_values
        )
        or not 0 <= float(prior_pbo_probability) <= 1
    ):
        raise LearningExperimentError(
            "prior selection-trial statistics are invalid"
        )
    combined_p_values = {
        **p_values,
        **{
            f"prior-trial-{index:03d}": float(value)
            for index, value in enumerate(prior_trial_p_values, 1)
        },
    }
    holm = holm_family_decisions(combined_p_values, alpha=0.10)
    pbo = probability_of_backtest_overfitting(daily_returns_by_id)
    trial_sharpes = [
        *sharpes.values(),
        *map(float, prior_trial_sharpes),
    ]
    rebuilt: dict[str, dict[str, Any]] = {}
    for item in trials:
        trial_id = str(item["trial_id"])
        daily_returns = daily_returns_by_id[trial_id]
        filled_returns = filled_returns_by_id[trial_id]
        dollars_raw = item["metrics"].get("oof_net_pnl_dollars")
        dollars = (
            [float(value) for value in dollars_raw]
            if isinstance(dollars_raw, list)
            else [value * 100_000 for value in filled_returns]
        )
        if len(dollars) != len(filled_returns):
            raise LearningExperimentError(
                "oof_net_pnl_dollars must align with filled account returns"
            )
        if fold_indices is None:
            fold_size = max(1, len(daily_returns) // 5)
            folds = [
                daily_returns[start : min(len(daily_returns), start + fold_size)]
                for start in range(0, len(daily_returns), fold_size)
            ]
        else:
            folds = [
                [daily_returns[index] for index in indices]
                for indices in fold_indices
            ]
        bootstrap = (
            stationary_bootstrap_summary(
                filled_returns,
                confidence=0.90,
                samples=2_000,
            )
            if filled_returns
            else {
                "confidence": 0.90,
                "samples": 0,
                "mean": 0.0,
                "lower_one_sided": -1.0,
                "average_block_length": None,
                "seed": None,
                "method": "no-filled-trades",
            }
        )
        risk_fraction = float(item["metrics"].get("risk_fraction", 0.005))
        if risk_fraction <= 0:
            raise LearningExperimentError("risk_fraction must be positive")
        stressed_profit_factor = profit_factor(dollars)
        stressed_profit_factor_is_infinite = stressed_profit_factor == math.inf
        rebuilt[trial_id] = {
            "stress_20bps_total_log_growth": sum(
                math.log1p(value) for value in daily_returns
            ),
            "stress_20bps_bootstrap_lower_mean_account_return": bootstrap[
                "lower_one_sided"
            ],
            "stress_20bps_profit_factor": (
                None
                if stressed_profit_factor_is_infinite
                else stressed_profit_factor
            ),
            "stress_20bps_profit_factor_is_infinite": (
                stressed_profit_factor_is_infinite
            ),
            "stress_20bps_maximum_drawdown_r": maximum_drawdown_fraction(daily_returns)
            / risk_fraction,
            "deflated_sharpe_probability": deflated_sharpe_probability(
                daily_returns, trial_sharpes
            )["probability"],
            "pbo_probability": max(
                float(pbo["probability"]),
                float(prior_pbo_probability),
            ),
            "holm_reject_null": holm[trial_id]["reject_null"],
            "rolling_folds_positive": all(
                sum(math.log1p(value) for value in fold) > 0 for fold in folds
            ),
            "rolling_origin_fold_log_growth": [
                sum(math.log1p(value) for value in fold) for fold in folds
            ],
            "rules_complete": item["metrics"]["rules_complete"],
            "trial_accounting_complete": item["metrics"][
                "trial_accounting_complete"
            ],
            "oof_daily_account_returns": daily_returns,
            "oof_filled_account_returns": filled_returns,
            "oof_net_pnl_dollars": dollars,
            "risk_fraction": risk_fraction,
            "stationary_bootstrap": bootstrap,
        }
    return rebuilt


def select_development_winner(
    contract: Mapping[str, Any], result: Mapping[str, Any]
) -> dict[str, Any]:
    normalized = validate_complete_evaluation(contract, result)
    frozen = validate_hypothesis_contract(
        {
            key: value
            for key, value in contract.items()
            if key not in {"contract_sha256", "trial_family", "primary_trial_id"}
        }
    )
    if frozen["selection_mode"] != "development_search":
        raise LearningExperimentError(
            "winner selection requires selection_mode=development_search"
        )
    trials_by_id = {str(item["trial_id"]): item for item in normalized["trials"]}
    rolling_origin_plan = frozen.get("rolling_origin_plan")
    development_dates = frozen.get("development_dates")
    if not isinstance(rolling_origin_plan, list) or not isinstance(
        development_dates, list
    ):
        raise LearningExperimentError(
            "development search lacks its frozen rolling-origin plan"
        )
    rebuilt_by_id = _rebuild_development_statistics(
        normalized["trials"],
        development_dates=development_dates,
        rolling_origin_plan=rolling_origin_plan,
        prior_trial_sharpes=contract.get(
            "prior_trial_sharpes", ()
        ),
        prior_trial_p_values=contract.get(
            "prior_trial_p_values", ()
        ),
        prior_pbo_probability=float(
            contract.get("prior_pbo_probability", 0.0)
        ),
    )
    trial_contracts = {
        str(item["trial_id"]): item for item in frozen["trial_family"]
    }
    classifications: list[dict[str, Any]] = []
    survivors: list[dict[str, Any]] = []
    for trial_id in sorted(trials_by_id):
        item = trials_by_id[trial_id]
        metrics = rebuilt_by_id[trial_id]
        neighbors = parameter_neighbors(
            trial_contracts[trial_id],
            frozen["parameter_grid"],
            frozen["trial_family"],
        )
        positive_neighbors = sum(
            rebuilt_by_id[neighbor]["stress_20bps_total_log_growth"] > 0
            for neighbor in neighbors
        )
        neighbor_fraction = positive_neighbors / len(neighbors) if neighbors else 1.0
        gates = {
            "positive_20bps_total_growth": metrics[
                "stress_20bps_total_log_growth"
            ]
            > 0,
            "stressed_profit_factor": metrics[
                "stress_20bps_profit_factor_is_infinite"
            ]
            or (
                metrics["stress_20bps_profit_factor"] is not None
                and metrics["stress_20bps_profit_factor"] >= 1.20
            ),
            "stressed_drawdown": metrics["stress_20bps_maximum_drawdown_r"] <= 6.0,
            "rolling_fold_stability": metrics["rolling_folds_positive"] is True,
            "rule_completeness": metrics["rules_complete"] is True,
            "trial_accounting": metrics["trial_accounting_complete"] is True,
            "deflated_sharpe": metrics["deflated_sharpe_probability"] is not None
            and metrics["deflated_sharpe_probability"] >= 0.90,
            "holm_family": metrics["holm_reject_null"] is True,
            "backtest_overfitting": metrics["pbo_probability"] is not None
            and metrics["pbo_probability"] <= 0.50,
            "neighbor_stability": neighbor_fraction >= 0.50,
        }
        classification = {
            "trial_id": trial_id,
            "neighbors": neighbors,
            "positive_20bps_neighbors": positive_neighbors,
            "neighbor_positive_fraction": neighbor_fraction,
            "gates": gates,
            "rebuilt_metrics": metrics,
            "status": "SURVIVOR" if all(gates.values()) else "REJECTED",
        }
        classifications.append(classification)
        if classification["status"] == "SURVIVOR":
            survivors.append(item)
    survivors.sort(
        key=lambda item: (
            -float(
                rebuilt_by_id[str(item["trial_id"])][
                    "stress_20bps_bootstrap_lower_mean_account_return"
                ]
            ),
            -float(
                rebuilt_by_id[str(item["trial_id"])][
                    "stress_20bps_total_log_growth"
                ]
            ),
            float(
                rebuilt_by_id[str(item["trial_id"])][
                    "stress_20bps_maximum_drawdown_r"
                ]
            ),
            str(item["trial_id"]),
        )
    )
    if not survivors:
        return {
            "status": "REJECTED",
            "reason": "no trial survived the frozen selection-aware gates",
            "selected_trial_id": None,
            "trial_classifications": classifications,
        }
    selected = survivors[0]
    returns = rebuilt_by_id[str(selected["trial_id"])][
        "oof_filled_account_returns"
    ]
    mean = sum(returns) / len(returns)
    deviation = math.sqrt(
        sum((value - mean) ** 2 for value in returns) / (len(returns) - 1)
    ) if len(returns) > 1 else 0.0
    try:
        power_target = power_sample_target(
            mean,
            deviation,
            alpha=0.10,
            power=0.80,
            configured_floor=50,
        )
    except ValueError:
        power_target = 50
    required_total = max(50, power_target)
    return {
        "status": "WINNER_SELECTED",
        "reason": "frozen deterministic ranking selected one surviving trial",
        "selected_trial_id": selected["trial_id"],
        "selected_parameters": trial_contracts[str(selected["trial_id"])][
            "parameters"
        ],
        "power_target": power_target,
        "development_filled_signals": len(returns),
        "required_total_signals": required_total,
        "required_confirmation_signals": max(
            20, math.ceil(required_total * 0.30)
        ),
        "trial_classifications": classifications,
    }


def disposition_from_result(
    contract: Mapping[str, Any], result: Mapping[str, Any]
) -> dict[str, Any]:
    normalized = validate_complete_evaluation(contract, result)
    frozen = validate_hypothesis_contract(
        {
            key: value
            for key, value in contract.items()
            if key not in {"contract_sha256", "trial_family", "primary_trial_id"}
        }
    )
    if frozen["selection_mode"] == "development_search":
        selection = select_development_winner(contract, normalized)
        return {
            **selection,
            "automatic_strategy_application": False,
        }
    primary = next(
        item
        for item in normalized["trials"]
        if item["trial_id"] == frozen["primary_trial_id"]
    )
    metrics = primary["metrics"]
    criteria = frozen["falsification_criteria"]
    gates = {
        "positive_log_growth": metrics["total_log_growth"]
        > float(criteria.get("minimum_total_log_growth", 0)),
        "positive_bootstrap_lower_mean": metrics["bootstrap_lower_mean"]
        > float(criteria.get("minimum_bootstrap_lower_mean", 0)),
        "profit_factor": metrics["profit_factor"]
        >= float(criteria.get("minimum_profit_factor", 1.2)),
        "drawdown": metrics["maximum_drawdown"]
        <= float(criteria.get("maximum_drawdown", 6.0)),
        "deflated_sharpe": metrics["deflated_sharpe_probability"]
        >= float(criteria.get("minimum_deflated_sharpe_probability", 0.90)),
        "backtest_overfitting": metrics["pbo_probability"]
        <= float(criteria.get("maximum_pbo_probability", 0.50)),
        "holm_family": metrics["holm_reject_null"] is True,
        "rolling_stability": metrics["rolling_folds_positive"] is True,
    }
    passed = all(gates.values())
    if not passed:
        status = "REJECTED"
        reason = "frozen primary trial failed one or more preregistered gates"
    elif frozen["dataset_lane"] == "confirmation":
        status = "SHADOW_QUEUED"
        reason = "independent alpha gates passed; execution evidence is next"
    else:
        status = "CONFIRMATION_QUEUED"
        reason = "development gates passed; independent evidence is next"
    return {
        "status": status,
        "reason": reason,
        "primary_trial_id": frozen["primary_trial_id"],
        "gates": gates,
        "automatic_strategy_application": False,
    }


def audit_experiment_program(
    *,
    registry_root: Path = REGISTRY_ROOT,
    hypothesis_root: Path = DEFAULT_HYPOTHESIS_ROOT,
) -> dict[str, Any]:
    entities = current_entities("experiments", registry_root)
    project_root = registry_root.parent
    contracts = 0
    for entity_id, event in entities.items():
        path_text = event["payload"].get("contract_path")
        if path_text is None:
            continue
        path = project_root / str(path_text)
        contract = load_hypothesis_contract(path)
        if contract["experiment_id"] != entity_id:
            raise LearningExperimentError(
                f"{entity_id}: registry identity does not match hypothesis contract"
            )
        contracts += 1
    return {
        "valid": True,
        "experiments": len(entities),
        "frozen_contracts": contracts,
        "hypothesis_root": str(hypothesis_root),
        "weekly_limit": MAX_NEW_HYPOTHESES_PER_ISO_WEEK,
        "research_lock": research_lock_status(
            registry_root=registry_root,
            lock_path=registry_root / "RESEARCH_LOCK.json",
        ),
    }


def _read_object(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise LearningExperimentError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise LearningExperimentError("input must contain an object")
    return value


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("audit", help="audit registered hypothesis contracts")
    freeze = subparsers.add_parser("freeze", help="freeze one hypothesis contract")
    freeze.add_argument("input", type=Path)
    freeze.add_argument("--output-root", type=Path, default=DEFAULT_HYPOTHESIS_ROOT)
    validate = subparsers.add_parser("validate", help="validate a frozen hypothesis")
    validate.add_argument("contract", type=Path)
    plan = subparsers.add_parser(
        "rolling-plan", help="build a chronological validation plan"
    )
    plan.add_argument("dates", type=Path)
    evaluate = subparsers.add_parser(
        "validate-result", help="validate complete family output"
    )
    evaluate.add_argument("contract", type=Path)
    evaluate.add_argument("result", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        if args.command == "audit":
            result: Any = audit_experiment_program()
        elif args.command == "freeze":
            raw = _read_object(args.input)
            enforce_weekly_hypothesis_budget(raw)
            path, contract = freeze_hypothesis_contract(raw, args.output_root)
            result = {"path": str(path), "contract_sha256": contract["contract_sha256"]}
        elif args.command == "validate":
            result = load_hypothesis_contract(args.contract)
        elif args.command == "rolling-plan":
            value = _read_object(args.dates)
            result = {
                "folds": build_rolling_origin_plan(value.get("requested_dates", []))
            }
        else:
            contract = load_hypothesis_contract(args.contract)
            result = disposition_from_result(contract, _read_object(args.result))
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    except (LearningExperimentError, OSError, ValueError) as exc:
        print(
            json.dumps({"error": str(exc), "error_type": type(exc).__name__}, indent=2)
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

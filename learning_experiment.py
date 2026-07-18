"""Preregister bounded strategy hypotheses and enforce complete experiment families."""

from __future__ import annotations

import argparse
import hashlib
import itertools
import json
import math
from collections.abc import Mapping, Sequence
from datetime import date, datetime
from pathlib import Path
from typing import Any

from learning_data import DATASET_LANES, load_frozen_dataset_contract
from learning_registry import REGISTRY_ROOT, current_entities


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_HYPOTHESIS_ROOT = REGISTRY_ROOT / "hypotheses"
SCHEMA_VERSION = 1
MAX_NEW_HYPOTHESES_PER_ISO_WEEK = 3
MAX_TRIALS_PER_FAMILY = 256
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


def build_rolling_origin_plan(
    requested_dates: Sequence[str],
    *,
    minimum_train_days: int = 40,
    embargo_days: int = 1,
) -> list[dict[str, Any]]:
    if embargo_days < 1:
        raise LearningExperimentError(
            "rolling-origin plan needs at least one embargo day"
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
        folds.append(
            {
                "fold": len(folds) + 1,
                "train_dates": [item.isoformat() for item in parsed[:train_end]],
                "embargo_dates": [
                    item.isoformat() for item in parsed[train_end:test_start]
                ],
                "test_dates": [
                    item.isoformat() for item in parsed[test_start:test_end]
                ],
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
    primary = contract.get("primary_parameters")
    if not isinstance(primary, Mapping):
        raise LearningExperimentError("primary_parameters must be an object")
    matching = [trial for trial in trials if trial["parameters"] == dict(primary)]
    if len(matching) != 1:
        raise LearningExperimentError(
            "primary_parameters must select exactly one registered grid trial"
        )
    parent = contract.get("parent_experiment_id")
    if parent is not None and (
        not isinstance(parent, str) or parent == contract["experiment_id"]
    ):
        raise LearningExperimentError("parent_experiment_id is invalid")
    contract["trial_family"] = trials
    contract["primary_trial_id"] = matching[0]["trial_id"]
    return contract


def freeze_hypothesis_contract(
    value: Mapping[str, Any], output_root: Path = DEFAULT_HYPOTHESIS_ROOT
) -> tuple[Path, dict[str, Any]]:
    contract = validate_hypothesis_contract(value)
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
        for field in (
            "total_log_growth",
            "bootstrap_lower_mean",
            "profit_factor",
            "maximum_drawdown",
            "deflated_sharpe_probability",
            "pbo_probability",
        ):
            value = metrics.get(field)
            if (
                isinstance(value, bool)
                or not isinstance(value, (int, float))
                or not math.isfinite(value)
            ):
                raise LearningExperimentError(f"trial metric {field} must be finite")
        for field in ("holm_reject_null", "rolling_folds_positive"):
            if not isinstance(metrics.get(field), bool):
                raise LearningExperimentError(f"trial metric {field} must be boolean")
    implementation = result.get("implementation_sha256")
    if not isinstance(implementation, str) or len(implementation) != 64:
        raise LearningExperimentError("evaluation needs implementation_sha256")
    return dict(result)


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

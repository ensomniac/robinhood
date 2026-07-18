"""Three-axis strategy evidence, champion comparison, and degradation monitoring."""

from __future__ import annotations

import argparse
import json
import math
import statistics
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from learning_registry import REGISTRY_ROOT, current_entities, load_registry
from strategy_ledger import DEFAULT_LEDGER_PATH, build_report, read_records


ALPHA_STATES = {"UNTESTED", "DEVELOPMENT", "CONFIRMED", "DEGRADED", "RETIRED"}
EXECUTION_STATES = {
    "UNVERIFIED",
    "BAR_ONLY",
    "SHADOW_VERIFIED",
    "LIVE_CALIBRATED",
}
OPERATIONS_STATES = {"READY", "PAUSED", "KILL_SWITCH"}
ALPHA_TRANSITIONS = {
    "UNTESTED": {"DEVELOPMENT", "RETIRED"},
    "DEVELOPMENT": {"CONFIRMED", "DEGRADED", "RETIRED"},
    "CONFIRMED": {"DEGRADED", "RETIRED"},
    "DEGRADED": {"CONFIRMED", "RETIRED"},
    "RETIRED": set(),
}
EXECUTION_TRANSITIONS = {
    "UNVERIFIED": {"BAR_ONLY"},
    "BAR_ONLY": {"SHADOW_VERIFIED"},
    "SHADOW_VERIFIED": {"LIVE_CALIBRATED", "BAR_ONLY"},
    "LIVE_CALIBRATED": {"SHADOW_VERIFIED"},
}
OPERATIONS_TRANSITIONS = {
    "READY": {"PAUSED", "KILL_SWITCH"},
    "PAUSED": {"READY", "KILL_SWITCH"},
    "KILL_SWITCH": {"PAUSED"},
}
PRODUCTION_ROLES = {
    "CHAMPION",
    "ACTIVE_CHALLENGER",
    "RESEARCH_CHALLENGER",
    "RETIRED_CHALLENGER",
}


class LearningStrategyError(ValueError):
    """Raised when strategy evidence axes or paired account evidence are unsafe."""


def _finite(value: Any, field: str) -> float:
    if isinstance(value, bool):
        raise LearningStrategyError(f"{field} must be numeric")
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise LearningStrategyError(f"{field} must be numeric") from exc
    if not math.isfinite(result):
        raise LearningStrategyError(f"{field} must be finite")
    return result


def validate_axis_transition(axis: str, previous: str, current: str) -> None:
    tables = {
        "alpha": ALPHA_TRANSITIONS,
        "execution": EXECUTION_TRANSITIONS,
        "operations": OPERATIONS_TRANSITIONS,
    }
    try:
        allowed = tables[axis][previous]
    except KeyError as exc:
        raise LearningStrategyError(f"unknown {axis} state {previous!r}") from exc
    if current not in allowed:
        raise LearningStrategyError(
            f"{axis} transition {previous} -> {current} is not allowed"
        )


def strategy_readiness(
    payload: Mapping[str, Any], *, earned_maturity: str | None = None
) -> str:
    alpha = payload.get("alpha_state")
    execution = payload.get("execution_state")
    operations = payload.get("operations_state")
    role = payload.get("production_role")
    if alpha == "RETIRED":
        return "RETIRED"
    if operations != "READY":
        return "PAUSED"
    if alpha != "CONFIRMED" or execution in {"UNVERIFIED", "BAR_ONLY"}:
        return "RESEARCH_ONLY"
    if execution == "SHADOW_VERIFIED":
        return "SHADOW_QUALIFIED"
    if execution == "LIVE_CALIBRATED":
        if role == "CHAMPION" and earned_maturity in {"PROVISIONAL", "VALIDATED"}:
            return str(earned_maturity)
        return "LIVE_PILOT_PROPOSAL"
    raise LearningStrategyError("strategy evidence axes are invalid")


def audit_strategy_evidence(root: Path = REGISTRY_ROOT) -> dict[str, Any]:
    events = load_registry("strategies", root)
    previous: dict[str, Mapping[str, Any]] = {}
    for event in events:
        payload = event["payload"]
        if payload.get("alpha_state") not in ALPHA_STATES:
            raise LearningStrategyError("invalid alpha state")
        if payload.get("execution_state") not in EXECUTION_STATES:
            raise LearningStrategyError("invalid execution state")
        if payload.get("operations_state") not in OPERATIONS_STATES:
            raise LearningStrategyError("invalid operations state")
        if payload.get("production_role") not in PRODUCTION_ROLES:
            raise LearningStrategyError("invalid production role")
        entity_id = str(event["entity_id"])
        if entity_id in previous:
            prior = previous[entity_id]
            for axis, field in (
                ("alpha", "alpha_state"),
                ("execution", "execution_state"),
                ("operations", "operations_state"),
            ):
                if prior[field] != payload[field]:
                    validate_axis_transition(
                        axis, str(prior[field]), str(payload[field])
                    )
        previous[entity_id] = payload
    current = current_entities("strategies", root)
    champions = [
        entity_id
        for entity_id, event in current.items()
        if event["payload"]["production_role"] == "CHAMPION"
    ]
    if len(champions) != 1:
        raise LearningStrategyError(
            "strategy registry must contain exactly one champion"
        )
    return {
        "valid": True,
        "strategies": len(current),
        "events": len(events),
        "champion": champions[0],
    }


def build_strategy_evidence_report(
    *,
    registry_root: Path = REGISTRY_ROOT,
    ledger_path: Path = DEFAULT_LEDGER_PATH,
) -> dict[str, Any]:
    audit = audit_strategy_evidence(registry_root)
    ledger = build_report(read_records(ledger_path))
    production_version = ledger["strategy_version"]
    maturity = ledger["maturity"]["earned_maturity"]
    strategies: dict[str, Any] = {}
    for entity_id, event in current_entities("strategies", registry_root).items():
        payload = event["payload"]
        strategy_maturity = (
            maturity if payload["version"] == production_version else None
        )
        strategies[entity_id] = {
            **payload,
            "earned_production_maturity": strategy_maturity,
            "readiness": strategy_readiness(payload, earned_maturity=strategy_maturity),
        }
    return {
        "valid": True,
        "champion": audit["champion"],
        "production_strategy_version": production_version,
        "production_maturity": maturity,
        "strategies": strategies,
        "automatic_strategy_application": False,
    }


def anytime_hoeffding_confidence_sequence(
    values: Sequence[float], *, alpha: float = 0.10, absolute_bound: float = 0.10
) -> list[dict[str, float | int]]:
    """Time-uniform mean bounds via a summable per-time error schedule."""
    if not 0 < alpha < 1:
        raise LearningStrategyError("alpha must be between zero and one")
    if absolute_bound <= 0:
        raise LearningStrategyError("absolute_bound must be positive")
    normalized = [_finite(value, "confidence-sequence value") for value in values]
    if any(abs(value) > absolute_bound for value in normalized):
        raise LearningStrategyError("confidence-sequence value exceeds frozen bound")
    result: list[dict[str, float | int]] = []
    total = 0.0
    for index, value in enumerate(normalized, 1):
        total += value
        mean = total / index
        # alpha_t = alpha/(t(t+1)); the series sums to alpha.
        alpha_t = alpha / (index * (index + 1))
        radius = absolute_bound * math.sqrt(2 * math.log(2 / alpha_t) / index)
        result.append(
            {
                "observations": index,
                "mean": mean,
                "lower": mean - radius,
                "upper": mean + radius,
            }
        )
    return result


def compare_champion_challenger(
    days: Sequence[Mapping[str, Any]], *, alpha: float = 0.10
) -> dict[str, Any]:
    if not days:
        raise LearningStrategyError("paired comparison needs requested days")
    dates: list[str] = []
    champion: list[float] = []
    challenger: list[float] = []
    log_differences: list[float] = []
    for index, item in enumerate(days):
        if not isinstance(item, Mapping):
            raise LearningStrategyError(f"days[{index}] must be an object")
        day = item.get("date")
        if not isinstance(day, str) or (dates and day <= dates[-1]):
            raise LearningStrategyError(
                "comparison dates must be unique and chronological"
            )
        champion_return = _finite(item.get("champion_return"), "champion_return")
        challenger_return = _finite(item.get("challenger_return"), "challenger_return")
        if champion_return <= -1 or challenger_return <= -1:
            raise LearningStrategyError(
                "paired account return cannot lose 100% or more"
            )
        dates.append(day)
        champion.append(champion_return)
        challenger.append(challenger_return)
        log_differences.append(
            math.log1p(challenger_return) - math.log1p(champion_return)
        )
    sequence = anytime_hoeffding_confidence_sequence(log_differences, alpha=alpha)
    final = sequence[-1]
    if final["lower"] > 0:
        decision = "CHALLENGER_SUPERIOR"
    elif final["upper"] < 0:
        decision = "CHALLENGER_INFERIOR"
    else:
        decision = "CONTINUE_PAIRED_EVIDENCE"
    return {
        "requested_days": len(days),
        "champion_zero_days": sum(value == 0 for value in champion),
        "challenger_zero_days": sum(value == 0 for value in challenger),
        "champion_log_growth": sum(math.log1p(value) for value in champion),
        "challenger_log_growth": sum(math.log1p(value) for value in challenger),
        "mean_paired_log_advantage": statistics.fmean(log_differences),
        "confidence_sequence": sequence,
        "decision": decision,
        "automatic_promotion": False,
    }


def monitor_strategy_health(
    daily_returns: Sequence[float],
    *,
    rule_violations: int,
    protection_failures: int,
    evidence_hash_matches: bool,
    alpha: float = 0.10,
) -> dict[str, Any]:
    if rule_violations < 0 or protection_failures < 0:
        raise LearningStrategyError("violation counts cannot be negative")
    if not evidence_hash_matches or rule_violations or protection_failures:
        return {
            "status": "PAUSE_IMMEDIATELY",
            "reason": "integrity, rule, or protection failure",
            "automatic_strategy_change": False,
        }
    log_returns = []
    for value in daily_returns:
        normalized = _finite(value, "daily return")
        if normalized <= -1:
            raise LearningStrategyError("daily return cannot lose 100% or more")
        log_returns.append(math.log1p(normalized))
    if len(log_returns) < 20:
        return {
            "status": "INSUFFICIENT_MONITORING_EVIDENCE",
            "observations": len(log_returns),
            "automatic_strategy_change": False,
        }
    sequence = anytime_hoeffding_confidence_sequence(log_returns, alpha=alpha)
    final = sequence[-1]
    status = "DEGRADED" if final["upper"] < 0 else "HEALTHY_OR_INCONCLUSIVE"
    return {
        "status": status,
        "observations": len(log_returns),
        "total_log_growth": sum(log_returns),
        "confidence_sequence_final": final,
        "automatic_strategy_change": False,
    }


def _read_object(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise LearningStrategyError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise LearningStrategyError("input must contain an object")
    return value


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("audit", help="audit three-axis strategy evidence")
    subparsers.add_parser(
        "report", help="combine strategy axes and production maturity"
    )
    compare = subparsers.add_parser(
        "compare", help="compare paired daily account returns"
    )
    compare.add_argument("input", type=Path)
    health = subparsers.add_parser(
        "health", help="evaluate degradation and safety signals"
    )
    health.add_argument("input", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        if args.command == "audit":
            result: Any = audit_strategy_evidence()
        elif args.command == "report":
            result = build_strategy_evidence_report()
        elif args.command == "compare":
            result = compare_champion_challenger(
                _read_object(args.input).get("days", [])
            )
        else:
            value = _read_object(args.input)
            result = monitor_strategy_health(
                value.get("daily_returns", []),
                rule_violations=int(value.get("rule_violations", 0)),
                protection_failures=int(value.get("protection_failures", 0)),
                evidence_hash_matches=value.get("evidence_hash_matches") is True,
            )
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    except (LearningStrategyError, OSError, ValueError) as exc:
        print(
            json.dumps({"error": str(exc), "error_type": type(exc).__name__}, indent=2)
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

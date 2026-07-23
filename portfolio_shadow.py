"""Prospective, zero-broker shadow lifecycle for an exact frozen v2 winner.

Entry evaluation and fill observation are captured while quotes are fresh.  A
filled shadow is closed later from a second fresh bid/ask observation.  Nothing
in this module imports a broker connector or performs a broker action.
"""

from __future__ import annotations

import argparse
import json
import math
import re
import sys
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import portfolio_execution
import portfolio_maturity
import strategy_discovery


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_ROOT = strategy_discovery.DEFAULT_ROOT
SCHEMA_VERSION = 1
MAXIMUM_OBSERVATION_AGE_SECONDS = 5
PRIVATE_FIELD_PATTERN = re.compile(
    r"(?:account_number|broker_order_id|client_ref_id|confirmation_id|"
    r"cancel(?:lation)?_id|replacement_id|password|token|cookie|credential|mfa)",
    re.IGNORECASE,
)
SETUP_FIELDS = {
    "schema_version",
    "session_date",
    "market_facts",
    "account",
    "fill_observation",
}
CLOSURE_FIELDS = {
    "schema_version",
    "protection_observation",
    "exit_observation",
    "monitoring_complete",
    "journal_complete",
    "session_capture_complete",
    "rule_violations",
}
EXIT_REASONS = {
    "strategy_exit",
    "stop",
    "target",
    "safe_cutoff",
    "maximum_hold",
}


class PortfolioShadowError(ValueError):
    """A shadow transition is stale, incomplete, drifted, or privacy-unsafe."""


def _read_object(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise PortfolioShadowError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise PortfolioShadowError(f"{path} must contain an object")
    return value


def _object(value: Any, field: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise PortfolioShadowError(f"{field} must be an object")
    return dict(value)


def _boolean(value: Any, field: str) -> bool:
    if not isinstance(value, bool):
        raise PortfolioShadowError(f"{field} must be boolean")
    return value


def _number(value: Any, field: str, *, positive: bool = False) -> float:
    if isinstance(value, bool):
        raise PortfolioShadowError(f"{field} must be numeric")
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise PortfolioShadowError(f"{field} must be numeric") from exc
    if not math.isfinite(result) or (positive and result <= 0):
        raise PortfolioShadowError(f"{field} is invalid")
    return result


def _timestamp(value: Any, field: str) -> datetime:
    if not isinstance(value, str):
        raise PortfolioShadowError(f"{field} must be an ISO timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise PortfolioShadowError(f"{field} must be an ISO timestamp") from exc
    if parsed.tzinfo is None:
        raise PortfolioShadowError(f"{field} must include a timezone")
    return parsed.astimezone(UTC)


def _fresh_observation(
    observed_at: datetime,
    current: datetime,
    field: str,
) -> None:
    age = (current - observed_at).total_seconds()
    if age < 0 or age > MAXIMUM_OBSERVATION_AGE_SECONDS:
        raise PortfolioShadowError(f"{field} is stale or future-dated")


def _assert_privacy_safe(value: Any, path: str = "input") -> None:
    if isinstance(value, Mapping):
        for key, item in value.items():
            if PRIVATE_FIELD_PATTERN.search(str(key)):
                raise PortfolioShadowError(f"private field is forbidden: {path}.{key}")
            _assert_privacy_safe(item, f"{path}.{key}")
    elif isinstance(value, list):
        for index, item in enumerate(value):
            _assert_privacy_safe(item, f"{path}[{index}]")


def _validate_top_level(value: Mapping[str, Any], allowed: set[str], label: str) -> None:
    extras = sorted(set(value) - allowed)
    missing = sorted(allowed - set(value))
    if extras or missing:
        raise PortfolioShadowError(
            f"{label} fields differ; missing={missing}, extras={extras}"
        )
    if value.get("schema_version") != SCHEMA_VERSION:
        raise PortfolioShadowError(f"{label} schema_version must be 1")
    _assert_privacy_safe(value, label)


def _load_queue_and_winner(
    queue_path: Path,
    *,
    enforce_commit: bool,
) -> tuple[dict[str, Any], Path, dict[str, Any]]:
    if enforce_commit:
        strategy_discovery.require_committed(queue_path)
    queue = strategy_discovery.load_artifact(
        queue_path, expected_kind="prospective-shadow-queue"
    )
    if queue.get("state") != "SHADOW_QUEUED":
        raise PortfolioShadowError("shadow queue is not active")
    if queue.get("campaign_id") != strategy_discovery.CAMPAIGN_ID:
        raise PortfolioShadowError("shadow queue campaign drifted")
    queued_at = _timestamp(queue.get("queued_at"), "shadow queue queued_at")
    raw_winner_path = queue.get("winner_path")
    if not isinstance(raw_winner_path, str):
        raise PortfolioShadowError("shadow queue does not bind its winner path")
    winner_path = PROJECT_ROOT / raw_winner_path
    if enforce_commit:
        strategy_discovery.require_committed(winner_path)
    winner = strategy_discovery.load_artifact(
        winner_path, expected_kind="frozen-strategy-winner"
    )
    if winner.get("artifact_sha256") != queue.get("winner_sha256"):
        raise PortfolioShadowError("shadow queue winner artifact drifted")
    for field in ("strategy_id", "strategy_version", "rules_hash", "family_id"):
        if winner.get(field) != queue.get(field):
            raise PortfolioShadowError(f"shadow queue winner {field} drifted")
    winner_recorded_at = _timestamp(
        winner.get("recorded_at"), "winner recorded_at"
    )
    if queued_at <= winner_recorded_at:
        raise PortfolioShadowError(
            "shadow queue activation does not follow winner preregistration"
        )
    if queue.get("broker_actions_permitted") is not False:
        raise PortfolioShadowError("shadow queue does not preserve zero broker actions")
    admitted = queue.get("historical_records_admitted")
    if (
        queue.get("required_clean_closed_shadows") != 5
        or queue.get("completed_clean_closed_shadows") != 0
        or isinstance(admitted, bool)
        or not isinstance(admitted, int)
        or admitted < 1
        or queue.get("historical_admission_verified") is not True
        or queue.get("historical_validation_phase") != "SHADOW_QUALIFICATION"
    ):
        raise PortfolioShadowError("shadow queue qualification contract is invalid")

    predecessor_specs = (
        (
            "confirmation_inspection_path",
            "confirmation_inspection_sha256",
            "confirmation-inspection",
        ),
        (
            "historical_maturity_ledger_path",
            "historical_maturity_ledger_sha256",
            "historical-maturity-ledger",
        ),
    )
    predecessors: dict[str, dict[str, Any]] = {}
    for path_field, hash_field, kind in predecessor_specs:
        relative = queue.get(path_field)
        if not isinstance(relative, str):
            raise PortfolioShadowError(f"shadow queue {path_field} is missing")
        path = PROJECT_ROOT / relative
        if enforce_commit:
            strategy_discovery.require_committed(path)
        artifact = strategy_discovery.load_artifact(path, expected_kind=kind)
        if artifact.get("artifact_sha256") != queue.get(hash_field):
            raise PortfolioShadowError(f"shadow queue {kind} binding drifted")
        predecessors[kind] = artifact

    confirmation = predecessors["confirmation-inspection"]
    historical = predecessors["historical-maturity-ledger"]
    if not (
        confirmation.get("state") == "CONFIRMATION_PASSED"
        and confirmation.get("shadow_queue_permitted") is True
        and confirmation.get("broker_actions_permitted") is False
        and confirmation.get("winner_sha256") == winner.get("artifact_sha256")
        and historical.get("state") == "HISTORICAL_EVIDENCE_INSPECTED"
        and historical.get("append_permitted") is True
        and historical.get("broker_actions_permitted") is False
        and historical.get("confirmation_inspection_path")
        == queue.get("confirmation_inspection_path")
    ):
        raise PortfolioShadowError("shadow queue predecessor state is invalid")
    for predecessor in (confirmation, historical):
        for field in ("campaign_id", "family_id", "strategy_id", "strategy_version", "rules_hash"):
            if predecessor.get(field) != queue.get(field):
                raise PortfolioShadowError(
                    f"shadow queue predecessor {field} drifted"
                )
    return queue, winner_path, winner


def _fill_from_observation(
    evaluation: Mapping[str, Any],
    observation: Mapping[str, Any],
    *,
    evaluation_now: datetime,
) -> dict[str, Any]:
    value = _object(observation, "fill_observation")
    expected = {"observed_at", "bid", "ask", "available_ask_quantity"}
    if set(value) != expected:
        raise PortfolioShadowError("fill_observation has unexpected fields")
    observed_at = _timestamp(value["observed_at"], "fill_observation.observed_at")
    _fresh_observation(observed_at, evaluation_now, "fill observation")
    market_observed_at = _timestamp(
        evaluation["market"]["observed_at"], "production market observed_at"
    )
    if observed_at < market_observed_at:
        raise PortfolioShadowError("fill observation predates production evaluation")
    bid = _number(value["bid"], "fill_observation.bid", positive=True)
    ask = _number(value["ask"], "fill_observation.ask", positive=True)
    if bid > ask:
        raise PortfolioShadowError("fill observation quote is crossed")
    available = _number(
        value["available_ask_quantity"],
        "fill_observation.available_ask_quantity",
    )
    if available < 0 or not available.is_integer():
        raise PortfolioShadowError("available ask quantity must be a whole number")
    order = evaluation["order"]
    requested = int(order["quantity"])
    filled = min(requested, int(available)) if ask <= float(order["limit_price"]) else 0
    if filled == 0:
        status = "missed_limit"
        fill_price = None
    elif filled < requested:
        status = "partial_fill"
        fill_price = ask
    else:
        status = "filled"
        fill_price = ask
    return {
        "status": status,
        "observed_at": observed_at.isoformat(),
        "bid": bid,
        "ask": ask,
        "requested_quantity": requested,
        "filled_quantity": filled,
        "residual_quantity": requested - filled,
        "fill_price": fill_price,
        "broker_actions": 0,
    }


def start_shadow(
    queue_path: Path,
    setup: Mapping[str, Any],
    *,
    root: Path = DEFAULT_ROOT,
    enforce_commit: bool = True,
    now: datetime | None = None,
) -> tuple[Path, dict[str, Any]]:
    """Capture one fresh exact-rule evaluation and realistic simulated fill."""
    _validate_top_level(setup, SETUP_FIELDS, "shadow setup")
    try:
        session_date = date.fromisoformat(str(setup["session_date"]))
    except ValueError as exc:
        raise PortfolioShadowError("session_date must be an ISO date") from exc
    queue, winner_path, winner = _load_queue_and_winner(
        queue_path, enforce_commit=enforce_commit
    )
    account = _object(setup["account"], "account")
    if set(account) != {"equity", "buying_power"}:
        raise PortfolioShadowError("shadow account permits only equity and buying_power")
    current = (now or datetime.now(UTC)).astimezone(UTC)
    queued_at = _timestamp(queue["queued_at"], "shadow queue queued_at")
    if current <= queued_at:
        raise PortfolioShadowError(
            "shadow evaluation must follow queue activation"
        )
    config = portfolio_maturity.load_config()
    try:
        evaluation = portfolio_execution.evaluate_frozen_winner(
            winner,
            _object(setup["market_facts"], "market_facts"),
            account,
            config,
            now=current,
        )
    except portfolio_execution.PortfolioExecutionError as exc:
        raise PortfolioShadowError(str(exc)) from exc
    market_day = _timestamp(
        evaluation["market"]["observed_at"], "production market observed_at"
    ).date()
    if _timestamp(
        evaluation["market"]["observed_at"], "production market observed_at"
    ) < queued_at:
        raise PortfolioShadowError(
            "production market observation predates shadow queue activation"
        )
    if market_day != session_date:
        raise PortfolioShadowError("session_date differs from the fresh market date")
    fill = _fill_from_observation(
        evaluation,
        _object(setup["fill_observation"], "fill_observation"),
        evaluation_now=current,
    )
    signal_id = f"{session_date.isoformat()}-{winner['strategy_id']}-shadow"
    existing = list(
        (root / str(winner["family_id"]) / "shadow-entry").glob(
            f"{signal_id}-*.json"
        )
    )
    if existing:
        raise PortfolioShadowError("this exact strategy already has a shadow entry today")
    payload = {
        "schema_version": SCHEMA_VERSION,
        "artifact_kind": "prospective-shadow-entry",
        "campaign_id": strategy_discovery.CAMPAIGN_ID,
        "family_id": winner["family_id"],
        "strategy_id": winner["strategy_id"],
        "strategy_version": winner["strategy_version"],
        "rules_hash": winner["rules_hash"],
        "signal_id": signal_id,
        "session_date": session_date.isoformat(),
        "state": "SHADOW_ENTRY_FILLED" if fill["filled_quantity"] else "SHADOW_ENTRY_MISSED",
        "queue_path": strategy_discovery._relative(queue_path),
        "queue_sha256": queue["artifact_sha256"],
        "winner_path": strategy_discovery._relative(winner_path),
        "winner_sha256": winner["artifact_sha256"],
        "evaluation_now": current.isoformat(),
        "setup": dict(setup),
        "production_evaluation": evaluation,
        "fill": fill,
        "broker_actions_performed": 0,
    }
    return strategy_discovery._write_artifact(
        payload,
        root / str(winner["family_id"]) / "shadow-entry",
        signal_id,
    )


def _protection_result(
    entry: Mapping[str, Any],
    value: Any,
    *,
    close_now: datetime,
) -> dict[str, Any]:
    filled_at = _timestamp(entry["fill"]["observed_at"], "fill observed_at")
    if not entry["fill"]["filled_quantity"]:
        if value is not None:
            raise PortfolioShadowError("missed limit must not invent protection coverage")
        return {
            "plan_complete": True,
            "simulated_coverage_ready": False,
            "unprotected_seconds": 0.0,
        }
    if value is None:
        return {
            "plan_complete": False,
            "simulated_coverage_ready": False,
            "unprotected_seconds": (
                close_now - filled_at
            ).total_seconds(),
        }
    observed = _object(value, "protection_observation")
    expected = {"planned_at", "ready_at", "time_in_force", "failure_safe_cutoff"}
    if set(observed) != expected:
        raise PortfolioShadowError("protection_observation has unexpected fields")
    planned_at = _timestamp(observed["planned_at"], "protection planned_at")
    ready_at = _timestamp(observed["ready_at"], "protection ready_at")
    if planned_at > filled_at or ready_at < filled_at or ready_at > close_now:
        raise PortfolioShadowError("shadow protection timing is impossible")
    expected_protection = entry["production_evaluation"]["protection"]
    if observed["time_in_force"] != expected_protection["time_in_force"]:
        raise PortfolioShadowError("shadow protection time-in-force drifted")
    if observed["failure_safe_cutoff"] != expected_protection["failure_safe_cutoff"]:
        raise PortfolioShadowError("shadow protection safe cutoff drifted")
    return {
        "plan_complete": True,
        "simulated_coverage_ready": True,
        "planned_at": planned_at.isoformat(),
        "ready_at": ready_at.isoformat(),
        "time_in_force": observed["time_in_force"],
        "failure_safe_cutoff": observed["failure_safe_cutoff"],
        "unprotected_seconds": (ready_at - filled_at).total_seconds(),
    }


def _exit_result(
    entry: Mapping[str, Any],
    value: Any,
    *,
    close_now: datetime,
) -> dict[str, Any] | None:
    if not entry["fill"]["filled_quantity"]:
        if value is not None:
            raise PortfolioShadowError("missed limit must not invent an exit")
        return None
    observed = _object(value, "exit_observation")
    expected = {"observed_at", "bid", "ask", "reason"}
    if set(observed) != expected:
        raise PortfolioShadowError("exit_observation has unexpected fields")
    exit_at = _timestamp(observed["observed_at"], "exit observed_at")
    _fresh_observation(exit_at, close_now, "exit observation")
    filled_at = _timestamp(entry["fill"]["observed_at"], "fill observed_at")
    if exit_at < filled_at:
        raise PortfolioShadowError("shadow exit predates its fill")
    bid = _number(observed["bid"], "exit bid", positive=True)
    ask = _number(observed["ask"], "exit ask", positive=True)
    if bid > ask:
        raise PortfolioShadowError("exit quote is crossed")
    reason = observed["reason"]
    if reason not in EXIT_REASONS:
        raise PortfolioShadowError("shadow exit reason is not frozen")
    return {
        "observed_at": exit_at.isoformat(),
        "bid": bid,
        "ask": ask,
        "exit_price": bid,
        "reason": reason,
        "stop_executed": reason == "stop",
    }


def _return_metrics(
    entry: Mapping[str, Any], exit_result: Mapping[str, Any] | None
) -> dict[str, float]:
    if exit_result is None:
        return {"net_r": 0.0, "stress_10bps_r": 0.0, "stress_20bps_r": 0.0}
    entry_price = float(entry["fill"]["fill_price"])
    exit_price = float(exit_result["exit_price"])
    stop = float(entry["production_evaluation"]["protection"]["stop_price"])
    risk_per_share = entry_price - stop
    if risk_per_share <= 0:
        raise PortfolioShadowError("observed fill destroyed the structural stop")

    def net_r(cost_bps_per_side: int) -> float:
        cost = (entry_price + exit_price) * cost_bps_per_side / 10_000
        return (exit_price - entry_price - cost) / risk_per_share

    return {
        "net_r": net_r(5),
        "stress_10bps_r": net_r(10),
        "stress_20bps_r": net_r(20),
    }


def rebuild_final(
    entry: Mapping[str, Any], closure: Mapping[str, Any]
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Deterministically rebuild final shadow facts and its maturity row."""
    public_closure = {key: item for key, item in closure.items() if key != "close_now"}
    _validate_top_level(public_closure, CLOSURE_FIELDS, "shadow closure")
    if "close_now" not in closure:
        raise PortfolioShadowError("internal closure is missing close_now")
    close_now = _timestamp(closure["close_now"], "close_now")
    evaluation_now = _timestamp(entry.get("evaluation_now"), "evaluation_now")
    if close_now < evaluation_now:
        raise PortfolioShadowError("shadow close predates its evaluation")
    protection = _protection_result(
        entry, closure["protection_observation"], close_now=close_now
    )
    exit_result = _exit_result(
        entry, closure["exit_observation"], close_now=close_now
    )
    monitoring_complete = _boolean(
        closure["monitoring_complete"], "monitoring_complete"
    )
    journal_complete = _boolean(closure["journal_complete"], "journal_complete")
    capture_complete = _boolean(
        closure["session_capture_complete"], "session_capture_complete"
    )
    supplied_violations = closure["rule_violations"]
    if not isinstance(supplied_violations, list) or any(
        not isinstance(item, str) or not item.strip()
        for item in supplied_violations
    ):
        raise PortfolioShadowError("rule_violations must be an array of strings")
    violations = list(supplied_violations)
    for incomplete, violation in (
        (not protection["plan_complete"], "shadow_protection_path_incomplete"),
        (not monitoring_complete, "shadow_monitoring_incomplete"),
        (not journal_complete, "shadow_journal_incomplete"),
    ):
        if incomplete and violation not in violations:
            violations.append(violation)
    path_complete = all(
        (protection["plan_complete"], monitoring_complete, journal_complete)
    )
    filled = int(entry["fill"]["filled_quantity"]) > 0
    eligible = filled and path_complete and capture_complete and not violations
    metrics = _return_metrics(entry, exit_result)
    final = {
        "protection": protection,
        "exit": exit_result,
        "path_complete": path_complete,
        "session_capture_complete": capture_complete,
        "rule_violations": list(violations),
        "eligible_clean_closed_shadow": eligible,
        "fill_status": entry["fill"]["status"],
        "broker_actions_performed": 0,
        **metrics,
    }
    record = {
        "schema_version": portfolio_maturity.SCHEMA_VERSION,
        "research_campaign_id": strategy_discovery.CAMPAIGN_ID,
        "record_type": "signal",
        "recorded_at": close_now.isoformat(),
        "strategy_id": entry["strategy_id"],
        "strategy_version": entry["strategy_version"],
        "mechanism_family": entry["family_id"],
        "rules_hash": entry["rules_hash"],
        "date": entry["session_date"],
        "sample_phase": "shadow",
        "mode": "shadow",
        "signal_id": entry["signal_id"],
        "closed": True,
        "eligible": eligible,
        **metrics,
        "stop_executed": bool(exit_result and exit_result["stop_executed"]),
        "discovery_complete": True,
        "evaluation_complete": True,
        "sizing_complete": True,
        "order_construction_complete": True,
        "protection_plan_complete": bool(protection["plan_complete"]),
        "monitoring_complete": monitoring_complete,
        "journal_complete": journal_complete,
        "broker_actions": 0,
        "session_capture_complete": capture_complete,
        "rule_violations": list(violations),
    }
    portfolio_maturity.validate_record(record)
    return final, record


def close_shadow(
    entry_path: Path,
    closure: Mapping[str, Any],
    *,
    root: Path = DEFAULT_ROOT,
    enforce_commit: bool = True,
    now: datetime | None = None,
) -> tuple[Path, dict[str, Any]]:
    """Close or expire a committed entry capture with fresh exit facts."""
    if enforce_commit:
        strategy_discovery.require_committed(entry_path)
    entry = strategy_discovery.load_artifact(
        entry_path, expected_kind="prospective-shadow-entry"
    )
    value = dict(closure)
    _validate_top_level(value, CLOSURE_FIELDS, "shadow closure")
    close_now = (now or datetime.now(UTC)).astimezone(UTC)
    internal_closure = {**value, "close_now": close_now.isoformat()}
    final, record = rebuild_final(entry, internal_closure)
    payload = {
        "schema_version": SCHEMA_VERSION,
        "artifact_kind": "prospective-shadow-final",
        "campaign_id": entry["campaign_id"],
        "family_id": entry["family_id"],
        "strategy_id": entry["strategy_id"],
        "strategy_version": entry["strategy_version"],
        "rules_hash": entry["rules_hash"],
        "signal_id": entry["signal_id"],
        "state": "SHADOW_CLOSED_CLEAN" if record["eligible"] else "SHADOW_CLOSED_NONQUALIFYING",
        "entry_path": strategy_discovery._relative(entry_path),
        "entry_sha256": entry["artifact_sha256"],
        "closure": internal_closure,
        "final": final,
        "maturity_record": record,
        "broker_actions_performed": 0,
        "admission_permitted_after_independent_inspection": True,
    }
    return strategy_discovery._write_artifact(
        payload,
        root / str(entry["family_id"]) / "shadow-final",
        entry["signal_id"],
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    subparsers = parser.add_subparsers(dest="command", required=True)
    start = subparsers.add_parser("start")
    start.add_argument("queue", type=Path)
    start.add_argument("setup", type=Path)
    close = subparsers.add_parser("close")
    close.add_argument("entry", type=Path)
    close.add_argument("closure", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "start":
            path, artifact = start_shadow(
                args.queue, _read_object(args.setup), root=args.root
            )
        else:
            path, artifact = close_shadow(
                args.entry, _read_object(args.closure), root=args.root
            )
        print(
            json.dumps(
                {
                    "written": strategy_discovery._relative(path),
                    "artifact_sha256": artifact["artifact_sha256"],
                    "state": artifact["state"],
                    "broker_actions_performed": 0,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    except (
        OSError,
        PortfolioShadowError,
        strategy_discovery.StrategyDiscoveryError,
        ValueError,
    ) as exc:
        print(
            json.dumps(
                {"error": str(exc), "error_type": type(exc).__name__},
                indent=2,
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

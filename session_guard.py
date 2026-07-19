"""Deterministic pre-entry and open-position safety interlock.

This module evaluates broker/tool facts supplied by the active workflow. It does
not fetch account state or place/cancel orders; those tool responses remain the
authoritative source of each input.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from dataclasses import asdict, dataclass
from datetime import time
from pathlib import Path
from typing import Any, Mapping, Sequence

from strategy_engine import StrategyConfig, StrategyInputError, load_config
from strategy_ledger import DEFAULT_LEDGER_PATH, LedgerError, read_records
from strategy_maturity import assess_maturity


MATURITY_RANK = {"UNVALIDATED": 0, "PROVISIONAL": 1, "VALIDATED": 2}


class GuardInputError(ValueError):
    """Raised when a safety snapshot is incomplete or invalid."""


@dataclass(frozen=True)
class GuardDecision:
    status: str
    entry_allowed: bool
    must_flatten: bool
    strategy_version: str
    rules_hash: str
    declared_maturity: str
    earned_maturity: str
    reasons: tuple[str, ...]
    required_actions: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def _required(snapshot: Mapping[str, Any], field: str) -> Any:
    try:
        return snapshot[field]
    except KeyError as exc:
        raise GuardInputError(f"missing required field: {field}") from exc


def _boolean(snapshot: Mapping[str, Any], field: str) -> bool:
    value = _required(snapshot, field)
    if not isinstance(value, bool):
        raise GuardInputError(f"{field} must be true or false")
    return value


def _integer(snapshot: Mapping[str, Any], field: str) -> int:
    value = _required(snapshot, field)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise GuardInputError(f"{field} must be an integer >= 0")
    return value


def _number(snapshot: Mapping[str, Any], field: str) -> float:
    value = _required(snapshot, field)
    if isinstance(value, bool):
        raise GuardInputError(f"{field} must be numeric")
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise GuardInputError(f"{field} must be numeric") from exc
    if not math.isfinite(result) or result < 0:
        raise GuardInputError(f"{field} must be finite and >= 0")
    return result


def _time(value: Any, field: str) -> time:
    if not isinstance(value, str):
        raise GuardInputError(f"{field} must be HH:MM:SS")
    try:
        return time.fromisoformat(value)
    except ValueError as exc:
        raise GuardInputError(f"{field} must be HH:MM:SS") from exc


def _decision(
    status: str,
    snapshot: Mapping[str, Any],
    earned_maturity: str,
    config: StrategyConfig,
    reasons: Sequence[str],
    actions: Sequence[str],
    *,
    entry_allowed: bool = False,
    must_flatten: bool = False,
) -> GuardDecision:
    return GuardDecision(
        status=status,
        entry_allowed=entry_allowed,
        must_flatten=must_flatten,
        strategy_version=config.version,
        rules_hash=config.rules_hash,
        declared_maturity=str(snapshot["declared_maturity"]),
        earned_maturity=earned_maturity,
        reasons=tuple(dict.fromkeys(reasons)),
        required_actions=tuple(dict.fromkeys(actions)),
    )


def evaluate_guard(
    snapshot: Mapping[str, Any],
    earned_maturity: str,
    config: StrategyConfig | None = None,
) -> GuardDecision:
    config = config or load_config()
    if earned_maturity not in MATURITY_RANK:
        raise GuardInputError(f"unknown earned maturity: {earned_maturity}")
    declared = str(_required(snapshot, "declared_maturity"))
    if declared not in MATURITY_RANK:
        raise GuardInputError(f"unknown declared maturity: {declared}")

    now = _time(_required(snapshot, "time_et"), "time_et")
    entry_start = _time(config.raw["strategy"]["entry_start_et"], "entry_start_et")
    entry_cutoff = _time(config.raw["strategy"]["entry_cutoff_et"], "entry_cutoff_et")
    force_flat = _time(config.raw["strategy"]["force_flat_et"], "force_flat_et")
    max_data_age = float(config.raw["execution"]["maximum_quote_age_seconds"])
    max_heartbeat = float(config.raw["execution"]["monitoring_heartbeat_seconds"])
    transition_limit = float(config.raw["execution"]["entry_timeout_seconds"])
    circuit_breakers = config.raw["circuit_breakers"]

    position_quantity = _integer(snapshot, "position_quantity")
    active_entry_orders = _integer(snapshot, "active_entry_orders")
    active_exit_orders = _integer(snapshot, "active_exit_orders")
    active_stop_orders = _integer(snapshot, "active_stop_orders")
    active_stop_quantity = _integer(snapshot, "active_stop_quantity")
    unknown_orders = _integer(snapshot, "unknown_orders")
    filled_entries_today = _integer(snapshot, "filled_entries_today")
    entry_order_age = _number(snapshot, "entry_order_age_seconds")
    transition_age = _number(snapshot, "protection_transition_age_seconds")
    data_age = _number(snapshot, "market_data_age_seconds")
    heartbeat_age = _number(snapshot, "monitor_heartbeat_age_seconds")

    tool_ready = all(
        _boolean(snapshot, field)
        for field in (
            "account_identified",
            "agentic_allowed",
            "broker_available",
            "monitoring_available",
        )
    )
    stale_data = data_age > max_data_age
    stale_monitor = heartbeat_age > max_heartbeat or not _boolean(
        snapshot, "monitoring_available"
    )

    # Exposure always takes priority over eligibility and research state.
    if position_quantity > 0:
        fatal_reasons: list[str] = []
        fatal_actions: list[str] = []
        if now >= force_flat:
            fatal_reasons.append("force-flat time reached with an open position")
        if stale_data:
            fatal_reasons.append("market data is stale during exposure")
        if stale_monitor:
            fatal_reasons.append("monitoring heartbeat is stale during exposure")
        if not _boolean(snapshot, "broker_available"):
            fatal_reasons.append("broker connection is unavailable during exposure")
        if unknown_orders:
            fatal_reasons.append("unknown order state exists during exposure")
        if (
            active_stop_orders > 1
            or active_stop_quantity > position_quantity
            or (active_stop_orders and active_exit_orders)
        ):
            fatal_reasons.append("protective orders can oversell the live position")
            fatal_actions.append("reconcile and cancel duplicate or excess sell orders")
        if fatal_reasons:
            fatal_actions.insert(
                0, "flatten the live position through the safest broker-permitted route"
            )
            fatal_actions.append(
                "confirm zero position and reconcile every remaining order"
            )
            return _decision(
                "KILL_SWITCH_FLATTEN",
                snapshot,
                earned_maturity,
                config,
                fatal_reasons,
                fatal_actions,
                must_flatten=True,
            )

        fully_protected = (
            active_stop_orders == 1 and active_stop_quantity == position_quantity
        )
        if not fully_protected:
            reasons = [
                "live position is not fully covered by exactly one protective stop"
            ]
            if active_entry_orders:
                reasons.append("an entry remainder remains open after a fill")
            if (
                transition_age <= transition_limit
                and tool_ready
                and _boolean(snapshot, "protective_stop_workflow_ready")
            ):
                actions = []
                if active_entry_orders:
                    actions.append("cancel and confirm the unfilled entry remainder")
                if active_exit_orders:
                    actions.append("submit or confirm the prepared exit immediately")
                else:
                    actions.append(
                        "place and confirm a stop for the actual filled quantity immediately"
                    )
                actions.append("re-run the guard from fresh broker state")
                return _decision(
                    "PROTECT_NOW",
                    snapshot,
                    earned_maturity,
                    config,
                    reasons,
                    actions,
                )
            return _decision(
                "KILL_SWITCH_FLATTEN",
                snapshot,
                earned_maturity,
                config,
                reasons
                + ["the protection transition window expired or tooling is not ready"],
                (
                    "flatten the unprotected position immediately",
                    "confirm zero position and reconcile every remaining order",
                ),
                must_flatten=True,
            )

        if active_entry_orders:
            return _decision(
                "CANCEL_ENTRY_REMAINDER",
                snapshot,
                earned_maturity,
                config,
                ("a filled position still has an active entry remainder",),
                (
                    "cancel and confirm the unfilled entry remainder immediately",
                    "re-run the guard from fresh position, stop, and order state",
                ),
            )

        return _decision(
            "MANAGE_POSITION",
            snapshot,
            earned_maturity,
            config,
            ("one fully protected position is open",),
            ("continue quote, book, order, thesis, and force-flat monitoring",),
        )

    # Flat accounts must not carry sell orders or ambiguous broker state.
    if active_stop_orders or active_stop_quantity or active_exit_orders:
        return _decision(
            "RECONCILE_FLAT_ORDERS",
            snapshot,
            earned_maturity,
            config,
            ("sell or protective orders exist while the position is flat",),
            (
                "cancel and confirm every orphan sell order",
                "re-query positions and orders before any new entry",
            ),
        )
    if unknown_orders:
        return _decision(
            "RECONCILE_UNKNOWN_ORDER",
            snapshot,
            earned_maturity,
            config,
            ("an order has unknown outcome",),
            (
                "query current orders and reconcile the original logical order before retrying",
            ),
        )
    if active_entry_orders:
        actions = ["monitor the live entry order and do not submit another entry"]
        reasons = ["an entry order is already active"]
        cancel_reasons: list[str] = []
        if entry_order_age >= transition_limit:
            cancel_reasons.append("entry order reached its timeout")
        if stale_data or stale_monitor:
            cancel_reasons.append(
                "data or monitoring became stale while entry was open"
            )
        if not _boolean(snapshot, "broker_available"):
            cancel_reasons.append(
                "broker tool surface became unavailable while entry was open"
            )
        if now > entry_cutoff:
            cancel_reasons.append("entry cutoff passed while entry was open")
        if cancel_reasons:
            reasons.extend(cancel_reasons)
            actions = [
                "cancel and confirm the stale entry order",
                "re-query position and orders",
            ]
        return _decision(
            "MANAGE_ENTRY_ORDER",
            snapshot,
            earned_maturity,
            config,
            reasons,
            actions,
        )

    blockers: list[str] = []
    required_true = {
        "account_identified": "authorized Agentic account is not identified",
        "agentic_allowed": "account is not agentic-authorized",
        "broker_available": "broker tool surface is unavailable",
        "monitoring_available": "continuous monitoring is unavailable",
        "protective_stop_workflow_ready": "protective-stop workflow is not ready",
        "encryption_ready": "identifier encryption check is not ready",
        "identifier_audit_ready": "identifier audit is not ready",
        "candidate_evaluation_eligible": "deterministic candidate evaluation did not pass",
        "broker_review_clear": "broker review is missing or has unresolved alerts",
        "confirmation_satisfied": "tool-required confirmation is not satisfied",
    }
    for field, message in required_true.items():
        if not _boolean(snapshot, field):
            blockers.append(message)
    if str(_required(snapshot, "evaluation_rules_hash")) != config.rules_hash:
        blockers.append("candidate evaluation used stale or different rules")
    if MATURITY_RANK[declared] > MATURITY_RANK[earned_maturity]:
        blockers.append("declared maturity exceeds evidence-earned maturity")
    if filled_entries_today:
        blockers.append("today's filled-entry limit is exhausted")
    if _number(snapshot, "rolling_five_session_drawdown_fraction") >= float(
        circuit_breakers["maximum_rolling_five_session_drawdown_fraction"]
    ):
        blockers.append("rolling five-session drawdown circuit breaker is active")
    if _number(snapshot, "strategy_drawdown_fraction") >= float(
        circuit_breakers["maximum_strategy_drawdown_fraction"]
    ):
        blockers.append("strategy peak-to-trough drawdown circuit breaker is active")
    if _integer(snapshot, "consecutive_losses") >= int(
        circuit_breakers["maximum_consecutive_losses"]
    ):
        blockers.append("three-consecutive-loss circuit breaker is active")
    if _boolean(snapshot, "manual_circuit_breaker_active"):
        blockers.append("manual or external circuit breaker is active")
    if stale_data:
        blockers.append("market data is stale")
    if stale_monitor:
        blockers.append("monitoring heartbeat is stale")
    if not entry_start <= now <= entry_cutoff:
        blockers.append("current time is outside the production entry window")

    if blockers:
        return _decision(
            "ENTRY_BLOCKED",
            snapshot,
            earned_maturity,
            config,
            blockers,
            (
                "resolve every blocker and re-run the guard from fresh broker and market state",
            ),
        )
    return _decision(
        "ENTRY_READY",
        snapshot,
        earned_maturity,
        config,
        (),
        ("place only the reviewed logical entry, then immediately re-run the guard",),
        entry_allowed=True,
    )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "snapshot", type=Path, help="JSON broker/session safety snapshot"
    )
    parser.add_argument("--ledger", type=Path, default=DEFAULT_LEDGER_PATH)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        payload = json.loads(args.snapshot.read_text(encoding="utf-8"))
        if not isinstance(payload, Mapping):
            raise GuardInputError("snapshot JSON must contain an object")
        config = load_config()
        earned = assess_maturity(read_records(args.ledger), config).earned_maturity
        result = evaluate_guard(payload, earned, config)
        print(json.dumps(result.to_dict(), indent=2, sort_keys=True))
        if result.must_flatten:
            return 3
        return 0 if result.entry_allowed or result.status == "MANAGE_POSITION" else 2
    except (
        OSError,
        json.JSONDecodeError,
        StrategyInputError,
        GuardInputError,
        LedgerError,
    ) as exc:
        print(
            json.dumps({"error": str(exc), "error_type": type(exc).__name__}, indent=2),
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

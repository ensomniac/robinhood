"""Fail closed before a controlled multi-strategy live-pilot entry.

This pure guard does not contact a broker or place an order. It composes an
exact portfolio maturity report with a fresh privacy-safe broker/risk snapshot.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from portfolio_maturity import PortfolioConfig, build_report, load_config


SCHEMA_VERSION = 1
MAXIMUM_SNAPSHOT_AGE_SECONDS = 15
ALLOWED_FIELDS = {
    "schema_version",
    "observed_at",
    "strategy_id",
    "strategy_version",
    "rules_hash",
    "broker_state",
    "account_reconciled",
    "orders_reconciled",
    "positions_count",
    "protected_positions_count",
    "unknown_orders_count",
    "unprotected_positions_count",
    "new_entries_today",
    "gross_notional_fraction",
    "aggregate_planned_open_loss_fraction",
    "daily_loss_fraction",
    "weekly_loss_fraction",
    "peak_to_trough_drawdown_fraction",
    "proposed_position_loss_fraction",
    "proposed_gross_notional_fraction",
    "proposed_holding_trading_days",
    "tradable",
    "broker_review_passed",
    "broker_confirmation_required",
    "broker_confirmation_satisfied",
    "protective_order_route_ready",
    "monitoring_ready",
    "source",
}


class PortfolioGuardError(ValueError):
    """The proposed entry snapshot is malformed or privacy-unsafe."""


def _string(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise PortfolioGuardError(f"{field} must be a non-empty string")
    return value.strip()


def _boolean(value: Any, field: str) -> bool:
    if not isinstance(value, bool):
        raise PortfolioGuardError(f"{field} must be boolean")
    return value


def _integer(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise PortfolioGuardError(f"{field} must be a nonnegative integer")
    return value


def _number(value: Any, field: str) -> float:
    if isinstance(value, bool):
        raise PortfolioGuardError(f"{field} must be a nonnegative number")
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise PortfolioGuardError(f"{field} must be a nonnegative number") from exc
    if not math.isfinite(result) or result < 0:
        raise PortfolioGuardError(f"{field} must be a nonnegative number")
    return result


def _timestamp(value: Any, field: str) -> datetime:
    normalized = _string(value, field)
    try:
        result = datetime.fromisoformat(normalized.replace("Z", "+00:00"))
    except ValueError as exc:
        raise PortfolioGuardError(f"{field} must be an ISO timestamp") from exc
    if result.tzinfo is None:
        raise PortfolioGuardError(f"{field} must include a timezone")
    return result.astimezone(UTC)


def validate_snapshot(value: Any) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise PortfolioGuardError("entry snapshot must be an object")
    snapshot = dict(value)
    extras = sorted(set(snapshot) - ALLOWED_FIELDS)
    if extras:
        raise PortfolioGuardError(f"entry snapshot contains forbidden fields: {extras}")
    if snapshot.get("schema_version") != SCHEMA_VERSION:
        raise PortfolioGuardError("entry snapshot schema_version must be 1")
    _timestamp(snapshot.get("observed_at"), "observed_at")
    for field in ("strategy_id", "strategy_version", "rules_hash", "source"):
        snapshot[field] = _string(snapshot.get(field), field)
    if snapshot["broker_state"] not in {
        "FLAT_RECONCILED",
        "PROTECTED_EXPOSURE_RECONCILED",
    }:
        raise PortfolioGuardError("broker_state must be safely reconciled")
    for field in (
        "account_reconciled",
        "orders_reconciled",
        "tradable",
        "broker_review_passed",
        "broker_confirmation_required",
        "broker_confirmation_satisfied",
        "protective_order_route_ready",
        "monitoring_ready",
    ):
        snapshot[field] = _boolean(snapshot.get(field), field)
    for field in (
        "positions_count",
        "protected_positions_count",
        "unknown_orders_count",
        "unprotected_positions_count",
        "new_entries_today",
        "proposed_holding_trading_days",
    ):
        snapshot[field] = _integer(snapshot.get(field), field)
    for field in (
        "gross_notional_fraction",
        "aggregate_planned_open_loss_fraction",
        "daily_loss_fraction",
        "weekly_loss_fraction",
        "peak_to_trough_drawdown_fraction",
        "proposed_position_loss_fraction",
        "proposed_gross_notional_fraction",
    ):
        snapshot[field] = _number(snapshot.get(field), field)
    return snapshot


def evaluate_entry(
    snapshot: Mapping[str, Any],
    report: Mapping[str, Any],
    config: PortfolioConfig,
    *,
    now: datetime | None = None,
) -> dict[str, Any]:
    value = validate_snapshot(snapshot)
    identities = [
        assessment
        for assessment in report.get("strategies", [])
        if assessment.get("strategy_id") == value["strategy_id"]
        and assessment.get("strategy_version") == value["strategy_version"]
    ]
    readiness_blockers: list[str] = []
    if len(identities) != 1:
        readiness_blockers.append(
            "strategy identity is absent or ambiguous in the maturity report"
        )
        assessment: Mapping[str, Any] | None = None
    else:
        assessment = identities[0]
        if assessment.get("rules_hash") != value["rules_hash"]:
            readiness_blockers.append("strategy rules hash does not match maturity")
        if assessment.get("pilot_ready") is not True:
            readiness_blockers.append("strategy has not earned PILOT_READY")
    safety_blockers: list[str] = []
    observed = _timestamp(value["observed_at"], "observed_at")
    age = ((now or datetime.now(UTC)).astimezone(UTC) - observed).total_seconds()
    if age < 0 or age > MAXIMUM_SNAPSHOT_AGE_SECONDS:
        safety_blockers.append("entry snapshot is stale")
    for field in ("account_reconciled", "orders_reconciled"):
        if value[field] is not True:
            safety_blockers.append(f"{field} is not true")
    if value["unknown_orders_count"]:
        safety_blockers.append("unknown orders are present")
    if value["unprotected_positions_count"]:
        safety_blockers.append("unprotected positions are present")
    if value["protected_positions_count"] != value["positions_count"]:
        safety_blockers.append("existing positions are not all protected")
    portfolio = config.raw["portfolio"]
    risk = config.raw["pilot_risk"]
    if value["positions_count"] + 1 > int(portfolio["maximum_concurrent_positions"]):
        safety_blockers.append("post-entry position count exceeds the portfolio cap")
    if value["new_entries_today"] + 1 > int(portfolio["maximum_new_entries_per_day"]):
        safety_blockers.append("post-entry daily entry count exceeds the portfolio cap")
    if value["proposed_holding_trading_days"] < 1 or value[
        "proposed_holding_trading_days"
    ] > int(portfolio["maximum_holding_trading_days"]):
        safety_blockers.append("proposed holding period exceeds the portfolio cap")
    if value["proposed_position_loss_fraction"] > float(
        risk["maximum_planned_loss_fraction_per_position"]
    ):
        safety_blockers.append("proposed position loss exceeds the pilot cap")
    post_entry_limits = {
        "aggregate planned open loss": (
            value["aggregate_planned_open_loss_fraction"]
            + value["proposed_position_loss_fraction"],
            risk["maximum_aggregate_planned_open_loss_fraction"],
        ),
        "gross notional": (
            value["gross_notional_fraction"]
            + value["proposed_gross_notional_fraction"],
            risk["maximum_gross_notional_fraction"],
        ),
        "daily loss": (
            value["daily_loss_fraction"] + value["proposed_position_loss_fraction"],
            risk["maximum_daily_loss_fraction"],
        ),
        "weekly loss": (
            value["weekly_loss_fraction"] + value["proposed_position_loss_fraction"],
            risk["maximum_weekly_loss_fraction"],
        ),
        "peak-to-trough drawdown": (
            value["peak_to_trough_drawdown_fraction"]
            + value["proposed_position_loss_fraction"],
            risk["maximum_peak_to_trough_drawdown_fraction"],
        ),
    }
    for name, (projected, maximum) in post_entry_limits.items():
        if projected > float(maximum):
            safety_blockers.append(f"post-entry {name} exceeds the pilot cap")
    for field, message in (
        ("tradable", "security is not currently tradable"),
        ("protective_order_route_ready", "protective-order route is not ready"),
        ("monitoring_ready", "monitoring is not ready"),
    ):
        if value[field] is not True:
            safety_blockers.append(message)
    review_blockers: list[str] = []
    if value["broker_review_passed"] is not True:
        review_blockers.append("broker order review has not passed")
    confirmation_blockers: list[str] = []
    if value["broker_confirmation_required"] and not value[
        "broker_confirmation_satisfied"
    ]:
        confirmation_blockers.append("broker-required user confirmation is pending")
    if readiness_blockers:
        status = "NOT_PILOT_READY"
    elif safety_blockers:
        status = "PAUSED_SAFETY"
    elif review_blockers:
        status = "WAITING_REVIEW"
    elif confirmation_blockers:
        status = "WAITING_USER_CONFIRMATION"
    else:
        status = "ENTRY_READY"
    blockers = [
        *readiness_blockers,
        *safety_blockers,
        *review_blockers,
        *confirmation_blockers,
    ]
    return {
        "schema_version": SCHEMA_VERSION,
        "status": status,
        "entry_allowed": status == "ENTRY_READY",
        "strategy_id": value["strategy_id"],
        "strategy_version": value["strategy_version"],
        "rules_hash": value["rules_hash"],
        "maturity": assessment.get("maturity") if assessment else None,
        "blockers": blockers,
        "post_entry": {
            "positions_count": value["positions_count"] + 1,
            "new_entries_today": value["new_entries_today"] + 1,
            "aggregate_planned_open_loss_fraction": (
                value["aggregate_planned_open_loss_fraction"]
                + value["proposed_position_loss_fraction"]
            ),
            "gross_notional_fraction": (
                value["gross_notional_fraction"]
                + value["proposed_gross_notional_fraction"]
            ),
        },
    }


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise PortfolioGuardError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise PortfolioGuardError(f"{path} must contain an object")
    return value


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("snapshot", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        result = evaluate_entry(_read_json(args.snapshot), build_report(), load_config())
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0 if result["entry_allowed"] else 2
    except (OSError, PortfolioGuardError, ValueError) as exc:
        print(
            json.dumps({"error": str(exc), "error_type": type(exc).__name__}, indent=2),
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

"""Pure production evaluation and unknown-submission reconciliation for v2 pilots.

This module never contacts a broker or submits/cancels an order. It converts an
exact frozen winner plus fresh privacy-safe market/account facts into a bounded
order and protection plan that still requires portfolio_guard and broker review.
"""

from __future__ import annotations

import math
import hashlib
import importlib
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from portfolio_maturity import PortfolioConfig


MAXIMUM_MARKET_DATA_AGE_SECONDS = 5
MAXIMUM_SPREAD_FRACTION = 0.0015
MAXIMUM_DEPTH_FRACTION = 0.05
MAXIMUM_RECENT_VOLUME_FRACTION = 0.05
PRIMARY_ROUND_TRIP_COST_FRACTION = 0.0010
MINIMUM_GROSS_MOVE_TO_COST_MULTIPLE = 5.0
PROJECT_ROOT = Path(__file__).resolve().parent


class PortfolioExecutionError(ValueError):
    """Production evaluation facts are stale, incomplete, or unsafe."""


def _file_hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def evaluate_frozen_winner(
    winner: Mapping[str, Any],
    market_facts: Mapping[str, Any],
    account: Mapping[str, Any],
    config: PortfolioConfig,
    *,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Build and validate a live candidate through the frozen strategy plugin."""
    implementation_hashes = winner.get("implementation_hashes")
    if not isinstance(implementation_hashes, Mapping) or not implementation_hashes:
        raise PortfolioExecutionError("winner implementation hashes are missing")
    for relative, expected in implementation_hashes.items():
        path = (PROJECT_ROOT / str(relative)).resolve()
        try:
            path.relative_to(PROJECT_ROOT.resolve())
        except ValueError as exc:
            raise PortfolioExecutionError("winner implementation path is unsafe") from exc
        if not path.is_file() or _file_hash(path) != expected:
            raise PortfolioExecutionError("winner implementation drifted")
    plugin = winner.get("plugin")
    if not isinstance(plugin, Mapping):
        raise PortfolioExecutionError("winner plugin is missing")
    try:
        module = importlib.import_module(str(plugin["module"]))
        evaluator = getattr(module, str(plugin["evaluate_production"]))
    except (ImportError, AttributeError, KeyError) as exc:
        raise PortfolioExecutionError("winner production plugin cannot be loaded") from exc
    if not callable(evaluator):
        raise PortfolioExecutionError("winner production plugin is not callable")
    candidate = evaluator(winner, market_facts)
    if not isinstance(candidate, Mapping):
        raise PortfolioExecutionError("winner production plugin returned invalid facts")
    return evaluate_production_candidate(winner, candidate, account, config, now=now)


def _number(value: Any, field: str, *, positive: bool = False) -> float:
    if isinstance(value, bool):
        raise PortfolioExecutionError(f"{field} must be numeric")
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise PortfolioExecutionError(f"{field} must be numeric") from exc
    if not math.isfinite(result) or (positive and result <= 0):
        raise PortfolioExecutionError(f"{field} is invalid")
    return result


def _integer(value: Any, field: str, *, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise PortfolioExecutionError(f"{field} must be an integer >= {minimum}")
    return value


def _boolean(value: Any, field: str) -> bool:
    if not isinstance(value, bool):
        raise PortfolioExecutionError(f"{field} must be boolean")
    return value


def _timestamp(value: Any, field: str) -> datetime:
    if not isinstance(value, str):
        raise PortfolioExecutionError(f"{field} must be an ISO timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise PortfolioExecutionError(f"{field} must be an ISO timestamp") from exc
    if parsed.tzinfo is None:
        raise PortfolioExecutionError(f"{field} must include a timezone")
    return parsed.astimezone(UTC)


def evaluate_production_candidate(
    winner: Mapping[str, Any],
    candidate: Mapping[str, Any],
    account: Mapping[str, Any],
    config: PortfolioConfig,
    *,
    now: datetime | None = None,
) -> dict[str, Any]:
    current = (now or datetime.now(UTC)).astimezone(UTC)
    for field in ("strategy_id", "strategy_version", "rules_hash"):
        if candidate.get(field) != winner.get(field):
            raise PortfolioExecutionError(f"candidate {field} drifted from winner")
    if candidate.get("historical_semantics_sha256") != winner.get("rules_hash"):
        raise PortfolioExecutionError("live discovery semantics drifted from history")
    if _boolean(candidate.get("ranking_complete"), "ranking_complete") is not True:
        raise PortfolioExecutionError("live universe ranking is incomplete")
    observed = _timestamp(candidate.get("observed_at"), "observed_at")
    age = (current - observed).total_seconds()
    if age < 0 or age > MAXIMUM_MARKET_DATA_AGE_SECONDS:
        raise PortfolioExecutionError("market data is stale")
    if _boolean(candidate.get("halted"), "halted"):
        raise PortfolioExecutionError("security is halted")
    if _boolean(candidate.get("tradable"), "tradable") is not True:
        raise PortfolioExecutionError("security is not tradable")
    bid = _number(candidate.get("bid"), "bid", positive=True)
    ask = _number(candidate.get("ask"), "ask", positive=True)
    if bid > ask:
        raise PortfolioExecutionError("quote is crossed")
    spread = (ask - bid) / ((ask + bid) / 2)
    if spread > MAXIMUM_SPREAD_FRACTION:
        raise PortfolioExecutionError("spread exceeds the production ceiling")
    entry = _number(candidate.get("entry_limit"), "entry_limit", positive=True)
    stop = _number(candidate.get("stop_price"), "stop_price", positive=True)
    if stop >= bid or entry < ask:
        raise PortfolioExecutionError(
            "entry is not marketable or the stop is not below the observed bid"
        )
    expected_move = _number(
        candidate.get("expected_gross_move_fraction"),
        "expected_gross_move_fraction",
        positive=True,
    )
    if expected_move < (
        PRIMARY_ROUND_TRIP_COST_FRACTION * MINIMUM_GROSS_MOVE_TO_COST_MULTIPLE
    ):
        raise PortfolioExecutionError("expected gross move does not clear the cost floor")
    hold = _integer(
        candidate.get("holding_trading_days"),
        "holding_trading_days",
        minimum=1,
    )
    if hold > int(config.raw["portfolio"]["maximum_holding_trading_days"]):
        raise PortfolioExecutionError("holding period exceeds the portfolio cap")
    for field in (
        "before_open_account_reconciled",
        "before_open_orders_reconciled",
        "before_open_protection_reconciled",
        "before_open_tradability_reconciled",
        "before_open_news_reconciled",
        "protective_order_route_ready",
        "monitoring_ready",
    ):
        if _boolean(candidate.get(field), field) is not True:
            raise PortfolioExecutionError(f"{field} is not true")
    if hold > 1 and candidate.get("protection_time_in_force") != "gtc":
        raise PortfolioExecutionError("multi-session protection must be GTC")
    safe_cutoff = candidate.get("protection_failure_safe_cutoff")
    if not isinstance(safe_cutoff, str) or not safe_cutoff:
        raise PortfolioExecutionError("protection failure safe cutoff is missing")
    equity = _number(account.get("equity"), "equity", positive=True)
    buying_power = _number(
        account.get("buying_power"), "buying_power", positive=True
    )
    stop_distance = entry - stop
    risk_limit = float(
        config.raw["pilot_risk"]["maximum_planned_loss_fraction_per_position"]
    )
    risk_quantity = math.floor(equity * risk_limit / stop_distance)
    gross_quantity = math.floor(
        equity
        * float(config.raw["pilot_risk"]["maximum_gross_notional_fraction"])
        / entry
    )
    buying_power_quantity = math.floor(buying_power / entry)
    depth_quantity = math.floor(
        _number(candidate.get("executable_ask_depth"), "executable_ask_depth")
        * MAXIMUM_DEPTH_FRACTION
    )
    volume_quantity = math.floor(
        _number(candidate.get("recent_real_minute_volume"), "recent_real_minute_volume")
        * MAXIMUM_RECENT_VOLUME_FRACTION
    )
    quantity = min(
        risk_quantity,
        gross_quantity,
        buying_power_quantity,
        depth_quantity,
        volume_quantity,
    )
    if quantity < 1:
        raise PortfolioExecutionError("whole-share sizing produced zero quantity")
    planned_loss_fraction = quantity * stop_distance / equity
    gross_notional_fraction = quantity * entry / equity
    return {
        "status": "PRODUCTION_EVALUATION_READY",
        "strategy_id": winner["strategy_id"],
        "strategy_version": winner["strategy_version"],
        "rules_hash": winner["rules_hash"],
        "symbol": candidate.get("symbol"),
        "order": {
            "side": "buy",
            "type": "limit",
            "limit_price": entry,
            "quantity": quantity,
            "whole_shares": True,
        },
        "protection": {
            "stop_price": stop,
            "time_in_force": candidate.get("protection_time_in_force"),
            "route_ready": True,
            "failure_safe_cutoff": safe_cutoff,
        },
        "risk": {
            "planned_loss_fraction": planned_loss_fraction,
            "gross_notional_fraction": gross_notional_fraction,
            "holding_trading_days": hold,
        },
        "market": {
            "observed_at": observed.isoformat(),
            "age_seconds": age,
            "spread_fraction": spread,
            "expected_gross_move_fraction": expected_move,
        },
        "next_gate": "portfolio_guard_then_broker_review",
        "broker_actions_performed": 0,
    }


def reconcile_unknown_submission(
    logical_order_alias: str,
    observed_orders: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Decide whether an ambiguous submission can be retried without duplication."""
    if not isinstance(logical_order_alias, str) or not logical_order_alias:
        raise PortfolioExecutionError("logical_order_alias must be non-empty")
    matches = [
        order
        for order in observed_orders
        if order.get("logical_order_alias") == logical_order_alias
    ]
    if len(matches) > 1:
        return {
            "status": "DUPLICATE_OR_AMBIGUOUS_PAUSE",
            "retry_permitted": False,
            "matched_orders": len(matches),
        }
    if not matches:
        return {
            "status": "CONFIRMED_ABSENT_RETRY_SAME_LOGICAL_ORDER",
            "retry_permitted": True,
            "matched_orders": 0,
        }
    state = matches[0].get("state")
    if state in {"rejected", "cancelled", "failed"}:
        status = "TERMINAL_NO_EXPOSURE_RETRY_SAME_LOGICAL_ORDER"
        retry = True
    elif state in {"filled", "partially_filled", "open", "queued", "pending"}:
        status = "FOUND_DO_NOT_RETRY_RECONCILE_EXPOSURE"
        retry = False
    else:
        status = "UNKNOWN_BROKER_STATE_PAUSE"
        retry = False
    return {
        "status": status,
        "retry_permitted": retry,
        "matched_orders": 1,
        "broker_state": state,
    }

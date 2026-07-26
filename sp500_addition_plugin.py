"""Discovery plugin for the exact S&P 500 addition forced-demand family."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime, time, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import dense_strategy_plugin as dense
import dense_strategy_runtime as runtime
from historical_store import canonical_sha256


MARKET_TIME_ZONE = ZoneInfo("America/New_York")


class Sp500AdditionPluginError(RuntimeError):
    """The S&P addition historical or production binding drifted."""


def preflight(contract: Mapping[str, Any]) -> dict[str, Any]:
    return dense.preflight(contract)


def evaluate_development(
    contract: Mapping[str, Any],
    trials: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    return dense.evaluate_development(contract, trials)


def evaluate_confirmation(
    winner: Mapping[str, Any],
) -> dict[str, Any]:
    return dense.evaluate_confirmation(winner)


def _parameters(value: Mapping[str, Any]) -> dict[str, Any]:
    normalized = {
        "exit_mode": str(value["exit_mode"]),
        "maximum_hold_sessions": int(value["maximum_hold_sessions"]),
        "maximum_positive_announcement_gap_fraction": float(
            value["maximum_positive_announcement_gap_fraction"]
        ),
        "minimum_sessions_to_effective": int(
            value["minimum_sessions_to_effective"]
        ),
        "stop_buffer_below_reference_close": float(
            value["stop_buffer_below_reference_close"]
        ),
    }
    if (
        normalized["exit_mode"]
        not in {"maximum_hold", "pre_effective_or_maximum_hold"}
        or normalized["maximum_hold_sessions"] not in {2, 5}
        or normalized[
            "maximum_positive_announcement_gap_fraction"
        ]
        not in {0.02, 0.04}
        or normalized["minimum_sessions_to_effective"] not in {2, 4}
        or normalized["stop_buffer_below_reference_close"]
        not in {0.0, 0.01}
    ):
        raise Sp500AdditionPluginError(
            "live parameters escaped the frozen grid"
        )
    return normalized


def _events(value: Any, decision_date: str, next_session: str) -> list[dict[str, Any]]:
    if not isinstance(value, list) or not value:
        raise Sp500AdditionPluginError(
            "live official event set is empty"
        )
    rows: list[dict[str, Any]] = []
    for raw in value:
        if not isinstance(raw, Mapping) or set(raw) != {
            "action",
            "announcement_at",
            "announcement_date",
            "company_name",
            "effective_date",
            "entry_date",
            "event_id",
            "holding_dates",
            "index_name",
            "pre_effective_date",
            "reference_date",
            "sessions_to_effective",
            "source_url",
            "ticker",
        }:
            raise Sp500AdditionPluginError(
                "live official event schema drifted"
            )
        row = dict(raw)
        event_id = row.pop("event_id")
        if (
            event_id != canonical_sha256(row)
            or row["action"] != "Addition"
            or row["index_name"] != "S&P 500"
            or row["announcement_date"] != decision_date
            or row["entry_date"] != next_session
            or not isinstance(row["sessions_to_effective"], int)
            or isinstance(row["sessions_to_effective"], bool)
            or row["sessions_to_effective"] < 1
        ):
            raise Sp500AdditionPluginError(
                "live official event identity or timing drifted"
            )
        row["event_id"] = event_id
        rows.append(row)
    ranked = sorted(
        rows,
        key=lambda row: (
            -int(row["sessions_to_effective"]),
            str(row["ticker"]),
            str(row["event_id"]),
        ),
    )
    if rows != ranked:
        raise Sp500AdditionPluginError(
            "live official event ranks drifted"
        )
    return rows


def evaluate_production(
    winner: Mapping[str, Any],
    market_facts: Mapping[str, Any],
) -> dict[str, Any]:
    """Select one next-open event through the frozen historical semantics."""

    if market_facts.get("selected_trial_id") != winner["exact_rules"][
        "selected_trial_id"
    ]:
        raise Sp500AdditionPluginError(
            "live selected trial drifted"
        )
    if market_facts.get("parameters") != winner["exact_rules"][
        "parameters"
    ]:
        raise Sp500AdditionPluginError(
            "live exact parameters drifted"
        )
    if set(market_facts) != {
        "selected_trial_id",
        "parameters",
        "decision_data",
        "quotes",
        "operational",
    }:
        raise Sp500AdditionPluginError(
            "live market-fact schema drifted"
        )
    parameters = _parameters(market_facts["parameters"])
    decision = market_facts["decision_data"]
    if not isinstance(decision, Mapping) or set(decision) != {
        "decision_date",
        "events",
        "family_id",
        "next_session_date",
        "reference_closes",
    }:
        raise Sp500AdditionPluginError(
            "live decision-data schema drifted"
        )
    decision_date = str(decision["decision_date"])
    next_session = str(decision["next_session_date"])
    if decision["family_id"] != runtime.SP500_ADDITION_FORCED_DEMAND_FAMILY:
        raise Sp500AdditionPluginError(
            "live family binding drifted"
        )
    events = _events(decision["events"], decision_date, next_session)
    references = decision["reference_closes"]
    if not isinstance(references, Mapping) or set(references) != {
        str(row["ticker"]) for row in events
    }:
        raise Sp500AdditionPluginError(
            "live reference closes are incomplete"
        )
    quotes = market_facts["quotes"]
    if not isinstance(quotes, list) or not quotes:
        raise Sp500AdditionPluginError("live quotes are missing")
    by_symbol: dict[str, dict[str, Any]] = {}
    quote_fields = {
        "ask",
        "bid",
        "depth_gate_passed",
        "executable_ask_depth",
        "halted",
        "news_gate_passed",
        "observed_at",
        "recent_real_minute_volume",
        "spread_gate_passed",
        "symbol",
        "tradable",
    }
    market_open = datetime.combine(
        datetime.fromisoformat(next_session).date(),
        time(9, 30),
        tzinfo=MARKET_TIME_ZONE,
    )
    for raw in quotes:
        if not isinstance(raw, Mapping) or set(raw) != quote_fields:
            raise Sp500AdditionPluginError(
                "live quote schema drifted"
            )
        quote = dict(raw)
        symbol = str(quote["symbol"])
        if symbol in by_symbol:
            raise Sp500AdditionPluginError(
                "live quote symbol is duplicated"
            )
        try:
            observed = datetime.fromisoformat(
                str(quote["observed_at"])
            )
            bid = float(quote["bid"])
            ask = float(quote["ask"])
        except (TypeError, ValueError) as exc:
            raise Sp500AdditionPluginError(
                "live quote values are invalid"
            ) from exc
        if (
            observed.tzinfo is None
            or observed.astimezone(MARKET_TIME_ZONE) < market_open
            or observed.astimezone(MARKET_TIME_ZONE)
            >= market_open + timedelta(minutes=1)
            or bid <= 0
            or ask < bid
        ):
            raise Sp500AdditionPluginError(
                "live quote is not a valid next-open observation"
            )
        by_symbol[symbol] = quote
    if set(by_symbol) != {str(row["ticker"]) for row in events}:
        raise Sp500AdditionPluginError(
            "live quotes do not cover every ranked official event"
        )
    selected: tuple[dict[str, Any], dict[str, Any], float, float, float] | None = None
    maximum_gap = parameters[
        "maximum_positive_announcement_gap_fraction"
    ]
    stop_buffer = parameters[
        "stop_buffer_below_reference_close"
    ]
    for event in events:
        symbol = str(event["ticker"])
        quote = by_symbol[symbol]
        reference = float(references[symbol])
        ask = float(quote["ask"])
        if (
            reference <= 0
            or int(event["sessions_to_effective"])
            < parameters["minimum_sessions_to_effective"]
            or quote["halted"] is not False
            or quote["tradable"] is not True
            or quote["spread_gate_passed"] is not True
            or quote["depth_gate_passed"] is not True
            or quote["news_gate_passed"] is not True
        ):
            continue
        entry_gap = ask / reference - 1
        expected_gross = maximum_gap - max(entry_gap, 0.0)
        stop = reference * (1 - stop_buffer)
        if (
            entry_gap > maximum_gap + 1e-12
            or not runtime._cost_floor(expected_gross)
            or stop <= 0
            or stop >= ask
        ):
            continue
        selected = (event, quote, stop, entry_gap, expected_gross)
        break
    if selected is None:
        raise Sp500AdditionPluginError(
            "no exact live S&P addition signal"
        )
    event, quote, stop, entry_gap, expected_gross = selected
    operational = market_facts["operational"]
    operational_fields = {
        "before_open_account_reconciled",
        "before_open_news_reconciled",
        "before_open_orders_reconciled",
        "before_open_protection_reconciled",
        "before_open_tradability_reconciled",
        "monitoring_ready",
        "protection_failure_safe_cutoff",
        "protective_order_route_ready",
    }
    if not isinstance(operational, Mapping) or set(
        operational
    ) != operational_fields:
        raise Sp500AdditionPluginError(
            "live operational fact schema drifted"
        )
    if any(
        operational[field] is not True
        for field in operational_fields
        - {"protection_failure_safe_cutoff"}
    ):
        raise Sp500AdditionPluginError(
            "live operational readiness is incomplete"
        )
    hold = parameters["maximum_hold_sessions"]
    exit_type = (
        "stop_or_pre_effective_or_maximum_hold_close"
        if parameters["exit_mode"]
        == "pre_effective_or_maximum_hold"
        else "stop_or_maximum_hold_close"
    )
    return {
        "strategy_id": winner["strategy_id"],
        "strategy_version": winner["strategy_version"],
        "rules_hash": winner["rules_hash"],
        "historical_semantics_sha256": winner["rules_hash"],
        "ranking_complete": True,
        "observed_at": quote["observed_at"],
        "symbol": event["ticker"],
        "event_id": event["event_id"],
        "rank": events.index(event) + 1,
        "score": event["sessions_to_effective"],
        "halted": quote["halted"],
        "tradable": quote["tradable"],
        "bid": quote["bid"],
        "ask": quote["ask"],
        "entry_limit": quote["ask"],
        "stop_price": stop,
        "entry_gap_fraction": entry_gap,
        "expected_gross_move_fraction": expected_gross,
        "holding_trading_days": hold,
        **dict(operational),
        "protection_time_in_force": "gtc",
        "executable_ask_depth": quote["executable_ask_depth"],
        "recent_real_minute_volume": quote[
            "recent_real_minute_volume"
        ],
        "exit_plan": {
            "type": exit_type,
            "maximum_hold_sessions": hold,
            "pre_effective_date": event["pre_effective_date"],
            "same_interval_ambiguity": "stop_first",
        },
    }

"""Causal stop-first simulator shared by every declarative strategy."""

from __future__ import annotations

import math
from collections import defaultdict
from dataclasses import dataclass
from datetime import date
from typing import Any, Sequence

from .config import LabConfig
from .contracts import (
    ALLOWED_FEATURES,
    BOOLEAN_OPERATORS,
    COMPARISON_OPERATORS,
    ContractError,
    StrategySpec,
)
from .database import LabDatabase
from .hashing import canonical_sha256
from .statistics import (
    bootstrap_lower_mean,
    deflated_sharpe_probability,
    maximum_drawdown_r,
    one_sided_mean_pvalue,
    one_sided_wilson_lower,
    positive_halves,
    positive_without_best_five,
    profit_factor,
)


class BacktestError(RuntimeError):
    """Raised when a strategy cannot be evaluated causally."""


SQL_OPERATORS = {"gt": ">", "gte": ">=", "lt": "<", "lte": "<="}


def _operand_sql(value: Any) -> tuple[str, list[float]]:
    if not isinstance(value, dict) or len(value) != 1:
        raise ContractError("invalid signal operand")
    if "feature" in value:
        feature = str(value["feature"])
        if feature not in ALLOWED_FEATURES:
            raise ContractError(f"unsupported feature {feature!r}")
        return f'"{feature}"', []
    if "value" in value:
        number = float(value["value"])
        if not math.isfinite(number):
            raise ContractError("signal constants must be finite")
        return "?", [number]
    raise ContractError("signal operand must contain feature or value")


def compile_signal_sql(node: dict[str, Any]) -> tuple[str, list[float]]:
    op = node.get("op")
    if op in BOOLEAN_OPERATORS:
        fragments: list[str] = []
        parameters: list[float] = []
        for child in node["args"]:
            fragment, child_parameters = compile_signal_sql(child)
            fragments.append(f"({fragment})")
            parameters.extend(child_parameters)
        conjunction = " AND " if op == "all" else " OR "
        return conjunction.join(fragments), parameters
    if op in COMPARISON_OPERATORS:
        left, left_parameters = _operand_sql(node["left"])
        right, right_parameters = _operand_sql(node["right"])
        return (
            f"{left} {SQL_OPERATORS[str(op)]} {right}",
            [*left_parameters, *right_parameters],
        )
    raise ContractError(f"unsupported signal operator {op!r}")


@dataclass(frozen=True)
class SimulatedTrade:
    signal_date: date
    entry_date: date
    exit_date: date
    symbol: str
    entry_price: float
    exit_price: float
    exit_reason: str
    gross_return: float
    net_return: float
    stressed_net_return: float
    return_r: float
    stressed_return_r: float
    notional_fraction: float
    rank_value: float | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "signal_date": self.signal_date.isoformat(),
            "entry_date": self.entry_date.isoformat(),
            "exit_date": self.exit_date.isoformat(),
            "symbol": self.symbol,
            "entry_price": self.entry_price,
            "exit_price": self.exit_price,
            "exit_reason": self.exit_reason,
            "gross_return": self.gross_return,
            "net_return": self.net_return,
            "stressed_net_return": self.stressed_net_return,
            "return_r": self.return_r,
            "stressed_return_r": self.stressed_return_r,
            "notional_fraction": self.notional_fraction,
            "rank_value": self.rank_value,
        }


@dataclass(frozen=True)
class BacktestEvaluation:
    strategy_id: str
    rules_sha256: str
    family_id: str
    phase: str
    metrics: dict[str, Any]
    trades: tuple[SimulatedTrade, ...]

    @property
    def selection_row(self) -> dict[str, Any]:
        return {
            "strategy_id": self.strategy_id,
            "pvalue": self.metrics["pvalue"],
            "stressed_log_growth": self.metrics["stressed_total_log_growth"],
            "fold_log_growth": self.metrics["fold_log_growth"],
            "parameters": self.metrics.get("parameters", {}),
        }


FEATURE_COLUMNS = [
    "symbol",
    "session_date",
    "rank_value",
    "intraday_entry_price",
    "intraday_future_high",
    "intraday_future_low",
    "intraday_exit_price",
    "future_date_1",
    "future_open_1",
    "future_high_1",
    "future_low_1",
    "future_close_1",
    "future_date_2",
    "future_open_2",
    "future_high_2",
    "future_low_2",
    "future_close_2",
    "future_date_3",
    "future_open_3",
    "future_high_3",
    "future_low_3",
    "future_close_3",
    "future_date_4",
    "future_open_4",
    "future_high_4",
    "future_low_4",
    "future_close_4",
    "future_date_5",
    "future_open_5",
    "future_high_5",
    "future_low_5",
    "future_close_5",
]


class BacktestEngine:
    def __init__(self, config: LabConfig, database: LabDatabase) -> None:
        self.config = config
        self.database = database

    def _candidate_rows(self, spec: StrategySpec, phase: str) -> list[dict[str, Any]]:
        if phase not in {"development", "holdout"}:
            raise BacktestError("phase must be development or holdout")
        signal_sql, parameters = compile_signal_sql(spec.signal)
        rank_direction = "ASC" if spec.rank_direction == "asc" else "DESC"
        maximum_entries = int(
            self.config.section("execution")["maximum_new_entries_per_day"]
        )
        horizon_guard = (
            "intraday_complete AND intraday_entry_price IS NOT NULL"
            if spec.horizon == "intraday"
            else "future_open_1 IS NOT NULL"
        )
        query = f"""
            SELECT *
            FROM (
                SELECT
                    symbol,
                    session_date,
                    "{spec.rank_by}" AS rank_value,
                    intraday_entry_price,
                    intraday_future_high,
                    intraday_future_low,
                    intraday_exit_price,
                    future_date_1,
                    future_open_1,
                    future_high_1,
                    future_low_1,
                    future_close_1,
                    future_date_2,
                    future_open_2,
                    future_high_2,
                    future_low_2,
                    future_close_2,
                    future_date_3,
                    future_open_3,
                    future_high_3,
                    future_low_3,
                    future_close_3,
                    future_date_4,
                    future_open_4,
                    future_high_4,
                    future_low_4,
                    future_close_4,
                    future_date_5,
                    future_open_5,
                    future_high_5,
                    future_low_5,
                    future_close_5,
                    row_number() OVER (
                        PARTITION BY session_date
                        ORDER BY "{spec.rank_by}" {rank_direction} NULLS LAST, symbol
                    ) AS daily_rank
                FROM features
                WHERE data_partition = ?
                  AND eligible
                  AND {horizon_guard}
                  AND ({signal_sql})
            )
            WHERE daily_rank <= ?
            ORDER BY session_date, daily_rank, symbol
        """
        rows = self.database.connection.execute(
            query, [phase, *parameters, maximum_entries]
        ).fetchall()
        columns = [
            description[0] for description in self.database.connection.description
        ]
        return [dict(zip(columns, row, strict=True)) for row in rows]

    def _raw_trade(
        self,
        spec: StrategySpec,
        row: dict[str, Any],
        *,
        stress_bps: int,
    ) -> dict[str, Any] | None:
        if spec.horizon == "intraday":
            entry = row["intraday_entry_price"]
            high = row["intraday_future_high"]
            low = row["intraday_future_low"]
            close = row["intraday_exit_price"]
            if None in {entry, high, low, close}:
                return None
            entry_price = float(entry)
            exit_date = row["session_date"]
            stop = entry_price * (1 - spec.stop_loss_pct)
            target = entry_price * (1 + spec.target_pct)
            if float(low) <= stop:
                exit_price, reason = stop, "stop"
            elif float(high) >= target:
                exit_price, reason = target, "target"
            else:
                exit_price, reason = float(close), "session_close"
            entry_date = row["session_date"]
        else:
            entry = row["future_open_1"]
            entry_date = row["future_date_1"]
            if entry is None or entry_date is None:
                return None
            entry_price = float(entry)
            stop = entry_price * (1 - spec.stop_loss_pct)
            target = entry_price * (1 + spec.target_pct)
            exit_price = None
            exit_date = None
            reason = "maximum_hold"
            for offset in range(1, spec.maximum_hold_sessions + 1):
                current_open = row.get(f"future_open_{offset}")
                high = row.get(f"future_high_{offset}")
                low = row.get(f"future_low_{offset}")
                close = row.get(f"future_close_{offset}")
                current_date = row.get(f"future_date_{offset}")
                if None in {current_open, high, low, close, current_date}:
                    return None
                if offset > 1 and float(current_open) <= stop:
                    exit_price, exit_date, reason = (
                        float(current_open),
                        current_date,
                        "gap_stop",
                    )
                    break
                if offset > 1 and float(current_open) >= target:
                    exit_price, exit_date, reason = (
                        float(current_open),
                        current_date,
                        "gap_target",
                    )
                    break
                if float(low) <= stop:
                    exit_price, exit_date, reason = stop, current_date, "stop"
                    break
                if float(high) >= target:
                    exit_price, exit_date, reason = target, current_date, "target"
                    break
                exit_price, exit_date = float(close), current_date
            if exit_price is None or exit_date is None:
                return None
        gross = float(exit_price) / entry_price - 1
        base_cost = spec.round_trip_bps / 10_000
        stress_cost = max(spec.round_trip_bps, stress_bps) / 10_000
        return {
            "signal_date": row["session_date"],
            "entry_date": entry_date,
            "exit_date": exit_date,
            "symbol": str(row["symbol"]),
            "entry_price": entry_price,
            "exit_price": float(exit_price),
            "exit_reason": reason,
            "gross_return": gross,
            "net_return": gross - base_cost,
            "stressed_net_return": gross - stress_cost,
            "rank_value": float(row["rank_value"])
            if row["rank_value"] is not None
            else None,
        }

    def _allocate(
        self, spec: StrategySpec, raw: Sequence[dict[str, Any]]
    ) -> tuple[SimulatedTrade, ...]:
        risk = self.config.section("pilot_risk")
        execution = self.config.section("execution")
        maximum_positions = int(execution["maximum_concurrent_positions"])
        maximum_entries = int(execution["maximum_new_entries_per_day"])
        maximum_gross = float(risk["maximum_gross_notional_fraction"])
        risk_per_position = float(risk["maximum_planned_loss_fraction_per_position"])
        maximum_aggregate_risk = float(
            risk["maximum_aggregate_planned_open_loss_fraction"]
        )
        desired_notional = min(maximum_gross, risk_per_position / spec.stop_loss_pct)
        open_positions: list[tuple[date, float, float]] = []
        daily_entries: dict[date, int] = defaultdict(int)
        result: list[SimulatedTrade] = []

        def allocation_key(value: dict[str, Any]) -> tuple[Any, float, str]:
            rank = (
                float(value["rank_value"]) if value["rank_value"] is not None else 0.0
            )
            return (
                value["entry_date"],
                -rank if spec.rank_direction == "desc" else rank,
                value["symbol"],
            )

        for item in sorted(raw, key=allocation_key):
            entry_date = item["entry_date"]
            open_positions = [
                position for position in open_positions if position[0] >= entry_date
            ]
            if (
                len(open_positions) >= maximum_positions
                or daily_entries[entry_date] >= maximum_entries
            ):
                continue
            gross_open = sum(position[1] for position in open_positions)
            planned_open_loss = sum(position[2] for position in open_positions)
            remaining_risk_notional = max(
                0.0,
                (maximum_aggregate_risk - planned_open_loss) / spec.stop_loss_pct,
            )
            notional = min(
                desired_notional,
                maximum_gross - gross_open,
                remaining_risk_notional,
            )
            if notional <= 0:
                continue
            daily_entries[entry_date] += 1
            open_positions.append(
                (item["exit_date"], notional, notional * spec.stop_loss_pct)
            )
            result.append(
                SimulatedTrade(
                    **item,
                    return_r=item["net_return"] / spec.stop_loss_pct,
                    stressed_return_r=item["stressed_net_return"] / spec.stop_loss_pct,
                    notional_fraction=notional,
                )
            )
        return tuple(sorted(result, key=lambda trade: (trade.exit_date, trade.symbol)))

    def _fold_growth(self, trades: Sequence[SimulatedTrade], phase: str) -> list[float]:
        folds = int(self.config.section("validation")["walk_forward_folds"])
        dates = [
            row[0]
            for row in self.database.connection.execute(
                "SELECT DISTINCT session_date FROM features WHERE data_partition = ? ORDER BY session_date",
                [phase],
            ).fetchall()
        ]
        if not dates:
            return [0.0] * folds
        boundaries = [
            dates[min(len(dates) - 1, math.floor(len(dates) * (index + 1) / folds) - 1)]
            for index in range(folds)
        ]
        result = [0.0] * folds
        for trade in trades:
            fold = next(
                (
                    index
                    for index, boundary in enumerate(boundaries)
                    if trade.signal_date <= boundary
                ),
                folds - 1,
            )
            account_return = trade.stressed_net_return * trade.notional_fraction
            if account_return > -1:
                result[fold] += math.log1p(account_return)
        return result

    def evaluate(
        self,
        spec: StrategySpec,
        *,
        phase: str,
        cumulative_trial_count: int,
    ) -> BacktestEvaluation:
        if self.database.latest_data_version() is None:
            raise BacktestError("feature mart has not been built")
        rows = self._candidate_rows(spec, phase)
        stress_bps = int(self.config.section("execution")["stress_round_trip_bps"])
        raw = [
            trade
            for row in rows
            if (trade := self._raw_trade(spec, row, stress_bps=stress_bps)) is not None
        ]
        trades = self._allocate(spec, raw)
        base = [trade.net_return for trade in trades]
        stressed = [trade.stressed_net_return for trade in trades]
        stressed_r = [trade.stressed_return_r for trade in trades]
        base_account = [trade.net_return * trade.notional_fraction for trade in trades]
        stressed_account = [
            trade.stressed_net_return * trade.notional_fraction for trade in trades
        ]
        wins = sum(value > 0 for value in stressed)
        validation = self.config.section("validation")
        confidence = float(validation["minimum_bootstrap_confidence"])
        seed = int(
            canonical_sha256({"rules": spec.rules_sha256, "phase": phase})[:16], 16
        )
        metrics: dict[str, Any] = {
            "parameters": dict(spec.parameters),
            "trade_count": len(trades),
            "candidate_signal_count": len(rows),
            "missed_signal_count": max(0, len(rows) - len(trades)),
            "total_log_growth": sum(
                math.log1p(value) for value in base_account if value > -1
            ),
            "stressed_total_log_growth": sum(
                math.log1p(value) for value in stressed_account if value > -1
            ),
            "profit_factor": profit_factor(base),
            "stressed_profit_factor": profit_factor(stressed),
            "win_rate": wins / len(trades) if trades else 0.0,
            "win_rate_lower_bound": one_sided_wilson_lower(
                wins, len(trades), confidence
            ),
            "bootstrap_lower_expectancy": bootstrap_lower_mean(
                stressed_r,
                confidence=confidence,
                samples=int(validation["bootstrap_samples"]),
                seed=seed,
            ),
            "maximum_drawdown_r": maximum_drawdown_r(stressed_r),
            "positive_chronological_halves": positive_halves(stressed_account),
            "positive_without_five_best": positive_without_best_five(stressed_account),
            "deflated_sharpe_probability": deflated_sharpe_probability(
                stressed_account, cumulative_trial_count
            ),
            "pvalue": one_sided_mean_pvalue(stressed_account),
            "fold_log_growth": self._fold_growth(trades, phase),
            "stop_count": sum(trade.exit_reason == "stop" for trade in trades),
            "target_count": sum(trade.exit_reason == "target" for trade in trades),
            "concentration_top_five_fraction": (
                sum(
                    sorted(
                        (max(0.0, value) for value in stressed_account), reverse=True
                    )[:5]
                )
                / sum(max(0.0, value) for value in stressed_account)
                if sum(max(0.0, value) for value in stressed_account) > 0
                else 1.0
            ),
            "provider_requests": 0,
            "broker_actions": 0,
            "evaluation_sha256": canonical_sha256(
                {
                    "rules": spec.rules_sha256,
                    "phase": phase,
                    "trades": [trade.to_dict() for trade in trades],
                }
            ),
        }
        return BacktestEvaluation(
            strategy_id=spec.strategy_id,
            rules_sha256=spec.rules_sha256,
            family_id=spec.family_id,
            phase=phase,
            metrics=metrics,
            trades=trades,
        )

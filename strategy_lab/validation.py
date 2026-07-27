"""Machine-earned promotion gates and persisted candidate state."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from .config import LabConfig
from .contracts import CandidateState, StrategySpec
from .database import LabDatabase, utc_now
from .engine import BacktestEvaluation
from .statistics import FamilyAdjustment


@dataclass(frozen=True)
class GateDecision:
    state: CandidateState
    failures: tuple[str, ...]
    metrics: dict[str, Any]

    @property
    def passed(self) -> bool:
        return not self.failures


def decide(
    config: LabConfig,
    evaluation: BacktestEvaluation,
    adjustment: FamilyAdjustment,
) -> GateDecision:
    metrics = dict(evaluation.metrics)
    strategy_id = evaluation.strategy_id
    metrics["probability_backtest_overfit"] = adjustment.pbo
    metrics["holm_pass"] = bool(adjustment.holm_pass.get(strategy_id, False))
    metrics["neighbor_stability"] = bool(
        adjustment.neighbor_stability.get(strategy_id, False)
    )
    validation = config.section("validation")
    minimum_trades = (
        int(validation["minimum_development_trades"])
        if evaluation.phase == "development"
        else int(validation["minimum_holdout_trades"])
    )
    checks = [
        (metrics["trade_count"] >= minimum_trades, f"trade_count<{minimum_trades}"),
        (metrics["stressed_total_log_growth"] > 0, "stressed_log_growth<=0"),
        (
            metrics["profit_factor"] >= float(validation["minimum_profit_factor"]),
            "profit_factor_below_gate",
        ),
        (
            metrics["stressed_profit_factor"]
            >= float(validation["minimum_stressed_profit_factor"]),
            "stressed_profit_factor_below_gate",
        ),
        (
            metrics["bootstrap_lower_expectancy"] > 0,
            "bootstrap_lower_expectancy<=0",
        ),
        (
            metrics["win_rate_lower_bound"]
            > float(validation["minimum_win_rate_lower_bound"]),
            "win_rate_lower_bound<=0.50",
        ),
        (
            metrics["maximum_drawdown_r"] <= float(validation["maximum_drawdown_r"]),
            "maximum_drawdown_r_above_gate",
        ),
        (
            bool(metrics["positive_chronological_halves"]),
            "chronological_half_nonpositive",
        ),
        (
            bool(metrics["positive_without_five_best"]),
            "without_five_best_nonpositive",
        ),
        (
            bool(metrics["fold_log_growth"])
            and all(float(value) > 0 for value in metrics["fold_log_growth"]),
            "rolling_fold_nonpositive",
        ),
        (
            metrics["deflated_sharpe_probability"]
            >= float(validation["minimum_deflated_sharpe_probability"]),
            "deflated_sharpe_below_gate",
        ),
        (bool(metrics["holm_pass"]), "holm_family_null_not_rejected"),
        (
            metrics["probability_backtest_overfit"]
            <= float(validation["maximum_probability_backtest_overfit"]),
            "probability_backtest_overfit_above_gate",
        ),
        (bool(metrics["neighbor_stability"]), "neighbor_stability_failed"),
    ]
    failures = tuple(reason for passed, reason in checks if not passed)
    if failures:
        state = CandidateState.REJECTED
    elif evaluation.phase == "development":
        state = CandidateState.DEVELOPMENT_PASS
    else:
        state = CandidateState.HISTORICALLY_VALIDATED
    return GateDecision(state=state, failures=failures, metrics=metrics)


def persist_evaluation(
    database: LabDatabase,
    spec: StrategySpec,
    run_id: str,
    evaluation: BacktestEvaluation,
    decision: GateDecision,
) -> None:
    metrics = decision.metrics
    with database.transaction():
        database.connection.execute(
            """
            INSERT INTO experiment_results VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            )
            """,
            [
                run_id,
                spec.strategy_id,
                spec.rules_sha256,
                spec.family_id,
                evaluation.phase,
                str(decision.state),
                int(metrics["trade_count"]),
                float(metrics["total_log_growth"]),
                float(metrics["profit_factor"]),
                float(metrics["stressed_profit_factor"]),
                float(metrics["win_rate"]),
                float(metrics["win_rate_lower_bound"]),
                float(metrics["bootstrap_lower_expectancy"]),
                float(metrics["maximum_drawdown_r"]),
                float(metrics["deflated_sharpe_probability"]),
                float(metrics["probability_backtest_overfit"]),
                bool(metrics["holm_pass"]),
                bool(metrics["neighbor_stability"]),
                json.dumps(list(decision.failures), separators=(",", ":")),
                json.dumps(metrics, sort_keys=True, separators=(",", ":")),
                utc_now(),
            ],
        )
        if evaluation.trades:
            database.connection.executemany(
                """
                INSERT INTO trades VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                )
                """,
                [
                    [
                        run_id,
                        spec.strategy_id,
                        evaluation.phase,
                        trade.signal_date,
                        trade.entry_date,
                        trade.exit_date,
                        trade.symbol,
                        trade.entry_price,
                        trade.exit_price,
                        trade.exit_reason,
                        trade.gross_return,
                        trade.net_return,
                        trade.return_r,
                        trade.notional_fraction,
                        trade.rank_value,
                    ]
                    for trade in evaluation.trades
                ],
            )
        database.upsert_candidate(
            spec,
            state=decision.state,
            reason="passed all gates"
            if decision.passed
            else ", ".join(decision.failures),
            development_run_id=run_id if evaluation.phase == "development" else None,
            holdout_run_id=run_id if evaluation.phase == "holdout" else None,
        )

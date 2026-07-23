"""Deterministic no-provider plugin used by discovery pipeline tests."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from learning_data import freeze_dataset_contract


def preflight(contract: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "verified_capacity": int(contract.get("synthetic_capacity", 120)),
        "point_in_time_complete": True,
        "provider_telemetry": {
            "requests": 0,
            "request_seconds": 0.0,
            "pacing_wait_seconds": 0.0,
            "cache_hits": 1,
            "failures": 0,
            "dataset_loads": 1,
        },
    }


def evaluate_development(
    contract: Mapping[str, Any], trials: Sequence[Mapping[str, Any]]
) -> dict[str, Any]:
    overfit = contract.get("synthetic_mode") == "overfit"
    evidence_dates = [
        day
        for fold in contract["rolling_origin_plan"]
        for day in fold["test_dates"]
    ]
    observation_count = len(evidence_dates)
    evaluated: list[dict[str, Any]] = []
    for index, trial in enumerate(trials):
        if overfit:
            midpoint = observation_count // 2
            returns = (
                [0.010] * midpoint
                + [-0.009] * (observation_count - midpoint)
                if index == 0
                else [-0.001] * observation_count
            )
        else:
            if index == len(trials) - 1:
                returns = [
                    0.0005 if item % 2 == 0 else 0.0015
                    for item in range(observation_count)
                ]
            else:
                gain = 0.00020 + index * 0.000001
                loss = -0.00019
                returns = [
                    loss if item % 2 == 0 else gain
                    for item in range(observation_count)
                ]
        metrics = {
            "stress_20bps_total_log_growth": (
                0.10 + index * 0.001 if not overfit else (-0.01 if index else 0.50)
            ),
            "stress_20bps_bootstrap_lower_mean_account_return": (
                0.0005 + index * 0.000001 if not overfit else (0.01 if index == 0 else -0.01)
            ),
            "stress_20bps_profit_factor": 1.50 if not overfit else (4.0 if index == 0 else 0.8),
            "stress_20bps_maximum_drawdown_r": 1.0 if not overfit else (1.0 if index == 0 else 8.0),
            "deflated_sharpe_probability": 0.97 if not overfit else 0.20,
            "pbo_probability": 0.20 if not overfit else 0.90,
            "holm_reject_null": not overfit,
            "rolling_folds_positive": not overfit,
            "rules_complete": True,
            "trial_accounting_complete": True,
            "oof_daily_account_returns": returns,
            "oof_filled_account_returns": returns,
            "oof_net_pnl_dollars": [value * 100_000 for value in returns],
            "risk_fraction": 0.005,
        }
        rows = [
            {
                "date": day,
                "session_outcome": "filled",
                "primary_account_return_fraction": value + 0.0002,
                "stress_10bps_account_return_fraction": value + 0.0001,
                "stress_20bps_account_return_fraction": value,
                "net_r": (value + 0.0002) / 0.005,
                "stress_10bps_r": (value + 0.0001) / 0.005,
                "stress_20bps_r": value / 0.005,
                "net_pnl_dollars": (value + 0.0002) * 100_000,
                "stress_10bps_net_pnl_dollars": (value + 0.0001) * 100_000,
                "stress_20bps_net_pnl_dollars": value * 100_000,
                "stop_executed": value < 0,
            }
            for day, value in zip(
                evidence_dates, returns, strict=True
            )
        ]
        evaluated.append(
            {
                "trial_id": trial["trial_id"],
                "metrics": metrics,
                "trial_accounting": [
                    {
                        "date": day,
                        "outcome": "account_return_day",
                    }
                    for day in evidence_dates
                ],
                "maturity_rows": rows,
            }
        )
    return {
        "dataset_manifest": contract["dataset_manifest"],
        "trials": evaluated,
        "provider_telemetry": {
            "requests": 0,
            "request_seconds": 0.0,
            "pacing_wait_seconds": 0.0,
            "cache_hits": 1,
            "failures": 0,
            "dataset_loads": 1,
        },
    }


def evaluate_confirmation(winner: Mapping[str, Any]) -> dict[str, Any]:
    dates = list(winner["confirmation_dates"])
    development_manifest = Path(str(winner["development_dataset_manifest"]))
    confirmation_manifest, _ = freeze_dataset_contract(
        {
            "schema_version": 1,
            "dataset_id": f"dataset-{winner['family_id']}-confirmation",
            "registered_at": winner["recorded_at"],
            "requested_dates": dates,
            "dataset_payload": {
                "lane": "confirmation",
                "claim_scope": "EXACT_PREREGISTERED_CONTRACT_ONLY",
                "evidence_paths": ["tests/synthetic_discovery_plugin.py"],
                "inspected": True,
                "preregistration_sha256": winner["rules_hash"],
                "preregistered_at": winner["recorded_at"],
                "capture_after_preregistration_attested": True,
            },
        },
        development_manifest.parent / "confirmation",
    )

    def scenario(gain: float, loss: float) -> dict[str, Any]:
        filled = [gain if index % 5 else loss for index in range(len(dates))]
        return {
            "daily_account_returns": filled,
            "filled_account_returns": filled,
            "net_pnl_dollars": [value * 100_000 for value in filled],
        }

    primary = scenario(0.0020, -0.0002)
    stress_10 = scenario(0.0018, -0.0003)
    stress_20 = scenario(0.0015, -0.0004)
    return {
        "rules_hash": winner["rules_hash"],
        "parameter_alternatives": 0,
        "observed_dates": dates,
        "outcome_access_before_winner_freeze": False,
        "dataset_manifest": str(confirmation_manifest),
        "scenarios": {
            "primary_5bps": primary,
            "stress_10bps": stress_10,
            "stress_20bps": stress_20,
        },
        "maturity_rows": [
            {
                "date": day,
                "session_outcome": "filled",
                "primary_account_return_fraction": primary[
                    "filled_account_returns"
                ][index],
                "stress_10bps_account_return_fraction": stress_10[
                    "filled_account_returns"
                ][index],
                "stress_20bps_account_return_fraction": stress_20[
                    "filled_account_returns"
                ][index],
                "net_r": primary["filled_account_returns"][index] / 0.005,
                "stress_10bps_r": stress_10["filled_account_returns"][index]
                / 0.005,
                "stress_20bps_r": stress_20["filled_account_returns"][index]
                / 0.005,
                "net_pnl_dollars": primary["net_pnl_dollars"][index],
                "stress_10bps_net_pnl_dollars": stress_10[
                    "net_pnl_dollars"
                ][index],
                "stress_20bps_net_pnl_dollars": stress_20[
                    "net_pnl_dollars"
                ][index],
                "stop_executed": primary["filled_account_returns"][index] < 0,
            }
            for index, day in enumerate(dates)
        ],
        "rule_violations": [],
        "capture_complete": True,
        "provider_telemetry": {
            "requests": 0,
            "request_seconds": 0.0,
            "pacing_wait_seconds": 0.0,
            "cache_hits": 1,
            "failures": 0,
            "dataset_loads": 1,
        },
    }


def evaluate_production(
    winner: Mapping[str, Any], market_facts: Mapping[str, Any]
) -> dict[str, Any]:
    """Synthetic exact-rule live adapter used only by integration tests."""
    return {
        **dict(market_facts),
        "strategy_id": winner["strategy_id"],
        "strategy_version": winner["strategy_version"],
        "rules_hash": winner["rules_hash"],
        "historical_semantics_sha256": winner["rules_hash"],
    }

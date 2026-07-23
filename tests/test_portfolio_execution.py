from __future__ import annotations

from datetime import UTC, datetime, timedelta
import hashlib
from pathlib import Path

import pytest

import portfolio_execution as execution
import portfolio_maturity


WINNER = {
    "strategy_id": "strategy-test-edge",
    "strategy_version": "strategy-test-edge-abc123",
    "rules_hash": "a" * 64,
}


def _plugin_winner():
    path = Path("tests/synthetic_discovery_plugin.py")
    return {
        **WINNER,
        "plugin": {
            "module": "tests.synthetic_discovery_plugin",
            "evaluate_production": "evaluate_production",
        },
        "implementation_hashes": {
            str(path): hashlib.sha256(path.read_bytes()).hexdigest()
        },
    }


def candidate(**overrides):
    value = {
        **WINNER,
        "historical_semantics_sha256": WINNER["rules_hash"],
        "ranking_complete": True,
        "observed_at": datetime.now(UTC).isoformat(),
        "symbol": "TEST",
        "halted": False,
        "tradable": True,
        "bid": 99.98,
        "ask": 100.00,
        "entry_limit": 100.01,
        "stop_price": 99.00,
        "expected_gross_move_fraction": 0.010,
        "holding_trading_days": 2,
        "before_open_account_reconciled": True,
        "before_open_orders_reconciled": True,
        "before_open_protection_reconciled": True,
        "before_open_tradability_reconciled": True,
        "before_open_news_reconciled": True,
        "protective_order_route_ready": True,
        "monitoring_ready": True,
        "protection_time_in_force": "gtc",
        "protection_failure_safe_cutoff": "15:45 ET",
        "exit_plan": {
            "type": "stop_or_maximum_hold_close",
            "maximum_hold_sessions": 2,
            "same_interval_ambiguity": "stop_first",
        },
        "executable_ask_depth": 20_000,
        "recent_real_minute_volume": 30_000,
    }
    value.update(overrides)
    return value


def test_exact_winner_builds_whole_share_order_and_gtc_protection():
    config = portfolio_maturity.load_config()
    now = datetime.now(UTC)
    result = execution.evaluate_production_candidate(
        WINNER,
        candidate(observed_at=now.isoformat()),
        {"equity": 100_000, "buying_power": 100_000},
        config,
        now=now,
    )
    assert result["status"] == "PRODUCTION_EVALUATION_READY"
    assert isinstance(result["order"]["quantity"], int)
    assert result["order"]["whole_shares"] is True
    assert result["protection"]["time_in_force"] == "gtc"
    assert result["risk"]["planned_loss_fraction"] <= 0.005
    assert result["broker_actions_performed"] == 0


def test_same_frozen_plugin_builds_the_production_candidate():
    now = datetime.now(UTC)
    result = execution.evaluate_frozen_winner(
        _plugin_winner(),
        {
            key: value
            for key, value in candidate(observed_at=now.isoformat()).items()
            if key not in WINNER
        },
        {"equity": 100_000, "buying_power": 100_000},
        portfolio_maturity.load_config(),
        now=now,
    )
    assert result["status"] == "PRODUCTION_EVALUATION_READY"


def test_frozen_plugin_implementation_drift_fails_closed():
    winner = _plugin_winner()
    winner["implementation_hashes"]["tests/synthetic_discovery_plugin.py"] = "0" * 64
    with pytest.raises(execution.PortfolioExecutionError, match="implementation drifted"):
        execution.evaluate_frozen_winner(
            winner,
            {},
            {"equity": 100_000, "buying_power": 100_000},
            portfolio_maturity.load_config(),
        )


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"rules_hash": "b" * 64}, "rules_hash drifted"),
        (
            {"observed_at": (datetime.now(UTC) - timedelta(seconds=6)).isoformat()},
            "stale",
        ),
        ({"expected_gross_move_fraction": 0.0049}, "cost floor"),
        ({"stop_price": 99.99}, "stop is not below"),
        ({"holding_trading_days": 2, "protection_time_in_force": "day"}, "GTC"),
        ({"before_open_news_reconciled": False}, "is not true"),
    ],
)
def test_production_evaluation_fails_closed(overrides, message):
    with pytest.raises(execution.PortfolioExecutionError, match=message):
        execution.evaluate_production_candidate(
            WINNER,
            candidate(**overrides),
            {"equity": 100_000, "buying_power": 100_000},
            portfolio_maturity.load_config(),
        )


def test_unknown_submission_reconciliation_prevents_duplicate_orders():
    alias = "entry-attempt-one"
    absent = execution.reconcile_unknown_submission(alias, [])
    active = execution.reconcile_unknown_submission(
        alias, [{"logical_order_alias": alias, "state": "open"}]
    )
    terminal = execution.reconcile_unknown_submission(
        alias, [{"logical_order_alias": alias, "state": "rejected"}]
    )
    duplicate = execution.reconcile_unknown_submission(
        alias,
        [
            {"logical_order_alias": alias, "state": "open"},
            {"logical_order_alias": alias, "state": "filled"},
        ],
    )
    assert absent["retry_permitted"] is True
    assert terminal["retry_permitted"] is True
    assert active["retry_permitted"] is False
    assert duplicate["status"] == "DUPLICATE_OR_AMBIGUOUS_PAUSE"

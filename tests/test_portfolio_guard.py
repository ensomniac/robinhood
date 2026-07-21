from __future__ import annotations

import unittest
from datetime import UTC, datetime, timedelta

import portfolio_guard as guard
import portfolio_maturity as maturity


RULES_HASH = "a" * 64


class PortfolioGuardTests(unittest.TestCase):
    def setUp(self):
        self.config = maturity.load_config()

    def _snapshot(self, **overrides):
        value = {
            "schema_version": 1,
            "observed_at": datetime.now(UTC).isoformat(),
            "strategy_id": "strategy-one",
            "strategy_version": "v1",
            "rules_hash": RULES_HASH,
            "broker_state": "FLAT_RECONCILED",
            "account_reconciled": True,
            "orders_reconciled": True,
            "positions_count": 0,
            "protected_positions_count": 0,
            "unknown_orders_count": 0,
            "unprotected_positions_count": 0,
            "new_entries_today": 0,
            "gross_notional_fraction": 0.0,
            "aggregate_planned_open_loss_fraction": 0.0,
            "daily_loss_fraction": 0.0,
            "weekly_loss_fraction": 0.0,
            "peak_to_trough_drawdown_fraction": 0.0,
            "proposed_position_loss_fraction": 0.004,
            "proposed_gross_notional_fraction": 0.25,
            "proposed_holding_trading_days": 2,
            "tradable": True,
            "broker_review_passed": True,
            "broker_confirmation_required": False,
            "broker_confirmation_satisfied": False,
            "protective_order_route_ready": True,
            "monitoring_ready": True,
            "source": "fresh privacy-safe broker reconciliation",
        }
        value.update(overrides)
        return value

    def _report(self, *, ready=True, rules_hash=RULES_HASH):
        return {
            "strategies": [
                {
                    "strategy_id": "strategy-one",
                    "strategy_version": "v1",
                    "rules_hash": rules_hash,
                    "pilot_ready": ready,
                    "maturity": "PILOT_READY" if ready else "RESEARCH",
                }
            ]
        }

    def test_entry_ready_requires_exact_ready_strategy_and_safe_post_entry_risk(self):
        result = guard.evaluate_entry(
            self._snapshot(), self._report(), self.config
        )
        self.assertEqual(result["status"], "ENTRY_READY")
        self.assertTrue(result["entry_allowed"])
        self.assertEqual(result["post_entry"]["positions_count"], 1)

    def test_unready_or_stale_rules_strategy_cannot_enter(self):
        unready = guard.evaluate_entry(
            self._snapshot(), self._report(ready=False), self.config
        )
        stale = guard.evaluate_entry(
            self._snapshot(), self._report(rules_hash="b" * 64), self.config
        )
        self.assertEqual(unready["status"], "NOT_PILOT_READY")
        self.assertEqual(stale["status"], "NOT_PILOT_READY")
        self.assertFalse(unready["entry_allowed"])
        self.assertFalse(stale["entry_allowed"])

    def test_post_entry_portfolio_caps_fail_closed(self):
        result = guard.evaluate_entry(
            self._snapshot(
                positions_count=3,
                protected_positions_count=3,
                aggregate_planned_open_loss_fraction=0.01,
                proposed_position_loss_fraction=0.005,
                gross_notional_fraction=0.9,
                proposed_gross_notional_fraction=0.2,
            ),
            self._report(),
            self.config,
        )
        self.assertEqual(result["status"], "PAUSED_SAFETY")
        self.assertTrue(any("position count" in item for item in result["blockers"]))
        self.assertTrue(any("planned open loss" in item for item in result["blockers"]))
        self.assertTrue(any("gross notional" in item for item in result["blockers"]))

    def test_unknown_orders_missing_protection_or_monitoring_fail_closed(self):
        result = guard.evaluate_entry(
            self._snapshot(
                unknown_orders_count=1,
                protective_order_route_ready=False,
                monitoring_ready=False,
            ),
            self._report(),
            self.config,
        )
        self.assertEqual(result["status"], "PAUSED_SAFETY")
        self.assertFalse(result["entry_allowed"])

    def test_review_and_required_human_confirmation_are_distinct_waits(self):
        review = guard.evaluate_entry(
            self._snapshot(broker_review_passed=False),
            self._report(),
            self.config,
        )
        confirmation = guard.evaluate_entry(
            self._snapshot(
                broker_confirmation_required=True,
                broker_confirmation_satisfied=False,
            ),
            self._report(),
            self.config,
        )
        self.assertEqual(review["status"], "WAITING_REVIEW")
        self.assertEqual(confirmation["status"], "WAITING_USER_CONFIRMATION")

    def test_stale_snapshot_and_identifier_field_are_rejected(self):
        stale = guard.evaluate_entry(
            self._snapshot(
                observed_at=(datetime.now(UTC) - timedelta(seconds=16)).isoformat()
            ),
            self._report(),
            self.config,
        )
        self.assertEqual(stale["status"], "PAUSED_SAFETY")
        with self.assertRaisesRegex(guard.PortfolioGuardError, "forbidden fields"):
            guard.validate_snapshot(self._snapshot(account_number="forbidden"))


if __name__ == "__main__":
    unittest.main()

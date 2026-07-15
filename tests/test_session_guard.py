import unittest

from session_guard import evaluate_guard
from strategy_engine import load_config


def safe_snapshot():
    config = load_config()
    return {
        "time_et": "09:45:00",
        "declared_maturity": "UNVALIDATED",
        "evaluation_rules_hash": config.rules_hash,
        "account_identified": True,
        "agentic_allowed": True,
        "broker_available": True,
        "monitoring_available": True,
        "protective_stop_workflow_ready": True,
        "encryption_ready": True,
        "identifier_audit_ready": True,
        "candidate_evaluation_eligible": True,
        "broker_review_clear": True,
        "confirmation_satisfied": True,
        "position_quantity": 0,
        "active_entry_orders": 0,
        "active_exit_orders": 0,
        "active_stop_orders": 0,
        "active_stop_quantity": 0,
        "unknown_orders": 0,
        "filled_entries_today": 0,
        "entry_order_age_seconds": 0,
        "protection_transition_age_seconds": 0,
        "market_data_age_seconds": 1,
        "monitor_heartbeat_age_seconds": 1,
        "rolling_five_session_drawdown_fraction": 0,
        "strategy_drawdown_fraction": 0,
        "consecutive_losses": 0,
        "manual_circuit_breaker_active": False,
    }


class EntryGuardTests(unittest.TestCase):
    def test_clean_snapshot_is_entry_ready(self):
        result = evaluate_guard(safe_snapshot(), "UNVALIDATED")

        self.assertEqual(result.status, "ENTRY_READY")
        self.assertTrue(result.entry_allowed)

    def test_claimed_maturity_cannot_exceed_ledger_evidence(self):
        snapshot = safe_snapshot()
        snapshot["declared_maturity"] = "VALIDATED"

        result = evaluate_guard(snapshot, "UNVALIDATED")

        self.assertEqual(result.status, "ENTRY_BLOCKED")
        self.assertIn(
            "declared maturity exceeds evidence-earned maturity", result.reasons
        )

    def test_day_limit_and_drawdown_are_machine_blockers(self):
        snapshot = safe_snapshot()
        snapshot["filled_entries_today"] = 1
        snapshot["rolling_five_session_drawdown_fraction"] = 0.02

        result = evaluate_guard(snapshot, "UNVALIDATED")

        self.assertFalse(result.entry_allowed)
        self.assertIn("today's filled-entry limit is exhausted", result.reasons)
        self.assertIn(
            "rolling five-session drawdown circuit breaker is active", result.reasons
        )

    def test_stale_strategy_evaluation_is_blocked(self):
        snapshot = safe_snapshot()
        snapshot["evaluation_rules_hash"] = "old-rules"

        result = evaluate_guard(snapshot, "UNVALIDATED")

        self.assertIn(
            "candidate evaluation used stale or different rules", result.reasons
        )


class ExposureGuardTests(unittest.TestCase):
    def test_fully_protected_position_is_managed(self):
        snapshot = safe_snapshot()
        snapshot["position_quantity"] = 100
        snapshot["active_stop_orders"] = 1
        snapshot["active_stop_quantity"] = 100

        result = evaluate_guard(snapshot, "UNVALIDATED")

        self.assertEqual(result.status, "MANAGE_POSITION")
        self.assertFalse(result.must_flatten)

    def test_protected_partial_fill_still_cancels_entry_remainder(self):
        snapshot = safe_snapshot()
        snapshot["position_quantity"] = 100
        snapshot["active_entry_orders"] = 1
        snapshot["active_stop_orders"] = 1
        snapshot["active_stop_quantity"] = 100

        result = evaluate_guard(snapshot, "UNVALIDATED")

        self.assertEqual(result.status, "CANCEL_ENTRY_REMAINDER")

    def test_fresh_fill_gets_short_protect_now_window(self):
        snapshot = safe_snapshot()
        snapshot["position_quantity"] = 100
        snapshot["active_entry_orders"] = 1
        snapshot["protection_transition_age_seconds"] = 4

        result = evaluate_guard(snapshot, "UNVALIDATED")

        self.assertEqual(result.status, "PROTECT_NOW")
        self.assertIn(
            "cancel and confirm the unfilled entry remainder", result.required_actions
        )

    def test_expired_unprotected_fill_must_flatten(self):
        snapshot = safe_snapshot()
        snapshot["position_quantity"] = 100
        snapshot["protection_transition_age_seconds"] = 10.1

        result = evaluate_guard(snapshot, "UNVALIDATED")

        self.assertEqual(result.status, "KILL_SWITCH_FLATTEN")
        self.assertTrue(result.must_flatten)

    def test_stale_monitoring_during_exposure_must_flatten(self):
        snapshot = safe_snapshot()
        snapshot["position_quantity"] = 100
        snapshot["active_stop_orders"] = 1
        snapshot["active_stop_quantity"] = 100
        snapshot["monitor_heartbeat_age_seconds"] = 15.1

        result = evaluate_guard(snapshot, "UNVALIDATED")

        self.assertTrue(result.must_flatten)
        self.assertIn("monitoring heartbeat is stale during exposure", result.reasons)

    def test_force_flat_time_is_enforced(self):
        snapshot = safe_snapshot()
        snapshot["time_et"] = "15:50:00"
        snapshot["position_quantity"] = 100
        snapshot["active_stop_orders"] = 1
        snapshot["active_stop_quantity"] = 100

        result = evaluate_guard(snapshot, "UNVALIDATED")

        self.assertTrue(result.must_flatten)
        self.assertIn("force-flat time reached with an open position", result.reasons)

    def test_orphan_stop_while_flat_requires_reconciliation(self):
        snapshot = safe_snapshot()
        snapshot["active_stop_orders"] = 1
        snapshot["active_stop_quantity"] = 100

        result = evaluate_guard(snapshot, "UNVALIDATED")

        self.assertEqual(result.status, "RECONCILE_FLAT_ORDERS")
        self.assertFalse(result.entry_allowed)

    def test_independent_exit_and_stop_are_a_flatten_kill_switch(self):
        snapshot = safe_snapshot()
        snapshot["position_quantity"] = 100
        snapshot["active_stop_orders"] = 1
        snapshot["active_stop_quantity"] = 100
        snapshot["active_exit_orders"] = 1

        result = evaluate_guard(snapshot, "UNVALIDATED")

        self.assertTrue(result.must_flatten)
        self.assertIn(
            "protective orders can oversell the live position", result.reasons
        )

    def test_timed_out_entry_must_be_cancelled(self):
        snapshot = safe_snapshot()
        snapshot["active_entry_orders"] = 1
        snapshot["entry_order_age_seconds"] = 10

        result = evaluate_guard(snapshot, "UNVALIDATED")

        self.assertEqual(result.status, "MANAGE_ENTRY_ORDER")
        self.assertIn(
            "cancel and confirm the stale entry order", result.required_actions
        )

    def test_stale_data_cancels_even_a_young_entry_order(self):
        snapshot = safe_snapshot()
        snapshot["active_entry_orders"] = 1
        snapshot["entry_order_age_seconds"] = 2
        snapshot["market_data_age_seconds"] = 5.1

        result = evaluate_guard(snapshot, "UNVALIDATED")

        self.assertIn(
            "data or monitoring became stale while entry was open", result.reasons
        )
        self.assertIn(
            "cancel and confirm the stale entry order", result.required_actions
        )


if __name__ == "__main__":
    unittest.main()

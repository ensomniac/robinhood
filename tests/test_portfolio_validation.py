from __future__ import annotations

import json
import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import patch

import portfolio_validation as validation


class PortfolioValidationTests(unittest.TestCase):
    def _lineage(self):
        return {
            "plan_sha256": "plan-hash",
            "portfolio_config_sha256": "config-hash",
            "portfolio_ledger_sha256": "ledger-hash",
            "upstream_artifact_hashes": {"AGENTS.md": "agents-hash"},
            "strategy_bindings": [],
            "legacy_campaign_binding": {
                "campaign_id": "production-strategy-validation",
                "sequence": 102,
                "phase": "DEVELOPMENT_ACQUISITION",
                "status": "READY",
                "event_sha256": "legacy-event-hash",
                "events_file_sha256": "legacy-file-hash",
            },
        }

    def _safety(self, **overrides):
        value = {
            "schema_version": 1,
            "observed_at": datetime.now(UTC).isoformat(),
            "broker_state": "FLAT_RECONCILED",
            "account_reconciled": True,
            "orders_reconciled": True,
            "positions_count": 0,
            "protected_positions_count": 0,
            "open_orders_count": 0,
            "unknown_orders_count": 0,
            "unprotected_positions_count": 0,
            "new_entries_today": 0,
            "gross_notional_fraction": 0.0,
            "aggregate_planned_open_loss_fraction": 0.0,
            "daily_loss_fraction": 0.0,
            "weekly_loss_fraction": 0.0,
            "peak_to_trough_drawdown_fraction": 0.0,
            "source": "fresh broker reconciliation",
        }
        value.update(overrides)
        return value

    def _event(self, *, sequence=1, previous_hash="", **overrides):
        value = {
            "schema_version": 1,
            "campaign_id": validation.CAMPAIGN_ID,
            "sequence": sequence,
            "recorded_at": datetime.now(UTC).isoformat(),
            "transition_kind": "INITIALIZED" if sequence == 1 else "RECORDED",
            "phase": "SUPERSESSION",
            "status": "READY",
            "active_objective": "preserve-old-campaign",
            "blocker": "",
            "next_action": "Inspect preserved evidence.",
            "superseded_campaign_status": "SUPERSEDED_PAUSED",
            **self._lineage(),
            "evidence_hashes": {},
            "safety_snapshot": None,
            "previous_event_sha256": previous_hash,
        }
        value.update(overrides)
        value["event_sha256"] = validation._event_hash(value)
        return value

    def _authoritative(self, current, **overrides):
        value = {
            "lineage": {
                field: current[field]
                for field in (
                    "plan_sha256",
                    "portfolio_config_sha256",
                    "portfolio_ledger_sha256",
                    "upstream_artifact_hashes",
                    "strategy_bindings",
                    "legacy_campaign_binding",
                )
            },
            "portfolio_config": validation.load_portfolio_config().raw,
            "portfolio_ledger_audit": {"valid": True},
            "funnel_audit": {"valid": True},
            "funnel": {"valid": True},
            "portfolio_report": {
                "earned_milestone": validation.TERMINAL_PHASE,
                "milestone_blockers": [],
                "earned_interim_milestones": [validation.FIRST_PILOT_MILESTONE],
                "interim_milestone_blockers": [],
            },
            "registry_audit": {"valid": True},
            "strategy_audit": {"valid": True},
            "learning_data_audit": {"valid": True},
            "orb_ledger_audit": {"valid": True},
            "lifecycle_audit": {"valid": True},
            "privacy_audit": {"valid": True},
            "progress_audit": {"valid": True},
            "store_capacity": {"capacity_ready": True},
            "git": {"valid": True, "clean": True, "head_equals_upstream": True},
        }
        value.update(overrides)
        return value

    def test_phase_table_rejects_skipping_to_live(self):
        with self.assertRaisesRegex(
            validation.PortfolioValidationError, "unsafe campaign phase transition"
        ):
            validation.validate_phase_transition("SUPERSESSION", "LIVE_PILOT")

    def test_phase_table_allows_live_to_return_to_development(self):
        validation.validate_phase_transition("LIVE_PILOT", "DEVELOPMENT")

    def test_waiting_event_requires_blocker(self):
        event = self._event(status="WAITING_DATA")
        with self.assertRaisesRegex(validation.PortfolioValidationError, "blocker"):
            validation._validate_event(event, previous_hash="", expected_sequence=1)

    def test_safety_snapshot_rejects_identifier_fields(self):
        snapshot = self._safety(account_number="forbidden")
        with self.assertRaisesRegex(
            validation.PortfolioValidationError, "forbidden fields"
        ):
            validation._validate_safety_snapshot(snapshot)

    def test_safe_protected_exposure_is_allowed_inside_caps(self):
        snapshot = self._safety(
            broker_state="PROTECTED_EXPOSURE_RECONCILED",
            positions_count=2,
            protected_positions_count=2,
            open_orders_count=2,
            new_entries_today=3,
            gross_notional_fraction=0.8,
            aggregate_planned_open_loss_fraction=0.01,
        )
        blockers = validation._safety_snapshot_blockers(
            snapshot, validation.load_portfolio_config().raw
        )
        self.assertEqual(blockers, [])

    def test_risk_or_position_cap_breach_pauses_finalization(self):
        snapshot = self._safety(
            broker_state="PROTECTED_EXPOSURE_RECONCILED",
            positions_count=4,
            protected_positions_count=4,
            aggregate_planned_open_loss_fraction=0.02,
        )
        blockers = validation._safety_snapshot_blockers(
            snapshot, validation.load_portfolio_config().raw
        )
        self.assertTrue(any("positions_count exceeds" in item for item in blockers))
        self.assertTrue(
            any("aggregate_planned_open_loss_fraction exceeds" in item for item in blockers)
        )

    def test_stale_safety_snapshot_is_rejected(self):
        snapshot = self._safety(
            observed_at=(datetime.now(UTC) - timedelta(hours=1)).isoformat()
        )
        blockers = validation._safety_snapshot_blockers(
            snapshot, validation.load_portfolio_config().raw
        )
        self.assertIn("broker safety snapshot is stale", blockers)

    def test_event_log_detects_content_tampering(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "events.jsonl"
            event = self._event()
            event["phase"] = "LIVE_PILOT"
            path.write_text(json.dumps(event) + "\n", encoding="utf-8")
            with self.assertRaisesRegex(
                validation.PortfolioValidationError, "content hash"
            ):
                validation._load_events(path)

    def test_state_projection_must_match_event_log(self):
        with tempfile.TemporaryDirectory() as directory:
            run_root = Path(directory)
            event = self._event()
            (run_root / "events.jsonl").write_text(
                json.dumps(event) + "\n", encoding="utf-8"
            )
            state = validation._project_state(event)
            state["phase"] = "LIVE_PILOT"
            (run_root / "state.json").write_text(json.dumps(state), encoding="utf-8")
            with self.assertRaisesRegex(
                validation.PortfolioValidationError, "projection"
            ):
                validation._load_current(run_root)

    def test_manual_terminal_record_is_refused(self):
        with self.assertRaisesRegex(
            validation.PortfolioValidationError, "cannot be recorded manually"
        ):
            validation.record_transition(
                phase=validation.TERMINAL_PHASE,
                status="READY",
                objective="bypass",
                blocker="",
                next_action="none",
                evidence_paths=[],
                safety_path=None,
            )

    def test_insufficient_strategy_report_cannot_finalize(self):
        current = self._event(safety_snapshot=self._safety())
        authoritative = self._authoritative(current)
        authoritative["portfolio_report"] = {
            "earned_milestone": "RESEARCH",
            "milestone_blockers": ["pilot-ready strategies 0 is below required 3"],
        }
        with (
            patch.object(validation, "_load_current", return_value=([current], current)),
            patch.object(validation, "_authoritative_snapshot", return_value=authoritative),
            patch.object(validation, "_append_event") as append,
        ):
            result = validation.audit_campaign()
        self.assertFalse(result["terminal"])
        self.assertIn(
            "pilot-ready strategies 0 is below required 3",
            result["finalization_blockers"],
        )
        append.assert_not_called()

    def test_stale_strategy_binding_blocks_finalization(self):
        current = self._event(safety_snapshot=self._safety())
        authoritative = self._authoritative(current)
        authoritative["lineage"]["strategy_bindings"] = [
            {"strategy_id": "new-evidence"}
        ]
        blockers = validation._finalization_blockers(current, authoritative)
        self.assertIn("campaign strategy_bindings binding is stale", blockers)

    def test_next_emits_only_one_nonexecuting_handoff(self):
        current = self._event(phase="TOURNAMENT")
        with (
            patch.object(validation, "_load_current", return_value=([current], current)),
            patch.object(validation, "_lineage", return_value=self._lineage()),
        ):
            result = validation.next_handoff()
        self.assertFalse(result["bounded_handoff"]["perform_by_controller"])
        self.assertEqual(result["bounded_handoff"]["objective"], "falsify-one-preregistered-mechanism-variant")

    def test_interim_goal_requires_ready_strategy_live_evidence(self):
        current = self._event(safety_snapshot=self._safety())
        authoritative = self._authoritative(current)
        authoritative["portfolio_report"]["earned_interim_milestones"] = []
        authoritative["portfolio_report"]["interim_milestone_blockers"] = [
            "pilot-ready strategies with a completed live execution 0 is below required 1"
        ]
        with (
            patch.object(validation, "_load_current", return_value=([current], current)),
            patch.object(validation, "_authoritative_snapshot", return_value=authoritative),
            patch.object(validation, "_append_event") as append,
        ):
            result = validation.audit_campaign(goal=validation.FIRST_PILOT_GOAL)
        self.assertFalse(result["goal_complete"])
        self.assertIn("completed live execution", result["finalization_blockers"][0])
        append.assert_not_called()

    def test_interim_goal_requires_flat_reconciled_snapshot(self):
        current = self._event(
            safety_snapshot=self._safety(
                broker_state="PROTECTED_EXPOSURE_RECONCILED",
                positions_count=1,
                protected_positions_count=1,
                open_orders_count=1,
            )
        )
        authoritative = self._authoritative(current)
        blockers = validation._interim_goal_blockers(current, authoritative)
        self.assertIn("broker state is not flat and reconciled", blockers)
        self.assertIn("broker snapshot retains positions or open orders", blockers)

    def test_interim_goal_records_nonterminal_idempotent_milestone(self):
        current = self._event(phase="LIVE_PILOT", safety_snapshot=self._safety())
        authoritative = self._authoritative(current)
        recorded = self._event(
            sequence=2,
            previous_hash=current["event_sha256"],
            transition_kind="MILESTONE",
            phase="LIVE_PILOT",
            milestone=validation.FIRST_PILOT_MILESTONE,
            safety_snapshot=self._safety(),
        )
        with (
            patch.object(validation, "_load_current", return_value=([current], current)),
            patch.object(validation, "_authoritative_snapshot", return_value=authoritative),
            patch.object(validation, "_append_event", return_value=recorded) as append,
        ):
            result = validation.audit_campaign(goal=validation.FIRST_PILOT_GOAL)
        self.assertTrue(result["goal_complete"])
        self.assertTrue(result["milestone_recorded_now"])
        self.assertFalse(result["terminal"])
        append.assert_called_once()

        with (
            patch.object(
                validation,
                "_load_current",
                return_value=([current, recorded], recorded),
            ),
            patch.object(validation, "_authoritative_snapshot", return_value=authoritative),
            patch.object(validation, "_append_event") as append_again,
        ):
            repeated = validation.audit_campaign(goal=validation.FIRST_PILOT_GOAL)
        self.assertTrue(repeated["goal_complete"])
        self.assertFalse(repeated["milestone_recorded_now"])
        append_again.assert_not_called()


if __name__ == "__main__":
    unittest.main()

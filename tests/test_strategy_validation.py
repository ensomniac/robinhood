from __future__ import annotations

import json
import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import patch

import strategy_validation as validation


class StrategyValidationTests(unittest.TestCase):
    def _safety(self, **overrides):
        value = {
            "schema_version": 1,
            "observed_at": datetime.now(UTC).isoformat(),
            "broker_state": "FLAT_RECONCILED",
            "account_reconciled": True,
            "positions_count": 0,
            "open_orders_count": 0,
            "unknown_orders_count": 0,
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
            "phase": "SOURCE_SEMANTICS",
            "status": "READY",
            "active_objective": "close-source-semantics",
            "blocker": "",
            "next_action": "Freeze the source contract.",
            "champion_id": "strategy-v3",
            "strategy_version": "v3",
            "rules_hash": "rules-v3",
            "plan_sha256": "plan-v1",
            "upstream_artifact_hashes": {"AGENTS.md": "hash"},
            "evidence_hashes": {},
            "safety_snapshot": None,
            "previous_event_sha256": previous_hash,
        }
        value.update(overrides)
        value["event_sha256"] = validation._event_hash(value)
        return value

    def _authoritative(self, current, **overrides):
        lineage = {
            field: current[field]
            for field in (
                "champion_id",
                "strategy_version",
                "rules_hash",
                "plan_sha256",
                "upstream_artifact_hashes",
            )
        }
        value = {
            "lineage": lineage,
            "ledger_audit": {"valid": True},
            "ledger_report": {
                "maturity": {
                    "earned_maturity": "VALIDATED",
                    "validated_blockers": [],
                }
            },
            "registry_audit": {"valid": True},
            "strategy_audit": {"valid": True},
            "champion": {
                "alpha_state": "CONFIRMED",
                "execution_state": "LIVE_CALIBRATED",
                "operations_state": "READY",
                "readiness": "VALIDATED",
            },
            "lifecycle_audit": {"valid": True},
            "learning_data_audit": {"valid": True},
            "privacy_audit": {"valid": True},
            "progress_audit": {"valid": True},
            "store_capacity": {"capacity_ready": True},
            "git": {
                "valid": True,
                "clean": True,
                "head_equals_upstream": True,
            },
        }
        value.update(overrides)
        return value

    def test_phase_table_rejects_skipping_source_to_live(self):
        with self.assertRaisesRegex(
            validation.StrategyValidationError, "unsafe campaign phase transition"
        ):
            validation.validate_phase_transition("SOURCE_SEMANTICS", "LIVE_PILOT")

    def test_phase_table_allows_source_recovery_loop(self):
        validation.validate_phase_transition("SOURCE_SEMANTICS", "SOURCE_RECOVERY")
        validation.validate_phase_transition("SOURCE_RECOVERY", "SOURCE_SEMANTICS")

    def test_waiting_event_requires_blocker(self):
        event = self._event(status="WAITING_PROVIDER")
        with self.assertRaisesRegex(validation.StrategyValidationError, "blocker"):
            validation._validate_event(event, previous_hash="", expected_sequence=1)

    def test_safety_snapshot_rejects_identifier_fields(self):
        snapshot = self._safety(account_number="forbidden")
        with self.assertRaisesRegex(
            validation.StrategyValidationError, "forbidden fields"
        ):
            validation._validate_safety_snapshot(snapshot)

    def test_stale_safety_snapshot_blocks_completion(self):
        snapshot = self._safety(
            observed_at=(datetime.now(UTC) - timedelta(hours=1)).isoformat()
        )
        self.assertIn(
            "broker safety snapshot is stale",
            validation._safety_snapshot_blockers(snapshot),
        )

    def test_exposure_blocks_completion(self):
        snapshot = self._safety(broker_state="EXPOSED", positions_count=1)
        blockers = validation._safety_snapshot_blockers(snapshot)
        self.assertIn("broker safety snapshot is not reconciled flat", blockers)
        self.assertIn("broker safety snapshot has nonzero positions_count", blockers)

    def test_event_log_detects_content_tampering(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "events.jsonl"
            event = self._event()
            event["phase"] = "LIVE_PILOT"
            path.write_text(json.dumps(event) + "\n", encoding="utf-8")
            with self.assertRaisesRegex(
                validation.StrategyValidationError, "content hash"
            ):
                validation._load_events(path)

    def test_event_log_detects_broken_hash_chain(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "events.jsonl"
            first = self._event()
            second = self._event(sequence=2, previous_hash="wrong")
            path.write_text(
                json.dumps(first) + "\n" + json.dumps(second) + "\n",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(
                validation.StrategyValidationError, "hash chain"
            ):
                validation._load_events(path)

    def test_state_projection_must_match_last_event(self):
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
                validation.StrategyValidationError, "projection"
            ):
                validation._load_current(run_root)

    def test_manual_validated_record_is_refused(self):
        with self.assertRaisesRegex(
            validation.StrategyValidationError, "cannot be recorded manually"
        ):
            validation.record_transition(
                phase="VALIDATED",
                status="READY",
                objective="bypass",
                blocker="",
                next_action="none",
                evidence_paths=[],
                safety_path=None,
            )

    def test_negative_or_insufficient_maturity_cannot_finalize(self):
        current = self._event(safety_snapshot=self._safety())
        authoritative = self._authoritative(current)
        authoritative["ledger_report"]["maturity"] = {
            "earned_maturity": "UNVALIDATED",
            "validated_blockers": [
                "closed signals 0 is below required 50",
                "expectancy R is not above 0",
            ],
        }
        with (
            patch.object(
                validation, "_load_current", return_value=([current], current)
            ),
            patch.object(
                validation, "_authoritative_snapshot", return_value=authoritative
            ),
            patch.object(validation, "_append_event") as append,
        ):
            result = validation.audit_campaign()
        self.assertFalse(result["terminal"])
        self.assertIn("expectancy R is not above 0", result["finalization_blockers"])
        append.assert_not_called()

    def test_execution_budget_failure_cannot_finalize(self):
        current = self._event(safety_snapshot=self._safety())
        authoritative = self._authoritative(current)
        authoritative["ledger_report"]["maturity"] = {
            "earned_maturity": "UNVALIDATED",
            "validated_blockers": ["entry slippage p95 is missing or above budget"],
        }
        blockers = validation._finalization_blockers(current, authoritative)
        self.assertIn("entry slippage p95 is missing or above budget", blockers)

    def test_rule_or_capture_audit_failure_cannot_finalize(self):
        current = self._event(safety_snapshot=self._safety())
        authoritative = self._authoritative(current, ledger_audit={"valid": False})
        blockers = validation._finalization_blockers(current, authoritative)
        self.assertIn("ledger_audit is not valid", blockers)

    def test_stale_rules_binding_cannot_finalize(self):
        current = self._event(safety_snapshot=self._safety())
        authoritative = self._authoritative(current)
        authoritative["lineage"] = {
            **authoritative["lineage"],
            "rules_hash": "new-rules",
        }
        blockers = validation._finalization_blockers(current, authoritative)
        self.assertIn("campaign rules_hash is stale", blockers)

    def test_clean_authoritative_state_can_finalize(self):
        current = self._event(safety_snapshot=self._safety())
        authoritative = self._authoritative(current)
        terminal = self._event(
            sequence=2,
            previous_hash=current["event_sha256"],
            transition_kind="VALIDATED",
            phase="VALIDATED",
            active_objective="campaign-complete",
            next_action="",
            safety_snapshot=self._safety(),
        )
        with (
            patch.object(
                validation, "_load_current", return_value=([current], current)
            ),
            patch.object(
                validation, "_authoritative_snapshot", return_value=authoritative
            ),
            patch.object(validation, "_append_event", return_value=terminal) as append,
        ):
            result = validation.audit_campaign()
        self.assertTrue(result["terminal"])
        self.assertTrue(result["finalized_now"])
        append.assert_called_once()

    def test_next_emits_handoff_without_performing_action(self):
        current = self._event()
        with (
            patch.object(
                validation, "_load_current", return_value=([current], current)
            ),
            patch.object(validation, "_reset_for_lineage_change", return_value=None),
        ):
            result = validation.next_handoff()
        self.assertFalse(result["bounded_handoff"]["perform_by_controller"])
        self.assertIn(
            "catalyst_source_semantics.py", result["bounded_handoff"]["command"]
        )

    def test_version_change_restarts_development(self):
        current = self._event()
        new_lineage = {
            "champion_id": "strategy-v4",
            "strategy_version": "v4",
            "rules_hash": "rules-v4",
            "plan_sha256": "plan-v1",
            "upstream_artifact_hashes": {"AGENTS.md": "hash"},
        }
        with (
            patch.object(validation, "_lineage", return_value=new_lineage),
            patch.object(
                validation,
                "_append_event",
                return_value={"phase": "DEVELOPMENT_ACQUISITION"},
            ) as append,
        ):
            result = validation._reset_for_lineage_change(
                root=Path("."), run_root=Path("run"), current=current
            )
        self.assertEqual(result["phase"], "DEVELOPMENT_ACQUISITION")
        self.assertEqual(append.call_args.kwargs["transition_kind"], "VERSION_RESET")


if __name__ == "__main__":
    unittest.main()

import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import patch

from learning_cadence import (
    LearningCadenceError,
    audit_ledger_context_alignment,
    cadence_status,
    complete_task,
    load_state,
    run_due_tasks,
)
from learning_registry import append_event


class CadenceStateTests(unittest.TestCase):
    def test_new_state_has_every_task_due_and_no_authority(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "learning_runs" / "cadence-state.json"
            now = datetime(2026, 7, 18, 20, tzinfo=UTC)
            status = cadence_status(state_path=path, as_of=now)

            self.assertEqual(status["due_tasks"], 5)
            self.assertFalse(status["broker_actions_allowed"])
            self.assertFalse(status["provider_actions_allowed"])
            self.assertFalse(status["automatic_strategy_application"])

    def test_completion_is_atomic_and_task_becomes_not_due(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "learning_runs" / "cadence-state.json"
            now = datetime(2026, 7, 18, 20, tzinfo=UTC)
            with patch("learning_cadence.PROJECT_ROOT", Path(directory)):
                complete_task(
                    "daily_integrity",
                    "completed",
                    state_path=path,
                    completed_at=now,
                )
            status = cadence_status(state_path=path, as_of=now + timedelta(hours=1))

            daily = next(
                item for item in status["tasks"] if item["task"] == "daily_integrity"
            )
            self.assertFalse(daily["due"])
            self.assertEqual(
                load_state(path)["completions"]["daily_integrity"]["outcome"],
                "completed",
            )

    def test_unsafe_evidence_path_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "learning_runs" / "cadence-state.json"
            with self.assertRaisesRegex(LearningCadenceError, "repository relative"):
                complete_task(
                    "weekly_hypothesis_review",
                    "blocked",
                    evidence_paths=["../private.json"],
                    state_path=path,
                )


class CadenceExecutionTests(unittest.TestCase):
    def test_ledger_context_alignment_requires_matching_public_truth(self):
        record = {
            "record_type": "signal",
            "signal_id": "2026-07-18-XYZ-1",
            "date": "2026-07-18",
            "mode": "shadow",
            "strategy_version": "v1",
            "rules_hash": "hash",
            "decision": "rejected",
            "features": {"score": 90.0},
        }
        outcome = {
            "context_id": "2026-07-18-XYZ-1",
            "date": "2026-07-18",
            "mode": "shadow",
            "strategy_version": "v1",
            "rules_hash": "hash",
            "context_kind": "trade_idea",
            "result": "rejected",
            "metrics": {"score": 90.0},
        }

        aligned = audit_ledger_context_alignment([record], [outcome])
        drifted = audit_ledger_context_alignment(
            [record], [{**outcome, "mode": "live"}]
        )
        orphaned = audit_ledger_context_alignment([record], [])

        self.assertTrue(aligned["valid"])
        self.assertFalse(drifted["valid"])
        self.assertTrue(any("mode" in item for item in drifted["violations"]))
        self.assertFalse(orphaned["valid"])
        self.assertTrue(
            any("no archived context" in item for item in orphaned["violations"])
        )

    def test_deterministic_tasks_complete_and_agent_task_stays_due(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "learning_runs" / "cadence-state.json"
            now = datetime(2026, 7, 18, 20, tzinfo=UTC)
            with patch("learning_cadence.PROJECT_ROOT", Path(directory)):
                with patch(
                    "learning_cadence._run_integrity", return_value={"valid": True}
                ):
                    result = run_due_tasks(
                        max_tasks=3,
                        state_path=path,
                        research_lock_path=Path(directory) / "missing-lock.json",
                        registry_root=Path(directory) / "learning",
                        as_of=now,
                    )

            self.assertEqual(result["results"][0]["task"], "daily_integrity")
            self.assertEqual(result["results"][0]["status"], "completed")
            self.assertEqual(result["results"][1]["task"], "nightly_frozen_collection")
            self.assertEqual(result["results"][1]["status"], "no_op")
            self.assertEqual(result["results"][2]["task"], "weekly_hypothesis_review")
            self.assertEqual(result["results"][2]["status"], "needs_agent")
            weekly = next(
                item
                for item in result["remaining"]["tasks"]
                if item["task"] == "weekly_hypothesis_review"
            )
            self.assertTrue(weekly["due"])

    def test_active_research_lock_no_ops_weekly_invention(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            state_path = root / "learning_runs" / "cadence-state.json"
            lock_path = root / "RESEARCH_LOCK.json"
            lock_path.write_text(
                '{"schema_version":1,"active":true,'
                '"blocked_dataset_lane":"catalyst_falsification",'
                '"required_dataset_id":"dataset-scanner-replay",'
                '"required_dataset_inspected":true,'
                '"required_dataset_lane":"production_scanner_replay",'
                '"required_dataset_status":"READY",'
                '"evidence_path":"collection-status.json",'
                '"reason":"scanner fidelity first"}\n',
                encoding="utf-8",
            )
            now = datetime(2026, 7, 18, 20, tzinfo=UTC)
            with patch("learning_cadence.PROJECT_ROOT", root):
                with patch(
                    "learning_cadence._run_integrity", return_value={"valid": True}
                ):
                    result = run_due_tasks(
                        max_tasks=3,
                        state_path=state_path,
                        research_lock_path=lock_path,
                        registry_root=root / "learning",
                        as_of=now,
                    )

            weekly = result["results"][2]
            self.assertEqual(weekly["task"], "weekly_hypothesis_review")
            self.assertEqual(weekly["status"], "no_op")
            self.assertEqual(weekly["reason"], "research_fidelity_lock")
            self.assertEqual(weekly["required_dataset_id"], "dataset-scanner-replay")

    def test_collecting_dataset_remains_a_nightly_agent_handoff(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            registry_root = root / "learning"
            append_event(
                "datasets",
                {
                    "schema_version": 1,
                    "event_id": "dataset-scanner-registered",
                    "entity_id": "dataset-scanner",
                    "event_type": "registered",
                    "recorded_at": "2026-07-18T20:00:00-04:00",
                    "payload": {
                        "lane": "development",
                        "status": "COLLECTING",
                        "evidence_paths": ["manifest.json"],
                        "inspected": False,
                    },
                },
                registry_root,
            )
            now = datetime(2026, 7, 18, 20, tzinfo=UTC)
            with patch("learning_cadence.PROJECT_ROOT", root):
                with patch(
                    "learning_cadence._run_integrity", return_value={"valid": True}
                ):
                    result = run_due_tasks(
                        max_tasks=2,
                        state_path=root / "learning_runs" / "cadence-state.json",
                        research_lock_path=root / "missing-lock.json",
                        registry_root=registry_root,
                        as_of=now,
                    )

            nightly = result["results"][1]
            self.assertEqual(nightly["status"], "needs_agent")
            self.assertEqual(nightly["pending_datasets"], ["dataset-scanner"])

    def test_corrupt_authority_state_fails_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "cadence-state.json"
            path.write_text(
                '{"schema_version":1,"automatic_strategy_application":true,"broker_actions_allowed":false,"provider_actions_allowed":false,"completions":{}}',
                encoding="utf-8",
            )
            with self.assertRaisesRegex(LearningCadenceError, "authorize"):
                load_state(path)


if __name__ == "__main__":
    unittest.main()

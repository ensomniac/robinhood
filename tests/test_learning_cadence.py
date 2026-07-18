import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import patch

from learning_cadence import (
    LearningCadenceError,
    cadence_status,
    complete_task,
    load_state,
    run_due_tasks,
)


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
    def test_deterministic_tasks_complete_and_agent_task_stays_due(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "learning_runs" / "cadence-state.json"
            now = datetime(2026, 7, 18, 20, tzinfo=UTC)
            with patch("learning_cadence.PROJECT_ROOT", Path(directory)):
                with patch(
                    "learning_cadence._run_integrity", return_value={"valid": True}
                ):
                    result = run_due_tasks(max_tasks=3, state_path=path, as_of=now)

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

import tempfile
import unittest
from pathlib import Path

from learning_loop import (
    LearningLoopError,
    audit_program,
    inspect_repository,
    load_prompt,
    review_change_plan,
)


class LearningPromptTests(unittest.TestCase):
    def test_public_prompt_is_versioned_ordered_and_bounded(self):
        contract = load_prompt()

        self.assertEqual(contract["version"], "2026-08-07-v1")
        self.assertEqual(len(contract["sections"]), 5)
        self.assertTrue(contract["valid"])

    def test_prompt_missing_a_required_boundary_fails_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "prompt.md"
            path.write_text(
                Path("LEARNING_LOOP.md")
                .read_text(encoding="utf-8")
                .replace("No active strategy", "No omitted boundary"),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(LearningLoopError, "No active strategy"):
                load_prompt(path)


class ChangePlanTests(unittest.TestCase):
    def test_safe_data_slice_is_bounded_and_non_executing(self):
        result = review_change_plan(
            "Improve refresh diagnostics",
            ["historical_data_cli.py", "tests/test_historical_data_cli.py"],
        )

        self.assertEqual(result["status"], "approved_for_bounded_engineering_review")
        self.assertFalse(result["strategy_allowed"])
        self.assertFalse(result["broker_actions_allowed"])
        self.assertFalse(result["external_writes_allowed"])
        self.assertEqual(len(result["phases"]), 4)

    def test_strategy_and_frozen_history_paths_are_refused(self):
        with self.assertRaisesRegex(LearningLoopError, "strategy surface"):
            review_change_plan("Restore live code", ["strategy_engine.py"])
        with self.assertRaisesRegex(LearningLoopError, "frozen preservation"):
            review_change_plan(
                "Rewrite evidence", ["history/legacy/SIGNALS.jsonl"]
            )

    def test_external_scope_is_refused(self):
        with self.assertRaisesRegex(LearningLoopError, "repository-relative"):
            review_change_plan("Edit outside", ["../outside.py"])


class RepositoryAuditTests(unittest.TestCase):
    def test_inspection_is_explicitly_not_a_full_data_audit(self):
        result = inspect_repository()

        self.assertTrue(result["valid"])
        self.assertFalse(
            result["historical_store"]["full_document_audit_performed"]
        )
        self.assertFalse(result["safety"]["live_trading_enabled"])
        self.assertEqual(result["safety"]["broker_action_modes"], [])
        self.assertEqual(result["safety"]["active_strategy_files"], [])

    def test_clean_slate_program_audit_passes(self):
        result = audit_program()

        self.assertTrue(result["valid"])
        self.assertFalse(result["live_trading_enabled"])
        self.assertEqual(result["active_strategy_files"], [])
        self.assertTrue(result["preservation"]["valid"])


if __name__ == "__main__":
    unittest.main()

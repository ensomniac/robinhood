import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from progress_history import (
    ProgressHistoryError,
    append_entry,
    check_protected_history,
    contribution_required,
    load_history,
    validate_entry,
)


def entry(identifier="2026-07-15-example"):
    return {
        "schema_version": 1,
        "id": identifier,
        "recorded_at": "2026-07-15T23:00:00-04:00",
        "category": "reliability",
        "title": "Example finding",
        "summary": "A reusable observation.",
        "findings": ["The exact evidence."],
        "impact": ["The future workflow improves."],
        "follow_ups": [],
        "related_files": ["example.py"],
    }


class EntryValidationTests(unittest.TestCase):
    def test_accepts_structured_entry(self):
        self.assertEqual(validate_entry(entry())["id"], "2026-07-15-example")

    def test_rejects_absolute_related_path(self):
        value = entry()
        value["related_files"] = ["/private/example.py"]

        with self.assertRaisesRegex(ProgressHistoryError, "repository-relative"):
            validate_entry(value)

    def test_append_is_jsonl_and_duplicate_safe(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "HISTORY.jsonl"
            append_entry(entry(), path)
            stored = load_history(path)

            self.assertEqual(len(stored), 1)
            self.assertEqual(json.loads(path.read_text())["id"], "2026-07-15-example")
            with self.assertRaisesRegex(ProgressHistoryError, "duplicate id"):
                append_entry(entry(), path)


class ContributionHookTests(unittest.TestCase):
    def test_source_or_operating_docs_require_history_contribution(self):
        self.assertTrue(contribution_required(["historical_data_cli.py"]))
        self.assertTrue(contribution_required(["AGENTS.md"]))
        self.assertTrue(contribution_required(["progress_history.py"]))
        self.assertTrue(contribution_required(["history/ACCOUNT_HISTORY.md"]))

    def test_progress_tests_and_license_are_exempt(self):
        self.assertFalse(
            contribution_required(
                ["progress/HISTORY.jsonl", "tests/test_example.py", "LICENSE"]
            )
        )

    def test_frozen_legacy_ledger_change_is_rejected(self):
        def fake_git(*args):
            path = args[-1]
            if path == "history/legacy/SIGNALS.jsonl":
                return path + "\n"
            return ""

        with patch("progress_history._git", side_effect=fake_git):
            with self.assertRaisesRegex(ProgressHistoryError, "cannot change"):
                check_protected_history()

    def test_outcome_exposure_deletion_is_rejected(self):
        def fake_git(*args):
            if "--numstat" in args and args[-1] == (
                "history/OUTCOME_EXPOSURE_INDEX.jsonl"
            ):
                return "0\t1\thistory/OUTCOME_EXPOSURE_INDEX.jsonl\n"
            return ""

        with patch("progress_history._git", side_effect=fake_git):
            with self.assertRaisesRegex(ProgressHistoryError, "append-only"):
                check_protected_history()


if __name__ == "__main__":
    unittest.main()

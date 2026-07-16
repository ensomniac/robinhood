import json
import tempfile
import unittest
from pathlib import Path

from progress_history import (
    ProgressHistoryError,
    append_entry,
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
        self.assertTrue(contribution_required(["strategy_engine.py"]))
        self.assertTrue(contribution_required(["AGENTS.md"]))
        self.assertTrue(contribution_required(["progress_history.py"]))

    def test_generated_and_history_paths_are_exempt(self):
        self.assertFalse(contribution_required(["TRADES.md", "trades/active/a.md"]))
        self.assertFalse(
            contribution_required(["progress/HISTORY.jsonl", "tests/test_example.py"])
        )


if __name__ == "__main__":
    unittest.main()

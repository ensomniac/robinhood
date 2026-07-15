import tempfile
import unittest
from datetime import date
from pathlib import Path

from strategy_engine import load_config
from trade_lifecycle import (
    LifecycleError,
    archive_context,
    audit_lifecycle,
    extract_outcome,
    load_archived_outcomes,
    prepare_outcome,
)


def outcome_payload(day="2026-07-15"):
    config = load_config()
    return {
        "context_id": f"{day}-XYZ-1",
        "date": day,
        "context_kind": "trade",
        "mode": "shadow",
        "result": "success",
        "summary": "The frozen setup reached its planned structural exit.",
        "primary_reason": "two_percent_milestone",
        "thesis_result": "held",
        "what_worked": ["Catalyst and opening volume stayed supportive."],
        "what_failed": [],
        "lessons": ["Keep this as one observation until the review cadence."],
        "next_time": ["Collect another complete session."],
        "metrics": {"net_r": 2.1},
        "strategy_version": config.version,
        "rules_hash": config.rules_hash,
    }


class ArchiveTests(unittest.TestCase):
    def test_close_embeds_outcome_and_moves_to_daily_archive(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            active = root / "trades" / "active"
            archive = root / "trades" / "archived"
            active.mkdir(parents=True)
            source = active / "2026-07-15-XYZ-1.md"
            source.write_text("# Trade Context\n", encoding="utf-8")

            destination = archive_context(
                source,
                outcome_payload(),
                active_root=active,
                archive_root=archive,
                today_et=date(2026, 7, 15),
            )
            embedded = extract_outcome(destination.read_text(encoding="utf-8"))
            audit = audit_lifecycle(
                active_root=active,
                archive_root=archive,
                today_et=date(2026, 7, 15),
            )
            outcomes = load_archived_outcomes(archive)

        self.assertFalse(source.exists())
        self.assertEqual(destination.parent.name, "2026_07_15")
        self.assertEqual(embedded["context_id"], "2026-07-15-XYZ-1")
        self.assertEqual(embedded["metrics"]["net_r"], 2.1)
        self.assertTrue(audit.valid)
        self.assertEqual(len(outcomes), 1)

    def test_noncurrent_archive_requires_explicit_historical_mode(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            active = root / "active"
            active.mkdir()
            source = active / "2026-07-15-XYZ-1.md"
            source.write_text("# Context\n", encoding="utf-8")

            with self.assertRaisesRegex(LifecycleError, "only today's"):
                archive_context(
                    source,
                    outcome_payload(),
                    active_root=active,
                    archive_root=root / "archive",
                    today_et=date(2026, 7, 16),
                )

    def test_outcome_rejects_private_identifiers(self):
        payload = outcome_payload()
        payload["summary"] = "Order 123e4567-e89b-42d3-a456-426614174000 worked"

        with self.assertRaisesRegex(LifecycleError, "UUID-shaped"):
            prepare_outcome(payload)

    def test_trade_result_must_match_net_r(self):
        payload = outcome_payload()
        payload["metrics"]["net_r"] = -1.0

        with self.assertRaisesRegex(LifecycleError, "successful trade"):
            prepare_outcome(payload)


class AuditTests(unittest.TestCase):
    def test_stale_active_context_is_reported(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            active = root / "active"
            active.mkdir()
            (active / "2026-07-14-session.md").write_text(
                "# Session\n", encoding="utf-8"
            )

            audit = audit_lifecycle(
                active_root=active,
                archive_root=root / "archive",
                today_et=date(2026, 7, 15),
            )

        self.assertFalse(audit.valid)
        self.assertIn("stale active context", audit.violations[0])

    def test_archive_without_outcome_is_reported(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            archived = root / "archive" / "2026_07_15"
            archived.mkdir(parents=True)
            (archived / "2026-07-15-session.md").write_text(
                "# Incomplete\n", encoding="utf-8"
            )

            audit = audit_lifecycle(
                active_root=root / "active",
                archive_root=root / "archive",
                today_et=date(2026, 7, 15),
            )

        self.assertFalse(audit.valid)
        self.assertIn("no embedded terminal outcome", audit.violations[0])

    def test_unexpected_active_file_type_is_reported(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            active = root / "active"
            active.mkdir()
            (active / "scratch.json").write_text("{}\n", encoding="utf-8")

            audit = audit_lifecycle(
                active_root=active,
                archive_root=root / "archive",
                today_et=date(2026, 7, 15),
            )

        self.assertFalse(audit.valid)
        self.assertIn("direct Markdown", audit.violations[0])


if __name__ == "__main__":
    unittest.main()

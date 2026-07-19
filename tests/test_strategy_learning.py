import json
import tempfile
import unittest
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import patch

from strategy_engine import load_config
from strategy_learning import (
    LearningError,
    assess_review_cadence,
    build_learning_report,
    write_proposal,
)
from strategy_ledger import prepare_record


def closed_signal(index, *, high_quality):
    day = date(2026, 7, 16) + timedelta(days=index)
    symbol = f"S{index:02d}"
    net_r = 1.0 if high_quality else -0.5
    return prepare_record(
        {
            "record_type": "signal",
            "signal_id": f"{day.isoformat()}-{symbol}-1",
            "session_id": f"{day.isoformat()}-session",
            "date": day.isoformat(),
            "symbol": symbol,
            "mode": "shadow",
            "sample_phase": "pilot",
            "session_capture_complete": True,
            "triggered": True,
            "eligible": True,
            "decision": "shadow",
            "closed": True,
            "net_r": net_r,
            "net_pnl_dollars": net_r * 100,
            "project_exit_net_r": net_r,
            "paper_baseline_eligible": True,
            "paper_eod_shadow_net_r": net_r,
            "entry_slippage_bps": None,
            "unprotected_seconds": None,
            "stop_executed": False,
            "stop_slippage_bps": None,
            "stop_reserve_bps": None,
            "rule_violations": [],
            "rejection_reasons": [],
            "features": {
                "opening_relative_volume": 5.0 if high_quality else 1.5,
                "score": 96 if high_quality else 92,
            },
        }
    )


def rejected_signal(index, reasons):
    day = date(2026, 7, 16) + timedelta(days=index)
    symbol = f"R{index:02d}"
    return prepare_record(
        {
            "record_type": "signal",
            "signal_id": f"{day.isoformat()}-{symbol}-1",
            "session_id": f"{day.isoformat()}-session",
            "date": day.isoformat(),
            "symbol": symbol,
            "mode": "shadow",
            "sample_phase": "pilot",
            "session_capture_complete": True,
            "triggered": True,
            "eligible": False,
            "decision": "rejected",
            "closed": False,
            "net_r": None,
            "paper_baseline_eligible": False,
            "entry_slippage_bps": None,
            "unprotected_seconds": None,
            "stop_executed": False,
            "stop_slippage_bps": None,
            "stop_reserve_bps": None,
            "rule_violations": [],
            "rejection_reasons": reasons,
            "features": {},
        }
    )


class CadenceTests(unittest.TestCase):
    def test_review_requires_both_signals_and_elapsed_time(self):
        records = [
            closed_signal(index, high_quality=index >= 10) for index in range(20)
        ]
        with tempfile.TemporaryDirectory() as directory:
            blocked = assess_review_cadence(
                records,
                proposal_root=Path(directory),
                as_of=date(2026, 8, 1),
            )
            ready = assess_review_cadence(
                records,
                proposal_root=Path(directory),
                as_of=date(2026, 8, 20),
            )

        self.assertFalse(blocked.eligible)
        self.assertTrue(ready.eligible)

    def test_report_generates_hypothesis_but_never_applies_it(self):
        records = [
            closed_signal(index, high_quality=index >= 10) for index in range(20)
        ]
        with tempfile.TemporaryDirectory() as directory:
            proposal_root = Path(directory)
            report = build_learning_report(
                records,
                [],
                proposal_root=proposal_root,
                as_of=date(2026, 8, 20),
                research_lock_path=proposal_root / "missing-lock.json",
                registry_root=proposal_root,
            )
            path = write_proposal(
                report,
                proposal_root,
                research_lock_path=proposal_root / "missing-lock.json",
                registry_root=proposal_root,
            )
            proposal = json.loads(path.read_text(encoding="utf-8"))

        categories = {item["category"] for item in report["hypotheses"]}
        self.assertEqual(report["status"], "review_ready")
        self.assertFalse(report["automatic_application"])
        self.assertIn("opening_relative_volume", categories)
        self.assertTrue(path.name.endswith("-strategy-review.json"))
        self.assertFalse(proposal["requires_user_approval"])
        self.assertTrue(proposal["delegated_application_decision"])
        self.assertTrue(proposal["requires_separate_production_change_workflow"])
        self.assertEqual(load_config().version, report["strategy_version"])

    def test_blocked_report_cannot_be_written_as_proposal(self):
        report = build_learning_report([], [], as_of=date(2026, 8, 20))

        with self.assertRaisesRegex(LearningError, "proposal is blocked"):
            write_proposal(report, Path("unused"))

    def test_active_catalyst_research_lock_blocks_hypothesis_proposal(self):
        records = [
            closed_signal(index, high_quality=index >= 10) for index in range(20)
        ]
        lock = {
            "blocks_new_hypotheses": True,
            "blocked_dataset_lane": "catalyst_falsification",
            "required_dataset_id": "dataset-scanner-expansion",
            "reason": "scanner expansion must be independently READY",
        }
        with tempfile.TemporaryDirectory() as directory, patch(
            "strategy_learning.research_lock_status", return_value=lock
        ):
            report = build_learning_report(
                records,
                [],
                proposal_root=Path(directory),
                as_of=date(2026, 8, 20),
            )

            with self.assertRaisesRegex(LearningError, "research_locked"):
                write_proposal(report, Path(directory))

        self.assertEqual(report["status"], "research_locked")
        self.assertEqual(report["hypotheses"], [])
        self.assertEqual(
            report["research_lock"]["required_dataset_id"],
            "dataset-scanner-expansion",
        )

    def test_stale_unlocked_report_is_rechecked_before_write(self):
        records = [
            closed_signal(index, high_quality=index >= 10) for index in range(20)
        ]
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            report = build_learning_report(
                records,
                [],
                proposal_root=root,
                as_of=date(2026, 8, 20),
                research_lock_path=root / "missing-lock.json",
                registry_root=root,
            )
            active_lock = {
                "blocks_new_hypotheses": True,
                "blocked_dataset_lane": "catalyst_falsification",
                "reason": "new lock activated after report generation",
            }
            with patch(
                "strategy_learning.research_lock_status", return_value=active_lock
            ), self.assertRaisesRegex(LearningError, "new lock activated"):
                write_proposal(report, root)

        self.assertEqual(report["status"], "review_ready")

    def test_malformed_rejected_return_cannot_unlock_review(self):
        malformed = dict(closed_signal(0, high_quality=True))
        malformed.update({"eligible": False, "decision": "rejected"})
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            report = build_learning_report(
                [malformed] * 20,
                [],
                proposal_root=root,
                as_of=date(2026, 8, 20),
                research_lock_path=root / "missing-lock.json",
                registry_root=root,
            )

        self.assertEqual(report["cadence"]["current_closed_signals"], 0)
        self.assertEqual(report["status"], "cadence_blocked")

    def test_report_exposes_pre_session_universe_waste(self):
        records = [
            rejected_signal(0, ["average daily volume is below the universe minimum"]),
            rejected_signal(1, ["daily ATR is below the universe minimum"]),
            rejected_signal(2, ["opening relative volume is below 1.0"]),
            rejected_signal(3, ["security is not a U.S.-listed common stock"]),
        ]

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            report = build_learning_report(
                records,
                [],
                as_of=date(2026, 8, 20),
                research_lock_path=root / "missing-lock.json",
                registry_root=root,
            )
        diagnostics = report["signal_diagnostics"]

        self.assertEqual(report["schema_version"], 2)
        self.assertEqual(diagnostics["signals"], 4)
        self.assertEqual(diagnostics["pre_session_universe_rejects"], 3)
        self.assertEqual(diagnostics["daily_metric_preflight_rejects"], 2)
        self.assertAlmostEqual(
            diagnostics["pre_session_universe_reject_fraction"], 3 / 4
        )
        self.assertIn("enforce pre-session universe gates", report["recommendation"])


if __name__ == "__main__":
    unittest.main()

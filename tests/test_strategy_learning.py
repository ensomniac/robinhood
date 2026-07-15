import tempfile
import unittest
from datetime import date, timedelta
from pathlib import Path

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
            )
            path = write_proposal(report, proposal_root)

        categories = {item["category"] for item in report["hypotheses"]}
        self.assertEqual(report["status"], "review_ready")
        self.assertFalse(report["automatic_application"])
        self.assertIn("opening_relative_volume", categories)
        self.assertTrue(path.name.endswith("-strategy-review.json"))
        self.assertEqual(load_config().version, report["strategy_version"])

    def test_blocked_report_cannot_be_written_as_proposal(self):
        report = build_learning_report([], [], as_of=date(2026, 8, 20))

        with self.assertRaisesRegex(LearningError, "cadence is blocked"):
            write_proposal(report, Path("unused"))


if __name__ == "__main__":
    unittest.main()

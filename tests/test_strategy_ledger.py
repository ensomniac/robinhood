import tempfile
import unittest
from pathlib import Path

from strategy_engine import load_config
from strategy_ledger import (
    LedgerError,
    append_record,
    append_records,
    audit_ledger,
    build_report,
    read_records,
)


def session_payload(day="2026-07-16", trade_taken=False):
    return {
        "record_type": "session",
        "session_id": f"{day}-session",
        "date": day,
        "mode": "shadow",
        "sample_phase": "pilot",
        "closed": True,
        "session_capture_complete": True,
        "trade_taken": trade_taken,
        "candidate_count": 3,
        "triggered_signal_count": 1 if trade_taken else 0,
        "no_trade_reason": "" if trade_taken else "No hard-gate pass",
        "rule_violations": [],
    }


def signal_payload(day="2026-07-16", symbol="XYZ", mode="shadow"):
    return {
        "record_type": "signal",
        "signal_id": f"{day}-{symbol}-1",
        "session_id": f"{day}-session",
        "date": day,
        "symbol": symbol,
        "mode": mode,
        "sample_phase": "pilot",
        "session_capture_complete": True,
        "triggered": True,
        "eligible": True,
        "decision": mode,
        "closed": True,
        "net_r": 1.0,
        "net_pnl_dollars": 100.0,
        "project_exit_net_r": 1.0,
        "paper_baseline_eligible": True,
        "paper_eod_shadow_net_r": 1.5,
        "entry_slippage_bps": 5.0 if mode == "live" else None,
        "unprotected_seconds": 4.0 if mode == "live" else None,
        "stop_executed": False,
        "stop_slippage_bps": None,
        "stop_reserve_bps": None,
        "rule_violations": [],
        "rejection_reasons": [],
    }


class AppendAndAuditTests(unittest.TestCase):
    def test_append_adds_current_version_hash_and_is_readable(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "signals.jsonl"
            payload = session_payload()
            payload["candidate_count"] = 0
            payload["triggered_signal_count"] = 0
            appended = append_record(payload, path)
            records = read_records(path)
            audit = audit_ledger(path)

        self.assertEqual(appended["strategy_version"], load_config().version)
        self.assertEqual(appended["rules_hash"], load_config().rules_hash)
        self.assertEqual(records, [appended])
        self.assertTrue(audit.valid)

    def test_duplicate_public_id_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "signals.jsonl"
            append_record(session_payload(), path)

            with self.assertRaisesRegex(LedgerError, "duplicate"):
                append_record(session_payload(), path)

    def test_uuid_or_identifier_fields_are_rejected(self):
        payload = signal_payload()
        payload["notes"] = "123e4567-e89b-42d3-a456-426614174000"

        with self.assertRaisesRegex(LedgerError, "UUID-shaped"):
            append_record(payload, Path("unused.jsonl"))

        payload = signal_payload()
        payload["broker_order_id"] = "enc:fernet:v1:opaque"
        with self.assertRaisesRegex(LedgerError, "forbidden"):
            append_record(payload, Path("unused.jsonl"))

    def test_audit_reports_malformed_json(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "signals.jsonl"
            path.write_text("{not-json}\n", encoding="utf-8")

            audit = audit_ledger(path)

        self.assertFalse(audit.valid)
        self.assertIn("invalid JSON", audit.violations[0])

    def test_audit_requires_one_coherent_session_signal_group(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "signals.jsonl"
            session = session_payload(trade_taken=True)
            session["candidate_count"] = 1
            append_records([session, signal_payload()], path)

            audit = audit_ledger(path)

        self.assertTrue(audit.valid)

    def test_audit_rejects_orphans_and_session_count_disagreement(self):
        with tempfile.TemporaryDirectory() as directory:
            orphan_path = Path(directory) / "orphan.jsonl"
            append_record(signal_payload(), orphan_path)
            orphan = audit_ledger(orphan_path)

            mismatch_path = Path(directory) / "mismatch.jsonl"
            append_records(
                [session_payload(trade_taken=True), signal_payload()], mismatch_path
            )
            mismatch = audit_ledger(mismatch_path)

        self.assertFalse(orphan.valid)
        self.assertTrue(
            any("exactly one session record" in item for item in orphan.violations)
        )
        self.assertFalse(mismatch.valid)
        self.assertTrue(
            any("candidate_count" in item for item in mismatch.violations)
        )

    def test_closed_trigger_requires_paired_eod_outcome(self):
        payload = signal_payload()
        payload["paper_eod_shadow_net_r"] = None

        with self.assertRaisesRegex(LedgerError, "paper_eod_shadow_net_r"):
            append_record(payload, Path("unused.jsonl"))

    def test_ineligible_or_mode_mismatched_return_is_rejected(self):
        payload = signal_payload()
        payload["eligible"] = False
        payload["decision"] = "rejected"

        with self.assertRaisesRegex(LedgerError, "closed triggered return"):
            append_record(payload, Path("unused.jsonl"))

        payload = signal_payload(mode="live")
        payload["decision"] = "shadow"
        with self.assertRaisesRegex(LedgerError, "must match mode"):
            append_record(payload, Path("unused.jsonl"))


class ReportTests(unittest.TestCase):
    def test_report_calculates_no_trade_and_paired_exit_metrics(self):
        config = load_config()
        session = session_payload()
        signal = signal_payload()
        for record in (session, signal):
            record["schema_version"] = 1
            record["strategy_version"] = config.version
            record["rules_hash"] = config.rules_hash

        report = build_report([session, signal], config)

        self.assertEqual(report["session_metrics"]["no_trade_frequency"], 1.0)
        self.assertEqual(report["exit_overlay"]["paired_signals"], 1)
        self.assertEqual(report["exit_overlay"]["mean_project_minus_paper_r"], -0.5)
        self.assertEqual(report["maturity"]["earned_maturity"], "UNVALIDATED")

    def test_report_blocks_same_version_with_mixed_rules(self):
        config = load_config()
        signal = signal_payload()
        signal["schema_version"] = 1
        signal["strategy_version"] = config.version
        signal["rules_hash"] = "stale-rules"

        report = build_report([signal], config)

        self.assertEqual(report["maturity"]["metrics"]["mismatched_rule_records"], 1)
        self.assertIn(
            "one or more records use a different rules hash",
            report["maturity"]["provisional_blockers"],
        )


if __name__ == "__main__":
    unittest.main()

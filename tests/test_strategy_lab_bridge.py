from __future__ import annotations

import tempfile
import unittest
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import patch

from strategy_lab.bridge import BridgeError, BridgeWorker, sign_payload, verify_envelope
from strategy_lab.cli import _research_progress_publisher
from strategy_lab.contracts import CandidateState, StrategySpec
from strategy_lab.database import LabDatabase
from strategy_lab.live import CodexMCPExecutor, LiveBridgeError

from tests.strategy_lab_helpers import make_test_config


def candidate_spec() -> StrategySpec:
    return StrategySpec(
        strategy_id="strategy-pilot-ready-test",
        family_id="family-pilot-ready-test",
        idea_id="idea-pilot-ready-test",
        horizon="daily",
        signal={
            "op": "gte",
            "left": {"feature": "return_5d"},
            "right": {"value": 0.05},
        },
        rank_by="return_5d",
        rank_direction="desc",
        entry="next_open",
        stop_loss_pct=0.01,
        target_pct=0.02,
        maximum_hold_sessions=2,
        round_trip_bps=10,
        causal_thesis="Synthetic candidate proves the exact rules-hash arm boundary.",
        falsifier="Any mismatched hash or maturity state must reject arming.",
        parameters={"momentum": 0.05, "stop": 0.01, "target": 0.02},
    )


class StrategyLabBridgeTests(unittest.TestCase):
    def test_manual_research_progress_uses_signed_dashboard_publisher(self):
        with tempfile.TemporaryDirectory() as directory:
            config = make_test_config(Path(directory))
            with LabDatabase(config) as database:
                snapshot = {"schema_version": 1, "run": {"status": "RUNNING"}}
                with (
                    patch("strategy_lab.cli.SmartSiouxClient") as client_type,
                    patch(
                        "strategy_lab.cli.build_and_write_snapshot",
                        return_value=snapshot,
                    ),
                ):
                    client_type.return_value.configured.return_value = True
                    publish = _research_progress_publisher(config, database)
                    publish("daily-test")
                    client_type.return_value.publish.assert_called_once_with(snapshot)

    def test_signed_envelope_detects_tampering_and_staleness(self):
        secret = "test-secret"
        timestamp = datetime.now(UTC).isoformat()
        nonce = "nonce"
        payload = '{"command":"none"}'
        envelope = {
            "timestamp": timestamp,
            "nonce": nonce,
            "payload": payload,
            "signature": sign_payload(secret, timestamp, nonce, payload),
        }
        self.assertEqual(
            verify_envelope(envelope, secret=secret, maximum_clock_skew_seconds=300)[
                "command"
            ],
            "none",
        )
        envelope["payload"] = '{"command":"tampered"}'
        with self.assertRaisesRegex(BridgeError, "signature"):
            verify_envelope(envelope, secret=secret, maximum_clock_skew_seconds=300)

    def test_only_exact_pilot_ready_hash_can_be_armed(self):
        with tempfile.TemporaryDirectory() as directory:
            config = make_test_config(Path(directory))
            with LabDatabase(config) as database:
                spec = candidate_spec()
                database.register_spec(spec)
                database.upsert_candidate(
                    spec,
                    state=CandidateState.PILOT_READY,
                    reason="test evidence",
                )
                executor = CodexMCPExecutor(config, database)
                expires = (datetime.now(UTC) + timedelta(minutes=5)).isoformat()
                result = executor.arm(spec.strategy_id, spec.rules_sha256, expires)
                self.assertEqual(result["status"], "ARMED")
                executor.disarm(spec.strategy_id)
                with self.assertRaisesRegex(LiveBridgeError, "rules hash"):
                    executor.arm(spec.strategy_id, "0" * 64, expires)

    def test_command_allowlist_rejects_shell_fields_and_non_operator(self):
        with tempfile.TemporaryDirectory() as directory:
            config = make_test_config(Path(directory))
            with patch.dict(
                "os.environ",
                {"STRATEGY_LAB_OPERATOR_EMAIL": "ryan@example.com"},
                clear=False,
            ):
                with LabDatabase(config) as database:
                    worker = BridgeWorker(config, database)
                    now = datetime.now(UTC)
                    command = {
                        "schema_version": 1,
                        "command_id": "command-test-one",
                        "command_type": "pause_scheduler",
                        "requested_by": "ryan@example.com",
                        "requested_at": now.isoformat(),
                        "expires_at": (now + timedelta(minutes=5)).isoformat(),
                        "payload": {"shell": "rm -rf /"},
                    }
                    result = worker.process_command(command)
                    self.assertEqual(result["status"], "FAILED")
                    self.assertIn("forbidden", result["result"]["error"])
                    command["command_id"] = "command-test-two"
                    command["payload"] = {}
                    command["requested_by"] = "someone@example.com"
                    result = worker.process_command(command)
                    self.assertEqual(result["status"], "FAILED")
                    self.assertIn("operator", result["result"]["error"])


if __name__ == "__main__":
    unittest.main()

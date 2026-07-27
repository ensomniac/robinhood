"""Signed SmartSioux snapshot publication and typed command execution."""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import secrets
import time
from datetime import UTC, datetime, timedelta
from typing import Any

import requests

from .config import LabConfig
from .contracts import CandidateState
from .database import LabDatabase, utc_now
from .hashing import canonical_json
from .live import CodexMCPExecutor
from .runner import StrategyLabRunner
from .snapshot import build_and_write_snapshot


ALLOWED_COMMANDS = {
    "start_daily_run",
    "pause_scheduler",
    "resume_scheduler",
    "retry_failed_run",
    "promote_holdout",
    "start_paper",
    "stop_paper",
    "arm_pilot",
    "disarm_pilot",
    "cancel_open_orders",
    "flatten_pilot",
}


class BridgeError(RuntimeError):
    """Raised when a SmartSioux envelope or command fails closed."""


def sign_payload(secret: str, timestamp: str, nonce: str, payload: str) -> str:
    message = "\n".join([timestamp, nonce, payload]).encode("utf-8")
    return hmac.new(secret.encode("utf-8"), message, hashlib.sha256).hexdigest()


def verify_envelope(
    envelope: dict[str, Any],
    *,
    secret: str,
    maximum_clock_skew_seconds: int,
) -> dict[str, Any]:
    required = {"timestamp", "nonce", "payload", "signature"}
    if set(envelope) != required:
        raise BridgeError("signed envelope has unexpected fields")
    timestamp = str(envelope["timestamp"])
    nonce = str(envelope["nonce"])
    payload = str(envelope["payload"])
    signature = str(envelope["signature"])
    try:
        instant = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
    except ValueError as exc:
        raise BridgeError("signed envelope timestamp is invalid") from exc
    if instant.tzinfo is None:
        raise BridgeError("signed envelope timestamp has no timezone")
    if abs((datetime.now(UTC) - instant).total_seconds()) > maximum_clock_skew_seconds:
        raise BridgeError("signed envelope timestamp is stale")
    expected = sign_payload(secret, timestamp, nonce, payload)
    if not hmac.compare_digest(expected, signature):
        raise BridgeError("signed envelope signature is invalid")
    try:
        value = json.loads(payload)
    except json.JSONDecodeError as exc:
        raise BridgeError("signed envelope payload is invalid JSON") from exc
    if not isinstance(value, dict):
        raise BridgeError("signed envelope payload must be an object")
    return value


class SmartSiouxClient:
    def __init__(self, config: LabConfig) -> None:
        self.config = config
        bridge = config.section("bridge")
        self.endpoint = str(bridge["smartsioux_endpoint"])
        self.secret = os.getenv(str(bridge["hmac_secret_env"]), "")
        if not self.secret and config.bridge_secret_path.exists():
            self.secret = config.bridge_secret_path.read_text(encoding="utf-8").strip()
        self.maximum_bytes = int(bridge["maximum_payload_bytes"])
        self.maximum_skew = int(bridge["maximum_clock_skew_seconds"])

    def configured(self) -> bool:
        return bool(self.endpoint and self.secret)

    def _envelope(self, payload: dict[str, Any]) -> dict[str, str]:
        rendered = canonical_json(payload)
        if len(rendered.encode("utf-8")) > self.maximum_bytes:
            raise BridgeError("SmartSioux payload exceeds the configured maximum")
        timestamp = datetime.now(UTC).isoformat()
        nonce = secrets.token_hex(16)
        return {
            "timestamp": timestamp,
            "nonce": nonce,
            "payload": rendered,
            "signature": sign_payload(self.secret, timestamp, nonce, rendered),
        }

    def _post(self, function: str, payload: dict[str, Any]) -> dict[str, Any]:
        response = requests.post(
            self.endpoint,
            data={"f": function, **self._envelope(payload)},
            timeout=20,
        )
        response.raise_for_status()
        value = response.json()
        if not isinstance(value, dict):
            raise BridgeError("SmartSioux returned a non-object response")
        envelope_fields = {"timestamp", "nonce", "payload", "signature"}
        if envelope_fields.issubset(value):
            return verify_envelope(
                {key: value[key] for key in envelope_fields},
                secret=self.secret,
                maximum_clock_skew_seconds=self.maximum_skew,
            )
        if value.get("status") == "error":
            raise BridgeError(
                str(value.get("error", "SmartSioux rejected the request"))
            )
        raise BridgeError("SmartSioux returned an unsigned transport response")

    def publish(self, snapshot: dict[str, Any]) -> dict[str, Any]:
        return self._post("ingest", {"snapshot": snapshot})

    def poll_commands(self, bridge_id: str) -> list[dict[str, Any]]:
        response = self._post("poll_commands", {"bridge_id": bridge_id})
        commands = response.get("commands", [])
        if not isinstance(commands, list) or any(
            not isinstance(item, dict) for item in commands
        ):
            raise BridgeError("SmartSioux commands response is invalid")
        return list(commands)

    def acknowledge(
        self,
        command_id: str,
        status: str,
        result: dict[str, Any],
    ) -> dict[str, Any]:
        return self._post(
            "acknowledge",
            {"command_id": command_id, "status": status, "result": result},
        )


class BridgeWorker:
    def __init__(self, config: LabConfig, database: LabDatabase) -> None:
        self.config = config
        self.database = database
        self.runner = StrategyLabRunner(config, database)
        self.live = CodexMCPExecutor(config, database)
        self.client = SmartSiouxClient(config)
        self.bridge_id = f"{os.uname().nodename}-strategy-lab-v1"

    def _publish_progress(self, _run_id: str) -> None:
        if self.client.configured():
            self.client.publish(build_and_write_snapshot(self.config, self.database))

    def _operator_email(self) -> str:
        key = str(self.config.section("bridge")["operator_email_env"])
        return (
            (
                os.getenv(key)
                or str(self.config.section("bridge").get("operator_email") or "")
            )
            .strip()
            .lower()
        )

    def _validate_command(self, command: dict[str, Any]) -> dict[str, Any]:
        required = {
            "schema_version",
            "command_id",
            "command_type",
            "requested_by",
            "requested_at",
            "expires_at",
            "payload",
        }
        if set(command) != required:
            raise BridgeError("command has unexpected fields")
        if command["schema_version"] != int(
            self.config.section("bridge")["command_schema_version"]
        ):
            raise BridgeError("command schema version is unsupported")
        command_type = str(command["command_type"])
        if command_type not in ALLOWED_COMMANDS:
            raise BridgeError(f"command type {command_type!r} is not allowed")
        operator = self._operator_email()
        requester = str(command["requested_by"]).strip().lower()
        if not operator or requester != operator:
            raise BridgeError("command requester is not the configured Ryan operator")
        try:
            requested = datetime.fromisoformat(
                str(command["requested_at"]).replace("Z", "+00:00")
            )
            expires = datetime.fromisoformat(
                str(command["expires_at"]).replace("Z", "+00:00")
            )
        except ValueError as exc:
            raise BridgeError("command timestamps are invalid") from exc
        now = datetime.now(UTC)
        if requested.tzinfo is None or expires.tzinfo is None:
            raise BridgeError("command timestamps require timezones")
        maximum_ttl = int(self.config.section("bridge")["command_ttl_seconds"])
        if expires <= now or expires - requested > timedelta(seconds=maximum_ttl):
            raise BridgeError("command is expired or exceeds the maximum TTL")
        if not isinstance(command["payload"], dict):
            raise BridgeError("command payload must be an object")
        if any(
            key in command["payload"]
            for key in ("command", "shell", "prompt", "account_id", "order_id")
        ):
            raise BridgeError("command payload contains a forbidden free-form field")
        return command

    def _candidate_state(self, strategy_id: str) -> str:
        row = self.database.connection.execute(
            "SELECT state FROM candidates WHERE strategy_id=?", [strategy_id]
        ).fetchone()
        if not row:
            raise BridgeError(f"unknown candidate {strategy_id}")
        return str(row[0])

    def _execute(self, command: dict[str, Any]) -> dict[str, Any]:
        kind = str(command["command_type"])
        payload = command["payload"]
        if kind in {"start_daily_run", "retry_failed_run"}:
            target = payload.get("target")
            if target is not None and (
                isinstance(target, bool) or not isinstance(target, int)
            ):
                raise BridgeError("daily target must be an integer")
            return self.runner.run_daily(
                target=target,
                progress_callback=self._publish_progress,
            )
        if kind == "pause_scheduler":
            self.database.set_metadata("scheduler_paused", "true")
            return {"status": "PAUSED"}
        if kind == "resume_scheduler":
            self.database.set_metadata("scheduler_paused", "false")
            return {"status": "RUNNING"}
        strategy_id = str(payload.get("strategy_id", ""))
        if not strategy_id:
            raise BridgeError(f"{kind} requires strategy_id")
        if kind == "promote_holdout":
            return self.runner.promote_holdout(strategy_id)
        if kind == "start_paper":
            if (
                self._candidate_state(strategy_id)
                != CandidateState.HISTORICALLY_VALIDATED
            ):
                raise BridgeError("paper monitoring requires HISTORICALLY_VALIDATED")
            self.database.connection.execute(
                "UPDATE candidates SET state='PAPER_ACTIVE', state_reason=?, updated_at=? WHERE strategy_id=?",
                ["prospective paper monitoring active", utc_now(), strategy_id],
            )
            return {"status": "PAPER_ACTIVE", "strategy_id": strategy_id}
        if kind == "stop_paper":
            if self._candidate_state(strategy_id) != CandidateState.PAPER_ACTIVE:
                raise BridgeError("candidate is not in paper monitoring")
            self.database.connection.execute(
                "UPDATE candidates SET state='HISTORICALLY_VALIDATED', state_reason=?, updated_at=? WHERE strategy_id=?",
                ["paper monitoring stopped", utc_now(), strategy_id],
            )
            return {"status": "HISTORICALLY_VALIDATED", "strategy_id": strategy_id}
        if kind == "arm_pilot":
            rules = str(payload.get("rules_sha256", ""))
            expires = str(payload.get("arm_expires_at", ""))
            return self.live.arm(strategy_id, rules, expires)
        if kind == "disarm_pilot":
            return self.live.disarm(strategy_id)
        if kind == "cancel_open_orders":
            return self.live.execute(strategy_id, "cancel_open_orders")
        if kind == "flatten_pilot":
            if payload.get("confirmation_text") != "FLATTEN":
                raise BridgeError("flatten requires exact confirmation_text FLATTEN")
            return self.live.execute(strategy_id, "flatten")
        raise BridgeError(f"unhandled command {kind}")

    def process_command(self, raw: dict[str, Any]) -> dict[str, Any]:
        command_id = str(raw.get("command_id", "unknown"))
        try:
            command = self._validate_command(raw)
            existing = self.database.connection.execute(
                "SELECT status, result_json FROM commands WHERE command_id=?",
                [command_id],
            ).fetchone()
            if existing:
                return {
                    "command_id": command_id,
                    "status": str(existing[0]),
                    "result": json.loads(str(existing[1])) if existing[1] else {},
                    "idempotent": True,
                }
            self.database.connection.execute(
                "INSERT INTO commands VALUES (?, ?, 'RUNNING', ?, ?, ?, ?, ?, ?, NULL, ?)",
                [
                    command_id,
                    command["command_type"],
                    command["payload"].get("strategy_id"),
                    command["payload"].get("rules_sha256"),
                    command["requested_by"],
                    command["requested_at"],
                    command["expires_at"],
                    canonical_json(command["payload"]),
                    utc_now(),
                ],
            )
            result = self._execute(command)
            status = (
                "AWAITING_CONFIRMATION"
                if result.get("status") == "AWAITING_CONFIRMATION"
                else "COMPLETED"
            )
        except Exception as exc:
            result = {"error": str(exc)}
            status = "FAILED"
            existing = self.database.connection.execute(
                "SELECT 1 FROM commands WHERE command_id=?", [command_id]
            ).fetchone()
            if not existing and command_id != "unknown":
                requested_at = raw.get("requested_at") or utc_now()
                expires_at = (
                    raw.get("expires_at")
                    or (datetime.now(UTC) + timedelta(minutes=1)).isoformat()
                )
                self.database.connection.execute(
                    "INSERT INTO commands VALUES (?, ?, ?, NULL, NULL, ?, ?, ?, '{}', ?, ?)",
                    [
                        command_id,
                        str(raw.get("command_type", "invalid")),
                        status,
                        str(raw.get("requested_by", "unknown")),
                        requested_at,
                        expires_at,
                        canonical_json(result),
                        utc_now(),
                    ],
                )
        self.database.connection.execute(
            "UPDATE commands SET status=?, result_json=?, updated_at=? WHERE command_id=?",
            [status, canonical_json(result), utc_now(), command_id],
        )
        return {"command_id": command_id, "status": status, "result": result}

    def run_once(self) -> dict[str, Any]:
        published: dict[str, Any] | None = None
        acknowledgements: list[dict[str, Any]] = []
        if self.client.configured():
            commands = self.client.poll_commands(self.bridge_id)
            self.database.set_metadata("last_command_poll", utc_now())
            for command in commands:
                outcome = self.process_command(command)
                acknowledgements.append(outcome)
                self.client.acknowledge(
                    outcome["command_id"], outcome["status"], outcome["result"]
                )
        live_results = (
            self.live.tick()
            if os.getenv(str(self.config.section("bridge")["live_enabled_env"])) == "1"
            else []
        )
        final_snapshot = build_and_write_snapshot(self.config, self.database)
        if self.client.configured():
            published = self.client.publish(final_snapshot)
        return {
            "status": "READY",
            "snapshot_sha256": final_snapshot["snapshot_sha256"],
            "published": published,
            "commands_processed": acknowledgements,
            "live_results": live_results,
        }

    def run_forever(self) -> None:
        active = int(self.config.section("bridge")["active_poll_seconds"])
        idle = int(self.config.section("bridge")["idle_poll_seconds"])
        while True:
            try:
                result = self.run_once()
                delay = (
                    active
                    if result["commands_processed"] or result["live_results"]
                    else idle
                )
            except Exception:
                delay = idle
            time.sleep(delay)

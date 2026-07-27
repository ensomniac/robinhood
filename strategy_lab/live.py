"""Fail-closed local bridge to an authenticated Codex Robinhood MCP agent."""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from .config import LabConfig
from .contracts import CandidateState
from .database import LabDatabase, utc_now


PROJECT_ROOT = Path(__file__).resolve().parents[1]
EASTERN = ZoneInfo("America/New_York")


class LiveBridgeError(RuntimeError):
    """Raised when a live bridge action fails a local evidence or safety gate."""


class CodexMCPExecutor:
    """Runs a fixed, non-user-authored live workflow through Codex and Robinhood MCP.

    Raw agent output can contain broker facts, so only its SHA-256 and terminal
    disposition are retained. The normal public encrypted trade context remains
    owned by the repository's live workflow.
    """

    def __init__(self, config: LabConfig, database: LabDatabase) -> None:
        self.config = config
        self.database = database

    def _live_enabled(self) -> bool:
        key = str(self.config.section("bridge")["live_enabled_env"])
        return os.getenv(key) == "1"

    def _candidate(self, strategy_id: str) -> dict[str, Any]:
        row = self.database.connection.execute(
            """
            SELECT strategy_id, rules_sha256, state, armed, armed_until
            FROM candidates WHERE strategy_id=?
            """,
            [strategy_id],
        ).fetchone()
        if not row:
            raise LiveBridgeError(f"unknown candidate {strategy_id}")
        return {
            "strategy_id": str(row[0]),
            "rules_sha256": str(row[1]),
            "state": str(row[2]),
            "armed": bool(row[3]),
            "armed_until": row[4],
        }

    def arm(
        self, strategy_id: str, rules_sha256: str, expires_at: str
    ) -> dict[str, Any]:
        candidate = self._candidate(strategy_id)
        if candidate["state"] != CandidateState.PILOT_READY:
            raise LiveBridgeError("only an exact PILOT_READY strategy may be armed")
        if candidate["rules_sha256"] != rules_sha256:
            raise LiveBridgeError("arm command rules hash does not match the candidate")
        expiration = datetime.fromisoformat(expires_at.replace("Z", "+00:00"))
        if expiration.tzinfo is None or expiration <= datetime.now(UTC):
            raise LiveBridgeError(
                "arm expiration must be a future timezone-aware timestamp"
            )
        self.database.connection.execute(
            """
            UPDATE candidates SET armed=TRUE, armed_until=?, state_reason=?,
                updated_at=? WHERE strategy_id=?
            """,
            [
                expiration,
                "Ryan armed the next exact strategy signal",
                utc_now(),
                strategy_id,
            ],
        )
        return {
            "status": "ARMED",
            "strategy_id": strategy_id,
            "rules_sha256": rules_sha256,
            "expires_at": expiration.isoformat(),
        }

    def disarm(self, strategy_id: str) -> dict[str, Any]:
        self._candidate(strategy_id)
        self.database.connection.execute(
            """
            UPDATE candidates SET armed=FALSE, armed_until=NULL, state_reason=?,
                updated_at=? WHERE strategy_id=?
            """,
            ["pilot disarmed", utc_now(), strategy_id],
        )
        return {"status": "DISARMED", "strategy_id": strategy_id}

    def _prompt(self, action: str, candidate: dict[str, Any]) -> str:
        if action not in {"evaluate_and_trade", "cancel_open_orders", "flatten"}:
            raise LiveBridgeError(f"unsupported live action {action!r}")
        instruction = {
            "evaluate_and_trade": (
                "Evaluate the exact armed strategy's current frozen production signal. "
                "If and only if every maturity, evaluator, portfolio_guard, account, market, "
                "risk, review, protection, and current-session gate passes, execute its next "
                "eligible long-equity/ETF pilot through Robinhood Trading MCP. Otherwise record "
                "the exact no-trade or waiting disposition. Never invent inputs or self-confirm "
                "a tool-required Ryan confirmation."
            ),
            "cancel_open_orders": (
                "Reconcile the exact strategy's broker orders and cancel only its confirmed "
                "open or redundant orders when safe. Preserve any protection still required "
                "for real exposure, obey tool confirmation, and record the result."
            ),
            "flatten": (
                "Treat this as an authorized safety flatten for the exact strategy. Reconcile "
                "position and orders, safely close its confirmed exposure, cancel residual "
                "orders, verify flat state, and journal the result. Obey any broker-required "
                "Ryan confirmation and never act on another strategy."
            ),
        }[action]
        return (
            "Run from /Users/ensomniac/trade/robinhood_codex. Read and obey AGENTS.md. "
            "Select live mode, run every required sensitive-data, maturity, repository, "
            "portfolio, and broker preflight, and use only the configured Robinhood Trading MCP. "
            f"Exact strategy_id={candidate['strategy_id']} and "
            f"rules_sha256={candidate['rules_sha256']}. {instruction} "
            "Return a concise terminal disposition with no plaintext account or broker identifiers."
        )

    def execute(self, strategy_id: str, action: str) -> dict[str, Any]:
        if not self._live_enabled():
            raise LiveBridgeError("STRATEGY_LAB_LIVE_ENABLED is not 1")
        candidate = self._candidate(strategy_id)
        if action == "evaluate_and_trade":
            if (
                candidate["state"] != CandidateState.PILOT_READY
                or not candidate["armed"]
            ):
                raise LiveBridgeError("candidate is not armed and PILOT_READY")
            if candidate["armed_until"] is None or candidate[
                "armed_until"
            ] <= datetime.now(UTC):
                self.disarm(strategy_id)
                raise LiveBridgeError("candidate arm has expired")
        prompt = self._prompt(action, candidate)
        completed = subprocess.run(
            [
                "codex",
                "exec",
                "--json",
                "--ephemeral",
                "--sandbox",
                "danger-full-access",
                "-C",
                str(PROJECT_ROOT),
                prompt,
            ],
            cwd=PROJECT_ROOT,
            capture_output=True,
            text=True,
            timeout=900,
            check=False,
        )
        raw = (completed.stdout + "\n" + completed.stderr).encode("utf-8")
        lowered = raw.lower()
        if b"confirmation" in lowered and (
            b"required" in lowered or b"awaiting" in lowered
        ):
            status = "AWAITING_CONFIRMATION"
        elif completed.returncode == 0:
            status = "COMPLETED"
        else:
            status = "FAILED"
        result = {
            "status": status,
            "strategy_id": strategy_id,
            "rules_sha256": candidate["rules_sha256"],
            "action": action,
            "agent_output_sha256": hashlib.sha256(raw).hexdigest(),
            "return_code": completed.returncode,
            "completed_at": utc_now(),
        }
        self.database.connection.execute(
            "INSERT INTO live_events VALUES (?, ?, ?, ?, ?)",
            [
                f"live-agent-{hashlib.sha256(raw).hexdigest()[:24]}",
                strategy_id,
                action,
                utc_now(),
                json.dumps(result, sort_keys=True, separators=(",", ":")),
            ],
        )
        if action == "evaluate_and_trade" and status in {
            "COMPLETED",
            "AWAITING_CONFIRMATION",
        }:
            self.disarm(strategy_id)
        return result

    def tick(self) -> list[dict[str, Any]]:
        now = datetime.now(EASTERN)
        if now.weekday() >= 5 or not (9 <= now.hour < 16):
            return []
        rows = self.database.connection.execute(
            """
            SELECT strategy_id FROM candidates
            WHERE armed AND armed_until > current_timestamp AND state='PILOT_READY'
            ORDER BY strategy_id
            """
        ).fetchall()
        results: list[dict[str, Any]] = []
        for (strategy_id,) in rows:
            key = f"last_live_check:{strategy_id}"
            previous = self.database.get_metadata(key)
            if previous:
                elapsed = datetime.now(UTC) - datetime.fromisoformat(previous)
                if elapsed.total_seconds() < 60:
                    continue
            self.database.set_metadata(key, datetime.now(UTC).isoformat())
            results.append(self.execute(str(strategy_id), "evaluate_and_trade"))
        return results

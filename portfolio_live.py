"""Fail-closed artifact lifecycle for one controlled v2 live pilot.

The module never calls a broker.  It validates privacy-safe facts returned by
the broker workflow, authenticates encrypted identifiers locally, and emits the
next safe state.  Broker actions remain external and subject to tool-required
human confirmation.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import subprocess
import sys
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import portfolio_execution
import portfolio_guard
import portfolio_maturity
import sensitive_data
import strategy_discovery
import strategy_ledger
import trade_lifecycle


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_ROOT = PROJECT_ROOT / "strategy_tournament/v2/live"
SCHEMA_VERSION = 1
MAXIMUM_BROKER_OBSERVATION_AGE_SECONDS = 15
NOTIFICATION_STATES = {"sent", "skipped", "failed"}
ENTRY_STATES = {"filled", "partially_filled", "open", "rejected", "unknown"}
PROTECTION_STATES = {"accepted", "open", "rejected", "unknown"}
TERMINAL_ORDER_STATES = {"filled", "cancelled", "rejected", "failed", "expired"}
EXIT_REASONS = {
    "strategy_exit",
    "stop",
    "target",
    "safe_cutoff",
    "maximum_hold",
    "protection_failure",
}


class PortfolioLiveError(ValueError):
    """A live transition is stale, unreconciled, private, or otherwise unsafe."""


def _read_object(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise PortfolioLiveError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise PortfolioLiveError(f"{path} must contain an object")
    return value


def _object(value: Any, field: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise PortfolioLiveError(f"{field} must be an object")
    return dict(value)


def _boolean(value: Any, field: str) -> bool:
    if not isinstance(value, bool):
        raise PortfolioLiveError(f"{field} must be boolean")
    return value


def _number(value: Any, field: str, *, minimum: float | None = None) -> float:
    if isinstance(value, bool):
        raise PortfolioLiveError(f"{field} must be numeric")
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise PortfolioLiveError(f"{field} must be numeric") from exc
    if not math.isfinite(result) or (minimum is not None and result < minimum):
        raise PortfolioLiveError(f"{field} is invalid")
    return result


def _integer(value: Any, field: str, *, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise PortfolioLiveError(f"{field} must be an integer >= {minimum}")
    return value


def _timestamp(value: Any, field: str) -> datetime:
    if not isinstance(value, str):
        raise PortfolioLiveError(f"{field} must be an ISO timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise PortfolioLiveError(f"{field} must be an ISO timestamp") from exc
    if parsed.tzinfo is None:
        raise PortfolioLiveError(f"{field} must include a timezone")
    return parsed.astimezone(UTC)


def _fresh(value: Any, field: str, now: datetime) -> datetime:
    observed = _timestamp(value, field)
    age = (now - observed).total_seconds()
    if age < 0 or age > MAXIMUM_BROKER_OBSERVATION_AGE_SECONDS:
        raise PortfolioLiveError(f"{field} is stale or future-dated")
    return observed


def _exact_fields(value: Mapping[str, Any], expected: set[str], field: str) -> None:
    missing = sorted(expected - set(value))
    extras = sorted(set(value) - expected)
    if missing or extras:
        raise PortfolioLiveError(
            f"{field} fields differ; missing={missing}, extras={extras}"
        )


def _notification(value: Any, field: str) -> str:
    if value not in NOTIFICATION_STATES:
        raise PortfolioLiveError(f"{field} must record sent, skipped, or failed")
    return str(value)


def _token(value: Any, field: str, cipher: sensitive_data.SensitiveDataCipher) -> str:
    if not isinstance(value, str):
        raise PortfolioLiveError(f"encrypted {field} is missing")
    try:
        cipher.decrypt(value, field)
    except sensitive_data.SensitiveDataError as exc:
        raise PortfolioLiveError(f"encrypted {field} is invalid: {exc}") from exc
    return value


def _file_hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def run_repository_preentry_checks() -> dict[str, Any]:
    """Run local no-broker checks before any controlled order can be prepared."""
    cipher = sensitive_data.get_cipher()
    probe = cipher.encrypt("portfolio-live-check", "other_identifier")
    if cipher.decrypt(probe, "other_identifier").value != "portfolio-live-check":
        raise PortfolioLiveError("identifier encryption round trip failed")
    sensitive_audit = sensitive_data.audit_context_files(
        [PROJECT_ROOT / "TRADES.md", PROJECT_ROOT / "trades"], cipher
    )
    lifecycle = trade_lifecycle.audit_lifecycle()
    orb_ledger = strategy_ledger.audit_ledger()
    portfolio_ledger = portfolio_maturity.audit_ledger()
    status = subprocess.run(
        ["git", "status", "--porcelain"],
        cwd=PROJECT_ROOT,
        capture_output=True,
        text=True,
        check=True,
    ).stdout.strip()
    blockers = []
    if sensitive_audit.violations:
        blockers.append("sensitive-data audit failed")
    if not lifecycle.valid:
        blockers.append("trade lifecycle audit failed")
    if not orb_ledger.valid:
        blockers.append("strategy ledger audit failed")
    if portfolio_ledger.get("valid") is not True:
        blockers.append("portfolio ledger audit failed")
    if status:
        blockers.append("Git worktree is not clean before live preparation")
    if blockers:
        raise PortfolioLiveError("; ".join(blockers))
    return {
        "encryption_round_trip": True,
        "sensitive_data_audit": True,
        "trade_lifecycle_audit": True,
        "strategy_ledger_audit": True,
        "portfolio_ledger_audit": True,
        "git_clean_at_start": True,
    }


def _load_winner(
    winner_path: Path, *, enforce_commit: bool
) -> dict[str, Any]:
    if enforce_commit:
        strategy_discovery.require_committed(winner_path)
    winner = strategy_discovery.load_artifact(
        winner_path, expected_kind="frozen-strategy-winner"
    )
    if winner.get("state") != "WINNER_FROZEN":
        raise PortfolioLiveError("live winner is not frozen")
    return winner


def _validate_review(
    review: Mapping[str, Any], evaluation: Mapping[str, Any]
) -> dict[str, Any]:
    value = dict(review)
    expected = {
        "passed",
        "confirmation_required",
        "confirmation_satisfied",
        "reviewed_symbol",
        "reviewed_side",
        "reviewed_order_type",
        "reviewed_limit_price",
        "reviewed_quantity",
        "buying_power_sufficient",
        "tradable",
        "halted",
        "blocking_alerts",
    }
    _exact_fields(value, expected, "broker_review")
    for field in (
        "passed",
        "confirmation_required",
        "confirmation_satisfied",
        "buying_power_sufficient",
        "tradable",
        "halted",
    ):
        _boolean(value[field], f"broker_review.{field}")
    if (
        value["passed"] is not True
        or value["buying_power_sufficient"] is not True
        or value["tradable"] is not True
        or value["halted"] is True
        or value["reviewed_symbol"] != evaluation["symbol"]
        or value["reviewed_side"] != evaluation["order"]["side"]
        or value["reviewed_order_type"] != evaluation["order"]["type"]
        or _number(value["reviewed_limit_price"], "reviewed_limit_price")
        != float(evaluation["order"]["limit_price"])
        or _integer(value["reviewed_quantity"], "reviewed_quantity", minimum=1)
        != int(evaluation["order"]["quantity"])
    ):
        raise PortfolioLiveError("broker review does not pass the exact order")
    alerts = value["blocking_alerts"]
    if not isinstance(alerts, list) or alerts:
        raise PortfolioLiveError("broker review has blocking alerts")
    if value["confirmation_required"] and not value["confirmation_satisfied"]:
        raise PortfolioLiveError("broker-required human confirmation is pending")
    return value


def _validate_guard_snapshot(
    snapshot: Mapping[str, Any],
    winner: Mapping[str, Any],
    evaluation: Mapping[str, Any],
    review: Mapping[str, Any],
) -> dict[str, Any]:
    value = portfolio_guard.validate_snapshot(snapshot)
    if value["broker_state"] != "FLAT_RECONCILED":
        raise PortfolioLiveError("controlled pilot requires FLAT_RECONCILED")
    if any(
        value[field]
        for field in (
            "positions_count",
            "protected_positions_count",
            "unknown_orders_count",
            "unprotected_positions_count",
        )
    ):
        raise PortfolioLiveError("controlled pilot did not start from a flat account")
    for field in ("strategy_id", "strategy_version", "rules_hash"):
        if value[field] != winner[field]:
            raise PortfolioLiveError(f"guard snapshot {field} drifted")
    expected_risk = evaluation["risk"]
    comparisons = {
        "proposed_position_loss_fraction": expected_risk["planned_loss_fraction"],
        "proposed_gross_notional_fraction": expected_risk[
            "gross_notional_fraction"
        ],
        "proposed_holding_trading_days": expected_risk["holding_trading_days"],
    }
    for field, expected in comparisons.items():
        if not math.isclose(float(value[field]), float(expected), rel_tol=0, abs_tol=1e-12):
            raise PortfolioLiveError(f"guard snapshot {field} drifted")
    for field, expected in (
        ("broker_review_passed", review["passed"]),
        ("broker_confirmation_required", review["confirmation_required"]),
        ("broker_confirmation_satisfied", review["confirmation_satisfied"]),
    ):
        if value[field] is not expected:
            raise PortfolioLiveError(f"guard snapshot {field} drifted")
    return value


def prepare_live(
    winner_path: Path,
    setup: Mapping[str, Any],
    *,
    root: Path = DEFAULT_ROOT,
    enforce_commit: bool = True,
    enforce_repository_checks: bool = True,
    report: Mapping[str, Any] | None = None,
    now: datetime | None = None,
) -> tuple[Path, dict[str, Any]]:
    """Build the exact order and require a fresh flat ENTRY_READY guard."""
    value = dict(setup)
    expected = {
        "schema_version",
        "mode",
        "market_facts",
        "account",
        "broker_review",
        "guard_snapshot",
    }
    _exact_fields(value, expected, "live setup")
    if value.get("schema_version") != SCHEMA_VERSION or value.get("mode") != "live":
        raise PortfolioLiveError("controlled pilot setup must explicitly select live mode")
    winner = _load_winner(winner_path, enforce_commit=enforce_commit)
    current = (now or datetime.now(UTC)).astimezone(UTC)
    account = _object(value["account"], "account")
    if set(account) != {"equity", "buying_power"}:
        raise PortfolioLiveError("live account permits only equity and buying_power")
    try:
        evaluation = portfolio_execution.evaluate_frozen_winner(
            winner,
            _object(value["market_facts"], "market_facts"),
            account,
            portfolio_maturity.load_config(),
            now=current,
        )
    except portfolio_execution.PortfolioExecutionError as exc:
        raise PortfolioLiveError(str(exc)) from exc
    review = _validate_review(
        _object(value["broker_review"], "broker_review"), evaluation
    )
    snapshot = _validate_guard_snapshot(
        _object(value["guard_snapshot"], "guard_snapshot"),
        winner,
        evaluation,
        review,
    )
    maturity_report = dict(report) if report is not None else portfolio_maturity.build_report()
    guard = portfolio_guard.evaluate_entry(
        snapshot,
        maturity_report,
        portfolio_maturity.load_config(),
        now=current,
    )
    if guard.get("status") != "ENTRY_READY" or guard.get("entry_allowed") is not True:
        raise PortfolioLiveError(
            f"portfolio guard is not ENTRY_READY: {guard.get('blockers')}"
        )
    checks = (
        run_repository_preentry_checks()
        if enforce_repository_checks
        else {"test_injection_repository_checks_bypassed": True}
    )
    payload = {
        "schema_version": SCHEMA_VERSION,
        "artifact_kind": "controlled-live-preparation",
        "campaign_id": strategy_discovery.CAMPAIGN_ID,
        "family_id": winner["family_id"],
        "strategy_id": winner["strategy_id"],
        "strategy_version": winner["strategy_version"],
        "rules_hash": winner["rules_hash"],
        "state": "LIVE_ENTRY_READY",
        "prepared_at": current.isoformat(),
        "winner_path": strategy_discovery._relative(winner_path),
        "winner_sha256": winner["artifact_sha256"],
        "account": account,
        "production_evaluation": evaluation,
        "broker_review": review,
        "guard_snapshot": snapshot,
        "portfolio_guard": guard,
        "repository_checks": checks,
        "broker_actions_performed": 0,
        "next_action": "submit_exact_reviewed_marketable_limit_subject_to_tool_confirmation",
    }
    return strategy_discovery._write_artifact(
        payload,
        root / str(winner["family_id"]) / "preparation",
        f"{current.date()}-{winner['strategy_id']}-live-preparation",
    )


def record_entry_result(
    preparation_path: Path,
    observation: Mapping[str, Any],
    *,
    root: Path = DEFAULT_ROOT,
    now: datetime | None = None,
) -> tuple[Path, dict[str, Any]]:
    preparation = strategy_discovery.load_artifact(
        preparation_path, expected_kind="controlled-live-preparation"
    )
    if preparation.get("state") != "LIVE_ENTRY_READY":
        raise PortfolioLiveError("live preparation is not entry-ready")
    value = dict(observation)
    expected = {
        "schema_version",
        "observed_at",
        "logical_order_alias",
        "state",
        "filled_quantity",
        "remaining_quantity",
        "average_fill_price",
        "filled_at",
        "encrypted_broker_order_id",
        "encrypted_client_ref_id",
        "notification_status",
    }
    _exact_fields(value, expected, "entry observation")
    if value.get("schema_version") != SCHEMA_VERSION:
        raise PortfolioLiveError("entry observation schema_version must be 1")
    current = (now or datetime.now(UTC)).astimezone(UTC)
    observed_at = _fresh(value["observed_at"], "entry observed_at", current)
    alias = value["logical_order_alias"]
    if not isinstance(alias, str) or not alias or sensitive_data.RAW_UUID_PATTERN.search(alias):
        raise PortfolioLiveError("logical order alias is missing or private")
    state = value["state"]
    if state not in ENTRY_STATES:
        raise PortfolioLiveError("entry broker state is invalid")
    requested = int(preparation["production_evaluation"]["order"]["quantity"])
    filled = _integer(value["filled_quantity"], "filled_quantity")
    remaining = _integer(value["remaining_quantity"], "remaining_quantity")
    if filled + remaining != requested:
        raise PortfolioLiveError("entry quantities do not reconcile to the reviewed order")
    fill_price = value["average_fill_price"]
    filled_at = value["filled_at"]
    if filled:
        fill_price = _number(fill_price, "average_fill_price", minimum=0.000001)
        filled_at = _timestamp(filled_at, "filled_at")
        if filled_at > observed_at:
            raise PortfolioLiveError("entry fill timestamp is future-dated")
        if fill_price > float(preparation["production_evaluation"]["order"]["limit_price"]):
            raise PortfolioLiveError("buy fill exceeded the reviewed limit")
    elif fill_price is not None or filled_at is not None:
        raise PortfolioLiveError("zero fill must not record fill price or timestamp")
    if state == "filled" and (filled != requested or remaining):
        raise PortfolioLiveError("filled state has unreconciled quantity")
    if state == "partially_filled" and not (0 < filled < requested and remaining):
        raise PortfolioLiveError("partial-fill state has invalid quantity")
    if state in {"rejected", "unknown", "open"} and filled:
        raise PortfolioLiveError(f"{state} entry state cannot claim a fill")
    cipher = sensitive_data.get_cipher()
    broker_token = value["encrypted_broker_order_id"]
    if broker_token is not None:
        broker_token = _token(broker_token, "broker_order_id", cipher)
    elif state != "unknown":
        raise PortfolioLiveError("definitive entry state needs an encrypted broker ID")
    ref_token = _token(value["encrypted_client_ref_id"], "client_ref_id", cipher)
    notification = _notification(value["notification_status"], "entry notification")
    if state == "unknown":
        artifact_state = "ENTRY_SUBMISSION_UNKNOWN_RECONCILE_REQUIRED"
        next_action = "query_orders_before_any_retry"
    elif filled:
        artifact_state = "ENTRY_EXPOSURE_CONFIRMED_PROTECT_NOW"
        next_action = "establish_or_safely_resolve_protection_immediately"
    elif state == "open":
        artifact_state = "ENTRY_OPEN_MONITOR_WITHOUT_DUPLICATE"
        next_action = "reconcile_entry_before_protection_or_retry"
    else:
        artifact_state = "ENTRY_TERMINAL_NO_EXPOSURE"
        next_action = "journal_no_trade"
    planned = float(preparation["production_evaluation"]["market"]["ask"])
    entry_slippage = (
        max(0.0, (float(fill_price) - planned) / planned * 10_000)
        if filled
        else 0.0
    )
    payload = {
        "schema_version": SCHEMA_VERSION,
        "artifact_kind": "controlled-live-entry-result",
        "campaign_id": preparation["campaign_id"],
        "family_id": preparation["family_id"],
        "strategy_id": preparation["strategy_id"],
        "strategy_version": preparation["strategy_version"],
        "rules_hash": preparation["rules_hash"],
        "state": artifact_state,
        "observed_at": observed_at.isoformat(),
        "preparation_path": strategy_discovery._relative(preparation_path),
        "preparation_sha256": preparation["artifact_sha256"],
        "logical_order_alias": alias,
        "broker_state": state,
        "filled_quantity": filled,
        "remaining_quantity": remaining,
        "average_fill_price": fill_price,
        "filled_at": filled_at.isoformat() if filled else None,
        "entry_slippage_bps": entry_slippage,
        "encrypted_broker_order_id": broker_token,
        "encrypted_client_ref_id": ref_token,
        "notification_status": notification,
        "broker_actions_performed": 1,
        "next_action": next_action,
    }
    return strategy_discovery._write_artifact(
        payload,
        root / str(preparation["family_id"]) / "entry",
        f"{observed_at.date()}-{preparation['strategy_id']}-live-entry",
    )


def reconcile_unknown_entry(
    entry_path: Path,
    reconciliation: Mapping[str, Any],
    *,
    root: Path = DEFAULT_ROOT,
    now: datetime | None = None,
) -> tuple[Path, dict[str, Any]]:
    entry = strategy_discovery.load_artifact(
        entry_path, expected_kind="controlled-live-entry-result"
    )
    if entry.get("state") not in {
        "ENTRY_SUBMISSION_UNKNOWN_RECONCILE_REQUIRED",
        "ENTRY_OPEN_MONITOR_WITHOUT_DUPLICATE",
    }:
        raise PortfolioLiveError("entry does not require broker reconciliation")
    value = dict(reconciliation)
    _exact_fields(value, {"schema_version", "observed_at", "orders"}, "entry reconciliation")
    if value.get("schema_version") != SCHEMA_VERSION:
        raise PortfolioLiveError("entry reconciliation schema_version must be 1")
    current = (now or datetime.now(UTC)).astimezone(UTC)
    observed_at = _fresh(value["observed_at"], "reconciliation observed_at", current)
    orders = value["orders"]
    if not isinstance(orders, list):
        raise PortfolioLiveError("reconciliation orders must be an array")
    normalized: list[dict[str, Any]] = []
    cipher = sensitive_data.get_cipher()
    for index, raw in enumerate(orders):
        order = _object(raw, f"orders[{index}]")
        expected = {
            "logical_order_alias",
            "state",
            "filled_quantity",
            "remaining_quantity",
            "average_fill_price",
            "filled_at",
            "encrypted_broker_order_id",
        }
        _exact_fields(order, expected, f"orders[{index}]")
        alias = order["logical_order_alias"]
        if (
            not isinstance(alias, str)
            or not alias
            or sensitive_data.RAW_UUID_PATTERN.search(alias)
        ):
            raise PortfolioLiveError(
                f"orders[{index}].logical_order_alias is missing or private"
            )
        token = order["encrypted_broker_order_id"]
        if token is None:
            raise PortfolioLiveError(
                f"orders[{index}] needs an encrypted broker order ID"
            )
        _token(token, "broker_order_id", cipher)
        normalized.append(order)
    decision = portfolio_execution.reconcile_unknown_submission(
        entry["logical_order_alias"], normalized
    )
    matches = [
        order
        for order in normalized
        if order["logical_order_alias"] == entry["logical_order_alias"]
    ]
    exposure: dict[str, Any] | None = None
    if len(matches) == 1 and matches[0]["state"] in {
        "filled",
        "partially_filled",
    }:
        match = matches[0]
        filled = _integer(match["filled_quantity"], "reconciled filled_quantity", minimum=1)
        remaining = _integer(match["remaining_quantity"], "reconciled remaining_quantity")
        requested = filled + remaining
        prepared = strategy_discovery.load_artifact(
            PROJECT_ROOT / entry["preparation_path"],
            expected_kind="controlled-live-preparation",
        )
        if requested != int(prepared["production_evaluation"]["order"]["quantity"]):
            raise PortfolioLiveError("reconciled entry quantities drifted")
        price = _number(match["average_fill_price"], "reconciled fill price", minimum=0.000001)
        filled_at = _timestamp(match["filled_at"], "reconciled filled_at")
        if filled_at > observed_at:
            raise PortfolioLiveError("reconciled fill timestamp is future-dated")
        if price > float(prepared["production_evaluation"]["order"]["limit_price"]):
            raise PortfolioLiveError("reconciled buy fill exceeded its limit")
        broker_token = match["encrypted_broker_order_id"]
        if broker_token is None:
            raise PortfolioLiveError(
                "reconciled exposure needs an encrypted broker order ID"
            )
        exposure = {
            "filled_quantity": filled,
            "remaining_quantity": remaining,
            "average_fill_price": price,
            "filled_at": filled_at.isoformat(),
            "entry_slippage_bps": max(
                0.0,
                (price - float(prepared["production_evaluation"]["market"]["ask"]))
                / float(prepared["production_evaluation"]["market"]["ask"])
                * 10_000,
            ),
            "encrypted_broker_order_id": broker_token,
        }
    payload = {
        "schema_version": SCHEMA_VERSION,
        "artifact_kind": "controlled-live-entry-reconciliation",
        "campaign_id": entry["campaign_id"],
        "family_id": entry["family_id"],
        "strategy_id": entry["strategy_id"],
        "strategy_version": entry["strategy_version"],
        "rules_hash": entry["rules_hash"],
        "state": (
            "ENTRY_EXPOSURE_RECONCILED_PROTECT_NOW"
            if exposure
            else decision["status"]
        ),
        "observed_at": observed_at.isoformat(),
        "entry_path": strategy_discovery._relative(entry_path),
        "entry_sha256": entry["artifact_sha256"],
        "decision": decision,
        "exposure": exposure,
        "broker_actions_performed": 1,
    }
    return strategy_discovery._write_artifact(
        payload,
        root / str(entry["family_id"]) / "entry-reconciliation",
        f"{observed_at.date()}-{entry['strategy_id']}-entry-reconciliation",
    )


def _entry_exposure(
    path: Path,
) -> tuple[dict[str, Any], dict[str, Any], int, float, Path]:
    artifact = strategy_discovery.load_artifact(path)
    if artifact.get("artifact_kind") == "controlled-live-entry-result":
        if artifact.get("state") != "ENTRY_EXPOSURE_CONFIRMED_PROTECT_NOW":
            raise PortfolioLiveError("entry exposure is not confirmed")
        entry = artifact
        filled = int(entry["filled_quantity"])
        price = float(entry["average_fill_price"])
        preparation_path = PROJECT_ROOT / entry["preparation_path"]
    elif artifact.get("artifact_kind") == "controlled-live-entry-reconciliation":
        if artifact.get("state") != "ENTRY_EXPOSURE_RECONCILED_PROTECT_NOW":
            raise PortfolioLiveError("reconciled entry has no confirmed exposure")
        original = strategy_discovery.load_artifact(
            PROJECT_ROOT / artifact["entry_path"],
            expected_kind="controlled-live-entry-result",
        )
        if original["artifact_sha256"] != artifact.get("entry_sha256"):
            raise PortfolioLiveError("reconciled entry binding drifted")
        entry = {**original, **dict(artifact["exposure"])}
        filled = int(artifact["exposure"]["filled_quantity"])
        price = float(artifact["exposure"]["average_fill_price"])
        preparation_path = PROJECT_ROOT / original["preparation_path"]
    else:
        raise PortfolioLiveError("unsupported entry exposure artifact")
    preparation = strategy_discovery.load_artifact(
        preparation_path, expected_kind="controlled-live-preparation"
    )
    expected_preparation_sha256 = (
        artifact.get("preparation_sha256")
        if artifact.get("artifact_kind") == "controlled-live-entry-result"
        else original.get("preparation_sha256")
    )
    if preparation["artifact_sha256"] != expected_preparation_sha256:
        raise PortfolioLiveError("live preparation binding drifted")
    for field in ("campaign_id", "family_id", "strategy_id", "strategy_version", "rules_hash"):
        if artifact.get(field) != preparation.get(field):
            raise PortfolioLiveError(f"live exposure {field} drifted")
    return artifact, preparation, filled, price, preparation_path


def record_protection(
    exposure_path: Path,
    observation: Mapping[str, Any],
    *,
    root: Path = DEFAULT_ROOT,
    now: datetime | None = None,
) -> tuple[Path, dict[str, Any]]:
    exposure, preparation, filled, _entry_price, preparation_path = _entry_exposure(
        exposure_path
    )
    value = dict(observation)
    expected = {
        "schema_version",
        "observed_at",
        "state",
        "coverage_quantity",
        "entry_remainder_state",
        "time_in_force",
        "encrypted_broker_order_id",
        "encrypted_client_ref_id",
        "notification_status",
    }
    _exact_fields(value, expected, "protection observation")
    if value.get("schema_version") != SCHEMA_VERSION:
        raise PortfolioLiveError("protection observation schema_version must be 1")
    current = (now or datetime.now(UTC)).astimezone(UTC)
    observed_at = _fresh(value["observed_at"], "protection observed_at", current)
    state = value["state"]
    if state not in PROTECTION_STATES:
        raise PortfolioLiveError("protection state is invalid")
    coverage = _integer(value["coverage_quantity"], "coverage_quantity")
    expected_tif = preparation["production_evaluation"]["protection"]["time_in_force"]
    confirmed = state in {"accepted", "open"}
    if confirmed and coverage != filled:
        raise PortfolioLiveError("protection does not cover the filled quantity")
    if not confirmed and coverage:
        raise PortfolioLiveError("unconfirmed protection cannot claim coverage")
    remaining = int(
        exposure.get("remaining_quantity")
        if exposure.get("remaining_quantity") is not None
        else exposure.get("exposure", {}).get("remaining_quantity", 0)
    )
    remainder_state = value["entry_remainder_state"]
    if remaining:
        if remainder_state not in {"cancelled", "rejected", "expired"}:
            raise PortfolioLiveError(
                "partial entry remainder must be terminal before protection completes"
            )
    elif remainder_state != "none":
        raise PortfolioLiveError("filled entry must use entry_remainder_state=none")
    if value["time_in_force"] != expected_tif:
        raise PortfolioLiveError("protection time-in-force drifted")
    cipher = sensitive_data.get_cipher()
    broker_token = value["encrypted_broker_order_id"]
    ref_token = value["encrypted_client_ref_id"]
    if broker_token is not None:
        broker_token = _token(broker_token, "broker_order_id", cipher)
    if ref_token is not None:
        ref_token = _token(ref_token, "client_ref_id", cipher)
    if confirmed and (broker_token is None or ref_token is None):
        raise PortfolioLiveError("confirmed protection needs encrypted identifiers")
    if exposure.get("artifact_kind") == "controlled-live-entry-reconciliation":
        entry_at = _timestamp(exposure["exposure"]["filled_at"], "entry filled_at")
    else:
        entry_at = _timestamp(exposure["filled_at"], "entry filled_at")
    if observed_at < entry_at:
        raise PortfolioLiveError("protection observation predates the entry fill")
    unprotected_seconds = (observed_at - entry_at).total_seconds()
    payload = {
        "schema_version": SCHEMA_VERSION,
        "artifact_kind": "controlled-live-protection-result",
        "campaign_id": preparation["campaign_id"],
        "family_id": preparation["family_id"],
        "strategy_id": preparation["strategy_id"],
        "strategy_version": preparation["strategy_version"],
        "rules_hash": preparation["rules_hash"],
        "state": "LIVE_PROTECTED_MONITOR" if confirmed else "PROTECTION_FAILED_FLATTEN_REQUIRED",
        "observed_at": observed_at.isoformat(),
        "exposure_path": strategy_discovery._relative(exposure_path),
        "exposure_sha256": exposure["artifact_sha256"],
        "preparation_path": strategy_discovery._relative(preparation_path),
        "preparation_sha256": preparation["artifact_sha256"],
        "protection_confirmed": confirmed,
        "coverage_quantity": coverage,
        "entry_remainder_state": remainder_state,
        "time_in_force": value["time_in_force"],
        "unprotected_seconds": unprotected_seconds,
        "encrypted_broker_order_id": broker_token,
        "encrypted_client_ref_id": ref_token,
        "notification_status": _notification(
            value["notification_status"], "protection notification"
        ),
        "broker_actions_performed": 2,
        "next_action": (
            "monitor_exact_exit_path"
            if confirmed
            else "flatten_by_safe_cutoff_and_reconcile"
        ),
    }
    return strategy_discovery._write_artifact(
        payload,
        root / str(preparation["family_id"]) / "protection",
        f"{observed_at.date()}-{preparation['strategy_id']}-live-protection",
    )


def rebuild_close(
    protection: Mapping[str, Any],
    exposure: Mapping[str, Any],
    preparation: Mapping[str, Any],
    closure: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Rebuild terminal reconciliation and the exact schema-2 live row."""
    value = dict(closure)
    expected = {
        "schema_version",
        "observed_at",
        "exit_reason",
        "exit_quantity",
        "average_exit_price",
        "expected_exit_price",
        "realized_net_dollars",
        "ending_equity",
        "stop_executed",
        "stop_slippage_bps",
        "stop_reserve_bps",
        "exit_used_existing_protection",
        "encrypted_broker_order_id",
        "encrypted_client_ref_id",
        "residual_orders",
        "broker_snapshot",
        "monitoring_complete",
        "journal_complete",
        "session_capture_complete",
        "rule_violations",
        "journal_path",
        "notification_status",
    }
    _exact_fields(value, expected, "live closure")
    if value.get("schema_version") != SCHEMA_VERSION:
        raise PortfolioLiveError("live closure schema_version must be 1")
    observed_at = _timestamp(value["observed_at"], "close observed_at")
    protection_at = _timestamp(protection.get("observed_at"), "protection observed_at")
    if observed_at < protection_at:
        raise PortfolioLiveError("live close predates the protection result")
    if exposure.get("artifact_sha256") != protection.get("exposure_sha256"):
        raise PortfolioLiveError("protection exposure binding drifted")
    if preparation.get("artifact_sha256") != protection.get("preparation_sha256"):
        raise PortfolioLiveError("protection preparation binding drifted")
    for field in ("campaign_id", "family_id", "strategy_id", "strategy_version", "rules_hash"):
        identities = {
            str(protection.get(field)),
            str(exposure.get(field)),
            str(preparation.get(field)),
        }
        if len(identities) != 1:
            raise PortfolioLiveError(f"live lifecycle {field} drifted")
    filled = int(
        exposure.get("filled_quantity")
        or _object(exposure.get("exposure"), "entry exposure")["filled_quantity"]
    )
    entry_price = float(
        exposure.get("average_fill_price")
        or _object(exposure.get("exposure"), "entry exposure")["average_fill_price"]
    )
    exit_quantity = _integer(value["exit_quantity"], "exit_quantity", minimum=1)
    if exit_quantity != filled:
        raise PortfolioLiveError("exit quantity does not flatten the filled quantity")
    exit_price = _number(value["average_exit_price"], "average_exit_price", minimum=0.000001)
    expected_exit = _number(value["expected_exit_price"], "expected_exit_price", minimum=0.000001)
    realized = _number(value["realized_net_dollars"], "realized_net_dollars")
    ending_equity = _number(value["ending_equity"], "ending_equity", minimum=0.000001)
    stop_executed = _boolean(value["stop_executed"], "stop_executed")
    stop_slippage = _number(value["stop_slippage_bps"], "stop_slippage_bps", minimum=0)
    stop_reserve = _number(value["stop_reserve_bps"], "stop_reserve_bps", minimum=0)
    if not stop_executed and (stop_slippage or stop_reserve):
        raise PortfolioLiveError("non-stop exit must not invent stop slippage")
    if value["exit_reason"] not in EXIT_REASONS:
        raise PortfolioLiveError("live exit reason is invalid")
    if stop_executed != (value["exit_reason"] == "stop"):
        raise PortfolioLiveError("stop execution and exit reason disagree")
    gross_dollars = filled * (exit_price - entry_price)
    if realized > gross_dollars + 0.01:
        raise PortfolioLiveError("realized net dollars exceed gross price PnL")
    cipher = sensitive_data.get_cipher()
    exit_broker_token = _token(
        value["encrypted_broker_order_id"], "broker_order_id", cipher
    )
    exit_ref_token = _token(value["encrypted_client_ref_id"], "client_ref_id", cipher)
    used_existing_protection = _boolean(
        value["exit_used_existing_protection"], "exit_used_existing_protection"
    )
    if used_existing_protection:
        if not stop_executed or (
            exit_broker_token != protection.get("encrypted_broker_order_id")
            or exit_ref_token != protection.get("encrypted_client_ref_id")
        ):
            raise PortfolioLiveError(
                "existing-protection exit must be the authenticated stop order"
            )
    residual_orders = value["residual_orders"]
    if not isinstance(residual_orders, list):
        raise PortfolioLiveError("residual_orders must be an array")
    normalized_residuals = []
    for index, raw in enumerate(residual_orders):
        order = _object(raw, f"residual_orders[{index}]")
        _exact_fields(
            order,
            {"logical_order_alias", "state", "encrypted_broker_order_id"},
            f"residual_orders[{index}]",
        )
        if order["state"] not in TERMINAL_ORDER_STATES:
            raise PortfolioLiveError("a residual order is not terminal")
        alias = order["logical_order_alias"]
        if (
            not isinstance(alias, str)
            or not alias
            or sensitive_data.RAW_UUID_PATTERN.search(alias)
        ):
            raise PortfolioLiveError("a residual order alias is missing or private")
        _token(order["encrypted_broker_order_id"], "broker_order_id", cipher)
        normalized_residuals.append(order)
    snapshot = _object(value["broker_snapshot"], "broker_snapshot")
    _exact_fields(
        snapshot,
        {
            "state",
            "account_reconciled",
            "orders_reconciled",
            "positions_count",
            "open_orders_count",
            "unknown_orders_count",
        },
        "close broker_snapshot",
    )
    flat = (
        snapshot["state"] == "FLAT_RECONCILED"
        and snapshot["account_reconciled"] is True
        and snapshot["orders_reconciled"] is True
        and all(
            _integer(snapshot[field], f"broker_snapshot.{field}") == 0
            for field in (
                "positions_count",
                "open_orders_count",
                "unknown_orders_count",
            )
        )
    )
    if not flat:
        raise PortfolioLiveError("live close is not flat and fully reconciled")
    for field in (
        "monitoring_complete",
        "journal_complete",
        "session_capture_complete",
    ):
        _boolean(value[field], field)
    violations = value["rule_violations"]
    if not isinstance(violations, list) or any(
        not isinstance(item, str) or not item.strip() for item in violations
    ):
        raise PortfolioLiveError("rule_violations must be an array of strings")
    journal_relative = value["journal_path"]
    if not isinstance(journal_relative, str):
        raise PortfolioLiveError("journal_path is missing")
    journal_path = PROJECT_ROOT / journal_relative
    try:
        journal_path.resolve().relative_to(PROJECT_ROOT.resolve())
    except ValueError as exc:
        raise PortfolioLiveError("journal_path is unsafe") from exc
    if not journal_path.is_file():
        raise PortfolioLiveError("live journal file is missing")
    journal_text = journal_path.read_text(encoding="utf-8")
    entry_broker_token = exposure.get("encrypted_broker_order_id")
    entry_ref_token = exposure.get("encrypted_client_ref_id")
    if entry_broker_token is None and exposure.get("exposure") is not None:
        entry_broker_token = exposure["exposure"].get(
            "encrypted_broker_order_id"
        )
        original = strategy_discovery.load_artifact(
            PROJECT_ROOT / exposure["entry_path"],
            expected_kind="controlled-live-entry-result",
        )
        if original["artifact_sha256"] != exposure.get("entry_sha256"):
            raise PortfolioLiveError("reconciled entry binding drifted")
        entry_ref_token = original.get("encrypted_client_ref_id")
    entry_broker_token = _token(entry_broker_token, "broker_order_id", cipher)
    entry_ref_token = _token(entry_ref_token, "client_ref_id", cipher)
    required_tokens = [
        entry_broker_token,
        entry_ref_token,
        protection.get("encrypted_broker_order_id"),
        protection.get("encrypted_client_ref_id"),
        exit_broker_token,
        exit_ref_token,
        *(order["encrypted_broker_order_id"] for order in normalized_residuals),
    ]
    if any(token is not None and token not in journal_text for token in required_tokens):
        raise PortfolioLiveError(
            "live journal does not retain every encrypted broker identifier"
        )
    journal_audit = sensitive_data.audit_context_files(
        [journal_path], cipher
    )
    if journal_audit.violations:
        raise PortfolioLiveError("live journal sensitive-data audit failed")
    notification = _notification(value["notification_status"], "close notification")
    protection_confirmed = protection.get("protection_confirmed") is True
    clean = (
        protection_confirmed
        and value["monitoring_complete"] is True
        and value["journal_complete"] is True
        and value["session_capture_complete"] is True
        and not violations
    )
    if not protection_confirmed and value["exit_reason"] not in {
        "protection_failure",
        "safe_cutoff",
    }:
        raise PortfolioLiveError("unprotected exposure needs a safety exit reason")
    risk_dollars = filled * (
        entry_price - float(preparation["production_evaluation"]["protection"]["stop_price"])
    )
    if risk_dollars <= 0:
        raise PortfolioLiveError("live entry risk is invalid")
    net_r = realized / risk_dollars

    def modeled_r(cost_bps: int) -> float:
        modeled = filled * (
            exit_price
            - entry_price
            - (entry_price + exit_price) * cost_bps / 10_000
        )
        return modeled / risk_dollars

    initial_equity = float(preparation["account"]["equity"])
    if not math.isclose(
        ending_equity,
        initial_equity + realized,
        rel_tol=0,
        abs_tol=0.01,
    ):
        raise PortfolioLiveError(
            "ending equity does not reconcile to initial equity and realized net dollars"
        )
    account_return = realized / initial_equity
    exit_slippage = abs(exit_price - expected_exit) / expected_exit * 10_000
    broker_actions = 2 if used_existing_protection else 3
    close_facts = {
        "flat_reconciled": True,
        "residual_orders_terminal": True,
        "protection_confirmed": protection_confirmed,
        "eligible_reconciled_live_close": clean,
        "filled_quantity": filled,
        "entry_price": entry_price,
        "exit_price": exit_price,
        "realized_net_dollars": realized,
        "ending_equity": ending_equity,
        "net_account_return_fraction": account_return,
        "net_r": net_r,
        "stress_10bps_r": modeled_r(10),
        "stress_20bps_r": modeled_r(20),
        "entry_slippage_bps": float(
            exposure["entry_slippage_bps"]
            if "entry_slippage_bps" in exposure
            else _object(exposure.get("exposure"), "entry exposure").get(
                "entry_slippage_bps", 0.0
            )
        ),
        "exit_slippage_bps": exit_slippage,
        "unprotected_seconds": float(protection["unprotected_seconds"]),
        "stop_slippage_bps": stop_slippage,
        "stop_reserve_bps": stop_reserve,
        "exit_reason": value["exit_reason"],
        "journal_path": journal_relative,
        "journal_sha256": _file_hash(journal_path),
        "notification_status": notification,
        "encrypted_exit_broker_order_id": exit_broker_token,
        "encrypted_exit_client_ref_id": exit_ref_token,
        "residual_orders": normalized_residuals,
        "broker_actions_performed": broker_actions,
    }
    record = {
        "schema_version": portfolio_maturity.SCHEMA_VERSION,
        "research_campaign_id": strategy_discovery.CAMPAIGN_ID,
        "record_type": "signal",
        "recorded_at": observed_at.isoformat(),
        "strategy_id": preparation["strategy_id"],
        "strategy_version": preparation["strategy_version"],
        "mechanism_family": preparation["family_id"],
        "rules_hash": preparation["rules_hash"],
        "date": observed_at.date().isoformat(),
        "sample_phase": "live",
        "mode": "live",
        "signal_id": f"{observed_at.date()}-{preparation['strategy_id']}-live",
        "closed": True,
        "eligible": clean,
        "net_r": net_r,
        "stress_10bps_r": modeled_r(10),
        "stress_20bps_r": modeled_r(20),
        "stop_executed": stop_executed,
        "portfolio_guard_status": preparation["portfolio_guard"]["status"],
        "broker_review_passed": preparation["broker_review"]["passed"],
        "broker_confirmation_required": preparation["broker_review"][
            "confirmation_required"
        ],
        "broker_confirmation_satisfied": preparation["broker_review"][
            "confirmation_satisfied"
        ],
        "protection_confirmed": protection_confirmed,
        "monitoring_complete": value["monitoring_complete"],
        "journal_complete": value["journal_complete"],
        "position_flat_confirmed": True,
        "residual_orders_terminal": True,
        "account_reconciled_after_close": True,
        "encrypted_identifiers_recorded": True,
        "notification_status_recorded": True,
        "entry_slippage_bps": close_facts["entry_slippage_bps"],
        "exit_slippage_bps": exit_slippage,
        "unprotected_seconds": close_facts["unprotected_seconds"],
        "realized_net_dollars": realized,
        "net_account_return_fraction": account_return,
        "exit_reason": value["exit_reason"],
        "broker_actions": broker_actions,
        "session_capture_complete": value["session_capture_complete"],
        "rule_violations": list(violations),
    }
    if stop_executed:
        record["stop_slippage_bps"] = stop_slippage
        record["stop_reserve_bps"] = stop_reserve
    if clean:
        portfolio_maturity.validate_record(record)
    return close_facts, record


def close_live(
    protection_path: Path,
    closure: Mapping[str, Any],
    *,
    root: Path = DEFAULT_ROOT,
    now: datetime | None = None,
) -> tuple[Path, dict[str, Any]]:
    protection = strategy_discovery.load_artifact(
        protection_path, expected_kind="controlled-live-protection-result"
    )
    exposure_path = PROJECT_ROOT / protection["exposure_path"]
    exposure, preparation, _filled, _price, _preparation_path = _entry_exposure(
        exposure_path
    )
    current = (now or datetime.now(UTC)).astimezone(UTC)
    _fresh(closure.get("observed_at"), "close observed_at", current)
    close_facts, record = rebuild_close(
        protection, exposure, preparation, closure
    )
    payload = {
        "schema_version": SCHEMA_VERSION,
        "artifact_kind": "controlled-live-final",
        "campaign_id": preparation["campaign_id"],
        "family_id": preparation["family_id"],
        "strategy_id": preparation["strategy_id"],
        "strategy_version": preparation["strategy_version"],
        "rules_hash": preparation["rules_hash"],
        "state": (
            "LIVE_CLOSED_RECONCILED_INSPECTION_REQUIRED"
            if close_facts["eligible_reconciled_live_close"]
            else "LIVE_CLOSED_SAFETY_FAILURE"
        ),
        "protection_path": strategy_discovery._relative(protection_path),
        "protection_sha256": protection["artifact_sha256"],
        "exposure_path": strategy_discovery._relative(exposure_path),
        "exposure_sha256": exposure["artifact_sha256"],
        "closure": dict(closure),
        "close_facts": close_facts,
        "maturity_record": record,
        "ledger_admission_permitted_after_inspection": close_facts[
            "eligible_reconciled_live_close"
        ],
    }
    observed_at = _timestamp(closure["observed_at"], "close observed_at")
    return strategy_discovery._write_artifact(
        payload,
        root / str(preparation["family_id"]) / "final",
        f"{observed_at.date()}-{preparation['strategy_id']}-live-final",
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    subparsers = parser.add_subparsers(dest="command", required=True)
    prepare = subparsers.add_parser("prepare")
    prepare.add_argument("winner", type=Path)
    prepare.add_argument("setup", type=Path)
    entry = subparsers.add_parser("record-entry")
    entry.add_argument("preparation", type=Path)
    entry.add_argument("observation", type=Path)
    reconcile = subparsers.add_parser("reconcile-unknown-entry")
    reconcile.add_argument("entry", type=Path)
    reconcile.add_argument("reconciliation", type=Path)
    protection = subparsers.add_parser("record-protection")
    protection.add_argument("exposure", type=Path)
    protection.add_argument("observation", type=Path)
    close = subparsers.add_parser("close")
    close.add_argument("protection", type=Path)
    close.add_argument("closure", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "prepare":
            path, artifact = prepare_live(
                args.winner, _read_object(args.setup), root=args.root
            )
        elif args.command == "record-entry":
            path, artifact = record_entry_result(
                args.preparation,
                _read_object(args.observation),
                root=args.root,
            )
        elif args.command == "reconcile-unknown-entry":
            path, artifact = reconcile_unknown_entry(
                args.entry,
                _read_object(args.reconciliation),
                root=args.root,
            )
        elif args.command == "record-protection":
            path, artifact = record_protection(
                args.exposure,
                _read_object(args.observation),
                root=args.root,
            )
        else:
            path, artifact = close_live(
                args.protection,
                _read_object(args.closure),
                root=args.root,
            )
        print(
            json.dumps(
                {
                    "written": strategy_discovery._relative(path),
                    "artifact_sha256": artifact["artifact_sha256"],
                    "state": artifact["state"],
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    except (
        OSError,
        PortfolioLiveError,
        sensitive_data.SensitiveDataError,
        strategy_discovery.StrategyDiscoveryError,
        ValueError,
    ) as exc:
        print(
            json.dumps(
                {"error": str(exc), "error_type": type(exc).__name__},
                indent=2,
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

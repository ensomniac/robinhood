from __future__ import annotations

import hashlib
import tempfile
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

import portfolio_execution
import portfolio_live
import portfolio_live_inspection
import portfolio_maturity
import sensitive_data
import strategy_discovery


NOW = datetime(2026, 7, 22, 14, 0, 4, tzinfo=UTC)


def _winner(work: Path):
    implementation = Path("tests/synthetic_discovery_plugin.py")
    return strategy_discovery._write_artifact(
        {
            "schema_version": 1,
            "artifact_kind": "frozen-strategy-winner",
            "campaign_id": strategy_discovery.CAMPAIGN_ID,
            "family_id": "synthetic-live-family",
            "strategy_id": "synthetic-live-edge",
            "strategy_version": "synthetic-live-edge-v1",
            "rules_hash": "a" * 64,
            "state": "WINNER_FROZEN",
            "plugin": {
                "module": "tests.synthetic_discovery_plugin",
                "evaluate_production": "evaluate_production",
            },
            "implementation_hashes": {
                str(implementation): hashlib.sha256(
                    implementation.read_bytes()
                ).hexdigest()
            },
        },
        work / "winner",
        "synthetic-live-winner",
    )


def _market_facts():
    return {
        "ranking_complete": True,
        "observed_at": (NOW - timedelta(seconds=1)).isoformat(),
        "symbol": "TEST",
        "halted": False,
        "tradable": True,
        "bid": 99.98,
        "ask": 100.00,
        "entry_limit": 100.01,
        "stop_price": 99.00,
        "expected_gross_move_fraction": 0.010,
        "holding_trading_days": 2,
        "before_open_account_reconciled": True,
        "before_open_orders_reconciled": True,
        "before_open_protection_reconciled": True,
        "before_open_tradability_reconciled": True,
        "before_open_news_reconciled": True,
        "protective_order_route_ready": True,
        "monitoring_ready": True,
        "protection_time_in_force": "gtc",
        "protection_failure_safe_cutoff": "15:45 ET",
        "exit_plan": {
            "type": "stop_or_maximum_hold_close",
            "maximum_hold_sessions": 2,
            "same_interval_ambiguity": "stop_first",
        },
        "executable_ask_depth": 20_000,
        "recent_real_minute_volume": 30_000,
    }


def _setup(winner):
    account = {"equity": 100_000, "buying_power": 100_000}
    evaluation = portfolio_execution.evaluate_frozen_winner(
        winner,
        _market_facts(),
        account,
        portfolio_maturity.load_config(),
        now=NOW,
    )
    review = {
        "passed": True,
        "confirmation_required": False,
        "confirmation_satisfied": False,
        "reviewed_symbol": "TEST",
        "reviewed_side": "buy",
        "reviewed_order_type": "limit",
        "reviewed_limit_price": evaluation["order"]["limit_price"],
        "reviewed_quantity": evaluation["order"]["quantity"],
        "buying_power_sufficient": True,
        "tradable": True,
        "halted": False,
        "blocking_alerts": [],
    }
    risk = evaluation["risk"]
    guard = {
        "schema_version": 1,
        "observed_at": NOW.isoformat(),
        "strategy_id": winner["strategy_id"],
        "strategy_version": winner["strategy_version"],
        "rules_hash": winner["rules_hash"],
        "broker_state": "FLAT_RECONCILED",
        "account_reconciled": True,
        "orders_reconciled": True,
        "positions_count": 0,
        "protected_positions_count": 0,
        "unknown_orders_count": 0,
        "unprotected_positions_count": 0,
        "new_entries_today": 0,
        "gross_notional_fraction": 0.0,
        "aggregate_planned_open_loss_fraction": 0.0,
        "daily_loss_fraction": 0.0,
        "weekly_loss_fraction": 0.0,
        "peak_to_trough_drawdown_fraction": 0.0,
        "proposed_position_loss_fraction": risk["planned_loss_fraction"],
        "proposed_gross_notional_fraction": risk["gross_notional_fraction"],
        "proposed_holding_trading_days": risk["holding_trading_days"],
        "tradable": True,
        "broker_review_passed": True,
        "broker_confirmation_required": False,
        "broker_confirmation_satisfied": False,
        "protective_order_route_ready": True,
        "monitoring_ready": True,
        "source": "synthetic privacy-safe broker reconciliation",
    }
    return {
        "schema_version": 1,
        "mode": "live",
        "market_facts": _market_facts(),
        "account": account,
        "broker_review": review,
        "guard_snapshot": guard,
    }, evaluation


def _report(winner):
    return {
        "strategies": [
            {
                "strategy_id": winner["strategy_id"],
                "strategy_version": winner["strategy_version"],
                "rules_hash": winner["rules_hash"],
                "pilot_ready": True,
                "maturity": "PILOT_READY",
            }
        ]
    }


def _tokens():
    cipher = sensitive_data.get_cipher()
    return {
        "entry_order": cipher.encrypt("entry-order", "broker_order_id"),
        "entry_ref": cipher.encrypt("entry-ref", "client_ref_id"),
        "protection_order": cipher.encrypt("protection-order", "broker_order_id"),
        "protection_ref": cipher.encrypt("protection-ref", "client_ref_id"),
        "exit_order": cipher.encrypt("exit-order", "broker_order_id"),
        "exit_ref": cipher.encrypt("exit-ref", "client_ref_id"),
    }


def _prepare(work: Path):
    root = work / "artifacts"
    winner_path, winner = _winner(work)
    setup, evaluation = _setup(winner)
    preparation_path, preparation = portfolio_live.prepare_live(
        winner_path,
        setup,
        root=root,
        enforce_commit=False,
        enforce_repository_checks=False,
        report=_report(winner),
        now=NOW,
    )
    return root, winner, evaluation, preparation_path, preparation


def _entry_and_protection(work: Path):
    root, winner, evaluation, preparation_path, preparation = _prepare(work)
    tokens = _tokens()
    quantity = evaluation["order"]["quantity"]
    entry_at = NOW + timedelta(seconds=1)
    entry_path, entry = portfolio_live.record_entry_result(
        preparation_path,
        {
            "schema_version": 1,
            "observed_at": entry_at.isoformat(),
            "logical_order_alias": "entry-one",
            "state": "filled",
            "filled_quantity": quantity,
            "remaining_quantity": 0,
            "average_fill_price": evaluation["order"]["limit_price"],
            "filled_at": entry_at.isoformat(),
            "encrypted_broker_order_id": tokens["entry_order"],
            "encrypted_client_ref_id": tokens["entry_ref"],
            "notification_status": "sent",
        },
        root=root,
        now=entry_at,
    )
    protection_at = entry_at + timedelta(seconds=2)
    protection_path, protection = portfolio_live.record_protection(
        entry_path,
        {
            "schema_version": 1,
            "observed_at": protection_at.isoformat(),
            "state": "accepted",
            "coverage_quantity": quantity,
            "entry_remainder_state": "none",
            "time_in_force": "gtc",
            "encrypted_broker_order_id": tokens["protection_order"],
            "encrypted_client_ref_id": tokens["protection_ref"],
            "notification_status": "sent",
        },
        root=root,
        now=protection_at,
    )
    return (
        root,
        winner,
        evaluation,
        preparation,
        entry,
        protection_path,
        protection,
        tokens,
    )


def _closure(work: Path, evaluation, tokens, close_at: datetime):
    journal = work / "live-journal.md"
    journal.write_text(
        "\n".join(
            [
                f"Broker order ID: {tokens['entry_order']}",
                f"Client ref ID: {tokens['entry_ref']}",
                f"Broker protective order ID: {tokens['protection_order']}",
                f"Client ref ID: {tokens['protection_ref']}",
                f"Broker exit order ID: {tokens['exit_order']}",
                f"Client ref ID: {tokens['exit_ref']}",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    return {
        "schema_version": 1,
        "observed_at": close_at.isoformat(),
        "exit_reason": "strategy_exit",
        "exit_quantity": evaluation["order"]["quantity"],
        "average_exit_price": 102.0,
        "expected_exit_price": 102.0,
        "realized_net_dollars": 200.0,
        "ending_equity": 100_200.0,
        "stop_executed": False,
        "stop_slippage_bps": 0.0,
        "stop_reserve_bps": 0.0,
        "exit_used_existing_protection": False,
        "encrypted_broker_order_id": tokens["exit_order"],
        "encrypted_client_ref_id": tokens["exit_ref"],
        "residual_orders": [
            {
                "logical_order_alias": "protective-stop-one",
                "state": "cancelled",
                "encrypted_broker_order_id": tokens["protection_order"],
            }
        ],
        "broker_snapshot": {
            "state": "FLAT_RECONCILED",
            "account_reconciled": True,
            "orders_reconciled": True,
            "positions_count": 0,
            "open_orders_count": 0,
            "unknown_orders_count": 0,
        },
        "monitoring_complete": True,
        "journal_complete": True,
        "session_capture_complete": True,
        "rule_violations": [],
        "journal_path": str(journal.relative_to(portfolio_live.PROJECT_ROOT)),
        "notification_status": "sent",
    }


def test_controlled_live_close_replays_flat_and_admits_only_after_inspection():
    with tempfile.TemporaryDirectory(dir=portfolio_live.PROJECT_ROOT) as directory:
        work = Path(directory)
        (
            root,
            _winner_artifact,
            evaluation,
            _preparation,
            _entry,
            protection_path,
            _protection,
            tokens,
        ) = _entry_and_protection(work)
        close_at = NOW + timedelta(days=2)
        final_path, final = portfolio_live.close_live(
            protection_path,
            _closure(work, evaluation, tokens, close_at),
            root=root,
            now=close_at,
        )
        assert final["state"] == "LIVE_CLOSED_RECONCILED_INSPECTION_REQUIRED"
        assert final["close_facts"]["flat_reconciled"] is True
        assert final["maturity_record"]["eligible"] is True

        inspection_path, inspection = portfolio_live_inspection.inspect_live(
            final_path, root=root
        )
        assert inspection["state"] == "LIVE_CLOSE_INSPECTED_ADMISSION_READY"
        ledger = work / "portfolio-signals.jsonl"
        admitted = portfolio_live_inspection.admit_live(
            inspection_path, ledger_path=ledger
        )
        assert admitted["state"] == "LIVE_CLOSE_ADMITTED"
        record = portfolio_maturity.read_records(ledger)[0]
        assert record["position_flat_confirmed"] is True
        assert record["residual_orders_terminal"] is True
        assert record["broker_actions"] == 3


def test_unknown_entry_is_reconciled_before_retry_or_protection():
    with tempfile.TemporaryDirectory(dir=portfolio_live.PROJECT_ROOT) as directory:
        work = Path(directory)
        root, _winner_artifact, evaluation, preparation_path, _preparation = _prepare(
            work
        )
        tokens = _tokens()
        quantity = evaluation["order"]["quantity"]
        entry_at = NOW + timedelta(seconds=1)
        entry_path, entry = portfolio_live.record_entry_result(
            preparation_path,
            {
                "schema_version": 1,
                "observed_at": entry_at.isoformat(),
                "logical_order_alias": "entry-unknown",
                "state": "unknown",
                "filled_quantity": 0,
                "remaining_quantity": quantity,
                "average_fill_price": None,
                "filled_at": None,
                "encrypted_broker_order_id": None,
                "encrypted_client_ref_id": tokens["entry_ref"],
                "notification_status": "skipped",
            },
            root=root,
            now=entry_at,
        )
        assert entry["state"] == "ENTRY_SUBMISSION_UNKNOWN_RECONCILE_REQUIRED"
        absent_at = entry_at + timedelta(seconds=1)
        _, absent = portfolio_live.reconcile_unknown_entry(
            entry_path,
            {
                "schema_version": 1,
                "observed_at": absent_at.isoformat(),
                "orders": [],
            },
            root=root,
            now=absent_at,
        )
        assert absent["decision"]["retry_permitted"] is True

        found_at = entry_at + timedelta(seconds=2)
        reconciliation_path, found = portfolio_live.reconcile_unknown_entry(
            entry_path,
            {
                "schema_version": 1,
                "observed_at": found_at.isoformat(),
                "orders": [
                    {
                        "logical_order_alias": "entry-unknown",
                        "state": "filled",
                        "filled_quantity": quantity,
                        "remaining_quantity": 0,
                        "average_fill_price": evaluation["order"]["limit_price"],
                        "filled_at": entry_at.isoformat(),
                        "encrypted_broker_order_id": tokens["entry_order"],
                    }
                ],
            },
            root=root,
            now=found_at,
        )
        assert found["state"] == "ENTRY_EXPOSURE_RECONCILED_PROTECT_NOW"
        protection_at = found_at + timedelta(seconds=1)
        _, protection = portfolio_live.record_protection(
            reconciliation_path,
            {
                "schema_version": 1,
                "observed_at": protection_at.isoformat(),
                "state": "accepted",
                "coverage_quantity": quantity,
                "entry_remainder_state": "none",
                "time_in_force": "gtc",
                "encrypted_broker_order_id": tokens["protection_order"],
                "encrypted_client_ref_id": tokens["protection_ref"],
                "notification_status": "sent",
            },
            root=root,
            now=protection_at,
        )
        assert protection["state"] == "LIVE_PROTECTED_MONITOR"


def test_nonterminal_residual_order_or_bad_identifier_fails_closed():
    with tempfile.TemporaryDirectory(dir=portfolio_live.PROJECT_ROOT) as directory:
        work = Path(directory)
        (
            root,
            _winner_artifact,
            evaluation,
            _preparation,
            _entry,
            protection_path,
            _protection,
            tokens,
        ) = _entry_and_protection(work)
        close_at = NOW + timedelta(days=2)
        closure = _closure(work, evaluation, tokens, close_at)
        closure["residual_orders"][0]["state"] = "open"
        with pytest.raises(portfolio_live.PortfolioLiveError, match="not terminal"):
            portfolio_live.close_live(
                protection_path, closure, root=root, now=close_at
            )


def test_partial_fill_remainder_must_be_terminal_before_protection():
    with tempfile.TemporaryDirectory(dir=portfolio_live.PROJECT_ROOT) as directory:
        work = Path(directory)
        root, _winner_artifact, evaluation, preparation_path, _preparation = _prepare(
            work
        )
        tokens = _tokens()
        requested = evaluation["order"]["quantity"]
        filled = max(1, requested // 2)
        entry_at = NOW + timedelta(seconds=1)
        entry_path, _entry = portfolio_live.record_entry_result(
            preparation_path,
            {
                "schema_version": 1,
                "observed_at": entry_at.isoformat(),
                "logical_order_alias": "entry-partial",
                "state": "partially_filled",
                "filled_quantity": filled,
                "remaining_quantity": requested - filled,
                "average_fill_price": evaluation["order"]["limit_price"],
                "filled_at": entry_at.isoformat(),
                "encrypted_broker_order_id": tokens["entry_order"],
                "encrypted_client_ref_id": tokens["entry_ref"],
                "notification_status": "sent",
            },
            root=root,
            now=entry_at,
        )
        protection_at = entry_at + timedelta(seconds=1)
        observation = {
            "schema_version": 1,
            "observed_at": protection_at.isoformat(),
            "state": "accepted",
            "coverage_quantity": filled,
            "entry_remainder_state": "open",
            "time_in_force": "gtc",
            "encrypted_broker_order_id": tokens["protection_order"],
            "encrypted_client_ref_id": tokens["protection_ref"],
            "notification_status": "sent",
        }
        with pytest.raises(portfolio_live.PortfolioLiveError, match="must be terminal"):
            portfolio_live.record_protection(
                entry_path, observation, root=root, now=protection_at
            )
        observation["entry_remainder_state"] = "cancelled"
        _, protected = portfolio_live.record_protection(
            entry_path, observation, root=root, now=protection_at
        )
        assert protected["state"] == "LIVE_PROTECTED_MONITOR"


def test_protection_failure_forces_flat_but_cannot_earn_live_admission():
    with tempfile.TemporaryDirectory(dir=portfolio_live.PROJECT_ROOT) as directory:
        work = Path(directory)
        root, _winner_artifact, evaluation, preparation_path, _preparation = _prepare(
            work
        )
        tokens = _tokens()
        quantity = evaluation["order"]["quantity"]
        entry_at = NOW + timedelta(seconds=1)
        entry_path, _entry = portfolio_live.record_entry_result(
            preparation_path,
            {
                "schema_version": 1,
                "observed_at": entry_at.isoformat(),
                "logical_order_alias": "entry-protection-failure",
                "state": "filled",
                "filled_quantity": quantity,
                "remaining_quantity": 0,
                "average_fill_price": evaluation["order"]["limit_price"],
                "filled_at": entry_at.isoformat(),
                "encrypted_broker_order_id": tokens["entry_order"],
                "encrypted_client_ref_id": tokens["entry_ref"],
                "notification_status": "sent",
            },
            root=root,
            now=entry_at,
        )
        protection_at = entry_at + timedelta(seconds=1)
        protection_path, failed = portfolio_live.record_protection(
            entry_path,
            {
                "schema_version": 1,
                "observed_at": protection_at.isoformat(),
                "state": "rejected",
                "coverage_quantity": 0,
                "entry_remainder_state": "none",
                "time_in_force": "gtc",
                "encrypted_broker_order_id": None,
                "encrypted_client_ref_id": None,
                "notification_status": "sent",
            },
            root=root,
            now=protection_at,
        )
        assert failed["state"] == "PROTECTION_FAILED_FLATTEN_REQUIRED"
        close_at = protection_at + timedelta(seconds=5)
        closure = _closure(work, evaluation, tokens, close_at)
        closure["exit_reason"] = "protection_failure"
        closure["average_exit_price"] = 99.5
        closure["expected_exit_price"] = 99.5
        closure["realized_net_dollars"] = -255.0
        closure["ending_equity"] = 99_745.0
        final_path, final = portfolio_live.close_live(
            protection_path, closure, root=root, now=close_at
        )
        assert final["state"] == "LIVE_CLOSED_SAFETY_FAILURE"
        inspection_path, inspection = portfolio_live_inspection.inspect_live(
            final_path, root=root
        )
        assert inspection["state"] == "LIVE_CLOSE_INSPECTED_NONQUALIFYING"
        with pytest.raises(
            portfolio_live_inspection.PortfolioLiveInspectionError,
            match="not eligible",
        ):
            portfolio_live_inspection.admit_live(
                inspection_path, ledger_path=work / "signals.jsonl"
            )

        closure = _closure(work, evaluation, tokens, close_at)
        closure["encrypted_broker_order_id"] = "enc:fernet:v1:not-authentic"
        with pytest.raises(portfolio_live.PortfolioLiveError, match="invalid"):
            portfolio_live.close_live(
                protection_path, closure, root=root, now=close_at
            )


def test_reconciled_fill_requires_authenticated_broker_identifier():
    with tempfile.TemporaryDirectory(dir=portfolio_live.PROJECT_ROOT) as directory:
        work = Path(directory)
        root, _winner_artifact, evaluation, preparation_path, _preparation = _prepare(
            work
        )
        tokens = _tokens()
        quantity = evaluation["order"]["quantity"]
        entry_at = NOW + timedelta(seconds=1)
        entry_path, _entry = portfolio_live.record_entry_result(
            preparation_path,
            {
                "schema_version": 1,
                "observed_at": entry_at.isoformat(),
                "logical_order_alias": "entry-missing-broker-id",
                "state": "unknown",
                "filled_quantity": 0,
                "remaining_quantity": quantity,
                "average_fill_price": None,
                "filled_at": None,
                "encrypted_broker_order_id": None,
                "encrypted_client_ref_id": tokens["entry_ref"],
                "notification_status": "skipped",
            },
            root=root,
            now=entry_at,
        )
        reconciled_at = entry_at + timedelta(seconds=1)
        with pytest.raises(
            portfolio_live.PortfolioLiveError,
            match="encrypted broker order ID",
        ):
            portfolio_live.reconcile_unknown_entry(
                entry_path,
                {
                    "schema_version": 1,
                    "observed_at": reconciled_at.isoformat(),
                    "orders": [
                        {
                            "logical_order_alias": "entry-missing-broker-id",
                            "state": "filled",
                            "filled_quantity": quantity,
                            "remaining_quantity": 0,
                            "average_fill_price": evaluation["order"]["limit_price"],
                            "filled_at": entry_at.isoformat(),
                            "encrypted_broker_order_id": None,
                        }
                    ],
                },
                root=root,
                now=reconciled_at,
            )


def test_live_close_rejects_lifecycle_binding_drift():
    with tempfile.TemporaryDirectory(dir=portfolio_live.PROJECT_ROOT) as directory:
        work = Path(directory)
        (
            _root,
            _winner_artifact,
            evaluation,
            preparation,
            _entry,
            protection_path,
            protection,
            tokens,
        ) = _entry_and_protection(work)
        exposure = strategy_discovery.load_artifact(
            portfolio_live.PROJECT_ROOT / protection["exposure_path"]
        )
        protection = dict(protection)
        protection["exposure_sha256"] = "0" * 64
        with pytest.raises(portfolio_live.PortfolioLiveError, match="binding drifted"):
            portfolio_live.rebuild_close(
                protection,
                exposure,
                preparation,
                _closure(work, evaluation, tokens, NOW + timedelta(days=2)),
            )

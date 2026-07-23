from __future__ import annotations

import hashlib
import json
import tempfile
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

import portfolio_maturity
import portfolio_shadow
import portfolio_shadow_inspection
import strategy_discovery


NOW = datetime(2026, 7, 22, 14, 0, 4, tzinfo=UTC)


def test_shadow_clis_report_predecessor_failures_as_structured_json(
    monkeypatch,
    capsys,
):
    def reject(*_args, **_kwargs):
        raise strategy_discovery.StrategyDiscoveryError(
            "predecessor artifact must be committed"
        )

    monkeypatch.setattr(portfolio_shadow, "start_shadow", reject)
    monkeypatch.setattr(portfolio_shadow, "_read_object", lambda _path: {})
    assert (
        portfolio_shadow.main(
            ["start", "queue.json", "setup.json"]
        )
        == 1
    )
    shadow_error = json.loads(capsys.readouterr().err)
    assert shadow_error == {
        "error": "predecessor artifact must be committed",
        "error_type": "StrategyDiscoveryError",
    }

    monkeypatch.setattr(portfolio_shadow_inspection, "inspect_shadow", reject)
    assert (
        portfolio_shadow_inspection.main(["inspect", "final.json"])
        == 1
    )
    inspection_error = json.loads(capsys.readouterr().err)
    assert inspection_error == {
        "error": "predecessor artifact must be committed",
        "error_type": "StrategyDiscoveryError",
    }


def _winner_and_queue(work: Path):
    implementation = Path("tests/synthetic_discovery_plugin.py")
    winner_path, winner = strategy_discovery._write_artifact(
        {
            "schema_version": 1,
            "artifact_kind": "frozen-strategy-winner",
            "campaign_id": strategy_discovery.CAMPAIGN_ID,
            "family_id": "synthetic-shadow-family",
            "strategy_id": "synthetic-shadow-edge",
            "strategy_version": "synthetic-shadow-edge-v1",
            "rules_hash": "a" * 64,
            "state": "WINNER_FROZEN",
            "recorded_at": (NOW - timedelta(minutes=1)).isoformat(),
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
        "synthetic-winner",
    )
    confirmation_path, confirmation = strategy_discovery._write_artifact(
        {
            "schema_version": 1,
            "artifact_kind": "confirmation-inspection",
            "campaign_id": strategy_discovery.CAMPAIGN_ID,
            "family_id": winner["family_id"],
            "strategy_id": winner["strategy_id"],
            "strategy_version": winner["strategy_version"],
            "rules_hash": winner["rules_hash"],
            "state": "CONFIRMATION_PASSED",
            "winner_sha256": winner["artifact_sha256"],
            "shadow_queue_permitted": True,
            "broker_actions_permitted": False,
        },
        work / "confirmation",
        "synthetic-confirmation",
    )
    historical_path, historical = strategy_discovery._write_artifact(
        {
            "schema_version": 1,
            "artifact_kind": "historical-maturity-ledger",
            "campaign_id": strategy_discovery.CAMPAIGN_ID,
            "family_id": winner["family_id"],
            "strategy_id": winner["strategy_id"],
            "strategy_version": winner["strategy_version"],
            "rules_hash": winner["rules_hash"],
            "state": "HISTORICAL_EVIDENCE_INSPECTED",
            "confirmation_inspection_path": strategy_discovery._relative(
                confirmation_path
            ),
            "append_permitted": True,
            "broker_actions_permitted": False,
        },
        work / "historical",
        "synthetic-historical",
    )
    queue_path, queue = strategy_discovery._write_artifact(
        {
            "schema_version": 1,
            "artifact_kind": "prospective-shadow-queue",
            "campaign_id": strategy_discovery.CAMPAIGN_ID,
            "family_id": winner["family_id"],
            "strategy_id": winner["strategy_id"],
            "strategy_version": winner["strategy_version"],
            "rules_hash": winner["rules_hash"],
            "state": "SHADOW_QUEUED",
            "queued_at": (NOW - timedelta(seconds=10)).isoformat(),
            "winner_path": strategy_discovery._relative(winner_path),
            "winner_sha256": winner["artifact_sha256"],
            "required_clean_closed_shadows": 5,
            "completed_clean_closed_shadows": 0,
            "confirmation_inspection_path": strategy_discovery._relative(
                confirmation_path
            ),
            "confirmation_inspection_sha256": confirmation["artifact_sha256"],
            "historical_maturity_ledger_path": strategy_discovery._relative(
                historical_path
            ),
            "historical_maturity_ledger_sha256": historical["artifact_sha256"],
            "historical_records_admitted": 1,
            "historical_admission_verified": True,
            "historical_validation_phase": "SHADOW_QUALIFICATION",
            "broker_actions_permitted": False,
        },
        work / "queue",
        "synthetic-queue",
    )
    return queue_path, queue


def _setup(*, ask: float = 100.0):
    market_observed = NOW - timedelta(seconds=2)
    fill_observed = NOW - timedelta(seconds=1)
    return {
        "schema_version": 1,
        "session_date": "2026-07-22",
        "market_facts": {
            "ranking_complete": True,
            "observed_at": market_observed.isoformat(),
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
        },
        "account": {"equity": 100_000, "buying_power": 100_000},
        "fill_observation": {
            "observed_at": fill_observed.isoformat(),
            "bid": 99.98,
            "ask": ask,
            "available_ask_quantity": 500,
        },
    }


def _closure(entry, close_now: datetime, *, violation: bool = False):
    fill_at = datetime.fromisoformat(entry["fill"]["observed_at"])
    return {
        "schema_version": 1,
        "protection_observation": {
            "planned_at": (fill_at - timedelta(seconds=1)).isoformat(),
            "ready_at": (fill_at + timedelta(seconds=2)).isoformat(),
            "time_in_force": "gtc",
            "failure_safe_cutoff": "15:45 ET",
        },
        "exit_observation": {
            "observed_at": (close_now - timedelta(seconds=1)).isoformat(),
            "bid": 102.00,
            "ask": 102.02,
            "reason": "target",
        },
        "monitoring_complete": True,
        "journal_complete": True,
        "session_capture_complete": True,
        "rule_violations": ["synthetic capture violation"] if violation else [],
    }


def test_shadow_lifecycle_replays_and_only_inspection_admits_ledger_row():
    with tempfile.TemporaryDirectory(dir=portfolio_shadow.PROJECT_ROOT) as directory:
        work = Path(directory)
        root = work / "artifacts"
        queue_path, _ = _winner_and_queue(work)
        entry_path, entry = portfolio_shadow.start_shadow(
            queue_path,
            _setup(),
            root=root,
            enforce_commit=False,
            now=NOW,
        )
        assert entry["state"] == "SHADOW_ENTRY_FILLED"
        assert entry["fill"]["fill_price"] == 100.0
        assert entry["broker_actions_performed"] == 0

        close_now = NOW + timedelta(days=2)
        final_path, final = portfolio_shadow.close_shadow(
            entry_path,
            _closure(entry, close_now),
            root=root,
            enforce_commit=False,
            now=close_now,
        )
        assert final["state"] == "SHADOW_CLOSED_CLEAN"
        assert final["maturity_record"]["eligible"] is True
        assert final["final"]["net_r"] > 0

        inspection_path, inspection = portfolio_shadow_inspection.inspect_shadow(
            final_path, root=root, enforce_commit=False
        )
        assert inspection["inspection"]["valid"] is True
        assert inspection["inspection"]["committed_shadow_queue_rebuilt"] is True
        ledger = work / "signals.jsonl"
        admitted = portfolio_shadow_inspection.admit_shadow(
            inspection_path, ledger_path=ledger, enforce_commit=False
        )
        assert admitted["state"] == "SHADOW_ADMITTED"
        records = portfolio_maturity.read_records(ledger)
        assert len(records) == 1
        assert records[0]["broker_actions"] == 0


def test_missed_marketable_limit_is_recorded_but_not_qualification_evidence():
    with tempfile.TemporaryDirectory(dir=portfolio_shadow.PROJECT_ROOT) as directory:
        work = Path(directory)
        root = work / "artifacts"
        queue_path, _ = _winner_and_queue(work)
        entry_path, entry = portfolio_shadow.start_shadow(
            queue_path,
            _setup(ask=100.02),
            root=root,
            enforce_commit=False,
            now=NOW,
        )
        assert entry["state"] == "SHADOW_ENTRY_MISSED"
        closure = {
            "schema_version": 1,
            "protection_observation": None,
            "exit_observation": None,
            "monitoring_complete": True,
            "journal_complete": True,
            "session_capture_complete": True,
            "rule_violations": [],
        }
        _, final = portfolio_shadow.close_shadow(
            entry_path,
            closure,
            root=root,
            enforce_commit=False,
            now=NOW + timedelta(seconds=3),
        )
        assert final["maturity_record"]["closed"] is True
        assert final["maturity_record"]["eligible"] is False
        assert final["maturity_record"]["net_r"] == 0.0


def test_shadow_start_rejects_stale_quote_and_private_identifier_fields():
    with tempfile.TemporaryDirectory(dir=portfolio_shadow.PROJECT_ROOT) as directory:
        work = Path(directory)
        queue_path, _ = _winner_and_queue(work)
        stale = _setup()
        stale["market_facts"]["observed_at"] = (
            NOW - timedelta(seconds=6)
        ).isoformat()
        with pytest.raises(portfolio_shadow.PortfolioShadowError, match="stale"):
            portfolio_shadow.start_shadow(
                queue_path, stale, root=work / "artifacts", enforce_commit=False, now=NOW
            )
        private = _setup()
        private["market_facts"]["broker_order_id"] = "forbidden"
        with pytest.raises(portfolio_shadow.PortfolioShadowError, match="private field"):
            portfolio_shadow.start_shadow(
                queue_path,
                private,
                root=work / "artifacts",
                enforce_commit=False,
                now=NOW,
            )


def test_shadow_start_rejects_prequeue_observation_and_predecessor_drift():
    with tempfile.TemporaryDirectory(dir=portfolio_shadow.PROJECT_ROOT) as directory:
        work = Path(directory)
        queue_path, queue = _winner_and_queue(work)
        future_queue = {
            key: value for key, value in queue.items() if key != "artifact_sha256"
        }
        future_queue["queued_at"] = (NOW + timedelta(seconds=1)).isoformat()
        future_path, _ = strategy_discovery._write_artifact(
            future_queue, work / "future-queue", "future-queue"
        )
        with pytest.raises(
            portfolio_shadow.PortfolioShadowError,
            match="follow queue activation",
        ):
            portfolio_shadow.start_shadow(
                future_path,
                _setup(),
                root=work / "artifacts",
                enforce_commit=False,
                now=NOW,
            )

        drifted_queue = {
            key: value for key, value in queue.items() if key != "artifact_sha256"
        }
        drifted_queue["confirmation_inspection_sha256"] = "b" * 64
        drifted_path, _ = strategy_discovery._write_artifact(
            drifted_queue, work / "drifted-queue", "drifted-queue"
        )
        with pytest.raises(
            portfolio_shadow.PortfolioShadowError,
            match="confirmation-inspection binding drifted",
        ):
            portfolio_shadow.start_shadow(
                drifted_path,
                _setup(),
                root=work / "artifacts",
                enforce_commit=False,
                now=NOW,
            )

        assert queue_path.is_file()


def test_shadow_inspection_reopens_exact_queue_binding():
    with tempfile.TemporaryDirectory(dir=portfolio_shadow.PROJECT_ROOT) as directory:
        work = Path(directory)
        root = work / "artifacts"
        queue_path, _ = _winner_and_queue(work)
        entry_path, entry = portfolio_shadow.start_shadow(
            queue_path,
            _setup(),
            root=root,
            enforce_commit=False,
            now=NOW,
        )
        forged_entry = {
            key: value for key, value in entry.items() if key != "artifact_sha256"
        }
        forged_entry["queue_sha256"] = "c" * 64
        forged_entry_path, forged_entry_artifact = strategy_discovery._write_artifact(
            forged_entry, root / "forged-entry", "forged-entry"
        )
        close_now = NOW + timedelta(hours=1)
        final_path, _ = portfolio_shadow.close_shadow(
            forged_entry_path,
            _closure(forged_entry_artifact, close_now),
            root=root,
            enforce_commit=False,
            now=close_now,
        )
        with pytest.raises(
            portfolio_shadow_inspection.PortfolioShadowInspectionError,
            match="queue binding drifted",
        ):
            portfolio_shadow_inspection.inspect_shadow(
                final_path, root=root, enforce_commit=False
            )
        assert entry_path.is_file()


def test_missed_shadow_close_cannot_predate_evaluation():
    with tempfile.TemporaryDirectory(dir=portfolio_shadow.PROJECT_ROOT) as directory:
        work = Path(directory)
        root = work / "artifacts"
        queue_path, _ = _winner_and_queue(work)
        entry_path, _ = portfolio_shadow.start_shadow(
            queue_path,
            _setup(ask=100.02),
            root=root,
            enforce_commit=False,
            now=NOW,
        )
        closure = {
            "schema_version": 1,
            "protection_observation": None,
            "exit_observation": None,
            "monitoring_complete": True,
            "journal_complete": True,
            "session_capture_complete": True,
            "rule_violations": [],
        }
        with pytest.raises(
            portfolio_shadow.PortfolioShadowError,
            match="close predates its evaluation",
        ):
            portfolio_shadow.close_shadow(
                entry_path,
                closure,
                root=root,
                enforce_commit=False,
                now=NOW - timedelta(seconds=1),
            )


def test_rule_violation_produces_reset_evidence_not_clean_shadow():
    with tempfile.TemporaryDirectory(dir=portfolio_shadow.PROJECT_ROOT) as directory:
        work = Path(directory)
        root = work / "artifacts"
        queue_path, _ = _winner_and_queue(work)
        entry_path, entry = portfolio_shadow.start_shadow(
            queue_path,
            _setup(),
            root=root,
            enforce_commit=False,
            now=NOW,
        )
        close_now = NOW + timedelta(days=2)
        _, final = portfolio_shadow.close_shadow(
            entry_path,
            _closure(entry, close_now, violation=True),
            root=root,
            enforce_commit=False,
            now=close_now,
        )
        assert final["state"] == "SHADOW_CLOSED_NONQUALIFYING"
        assert final["maturity_record"]["eligible"] is False
        assert final["maturity_record"]["rule_violations"]


def test_incomplete_protection_or_monitoring_is_automatically_reset_evidence():
    with tempfile.TemporaryDirectory(dir=portfolio_shadow.PROJECT_ROOT) as directory:
        work = Path(directory)
        root = work / "artifacts"
        queue_path, _ = _winner_and_queue(work)
        entry_path, entry = portfolio_shadow.start_shadow(
            queue_path,
            _setup(),
            root=root,
            enforce_commit=False,
            now=NOW,
        )
        close_now = NOW + timedelta(hours=1)
        closure = _closure(entry, close_now)
        closure["protection_observation"] = None
        closure["monitoring_complete"] = False
        closure["exit_observation"]["reason"] = "safe_cutoff"
        _, final = portfolio_shadow.close_shadow(
            entry_path,
            closure,
            root=root,
            enforce_commit=False,
            now=close_now,
        )
        record = final["maturity_record"]
        assert record["eligible"] is False
        assert "shadow_protection_path_incomplete" in record["rule_violations"]
        assert "shadow_monitoring_incomplete" in record["rule_violations"]

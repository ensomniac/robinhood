from __future__ import annotations

import json
import tempfile
import time
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest

import portfolio_guard
import portfolio_maturity
import portfolio_shadow
import portfolio_shadow_inspection
import strategy_discovery as discovery
import tests.synthetic_discovery_plugin as synthetic_plugin
from learning_data import freeze_dataset_contract
from learning_experiment import DEVELOPMENT_SEARCH_RULE, enumerate_trials
from learning_experiment import build_rolling_origin_plan


def _dates(start: date, count: int) -> list[str]:
    return [(start + timedelta(days=index)).isoformat() for index in range(count)]


def family_contract(
    dataset_manifest: Path,
    *,
    family_id: str = "synthetic-edge-family",
    mode: str = "genuine",
    capacity: int = 120,
    confirmation_count: int = 30,
) -> dict:
    return {
        "schema_version": 1,
        "campaign_id": discovery.CAMPAIGN_ID,
        "experiment_id": f"experiment-{family_id}",
        "family_id": family_id,
        "strategy_id": f"strategy-{family_id}",
        "parent_experiment_id": None,
        "created_at": "2026-07-22T19:00:00-04:00",
        "status": "INVENTED",
        "mechanism": "Synthetic persistent flow produces stable account growth.",
        "expected_holding_behavior": "Hold no longer than five sessions.",
        "dataset_lane": "development",
        "universe_requirements": {"claim_scope": "SYNTHETIC_TEST_ONLY"},
        "entry_rule": "Enter on the next observable open.",
        "stop_rule": "Exit at the frozen structural stop.",
        "exit_rule": "Exit at target, stop, or the fifth session.",
        "ranking_rule": "Rank descending strength then canonical identifier.",
        "selection_rule": "At most one new family entry per day.",
        "parameter_grid": {"lookback": [1, 3], "hold": [2, 5]},
        "selection_mode": "development_search",
        "winner_selection": DEVELOPMENT_SEARCH_RULE,
        "primary_outcome": "selection-adjusted account log growth",
        "execution_assumptions": {
            "next_observable_fill": True,
            "ambiguity": "stop_first",
            "maximum_hold_sessions": 5,
        },
        "falsification_criteria": {
            "minimum_20bps_log_growth": 0.0,
            "minimum_stressed_profit_factor": 1.2,
            "maximum_drawdown_r": 6.0,
        },
        "minimum_evidence": {"configured_floor": 50, "power": 0.8},
        "contamination_risks": ["Synthetic dates must remain partitioned."],
        "production_compatibility_risks": [
            "Live spread and depth still require fresh evaluation."
        ],
        "material_difference_rationale": (
            "This synthetic fixture exercises a distinct causal flow mechanism."
        ),
        "development_dates": _dates(date(2025, 1, 2), 120),
        "embargo_dates": _dates(date(2025, 6, 1), 5),
        "confirmation_dates": _dates(date(2025, 6, 6), confirmation_count),
        "universe": {"symbols": ["SYNTH"], "point_in_time": True},
        "costs_bps_per_side": [5, 10, 20],
        "partitions": {"rolling_origin": True, "confirmation_untouched": True},
        "falsifiers": ["nonpositive stressed log growth", "unstable neighbors"],
        "implementation_files": ["tests/synthetic_discovery_plugin.py"],
        "plugin": {
            "module": "tests.synthetic_discovery_plugin",
            "preflight": "preflight",
            "evaluate_development": "evaluate_development",
            "evaluate_confirmation": "evaluate_confirmation",
            "evaluate_production": "evaluate_production",
        },
        "capacity_policy": {"retire_below": 50, "fast_lane_at": 100},
        "synthetic_capacity": capacity,
        "synthetic_mode": mode,
        "dataset_manifest": str(dataset_manifest),
    }


def _dataset(root: Path) -> Path:
    path, _ = freeze_dataset_contract(
        {
            "schema_version": 1,
            "dataset_id": "dataset-synthetic-discovery",
            "registered_at": "2026-07-22T19:00:00-04:00",
            "requested_dates": _dates(date(2025, 1, 2), 120),
            "dataset_payload": {
                "lane": "development",
                "claim_scope": "DEVELOPMENT_ONLY",
                "evidence_paths": ["tests/synthetic_discovery_plugin.py"],
                "inspected": True,
                "point_in_time_evidence": True,
            },
        },
        root / "datasets",
    )
    return path


def _write_contract(root: Path, value: dict) -> Path:
    path = root / f"{value['family_id']}.json"
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _freeze_synthetic_winner(work: Path, *, confirmation_count: int = 30):
    artifact_root = work / "artifacts"
    contract_path = _write_contract(
        work,
        family_contract(
            _dataset(work),
            family_id="transition-controls",
            confirmation_count=confirmation_count,
        ),
    )
    discovery.run_preflight(
        contract_path, root=artifact_root, enforce_commit=False
    )
    search_path, _ = discovery.freeze_search(
        contract_path, root=artifact_root, enforce_commit=False
    )
    development_path, _ = discovery.evaluate_development(
        search_path, root=artifact_root, enforce_commit=False
    )
    inspection_path, inspection = discovery.inspect_development(
        development_path, root=artifact_root, enforce_commit=False
    )
    if inspection["state"] != "WINNER_SELECTED":
        return artifact_root, inspection_path, inspection, None
    winner_path, winner = discovery.freeze_winner(
        inspection_path,
        root=artifact_root,
        enforce_commit=False,
        recorded_at="2026-07-23T00:00:00-04:00",
    )
    return artifact_root, winner_path, inspection, winner


def test_genuine_edge_reaches_frozen_shadow_queue_without_broker_actions():
    with tempfile.TemporaryDirectory(dir=discovery.PROJECT_ROOT) as directory:
        work = Path(directory)
        artifact_root = work / "artifacts"
        contract = family_contract(_dataset(work), family_id="genuine-edge")
        contract_path = _write_contract(work, contract)
        preflight_path, preflight = discovery.run_preflight(
            contract_path, root=artifact_root, enforce_commit=False
        )
        assert preflight["state"] == "CAPACITY_READY"
        assert preflight["validated_contract_sha256"]
        assert preflight["implementation_sha256"]
        search_path, search = discovery.freeze_search(
            contract_path, root=artifact_root, enforce_commit=False
        )
        assert search["trial_count"] == 4
        assert search["family_contract"]["rolling_origin_plan"] == (
            build_rolling_origin_plan(contract["development_dates"])
        )
        development_path, development = discovery.evaluate_development(
            search_path, root=artifact_root, enforce_commit=False
        )
        assert development["provider_telemetry"]["requests"] == 0
        inspection_path, inspection = discovery.inspect_development(
            development_path, root=artifact_root, enforce_commit=False
        )
        assert inspection["state"] == "WINNER_SELECTED"
        assert inspection["selection"]["required_total_signals"] >= 50
        with pytest.raises(
            discovery.StrategyDiscoveryError,
            match="must follow family-contract creation",
        ):
            discovery.freeze_winner(
                inspection_path,
                root=artifact_root,
                enforce_commit=False,
                recorded_at=contract["created_at"],
            )
        winner_path, winner = discovery.freeze_winner(
            inspection_path,
            root=artifact_root,
            enforce_commit=False,
            recorded_at="2026-07-23T00:00:00-04:00",
        )
        assert winner["confirmation_parameter_alternatives"] == 0
        assert winner["recorded_at"] == "2026-07-23T00:00:00-04:00"
        assert winner["family_contract_created_at"] == contract["created_at"]
        assert winner["exact_rules"]["universe"] == contract["universe"]
        confirmation_path, _ = discovery.evaluate_confirmation(
            winner_path, root=artifact_root, enforce_commit=False
        )
        confirmation_inspection_path, confirmation = (
            discovery.inspect_confirmation(
                confirmation_path, root=artifact_root, enforce_commit=False
            )
        )
        assert confirmation["state"] == "CONFIRMATION_PASSED"
        historical_path = next(
            (
                artifact_root / winner["family_id"] / "maturity-ledger"
            ).glob("*.json")
        )
        portfolio_ledger = work / "PORTFOLIO_SIGNALS.jsonl"
        with pytest.raises(
            discovery.StrategyDiscoveryError,
            match="has not been admitted unchanged",
        ):
            discovery.queue_shadow(
                winner_path,
                root=artifact_root,
                ledger_path=portfolio_ledger,
                enforce_commit=False,
            )
        admission = discovery.admit_historical(
            historical_path,
            ledger_path=portfolio_ledger,
            enforce_commit=False,
        )
        assert admission["admitted"] > 0
        repeated = discovery.admit_historical(
            historical_path,
            ledger_path=portfolio_ledger,
            enforce_commit=False,
        )
        assert repeated["admitted"] == 0
        assert repeated["already_present"] == admission["total_requested"]
        queue_path, queue = discovery.queue_shadow(
            winner_path,
            root=artifact_root,
            ledger_path=portfolio_ledger,
            enforce_commit=False,
            queued_at="2026-07-24T00:00:00-04:00",
        )
        assert queue_path.is_file()
        assert queue["state"] == "SHADOW_QUEUED"
        assert datetime.fromisoformat(
            queue["queued_at"].replace("Z", "+00:00")
        ) > datetime.fromisoformat(winner["recorded_at"])
        assert queue["required_clean_closed_shadows"] == 5
        assert queue["historical_admission_verified"] is True
        assert queue["historical_records_admitted"] == admission["total_requested"]
        assert queue["historical_validation_phase"] == "SHADOW_QUALIFICATION"
        assert queue["broker_actions_permitted"] is False
        assert confirmation_inspection_path.is_file()
        assert preflight_path.is_file()
        status = discovery.build_status(root=artifact_root)
        family_status = next(
            item
            for item in status["families"]
            if item["family_id"] == winner["family_id"]
        )
        assert family_status["current_state"] == "SHADOW_QUEUED"
        assert family_status["trial_count"] == 4
        assert family_status["power_target"] == winner["power_target"]
        assert family_status["required_total_signals"] == winner[
            "required_total_signals"
        ]
        assert family_status["development_filled_signals"] == inspection[
            "selection"
        ]["development_filled_signals"]
        assert family_status["maximum_total_signal_capacity"] == inspection[
            "maximum_total_signal_capacity"
        ]
        assert family_status["required_confirmation_signals"] == winner[
            "required_confirmation_signals"
        ]
        assert family_status["confirmation_reserved_sessions"] == len(
            winner["confirmation_dates"]
        )
        assert family_status["confirmation_state"] == "CONFIRMATION_PASSED"
        assert family_status["shadow_progress"] == {
            "required_clean_closed": 5,
            "completed_clean_closed": 0,
            "attempts": 0,
            "qualification_resets": 0,
        }
        assert family_status["blockers"] == [
            "clean closed shadows 0 is below required 5"
        ]
        for index in range(5):
            shadow_now = datetime(
                2026, 8, 1 + index * 3, 14, 0, 4, tzinfo=UTC
            )
            setup = {
                "schema_version": 1,
                "session_date": shadow_now.date().isoformat(),
                "market_facts": {
                    "ranking_complete": True,
                    "observed_at": (shadow_now - timedelta(seconds=2)).isoformat(),
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
                    "observed_at": (shadow_now - timedelta(seconds=1)).isoformat(),
                    "bid": 99.98,
                    "ask": 100.00,
                    "available_ask_quantity": 500,
                },
            }
            entry_path, entry = portfolio_shadow.start_shadow(
                queue_path,
                setup,
                root=artifact_root,
                enforce_commit=False,
                now=shadow_now,
            )
            close_now = shadow_now + timedelta(days=2)
            fill_at = datetime.fromisoformat(entry["fill"]["observed_at"])
            final_path, final = portfolio_shadow.close_shadow(
                entry_path,
                {
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
                    "rule_violations": [],
                },
                root=artifact_root,
                enforce_commit=False,
                now=close_now,
            )
            assert final["state"] == "SHADOW_CLOSED_CLEAN"
            shadow_inspection_path, _ = portfolio_shadow_inspection.inspect_shadow(
                final_path, root=artifact_root, enforce_commit=False
            )
            portfolio_shadow_inspection.admit_shadow(
                shadow_inspection_path,
                ledger_path=portfolio_ledger,
                enforce_commit=False,
            )
        records = portfolio_maturity.read_records(portfolio_ledger)
        report = portfolio_maturity.build_report(
            records, portfolio_maturity.load_config()
        )
        assert report["pilot_ready_strategy_count"] == 1
        exact = report["strategies"][0]
        assert exact["maturity"] == "PILOT_READY"
        now = datetime.now(UTC)
        guard = portfolio_guard.evaluate_entry(
            {
                "schema_version": 1,
                "observed_at": now.isoformat(),
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
                "proposed_position_loss_fraction": 0.005,
                "proposed_gross_notional_fraction": 0.25,
                "proposed_holding_trading_days": 2,
                "tradable": True,
                "broker_review_passed": True,
                "broker_confirmation_required": False,
                "broker_confirmation_satisfied": False,
                "protective_order_route_ready": True,
                "monitoring_ready": True,
                "source": "synthetic privacy-safe reconciliation",
            },
            report,
            portfolio_maturity.load_config(),
            now=now,
        )
        assert guard["status"] == "ENTRY_READY"


def test_development_transitions_reopen_every_committed_predecessor(monkeypatch):
    with tempfile.TemporaryDirectory(dir=discovery.PROJECT_ROOT) as directory:
        work = Path(directory)
        artifact_root = work / "artifacts"
        dataset_path = _dataset(work)
        contract_path = _write_contract(
            work,
            family_contract(dataset_path, family_id="committed-chain"),
        )
        discovery.run_preflight(
            contract_path, root=artifact_root, enforce_commit=False
        )
        search_path, _search = discovery.freeze_search(
            contract_path, root=artifact_root, enforce_commit=False
        )
        checked: list[Path] = []
        monkeypatch.setattr(
            discovery,
            "require_committed",
            lambda path: checked.append(Path(path).resolve()),
        )

        development_path, _development = discovery.evaluate_development(
            search_path, root=artifact_root
        )
        assert checked == [
            search_path.resolve(),
            (
                discovery.PROJECT_ROOT
                / "tests/synthetic_discovery_plugin.py"
            ).resolve(),
            dataset_path.resolve(),
        ]

        checked.clear()
        inspection_path, inspection = discovery.inspect_development(
            development_path, root=artifact_root
        )
        assert inspection["state"] == "WINNER_SELECTED"
        assert checked == [
            development_path.resolve(),
            search_path.resolve(),
            dataset_path.resolve(),
        ]

        checked.clear()
        discovery.freeze_winner(
            inspection_path,
            root=artifact_root,
            recorded_at="2026-07-23T00:00:00-04:00",
        )
        assert checked == [
            inspection_path.resolve(),
            development_path.resolve(),
            search_path.resolve(),
            dataset_path.resolve(),
        ]


def test_winner_freeze_rejects_forged_development_result_binding():
    with tempfile.TemporaryDirectory(dir=discovery.PROJECT_ROOT) as directory:
        work = Path(directory)
        artifact_root, _winner_path, inspection, _winner = (
            _freeze_synthetic_winner(work)
        )
        assert inspection["state"] == "WINNER_SELECTED"
        inspection_path = next(
            (
                artifact_root
                / "transition-controls"
                / "development-inspection"
            ).glob("*.json")
        )
        forged = {
            key: value
            for key, value in discovery.load_artifact(
                inspection_path,
                expected_kind="development-search-inspection",
            ).items()
            if key != "artifact_sha256"
        }
        forged["result_sha256"] = "f" * 64
        forged_path, _forged = discovery._write_artifact(
            forged,
            artifact_root / "forged-inspection",
            "forged-development-inspection",
        )

        with pytest.raises(
            discovery.StrategyDiscoveryError,
            match="development result binding drifted",
        ):
            discovery.freeze_winner(
                forged_path,
                root=artifact_root,
                enforce_commit=False,
                recorded_at="2026-07-23T00:00:01-04:00",
            )


def test_confirmation_inspection_reopens_committed_winner_evidence(monkeypatch):
    with tempfile.TemporaryDirectory(dir=discovery.PROJECT_ROOT) as directory:
        work = Path(directory)
        artifact_root, winner_path, inspection, winner = (
            _freeze_synthetic_winner(work)
        )
        assert inspection["state"] == "WINNER_SELECTED"
        confirmation_path, confirmation = discovery.evaluate_confirmation(
            winner_path,
            root=artifact_root,
            enforce_commit=False,
        )
        confirmation_manifest = Path(
            confirmation["result"]["dataset_manifest"]
        )
        development_inspection_path = (
            discovery.PROJECT_ROOT
            / str(winner["development_inspection_path"])
        )
        checked: list[Path] = []
        monkeypatch.setattr(
            discovery,
            "require_committed",
            lambda path: checked.append(Path(path).resolve()),
        )

        _path, result = discovery.inspect_confirmation(
            confirmation_path,
            root=artifact_root,
        )

        assert result["state"] == "CONFIRMATION_PASSED"
        assert checked == [
            confirmation_path.resolve(),
            winner_path.resolve(),
            confirmation_manifest.resolve(),
            development_inspection_path.resolve(),
        ]


def test_overfit_family_is_rejected_before_winner_or_confirmation():
    with tempfile.TemporaryDirectory(dir=discovery.PROJECT_ROOT) as directory:
        work = Path(directory)
        artifact_root = work / "artifacts"
        contract_path = _write_contract(
            work,
            family_contract(
                _dataset(work), family_id="overfit-family", mode="overfit"
            ),
        )
        discovery.run_preflight(
            contract_path, root=artifact_root, enforce_commit=False
        )
        search_path, _ = discovery.freeze_search(
            contract_path, root=artifact_root, enforce_commit=False
        )
        development_path, _ = discovery.evaluate_development(
            search_path, root=artifact_root, enforce_commit=False
        )
        inspection_path, inspection = discovery.inspect_development(
            development_path, root=artifact_root, enforce_commit=False
        )
        assert inspection["state"] == "REJECTED"
        with pytest.raises(discovery.StrategyDiscoveryError, match="no winner"):
            discovery.freeze_winner(
                inspection_path, root=artifact_root, enforce_commit=False
            )


def test_capacity_policy_retires_or_defers_without_outcomes():
    with tempfile.TemporaryDirectory(dir=discovery.PROJECT_ROOT) as directory:
        work = Path(directory)
        for capacity, expected in (
            (49, "RETIRED_INSUFFICIENT_FORMAL_CAPACITY"),
            (75, "PRESERVED_LATER_SINGLE_RULE"),
        ):
            family_id = f"capacity-{capacity}"
            path = _write_contract(
                work,
                family_contract(
                    _dataset(work / family_id),
                    family_id=family_id,
                    capacity=capacity,
                ),
            )
            _, artifact = discovery.run_preflight(
                path, root=work / "artifacts", enforce_commit=False
            )
            assert artifact["state"] == expected
            assert artifact["outcomes_accessed"] is False


def test_uncommitted_transition_fails_closed():
    with tempfile.TemporaryDirectory(dir=discovery.PROJECT_ROOT) as directory:
        work = Path(directory)
        contract_path = _write_contract(
            work, family_contract(_dataset(work), family_id="commit-boundary")
        )
        discovery.run_preflight(
            contract_path, root=work / "artifacts", enforce_commit=False
        )
        with pytest.raises(discovery.StrategyDiscoveryError, match="committed"):
            discovery.freeze_search(contract_path, root=work / "artifacts")


def test_search_freeze_rejects_implementation_drift_after_preflight(monkeypatch):
    with tempfile.TemporaryDirectory(dir=discovery.PROJECT_ROOT) as directory:
        work = Path(directory)
        artifact_root = work / "artifacts"
        contract_path = _write_contract(
            work,
            family_contract(_dataset(work), family_id="preflight-code-drift"),
        )
        discovery.run_preflight(
            contract_path,
            root=artifact_root,
            enforce_commit=False,
        )
        implementation = (
            discovery.PROJECT_ROOT / "tests/synthetic_discovery_plugin.py"
        ).resolve()
        original = discovery._file_hash

        def drifted(path):
            if path.resolve() == implementation:
                return "0" * 64
            return original(path)

        monkeypatch.setattr(discovery, "_file_hash", drifted)
        with pytest.raises(
            discovery.StrategyDiscoveryError,
            match="implementation drifted after preflight",
        ):
            discovery.freeze_search(
                contract_path,
                root=artifact_root,
                enforce_commit=False,
            )


def test_search_freeze_selects_preflight_bound_to_superseding_contract():
    with tempfile.TemporaryDirectory(dir=discovery.PROJECT_ROOT) as directory:
        work = Path(directory)
        artifact_root = work / "artifacts"
        family_id = "superseding-contract"
        first = family_contract(_dataset(work), family_id=family_id)
        first_path = work / "first.json"
        first_path.write_text(
            json.dumps(first, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        _, first_preflight = discovery.run_preflight(
            first_path,
            root=artifact_root,
            enforce_commit=False,
        )
        second = {**first, "created_at": "2026-07-22T20:00:00-04:00"}
        second_path = work / "second.json"
        second_path.write_text(
            json.dumps(second, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        _, second_preflight = discovery.run_preflight(
            second_path,
            root=artifact_root,
            enforce_commit=False,
        )

        _, search = discovery.freeze_search(
            second_path,
            root=artifact_root,
            enforce_commit=False,
        )

        assert search["preflight_sha256"] == second_preflight["artifact_sha256"]
        assert search["preflight_sha256"] != first_preflight["artifact_sha256"]


def test_evaluations_reject_code_drift_from_frozen_implementation(monkeypatch):
    with tempfile.TemporaryDirectory(dir=discovery.PROJECT_ROOT) as directory:
        work = Path(directory)
        artifact_root = work / "artifacts"
        contract_path = _write_contract(
            work,
            family_contract(_dataset(work), family_id="evaluation-code-drift"),
        )
        discovery.run_preflight(
            contract_path,
            root=artifact_root,
            enforce_commit=False,
        )
        search_path, _ = discovery.freeze_search(
            contract_path,
            root=artifact_root,
            enforce_commit=False,
        )
        implementation = (
            discovery.PROJECT_ROOT / "tests/synthetic_discovery_plugin.py"
        ).resolve()
        original = discovery._file_hash

        def drifted(path):
            if path.resolve() == implementation:
                return "0" * 64
            return original(path)

        monkeypatch.setattr(discovery, "_file_hash", drifted)
        with pytest.raises(
            discovery.StrategyDiscoveryError,
            match="frozen implementation drifted",
        ):
            discovery.evaluate_development(
                search_path,
                root=artifact_root,
                enforce_commit=False,
            )

        winner = {
            "artifact_sha256": "a" * 64,
            "state": "WINNER_FROZEN",
            "implementation_hashes": {
                "tests/synthetic_discovery_plugin.py": original(implementation),
            },
        }
        winner_path = work / "winner.json"
        winner_path.write_text(
            json.dumps(winner, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        monkeypatch.setattr(
            discovery,
            "load_artifact",
            lambda *_args, **_kwargs: winner,
        )
        with pytest.raises(
            discovery.StrategyDiscoveryError,
            match="frozen implementation drifted",
        ):
            discovery.evaluate_confirmation(
                winner_path,
                root=artifact_root,
                enforce_commit=False,
            )


def test_insufficient_confirmation_inventory_freezes_without_outcome_access():
    with tempfile.TemporaryDirectory(dir=discovery.PROJECT_ROOT) as directory:
        work = Path(directory)
        _, _, inspection, winner = _freeze_synthetic_winner(
            work, confirmation_count=10
        )
        assert inspection["state"] == "INSUFFICIENT_POWER_CAPACITY"
        assert inspection["confirmation_access_permitted"] is False
        assert winner is None


def test_total_signal_power_capacity_is_frozen_before_confirmation(
    monkeypatch,
):
    original = synthetic_plugin.evaluate_development

    def sparse_development(contract, trials):
        result = original(contract, trials)
        for trial in result["trials"]:
            metrics = trial["metrics"]
            metrics["oof_filled_account_returns"] = metrics[
                "oof_filled_account_returns"
            ][:10]
            metrics["oof_net_pnl_dollars"] = metrics["oof_net_pnl_dollars"][:10]
            for row in trial["maturity_rows"][10:]:
                row["session_outcome"] = "position_open"
                row["eligible_signal"] = False
        return result

    monkeypatch.setattr(
        synthetic_plugin, "evaluate_development", sparse_development
    )
    with tempfile.TemporaryDirectory(dir=discovery.PROJECT_ROOT) as directory:
        work = Path(directory)
        _root, _inspection_path, inspection, winner = _freeze_synthetic_winner(
            work, confirmation_count=20
        )

        assert inspection["state"] == "INSUFFICIENT_POWER_CAPACITY"
        assert inspection["selection"]["development_filled_signals"] == 10
        assert inspection["selection"]["required_total_signals"] == 50
        assert inspection["confirmation_inventory"] == 20
        assert inspection["maximum_total_signal_capacity"] == 30
        assert inspection["confirmation_access_permitted"] is False
        assert winner is None


def test_confirmation_rejects_date_substitution_rules_drift_and_early_peeking(
    monkeypatch,
):
    with tempfile.TemporaryDirectory(dir=discovery.PROJECT_ROOT) as directory:
        work = Path(directory)
        artifact_root, winner_path, _, winner = _freeze_synthetic_winner(work)
        assert winner is not None
        original = synthetic_plugin.evaluate_confirmation

        def modified(field: str):
            def evaluate(value):
                result = original(value)
                if field == "dates":
                    result["observed_dates"] = result["observed_dates"][1:]
                elif field == "rules":
                    result["rules_hash"] = "f" * 64
                elif field == "dataset":
                    result["dataset_manifest"] = value[
                        "development_dataset_manifest"
                    ]
                else:
                    result["outcome_access_before_winner_freeze"] = True
                return result

            return evaluate

        for field, message in (
            ("dates", "dates were substituted"),
            ("rules", "rules hash drifted"),
            ("dataset", "not exact, untouched, and winner-bound"),
            ("peeking", "outcome-access attestation"),
        ):
            monkeypatch.setattr(
                synthetic_plugin, "evaluate_confirmation", modified(field)
            )
            with pytest.raises(discovery.StrategyDiscoveryError, match=message):
                discovery.evaluate_confirmation(
                    winner_path, root=artifact_root, enforce_commit=False
                )


@pytest.mark.parametrize(
    ("drift", "message"),
    (
        ("daily-calendar", "primary_5bps confirmation accounting is incomplete"),
        (
            "stress-filled",
            "stress_20bps confirmation accounting differs from maturity rows",
        ),
        (
            "maturity-signals",
            "primary_5bps confirmation accounting differs from maturity rows",
        ),
    ),
)
def test_confirmation_inspection_reconciles_calendar_and_filled_accounting(
    monkeypatch, drift, message
):
    with tempfile.TemporaryDirectory(dir=discovery.PROJECT_ROOT) as directory:
        work = Path(directory)
        artifact_root, winner_path, _, winner = _freeze_synthetic_winner(work)
        assert winner is not None
        original = synthetic_plugin.evaluate_confirmation

        def drifted(value):
            result = original(value)
            if drift == "daily-calendar":
                result["scenarios"]["primary_5bps"][
                    "daily_account_returns"
                ].pop()
            elif drift == "stress-filled":
                scenario = result["scenarios"]["stress_20bps"]
                scenario["filled_account_returns"] = list(
                    scenario["filled_account_returns"]
                )[:-1]
                scenario["net_pnl_dollars"] = list(
                    scenario["net_pnl_dollars"]
                )[:-1]
            else:
                result["maturity_rows"][-1]["session_outcome"] = "position_open"
            return result

        monkeypatch.setattr(synthetic_plugin, "evaluate_confirmation", drifted)
        confirmation_path, _ = discovery.evaluate_confirmation(
            winner_path, root=artifact_root, enforce_commit=False
        )
        with pytest.raises(discovery.StrategyDiscoveryError, match=message):
            discovery.inspect_confirmation(
                confirmation_path, root=artifact_root, enforce_commit=False
            )


def test_development_rejects_missing_trial_accounting(monkeypatch):
    with tempfile.TemporaryDirectory(dir=discovery.PROJECT_ROOT) as directory:
        work = Path(directory)
        artifact_root = work / "artifacts"
        contract_path = _write_contract(
            work, family_contract(_dataset(work), family_id="missing-accounting")
        )
        discovery.run_preflight(
            contract_path, root=artifact_root, enforce_commit=False
        )
        search_path, _ = discovery.freeze_search(
            contract_path, root=artifact_root, enforce_commit=False
        )
        original = synthetic_plugin.evaluate_development

        def incomplete(contract, trials):
            result = original(contract, trials)
            result["trials"][0]["trial_accounting"] = []
            return result

        monkeypatch.setattr(synthetic_plugin, "evaluate_development", incomplete)
        with pytest.raises(discovery.StrategyDiscoveryError, match="trial_accounting"):
            discovery.evaluate_development(
                search_path, root=artifact_root, enforce_commit=False
            )


def test_development_rejects_training_dates_in_oof_evidence(monkeypatch):
    with tempfile.TemporaryDirectory(dir=discovery.PROJECT_ROOT) as directory:
        work = Path(directory)
        artifact_root = work / "artifacts"
        contract_path = _write_contract(
            work, family_contract(_dataset(work), family_id="training-date-leak")
        )
        discovery.run_preflight(
            contract_path, root=artifact_root, enforce_commit=False
        )
        search_path, _ = discovery.freeze_search(
            contract_path, root=artifact_root, enforce_commit=False
        )
        original = synthetic_plugin.evaluate_development

        def leaked(contract, trials):
            result = original(contract, trials)
            for trial in result["trials"]:
                missing = len(contract["development_dates"]) - len(
                    trial["metrics"]["oof_daily_account_returns"]
                )
                for field in (
                    "oof_daily_account_returns",
                    "oof_filled_account_returns",
                ):
                    trial["metrics"][field].extend([0.001] * missing)
                trial["metrics"]["oof_net_pnl_dollars"].extend(
                    [100.0] * missing
                )
                trial["trial_accounting"].extend(
                    {
                        "date": day,
                        "outcome": "account_return_day",
                    }
                    for day in contract["development_dates"][-missing:]
                )
                template = trial["maturity_rows"][0]
                trial["maturity_rows"] = [
                    {**template, "date": day}
                    for day in contract["development_dates"]
                ]
            return result

        monkeypatch.setattr(synthetic_plugin, "evaluate_development", leaked)
        with pytest.raises(
            discovery.StrategyDiscoveryError,
            match="trial accounting|frozen OOF test date",
        ):
            discovery.evaluate_development(
                search_path, root=artifact_root, enforce_commit=False
            )


def test_development_rejects_filled_count_maturity_drift(monkeypatch):
    with tempfile.TemporaryDirectory(dir=discovery.PROJECT_ROOT) as directory:
        work = Path(directory)
        artifact_root = work / "artifacts"
        contract_path = _write_contract(
            work, family_contract(_dataset(work), family_id="filled-count-drift")
        )
        discovery.run_preflight(
            contract_path, root=artifact_root, enforce_commit=False
        )
        search_path, _ = discovery.freeze_search(
            contract_path, root=artifact_root, enforce_commit=False
        )
        original = synthetic_plugin.evaluate_development

        def drifted(contract, trials):
            result = original(contract, trials)
            metrics = result["trials"][0]["metrics"]
            metrics["oof_filled_account_returns"] = metrics[
                "oof_filled_account_returns"
            ][:-1]
            metrics["oof_net_pnl_dollars"] = metrics["oof_net_pnl_dollars"][:-1]
            return result

        monkeypatch.setattr(
            synthetic_plugin, "evaluate_development", drifted
        )
        with pytest.raises(
            discovery.StrategyDiscoveryError,
            match="account evidence differs",
        ):
            discovery.evaluate_development(
                search_path, root=artifact_root, enforce_commit=False
            )


def test_development_inspection_retires_winner_with_weak_primary_account_path(
    monkeypatch,
):
    with tempfile.TemporaryDirectory(dir=discovery.PROJECT_ROOT) as directory:
        work = Path(directory)
        artifact_root = work / "artifacts"
        contract_path = _write_contract(
            work, family_contract(_dataset(work), family_id="weak-primary-path")
        )
        discovery.run_preflight(
            contract_path, root=artifact_root, enforce_commit=False
        )
        search_path, _ = discovery.freeze_search(
            contract_path, root=artifact_root, enforce_commit=False
        )
        original = synthetic_plugin.evaluate_development

        def weak_primary(contract, trials):
            result = original(contract, trials)
            for trial in result["trials"]:
                for row in trial["maturity_rows"]:
                    row["primary_account_return_fraction"] = -0.001
                    row["net_r"] = -0.2
                    row["net_pnl_dollars"] = -100.0
            return result

        monkeypatch.setattr(
            synthetic_plugin, "evaluate_development", weak_primary
        )
        development_path, _ = discovery.evaluate_development(
            search_path, root=artifact_root, enforce_commit=False
        )
        inspection_path, inspection = discovery.inspect_development(
            development_path, root=artifact_root, enforce_commit=False
        )
        assert inspection["state"] == "RETIRED_DEVELOPMENT_ACCOUNT_GATES"
        assert inspection["confirmation_access_permitted"] is False
        assert inspection["development_account_inspection"]["passed"] is False
        with pytest.raises(discovery.StrategyDiscoveryError, match="no winner"):
            discovery.freeze_winner(
                inspection_path, root=artifact_root, enforce_commit=False
            )


def test_development_rejects_dataset_substitution(monkeypatch):
    with tempfile.TemporaryDirectory(dir=discovery.PROJECT_ROOT) as directory:
        work = Path(directory)
        artifact_root = work / "artifacts"
        contract_path = _write_contract(
            work, family_contract(_dataset(work), family_id="dataset-substitution")
        )
        discovery.run_preflight(
            contract_path, root=artifact_root, enforce_commit=False
        )
        search_path, _ = discovery.freeze_search(
            contract_path, root=artifact_root, enforce_commit=False
        )
        substituted = _dataset(work / "substituted")
        original = synthetic_plugin.evaluate_development

        def replace_manifest(contract, trials):
            result = original(contract, trials)
            result["dataset_manifest"] = str(substituted)
            return result

        monkeypatch.setattr(
            synthetic_plugin, "evaluate_development", replace_manifest
        )
        with pytest.raises(discovery.StrategyDiscoveryError, match="substituted"):
            discovery.evaluate_development(
                search_path, root=artifact_root, enforce_commit=False
            )


def test_maturity_rows_count_closed_signals_separately_from_entry_days():
    dates = ["2025-01-02", "2025-01-03", "2025-01-06"]
    winner = {
        "recorded_at": "2026-07-22T19:00:00-04:00",
        "strategy_id": "strategy-overlap",
        "strategy_version": "strategy-overlap-v1",
        "family_id": "overlap-family",
        "rules_hash": "a" * 64,
    }
    rows = [
        {
            "date": day,
            "session_outcome": outcome,
            "primary_account_return_fraction": daily_return,
            "stress_10bps_account_return_fraction": daily_return - 0.0001,
            "stress_20bps_account_return_fraction": daily_return - 0.0002,
            "signals": signals,
        }
        for day, outcome, daily_return, signals in (
            ("2025-01-02", "filled", -0.0005, []),
            ("2025-01-03", "position_open", 0.0010, []),
            (
                "2025-01-06",
                "exit",
                0.0020,
                [
                    {
                        "date": "2025-01-02",
                        "signal_id": "overlap-signal-1",
                        "primary_account_return_fraction": 0.0025,
                        "stress_10bps_account_return_fraction": 0.0023,
                        "stress_20bps_account_return_fraction": 0.0019,
                        "net_r": 0.50,
                        "stress_10bps_r": 0.46,
                        "stress_20bps_r": 0.38,
                        "net_pnl_dollars": 250.0,
                        "stress_10bps_net_pnl_dollars": 230.0,
                        "stress_20bps_net_pnl_dollars": 190.0,
                        "stop_executed": False,
                    }
                ],
            ),
        )
    ]

    records = discovery._phase_maturity_records(
        rows,
        phase="development",
        expected_dates=dates,
        winner=winner,
    )

    sessions = [item for item in records if item["record_type"] == "session"]
    signals = [item for item in records if item["record_type"] == "signal"]
    assert len(sessions) == 3
    assert [item["eligible_signal"] for item in sessions] == [True, False, False]
    assert len(signals) == 1
    assert signals[0]["date"] == "2025-01-02"
    assert signals[0]["net_account_return_fraction"] == pytest.approx(0.0025)


def test_declared_family_sizes_are_complete_and_bounded():
    equity = enumerate_trials(
        {
            "return_window": [1, 3],
            "z_threshold": [-1.5, -2.0, -2.5],
            "trend_gate": ["sma100", "sma200"],
            "stop_atr": [1.0, 1.5],
            "hold": [2, 5],
        }
    )
    intraday = enumerate_trials(
        {
            "opening_window": [15, 30],
            "z_threshold": [-1.5, -2.0],
            "vwap_bars": [1, 2],
            "stop_atr": [1.0, 1.5],
            "target_r": [1.0, 1.5],
        }
    )
    pullback = enumerate_trials(
        {
            "trend_sma": [100, 200],
            "rsi2_max": [5, 10],
            "decline": [0.02, 0.03],
            "stop_atr": [1.0, 1.5],
            "hold": [3, 5],
        }
    )
    assert len(equity) == 48
    assert len(intraday) == 32
    assert len(pullback) == 32


def test_warm_48_trial_family_uses_one_dataset_load_and_no_provider_requests():
    with tempfile.TemporaryDirectory(dir=discovery.PROJECT_ROOT) as directory:
        work = Path(directory)
        artifact_root = work / "artifacts"
        contract = family_contract(_dataset(work), family_id="performance-family")
        contract["parameter_grid"] = {
            "return_window": [1, 3],
            "z_threshold": [-1.5, -2.0, -2.5],
            "trend_gate": ["sma100", "sma200"],
            "stop_atr": [1.0, 1.5],
            "hold": [2, 5],
        }
        contract_path = _write_contract(work, contract)
        discovery.run_preflight(
            contract_path, root=artifact_root, enforce_commit=False
        )
        search_path, search = discovery.freeze_search(
            contract_path, root=artifact_root, enforce_commit=False
        )
        started = time.monotonic()
        _, result = discovery.evaluate_development(
            search_path, root=artifact_root, enforce_commit=False
        )
        assert time.monotonic() - started <= 60
        assert search["trial_count"] == 48
        assert result["provider_telemetry"]["requests"] == 0
        assert result["provider_telemetry"]["dataset_loads"] == 1

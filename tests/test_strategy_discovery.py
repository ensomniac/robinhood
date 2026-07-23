from __future__ import annotations

import json
import tempfile
import time
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import pytest

import portfolio_guard
import portfolio_maturity
import strategy_discovery as discovery
import tests.synthetic_discovery_plugin as synthetic_plugin
from learning_data import freeze_dataset_contract
from learning_experiment import DEVELOPMENT_SEARCH_RULE, enumerate_trials


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
        inspection_path, root=artifact_root, enforce_commit=False
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
        search_path, search = discovery.freeze_search(
            contract_path, root=artifact_root, enforce_commit=False
        )
        assert search["trial_count"] == 4
        development_path, development = discovery.evaluate_development(
            search_path, root=artifact_root, enforce_commit=False
        )
        assert development["provider_telemetry"]["requests"] == 0
        inspection_path, inspection = discovery.inspect_development(
            development_path, root=artifact_root, enforce_commit=False
        )
        assert inspection["state"] == "WINNER_SELECTED"
        assert inspection["selection"]["required_total_signals"] >= 50
        winner_path, winner = discovery.freeze_winner(
            inspection_path, root=artifact_root, enforce_commit=False
        )
        assert winner["confirmation_parameter_alternatives"] == 0
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
        queue_path, queue = discovery.queue_shadow(
            winner_path, root=artifact_root, enforce_commit=False
        )
        assert queue_path.is_file()
        assert queue["state"] == "SHADOW_QUEUED"
        assert queue["required_clean_closed_shadows"] == 5
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
        historical = discovery.load_artifact(
            discovery.PROJECT_ROOT / queue["historical_maturity_ledger_path"],
            expected_kind="historical-maturity-ledger",
        )
        records = list(historical["records"])
        for index in range(5):
            day = date(2025, 8, 1) + timedelta(days=index)
            records.append(
                {
                    "schema_version": 2,
                    "research_campaign_id": discovery.CAMPAIGN_ID,
                    "record_type": "signal",
                    "recorded_at": "2026-07-22T23:00:00+00:00",
                    "strategy_id": winner["strategy_id"],
                    "strategy_version": winner["strategy_version"],
                    "mechanism_family": winner["family_id"],
                    "rules_hash": winner["rules_hash"],
                    "date": day.isoformat(),
                    "sample_phase": "shadow",
                    "mode": "shadow",
                    "signal_id": f"{day.isoformat()}-{winner['strategy_id']}-shadow",
                    "closed": True,
                    "eligible": True,
                    "net_r": 0.25,
                    "stress_10bps_r": 0.20,
                    "stress_20bps_r": 0.15,
                    "stop_executed": False,
                    "discovery_complete": True,
                    "evaluation_complete": True,
                    "sizing_complete": True,
                    "order_construction_complete": True,
                    "protection_plan_complete": True,
                    "monitoring_complete": True,
                    "journal_complete": True,
                    "broker_actions": 0,
                    "session_capture_complete": True,
                    "rule_violations": [],
                }
            )
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


def test_insufficient_confirmation_inventory_freezes_without_outcome_access():
    with tempfile.TemporaryDirectory(dir=discovery.PROJECT_ROOT) as directory:
        work = Path(directory)
        _, _, inspection, winner = _freeze_synthetic_winner(
            work, confirmation_count=10
        )
        assert inspection["state"] == "INSUFFICIENT_POWER_CAPACITY"
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

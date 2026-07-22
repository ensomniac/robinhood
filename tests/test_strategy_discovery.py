from __future__ import annotations

import json
import tempfile
import time
from datetime import date, timedelta
from pathlib import Path

import pytest

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
        "development_dates": _dates(date(2025, 1, 2), 60),
        "embargo_dates": _dates(date(2025, 4, 1), 5),
        "confirmation_dates": _dates(date(2025, 4, 6), confirmation_count),
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
            "requested_dates": _dates(date(2025, 1, 2), 60),
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
        contract_path = _write_contract(
            work, family_contract(_dataset(work), family_id="genuine-edge")
        )
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
                else:
                    result["outcome_access_before_winner_freeze"] = True
                return result

            return evaluate

        for field, message in (
            ("dates", "dates were substituted"),
            ("rules", "rules hash drifted"),
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

import tempfile
import unittest
import math
from datetime import date, timedelta
from pathlib import Path

from learning_data import freeze_dataset_contract
from learning_experiment import (
    LearningExperimentError,
    audit_experiment_program,
    build_rolling_origin_plan,
    disposition_from_result,
    enforce_research_lock,
    enforce_weekly_hypothesis_budget,
    enumerate_trials,
    freeze_hypothesis_contract,
    load_hypothesis_contract,
    research_lock_status,
    validate_complete_evaluation,
    validate_hypothesis_contract,
    validate_transition,
    _rebuild_development_statistics,
)
from learning_statistics import stationary_bootstrap_summary
from learning_registry import append_event


def hypothesis_contract(lane="catalyst_falsification"):
    return {
        "schema_version": 1,
        "experiment_id": "experiment-test-mechanism",
        "family_id": "family-test-v1",
        "strategy_id": "strategy-test-v1",
        "parent_experiment_id": None,
        "created_at": "2026-07-18T19:00:00-04:00",
        "status": "INVENTED",
        "mechanism": "Opening liquidity imbalance persists briefly after confirmation.",
        "expected_holding_behavior": "Enter after confirmation and exit intraday.",
        "dataset_lane": lane,
        "universe_requirements": {"claim_scope": "FALSIFICATION_ONLY"},
        "entry_rule": "Enter on the next completed-bar open after confirmation.",
        "stop_rule": "Exit at frozen structural invalidation without compression.",
        "exit_rule": "Exit at 2R, structural stop, or 15:50 ET.",
        "ranking_rule": "Earliest signal, then strength, then symbol.",
        "selection_rule": "At most one selected signal per date.",
        "parameter_grid": {"target_r": [1.5, 2.0], "slippage_bps": [5, 10]},
        "primary_parameters": {"target_r": 2.0, "slippage_bps": 10},
        "primary_outcome": "total_log_growth",
        "execution_assumptions": {"next_bar_fill": True, "ambiguity": "stop_first"},
        "falsification_criteria": {
            "minimum_total_log_growth": 0.0,
            "minimum_bootstrap_lower_mean": 0.0,
            "minimum_profit_factor": 1.2,
            "maximum_drawdown": 6.0,
            "minimum_deflated_sharpe_probability": 0.9,
            "maximum_pbo_probability": 0.5,
        },
        "minimum_evidence": {"configured_floor": 50, "power": 0.8},
        "contamination_risks": ["The mechanism may reuse inspected dates."],
        "production_compatibility_risks": [
            "Structural stops may constrain allocation."
        ],
        "material_difference_rationale": "This mechanism changes the causal signal, not only a numeric threshold.",
    }


def dataset_contract(lane="catalyst_falsification"):
    payload = {
        "lane": lane,
        "claim_scope": "FALSIFICATION_ONLY",
        "evidence_paths": ["historical_batches/test.json"],
        "inspected": True,
        "point_in_time_evidence": True,
    }
    if lane == "confirmation":
        payload = {
            "lane": "confirmation",
            "claim_scope": "EXACT_PREREGISTERED_CONTRACT_ONLY",
            "evidence_paths": ["historical_batches/test.json"],
            "inspected": False,
            "preregistration_sha256": "a" * 64,
            "preregistered_at": "2026-07-18T19:00:00-04:00",
            "capture_after_preregistration_attested": True,
        }
    return {
        "schema_version": 1,
        "dataset_id": "dataset-test",
        "registered_at": "2026-07-18T19:00:00-04:00",
        "requested_dates": ["2026-07-14", "2026-07-15"],
        "dataset_payload": payload,
    }


def complete_result(contract, dataset_path, *, passing=True):
    normalized = validate_hypothesis_contract(contract)
    metrics = {
        "total_log_growth": 0.1 if passing else -0.1,
        "bootstrap_lower_mean": 0.01 if passing else -0.01,
        "profit_factor": 1.5 if passing else 0.8,
        "maximum_drawdown": 2.0 if passing else 8.0,
        "deflated_sharpe_probability": 0.95 if passing else 0.2,
        "pbo_probability": 0.2 if passing else 0.8,
        "holm_reject_null": passing,
        "rolling_folds_positive": passing,
    }
    return {
        "experiment_id": contract["experiment_id"],
        "dataset_manifest": str(dataset_path),
        "implementation_sha256": "b" * 64,
        "trials": [
            {"trial_id": item["trial_id"], "metrics": dict(metrics)}
            for item in normalized["trial_family"]
        ],
    }


class HypothesisContractTests(unittest.TestCase):
    def test_grid_is_complete_deterministic_and_primary_is_locked(self):
        first = enumerate_trials({"b": [1, 2], "a": [True, False]})
        second = enumerate_trials({"a": [True, False], "b": [1, 2]})
        contract = validate_hypothesis_contract(hypothesis_contract())

        self.assertEqual(first, second)
        self.assertEqual(len(contract["trial_family"]), 4)
        self.assertTrue(contract["primary_trial_id"].startswith("trial-"))

    def test_primary_must_be_in_grid_and_grid_is_bounded(self):
        contract = hypothesis_contract()
        contract["primary_parameters"]["target_r"] = 3.0
        with self.assertRaisesRegex(LearningExperimentError, "exactly one"):
            validate_hypothesis_contract(contract)
        with self.assertRaisesRegex(LearningExperimentError, "maximum"):
            enumerate_trials({"one": list(range(17)), "two": list(range(17))})

    def test_hash_addressed_hypothesis_rejects_mutation(self):
        with tempfile.TemporaryDirectory() as directory:
            path, frozen = freeze_hypothesis_contract(
                hypothesis_contract(),
                Path(directory),
                research_lock_path=Path(directory) / "missing-lock.json",
            )
            self.assertEqual(load_hypothesis_contract(path), frozen)
            path.write_text(
                path.read_text(encoding="utf-8").replace("briefly", "forever"),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(LearningExperimentError, "mutated"):
                load_hypothesis_contract(path)

    def test_weekly_budget_caps_new_mechanisms(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for index in range(3):
                append_event(
                    "experiments",
                    {
                        "schema_version": 1,
                        "event_id": f"experiment-week-{index}-registered",
                        "entity_id": f"experiment-week-{index}",
                        "event_type": "registered",
                        "recorded_at": "2026-07-18T19:00:00-04:00",
                        "payload": {
                            "family_id": f"family-week-{index}",
                            "status": "INVENTED",
                            "hypothesis": "test",
                            "result_paths": [],
                            "disposition": "pending",
                            "created_at": "2026-07-18T19:00:00-04:00",
                        },
                    },
                    root,
                )
            with self.assertRaisesRegex(LearningExperimentError, "budget"):
                enforce_weekly_hypothesis_budget(
                    hypothesis_contract(), registry_root=root
                )

    def test_research_lock_blocks_the_inspected_catalyst_lane(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            lock_path = root / "RESEARCH_LOCK.json"
            lock_path.write_text(
                '{"schema_version":1,"active":true,'
                '"blocked_dataset_lane":"catalyst_falsification",'
                '"required_dataset_id":"dataset-scanner-replay",'
                '"required_dataset_inspected":true,'
                '"required_dataset_lane":"production_scanner_replay",'
                '"required_dataset_status":"READY",'
                '"evidence_path":"collection-status.json",'
                '"reason":"No additional strategy variant until fidelity is READY."}\n',
                encoding="utf-8",
            )

            with self.assertRaisesRegex(
                LearningExperimentError, "No additional strategy variant"
            ):
                enforce_research_lock(
                    hypothesis_contract(),
                    lock_path=lock_path,
                    registry_root=root,
                )

            enforce_research_lock(
                hypothesis_contract("development"),
                lock_path=lock_path,
                registry_root=root,
            )

            append_event(
                "datasets",
                {
                    "schema_version": 1,
                    "event_id": "dataset-scanner-replay-ready",
                    "entity_id": "dataset-scanner-replay",
                    "event_type": "registered",
                    "recorded_at": "2026-07-18T20:00:00-04:00",
                    "payload": {
                        "lane": "production_scanner_replay",
                        "status": "READY",
                        "evidence_paths": ["collection-status.json"],
                        "inspected": True,
                    },
                },
                root,
            )
            self.assertFalse(
                research_lock_status(lock_path=lock_path, registry_root=root)[
                    "blocks_new_hypotheses"
                ]
            )
            enforce_research_lock(
                hypothesis_contract(),
                lock_path=lock_path,
                registry_root=root,
            )

    def test_failed_confirmation_cannot_return_to_tuning(self):
        with self.assertRaisesRegex(LearningExperimentError, "not allowed"):
            validate_transition("FAILED", "PREREGISTERED")


class EvaluationContractTests(unittest.TestCase):
    def test_development_rebuild_separates_daily_path_from_filled_confidence(self):
        daily_left = [0.0, 0.002, 0.0, -0.001] * 4
        filled_left = [0.002, -0.001, 0.002, -0.001]
        daily_right = [0.0, 0.0015, 0.0, -0.001] * 4
        filled_right = [0.0015, -0.001, 0.0015, -0.001]

        def trial(trial_id, daily, filled):
            return {
                "trial_id": trial_id,
                "metrics": {
                    "oof_daily_account_returns": daily,
                    "oof_filled_account_returns": filled,
                    "oof_net_pnl_dollars": [value * 100_000 for value in filled],
                    "risk_fraction": 0.005,
                    "rules_complete": True,
                    "trial_accounting_complete": True,
                },
            }

        rebuilt = _rebuild_development_statistics(
            [
                trial("left", daily_left, filled_left),
                trial("right", daily_right, filled_right),
            ]
        )["left"]
        expected_bootstrap = stationary_bootstrap_summary(
            filled_left, confidence=0.90, samples=2_000
        )
        self.assertAlmostEqual(
            rebuilt["stress_20bps_total_log_growth"],
            sum(math.log1p(value) for value in daily_left),
        )
        self.assertEqual(rebuilt["stationary_bootstrap"], expected_bootstrap)
        self.assertEqual(rebuilt["oof_daily_account_returns"], daily_left)
        self.assertEqual(rebuilt["oof_filled_account_returns"], filled_left)

    def test_rolling_origin_plan_has_expanding_train_and_embargo(self):
        start = date(2026, 1, 1)
        dates = [(start + timedelta(days=index)).isoformat() for index in range(80)]
        folds = build_rolling_origin_plan(dates)

        self.assertGreaterEqual(len(folds), 2)
        self.assertEqual(len(folds[0]["embargo_dates"]), 1)
        self.assertGreater(len(folds[1]["train_dates"]), len(folds[0]["train_dates"]))
        for fold in folds:
            self.assertEqual(
                fold["test_dates"],
                [*fold["entry_dates"], *fold["settlement_only_dates"]],
            )
            self.assertEqual(len(fold["settlement_only_dates"]), 4)

    def test_rebuilt_fold_stability_uses_the_exact_frozen_rolling_plan(self):
        start = date(2026, 1, 1)
        dates = [(start + timedelta(days=index)).isoformat() for index in range(80)]
        plan = build_rolling_origin_plan(dates)
        account_dates = [day for fold in plan for day in fold["test_dates"]]
        daily = [0.01] * len(account_dates)
        positions = {day: index for index, day in enumerate(account_dates)}
        for day in plan[0]["test_dates"]:
            daily[positions[day]] = -0.02

        def trial(trial_id, scale):
            returns = [value * scale for value in daily]
            return {
                "trial_id": trial_id,
                "metrics": {
                    "oof_daily_account_returns": returns,
                    "oof_filled_account_returns": returns,
                    "oof_net_pnl_dollars": [value * 100_000 for value in returns],
                    "risk_fraction": 0.005,
                    "rules_complete": True,
                    "trial_accounting_complete": True,
                },
            }

        rebuilt = _rebuild_development_statistics(
            [trial("left", 1.0), trial("right", 0.9)],
            development_dates=dates,
            rolling_origin_plan=plan,
        )
        self.assertFalse(rebuilt["left"]["rolling_folds_positive"])
        self.assertEqual(
            len(rebuilt["left"]["rolling_origin_fold_log_growth"]), len(plan)
        )

        drifted = [dict(fold) for fold in plan]
        drifted[0] = {**drifted[0], "test_dates": drifted[0]["test_dates"][:-1]}
        with self.assertRaisesRegex(LearningExperimentError, "plan drifted"):
            _rebuild_development_statistics(
                [trial("left", 1.0), trial("right", 0.9)],
                development_dates=dates,
                rolling_origin_plan=drifted,
            )

    def test_complete_family_is_required_and_routes_development_to_confirmation(self):
        contract = hypothesis_contract()
        with tempfile.TemporaryDirectory() as directory:
            dataset_path, _ = freeze_dataset_contract(
                dataset_contract(), Path(directory) / "datasets"
            )
            result = complete_result(contract, dataset_path)
            disposition = disposition_from_result(contract, result)
            result["trials"].pop()
            with self.assertRaisesRegex(LearningExperimentError, "complete family"):
                validate_complete_evaluation(contract, result)

        self.assertEqual(disposition["status"], "CONFIRMATION_QUEUED")
        self.assertFalse(disposition["automatic_strategy_application"])

    def test_failed_primary_is_rejected_and_confirmation_pass_routes_shadow(self):
        development = hypothesis_contract()
        confirmation = hypothesis_contract("confirmation")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            development_dataset, _ = freeze_dataset_contract(
                dataset_contract(), root / "development"
            )
            confirmation_dataset, _ = freeze_dataset_contract(
                dataset_contract("confirmation"), root / "confirmation"
            )
            failed = disposition_from_result(
                development,
                complete_result(development, development_dataset, passing=False),
            )
            passed = disposition_from_result(
                confirmation,
                complete_result(confirmation, confirmation_dataset, passing=True),
            )

        self.assertEqual(failed["status"], "REJECTED")
        self.assertEqual(passed["status"], "SHADOW_QUEUED")

    def test_current_program_audits_legacy_experiments(self):
        self.assertTrue(audit_experiment_program()["valid"])


if __name__ == "__main__":
    unittest.main()

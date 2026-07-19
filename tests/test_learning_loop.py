import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from learning_loop import (
    LearningLoopError,
    audit_program,
    build_inventory,
    classify_bottleneck,
    close_run,
    load_prompt,
    review_change_plan,
    run_bounded,
    run_next,
    start_run,
)


class LearningPromptTests(unittest.TestCase):
    def test_public_prompt_is_versioned_ordered_and_bounded(self):
        contract = load_prompt()

        self.assertEqual(contract["version"], "2026-07-19-v3")
        self.assertEqual(contract["max_apply_rounds"], 1)
        self.assertEqual(len(contract["phases"]), 8)

    def test_prompt_missing_a_required_phase_fails_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "prompt.md"
            path.write_text(
                Path("LEARNING_LOOP.md")
                .read_text(encoding="utf-8")
                .replace("## Phase 4 - Adversarial Filter", "## Removed"),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(LearningLoopError, "Phase 4"):
                load_prompt(path)


class BottleneckTests(unittest.TestCase):
    def test_market_data_is_distinguished_from_secondary_symbol_lookup(self):
        batch = {
            "cold_path": {
                "end_to_end": {"elapsed_seconds": 1000},
                "preflight": {
                    "contract_detail_requests": 100,
                    "historical_bar_requests_submitted": 400,
                },
            },
            "warm_path": {"full_local_replay": {"elapsed_seconds": 2}},
        }
        evidence = {
            "preflight": {
                "performance": {
                    "ibkr_requests": {
                        "submitted_by_kind": {
                            "contract-details": 100,
                            "historical-bars": 400,
                        },
                        "request_seconds_by_kind": {
                            "contract-details": 10,
                            "historical-bars": 900,
                        },
                    }
                }
            }
        }
        research = {"runtime": {"elapsed_seconds": 1, "provider_requests": 0}}

        result = classify_bottleneck(batch, evidence, research)

        self.assertEqual(result["primary_bottleneck"], "cold_market_data_acquisition")
        self.assertEqual(result["symbol_lookup_assessment"], "secondary_repeated_cost")
        self.assertEqual(result["provider_requests_during_local_research"], 0)

    def test_inventory_preserves_preexisting_dirty_paths_as_baseline(self):
        with patch("learning_loop._git_dirty_paths", return_value=["user-change.py"]):
            with patch("learning_loop._active_contexts", return_value=[]):
                with patch("learning_loop._progress_entries", return_value=12):
                    with patch(
                        "learning_loop._load_json",
                        side_effect=[
                            {
                                "cold_path": {
                                    "end_to_end": {"elapsed_seconds": 100},
                                    "preflight": {},
                                },
                                "warm_path": {
                                    "full_local_replay": {"elapsed_seconds": 1}
                                },
                            },
                            {"preflight": {"performance": {"ibkr_requests": {}}}},
                            {"runtime": {"elapsed_seconds": 1}},
                        ],
                    ):
                        inventory = build_inventory()

        self.assertEqual(inventory["worktree"]["dirty_paths"], ["user-change.py"])
        self.assertTrue(inventory["worktree"]["preserve_as_baseline"])
        self.assertEqual(inventory["progress_entries_before"], 12)


class ChangePlanTests(unittest.TestCase):
    def test_empty_plan_is_a_valid_no_op(self):
        result = review_change_plan({"changes": []})

        self.assertEqual(result["status"], "no_op")
        self.assertFalse(result["requires_progress_entry"])

    def test_safe_engineering_slice_requires_progress(self):
        result = review_change_plan(
            {
                "apply_rounds": 1,
                "broker_actions": False,
                "changes": [
                    {
                        "path": "ibkr_historical.py",
                        "kind": "engineering",
                        "reason": "deduplicate provider lookups",
                    }
                ],
            }
        )

        self.assertEqual(result["status"], "approved_for_bounded_apply")
        self.assertTrue(result["requires_progress_entry"])

    def test_production_strategy_edit_is_refused(self):
        with self.assertRaisesRegex(LearningLoopError, "strategy_config.toml"):
            review_change_plan(
                {
                    "changes": [
                        {
                            "path": "strategy_config.toml",
                            "kind": "engineering",
                            "reason": "change a live threshold",
                        }
                    ]
                }
            )

    def test_external_or_recursive_apply_is_refused(self):
        with self.assertRaisesRegex(LearningLoopError, "external application actions"):
            review_change_plan(
                {"external_actions": ["send_slack_message"], "changes": []}
            )
        with self.assertRaisesRegex(LearningLoopError, "at most one"):
            review_change_plan({"apply_rounds": 2, "changes": []})


class PersistentRunTests(unittest.TestCase):
    def _root(self, directory):
        root = Path(directory)
        (root / "learning").mkdir()
        (root / "LEARNING_PROGRAM.md").write_text("program\n", encoding="utf-8")
        dataset = {
            "schema_version": 1,
            "event_id": "dataset-test-registered",
            "entity_id": "dataset-test",
            "event_type": "registered",
            "recorded_at": "2026-07-18T17:00:00-04:00",
            "payload": {
                "lane": "development",
                "status": "COLLECTING",
                "evidence_paths": ["evidence.json"],
                "inspected": False,
                "claim_scope": "DEVELOPMENT_ONLY",
            },
        }
        (root / "learning" / "DATASETS.jsonl").write_text(
            json.dumps(dataset) + "\n", encoding="utf-8"
        )
        strategy = {
            "schema_version": 1,
            "event_id": "strategy-test-registered",
            "entity_id": "strategy-test",
            "event_type": "registered",
            "recorded_at": "2026-07-18T17:00:00-04:00",
            "payload": {
                "version": "strategy-test",
                "alpha_state": "DEVELOPMENT",
                "execution_state": "UNVERIFIED",
                "operations_state": "READY",
                "production_role": "CHAMPION",
            },
        }
        (root / "learning" / "STRATEGIES.jsonl").write_text(
            json.dumps(strategy) + "\n", encoding="utf-8"
        )
        experiment = {
            "schema_version": 1,
            "event_id": "experiment-test-registered",
            "entity_id": "experiment-test",
            "event_type": "registered",
            "recorded_at": "2026-07-18T17:00:00-04:00",
            "payload": {
                "family_id": "family-test",
                "status": "FAILED",
                "hypothesis": "A registered test hypothesis.",
                "result_paths": [],
                "disposition": "failed",
            },
        }
        (root / "learning" / "EXPERIMENTS.jsonl").write_text(
            json.dumps(experiment) + "\n", encoding="utf-8"
        )
        return root

    def test_failed_objective_resumes_to_terminal_close(self):
        with tempfile.TemporaryDirectory() as directory:
            root = self._root(directory)
            run_root = root / "learning_runs"
            with patch("learning_loop._git_dirty_paths", return_value=[]):
                state = start_run("experiment-test", root=root, run_root=run_root)
            first = run_next(state["run_id"], root=root, run_root=run_root)
            second = run_next(state["run_id"], root=root, run_root=run_root)
            third = run_next(state["run_id"], root=root, run_root=run_root)

            self.assertEqual(first["status"], "INVENTORIED")
            self.assertEqual(second["status"], "REJECTED")
            self.assertEqual(third["status"], "CLOSED")

    def test_collecting_dataset_is_a_resumable_objective(self):
        with tempfile.TemporaryDirectory() as directory:
            root = self._root(directory)
            run_root = root / "learning_runs"
            with patch("learning_loop._git_dirty_paths", return_value=[]):
                state = start_run("dataset-test", root=root, run_root=run_root)

            result = run_bounded(
                state["run_id"], 5, root=root, run_root=run_root
            )

            self.assertEqual(state["objective_registry"], "datasets")
            self.assertEqual(result["state"]["status"], "PREREGISTERED")
            self.assertEqual(
                result["state"]["next_action"],
                "collect the exact frozen dataset contract",
            )

    def test_inspected_ready_dataset_closes_as_completed(self):
        with tempfile.TemporaryDirectory() as directory:
            root = self._root(directory)
            run_root = root / "learning_runs"
            with patch("learning_loop._git_dirty_paths", return_value=[]):
                state = start_run("dataset-test", root=root, run_root=run_root)
            dataset_path = root / "learning" / "DATASETS.jsonl"
            event = json.loads(dataset_path.read_text(encoding="utf-8"))
            event["event_id"] = "dataset-test-ready"
            event["event_type"] = "status"
            event["payload"]["status"] = "READY"
            event["payload"]["inspected"] = True
            with dataset_path.open("a", encoding="utf-8") as stream:
                stream.write(json.dumps(event) + "\n")

            first = run_next(state["run_id"], root=root, run_root=run_root)
            second = run_next(state["run_id"], root=root, run_root=run_root)

            self.assertEqual(first["status"], "INVENTORIED")
            self.assertEqual(second["status"], "CLOSED")
            self.assertEqual(second["outcome"], "completed")

    def test_uninspected_ready_dataset_stops_for_independent_inspection(self):
        with tempfile.TemporaryDirectory() as directory:
            root = self._root(directory)
            run_root = root / "learning_runs"
            dataset_path = root / "learning" / "DATASETS.jsonl"
            event = json.loads(dataset_path.read_text(encoding="utf-8"))
            event["payload"]["status"] = "READY"
            dataset_path.write_text(json.dumps(event) + "\n", encoding="utf-8")
            with patch("learning_loop._git_dirty_paths", return_value=[]):
                state = start_run("dataset-test", root=root, run_root=run_root)

            result = run_bounded(
                state["run_id"], 5, root=root, run_root=run_root
            )

            self.assertEqual(result["state"]["status"], "DATA_READY")
            self.assertEqual(
                result["state"]["next_action"],
                "independently inspect the frozen dataset",
            )

    def test_bounded_run_stops_when_judgment_is_needed(self):
        with tempfile.TemporaryDirectory() as directory:
            root = self._root(directory)
            run_root = root / "learning_runs"
            experiment_path = root / "learning" / "EXPERIMENTS.jsonl"
            value = json.loads(experiment_path.read_text(encoding="utf-8"))
            value["payload"]["status"] = "INVENTED"
            experiment_path.write_text(json.dumps(value) + "\n", encoding="utf-8")
            with patch("learning_loop._git_dirty_paths", return_value=[]):
                state = start_run("experiment-test", root=root, run_root=run_root)

            result = run_bounded(state["run_id"], 5, root=root, run_root=run_root)

            self.assertEqual(result["state"]["status"], "HYPOTHESIS_REGISTERED")
            self.assertFalse(result["steps"][-1]["progressed"])

    def test_completed_experiment_closes_as_completed_not_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = self._root(directory)
            run_root = root / "learning_runs"
            experiment_path = root / "learning" / "EXPERIMENTS.jsonl"
            value = json.loads(experiment_path.read_text(encoding="utf-8"))
            value["payload"]["status"] = "CLOSED"
            experiment_path.write_text(json.dumps(value) + "\n", encoding="utf-8")
            with patch("learning_loop._git_dirty_paths", return_value=[]):
                state = start_run("experiment-test", root=root, run_root=run_root)

            result = run_bounded(
                state["run_id"], 5, root=root, run_root=run_root
            )

            self.assertEqual(result["state"]["status"], "CLOSED")
            self.assertEqual(result["state"]["outcome"], "completed")

    def test_production_artifact_change_fails_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            root = self._root(directory)
            (root / "strategy_config.toml").write_text(
                "version = 1\n", encoding="utf-8"
            )
            run_root = root / "learning_runs"
            with patch("learning_loop._git_dirty_paths", return_value=[]):
                state = start_run("experiment-test", root=root, run_root=run_root)
            (root / "strategy_config.toml").write_text(
                "version = 2\n", encoding="utf-8"
            )

            with self.assertRaisesRegex(LearningLoopError, "production artifacts"):
                run_next(state["run_id"], root=root, run_root=run_root)

    def test_explicit_close_and_audit(self):
        with tempfile.TemporaryDirectory() as directory:
            root = self._root(directory)
            run_root = root / "learning_runs"
            with patch("learning_loop._git_dirty_paths", return_value=[]):
                state = start_run("experiment-test", root=root, run_root=run_root)
            closed = close_run(state["run_id"], "blocked", root=root, run_root=run_root)
            audit = audit_program(root=root, run_root=run_root)

            self.assertEqual(closed["status"], "CLOSED")
            self.assertEqual(closed["outcome"], "blocked")
            self.assertTrue(audit["valid"])


if __name__ == "__main__":
    unittest.main()

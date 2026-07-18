import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from learning_loop import (
    LearningLoopError,
    build_inventory,
    classify_bottleneck,
    load_prompt,
    review_change_plan,
)


class LearningPromptTests(unittest.TestCase):
    def test_public_prompt_is_versioned_ordered_and_bounded(self):
        contract = load_prompt()

        self.assertEqual(contract["version"], "2026-07-18-v1")
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


if __name__ == "__main__":
    unittest.main()

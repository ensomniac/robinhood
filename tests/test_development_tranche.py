from __future__ import annotations

import json
import unittest
from datetime import date, timedelta
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import patch

import development_tranche as tranche


class DevelopmentTrancheSelectionTests(unittest.TestCase):
    def test_prior_acquisition_exit_requires_inspected_19_pair_capacity(self) -> None:
        value = {
            "status": "COLLECTION_INSPECTED",
            "inspected": True,
            "pairs_expected": 21,
            "pairs_terminal": 21,
            "provider_rows_after_final_decision": False,
            "target_outcomes_observed_or_derived": False,
            "terminal_counts": {
                "PREENTRY_INPUTS_COLLECTED": 19,
                "NO_CLEAN_CROSS_BEFORE_CUTOFF": 2,
            },
        }
        tranche._validate_acquisition_exit(value)
        value["target_outcomes_observed_or_derived"] = True
        with self.assertRaisesRegex(
            tranche.DevelopmentTrancheError, "exit evidence differs"
        ):
            tranche._validate_acquisition_exit(value)

    def test_selection_is_exact_disjoint_and_deterministic(self) -> None:
        start = date(2024, 12, 1)
        calendar = [(start + timedelta(days=index)).isoformat() for index in range(400)]
        exclusions = {
            "excluded_dates": calendar[40:70],
            "excluded_dates_sha256": tranche._sha256_json(calendar[40:70]),
        }
        first, required = tranche.build_selection(
            calendar_dates=calendar, exclusion_snapshot=exclusions
        )
        second, _ = tranche.build_selection(
            calendar_dates=calendar, exclusion_snapshot=exclusions
        )
        self.assertEqual(first, second)
        self.assertEqual(len(first["selected_dates"]), 100)
        self.assertFalse(set(first["selected_dates"]) & set(calendar[40:70]))
        self.assertEqual(len(required), first["required_session_count"])
        self.assertFalse(first["substitution_allowed"])

    def test_insufficient_pool_fails_without_substitution(self) -> None:
        calendar = [
            (date(2025, 1, 1) + timedelta(days=index)).isoformat()
            for index in range(120)
        ]
        exclusions = {
            "excluded_dates": calendar[20:80],
            "excluded_dates_sha256": tranche._sha256_json(calendar[20:80]),
        }
        with self.assertRaisesRegex(tranche.DevelopmentTrancheError, "needs 100"):
            tranche.build_selection(
                calendar_dates=calendar, exclusion_snapshot=exclusions
            )

    def test_calendar_accepts_attested_object_rows(self) -> None:
        with TemporaryDirectory() as directory:
            path = Path(directory) / "calendar.json"
            path.write_text(
                json.dumps(
                    [
                        {"date": "2025-01-02", "open_et": "09:30"},
                        {"date": "2025-01-03", "open_et": "09:30"},
                    ]
                ),
                encoding="utf-8",
            )
            self.assertEqual(
                tranche._calendar_dates(path), ["2025-01-02", "2025-01-03"]
            )


class DevelopmentTrancheExclusionTests(unittest.TestCase):
    def test_all_exclusion_surfaces_are_unioned_and_hashed(self) -> None:
        with TemporaryDirectory(dir=tranche.PROJECT_ROOT) as directory:
            root = Path(directory)
            signals = root / "SIGNALS.jsonl"
            signals.write_text(
                json.dumps({"date": "2025-01-02"}) + "\n", encoding="utf-8"
            )
            archive = root / "trades" / "archived" / "2025_01_03"
            archive.mkdir(parents=True)
            (archive / "2025-01-03-session.md").write_text(
                "fixture\n", encoding="utf-8"
            )
            selection = root / "selection.json"
            selection.write_text(
                json.dumps({"seed": 1, "selected_dates": ["2025-01-06"]}),
                encoding="utf-8",
            )
            value = tranche.build_exclusion_snapshot(
                signal_path=signals,
                archive_root=archive.parent,
                prior_selection_paths=[selection],
                inspected_evidence=(
                    set(),
                    {
                        "registry_sha256": "a" * 64,
                        "artifact_set_sha256": "b" * 64,
                    },
                ),
            )
            self.assertEqual(
                value["excluded_dates"],
                ["2025-01-02", "2025-01-03", "2025-01-06"],
            )
            self.assertEqual(value["archived_contexts"]["files"], 1)
            self.assertFalse(value["target_outcomes_observed_or_derived"])

    def test_explicit_target_date_fields_are_extracted_recursively(self) -> None:
        value = {
            "requested_dates": ["2025-01-02"],
            "nested": {
                "selected_dates": ["2025-01-03"],
                "frozen_dates": [{"date": "2025-01-06"}],
                "candidates_by_date": {"2025-01-07": []},
            },
            "recorded_at": "2025-01-08T12:00:00Z",
            "unrelated_dates": ["2025-01-09"],
        }
        self.assertEqual(
            tranche._extract_target_dates(value),
            {"2025-01-02", "2025-01-03", "2025-01-06", "2025-01-07"},
        )

    def test_conflicting_archive_dates_fail_closed(self) -> None:
        with TemporaryDirectory(dir=tranche.PROJECT_ROOT) as directory:
            root = Path(directory) / "2025_01_03"
            root.mkdir()
            (root / "2025-01-04-session.md").write_text("fixture\n", encoding="utf-8")
            with self.assertRaisesRegex(
                tranche.DevelopmentTrancheError, "conflicting dates"
            ):
                tranche._archived_context_exclusions(root.parent)

    def test_capacity_projection_fails_before_reserve_breach(self) -> None:
        with TemporaryDirectory(dir=tranche.PROJECT_ROOT) as directory:
            root = Path(directory)
            status = root / "status.json"
            status.write_text(
                json.dumps(
                    {
                        "status": "READY",
                        "dataset_id": "dataset-fixture",
                        "collection": {
                            "sessions_ready": 133,
                            "provider_requests": 2808,
                            "canonical_day_merges": 103453,
                        },
                    }
                ),
                encoding="utf-8",
            )
            run_root = root / "run"
            derived_root = root / "derived"
            run_root.mkdir()
            derived_root.mkdir()
            (run_root / "bytes.bin").write_bytes(b"x" * 1_000)
            store = SimpleNamespace(root=root, min_free_bytes=20_000)
            with (
                patch.object(tranche, "PRIOR_SCANNER_STATUS", status),
                patch.object(tranche, "PRIOR_RUN_ROOT", run_root),
                patch.object(tranche, "PRIOR_DERIVED_ROOT", derived_root),
                patch.object(
                    tranche.shutil,
                    "disk_usage",
                    return_value=SimpleNamespace(free=20_001),
                ),
                self.assertRaisesRegex(
                    tranche.DevelopmentTrancheError, "breach.*reserve"
                ),
            ):
                tranche._capacity_projection(required_session_count=243, store=store)


if __name__ == "__main__":
    unittest.main()

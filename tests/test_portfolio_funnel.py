from __future__ import annotations

import json
from pathlib import Path
import unittest

import portfolio_funnel as funnel
import portfolio_maturity as maturity


class PortfolioFunnelTests(unittest.TestCase):
    def test_published_dispositions_are_an_ordered_prefix_with_one_survivor(self):
        dispositions = funnel.load_stage0_dispositions()
        self.assertEqual(
            [item["variant_id"] for item in dispositions],
            list(funnel.FIRST_WAVE_ORDER),
        )
        self.assertEqual(
            [item["status"] for item in dispositions],
            [
                "RETIRED",
                "RETIRED",
                "SURVIVED",
                "RETIRED",
                "RETIRED",
                "RETIRED",
                "RETIRED",
                "RETIRED",
                "RETIRED",
                "RETIRED",
            ],
        )
        self.assertEqual(dispositions[2]["blockers"], [])

    def test_status_exposes_retired_development_and_zero_of_three_progress(self):
        status = funnel.build_funnel_status(maturity.build_report())
        self.assertEqual(status["first_wave"]["retired_count"], 9)
        self.assertEqual(status["first_wave"]["survivor_count"], 1)
        self.assertEqual(
            status["progress"], {"pilot_ready": 0, "live_started": 0, "target": 3}
        )
        self.assertIsNone(status["lanes"]["stage0_falsification"])
        self.assertIsNone(status["lanes"]["representative_development"])
        self.assertIsNone(status["lanes"]["confirmation_or_shadow"])
        self.assertEqual(
            status["validation_candidates"][0]["status"],
            "RETIRED_DEVELOPMENT",
        )
        self.assertEqual(
            status["validation_candidates"][0]["validation_phase"],
            "RETIRED_DEVELOPMENT",
        )
        self.assertFalse(status["notification_due"])
        self.assertEqual(status["notification_reasons"], [])

    def test_remaining_first_wave_queue_matches_frozen_plan(self):
        status = funnel.build_funnel_status(maturity.build_report())
        self.assertEqual(
            [item["variant_id"] for item in status["first_wave"]["candidate_queue"]],
            [],
        )

    def test_second_wave_is_fixed_and_open_after_taxonomy_inspection(self):
        status = funnel.build_funnel_status(maturity.build_report())
        self.assertTrue(status["second_wave"]["required"])
        self.assertTrue(status["second_wave"]["open"])
        self.assertEqual(len(status["second_wave"]["candidate_queue"]), 6)
        self.assertEqual(
            [
                item["mechanism_family"]
                for item in status["second_wave"]["candidate_queue"]
            ],
            [family for _, family in funnel.SECOND_WAVE],
        )

    def test_first_wave_taxonomy_and_independent_inspection_rebuild(self):
        status = funnel.build_funnel_status(maturity.build_report())
        self.assertEqual(len(status["second_wave"]["failure_taxonomy_paths"]), 1)
        self.assertEqual(
            len(status["second_wave"]["failure_taxonomy_inspection_paths"]), 1
        )
        path = Path(__file__).resolve().parents[1] / status["second_wave"][
            "failure_taxonomy_paths"
        ][0]
        taxonomy = json.loads(path.read_text(encoding="utf-8"))
        self.assertEqual(
            taxonomy["taxonomy_sha256"],
            funnel._self_hash(taxonomy, "taxonomy_sha256"),
        )
        self.assertEqual(taxonomy["stage0_disposed_count"], 10)
        self.assertEqual(taxonomy["stage0_retired_count"], 9)
        self.assertEqual(taxonomy["stage0_survivor_count"], 1)
        self.assertEqual(taxonomy["first_wave_active_candidates"], 0)
        self.assertEqual(len(taxonomy["development_dispositions"]), 1)
        self.assertEqual(taxonomy["maturity_effect"], "NONE")
        inspection_path = Path(__file__).resolve().parents[1] / status[
            "second_wave"
        ]["failure_taxonomy_inspection_paths"][0]
        inspection = json.loads(inspection_path.read_text(encoding="utf-8"))
        self.assertEqual(
            inspection["inspection_sha256"],
            funnel._self_hash(inspection, "inspection_sha256"),
        )
        self.assertEqual(inspection["taxonomy_sha256"], taxonomy["taxonomy_sha256"])
        self.assertEqual(inspection["evidence_files_verified"], 23)
        self.assertEqual(inspection["returns_computed"], 0)
        self.assertTrue(inspection["valid"])

    def test_stage0_gate_rebuilds_all_published_failures(self):
        dispositions = funnel.load_stage0_dispositions()
        self.assertEqual(dispositions[0]["closed_signals"], 97)
        self.assertEqual(dispositions[1]["closed_signals"], 29)
        self.assertIn(
            "closed signals are below the Stage 0 minimum",
            dispositions[1]["blockers"],
        )
        self.assertEqual(dispositions[3]["closed_signals"], 65)
        self.assertIn(
            "primary expectancy is not positive",
            dispositions[3]["blockers"],
        )
        self.assertEqual(dispositions[4]["closed_signals"], 95)
        self.assertIn(
            "primary drawdown exceeds the Stage 0 maximum",
            dispositions[4]["blockers"],
        )
        self.assertEqual(dispositions[5]["closed_signals"], 10)
        self.assertEqual(
            dispositions[5]["blockers"],
            ["closed signals are below the Stage 0 minimum"],
        )
        self.assertEqual(dispositions[6]["closed_signals"], 72)
        self.assertEqual(
            dispositions[6]["blockers"],
            ["primary drawdown exceeds the Stage 0 maximum"],
        )
        self.assertEqual(dispositions[7]["closed_signals"], 6)
        self.assertEqual(
            dispositions[7]["blockers"],
            [
                "closed signals are below the Stage 0 minimum",
                "primary expectancy is not positive",
                "primary profit factor is below the Stage 0 minimum",
                "20 bps-per-side total R is not positive",
            ],
        )
        self.assertEqual(dispositions[8]["closed_signals"], 79)
        self.assertEqual(
            dispositions[8]["blockers"],
            [
                "primary expectancy is not positive",
                "primary profit factor is below the Stage 0 minimum",
                "primary drawdown exceeds the Stage 0 maximum",
                "20 bps-per-side total R is not positive",
            ],
        )
        self.assertEqual(dispositions[9]["closed_signals"], 4)
        self.assertEqual(
            dispositions[9]["blockers"],
            ["closed signals are below the Stage 0 minimum"],
        )


if __name__ == "__main__":
    unittest.main()

from __future__ import annotations

import unittest

import portfolio_funnel as funnel
import portfolio_maturity as maturity


class PortfolioFunnelTests(unittest.TestCase):
    def test_published_dispositions_are_an_ordered_retired_prefix(self):
        dispositions = funnel.load_stage0_dispositions()
        self.assertEqual(
            [item["variant_id"] for item in dispositions],
            list(funnel.FIRST_WAVE_ORDER[:2]),
        )
        self.assertTrue(all(item["status"] == "RETIRED" for item in dispositions))
        self.assertTrue(all(item["blockers"] for item in dispositions))

    def test_status_exposes_three_lanes_and_zero_of_three_progress(self):
        status = funnel.build_funnel_status(maturity.build_report())
        self.assertEqual(status["first_wave"]["retired_count"], 2)
        self.assertEqual(status["first_wave"]["survivor_count"], 0)
        self.assertEqual(status["progress"], {"pilot_ready": 0, "live_started": 0, "target": 3})
        self.assertEqual(
            status["lanes"]["stage0_falsification"]["variant_id"],
            "equity-gap-continuation-v1",
        )
        self.assertIsNone(status["lanes"]["representative_development"])
        self.assertIsNone(status["lanes"]["confirmation_or_shadow"])

    def test_remaining_first_wave_queue_matches_frozen_plan(self):
        status = funnel.build_funnel_status(maturity.build_report())
        self.assertEqual(
            [item["variant_id"] for item in status["first_wave"]["candidate_queue"]],
            list(funnel.FIRST_WAVE_ORDER[2:]),
        )

    def test_second_wave_is_fixed_and_closed_before_failure_taxonomy(self):
        status = funnel.build_funnel_status(maturity.build_report())
        self.assertFalse(status["second_wave"]["open"])
        self.assertEqual(len(status["second_wave"]["candidate_queue"]), 6)
        self.assertEqual(
            [item["mechanism_family"] for item in status["second_wave"]["candidate_queue"]],
            [family for _, family in funnel.SECOND_WAVE],
        )

    def test_stage0_gate_rebuilds_both_published_failures(self):
        dispositions = funnel.load_stage0_dispositions()
        self.assertEqual(dispositions[0]["closed_signals"], 97)
        self.assertEqual(dispositions[1]["closed_signals"], 29)
        self.assertIn(
            "closed signals are below the Stage 0 minimum",
            dispositions[1]["blockers"],
        )


if __name__ == "__main__":
    unittest.main()

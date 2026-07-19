import json
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


class SelectedCandidateFidelityInspectionTests(unittest.TestCase):
    def test_public_inspection_is_aggregate_and_ready(self):
        path = (
            PROJECT_ROOT
            / "research_results"
            / "2026-07-19-selected-candidate-fidelity-inspection.json"
        )
        if not path.exists():
            self.skipTest("inspection artifact is generated after collection")
        value = json.loads(path.read_text(encoding="utf-8"))
        self.assertEqual(value["status"], "READY")
        self.assertTrue(value["inspected"])
        self.assertEqual(value["sec_counts"]["selected_pairs"], 389)
        self.assertEqual(value["clean_trigger_counts"]["crossing_windows"], 325)
        rendered = path.read_text(encoding="utf-8")
        self.assertNotIn('"symbol"', rendered)
        self.assertNotIn('"cik"', rendered)
        self.assertNotIn('"records"', rendered)
        self.assertNotIn('"source_url"', rendered)
        self.assertNotIn('"accession"', rendered)


if __name__ == "__main__":
    unittest.main()

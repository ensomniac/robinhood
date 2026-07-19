import json
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


class ChampionInputReadinessInspectionTests(unittest.TestCase):
    def test_public_inspection_is_aggregate_and_outcome_blind(self):
        path = (
            PROJECT_ROOT
            / "research_results"
            / "2026-07-19-champion-input-readiness-inspection.json"
        )
        if not path.exists():
            self.skipTest("independent readiness inspection is generated later")
        value = json.loads(path.read_text(encoding="utf-8"))
        self.assertEqual(value["status"], "READY")
        self.assertTrue(value["independently_inspected"])
        self.assertEqual(value["counts"]["selected_pairs"], 389)
        self.assertEqual(value["counts"]["known_hard_gate_pass"], 0)
        self.assertGreaterEqual(
            value["diagnostic_counts"]["execution_geometry_pass"], 0
        )
        rendered = path.read_text(encoding="utf-8")
        for forbidden in ('"symbol"', '"records"', '"cik"', '"accession"'):
            self.assertNotIn(forbidden, rendered)


if __name__ == "__main__":
    unittest.main()

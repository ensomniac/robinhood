import json
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


class ChampionInputFidelityInspectionTests(unittest.TestCase):
    def test_public_inspection_is_ready_and_aggregate_only(self):
        path = (
            PROJECT_ROOT
            / "research_results"
            / "2026-07-19-champion-input-fidelity-inspection.json"
        )
        if not path.exists():
            self.skipTest("inspection artifact is generated after collection")
        value = json.loads(path.read_text(encoding="utf-8"))
        self.assertEqual(value["status"], "READY")
        self.assertTrue(value["inspected"])
        self.assertEqual(value["counts"]["selected_pairs"], 389)
        self.assertEqual(value["counts"]["trigger_halt_windows_evaluated"], 325)
        rendered = path.read_text(encoding="utf-8")
        for forbidden in ('"symbol"', '"cik"', '"accession"', '"records"'):
            self.assertNotIn(forbidden, rendered)


if __name__ == "__main__":
    unittest.main()

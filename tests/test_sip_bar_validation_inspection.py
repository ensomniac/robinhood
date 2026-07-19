import json
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


class SipBarValidationInspectionTests(unittest.TestCase):
    def test_public_inspection_is_ready_aggregate_and_outcome_blind(self):
        path = (
            PROJECT_ROOT
            / "research_results"
            / "2026-07-19-sip-bar-validation-inspection.json"
        )
        if not path.exists():
            self.skipTest("independent SIP inspection is generated later")
        value = json.loads(path.read_text(encoding="utf-8"))
        self.assertEqual(value["status"], "READY")
        self.assertTrue(value["independently_inspected"])
        self.assertEqual(value["counts"]["minutes"], 325)
        self.assertEqual(value["counts"]["wap_matches"], 325)
        self.assertTrue(all(value["checks"].values()))
        self.assertFalse(value["findings"]["production_rule_change_earned"])
        rendered = path.read_text(encoding="utf-8")
        for forbidden in ('"symbol"', '"records"', '"trade_id"'):
            self.assertNotIn(forbidden, rendered)


if __name__ == "__main__":
    unittest.main()

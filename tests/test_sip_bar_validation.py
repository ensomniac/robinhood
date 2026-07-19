import json
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


class SipBarValidationTests(unittest.TestCase):
    def test_public_result_is_aggregate_and_ready_when_generated(self):
        path = PROJECT_ROOT / "research_results" / "2026-07-19-sip-bar-validation.json"
        if not path.exists():
            self.skipTest("validation result is generated after freezing")
        value = json.loads(path.read_text(encoding="utf-8"))
        self.assertEqual(value["status"], "READY")
        self.assertEqual(value["counts"]["minutes"], 325)
        self.assertEqual(value["counts"]["wap_matches"], 325)
        self.assertTrue(value["findings"]["raw_prefix_required_for_exact_session_vwap"])
        rendered = path.read_text(encoding="utf-8")
        for forbidden in ('"symbol"', '"records"', '"trade_id"'):
            self.assertNotIn(forbidden, rendered)


if __name__ == "__main__":
    unittest.main()

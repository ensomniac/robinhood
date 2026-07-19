import unittest

from preentry_structure_dataset import _collection_coverage


class CollectionCoverageTests(unittest.TestCase):
    def test_exhausted_partial_history_is_terminal_but_not_full_coverage(self):
        result = _collection_coverage(
            {
                "daily_requests": {
                    "one": {
                        "status": "COMPLETE",
                        "required_sessions": 252,
                        "complete_sessions": 17,
                    }
                },
                "premarket_requests": {
                    "day|one": {"status": "COMPLETE", "row_count": 0}
                },
            },
            expected_daily_identities=1,
            expected_premarket_windows=1,
        )

        self.assertEqual(result["status"], "COLLECTION_COMPLETE")
        self.assertEqual(result["daily_identity_requests_complete"], 1)
        self.assertEqual(result["daily_identities_full_252_session_coverage"], 0)
        self.assertEqual(result["daily_identities_with_coverage_gaps"], 1)
        self.assertEqual(result["premarket_windows_complete"], 1)

    def test_error_request_keeps_collection_incomplete(self):
        result = _collection_coverage(
            {
                "daily_requests": {
                    "one": {
                        "status": "ERROR",
                        "required_sessions": 252,
                        "complete_sessions": 0,
                    }
                },
                "premarket_requests": {},
            },
            expected_daily_identities=1,
            expected_premarket_windows=1,
        )

        self.assertEqual(result["status"], "COLLECTION_INCOMPLETE")


if __name__ == "__main__":
    unittest.main()

import unittest

from clean_trigger_fidelity_expansion import _summarize


class CleanTriggerFidelityExpansionTests(unittest.TestCase):
    def test_missing_nbbo_is_counted_without_becoming_collection_error(self):
        records = [
            {
                "status": "MISSING_TRIGGER_NBBO",
                "first_raw_cross": {
                    "decision": {
                        "updates_minute_high_low": True,
                        "establishes_continuous_cross": True,
                    }
                },
                "clean_cross": {"shift_from_first_raw_cross_seconds": 0.1},
                "quote_snapshots": [],
                "basic_fresh_uncrossed": False,
                "final_ask_within_chase_cap": False,
            }
        ]

        summary = _summarize(records)

        self.assertEqual(summary["counts"]["missing_trigger_nbbo"], 1)
        self.assertEqual(summary["counts"]["collection_errors"], 0)
        self.assertEqual(summary["counts"]["clean_crosses"], 1)

    def test_collection_error_remains_retryable_in_summary(self):
        summary = _summarize([{"status": "COLLECTION_ERROR"}])

        self.assertEqual(summary["counts"]["collection_errors"], 1)
        self.assertEqual(summary["counts"]["missing_trigger_nbbo"], 0)


if __name__ == "__main__":
    unittest.main()

import unittest

from champion_input_readiness_expansion import (
    ExpansionReadinessError,
    _filtered_selection,
)


class ExpansionReadinessTests(unittest.TestCase):
    def test_filters_only_verified_positive_nonconflict_pairs(self):
        pairs = [
            {"date": "2026-01-02", "symbol": f"S{index:04d}"}
            for index in range(1987)
        ]
        catalysts = [
            {
                **pair,
                "verified_positive_direction": index < 15,
                "dilution_or_negative_conflict": index == 14,
            }
            for index, pair in enumerate(pairs)
        ]
        result = _filtered_selection(
            {"pairs": pairs, "selected_pair_count": 1987},
            {"catalyst_records": catalysts},
        )
        self.assertEqual(result["source_selected_pair_count"], 1987)
        self.assertEqual(result["selected_pair_count"], 14)
        self.assertEqual(len(result["pairs"]), 14)
        self.assertEqual(
            result["selection_reason"],
            "verified_positive_SEC_primary_without_conflict",
        )

    def test_rejects_any_evaluation_count_drift(self):
        pairs = [
            {"date": "2026-01-02", "symbol": f"S{index:04d}"}
            for index in range(1987)
        ]
        catalysts = [
            {
                **pair,
                "verified_positive_direction": index < 13,
                "dilution_or_negative_conflict": False,
            }
            for index, pair in enumerate(pairs)
        ]
        with self.assertRaisesRegex(ExpansionReadinessError, "exactly 14"):
            _filtered_selection({"pairs": pairs}, {"catalyst_records": catalysts})


if __name__ == "__main__":
    unittest.main()

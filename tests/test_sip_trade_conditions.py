import unittest

from sip_trade_conditions import (
    CONTINUOUS_CROSS_VERSION,
    RULE_VERSION,
    classify_trade_conditions,
    establishes_continuous_cross,
    updates_minute_high_low,
)


class SipTradeConditionTests(unittest.TestCase):
    def test_rule_versions_are_frozen_and_distinct(self):
        self.assertEqual(RULE_VERSION, "alpaca-sip-minute-v1")
        self.assertEqual(CONTINUOUS_CROSS_VERSION, "continuous-regular-cross-v1")

    def test_observed_regular_and_sweep_prints_are_clean(self):
        self.assertTrue(establishes_continuous_cross("A", [" "]))
        self.assertTrue(establishes_continuous_cross("B", [" ", "F"]))
        self.assertTrue(establishes_continuous_cross("C", ["@"]))
        self.assertTrue(establishes_continuous_cross("C", ["@", "F"]))

    def test_odd_lots_neither_update_high_low_nor_establish_cross(self):
        decision = classify_trade_conditions("C", ["@", "F", "I"])
        self.assertFalse(decision.updates_minute_high_low)
        self.assertFalse(decision.establishes_continuous_cross)
        self.assertEqual(decision.reason, "minute_high_low_ineligible:I")

    def test_special_print_can_update_bar_without_being_a_clean_cross(self):
        for tape, condition in (
            ("A", "O"),
            ("B", "X"),
            ("C", "A"),
            ("C", "D"),
            ("C", "T"),
            ("C", "Y"),
            ("C", "5"),
        ):
            with self.subTest(tape=tape, condition=condition):
                self.assertTrue(updates_minute_high_low(tape, [condition]))
                self.assertFalse(establishes_continuous_cross(tape, [condition]))

    def test_published_minute_ineligible_conditions_are_rejected(self):
        for tape in ("A", "B", "C"):
            for condition in (
                "C",
                "H",
                "I",
                "M",
                "N",
                "P",
                "Q",
                "R",
                "U",
                "V",
                "Z",
                "4",
                "7",
                "9",
            ):
                with self.subTest(tape=tape, condition=condition):
                    self.assertFalse(updates_minute_high_low(tape, [condition]))

    def test_tape_specific_average_price_and_bunched_trade_rules(self):
        self.assertFalse(updates_minute_high_low("A", ["B"]))
        self.assertFalse(updates_minute_high_low("B", ["B"]))
        self.assertTrue(updates_minute_high_low("C", ["B"]))
        self.assertFalse(updates_minute_high_low("C", ["W"]))

    def test_strictest_condition_wins(self):
        self.assertTrue(updates_minute_high_low("C", ["@", "F"]))
        self.assertFalse(updates_minute_high_low("C", ["@", "F", "I"]))

    def test_missing_unknown_and_otc_are_fail_closed(self):
        self.assertFalse(updates_minute_high_low("A", []))
        self.assertFalse(updates_minute_high_low("C", ["?"]))
        self.assertFalse(updates_minute_high_low("O", ["@"]))


if __name__ == "__main__":
    unittest.main()

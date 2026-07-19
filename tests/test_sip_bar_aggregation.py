import unittest

from sip_bar_aggregation import (
    GREEN,
    RED,
    aggregate_minute,
    aggregate_prefix_vwap,
    classify_minute_update,
)


def trade(second, price, size, conditions=("@",), tape="C"):
    return {
        "source_timestamp": f"2026-01-02T14:35:{second:02d}.000000Z",
        "price": price,
        "size": size,
        "conditions": list(conditions),
        "tape": tape,
        "trade_id": str(second),
    }


class SipBarAggregationTests(unittest.TestCase):
    def test_regular_sales_update_every_field(self):
        decision = classify_minute_update("C", ["@"])
        self.assertEqual(
            (
                decision.update_open_close,
                decision.update_high_low,
                decision.update_volume,
            ),
            (GREEN, GREEN, GREEN),
        )

    def test_volume_only_trade_does_not_enter_vwap(self):
        rows = [trade(1, 10, 100), trade(2, 99, 50, ("I",))]
        bar = aggregate_minute(rows)
        self.assertIsNotNone(bar)
        self.assertEqual(bar["open"], 10)
        self.assertEqual(bar["high"], 10)
        self.assertEqual(bar["low"], 10)
        self.assertEqual(bar["close"], 10)
        self.assertEqual(bar["volume"], 150)
        self.assertEqual(bar["count"], 2)
        self.assertEqual(bar["wap"], 10)
        self.assertEqual(bar["vwap_eligible_volume"], 100)

    def test_strictest_condition_wins_independently_by_field(self):
        decision = classify_minute_update("C", ["@", "I"])
        self.assertEqual(decision.update_open_close, RED)
        self.assertEqual(decision.update_high_low, RED)
        self.assertEqual(decision.update_volume, GREEN)

    def test_unknown_condition_fails_closed_and_is_counted(self):
        rows = [trade(1, 10, 100), trade(2, 11, 100, ("?",))]
        bar = aggregate_minute(rows)
        self.assertIsNotNone(bar)
        self.assertEqual(bar["unsupported_trade_count"], 1)
        self.assertEqual(bar["close"], 10)

    def test_prefix_vwap_uses_high_low_and_volume_intersection(self):
        rows = [
            trade(1, 10, 100),
            trade(2, 99, 50, ("I",)),
            trade(3, 12, 100),
        ]
        result = aggregate_prefix_vwap(rows)
        self.assertEqual(result["reported_volume"], 250)
        self.assertEqual(result["vwap_eligible_volume"], 200)
        self.assertEqual(result["wap"], 11)

    def test_multiple_minutes_are_rejected(self):
        rows = [trade(1, 10, 100), trade(2, 11, 100)]
        rows[-1]["source_timestamp"] = "2026-01-02T14:36:02Z"
        with self.assertRaisesRegex(ValueError, "multiple minutes"):
            aggregate_minute(rows)


if __name__ == "__main__":
    unittest.main()

import unittest

from historical_metrics import (
    HistoricalMetricError,
    average_daily_volume,
    average_true_range,
)


def daily_bar(epoch, *, high=11.0, low=9.0, close=10.0, volume=1_000_000):
    return {
        "epoch": epoch,
        "high": high,
        "low": low,
        "close": close,
        "volume": volume,
    }


class HistoricalMetricTests(unittest.TestCase):
    def test_average_volume_uses_latest_ordered_periods(self):
        rows = [
            daily_bar(3, volume=3_000_000),
            daily_bar(1, volume=1_000_000),
            daily_bar(2, volume=2_000_000),
        ]

        self.assertEqual(average_daily_volume(rows, 2), 2_500_000)

    def test_atr_includes_previous_close_gap(self):
        rows = [
            daily_bar(1, high=10.0, low=9.0, close=9.5),
            daily_bar(2, high=12.0, low=11.0, close=11.5),
        ]

        self.assertEqual(average_true_range(rows, 1), 2.5)

    def test_duplicate_daily_epochs_fail_closed(self):
        with self.assertRaisesRegex(HistoricalMetricError, "duplicate epochs"):
            average_daily_volume([daily_bar(1), daily_bar(1)], 2)

    def test_invalid_daily_ohlc_fails_closed(self):
        rows = [daily_bar(1), daily_bar(2, high=8.0, low=9.0)]

        with self.assertRaisesRegex(HistoricalMetricError, "high cannot be below"):
            average_true_range(rows, 1)


if __name__ == "__main__":
    unittest.main()

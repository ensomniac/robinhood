import threading
import time
import unittest

from historical_concurrency import ordered_bounded_results


class OrderedBoundedResultsTests(unittest.TestCase):
    def test_overlaps_work_but_yields_in_input_order(self):
        lock = threading.Lock()
        active = 0
        peak = 0

        def work(value):
            nonlocal active, peak
            with lock:
                active += 1
                peak = max(peak, active)
            try:
                time.sleep(0.01 * (4 - value))
                return value * 10
            finally:
                with lock:
                    active -= 1

        results = list(ordered_bounded_results([1, 2, 3], work, max_workers=3))

        self.assertGreaterEqual(peak, 2)
        self.assertEqual([result.item for result in results], [1, 2, 3])
        self.assertEqual([result.unwrap() for result in results], [10, 20, 30])

    def test_break_does_not_submit_the_next_batch(self):
        calls = []

        def work(value):
            calls.append(value)
            if value == 2:
                raise RuntimeError("provider failed")
            return value

        for result in ordered_bounded_results(range(8), work, max_workers=3):
            if result.error is not None:
                break

        self.assertEqual(sorted(calls), [0, 1, 2])

    def test_rejects_invalid_worker_count(self):
        with self.assertRaisesRegex(ValueError, "positive integer"):
            list(ordered_bounded_results([], lambda value: value, max_workers=0))


if __name__ == "__main__":
    unittest.main()

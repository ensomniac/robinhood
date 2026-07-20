import json
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path

import development_non_return_collection as collection


class DevelopmentNonReturnCollectionTests(unittest.TestCase):
    def setUp(self):
        self.start = datetime.fromisoformat("2025-01-02T09:35:00-05:00")

    @staticmethod
    def _trade(observed, price, conditions=("@",)):
        return {
            "source_timestamp": observed.isoformat(),
            "time_et": observed.isoformat(),
            "price": price,
            "size": 100,
            "conditions": list(conditions),
            "tape": "C",
            "trade_id": observed.isoformat(),
        }

    def test_clean_cross_search_stops_before_any_later_window(self):
        calls = []
        checkpoints = []

        def fetch(start, end):
            calls.append((start, end))
            if start == self.start + timedelta(seconds=2):
                return [self._trade(start + timedelta(milliseconds=100), 10.01)]
            return []

        clean, windows = collection.search_clean_cross(
            fetch_second=fetch,
            start=self.start,
            end=self.start + timedelta(seconds=10),
            opening_high=10.0,
            checkpoint=lambda end, rows, clean: checkpoints.append(
                (end, list(rows), clean)
            ),
        )
        self.assertIsNotNone(clean)
        self.assertEqual(windows, 3)
        self.assertEqual(len(calls), 3)
        self.assertEqual(calls[-1][0], self.start + timedelta(seconds=2))
        self.assertEqual(len(checkpoints), 3)
        self.assertIsNotNone(checkpoints[-1][2])

    def test_special_print_does_not_stop_search(self):
        calls = []

        def fetch(start, end):
            calls.append((start, end))
            if len(calls) == 1:
                return [self._trade(start, 10.1, conditions=("I",))]
            if len(calls) == 2:
                return [self._trade(start, 10.2)]
            return []

        clean, windows = collection.search_clean_cross(
            fetch_second=fetch,
            start=self.start,
            end=self.start + timedelta(seconds=5),
            opening_high=10.0,
        )
        self.assertEqual(windows, 2)
        self.assertEqual(clean["price"], 10.2)

    def test_search_checkpoint_flushes_completed_minutes_without_rewriting_history(self):
        pair = {
            "date": "2025-01-02",
            "symbol": "PRIVATE",
            "instrument_id": "FIGI:PRIVATE",
            "rank": 1,
            "scanner_fields": {"opening_high": 10.0},
        }
        state = collection._initial_pair_state(pair, "a" * 64)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            state_path = collection._pair_path(root, pair)
            for offset in range(61):
                collection._record_search_window(
                    store_root=root,
                    pair=pair,
                    state_path=state_path,
                    state=state,
                    next_at=self.start + timedelta(seconds=offset + 1),
                    rows=[],
                    clean=None,
                )
            self.assertEqual(state["search_windows_complete"], 61)
            self.assertEqual(len(state["search_minute_files"]), 1)
            self.assertEqual(state["active_search_minute"]["windows_complete"], 1)
            rebuilt_rows, rebuilt_windows = collection._search_evidence(
                store_root=root, pair=pair, state=state
            )
            self.assertEqual(rebuilt_rows, [])
            self.assertEqual(rebuilt_windows, 61)

    def test_boundary_rejects_a_provider_row_at_exclusive_end(self):
        row = self._trade(self.start + timedelta(seconds=1), 10.0)
        with self.assertRaisesRegex(
            collection.DevelopmentNonReturnCollectionError,
            "outside the causal request boundary",
        ):
            collection._validate_boundary(
                [row],
                start=self.start,
                end=self.start + timedelta(seconds=1),
                inclusive_end=False,
            )

    def test_quote_snapshots_use_latest_past_quote_and_convert_old_round_lots(self):
        quotes = []
        for offset in (-1, 4, 9):
            observed = self.start + timedelta(seconds=offset)
            quotes.append(
                {
                    "source_timestamp": observed.isoformat(),
                    "time_et": observed.isoformat(),
                    "bid": 10.0,
                    "ask": 10.01,
                    "bid_size": 2,
                    "ask_size": 3,
                }
            )
        snapshots = collection.quote_snapshots(quotes, self.start)
        self.assertEqual([row["age_seconds"] for row in snapshots], [1.0, 1.0, 1.0])
        self.assertTrue(all(row["ask_size"] == 300 for row in snapshots))

    def test_contract_status_is_aggregate_only_and_outcome_locked(self):
        result = collection._contract_status(
            {"manifest_sha256": "a" * 64},
            status="FROZEN_READY",
            inspected=True,
        )
        rendered = json.dumps(result, sort_keys=True)
        for private in ("SECRET_TICKER", "FIGI:PRIVATE", "2025-01-02"):
            self.assertNotIn(private, rendered)
        self.assertFalse(result["provider_access_performed"])
        self.assertFalse(result["target_outcomes_observed_or_derived"])


if __name__ == "__main__":
    unittest.main()

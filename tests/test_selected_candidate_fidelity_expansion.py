import gzip
import json
import tempfile
import unittest
from pathlib import Path

from scanner_replay import _instrument_id
from selected_candidate_fidelity_expansion import (
    PROJECT_ROOT,
    _point_in_time_cik_map,
)


class SelectedCandidateFidelityExpansionTests(unittest.TestCase):
    def test_point_in_time_cik_mapping_uses_exact_listing_identity(self):
        with tempfile.TemporaryDirectory(dir=PROJECT_ROOT) as directory:
            root = Path(directory)
            day = "2026-03-03"
            rows = [
                {
                    "ticker": "AAA",
                    "primary_exchange": "XNAS",
                    "composite_figi": "COMP-A",
                    "share_class_figi": "SHARED",
                    "cik": "0000000001",
                    "name": "Listing A",
                },
                {
                    "ticker": "AAB",
                    "primary_exchange": "XNYS",
                    "composite_figi": "COMP-B",
                    "share_class_figi": "SHARED",
                    "cik": "0000000002",
                    "name": "Listing B",
                },
                {
                    "ticker": "AAC",
                    "primary_exchange": "XNYS",
                    "cik": "0000000003",
                    "name": "Fallback Listing",
                },
            ]
            root.mkdir(exist_ok=True)
            with gzip.open(root / f"{day}.json.gz", "wt", encoding="utf-8") as target:
                json.dump(rows, target)
            pairs = [
                {
                    "date": day,
                    "symbol": row["ticker"],
                    "primary_exchange": row["primary_exchange"],
                    "instrument_id": _instrument_id(row)[0],
                }
                for row in rows
            ]

            mapped, sources = _point_in_time_cik_map(pairs, root)

            self.assertEqual([row["cik"] for row in mapped], ["1", "2", "3"])
            self.assertEqual(len(sources), 1)
            self.assertTrue(
                all(
                    row["cik_match_basis"] == "exact_instrument_ticker_exchange"
                    for row in mapped
                )
            )


if __name__ == "__main__":
    unittest.main()

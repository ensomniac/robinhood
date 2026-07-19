import gzip
import json
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path

from historical_migration import LegacyMigrator
from historical_store import EASTERN, HistoricalDayStore


def write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value), encoding="utf-8")


def bar(observed, price=10.0):
    return {
        "epoch": int(observed.timestamp()),
        "time_et": observed.isoformat(),
        "date_et": observed.date().isoformat(),
        "open": price,
        "high": price + 0.1,
        "low": price - 0.1,
        "close": price + 0.05,
        "volume": 100,
        "count": 2,
        "wap": price,
        "interpolated": False,
    }


class HistoricalMigrationTests(unittest.TestCase):
    def test_every_source_is_hashed_and_sources_remain_unchanged(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            legacy = root / "legacy"
            repo = root / "repo-history"
            store = HistoricalDayStore(root / "canonical")
            observed = datetime(2026, 3, 3, 9, 30, tzinfo=EASTERN)
            legacy_path = legacy / "aapl" / "minute" / "2026_3_3" / "ticks_trades.json"
            write_json(
                legacy_path,
                {
                    "complete": True,
                    "last_processed": observed.isoformat(),
                    "contract": {"exchange": "SMART"},
                    "data": {
                        "what": "TRADES",
                        "bar_size": "1 min",
                        "useRTH": 1,
                        "ticks": [bar(observed)],
                    },
                },
            )
            candidate_path = repo / "ibkr" / "2026-03-03-AAPL.json"
            write_json(
                candidate_path,
                {
                    "provider": "Interactive Brokers TWS API",
                    "captured_at": observed.isoformat(),
                    "request": {"symbol": "AAPL", "date": "2026-03-03"},
                    "session_bars": [bar(observed)],
                    "daily_bars": [bar(observed - timedelta(days=1))],
                    "prior_opening_bars": [bar(observed - timedelta(days=1))],
                    "bid_ask_ticks": [
                        {
                            "epoch": int(observed.timestamp()),
                            "time_et": observed.isoformat(),
                            "bid": 10,
                            "ask": 10.01,
                            "bid_size": 100,
                            "ask_size": 200,
                        }
                    ],
                },
            )
            bundle_path = repo / "2026-03-03.json"
            write_json(
                bundle_path,
                {
                    "date": "2026-03-03",
                    "source": {"captured_at": observed.isoformat()},
                    "candidates": [
                        {
                            "symbol": "AAPL",
                            "bars": [
                                {
                                    "time_et": "09:30:00",
                                    "open": 10,
                                    "high": 10.1,
                                    "low": 9.9,
                                    "close": 10.05,
                                    "volume": 100,
                                }
                            ],
                            "score": 90,
                        }
                    ],
                },
            )
            retained_path = repo / "manifests" / "frozen.json"
            write_json(retained_path, {"frozen": True})
            source_paths = [legacy_path, candidate_path, bundle_path, retained_path]
            source_hashes = {path: path.read_bytes() for path in source_paths}

            result = LegacyMigrator(store, legacy, repo).run()

            self.assertTrue(result["valid"])
            self.assertEqual(result["source_files"], len(source_paths))
            self.assertEqual(result["source_status_counts"]["migrated"], 3)
            self.assertEqual(result["source_status_counts"]["retained"], 1)
            for path, content in source_hashes.items():
                self.assertEqual(path.read_bytes(), content)
            document = store.load("AAPL", "2026-03-03")
            self.assertGreaterEqual(len(document["datasets"]), 4)
            context_kinds = {row["kind"] for row in document["contexts"]}
            self.assertIn("legacy_intraday_capture", context_kinds)
            self.assertIn("candidate_market_evidence", context_kinds)
            self.assertIn("replay_candidate", context_kinds)
            ledger_path = (
                store.root
                / "_migrations"
                / "robinhood-codex-canonical-day-v1"
                / "source-files.jsonl.gz"
            )
            with gzip.open(ledger_path, "rt", encoding="utf-8") as stream:
                ledger = [json.loads(line) for line in stream]
            self.assertEqual(len(ledger), len(source_paths))
            self.assertTrue(all(len(row["source_sha256"]) == 64 for row in ledger))


if __name__ == "__main__":
    unittest.main()

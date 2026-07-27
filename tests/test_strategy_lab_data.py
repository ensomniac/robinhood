from __future__ import annotations

import gzip
import json
import tempfile
import unittest
from datetime import datetime, timedelta
from pathlib import Path

from strategy_lab.data import HistoricalCatalog, parse_history_file
from strategy_lab.database import LabDatabase

from tests.strategy_lab_helpers import make_test_config, populate_observations


class StrategyLabDataTests(unittest.TestCase):
    def test_canonical_file_parser_prefers_complete_daily_and_derives_opening_features(
        self,
    ):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / "aaa" / "2024" / "2024-01-02.json.gz"
            path.parent.mkdir(parents=True)
            start = datetime(2024, 1, 2, 9, 30)
            bars = [
                {
                    "t": (start + timedelta(minutes=15 * index)).isoformat() + "-05:00",
                    "o": 100 + index,
                    "h": 101 + index,
                    "l": 99 + index,
                    "c": 100.5 + index,
                    "v": 1000,
                }
                for index in range(25)
            ]
            payload = {
                "schema_version": 1,
                "kind": "us_equity_daily_history",
                "symbol": "AAA",
                "date": "2024-01-02",
                "datasets": [
                    {
                        "id": "bars:test:15m",
                        "provider": "alpaca",
                        "timeframe": "15m",
                        "quality": {"complete": True},
                        "rows": bars,
                    },
                    {
                        "id": "bars:test:1d",
                        "provider": "alpaca",
                        "timeframe": "1d",
                        "quality": {"complete": True},
                        "rows": [
                            {
                                "o": 100,
                                "h": 130,
                                "l": 99,
                                "c": 124.5,
                                "v": 25000,
                            }
                        ],
                    },
                ],
            }
            with gzip.open(path, "wt", encoding="utf-8") as output:
                json.dump(payload, output)
            row = parse_history_file(path, "COMMON")
            self.assertEqual(row.symbol, "AAA")
            self.assertTrue(row.daily_complete)
            self.assertTrue(row.intraday_complete)
            self.assertAlmostEqual(row.opening_return_30m, 101.5 / 100 - 1)
            self.assertEqual(row.intraday_entry_price, 102)

    def test_feature_mart_has_locked_partitions_and_parquet_identity(self):
        with tempfile.TemporaryDirectory() as directory:
            config = make_test_config(Path(directory), sessions=24)
            with LabDatabase(config) as database:
                populate_observations(database, sessions=24)
                result = HistoricalCatalog(config, database).build_feature_mart()
                self.assertEqual(result["capacity_state"], "CAPACITY_READY")
                self.assertTrue(config.feature_path.is_file())
                self.assertEqual(len(result["feature_sha256"]), 64)
                partitions = dict(
                    database.connection.execute(
                        "SELECT data_partition, count(*) FROM features GROUP BY data_partition"
                    ).fetchall()
                )
                self.assertGreater(partitions["development"], 0)
                self.assertGreater(partitions["holdout"], 0)
                self.assertGreater(partitions["embargo"], 0)
                eligible = database.connection.execute(
                    "SELECT count(*) FROM features WHERE eligible"
                ).fetchone()[0]
                self.assertGreater(eligible, 0)

    def test_coverage_is_measured_within_each_symbols_observed_lifetime(self):
        with tempfile.TemporaryDirectory() as directory:
            config = make_test_config(Path(directory), sessions=24)
            config.raw["catalog"]["minimum_prior_sessions"] = 3
            with LabDatabase(config) as database:
                populate_observations(database, sessions=24)
                database.connection.execute(
                    """
                    DELETE FROM observations
                    WHERE symbol='AAA'
                      AND session_date < (
                          SELECT min(session_date) + INTERVAL 6 DAY
                          FROM observations
                      )
                    """
                )
                HistoricalCatalog(config, database).build_feature_mart()
                coverage = database.connection.execute(
                    """
                    SELECT min(coverage_ratio), max(coverage_ratio),
                           count(*) FILTER (WHERE eligible)
                    FROM features WHERE symbol='AAA'
                    """
                ).fetchone()
                self.assertEqual(coverage[:2], (1.0, 1.0))
                self.assertGreater(coverage[2], 0)

    def test_structural_rejections_are_content_hashed_and_not_reparsed(self):
        with tempfile.TemporaryDirectory() as directory:
            config = make_test_config(Path(directory), sessions=24)
            path = config.historical_data_root / "spy" / "2024" / "2024-01-02.json.gz"
            path.parent.mkdir(parents=True)
            with gzip.open(path, "wt", encoding="utf-8") as output:
                json.dump(
                    {
                        "symbol": "SPY",
                        "date": "2024-01-02",
                        "datasets": [],
                    },
                    output,
                )
            with LabDatabase(config) as database:
                catalog = HistoricalCatalog(config, database)
                first = catalog.sync(symbols=["SPY"], rebuild_features=False)
                second = catalog.sync(symbols=["SPY"], rebuild_features=False)
                self.assertEqual(first["files_rejected"], 1)
                self.assertEqual(second["files_rejected"], 0)
                self.assertEqual(second["files_unchanged"], 1)
                disposition, identity = database.connection.execute(
                    "SELECT disposition, content_identity FROM raw_files"
                ).fetchone()
                self.assertTrue(disposition.startswith("REJECTED_"))
                self.assertEqual(len(identity), 64)


if __name__ == "__main__":
    unittest.main()

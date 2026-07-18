import csv
import gzip
import json
import tempfile
import unittest
from datetime import date, datetime, timedelta
from pathlib import Path

from learning_data import load_security_master, resolve_security
from scanner_replay import (
    EASTERN,
    PROJECT_ROOT,
    MassiveFlatFileConfig,
    ScannerReplayError,
    _signed_s3_headers,
    build_scanner_replay,
    build_security_master,
    load_selection,
    required_sessions,
    split_adjustment_factor,
)


def write_minute_file(path, day, *, target=False):
    path.parent.mkdir(parents=True, exist_ok=True)
    start = datetime.combine(date.fromisoformat(day), datetime.min.time(), EASTERN)
    rows = []
    for minute in range(5):
        stamp = start.replace(hour=9, minute=30) + timedelta(minutes=minute)
        opened = 10.2 if target else 10.0
        closed = opened + (minute + 1) * 0.02
        rows.append(
            {
                "ticker": "AAA",
                "volume": 40 if target else 20,
                "open": opened,
                "close": closed,
                "high": closed + 0.05,
                "low": opened - 0.05,
                "window_start": int(stamp.timestamp() * 1_000_000_000),
                "transactions": 10,
            }
        )
    close_stamp = start.replace(hour=15, minute=59)
    rows.append(
        {
            "ticker": "AAA",
            "volume": 1000,
            "open": 10.0,
            "close": 10.0,
            "high": 10.6,
            "low": 9.4,
            "window_start": int(close_stamp.timestamp() * 1_000_000_000),
            "transactions": 10,
        }
    )
    with gzip.open(path, "wt", encoding="utf-8", newline="") as target_file:
        writer = csv.DictWriter(target_file, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


class SelectionContractTests(unittest.TestCase):
    def test_exact_random_selection_order_is_preserved(self):
        dates = [f"2026-01-{day:02d}" for day in range(2, 22)]
        dates[0], dates[-1] = dates[-1], dates[0]
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "selection.json"
            path.write_text(
                json.dumps({"seed": 7, "selected_dates": dates}), encoding="utf-8"
            )

            loaded = load_selection(path)

        self.assertEqual(loaded["selected_dates"], dates)

    def test_required_sessions_never_substitutes_a_missing_lookback(self):
        calendar = [f"2026-01-{day:02d}" for day in range(1, 18)]
        self.assertEqual(len(required_sessions([calendar[-1]], calendar)), 16)
        with self.assertRaisesRegex(ScannerReplayError, "lacks 15"):
            required_sessions([calendar[10]], calendar)


class SecurityMasterBuildTests(unittest.TestCase):
    def test_sourced_observation_dates_preserve_a_symbol_change_gap(self):
        with tempfile.TemporaryDirectory(dir=PROJECT_ROOT) as directory:
            root = Path(directory)
            snapshots = root / "reference"
            snapshots.mkdir()
            common = {
                "active": True,
                "market": "stocks",
                "locale": "us",
                "type": "CS",
                "primary_exchange": "XNAS",
                "share_class_figi": "BBGTESTSHARE",
                "composite_figi": "BBGTESTCOMP",
                "cik": "1",
                "name": "Test Corp",
            }
            for day, ticker in (("2026-01-05", "OLD"), ("2026-03-16", "NEW")):
                with gzip.open(
                    snapshots / f"{day}.json.gz", "wt", encoding="utf-8"
                ) as target:
                    json.dump([{**common, "ticker": ticker}], target)
            output = root / "SECURITY_MASTER.jsonl"
            source = root / "source.json"

            result = build_security_master(
                ["2026-01-05", "2026-03-16"],
                snapshots_root=snapshots,
                output=output,
                source_manifest=source,
                recorded_at="2026-07-18T21:00:00-04:00",
            )

            self.assertEqual(result["security_master"]["records"], 2)
            records = load_security_master(output)
            self.assertEqual(records[0]["instrument_id"], records[1]["instrument_id"])
            self.assertEqual(
                resolve_security("OLD", date(2026, 1, 5), path=output)["symbol"],
                "OLD",
            )
            with self.assertRaisesRegex(ValueError, "exactly one"):
                resolve_security("OLD", date(2026, 2, 2), path=output)


class FlatFileTests(unittest.TestCase):
    def test_s3_signature_is_deterministic_and_never_contains_secret(self):
        config = MassiveFlatFileConfig("access", "top-secret")
        url, headers = _signed_s3_headers(
            config,
            "us_stocks_sip/minute_aggs_v1/2026/01/2026-01-05.csv.gz",
            datetime(2026, 7, 18, 12, 0, tzinfo=EASTERN),
        )

        self.assertTrue(url.startswith("https://files.massive.com/flatfiles/"))
        self.assertIn("Credential=access/", headers["Authorization"])
        self.assertNotIn("top-secret", str(headers))


class ReplayBuildTests(unittest.TestCase):
    def test_split_adjustment_uses_only_actions_effective_by_target(self):
        actions = {
            "AAA": [
                {
                    "execution_date": date(2026, 1, 20),
                    "split_from": 1.0,
                    "split_to": 4.0,
                },
                {
                    "execution_date": date(2026, 2, 20),
                    "split_from": 1.0,
                    "split_to": 2.0,
                },
            ]
        }

        self.assertEqual(
            split_adjustment_factor("AAA", "2026-01-10", "2026-01-30", actions),
            0.25,
        )
        self.assertEqual(
            split_adjustment_factor("AAA", "2026-01-10", "2026-03-01", actions),
            0.125,
        )

    def test_full_universe_ranking_uses_only_target_open_and_prior_sessions(self):
        with tempfile.TemporaryDirectory(dir=PROJECT_ROOT) as directory:
            root = Path(directory)
            calendar = []
            day = date(2026, 1, 2)
            while len(calendar) < 16:
                if day.weekday() < 5:
                    calendar.append(day.isoformat())
                day += timedelta(days=1)
            target_day = calendar[-1]
            minute_root = root / "minutes"
            for session in calendar:
                write_minute_file(
                    minute_root / f"{session}.csv.gz",
                    session,
                    target=session == target_day,
                )
            master = root / "SECURITY_MASTER.jsonl"
            record = {
                "schema_version": 1,
                "record_id": "aaa-target",
                "instrument_id": "FIGI-SHARE:AAA",
                "symbol": "AAA",
                "primary_exchange": "XNAS",
                "security_type": "COMMON",
                "valid_from": target_day,
                "valid_to": target_day,
                "observed_dates": [target_day],
                "status": "ACTIVE",
                "recorded_at": "2026-07-18T21:00:00-04:00",
                "provenance_paths": ["historical_batches/test.json"],
            }
            master.write_text(json.dumps(record) + "\n", encoding="utf-8")
            splits = root / "splits.json.gz"
            with gzip.open(splits, "wt", encoding="utf-8") as target:
                json.dump([], target)
            rules = {
                "allowed_primary_exchanges": ["XNAS"],
                "thresholds": {
                    "minimum_open_price": 5,
                    "minimum_average_daily_volume_14": 0,
                    "minimum_daily_atr_14": 0,
                    "minimum_opening_relative_volume": 1,
                },
                "shortlist_size": 20,
            }

            result = build_scanner_replay(
                selected_dates=[target_day],
                calendar=calendar,
                rules=rules,
                security_path=master,
                minute_root=minute_root,
                split_path=splits,
                detailed_output=root / "detail.json",
                summary_output=root / "summary.json",
            )

            self.assertTrue(result["complete_universe"])
            self.assertEqual(result["dates"][0]["evaluated_count"], 1)
            self.assertEqual(result["dates"][0]["shortlist_count"], 1)
            self.assertEqual(len(result["dates"][0]["shortlist_sha256"]), 64)
            public_text = (root / "summary.json").read_text(encoding="utf-8")
            self.assertNotIn("AAA", public_text)


if __name__ == "__main__":
    unittest.main()

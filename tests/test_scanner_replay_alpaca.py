import csv
import gzip
import json
import tempfile
import unittest
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from historical_store import HistoricalDayStore
from scanner_replay import EASTERN, ScannerReplayError, _parse_minute_file
from scanner_replay_alpaca import (
    AlpacaBulkBarsClient,
    AlpacaBulkConfig,
    DATASET_ID,
    DEFAULT_RULES,
    collect_day,
    freeze_contract,
    load_contract,
)


class FakeResponse:
    def __init__(self, payload, status_code=200, headers=None):
        self.payload = payload
        self.status_code = status_code
        self.headers = dict(headers or {})

    def json(self):
        return self.payload


class FakeSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def get(self, url, *, params, headers, timeout):
        self.calls.append((url, dict(params), dict(headers), timeout))
        return self.responses.pop(0)

    def close(self):
        return None


def alpaca_bar(stamp, *, price=10.0, volume=100, count=10):
    return {
        "t": stamp.astimezone(timezone.utc).isoformat(),
        "o": price,
        "h": price + 0.2,
        "l": price - 0.2,
        "c": price + 0.1,
        "v": volume,
        "n": count,
        "vw": price + 0.05,
    }


class IncrementingClock:
    def __init__(self):
        self.value = 0.0

    def __call__(self):
        self.value += 1.0
        return self.value


class AlpacaBulkConfigTests(unittest.TestCase):
    def test_configuration_never_exposes_credentials(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / ".env"
            path.write_text(
                "ALPACA_KEY=key-value\nALPACA_SECRET=secret-value\n",
                encoding="utf-8",
            )
            config = AlpacaBulkConfig.from_env(path)

        rendered = str(config.public_dict())
        self.assertNotIn("key-value", rendered)
        self.assertNotIn("secret-value", rendered)
        self.assertEqual(config.public_dict()["feed"], "sip")
        self.assertEqual(config.public_dict()["asof"], "-")


class AlpacaBulkClientTests(unittest.TestCase):
    def test_multi_symbol_pages_preserve_source_rows_and_frozen_query_controls(self):
        day = date(2026, 3, 3)
        opened = datetime.combine(day, datetime.min.time(), EASTERN).replace(
            hour=9, minute=30
        )
        session = FakeSession(
            [
                FakeResponse(
                    {
                        "bars": {"AAA": [alpaca_bar(opened)]},
                        "next_page_token": "next-token",
                    }
                ),
                FakeResponse(
                    {"bars": {"BBB": [alpaca_bar(opened)]}, "next_page_token": None}
                ),
            ]
        )
        client = AlpacaBulkBarsClient(
            AlpacaBulkConfig("key-value", "secret-value"),
            session=session,
            sleeper=lambda _: None,
            monotonic=IncrementingClock(),
        )

        result, pages = client.fetch(
            ["AAA", "BBB"],
            timeframe="1Min",
            start=opened,
            end=opened + timedelta(minutes=5) - timedelta(microseconds=1),
        )

        self.assertEqual(pages, 2)
        self.assertEqual(sorted(result), ["AAA", "BBB"])
        self.assertEqual(session.calls[0][1]["feed"], "sip")
        self.assertEqual(session.calls[0][1]["adjustment"], "raw")
        self.assertEqual(session.calls[0][1]["asof"], "-")
        self.assertEqual(session.calls[1][1]["page_token"], "next-token")
        self.assertEqual(session.calls[0][2]["APCA-API-KEY-ID"], "key-value")


class FakeBulkClient:
    def __init__(self, day):
        self.config = AlpacaBulkConfig(
            "key", "secret", batch_size=2, minimum_interval_seconds=0
        )
        self.day = day
        self.request_count = 0
        self.retry_count = 0
        self.request_seconds = 0.0

    def fetch(self, symbols, *, timeframe, start, end):
        self.request_count += 1
        self.request_seconds += 0.01
        if timeframe == "15Min":
            stamp = datetime.combine(self.day, datetime.min.time(), EASTERN).replace(
                hour=9, minute=30
            )
            return {
                symbol: [
                    alpaca_bar(
                        stamp + timedelta(minutes=15 * index),
                        price=10,
                        volume=100,
                        count=10,
                    )
                    for index in range(26)
                ]
                for symbol in symbols
            }, 1
        opened = datetime.combine(self.day, datetime.min.time(), EASTERN).replace(
            hour=9, minute=30
        )
        result = {}
        for symbol in symbols:
            minutes = 5 if symbol == "AAA" else 4
            result[symbol] = [
                alpaca_bar(
                    opened + timedelta(minutes=index),
                    price=10,
                    volume=20,
                    count=2,
                )
                for index in range(minutes)
            ]
        return result, 1


class CanonicalScannerCollectionTests(unittest.TestCase):
    def test_day_collection_is_canonical_exact_and_resumable(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            store = HistoricalDayStore(root / "history")
            client = FakeBulkClient(date(2026, 3, 3))
            index = root / "index"

            result = collect_day(
                "2026-03-03",
                ["AAA", "BBB"],
                client=client,
                store=store,
                index_root=index,
            )

            self.assertEqual(result["daily_symbols"], 2)
            self.assertEqual(result["complete_opening_symbols"], 1)
            self.assertEqual(result["provider_requests"], 2)
            source = index / "minute_aggs" / "2026" / "2026-03-03.csv.gz"
            with gzip.open(source, "rt", encoding="utf-8", newline="") as stream:
                rows = list(csv.DictReader(stream))
            self.assertEqual(len(rows), 11)
            parsed = _parse_minute_file(source)
            self.assertEqual(parsed["AAA"].opening_minutes, 5)
            self.assertEqual(parsed["BBB"].opening_minutes, 4)
            self.assertEqual(parsed["AAA"].volume, 2600)
            self.assertEqual(parsed["AAA"].high, 10.2)
            self.assertEqual(parsed["AAA"].low, 9.8)

            regular = store.select_dataset(
                "AAA",
                "2026-03-03",
                kind="bars",
                channel="trades",
                timeframe="15m",
                providers=("alpaca",),
                require_complete=True,
                feed="sip",
                adjustment="raw",
            )
            accidental_full_session = store.select_dataset(
                "AAA",
                "2026-03-03",
                kind="bars",
                channel="trades",
                timeframe="1m",
                providers=("alpaca",),
                require_complete=True,
                feed="sip",
                adjustment="raw",
            )
            document = store.load("AAA", "2026-03-03")
            opening = [
                item for item in document["datasets"] if item["timeframe"] == "1m"
            ][0]
            self.assertIsNotNone(regular)
            self.assertIsNone(accidental_full_session)
            self.assertTrue(opening["quality"]["opening_window_complete"])
            self.assertFalse(opening["quality"]["complete"])

            calls = client.request_count
            cached = collect_day(
                "2026-03-03",
                ["AAA", "BBB"],
                client=client,
                store=store,
                index_root=index,
            )
            self.assertEqual(cached["disposition"], "cached")
            self.assertEqual(client.request_count, calls)


class AlpacaFreezeTests(unittest.TestCase):
    def test_hash_addressed_contract_keeps_original_dates_rules_and_master(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            output = root / "manifests"
            index = root / "index"

            path, manifest = freeze_contract(
                selection_path=Path(
                    "historical_batches/scanner_replay/selection-2026-07-18-20-days.json"
                ),
                calendar_path=Path(
                    "historical_batches/scanner_replay/session-calendar-2025-12-through-2026-06.json"
                ),
                rules_path=DEFAULT_RULES,
                security_path=Path("learning/SECURITY_MASTER.jsonl"),
                output_root=output,
                index_root=index,
            )

            loaded = load_contract(path)
            self.assertEqual(loaded["dataset_id"], DATASET_ID)
            self.assertEqual(len(loaded["requested_dates"]), 20)
            self.assertEqual(
                loaded["collection_contract"]["required_session_count"], 118
            )
            self.assertFalse(
                loaded["collection_contract"]["pre_freeze_transport_probe"][
                    "price_rows_retained_or_inspected"
                ]
            )
            self.assertEqual(manifest["manifest_sha256"], loaded["manifest_sha256"])

            corrupted = json.loads(path.read_text(encoding="utf-8"))
            corrupted["collection_contract"]["feed"] = "iex"
            path.write_text(json.dumps(corrupted), encoding="utf-8")
            with self.assertRaisesRegex(ScannerReplayError, "mutated or renamed"):
                load_contract(path)


if __name__ == "__main__":
    unittest.main()

import gzip
import json
import tempfile
import unittest
from datetime import datetime
from pathlib import Path

from historical_store import (
    EASTERN,
    HistoricalDayStore,
    HistoricalStoreError,
    build_context,
    build_dataset,
    canonical_sha256,
    compact_bar,
    compact_quote,
    expand_bar,
    expand_quote,
)


class HistoricalDayStoreTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name) / "history"
        self.store = HistoricalDayStore(self.root)

    def tearDown(self):
        self.temporary.cleanup()

    def test_symbol_first_layout_and_idempotent_provenance(self):
        observed = datetime(2026, 3, 3, 9, 30, tzinfo=EASTERN)
        row = compact_bar(
            {
                "time_et": observed.isoformat(),
                "open": 10,
                "high": 11,
                "low": 9,
                "close": 10.5,
                "volume": 100,
                "count": 3,
                "wap": 10.25,
                "provider_flag": "preserved",
            }
        )
        dataset = build_dataset(
            kind="bars",
            provider="ibkr",
            rows=[row],
            channel="trades",
            timeframe="1m",
            quality={"complete": False},
            provenance={"source_path": "first.json"},
        )

        first = self.store.merge("AAPL", "2026-03-03", datasets=[dataset])
        second = self.store.merge("AAPL", "2026-03-03", datasets=[dataset])

        self.assertTrue(first["changed"])
        self.assertFalse(second["changed"])
        self.assertEqual(
            Path(first["path"]),
            self.root.resolve() / "aapl" / "2026" / "2026-03-03.json.gz",
        )
        self.assertEqual(expand_bar(row)["provider_flag"], "preserved")
        self.assertTrue(self.store.audit()["valid"])

    def test_provider_series_are_not_overwritten_and_selection_is_ordered(self):
        observed = datetime(2026, 3, 3, 9, 30, tzinfo=EASTERN)
        datasets = []
        for provider, close in (("alpaca", 10.1), ("massive", 10.2)):
            datasets.append(
                build_dataset(
                    kind="bars",
                    provider=provider,
                    rows=[
                        compact_bar(
                            {
                                "time_et": observed.isoformat(),
                                "open": 10,
                                "high": 10.3,
                                "low": 9.9,
                                "close": close,
                                "volume": 100,
                            }
                        )
                    ],
                    channel="trades",
                    timeframe="1m",
                    quality={"complete": True},
                )
            )
        self.store.merge("AAPL", "2026-03-03", datasets=datasets)

        selected = self.store.select_dataset(
            "AAPL",
            "2026-03-03",
            kind="bars",
            channel="trades",
            timeframe="1m",
            providers=("massive", "alpaca"),
            require_complete=True,
        )

        self.assertEqual(selected["provider"], "massive")
        self.assertEqual(len(self.store.load("AAPL", "2026-03-03")["datasets"]), 2)

    def test_quote_round_trip_preserves_provider_fields(self):
        row = compact_quote(
            {
                "time_et": "2026-03-03T09:35:00-05:00",
                "bid": 10,
                "ask": 10.01,
                "bid_size": 100,
                "ask_size": 200,
                "source_timestamp": "raw-provider-value",
            }
        )
        expanded = expand_quote(row)

        self.assertEqual(expanded["bid"], 10)
        self.assertEqual(expanded["source_timestamp"], "raw-provider-value")

    def test_audit_detects_content_tampering(self):
        context = build_context(
            kind="test",
            provider="ibkr",
            payload={"value": 1},
        )
        self.store.merge("AAPL", "2026-03-03", contexts=[context])
        path = self.store.path_for("AAPL", "2026-03-03")
        with gzip.open(path, "rt", encoding="utf-8") as stream:
            payload = json.load(stream)
        payload["contexts"][0]["payload"]["value"] = 2
        with gzip.open(path, "wt", encoding="utf-8") as stream:
            json.dump(payload, stream)

        audit = self.store.audit()

        self.assertFalse(audit["valid"])
        self.assertIn("corrupt context", audit["errors"][0]["error"])

    def test_invalid_symbol_is_rejected(self):
        with self.assertRaises(HistoricalStoreError):
            self.store.path_for("../secret", "2026-03-03")

    def test_known_provider_aliases_are_canonicalized_before_storage(self):
        observed = datetime(2026, 3, 3, 9, 30, tzinfo=EASTERN)
        dataset = build_dataset(
            kind="bars",
            provider="Interactive Brokers TWS API pre-session history",
            rows=[
                compact_bar(
                    {
                        "time_et": observed.isoformat(),
                        "open": 10,
                        "high": 10.1,
                        "low": 9.9,
                        "close": 10.05,
                        "volume": 100,
                    }
                )
            ],
            channel="trades",
            timeframe="1m",
        )

        self.assertEqual(dataset["provider"], "ibkr")

    def test_alias_repair_rekeys_existing_documents(self):
        observed = datetime(2026, 3, 3, 9, 30, tzinfo=EASTERN)
        dataset = build_dataset(
            kind="bars",
            provider="ibkr",
            rows=[
                compact_bar(
                    {
                        "time_et": observed.isoformat(),
                        "open": 10,
                        "high": 10.1,
                        "low": 9.9,
                        "close": 10.05,
                        "volume": 100,
                    }
                )
            ],
            channel="trades",
            timeframe="1m",
        )
        self.store.merge("AAPL", "2026-03-03", datasets=[dataset])
        path = self.store.path_for("AAPL", "2026-03-03")
        with gzip.open(path, "rt", encoding="utf-8") as stream:
            document = json.load(stream)
        stored = document["datasets"][0]
        stored["provider"] = "interactive_brokers_tws_api_pre_session_history"
        identity = {
            key: stored.get(key)
            for key in (
                "kind",
                "provider",
                "channel",
                "timeframe",
                "feed",
                "adjustment",
                "session",
                "scope",
                "quality",
                "limitations",
                "rows",
            )
        }
        content_hash = canonical_sha256(identity)
        stored["content_sha256"] = content_hash
        stored["id"] = f"bars:{stored['provider']}:trades:1m:{content_hash[:16]}"
        with gzip.open(path, "wt", encoding="utf-8") as stream:
            json.dump(document, stream)

        repair = self.store.repair_provider_aliases()

        self.assertEqual(repair["files_changed"], 1)
        self.assertEqual(repair["items_rekeyed"], 1)
        self.assertEqual(
            self.store.load("AAPL", "2026-03-03")["datasets"][0]["provider"],
            "ibkr",
        )
        self.assertTrue(self.store.audit()["valid"])

    def test_alias_repair_merges_first_release_pre_session_feed_duplicate(self):
        observed = datetime(2026, 3, 2, 0, 0, tzinfo=EASTERN)
        row = compact_bar(
            {
                "time_et": observed.isoformat(),
                "open": 10,
                "high": 10.1,
                "low": 9.9,
                "close": 10.05,
                "volume": 100,
            }
        )
        common = {
            "kind": "bars",
            "provider": "ibkr",
            "rows": [row],
            "channel": "trades",
            "timeframe": "1d",
            "adjustment": "provider_adjusted_unknown_basis",
            "scope": "daily_summary",
            "quality": {"complete": True},
        }
        old = build_dataset(
            **common,
            feed="unknown",
            provenance={"source_path": "old.json"},
        )
        corrected = build_dataset(
            **common,
            feed="smart",
            provenance={"source_path": "corrected.json"},
        )
        self.store.merge("AAPL", "2026-03-02", datasets=[old, corrected])

        repair = self.store.repair_provider_aliases()
        document = self.store.load("AAPL", "2026-03-02")

        self.assertEqual(repair["items_merged"], 1)
        self.assertEqual(len(document["datasets"]), 1)
        self.assertEqual(document["datasets"][0]["feed"], "smart")
        self.assertEqual(
            document["datasets"][0]["provenance"]["source_count"], 2
        )


if __name__ == "__main__":
    unittest.main()

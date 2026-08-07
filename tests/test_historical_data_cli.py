import json
import stat
import tempfile
import unittest
from argparse import Namespace
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

from historical_data_cli import (
    _checkpoint_path,
    _load_refresh_manifest,
    _refresh,
)
from historical_providers import HistoricalProviderError
from historical_service import OpenProviderSet, ProviderAttempt, RecordingHistoricalClient
from historical_store import (
    EASTERN,
    HistoricalDayStore,
    HistoricalStoreError,
    build_dataset,
    compact_bar,
)


def minute_row(symbol_day="2026-03-03"):
    observed = datetime.fromisoformat(f"{symbol_day}T09:30:00-05:00")
    return {
        "epoch": int(observed.timestamp()),
        "time_et": observed.astimezone(EASTERN).isoformat(),
        "date_et": symbol_day,
        "open": 10.0,
        "high": 10.1,
        "low": 9.9,
        "close": 10.05,
        "volume": 100,
        "count": 2,
        "wap": 10.02,
        "interpolated": False,
    }


class FakeProvider:
    provider_name = "Alpaca Market Data API"
    cache_namespace = "alpaca"
    feed = "sip"
    adjustment = "raw"

    def fetch_bars(self, *args, **kwargs):
        return [minute_row()]


class FailingProvider:
    provider_name = "Unavailable historical provider"

    def fetch_bars(self, *args, **kwargs):
        raise HistoricalProviderError("offline", category="retryable_transport")


class RefreshManifestTests(unittest.TestCase):
    def test_manifest_is_normalized_sorted_and_content_addressed(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "requests.json"
            path.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "requests": [
                            {"symbol": "msft", "date": "2026-03-03"},
                            {"symbol": "AAPL", "date": "2026-03-03"},
                        ],
                    }
                ),
                encoding="utf-8",
            )

            manifest, identity = _load_refresh_manifest(path)

        self.assertEqual(
            manifest["requests"],
            [
                {"date": "2026-03-03", "symbol": "AAPL"},
                {"date": "2026-03-03", "symbol": "MSFT"},
            ],
        )
        self.assertEqual(len(identity), 64)

    def test_duplicates_and_unknown_fields_fail_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "requests.json"
            path.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "requests": [
                            {"symbol": "AAPL", "date": "2026-03-03"},
                            {"symbol": "aapl", "date": "2026-03-03"},
                        ],
                    }
                ),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(HistoricalStoreError, "duplicate"):
                _load_refresh_manifest(path)

            path.write_text(
                json.dumps(
                    {
                        "schema_version": 1,
                        "requests": [
                            {
                                "symbol": "AAPL",
                                "date": "2026-03-03",
                                "rank": 1,
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
            with self.assertRaisesRegex(HistoricalStoreError, "exactly"):
                _load_refresh_manifest(path)


class RefreshExecutionTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.store = HistoricalDayStore(self.root / "store", min_free_bytes=0)
        self.manifest = self.root / "requests.json"
        self.manifest.write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "requests": [{"symbol": "AAPL", "date": "2026-03-03"}],
                }
            ),
            encoding="utf-8",
        )
        self.args = Namespace(
            manifest=self.manifest,
            env_file=self.root / "missing.env",
        )

    def tearDown(self):
        self.temporary.cleanup()

    def _seed_cache(self):
        row = minute_row()
        self.store.merge(
            "AAPL",
            "2026-03-03",
            datasets=[
                build_dataset(
                    kind="bars",
                    provider="ibkr",
                    rows=[compact_bar(row)],
                    channel="trades",
                    timeframe="1m",
                    feed="smart",
                    adjustment="provider_adjusted_unknown_basis",
                    scope="full_session",
                    quality={"complete": True},
                )
            ],
        )

    def test_cache_hit_checkpoints_once_and_resumes_without_provider(self):
        self._seed_cache()
        with patch(
            "historical_data_cli.open_provider_set",
            side_effect=AssertionError("cache hit must not open providers"),
        ):
            first = _refresh(self.args, self.store)
            second = _refresh(self.args, self.store)

        path = Path(first["checkpoint_path"])
        self.assertTrue(first["valid"])
        self.assertEqual(first["completed"], 1)
        self.assertEqual(first["checkpointed_before"], 0)
        self.assertEqual(second["checkpointed_before"], 1)
        self.assertEqual(second["results"][0]["status"], "checkpointed")
        self.assertEqual(len(path.read_text(encoding="utf-8").splitlines()), 1)
        self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)

    def test_live_success_is_recorded_before_checkpoint(self):
        recording = RecordingHistoricalClient(FakeProvider(), self.store)

        @contextmanager
        def providers(*args, **kwargs):
            yield OpenProviderSet(
                cache_clients=[],
                live_clients=[recording],
                startup_attempts=[
                    ProviderAttempt(
                        recording.provider_name, "ready", None, 0.0
                    )
                ],
            )

        with patch("historical_data_cli.open_provider_set", providers):
            result = _refresh(self.args, self.store)

        self.assertTrue(result["valid"])
        self.assertEqual(result["completed"], 1)
        self.assertTrue(self.store.path_for("AAPL", "2026-03-03").is_file())
        self.assertTrue(Path(result["checkpoint_path"]).is_file())

    def test_failure_is_reported_and_not_checkpointed(self):
        @contextmanager
        def providers(*args, **kwargs):
            yield OpenProviderSet(
                cache_clients=[],
                live_clients=[FailingProvider()],
                startup_attempts=[],
            )

        with patch("historical_data_cli.open_provider_set", providers):
            result = _refresh(self.args, self.store)

        manifest, identity = _load_refresh_manifest(self.manifest)
        self.assertEqual(len(manifest["requests"]), 1)
        self.assertFalse(result["valid"])
        self.assertEqual(result["failed"], 1)
        self.assertFalse(_checkpoint_path(self.store, identity).exists())

    def test_tampered_checkpoint_fails_closed(self):
        self._seed_cache()
        result = _refresh(self.args, self.store)
        path = Path(result["checkpoint_path"])
        value = json.loads(path.read_text(encoding="utf-8"))
        value["provider"] = "tampered"
        path.write_text(json.dumps(value) + "\n", encoding="utf-8")

        with self.assertRaisesRegex(HistoricalStoreError, "record hash drifted"):
            _refresh(self.args, self.store)


if __name__ == "__main__":
    unittest.main()

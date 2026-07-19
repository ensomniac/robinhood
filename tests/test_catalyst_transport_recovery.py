from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import Mock, patch

import catalyst_primary_source_capture as primary
import catalyst_transport_recovery as recovery
from historical_store import HistoricalDayStore


def fixtures():
    capture_records = []
    index_records = {}
    pair_records = []
    identities = []
    for pair_index in range(6):
        date_value = f"2026-01-{pair_index + 2:02d}"
        symbol = f"T{pair_index}"
        article_key = f"article-{pair_index}"
        pair_records.append(
            {
                "date": date_value,
                "symbol": symbol,
                "article_keys": [article_key],
            }
        )
        identities.append(
            {
                "date": date_value,
                "symbol": symbol,
                "instrument_id": f"instrument-{pair_index}",
                "primary_exchange": "XNYS",
                "issuer_name": f"Issuer {pair_index}",
                "cik": str(1000 + pair_index),
            }
        )
    for index in range(13):
        source_hash = f"{index:064x}"
        capture_records.append(
            {
                "url": f"https://issuer{index}.example/release",
                "url_sha256": source_hash,
                "category": recovery.EXPECTED_CATEGORY,
                "article_keys": [f"article-{index % 6}"],
            }
        )
        index_records[source_hash] = {
            "status": "CAPTURE_ERROR",
            "error": recovery.EXPECTED_ERROR,
        }
    return {
        "capture_selection": {"records": capture_records},
        "capture_index": {"records": index_records},
        "discovery": {"pair_records": pair_records},
        "identities": {"pairs": identities},
    }


class TransportSelectionTests(unittest.TestCase):
    def test_exact_transport_surface_rebuilds(self) -> None:
        selection = recovery.build_selection(**fixtures())
        self.assertEqual(
            selection["counts"],
            {
                "transport_failure_sources": 13,
                "pair_source_joins": 13,
                "pairs": 6,
            },
        )
        self.assertFalse(selection["target_outcomes_observed_or_derived"])

    def test_nontransport_failure_cannot_enter(self) -> None:
        values = fixtures()
        first = next(iter(values["capture_index"]["records"].values()))
        first["error"] = "different failure"
        with self.assertRaisesRegex(
            recovery.CatalystTransportRecoveryError, "counts differ"
        ):
            recovery.build_selection(**values)

    def test_unexpected_category_fails_closed(self) -> None:
        values = fixtures()
        values["capture_selection"]["records"][0]["category"] = "AUTHORITY_CANDIDATE"
        with self.assertRaisesRegex(
            recovery.CatalystTransportRecoveryError, "unexpected source category"
        ):
            recovery.build_selection(**values)

    def test_missing_pair_identity_fails_closed(self) -> None:
        values = fixtures()
        values["identities"]["pairs"].pop()
        with self.assertRaisesRegex(
            recovery.CatalystTransportRecoveryError, "lacks frozen identity"
        ):
            recovery.build_selection(**values)


class TransportRetryTests(unittest.TestCase):
    @patch("catalyst_transport_recovery.time.sleep")
    @patch("catalyst_transport_recovery.primary._request_once")
    def test_transport_failures_retry_then_retain_success(
        self, request_once: Mock, sleep: Mock
    ) -> None:
        request_once.side_effect = [
            primary.PrimarySourceCaptureError("source request transport failure"),
            primary.PrimarySourceCaptureError("source request transport failure"),
            {"status": "RESPONSE_CAPTURED", "http_status": 200, "body": b"ok"},
        ]
        result = recovery._capture_with_retry(Mock(), "https://example.com")
        self.assertEqual(result["http_status"], 200)
        self.assertEqual(request_once.call_count, 3)
        self.assertEqual(sleep.call_count, 2)

    @patch("catalyst_transport_recovery.time.sleep")
    @patch("catalyst_transport_recovery.primary._request_once")
    def test_permanent_error_is_not_retried(
        self, request_once: Mock, sleep: Mock
    ) -> None:
        request_once.side_effect = primary.PrimarySourceCaptureError(
            "response exceeded frozen byte limit"
        )
        result = recovery._capture_with_retry(Mock(), "https://example.com")
        self.assertEqual(result["status"], "CAPTURE_ERROR")
        self.assertEqual(request_once.call_count, 1)
        sleep.assert_not_called()

    @patch("catalyst_transport_recovery.time.sleep")
    @patch("catalyst_transport_recovery.primary._request_once")
    def test_http_429_retries(self, request_once: Mock, sleep: Mock) -> None:
        request_once.side_effect = [
            {"status": "RESPONSE_CAPTURED", "http_status": 429, "body": b""},
            {"status": "RESPONSE_CAPTURED", "http_status": 200, "body": b"ok"},
        ]
        result = recovery._capture_with_retry(Mock(), "https://example.com")
        self.assertEqual(result["http_status"], 200)
        self.assertEqual(request_once.call_count, 2)
        sleep.assert_called_once()

    def test_public_summary_excludes_private_rows(self) -> None:
        selection = {
            "counts": {
                "transport_failure_sources": 13,
                "pair_source_joins": 13,
                "pairs": 6,
            }
        }
        index = {
            "status": "COLLECTION_COMPLETE",
            "records": {
                "hash": {
                    "status": "RESPONSE_CAPTURED",
                    "http_status": 200,
                    "body_bytes": 10,
                }
            },
        }
        summary = recovery._public_summary(
            {"manifest_sha256": "manifest"}, selection, index
        )
        self.assertEqual(summary["response_bytes"], 10)
        self.assertNotIn("records", summary)
        self.assertFalse(summary["target_outcomes_observed_or_derived"])


class TransportCollectionTests(unittest.TestCase):
    @staticmethod
    def _selection() -> dict:
        return {
            "counts": {
                "transport_failure_sources": 13,
                "pair_source_joins": 13,
                "pairs": 6,
            },
            "records": [
                {
                    "source_sha256": f"{index:064x}",
                    "source_url": f"https://issuer{index}.example/release",
                }
                for index in range(13)
            ],
        }

    @patch("catalyst_transport_recovery.shutil.disk_usage")
    @patch("catalyst_transport_recovery.HistoricalDayStore.from_env")
    @patch("catalyst_transport_recovery._verify_contract")
    def test_disk_reserve_failure_precedes_network(
        self, verify: Mock, from_env: Mock, disk_usage: Mock
    ) -> None:
        with TemporaryDirectory() as directory:
            store = HistoricalDayStore(Path(directory), min_free_bytes=100)
            from_env.return_value = store
            verify.return_value = (
                {"manifest_sha256": "manifest"},
                self._selection(),
            )
            disk_usage.return_value = Mock(free=99)
            with self.assertRaisesRegex(
                recovery.CatalystTransportRecoveryError, "disk reserve"
            ):
                recovery.collect(
                    manifest_path=Path("manifest.json"),
                    env_path=Path(".env"),
                    public_status_path=Path(directory) / "status.json",
                )

    @patch("catalyst_transport_recovery.time.sleep")
    @patch("catalyst_transport_recovery.shutil.disk_usage")
    @patch("catalyst_transport_recovery.requests.Session")
    @patch("catalyst_transport_recovery._capture_with_retry")
    @patch("catalyst_transport_recovery.HistoricalDayStore.from_env")
    @patch("catalyst_transport_recovery._verify_contract")
    def test_collection_resumes_from_existing_checkpoint(
        self,
        verify: Mock,
        from_env: Mock,
        capture: Mock,
        session_type: Mock,
        disk_usage: Mock,
        sleep: Mock,
    ) -> None:
        with TemporaryDirectory() as directory:
            store = HistoricalDayStore(Path(directory), min_free_bytes=100)
            from_env.return_value = store
            selection = self._selection()
            manifest = {"manifest_sha256": "manifest"}
            verify.return_value = (manifest, selection)
            disk_usage.return_value = Mock(free=10_000)
            capture.return_value = {
                "status": "RESPONSE_CAPTURED",
                "http_status": 200,
                "body": b"ok",
            }
            first_hash = selection["records"][0]["source_sha256"]
            recovery._write_json(
                recovery._index_path(store.root),
                {
                    "schema_version": 1,
                    "dataset_id": recovery.DATASET_ID,
                    "manifest_sha256": "manifest",
                    "status": "COLLECTING",
                    "records": {first_hash: {"status": "CAPTURE_ERROR"}},
                    "target_outcomes_observed_or_derived": False,
                },
            )
            result = recovery.collect(
                manifest_path=Path("manifest.json"),
                env_path=Path(".env"),
                public_status_path=Path(directory) / "status.json",
            )
            self.assertEqual(capture.call_count, 12)
            self.assertEqual(result["terminal_sources"], 13)
            self.assertEqual(result["status"], "COLLECTION_COMPLETE")
            session_type.return_value.close.assert_called_once()
            self.assertGreaterEqual(sleep.call_count, 1)


if __name__ == "__main__":
    unittest.main()

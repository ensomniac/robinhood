from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import Mock, patch

import requests

import catalyst_issuer_chain_recovery as recovery
from historical_store import HistoricalDayStore


def source_selection():
    records = []
    for pair_index in range(6):
        source_count = 8 if pair_index == 0 else 1
        for source_index in range(source_count):
            records.append(
                {
                    "source_sha256": f"{pair_index:02x}{source_index:062x}",
                    "pairs": [
                        {
                            "date": f"2026-01-{pair_index + 2:02d}",
                            "symbol": f"T{pair_index}",
                            "instrument_id": f"instrument-{pair_index}",
                            "primary_exchange": "XNYS",
                            "issuer_name": f"Target {pair_index}",
                            "cik": str(1000 + pair_index),
                        }
                    ],
                }
            )
    return {"records": records}


def completed_plan():
    template = recovery.plan_template(source_selection())
    template["review_completed"] = True
    for index, row in enumerate(template["records"]):
        row["issuer_domain"] = f"issuer{index}.example"
        row["chain_urls"] = [
            f"https://issuer{index}.example/news",
            f"https://issuer{index}.example/news/document",
        ]
        row["selection_basis"] = "CANONICAL_ISSUER_NEWSROOM_CHAIN"
        row["notes"] = "Official issuer-controlled chain."
    return template


class IssuerChainSelectionTests(unittest.TestCase):
    def test_exact_chain_surface_rebuilds(self) -> None:
        selection = recovery.build_selection(source_selection(), completed_plan())
        self.assertEqual(
            selection["counts"],
            {
                "chains": 6,
                "source_predecessors": 13,
                "chain_urls": 12,
                "unique_urls": 12,
            },
        )
        self.assertFalse(selection["target_outcomes_observed_or_derived"])

    def test_pair_identity_cannot_change(self) -> None:
        plan = completed_plan()
        plan["records"][0]["target_cik"] = "9999"
        with self.assertRaisesRegex(
            recovery.IssuerChainRecoveryError, "identity or source set differs"
        ):
            recovery.build_selection(source_selection(), plan)

    def test_source_predecessor_cannot_be_dropped(self) -> None:
        plan = completed_plan()
        plan["records"][0]["source_predecessor_sha256"].pop()
        with self.assertRaisesRegex(
            recovery.IssuerChainRecoveryError, "identity or source set differs"
        ):
            recovery.build_selection(source_selection(), plan)

    def test_chain_url_must_stay_under_issuer_domain(self) -> None:
        plan = completed_plan()
        plan["records"][0]["chain_urls"][1] = "https://secondary.example/document"
        with self.assertRaisesRegex(
            recovery.IssuerChainRecoveryError, "frozen issuer domain"
        ):
            recovery.build_selection(source_selection(), plan)

    def test_outcome_shaped_plan_field_is_rejected(self) -> None:
        plan = completed_plan()
        plan["future_price"] = 10
        with self.assertRaisesRegex(
            recovery.IssuerChainRecoveryError, "plan contract is invalid"
        ):
            recovery.build_selection(source_selection(), plan)


class IssuerChainNetworkTests(unittest.TestCase):
    @patch("catalyst_issuer_chain_recovery._validate_network_target")
    def test_cross_domain_redirect_is_rejected(self, validate: Mock) -> None:
        validate.return_value = ["8.8.8.8"]
        response = Mock()
        response.status_code = 302
        response.headers = {"Location": "https://secondary.example/document"}
        session = Mock()
        session.get.return_value = response
        with self.assertRaisesRegex(
            recovery.IssuerChainRecoveryError, "frozen issuer domain"
        ):
            recovery._request_once(
                session, "https://issuer.example/news", "issuer.example"
            )
        response.close.assert_called_once()

    @patch("catalyst_issuer_chain_recovery._validate_network_target")
    def test_same_domain_response_is_captured(self, validate: Mock) -> None:
        validate.return_value = ["8.8.8.8"]
        response = Mock()
        response.status_code = 200
        response.headers = {"Content-Type": "text/html"}
        response.iter_content.return_value = [b"issuer"]
        session = Mock()
        session.get.return_value = response
        result = recovery._request_once(
            session, "https://issuer.example/news", "issuer.example"
        )
        self.assertEqual(result["body"], b"issuer")
        self.assertEqual(result["http_status"], 200)

    @patch("catalyst_issuer_chain_recovery.time.sleep")
    @patch("catalyst_issuer_chain_recovery._request_once")
    def test_transport_retry_is_bounded(self, request_once: Mock, sleep: Mock) -> None:
        request_once.side_effect = requests.RequestException("unexpected")
        with self.assertRaises(requests.RequestException):
            recovery._capture_with_retry(
                Mock(), "https://issuer.example/news", "issuer.example"
            )
        sleep.assert_not_called()

    @patch("catalyst_issuer_chain_recovery.time.sleep")
    @patch("catalyst_issuer_chain_recovery._request_once")
    def test_declared_transport_failure_retries(
        self, request_once: Mock, sleep: Mock
    ) -> None:
        request_once.side_effect = [
            recovery.IssuerChainRecoveryError("issuer-chain transport failure"),
            {"status": "RESPONSE_CAPTURED", "http_status": 200, "body": b"ok"},
        ]
        result = recovery._capture_with_retry(
            Mock(), "https://issuer.example/news", "issuer.example"
        )
        self.assertEqual(result["http_status"], 200)
        self.assertEqual(request_once.call_count, 2)
        sleep.assert_called_once()


class IssuerChainCollectionTests(unittest.TestCase):
    @staticmethod
    def _selection() -> dict:
        return {
            "counts": {
                "chains": 6,
                "source_predecessors": 13,
                "chain_urls": 12,
                "unique_urls": 12,
            },
            "urls": [
                {
                    "url_sha256": f"{index:064x}",
                    "url": f"https://issuer{index}.example/news",
                    "issuer_domain": f"issuer{index}.example",
                }
                for index in range(12)
            ],
        }

    @patch("catalyst_issuer_chain_recovery.shutil.disk_usage")
    @patch("catalyst_issuer_chain_recovery.HistoricalDayStore.from_env")
    @patch("catalyst_issuer_chain_recovery._verify_contract")
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
                recovery.IssuerChainRecoveryError, "disk reserve"
            ):
                recovery.collect(
                    manifest_path=Path("manifest.json"),
                    env_path=Path(".env"),
                    public_status_path=Path(directory) / "status.json",
                )

    @patch("catalyst_issuer_chain_recovery.time.sleep")
    @patch("catalyst_issuer_chain_recovery.shutil.disk_usage")
    @patch("catalyst_issuer_chain_recovery.requests.Session")
    @patch("catalyst_issuer_chain_recovery._capture_with_retry")
    @patch("catalyst_issuer_chain_recovery.HistoricalDayStore.from_env")
    @patch("catalyst_issuer_chain_recovery._verify_contract")
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
            first_hash = selection["urls"][0]["url_sha256"]
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
            self.assertEqual(capture.call_count, 11)
            self.assertEqual(result["terminal_urls"], 12)
            self.assertEqual(result["status"], "COLLECTION_COMPLETE")
            session_type.return_value.close.assert_called_once()
            self.assertGreaterEqual(sleep.call_count, 1)


if __name__ == "__main__":
    unittest.main()

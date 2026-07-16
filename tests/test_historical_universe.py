import json
import tempfile
import unittest
from pathlib import Path

from historical_universe import (
    HistoricalUniverseError,
    ResumablePreflightProbe,
    freeze_candidate_universe,
)
from ibkr_historical import IBKRRequestError


def candidate(symbol):
    return {
        "symbol": symbol,
        "is_common_stock": True,
        "catalyst": {"point_in_time": True},
    }


def draft_manifest(count=12):
    return {
        "schema_version": 1,
        "scanner": {"universe_capture_complete": True},
        "candidate_pool_by_date": {
            "2026-03-03": [candidate(f"T{index:02d}") for index in range(count)]
        },
    }


class FreezeCandidateUniverseTests(unittest.TestCase):
    def test_skips_unresolvable_symbol_before_freeze_and_uses_ranked_buffer(self):
        def probe(symbol, day):
            self.assertEqual(day, "2026-03-03")
            if symbol == "T02":
                return {
                    "symbol": symbol,
                    "viable": False,
                    "reason": "unresolvable_security_definition",
                    "error_code": 200,
                }
            return {
                "symbol": symbol,
                "viable": True,
                "reason": "contract_resolved",
                "error_code": None,
            }

        frozen = freeze_candidate_universe(
            draft_manifest(),
            probe,
            performed_at="2026-07-15T23:00:00+00:00",
        )

        accepted = frozen["candidates_by_date"]["2026-03-03"]
        self.assertEqual(
            [row["symbol"] for row in accepted],
            ["T00", "T01", "T03", "T04", "T05", "T06", "T07", "T08", "T09", "T10"],
        )
        report = frozen["preflight"]["dates"]["2026-03-03"]
        self.assertEqual(report["skipped"][0]["symbol"], "T02")
        self.assertEqual(report["unused_buffer_symbols"], ["T11"])
        self.assertFalse(frozen["preflight"]["target_session_prices_observed"])

    def test_exhausted_pool_fails_instead_of_freezing_too_few_names(self):
        def probe(symbol, day):
            return {
                "symbol": symbol,
                "viable": symbol != "T02",
                "reason": (
                    "contract_resolved"
                    if symbol != "T02"
                    else "unresolvable_security_definition"
                ),
                "error_code": 200 if symbol == "T02" else None,
            }

        with self.assertRaisesRegex(HistoricalUniverseError, "only 9 viable"):
            freeze_candidate_universe(draft_manifest(count=10), probe)

    def test_provider_failure_stops_preflight_instead_of_skipping_symbol(self):
        def probe(symbol, day):
            raise IBKRRequestError("not connected", error_code=504)

        with self.assertRaisesRegex(IBKRRequestError, "not connected"):
            freeze_candidate_universe(draft_manifest(), probe)

    def test_known_non_common_stock_is_skipped_without_provider_request(self):
        draft = draft_manifest()
        draft["candidate_pool_by_date"]["2026-03-03"][0][
            "is_common_stock"
        ] = False
        calls = []

        def probe(symbol, day):
            calls.append(symbol)
            return {
                "symbol": symbol,
                "viable": True,
                "reason": "pre_session_history_available",
                "error_code": None,
            }

        frozen = freeze_candidate_universe(draft, probe)

        report = frozen["preflight"]["dates"]["2026-03-03"]
        self.assertEqual(report["skipped"][0]["reason"], "not_us_listed_common_stock")
        self.assertNotIn("T00", calls)
        self.assertEqual(len(frozen["candidates_by_date"]["2026-03-03"]), 10)

    def test_preflight_cache_resumes_without_repeating_provider_request(self):
        calls = []

        def detailed_probe(symbol, day):
            calls.append((symbol, day))
            return (
                {
                    "symbol": symbol,
                    "viable": True,
                    "reason": "pre_session_history_available",
                    "error_code": None,
                },
                {
                    "schema_version": 1,
                    "symbol": symbol,
                    "session_date": day,
                    "target_session_prices_observed": False,
                    "prior_opening_bars": [],
                    "daily_bars": [],
                },
            )

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            first = ResumablePreflightProbe(root, detailed_probe)
            first_result = first("T00", "2026-03-03")
            self.assertTrue(first_result["viable"])
            self.assertEqual(len(first_result["pre_session_history_sha256"]), 64)
            cached_payload = json.loads(
                (root / "2026-03-03" / "T00.json").read_text(encoding="utf-8")
            )
            self.assertEqual(cached_payload["schema_version"], 2)

            def should_not_run(symbol, day):
                raise AssertionError(f"unexpected cache miss for {symbol} {day}")

            resumed = ResumablePreflightProbe(root, should_not_run)
            self.assertTrue(resumed("T00", "2026-03-03")["viable"])

        self.assertEqual(calls, [("T00", "2026-03-03")])

    def test_reusable_history_manifest_requires_content_hashes(self):
        def probe(symbol, day):
            return {
                "symbol": symbol,
                "viable": True,
                "reason": "pre_session_history_available",
                "error_code": None,
            }

        with self.assertRaisesRegex(HistoricalUniverseError, "history hash"):
            freeze_candidate_universe(
                draft_manifest(),
                probe,
                cache_metadata={"reusable_pre_session_history": True},
            )


if __name__ == "__main__":
    unittest.main()

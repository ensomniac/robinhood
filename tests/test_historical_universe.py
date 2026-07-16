import unittest

from historical_universe import (
    HistoricalUniverseError,
    freeze_candidate_universe,
)
from ibkr_historical import IBKRRequestError


def candidate(symbol):
    return {"symbol": symbol, "catalyst": {"point_in_time": True}}


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


if __name__ == "__main__":
    unittest.main()

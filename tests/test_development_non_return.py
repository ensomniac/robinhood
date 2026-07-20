import json
import tempfile
import unittest
from pathlib import Path

import development_non_return as non_return


def _pair(index, day="2025-01-02"):
    return {
        "date": day,
        "symbol": f"T{index}",
        "instrument_id": f"FIGI:{index}",
        "primary_exchange": "XNAS",
        "rank": index + 1,
        "scanner_fields": {
            "open_price": 10.0,
            "opening_high": 10.5,
            "opening_low": 9.9,
            "opening_close": 10.4,
            "opening_volume": 100_000,
            "opening_relative_volume": 3.0,
            "opening_return": 0.04,
            "average_daily_volume_14": 2_000_000,
            "daily_atr_14": 1.0,
            "prior_close": 10.0,
        },
    }


class DevelopmentNonReturnTests(unittest.TestCase):
    def test_positive_hash_join_retains_only_terminal_positive_pairs(self):
        pairs = [_pair(index) for index in range(3)]
        dispositions = {
            non_return.source_semantics._sha256_json(
                (pair["date"], pair["instrument_id"])
            ): disposition
            for pair, disposition in zip(
                pairs,
                (
                    "VERIFIED_POSITIVE_PRIMARY",
                    "VERIFIED_CONFLICT",
                    "VERIFIED_POSITIVE_PRIMARY",
                ),
                strict=True,
            )
        }
        result = non_return.build_positive_selection(
            {"selected_pairs": pairs},
            dispositions,
            expected_source_pairs=3,
            expected_positive_pairs=2,
        )
        self.assertEqual(result["source_selected_pair_count"], 3)
        self.assertEqual(result["positive_pair_count"], 2)
        self.assertFalse(result["target_outcomes_observed_or_derived"])

    def test_positive_hash_join_rejects_denominator_or_identity_drift(self):
        pairs = [_pair(index) for index in range(2)]
        dispositions = {
            non_return.source_semantics._sha256_json(
                (pairs[0]["date"], pairs[0]["instrument_id"])
            ): "VERIFIED_POSITIVE_PRIMARY",
            "f" * 64: "VERIFIED_POSITIVE_PRIMARY",
        }
        with self.assertRaisesRegex(
            non_return.DevelopmentNonReturnError, "hashes do not match"
        ):
            non_return.build_positive_selection(
                {"selected_pairs": pairs},
                dispositions,
                expected_source_pairs=2,
                expected_positive_pairs=2,
            )

    def test_request_graph_is_causal_and_aggregate_counts_reconcile(self):
        selection = {
            "positive_pairs": [
                _pair(0, "2025-01-02"),
                _pair(1, "2025-01-02"),
                _pair(2, "2025-01-03"),
            ]
        }
        result = non_return.build_request_graph(selection)
        self.assertEqual(
            result["counts"],
            {
                "positive_pairs": 3,
                "positive_dates": 2,
                "candidate_bar_prefixes": 3,
                "benchmark_bar_prefixes": 4,
                "candidate_premarket_prefixes": 3,
                "candidate_history_prefixes": 3,
                "official_halt_dates": 2,
            },
        )
        rendered = json.dumps(result["graph"], sort_keys=True)
        self.assertIn("10:30_ET", rendered)
        self.assertNotIn("15:50", rendered)
        self.assertNotIn("PLUS_11", rendered)
        self.assertEqual(
            result["graph"]["conditional_clean_cross_search"]["window_seconds"],
            1,
        )
        self.assertFalse(
            result["graph"]["conditional_clean_cross_search"][
                "later_windows_after_clean_cross_allowed"
            ]
        )
        self.assertEqual(
            result["graph"]["conditional_quote_window"]["end"],
            "CLEAN_CROSS_PLUS_10_SECONDS_INCLUSIVE",
        )
        self.assertFalse(result["graph"]["target_outcomes_observed_or_derived"])
        self.assertFalse(
            result["graph"]["provider_policy"]["provider_switching_allowed"]
        )

    def test_strategy_contract_fails_closed_on_coarse_gate_drift(self):
        pair = _pair(0)
        pair["scanner_fields"]["opening_close"] = pair["scanner_fields"]["open_price"]
        with self.assertRaisesRegex(
            non_return.DevelopmentNonReturnError, "bullish_opening_candle"
        ):
            non_return._strategy_contract({"positive_pairs": [pair]})

    def test_public_status_contains_no_private_identity(self):
        selection = {
            "source_selected_pair_count": 1,
            "positive_pair_count": 1,
            "positive_pair_identity_sha256": "a" * 64,
            "positive_pairs": [_pair(0)],
        }
        request = {
            "counts": {"positive_pairs": 1, "positive_dates": 1},
            "positive_date_identity_sha256": "b" * 64,
            "request_graph_sha256": "c" * 64,
        }
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "private.gz"
            path.write_bytes(b"private")
            result = non_return._public_status(
                manifest={"manifest_sha256": "d" * 64},
                selection=selection,
                request=request,
                private_path=path,
                status="FROZEN_READY",
                inspected=True,
            )
        rendered = json.dumps(result)
        self.assertNotIn("T0", rendered)
        self.assertNotIn("2025-01-02", rendered)
        self.assertNotIn("FIGI:0", rendered)
        self.assertFalse(result["outcome_contract_permitted"])


if __name__ == "__main__":
    unittest.main()

import unittest
from datetime import date, timedelta

from shadow_reversal import (
    EXTERNAL_WRITE_CAPABILITIES,
    ORDER_ACTIONS_ALLOWED,
    evaluate_shadow_session,
    qualify_shadow_records,
    verify_shadow_record,
)


def reversal_bars():
    bars = []
    minute_of_day = 9 * 60 + 30
    for _ in range(390):
        hour, minute = divmod(minute_of_day, 60)
        bars.append(
            {
                "time_et": f"{hour:02d}:{minute:02d}:00",
                "open": 100.0,
                "high": 100.1,
                "low": 99.9,
                "close": 100.0,
                "volume": 10_000,
                "interpolated": False,
            }
        )
        minute_of_day += 1
    for index in range(5):
        bars[index].update({"open": 100.0, "high": 100.1, "low": 99.7, "close": 99.8})
    bars[5].update(
        {
            "open": 99.8,
            "high": 100.1,
            "low": 99.75,
            "close": 100.05,
            "volume": 10_000,
        }
    )
    return bars


def capture(day="2026-01-02"):
    prior = (date.fromisoformat(day) - timedelta(days=1)).isoformat()
    symbol = "TEST"
    return {
        "schema_version": 1,
        "mode": "shadow",
        "date": day,
        "premarket_shortlist_frozen_at": f"{day}T09:00:00-05:00",
        "shortlist_complete": True,
        "simulation_account_equity": 100_000.0,
        "simulation_buying_power": 100_000.0,
        "candidates": [
            {
                "symbol": symbol,
                "public_alias": f"{day}-{symbol}-1",
                "catalyst": {
                    "point_in_time": True,
                    "published_at": f"{prior}T17:00:00-05:00",
                    "source_url": "https://www.sec.gov/test",
                    "filing_items": "2.02,9.01",
                },
                "bars": reversal_bars(),
                "signal_detected_at": f"{day}T09:36:01-05:00",
                "quote_snapshots": [
                    {
                        "quoted_at": f"{day}T09:36:01-05:00",
                        "observed_at": f"{day}T09:36:02-05:00",
                        "bid": 100.04,
                        "ask": 100.06,
                        "bid_size": 100_000,
                        "ask_size": 100_000,
                    },
                    {
                        "quoted_at": f"{day}T09:36:05-05:00",
                        "observed_at": f"{day}T09:36:06-05:00",
                        "bid": 100.04,
                        "ask": 100.06,
                        "bid_size": 100_000,
                        "ask_size": 100_000,
                    },
                    {
                        "quoted_at": f"{day}T09:36:09-05:00",
                        "observed_at": f"{day}T09:36:10-05:00",
                        "bid": 100.04,
                        "ask": 100.06,
                        "bid_size": 100_000,
                        "ask_size": 100_000,
                    },
                ],
                "stop_ready_at": f"{day}T09:36:08-05:00",
                "simulated_protection_at": f"{day}T09:36:12-05:00",
                "monitoring": {"complete": True, "maximum_gap_seconds": 5},
                "market_path": [
                    {
                        "quoted_at": f"{day}T09:36:59-05:00",
                        "observed_at": f"{day}T09:37:00-05:00",
                        "bid": 100.90,
                        "ask": 100.92,
                        "bid_size": 100_000,
                        "ask_size": 100_000,
                        "interval_low_bid": 100.00,
                        "interval_high_bid": 100.90,
                    }
                ],
            }
        ],
    }


class ShadowCaptureTests(unittest.TestCase):
    def test_complete_target_lifecycle_is_privacy_safe_and_risk_sized(self):
        record = evaluate_shadow_session(capture())

        self.assertEqual(record["status"], "trade")
        self.assertTrue(record["lifecycle_complete"])
        self.assertEqual(record["outcome"]["exit_reason"], "target_2r")
        self.assertFalse(record["structural_stop_compressed"])
        self.assertLessEqual(record["planned_loss_fraction"], 0.0025)
        self.assertEqual(verify_shadow_record(record), record)

    def test_stale_quote_chase_miss_partial_data_and_monitoring_gap_fail_closed(self):
        stale = capture()
        stale["candidates"][0]["quote_snapshots"][0]["quoted_at"] = (
            "2026-01-02T09:35:50-05:00"
        )
        stale_record = evaluate_shadow_session(stale)
        self.assertEqual(stale_record["status"], "missed_signal")
        self.assertIn("stale_entry_quote", stale_record["rule_violations"])

        chase = capture()
        for snapshot in chase["candidates"][0]["quote_snapshots"]:
            snapshot["bid"] = 100.23
            snapshot["ask"] = 100.25
        chase_record = evaluate_shadow_session(chase)
        self.assertEqual(chase_record["status"], "missed_signal")
        self.assertIn("fresh_ask_above_chase_cap", chase_record["reason"])

        partial = capture()
        partial["candidates"][0]["quote_snapshots"] = partial["candidates"][0][
            "quote_snapshots"
        ][:2]
        partial_record = evaluate_shadow_session(partial)
        self.assertEqual(partial_record["status"], "missed_signal")
        self.assertIn("three_quote_snapshots_missing", partial_record["reason"])

        gap = capture()
        gap["candidates"][0]["monitoring"]["maximum_gap_seconds"] = 16
        gap_record = evaluate_shadow_session(gap)
        self.assertEqual(gap_record["status"], "missed_signal")
        self.assertIn(
            "monitoring_gap_exceeds_15_seconds", gap_record["rule_violations"]
        )

    def test_stop_target_ambiguity_is_stop_first(self):
        ambiguous = capture()
        point = ambiguous["candidates"][0]["market_path"][0]
        point.update(
            {
                "bid": 99.60,
                "ask": 99.62,
                "interval_low_bid": 99.50,
                "interval_high_bid": 101.00,
            }
        )

        record = evaluate_shadow_session(ambiguous)

        self.assertEqual(record["status"], "trade")
        self.assertEqual(
            record["outcome"]["exit_reason"], "stop_first_ambiguous_interval"
        )
        self.assertTrue(record["outcome"]["same_interval_ambiguity"])

    def test_force_flat_requires_and_uses_1550_bid(self):
        force_flat = capture()
        force_flat["candidates"][0]["market_path"] = [
            {
                "quoted_at": "2026-01-02T15:49:59-05:00",
                "observed_at": "2026-01-02T15:50:00-05:00",
                "bid": 100.20,
                "ask": 100.22,
                "bid_size": 100_000,
                "ask_size": 100_000,
            }
        ]

        record = evaluate_shadow_session(force_flat)

        self.assertEqual(record["status"], "trade")
        self.assertEqual(record["outcome"]["exit_reason"], "force_flat_15_50")
        self.assertEqual(record["outcome"]["exit_price"], 100.20)

    def test_shadow_module_has_no_order_or_external_write_capability(self):
        self.assertFalse(ORDER_ACTIONS_ALLOWED)
        self.assertEqual(EXTERNAL_WRITE_CAPABILITIES, ())


class ShadowQualificationTests(unittest.TestCase):
    def test_twenty_signals_over_thirty_days_pass_execution_gates(self):
        days = [
            (date(2026, 1, 2) + timedelta(days=index)).isoformat()
            for index in range(19)
        ] + ["2026-01-31"]
        records = [evaluate_shadow_session(capture(day)) for day in days]
        confirmation = {
            "acceptance": {"all_passed": True},
            "decision": {
                "next_stage": "advance_structural_stop_risk_sized_arm_to_shadow"
            },
        }

        result = qualify_shadow_records(records, confirmation_result=confirmation)

        self.assertTrue(result["shadow_execution_passed"])
        self.assertTrue(result["historical_confirmation_passed"])
        self.assertEqual(
            result["proposal_gate_status"], "qualified_for_normal_cadence_review"
        )
        self.assertEqual(result["calendar_span_days"], 30)
        self.assertEqual(result["modeled_trade_days"], 20)
        self.assertFalse(result["automatic_strategy_application"])

    def test_missed_signal_is_journaled_as_zero_r_and_blocks_incomplete_sample(self):
        missed_capture = capture()
        for snapshot in missed_capture["candidates"][0]["quote_snapshots"]:
            snapshot["bid"] = 100.23
            snapshot["ask"] = 100.25
        missed = evaluate_shadow_session(missed_capture)
        result = qualify_shadow_records([missed])

        self.assertEqual(result["missed_signal_days"], 1)
        self.assertEqual(
            result["signal_statistics_including_operational_misses_as_zero_r"][
                "mean_r"
            ],
            0.0,
        )
        self.assertFalse(result["shadow_execution_passed"])


if __name__ == "__main__":
    unittest.main()

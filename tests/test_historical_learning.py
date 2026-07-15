import json
import tempfile
import unittest
from datetime import date
from pathlib import Path

from historical_learning import (
    HistoricalLearningError,
    run_selected_dates,
    select_random_dates,
    validate_bundle,
)
from strategy_ledger import audit_ledger, read_records
from trade_lifecycle import audit_lifecycle, load_archived_outcomes


def bars():
    result = []
    hour = 9
    minute = 30
    for _ in range(390):
        time_text = f"{hour:02d}:{minute:02d}:00"
        before_trigger = "09:35:00" <= time_text < "09:40:00"
        result.append(
            {
                "time_et": time_text,
                "open": 49.95 if before_trigger else 50.0,
                "high": 49.99 if before_trigger else 50.1,
                "low": 49.9,
                "close": 49.95 if before_trigger else 50.0,
                "volume": 10000,
                "interpolated": False,
            }
        )
        minute += 1
        if minute == 60:
            hour += 1
            minute = 0
    return result


def evaluation_payload(symbol, rank):
    return {
        "session": {
            "time_et": "09:40:00",
            "mode": "shadow",
            "maturity": "UNVALIDATED",
            "agentic_allowed": True,
            "account_identified": True,
            "encryption_ready": True,
            "monitoring_available": True,
            "protective_stop_workflow_ready": True,
            "broker_review_available": True,
            "open_positions": 0,
            "unresolved_orders": 0,
            "filled_entries_today": 0,
            "circuit_breaker_active": False,
            "account_equity": 25000,
            "buying_power": 25000,
        },
        "candidate": {
            "symbol": symbol,
            "is_common_stock": True,
            "opening_price": 50.0,
            "average_daily_volume_14": 5000000,
            "daily_atr_14": 3.0,
            "opening_bar": {
                "open": 49.5,
                "high": 50.0,
                "low": 49.4,
                "close": 49.9,
                "volume": 600000,
            },
            "prior_opening_volumes": [100000] * 14,
            "opening_rvol_rank": rank,
            "ranking_scope": "full_eligible_universe",
            "ranking_scope_count": 10,
            "verified_catalyst": True,
            "catalyst_score": 25,
            "dilution_conflict": False,
            "halt_risk": False,
            "tradable": True,
            "clean_break": True,
            "above_vwap": True,
            "vwap_flat_or_rising": True,
            "benchmark_supportive_or_independent_strength": True,
            "sector_relative_strength": True,
            "stop_outside_noise": True,
            "entry_limit": 50.05,
            "technical_invalidation": 49.75,
            "resistance_price": 51.55,
            "observed_stop_slippage_p95_fraction": 0.001,
        },
        "quotes": [
            {
                "observed_at_et": "09:39:50",
                "age_seconds": 1.0,
                "bid": 50.00,
                "ask": 50.04,
                "ask_depth": 10000,
                "recent_real_1m_volume": 20000,
            },
            {
                "observed_at_et": "09:39:55",
                "age_seconds": 1.5,
                "bid": 50.01,
                "ask": 50.05,
                "ask_depth": 10000,
                "recent_real_1m_volume": 20000,
            },
            {
                "observed_at_et": "09:39:59",
                "age_seconds": 2.0,
                "bid": 50.01,
                "ask": 50.05,
                "ask_depth": 10000,
                "recent_real_1m_volume": 20000,
            },
        ],
    }


def replay_bundle():
    day = "2025-06-02"
    candidates = []
    for index in range(10):
        symbol = f"A{index:02d}"
        candidates.append(
            {
                "signal_id": f"{day}-{symbol}-1",
                "symbol": symbol,
                "evaluation_time_et": "09:40:00",
                "catalyst": {
                    "source_url": f"https://example.com/{symbol}",
                    "published_at": "2025-06-01T12:00:00+00:00",
                    "point_in_time": True,
                },
                "evaluation_payload": evaluation_payload(symbol, index + 1),
                "bars": bars(),
            }
        )
    return {
        "schema_version": 1,
        "date": day,
        "sample_phase": "pilot",
        "session_capture_complete": True,
        "simulation_account_equity": 25000,
        "simulation_buying_power": 25000,
        "source": {
            "provider": "test point-in-time feed",
            "captured_at": "2026-07-15T12:00:00+00:00",
            "point_in_time": True,
            "regular_hours_only": True,
            "split_adjusted": True,
            "historical_quotes_and_depth": True,
            "catalysts_point_in_time": True,
            "universe_capture_complete": True,
        },
        "candidates": candidates,
    }


class BundleTests(unittest.TestCase):
    def test_complete_bundle_validates(self):
        validate_bundle(replay_bundle(), today_et=date(2026, 7, 15))

    def test_missing_depth_attestation_is_rejected(self):
        bundle = replay_bundle()
        bundle["source"]["historical_quotes_and_depth"] = False

        with self.assertRaisesRegex(HistoricalLearningError, "historical_quotes"):
            validate_bundle(bundle, today_et=date(2026, 7, 15))

    def test_delayed_breakout_observation_is_rejected(self):
        bundle = replay_bundle()
        bundle["candidates"][0]["bars"][8]["open"] = 50.01
        bundle["candidates"][0]["bars"][8]["high"] = 50.02
        bundle["candidates"][0]["bars"][8]["close"] = 50.01

        with self.assertRaisesRegex(HistoricalLearningError, "after the first"):
            validate_bundle(bundle, today_et=date(2026, 7, 15))

    def test_random_selection_excludes_archived_days(self):
        with tempfile.TemporaryDirectory() as directory:
            archive = Path(directory)
            (archive / "2025_06_02").mkdir()
            selected, seed = select_random_dates(
                ["2025-06-02", "2025-06-03", "2025-06-04"],
                2,
                archive,
                seed=42,
                today_et=date(2026, 7, 15),
            )

        self.assertEqual(seed, 42)
        self.assertEqual(set(selected), {"2025-06-03", "2025-06-04"})


class ReplayTests(unittest.TestCase):
    def test_replay_selects_one_trade_and_archives_every_context(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            active = root / "trades" / "active"
            archive = root / "trades" / "archived"
            ledger = root / "SIGNALS.jsonl"
            data_root = root / "historical_data"
            active.mkdir(parents=True)
            data_root.mkdir()
            (data_root / "2025-06-02.json").write_text(
                json.dumps(replay_bundle()), encoding="utf-8"
            )

            batch = run_selected_dates(
                ["2025-06-02"],
                data_root=data_root,
                ledger_path=ledger,
                active_root=active,
                archive_root=archive,
                seed=7,
            )
            result = batch["results"][0]
            records = read_records(ledger)
            outcomes = load_archived_outcomes(archive)
            ledger_audit = audit_ledger(ledger)
            lifecycle_audit = audit_lifecycle(
                active_root=active,
                archive_root=archive,
                today_et=date(2026, 7, 15),
            )

        signals = [record for record in records if record["record_type"] == "signal"]
        selected = [record for record in signals if record["decision"] == "shadow"]
        missed = [record for record in signals if record["decision"] == "missed"]
        self.assertEqual(batch["selected_dates"], ["2025-06-02"])
        self.assertEqual(result["selected_signal_id"], "2025-06-02-A00-1")
        self.assertEqual(len(selected), 1)
        self.assertEqual(len(missed), 9)
        self.assertEqual(len(records), 11)
        self.assertEqual(len(outcomes), 11)
        self.assertFalse(list(active.glob("*.md")))
        self.assertTrue(ledger_audit.valid)
        self.assertTrue(lifecycle_audit.valid)


if __name__ == "__main__":
    unittest.main()

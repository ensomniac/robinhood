import copy
import csv
import gzip
import inspect
import json
import tempfile
import unittest
from datetime import date, datetime, time, timedelta
from pathlib import Path

from scanner_replay import EASTERN, _sha256_json
from scanner_replay_alpaca import CSV_FIELDS, DATASET_ID
from scanner_replay_inspection import (
    ScannerInspectionError,
    _compare_independent_detail,
    _independently_recompute_detail,
    _load_independent_source_day,
    _verify_source_attestations,
    inspect_payloads,
)


def fixtures():
    row = {
        "symbol": "AAA",
        "instrument_id": "FIGI-SHARE:AAA",
        "primary_exchange": "XNAS",
        "open_price": 10.0,
        "opening_high": 10.3,
        "opening_low": 9.9,
        "opening_close": 10.2,
        "opening_volume": 200,
        "prior_opening_volume_mean_14": 100.0,
        "opening_relative_volume": 2.0,
        "average_daily_volume_14": 2_000_000.0,
        "daily_atr_14": 1.0,
        "split_adjustment_factor_oldest_session": 1.0,
        "prior_close": 10.0,
        "opening_return": 0.02,
        "bullish_opening_candle": True,
        "disposition": "eligible",
        "opening_rvol_rank": 1,
    }
    shortlist_hash = _sha256_json(
        [
            {
                "symbol": "AAA",
                "instrument_id": "FIGI-SHARE:AAA",
                "opening_relative_volume": 2.0,
                "opening_return": 0.02,
                "rank": 1,
            }
        ]
    )
    manifest = {
        "dataset_id": DATASET_ID,
        "manifest_sha256": "m" * 64,
        "requested_dates": ["2026-03-03"],
        "collection_contract": {"required_session_count": 1},
    }
    summary = {
        "dataset_id": DATASET_ID,
        "status": "READY",
        "complete_universe": True,
        "selection_is_dynamic": True,
        "completed_dates": 1,
        "requested_dates": ["2026-03-03"],
        "selection_time_et": "09:35:00",
        "information_cutoff": "TARGET_SESSION_09:35_ET",
        "scanner_rules_sha256": "r" * 64,
        "security_master_sha256": "s" * 64,
        "split_actions_sha256": "p" * 64,
        "detailed_artifact": {"sha256": "d" * 64},
        "source": {"contract_sha256": "m" * 64},
        "claim_boundary": "scanner only",
        "dates": [
            {
                "date": "2026-03-03",
                "master_common_stock_count": 1,
                "evaluated_count": 1,
                "eligible_count": 1,
                "rejection_counts": {"eligible": 1},
                "shortlist_count": 1,
                "shortlist_sha256": shortlist_hash,
            }
        ],
    }
    detail = {
        "scanner_rules_sha256": "r" * 64,
        "security_master_sha256": "s" * 64,
        "split_actions_sha256": "p" * 64,
        "dates": {
            "2026-03-03": {
                "master_common_stock_count": 1,
                "evaluations": [row],
                "selected_symbols": ["AAA"],
            }
        },
    }
    rules = {
        "shortlist_size": 20,
        "thresholds": {
            "minimum_open_price": 5,
            "minimum_average_daily_volume_14": 1_000_000,
            "minimum_daily_atr_14": 0.5,
            "minimum_opening_relative_volume": 1,
        },
    }
    source_status = {
        "valid": True,
        "complete": True,
        "session_files": {"ready": 1},
    }
    return manifest, summary, detail, rules, source_status


class ScannerInspectionTests(unittest.TestCase):
    def test_independent_recompute_boundary_contains_only_consumed_inputs(self):
        self.assertEqual(
            set(inspect.signature(_independently_recompute_detail).parameters),
            {
                "manifest",
                "rules",
                "security_path",
                "calendar_path",
                "split_path",
                "source_root",
            },
        )

    def test_source_attestation_must_match_contract_and_postdate_freeze(self):
        day = "2026-03-03"
        manifest = {
            "registered_at": "2026-03-01T12:00:00-05:00",
            "collection_contract": {
                "target_symbol_union_count": 1,
                "required_session_dates": [day],
            },
        }
        sidecar = {
            "schema_version": 1,
            "dataset_id": DATASET_ID,
            "date": day,
            "status": "READY",
            "source": {
                "provider": "Alpaca",
                "feed": "sip",
                "adjustment": "raw",
                "asof": "-",
            },
            "requested_symbols": 1,
            "daily_symbols": 1,
            "opening_symbols": 1,
            "complete_opening_symbols": 1,
            "derived_rows": 6,
            "provider_requests": 2,
            "provider_retries": 0,
            "canonical_files_changed": 1,
            "source_sha256": "a" * 64,
            "captured_at": "2026-03-02T18:00:00+00:00",
        }
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / "attestations" / "2026" / f"{day}.json"
            path.parent.mkdir(parents=True)
            path.write_text(json.dumps(sidecar), encoding="utf-8")

            result = _verify_source_attestations(manifest, root)
            self.assertTrue(result["all_captured_after_freeze"])

            sidecar["captured_at"] = "2026-03-01T12:00:00-05:00"
            path.write_text(json.dumps(sidecar), encoding="utf-8")
            with self.assertRaisesRegex(ScannerInspectionError, "postdate"):
                _verify_source_attestations(manifest, root)

    def test_independent_parser_requires_exact_opening_and_one_residual(self):
        day = "2026-03-03"
        opened = datetime.combine(date.fromisoformat(day), time(9, 30), tzinfo=EASTERN)
        rows = []
        for offset in range(5):
            observed = opened + timedelta(minutes=offset)
            rows.append(
                {
                    "ticker": "AAA",
                    "volume": 100 + offset,
                    "open": 10.0 + offset / 100,
                    "close": 10.01 + offset / 100,
                    "high": 10.02 + offset / 100,
                    "low": 9.99 + offset / 100,
                    "window_start": int(observed.timestamp() * 1_000_000_000),
                    "transactions": 10,
                }
            )
        rows.append(
            {
                "ticker": "AAA",
                "volume": 1_000,
                "open": 10.0,
                "close": 10.5,
                "high": 10.7,
                "low": 9.8,
                "window_start": int(
                    datetime.combine(
                        date.fromisoformat(day), time(15, 59), tzinfo=EASTERN
                    ).timestamp()
                    * 1_000_000_000
                ),
                "transactions": 50,
            }
        )
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "day.csv.gz"
            with gzip.open(path, "wt", encoding="utf-8", newline="") as target:
                writer = csv.DictWriter(target, fieldnames=CSV_FIELDS)
                writer.writeheader()
                writer.writerows(rows)
            parsed = _load_independent_source_day(path, day)

            self.assertTrue(parsed["AAA"]["opening_exact"])
            self.assertEqual(parsed["AAA"]["opening_volume"], 510)
            self.assertEqual(parsed["AAA"]["volume"], 1_510)

            rows[-1]["window_start"] = int(
                datetime.combine(
                    date.fromisoformat(day), time(15, 58), tzinfo=EASTERN
                ).timestamp()
                * 1_000_000_000
            )
            with gzip.open(path, "wt", encoding="utf-8", newline="") as target:
                writer = csv.DictWriter(target, fieldnames=CSV_FIELDS)
                writer.writeheader()
                writer.writerows(rows)
            with self.assertRaisesRegex(ScannerInspectionError, "residual"):
                _load_independent_source_day(path, day)

    def test_recomputes_denominators_disposition_rank_and_public_hash(self):
        manifest, summary, detail, rules, source_status = fixtures()

        result = inspect_payloads(
            manifest=manifest,
            summary=summary,
            detail=detail,
            rules=rules,
            source_status=source_status,
            independent_detail=detail,
            detail_sha256="d" * 64,
            summary_sha256="u" * 64,
        )

        self.assertTrue(result["valid"])
        self.assertEqual(result["total_evaluated"], 1)
        self.assertEqual(result["total_selected"], 1)
        self.assertTrue(result["invariants"]["denominators_recomputed"])

    def test_rejects_a_tampered_rvol_or_incomplete_source(self):
        manifest, summary, detail, rules, source_status = fixtures()
        tampered = copy.deepcopy(detail)
        tampered["dates"]["2026-03-03"]["evaluations"][0]["opening_relative_volume"] = (
            3.0
        )

        with self.assertRaisesRegex(
            ScannerInspectionError, "opening_relative_volume|RVOL"
        ):
            inspect_payloads(
                manifest=manifest,
                summary=summary,
                detail=tampered,
                rules=rules,
                source_status=source_status,
                independent_detail=detail,
                detail_sha256="d" * 64,
                summary_sha256="u" * 64,
            )

        source_status["complete"] = False
        with self.assertRaisesRegex(ScannerInspectionError, "incomplete"):
            inspect_payloads(
                manifest=manifest,
                summary=summary,
                detail=detail,
                rules=rules,
                source_status=source_status,
                independent_detail=detail,
                detail_sha256="d" * 64,
                summary_sha256="u" * 64,
            )

    def test_rejects_metric_that_only_raw_source_recomputation_can_detect(self):
        _, _, detail, _, _ = fixtures()
        tampered = copy.deepcopy(detail)
        tampered["dates"]["2026-03-03"]["evaluations"][0][
            "average_daily_volume_14"
        ] = 9_999_999.0

        with self.assertRaisesRegex(ScannerInspectionError, "average_daily_volume_14"):
            _compare_independent_detail(tampered, detail)


if __name__ == "__main__":
    unittest.main()

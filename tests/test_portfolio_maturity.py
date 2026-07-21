from __future__ import annotations

import hashlib
import json
import math
import tempfile
import unittest
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import portfolio_maturity as maturity
import portfolio_funnel as funnel


RULES_HASH = "a" * 64


class PortfolioMaturityTests(unittest.TestCase):
    def setUp(self):
        self.config = maturity.load_config()

    def _inspection(
        self,
        root: Path,
        strategy_id: str,
        family: str,
        *,
        variant_ordinal: int = 1,
        stage0_survived: bool = True,
    ):
        evidence = root / f"{strategy_id}.json"
        evidence.write_text('{"inspected":true}\n', encoding="utf-8")
        variant_id = f"{strategy_id}-stage0"
        result = {
            "schema_version": 1,
            "result_kind": "stage0-falsification",
            "variant_id": variant_id,
            "mechanism_family": family,
            "stage0_survived": stage0_survived,
            "stage0_blockers": [] if stage0_survived else ["failed frozen gate"],
            "maturity_effect": "NONE",
            "development_evidence_eligible": False,
            "confirmation_evidence_eligible": False,
        }
        result["result_sha256"] = funnel._self_hash(result, "result_sha256")
        result_path = root / f"{strategy_id}-stage0-result.json"
        result_path.write_text(json.dumps(result), encoding="utf-8")
        result_inspection = {
            "schema_version": 1,
            "inspection_kind": "stage0-result-inspection",
            "variant_id": variant_id,
            "result_sha256": result["result_sha256"],
            "result_file_sha256": funnel._file_hash(result_path),
            "stage0_survived": stage0_survived,
            "maturity_effect": "NONE",
            "valid": True,
        }
        result_inspection["inspection_sha256"] = funnel._self_hash(
            result_inspection, "inspection_sha256"
        )
        result_inspection_path = root / f"{strategy_id}-stage0-inspection.json"
        result_inspection_path.write_text(json.dumps(result_inspection), encoding="utf-8")
        evidence_hashes = {
            path.name: hashlib.sha256(path.read_bytes()).hexdigest()
            for path in (evidence, result_path, result_inspection_path)
        }
        return {
            "schema_version": 1,
            "record_type": "inspection",
            "inspection_id": f"{strategy_id}-final-inspection",
            "recorded_at": "2026-07-21T12:00:00+00:00",
            "strategy_id": strategy_id,
            "strategy_version": "v1",
            "mechanism_family": family,
            "rules_hash": RULES_HASH,
            "trial_count": 10,
            "tournament_wave": 1,
            "variant_ordinal": variant_ordinal,
            "trial_accounting_complete": True,
            "multiple_testing_clear": True,
            "execution_model_complete": True,
            "development_universe_representative": True,
            "confirmation_untouched": True,
            "confirmation_embargo_trading_days": 5,
            "source_stage0_variant_id": variant_id,
            "source_stage0_result_sha256": result["result_sha256"],
            "source_stage0_result_path": result_path.name,
            "source_stage0_result_inspection_path": result_inspection_path.name,
            "evidence_hashes": evidence_hashes,
        }

    def _strategy_records(
        self,
        root: Path,
        strategy_id: str,
        family: str,
        *,
        phase_offset: float = 0.0,
        live: bool = False,
        variant_ordinal: int = 1,
    ):
        records = [
            self._inspection(
                root,
                strategy_id,
                family,
                variant_ordinal=variant_ordinal,
            )
        ]
        development_start = date(2025, 1, 2)
        confirmation_start = date(2025, 4, 1)
        for index in range(50):
            confirmation = index >= 30
            day = (
                confirmation_start + timedelta(days=index - 30)
                if confirmation
                else development_start + timedelta(days=index)
            )
            value = (
                0.55 + 0.65 * math.sin((index - 30) * 1.7 + phase_offset)
                if confirmation
                else 0.55 + 0.60 * math.sin(index * 1.3 + phase_offset)
            )
            phase = "confirmation" if confirmation else "development"
            records.append(
                {
                    "schema_version": 1,
                    "record_type": "signal",
                    "recorded_at": datetime.now(UTC).isoformat(),
                    "strategy_id": strategy_id,
                    "strategy_version": "v1",
                    "mechanism_family": family,
                    "rules_hash": RULES_HASH,
                    "date": day.isoformat(),
                    "sample_phase": phase,
                    "mode": "historical",
                    "signal_id": f"{day.isoformat()}-{strategy_id}-signal",
                    "closed": True,
                    "eligible": True,
                    "net_r": value,
                    "stress_10bps_r": value - 0.10,
                    "stress_20bps_r": value - 0.20,
                    "stop_executed": value < 0,
                    "session_capture_complete": True,
                    "rule_violations": [],
                }
            )
            if confirmation:
                records.append(
                    {
                        "schema_version": 1,
                        "record_type": "session",
                        "recorded_at": datetime.now(UTC).isoformat(),
                        "strategy_id": strategy_id,
                        "strategy_version": "v1",
                        "mechanism_family": family,
                        "rules_hash": RULES_HASH,
                        "date": day.isoformat(),
                        "sample_phase": "confirmation",
                        "mode": "historical",
                        "session_id": f"{day.isoformat()}-{strategy_id}-session",
                        "eligible_signal": True,
                        "session_capture_complete": True,
                        "rule_violations": [],
                    }
                )
        for index in range(5):
            day = date(2025, 6, 2) + timedelta(days=index)
            records.append(
                {
                    "schema_version": 1,
                    "record_type": "signal",
                    "recorded_at": datetime.now(UTC).isoformat(),
                    "strategy_id": strategy_id,
                    "strategy_version": "v1",
                    "mechanism_family": family,
                    "rules_hash": RULES_HASH,
                    "date": day.isoformat(),
                    "sample_phase": "shadow",
                    "mode": "shadow",
                    "signal_id": f"{day.isoformat()}-{strategy_id}-shadow",
                    "closed": True,
                    "eligible": True,
                    "net_r": 0.3,
                    "stress_10bps_r": 0.2,
                    "stress_20bps_r": 0.1,
                    "stop_executed": False,
                    "discovery_complete": True,
                    "evaluation_complete": True,
                    "sizing_complete": True,
                    "order_construction_complete": True,
                    "protection_plan_complete": True,
                    "monitoring_complete": True,
                    "journal_complete": True,
                    "broker_actions": 0,
                    "session_capture_complete": True,
                    "rule_violations": [],
                }
            )
        if live:
            day = date(2025, 6, 16)
            records.append(
                {
                    "schema_version": 1,
                    "record_type": "signal",
                    "recorded_at": datetime.now(UTC).isoformat(),
                    "strategy_id": strategy_id,
                    "strategy_version": "v1",
                    "mechanism_family": family,
                    "rules_hash": RULES_HASH,
                    "date": day.isoformat(),
                    "sample_phase": "live",
                    "mode": "live",
                    "signal_id": f"{day.isoformat()}-{strategy_id}-live",
                    "closed": True,
                    "eligible": True,
                    "net_r": 0.2,
                    "stress_10bps_r": 0.1,
                    "stress_20bps_r": 0.05,
                    "stop_executed": False,
                    "portfolio_guard_status": "ENTRY_READY",
                    "broker_review_passed": True,
                    "broker_confirmation_required": False,
                    "broker_confirmation_satisfied": False,
                    "protection_confirmed": True,
                    "monitoring_complete": True,
                    "journal_complete": True,
                    "entry_slippage_bps": 5.0,
                    "unprotected_seconds": 4.0,
                    "session_capture_complete": True,
                    "rule_violations": [],
                }
            )
        return records

    def test_config_matches_authorized_portfolio_limits(self):
        portfolio = self.config.raw["portfolio"]
        self.assertEqual(portfolio["maximum_concurrent_positions"], 3)
        self.assertEqual(portfolio["maximum_new_entries_per_day"], 5)
        self.assertEqual(portfolio["maximum_holding_trading_days"], 5)

    def test_empty_report_refuses_milestone(self):
        report = maturity.build_report([], self.config)
        self.assertEqual(report["earned_milestone"], "RESEARCH")
        self.assertEqual(report["pilot_ready_strategy_count"], 0)
        self.assertEqual(report["earned_interim_milestones"], [])

    def test_ready_strategy_without_live_execution_does_not_earn_interim_milestone(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            records = self._strategy_records(root, "strategy-one", "momentum")
            report = maturity.build_report(records, self.config)
        self.assertEqual(report["pilot_ready_strategy_count"], 1)
        self.assertEqual(report["earned_interim_milestones"], [])

    def test_ready_strategy_with_closed_live_execution_earns_interim_milestone(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            records = self._strategy_records(
                root, "strategy-one", "momentum", live=True
            )
            report = maturity.build_report(records, self.config)
        self.assertEqual(
            report["earned_interim_milestones"],
            [maturity.FIRST_PILOT_MILESTONE],
        )
        self.assertEqual(report["pilot_ready_live_started_strategy_count"], 1)
        self.assertEqual(report["earned_milestone"], "RESEARCH")

    def test_strong_strategy_earns_pilot_ready(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            records = self._strategy_records(root, "strategy-one", "momentum")
            for record in records:
                maturity.validate_record(record, root=root)
            assessment = maturity.assess_strategy(records, self.config)
        self.assertEqual(assessment["maturity"], "PILOT_READY")
        self.assertEqual(assessment["pilot_ready_blockers"], [])

    def test_weak_confirmation_blocks_pilot_ready(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            records = self._strategy_records(root, "strategy-one", "momentum")
            for record in records:
                if record.get("sample_phase") == "confirmation" and record.get(
                    "record_type"
                ) == "signal":
                    record["net_r"] = -0.2
                    record["stress_10bps_r"] = -0.3
                    record["stress_20bps_r"] = -0.4
            assessment = maturity.assess_strategy(records, self.config)
        self.assertFalse(assessment["pilot_ready"])
        self.assertIn(
            "confirmation expectancy R is not above required 0.0",
            assessment["pilot_ready_blockers"],
        )

    def test_positive_but_fragile_confirmation_blocks_pilot_ready(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            records = self._strategy_records(root, "strategy-one", "momentum")
            confirmation_index = 0
            for record in records:
                if record.get("sample_phase") != "confirmation" or record.get(
                    "record_type"
                ) != "signal":
                    continue
                value = 1.0 if confirmation_index < 5 else -0.1
                record["net_r"] = value
                record["stress_10bps_r"] = value - 0.1
                record["stress_20bps_r"] = value - 0.2
                confirmation_index += 1
            assessment = maturity.assess_strategy(records, self.config)
        self.assertGreater(assessment["metrics"]["confirmation"]["expectancy_r"], 0)
        self.assertIn(
            "confirmation total R without five best is not above 0",
            assessment["pilot_ready_blockers"],
        )
        self.assertEqual(assessment["validation_phase"], "CONFIRMATION")

    def test_strong_confirmation_cannot_hide_weak_development(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            records = self._strategy_records(root, "strategy-one", "momentum")
            for record in records:
                if record.get("sample_phase") == "development" and record.get(
                    "record_type"
                ) == "signal":
                    record["net_r"] = -0.05
                    record["stress_10bps_r"] = -0.15
                    record["stress_20bps_r"] = -0.25
            assessment = maturity.assess_strategy(records, self.config)
        self.assertIn(
            "development expectancy R is not above required 0.0",
            assessment["pilot_ready_blockers"],
        )
        self.assertEqual(assessment["validation_phase"], "DEVELOPMENT")

    def test_missing_shadows_block_after_both_historical_phases_pass(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            records = [
                record
                for record in self._strategy_records(root, "strategy-one", "momentum")
                if record.get("sample_phase") != "shadow"
            ]
            assessment = maturity.assess_strategy(records, self.config)
        self.assertIn(
            "shadow executions 0 is below required 5",
            assessment["pilot_ready_blockers"],
        )
        self.assertEqual(assessment["validation_phase"], "SHADOW_QUALIFICATION")

    def test_incomplete_shadow_path_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            records = self._strategy_records(root, "strategy-one", "momentum")
            shadow = next(
                record for record in records if record.get("sample_phase") == "shadow"
            )
            shadow["monitoring_complete"] = False
            with self.assertRaisesRegex(
                maturity.PortfolioMaturityError,
                "complete shadow execution requires monitoring_complete=true",
            ):
                maturity.validate_record(shadow, root=root)

    def test_live_evidence_without_entry_ready_guard_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            records = self._strategy_records(
                root,
                "strategy-one",
                "momentum",
                live=True,
            )
            live = next(
                record for record in records if record.get("sample_phase") == "live"
            )
            live["portfolio_guard_status"] = "PAUSED_SAFETY"
            with self.assertRaisesRegex(
                maturity.PortfolioMaturityError,
                "portfolio_guard_status=ENTRY_READY",
            ):
                maturity.validate_record(live, root=root)

    def test_retired_stage0_variant_cannot_enter_maturity(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            inspection = self._inspection(
                root,
                "strategy-one",
                "momentum",
                stage0_survived=False,
            )
            with self.assertRaisesRegex(
                maturity.PortfolioMaturityError,
                "retired Stage 0 variants cannot enter maturity",
            ):
                maturity.validate_record(inspection, root=root)

    def test_rule_violation_or_incomplete_capture_blocks_pilot_ready(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            records = self._strategy_records(root, "strategy-one", "momentum")
            records[1]["rule_violations"] = ["lookahead"]
            records[2]["session_capture_complete"] = False
            assessment = maturity.assess_strategy(records, self.config)
        self.assertIn("rule violations exceed zero", assessment["pilot_ready_blockers"])
        self.assertIn(
            "evidence contains incomplete capture records",
            assessment["pilot_ready_blockers"],
        )

    def test_inspection_hash_drift_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            inspection = self._inspection(root, "strategy-one", "momentum")
            (root / "strategy-one.json").write_text("changed\n", encoding="utf-8")
            with self.assertRaisesRegex(maturity.PortfolioMaturityError, "drifted"):
                maturity.validate_record(inspection, root=root)

    def test_duplicate_tournament_variant_identity_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            left = self._inspection(root, "strategy-one", "momentum")
            right = self._inspection(root, "strategy-two", "reversal")
            ledger = root / "portfolio.jsonl"
            ledger.write_text(
                json.dumps(left) + "\n" + json.dumps(right) + "\n",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(
                maturity.PortfolioMaturityError,
                "duplicate tournament variant identity",
            ):
                maturity.read_records(ledger, root=root)

    def test_same_family_cannot_form_portfolio(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            records = []
            for index, phase in enumerate((0.0, 2.1, 4.2), 1):
                records.extend(
                    self._strategy_records(
                        root,
                        f"strategy-{index}",
                        "same-family",
                        phase_offset=phase,
                        live=True,
                        variant_ordinal=index,
                    )
                )
            report = maturity.build_report(records, self.config)
        self.assertEqual(report["pilot_ready_strategy_count"], 3)
        self.assertEqual(report["earned_milestone"], "RESEARCH")

    def test_distinct_low_correlation_live_started_strategies_earn_milestone(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            records = []
            for index, phase in enumerate((0.0, 2.1, 4.2), 1):
                records.extend(
                    self._strategy_records(
                        root,
                        f"strategy-{index}",
                        f"family-{index}",
                        phase_offset=phase,
                        live=True,
                        variant_ordinal=index,
                    )
                )
            report = maturity.build_report(records, self.config)
        self.assertEqual(report["earned_milestone"], "THREE_PILOT_READY_LIVE_STARTED")
        self.assertEqual(report["live_started_selected_strategy_count"], 3)
        self.assertGreaterEqual(report["selected_confirmation_opportunity_coverage"], 0.60)

    def test_live_must_start_for_every_selected_strategy(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            records = []
            for index, phase in enumerate((0.0, 2.1, 4.2), 1):
                records.extend(
                    self._strategy_records(
                        root,
                        f"strategy-{index}",
                        f"family-{index}",
                        phase_offset=phase,
                        live=index < 3,
                        variant_ordinal=index,
                    )
                )
            report = maturity.build_report(records, self.config)
        self.assertEqual(report["earned_milestone"], "RESEARCH")
        self.assertIn(
            "selected strategies with a started live pilot 2 is below required 3",
            report["milestone_blockers"],
        )


if __name__ == "__main__":
    unittest.main()

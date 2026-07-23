from __future__ import annotations

import hashlib
import json
import math
import tempfile
import unittest
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from unittest.mock import patch

import portfolio_maturity as maturity
import portfolio_funnel as funnel


RULES_HASH = "a" * 64
CAMPAIGN_ID = maturity.V2_CAMPAIGN_ID


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
        if stage0_survived:
            confirmation = {
                "schema_version": 1,
                "artifact_kind": "confirmation-inspection",
                "campaign_id": CAMPAIGN_ID,
                "family_id": family,
                "strategy_id": strategy_id,
                "strategy_version": "v1",
                "rules_hash": RULES_HASH,
                "state": "CONFIRMATION_PASSED",
                "inspection": {"passed": True},
                "broker_actions_permitted": False,
            }
            confirmation["artifact_sha256"] = hashlib.sha256(
                json.dumps(
                    confirmation,
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode()
            ).hexdigest()
            confirmation_path = root / (
                f"{strategy_id}-confirmation-{confirmation['artifact_sha256']}.json"
            )
            confirmation_path.write_text(
                json.dumps(confirmation, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            evidence_hashes = {
                path.name: hashlib.sha256(path.read_bytes()).hexdigest()
                for path in (evidence, confirmation_path)
            }
            return {
                "schema_version": 2,
                "research_campaign_id": CAMPAIGN_ID,
                "record_type": "inspection",
                "inspection_id": f"{strategy_id}-final-inspection",
                "recorded_at": "2026-07-21T12:00:00+00:00",
                "strategy_id": strategy_id,
                "strategy_version": "v1",
                "mechanism_family": family,
                "rules_hash": RULES_HASH,
                "trial_count": 10,
                "selection_mode": "development_search",
                "power_target": 50,
                "required_total_signals": 50,
                "required_confirmation_signals": 20,
                "evidence_counts_frozen_before_confirmation": True,
                "trial_accounting_complete": True,
                "multiple_testing_clear": True,
                "execution_model_complete": True,
                "development_universe_representative": True,
                "confirmation_untouched": True,
                "confirmation_embargo_trading_days": 5,
                "discovery_confirmation_inspection_path": confirmation_path.name,
                "discovery_confirmation_inspection_sha256": confirmation[
                    "artifact_sha256"
                ],
                "evidence_hashes": evidence_hashes,
            }
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
                    "schema_version": 2,
                    "research_campaign_id": CAMPAIGN_ID,
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
                    "net_account_return_fraction": value * 0.005,
                    "stress_10bps_account_return_fraction": (value - 0.10) * 0.005,
                    "stress_20bps_account_return_fraction": (value - 0.20) * 0.005,
                    "net_pnl_dollars": value * 500.0,
                    "stress_10bps_net_pnl_dollars": (value - 0.10) * 500.0,
                    "stress_20bps_net_pnl_dollars": (value - 0.20) * 500.0,
                    "stop_executed": value < 0,
                    "session_capture_complete": True,
                    "rule_violations": [],
                }
            )
            records.append(
                {
                    "schema_version": 2,
                    "research_campaign_id": CAMPAIGN_ID,
                    "record_type": "session",
                    "recorded_at": datetime.now(UTC).isoformat(),
                    "strategy_id": strategy_id,
                    "strategy_version": "v1",
                    "mechanism_family": family,
                    "rules_hash": RULES_HASH,
                    "date": day.isoformat(),
                    "sample_phase": phase,
                    "mode": "historical",
                    "session_id": f"{day.isoformat()}-{strategy_id}-session",
                    "eligible_signal": True,
                    "session_outcome": "filled",
                    "daily_account_return_fraction": value * 0.005,
                    "stress_10bps_daily_account_return_fraction": (value - 0.10)
                    * 0.005,
                    "stress_20bps_daily_account_return_fraction": (value - 0.20)
                    * 0.005,
                    "session_capture_complete": True,
                    "rule_violations": [],
                }
            )
        for index in range(5):
            day = date(2025, 6, 2) + timedelta(days=index)
            records.append(
                {
                    "schema_version": 2,
                    "research_campaign_id": CAMPAIGN_ID,
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
                    "schema_version": 2,
                    "research_campaign_id": CAMPAIGN_ID,
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
                    "position_flat_confirmed": True,
                    "residual_orders_terminal": True,
                    "account_reconciled_after_close": True,
                    "encrypted_identifiers_recorded": True,
                    "notification_status_recorded": True,
                    "entry_slippage_bps": 5.0,
                    "exit_slippage_bps": 4.0,
                    "unprotected_seconds": 4.0,
                    "realized_net_dollars": 200.0,
                    "net_account_return_fraction": 0.002,
                    "exit_reason": "strategy_exit",
                    "broker_actions": 3,
                    "session_capture_complete": True,
                    "rule_violations": [],
                }
            )
        return records

    def test_config_matches_authorized_portfolio_limits(self):
        portfolio = self.config.raw["portfolio"]
        campaign = self.config.raw["campaign"]
        self.assertEqual(campaign["schema_version"], 2)
        self.assertEqual(
            campaign["active_research_campaign_id"], maturity.V2_CAMPAIGN_ID
        )
        self.assertEqual(campaign["first_pilot_ready_target"], 1)
        self.assertEqual(campaign["portfolio_target"], 3)
        self.assertEqual(portfolio["maximum_concurrent_positions"], 3)
        self.assertEqual(portfolio["maximum_new_entries_per_day"], 5)
        self.assertEqual(portfolio["maximum_holding_trading_days"], 5)

    def test_schema_one_config_remains_loadable_and_v1_evidence_is_adverse_only(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "portfolio-v1.toml"
            current = maturity.DEFAULT_CONFIG_PATH.read_text(encoding="utf-8")
            campaign_end = current.index("\n[portfolio]")
            legacy_campaign = """[campaign]
id = "multi-strategy-portfolio-validation-v1"
schema_version = 1
target_pilot_ready_strategies = 3
initial_mechanism_families = 10
maximum_initial_variants = 20
maximum_second_wave_families = 6
"""
            path.write_text(
                legacy_campaign + current[campaign_end + 1 :], encoding="utf-8"
            )
            legacy = maturity.load_config(path)
            self.assertEqual(legacy.schema_version, 1)
            self.assertEqual(legacy.portfolio_target, 3)

            root = Path(directory)
            records = self._strategy_records(root, "strategy-one", "momentum")
            for record in records:
                record["schema_version"] = 1
                record.pop("research_campaign_id", None)
            report = maturity.build_report(records, self.config)
            self.assertEqual(report["pilot_ready_strategy_count"], 0)
            self.assertEqual(report["preserved_adverse_record_count"], len(records))

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

    def test_positive_r_cannot_hide_negative_account_growth(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            records = self._strategy_records(root, "strategy-one", "momentum")
            for record in records:
                if record.get("record_type") == "session":
                    record["daily_account_return_fraction"] = -0.001
                    record["stress_10bps_daily_account_return_fraction"] = -0.002
                    record["stress_20bps_daily_account_return_fraction"] = -0.003
                elif record.get("record_type") == "signal" and record.get(
                    "mode"
                ) == "historical":
                    record["net_account_return_fraction"] = -0.001
                    record["stress_10bps_account_return_fraction"] = -0.002
                    record["stress_20bps_account_return_fraction"] = -0.003
                    record["net_pnl_dollars"] = -100.0
                    record["stress_10bps_net_pnl_dollars"] = -200.0
                    record["stress_20bps_net_pnl_dollars"] = -300.0
            assessment = maturity.assess_strategy(records, self.config)
        self.assertIn(
            "combined historical total log growth is not above 0",
            assessment["pilot_ready_blockers"],
        )
        self.assertFalse(assessment["pilot_ready"])

    def test_account_chronological_halves_use_daily_path_not_trade_subset(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            records = self._strategy_records(root, "strategy-one", "momentum")
            historical = [
                record
                for record in records
                if record.get("sample_phase") in {"development", "confirmation"}
            ]
            metrics = maturity._account_growth_metrics(historical, 0.90)
        daily = [
            float(record["daily_account_return_fraction"])
            for record in sorted(
                (
                    record
                    for record in historical
                    if record.get("record_type") == "session"
                ),
                key=lambda item: (item["date"], item["session_id"]),
            )
        ]
        midpoint = len(daily) // 2
        self.assertAlmostEqual(
            metrics["primary_5bps"]["first_half_log_growth"],
            sum(math.log1p(value) for value in daily[:midpoint]),
        )

    def test_dynamic_power_target_is_frozen_and_authoritative(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            records = self._strategy_records(root, "strategy-one", "momentum")
            inspection = next(
                record for record in records if record["record_type"] == "inspection"
            )
            inspection["power_target"] = 70
            inspection["required_total_signals"] = 70
            inspection["required_confirmation_signals"] = 21
            assessment = maturity.assess_strategy(records, self.config)
        self.assertIn(
            "historical signals 50 is below required 70",
            assessment["pilot_ready_blockers"],
        )

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

    def test_inspected_failed_development_is_terminal_and_blocks_later_phases(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            records = [
                record
                for record in self._strategy_records(root, "strategy-one", "momentum")
                if record.get("sample_phase") not in {"confirmation", "shadow", "live"}
            ]
            inspection = next(
                record for record in records if record["record_type"] == "inspection"
            )
            inspection["retired_after_development"] = True
            for record in records:
                if record.get("record_type") == "signal":
                    record["net_r"] = -0.05
                    record["stress_10bps_r"] = -0.15
                    record["stress_20bps_r"] = -0.25
            assessment = maturity.assess_strategy(records, self.config)
            self.assertTrue(assessment["retired_after_development"])
            self.assertEqual(assessment["validation_phase"], "RETIRED_DEVELOPMENT")
            records.append(
                {
                    **next(
                        record
                        for record in self._strategy_records(
                            root, "strategy-one", "momentum"
                        )
                        if record.get("sample_phase") == "confirmation"
                    ),
                    "signal_id": "2025-04-01-strategy-one-forbidden-confirmation",
                }
            )
            with self.assertRaisesRegex(
                maturity.PortfolioMaturityError,
                "cannot contain later-phase evidence",
            ):
                maturity.assess_strategy(records, self.config)

    def test_passing_development_cannot_be_marked_retired(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            records = [
                record
                for record in self._strategy_records(root, "strategy-one", "momentum")
                if record.get("sample_phase") not in {"confirmation", "shadow", "live"}
            ]
            inspection = next(
                record for record in records if record["record_type"] == "inspection"
            )
            inspection["retired_after_development"] = True
            with self.assertRaisesRegex(
                maturity.PortfolioMaturityError,
                "requires a failed development gate",
            ):
                maturity.assess_strategy(records, self.config)

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

    def test_shadow_violation_resets_then_five_new_clean_closes_requalify(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            records = self._strategy_records(root, "strategy-one", "momentum")
            shadows = [
                record
                for record in records
                if record.get("sample_phase") == "shadow"
            ]
            shadows[0]["eligible"] = False
            shadows[0]["rule_violations"] = ["shadow capture violation"]
            reset = maturity.assess_strategy(records, self.config)
            self.assertEqual(reset["metrics"]["shadow_executions"], 4)
            self.assertEqual(reset["metrics"]["shadow_qualification_resets"], 1)
            self.assertFalse(reset["pilot_ready"])

            replacement = dict(shadows[-1])
            replacement["date"] = "2025-06-07"
            replacement["signal_id"] = "2025-06-07-strategy-one-shadow"
            records.append(replacement)
            requalified = maturity.assess_strategy(records, self.config)
            self.assertEqual(requalified["metrics"]["shadow_executions"], 5)
            self.assertEqual(requalified["maturity"], "PILOT_READY")

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

    def test_live_close_without_flat_terminal_reconciliation_is_rejected(self):
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
            live["residual_orders_terminal"] = False
            with self.assertRaisesRegex(
                maturity.PortfolioMaturityError,
                "residual_orders_terminal=true",
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

    def test_schema_two_reports_but_does_not_gate_on_independent_r_bootstrap(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            records = self._strategy_records(root, "strategy-one", "momentum")
            with patch.object(maturity, "_bootstrap_lower", return_value=-1.0):
                assessment = maturity.assess_strategy(records, self.config)

        self.assertEqual(assessment["maturity"], "PILOT_READY")
        self.assertEqual(assessment["metrics"]["bootstrap_lower_expectancy_r"], -1.0)
        self.assertFalse(
            any(
                "bootstrap lower expectancy R" in blocker
                for blocker in assessment["pilot_ready_blockers"]
            )
        )
        with patch.object(maturity, "_bootstrap_lower", return_value=-1.0):
            legacy_metrics = maturity._metrics(records, 0.90).development
        legacy_blockers = maturity._robustness_blockers(
            "legacy development",
            legacy_metrics,
            minimum_signals=30,
            expectancy_threshold=0.0,
            gate=self.config.raw["pilot_ready"],
        )
        self.assertTrue(
            any(
                "bootstrap lower expectancy R" in blocker
                for blocker in legacy_blockers
            )
        )

    def test_inspection_hash_drift_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            inspection = self._inspection(root, "strategy-one", "momentum")
            (root / "strategy-one.json").write_text("changed\n", encoding="utf-8")
            with self.assertRaisesRegex(maturity.PortfolioMaturityError, "drifted"):
                maturity.validate_record(inspection, root=root)

    def test_schema_two_discovery_inspections_do_not_consume_legacy_ordinals(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            left = self._inspection(root, "strategy-one", "momentum")
            right = self._inspection(root, "strategy-two", "reversal")
            ledger = root / "portfolio.jsonl"
            ledger.write_text(
                json.dumps(left) + "\n" + json.dumps(right) + "\n",
                encoding="utf-8",
            )
            records = maturity.read_records(ledger, root=root)
        self.assertEqual(len(records), 2)
        self.assertTrue(all("tournament_wave" not in item for item in records))

    def test_record_batch_is_atomic_and_idempotent(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            ledger = root / "portfolio.jsonl"
            records = self._strategy_records(
                root, "strategy-one", "momentum"
            )[:4]
            first = maturity.append_records(
                records[:2], ledger, root=root, idempotent=True
            )
            self.assertEqual(first["admitted"], 2)
            repeated = maturity.append_records(
                records[:2], ledger, root=root, idempotent=True
            )
            self.assertEqual(repeated["already_present"], 2)
            before = ledger.read_bytes()
            conflicting = dict(records[1])
            conflicting["net_r"] = float(conflicting["net_r"]) + 0.01
            with self.assertRaisesRegex(
                maturity.PortfolioMaturityError,
                "already exists with different evidence",
            ):
                maturity.append_records(
                    [records[2], conflicting],
                    ledger,
                    root=root,
                    idempotent=True,
                )
            self.assertEqual(ledger.read_bytes(), before)
            self.assertEqual(len(maturity.read_records(ledger, root=root)), 2)

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

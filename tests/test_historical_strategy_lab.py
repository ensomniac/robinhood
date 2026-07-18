import copy
import hashlib
import json
import tempfile
import unittest
from datetime import date, timedelta
from pathlib import Path

from historical_research import ExecutionConfig
from historical_research_strategies import CandidateContext, SignalDecision, parse_bars
from historical_strategy_lab import (
    HistoricalStrategyLabError,
    Observation,
    Policy,
    _bootstrap_mean,
    _expected_frozen_hashes,
    _load_verified_bundle,
    _safe_publish_prefix,
    _simulate_deployment,
    _status,
    evaluate_policy_day,
    freeze_confirmation_manifest,
    load_confirmation_manifest,
    normalize_rejection_reason,
    rejection_category,
    run_lab,
    run_confirmation,
)
from strategy_engine import evaluate_candidate
from tests.test_strategy_engine import qualifying_payload


def minute_bars(price=100.0):
    result = []
    minute_of_day = 9 * 60 + 30
    for _ in range(390):
        hour, minute = divmod(minute_of_day, 60)
        result.append(
            {
                "time_et": f"{hour:02d}:{minute:02d}:00",
                "open": price,
                "high": price + 0.1,
                "low": price - 0.1,
                "close": price,
                "volume": 1000,
                "interpolated": False,
            }
        )
        minute_of_day += 1
    return result


def orb_bars():
    bars = minute_bars()
    for index in range(5):
        bars[index].update(
            {
                "open": 100.0,
                "high": 100.2,
                "low": 99.8,
                "close": 100.01 + index * 0.01,
            }
        )
    bars[5].update(
        {
            "open": 100.1,
            "high": 100.4,
            "low": 100.05,
            "close": 100.3,
            "volume": 2000,
        }
    )
    bars[6].update(
        {
            "open": 100.21,
            "high": 100.3,
            "low": 100.15,
            "close": 100.2,
        }
    )
    return bars


def reversal_bars():
    bars = minute_bars(100.0)
    for index in range(5):
        bars[index].update(
            {
                "open": 100.0,
                "high": 100.1,
                "low": 99.7,
                "close": 99.8,
                "volume": 10_000,
            }
        )
    bars[5].update(
        {
            "open": 99.8,
            "high": 100.1,
            "low": 99.75,
            "close": 100.05,
            "volume": 10_000,
        }
    )
    bars[6].update({"open": 100.06, "high": 101.0, "low": 100.0, "close": 100.9})
    return bars


def observation(
    symbol,
    *,
    signal_index=5,
    strength=1.0,
    technical_stop=99.5,
    earnings=True,
):
    payload = copy.deepcopy(qualifying_payload())
    payload["candidate"]["symbol"] = symbol
    payload["session"]["time_et"] = "09:37:00"
    context = CandidateContext(
        "2026-01-02",
        symbol,
        f"2026-01-02-{symbol}-1",
        parse_bars(minute_bars()),
    )
    return Observation(
        date="2026-01-02",
        symbol=symbol,
        signal_id=f"2026-01-02-{symbol}-1",
        strategy_id="orb-5m-research",
        context=context,
        decision=SignalDecision(
            signal_index,
            technical_stop,
            strength,
            "test_signal",
        ),
        no_signal_reason="signal",
        production=evaluate_candidate(payload),
        earnings_2_02=earnings,
        production_evaluation_time_et="09:37:00",
    )


def confirmation_candidate(symbol="TEST"):
    return {
        "symbol": symbol,
        "catalyst": {
            "point_in_time": True,
            "published_at": "2026-07-17T08:00:00-04:00",
            "source_url": f"https://www.sec.gov/{symbol}",
        },
        "discovery": {"filing_items": "2.02,9.01"},
    }


def confirmation_evidence(days, symbols=("TEST",)):
    return {
        "schema_version": 1,
        "prepared_at": "2026-07-18T10:00:00+00:00",
        "selection_seed": 7,
        "scanner": {
            "point_in_time": True,
            "universe_capture_complete": True,
            "target_session_prices_observed": False,
        },
        "candidates_by_date": {
            day: [confirmation_candidate(symbol) for symbol in symbols] for day in days
        },
    }


def consecutive_days(start, count):
    first = date.fromisoformat(start)
    return [(first + timedelta(days=index)).isoformat() for index in range(count)]


def freeze_fixture(root, *, symbols=("TEST",)):
    old_days = consecutive_days("2025-01-01", 100)
    old_evidence = root / "old-evidence.json"
    old_evidence.write_text(
        json.dumps(confirmation_evidence(old_days, symbols=("OLD",))),
        encoding="utf-8",
    )
    old_result = root / "old-result.json"
    old_result.write_text(
        json.dumps(
            {
                "manifest": {
                    "run_id": "strategy-lab-651f20ff135e-b268f18ec755",
                    "dataset": {
                        "requested_dates": 100,
                        "dataset_hash": "a" * 64,
                        "evidence_manifest": str(old_evidence),
                    },
                }
            }
        ),
        encoding="utf-8",
    )
    days = consecutive_days("2027-01-01", 100)
    evidence = root / "new-evidence.json"
    evidence.write_text(
        json.dumps(confirmation_evidence(days, symbols=symbols)),
        encoding="utf-8",
    )
    path, manifest = freeze_confirmation_manifest(
        evidence,
        excluded_result_paths=(old_result,),
        output_root=root / "manifests",
        registered_at="2026-07-18T12:00:00+00:00",
    )
    return days, path, manifest


def write_confirmation_bundle(
    data_root,
    day,
    manifest,
    *,
    symbols=("TEST",),
    captured_at=None,
):
    frozen = next(value for value in manifest["frozen_dates"] if value["date"] == day)
    candidates = []
    for symbol in symbols:
        payload = copy.deepcopy(qualifying_payload())
        payload["candidate"]["symbol"] = symbol
        payload["session"]["time_et"] = "09:36:00"
        candidates.append(
            {
                "symbol": symbol,
                "signal_id": f"{day}-{symbol}-1",
                "bars": reversal_bars(),
                "evaluation_payload": payload,
                "evaluation_time_et": "09:36:00",
                "discovery": {"filing_items": "2.02,9.01"},
            }
        )
    bundle = {
        "date": day,
        "schema_version": 2,
        "sample_phase": "confirmation",
        "preregistration": {
            "registered_at": manifest["registered_at"],
            "manifest_hash": manifest["manifest_sha256"],
        },
        "session_capture_complete": True,
        "source": {
            "point_in_time": True,
            "regular_hours_only": True,
            "split_adjusted": True,
            "universe_capture_complete": True,
            "frozen_evidence_sha256": frozen["frozen_evidence_sha256"],
            "captured_at": captured_at or f"{day}T21:00:00+00:00",
        },
        "candidates": candidates,
    }
    data_root.mkdir(parents=True, exist_ok=True)
    path = data_root / f"{day}.json"
    path.write_text(json.dumps(bundle), encoding="utf-8")
    return path


class GateTaxonomyTests(unittest.TestCase):
    def test_dynamic_reasons_are_normalized_and_categorized(self):
        self.assertEqual(
            normalize_rejection_reason("score 74 is below the 90-point maturity gate"),
            "score below maturity gate",
        )
        self.assertEqual(
            normalize_rejection_reason("snapshot 2 is stale"),
            "quote snapshot is stale",
        )
        self.assertEqual(
            rejection_category(
                "entry would chase too far above the opening-range high"
            ),
            "execution",
        )
        self.assertEqual(
            rejection_category("planned stop is inside ordinary noise"),
            "risk_and_exit",
        )


class PolicyTests(unittest.TestCase):
    def test_early_cutoff_filters_late_signals(self):
        policy = Policy(
            "early",
            "orb-5m-research",
            "test early cutoff",
            signal_cutoff_et="09:40:00",
        )

        result = evaluate_policy_day(
            [observation("EARLY"), observation("LATE", signal_index=20)],
            policy,
            ExecutionConfig(entry_slippage_bps=0, exit_slippage_bps=0),
        )

        self.assertEqual(result["trade"]["symbol"], "EARLY")
        self.assertEqual(result["filtered"]["signal_at_or_after_cutoff"], 1)

    def test_one_trade_policy_ranks_simultaneous_signals(self):
        policy = Policy("rank", "orb-5m-research", "test strength ranking")

        result = evaluate_policy_day(
            [
                observation("WEAK", strength=1.0),
                observation("STRONG", strength=5.0),
            ],
            policy,
            ExecutionConfig(entry_slippage_bps=0, exit_slippage_bps=0),
        )

        self.assertEqual(result["eligible_counterfactuals"], 2)
        self.assertEqual(result["trade"]["symbol"], "STRONG")

    def test_frozen_stop_policy_rejects_wide_counterfactual(self):
        policy = Policy(
            "tight",
            "orb-5m-research",
            "test production stop cap",
            maximum_stop_fraction=0.008,
        )

        result = evaluate_policy_day(
            [observation("WIDE", technical_stop=99.0)],
            policy,
            ExecutionConfig(entry_slippage_bps=0, exit_slippage_bps=0),
        )

        self.assertIsNone(result["trade"])
        self.assertEqual(result["filtered"]["stop_exceeds_policy_maximum"], 1)

    def test_bootstrap_is_deterministic(self):
        first = _bootstrap_mean([1.0, -1.0, 2.0], seed=7, samples=500)
        second = _bootstrap_mean([1.0, -1.0, 2.0], seed=7, samples=500)

        self.assertEqual(first, second)


class EvidenceBoundaryTests(unittest.TestCase):
    def test_bundle_requires_exact_ordered_symbols(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "2026-01-02.json"
            bundle = {
                "date": "2026-01-02",
                "session_capture_complete": True,
                "source": {
                    "point_in_time": True,
                    "regular_hours_only": True,
                    "split_adjusted": True,
                    "universe_capture_complete": True,
                    "frozen_evidence_sha256": "a" * 64,
                },
                "candidates": [{"symbol": "AAA"}, {"symbol": "BBB"}],
            }
            path.write_text(json.dumps(bundle), encoding="utf-8")
            item = {
                "date": "2026-01-02",
                "status": "available",
                "path": str(path),
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                "expected_symbols": ["BBB", "AAA"],
            }

            with self.assertRaisesRegex(
                HistoricalStrategyLabError, "ordered candidate universe"
            ):
                _load_verified_bundle(item)

    def test_bundle_requires_exact_frozen_evidence_identity(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            evidence_path = root / "evidence.json"
            evidence_path.write_text(
                json.dumps(
                    {
                        "scanner": {"point_in_time": True},
                        "candidates_by_date": {
                            "2026-01-02": [{"symbol": "AAA", "rank": 1}]
                        },
                    }
                ),
                encoding="utf-8",
            )
            path = root / "2026-01-02.json"
            bundle = {
                "date": "2026-01-02",
                "session_capture_complete": True,
                "source": {
                    "point_in_time": True,
                    "regular_hours_only": True,
                    "split_adjusted": True,
                    "universe_capture_complete": True,
                    "frozen_evidence_sha256": "f" * 64,
                },
                "candidates": [{"symbol": "AAA"}],
            }
            path.write_text(json.dumps(bundle), encoding="utf-8")
            item = {
                "date": "2026-01-02",
                "status": "available",
                "path": str(path),
                "sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                "expected_symbols": ["AAA"],
            }

            with self.assertRaisesRegex(
                HistoricalStrategyLabError, "different frozen evidence"
            ):
                _load_verified_bundle(
                    item,
                    expected_frozen_hash=_expected_frozen_hashes(evidence_path)[
                        "2026-01-02"
                    ],
                )

    def test_unsafe_output_path_is_refused(self):
        with self.assertRaisesRegex(HistoricalStrategyLabError, "production artifact"):
            _safe_publish_prefix(Path("trades/research-result"))

    def test_minimal_run_is_evidence_bound_and_research_only(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            data_root = root / "data"
            data_root.mkdir()
            evidence_path = root / "evidence.json"
            evidence_path.write_text(
                json.dumps(
                    {
                        "scanner": {
                            "point_in_time": True,
                            "universe_capture_complete": True,
                        },
                        "candidates_by_date": {"2026-01-02": [{"symbol": "TEST"}]},
                    }
                ),
                encoding="utf-8",
            )
            frozen_hash = _expected_frozen_hashes(evidence_path)["2026-01-02"]
            payload = copy.deepcopy(qualifying_payload())
            payload["candidate"]["symbol"] = "TEST"
            payload["session"]["time_et"] = "09:37:00"
            bundle = {
                "date": "2026-01-02",
                "schema_version": 2,
                "session_capture_complete": True,
                "source": {
                    "point_in_time": True,
                    "regular_hours_only": True,
                    "split_adjusted": True,
                    "universe_capture_complete": True,
                    "frozen_evidence_sha256": frozen_hash,
                },
                "candidates": [
                    {
                        "symbol": "TEST",
                        "signal_id": "2026-01-02-TEST-1",
                        "bars": orb_bars(),
                        "evaluation_payload": payload,
                        "evaluation_time_et": "09:37:00",
                        "discovery": {"filing_items": "2.02,9.01"},
                    }
                ],
            }
            (data_root / "2026-01-02.json").write_text(
                json.dumps(bundle), encoding="utf-8"
            )
            policy = Policy(
                "minimal-orb",
                "orb-5m-research",
                "minimal deterministic fixture",
            )

            result = run_lab(
                evidence_path,
                data_root=data_root,
                output_root=root / "runs",
                policies=[policy],
                slippages=[5],
                targets=[2],
                bootstrap_samples=100,
                publish_prefix=root / "published",
            )

            self.assertEqual(result["manifest"]["provider_requests"], 0)
            self.assertFalse(result["manifest"]["automatic_strategy_application"])
            self.assertTrue(
                result["manifest"]["production_isolation"]["verified_unchanged"]
            )
            self.assertEqual(result["policy_summaries"][0]["trades"], 1)
            self.assertTrue((root / "published.json").is_file())
            self.assertTrue((root / "published.md").is_file())


class IndependentConfirmationTests(unittest.TestCase):
    def test_complete_universe_may_include_non_2_02_candidates(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            days = consecutive_days("2027-01-01", 100)
            evidence_value = confirmation_evidence(days)
            evidence_value["candidates_by_date"][days[0]][0]["discovery"][
                "filing_items"
            ] = "8.01,9.01"
            evidence = root / "new-evidence.json"
            evidence.write_text(json.dumps(evidence_value), encoding="utf-8")
            old_evidence = root / "old-evidence.json"
            old_evidence.write_text(
                json.dumps(
                    confirmation_evidence(
                        consecutive_days("2025-01-01", 100), symbols=("OLD",)
                    )
                ),
                encoding="utf-8",
            )
            old_result = root / "old-result.json"
            old_result.write_text(
                json.dumps(
                    {
                        "manifest": {
                            "run_id": "strategy-lab-651f20ff135e-b268f18ec755",
                            "dataset": {
                                "requested_dates": 100,
                                "dataset_hash": "a" * 64,
                                "evidence_manifest": str(old_evidence),
                            },
                        }
                    }
                ),
                encoding="utf-8",
            )

            _path, manifest = freeze_confirmation_manifest(
                evidence,
                excluded_result_paths=(old_result,),
                output_root=root / "manifests",
                registered_at="2026-07-18T12:00:00+00:00",
            )

        self.assertEqual(len(manifest["frozen_dates"]), 100)
        self.assertTrue(manifest["policy"]["require_earnings_2_02"])

    def test_freeze_retains_preflight_blocker_in_exact_100_date_sample(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            old_days = consecutive_days("2025-01-01", 100)
            old_evidence = root / "old-evidence.json"
            old_evidence.write_text(
                json.dumps(confirmation_evidence(old_days, symbols=("OLD",))),
                encoding="utf-8",
            )
            old_result = root / "old-result.json"
            old_result.write_text(
                json.dumps(
                    {
                        "manifest": {
                            "run_id": "strategy-lab-651f20ff135e-b268f18ec755",
                            "dataset": {
                                "requested_dates": 100,
                                "dataset_hash": "a" * 64,
                                "evidence_manifest": str(old_evidence),
                            },
                        }
                    }
                ),
                encoding="utf-8",
            )
            days = consecutive_days("2027-01-01", 100)
            blocked_day = days[-1]
            selection = root / "selection.json"
            selection.write_text(
                json.dumps({"seed": 73, "selected_dates": list(reversed(days))}),
                encoding="utf-8",
            )
            evidence_value = confirmation_evidence(days[:-1])
            evidence_value["selection_seed"] = 73
            evidence_value["parent_selection_file"] = str(selection)
            blocked_candidate = confirmation_candidate("BLOCK")
            evidence_value["blocked_candidates_by_date"] = {
                blocked_day: [blocked_candidate]
            }
            blocker_report = {
                "blocked": True,
                "blocked_reason": "preflight_exhausted:1_of_10_required",
                "accepted_symbols": ["BLOCK"],
                "examined_count": 80,
            }
            evidence_value["preflight"] = {
                "minimum_candidates": 10,
                "blocked_dates": [blocked_day],
                "dates": {blocked_day: blocker_report},
            }
            evidence = root / "evidence.json"
            evidence.write_text(json.dumps(evidence_value), encoding="utf-8")

            _path, manifest = freeze_confirmation_manifest(
                evidence,
                excluded_result_paths=(old_result,),
                output_root=root / "manifests",
                registered_at="2026-07-18T12:00:00+00:00",
            )

        self.assertEqual(len(manifest["frozen_dates"]), 100)
        frozen = manifest["frozen_dates"][-1]
        self.assertEqual(frozen["date"], blocked_day)
        self.assertEqual(frozen["status"], "precollection_blocked")
        self.assertEqual(frozen["ordered_symbols"], ["BLOCK"])
        self.assertEqual(
            frozen["precollection_blocker"]["reason_code"],
            "preflight_exhausted:1_of_10_required",
        )

    def test_freeze_rejects_overlap_with_inspected_run(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            old_days = consecutive_days("2025-01-01", 100)
            old_evidence = root / "old.json"
            old_evidence.write_text(
                json.dumps(confirmation_evidence(old_days, symbols=("OLD",))),
                encoding="utf-8",
            )
            old_result = root / "result.json"
            old_result.write_text(
                json.dumps(
                    {
                        "manifest": {
                            "run_id": "strategy-lab-651f20ff135e-b268f18ec755",
                            "dataset": {
                                "requested_dates": 100,
                                "dataset_hash": "a" * 64,
                                "evidence_manifest": str(old_evidence),
                            },
                        }
                    }
                ),
                encoding="utf-8",
            )

            with self.assertRaisesRegex(
                HistoricalStrategyLabError, "overlap previously inspected"
            ):
                freeze_confirmation_manifest(
                    old_evidence,
                    excluded_result_paths=(old_result,),
                    output_root=root / "manifests",
                    registered_at="2026-07-18T12:00:00+00:00",
                )

    def test_manifest_is_hash_addressed_and_mutation_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            _days, path, manifest = freeze_fixture(root)

            self.assertEqual(
                path.name, f"confirmation-{manifest['manifest_sha256']}.json"
            )
            self.assertEqual(load_confirmation_manifest(path), manifest)
            mutated = copy.deepcopy(manifest)
            mutated["registered_at"] = "2026-07-18T12:00:01+00:00"
            path.write_text(json.dumps(mutated), encoding="utf-8")

            with self.assertRaisesRegex(
                HistoricalStrategyLabError, "manifest was mutated"
            ):
                load_confirmation_manifest(path)

    def test_capture_before_preregistration_and_symbol_reordering_are_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            days, manifest_path, manifest = freeze_fixture(root)
            data_root = root / "data"
            write_confirmation_bundle(
                data_root,
                days[0],
                manifest,
                captured_at="2026-07-18T11:59:59+00:00",
            )

            with self.assertRaisesRegex(
                HistoricalStrategyLabError, "capture must follow preregistration"
            ):
                run_confirmation(
                    manifest_path,
                    data_root=data_root,
                    output_root=root / "runs",
                    bootstrap_samples=100,
                )

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            days, manifest_path, manifest = freeze_fixture(root, symbols=("AAA", "BBB"))
            data_root = root / "data"
            write_confirmation_bundle(
                data_root, days[0], manifest, symbols=("BBB", "AAA")
            )

            with self.assertRaisesRegex(
                HistoricalStrategyLabError, "ordered candidate universe"
            ):
                run_confirmation(
                    manifest_path,
                    data_root=data_root,
                    output_root=root / "runs",
                    bootstrap_samples=100,
                )

    def test_confirmation_executes_only_frozen_policy_and_is_deterministic(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            days, manifest_path, manifest = freeze_fixture(root)
            data_root = root / "data"
            write_confirmation_bundle(data_root, days[0], manifest)

            first = run_confirmation(
                manifest_path,
                data_root=data_root,
                output_root=root / "runs-1",
                bootstrap_samples=100,
            )
            second = run_confirmation(
                manifest_path,
                data_root=data_root,
                output_root=root / "runs-2",
                bootstrap_samples=100,
            )

            self.assertEqual(
                first["policy_result"]["policy"]["policy_id"],
                "reversal-early-earnings",
            )
            self.assertEqual(first["runtime"]["policy_count"], 1)
            self.assertEqual(first["policy_result"], second["policy_result"])
            self.assertEqual(first["acceptance"], second["acceptance"])
            self.assertEqual(
                first["decision"]["next_stage"], "stop_without_threshold_tuning"
            )
            self.assertTrue(
                first["manifest"]["production_isolation"]["verified_unchanged"]
            )

    def test_risk_sized_deployment_never_compresses_stop_or_forces_allocation(self):
        daily = []
        for index in range(6):
            daily.append(
                {
                    "date": f"2027-01-{index + 1:02d}",
                    "status": "trade",
                    "trade": {
                        "signal_id": f"2027-01-{index + 1:02d}-TEST-1",
                        "symbol": "TEST",
                        "entry_price": 100.0,
                        "technical_stop": 98.0,
                        "exit_price": 104.0,
                        "exit_reason": "target",
                    },
                }
            )

        deployment = _simulate_deployment(daily)

        self.assertEqual(deployment["stop_compressions"], 0)
        self.assertEqual(deployment["risk_cap_violations"], 0)
        self.assertTrue(
            all(
                value["deployed_stop"] == value["structural_stop"]
                for value in deployment["details"]
            )
        )
        self.assertTrue(
            all(value["allocation_fraction"] < 0.70 for value in deployment["details"])
        )
        self.assertTrue(
            all(
                value["planned_loss_fraction"] <= 0.0025
                for value in deployment["details"]
            )
        )


class ResearchGateTests(unittest.TestCase):
    @staticmethod
    def summary():
        return {
            "trades": 40,
            "mean_r": 0.2,
            "profit_factor": 1.5,
            "maximum_drawdown_r": 4.0,
            "bootstrap_trade_mean": {"lower_90_one_sided": 0.05},
            "chronological_phases": {
                "retrospective_validation": {"total_r": 2.0},
                "retrospective_holdout": {"total_r": 2.0},
            },
            "stop_geometry": {"at_or_below_0.8pct_fraction": 0.8},
            "policy": {"maximum_stop_fraction": None},
        }

    @staticmethod
    def sensitivity(twenty_bps_pf=1.3):
        rows = []
        for target in (1.0, 1.5, 2.0, 3.0):
            rows.append(
                {
                    "entry_slippage_bps": 5.0,
                    "exit_slippage_bps": 5.0,
                    "target_r": target,
                    "total_r": 3.0,
                    "profit_factor": 1.4,
                    "maximum_drawdown_r": 4.0,
                }
            )
        for slippage, profit_factor in ((10.0, 1.3), (20.0, twenty_bps_pf)):
            rows.append(
                {
                    "entry_slippage_bps": slippage,
                    "exit_slippage_bps": slippage,
                    "target_r": 2.0,
                    "total_r": 2.0,
                    "profit_factor": profit_factor,
                    "maximum_drawdown_r": 5.0,
                }
            )
        return rows

    def test_gate_requires_severe_cost_profit_factor(self):
        status, _ = _status(self.summary(), self.sensitivity(twenty_bps_pf=1.19))
        self.assertEqual(status, "inconclusive")

    def test_gate_accepts_only_complete_robustness_contract(self):
        status, _ = _status(self.summary(), self.sensitivity())
        self.assertEqual(status, "promising_for_independent_confirmation")


if __name__ == "__main__":
    unittest.main()

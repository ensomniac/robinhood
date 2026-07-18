import copy
import hashlib
import json
import tempfile
import unittest
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
    _status,
    evaluate_policy_day,
    normalize_rejection_reason,
    rejection_category,
    run_lab,
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

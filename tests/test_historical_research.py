import copy
import hashlib
import json
import tempfile
import unittest
from dataclasses import FrozenInstanceError
from pathlib import Path

from historical_research import (
    ExecutionConfig,
    _evaluate_strategy,
    _trade_from_decision,
    run_research,
)
from historical_research_strategies import (
    CandidateContext,
    DataRequirements,
    HighOfDayContinuation,
    OpeningRangeBreakout,
    OpeningReversal,
    ResearchStrategyError,
    SignalDecision,
    SignalSearch,
    VwapPullback,
    parse_bars,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]


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


def set_bar(bars, index, *, open_price, high, low, close, volume=1000):
    bars[index].update(
        {
            "open": open_price,
            "high": high,
            "low": low,
            "close": close,
            "volume": volume,
        }
    )


def orb_bars():
    bars = minute_bars()
    for index in range(5):
        set_bar(
            bars,
            index,
            open_price=100.0,
            high=100.2,
            low=99.8,
            close=100.05 + index * 0.01,
        )
    set_bar(
        bars,
        5,
        open_price=100.1,
        high=100.3,
        low=100.05,
        close=100.25,
        volume=2000,
    )
    set_bar(
        bars,
        6,
        open_price=100.21,
        high=100.25,
        low=100.0,
        close=100.1,
    )
    return bars


def context(raw_bars, symbol="TEST"):
    return CandidateContext(
        date="2026-01-02",
        symbol=symbol,
        signal_id=f"2026-01-02-{symbol}-1",
        bars=parse_bars(raw_bars),
    )


class DepthStrategy:
    strategy_id = "depth-required-test"
    version = "1.0.0"
    description = "Test plugin requiring unavailable historical depth."
    requirements = DataRequirements(historical_depth=True)

    def find_signal(self, candidate):
        return SignalSearch(None, "not_reached")


class BackdatedSignalStrategy:
    strategy_id = "backdated-signal-test"
    version = "1.0.0"
    description = "Test plugin that illegally backdates a decision."
    requirements = DataRequirements()

    def find_signal(self, candidate):
        if len(candidate.bars) >= 7:
            return SignalSearch(
                SignalDecision(5, 99.0, 1.0, "illegal_backdated_signal"),
                "signal",
            )
        return SignalSearch(None, "waiting")


class HistoricalResearchStrategyTests(unittest.TestCase):
    def test_context_and_bars_are_immutable(self):
        candidate = context(orb_bars())

        with self.assertRaises(FrozenInstanceError):
            candidate.symbol = "CHANGED"
        with self.assertRaises(FrozenInstanceError):
            candidate.bars[0].close = 0

    def test_orb_signal_uses_completed_bar_and_does_not_look_forward(self):
        original = orb_bars()
        future_changed = copy.deepcopy(original)
        for index in range(6, len(future_changed)):
            price = 150.0 + index / 100
            set_bar(
                future_changed,
                index,
                open_price=price,
                high=price + 1,
                low=price - 1,
                close=price,
                volume=999999,
            )

        first = OpeningRangeBreakout().find_signal(context(original)).decision
        second = OpeningRangeBreakout().find_signal(context(future_changed)).decision

        self.assertIsNotNone(first)
        self.assertEqual(first, second)
        self.assertEqual(first.signal_index, 5)

    def test_common_execution_enters_next_bar_and_is_stop_first(self):
        raw = orb_bars()
        candidate = context(raw)
        decision = SignalDecision(5, 99.5, 1.0, "test")
        # The entry minute contains both target and stop, so adverse order wins.
        set_bar(
            raw,
            6,
            open_price=100.2,
            high=102.0,
            low=99.0,
            close=100.5,
        )
        candidate = context(raw)

        trade = _trade_from_decision(
            candidate,
            decision,
            ExecutionConfig(entry_slippage_bps=0, exit_slippage_bps=0, target_r=2),
        )

        self.assertEqual(trade["entry_time_et"], "09:36:00")
        self.assertEqual(trade["entry_price"], 100.2)
        self.assertEqual(trade["exit_reason"], "stop_first_ambiguous_bar")
        self.assertEqual(trade["exit_price"], 99.5)

    def test_builtin_variants_find_their_declared_patterns(self):
        vwap = minute_bars()
        for index in range(10):
            set_bar(
                vwap,
                index,
                open_price=100.1,
                high=100.3,
                low=100.0,
                close=100.2,
            )
        set_bar(
            vwap,
            10,
            open_price=100.1,
            high=100.5,
            low=99.9,
            close=100.4,
        )

        hod = minute_bars()
        set_bar(
            hod,
            10,
            open_price=100.1,
            high=100.8,
            low=100.0,
            close=100.7,
            volume=2000,
        )

        reversal = minute_bars()
        for index in range(5):
            set_bar(
                reversal,
                index,
                open_price=100.5,
                high=100.6,
                low=99.4,
                close=99.5,
            )
        set_bar(
            reversal,
            5,
            open_price=99.8,
            high=100.5,
            low=99.7,
            close=100.4,
        )

        self.assertIsNotNone(VwapPullback().find_signal(context(vwap)).decision)
        self.assertIsNotNone(HighOfDayContinuation().find_signal(context(hod)).decision)
        self.assertIsNotNone(OpeningReversal().find_signal(context(reversal)).decision)

    def test_incomplete_or_interpolated_bars_are_rejected(self):
        with self.assertRaisesRegex(ResearchStrategyError, "390"):
            parse_bars(minute_bars()[:-1])
        bars = minute_bars()
        bars[42]["interpolated"] = True
        with self.assertRaisesRegex(ResearchStrategyError, "interpolated"):
            parse_bars(bars)

    def test_unavailable_declared_requirement_blocks_instead_of_approximating(self):
        result = _evaluate_strategy(
            "2026-01-02",
            [{"symbol": "TEST", "bars": minute_bars()}],
            "tests.test_historical_research:DepthStrategy",
            ExecutionConfig(),
        )

        self.assertEqual(result["coverage_status"], "blocked")
        self.assertIn("historical_depth", result["block_reason"])

    def test_runner_rejects_a_plugin_that_backdates_a_signal(self):
        result = _evaluate_strategy(
            "2026-01-02",
            [{"symbol": "TEST", "bars": minute_bars()}],
            "tests.test_historical_research:BackdatedSignalStrategy",
            ExecutionConfig(),
        )

        self.assertEqual(result["coverage_status"], "blocked")
        self.assertIn("final bar", result["block_reason"])


class HistoricalResearchRunnerTests(unittest.TestCase):
    def _write_dataset(self, root):
        evidence = root / "evidence.json"
        data = root / "data"
        data.mkdir()
        dates = ("2026-01-02", "2026-01-05")
        evidence.write_text(
            json.dumps(
                {"candidates_by_date": {day: [{"symbol": "TEST"}] for day in dates}}
            ),
            encoding="utf-8",
        )
        for day in dates:
            (data / f"{day}.json").write_text(
                json.dumps(
                    {
                        "schema_version": 2,
                        "date": day,
                        "session_capture_complete": True,
                        "source": {
                            "point_in_time": True,
                            "regular_hours_only": True,
                            "split_adjusted": True,
                            "universe_capture_complete": True,
                        },
                        "candidates": [
                            {
                                "symbol": "TEST",
                                "signal_id": f"{day}-TEST-1",
                                "bars": orb_bars(),
                            }
                        ],
                    }
                ),
                encoding="utf-8",
            )
        return evidence, data

    def test_worker_counts_are_deterministic_and_production_is_untouched(self):
        protected = [PROJECT_ROOT / "SIGNALS.jsonl", PROJECT_ROOT / "TRADES.md"]
        before = {
            str(path): hashlib.sha256(path.read_bytes()).hexdigest()
            for path in protected
        }
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            evidence, data = self._write_dataset(root)
            serial = run_research(
                evidence_path=evidence,
                data_root=data,
                output_root=root / "serial",
                strategy_specs=("orb-5m-research", "hod-continuation"),
                workers=1,
            )
            parallel = run_research(
                evidence_path=evidence,
                data_root=data,
                output_root=root / "parallel",
                strategy_specs=("orb-5m-research", "hod-continuation"),
                workers=2,
            )

        self.assertEqual(serial["manifest"], parallel["manifest"])
        self.assertEqual(serial["strategy_summaries"], parallel["strategy_summaries"])
        self.assertEqual(serial["comparison"], parallel["comparison"])
        after = {
            str(path): hashlib.sha256(path.read_bytes()).hexdigest()
            for path in protected
        }
        self.assertEqual(before, after)

    def test_bundle_candidate_set_must_match_frozen_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            evidence, data = self._write_dataset(root)
            path = data / "2026-01-02.json"
            bundle = json.loads(path.read_text(encoding="utf-8"))
            bundle["candidates"][0]["symbol"] = "WRONG"
            path.write_text(json.dumps(bundle), encoding="utf-8")

            result = run_research(
                evidence_path=evidence,
                data_root=data,
                output_root=root / "runs",
                strategy_specs=("orb-5m-research",),
                workers=1,
            )

        summary = result["strategy_summaries"][0]
        self.assertEqual(summary["covered_days"], 1)
        self.assertEqual(summary["blocked_days"], 1)
        self.assertEqual(
            summary["block_reasons"], {"frozen_candidate_universe_mismatch": 1}
        )


if __name__ == "__main__":
    unittest.main()

import json
from datetime import datetime, timedelta
from pathlib import Path

import pytest

from challenger_orb_retest import (
    HYPOTHESIS_SHA256,
    ChallengerOrbRetestError,
    evaluate_retest_trigger,
)


START = datetime.fromisoformat("2026-01-05T09:35:00-05:00")
CUTOFF = datetime.fromisoformat("2026-01-05T10:30:00-05:00")


def test_implementation_is_bound_to_frozen_hypothesis():
    path = (
        Path(__file__).resolve().parents[1]
        / "learning/hypotheses"
        / f"experiment-catalyst-orb-retest-v1-{HYPOTHESIS_SHA256}.json"
    )
    contract = json.loads(path.read_text(encoding="utf-8"))

    assert contract["contract_sha256"] == HYPOTHESIS_SHA256
    assert len(contract["trial_family"]) == 1


def _trade(at, price, conditions=("@",)):
    return {
        "source_timestamp": at.isoformat(),
        "price": price,
        "conditions": list(conditions),
        "tape": "C",
        "trade_id": at.isoformat(),
    }


def _bar(minute, *, low=10.0, high=10.1, close=10.05, interpolated=False):
    at = START.replace(minute=minute, second=0)
    return {
        "time_et": at.isoformat(),
        "open": 10.05,
        "high": high,
        "low": low,
        "close": close,
        "volume": 1000,
        "interpolated": interpolated,
    }


def _evaluate(trades, bars, *, captured_through=CUTOFF):
    return evaluate_retest_trigger(
        opening_high=10.0,
        search_start=START,
        cutoff=CUTOFF,
        captured_through=captured_through,
        trades=trades,
        completed_minute_bars=bars,
        trade_window_complete=True,
        bar_window_complete=True,
    )


def test_finds_causal_break_retest_and_later_rebreak():
    initial = START + timedelta(seconds=10)
    rebreak = START.replace(minute=37, second=2)
    result = _evaluate(
        [_trade(initial, 10.01), _trade(rebreak, 10.11)],
        [_bar(36)],
        captured_through=rebreak + timedelta(seconds=10),
    )

    assert result["terminal_reason"] == "TRIGGER_FOUND"
    assert result["trigger"]["initial_break_at_et"] == initial.isoformat()
    assert result["trigger"]["retest_bar_start_et"] == START.replace(
        minute=36, second=0
    ).isoformat()
    assert result["trigger"]["rebreak_at_et"] == rebreak.isoformat()
    assert result["target_outcome_observed_or_derived"] is False


def test_crossing_minute_cannot_double_as_retest():
    initial = START.replace(minute=36, second=10)
    result = _evaluate(
        [_trade(initial, 10.01), _trade(START.replace(minute=37, second=2), 10.11)],
        [_bar(36)],
    )

    assert result["terminal_reason"] == "NO_RETEST_BEFORE_CUTOFF"


def test_first_touch_must_hold_opening_range_high():
    result = _evaluate(
        [_trade(START + timedelta(seconds=1), 10.01)],
        [_bar(36, low=9.98, close=9.99)],
        captured_through=START.replace(minute=37, second=0),
    )

    assert result["terminal_reason"] == "RETEST_HOLD_FAILED"


def test_special_print_cannot_establish_initial_break_or_rebreak():
    result = _evaluate(
        [
            _trade(START + timedelta(seconds=1), 10.01, conditions=("I",)),
            _trade(START.replace(minute=37, second=2), 10.11, conditions=("I",)),
        ],
        [_bar(36)],
    )

    assert result["terminal_reason"] == "NO_INITIAL_BREAK"


def test_rebreak_must_follow_completed_retest_bar():
    result = _evaluate(
        [
            _trade(START + timedelta(seconds=1), 10.01),
            _trade(START.replace(minute=36, second=30), 10.11),
        ],
        [_bar(36)],
    )

    assert result["terminal_reason"] == "NO_REBREAK_BEFORE_CUTOFF"


def test_decision_snapshot_must_complete_before_cutoff():
    result = _evaluate(
        [
            _trade(START + timedelta(seconds=1), 10.01),
            _trade(CUTOFF - timedelta(seconds=5), 10.11),
        ],
        [_bar(36)],
    )

    assert result["terminal_reason"] == "FINAL_DECISION_AFTER_CUTOFF"


def test_interpolated_or_incomplete_windows_fail_closed():
    with pytest.raises(ChallengerOrbRetestError, match="interpolation"):
        _evaluate([_trade(START + timedelta(seconds=1), 10.01)], [_bar(36, interpolated=True)])

    with pytest.raises(ChallengerOrbRetestError, match="incomplete"):
        evaluate_retest_trigger(
            opening_high=10.0,
            search_start=START,
            cutoff=CUTOFF,
            captured_through=CUTOFF,
            trades=[],
            completed_minute_bars=[],
            trade_window_complete=False,
            bar_window_complete=True,
        )


def test_trade_outside_causal_window_fails_closed():
    with pytest.raises(ChallengerOrbRetestError, match="outside"):
        _evaluate([_trade(CUTOFF, 10.01)], [])


def test_trigger_rejects_rows_or_capture_after_its_decision():
    initial = START + timedelta(seconds=10)
    rebreak = START.replace(minute=37, second=2)
    decision = rebreak + timedelta(seconds=10)
    with pytest.raises(ChallengerOrbRetestError, match="end at decision"):
        _evaluate(
            [
                _trade(initial, 10.01),
                _trade(rebreak, 10.11),
                _trade(decision + timedelta(seconds=1), 10.12),
            ],
            [_bar(36)],
            captured_through=decision + timedelta(seconds=2),
        )


def test_completed_bar_after_capture_fails_closed():
    with pytest.raises(ChallengerOrbRetestError, match="outside"):
        _evaluate(
            [_trade(START + timedelta(seconds=1), 10.01)],
            [_bar(36)],
            captured_through=START.replace(minute=36, second=30),
        )

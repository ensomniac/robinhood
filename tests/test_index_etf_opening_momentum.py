from __future__ import annotations

from datetime import datetime, timedelta

import dense_strategy_runtime as runtime
import index_etf_opening_momentum as momentum


def _session(day: str, opening_gain: float, count: int = 70):
    start = datetime.fromisoformat(f"{day}T09:30:00-05:00")
    bars = []
    previous = 100.0
    for index in range(count):
        progress = min((index + 1) / 30.0, 1.0)
        close = 100.0 * (1.0 + opening_gain * progress)
        bars.append(
            {
                "timestamp": (
                    start + timedelta(minutes=index)
                ).isoformat(),
                "open": previous,
                "high": max(previous, close) + 0.02,
                "low": min(previous, close) - 0.02,
                "close": close,
                "volume": 10_000,
                "vwap_numerator": close * 10_000,
                "vwap_denominator": 10_000,
            }
        )
        previous = close
    return bars


def _parameters():
    return {
        "opening_window_minutes": 30,
        "minimum_opening_return": 0.005,
        "vwap_confirmation_completed_bars": 1,
        "stop_intraday_atr": 1.0,
        "target_r": 1.0,
    }


def test_opening_momentum_historical_and_production_rank_same_symbol():
    day = "2024-01-03"
    minute_bars = {
        day: {
            "DIA": _session(day, 0.004),
            "IWM": _session(day, 0.007),
            "QQQ": _session(day, 0.006),
            "SPY": _session(day, 0.012),
        }
    }
    dataset = runtime.prepare_dataset(
        {
            "schema_version": 1,
            "family_id": runtime.INDEX_ETF_OPENING_MOMENTUM_FAMILY,
            "evaluation_dates": [day],
            "symbols": ["DIA", "IWM", "QQQ", "SPY"],
            "regular_session_minutes_by_date": {day: 70},
            "missed_data_dates": [],
            "minute_bars": minute_bars,
        }
    )

    candidates = runtime.build_candidates(
        dataset,
        runtime.INDEX_ETF_OPENING_MOMENTUM_FAMILY,
        _parameters(),
    )
    assert len(candidates) == 1
    assert candidates[0]["symbol"] == "SPY"
    assert candidates[0]["entry_price"] == minute_bars[day]["SPY"][31][
        "open"
    ]

    current = {
        symbol: bars[:31]
        for symbol, bars in minute_bars[day].items()
    }
    production = runtime.evaluate_production_signal(
        {
            "family_id": runtime.INDEX_ETF_OPENING_MOMENTUM_FAMILY,
            "session_date": day,
            "calendar_sessions": [day],
            "minute_history_complete": True,
            "symbols": ["DIA", "IWM", "QQQ", "SPY"],
            "minute_bars": {day: current},
        },
        family_id=runtime.INDEX_ETF_OPENING_MOMENTUM_FAMILY,
        parameters=_parameters(),
        frozen_universe={
            "symbols": ["DIA", "IWM", "QQQ", "SPY"]
        },
    )
    assert production["symbol"] == candidates[0]["symbol"]
    assert production["score"] == candidates[0]["opening_return"]


def test_opening_momentum_contract_freezes_new_mechanism_on_reused_inputs(
    tmp_path, monkeypatch
):
    original_repo_path = momentum._repo_path

    def repo_path(path):
        try:
            return original_repo_path(path)
        except ValueError:
            return f"strategy_tournament/v2/test/{path.name}"

    monkeypatch.setattr(momentum, "DEFAULT_ROOT", tmp_path)
    monkeypatch.setattr(momentum, "_repo_path", repo_path)

    _path, contract, capacity = momentum.freeze_contract(
        created_at=datetime.now().astimezone().isoformat(),
        enforce_commit=False,
    )

    assert capacity.is_file()
    assert contract["family_id"] == momentum.FAMILY_ID
    assert contract["mechanism_family"] == momentum.MECHANISM_FAMILY
    assert contract["new_mechanism_family_slot_consumed"] is True
    assert len(contract["trial_family"]) == 32
    assert (
        contract["development_evidence_classification"]
        == "CONTAMINATED_TRAINING_ONLY"
    )
    assert contract["confirmation_signal_capacity"] == 310
    assert contract["dataset_manifest"] == momentum._repo_path(
        momentum.SOURCE_MANIFEST
    )

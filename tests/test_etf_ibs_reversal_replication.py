from __future__ import annotations

from datetime import date, datetime, timedelta

import dense_strategy_runtime as runtime
import etf_ibs_reversal_replication as replication


def _business_dates(count: int) -> list[str]:
    result: list[str] = []
    current = date(2023, 1, 3)
    while len(result) < count:
        if current.weekday() < 5:
            result.append(current.isoformat())
        current += timedelta(days=1)
    return result


def _bars(
    dates: list[str],
    *,
    decision_date: str,
    decline: float,
    internal_bar_strength: float,
) -> list[dict[str, float | str]]:
    rows: list[dict[str, float | str]] = []
    previous = 100.0
    for day in dates:
        close = (
            previous * (1 + decline)
            if day == decision_date
            else previous * 1.002
        )
        if day == decision_date:
            low = close - 1.0
            high = low + 1.0 / internal_bar_strength
        else:
            low = min(previous, close) - 0.25
            high = max(previous, close) + 0.25
        rows.append(
            {
                "date": day,
                "open": previous,
                "high": high,
                "low": low,
                "close": close,
                "volume": 1_000_000,
            }
        )
        previous = close
    return rows


def _parameters() -> dict[str, float | int]:
    return {
        "internal_bar_strength_maximum": 0.2,
        "maximum_hold_sessions": 2,
        "one_session_decline_fraction": 0.005,
        "stop_atr14": 1.0,
        "trend_sma": 100,
    }


def test_replication_historical_and_production_rank_identically():
    dates = _business_dates(230)
    evaluation = dates[-12:]
    decision = evaluation[3]
    bars = {
        symbol: _bars(
            dates,
            decision_date=decision,
            decline=(-0.012 if symbol in {"IVV", "VBR"} else 0.001),
            internal_bar_strength=(
                0.08
                if symbol == "VBR"
                else 0.16
                if symbol == "IVV"
                else 0.5
            ),
        )
        for symbol in replication.SYMBOLS
    }
    family_id = runtime.ETF_IBS_REVERSAL_REPLICATION_FAMILY
    candidates = runtime.build_candidates(
        runtime.prepare_dataset(
            {
                "schema_version": 1,
                "family_id": family_id,
                "evaluation_dates": evaluation,
                "symbols": list(replication.SYMBOLS),
                "daily_bars": bars,
            }
        ),
        family_id,
        _parameters(),
    )
    observed = [
        item for item in candidates if item["decision_date"] == decision
    ]
    assert [item["symbol"] for item in observed] == ["VBR", "IVV"]
    assert all(f"-{family_id}-" in item["signal_id"] for item in observed)

    decision_index = dates.index(decision)
    production = runtime.evaluate_production_signal(
        {
            "family_id": family_id,
            "decision_date": decision,
            "next_session_date": dates[decision_index + 1],
            "calendar_dates": dates[: decision_index + 1],
            "daily_history_complete": True,
            "symbols": list(replication.SYMBOLS),
            "daily_bars": {
                symbol: rows[: decision_index + 1]
                for symbol, rows in bars.items()
            },
        },
        family_id=family_id,
        parameters=_parameters(),
        frozen_universe={
            "symbols": list(replication.SYMBOLS),
            "point_in_time": True,
        },
    )
    assert production["symbol"] == "VBR"
    assert production["holding_trading_days"] == 2
    assert production["overnight_hold"] is True


def test_replication_contract_preserves_grid_and_uses_disjoint_symbols(
    tmp_path, monkeypatch
):
    original_repo_path = replication._repo_path

    def repo_path(path):
        try:
            return original_repo_path(path)
        except ValueError:
            return f"strategy_tournament/v2/test/{path.name}"

    monkeypatch.setattr(replication, "DEFAULT_ROOT", tmp_path)
    monkeypatch.setattr(replication, "_repo_path", repo_path)
    monkeypatch.setattr(
        replication.outcome_exposure,
        "read_index",
        lambda *_args, **_kwargs: [],
    )
    monkeypatch.setattr(
        replication.outcome_exposure,
        "audit",
        lambda *_args, **_kwargs: {"index_sha256": "0" * 64},
    )

    _path, contract, capacity = replication.freeze_contract(
        created_at=datetime.now().astimezone().isoformat(),
        enforce_commit=False,
    )

    predecessor, *_rest = replication._predecessor_graph(
        enforce_commit=False
    )
    assert capacity.is_file()
    assert len(contract["trial_family"]) == 32
    assert contract["parameter_grid"] == predecessor["parameter_grid"]
    assert contract["new_mechanism_family_slot_consumed"] is False
    assert contract["research_generation"] == "existing_family_successor"
    assert contract["universe"]["symbols"] == replication.SYMBOLS
    assert contract["predecessor"]["promotion_evidence_reused"] is False
    assert contract["predecessor"]["parameter_grid_changed"] is False
    assert contract["confirmation_signal_capacity"] == 498

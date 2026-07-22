from __future__ import annotations

from datetime import date, timedelta

import pytest

import multi_asset_etf_tsmom_capacity as capacity_contract
import multi_asset_etf_tsmom_capacity_run as capacity_run
import multi_asset_etf_tsmom_stage0 as stage0


def _weekdays(start: date, count: int) -> list[str]:
    values = []
    current = start
    while len(values) < count:
        if current.weekday() < 5:
            values.append(current.isoformat())
        current += timedelta(days=1)
    return values


def _capacity_state() -> tuple[dict, list[str]]:
    dates = _weekdays(date(2021, 12, 1), 1065)
    schedule = capacity_run._weekly_schedule(dates)
    records = []
    for row in schedule:
        records.append({**row, "momentum_positive": True, "signal": True})
    return {"selection_sha256": "selection", "records": records}, dates


def _daily_row(day: str, *, open_price: float, high: float, low: float, close: float):
    return {
        "t": f"{day}T00:00:00-05:00",
        "o": open_price,
        "h": high,
        "l": low,
        "c": close,
        "v": 1_000_000,
        "i": False,
    }


def test_partition_freezes_stage0_development_embargo_and_confirmation():
    state, dates = _capacity_state()
    partition = stage0._partition(state, dates)
    assert partition["stage0_signal_count"] == 40
    assert partition["development_signal_count"] >= 30
    assert partition["confirmation_signal_count"] == 20
    assert partition["confirmation_embargo_sessions"] == 5
    stage0_rows = partition["partitions"]["stage0"]
    confirmation = partition["partitions"]["confirmation"]
    assert stage0_rows[-1]["week_ordinal"] < confirmation[0]["week_ordinal"]
    assert partition["market_outcomes_accessed"] is False
    assert partition["returns_computed"] == 0


def test_partition_rejects_insufficient_development_reserve():
    state, dates = _capacity_state()
    state["records"] = state["records"][:65]
    with pytest.raises(stage0.MultiAssetEtfTsmomStage0Error):
        stage0._partition(state, dates)


def test_outcome_uses_next_open_atr_stop_and_fifth_close():
    atr_rows = [
        _daily_row(
            f"2023-01-{index + 1:02d}",
            open_price=100,
            high=101,
            low=99,
            close=100,
        )
        for index in range(15)
    ]
    outcome_rows = [
        _daily_row(
            f"2023-02-{index + 1:02d}",
            open_price=100 + index,
            high=102 + index,
            low=99,
            close=101 + index,
        )
        for index in range(5)
    ]
    result = stage0._outcome(atr_rows, outcome_rows, 5)
    assert result["exit_reason"] == "force_flat_fifth_close"
    assert result["exit_date"] == "2023-02-05"
    assert result["net_r"] > 0


def test_outcome_applies_adverse_gap_before_intraday_stop():
    atr_rows = [
        _daily_row(
            f"2023-01-{index + 1:02d}",
            open_price=100,
            high=101,
            low=99,
            close=100,
        )
        for index in range(15)
    ]
    outcome_rows = [
        _daily_row(
            "2023-02-01",
            open_price=100,
            high=101,
            low=99,
            close=100,
        ),
        _daily_row(
            "2023-02-02",
            open_price=95,
            high=96,
            low=94,
            close=95,
        ),
        *[
            _daily_row(
                f"2023-02-{index:02d}",
                open_price=95,
                high=96,
                low=94,
                close=95,
            )
            for index in range(3, 6)
        ],
    ]
    result = stage0._outcome(atr_rows, outcome_rows, 5)
    assert result["exit_reason"] == "stop_gap"
    assert result["exit_date"] == "2023-02-02"
    assert result["stop_executed"] is True
    assert result["net_r"] < -1


def test_stage0_constants_preserve_portfolio_and_falsification_gates():
    assert stage0.STAGE0_SIGNAL_COUNT == 40
    assert stage0.CONFIRMATION_SIGNAL_COUNT == 20
    assert stage0.CONFIRMATION_EMBARGO_SESSIONS == 5
    assert stage0.MAXIMUM_HOLDING_SESSIONS == 5
    assert stage0.PRIMARY_COST_BPS == 5
    assert stage0.STRESS_COST_BPS == (10, 20)
    assert [item[0] for item in capacity_contract.UNIVERSE] == [
        "SPY",
        "EFA",
        "EEM",
        "IEF",
        "GLD",
        "DBC",
        "UUP",
    ]

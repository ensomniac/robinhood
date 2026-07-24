from __future__ import annotations

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pytest

import dense_data_collection as collection
import dense_intraday_recovery as recovery
import dense_strategy_runtime as runtime
from historical_store import HistoricalStoreConfig, canonical_sha256


EASTERN = ZoneInfo("America/New_York")


def _task(day: str, symbol: str) -> dict:
    value = {
        "kind": "sip_minute_bars",
        "date": day,
        "symbol": symbol,
    }
    value["task_id"] = canonical_sha256(value)
    return value


def _rows(day: str, *, missing_index: int | None = None) -> list[dict]:
    start = datetime.fromisoformat(f"{day}T09:30:00").replace(
        tzinfo=EASTERN
    )
    rows = []
    for index in range(390):
        if index == missing_index:
            continue
        timestamp = start + timedelta(minutes=index)
        price = 100.0 + index / 10_000
        rows.append(
            {
                "time_et": timestamp.isoformat(),
                "open": price,
                "high": price + 0.01,
                "low": price - 0.01,
                "close": price,
                "volume": 1000,
                "wap": price,
            }
        )
    return rows


def _fixture(tmp_path):
    days = ["2024-01-02", "2024-01-03"]
    symbols = ["QQQ", "SPY"]
    tasks = [_task(day, symbol) for day in days for symbol in symbols]
    source_root = tmp_path / "source"
    config = HistoricalStoreConfig(tmp_path / "store", min_free_bytes=0)
    incomplete = {
        "date": days[0],
        "symbol": "QQQ",
        "observed_minutes": 389,
        "expected_minutes": 390,
    }
    for task in tasks:
        rows = _rows(
            task["date"],
            missing_index=(
                100
                if task["date"] == days[0] and task["symbol"] == "QQQ"
                else None
            ),
        )
        collection._write_external(
            collection._checkpoint_path(source_root, task),
            {
                "schema_version": 1,
                "task": task,
                "rows": rows,
                "rows_sha256": canonical_sha256(rows),
            },
            config,
        )
    plan = {
        "evaluation_dates": days,
        "required_dates": days,
        "symbols": symbols,
        "tasks": tasks,
        "missing_data_policy": {
            "missed_dates": [days[0]],
            "missed_evaluation_dates": [days[0]],
            "incomplete_sessions": [incomplete],
        },
    }
    return source_root, plan


def test_retained_intraday_dataset_omits_only_the_incomplete_symbol_session(
    tmp_path,
):
    source_root, plan = _fixture(tmp_path)

    dataset = recovery.build_dataset(source_root, plan)
    prepared = runtime.prepare_dataset(dataset)

    assert dataset["missed_data_dates"] == ["2024-01-02"]
    assert set(dataset["minute_bars"]["2024-01-02"]) == {"SPY"}
    assert set(dataset["minute_bars"]["2024-01-03"]) == {"QQQ", "SPY"}
    assert prepared["_prepared_missed_data_dates"] == {"2024-01-02"}
    assert dataset["source_semantics"]["interpolation"] == "forbidden"
    assert dataset["source_semantics"]["substitution"] == "forbidden"


def test_retained_intraday_dataset_fails_closed_if_checkpoint_gaps_drift(
    tmp_path,
):
    source_root, plan = _fixture(tmp_path)
    plan["missing_data_policy"]["incomplete_sessions"][0][
        "observed_minutes"
    ] = 388

    with pytest.raises(
        recovery.DenseIntradayRecoveryError,
        match="inventory drifted",
    ):
        recovery.build_dataset(source_root, plan)

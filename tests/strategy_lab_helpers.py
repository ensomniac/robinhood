from __future__ import annotations

import copy
from datetime import date, timedelta
from pathlib import Path

from strategy_lab.config import LabConfig, load_config
from strategy_lab.database import LabDatabase


def make_test_config(root: Path, *, sessions: int = 20) -> LabConfig:
    raw = copy.deepcopy(load_config().raw)
    raw["paths"]["historical_data_root"] = str(root / "history")
    raw["paths"]["state_directory_name"] = "_state"
    raw["catalog"].update(
        {
            "minimum_common_sessions": max(10, sessions - 2),
            "minimum_development_sessions": max(5, sessions - 7),
            "minimum_holdout_sessions": 4,
            "holdout_fraction": 0.20,
            "embargo_sessions": 1,
            "minimum_symbol_coverage": 0.80,
            "minimum_price": 1.0,
            "minimum_median_dollar_volume_20": 1.0,
            "minimum_prior_sessions": 3,
        }
    )
    raw["validation"].update(
        {
            "walk_forward_folds": 5,
            "minimum_development_trades": 5,
            "minimum_holdout_trades": 2,
            "bootstrap_samples": 100,
        }
    )
    raw["daily_run"]["minimum_free_disk_gb"] = 1
    return LabConfig(raw=raw, path=root / "strategy_lab.toml")


def populate_observations(
    database: LabDatabase,
    *,
    sessions: int = 20,
    symbols: tuple[str, ...] = ("SPY", "AAA"),
    both_stop_and_target: bool = False,
) -> None:
    start = date(2024, 1, 2)
    rows = []
    for symbol_index, symbol in enumerate(symbols):
        for offset in range(sessions):
            day = start + timedelta(days=offset)
            price = 100.0 + symbol_index + offset * 0.01
            low = price * (0.98 if both_stop_and_target else 0.995)
            high = price * (1.02 if both_stop_and_target else 1.005)
            rows.append(
                [
                    symbol,
                    day,
                    "ETF" if symbol == "SPY" else "COMMON",
                    price,
                    high,
                    low,
                    price,
                    1_000_000.0,
                    0.0,
                    0.01,
                    price,
                    high,
                    low,
                    price,
                    True,
                    True,
                    "test",
                    f"source-{symbol}-{day}",
                    f"/test/{symbol}/{day}.json.gz",
                ]
            )
    database.connection.executemany(
        "INSERT INTO observations VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        rows,
    )

from __future__ import annotations

import copy
from pathlib import Path

import oversold_replication_development as development


def test_parameter_grid_is_unchanged_32_trials() -> None:
    assert development._trial_count() == 32
    assert development.PARAMETER_GRID == {
        "lookback_minutes": [15, 30],
        "rsi_maximum": [15.0, 20.0],
        "rsi_period": [3, 5],
        "selloff_threshold": [-0.02, -0.03],
        "target_r": [1.0, 1.5],
    }


def test_build_inventory_retains_empty_dates_and_exact_candidates(
    monkeypatch,
) -> None:
    dates = {
        "2023-01-26": {
            "evaluations": [
                {
                    "symbol": "EDGE",
                    "instrument_id": "id",
                    "primary_exchange": "XNYS",
                    "open_price": 10.2,
                    "prior_close": 10.0,
                }
            ]
        },
        "2023-01-27": {"evaluations": []},
        "2024-12-24": {"evaluations": []},
    }
    for index in range(396):
        dates[f"2023-02-{index:03d}"] = {"evaluations": []}
    nonempty = [key for key in sorted(dates) if key != "2023-01-27"][:100]
    for day in nonempty:
        if dates[day]["evaluations"]:
            continue
        dates[day]["evaluations"] = [
            {
                "symbol": f"S{len(day)}{day[-2:]}",
                "instrument_id": "id",
                "primary_exchange": "XNAS",
                "open_price": 10.2,
                "prior_close": 10.0,
            }
        ]
    # Expand one signal date so the frozen total is exactly 2,334.
    current = sum(
        len(development.gap._candidates(day, row))
        for day, row in dates.items()
    )
    needed = 2_334 - current
    target = nonempty[0]
    dates[target]["evaluations"].extend(
        {
            "symbol": f"X{index:04d}",
            "instrument_id": f"id-{index}",
            "primary_exchange": "XNYS",
            "open_price": 10.2,
            "prior_close": 10.0,
        }
        for index in range(needed)
    )
    monkeypatch.setattr(
        development,
        "sha256_file",
        lambda _path: "source-hash",
    )
    result = development._build_inventory({"dates": dates})
    assert len(result["evaluation_dates"]) == 399
    assert len(result["signal_dates"]) == 100
    assert len(result["zero_signal_dates"]) == 299
    assert sum(map(len, result["candidates_by_date"].values())) == 2_334
    assert "2023-01-27" in result["zero_signal_dates"]


def test_load_contract_rejects_content_drift(tmp_path: Path) -> None:
    content = {
        "artifact_kind": "oversold_replication_development_inventory_contract",
        "state": "DEVELOPMENT_INVENTORY_FROZEN_AWAITING_INSPECTION",
    }
    identity = development._hash(content)
    path = tmp_path / f"contract-{identity}.json"
    frozen = {**content, "contract_sha256": identity}
    development._write(path, frozen)
    assert development._load_contract(path) == frozen

    drifted = copy.deepcopy(frozen)
    drifted["state"] = "DRIFTED"
    development._write(path, drifted)
    try:
        development._load_contract(path)
    except development.OversoldReplicationDevelopmentError as exc:
        assert "hash is invalid" in str(exc)
    else:
        raise AssertionError("contract drift must fail closed")


def test_exposure_accounting_ignores_wildcard_pseudo_pairs(
    monkeypatch,
) -> None:
    inventory = {
        "outcome_scope": {
            "dates": ["2023-01-26"],
            "symbols_by_date": {
                "2023-01-26": ["A", "B"],
            },
        }
    }
    monkeypatch.setattr(
        development.outcome_exposure,
        "read_index",
        lambda: [{"scope": "ignored"}],
    )
    monkeypatch.setattr(
        development.outcome_exposure,
        "find_overlaps",
        lambda _scope, _records: [
            {
                "date": "2023-01-26",
                "symbol": "A",
                "exposure_id": "exact",
            },
            {
                "date": "2023-01-26",
                "symbol": "*",
                "exposure_id": "wildcard",
            },
        ],
    )
    monkeypatch.setattr(
        development.outcome_exposure,
        "audit",
        lambda: {"index_sha256": "index"},
    )
    result = development._exposure_accounting(inventory)
    assert result["previously_exposed_symbol_sessions"] == 1
    assert result["new_development_symbol_sessions"] == 1

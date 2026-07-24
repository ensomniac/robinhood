"""Residual-replication plugin with hash-after-load empty-series normalization.

The provider returned no daily rows for 35 of 5,985 requested symbols.  Those
symbols remain in the point-in-time reference denominator and cannot qualify
for the liquidity universe.  The frozen raw dataset is fully hash-verified
before this adapter removes only its empty ``daily_bars`` arrays in memory.
No non-empty row, date, identity, parameter, or strategy outcome is changed.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

import dense_strategy_plugin
import dense_strategy_runtime as runtime
import residual_replication_plugin as original


FAMILY_ID = runtime.EQUITY_RESIDUAL_REPLICATION_FAMILY


class ResidualReplicationNormalizationError(RuntimeError):
    """The frozen dataset cannot be normalized without semantic drift."""


def _drop_empty_series(dataset: dict[str, Any]) -> tuple[dict[str, Any], list[str]]:
    raw = dataset.get("daily_bars")
    if not isinstance(raw, Mapping):
        raise ResidualReplicationNormalizationError(
            "replication daily bars are missing"
        )
    invalid = [
        str(symbol)
        for symbol, rows in raw.items()
        if not isinstance(rows, list)
    ]
    if invalid:
        raise ResidualReplicationNormalizationError(
            "non-list daily series cannot be normalized"
        )
    removed = sorted(str(symbol) for symbol, rows in raw.items() if not rows)
    normalized = {
        str(symbol): rows
        for symbol, rows in raw.items()
        if isinstance(rows, list) and rows
    }
    if "SPY" not in normalized:
        raise ResidualReplicationNormalizationError(
            "SPY cannot be absent or empty"
        )
    if len(normalized) + len(removed) != len(raw):
        raise ResidualReplicationNormalizationError(
            "empty-series accounting is incomplete"
        )
    dataset["daily_bars"] = normalized
    dataset["input_normalization"] = {
        "rule": "remove_empty_daily_series_after_manifest_hash_verification",
        "removed_symbols": removed,
        "removed_symbol_count": len(removed),
        "nonempty_rows_changed": 0,
        "reference_identities_changed": 0,
    }
    return dataset, removed


def _load(
    contract: Mapping[str, Any],
    *,
    lane: str,
    expected_dates: Sequence[str],
    preregistration_sha256: str | None = None,
) -> dict[str, Any]:
    if lane == "development":
        manifest_path = dense_strategy_plugin._development_manifest_path(
            contract
        )
    else:
        manifest_path = dense_strategy_plugin._confirmation_manifest(contract)
    dataset, _manifest = dense_strategy_plugin._load_dataset(
        manifest_path,
        family_id=FAMILY_ID,
        lane=lane,
        expected_dates=expected_dates,
        preregistration_sha256=preregistration_sha256,
        enforce_commit=True,
    )
    normalized, removed = _drop_empty_series(dataset)
    if lane == "development" and len(removed) != 35:
        raise ResidualReplicationNormalizationError(
            "development empty-series count drifted"
        )
    decision_field = (
        "development_signal_dates"
        if lane == "development"
        else "confirmation_signal_dates"
    )
    if normalized.get("decision_dates") != contract.get(decision_field):
        raise ResidualReplicationNormalizationError(
            f"{lane} signal dates drifted"
        )
    return original._select_universes(normalized)


def preflight(contract: Mapping[str, Any]) -> dict[str, Any]:
    """Validate metadata only; price rows remain unopened."""

    result = original.preflight(contract)
    checks = dict(result["metadata_checks"])
    checks["empty_series_normalization_frozen"] = (
        contract.get("input_normalization")
        == {
            "stage": "after_manifest_hash_verification",
            "rule": "remove_only_empty_daily_series",
            "expected_development_empty_series": 35,
            "nonempty_rows_changed": 0,
        }
    )
    return {
        **result,
        "point_in_time_complete": all(checks.values()),
        "metadata_checks": checks,
    }


def evaluate_development(
    contract: Mapping[str, Any], trials: Sequence[Mapping[str, Any]]
) -> dict[str, Any]:
    dataset = _load(
        contract,
        lane="development",
        expected_dates=contract["development_dates"],
    )
    policy = original._account_policy()
    return {
        "dataset_manifest": str(contract["dataset_manifest"]),
        "trials": [
            runtime.evaluate_trial(
                dataset,
                family_id=FAMILY_ID,
                trial_id=str(trial["trial_id"]),
                parameters=trial["parameters"],
                account_policy=policy,
                rolling_origin_plan=contract.get("rolling_origin_plan"),
            )
            for trial in trials
        ],
        "provider_telemetry": original._telemetry(1),
        "input_normalization": dataset["input_normalization"],
    }


def evaluate_confirmation(winner: Mapping[str, Any]) -> dict[str, Any]:
    prepared = _load(
        winner,
        lane="confirmation",
        expected_dates=winner["confirmation_dates"],
        preregistration_sha256=str(winner["rules_hash"]),
    )
    exact = runtime.evaluate_trial(
        prepared,
        family_id=FAMILY_ID,
        trial_id=str(winner["exact_rules"]["selected_trial_id"]),
        parameters=winner["exact_rules"]["parameters"],
        account_policy=original._account_policy(),
    )
    return {
        "rules_hash": winner["rules_hash"],
        "parameter_alternatives": 0,
        "observed_dates": list(winner["confirmation_dates"]),
        "outcome_access_before_winner_freeze": False,
        "dataset_manifest": str(
            dense_strategy_plugin._confirmation_manifest(winner)
        ),
        "scenarios": {
            "primary_5bps": exact["scenarios"]["5bps"],
            "stress_10bps": exact["scenarios"]["10bps"],
            "stress_20bps": exact["scenarios"]["20bps"],
        },
        "maturity_rows": exact["maturity_rows"],
        "rule_violations": [],
        "capture_complete": True,
        "provider_telemetry": original._telemetry(1),
    }


def evaluate_production(
    winner: Mapping[str, Any], market_facts: Mapping[str, Any]
) -> dict[str, Any]:
    return original.evaluate_production(winner, market_facts)

"""Discovery adapter for the residual-reversal v7 temporal expansion."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

import dense_strategy_plugin
import dense_strategy_runtime as runtime
import residual_replication_plugin as original
import residual_temporal_data as data


FAMILY_ID = data.FAMILY_ID


class ResidualTemporalPluginError(RuntimeError):
    """The v7 dataset or exact input normalization drifted."""


def _drop_empty_series(
    dataset: dict[str, Any],
) -> tuple[dict[str, Any], list[str]]:
    raw = dataset.get("daily_bars")
    if not isinstance(raw, Mapping):
        raise ResidualTemporalPluginError("v7 daily bars are missing")
    if any(not isinstance(rows, list) for rows in raw.values()):
        raise ResidualTemporalPluginError(
            "v7 daily series must all be arrays"
        )
    removed = sorted(str(symbol) for symbol, rows in raw.items() if not rows)
    normalized = {
        str(symbol): rows
        for symbol, rows in raw.items()
        if isinstance(rows, list) and rows
    }
    if "SPY" not in normalized:
        raise ResidualTemporalPluginError("SPY cannot be empty")
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
        manifest_path = dense_strategy_plugin._confirmation_manifest(
            contract
        )
    dataset, manifest = dense_strategy_plugin._load_dataset(
        manifest_path,
        family_id=FAMILY_ID,
        lane=lane,
        expected_dates=expected_dates,
        preregistration_sha256=preregistration_sha256,
        enforce_commit=True,
    )
    normalized, removed = _drop_empty_series(dataset)
    binding = manifest["dataset_payload"]["dense_runtime"]
    expected_empty = binding.get("expected_empty_series")
    if (
        isinstance(expected_empty, bool)
        or not isinstance(expected_empty, int)
        or len(removed) != expected_empty
    ):
        raise ResidualTemporalPluginError(
            f"{lane} empty-series count drifted"
        )
    decision_field = (
        "development_signal_dates"
        if lane == "development"
        else "confirmation_signal_dates"
    )
    if normalized.get("decision_dates") != contract.get(decision_field):
        raise ResidualTemporalPluginError(f"{lane} signal dates drifted")
    return original._select_universes(normalized)


def preflight(contract: Mapping[str, Any]) -> dict[str, Any]:
    result = dense_strategy_plugin.preflight(contract)
    checks = dict(result["metadata_checks"])
    checks.update(
        {
            "temporal_expansion": contract.get("research_generation")
            == "existing_family_temporal_expansion",
            "development_signal_capacity": len(
                contract.get("development_signal_dates", [])
            )
            == 400,
            "fresh_development_signals": contract.get(
                "fresh_development_signal_count"
            )
            == 200,
            "contaminated_training_signals": contract.get(
                "contaminated_training_signal_count"
            )
            == 200,
            "confirmation_signal_capacity": len(
                contract.get("confirmation_signal_dates", [])
            )
            == 93,
            "rules_and_grid_unchanged": contract.get(
                "rules_or_parameter_grid_changed"
            )
            is False,
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
    dataset = _load(
        winner,
        lane="confirmation",
        expected_dates=winner["confirmation_dates"],
        preregistration_sha256=str(winner["rules_hash"]),
    )
    exact = runtime.evaluate_trial(
        dataset,
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
    return dense_strategy_plugin.evaluate_production(winner, market_facts)

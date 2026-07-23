"""Discovery adapter for the SPY turn-of-month seasonality successor."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

import dense_strategy_plugin
import dense_strategy_runtime as runtime
import etf_cross_sectional_momentum_plugin as shared


TARGET_FAMILY_ID = runtime.ETF_TURN_OF_MONTH_FAMILY
SOURCE_KEY = "etf_turn_of_month_source"


def _load_dataset(
    manifest_path: Any,
    *,
    lane: str,
    expected_dates: Sequence[str],
    preregistration_sha256: str | None = None,
) -> dict[str, Any]:
    source = shared._load_dataset(
        manifest_path,
        lane=lane,
        expected_dates=expected_dates,
        preregistration_sha256=preregistration_sha256,
        source_key=SOURCE_KEY,
        target_family_id=TARGET_FAMILY_ID,
    )
    source.pop("_prepared_daily_bars", None)
    source["symbols"] = ["SPY"]
    source["daily_bars"] = {"SPY": source["daily_bars"]["SPY"]}
    return runtime.prepare_dataset(source)


def preflight(contract: Mapping[str, Any]) -> dict[str, Any]:
    """Inspect committed metadata without opening the external bar dataset."""

    manifest_path = shared._manifest_path(
        contract.get("capacity_manifest", contract.get("dataset_manifest"))
    )
    shared._require_committed(manifest_path)
    manifest = shared._load_manifest(manifest_path)
    binding = shared._binding(
        manifest,
        source_key=SOURCE_KEY,
        target_family_id=TARGET_FAMILY_ID,
    )
    checks = {
        "development_lane": manifest["dataset_payload"].get("lane")
        == "development",
        "family_bound": binding["target_family_id"]
        == contract.get("family_id"),
        "dates_bound": manifest.get("requested_dates")
        == list(contract.get("development_dates", [])),
        "point_in_time": manifest["dataset_payload"].get(
            "point_in_time_evidence"
        )
        is True,
        "contaminated_training_declared": manifest["dataset_payload"].get(
            "development_training_contaminated"
        )
        is True,
        "confirmation_locked": contract.get("confirmation_access_permitted")
        is not True,
    }
    return {
        "verified_capacity": int(binding["formal_capacity"]),
        "point_in_time_complete": all(checks.values()),
        "metadata_checks": checks,
        "external_dataset_opened": False,
        "provider_telemetry": shared._telemetry(dataset_loads=0),
    }


def evaluate_development(
    contract: Mapping[str, Any], trials: Sequence[Mapping[str, Any]]
) -> dict[str, Any]:
    dataset = _load_dataset(
        shared._manifest_path(contract.get("dataset_manifest")),
        lane="development",
        expected_dates=contract["development_dates"],
    )
    policy = shared._account_policy()
    return {
        "dataset_manifest": str(contract["dataset_manifest"]),
        "trials": [
            runtime.evaluate_trial(
                dataset,
                family_id=TARGET_FAMILY_ID,
                trial_id=str(trial["trial_id"]),
                parameters=trial["parameters"],
                account_policy=policy,
                rolling_origin_plan=contract.get("rolling_origin_plan"),
            )
            for trial in trials
        ],
        "provider_telemetry": shared._telemetry(dataset_loads=1),
    }


def evaluate_confirmation(winner: Mapping[str, Any]) -> dict[str, Any]:
    dataset = _load_dataset(
        shared._manifest_path(winner.get("confirmation_dataset_manifest")),
        lane="confirmation",
        expected_dates=winner["confirmation_dates"],
        preregistration_sha256=str(winner["rules_hash"]),
    )
    exact = runtime.evaluate_trial(
        dataset,
        family_id=TARGET_FAMILY_ID,
        trial_id=str(winner["exact_rules"]["selected_trial_id"]),
        parameters=winner["exact_rules"]["parameters"],
        account_policy=shared._account_policy(),
    )
    return {
        "rules_hash": winner["rules_hash"],
        "parameter_alternatives": 0,
        "observed_dates": list(winner["confirmation_dates"]),
        "outcome_access_before_winner_freeze": False,
        "dataset_manifest": str(winner["confirmation_dataset_manifest"]),
        "scenarios": {
            "primary_5bps": exact["scenarios"]["5bps"],
            "stress_10bps": exact["scenarios"]["10bps"],
            "stress_20bps": exact["scenarios"]["20bps"],
        },
        "maturity_rows": exact["maturity_rows"],
        "rule_violations": [],
        "capture_complete": True,
        "provider_telemetry": shared._telemetry(dataset_loads=1),
    }


def evaluate_production(
    winner: Mapping[str, Any], market_facts: Mapping[str, Any]
) -> dict[str, Any]:
    return dense_strategy_plugin.evaluate_production(winner, market_facts)

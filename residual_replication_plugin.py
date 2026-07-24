"""Discovery adapter for the disjoint long-history residual replication."""

from __future__ import annotations

import statistics
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import dense_strategy_plugin
import dense_strategy_runtime as runtime
import portfolio_maturity


PROJECT_ROOT = Path(__file__).resolve().parent
FAMILY_ID = runtime.EQUITY_RESIDUAL_REPLICATION_FAMILY


class ResidualReplicationPluginError(RuntimeError):
    """The exact replication input or universe selection is incomplete."""


def _select_universes(dataset: dict[str, Any]) -> dict[str, Any]:
    identities = dataset.get("reference_identities_by_date")
    decision_dates = dataset.get("decision_dates")
    daily = dataset.get("daily_bars")
    if not (
        isinstance(identities, Mapping)
        and isinstance(decision_dates, list)
        and isinstance(daily, Mapping)
        and set(identities) == set(decision_dates)
    ):
        raise ResidualReplicationPluginError(
            "replication dataset topology is incomplete"
        )
    spy_rows = daily.get("SPY")
    if not isinstance(spy_rows, list):
        raise ResidualReplicationPluginError(
            "replication SPY session calendar is missing"
        )
    calendar = [
        str(row["date"])
        for row in spy_rows
        if isinstance(row, Mapping) and isinstance(row.get("date"), str)
    ]
    if len(calendar) != len(set(calendar)) or calendar != sorted(calendar):
        raise ResidualReplicationPluginError(
            "replication SPY session calendar is malformed"
        )
    positions = {str(day): index for index, day in enumerate(calendar)}
    indices: dict[str, dict[str, Mapping[str, Any]]] = {}
    for raw_symbol, rows in daily.items():
        if not isinstance(rows, list):
            raise ResidualReplicationPluginError(
                "replication daily series is malformed"
            )
        symbol = str(raw_symbol)
        mapped: dict[str, Mapping[str, Any]] = {}
        for row in rows:
            if not isinstance(row, Mapping) or not isinstance(
                row.get("date"), str
            ):
                raise ResidualReplicationPluginError(
                    f"{symbol}: replication daily row is malformed"
                )
            day = str(row["date"])
            if day in mapped:
                raise ResidualReplicationPluginError(
                    f"{symbol}: duplicate daily row on {day}"
                )
            mapped[day] = row
        indices[symbol] = mapped
    universe_by_date: dict[str, list[str]] = {}
    selected_identities: dict[str, dict[str, str]] = {}
    for decision_date in decision_dates:
        index = positions.get(str(decision_date), -1)
        if index < 59:
            universe_by_date[str(decision_date)] = []
            selected_identities[str(decision_date)] = {}
            continue
        prior_60 = list(map(str, calendar[index - 59 : index + 1]))
        candidates: list[tuple[float, str, str]] = []
        day_identities = identities[decision_date]
        if not isinstance(day_identities, Mapping):
            raise ResidualReplicationPluginError(
                f"{decision_date}: identity denominator is malformed"
            )
        for raw_symbol, raw_identity in day_identities.items():
            symbol = str(raw_symbol)
            identity = str(raw_identity)
            series = indices.get(symbol)
            if series is None:
                continue
            history = [series.get(day) for day in prior_60]
            if any(row is None for row in history):
                continue
            complete = [row for row in history if row is not None]
            dollars = [
                float(row["close"]) * float(row["volume"]) for row in complete
            ]
            if (
                float(complete[-1]["close"]) < 10
                or statistics.median(dollars[-20:]) < 50_000_000
            ):
                continue
            candidates.append(
                (statistics.median(dollars), symbol, identity)
            )
        selected = sorted(
            candidates, key=lambda item: (-item[0], item[1])
        )[:250]
        if index >= 60 and len(selected) != 250:
            raise ResidualReplicationPluginError(
                f"{decision_date}: only {len(selected)} liquid common stocks"
            )
        identities_seen = [item[2] for item in selected]
        if len(identities_seen) != len(set(identities_seen)):
            raise ResidualReplicationPluginError(
                f"{decision_date}: duplicate selected listing identity"
            )
        universe_by_date[str(decision_date)] = [item[1] for item in selected]
        selected_identities[str(decision_date)] = {
            symbol: identity for _liquidity, symbol, identity in selected
        }
    dataset["universe_by_date"] = universe_by_date
    dataset["universe_identity_by_date"] = selected_identities
    dataset.pop("reference_identities_by_date", None)
    return runtime.prepare_dataset(dataset)


def _load_development(contract: Mapping[str, Any]) -> dict[str, Any]:
    manifest_path = dense_strategy_plugin._development_manifest_path(contract)
    dataset, _manifest = dense_strategy_plugin._load_dataset(
        manifest_path,
        family_id=FAMILY_ID,
        lane="development",
        expected_dates=contract["development_dates"],
        enforce_commit=True,
    )
    if dataset.get("decision_dates") != contract.get(
        "development_signal_dates"
    ):
        raise ResidualReplicationPluginError(
            "development signal dates drifted"
        )
    return _select_universes(dataset)


def _account_policy() -> dict[str, Any]:
    config = portfolio_maturity.load_config()
    return {
        "starting_equity": 100_000.0,
        "risk_fraction": config.raw["pilot_risk"][
            "maximum_planned_loss_fraction_per_position"
        ],
        "maximum_concurrent_positions": config.raw["portfolio"][
            "maximum_concurrent_positions"
        ],
        "maximum_aggregate_risk_fraction": config.raw["pilot_risk"][
            "maximum_aggregate_planned_open_loss_fraction"
        ],
        "maximum_gross_notional_fraction": config.raw["pilot_risk"][
            "maximum_gross_notional_fraction"
        ],
    }


def _telemetry(loads: int) -> dict[str, Any]:
    return {
        "requests": 0,
        "request_seconds": 0.0,
        "pacing_wait_seconds": 0.0,
        "cache_hits": 1,
        "failures": 0,
        "dataset_loads": loads,
    }


def preflight(contract: Mapping[str, Any]) -> dict[str, Any]:
    """Inspect the committed dataset manifest without opening price rows."""

    result = dense_strategy_plugin.preflight(contract)
    checks = dict(result["metadata_checks"])
    checks.update(
        {
            "disjoint_replication": contract.get("research_generation")
            == "existing_family_disjoint_replication",
            "development_signal_capacity": len(
                contract.get("development_signal_dates", [])
            )
            == 200,
            "confirmation_signal_capacity": len(
                contract.get("confirmation_signal_dates", [])
            )
            >= 20,
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
    dataset = _load_development(contract)
    policy = _account_policy()
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
        "provider_telemetry": _telemetry(1),
    }


def evaluate_confirmation(winner: Mapping[str, Any]) -> dict[str, Any]:
    manifest_path = dense_strategy_plugin._confirmation_manifest(winner)
    dataset, _manifest = dense_strategy_plugin._load_dataset(
        manifest_path,
        family_id=FAMILY_ID,
        lane="confirmation",
        expected_dates=winner["confirmation_dates"],
        preregistration_sha256=str(winner["rules_hash"]),
    )
    if dataset.get("decision_dates") != winner.get(
        "confirmation_signal_dates"
    ):
        raise ResidualReplicationPluginError(
            "confirmation signal dates drifted"
        )
    prepared = _select_universes(dataset)
    exact = runtime.evaluate_trial(
        prepared,
        family_id=FAMILY_ID,
        trial_id=str(winner["exact_rules"]["selected_trial_id"]),
        parameters=winner["exact_rules"]["parameters"],
        account_policy=_account_policy(),
    )
    return {
        "rules_hash": winner["rules_hash"],
        "parameter_alternatives": 0,
        "observed_dates": list(winner["confirmation_dates"]),
        "outcome_access_before_winner_freeze": False,
        "dataset_manifest": str(manifest_path),
        "scenarios": {
            "primary_5bps": exact["scenarios"]["5bps"],
            "stress_10bps": exact["scenarios"]["10bps"],
            "stress_20bps": exact["scenarios"]["20bps"],
        },
        "maturity_rows": exact["maturity_rows"],
        "rule_violations": [],
        "capture_complete": True,
        "provider_telemetry": _telemetry(1),
    }


def evaluate_production(
    winner: Mapping[str, Any], market_facts: Mapping[str, Any]
) -> dict[str, Any]:
    return dense_strategy_plugin.evaluate_production(winner, market_facts)

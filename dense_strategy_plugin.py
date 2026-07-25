"""Discovery-plugin adapter for the three dense v2 strategy families.

Row-level daily/minute data remains in the ignored historical store.  Git-visible
dataset manifests bind the exact external path, file bytes, canonical payload,
family, dates, and evidence lane before this adapter reads any outcomes.
"""

from __future__ import annotations

import gzip
import json
import subprocess
from collections.abc import Mapping, Sequence
from datetime import datetime, time, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import dense_strategy_runtime as runtime
import portfolio_maturity
from historical_store import (
    DEFAULT_ENV_PATH,
    HistoricalStoreConfig,
    HistoricalStoreError,
    canonical_sha256,
    sha256_file,
)
from learning_data import LearningDataError, load_frozen_dataset_contract


PROJECT_ROOT = Path(__file__).resolve().parent
MARKET_TIME_ZONE = ZoneInfo("America/New_York")
CONFIRMATION_MANIFEST_ROOT = (
    PROJECT_ROOT / "strategy_tournament/v2/discovery"
)


class DenseStrategyPluginError(RuntimeError):
    """A dense-family manifest, private dataset, or exact binding is invalid."""


def _manifest_path(value: Any) -> Path:
    if not isinstance(value, str) or not value:
        raise DenseStrategyPluginError("dataset manifest path is missing")
    path = Path(value)
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    return path


def _load_manifest(path: Path) -> dict[str, Any]:
    try:
        return load_frozen_dataset_contract(path)
    except (LearningDataError, OSError) as exc:
        raise DenseStrategyPluginError(f"dataset manifest is invalid: {exc}") from exc


def _runtime_binding(manifest: Mapping[str, Any]) -> dict[str, Any]:
    binding = manifest["dataset_payload"].get("dense_runtime")
    if not isinstance(binding, Mapping):
        raise DenseStrategyPluginError("dataset manifest lacks dense_runtime binding")
    required_text = (
        "family_id",
        "external_relative_path",
        "external_file_sha256",
        "dataset_sha256",
    )
    for field in required_text:
        if not isinstance(binding.get(field), str) or not binding[field]:
            raise DenseStrategyPluginError(f"dense_runtime.{field} is missing")
    if binding.get("format") not in {"json", "json.gz"}:
        raise DenseStrategyPluginError("dense_runtime.format is unsupported")
    if any(
        len(str(binding[field])) != 64
        for field in ("external_file_sha256", "dataset_sha256")
    ):
        raise DenseStrategyPluginError("dense runtime hashes must be SHA-256")
    relative = Path(str(binding["external_relative_path"]))
    if relative.is_absolute() or ".." in relative.parts:
        raise DenseStrategyPluginError("dense runtime external path is unsafe")
    capacity = binding.get("formal_capacity")
    if isinstance(capacity, bool) or not isinstance(capacity, int) or capacity < 0:
        raise DenseStrategyPluginError("dense_runtime.formal_capacity is invalid")
    return dict(binding)


def _require_committed(path: Path) -> None:
    try:
        relative = str(path.resolve().relative_to(PROJECT_ROOT.resolve()))
    except ValueError as exc:
        raise DenseStrategyPluginError(
            "dense dataset manifest must be stored in the repository"
        ) from exc
    tracked = subprocess.run(
        ["git", "ls-files", "--error-unmatch", "--", relative],
        cwd=PROJECT_ROOT,
        capture_output=True,
    )
    clean = subprocess.run(
        ["git", "diff", "--quiet", "HEAD", "--", relative],
        cwd=PROJECT_ROOT,
    )
    if tracked.returncode != 0 or clean.returncode != 0:
        raise DenseStrategyPluginError(
            "dense dataset manifest must be committed and unchanged"
        )


def _load_dataset(
    manifest_path: Path,
    *,
    family_id: str,
    lane: str,
    expected_dates: Sequence[str],
    preregistration_sha256: str | None = None,
    development_search_sha256: str | None = None,
    enforce_commit: bool = True,
) -> tuple[dict[str, Any], dict[str, Any]]:
    if enforce_commit:
        _require_committed(manifest_path)
    manifest = _load_manifest(manifest_path)
    if manifest.get("requested_dates") != list(expected_dates):
        raise DenseStrategyPluginError("dataset dates drifted from the frozen partition")
    payload = manifest["dataset_payload"]
    if payload.get("lane") != lane or payload.get("inspected") is not True:
        raise DenseStrategyPluginError("dataset lane is wrong or uninspected")
    if lane == "confirmation" and payload.get(
        "preregistration_sha256"
    ) != preregistration_sha256:
        raise DenseStrategyPluginError(
            "confirmation data is not bound to the frozen winner"
        )
    if lane == "development" and development_search_sha256 is not None and payload.get(
        "development_search_sha256"
    ) != development_search_sha256:
        raise DenseStrategyPluginError(
            "development data is not bound to the frozen search"
        )
    binding = _runtime_binding(manifest)
    if binding["family_id"] != family_id:
        raise DenseStrategyPluginError("dataset family binding drifted")
    try:
        store = HistoricalStoreConfig.from_env(DEFAULT_ENV_PATH)
    except HistoricalStoreError as exc:
        raise DenseStrategyPluginError(str(exc)) from exc
    path = (store.root / binding["external_relative_path"]).resolve()
    if store.root.resolve() not in path.parents:
        raise DenseStrategyPluginError("external dataset escaped the historical store")
    if not path.is_file() or sha256_file(path) != binding["external_file_sha256"]:
        raise DenseStrategyPluginError("external dense dataset is missing or drifted")
    try:
        if binding["format"] == "json.gz":
            with gzip.open(path, "rt", encoding="utf-8") as source:
                dataset = json.load(source)
        else:
            dataset = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise DenseStrategyPluginError("external dense dataset is unreadable") from exc
    if not isinstance(dataset, dict):
        raise DenseStrategyPluginError("external dense dataset must be an object")
    if canonical_sha256(dataset) != binding["dataset_sha256"]:
        raise DenseStrategyPluginError("external dense dataset content hash drifted")
    if dataset.get("family_id") != family_id or dataset.get(
        "evaluation_dates"
    ) != list(expected_dates):
        raise DenseStrategyPluginError("external dense dataset scope drifted")
    return dataset, manifest


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


def _telemetry(*, dataset_loads: int) -> dict[str, Any]:
    return {
        "requests": 0,
        "request_seconds": 0.0,
        "pacing_wait_seconds": 0.0,
        "cache_hits": 1,
        "failures": 0,
        "dataset_loads": dataset_loads,
    }


def preflight(contract: Mapping[str, Any]) -> dict[str, Any]:
    """Inspect only frozen metadata; never open the external outcome dataset."""

    manifest_path = _manifest_path(
        contract.get("capacity_manifest", contract.get("dataset_manifest"))
    )
    _require_committed(manifest_path)
    manifest = _load_manifest(manifest_path)
    capacity = manifest["dataset_payload"].get("dense_capacity")
    binding = (
        dict(capacity)
        if isinstance(capacity, Mapping)
        else _runtime_binding(manifest)
    )
    if isinstance(binding.get("formal_capacity"), bool) or not isinstance(
        binding.get("formal_capacity"), int
    ):
        raise DenseStrategyPluginError("dense capacity is invalid")
    checks = {
        "development_lane": manifest["dataset_payload"].get("lane") == "development",
        "family_bound": binding["family_id"] == contract.get("family_id"),
        "dates_bound": set(manifest.get("requested_dates", []))
        >= set(contract.get("development_dates", [])),
        "point_in_time": manifest["dataset_payload"].get(
            "point_in_time_evidence"
        )
        is True,
    }
    return {
        "verified_capacity": int(binding["formal_capacity"]),
        "point_in_time_complete": all(checks.values()),
        "metadata_checks": checks,
        "external_dataset_opened": False,
        "provider_telemetry": _telemetry(dataset_loads=0),
    }


def _development_manifest_path(contract: Mapping[str, Any]) -> Path:
    explicit = contract.get("dataset_manifest")
    if explicit is not None:
        return _manifest_path(explicit)
    expected_search = contract.get("development_search_sha256")
    if not isinstance(expected_search, str) or len(expected_search) != 64:
        raise DenseStrategyPluginError(
            "development search binding is missing"
        )
    directory = (
        CONFIRMATION_MANIFEST_ROOT
        / str(contract["family_id"])
        / "development-dataset"
    )
    matches: list[Path] = []
    for path in sorted(directory.glob("dataset-*.json")):
        manifest = _load_manifest(path)
        payload = manifest["dataset_payload"]
        binding = payload.get("dense_runtime")
        if (
            payload.get("lane") == "development"
            and payload.get("development_search_sha256") == expected_search
            and isinstance(binding, Mapping)
            and binding.get("family_id") == contract["family_id"]
            and manifest.get("requested_dates")
            == contract["development_dates"]
        ):
            matches.append(path)
    if len(matches) != 1:
        raise DenseStrategyPluginError(
            "expected exactly one development dataset manifest bound to "
            f"search {expected_search}; found {len(matches)}"
        )
    return matches[0]


def evaluate_development(
    contract: Mapping[str, Any], trials: Sequence[Mapping[str, Any]]
) -> dict[str, Any]:
    explicit = contract.get("dataset_manifest")
    manifest_path = _development_manifest_path(contract)
    dataset, _manifest = _load_dataset(
        manifest_path,
        family_id=str(contract["family_id"]),
        lane="development",
        expected_dates=contract["development_dates"],
        development_search_sha256=(
            str(contract["development_search_sha256"])
            if explicit is None
            and contract.get("development_search_sha256") is not None
            else None
        ),
        enforce_commit=explicit is None,
    )
    dataset = runtime.prepare_dataset(dataset)
    policy = _account_policy()
    return {
        "dataset_manifest": (
            str(explicit) if explicit is not None else str(manifest_path)
        ),
        "trials": [
            runtime.evaluate_trial(
                dataset,
                family_id=str(contract["family_id"]),
                trial_id=str(trial["trial_id"]),
                parameters=trial["parameters"],
                account_policy=policy,
                rolling_origin_plan=contract.get("rolling_origin_plan"),
            )
            for trial in trials
        ],
        "provider_telemetry": _telemetry(dataset_loads=1),
    }


def _confirmation_manifest(winner: Mapping[str, Any]) -> Path:
    explicit = winner.get("confirmation_dataset_manifest")
    if explicit is not None:
        return _manifest_path(explicit)
    directory = (
        CONFIRMATION_MANIFEST_ROOT
        / str(winner["family_id"])
        / "confirmation-dataset"
    )
    paths = sorted(directory.glob("dataset-*.json"))
    if len(paths) != 1:
        raise DenseStrategyPluginError(
            "expected exactly one frozen confirmation dataset manifest"
        )
    return paths[0]


def evaluate_confirmation(winner: Mapping[str, Any]) -> dict[str, Any]:
    manifest_path = _confirmation_manifest(winner)
    dataset, _manifest = _load_dataset(
        manifest_path,
        family_id=str(winner["family_id"]),
        lane="confirmation",
        expected_dates=winner["confirmation_dates"],
        preregistration_sha256=str(winner["rules_hash"]),
    )
    dataset = runtime.prepare_dataset(dataset)
    exact = runtime.evaluate_trial(
        dataset,
        family_id=str(winner["family_id"]),
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
        "provider_telemetry": _telemetry(dataset_loads=1),
    }


def evaluate_production(
    winner: Mapping[str, Any], market_facts: Mapping[str, Any]
) -> dict[str, Any]:
    """Rebuild live discovery/ranking through the frozen historical semantics."""

    if market_facts.get("selected_trial_id") != winner["exact_rules"][
        "selected_trial_id"
    ]:
        raise DenseStrategyPluginError("live selected trial drifted")
    if market_facts.get("parameters") != winner["exact_rules"]["parameters"]:
        raise DenseStrategyPluginError("live exact parameters drifted")
    expected = {
        "selected_trial_id",
        "parameters",
        "decision_data",
        "quote",
        "operational",
    }
    if set(market_facts) != expected:
        raise DenseStrategyPluginError("live market-fact schema drifted")
    exact_rules = winner["exact_rules"]
    universe = exact_rules.get("universe")
    if not isinstance(universe, Mapping):
        raise DenseStrategyPluginError("frozen winner does not bind its universe")
    try:
        signal = runtime.evaluate_production_signal(
            market_facts["decision_data"],
            family_id=str(winner["family_id"]),
            parameters=exact_rules["parameters"],
            frozen_universe=universe,
        )
    except runtime.DenseStrategyRuntimeError as exc:
        raise DenseStrategyPluginError(str(exc)) from exc
    quote = market_facts["quote"]
    if not isinstance(quote, Mapping) or set(quote) != {
        "symbol",
        "observed_at",
        "halted",
        "tradable",
        "bid",
        "ask",
        "executable_ask_depth",
        "recent_real_minute_volume",
    }:
        raise DenseStrategyPluginError("live quote schema drifted")
    if quote["symbol"] != signal["symbol"]:
        raise DenseStrategyPluginError("fresh quote does not match ranked signal")
    try:
        quote_observed = datetime.fromisoformat(str(quote["observed_at"]))
    except ValueError as exc:
        raise DenseStrategyPluginError("live quote timestamp is invalid") from exc
    if quote_observed.tzinfo is None:
        raise DenseStrategyPluginError("live quote timestamp needs a timezone")
    if signal.get("trigger_bar_timestamp") is not None:
        try:
            trigger = datetime.fromisoformat(str(signal["trigger_bar_timestamp"]))
        except ValueError as exc:
            raise DenseStrategyPluginError(
                "intraday signal timestamps are invalid"
            ) from exc
        interval_minutes = signal.get("entry_interval_minutes", 1)
        if (
            isinstance(interval_minutes, bool)
            or not isinstance(interval_minutes, int)
            or interval_minutes not in {1, 15}
        ):
            raise DenseStrategyPluginError(
                "intraday entry interval is invalid"
            )
        next_interval = trigger + timedelta(minutes=interval_minutes)
        if (
            trigger.tzinfo is None
            or quote_observed < next_interval
            or quote_observed >= next_interval + timedelta(minutes=1)
            or quote_observed.astimezone(MARKET_TIME_ZONE).date()
            != trigger.astimezone(MARKET_TIME_ZONE).date()
        ):
            raise DenseStrategyPluginError(
                "quote is not from the next observable intraday interval"
            )
    else:
        try:
            next_session = datetime.fromisoformat(
                str(signal["next_session_date"])
            ).date()
        except ValueError as exc:
            raise DenseStrategyPluginError("daily session date is invalid") from exc
        local_quote = quote_observed.astimezone(MARKET_TIME_ZONE)
        market_open = datetime.combine(
            next_session,
            time(hour=9, minute=30),
            tzinfo=MARKET_TIME_ZONE,
        )
        if (
            local_quote.date() != next_session
            or local_quote < market_open
            or local_quote >= market_open + timedelta(minutes=1)
        ):
            raise DenseStrategyPluginError(
                "daily quote is not from the frozen next-session opening interval"
            )
    operational = market_facts["operational"]
    operational_fields = {
        "before_open_account_reconciled",
        "before_open_orders_reconciled",
        "before_open_protection_reconciled",
        "before_open_tradability_reconciled",
        "before_open_news_reconciled",
        "protective_order_route_ready",
        "monitoring_ready",
        "protection_failure_safe_cutoff",
    }
    if not isinstance(operational, Mapping) or set(operational) != operational_fields:
        raise DenseStrategyPluginError("live operational fact schema drifted")
    try:
        entry = float(quote["ask"])
        stop = entry - float(signal["stop_atr_multiple"]) * float(signal["atr"])
    except (TypeError, ValueError) as exc:
        raise DenseStrategyPluginError("live stop inputs are invalid") from exc
    if entry <= 0 or stop <= 0 or stop >= entry:
        raise DenseStrategyPluginError("live structural stop is invalid")
    expected_gross_move_fraction = float(
        signal["expected_gross_move_fraction"]
    )
    if winner["family_id"] == runtime.SPY_RSI2_PULLBACK_FAMILY:
        expected_gross_move_fraction = (
            float(signal["mean_reversion_reference_price"]) / entry - 1
        )
        if not runtime._cost_floor(expected_gross_move_fraction):
            raise DenseStrategyPluginError(
                "live SPY RSI(2) expected move is below the frozen cost floor"
            )
    hold = int(signal["holding_trading_days"])
    exit_plan = dict(signal["exit_plan"])
    if "target_r" in signal:
        exit_plan["target_price"] = entry + float(signal["target_r"]) * (
            entry - stop
        )
    return {
        "strategy_id": winner["strategy_id"],
        "strategy_version": winner["strategy_version"],
        "rules_hash": winner["rules_hash"],
        "historical_semantics_sha256": winner["rules_hash"],
        "ranking_complete": True,
        "observed_at": quote["observed_at"],
        "symbol": signal["symbol"],
        "rank": signal["rank"],
        "score": signal["score"],
        "halted": quote["halted"],
        "tradable": quote["tradable"],
        "bid": quote["bid"],
        "ask": quote["ask"],
        "entry_limit": entry,
        "stop_price": stop,
        "expected_gross_move_fraction": expected_gross_move_fraction,
        "holding_trading_days": hold,
        **dict(operational),
        "protection_time_in_force": (
            "gtc"
            if hold > 1 or signal.get("overnight_hold") is True
            else "day"
        ),
        "executable_ask_depth": quote["executable_ask_depth"],
        "recent_real_minute_volume": quote["recent_real_minute_volume"],
        "exit_plan": exit_plan,
    }

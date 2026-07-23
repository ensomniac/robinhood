"""Discovery adapter for the liquid-equity cross-sectional momentum successor."""

from __future__ import annotations

import gzip
import json
import subprocess
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import cross_sectional_momentum_stage0 as source_strategy
import dense_strategy_plugin
import dense_strategy_runtime as runtime
import equity_gap_continuation_validation as universe_source
import portfolio_maturity
from historical_store import (
    DEFAULT_ENV_PATH,
    HistoricalStoreConfig,
    HistoricalStoreError,
    sha256_file,
)
from learning_data import LearningDataError, load_frozen_dataset_contract


PROJECT_ROOT = Path(__file__).resolve().parent
FAMILY_ID = runtime.LIQUID_EQUITY_MOMENTUM_FAMILY
SOURCE_RELATIVE_PATH = (
    "_derived/cross_sectional_momentum_stage0/"
    "dataset-cross-sectional-momentum-stage0-2026-07-21-v1/daily-bars.json.gz"
)
CONFIRMATION_MANIFEST_ROOT = (
    PROJECT_ROOT
    / "strategy_tournament/v2/discovery"
    / FAMILY_ID
    / "confirmation-dataset"
)


class LiquidEquityMomentumPluginError(RuntimeError):
    """A source binding or exact strategy transition is incomplete."""


def _manifest_path(value: Any) -> Path:
    if not isinstance(value, str) or not value:
        raise LiquidEquityMomentumPluginError("dataset manifest path is missing")
    path = Path(value)
    return path if path.is_absolute() else PROJECT_ROOT / path


def _require_committed(path: Path) -> None:
    try:
        relative = str(path.resolve().relative_to(PROJECT_ROOT.resolve()))
    except ValueError as exc:
        raise LiquidEquityMomentumPluginError(
            "dataset manifest escaped the repository"
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
        raise LiquidEquityMomentumPluginError(
            "dataset manifest must be committed and unchanged"
        )


def _load_manifest(path: Path) -> dict[str, Any]:
    try:
        return load_frozen_dataset_contract(path)
    except (LearningDataError, OSError) as exc:
        raise LiquidEquityMomentumPluginError(
            f"dataset manifest is invalid: {exc}"
        ) from exc


def _binding(manifest: Mapping[str, Any]) -> dict[str, Any]:
    raw = manifest["dataset_payload"].get("liquid_equity_momentum_source")
    if not isinstance(raw, Mapping):
        raise LiquidEquityMomentumPluginError(
            "dataset lacks the liquid-equity source binding"
        )
    binding = dict(raw)
    required_text = (
        "family_id",
        "external_relative_path",
        "external_file_sha256",
        "format",
    )
    if any(
        not isinstance(binding.get(field), str) or not binding[field]
        for field in required_text
    ):
        raise LiquidEquityMomentumPluginError(
            "liquid-equity source binding is incomplete"
        )
    if (
        binding["family_id"] != FAMILY_ID
        or binding["external_relative_path"] != SOURCE_RELATIVE_PATH
        or binding["format"] != "json.gz"
        or len(binding["external_file_sha256"]) != 64
    ):
        raise LiquidEquityMomentumPluginError(
            "liquid-equity source identity drifted"
        )
    signal_dates = binding.get("signal_dates")
    if (
        not isinstance(signal_dates, list)
        or not signal_dates
        or signal_dates != sorted(set(map(str, signal_dates)))
    ):
        raise LiquidEquityMomentumPluginError(
            "liquid-equity signal dates are invalid"
        )
    capacity = binding.get("formal_capacity")
    if isinstance(capacity, bool) or not isinstance(capacity, int) or capacity < 0:
        raise LiquidEquityMomentumPluginError(
            "liquid-equity formal capacity is invalid"
        )
    return binding


def _point_in_time_universe(
    decision_dates: Sequence[str],
) -> tuple[dict[str, list[str]], dict[str, dict[str, str]]]:
    source_dates, details, _bindings = universe_source._source_graph()
    if not set(decision_dates).issubset(source_dates):
        raise LiquidEquityMomentumPluginError(
            "signal dates escaped the inspected point-in-time source"
        )
    universe: dict[str, list[str]] = {}
    identities: dict[str, dict[str, str]] = {}
    for day in decision_dates:
        symbols: list[str] = []
        by_symbol: dict[str, str] = {}
        for raw in details[day]["evaluations"]:
            if not isinstance(raw, Mapping):
                raise LiquidEquityMomentumPluginError(
                    f"{day}: point-in-time universe row is malformed"
                )
            symbol = str(raw.get("symbol", "")).strip().upper()
            exchange = str(raw.get("primary_exchange", ""))
            identity = str(raw.get("instrument_id", ""))
            if exchange not in {"XNAS", "XNYS"} or not symbol:
                continue
            if symbol in by_symbol or not identity:
                raise LiquidEquityMomentumPluginError(
                    f"{day}: duplicate or missing listing identity"
                )
            symbols.append(symbol)
            by_symbol[symbol] = identity
        symbols.sort()
        if len(symbols) < 500:
            raise LiquidEquityMomentumPluginError(
                f"{day}: point-in-time common-stock universe is too small"
            )
        universe[day] = symbols
        identities[day] = by_symbol
    return universe, identities


def _split_execution_dates() -> dict[str, list[str]]:
    return {
        symbol: sorted(
            {
                row["execution_date"].isoformat()
                for row in rows
                if row.get("execution_date") is not None
            }
        )
        for symbol, rows in source_strategy._merged_split_actions().items()
    }


def _session_dates() -> list[str]:
    try:
        raw = json.loads(source_strategy.CALENDAR_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise LiquidEquityMomentumPluginError(
            "frozen session calendar is unavailable"
        ) from exc
    if not isinstance(raw, list):
        raise LiquidEquityMomentumPluginError(
            "frozen session calendar is malformed"
        )
    dates = [
        str(row.get("date"))
        for row in raw
        if isinstance(row, Mapping)
        and "2024-12-16" <= str(row.get("date")) <= "2025-12-22"
    ]
    if dates != sorted(set(dates)):
        raise LiquidEquityMomentumPluginError(
            "frozen session calendar is not chronological"
        )
    return dates


def _load_dataset(
    manifest_path: Path,
    *,
    lane: str,
    expected_dates: Sequence[str],
    expected_signal_dates: Sequence[str],
    preregistration_sha256: str | None = None,
) -> dict[str, Any]:
    _require_committed(manifest_path)
    manifest = _load_manifest(manifest_path)
    if manifest.get("requested_dates") != list(expected_dates):
        raise LiquidEquityMomentumPluginError(
            "dataset account dates drifted from the frozen partition"
        )
    payload = manifest["dataset_payload"]
    if payload.get("lane") != lane or payload.get("inspected") is not True:
        raise LiquidEquityMomentumPluginError(
            "dataset lane is wrong or uninspected"
        )
    if (
        lane == "confirmation"
        and payload.get("preregistration_sha256") != preregistration_sha256
    ):
        raise LiquidEquityMomentumPluginError(
            "confirmation data is not winner-bound"
        )
    binding = _binding(manifest)
    if binding["signal_dates"] != list(expected_signal_dates):
        raise LiquidEquityMomentumPluginError(
            "dataset signal dates drifted from the frozen partition"
        )
    try:
        store = HistoricalStoreConfig.from_env(DEFAULT_ENV_PATH)
    except HistoricalStoreError as exc:
        raise LiquidEquityMomentumPluginError(str(exc)) from exc
    path = (store.root / binding["external_relative_path"]).resolve()
    if store.root.resolve() not in path.parents:
        raise LiquidEquityMomentumPluginError(
            "external dataset escaped the historical store"
        )
    if not path.is_file() or sha256_file(path) != binding["external_file_sha256"]:
        raise LiquidEquityMomentumPluginError(
            "external liquid-equity dataset is missing or drifted"
        )
    try:
        with gzip.open(path, "rt", encoding="utf-8") as source:
            source_data = json.load(source)
    except (OSError, json.JSONDecodeError) as exc:
        raise LiquidEquityMomentumPluginError(
            "external liquid-equity dataset is unreadable"
        ) from exc
    if not (
        isinstance(source_data, Mapping)
        and source_data.get("dataset_id")
        == "dataset-cross-sectional-momentum-stage0-2026-07-21-v1"
        and source_data.get("symbols")
        == sorted(source_data.get("rows_by_symbol", {}))
        and source_data.get("adjustment") == "raw"
        and source_data.get("timeframe") == "1Day"
    ):
        raise LiquidEquityMomentumPluginError(
            "external liquid-equity dataset scope drifted"
        )
    universe, identities = _point_in_time_universe(expected_signal_dates)
    dataset = {
        "family_id": FAMILY_ID,
        "evaluation_dates": list(expected_dates),
        "session_dates": _session_dates(),
        "universe_by_date": universe,
        "universe_identity_by_date": identities,
        "split_execution_dates_by_symbol": _split_execution_dates(),
        "daily_bars": dict(source_data["rows_by_symbol"]),
    }
    return runtime.prepare_dataset(dataset)


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
    """Inspect committed metadata without opening the external price file."""

    manifest_path = _manifest_path(
        contract.get("capacity_manifest", contract.get("dataset_manifest"))
    )
    _require_committed(manifest_path)
    manifest = _load_manifest(manifest_path)
    binding = _binding(manifest)
    checks = {
        "development_lane": manifest["dataset_payload"].get("lane")
        == "development",
        "family_bound": binding["family_id"] == contract.get("family_id"),
        "account_dates_bound": manifest.get("requested_dates")
        == list(contract.get("development_dates", [])),
        "signal_dates_bound": binding["signal_dates"]
        == list(contract.get("development_signal_dates", [])),
        "point_in_time": manifest["dataset_payload"].get(
            "point_in_time_evidence"
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
        "provider_telemetry": _telemetry(dataset_loads=0),
    }


def evaluate_development(
    contract: Mapping[str, Any], trials: Sequence[Mapping[str, Any]]
) -> dict[str, Any]:
    dataset = _load_dataset(
        _manifest_path(contract.get("dataset_manifest")),
        lane="development",
        expected_dates=contract["development_dates"],
        expected_signal_dates=contract["development_signal_dates"],
    )
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
        "provider_telemetry": _telemetry(dataset_loads=1),
    }


def evaluate_confirmation(winner: Mapping[str, Any]) -> dict[str, Any]:
    paths = sorted(CONFIRMATION_MANIFEST_ROOT.glob("dataset-*.json"))
    if len(paths) != 1:
        raise LiquidEquityMomentumPluginError(
            "expected exactly one frozen confirmation dataset manifest"
        )
    dataset = _load_dataset(
        paths[0],
        lane="confirmation",
        expected_dates=winner["confirmation_dates"],
        expected_signal_dates=winner["confirmation_signal_dates"],
        preregistration_sha256=str(winner["rules_hash"]),
    )
    exact = runtime.evaluate_trial(
        dataset,
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
        "dataset_manifest": str(paths[0].relative_to(PROJECT_ROOT)),
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
    return dense_strategy_plugin.evaluate_production(winner, market_facts)

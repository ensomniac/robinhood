"""Discovery adapter for the liquid close-to-open ETF momentum successor."""

from __future__ import annotations

import gzip
import json
import subprocess
from collections.abc import Mapping, Sequence
from datetime import datetime
from pathlib import Path
from typing import Any

import close_to_open_etf_momentum_stage0 as source
import dense_strategy_plugin
import dense_strategy_runtime as runtime
import etf_or_momentum_stage0 as source_hash
import portfolio_maturity
from historical_store import (
    DEFAULT_ENV_PATH,
    HistoricalStoreConfig,
    HistoricalStoreError,
    sha256_file,
)
from learning_data import LearningDataError, load_frozen_dataset_contract


PROJECT_ROOT = Path(__file__).resolve().parent
TARGET_FAMILY_ID = runtime.ETF_CLOSE_TO_OPEN_FAMILY
SOURCE_KEY = "close_to_open_runtime"


class CloseToOpenEtfPluginError(RuntimeError):
    """The frozen ETF source or exact-strategy transition is invalid."""


def _manifest_path(value: Any) -> Path:
    if not isinstance(value, str) or not value:
        raise CloseToOpenEtfPluginError("dataset manifest path is missing")
    path = Path(value)
    return path if path.is_absolute() else PROJECT_ROOT / path


def _require_committed(path: Path) -> None:
    try:
        relative = str(path.resolve().relative_to(PROJECT_ROOT.resolve()))
    except ValueError as exc:
        raise CloseToOpenEtfPluginError(
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
        raise CloseToOpenEtfPluginError(
            "dataset manifest must be committed and unchanged"
        )


def _load_manifest(path: Path) -> dict[str, Any]:
    try:
        return load_frozen_dataset_contract(path)
    except (LearningDataError, OSError) as exc:
        raise CloseToOpenEtfPluginError(
            f"dataset manifest is invalid: {exc}"
        ) from exc


def _binding(manifest: Mapping[str, Any]) -> dict[str, Any]:
    raw = manifest["dataset_payload"].get(SOURCE_KEY)
    if not isinstance(raw, Mapping):
        raise CloseToOpenEtfPluginError(
            "dataset lacks the close-to-open runtime binding"
        )
    binding = dict(raw)
    required_text = (
        "source_variant_id",
        "target_family_id",
        "external_relative_path",
        "external_file_sha256",
        "input_sha256",
        "format",
        "sample_phase",
    )
    if any(
        not isinstance(binding.get(field), str) or not binding[field]
        for field in required_text
    ):
        raise CloseToOpenEtfPluginError(
            "close-to-open runtime binding is incomplete"
        )
    if (
        binding["source_variant_id"] != source.VARIANT_ID
        or binding["target_family_id"] != TARGET_FAMILY_ID
        or binding["format"] != "json.gz"
        or binding["sample_phase"] not in {"development", "confirmation"}
        or len(binding["external_file_sha256"]) != 64
        or len(binding["input_sha256"]) != 64
    ):
        raise CloseToOpenEtfPluginError(
            "close-to-open source identity drifted"
        )
    relative = Path(binding["external_relative_path"])
    if relative.is_absolute() or ".." in relative.parts:
        raise CloseToOpenEtfPluginError(
            "close-to-open external path is unsafe"
        )
    capacity = binding.get("formal_capacity")
    if (
        binding.get("symbols") != list(source.SYMBOLS)
        or isinstance(capacity, bool)
        or not isinstance(capacity, int)
        or capacity < 0
    ):
        raise CloseToOpenEtfPluginError(
            "close-to-open universe or capacity drifted"
        )
    return binding


def _local_timestamp(value: Any) -> str:
    observed = datetime.fromisoformat(str(value))
    local = (
        observed.astimezone(source.EASTERN)
        if observed.tzinfo is not None
        else observed.replace(tzinfo=source.EASTERN)
    )
    return local.isoformat()


def _load_dataset(
    manifest_path: Path,
    *,
    lane: str,
    expected_dates: Sequence[str],
    preregistration_sha256: str | None = None,
) -> dict[str, Any]:
    _require_committed(manifest_path)
    manifest = _load_manifest(manifest_path)
    if manifest.get("requested_dates") != list(expected_dates):
        raise CloseToOpenEtfPluginError(
            "dataset dates drifted from the frozen partition"
        )
    payload = manifest["dataset_payload"]
    if (
        payload.get("lane") != lane
        or payload.get("inspected") is not True
        or payload.get("point_in_time_evidence") is not True
    ):
        raise CloseToOpenEtfPluginError(
            "dataset lane is wrong, uninspected, or not point-in-time"
        )
    binding = _binding(manifest)
    if binding["sample_phase"] != lane:
        raise CloseToOpenEtfPluginError(
            "close-to-open sample phase drifted"
        )
    if lane == "confirmation" and (
        payload.get("claim_scope")
        != "EXACT_PREREGISTERED_CONTRACT_ONLY"
        or payload.get("preregistration_sha256")
        != preregistration_sha256
        or payload.get("capture_after_preregistration_attested") is not True
    ):
        raise CloseToOpenEtfPluginError(
            "confirmation data is not exact-winner bound"
        )
    try:
        store = HistoricalStoreConfig.from_env(DEFAULT_ENV_PATH)
    except HistoricalStoreError as exc:
        raise CloseToOpenEtfPluginError(str(exc)) from exc
    path = (store.root / binding["external_relative_path"]).resolve()
    if store.root.resolve() not in path.parents:
        raise CloseToOpenEtfPluginError(
            "external close-to-open dataset escaped the historical store"
        )
    if (
        not path.is_file()
        or sha256_file(path) != binding["external_file_sha256"]
    ):
        raise CloseToOpenEtfPluginError(
            "external close-to-open dataset is missing or drifted"
        )
    try:
        with gzip.open(path, "rt", encoding="utf-8") as stream:
            graph = json.load(stream)
    except (OSError, json.JSONDecodeError) as exc:
        raise CloseToOpenEtfPluginError(
            "external close-to-open dataset is unreadable"
        ) from exc
    if (
        not isinstance(graph, dict)
        or graph.get("input_sha256")
        != source_hash._self_hash(graph, "input_sha256")
        or graph.get("input_sha256") != binding["input_sha256"]
        or graph.get("variant_id") != source.VARIANT_ID
        or graph.get("symbols") != list(source.SYMBOLS)
        or graph.get("provider") != "ibkr"
        or graph.get("feed") != "smart"
        or graph.get("adjustment") != "provider_adjusted_unknown_basis"
        or graph.get("daily_timeframe") != "1d"
        or graph.get("signal_timeframe") != "15m"
        or graph.get("provider_requests") != 0
        or graph.get("broker_actions") != 0
        or graph.get("returns_computed") != 0
    ):
        raise CloseToOpenEtfPluginError(
            "external close-to-open identity or capture contract drifted"
        )
    graph_dates = graph.get("dates")
    daily_rows = graph.get("daily_rows_by_symbol")
    intraday_rows = graph.get("intraday_rows_by_symbol")
    if (
        not isinstance(graph_dates, list)
        or not isinstance(daily_rows, Mapping)
        or not isinstance(intraday_rows, Mapping)
        or not set(expected_dates).issubset(set(graph_dates))
        or set(daily_rows) != set(source.SYMBOLS)
        or set(intraday_rows) != set(source.SYMBOLS)
    ):
        raise CloseToOpenEtfPluginError(
            "external close-to-open scope is incomplete"
        )
    positions = {day: index for index, day in enumerate(graph_dates)}
    history_count = positions[max(expected_dates)] + 1
    daily_bars: dict[str, list[dict[str, Any]]] = {}
    fifteen: dict[str, dict[str, list[dict[str, Any]]]] = {
        day: {} for day in expected_dates
    }
    for symbol in source.SYMBOLS:
        raw_daily = daily_rows[symbol]
        raw_intraday = intraday_rows[symbol]
        if (
            not isinstance(raw_daily, list)
            or not isinstance(raw_intraday, list)
            or len(raw_daily) != len(graph_dates)
            or len(raw_intraday) != len(graph_dates)
        ):
            raise CloseToOpenEtfPluginError(
                f"{symbol} source denominator drifted"
            )
        bars = []
        for day, raw_row in zip(
            graph_dates[:history_count],
            raw_daily[:history_count],
            strict=True,
        ):
            row = source.daily._validate_daily_row(
                raw_row, symbol, day
            )
            bars.append(
                {
                    "date": day,
                    "open": row["o"],
                    "high": row["h"],
                    "low": row["l"],
                    "close": row["c"],
                    "volume": row["v"],
                }
            )
        daily_bars[symbol] = bars
        for day in expected_dates:
            rows = source._validate_intraday_rows(
                raw_intraday[positions[day]], symbol, day
            )
            fifteen[day][symbol] = [
                {
                    "timestamp": _local_timestamp(row["t"]),
                    "open": row["o"],
                    "high": row["h"],
                    "low": row["l"],
                    "close": row["c"],
                    "volume": row["v"],
                }
                for row in rows
            ]
    return runtime.prepare_dataset(
        {
            "family_id": TARGET_FAMILY_ID,
            "evaluation_dates": list(expected_dates),
            "symbols": list(source.SYMBOLS),
            "daily_bars": daily_bars,
            "fifteen_minute_bars": fifteen,
        }
    )


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
    """Inspect committed capacity metadata without opening price rows."""

    manifest_path = _manifest_path(
        contract.get("capacity_manifest", contract.get("dataset_manifest"))
    )
    _require_committed(manifest_path)
    manifest = _load_manifest(manifest_path)
    binding = _binding(manifest)
    checks = {
        "development_lane": manifest["dataset_payload"].get("lane")
        == "development",
        "development_phase": binding["sample_phase"] == "development",
        "family_bound": binding["target_family_id"]
        == contract.get("family_id"),
        "dates_bound": manifest.get("requested_dates")
        == list(contract.get("development_dates", [])),
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
    )
    policy = _account_policy()
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
        "provider_telemetry": _telemetry(dataset_loads=1),
    }


def evaluate_confirmation(winner: Mapping[str, Any]) -> dict[str, Any]:
    dataset = _load_dataset(
        _manifest_path(winner.get("confirmation_dataset_manifest")),
        lane="confirmation",
        expected_dates=winner["confirmation_dates"],
        preregistration_sha256=str(winner["rules_hash"]),
    )
    exact = runtime.evaluate_trial(
        dataset,
        family_id=TARGET_FAMILY_ID,
        trial_id=str(winner["exact_rules"]["selected_trial_id"]),
        parameters=winner["exact_rules"]["parameters"],
        account_policy=_account_policy(),
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
        "provider_telemetry": _telemetry(dataset_loads=1),
    }


def evaluate_production(
    winner: Mapping[str, Any], market_facts: Mapping[str, Any]
) -> dict[str, Any]:
    return dense_strategy_plugin.evaluate_production(winner, market_facts)

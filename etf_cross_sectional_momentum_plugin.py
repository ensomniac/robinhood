"""Discovery adapter for the liquid-index-ETF cross-sectional momentum successor."""

from __future__ import annotations

import gzip
import json
import subprocess
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import dense_strategy_plugin
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
SOURCE_FAMILY_ID = runtime.ETF_PULLBACK_FAMILY
TARGET_FAMILY_ID = runtime.ETF_CROSS_SECTIONAL_MOMENTUM_FAMILY


class EtfCrossSectionalMomentumPluginError(RuntimeError):
    """A source binding or exact strategy transition is incomplete."""


def _manifest_path(value: Any) -> Path:
    if not isinstance(value, str) or not value:
        raise EtfCrossSectionalMomentumPluginError("dataset manifest path is missing")
    path = Path(value)
    return path if path.is_absolute() else PROJECT_ROOT / path


def _require_committed(path: Path) -> None:
    try:
        relative = str(path.resolve().relative_to(PROJECT_ROOT.resolve()))
    except ValueError as exc:
        raise EtfCrossSectionalMomentumPluginError(
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
        raise EtfCrossSectionalMomentumPluginError(
            "dataset manifest must be committed and unchanged"
        )


def _load_manifest(path: Path) -> dict[str, Any]:
    try:
        return load_frozen_dataset_contract(path)
    except (LearningDataError, OSError) as exc:
        raise EtfCrossSectionalMomentumPluginError(
            f"dataset manifest is invalid: {exc}"
        ) from exc


def _binding(
    manifest: Mapping[str, Any],
    *,
    source_key: str = "etf_cross_sectional_momentum_source",
    target_family_id: str = TARGET_FAMILY_ID,
) -> dict[str, Any]:
    raw = manifest["dataset_payload"].get(source_key)
    if not isinstance(raw, Mapping):
        raise EtfCrossSectionalMomentumPluginError(
            "dataset lacks the translated ETF source binding"
        )
    binding = dict(raw)
    required_text = (
        "source_family_id",
        "target_family_id",
        "external_relative_path",
        "external_file_sha256",
        "dataset_sha256",
        "format",
    )
    if any(not isinstance(binding.get(field), str) or not binding[field] for field in required_text):
        raise EtfCrossSectionalMomentumPluginError(
            "ETF momentum source binding is incomplete"
        )
    if (
        binding["source_family_id"] not in {SOURCE_FAMILY_ID, target_family_id}
        or binding["target_family_id"] != target_family_id
        or binding["format"] not in {"json", "json.gz"}
        or len(binding["external_file_sha256"]) != 64
        or len(binding["dataset_sha256"]) != 64
    ):
        raise EtfCrossSectionalMomentumPluginError(
            "ETF momentum source identity drifted"
        )
    relative = Path(binding["external_relative_path"])
    if relative.is_absolute() or ".." in relative.parts:
        raise EtfCrossSectionalMomentumPluginError(
            "ETF momentum external path is unsafe"
        )
    capacity = binding.get("formal_capacity")
    if isinstance(capacity, bool) or not isinstance(capacity, int) or capacity < 0:
        raise EtfCrossSectionalMomentumPluginError(
            "ETF momentum formal capacity is invalid"
        )
    return binding


def _load_dataset(
    manifest_path: Path,
    *,
    lane: str,
    expected_dates: Sequence[str],
    preregistration_sha256: str | None = None,
    source_key: str = "etf_cross_sectional_momentum_source",
    target_family_id: str = TARGET_FAMILY_ID,
) -> dict[str, Any]:
    _require_committed(manifest_path)
    manifest = _load_manifest(manifest_path)
    if manifest.get("requested_dates") != list(expected_dates):
        raise EtfCrossSectionalMomentumPluginError(
            "dataset dates drifted from the frozen partition"
        )
    payload = manifest["dataset_payload"]
    if payload.get("lane") != lane or payload.get("inspected") is not True:
        raise EtfCrossSectionalMomentumPluginError(
            "dataset lane is wrong or uninspected"
        )
    if (
        lane == "confirmation"
        and payload.get("preregistration_sha256") != preregistration_sha256
    ):
        raise EtfCrossSectionalMomentumPluginError(
            "confirmation data is not winner-bound"
        )
    binding = _binding(
        manifest,
        source_key=source_key,
        target_family_id=target_family_id,
    )
    try:
        store = HistoricalStoreConfig.from_env(DEFAULT_ENV_PATH)
    except HistoricalStoreError as exc:
        raise EtfCrossSectionalMomentumPluginError(str(exc)) from exc
    path = (store.root / binding["external_relative_path"]).resolve()
    if store.root.resolve() not in path.parents:
        raise EtfCrossSectionalMomentumPluginError(
            "external dataset escaped the historical store"
        )
    if not path.is_file() or sha256_file(path) != binding["external_file_sha256"]:
        raise EtfCrossSectionalMomentumPluginError(
            "external ETF momentum dataset is missing or drifted"
        )
    try:
        if binding["format"] == "json.gz":
            with gzip.open(path, "rt", encoding="utf-8") as source:
                dataset = json.load(source)
        else:
            dataset = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise EtfCrossSectionalMomentumPluginError(
            "external ETF momentum dataset is unreadable"
        ) from exc
    if (
        not isinstance(dataset, dict)
        or canonical_sha256(dataset) != binding["dataset_sha256"]
        or dataset.get("family_id") != binding["source_family_id"]
        or dataset.get("evaluation_dates") != list(expected_dates)
        or dataset.get("symbols") != ["DIA", "IWM", "QQQ", "SPY"]
    ):
        raise EtfCrossSectionalMomentumPluginError(
            "external ETF momentum dataset scope drifted"
        )
    translated = dict(dataset)
    translated["family_id"] = target_family_id
    return runtime.prepare_dataset(translated)


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
    """Inspect committed metadata without opening the external bar dataset."""

    manifest_path = _manifest_path(
        contract.get("capacity_manifest", contract.get("dataset_manifest"))
    )
    _require_committed(manifest_path)
    manifest = _load_manifest(manifest_path)
    binding = _binding(manifest)
    checks = {
        "development_lane": manifest["dataset_payload"].get("lane") == "development",
        "family_bound": binding["target_family_id"] == contract.get("family_id"),
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
        "provider_telemetry": _telemetry(dataset_loads=0),
    }


def evaluate_development(
    contract: Mapping[str, Any], trials: Sequence[Mapping[str, Any]]
) -> dict[str, Any]:
    manifest_path = _manifest_path(contract.get("dataset_manifest"))
    dataset = _load_dataset(
        manifest_path,
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
    manifest_path = _manifest_path(winner.get("confirmation_dataset_manifest"))
    dataset = _load_dataset(
        manifest_path,
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

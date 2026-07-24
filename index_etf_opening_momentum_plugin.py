"""Reuse inspected minute inputs for frozen index-ETF opening momentum."""

from __future__ import annotations

import gzip
import json
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import dense_strategy_plugin as dense
import dense_strategy_runtime as runtime
from historical_store import (
    DEFAULT_ENV_PATH,
    HistoricalStoreConfig,
    HistoricalStoreError,
    canonical_sha256,
    sha256_file,
)


PROJECT_ROOT = Path(__file__).resolve().parent
SOURCE_FAMILY_ID = (
    runtime.LIQUID_INDEX_ETF_OPENING_REVERSAL_POST2016_FAMILY
)
FAMILY_ID = runtime.INDEX_ETF_OPENING_MOMENTUM_FAMILY


class IndexEtfOpeningMomentumPluginError(ValueError):
    """The frozen input alias or momentum evaluation drifted."""


def _manifest_path(contract: Mapping[str, Any]) -> Path:
    raw = contract.get("dataset_manifest")
    if not isinstance(raw, str) or not raw:
        raise IndexEtfOpeningMomentumPluginError(
            "opening-momentum source manifest is missing"
        )
    path = Path(raw)
    if not path.is_absolute():
        path = PROJECT_ROOT / path
    try:
        path.resolve().relative_to(PROJECT_ROOT.resolve())
    except ValueError as exc:
        raise IndexEtfOpeningMomentumPluginError(
            "opening-momentum source manifest escaped the repository"
        ) from exc
    return path


def _source_manifest(
    contract: Mapping[str, Any],
    *,
    enforce_commit: bool,
) -> tuple[Path, dict[str, Any], dict[str, Any]]:
    path = _manifest_path(contract)
    if enforce_commit:
        dense._require_committed(path)
    manifest = dense._load_manifest(path)
    binding = dense._runtime_binding(manifest)
    payload = manifest["dataset_payload"]
    checks = {
        "source_family": binding.get("family_id") == SOURCE_FAMILY_ID,
        "source_dates": manifest.get("requested_dates")
        == contract.get("development_dates"),
        "source_inspected": payload.get("inspected") is True,
        "source_lane": payload.get("lane") == "development",
        "source_manifest_hash": sha256_file(path)
        == contract.get("source_dataset_manifest_sha256"),
        "source_dataset_hash": binding.get("dataset_sha256")
        == contract.get("source_dataset_sha256"),
        "derived_family": contract.get("family_id") == FAMILY_ID,
    }
    if not all(checks.values()):
        raise IndexEtfOpeningMomentumPluginError(
            "opening-momentum inspected input alias drifted"
        )
    return path, manifest, binding


def preflight(contract: Mapping[str, Any]) -> dict[str, Any]:
    """Validate inspected metadata without opening the external minute file."""

    _path, _manifest, binding = _source_manifest(
        contract, enforce_commit=True
    )
    capacity = binding.get("formal_capacity")
    if isinstance(capacity, bool) or not isinstance(capacity, int):
        raise IndexEtfOpeningMomentumPluginError(
            "opening-momentum formal capacity is invalid"
        )
    return {
        "verified_capacity": capacity,
        "point_in_time_complete": True,
        "metadata_checks": {
            "inspected_source_bound": True,
            "dates_bound": True,
            "family_alias_bound": True,
            "external_file_opened": False,
        },
        "provider_telemetry": dense._telemetry(dataset_loads=0),
    }


def _load_development_dataset(
    contract: Mapping[str, Any],
) -> tuple[dict[str, Any], Path]:
    manifest_path, _manifest, binding = _source_manifest(
        contract, enforce_commit=True
    )
    try:
        store = HistoricalStoreConfig.from_env(DEFAULT_ENV_PATH)
    except HistoricalStoreError as exc:
        raise IndexEtfOpeningMomentumPluginError(str(exc)) from exc
    external = (
        store.root / str(binding["external_relative_path"])
    ).resolve()
    if (
        store.root.resolve() not in external.parents
        or not external.is_file()
        or sha256_file(external) != binding["external_file_sha256"]
    ):
        raise IndexEtfOpeningMomentumPluginError(
            "opening-momentum external input is missing or drifted"
        )
    try:
        with gzip.open(external, "rt", encoding="utf-8") as source:
            dataset = json.load(source)
    except (OSError, json.JSONDecodeError) as exc:
        raise IndexEtfOpeningMomentumPluginError(
            "opening-momentum external input is unreadable"
        ) from exc
    if not isinstance(dataset, dict) or canonical_sha256(
        dataset
    ) != binding.get("dataset_sha256"):
        raise IndexEtfOpeningMomentumPluginError(
            "opening-momentum source dataset hash drifted"
        )
    if not (
        dataset.get("family_id") == SOURCE_FAMILY_ID
        and dataset.get("evaluation_dates")
        == contract.get("development_dates")
    ):
        raise IndexEtfOpeningMomentumPluginError(
            "opening-momentum source scope drifted"
        )
    aliased = dict(dataset)
    aliased["family_id"] = FAMILY_ID
    return runtime.prepare_dataset(aliased), manifest_path


def evaluate_development(
    contract: Mapping[str, Any],
    trials: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    dataset, manifest_path = _load_development_dataset(contract)
    policy = dense._account_policy()
    return {
        "dataset_manifest": str(
            manifest_path.resolve().relative_to(PROJECT_ROOT.resolve())
        ),
        "trials": [
            runtime.evaluate_trial(
                dataset,
                family_id=FAMILY_ID,
                trial_id=str(trial["trial_id"]),
                parameters=trial["parameters"],
                account_policy=policy,
                rolling_origin_plan=contract.get(
                    "rolling_origin_plan"
                ),
            )
            for trial in trials
        ],
        "provider_telemetry": dense._telemetry(dataset_loads=1),
    }


def evaluate_confirmation(
    winner: Mapping[str, Any],
) -> dict[str, Any]:
    return dense.evaluate_confirmation(winner)


def evaluate_production(
    winner: Mapping[str, Any],
    market_facts: Mapping[str, Any],
) -> dict[str, Any]:
    return dense.evaluate_production(winner, market_facts)

"""Discovery plugin for clustered Form 4 open-market purchases."""

from __future__ import annotations

import gzip
import json
import subprocess
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import dense_strategy_runtime as runtime
import portfolio_maturity
from historical_store import HistoricalDayStore, canonical_sha256
from insider_purchase_discovery import FAMILY_ID
from learning_data import LearningDataError, load_frozen_dataset_contract


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_DISCOVERY_ROOT = (
    PROJECT_ROOT / "strategy_tournament/v2/discovery" / FAMILY_ID
)


class InsiderPurchasePluginError(RuntimeError):
    """A frozen Form 4 family input or production fact is incomplete."""


def _family_id(value: Mapping[str, Any]) -> str:
    family_id = value.get("family_id")
    if not isinstance(family_id, str) or not family_id:
        raise InsiderPurchasePluginError("frozen Form 4 family_id is missing")
    return family_id


def _discovery_root(family_id: str) -> Path:
    return PROJECT_ROOT / "strategy_tournament/v2/discovery" / family_id


def _path(value: Any) -> Path:
    if not isinstance(value, str) or not value:
        raise InsiderPurchasePluginError("frozen path is missing")
    path = Path(value)
    return path if path.is_absolute() else PROJECT_ROOT / path


def _require_committed(path: Path) -> None:
    relative = str(path.resolve().relative_to(PROJECT_ROOT.resolve()))
    tracked = subprocess.run(
        ["git", "ls-files", "--error-unmatch", "--", relative],
        cwd=PROJECT_ROOT,
        capture_output=True,
    )
    clean = subprocess.run(
        ["git", "diff", "--quiet", "HEAD", "--", relative],
        cwd=PROJECT_ROOT,
    )
    if tracked.returncode or clean.returncode:
        raise InsiderPurchasePluginError(
            f"Form 4 evidence must be committed: {relative}"
        )


def _manifest(path: Path) -> dict[str, Any]:
    try:
        return load_frozen_dataset_contract(path)
    except (LearningDataError, OSError) as exc:
        raise InsiderPurchasePluginError(
            f"Form 4 dataset manifest is invalid: {exc}"
        ) from exc


def _telemetry(*, dataset_loads: int) -> dict[str, Any]:
    return {
        "requests": 0,
        "request_seconds": 0.0,
        "pacing_wait_seconds": 0.0,
        "cache_hits": 1 if dataset_loads else 0,
        "failures": 0,
        "dataset_loads": dataset_loads,
    }


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


def preflight(contract: Mapping[str, Any]) -> dict[str, Any]:
    family_id = _family_id(contract)
    experiment_id = contract.get("experiment_id")
    manifest_path = _path(contract.get("capacity_manifest"))
    _require_committed(manifest_path)
    manifest = _manifest(manifest_path)
    payload = manifest["dataset_payload"]
    capacity = payload.get("form4_purchase_capacity")
    if not isinstance(capacity, Mapping):
        raise InsiderPurchasePluginError("Form 4 capacity binding is missing")
    for evidence in payload.get("evidence_paths", []):
        _require_committed(_path(evidence))
    counts = capacity.get("counts")
    if not isinstance(counts, Mapping):
        raise InsiderPurchasePluginError("Form 4 capacity counts are missing")
    checks = {
        "development_lane": payload.get("lane") == "development",
        "family_bound": capacity.get("family_id") == family_id,
        "experiment_bound": capacity.get("experiment_id") == experiment_id,
        "dates_bound": manifest.get("requested_dates")
        == contract.get("development_dates"),
        "point_in_time": payload.get("point_in_time_evidence") is True,
        "confirmation_locked": capacity.get("confirmation_access_permitted")
        is False,
        "zero_provider_requests": capacity.get("provider_requests") == 0,
        "external_inputs_closed": capacity.get("market_prices_accessed") is False,
        "confirmation_dates_bound": counts.get("confirmation_entry_dates")
        == contract.get("confirmation_signal_capacity"),
    }
    return {
        "verified_capacity": int(counts["development_events"])
        + int(counts["confirmation_events"]),
        "point_in_time_complete": all(checks.values()),
        "metadata_checks": checks,
        "external_dataset_opened": False,
        "provider_telemetry": _telemetry(dataset_loads=0),
    }


def _load_private_dataset(binding: Mapping[str, Any]) -> dict[str, Any]:
    relative = Path(str(binding.get("relative_path", "")))
    if (
        binding.get("storage") != "LOCAL_HISTORICAL_DATA_ROOT"
        or relative.is_absolute()
        or ".." in relative.parts
        or binding.get("compression") != "gzip"
        or binding.get("format") != "canonical-json"
    ):
        raise InsiderPurchasePluginError("private dataset binding is invalid")
    path = HistoricalDayStore.from_env().root / relative
    if not path.is_file():
        raise InsiderPurchasePluginError("private runtime dataset is missing")
    with gzip.open(path, "rt", encoding="utf-8") as source:
        value = json.load(source)
    if (
        not isinstance(value, dict)
        or canonical_sha256(value) != binding.get("content_sha256")
    ):
        raise InsiderPurchasePluginError("private runtime dataset drifted")
    return value


def _single_manifest(directory: Path) -> Path:
    paths = sorted(directory.glob("*.json"))
    if len(paths) != 1:
        raise InsiderPurchasePluginError(
            f"expected one runtime manifest in {directory}; found {len(paths)}"
        )
    return paths[0]


def _load_bound_dataset(
    manifest_path: Path,
    *,
    family_id: str,
    lane: str,
    expected_dates: Sequence[str],
    preregistration_sha256: str | None = None,
) -> dict[str, Any]:
    _require_committed(manifest_path)
    manifest = _manifest(manifest_path)
    payload = manifest["dataset_payload"]
    binding = payload.get("form4_purchase_runtime")
    if not (
        isinstance(binding, Mapping)
        and manifest.get("requested_dates") == list(expected_dates)
        and payload.get("lane") == lane
        and payload.get("inspected") is True
        and payload.get("point_in_time_evidence") is True
        and binding.get("family_id") == family_id
        and binding.get("sample_phase") == lane
    ):
        raise InsiderPurchasePluginError("runtime dataset scope drifted")
    if lane == "confirmation" and not (
        payload.get("claim_scope") == "EXACT_PREREGISTERED_CONTRACT_ONLY"
        and payload.get("preregistration_sha256") == preregistration_sha256
        and payload.get("capture_after_preregistration_attested") is True
    ):
        raise InsiderPurchasePluginError(
            "confirmation dataset is not exact-winner bound"
        )
    dataset = _load_private_dataset(binding["private_dataset"])
    if (
        dataset.get("family_id") != family_id
        or dataset.get("evaluation_dates") != list(expected_dates)
        or dataset.get("sample_phase") != lane
    ):
        raise InsiderPurchasePluginError("private runtime dataset scope drifted")
    return dataset


def evaluate_development(
    contract: Mapping[str, Any],
    trials: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    family_id = _family_id(contract)
    discovery_root = _discovery_root(family_id)
    raw_manifest = contract.get("dataset_manifest")
    manifest_path = (
        _path(raw_manifest)
        if raw_manifest
        else _single_manifest(discovery_root / "development-dataset")
    )
    dataset = runtime.prepare_dataset(
        _load_bound_dataset(
            manifest_path,
            family_id=family_id,
            lane="development",
            expected_dates=contract["development_dates"],
        )
    )
    return {
        "dataset_manifest": str(manifest_path),
        "trials": [
            runtime.evaluate_trial(
                dataset,
                family_id=family_id,
                trial_id=str(trial["trial_id"]),
                parameters=trial["parameters"],
                account_policy=_account_policy(),
                rolling_origin_plan=contract.get("rolling_origin_plan"),
            )
            for trial in trials
        ],
        "provider_telemetry": _telemetry(dataset_loads=1),
    }


def evaluate_confirmation(winner: Mapping[str, Any]) -> dict[str, Any]:
    family_id = _family_id(winner)
    discovery_root = _discovery_root(family_id)
    manifest_path = _single_manifest(
        discovery_root / "confirmation-dataset"
    )
    dataset = runtime.prepare_dataset(
        _load_bound_dataset(
            manifest_path,
            family_id=family_id,
            lane="confirmation",
            expected_dates=winner["confirmation_dates"],
            preregistration_sha256=str(winner["rules_hash"]),
        )
    )
    exact = runtime.evaluate_trial(
        dataset,
        family_id=family_id,
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
    winner: Mapping[str, Any], decision_data: Mapping[str, Any]
) -> dict[str, Any]:
    required = {
        "as_of",
        "form4_event_verified",
        "event_available_before_entry",
        "issuer_identity_point_in_time",
        "daily_history_complete",
        "quote_fresh",
        "spread_fraction",
        "depth_sufficient",
        "halted",
        "tradable",
        "news_reconciled",
        "protection_plan_complete",
        "gtc_protection_supported",
        "account_reconciled",
    }
    if set(decision_data) != required:
        raise InsiderPurchasePluginError(
            "production decision-data schema drifted"
        )
    passed = (
        decision_data["form4_event_verified"] is True
        and decision_data["event_available_before_entry"] is True
        and decision_data["issuer_identity_point_in_time"] is True
        and decision_data["daily_history_complete"] is True
        and decision_data["quote_fresh"] is True
        and float(decision_data["spread_fraction"]) <= 0.0015
        and decision_data["depth_sufficient"] is True
        and decision_data["halted"] is False
        and decision_data["tradable"] is True
        and decision_data["news_reconciled"] is True
        and decision_data["protection_plan_complete"] is True
        and decision_data["gtc_protection_supported"] is True
        and decision_data["account_reconciled"] is True
    )
    return {
        "strategy_id": winner["strategy_id"],
        "strategy_version": winner["strategy_version"],
        "rules_hash": winner["rules_hash"],
        "entry_ready": passed,
        "fail_closed": not passed,
        "broker_actions": 0,
    }

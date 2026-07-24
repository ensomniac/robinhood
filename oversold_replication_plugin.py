"""Strategy-discovery plugin for exact-grid oversold replication.

Development and confirmation use one generic inspected-input schema.  The
manifest binds a private point-in-time candidate inventory, a private canonical
minute-input index, and committed public inspections.  Sparse candidate
sessions remain no-signals and every evaluation date, including empty dates,
is retained in the chronological account path.
"""

from __future__ import annotations

import gzip
import json
import subprocess
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import dense_strategy_runtime as runtime
import oversold_replication_development_collection as collection
import oversold_reversal_plugin as production
import portfolio_maturity
from historical_store import (
    HistoricalDayStore,
    canonical_sha256,
    sha256_file,
)
from learning_data import LearningDataError, load_frozen_dataset_contract


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_MANIFEST_ROOT = (
    PROJECT_ROOT
    / "strategy_tournament/v2/discovery"
    / runtime.OVERSOLD_REVERSAL_FAMILY
)


class OversoldReplicationPluginError(RuntimeError):
    """A replication input, rule, or public binding is incomplete."""


def _repo_path(value: Any) -> Path:
    if not isinstance(value, str) or not value:
        raise OversoldReplicationPluginError(
            "frozen repository path is missing"
        )
    path = Path(value)
    resolved = path if path.is_absolute() else PROJECT_ROOT / path
    try:
        resolved.resolve().relative_to(PROJECT_ROOT.resolve())
    except ValueError as exc:
        raise OversoldReplicationPluginError(
            "frozen repository path escaped the repository"
        ) from exc
    return resolved


def _store_path(store: HistoricalDayStore, value: Any) -> Path:
    if not isinstance(value, str) or not value:
        raise OversoldReplicationPluginError(
            "frozen private path is missing"
        )
    path = store.root / value
    try:
        path.resolve().relative_to(store.root.resolve())
    except ValueError as exc:
        raise OversoldReplicationPluginError(
            "frozen private path escaped the historical store"
        ) from exc
    return path


def _require_committed(path: Path) -> None:
    try:
        relative = path.resolve().relative_to(PROJECT_ROOT.resolve()).as_posix()
    except ValueError as exc:
        raise OversoldReplicationPluginError(
            "replication evidence escaped the repository"
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
        raise OversoldReplicationPluginError(
            f"replication evidence must be committed: {relative}"
        )


def _read_gzip(path: Path) -> dict[str, Any]:
    try:
        with gzip.open(path, "rt", encoding="utf-8") as stream:
            value = json.load(stream)
    except (OSError, json.JSONDecodeError) as exc:
        raise OversoldReplicationPluginError(
            f"cannot read frozen private input {path}: {exc}"
        ) from exc
    if not isinstance(value, dict):
        raise OversoldReplicationPluginError(
            f"{path} must contain an object"
        )
    return value


def _manifest(path: Path) -> dict[str, Any]:
    try:
        return load_frozen_dataset_contract(path)
    except (LearningDataError, OSError) as exc:
        raise OversoldReplicationPluginError(
            f"replication dataset manifest is invalid: {exc}"
        ) from exc


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
    """Inspect only the committed capacity metadata."""

    path = _repo_path(contract.get("capacity_manifest"))
    _require_committed(path)
    manifest = _manifest(path)
    payload = manifest["dataset_payload"]
    capacity = payload.get("oversold_replication_capacity")
    if not isinstance(capacity, Mapping):
        raise OversoldReplicationPluginError(
            "replication capacity binding is missing"
        )
    evidence_paths = [
        _repo_path(item)
        for item in payload.get("evidence_paths", [])
    ]
    for evidence_path in evidence_paths:
        _require_committed(evidence_path)
    checks = {
        "development_lane": payload.get("lane") == "development",
        "family_bound": capacity.get("family_id")
        == runtime.OVERSOLD_REVERSAL_FAMILY,
        "dates_bound": manifest.get("requested_dates")
        == contract.get("development_dates"),
        "point_in_time": payload.get("point_in_time_evidence") is True,
        "development_assigned": capacity.get(
            "development_training_contaminated"
        )
        is True,
        "confirmation_locked": capacity.get(
            "confirmation_access_permitted"
        )
        is False,
        "complete_zero_days": capacity.get("zero_signal_days") == 299,
    }
    formal_capacity = capacity.get("formal_capacity")
    if (
        isinstance(formal_capacity, bool)
        or not isinstance(formal_capacity, int)
        or formal_capacity < 0
    ):
        raise OversoldReplicationPluginError(
            "replication formal capacity is invalid"
        )
    return {
        "verified_capacity": formal_capacity,
        "point_in_time_complete": all(checks.values()),
        "metadata_checks": checks,
        "external_dataset_opened": False,
        "provider_telemetry": _telemetry(dataset_loads=0),
    }


def _validate_public_bindings(
    binding: Mapping[str, Any],
) -> None:
    raw = binding.get("public_bindings")
    if not isinstance(raw, list) or not raw:
        raise OversoldReplicationPluginError(
            "public input bindings are missing"
        )
    for item in raw:
        if not isinstance(item, Mapping):
            raise OversoldReplicationPluginError(
                "public input binding is malformed"
            )
        path = _repo_path(item.get("path"))
        _require_committed(path)
        if sha256_file(path) != item.get("file_sha256"):
            raise OversoldReplicationPluginError(
                f"public input binding drifted: {path}"
            )


def _load_bound_dataset(
    manifest_path: Path,
    *,
    lane: str,
    expected_dates: Sequence[str],
    preregistration_sha256: str | None = None,
) -> dict[str, Any]:
    _require_committed(manifest_path)
    manifest = _manifest(manifest_path)
    payload = manifest["dataset_payload"]
    binding = payload.get("oversold_replication_runtime")
    if not isinstance(binding, Mapping):
        raise OversoldReplicationPluginError(
            "replication runtime binding is missing"
        )
    if not (
        manifest.get("requested_dates") == list(expected_dates)
        and payload.get("lane") == lane
        and payload.get("inspected") is True
        and payload.get("point_in_time_evidence") is True
        and binding.get("family_id")
        == runtime.OVERSOLD_REVERSAL_FAMILY
        and binding.get("sample_phase") == lane
    ):
        raise OversoldReplicationPluginError(
            "replication dataset scope drifted"
        )
    if lane == "confirmation" and not (
        payload.get("claim_scope")
        == "EXACT_PREREGISTERED_CONTRACT_ONLY"
        and payload.get("preregistration_sha256")
        == preregistration_sha256
        and payload.get("capture_after_preregistration_attested") is True
    ):
        raise OversoldReplicationPluginError(
            "confirmation dataset is not winner-bound"
        )
    _validate_public_bindings(binding)
    store = HistoricalDayStore.from_env()
    inventory_path = _store_path(
        store,
        binding.get("private_inventory_path"),
    )
    input_index_path = _store_path(
        store,
        binding.get("private_input_index_path"),
    )
    if not (
        sha256_file(inventory_path)
        == binding.get("private_inventory_file_sha256")
        and sha256_file(input_index_path)
        == binding.get("private_input_index_file_sha256")
    ):
        raise OversoldReplicationPluginError(
            "private replication input file drifted"
        )
    inventory = _read_gzip(inventory_path)
    input_index = _read_gzip(input_index_path)
    if not (
        inventory.get("content_sha256")
        == binding.get("private_inventory_content_sha256")
        and canonical_sha256(input_index)
        == binding.get("private_input_index_content_sha256")
        and inventory.get("lane") == lane
        and input_index.get("lane") == lane
        and inventory.get("evaluation_dates") == list(expected_dates)
        and input_index.get("evaluation_dates") == list(expected_dates)
        and inventory.get("target_outcomes_observed_or_derived") is False
    ):
        raise OversoldReplicationPluginError(
            "private replication input semantics drifted"
        )
    candidates_by_date = inventory.get("candidates_by_date")
    if not isinstance(candidates_by_date, Mapping) or set(
        candidates_by_date
    ) != set(expected_dates):
        raise OversoldReplicationPluginError(
            "candidate denominator is incomplete"
        )
    indexed = {
        (str(row["date"]), str(row["symbol"])): row
        for row in input_index.get("input_rows", [])
    }
    expected_pairs = {
        (day, str(row["symbol"]))
        for day in expected_dates
        for row in candidates_by_date[day]
    }
    if set(indexed) != expected_pairs:
        raise OversoldReplicationPluginError(
            "minute input index does not cover the candidate denominator"
        )
    candidate_symbols_by_date: dict[str, list[str]] = {}
    minute_bars: dict[
        str,
        dict[str, list[dict[str, Any]]],
    ] = {}
    for day in expected_dates:
        symbols = sorted(
            str(item["symbol"])
            for item in candidates_by_date[day]
        )
        candidate_symbols_by_date[day] = symbols
        minute_bars[day] = {}
        for symbol in symbols:
            frozen_input = indexed[(day, symbol)]
            dataset = collection._full_dataset(
                store,
                symbol,
                day,
            )
            if dataset is None or not (
                dataset["id"] == frozen_input["dataset_id"]
                and dataset["content_sha256"]
                == frozen_input["dataset_sha256"]
            ):
                raise OversoldReplicationPluginError(
                    f"inspected minute input drifted: {day} {symbol}"
                )
            rows, exact = collection._expanded_rows(
                dataset,
                day=day,
                symbol=symbol,
            )
            if not exact:
                continue
            minute_bars[day][symbol] = [
                {
                    "timestamp": str(row["time_et"]),
                    "open": float(row["open"]),
                    "high": float(row["high"]),
                    "low": float(row["low"]),
                    "close": float(row["close"]),
                    "volume": int(row.get("volume", 0)),
                    "vwap_numerator": (
                        (
                            float(row["high"])
                            + float(row["low"])
                            + float(row["close"])
                        )
                        / 3.0
                        * int(row.get("volume", 0))
                    ),
                    "vwap_denominator": int(
                        row.get("volume", 0)
                    ),
                }
                for row in rows
            ]
    return {
        "schema_version": 1,
        "family_id": runtime.OVERSOLD_REVERSAL_FAMILY,
        "evaluation_dates": list(expected_dates),
        "candidate_symbols_by_date": candidate_symbols_by_date,
        "regular_session_minutes_by_date": {
            day: 390 for day in expected_dates
        },
        "minute_bars": minute_bars,
        "source_semantics": {
            "universe": (
                "point-in-time U.S. common stocks opening above 5 dollars "
                "and 2-8% above prior close at 09:35 ET"
            ),
            "feed": "Alpaca SIP",
            "adjustment": "raw",
            "sparse_policy": "retained_in_denominator_no_signal",
            "empty_date_policy": "explicit_zero_return_day",
        },
    }


def evaluate_development(
    contract: Mapping[str, Any],
    trials: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    manifest_path = _repo_path(contract.get("dataset_manifest"))
    dataset = runtime.prepare_dataset(
        _load_bound_dataset(
            manifest_path,
            lane="development",
            expected_dates=contract["development_dates"],
        )
    )
    policy = _account_policy()
    return {
        "dataset_manifest": str(contract["dataset_manifest"]),
        "trials": [
            runtime.evaluate_trial(
                dataset,
                family_id=runtime.OVERSOLD_REVERSAL_FAMILY,
                trial_id=str(trial["trial_id"]),
                parameters=trial["parameters"],
                account_policy=policy,
                rolling_origin_plan=contract.get(
                    "rolling_origin_plan"
                ),
            )
            for trial in trials
        ],
        "provider_telemetry": _telemetry(dataset_loads=1),
    }


def _confirmation_manifest() -> Path:
    paths = sorted(
        (DEFAULT_MANIFEST_ROOT / "confirmation-dataset").glob("*.json")
    )
    if len(paths) != 1:
        raise OversoldReplicationPluginError(
            "expected one frozen replication confirmation manifest"
        )
    return paths[0]


def evaluate_confirmation(
    winner: Mapping[str, Any],
) -> dict[str, Any]:
    manifest_path = _confirmation_manifest()
    dataset = runtime.prepare_dataset(
        _load_bound_dataset(
            manifest_path,
            lane="confirmation",
            expected_dates=winner["confirmation_dates"],
            preregistration_sha256=str(winner["rules_hash"]),
        )
    )
    exact = runtime.evaluate_trial(
        dataset,
        family_id=runtime.OVERSOLD_REVERSAL_FAMILY,
        trial_id=str(
            winner["exact_rules"]["selected_trial_id"]
        ),
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
    winner: Mapping[str, Any],
    market_facts: Mapping[str, Any],
) -> dict[str, Any]:
    """Delegate the unchanged live semantics to the bound production evaluator."""

    return production.evaluate_production(winner, market_facts)

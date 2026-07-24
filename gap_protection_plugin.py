"""Discovery plugin for the protection-capped gap-continuation successor."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import dense_strategy_runtime as runtime
import equity_gap_continuation_plugin as legacy_plugin
import gap_protection_collection as collection
import gap_protection_successor as successor
from historical_store import HistoricalDayStore, sha256_file
from learning_data import LearningDataError, load_frozen_dataset_contract


PROJECT_ROOT = Path(__file__).resolve().parent


class GapProtectionPluginError(RuntimeError):
    """The exact dataset, rule, or production binding drifted."""


def _path(value: Any) -> Path:
    path = Path(str(value))
    return path if path.is_absolute() else PROJECT_ROOT / path


def _require_committed(path: Path) -> None:
    try:
        import strategy_discovery

        strategy_discovery.require_committed(path)
    except (OSError, ValueError) as exc:
        raise GapProtectionPluginError(
            f"required evidence is not committed: {path}"
        ) from exc


def _manifest(path: Path) -> dict[str, Any]:
    try:
        return load_frozen_dataset_contract(path)
    except (LearningDataError, OSError) as exc:
        raise GapProtectionPluginError(
            f"gap-protection dataset manifest is invalid: {exc}"
        ) from exc


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
    """Inspect only committed capacity metadata."""

    path = _path(contract.get("capacity_manifest"))
    _require_committed(path)
    manifest = _manifest(path)
    payload = manifest["dataset_payload"]
    capacity = payload.get("gap_protection_capacity")
    if not isinstance(capacity, Mapping):
        raise GapProtectionPluginError("gap-protection capacity is missing")
    for evidence in payload.get("evidence_paths", []):
        _require_committed(_path(evidence))
    checks = {
        "development_lane": payload.get("lane") == "development",
        "family_bound": capacity.get("family_id")
        == runtime.EQUITY_GAP_CONTINUATION_FAMILY,
        "dates_bound": manifest.get("requested_dates")
        == contract.get("development_dates"),
        "point_in_time": payload.get("point_in_time_evidence") is True,
        "development_exposed": capacity.get(
            "development_scope_indexed_as_exposed"
        )
        is True,
        "confirmation_locked": capacity.get("confirmation_access_permitted")
        is False,
        "inputs_inspected": capacity.get("development_data_inspected") is True,
    }
    formal_capacity = capacity.get("formal_capacity")
    if (
        isinstance(formal_capacity, bool)
        or not isinstance(formal_capacity, int)
        or formal_capacity < 0
    ):
        raise GapProtectionPluginError("formal capacity is invalid")
    return {
        "verified_capacity": formal_capacity,
        "point_in_time_complete": all(checks.values()),
        "metadata_checks": checks,
        "external_dataset_opened": False,
        "provider_telemetry": _telemetry(dataset_loads=0),
    }


def _account_policy() -> dict[str, Any]:
    return legacy_plugin._account_policy()


def _load_bound_dataset(
    manifest_path: Path,
    *,
    lane: str,
    expected_dates: Sequence[str],
) -> dict[str, Any]:
    _require_committed(manifest_path)
    manifest = _manifest(manifest_path)
    payload = manifest["dataset_payload"]
    binding = payload.get("gap_protection_runtime")
    if not (
        isinstance(binding, Mapping)
        and manifest.get("requested_dates") == list(expected_dates)
        and payload.get("lane") == lane
        and payload.get("inspected") is True
        and payload.get("point_in_time_evidence") is True
        and binding.get("family_id")
        == runtime.EQUITY_GAP_CONTINUATION_FAMILY
        and binding.get("sample_phase") == lane
    ):
        raise GapProtectionPluginError("gap-protection dataset scope drifted")
    if lane != "development":
        raise GapProtectionPluginError(
            "confirmation dataset collection is not yet admitted"
        )
    inspection_path = _path(binding.get("input_inspection_path"))
    _require_committed(inspection_path)
    if sha256_file(inspection_path) != binding.get(
        "input_inspection_file_sha256"
    ):
        raise GapProtectionPluginError("input inspection file drifted")
    inspection = collection._load_json(inspection_path)
    if (
        inspection.get("inspection_sha256")
        != binding.get("input_inspection_sha256")
        or inspection.get("private_input_index_content_sha256")
        != binding.get("private_input_index_content_sha256")
        or inspection.get("valid") is not True
    ):
        raise GapProtectionPluginError("input inspection semantics drifted")
    store = HistoricalDayStore.from_env()
    inventory = collection._load_gzip(successor._inventory_path(store))
    input_index = collection._load_gzip(collection._input_index_path(store))
    if (
        inventory.get("content_sha256")
        != binding.get("preentry_inventory_content_sha256")
        or collection._hash(input_index)
        != inspection["private_input_index_content_sha256"]
        or input_index.get("lane") != lane
    ):
        raise GapProtectionPluginError("private dataset index drifted")
    indexed = {
        (str(row["date"]), str(row["symbol"])): row
        for row in input_index["input_rows"]
    }
    candidate_symbols_by_date: dict[str, list[str]] = {}
    candidate_metadata_by_date: dict[
        str, dict[str, dict[str, Any]]
    ] = {}
    minute_bars: dict[str, dict[str, list[dict[str, Any]]]] = {}
    for day in expected_dates:
        frozen_candidates = inventory["candidates_by_date"][day]
        symbols = sorted(str(row["symbol"]) for row in frozen_candidates)
        candidate_symbols_by_date[day] = symbols
        candidate_metadata_by_date[day] = {
            str(row["symbol"]): {
                "symbol": str(row["symbol"]),
                "gap_fraction": float(row["gap_fraction"]),
            }
            for row in frozen_candidates
        }
        minute_bars[day] = {}
        for symbol in symbols:
            frozen = indexed.get((day, symbol))
            dataset = collection._full_dataset(store, symbol, day)
            if frozen is None or dataset is None:
                raise GapProtectionPluginError(
                    f"inspected input is missing: {day} {symbol}"
                )
            if (
                dataset["id"] != frozen["dataset_id"]
                or dataset["content_sha256"] != frozen["dataset_sha256"]
            ):
                raise GapProtectionPluginError(
                    f"inspected input drifted: {day} {symbol}"
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
                    "vwap_denominator": int(row.get("volume", 0)),
                }
                for row in rows
            ]
    return {
        "schema_version": 1,
        "family_id": runtime.EQUITY_GAP_CONTINUATION_FAMILY,
        "evaluation_dates": list(expected_dates),
        "candidate_symbols_by_date": candidate_symbols_by_date,
        "candidate_metadata_by_date": candidate_metadata_by_date,
        "regular_session_minutes_by_date": {
            day: 390 for day in expected_dates
        },
        "minute_bars": minute_bars,
        "source_semantics": {
            "universe": (
                "point-in-time active common stocks opening above 5 dollars "
                "and 2-8% above prior close at 09:35 ET"
            ),
            "feed": "Alpaca SIP",
            "adjustment": "raw",
            "sparse_policy": "retained_in_denominator_no_signal",
        },
    }


def evaluate_development(
    contract: Mapping[str, Any],
    trials: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    manifest_path = _path(contract.get("dataset_manifest"))
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
                family_id=runtime.EQUITY_GAP_CONTINUATION_FAMILY,
                trial_id=str(trial["trial_id"]),
                parameters=trial["parameters"],
                account_policy=policy,
                rolling_origin_plan=contract.get("rolling_origin_plan"),
            )
            for trial in trials
        ],
        "provider_telemetry": _telemetry(dataset_loads=1),
    }


def evaluate_confirmation(_winner: Mapping[str, Any]) -> dict[str, Any]:
    raise GapProtectionPluginError(
        "confirmation remains locked until a development winner freezes its counts"
    )


def evaluate_production(
    winner: Mapping[str, Any],
    market_facts: Mapping[str, Any],
) -> dict[str, Any]:
    return legacy_plugin.evaluate_production(winner, market_facts)

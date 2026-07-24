"""Plugin for the frozen positive-earnings-surprise drift family."""

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
from learning_data import LearningDataError, load_frozen_dataset_contract


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_MANIFEST_ROOT = (
    PROJECT_ROOT
    / "strategy_tournament/v2/discovery"
    / runtime.EARNINGS_PEAD_FAMILY
)


class EarningsPeadPluginError(RuntimeError):
    """A frozen PEAD input, exact rule, or live fact is incomplete."""


def _path(value: Any) -> Path:
    if not isinstance(value, str) or not value:
        raise EarningsPeadPluginError("frozen path is missing")
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
    if tracked.returncode != 0 or clean.returncode != 0:
        raise EarningsPeadPluginError(
            f"PEAD evidence must be committed and unchanged: {relative}"
        )


def _manifest(path: Path) -> dict[str, Any]:
    try:
        return load_frozen_dataset_contract(path)
    except (LearningDataError, OSError) as exc:
        raise EarningsPeadPluginError(
            f"PEAD dataset manifest is invalid: {exc}"
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
    path = _path(contract.get("capacity_manifest"))
    _require_committed(path)
    manifest = _manifest(path)
    payload = manifest["dataset_payload"]
    capacity = payload.get("earnings_pead_capacity")
    if not isinstance(capacity, Mapping):
        raise EarningsPeadPluginError("PEAD capacity binding is missing")
    for evidence in payload.get("evidence_paths", []):
        _require_committed(_path(evidence))
    checks = {
        "development_lane": payload.get("lane") == "development",
        "family_bound": capacity.get("family_id")
        == runtime.EARNINGS_PEAD_FAMILY,
        "dates_bound": manifest.get("requested_dates")
        == contract.get("development_dates"),
        "point_in_time": payload.get("point_in_time_evidence") is True,
        "confirmation_locked": capacity.get(
            "confirmation_access_permitted"
        )
        is False,
        "zero_provider_requests": capacity.get("provider_requests") == 0,
    }
    formal_capacity = int(capacity["development_event_pairs"]) + int(
        capacity["confirmation_event_pairs"]
    )
    return {
        "verified_capacity": formal_capacity,
        "point_in_time_complete": all(checks.values()),
        "metadata_checks": checks,
        "external_dataset_opened": False,
        "provider_telemetry": _telemetry(dataset_loads=0),
    }


def _private_selection(
    binding: Mapping[str, Any], store: HistoricalDayStore
) -> dict[str, Any]:
    raw = str(binding.get("private_selection", ""))
    prefix = "LOCAL_HISTORICAL_DATA_ROOT/"
    if not raw.startswith(prefix):
        raise EarningsPeadPluginError(
            "PEAD private selection path is invalid"
        )
    path = store.root / raw[len(prefix) :]
    with gzip.open(path, "rt", encoding="utf-8") as stream:
        value = json.load(stream)
    if (
        not isinstance(value, dict)
        or canonical_sha256(value)
        != binding.get("private_selection_content_sha256")
        or value.get("confirmation_outcomes_accessed") is not False
    ):
        raise EarningsPeadPluginError(
            "PEAD private selection content drifted"
        )
    return value


def _daily_bar(
    store: HistoricalDayStore, symbol: str, day: str
) -> dict[str, Any]:
    dataset = store.select_dataset(
        symbol,
        day,
        kind="derived",
        channel="minute_aggregate_regular",
        timeframe="1d",
        providers=("alpaca",),
        require_complete=True,
    )
    if dataset is None or len(dataset.get("rows", [])) != 1:
        raise EarningsPeadPluginError(
            f"complete daily bar is missing: {day} {symbol}"
        )
    row = dataset["rows"][0]
    return {
        "date": day,
        "open": float(row["o"]),
        "high": float(row["h"]),
        "low": float(row["l"]),
        "close": float(row["c"]),
        "volume": int(row.get("v", 0)),
    }


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
    binding = payload.get("earnings_pead_runtime")
    if not (
        isinstance(binding, Mapping)
        and manifest.get("requested_dates") == list(expected_dates)
        and payload.get("lane") == lane
        and payload.get("inspected") is True
        and payload.get("point_in_time_evidence") is True
        and binding.get("family_id") == runtime.EARNINGS_PEAD_FAMILY
        and binding.get("sample_phase") == lane
    ):
        raise EarningsPeadPluginError("PEAD dataset scope drifted")
    if lane == "confirmation" and not (
        payload.get("claim_scope")
        == "EXACT_PREREGISTERED_CONTRACT_ONLY"
        and payload.get("preregistration_sha256")
        == preregistration_sha256
        and payload.get("capture_after_preregistration_attested") is True
    ):
        raise EarningsPeadPluginError(
            "PEAD confirmation dataset is not winner-bound"
        )
    store = HistoricalDayStore.from_env()
    selection = _private_selection(binding, store)
    if selection[f"{lane}_dates"] != list(expected_dates):
        raise EarningsPeadPluginError(
            "PEAD private account calendar drifted"
        )
    calendar = [
        day
        for day in store.dates("SPY")
        if "2024-01-01" <= day <= "2025-12-31"
    ]
    calendar_index = {day: index for index, day in enumerate(calendar)}
    metadata = selection["candidates_by_phase"][lane]
    daily: dict[str, list[dict[str, Any]]] = {
        "SPY": [_daily_bar(store, "SPY", day) for day in calendar]
    }
    needed: dict[str, set[str]] = {}
    for day, rows in metadata.items():
        index = calendar_index[day]
        for row in rows:
            needed.setdefault(str(row["symbol"]), set()).update(
                calendar[index - 25 : index + 6]
            )
    for symbol, dates in sorted(needed.items()):
        daily[symbol] = [
            _daily_bar(store, symbol, day) for day in sorted(dates)
        ]
    return {
        "schema_version": 1,
        "family_id": runtime.EARNINGS_PEAD_FAMILY,
        "evaluation_dates": list(expected_dates),
        "event_metadata_by_date": metadata,
        "daily_bars": daily,
        "source_semantics": {
            "event_timestamp": "provider verified am or pm classification",
            "entry": "reaction-session raw open",
            "bars": "Alpaca SIP raw minute-derived daily OHLCV",
            "sparse_policy": "retained account-calendar zero-return day",
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
    return {
        "dataset_manifest": str(contract["dataset_manifest"]),
        "trials": [
            runtime.evaluate_trial(
                dataset,
                family_id=runtime.EARNINGS_PEAD_FAMILY,
                trial_id=str(trial["trial_id"]),
                parameters=trial["parameters"],
                account_policy=_account_policy(),
                rolling_origin_plan=contract.get("rolling_origin_plan"),
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
        raise EarningsPeadPluginError(
            "expected one frozen PEAD confirmation dataset manifest"
        )
    return paths[0]


def evaluate_confirmation(winner: Mapping[str, Any]) -> dict[str, Any]:
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
        family_id=runtime.EARNINGS_PEAD_FAMILY,
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
        "event_verified",
        "event_timing",
        "event_timestamp_fresh",
        "identity_point_in_time",
        "daily_history_complete",
        "quote_fresh",
        "spread_fraction",
        "depth_sufficient",
        "halted",
        "tradable",
        "news_reconciled",
        "protection_plan_complete",
        "account_reconciled",
    }
    if set(decision_data) != required:
        raise EarningsPeadPluginError(
            "PEAD production decision-data schema drifted"
        )
    passed = (
        decision_data["event_verified"] is True
        and decision_data["event_timestamp_fresh"] is True
        and decision_data["identity_point_in_time"] is True
        and decision_data["daily_history_complete"] is True
        and decision_data["quote_fresh"] is True
        and float(decision_data["spread_fraction"]) <= 0.0015
        and decision_data["depth_sufficient"] is True
        and decision_data["halted"] is False
        and decision_data["tradable"] is True
        and decision_data["news_reconciled"] is True
        and decision_data["protection_plan_complete"] is True
        and decision_data["account_reconciled"] is True
        and decision_data["event_timing"] in {"am", "pm"}
    )
    return {
        "strategy_id": winner["strategy_id"],
        "strategy_version": winner["strategy_version"],
        "rules_hash": winner["rules_hash"],
        "entry_ready": passed,
        "fail_closed": not passed,
        "broker_actions": 0,
    }

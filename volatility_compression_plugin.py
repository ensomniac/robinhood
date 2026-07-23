"""Discovery plugin for the gap-universe volatility-compression successor."""

from __future__ import annotations

import subprocess
from collections.abc import Mapping, Sequence
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import dense_strategy_runtime as runtime
import equity_gap_continuation_plugin as source_plugin
import portfolio_maturity
from learning_data import LearningDataError, load_frozen_dataset_contract


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_MANIFEST_ROOT = (
    PROJECT_ROOT
    / "strategy_tournament/v2/discovery"
    / runtime.VOLATILITY_COMPRESSION_FAMILY
)


class VolatilityCompressionPluginError(RuntimeError):
    """A frozen compression input, exact rule, or live fact is incomplete."""


def _path(value: Any) -> Path:
    if not isinstance(value, str) or not value:
        raise VolatilityCompressionPluginError("frozen path is missing")
    path = Path(value)
    return path if path.is_absolute() else PROJECT_ROOT / path


def _require_committed(path: Path) -> None:
    try:
        relative = str(path.resolve().relative_to(PROJECT_ROOT.resolve()))
    except ValueError as exc:
        raise VolatilityCompressionPluginError(
            "compression evidence must remain inside the repository"
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
        raise VolatilityCompressionPluginError(
            f"compression evidence must be committed and unchanged: {relative}"
        )


def _manifest(path: Path) -> dict[str, Any]:
    try:
        return load_frozen_dataset_contract(path)
    except (LearningDataError, OSError) as exc:
        raise VolatilityCompressionPluginError(
            f"compression dataset manifest is invalid: {exc}"
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
    """Inspect only committed outcome-blind capacity metadata."""

    path = _path(contract.get("capacity_manifest"))
    _require_committed(path)
    manifest = _manifest(path)
    payload = manifest["dataset_payload"]
    capacity = payload.get("volatility_compression_capacity")
    if not isinstance(capacity, Mapping):
        raise VolatilityCompressionPluginError(
            "compression capacity binding is missing"
        )
    for evidence in payload.get("evidence_paths", []):
        _require_committed(_path(evidence))
    checks = {
        "development_lane": payload.get("lane") == "development",
        "family_bound": capacity.get("family_id")
        == runtime.VOLATILITY_COMPRESSION_FAMILY,
        "dates_bound": manifest.get("requested_dates")
        == contract.get("development_dates"),
        "point_in_time": payload.get("point_in_time_evidence") is True,
        "contaminated_training_declared": capacity.get(
            "development_training_contaminated"
        )
        is True,
        "confirmation_locked": capacity.get("confirmation_access_permitted")
        is False,
    }
    formal_capacity = capacity.get("formal_capacity")
    if (
        isinstance(formal_capacity, bool)
        or not isinstance(formal_capacity, int)
        or formal_capacity < 0
    ):
        raise VolatilityCompressionPluginError(
            "compression formal capacity is invalid"
        )
    return {
        "verified_capacity": formal_capacity,
        "point_in_time_complete": all(checks.values()),
        "metadata_checks": checks,
        "external_dataset_opened": False,
        "provider_telemetry": _telemetry(dataset_loads=0),
    }


def _load_dataset(
    manifest_path: Path,
    *,
    lane: str,
    expected_dates: Sequence[str],
    preregistration_sha256: str | None = None,
) -> dict[str, Any]:
    return source_plugin._load_bound_dataset(
        manifest_path,
        lane=lane,
        expected_dates=expected_dates,
        preregistration_sha256=preregistration_sha256,
        target_family_id=runtime.VOLATILITY_COMPRESSION_FAMILY,
    )


def evaluate_development(
    contract: Mapping[str, Any], trials: Sequence[Mapping[str, Any]]
) -> dict[str, Any]:
    manifest_path = _path(contract.get("dataset_manifest"))
    dataset = runtime.prepare_dataset(
        _load_dataset(
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
                family_id=runtime.VOLATILITY_COMPRESSION_FAMILY,
                trial_id=str(trial["trial_id"]),
                parameters=trial["parameters"],
                account_policy=policy,
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
        raise VolatilityCompressionPluginError(
            "expected one frozen compression confirmation dataset manifest"
        )
    return paths[0]


def evaluate_confirmation(winner: Mapping[str, Any]) -> dict[str, Any]:
    manifest_path = _confirmation_manifest()
    dataset = runtime.prepare_dataset(
        _load_dataset(
            manifest_path,
            lane="confirmation",
            expected_dates=winner["confirmation_dates"],
            preregistration_sha256=str(winner["rules_hash"]),
        )
    )
    exact = runtime.evaluate_trial(
        dataset,
        family_id=runtime.VOLATILITY_COMPRESSION_FAMILY,
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


def _live_trigger(
    bars_by_symbol: Mapping[str, Sequence[Mapping[str, Any]]],
    parameters: Mapping[str, Any],
) -> dict[str, Any]:
    compression_bars = int(parameters["compression_bars"])
    maximum_ratio = float(parameters["maximum_compression_ratio"])
    volume_threshold = float(parameters["breakout_volume_multiple"])
    signal_cutoff = int(parameters["signal_cutoff_minutes"])
    target_r = float(parameters["target_r"])
    qualified: list[tuple[int, float, float, str, float]] = []
    for symbol, bars in bars_by_symbol.items():
        if len(bars) <= runtime.COMPRESSION_SIGNAL_START_INDEX:
            continue
        first_thirty_range = (
            max(float(bar["high"]) for bar in bars[:30])
            - min(float(bar["low"]) for bar in bars[:30])
        )
        if first_thirty_range <= 0:
            continue
        numerator = 0.0
        denominator = 0.0
        for index, bar in enumerate(bars):
            volume = float(bar["volume"])
            numerator += (
                (
                    float(bar["high"])
                    + float(bar["low"])
                    + float(bar["close"])
                )
                / 3.0
                * volume
            )
            denominator += volume
            if not (
                runtime.COMPRESSION_SIGNAL_START_INDEX
                <= index
                <= signal_cutoff
                and index >= compression_bars
            ):
                continue
            window = bars[index - compression_bars : index]
            compression_high = max(float(item["high"]) for item in window)
            compression_low = min(float(item["low"]) for item in window)
            ratio = (
                compression_high - compression_low
            ) / first_thirty_range
            mean_volume = sum(float(item["volume"]) for item in window) / len(
                window
            )
            volume_multiple = volume / mean_volume if mean_volume > 0 else 0.0
            close = float(bar["close"])
            if (
                denominator > 0
                and ratio <= maximum_ratio + 1e-12
                and volume_multiple + 1e-12 >= volume_threshold
                and close > compression_high
                and close > numerator / denominator
            ):
                qualified.append(
                    (
                        index + 1,
                        ratio,
                        -volume_multiple,
                        symbol,
                        compression_low,
                    )
                )
                break
    if not qualified:
        raise VolatilityCompressionPluginError(
            "no exact live volatility-compression trigger"
        )
    entry_index, ratio, negative_volume, symbol, stop = sorted(qualified)[0]
    return {
        "symbol": symbol,
        "trigger_index": entry_index - 1,
        "entry_index": entry_index,
        "compression_ratio": ratio,
        "volume_multiple": -negative_volume,
        "stop_price": stop,
        "target_r": target_r,
    }


def evaluate_production(
    winner: Mapping[str, Any], market_facts: Mapping[str, Any]
) -> dict[str, Any]:
    """Evaluate the immutable rule from one synchronized live decision boundary."""

    expected = {
        "selected_trial_id",
        "parameters",
        "session_date",
        "candidate_symbols",
        "selection_complete",
        "bars_by_symbol",
        "quote",
        "operational",
    }
    if set(market_facts) != expected:
        raise VolatilityCompressionPluginError(
            "live compression fact schema drifted"
        )
    if (
        market_facts["selected_trial_id"]
        != winner["exact_rules"]["selected_trial_id"]
        or market_facts["parameters"] != winner["exact_rules"]["parameters"]
        or market_facts["selection_complete"] is not True
    ):
        raise VolatilityCompressionPluginError(
            "live compression exact rules drifted"
        )
    symbols = market_facts["candidate_symbols"]
    bars = market_facts["bars_by_symbol"]
    if (
        not isinstance(symbols, list)
        or symbols != sorted(set(map(str, symbols)))
        or not isinstance(bars, Mapping)
        or set(bars) != set(symbols)
    ):
        raise VolatilityCompressionPluginError(
            "live point-in-time candidate denominator is incomplete"
        )
    bar_lengths = {
        len(value)
        for value in bars.values()
        if isinstance(value, Sequence) and not isinstance(value, (str, bytes))
    }
    last_timestamps = {
        str(value[-1].get("timestamp"))
        for value in bars.values()
        if isinstance(value, Sequence)
        and not isinstance(value, (str, bytes))
        and value
        and isinstance(value[-1], Mapping)
    }
    if (
        len(bar_lengths) != 1
        or len(last_timestamps) != 1
        or len(bar_lengths) != len(last_timestamps)
    ):
        raise VolatilityCompressionPluginError(
            "live candidate bars do not share one completed boundary"
        )
    trigger = _live_trigger(bars, market_facts["parameters"])
    if trigger["trigger_index"] != next(iter(bar_lengths)) - 1:
        raise VolatilityCompressionPluginError(
            "live compression trigger was observable on an earlier bar"
        )
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
        raise VolatilityCompressionPluginError("live quote schema drifted")
    if (
        quote["symbol"] != trigger["symbol"]
        or quote["halted"] is not False
        or quote["tradable"] is not True
        or float(quote["bid"]) <= 0
        or float(quote["ask"]) < float(quote["bid"])
        or int(quote["executable_ask_depth"]) <= 0
        or int(quote["recent_real_minute_volume"]) <= 0
    ):
        raise VolatilityCompressionPluginError(
            "live ranked quote is not executable"
        )
    try:
        observed = datetime.fromisoformat(str(quote["observed_at"]))
        trigger_bar = datetime.fromisoformat(
            str(bars[trigger["symbol"]][trigger["trigger_index"]]["timestamp"])
        )
    except ValueError as exc:
        raise VolatilityCompressionPluginError(
            "live timestamps are invalid"
        ) from exc
    if (
        observed.tzinfo is None
        or trigger_bar.tzinfo is None
        or observed < trigger_bar + timedelta(minutes=1)
        or observed >= trigger_bar + timedelta(minutes=2)
    ):
        raise VolatilityCompressionPluginError(
            "live quote is not in the next observable minute"
        )
    operational = market_facts["operational"]
    required_operational = {
        "account_reconciled",
        "orders_reconciled",
        "protection_reconciled",
        "tradability_reconciled",
        "news_reconciled",
        "protective_order_route_ready",
        "monitoring_ready",
        "safe_cutoff",
    }
    if (
        not isinstance(operational, Mapping)
        or set(operational) != required_operational
        or any(
            operational[field] is not True
            for field in required_operational - {"safe_cutoff"}
        )
    ):
        raise VolatilityCompressionPluginError(
            "live operational gates are incomplete"
        )
    entry = float(quote["ask"])
    stop = float(trigger["stop_price"])
    target = entry + float(trigger["target_r"]) * (entry - stop)
    if not 0 < stop < entry < target:
        raise VolatilityCompressionPluginError(
            "live entry protection is invalid"
        )
    expected_gross = (target - entry) / entry
    if expected_gross < (
        runtime.MINIMUM_GROSS_TO_COST_MULTIPLE
        * runtime.PRIMARY_ROUND_TRIP_COST_FRACTION
    ):
        raise VolatilityCompressionPluginError(
            "live expected move misses cost floor"
        )
    return {
        "strategy_id": winner["strategy_id"],
        "strategy_version": winner["strategy_version"],
        "rules_hash": winner["rules_hash"],
        "historical_semantics_sha256": winner["rules_hash"],
        "ranking_complete": True,
        "observed_at": quote["observed_at"],
        "symbol": trigger["symbol"],
        "rank": 1,
        "score": (
            -trigger["compression_ratio"]
            + trigger["volume_multiple"] / 100.0
        ),
        "halted": quote["halted"],
        "tradable": quote["tradable"],
        "bid": quote["bid"],
        "ask": quote["ask"],
        "entry_limit": entry,
        "stop_price": stop,
        "target_price": target,
        "expected_gross_move_fraction": expected_gross,
        "holding_trading_days": 1,
        "protection_time_in_force": "day",
        "executable_ask_depth": quote["executable_ask_depth"],
        "recent_real_minute_volume": quote["recent_real_minute_volume"],
        "exit_plan": {
            "type": "stop_target_or_force_flat",
            "same_interval_ambiguity": "stop_first",
            "force_flat_et": "15:50:00",
            "safe_cutoff": operational["safe_cutoff"],
        },
        **dict(operational),
    }

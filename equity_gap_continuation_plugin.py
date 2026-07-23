"""Discovery plugin for the selection-aware equity-gap-continuation successor.

Development reuses the already exposed representative 2025 minute corpus only
as contaminated training. Confirmation is a separately frozen, winner-bound
phase. Historical and production evaluation share the exact gap, opening-range,
volume, ranking, stop, target, and force-flat semantics.
"""

from __future__ import annotations

import subprocess
from collections.abc import Mapping, Sequence
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import dense_strategy_runtime as runtime
import equity_gap_continuation_validation as gap
import portfolio_maturity
from historical_store import HistoricalDayStore, canonical_sha256, sha256_file
from learning_data import LearningDataError, load_frozen_dataset_contract


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_MANIFEST_ROOT = (
    PROJECT_ROOT
    / "strategy_tournament/v2/discovery"
    / runtime.EQUITY_GAP_CONTINUATION_FAMILY
)


class EquityGapContinuationPluginError(RuntimeError):
    """A frozen gap-continuation input, rule, or live fact is incomplete."""


def _path(value: Any) -> Path:
    if not isinstance(value, str) or not value:
        raise EquityGapContinuationPluginError("frozen path is missing")
    path = Path(value)
    return path if path.is_absolute() else PROJECT_ROOT / path


def _require_committed(path: Path) -> None:
    try:
        relative = str(path.resolve().relative_to(PROJECT_ROOT.resolve()))
    except ValueError as exc:
        raise EquityGapContinuationPluginError(
            "gap-continuation evidence must remain inside the repository"
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
        raise EquityGapContinuationPluginError(
            "gap-continuation evidence must be committed and unchanged: "
            f"{relative}"
        )


def _manifest(path: Path) -> dict[str, Any]:
    try:
        return load_frozen_dataset_contract(path)
    except (LearningDataError, OSError) as exc:
        raise EquityGapContinuationPluginError(
            f"gap-continuation dataset manifest is invalid: {exc}"
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
    """Inspect committed public metadata without opening minute bars."""

    path = _path(contract.get("capacity_manifest"))
    _require_committed(path)
    manifest = _manifest(path)
    payload = manifest["dataset_payload"]
    capacity = payload.get("gap_continuation_capacity")
    if not isinstance(capacity, Mapping):
        raise EquityGapContinuationPluginError(
            "gap-continuation capacity binding is missing"
        )
    for evidence in payload.get("evidence_paths", []):
        _require_committed(_path(evidence))
    checks = {
        "development_lane": payload.get("lane") == "development",
        "family_bound": capacity.get("family_id")
        == runtime.EQUITY_GAP_CONTINUATION_FAMILY,
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
        raise EquityGapContinuationPluginError(
            "gap-continuation formal capacity is invalid"
        )
    return {
        "verified_capacity": formal_capacity,
        "point_in_time_complete": all(checks.values()),
        "metadata_checks": checks,
        "external_dataset_opened": False,
        "provider_telemetry": _telemetry(dataset_loads=0),
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
    binding = payload.get("gap_continuation_runtime")
    if not isinstance(binding, Mapping):
        raise EquityGapContinuationPluginError(
            "gap-continuation runtime binding is missing"
        )
    if not (
        manifest.get("requested_dates") == list(expected_dates)
        and payload.get("lane") == lane
        and payload.get("inspected") is True
        and payload.get("point_in_time_evidence") is True
        and binding.get("family_id")
        == runtime.EQUITY_GAP_CONTINUATION_FAMILY
        and binding.get("sample_phase") == lane
    ):
        raise EquityGapContinuationPluginError(
            "gap-continuation dataset scope drifted"
        )
    if lane == "confirmation" and not (
        payload.get("claim_scope") == "EXACT_PREREGISTERED_CONTRACT_ONLY"
        and payload.get("preregistration_sha256")
        == preregistration_sha256
        and payload.get("capture_after_preregistration_attested") is True
    ):
        raise EquityGapContinuationPluginError(
            "confirmation dataset is not winner-bound"
        )
    source_manifest_path = _path(
        binding.get("source_selection_manifest_path")
    )
    input_inspection_path = _path(binding.get("input_inspection_path"))
    _require_committed(source_manifest_path)
    _require_committed(input_inspection_path)
    if (
        sha256_file(source_manifest_path)
        != binding.get("source_selection_manifest_file_sha256")
        or sha256_file(input_inspection_path)
        != binding.get("input_inspection_file_sha256")
    ):
        raise EquityGapContinuationPluginError(
            "gap-continuation source binding drifted"
        )
    source_manifest = gap._load_json(source_manifest_path)
    selection_hash = source_manifest.get("private_selection", {}).get(
        "content_sha256"
    )
    store = HistoricalDayStore.from_env()
    selection = gap._load_gzip(gap._selection_path(store))
    if (
        canonical_sha256(selection) != selection_hash
        or selection["phases"][lane]["dates"] != list(expected_dates)
    ):
        raise EquityGapContinuationPluginError(
            "gap-continuation private selection graph drifted"
        )
    inspection = gap._load_json(input_inspection_path)
    input_index = gap._load_gzip(gap._input_index_path(store, lane))
    if (
        canonical_sha256(input_index)
        != inspection.get("private_input_index_content_sha256")
        or input_index.get("sample_phase") != lane
    ):
        raise EquityGapContinuationPluginError(
            "gap-continuation inspected input index drifted"
        )
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
        frozen_candidates = selection["phases"][lane][
            "candidates_by_date"
        ][day]
        symbols = sorted(str(item["symbol"]) for item in frozen_candidates)
        candidate_symbols_by_date[day] = symbols
        candidate_metadata_by_date[day] = {
            str(item["symbol"]): {
                "symbol": str(item["symbol"]),
                "gap_fraction": float(item["gap_fraction"]),
            }
            for item in frozen_candidates
        }
        minute_bars[day] = {}
        for symbol in symbols:
            frozen_input = indexed.get((day, symbol))
            dataset = gap._full_minute_dataset(store, symbol, day)
            if frozen_input is None or dataset is None:
                raise EquityGapContinuationPluginError(
                    f"inspected gap-continuation input is missing: {day} {symbol}"
                )
            if not (
                dataset["id"] == frozen_input["dataset_id"]
                and dataset["content_sha256"]
                == frozen_input["dataset_sha256"]
            ):
                raise EquityGapContinuationPluginError(
                    f"inspected gap-continuation input drifted: {day} {symbol}"
                )
            rows, exact = gap._dataset_rows(dataset, day, symbol)
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
                "point-in-time common stocks opening above 5 dollars and "
                "2-8% above prior close at 09:35 ET"
            ),
            "feed": "Alpaca SIP",
            "adjustment": "raw",
            "sparse_policy": "retained_in_denominator_no_signal",
        },
    }


def evaluate_development(
    contract: Mapping[str, Any], trials: Sequence[Mapping[str, Any]]
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


def _confirmation_manifest() -> Path:
    paths = sorted(
        (DEFAULT_MANIFEST_ROOT / "confirmation-dataset").glob("*.json")
    )
    if len(paths) != 1:
        raise EquityGapContinuationPluginError(
            "expected one frozen gap-continuation confirmation dataset manifest"
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
        family_id=runtime.EQUITY_GAP_CONTINUATION_FAMILY,
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
    metadata_by_symbol: Mapping[str, Mapping[str, Any]],
    parameters: Mapping[str, Any],
) -> dict[str, Any]:
    minimum_gap = float(parameters["minimum_gap_fraction"])
    opening_range = int(parameters["opening_range_minutes"])
    volume_threshold = float(parameters["breakout_volume_multiple"])
    signal_cutoff = int(parameters["signal_cutoff_minutes"])
    target_r = float(parameters["target_r"])
    qualified: list[tuple[int, float, float, str, float]] = []
    for symbol, bars in bars_by_symbol.items():
        if len(bars) <= max(opening_range, runtime.GAP_VOLUME_LOOKBACK_BARS):
            continue
        gap_fraction = float(metadata_by_symbol[symbol]["gap_fraction"])
        if gap_fraction + 1e-12 < minimum_gap:
            continue
        range_high = max(float(bar["high"]) for bar in bars[:opening_range])
        range_low = min(float(bar["low"]) for bar in bars[:opening_range])
        numerator = 0.0
        denominator = 0.0
        for index, bar in enumerate(bars):
            volume = int(bar["volume"])
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
                max(opening_range, runtime.GAP_VOLUME_LOOKBACK_BARS)
                <= index
                <= signal_cutoff
            ):
                continue
            prior = bars[
                index - runtime.GAP_VOLUME_LOOKBACK_BARS : index
            ]
            mean_volume = sum(float(item["volume"]) for item in prior) / len(
                prior
            )
            volume_multiple = (
                volume / mean_volume if mean_volume > 0 else 0.0
            )
            close = float(bar["close"])
            if (
                denominator > 0
                and close > range_high
                and close > numerator / denominator
                and volume_multiple + 1e-12 >= volume_threshold
            ):
                qualified.append(
                    (
                        index + 1,
                        -volume_multiple,
                        -gap_fraction,
                        symbol,
                        range_low,
                    )
                )
                break
    if not qualified:
        raise EquityGapContinuationPluginError(
            "no exact live gap-continuation trigger"
        )
    (
        entry_index,
        negative_volume,
        negative_gap,
        symbol,
        stop,
    ) = sorted(qualified)[0]
    return {
        "symbol": symbol,
        "trigger_index": entry_index - 1,
        "entry_index": entry_index,
        "volume_multiple": -negative_volume,
        "gap_fraction": -negative_gap,
        "stop_price": stop,
        "target_r": target_r,
    }


def evaluate_production(
    winner: Mapping[str, Any], market_facts: Mapping[str, Any]
) -> dict[str, Any]:
    """Evaluate the exact frozen rule from complete observable live facts."""

    expected = {
        "selected_trial_id",
        "parameters",
        "session_date",
        "candidate_symbols",
        "candidate_metadata",
        "selection_complete",
        "bars_by_symbol",
        "quote",
        "operational",
    }
    if set(market_facts) != expected:
        raise EquityGapContinuationPluginError(
            "live gap-continuation fact schema drifted"
        )
    if (
        market_facts["selected_trial_id"]
        != winner["exact_rules"]["selected_trial_id"]
        or market_facts["parameters"] != winner["exact_rules"]["parameters"]
        or market_facts["selection_complete"] is not True
    ):
        raise EquityGapContinuationPluginError(
            "live gap-continuation exact rules drifted"
        )
    symbols = market_facts["candidate_symbols"]
    bars = market_facts["bars_by_symbol"]
    metadata = market_facts["candidate_metadata"]
    if (
        not isinstance(symbols, list)
        or symbols != sorted(set(map(str, symbols)))
        or not isinstance(bars, Mapping)
        or not isinstance(metadata, Mapping)
        or set(bars) != set(symbols)
        or set(metadata) != set(symbols)
    ):
        raise EquityGapContinuationPluginError(
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
        raise EquityGapContinuationPluginError(
            "live candidate bars do not share one completed decision boundary"
        )
    trigger = _live_trigger(bars, metadata, market_facts["parameters"])
    if trigger["trigger_index"] != next(iter(bar_lengths)) - 1:
        raise EquityGapContinuationPluginError(
            "live gap-continuation trigger was not discovered at the current boundary"
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
        raise EquityGapContinuationPluginError("live quote schema drifted")
    if quote["symbol"] != trigger["symbol"]:
        raise EquityGapContinuationPluginError(
            "live quote symbol is not ranked first"
        )
    try:
        observed = datetime.fromisoformat(str(quote["observed_at"]))
        trigger_bar = datetime.fromisoformat(
            str(
                bars[trigger["symbol"]][trigger["trigger_index"]][
                    "timestamp"
                ]
            )
        )
    except ValueError as exc:
        raise EquityGapContinuationPluginError(
            "live timestamps are invalid"
        ) from exc
    if (
        observed.tzinfo is None
        or trigger_bar.tzinfo is None
        or observed < trigger_bar + timedelta(minutes=1)
        or observed >= trigger_bar + timedelta(minutes=2)
    ):
        raise EquityGapContinuationPluginError(
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
        raise EquityGapContinuationPluginError(
            "live operational gates are incomplete"
        )
    entry = float(quote["ask"])
    stop = float(trigger["stop_price"])
    target = entry + float(trigger["target_r"]) * (entry - stop)
    if not 0 < stop < entry < target:
        raise EquityGapContinuationPluginError(
            "live entry protection is invalid"
        )
    expected_gross = (target - entry) / entry
    if expected_gross < (
        runtime.MINIMUM_GROSS_TO_COST_MULTIPLE
        * runtime.PRIMARY_ROUND_TRIP_COST_FRACTION
    ):
        raise EquityGapContinuationPluginError(
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
        "score": trigger["volume_multiple"] + trigger["gap_fraction"],
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

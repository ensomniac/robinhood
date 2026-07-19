"""Collect and assemble validation-grade historical replay bundles.

The input manifest freezes dates, symbols, scanner ranking, and time-valid
catalyst evidence before target-session market data is requested. This module
uses provider-neutral read-only clients in IBKR, Massive, then Alpaca order,
records successful responses in the external canonical day store, derives
inputs without future bars, and validates a complete bundle before writing the
compact replay artifact under ``historical_data/``.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import statistics
import sys
from collections import Counter
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from time import monotonic
from typing import Any, Mapping, Sequence
from zoneinfo import ZoneInfo

from historical_concurrency import ordered_bounded_results
from historical_learning import (
    BUNDLE_SCHEMA_VERSION,
    HistoricalLearningError,
    batch_acceptance,
    validate_bundle,
)
from historical_metrics import (
    HistoricalMetricError,
    average_daily_volume,
    average_true_range,
)
from historical_service import open_provider_set
from historical_store import HistoricalDayStore
from ibkr_historical import (
    DEFAULT_ENV_PATH,
    IBKRConfig,
    IBKRConfigurationError,
    IBKRHistoricalClient,
    IBKRHistoricalError,
    PRE_SESSION_CACHE_VERSION,
    collect_candidate_history,
    historical_error_category,
    is_retryable_historical_error,
)
from historical_providers import (
    HistoricalMarketDataClient,
    HistoricalProviderError,
    MassiveConfig,
    MassiveHistoricalClient,
)
from strategy_engine import StrategyInputError, evaluate_candidate, load_config


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_DATA_ROOT = PROJECT_ROOT / "historical_data"
EASTERN = ZoneInfo("America/New_York")
UTC = timezone.utc
BENCHMARKS = ("SPY", "QQQ")
EXPECTED_MINUTES = 390
DEFAULT_COLLECTION_WORKERS = 4


class HistoricalBundleBuildError(ValueError):
    """Raised when frozen evidence or collected data cannot build a bundle."""


def _number(value: Any, field: str, *, positive: bool = False) -> float:
    if isinstance(value, bool):
        raise HistoricalBundleBuildError(f"{field} must be numeric")
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise HistoricalBundleBuildError(f"{field} must be numeric") from exc
    if not math.isfinite(result) or (positive and result <= 0):
        qualifier = "positive and finite" if positive else "finite"
        raise HistoricalBundleBuildError(f"{field} must be {qualifier}")
    return result


def _time_et(row: Mapping[str, Any]) -> time:
    value = row.get("time_et")
    if not isinstance(value, str):
        raise HistoricalBundleBuildError("bar time_et must be an ISO timestamp")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise HistoricalBundleBuildError(f"invalid bar time_et: {value}") from exc
    return parsed.time().replace(tzinfo=None)


def _time_text(value: str) -> str:
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise HistoricalBundleBuildError(f"invalid timestamp: {value}") from exc
    return parsed.time().replace(tzinfo=None, microsecond=0).isoformat()


def _ordered_session_bars(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    ordered = sorted((dict(row) for row in rows), key=lambda row: int(row["epoch"]))
    if len(ordered) != EXPECTED_MINUTES:
        raise HistoricalBundleBuildError(
            f"session needs {EXPECTED_MINUTES} minute bars, got {len(ordered)}"
        )
    expected = datetime.combine(date(2000, 1, 1), time(9, 30))
    for index, row in enumerate(ordered):
        actual = _time_et(row)
        wanted = expected + timedelta(minutes=index)
        if actual != wanted.time():
            raise HistoricalBundleBuildError(
                f"session bar {index} is {actual.isoformat()}, expected {wanted.time().isoformat()}"
            )
        if row.get("interpolated") is not False:
            raise HistoricalBundleBuildError("session contains an interpolated bar")
    return ordered


def determine_evaluation(
    rows: Sequence[Mapping[str, Any]],
) -> tuple[str, bool, dict[str, float]]:
    """Return a point-in-time evaluation after the first eligible ORB bar."""
    ordered = _ordered_session_bars(rows)
    opening_rows = [
        row for row in ordered if time(9, 30) <= _time_et(row) < time(9, 35)
    ]
    if len(opening_rows) != 5:
        raise HistoricalBundleBuildError(
            "session does not contain the five opening minutes"
        )
    opening = {
        "open": _number(opening_rows[0]["open"], "opening.open", positive=True),
        "high": max(
            _number(row["high"], "opening.high", positive=True) for row in opening_rows
        ),
        "low": min(
            _number(row["low"], "opening.low", positive=True) for row in opening_rows
        ),
        "close": _number(opening_rows[-1]["close"], "opening.close", positive=True),
        "volume": float(sum(int(row["volume"]) for row in opening_rows)),
    }
    for row in ordered:
        clock = _time_et(row)
        if time(9, 35) <= clock < time(10, 29) and float(row["high"]) > opening["high"]:
            evaluation_at = (
                datetime.combine(date.min, clock) + timedelta(minutes=2)
            ).time()
            return evaluation_at.replace(microsecond=0).isoformat(), True, opening
    return "10:30:00", False, opening


def _atr14(rows: Sequence[Mapping[str, Any]]) -> float:
    try:
        return average_true_range(rows, 14)
    except HistoricalMetricError as exc:
        raise HistoricalBundleBuildError(str(exc)) from exc


def _cumulative_vwap(rows: Sequence[Mapping[str, Any]]) -> float:
    if not rows:
        raise HistoricalBundleBuildError("VWAP needs at least one completed bar")
    weighted = 0.0
    volume = 0
    for row in rows:
        row_volume = int(row["volume"])
        price = float(row.get("wap") or 0)
        if price <= 0:
            price = (float(row["high"]) + float(row["low"]) + float(row["close"])) / 3
        weighted += price * row_volume
        volume += row_volume
    if volume <= 0:
        raise HistoricalBundleBuildError("VWAP completed volume must be positive")
    return weighted / volume


def _available_cumulative_vwap(
    rows: Sequence[Mapping[str, Any]],
) -> float | None:
    """Return VWAP when the window contains real volume, otherwise ``None``."""
    if not rows or sum(int(row["volume"]) for row in rows) <= 0:
        return None
    return _cumulative_vwap(rows)


def _market_metrics(
    rows: Sequence[Mapping[str, Any]], evaluation_time: str
) -> dict[str, float | bool]:
    ordered = _ordered_session_bars(rows)
    evaluation = time.fromisoformat(evaluation_time)
    completed = [row for row in ordered if _time_et(row) < evaluation]
    if not completed:
        raise HistoricalBundleBuildError("evaluation has no completed benchmark bars")
    vwap = _cumulative_vwap(completed)
    earlier = _cumulative_vwap(completed[:-5] or completed[:1])
    last = float(completed[-1]["close"])
    opening = float(ordered[0]["open"])
    return {
        "last": last,
        "vwap": vwap,
        "vwap_flat_or_rising": vwap >= earlier,
        "above_vwap": last >= vwap,
        "return_fraction": last / opening - 1,
    }


def _quote_payload(raw: Mapping[str, Any]) -> list[dict[str, Any]]:
    snapshots = raw.get("quote_snapshots")
    if not isinstance(snapshots, list) or len(snapshots) != 3:
        raise HistoricalBundleBuildError(
            "candidate needs three historical quote snapshots"
        )
    result: list[dict[str, Any]] = []
    for row in snapshots:
        result.append(
            {
                "observed_at_et": _time_text(str(row["observed_at_et"])),
                "age_seconds": float(row["age_seconds"]),
                "bid": float(row["bid"]),
                "ask": float(row["ask"]),
                "ask_depth": int(row["ask_depth"]),
                "recent_real_1m_volume": int(row["recent_real_1m_volume"]),
            }
        )
    return result


def _replay_bars(raw: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows = _ordered_session_bars(raw["session_bars"])
    return [
        {
            "time_et": _time_et(row).replace(microsecond=0).isoformat(),
            "open": float(row["open"]),
            "high": float(row["high"]),
            "low": float(row["low"]),
            "close": float(row["close"]),
            "volume": int(row["volume"]),
            "interpolated": False,
        }
        for row in rows
    ]


def _discovery_payload(evidence: Mapping[str, Any]) -> dict[str, Any]:
    """Preserve frozen discovery evidence without assuming an earnings catalyst."""
    discovery = evidence.get("discovery")
    if isinstance(discovery, Mapping):
        if not discovery:
            raise HistoricalBundleBuildError(
                "generic discovery evidence cannot be empty"
            )
        return dict(discovery)

    required = (
        "surprise_rank",
        "report_date",
        "report_timing",
        "eps_estimate",
        "eps_actual",
    )
    missing = [field for field in required if field not in evidence]
    if missing:
        raise HistoricalBundleBuildError(
            f"candidate discovery evidence is incomplete: {missing}"
        )
    return {
        "scanner_surprise_rank": int(evidence["surprise_rank"]),
        "report_date": evidence["report_date"],
        "report_timing": evidence["report_timing"],
        "eps_estimate": evidence["eps_estimate"],
        "eps_actual": evidence["eps_actual"],
    }


def _build_candidate(
    day: str,
    evidence: Mapping[str, Any],
    raw: Mapping[str, Any],
    benchmarks: Mapping[str, Sequence[Mapping[str, Any]]],
    rvol_rank: int,
    ranking_count: int,
    synthetic_equity: float,
) -> dict[str, Any]:
    symbol = str(evidence["symbol"])
    evaluation_time, clean_break, opening = determine_evaluation(raw["session_bars"])
    request = raw.get("request", {})
    if request.get("symbol") != symbol or request.get("date") != day:
        raise HistoricalBundleBuildError(
            f"{day} {symbol}: cached provider request mismatch"
        )
    if request.get("evaluation_time_et") != evaluation_time:
        raise HistoricalBundleBuildError(
            f"{day} {symbol}: cached evaluation {request.get('evaluation_time_et')} "
            f"does not match derived {evaluation_time}"
        )
    quotes = _quote_payload(raw)
    latest_bid = float(quotes[-1]["bid"])
    latest_ask = float(quotes[-1]["ask"])
    entry_limit = latest_ask
    midpoint = (latest_bid + latest_ask) / 2
    spreads = [float(row["ask"]) - float(row["bid"]) for row in quotes]
    median_spread = statistics.median(spreads)

    session_rows = _ordered_session_bars(raw["session_bars"])
    evaluation_clock = time.fromisoformat(evaluation_time)
    completed = [row for row in session_rows if _time_et(row) < evaluation_clock]
    vwap = _cumulative_vwap(completed)
    earlier_vwap = _available_cumulative_vwap(completed[:-5] or completed[:1])
    vwap_flat_or_rising = earlier_vwap is not None and vwap >= earlier_vwap
    recent_rows = completed[-3:] or session_rows[:5]
    support_candidates = [
        float(opening["high"]),
        vwap,
        min(float(row["low"]) for row in recent_rows),
    ]
    below_entry = [value for value in support_candidates if 0 < value < entry_limit]
    has_structural_invalidation = bool(below_entry)
    technical_invalidation = (
        max(below_entry)
        if below_entry
        else min(
            float(opening["low"]),
            min(float(row["low"]) for row in recent_rows),
            entry_limit * 0.999,
        )
    )

    daily = sorted(raw["daily_bars"], key=lambda row: int(row["epoch"]))
    atr = _atr14(daily)
    try:
        average_volume = average_daily_volume(daily, 14)
    except HistoricalMetricError as exc:
        raise HistoricalBundleBuildError(str(exc)) from exc
    prior_highs = [float(row["high"]) for row in daily]
    overhead = [value for value in prior_highs if value > entry_limit]
    resistance = min(overhead) if overhead else max(prior_highs)

    stop_distance = max(0.10 * atr, entry_limit - technical_invalidation)
    planned_stop = entry_limit - stop_distance
    stop_outside_noise = (
        has_structural_invalidation
        and planned_stop <= technical_invalidation
        and planned_stop <= min(float(row["low"]) for row in recent_rows)
        and planned_stop < min(float(row["bid"]) for row in quotes) - median_spread
        and stop_distance >= max(2 * median_spread, 0.001 * entry_limit)
    )

    benchmark_metrics = {
        name: _market_metrics(rows, evaluation_time)
        for name, rows in benchmarks.items()
    }
    benchmark_returns = [
        float(value["return_fraction"]) for value in benchmark_metrics.values()
    ]
    candidate_return = midpoint / float(opening["open"]) - 1
    above_vwap = midpoint >= vwap
    market_supportive = any(
        bool(value["above_vwap"]) and bool(value["vwap_flat_or_rising"])
        for value in benchmark_metrics.values()
    )
    independent_strength = (
        above_vwap and candidate_return >= max(benchmark_returns) + 0.002
    )
    quality = raw.get("session_bar_quality", {})
    tradable = (
        quality.get("complete") is True
        and all(float(row["bid"]) > 0 < float(row["ask"]) for row in quotes)
        and all(float(row["ask"]) >= float(row["bid"]) for row in quotes)
    )

    payload = {
        "session": {
            "time_et": evaluation_time,
            "mode": "shadow",
            "maturity": "UNVALIDATED",
            "agentic_allowed": True,
            "account_identified": True,
            "encryption_ready": True,
            "monitoring_available": True,
            "protective_stop_workflow_ready": True,
            "broker_review_available": True,
            "open_positions": 0,
            "unresolved_orders": 0,
            "filled_entries_today": 0,
            "circuit_breaker_active": False,
            "account_equity": synthetic_equity,
            "buying_power": synthetic_equity,
        },
        "candidate": {
            "symbol": symbol,
            "is_common_stock": evidence.get("is_common_stock") is True,
            "average_daily_volume_14": average_volume,
            "daily_atr_14": atr,
            "opening_bar": opening,
            "prior_opening_volumes": [
                int(value) for value in raw["prior_opening_volumes"]
            ],
            "opening_rvol_rank": rvol_rank,
            "ranking_scope": "scanner_results",
            "ranking_scope_count": ranking_count,
            "verified_catalyst": True,
            "catalyst_score": 25,
            "dilution_conflict": evidence.get("dilution_conflict") is True,
            "halt_risk": quality.get("complete") is not True,
            "tradable": tradable,
            "clean_break": clean_break,
            "above_vwap": above_vwap,
            "vwap_flat_or_rising": vwap_flat_or_rising,
            "benchmark_supportive_or_independent_strength": market_supportive
            or independent_strength,
            "sector_relative_strength": candidate_return > max(benchmark_returns),
            "stop_outside_noise": stop_outside_noise,
            "entry_limit": entry_limit,
            "technical_invalidation": technical_invalidation,
            "resistance_price": resistance,
            "observed_stop_slippage_p95_fraction": 0.0,
        },
        "quotes": quotes,
    }
    # Prove every derived payload is evaluator-safe before it reaches a bundle.
    evaluate_candidate(payload, load_config())
    return {
        "signal_id": f"{day}-{symbol}-1",
        "symbol": symbol,
        "evaluation_time_et": evaluation_time,
        "evaluation_basis": "next_minute_after_completed_breakout_bar",
        "catalyst": dict(evidence["catalyst"]),
        "discovery": _discovery_payload(evidence),
        "market_alignment": {
            "candidate_return_fraction": candidate_return,
            "candidate_vwap": vwap,
            "benchmarks": benchmark_metrics,
        },
        "data_provenance": dict(raw.get("provenance", {})),
        "evaluation_payload": payload,
        "bars": _replay_bars(raw),
    }


def _frozen_evidence_hash(
    day: str,
    evidence_rows: Sequence[Mapping[str, Any]],
    scanner: Mapping[str, Any],
) -> str:
    return _canonical_hash(
        {
            "date": day,
            "candidates": list(evidence_rows),
            "scanner": dict(scanner),
        }
    )


def build_bundle(
    day: str,
    evidence_rows: Sequence[Mapping[str, Any]],
    raw_by_symbol: Mapping[str, Mapping[str, Any]],
    benchmarks: Mapping[str, Sequence[Mapping[str, Any]]],
    *,
    synthetic_equity: float,
    scanner: Mapping[str, Any],
    benchmark_providers: Mapping[str, str] | None = None,
    preregistration: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    if len(evidence_rows) < 10:
        raise HistoricalBundleBuildError(
            f"{day}: frozen universe has fewer than ten candidates"
        )
    rvols: dict[str, float] = {}
    for evidence in evidence_rows:
        symbol = str(evidence["symbol"])
        raw = raw_by_symbol[symbol]
        provider = raw.get("provider")
        if not isinstance(provider, str) or not provider.strip():
            raise HistoricalBundleBuildError(
                f"{day} {symbol}: market-data provider provenance is missing"
            )
        opening_volume = float(raw["opening_bar"]["volume"])
        prior = [float(value) for value in raw["prior_opening_volumes"]]
        if len(prior) != 14 or any(value <= 0 for value in prior):
            raise HistoricalBundleBuildError(
                f"{day} {symbol}: invalid opening-volume lookback"
            )
        rvols[symbol] = opening_volume / statistics.fmean(prior)
    ranked = sorted(rvols, key=lambda symbol: (-rvols[symbol], symbol))
    ranks = {symbol: index + 1 for index, symbol in enumerate(ranked)}
    candidates = [
        _build_candidate(
            day,
            evidence,
            raw_by_symbol[str(evidence["symbol"])],
            benchmarks,
            ranks[str(evidence["symbol"])],
            len(evidence_rows),
            synthetic_equity,
        )
        for evidence in evidence_rows
    ]
    candidate_providers = {
        str(evidence["symbol"]): str(
            raw_by_symbol[str(evidence["symbol"])].get("provider", "unknown")
        )
        for evidence in evidence_rows
    }
    all_market_providers = set(candidate_providers.values())
    all_market_providers.update((benchmark_providers or {}).values())
    market_provider_text = " + ".join(sorted(all_market_providers))
    bundle = {
        "schema_version": BUNDLE_SCHEMA_VERSION,
        "date": day,
        "sample_phase": "confirmation" if preregistration is not None else "pilot",
        "session_capture_complete": True,
        "simulation_account_equity": synthetic_equity,
        "simulation_buying_power": synthetic_equity,
        "source": {
            "provider": (
                "Robinhood earnings calendar + SEC/issuer catalysts + "
                f"{market_provider_text}"
            ),
            "market_data_providers": sorted(all_market_providers),
            "candidate_provider_by_symbol": candidate_providers,
            "benchmark_provider_by_symbol": dict(benchmark_providers or {}),
            "captured_at": datetime.now(UTC).isoformat(),
            "point_in_time": True,
            "regular_hours_only": True,
            "split_adjusted": True,
            "historical_quotes_and_depth": True,
            "catalysts_point_in_time": True,
            "universe_capture_complete": scanner.get("universe_capture_complete")
            is True,
            "frozen_evidence_sha256": _frozen_evidence_hash(
                day, evidence_rows, scanner
            ),
            "scanner_definition": dict(scanner),
        },
        "candidates": candidates,
    }
    if preregistration is not None:
        bundle["preregistration"] = dict(preregistration)
    return bundle


def _load_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise HistoricalBundleBuildError(f"cannot read {path}: {exc}") from exc


def _canonical_hash(value: Mapping[str, Any]) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _load_pre_session_history(
    preflight: Mapping[str, Any] | None,
    day: str,
    symbol: str,
) -> Mapping[str, Any] | None:
    if not isinstance(preflight, Mapping):
        return None
    cache = preflight.get("cache")
    if (
        not isinstance(cache, Mapping)
        or cache.get("reusable_pre_session_history") is not True
    ):
        return None
    cache_schema = cache.get("schema_version")
    if cache_schema not in (1, 2):
        return None
    root_text = cache.get("root")
    if not isinstance(root_text, str) or not root_text.strip():
        return None
    root = Path(root_text)
    if not root.is_absolute():
        root = PROJECT_ROOT / root
    path = root / day / f"{symbol}.json"
    if not path.exists():
        return None
    try:
        payload = _load_json(path)
    except HistoricalBundleBuildError:
        return None
    if not isinstance(payload, Mapping):
        return None
    if (
        payload.get("schema_version") != cache_schema
        or payload.get("probe_contract_version") != PRE_SESSION_CACHE_VERSION
        or payload.get("symbol") != symbol
        or payload.get("session_date") != day
    ):
        return None
    if cache_schema >= 2 and payload.get("qualification_sha256") != cache.get(
        "qualification_sha256"
    ):
        return None
    result = payload.get("result")
    history = payload.get("pre_session_history")
    if (
        not isinstance(result, Mapping)
        or result.get("viable") is not True
        or not isinstance(history, Mapping)
    ):
        return None
    if cache_schema >= 2:
        expected_hash = result.get("pre_session_history_sha256")
        dates = preflight.get("dates")
        date_report = dates.get(day) if isinstance(dates, Mapping) else None
        accepted = (
            date_report.get("accepted") if isinstance(date_report, Mapping) else None
        )
        accepted_row = (
            next(
                (
                    row
                    for row in accepted
                    if isinstance(row, Mapping) and row.get("symbol") == symbol
                ),
                None,
            )
            if isinstance(accepted, list)
            else None
        )
        manifest_hash = (
            accepted_row.get("pre_session_history_sha256")
            if isinstance(accepted_row, Mapping)
            else None
        )
        actual_hash = _canonical_hash(history)
        if (
            not isinstance(expected_hash, str)
            or expected_hash != actual_hash
            or manifest_hash != actual_hash
        ):
            return None
    return history


def _write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    rendered = json.dumps(value, indent=2, sort_keys=True) + "\n"
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(rendered, encoding="utf-8")
    temporary.replace(path)


def _failure_row(
    day: str,
    symbol: str,
    stage: str,
    exc: BaseException,
) -> dict[str, Any]:
    category = (
        exc.category
        if isinstance(exc, HistoricalProviderError)
        else historical_error_category(exc)
    )
    if isinstance(exc, HistoricalBundleBuildError) and stage in (
        "benchmark",
        "benchmark_fallback",
        "candidate",
        "candidate_fallback",
    ):
        category = "permanent_fidelity"
    return {
        "date": day,
        "symbol": symbol,
        "stage": stage,
        "category": category,
        "retryable": category.startswith("retryable_"),
        "error_type": type(exc).__name__,
        "error": str(exc),
    }


def _collection_status(
    manifest_path: Path,
    selection_seed: int | None,
    selection_integrity: Mapping[str, Any],
    selected_dates: Sequence[str],
    built_dates: Sequence[str],
    failures: Sequence[Mapping[str, Any]],
    recoveries: Sequence[Mapping[str, Any]],
    precollection_blockers: Mapping[str, Mapping[str, Any]],
    *,
    interrupted: bool,
    created_at: str | None,
) -> dict[str, Any]:
    now = datetime.now(UTC).isoformat()
    blocked: list[dict[str, Any]] = []
    for day in selected_dates:
        if day in built_dates:
            continue
        precollection = precollection_blockers.get(day)
        if precollection is not None:
            blocked.append(
                {
                    "date": day,
                    "reason_code": str(precollection["reason_code"]),
                    "detail": "candidate universe blocked before target-session collection",
                    "failures": [],
                    "precollection_blocker": dict(precollection),
                }
            )
            continue
        day_failures = [dict(row) for row in failures if row.get("date") == day]
        if not day_failures:
            continue
        first = next(
            (row for row in reversed(day_failures) if not row.get("retryable")),
            day_failures[-1],
        )
        blocked.append(
            {
                "date": day,
                "reason_code": str(first["category"]),
                "detail": str(first["error"]),
                "failures": day_failures,
            }
        )
    built = list(dict.fromkeys(built_dates))
    return {
        "schema_version": 1,
        "mode": "historical_collection",
        "manifest": manifest_path.name,
        "seed": selection_seed,
        "selection_integrity": dict(selection_integrity),
        "created_at": created_at or now,
        "updated_at": now,
        "requested_days": len(selected_dates),
        "ready_dates": built,
        "blocked_dates": blocked,
        "fallback_recoveries": [dict(row) for row in recoveries],
        "pending_dates": [
            day
            for day in selected_dates
            if day not in built and not any(row["date"] == day for row in blocked)
        ],
        "substituted_dates": [],
        "cascade_error_count": 0,
        "engineering_acceptance": batch_acceptance(
            len(selected_dates),
            len(built),
            substitution_count=0,
            cascade_error_count=0,
        ),
        "interrupted": interrupted,
        "valid": len(built) + len(blocked) == len(selected_dates),
    }


def _provider_name(client: HistoricalMarketDataClient) -> str:
    return str(getattr(client, "provider_name", type(client).__name__))


def _selection_metadata(
    manifest_path: Path,
    manifest: Mapping[str, Any],
    candidates_by_date: Mapping[str, Any],
) -> tuple[list[str], int | None, dict[str, Any]]:
    candidate_dates = [str(day) for day in candidates_by_date]
    copied_seed = manifest.get("selection_seed")
    if copied_seed is not None and not isinstance(copied_seed, int):
        raise HistoricalBundleBuildError("selection_seed must be an integer")
    selection_file = manifest.get("selection_file")
    if not isinstance(selection_file, str) or not selection_file.strip():
        return (
            candidate_dates,
            copied_seed,
            {
                "source": "evidence_manifest",
                "date_set_matches": True,
                "copied_seed_matches": True,
            },
        )
    selection_path = Path(selection_file)
    if not selection_path.is_absolute():
        project_path = PROJECT_ROOT / selection_path
        local_path = manifest_path.parent / selection_path
        selection_path = project_path if project_path.exists() else local_path
    selection = _load_json(selection_path)
    if not isinstance(selection, Mapping):
        raise HistoricalBundleBuildError("selection manifest must be an object")
    seed = selection.get("seed")
    dates = selection.get("selected_dates")
    if (
        not isinstance(seed, int)
        or not isinstance(dates, list)
        or not all(isinstance(day, str) for day in dates)
    ):
        raise HistoricalBundleBuildError(
            "selection manifest needs an integer seed and selected_dates array"
        )
    if len(dates) != len(set(dates)):
        raise HistoricalBundleBuildError("selection manifest dates must be unique")
    if set(dates) != set(candidate_dates):
        raise HistoricalBundleBuildError(
            "evidence candidate dates do not match the linked selection manifest"
        )
    return (
        list(dates),
        seed,
        {
            "source": selection_path.name,
            "date_set_matches": True,
            "copied_seed_matches": copied_seed in (None, seed),
            "copied_seed": copied_seed,
            "authoritative_seed": seed,
        },
    )


def _confirmation_collection_context(
    evidence_path: Path,
    evidence: Mapping[str, Any],
    confirmation_manifest_path: Path,
) -> dict[str, Any]:
    """Bind collection to a verified preregistration without importing prices."""

    # Local import avoids making the general pilot builder depend on the
    # strategy-lab module during ordinary collection.
    from historical_strategy_lab import (  # pylint: disable=import-outside-toplevel
        HistoricalStrategyLabError,
        load_confirmation_manifest,
    )

    try:
        confirmation = load_confirmation_manifest(confirmation_manifest_path.resolve())
    except HistoricalStrategyLabError as exc:
        raise HistoricalBundleBuildError(str(exc)) from exc
    source = confirmation.get("source_evidence")
    evidence_sha = hashlib.sha256(evidence_path.resolve().read_bytes()).hexdigest()
    if not isinstance(source, Mapping) or source.get("sha256") != evidence_sha:
        raise HistoricalBundleBuildError(
            "confirmation manifest belongs to different source evidence"
        )
    candidates_by_date = evidence.get("candidates_by_date")
    blocked_by_date = evidence.get("blocked_candidates_by_date", {})
    if not isinstance(candidates_by_date, Mapping) or not isinstance(
        blocked_by_date, Mapping
    ):
        raise HistoricalBundleBuildError(
            "confirmation evidence candidate universes are malformed"
        )
    scanner = evidence.get("scanner")
    if not isinstance(scanner, Mapping):
        raise HistoricalBundleBuildError("confirmation evidence scanner is missing")
    frozen_dates = confirmation.get("frozen_dates")
    if not isinstance(frozen_dates, list):
        raise HistoricalBundleBuildError(
            "confirmation manifest frozen dates are missing"
        )
    selected_dates: list[str] = []
    precollection_blockers: dict[str, Mapping[str, Any]] = {}
    ready_dates: set[str] = set()
    for frozen in frozen_dates:
        if not isinstance(frozen, Mapping):
            raise HistoricalBundleBuildError("confirmation frozen date is malformed")
        day = str(frozen.get("date", ""))
        selected_dates.append(day)
        status = str(frozen.get("status", "validation_ready"))
        expected_symbols = list(frozen.get("ordered_symbols", []))
        if status == "precollection_blocked":
            rows = blocked_by_date.get(day)
            if (
                not isinstance(rows, list)
                or [row.get("symbol") for row in rows] != expected_symbols
            ):
                raise HistoricalBundleBuildError(
                    f"{day}: blocked universe differs from confirmation manifest"
                )
            blocker = frozen.get("precollection_blocker")
            if not isinstance(blocker, Mapping):
                raise HistoricalBundleBuildError(
                    f"{day}: confirmation precollection blocker is missing"
                )
            precollection_blockers[day] = dict(blocker)
            continue
        rows = candidates_by_date.get(day)
        if (
            not isinstance(rows, list)
            or [row.get("symbol") for row in rows] != expected_symbols
        ):
            raise HistoricalBundleBuildError(
                f"{day}: candidate order differs from confirmation manifest"
            )
        if _frozen_evidence_hash(day, rows, scanner) != frozen.get(
            "frozen_evidence_sha256"
        ):
            raise HistoricalBundleBuildError(
                f"{day}: frozen evidence differs from confirmation manifest"
            )
        ready_dates.add(day)
    if ready_dates != {str(day) for day in candidates_by_date}:
        raise HistoricalBundleBuildError(
            "confirmation manifest does not cover the exact collectable date set"
        )
    return {
        "selected_dates": selected_dates,
        "selection_seed": source.get("selection_seed"),
        "selection_integrity": {
            "source": confirmation_manifest_path.name,
            "confirmation_manifest_sha256": confirmation["manifest_sha256"],
            "date_set_matches": True,
            "copied_seed_matches": True,
        },
        "precollection_blockers": precollection_blockers,
        "preregistration": {
            "manifest_hash": confirmation["manifest_sha256"],
            "registered_at": confirmation["registered_at"],
        },
    }


def _benchmark_history(
    client: HistoricalMarketDataClient, day: str, symbol: str
) -> dict[str, Any]:
    start = datetime.combine(date.fromisoformat(day), time(9, 30), tzinfo=EASTERN)
    end = datetime.combine(date.fromisoformat(day), time(16, 0), tzinfo=EASTERN)
    bars = client.fetch_bars(symbol, start, end, bar_size="1 min", what="TRADES")
    _ordered_session_bars(bars)
    return {
        "provider": _provider_name(client),
        "captured_at": datetime.now(UTC).isoformat(),
        "symbol": symbol,
        "date": day,
        "session_bars": bars,
    }


def _validate_candidate_raw(raw: Mapping[str, Any], day: str, symbol: str) -> None:
    request = raw.get("request")
    if not isinstance(request, Mapping):
        raise HistoricalBundleBuildError(f"{day} {symbol}: cached request is missing")
    if request.get("symbol") != symbol or request.get("date") != day:
        raise HistoricalBundleBuildError(
            f"{day} {symbol}: cached provider request mismatch"
        )
    evaluation_time, _, _ = determine_evaluation(raw["session_bars"])
    if request.get("evaluation_time_et") != evaluation_time:
        raise HistoricalBundleBuildError(f"{day} {symbol}: cached evaluation mismatch")
    prior = raw.get("prior_opening_volumes")
    if not isinstance(prior, list) or len(prior) != 14:
        raise HistoricalBundleBuildError(
            f"{day} {symbol}: opening-volume lookback needs 14 sessions"
        )
    if any(float(value) <= 0 for value in prior):
        raise HistoricalBundleBuildError(
            f"{day} {symbol}: opening-volume lookback contains nonpositive volume"
        )
    daily = raw.get("daily_bars")
    if not isinstance(daily, list) or len(daily) < 15:
        raise HistoricalBundleBuildError(
            f"{day} {symbol}: daily history needs at least 15 sessions"
        )
    _quote_payload(raw)


def _collect_candidate_raw(
    client: HistoricalMarketDataClient,
    symbol: str,
    day: str,
    *,
    pre_session_history: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    start = datetime.combine(date.fromisoformat(day), time(9, 30), tzinfo=EASTERN)
    end = datetime.combine(date.fromisoformat(day), time(16, 0), tzinfo=EASTERN)
    preview = client.fetch_bars(symbol, start, end, bar_size="1 min", what="TRADES")
    evaluation_time, _, _ = determine_evaluation(preview)
    raw = collect_candidate_history(
        client,
        symbol,
        day,
        evaluation_time,
        session_bars=preview,
        pre_session_history=pre_session_history,
    )
    _validate_candidate_raw(raw, day, symbol)
    return raw


def _collect_candidate_with_preflight_fallback(
    client: HistoricalMarketDataClient,
    symbol: str,
    day: str,
    pre_session_history: Mapping[str, Any] | None,
) -> tuple[dict[str, Any], bool]:
    if pre_session_history is None:
        return _collect_candidate_raw(client, symbol, day), False
    try:
        return (
            _collect_candidate_raw(
                client,
                symbol,
                day,
                pre_session_history=pre_session_history,
            ),
            True,
        )
    except IBKRConfigurationError:
        return _collect_candidate_raw(client, symbol, day), False


def _load_or_collect_benchmark(
    client: HistoricalMarketDataClient,
    raw_root: Path,
    day: str,
    symbol: str,
) -> tuple[dict[str, Any], bool]:
    path = raw_root / f"{day}-{symbol}-benchmark.json"
    cache_hit = path.exists()
    raw = _load_json(path) if cache_hit else _benchmark_history(client, day, symbol)
    if not isinstance(raw, Mapping):
        raise HistoricalBundleBuildError(f"{day} {symbol}: raw benchmark is invalid")
    _ordered_session_bars(raw["session_bars"])
    normalized = dict(raw)
    if not cache_hit:
        _write_json(path, normalized)
    return normalized, cache_hit


def _load_or_collect_candidate(
    client: HistoricalMarketDataClient,
    raw_root: Path,
    preflight: Mapping[str, Any] | None,
    day: str,
    evidence: Mapping[str, Any],
) -> tuple[dict[str, Any], bool, bool]:
    symbol = str(evidence["symbol"])
    path = raw_root / f"{day}-{symbol}.json"
    cache_hit = path.exists()
    history: Mapping[str, Any] | None = None
    reused_preflight = False
    if cache_hit:
        raw = _load_json(path)
        if isinstance(raw, Mapping):
            normalized = dict(raw)
            try:
                _validate_candidate_raw(normalized, day, symbol)
                return normalized, True, False
            except (HistoricalBundleBuildError, KeyError, TypeError, ValueError):
                # Raw provider caches are disposable. Refresh an incompatible
                # payload once instead of turning an old derivation contract
                # into a permanent point-in-time data failure.
                pass
    history = _load_pre_session_history(preflight, day, symbol)
    raw, reused_preflight = _collect_candidate_with_preflight_fallback(
        client,
        symbol,
        day,
        history,
    )
    if not isinstance(raw, Mapping):
        raise HistoricalBundleBuildError(f"{day} {symbol}: raw candidate is invalid")
    normalized = dict(raw)
    _validate_candidate_raw(normalized, day, symbol)
    _write_json(path, normalized)
    return normalized, False, reused_preflight


def collect_manifest(
    manifest_path: Path,
    *,
    data_root: Path = DEFAULT_DATA_ROOT,
    env_file: Path = DEFAULT_ENV_PATH,
    synthetic_equity: float = 25_000.0,
    status_path: Path | None = None,
    transport_retries: int = 1,
    max_workers: int = DEFAULT_COLLECTION_WORKERS,
    confirmation_manifest_path: Path | None = None,
    historical_store: HistoricalDayStore | None = None,
) -> dict[str, Any]:
    if isinstance(max_workers, bool) or max_workers < 1:
        raise HistoricalBundleBuildError("max_workers must be a positive integer")
    collection_started = monotonic()
    manifest = _load_json(manifest_path)
    if not isinstance(manifest, Mapping):
        raise HistoricalBundleBuildError("evidence manifest must be an object")
    scanner = manifest.get("scanner")
    candidates_by_date = manifest.get("candidates_by_date")
    if not isinstance(scanner, Mapping) or not isinstance(candidates_by_date, Mapping):
        raise HistoricalBundleBuildError(
            "manifest needs scanner and candidates_by_date objects"
        )
    _number(synthetic_equity, "synthetic_equity", positive=True)
    if transport_retries < 0:
        raise HistoricalBundleBuildError("transport_retries cannot be negative")
    raw_root = data_root / "ibkr"
    fallback_root = data_root / "massive"
    built: list[str] = []
    failures: list[dict[str, Any]] = []
    recoveries: list[dict[str, Any]] = []
    selected_dates, selection_seed, selection_integrity = _selection_metadata(
        manifest_path, manifest, candidates_by_date
    )
    precollection_blockers: Mapping[str, Mapping[str, Any]] = {}
    preregistration: Mapping[str, str] | None = None
    if confirmation_manifest_path is not None:
        confirmation_context = _confirmation_collection_context(
            manifest_path,
            manifest,
            confirmation_manifest_path,
        )
        selected_dates = confirmation_context["selected_dates"]
        selection_seed = confirmation_context["selection_seed"]
        selection_integrity = confirmation_context["selection_integrity"]
        precollection_blockers = confirmation_context["precollection_blockers"]
        preregistration = confirmation_context["preregistration"]
    raw_preflight = manifest.get("preflight")
    preflight = raw_preflight if isinstance(raw_preflight, Mapping) else None
    created_at: str | None = None
    if status_path is not None and status_path.exists():
        previous = _load_json(status_path)
        if isinstance(previous, Mapping) and isinstance(
            previous.get("created_at"), str
        ):
            created_at = str(previous["created_at"])

    def persist(*, interrupted: bool) -> None:
        nonlocal created_at
        if status_path is None:
            return
        status = _collection_status(
            manifest_path,
            selection_seed,
            selection_integrity,
            selected_dates,
            built,
            failures,
            recoveries,
            precollection_blockers,
            interrupted=interrupted,
            created_at=created_at,
        )
        _write_json(status_path, status)
        created_at = str(status["created_at"])

    persist(interrupted=False)
    ibkr_request_profiles: list[dict[str, Any]] = []
    provider_startup: list[dict[str, Any]] = []
    performance_counters: Counter[str] = Counter()
    if historical_store is not None:
        with open_provider_set(env_file, historical_store) as providers:
            provider_startup = [
                attempt.public_dict() for attempt in providers.startup_attempts
            ]
            if not providers.live_clients:
                raise HistoricalProviderError(
                    "IBKR, Massive, and Alpaca are all unavailable",
                    category="local_configuration",
                )
            primary = providers.live_clients[0]
            primary_namespace = str(
                getattr(primary, "cache_namespace", "provider")
            )
            fallbacks = [
                (
                    candidate,
                    data_root
                    / str(getattr(candidate, "cache_namespace", "provider")),
                )
                for candidate in providers.live_clients[1:]
            ]
            _collect_manifest_dates(
                primary,
                fallbacks,
                candidates_by_date,
                scanner,
                preflight if primary_namespace == "ibkr" else None,
                data_root=data_root,
                raw_root=data_root / primary_namespace,
                fallback_root=fallback_root,
                synthetic_equity=synthetic_equity,
                built=built,
                failures=failures,
                recoveries=recoveries,
                persist=persist,
                max_workers=max_workers,
                performance=performance_counters,
                preregistration=preregistration,
                fallback_on_retryable=True,
            )
            telemetry = getattr(primary, "request_telemetry", None)
            if callable(telemetry) and primary_namespace == "ibkr":
                ibkr_request_profiles.append(telemetry())
    else:
        config = IBKRConfig.from_env(env_file)
        massive_config = MassiveConfig.optional_from_env(env_file)
        fallback_client = (
            MassiveHistoricalClient(massive_config)
            if massive_config is not None
            else None
        )
        try:
            attempt = 0
            while True:
                try:
                    with IBKRHistoricalClient(config) as client:
                        try:
                            _collect_manifest_dates(
                                client,
                                (
                                    [(fallback_client, fallback_root)]
                                    if fallback_client is not None
                                    else []
                                ),
                                candidates_by_date,
                                scanner,
                                preflight,
                                data_root=data_root,
                                raw_root=raw_root,
                                fallback_root=fallback_root,
                                synthetic_equity=synthetic_equity,
                                built=built,
                                failures=failures,
                                recoveries=recoveries,
                                persist=persist,
                                max_workers=max_workers,
                                performance=performance_counters,
                                preregistration=preregistration,
                            )
                        finally:
                            telemetry = getattr(client, "request_telemetry", None)
                            if callable(telemetry):
                                ibkr_request_profiles.append(telemetry())
                    break
                except (IBKRHistoricalError, HistoricalProviderError) as exc:
                    retryable = (
                        is_retryable_historical_error(exc)
                        if isinstance(exc, IBKRHistoricalError)
                        else exc.retryable
                    )
                    if retryable:
                        persist(interrupted=True)
                    if not retryable or attempt >= transport_retries:
                        raise
                    attempt += 1
                    print(
                        f"historical provider interrupted; reconnecting "
                        f"({attempt}/{transport_retries})",
                        file=sys.stderr,
                        flush=True,
                    )
        finally:
            if fallback_client is not None:
                fallback_client.close()
    result = {
        "built_dates": built,
        "blocked_dates": [day for day in selected_dates if day not in built],
        "failures": failures,
        "fallback_recoveries": recoveries,
        "valid": (
            len(built) == len(candidates_by_date)
            and len(built) + len(precollection_blockers) == len(selected_dates)
        ),
        "performance": {
            "elapsed_seconds": monotonic() - collection_started,
            "max_workers": max_workers,
            "ibkr_connections": ibkr_request_profiles,
            "provider_startup": provider_startup,
            "counters": dict(sorted(performance_counters.items())),
        },
    }
    persist(interrupted=False)
    if status_path is not None:
        final_status = _load_json(status_path)
        if isinstance(final_status, Mapping):
            final_status = dict(final_status)
            final_status["performance"] = result["performance"]
            _write_json(status_path, final_status)
    return result


def _collect_manifest_dates(
    client: IBKRHistoricalClient,
    fallback_clients: Sequence[tuple[HistoricalMarketDataClient, Path]],
    candidates_by_date: Mapping[str, Any],
    scanner: Mapping[str, Any],
    preflight: Mapping[str, Any] | None,
    *,
    data_root: Path,
    raw_root: Path,
    fallback_root: Path,
    synthetic_equity: float,
    built: list[str],
    failures: list[dict[str, Any]],
    recoveries: list[dict[str, Any]],
    persist: Any,
    max_workers: int,
    performance: Counter[str],
    preregistration: Mapping[str, str] | None,
    fallback_on_retryable: bool = False,
) -> None:
    active_fallbacks = list(fallback_clients)
    for day, values in candidates_by_date.items():
        if not isinstance(day, str) or not isinstance(values, list):
            raise HistoricalBundleBuildError("candidate dates must map to arrays")
        print(f"collecting {day}", file=sys.stderr, flush=True)
        bundle_path = data_root / f"{day}.json"
        if bundle_path.exists():
            try:
                cached_bundle = _load_json(bundle_path)
                validate_bundle(cached_bundle)
                expected_evidence_hash = _frozen_evidence_hash(day, values, scanner)
                source = cached_bundle.get("source")
                if (
                    not isinstance(source, Mapping)
                    or source.get("frozen_evidence_sha256") != expected_evidence_hash
                ):
                    raise HistoricalBundleBuildError(
                        f"{day}: cached bundle belongs to different frozen evidence"
                    )
                if preregistration is not None and (
                    cached_bundle.get("sample_phase") != "confirmation"
                    or cached_bundle.get("preregistration") != preregistration
                ):
                    raise HistoricalBundleBuildError(
                        f"{day}: cached bundle belongs to different preregistration"
                    )
                if day not in built:
                    built.append(day)
                print("  validated bundle already ready", file=sys.stderr, flush=True)
                performance["bundle_cache_hits"] += 1
                persist(interrupted=False)
                continue
            except (HistoricalBundleBuildError, HistoricalLearningError):
                pass
        benchmark_rows: dict[str, Sequence[Mapping[str, Any]]] = {}
        benchmark_providers: dict[str, str] = {}
        date_blocked = False
        benchmark_outcomes = ordered_bounded_results(
            BENCHMARKS,
            lambda symbol: _load_or_collect_benchmark(client, raw_root, day, symbol),
            max_workers=min(max_workers, len(BENCHMARKS)),
        )
        try:
            for outcome in benchmark_outcomes:
                symbol = outcome.item
                try:
                    raw, cache_hit = outcome.unwrap()
                    performance[
                        "benchmark_cache_hits"
                        if cache_hit
                        else "benchmark_cache_misses"
                    ] += 1
                    benchmark_rows[symbol] = raw["session_bars"]
                    benchmark_providers[symbol] = str(raw["provider"])
                except (
                    HistoricalBundleBuildError,
                    IBKRHistoricalError,
                    HistoricalProviderError,
                    OSError,
                    KeyError,
                ) as exc:
                    if (
                        isinstance(exc, IBKRHistoricalError)
                        and is_retryable_historical_error(exc)
                        and (not active_fallbacks or not fallback_on_retryable)
                    ):
                        failure = _failure_row(day, symbol, "benchmark", exc)
                        failures.append(failure)
                        persist(interrupted=True)
                        raise
                    fallback_failed = False
                    fallback_errors: list[dict[str, Any]] = []
                    recovered = False
                    for fallback_client, fallback_root in list(active_fallbacks):
                        try:
                            fallback_raw, _ = _load_or_collect_benchmark(
                                fallback_client,
                                fallback_root,
                                day,
                                symbol,
                            )
                            benchmark_rows[symbol] = fallback_raw["session_bars"]
                            benchmark_providers[symbol] = str(fallback_raw["provider"])
                            recoveries.append(
                                {
                                    "date": day,
                                    "symbol": symbol,
                                    "stage": "benchmark",
                                    "primary_provider": _provider_name(client),
                                    "primary_error": str(exc),
                                    "fallback_provider": _provider_name(
                                        fallback_client
                                    ),
                                }
                            )
                            print(
                                f"  {symbol} recovered by {_provider_name(fallback_client)}",
                                file=sys.stderr,
                                flush=True,
                            )
                            recovered = True
                            break
                        except (
                            HistoricalBundleBuildError,
                            HistoricalProviderError,
                            IBKRHistoricalError,
                            OSError,
                            KeyError,
                        ) as fallback_exc:
                            fallback_failed = True
                            fallback_failure = _failure_row(
                                day, symbol, "benchmark_fallback", fallback_exc
                            )
                            fallback_failure["provider"] = _provider_name(
                                fallback_client
                            )
                            fallback_errors.append(fallback_failure)
                            if (
                                isinstance(fallback_exc, HistoricalProviderError)
                                and fallback_exc.category == "permanent_permission"
                            ):
                                print(
                                    f"  {_provider_name(fallback_client)} fallback "
                                    "disabled after permission failure",
                                    file=sys.stderr,
                                    flush=True,
                                )
                                active_fallbacks.remove(
                                    (fallback_client, fallback_root)
                                )
                    if recovered:
                        continue
                    if fallback_errors:
                        failure = dict(fallback_errors[-1])
                        failure["primary_error"] = str(exc)
                        failure["fallback_errors"] = fallback_errors
                    else:
                        failure = _failure_row(day, symbol, "benchmark", exc)
                    failures.append(failure)
                    persist(interrupted=failure["retryable"] and not fallback_failed)
                    if failure["retryable"] and not fallback_failed:
                        raise HistoricalProviderError(
                            failure["error"], category=failure["category"]
                        )
                    date_blocked = True
                    print(
                        f"  {symbol} blocked: {failure['error']}",
                        file=sys.stderr,
                        flush=True,
                    )
                    break
        finally:
            benchmark_outcomes.close()
        if date_blocked:
            continue
        raw_by_symbol: dict[str, Mapping[str, Any]] = {}
        candidate_values: list[Mapping[str, Any]] = []
        for evidence in values:
            if not isinstance(evidence, Mapping) or not isinstance(
                evidence.get("symbol"), str
            ):
                raise HistoricalBundleBuildError(f"{day}: malformed candidate evidence")
            candidate_values.append(evidence)
        candidate_outcomes = ordered_bounded_results(
            candidate_values,
            lambda evidence: _load_or_collect_candidate(
                client,
                raw_root,
                preflight,
                day,
                evidence,
            ),
            max_workers=max_workers,
        )
        try:
            for outcome in candidate_outcomes:
                evidence = outcome.item
                symbol = str(evidence["symbol"])
                try:
                    raw, cache_hit, reused_preflight = outcome.unwrap()
                    performance[
                        "candidate_cache_hits"
                        if cache_hit
                        else "candidate_cache_misses"
                    ] += 1
                    if reused_preflight:
                        performance["preflight_histories_reused"] += 1
                    raw_by_symbol[symbol] = raw
                    if cache_hit:
                        suffix = " (raw cache hit)"
                    elif reused_preflight:
                        suffix = " (preflight reused)"
                    else:
                        suffix = ""
                    print(f"  {symbol} ready{suffix}", file=sys.stderr, flush=True)
                except (
                    HistoricalBundleBuildError,
                    IBKRHistoricalError,
                    HistoricalProviderError,
                    OSError,
                    KeyError,
                    TypeError,
                    ValueError,
                ) as exc:
                    if (
                        isinstance(exc, IBKRHistoricalError)
                        and is_retryable_historical_error(exc)
                        and (not active_fallbacks or not fallback_on_retryable)
                    ):
                        failure = _failure_row(day, symbol, "candidate", exc)
                        failures.append(failure)
                        persist(interrupted=True)
                        raise
                    fallback_failed = False
                    fallback_errors = []
                    recovered = False
                    for fallback_client, fallback_root in list(active_fallbacks):
                        try:
                            fallback_raw, _, _ = _load_or_collect_candidate(
                                fallback_client,
                                fallback_root,
                                None,
                                day,
                                evidence,
                            )
                            raw_by_symbol[symbol] = fallback_raw
                            recoveries.append(
                                {
                                    "date": day,
                                    "symbol": symbol,
                                    "stage": "candidate",
                                    "primary_provider": _provider_name(client),
                                    "primary_error": str(exc),
                                    "fallback_provider": _provider_name(
                                        fallback_client
                                    ),
                                }
                            )
                            print(
                                f"  {symbol} recovered by {_provider_name(fallback_client)}",
                                file=sys.stderr,
                                flush=True,
                            )
                            recovered = True
                            break
                        except (
                            HistoricalBundleBuildError,
                            HistoricalProviderError,
                            IBKRHistoricalError,
                            OSError,
                            KeyError,
                            TypeError,
                            ValueError,
                        ) as fallback_exc:
                            fallback_failed = True
                            fallback_failure = _failure_row(
                                day, symbol, "candidate_fallback", fallback_exc
                            )
                            fallback_failure["provider"] = _provider_name(
                                fallback_client
                            )
                            fallback_errors.append(fallback_failure)
                            if (
                                isinstance(fallback_exc, HistoricalProviderError)
                                and fallback_exc.category == "permanent_permission"
                            ):
                                print(
                                    f"  {_provider_name(fallback_client)} fallback "
                                    "disabled after permission failure",
                                    file=sys.stderr,
                                    flush=True,
                                )
                                active_fallbacks.remove(
                                    (fallback_client, fallback_root)
                                )
                    if recovered:
                        continue
                    if fallback_errors:
                        failure = dict(fallback_errors[-1])
                        failure["primary_error"] = str(exc)
                        failure["fallback_errors"] = fallback_errors
                    else:
                        failure = _failure_row(day, symbol, "candidate", exc)
                    failures.append(failure)
                    print(
                        f"  {symbol} blocked: {failure['error']}",
                        file=sys.stderr,
                        flush=True,
                    )
                    persist(interrupted=failure["retryable"] and not fallback_failed)
                    if failure["retryable"] and not fallback_failed:
                        raise HistoricalProviderError(
                            failure["error"], category=failure["category"]
                        )
                    date_blocked = True
                    break
        finally:
            candidate_outcomes.close()
        if date_blocked:
            continue
        if len(raw_by_symbol) != len(values) or len(benchmark_rows) != len(BENCHMARKS):
            continue
        try:
            bundle = build_bundle(
                day,
                values,
                raw_by_symbol,
                benchmark_rows,
                synthetic_equity=synthetic_equity,
                scanner=scanner,
                benchmark_providers=benchmark_providers,
                preregistration=preregistration,
            )
            validate_bundle(bundle)
            _write_json(bundle_path, bundle)
            if day not in built:
                built.append(day)
            performance["bundles_built"] += 1
        except (
            HistoricalBundleBuildError,
            HistoricalLearningError,
            StrategyInputError,
            OSError,
            KeyError,
        ) as exc:
            failures.append(_failure_row(day, "bundle", "assembly", exc))
        persist(interrupted=False)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("manifest", type=Path, help="pre-frozen evidence manifest")
    parser.add_argument("--data-root", type=Path, default=DEFAULT_DATA_ROOT)
    parser.add_argument("--env-file", type=Path, default=DEFAULT_ENV_PATH)
    parser.add_argument("--synthetic-equity", type=float, default=25_000.0)
    parser.add_argument(
        "--transport-retries",
        type=int,
        default=1,
        help="fresh provider connections after retryable interruption",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=DEFAULT_COLLECTION_WORKERS,
        help=(
            "bounded deterministic collection workers "
            f"(default: {DEFAULT_COLLECTION_WORKERS})"
        ),
    )
    parser.add_argument(
        "--status-output",
        type=Path,
        help="atomic batch status (defaults to historical_batches/<manifest>.json)",
    )
    parser.add_argument(
        "--confirmation-manifest",
        type=Path,
        help="verified preregistration manifest that binds confirmation bundles",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        result = collect_manifest(
            args.manifest,
            data_root=args.data_root,
            env_file=args.env_file,
            synthetic_equity=args.synthetic_equity,
            transport_retries=args.transport_retries,
            max_workers=args.workers,
            status_path=(
                args.status_output
                or PROJECT_ROOT / "historical_batches" / f"{args.manifest.stem}.json"
            ),
            confirmation_manifest_path=args.confirmation_manifest,
            historical_store=HistoricalDayStore.from_env(args.env_file),
        )
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0 if result["valid"] else 1
    except (
        HistoricalBundleBuildError,
        HistoricalProviderError,
        IBKRHistoricalError,
        OSError,
    ) as exc:
        print(
            json.dumps({"error": str(exc), "error_type": type(exc).__name__}, indent=2)
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

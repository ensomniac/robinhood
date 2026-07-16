"""Collect and assemble validation-grade historical replay bundles.

The input manifest freezes dates, symbols, scanner ranking, and time-valid
catalyst evidence before any IBKR market data is requested.  This module uses
only the read-only :mod:`ibkr_historical` facade, caches raw provider responses,
derives strategy inputs without future bars, and validates a complete bundle
before writing it under ``historical_data/``.
"""

from __future__ import annotations

import argparse
import json
import math
import statistics
import sys
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence
from zoneinfo import ZoneInfo

from historical_learning import HistoricalLearningError, validate_bundle
from ibkr_historical import (
    DEFAULT_ENV_PATH,
    IBKRConfig,
    IBKRHistoricalClient,
    IBKRHistoricalError,
    collect_candidate_history,
)
from strategy_engine import StrategyInputError, evaluate_candidate, load_config


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_DATA_ROOT = PROJECT_ROOT / "historical_data"
EASTERN = ZoneInfo("America/New_York")
UTC = timezone.utc
BENCHMARKS = ("SPY", "QQQ")
EXPECTED_MINUTES = 390


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
    """Return the first 9:35-10:30 opening-range break, or 10:30 no-break."""
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
        if (
            time(9, 35) <= clock <= time(10, 30)
            and float(row["high"]) > opening["high"]
        ):
            return clock.replace(microsecond=0).isoformat(), True, opening
    return "10:30:00", False, opening


def _atr14(rows: Sequence[Mapping[str, Any]]) -> float:
    if len(rows) < 15:
        raise HistoricalBundleBuildError("ATR(14) needs at least 15 prior daily bars")
    ordered = sorted(rows, key=lambda row: int(row["epoch"]))[-15:]
    ranges: list[float] = []
    for previous, current in zip(ordered, ordered[1:]):
        high = float(current["high"])
        low = float(current["low"])
        previous_close = float(previous["close"])
        ranges.append(
            max(high - low, abs(high - previous_close), abs(low - previous_close))
        )
    return statistics.fmean(ranges)


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
        raise HistoricalBundleBuildError("candidate needs three IBKR quote snapshots")
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
            f"{day} {symbol}: cached IBKR request mismatch"
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
    earlier_vwap = _cumulative_vwap(completed[:-5] or completed[:1])
    recent_rows = completed[-3:] or session_rows[:5]
    support_candidates = [
        float(opening["high"]),
        vwap,
        min(float(row["low"]) for row in recent_rows),
    ]
    below_entry = [value for value in support_candidates if 0 < value < entry_limit]
    technical_invalidation = max(below_entry) if below_entry else float(opening["low"])

    daily = sorted(raw["daily_bars"], key=lambda row: int(row["epoch"]))
    atr = _atr14(daily)
    average_volume = statistics.fmean(int(row["volume"]) for row in daily[-14:])
    prior_highs = [float(row["high"]) for row in daily]
    overhead = [value for value in prior_highs if value > entry_limit]
    resistance = min(overhead) if overhead else max(prior_highs)

    stop_distance = max(0.10 * atr, entry_limit - technical_invalidation)
    planned_stop = entry_limit - stop_distance
    stop_outside_noise = (
        planned_stop <= technical_invalidation
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
            "opening_price": float(opening["open"]),
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
            "vwap_flat_or_rising": vwap >= earlier_vwap,
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
        "catalyst": dict(evidence["catalyst"]),
        "discovery": _discovery_payload(evidence),
        "market_alignment": {
            "candidate_return_fraction": candidate_return,
            "candidate_vwap": vwap,
            "benchmarks": benchmark_metrics,
        },
        "evaluation_payload": payload,
        "bars": _replay_bars(raw),
    }


def build_bundle(
    day: str,
    evidence_rows: Sequence[Mapping[str, Any]],
    raw_by_symbol: Mapping[str, Mapping[str, Any]],
    benchmarks: Mapping[str, Sequence[Mapping[str, Any]]],
    *,
    synthetic_equity: float,
    scanner: Mapping[str, Any],
) -> dict[str, Any]:
    if len(evidence_rows) < 10:
        raise HistoricalBundleBuildError(
            f"{day}: frozen universe has fewer than ten candidates"
        )
    rvols: dict[str, float] = {}
    for evidence in evidence_rows:
        symbol = str(evidence["symbol"])
        raw = raw_by_symbol[symbol]
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
    return {
        "schema_version": 1,
        "date": day,
        "sample_phase": "pilot",
        "session_capture_complete": True,
        "simulation_account_equity": synthetic_equity,
        "simulation_buying_power": synthetic_equity,
        "source": {
            "provider": "Robinhood earnings calendar + SEC/issuer catalysts + Interactive Brokers TWS API",
            "captured_at": datetime.now(UTC).isoformat(),
            "point_in_time": True,
            "regular_hours_only": True,
            "split_adjusted": True,
            "historical_quotes_and_depth": True,
            "catalysts_point_in_time": True,
            "universe_capture_complete": scanner.get("universe_capture_complete")
            is True,
            "scanner_definition": dict(scanner),
        },
        "candidates": candidates,
    }


def _load_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise HistoricalBundleBuildError(f"cannot read {path}: {exc}") from exc


def _write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    rendered = json.dumps(value, indent=2, sort_keys=True) + "\n"
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(rendered, encoding="utf-8")
    temporary.replace(path)


def _benchmark_history(
    client: IBKRHistoricalClient, day: str, symbol: str
) -> dict[str, Any]:
    start = datetime.combine(date.fromisoformat(day), time(9, 30), tzinfo=EASTERN)
    end = datetime.combine(date.fromisoformat(day), time(16, 0), tzinfo=EASTERN)
    bars = client.fetch_bars(symbol, start, end, bar_size="1 min", what="TRADES")
    _ordered_session_bars(bars)
    return {
        "provider": "Interactive Brokers TWS API",
        "captured_at": datetime.now(UTC).isoformat(),
        "symbol": symbol,
        "date": day,
        "session_bars": bars,
    }


def collect_manifest(
    manifest_path: Path,
    *,
    data_root: Path = DEFAULT_DATA_ROOT,
    env_file: Path = DEFAULT_ENV_PATH,
    synthetic_equity: float = 25_000.0,
) -> dict[str, Any]:
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
    raw_root = data_root / "ibkr"
    built: list[str] = []
    failures: list[dict[str, str]] = []
    config = IBKRConfig.from_env(env_file)
    with IBKRHistoricalClient(config) as client:
        for day, values in candidates_by_date.items():
            if not isinstance(day, str) or not isinstance(values, list):
                raise HistoricalBundleBuildError("candidate dates must map to arrays")
            print(f"collecting {day}", file=sys.stderr, flush=True)
            benchmark_rows: dict[str, Sequence[Mapping[str, Any]]] = {}
            for symbol in BENCHMARKS:
                path = raw_root / f"{day}-{symbol}-benchmark.json"
                try:
                    raw = (
                        _load_json(path)
                        if path.exists()
                        else _benchmark_history(client, day, symbol)
                    )
                    if not path.exists():
                        _write_json(path, raw)
                    benchmark_rows[symbol] = raw["session_bars"]
                except (
                    HistoricalBundleBuildError,
                    IBKRHistoricalError,
                    OSError,
                    KeyError,
                ) as exc:
                    failures.append({"date": day, "symbol": symbol, "error": str(exc)})
            raw_by_symbol: dict[str, Mapping[str, Any]] = {}
            for evidence in values:
                if not isinstance(evidence, Mapping) or not isinstance(
                    evidence.get("symbol"), str
                ):
                    raise HistoricalBundleBuildError(
                        f"{day}: malformed candidate evidence"
                    )
                symbol = str(evidence["symbol"])
                path = raw_root / f"{day}-{symbol}.json"
                try:
                    if path.exists():
                        raw = _load_json(path)
                    else:
                        start = datetime.combine(
                            date.fromisoformat(day), time(9, 30), tzinfo=EASTERN
                        )
                        end = datetime.combine(
                            date.fromisoformat(day), time(16, 0), tzinfo=EASTERN
                        )
                        preview = client.fetch_bars(
                            symbol, start, end, bar_size="1 min", what="TRADES"
                        )
                        evaluation_time, _, _ = determine_evaluation(preview)
                        raw = collect_candidate_history(
                            client, symbol, day, evaluation_time
                        )
                        _write_json(path, raw)
                    raw_by_symbol[symbol] = raw
                    print(f"  {symbol} ready", file=sys.stderr, flush=True)
                except (
                    HistoricalBundleBuildError,
                    IBKRHistoricalError,
                    OSError,
                    KeyError,
                ) as exc:
                    failures.append({"date": day, "symbol": symbol, "error": str(exc)})
                    print(f"  {symbol} blocked: {exc}", file=sys.stderr, flush=True)
            if len(raw_by_symbol) != len(values) or len(benchmark_rows) != len(
                BENCHMARKS
            ):
                continue
            try:
                bundle = build_bundle(
                    day,
                    values,
                    raw_by_symbol,
                    benchmark_rows,
                    synthetic_equity=synthetic_equity,
                    scanner=scanner,
                )
                validate_bundle(bundle)
                _write_json(data_root / f"{day}.json", bundle)
                built.append(day)
            except (
                HistoricalBundleBuildError,
                HistoricalLearningError,
                StrategyInputError,
                OSError,
                KeyError,
            ) as exc:
                failures.append({"date": day, "symbol": "bundle", "error": str(exc)})
    return {"built_dates": built, "failures": failures, "valid": not failures}


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("manifest", type=Path, help="pre-frozen evidence manifest")
    parser.add_argument("--data-root", type=Path, default=DEFAULT_DATA_ROOT)
    parser.add_argument("--env-file", type=Path, default=DEFAULT_ENV_PATH)
    parser.add_argument("--synthetic-equity", type=float, default=25_000.0)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        result = collect_manifest(
            args.manifest,
            data_root=args.data_root,
            env_file=args.env_file,
            synthetic_equity=args.synthetic_equity,
        )
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0 if result["valid"] else 1
    except (HistoricalBundleBuildError, IBKRHistoricalError, OSError) as exc:
        print(
            json.dumps({"error": str(exc), "error_type": type(exc).__name__}, indent=2)
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

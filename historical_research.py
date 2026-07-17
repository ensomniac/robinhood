"""Run versioned research strategies over immutable local replay bundles.

This is a research-only execution plane. It never writes ``SIGNALS.jsonl``,
``TRADES.md``, ``trades/``, or strategy maturity state. Date tasks run in
separate processes; each worker loads a daily bundle once and evaluates every
selected strategy against the same frozen candidates.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import statistics
import sys
import time
from collections import Counter
from collections.abc import Mapping, Sequence
from concurrent.futures import ProcessPoolExecutor
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from historical_research_strategies import (
    BUILTIN_STRATEGIES,
    CandidateContext,
    ResearchStrategyError,
    SignalDecision,
    SignalSearch,
    load_strategy,
    parse_bars,
    plugin_manifest_hash_input,
)


SCHEMA_VERSION = 1
PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_DATA_ROOT = PROJECT_ROOT / "historical_data"
DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "research_runs"
PROTECTED_OUTPUTS = {
    (PROJECT_ROOT / "SIGNALS.jsonl").resolve(),
    (PROJECT_ROOT / "TRADES.md").resolve(),
    (PROJECT_ROOT / "trades").resolve(),
    (PROJECT_ROOT / "progress" / "HISTORY.jsonl").resolve(),
}


class HistoricalResearchError(RuntimeError):
    """Raised when a research run cannot preserve its fidelity contract."""


@dataclass(frozen=True, slots=True)
class ExecutionConfig:
    """A common execution model shared by all compared signal plugins."""

    entry_slippage_bps: float = 5.0
    exit_slippage_bps: float = 5.0
    target_r: float = 2.0
    force_flat_time_et: str = "15:50:00"

    def validate(self) -> None:
        for name, value in (
            ("entry_slippage_bps", self.entry_slippage_bps),
            ("exit_slippage_bps", self.exit_slippage_bps),
            ("target_r", self.target_r),
        ):
            if not math.isfinite(value) or value < 0:
                raise HistoricalResearchError(f"{name} must be finite and non-negative")
        if self.target_r <= 0:
            raise HistoricalResearchError("target_r must be positive")
        if not (
            len(self.force_flat_time_et) == 8
            and "09:31:00" <= self.force_flat_time_et <= "15:59:00"
        ):
            raise HistoricalResearchError("force_flat_time_et must be an RTH HH:MM:SS")


def _canonical_json(value: Any) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode("utf-8")


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _atomic_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(
        json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    os.replace(temporary, path)


def _atomic_text(path: Path, value: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(value, encoding="utf-8")
    os.replace(temporary, path)


def _safe_output_path(path: Path) -> Path:
    resolved = path.resolve()
    for protected in PROTECTED_OUTPUTS:
        if resolved == protected or protected in resolved.parents:
            raise HistoricalResearchError(
                f"research output cannot target production artifact {protected}"
            )
    return resolved


def _load_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise HistoricalResearchError(f"cannot read JSON {path}: {exc}") from exc


def load_dataset(evidence_path: Path, data_root: Path) -> dict[str, Any]:
    """Freeze ordered date paths and bundle hashes from a public evidence manifest."""

    evidence = _load_json(evidence_path)
    candidates_by_date = evidence.get("candidates_by_date")
    if not isinstance(candidates_by_date, Mapping) or not candidates_by_date:
        raise HistoricalResearchError(
            f"{evidence_path} must contain non-empty candidates_by_date"
        )
    dates = list(candidates_by_date)
    if len(dates) != len(set(dates)):
        raise HistoricalResearchError("evidence manifest contains duplicate dates")
    bundles: list[dict[str, Any]] = []
    for day in dates:
        evidence_rows = candidates_by_date[day]
        if not isinstance(evidence_rows, list) or not evidence_rows:
            raise HistoricalResearchError(f"{day} evidence candidates must be an array")
        expected_symbols = [
            str(row.get("symbol", "")).strip().upper()
            for row in evidence_rows
            if isinstance(row, Mapping)
        ]
        if (
            len(expected_symbols) != len(evidence_rows)
            or any(not symbol for symbol in expected_symbols)
            or len(expected_symbols) != len(set(expected_symbols))
        ):
            raise HistoricalResearchError(f"{day} has malformed or duplicate symbols")
        path = data_root / f"{day}.json"
        bundles.append(
            {
                "date": day,
                "path": str(path),
                "expected_symbols": expected_symbols,
                "status": "available" if path.is_file() else "missing",
                "sha256": _sha256_file(path) if path.is_file() else None,
            }
        )
    identity_input = {
        "evidence_sha256": _sha256_file(evidence_path),
        "bundles": [
            {
                "date": item["date"],
                "status": item["status"],
                "sha256": item["sha256"],
            }
            for item in bundles
        ],
    }
    return {
        "evidence_path": str(evidence_path),
        "evidence_sha256": identity_input["evidence_sha256"],
        "dataset_hash": _sha256_bytes(_canonical_json(identity_input)),
        "dates": dates,
        "bundles": bundles,
        "available_dates": sum(item["status"] == "available" for item in bundles),
        "missing_dates": sum(item["status"] == "missing" for item in bundles),
    }


def _trade_from_decision(
    candidate: CandidateContext,
    decision: SignalDecision,
    execution: ExecutionConfig,
) -> dict[str, Any]:
    bars = candidate.bars
    entry_index = decision.signal_index + 1
    if entry_index >= len(bars):
        raise ResearchStrategyError("signal has no following entry bar")
    entry_bar = bars[entry_index]
    entry = entry_bar.open * (1.0 + execution.entry_slippage_bps / 10_000.0)
    if decision.max_entry_price is not None and entry > decision.max_entry_price:
        raise ResearchStrategyError("next-open entry exceeds strategy chase cap")
    stop = float(decision.technical_stop)
    if not math.isfinite(stop) or stop <= 0 or stop >= entry:
        raise ResearchStrategyError("technical stop must be positive and below entry")
    planned_risk = entry - stop
    target = entry + execution.target_r * planned_risk
    exit_price: float | None = None
    exit_time: str | None = None
    exit_reason: str | None = None
    ambiguity = False
    exit_slippage = execution.exit_slippage_bps / 10_000.0

    for bar in bars[entry_index:]:
        if bar.time_et >= execution.force_flat_time_et:
            exit_price = bar.open * (1.0 - exit_slippage)
            exit_time = bar.time_et
            exit_reason = "force_flat"
            break
        hit_stop = bar.low <= stop
        hit_target = bar.high >= target
        if hit_stop and hit_target:
            ambiguity = True
            exit_price = min(bar.open, stop) * (1.0 - exit_slippage)
            exit_time = bar.time_et
            exit_reason = "stop_first_ambiguous_bar"
            break
        if hit_stop:
            exit_price = min(bar.open, stop) * (1.0 - exit_slippage)
            exit_time = bar.time_et
            exit_reason = "stop"
            break
        if hit_target:
            exit_price = target * (1.0 - exit_slippage)
            exit_time = bar.time_et
            exit_reason = "target"
            break

    if exit_price is None:
        final_bar = bars[-1]
        exit_price = final_bar.close * (1.0 - exit_slippage)
        exit_time = final_bar.time_et
        exit_reason = "session_end_fallback"

    net_r = (exit_price - entry) / planned_risk
    return {
        "symbol": candidate.symbol,
        "signal_id": candidate.signal_id,
        "signal_time_et": bars[decision.signal_index].time_et,
        "entry_time_et": entry_bar.time_et,
        "entry_price": round(entry, 8),
        "technical_stop": round(stop, 8),
        "planned_risk_per_share": round(planned_risk, 8),
        "target_price": round(target, 8),
        "exit_time_et": exit_time,
        "exit_price": round(exit_price, 8),
        "exit_reason": exit_reason,
        "same_minute_ambiguity": ambiguity,
        "net_r": round(net_r, 8),
        "signal_strength": round(decision.strength, 8),
        "signal_reason": decision.reason,
    }


def _blocked_result(
    day: str,
    strategy_spec: str,
    reason: str,
    candidate_count: int = 0,
    signals_found: int = 0,
    candidate_results: Sequence[Mapping[str, Any]] = (),
) -> dict[str, Any]:
    plugin = load_strategy(strategy_spec)
    return {
        "date": day,
        "strategy_id": plugin.strategy_id,
        "strategy_version": plugin.version,
        "coverage_status": "blocked",
        "block_reason": reason,
        "candidate_count": candidate_count,
        "signals_found": signals_found,
        "selected_trade": None,
        "candidate_results": list(candidate_results),
    }


def _find_signal_point_in_time(
    plugin: Any, candidate: CandidateContext
) -> SignalSearch:
    """Reveal one completed bar at a time and never expose future bars to a plugin."""

    last_search = SignalSearch(None, "no_signal_before_cutoff")
    for reveal_index, bar in enumerate(candidate.bars):
        if reveal_index < 5:
            continue
        if bar.time_et >= "10:30:00":
            break
        prefix = CandidateContext(
            date=candidate.date,
            symbol=candidate.symbol,
            signal_id=candidate.signal_id,
            bars=candidate.bars[: reveal_index + 1],
        )
        search = plugin.find_signal(prefix)
        if not isinstance(search, SignalSearch):
            raise ResearchStrategyError("plugin must return SignalSearch")
        last_search = search
        if search.decision is None:
            continue
        if search.decision.signal_index != reveal_index:
            raise ResearchStrategyError(
                "plugin decision must identify the final bar in its revealed prefix"
            )
        return search
    return last_search


def _evaluate_strategy(
    day: str,
    candidates: Sequence[Mapping[str, Any]],
    strategy_spec: str,
    execution: ExecutionConfig,
) -> dict[str, Any]:
    plugin = load_strategy(strategy_spec)
    supported = {
        "regular_session_one_minute_bars",
        "complete_regular_session",
        "real_bars_only",
    }
    unsupported = {
        key
        for key, required in asdict(plugin.requirements).items()
        if required and key not in supported
    }
    if unsupported:
        return _blocked_result(
            day,
            strategy_spec,
            "unsupported_requirements:" + ",".join(sorted(unsupported)),
            len(candidates),
        )

    candidate_results: list[dict[str, Any]] = []
    contexts: dict[str, CandidateContext] = {}
    for raw in candidates:
        symbol = str(raw.get("symbol", "")).strip().upper()
        signal_id = str(raw.get("signal_id", f"{day}-{symbol}-research"))
        if not symbol:
            return _blocked_result(
                day, strategy_spec, "candidate_missing_symbol", len(candidates)
            )
        try:
            bars = parse_bars(raw.get("bars"))
        except ResearchStrategyError as exc:
            return _blocked_result(
                day, strategy_spec, f"{symbol}:{exc}", len(candidates)
            )
        context = CandidateContext(day, symbol, signal_id, bars)
        contexts[symbol] = context
        try:
            search = _find_signal_point_in_time(plugin, context)
        except Exception as exc:  # Plugin faults become isolated date blockers.
            return _blocked_result(
                day,
                strategy_spec,
                f"{symbol}:plugin_error:{type(exc).__name__}:{exc}",
                len(candidates),
            )
        result: dict[str, Any] = {
            "symbol": symbol,
            "status": "signal" if search.decision is not None else "no_signal",
            "reason": search.reason,
        }
        if search.decision is not None:
            result.update(
                {
                    "signal_index": search.decision.signal_index,
                    "signal_time_et": bars[search.decision.signal_index].time_et,
                    "strength": round(search.decision.strength, 8),
                    "technical_stop": round(search.decision.technical_stop, 8),
                    "decision_reason": search.decision.reason,
                    "max_entry_price": search.decision.max_entry_price,
                }
            )
        candidate_results.append(result)

    signal_results = [
        value for value in candidate_results if value["status"] == "signal"
    ]
    signal_results.sort(
        key=lambda value: (
            int(value["signal_index"]),
            -float(value["strength"]),
            str(value["symbol"]),
        )
    )
    selected_trade = None
    for selected in signal_results:
        decision = SignalDecision(
            signal_index=int(selected["signal_index"]),
            technical_stop=float(selected["technical_stop"]),
            strength=float(selected["strength"]),
            reason=str(selected["decision_reason"]),
            max_entry_price=(
                float(selected["max_entry_price"])
                if selected["max_entry_price"] is not None
                else None
            ),
        )
        try:
            selected_trade = _trade_from_decision(
                contexts[str(selected["symbol"])], decision, execution
            )
        except ResearchStrategyError as exc:
            selected["status"] = "signal_unexecutable"
            selected["reason"] = f"next_open_execution_rejected:{exc}"
            continue
        break
    return {
        "date": day,
        "strategy_id": plugin.strategy_id,
        "strategy_version": plugin.version,
        "coverage_status": "covered",
        "block_reason": None,
        "candidate_count": len(candidates),
        "signals_found": len(signal_results),
        "selected_trade": selected_trade,
        "candidate_results": sorted(
            candidate_results, key=lambda value: value["symbol"]
        ),
    }


def _evaluate_date_task(
    day: str,
    bundle_path: str,
    expected_symbols: tuple[str, ...],
    strategy_specs: tuple[str, ...],
    execution_values: dict[str, Any],
) -> list[dict[str, Any]]:
    execution = ExecutionConfig(**execution_values)
    path = Path(bundle_path)
    if not path.is_file():
        return [_blocked_result(day, spec, "bundle_missing") for spec in strategy_specs]
    try:
        bundle = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        reason = f"bundle_unreadable:{type(exc).__name__}"
        return [_blocked_result(day, spec, reason) for spec in strategy_specs]
    if bundle.get("date") != day:
        return [
            _blocked_result(day, spec, "bundle_date_mismatch")
            for spec in strategy_specs
        ]
    if bundle.get("session_capture_complete") is not True:
        return [
            _blocked_result(day, spec, "bundle_session_capture_incomplete")
            for spec in strategy_specs
        ]
    source = bundle.get("source")
    required_attestations = (
        "point_in_time",
        "regular_hours_only",
        "split_adjusted",
        "universe_capture_complete",
    )
    if not isinstance(source, Mapping) or any(
        source.get(field) is not True for field in required_attestations
    ):
        return [
            _blocked_result(day, spec, "bundle_source_attestation_incomplete")
            for spec in strategy_specs
        ]
    candidates = bundle.get("candidates")
    if not isinstance(candidates, list) or not candidates:
        return [
            _blocked_result(day, spec, "bundle_has_no_candidates")
            for spec in strategy_specs
        ]
    actual_symbols = tuple(
        str(value.get("symbol", "")).strip().upper()
        for value in candidates
        if isinstance(value, Mapping)
    )
    if actual_symbols != expected_symbols:
        return [
            _blocked_result(day, spec, "frozen_candidate_universe_mismatch")
            for spec in strategy_specs
        ]
    return [
        _evaluate_strategy(day, candidates, spec, execution) for spec in strategy_specs
    ]


def _drawdown(values: Sequence[float]) -> float:
    equity = 0.0
    peak = 0.0
    maximum = 0.0
    for value in values:
        equity += value
        peak = max(peak, equity)
        maximum = max(maximum, peak - equity)
    return maximum


def _strategy_summary(
    strategy: Mapping[str, Any], results: Sequence[Mapping[str, Any]]
) -> dict[str, Any]:
    covered = [value for value in results if value["coverage_status"] == "covered"]
    trades = [value["selected_trade"] for value in covered if value["selected_trade"]]
    returns = [float(value["net_r"]) for value in trades]
    wins = [value for value in returns if value > 0]
    losses = [value for value in returns if value < 0]
    reasons = Counter(
        candidate["reason"]
        for result in covered
        for candidate in result["candidate_results"]
        if candidate["status"] == "no_signal"
    )
    unexecutable = Counter(
        candidate["reason"]
        for result in covered
        for candidate in result["candidate_results"]
        if candidate["status"] == "signal_unexecutable"
    )
    blocked = Counter(
        str(value["block_reason"])
        for value in results
        if value["coverage_status"] != "covered"
    )
    gross_profit = sum(wins)
    gross_loss = -sum(losses)
    profit_factor = gross_profit / gross_loss if gross_loss > 0 else None
    return {
        "strategy_id": strategy["strategy_id"],
        "strategy_version": strategy["version"],
        "requested_days": len(results),
        "covered_days": len(covered),
        "blocked_days": len(results) - len(covered),
        "trade_days": len(trades),
        "no_trade_days": len(covered) - len(trades),
        "signals_found": sum(int(value["signals_found"]) for value in covered),
        "unexecutable_signals": sum(unexecutable.values()),
        "wins": len(wins),
        "losses": len(losses),
        "breakeven": len(returns) - len(wins) - len(losses),
        "win_rate": round(len(wins) / len(returns), 8) if returns else None,
        "mean_r": round(statistics.fmean(returns), 8) if returns else None,
        "median_r": round(statistics.median(returns), 8) if returns else None,
        "total_r": round(sum(returns), 8),
        "average_win_r": round(statistics.fmean(wins), 8) if wins else None,
        "average_loss_r": round(statistics.fmean(losses), 8) if losses else None,
        "profit_factor": round(profit_factor, 8) if profit_factor is not None else None,
        "maximum_drawdown_r": round(_drawdown(returns), 8),
        "exit_reasons": dict(
            sorted(Counter(value["exit_reason"] for value in trades).items())
        ),
        "top_no_signal_reasons": dict(reasons.most_common(8)),
        "unexecutable_signal_reasons": dict(unexecutable.most_common(8)),
        "block_reasons": dict(sorted(blocked.items())),
    }


def build_comparison(
    summaries: Sequence[Mapping[str, Any]],
    results: Sequence[Mapping[str, Any]],
    baseline_id: str,
) -> dict[str, Any]:
    ids = {str(value["strategy_id"]) for value in summaries}
    if baseline_id not in ids:
        raise HistoricalResearchError(f"baseline strategy {baseline_id!r} was not run")
    daily: dict[str, dict[str, float]] = {}
    covered: dict[str, set[str]] = {strategy_id: set() for strategy_id in ids}
    for result in results:
        strategy_id = str(result["strategy_id"])
        day = str(result["date"])
        if result["coverage_status"] != "covered":
            continue
        covered[strategy_id].add(day)
        trade = result["selected_trade"]
        daily.setdefault(day, {})[strategy_id] = float(trade["net_r"]) if trade else 0.0
    comparisons: list[dict[str, Any]] = []
    for strategy_id in sorted(ids - {baseline_id}):
        common = sorted(covered[baseline_id] & covered[strategy_id])
        differences = [
            daily[day].get(strategy_id, 0.0) - daily[day].get(baseline_id, 0.0)
            for day in common
        ]
        comparisons.append(
            {
                "strategy_id": strategy_id,
                "common_covered_days": len(common),
                "mean_paired_day_r_difference": round(statistics.fmean(differences), 8)
                if differences
                else None,
                "cumulative_paired_day_r_difference": round(sum(differences), 8),
                "better_days": sum(value > 0 for value in differences),
                "worse_days": sum(value < 0 for value in differences),
                "tied_days": sum(value == 0 for value in differences),
            }
        )
    return {"baseline_strategy_id": baseline_id, "paired_comparisons": comparisons}


def render_report(public_result: Mapping[str, Any]) -> str:
    manifest = public_result["manifest"]
    summaries = public_result["strategy_summaries"]
    comparison = public_result["comparison"]
    lines = [
        "# Historical Multi-Strategy Research Result",
        "",
        f"Run ID: `{manifest['run_id']}`",
        "",
        "## Scope",
        "",
        f"- Requested dates: {manifest['dataset']['requested_dates']}",
        f"- Available immutable bundles: {manifest['dataset']['available_dates']}",
        f"- Missing bundles retained as blockers: {manifest['dataset']['missing_dates']}",
        f"- Dataset hash: `{manifest['dataset']['dataset_hash']}`",
        f"- Worker processes: {public_result['runtime']['workers']}",
        f"- Wall time: {public_result['runtime']['elapsed_seconds']:.3f} seconds",
        "- Provider requests: 0",
        "- Production ledger, archive, and maturity writes: 0",
        "",
        "## Strategy Results",
        "",
        "| Strategy | Covered | Trades | Win rate | Mean R | Total R | Profit factor | Max DD R |",
        "|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for value in summaries:
        win_rate = (
            "n/a" if value["win_rate"] is None else f"{100 * value['win_rate']:.1f}%"
        )
        mean_r = "n/a" if value["mean_r"] is None else f"{value['mean_r']:.3f}"
        profit_factor = (
            "n/a" if value["profit_factor"] is None else f"{value['profit_factor']:.3f}"
        )
        lines.append(
            f"| {value['strategy_id']}@{value['strategy_version']} | "
            f"{value['covered_days']} | {value['trade_days']} | {win_rate} | "
            f"{mean_r} | {value['total_r']:.3f} | {profit_factor} | "
            f"{value['maximum_drawdown_r']:.3f} |"
        )
    lines.extend(
        [
            "",
            "## Paired Comparison",
            "",
            f"Baseline: `{comparison['baseline_strategy_id']}`. No-trade days count as 0R; only "
            "dates covered by both strategies enter each pair.",
            "",
            "| Strategy | Common days | Mean delta R/day | Cumulative delta R | Better | Worse | Tied |",
            "|---|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for value in comparison["paired_comparisons"]:
        mean = value["mean_paired_day_r_difference"]
        mean_text = "n/a" if mean is None else f"{mean:.4f}"
        lines.append(
            f"| {value['strategy_id']} | {value['common_covered_days']} | "
            f"{mean_text} | {value['cumulative_paired_day_r_difference']:.3f} | "
            f"{value['better_days']} | {value['worse_days']} | {value['tied_days']} |"
        )
    lines.extend(
        [
            "",
            "## Interpretation Boundary",
            "",
            "These are research-only counterfactuals over the already frozen, catalyst-selected "
            "candidate universe. They do not validate a strategy over the full market. The current "
            "bundles support complete one-minute bar strategies, but not arbitrary-time NBBO, full "
            "depth, tick-order, or independent benchmark-bar rules. Results do not update "
            "`SIGNALS.jsonl`, the public trade archive, or maturity.",
            "",
        ]
    )
    return "\n".join(lines)


def run_research(
    *,
    evidence_path: Path,
    data_root: Path = DEFAULT_DATA_ROOT,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    strategy_specs: Sequence[str] = tuple(BUILTIN_STRATEGIES),
    baseline_id: str = "orb-5m-research",
    workers: int | None = None,
    execution: ExecutionConfig = ExecutionConfig(),
    publish_prefix: Path | None = None,
) -> dict[str, Any]:
    """Execute a deterministic date x strategy matrix and persist isolated results."""

    execution.validate()
    if not strategy_specs:
        raise HistoricalResearchError("at least one strategy is required")
    manifests = plugin_manifest_hash_input(strategy_specs)
    identifiers = [value["strategy_id"] for value in manifests]
    if len(identifiers) != len(set(identifiers)):
        raise HistoricalResearchError("strategy ids must be unique within a run")
    dataset = load_dataset(evidence_path, data_root)
    requested_workers = workers or min(8, os.cpu_count() or 1)
    if requested_workers < 1:
        raise HistoricalResearchError("workers must be at least 1")
    worker_count = min(requested_workers, len(dataset["dates"]))
    configuration = {
        "strategies": manifests,
        "execution": asdict(execution),
        "baseline_strategy_id": baseline_id,
        "engine": {
            "module": "historical_research",
            "schema_version": SCHEMA_VERSION,
            "sha256": _sha256_file(Path(__file__)),
        },
    }
    configuration_hash = _sha256_bytes(_canonical_json(configuration))
    run_id = f"research-{dataset['dataset_hash'][:12]}-{configuration_hash[:12]}"
    run_directory = _safe_output_path(output_root / run_id)
    stable_manifest = {
        "schema_version": SCHEMA_VERSION,
        "run_id": run_id,
        "mode": "research_only",
        "dataset": {
            "evidence_manifest": os.path.relpath(evidence_path.resolve(), PROJECT_ROOT),
            "evidence_sha256": dataset["evidence_sha256"],
            "dataset_hash": dataset["dataset_hash"],
            "requested_dates": len(dataset["dates"]),
            "available_dates": dataset["available_dates"],
            "missing_dates": dataset["missing_dates"],
            "bundle_hashes": [
                {
                    "date": value["date"],
                    "status": value["status"],
                    "sha256": value["sha256"],
                }
                for value in dataset["bundles"]
            ],
        },
        "configuration_hash": configuration_hash,
        "strategies": manifests,
        "engine": configuration["engine"],
        "execution": asdict(execution),
        "isolation": {
            "provider_requests": 0,
            "production_ledger_writes": 0,
            "trade_archive_writes": 0,
            "maturity_writes": 0,
        },
    }
    _atomic_json(run_directory / "manifest.json", stable_manifest)

    started = time.perf_counter()
    execution_values = asdict(execution)
    tasks = [
        (
            value["date"],
            value["path"],
            tuple(value["expected_symbols"]),
            tuple(strategy_specs),
            execution_values,
        )
        for value in dataset["bundles"]
    ]
    if worker_count == 1:
        per_date = [_evaluate_date_task(*task) for task in tasks]
    else:
        days, paths, expected, specs, executions = zip(*tasks)
        with ProcessPoolExecutor(max_workers=worker_count) as executor:
            per_date = list(
                executor.map(
                    _evaluate_date_task, days, paths, expected, specs, executions
                )
            )
    elapsed = time.perf_counter() - started
    results = [result for date_results in per_date for result in date_results]
    results.sort(key=lambda value: (value["date"], value["strategy_id"]))

    for result in results:
        _atomic_json(
            run_directory
            / "shards"
            / str(result["strategy_id"])
            / f"{result['date']}.json",
            result,
        )
    summaries = [
        _strategy_summary(
            strategy,
            [
                value
                for value in results
                if value["strategy_id"] == strategy["strategy_id"]
            ],
        )
        for strategy in manifests
    ]
    comparison = build_comparison(summaries, results, baseline_id)
    stable_result = {
        "schema_version": SCHEMA_VERSION,
        "manifest": stable_manifest,
        "strategy_summaries": summaries,
        "comparison": comparison,
    }
    runtime = {
        "workers": worker_count,
        "elapsed_seconds": round(elapsed, 6),
        "dates_per_second": round(len(dataset["dates"]) / elapsed, 6)
        if elapsed
        else None,
        "date_strategy_evaluations": len(results),
        "evaluations_per_second": round(len(results) / elapsed, 6) if elapsed else None,
        "provider_requests": 0,
    }
    public_result = {**stable_result, "runtime": runtime}
    _atomic_json(run_directory / "result.json", stable_result)
    _atomic_json(run_directory / "runtime.json", runtime)
    _atomic_text(run_directory / "report.md", render_report(public_result))
    if publish_prefix is not None:
        prefix = _safe_output_path(publish_prefix)
        _atomic_json(prefix.with_suffix(".json"), public_result)
        _atomic_text(prefix.with_suffix(".md"), render_report(public_result))
    return public_result


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("list-strategies", help="show built-in plugin contracts")
    run = subparsers.add_parser("run", help="run a date x strategy research matrix")
    run.add_argument("--evidence", type=Path, required=True)
    run.add_argument("--data-root", type=Path, default=DEFAULT_DATA_ROOT)
    run.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    run.add_argument(
        "--strategy",
        action="append",
        dest="strategies",
        help="built-in id or module:attribute; repeat; default is every built-in",
    )
    run.add_argument("--baseline", default="orb-5m-research")
    run.add_argument("--workers", type=int)
    run.add_argument("--entry-slippage-bps", type=float, default=5.0)
    run.add_argument("--exit-slippage-bps", type=float, default=5.0)
    run.add_argument("--target-r", type=float, default=2.0)
    run.add_argument("--force-flat-time", default="15:50:00")
    run.add_argument(
        "--publish-prefix",
        type=Path,
        help="also write compact tracked .json and .md results at this prefix",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        if args.command == "list-strategies":
            print(
                json.dumps(
                    plugin_manifest_hash_input(tuple(BUILTIN_STRATEGIES)), indent=2
                )
            )
            return 0
        result = run_research(
            evidence_path=args.evidence,
            data_root=args.data_root,
            output_root=args.output_root,
            strategy_specs=args.strategies or tuple(BUILTIN_STRATEGIES),
            baseline_id=args.baseline,
            workers=args.workers,
            execution=ExecutionConfig(
                entry_slippage_bps=args.entry_slippage_bps,
                exit_slippage_bps=args.exit_slippage_bps,
                target_r=args.target_r,
                force_flat_time_et=args.force_flat_time,
            ),
            publish_prefix=args.publish_prefix,
        )
    except (HistoricalResearchError, ResearchStrategyError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""Append-only multi-strategy evidence ledger and portfolio maturity report."""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import itertools
import json
import math
import os
import re
import statistics
import sys
import tomllib
from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any

from portfolio_funnel import PortfolioFunnelError, validate_stage0_survivor_binding


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_CONFIG_PATH = PROJECT_ROOT / "portfolio_config.toml"
DEFAULT_LEDGER_PATH = PROJECT_ROOT / "PORTFOLIO_SIGNALS.jsonl"
SCHEMA_VERSION = 1
FIRST_PILOT_MILESTONE = "FIRST_PILOT_READY_LIVE_STARTED"
STRATEGY_ID_PATTERN = re.compile(r"^[a-z0-9][a-z0-9-]{2,79}$")
VERSION_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{1,79}$")
SHA256_PATTERN = re.compile(r"^[0-9a-f]{64}$")
RECORD_TYPES = {"inspection", "session", "signal"}
SAMPLE_PHASES = {"development", "confirmation", "shadow", "live"}
MODES = {"historical", "shadow", "live"}


class PortfolioMaturityError(ValueError):
    """Portfolio evidence or configuration is malformed or unsafe."""


@dataclass(frozen=True)
class PortfolioConfig:
    path: Path
    raw: Mapping[str, Any]
    sha256: str


@dataclass(frozen=True)
class RobustnessMetrics:
    signals: int
    expectancy_r: float | None
    profit_factor: float | None
    maximum_drawdown_r: float
    bootstrap_lower_expectancy_r: float | None
    first_half_total_r: float | None
    second_half_total_r: float | None
    without_five_best_total_r: float | None
    stress_10_total_r: float | None
    stress_10_profit_factor: float | None
    stress_10_drawdown_r: float | None
    stress_20_total_r: float | None
    stress_20_profit_factor: float | None
    stress_20_drawdown_r: float | None
    rule_violations: int
    incomplete_capture_records: int


@dataclass(frozen=True)
class StrategyMetrics:
    historical_signals: int
    development_signals: int
    confirmation_signals: int
    shadow_executions: int
    live_executions: int
    natural_stop_executions: int
    expectancy_r: float | None
    confirmation_expectancy_r: float | None
    profit_factor: float | None
    maximum_drawdown_r: float
    bootstrap_lower_expectancy_r: float | None
    first_half_total_r: float | None
    second_half_total_r: float | None
    without_five_best_total_r: float | None
    stress_10_total_r: float | None
    stress_10_profit_factor: float | None
    stress_10_drawdown_r: float | None
    stress_20_total_r: float | None
    stress_20_profit_factor: float | None
    stress_20_drawdown_r: float | None
    development: RobustnessMetrics
    confirmation: RobustnessMetrics
    entry_slippage_p95_bps: float | None
    unprotected_p95_seconds: float | None
    stop_slippage_excess_p95_bps: float | None
    rule_violations: int
    incomplete_capture_records: int


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _finite(value: Any, field: str, *, minimum: float | None = None) -> float:
    if isinstance(value, bool):
        raise PortfolioMaturityError(f"{field} must be numeric")
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise PortfolioMaturityError(f"{field} must be numeric") from exc
    if not math.isfinite(result) or (minimum is not None and result < minimum):
        raise PortfolioMaturityError(f"{field} must be finite and >= {minimum}")
    return result


def _integer(value: Any, field: str, *, minimum: int = 0) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise PortfolioMaturityError(f"{field} must be an integer >= {minimum}")
    return value


def _string(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise PortfolioMaturityError(f"{field} must be a non-empty string")
    return value.strip()


def _boolean(value: Any, field: str) -> bool:
    if not isinstance(value, bool):
        raise PortfolioMaturityError(f"{field} must be true or false")
    return value


def _iso_date(value: Any, field: str) -> str:
    normalized = _string(value, field)
    try:
        date.fromisoformat(normalized)
    except ValueError as exc:
        raise PortfolioMaturityError(f"{field} must be an ISO date") from exc
    return normalized


def _timestamp(value: Any, field: str) -> str:
    normalized = _string(value, field)
    try:
        parsed = datetime.fromisoformat(normalized.replace("Z", "+00:00"))
    except ValueError as exc:
        raise PortfolioMaturityError(f"{field} must be an ISO timestamp") from exc
    if parsed.tzinfo is None:
        raise PortfolioMaturityError(f"{field} must include a timezone")
    return normalized


def _sha256(value: Any, field: str) -> str:
    normalized = _string(value, field)
    if not SHA256_PATTERN.fullmatch(normalized):
        raise PortfolioMaturityError(f"{field} must be a lowercase SHA-256")
    return normalized


def _strategy_identity(record: Mapping[str, Any]) -> tuple[str, str, str, str]:
    strategy_id = _string(record.get("strategy_id"), "strategy_id")
    version = _string(record.get("strategy_version"), "strategy_version")
    family = _string(record.get("mechanism_family"), "mechanism_family")
    rules_hash = _sha256(record.get("rules_hash"), "rules_hash")
    if not STRATEGY_ID_PATTERN.fullmatch(strategy_id):
        raise PortfolioMaturityError("strategy_id is unsafe")
    if not VERSION_PATTERN.fullmatch(version):
        raise PortfolioMaturityError("strategy_version is unsafe")
    if not STRATEGY_ID_PATTERN.fullmatch(family):
        raise PortfolioMaturityError("mechanism_family is unsafe")
    return strategy_id, version, family, rules_hash


def _required_string_list(value: Any, field: str) -> list[str]:
    if not isinstance(value, list) or any(
        not isinstance(item, str) or not item.strip() for item in value
    ):
        raise PortfolioMaturityError(f"{field} must be an array of strings")
    return [str(item) for item in value]


def load_config(path: Path = DEFAULT_CONFIG_PATH) -> PortfolioConfig:
    try:
        raw = tomllib.loads(path.read_text(encoding="utf-8"))
    except (OSError, tomllib.TOMLDecodeError) as exc:
        raise PortfolioMaturityError(f"cannot load portfolio config: {exc}") from exc
    required = {
        "campaign",
        "portfolio",
        "pilot_risk",
        "scaled_risk",
        "pilot_ready",
        "live_validated",
        "stress",
    }
    if set(raw) != required:
        raise PortfolioMaturityError("portfolio config sections are incomplete")
    campaign = raw["campaign"]
    portfolio = raw["portfolio"]
    pilot_risk = raw["pilot_risk"]
    scaled = raw["scaled_risk"]
    if campaign.get("id") != "multi-strategy-portfolio-validation-v1":
        raise PortfolioMaturityError("unexpected portfolio campaign ID")
    if campaign.get("schema_version") != 1:
        raise PortfolioMaturityError("portfolio schema_version must be 1")
    for field in (
        "target_pilot_ready_strategies",
        "initial_mechanism_families",
        "maximum_initial_variants",
        "maximum_second_wave_families",
    ):
        _integer(campaign.get(field), f"campaign.{field}", minimum=1)
    for field in (
        "maximum_concurrent_positions",
        "maximum_new_entries_per_day",
        "maximum_holding_trading_days",
    ):
        _integer(portfolio.get(field), f"portfolio.{field}", minimum=1)
    if portfolio["maximum_concurrent_positions"] > 3:
        raise PortfolioMaturityError("concurrent positions exceed authorization")
    if portfolio["maximum_new_entries_per_day"] > 5:
        raise PortfolioMaturityError("daily entries exceed authorization")
    if portfolio["maximum_holding_trading_days"] > 5:
        raise PortfolioMaturityError("holding period exceeds authorization")
    if not 0 <= _finite(
        portfolio.get("maximum_ready_pairwise_correlation"),
        "portfolio.maximum_ready_pairwise_correlation",
    ) < 1:
        raise PortfolioMaturityError("correlation cap must be in [0, 1)")
    if not 0 < _finite(
        portfolio.get("minimum_combined_confirmation_opportunity_coverage"),
        "portfolio.minimum_combined_confirmation_opportunity_coverage",
    ) <= 1:
        raise PortfolioMaturityError("opportunity coverage must be in (0, 1]")
    for section_name, section in (("pilot_risk", pilot_risk), ("scaled_risk", scaled)):
        for field, value in section.items():
            if field == "minimum_clean_portfolio_live_closes":
                _integer(value, f"{section_name}.{field}", minimum=1)
            else:
                normalized = _finite(value, f"{section_name}.{field}", minimum=0)
                if normalized > 1.25:
                    raise PortfolioMaturityError(
                        f"{section_name}.{field} exceeds the authorized envelope"
                    )
    if pilot_risk["maximum_planned_loss_fraction_per_position"] > scaled[
        "maximum_planned_loss_fraction_per_position"
    ]:
        raise PortfolioMaturityError("pilot position risk exceeds scaled risk")
    if pilot_risk["maximum_aggregate_planned_open_loss_fraction"] > scaled[
        "maximum_aggregate_planned_open_loss_fraction"
    ]:
        raise PortfolioMaturityError("pilot aggregate risk exceeds scaled risk")
    ready = raw["pilot_ready"]
    live = raw["live_validated"]
    for field in (
        "minimum_closed_historical_signals",
        "minimum_confirmation_signals",
        "minimum_shadow_executions",
        "maximum_rule_violations",
    ):
        _integer(ready.get(field), f"pilot_ready.{field}")
    for field in (
        "minimum_live_executions",
        "minimum_natural_stop_executions",
        "maximum_rule_violations",
    ):
        _integer(live.get(field), f"live_validated.{field}")
    for field in (
        "require_positive_chronological_halves",
        "require_positive_without_five_best",
        "require_complete_trial_accounting",
        "require_clear_multiple_testing_audit",
        "require_complete_execution_model",
    ):
        _boolean(ready.get(field), f"pilot_ready.{field}")
    for field in (
        "minimum_expectancy_r",
        "minimum_confirmation_expectancy_r",
        "minimum_profit_factor",
        "minimum_bootstrap_confidence",
        "maximum_drawdown_r",
        "minimum_stressed_profit_factor",
        "maximum_stressed_drawdown_r",
    ):
        _finite(ready.get(field), f"pilot_ready.{field}", minimum=0)
    if not 0 < ready["minimum_bootstrap_confidence"] < 1:
        raise PortfolioMaturityError("bootstrap confidence must be in (0, 1)")
    for field in (
        "maximum_entry_slippage_p95_bps",
        "maximum_unprotected_p95_seconds",
        "maximum_stop_slippage_excess_p95_bps",
    ):
        _finite(live.get(field), f"live_validated.{field}", minimum=0)
    stress = raw["stress"]
    if stress.get("entry_exit_bps") != [10, 20]:
        raise PortfolioMaturityError("stress entry_exit_bps must remain [10, 20]")
    if stress.get("same_interval_ambiguity") != "stop_first":
        raise PortfolioMaturityError("same-interval ambiguity must remain stop_first")
    return PortfolioConfig(path=path, raw=raw, sha256=_sha256_file(path))


def validate_record(record: Mapping[str, Any], *, root: Path = PROJECT_ROOT) -> None:
    if record.get("schema_version") != SCHEMA_VERSION:
        raise PortfolioMaturityError("record schema_version must be 1")
    record_type = _string(record.get("record_type"), "record_type")
    if record_type not in RECORD_TYPES:
        raise PortfolioMaturityError(f"unsupported record_type {record_type!r}")
    _strategy_identity(record)
    _timestamp(record.get("recorded_at"), "recorded_at")
    if record_type == "inspection":
        inspection_id = _string(record.get("inspection_id"), "inspection_id")
        if not inspection_id.startswith(_string(record.get("strategy_id"), "strategy_id")):
            raise PortfolioMaturityError("inspection_id must begin with strategy_id")
        trial_count = _integer(record.get("trial_count"), "trial_count", minimum=1)
        if trial_count > 26:
            raise PortfolioMaturityError("trial_count exceeds both authorized waves")
        wave = _integer(record.get("tournament_wave"), "tournament_wave", minimum=1)
        if wave not in {1, 2}:
            raise PortfolioMaturityError("tournament_wave must be 1 or 2")
        ordinal = _integer(record.get("variant_ordinal"), "variant_ordinal", minimum=1)
        maximum_ordinal = 20 if wave == 1 else 6
        if ordinal > maximum_ordinal:
            raise PortfolioMaturityError(
                f"wave {wave} variant_ordinal exceeds {maximum_ordinal}"
            )
        for field in (
            "trial_accounting_complete",
            "multiple_testing_clear",
            "execution_model_complete",
            "development_universe_representative",
            "confirmation_untouched",
        ):
            _boolean(record.get(field), field)
        if "retired_after_development" in record:
            _boolean(record.get("retired_after_development"), "retired_after_development")
        _integer(
            record.get("confirmation_embargo_trading_days"),
            "confirmation_embargo_trading_days",
        )
        evidence = record.get("evidence_hashes")
        if not isinstance(evidence, Mapping) or not evidence:
            raise PortfolioMaturityError("inspection evidence_hashes must be non-empty")
        for supplied, expected in evidence.items():
            relative = Path(_string(supplied, "evidence path"))
            if relative.is_absolute() or ".." in relative.parts:
                raise PortfolioMaturityError("inspection evidence path is unsafe")
            path = root / relative
            if not path.is_file() or _sha256_file(path) != _sha256(
                expected, f"evidence_hashes.{supplied}"
            ):
                raise PortfolioMaturityError(f"inspection evidence drifted: {relative}")
        taxonomy_path = record.get("second_wave_failure_taxonomy_path")
        if wave == 1 and taxonomy_path is not None:
            raise PortfolioMaturityError("wave 1 cannot name a second-wave taxonomy")
        if wave == 2:
            taxonomy = _string(
                taxonomy_path, "second_wave_failure_taxonomy_path"
            )
            if taxonomy not in evidence:
                raise PortfolioMaturityError(
                    "wave 2 taxonomy must be included in evidence_hashes"
                )
        try:
            validate_stage0_survivor_binding(record, root=root)
        except PortfolioFunnelError as exc:
            raise PortfolioMaturityError(str(exc)) from exc
        return
    record_date = _iso_date(record.get("date"), "date")
    phase = _string(record.get("sample_phase"), "sample_phase")
    mode = _string(record.get("mode"), "mode")
    if phase not in SAMPLE_PHASES or mode not in MODES:
        raise PortfolioMaturityError("invalid sample phase or mode")
    if (phase == "development" or phase == "confirmation") and mode != "historical":
        raise PortfolioMaturityError("historical phases require historical mode")
    if phase == "shadow" and mode != "shadow":
        raise PortfolioMaturityError("shadow phase requires shadow mode")
    if phase == "live" and mode != "live":
        raise PortfolioMaturityError("live phase requires live mode")
    _boolean(record.get("session_capture_complete"), "session_capture_complete")
    _required_string_list(record.get("rule_violations"), "rule_violations")
    if record_type == "session":
        session_id = _string(record.get("session_id"), "session_id")
        if not session_id.startswith(record_date):
            raise PortfolioMaturityError("session_id must begin with its date")
        _boolean(record.get("eligible_signal"), "eligible_signal")
        return
    signal_id = _string(record.get("signal_id"), "signal_id")
    if not signal_id.startswith(record_date):
        raise PortfolioMaturityError("signal_id must begin with its date")
    _boolean(record.get("closed"), "closed")
    _boolean(record.get("eligible"), "eligible")
    for field in ("net_r", "stress_10bps_r", "stress_20bps_r"):
        _finite(record.get(field), field)
    _boolean(record.get("stop_executed"), "stop_executed")
    if mode == "shadow":
        for field in (
            "discovery_complete",
            "evaluation_complete",
            "sizing_complete",
            "order_construction_complete",
            "protection_plan_complete",
            "monitoring_complete",
            "journal_complete",
        ):
            if _boolean(record.get(field), field) is not True:
                raise PortfolioMaturityError(
                    f"complete shadow execution requires {field}=true"
                )
        if _integer(record.get("broker_actions"), "broker_actions") != 0:
            raise PortfolioMaturityError("shadow execution must have zero broker actions")
    if mode == "live":
        if _string(record.get("portfolio_guard_status"), "portfolio_guard_status") != "ENTRY_READY":
            raise PortfolioMaturityError(
                "live portfolio evidence requires portfolio_guard_status=ENTRY_READY"
            )
        for field in (
            "broker_review_passed",
            "protection_confirmed",
            "monitoring_complete",
            "journal_complete",
        ):
            if _boolean(record.get(field), field) is not True:
                raise PortfolioMaturityError(
                    f"complete live execution requires {field}=true"
                )
        confirmation_required = _boolean(
            record.get("broker_confirmation_required"),
            "broker_confirmation_required",
        )
        confirmation_satisfied = _boolean(
            record.get("broker_confirmation_satisfied"),
            "broker_confirmation_satisfied",
        )
        if confirmation_required and not confirmation_satisfied:
            raise PortfolioMaturityError(
                "broker-required confirmation was not satisfied"
            )
        for field in ("entry_slippage_bps", "unprotected_seconds"):
            _finite(record.get(field), field, minimum=0)
        if record.get("stop_executed") is True:
            _finite(record.get("stop_slippage_bps"), "stop_slippage_bps", minimum=0)
            _finite(record.get("stop_reserve_bps"), "stop_reserve_bps", minimum=0)


def read_records(path: Path = DEFAULT_LEDGER_PATH, *, root: Path = PROJECT_ROOT) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    records: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    seen_variants: set[tuple[int, int]] = set()
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if not line.strip():
            raise PortfolioMaturityError(f"ledger line {line_number} is blank")
        try:
            value = json.loads(line)
        except json.JSONDecodeError as exc:
            raise PortfolioMaturityError(
                f"ledger line {line_number} is invalid JSON: {exc.msg}"
            ) from exc
        if not isinstance(value, dict):
            raise PortfolioMaturityError(f"ledger line {line_number} must be an object")
        validate_record(value, root=root)
        record_id = (
            value.get("signal_id")
            or value.get("session_id")
            or value.get("inspection_id")
        )
        key = (str(value["record_type"]), str(record_id))
        if key in seen:
            raise PortfolioMaturityError(f"duplicate ledger record identity {key}")
        if value["record_type"] == "inspection":
            variant_key = (int(value["tournament_wave"]), int(value["variant_ordinal"]))
            if variant_key in seen_variants:
                raise PortfolioMaturityError(
                    f"duplicate tournament variant identity {variant_key}"
                )
            seen_variants.add(variant_key)
        seen.add(key)
        records.append(value)
    return records


def append_record(
    record: Mapping[str, Any],
    path: Path = DEFAULT_LEDGER_PATH,
    *,
    root: Path = PROJECT_ROOT,
) -> None:
    validate_record(record, root=root)
    path.parent.mkdir(parents=True, exist_ok=True)
    lock_path = path.with_suffix(path.suffix + ".lock")
    with lock_path.open("a+", encoding="utf-8") as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        existing = read_records(path, root=root)
        candidate = dict(record)
        identity = (
            candidate.get("signal_id")
            or candidate.get("session_id")
            or candidate.get("inspection_id")
        )
        for item in existing:
            current = (
                item.get("signal_id")
                or item.get("session_id")
                or item.get("inspection_id")
            )
            if item["record_type"] == candidate["record_type"] and current == identity:
                raise PortfolioMaturityError("record identity already exists")
            if (
                item["record_type"] == "inspection"
                and candidate["record_type"] == "inspection"
                and item["tournament_wave"] == candidate["tournament_wave"]
                and item["variant_ordinal"] == candidate["variant_ordinal"]
            ):
                raise PortfolioMaturityError("tournament variant identity already exists")
        payload = json.dumps(candidate, sort_keys=True, separators=(",", ":")) + "\n"
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o644)
        try:
            os.write(descriptor, payload.encode())
            os.fsync(descriptor)
        finally:
            os.close(descriptor)


def _profit_factor(values: Sequence[float]) -> float | None:
    if not values:
        return None
    gains = sum(value for value in values if value > 0)
    losses = abs(sum(value for value in values if value < 0))
    if losses == 0:
        return math.inf if gains > 0 else None
    return gains / losses


def _drawdown(values: Sequence[float]) -> float:
    equity = peak = maximum = 0.0
    for value in values:
        equity += value
        peak = max(peak, equity)
        maximum = max(maximum, peak - equity)
    return maximum


def _percentile(values: Sequence[float], fraction: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    return ordered[max(0, math.ceil(fraction * len(ordered)) - 1)]


def _bootstrap_lower(values: Sequence[float], confidence: float) -> float | None:
    if not values:
        return None
    import random

    digest = hashlib.sha256(_canonical_bytes([round(value, 10) for value in values])).hexdigest()
    rng = random.Random(int(digest[:16], 16))
    size = len(values)
    means = [statistics.fmean(rng.choice(values) for _ in range(size)) for _ in range(5000)]
    return _percentile(means, 1 - confidence)


def _robustness_metrics(
    records: Sequence[Mapping[str, Any]], confidence: float
) -> RobustnessMetrics:
    signals = [
        record
        for record in records
        if record["record_type"] == "signal"
        and record["mode"] == "historical"
        and record["closed"]
        and record["eligible"]
    ]
    signals.sort(key=lambda item: (str(item["date"]), str(item["signal_id"])))
    values = [float(record["net_r"]) for record in signals]
    midpoint = len(values) // 2
    without_best = sorted(values, reverse=True)[5:]
    stress_10 = [float(record["stress_10bps_r"]) for record in signals]
    stress_20 = [float(record["stress_20bps_r"]) for record in signals]
    return RobustnessMetrics(
        signals=len(signals),
        expectancy_r=statistics.fmean(values) if values else None,
        profit_factor=_profit_factor(values),
        maximum_drawdown_r=_drawdown(values),
        bootstrap_lower_expectancy_r=_bootstrap_lower(values, confidence),
        first_half_total_r=sum(values[:midpoint]) if midpoint else None,
        second_half_total_r=sum(values[midpoint:]) if midpoint else None,
        without_five_best_total_r=sum(without_best) if without_best else None,
        stress_10_total_r=sum(stress_10) if stress_10 else None,
        stress_10_profit_factor=_profit_factor(stress_10),
        stress_10_drawdown_r=_drawdown(stress_10) if stress_10 else None,
        stress_20_total_r=sum(stress_20) if stress_20 else None,
        stress_20_profit_factor=_profit_factor(stress_20),
        stress_20_drawdown_r=_drawdown(stress_20) if stress_20 else None,
        rule_violations=sum(
            len(record["rule_violations"])
            for record in records
            if record["record_type"] != "inspection"
        ),
        incomplete_capture_records=sum(
            record.get("session_capture_complete") is not True
            for record in records
            if record["record_type"] != "inspection"
        ),
    )


def _metrics(records: Sequence[Mapping[str, Any]], confidence: float) -> StrategyMetrics:
    signals = [record for record in records if record["record_type"] == "signal"]
    historical_records = [
        record
        for record in records
        if record.get("sample_phase") in {"development", "confirmation"}
    ]
    development_records = [
        record for record in records if record.get("sample_phase") == "development"
    ]
    confirmation_records = [
        record for record in records if record.get("sample_phase") == "confirmation"
    ]
    historical = _robustness_metrics(historical_records, confidence)
    development = _robustness_metrics(development_records, confidence)
    confirmation = _robustness_metrics(confirmation_records, confidence)
    shadow = [
        record
        for record in signals
        if record["mode"] == "shadow" and record["closed"] and record["eligible"]
    ]
    live = [
        record
        for record in signals
        if record["mode"] == "live" and record["closed"] and record["eligible"]
    ]
    violations = sum(
        len(record["rule_violations"])
        for record in records
        if record["record_type"] != "inspection"
    )
    incomplete = sum(
        record.get("session_capture_complete") is not True
        for record in records
        if record["record_type"] != "inspection"
    )
    entry_slippage = [float(record["entry_slippage_bps"]) for record in live]
    unprotected = [float(record["unprotected_seconds"]) for record in live]
    stopped = [record for record in live if record["stop_executed"] is True]
    stop_excess = [
        float(record["stop_slippage_bps"]) - float(record["stop_reserve_bps"])
        for record in stopped
    ]
    return StrategyMetrics(
        historical_signals=historical.signals,
        development_signals=development.signals,
        confirmation_signals=confirmation.signals,
        shadow_executions=len(shadow),
        live_executions=len(live),
        natural_stop_executions=len(stopped),
        expectancy_r=historical.expectancy_r,
        confirmation_expectancy_r=confirmation.expectancy_r,
        profit_factor=historical.profit_factor,
        maximum_drawdown_r=historical.maximum_drawdown_r,
        bootstrap_lower_expectancy_r=historical.bootstrap_lower_expectancy_r,
        first_half_total_r=historical.first_half_total_r,
        second_half_total_r=historical.second_half_total_r,
        without_five_best_total_r=historical.without_five_best_total_r,
        stress_10_total_r=historical.stress_10_total_r,
        stress_10_profit_factor=historical.stress_10_profit_factor,
        stress_10_drawdown_r=historical.stress_10_drawdown_r,
        stress_20_total_r=historical.stress_20_total_r,
        stress_20_profit_factor=historical.stress_20_profit_factor,
        stress_20_drawdown_r=historical.stress_20_drawdown_r,
        development=development,
        confirmation=confirmation,
        entry_slippage_p95_bps=_percentile(entry_slippage, 0.95),
        unprotected_p95_seconds=_percentile(unprotected, 0.95),
        stop_slippage_excess_p95_bps=_percentile(stop_excess, 0.95),
        rule_violations=violations,
        incomplete_capture_records=incomplete,
    )


def _inspection_blockers(
    inspection: Mapping[str, Any] | None,
    config: PortfolioConfig,
) -> list[str]:
    if inspection is None:
        return ["independent strategy inspection is missing"]
    blockers: list[str] = []
    ready = config.raw["pilot_ready"]
    required = {
        "trial_accounting_complete": ready["require_complete_trial_accounting"],
        "multiple_testing_clear": ready["require_clear_multiple_testing_audit"],
        "execution_model_complete": ready["require_complete_execution_model"],
        "development_universe_representative": True,
        "confirmation_untouched": True,
    }
    for field, expected in required.items():
        if expected and inspection.get(field) is not True:
            blockers.append(f"{field} is not true")
    if int(inspection.get("confirmation_embargo_trading_days", -1)) < int(
        config.raw["portfolio"]["maximum_holding_trading_days"]
    ):
        blockers.append("confirmation embargo is shorter than maximum holding period")
    return blockers


def _at_least(blockers: list[str], name: str, value: int, required: int) -> None:
    if value < required:
        blockers.append(f"{name} {value} is below required {required}")


def _positive(blockers: list[str], name: str, value: float | None) -> None:
    if value is None or value <= 0:
        blockers.append(f"{name} is not above 0")


def _above_number(
    blockers: list[str], name: str, value: float | None, threshold: float
) -> None:
    if value is None or value <= threshold:
        blockers.append(f"{name} is not above required {threshold}")


def _minimum_number(blockers: list[str], name: str, value: float | None, required: float) -> None:
    if value is None or value < required:
        blockers.append(f"{name} is below required {required}")


def _maximum_number(blockers: list[str], name: str, value: float | None, maximum: float) -> None:
    if value is None or value > maximum:
        blockers.append(f"{name} is missing or above {maximum}")


def _robustness_blockers(
    name: str,
    metrics: RobustnessMetrics,
    *,
    minimum_signals: int,
    expectancy_threshold: float,
    gate: Mapping[str, Any],
) -> list[str]:
    blockers: list[str] = []
    _at_least(blockers, f"{name} signals", metrics.signals, minimum_signals)
    _above_number(
        blockers,
        f"{name} expectancy R",
        metrics.expectancy_r,
        expectancy_threshold,
    )
    _minimum_number(
        blockers,
        f"{name} profit factor",
        metrics.profit_factor,
        float(gate["minimum_profit_factor"]),
    )
    _positive(
        blockers,
        f"{name} bootstrap lower expectancy R",
        metrics.bootstrap_lower_expectancy_r,
    )
    _maximum_number(
        blockers,
        f"{name} maximum drawdown R",
        metrics.maximum_drawdown_r,
        float(gate["maximum_drawdown_r"]),
    )
    if gate["require_positive_chronological_halves"]:
        _positive(
            blockers,
            f"{name} first chronological half total R",
            metrics.first_half_total_r,
        )
        _positive(
            blockers,
            f"{name} second chronological half total R",
            metrics.second_half_total_r,
        )
    if gate["require_positive_without_five_best"]:
        _positive(
            blockers,
            f"{name} total R without five best",
            metrics.without_five_best_total_r,
        )
    for bps in (10, 20):
        _positive(
            blockers,
            f"{name} {bps} bps stress total R",
            getattr(metrics, f"stress_{bps}_total_r"),
        )
        _minimum_number(
            blockers,
            f"{name} {bps} bps stress profit factor",
            getattr(metrics, f"stress_{bps}_profit_factor"),
            float(gate["minimum_stressed_profit_factor"]),
        )
        _maximum_number(
            blockers,
            f"{name} {bps} bps stress drawdown R",
            getattr(metrics, f"stress_{bps}_drawdown_r"),
            float(gate["maximum_stressed_drawdown_r"]),
        )
    if metrics.rule_violations > int(gate["maximum_rule_violations"]):
        blockers.append(f"{name} rule violations exceed zero")
    if metrics.incomplete_capture_records:
        blockers.append(f"{name} evidence contains incomplete capture records")
    return blockers


def assess_strategy(
    records: Sequence[Mapping[str, Any]],
    config: PortfolioConfig,
) -> dict[str, Any]:
    if not records:
        raise PortfolioMaturityError("strategy assessment requires records")
    identities = {_strategy_identity(record) for record in records}
    if len(identities) != 1:
        raise PortfolioMaturityError("strategy evidence identity drifted")
    strategy_id, version, family, rules_hash = next(iter(identities))
    inspections = [record for record in records if record["record_type"] == "inspection"]
    if len(inspections) > 1:
        raise PortfolioMaturityError("strategy has multiple final inspections")
    confidence = float(config.raw["pilot_ready"]["minimum_bootstrap_confidence"])
    metrics = _metrics(records, confidence)
    gate = config.raw["pilot_ready"]
    inspection = inspections[0] if inspections else None
    retired_after_development = bool(
        inspection and inspection.get("retired_after_development") is True
    )
    inspection_blockers = _inspection_blockers(inspection, config)
    minimum_confirmation = int(gate["minimum_confirmation_signals"])
    minimum_development = (
        int(gate["minimum_closed_historical_signals"]) - minimum_confirmation
    )
    if minimum_development < 1:
        raise PortfolioMaturityError(
            "historical signal minimum must exceed confirmation minimum"
        )
    development_blockers = _robustness_blockers(
        "development",
        metrics.development,
        minimum_signals=minimum_development,
        expectancy_threshold=float(gate["minimum_expectancy_r"]),
        gate=gate,
    )
    if retired_after_development:
        if not development_blockers:
            raise PortfolioMaturityError(
                "retired_after_development requires a failed development gate"
            )
        forbidden = [
            record
            for record in records
            if record.get("sample_phase") in {"confirmation", "shadow", "live"}
        ]
        if forbidden:
            raise PortfolioMaturityError(
                "retired development strategy cannot contain later-phase evidence"
            )
    confirmation_blockers = _robustness_blockers(
        "confirmation",
        metrics.confirmation,
        minimum_signals=minimum_confirmation,
        expectancy_threshold=float(gate["minimum_confirmation_expectancy_r"]),
        gate=gate,
    )
    blockers = [
        *inspection_blockers,
        *development_blockers,
        *confirmation_blockers,
    ]
    _at_least(blockers, "historical signals", metrics.historical_signals, int(gate["minimum_closed_historical_signals"]))
    _at_least(blockers, "shadow executions", metrics.shadow_executions, int(gate["minimum_shadow_executions"]))
    _above_number(
        blockers,
        "expectancy R",
        metrics.expectancy_r,
        float(gate["minimum_expectancy_r"]),
    )
    _above_number(
        blockers,
        "confirmation expectancy R",
        metrics.confirmation_expectancy_r,
        float(gate["minimum_confirmation_expectancy_r"]),
    )
    _minimum_number(blockers, "profit factor", metrics.profit_factor, float(gate["minimum_profit_factor"]))
    _positive(blockers, "bootstrap lower expectancy R", metrics.bootstrap_lower_expectancy_r)
    _maximum_number(blockers, "maximum drawdown R", metrics.maximum_drawdown_r, float(gate["maximum_drawdown_r"]))
    if gate["require_positive_chronological_halves"]:
        _positive(blockers, "first chronological half total R", metrics.first_half_total_r)
        _positive(blockers, "second chronological half total R", metrics.second_half_total_r)
    if gate["require_positive_without_five_best"]:
        _positive(blockers, "total R without five best", metrics.without_five_best_total_r)
    for bps in (10, 20):
        _positive(blockers, f"{bps} bps stress total R", getattr(metrics, f"stress_{bps}_total_r"))
        _minimum_number(blockers, f"{bps} bps stress profit factor", getattr(metrics, f"stress_{bps}_profit_factor"), float(gate["minimum_stressed_profit_factor"]))
        _maximum_number(blockers, f"{bps} bps stress drawdown R", getattr(metrics, f"stress_{bps}_drawdown_r"), float(gate["maximum_stressed_drawdown_r"]))
    if metrics.rule_violations > int(gate["maximum_rule_violations"]):
        blockers.append("rule violations exceed zero")
    if metrics.incomplete_capture_records:
        blockers.append("evidence contains incomplete capture records")
    blockers = list(dict.fromkeys(blockers))
    if retired_after_development:
        validation_phase = "RETIRED_DEVELOPMENT"
        current_phase_blockers = development_blockers
    elif inspection_blockers or development_blockers:
        validation_phase = "DEVELOPMENT"
        current_phase_blockers = [*inspection_blockers, *development_blockers]
    elif confirmation_blockers:
        validation_phase = "CONFIRMATION"
        current_phase_blockers = confirmation_blockers
    elif metrics.shadow_executions < int(gate["minimum_shadow_executions"]):
        validation_phase = "SHADOW_QUALIFICATION"
        current_phase_blockers = [
            item for item in blockers if item.startswith("shadow executions")
        ]
    elif blockers:
        validation_phase = "INDEPENDENT_INSPECTION"
        current_phase_blockers = blockers
    elif metrics.live_executions == 0:
        validation_phase = "LIVE_PILOT_READY"
        current_phase_blockers = []
    else:
        validation_phase = "LIVE_PILOT"
        current_phase_blockers = []
    live_gate = config.raw["live_validated"]
    live_blockers: list[str] = list(blockers)
    _at_least(live_blockers, "live executions", metrics.live_executions, int(live_gate["minimum_live_executions"]))
    _at_least(live_blockers, "natural stop executions", metrics.natural_stop_executions, int(live_gate["minimum_natural_stop_executions"]))
    _maximum_number(live_blockers, "entry slippage p95 bps", metrics.entry_slippage_p95_bps, float(live_gate["maximum_entry_slippage_p95_bps"]))
    _maximum_number(live_blockers, "unprotected p95 seconds", metrics.unprotected_p95_seconds, float(live_gate["maximum_unprotected_p95_seconds"]))
    _maximum_number(live_blockers, "stop slippage excess p95 bps", metrics.stop_slippage_excess_p95_bps, float(live_gate["maximum_stop_slippage_excess_p95_bps"]))
    return {
        "strategy_id": strategy_id,
        "strategy_version": version,
        "mechanism_family": family,
        "rules_hash": rules_hash,
        "source_stage0_variant_id": (
            inspection.get("source_stage0_variant_id") if inspection else None
        ),
        "source_stage0_result_sha256": (
            inspection.get("source_stage0_result_sha256") if inspection else None
        ),
        "retired_after_development": retired_after_development,
        "maturity": "LIVE_VALIDATED" if not live_blockers else ("PILOT_READY" if not blockers else "RESEARCH"),
        "pilot_ready": not blockers,
        "live_started": metrics.live_executions > 0,
        "live_validated": not live_blockers,
        "validation_phase": validation_phase,
        "current_phase_blockers": current_phase_blockers,
        "metrics": asdict(metrics),
        "pilot_ready_blockers": blockers,
        "live_validated_blockers": live_blockers,
    }


def _pearson(left: Sequence[float], right: Sequence[float]) -> float | None:
    if len(left) != len(right) or len(left) < 2:
        return None
    left_mean = statistics.fmean(left)
    right_mean = statistics.fmean(right)
    left_deviation = [value - left_mean for value in left]
    right_deviation = [value - right_mean for value in right]
    denominator = math.sqrt(
        sum(value * value for value in left_deviation)
        * sum(value * value for value in right_deviation)
    )
    if denominator == 0:
        return None
    return sum(a * b for a, b in zip(left_deviation, right_deviation, strict=True)) / denominator


def _confirmation_daily_returns(records: Sequence[Mapping[str, Any]]) -> dict[str, float]:
    sessions = {
        str(record["date"]): 0.0
        for record in records
        if record["record_type"] == "session" and record["sample_phase"] == "confirmation"
    }
    for record in records:
        if record["record_type"] == "signal" and record["sample_phase"] == "confirmation":
            sessions[str(record["date"])] = sessions.get(str(record["date"]), 0.0) + float(record["net_r"])
    return sessions


def _pair_correlation(left: Sequence[Mapping[str, Any]], right: Sequence[Mapping[str, Any]]) -> float | None:
    left_daily = _confirmation_daily_returns(left)
    right_daily = _confirmation_daily_returns(right)
    common = sorted(set(left_daily) & set(right_daily))
    if len(common) < 20:
        return None
    return _pearson([left_daily[day] for day in common], [right_daily[day] for day in common])


def _coverage(selected: Sequence[Sequence[Mapping[str, Any]]]) -> float | None:
    date_sets: list[set[str]] = []
    eligible: set[str] = set()
    for records in selected:
        strategy_dates: set[str] = set()
        for record in records:
            if record["record_type"] != "session" or record["sample_phase"] != "confirmation":
                continue
            day = str(record["date"])
            strategy_dates.add(day)
            if record["eligible_signal"] is True:
                eligible.add(day)
        date_sets.append(strategy_dates)
    shared_dates = set.intersection(*date_sets) if date_sets else set()
    return len(eligible & shared_dates) / len(shared_dates) if shared_dates else None


def build_report(
    records: Sequence[Mapping[str, Any]] | None = None,
    config: PortfolioConfig | None = None,
) -> dict[str, Any]:
    config = config or load_config()
    records = list(records if records is not None else read_records())
    grouped: dict[tuple[str, str], list[Mapping[str, Any]]] = defaultdict(list)
    for record in records:
        grouped[(str(record["strategy_id"]), str(record["strategy_version"]))].append(record)
    assessments = [assess_strategy(items, config) for _, items in sorted(grouped.items())]
    ready = [assessment for assessment in assessments if assessment["pilot_ready"]]
    ready_live_started = [
        assessment for assessment in ready if assessment["live_started"]
    ]
    target = int(config.raw["campaign"]["target_pilot_ready_strategies"])
    correlation_cap = float(config.raw["portfolio"]["maximum_ready_pairwise_correlation"])
    coverage_floor = float(config.raw["portfolio"]["minimum_combined_confirmation_opportunity_coverage"])
    selected_ids: list[str] = []
    selected_strategies: list[dict[str, str]] = []
    selected_candidate: tuple[dict[str, Any], ...] = ()
    pairwise: dict[str, float | None] = {}
    selected_coverage: float | None = None
    grouped_by_identity = {
        (str(items[0]["strategy_id"]), str(items[0]["strategy_version"])): items
        for items in grouped.values()
    }
    for candidate in itertools.combinations(ready, target):
        if len({str(item["mechanism_family"]) for item in candidate}) != target:
            continue
        candidate_pairs: dict[str, float | None] = {}
        clean = True
        candidate_records = []
        for item in candidate:
            candidate_records.append(grouped_by_identity[(str(item["strategy_id"]), str(item["strategy_version"]))])
        for left, right in itertools.combinations(candidate, 2):
            left_records = grouped_by_identity[(str(left["strategy_id"]), str(left["strategy_version"]))]
            right_records = grouped_by_identity[(str(right["strategy_id"]), str(right["strategy_version"]))]
            correlation = _pair_correlation(left_records, right_records)
            key = (
                f"{left['strategy_id']}@{left['strategy_version']}::"
                f"{right['strategy_id']}@{right['strategy_version']}"
            )
            candidate_pairs[key] = correlation
            if correlation is None or correlation >= correlation_cap:
                clean = False
        candidate_coverage = _coverage(candidate_records)
        if candidate_coverage is None or candidate_coverage < coverage_floor:
            clean = False
        if clean:
            selected_ids = [str(item["strategy_id"]) for item in candidate]
            selected_strategies = [
                {
                    "strategy_id": str(item["strategy_id"]),
                    "strategy_version": str(item["strategy_version"]),
                }
                for item in candidate
            ]
            selected_candidate = candidate
            pairwise = candidate_pairs
            selected_coverage = candidate_coverage
            break
    blockers: list[str] = []
    inspections = [
        record for record in records if record["record_type"] == "inspection"
    ]
    wave_one_families = {
        str(record["mechanism_family"])
        for record in inspections
        if record.get("tournament_wave") == 1
    }
    wave_two_families = {
        str(record["mechanism_family"])
        for record in inspections
        if record.get("tournament_wave") == 2
    }
    if len(wave_one_families) > int(config.raw["campaign"]["initial_mechanism_families"]):
        blockers.append("initial mechanism-family count exceeds campaign ceiling")
    if sum(record.get("tournament_wave") == 1 for record in inspections) > int(
        config.raw["campaign"]["maximum_initial_variants"]
    ):
        blockers.append("initial variant count exceeds campaign ceiling")
    if len(wave_two_families) > int(config.raw["campaign"]["maximum_second_wave_families"]):
        blockers.append("second-wave mechanism-family count exceeds campaign ceiling")
    if len(ready) < target:
        blockers.append(f"pilot-ready strategies {len(ready)} is below required {target}")
    if not selected_ids:
        blockers.append("no three-strategy subset passes family, correlation, and opportunity-coverage gates")
    live_started = [item for item in selected_candidate if item["live_started"]]
    if selected_ids and len(live_started) < target:
        blockers.append(f"selected strategies with a started live pilot {len(live_started)} is below required {target}")
    interim_blockers: list[str] = []
    if not ready_live_started:
        interim_blockers.append(
            "pilot-ready strategies with a completed live execution 0 is below required 1"
        )
    return {
        "schema_version": 1,
        "campaign_id": config.raw["campaign"]["id"],
        "portfolio_config_sha256": config.sha256,
        "target_pilot_ready_strategies": target,
        "strategies": assessments,
        "pilot_ready_strategy_count": len(ready),
        "live_started_strategy_count": sum(
            assessment["live_started"] for assessment in assessments
        ),
        "pilot_ready_live_started_strategy_count": len(ready_live_started),
        "pilot_ready_live_started_strategies": [
            {
                "strategy_id": str(assessment["strategy_id"]),
                "strategy_version": str(assessment["strategy_version"]),
                "mechanism_family": str(assessment["mechanism_family"]),
                "rules_hash": str(assessment["rules_hash"]),
            }
            for assessment in ready_live_started
        ],
        "earned_interim_milestones": (
            [FIRST_PILOT_MILESTONE] if not interim_blockers else []
        ),
        "interim_milestone_blockers": interim_blockers,
        "selected_strategy_ids": selected_ids,
        "selected_strategies": selected_strategies,
        "selected_pairwise_confirmation_correlations": pairwise,
        "selected_confirmation_opportunity_coverage": selected_coverage,
        "live_started_selected_strategy_count": len(live_started),
        "earned_milestone": "THREE_PILOT_READY_LIVE_STARTED" if not blockers else "RESEARCH",
        "milestone_blockers": blockers,
    }


def audit_ledger(path: Path = DEFAULT_LEDGER_PATH, *, root: Path = PROJECT_ROOT) -> dict[str, Any]:
    records = read_records(path, root=root)
    return {
        "path": str(path),
        "records": len(records),
        "strategies": len({(record["strategy_id"], record["strategy_version"]) for record in records}),
        "valid": True,
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ledger", type=Path, default=DEFAULT_LEDGER_PATH)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG_PATH)
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("audit")
    subparsers.add_parser("report")
    record = subparsers.add_parser("record")
    record.add_argument("json_file", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        config = load_config(args.config)
        if args.command == "audit":
            result = audit_ledger(args.ledger)
        elif args.command == "report":
            result = build_report(read_records(args.ledger), config)
        else:
            value = json.loads(args.json_file.read_text(encoding="utf-8"))
            if not isinstance(value, dict):
                raise PortfolioMaturityError("record file must contain an object")
            append_record(value, args.ledger)
            result = {"recorded": True, "record_type": value.get("record_type")}
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    except (PortfolioMaturityError, OSError, json.JSONDecodeError) as exc:
        print(json.dumps({"error": str(exc), "error_type": type(exc).__name__}, indent=2), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

"""Produce evidence-gated strategy learning reports and review proposals.

This module never edits ``strategy_config.toml``.  It converts frozen-rule
signal history and archived terminal outcomes into diagnostics and, only after
the configured review cadence is earned, a proposal for an evidence-backed
application decision. Applying a proposal requires a separate strategy-version
workflow and can never occur through this module.
"""

from __future__ import annotations

import argparse
import json
import math
import statistics
import sys
from collections import Counter
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence
from zoneinfo import ZoneInfo

from strategy_engine import StrategyConfig, StrategyInputError, load_config
from strategy_ledger import (
    DEFAULT_LEDGER_PATH,
    LedgerError,
    audit_ledger,
    build_report,
    read_records,
)
from trade_lifecycle import (
    DEFAULT_ARCHIVE_ROOT,
    LifecycleError,
    load_archived_outcomes,
)


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_PROPOSAL_ROOT = PROJECT_ROOT / "strategy_proposals"
LEARNING_SCHEMA_VERSION = 2
MINIMUM_SIGNALS_BETWEEN_REVIEWS = 20
MINIMUM_DAYS_BETWEEN_REVIEWS = 30
MINIMUM_FEATURE_COHORT = 8
TRACKED_FEATURES = (
    "opening_relative_volume",
    "score",
    "median_spread_bps",
    "stop_fraction",
    "resistance_room_fraction",
    "reward_risk",
)
DAILY_METRIC_PREFLIGHT_REASONS = frozenset(
    {
        "average daily volume is below the universe minimum",
        "daily ATR is below the universe minimum",
    }
)
PRE_SESSION_UNIVERSE_REASONS = DAILY_METRIC_PREFLIGHT_REASONS | frozenset(
    {
        "security is not a U.S.-listed common stock",
        "catalyst has a dilution or financing conflict",
    }
)


class LearningError(ValueError):
    """Raised when the learning dataset or review request is invalid."""


@dataclass(frozen=True)
class ReviewCadence:
    eligible: bool
    as_of: str
    last_review_date: str
    current_closed_signals: int
    closed_signals_at_last_review: int
    new_closed_signals: int
    elapsed_days: int
    blockers: tuple[str, ...]


def _finite(value: Any) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _current_records(
    records: Sequence[Mapping[str, Any]], config: StrategyConfig
) -> list[Mapping[str, Any]]:
    return [
        record
        for record in records
        if record.get("strategy_version") == config.version
        and record.get("rules_hash") == config.rules_hash
    ]


def _closed_signals(
    records: Sequence[Mapping[str, Any]], config: StrategyConfig
) -> list[Mapping[str, Any]]:
    return [
        record
        for record in _current_records(records, config)
        if record.get("record_type") == "signal"
        and record.get("closed") is True
        and record.get("triggered") is True
        and _finite(record.get("net_r")) is not None
    ]


def _version_start(config: StrategyConfig) -> date:
    prefix = config.version[:10]
    try:
        return date.fromisoformat(prefix)
    except ValueError:
        return datetime.now(ZoneInfo("America/New_York")).date()


def _review_files(proposal_root: Path) -> list[Path]:
    return sorted(proposal_root.glob("*.json")) if proposal_root.exists() else []


def _last_review(
    proposal_root: Path, config: StrategyConfig
) -> tuple[date, int, str | None]:
    candidates: list[tuple[date, int, str]] = []
    for path in _review_files(proposal_root):
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise LearningError(f"cannot read prior proposal {path}: {exc}") from exc
        if not isinstance(value, Mapping):
            raise LearningError(f"prior proposal must be an object: {path}")
        if (
            value.get("strategy_version") != config.version
            or value.get("rules_hash") != config.rules_hash
        ):
            continue
        generated = value.get("generated_at")
        reviewed_count = value.get("reviewed_closed_signals")
        if not isinstance(generated, str) or not isinstance(reviewed_count, int):
            raise LearningError(f"prior proposal metadata is incomplete: {path}")
        try:
            generated_day = datetime.fromisoformat(generated).date()
        except ValueError as exc:
            raise LearningError(f"prior proposal timestamp is invalid: {path}") from exc
        candidates.append((generated_day, reviewed_count, str(path)))
    if not candidates:
        return _version_start(config), 0, None
    return max(candidates, key=lambda item: (item[0], item[1]))


def assess_review_cadence(
    records: Sequence[Mapping[str, Any]],
    config: StrategyConfig | None = None,
    *,
    proposal_root: Path = DEFAULT_PROPOSAL_ROOT,
    as_of: date | None = None,
) -> ReviewCadence:
    config = config or load_config()
    current_day = as_of or datetime.now(ZoneInfo("America/New_York")).date()
    closed_count = len(_closed_signals(records, config))
    last_day, last_count, _ = _last_review(proposal_root, config)
    new_count = max(0, closed_count - last_count)
    elapsed = (current_day - last_day).days
    blockers: list[str] = []
    if closed_count < last_count:
        blockers.append(
            "current closed-signal count is below the last reviewed count; history may have been removed"
        )
    if new_count < MINIMUM_SIGNALS_BETWEEN_REVIEWS:
        blockers.append(
            f"new closed signals {new_count} is below required "
            f"{MINIMUM_SIGNALS_BETWEEN_REVIEWS}"
        )
    if elapsed < MINIMUM_DAYS_BETWEEN_REVIEWS:
        blockers.append(
            f"days since last review {elapsed} is below required "
            f"{MINIMUM_DAYS_BETWEEN_REVIEWS}"
        )
    return ReviewCadence(
        eligible=not blockers,
        as_of=current_day.isoformat(),
        last_review_date=last_day.isoformat(),
        current_closed_signals=closed_count,
        closed_signals_at_last_review=last_count,
        new_closed_signals=new_count,
        elapsed_days=elapsed,
        blockers=tuple(blockers),
    )


def _performance(values: Sequence[float]) -> dict[str, Any]:
    if not values:
        return {"count": 0, "expectancy_r": None, "win_rate": None}
    return {
        "count": len(values),
        "expectancy_r": statistics.fmean(values),
        "win_rate": sum(value > 0 for value in values) / len(values),
    }


def _feature_diagnostics(
    closed: Sequence[Mapping[str, Any]],
) -> dict[str, dict[str, Any]]:
    diagnostics: dict[str, dict[str, Any]] = {}
    for name in TRACKED_FEATURES:
        points: list[tuple[float, float]] = []
        for record in closed:
            features = record.get("features")
            if not isinstance(features, Mapping):
                continue
            feature_value = _finite(features.get(name))
            net_r = _finite(record.get("net_r"))
            if feature_value is not None and net_r is not None:
                points.append((feature_value, net_r))
        diagnostics[name] = {
            "available": len(points),
            "missing": len(closed) - len(points),
            "minimum": min((value for value, _ in points), default=None),
            "median": statistics.median(value for value, _ in points)
            if points
            else None,
            "maximum": max((value for value, _ in points), default=None),
        }
    return diagnostics


def _signal_diagnostics(
    records: Sequence[Mapping[str, Any]], config: StrategyConfig
) -> dict[str, Any]:
    signals = [
        record
        for record in _current_records(records, config)
        if record.get("record_type") == "signal"
    ]
    reason_counts: Counter[str] = Counter()
    pre_session_rejects = 0
    daily_metric_rejects = 0
    for record in signals:
        raw_reasons = record.get("rejection_reasons")
        reasons = (
            [str(value) for value in raw_reasons]
            if isinstance(raw_reasons, list)
            else []
        )
        reason_counts.update(reasons)
        if PRE_SESSION_UNIVERSE_REASONS.intersection(reasons):
            pre_session_rejects += 1
        if DAILY_METRIC_PREFLIGHT_REASONS.intersection(reasons):
            daily_metric_rejects += 1
    return {
        "signals": len(signals),
        "triggered_signals": sum(record.get("triggered") is True for record in signals),
        "eligible_signals": sum(record.get("eligible") is True for record in signals),
        "closed_performance_signals": len(_closed_signals(signals, config)),
        "rejected_signals": sum(
            record.get("decision") == "rejected" for record in signals
        ),
        "pre_session_universe_rejects": pre_session_rejects,
        "pre_session_universe_reject_fraction": (
            pre_session_rejects / len(signals) if signals else None
        ),
        "daily_metric_preflight_rejects": daily_metric_rejects,
        "daily_metric_preflight_reject_fraction": (
            daily_metric_rejects / len(signals) if signals else None
        ),
        "rejection_reason_counts": dict(reason_counts.most_common()),
    }


def _split_feature(
    closed: Sequence[Mapping[str, Any]], name: str, threshold: float
) -> tuple[list[float], list[float]]:
    lower: list[float] = []
    upper: list[float] = []
    for record in closed:
        features = record.get("features")
        value = _finite(features.get(name)) if isinstance(features, Mapping) else None
        net_r = _finite(record.get("net_r"))
        if value is None or net_r is None:
            continue
        (upper if value >= threshold else lower).append(net_r)
    return lower, upper


def _hypotheses(
    closed: Sequence[Mapping[str, Any]], report: Mapping[str, Any]
) -> list[dict[str, Any]]:
    """Create bounded research hypotheses, never executable config patches."""
    hypotheses: list[dict[str, Any]] = []
    overlay = report["exit_overlay"]
    if (
        overlay["paired_signals"] >= MINIMUM_SIGNALS_BETWEEN_REVIEWS
        and _finite(overlay["mean_project_minus_paper_r"]) is not None
        and float(overlay["mean_project_minus_paper_r"]) <= -0.25
    ):
        hypotheses.append(
            {
                "category": "exit_overlay",
                "observation": "The project exit trails the paired EOD baseline by at least 0.25R on average.",
                "suggested_research": "Test a separately versioned exit overlay against the frozen baseline.",
                "evidence": dict(overlay),
                "requires_confirmation_sample": True,
            }
        )

    maturity_metrics = report["maturity"]["metrics"]
    stop_excess = _finite(maturity_metrics.get("stop_slippage_excess_p95_bps"))
    if (
        maturity_metrics.get("stop_execution_signals", 0) >= 5
        and stop_excess is not None
        and stop_excess > 0
    ):
        hypotheses.append(
            {
                "category": "stop_slippage",
                "observation": "Observed p95 stop slippage exceeds the planned reserve.",
                "suggested_research": "Calibrate a larger stop-slippage reserve in a new strategy version; do not widen technical stops.",
                "evidence": {
                    "stop_execution_signals": maturity_metrics[
                        "stop_execution_signals"
                    ],
                    "stop_slippage_excess_p95_bps": stop_excess,
                },
                "requires_confirmation_sample": True,
            }
        )

    threshold_checks = (
        (
            "opening_relative_volume",
            3.0,
            "Test raising the minimum opening relative volume to 3.0.",
        ),
        ("score", 95.0, "Test a 95-point minimum score."),
    )
    for feature, threshold, suggestion in threshold_checks:
        lower, upper = _split_feature(closed, feature, threshold)
        if len(lower) < MINIMUM_FEATURE_COHORT or len(upper) < MINIMUM_FEATURE_COHORT:
            continue
        low_metrics = _performance(lower)
        high_metrics = _performance(upper)
        low_expectancy = float(low_metrics["expectancy_r"])
        high_expectancy = float(high_metrics["expectancy_r"])
        if (
            low_expectancy <= 0 < high_expectancy
            and high_expectancy - low_expectancy >= 0.25
        ):
            hypotheses.append(
                {
                    "category": feature,
                    "observation": "The predeclared higher-quality cohort outperformed the lower cohort with opposite expectancy signs.",
                    "suggested_research": suggestion,
                    "evidence": {
                        "threshold": threshold,
                        "below": low_metrics,
                        "at_or_above": high_metrics,
                    },
                    "requires_confirmation_sample": True,
                }
            )
    return hypotheses


def build_learning_report(
    records: Sequence[Mapping[str, Any]],
    outcomes: Sequence[Mapping[str, Any]],
    config: StrategyConfig | None = None,
    *,
    proposal_root: Path = DEFAULT_PROPOSAL_ROOT,
    as_of: date | None = None,
) -> dict[str, Any]:
    config = config or load_config()
    cadence = assess_review_cadence(
        records, config, proposal_root=proposal_root, as_of=as_of
    )
    closed = _closed_signals(records, config)
    strategy_report = build_report(records, config)
    relevant_outcomes = [
        outcome
        for outcome in outcomes
        if outcome.get("strategy_version") == config.version
        and outcome.get("rules_hash") == config.rules_hash
    ]
    result_counts = Counter(str(item.get("result")) for item in relevant_outcomes)
    reason_counts = Counter(
        str(item.get("primary_reason")) for item in relevant_outcomes
    )
    signal_diagnostics = _signal_diagnostics(records, config)
    hypotheses = _hypotheses(closed, strategy_report) if cadence.eligible else []
    return {
        "schema_version": LEARNING_SCHEMA_VERSION,
        "strategy_version": config.version,
        "rules_hash": config.rules_hash,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "automatic_application": False,
        "status": "review_ready" if cadence.eligible else "cadence_blocked",
        "cadence": asdict(cadence),
        "performance": strategy_report,
        "terminal_outcomes": {
            "records": len(relevant_outcomes),
            "result_counts": dict(sorted(result_counts.items())),
            "primary_reason_counts": dict(reason_counts.most_common()),
        },
        "signal_diagnostics": signal_diagnostics,
        "feature_diagnostics": _feature_diagnostics(closed),
        "hypotheses": hypotheses,
        "recommendation": (
            "Review the listed hypotheses; any accepted change must create a new strategy version and confirmation sample."
            if hypotheses
            else (
                "Keep the frozen rules; enforce pre-session universe gates before "
                "target-session collection, then continue complete data collection."
                if signal_diagnostics["pre_session_universe_rejects"]
                else "Keep the frozen rules and continue complete data collection."
            )
        ),
    }


def write_proposal(
    report: Mapping[str, Any], proposal_root: Path = DEFAULT_PROPOSAL_ROOT
) -> Path:
    if report.get("status") != "review_ready":
        blockers = report.get("cadence", {}).get("blockers", [])
        raise LearningError(f"strategy review cadence is blocked: {blockers}")
    proposal_root.mkdir(parents=True, exist_ok=True)
    generated = datetime.fromisoformat(str(report["generated_at"]))
    filename = generated.strftime("%Y%m%dT%H%M%SZ") + "-strategy-review.json"
    destination = proposal_root / filename
    if destination.exists():
        raise LearningError(f"proposal already exists: {destination}")
    proposal = dict(report)
    cadence = report["cadence"]
    proposal["reviewed_closed_signals"] = cadence["current_closed_signals"]
    # Retained for schema compatibility. Repository governance delegates the
    # evidence-backed decision to Codex, while this tool remains non-applying.
    proposal["requires_user_approval"] = False
    proposal["delegated_application_decision"] = True
    proposal["requires_separate_production_change_workflow"] = True
    proposal["requires_new_strategy_version"] = bool(report.get("hypotheses"))
    try:
        with destination.open("x", encoding="utf-8") as handle:
            handle.write(json.dumps(proposal, indent=2, sort_keys=True) + "\n")
    except FileExistsError as exc:
        raise LearningError(f"proposal already exists: {destination}") from exc
    return destination


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ledger", type=Path, default=DEFAULT_LEDGER_PATH)
    parser.add_argument("--archive-root", type=Path, default=DEFAULT_ARCHIVE_ROOT)
    parser.add_argument("--proposal-root", type=Path, default=DEFAULT_PROPOSAL_ROOT)
    parser.add_argument("--as-of", type=date.fromisoformat, default=None)
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("report", help="print diagnostics without changing state")
    subparsers.add_parser(
        "propose", help="write a cadence-qualified, non-executable review proposal"
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        audit = audit_ledger(args.ledger)
        if not audit.valid:
            raise LearningError(f"signal ledger audit failed: {list(audit.violations)}")
        records = read_records(args.ledger)
        outcomes = load_archived_outcomes(args.archive_root)
        report = build_learning_report(
            records,
            outcomes,
            proposal_root=args.proposal_root,
            as_of=args.as_of,
        )
        if args.command == "propose":
            report = {
                **report,
                "proposal_path": str(write_proposal(report, args.proposal_root)),
            }
        print(json.dumps(report, indent=2, sort_keys=True))
        return 0
    except (
        OSError,
        LedgerError,
        LifecycleError,
        LearningError,
        StrategyInputError,
    ) as exc:
        print(
            json.dumps({"error": str(exc), "error_type": type(exc).__name__}, indent=2),
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

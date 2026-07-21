"""Audit Stage 0 dispositions and compose the three-lane discovery funnel."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parent
SCHEMA_VERSION = 1

# The first two variants were evaluated before the post-retirement queue was
# frozen. The remaining order is the authorized order and is deliberately not
# the slate's ordinal order.
FIRST_WAVE_ORDER = (
    "etf-or-momentum-v1",
    "etf-vwap-mean-reversion-v1",
    "equity-gap-continuation-v1",
    "equity-gap-recovery-v1",
    "volatility-compression-breakout-v1",
    "short-horizon-oversold-reversal-v1",
    "cross-sectional-momentum-v1",
    "post-earnings-drift-v1",
    "relative-strength-continuation-v1",
    "catalyst-orb-retest-v1",
)

SECOND_WAVE = (
    ("sector-etf-rotation-v1", "sector-etf-rotation"),
    ("broad-etf-trend-pullback-v1", "broad-etf-trend-pullback"),
    ("close-to-open-etf-momentum-v1", "close-to-open-etf-momentum"),
    (
        "two-to-three-day-cross-sectional-reversal-v1",
        "two-to-three-day-cross-sectional-reversal",
    ),
    (
        "five-day-52-week-high-continuation-v1",
        "five-day-52-week-high-continuation",
    ),
    ("turn-of-month-etf-seasonality-v1", "turn-of-month-etf-seasonality"),
)


class PortfolioFunnelError(RuntimeError):
    """The public discovery funnel is incomplete, inconsistent, or unsafe."""


def _normalize_numbers(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _normalize_numbers(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_normalize_numbers(item) for item in value]
    if isinstance(value, float) and value.is_integer():
        return int(value)
    return value


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        _normalize_numbers(value),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode()


def _hash(value: Any) -> str:
    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def _self_hash(value: Mapping[str, Any], field: str) -> str:
    return _hash({key: item for key, item in value.items() if key != field})


def _file_hash(path: Path) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as source:
            for chunk in iter(lambda: source.read(1024 * 1024), b""):
                digest.update(chunk)
    except OSError as exc:
        raise PortfolioFunnelError(f"cannot hash {path}: {exc}") from exc
    return digest.hexdigest()


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise PortfolioFunnelError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise PortfolioFunnelError(f"{path} must contain an object")
    return value


def _one_path(root: Path, pattern: str, description: str) -> Path:
    matches = sorted(root.glob(pattern))
    if len(matches) != 1:
        raise PortfolioFunnelError(
            f"expected exactly one {description}; found {len(matches)}"
        )
    return matches[0]


def _stage0_blockers(result: Mapping[str, Any], gate: Mapping[str, Any]) -> list[str]:
    denominator = result.get("denominator")
    primary = result.get("primary_5bps")
    stress = result.get("stress")
    if not isinstance(denominator, Mapping):
        raise PortfolioFunnelError("Stage 0 result denominator is missing")
    if not isinstance(primary, Mapping):
        raise PortfolioFunnelError("Stage 0 primary metrics are missing")
    if not isinstance(stress, Mapping) or not isinstance(stress.get("20"), Mapping):
        raise PortfolioFunnelError("Stage 0 20 bps stress metrics are missing")
    blockers: list[str] = []
    if int(denominator.get("closed_signals", -1)) < int(
        gate["minimum_closed_signals"]
    ):
        blockers.append("closed signals are below the Stage 0 minimum")
    expectancy = primary.get("expectancy_r")
    if not isinstance(expectancy, (int, float)) or isinstance(expectancy, bool) or expectancy <= 0:
        blockers.append("primary expectancy is not positive")
    profit_factor = primary.get("profit_factor")
    infinite = primary.get("profit_factor_infinite") is True
    if not infinite and (
        not isinstance(profit_factor, (int, float))
        or isinstance(profit_factor, bool)
        or profit_factor < float(gate["minimum_profit_factor"])
    ):
        blockers.append("primary profit factor is below the Stage 0 minimum")
    drawdown = primary.get("maximum_drawdown_r")
    if (
        not isinstance(drawdown, (int, float))
        or isinstance(drawdown, bool)
        or drawdown > float(gate["maximum_drawdown_r"])
    ):
        blockers.append("primary drawdown exceeds the Stage 0 maximum")
    stress_total = stress["20"].get("total_r")
    if (
        not isinstance(stress_total, (int, float))
        or isinstance(stress_total, bool)
        or stress_total <= 0
    ):
        blockers.append("20 bps-per-side total R is not positive")
    if int(denominator.get("rule_violations", -1)) != int(
        gate["rule_violations"]
    ):
        blockers.append("Stage 0 rule violations are not zero")
    return blockers


def _load_slate(root: Path) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    path = _one_path(
        root,
        "strategy_tournament/manifests/portfolio-stage0-slate-*.json",
        "first-wave slate",
    )
    slate = _read_json(path)
    if slate.get("manifest_sha256") != _self_hash(slate, "manifest_sha256"):
        raise PortfolioFunnelError("first-wave slate content hash is invalid")
    variants = slate.get("variants")
    if not isinstance(variants, list):
        raise PortfolioFunnelError("first-wave slate variants are missing")
    by_id = {str(item.get("variant_id")): dict(item) for item in variants}
    if set(by_id) != set(FIRST_WAVE_ORDER):
        raise PortfolioFunnelError("first-wave slate does not match the frozen queue")
    return slate, by_id


def load_stage0_dispositions(root: Path = PROJECT_ROOT) -> list[dict[str, Any]]:
    """Return independently checked, ordered Stage 0 dispositions."""

    slate, variants = _load_slate(root)
    gate = slate.get("stage0_falsification")
    if not isinstance(gate, Mapping):
        raise PortfolioFunnelError("first-wave Stage 0 gate is missing")
    dispositions: list[dict[str, Any]] = []
    for inspection_path in sorted(
        (root / "strategy_tournament" / "inspections").glob("*-result-*.json")
    ):
        inspection = _read_json(inspection_path)
        if inspection.get("inspection_kind") != "stage0-result-inspection":
            continue
        if inspection.get("inspection_sha256") != _self_hash(
            inspection, "inspection_sha256"
        ):
            raise PortfolioFunnelError(
                f"Stage 0 result inspection hash is invalid: {inspection_path.name}"
            )
        if inspection.get("valid") is not True:
            raise PortfolioFunnelError(
                f"Stage 0 result inspection is not valid: {inspection_path.name}"
            )
        variant_id = str(inspection.get("variant_id"))
        if variant_id not in variants:
            raise PortfolioFunnelError(
                f"Stage 0 result names an unknown first-wave variant: {variant_id}"
            )
        result_sha256 = str(inspection.get("result_sha256"))
        result_path = _one_path(
            root,
            f"research_results/*-stage0-{result_sha256}.json",
            f"Stage 0 result {result_sha256}",
        )
        result = _read_json(result_path)
        if result.get("result_sha256") != _self_hash(result, "result_sha256"):
            raise PortfolioFunnelError(
                f"Stage 0 result content hash is invalid: {result_path.name}"
            )
        if _file_hash(result_path) != inspection.get("result_file_sha256"):
            raise PortfolioFunnelError(
                f"Stage 0 result file hash drifted: {result_path.name}"
            )
        if result.get("variant_id") != variant_id:
            raise PortfolioFunnelError("Stage 0 result variant identity drifted")
        variant = variants[variant_id]
        for result_field, variant_field in (
            ("strategy_version", "version"),
            ("mechanism_family", "mechanism_family"),
            ("base_rules_hash", "rules_hash"),
        ):
            if result.get(result_field) != variant.get(variant_field):
                raise PortfolioFunnelError(
                    f"Stage 0 result {result_field} drifted for {variant_id}"
                )
        if result.get("claim_scope") != "FALSIFICATION_ONLY":
            raise PortfolioFunnelError("Stage 0 result claim scope drifted")
        if result.get("maturity_effect") != "NONE":
            raise PortfolioFunnelError("Stage 0 result cannot affect maturity")
        if result.get("development_evidence_eligible") is not False or result.get(
            "confirmation_evidence_eligible"
        ) is not False:
            raise PortfolioFunnelError("Stage 0 result entered maturity evidence")
        blockers = _stage0_blockers(result, gate)
        survived = not blockers
        if result.get("stage0_blockers") != blockers:
            raise PortfolioFunnelError("Stage 0 result blockers do not rebuild")
        if result.get("stage0_survived") is not survived:
            raise PortfolioFunnelError("Stage 0 disposition does not rebuild")
        if inspection.get("stage0_survived") is not survived:
            raise PortfolioFunnelError("Stage 0 inspection disposition drifted")
        if inspection.get("maturity_effect") != "NONE":
            raise PortfolioFunnelError("Stage 0 inspection cannot affect maturity")
        dispositions.append(
            {
                "variant_id": variant_id,
                "variant_ordinal": int(variant["variant_ordinal"]),
                "mechanism_family": str(variant["mechanism_family"]),
                "rules_hash": str(variant["rules_hash"]),
                "stage0_result_sha256": result_sha256,
                "result_path": str(result_path.relative_to(root)),
                "result_inspection_path": str(inspection_path.relative_to(root)),
                "closed_signals": int(result["denominator"]["closed_signals"]),
                "status": "SURVIVED" if survived else "RETIRED",
                "blockers": blockers,
            }
        )
    by_disposition = {item["variant_id"]: item for item in dispositions}
    if len(by_disposition) != len(dispositions):
        raise PortfolioFunnelError("a first-wave variant has multiple dispositions")
    ordered_ids = [item for item in FIRST_WAVE_ORDER if item in by_disposition]
    if ordered_ids != list(FIRST_WAVE_ORDER[: len(ordered_ids)]):
        raise PortfolioFunnelError("Stage 0 dispositions skipped the frozen queue")
    return [by_disposition[item] for item in ordered_ids]


def validate_stage0_survivor_binding(
    inspection: Mapping[str, Any], *, root: Path = PROJECT_ROOT
) -> None:
    """Prove a maturity inspection descends from an actual Stage 0 survivor."""

    variant_id = inspection.get("source_stage0_variant_id")
    result_sha256 = inspection.get("source_stage0_result_sha256")
    if not isinstance(variant_id, str) or not variant_id:
        raise PortfolioFunnelError("source_stage0_variant_id is required")
    if not isinstance(result_sha256, str) or len(result_sha256) != 64:
        raise PortfolioFunnelError("source_stage0_result_sha256 is required")
    evidence = inspection.get("evidence_hashes")
    if not isinstance(evidence, Mapping):
        raise PortfolioFunnelError("maturity inspection evidence hashes are missing")
    supplied_paths = (
        inspection.get("source_stage0_result_path"),
        inspection.get("source_stage0_result_inspection_path"),
    )
    resolved: list[Path] = []
    for field, supplied in zip(
        ("source_stage0_result_path", "source_stage0_result_inspection_path"),
        supplied_paths,
        strict=True,
    ):
        if not isinstance(supplied, str) or not supplied:
            raise PortfolioFunnelError(f"{field} is required")
        relative = Path(supplied)
        if relative.is_absolute() or ".." in relative.parts:
            raise PortfolioFunnelError(f"{field} is unsafe")
        if supplied not in evidence:
            raise PortfolioFunnelError(f"{field} must be included in evidence_hashes")
        resolved.append(root / relative)
    result_path, result_inspection_path = resolved
    result = _read_json(result_path)
    result_inspection = _read_json(result_inspection_path)
    if result.get("result_sha256") != _self_hash(result, "result_sha256"):
        raise PortfolioFunnelError("source Stage 0 result content hash is invalid")
    if result.get("result_sha256") != result_sha256:
        raise PortfolioFunnelError("source Stage 0 result identity drifted")
    if result.get("variant_id") != variant_id:
        raise PortfolioFunnelError("source Stage 0 variant identity drifted")
    if result.get("mechanism_family") != inspection.get("mechanism_family"):
        raise PortfolioFunnelError("source Stage 0 mechanism family drifted")
    if result.get("stage0_survived") is not True:
        raise PortfolioFunnelError("retired Stage 0 variants cannot enter maturity")
    if result.get("stage0_blockers") != []:
        raise PortfolioFunnelError("Stage 0 survivor has disposition blockers")
    if result.get("maturity_effect") != "NONE":
        raise PortfolioFunnelError("Stage 0 result cannot directly affect maturity")
    if result.get("development_evidence_eligible") is not False or result.get(
        "confirmation_evidence_eligible"
    ) is not False:
        raise PortfolioFunnelError("Stage 0 records cannot become maturity samples")
    if result_inspection.get("inspection_sha256") != _self_hash(
        result_inspection, "inspection_sha256"
    ):
        raise PortfolioFunnelError("source Stage 0 result inspection hash is invalid")
    required_inspection = {
        "inspection_kind": "stage0-result-inspection",
        "variant_id": variant_id,
        "result_sha256": result_sha256,
        "result_file_sha256": _file_hash(result_path),
        "stage0_survived": True,
        "maturity_effect": "NONE",
        "valid": True,
    }
    for field, expected in required_inspection.items():
        if result_inspection.get(field) != expected:
            raise PortfolioFunnelError(
                f"source Stage 0 result inspection {field} drifted"
            )
    published_slates = sorted(
        root.glob("strategy_tournament/manifests/portfolio-stage0-slate-*.json")
    )
    if variant_id in FIRST_WAVE_ORDER and published_slates:
        matches = [
            item
            for item in load_stage0_dispositions(root)
            if item["variant_id"] == variant_id
            and item["stage0_result_sha256"] == result_sha256
        ]
        if len(matches) != 1 or matches[0]["status"] != "SURVIVED":
            raise PortfolioFunnelError(
                "maturity inspection does not bind one published Stage 0 survivor"
            )
    elif inspection.get("tournament_wave") == 1 and published_slates:
        raise PortfolioFunnelError(
            "wave-one maturity inspection names a variant outside the frozen slate"
        )


def _candidate_phase(assessment: Mapping[str, Any]) -> str:
    return str(assessment.get("validation_phase", "DEVELOPMENT"))


def build_funnel_status(
    maturity_report: Mapping[str, Any], *, root: Path = PROJECT_ROOT
) -> dict[str, Any]:
    dispositions = load_stage0_dispositions(root)
    disposed_ids = {item["variant_id"] for item in dispositions}
    _, variants = _load_slate(root)
    queue = [
        {
            "queue_position": index + 1,
            "variant_id": variant_id,
            "mechanism_family": variants[variant_id]["mechanism_family"],
        }
        for index, variant_id in enumerate(
            item for item in FIRST_WAVE_ORDER if item not in disposed_ids
        )
    ]
    survivors = [item for item in dispositions if item["status"] == "SURVIVED"]
    assessments = maturity_report.get("strategies")
    if not isinstance(assessments, list):
        raise PortfolioFunnelError("portfolio maturity report lacks strategies")
    by_source: dict[str, list[Mapping[str, Any]]] = {}
    for assessment in assessments:
        source = assessment.get("source_stage0_variant_id")
        if isinstance(source, str):
            by_source.setdefault(source, []).append(assessment)
    validation_candidates: list[dict[str, Any]] = []
    for survivor in survivors:
        linked = by_source.get(survivor["variant_id"], [])
        if len(linked) > 1:
            raise PortfolioFunnelError(
                f"Stage 0 survivor has multiple active maturity versions: {survivor['variant_id']}"
            )
        if not linked:
            validation_candidates.append(
                {
                    "source_stage0_variant_id": survivor["variant_id"],
                    "mechanism_family": survivor["mechanism_family"],
                    "validation_phase": "DEVELOPMENT",
                    "status": "AWAITING_REPRESENTATIVE_DEVELOPMENT_FREEZE",
                    "blockers": [
                        "representative development version and inspection are missing"
                    ],
                }
            )
            continue
        assessment = linked[0]
        validation_candidates.append(
            {
                "source_stage0_variant_id": survivor["variant_id"],
                "strategy_id": assessment["strategy_id"],
                "strategy_version": assessment["strategy_version"],
                "mechanism_family": assessment["mechanism_family"],
                "validation_phase": _candidate_phase(assessment),
                "maturity": assessment["maturity"],
                "blockers": assessment["current_phase_blockers"],
            }
        )
    development = [
        item
        for item in validation_candidates
        if item["validation_phase"] == "DEVELOPMENT"
    ]
    advanced = [
        item
        for item in validation_candidates
        if item["validation_phase"] in {"CONFIRMATION", "SHADOW_QUALIFICATION"}
    ]
    first_wave_complete = len(dispositions) == len(FIRST_WAVE_ORDER)
    failure_taxonomies = sorted(
        (root / "strategy_tournament" / "second_wave").glob(
            "first-wave-failure-taxonomy-*.json"
        )
    )
    second_wave_required = first_wave_complete and len(survivors) < int(
        maturity_report.get("target_pilot_ready_strategies", 3)
    )
    second_wave_open = second_wave_required and len(failure_taxonomies) == 1
    notification_reasons: list[str] = []
    if dispositions and len(dispositions) % 3 == 0:
        notification_reasons.append("three Stage 0 dispositions completed")
    if dispositions and dispositions[-1]["status"] == "SURVIVED":
        notification_reasons.append("new Stage 0 survivor")
    ready_count = int(maturity_report.get("pilot_ready_strategy_count", 0))
    live_started = int(maturity_report.get("live_started_strategy_count", 0))
    blockers: list[str] = []
    if not survivors:
        blockers.append("no Stage 0 survivor is available for representative development")
    if not development:
        blockers.append("development lane is vacant")
    if not advanced:
        blockers.append("confirmation or shadow lane is vacant")
    if second_wave_required and not second_wave_open:
        blockers.append("first-wave failure taxonomy is required before wave two")
    return {
        "schema_version": SCHEMA_VERSION,
        "valid": True,
        "first_wave": {
            "dispositions": dispositions,
            "disposed_count": len(dispositions),
            "retired_count": sum(item["status"] == "RETIRED" for item in dispositions),
            "survivor_count": len(survivors),
            "candidate_queue": queue,
            "complete": first_wave_complete,
        },
        "second_wave": {
            "required": second_wave_required,
            "open": second_wave_open,
            "failure_taxonomy_paths": [
                str(path.relative_to(root)) for path in failure_taxonomies
            ],
            "candidate_queue": [
                {"variant_id": variant_id, "mechanism_family": family}
                for variant_id, family in SECOND_WAVE
            ],
        },
        "lanes": {
            "stage0_falsification": queue[0] if queue else None,
            "representative_development": development[0] if development else None,
            "confirmation_or_shadow": advanced[0] if advanced else None,
        },
        "validation_candidates": validation_candidates,
        "progress": {
            "pilot_ready": ready_count,
            "live_started": live_started,
            "target": int(maturity_report.get("target_pilot_ready_strategies", 3)),
        },
        "notification_due": bool(notification_reasons),
        "notification_reasons": notification_reasons,
        "blockers": blockers,
    }


def audit_funnel(
    maturity_report: Mapping[str, Any], *, root: Path = PROJECT_ROOT
) -> dict[str, Any]:
    status = build_funnel_status(maturity_report, root=root)
    return {
        "valid": True,
        "disposed_stage0_variants": status["first_wave"]["disposed_count"],
        "retired_stage0_variants": status["first_wave"]["retired_count"],
        "stage0_survivors": status["first_wave"]["survivor_count"],
        "pilot_ready_progress": status["progress"],
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("status", "audit"))
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        from portfolio_maturity import build_report

        report = build_report()
        result = (
            build_funnel_status(report)
            if args.command == "status"
            else audit_funnel(report)
        )
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    except (OSError, PortfolioFunnelError, ValueError) as exc:
        print(
            json.dumps({"error": str(exc), "error_type": type(exc).__name__}, indent=2),
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

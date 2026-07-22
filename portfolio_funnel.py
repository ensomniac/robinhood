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

FAILURE_CATEGORY_RULES = (
    ("insufficient_signal_capacity", "closed signals are below the Stage 0 minimum"),
    ("nonpositive_expectancy", "primary expectancy is not positive"),
    ("weak_profit_factor", "primary profit factor is below the Stage 0 minimum"),
    ("excessive_drawdown", "primary drawdown exceeds the Stage 0 maximum"),
    ("cost_fragility", "20 bps-per-side total R is not positive"),
    ("rule_violations", "Stage 0 rule violations are not zero"),
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
    if int(denominator.get("closed_signals", -1)) < int(gate["minimum_closed_signals"]):
        blockers.append("closed signals are below the Stage 0 minimum")
    expectancy = primary.get("expectancy_r")
    if (
        not isinstance(expectancy, (int, float))
        or isinstance(expectancy, bool)
        or expectancy <= 0
    ):
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
    if int(denominator.get("rule_violations", -1)) != int(gate["rule_violations"]):
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
        if (
            result.get("development_evidence_eligible") is not False
            or result.get("confirmation_evidence_eligible") is not False
        ):
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
    if (
        result.get("development_evidence_eligible") is not False
        or result.get("confirmation_evidence_eligible") is not False
    ):
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
    wave = inspection.get("tournament_wave")
    published_slates = sorted(
        root.glob("strategy_tournament/manifests/portfolio-stage0-slate-*.json")
    )
    if wave == 1 and variant_id in FIRST_WAVE_ORDER and published_slates:
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
    elif wave == 1 and published_slates:
        raise PortfolioFunnelError(
            "wave-one maturity inspection names a variant outside the frozen slate"
        )
    elif wave == 2:
        published_second_wave_slates = sorted(
            root.glob(
                "strategy_tournament/second_wave/manifests/"
                "portfolio-stage0-second-wave-slate-*.json"
            )
        )
        if not published_second_wave_slates:
            raise PortfolioFunnelError(
                "wave-two maturity inspection has no frozen second-wave slate"
            )
        matches = [
            item
            for item in load_second_wave_dispositions(root)
            if item["variant_id"] == variant_id
            and item["stage0_result_sha256"] == result_sha256
        ]
        if len(matches) != 1 or matches[0]["status"] != "SURVIVED":
            raise PortfolioFunnelError(
                "maturity inspection does not bind one published wave-two survivor"
            )


def _candidate_phase(assessment: Mapping[str, Any]) -> str:
    return str(assessment.get("validation_phase", "DEVELOPMENT"))


def _development_failure(
    maturity_report: Mapping[str, Any], *, root: Path
) -> dict[str, Any]:
    assessments = maturity_report.get("strategies")
    if not isinstance(assessments, list):
        raise PortfolioFunnelError("portfolio maturity report lacks strategies")
    matches = [
        item
        for item in assessments
        if item.get("source_stage0_variant_id") == "equity-gap-continuation-v1"
    ]
    if len(matches) != 1 or matches[0].get("retired_after_development") is not True:
        raise PortfolioFunnelError(
            "first-wave taxonomy requires the inspected development retirement"
        )
    assessment = matches[0]
    metrics = assessment.get("metrics")
    development = metrics.get("development") if isinstance(metrics, Mapping) else None
    if not isinstance(development, Mapping):
        raise PortfolioFunnelError("development failure metrics are missing")
    ledger_path = root / "PORTFOLIO_SIGNALS.jsonl"
    try:
        records = [
            json.loads(line)
            for line in ledger_path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
    except (OSError, json.JSONDecodeError) as exc:
        raise PortfolioFunnelError(f"cannot read {ledger_path}: {exc}") from exc
    inspections = [
        item
        for item in records
        if item.get("record_type") == "inspection"
        and item.get("source_stage0_variant_id") == "equity-gap-continuation-v1"
        and item.get("retired_after_development") is True
    ]
    if len(inspections) != 1:
        raise PortfolioFunnelError(
            "first-wave taxonomy requires one development inspection ledger record"
        )
    evidence = inspections[0].get("evidence_hashes")
    if not isinstance(evidence, Mapping):
        raise PortfolioFunnelError("development inspection evidence hashes are missing")
    result_paths = [
        path
        for path in evidence
        if path.startswith("research_results/") and "-development-" in path
    ]
    inspection_paths = [
        path
        for path in evidence
        if "strategy_validation/" in path and "-development-result-" in path
    ]
    if len(result_paths) != 1 or len(inspection_paths) != 1:
        raise PortfolioFunnelError(
            "development result and result-inspection evidence are ambiguous"
        )
    for path in (*result_paths, *inspection_paths):
        if _file_hash(root / path) != evidence[path]:
            raise PortfolioFunnelError(f"development evidence hash drifted: {path}")
    return {
        "source_stage0_variant_id": "equity-gap-continuation-v1",
        "strategy_id": str(assessment["strategy_id"]),
        "strategy_version": str(assessment["strategy_version"]),
        "status": "RETIRED_DEVELOPMENT",
        "closed_signals": int(development["signals"]),
        "expectancy_r": development["expectancy_r"],
        "profit_factor": development["profit_factor"],
        "bootstrap_lower_expectancy_r": development["bootstrap_lower_expectancy_r"],
        "maximum_drawdown_r": development["maximum_drawdown_r"],
        "phase_blockers": list(assessment["current_phase_blockers"]),
        "development_result_path": result_paths[0],
        "development_result_file_sha256": str(evidence[result_paths[0]]),
        "development_result_inspection_path": inspection_paths[0],
        "development_result_inspection_file_sha256": str(evidence[inspection_paths[0]]),
    }


def build_failure_taxonomy(
    maturity_report: Mapping[str, Any], *, root: Path = PROJECT_ROOT
) -> dict[str, Any]:
    """Rebuild the immutable first-wave failure taxonomy from inspected evidence."""

    dispositions = load_stage0_dispositions(root)
    if len(dispositions) != len(FIRST_WAVE_ORDER):
        raise PortfolioFunnelError("first wave is incomplete")
    development_failure = _development_failure(maturity_report, root=root)
    slate_path = _one_path(
        root,
        "strategy_tournament/manifests/portfolio-stage0-slate-*.json",
        "first-wave slate",
    )
    slate = _read_json(slate_path)
    disposition_records: list[dict[str, Any]] = []
    evidence_hashes = {str(slate_path.relative_to(root)): _file_hash(slate_path)}
    for disposition in dispositions:
        inspection = _read_json(root / disposition["result_inspection_path"])
        result_path = str(disposition["result_path"])
        inspection_path = str(disposition["result_inspection_path"])
        evidence_hashes[result_path] = _file_hash(root / result_path)
        evidence_hashes[inspection_path] = _file_hash(root / inspection_path)
        disposition_records.append(
            {
                "variant_id": disposition["variant_id"],
                "mechanism_family": disposition["mechanism_family"],
                "status": disposition["status"],
                "closed_signals": disposition["closed_signals"],
                "rules_hash": disposition["rules_hash"],
                "stage0_result_sha256": disposition["stage0_result_sha256"],
                "result_inspection_sha256": inspection["inspection_sha256"],
                "blockers": disposition["blockers"],
            }
        )
    evidence_hashes[development_failure["development_result_path"]] = (
        development_failure["development_result_file_sha256"]
    )
    evidence_hashes[development_failure["development_result_inspection_path"]] = (
        development_failure["development_result_inspection_file_sha256"]
    )
    failure_categories = []
    for category, blocker in FAILURE_CATEGORY_RULES:
        variants = [
            item["variant_id"] for item in dispositions if blocker in item["blockers"]
        ]
        if variants:
            failure_categories.append(
                {
                    "category": category,
                    "gate": blocker,
                    "variant_count": len(variants),
                    "variant_ids": variants,
                }
            )
    taxonomy: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "artifact_kind": "first-wave-failure-taxonomy",
        "campaign_id": "multi-strategy-portfolio-validation-v1",
        "claim_scope": "FIRST_WAVE_FAILURE_CLASSIFICATION_ONLY",
        "first_wave_slate_manifest_sha256": slate["manifest_sha256"],
        "first_wave_complete": True,
        "stage0_disposed_count": len(dispositions),
        "stage0_retired_count": sum(
            item["status"] == "RETIRED" for item in dispositions
        ),
        "stage0_survivor_count": sum(
            item["status"] == "SURVIVED" for item in dispositions
        ),
        "stage0_dispositions": disposition_records,
        "stage0_failure_categories": failure_categories,
        "development_dispositions": [development_failure],
        "first_wave_active_candidates": 0,
        "failure_summary": [
            "Most first-wave exact variants failed edge, drawdown, or 20-bps cost gates.",
            "Four exact variants were capacity-limited on their frozen screening corpora.",
            "The sole Stage 0 survivor failed representative development across edge, bootstrap, drawdown, half-sample, best-trade removal, and cost-stress gates.",
        ],
        "second_wave_rationale": [
            "Use highly liquid ETF rotation and trend mechanisms to reduce dependence on sparse single-name catalyst corpora.",
            "Test overnight, multi-day reversal, structural momentum, and calendar mechanisms that are distinct from the failed intraday breakout and mean-reversion variants.",
            "Treat every second-wave entry as a new exact frozen variant; no first-wave parameter repair or maturity inheritance is permitted.",
        ],
        "second_wave_queue": [
            {"variant_id": variant_id, "mechanism_family": family}
            for variant_id, family in SECOND_WAVE
        ],
        "second_wave_rules_frozen": False,
        "second_wave_outcome_access_authorized": False,
        "maturity_effect": "NONE",
        "provider_requests": 0,
        "broker_actions": 0,
        "evidence_hashes": dict(sorted(evidence_hashes.items())),
    }
    taxonomy["taxonomy_sha256"] = _self_hash(taxonomy, "taxonomy_sha256")
    return taxonomy


def build_failure_taxonomy_inspection(
    taxonomy_path: Path,
    maturity_report: Mapping[str, Any],
    *,
    root: Path = PROJECT_ROOT,
) -> dict[str, Any]:
    """Independently rebuild a published taxonomy without outcome computation."""

    taxonomy = _read_json(taxonomy_path)
    expected = build_failure_taxonomy(maturity_report, root=root)
    if taxonomy != expected:
        raise PortfolioFunnelError("first-wave failure taxonomy does not rebuild")
    inspection: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "inspection_kind": "first-wave-failure-taxonomy-inspection",
        "taxonomy_sha256": taxonomy["taxonomy_sha256"],
        "taxonomy_file_sha256": _file_hash(taxonomy_path),
        "stage0_disposed_count": taxonomy["stage0_disposed_count"],
        "stage0_retired_count": taxonomy["stage0_retired_count"],
        "stage0_survivor_count": taxonomy["stage0_survivor_count"],
        "retired_development_count": len(taxonomy["development_dispositions"]),
        "evidence_files_verified": len(taxonomy["evidence_hashes"]),
        "provider_requests": 0,
        "broker_actions": 0,
        "returns_computed": 0,
        "maturity_effect": "NONE",
        "valid": True,
    }
    inspection["inspection_sha256"] = _self_hash(inspection, "inspection_sha256")
    return inspection


def load_failure_taxonomy_status(
    maturity_report: Mapping[str, Any], *, root: Path = PROJECT_ROOT
) -> dict[str, Any]:
    taxonomy_paths = sorted(
        (root / "strategy_tournament" / "second_wave").glob(
            "first-wave-failure-taxonomy-*.json"
        )
    )
    if len(taxonomy_paths) > 1:
        raise PortfolioFunnelError("multiple first-wave failure taxonomies exist")
    inspection_paths = sorted(
        (root / "strategy_tournament" / "second_wave" / "inspections").glob(
            "first-wave-failure-taxonomy-*.json"
        )
    )
    if len(inspection_paths) > 1:
        raise PortfolioFunnelError("multiple failure-taxonomy inspections exist")
    if not taxonomy_paths:
        if inspection_paths:
            raise PortfolioFunnelError("failure-taxonomy inspection has no artifact")
        return {"taxonomy_paths": [], "inspection_paths": [], "inspected": False}
    taxonomy_path = taxonomy_paths[0]
    taxonomy = _read_json(taxonomy_path)
    expected = build_failure_taxonomy(maturity_report, root=root)
    if taxonomy != expected:
        raise PortfolioFunnelError("first-wave failure taxonomy does not rebuild")
    if not inspection_paths:
        return {
            "taxonomy_paths": [str(taxonomy_path.relative_to(root))],
            "inspection_paths": [],
            "inspected": False,
        }
    inspection_path = inspection_paths[0]
    inspection = _read_json(inspection_path)
    expected_inspection = build_failure_taxonomy_inspection(
        taxonomy_path, maturity_report, root=root
    )
    if inspection != expected_inspection:
        raise PortfolioFunnelError("first-wave failure-taxonomy inspection drifted")
    return {
        "taxonomy_paths": [str(taxonomy_path.relative_to(root))],
        "inspection_paths": [str(inspection_path.relative_to(root))],
        "inspected": True,
    }


def load_second_wave_slate_status(*, root: Path = PROJECT_ROOT) -> dict[str, Any]:
    manifest_paths = sorted(
        (root / "strategy_tournament" / "second_wave" / "manifests").glob(
            "portfolio-stage0-second-wave-slate-*.json"
        )
    )
    if len(manifest_paths) > 1:
        raise PortfolioFunnelError("multiple second-wave Stage 0 slates exist")
    inspection_paths = sorted(
        (root / "strategy_tournament" / "second_wave" / "inspections").glob(
            "portfolio-stage0-second-wave-slate-*.json"
        )
    )
    if len(inspection_paths) > 1:
        raise PortfolioFunnelError("multiple second-wave slate inspections exist")
    if not manifest_paths:
        if inspection_paths:
            raise PortfolioFunnelError("second-wave slate inspection has no manifest")
        return {"manifest_paths": [], "inspection_paths": [], "inspected": False}
    manifest_path = manifest_paths[0]
    manifest = _read_json(manifest_path)
    if manifest.get("manifest_sha256") != _self_hash(manifest, "manifest_sha256"):
        raise PortfolioFunnelError("second-wave slate content hash is invalid")
    if manifest.get("manifest_kind") != "portfolio-stage0-second-wave-slate":
        raise PortfolioFunnelError("second-wave slate kind drifted")
    if manifest.get("ordered_variant_ids") != [item[0] for item in SECOND_WAVE]:
        raise PortfolioFunnelError("second-wave slate order drifted")
    variants = manifest.get("variants")
    if not isinstance(variants, list) or [
        (item.get("variant_id"), item.get("mechanism_family")) for item in variants
    ] != list(SECOND_WAVE):
        raise PortfolioFunnelError("second-wave slate variants drifted")
    if manifest.get("maturity_effect") != "NONE":
        raise PortfolioFunnelError("second-wave slate cannot affect maturity")
    if manifest.get("return_evaluation_authorized_before_inspection") is not False:
        raise PortfolioFunnelError("second-wave slate bypasses input inspection")
    if not inspection_paths:
        return {
            "manifest_paths": [str(manifest_path.relative_to(root))],
            "inspection_paths": [],
            "inspected": False,
        }
    inspection_path = inspection_paths[0]
    inspection = _read_json(inspection_path)
    if inspection.get("inspection_sha256") != _self_hash(
        inspection, "inspection_sha256"
    ):
        raise PortfolioFunnelError("second-wave slate inspection hash is invalid")
    required = {
        "inspection_kind": "portfolio-stage0-second-wave-slate-inspection",
        "manifest_sha256": manifest["manifest_sha256"],
        "manifest_file_sha256": _file_hash(manifest_path),
        "variant_count": len(SECOND_WAVE),
        "ordered_variant_ids": manifest["ordered_variant_ids"],
        "outcomes_accessed": 0,
        "returns_computed": 0,
        "provider_requests": 0,
        "broker_actions": 0,
        "maturity_effect": "NONE",
        "return_evaluation_authorized": True,
        "valid": True,
    }
    for field, expected in required.items():
        if inspection.get(field) != expected:
            raise PortfolioFunnelError(f"second-wave slate inspection {field} drifted")
    return {
        "manifest_paths": [str(manifest_path.relative_to(root))],
        "inspection_paths": [str(inspection_path.relative_to(root))],
        "inspected": True,
    }


def load_second_wave_dispositions(root: Path = PROJECT_ROOT) -> list[dict[str, Any]]:
    """Return independently checked, ordered second-wave Stage 0 dispositions."""

    slate_status = load_second_wave_slate_status(root=root)
    if not slate_status["inspected"]:
        return []
    manifest_path = root / slate_status["manifest_paths"][0]
    manifest = _read_json(manifest_path)
    gate = manifest.get("stage0_falsification")
    variants_value = manifest.get("variants")
    if not isinstance(gate, Mapping) or not isinstance(variants_value, list):
        raise PortfolioFunnelError("second-wave Stage 0 contract is incomplete")
    variants = {str(item.get("variant_id")): dict(item) for item in variants_value}
    dispositions: list[dict[str, Any]] = []
    inspection_dir = root / "strategy_tournament" / "second_wave" / "inspections"
    for inspection_path in sorted(inspection_dir.glob("*-result-*.json")):
        inspection = _read_json(inspection_path)
        if inspection.get("inspection_kind") != "stage0-result-inspection":
            continue
        if inspection.get("inspection_sha256") != _self_hash(
            inspection, "inspection_sha256"
        ):
            raise PortfolioFunnelError(
                f"second-wave result inspection hash is invalid: {inspection_path.name}"
            )
        if inspection.get("valid") is not True:
            raise PortfolioFunnelError(
                f"second-wave result inspection is not valid: {inspection_path.name}"
            )
        variant_id = str(inspection.get("variant_id"))
        if variant_id not in variants:
            raise PortfolioFunnelError(
                f"second-wave result names an unknown variant: {variant_id}"
            )
        result_sha256 = str(inspection.get("result_sha256"))
        result_path = _one_path(
            root,
            f"research_results/*-stage0-{result_sha256}.json",
            f"second-wave Stage 0 result {result_sha256}",
        )
        result = _read_json(result_path)
        if result.get("result_sha256") != _self_hash(result, "result_sha256"):
            raise PortfolioFunnelError(
                f"second-wave result content hash is invalid: {result_path.name}"
            )
        if _file_hash(result_path) != inspection.get("result_file_sha256"):
            raise PortfolioFunnelError(
                f"second-wave result file hash drifted: {result_path.name}"
            )
        variant = variants[variant_id]
        required_result = {
            "variant_id": variant_id,
            "strategy_version": variant.get("version"),
            "mechanism_family": variant.get("mechanism_family"),
            "base_rules_hash": variant.get("rules_hash"),
            "claim_scope": "FALSIFICATION_ONLY",
            "maturity_effect": "NONE",
            "development_evidence_eligible": False,
            "confirmation_evidence_eligible": False,
        }
        for field, expected in required_result.items():
            if result.get(field) != expected:
                raise PortfolioFunnelError(
                    f"second-wave Stage 0 result {field} drifted for {variant_id}"
                )
        blockers = _stage0_blockers(result, gate)
        survived = not blockers
        required_inspection = {
            "variant_id": variant_id,
            "result_sha256": result_sha256,
            "stage0_survived": survived,
            "maturity_effect": "NONE",
            "valid": True,
        }
        for field, expected in required_inspection.items():
            if inspection.get(field) != expected:
                raise PortfolioFunnelError(
                    f"second-wave Stage 0 inspection {field} drifted"
                )
        if result.get("stage0_blockers") != blockers:
            raise PortfolioFunnelError("second-wave Stage 0 blockers do not rebuild")
        if result.get("stage0_survived") is not survived:
            raise PortfolioFunnelError(
                "second-wave Stage 0 disposition does not rebuild"
            )
        dispositions.append(
            {
                "variant_id": variant_id,
                "variant_ordinal": [item[0] for item in SECOND_WAVE].index(variant_id)
                + 1,
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
        raise PortfolioFunnelError("a second-wave variant has multiple dispositions")
    frozen_order = [item[0] for item in SECOND_WAVE]
    ordered_ids = [item for item in frozen_order if item in by_disposition]
    if ordered_ids != frozen_order[: len(ordered_ids)]:
        raise PortfolioFunnelError("second-wave dispositions skipped the frozen queue")
    return [by_disposition[item] for item in ordered_ids]


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
    first_wave_survivors = [
        item for item in dispositions if item["status"] == "SURVIVED"
    ]
    second_wave_dispositions = load_second_wave_dispositions(root)
    second_wave_survivors = [
        item for item in second_wave_dispositions if item["status"] == "SURVIVED"
    ]
    survivors = [*first_wave_survivors, *second_wave_survivors]
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
                "status": (
                    "RETIRED_DEVELOPMENT"
                    if assessment.get("retired_after_development") is True
                    else "ACTIVE_VALIDATION"
                ),
                "blockers": assessment["current_phase_blockers"],
            }
        )
    development = [
        item
        for item in validation_candidates
        if item["validation_phase"] == "DEVELOPMENT"
        and item.get("status") != "RETIRED_DEVELOPMENT"
    ]
    advanced = [
        item
        for item in validation_candidates
        if item["validation_phase"] in {"CONFIRMATION", "SHADOW_QUALIFICATION"}
    ]
    first_wave_complete = len(dispositions) == len(FIRST_WAVE_ORDER)
    taxonomy_status = load_failure_taxonomy_status(maturity_report, root=root)
    second_wave_slate = load_second_wave_slate_status(root=root)
    second_wave_required = first_wave_complete and len(first_wave_survivors) < int(
        maturity_report.get("target_pilot_ready_strategies", 3)
    )
    second_wave_open = second_wave_required and taxonomy_status["inspected"]
    second_wave_outcome_access_open = (
        second_wave_open and second_wave_slate["inspected"]
    )
    second_wave_disposed_ids = {item["variant_id"] for item in second_wave_dispositions}
    second_wave_queue = [
        {
            "queue_position": index + 1,
            "variant_id": variant_id,
            "mechanism_family": family,
        }
        for index, (variant_id, family) in enumerate(
            item for item in SECOND_WAVE if item[0] not in second_wave_disposed_ids
        )
    ]
    stage0_lane = queue[0] if queue else None
    if stage0_lane is None and second_wave_outcome_access_open and second_wave_queue:
        stage0_lane = second_wave_queue[0]
    notification_reasons: list[str] = []
    all_dispositions = [*dispositions, *second_wave_dispositions]
    if all_dispositions and len(all_dispositions) % 3 == 0:
        notification_reasons.append("three Stage 0 dispositions completed")
    if all_dispositions and all_dispositions[-1]["status"] == "SURVIVED":
        notification_reasons.append("new Stage 0 survivor")
    ready_count = int(maturity_report.get("pilot_ready_strategy_count", 0))
    live_started = int(maturity_report.get("live_started_strategy_count", 0))
    blockers: list[str] = []
    if not survivors:
        blockers.append(
            "no Stage 0 survivor is available for representative development"
        )
    if not development:
        blockers.append("development lane is vacant")
    if not advanced:
        blockers.append("confirmation or shadow lane is vacant")
    if second_wave_required and not second_wave_open:
        blockers.append("first-wave failure taxonomy is required before wave two")
    if second_wave_open and not second_wave_outcome_access_open:
        blockers.append("inspected second-wave slate is required before outcome access")
    second_wave_complete = len(second_wave_dispositions) == len(SECOND_WAVE)
    if second_wave_complete and not development and not advanced and ready_count == 0:
        blockers.append(
            "authorized Stage 0 tournament is exhausted without an active survivor"
        )
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
            "outcome_access_open": second_wave_outcome_access_open,
            "failure_taxonomy_paths": [*taxonomy_status["taxonomy_paths"]],
            "failure_taxonomy_inspection_paths": [*taxonomy_status["inspection_paths"]],
            "slate_paths": [*second_wave_slate["manifest_paths"]],
            "slate_inspection_paths": [*second_wave_slate["inspection_paths"]],
            "dispositions": second_wave_dispositions,
            "disposed_count": len(second_wave_dispositions),
            "retired_count": sum(
                item["status"] == "RETIRED" for item in second_wave_dispositions
            ),
            "survivor_count": len(second_wave_survivors),
            "candidate_queue": second_wave_queue,
            "complete": second_wave_complete,
        },
        "lanes": {
            "stage0_falsification": stage0_lane,
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
    disposed = (
        status["first_wave"]["disposed_count"] + status["second_wave"]["disposed_count"]
    )
    retired = (
        status["first_wave"]["retired_count"] + status["second_wave"]["retired_count"]
    )
    survivors = (
        status["first_wave"]["survivor_count"] + status["second_wave"]["survivor_count"]
    )
    return {
        "valid": True,
        "disposed_stage0_variants": disposed,
        "retired_stage0_variants": retired,
        "stage0_survivors": survivors,
        "pilot_ready_progress": status["progress"],
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "command",
        choices=(
            "status",
            "audit",
            "build-failure-taxonomy",
            "inspect-failure-taxonomy",
        ),
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        from portfolio_maturity import build_report

        report = build_report()
        if args.command == "status":
            result = build_funnel_status(report)
        elif args.command == "audit":
            result = audit_funnel(report)
        elif args.command == "build-failure-taxonomy":
            taxonomy = build_failure_taxonomy(report)
            output_path = (
                PROJECT_ROOT
                / "strategy_tournament"
                / "second_wave"
                / f"first-wave-failure-taxonomy-{taxonomy['taxonomy_sha256']}.json"
            )
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text(
                json.dumps(taxonomy, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            result = {
                "taxonomy_sha256": taxonomy["taxonomy_sha256"],
                "written": str(output_path.relative_to(PROJECT_ROOT)),
            }
        else:
            taxonomy_path = _one_path(
                PROJECT_ROOT,
                "strategy_tournament/second_wave/first-wave-failure-taxonomy-*.json",
                "first-wave failure taxonomy",
            )
            inspection = build_failure_taxonomy_inspection(taxonomy_path, report)
            output_path = (
                PROJECT_ROOT
                / "strategy_tournament"
                / "second_wave"
                / "inspections"
                / "first-wave-failure-taxonomy-"
                f"{inspection['inspection_sha256']}.json"
            )
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text(
                json.dumps(inspection, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
            result = {
                "inspection_sha256": inspection["inspection_sha256"],
                "written": str(output_path.relative_to(PROJECT_ROOT)),
            }
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

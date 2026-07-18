"""Run production-aware counterfactual strategy research on frozen local data.

The lab is deliberately research-only. It reuses the immutable evidence and
completed-bar execution contracts from :mod:`historical_research`, evaluates
every bar-executable signal, and then applies versioned one-trade-per-day
policies. It never edits production strategy configuration, ledgers, trade
contexts, maturity state, or broker state.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import random
import re
import statistics
import time
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from historical_research import (
    DEFAULT_DATA_ROOT,
    ExecutionConfig,
    HistoricalResearchError,
    _find_signal_point_in_time,
    _trade_from_decision,
    load_dataset,
)
from historical_research_strategies import (
    CandidateContext,
    ResearchStrategyError,
    SignalDecision,
    StrategyPlugin,
    load_strategy,
    parse_bars,
    plugin_manifest_hash_input,
)
from strategy_engine import (
    EvaluationResult,
    StrategyConfig,
    evaluate_candidate,
    load_config,
)


SCHEMA_VERSION = 1
LAB_VERSION = "2026-07-18-v3"
CONFIRMATION_SCHEMA_VERSION = 1
CONFIRMATION_CONTRACT_ID = "early-item-2.02-reversal-confirmation-v1"
PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_EVIDENCE = (
    PROJECT_ROOT / "historical_batches" / "evidence-2026-07-16-one-hundred-days.json"
)
DEFAULT_OUTPUT_ROOT = PROJECT_ROOT / "research_runs"
DEFAULT_CONFIRMATION_ROOT = (
    PROJECT_ROOT / "historical_batches" / "confirmation_manifests"
)
DEFAULT_EXCLUDED_RESULT = (
    PROJECT_ROOT
    / "research_results"
    / "2026-07-18-production-aware-strategy-lab.json"
)
PRODUCTION_PATHS = (
    PROJECT_ROOT / "strategy_config.toml",
    PROJECT_ROOT / "SIGNALS.jsonl",
    PROJECT_ROOT / "TRADES.md",
    PROJECT_ROOT / "trades",
)
PROTECTED_PUBLISH_PATHS = PRODUCTION_PATHS + (
    PROJECT_ROOT / "progress" / "HISTORY.jsonl",
    PROJECT_ROOT / "AGENTS.md",
)
SCORE_REASON = re.compile(r"^score \d+ is below the \d+-point maturity gate$")
STALE_REASON = re.compile(r"^snapshot \d+ is stale$")
CROSSED_REASON = re.compile(r"^snapshot \d+ is crossed$")
DATE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")


class HistoricalStrategyLabError(RuntimeError):
    """Raised when the lab cannot preserve its research-fidelity boundary."""


@dataclass(frozen=True, slots=True)
class Policy:
    """One predeclared one-trade-per-day counterfactual selection policy."""

    policy_id: str
    strategy_spec: str
    description: str
    version: str = "1.0.0"
    signal_cutoff_et: str | None = None
    require_earnings_2_02: bool = False
    maximum_stop_fraction: float | None = None
    selection: str = "strength"
    require_production_eligible: bool = False

    def validate(self) -> None:
        if not self.policy_id or not self.description or not self.strategy_spec:
            raise HistoricalStrategyLabError("policy identity fields cannot be empty")
        if self.signal_cutoff_et is not None and not (
            "09:35:00" < self.signal_cutoff_et <= "10:30:00"
        ):
            raise HistoricalStrategyLabError(
                f"{self.policy_id}: signal cutoff must be after 09:35 and at or before 10:30"
            )
        if self.maximum_stop_fraction is not None and not (
            math.isfinite(self.maximum_stop_fraction) and self.maximum_stop_fraction > 0
        ):
            raise HistoricalStrategyLabError(
                f"{self.policy_id}: maximum stop fraction must be positive"
            )
        if self.selection not in {"strength", "production_score", "opening_rvol"}:
            raise HistoricalStrategyLabError(
                f"{self.policy_id}: unsupported selection {self.selection!r}"
            )


BUILTIN_POLICIES: dict[str, Policy] = {
    policy.policy_id: policy
    for policy in (
        Policy(
            "orb-production-stack",
            "orb-5m-research",
            "Research ORB signal requiring every current production gate.",
            require_production_eligible=True,
        ),
        Policy(
            "orb-all-strength",
            "orb-5m-research",
            "All-window ORB counterfactual; earliest signal then strength.",
        ),
        Policy(
            "orb-early-strength",
            "orb-5m-research",
            "ORB signal-bar start before 09:40; earliest signal then strength.",
            signal_cutoff_et="09:40:00",
        ),
        Policy(
            "orb-early-score",
            "orb-5m-research",
            "Early ORB signal-bar start; production score ranks simultaneous signals.",
            signal_cutoff_et="09:40:00",
            selection="production_score",
        ),
        Policy(
            "orb-early-rvol",
            "orb-5m-research",
            "Early ORB signal-bar start; opening-relative-volume ranks simultaneous signals.",
            signal_cutoff_et="09:40:00",
            selection="opening_rvol",
        ),
        Policy(
            "orb-early-earnings",
            "orb-5m-research",
            "Early ORB restricted to SEC item 2.02 earnings catalysts.",
            signal_cutoff_et="09:40:00",
            require_earnings_2_02=True,
        ),
        Policy(
            "orb-early-stop-0.8pct",
            "orb-5m-research",
            "Early ORB enforcing the frozen production 0.8% maximum stop.",
            signal_cutoff_et="09:40:00",
            maximum_stop_fraction=0.008,
        ),
        Policy(
            "reversal-all-strength",
            "opening-reversal",
            "All-window opening reversal; earliest signal then strength.",
        ),
        Policy(
            "reversal-early-strength",
            "opening-reversal",
            "Opening reversal signal-bar start before 09:40; earliest signal then strength.",
            signal_cutoff_et="09:40:00",
        ),
        Policy(
            "reversal-early-score",
            "opening-reversal",
            "Early opening reversal; production score ranks simultaneous signals.",
            signal_cutoff_et="09:40:00",
            selection="production_score",
        ),
        Policy(
            "reversal-early-rvol",
            "opening-reversal",
            "Early opening reversal; opening-relative-volume rank orders simultaneous signals.",
            signal_cutoff_et="09:40:00",
            selection="opening_rvol",
        ),
        Policy(
            "reversal-early-earnings",
            "opening-reversal",
            "Early opening reversal restricted to SEC item 2.02 earnings catalysts.",
            signal_cutoff_et="09:40:00",
            require_earnings_2_02=True,
        ),
        Policy(
            "reversal-early-stop-0.8pct",
            "opening-reversal",
            "Early opening reversal enforcing the frozen production 0.8% maximum stop.",
            signal_cutoff_et="09:40:00",
            maximum_stop_fraction=0.008,
        ),
        Policy(
            "vwap-pullback-all",
            "vwap-pullback",
            "Existing VWAP-pullback research definition for deletion evidence.",
        ),
        Policy(
            "hod-continuation-all",
            "hod-continuation",
            "Existing high-of-day continuation definition for deletion evidence.",
        ),
    )
}

# This is the only policy the prospective confirmation workflow may execute.
# It is deliberately a separate constant so a caller cannot supply a policy
# grid or opportunistically substitute another selector after outcomes exist.
FROZEN_CONFIRMATION_POLICY = BUILTIN_POLICIES["reversal-early-earnings"]
CONFIRMATION_EXECUTION_GRID = {
    "slippage_bps_per_side": [5.0, 10.0, 20.0],
    "target_r": [1.0, 1.5, 2.0, 3.0],
    "primary": {"slippage_bps_per_side": 5.0, "target_r": 2.0},
    "force_flat_time_et": "15:50:00",
    "same_bar_ambiguity": "stop_first",
}
CONFIRMATION_ACCEPTANCE_THRESHOLDS = {
    "minimum_requested_dates": 100,
    "minimum_validation_grade_dates": 80,
    "minimum_executed_signals": 50,
    "primary_minimum_profit_factor": 1.30,
    "stress_minimum_profit_factor": 1.20,
    "maximum_drawdown_r": 6.0,
    "bootstrap_one_sided_confidence": 0.90,
    "chronological_halves_must_be_positive": True,
    "total_without_best_five_must_be_positive": True,
    "all_frozen_targets_must_be_positive": True,
}
CONFIRMATION_DEPLOYMENT = {
    "synthetic_starting_equity": 100_000.0,
    "synthetic_buying_power_fraction": 1.0,
    "account_risk_fraction": 0.0025,
    "stop_slippage_reserve_fraction": 0.001,
    "allocation_cap_fraction": 0.80,
    "allocation_objective_floor_fraction": 0.70,
    "production_compatible_stop_fraction": 0.008,
    "minimum_production_compatible_trades_for_inference": 20,
    "whole_shares_only": True,
    "structural_stops_must_not_be_compressed": True,
}


@dataclass(frozen=True, slots=True)
class Observation:
    """A point-in-time signal decision plus facts known to the lab policy."""

    date: str
    symbol: str
    signal_id: str
    strategy_id: str
    context: CandidateContext
    decision: SignalDecision | None
    no_signal_reason: str
    production: EvaluationResult
    earnings_2_02: bool
    production_evaluation_time_et: str


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


def _tree_hash(path: Path) -> str | None:
    if not path.exists():
        return None
    if path.is_file():
        return _sha256_file(path)
    rows = [
        {
            "path": str(value.relative_to(path)),
            "sha256": _sha256_file(value),
        }
        for value in sorted(path.rglob("*"))
        if value.is_file()
    ]
    return _sha256_bytes(_canonical_json(rows))


def production_hashes() -> dict[str, str | None]:
    return {
        str(path.relative_to(PROJECT_ROOT)): _tree_hash(path)
        for path in PRODUCTION_PATHS
    }


def _safe_publish_prefix(prefix: Path) -> Path:
    resolved = prefix.resolve()
    for protected in PROTECTED_PUBLISH_PATHS:
        protected = protected.resolve()
        if resolved == protected or protected in resolved.parents:
            raise HistoricalStrategyLabError(
                f"strategy lab output cannot target production artifact {protected}"
            )
    return resolved


def _atomic_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(text, encoding="utf-8")
    os.replace(temporary, path)


def _atomic_json(path: Path, value: Any) -> None:
    _atomic_text(path, json.dumps(value, indent=2, sort_keys=True) + "\n")


def _read_json_object(path: Path, label: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise HistoricalStrategyLabError(f"cannot read {label} {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise HistoricalStrategyLabError(f"{label} must be a JSON object")
    return value


def _parse_timestamp(value: Any, label: str) -> datetime:
    if not isinstance(value, str):
        raise HistoricalStrategyLabError(f"{label} must be an ISO timestamp")
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError as exc:
        raise HistoricalStrategyLabError(f"{label} must be an ISO timestamp") from exc
    if parsed.tzinfo is None:
        raise HistoricalStrategyLabError(f"{label} must include a timezone")
    return parsed


def _display_path(path: Path) -> str:
    resolved = path.resolve()
    try:
        return str(resolved.relative_to(PROJECT_ROOT))
    except ValueError:
        return str(resolved)


def _resolve_recorded_path(value: Any, *, relative_to: Path) -> Path:
    path = Path(str(value))
    if path.is_absolute():
        return path
    project_path = PROJECT_ROOT / path
    if project_path.exists():
        return project_path
    return relative_to / path


def _candidate_symbols(rows: Any, day: str) -> tuple[str, ...]:
    if not isinstance(rows, list) or not rows:
        raise HistoricalStrategyLabError(
            f"{day}: frozen candidate universe must be a non-empty array"
        )
    symbols: list[str] = []
    for index, row in enumerate(rows, 1):
        if not isinstance(row, Mapping):
            raise HistoricalStrategyLabError(
                f"{day}: candidate {index} must be an object"
            )
        symbol = str(row.get("symbol", "")).strip().upper()
        if not symbol:
            raise HistoricalStrategyLabError(
                f"{day}: candidate {index} has no symbol"
            )
        catalyst = row.get("catalyst")
        discovery = row.get("discovery")
        if (
            not isinstance(catalyst, Mapping)
            or catalyst.get("point_in_time") is not True
            or not str(catalyst.get("source_url", "")).strip()
        ):
            raise HistoricalStrategyLabError(
                f"{day} {symbol}: point-in-time catalyst evidence is incomplete"
            )
        _parse_timestamp(
            catalyst.get("published_at"), f"{day} {symbol} catalyst.published_at"
        )
        symbols.append(symbol)
    if len(set(symbols)) != len(symbols):
        raise HistoricalStrategyLabError(f"{day}: candidate symbols must be unique")
    return tuple(symbols)


def _excluded_run_identity(result_path: Path) -> dict[str, Any]:
    result = _read_json_object(result_path, "excluded strategy-lab result")
    manifest = result.get("manifest")
    if not isinstance(manifest, Mapping):
        raise HistoricalStrategyLabError(
            f"{result_path}: excluded result has no manifest"
        )
    dataset = manifest.get("dataset")
    if not isinstance(dataset, Mapping):
        raise HistoricalStrategyLabError(
            f"{result_path}: excluded result has no dataset identity"
        )
    evidence_path = _resolve_recorded_path(
        dataset.get("evidence_manifest"), relative_to=result_path.parent
    )
    evidence = _read_json_object(evidence_path, "excluded evidence manifest")
    candidates_by_date = evidence.get("candidates_by_date")
    if not isinstance(candidates_by_date, Mapping) or not candidates_by_date:
        raise HistoricalStrategyLabError(
            f"{evidence_path}: excluded evidence has no inspected dates"
        )
    dates = sorted(str(day) for day in candidates_by_date)
    if int(dataset.get("requested_dates", -1)) != len(dates):
        raise HistoricalStrategyLabError(
            f"{result_path}: excluded requested-date count is inconsistent"
        )
    return {
        "run_id": str(manifest.get("run_id", "")),
        "dataset_hash": str(dataset.get("dataset_hash", "")),
        "result_path": _display_path(result_path),
        "result_sha256": _sha256_file(result_path),
        "evidence_path": _display_path(evidence_path),
        "evidence_sha256": _sha256_file(evidence_path),
        "requested_dates": dates,
        "requested_dates_sha256": _sha256_bytes(_canonical_json(dates)),
    }


def _confirmation_selection_boundary(
    evidence_path: Path,
    evidence: Mapping[str, Any],
    candidates_by_date: Mapping[str, Any],
    blocked_candidates_by_date: Mapping[str, Any],
) -> tuple[list[str], dict[str, Any] | None]:
    """Resolve the exact requested sample, including preflight-blocked dates."""

    ready_dates = {str(value) for value in candidates_by_date}
    blocked_dates = {str(value) for value in blocked_candidates_by_date}
    if ready_dates & blocked_dates:
        raise HistoricalStrategyLabError(
            "confirmation dates cannot be both ready and preflight-blocked"
        )
    selection_value = evidence.get("parent_selection_file") or evidence.get(
        "selection_file"
    )
    if not isinstance(selection_value, str) or not selection_value.strip():
        # Backward-compatible fixture path: a confirmation evidence file that
        # predates linked selections still freezes every represented date.
        return sorted(ready_dates | blocked_dates), None
    selection_path = _resolve_recorded_path(
        selection_value, relative_to=evidence_path.parent
    ).resolve()
    selection = _read_json_object(selection_path, "confirmation selection manifest")
    selected_dates = selection.get("selected_dates")
    seed = selection.get("seed")
    if (
        not isinstance(seed, int)
        or not isinstance(selected_dates, list)
        or not all(isinstance(day, str) and DATE_PATTERN.fullmatch(day) for day in selected_dates)
        or len(selected_dates) != len(set(selected_dates))
    ):
        raise HistoricalStrategyLabError(
            "confirmation selection requires a unique selected_dates array and integer seed"
        )
    represented = ready_dates | blocked_dates
    if set(selected_dates) != represented:
        missing = sorted(set(selected_dates) - represented)
        extra = sorted(represented - set(selected_dates))
        raise HistoricalStrategyLabError(
            "confirmation evidence does not represent the exact selected dates "
            f"(missing={missing[:10]}, extra={extra[:10]})"
        )
    copied_seed = evidence.get("selection_seed")
    if copied_seed != seed:
        raise HistoricalStrategyLabError(
            "confirmation evidence selection seed does not match linked selection"
        )
    preflight = evidence.get("preflight")
    declared_blocked = (
        preflight.get("blocked_dates") if isinstance(preflight, Mapping) else None
    )
    if not isinstance(declared_blocked, list) or set(declared_blocked) != blocked_dates:
        raise HistoricalStrategyLabError(
            "confirmation preflight blockers do not match blocked candidate universes"
        )
    return sorted(selected_dates), {
        "path": _display_path(selection_path),
        "sha256": _sha256_file(selection_path),
        "seed": seed,
        "selected_dates_sha256": _sha256_bytes(_canonical_json(selected_dates)),
    }


def freeze_confirmation_manifest(
    evidence_path: Path,
    *,
    excluded_result_paths: Sequence[Path] = (DEFAULT_EXCLUDED_RESULT,),
    output_root: Path = DEFAULT_CONFIRMATION_ROOT,
    registered_at: str | None = None,
) -> tuple[Path, dict[str, Any]]:
    """Freeze the only permitted confirmation policy before price collection."""

    evidence_path = evidence_path.resolve()
    evidence = _read_json_object(evidence_path, "confirmation evidence manifest")
    scanner = evidence.get("scanner")
    candidates_by_date = evidence.get("candidates_by_date")
    blocked_candidates_by_date = evidence.get("blocked_candidates_by_date", {})
    if not isinstance(scanner, Mapping) or not isinstance(candidates_by_date, Mapping):
        raise HistoricalStrategyLabError(
            "confirmation evidence must contain scanner and candidates_by_date objects"
        )
    if not isinstance(blocked_candidates_by_date, Mapping):
        raise HistoricalStrategyLabError(
            "confirmation blocked_candidates_by_date must be an object"
        )
    if (
        scanner.get("point_in_time") is not True
        or scanner.get("universe_capture_complete") is not True
        or scanner.get("target_session_prices_observed") is not False
    ):
        raise HistoricalStrategyLabError(
            "confirmation discovery must attest a complete point-in-time universe "
            "with target_session_prices_observed=false"
        )
    registered = _parse_timestamp(
        registered_at or datetime.now(UTC).isoformat(), "registered_at"
    )
    prepared = _parse_timestamp(evidence.get("prepared_at"), "evidence.prepared_at")
    if prepared > registered:
        raise HistoricalStrategyLabError(
            "candidate evidence cannot be prepared after preregistration"
        )
    requested_dates, selection_identity = _confirmation_selection_boundary(
        evidence_path,
        evidence,
        candidates_by_date,
        blocked_candidates_by_date,
    )
    preflight = evidence.get("preflight")
    preflight_dates = (
        preflight.get("dates") if isinstance(preflight, Mapping) else None
    )
    frozen_dates: list[dict[str, Any]] = []
    for day in requested_dates:
        if not DATE_PATTERN.fullmatch(day):
            raise HistoricalStrategyLabError(f"invalid confirmation date {day!r}")
        blocked = day in blocked_candidates_by_date
        rows = (
            blocked_candidates_by_date[day]
            if blocked
            else candidates_by_date[day]
        )
        if blocked and not isinstance(rows, list):
            raise HistoricalStrategyLabError(
                f"{day}: blocked candidate universe must be an array"
            )
        symbols = _candidate_symbols(rows, day) if rows else ()
        for row in rows:
            published = _parse_timestamp(
                row["catalyst"]["published_at"],
                f"{day} {row['symbol']} catalyst.published_at",
            )
            if published > registered:
                raise HistoricalStrategyLabError(
                    f"{day} {row['symbol']}: catalyst evidence postdates preregistration"
                )
        blocker: dict[str, Any] | None = None
        if blocked:
            report = (
                preflight_dates.get(day)
                if isinstance(preflight_dates, Mapping)
                else None
            )
            if not isinstance(report, Mapping) or report.get("blocked") is not True:
                raise HistoricalStrategyLabError(
                    f"{day}: preflight blocker detail is missing"
                )
            if list(report.get("accepted_symbols", [])) != list(symbols):
                raise HistoricalStrategyLabError(
                    f"{day}: blocked candidate order differs from preflight"
                )
            blocker = {
                "reason_code": str(report.get("blocked_reason", "preflight_blocked")),
                "accepted_candidates": len(symbols),
                "required_candidates": int(preflight.get("minimum_candidates", 10)),
                "examined_candidates": int(report.get("examined_count", 0)),
                "preflight_report_sha256": _sha256_bytes(_canonical_json(report)),
            }
        frozen_evidence = {
            "date": day,
            "candidates": rows,
            "scanner": dict(scanner),
        }
        if blocker is not None:
            frozen_evidence["precollection_blocker"] = blocker
        frozen_dates.append(
            {
                "date": day,
                "status": (
                    "precollection_blocked" if blocked else "validation_ready"
                ),
                "ordered_symbols": list(symbols),
                "ordered_candidates": rows,
                "precollection_blocker": blocker,
                "frozen_evidence_sha256": _sha256_bytes(
                    _canonical_json(frozen_evidence)
                ),
            }
        )
    minimum_dates = int(
        CONFIRMATION_ACCEPTANCE_THRESHOLDS["minimum_requested_dates"]
    )
    if len(frozen_dates) < minimum_dates:
        raise HistoricalStrategyLabError(
            f"confirmation requires at least {minimum_dates} frozen dates"
        )
    excluded_runs = [
        _excluded_run_identity(path.resolve()) for path in excluded_result_paths
    ]
    required_run = "strategy-lab-651f20ff135e-b268f18ec755"
    if required_run not in {value["run_id"] for value in excluded_runs}:
        raise HistoricalStrategyLabError(
            f"confirmation must exclude inspected run {required_run}"
        )
    excluded_dates = {
        day for value in excluded_runs for day in value["requested_dates"]
    }
    overlap = sorted(
        value["date"] for value in frozen_dates if value["date"] in excluded_dates
    )
    if overlap:
        raise HistoricalStrategyLabError(
            "confirmation dates overlap previously inspected dates: "
            + ", ".join(overlap[:10])
        )
    config = load_config()
    plugin_identity = plugin_manifest_hash_input(("opening-reversal",))
    implementation_paths = (
        Path(__file__),
        PROJECT_ROOT / "historical_research.py",
        PROJECT_ROOT / "historical_research_strategies.py",
    )
    body: dict[str, Any] = {
        "schema_version": CONFIRMATION_SCHEMA_VERSION,
        "kind": "historical_strategy_confirmation_preregistration",
        "confirmation_contract_id": CONFIRMATION_CONTRACT_ID,
        "registered_at": registered.isoformat(),
        "status": "frozen_before_target_session_collection",
        "source_evidence": {
            "path": _display_path(evidence_path),
            "sha256": _sha256_file(evidence_path),
            "prepared_at": evidence["prepared_at"],
            "parent_selection_file": evidence.get("parent_selection_file"),
            "selection_seed": evidence.get("selection_seed"),
            "selection_manifest": selection_identity,
            "scanner": dict(scanner),
        },
        "excluded_inspected_runs": excluded_runs,
        "frozen_dates": frozen_dates,
        "policy": asdict(FROZEN_CONFIRMATION_POLICY),
        "strategy_plugin": plugin_identity,
        "execution_grid": CONFIRMATION_EXECUTION_GRID,
        "acceptance_thresholds": CONFIRMATION_ACCEPTANCE_THRESHOLDS,
        "deployment_views": CONFIRMATION_DEPLOYMENT,
        "production_baseline": {
            "strategy_version": config.version,
            "rules_hash": config.rules_hash,
            "production_rules_must_remain_unchanged": True,
        },
        "collection_contract": {
            "target_session_capture_must_follow_registered_at": True,
            "bundle_sample_phase": "confirmation",
            "bundle_manifest_hash_must_match": True,
            "candidate_order_must_match": True,
            "date_and_symbol_substitution_allowed": False,
        },
        "implementations": {
            str(path.relative_to(PROJECT_ROOT)): _sha256_file(path)
            for path in implementation_paths
        },
        "automatic_strategy_application": False,
        "broker_actions_allowed": False,
    }
    manifest_hash = _sha256_bytes(_canonical_json(body))
    manifest = {**body, "manifest_sha256": manifest_hash}
    output_root = _safe_publish_prefix(output_root / "manifest").parent
    output_path = output_root / f"confirmation-{manifest_hash}.json"
    if output_path.exists():
        if _read_json_object(output_path, "confirmation manifest") != manifest:
            raise HistoricalStrategyLabError(
                "hash-addressed confirmation manifest already exists with other content"
            )
    else:
        _atomic_json(output_path, manifest)
    return output_path, manifest


def load_confirmation_manifest(path: Path) -> dict[str, Any]:
    manifest = _read_json_object(path, "confirmation manifest")
    recorded_hash = manifest.get("manifest_sha256")
    if not isinstance(recorded_hash, str) or len(recorded_hash) != 64:
        raise HistoricalStrategyLabError("confirmation manifest hash is missing")
    body = dict(manifest)
    body.pop("manifest_sha256", None)
    actual_hash = _sha256_bytes(_canonical_json(body))
    if actual_hash != recorded_hash:
        raise HistoricalStrategyLabError("confirmation manifest was mutated")
    if path.name != f"confirmation-{recorded_hash}.json":
        raise HistoricalStrategyLabError(
            "confirmation manifest filename is not hash-addressed"
        )
    if (
        manifest.get("schema_version") != CONFIRMATION_SCHEMA_VERSION
        or manifest.get("confirmation_contract_id") != CONFIRMATION_CONTRACT_ID
        or manifest.get("policy") != asdict(FROZEN_CONFIRMATION_POLICY)
        or manifest.get("execution_grid") != CONFIRMATION_EXECUTION_GRID
        or manifest.get("acceptance_thresholds")
        != CONFIRMATION_ACCEPTANCE_THRESHOLDS
        or manifest.get("deployment_views") != CONFIRMATION_DEPLOYMENT
    ):
        raise HistoricalStrategyLabError(
            "confirmation manifest does not match the frozen executable contract"
        )
    _parse_timestamp(manifest.get("registered_at"), "manifest.registered_at")
    frozen_dates = manifest.get("frozen_dates")
    if not isinstance(frozen_dates, list) or len(frozen_dates) < 100:
        raise HistoricalStrategyLabError("confirmation manifest has too few dates")
    dates = [str(value.get("date", "")) for value in frozen_dates]
    if dates != sorted(dates) or len(set(dates)) != len(dates):
        raise HistoricalStrategyLabError(
            "confirmation dates must be unique and chronological"
        )
    for frozen in frozen_dates:
        status = frozen.get("status", "validation_ready")
        symbols = frozen.get("ordered_symbols")
        candidates = frozen.get("ordered_candidates")
        if status not in {"validation_ready", "precollection_blocked"}:
            raise HistoricalStrategyLabError(
                f"{frozen.get('date')}: invalid frozen-date status"
            )
        if not isinstance(symbols, list) or not isinstance(candidates, list):
            raise HistoricalStrategyLabError(
                f"{frozen.get('date')}: malformed frozen candidate universe"
            )
        if status == "validation_ready" and not symbols:
            raise HistoricalStrategyLabError(
                f"{frozen.get('date')}: validation-ready date has no candidates"
            )
        if status == "precollection_blocked" and not isinstance(
            frozen.get("precollection_blocker"), Mapping
        ):
            raise HistoricalStrategyLabError(
                f"{frozen.get('date')}: precollection blocker is missing"
            )
    current_plugin = plugin_manifest_hash_input(("opening-reversal",))
    if manifest.get("strategy_plugin") != current_plugin:
        raise HistoricalStrategyLabError(
            "opening-reversal implementation changed after preregistration"
        )
    for relative, expected_hash in manifest.get("implementations", {}).items():
        path_value = PROJECT_ROOT / str(relative)
        if not path_value.is_file() or _sha256_file(path_value) != expected_hash:
            raise HistoricalStrategyLabError(
                f"confirmation implementation changed after preregistration: {relative}"
            )
    config = load_config()
    baseline = manifest.get("production_baseline")
    if not isinstance(baseline, Mapping) or (
        baseline.get("strategy_version") != config.version
        or baseline.get("rules_hash") != config.rules_hash
    ):
        raise HistoricalStrategyLabError(
            "production baseline changed after confirmation preregistration"
        )
    return manifest


def _seconds(time_et: str) -> int:
    try:
        hour, minute, second = (int(value) for value in time_et.split(":"))
    except (TypeError, ValueError) as exc:
        raise HistoricalStrategyLabError(f"invalid ET time {time_et!r}") from exc
    return hour * 3600 + minute * 60 + second


def normalize_rejection_reason(reason: str) -> str:
    if SCORE_REASON.match(reason):
        return "score below maturity gate"
    if STALE_REASON.match(reason):
        return "quote snapshot is stale"
    if CROSSED_REASON.match(reason):
        return "quote snapshot is crossed"
    return reason


def rejection_category(reason: str) -> str:
    reason = normalize_rejection_reason(reason)
    operational = {
        "account is not agentic-authorized",
        "authorized account was not identified",
        "identifier encryption or audit is unavailable",
        "continuous monitoring is unavailable",
        "prompt protective-stop workflow is unavailable",
        "broker review workflow is unavailable",
        "an equity position already exists",
        "an unresolved equity order already exists",
        "today's filled-entry limit is exhausted",
        "a circuit breaker is active",
        "symbol is not currently long-tradable",
        "halt or unstable-liquidity risk is present",
        "quote snapshot is crossed",
        "entry limit is not marketable against the fresh ask",
        "risk, allocation, or liquidity caps produce zero shares",
    }
    universe = {
        "opening price is below the universe minimum",
        "average daily volume is below the universe minimum",
        "daily ATR is below the universe minimum",
        "opening relative volume is below 1.0",
        "security is not a U.S.-listed common stock",
        "catalyst is not verified",
        "catalyst has a dilution or financing conflict",
    }
    structure = {
        "first five-minute candle is not bullish",
        "opening-range breakout is not clean",
        "price is not above session VWAP",
        "session VWAP is falling",
        "market alignment gate failed",
        "sector or candidate relative-strength gate failed",
    }
    execution = {
        "quote snapshot is stale",
        "median spread exceeds operating limit",
        "a snapshot spread exceeds the hard limit",
        "entry would chase too far above the opening-range high",
        "entry time is outside the production window",
    }
    risk_exit = {
        "planned stop is inside ordinary noise",
        "stop distance exceeds the maximum fraction",
        "resistance room is below 2.2%",
        "reward/risk before resistance is below 2.5",
    }
    if reason in operational:
        return "operational_safety"
    if reason in universe:
        return "universe"
    if reason in structure:
        return "signal_structure"
    if reason in execution:
        return "execution"
    if reason in risk_exit:
        return "risk_and_exit"
    if reason == "score below maturity gate":
        return "derived_score"
    if reason.startswith("live entries are disabled for "):
        return "operational_safety"
    return "unclassified"


def _load_verified_bundle(
    item: Mapping[str, Any], *, expected_frozen_hash: str | None = None
) -> Mapping[str, Any] | None:
    if item["status"] != "available":
        return None
    path = Path(str(item["path"]))
    try:
        bundle = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise HistoricalStrategyLabError(f"cannot read bundle {path}: {exc}") from exc
    day = str(item["date"])
    if bundle.get("date") != day:
        raise HistoricalStrategyLabError(f"{day}: bundle date mismatch")
    if bundle.get("session_capture_complete") is not True:
        raise HistoricalStrategyLabError(f"{day}: incomplete session capture")
    source = bundle.get("source")
    required = (
        "point_in_time",
        "regular_hours_only",
        "split_adjusted",
        "universe_capture_complete",
    )
    if not isinstance(source, Mapping) or any(
        source.get(value) is not True for value in required
    ):
        raise HistoricalStrategyLabError(f"{day}: source attestations are incomplete")
    frozen_hash = source.get("frozen_evidence_sha256")
    if not isinstance(frozen_hash, str) or len(frozen_hash) != 64:
        raise HistoricalStrategyLabError(f"{day}: frozen evidence identity is missing")
    if expected_frozen_hash is not None and frozen_hash != expected_frozen_hash:
        raise HistoricalStrategyLabError(
            f"{day}: bundle belongs to different frozen evidence"
        )
    candidates = bundle.get("candidates")
    if not isinstance(candidates, list) or not candidates:
        raise HistoricalStrategyLabError(f"{day}: bundle has no candidates")
    symbols = tuple(
        str(value.get("symbol", "")).strip().upper() for value in candidates
    )
    expected = tuple(item["expected_symbols"])
    if symbols != expected:
        raise HistoricalStrategyLabError(f"{day}: ordered candidate universe mismatch")
    actual_hash = _sha256_file(path)
    if actual_hash != item["sha256"]:
        raise HistoricalStrategyLabError(f"{day}: bundle changed after inventory")
    return bundle


def _expected_frozen_hashes(evidence_path: Path) -> dict[str, str]:
    """Reconstruct the builder's exact per-date evidence identity."""

    try:
        evidence = json.loads(evidence_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise HistoricalStrategyLabError(
            f"cannot read evidence manifest {evidence_path}: {exc}"
        ) from exc
    scanner = evidence.get("scanner")
    candidates_by_date = evidence.get("candidates_by_date")
    if not isinstance(scanner, Mapping) or not isinstance(candidates_by_date, Mapping):
        raise HistoricalStrategyLabError(
            "evidence manifest must contain scanner and candidates_by_date objects"
        )
    expected: dict[str, str] = {}
    for day, rows in candidates_by_date.items():
        if not isinstance(day, str) or not isinstance(rows, list) or not rows:
            raise HistoricalStrategyLabError(
                "evidence dates must map to non-empty candidate arrays"
            )
        expected[day] = _sha256_bytes(
            _canonical_json(
                {
                    "date": day,
                    "candidates": rows,
                    "scanner": dict(scanner),
                }
            )
        )
    return expected


def _observation(
    day: str,
    raw: Mapping[str, Any],
    plugin: StrategyPlugin,
    config: StrategyConfig,
) -> Observation:
    symbol = str(raw.get("symbol", "")).strip().upper()
    signal_id = str(raw.get("signal_id", f"{day}-{symbol}-research"))
    context = CandidateContext(day, symbol, signal_id, parse_bars(raw.get("bars")))
    try:
        search = _find_signal_point_in_time(plugin, context)
    except (ResearchStrategyError, HistoricalResearchError) as exc:
        raise HistoricalStrategyLabError(
            f"{day} {symbol} {plugin.strategy_id}: {exc}"
        ) from exc
    payload = raw.get("evaluation_payload")
    if not isinstance(payload, Mapping):
        raise HistoricalStrategyLabError(f"{day} {symbol}: evaluation payload missing")
    production = evaluate_candidate(payload, config)
    discovery = raw.get("discovery")
    items = (
        str(discovery.get("filing_items", "")) if isinstance(discovery, Mapping) else ""
    )
    evaluation_time = str(raw.get("evaluation_time_et", ""))
    _seconds(evaluation_time)
    return Observation(
        date=day,
        symbol=symbol,
        signal_id=signal_id,
        strategy_id=plugin.strategy_id,
        context=context,
        decision=search.decision,
        no_signal_reason=search.reason,
        production=production,
        earnings_2_02="2.02" in {value.strip() for value in items.split(",")},
        production_evaluation_time_et=evaluation_time,
    )


def build_observations(
    dataset: Mapping[str, Any],
    policies: Sequence[Policy],
    expected_frozen_hashes: Mapping[str, str],
) -> tuple[dict[str, list[Observation]], dict[str, str]]:
    strategy_specs = tuple(dict.fromkeys(policy.strategy_spec for policy in policies))
    strategy_plugins = {
        strategy_spec: load_strategy(strategy_spec) for strategy_spec in strategy_specs
    }
    config = load_config()
    observations: dict[str, list[Observation]] = defaultdict(list)
    blocked: dict[str, str] = {}
    for item in dataset["bundles"]:
        day = str(item["date"])
        expected_frozen_hash = expected_frozen_hashes.get(day)
        if expected_frozen_hash is None:
            raise HistoricalStrategyLabError(
                f"{day}: evidence identity was not reconstructed"
            )
        bundle = _load_verified_bundle(item, expected_frozen_hash=expected_frozen_hash)
        if bundle is None:
            blocked[day] = "bundle_missing"
            continue
        for raw in bundle["candidates"]:
            for strategy_spec in strategy_specs:
                observations[day].append(
                    _observation(day, raw, strategy_plugins[strategy_spec], config)
                )
    return dict(observations), blocked


def _selection_key(observation: Observation, policy: Policy) -> tuple[Any, ...]:
    decision = observation.decision
    if decision is None:
        raise HistoricalStrategyLabError("cannot rank an observation without a signal")
    if policy.selection == "production_score":
        secondary = (
            -observation.production.score,
            observation.production.opening_rvol_rank,
            -decision.strength,
        )
    elif policy.selection == "opening_rvol":
        secondary = (
            observation.production.opening_rvol_rank,
            -observation.production.opening_relative_volume,
            -decision.strength,
        )
    else:
        secondary = (-decision.strength,)
    return (decision.signal_index, *secondary, observation.symbol)


def evaluate_policy_day(
    observations: Sequence[Observation],
    policy: Policy,
    execution: ExecutionConfig,
) -> dict[str, Any]:
    policy.validate()
    candidates: list[tuple[Observation, dict[str, Any], float]] = []
    filtered = Counter()
    unexecutable = Counter()
    strategy_id = load_strategy(policy.strategy_spec).strategy_id
    for observation in observations:
        if observation.strategy_id != strategy_id:
            continue
        decision = observation.decision
        if decision is None:
            filtered["no_signal"] += 1
            continue
        signal_time = observation.context.bars[decision.signal_index].time_et
        if (
            policy.signal_cutoff_et is not None
            and signal_time >= policy.signal_cutoff_et
        ):
            filtered["signal_at_or_after_cutoff"] += 1
            continue
        if policy.require_earnings_2_02 and not observation.earnings_2_02:
            filtered["not_item_2.02_earnings"] += 1
            continue
        if policy.require_production_eligible and not observation.production.eligible:
            filtered["production_gate_stack_rejected"] += 1
            continue
        try:
            trade = _trade_from_decision(observation.context, decision, execution)
        except ResearchStrategyError as exc:
            unexecutable[str(exc)] += 1
            continue
        stop_fraction = (
            float(trade["entry_price"]) - float(trade["technical_stop"])
        ) / float(trade["entry_price"])
        if (
            policy.maximum_stop_fraction is not None
            and stop_fraction > policy.maximum_stop_fraction
        ):
            filtered["stop_exceeds_policy_maximum"] += 1
            continue
        candidates.append((observation, trade, stop_fraction))
    candidates.sort(key=lambda value: _selection_key(value[0], policy))
    if not candidates:
        return {
            "trade": None,
            "eligible_counterfactuals": 0,
            "filtered": dict(sorted(filtered.items())),
            "unexecutable": dict(sorted(unexecutable.items())),
        }
    observation, trade, stop_fraction = candidates[0]
    return {
        "trade": {
            **trade,
            "production_score": observation.production.score,
            "opening_rvol": round(observation.production.opening_relative_volume, 8),
            "opening_rvol_rank": observation.production.opening_rvol_rank,
            "earnings_2_02": observation.earnings_2_02,
            "stop_fraction": round(stop_fraction, 8),
            "production_eligible": observation.production.eligible,
            "production_rejection_reasons": [
                normalize_rejection_reason(reason)
                for reason in observation.production.hard_rejects
            ],
            "production_evaluation_time_et": observation.production_evaluation_time_et,
            "evaluation_minus_signal_bar_start_seconds": _seconds(
                observation.production_evaluation_time_et
            )
            - _seconds(str(trade["signal_time_et"])),
            "evaluation_minus_entry_bar_start_seconds": _seconds(
                observation.production_evaluation_time_et
            )
            - _seconds(str(trade["entry_time_et"])),
        },
        "eligible_counterfactuals": len(candidates),
        "filtered": dict(sorted(filtered.items())),
        "unexecutable": dict(sorted(unexecutable.items())),
    }


def _drawdown(values: Sequence[float]) -> float:
    equity = 0.0
    peak = 0.0
    maximum = 0.0
    for value in values:
        equity += value
        peak = max(peak, equity)
        maximum = max(maximum, peak - equity)
    return maximum


def _quantile(values: Sequence[float], fraction: float) -> float:
    if not values:
        raise HistoricalStrategyLabError("quantile requires values")
    ordered = sorted(values)
    position = (len(ordered) - 1) * fraction
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    weight = position - lower
    return ordered[lower] * (1 - weight) + ordered[upper] * weight


def _bootstrap_mean(
    values: Sequence[float], *, seed: int, samples: int
) -> dict[str, Any]:
    if not values:
        return {
            "samples": samples,
            "lower_90_one_sided": None,
            "confidence_interval_95": [None, None],
            "probability_mean_positive": None,
        }
    generator = random.Random(seed)
    count = len(values)
    means = [
        sum(values[generator.randrange(count)] for _ in range(count)) / count
        for _ in range(samples)
    ]
    return {
        "samples": samples,
        "lower_90_one_sided": round(_quantile(means, 0.10), 8),
        "confidence_interval_95": [
            round(_quantile(means, 0.025), 8),
            round(_quantile(means, 0.975), 8),
        ],
        "probability_mean_positive": round(
            sum(value > 0 for value in means) / samples, 8
        ),
    }


def _return_stats(values: Sequence[float]) -> dict[str, Any]:
    if not values:
        return {
            "trades": 0,
            "wins": 0,
            "losses": 0,
            "win_rate": None,
            "mean_r": None,
            "median_r": None,
            "total_r": 0.0,
            "profit_factor": None,
            "maximum_drawdown_r": 0.0,
        }
    wins = [value for value in values if value > 0]
    losses = [value for value in values if value < 0]
    gross_loss = -sum(losses)
    return {
        "trades": len(values),
        "wins": len(wins),
        "losses": len(losses),
        "win_rate": round(len(wins) / len(values), 8),
        "mean_r": round(statistics.fmean(values), 8),
        "median_r": round(statistics.median(values), 8),
        "total_r": round(sum(values), 8),
        "profit_factor": (round(sum(wins) / gross_loss, 8) if gross_loss > 0 else None),
        "maximum_drawdown_r": round(_drawdown(values), 8),
    }


def _phase_ranges(requested_dates: Sequence[str]) -> list[tuple[str, int, int]]:
    count = len(requested_dates)
    development_end = math.floor(count * 0.60)
    validation_end = math.floor(count * 0.80)
    return [
        ("development", 0, development_end),
        ("retrospective_validation", development_end, validation_end),
        ("retrospective_holdout", validation_end, count),
    ]


def summarize_policy(
    policy: Policy,
    requested_dates: Sequence[str],
    observations: Mapping[str, Sequence[Observation]],
    blocked: Mapping[str, str],
    execution: ExecutionConfig,
    *,
    bootstrap_samples: int,
    include_daily: bool = False,
) -> dict[str, Any]:
    daily: list[dict[str, Any]] = []
    aggregate_filtered = Counter()
    aggregate_unexecutable = Counter()
    for day in requested_dates:
        if day in blocked:
            daily.append({"date": day, "status": "blocked", "reason": blocked[day]})
            continue
        result = evaluate_policy_day(observations.get(day, ()), policy, execution)
        aggregate_filtered.update(result["filtered"])
        aggregate_unexecutable.update(result["unexecutable"])
        daily.append(
            {
                "date": day,
                "status": "trade" if result["trade"] else "no_trade",
                "trade": result["trade"],
                "eligible_counterfactuals": result["eligible_counterfactuals"],
            }
        )
    covered = [value for value in daily if value["status"] != "blocked"]
    trades = [value["trade"] for value in daily if value["status"] == "trade"]
    returns = [float(value["net_r"]) for value in trades]
    stats = _return_stats(returns)
    monthly: dict[str, list[float]] = defaultdict(list)
    exits = Counter()
    for row in daily:
        if row["status"] != "trade":
            continue
        trade = row["trade"]
        monthly[row["date"][:7]].append(float(trade["net_r"]))
        exits[str(trade["exit_reason"])] += 1
    phases = {}
    for label, start, end in _phase_ranges(requested_dates):
        rows = daily[start:end]
        phase_returns = [
            float(value["trade"]["net_r"])
            for value in rows
            if value["status"] == "trade"
        ]
        phase_stats = _return_stats(phase_returns)
        phase_stats.update(
            {
                "requested_days": len(rows),
                "covered_days": sum(value["status"] != "blocked" for value in rows),
                "first_date": rows[0]["date"] if rows else None,
                "last_date": rows[-1]["date"] if rows else None,
            }
        )
        phases[label] = phase_stats
    stop_fractions = [float(value["stop_fraction"]) for value in trades]
    implied_allocations = [
        min(0.80, 0.0025 / (value + 0.001)) for value in stop_fractions
    ]
    ordered_returns = sorted(returns, reverse=True)
    seed = int(
        _sha256_bytes(
            _canonical_json(
                {
                    "policy": asdict(policy),
                    "execution": asdict(execution),
                    "dates": list(requested_dates),
                }
            )
        )[:16],
        16,
    )
    summary = {
        "policy": asdict(policy),
        "requested_days": len(requested_dates),
        "covered_days": len(covered),
        "blocked_days": len(daily) - len(covered),
        "no_trade_days": sum(value["status"] == "no_trade" for value in daily),
        **stats,
        "mean_r_per_covered_day": (
            round(sum(returns) / len(covered), 8) if covered else None
        ),
        "bootstrap_trade_mean": _bootstrap_mean(
            returns, seed=seed, samples=bootstrap_samples
        ),
        "chronological_phases": phases,
        "monthly": {
            month: _return_stats(values) for month, values in sorted(monthly.items())
        },
        "exit_reasons": dict(sorted(exits.items())),
        "filtered_signals": dict(sorted(aggregate_filtered.items())),
        "unexecutable_signals": dict(sorted(aggregate_unexecutable.items())),
        "stop_geometry": {
            "median_fraction": (
                round(statistics.median(stop_fractions), 8) if stop_fractions else None
            ),
            "p90_fraction": (
                round(_quantile(stop_fractions, 0.90), 8) if stop_fractions else None
            ),
            "at_or_below_0.8pct": sum(value <= 0.008 for value in stop_fractions),
            "at_or_below_0.8pct_fraction": (
                round(sum(value <= 0.008 for value in stop_fractions) / len(trades), 8)
                if trades
                else None
            ),
            "median_implied_unvalidated_notional_fraction": (
                round(statistics.median(implied_allocations), 8)
                if implied_allocations
                else None
            ),
            "assumptions": {
                "account_risk_fraction": 0.0025,
                "reserve_fraction": 0.001,
                "allocation_cap_fraction": 0.80,
            },
        },
        "concentration": {
            "best_trade_r": round(ordered_returns[0], 8) if ordered_returns else None,
            "top_five_total_r": (
                round(sum(ordered_returns[:5]), 8) if ordered_returns else None
            ),
            "total_without_top_five_r": (
                round(sum(ordered_returns[5:]), 8) if ordered_returns else None
            ),
        },
    }
    if include_daily:
        summary["execution"] = asdict(execution)
        summary["daily"] = daily
    return summary


def _all_signal_diagnostics(
    observations: Mapping[str, Sequence[Observation]],
    execution: ExecutionConfig,
) -> dict[str, Any]:
    rows: list[tuple[Observation, dict[str, Any]]] = []
    unexecutable = Counter()
    production_evaluations: dict[str, EvaluationResult] = {}
    for day_observations in observations.values():
        for observation in day_observations:
            production_evaluations.setdefault(
                observation.signal_id, observation.production
            )
            if (
                observation.strategy_id != "orb-5m-research"
                or observation.decision is None
            ):
                continue
            try:
                trade = _trade_from_decision(
                    observation.context, observation.decision, execution
                )
            except ResearchStrategyError as exc:
                unexecutable[str(exc)] += 1
                continue
            rows.append((observation, trade))
    reasons = sorted(
        {
            normalize_rejection_reason(reason)
            for observation, _ in rows
            for reason in observation.production.hard_rejects
        }
    )
    attribution = []
    for reason in reasons:
        failed = [
            float(trade["net_r"])
            for observation, trade in rows
            if reason
            in {
                normalize_rejection_reason(value)
                for value in observation.production.hard_rejects
            }
        ]
        passed = [
            float(trade["net_r"])
            for observation, trade in rows
            if reason
            not in {
                normalize_rejection_reason(value)
                for value in observation.production.hard_rejects
            }
        ]
        attribution.append(
            {
                "reason": reason,
                "category": rejection_category(reason),
                "failed": _return_stats(failed),
                "passed": _return_stats(passed),
                "pass_minus_fail_mean_r": (
                    round(statistics.fmean(passed) - statistics.fmean(failed), 8)
                    if passed and failed
                    else None
                ),
            }
        )
    reason_counts = Counter()
    category_counts = Counter()
    production_eligible = 0
    for evaluation in production_evaluations.values():
        production_eligible += evaluation.eligible
        for raw_reason in evaluation.hard_rejects:
            reason = normalize_rejection_reason(raw_reason)
            reason_counts[reason] += 1
            category_counts[rejection_category(reason)] += 1
    delays_signal = [
        _seconds(observation.production_evaluation_time_et)
        - _seconds(str(trade["signal_time_et"]))
        for observation, trade in rows
    ]
    delays_entry = [
        _seconds(observation.production_evaluation_time_et)
        - _seconds(str(trade["entry_time_et"]))
        for observation, trade in rows
    ]
    chase_rejects = sum(
        "entry would chase too far above the opening-range high"
        in observation.production.hard_rejects
        for observation, _ in rows
    )
    return {
        "production_candidate_evaluations": len(production_evaluations),
        "production_eligible": production_eligible,
        "executable_orb_counterfactual_signals": len(rows),
        "unexecutable_orb_signals": dict(sorted(unexecutable.items())),
        "chase_rejected_executable_orb_signals": chase_rejects,
        "chase_rejected_fraction": (
            round(chase_rejects / len(rows), 8) if rows else None
        ),
        "production_rejection_reason_counts": dict(reason_counts.most_common()),
        "production_rejection_category_counts": dict(sorted(category_counts.items())),
        "evaluation_delay_seconds": {
            "from_signal_bar_start_median": (
                statistics.median(delays_signal) if delays_signal else None
            ),
            "from_signal_bar_start_p90": (
                _quantile(delays_signal, 0.90) if delays_signal else None
            ),
            "from_research_entry_bar_start_median": (
                statistics.median(delays_entry) if delays_entry else None
            ),
            "from_research_entry_bar_start_p90": (
                _quantile(delays_entry, 0.90) if delays_entry else None
            ),
            "note": "Minute timestamps identify bar starts; this is a fidelity diagnostic, not observed live latency.",
        },
        "gate_attribution": sorted(
            attribution,
            key=lambda value: (
                value["category"],
                value["reason"],
            ),
        ),
    }


def _compact_sensitivity(summary: Mapping[str, Any]) -> dict[str, Any]:
    return {
        key: summary[key]
        for key in (
            "trades",
            "win_rate",
            "mean_r",
            "total_r",
            "profit_factor",
            "maximum_drawdown_r",
            "mean_r_per_covered_day",
        )
    }


def _status(
    summary: Mapping[str, Any], sensitivity: Sequence[Mapping[str, Any]]
) -> tuple[str, str]:
    if not summary["trades"]:
        research = "no_trade_evidence"
    else:
        validation = summary["chronological_phases"]["retrospective_validation"]
        holdout = summary["chronological_phases"]["retrospective_holdout"]
        bootstrap = summary["bootstrap_trade_mean"]

        def sensitivity_point(slippage_bps: float, target_r: float) -> Any:
            return next(
                (
                    value
                    for value in sensitivity
                    if value["entry_slippage_bps"] == slippage_bps
                    and value["exit_slippage_bps"] == slippage_bps
                    and value["target_r"] == target_r
                ),
                None,
            )

        ten_bps = sensitivity_point(10.0, 2.0)
        twenty_bps = sensitivity_point(20.0, 2.0)
        base_cost_targets = [
            sensitivity_point(5.0, target_r) for target_r in (1.0, 1.5, 2.0, 3.0)
        ]
        passes = (
            summary["mean_r"] is not None
            and summary["mean_r"] > 0
            and summary["profit_factor"] is not None
            and summary["profit_factor"] >= 1.20
            and summary["maximum_drawdown_r"] <= 6.0
            and bootstrap["lower_90_one_sided"] is not None
            and bootstrap["lower_90_one_sided"] > 0
            and validation["total_r"] > 0
            and holdout["total_r"] > 0
            and ten_bps is not None
            and ten_bps["total_r"] > 0
            and ten_bps["profit_factor"] is not None
            and ten_bps["profit_factor"] >= 1.20
            and ten_bps["maximum_drawdown_r"] <= 6.0
            and twenty_bps is not None
            and twenty_bps["total_r"] > 0
            and twenty_bps["profit_factor"] is not None
            and twenty_bps["profit_factor"] >= 1.20
            and twenty_bps["maximum_drawdown_r"] <= 6.0
            and all(
                value is not None and value["total_r"] > 0
                for value in base_cost_targets
            )
        )
        if passes:
            research = "promising_for_independent_confirmation"
        elif summary["mean_r"] is not None and summary["mean_r"] <= 0:
            research = "reject_or_redesign"
        else:
            research = "inconclusive"
    geometry = summary["stop_geometry"]
    policy = summary["policy"]
    if not summary["trades"]:
        deployment = "no_trade_evidence"
    elif policy["maximum_stop_fraction"] == 0.008:
        deployment = "production_stop_geometry_enforced"
    elif (
        geometry["at_or_below_0.8pct_fraction"] is not None
        and geometry["at_or_below_0.8pct_fraction"] < 0.50
    ):
        deployment = "production_incompatible_stop_geometry"
    else:
        deployment = "stop_geometry_needs_live_execution_confirmation"
    return research, deployment


def _result_identity(
    dataset: Mapping[str, Any],
    policies: Sequence[Policy],
    slippages: Sequence[float],
    targets: Sequence[float],
) -> dict[str, Any]:
    config = load_config()
    implementation_paths = (
        Path(__file__),
        PROJECT_ROOT / "historical_research.py",
        PROJECT_ROOT / "historical_research_strategies.py",
        PROJECT_ROOT / "strategy_engine.py",
    )
    identity = {
        "lab_version": LAB_VERSION,
        "dataset_hash": dataset["dataset_hash"],
        "evidence_sha256": dataset["evidence_sha256"],
        "production_strategy_version": config.version,
        "production_rules_hash": config.rules_hash,
        "policies": [asdict(value) for value in policies],
        "strategy_plugins": plugin_manifest_hash_input(
            tuple(dict.fromkeys(value.strategy_spec for value in policies))
        ),
        "execution_grid": {
            "slippage_bps": list(slippages),
            "target_r": list(targets),
            "force_flat_time_et": "15:50:00",
        },
        "implementations": {
            str(path.relative_to(PROJECT_ROOT)): _sha256_file(path)
            for path in implementation_paths
        },
    }
    identity["configuration_hash"] = _sha256_bytes(_canonical_json(identity))
    identity["run_id"] = (
        f"strategy-lab-{dataset['dataset_hash'][:12]}-"
        f"{identity['configuration_hash'][:12]}"
    )
    return identity


def run_lab(
    evidence_path: Path,
    *,
    data_root: Path = DEFAULT_DATA_ROOT,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    policies: Sequence[Policy] | None = None,
    slippages: Sequence[float] = (0.0, 5.0, 10.0, 20.0),
    targets: Sequence[float] = (1.0, 1.5, 2.0, 3.0),
    bootstrap_samples: int = 10_000,
    publish_prefix: Path | None = None,
) -> dict[str, Any]:
    started = time.perf_counter()
    chosen = tuple(policies or BUILTIN_POLICIES.values())
    if not chosen or len({value.policy_id for value in chosen}) != len(chosen):
        raise HistoricalStrategyLabError("policies must be non-empty and unique")
    for policy in chosen:
        policy.validate()
    slippages = tuple(float(value) for value in slippages)
    targets = tuple(float(value) for value in targets)
    if any(not math.isfinite(value) or value < 0 for value in slippages):
        raise HistoricalStrategyLabError("slippage grid must be finite and nonnegative")
    if any(not math.isfinite(value) or value <= 0 for value in targets):
        raise HistoricalStrategyLabError("target grid must be finite and positive")
    if bootstrap_samples < 100:
        raise HistoricalStrategyLabError("bootstrap samples must be at least 100")
    before = production_hashes()
    dataset = load_dataset(evidence_path, data_root)
    expected_frozen_hashes = _expected_frozen_hashes(evidence_path)
    observations, blocked = build_observations(dataset, chosen, expected_frozen_hashes)
    requested_dates = tuple(dataset["dates"])
    base_execution = ExecutionConfig()
    base_summaries = {
        policy.policy_id: summarize_policy(
            policy,
            requested_dates,
            observations,
            blocked,
            base_execution,
            bootstrap_samples=bootstrap_samples,
        )
        for policy in chosen
    }
    sensitivity: dict[str, list[dict[str, Any]]] = {
        policy.policy_id: [] for policy in chosen
    }
    for slippage in slippages:
        for target in targets:
            execution = ExecutionConfig(
                entry_slippage_bps=slippage,
                exit_slippage_bps=slippage,
                target_r=target,
            )
            execution.validate()
            for policy in chosen:
                summary = summarize_policy(
                    policy,
                    requested_dates,
                    observations,
                    blocked,
                    execution,
                    bootstrap_samples=100,
                )
                sensitivity[policy.policy_id].append(
                    {
                        "entry_slippage_bps": slippage,
                        "exit_slippage_bps": slippage,
                        "target_r": target,
                        **_compact_sensitivity(summary),
                    }
                )
    for policy_id, summary in base_summaries.items():
        research, deployment = _status(summary, sensitivity[policy_id])
        summary["research_status"] = research
        summary["deployment_status"] = deployment
    identity = _result_identity(dataset, chosen, slippages, targets)
    result = {
        "schema_version": SCHEMA_VERSION,
        "manifest": {
            **identity,
            "mode": "research_only",
            "automatic_strategy_application": False,
            "provider_requests": 0,
            "dataset": {
                "evidence_manifest": str(evidence_path),
                "dataset_hash": dataset["dataset_hash"],
                "requested_dates": len(requested_dates),
                "available_dates": dataset["available_dates"],
                "missing_dates": dataset["missing_dates"],
                "first_date": min(requested_dates),
                "last_date": max(requested_dates),
            },
            "sample_disclosure": {
                "independent_confirmation": False,
                "full_corpus_previously_inspected": True,
                "chronological_phases_are_retrospective_stability_checks": True,
                "promotion_evidence": False,
            },
        },
        "policy_summaries": [base_summaries[policy.policy_id] for policy in chosen],
        "sensitivity": sensitivity,
        "all_signal_diagnostics": _all_signal_diagnostics(observations, base_execution),
        "interpretation": {
            "promising_policies": [
                policy_id
                for policy_id, summary in base_summaries.items()
                if summary["research_status"]
                == "promising_for_independent_confirmation"
            ],
            "rejected_or_redesign_policies": [
                policy_id
                for policy_id, summary in base_summaries.items()
                if summary["research_status"] == "reject_or_redesign"
            ],
            "production_strategy_changed": False,
            "next_evidence_gate": "newly frozen dates not inspected in this run",
            "research_gate": {
                "base": "positive mean R, PF >= 1.20, max drawdown <= 6R, and one-sided 90% bootstrap lower mean R > 0",
                "retrospective_stability": "positive total R in both chronological validation and holdout phases",
                "cost_stress": "10 and 20 bps per side at 2R each require positive total R, PF >= 1.20, and max drawdown <= 6R",
                "target_stress": "1R, 1.5R, 2R, and 3R at 5 bps per side must each have positive total R",
                "independence": "still requires newly frozen dates; this corpus cannot promote a strategy",
            },
        },
        "runtime": {
            "elapsed_seconds": round(time.perf_counter() - started, 6),
            "provider_requests": 0,
            "candidate_strategy_observations": sum(
                len(value) for value in observations.values()
            ),
            "policy_cost_target_evaluations": (
                len(chosen) * len(slippages) * len(targets)
            ),
            "bootstrap_samples_for_base": bootstrap_samples,
        },
    }
    after = production_hashes()
    if before != after:
        changed = [key for key in before if before[key] != after[key]]
        raise HistoricalStrategyLabError(
            f"research run changed protected production artifacts: {changed}"
        )
    result["manifest"]["production_isolation"] = {
        "verified_unchanged": True,
        "paths": sorted(before),
    }
    output_path = _safe_publish_prefix(
        output_root / identity["run_id"] / "result"
    ).with_suffix(".json")
    _atomic_json(output_path, result)
    report = render_report(result)
    _atomic_text(output_path.with_name("report.md"), report)
    if publish_prefix is not None:
        prefix = _safe_publish_prefix(publish_prefix)
        _atomic_json(prefix.with_suffix(".json"), result)
        _atomic_text(prefix.with_suffix(".md"), report)
    return result


def _confirmation_dataset(
    manifest: Mapping[str, Any], data_root: Path
) -> dict[str, Any]:
    bundles: list[dict[str, Any]] = []
    for frozen in manifest["frozen_dates"]:
        day = str(frozen["date"])
        path = data_root / f"{day}.json"
        frozen_status = str(frozen.get("status", "validation_ready"))
        if frozen_status == "precollection_blocked" and path.is_file():
            raise HistoricalStrategyLabError(
                f"{day}: precollection-blocked date cannot receive a target bundle"
            )
        status = (
            "precollection_blocked"
            if frozen_status == "precollection_blocked"
            else "available"
            if path.is_file()
            else "missing"
        )
        bundles.append(
            {
                "date": day,
                "path": str(path),
                "status": status,
                "sha256": _sha256_file(path) if path.is_file() else None,
                "expected_symbols": list(frozen["ordered_symbols"]),
                "expected_filing_items": {
                    str(row["symbol"]): str(row.get("discovery", {}).get("filing_items", ""))
                    for row in frozen["ordered_candidates"]
                },
                "expected_frozen_hash": frozen["frozen_evidence_sha256"],
                "blocker_reason": (
                    frozen.get("precollection_blocker", {}).get("reason_code")
                    if frozen_status == "precollection_blocked"
                    else None
                ),
            }
        )
    identity = {
        "confirmation_manifest_sha256": manifest["manifest_sha256"],
        "bundles": [
            {
                "date": value["date"],
                "status": value["status"],
                "sha256": value["sha256"],
            }
            for value in bundles
        ],
    }
    return {
        "dates": [str(value["date"]) for value in manifest["frozen_dates"]],
        "bundles": bundles,
        "dataset_hash": _sha256_bytes(_canonical_json(identity)),
        "available_dates": sum(value["status"] == "available" for value in bundles),
        "missing_dates": sum(value["status"] != "available" for value in bundles),
    }


def _verify_confirmation_bundle(
    item: Mapping[str, Any], manifest: Mapping[str, Any]
) -> Mapping[str, Any] | None:
    bundle = _load_verified_bundle(
        item, expected_frozen_hash=str(item["expected_frozen_hash"])
    )
    if bundle is None:
        return None
    day = str(item["date"])
    registered = _parse_timestamp(
        manifest["registered_at"], "manifest.registered_at"
    )
    source = bundle["source"]
    captured = _parse_timestamp(source.get("captured_at"), f"{day} source.captured_at")
    if captured <= registered:
        raise HistoricalStrategyLabError(
            f"{day}: target-session capture must follow preregistration"
        )
    if bundle.get("sample_phase") != "confirmation":
        raise HistoricalStrategyLabError(
            f"{day}: confirmation bundle cannot be relabeled from another phase"
        )
    preregistration = bundle.get("preregistration")
    if not isinstance(preregistration, Mapping) or (
        preregistration.get("manifest_hash") != manifest["manifest_sha256"]
        or preregistration.get("registered_at") != manifest["registered_at"]
    ):
        raise HistoricalStrategyLabError(
            f"{day}: bundle preregistration does not match frozen manifest"
        )
    expected_filing_items = item.get("expected_filing_items", {})
    for raw in bundle["candidates"]:
        discovery = raw.get("discovery")
        items = (
            str(discovery.get("filing_items", ""))
            if isinstance(discovery, Mapping)
            else ""
        )
        symbol = str(raw.get("symbol", ""))
        if items != expected_filing_items.get(symbol):
            raise HistoricalStrategyLabError(
                f"{day} {symbol}: bundle filing-item evidence differs from frozen universe"
            )
    return bundle


def _confirmation_observations(
    dataset: Mapping[str, Any], manifest: Mapping[str, Any]
) -> tuple[dict[str, list[Observation]], dict[str, str]]:
    plugin = load_strategy(FROZEN_CONFIRMATION_POLICY.strategy_spec)
    config = load_config()
    observations: dict[str, list[Observation]] = defaultdict(list)
    blocked: dict[str, str] = {}
    for item in dataset["bundles"]:
        day = str(item["date"])
        bundle = _verify_confirmation_bundle(item, manifest)
        if bundle is None:
            blocked[day] = str(item.get("blocker_reason") or "bundle_missing")
            continue
        for raw in bundle["candidates"]:
            observations[day].append(_observation(day, raw, plugin, config))
    return dict(observations), blocked


def _fractional_drawdown(equities: Sequence[float]) -> float:
    if not equities:
        return 0.0
    peak = equities[0]
    maximum = 0.0
    for equity in equities:
        peak = max(peak, equity)
        if peak > 0:
            maximum = max(maximum, (peak - equity) / peak)
    return maximum


def _simulate_deployment(
    daily: Sequence[Mapping[str, Any]],
    *,
    maximum_stop_fraction: float | None = None,
    excluded_signal_ids: frozenset[str] = frozenset(),
) -> dict[str, Any]:
    assumptions = CONFIRMATION_DEPLOYMENT
    starting_equity = float(assumptions["synthetic_starting_equity"])
    equity = starting_equity
    equity_path = [equity]
    details: list[dict[str, Any]] = []
    stopped_slippage_bps: list[float] = []
    for row in daily:
        if row.get("status") != "trade":
            continue
        trade = row["trade"]
        signal_id = str(trade["signal_id"])
        if signal_id in excluded_signal_ids:
            continue
        entry = float(trade["entry_price"])
        structural_stop = float(trade["technical_stop"])
        stop_distance = entry - structural_stop
        stop_fraction = stop_distance / entry
        if maximum_stop_fraction is not None and stop_fraction > maximum_stop_fraction:
            continue
        reserve = entry * float(assumptions["stop_slippage_reserve_fraction"])
        risk_per_share = stop_distance + reserve
        risk_budget = equity * float(assumptions["account_risk_fraction"])
        buying_power = equity * float(
            assumptions["synthetic_buying_power_fraction"]
        )
        allocation_budget = (
            buying_power * float(assumptions["allocation_cap_fraction"])
        )
        q_risk = math.floor(risk_budget / risk_per_share)
        q_allocation = math.floor(allocation_budget / entry)
        quantity = min(q_risk, q_allocation)
        binding_cap = "risk" if q_risk <= q_allocation else "allocation"
        equity_before = equity
        pnl = quantity * (float(trade["exit_price"]) - entry)
        equity += pnl
        account_return = pnl / equity_before if equity_before else 0.0
        allocation = quantity * entry / equity_before if equity_before else 0.0
        planned_loss_fraction = (
            quantity * risk_per_share / equity_before if equity_before else 0.0
        )
        if quantity > 0 and planned_loss_fraction > float(
            assumptions["account_risk_fraction"]
        ) + 1e-12:
            raise HistoricalStrategyLabError(
                f"{signal_id}: whole-share sizing exceeded the account-risk cap"
            )
        stop_slippage_bps = None
        if "stop" in str(trade["exit_reason"]):
            stop_slippage_bps = max(
                0.0,
                (structural_stop - float(trade["exit_price"])) / entry * 10_000.0,
            )
            stopped_slippage_bps.append(stop_slippage_bps)
        details.append(
            {
                "date": row["date"],
                "signal_id": signal_id,
                "symbol": trade["symbol"],
                "structural_stop": structural_stop,
                "deployed_stop": structural_stop,
                "stop_compressed": False,
                "stop_fraction": round(stop_fraction, 8),
                "reserve_fraction": assumptions[
                    "stop_slippage_reserve_fraction"
                ],
                "q_risk": q_risk,
                "q_allocation": q_allocation,
                "quantity": quantity,
                "binding_cap": binding_cap,
                "planned_loss_fraction": round(planned_loss_fraction, 8),
                "allocation_fraction": round(allocation, 8),
                "allocation_shortfall_fraction": round(
                    max(
                        0.0,
                        float(assumptions["allocation_objective_floor_fraction"])
                        - allocation,
                    ),
                    8,
                ),
                "modeled_stop_slippage_bps": (
                    round(stop_slippage_bps, 8)
                    if stop_slippage_bps is not None
                    else None
                ),
                "account_return_fraction": round(account_return, 10),
                "equity_before": round(equity_before, 8),
                "equity_after": round(equity, 8),
            }
        )
        equity_path.append(equity)
    returns = [float(value["account_return_fraction"]) for value in details]
    allocations = [float(value["allocation_fraction"]) for value in details]
    shortfalls = [
        float(value["allocation_shortfall_fraction"]) for value in details
    ]
    stops = [float(value["stop_fraction"]) for value in details]
    log_returns = [math.log1p(value) for value in returns if value > -1.0]
    return {
        "trades": len(details),
        "starting_equity": starting_equity,
        "ending_equity": round(equity, 8),
        "compounded_account_return_fraction": round(
            equity / starting_equity - 1.0, 10
        ),
        "mean_account_return_fraction": (
            round(statistics.fmean(returns), 10) if returns else None
        ),
        "expected_log_growth_per_trade": (
            round(statistics.fmean(log_returns), 10) if log_returns else None
        ),
        "total_log_growth": round(sum(log_returns), 10),
        "peak_to_trough_account_drawdown_fraction": round(
            _fractional_drawdown(equity_path), 10
        ),
        "median_allocation_fraction": (
            round(statistics.median(allocations), 8) if allocations else None
        ),
        "p90_allocation_fraction": (
            round(_quantile(allocations, 0.90), 8) if allocations else None
        ),
        "median_allocation_shortfall_fraction": (
            round(statistics.median(shortfalls), 8) if shortfalls else None
        ),
        "p90_allocation_shortfall_fraction": (
            round(_quantile(shortfalls, 0.90), 8) if shortfalls else None
        ),
        "median_stop_fraction": (
            round(statistics.median(stops), 8) if stops else None
        ),
        "p90_stop_fraction": (
            round(_quantile(stops, 0.90), 8) if stops else None
        ),
        "median_modeled_stop_slippage_bps": (
            round(statistics.median(stopped_slippage_bps), 8)
            if stopped_slippage_bps
            else None
        ),
        "p90_modeled_stop_slippage_bps": (
            round(_quantile(stopped_slippage_bps, 0.90), 8)
            if stopped_slippage_bps
            else None
        ),
        "stop_compressions": sum(value["stop_compressed"] for value in details),
        "risk_cap_violations": sum(
            value["planned_loss_fraction"]
            > float(assumptions["account_risk_fraction"]) + 1e-12
            for value in details
        ),
        "details": details,
    }


def _deployment_views(daily: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    structural = _simulate_deployment(daily)
    largest_gains = frozenset(
        value["signal_id"]
        for value in sorted(
            structural["details"],
            key=lambda row: (-float(row["account_return_fraction"]), row["signal_id"]),
        )[:5]
    )
    structural["performance_without_largest_five_gains"] = _simulate_deployment(
        daily, excluded_signal_ids=largest_gains
    )
    structural["largest_five_gain_signal_ids"] = sorted(largest_gains)
    compatible = _simulate_deployment(
        daily,
        maximum_stop_fraction=float(
            CONFIRMATION_DEPLOYMENT["production_compatible_stop_fraction"]
        ),
    )
    compatible["minimum_trades_for_inference"] = CONFIRMATION_DEPLOYMENT[
        "minimum_production_compatible_trades_for_inference"
    ]
    compatible["sufficient_for_inference"] = compatible["trades"] >= int(
        compatible["minimum_trades_for_inference"]
    )
    compatible["wider_stops_tightened_into_cohort"] = 0
    compatible_largest_gains = frozenset(
        value["signal_id"]
        for value in sorted(
            compatible["details"],
            key=lambda row: (-float(row["account_return_fraction"]), row["signal_id"]),
        )[:5]
    )
    compatible["performance_without_largest_five_gains"] = _simulate_deployment(
        daily,
        maximum_stop_fraction=float(
            CONFIRMATION_DEPLOYMENT["production_compatible_stop_fraction"]
        ),
        excluded_signal_ids=compatible_largest_gains,
    )
    compatible["largest_five_gain_signal_ids"] = sorted(compatible_largest_gains)
    return {
        "assumptions": CONFIRMATION_DEPLOYMENT,
        "structural_stop_risk_sized": structural,
        "naturally_production_compatible_stop_cohort": compatible,
    }


def _profit_factor_pass(summary: Mapping[str, Any], minimum: float) -> bool:
    value = summary.get("profit_factor")
    if value is None:
        return bool(summary.get("total_r", 0) > 0)
    return float(value) >= minimum


def _confirmation_acceptance(
    primary: Mapping[str, Any], sensitivity: Sequence[Mapping[str, Any]]
) -> dict[str, Any]:
    thresholds = CONFIRMATION_ACCEPTANCE_THRESHOLDS
    daily = primary["daily"]
    midpoint = len(daily) // 2
    halves = []
    for label, rows in (("first", daily[:midpoint]), ("second", daily[midpoint:])):
        values = [
            float(value["trade"]["net_r"])
            for value in rows
            if value["status"] == "trade"
        ]
        halves.append({"half": label, **_return_stats(values)})

    def cell(slippage: float, target: float) -> Mapping[str, Any]:
        return next(
            value
            for value in sensitivity
            if value["entry_slippage_bps"] == slippage
            and value["exit_slippage_bps"] == slippage
            and value["target_r"] == target
        )

    gates: dict[str, dict[str, Any]] = {}

    def gate(name: str, passed: bool, actual: Any, required: str) -> None:
        gates[name] = {"passed": bool(passed), "actual": actual, "required": required}

    gate(
        "requested_dates",
        primary["requested_days"] >= int(thresholds["minimum_requested_dates"]),
        primary["requested_days"],
        f">={thresholds['minimum_requested_dates']}",
    )
    gate(
        "validation_grade_dates",
        primary["covered_days"]
        >= int(thresholds["minimum_validation_grade_dates"]),
        primary["covered_days"],
        f">={thresholds['minimum_validation_grade_dates']}",
    )
    gate(
        "executed_signals",
        primary["trades"] >= int(thresholds["minimum_executed_signals"]),
        primary["trades"],
        f">={thresholds['minimum_executed_signals']}",
    )
    gate("primary_expectancy", bool(primary["mean_r"] and primary["mean_r"] > 0), primary["mean_r"], ">0")
    gate(
        "primary_profit_factor",
        _profit_factor_pass(primary, float(thresholds["primary_minimum_profit_factor"])),
        primary["profit_factor"],
        f">={thresholds['primary_minimum_profit_factor']}",
    )
    gate(
        "primary_drawdown",
        primary["maximum_drawdown_r"] <= float(thresholds["maximum_drawdown_r"]),
        primary["maximum_drawdown_r"],
        f"<={thresholds['maximum_drawdown_r']}",
    )
    lower = primary["bootstrap_trade_mean"]["lower_90_one_sided"]
    gate("bootstrap_lower_mean_r", lower is not None and lower > 0, lower, ">0")
    gate(
        "chronological_halves",
        all(value["total_r"] > 0 for value in halves),
        halves,
        "positive total R in both halves",
    )
    without_five = primary["concentration"]["total_without_top_five_r"]
    gate(
        "without_best_five",
        without_five is not None and without_five > 0,
        without_five,
        ">0 total R",
    )
    for slippage in (10.0, 20.0):
        value = cell(slippage, 2.0)
        gate(
            f"cost_stress_{int(slippage)}bps",
            value["total_r"] > 0
            and _profit_factor_pass(
                value, float(thresholds["stress_minimum_profit_factor"])
            )
            and value["maximum_drawdown_r"]
            <= float(thresholds["maximum_drawdown_r"]),
            dict(value),
            "positive total R, PF>=1.20, drawdown<=6R",
        )
    target_cells = [cell(5.0, target) for target in (1.0, 1.5, 2.0, 3.0)]
    gate(
        "all_frozen_targets",
        all(value["total_r"] > 0 for value in target_cells),
        [dict(value) for value in target_cells],
        "positive total R at 1R, 1.5R, 2R, and 3R",
    )
    minimums_passed = all(
        gates[value]["passed"]
        for value in ("requested_dates", "validation_grade_dates", "executed_signals")
    )
    all_passed = all(value["passed"] for value in gates.values())
    return {
        "status": (
            "passed"
            if all_passed
            else "rejected"
            if minimums_passed
            else "insufficient_independent_evidence"
        ),
        "all_passed": all_passed,
        "gates": gates,
    }


def run_confirmation(
    manifest_path: Path,
    *,
    data_root: Path = DEFAULT_DATA_ROOT,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    bootstrap_samples: int = 10_000,
    publish_prefix: Path | None = None,
) -> dict[str, Any]:
    """Evaluate exactly one preregistered policy on post-freeze bundles."""

    if bootstrap_samples < 100:
        raise HistoricalStrategyLabError("bootstrap samples must be at least 100")
    started = time.perf_counter()
    before = production_hashes()
    manifest = load_confirmation_manifest(manifest_path.resolve())
    dataset = _confirmation_dataset(manifest, data_root)
    observations, blocked = _confirmation_observations(dataset, manifest)
    dates = tuple(dataset["dates"])
    primary_execution = ExecutionConfig(
        entry_slippage_bps=5.0,
        exit_slippage_bps=5.0,
        target_r=2.0,
        force_flat_time_et="15:50:00",
    )
    primary = summarize_policy(
        FROZEN_CONFIRMATION_POLICY,
        dates,
        observations,
        blocked,
        primary_execution,
        bootstrap_samples=bootstrap_samples,
        include_daily=True,
    )
    sensitivity: list[dict[str, Any]] = []
    for slippage in CONFIRMATION_EXECUTION_GRID["slippage_bps_per_side"]:
        for target in CONFIRMATION_EXECUTION_GRID["target_r"]:
            execution = ExecutionConfig(
                entry_slippage_bps=float(slippage),
                exit_slippage_bps=float(slippage),
                target_r=float(target),
                force_flat_time_et="15:50:00",
            )
            summary = summarize_policy(
                FROZEN_CONFIRMATION_POLICY,
                dates,
                observations,
                blocked,
                execution,
                bootstrap_samples=100,
            )
            sensitivity.append(
                {
                    "entry_slippage_bps": float(slippage),
                    "exit_slippage_bps": float(slippage),
                    "target_r": float(target),
                    **_compact_sensitivity(summary),
                }
            )
    acceptance = _confirmation_acceptance(primary, sensitivity)
    deployment = _deployment_views(primary["daily"])
    decision = (
        "advance_structural_stop_risk_sized_arm_to_shadow"
        if acceptance["all_passed"]
        else "stop_without_threshold_tuning"
    )
    result_id = (
        f"strategy-confirmation-{manifest['manifest_sha256'][:12]}-"
        f"{dataset['dataset_hash'][:12]}"
    )
    result = {
        "schema_version": CONFIRMATION_SCHEMA_VERSION,
        "manifest": {
            "result_id": result_id,
            "confirmation_manifest": _display_path(manifest_path),
            "confirmation_manifest_sha256": manifest["manifest_sha256"],
            "confirmation_contract_id": CONFIRMATION_CONTRACT_ID,
            "registered_at": manifest["registered_at"],
            "dataset_hash": dataset["dataset_hash"],
            "requested_dates": len(dates),
            "available_dates": dataset["available_dates"],
            "missing_dates": dataset["missing_dates"],
            "independent_confirmation": True,
            "previously_inspected_date_overlap": 0,
            "provider_requests": 0,
            "automatic_strategy_application": False,
            "broker_actions_allowed": False,
        },
        "policy_result": primary,
        "execution_cost_target_cells": sensitivity,
        "acceptance": acceptance,
        "deployment_views": deployment,
        "decision": {
            "historical_confirmation": acceptance["status"],
            "next_stage": decision,
            "production_maximum_stop_changed": False,
            "production_strategy_changed": False,
            "production_compatible_cohort_is_separate": True,
        },
        "runtime": {
            "elapsed_seconds": round(time.perf_counter() - started, 6),
            "provider_requests": 0,
            "policy_count": 1,
            "cost_target_cells": len(sensitivity),
            "bootstrap_samples": bootstrap_samples,
        },
    }
    after = production_hashes()
    if before != after:
        changed = [key for key in before if before[key] != after[key]]
        raise HistoricalStrategyLabError(
            f"confirmation changed protected production artifacts: {changed}"
        )
    result["manifest"]["production_isolation"] = {
        "verified_unchanged": True,
        "paths": sorted(before),
    }
    output_path = _safe_publish_prefix(
        output_root / result_id / "result"
    ).with_suffix(".json")
    _atomic_json(output_path, result)
    report = render_confirmation_report(result)
    _atomic_text(output_path.with_name("report.md"), report)
    if publish_prefix is not None:
        prefix = _safe_publish_prefix(publish_prefix)
        _atomic_json(prefix.with_suffix(".json"), result)
        _atomic_text(prefix.with_suffix(".md"), report)
    return result


def render_confirmation_report(result: Mapping[str, Any]) -> str:
    manifest = result["manifest"]
    primary = result["policy_result"]
    structural = result["deployment_views"]["structural_stop_risk_sized"]
    compatible = result["deployment_views"][
        "naturally_production_compatible_stop_cohort"
    ]
    gates = result["acceptance"]["gates"]
    lines = [
        "# Independent Early Item 2.02 Reversal Confirmation",
        "",
        f"Result ID: `{manifest['result_id']}`",
        "",
        "## Evidence Boundary",
        "",
        f"- Preregistered manifest: `{manifest['confirmation_manifest_sha256']}`",
        f"- Requested dates: {manifest['requested_dates']}",
        f"- Validation-grade dates: {manifest['available_dates']}",
        f"- Missing dates retained as blockers: {manifest['missing_dates']}",
        "- Prior inspected-date overlap: 0",
        "- Policies executed: 1",
        "- Provider and broker actions during evaluation: 0",
        "- Production strategy changes: 0",
        "",
        "## Primary 5 bps / 2R Result",
        "",
        f"- Trades: {primary['trades']}",
        f"- Mean R: {_format_number(primary['mean_r'])}",
        f"- Total R: {_format_number(primary['total_r'])}",
        f"- Profit factor: {_format_number(primary['profit_factor'])}",
        f"- Maximum drawdown: {_format_number(primary['maximum_drawdown_r'])}R",
        f"- One-sided 90% bootstrap lower mean R: {_format_number(primary['bootstrap_trade_mean']['lower_90_one_sided'])}",
        "",
        "## Acceptance",
        "",
        f"Overall: `{result['acceptance']['status']}`",
        "",
        "| Gate | Pass | Actual | Required |",
        "|---|---|---|---|",
    ]
    for name, gate in gates.items():
        actual = gate["actual"]
        if isinstance(actual, (dict, list)):
            actual = "see JSON"
        lines.append(
            f"| {name} | {'yes' if gate['passed'] else 'no'} | {actual} | {gate['required']} |"
        )
    lines.extend(
        [
            "",
            "## Risk-Sized Deployment Geometry",
            "",
            f"- Structural-arm trades: {structural['trades']}",
            f"- Compounded account return: {_format_percent(structural['compounded_account_return_fraction'])}",
            f"- Expected log growth per trade: {_format_number(structural['expected_log_growth_per_trade'])}",
            f"- Peak-to-trough account drawdown: {_format_percent(structural['peak_to_trough_account_drawdown_fraction'])}",
            f"- Median allocation: {_format_percent(structural['median_allocation_fraction'])}",
            f"- P90 allocation: {_format_percent(structural['p90_allocation_fraction'])}",
            f"- Structural-stop compressions: {structural['stop_compressions']}",
            f"- Risk-cap violations: {structural['risk_cap_violations']}",
            f"- Naturally <=0.8% cohort: {compatible['trades']} trades; inference sufficient: {'yes' if compatible['sufficient_for_inference'] else 'no'}",
            "",
            f"Decision: `{result['decision']['next_stage']}`.",
            "",
            "A failed gate stops advancement and does not authorize tuning on this sample. Passing advances only the unchanged structural-stop, risk-sized arm to prospective shadow qualification.",
            "",
        ]
    )
    return "\n".join(lines)


def render_report(result: Mapping[str, Any]) -> str:
    manifest = result["manifest"]
    summaries = result["policy_summaries"]
    diagnostics = result["all_signal_diagnostics"]
    by_id = {value["policy"]["policy_id"]: value for value in summaries}

    def sensitivity_point(policy_id: str, slippage_bps: float, target_r: float) -> Any:
        return next(
            (
                value
                for value in result["sensitivity"].get(policy_id, ())
                if value["entry_slippage_bps"] == slippage_bps
                and value["exit_slippage_bps"] == slippage_bps
                and value["target_r"] == target_r
            ),
            None,
        )

    lines = [
        "# Production-Aware Historical Strategy Lab",
        "",
        f"Run ID: `{manifest['run_id']}`",
        "",
        "## Scope And Boundary",
        "",
        f"- Requested dates: {manifest['dataset']['requested_dates']}",
        f"- Available immutable bundles: {manifest['dataset']['available_dates']}",
        f"- Missing bundles retained as blockers: {manifest['dataset']['missing_dates']}",
        f"- Dataset hash: `{manifest['dataset']['dataset_hash']}`",
        "- Provider requests: 0",
        "- Production strategy/configuration changes: 0",
        "- Independent confirmation: no; every split is a retrospective stability check.",
        "- Full tested family: all policy, cost, and target cells are retained in the JSON result.",
        "",
        "## Declared Adversarial Gate",
        "",
        "A policy is only `promising_for_independent_confirmation` when all of the following hold:",
        "",
        "- Base 5 bps-per-side, 2R result: positive mean R, PF at least 1.20, maximum drawdown at most 6R, and one-sided 90% bootstrap lower mean R above zero.",
        "- Positive total R in both retrospective chronological validation and holdout phases.",
        "- At both 10 and 20 bps per side with a 2R target: positive total R, PF at least 1.20, and maximum drawdown at most 6R.",
        "- Positive total R at 1R, 1.5R, 2R, and 3R targets under 5 bps-per-side costs.",
        "- Passing is a freeze-and-confirm signal, never promotion evidence.",
        "",
        "## Base Policy Results",
        "",
        "| Policy | Trades | Mean R | Total R | PF | Max DD R | 90% lower mean R | Research status | Deployment |",
        "|---|---:|---:|---:|---:|---:|---:|---|---|",
    ]
    for summary in summaries:
        bootstrap = summary["bootstrap_trade_mean"]
        lines.append(
            "| {policy} | {trades} | {mean} | {total} | {pf} | {dd} | {lower} | {status} | {deployment} |".format(
                policy=summary["policy"]["policy_id"],
                trades=summary["trades"],
                mean=_format_number(summary["mean_r"]),
                total=_format_number(summary["total_r"]),
                pf=_format_number(summary["profit_factor"]),
                dd=_format_number(summary["maximum_drawdown_r"]),
                lower=_format_number(bootstrap["lower_90_one_sided"]),
                status=summary["research_status"],
                deployment=summary["deployment_status"],
            )
        )
    promising = result["interpretation"]["promising_policies"]
    lines.extend(
        [
            "",
            "## Survivors Under Cost Stress",
            "",
            "| Policy | 10 bps Total R | 10 bps PF | 20 bps Total R | 20 bps PF | 20 bps DD R | Median stop | <=0.8% stops | Implied median notional |",
            "|---|---:|---:|---:|---:|---:|---:|---:|---:|",
        ]
    )
    for policy_id in promising:
        summary = by_id[policy_id]
        ten = sensitivity_point(policy_id, 10.0, 2.0)
        twenty = sensitivity_point(policy_id, 20.0, 2.0)
        geometry = summary["stop_geometry"]
        lines.append(
            "| {policy} | {ten_total} | {ten_pf} | {twenty_total} | {twenty_pf} | {twenty_dd} | {stop} | {tight} | {notional} |".format(
                policy=policy_id,
                ten_total=_format_number(ten["total_r"] if ten else None),
                ten_pf=_format_number(ten["profit_factor"] if ten else None),
                twenty_total=_format_number(twenty["total_r"] if twenty else None),
                twenty_pf=_format_number(twenty["profit_factor"] if twenty else None),
                twenty_dd=_format_number(
                    twenty["maximum_drawdown_r"] if twenty else None
                ),
                stop=_format_percent(geometry["median_fraction"]),
                tight=_format_percent(geometry["at_or_below_0.8pct_fraction"]),
                notional=_format_percent(
                    geometry["median_implied_unvalidated_notional_fraction"]
                ),
            )
        )
    if not promising:
        lines.append("| None | n/a | n/a | n/a | n/a | n/a | n/a | n/a | n/a |")
    lines.extend(
        [
            "",
            "The stop and implied-notional columns are the decisive deployment warning. A policy can have positive R expectancy while remaining incompatible with the current 0.8% production stop cap and 70-80% allocation objective.",
            "",
            "## Production Gate And Timing Diagnostics",
            "",
            f"- Production candidate evaluations: {diagnostics['production_candidate_evaluations']}",
            f"- Production-eligible candidates: {diagnostics['production_eligible']}",
            f"- Executable ORB counterfactual signals: {diagnostics['executable_orb_counterfactual_signals']}",
            f"- Chase-rejected executable ORBs: {diagnostics['chase_rejected_executable_orb_signals']} ({_format_percent(diagnostics['chase_rejected_fraction'])})",
            f"- Median production evaluation delay from signal-bar start: {diagnostics['evaluation_delay_seconds']['from_signal_bar_start_median']} seconds",
            f"- Median production evaluation delay from research entry-bar start: {diagnostics['evaluation_delay_seconds']['from_research_entry_bar_start_median']} seconds",
            "",
            "The delay uses one-minute bar-start timestamps and is not observed live latency. It demonstrates why the stored production adapter and the next-open research model are not interchangeable.",
            "",
            "Gate attribution is descriptive, not causal. In particular, the fixed-slippage minute-bar model cannot disprove live spread, freshness, depth, chase, or protection controls. It can identify gates whose historical labels deserve a cleaner prospective test.",
            "",
            "## Theory Decisions",
            "",
        ]
    )
    if "reversal-early-earnings" in by_id:
        preferred = by_id["reversal-early-earnings"]
        phases = preferred["chronological_phases"]
        twenty = sensitivity_point("reversal-early-earnings", 20.0, 2.0)
        lines.extend(
            [
                "1. **Freeze the simple early earnings reversal for independent confirmation.** It produced {trades} trades, {total}R, PF {pf}, and {dd}R maximum drawdown; development, retrospective validation, and retrospective holdout were all positive ({dev}R, {validation}R, {holdout}R). At 20 bps per side it retained {stress}R and PF {stress_pf}.".format(
                    trades=preferred["trades"],
                    total=_format_number(preferred["total_r"]),
                    pf=_format_number(preferred["profit_factor"]),
                    dd=_format_number(preferred["maximum_drawdown_r"]),
                    dev=_format_number(phases["development"]["total_r"]),
                    validation=_format_number(
                        phases["retrospective_validation"]["total_r"]
                    ),
                    holdout=_format_number(phases["retrospective_holdout"]["total_r"]),
                    stress=_format_number(twenty["total_r"] if twenty else None),
                    stress_pf=_format_number(
                        twenty["profit_factor"] if twenty else None
                    ),
                ),
                "2. **Keep the current production ORB frozen.** The complete production gate stack selected zero trades. Relaxed ORB variants did trade, but none cleared the statistical and severe-cost gate.",
                "3. **Do not use production score or RVOL ranking as reversal selectors.** They were tested on the same corpus, add complexity, and failed the 20 bps PF/drawdown gate; simple signal strength is the safer comparator.",
                "4. **Reject the 0.8% stop as a drop-in fit for these stored signals.** The tight-stop early ORB lost money, and only {tight_count} of {trades} preferred reversal trades fit the current cap. The preferred cohort's median structural stop was {median_stop}, implying only {notional} median notional at the current 0.25% risk budget plus reserve.".format(
                    tight_count=preferred["stop_geometry"]["at_or_below_0.8pct"],
                    trades=preferred["trades"],
                    median_stop=_format_percent(
                        preferred["stop_geometry"]["median_fraction"]
                    ),
                    notional=_format_percent(
                        preferred["stop_geometry"][
                            "median_implied_unvalidated_notional_fraction"
                        ]
                    ),
                ),
                "5. **Retire the current VWAP-pullback branch and deprioritize HOD continuation.** The former had negative expectancy; the latter was near flat with drawdown above the production maturity budget. More tuning on this inspected corpus would spend learning capacity on overfit risk.",
            ]
        )
    lines.extend(
        [
            "",
            "## Selection Artifact Warning",
            "",
        ]
    )
    orb_counts = [
        sensitivity_point("orb-early-strength", slippage, 2.0)
        for slippage in (0.0, 5.0, 10.0, 20.0)
    ]
    if all(value is not None for value in orb_counts):
        lines.append(
            "The early ORB trade count fell from {zero} at 0 bps to {five}, {ten}, and {twenty} at 5, 10, and 20 bps. Modest added slippage can therefore appear to improve ORB returns by pushing entries past the chase cap and deleting trades. This is a selection artifact, not evidence that worse execution helps.".format(
                zero=orb_counts[0]["trades"],
                five=orb_counts[1]["trades"],
                ten=orb_counts[2]["trades"],
                twenty=orb_counts[3]["trades"],
            )
        )
    else:
        lines.append(
            "The requested grid did not include every standard ORB cost point, so the chase-cap selection artifact could not be fully evaluated."
        )
    lines.extend(
        [
            "",
            "Promising for independent confirmation:",
            "",
        ]
    )
    lines.extend(
        [f"- `{value}`" for value in promising]
        if promising
        else ["- None met every declared research gate."]
    )
    lines.extend(
        [
            "",
            "Reject or redesign:",
            "",
        ]
    )
    rejected = result["interpretation"]["rejected_or_redesign_policies"]
    lines.extend(
        [f"- `{value}`" for value in rejected]
        if rejected
        else ["- None were automatically rejected."]
    )
    lines.extend(
        [
            "",
            "## Required Next Gate",
            "",
            "Freeze new dates and symbols before viewing their target-session outcomes. Re-run only the exact simple early-earnings-reversal confirmation contract, retain every blocked date, collect full-universe and subminute quote/execution evidence where available, and do not edit production rules until both independent expectancy and deployable stop/protection geometry pass.",
            "",
        ]
    )
    return "\n".join(lines)


def _format_number(value: Any) -> str:
    if value is None:
        return "n/a"
    return f"{float(value):.3f}"


def _format_percent(value: Any) -> str:
    if value is None:
        return "n/a"
    return f"{100 * float(value):.1f}%"


def _parse_csv_numbers(value: str, *, positive: bool) -> tuple[float, ...]:
    try:
        numbers = tuple(float(part.strip()) for part in value.split(","))
    except ValueError as exc:
        raise argparse.ArgumentTypeError("expected comma-separated numbers") from exc
    if not numbers or any(
        not math.isfinite(number) or (number <= 0 if positive else number < 0)
        for number in numbers
    ):
        qualifier = "positive" if positive else "nonnegative"
        raise argparse.ArgumentTypeError(f"numbers must be finite and {qualifier}")
    return numbers


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("list-policies", help="list versioned built-in policies")
    run = subparsers.add_parser("run", help="run the evidence-bound strategy lab")
    run.add_argument("--evidence", type=Path, default=DEFAULT_EVIDENCE)
    run.add_argument("--data-root", type=Path, default=DEFAULT_DATA_ROOT)
    run.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    run.add_argument("--policy", action="append", dest="policies")
    run.add_argument(
        "--slippage-grid",
        type=lambda value: _parse_csv_numbers(value, positive=False),
        default=(0.0, 5.0, 10.0, 20.0),
    )
    run.add_argument(
        "--target-grid",
        type=lambda value: _parse_csv_numbers(value, positive=True),
        default=(1.0, 1.5, 2.0, 3.0),
    )
    run.add_argument("--bootstrap-samples", type=int, default=10_000)
    run.add_argument("--publish-prefix", type=Path)
    freeze = subparsers.add_parser(
        "freeze-confirmation",
        help="freeze a hash-addressed independent-confirmation manifest",
    )
    freeze.add_argument("--evidence", type=Path, required=True)
    freeze.add_argument("--exclude-result", type=Path, action="append")
    freeze.add_argument("--output-root", type=Path, default=DEFAULT_CONFIRMATION_ROOT)
    freeze.add_argument("--registered-at")
    validate = subparsers.add_parser(
        "validate-confirmation",
        help="verify a frozen confirmation manifest and implementation identity",
    )
    validate.add_argument("manifest", type=Path)
    confirm = subparsers.add_parser(
        "run-confirmation",
        help="evaluate the sole frozen policy on post-preregistration bundles",
    )
    confirm.add_argument("manifest", type=Path)
    confirm.add_argument("--data-root", type=Path, default=DEFAULT_DATA_ROOT)
    confirm.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    confirm.add_argument("--bootstrap-samples", type=int, default=10_000)
    confirm.add_argument("--publish-prefix", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        if args.command == "list-policies":
            print(
                json.dumps(
                    [asdict(value) for value in BUILTIN_POLICIES.values()],
                    indent=2,
                    sort_keys=True,
                )
            )
            return 0
        if args.command == "freeze-confirmation":
            excluded = tuple(args.exclude_result or (DEFAULT_EXCLUDED_RESULT,))
            path, manifest = freeze_confirmation_manifest(
                args.evidence,
                excluded_result_paths=excluded,
                output_root=args.output_root,
                registered_at=args.registered_at,
            )
            print(
                json.dumps(
                    {
                        "manifest": str(path),
                        "manifest_sha256": manifest["manifest_sha256"],
                        "registered_at": manifest["registered_at"],
                        "frozen_dates": len(manifest["frozen_dates"]),
                        "broker_actions_allowed": False,
                    },
                    indent=2,
                    sort_keys=True,
                )
            )
            return 0
        if args.command == "validate-confirmation":
            manifest = load_confirmation_manifest(args.manifest.resolve())
            print(
                json.dumps(
                    {
                        "manifest_sha256": manifest["manifest_sha256"],
                        "registered_at": manifest["registered_at"],
                        "frozen_dates": len(manifest["frozen_dates"]),
                        "status": "valid",
                    },
                    indent=2,
                    sort_keys=True,
                )
            )
            return 0
        if args.command == "run-confirmation":
            result = run_confirmation(
                args.manifest,
                data_root=args.data_root,
                output_root=args.output_root,
                bootstrap_samples=args.bootstrap_samples,
                publish_prefix=args.publish_prefix,
            )
            print(
                json.dumps(
                    {
                        "result_id": result["manifest"]["result_id"],
                        "acceptance": result["acceptance"]["status"],
                        "next_stage": result["decision"]["next_stage"],
                        "provider_requests": 0,
                        "production_strategy_changed": False,
                    },
                    indent=2,
                    sort_keys=True,
                )
            )
            return 0
        policies = None
        if args.policies:
            missing = [
                value for value in args.policies if value not in BUILTIN_POLICIES
            ]
            if missing:
                raise HistoricalStrategyLabError(f"unknown policies: {missing}")
            policies = tuple(BUILTIN_POLICIES[value] for value in args.policies)
        result = run_lab(
            args.evidence,
            data_root=args.data_root,
            output_root=args.output_root,
            policies=policies,
            slippages=args.slippage_grid,
            targets=args.target_grid,
            bootstrap_samples=args.bootstrap_samples,
            publish_prefix=args.publish_prefix,
        )
        print(
            json.dumps(
                {
                    "run_id": result["manifest"]["run_id"],
                    "dataset_hash": result["manifest"]["dataset"]["dataset_hash"],
                    "promising_policies": result["interpretation"][
                        "promising_policies"
                    ],
                    "rejected_or_redesign_policies": result["interpretation"][
                        "rejected_or_redesign_policies"
                    ],
                    "provider_requests": 0,
                    "production_strategy_changed": False,
                    "elapsed_seconds": result["runtime"]["elapsed_seconds"],
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    except (
        HistoricalStrategyLabError,
        HistoricalResearchError,
        ResearchStrategyError,
        OSError,
        ValueError,
    ) as exc:
        print(
            json.dumps({"error": str(exc), "error_type": type(exc).__name__}, indent=2)
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

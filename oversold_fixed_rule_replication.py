"""Freeze and publish the selection-corrected oversold fixed-rule replication.

This successor does not search another parameter grid.  It deterministically
selects the only prior oversold trial that retained positive 20-bps growth,
profit factor, drawdown, both chronological halves, concentration robustness,
and a positive stationary-bootstrap lower bound across the combined 2023-2025
evidence.  The current evaluation is one exact rule, while all 32 prior trial
statistics remain in DSR, Holm, and PBO.

The boundary command reads only already-inspected 09:35 scanner artifacts.  It
does not open full-session inputs for newly assigned development pairs and it
does not access confirmation outcomes.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import io
import json
import math
import os
import statistics
from collections.abc import Mapping, Sequence
from datetime import datetime
from pathlib import Path
from statistics import NormalDist
from typing import Any

import dense_strategy_runtime as runtime
import equity_gap_continuation_validation as gap
import outcome_exposure
import portfolio_maturity
import strategy_discovery
from historical_store import (
    HistoricalDayStore,
    canonical_sha256,
    sha256_file,
)
from learning_data import (
    freeze_dataset_contract,
    load_frozen_dataset_contract,
    validate_dataset_contract,
)
from learning_experiment import DEVELOPMENT_SEARCH_RULE
from learning_statistics import (
    annualized_sharpe,
    maximum_drawdown_fraction,
    probability_of_backtest_overfitting,
    profit_factor,
    stationary_bootstrap_summary,
)


PROJECT_ROOT = Path(__file__).resolve().parent
CAMPAIGN_ID = portfolio_maturity.V2_CAMPAIGN_ID
FAMILY_ID = runtime.OVERSOLD_REVERSAL_FAMILY
MECHANISM_FAMILY = "short-horizon-oversold-reversal"
STRATEGY_ID = MECHANISM_FAMILY
SUCCESSOR_ID = "short-horizon-oversold-reversal-v7-fixed-rule-replication"
DATASET_ID = f"dataset-{SUCCESSOR_ID}-development"
EXPERIMENT_ID = f"experiment-{SUCCESSOR_ID}"
SELECTED_PRIOR_TRIAL_ID = "trial-524596217b6ec76a"
SELECTED_PARAMETERS = {
    "lookback_minutes": 15,
    "rsi_maximum": 20.0,
    "rsi_period": 3,
    "selloff_threshold": -0.02,
    "target_r": 1.5,
}
PRIOR_EVIDENCE_END = "2025-07-25"
DEFAULT_ROOT = (
    PROJECT_ROOT / "strategy_tournament/v2/continuous" / SUCCESSOR_ID
)
BOUNDARY_ROOT = DEFAULT_ROOT / "boundary"
BOUNDARY_INSPECTION_ROOT = DEFAULT_ROOT / "boundary-inspection"
CAPACITY_ROOT = DEFAULT_ROOT / "capacity"
CAPACITY_INSPECTION_ROOT = DEFAULT_ROOT / "capacity-inspection"

PRIOR_RESULTS = (
    PROJECT_ROOT
    / "strategy_tournament/v2/discovery/gap-universe-oversold-reversal/"
    "development/gap-universe-oversold-reversal-development-"
    "e127ed57211b28ca1c52ecd61467f3ae1634bfeb55075f4d3c91f33fbe131a01.json",
    PROJECT_ROOT
    / "strategy_tournament/v2/discovery/gap-universe-oversold-reversal/"
    "development/gap-universe-oversold-reversal-development-"
    "57adbee98a36d50e3e913464caa350ebcdd4e376a8f408ac47729a850f2e0b73.json",
)
PRIOR_INSPECTIONS = (
    PROJECT_ROOT
    / "strategy_tournament/v2/discovery/gap-universe-oversold-reversal/"
    "development-inspection/gap-universe-oversold-reversal-development-"
    "inspection-2865e07ff8055357561acb3eca254dddc9edd3263d82ed0a9f48fd0b48588844.json",
    PROJECT_ROOT
    / "strategy_tournament/v2/discovery/gap-universe-oversold-reversal/"
    "development-inspection/gap-universe-oversold-reversal-development-"
    "inspection-d8d9467606a1672a9fd516ab00affc514f7e496ce2bd101b3a9bcd8b2f9e0c0f.json",
)

# Priority is frozen independently of returns.  The completed-reserve scanner
# is preferred, followed by the newest audited tranche and expansion sources.
SOURCE_BINDINGS = (
    {
        "detail": (
            "learning_runs/oversold_replication_v5/scanner_replay_v2/"
            "scanner-replay-detail.json"
        ),
        "summary": (
            "historical_batches/oversold_replication_v5/scanner-v2-build-status/"
            "oversold-replication-scanner-build-status-"
            "4e4391eb40bfa48b91e27ac075e4511c01a7b796e19c03a678d424d28228f9b5.json"
        ),
        "inspection": (
            "historical_batches/oversold_replication_v5/scanner-v2-data-inspection/"
            "oversold-replication-scanner-data-inspection-"
            "029a795393adcb6c4b3b61d5691150692f62fe7584a093d0354bfab0e46da800.json"
        ),
        "detail_hash_field": "detail_file_sha256",
    },
    {
        "detail": (
            "learning_runs/challenger_orb_retest_v1_tranche3/scanner_replay/"
            "scanner-replay-detail.json"
        ),
        "summary": (
            "research_results/2026-07-22-challenger-orb-retest-"
            "scanner-tranche3-v1.json"
        ),
        "inspection": (
            "research_results/2026-07-22-challenger-orb-retest-"
            "scanner-tranche3-v1-inspection.json"
        ),
        "detail_hash_field": "detail_sha256",
    },
    {
        "detail": (
            "learning_runs/challenger_orb_retest_v1_tranche2/scanner_replay/"
            "scanner-replay-detail.json"
        ),
        "summary": (
            "research_results/2026-07-22-challenger-orb-retest-"
            "scanner-tranche2-v1.json"
        ),
        "inspection": (
            "research_results/2026-07-22-challenger-orb-retest-"
            "scanner-tranche2-v1-inspection.json"
        ),
        "detail_hash_field": "detail_sha256",
    },
    {
        "detail": "learning_runs/scanner_expansion/scanner-replay-detail.json",
        "summary": "research_results/2026-07-19-scanner-expansion.json",
        "inspection": (
            "research_results/2026-07-19-scanner-expansion-inspection.json"
        ),
        "detail_hash_field": "detail_sha256",
    },
    {
        "detail": (
            "learning_runs/development_tranche_v3/scanner_replay/"
            "scanner-replay-detail.json"
        ),
        "summary": (
            "research_results/2026-07-20-development-tranche-v3-scanner.json"
        ),
        "inspection": (
            "research_results/2026-07-20-development-tranche-v3-"
            "scanner-inspection.json"
        ),
        "detail_hash_field": "detail_sha256",
    },
    {
        "detail": "learning_runs/scanner_expansion_v2/scanner-replay-detail.json",
        "summary": (
            "research_results/2026-07-19-development-tranche-scanner.json"
        ),
        "inspection": (
            "research_results/2026-07-19-development-tranche-"
            "scanner-inspection.json"
        ),
        "detail_hash_field": "detail_sha256",
    },
    {
        "detail": (
            "learning_runs/challenger_orb_retest_v1/scanner_replay_v2/"
            "scanner-replay-detail.json"
        ),
        "summary": (
            "research_results/2026-07-21-challenger-orb-retest-scanner-v2.json"
        ),
        "inspection": (
            "research_results/2026-07-21-challenger-orb-retest-"
            "scanner-v2-inspection.json"
        ),
        "detail_hash_field": "detail_sha256",
    },
)


class OversoldFixedRuleError(RuntimeError):
    """The fixed-rule evidence boundary is incomplete or drifted."""


def _canonical(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode()


def _hash(value: Any) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _read(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise OversoldFixedRuleError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise OversoldFixedRuleError(f"{path} must contain an object")
    return value


def _write(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    rendered = json.dumps(value, indent=2, sort_keys=True) + "\n"
    if path.exists() and path.read_text(encoding="utf-8") == rendered:
        return
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_text(rendered, encoding="utf-8")
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _write_gzip(path: Path, value: Any) -> None:
    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode="wb", mtime=0) as stream:
        stream.write(_canonical(value))
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = buffer.getvalue()
    if path.exists() and path.read_bytes() == encoded:
        return
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_bytes(encoded)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _read_gzip(path: Path) -> dict[str, Any]:
    try:
        with gzip.open(path, "rt", encoding="utf-8") as stream:
            value = json.load(stream)
    except (OSError, json.JSONDecodeError) as exc:
        raise OversoldFixedRuleError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise OversoldFixedRuleError(f"{path} must contain an object")
    return value


def _publish(
    root: Path,
    stem: str,
    content: Mapping[str, Any],
    identity_field: str,
) -> tuple[Path, dict[str, Any]]:
    value = dict(content)
    identity = _hash(value)
    value[identity_field] = identity
    path = root / f"{stem}-{identity}.json"
    if path.exists() and _read(path) != value:
        raise OversoldFixedRuleError(f"artifact drifted: {path}")
    _write(path, value)
    return path, value


def _load_hashed(
    path: Path, *, identity_field: str, expected_kind: str
) -> dict[str, Any]:
    value = _read(path)
    supplied = value.pop(identity_field, None)
    expected = _hash(value)
    value[identity_field] = supplied
    if not (
        supplied == expected
        and path.name.endswith(f"-{expected}.json")
        and value.get("artifact_kind") == expected_kind
    ):
        raise OversoldFixedRuleError(f"invalid {expected_kind}: {path}")
    return value


def _repo_path(path: Path) -> str:
    try:
        return path.resolve().relative_to(PROJECT_ROOT.resolve()).as_posix()
    except ValueError as exc:
        raise OversoldFixedRuleError(f"path escaped repository: {path}") from exc


def _store_path(store: HistoricalDayStore, path: Path) -> str:
    try:
        return path.resolve().relative_to(store.root.resolve()).as_posix()
    except ValueError as exc:
        raise OversoldFixedRuleError(
            f"path escaped historical store: {path}"
        ) from exc


def _timestamp(value: str, field: str) -> None:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise OversoldFixedRuleError(f"{field} is invalid") from exc
    if parsed.tzinfo is None:
        raise OversoldFixedRuleError(f"{field} needs a timezone")


def _one(root: Path, pattern: str = "*.json") -> Path:
    paths = sorted(root.glob(pattern))
    if len(paths) != 1:
        raise OversoldFixedRuleError(
            f"expected one artifact in {root}; found {len(paths)}"
        )
    return paths[0]


def _private_root(store: HistoricalDayStore) -> Path:
    return store.root / "_derived/oversold_fixed_rule" / SUCCESSOR_ID


def development_inventory_path(store: HistoricalDayStore) -> Path:
    return _private_root(store) / "development-inventory.json.gz"


def confirmation_inventory_path(store: HistoricalDayStore) -> Path:
    return _private_root(store) / "confirmation-inventory.json.gz"


def development_input_index_path(store: HistoricalDayStore) -> Path:
    return _private_root(store) / "development-input-index.json.gz"


def _source_graph(
    *, enforce_commit: bool
) -> tuple[
    dict[str, list[dict[str, Any]]],
    dict[str, str],
    list[dict[str, Any]],
]:
    candidates: dict[str, list[dict[str, Any]]] = {}
    selected_source: dict[str, str] = {}
    public_bindings: list[dict[str, Any]] = []
    for binding in SOURCE_BINDINGS:
        detail_path = PROJECT_ROOT / binding["detail"]
        summary_path = PROJECT_ROOT / binding["summary"]
        inspection_path = PROJECT_ROOT / binding["inspection"]
        if enforce_commit:
            for path in (summary_path, inspection_path):
                strategy_discovery.require_committed(path)
        summary = _read(summary_path)
        inspection = _read(inspection_path)
        detail_hash = sha256_file(detail_path)
        hash_field = str(binding["detail_hash_field"])
        if not (
            inspection.get("valid") is True
            and inspection.get(hash_field) == detail_hash
            and summary.get("information_cutoff", "TARGET_SESSION_09:35_ET")
            == "TARGET_SESSION_09:35_ET"
        ):
            raise OversoldFixedRuleError(
                f"scanner source is not independently bound: {detail_path}"
            )
        detail = _read(detail_path)
        if not (
            detail.get("information_cutoff") == "TARGET_SESSION_09:35_ET"
            and detail.get("selection_time_et") == "09:35:00"
            and isinstance(detail.get("dates"), Mapping)
        ):
            raise OversoldFixedRuleError(
                f"scanner source escaped the 09:35 wall: {detail_path}"
            )
        for day, raw in detail["dates"].items():
            rows = gap._candidates(str(day), raw)
            if rows and day not in candidates:
                candidates[str(day)] = rows
                selected_source[str(day)] = _repo_path(detail_path)
        public_bindings.append(
            {
                "detail_path": _repo_path(detail_path),
                "detail_file_sha256": detail_hash,
                "summary_path": _repo_path(summary_path),
                "summary_file_sha256": sha256_file(summary_path),
                "inspection_path": _repo_path(inspection_path),
                "inspection_file_sha256": sha256_file(inspection_path),
            }
        )
    return candidates, selected_source, public_bindings


def _prior_statistics(
    store: HistoricalDayStore, *, enforce_commit: bool
) -> dict[str, Any]:
    if enforce_commit:
        for path in (*PRIOR_RESULTS, *PRIOR_INSPECTIONS):
            strategy_discovery.require_committed(path)
    evaluations: list[dict[str, Any]] = []
    for result_path in PRIOR_RESULTS:
        result = strategy_discovery.load_artifact(
            result_path, expected_kind="development-search-result"
        )
        evaluations.append(
            strategy_discovery._load_development_evaluation(
                result, root=strategy_discovery.DEFAULT_ROOT
            )
        )
    inspections = [
        strategy_discovery.load_artifact(
            path, expected_kind="development-search-inspection"
        )
        for path in PRIOR_INSPECTIONS
    ]
    if not all(
        item.get("state") == "REJECTED"
        and len(item["selection"]["trial_classifications"]) == 32
        for item in inspections
    ):
        raise OversoldFixedRuleError("prior rejection graph drifted")
    trials_by_evaluation = [
        {str(row["trial_id"]): row for row in evaluation["trials"]}
        for evaluation in evaluations
    ]
    trial_ids = sorted(trials_by_evaluation[0])
    if not (
        len(trial_ids) == 32
        and all(sorted(rows) == trial_ids for rows in trials_by_evaluation)
    ):
        raise OversoldFixedRuleError("prior complete trial family drifted")
    daily: dict[str, list[float]] = {}
    filled: dict[str, list[float]] = {}
    dollars: dict[str, list[float]] = {}
    for trial_id in trial_ids:
        daily[trial_id] = [
            float(value)
            for rows in trials_by_evaluation
            for value in rows[trial_id]["metrics"]["oof_daily_account_returns"]
        ]
        filled[trial_id] = [
            float(value)
            for rows in trials_by_evaluation
            for value in rows[trial_id]["metrics"]["oof_filled_account_returns"]
        ]
        dollars[trial_id] = [
            float(value)
            for rows in trials_by_evaluation
            for value in rows[trial_id]["metrics"]["oof_net_pnl_dollars"]
        ]
    sharpes = [annualized_sharpe(daily[trial_id]) or 0.0 for trial_id in trial_ids]
    p_values: list[float] = []
    robust: list[dict[str, Any]] = []
    for trial_id in trial_ids:
        returns = daily[trial_id]
        mean = statistics.fmean(returns)
        deviation = statistics.stdev(returns)
        statistic = (
            mean / (deviation / math.sqrt(len(returns))) if deviation else 0.0
        )
        p_values.append(1 - NormalDist().cdf(statistic))
        trade_returns = filled[trial_id]
        midpoint = len(returns) // 2
        metrics = {
            "trial_id": trial_id,
            "stress_20bps_total_log_growth": sum(
                math.log1p(value) for value in returns
            ),
            "stress_20bps_profit_factor": profit_factor(dollars[trial_id]),
            "stress_20bps_maximum_drawdown_r": (
                maximum_drawdown_fraction(returns) / 0.005
            ),
            "first_half_log_growth": sum(
                math.log1p(value) for value in returns[:midpoint]
            ),
            "second_half_log_growth": sum(
                math.log1p(value) for value in returns[midpoint:]
            ),
            "without_five_best_log_growth": (
                sum(math.log1p(value) for value in sorted(trade_returns)[:-5])
                if len(trade_returns) > 5
                else -1.0
            ),
            "bootstrap_lower": (
                stationary_bootstrap_summary(
                    trade_returns, confidence=0.90, samples=2_000
                )["lower_one_sided"]
                if trade_returns
                else -1.0
            ),
            "filled_signals": len(trade_returns),
        }
        metrics["passes_replication_candidate_gates"] = bool(
            metrics["stress_20bps_total_log_growth"] > 0
            and (metrics["stress_20bps_profit_factor"] or 0) >= 1.20
            and metrics["stress_20bps_maximum_drawdown_r"] <= 6.0
            and metrics["first_half_log_growth"] > 0
            and metrics["second_half_log_growth"] > 0
            and metrics["without_five_best_log_growth"] > 0
            and metrics["bootstrap_lower"] > 0
        )
        if metrics["passes_replication_candidate_gates"]:
            robust.append(metrics)
    selected_rows = [
        rows[SELECTED_PRIOR_TRIAL_ID] for rows in trials_by_evaluation
    ]
    if not (
        len(robust) == 1
        and robust[0]["trial_id"] == SELECTED_PRIOR_TRIAL_ID
        and all(row["parameters"] == SELECTED_PARAMETERS for row in selected_rows)
    ):
        raise OversoldFixedRuleError(
            "deterministic fixed-rule selection drifted"
        )
    used_account_dates = sorted(
        {
            str(row["date"])
            for trial in selected_rows
            for row in trial["maturity_rows"]
        }
    )
    pbo = probability_of_backtest_overfitting(daily)["probability"]
    if pbo is None:
        raise OversoldFixedRuleError("prior PBO is unavailable")
    return {
        "trial_count": 32,
        "trial_ids": trial_ids,
        "trial_sharpes": sharpes,
        "trial_p_values": p_values,
        "pbo_probability": float(pbo),
        "selected_trial_id": SELECTED_PRIOR_TRIAL_ID,
        "selected_parameters": SELECTED_PARAMETERS,
        "selected_combined_metrics": robust[0],
        "used_account_dates": used_account_dates,
        "source_results": [
            {
                "path": _repo_path(path),
                "file_sha256": sha256_file(path),
            }
            for path in PRIOR_RESULTS
        ],
        "source_inspections": [
            {
                "path": _repo_path(path),
                "file_sha256": sha256_file(path),
            }
            for path in PRIOR_INSPECTIONS
        ],
    }


def _exposure_sets() -> tuple[set[tuple[str, str]], set[str]]:
    exact: set[tuple[str, str]] = set()
    wildcard_dates: set[str] = set()
    for record in outcome_exposure.read_index():
        scope = record["scope"]
        rows = scope.get("symbols_by_date") or {
            day: scope["symbols"] for day in scope["dates"]
        }
        for day, symbols in rows.items():
            if symbols == ["*"]:
                wildcard_dates.add(day)
            else:
                exact.update((day, symbol) for symbol in symbols)
    return exact, wildcard_dates


def _scope(candidates_by_date: Mapping[str, Sequence[Mapping[str, Any]]]) -> dict[str, Any]:
    dates = sorted(candidates_by_date)
    return {
        "dates": dates,
        "symbols_by_date": {
            day: sorted(str(row["symbol"]) for row in candidates_by_date[day])
            for day in dates
        },
    }


def _inventory(
    *,
    lane: str,
    candidates_by_date: Mapping[str, Sequence[Mapping[str, Any]]],
    sources_by_date: Mapping[str, str],
) -> dict[str, Any]:
    dates = sorted(candidates_by_date)
    content: dict[str, Any] = {
        "schema_version": 1,
        "dataset_id": DATASET_ID if lane == "development" else (
            f"dataset-{SUCCESSOR_ID}-confirmation"
        ),
        "lane": lane,
        "evaluation_dates": dates,
        "signal_dates": dates,
        "zero_signal_dates": [],
        "candidates_by_date": {
            day: list(candidates_by_date[day]) for day in dates
        },
        "source_by_date": {day: sources_by_date[day] for day in dates},
        "outcome_scope": _scope(candidates_by_date),
        "target_outcomes_observed_or_derived": False,
        "confirmation_access_permitted": False,
        "broker_actions": 0,
    }
    content["content_sha256"] = canonical_sha256(content)
    return content


def build_boundary(
    *,
    created_at: str,
    store: HistoricalDayStore | None = None,
    enforce_commit: bool = True,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    _timestamp(created_at, "created_at")
    if enforce_commit:
        strategy_discovery.require_committed(Path(__file__).resolve())
    source = store or HistoricalDayStore.from_env()
    prior = _prior_statistics(source, enforce_commit=enforce_commit)
    candidates, sources_by_date, source_bindings = _source_graph(
        enforce_commit=enforce_commit
    )
    exact_exposures, wildcard_dates = _exposure_sets()
    used_dates = set(prior["used_account_dates"])
    development: dict[str, list[dict[str, Any]]] = {}
    confirmation: dict[str, list[dict[str, Any]]] = {}
    partial_dates: list[str] = []
    for day in sorted(candidates):
        if day in used_dates:
            continue
        rows = candidates[day]
        pairs = {(day, str(row["symbol"])) for row in rows}
        exposed_count = (
            len(pairs)
            if day in wildcard_dates
            else len(pairs & exact_exposures)
        )
        if day <= PRIOR_EVIDENCE_END:
            if exposed_count in {0, len(pairs)}:
                development[day] = rows
            else:
                partial_dates.append(day)
        elif exposed_count == 0:
            confirmation[day] = rows
        else:
            partial_dates.append(day)
    if not (
        len(development) == 118
        and len(confirmation) == 66
        and max(development) == "2025-06-09"
        and min(confirmation) == "2025-08-13"
    ):
        raise OversoldFixedRuleError(
            "fixed-rule temporal inventory capacity drifted"
        )
    calendar = [
        day for day in source.dates("SPY") if PRIOR_EVIDENCE_END < day
    ]
    embargo_dates = calendar[:5]
    if not (
        len(embargo_dates) == 5
        and embargo_dates[-1] < min(confirmation)
    ):
        raise OversoldFixedRuleError("five-session embargo is unavailable")
    development_inventory = _inventory(
        lane="development",
        candidates_by_date=development,
        sources_by_date=sources_by_date,
    )
    confirmation_inventory = _inventory(
        lane="confirmation",
        candidates_by_date=confirmation,
        sources_by_date=sources_by_date,
    )
    outcome_exposure.assert_untouched(
        confirmation_inventory["outcome_scope"],
        outcome_exposure.read_index(),
    )
    outcome_exposure.assert_disjoint(
        [
            development_inventory["outcome_scope"],
            confirmation_inventory["outcome_scope"],
        ]
    )
    development_path = development_inventory_path(source)
    confirmation_path = confirmation_inventory_path(source)
    _write_gzip(development_path, development_inventory)
    _write_gzip(confirmation_path, confirmation_inventory)
    content: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": "oversold_fixed_rule_replication_boundary",
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "mechanism_family": MECHANISM_FAMILY,
        "successor_id": SUCCESSOR_ID,
        "dataset_id": DATASET_ID,
        "created_at": created_at,
        "state": "BOUNDARY_FROZEN_AWAITING_INSPECTION",
        "selection": {
            "algorithm": (
                "select the unique prior trial passing positive 20-bps "
                "growth, PF>=1.20, drawdown<=6R, positive halves, positive "
                "without-five-best growth, and positive one-sided 90% "
                "stationary-bootstrap lower mean"
            ),
            "selected_trial_id": prior["selected_trial_id"],
            "selected_parameters": prior["selected_parameters"],
            "selected_combined_metrics": prior[
                "selected_combined_metrics"
            ],
            "prior_trial_count": prior["trial_count"],
            "prior_trial_sharpes": prior["trial_sharpes"],
            "prior_trial_p_values": prior["trial_p_values"],
            "prior_pbo_probability": prior["pbo_probability"],
        },
        "prior_evidence_end": PRIOR_EVIDENCE_END,
        "prior_used_account_dates": prior["used_account_dates"],
        "development_dates": development_inventory["evaluation_dates"],
        "embargo_dates": embargo_dates,
        "confirmation_dates": confirmation_inventory["evaluation_dates"],
        "development_scope": development_inventory["outcome_scope"],
        "confirmation_scope": confirmation_inventory["outcome_scope"],
        "development_candidate_symbol_sessions": sum(
            len(rows) for rows in development.values()
        ),
        "confirmation_candidate_symbol_sessions": sum(
            len(rows) for rows in confirmation.values()
        ),
        "partial_exposure_dates_excluded": partial_dates,
        "source_priority": [binding["detail"] for binding in SOURCE_BINDINGS],
        "source_bindings": source_bindings,
        "prior_bindings": {
            "results": prior["source_results"],
            "inspections": prior["source_inspections"],
        },
        "private_development_inventory": {
            "relative_path": _store_path(source, development_path),
            "file_sha256": sha256_file(development_path),
            "content_sha256": development_inventory["content_sha256"],
        },
        "private_confirmation_inventory": {
            "relative_path": _store_path(source, confirmation_path),
            "file_sha256": sha256_file(confirmation_path),
            "content_sha256": confirmation_inventory["content_sha256"],
        },
        "outcome_exposure_index_sha256": outcome_exposure.audit()[
            "index_sha256"
        ],
        "new_development_outcomes_accessed": False,
        "confirmation_outcomes_accessed": False,
        "provider_requests": 0,
        "broker_actions": 0,
    }
    return content, development_inventory, confirmation_inventory


def freeze_boundary(
    *, created_at: str, store: HistoricalDayStore | None = None
) -> tuple[Path, dict[str, Any]]:
    content, _development, _confirmation = build_boundary(
        created_at=created_at, store=store
    )
    return _publish(
        BOUNDARY_ROOT,
        "oversold-fixed-rule-boundary",
        content,
        "contract_sha256",
    )


def inspect_boundary(
    contract_path: Path,
    *,
    inspected_at: str,
    store: HistoricalDayStore | None = None,
) -> tuple[Path, dict[str, Any]]:
    _timestamp(inspected_at, "inspected_at")
    strategy_discovery.require_committed(contract_path)
    contract = _load_hashed(
        contract_path,
        identity_field="contract_sha256",
        expected_kind="oversold_fixed_rule_replication_boundary",
    )
    rebuilt, development, confirmation = build_boundary(
        created_at=str(contract["created_at"]),
        store=store,
    )
    checks = {
        "exact_rebuild": {
            key: value
            for key, value in contract.items()
            if key != "contract_sha256"
        }
        == rebuilt,
        "singleton_rule": contract["selection"]["selected_parameters"]
        == SELECTED_PARAMETERS,
        "prior_correction_complete": contract["selection"][
            "prior_trial_count"
        ]
        == 32
        and len(contract["selection"]["prior_trial_sharpes"]) == 32
        and len(contract["selection"]["prior_trial_p_values"]) == 32,
        "development_capacity": len(development["signal_dates"]) == 118,
        "confirmation_capacity": len(confirmation["signal_dates"]) == 66,
        "five_session_embargo": len(contract["embargo_dates"]) == 5,
        "confirmation_untouched": not outcome_exposure.find_overlaps(
            contract["confirmation_scope"], outcome_exposure.read_index()
        ),
        "no_new_outcomes": contract["new_development_outcomes_accessed"]
        is False,
        "no_confirmation_outcomes": contract["confirmation_outcomes_accessed"]
        is False,
        "no_provider": contract["provider_requests"] == 0,
        "no_broker": contract["broker_actions"] == 0,
    }
    if not all(checks.values()):
        raise OversoldFixedRuleError("boundary inspection failed")
    content = {
        "schema_version": 1,
        "artifact_kind": "oversold_fixed_rule_replication_boundary_inspection",
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "successor_id": SUCCESSOR_ID,
        "state": "BOUNDARY_INSPECTED_COLLECTION_READY",
        "inspected_at": inspected_at,
        "contract_path": _repo_path(contract_path),
        "contract_file_sha256": sha256_file(contract_path),
        "contract_sha256": contract["contract_sha256"],
        "checks": checks,
        "development_candidate_symbol_sessions": contract[
            "development_candidate_symbol_sessions"
        ],
        "confirmation_candidate_symbol_sessions": contract[
            "confirmation_candidate_symbol_sessions"
        ],
        "provider_access_permitted_after_separate_collection_contract": True,
        "confirmation_access_permitted": False,
        "broker_actions_permitted": False,
        "valid": True,
    }
    return _publish(
        BOUNDARY_INSPECTION_ROOT,
        "oversold-fixed-rule-boundary-inspection",
        content,
        "inspection_sha256",
    )


def _boundary_chain(
    store: HistoricalDayStore, *, require_committed: bool
) -> tuple[Path, dict[str, Any], Path, dict[str, Any], dict[str, Any]]:
    contract_path = _one(BOUNDARY_ROOT)
    inspection_path = _one(BOUNDARY_INSPECTION_ROOT)
    if require_committed:
        for path in (contract_path, inspection_path):
            strategy_discovery.require_committed(path)
    contract = _load_hashed(
        contract_path,
        identity_field="contract_sha256",
        expected_kind="oversold_fixed_rule_replication_boundary",
    )
    inspection = _load_hashed(
        inspection_path,
        identity_field="inspection_sha256",
        expected_kind="oversold_fixed_rule_replication_boundary_inspection",
    )
    inventory = _read_gzip(development_inventory_path(store))
    if not (
        inspection["valid"] is True
        and inspection["contract_sha256"] == contract["contract_sha256"]
        and inventory["content_sha256"]
        == contract["private_development_inventory"]["content_sha256"]
        and sha256_file(development_inventory_path(store))
        == contract["private_development_inventory"]["file_sha256"]
    ):
        raise OversoldFixedRuleError("boundary chain drifted")
    return contract_path, contract, inspection_path, inspection, inventory


def _collection_public_paths() -> list[Path]:
    import oversold_fixed_rule_collection as collection

    return [
        _one(collection.CONTRACT_ROOT),
        _one(collection.CONTRACT_INSPECTION_ROOT),
        _one(collection.AUTHORIZATION_ROOT),
        _one(collection.STATUS_ROOT),
        _one(collection.DATA_INSPECTION_ROOT),
    ]


def _dataset_contract(
    *,
    registered_at: str,
    store: HistoricalDayStore,
) -> dict[str, Any]:
    _timestamp(registered_at, "registered_at")
    _contract_path, boundary, _inspection_path, _inspection, inventory = (
        _boundary_chain(store, require_committed=True)
    )
    public_paths = _collection_public_paths()
    for path in public_paths:
        strategy_discovery.require_committed(path)
    import oversold_fixed_rule_collection as collection

    data_inspection = collection.load_artifact(
        public_paths[-1],
        identity_field="inspection_sha256",
        expected_kind="oversold_replication_development_data_inspection",
    )
    input_index = _read_gzip(development_input_index_path(store))
    if not (
        data_inspection["state"] == "DEVELOPMENT_DATA_INSPECTED_READY"
        and data_inspection["valid"] is True
        and data_inspection["private_input_index_content_sha256"]
        == canonical_sha256(input_index)
        and input_index["evaluation_dates"] == inventory["evaluation_dates"]
    ):
        raise OversoldFixedRuleError("development inputs are not inspected")
    runtime_binding = {
        "family_id": FAMILY_ID,
        "sample_phase": "development",
        "private_inventory_path": _store_path(
            store, development_inventory_path(store)
        ),
        "private_inventory_file_sha256": sha256_file(
            development_inventory_path(store)
        ),
        "private_inventory_content_sha256": inventory["content_sha256"],
        "private_input_index_path": _store_path(
            store, development_input_index_path(store)
        ),
        "private_input_index_file_sha256": sha256_file(
            development_input_index_path(store)
        ),
        "private_input_index_content_sha256": canonical_sha256(input_index),
        "public_bindings": [
            {"path": _repo_path(path), "file_sha256": sha256_file(path)}
            for path in public_paths
        ],
        "dataset_loads_per_evaluation": 1,
        "provider_requests": 0,
    }
    contract = {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "registered_at": registered_at,
        "requested_dates": inventory["evaluation_dates"],
        "dataset_payload": {
            "lane": "development",
            "claim_scope": "DEVELOPMENT_ONLY",
            "evidence_paths": [_repo_path(path) for path in public_paths],
            "inspected": True,
            "point_in_time_evidence": True,
            "oversold_replication_capacity": {
                "family_id": FAMILY_ID,
                "mechanism_family": MECHANISM_FAMILY,
                "formal_capacity": len(inventory["signal_dates"]),
                "capacity_unit": "point-in-time signal-capable sessions",
                "evaluation_sessions": len(inventory["evaluation_dates"]),
                "signal_capable_sessions": len(inventory["signal_dates"]),
                "zero_signal_days": 0,
                "candidate_symbol_sessions": sum(
                    len(rows)
                    for rows in inventory["candidates_by_date"].values()
                ),
                "exact_390_contiguous_symbol_sessions": data_inspection[
                    "exact_390_contiguous_symbol_sessions"
                ],
                "sparse_symbol_sessions": data_inspection[
                    "sparse_symbol_sessions_retained_as_no_signal"
                ],
                "development_training_contaminated": True,
                "confirmation_access_permitted": False,
                "provider_requests": 0,
            },
            "oversold_replication_runtime": runtime_binding,
            "implementation_binding": {
                "publisher_path": _repo_path(Path(__file__).resolve()),
                "publisher_sha256": sha256_file(Path(__file__).resolve()),
                "plugin_path": "oversold_replication_plugin.py",
                "plugin_sha256": sha256_file(
                    PROJECT_ROOT / "oversold_replication_plugin.py"
                ),
                "runtime_path": "dense_strategy_runtime.py",
                "runtime_sha256": sha256_file(
                    PROJECT_ROOT / "dense_strategy_runtime.py"
                ),
            },
            "boundary_contract_sha256": boundary["contract_sha256"],
        },
    }
    return validate_dataset_contract(contract)


def publish_development(
    *,
    registered_at: str,
    store: HistoricalDayStore | None = None,
) -> tuple[Path, dict[str, Any]]:
    for path in (
        Path(__file__).resolve(),
        PROJECT_ROOT / "oversold_replication_plugin.py",
        PROJECT_ROOT / "dense_strategy_runtime.py",
    ):
        strategy_discovery.require_committed(path)
    source = store or HistoricalDayStore.from_env()
    return freeze_dataset_contract(
        _dataset_contract(registered_at=registered_at, store=source),
        CAPACITY_ROOT,
    )


def inspect_development(
    manifest_path: Path,
    *,
    inspected_at: str,
    store: HistoricalDayStore | None = None,
) -> tuple[Path, dict[str, Any]]:
    _timestamp(inspected_at, "inspected_at")
    strategy_discovery.require_committed(manifest_path)
    frozen = load_frozen_dataset_contract(manifest_path)
    source = store or HistoricalDayStore.from_env()
    expected = _dataset_contract(
        registered_at=str(frozen["registered_at"]), store=source
    )
    checks = {
        "exact_rebuild": {
            key: value
            for key, value in frozen.items()
            if key != "manifest_sha256"
        }
        == expected,
        "singleton_capacity": len(frozen["requested_dates"]) == 118,
        "one_dataset_load": frozen["dataset_payload"][
            "oversold_replication_runtime"
        ]["dataset_loads_per_evaluation"]
        == 1,
        "confirmation_locked": frozen["dataset_payload"][
            "oversold_replication_capacity"
        ]["confirmation_access_permitted"]
        is False,
    }
    if not all(checks.values()):
        raise OversoldFixedRuleError("development manifest inspection failed")
    content = {
        "schema_version": 1,
        "artifact_kind": "oversold_fixed_rule_development_manifest_inspection",
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "successor_id": SUCCESSOR_ID,
        "state": "DEVELOPMENT_MANIFEST_INSPECTED_READY",
        "inspected_at": inspected_at,
        "manifest_path": _repo_path(manifest_path),
        "manifest_file_sha256": sha256_file(manifest_path),
        "manifest_sha256": frozen["manifest_sha256"],
        "checks": checks,
        "confirmation_access_permitted": False,
        "broker_actions_permitted": False,
        "valid": True,
    }
    return _publish(
        CAPACITY_INSPECTION_ROOT,
        "oversold-fixed-rule-development-manifest-inspection",
        content,
        "inspection_sha256",
    )


def build_family_contract(
    *,
    created_at: str,
    store: HistoricalDayStore | None = None,
) -> dict[str, Any]:
    _timestamp(created_at, "created_at")
    source = store or HistoricalDayStore.from_env()
    boundary_path, boundary, boundary_inspection_path, boundary_inspection, _ = (
        _boundary_chain(source, require_committed=True)
    )
    manifest_path = _one(CAPACITY_ROOT)
    manifest_inspection_path = _one(CAPACITY_INSPECTION_ROOT)
    for path in (manifest_path, manifest_inspection_path):
        strategy_discovery.require_committed(path)
    manifest = load_frozen_dataset_contract(manifest_path)
    manifest_inspection = _load_hashed(
        manifest_inspection_path,
        identity_field="inspection_sha256",
        expected_kind="oversold_fixed_rule_development_manifest_inspection",
    )
    confirmation_inventory = _read_gzip(
        confirmation_inventory_path(source)
    )
    outcome_exposure.assert_untouched(
        confirmation_inventory["outcome_scope"],
        outcome_exposure.read_index(),
    )
    if not (
        boundary_inspection["valid"] is True
        and manifest_inspection["valid"] is True
        and manifest_inspection["manifest_sha256"] == manifest["manifest_sha256"]
    ):
        raise OversoldFixedRuleError("family evidence chain is incomplete")
    selected = boundary["selection"]
    contract: dict[str, Any] = {
        "schema_version": 1,
        "campaign_id": CAMPAIGN_ID,
        "experiment_id": EXPERIMENT_ID,
        "family_id": FAMILY_ID,
        "mechanism_family": MECHANISM_FAMILY,
        "strategy_id": STRATEGY_ID,
        "parent_experiment_id": (
            "experiment-short-horizon-oversold-reversal-v5-completed-reserve"
        ),
        "created_at": created_at,
        "status": "INVENTED",
        "research_generation": "existing_family_fixed_rule_replication",
        "successor_id": SUCCESSOR_ID,
        "new_mechanism_family_slot_consumed": False,
        "dataset_lane": "development",
        "mechanism": (
            "Exact temporal replication of intraday mean reversion after a "
            "completed short-horizon oversold selloff inside a point-in-time "
            "liquid opening-gap common-stock universe."
        ),
        "expected_holding_behavior": (
            "Long only, next-minute entry, and flat by 15:50 ET on the "
            "signal day."
        ),
        "entry_rule": (
            "After a completed 15-minute selloff of at least two percent, "
            "RSI(3)<=20, bullish prior-high and session-VWAP reclaim gates, "
            "enter the ranked symbol at the next observed one-minute open."
        ),
        "stop_rule": (
            "Use the lowest completed session low through the trigger bar; "
            "a nonpositive structural stop produces a missed trade."
        ),
        "exit_rule": (
            "Resolve the frozen 1.5R target or stop with stop-first same-minute "
            "ambiguity and otherwise force flat at the 15:50 bar open."
        ),
        "ranking_rule": (
            "Earliest next-minute entry, then deepest selloff, lowest RSI, "
            "and lexical symbol."
        ),
        "selection_rule": (
            "At most one new family entry per day under authoritative "
            "portfolio risk, notional, entry, and capital-contention caps."
        ),
        "primary_outcome": (
            "Selection-adjusted chronological account log growth after costs."
        ),
        "material_difference_rationale": (
            "This is a one-rule temporal replication on dates absent from the "
            "selected rule's prior account paths. It permits no alternatives "
            "and carries all 32 prior trial statistics into DSR, Holm, and PBO."
        ),
        "universe_requirements": {
            "security_type": "point-in-time active U.S. common stocks",
            "opening_price_minimum": 5.0,
            "opening_gap_fraction": [0.02, 0.08],
            "selection_time_et": "09:35:00",
            "complete_candidate_denominator": True,
        },
        "execution_assumptions": {
            "next_observable_fill": True,
            "ambiguity": "stop_first",
            "maximum_hold_sessions": 1,
            "missing_data": "retained_denominator_no_signal",
            "minimum_gross_to_primary_round_trip_cost": 5.0,
        },
        "falsification_criteria": {
            "minimum_20bps_log_growth": 0.0,
            "minimum_stressed_profit_factor": 1.2,
            "maximum_drawdown_r": 6.0,
            "minimum_deflated_sharpe_probability": 0.9,
            "maximum_pbo_probability": 0.5,
        },
        "minimum_evidence": {
            "configured_floor": 50,
            "confirmation_floor": 20,
            "power": 0.8,
            "alpha": 0.1,
        },
        "contamination_risks": [
            "The rule was selected from 32 prior trials across two corpora.",
            "All 32 prior statistics remain in the selection correction.",
            "Development is contaminated; confirmation pairs are untouched.",
        ],
        "production_compatibility_risks": [
            "Live 09:35 universe completeness, quote, spread, depth, halt, "
            "tradability, timing, protection, and reconciliation remain mandatory."
        ],
        "parameter_grid": {
            key: [value] for key, value in SELECTED_PARAMETERS.items()
        },
        "selection_mode": "development_search",
        "winner_selection": DEVELOPMENT_SEARCH_RULE,
        "prior_trial_sharpes": selected["prior_trial_sharpes"],
        "prior_trial_p_values": selected["prior_trial_p_values"],
        "prior_pbo_probability": selected["prior_pbo_probability"],
        "prior_selection_trial_count": selected["prior_trial_count"],
        "prior_selected_trial_id": selected["selected_trial_id"],
        "development_dates": boundary["development_dates"],
        "development_signal_dates": boundary["development_dates"],
        "embargo_dates": boundary["embargo_dates"],
        "confirmation_dates": boundary["confirmation_dates"],
        "confirmation_signal_dates": boundary["confirmation_dates"],
        "confirmation_signal_capacity": len(boundary["confirmation_dates"]),
        "development_scope": boundary["development_scope"],
        "confirmation_scope": boundary["confirmation_scope"],
        "outcome_exposure_index_sha256": outcome_exposure.audit()[
            "index_sha256"
        ],
        "universe": {
            "identity": (
                "complete point-in-time active U.S. common-stock 2-8% "
                "opening-gap candidates at 09:35 ET"
            ),
            "development_candidate_symbol_sessions": boundary[
                "development_candidate_symbol_sessions"
            ],
            "confirmation_candidate_symbol_sessions": boundary[
                "confirmation_candidate_symbol_sessions"
            ],
            "point_in_time": True,
        },
        "costs_bps_per_side": [5, 10, 20],
        "partitions": {
            "rolling_origin": True,
            "confirmation_untouched": True,
            "development_training_contaminated": True,
            "five_session_embargo": True,
        },
        "falsifiers": [
            "nonpositive stressed growth",
            "prior-trial-adjusted DSR or Holm rejection",
            "rolling-fold instability",
            "insufficient frozen confirmation capacity",
        ],
        "implementation_files": [
            "oversold_fixed_rule_replication.py",
            "oversold_fixed_rule_collection.py",
            "oversold_replication_plugin.py",
            "oversold_reversal_plugin.py",
            "oversold_replication_development_collection.py",
            "dense_strategy_runtime.py",
            "learning_statistics.py",
            "learning_experiment.py",
            "strategy_discovery.py",
            "outcome_exposure.py",
            "portfolio_maturity.py",
            "portfolio_config.toml",
        ],
        "plugin": {
            "module": "oversold_replication_plugin",
            "preflight": "preflight",
            "evaluate_development": "evaluate_development",
            "evaluate_confirmation": "evaluate_confirmation",
            "evaluate_production": "evaluate_production",
        },
        "capacity_policy": {"retire_below": 50, "fast_lane_at": 100},
        "capacity_manifest": _repo_path(manifest_path),
        "dataset_manifest": _repo_path(manifest_path),
        "evidence_paths": [
            _repo_path(boundary_path),
            _repo_path(boundary_inspection_path),
            _repo_path(manifest_path),
            _repo_path(manifest_inspection_path),
        ],
    }
    normalized = strategy_discovery._validate_family_contract(contract)
    if len(normalized["trial_family"]) + selected["prior_trial_count"] != 33:
        raise OversoldFixedRuleError("selection trial correction count drifted")
    return normalized


def freeze_family(
    *,
    created_at: str,
    store: HistoricalDayStore | None = None,
) -> tuple[Path, dict[str, Any]]:
    for path in (
        Path(__file__).resolve(),
        PROJECT_ROOT / "oversold_fixed_rule_collection.py",
        PROJECT_ROOT / "oversold_replication_plugin.py",
        PROJECT_ROOT / "oversold_reversal_plugin.py",
        PROJECT_ROOT / "oversold_replication_development_collection.py",
        PROJECT_ROOT / "dense_strategy_runtime.py",
        PROJECT_ROOT / "learning_statistics.py",
        PROJECT_ROOT / "learning_experiment.py",
        PROJECT_ROOT / "strategy_discovery.py",
        PROJECT_ROOT / "outcome_exposure.py",
        PROJECT_ROOT / "portfolio_maturity.py",
        PROJECT_ROOT / "portfolio_config.toml",
    ):
        strategy_discovery.require_committed(path)
    contract = build_family_contract(created_at=created_at, store=store)
    digest = hashlib.sha256(_canonical(contract)).hexdigest()
    path = DEFAULT_ROOT / "family-contract" / f"contract-{digest}.json"
    _write(path, contract)
    return path, contract


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    freeze = sub.add_parser("freeze-boundary")
    freeze.add_argument("--created-at", required=True)
    inspect = sub.add_parser("inspect-boundary")
    inspect.add_argument("contract", type=Path)
    inspect.add_argument("--inspected-at", required=True)
    publish = sub.add_parser("publish-development")
    publish.add_argument("--registered-at", required=True)
    inspect_development_parser = sub.add_parser("inspect-development")
    inspect_development_parser.add_argument("manifest", type=Path)
    inspect_development_parser.add_argument("--inspected-at", required=True)
    family = sub.add_parser("freeze-family")
    family.add_argument("--created-at", required=True)
    args = parser.parse_args(argv)
    try:
        if args.command == "freeze-boundary":
            path, value = freeze_boundary(created_at=args.created_at)
            state = value["state"]
        elif args.command == "inspect-boundary":
            path, value = inspect_boundary(
                args.contract, inspected_at=args.inspected_at
            )
            state = value["state"]
        elif args.command == "publish-development":
            path, value = publish_development(
                registered_at=args.registered_at
            )
            state = "DEVELOPMENT_MANIFEST_FROZEN"
        elif args.command == "inspect-development":
            path, value = inspect_development(
                args.manifest, inspected_at=args.inspected_at
            )
            state = value["state"]
        else:
            path, value = freeze_family(created_at=args.created_at)
            state = value["status"]
        print(
            json.dumps(
                {
                    "path": _repo_path(path),
                    "state": state,
                    "successor_id": SUCCESSOR_ID,
                    "development_dates": len(
                        value.get("development_dates", [])
                    ),
                    "confirmation_dates": len(
                        value.get("confirmation_dates", [])
                    ),
                    "confirmation_access_permitted": False,
                    "broker_actions": 0,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    except (
        KeyError,
        OSError,
        OversoldFixedRuleError,
        outcome_exposure.OutcomeExposureError,
        strategy_discovery.StrategyDiscoveryError,
        ValueError,
    ) as exc:
        print(
            json.dumps(
                {
                    "state": "BLOCKED",
                    "error": str(exc),
                    "confirmation_access_permitted": False,
                    "broker_actions": 0,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())

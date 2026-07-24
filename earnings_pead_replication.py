"""Freeze a prior-trial-corrected single-rule PEAD temporal replication."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import math
import statistics
from pathlib import Path
from statistics import NormalDist
from typing import Any

import dense_strategy_runtime as runtime
import earnings_gap_continuation as earnings
import earnings_pead_discovery as source
import outcome_exposure
import portfolio_maturity
import strategy_discovery
from historical_store import HistoricalDayStore, canonical_sha256
from learning_data import freeze_dataset_contract
from learning_experiment import DEVELOPMENT_SEARCH_RULE
from learning_statistics import annualized_sharpe


PROJECT_ROOT = Path(__file__).resolve().parent
CAMPAIGN_ID = portfolio_maturity.V2_CAMPAIGN_ID
FAMILY_ID = runtime.EARNINGS_PEAD_FAMILY
SUCCESSOR_ID = "earnings-positive-surprise-drift-v3-temporal-replication"
DEFAULT_ROOT = PROJECT_ROOT / "strategy_tournament/v2/continuous"
V2_RESULT = (
    PROJECT_ROOT
    / "strategy_tournament/v2/discovery/"
    "earnings-positive-surprise-drift/development/"
    "earnings-positive-surprise-drift-development-"
    "2681b9f952b6099b8a76beff460fd915bfb8ffb90ffab468ba1b94d6d68f830b.json"
)
V2_INSPECTION = source.V2_INSPECTION if hasattr(source, "V2_INSPECTION") else (
    PROJECT_ROOT
    / "strategy_tournament/v2/discovery/"
    "earnings-positive-surprise-drift/development-inspection/"
    "earnings-positive-surprise-drift-development-inspection-"
    "1921a1b3228088abdc5320333b0d8aa56a81f4243701ee5a3936018c27d52c6d.json"
)
BACKFILL_INSPECTION = (
    PROJECT_ROOT
    / "strategy_tournament/v2/continuous/"
    "earnings-positive-surprise-drift-v3-2024-backfill/"
    "metadata-collection-inspection/"
    "inspection-fb6f92f1f85cc35ae5fcc9e0510cf729b04f04a050f74dafcd3c1eada79c2006.json"
)
DEVELOPMENT_START = "2025-01-02"
DEVELOPMENT_END = "2025-08-29"
EMBARGO_START = "2025-09-02"
EMBARGO_END = "2025-09-08"
CONFIRMATION_START = "2025-09-09"
CONFIRMATION_END = "2025-12-23"
SELECTED_PRIOR_TRIAL_ID = "trial-fafd1af9fb50982e"
SELECTED_PARAMETERS = {
    "market_trend_gate": "SPY>SMA100",
    "maximum_hold_sessions": 2,
    "minimum_opening_gap_fraction": -0.02,
    "minimum_surprise_ratio": 0.0,
    "stop_atr14": 1.0,
}


class EarningsPeadReplicationError(RuntimeError):
    """The temporal replication or prior-selection correction drifted."""


def _read(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise EarningsPeadReplicationError(f"{path} must be an object")
    return value


def _gzip(path: Path) -> dict[str, Any]:
    with gzip.open(path, "rt", encoding="utf-8") as stream:
        value = json.load(stream)
    if not isinstance(value, dict):
        raise EarningsPeadReplicationError(f"{path} must be an object")
    return value


def _repo_path(path: Path) -> str:
    return str(path.resolve().relative_to(PROJECT_ROOT.resolve()))


def _prior_statistics(
    store: HistoricalDayStore, *, enforce_commit: bool
) -> dict[str, Any]:
    if enforce_commit:
        for path in (V2_RESULT, V2_INSPECTION, BACKFILL_INSPECTION):
            strategy_discovery.require_committed(path)
    result = _read(V2_RESULT)
    inspection = _read(V2_INSPECTION)
    backfill = _read(BACKFILL_INSPECTION)
    binding = result["evaluation_binding"]
    evaluation_path = store.root / binding["relative_path"]
    evaluation = _gzip(evaluation_path)
    if not (
        canonical_sha256(evaluation) == binding["content_sha256"]
        and inspection.get("state") == "REJECTED"
        and len(inspection["selection"]["trial_classifications"]) == 32
        and backfill.get("state") == "INSUFFICIENT_POWER_CAPACITY"
        and backfill.get("verified_positive_surprises") == 0
    ):
        raise EarningsPeadReplicationError(
            "prior outcome or zero-capacity graph drifted"
        )
    sharpes: list[float] = []
    p_values: list[float] = []
    trials = sorted(evaluation["trials"], key=lambda row: row["trial_id"])
    for trial in trials:
        returns = [
            float(value)
            for value in trial["metrics"]["oof_daily_account_returns"]
        ]
        sharpes.append(annualized_sharpe(returns) or 0.0)
        mean = statistics.fmean(returns)
        deviation = statistics.stdev(returns)
        statistic = (
            mean / (deviation / math.sqrt(len(returns)))
            if deviation
            else 0.0
        )
        p_values.append(1 - NormalDist().cdf(statistic))
    classifications = inspection["selection"]["trial_classifications"]
    eligible = [
        row
        for row in classifications
        if row["rebuilt_metrics"][
            "stress_20bps_bootstrap_lower_mean_account_return"
        ]
        > 0
        and row["gates"]["deflated_sharpe"]
        and row["gates"]["backtest_overfitting"]
        and row["gates"]["neighbor_stability"]
    ]
    selected = max(
        eligible,
        key=lambda row: (
            row["rebuilt_metrics"]["stress_20bps_total_log_growth"],
            row["trial_id"],
        ),
    )
    selected_trial = next(
        trial
        for trial in trials
        if trial["trial_id"] == SELECTED_PRIOR_TRIAL_ID
    )
    if not (
        len(trials) == 32
        and selected["trial_id"] == SELECTED_PRIOR_TRIAL_ID
        and selected_trial["parameters"] == SELECTED_PARAMETERS
    ):
        raise EarningsPeadReplicationError(
            "deterministic prior-trial selection drifted"
        )
    return {
        "trial_count": 32,
        "trial_sharpes": sharpes,
        "trial_p_values": p_values,
        "pbo_probability": max(
            float(row["rebuilt_metrics"]["pbo_probability"])
            for row in classifications
        ),
        "selected_trial_id": SELECTED_PRIOR_TRIAL_ID,
        "selected_parameters": SELECTED_PARAMETERS,
        "evaluation_content_sha256": binding["content_sha256"],
    }


def _exposure_sets() -> tuple[set[tuple[str, str]], set[str]]:
    exact: set[tuple[str, str]] = set()
    wild: set[str] = set()
    for record in outcome_exposure.read_index():
        scope = record["scope"]
        rows = scope.get("symbols_by_date") or {
            day: scope["symbols"] for day in scope["dates"]
        }
        for day, symbols in rows.items():
            if symbols == ["*"]:
                wild.add(day)
            else:
                exact.update((day, symbol) for symbol in symbols)
    return exact, wild


def _selection(
    store: HistoricalDayStore, *, enforce_commit: bool
) -> dict[str, Any]:
    events, first_seen = source._source_graph(
        store, enforce_commit=enforce_commit
    )
    calendar = [
        day
        for day in store.dates("SPY")
        if "2024-01-01" <= day <= "2025-12-31"
    ]
    indices = {day: index for index, day in enumerate(calendar)}
    development_dates = [
        day
        for day in calendar
        if DEVELOPMENT_START <= day <= DEVELOPMENT_END
    ]
    embargo_dates = [
        day for day in calendar if EMBARGO_START <= day <= EMBARGO_END
    ]
    confirmation_dates = [
        day
        for day in calendar
        if CONFIRMATION_START <= day <= CONFIRMATION_END
    ]
    if len(embargo_dates) != 5:
        raise EarningsPeadReplicationError(
            "replication embargo is incomplete"
        )
    candidates = {
        "development": {day: [] for day in development_dates},
        "confirmation": {day: [] for day in confirmation_dates},
    }
    phases = {
        **{day: "development" for day in development_dates},
        **{day: "confirmation" for day in confirmation_dates},
    }
    exact, wild = _exposure_sets()
    for event in events:
        actual = event.get("actual_eps")
        estimate = event.get("estimated_eps")
        if not (
            event.get("verified") is True
            and isinstance(actual, (int, float))
            and isinstance(estimate, (int, float))
            and actual > estimate
            and event.get("timing") in {"am", "pm"}
        ):
            continue
        report_index = indices.get(str(event["report_date"]))
        if report_index is None:
            continue
        reaction_index = report_index + (
            1 if event["timing"] == "pm" else 0
        )
        if not 25 <= reaction_index < len(calendar) - 5:
            continue
        reaction_date = calendar[reaction_index]
        phase = phases.get(reaction_date)
        symbol = str(event["symbol"])
        if (
            phase is None
            or first_seen.get(symbol, "9999-12-31") >= reaction_date
        ):
            continue
        required = calendar[reaction_index - 25 : reaction_index + 6]
        if any(
            not (
                store.root
                / symbol.lower()
                / day[:4]
                / f"{day}.json.gz"
            ).is_file()
            for day in required
        ):
            continue
        if phase == "confirmation" and (
            reaction_date in wild or (reaction_date, symbol) in exact
        ):
            continue
        candidates[phase][reaction_date].append(
            {
                "symbol": symbol,
                "report_date": event["report_date"],
                "reaction_date": reaction_date,
                "timing": event["timing"],
                "actual_eps": float(actual),
                "estimated_eps": float(estimate),
                "identity_first_observed": first_seen[symbol],
            }
        )
    for phase in candidates.values():
        for day in phase:
            phase[day] = sorted(
                phase[day],
                key=lambda row: (
                    row["symbol"],
                    row["report_date"],
                    row["timing"],
                ),
            )
    development_signal_dates = [
        day for day, rows in candidates["development"].items() if rows
    ]
    confirmation_signal_dates = [
        day for day, rows in candidates["confirmation"].items() if rows
    ]
    if not (
        len(development_signal_dates) >= 80
        and len(confirmation_signal_dates) >= 20
    ):
        raise EarningsPeadReplicationError(
            "temporal replication capacity is insufficient"
        )
    return {
        "schema_version": 1,
        "successor_id": SUCCESSOR_ID,
        "family_id": FAMILY_ID,
        "development_dates": development_dates,
        "embargo_dates": embargo_dates,
        "confirmation_dates": confirmation_dates,
        "development_signal_dates": development_signal_dates,
        "confirmation_signal_dates": confirmation_signal_dates,
        "candidates_by_phase": candidates,
        "event_selection_uses_price_contents": False,
        "development_training_contaminated": True,
        "confirmation_outcomes_accessed": False,
        "provider_requests": 0,
        "broker_actions": 0,
    }


def _scope(rows_by_date: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
    populated = {
        day: sorted({row["symbol"] for row in rows})
        for day, rows in rows_by_date.items()
        if rows
    }
    return {
        "dates": sorted(populated),
        "symbols_by_date": {
            day: populated[day] for day in sorted(populated)
        },
    }


def freeze_family(
    *,
    created_at: str,
    root: Path = DEFAULT_ROOT,
    store: HistoricalDayStore | None = None,
    enforce_commit: bool = True,
) -> tuple[Path, dict[str, Any], Path]:
    earnings._timestamp(created_at, "created_at")
    if enforce_commit:
        for path in (
            Path(__file__).resolve(),
            PROJECT_ROOT / "earnings_pead_plugin.py",
            PROJECT_ROOT / "dense_strategy_runtime.py",
            PROJECT_ROOT / "learning_statistics.py",
            PROJECT_ROOT / "learning_experiment.py",
            PROJECT_ROOT / "strategy_discovery.py",
            PROJECT_ROOT / "outcome_exposure.py",
            PROJECT_ROOT / "portfolio_maturity.py",
            PROJECT_ROOT / "portfolio_config.toml",
        ):
            strategy_discovery.require_committed(path)
    source_store = store or HistoricalDayStore.from_env()
    prior = _prior_statistics(
        source_store, enforce_commit=enforce_commit
    )
    selection = _selection(
        source_store, enforce_commit=enforce_commit
    )
    selection_sha = canonical_sha256(selection)
    private = (
        source_store.root
        / "_derived/earnings_pead"
        / selection_sha
        / "selection.json.gz"
    )
    earnings._write_gzip(private, selection)
    development_scope = _scope(
        selection["candidates_by_phase"]["development"]
    )
    confirmation_scope = _scope(
        selection["candidates_by_phase"]["confirmation"]
    )
    outcome_exposure.assert_untouched(
        confirmation_scope, outcome_exposure.read_index()
    )
    outcome_exposure.assert_disjoint(
        [development_scope, confirmation_scope]
    )
    evidence_paths = [
        _repo_path(source.SOURCE_COLLECTION),
        _repo_path(V2_RESULT),
        _repo_path(V2_INSPECTION),
        _repo_path(BACKFILL_INSPECTION),
        "strategy_tournament/v2/OUTCOME_EXPOSURE_INDEX.jsonl",
    ]
    capacity_path, _ = freeze_dataset_contract(
        {
            "schema_version": 1,
            "dataset_id": f"dataset-{SUCCESSOR_ID}-development",
            "registered_at": created_at,
            "requested_dates": selection["development_dates"],
            "dataset_payload": {
                "lane": "development",
                "claim_scope": "DEVELOPMENT_ONLY",
                "inspected": True,
                "point_in_time_evidence": True,
                "evidence_paths": evidence_paths,
                "earnings_pead_capacity": {
                    "family_id": FAMILY_ID,
                    "development_event_pairs": sum(
                        len(rows)
                        for rows in selection["candidates_by_phase"][
                            "development"
                        ].values()
                    ),
                    "development_signal_dates": len(
                        selection["development_signal_dates"]
                    ),
                    "confirmation_event_pairs": sum(
                        len(rows)
                        for rows in selection["candidates_by_phase"][
                            "confirmation"
                        ].values()
                    ),
                    "confirmation_signal_dates": len(
                        selection["confirmation_signal_dates"]
                    ),
                    "confirmation_access_permitted": False,
                    "provider_requests": 0,
                },
                "earnings_pead_runtime": {
                    "family_id": FAMILY_ID,
                    "sample_phase": "development",
                    "private_selection": (
                        "LOCAL_HISTORICAL_DATA_ROOT/_derived/"
                        f"earnings_pead/{selection_sha}/selection.json.gz"
                    ),
                    "private_selection_content_sha256": selection_sha,
                    "provider_requests": 0,
                },
            },
        },
        root / SUCCESSOR_ID / "capacity",
    )
    contract: dict[str, Any] = {
        "schema_version": 1,
        "campaign_id": CAMPAIGN_ID,
        "experiment_id": f"experiment-{SUCCESSOR_ID}",
        "family_id": FAMILY_ID,
        "mechanism_family": "earnings-gap-continuation",
        "strategy_id": "earnings-positive-surprise-drift",
        "parent_experiment_id": (
            "experiment-earnings-positive-surprise-drift-"
            "v2-complete-daily-fallback"
        ),
        "created_at": created_at,
        "status": "INVENTED",
        "dataset_lane": "development",
        "mechanism": (
            "Temporal replication of post-earnings drift after a verified "
            "positive EPS surprise."
        ),
        "expected_holding_behavior": (
            "Long at the reaction-session open and flat at the prior-ATR stop "
            "or the second trading-session close."
        ),
        "entry_rule": (
            "Enter the verified report reaction open when gap is at least "
            "minus two percent, prior close and liquidity pass, and SPY is "
            "above SMA100."
        ),
        "stop_rule": (
            "Use one prior ATR14 below entry with stop-first and gap-through "
            "execution."
        ),
        "exit_rule": (
            "Exit on the structural stop or the second holding-session close."
        ),
        "ranking_rule": (
            "Highest positive EPS surprise ratio, then prior median dollar "
            "volume, then lexical symbol."
        ),
        "selection_rule": (
            "One family entry per day under portfolio risk and capital caps."
        ),
        "primary_outcome": (
            "Selection-adjusted chronological account log growth after costs."
        ),
        "material_difference_rationale": (
            "This replication adds June-August temporal development evidence, "
            "uses September-December untouched confirmation, evaluates only "
            "the deterministic broad v2 plateau rule, and carries all 32 "
            "prior trial statistics into DSR, Holm, and PBO."
        ),
        "universe_requirements": {
            "security_type": "previously observed point-in-time U.S. common stock",
            "prior_close_minimum": 10.0,
            "prior_20_session_median_dollar_volume_minimum": 50_000_000.0,
            "identity_must_precede_reaction": True,
        },
        "execution_assumptions": {
            "next_observable_open": True,
            "maximum_hold_sessions": 2,
            "same_interval_ambiguity": "stop_first",
            "missing_or_invalid_stop": "missed_or_rejected",
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
            "January-May development was used by the rejected v2 search.",
            "All 32 prior trial statistics remain in the selection correction.",
            "September-December confirmation pairs are globally untouched.",
        ],
        "production_compatibility_risks": [
            "Live event timestamp, identity, quote, spread, depth, halt, tradability, news, protection, and reconciliation remain mandatory."
        ],
        "parameter_grid": {
            key: [value] for key, value in SELECTED_PARAMETERS.items()
        },
        "selection_mode": "development_search",
        "winner_selection": DEVELOPMENT_SEARCH_RULE,
        "prior_trial_sharpes": prior["trial_sharpes"],
        "prior_trial_p_values": prior["trial_p_values"],
        "prior_pbo_probability": prior["pbo_probability"],
        "prior_selection_trial_count": prior["trial_count"],
        "prior_selected_trial_id": prior["selected_trial_id"],
        "development_dates": selection["development_dates"],
        "development_signal_dates": selection[
            "development_signal_dates"
        ],
        "embargo_dates": selection["embargo_dates"],
        "confirmation_dates": selection["confirmation_dates"],
        "confirmation_signal_dates": selection[
            "confirmation_signal_dates"
        ],
        "confirmation_signal_capacity": len(
            selection["confirmation_signal_dates"]
        ),
        "development_scope": development_scope,
        "confirmation_scope": confirmation_scope,
        "outcome_exposure_index_sha256": outcome_exposure.audit()[
            "index_sha256"
        ],
        "universe": {
            "identity": "verified positive EPS surprise with prior common-stock identity",
            "selection_sha256": selection_sha,
            "point_in_time": True,
        },
        "costs_bps_per_side": [5, 10, 20],
        "partitions": {
            "rolling_origin": True,
            "confirmation_untouched": True,
            "development_training_contaminated": True,
        },
        "falsifiers": [
            "nonpositive stressed growth",
            "prior-trial-adjusted DSR or Holm rejection",
            "rolling-fold instability",
            "insufficient frozen confirmation capacity",
        ],
        "implementation_files": [
            "earnings_pead_replication.py",
            "earnings_pead_plugin.py",
            "dense_strategy_runtime.py",
            "learning_statistics.py",
            "learning_experiment.py",
            "strategy_discovery.py",
            "outcome_exposure.py",
            "portfolio_maturity.py",
            "portfolio_config.toml",
        ],
        "plugin": {
            "module": "earnings_pead_plugin",
            "preflight": "preflight",
            "evaluate_development": "evaluate_development",
            "evaluate_confirmation": "evaluate_confirmation",
            "evaluate_production": "evaluate_production",
        },
        "capacity_policy": {"retire_below": 50, "fast_lane_at": 100},
        "capacity_manifest": _repo_path(capacity_path),
        "dataset_manifest": _repo_path(capacity_path),
    }
    contract = strategy_discovery._validate_family_contract(contract)
    if len(contract["trial_family"]) + prior["trial_count"] != 33:
        raise EarningsPeadReplicationError(
            "combined selection trial count drifted"
        )
    digest = hashlib.sha256(earnings._canonical(contract)).hexdigest()
    path = (
        root
        / SUCCESSOR_ID
        / "family-contract"
        / f"contract-{digest}.json"
    )
    earnings._write_json(path, contract)
    return path, contract, capacity_path


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("freeze",))
    parser.add_argument("--created-at", required=True)
    args = parser.parse_args()
    path, contract, capacity = freeze_family(
        created_at=args.created_at
    )
    print(
        json.dumps(
            {
                "state": "FAMILY_FROZEN",
                "path": _repo_path(path),
                "capacity_path": _repo_path(capacity),
                "current_trials": len(contract["trial_family"]),
                "prior_selection_trials": contract[
                    "prior_selection_trial_count"
                ],
                "development_signal_dates": len(
                    contract["development_signal_dates"]
                ),
                "confirmation_signal_dates": len(
                    contract["confirmation_signal_dates"]
                ),
                "confirmation_outcomes_accessed": False,
                "provider_requests": 0,
                "broker_actions": 0,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

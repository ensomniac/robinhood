"""Freeze and bind the broad SEC year-over-year EPS drift successor.

The family reuses only the already-inspected 2012-2014 development graph.
Its 2015 confirmation symbols remain inaccessible until one exact winner is
frozen.  The controller never requests provider data or broker actions.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import math
import statistics
from collections.abc import Mapping, Sequence
from datetime import date
from pathlib import Path
from statistics import NormalDist
from typing import Any

import dense_strategy_runtime as runtime
import earnings_sec_corrected_expansion as capacity
import earnings_sec_market_data as market
import earnings_sec_reaction_v13_search as v13
import earnings_sec_reaction_v14_search as v14
import outcome_exposure
import strategy_discovery
from historical_store import (
    DEFAULT_ENV_PATH,
    HistoricalDayStore,
    HistoricalStoreConfig,
    canonical_sha256,
    sha256_file,
)
from learning_data import freeze_dataset_contract, load_frozen_dataset_contract
from learning_experiment import DEVELOPMENT_SEARCH_RULE
from learning_statistics import annualized_sharpe


PROJECT_ROOT = Path(__file__).resolve().parent
CAMPAIGN_ID = v14.CAMPAIGN_ID
FAMILY_ID = runtime.SEC_BROAD_PEAD_FAMILY
SUCCESSOR_ID = "sec-yoy-eps-improvement-broad-drift-v1"
DEFAULT_ROOT = (
    PROJECT_ROOT / "strategy_tournament/v2/continuous" / SUCCESSOR_ID
)
SOURCE_DATASET_MANIFEST = (
    PROJECT_ROOT
    / "strategy_tournament/v2/discovery"
    / runtime.EARNINGS_SEC_REACTION_FAMILY
    / "development-dataset"
    / (
        "dataset-earnings-positive-surprise-drift-v14-sec-2012-2015-"
        "invalid-ohlcv-policy-development-"
        "42c50cedd2a419189f355cbbfb2e78286d998c1922168af31be369efabdb84f3"
        ".json"
    )
)
PRIOR_RESULTS = (
    PROJECT_ROOT
    / "strategy_tournament/v2/discovery/earnings-positive-surprise-drift"
    / "development"
    / (
        "earnings-positive-surprise-drift-development-"
        "2681b9f952b6099b8a76beff460fd915bfb8ffb90ffab468ba1b94d6d68f830b"
        ".json"
    ),
    PROJECT_ROOT
    / "strategy_tournament/v2/discovery/earnings-positive-surprise-drift"
    / "development"
    / (
        "earnings-positive-surprise-drift-development-"
        "d39f0af414a6d8a3a2a1ee7195e1afa7dbff5b4502ce0a129aab5cf99ebf8c75"
        ".json"
    ),
)
PRIOR_INSPECTIONS = (
    PROJECT_ROOT
    / "strategy_tournament/v2/discovery/earnings-positive-surprise-drift"
    / "development-inspection"
    / (
        "earnings-positive-surprise-drift-development-inspection-"
        "1921a1b3228088abdc5320333b0d8aa56a81f4243701ee5a3936018c27d52c6d"
        ".json"
    ),
    PROJECT_ROOT
    / "strategy_tournament/v2/discovery/earnings-positive-surprise-drift"
    / "development-inspection"
    / (
        "earnings-positive-surprise-drift-development-inspection-"
        "9db29e91bad048f427735b21f837f6d04b80a9ccedfd5626373af062079f4648"
        ".json"
    ),
)
EXPECTED_PRIOR_TRIALS = 33
EXPECTED_DEVELOPMENT_EVENTS = 1019
EXPECTED_DEVELOPMENT_SIGNAL_DATES = 499
EXPECTED_CONFIRMATION_EVENTS = 197
EXPECTED_CONFIRMATION_SIGNAL_DATES = 118
EXPECTED_CONFIRMATION_SYMBOLS = 174
EXPECTED_EXPOSED_CONFIRMATION_SYMBOLS = ("AGN", "BEAM", "EQIX", "WPX")


class SecBroadPeadError(RuntimeError):
    """The broad SEC PEAD search or data binding drifted."""


def _repo_path(path: Path) -> str:
    return str(path.resolve().relative_to(PROJECT_ROOT.resolve()))


def _expanded_confirmation_dates() -> list[str]:
    full = market._sessions(date(2014, 1, 2), v13.CONFIRMATION_END)
    first = full.index(v13.CONFIRMATION_START.isoformat())
    if first < 205:
        raise SecBroadPeadError(
            "confirmation history cannot support the frozen SMA200 gate"
        )
    return full[first - 205 :]


def _globally_exposed_symbols(
    *, dates: Sequence[str], symbols: Sequence[str]
) -> list[str]:
    date_set = set(dates)
    symbol_set = set(symbols)
    exposed: set[str] = set()
    for raw in outcome_exposure.read_index():
        record = outcome_exposure.validate_record(raw)
        scope = record["scope"]
        for day in date_set.intersection(scope["dates"]):
            scoped = (
                set(scope["symbols_by_date"][day])
                if "symbols_by_date" in scope
                else set(scope["symbols"])
            )
            exposed.update(
                symbol_set if "*" in scoped else symbol_set.intersection(scoped)
            )
    return sorted(exposed)


def selection(
    store: HistoricalDayStore | None = None,
) -> dict[str, Any]:
    """Rebuild event partitions without opening any price outcomes."""

    source = v14.selection(store)
    confirmation_opened_dates = _expanded_confirmation_dates()
    exposed = _globally_exposed_symbols(
        dates=confirmation_opened_dates,
        symbols=source["confirmation_symbols"],
    )
    confirmation_metadata = {
        day: [
            dict(row)
            for row in rows
            if str(row["symbol"]) not in set(exposed)
        ]
        for day, rows in source["confirmation_metadata_by_date"].items()
    }
    confirmation_signal_dates = [
        day for day, rows in confirmation_metadata.items() if rows
    ]
    confirmation_symbols = sorted(
        {
            str(row["symbol"])
            for rows in confirmation_metadata.values()
            for row in rows
        }
    )
    confirmation_requests = [
        v13._request(
            symbol,
            start=confirmation_opened_dates[0],
            end=confirmation_opened_dates[-1],
        )
        for symbol in confirmation_symbols
    ]
    selected = {
        "schema_version": 1,
        "family_id": FAMILY_ID,
        "successor_id": SUCCESSOR_ID,
        "development_dates": source["development_dates"],
        "development_signal_dates": source["development_signal_dates"],
        "development_event_count": source["development_event_count"],
        "development_symbols": source["development_symbols"],
        "development_opened_dates": source["development_opened_dates"],
        "development_metadata_by_date": source[
            "development_metadata_by_date"
        ],
        "permanently_excluded_development_symbols": source[
            "permanently_excluded_symbols"
        ],
        "embargo_dates": source["embargo_dates"],
        "confirmation_dates": source["confirmation_dates"],
        "confirmation_signal_dates": confirmation_signal_dates,
        "confirmation_event_count": sum(
            len(rows) for rows in confirmation_metadata.values()
        ),
        "confirmation_symbols": confirmation_symbols,
        "confirmation_metadata_by_date": confirmation_metadata,
        "confirmation_opened_dates": confirmation_opened_dates,
        "confirmation_requests": confirmation_requests,
        "globally_exposed_confirmation_symbols": exposed,
        "development_training_contaminated": True,
        "development_prices_accessed": False,
        "confirmation_prices_accessed": False,
        "strategy_metrics_computed": 0,
        "provider_requests": 0,
        "broker_actions": 0,
    }
    if not (
        selected["development_event_count"]
        == EXPECTED_DEVELOPMENT_EVENTS
        and len(selected["development_signal_dates"])
        == EXPECTED_DEVELOPMENT_SIGNAL_DATES
        and selected["confirmation_event_count"]
        == EXPECTED_CONFIRMATION_EVENTS
        and len(selected["confirmation_signal_dates"])
        == EXPECTED_CONFIRMATION_SIGNAL_DATES
        and len(selected["confirmation_symbols"])
        == EXPECTED_CONFIRMATION_SYMBOLS
        and tuple(exposed) == EXPECTED_EXPOSED_CONFIRMATION_SYMBOLS
        and not set(selected["development_symbols"]).intersection(
            selected["confirmation_symbols"]
        )
        and len(selected["embargo_dates"]) >= 5
    ):
        raise SecBroadPeadError(
            "broad SEC PEAD event partition or reserve changed"
        )
    confirmation_scope = {
        "dates": confirmation_opened_dates,
        "symbols": confirmation_symbols,
    }
    outcome_exposure.assert_untouched(
        confirmation_scope, outcome_exposure.read_index()
    )
    return selected


def prior_statistics() -> dict[str, Any]:
    sharpes: list[float] = []
    p_values: list[float] = []
    lineage: list[dict[str, Any]] = []
    trial_count = 0
    for result_path, inspection_path in zip(
        PRIOR_RESULTS, PRIOR_INSPECTIONS, strict=True
    ):
        strategy_discovery.require_committed(result_path)
        strategy_discovery.require_committed(inspection_path)
        result = strategy_discovery.load_artifact(
            result_path, expected_kind="development-search-result"
        )
        inspection = strategy_discovery.load_artifact(
            inspection_path,
            expected_kind="development-search-inspection",
        )
        if not (
            inspection.get("state") == "REJECTED"
            and inspection.get("selection", {}).get("status") == "REJECTED"
            and inspection.get("result_sha256")
            == result.get("artifact_sha256")
        ):
            raise SecBroadPeadError(
                "prior earnings selection lineage is not terminal"
            )
        evaluation = strategy_discovery._load_development_evaluation(
            result, root=strategy_discovery.DEFAULT_ROOT
        )
        trials = evaluation.get("trials")
        if not isinstance(trials, list) or not trials:
            raise SecBroadPeadError(
                "prior earnings evaluation lacks complete trials"
            )
        for trial in trials:
            values = [
                float(value)
                for value in trial["metrics"]["oof_daily_account_returns"]
            ]
            mean = statistics.fmean(values)
            deviation = (
                statistics.stdev(values) if len(values) > 1 else 0.0
            )
            statistic = (
                mean / (deviation / math.sqrt(len(values)))
                if deviation
                else 0.0
            )
            sharpes.append(float(annualized_sharpe(values) or 0.0))
            p_values.append(1 - NormalDist().cdf(statistic))
        trial_count += len(trials)
        lineage.append(
            {
                "result_path": _repo_path(result_path),
                "result_file_sha256": sha256_file(result_path),
                "result_sha256": result["artifact_sha256"],
                "inspection_path": _repo_path(inspection_path),
                "inspection_file_sha256": sha256_file(inspection_path),
                "inspection_sha256": inspection["artifact_sha256"],
                "trial_count": len(trials),
                "evaluation_binding": dict(result["evaluation_binding"]),
            }
        )
    if not (
        trial_count == EXPECTED_PRIOR_TRIALS
        and len(sharpes) == len(p_values) == EXPECTED_PRIOR_TRIALS
    ):
        raise SecBroadPeadError(
            "cumulative earnings selection accounting changed"
        )
    return {
        "trial_count": trial_count,
        "trial_sharpes": sharpes,
        "trial_p_values": p_values,
        "lineage": lineage,
    }


def build_contract(
    *,
    created_at: str,
    capacity_manifest: Path,
    selected: Mapping[str, Any],
    prior: Mapping[str, Any],
) -> dict[str, Any]:
    development_scope = {
        "dates": list(selected["development_opened_dates"]),
        "symbols": list(selected["development_symbols"]),
    }
    confirmation_scope = {
        "dates": list(selected["confirmation_opened_dates"]),
        "symbols": list(selected["confirmation_symbols"]),
    }
    outcome_exposure.assert_untouched(
        confirmation_scope, outcome_exposure.read_index()
    )
    outcome_exposure.assert_disjoint(
        [development_scope, confirmation_scope]
    )
    contract: dict[str, Any] = {
        "schema_version": 1,
        "campaign_id": CAMPAIGN_ID,
        "experiment_id": f"experiment-{SUCCESSOR_ID}",
        "family_id": FAMILY_ID,
        "mechanism_family": "earnings-gap-continuation",
        "strategy_id": "sec-yoy-eps-improvement-broad-drift",
        "parent_experiment_id": (
            "experiment-earnings-positive-surprise-drift-v14-"
            "sec-2012-2015-invalid-ohlcv-policy"
        ),
        "created_at": market._timestamp(created_at, "created_at"),
        "status": "INVENTED",
        "dataset_lane": "development",
        "mechanism": (
            "Post-earnings drift after point-in-time SEC evidence of positive "
            "year-over-year EPS improvement, without selecting on the "
            "reaction-session return."
        ),
        "expected_holding_behavior": (
            "Long at the first observable regular-session open and exit at "
            "the prior-ATR stop or after two or five completed sessions."
        ),
        "entry_rule": (
            "Enter the first open that could know the SEC filing when EPS "
            "improved year over year, the opening gap passes, prior close is "
            "above the frozen own-security trend average, and prior price, "
            "liquidity, ATR, and cost-floor gates pass."
        ),
        "stop_rule": (
            "Place the structural stop one or one-and-a-half completed ATR14 "
            "below entry; gap-through and same-session ambiguity are stop-first."
        ),
        "exit_rule": (
            "Exit on the structural stop or the second or fifth session close."
        ),
        "ranking_rule": (
            "Largest year-over-year EPS change ratio, then greatest prior "
            "20-session median dollar volume, then lexical symbol."
        ),
        "selection_rule": (
            "Evaluate all 16 frozen trials; select only through cumulative "
            "49-attempt DSR and Holm correction plus current-family PBO, "
            "neighbor, stress, stability, and account-growth gates."
        ),
        "primary_outcome": (
            "Selection-adjusted chronological account log growth after costs."
        ),
        "material_difference_rationale": (
            "The prior SEC reaction-confirmed family produced too few fills. "
            "This prospective successor removes all reaction-return selection, "
            "enters at the first observable open, and tests the broader "
            "point-in-time EPS-improvement mechanism on already-contaminated "
            "development evidence while carrying 33 broad-PEAD attempts into "
            "DSR and Holm correction."
        ),
        "universe_requirements": {
            "security_type": "SEC same-accession verified common equity",
            "positive_year_over_year_eps_improvement": True,
            "prior_close_minimum": 10.0,
            "prior_20_session_median_dollar_volume_minimum": 50_000_000.0,
            "confirmation_symbol_disjoint_from_development": True,
        },
        "execution_assumptions": {
            "first_observable_open": True,
            "same_day_sec_cutoff_eastern": "09:25:00",
            "maximum_hold_sessions": 5,
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
            "All 2012-2014 development prices were opened by the rejected SEC reaction-confirmed family.",
            "All 33 prior broad PEAD trial statistics enter cumulative DSR and Holm correction.",
            "No prior return path is mapped into current PBO because the parameter families differ.",
            "Four globally exposed confirmation symbols are excluded without replacement.",
            "Confirmation prices remain inaccessible before an exact winner freeze.",
        ],
        "production_compatibility_risks": [
            "Live SEC inventory, acceptance timestamp, opening print, ranking, quote, spread, depth, halt, tradability, news, GTC protection, and reconciliation must all be complete."
        ],
        "parameter_grid": {
            "minimum_yoy_eps_change_ratio": [0.0],
            "minimum_opening_gap_fraction": [-0.02, 0.0],
            "security_trend_gate": [
                "price>SMA100",
                "price>SMA200",
            ],
            "stop_atr14": [1.0, 1.5],
            "maximum_hold_sessions": [2, 5],
        },
        "selection_mode": "development_search",
        "winner_selection": DEVELOPMENT_SEARCH_RULE,
        "prior_trial_sharpes": list(prior["trial_sharpes"]),
        "prior_trial_p_values": list(prior["trial_p_values"]),
        "prior_pbo_probability": 0.0,
        "prior_selection_trial_count": int(prior["trial_count"]),
        "prior_selection_lineage": list(prior["lineage"]),
        "selection_accounting": {
            "current_trial_count": 16,
            "prior_evaluated_trial_count": int(prior["trial_count"]),
            "cumulative_trial_count": 16 + int(prior["trial_count"]),
            "deflated_sharpe_trial_count": 16
            + int(prior["trial_count"]),
            "holm_p_value_count": 16 + int(prior["trial_count"]),
            "pbo_trial_count": 16,
            "prior_standalone_pbo_is_not_irreversible_veto": True,
        },
        "development_dates": list(selected["development_dates"]),
        "development_signal_dates": list(
            selected["development_signal_dates"]
        ),
        "embargo_dates": list(selected["embargo_dates"]),
        "confirmation_dates": list(selected["confirmation_dates"]),
        "confirmation_signal_dates": list(
            selected["confirmation_signal_dates"]
        ),
        "confirmation_signal_capacity": len(
            selected["confirmation_signal_dates"]
        ),
        "development_scope": development_scope,
        "confirmation_scope": confirmation_scope,
        "outcome_exposure_index_sha256": outcome_exposure.audit()[
            "index_sha256"
        ],
        "universe": {
            "point_in_time": True,
            "security_type": "SEC same-accession verified common equity",
            "excluded_symbols": sorted(
                {
                    *selected[
                        "permanently_excluded_development_symbols"
                    ],
                    *selected["globally_exposed_confirmation_symbols"],
                }
            ),
            "symbols": len(selected["development_symbols"]),
            "confirmation_symbols": len(
                selected["confirmation_symbols"]
            ),
            "selection_sha256": canonical_sha256(selected),
            "development_event_count": selected[
                "development_event_count"
            ],
            "confirmation_event_count": selected[
                "confirmation_event_count"
            ],
        },
        "development_data_policy": {
            "source": "inspected v14 Yahoo daily graph",
            "provider_requests": 0,
            "development_training_contaminated": True,
            "source_dataset_manifest": _repo_path(
                SOURCE_DATASET_MANIFEST
            ),
            "family_rebind_only_after_search_freeze": True,
        },
        "confirmation_data_reserve": {
            "source": "Yahoo Finance historical chart JSON",
            "authorized_only_after_frozen_winner": True,
            "request_count": len(selected["confirmation_requests"]),
            "request_graph_sha256": canonical_sha256(
                selected["confirmation_requests"]
            ),
            "globally_exposed_symbols_excluded": list(
                selected["globally_exposed_confirmation_symbols"]
            ),
            "substitutions": 0,
        },
        "costs_bps_per_side": [5, 10, 20],
        "partitions": {
            "rolling_origin": True,
            "confirmation_untouched": True,
            "development_training_contaminated": True,
        },
        "falsifiers": [
            "nonpositive stressed log growth",
            "stressed profit factor below 1.20 or drawdown above 6R",
            "unstable one-step parameter neighbors",
            "selection-aware DSR, Holm, or PBO rejection",
            "rolling-fold or chronological-half instability",
            "incomplete account or candidate accounting",
            "insufficient frozen confirmation power capacity",
            "any access to excluded or substituted confirmation evidence",
        ],
        "implementation_files": [
            "sec_broad_pead.py",
            "dense_strategy_runtime.py",
            "dense_strategy_plugin.py",
            "learning_statistics.py",
            "learning_experiment.py",
            "strategy_discovery.py",
            "outcome_exposure.py",
            "portfolio_maturity.py",
            "portfolio_config.toml",
        ],
        "plugin": {
            "module": "dense_strategy_plugin",
            "preflight": "preflight",
            "evaluate_development": "evaluate_development",
            "evaluate_confirmation": "evaluate_confirmation",
            "evaluate_production": "evaluate_production",
        },
        "capacity_policy": {"retire_below": 50, "fast_lane_at": 100},
        "capacity_manifest": _repo_path(capacity_manifest),
        "new_mechanism_family_slot_consumed": False,
        "provider_requests_permitted": 0,
        "confirmation_outcomes_accessed": False,
        "broker_actions": 0,
    }
    frozen = strategy_discovery._validate_family_contract(contract)
    if not (
        len(frozen["trial_family"]) == 16
        and len(frozen["trial_family"])
        + int(prior["trial_count"])
        == 49
    ):
        raise SecBroadPeadError(
            "broad SEC PEAD trial accounting escaped the frozen bounds"
        )
    return frozen


def freeze_family(
    *,
    created_at: str,
    root: Path = DEFAULT_ROOT,
    store: HistoricalDayStore | None = None,
    enforce_commit: bool = True,
) -> tuple[Path, dict[str, Any], Path]:
    if enforce_commit:
        for path in (
            Path(__file__).resolve(),
            PROJECT_ROOT / "dense_strategy_runtime.py",
            PROJECT_ROOT / "dense_strategy_plugin.py",
            PROJECT_ROOT / "learning_experiment.py",
            PROJECT_ROOT / "strategy_discovery.py",
            PROJECT_ROOT / "outcome_exposure.py",
        ):
            strategy_discovery.require_committed(path)
    strategy_discovery.require_committed(SOURCE_DATASET_MANIFEST)
    selected = selection(store)
    prior = prior_statistics()
    evidence_paths = [
        _repo_path(SOURCE_DATASET_MANIFEST),
        *[_repo_path(path) for path in PRIOR_RESULTS],
        *[_repo_path(path) for path in PRIOR_INSPECTIONS],
        "strategy_tournament/v2/OUTCOME_EXPOSURE_INDEX.jsonl",
    ]
    capacity_path, _ = freeze_dataset_contract(
        {
            "schema_version": 1,
            "dataset_id": f"dataset-{SUCCESSOR_ID}-capacity",
            "registered_at": market._timestamp(
                created_at, "created_at"
            ),
            "requested_dates": selected["development_dates"],
            "dataset_payload": {
                "lane": "development",
                "claim_scope": "DEVELOPMENT_ONLY",
                "inspected": True,
                "point_in_time_evidence": True,
                "development_training_contaminated": True,
                "confirmation_access_permitted": False,
                "evidence_paths": evidence_paths,
                "dense_capacity": {
                    "family_id": FAMILY_ID,
                    "formal_capacity": selected[
                        "development_event_count"
                    ],
                    "signal_dates": len(
                        selected["development_signal_dates"]
                    ),
                    "confirmation_events": selected[
                        "confirmation_event_count"
                    ],
                    "confirmation_signal_dates": len(
                        selected["confirmation_signal_dates"]
                    ),
                    "external_dataset_opened": False,
                    "provider_requests": 0,
                },
            },
        },
        root / "capacity",
    )
    contract = build_contract(
        created_at=created_at,
        capacity_manifest=capacity_path,
        selected=selected,
        prior=prior,
    )
    digest = hashlib.sha256(
        capacity.canonical_bytes(contract)
    ).hexdigest()
    path = root / "family-contract" / f"contract-{digest}.json"
    market._write(path, contract)
    return path, contract, capacity_path


def publish_development(
    *,
    search_path: Path,
    published_at: str,
    root: Path = DEFAULT_ROOT,
) -> tuple[Path, dict[str, Any]]:
    """Rebind the inspected, contaminated v14 dataset after search freeze."""

    strategy_discovery.require_committed(search_path)
    strategy_discovery.require_committed(SOURCE_DATASET_MANIFEST)
    search = strategy_discovery.load_artifact(
        search_path, expected_kind="frozen-development-search"
    )
    contract = search["family_contract"]
    if not (
        contract.get("family_id") == FAMILY_ID
        and len(contract.get("trial_family", [])) == 16
        and contract.get("confirmation_outcomes_accessed") is False
    ):
        raise SecBroadPeadError(
            "broad SEC PEAD search is not frozen for data access"
        )
    source_manifest = load_frozen_dataset_contract(
        SOURCE_DATASET_MANIFEST
    )
    source_binding = source_manifest["dataset_payload"]["dense_runtime"]
    config = HistoricalStoreConfig.from_env(DEFAULT_ENV_PATH)
    source_path = config.root / source_binding["external_relative_path"]
    if sha256_file(source_path) != source_binding["external_file_sha256"]:
        raise SecBroadPeadError("source development dataset bytes drifted")
    with gzip.open(source_path, "rt", encoding="utf-8") as stream:
        dataset = json.load(stream)
    if not (
        canonical_sha256(dataset) == source_binding["dataset_sha256"]
        and dataset.get("family_id")
        == runtime.EARNINGS_SEC_REACTION_FAMILY
        and dataset.get("evaluation_dates")
        == contract["development_dates"]
    ):
        raise SecBroadPeadError(
            "source development dataset content drifted"
        )
    rebound = {**dataset, "family_id": FAMILY_ID}
    dataset_sha = canonical_sha256(rebound)
    relative = (
        Path("_derived/sec_broad_pead")
        / search["artifact_sha256"]
        / f"{dataset_sha}.json.gz"
    )
    private_path = config.root / relative
    market._write_private(private_path, rebound)
    manifest_path, manifest = freeze_dataset_contract(
        {
            "schema_version": 1,
            "dataset_id": f"dataset-{SUCCESSOR_ID}-development",
            "registered_at": market._timestamp(
                published_at, "published_at"
            ),
            "requested_dates": contract["development_dates"],
            "dataset_payload": {
                "lane": "development",
                "claim_scope": "DEVELOPMENT_ONLY",
                "inspected": True,
                "point_in_time_evidence": True,
                "development_training_contaminated": True,
                "confirmation_access_permitted": False,
                "development_search_sha256": search[
                    "artifact_sha256"
                ],
                "evidence_paths": [
                    _repo_path(search_path),
                    _repo_path(SOURCE_DATASET_MANIFEST),
                    "strategy_tournament/v2/OUTCOME_EXPOSURE_INDEX.jsonl",
                ],
                "dense_runtime": {
                    "family_id": FAMILY_ID,
                    "format": "json.gz",
                    "external_relative_path": str(relative),
                    "external_file_sha256": sha256_file(private_path),
                    "dataset_sha256": dataset_sha,
                    "formal_capacity": EXPECTED_DEVELOPMENT_EVENTS,
                    "source_dataset_sha256": source_binding[
                        "dataset_sha256"
                    ],
                    "provider_requests": 0,
                    "substitutions": 0,
                },
            },
        },
        (
            PROJECT_ROOT
            / "strategy_tournament/v2/discovery"
            / FAMILY_ID
            / "development-dataset"
        ),
    )
    return manifest_path, manifest


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("status")
    freeze = subparsers.add_parser("freeze-family")
    freeze.add_argument("--created-at", required=True)
    publish = subparsers.add_parser("publish-development")
    publish.add_argument("search", type=Path)
    publish.add_argument("--published-at", required=True)
    args = parser.parse_args(argv)
    if args.command == "status":
        selected = selection()
        value = {
            "state": "READY_TO_FREEZE",
            "family_id": FAMILY_ID,
            "calendar_wait_required": False,
            "development_events": selected["development_event_count"],
            "development_signal_dates": len(
                selected["development_signal_dates"]
            ),
            "confirmation_events": selected["confirmation_event_count"],
            "confirmation_signal_dates": len(
                selected["confirmation_signal_dates"]
            ),
            "globally_exposed_confirmation_symbols": selected[
                "globally_exposed_confirmation_symbols"
            ],
            "confirmation_outcomes_accessed": False,
            "provider_requests": 0,
            "broker_actions": 0,
        }
    elif args.command == "freeze-family":
        path, contract, capacity_path = freeze_family(
            created_at=args.created_at
        )
        value = {
            "state": "FAMILY_FROZEN",
            "path": _repo_path(path),
            "capacity_path": _repo_path(capacity_path),
            "current_trials": len(contract["trial_family"]),
            "prior_trials": contract["prior_selection_trial_count"],
            "cumulative_trials": contract["selection_accounting"][
                "cumulative_trial_count"
            ],
            "confirmation_outcomes_accessed": False,
            "provider_requests": 0,
        }
    else:
        path, _ = publish_development(
            search_path=args.search,
            published_at=args.published_at,
        )
        value = {
            "state": "DEVELOPMENT_DATASET_BOUND",
            "path": _repo_path(path),
            "confirmation_outcomes_accessed": False,
            "provider_requests": 0,
        }
    print(json.dumps(value, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

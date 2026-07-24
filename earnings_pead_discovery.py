"""Freeze a dense positive-earnings-surprise drift development search.

This successor uses the already inspected 2025 earnings metadata inventory and
locally cached market files. Candidate selection reads no price contents. The
development, five-session embargo, and globally untouched confirmation scopes
are frozen before the development plugin may open any outcome.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import dense_strategy_runtime as runtime
import earnings_gap_continuation as earnings
import outcome_exposure
import portfolio_maturity
import strategy_discovery
from historical_store import HistoricalDayStore, canonical_sha256
from learning_data import freeze_dataset_contract
from learning_experiment import DEVELOPMENT_SEARCH_RULE


PROJECT_ROOT = Path(__file__).resolve().parent
CAMPAIGN_ID = portfolio_maturity.V2_CAMPAIGN_ID
FAMILY_ID = runtime.EARNINGS_PEAD_FAMILY
MECHANISM_FAMILY = "earnings-gap-continuation"
STRATEGY_ID = "earnings-positive-surprise-drift"
SUCCESSOR_ID = "earnings-positive-surprise-drift-v1"
DEFAULT_ROOT = PROJECT_ROOT / "strategy_tournament/v2/continuous"
SOURCE_COLLECTION = (
    PROJECT_ROOT
    / "strategy_tournament/v2/continuous/"
    "earnings-gap-continuation-v1-development-search/event-collection/"
    "earnings-gap-event-collection-"
    "5c51a8f58f326bc1fe3c745a8dfc421ff9de51150ed6b933c0bab9b67b9cfbf6.json"
)
SOURCE_CAPACITY_INSPECTION = (
    PROJECT_ROOT
    / "strategy_tournament/v2/continuous/"
    "earnings-gap-continuation-v1-development-search/event-capacity-inspection/"
    "earnings-gap-event-capacity-inspection-"
    "d3ba91c913cc3a21d1faeb5fdb410935ea24deb5484b62fdf229282134df8e3f.json"
)
RETRY_FAILURE = (
    PROJECT_ROOT
    / "strategy_tournament/v2/continuous/"
    "earnings-gap-continuation-v2-dense-pead/event-retry-failure/"
    "earnings-gap-v2-event-retry-failure-"
    "aac4b782c8daed1334cba5ca95f7afffbeb12a4da38f0f843185eb315437a0ca.json"
)
IDENTITY_MANIFEST = (
    PROJECT_ROOT
    / "strategy_validation/equity_gap_continuation/manifests/"
    "equity-gap-continuation-v1-"
    "0145f77948ff8d7398c69ec7ae2229b72cfb7a07bab055c3dcfb15762d5cea43.json"
)
EVENT_PRIVATE = (
    "_derived/earnings_gap_continuation/"
    "6fe8fa2b5016ef949ab5fb8bd5dae901192aeb4cef480d407628d9f7ee30f8a1/"
    "event-calendar.json.gz"
)
IDENTITY_PRIVATE = (
    "_derived/equity_gap_continuation_validation/"
    "dataset-equity-gap-continuation-validation-2026-07-21-v3/"
    "frozen-candidates.json.gz"
)
DEVELOPMENT_START = "2025-01-02"
DEVELOPMENT_END = "2025-05-30"
EMBARGO_START = "2025-06-02"
EMBARGO_END = "2025-06-06"
CONFIRMATION_START = "2025-06-09"
CONFIRMATION_END = "2025-12-23"


class EarningsPeadDiscoveryError(RuntimeError):
    """The outcome-blind PEAD selection or evidence boundary drifted."""


def _read(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise EarningsPeadDiscoveryError(f"{path} must contain an object")
    return value


def _repo_path(path: Path) -> str:
    return str(path.resolve().relative_to(PROJECT_ROOT.resolve()))


def _gzip(path: Path) -> dict[str, Any]:
    with gzip.open(path, "rt", encoding="utf-8") as stream:
        value = json.load(stream)
    if not isinstance(value, dict):
        raise EarningsPeadDiscoveryError(f"{path} must contain an object")
    return value


def _source_graph(
    store: HistoricalDayStore, *, enforce_commit: bool
) -> tuple[list[dict[str, Any]], dict[str, str]]:
    for path in (
        SOURCE_COLLECTION,
        SOURCE_CAPACITY_INSPECTION,
        RETRY_FAILURE,
        IDENTITY_MANIFEST,
    ):
        if enforce_commit:
            strategy_discovery.require_committed(path)
    collection = _read(SOURCE_COLLECTION)
    inspection = _read(SOURCE_CAPACITY_INSPECTION)
    retry = _read(RETRY_FAILURE)
    identity_manifest = _read(IDENTITY_MANIFEST)
    event_path = store.root / EVENT_PRIVATE
    identity_path = store.root / IDENTITY_PRIVATE
    events = _gzip(event_path)
    identities = _gzip(identity_path)
    if not (
        collection.get("verified_positive_surprises") == 9_516
        and inspection.get("strategy_returns_computed") == 0
        and retry.get("provider_retry_permitted") is False
        and retry.get("provider_requests_total") == 14
        and retry.get("market_prices_accessed") is False
        and canonical_sha256(events)
        == collection.get("private_payload_content_sha256")
        and canonical_sha256(identities)
        == identity_manifest.get("private_selection", {}).get("content_sha256")
        and identities.get("target_outcomes_observed_or_derived") is False
    ):
        raise EarningsPeadDiscoveryError(
            "committed metadata or identity source graph drifted"
        )
    first_seen: dict[str, str] = {}
    for phase in identities["phases"].values():
        for day, rows in phase["candidates_by_date"].items():
            for row in rows:
                symbol = str(row["symbol"])
                first_seen[symbol] = min(first_seen.get(symbol, day), day)
    return list(events["events"]), first_seen


def _exposure_sets() -> tuple[set[tuple[str, str]], set[str]]:
    exact: set[tuple[str, str]] = set()
    wild: set[str] = set()
    for record in outcome_exposure.read_index():
        scope = record["scope"]
        if "symbols_by_date" in scope:
            rows = scope["symbols_by_date"].items()
        else:
            rows = ((day, scope["symbols"]) for day in scope["dates"])
        for day, symbols in rows:
            if symbols == ["*"]:
                wild.add(day)
            else:
                exact.update((day, symbol) for symbol in symbols)
    return exact, wild


def _candidate_selection(
    store: HistoricalDayStore,
    *,
    enforce_commit: bool,
) -> dict[str, Any]:
    events, first_seen = _source_graph(
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
    if not (
        len(development_dates) >= 60
        and len(embargo_dates) == 5
        and len(confirmation_dates) >= 60
    ):
        raise EarningsPeadDiscoveryError(
            "PEAD account calendars are incomplete"
        )
    exact_exposure, wild_exposure = _exposure_sets()
    candidates: dict[str, dict[str, list[dict[str, Any]]]] = {
        "development": {day: [] for day in development_dates},
        "confirmation": {day: [] for day in confirmation_dates},
    }
    phases = {
        **{day: "development" for day in development_dates},
        **{day: "confirmation" for day in confirmation_dates},
    }
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
        report_date = str(event["report_date"])
        report_index = indices.get(report_date)
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
        required_dates = calendar[
            reaction_index - 25 : reaction_index + 6
        ]
        if any(
            not (
                store.root
                / symbol.lower()
                / day[:4]
                / f"{day}.json.gz"
            ).is_file()
            for day in required_dates
        ):
            continue
        if phase == "confirmation" and (
            reaction_date in wild_exposure
            or (reaction_date, symbol) in exact_exposure
        ):
            continue
        candidates[phase][reaction_date].append(
            {
                "symbol": symbol,
                "report_date": report_date,
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
        len(development_signal_dates) >= 50
        and len(confirmation_signal_dates) >= 20
    ):
        raise EarningsPeadDiscoveryError(
            "PEAD outcome-blind signal capacity is insufficient"
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
        "confirmation_outcomes_accessed": False,
        "provider_requests": 0,
        "broker_actions": 0,
    }


def _scope(
    rows_by_date: Mapping[str, Sequence[Mapping[str, Any]]],
) -> dict[str, Any]:
    populated = {
        day: sorted({str(row["symbol"]) for row in rows})
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
            PROJECT_ROOT / "strategy_discovery.py",
            PROJECT_ROOT / "outcome_exposure.py",
            PROJECT_ROOT / "portfolio_maturity.py",
            PROJECT_ROOT / "portfolio_config.toml",
        ):
            strategy_discovery.require_committed(path)
    source = store or HistoricalDayStore.from_env()
    selection = _candidate_selection(
        source, enforce_commit=enforce_commit
    )
    selection_sha = canonical_sha256(selection)
    private_path = (
        source.root
        / "_derived/earnings_pead"
        / selection_sha
        / "selection.json.gz"
    )
    earnings._write_gzip(private_path, selection)
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
        _repo_path(SOURCE_COLLECTION),
        _repo_path(SOURCE_CAPACITY_INSPECTION),
        _repo_path(RETRY_FAILURE),
        _repo_path(IDENTITY_MANIFEST),
        "strategy_tournament/v2/OUTCOME_EXPOSURE_INDEX.jsonl",
    ]
    capacity_path, _capacity = freeze_dataset_contract(
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
        "mechanism_family": MECHANISM_FAMILY,
        "strategy_id": STRATEGY_ID,
        "parent_experiment_id": (
            "earnings-gap-continuation-v1-development-search"
        ),
        "created_at": created_at,
        "status": "INVENTED",
        "dataset_lane": "development",
        "mechanism": (
            "Post-earnings-announcement drift after a verified positive EPS "
            "surprise in a previously observed point-in-time common stock."
        ),
        "expected_holding_behavior": (
            "Long at the reaction-session open, protected by prior ATR, and "
            "closed at the stop or after at most five trading sessions."
        ),
        "entry_rule": (
            "For a verified before-open report enter that session open; for "
            "an after-close report enter the next session open, after frozen "
            "surprise, gap, liquidity, price, and SPY trend gates."
        ),
        "stop_rule": (
            "Place the frozen ATR14 multiple below entry; gap-through exits "
            "at the observed open and same-day low contact resolves stop-first."
        ),
        "exit_rule": (
            "Exit on the structural stop or the close of the frozen second "
            "or fifth holding session, whichever occurs first."
        ),
        "ranking_rule": (
            "Highest EPS surprise ratio, then prior twenty-session median "
            "dollar volume, then lexical symbol; one family entry per day."
        ),
        "selection_rule": (
            "At most one new family entry per day under authoritative "
            "portfolio risk, notional, concurrency, and capital caps."
        ),
        "primary_outcome": (
            "Selection-adjusted chronological account log growth after costs."
        ),
        "material_difference_rationale": (
            "This successor removes the v1 2-8 percent opening-gap dependency "
            "and tests dense multi-session PEAD directly while preserving "
            "verified reports, immutable timing, risk, and untouched evidence."
        ),
        "universe_requirements": {
            "security_type": "previously observed point-in-time U.S. common stock",
            "prior_close_minimum": 10.0,
            "prior_20_session_median_dollar_volume_minimum": 50_000_000.0,
            "identity_must_precede_reaction": True,
        },
        "execution_assumptions": {
            "next_observable_open": True,
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
            "The 2025 metadata inventory was previously inspected but contains no target outcomes.",
            "Development prices become contaminated when the frozen search evaluates them.",
            "Every globally exposed confirmation symbol-date pair is excluded before price access.",
        ],
        "production_compatibility_risks": [
            "Live reports need an exact timestamp and verified identity before entry.",
            "Fresh quote, spread, depth, halt, tradability, news, protection, and reconciliation remain mandatory.",
        ],
        "parameter_grid": {
            "minimum_surprise_ratio": [0.0, 0.25],
            "minimum_opening_gap_fraction": [-0.02, 0.0],
            "market_trend_gate": ["SPY>SMA100", "SPY>SMA200"],
            "stop_atr14": [1.0, 1.5],
            "maximum_hold_sessions": [2, 5],
        },
        "selection_mode": "development_search",
        "winner_selection": DEVELOPMENT_SEARCH_RULE,
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
            "development_event_pairs": sum(
                len(rows)
                for rows in selection["candidates_by_phase"][
                    "development"
                ].values()
            ),
            "confirmation_event_pairs": sum(
                len(rows)
                for rows in selection["candidates_by_phase"][
                    "confirmation"
                ].values()
            ),
            "point_in_time": True,
        },
        "costs_bps_per_side": [5, 10, 20],
        "partitions": {
            "rolling_origin": True,
            "confirmation_untouched": True,
            "development_training_contaminated": False,
        },
        "falsifiers": [
            "nonpositive stressed log growth",
            "unstable parameter neighbors",
            "selection-aware statistical rejection",
            "incomplete account or candidate accounting",
            "insufficient frozen confirmation power capacity",
        ],
        "implementation_files": [
            "earnings_pead_discovery.py",
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
    digest = hashlib.sha256(earnings._canonical(contract)).hexdigest()
    path = (
        root
        / SUCCESSOR_ID
        / "family-contract"
        / f"contract-{digest}.json"
    )
    earnings._write_json(path, contract)
    return path, contract, capacity_path


def status(*, root: Path = DEFAULT_ROOT) -> dict[str, Any]:
    contracts = sorted(
        (root / SUCCESSOR_ID / "family-contract").glob("*.json")
    )
    return {
        "campaign_id": CAMPAIGN_ID,
        "successor_id": SUCCESSOR_ID,
        "family_id": FAMILY_ID,
        "state": "READY_TO_FREEZE" if not contracts else "CONTRACT_FROZEN",
        "calendar_wait_required": False,
        "provider_requests_permitted": 0,
        "confirmation_outcomes_accessed": False,
        "broker_actions_permitted": False,
    }


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("status", "freeze"))
    parser.add_argument("--created-at")
    args = parser.parse_args(argv)
    if args.command == "status":
        result = status()
    else:
        if not args.created_at:
            raise EarningsPeadDiscoveryError("--created-at is required")
        path, contract, capacity = freeze_family(
            created_at=args.created_at
        )
        result = {
            "state": "FAMILY_FROZEN",
            "path": _repo_path(path),
            "capacity_path": _repo_path(capacity),
            "trial_count": len(contract["trial_family"]),
            "development_signal_dates": len(
                contract["development_signal_dates"]
            ),
            "confirmation_signal_dates": len(
                contract["confirmation_signal_dates"]
            ),
            "confirmation_outcomes_accessed": False,
            "provider_requests": 0,
            "broker_actions": 0,
        }
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

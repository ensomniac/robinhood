"""Freeze the event-scoped S&P 500 addition forced-demand search.

The official S&P release graph contains no target returns.  This module turns
that inspected metadata into exact development and untouched confirmation event
windows before any market-price provider is contacted.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from collections.abc import Mapping, Sequence
from datetime import datetime, time
from pathlib import Path
from typing import Any

import dense_strategy_runtime as runtime
import outcome_exposure
import portfolio_maturity
import sp500_addition_capacity
import strategy_discovery
from historical_store import canonical_sha256, sha256_file
from learning_data import freeze_dataset_contract
from learning_experiment import DEVELOPMENT_SEARCH_RULE


PROJECT_ROOT = Path(__file__).resolve().parent
CAMPAIGN_ID = portfolio_maturity.V2_CAMPAIGN_ID
FAMILY_ID = runtime.SP500_ADDITION_FORCED_DEMAND_FAMILY
MECHANISM_FAMILY = "sp500-index-addition-forced-demand"
STRATEGY_ID = "sp500-index-addition-forced-demand"
SUCCESSOR_ID = (
    "sp500-index-addition-forced-demand-v3-yahoo-invalid-as-missed"
)
DEFAULT_ROOT = PROJECT_ROOT / "strategy_tournament/v2/discovery"
CAPACITY_INSPECTION = (
    PROJECT_ROOT
    / "strategy_tournament/v2/sp500-index-addition-capacity/"
    "capacity-inspection/"
    "sp500-addition-capacity-inspection-"
    "39c62f408f2dccd31435d579d55c971b4b907ea0b14e1c7fd20dc7c84f215e22.json"
)
CALENDAR_PATHS = (
    PROJECT_ROOT
    / "historical_batches/etf_pullback_replication/"
    "session-calendar-2008-01-through-2015-12.json",
    PROJECT_ROOT
    / "historical_batches/continuous_v2/"
    "session-calendar-2014-01-through-2022-12.json",
    PROJECT_ROOT
    / "historical_batches/dense_v2/"
    "session-calendar-2020-01-through-2026-07.json",
)
DENSE_CALENDAR_INSPECTION = (
    PROJECT_ROOT
    / "strategy_tournament/v2/calendar/allocation/inspection/"
    "dense-calendar-allocation-inspection-"
    "5920eedbba8149183cfdfe4dd5ea2468def96316fa1701803958a22348667c50.json"
)
DEVELOPMENT_EVENT_END = "2018-12-31"
CONFIRMATION_EVENT_END = "2025-12-31"
MAXIMUM_HOLD_SESSIONS = 5
CONFIRMATION_EMBARGO_SESSIONS = 5


class Sp500AdditionDiscoveryError(RuntimeError):
    """The official event graph or its outcome-blind partition drifted."""


def _repo_path(path: Path) -> str:
    return str(path.resolve().relative_to(PROJECT_ROOT.resolve()))


def _timestamp(value: str, field: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise Sp500AdditionDiscoveryError(f"{field} is invalid") from exc
    if parsed.tzinfo is None:
        raise Sp500AdditionDiscoveryError(f"{field} needs a timezone")
    return parsed


def _calendar(
    *, enforce_commit: bool
) -> tuple[list[str], dict[str, Any]]:
    by_date: dict[str, dict[str, str]] = {}
    hashes: dict[str, str] = {}
    authorities: dict[str, str] = {}
    for path in CALENDAR_PATHS:
        if enforce_commit:
            if path == CALENDAR_PATHS[-1]:
                strategy_discovery.require_committed(
                    DENSE_CALENDAR_INSPECTION
                )
            else:
                strategy_discovery.require_committed(path)
        try:
            rows = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise Sp500AdditionDiscoveryError(
                f"calendar is unreadable: {path}"
            ) from exc
        if not isinstance(rows, list) or not rows:
            raise Sp500AdditionDiscoveryError(
                f"calendar is invalid: {path}"
            )
        hashes[_repo_path(path)] = sha256_file(path)
        if path == CALENDAR_PATHS[-1]:
            inspection = strategy_discovery.load_artifact(
                DENSE_CALENDAR_INSPECTION,
                expected_kind="dense-calendar-allocation-inspection",
            )
            checks = inspection.get("checks")
            if not (
                inspection.get("state")
                == "CALENDAR_ALLOCATION_INSPECTED_READY"
                and inspection.get("calendar_path") == _repo_path(path)
                and inspection.get("calendar_sha256")
                == hashes[_repo_path(path)]
                and isinstance(checks, Mapping)
                and checks
                and all(checks.values())
                and inspection.get("market_prices_accessed") is False
                and inspection.get("target_outcomes_accessed") is False
            ):
                raise Sp500AdditionDiscoveryError(
                    "ignored dense calendar lacks a valid committed inspection"
                )
            authorities[_repo_path(path)] = _repo_path(
                DENSE_CALENDAR_INSPECTION
            )
        for row in rows:
            if (
                not isinstance(row, Mapping)
                or set(row) != {"date", "open_et", "close_et"}
                or row["open_et"] != "09:30"
                or row["close_et"] not in {"13:00", "16:00"}
            ):
                raise Sp500AdditionDiscoveryError(
                    f"calendar row is invalid: {path}"
                )
            day = str(row["date"])
            normalized = {
                "date": day,
                "open_et": str(row["open_et"]),
                "close_et": str(row["close_et"]),
            }
            existing = by_date.get(day)
            if existing is not None and existing != normalized:
                raise Sp500AdditionDiscoveryError(
                    f"overlapping calendars disagree on {day}"
                )
            by_date[day] = normalized
    dates = sorted(by_date)
    if len(dates) != 4664 or dates[0] != "2008-01-02" or dates[-1] != "2026-07-17":
        raise Sp500AdditionDiscoveryError(
            "merged session calendar coverage drifted"
        )
    return dates, {
        "paths": hashes,
        "committed_authorities": authorities,
        "merged_session_count": len(dates),
        "merged_start": dates[0],
        "merged_end": dates[-1],
        "merged_sha256": canonical_sha256(
            [by_date[day] for day in dates]
        ),
    }


def _event_window(
    raw: Mapping[str, Any],
    calendar: Sequence[str],
) -> dict[str, Any]:
    required = {
        "action",
        "announcement_at",
        "announcement_date",
        "company_name",
        "effective_date",
        "index_name",
        "source_url",
        "ticker",
    }
    if set(raw) != required:
        raise Sp500AdditionDiscoveryError(
            "official S&P event schema drifted"
        )
    announcement = _timestamp(str(raw["announcement_at"]), "announcement_at")
    announcement_date = str(raw["announcement_date"])
    if announcement.date().isoformat() != announcement_date:
        raise Sp500AdditionDiscoveryError(
            "announcement timestamp and date disagree"
        )
    positions = {day: index for index, day in enumerate(calendar)}
    announcement_index = positions.get(announcement_date)
    before_open = announcement.timetz().replace(tzinfo=None) < time(9, 30)
    if announcement_index is not None and before_open:
        entry_index = announcement_index
    else:
        try:
            entry_index = next(
                index
                for index, day in enumerate(calendar)
                if day > announcement_date
            )
        except StopIteration as exc:
            raise Sp500AdditionDiscoveryError(
                "announcement lacks a later market session"
            ) from exc
    if entry_index < 1 or entry_index + MAXIMUM_HOLD_SESSIONS > len(calendar):
        raise Sp500AdditionDiscoveryError(
            "event window escaped the frozen calendar"
        )
    effective_date = str(raw["effective_date"])
    try:
        effective_index = next(
            index
            for index, day in enumerate(calendar)
            if day >= effective_date
        )
    except StopIteration as exc:
        raise Sp500AdditionDiscoveryError(
            "effective date escaped the frozen calendar"
        ) from exc
    if effective_index < 1:
        raise Sp500AdditionDiscoveryError(
            "effective date lacks a preceding session"
        )
    pre_effective_index = effective_index - 1
    sessions_to_effective = pre_effective_index - entry_index + 1
    event = {
        **dict(raw),
        "entry_date": calendar[entry_index],
        "reference_date": calendar[entry_index - 1],
        "holding_dates": list(
            calendar[
                entry_index : entry_index + MAXIMUM_HOLD_SESSIONS
            ]
        ),
        "pre_effective_date": calendar[pre_effective_index],
        "sessions_to_effective": sessions_to_effective,
    }
    event["event_id"] = canonical_sha256(event)
    return event


def _exposure_lookup(
    records: Sequence[Mapping[str, Any]],
) -> tuple[set[str], set[tuple[str, str]]]:
    wildcard_dates: set[str] = set()
    exact_pairs: set[tuple[str, str]] = set()
    for record in records:
        for day, symbol in outcome_exposure.scope_pairs(record["scope"]):
            if symbol == "*":
                wildcard_dates.add(day)
            else:
                exact_pairs.add((day, symbol))
    return wildcard_dates, exact_pairs


def _event_scope(events: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    symbols_by_date: dict[str, set[str]] = {}
    for event in events:
        for day in [
            str(event["reference_date"]),
            *map(str, event["holding_dates"]),
        ]:
            symbols_by_date.setdefault(day, set()).add(
                str(event["ticker"])
            )
    dates = sorted(symbols_by_date)
    if not dates:
        raise Sp500AdditionDiscoveryError("event scope is empty")
    return {
        "dates": dates,
        "symbols_by_date": {
            day: sorted(symbols_by_date[day]) for day in dates
        },
    }


def _partition(
    *, enforce_commit: bool
) -> tuple[dict[str, Any], dict[str, Any]]:
    if enforce_commit:
        strategy_discovery.require_committed(CAPACITY_INSPECTION)
        strategy_discovery.require_committed(
            outcome_exposure.DEFAULT_INDEX
        )
    capacity = strategy_discovery.load_artifact(
        CAPACITY_INSPECTION,
        expected_kind="sp500-addition-capacity-inspection",
    )
    if not (
        capacity.get("state") == "CAPACITY_READY_FAST_LANE"
        and capacity.get("market_outcomes_accessed") is False
        and capacity.get("target_return_access_permitted") is False
        and capacity.get("eligible_event_count") == len(
            capacity.get("events", [])
        )
    ):
        raise Sp500AdditionDiscoveryError(
            "official S&P capacity inspection is not outcome-blind and ready"
        )
    calendar, calendar_binding = _calendar(
        enforce_commit=enforce_commit
    )
    windowed_events = [
        _event_window(row, calendar) for row in capacity["events"]
    ]
    causal_ineligible_events = [
        {
            "event_id": event["event_id"],
            "ticker": event["ticker"],
            "announcement_date": event["announcement_date"],
            "effective_date": event["effective_date"],
            "entry_date": event["entry_date"],
            "pre_effective_date": event["pre_effective_date"],
            "terminal_reason": "NO_NEXT_OPEN_BEFORE_REBALANCE_CLOSE",
        }
        for event in windowed_events
        if int(event["sessions_to_effective"]) < 1
    ]
    events = [
        event
        for event in windowed_events
        if int(event["sessions_to_effective"]) >= 1
    ]
    records = outcome_exposure.read_index()
    wildcard_dates, exact_pairs = _exposure_lookup(records)
    development = [
        event
        for event in events
        if str(event["announcement_date"]) <= DEVELOPMENT_EVENT_END
    ]
    confirmation: list[dict[str, Any]] = []
    excluded_confirmation: list[dict[str, Any]] = []
    for event in events:
        announcement_date = str(event["announcement_date"])
        if not (
            DEVELOPMENT_EVENT_END < announcement_date
            <= CONFIRMATION_EVENT_END
        ):
            continue
        pairs = {
            (day, str(event["ticker"]))
            for day in [
                str(event["reference_date"]),
                *map(str, event["holding_dates"]),
            ]
        }
        overlaps = sorted(
            (day, symbol)
            for day, symbol in pairs
            if day in wildcard_dates or (day, symbol) in exact_pairs
        )
        if overlaps:
            excluded_confirmation.append(
                {
                    "event_id": event["event_id"],
                    "ticker": event["ticker"],
                    "announcement_date": announcement_date,
                    "overlap_count": len(overlaps),
                }
            )
        else:
            confirmation.append(event)
    def order(event: Mapping[str, Any]) -> tuple[str, int, str, str]:
        return (
            str(event["entry_date"]),
            -int(event["sessions_to_effective"]),
            str(event["ticker"]),
            str(event["event_id"]),
        )
    development = sorted(development, key=order)
    confirmation = sorted(confirmation, key=order)
    if not (
        len(development) == 144
        and len({row["entry_date"] for row in development}) == 100
    ):
        raise Sp500AdditionDiscoveryError(
            "outcome-blind S&P development capacity drifted"
        )
    if (
        len({row["entry_date"] for row in confirmation})
        < sp500_addition_capacity.MINIMUM_CONFIRMATION_SIGNAL_DATES
    ):
        raise Sp500AdditionDiscoveryError(
            "S&P untouched confirmation capacity is below the frozen floor"
        )
    positions = {day: index for index, day in enumerate(calendar)}
    development_start = min(
        str(row["entry_date"]) for row in development
    )
    development_end = max(
        max(map(str, row["holding_dates"])) for row in development
    )
    development_dates = list(
        calendar[
            positions[development_start] : positions[development_end] + 1
        ]
    )
    embargo_start = positions[development_end] + 1
    embargo_dates = list(
        calendar[
            embargo_start : embargo_start
            + CONFIRMATION_EMBARGO_SESSIONS
        ]
    )
    confirmation_start = embargo_start + CONFIRMATION_EMBARGO_SESSIONS
    confirmation_end = max(
        max(map(str, row["holding_dates"])) for row in confirmation
    )
    confirmation_dates = list(
        calendar[
            confirmation_start : positions[confirmation_end] + 1
        ]
    )
    development_scope = _event_scope(development)
    confirmation_scope = _event_scope(confirmation)
    outcome_exposure.assert_untouched(confirmation_scope, records)
    outcome_exposure.assert_disjoint(
        [development_scope, confirmation_scope]
    )
    return {
        "schema_version": 1,
        "artifact_kind": "sp500-addition-event-scope",
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "successor_id": SUCCESSOR_ID,
        "capacity_inspection_path": _repo_path(CAPACITY_INSPECTION),
        "capacity_inspection_sha256": capacity["artifact_sha256"],
        "calendar_binding": calendar_binding,
        "development_events": development,
        "confirmation_events": confirmation,
        "causal_ineligible_events": causal_ineligible_events,
        "excluded_confirmation_events": excluded_confirmation,
        "development_dates": development_dates,
        "development_signal_dates": sorted(
            {str(row["entry_date"]) for row in development}
        ),
        "embargo_dates": embargo_dates,
        "confirmation_dates": confirmation_dates,
        "confirmation_signal_dates": sorted(
            {str(row["entry_date"]) for row in confirmation}
        ),
        "development_scope": development_scope,
        "confirmation_scope": confirmation_scope,
        "outcome_exposure_index_sha256": outcome_exposure.audit()[
            "index_sha256"
        ],
        "market_outcomes_accessed": False,
        "provider_requests": 0,
        "broker_actions": 0,
    }, capacity


def freeze_family(
    *,
    created_at: str,
    root: Path = DEFAULT_ROOT,
    enforce_commit: bool = True,
) -> tuple[Path, dict[str, Any], Path, Path]:
    _timestamp(created_at, "created_at")
    implementation_files = [
        "sp500_addition_discovery.py",
        "sp500_addition_collection.py",
        "sp500_addition_collection_inspection.py",
        "sp500_addition_plugin.py",
        "dense_strategy_plugin.py",
        "dense_strategy_runtime.py",
        "learning_statistics.py",
        "learning_experiment.py",
        "strategy_discovery.py",
        "outcome_exposure.py",
        "portfolio_maturity.py",
        "portfolio_config.toml",
    ]
    if enforce_commit:
        for relative in implementation_files:
            strategy_discovery.require_committed(
                PROJECT_ROOT / relative
            )
    scope_payload, _capacity = _partition(
        enforce_commit=enforce_commit
    )
    scope_path, scope = strategy_discovery._write_artifact(
        scope_payload,
        root / SUCCESSOR_ID / "event-scope",
        "sp500-addition-event-scope",
    )
    capacity_path, _manifest = freeze_dataset_contract(
        {
            "schema_version": 1,
            "dataset_id": f"dataset-{SUCCESSOR_ID}-capacity",
            "registered_at": created_at,
            "requested_dates": scope["development_dates"],
            "dataset_payload": {
                "lane": "development",
                "claim_scope": "DEVELOPMENT_ONLY",
                "evidence_phase": "OUTCOME_BLIND_CAPACITY_ONLY",
                "inspected": True,
                "point_in_time_evidence": True,
                "evidence_paths": [
                    _repo_path(CAPACITY_INSPECTION),
                    _repo_path(scope_path),
                    "strategy_tournament/v2/OUTCOME_EXPOSURE_INDEX.jsonl",
                    _repo_path(DENSE_CALENDAR_INSPECTION),
                    *map(_repo_path, CALENDAR_PATHS),
                ],
                "dense_capacity": {
                    "family_id": FAMILY_ID,
                    "formal_capacity": len(
                        scope["development_signal_dates"]
                    ),
                    "development_event_count": len(
                        scope["development_events"]
                    ),
                    "confirmation_event_count": len(
                        scope["confirmation_events"]
                    ),
                    "confirmation_signal_capacity": len(
                        scope["confirmation_signal_dates"]
                    ),
                    "event_scope_sha256": scope["artifact_sha256"],
                    "market_outcomes_accessed": False,
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
        "created_at": created_at,
        "status": "INVENTED",
        "dataset_lane": "development",
        "mechanism": (
            "Official S&P 500 additions create a bounded forced-demand "
            "interval because index trackers must acquire the added common "
            "equity before the change becomes effective."
        ),
        "expected_holding_behavior": (
            "Long from the first regular-session open observable after the "
            "official timestamp until the structural stop, the frozen "
            "maximum hold, or the pre-effective rebalance close."
        ),
        "entry_rule": (
            "Rank same-entry-date official additions by longest remaining "
            "sessions to effectiveness then ticker; at the next observable "
            "open, accept the first whose frozen lead, maximum positive gap, "
            "cost-floor, and structural-stop rules pass."
        ),
        "stop_rule": (
            "Use the last completed pre-entry close with the frozen zero or "
            "one-percent downside buffer; a nonpositive stop or a stop not "
            "strictly below entry is a missed trade, never a repaired rule."
        ),
        "exit_rule": (
            "Exit stop-first on a daily gap or low, otherwise at the second "
            "or fifth holding-session close, with the optional causal exit "
            "at the last close before index effectiveness."
        ),
        "ranking_rule": (
            "Longest trading-session lead to the pre-effective rebalance "
            "close, then canonical ticker, then official event hash."
        ),
        "selection_rule": (
            "At most one new family entry per day under authoritative "
            "portfolio risk, notional, concurrency, and capital caps."
        ),
        "primary_outcome": (
            "Selection-adjusted chronological account log growth after "
            "5/10/20-bps-per-side costs."
        ),
        "material_difference_rationale": (
            "This is a public-index-constitution forced-flow mechanism, "
            "distinct from price-only reversal, momentum, pullback, earnings, "
            "activist, and repurchase families already exposed."
        ),
        "universe_requirements": {
            "security_type": "officially identified long U.S. common equity",
            "official_index": "S&P 500",
            "official_action": "Addition",
            "point_in_time_timestamp_required": True,
            "effective_date_required": True,
        },
        "execution_assumptions": {
            "next_observable_open": True,
            "maximum_hold_sessions": 5,
            "same_interval_ambiguity": "stop_first",
            "missing_or_invalid_stop": "missed_trade",
            "minimum_gross_to_primary_round_trip_cost": 5.0,
            "effective_semantics": (
                "index change effective before the effective-session open; "
                "pre-effective close is the causal rebalance close"
            ),
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
            "Official event metadata is outcome-blind but previously inspected.",
            "All development event-window prices are explicitly contaminated training evidence.",
            "Every globally exposed post-2018 event window is excluded before provider access.",
        ],
        "production_compatibility_risks": [
            "A live event needs an official timestamp and effective date before ranking.",
            "Fresh quote, spread, depth, halt, tradability, news, GTC protection, and reconciliation remain mandatory.",
        ],
        "parameter_grid": {
            "exit_mode": [
                "maximum_hold",
                "pre_effective_or_maximum_hold",
            ],
            "maximum_hold_sessions": [2, 5],
            "maximum_positive_announcement_gap_fraction": [0.02, 0.04],
            "minimum_sessions_to_effective": [2, 4],
            "stop_buffer_below_reference_close": [0.0, 0.01],
        },
        "selection_mode": "development_search",
        "winner_selection": DEVELOPMENT_SEARCH_RULE,
        "development_dates": scope["development_dates"],
        "development_signal_dates": scope[
            "development_signal_dates"
        ],
        "embargo_dates": scope["embargo_dates"],
        "confirmation_dates": scope["confirmation_dates"],
        "confirmation_signal_dates": scope[
            "confirmation_signal_dates"
        ],
        "confirmation_signal_capacity": len(
            scope["confirmation_signal_dates"]
        ),
        "development_scope": scope["development_scope"],
        "confirmation_scope": scope["confirmation_scope"],
        "outcome_exposure_index_sha256": scope[
            "outcome_exposure_index_sha256"
        ],
        "universe": {
            "official_index": "S&P 500",
            "official_action": "Addition",
            "event_scope_path": _repo_path(scope_path),
            "event_scope_sha256": scope["artifact_sha256"],
            "calendar_sha256": scope["calendar_binding"][
                "merged_sha256"
            ],
            "ranking": (
                "sessions_to_effective_desc,ticker,event_id"
            ),
        },
        "costs_bps_per_side": [5, 10, 20],
        "partitions": {
            "rolling_origin": True,
            "confirmation_untouched": True,
            "development_training_contaminated": True,
            "confirmation_embargo_sessions": 5,
        },
        "falsifiers": [
            "nonpositive stressed log growth",
            "stressed profit-factor or drawdown failure",
            "unstable one-step parameter neighbors",
            "selection-aware DSR, Holm, or PBO rejection",
            "incomplete event, execution, or trial accounting",
            "insufficient frozen confirmation power capacity",
        ],
        "historical_data_contract": {
            "daily_provider": "yahoo",
            "daily_adjusted": False,
            "daily_request_mode": "exact_event_window",
            "invalid_daily_response": "missed_trade",
            "permanent_missing_symbol_response": "missed_trade",
            "split_provider": "massive",
            "substitutions_allowed": False,
            "market_price_access_before_search_freeze": False,
        },
        "implementation_files": implementation_files,
        "plugin": {
            "module": "sp500_addition_plugin",
            "preflight": "preflight",
            "evaluate_development": "evaluate_development",
            "evaluate_confirmation": "evaluate_confirmation",
            "evaluate_production": "evaluate_production",
        },
        "capacity_policy": {"retire_below": 50, "fast_lane_at": 100},
        "capacity_manifest": _repo_path(capacity_path),
        "event_scope_path": _repo_path(scope_path),
        "event_scope_sha256": scope["artifact_sha256"],
    }
    validated = strategy_discovery._validate_family_contract(contract)
    digest = hashlib.sha256(
        json.dumps(
            validated,
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()
    contract_path = (
        root
        / SUCCESSOR_ID
        / "family-contract"
        / f"contract-{digest}.json"
    )
    contract_path.parent.mkdir(parents=True, exist_ok=True)
    rendered = json.dumps(validated, indent=2, sort_keys=True) + "\n"
    if contract_path.exists():
        if contract_path.read_text(encoding="utf-8") != rendered:
            raise Sp500AdditionDiscoveryError(
                "content-addressed family contract has other content"
            )
    else:
        temporary = contract_path.with_suffix(".json.tmp")
        temporary.write_text(rendered, encoding="utf-8")
        temporary.replace(contract_path)
    return contract_path, validated, capacity_path, scope_path


def status(*, root: Path = DEFAULT_ROOT) -> dict[str, Any]:
    contracts = sorted(
        (root / SUCCESSOR_ID / "family-contract").glob(
            "contract-*.json"
        )
    )
    discovery_root = strategy_discovery.DEFAULT_ROOT / FAMILY_ID
    return {
        "schema_version": 1,
        "campaign_id": CAMPAIGN_ID,
        "successor_id": SUCCESSOR_ID,
        "family_id": FAMILY_ID,
        "mechanism_family": MECHANISM_FAMILY,
        "calendar_wait_required": False,
        "contracts": len(contracts),
        "discovery_started": discovery_root.exists(),
        "confirmation_outcomes_accessed": False,
        "broker_actions_permitted": False,
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("status")
    freeze = sub.add_parser("freeze")
    freeze.add_argument("--created-at", required=True)
    return parser


def main() -> int:
    args = _parser().parse_args()
    try:
        if args.command == "status":
            result = status(root=args.root)
        else:
            path, artifact, capacity, scope = freeze_family(
                created_at=args.created_at,
                root=args.root,
            )
            result = {
                "path": _repo_path(path),
                "capacity_manifest": _repo_path(capacity),
                "event_scope": _repo_path(scope),
                "state": artifact["status"],
                "trial_count": len(artifact["trial_family"]),
                "development_signal_dates": len(
                    artifact["development_signal_dates"]
                ),
                "confirmation_signal_capacity": artifact[
                    "confirmation_signal_capacity"
                ],
                "calendar_wait_required": False,
            }
    except (
        Sp500AdditionDiscoveryError,
        strategy_discovery.StrategyDiscoveryError,
        outcome_exposure.OutcomeExposureError,
        OSError,
        ValueError,
    ) as exc:
        print(
            json.dumps(
                {"error": str(exc), "error_type": type(exc).__name__},
                indent=2,
                sort_keys=True,
            )
        )
        return 1
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

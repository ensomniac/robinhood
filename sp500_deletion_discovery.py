"""Freeze S&P 500 deletion forced-selling rebound discovery.

Official deletion metadata is partitioned and hash-bound before any historical
price is opened. The strategy enters only at the effective-session open, after
the index rebalance close has completed the hypothesized forced selling.
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
import sp500_addition_discovery as calendar_source
import strategy_discovery
from historical_store import canonical_sha256
from learning_data import freeze_dataset_contract
from learning_experiment import DEVELOPMENT_SEARCH_RULE


PROJECT_ROOT = Path(__file__).resolve().parent
CAMPAIGN_ID = portfolio_maturity.V2_CAMPAIGN_ID
FAMILY_ID = runtime.SP500_DELETION_FORCED_SELLING_FAMILY
MECHANISM_FAMILY = "large-index-deletion-forced-selling-rebound"
STRATEGY_ID = FAMILY_ID
SUCCESSOR_ID = "sp500-deletion-forced-selling-rebound-v2-fast-lane"
DEFAULT_ROOT = PROJECT_ROOT / "strategy_tournament/v2/discovery"
CAPACITY_INSPECTION = (
    PROJECT_ROOT
    / "strategy_tournament/v2/sp500-deletion-capacity/"
    "capacity-inspection/"
    "sp500-deletion-capacity-inspection-"
    "718148972c333e40b89ec760e3c9421987bce14203afa78034162047506efcb2.json"
)
DEVELOPMENT_EVENT_END = "2018-12-31"
CONFIRMATION_EVENT_END = "2025-12-31"
MAXIMUM_HOLD_SESSIONS = 5
CONFIRMATION_EMBARGO_SESSIONS = 5
EXPECTED_ELIGIBLE_EVENTS = 265
EXPECTED_CAUSAL_INELIGIBLE_EVENTS = 5
EXPECTED_DEVELOPMENT_EVENTS = 145
EXPECTED_DEVELOPMENT_SIGNAL_DATES = 102
EXPECTED_CONFIRMATION_EVENTS = 26
EXPECTED_CONFIRMATION_SIGNAL_DATES = 20


class Sp500DeletionDiscoveryError(RuntimeError):
    """Official deletion scope or its outcome-blind partition drifted."""


def _repo_path(path: Path) -> str:
    return str(path.resolve().relative_to(PROJECT_ROOT.resolve()))


def _timestamp(value: str, field: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise Sp500DeletionDiscoveryError(
            f"{field} is invalid"
        ) from exc
    if parsed.tzinfo is None:
        raise Sp500DeletionDiscoveryError(
            f"{field} needs a timezone"
        )
    return parsed


def _event_window(
    raw: Mapping[str, Any],
    calendar: Sequence[str],
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
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
        raise Sp500DeletionDiscoveryError(
            "official S&P deletion schema drifted"
        )
    if raw["action"] != "Deletion" or raw["index_name"] != "S&P 500":
        raise Sp500DeletionDiscoveryError(
            "official S&P deletion identity drifted"
        )
    announcement = _timestamp(
        str(raw["announcement_at"]), "announcement_at"
    )
    announcement_date = str(raw["announcement_date"])
    if announcement.date().isoformat() != announcement_date:
        raise Sp500DeletionDiscoveryError(
            "announcement timestamp and date disagree"
        )
    positions = {day: index for index, day in enumerate(calendar)}
    announcement_index = positions.get(announcement_date)
    before_open = (
        announcement.timetz().replace(tzinfo=None) < time(9, 30)
    )
    if announcement_index is not None and before_open:
        flow_start_index = announcement_index
    else:
        try:
            flow_start_index = next(
                index
                for index, day in enumerate(calendar)
                if day > announcement_date
            )
        except StopIteration as exc:
            raise Sp500DeletionDiscoveryError(
                "announcement lacks a later market session"
            ) from exc
    effective_date = str(raw["effective_date"])
    try:
        effective_index = next(
            index
            for index, day in enumerate(calendar)
            if day >= effective_date
        )
    except StopIteration as exc:
        raise Sp500DeletionDiscoveryError(
            "effective date escaped the frozen calendar"
        ) from exc
    if (
        flow_start_index < 1
        or effective_index + MAXIMUM_HOLD_SESSIONS > len(calendar)
    ):
        raise Sp500DeletionDiscoveryError(
            "deletion event window escaped the frozen calendar"
        )
    pre_effective_index = effective_index - 1
    sessions_to_effective = (
        pre_effective_index - flow_start_index + 1
    )
    if sessions_to_effective < 1:
        disposition = {
            **dict(raw),
            "flow_start_date": calendar[flow_start_index],
            "entry_date": calendar[effective_index],
            "terminal_reason": "NO_FORCED_SELLING_SESSION_BEFORE_REBALANCE",
        }
        disposition["event_id"] = canonical_sha256(disposition)
        return None, disposition
    reference_index = flow_start_index - 1
    holding_dates = list(
        calendar[
            effective_index : effective_index
            + MAXIMUM_HOLD_SESSIONS
        ]
    )
    event = {
        **dict(raw),
        "flow_start_date": calendar[flow_start_index],
        "reference_date": calendar[reference_index],
        "pre_effective_date": calendar[pre_effective_index],
        "entry_date": calendar[effective_index],
        "holding_dates": holding_dates,
        "observation_dates": list(
            calendar[
                reference_index : effective_index
                + MAXIMUM_HOLD_SESSIONS
            ]
        ),
        "sessions_to_effective": sessions_to_effective,
    }
    event["event_id"] = canonical_sha256(event)
    return event, None


def _exposure_lookup(
    records: Sequence[Mapping[str, Any]],
) -> tuple[set[str], set[tuple[str, str]]]:
    wildcard_dates: set[str] = set()
    exact_pairs: set[tuple[str, str]] = set()
    for record in records:
        for day, symbol in outcome_exposure.scope_pairs(
            record["scope"]
        ):
            if symbol == "*":
                wildcard_dates.add(day)
            else:
                exact_pairs.add((day, symbol))
    return wildcard_dates, exact_pairs


def _event_scope(
    events: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    symbols_by_date: dict[str, set[str]] = {}
    for event in events:
        for day in map(str, event["observation_dates"]):
            symbols_by_date.setdefault(day, set()).add(
                str(event["ticker"])
            )
    dates = sorted(symbols_by_date)
    if not dates:
        raise Sp500DeletionDiscoveryError(
            "deletion event scope is empty"
        )
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
        expected_kind="sp500-deletion-capacity-inspection",
    )
    checks = capacity.get("checks")
    if not (
        capacity.get("state") == "CAPACITY_READY_FAST_LANE"
        and capacity.get("market_outcomes_accessed") is False
        and capacity.get("target_return_access_permitted") is False
        and capacity.get("development_search_permitted") is True
        and isinstance(checks, Mapping)
        and checks
        and all(checks.values())
        and capacity.get("eligible_event_count")
        == len(capacity.get("events", []))
        == 270
    ):
        raise Sp500DeletionDiscoveryError(
            "official deletion capacity is not inspected and ready"
        )
    calendar, calendar_binding = calendar_source._calendar(
        enforce_commit=enforce_commit
    )
    eligible: list[dict[str, Any]] = []
    causal_ineligible: list[dict[str, Any]] = []
    for raw in capacity["events"]:
        event, disposition = _event_window(raw, calendar)
        if event is not None:
            eligible.append(event)
        elif disposition is not None:
            causal_ineligible.append(disposition)
    if not (
        len(eligible) == EXPECTED_ELIGIBLE_EVENTS
        and len(causal_ineligible)
        == EXPECTED_CAUSAL_INELIGIBLE_EVENTS
    ):
        raise Sp500DeletionDiscoveryError(
            "causal deletion inventory drifted"
        )
    records = outcome_exposure.read_index()
    wildcard_dates, exact_pairs = _exposure_lookup(records)
    development = [
        event
        for event in eligible
        if str(event["announcement_date"])
        <= DEVELOPMENT_EVENT_END
    ]
    confirmation: list[dict[str, Any]] = []
    excluded_confirmation: list[dict[str, Any]] = []
    for event in eligible:
        announcement_date = str(event["announcement_date"])
        if not (
            DEVELOPMENT_EVENT_END < announcement_date
            <= CONFIRMATION_EVENT_END
        ):
            continue
        overlaps = sorted(
            (day, str(event["ticker"]))
            for day in map(str, event["observation_dates"])
            if day in wildcard_dates
            or (day, str(event["ticker"])) in exact_pairs
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

    def order(
        event: Mapping[str, Any],
    ) -> tuple[str, int, str, str]:
        return (
            str(event["entry_date"]),
            -int(event["sessions_to_effective"]),
            str(event["ticker"]),
            str(event["event_id"]),
        )

    development = sorted(development, key=order)
    confirmation = sorted(confirmation, key=order)
    if not (
        len(development) == EXPECTED_DEVELOPMENT_EVENTS
        and len({row["entry_date"] for row in development})
        == EXPECTED_DEVELOPMENT_SIGNAL_DATES
        and len(confirmation) == EXPECTED_CONFIRMATION_EVENTS
        and len({row["entry_date"] for row in confirmation})
        == EXPECTED_CONFIRMATION_SIGNAL_DATES
    ):
        raise Sp500DeletionDiscoveryError(
            "outcome-blind deletion partition drifted"
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
    confirmation_start = (
        embargo_start + CONFIRMATION_EMBARGO_SESSIONS
    )
    first_confirmation_observation = min(
        min(map(str, row["observation_dates"]))
        for row in confirmation
    )
    if (
        positions[first_confirmation_observation]
        < confirmation_start
    ):
        raise Sp500DeletionDiscoveryError(
            "confirmation observation breached the frozen embargo"
        )
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
    outcome_exposure.assert_untouched(
        confirmation_scope, records
    )
    outcome_exposure.assert_disjoint(
        [development_scope, confirmation_scope]
    )
    return {
        "schema_version": 1,
        "artifact_kind": "sp500-deletion-event-scope",
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "successor_id": SUCCESSOR_ID,
        "capacity_inspection_path": _repo_path(
            CAPACITY_INSPECTION
        ),
        "capacity_inspection_sha256": capacity["artifact_sha256"],
        "calendar_binding": calendar_binding,
        "development_events": development,
        "confirmation_events": confirmation,
        "causal_ineligible_events": causal_ineligible,
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
        "sp500_deletion_discovery.py",
        "sp500_deletion_plugin.py",
        "sp500_deletion_collection.py",
        "sp500_deletion_collection_inspection.py",
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
        "sp500-deletion-event-scope",
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
                    _repo_path(
                        calendar_source.DENSE_CALENDAR_INSPECTION
                    ),
                    *map(_repo_path, calendar_source.CALENDAR_PATHS),
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
            "S&P 500 trackers must sell deletions by the rebalance close; "
            "a rebound may begin only after that bounded forced flow has "
            "culminated."
        ),
        "expected_holding_behavior": (
            "Long from the effective-session open after the rebalance "
            "close until the structural stop or the frozen two- or "
            "five-session maximum hold."
        ),
        "entry_rule": (
            "At the effective-session open, rank official deletions by the "
            "largest fully observed announcement-to-pre-effective-close "
            "decline, then flow-window length, ticker, and event hash; "
            "accept the first passing the frozen decline, lead, gap, cost, "
            "and structural-stop gates."
        ),
        "stop_rule": (
            "Use the completed pre-effective close with the frozen zero or "
            "one-percent downside buffer; a nonpositive stop or stop not "
            "strictly below entry is a missed trade."
        ),
        "exit_rule": (
            "Resolve a daily gap or low through the stop first; otherwise "
            "exit at the second or fifth holding-session close."
        ),
        "ranking_rule": (
            "Completed announcement-to-pre-effective decline descending, "
            "sessions-to-effective descending, ticker, then event hash."
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
            "This enters only after deletion-driven forced index selling; "
            "it is the opposite flow direction and timing from the retired "
            "pre-effective S&P addition continuation family."
        ),
        "universe_requirements": {
            "security_type": "officially identified long U.S. common equity",
            "official_index": "S&P 500",
            "official_action": "Deletion",
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
                "the preceding close is the forced-sale rebalance close"
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
            "Official metadata is inspected but contains no target returns.",
            "All development observation-window prices become contaminated training evidence.",
            "Every globally exposed confirmation window is removed before provider access.",
        ],
        "production_compatibility_risks": [
            "A live event needs an official timestamp and effective date.",
            "Fresh quotes, spread, depth, halt, tradability, news, GTC protection, and reconciliation remain mandatory.",
        ],
        "parameter_grid": {
            "maximum_hold_sessions": [2, 5],
            "maximum_positive_effective_gap_fraction": [0.02, 0.04],
            "minimum_pre_effective_decline_fraction": [0.02, 0.05],
            "minimum_sessions_to_effective": [2, 4],
            "stop_buffer_below_pre_effective_close": [0.0, 0.01],
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
            "official_action": "Deletion",
            "event_scope_path": _repo_path(scope_path),
            "event_scope_sha256": scope["artifact_sha256"],
            "calendar_sha256": scope["calendar_binding"][
                "merged_sha256"
            ],
            "ranking": (
                "forced_selling_decline_desc,"
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
            "daily_request_mode": "exact_event_observation_window",
            "invalid_daily_response": "missed_trade",
            "permanent_missing_symbol_response": "missed_trade",
            "split_provider": "massive",
            "substitutions_allowed": False,
            "market_price_access_before_search_freeze": False,
        },
        "implementation_files": implementation_files,
        "plugin": {
            "module": "sp500_deletion_plugin",
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
    rendered = (
        json.dumps(validated, indent=2, sort_keys=True) + "\n"
    )
    if contract_path.exists():
        if contract_path.read_text(encoding="utf-8") != rendered:
            raise Sp500DeletionDiscoveryError(
                "content-addressed deletion contract has other content"
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
        Sp500DeletionDiscoveryError,
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

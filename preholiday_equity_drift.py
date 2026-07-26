"""Freeze the fixed-rule IWV pre-holiday intraday drift family."""

from __future__ import annotations

import argparse
import hashlib
import json
from collections.abc import Mapping, Sequence
from datetime import date, datetime
from functools import lru_cache
from pathlib import Path
from typing import Any

import dense_data_collection
import dense_strategy_runtime as runtime
import etf_close_strength_continuation as rolling_support
import fomc_preannouncement as calendar_support
import outcome_exposure
import portfolio_maturity
import strategy_discovery
from historical_store import sha256_file
from learning_data import freeze_dataset_contract
from learning_experiment import DEVELOPMENT_SEARCH_RULE


PROJECT_ROOT = Path(__file__).resolve().parent
CAMPAIGN_ID = portfolio_maturity.V2_CAMPAIGN_ID
FAMILY_ID = runtime.PREHOLIDAY_EQUITY_DRIFT_FAMILY
MECHANISM_FAMILY = "scheduled-preholiday-equity-drift"
STRATEGY_ID = FAMILY_ID
SUCCESSOR_ID = f"{FAMILY_ID}-v1"
RESEARCH_GENERATION = "new_mechanism_family"
DEFAULT_ROOT = PROJECT_ROOT / "strategy_tournament/v2/continuous" / SUCCESSOR_ID
CALENDAR_PATH = (
    PROJECT_ROOT
    / "historical_batches/preholiday_equity_drift/"
    "session-calendar-2009-01-through-2025-12.json"
)
CALENDAR_LINEAGE_KIND = "preholiday-equity-calendar-lineage"
CALENDAR_INSPECTION_KIND = "preholiday-equity-calendar-inspection"
SOURCE_CALENDARS = calendar_support.SOURCE_CALENDARS
SYMBOLS = [runtime.PREHOLIDAY_EQUITY_DRIFT_SYMBOL]
DEVELOPMENT_START = "2010-01-04"
DEVELOPMENT_END = "2018-12-31"
CONFIRMATION_START = "2019-01-02"
CONFIRMATION_END = "2025-12-31"
WARMUP_SESSIONS = dense_data_collection.DAILY_WARMUP_SESSIONS
EMBARGO_SESSIONS = 5
UNSCHEDULED_CLOSURE_PRIOR_SESSIONS = {
    "2012-10-26": "Hurricane Sandy unscheduled closure",
    "2018-12-04": "George H.W. Bush national day of mourning",
    "2025-01-08": "Jimmy Carter national day of mourning",
}
PARAMETERS = {
    "expected_gross_move_fraction": 0.005,
    "stop_fraction": 0.015,
    "maximum_hold_sessions": 1,
    "entry_timing": "scheduled_preholiday_session_open",
    "exit_timing": "same_session_close",
}
RESEARCH_SOURCES = {
    "high_returns_before_holidays": (
        "https://doi.org/10.1111/j.1540-6261.1990.tb03731.x"
    ),
    "us_market_replication": (
        "https://www.cambridge.org/core/journals/"
        "journal-of-financial-and-quantitative-analysis/article/abs/"
        "holiday-effects-and-stock-returns-further-evidence/"
        "7D99DF3FE94C1636B773FAFA751937C5"
    ),
    "decline_and_reversal_falsifier": (
        "https://doi.org/10.1016/j.intfin.2005.12.001"
    ),
    "iwv_fund_authority": (
        "https://www.ishares.com/us/products/239714/"
        "ishares-russell-3000-etf"
    ),
}


class PreholidayEquityDriftError(ValueError):
    """The calendar, evidence partitions, or fixed rule drifted."""


def _repo_path(path: Path) -> str:
    try:
        return path.resolve().relative_to(PROJECT_ROOT.resolve()).as_posix()
    except ValueError as exc:
        raise PreholidayEquityDriftError(
            f"path escaped repository: {path}"
        ) from exc


def _timestamp(value: str, field: str) -> str:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise PreholidayEquityDriftError(f"{field} is invalid") from exc
    if parsed.tzinfo is None:
        raise PreholidayEquityDriftError(
            f"{field} must include a timezone"
        )
    if parsed.date() > date.today():
        raise PreholidayEquityDriftError(f"{field} cannot be future-dated")
    return value


def _write_json(path: Path, value: Any) -> None:
    rendered = json.dumps(value, indent=2, sort_keys=True) + "\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and path.read_text(encoding="utf-8") != rendered:
        raise PreholidayEquityDriftError(
            f"content-addressed output differs: {path}"
        )
    if not path.exists():
        temporary = path.with_suffix(path.suffix + ".tmp")
        temporary.write_text(rendered, encoding="utf-8")
        temporary.replace(path)


def _source_authorities(*, enforce_commit: bool) -> list[dict[str, str]]:
    return calendar_support._source_authorities(
        enforce_commit=enforce_commit
    )


def merge_calendar_rows() -> list[dict[str, str]]:
    """Rebuild all regular opens, retaining full and early closes."""

    merged: dict[str, dict[str, str]] = {}
    for path in SOURCE_CALENDARS:
        raw = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(raw, list):
            raise PreholidayEquityDriftError(
                f"calendar must be an array: {path}"
            )
        for raw_row in raw:
            if not isinstance(raw_row, Mapping):
                continue
            day = raw_row.get("date")
            close_et = raw_row.get("close_et")
            if not (
                isinstance(day, str)
                and "2009-01-01" <= day <= CONFIRMATION_END
                and raw_row.get("open_et") == "09:30"
                and close_et in {"13:00", "16:00"}
            ):
                continue
            row = {
                "date": day,
                "open_et": "09:30",
                "close_et": str(close_et),
            }
            if day in merged and merged[day] != row:
                raise PreholidayEquityDriftError(
                    f"calendar sources disagree on {day}"
                )
            merged[day] = row
    rows = [merged[day] for day in sorted(merged)]
    if not (
        len(rows) == 4_276
        and rows[0]["date"] == "2009-01-02"
        and rows[-1]["date"] == CONFIRMATION_END
    ):
        raise PreholidayEquityDriftError(
            "merged all-session calendar coverage drifted"
        )
    return rows


def preholiday_events() -> list[dict[str, Any]]:
    """Derive scheduled closure-eve sessions without using market outcomes."""

    rows = merge_calendar_rows()
    events: list[dict[str, Any]] = []
    for current, following in zip(rows, rows[1:], strict=False):
        current_day = str(current["date"])
        gap_days = (
            date.fromisoformat(str(following["date"]))
            - date.fromisoformat(current_day)
        ).days
        if (
            current_day < DEVELOPMENT_START
            or gap_days in {1, 3}
            or current_day in UNSCHEDULED_CLOSURE_PRIOR_SESSIONS
        ):
            continue
        events.append(
            {
                "signal_date": current_day,
                "scheduled_close_et": current["close_et"],
                "next_session_date": following["date"],
                "calendar_gap_days": gap_days,
            }
        )
    if not (
        len(events) == 145
        and events[0]["signal_date"] == "2010-01-15"
        and events[-1]["signal_date"] == "2025-12-24"
    ):
        raise PreholidayEquityDriftError(
            "scheduled pre-holiday inventory drifted"
        )
    return events


def build_calendar(
    *, created_at: str, enforce_commit: bool = True
) -> tuple[Path, dict[str, Any]]:
    _timestamp(created_at, "created_at")
    authorities = _source_authorities(enforce_commit=enforce_commit)
    rows = merge_calendar_rows()
    _write_json(CALENDAR_PATH, rows)
    payload = {
        "schema_version": 1,
        "artifact_kind": CALENDAR_LINEAGE_KIND,
        "campaign_id": CAMPAIGN_ID,
        "state": "CALENDAR_MERGED_UNINSPECTED",
        "created_at": created_at,
        "calendar_path": _repo_path(CALENDAR_PATH),
        "calendar_sha256": sha256_file(CALENDAR_PATH),
        "sessions": len(rows),
        "first_session": rows[0]["date"],
        "last_session": rows[-1]["date"],
        "source_authorities": authorities,
        "merge_semantics": (
            "unique 09:30 opens with exact 13:00 or 16:00 closes"
        ),
        "unscheduled_closure_prior_sessions_excluded": dict(
            UNSCHEDULED_CLOSURE_PRIOR_SESSIONS
        ),
        "formal_signal_capacity": len(preholiday_events()),
        "provider_requests_added": 0,
        "market_prices_accessed": False,
        "target_outcomes_accessed": False,
        "broker_actions": 0,
    }
    return strategy_discovery._write_artifact(
        payload,
        DEFAULT_ROOT / "calendar-lineage",
        CALENDAR_LINEAGE_KIND,
    )


@lru_cache(maxsize=8)
def _exposed_iwv_dates(index_sha256: str) -> frozenset[str]:
    if index_sha256 != sha256_file(outcome_exposure.DEFAULT_INDEX):
        raise PreholidayEquityDriftError(
            "outcome-exposure index hash drifted"
        )
    exposed: set[str] = set()
    for record in outcome_exposure.read_index():
        scope = record["scope"]
        if "symbols" in scope:
            if (
                "*" in scope["symbols"]
                or runtime.PREHOLIDAY_EQUITY_DRIFT_SYMBOL
                in scope["symbols"]
            ):
                exposed.update(scope["dates"])
            continue
        for day, symbols in scope["symbols_by_date"].items():
            if (
                "*" in symbols
                or runtime.PREHOLIDAY_EQUITY_DRIFT_SYMBOL in symbols
            ):
                exposed.add(day)
    return frozenset(exposed)


def partitions() -> dict[str, list[str]]:
    rows = merge_calendar_rows()
    calendar = [row["date"] for row in rows]
    positions = {day: index for index, day in enumerate(calendar)}
    development = [
        day for day in calendar if DEVELOPMENT_START <= day <= DEVELOPMENT_END
    ]
    confirmation_all = [
        day
        for day in calendar
        if CONFIRMATION_START <= day <= CONFIRMATION_END
    ]
    embargo = confirmation_all[:EMBARGO_SESSIONS]
    confirmation = confirmation_all[EMBARGO_SESSIONS:]
    event_dates = [item["signal_date"] for item in preholiday_events()]
    development_signals = [
        day for day in event_dates if DEVELOPMENT_START <= day <= DEVELOPMENT_END
    ]
    confirmation_inventory = [
        day for day in event_dates if confirmation[0] <= day <= CONFIRMATION_END
    ]
    exposed = _exposed_iwv_dates(
        sha256_file(outcome_exposure.DEFAULT_INDEX)
    )
    confirmation_signals = [
        day for day in confirmation_inventory if day not in exposed
    ]
    confirmation_excluded = [
        day for day in confirmation_inventory if day in exposed
    ]
    development_start_index = positions[development[0]]
    confirmation_start_index = positions[confirmation[0]]
    development_warmup = calendar[
        development_start_index - WARMUP_SESSIONS : development_start_index
    ]
    confirmation_warmup = calendar[
        confirmation_start_index
        - WARMUP_SESSIONS : confirmation_start_index
    ]
    if not (
        len(development_warmup) == WARMUP_SESSIONS
        and len(development_signals) == 80
        and len(embargo) == EMBARGO_SESSIONS
        and len(confirmation_warmup) == WARMUP_SESSIONS
        and len(confirmation_inventory) == 65
        and len(confirmation_signals) == 48
        and len(confirmation_excluded) == 17
        and set(development_signals).issubset(development)
        and set(confirmation_signals).issubset(confirmation)
        and not set(confirmation_signals) & set(confirmation_excluded)
        and development[-1] < embargo[0] < confirmation[0]
    ):
        raise PreholidayEquityDriftError(
            "fixed pre-holiday evidence partitions drifted"
        )
    return {
        "development_warmup_dates": development_warmup,
        "development_dates": development,
        "development_signal_dates": development_signals,
        "embargo_dates": embargo,
        "confirmation_warmup_dates": confirmation_warmup,
        "confirmation_dates": confirmation,
        "confirmation_inventory_signal_dates": confirmation_inventory,
        "confirmation_excluded_exposed_signal_dates": confirmation_excluded,
        "confirmation_signal_dates": confirmation_signals,
    }


def _scope(dates: Sequence[str]) -> dict[str, Any]:
    return outcome_exposure.validate_scope(
        {"dates": list(dates), "symbols": list(SYMBOLS)}
    )


def _calendar_inspection(
    *, enforce_commit: bool
) -> tuple[Path, dict[str, Any]]:
    expected_hash = sha256_file(CALENDAR_PATH)
    index_hash = sha256_file(outcome_exposure.DEFAULT_INDEX)
    matches: list[tuple[Path, dict[str, Any]]] = []
    for path in sorted((DEFAULT_ROOT / "calendar-inspection").glob("*.json")):
        value = strategy_discovery.load_artifact(
            path, expected_kind=CALENDAR_INSPECTION_KIND
        )
        if (
            value.get("state") == "CALENDAR_INSPECTED_READY"
            and value.get("calendar_path") == _repo_path(CALENDAR_PATH)
            and value.get("calendar_sha256") == expected_hash
            and value.get("outcome_exposure_index_sha256") == index_hash
            and value.get("untouched_confirmation_signal_capacity") == 48
            and all(value.get("checks", {}).values())
            and value.get("market_prices_accessed") is False
            and value.get("target_outcomes_accessed") is False
            and value.get("broker_actions") == 0
        ):
            matches.append((path, value))
    if len(matches) != 1:
        raise PreholidayEquityDriftError(
            "expected one current independent calendar inspection"
        )
    if enforce_commit:
        strategy_discovery.require_committed(CALENDAR_PATH)
        strategy_discovery.require_committed(matches[0][0])
    return matches[0]


def validate_contract(
    contract: Mapping[str, Any], *, enforce_commit: bool = True
) -> None:
    split = partitions()
    inspection_path, inspection = _calendar_inspection(
        enforce_commit=enforce_commit
    )
    if not (
        contract.get("campaign_id") == CAMPAIGN_ID
        and contract.get("family_id") == FAMILY_ID
        and contract.get("mechanism_family") == MECHANISM_FAMILY
        and contract.get("successor_id") == SUCCESSOR_ID
        and contract.get("research_generation") == RESEARCH_GENERATION
        and contract.get("new_mechanism_family_slot_consumed") is True
        and len(contract.get("trial_family", [])) == 1
        and contract["trial_family"][0]["parameters"] == PARAMETERS
        and all(contract.get(field) == values for field, values in split.items())
        and contract.get("development_scope")
        == _scope(
            [
                *split["development_warmup_dates"],
                *split["development_dates"],
            ]
        )
        and contract.get("confirmation_scope")
        == _scope(split["confirmation_signal_dates"])
        and contract.get("calendar_sha256") == sha256_file(CALENDAR_PATH)
        and contract.get("calendar_inspection_path")
        == _repo_path(inspection_path)
        and contract.get("calendar_inspection_sha256")
        == inspection["artifact_sha256"]
    ):
        raise PreholidayEquityDriftError(
            "pre-holiday family contract drifted"
        )
    records = outcome_exposure.read_index()
    outcome_exposure.assert_untouched(contract["development_scope"], records)
    outcome_exposure.assert_untouched(contract["confirmation_scope"], records)
    outcome_exposure.assert_disjoint(
        [contract["development_scope"], contract["confirmation_scope"]]
    )
    if enforce_commit:
        strategy_discovery.require_committed(Path(__file__).resolve())


def freeze_contract(
    *, created_at: str, enforce_commit: bool = True
) -> tuple[Path, dict[str, Any], Path]:
    _timestamp(created_at, "created_at")
    if enforce_commit:
        strategy_discovery.require_committed(Path(__file__).resolve())
    rolling = rolling_support._rolling_slot_authority(
        enforce_commit=enforce_commit
    )
    inspection_path, inspection = _calendar_inspection(
        enforce_commit=enforce_commit
    )
    split = partitions()
    development_scope = _scope(
        [
            *split["development_warmup_dates"],
            *split["development_dates"],
        ]
    )
    confirmation_scope = _scope(split["confirmation_signal_dates"])
    records = outcome_exposure.read_index()
    outcome_exposure.assert_untouched(development_scope, records)
    outcome_exposure.assert_untouched(confirmation_scope, records)
    outcome_exposure.assert_disjoint([development_scope, confirmation_scope])
    evidence_paths = [
        _repo_path(CALENDAR_PATH),
        _repo_path(inspection_path),
        "strategy_tournament/v2/OUTCOME_EXPOSURE_INDEX.jsonl",
    ]
    capacity_path, _ = freeze_dataset_contract(
        {
            "schema_version": 1,
            "dataset_id": f"dataset-{SUCCESSOR_ID}-capacity",
            "registered_at": created_at,
            "requested_dates": [
                *split["development_warmup_dates"],
                *split["development_dates"],
                *split["embargo_dates"],
                *split["confirmation_dates"],
            ],
            "dataset_payload": {
                "lane": "development",
                "claim_scope": "DEVELOPMENT_ONLY",
                "evidence_paths": evidence_paths,
                "inspected": True,
                "point_in_time_evidence": True,
                "dense_capacity": {
                    "family_id": FAMILY_ID,
                    "mechanism_family": MECHANISM_FAMILY,
                    "formal_capacity": len(preholiday_events()),
                    "capacity_unit": (
                        "scheduled NYSE closure-eve regular sessions"
                    ),
                    "development_sessions": len(split["development_dates"]),
                    "development_signal_dates": len(
                        split["development_signal_dates"]
                    ),
                    "embargo_sessions": len(split["embargo_dates"]),
                    "confirmation_sessions": len(
                        split["confirmation_dates"]
                    ),
                    "confirmation_signal_dates": len(
                        split["confirmation_signal_dates"]
                    ),
                    "calendar_sha256": sha256_file(CALENDAR_PATH),
                    "provider_requests": 0,
                    "market_prices_accessed": False,
                    "outcomes_accessed": False,
                },
            },
        },
        DEFAULT_ROOT / "capacity",
    )
    contract = {
        "schema_version": 1,
        "campaign_id": CAMPAIGN_ID,
        "experiment_id": f"experiment-{SUCCESSOR_ID}",
        "family_id": FAMILY_ID,
        "mechanism_family": MECHANISM_FAMILY,
        "strategy_id": STRATEGY_ID,
        "successor_id": SUCCESSOR_ID,
        "research_generation": RESEARCH_GENERATION,
        "new_mechanism_family_slot_consumed": True,
        "rolling_slot_authority": rolling,
        "created_at": created_at,
        "status": "INVENTED",
        "dataset_lane": "development",
        "selection_mode": "development_search",
        "mechanism": (
            "Scheduled exchange closures can concentrate optimistic risk "
            "bearing and reduced selling pressure into the preceding session."
        ),
        "expected_holding_behavior": (
            "Long IWV at the scheduled pre-holiday session open and flat at "
            "that session's exact close or the fixed protective stop."
        ),
        "entry_rule": (
            "Enter IWV at the first executable regular-session price on a "
            "frozen scheduled exchange-closure eve."
        ),
        "stop_rule": (
            "Protect at 1.5% below entry; same-session stop and close "
            "ambiguity resolves stop-first."
        ),
        "exit_rule": (
            "Exit at the scheduled 13:00 or 16:00 ET session close unless "
            "the stop has already resolved the trade."
        ),
        "ranking_rule": (
            "IWV is the sole tradable instrument and always ranks first."
        ),
        "selection_rule": (
            "Evaluate the sole preregistered rule on rolling-origin OOF "
            "evidence; no holiday, date, symbol, or parameter alternative "
            "is selected."
        ),
        "primary_outcome": (
            "Selection-aware chronological account log growth after costs."
        ),
        "material_difference_rationale": (
            "This scheduled exchange-closure mechanism is distinct from "
            "price, breadth, macro, FOMC, disclosure, and corporate-event "
            "families already evaluated."
        ),
        "parameter_grid": {key: [value] for key, value in PARAMETERS.items()},
        "winner_selection": DEVELOPMENT_SEARCH_RULE,
        "universe": {
            "symbols": list(SYMBOLS),
            "tradable_symbols": list(SYMBOLS),
            "feature_only_symbols": [],
            "point_in_time": True,
        },
        "universe_requirements": {
            "security_type": "long-only unlevered broad U.S. equity ETF",
            "fund_inception_before_development": True,
            "selection_basis": (
                "IWV is broad, long-lived, operationally liquid, and absent "
                "from exact development exposure."
            ),
        },
        "execution_assumptions": {
            "long_only": True,
            "entry": "scheduled_preholiday_session_open",
            "maximum_hold_sessions": 1,
            "ambiguity": "stop_first",
            "missing_data": "missed_trade_no_substitute",
            "minimum_gross_to_primary_round_trip_cost": 5.0,
            "overnight_protection": "not_applicable_flat_same_session",
        },
        "falsification_criteria": {
            "minimum_20bps_log_growth": 0.0,
            "minimum_stressed_profit_factor": 1.2,
            "maximum_drawdown_r": 6.0,
            "minimum_deflated_sharpe_probability": 0.9,
            "maximum_pbo_probability": 0.5,
            "all_rolling_folds_positive": True,
        },
        "minimum_evidence": {
            "configured_floor": 50,
            "confirmation_floor": 20,
            "alpha": 0.1,
            "power": 0.8,
        },
        "contamination_risks": [
            "The complete closure inventory and sole rule freeze before IWV price access.",
            "Unscheduled exchange closures are excluded prospectively.",
            "Confirmation reads only exact clean signal-day IWV bars while zero-return account days remain explicit.",
        ],
        "production_compatibility_risks": [
            "The entry requires a marketable opening-period limit and can miss.",
            "Early-close sessions require schedule-aware monitoring and exit.",
            "A fast opening move can outrun the marketable limit.",
        ],
        **split,
        "preholiday_events": preholiday_events(),
        "research_sources": RESEARCH_SOURCES,
        "confirmation_signal_capacity": len(
            split["confirmation_signal_dates"]
        ),
        "development_scope": development_scope,
        "confirmation_scope": confirmation_scope,
        "outcome_exposure_index_sha256": outcome_exposure.audit()[
            "index_sha256"
        ],
        "calendar_path": _repo_path(CALENDAR_PATH),
        "calendar_sha256": sha256_file(CALENDAR_PATH),
        "calendar_inspection_path": _repo_path(inspection_path),
        "calendar_inspection_sha256": inspection["artifact_sha256"],
        "historical_data_contract": {
            "daily_provider": "yahoo",
            "daily_endpoint": dense_data_collection.YAHOO_CHART_ENDPOINT,
            "daily_request_mode": "symbol_range",
            "confirmation_request_mode": "exact_signal_dates_only",
            "daily_adjustment": (
                dense_data_collection.YAHOO_SOURCE_RECOVERY_ADJUSTMENT
            ),
            "split_provider": "massive",
            "provider_substitutions_allowed": False,
            "no_purchase_required": True,
            "retries_permitted": 0,
        },
        "costs_bps_per_side": [5, 10, 20],
        "partitions": {
            "rolling_origin": True,
            "confirmation_untouched": True,
            "development_training_contaminated": False,
            "five_session_embargo": True,
            "sparse_confirmation_outcome_access": True,
        },
        "falsifiers": [
            "nonpositive 20-bps log growth",
            "20-bps profit factor below 1.20",
            "drawdown above 6R",
            "any nonpositive rolling fold",
            "selection-aware statistical rejection",
            "incomplete rule or account-path capture",
            "insufficient dynamically powered confirmation inventory",
        ],
        "implementation_files": [
            "preholiday_equity_drift.py",
            "preholiday_equity_drift_inspection.py",
            "dense_data_collection.py",
            "dense_data_collection_inspection.py",
            "dense_collection_plan_inspection.py",
            "dense_strategy_plugin.py",
            "dense_strategy_runtime.py",
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
        "capacity_manifest": _repo_path(capacity_path),
    }
    validated = strategy_discovery._validate_family_contract(contract)
    validate_contract(validated, enforce_commit=enforce_commit)
    digest = hashlib.sha256(
        json.dumps(
            validated,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        ).encode()
    ).hexdigest()
    path = DEFAULT_ROOT / "family-contract" / f"contract-{digest}.json"
    _write_json(path, validated)
    return path, validated, capacity_path


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "command", choices=("build-calendar", "freeze", "status")
    )
    parser.add_argument("--created-at")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "status":
            split = partitions()
            result = {
                "family_id": FAMILY_ID,
                "calendar_exists": CALENDAR_PATH.exists(),
                "family_contracts": len(
                    list((DEFAULT_ROOT / "family-contract").glob("*.json"))
                ),
                "formal_capacity": len(preholiday_events()),
                "development_signal_capacity": len(
                    split["development_signal_dates"]
                ),
                "confirmation_signal_inventory": len(
                    split["confirmation_inventory_signal_dates"]
                ),
                "confirmation_signal_capacity": len(
                    split["confirmation_signal_dates"]
                ),
                "provider_requests_added": 0,
                "market_prices_accessed": False,
                "broker_actions": 0,
            }
        elif args.command == "build-calendar":
            if not args.created_at:
                raise PreholidayEquityDriftError(
                    "build-calendar requires --created-at"
                )
            path, value = build_calendar(created_at=args.created_at)
            result = {
                "path": _repo_path(path),
                "calendar_path": value["calendar_path"],
                "sessions": value["sessions"],
                "formal_capacity": value["formal_signal_capacity"],
                "provider_requests_added": 0,
                "market_prices_accessed": False,
            }
        else:
            if not args.created_at:
                raise PreholidayEquityDriftError(
                    "freeze requires --created-at"
                )
            path, contract, capacity = freeze_contract(
                created_at=args.created_at
            )
            result = {
                "path": _repo_path(path),
                "capacity_manifest": _repo_path(capacity),
                "family_id": contract["family_id"],
                "trial_count": len(contract["trial_family"]),
                "development_signal_capacity": len(
                    contract["development_signal_dates"]
                ),
                "confirmation_signal_capacity": contract[
                    "confirmation_signal_capacity"
                ],
                "provider_requests": 0,
                "confirmation_access_permitted": False,
                "broker_actions": 0,
            }
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    except (
        PreholidayEquityDriftError,
        OSError,
        json.JSONDecodeError,
        outcome_exposure.OutcomeExposureError,
        strategy_discovery.StrategyDiscoveryError,
    ) as exc:
        print(
            json.dumps(
                {"error": str(exc), "error_type": type(exc).__name__},
                indent=2,
                sort_keys=True,
            )
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

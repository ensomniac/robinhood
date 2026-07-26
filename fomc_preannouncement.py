"""Freeze the fixed-rule SCHB pre-FOMC drift family before price access."""

from __future__ import annotations

import argparse
import hashlib
import json
from collections.abc import Mapping, Sequence
from datetime import datetime
from pathlib import Path
from typing import Any

import continuous_strategy_discovery as continuous_calendar
import dense_capacity_inventory
import dense_data_collection
import dense_session_calendar
import dense_strategy_runtime as runtime
import etf_close_strength_continuation as rolling_support
import etf_pullback_replication as early_calendar
import outcome_exposure
import portfolio_maturity
import strategy_discovery
from historical_store import sha256_file
from learning_data import freeze_dataset_contract
from learning_experiment import DEVELOPMENT_SEARCH_RULE


PROJECT_ROOT = Path(__file__).resolve().parent
CAMPAIGN_ID = portfolio_maturity.V2_CAMPAIGN_ID
FAMILY_ID = runtime.FOMC_PREANNOUNCEMENT_FAMILY
MECHANISM_FAMILY = "scheduled-fomc-preannouncement-equity-drift"
STRATEGY_ID = FAMILY_ID
SUCCESSOR_ID = f"{FAMILY_ID}-v1"
RESEARCH_GENERATION = "new_mechanism_family"
DEFAULT_ROOT = PROJECT_ROOT / "strategy_tournament/v2/continuous" / SUCCESSOR_ID
CALENDAR_PATH = (
    PROJECT_ROOT
    / "historical_batches/fomc_preannouncement/"
    "session-calendar-2009-01-through-2025-12.json"
)
CALENDAR_LINEAGE_KIND = "fomc-preannouncement-calendar-lineage"
CALENDAR_INSPECTION_KIND = "fomc-preannouncement-calendar-inspection"
SOURCE_CALENDARS = (
    early_calendar.CALENDAR_PATH,
    continuous_calendar.CALENDAR_PATH,
    dense_capacity_inventory.DEFAULT_CALENDAR,
)
DENSE_CALENDAR_COLLECTION = (
    PROJECT_ROOT
    / "strategy_tournament/v2/calendar/collection/"
    "dense-session-calendar-collection-"
    "95b4cba195ad5d472d397a64e38f4ea9f025efa3bf58d232f966c3b5d5213a05.json"
)
SYMBOLS = [runtime.FOMC_PREANNOUNCEMENT_SYMBOL]
DEVELOPMENT_START = "2011-01-03"
DEVELOPMENT_END = "2018-12-31"
CONFIRMATION_START = "2019-01-02"
CONFIRMATION_END = "2025-12-31"
WARMUP_SESSIONS = 200
EMBARGO_SESSIONS = 5
PARAMETERS = {
    "expected_gross_move_fraction": 0.005,
    "stop_fraction": 0.015,
    "maximum_hold_sessions": 1,
    "entry_timing": "prior_session_close",
    "exit_timing": "decision_session_close",
}
OFFICIAL_FOMC_SOURCES = {
    "historical_calendar_template": (
        "https://www.federalreserve.gov/monetarypolicy/"
        "fomchistoricalYYYY.htm"
    ),
    "current_calendar": (
        "https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm"
    ),
    "mechanism_research": (
        "https://www.newyorkfed.org/research/staff_reports/sr512.html"
    ),
}
FOMC_DECISION_DATES_BY_YEAR = {
    2011: (
        "2011-01-26", "2011-03-15", "2011-04-27", "2011-06-22",
        "2011-08-09", "2011-09-21", "2011-11-02", "2011-12-13",
    ),
    2012: (
        "2012-01-25", "2012-03-13", "2012-04-25", "2012-06-20",
        "2012-08-01", "2012-09-13", "2012-10-24", "2012-12-12",
    ),
    2013: (
        "2013-01-30", "2013-03-20", "2013-05-01", "2013-06-19",
        "2013-07-31", "2013-09-18", "2013-10-30", "2013-12-18",
    ),
    2014: (
        "2014-01-29", "2014-03-19", "2014-04-30", "2014-06-18",
        "2014-07-30", "2014-09-17", "2014-10-29", "2014-12-17",
    ),
    2015: (
        "2015-01-28", "2015-03-18", "2015-04-29", "2015-06-17",
        "2015-07-29", "2015-09-17", "2015-10-28", "2015-12-16",
    ),
    2016: (
        "2016-01-27", "2016-03-16", "2016-04-27", "2016-06-15",
        "2016-07-27", "2016-09-21", "2016-11-02", "2016-12-14",
    ),
    2017: (
        "2017-02-01", "2017-03-15", "2017-05-03", "2017-06-14",
        "2017-07-26", "2017-09-20", "2017-11-01", "2017-12-13",
    ),
    2018: (
        "2018-01-31", "2018-03-21", "2018-05-02", "2018-06-13",
        "2018-08-01", "2018-09-26", "2018-11-08", "2018-12-19",
    ),
    2019: (
        "2019-01-30", "2019-03-20", "2019-05-01", "2019-06-19",
        "2019-07-31", "2019-09-18", "2019-10-30", "2019-12-11",
    ),
    2020: (
        "2020-01-29", "2020-04-29", "2020-06-10", "2020-07-29",
        "2020-09-16", "2020-11-05", "2020-12-16",
    ),
    2021: (
        "2021-01-27", "2021-03-17", "2021-04-28", "2021-06-16",
        "2021-07-28", "2021-09-22", "2021-11-03", "2021-12-15",
    ),
    2022: (
        "2022-01-26", "2022-03-16", "2022-05-04", "2022-06-15",
        "2022-07-27", "2022-09-21", "2022-11-02", "2022-12-14",
    ),
    2023: (
        "2023-02-01", "2023-03-22", "2023-05-03", "2023-06-14",
        "2023-07-26", "2023-09-20", "2023-11-01", "2023-12-13",
    ),
    2024: (
        "2024-01-31", "2024-03-20", "2024-05-01", "2024-06-12",
        "2024-07-31", "2024-09-18", "2024-11-07", "2024-12-18",
    ),
    2025: (
        "2025-01-29", "2025-03-19", "2025-05-07", "2025-06-18",
        "2025-07-30", "2025-09-17", "2025-10-29", "2025-12-10",
    ),
}


class FomcPreannouncementError(ValueError):
    """The official calendar, evidence partitions, or fixed rule drifted."""


def _repo_path(path: Path) -> str:
    try:
        return path.resolve().relative_to(PROJECT_ROOT.resolve()).as_posix()
    except ValueError as exc:
        raise FomcPreannouncementError(f"path escaped repository: {path}") from exc


def _timestamp(value: str, field: str) -> str:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise FomcPreannouncementError(f"{field} is invalid") from exc
    if parsed.tzinfo is None:
        raise FomcPreannouncementError(f"{field} must include a timezone")
    return value


def _write_json(path: Path, value: Any) -> None:
    rendered = json.dumps(value, indent=2, sort_keys=True) + "\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and path.read_text(encoding="utf-8") != rendered:
        raise FomcPreannouncementError(f"content-addressed output differs: {path}")
    if not path.exists():
        temporary = path.with_suffix(path.suffix + ".tmp")
        temporary.write_text(rendered, encoding="utf-8")
        temporary.replace(path)


def _source_authorities(*, enforce_commit: bool) -> list[dict[str, str]]:
    early_path, early = early_calendar._calendar_data_inspection(
        enforce_commit=enforce_commit
    )
    continuous_path, continuous = (
        continuous_calendar._data_inspection_for_calendar(
            continuous_calendar.CALENDAR_PATH,
            root=continuous_calendar.CALENDAR_ROOT,
            enforce_commit=enforce_commit,
        )
    )
    if enforce_commit:
        strategy_discovery.require_committed(DENSE_CALENDAR_COLLECTION)
    dense = strategy_discovery.load_artifact(
        DENSE_CALENDAR_COLLECTION,
        expected_kind=dense_session_calendar.COLLECTION_KIND,
    )
    if not (
        dense.get("state") == "CALENDAR_COLLECTED_UNINSPECTED"
        and dense.get("calendar_path")
        == _repo_path(dense_capacity_inventory.DEFAULT_CALENDAR)
        and dense.get("calendar_sha256")
        == sha256_file(dense_capacity_inventory.DEFAULT_CALENDAR)
        and dense.get("market_prices_accessed") is False
        and dense.get("target_outcomes_accessed") is False
        and dense.get("broker_actions") == 0
    ):
        raise FomcPreannouncementError("dense calendar authority drifted")
    return [
        {
            "calendar_path": _repo_path(early_calendar.CALENDAR_PATH),
            "calendar_sha256": sha256_file(early_calendar.CALENDAR_PATH),
            "authority_path": _repo_path(early_path),
            "authority_sha256": early["artifact_sha256"],
        },
        {
            "calendar_path": _repo_path(continuous_calendar.CALENDAR_PATH),
            "calendar_sha256": sha256_file(continuous_calendar.CALENDAR_PATH),
            "authority_path": _repo_path(continuous_path),
            "authority_sha256": continuous["artifact_sha256"],
        },
        {
            "calendar_path": _repo_path(dense_capacity_inventory.DEFAULT_CALENDAR),
            "calendar_sha256": sha256_file(
                dense_capacity_inventory.DEFAULT_CALENDAR
            ),
            "authority_path": _repo_path(DENSE_CALENDAR_COLLECTION),
            "authority_sha256": dense["artifact_sha256"],
        },
    ]


def merge_calendar_rows() -> list[dict[str, str]]:
    """Rebuild the full-session 2009-2025 calendar from frozen source rows."""

    merged: dict[str, dict[str, str]] = {}
    for path in SOURCE_CALENDARS:
        raw = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(raw, list):
            raise FomcPreannouncementError(f"calendar must be an array: {path}")
        for row in raw:
            if not (
                isinstance(row, Mapping)
                and isinstance(row.get("date"), str)
                and row.get("open_et") == "09:30"
                and row.get("close_et") == "16:00"
                and "2009-01-01" <= row["date"] <= CONFIRMATION_END
            ):
                continue
            normalized = {
                "date": str(row["date"]),
                "open_et": "09:30",
                "close_et": "16:00",
            }
            previous = merged.get(normalized["date"])
            if previous is not None and previous != normalized:
                raise FomcPreannouncementError(
                    f"calendar sources disagree on {normalized['date']}"
                )
            merged[normalized["date"]] = normalized
    rows = [merged[day] for day in sorted(merged)]
    if not rows or rows[0]["date"] > "2009-01-02" or rows[-1]["date"] != "2025-12-31":
        raise FomcPreannouncementError("merged calendar coverage is incomplete")
    return rows


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
        "merge_semantics": "unique full 09:30-16:00 sessions only",
        "provider_requests_added": 0,
        "market_prices_accessed": False,
        "target_outcomes_accessed": False,
        "broker_actions": 0,
    }
    return strategy_discovery._write_artifact(
        payload, DEFAULT_ROOT / "calendar-lineage", CALENDAR_LINEAGE_KIND
    )


def _calendar_inspection(
    *, enforce_commit: bool
) -> tuple[Path, dict[str, Any]]:
    expected_hash = sha256_file(CALENDAR_PATH)
    matches: list[tuple[Path, dict[str, Any]]] = []
    for path in sorted((DEFAULT_ROOT / "calendar-inspection").glob("*.json")):
        value = strategy_discovery.load_artifact(
            path, expected_kind=CALENDAR_INSPECTION_KIND
        )
        if (
            value.get("state") == "CALENDAR_INSPECTED_READY"
            and value.get("calendar_path") == _repo_path(CALENDAR_PATH)
            and value.get("calendar_sha256") == expected_hash
            and all(value.get("checks", {}).values())
            and value.get("market_prices_accessed") is False
            and value.get("target_outcomes_accessed") is False
            and value.get("broker_actions") == 0
        ):
            matches.append((path, value))
    if len(matches) != 1:
        raise FomcPreannouncementError(
            "expected one independent inspection of the exact merged calendar"
        )
    if enforce_commit:
        strategy_discovery.require_committed(CALENDAR_PATH)
        strategy_discovery.require_committed(matches[0][0])
    return matches[0]


def decision_dates() -> list[str]:
    return [
        day
        for year in sorted(FOMC_DECISION_DATES_BY_YEAR)
        for day in FOMC_DECISION_DATES_BY_YEAR[year]
    ]


def partitions() -> dict[str, list[str]]:
    rows = merge_calendar_rows()
    calendar = [row["date"] for row in rows]
    positions = {day: index for index, day in enumerate(calendar)}
    development = [
        day for day in calendar if DEVELOPMENT_START <= day <= DEVELOPMENT_END
    ]
    confirmation_all = [
        day for day in calendar if CONFIRMATION_START <= day <= CONFIRMATION_END
    ]
    embargo = confirmation_all[:EMBARGO_SESSIONS]
    confirmation = confirmation_all[EMBARGO_SESSIONS:]
    development_decisions = [
        day for day in decision_dates() if DEVELOPMENT_START <= day <= DEVELOPMENT_END
    ]
    confirmation_decisions = [
        day for day in decision_dates() if confirmation[0] <= day <= CONFIRMATION_END
    ]
    if any(day not in positions for day in decision_dates()):
        raise FomcPreannouncementError("an FOMC decision date is not a full session")
    development_signals = [calendar[positions[day] - 1] for day in development_decisions]
    confirmation_signals = [
        calendar[positions[day] - 1] for day in confirmation_decisions
    ]
    warmup_start = positions[development[0]] - WARMUP_SESSIONS
    development_warmup = calendar[warmup_start : positions[development[0]]]
    confirmation_warmup = calendar[
        positions[confirmation[0]] - WARMUP_SESSIONS : positions[confirmation[0]]
    ]
    if not (
        len(development_warmup) == WARMUP_SESSIONS
        and len(development_signals) == 64
        and len(embargo) == EMBARGO_SESSIONS
        and len(confirmation_warmup) == WARMUP_SESSIONS
        and len(confirmation_signals) == 55
        and set(development_signals).issubset(development)
        and set(confirmation_signals).issubset(confirmation)
        and development[-1] < embargo[0] < confirmation[0]
    ):
        raise FomcPreannouncementError("fixed FOMC evidence partitions drifted")
    return {
        "development_warmup_dates": development_warmup,
        "development_dates": development,
        "development_signal_dates": development_signals,
        "embargo_dates": embargo,
        "confirmation_warmup_dates": confirmation_warmup,
        "confirmation_dates": confirmation,
        "confirmation_signal_dates": confirmation_signals,
    }


def _scope(dates: Sequence[str]) -> dict[str, Any]:
    return outcome_exposure.validate_scope(
        {"dates": list(dates), "symbols": list(SYMBOLS)}
    )


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
        == _scope([*split["development_warmup_dates"], *split["development_dates"]])
        and contract.get("confirmation_scope")
        == _scope(split["confirmation_signal_dates"])
        and contract.get("calendar_sha256") == sha256_file(CALENDAR_PATH)
        and contract.get("calendar_inspection_path") == _repo_path(inspection_path)
        and contract.get("calendar_inspection_sha256")
        == inspection["artifact_sha256"]
    ):
        raise FomcPreannouncementError("pre-FOMC family contract drifted")
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
        [*split["development_warmup_dates"], *split["development_dates"]]
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
                    "formal_capacity": len(decision_dates()),
                    "capacity_unit": "scheduled regular FOMC decision sessions",
                    "development_sessions": len(split["development_dates"]),
                    "development_signal_dates": len(
                        split["development_signal_dates"]
                    ),
                    "embargo_sessions": len(split["embargo_dates"]),
                    "confirmation_sessions": len(split["confirmation_dates"]),
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
            "Scheduled FOMC decisions can concentrate preannouncement risk "
            "bearing and dealer positioning into the preceding close-to-close interval."
        ),
        "expected_holding_behavior": (
            "Long SCHB at the final regular-session close before a scheduled "
            "FOMC decision and flat at the decision-session close or stop."
        ),
        "entry_rule": (
            "Enter SCHB at the completed close immediately preceding a frozen "
            "scheduled FOMC decision session."
        ),
        "stop_rule": (
            "Protect at 1.5% below entry; a decision-session opening gap through "
            "the stop exits at the open and intraday stop ambiguity is stop-first."
        ),
        "exit_rule": (
            "Exit at the scheduled decision-session close unless the frozen stop "
            "has already resolved the position."
        ),
        "ranking_rule": "SCHB is the sole tradable instrument and always ranks first.",
        "selection_rule": (
            "Evaluate the sole preregistered rule on rolling-origin OOF evidence; "
            "no date, symbol, parameter, or announcement alternative is selected."
        ),
        "primary_outcome": "Selection-aware chronological account log growth after costs.",
        "material_difference_rationale": (
            "This scheduled-policy risk-bearing mechanism is distinct from the "
            "repo's price, breadth, gap, earnings, insider, buyback, and index-event rules."
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
            "complete_frozen_daily_history": True,
            "selection_basis": "SCHB was absent from the global outcome-exposure index.",
        },
        "execution_assumptions": {
            "long_only": True,
            "entry": "prior_session_close",
            "maximum_hold_sessions": 1,
            "ambiguity": "stop_first",
            "missing_data": "missed_trade_no_substitute",
            "minimum_gross_to_primary_round_trip_cost": 5.0,
            "overnight_protection": "gtc_required",
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
            "The complete official decision calendar and sole rule are frozen before SCHB price access.",
            "Unscheduled actions, cancelled meetings, notation votes, and parameter alternatives are excluded.",
            "Confirmation pairs remain inaccessible until an exact inspected winner is frozen.",
        ],
        "production_compatibility_risks": [
            "The entry requires a marketable closing-period limit and can miss.",
            "The overnight position requires confirmed GTC protection.",
            "Decision-session gaps can exceed the modeled stop.",
        ],
        **split,
        "fomc_decision_dates": decision_dates(),
        "fomc_sources": OFFICIAL_FOMC_SOURCES,
        "confirmation_signal_capacity": len(split["confirmation_signal_dates"]),
        "development_scope": development_scope,
        "confirmation_scope": confirmation_scope,
        "outcome_exposure_index_sha256": outcome_exposure.audit()["index_sha256"],
        "calendar_path": _repo_path(CALENDAR_PATH),
        "calendar_sha256": sha256_file(CALENDAR_PATH),
        "calendar_inspection_path": _repo_path(inspection_path),
        "calendar_inspection_sha256": inspection["artifact_sha256"],
        "historical_data_contract": {
            "daily_provider": "yahoo",
            "daily_endpoint": dense_data_collection.YAHOO_CHART_ENDPOINT,
            "daily_request_mode": "symbol_range",
            "daily_adjustment": dense_data_collection.YAHOO_SOURCE_RECOVERY_ADJUSTMENT,
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
            "fomc_preannouncement.py",
            "fomc_preannouncement_inspection.py",
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
    parser.add_argument("command", choices=("build-calendar", "freeze", "status"))
    parser.add_argument("--created-at")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "status":
            result = {
                "family_id": FAMILY_ID,
                "calendar_exists": CALENDAR_PATH.exists(),
                "family_contracts": len(
                    list((DEFAULT_ROOT / "family-contract").glob("*.json"))
                ),
                "formal_capacity": len(decision_dates()),
                "development_signal_capacity": 64,
                "confirmation_signal_capacity": 55,
                "provider_requests_added": 0,
                "market_prices_accessed": False,
                "broker_actions": 0,
            }
        elif args.command == "build-calendar":
            if not args.created_at:
                raise FomcPreannouncementError(
                    "build-calendar requires --created-at"
                )
            path, value = build_calendar(created_at=args.created_at)
            result = {
                "path": _repo_path(path),
                "calendar_path": value["calendar_path"],
                "sessions": value["sessions"],
                "provider_requests_added": 0,
                "market_prices_accessed": False,
            }
        else:
            if not args.created_at:
                raise FomcPreannouncementError("freeze requires --created-at")
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
        FomcPreannouncementError,
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

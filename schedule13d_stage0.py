"""Freeze, inspect, acquire, and evaluate Schedule 13D Stage 0 evidence."""

from __future__ import annotations

import argparse
import json
import math
import subprocess
import sys
from collections.abc import Mapping, Sequence
from dataclasses import replace
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Any, Protocol
from zoneinfo import ZoneInfo

import etf_or_momentum_stage0 as common
import schedule13d_capacity as capacity
import schedule13d_semantic_capacity as semantic
import schedule13d_symbol_documents as symbols
import sector_etf_rotation_stage0 as daily
from historical_providers import (
    AlpacaConfig,
    AlpacaHistoricalClient,
    HistoricalProviderError,
)
from historical_store import sha256_file


PROJECT_ROOT = Path(__file__).resolve().parent
DATA_ROOT = Path("/Users/ensomniac/trade/historical_data")
CAPACITY_RESULT_SHA256 = (
    "f45b8cc66a6a45d2f559425ba729206492133497182249c26edd30925c05114b"
)
CAPACITY_INSPECTION_SHA256 = (
    "7459f2d37566019dbaef074ac2468102f8287d9ec1b76e00e272085f5f1c98c6"
)
CAPACITY_RESULT_PATH = (
    PROJECT_ROOT
    / "strategy_tournament/v2/schedule13d/symbols/results/"
    f"{capacity.CANDIDATE_ID}-{CAPACITY_RESULT_SHA256}.json"
)
CAPACITY_INSPECTION_PATH = (
    PROJECT_ROOT
    / "strategy_tournament/v2/schedule13d/symbols/inspections/"
    f"{capacity.CANDIDATE_ID}-capacity-{CAPACITY_INSPECTION_SHA256}.json"
)
PRIVATE_ROOT = (
    DATA_ROOT
    / "_derived/schedule13d_stage0/"
    "dataset-schedule-13d-activist-continuation-stage0-2026-07-22-v1"
)
PRIVATE_INPUT_PATH = PRIVATE_ROOT / "frozen-inputs.json.gz"
CONTRACT_ROOT = PROJECT_ROOT / "strategy_tournament/v2/schedule13d/stage0/contracts"
ACTIVATION_ROOT = PROJECT_ROOT / "strategy_tournament/v2/schedule13d/stage0/activations"
INSPECTION_ROOT = PROJECT_ROOT / "strategy_tournament/v2/schedule13d/stage0/inspections"
RESULT_ROOT = PROJECT_ROOT / "research_results"
INSPECTOR_PATH = PROJECT_ROOT / "schedule13d_stage0_inspection.py"
SCHEMA_VERSION = 1
VARIANT_ID = capacity.CANDIDATE_ID
STRATEGY_VERSION = capacity.STRATEGY_VERSION
STAGE0_SIGNAL_COUNT = 40
CONFIRMATION_SIGNAL_COUNT = 20
CONFIRMATION_EMBARGO_SESSIONS = 5
ATR_LENGTH = 14
STOP_ATR_MULTIPLE = 1.5
MAXIMUM_HOLDING_SESSIONS = 5
MAXIMUM_CONCURRENT_POSITIONS = 3
MAXIMUM_NEW_ENTRIES_PER_DAY = 5
PRIMARY_COST_BPS = 5
STRESS_COST_BPS = (10, 20)
CALENDAR_START = date(2021, 1, 1)
CALENDAR_END = date(2026, 2, 1)
EASTERN = ZoneInfo("America/New_York")


class Schedule13dStage0Error(RuntimeError):
    """The exact Stage 0 contract, inputs, or result are inconsistent."""


class DailyBarProvider(Protocol):
    def fetch_bars(
        self,
        symbol: str,
        start: str | datetime,
        end: str | datetime,
        *,
        bar_size: str = "1 day",
        what: str = "TRADES",
        use_rth: bool = True,
    ) -> list[dict[str, Any]]: ...


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise Schedule13dStage0Error(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise Schedule13dStage0Error(f"{path} must contain an object")
    return value


def _write_json(value: Mapping[str, Any], path: Path) -> None:
    symbols._write_json(value, path)


def _published(path: Path) -> None:
    relative = path.resolve().relative_to(PROJECT_ROOT)
    completed = subprocess.run(
        ("git", "status", "--porcelain=v1", "--", str(relative)),
        cwd=PROJECT_ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    if completed.stdout.strip():
        raise Schedule13dStage0Error(f"required artifact is not published: {relative}")


def _observed_holiday(day: date) -> date:
    if day.weekday() == 5:
        return day - timedelta(days=1)
    if day.weekday() == 6:
        return day + timedelta(days=1)
    return day


def _nth_weekday(year: int, month: int, weekday: int, ordinal: int) -> date:
    current = date(year, month, 1)
    current += timedelta(days=(weekday - current.weekday()) % 7)
    return current + timedelta(days=7 * (ordinal - 1))


def _last_weekday(year: int, month: int, weekday: int) -> date:
    next_month = date(year + (month == 12), month % 12 + 1, 1)
    current = next_month - timedelta(days=1)
    return current - timedelta(days=(current.weekday() - weekday) % 7)


def _easter(year: int) -> date:
    """Anonymous Gregorian algorithm."""

    a = year % 19
    b = year // 100
    c = year % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4
    k = c % 4
    ell = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * ell) // 451
    month = (h + ell - 7 * m + 114) // 31
    day = (h + ell - 7 * m + 114) % 31 + 1
    return date(year, month, day)


def _xnys_holidays(year: int) -> set[date]:
    holidays = {
        _observed_holiday(date(year, 1, 1)),
        _nth_weekday(year, 1, 0, 3),
        _nth_weekday(year, 2, 0, 3),
        _easter(year) - timedelta(days=2),
        _last_weekday(year, 5, 0),
        _observed_holiday(date(year, 7, 4)),
        _nth_weekday(year, 9, 0, 1),
        _nth_weekday(year, 11, 3, 4),
        _observed_holiday(date(year, 12, 25)),
    }
    if year >= 2022:
        holidays.add(_observed_holiday(date(year, 6, 19)))
    if year == 2025:
        holidays.add(date(2025, 1, 9))
    # A following year's Saturday New Year is observed in this year.
    following_new_year = date(year + 1, 1, 1)
    if following_new_year.weekday() == 5:
        holidays.add(following_new_year - timedelta(days=1))
    return holidays


def _xnys_sessions(start: date = CALENDAR_START, end: date = CALENDAR_END) -> list[str]:
    holidays = set().union(*(_xnys_holidays(year) for year in range(start.year, end.year + 1)))
    result = []
    current = start
    while current < end:
        if current.weekday() < 5 and current not in holidays:
            result.append(current.isoformat())
        current += timedelta(days=1)
    return result


def _load_capacity_evidence(*, require_published: bool) -> tuple[dict[str, Any], dict[str, Any]]:
    result = _read_json(CAPACITY_RESULT_PATH)
    inspection = _read_json(CAPACITY_INSPECTION_PATH)
    if not (
        result.get("result_sha256") == CAPACITY_RESULT_SHA256
        and result.get("result_sha256")
        == capacity.successor._self_hash(result, "result_sha256")
        and result.get("capacity_passed") is True
        and result.get("candidate_disposition")
        == "CAPACITY_SURVIVOR_STAGE0_FREEZE_REQUIRED"
        and result.get("verified_event_count") == 120
        and result.get("market_outcomes_accessed") is False
        and inspection.get("inspection_sha256") == CAPACITY_INSPECTION_SHA256
        and inspection.get("inspection_sha256")
        == capacity.successor._self_hash(inspection, "inspection_sha256")
        and inspection.get("result_sha256") == result["result_sha256"]
        and inspection.get("stage0_outcome_access_permitted") is False
        and inspection.get("valid") is True
    ):
        raise Schedule13dStage0Error("inspected capacity survivor is invalid")
    if require_published:
        for path in (CAPACITY_RESULT_PATH, CAPACITY_INSPECTION_PATH):
            _published(path)
    return result, inspection


def _verified_events() -> list[dict[str, Any]]:
    semantic_state = semantic._read_gzip_object(semantic._private_result_path())
    resolution_path = symbols._private_result_path(symbols._store_config().root)
    resolution = symbols._read_object(resolution_path)
    if not (
        semantic_state.get("classification_sha256")
        == capacity.successor._self_hash(semantic_state, "classification_sha256")
        and semantic_state.get("market_outcomes_accessed") is False
        and resolution.get("resolution_sha256")
        == capacity.successor._self_hash(resolution, "resolution_sha256")
        and resolution.get("verified_event_count") == 120
        and resolution.get("market_outcomes_accessed") is False
    ):
        raise Schedule13dStage0Error("private capacity lineage is invalid")
    semantic_by_ordinal = {
        int(row["ordinal"]): row for row in semantic_state["records"]
    }
    recovered = {
        int(row["event_ordinal"]): str(row["symbols"][0])
        for row in resolution["records"]
        if row.get("verified_event") is True
    }
    events = []
    for ordinal, row in semantic_by_ordinal.items():
        event_symbol = row.get("event_symbol") if row.get("verified_event") is True else None
        symbol = str(event_symbol or recovered.get(ordinal) or "").strip().upper()
        if not symbol:
            continue
        events.append(
            {
                "event_ordinal": ordinal,
                "event_accession": str(row["accession"]),
                "accepted_at_eastern": str(row["accepted_at"]),
                "accepted_date": str(row["accepted_date"]),
                "subject_cik": str(row["subject_cik"]),
                "symbol": symbol,
                "security_class": str(row["security_class"]),
                "control_category": str(row["control_category"]),
                "symbol_source": (
                    "event_filing" if event_symbol else "latest_causal_issuer_filing"
                ),
            }
        )
    events.sort(key=lambda row: (row["accepted_at_eastern"], row["event_accession"]))
    if len(events) != 120 or len({row["event_ordinal"] for row in events}) != 120:
        raise Schedule13dStage0Error("verified event denominator differs")
    return events


def _partition(events: Sequence[Mapping[str, Any]], sessions: Sequence[str]) -> dict[str, Any]:
    positions = {day: index for index, day in enumerate(sessions)}
    scheduled: list[dict[str, Any]] = []
    capacity_excluded: list[dict[str, Any]] = []
    active_until: list[int] = []
    entries_by_day: dict[str, int] = {}
    for source in events:
        accepted = str(source["accepted_date"])
        later = [day for day in sessions if day > accepted]
        if not later:
            raise Schedule13dStage0Error(f"no next XNYS session after {accepted}")
        entry = later[0]
        entry_index = positions[entry]
        active_until = [index for index in active_until if index >= entry_index]
        reason = None
        if len(active_until) >= MAXIMUM_CONCURRENT_POSITIONS:
            reason = "FROZEN_MAXIMUM_CONCURRENT_POSITIONS"
        elif entries_by_day.get(entry, 0) >= MAXIMUM_NEW_ENTRIES_PER_DAY:
            reason = "FROZEN_MAXIMUM_NEW_ENTRIES_PER_DAY"
        record = {
            **dict(source),
            "entry_session": entry,
            "maximum_hold_exit_session": sessions[
                entry_index + MAXIMUM_HOLDING_SESSIONS - 1
            ],
        }
        if reason:
            capacity_excluded.append({**record, "terminal_reason": reason})
            continue
        scheduled.append(record)
        active_until.append(entry_index + MAXIMUM_HOLDING_SESSIONS - 1)
        entries_by_day[entry] = entries_by_day.get(entry, 0) + 1
    if len(scheduled) < STAGE0_SIGNAL_COUNT + 30 + CONFIRMATION_SIGNAL_COUNT:
        raise Schedule13dStage0Error("capacity-compatible events cannot support partitions")
    stage0 = scheduled[:STAGE0_SIGNAL_COUNT]
    confirmation = scheduled[-CONFIRMATION_SIGNAL_COUNT:]
    first_confirmation = positions[confirmation[0]["entry_session"]]
    middle = scheduled[STAGE0_SIGNAL_COUNT:-CONFIRMATION_SIGNAL_COUNT]
    development = [
        row
        for row in middle
        if positions[row["entry_session"]]
        < first_confirmation - CONFIRMATION_EMBARGO_SESSIONS
    ]
    embargo = [row for row in middle if row not in development]
    if len(development) < 30:
        raise Schedule13dStage0Error("development reserve is below 30 after embargo")
    partitions = {
        "stage0": stage0,
        "development": development,
        "embargo_excluded": embargo,
        "confirmation": confirmation,
        "capacity_excluded": capacity_excluded,
    }
    value: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "dataset_id": (
            "dataset-schedule-13d-activist-continuation-stage0-2026-07-22-v1"
        ),
        "variant_id": VARIANT_ID,
        "verified_event_count": len(events),
        "capacity_compatible_event_count": len(scheduled),
        "stage0_signal_count": len(stage0),
        "development_signal_count": len(development),
        "embargo_excluded_signal_count": len(embargo),
        "confirmation_signal_count": len(confirmation),
        "capacity_excluded_signal_count": len(capacity_excluded),
        "confirmation_embargo_sessions": CONFIRMATION_EMBARGO_SESSIONS,
        "partitions": partitions,
        "market_outcomes_accessed": False,
        "returns_computed": 0,
        "broker_actions": 0,
    }
    value["partition_sha256"] = capacity.successor._self_hash(value, "partition_sha256")
    return value


def _request_contract(record: Mapping[str, Any], sessions: Sequence[str]) -> dict[str, Any]:
    positions = {day: index for index, day in enumerate(sessions)}
    entry_index = positions[str(record["entry_session"])]
    required = list(
        sessions[
            entry_index - (ATR_LENGTH + 1) : entry_index + MAXIMUM_HOLDING_SESSIONS
        ]
    )
    if len(required) != ATR_LENGTH + 1 + MAXIMUM_HOLDING_SESSIONS:
        raise Schedule13dStage0Error("Stage 0 request window is incomplete")
    request: dict[str, Any] = {
        "event_ordinal": int(record["event_ordinal"]),
        "event_accession": str(record["event_accession"]),
        "symbol": str(record["symbol"]),
        "accepted_at_eastern": str(record["accepted_at_eastern"]),
        "entry_session": str(record["entry_session"]),
        "atr_sessions": required[: ATR_LENGTH + 1],
        "outcome_sessions": required[ATR_LENGTH + 1 :],
        "request_start": required[0],
        "request_end_exclusive": (
            date.fromisoformat(required[-1]) + timedelta(days=1)
        ).isoformat(),
        "provider": "Alpaca",
        "endpoint": "https://data.alpaca.markets/v2/stocks/{symbol}/bars",
        "feed": "sip",
        "adjustment": "all",
        "asof": "-",
        "timeframe": "1Day",
    }
    request["request_sha256"] = capacity.successor._self_hash(
        request, "request_sha256"
    )
    return request


def build_contract(*, require_published: bool = True) -> dict[str, Any]:
    capacity_result, capacity_inspection = _load_capacity_evidence(
        require_published=require_published
    )
    sessions = _xnys_sessions()
    partition = _partition(_verified_events(), sessions)
    requests = [_request_contract(row, sessions) for row in partition["partitions"]["stage0"]]
    selection_contract = {
        "universe": "the 120 exact causally verified common-equity Schedule 13D events",
        "event_order": "SEC acceptance timestamp then lexical accession",
        "entry_session": "first complete XNYS session strictly after acceptance date",
        "portfolio_preselection": (
            "chronological conservative five-session occupancy; at most three concurrent "
            "positions and five new entries per day without using exits"
        ),
        "stage0_selection": "first 40 capacity-compatible events",
        "development_reserve": "later compatible events before the five-session confirmation embargo",
        "confirmation_reserve": "last 20 compatible events",
        "parameter_repair_on_this_corpus_permitted": False,
    }
    execution_contract = {
        "direction": "long",
        "entry": "frozen entry-session regular-session open",
        "entry_order_type_model": "marketable limit filled at observed open plus adverse cost",
        "maximum_concurrent_positions": MAXIMUM_CONCURRENT_POSITIONS,
        "maximum_new_entries_per_day": MAXIMUM_NEW_ENTRIES_PER_DAY,
        "maximum_holding_sessions": MAXIMUM_HOLDING_SESSIONS,
        "missing_data_behavior": (
            "preserve denominator as missed entry; no source, symbol, date, or event substitution"
        ),
    }
    exit_contract = {
        "atr_length_complete_sessions": ATR_LENGTH,
        "stop_distance": f"{STOP_ATR_MULTIPLE} times pre-entry ATR14",
        "stop_price": "raw entry open minus frozen stop distance",
        "stop_gap_fill": "worse of planned stop and observed session open",
        "intraday_stop_fill": "planned stop when observed low reaches stop",
        "same_session_ambiguity": "stop_first",
        "profit_target": None,
        "force_flat": "fifth held session observed close",
    }
    gate = {
        "minimum_closed_signals": 30,
        "minimum_expectancy_r_exclusive": 0,
        "minimum_profit_factor": 1.10,
        "maximum_drawdown_r": 8.0,
        "require_positive_20bps_total_r": True,
        "maximum_rule_violations": 0,
        "effect": "SURVIVE_TO_REPRESENTATIVE_DEVELOPMENT_ONLY",
    }
    cost_contract = {
        "bps_per_side": [PRIMARY_COST_BPS, *STRESS_COST_BPS],
        "entry_cost": "raw entry multiplied by one plus bps",
        "exit_cost": "raw exit multiplied by one minus bps",
        "planned_risk": "cost-adjusted entry minus cost-adjusted planned stop",
    }
    value: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "artifact_kind": "stage0-zero-result-contract",
        "campaign_id": capacity.CAMPAIGN_ID,
        "variant_id": VARIANT_ID,
        "strategy_version": STRATEGY_VERSION,
        "mechanism_family": capacity.THEME_ID,
        "capacity_result_sha256": capacity_result["result_sha256"],
        "capacity_inspection_sha256": capacity_inspection["inspection_sha256"],
        "capacity_verified_event_count": capacity_result["verified_event_count"],
        "partition_sha256": partition["partition_sha256"],
        "partition_denominator": {
            "stage0_signals": partition["stage0_signal_count"],
            "development_reserved_signals": partition["development_signal_count"],
            "embargo_excluded_signals": partition["embargo_excluded_signal_count"],
            "confirmation_reserved_signals": partition["confirmation_signal_count"],
            "capacity_excluded_signals": partition["capacity_excluded_signal_count"],
        },
        "frozen_partitions": partition["partitions"],
        "selection_contract": selection_contract,
        "execution_contract": execution_contract,
        "exit_contract": exit_contract,
        "cost_contract": cost_contract,
        "stage0_gate": gate,
        "stage0_input_requests": requests,
        "input_source_contract": {
            "provider": "Alpaca Market Data API",
            "feed": "sip",
            "adjustment": "all",
            "asof": "-",
            "timeframe": "1Day",
            "provider_substitution_permitted": False,
            "request_count": len(requests),
        },
        "access_contract": {
            "market_outcome_input_access_before_inspection_permitted": False,
            "market_outcome_input_access_after_contract_inspection_permitted": True,
            "return_evaluation_before_input_inspection_permitted": False,
            "development_outcome_access_permitted": False,
            "confirmation_outcome_access_permitted": False,
            "broker_actions_permitted": False,
            "maturity_effect": "NONE",
        },
        "implementation_sha256": sha256_file(Path(__file__).resolve()),
        "inspector_sha256": sha256_file(INSPECTOR_PATH),
        "outcomes_previously_accessed_for_exact_rules": False,
        "returns_computed": 0,
        "market_outcomes_accessed": False,
        "claim_limit": "Stage 0 falsification only; cannot contribute maturity evidence.",
    }
    value["rules_hash"] = common._hash(
        {
            "selection_contract": selection_contract,
            "execution_contract": execution_contract,
            "exit_contract": exit_contract,
            "cost_contract": cost_contract,
            "stage0_gate": gate,
        }
    )
    value["contract_sha256"] = capacity.successor._self_hash(value, "contract_sha256")
    return value


def contract_path(value: Mapping[str, Any]) -> Path:
    return CONTRACT_ROOT / f"{VARIANT_ID}-{value['contract_sha256']}.json"


def inspect_contract(path: Path) -> dict[str, Any]:
    recorded = _read_json(path)
    if recorded.get("contract_sha256") != capacity.successor._self_hash(
        recorded, "contract_sha256"
    ):
        raise Schedule13dStage0Error("Stage 0 contract hash is invalid")
    if recorded != build_contract(require_published=False):
        raise Schedule13dStage0Error("Stage 0 contract does not rebuild")
    denominator = recorded["partition_denominator"]
    if not (
        denominator["stage0_signals"] == STAGE0_SIGNAL_COUNT
        and denominator["development_reserved_signals"] >= 30
        and denominator["confirmation_reserved_signals"] == CONFIRMATION_SIGNAL_COUNT
        and len(recorded["stage0_input_requests"]) == STAGE0_SIGNAL_COUNT
        and recorded["outcomes_previously_accessed_for_exact_rules"] is False
        and recorded["returns_computed"] == 0
        and recorded["market_outcomes_accessed"] is False
    ):
        raise Schedule13dStage0Error("Stage 0 anti-tuning boundary differs")
    value: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "inspection_kind": "stage0-zero-result-contract-inspection",
        "variant_id": VARIANT_ID,
        "contract_sha256": recorded["contract_sha256"],
        "contract_file_sha256": sha256_file(path),
        "rules_hash": recorded["rules_hash"],
        "partition_sha256": recorded["partition_sha256"],
        "partition_denominator": denominator,
        "input_request_count": len(recorded["stage0_input_requests"]),
        "stage0_market_outcome_input_access_permitted": True,
        "stage0_return_evaluation_permitted": False,
        "development_outcome_access_permitted": False,
        "confirmation_outcome_access_permitted": False,
        "broker_actions_permitted": False,
        "returns_computed": 0,
        "market_outcomes_accessed": False,
        "maturity_effect": "NONE",
        "valid": True,
    }
    value["inspection_sha256"] = capacity.successor._self_hash(value, "inspection_sha256")
    return value


def _one(pattern: str, description: str) -> Path:
    matches = sorted(PROJECT_ROOT.glob(pattern))
    if len(matches) != 1:
        raise Schedule13dStage0Error(f"expected one {description}; found {len(matches)}")
    return matches[0]


def _contract_and_inspection() -> tuple[Path, Path, dict[str, Any], dict[str, Any]]:
    contract_file = _one(
        "strategy_tournament/v2/schedule13d/stage0/contracts/"
        "schedule-13d-activist-continuation-v1-*.json",
        "Stage 0 contract",
    )
    inspection_file = _one(
        "strategy_tournament/v2/schedule13d/stage0/inspections/"
        "schedule-13d-activist-continuation-v1-contract-*.json",
        "Stage 0 contract inspection",
    )
    frozen = _read_json(contract_file)
    inspected = _read_json(inspection_file)
    if inspected != inspect_contract(contract_file):
        raise Schedule13dStage0Error("Stage 0 contract inspection differs")
    return contract_file, inspection_file, frozen, inspected


def _normalized_bar(raw: Mapping[str, Any]) -> dict[str, Any]:
    try:
        day = str(raw["date_et"])
        values = {field: float(raw[field]) for field in ("open", "high", "low", "close")}
        volume = int(raw.get("volume") or 0)
    except (KeyError, TypeError, ValueError) as exc:
        raise Schedule13dStage0Error("provider daily bar is malformed") from exc
    if not (
        all(math.isfinite(value) and value > 0 for value in values.values())
        and values["low"] <= min(values["open"], values["close"])
        and values["high"] >= max(values["open"], values["close"])
        and volume >= 0
    ):
        raise Schedule13dStage0Error("provider daily bar values are invalid")
    return {"date": day, "o": values["open"], "h": values["high"], "l": values["low"], "c": values["close"], "v": volume}


def _collect_inputs(contract: Mapping[str, Any], provider: DailyBarProvider) -> dict[str, Any]:
    records = []
    for request in contract["stage0_input_requests"]:
        raw_rows = provider.fetch_bars(
            str(request["symbol"]),
            datetime.combine(
                date.fromisoformat(str(request["request_start"])), time(), tzinfo=EASTERN
            ),
            datetime.combine(
                date.fromisoformat(str(request["request_end_exclusive"])),
                time(),
                tzinfo=EASTERN,
            ),
            bar_size="1 day",
            what="TRADES",
            use_rth=True,
        )
        rows = [_normalized_bar(row) for row in raw_rows]
        by_day: dict[str, dict[str, Any]] = {}
        duplicate_days = set()
        for row in rows:
            if row["date"] in by_day:
                duplicate_days.add(row["date"])
            by_day[row["date"]] = row
        required = [*request["atr_sessions"], *request["outcome_sessions"]]
        missing = [day for day in required if day not in by_day]
        extras = sorted(set(by_day) - set(required))
        complete = not missing and not duplicate_days
        records.append(
            {
                "request": dict(request),
                "complete": complete,
                "missing_sessions": missing,
                "duplicate_sessions": sorted(duplicate_days),
                "extra_sessions_ignored": extras,
                "atr_rows": [by_day[day] for day in request["atr_sessions"] if day in by_day],
                "outcome_rows": [by_day[day] for day in request["outcome_sessions"] if day in by_day],
            }
        )
    value: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "dataset_id": (
            "dataset-schedule-13d-activist-continuation-stage0-2026-07-22-v1"
        ),
        "variant_id": VARIANT_ID,
        "contract_sha256": contract["contract_sha256"],
        "rules_hash": contract["rules_hash"],
        "records": records,
        "request_count": len(records),
        "complete_signal_count": sum(row["complete"] for row in records),
        "incomplete_signal_count": sum(not row["complete"] for row in records),
        "market_outcome_rows_frozen": sum(
            len(row["atr_rows"]) + len(row["outcome_rows"]) for row in records
        ),
        "returns_computed": 0,
        "broker_actions": 0,
    }
    value["input_sha256"] = capacity.successor._self_hash(value, "input_sha256")
    return value


def _activation_from_private(
    contract: Mapping[str, Any], inspection: Mapping[str, Any], graph: Mapping[str, Any]
) -> dict[str, Any]:
    if not (
        graph.get("input_sha256") == capacity.successor._self_hash(graph, "input_sha256")
        and graph.get("contract_sha256") == contract["contract_sha256"]
        and graph.get("request_count") == STAGE0_SIGNAL_COUNT
        and graph.get("returns_computed") == 0
    ):
        raise Schedule13dStage0Error("private Stage 0 input graph is invalid")
    value: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "artifact_kind": "stage0-input-activation",
        "variant_id": VARIANT_ID,
        "strategy_version": STRATEGY_VERSION,
        "mechanism_family": capacity.THEME_ID,
        "contract_sha256": contract["contract_sha256"],
        "contract_inspection_sha256": inspection["inspection_sha256"],
        "rules_hash": contract["rules_hash"],
        "partition_sha256": contract["partition_sha256"],
        "input_sha256": graph["input_sha256"],
        "private_input_file_sha256": sha256_file(PRIVATE_INPUT_PATH),
        "stage0_signals": graph["request_count"],
        "complete_signals": graph["complete_signal_count"],
        "incomplete_signals": graph["incomplete_signal_count"],
        "daily_rows_frozen": graph["market_outcome_rows_frozen"],
        "provider_requests": graph["request_count"],
        "provider": "Alpaca Market Data API",
        "feed": "sip",
        "adjustment": "all",
        "return_evaluation_before_input_inspection_permitted": False,
        "development_outcome_access_permitted": False,
        "confirmation_outcome_access_permitted": False,
        "broker_actions_permitted": False,
        "returns_computed": 0,
        "maturity_effect": "NONE",
    }
    value["activation_sha256"] = capacity.successor._self_hash(value, "activation_sha256")
    return value


def build_activation(
    provider: DailyBarProvider | None = None, *, write_private: bool = False
) -> dict[str, Any]:
    contract_file, inspection_file, frozen, inspected = _contract_and_inspection()
    daily._require_published((contract_file, inspection_file))
    if inspected.get("stage0_market_outcome_input_access_permitted") is not True:
        raise Schedule13dStage0Error("Stage 0 outcome input access is not permitted")
    if write_private:
        config = AlpacaConfig.optional_from_env(PROJECT_ROOT / ".env")
        if provider is None and config is None:
            raise Schedule13dStage0Error("Alpaca configuration is unavailable")
        owned = None
        try:
            if provider is None:
                owned = AlpacaHistoricalClient(replace(config, adjustment="all", feed="sip"))
                provider = owned
            graph = _collect_inputs(frozen, provider)
            semantic._write_gzip_json(graph, PRIVATE_INPUT_PATH)
        finally:
            if owned is not None:
                owned.close()
    if not PRIVATE_INPUT_PATH.is_file():
        raise Schedule13dStage0Error("private Stage 0 inputs are missing")
    graph = semantic._read_gzip_object(PRIVATE_INPUT_PATH)
    return _activation_from_private(frozen, inspected, graph)


def activation_path(value: Mapping[str, Any]) -> Path:
    return ACTIVATION_ROOT / f"{VARIANT_ID}-{value['activation_sha256']}.json"


def inspect_inputs(path: Path) -> dict[str, Any]:
    recorded = _read_json(path)
    if recorded.get("activation_sha256") != capacity.successor._self_hash(
        recorded, "activation_sha256"
    ):
        raise Schedule13dStage0Error("Stage 0 activation hash is invalid")
    if recorded != build_activation(write_private=False):
        raise Schedule13dStage0Error("Stage 0 activation does not rebuild")
    graph = semantic._read_gzip_object(PRIVATE_INPUT_PATH)
    contract = _read_json(
        _one(
            "strategy_tournament/v2/schedule13d/stage0/contracts/"
            "schedule-13d-activist-continuation-v1-*.json",
            "Stage 0 contract",
        )
    )
    expected = {row["request_sha256"] for row in contract["stage0_input_requests"]}
    observed = {row["request"]["request_sha256"] for row in graph["records"]}
    if expected != observed or len(observed) != STAGE0_SIGNAL_COUNT:
        raise Schedule13dStage0Error("Stage 0 input denominator differs")
    value: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "inspection_kind": "stage0-input-inspection",
        "variant_id": VARIANT_ID,
        "contract_sha256": recorded["contract_sha256"],
        "rules_hash": recorded["rules_hash"],
        "activation_sha256": recorded["activation_sha256"],
        "activation_file_sha256": sha256_file(path),
        "input_sha256": graph["input_sha256"],
        "private_input_file_sha256": sha256_file(PRIVATE_INPUT_PATH),
        "stage0_signals": graph["request_count"],
        "complete_signals": graph["complete_signal_count"],
        "incomplete_signals": graph["incomplete_signal_count"],
        "daily_rows_inspected": graph["market_outcome_rows_frozen"],
        "complete_denominator_verified": True,
        "stage0_return_evaluation_permitted": True,
        "development_outcome_access_permitted": False,
        "confirmation_outcome_access_permitted": False,
        "broker_actions": 0,
        "returns_computed": 0,
        "maturity_effect": "NONE",
        "valid": True,
    }
    value["inspection_sha256"] = capacity.successor._self_hash(value, "inspection_sha256")
    return value


def _atr(rows: Sequence[Mapping[str, Any]]) -> float:
    if len(rows) != ATR_LENGTH + 1:
        raise Schedule13dStage0Error("ATR input length differs")
    ranges = []
    for index in range(1, len(rows)):
        row = rows[index]
        previous_close = float(rows[index - 1]["c"])
        high = float(row["h"])
        low = float(row["l"])
        ranges.append(max(high - low, abs(high - previous_close), abs(low - previous_close)))
    return sum(ranges) / len(ranges)


def _outcome(
    atr_rows: Sequence[Mapping[str, Any]],
    outcome_rows: Sequence[Mapping[str, Any]],
    cost_bps: int,
) -> dict[str, Any]:
    if len(outcome_rows) != MAXIMUM_HOLDING_SESSIONS:
        raise Schedule13dStage0Error("outcome window length differs")
    raw_entry = float(outcome_rows[0]["o"])
    stop = raw_entry - STOP_ATR_MULTIPLE * _atr(atr_rows)
    if not (math.isfinite(stop) and 0 < stop < raw_entry):
        raise Schedule13dStage0Error("planned stop is invalid")
    exit_price = float(outcome_rows[-1]["c"])
    exit_reason = "force_flat_fifth_close"
    exit_row = outcome_rows[-1]
    for row in outcome_rows:
        if float(row["o"]) <= stop:
            exit_price = float(row["o"])
            exit_reason = "stop_gap"
        elif float(row["l"]) <= stop:
            exit_price = stop
            exit_reason = "stop"
        else:
            continue
        exit_row = row
        break
    cost = cost_bps / 10_000
    entry_fill = raw_entry * (1 + cost)
    exit_fill = exit_price * (1 - cost)
    planned_stop_fill = stop * (1 - cost)
    planned_risk = entry_fill - planned_stop_fill
    if planned_risk <= 0:
        raise Schedule13dStage0Error("cost-adjusted planned risk is invalid")
    return {
        "net_r": (exit_fill - entry_fill) / planned_risk,
        "exit_reason": exit_reason,
        "exit_date": str(exit_row["date"]),
        "stop_executed": exit_reason.startswith("stop"),
    }


def build_result(
    activation: Path, inspection: Path, *, require_published: bool = True
) -> dict[str, Any]:
    frozen_activation = _read_json(activation)
    frozen_inspection = _read_json(inspection)
    if frozen_inspection != inspect_inputs(activation):
        raise Schedule13dStage0Error("Stage 0 input inspection differs")
    if frozen_inspection.get("stage0_return_evaluation_permitted") is not True:
        raise Schedule13dStage0Error("Stage 0 return evaluation is not permitted")
    if require_published:
        daily._require_published((activation, inspection))
    graph = semantic._read_gzip_object(PRIVATE_INPUT_PATH)
    records = []
    missed = []
    for row in graph["records"]:
        request = row["request"]
        if row.get("complete") is not True:
            missed.append(
                {
                    "event_ordinal": request["event_ordinal"],
                    "event_accession": request["event_accession"],
                    "symbol": request["symbol"],
                    "entry_session": request["entry_session"],
                    "missing_sessions": row["missing_sessions"],
                    "duplicate_sessions": row["duplicate_sessions"],
                    "reason": "INCOMPLETE_FROZEN_PROVIDER_INPUT",
                }
            )
            continue
        outcomes = {
            str(cost): _outcome(row["atr_rows"], row["outcome_rows"], cost)
            for cost in (PRIMARY_COST_BPS, *STRESS_COST_BPS)
        }
        primary = outcomes[str(PRIMARY_COST_BPS)]
        records.append(
            {
                "event_ordinal": request["event_ordinal"],
                "event_accession": request["event_accession"],
                "signal_date": request["accepted_at_eastern"],
                "opportunity_date": request["entry_session"],
                "symbol": request["symbol"],
                "exit_date": primary["exit_date"],
                "exit_reason": primary["exit_reason"],
                "stop_executed": primary["stop_executed"],
                "net_r": primary["net_r"],
                "stress_10bps_r": outcomes["10"]["net_r"],
                "stress_20bps_r": outcomes["20"]["net_r"],
            }
        )
    primary_metrics = common._metrics([float(row["net_r"]) for row in records])
    stress = {
        "10": common._metrics([float(row["stress_10bps_r"]) for row in records]),
        "20": common._metrics([float(row["stress_20bps_r"]) for row in records]),
    }
    frozen_contract = _read_json(
        _one(
            "strategy_tournament/v2/schedule13d/stage0/contracts/"
            "schedule-13d-activist-continuation-v1-*.json",
            "Stage 0 contract",
        )
    )
    gate = frozen_contract["stage0_gate"]
    blockers = []
    if len(records) < int(gate["minimum_closed_signals"]):
        blockers.append("closed signals are below the Stage 0 minimum")
    if primary_metrics["expectancy_r"] is None or primary_metrics["expectancy_r"] <= 0:
        blockers.append("primary expectancy is not positive")
    if not primary_metrics["profit_factor_infinite"] and (
        primary_metrics["profit_factor"] is None
        or primary_metrics["profit_factor"] < float(gate["minimum_profit_factor"])
    ):
        blockers.append("primary profit factor is below the Stage 0 minimum")
    if primary_metrics["maximum_drawdown_r"] > float(gate["maximum_drawdown_r"]):
        blockers.append("primary drawdown exceeds the Stage 0 maximum")
    if stress["20"]["total_r"] <= 0:
        blockers.append("20 bps-per-side total R is not positive")
    rule_violations = 0
    if rule_violations > int(gate["maximum_rule_violations"]):
        blockers.append("rule violations exceed the Stage 0 maximum")
    value: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "result_kind": "stage0-falsification",
        "campaign_id": capacity.CAMPAIGN_ID,
        "variant_id": VARIANT_ID,
        "strategy_version": STRATEGY_VERSION,
        "mechanism_family": capacity.THEME_ID,
        "contract_sha256": frozen_activation["contract_sha256"],
        "rules_hash": frozen_activation["rules_hash"],
        "activation_sha256": frozen_activation["activation_sha256"],
        "input_inspection_sha256": frozen_inspection["inspection_sha256"],
        "claim_scope": "FALSIFICATION_ONLY",
        "development_evidence_eligible": False,
        "confirmation_evidence_eligible": False,
        "denominator": {
            "frozen_stage0_signals": graph["request_count"],
            "closed_signals": len(records),
            "missed_entries": len(missed),
            "rule_violations": rule_violations,
        },
        "primary_5bps": primary_metrics,
        "stress": stress,
        "stage0_survived": not blockers,
        "stage0_blockers": blockers,
        "next_action": (
            "freeze and inspect representative development without changing rules"
            if not blockers
            else "retire this exact variant without parameter repair on this corpus"
        ),
        "missed_entry_records": missed,
        "records": records,
        "broker_actions": 0,
        "maturity_effect": "NONE",
    }
    value["result_sha256"] = capacity.successor._self_hash(value, "result_sha256")
    return value


def inspect_result(activation: Path, inspection: Path, result_path: Path) -> dict[str, Any]:
    recorded = _read_json(result_path)
    if recorded.get("result_sha256") != capacity.successor._self_hash(
        recorded, "result_sha256"
    ):
        raise Schedule13dStage0Error("Stage 0 result hash is invalid")
    if recorded != build_result(activation, inspection, require_published=False):
        raise Schedule13dStage0Error("Stage 0 result does not rebuild")
    value: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "inspection_kind": "stage0-result-inspection",
        "variant_id": VARIANT_ID,
        "contract_sha256": recorded["contract_sha256"],
        "rules_hash": recorded["rules_hash"],
        "activation_sha256": recorded["activation_sha256"],
        "input_inspection_sha256": recorded["input_inspection_sha256"],
        "result_sha256": recorded["result_sha256"],
        "result_file_sha256": sha256_file(result_path),
        "denominator": recorded["denominator"],
        "primary_5bps": recorded["primary_5bps"],
        "stress": recorded["stress"],
        "stage0_survived": recorded["stage0_survived"],
        "stage0_blockers": recorded["stage0_blockers"],
        "development_evidence_eligible": False,
        "confirmation_evidence_eligible": False,
        "broker_actions": 0,
        "maturity_effect": "NONE",
        "valid": True,
    }
    value["inspection_sha256"] = capacity.successor._self_hash(value, "inspection_sha256")
    return value


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "command",
        choices=(
            "freeze-contract",
            "inspect-contract",
            "freeze-inputs",
            "inspect-inputs",
            "evaluate",
            "inspect-result",
        ),
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "freeze-contract":
            _published(Path(__file__).resolve())
            _published(INSPECTOR_PATH)
            value = build_contract()
            path = contract_path(value)
            _write_json(value, path)
            result: dict[str, Any] = {
                "contract_sha256": value["contract_sha256"],
                "rules_hash": value["rules_hash"],
                "partition_denominator": value["partition_denominator"],
                "input_request_count": len(value["stage0_input_requests"]),
                "written": str(path.relative_to(PROJECT_ROOT)),
                "outcome_access_permitted": False,
            }
        elif args.command == "inspect-contract":
            source = _one(
                "strategy_tournament/v2/schedule13d/stage0/contracts/"
                "schedule-13d-activist-continuation-v1-*.json",
                "Stage 0 contract",
            )
            value = inspect_contract(source)
            path = INSPECTION_ROOT / f"{VARIANT_ID}-contract-{value['inspection_sha256']}.json"
            _write_json(value, path)
            result = {
                "inspection_sha256": value["inspection_sha256"],
                "written": str(path.relative_to(PROJECT_ROOT)),
                "market_input_access_permitted": True,
                "return_evaluation_permitted": False,
            }
        elif args.command == "freeze-inputs":
            value = build_activation(write_private=True)
            path = activation_path(value)
            _write_json(value, path)
            result = {
                "activation_sha256": value["activation_sha256"],
                "stage0_signals": value["stage0_signals"],
                "complete_signals": value["complete_signals"],
                "incomplete_signals": value["incomplete_signals"],
                "written": str(path.relative_to(PROJECT_ROOT)),
                "return_evaluation_permitted": False,
            }
        elif args.command == "inspect-inputs":
            source = _one(
                "strategy_tournament/v2/schedule13d/stage0/activations/"
                "schedule-13d-activist-continuation-v1-*.json",
                "Stage 0 input activation",
            )
            value = inspect_inputs(source)
            path = INSPECTION_ROOT / f"{VARIANT_ID}-input-{value['inspection_sha256']}.json"
            _write_json(value, path)
            result = {
                "inspection_sha256": value["inspection_sha256"],
                "stage0_signals": value["stage0_signals"],
                "complete_signals": value["complete_signals"],
                "incomplete_signals": value["incomplete_signals"],
                "written": str(path.relative_to(PROJECT_ROOT)),
                "return_evaluation_permitted": True,
            }
        elif args.command == "evaluate":
            activation = _one(
                "strategy_tournament/v2/schedule13d/stage0/activations/"
                "schedule-13d-activist-continuation-v1-*.json",
                "Stage 0 input activation",
            )
            inspection = _one(
                "strategy_tournament/v2/schedule13d/stage0/inspections/"
                "schedule-13d-activist-continuation-v1-input-*.json",
                "Stage 0 input inspection",
            )
            value = build_result(activation, inspection)
            path = RESULT_ROOT / f"2026-07-22-{VARIANT_ID}-stage0-{value['result_sha256']}.json"
            _write_json(value, path)
            result = {
                "result_sha256": value["result_sha256"],
                "stage0_survived": value["stage0_survived"],
                "closed_signals": value["denominator"]["closed_signals"],
                "written": str(path.relative_to(PROJECT_ROOT)),
            }
        else:
            activation = _one(
                "strategy_tournament/v2/schedule13d/stage0/activations/"
                "schedule-13d-activist-continuation-v1-*.json",
                "Stage 0 input activation",
            )
            inspection = _one(
                "strategy_tournament/v2/schedule13d/stage0/inspections/"
                "schedule-13d-activist-continuation-v1-input-*.json",
                "Stage 0 input inspection",
            )
            result_path = _one(
                f"research_results/2026-07-22-{VARIANT_ID}-stage0-*.json",
                "Stage 0 result",
            )
            value = inspect_result(activation, inspection, result_path)
            path = INSPECTION_ROOT / f"{VARIANT_ID}-result-{value['inspection_sha256']}.json"
            _write_json(value, path)
            result = {
                "inspection_sha256": value["inspection_sha256"],
                "stage0_survived": value["stage0_survived"],
                "written": str(path.relative_to(PROJECT_ROOT)),
            }
    except (
        Schedule13dStage0Error,
        HistoricalProviderError,
        OSError,
        ValueError,
        subprocess.CalledProcessError,
    ) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}), file=sys.stderr)
        return 2
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""Freeze an outcome-clean, broad-ETF pullback replication before 2016."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from collections.abc import Callable, Mapping, Sequence
from datetime import date, datetime
from pathlib import Path
from typing import Any

import requests

import dense_strategy_runtime as runtime
import outcome_exposure
import portfolio_maturity
import strategy_discovery
from historical_providers import AlpacaConfig
from historical_store import DEFAULT_ENV_PATH, sha256_file
from learning_data import freeze_dataset_contract
from learning_experiment import DEVELOPMENT_SEARCH_RULE


PROJECT_ROOT = Path(__file__).resolve().parent
CAMPAIGN_ID = portfolio_maturity.V2_CAMPAIGN_ID
FAMILY_ID = runtime.ETF_PULLBACK_FAMILY
MECHANISM_FAMILY = "broad-etf-trend-pullback"
STRATEGY_ID = MECHANISM_FAMILY
SUCCESSOR_ID = "broad-etf-trend-pullback-v3-pre2016-broad-etf-replication"
RESEARCH_GENERATION = "existing_family_successor"
DEFAULT_ROOT = PROJECT_ROOT / "strategy_tournament/v2/continuous"
CALENDAR_ROOT = DEFAULT_ROOT / SUCCESSOR_ID / "calendar"
CALENDAR_PATH = (
    PROJECT_ROOT
    / "historical_batches/etf_pullback_replication/"
    "session-calendar-2008-01-through-2015-12.json"
)
CALENDAR_SOURCE_PATH = CALENDAR_PATH.with_name(
    "session-calendar-2008-01-through-2015-12-source.json"
)
CALENDAR_START = "2008-01-01"
CALENDAR_END = "2015-12-31"
CALENDAR_ENDPOINT = "https://api.alpaca.markets/v2/calendar"
CALENDAR_CONTRACT_KIND = "etf-pullback-replication-calendar-contract"
CALENDAR_CONTRACT_INSPECTION_KIND = (
    "etf-pullback-replication-calendar-contract-inspection"
)
CALENDAR_COLLECTION_KIND = "etf-pullback-replication-calendar-collection"
CALENDAR_DATA_INSPECTION_KIND = (
    "etf-pullback-replication-calendar-data-inspection"
)
MINIMUM_CALENDAR_SESSIONS = 2_000
DEVELOPMENT_WARMUP_SESSIONS = 200
DEVELOPMENT_SESSIONS = 1_200
EMBARGO_SESSIONS = 5
CONFIRMATION_SESSIONS = 500
TOTAL_SESSIONS = (
    DEVELOPMENT_WARMUP_SESSIONS
    + DEVELOPMENT_SESSIONS
    + EMBARGO_SESSIONS
    + CONFIRMATION_SESSIONS
)
SYMBOLS = [
    "DBC",
    "DIA",
    "EEM",
    "EFA",
    "GLD",
    "IEF",
    "IWM",
    "QQQ",
    "SPY",
    "TLT",
    "XLB",
    "XLE",
    "XLF",
    "XLI",
    "XLK",
    "XLP",
    "XLU",
    "XLV",
    "XLY",
]
INSPECTOR_PATH = PROJECT_ROOT / "etf_pullback_replication_inspection.py"
V2_SEARCH = (
    PROJECT_ROOT
    / "strategy_tournament/v2/discovery/liquid-etf-trend-pullback-cost-floor/"
    "search/liquid-etf-trend-pullback-cost-floor-search-"
    "b9003c468f6d21316482ae2e5d5527cb538636bddb41efabdfdfaf409aa78fb9.json"
)
V2_RESULT = (
    PROJECT_ROOT
    / "strategy_tournament/v2/discovery/liquid-etf-trend-pullback-cost-floor/"
    "development/liquid-etf-trend-pullback-cost-floor-development-"
    "b68f6c1b16339bd176b500f765b64e9646e99f4fb9eb3aff20fabff89b74f119.json"
)
V2_INSPECTION = (
    PROJECT_ROOT
    / "strategy_tournament/v2/discovery/liquid-etf-trend-pullback-cost-floor/"
    "development-inspection/liquid-etf-trend-pullback-cost-floor-development-"
    "inspection-650511bc77dc31fa3a9aeeab0029f9df7d6ffb99dfd165fbeb36b9908987665b.json"
)
ROLLING_INSPECTION = (
    PROJECT_ROOT
    / "strategy_tournament/v2/discovery/liquid-etf-trend-pullback-cost-floor/"
    "development-inspection/liquid-etf-trend-pullback-cost-floor-development-"
    "inspection-f460af568a398710b93035238eed03a61c35923af79f57c18441b07227af7174.json"
)


class EtfPullbackReplicationError(ValueError):
    """The replication evidence or frozen implementation drifted."""


def _canonical(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode()


def _repo_path(path: Path) -> str:
    try:
        return path.resolve().relative_to(PROJECT_ROOT).as_posix()
    except ValueError as exc:
        raise EtfPullbackReplicationError(
            f"path escaped repository: {path}"
        ) from exc


def _timestamp(value: str, field: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise EtfPullbackReplicationError(f"{field} is invalid") from exc
    if parsed.tzinfo is None:
        raise EtfPullbackReplicationError(f"{field} needs a timezone")
    if parsed.date() > date.today():
        raise EtfPullbackReplicationError(f"{field} cannot be future-dated")
    return parsed


def _write_json(path: Path, value: Any) -> None:
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


def normalize_calendar_rows(value: Any) -> list[dict[str, str]]:
    if not isinstance(value, list) or not value:
        raise EtfPullbackReplicationError("calendar response is empty")
    rows: list[dict[str, str]] = []
    for item in value:
        if not isinstance(item, Mapping):
            raise EtfPullbackReplicationError("calendar row is malformed")
        try:
            day = date.fromisoformat(str(item["date"]))
            opened = datetime.strptime(
                str(item.get("open", item.get("open_et"))), "%H:%M"
            ).time()
            closed = datetime.strptime(
                str(item.get("close", item.get("close_et"))), "%H:%M"
            ).time()
        except (KeyError, TypeError, ValueError) as exc:
            raise EtfPullbackReplicationError(
                "calendar row is malformed"
            ) from exc
        if day.weekday() >= 5 or opened >= closed:
            raise EtfPullbackReplicationError("calendar session is invalid")
        rows.append(
            {
                "date": day.isoformat(),
                "open_et": opened.isoformat(timespec="minutes"),
                "close_et": closed.isoformat(timespec="minutes"),
            }
        )
    dates = [row["date"] for row in rows]
    if (
        dates != sorted(dates)
        or len(dates) != len(set(dates))
        or dates[0] < CALENDAR_START
        or dates[-1] > CALENDAR_END
        or len(dates) < MINIMUM_CALENDAR_SESSIONS
    ):
        raise EtfPullbackReplicationError("calendar coverage is incomplete")
    return rows


def _implementation_hashes() -> dict[str, str]:
    paths = (
        Path(__file__).resolve(),
        INSPECTOR_PATH,
        PROJECT_ROOT / "dense_data_collection.py",
        PROJECT_ROOT / "dense_data_collection_inspection.py",
        PROJECT_ROOT / "dense_strategy_plugin.py",
        PROJECT_ROOT / "dense_strategy_runtime.py",
    )
    if any(not path.is_file() for path in paths):
        raise EtfPullbackReplicationError(
            "replication implementation is incomplete"
        )
    return {_repo_path(path): sha256_file(path) for path in paths}


def _predecessors(*, enforce_commit: bool) -> dict[str, dict[str, Any]]:
    paths = (V2_SEARCH, V2_RESULT, V2_INSPECTION, ROLLING_INSPECTION)
    if enforce_commit:
        for path in paths:
            strategy_discovery.require_committed(path)
    search = strategy_discovery.load_artifact(
        V2_SEARCH, expected_kind="frozen-development-search"
    )
    result = strategy_discovery.load_artifact(
        V2_RESULT, expected_kind="development-search-result"
    )
    inspection = strategy_discovery.load_artifact(
        V2_INSPECTION, expected_kind="development-search-inspection"
    )
    rolling = strategy_discovery.load_artifact(
        ROLLING_INSPECTION, expected_kind="development-search-inspection"
    )
    if not (
        search["family_contract"].get("successor_id")
        == "broad-etf-trend-pullback-v2-cost-floor"
        and result.get("search_sha256") == search["artifact_sha256"]
        and inspection.get("result_sha256") == result["artifact_sha256"]
        and inspection.get("state") == "REJECTED"
        and rolling.get("state") == "REJECTED"
        and search["family_contract"].get("family_id") == FAMILY_ID
    ):
        raise EtfPullbackReplicationError(
            "terminal predecessor evidence graph drifted"
        )
    return {
        "search": search,
        "result": result,
        "inspection": inspection,
        "rolling_inspection": rolling,
    }


def freeze_calendar_contract(
    *, created_at: str, enforce_commit: bool = True
) -> tuple[Path, dict[str, Any]]:
    _timestamp(created_at, "created_at")
    _predecessors(enforce_commit=enforce_commit)
    if enforce_commit:
        strategy_discovery.require_committed(Path(__file__).resolve())
        strategy_discovery.require_committed(INSPECTOR_PATH)
    if CALENDAR_PATH.exists() or CALENDAR_SOURCE_PATH.exists():
        raise EtfPullbackReplicationError(
            "calendar output exists before contract freeze"
        )
    payload = {
        "schema_version": 1,
        "artifact_kind": CALENDAR_CONTRACT_KIND,
        "campaign_id": CAMPAIGN_ID,
        "state": "CALENDAR_CONTRACT_FROZEN",
        "successor_id": SUCCESSOR_ID,
        "created_at": created_at,
        "provider": "Alpaca Market Calendar API",
        "endpoint": CALENDAR_ENDPOINT,
        "query": {"start": CALENDAR_START, "end": CALENDAR_END},
        "minimum_sessions": MINIMUM_CALENDAR_SESSIONS,
        "calendar_path": _repo_path(CALENDAR_PATH),
        "source_path": _repo_path(CALENDAR_SOURCE_PATH),
        "implementation_hashes": _implementation_hashes(),
        "date_substitutions_allowed": False,
        "provider_requests": 0,
        "market_prices_accessed": False,
        "target_outcomes_accessed": False,
        "broker_actions": 0,
    }
    return strategy_discovery._write_artifact(
        payload,
        CALENDAR_ROOT / "contract",
        "etf-pullback-replication-calendar-contract",
    )


def _contract_inspection(
    contract: Mapping[str, Any], *, enforce_commit: bool
) -> tuple[Path, dict[str, Any]]:
    matches: list[tuple[Path, dict[str, Any]]] = []
    for path in sorted((CALENDAR_ROOT / "contract-inspection").glob("*.json")):
        value = strategy_discovery.load_artifact(
            path, expected_kind=CALENDAR_CONTRACT_INSPECTION_KIND
        )
        if value.get("contract_sha256") == contract["artifact_sha256"]:
            matches.append((path, value))
    if len(matches) != 1:
        raise EtfPullbackReplicationError(
            "exact calendar contract inspection is missing"
        )
    path, value = matches[0]
    if enforce_commit:
        strategy_discovery.require_committed(path)
    if value.get("state") != "CALENDAR_CONTRACT_INSPECTED_READY":
        raise EtfPullbackReplicationError(
            "calendar contract inspection is not ready"
        )
    return path, value


def collect_calendar(
    contract_path: Path,
    *,
    collected_at: str,
    getter: Callable[..., Any] = requests.get,
    enforce_commit: bool = True,
) -> tuple[Path, dict[str, Any]]:
    collected = _timestamp(collected_at, "collected_at")
    if enforce_commit:
        strategy_discovery.require_committed(contract_path)
    contract = strategy_discovery.load_artifact(
        contract_path, expected_kind=CALENDAR_CONTRACT_KIND
    )
    if (
        collected <= _timestamp(str(contract["created_at"]), "created_at")
        or contract.get("implementation_hashes") != _implementation_hashes()
    ):
        raise EtfPullbackReplicationError(
            "calendar collection chronology or implementation drifted"
        )
    inspection_path, inspection = _contract_inspection(
        contract, enforce_commit=enforce_commit
    )
    config = AlpacaConfig.optional_from_env(DEFAULT_ENV_PATH)
    if config is None:
        raise EtfPullbackReplicationError("Alpaca credentials are unavailable")
    started = datetime.now().timestamp()
    response = getter(
        CALENDAR_ENDPOINT,
        params=dict(contract["query"]),
        headers={
            "APCA-API-KEY-ID": config.api_key,
            "APCA-API-SECRET-KEY": config.api_secret,
        },
        timeout=config.timeout_seconds,
    )
    elapsed = datetime.now().timestamp() - started
    if response.status_code != 200:
        raise EtfPullbackReplicationError(
            f"Alpaca calendar HTTP {response.status_code}"
        )
    rows = normalize_calendar_rows(response.json())
    _write_json(CALENDAR_PATH, rows)
    source = {
        "schema_version": 1,
        "campaign_id": CAMPAIGN_ID,
        "successor_id": SUCCESSOR_ID,
        "contract_sha256": contract["artifact_sha256"],
        "contract_inspection_sha256": inspection["artifact_sha256"],
        "provider": contract["provider"],
        "endpoint": contract["endpoint"],
        "query": contract["query"],
        "collected_at": collected_at,
        "sessions": len(rows),
        "first_session": rows[0]["date"],
        "last_session": rows[-1]["date"],
        "calendar_sha256": sha256_file(CALENDAR_PATH),
        "provider_requests": 1,
        "request_seconds": elapsed,
        "market_prices_accessed": False,
        "target_outcomes_accessed": False,
        "broker_actions": 0,
    }
    _write_json(CALENDAR_SOURCE_PATH, source)
    payload = {
        "schema_version": 1,
        "artifact_kind": CALENDAR_COLLECTION_KIND,
        "campaign_id": CAMPAIGN_ID,
        "state": "CALENDAR_COLLECTED_UNINSPECTED",
        "successor_id": SUCCESSOR_ID,
        "contract_path": _repo_path(contract_path),
        "contract_sha256": contract["artifact_sha256"],
        "contract_inspection_path": _repo_path(inspection_path),
        "contract_inspection_sha256": inspection["artifact_sha256"],
        "calendar_path": _repo_path(CALENDAR_PATH),
        "calendar_sha256": source["calendar_sha256"],
        "source_path": _repo_path(CALENDAR_SOURCE_PATH),
        "sessions": len(rows),
        "provider_requests": 1,
        "request_seconds": elapsed,
        "market_prices_accessed": False,
        "target_outcomes_accessed": False,
        "broker_actions": 0,
        "collected_at": collected_at,
    }
    return strategy_discovery._write_artifact(
        payload,
        CALENDAR_ROOT / "collection",
        "etf-pullback-replication-calendar-collection",
    )


def _calendar_dates() -> list[str]:
    rows = normalize_calendar_rows(
        json.loads(CALENDAR_PATH.read_text(encoding="utf-8"))
    )
    return [
        row["date"]
        for row in rows
        if row["open_et"] == "09:30" and row["close_et"] == "16:00"
    ]


def _calendar_data_inspection(
    *, enforce_commit: bool
) -> tuple[Path, dict[str, Any]]:
    matches: list[tuple[Path, dict[str, Any]]] = []
    expected_hash = sha256_file(CALENDAR_PATH)
    for path in sorted((CALENDAR_ROOT / "data-inspection").glob("*.json")):
        value = strategy_discovery.load_artifact(
            path, expected_kind=CALENDAR_DATA_INSPECTION_KIND
        )
        if value.get("calendar_sha256") == expected_hash:
            matches.append((path, value))
    if len(matches) != 1:
        raise EtfPullbackReplicationError(
            "exact calendar data inspection is missing"
        )
    path, value = matches[0]
    if enforce_commit:
        strategy_discovery.require_committed(path)
        strategy_discovery.require_committed(CALENDAR_PATH)
        strategy_discovery.require_committed(CALENDAR_SOURCE_PATH)
    if value.get("state") != "CALENDAR_INSPECTED_READY":
        raise EtfPullbackReplicationError(
            "calendar data inspection is not ready"
        )
    return path, value


def _scope(dates: Sequence[str]) -> dict[str, Any]:
    return {"dates": list(dates), "symbols": list(SYMBOLS)}


def freeze_successor_contract(
    *, created_at: str, enforce_commit: bool = True
) -> tuple[Path, dict[str, Any], Path]:
    _timestamp(created_at, "created_at")
    predecessors = _predecessors(enforce_commit=enforce_commit)
    inspection_path, inspection = _calendar_data_inspection(
        enforce_commit=enforce_commit
    )
    if inspection.get("calendar_sha256") != sha256_file(CALENDAR_PATH):
        raise EtfPullbackReplicationError("calendar inspection drifted")
    dates = _calendar_dates()
    if len(dates) < TOTAL_SESSIONS:
        raise EtfPullbackReplicationError(
            f"replication needs {TOTAL_SESSIONS} full sessions"
        )
    selected = dates[-TOTAL_SESSIONS:]
    warmup = selected[:DEVELOPMENT_WARMUP_SESSIONS]
    development = selected[
        DEVELOPMENT_WARMUP_SESSIONS:
        DEVELOPMENT_WARMUP_SESSIONS + DEVELOPMENT_SESSIONS
    ]
    embargo_start = DEVELOPMENT_WARMUP_SESSIONS + DEVELOPMENT_SESSIONS
    embargo = selected[embargo_start:embargo_start + EMBARGO_SESSIONS]
    confirmation = selected[-CONFIRMATION_SESSIONS:]
    confirmation_warmup = dates[
        dates.index(confirmation[0]) - DEVELOPMENT_WARMUP_SESSIONS:
        dates.index(confirmation[0])
    ]
    records = outcome_exposure.read_index()
    development_scope = _scope(development)
    confirmation_scope = _scope(confirmation)
    outcome_exposure.assert_untouched(development_scope, records)
    outcome_exposure.assert_untouched(confirmation_scope, records)
    outcome_exposure.assert_disjoint([development_scope, confirmation_scope])
    evidence_paths = [
        _repo_path(CALENDAR_PATH),
        _repo_path(CALENDAR_SOURCE_PATH),
        _repo_path(inspection_path),
        _repo_path(V2_SEARCH),
        _repo_path(V2_RESULT),
        _repo_path(V2_INSPECTION),
        _repo_path(ROLLING_INSPECTION),
        "strategy_tournament/v2/OUTCOME_EXPOSURE_INDEX.jsonl",
    ]
    capacity_path, _capacity = freeze_dataset_contract(
        {
            "schema_version": 1,
            "dataset_id": f"dataset-{SUCCESSOR_ID}-capacity",
            "registered_at": created_at,
            "requested_dates": development,
            "dataset_payload": {
                "lane": "development",
                "claim_scope": "DEVELOPMENT_ONLY",
                "evidence_paths": evidence_paths,
                "inspected": True,
                "point_in_time_evidence": True,
                "dense_capacity": {
                    "family_id": FAMILY_ID,
                    "mechanism_family": MECHANISM_FAMILY,
                    "formal_capacity": len(development) * len(SYMBOLS),
                    "capacity_unit": "frozen instrument-session observations",
                    "development_sessions": len(development),
                    "embargo_sessions": len(embargo),
                    "confirmation_sessions": len(confirmation),
                    "calendar_sha256": sha256_file(CALENDAR_PATH),
                    "provider_requests": 0,
                    "market_prices_accessed": False,
                    "outcomes_accessed": False,
                },
            },
        },
        DEFAULT_ROOT / SUCCESSOR_ID / "capacity",
    )
    contract: dict[str, Any] = {
        "schema_version": 1,
        "campaign_id": CAMPAIGN_ID,
        "experiment_id": f"experiment-{SUCCESSOR_ID}",
        "family_id": FAMILY_ID,
        "mechanism_family": MECHANISM_FAMILY,
        "strategy_id": STRATEGY_ID,
        "parent_experiment_id": (
            "experiment-broad-etf-trend-pullback-v2-cost-floor"
        ),
        "created_at": created_at,
        "status": "INVENTED",
        "research_generation": RESEARCH_GENERATION,
        "successor_id": SUCCESSOR_ID,
        "existing_successor_validator": {
            "module": "etf_pullback_replication",
            "function": "validate_existing_successor_contract",
        },
        "new_mechanism_family_slot_consumed": False,
        "prior_family_attempt_count": 3,
        "predecessor": {
            "search_path": _repo_path(V2_SEARCH),
            "search_sha256": predecessors["search"]["artifact_sha256"],
            "result_path": _repo_path(V2_RESULT),
            "result_sha256": predecessors["result"]["artifact_sha256"],
            "inspection_path": _repo_path(V2_INSPECTION),
            "inspection_sha256": predecessors["inspection"][
                "artifact_sha256"
            ],
            "rolling_inspection_path": _repo_path(ROLLING_INSPECTION),
            "rolling_inspection_sha256": predecessors[
                "rolling_inspection"
            ]["artifact_sha256"],
            "promotion_evidence_reused": False,
        },
        "mechanism": (
            "Buy the deepest cost-clearing three-session pullback among broad "
            "liquid ETFs that remain above a completed long-term trend."
        ),
        "expected_holding_behavior": (
            "Long only, next-session-open entry, and flat within five sessions."
        ),
        "dataset_lane": "development",
        "universe_requirements": {
            "symbols": list(SYMBOLS),
            "complete_frozen_daily_history": True,
            "begin_after_frozen_lookback": True,
        },
        "entry_rule": (
            "After completed SMA, RSI2, and three-session-decline qualification, "
            "rank the frozen ETF universe by lowest RSI2, deepest decline, then "
            "symbol, and enter at the next observable session open."
        ),
        "stop_rule": (
            "Use the exact one or one-and-a-half completed ATR14 stop below "
            "entry; invalid or missing stops are missed trades."
        ),
        "exit_rule": (
            "Resolve stop first on daily ambiguity and otherwise exit at the "
            "completed close after three or five sessions."
        ),
        "ranking_rule": "Lowest RSI2, deepest three-session decline, then symbol.",
        "selection_rule": (
            "At most one new family entry per day under all portfolio risk, "
            "notional, daily-entry, and capital-contention caps."
        ),
        "parameter_grid": {
            "trend_sma": [100, 200],
            "rsi2_maximum": [5, 10],
            "three_session_decline_fraction": [0.02, 0.03],
            "stop_atr14": [1.0, 1.5],
            "maximum_hold_sessions": [3, 5],
        },
        "selection_mode": "development_search",
        "winner_selection": DEVELOPMENT_SEARCH_RULE,
        "primary_outcome": (
            "Selection-adjusted chronological account log growth after costs."
        ),
        "execution_assumptions": {
            "next_observable_fill": True,
            "ambiguity": "stop_first",
            "maximum_hold_sessions": 5,
            "missing_data": "missed_trade_no_substitute",
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
            "Prior pullback outcomes motivate replication only and cannot count toward promotion.",
            "The pre-2016 development and confirmation targets are globally outcome-clean at freeze.",
            "Warmup is feature-only and cannot count as target evidence.",
        ],
        "production_compatibility_risks": [
            "Fresh quote, spread, depth, halt, tradability, timestamp, protection, and reconciliation remain mandatory."
        ],
        "material_difference_rationale": (
            "This existing-family replication preserves the exact authorized "
            "32-rule grid while expanding to the prospectively authorized 19 "
            "liquid ETFs and wholly disjoint pre-2016 evidence. It does not "
            "repair or open either prior confirmation reserve."
        ),
        "development_dates": development,
        "development_warmup_dates": warmup,
        "confirmation_warmup_dates": confirmation_warmup,
        "embargo_dates": embargo,
        "confirmation_dates": confirmation,
        "development_scope": development_scope,
        "confirmation_scope": confirmation_scope,
        "outcome_exposure_index_sha256": outcome_exposure.audit()[
            "index_sha256"
        ],
        "universe": {"symbols": list(SYMBOLS), "point_in_time": True},
        "historical_data_contract": {
            "daily_provider": "massive",
            "daily_endpoint": "/v2/aggs/ticker/{symbol}/range/1/day/{start}/{end}",
            "daily_adjusted": False,
            "split_provider": "massive",
            "provider_substitutions_allowed": False,
        },
        "calendar_path": _repo_path(CALENDAR_PATH),
        "costs_bps_per_side": [5, 10, 20],
        "partitions": {
            "rolling_origin": True,
            "confirmation_untouched": True,
            "predecessor_corpora_disjoint": True,
        },
        "falsifiers": [
            "nonpositive stressed log growth",
            "unstable parameter neighbors",
            "selection-aware statistical rejection",
            "incomplete execution or trial accounting",
            "insufficient frozen confirmation power capacity",
        ],
        "implementation_files": [
            "etf_pullback_replication.py",
            "etf_pullback_replication_inspection.py",
            "dense_data_collection.py",
            "dense_data_collection_inspection.py",
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
    contract = strategy_discovery._validate_family_contract(contract)
    validate_existing_successor_contract(
        contract, enforce_commit=enforce_commit
    )
    digest = hashlib.sha256(_canonical(contract)).hexdigest()
    path = (
        DEFAULT_ROOT
        / SUCCESSOR_ID
        / "family-contract"
        / f"contract-{digest}.json"
    )
    _write_json(path, contract)
    return path, contract, capacity_path


def validate_existing_successor_contract(
    contract: Mapping[str, Any], *, enforce_commit: bool = True
) -> None:
    if not (
        contract.get("campaign_id") == CAMPAIGN_ID
        and contract.get("family_id") == FAMILY_ID
        and contract.get("mechanism_family") == MECHANISM_FAMILY
        and contract.get("successor_id") == SUCCESSOR_ID
        and contract.get("research_generation") == RESEARCH_GENERATION
        and contract.get("new_mechanism_family_slot_consumed") is False
        and contract.get("prior_family_attempt_count") == 3
        and contract.get("selection_mode") == "development_search"
        and len(contract.get("trial_family", [])) == 32
        and contract.get("universe", {}).get("symbols") == SYMBOLS
        and contract.get("existing_successor_validator")
        == {
            "module": "etf_pullback_replication",
            "function": "validate_existing_successor_contract",
        }
        and contract.get("historical_data_contract")
        == {
            "daily_provider": "massive",
            "daily_endpoint": "/v2/aggs/ticker/{symbol}/range/1/day/{start}/{end}",
            "daily_adjusted": False,
            "split_provider": "massive",
            "provider_substitutions_allowed": False,
        }
        and contract.get("calendar_path") == _repo_path(CALENDAR_PATH)
        and contract.get("outcome_exposure_index_sha256")
        == outcome_exposure.audit()["index_sha256"]
    ):
        raise EtfPullbackReplicationError(
            "existing-family replication contract drifted"
        )
    _predecessors(enforce_commit=enforce_commit)
    outcome_exposure.assert_untouched(
        contract["development_scope"], outcome_exposure.read_index()
    )
    outcome_exposure.assert_untouched(
        contract["confirmation_scope"], outcome_exposure.read_index()
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    freeze_calendar = sub.add_parser("freeze-calendar")
    freeze_calendar.add_argument("--created-at", required=True)
    collect = sub.add_parser("collect-calendar")
    collect.add_argument("contract", type=Path)
    collect.add_argument("--collected-at", required=True)
    freeze = sub.add_parser("freeze")
    freeze.add_argument("--created-at", required=True)
    return parser


def main() -> int:
    args = _parser().parse_args()
    try:
        if args.command == "freeze-calendar":
            path, value = freeze_calendar_contract(
                created_at=args.created_at
            )
        elif args.command == "collect-calendar":
            path, value = collect_calendar(
                args.contract, collected_at=args.collected_at
            )
        else:
            path, value, capacity = freeze_successor_contract(
                created_at=args.created_at
            )
            print(
                json.dumps(
                    {
                        "path": _repo_path(path),
                        "capacity_manifest": _repo_path(capacity),
                        "state": value["status"],
                        "trial_count": len(value["trial_family"]),
                        "confirmation_access_permitted": False,
                    },
                    indent=2,
                    sort_keys=True,
                )
            )
            return 0
        print(
            json.dumps(
                {
                    "path": _repo_path(path),
                    "state": value["state"],
                    "sha256": value["artifact_sha256"],
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    except (
        EtfPullbackReplicationError,
        OSError,
        ValueError,
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

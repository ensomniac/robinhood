"""Run continuous historical discovery under rolling research slots.

The weekly campaign ceiling governs *new mechanism families*.  This controller
opens one prospectively frozen successor version of the already-evaluated
``broad-etf-trend-pullback`` mechanism.  It never reuses the predecessor's
2023-2025 outcome corpus for development or confirmation.
"""

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
import next_week_discovery_batch
import outcome_exposure
import portfolio_maturity
import strategy_discovery
from historical_providers import AlpacaConfig
from historical_store import DEFAULT_ENV_PATH
from learning_data import freeze_dataset_contract
from learning_experiment import DEVELOPMENT_SEARCH_RULE


PROJECT_ROOT = Path(__file__).resolve().parent
CAMPAIGN_ID = portfolio_maturity.V2_CAMPAIGN_ID
FAMILY_ID = runtime.ETF_PULLBACK_FAMILY
MECHANISM_FAMILY = "broad-etf-trend-pullback"
STRATEGY_ID = MECHANISM_FAMILY
SUCCESSOR_ID = "broad-etf-trend-pullback-v2-cost-floor"
RESEARCH_GENERATION = "existing_family_successor"
PREDECESSOR_VARIANT_ID = "broad-etf-trend-pullback-v1"
PREDECESSOR_RESULT = (
    PROJECT_ROOT
    / "research_results/2026-07-21-broad-etf-trend-pullback-stage0-"
    "2aa31af9eb16c6a268b19df57b5bfc3b9d6950b69fa00cddcbfcb12386a41209.json"
)
PREDECESSOR_INSPECTION = (
    PROJECT_ROOT
    / "strategy_tournament/second_wave/inspections/"
    "broad-etf-trend-pullback-v1-result-"
    "384c8372624f14ecc6b92c2a4406830db7061a27474e643f11777800675db8e3.json"
)
DEFAULT_ROOT = PROJECT_ROOT / "strategy_tournament/v2/continuous"
CALENDAR_ROOT = DEFAULT_ROOT / SUCCESSOR_ID / "calendar"
CALENDAR_PATH = (
    PROJECT_ROOT
    / "historical_batches/continuous_v2/"
    "session-calendar-2014-01-through-2022-12.json"
)
CALENDAR_SOURCE_PATH = (
    PROJECT_ROOT
    / "historical_batches/continuous_v2/"
    "session-calendar-2014-01-through-2022-12-source.json"
)
CALENDAR_START = "2014-01-01"
CALENDAR_END = "2022-12-31"
MINIMUM_CALENDAR_SESSIONS = 2_200
CALENDAR_ENDPOINT = "https://api.alpaca.markets/v2/calendar"
CALENDAR_CONTRACT_KIND = "continuous-successor-calendar-contract"
CALENDAR_CONTRACT_INSPECTION_KIND = (
    "continuous-successor-calendar-contract-inspection"
)
CALENDAR_COLLECTION_KIND = "continuous-successor-calendar-collection"
CALENDAR_DATA_INSPECTION_KIND = "continuous-successor-calendar-data-inspection"
DEVELOPMENT_WARMUP_SESSIONS = 200
DEVELOPMENT_SESSIONS = 1_000
EMBARGO_SESSIONS = 5
CONFIRMATION_SESSIONS = 500
TOTAL_SESSIONS = (
    DEVELOPMENT_WARMUP_SESSIONS
    + DEVELOPMENT_SESSIONS
    + EMBARGO_SESSIONS
    + CONFIRMATION_SESSIONS
)
SYMBOLS = ["SPY", "QQQ", "IWM", "DIA"]
INSPECTOR_PATH = PROJECT_ROOT / "continuous_strategy_discovery_inspection.py"


class ContinuousDiscoveryError(RuntimeError):
    """The successor lane, evidence boundary, or collection chain is invalid."""


def _canonical(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode()


def _self_hash(value: Mapping[str, Any], field: str) -> str:
    return hashlib.sha256(
        _canonical({key: item for key, item in value.items() if key != field})
    ).hexdigest()


def _read(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ContinuousDiscoveryError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise ContinuousDiscoveryError(f"{path} must contain an object")
    return value


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


def _repo_path(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(PROJECT_ROOT.resolve()))
    except ValueError as exc:
        raise ContinuousDiscoveryError(f"path is outside repository: {path}") from exc


def _timestamp(value: str, field: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ContinuousDiscoveryError(f"{field} is invalid") from exc
    if parsed.tzinfo is None:
        raise ContinuousDiscoveryError(f"{field} must include a timezone")
    return parsed


def normalize_calendar_rows(payload: Any) -> list[dict[str, str]]:
    """Normalize the exact historical calendar query without opening prices."""

    if not isinstance(payload, list) or not payload:
        raise ContinuousDiscoveryError("Alpaca calendar response is empty")
    rows: list[dict[str, str]] = []
    for item in payload:
        if not isinstance(item, Mapping):
            raise ContinuousDiscoveryError("Alpaca calendar row is malformed")
        try:
            session_date = date.fromisoformat(str(item["date"]))
            opened = str(item.get("open", item.get("open_et")))
            closed = str(item.get("close", item.get("close_et")))
            open_time = datetime.strptime(opened, "%H:%M").time()
            close_time = datetime.strptime(closed, "%H:%M").time()
        except (KeyError, TypeError, ValueError) as exc:
            raise ContinuousDiscoveryError(
                "Alpaca calendar row is malformed"
            ) from exc
        if session_date.weekday() >= 5 or open_time >= close_time:
            raise ContinuousDiscoveryError("Alpaca calendar session is invalid")
        rows.append(
            {
                "date": session_date.isoformat(),
                "open_et": open_time.isoformat(timespec="minutes"),
                "close_et": close_time.isoformat(timespec="minutes"),
            }
        )
    dates = [item["date"] for item in rows]
    if dates != sorted(dates) or len(dates) != len(set(dates)):
        raise ContinuousDiscoveryError(
            "calendar dates are not unique and chronological"
        )
    if (
        dates[0] < CALENDAR_START
        or dates[-1] > CALENDAR_END
        or len(rows) < MINIMUM_CALENDAR_SESSIONS
    ):
        raise ContinuousDiscoveryError("successor calendar coverage is incomplete")
    return rows


def _implementation_hashes() -> dict[str, str]:
    paths = (Path(__file__).resolve(), INSPECTOR_PATH)
    if any(not path.is_file() for path in paths):
        raise ContinuousDiscoveryError("continuous discovery implementation is incomplete")
    return {_repo_path(path): strategy_discovery._file_hash(path) for path in paths}


def _predecessor(*, enforce_commit: bool) -> tuple[dict[str, Any], dict[str, Any]]:
    if enforce_commit:
        strategy_discovery.require_committed(PREDECESSOR_RESULT)
        strategy_discovery.require_committed(PREDECESSOR_INSPECTION)
    result = _read(PREDECESSOR_RESULT)
    inspection = _read(PREDECESSOR_INSPECTION)
    if not (
        result.get("variant_id") == PREDECESSOR_VARIANT_ID
        and result.get("mechanism_family") == MECHANISM_FAMILY
        and result.get("stage0_survived") is False
        and result.get("stage0_blockers")
        == ["20 bps-per-side total R is not positive"]
        and result.get("development_evidence_eligible") is False
        and result.get("confirmation_evidence_eligible") is False
        and inspection.get("inspection_sha256")
        == _self_hash(inspection, "inspection_sha256")
        and inspection.get("result_sha256") == result["result_sha256"]
        and inspection.get("result_file_sha256")
        == strategy_discovery._file_hash(PREDECESSOR_RESULT)
        and inspection.get("stage0_survived") is False
        and inspection.get("valid") is True
    ):
        raise ContinuousDiscoveryError("retired predecessor binding is invalid")
    return result, inspection


def freeze_calendar_contract(
    *,
    created_at: str,
    root: Path = CALENDAR_ROOT,
    calendar_path: Path = CALENDAR_PATH,
    source_path: Path = CALENDAR_SOURCE_PATH,
    enforce_commit: bool = True,
) -> tuple[Path, dict[str, Any]]:
    """Freeze a zero-price public-session query for the successor evidence era."""

    _timestamp(created_at, "created_at")
    _predecessor(enforce_commit=enforce_commit)
    if enforce_commit:
        strategy_discovery.require_committed(Path(__file__).resolve())
        strategy_discovery.require_committed(INSPECTOR_PATH)
    if calendar_path.exists() or source_path.exists():
        raise ContinuousDiscoveryError("calendar output exists before contract freeze")
    payload = {
        "schema_version": 1,
        "artifact_kind": CALENDAR_CONTRACT_KIND,
        "campaign_id": CAMPAIGN_ID,
        "state": "CALENDAR_CONTRACT_FROZEN",
        "research_generation": RESEARCH_GENERATION,
        "successor_id": SUCCESSOR_ID,
        "mechanism_family": MECHANISM_FAMILY,
        "predecessor_variant_id": PREDECESSOR_VARIANT_ID,
        "predecessor_result_path": _repo_path(PREDECESSOR_RESULT),
        "predecessor_result_sha256": strategy_discovery._file_hash(
            PREDECESSOR_RESULT
        ),
        "predecessor_inspection_path": _repo_path(PREDECESSOR_INSPECTION),
        "predecessor_inspection_sha256": strategy_discovery._file_hash(
            PREDECESSOR_INSPECTION
        ),
        "created_at": created_at,
        "provider": "Alpaca Market Calendar API",
        "endpoint": CALENDAR_ENDPOINT,
        "query": {"start": CALENDAR_START, "end": CALENDAR_END},
        "minimum_sessions": MINIMUM_CALENDAR_SESSIONS,
        "calendar_path": _repo_path(calendar_path),
        "source_path": _repo_path(source_path),
        "implementation_hashes": _implementation_hashes(),
        "date_substitutions_allowed": False,
        "new_mechanism_family_slot_consumed": False,
        "provider_requests": 0,
        "market_prices_accessed": False,
        "target_outcomes_accessed": False,
        "broker_actions": 0,
    }
    return strategy_discovery._write_artifact(
        payload, root / "contract", "continuous-successor-calendar-contract"
    )


def _inspection_for_contract(
    contract: Mapping[str, Any],
    *,
    root: Path,
    enforce_commit: bool,
) -> tuple[Path, dict[str, Any]]:
    matches: list[tuple[Path, dict[str, Any]]] = []
    for path in sorted((root / "contract-inspection").glob("*.json")):
        try:
            value = strategy_discovery.load_artifact(
                path, expected_kind=CALENDAR_CONTRACT_INSPECTION_KIND
            )
        except strategy_discovery.StrategyDiscoveryError:
            continue
        if value.get("contract_sha256") == contract.get("artifact_sha256"):
            matches.append((path, value))
    if len(matches) != 1:
        raise ContinuousDiscoveryError(
            "expected one inspection for the exact successor calendar contract"
        )
    path, inspection = matches[0]
    if enforce_commit:
        strategy_discovery.require_committed(path)
    if inspection.get("state") != "CALENDAR_CONTRACT_INSPECTED_READY":
        raise ContinuousDiscoveryError("calendar contract inspection is not ready")
    return path, inspection


def collect_calendar(
    contract_path: Path,
    *,
    collected_at: str,
    root: Path = CALENDAR_ROOT,
    env_path: Path = DEFAULT_ENV_PATH,
    getter: Callable[..., Any] = requests.get,
    enforce_commit: bool = True,
) -> tuple[Path, dict[str, Any]]:
    """Make the single frozen calendar request; never request prices or returns."""

    collected = _timestamp(collected_at, "collected_at")
    if enforce_commit:
        strategy_discovery.require_committed(contract_path)
    contract = strategy_discovery.load_artifact(
        contract_path, expected_kind=CALENDAR_CONTRACT_KIND
    )
    if collected <= _timestamp(str(contract["created_at"]), "created_at"):
        raise ContinuousDiscoveryError("calendar collection must follow contract freeze")
    if contract.get("implementation_hashes") != _implementation_hashes():
        raise ContinuousDiscoveryError("calendar implementation drifted")
    _predecessor(enforce_commit=enforce_commit)
    inspection_path, inspection = _inspection_for_contract(
        contract, root=root, enforce_commit=enforce_commit
    )
    calendar_path = PROJECT_ROOT / str(contract["calendar_path"])
    source_path = PROJECT_ROOT / str(contract["source_path"])
    existing: list[tuple[Path, dict[str, Any]]] = []
    for path in sorted((root / "collection").glob("*.json")):
        status = strategy_discovery.load_artifact(
            path, expected_kind=CALENDAR_COLLECTION_KIND
        )
        if status.get("contract_sha256") == contract["artifact_sha256"]:
            existing.append((path, status))
    if len(existing) > 1:
        raise ContinuousDiscoveryError(
            "multiple calendar statuses bind the exact successor contract"
        )
    if existing:
        return existing[0]
    config = AlpacaConfig.optional_from_env(env_path)
    if config is None:
        raise ContinuousDiscoveryError("Alpaca calendar credentials are unavailable")
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
        raise ContinuousDiscoveryError(
            f"Alpaca calendar HTTP {response.status_code}"
        )
    try:
        rows = normalize_calendar_rows(response.json())
    except ValueError as exc:
        raise ContinuousDiscoveryError(
            "Alpaca calendar returned invalid JSON"
        ) from exc
    if (
        len(rows) < int(contract["minimum_sessions"])
        or rows[0]["date"] < CALENDAR_START
        or rows[-1]["date"] > CALENDAR_END
    ):
        raise ContinuousDiscoveryError("successor calendar coverage is incomplete")
    _write_json(calendar_path, rows)
    source = {
        "schema_version": 1,
        "campaign_id": CAMPAIGN_ID,
        "research_generation": RESEARCH_GENERATION,
        "contract_sha256": contract["artifact_sha256"],
        "contract_inspection_sha256": inspection["artifact_sha256"],
        "provider": contract["provider"],
        "endpoint": contract["endpoint"],
        "query": contract["query"],
        "collected_at": collected_at,
        "sessions": len(rows),
        "first_session": rows[0]["date"],
        "last_session": rows[-1]["date"],
        "calendar_sha256": strategy_discovery._file_hash(calendar_path),
        "provider_requests": 1,
        "request_seconds": elapsed,
        "market_prices_accessed": False,
        "target_outcomes_accessed": False,
        "broker_actions": 0,
    }
    _write_json(source_path, source)
    payload = {
        "schema_version": 1,
        "artifact_kind": CALENDAR_COLLECTION_KIND,
        "campaign_id": CAMPAIGN_ID,
        "state": "CALENDAR_COLLECTED_UNINSPECTED",
        "research_generation": RESEARCH_GENERATION,
        "successor_id": SUCCESSOR_ID,
        "contract_path": _repo_path(contract_path),
        "contract_sha256": contract["artifact_sha256"],
        "contract_inspection_path": _repo_path(inspection_path),
        "contract_inspection_sha256": inspection["artifact_sha256"],
        "calendar_path": contract["calendar_path"],
        "calendar_sha256": source["calendar_sha256"],
        "source_path": contract["source_path"],
        "sessions": len(rows),
        "provider_requests": 1,
        "request_seconds": elapsed,
        "market_prices_accessed": False,
        "target_outcomes_accessed": False,
        "broker_actions": 0,
        "collected_at": collected_at,
    }
    return strategy_discovery._write_artifact(
        payload, root / "collection", "continuous-successor-calendar-collection"
    )


def _full_sessions(path: Path) -> list[str]:
    try:
        rows = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ContinuousDiscoveryError(f"cannot read calendar: {exc}") from exc
    if not isinstance(rows, list):
        raise ContinuousDiscoveryError("calendar must be an array")
    dates = [
        str(row["date"])
        for row in rows
        if isinstance(row, Mapping)
        and row.get("open_et") == "09:30"
        and row.get("close_et") == "16:00"
    ]
    if dates != sorted(dates) or len(dates) != len(set(dates)):
        raise ContinuousDiscoveryError("full calendar sessions are invalid")
    return dates


def _data_inspection_for_calendar(
    calendar_path: Path,
    *,
    root: Path,
    enforce_commit: bool,
) -> tuple[Path, dict[str, Any]]:
    expected_path = _repo_path(calendar_path)
    expected_hash = strategy_discovery._file_hash(calendar_path)
    matches: list[tuple[Path, dict[str, Any]]] = []
    for path in sorted((root / "data-inspection").glob("*.json")):
        value = strategy_discovery.load_artifact(
            path, expected_kind=CALENDAR_DATA_INSPECTION_KIND
        )
        if (
            value.get("calendar_path") == expected_path
            and value.get("calendar_sha256") == expected_hash
        ):
            matches.append((path, value))
    if len(matches) != 1:
        raise ContinuousDiscoveryError(
            "expected one independent inspection for the exact successor calendar"
        )
    path, value = matches[0]
    if enforce_commit:
        strategy_discovery.require_committed(path)
    if value.get("state") != "CALENDAR_INSPECTED_READY":
        raise ContinuousDiscoveryError("successor calendar inspection is not ready")
    return path, value


def _scope(dates: Sequence[str]) -> dict[str, Any]:
    return {"dates": list(dates), "symbols": sorted(SYMBOLS)}


def freeze_successor_contract(
    *,
    created_at: str,
    root: Path = DEFAULT_ROOT,
    calendar_path: Path = CALENDAR_PATH,
    enforce_commit: bool = True,
) -> tuple[Path, dict[str, Any], Path]:
    """Freeze one exact 32-trial successor search on pre-predecessor evidence."""

    _timestamp(created_at, "created_at")
    result, inspection = _predecessor(enforce_commit=enforce_commit)
    data_inspection_path, data_inspection = _data_inspection_for_calendar(
        calendar_path,
        root=CALENDAR_ROOT,
        enforce_commit=enforce_commit,
    )
    if enforce_commit:
        strategy_discovery.require_committed(calendar_path)
    if (
        data_inspection.get("calendar_sha256")
        != strategy_discovery._file_hash(calendar_path)
        or data_inspection.get("outcome_exposure_index_sha256")
        != outcome_exposure.audit()["index_sha256"]
    ):
        raise ContinuousDiscoveryError("calendar or exposure inspection drifted")
    dates = _full_sessions(calendar_path)
    if len(dates) < TOTAL_SESSIONS:
        raise ContinuousDiscoveryError(
            f"successor needs {TOTAL_SESSIONS} full sessions"
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
    predecessor_start = min(
        str(item["signal_date"]) for item in result["records"]
    )
    if (
        confirmation[-1] > "2022-12-31"
        or confirmation[-1] >= predecessor_start
        or development[0] <= warmup[0]
        or not development[-1] < embargo[0] < confirmation[0]
    ):
        raise ContinuousDiscoveryError("successor evidence chronology is invalid")
    records = outcome_exposure.read_index()
    development_scope = _scope([*warmup, *development])
    confirmation_scope = _scope(confirmation)
    outcome_exposure.assert_untouched(development_scope, records)
    outcome_exposure.assert_untouched(confirmation_scope, records)
    outcome_exposure.assert_disjoint([development_scope, confirmation_scope])
    registered_at = created_at
    capacity_path, _capacity = freeze_dataset_contract(
        {
            "schema_version": 1,
            "dataset_id": f"dataset-{SUCCESSOR_ID}-capacity",
            "registered_at": registered_at,
            "requested_dates": selected,
            "dataset_payload": {
                "lane": "development",
                "claim_scope": "DEVELOPMENT_ONLY",
                "evidence_paths": [
                    _repo_path(calendar_path),
                    _repo_path(data_inspection_path),
                    _repo_path(PREDECESSOR_RESULT),
                    _repo_path(PREDECESSOR_INSPECTION),
                    "strategy_tournament/v2/OUTCOME_EXPOSURE_INDEX.jsonl",
                ],
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
                    "calendar_sha256": strategy_discovery._file_hash(calendar_path),
                    "provider_requests": 0,
                    "market_prices_accessed": False,
                    "outcomes_accessed": False,
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
        "parent_experiment_id": "broad-etf-trend-pullback-v1",
        "created_at": created_at,
        "status": "INVENTED",
        "research_generation": RESEARCH_GENERATION,
        "successor_id": SUCCESSOR_ID,
        "new_mechanism_family_slot_consumed": False,
        "prior_family_attempt_count": 1,
        "predecessor": {
            "variant_id": PREDECESSOR_VARIANT_ID,
            "result_path": _repo_path(PREDECESSOR_RESULT),
            "result_file_sha256": strategy_discovery._file_hash(PREDECESSOR_RESULT),
            "result_sha256": result["result_sha256"],
            "inspection_path": _repo_path(PREDECESSOR_INSPECTION),
            "inspection_file_sha256": strategy_discovery._file_hash(
                PREDECESSOR_INSPECTION
            ),
            "inspection_sha256": inspection["inspection_sha256"],
            "evaluated_outcome_start": min(
                str(item["signal_date"]) for item in result["records"]
            ),
            "evaluated_outcome_end": max(
                str(item["exit_date"]) for item in result["records"]
            ),
            "promotion_evidence_reused": False,
        },
        "mechanism": (
            "Cost-clearing short pullback inside the already-tested broad liquid-ETF "
            "uptrend mechanism."
        ),
        "expected_holding_behavior": (
            "Long only and flat no later than five trading sessions after entry."
        ),
        "dataset_lane": "development",
        "universe_requirements": {
            "symbols": list(SYMBOLS),
            "complete_frozen_daily_history": True,
        },
        "entry_rule": (
            "After completed SMA, RSI2, and three-session-decline qualification, "
            "enter the highest-ranked ETF at the next observable session open."
        ),
        "stop_rule": (
            "Freeze a long stop one or one-and-a-half completed ATR14 below the "
            "next-open entry; invalid stops are missed trades."
        ),
        "exit_rule": (
            "Resolve the frozen stop first and otherwise exit at the completed "
            "close after three or five sessions."
        ),
        "ranking_rule": "Lowest RSI2, deepest three-session decline, then symbol.",
        "selection_rule": (
            "At most one new family entry per day under portfolio risk, notional, "
            "daily-entry, and capital-contention caps."
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
            "The predecessor corpus is hypothesis-generating only and cannot count toward promotion.",
            "No date from the predecessor's 2023-2025 evaluated corpus may enter this successor.",
            "Any globally exposed date-symbol pair disqualifies confirmation.",
        ],
        "production_compatibility_risks": [
            "Live quote, spread, depth, halt, tradability, timing, protection, and reconciliation remain required."
        ],
        "material_difference_rationale": (
            "This is a new exact version inside the existing broad-ETF pullback "
            "mechanism, with a prospectively frozen complete grid and wholly disjoint "
            "predecessor evidence. It is not a new mechanism family."
        ),
        "development_dates": development,
        "development_warmup_dates": warmup,
        "confirmation_warmup_dates": selected[
            -CONFIRMATION_SESSIONS - DEVELOPMENT_WARMUP_SESSIONS:
            -CONFIRMATION_SESSIONS
        ],
        "embargo_dates": embargo,
        "confirmation_dates": confirmation,
        "development_scope": development_scope,
        "confirmation_scope": confirmation_scope,
        "outcome_exposure_index_sha256": outcome_exposure.audit()["index_sha256"],
        "universe": {"symbols": list(SYMBOLS), "point_in_time": True},
        "historical_data_contract": {
            "daily_provider": "alpaca",
            "daily_endpoint": "/v2/stocks/{symbol}/bars",
            "daily_feed": "sip",
            "daily_adjustment": "raw",
            "split_provider": "massive",
            "provider_substitutions_allowed": False,
        },
        "costs_bps_per_side": [5, 10, 20],
        "partitions": {
            "rolling_origin": True,
            "confirmation_untouched": True,
            "predecessor_corpus_disjoint": True,
        },
        "falsifiers": [
            "nonpositive stressed log growth",
            "unstable parameter neighbors",
            "selection-aware statistical rejection",
            "incomplete execution or trial accounting",
        ],
        "implementation_files": [
            "continuous_strategy_discovery.py",
            "continuous_strategy_discovery_inspection.py",
            "dense_data_collection.py",
            "dense_data_collection_inspection.py",
            "dense_strategy_plugin.py",
            "dense_strategy_runtime.py",
            "learning_statistics.py",
            "learning_experiment.py",
            "strategy_discovery.py",
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
        "calendar_path": _repo_path(calendar_path),
    }
    validated = strategy_discovery._validate_family_contract(contract)
    validate_existing_successor_contract(validated, enforce_commit=enforce_commit)
    digest = hashlib.sha256(_canonical(contract)).hexdigest()
    path = root / SUCCESSOR_ID / "family-contract" / f"contract-{digest}.json"
    _write_json(path, contract)
    return path, contract, capacity_path


def validate_existing_successor_contract(
    contract: Mapping[str, Any], *, enforce_commit: bool = True
) -> None:
    """Fail closed before any pre-reset data planning for this one successor."""

    if not (
        contract.get("campaign_id") == CAMPAIGN_ID
        and contract.get("family_id") == FAMILY_ID
        and contract.get("mechanism_family") == MECHANISM_FAMILY
        and contract.get("strategy_id") == STRATEGY_ID
        and contract.get("successor_id") == SUCCESSOR_ID
        and contract.get("research_generation") == RESEARCH_GENERATION
        and contract.get("new_mechanism_family_slot_consumed") is False
        and contract.get("prior_family_attempt_count") == 1
        and contract.get("selection_mode") == "development_search"
        and len(contract.get("trial_family", [])) == 32
        and contract.get("historical_data_contract")
        == {
            "daily_provider": "alpaca",
            "daily_endpoint": "/v2/stocks/{symbol}/bars",
            "daily_feed": "sip",
            "daily_adjustment": "raw",
            "split_provider": "massive",
            "provider_substitutions_allowed": False,
        }
    ):
        raise ContinuousDiscoveryError("existing-family successor identity drifted")
    predecessor = contract.get("predecessor")
    if not isinstance(predecessor, Mapping) or not (
        predecessor.get("variant_id") == PREDECESSOR_VARIANT_ID
        and predecessor.get("promotion_evidence_reused") is False
        and predecessor.get("result_file_sha256")
        == strategy_discovery._file_hash(PREDECESSOR_RESULT)
        and predecessor.get("inspection_file_sha256")
        == strategy_discovery._file_hash(PREDECESSOR_INSPECTION)
    ):
        raise ContinuousDiscoveryError("successor predecessor binding drifted")
    if max(map(str, contract["confirmation_dates"])) >= str(
        predecessor["evaluated_outcome_start"]
    ):
        raise ContinuousDiscoveryError(
            "successor confirmation is not pre-predecessor"
        )
    if contract.get("outcome_exposure_index_sha256") != outcome_exposure.audit()[
        "index_sha256"
    ]:
        raise ContinuousDiscoveryError("successor outcome-exposure index drifted")
    outcome_exposure.assert_untouched(
        contract["confirmation_scope"], outcome_exposure.read_index()
    )
    if enforce_commit:
        _predecessor(enforce_commit=True)


def _current_successor_inspection(
    *,
    discovery_root: Path,
    family_id: str,
    successor_id: str,
) -> dict[str, Any] | None:
    matches: list[dict[str, Any]] = []
    family_root = discovery_root / family_id
    for inspection_path in sorted(
        (family_root / "development-inspection").glob("*.json")
    ):
        try:
            inspection = strategy_discovery.load_artifact(
                inspection_path,
                expected_kind="development-search-inspection",
            )
            result_path = Path(str(inspection["result_path"]))
            if not result_path.is_absolute():
                result_path = PROJECT_ROOT / result_path
            result = strategy_discovery.load_artifact(
                result_path,
                expected_kind="development-search-result",
            )
            search_path = Path(str(result["search_path"]))
            if not search_path.is_absolute():
                search_path = PROJECT_ROOT / search_path
            search = strategy_discovery.load_artifact(
                search_path,
                expected_kind="frozen-development-search",
            )
        except (
            KeyError,
            OSError,
            strategy_discovery.StrategyDiscoveryError,
        ):
            continue
        contract = search.get("family_contract")
        if (
            isinstance(contract, Mapping)
            and contract.get("successor_id") == successor_id
        ):
            matches.append(inspection)
    if len(matches) > 1:
        raise ContinuousDiscoveryError(
            "current successor has multiple development inspections"
        )
    return matches[0] if matches else None


def build_status(
    *,
    root: Path = DEFAULT_ROOT,
    discovery_root: Path = strategy_discovery.DEFAULT_ROOT,
) -> dict[str, Any]:
    import liquid_equity_residual_reversal_discovery as current

    calendar_contracts = sorted((CALENDAR_ROOT / "contract").glob("*.json"))
    calendar_inspections = sorted(
        (CALENDAR_ROOT / "contract-inspection").glob("*.json")
    )
    calendar_collections = sorted((CALENDAR_ROOT / "collection").glob("*.json"))
    calendar_data_inspections = sorted(
        (CALENDAR_ROOT / "data-inspection").glob("*.json")
    )
    family_contracts = sorted(
        (root / current.SUCCESSOR_ID / "family-contract").glob("*.json")
    )
    current_inspection = _current_successor_inspection(
        discovery_root=discovery_root,
        family_id=current.FAMILY_ID,
        successor_id=current.SUCCESSOR_ID,
    )
    family_discovery_root = discovery_root / current.FAMILY_ID
    if not family_contracts:
        state = "READY_TO_FREEZE_SUCCESSOR"
        next_action = (
            "freeze the 48-trial liquid-equity residual-reversal successor "
            "and exact evidence partitions"
        )
    elif current_inspection is not None and current_inspection.get(
        "state"
    ) == "REJECTED":
        state = "EXISTING_FAMILY_QUEUE_EXHAUSTED"
        next_action = (
            "freeze and independently inspect the rolling calendar allocation "
            "contract, then freeze all three disjoint evidence contracts"
        )
    elif not family_discovery_root.exists():
        state = "SUCCESSOR_CONTRACT_FROZEN"
        next_action = "run discovery preflight and freeze the development search"
    else:
        state = "DISCOVERY_ACTIVE"
        next_action = "continue the strategy_discovery transition chain"
    if state == "EXISTING_FAMILY_QUEUE_EXHAUSTED":
        import dense_batch_readiness

        preactivation_readiness = dense_batch_readiness.build_status()
    else:
        preactivation_readiness = {
            "state": "NOT_CURRENT",
            "provider_access_permitted": False,
            "target_outcome_access_permitted": False,
            "broker_actions_permitted": False,
        }
    return {
        "schema_version": 1,
        "campaign_id": CAMPAIGN_ID,
        "state": state,
        "next_action": next_action,
        "successor": {
            "successor_id": current.SUCCESSOR_ID,
            "family_id": current.FAMILY_ID,
            "mechanism_family": current.MECHANISM_FAMILY,
            "research_generation": current.RESEARCH_GENERATION,
            "new_mechanism_family_slot_consumed": False,
            "predecessor_corpus_reused": False,
            "calendar_wait_required": False,
            "outcome_access_wait_required": False,
            "outcome_access_prerequisites_remaining": state
            == "EXISTING_FAMILY_QUEUE_EXHAUSTED",
            "development_disposition": (
                current_inspection.get("state")
                if current_inspection is not None
                else None
            ),
            "broker_actions_permitted": False,
        },
        "preactivation_readiness": preactivation_readiness,
        "artifacts": {
            "calendar_contracts": len(calendar_contracts),
            "calendar_contract_inspections": len(calendar_inspections),
            "calendar_collections": len(calendar_collections),
            "calendar_data_inspections": len(calendar_data_inspections),
            "family_contracts": len(family_contracts),
        },
        "new_family_batch": next_week_discovery_batch.activation_status(),
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("status")
    freeze_calendar = sub.add_parser("freeze-calendar")
    freeze_calendar.add_argument("--created-at", required=True)
    collect = sub.add_parser("collect-calendar")
    collect.add_argument("contract", type=Path)
    collect.add_argument("--collected-at", required=True)
    freeze_successor = sub.add_parser("freeze-successor")
    freeze_successor.add_argument("--created-at", required=True)
    return parser


def main() -> int:
    args = _parser().parse_args()
    try:
        if args.command == "status":
            result = build_status(root=args.root)
        elif args.command == "freeze-calendar":
            path, artifact = freeze_calendar_contract(
                created_at=args.created_at,
                root=args.root / SUCCESSOR_ID / "calendar",
            )
            result = {
                "path": _repo_path(path),
                "artifact_sha256": artifact["artifact_sha256"],
                "state": artifact["state"],
            }
        elif args.command == "collect-calendar":
            path, artifact = collect_calendar(
                args.contract,
                collected_at=args.collected_at,
                root=args.root / SUCCESSOR_ID / "calendar",
            )
            result = {
                "path": _repo_path(path),
                "artifact_sha256": artifact["artifact_sha256"],
                "state": artifact["state"],
                "provider_requests": artifact["provider_requests"],
            }
        else:
            path, artifact, capacity = freeze_successor_contract(
                created_at=args.created_at,
                root=args.root,
            )
            result = {
                "path": _repo_path(path),
                "capacity_manifest": _repo_path(capacity),
                "trial_count": 32,
                "state": artifact["status"],
            }
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    except (
        ContinuousDiscoveryError,
        outcome_exposure.OutcomeExposureError,
        strategy_discovery.StrategyDiscoveryError,
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


if __name__ == "__main__":
    raise SystemExit(main())

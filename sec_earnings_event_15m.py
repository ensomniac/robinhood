"""Freeze and materialize the event-first SEC earnings-gap 15-minute family.

The family contract is built only from the independently inspected SEC event
inventory and the committed market calendar.  Development market rows are
opened only after ``strategy_discovery.py freeze-search`` is committed.  The
2024 confirmation partition remains unopened until an exact winner is frozen.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import statistics
from collections import defaultdict
from collections.abc import Mapping, Sequence
from datetime import datetime
from pathlib import Path
from typing import Any

import dense_strategy_runtime as runtime
import outcome_exposure
import portfolio_maturity
import sec_earnings_full_inventory as inventory_v1
import sec_earnings_full_inventory_recovery as inventory_source
import strategy_discovery
from gap_protection_successor import CALENDAR_PATH
from historical_store import (
    HistoricalDayStore,
    canonical_sha256,
    expand_bar,
    sha256_file,
)
from learning_data import freeze_dataset_contract
from learning_experiment import DEVELOPMENT_SEARCH_RULE
from scanner_replay import load_calendar


PROJECT_ROOT = Path(__file__).resolve().parent
CAMPAIGN_ID = portfolio_maturity.V2_CAMPAIGN_ID
FAMILY_ID = runtime.SEC_EARNINGS_GAP_15M_FAMILY
MECHANISM_FAMILY = "sec-filed-earnings-gap-continuation"
STRATEGY_ID = FAMILY_ID
SUCCESSOR_ID = FAMILY_ID
SUPERSEDED_EXPANDED_CONTRACT_SHA256 = (
    "7cfc05ae201a533a621a53dce8206bb41b79d7f7c112da16df7735d306c21bfd"
)
SUPERSEDED_ADAPTER_CONTRACT_SHA256 = (
    "13914994fb7ad1627a154c61b88328c617a17726a530d2d178d97164731371f1"
)
DEFAULT_ROOT = PROJECT_ROOT / "strategy_tournament/v2/discovery"
CAPACITY_INSPECTION = (
    PROJECT_ROOT
    / "strategy_tournament/v2/continuous/"
    "sec-filed-earnings-gap-continuation-event-first/"
    "dataset-sec-filed-earnings-event-first-2023-2024-v2/"
    "capacity-inspection/"
    "inspection-"
    "9eeaf66bc3ff5c692fe2df650f4f6bf1ae48d4b12a025e1d87071cb25086d776"
    ".json"
)
CAPACITY_COLLECTION = (
    PROJECT_ROOT
    / "strategy_tournament/v2/continuous/"
    "sec-filed-earnings-gap-continuation-event-first/"
    "dataset-sec-filed-earnings-event-first-2023-2024-v2/"
    "capacity-collection/"
    "collection-"
    "c4229b71e5a2b27e3d11dd4f3b793993adc6d7bde48f3360e129b83b776bb104"
    ".json"
)
DEVELOPMENT_START = "2023-01-03"
DEVELOPMENT_END = "2023-12-29"
EMBARGO_DATES = [
    "2024-01-02",
    "2024-01-03",
    "2024-01-04",
    "2024-01-05",
    "2024-01-08",
]
CONFIRMATION_START = "2024-01-09"
CONFIRMATION_END = "2024-12-31"
LOOKBACK_SESSIONS = 20
PARAMETER_GRID = {
    "maximum_structural_stop_fraction": [0.03, 0.04],
    "minimum_gap_fraction": [0.02, 0.04],
    "minimum_opening_close_location": [0.5, 0.75],
    "minimum_opening_volume_ratio": [1.5, 2.5],
    "target_r": [1.5, 2.0],
}


class SecEarningsEvent15mError(RuntimeError):
    """The event-first family evidence graph is incomplete or drifted."""


def _repo_path(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(PROJECT_ROOT.resolve()))
    except ValueError as exc:
        raise SecEarningsEvent15mError(
            f"path escaped the repository: {path}"
        ) from exc


def _timestamp(value: str, field: str) -> str:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise SecEarningsEvent15mError(f"{field} is invalid") from exc
    if parsed.tzinfo is None:
        raise SecEarningsEvent15mError(f"{field} needs a timezone")
    return parsed.isoformat()


def _write_gzip(path: Path, value: Mapping[str, Any]) -> None:
    import io

    encoded = (
        json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode()
        + b"\n"
    )
    buffer = io.BytesIO()
    with gzip.GzipFile(
        fileobj=buffer,
        mode="wb",
        compresslevel=6,
        mtime=0,
    ) as stream:
        stream.write(encoded)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_bytes(buffer.getvalue())
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()


def _read_gzip(path: Path) -> dict[str, Any]:
    try:
        with gzip.open(path, "rt", encoding="utf-8") as source:
            value = json.load(source)
    except (OSError, json.JSONDecodeError) as exc:
        raise SecEarningsEvent15mError(
            f"cannot read private scope {path}: {exc}"
        ) from exc
    if not isinstance(value, dict):
        raise SecEarningsEvent15mError("private scope must be an object")
    return value


def _private_scope_path(store: HistoricalDayStore) -> Path:
    return (
        store.root
        / "_derived"
        / "sec_earnings_event_15m"
        / FAMILY_ID
        / "event-scope.json.gz"
    )


def _read_inventory(
    store: HistoricalDayStore,
    *,
    enforce_commit: bool,
) -> dict[str, Any]:
    if enforce_commit:
        strategy_discovery.require_committed(CAPACITY_INSPECTION)
        strategy_discovery.require_committed(CAPACITY_COLLECTION)
    inspection = inventory_v1._read_json(CAPACITY_INSPECTION)
    collection = inventory_v1._read_json(CAPACITY_COLLECTION)
    path = inventory_source._private_inventory_path(store)
    inventory = inventory_v1._read_gzip(path)
    if not (
        inspection.get("state") == "EVENT_FIRST_CAPACITY_INSPECTED_READY"
        and inspection.get("inspection_sha256")
        == inventory_v1.self_hash(inspection, "inspection_sha256")
        and collection.get("collection_sha256")
        == inventory_v1.self_hash(collection, "collection_sha256")
        and inspection.get("capacity_adequate") is True
        and inspection.get("valid") is True
        and inspection.get("market_prices_accessed") is False
        and inspection.get("forward_returns_accessed") is False
        and inspection.get("confirmation_outcomes_accessed") is False
        and inspection.get("collection_sha256")
        == collection.get("collection_sha256")
        and sha256_file(path)
        == collection.get("private_inventory_file_sha256")
        and inventory.get("content_sha256")
        == collection.get("private_inventory_content_sha256")
        and len(inventory.get("events", []))
        == collection.get("retained_event_pairs")
    ):
        raise SecEarningsEvent15mError(
            "independently inspected SEC event inventory is invalid"
        )
    return inventory


def _calendar_slice(
    calendar: Sequence[str], start: str, end: str
) -> list[str]:
    result = [day for day in calendar if start <= day <= end]
    if not result or result[0] != start or result[-1] != end:
        raise SecEarningsEvent15mError(
            f"calendar does not exactly cover {start} through {end}"
        )
    return result


def _scope(
    *,
    store: HistoricalDayStore | None = None,
    enforce_commit: bool = True,
) -> dict[str, Any]:
    source = store or HistoricalDayStore.from_env()
    inventory = _read_inventory(source, enforce_commit=enforce_commit)
    calendar = load_calendar(CALENDAR_PATH)
    development_dates = _calendar_slice(
        calendar, DEVELOPMENT_START, DEVELOPMENT_END
    )
    confirmation_dates = _calendar_slice(
        calendar, CONFIRMATION_START, CONFIRMATION_END
    )
    positions = {day: index for index, day in enumerate(calendar)}
    partitions: dict[str, list[dict[str, Any]]] = {
        "development": [],
        "confirmation": [],
    }
    for raw in inventory["events"]:
        partition = str(raw["partition"])
        if partition not in partitions:
            continue
        day = str(raw["signal_date"])
        index = positions.get(day)
        if index is None:
            raise SecEarningsEvent15mError(
                f"event escaped the committed calendar: {day}"
            )
        observation_dates = list(
            calendar[max(0, index - LOOKBACK_SESSIONS) : index + 1]
        )
        row = {
            "accepted_at": str(raw["accepted_at"]),
            "accession": str(raw["accession"]),
            "cik": str(raw["cik"]),
            "instrument_id": str(raw["instrument_id"]),
            "observation_dates": observation_dates,
            "primary_document": str(raw["primary_document"]),
            "signal_date": day,
            "symbol": str(raw["symbol"]),
        }
        row["event_id"] = canonical_sha256(row)
        partitions[partition].append(row)
    for rows in partitions.values():
        rows.sort(
            key=lambda item: (
                item["signal_date"],
                item["symbol"],
                item["event_id"],
            )
        )
    exposure_records = [
        outcome_exposure.validate_record(record)
        for record in outcome_exposure.read_index()
    ]
    wanted_by_date: dict[str, set[str]] = defaultdict(set)
    for event in partitions["confirmation"]:
        for day in event["observation_dates"]:
            wanted_by_date[str(day)].add(str(event["symbol"]))
    exposed_pairs: set[tuple[str, str]] = set()
    for record in exposure_records:
        exposure_scope = record["scope"]
        if "symbols_by_date" in exposure_scope:
            for day, symbols in exposure_scope["symbols_by_date"].items():
                wanted = wanted_by_date.get(day)
                if wanted:
                    if symbols == ["*"]:
                        exposed_pairs.update((day, symbol) for symbol in wanted)
                    else:
                        exposed_pairs.update(
                            (day, symbol)
                            for symbol in wanted.intersection(symbols)
                        )
        else:
            symbols = exposure_scope["symbols"]
            for day in exposure_scope["dates"]:
                wanted = wanted_by_date.get(day)
                if wanted:
                    if symbols == ["*"]:
                        exposed_pairs.update((day, symbol) for symbol in wanted)
                    else:
                        exposed_pairs.update(
                            (day, symbol)
                            for symbol in wanted.intersection(symbols)
                        )
    retained_confirmation: list[dict[str, Any]] = []
    removed_confirmation = 0
    for event in partitions["confirmation"]:
        if any(
            (day, str(event["symbol"])) in exposed_pairs
            for day in event["observation_dates"]
        ):
            removed_confirmation += 1
            continue
        retained_confirmation.append(event)
    partitions["confirmation"] = retained_confirmation

    def build_target_scope(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
        symbols_by_date: dict[str, set[str]] = defaultdict(set)
        for row in rows:
            symbols_by_date[str(row["signal_date"])].add(str(row["symbol"]))
        dates = sorted(symbols_by_date)
        return {
            "dates": dates,
            "symbols_by_date": {
                day: sorted(symbols_by_date[day]) for day in dates
            },
        }

    development_events = partitions["development"]
    confirmation_events = partitions["confirmation"]
    development_signal_dates = sorted(
        {str(row["signal_date"]) for row in development_events}
    )
    confirmation_signal_dates = sorted(
        {str(row["signal_date"]) for row in confirmation_events}
    )
    if len(confirmation_signal_dates) < 30:
        raise SecEarningsEvent15mError(
            "untouched confirmation capacity fell below 30 signal dates"
        )
    confirmation_scope = build_target_scope(confirmation_events)
    outcome_exposure.assert_untouched(
        confirmation_scope,
        outcome_exposure.read_index(),
    )
    private_scope = {
        "schema_version": 1,
        "family_id": FAMILY_ID,
        "development_events": development_events,
        "confirmation_events": confirmation_events,
        "confirmation_events_removed_for_lookback_exposure": (
            removed_confirmation
        ),
    }
    private_scope["content_sha256"] = canonical_sha256(private_scope)
    private_path = _private_scope_path(source)
    _write_gzip(private_path, private_scope)
    private_relative_path = str(
        private_path.resolve().relative_to(source.root.resolve())
    )
    return {
        "schema_version": 1,
        "artifact_kind": "sec-earnings-event-15m-scope",
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "state": "EVENT_SCOPE_FROZEN_OUTCOME_BLIND",
        "development_dates": development_dates,
        "development_signal_dates": development_signal_dates,
        "development_event_pairs": len(development_events),
        "development_events_sha256": canonical_sha256(development_events),
        "development_scope": build_target_scope(development_events),
        "embargo_dates": list(EMBARGO_DATES),
        "confirmation_dates": confirmation_dates,
        "confirmation_signal_dates": confirmation_signal_dates,
        "confirmation_event_pairs": len(confirmation_events),
        "confirmation_events_sha256": canonical_sha256(confirmation_events),
        "confirmation_events_removed_for_lookback_exposure": (
            removed_confirmation
        ),
        "confirmation_scope": confirmation_scope,
        "private_scope": {
            "format": "json.gz",
            "external_relative_path": private_relative_path,
            "external_file_sha256": sha256_file(private_path),
            "content_sha256": private_scope["content_sha256"],
        },
        "event_inventory_content_sha256": inventory["content_sha256"],
        "event_inventory_file_sha256": sha256_file(
            inventory_source._private_inventory_path(source)
        ),
        "outcome_exposure_index_sha256": outcome_exposure.audit()[
            "index_sha256"
        ],
        "market_prices_accessed": False,
        "forward_returns_accessed": False,
        "confirmation_outcomes_accessed": False,
        "broker_actions": 0,
    }


def _load_private_scope(
    store: HistoricalDayStore,
    public_scope: Mapping[str, Any],
) -> dict[str, Any]:
    binding = public_scope.get("private_scope")
    if not isinstance(binding, Mapping):
        raise SecEarningsEvent15mError("public scope lacks private binding")
    relative = Path(str(binding.get("external_relative_path", "")))
    if relative.is_absolute() or ".." in relative.parts:
        raise SecEarningsEvent15mError("private scope path is unsafe")
    path = (store.root / relative).resolve()
    if store.root.resolve() not in path.parents:
        raise SecEarningsEvent15mError("private scope escaped the historical store")
    value = _read_gzip(path)
    content = dict(value)
    supplied = content.pop("content_sha256", None)
    if not (
        binding.get("format") == "json.gz"
        and path.is_file()
        and sha256_file(path) == binding.get("external_file_sha256")
        and supplied == canonical_sha256(content)
        and supplied == binding.get("content_sha256")
        and value.get("family_id") == FAMILY_ID
        and canonical_sha256(value.get("development_events"))
        == public_scope.get("development_events_sha256")
        and canonical_sha256(value.get("confirmation_events"))
        == public_scope.get("confirmation_events_sha256")
        and len(value.get("development_events", []))
        == public_scope.get("development_event_pairs")
        and len(value.get("confirmation_events", []))
        == public_scope.get("confirmation_event_pairs")
    ):
        raise SecEarningsEvent15mError("private scope binding drifted")
    return value


def freeze_family(
    *,
    created_at: str,
    root: Path = DEFAULT_ROOT,
    store: HistoricalDayStore | None = None,
    enforce_commit: bool = True,
) -> tuple[Path, dict[str, Any], Path, Path]:
    """Freeze the complete 32-trial family before development outcomes."""

    created = _timestamp(created_at, "created_at")
    implementation_files = [
        "sec_earnings_event_15m.py",
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
            strategy_discovery.require_committed(PROJECT_ROOT / relative)
    scope_payload = _scope(store=store, enforce_commit=enforce_commit)
    scope_path, scope = strategy_discovery._write_artifact(
        scope_payload,
        root / SUCCESSOR_ID / "event-scope",
        "sec-earnings-event-15m-scope",
    )
    capacity_path, _capacity = freeze_dataset_contract(
        {
            "schema_version": 1,
            "dataset_id": f"dataset-{SUCCESSOR_ID}-capacity",
            "registered_at": created,
            "requested_dates": scope["development_dates"],
            "dataset_payload": {
                "lane": "development",
                "claim_scope": "DEVELOPMENT_ONLY",
                "evidence_phase": "OUTCOME_BLIND_CAPACITY_ONLY",
                "inspected": True,
                "point_in_time_evidence": True,
                "evidence_paths": [
                    _repo_path(CAPACITY_INSPECTION),
                    _repo_path(CAPACITY_COLLECTION),
                    _repo_path(scope_path),
                    _repo_path(CALENDAR_PATH),
                    "strategy_tournament/v2/OUTCOME_EXPOSURE_INDEX.jsonl",
                ],
                "dense_capacity": {
                    "family_id": FAMILY_ID,
                    "formal_capacity": len(
                        scope["development_signal_dates"]
                    ),
                    "development_event_pairs": scope[
                        "development_event_pairs"
                    ],
                    "confirmation_event_pairs": scope[
                        "confirmation_event_pairs"
                    ],
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
        "created_at": created,
        "status": "INVENTED",
        "dataset_lane": "development",
        "mechanism": (
            "A point-in-time SEC Item 2.02 earnings filing accepted after "
            "the prior close and by 09:25 ET can create continuing price "
            "discovery when a liquid common stock gaps higher and the first "
            "completed 15-minute interval confirms demand."
        ),
        "expected_holding_behavior": (
            "Long only from the 09:45 ET next observable 15-minute open "
            "until the structural stop, frozen R target, or 15:45 ET cutoff."
        ),
        "entry_rule": (
            "Require prior close at least $10, prior 20-session median dollar "
            "volume at least $50 million, the frozen positive gap, bullish "
            "first 15-minute close location, and opening-volume-ratio gates; "
            "rank and enter at the 09:45 ET open."
        ),
        "stop_rule": (
            "Use the completed 09:30-09:45 low, rejecting a nonpositive, "
            "nonprotective, or cap-exceeding stop."
        ),
        "exit_rule": (
            "Resolve gaps, stop, then target on each later completed "
            "15-minute interval, using stop first on ambiguity; otherwise "
            "exit at the 15:45 ET bar open."
        ),
        "ranking_rule": (
            "Largest positive gap, opening-volume ratio, prior median dollar "
            "volume, symbol, then canonical event ID."
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
            "Generic opening-gap trials carried no causal earnings source. "
            "This is the first development search whose complete denominator "
            "requires a point-in-time SEC Item 2.02 filing. The 96 generic-gap "
            "trials remain adverse history but are not relabeled as trials of "
            "this separate SEC-qualified mechanism."
        ),
        "universe_requirements": {
            "security_type": "point-in-time U.S. common stock",
            "sec_form": "8-K",
            "required_item": "2.02",
            "excluded_item": "3.02",
            "acceptance_cutoff_et": "09:25:00",
            "prior_close_minimum": 10.0,
            "prior_20_session_median_dollar_volume_minimum": 50_000_000,
            "complete_event_denominator": True,
        },
        "execution_assumptions": {
            "next_observable_fill": "09:45:00_ET_open",
            "same_interval_ambiguity": "stop_first",
            "maximum_hold_sessions": 1,
            "missing_or_invalid_data": "missed_trade_no_substitute",
            "minimum_gross_to_primary_round_trip_cost": 5.0,
            "force_flat_et": "15:45:00",
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
            "The development partition becomes contaminated training only after search freeze.",
            "The 2024 confirmation partition and its target returns remain unopened until one exact winner is frozen.",
            "A confirmation pair exposed in the global index receives zero credit.",
        ],
        "production_compatibility_risks": [
            "Live SEC filing capture, complete event ranking, fresh quote, spread, depth, halt, tradability, protection, and reconciliation remain mandatory."
        ],
        "parameter_grid": PARAMETER_GRID,
        "selection_mode": "development_search",
        "winner_selection": DEVELOPMENT_SEARCH_RULE,
        "development_dates": scope["development_dates"],
        "development_signal_dates": scope["development_signal_dates"],
        "embargo_dates": scope["embargo_dates"],
        "confirmation_dates": scope["confirmation_dates"],
        "confirmation_signal_dates": scope["confirmation_signal_dates"],
        "confirmation_signal_capacity": len(
            scope["confirmation_signal_dates"]
        ),
        "development_scope": scope["development_scope"],
        "confirmation_scope": scope["confirmation_scope"],
        "outcome_exposure_index_sha256": scope[
            "outcome_exposure_index_sha256"
        ],
        "universe": {
            "identity": "complete SEC Item 2.02 event-first common-stock graph",
            "event_scope_path": _repo_path(scope_path),
            "event_scope_sha256": scope["artifact_sha256"],
            "event_inventory_content_sha256": scope[
                "event_inventory_content_sha256"
            ],
            "point_in_time": True,
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
            "provider": "alpaca",
            "feed": "sip",
            "adjustment": "raw",
            "bar_timeframe": "15m",
            "lookback_sessions": LOOKBACK_SESSIONS,
            "substitutions_allowed": False,
            "market_price_access_before_search_freeze": False,
            "confirmation_access_before_winner_freeze": False,
        },
        "implementation_files": implementation_files,
        "plugin": {
            "module": "dense_strategy_plugin",
            "preflight": "preflight",
            "evaluate_development": "evaluate_development",
            "evaluate_confirmation": "evaluate_confirmation",
            "evaluate_production": "evaluate_production",
        },
        "capacity_policy": {"retire_below": 50, "fast_lane_at": 100},
        "capacity_manifest": _repo_path(capacity_path),
        "event_scope_path": _repo_path(scope_path),
        "event_scope_sha256": scope["artifact_sha256"],
        "prior_generic_gap_trials_preserved": 96,
        "prior_selection_trial_count": 0,
        "supersedes_contract_sha256": (
            SUPERSEDED_EXPANDED_CONTRACT_SHA256
        ),
        "superseded_adapter_contract_sha256": (
            SUPERSEDED_ADAPTER_CONTRACT_SHA256
        ),
        "supersession_reason": (
            "The same outcome-blind event graph and rules are republished "
            "with row-level events in the ignored content-addressed store; "
            "the expanded predecessor remains immutable Git history but is "
            "not an active discovery predecessor."
        ),
    }
    validated = strategy_discovery._validate_family_contract(contract)
    digest = hashlib.sha256(
        json.dumps(
            validated,
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    ).hexdigest()
    path = (
        root
        / SUCCESSOR_ID
        / "family-contract"
        / f"contract-{digest}.json"
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    rendered = json.dumps(validated, indent=2, sort_keys=True) + "\n"
    if path.exists():
        if path.read_text(encoding="utf-8") != rendered:
            raise SecEarningsEvent15mError(
                "hash-addressed family contract drifted"
            )
    else:
        temporary = path.with_suffix(".json.tmp")
        temporary.write_text(rendered, encoding="utf-8")
        temporary.replace(path)
    return path, validated, capacity_path, scope_path


def _select(
    document: Mapping[str, Any] | None,
    *,
    kind: str,
    channel: str,
    timeframe: str,
) -> Mapping[str, Any] | None:
    if not isinstance(document, Mapping):
        return None
    matches = [
        row
        for row in document.get("datasets", [])
        if row.get("kind") == kind
        and row.get("provider") == "alpaca"
        and row.get("channel") == channel
        and row.get("timeframe") == timeframe
        and row.get("feed") == "sip"
        and row.get("adjustment") == "raw"
        and row.get("quality", {}).get("complete") is True
    ]
    if len(matches) != 1:
        return None
    return matches[0]


def _daily_and_opening(
    store: HistoricalDayStore,
    symbol: str,
    day: str,
) -> tuple[dict[str, Any], float] | None:
    document = store.load(symbol, day)
    daily = _select(
        document,
        kind="derived",
        channel="minute_aggregate_regular",
        timeframe="1d",
    )
    intraday = _select(
        document,
        kind="bars",
        channel="trades",
        timeframe="15m",
    )
    if daily is None or intraday is None:
        return None
    daily_rows = daily.get("rows")
    intraday_rows = intraday.get("rows")
    if (
        not isinstance(daily_rows, list)
        or len(daily_rows) != 1
        or not isinstance(intraday_rows, list)
        or len(intraday_rows) != 26
    ):
        return None
    daily_bar = expand_bar(daily_rows[0])
    first = expand_bar(intraday_rows[0])
    return (
        {
            "close": float(daily_bar["close"]),
            "volume": float(daily_bar["volume"]),
        },
        float(first["volume"]),
    )


def _current_bars(
    store: HistoricalDayStore, symbol: str, day: str
) -> list[dict[str, Any]] | None:
    document = store.load(symbol, day)
    intraday = _select(
        document,
        kind="bars",
        channel="trades",
        timeframe="15m",
    )
    if intraday is None or len(intraday.get("rows", [])) != 26:
        return None
    rows = []
    for raw in intraday["rows"]:
        bar = expand_bar(raw)
        rows.append(
            {
                "timestamp": str(bar["time_et"]),
                "open": float(bar["open"]),
                "high": float(bar["high"]),
                "low": float(bar["low"]),
                "close": float(bar["close"]),
                "volume": float(bar["volume"]),
            }
        )
    return rows


def _deduplicate_events(
    events: Sequence[Mapping[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    by_day_symbol: dict[tuple[str, str], list[Mapping[str, Any]]] = defaultdict(
        list
    )
    for event in events:
        by_day_symbol[
            (str(event["signal_date"]), str(event["symbol"]))
        ].append(event)
    result: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for (day, _symbol), rows in sorted(by_day_symbol.items()):
        selected = sorted(
            rows,
            key=lambda row: (str(row["accepted_at"]), str(row["event_id"])),
        )[0]
        result[day].append(dict(selected))
    for rows in result.values():
        rows.sort(key=lambda row: str(row["symbol"]))
    return dict(result)


def _search_contract(search: Mapping[str, Any]) -> dict[str, Any]:
    contract = search.get("family_contract")
    if not (
        isinstance(contract, Mapping)
        and contract.get("family_id") == FAMILY_ID
        and contract.get("historical_data_contract", {}).get(
            "market_price_access_before_search_freeze"
        )
        is False
    ):
        raise SecEarningsEvent15mError(
            "development search is not the frozen SEC earnings family"
        )
    return dict(contract)


def build_development_dataset(
    *,
    search_path: Path,
    store: HistoricalDayStore | None = None,
    enforce_commit: bool = True,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Open only frozen development rows and build the shared 32-trial input."""

    source = store or HistoricalDayStore.from_env()
    if enforce_commit:
        strategy_discovery.require_committed(search_path)
    search = strategy_discovery.load_artifact(
        search_path,
        expected_kind="frozen-development-search",
    )
    contract = _search_contract(search)
    scope_path = PROJECT_ROOT / str(contract["event_scope_path"])
    if enforce_commit:
        strategy_discovery.require_committed(scope_path)
    scope = strategy_discovery.load_artifact(
        scope_path,
        expected_kind="sec-earnings-event-15m-scope",
    )
    if scope["artifact_sha256"] != contract["event_scope_sha256"]:
        raise SecEarningsEvent15mError("event scope drifted after search freeze")
    private_scope = _load_private_scope(source, scope)
    events = _deduplicate_events(private_scope["development_events"])
    calendar = list(contract["development_dates"])
    full_calendar = load_calendar(CALENDAR_PATH)
    positions = {day: index for index, day in enumerate(full_calendar)}
    feature_cache: dict[tuple[str, str], tuple[dict[str, Any], float] | None] = {}
    event_metadata_by_date: dict[str, list[dict[str, Any]]] = {
        day: [] for day in calendar
    }
    fifteen_minute_bars: dict[str, dict[str, list[dict[str, Any]]]] = {}
    unavailable_pairs: list[dict[str, str]] = []
    for day in contract["development_signal_dates"]:
        for event in events.get(day, []):
            symbol = str(event["symbol"])
            index = positions[day]
            prior_dates = full_calendar[
                max(0, index - LOOKBACK_SESSIONS) : index
            ]
            if len(prior_dates) != LOOKBACK_SESSIONS:
                unavailable_pairs.append(
                    {
                        "date": day,
                        "symbol": symbol,
                        "reason": "insufficient_frozen_calendar_lookback",
                    }
                )
                continue
            prior: list[tuple[dict[str, Any], float]] = []
            missing = False
            for prior_day in prior_dates:
                key = (symbol, prior_day)
                if key not in feature_cache:
                    feature_cache[key] = _daily_and_opening(
                        source, symbol, prior_day
                    )
                feature = feature_cache[key]
                if feature is None:
                    missing = True
                    break
                prior.append(feature)
            bars = _current_bars(source, symbol, day)
            if missing or bars is None:
                unavailable_pairs.append(
                    {
                        "date": day,
                        "symbol": symbol,
                        "reason": "incomplete_exact_sip_input",
                    }
                )
                continue
            prior_close = float(prior[-1][0]["close"])
            median_dollar_volume = statistics.median(
                float(daily["close"]) * float(daily["volume"])
                for daily, _opening in prior
            )
            median_opening_volume = statistics.median(
                opening for _daily, opening in prior
            )
            opening = bars[0]
            opening_range = float(opening["high"]) - float(opening["low"])
            opening_close_location = (
                (float(opening["close"]) - float(opening["low"]))
                / opening_range
                if opening_range > 0
                else 0.0
            )
            event_metadata_by_date[day].append(
                {
                    "accepted_at": str(event["accepted_at"]),
                    "event_id": str(event["event_id"]),
                    "gap_fraction": float(opening["open"]) / prior_close - 1,
                    "instrument_id": str(event["instrument_id"]),
                    "opening_bullish": (
                        float(opening["close"]) > float(opening["open"])
                    ),
                    "opening_close_location": opening_close_location,
                    "opening_volume_ratio": (
                        float(opening["volume"]) / median_opening_volume
                        if median_opening_volume > 0
                        else 0.0
                    ),
                    "prior_close": prior_close,
                    "prior_median_dollar_volume": median_dollar_volume,
                    "symbol": symbol,
                }
            )
            fifteen_minute_bars.setdefault(day, {})[symbol] = bars
        event_metadata_by_date[day].sort(key=lambda row: str(row["symbol"]))
    blocked_dates = sorted(
        {row["date"] for row in unavailable_pairs}
    )
    dataset = {
        "schema_version": 1,
        "family_id": FAMILY_ID,
        "evaluation_dates": calendar,
        "signal_dates": list(contract["development_signal_dates"]),
        "event_metadata_by_date": event_metadata_by_date,
        "fifteen_minute_bars": fifteen_minute_bars,
        "blocked_dates": blocked_dates,
        "source_semantics": {
            "filing": "SEC 8-K Item 2.02 accepted by 09:25 ET",
            "provider": "alpaca",
            "feed": "sip",
            "adjustment": "raw",
            "timeframe": "15m",
            "lookback_sessions": LOOKBACK_SESSIONS,
            "missing_data": "missed_trade_no_substitute",
        },
    }
    accounting = {
        "source_event_pairs": sum(len(rows) for rows in events.values()),
        "deduplicated_complete_event_pairs": sum(
            len(rows) for rows in event_metadata_by_date.values()
        ),
        "unavailable_event_pairs": len(unavailable_pairs),
        "blocked_dates": blocked_dates,
        "development_signal_dates": len(contract["development_signal_dates"]),
        "provider_requests": 0,
        "cache_hits": len(feature_cache),
        "confirmation_files_opened": 0,
    }
    runtime.prepare_dataset(dataset)
    return dataset, accounting


def publish_development(
    *,
    search_path: Path,
    registered_at: str,
    root: Path = DEFAULT_ROOT,
    store: HistoricalDayStore | None = None,
    enforce_commit: bool = True,
) -> tuple[Path, dict[str, Any], dict[str, Any]]:
    registered = _timestamp(registered_at, "registered_at")
    source = store or HistoricalDayStore.from_env()
    dataset, accounting = build_development_dataset(
        search_path=search_path,
        store=source,
        enforce_commit=enforce_commit,
    )
    search = strategy_discovery.load_artifact(
        search_path,
        expected_kind="frozen-development-search",
    )
    external = (
        source.root
        / "_derived"
        / "sec_earnings_event_15m"
        / FAMILY_ID
        / f"development-{search['artifact_sha256']}.json.gz"
    )
    _write_gzip(external, dataset)
    relative = str(external.resolve().relative_to(source.root.resolve()))
    manifest_path, manifest = freeze_dataset_contract(
        {
            "schema_version": 1,
            "dataset_id": (
                f"dataset-{FAMILY_ID}-development-"
                f"{search['artifact_sha256'][:16]}"
            ),
            "registered_at": registered,
            "requested_dates": dataset["evaluation_dates"],
            "dataset_payload": {
                "lane": "development",
                "claim_scope": "DEVELOPMENT_ONLY",
                "inspected": True,
                "point_in_time_evidence": True,
                "development_search_sha256": search["artifact_sha256"],
                "evidence_paths": [
                    _repo_path(search_path),
                    str(search["preflight_path"]),
                    str(search["family_contract"]["event_scope_path"]),
                    _repo_path(CAPACITY_INSPECTION),
                    _repo_path(CAPACITY_COLLECTION),
                    _repo_path(CALENDAR_PATH),
                ],
                "dense_runtime": {
                    "family_id": FAMILY_ID,
                    "format": "json.gz",
                    "external_relative_path": relative,
                    "external_file_sha256": sha256_file(external),
                    "dataset_sha256": canonical_sha256(dataset),
                    "formal_capacity": accounting[
                        "development_signal_dates"
                    ],
                    "provider_requests": 0,
                    "confirmation_files_opened": 0,
                },
                "collection_accounting": accounting,
            },
        },
        root / FAMILY_ID / "development-dataset",
    )
    return manifest_path, manifest, accounting


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    sub = parser.add_subparsers(dest="command", required=True)
    freeze = sub.add_parser("freeze-family")
    freeze.add_argument("--created-at", required=True)
    publish = sub.add_parser("publish-development")
    publish.add_argument("--search", type=Path, required=True)
    publish.add_argument("--registered-at", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "freeze-family":
            path, contract, capacity, scope = freeze_family(
                created_at=args.created_at,
                root=args.root,
            )
            result: Mapping[str, Any] = {
                "path": _repo_path(path),
                "capacity_manifest": _repo_path(capacity),
                "event_scope": _repo_path(scope),
                "trial_count": len(contract["trial_family"]),
                "development_signal_dates": len(
                    contract["development_signal_dates"]
                ),
                "confirmation_signal_capacity": contract[
                    "confirmation_signal_capacity"
                ],
                "calendar_wait_required": False,
            }
        else:
            path, manifest, accounting = publish_development(
                search_path=args.search,
                registered_at=args.registered_at,
                root=args.root,
            )
            result = {
                "path": _repo_path(path),
                "manifest_sha256": manifest["manifest_sha256"],
                "accounting": accounting,
            }
    except (
        SecEarningsEvent15mError,
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

"""Collect and evaluate the frozen ETF trend capacity contract without returns."""

from __future__ import annotations

import argparse
import gzip
import json
import math
import os
import sys
from collections.abc import Mapping, Sequence
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import multi_asset_etf_tsmom_capacity as contract
import sector_etf_rotation_stage0 as daily
from historical_concurrency import ordered_bounded_results
from historical_service import RecordingHistoricalClient
from historical_store import HistoricalDayStore, sha256_file
from ibkr_historical import IBKRConfig, IBKRHistoricalClient


PROJECT_ROOT = Path(__file__).resolve().parent
DATA_ROOT = Path("/Users/ensomniac/trade/historical_data")
CONTRACT_PATH = (
    PROJECT_ROOT
    / "strategy_tournament/v2/multi_asset_etf_tsmom/manifests/"
    "multi-asset-etf-tsmom-v1-"
    "a77cc1a26a926b57657c3226f311914c4f0b4b31e94dc8f3c50a0604fa16e063.json"
)
CONTRACT_INSPECTION_PATH = contract.DEFAULT_STATUS
COLLECTION_STATUS_PATH = (
    PROJECT_ROOT
    / "strategy_tournament/v2/multi_asset_etf_tsmom/collection-status.json"
)
PRIVATE_INPUT_PATH = (
    DATA_ROOT
    / "_derived/multi_asset_etf_tsmom_capacity/"
    "dataset-multi-asset-etf-tsmom-capacity-2026-07-22-v1/frozen-inputs.json.gz"
)
PRIVATE_CAPACITY_PATH = (
    DATA_ROOT
    / "_derived/multi_asset_etf_tsmom_capacity/"
    "dataset-multi-asset-etf-tsmom-capacity-2026-07-22-v1/"
    "capacity-selection.json.gz"
)
ACTIVATION_ROOT = (
    PROJECT_ROOT / "strategy_tournament/v2/multi_asset_etf_tsmom/activations"
)
INSPECTION_ROOT = (
    PROJECT_ROOT / "strategy_tournament/v2/multi_asset_etf_tsmom/inspections"
)
RESULT_ROOT = PROJECT_ROOT / "research_results"
RUN_INSPECTOR = PROJECT_ROOT / "multi_asset_etf_tsmom_capacity_run_inspection.py"
SCHEMA_VERSION = 1
DATASET_ID = "dataset-multi-asset-etf-tsmom-capacity-2026-07-22-v1"
MINIMUM_COMMON_SESSIONS = 1000


class MultiAssetEtfTsmomCapacityRunError(RuntimeError):
    """Frozen capacity inputs or a causal result are incomplete or inconsistent."""


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MultiAssetEtfTsmomCapacityRunError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise MultiAssetEtfTsmomCapacityRunError(f"{path} must contain an object")
    return value


def _write_json(value: Mapping[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _read_gzip_json(path: Path) -> dict[str, Any]:
    try:
        with gzip.open(path, "rt", encoding="utf-8") as source:
            value = json.load(source)
    except (OSError, json.JSONDecodeError) as exc:
        raise MultiAssetEtfTsmomCapacityRunError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise MultiAssetEtfTsmomCapacityRunError(f"{path} must contain an object")
    return value


def _write_gzip_json(value: Mapping[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + f".{os.getpid()}.tmp")
    payload = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    try:
        with temporary.open("wb") as raw:
            with gzip.GzipFile(fileobj=raw, mode="wb", mtime=0) as stream:
                stream.write(payload)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _load_contract_and_inspection() -> tuple[dict[str, Any], dict[str, Any]]:
    frozen = contract.load_contract(CONTRACT_PATH)
    inspected = _read_json(CONTRACT_INSPECTION_PATH)
    if not (
        frozen == contract.build_contract()
        and inspected.get("contract_sha256") == frozen["contract_sha256"]
        and inspected.get("inspection_sha256")
        == contract.successor._self_hash(inspected, "inspection_sha256")
        and inspected.get("status") == "CAPACITY_CONTRACT_INSPECTED"
        and inspected.get("provider_access_permitted") is True
        and inspected.get("capacity_signal_count_permitted") is True
        and inspected.get("outcome_access_permitted") is False
        and inspected.get("broker_actions_permitted") is False
        and inspected.get("valid") is True
    ):
        raise MultiAssetEtfTsmomCapacityRunError(
            "capacity contract is not inspected for exact input collection"
        )
    return frozen, inspected


def _dataset(
    store: HistoricalDayStore, symbol: str, day: str
) -> dict[str, Any] | None:
    value = store.select_dataset(
        symbol,
        day,
        kind="bars",
        channel="trades",
        timeframe="1d",
        providers=("ibkr",),
        require_complete=True,
        feed="smart",
        adjustment="provider_adjusted_unknown_basis",
    )
    if value is None or value.get("quality", {}).get("row_count") != 1:
        return None
    return value


def _symbol_dates(store: HistoricalDayStore, symbol: str) -> list[str]:
    return [
        day
        for day in store.dates(symbol)
        if contract.COLLECTION_START <= day < contract.COLLECTION_END_EXCLUSIVE
        and _dataset(store, symbol, day) is not None
    ]


def local_inventory(store: HistoricalDayStore | None = None) -> dict[str, Any]:
    """Inspect only data identities and completeness; never read price outcomes."""

    _load_contract_and_inspection()
    target = store or HistoricalDayStore.from_env()
    dates_by_symbol = {
        symbol: _symbol_dates(target, symbol) for symbol, _asset_class in contract.UNIVERSE
    }
    common = set(dates_by_symbol[next(iter(dates_by_symbol))])
    for dates in dates_by_symbol.values():
        common &= set(dates)
    common_dates = sorted(common)
    symbol_counts = {symbol: len(dates) for symbol, dates in dates_by_symbol.items()}
    valid = (
        len(common_dates) >= MINIMUM_COMMON_SESSIONS
        and bool(common_dates)
        and common_dates[0] <= "2021-12-03"
        and common_dates[-1] >= "2026-01-02"
        and all(count >= MINIMUM_COMMON_SESSIONS for count in symbol_counts.values())
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "inventory_kind": "outcome-blind-daily-input-inventory",
        "candidate_id": contract.CANDIDATE_ID,
        "contract_sha256": contract._read_json(CONTRACT_PATH)["contract_sha256"],
        "symbols": list(symbol_counts),
        "symbol_complete_session_counts": symbol_counts,
        "qualified_common_sessions": len(common_dates),
        "first_common_session": common_dates[0] if common_dates else None,
        "last_common_session": common_dates[-1] if common_dates else None,
        "minimum_common_sessions": MINIMUM_COMMON_SESSIONS,
        "price_values_read": 0,
        "signal_count": None,
        "returns_computed": 0,
        "market_outcomes_accessed": False,
        "provider_requests": 0,
        "broker_actions": 0,
        "valid": valid,
    }


def collect_inputs(store: HistoricalDayStore | None = None) -> dict[str, Any]:
    """Use existing complete inputs, or collect only the frozen daily request graph."""

    _load_contract_and_inspection()
    target = store or HistoricalDayStore.from_env()
    before = local_inventory(target)
    completed: list[dict[str, Any]] = []
    failures: list[dict[str, str]] = []
    telemetry: dict[str, Any] = {"requests": 0, "skipped_existing_complete": True}
    if not before["valid"]:
        config = IBKRConfig.from_env()
        with IBKRHistoricalClient(config) as raw_client:
            recorder = RecordingHistoricalClient(raw_client, target)

            def collect(symbol: str) -> int:
                rows = recorder.fetch_bars(
                    symbol,
                    f"{contract.COLLECTION_START}T00:00:00-05:00",
                    f"{contract.COLLECTION_END_EXCLUSIVE}T00:00:00-05:00",
                    bar_size="1 day",
                    what="TRADES",
                    use_rth=True,
                )
                return len(rows)

            for outcome in ordered_bounded_results(
                [item[0] for item in contract.UNIVERSE],
                collect,
                max_workers=config.max_concurrent_requests,
            ):
                if outcome.error is not None:
                    failures.append(
                        {"symbol": outcome.item, "error": str(outcome.error)}
                    )
                else:
                    completed.append(
                        {"symbol": outcome.item, "rows": int(outcome.value or 0)}
                    )
            telemetry = raw_client.request_telemetry()
    after = local_inventory(target)
    status = {
        **after,
        "inventory_kind": "outcome-blind-daily-input-collection",
        "local_inventory_valid_before_collection": before["valid"],
        "completed": completed,
        "failures": failures,
        "provider_telemetry": telemetry,
        "provider_requests": int(telemetry.get("requests", len(completed))),
        "valid": after["valid"] and not failures,
    }
    _write_json(status, COLLECTION_STATUS_PATH)
    if not status["valid"]:
        raise MultiAssetEtfTsmomCapacityRunError(
            "frozen daily input collection is incomplete"
        )
    return status


def _validate_close(raw: Any, symbol: str, day: str) -> dict[str, Any]:
    if not isinstance(raw, Mapping):
        raise MultiAssetEtfTsmomCapacityRunError(f"{symbol} {day} row is malformed")
    try:
        close = float(raw["c"])
        observed = datetime.fromisoformat(str(raw["t"]))
    except (KeyError, TypeError, ValueError) as exc:
        raise MultiAssetEtfTsmomCapacityRunError(
            f"{symbol} {day} close fields are invalid"
        ) from exc
    if (
        not math.isfinite(close)
        or close <= 0
        or observed.tzinfo is None
        or observed.date().isoformat() != day
        or raw.get("i") is not False
    ):
        raise MultiAssetEtfTsmomCapacityRunError(
            f"{symbol} {day} close is not a complete causal observation"
        )
    return {"t": str(raw["t"]), "c": close}


def _weekly_schedule(dates: Sequence[str]) -> list[dict[str, Any]]:
    parsed_dates = [date.fromisoformat(day) for day in dates]
    positions = {day: index for index, day in enumerate(parsed_dates)}
    anchor = date.fromisoformat(contract.EVALUATION_WEEK_START)
    last = date.fromisoformat(contract.EVALUATION_WEEK_END)
    universe = [item[0] for item in contract.UNIVERSE]
    schedule: list[dict[str, Any]] = []
    week_monday = anchor
    week_ordinal = 0
    while week_monday <= last:
        week_sessions = [
            day
            for day in parsed_dates
            if week_monday <= day < week_monday + timedelta(days=7)
        ]
        scheduled_symbol = universe[week_ordinal % len(universe)]
        row: dict[str, Any] = {
            "week_monday": week_monday.isoformat(),
            "week_ordinal": week_ordinal,
            "scheduled_symbol": scheduled_symbol,
            "opportunity_session": None,
            "decision_session": None,
            "lookback_session": None,
            "eligible": False,
            "ineligible_reason": None,
        }
        if not week_sessions:
            row["ineligible_reason"] = "missing_common_opportunity_session"
        else:
            opportunity = week_sessions[0]
            opportunity_index = positions[opportunity]
            decision_index = opportunity_index - 1
            lookback_index = decision_index - contract.LOOKBACK_SESSIONS
            row["opportunity_session"] = opportunity.isoformat()
            if decision_index < 0:
                row["ineligible_reason"] = "missing_decision_session"
            elif lookback_index < 0:
                row["decision_session"] = parsed_dates[decision_index].isoformat()
                row["ineligible_reason"] = "missing_exact_252_session_lookback"
            else:
                row["decision_session"] = parsed_dates[decision_index].isoformat()
                row["lookback_session"] = parsed_dates[lookback_index].isoformat()
                row["eligible"] = True
        schedule.append(row)
        week_monday += timedelta(days=7)
        week_ordinal += 1
    return schedule


def _build_input_graph(store: HistoricalDayStore) -> dict[str, Any]:
    status = _read_json(COLLECTION_STATUS_PATH)
    if not (
        status.get("contract_sha256")
        == contract._read_json(CONTRACT_PATH)["contract_sha256"]
        and status.get("valid") is True
        and status.get("signal_count") is None
        and status.get("returns_computed") == 0
        and status.get("market_outcomes_accessed") is False
        and status.get("broker_actions") == 0
    ):
        raise MultiAssetEtfTsmomCapacityRunError("collection status is invalid")
    symbols = [item[0] for item in contract.UNIVERSE]
    common = set(_symbol_dates(store, symbols[0]))
    for symbol in symbols[1:]:
        common &= set(_symbol_dates(store, symbol))
    dates = sorted(common)
    if len(dates) < MINIMUM_COMMON_SESSIONS:
        raise MultiAssetEtfTsmomCapacityRunError("common daily corpus is incomplete")
    closes_by_symbol: dict[str, list[dict[str, Any]]] = {
        symbol: [] for symbol in symbols
    }
    dataset_ids_by_symbol: dict[str, list[str]] = {symbol: [] for symbol in symbols}
    for day in dates:
        for symbol in symbols:
            dataset = _dataset(store, symbol, day)
            if dataset is None:
                raise MultiAssetEtfTsmomCapacityRunError(
                    f"{symbol} {day} disappeared from common corpus"
                )
            closes_by_symbol[symbol].append(
                _validate_close(dataset["rows"][0], symbol, day)
            )
            dataset_ids_by_symbol[symbol].append(str(dataset["id"]))
    schedule = _weekly_schedule(dates)
    if any(not row["eligible"] for row in schedule):
        reasons = sorted(
            {str(row["ineligible_reason"]) for row in schedule if not row["eligible"]}
        )
        raise MultiAssetEtfTsmomCapacityRunError(
            f"frozen weekly schedule lacks causal inputs: {reasons}"
        )
    graph: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "dataset_id": DATASET_ID,
        "candidate_id": contract.CANDIDATE_ID,
        "contract_sha256": contract._read_json(CONTRACT_PATH)["contract_sha256"],
        "dates": dates,
        "closes_by_symbol": closes_by_symbol,
        "dataset_ids_by_symbol": dataset_ids_by_symbol,
        "schedule_without_signals": schedule,
        "provider": "ibkr",
        "timeframe": "1d",
        "feed": "smart",
        "adjustment": "provider_adjusted_unknown_basis",
        "permitted_fields": ["timestamp", "close"],
        "signal_count": None,
        "returns_computed": 0,
        "market_outcomes_accessed": False,
        "broker_actions": 0,
    }
    graph["input_sha256"] = contract.successor._self_hash(graph, "input_sha256")
    return graph


def build_activation(
    store: HistoricalDayStore | None = None, *, write_private: bool = False
) -> dict[str, Any]:
    frozen, inspected = _load_contract_and_inspection()
    target = store or HistoricalDayStore.from_env()
    graph = _build_input_graph(target)
    if write_private:
        _write_gzip_json(graph, PRIVATE_INPUT_PATH)
    if not PRIVATE_INPUT_PATH.is_file():
        raise MultiAssetEtfTsmomCapacityRunError("private frozen inputs are missing")
    if _read_gzip_json(PRIVATE_INPUT_PATH) != graph:
        raise MultiAssetEtfTsmomCapacityRunError("private frozen inputs do not rebuild")
    collection = _read_json(COLLECTION_STATUS_PATH)
    activation: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "artifact_kind": "outcome-blind-capacity-input-activation",
        "campaign_id": contract.CAMPAIGN_ID,
        "candidate_id": contract.CANDIDATE_ID,
        "strategy_version": contract.STRATEGY_VERSION,
        "mechanism_family": contract.THEME_ID,
        "contract_sha256": frozen["contract_sha256"],
        "contract_inspection_sha256": inspected["inspection_sha256"],
        "implementation_sha256": sha256_file(Path(__file__).resolve()),
        "inspector_sha256": sha256_file(RUN_INSPECTOR),
        "source_selection": {
            "dataset_id": DATASET_ID,
            "symbols": [item[0] for item in contract.UNIVERSE],
            "collection_start": contract.COLLECTION_START,
            "collection_end_exclusive": contract.COLLECTION_END_EXCLUSIVE,
            "common_sessions": len(graph["dates"]),
            "scheduled_weeks": len(graph["schedule_without_signals"]),
            "input_symbol_sessions": len(graph["dates"]) * len(contract.UNIVERSE),
            "input_sha256": graph["input_sha256"],
            "private_input_file_sha256": sha256_file(PRIVATE_INPUT_PATH),
            "provider_requests": int(collection["provider_requests"]),
            "permitted_fields": ["timestamp", "close"],
        },
        "access_contract": {
            "capacity_evaluation_before_inspection_permitted": False,
            "entry_fill_access_permitted": False,
            "exit_or_stop_access_permitted": False,
            "forward_return_computation_permitted": False,
            "stage0_outcome_access_permitted": False,
            "broker_actions_permitted": False,
            "maturity_effect": "NONE",
        },
        "denominator": {
            "scheduled_weeks": len(graph["schedule_without_signals"]),
            "eligible_causal_evaluations": len(graph["schedule_without_signals"]),
            "signal_count": None,
            "preserve_every_scheduled_week": True,
        },
        "returns_computed": 0,
        "market_outcomes_accessed": False,
        "claim_limit": (
            "This activation freezes only timestamp and close inputs. It contains "
            "no signal count or performance outcome."
        ),
    }
    activation["activation_sha256"] = contract.successor._self_hash(
        activation, "activation_sha256"
    )
    return activation


def activation_path(value: Mapping[str, Any]) -> Path:
    return ACTIVATION_ROOT / f"{contract.CANDIDATE_ID}-{value['activation_sha256']}.json"


def inspect_inputs(path: Path, store: HistoricalDayStore | None = None) -> dict[str, Any]:
    recorded = _read_json(path)
    if recorded.get("activation_sha256") != contract.successor._self_hash(
        recorded, "activation_sha256"
    ):
        raise MultiAssetEtfTsmomCapacityRunError("input activation hash is invalid")
    expected = build_activation(store, write_private=False)
    if recorded != expected:
        raise MultiAssetEtfTsmomCapacityRunError("input activation does not rebuild")
    graph = _read_gzip_json(PRIVATE_INPUT_PATH)
    if graph.get("input_sha256") != contract.successor._self_hash(
        graph, "input_sha256"
    ):
        raise MultiAssetEtfTsmomCapacityRunError("private input hash is invalid")
    result: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "inspection_kind": "outcome-blind-capacity-input-inspection",
        "candidate_id": contract.CANDIDATE_ID,
        "contract_sha256": recorded["contract_sha256"],
        "activation_sha256": recorded["activation_sha256"],
        "activation_file_sha256": sha256_file(path),
        "input_sha256": graph["input_sha256"],
        "private_input_file_sha256": sha256_file(PRIVATE_INPUT_PATH),
        "common_sessions": len(graph["dates"]),
        "scheduled_weeks": len(graph["schedule_without_signals"]),
        "eligible_causal_evaluations": sum(
            bool(row["eligible"]) for row in graph["schedule_without_signals"]
        ),
        "capacity_evaluation_permitted": True,
        "stage0_outcome_access_permitted": False,
        "signal_count": None,
        "returns_computed": 0,
        "market_outcomes_accessed": False,
        "broker_actions": 0,
        "maturity_effect": "NONE",
        "valid": True,
    }
    result["inspection_sha256"] = contract.successor._self_hash(
        result, "inspection_sha256"
    )
    return result


def _capacity_state(graph: Mapping[str, Any]) -> dict[str, Any]:
    dates = [str(day) for day in graph["dates"]]
    positions = {day: index for index, day in enumerate(dates)}
    closes = {
        symbol: [float(row["c"]) for row in rows]
        for symbol, rows in graph["closes_by_symbol"].items()
    }
    records: list[dict[str, Any]] = []
    for scheduled in graph["schedule_without_signals"]:
        row = dict(scheduled)
        if not row["eligible"]:
            row.update({"momentum_positive": None, "signal": False})
        else:
            symbol = str(row["scheduled_symbol"])
            decision_index = positions[str(row["decision_session"])]
            lookback_index = positions[str(row["lookback_session"])]
            if decision_index - lookback_index != contract.LOOKBACK_SESSIONS:
                raise MultiAssetEtfTsmomCapacityRunError(
                    "capacity lookback distance drifted"
                )
            momentum_positive = (
                closes[symbol][decision_index] / closes[symbol][lookback_index] - 1
            ) > 0
            row.update(
                {"momentum_positive": momentum_positive, "signal": momentum_positive}
            )
        records.append(row)
    signals = sum(bool(row["signal"]) for row in records)
    eligible = sum(bool(row["eligible"]) for row in records)
    required = contract.MINIMUM_TOTAL_HISTORICAL_SIGNALS
    state: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "dataset_id": DATASET_ID,
        "candidate_id": contract.CANDIDATE_ID,
        "contract_sha256": graph["contract_sha256"],
        "scheduled_weeks": len(records),
        "eligible_causal_evaluations": eligible,
        "ineligible_causal_evaluations": len(records) - eligible,
        "causal_long_signals": signals,
        "causal_non_signals": eligible - signals,
        "minimum_required_signals": required,
        "signal_shortfall": max(0, required - signals),
        "capacity_passed": signals >= required,
        "records": records,
        "entry_fills_accessed": False,
        "exit_or_stop_values_accessed": False,
        "forward_returns_computed": 0,
        "market_outcomes_accessed": False,
        "broker_actions": 0,
    }
    state["selection_sha256"] = contract.successor._self_hash(
        state, "selection_sha256"
    )
    return state


def build_capacity_result(
    activation: Path,
    inspection: Path,
    *,
    require_published: bool = True,
    write_private: bool = False,
) -> dict[str, Any]:
    frozen_activation = _read_json(activation)
    frozen_inspection = _read_json(inspection)
    if frozen_inspection != inspect_inputs(activation):
        raise MultiAssetEtfTsmomCapacityRunError("input inspection does not rebuild")
    if frozen_inspection.get("capacity_evaluation_permitted") is not True:
        raise MultiAssetEtfTsmomCapacityRunError("capacity evaluation is not permitted")
    if require_published:
        daily._require_published((activation, inspection))
    graph = _read_gzip_json(PRIVATE_INPUT_PATH)
    state = _capacity_state(graph)
    if write_private:
        _write_gzip_json(state, PRIVATE_CAPACITY_PATH)
    if not PRIVATE_CAPACITY_PATH.is_file() or _read_gzip_json(PRIVATE_CAPACITY_PATH) != state:
        raise MultiAssetEtfTsmomCapacityRunError(
            "private capacity selection is missing or differs"
        )
    passed = bool(state["capacity_passed"])
    result: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "result_kind": "outcome-blind-capacity-result",
        "campaign_id": contract.CAMPAIGN_ID,
        "candidate_id": contract.CANDIDATE_ID,
        "strategy_version": contract.STRATEGY_VERSION,
        "mechanism_family": contract.THEME_ID,
        "contract_sha256": frozen_activation["contract_sha256"],
        "activation_sha256": frozen_activation["activation_sha256"],
        "input_inspection_sha256": frozen_inspection["inspection_sha256"],
        "input_sha256": frozen_inspection["input_sha256"],
        "selection_sha256": state["selection_sha256"],
        "private_capacity_file_sha256": sha256_file(PRIVATE_CAPACITY_PATH),
        "denominator": {
            "scheduled_weeks": state["scheduled_weeks"],
            "eligible_causal_evaluations": state["eligible_causal_evaluations"],
            "ineligible_causal_evaluations": state[
                "ineligible_causal_evaluations"
            ],
            "causal_long_signals": state["causal_long_signals"],
            "causal_non_signals": state["causal_non_signals"],
            "minimum_required_signals": state["minimum_required_signals"],
            "signal_shortfall": state["signal_shortfall"],
        },
        "capacity_passed": passed,
        "candidate_disposition": (
            "CAPACITY_SURVIVOR_STAGE0_FREEZE_REQUIRED"
            if passed
            else "RETIRED_CAPACITY_NO_PARAMETER_REPAIR"
        ),
        "next_action": (
            "freeze the exact Stage 0 rules, fills, exits, costs, dates, and "
            "falsification gates before any outcome access"
            if passed
            else "advance to the next authorized v2 mechanism family"
        ),
        "entry_fills_accessed": False,
        "exit_or_stop_values_accessed": False,
        "forward_returns_computed": 0,
        "market_outcomes_accessed": False,
        "broker_actions": 0,
        "development_evidence_eligible": False,
        "confirmation_evidence_eligible": False,
        "maturity_effect": "NONE",
        "records_published": 0,
    }
    result["result_sha256"] = contract.successor._self_hash(
        result, "result_sha256"
    )
    return result


def inspect_capacity_result(
    activation: Path, inspection: Path, result_path: Path
) -> dict[str, Any]:
    recorded = _read_json(result_path)
    if recorded.get("result_sha256") != contract.successor._self_hash(
        recorded, "result_sha256"
    ):
        raise MultiAssetEtfTsmomCapacityRunError("capacity result hash is invalid")
    expected = build_capacity_result(
        activation, inspection, require_published=False, write_private=False
    )
    if recorded != expected:
        raise MultiAssetEtfTsmomCapacityRunError(
            "capacity result does not independently rebuild"
        )
    result: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "inspection_kind": "outcome-blind-capacity-result-inspection",
        "candidate_id": contract.CANDIDATE_ID,
        "contract_sha256": recorded["contract_sha256"],
        "activation_sha256": recorded["activation_sha256"],
        "input_inspection_sha256": recorded["input_inspection_sha256"],
        "result_sha256": recorded["result_sha256"],
        "result_file_sha256": sha256_file(result_path),
        "denominator": recorded["denominator"],
        "capacity_passed": recorded["capacity_passed"],
        "candidate_disposition": recorded["candidate_disposition"],
        "stage0_outcome_access_permitted": False,
        "forward_returns_computed": 0,
        "market_outcomes_accessed": False,
        "broker_actions": 0,
        "maturity_effect": "NONE",
        "valid": True,
    }
    result["inspection_sha256"] = contract.successor._self_hash(
        result, "inspection_sha256"
    )
    return result


def _one(pattern: str, description: str) -> Path:
    matches = sorted(PROJECT_ROOT.glob(pattern))
    if len(matches) != 1:
        raise MultiAssetEtfTsmomCapacityRunError(
            f"expected one {description}; found {len(matches)}"
        )
    return matches[0]


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "command",
        choices=(
            "inventory",
            "collect",
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
        if args.command == "inventory":
            result: dict[str, Any] = local_inventory()
        elif args.command == "collect":
            result = collect_inputs()
        elif args.command == "freeze-inputs":
            value = build_activation(write_private=True)
            path = activation_path(value)
            _write_json(value, path)
            result = {
                "activation_sha256": value["activation_sha256"],
                "written": str(path.relative_to(PROJECT_ROOT)),
                "signal_count": None,
                "outcome_access_permitted": False,
            }
        elif args.command == "inspect-inputs":
            activation = _one(
                "strategy_tournament/v2/multi_asset_etf_tsmom/activations/"
                "multi-asset-etf-tsmom-v1-*.json",
                "capacity input activation",
            )
            value = inspect_inputs(activation)
            path = INSPECTION_ROOT / (
                f"{contract.CANDIDATE_ID}-input-{value['inspection_sha256']}.json"
            )
            _write_json(value, path)
            result = {
                "inspection_sha256": value["inspection_sha256"],
                "written": str(path.relative_to(PROJECT_ROOT)),
                "signal_count": None,
                "outcome_access_permitted": False,
            }
        elif args.command == "evaluate":
            activation = _one(
                "strategy_tournament/v2/multi_asset_etf_tsmom/activations/"
                "multi-asset-etf-tsmom-v1-*.json",
                "capacity input activation",
            )
            inspection = _one(
                "strategy_tournament/v2/multi_asset_etf_tsmom/inspections/"
                "multi-asset-etf-tsmom-v1-input-*.json",
                "capacity input inspection",
            )
            value = build_capacity_result(
                activation, inspection, require_published=True, write_private=True
            )
            path = RESULT_ROOT / (
                f"2026-07-22-{contract.CANDIDATE_ID}-capacity-"
                f"{value['result_sha256']}.json"
            )
            _write_json(value, path)
            result = {
                "result_sha256": value["result_sha256"],
                "capacity_passed": value["capacity_passed"],
                "causal_long_signals": value["denominator"]["causal_long_signals"],
                "written": str(path.relative_to(PROJECT_ROOT)),
                "outcome_access_permitted": False,
            }
        else:
            activation = _one(
                "strategy_tournament/v2/multi_asset_etf_tsmom/activations/"
                "multi-asset-etf-tsmom-v1-*.json",
                "capacity input activation",
            )
            inspection = _one(
                "strategy_tournament/v2/multi_asset_etf_tsmom/inspections/"
                "multi-asset-etf-tsmom-v1-input-*.json",
                "capacity input inspection",
            )
            result_path = _one(
                "research_results/2026-07-22-multi-asset-etf-tsmom-v1-capacity-*.json",
                "capacity result",
            )
            value = inspect_capacity_result(activation, inspection, result_path)
            path = INSPECTION_ROOT / (
                f"{contract.CANDIDATE_ID}-result-{value['inspection_sha256']}.json"
            )
            _write_json(value, path)
            result = {
                "inspection_sha256": value["inspection_sha256"],
                "capacity_passed": value["capacity_passed"],
                "written": str(path.relative_to(PROJECT_ROOT)),
                "outcome_access_permitted": False,
            }
    except (
        MultiAssetEtfTsmomCapacityRunError,
        contract.MultiAssetEtfTsmomCapacityError,
        OSError,
        ValueError,
    ) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}), file=sys.stderr)
        return 2
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""Freeze, inspect, and evaluate the v2 ETF trend Stage 0 candidate."""

from __future__ import annotations

import argparse
import json
import math
import sys
from collections.abc import Mapping, Sequence
from datetime import datetime
from pathlib import Path
from typing import Any

import etf_or_momentum_stage0 as common
import multi_asset_etf_tsmom_capacity as capacity_contract
import multi_asset_etf_tsmom_capacity_run as capacity_run
import sector_etf_rotation_stage0 as daily
from historical_store import HistoricalDayStore, sha256_file


PROJECT_ROOT = Path(__file__).resolve().parent
DATA_ROOT = Path("/Users/ensomniac/trade/historical_data")
CAPACITY_RESULT_PATH = (
    PROJECT_ROOT
    / "research_results/"
    "2026-07-22-multi-asset-etf-tsmom-v1-capacity-"
    "653e7eaa8fabcd18c6e196b399ac458f2db488df068de31c06e28455443d81b6.json"
)
CAPACITY_RESULT_INSPECTION_PATH = (
    PROJECT_ROOT
    / "strategy_tournament/v2/multi_asset_etf_tsmom/inspections/"
    "multi-asset-etf-tsmom-v1-result-"
    "38cbd1d878256990fb260471ef3d833d41a840302ddd787ba0545a315f9185c0.json"
)
PRIVATE_PARTITION_PATH = (
    DATA_ROOT
    / "_derived/multi_asset_etf_tsmom_stage0/"
    "dataset-multi-asset-etf-tsmom-stage0-2026-07-22-v1/partition.json.gz"
)
PRIVATE_INPUT_PATH = (
    DATA_ROOT
    / "_derived/multi_asset_etf_tsmom_stage0/"
    "dataset-multi-asset-etf-tsmom-stage0-2026-07-22-v1/frozen-inputs.json.gz"
)
CONTRACT_ROOT = PROJECT_ROOT / "strategy_tournament/v2/multi_asset_etf_tsmom/stage0/contracts"
ACTIVATION_ROOT = PROJECT_ROOT / "strategy_tournament/v2/multi_asset_etf_tsmom/stage0/activations"
INSPECTION_ROOT = PROJECT_ROOT / "strategy_tournament/v2/multi_asset_etf_tsmom/stage0/inspections"
RESULT_ROOT = PROJECT_ROOT / "research_results"
INSPECTOR_PATH = PROJECT_ROOT / "multi_asset_etf_tsmom_stage0_inspection.py"
SCHEMA_VERSION = 1
VARIANT_ID = "multi-asset-etf-tsmom-v1"
STRATEGY_VERSION = capacity_contract.STRATEGY_VERSION
STAGE0_SIGNAL_COUNT = 40
CONFIRMATION_SIGNAL_COUNT = 20
CONFIRMATION_EMBARGO_SESSIONS = 5
ATR_LENGTH = 14
STOP_ATR_MULTIPLE = 1.5
MAXIMUM_HOLDING_SESSIONS = 5
PRIMARY_COST_BPS = 5
STRESS_COST_BPS = (10, 20)


class MultiAssetEtfTsmomStage0Error(RuntimeError):
    """The exact Stage 0 contract, inputs, or result are inconsistent."""


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise MultiAssetEtfTsmomStage0Error(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise MultiAssetEtfTsmomStage0Error(f"{path} must contain an object")
    return value


def _write_json(value: Mapping[str, Any], path: Path) -> None:
    capacity_run._write_json(value, path)


def _load_capacity_evidence(*, require_published: bool) -> tuple[dict[str, Any], dict[str, Any]]:
    result = _read_json(CAPACITY_RESULT_PATH)
    inspection = _read_json(CAPACITY_RESULT_INSPECTION_PATH)
    if not (
        result.get("result_sha256")
        == capacity_contract.successor._self_hash(result, "result_sha256")
        and result.get("capacity_passed") is True
        and result.get("candidate_disposition")
        == "CAPACITY_SURVIVOR_STAGE0_FREEZE_REQUIRED"
        and result.get("forward_returns_computed") == 0
        and result.get("market_outcomes_accessed") is False
        and inspection
        == capacity_run.inspect_capacity_result(
            capacity_run._one(
                "strategy_tournament/v2/multi_asset_etf_tsmom/activations/"
                "multi-asset-etf-tsmom-v1-*.json",
                "capacity input activation",
            ),
            capacity_run._one(
                "strategy_tournament/v2/multi_asset_etf_tsmom/inspections/"
                "multi-asset-etf-tsmom-v1-input-*.json",
                "capacity input inspection",
            ),
            CAPACITY_RESULT_PATH,
        )
        and inspection.get("valid") is True
        and inspection.get("stage0_outcome_access_permitted") is False
    ):
        raise MultiAssetEtfTsmomStage0Error("inspected capacity survivor is invalid")
    if require_published:
        daily._require_published((CAPACITY_RESULT_PATH, CAPACITY_RESULT_INSPECTION_PATH))
    return result, inspection


def _partition(capacity_state: Mapping[str, Any], dates: Sequence[str]) -> dict[str, Any]:
    signal_records = [
        {
            "week_monday": str(row["week_monday"]),
            "week_ordinal": int(row["week_ordinal"]),
            "symbol": str(row["scheduled_symbol"]),
            "lookback_session": str(row["lookback_session"]),
            "decision_session": str(row["decision_session"]),
            "opportunity_session": str(row["opportunity_session"]),
        }
        for row in capacity_state["records"]
        if row.get("signal") is True
    ]
    if len(signal_records) < STAGE0_SIGNAL_COUNT + CONFIRMATION_SIGNAL_COUNT:
        raise MultiAssetEtfTsmomStage0Error("capacity survivor cannot support partitions")
    positions = {str(day): index for index, day in enumerate(dates)}
    stage0 = signal_records[:STAGE0_SIGNAL_COUNT]
    confirmation = signal_records[-CONFIRMATION_SIGNAL_COUNT:]
    confirmation_first_index = positions[confirmation[0]["opportunity_session"]]
    development = [
        row
        for row in signal_records[STAGE0_SIGNAL_COUNT:-CONFIRMATION_SIGNAL_COUNT]
        if positions[row["opportunity_session"]]
        < confirmation_first_index - CONFIRMATION_EMBARGO_SESSIONS
    ]
    embargo = [
        row
        for row in signal_records[STAGE0_SIGNAL_COUNT:-CONFIRMATION_SIGNAL_COUNT]
        if row not in development
    ]
    if len(development) < 30:
        raise MultiAssetEtfTsmomStage0Error(
            "development reserve is below 30 signals after embargo"
        )
    partitions = {
        "stage0": stage0,
        "development": development,
        "embargo_excluded": embargo,
        "confirmation": confirmation,
    }
    flattened = [
        {"phase": phase, **row}
        for phase, rows in partitions.items()
        for row in rows
    ]
    value: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "dataset_id": "dataset-multi-asset-etf-tsmom-stage0-2026-07-22-v1",
        "variant_id": VARIANT_ID,
        "capacity_selection_sha256": capacity_state["selection_sha256"],
        "capacity_signal_count": len(signal_records),
        "stage0_signal_count": len(stage0),
        "development_signal_count": len(development),
        "embargo_excluded_signal_count": len(embargo),
        "confirmation_signal_count": len(confirmation),
        "confirmation_embargo_sessions": CONFIRMATION_EMBARGO_SESSIONS,
        "partitions": partitions,
        "partitioned_records": flattened,
        "market_outcomes_accessed": False,
        "returns_computed": 0,
        "broker_actions": 0,
    }
    value["partition_sha256"] = capacity_contract.successor._self_hash(
        value, "partition_sha256"
    )
    return value


def _build_partition(*, write_private: bool) -> dict[str, Any]:
    state = capacity_run._read_gzip_json(capacity_run.PRIVATE_CAPACITY_PATH)
    graph = capacity_run._read_gzip_json(capacity_run.PRIVATE_INPUT_PATH)
    if not (
        state.get("selection_sha256")
        == capacity_contract.successor._self_hash(state, "selection_sha256")
        and state.get("capacity_passed") is True
        and state.get("market_outcomes_accessed") is False
        and graph.get("input_sha256")
        == capacity_contract.successor._self_hash(graph, "input_sha256")
    ):
        raise MultiAssetEtfTsmomStage0Error("private capacity evidence is invalid")
    value = _partition(state, list(graph["dates"]))
    if write_private:
        capacity_run._write_gzip_json(value, PRIVATE_PARTITION_PATH)
    if not PRIVATE_PARTITION_PATH.is_file():
        raise MultiAssetEtfTsmomStage0Error("private partition is missing")
    if capacity_run._read_gzip_json(PRIVATE_PARTITION_PATH) != value:
        raise MultiAssetEtfTsmomStage0Error("private partition does not rebuild")
    return value


def build_contract(*, write_private: bool = False) -> dict[str, Any]:
    capacity_result, capacity_inspection = _load_capacity_evidence(
        require_published=True
    )
    partition = _build_partition(write_private=write_private)
    selection_contract = {
        "universe": [item[0] for item in capacity_contract.UNIVERSE],
        "weekly_symbol_schedule": (
            "whole ISO weeks since 2023-01-02 modulo the frozen universe order"
        ),
        "signal": "scheduled ETF own 252-common-session close return strictly above zero",
        "cross_sectional_ranking": False,
        "stage0_selection": "first 40 chronological causal long signals",
        "development_reserve": (
            "all later signals before the confirmation embargo after Stage 0 selection"
        ),
        "confirmation_reserve": "last 20 chronological causal long signals",
        "confirmation_embargo_sessions": CONFIRMATION_EMBARGO_SESSIONS,
        "parameter_repair_on_this_corpus_permitted": False,
    }
    execution_contract = {
        "direction": "long",
        "entry": "scheduled opportunity session open after completed decision close",
        "entry_order_type_model": "marketable limit filled at observed open plus costs",
        "maximum_new_entries_per_day": 1,
        "maximum_new_entries_per_week": 1,
        "maximum_concurrent_positions": 2,
        "maximum_holding_sessions": MAXIMUM_HOLDING_SESSIONS,
        "entry_fill_miss_policy": "missing or invalid observed open is a violation and no substitution",
    }
    exit_contract = {
        "atr_length_complete_sessions": ATR_LENGTH,
        "stop_distance": f"{STOP_ATR_MULTIPLE} times decision-session ATR14",
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
    value: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "artifact_kind": "stage0-zero-result-contract",
        "campaign_id": capacity_contract.CAMPAIGN_ID,
        "variant_id": VARIANT_ID,
        "strategy_version": STRATEGY_VERSION,
        "mechanism_family": capacity_contract.THEME_ID,
        "capacity_result_sha256": capacity_result["result_sha256"],
        "capacity_inspection_sha256": capacity_inspection["inspection_sha256"],
        "capacity_signal_count": capacity_result["denominator"][
            "causal_long_signals"
        ],
        "partition_sha256": partition["partition_sha256"],
        "private_partition_file_sha256": sha256_file(PRIVATE_PARTITION_PATH),
        "partition_denominator": {
            "stage0_signals": partition["stage0_signal_count"],
            "development_reserved_signals": partition["development_signal_count"],
            "embargo_excluded_signals": partition["embargo_excluded_signal_count"],
            "confirmation_reserved_signals": partition["confirmation_signal_count"],
        },
        "selection_contract": selection_contract,
        "execution_contract": execution_contract,
        "exit_contract": exit_contract,
        "cost_contract": {
            "bps_per_side": [PRIMARY_COST_BPS, *STRESS_COST_BPS],
            "entry_cost": "raw entry multiplied by one plus bps",
            "exit_cost": "raw exit multiplied by one minus bps",
            "planned_risk": "cost-adjusted entry minus cost-adjusted planned stop",
        },
        "stage0_gate": gate,
        "access_contract": {
            "market_outcome_input_access_before_inspection_permitted": False,
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
            "cost_contract": value["cost_contract"],
            "stage0_gate": gate,
        }
    )
    value["contract_sha256"] = capacity_contract.successor._self_hash(
        value, "contract_sha256"
    )
    return value


def contract_path(value: Mapping[str, Any]) -> Path:
    return CONTRACT_ROOT / f"{VARIANT_ID}-{value['contract_sha256']}.json"


def inspect_contract(path: Path) -> dict[str, Any]:
    recorded = _read_json(path)
    if recorded.get("contract_sha256") != capacity_contract.successor._self_hash(
        recorded, "contract_sha256"
    ):
        raise MultiAssetEtfTsmomStage0Error("Stage 0 contract hash is invalid")
    if recorded != build_contract(write_private=False):
        raise MultiAssetEtfTsmomStage0Error("Stage 0 contract does not rebuild")
    denominator = recorded["partition_denominator"]
    if not (
        denominator["stage0_signals"] == STAGE0_SIGNAL_COUNT
        and denominator["development_reserved_signals"] >= 30
        and denominator["confirmation_reserved_signals"]
        == CONFIRMATION_SIGNAL_COUNT
        and recorded["access_contract"][
            "market_outcome_input_access_before_inspection_permitted"
        ]
        is False
        and recorded["access_contract"]["development_outcome_access_permitted"]
        is False
        and recorded["access_contract"]["confirmation_outcome_access_permitted"]
        is False
        and recorded["outcomes_previously_accessed_for_exact_rules"] is False
        and recorded["returns_computed"] == 0
    ):
        raise MultiAssetEtfTsmomStage0Error("Stage 0 anti-tuning boundary differs")
    value: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "inspection_kind": "stage0-zero-result-contract-inspection",
        "variant_id": VARIANT_ID,
        "contract_sha256": recorded["contract_sha256"],
        "contract_file_sha256": sha256_file(path),
        "rules_hash": recorded["rules_hash"],
        "partition_sha256": recorded["partition_sha256"],
        "partition_denominator": denominator,
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
    value["inspection_sha256"] = capacity_contract.successor._self_hash(
        value, "inspection_sha256"
    )
    return value


def _dataset(store: HistoricalDayStore, symbol: str, day: str) -> dict[str, Any]:
    value = capacity_run._dataset(store, symbol, day)
    if value is None:
        raise MultiAssetEtfTsmomStage0Error(f"missing frozen daily input {symbol} {day}")
    return value


def _stage0_contract_and_inspection() -> tuple[Path, Path, dict[str, Any], dict[str, Any]]:
    contract_file = _one(
        "strategy_tournament/v2/multi_asset_etf_tsmom/stage0/contracts/"
        "multi-asset-etf-tsmom-v1-*.json",
        "Stage 0 contract",
    )
    inspection_file = _one(
        "strategy_tournament/v2/multi_asset_etf_tsmom/stage0/inspections/"
        "multi-asset-etf-tsmom-v1-contract-*.json",
        "Stage 0 contract inspection",
    )
    frozen = _read_json(contract_file)
    inspected = _read_json(inspection_file)
    if inspected != inspect_contract(contract_file):
        raise MultiAssetEtfTsmomStage0Error("Stage 0 contract inspection differs")
    return contract_file, inspection_file, frozen, inspected


def _build_input_graph(store: HistoricalDayStore) -> dict[str, Any]:
    _contract_file, _inspection_file, frozen, inspected = _stage0_contract_and_inspection()
    if inspected.get("stage0_market_outcome_input_access_permitted") is not True:
        raise MultiAssetEtfTsmomStage0Error("Stage 0 outcome inputs are not permitted")
    partition = capacity_run._read_gzip_json(PRIVATE_PARTITION_PATH)
    capacity_graph = capacity_run._read_gzip_json(capacity_run.PRIVATE_INPUT_PATH)
    dates = [str(day) for day in capacity_graph["dates"]]
    positions = {day: index for index, day in enumerate(dates)}
    records = []
    for selected in partition["partitions"]["stage0"]:
        symbol = str(selected["symbol"])
        decision_index = positions[str(selected["decision_session"])]
        opportunity_index = positions[str(selected["opportunity_session"])]
        if opportunity_index != decision_index + 1:
            raise MultiAssetEtfTsmomStage0Error("entry is not next session")
        atr_dates = dates[decision_index - ATR_LENGTH : decision_index + 1]
        outcome_dates = dates[
            opportunity_index : opportunity_index + MAXIMUM_HOLDING_SESSIONS
        ]
        if len(atr_dates) != ATR_LENGTH + 1 or len(outcome_dates) != 5:
            raise MultiAssetEtfTsmomStage0Error("Stage 0 row window is incomplete")
        atr_rows = []
        outcome_rows = []
        dataset_ids = []
        for day in atr_dates:
            dataset = _dataset(store, symbol, day)
            atr_rows.append(daily._validate_daily_row(dataset["rows"][0], symbol, day))
            dataset_ids.append(str(dataset["id"]))
        for day in outcome_dates:
            dataset = _dataset(store, symbol, day)
            outcome_rows.append(
                daily._validate_daily_row(dataset["rows"][0], symbol, day)
            )
            dataset_ids.append(str(dataset["id"]))
        records.append(
            {
                "selection": dict(selected),
                "atr_dates": atr_dates,
                "atr_rows": atr_rows,
                "outcome_dates": outcome_dates,
                "outcome_rows": outcome_rows,
                "dataset_ids": dataset_ids,
            }
        )
    value: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "dataset_id": "dataset-multi-asset-etf-tsmom-stage0-2026-07-22-v1",
        "variant_id": VARIANT_ID,
        "contract_sha256": frozen["contract_sha256"],
        "contract_inspection_sha256": inspected["inspection_sha256"],
        "rules_hash": frozen["rules_hash"],
        "partition_sha256": partition["partition_sha256"],
        "records": records,
        "stage0_signals": len(records),
        "returns_computed": 0,
        "broker_actions": 0,
    }
    value["input_sha256"] = capacity_contract.successor._self_hash(
        value, "input_sha256"
    )
    return value


def build_activation(
    store: HistoricalDayStore | None = None, *, write_private: bool = False
) -> dict[str, Any]:
    contract_file, inspection_file, frozen, inspected = _stage0_contract_and_inspection()
    daily._require_published((contract_file, inspection_file))
    graph = _build_input_graph(store or HistoricalDayStore.from_env())
    if write_private:
        capacity_run._write_gzip_json(graph, PRIVATE_INPUT_PATH)
    if not PRIVATE_INPUT_PATH.is_file():
        raise MultiAssetEtfTsmomStage0Error("private Stage 0 inputs are missing")
    if capacity_run._read_gzip_json(PRIVATE_INPUT_PATH) != graph:
        raise MultiAssetEtfTsmomStage0Error("private Stage 0 inputs do not rebuild")
    value: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "artifact_kind": "stage0-input-activation",
        "variant_id": VARIANT_ID,
        "strategy_version": STRATEGY_VERSION,
        "mechanism_family": capacity_contract.THEME_ID,
        "contract_sha256": frozen["contract_sha256"],
        "contract_inspection_sha256": inspected["inspection_sha256"],
        "rules_hash": frozen["rules_hash"],
        "partition_sha256": frozen["partition_sha256"],
        "input_sha256": graph["input_sha256"],
        "private_input_file_sha256": sha256_file(PRIVATE_INPUT_PATH),
        "stage0_signals": len(graph["records"]),
        "daily_rows_frozen": sum(
            len(row["atr_rows"]) + len(row["outcome_rows"])
            for row in graph["records"]
        ),
        "return_evaluation_before_input_inspection_permitted": False,
        "development_outcome_access_permitted": False,
        "confirmation_outcome_access_permitted": False,
        "broker_actions_permitted": False,
        "returns_computed": 0,
        "maturity_effect": "NONE",
    }
    value["activation_sha256"] = capacity_contract.successor._self_hash(
        value, "activation_sha256"
    )
    return value


def activation_path(value: Mapping[str, Any]) -> Path:
    return ACTIVATION_ROOT / f"{VARIANT_ID}-{value['activation_sha256']}.json"


def inspect_inputs(path: Path, store: HistoricalDayStore | None = None) -> dict[str, Any]:
    recorded = _read_json(path)
    if recorded.get("activation_sha256") != capacity_contract.successor._self_hash(
        recorded, "activation_sha256"
    ):
        raise MultiAssetEtfTsmomStage0Error("Stage 0 activation hash is invalid")
    if recorded != build_activation(store, write_private=False):
        raise MultiAssetEtfTsmomStage0Error("Stage 0 activation does not rebuild")
    graph = capacity_run._read_gzip_json(PRIVATE_INPUT_PATH)
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
        "stage0_signals": len(graph["records"]),
        "daily_rows_inspected": sum(
            len(row["atr_rows"]) + len(row["outcome_rows"])
            for row in graph["records"]
        ),
        "stage0_return_evaluation_permitted": True,
        "development_outcome_access_permitted": False,
        "confirmation_outcome_access_permitted": False,
        "broker_actions": 0,
        "returns_computed": 0,
        "maturity_effect": "NONE",
        "valid": True,
    }
    value["inspection_sha256"] = capacity_contract.successor._self_hash(
        value, "inspection_sha256"
    )
    return value


def _atr(atr_rows: Sequence[Mapping[str, Any]]) -> float:
    if len(atr_rows) != ATR_LENGTH + 1:
        raise MultiAssetEtfTsmomStage0Error("ATR input length differs")
    ranges = []
    for index in range(1, len(atr_rows)):
        row = atr_rows[index]
        previous_close = float(atr_rows[index - 1]["c"])
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
        raise MultiAssetEtfTsmomStage0Error("outcome window length differs")
    raw_entry = float(outcome_rows[0]["o"])
    stop = raw_entry - STOP_ATR_MULTIPLE * _atr(atr_rows)
    if not (math.isfinite(stop) and 0 < stop < raw_entry):
        raise MultiAssetEtfTsmomStage0Error("planned stop is invalid")
    exit_price = float(outcome_rows[-1]["c"])
    exit_reason = "force_flat_fifth_close"
    exit_row = outcome_rows[-1]
    for row in outcome_rows:
        open_price = float(row["o"])
        if open_price <= stop:
            exit_price = open_price
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
        raise MultiAssetEtfTsmomStage0Error("cost-adjusted planned risk is invalid")
    return {
        "net_r": (exit_fill - entry_fill) / planned_risk,
        "exit_reason": exit_reason,
        "exit_date": datetime.fromisoformat(str(exit_row["t"])).date().isoformat(),
        "stop_executed": exit_reason.startswith("stop"),
    }


def build_result(
    activation: Path, inspection: Path, *, require_published: bool = True
) -> dict[str, Any]:
    frozen_activation = _read_json(activation)
    frozen_inspection = _read_json(inspection)
    if frozen_inspection != inspect_inputs(activation):
        raise MultiAssetEtfTsmomStage0Error("Stage 0 input inspection differs")
    if frozen_inspection.get("stage0_return_evaluation_permitted") is not True:
        raise MultiAssetEtfTsmomStage0Error("Stage 0 return evaluation is not permitted")
    if require_published:
        daily._require_published((activation, inspection))
    graph = capacity_run._read_gzip_json(PRIVATE_INPUT_PATH)
    records = []
    for row in graph["records"]:
        outcomes = {
            str(cost): _outcome(row["atr_rows"], row["outcome_rows"], cost)
            for cost in (PRIMARY_COST_BPS, *STRESS_COST_BPS)
        }
        primary = outcomes[str(PRIMARY_COST_BPS)]
        selected = row["selection"]
        records.append(
            {
                "signal_date": selected["decision_session"],
                "opportunity_date": selected["opportunity_session"],
                "symbol": selected["symbol"],
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
            "strategy_tournament/v2/multi_asset_etf_tsmom/stage0/contracts/"
            "multi-asset-etf-tsmom-v1-*.json",
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
        "campaign_id": capacity_contract.CAMPAIGN_ID,
        "variant_id": VARIANT_ID,
        "strategy_version": STRATEGY_VERSION,
        "mechanism_family": capacity_contract.THEME_ID,
        "contract_sha256": frozen_activation["contract_sha256"],
        "rules_hash": frozen_activation["rules_hash"],
        "activation_sha256": frozen_activation["activation_sha256"],
        "input_inspection_sha256": frozen_inspection["inspection_sha256"],
        "claim_scope": "FALSIFICATION_ONLY",
        "development_evidence_eligible": False,
        "confirmation_evidence_eligible": False,
        "denominator": {
            "frozen_stage0_signals": graph["stage0_signals"],
            "closed_signals": len(records),
            "missed_entries": 0,
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
        "broker_actions": 0,
        "maturity_effect": "NONE",
        "records": records,
    }
    value["result_sha256"] = capacity_contract.successor._self_hash(
        value, "result_sha256"
    )
    return value


def inspect_result(activation: Path, inspection: Path, result_path: Path) -> dict[str, Any]:
    recorded = _read_json(result_path)
    if recorded.get("result_sha256") != capacity_contract.successor._self_hash(
        recorded, "result_sha256"
    ):
        raise MultiAssetEtfTsmomStage0Error("Stage 0 result hash is invalid")
    if recorded != build_result(activation, inspection, require_published=False):
        raise MultiAssetEtfTsmomStage0Error("Stage 0 result does not rebuild")
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
    value["inspection_sha256"] = capacity_contract.successor._self_hash(
        value, "inspection_sha256"
    )
    return value


def _one(pattern: str, description: str) -> Path:
    matches = sorted(PROJECT_ROOT.glob(pattern))
    if len(matches) != 1:
        raise MultiAssetEtfTsmomStage0Error(
            f"expected one {description}; found {len(matches)}"
        )
    return matches[0]


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
            value = build_contract(write_private=True)
            path = contract_path(value)
            _write_json(value, path)
            result: dict[str, Any] = {
                "contract_sha256": value["contract_sha256"],
                "rules_hash": value["rules_hash"],
                "partition_denominator": value["partition_denominator"],
                "written": str(path.relative_to(PROJECT_ROOT)),
                "outcome_access_permitted": False,
            }
        elif args.command == "inspect-contract":
            source = _one(
                "strategy_tournament/v2/multi_asset_etf_tsmom/stage0/contracts/"
                "multi-asset-etf-tsmom-v1-*.json",
                "Stage 0 contract",
            )
            value = inspect_contract(source)
            path = INSPECTION_ROOT / (
                f"{VARIANT_ID}-contract-{value['inspection_sha256']}.json"
            )
            _write_json(value, path)
            result = {
                "inspection_sha256": value["inspection_sha256"],
                "written": str(path.relative_to(PROJECT_ROOT)),
                "return_evaluation_permitted": False,
            }
        elif args.command == "freeze-inputs":
            value = build_activation(write_private=True)
            path = activation_path(value)
            _write_json(value, path)
            result = {
                "activation_sha256": value["activation_sha256"],
                "stage0_signals": value["stage0_signals"],
                "written": str(path.relative_to(PROJECT_ROOT)),
                "return_evaluation_permitted": False,
            }
        elif args.command == "inspect-inputs":
            source = _one(
                "strategy_tournament/v2/multi_asset_etf_tsmom/stage0/activations/"
                "multi-asset-etf-tsmom-v1-*.json",
                "Stage 0 input activation",
            )
            value = inspect_inputs(source)
            path = INSPECTION_ROOT / (
                f"{VARIANT_ID}-input-{value['inspection_sha256']}.json"
            )
            _write_json(value, path)
            result = {
                "inspection_sha256": value["inspection_sha256"],
                "stage0_signals": value["stage0_signals"],
                "written": str(path.relative_to(PROJECT_ROOT)),
                "return_evaluation_permitted": True,
            }
        elif args.command == "evaluate":
            activation = _one(
                "strategy_tournament/v2/multi_asset_etf_tsmom/stage0/activations/"
                "multi-asset-etf-tsmom-v1-*.json",
                "Stage 0 input activation",
            )
            inspection = _one(
                "strategy_tournament/v2/multi_asset_etf_tsmom/stage0/inspections/"
                "multi-asset-etf-tsmom-v1-input-*.json",
                "Stage 0 input inspection",
            )
            value = build_result(activation, inspection)
            path = RESULT_ROOT / (
                f"2026-07-22-{VARIANT_ID}-stage0-{value['result_sha256']}.json"
            )
            _write_json(value, path)
            result = {
                "result_sha256": value["result_sha256"],
                "stage0_survived": value["stage0_survived"],
                "closed_signals": value["denominator"]["closed_signals"],
                "written": str(path.relative_to(PROJECT_ROOT)),
            }
        else:
            activation = _one(
                "strategy_tournament/v2/multi_asset_etf_tsmom/stage0/activations/"
                "multi-asset-etf-tsmom-v1-*.json",
                "Stage 0 input activation",
            )
            inspection = _one(
                "strategy_tournament/v2/multi_asset_etf_tsmom/stage0/inspections/"
                "multi-asset-etf-tsmom-v1-input-*.json",
                "Stage 0 input inspection",
            )
            result_path = _one(
                f"research_results/2026-07-22-{VARIANT_ID}-stage0-*.json",
                "Stage 0 result",
            )
            value = inspect_result(activation, inspection, result_path)
            path = INSPECTION_ROOT / (
                f"{VARIANT_ID}-result-{value['inspection_sha256']}.json"
            )
            _write_json(value, path)
            result = {
                "inspection_sha256": value["inspection_sha256"],
                "stage0_survived": value["stage0_survived"],
                "written": str(path.relative_to(PROJECT_ROOT)),
            }
    except (
        MultiAssetEtfTsmomStage0Error,
        capacity_run.MultiAssetEtfTsmomCapacityRunError,
        OSError,
        ValueError,
    ) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}), file=sys.stderr)
        return 2
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

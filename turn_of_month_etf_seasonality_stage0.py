"""Freeze, inspect, and evaluate turn-of-month ETF seasonality Stage 0."""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import etf_or_momentum_stage0 as common
import sector_etf_rotation_stage0 as daily
from historical_store import HistoricalDayStore, sha256_file


PROJECT_ROOT = Path(__file__).resolve().parent
DATA_ROOT = Path("/Users/ensomniac/trade/historical_data")
SLATE_PATH = daily.SLATE_PATH
SLATE_INSPECTION_PATH = daily.SLATE_INSPECTION_PATH
PRIVATE_INPUT_PATH = (
    DATA_ROOT / "_derived/turn_of_month_etf_seasonality_stage0/"
    "dataset-turn-of-month-etf-seasonality-stage0-2026-07-21-v1/"
    "frozen-inputs.json.gz"
)
VARIANT_ID = "turn-of-month-etf-seasonality-v1"
SYMBOL = "SPY"
COLLECTION_START = "2022-01-03"
EVALUATION_START = "2023-01-03"
EVALUATION_END = "2025-12-31"
COLLECTION_END_EXCLUSIVE = "2026-01-09"
PRIMARY_COST_BPS = 5
STRESS_COST_BPS = (10, 20)
SCHEMA_VERSION = 1


class TurnOfMonthStage0Error(RuntimeError):
    """The frozen turn-of-month evidence is incomplete or inconsistent."""


def _load_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise TurnOfMonthStage0Error(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise TurnOfMonthStage0Error(f"{path} must contain an object")
    return value


def _variant() -> dict[str, Any]:
    slate = _load_json(SLATE_PATH)
    if slate.get("manifest_sha256") != common._self_hash(slate, "manifest_sha256"):
        raise TurnOfMonthStage0Error("second-wave slate hash is invalid")
    matches = [item for item in slate["variants"] if item["variant_id"] == VARIANT_ID]
    if len(matches) != 1:
        raise TurnOfMonthStage0Error("turn-of-month variant is missing from the slate")
    inspection = _load_json(SLATE_INSPECTION_PATH)
    if (
        inspection.get("inspection_sha256")
        != common._self_hash(inspection, "inspection_sha256")
        or inspection.get("manifest_sha256") != slate["manifest_sha256"]
        or inspection.get("return_evaluation_authorized") is not True
    ):
        raise TurnOfMonthStage0Error("second-wave slate inspection is invalid")
    return dict(matches[0])


def _dataset(store: HistoricalDayStore, day: str) -> dict[str, Any] | None:
    return daily._dataset(store, SYMBOL, day)


def _monthly_events(dates: Sequence[str]) -> list[dict[str, Any]]:
    positions = {day: index for index, day in enumerate(dates)}
    by_month: dict[str, list[str]] = defaultdict(list)
    for day in dates:
        by_month[day[:7]].append(day)
    evaluation_months = sorted(
        {day[:7] for day in dates if EVALUATION_START <= day <= EVALUATION_END}
    )
    events: list[dict[str, Any]] = []
    for month in evaluation_months:
        sessions = by_month[month]
        if len(sessions) < 2:
            raise TurnOfMonthStage0Error(f"{month} lacks two XNYS sessions")
        year, month_number = (int(part) for part in month.split("-"))
        next_month = (
            f"{year + 1:04d}-01"
            if month_number == 12
            else f"{year:04d}-{month_number + 1:02d}"
        )
        next_sessions = by_month.get(next_month, [])
        if len(next_sessions) < 3:
            raise TurnOfMonthStage0Error(f"{next_month} lacks three outcome sessions")
        signal_date = sessions[-2]
        entry_date = sessions[-1]
        exit_date = next_sessions[2]
        if positions[signal_date] < 14:
            raise TurnOfMonthStage0Error(f"{signal_date} lacks ATR14 lookback")
        if not (
            positions[signal_date] + 1 == positions[entry_date]
            and positions[entry_date] < positions[exit_date]
        ):
            raise TurnOfMonthStage0Error(f"{month} event chronology drifted")
        events.append(
            {
                "month": month,
                "signal_date": signal_date,
                "entry_date": entry_date,
                "exit_date": exit_date,
            }
        )
    if len(events) != 36:
        raise TurnOfMonthStage0Error(
            f"expected 36 frozen monthly events; found {len(events)}"
        )
    return events


def _build_input_graph(store: HistoricalDayStore) -> dict[str, Any]:
    dates: list[str] = []
    rows: list[dict[str, Any]] = []
    dataset_ids: list[str] = []
    for day in store.dates(SYMBOL):
        if not COLLECTION_START <= day < COLLECTION_END_EXCLUSIVE:
            continue
        dataset = _dataset(store, day)
        if dataset is None:
            continue
        dates.append(day)
        rows.append(daily._validate_daily_row(dataset["rows"][0], SYMBOL, day))
        dataset_ids.append(str(dataset["id"]))
    if len(dates) < 900:
        raise TurnOfMonthStage0Error("SPY daily corpus is incomplete")
    graph: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "dataset_id": "dataset-turn-of-month-etf-seasonality-stage0-2026-07-21-v1",
        "variant_id": VARIANT_ID,
        "symbol": SYMBOL,
        "dates": dates,
        "rows": rows,
        "dataset_ids": dataset_ids,
        "monthly_events": _monthly_events(dates),
        "provider": "ibkr",
        "feed": "smart",
        "adjustment": "provider_adjusted_unknown_basis",
        "timeframe": "1d",
        "provider_requests": 0,
        "broker_actions": 0,
        "returns_computed": 0,
    }
    graph["input_sha256"] = common._self_hash(graph, "input_sha256")
    return graph


def build_activation(
    store: HistoricalDayStore | None = None, *, write_private: bool = False
) -> dict[str, Any]:
    variant = _variant()
    graph = _build_input_graph(store or HistoricalDayStore.from_env())
    if write_private:
        daily._write_gzip_json(graph, PRIVATE_INPUT_PATH)
    if not PRIVATE_INPUT_PATH.is_file():
        raise TurnOfMonthStage0Error("private frozen input graph is missing")
    if daily._load_gzip_json(PRIVATE_INPUT_PATH) != graph:
        raise TurnOfMonthStage0Error("private frozen input graph does not rebuild")
    slate = _load_json(SLATE_PATH)
    activation: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "contract_kind": "stage0-activation",
        "campaign_id": "multi-strategy-portfolio-validation-v1",
        "tournament_wave": 2,
        "variant_ordinal": 6,
        "variant_id": VARIANT_ID,
        "strategy_version": variant["version"],
        "mechanism_family": variant["mechanism_family"],
        "base_rules_hash": variant["rules_hash"],
        "slate_manifest_sha256": slate["manifest_sha256"],
        "slate_inspection_sha256": _load_json(SLATE_INSPECTION_PATH)[
            "inspection_sha256"
        ],
        "implementation_sha256": sha256_file(Path(__file__).resolve()),
        "claim_scope": "FALSIFICATION_ONLY",
        "outcomes_previously_accessed_for_exact_rules": False,
        "development_evidence_eligible": False,
        "confirmation_evidence_eligible": False,
        "provider_requests_authorized": False,
        "broker_actions_authorized": False,
        "return_evaluation_authorized_before_inspection": False,
        "source_selection": {
            "symbol": SYMBOL,
            "whole_provider": "ibkr",
            "provider_substitution": False,
            "common_calendar_dates": len(graph["dates"]),
            "evaluation_months": len(graph["monthly_events"]),
            "evaluation_start": EVALUATION_START,
            "evaluation_end": EVALUATION_END,
            "input_sha256": graph["input_sha256"],
            "private_input_file_sha256": sha256_file(PRIVATE_INPUT_PATH),
        },
        "selection_contract": dict(variant["signal"]),
        "outcome_contract": {
            **variant["execution"],
            **variant["exit"],
            "maximum_holding_trading_days": variant["maximum_holding_trading_days"],
            "planned_risk_r": ("cost-adjusted entry minus cost-adjusted planned stop"),
        },
        "stage0_gate": dict(slate["stage0_falsification"]),
        "denominator": {
            "decision_months": len(graph["monthly_events"]),
            "input_symbol_dates": len(graph["dates"]),
            "preserve_all_months": True,
            "maximum_strategy_entries_per_day": 1,
        },
        "maturity_effect": "NONE",
    }
    activation["activation_rules_hash"] = common._hash(
        {
            "base_rules_hash": activation["base_rules_hash"],
            "source_selection": activation["source_selection"],
            "selection_contract": activation["selection_contract"],
            "outcome_contract": activation["outcome_contract"],
            "stage0_gate": activation["stage0_gate"],
        }
    )
    activation["manifest_sha256"] = common._self_hash(activation, "manifest_sha256")
    return activation


def default_activation_path(activation: Mapping[str, Any]) -> Path:
    return (
        PROJECT_ROOT / "strategy_tournament/second_wave/activations/"
        f"{VARIANT_ID}-{activation['manifest_sha256']}.json"
    )


def inspect_activation(path: Path) -> dict[str, Any]:
    recorded = _load_json(path)
    if recorded.get("manifest_sha256") != common._self_hash(
        recorded, "manifest_sha256"
    ):
        raise TurnOfMonthStage0Error("activation content hash is invalid")
    if recorded != build_activation():
        raise TurnOfMonthStage0Error("activation does not rebuild")
    graph = daily._load_gzip_json(PRIVATE_INPUT_PATH)
    if graph.get("input_sha256") != common._self_hash(graph, "input_sha256"):
        raise TurnOfMonthStage0Error("private input content hash is invalid")
    if graph["input_sha256"] != recorded["source_selection"]["input_sha256"]:
        raise TurnOfMonthStage0Error("activation input identity drifted")
    if len(graph["dates"]) != len(graph["rows"]):
        raise TurnOfMonthStage0Error("daily input denominator drifted")
    for day, row in zip(graph["dates"], graph["rows"], strict=True):
        daily._validate_daily_row(row, SYMBOL, day)
    if graph["monthly_events"] != _monthly_events(graph["dates"]):
        raise TurnOfMonthStage0Error("monthly event mapping drifted")
    inspection: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "inspection_kind": "stage0-activation-input-inspection",
        "variant_id": VARIANT_ID,
        "manifest_sha256": recorded["manifest_sha256"],
        "manifest_file_sha256": sha256_file(path),
        "input_sha256": graph["input_sha256"],
        "input_symbol_dates": len(graph["dates"]),
        "evaluation_months": len(graph["monthly_events"]),
        "provider_requests": 0,
        "broker_actions": 0,
        "returns_computed": 0,
        "maturity_effect": "NONE",
        "return_evaluation_authorized": True,
        "valid": True,
    }
    inspection["inspection_sha256"] = common._self_hash(inspection, "inspection_sha256")
    return inspection


def _outcome(
    rows: Sequence[Mapping[str, Any]],
    signal_index: int,
    exit_index: int,
    cost_bps: int,
) -> dict[str, Any]:
    entry_index = signal_index + 1
    if exit_index <= entry_index:
        raise TurnOfMonthStage0Error("outcome chronology is invalid")
    raw_entry = float(rows[entry_index]["o"])
    stop = raw_entry - 1.5 * daily._atr(rows, signal_index)
    if stop <= 0 or stop >= raw_entry:
        raise TurnOfMonthStage0Error("planned stop is invalid")
    exit_price = float(rows[exit_index]["c"])
    exit_reason = "time_exit"
    actual_exit_index = exit_index
    for index in range(entry_index, exit_index + 1):
        row = rows[index]
        open_price = float(row["o"])
        if index > entry_index and open_price <= stop:
            exit_price = open_price
            exit_reason = "stop_gap"
        elif float(row["l"]) <= stop:
            exit_price = stop
            exit_reason = "stop"
        elif index < exit_index:
            continue
        actual_exit_index = index
        break
    cost = cost_bps / 10_000
    entry_fill = raw_entry * (1 + cost)
    exit_fill = exit_price * (1 - cost)
    stop_fill = stop * (1 - cost)
    planned_risk = entry_fill - stop_fill
    return {
        "net_r": (exit_fill - entry_fill) / planned_risk,
        "exit_reason": exit_reason,
        "exit_index": actual_exit_index,
        "stop_executed": exit_reason.startswith("stop"),
    }


def build_result(
    activation_path: Path,
    inspection_path: Path,
    *,
    require_published: bool = True,
) -> dict[str, Any]:
    activation = _load_json(activation_path)
    inspection = _load_json(inspection_path)
    if inspection != inspect_activation(activation_path):
        raise TurnOfMonthStage0Error("input inspection does not rebuild")
    if require_published:
        daily._require_published((activation_path, inspection_path))
    graph = daily._load_gzip_json(PRIVATE_INPUT_PATH)
    dates = list(graph["dates"])
    rows = list(graph["rows"])
    positions = {day: index for index, day in enumerate(dates)}
    records: list[dict[str, Any]] = []
    for event in graph["monthly_events"]:
        signal_index = positions[event["signal_date"]]
        exit_index = positions[event["exit_date"]]
        outcomes = {
            str(cost): _outcome(rows, signal_index, exit_index, cost)
            for cost in (PRIMARY_COST_BPS, *STRESS_COST_BPS)
        }
        primary = outcomes[str(PRIMARY_COST_BPS)]
        records.append(
            {
                **event,
                "symbol": SYMBOL,
                "exit_reason": primary["exit_reason"],
                "stop_executed": primary["stop_executed"],
                "net_r": primary["net_r"],
                "stress_10bps_r": outcomes["10"]["net_r"],
                "stress_20bps_r": outcomes["20"]["net_r"],
            }
        )
    primary_metrics = common._metrics([float(item["net_r"]) for item in records])
    stress = {
        "10": common._metrics([float(item["stress_10bps_r"]) for item in records]),
        "20": common._metrics([float(item["stress_20bps_r"]) for item in records]),
    }
    gate = activation["stage0_gate"]
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
    result: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "result_kind": "stage0-falsification",
        "variant_id": VARIANT_ID,
        "strategy_version": activation["strategy_version"],
        "mechanism_family": activation["mechanism_family"],
        "base_rules_hash": activation["base_rules_hash"],
        "activation_rules_hash": activation["activation_rules_hash"],
        "manifest_sha256": activation["manifest_sha256"],
        "inspection_sha256": inspection["inspection_sha256"],
        "implementation_sha256": activation["implementation_sha256"],
        "claim_scope": "FALSIFICATION_ONLY",
        "development_evidence_eligible": False,
        "confirmation_evidence_eligible": False,
        "provider_requests": 0,
        "broker_actions": 0,
        "denominator": {
            "decision_months": len(graph["monthly_events"]),
            "closed_signals": len(records),
            "no_trade_months": 0,
            "rule_violations": 0,
        },
        "primary_5bps": primary_metrics,
        "stress": stress,
        "stage0_survived": not blockers,
        "stage0_blockers": blockers,
        "next_action": (
            "freeze representative development without changing rules"
            if not blockers
            else "retire this exact variant; the authorized tournament is exhausted"
        ),
        "maturity_effect": "NONE",
        "records": records,
    }
    result["result_sha256"] = common._self_hash(result, "result_sha256")
    return result


def inspect_result(
    activation_path: Path, inspection_path: Path, result_path: Path
) -> dict[str, Any]:
    recorded = _load_json(result_path)
    if recorded.get("result_sha256") != common._self_hash(recorded, "result_sha256"):
        raise TurnOfMonthStage0Error("result content hash is invalid")
    if recorded != build_result(
        activation_path, inspection_path, require_published=False
    ):
        raise TurnOfMonthStage0Error("result does not independently rebuild")
    inspection: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "inspection_kind": "stage0-result-inspection",
        "variant_id": VARIANT_ID,
        "manifest_sha256": recorded["manifest_sha256"],
        "input_inspection_sha256": recorded["inspection_sha256"],
        "result_sha256": recorded["result_sha256"],
        "result_file_sha256": sha256_file(result_path),
        "closed_signals": recorded["denominator"]["closed_signals"],
        "stage0_survived": recorded["stage0_survived"],
        "provider_requests": 0,
        "broker_actions": 0,
        "maturity_effect": "NONE",
        "valid": True,
    }
    inspection["inspection_sha256"] = common._self_hash(inspection, "inspection_sha256")
    return inspection


def _one(pattern: str, description: str) -> Path:
    matches = sorted(PROJECT_ROOT.glob(pattern))
    if len(matches) != 1:
        raise TurnOfMonthStage0Error(
            f"expected one {description}; found {len(matches)}"
        )
    return matches[0]


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "command", choices=("freeze", "inspect-inputs", "evaluate", "inspect-result")
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "freeze":
            result = build_activation(write_private=True)
            path = default_activation_path(result)
            daily._write_json(result, path)
            output = {
                "manifest_sha256": result["manifest_sha256"],
                "evaluation_months": result["denominator"]["decision_months"],
                "written": str(path.relative_to(PROJECT_ROOT)),
            }
        elif args.command == "inspect-inputs":
            activation_path = _one(
                "strategy_tournament/second_wave/activations/"
                "turn-of-month-etf-seasonality-v1-*.json",
                "turn-of-month activation",
            )
            result = inspect_activation(activation_path)
            path = (
                PROJECT_ROOT / "strategy_tournament/second_wave/inspections/"
                f"{VARIANT_ID}-input-{result['inspection_sha256']}.json"
            )
            daily._write_json(result, path)
            output = {
                "inspection_sha256": result["inspection_sha256"],
                "written": str(path.relative_to(PROJECT_ROOT)),
            }
        elif args.command == "evaluate":
            activation_path = _one(
                "strategy_tournament/second_wave/activations/"
                "turn-of-month-etf-seasonality-v1-*.json",
                "turn-of-month activation",
            )
            inspection_path = _one(
                "strategy_tournament/second_wave/inspections/"
                "turn-of-month-etf-seasonality-v1-input-*.json",
                "turn-of-month input inspection",
            )
            result = build_result(activation_path, inspection_path)
            path = (
                PROJECT_ROOT / "research_results/"
                f"2026-07-21-turn-of-month-etf-seasonality-stage0-"
                f"{result['result_sha256']}.json"
            )
            daily._write_json(result, path)
            output = {
                "result_sha256": result["result_sha256"],
                "stage0_survived": result["stage0_survived"],
                "closed_signals": result["denominator"]["closed_signals"],
                "written": str(path.relative_to(PROJECT_ROOT)),
            }
        else:
            activation_path = _one(
                "strategy_tournament/second_wave/activations/"
                "turn-of-month-etf-seasonality-v1-*.json",
                "turn-of-month activation",
            )
            inspection_path = _one(
                "strategy_tournament/second_wave/inspections/"
                "turn-of-month-etf-seasonality-v1-input-*.json",
                "turn-of-month input inspection",
            )
            result_path = _one(
                "research_results/2026-07-21-turn-of-month-etf-seasonality-stage0-*.json",
                "turn-of-month Stage 0 result",
            )
            result = inspect_result(activation_path, inspection_path, result_path)
            path = (
                PROJECT_ROOT / "strategy_tournament/second_wave/inspections/"
                f"{VARIANT_ID}-result-{result['inspection_sha256']}.json"
            )
            daily._write_json(result, path)
            output = {
                "inspection_sha256": result["inspection_sha256"],
                "written": str(path.relative_to(PROJECT_ROOT)),
            }
        print(json.dumps(output, indent=2, sort_keys=True))
        return 0
    except (OSError, RuntimeError, ValueError) as exc:
        print(
            json.dumps({"error": str(exc), "error_type": type(exc).__name__}, indent=2),
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

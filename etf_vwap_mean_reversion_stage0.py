"""Freeze and evaluate the preregistered ETF VWAP-reversion Stage 0 trial."""

from __future__ import annotations

import argparse
import json
import statistics
import sys
from collections import Counter
from collections.abc import Mapping, Sequence
from datetime import time
from pathlib import Path
from typing import Any

import etf_or_momentum_stage0 as common
from historical_store import HistoricalDayStore, sha256_file
from portfolio_tournament import inspect_manifest as inspect_slate


PROJECT_ROOT = Path(__file__).resolve().parent
SLATE_PATH = common.SLATE_PATH
SHARED_INPUT_PATH = (
    PROJECT_ROOT
    / "strategy_tournament"
    / "activations"
    / "etf-or-momentum-v1-fbea206058e0530a38f89b4b19ccdb71949329960fc6ebbef63ae56966961b03.json"
)
VARIANT_ID = "etf-vwap-mean-reversion-v1"
VARIANT_ORDINAL = 2
SYMBOLS = common.SYMBOLS
SCHEMA_VERSION = 1
PRIMARY_COST_BPS = 5
STRESS_COST_BPS = (10, 20)
SIGNAL_START = time(10, 0)
SIGNAL_END = time(14, 30)
FORCE_FLAT = time(15, 50)


class EtfVwapStage0Error(RuntimeError):
    """The ETF VWAP Stage 0 contract or evidence is invalid."""


def _load_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise EtfVwapStage0Error(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise EtfVwapStage0Error(f"{path} must contain an object")
    return value


def _variant() -> dict[str, Any]:
    inspect_slate(SLATE_PATH)
    matches = [
        item
        for item in _load_json(SLATE_PATH)["variants"]
        if item.get("variant_id") == VARIANT_ID
        and item.get("variant_ordinal") == VARIANT_ORDINAL
    ]
    if len(matches) != 1:
        raise EtfVwapStage0Error("frozen slate variant identity is unavailable")
    return dict(matches[0])


def _shared_inputs(store: HistoricalDayStore) -> dict[str, Any]:
    recorded = _load_json(SHARED_INPUT_PATH)
    common._validate_manifest_identity(recorded)
    rebuilt = common.build_manifest(store)
    if recorded != rebuilt:
        raise EtfVwapStage0Error("shared ETF input graph does not exactly rebuild")
    return recorded


def build_manifest(store: HistoricalDayStore | None = None) -> dict[str, Any]:
    source = store or HistoricalDayStore.from_env()
    shared = _shared_inputs(source)
    variant = _variant()
    manifest: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "contract_kind": "stage0-activation",
        "campaign_id": "multi-strategy-portfolio-validation-v1",
        "tournament_wave": 1,
        "variant_ordinal": VARIANT_ORDINAL,
        "variant_id": VARIANT_ID,
        "strategy_version": variant["version"],
        "mechanism_family": variant["mechanism_family"],
        "base_rules_hash": variant["rules_hash"],
        "slate_manifest_sha256": _load_json(SLATE_PATH)["manifest_sha256"],
        "implementation_sha256": sha256_file(Path(__file__).resolve()),
        "shared_evaluator_sha256": sha256_file(Path(common.__file__).resolve()),
        "shared_input_manifest_sha256": shared["manifest_sha256"],
        "store_metadata_sha256": shared["store_metadata_sha256"],
        "claim_scope": "FALSIFICATION_ONLY",
        "outcomes_previously_accessed": True,
        "development_evidence_eligible": False,
        "confirmation_evidence_eligible": False,
        "provider_requests_authorized": False,
        "broker_actions_authorized": False,
        "return_evaluation_authorized_before_inspection": False,
        "source_selection": shared["source_selection"],
        "denominator": shared["denominator"],
        "selection_contract": {
            "signal_bar_starts_et": "10:00:00-14:30:00",
            "session_vwap": "cumulative sum(bar WAP times volume) divided by cumulative volume through each completed bar",
            "washout_arm": "completed close at least 0.75 percent below session VWAP with simple five-change RSI at or below 25",
            "simple_rsi": "100 minus 100 divided by one plus mean positive change over mean absolute negative change; five completed close changes",
            "trigger": "a later bullish completed close above the prior bar high while still below session VWAP",
            "entry": "next observed one-minute open",
            "daily_selection": "earliest executable entry; ties use deeper armed VWAP deviation then lexical symbol",
            "miss_policy": "an invalid target or stop remains in the denominator; a later executable symbol may be selected",
        },
        "outcome_contract": {
            "stop": "trigger-bar low minus 5 bps",
            "target": "session VWAP fixed at the completed trigger decision bar",
            "entry_exit_cost_bps_per_side": [
                PRIMARY_COST_BPS,
                *STRESS_COST_BPS,
            ],
            "same_interval_ambiguity": "stop_first",
            "stop_gap_fill": "worse of stop and observed bar open",
            "target_gap_fill": "target price without favorable gap improvement",
            "force_flat_et": "15:50:00 at observed bar open",
            "planned_risk_r": "cost-adjusted entry minus cost-adjusted planned stop",
        },
        "stage0_gate": {
            "minimum_closed_signals": 30,
            "minimum_expectancy_r_exclusive": 0,
            "minimum_profit_factor": 1.10,
            "maximum_drawdown_r": 8,
            "require_positive_20bps_total_r": True,
            "maximum_rule_violations": 0,
            "effect": "SURVIVE_TO_REPRESENTATIVE_DEVELOPMENT only; never PILOT_READY",
        },
        "excluded_dates": shared["excluded_dates"],
        "inputs": shared["inputs"],
    }
    manifest["activation_rules_hash"] = common._hash(
        {
            "base_rules_hash": manifest["base_rules_hash"],
            "source_selection": manifest["source_selection"],
            "selection_contract": manifest["selection_contract"],
            "outcome_contract": manifest["outcome_contract"],
            "stage0_gate": manifest["stage0_gate"],
        }
    )
    manifest["manifest_sha256"] = common._self_hash(manifest, "manifest_sha256")
    return manifest


def _validate_manifest(recorded: Mapping[str, Any]) -> None:
    if recorded.get("manifest_sha256") != common._self_hash(
        recorded, "manifest_sha256"
    ):
        raise EtfVwapStage0Error("activation manifest content hash is invalid")
    if recorded.get("variant_id") != VARIANT_ID:
        raise EtfVwapStage0Error("activation manifest variant identity is invalid")
    if recorded.get("claim_scope") != "FALSIFICATION_ONLY":
        raise EtfVwapStage0Error("activation manifest claim scope is invalid")
    for field in (
        "development_evidence_eligible",
        "confirmation_evidence_eligible",
        "provider_requests_authorized",
        "broker_actions_authorized",
        "return_evaluation_authorized_before_inspection",
    ):
        if recorded.get(field) is not False:
            raise EtfVwapStage0Error(f"activation manifest {field} must be false")


def inspect_activation(
    path: Path, store: HistoricalDayStore | None = None
) -> dict[str, Any]:
    recorded = _load_json(path)
    _validate_manifest(recorded)
    source = store or HistoricalDayStore.from_env()
    if recorded != build_manifest(source):
        raise EtfVwapStage0Error("activation manifest does not exactly rebuild")
    loaded = common._load_manifest_bars(recorded, source)
    result: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "inspection_kind": "stage0-activation-input-inspection",
        "variant_id": VARIANT_ID,
        "manifest_sha256": recorded["manifest_sha256"],
        "manifest_file_sha256": sha256_file(path),
        "implementation_sha256": recorded["implementation_sha256"],
        "shared_evaluator_sha256": recorded["shared_evaluator_sha256"],
        "selected_dates": recorded["denominator"]["included_dates"],
        "selected_symbol_sessions": len(loaded),
        "selected_bars": sum(len(rows) for rows in loaded.values()),
        "excluded_dates": recorded["denominator"]["excluded_dates"],
        "provider_requests": 0,
        "broker_actions": 0,
        "returns_computed": 0,
        "return_evaluation_authorized": True,
        "claim_scope": "FALSIFICATION_ONLY",
        "valid": True,
    }
    result["inspection_sha256"] = common._self_hash(result, "inspection_sha256")
    return result


def _validate_inspection(
    manifest_path: Path, inspection_path: Path, store: HistoricalDayStore
) -> dict[str, Any]:
    recorded = _load_json(inspection_path)
    if recorded.get("inspection_sha256") != common._self_hash(
        recorded, "inspection_sha256"
    ):
        raise EtfVwapStage0Error("activation inspection content hash is invalid")
    if recorded != inspect_activation(manifest_path, store):
        raise EtfVwapStage0Error("activation inspection does not exactly rebuild")
    if recorded.get("return_evaluation_authorized") is not True:
        raise EtfVwapStage0Error("activation inspection does not authorize evaluation")
    return recorded


def _rsi_five(rows: Sequence[Mapping[str, Any]], index: int) -> float | None:
    if index < 5:
        return None
    changes = [
        float(rows[offset]["c"]) - float(rows[offset - 1]["c"])
        for offset in range(index - 4, index + 1)
    ]
    gains = sum(max(change, 0) for change in changes) / 5
    losses = sum(max(-change, 0) for change in changes) / 5
    if gains == 0 and losses == 0:
        return 50.0
    if losses == 0:
        return 100.0
    return 100 - 100 / (1 + gains / losses)


def _candidate(
    *, day: str, symbol: str, rows: Sequence[Mapping[str, Any]]
) -> dict[str, Any]:
    cumulative_vwap = common._cumulative_vwap(rows)
    armed_index: int | None = None
    armed_deviation = 0.0
    saw_washout = False
    for index in range(5, len(rows) - 1):
        observed = common._row_time(rows[index]).time().replace(tzinfo=None)
        if observed < SIGNAL_START:
            continue
        if observed > SIGNAL_END:
            break
        close = float(rows[index]["c"])
        vwap = cumulative_vwap[index]
        deviation = (vwap - close) / vwap
        rsi = _rsi_five(rows, index)
        if deviation >= 0.0075 and rsi is not None and rsi <= 25:
            saw_washout = True
            if armed_index is None or deviation > armed_deviation:
                armed_index = index
                armed_deviation = deviation
        if armed_index is None or index <= armed_index:
            continue
        if (
            close > float(rows[index]["o"])
            and close > float(rows[index - 1]["h"])
            and close < vwap
        ):
            entry_index = index + 1
            entry_open = float(rows[entry_index]["o"])
            stop = float(rows[index]["l"]) * (1 - 0.0005)
            if entry_open <= stop:
                return {
                    "date": day,
                    "symbol": symbol,
                    "status": "nonpositive_stop_distance",
                    "trigger_time_et": rows[index]["t"],
                }
            if entry_open >= vwap:
                return {
                    "date": day,
                    "symbol": symbol,
                    "status": "target_not_above_entry",
                    "trigger_time_et": rows[index]["t"],
                }
            return {
                "date": day,
                "symbol": symbol,
                "status": "executable",
                "armed_time_et": rows[armed_index]["t"],
                "trigger_time_et": rows[index]["t"],
                "entry_time_et": rows[entry_index]["t"],
                "entry_index": entry_index,
                "entry_open": entry_open,
                "stop": stop,
                "target": vwap,
                "armed_deviation": armed_deviation,
            }
    return {
        "date": day,
        "symbol": symbol,
        "status": "no_reversal_trigger" if saw_washout else "no_washout",
    }


def _trade_outcome(
    candidate: Mapping[str, Any],
    rows: Sequence[Mapping[str, Any]],
    cost_bps: int,
) -> dict[str, Any]:
    entry_index = int(candidate["entry_index"])
    raw_entry = float(candidate["entry_open"])
    stop = float(candidate["stop"])
    target = float(candidate["target"])
    exit_price: float | None = None
    exit_reason = ""
    exit_time = ""
    for index in range(entry_index, len(rows)):
        row = rows[index]
        observed = common._row_time(row).time().replace(tzinfo=None)
        open_price = float(row["o"])
        if open_price <= stop:
            exit_price = open_price
            exit_reason = "stop_gap" if index > entry_index else "stop"
        elif observed >= FORCE_FLAT:
            exit_price = open_price
            exit_reason = "force_flat"
        else:
            stop_hit = float(row["l"]) <= stop
            target_hit = float(row["h"]) >= target
            if stop_hit:
                exit_price = stop
                exit_reason = "stop_first" if target_hit else "stop"
            elif target_hit:
                exit_price = target
                exit_reason = "target"
        if exit_price is not None:
            exit_time = str(row["t"])
            break
    if exit_price is None:
        raise EtfVwapStage0Error("trade did not resolve by force-flat")
    cost = cost_bps / 10_000
    entry_fill = raw_entry * (1 + cost)
    exit_fill = exit_price * (1 - cost)
    planned_risk = entry_fill - stop * (1 - cost)
    if planned_risk <= 0:
        raise EtfVwapStage0Error("cost-adjusted planned risk is not positive")
    return {
        "net_r": (exit_fill - entry_fill) / planned_risk,
        "exit_reason": exit_reason,
        "exit_time_et": exit_time,
        "stop_executed": exit_reason.startswith("stop"),
    }


def _metrics(values: Sequence[float]) -> dict[str, Any]:
    gains = sum(value for value in values if value > 0)
    losses = abs(sum(value for value in values if value < 0))
    profit_factor = gains / losses if losses else None
    return {
        "signals": len(values),
        "total_r": sum(values),
        "expectancy_r": statistics.fmean(values) if values else None,
        "profit_factor": profit_factor,
        "profit_factor_infinite": bool(values and gains > 0 and losses == 0),
        "maximum_drawdown_r": common._drawdown(values),
        "win_rate": (
            sum(value > 0 for value in values) / len(values) if values else None
        ),
    }


def build_result(
    manifest_path: Path,
    inspection_path: Path,
    store: HistoricalDayStore | None = None,
    *,
    require_published: bool = True,
) -> dict[str, Any]:
    source = store or HistoricalDayStore.from_env()
    manifest = _load_json(manifest_path)
    _validate_manifest(manifest)
    inspection = _validate_inspection(manifest_path, inspection_path, source)
    if require_published:
        common._require_published((manifest_path, inspection_path))
    bars = common._load_manifest_bars(manifest, source)
    dispositions: Counter[str] = Counter()
    records: list[dict[str, Any]] = []
    no_trade_dates = 0
    for session in manifest["inputs"]:
        day = str(session["date"])
        candidates = [
            _candidate(day=day, symbol=symbol, rows=bars[(day, symbol)])
            for symbol in SYMBOLS
        ]
        dispositions.update(item["status"] for item in candidates)
        executable = [item for item in candidates if item["status"] == "executable"]
        executable.sort(
            key=lambda item: (
                str(item["entry_time_et"]),
                -float(item["armed_deviation"]),
                str(item["symbol"]),
            )
        )
        if not executable:
            no_trade_dates += 1
            continue
        selected = executable[0]
        dispositions["selected"] += 1
        dispositions["not_selected_daily_cap"] += len(executable) - 1
        outcomes = {
            str(cost): _trade_outcome(
                selected,
                bars[(day, str(selected["symbol"]))],
                cost,
            )
            for cost in (PRIMARY_COST_BPS, *STRESS_COST_BPS)
        }
        primary = outcomes["5"]
        records.append(
            {
                "date": day,
                "symbol": selected["symbol"],
                "armed_time_et": selected["armed_time_et"],
                "trigger_time_et": selected["trigger_time_et"],
                "entry_time_et": selected["entry_time_et"],
                "exit_time_et": primary["exit_time_et"],
                "exit_reason": primary["exit_reason"],
                "stop_executed": primary["stop_executed"],
                "net_r": primary["net_r"],
                "stress_10bps_r": outcomes["10"]["net_r"],
                "stress_20bps_r": outcomes["20"]["net_r"],
            }
        )
    values = [float(item["net_r"]) for item in records]
    stress_10 = [float(item["stress_10bps_r"]) for item in records]
    stress_20 = [float(item["stress_20bps_r"]) for item in records]
    primary_metrics = _metrics(values)
    stress_metrics = {"10": _metrics(stress_10), "20": _metrics(stress_20)}
    gate = manifest["stage0_gate"]
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
    if stress_metrics["20"]["total_r"] <= 0:
        blockers.append("20 bps-per-side total R is not positive")
    result: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "result_kind": "stage0-falsification",
        "variant_id": VARIANT_ID,
        "strategy_version": manifest["strategy_version"],
        "mechanism_family": manifest["mechanism_family"],
        "base_rules_hash": manifest["base_rules_hash"],
        "activation_rules_hash": manifest["activation_rules_hash"],
        "manifest_sha256": manifest["manifest_sha256"],
        "inspection_sha256": inspection["inspection_sha256"],
        "implementation_sha256": manifest["implementation_sha256"],
        "claim_scope": "FALSIFICATION_ONLY",
        "development_evidence_eligible": False,
        "confirmation_evidence_eligible": False,
        "provider_requests": 0,
        "broker_actions": 0,
        "denominator": {
            "dates": manifest["denominator"]["included_dates"],
            "symbol_sessions": manifest["denominator"]["included_symbol_sessions"],
            "closed_signals": len(records),
            "no_trade_dates": no_trade_dates,
            "rule_violations": 0,
        },
        "disposition_counts": dict(sorted(dispositions.items())),
        "primary_5bps": primary_metrics,
        "stress": stress_metrics,
        "stage0_survived": not blockers,
        "stage0_blockers": blockers,
        "next_action": (
            "freeze a representative development corpus without changing rules"
            if not blockers
            else "retire this exact variant and advance to the next frozen mechanism"
        ),
        "maturity_effect": "NONE",
        "records": records,
    }
    result["result_sha256"] = common._self_hash(result, "result_sha256")
    return result


def inspect_result(
    manifest_path: Path,
    inspection_path: Path,
    result_path: Path,
    store: HistoricalDayStore | None = None,
) -> dict[str, Any]:
    recorded = _load_json(result_path)
    if recorded.get("result_sha256") != common._self_hash(recorded, "result_sha256"):
        raise EtfVwapStage0Error("Stage 0 result content hash is invalid")
    rebuilt = build_result(
        manifest_path,
        inspection_path,
        store,
        require_published=False,
    )
    if recorded != rebuilt:
        raise EtfVwapStage0Error("Stage 0 result does not independently rebuild")
    audit: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "inspection_kind": "stage0-result-inspection",
        "variant_id": VARIANT_ID,
        "manifest_sha256": recorded["manifest_sha256"],
        "input_inspection_sha256": recorded["inspection_sha256"],
        "result_sha256": recorded["result_sha256"],
        "result_file_sha256": sha256_file(result_path),
        "closed_signals": recorded["denominator"]["closed_signals"],
        "stage0_survived": recorded["stage0_survived"],
        "maturity_effect": "NONE",
        "provider_requests": 0,
        "broker_actions": 0,
        "valid": True,
    }
    audit["inspection_sha256"] = common._self_hash(audit, "inspection_sha256")
    return audit


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("freeze")
    inspect = commands.add_parser("inspect")
    inspect.add_argument("manifest", type=Path)
    evaluate = commands.add_parser("evaluate")
    evaluate.add_argument("manifest", type=Path)
    evaluate.add_argument("inspection", type=Path)
    audit = commands.add_parser("inspect-result")
    audit.add_argument("manifest", type=Path)
    audit.add_argument("inspection", type=Path)
    audit.add_argument("result", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "freeze":
            result = build_manifest()
        elif args.command == "inspect":
            result = inspect_activation(args.manifest)
        elif args.command == "evaluate":
            result = build_result(args.manifest, args.inspection)
        else:
            result = inspect_result(args.manifest, args.inspection, args.result)
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    except (EtfVwapStage0Error, common.EtfOrbStage0Error, OSError, ValueError) as exc:
        print(
            json.dumps({"error": str(exc), "error_type": type(exc).__name__}, indent=2),
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

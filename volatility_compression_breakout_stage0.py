"""Freeze and evaluate the volatility-compression-breakout Stage 0 trial."""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections import Counter
from collections.abc import Mapping, Sequence
from datetime import time
from pathlib import Path
from typing import Any

import equity_gap_continuation_stage0 as continuation
import etf_or_momentum_stage0 as common
from historical_store import HistoricalDayStore, sha256_file
from portfolio_tournament import inspect_manifest as inspect_slate


PROJECT_ROOT = Path(__file__).resolve().parent
SLATE_PATH = common.SLATE_PATH
VARIANT_ID = "volatility-compression-breakout-v1"
VARIANT_ORDINAL = 6
SCHEMA_VERSION = 1
PRIMARY_COST_BPS = 5
STRESS_COST_BPS = (10, 20)
SIGNAL_START = time(10, 15)
SIGNAL_END = time(14, 30)
FORCE_FLAT = time(15, 50)
FIRST_RANGE_BARS = 30
COMPRESSION_BARS = 20
MAX_COMPRESSION_RATIO = 0.60
MIN_VOLUME_MULTIPLE = 1.50


class VolatilityCompressionError(RuntimeError):
    """The volatility-compression Stage 0 evidence is invalid."""


def _load_json(path: Path) -> dict[str, Any]:
    try:
        return common._load_json(path)
    except common.EtfOrbStage0Error as exc:
        raise VolatilityCompressionError(str(exc)) from exc


def _variant() -> dict[str, Any]:
    inspect_slate(SLATE_PATH)
    matches = [
        item
        for item in _load_json(SLATE_PATH)["variants"]
        if item.get("variant_id") == VARIANT_ID
        and item.get("variant_ordinal") == VARIANT_ORDINAL
    ]
    if len(matches) != 1:
        raise VolatilityCompressionError("frozen slate variant identity is unavailable")
    return dict(matches[0])


def build_manifest(store: HistoricalDayStore | None = None) -> dict[str, Any]:
    source = store or HistoricalDayStore.from_env()
    base = continuation.build_manifest(source)
    variant = _variant()
    manifest: dict[str, Any] = {
        key: base[key]
        for key in (
            "schema_version",
            "contract_kind",
            "campaign_id",
            "tournament_wave",
            "slate_manifest_sha256",
            "legacy_evidence_sha256",
            "legacy_collection_sha256",
            "legacy_dataset_hash",
            "claim_scope",
            "outcomes_previously_accessed",
            "development_evidence_eligible",
            "confirmation_evidence_eligible",
            "provider_requests_authorized",
            "broker_actions_authorized",
            "return_evaluation_authorized_before_inspection",
            "source_selection",
            "denominator",
            "inputs",
        )
    }
    manifest.update(
        {
            "variant_ordinal": VARIANT_ORDINAL,
            "variant_id": VARIANT_ID,
            "strategy_version": variant["version"],
            "mechanism_family": variant["mechanism_family"],
            "base_rules_hash": variant["rules_hash"],
            "implementation_sha256": sha256_file(Path(__file__).resolve()),
            "selection_contract": {
                "asset_gate": "frozen candidate is attested common stock with 09:30 open strictly above 5 dollars",
                "first30_range": "maximum high minus minimum low of completed 09:30-09:59 one-minute bars",
                "compression": "the 20 completed one-minute bars immediately preceding the signal bar",
                "compression_gate": "compression high minus low divided by first-30-minute range is at most 0.60",
                "session_vwap": "cumulative volume-weighted typical price through the completed signal bar",
                "volume_gate": "signal-bar volume is at least 1.50 times the arithmetic mean volume of the preceding 20 compression bars",
                "trigger": "completed close strictly above both compression high and session VWAP",
                "signal_bar_starts_et": "10:15:00-14:30:00 inclusive",
                "entry": "next observed one-minute open",
                "daily_selection": "earliest entry, then lowest compression ratio, then lexical symbol",
                "miss_policy": "invalid bars, nonpositive first-30 range, stop distance, or target distance remains in denominator and cannot signal",
            },
            "outcome_contract": {
                "stop": "low of the 20 completed compression bars preceding the signal",
                "target": "raw 2R above entry",
                "entry_exit_cost_bps_per_side": [PRIMARY_COST_BPS, *STRESS_COST_BPS],
                "same_interval_ambiguity": "stop_first",
                "stop_gap_fill": "worse of stop and observed bar open",
                "target_gap_fill": "target price without favorable gap improvement",
                "force_flat_et": "15:50:00 at observed bar open",
                "planned_risk_r": "cost-adjusted entry minus cost-adjusted planned stop",
            },
            "stage0_gate": dict(base["stage0_gate"]),
        }
    )
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
        raise VolatilityCompressionError("activation manifest content hash is invalid")
    if (
        recorded.get("variant_id") != VARIANT_ID
        or recorded.get("variant_ordinal") != VARIANT_ORDINAL
        or recorded.get("claim_scope") != "FALSIFICATION_ONLY"
    ):
        raise VolatilityCompressionError("activation manifest identity is invalid")
    if recorded.get("implementation_sha256") != sha256_file(Path(__file__).resolve()):
        raise VolatilityCompressionError("activation implementation drifted")
    for field in (
        "development_evidence_eligible",
        "confirmation_evidence_eligible",
        "provider_requests_authorized",
        "broker_actions_authorized",
        "return_evaluation_authorized_before_inspection",
    ):
        if recorded.get(field) is not False:
            raise VolatilityCompressionError(
                f"activation manifest {field} must be false"
            )
    denominator = recorded.get("denominator", {})
    if (
        denominator.get("requested_dates") != 100
        or denominator.get("included_dates") != 95
        or denominator.get("excluded_dates") != 5
        or denominator.get("included_symbol_sessions") != 950
    ):
        raise VolatilityCompressionError(
            "activation denominator is not exactly 100/95/5/950"
        )


def inspect_activation(
    path: Path, store: HistoricalDayStore | None = None
) -> dict[str, Any]:
    source = store or HistoricalDayStore.from_env()
    recorded = _load_json(path)
    _validate_manifest(recorded)
    if recorded != build_manifest(source):
        raise VolatilityCompressionError("activation manifest does not exactly rebuild")
    sessions = continuation._load_inputs(recorded, source)
    result: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "inspection_kind": "stage0-activation-input-inspection",
        "variant_id": VARIANT_ID,
        "manifest_sha256": recorded["manifest_sha256"],
        "manifest_file_sha256": sha256_file(path),
        "implementation_sha256": recorded["implementation_sha256"],
        "requested_dates": recorded["denominator"]["requested_dates"],
        "selected_dates": len(sessions),
        "selected_symbol_sessions": sum(len(item["candidates"]) for item in sessions),
        "selected_bars": sum(
            len(candidate["bars"])
            for item in sessions
            for candidate in item["candidates"]
        ),
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
    manifest_path: Path,
    inspection_path: Path,
    store: HistoricalDayStore,
) -> dict[str, Any]:
    recorded = _load_json(inspection_path)
    if recorded.get("inspection_sha256") != common._self_hash(
        recorded, "inspection_sha256"
    ):
        raise VolatilityCompressionError(
            "activation inspection content hash is invalid"
        )
    if recorded != inspect_activation(manifest_path, store):
        raise VolatilityCompressionError(
            "activation inspection does not exactly rebuild"
        )
    if recorded.get("return_evaluation_authorized") is not True:
        raise VolatilityCompressionError(
            "activation inspection does not authorize evaluation"
        )
    return recorded


def _candidate(*, day: str, raw: Mapping[str, Any]) -> dict[str, Any]:
    symbol = str(raw.get("symbol", ""))
    bars = raw.get("bars")
    if not isinstance(bars, list):
        raise VolatilityCompressionError(f"{day} {symbol}: bars are malformed")
    payload = raw.get("evaluation_payload")
    candidate = payload.get("candidate") if isinstance(payload, Mapping) else None
    if (
        not isinstance(candidate, Mapping)
        or candidate.get("is_common_stock") is not True
    ):
        return {"date": day, "symbol": symbol, "status": "not_common_stock"}
    opening_price = float(bars[0]["open"])
    if opening_price <= 5:
        return {"date": day, "symbol": symbol, "status": "opening_price_not_above_5"}
    first30 = bars[:FIRST_RANGE_BARS]
    first30_range = max(float(row["high"]) for row in first30) - min(
        float(row["low"]) for row in first30
    )
    if first30_range <= 0:
        return {"date": day, "symbol": symbol, "status": "nonpositive_first30_range"}
    vwap = continuation._cumulative_vwap(bars)
    for index in range(FIRST_RANGE_BARS, len(bars) - 1):
        observed = continuation._bar_time(bars[index])
        if observed < SIGNAL_START:
            continue
        if observed > SIGNAL_END:
            break
        compression = bars[index - COMPRESSION_BARS : index]
        compression_high = max(float(row["high"]) for row in compression)
        compression_low = min(float(row["low"]) for row in compression)
        compression_ratio = (compression_high - compression_low) / first30_range
        if compression_ratio > MAX_COMPRESSION_RATIO + 1e-12:
            continue
        mean_volume = sum(int(row["volume"]) for row in compression) / COMPRESSION_BARS
        signal_volume = int(bars[index]["volume"])
        if signal_volume + 1e-12 < MIN_VOLUME_MULTIPLE * mean_volume:
            continue
        signal_close = float(bars[index]["close"])
        if signal_close <= compression_high or signal_close <= vwap[index]:
            continue
        entry_index = index + 1
        entry_open = float(bars[entry_index]["open"])
        if entry_open <= compression_low:
            return {
                "date": day,
                "symbol": symbol,
                "status": "nonpositive_stop_distance",
                "trigger_time_et": bars[index]["time_et"],
            }
        target = entry_open + 2 * (entry_open - compression_low)
        if target <= entry_open:
            return {
                "date": day,
                "symbol": symbol,
                "status": "nonpositive_target_distance",
                "trigger_time_et": bars[index]["time_et"],
            }
        return {
            "date": day,
            "symbol": symbol,
            "status": "executable",
            "compression_ratio": compression_ratio,
            "trigger_index": index,
            "entry_index": entry_index,
            "trigger_time_et": bars[index]["time_et"],
            "entry_time_et": bars[entry_index]["time_et"],
            "entry_open": entry_open,
            "stop": compression_low,
            "target": target,
        }
    return {"date": day, "symbol": symbol, "status": "no_compression_breakout"}


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
        observed = continuation._bar_time(row)
        open_price = float(row["open"])
        if open_price <= stop:
            exit_price = open_price
            exit_reason = "stop_gap" if index > entry_index else "stop"
        elif observed >= FORCE_FLAT:
            exit_price = open_price
            exit_reason = "force_flat"
        else:
            stop_hit = float(row["low"]) <= stop
            target_hit = float(row["high"]) >= target
            if stop_hit:
                exit_price = stop
                exit_reason = "stop_first" if target_hit else "stop"
            elif target_hit:
                exit_price = target
                exit_reason = "target"
        if exit_price is not None:
            exit_time = str(row["time_et"])
            break
    if exit_price is None:
        raise VolatilityCompressionError("trade did not resolve by force-flat")
    cost = cost_bps / 10_000
    entry_fill = raw_entry * (1 + cost)
    exit_fill = exit_price * (1 - cost)
    stop_fill = stop * (1 - cost)
    planned_risk = entry_fill - stop_fill
    if planned_risk <= 0:
        raise VolatilityCompressionError("cost-adjusted planned risk is not positive")
    return {
        "cost_bps_per_side": cost_bps,
        "net_r": (exit_fill - entry_fill) / planned_risk,
        "exit_reason": exit_reason,
        "exit_time_et": exit_time,
        "stop_executed": exit_reason.startswith("stop"),
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
    sessions = continuation._load_inputs(manifest, source)
    dispositions: Counter[str] = Counter()
    records: list[dict[str, Any]] = []
    no_trade_dates = 0
    for session in sessions:
        day = str(session["date"])
        candidates = [
            _candidate(day=day, raw=item["raw"]) for item in session["candidates"]
        ]
        dispositions.update(item["status"] for item in candidates)
        executable = [item for item in candidates if item["status"] == "executable"]
        executable.sort(
            key=lambda item: (
                str(item["entry_time_et"]),
                float(item["compression_ratio"]),
                str(item["symbol"]),
            )
        )
        if not executable:
            no_trade_dates += 1
            continue
        selected = executable[0]
        dispositions["selected"] += 1
        dispositions["not_selected_daily_cap"] += len(executable) - 1
        raw = next(
            item["raw"]
            for item in session["candidates"]
            if item["symbol"] == selected["symbol"]
        )
        outcomes = {
            str(cost): _trade_outcome(selected, raw["bars"], cost)
            for cost in (PRIMARY_COST_BPS, *STRESS_COST_BPS)
        }
        primary = outcomes[str(PRIMARY_COST_BPS)]
        records.append(
            {
                "date": day,
                "symbol": selected["symbol"],
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
    primary_metrics = common._metrics(values)
    stress_metrics = {
        "10": common._metrics(stress_10),
        "20": common._metrics(stress_20),
    }
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
            "requested_dates": manifest["denominator"]["requested_dates"],
            "dates": manifest["denominator"]["included_dates"],
            "excluded_dates": manifest["denominator"]["excluded_dates"],
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
        raise VolatilityCompressionError("Stage 0 result content hash is invalid")
    rebuilt = build_result(
        manifest_path,
        inspection_path,
        store,
        require_published=False,
    )
    if recorded != rebuilt:
        raise VolatilityCompressionError(
            "Stage 0 result does not independently rebuild"
        )
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


def _publish(value: Mapping[str, Any], path: Path) -> None:
    if "inspection_kind" in value:
        identity = str(value.get("inspection_sha256", ""))
    elif "result_kind" in value:
        identity = str(value.get("result_sha256", ""))
    else:
        identity = str(value.get("manifest_sha256", ""))
    if not identity or not path.name.endswith(f"-{identity}.json"):
        raise VolatilityCompressionError(
            "output filename must end with its content hash"
        )
    resolved = path.resolve()
    allowed = {
        (PROJECT_ROOT / "strategy_tournament" / "activations").resolve(),
        (PROJECT_ROOT / "strategy_tournament" / "inspections").resolve(),
        (PROJECT_ROOT / "research_results").resolve(),
    }
    if resolved.parent not in allowed:
        raise VolatilityCompressionError(
            "output path is outside an approved evidence directory"
        )
    resolved.parent.mkdir(parents=True, exist_ok=True)
    temporary = resolved.with_name(f".{resolved.name}.{os.getpid()}.tmp")
    try:
        temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
        os.replace(temporary, resolved)
    finally:
        if temporary.exists():
            temporary.unlink()


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    freeze = subparsers.add_parser("freeze")
    freeze.add_argument("--output", type=Path)
    inspect = subparsers.add_parser("inspect")
    inspect.add_argument("manifest", type=Path)
    inspect.add_argument("--output", type=Path)
    evaluate = subparsers.add_parser("evaluate")
    evaluate.add_argument("manifest", type=Path)
    evaluate.add_argument("inspection", type=Path)
    evaluate.add_argument("--output", type=Path)
    result = subparsers.add_parser("inspect-result")
    result.add_argument("manifest", type=Path)
    result.add_argument("inspection", type=Path)
    result.add_argument("result", type=Path)
    result.add_argument("--output", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "freeze":
            value = build_manifest()
        elif args.command == "inspect":
            value = inspect_activation(args.manifest)
        elif args.command == "evaluate":
            value = build_result(args.manifest, args.inspection)
        else:
            value = inspect_result(args.manifest, args.inspection, args.result)
        if args.output:
            _publish(value, args.output)
    except (
        VolatilityCompressionError,
        continuation.EquityGapStage0Error,
        OSError,
    ) as exc:
        print(json.dumps({"status": "BLOCKED", "error": str(exc)}), file=sys.stderr)
        return 2
    print(json.dumps(value, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

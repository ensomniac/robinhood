"""Acquire, freeze, inspect, and evaluate broad ETF trend pullback Stage 0."""

from __future__ import annotations

import argparse
import json
import statistics
import sys
from collections.abc import Mapping, Sequence
from datetime import datetime
from pathlib import Path
from typing import Any

import etf_or_momentum_stage0 as common
import sector_etf_rotation_stage0 as daily
from historical_concurrency import ordered_bounded_results
from historical_service import RecordingHistoricalClient
from historical_store import HistoricalDayStore, sha256_file
from ibkr_historical import IBKRConfig, IBKRHistoricalClient


PROJECT_ROOT = Path(__file__).resolve().parent
DATA_ROOT = Path("/Users/ensomniac/trade/historical_data")
SLATE_PATH = daily.SLATE_PATH
SLATE_INSPECTION_PATH = daily.SLATE_INSPECTION_PATH
COLLECTION_STATUS_PATH = (
    PROJECT_ROOT
    / "strategy_tournament/second_wave/broad_etf_trend_pullback/collection-status.json"
)
PRIVATE_INPUT_PATH = (
    DATA_ROOT
    / "_derived/broad_etf_trend_pullback_stage0/"
    "dataset-broad-etf-trend-pullback-stage0-2026-07-21-v1/frozen-inputs.json.gz"
)
VARIANT_ID = "broad-etf-trend-pullback-v1"
SYMBOLS = ("SPY", "QQQ", "IWM", "DIA")
ACQUIRE_SYMBOLS = ("QQQ", "IWM", "DIA")
COLLECTION_START = daily.COLLECTION_START
EVALUATION_START = daily.EVALUATION_START
EVALUATION_END = daily.EVALUATION_END
COLLECTION_END_EXCLUSIVE = daily.COLLECTION_END_EXCLUSIVE
SCHEMA_VERSION = 1
PRIMARY_COST_BPS = 5
STRESS_COST_BPS = (10, 20)


class BroadEtfTrendPullbackError(RuntimeError):
    """The frozen broad-ETF pullback evidence is incomplete or inconsistent."""


def _load_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise BroadEtfTrendPullbackError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise BroadEtfTrendPullbackError(f"{path} must contain an object")
    return value


def _variant() -> dict[str, Any]:
    slate = _load_json(SLATE_PATH)
    if slate.get("manifest_sha256") != common._self_hash(slate, "manifest_sha256"):
        raise BroadEtfTrendPullbackError("second-wave slate hash is invalid")
    matches = [item for item in slate["variants"] if item["variant_id"] == VARIANT_ID]
    if len(matches) != 1:
        raise BroadEtfTrendPullbackError("broad ETF variant is missing from the slate")
    inspection = _load_json(SLATE_INSPECTION_PATH)
    if (
        inspection.get("inspection_sha256")
        != common._self_hash(inspection, "inspection_sha256")
        or inspection.get("manifest_sha256") != slate["manifest_sha256"]
        or inspection.get("return_evaluation_authorized") is not True
    ):
        raise BroadEtfTrendPullbackError("second-wave slate inspection is invalid")
    return dict(matches[0])


def _dataset(store: HistoricalDayStore, symbol: str, day: str) -> dict[str, Any] | None:
    return daily._dataset(store, symbol, day)


def acquire_inputs(store: HistoricalDayStore | None = None) -> dict[str, Any]:
    """Collect missing daily bars without selecting signals or computing returns."""

    _variant()
    target_store = store or HistoricalDayStore.from_env()
    config = IBKRConfig.from_env()
    completed: list[dict[str, Any]] = []
    failures: list[dict[str, str]] = []
    with IBKRHistoricalClient(config) as raw_client:
        recorder = RecordingHistoricalClient(raw_client, target_store)

        def collect(symbol: str) -> int:
            rows = recorder.fetch_bars(
                symbol,
                f"{COLLECTION_START}T00:00:00-05:00",
                f"{COLLECTION_END_EXCLUSIVE}T00:00:00-05:00",
                bar_size="1 day",
                what="TRADES",
                use_rth=True,
            )
            return len(rows)

        for outcome in ordered_bounded_results(
            ACQUIRE_SYMBOLS, collect, max_workers=config.max_concurrent_requests
        ):
            if outcome.error is not None:
                failures.append({"symbol": outcome.item, "error": str(outcome.error)})
            else:
                completed.append(
                    {"symbol": outcome.item, "rows": int(outcome.value or 0)}
                )
        telemetry = raw_client.request_telemetry()
    common_dates = set(target_store.dates(SYMBOLS[0]))
    for symbol in SYMBOLS[1:]:
        common_dates &= set(target_store.dates(symbol))
    qualified = [
        day
        for day in sorted(common_dates)
        if COLLECTION_START <= day < COLLECTION_END_EXCLUSIVE
        and all(_dataset(target_store, symbol, day) is not None for symbol in SYMBOLS)
    ]
    status = {
        "schema_version": SCHEMA_VERSION,
        "collection_kind": "broad-etf-trend-pullback-stage0-daily-inputs",
        "variant_id": VARIANT_ID,
        "symbols": list(SYMBOLS),
        "requested_symbols": list(ACQUIRE_SYMBOLS),
        "collection_start": COLLECTION_START,
        "collection_end_exclusive": COLLECTION_END_EXCLUSIVE,
        "provider": "IBKR",
        "timeframe": "1d",
        "completed": completed,
        "failures": failures,
        "qualified_common_dates": len(qualified),
        "first_common_date": qualified[0] if qualified else None,
        "last_common_date": qualified[-1] if qualified else None,
        "provider_telemetry": telemetry,
        "signal_selections": 0,
        "returns_computed": 0,
        "broker_actions": 0,
        "valid": not failures and len(qualified) >= 800,
    }
    daily._write_json(status, COLLECTION_STATUS_PATH)
    if not status["valid"]:
        raise BroadEtfTrendPullbackError("broad ETF daily collection is incomplete")
    return status


def _build_input_graph(store: HistoricalDayStore) -> dict[str, Any]:
    common_dates = set(store.dates(SYMBOLS[0]))
    for symbol in SYMBOLS[1:]:
        common_dates &= set(store.dates(symbol))
    dates: list[str] = []
    rows_by_symbol: dict[str, list[dict[str, Any]]] = {symbol: [] for symbol in SYMBOLS}
    dataset_ids_by_symbol: dict[str, list[str]] = {symbol: [] for symbol in SYMBOLS}
    for day in sorted(common_dates):
        if not COLLECTION_START <= day < COLLECTION_END_EXCLUSIVE:
            continue
        datasets = {symbol: _dataset(store, symbol, day) for symbol in SYMBOLS}
        if any(value is None for value in datasets.values()):
            continue
        dates.append(day)
        for symbol in SYMBOLS:
            dataset = datasets[symbol]
            assert dataset is not None
            row = daily._validate_daily_row(dataset["rows"][0], symbol, day)
            rows_by_symbol[symbol].append(row)
            dataset_ids_by_symbol[symbol].append(str(dataset["id"]))
    evaluation_dates = [
        day for day in dates if EVALUATION_START <= day <= EVALUATION_END
    ]
    positions = {day: index for index, day in enumerate(dates)}
    if not evaluation_dates:
        raise BroadEtfTrendPullbackError("no frozen evaluation dates are available")
    for day in evaluation_dates:
        index = positions[day]
        if index < 200 or index + 5 >= len(dates):
            raise BroadEtfTrendPullbackError(
                f"{day} lacks frozen lookback or outcome bars"
            )
    graph: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "dataset_id": "dataset-broad-etf-trend-pullback-stage0-2026-07-21-v1",
        "variant_id": VARIANT_ID,
        "symbols": list(SYMBOLS),
        "dates": dates,
        "evaluation_dates": evaluation_dates,
        "rows_by_symbol": rows_by_symbol,
        "dataset_ids_by_symbol": dataset_ids_by_symbol,
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
    target_store = store or HistoricalDayStore.from_env()
    graph = _build_input_graph(target_store)
    if write_private:
        daily._write_gzip_json(graph, PRIVATE_INPUT_PATH)
    if not PRIVATE_INPUT_PATH.is_file():
        raise BroadEtfTrendPullbackError("private frozen input graph is missing")
    recorded_graph = daily._load_gzip_json(PRIVATE_INPUT_PATH)
    if recorded_graph != graph:
        raise BroadEtfTrendPullbackError("private frozen input graph does not rebuild")
    activation: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "contract_kind": "stage0-activation",
        "campaign_id": "multi-strategy-portfolio-validation-v1",
        "tournament_wave": 2,
        "variant_ordinal": 2,
        "variant_id": VARIANT_ID,
        "strategy_version": variant["version"],
        "mechanism_family": variant["mechanism_family"],
        "base_rules_hash": variant["rules_hash"],
        "slate_manifest_sha256": _load_json(SLATE_PATH)["manifest_sha256"],
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
            "symbols": list(SYMBOLS),
            "whole_provider": "ibkr",
            "provider_substitution": False,
            "common_calendar_dates": len(graph["dates"]),
            "evaluation_dates": len(graph["evaluation_dates"]),
            "evaluation_start": EVALUATION_START,
            "evaluation_end": EVALUATION_END,
            "input_sha256": graph["input_sha256"],
            "private_input_file_sha256": sha256_file(PRIVATE_INPUT_PATH),
        },
        "selection_contract": {
            **variant["signal"],
            "rsi2_calculation": (
                "Wilder smoothing initialized from the first two close changes"
            ),
        },
        "outcome_contract": {
            **variant["execution"],
            **variant["exit"],
            "planned_risk_r": (
                "cost-adjusted entry minus cost-adjusted planned stop"
            ),
        },
        "stage0_gate": _load_json(SLATE_PATH)["stage0_falsification"],
        "denominator": {
            "decision_dates": len(graph["evaluation_dates"]),
            "input_symbol_dates": len(graph["dates"]) * len(SYMBOLS),
            "preserve_no_trade_dates": True,
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
    activation["manifest_sha256"] = common._self_hash(
        activation, "manifest_sha256"
    )
    return activation


def default_activation_path(activation: Mapping[str, Any]) -> Path:
    return (
        PROJECT_ROOT
        / "strategy_tournament/second_wave/activations/"
        f"{VARIANT_ID}-{activation['manifest_sha256']}.json"
    )


def inspect_activation(path: Path) -> dict[str, Any]:
    recorded = _load_json(path)
    if recorded.get("manifest_sha256") != common._self_hash(
        recorded, "manifest_sha256"
    ):
        raise BroadEtfTrendPullbackError("activation content hash is invalid")
    if recorded != build_activation():
        raise BroadEtfTrendPullbackError("activation does not rebuild")
    graph = daily._load_gzip_json(PRIVATE_INPUT_PATH)
    if graph.get("input_sha256") != common._self_hash(graph, "input_sha256"):
        raise BroadEtfTrendPullbackError("private input content hash is invalid")
    if graph["input_sha256"] != recorded["source_selection"]["input_sha256"]:
        raise BroadEtfTrendPullbackError("activation input identity drifted")
    if sha256_file(PRIVATE_INPUT_PATH) != recorded["source_selection"][
        "private_input_file_sha256"
    ]:
        raise BroadEtfTrendPullbackError("private input file hash drifted")
    for symbol in SYMBOLS:
        rows = graph["rows_by_symbol"][symbol]
        if len(rows) != len(graph["dates"]):
            raise BroadEtfTrendPullbackError(f"{symbol} input denominator drifted")
        for day, row in zip(graph["dates"], rows, strict=True):
            daily._validate_daily_row(row, symbol, day)
    inspection: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "inspection_kind": "stage0-activation-input-inspection",
        "variant_id": VARIANT_ID,
        "manifest_sha256": recorded["manifest_sha256"],
        "manifest_file_sha256": sha256_file(path),
        "input_sha256": graph["input_sha256"],
        "input_symbol_dates": len(graph["dates"]) * len(SYMBOLS),
        "evaluation_dates": len(graph["evaluation_dates"]),
        "provider_requests": 0,
        "broker_actions": 0,
        "returns_computed": 0,
        "maturity_effect": "NONE",
        "return_evaluation_authorized": True,
        "valid": True,
    }
    inspection["inspection_sha256"] = common._self_hash(
        inspection, "inspection_sha256"
    )
    return inspection


def _rsi_wilder(values: Sequence[float], length: int = 2) -> list[float | None]:
    if len(values) <= length:
        raise BroadEtfTrendPullbackError("RSI lookback is incomplete")
    result: list[float | None] = [None] * len(values)
    changes = [values[index] - values[index - 1] for index in range(1, len(values))]
    average_gain = statistics.fmean(max(change, 0.0) for change in changes[:length])
    average_loss = statistics.fmean(max(-change, 0.0) for change in changes[:length])

    def value() -> float:
        if average_loss == 0:
            return 100.0 if average_gain > 0 else 50.0
        relative_strength = average_gain / average_loss
        return 100 - 100 / (1 + relative_strength)

    result[length] = value()
    for index in range(length + 1, len(values)):
        change = changes[index - 1]
        average_gain = (average_gain * (length - 1) + max(change, 0.0)) / length
        average_loss = (average_loss * (length - 1) + max(-change, 0.0)) / length
        result[index] = value()
    return result


def _outcome(
    rows: Sequence[Mapping[str, Any]], signal_index: int, cost_bps: int
) -> dict[str, Any]:
    entry_index = signal_index + 1
    final_index = entry_index + 4
    raw_entry = float(rows[entry_index]["o"])
    stop = raw_entry - 1.5 * daily._atr(rows, signal_index)
    if stop <= 0 or stop >= raw_entry:
        raise BroadEtfTrendPullbackError("planned stop is invalid")
    closes = [float(row["c"]) for row in rows]
    exit_price = float(rows[final_index]["c"])
    exit_reason = "time_exit"
    exit_index = final_index
    for index in range(entry_index, final_index + 1):
        row = rows[index]
        open_price = float(row["o"])
        if open_price <= stop:
            exit_price = open_price
            exit_reason = "stop_gap"
        elif float(row["l"]) <= stop:
            exit_price = stop
            exit_reason = "stop"
        elif closes[index] >= daily._sma(closes, index, 5):
            exit_price = closes[index]
            exit_reason = "recovery_exit"
        elif index < final_index:
            continue
        exit_index = index
        break
    cost = cost_bps / 10_000
    entry_fill = raw_entry * (1 + cost)
    exit_fill = exit_price * (1 - cost)
    stop_fill = stop * (1 - cost)
    planned_risk = entry_fill - stop_fill
    return {
        "net_r": (exit_fill - entry_fill) / planned_risk,
        "exit_reason": exit_reason,
        "exit_date": datetime.fromisoformat(str(rows[exit_index]["t"]))
        .date()
        .isoformat(),
        "exit_index": exit_index,
        "stop_executed": exit_reason.startswith("stop"),
    }


def _require_published(paths: Sequence[Path]) -> None:
    daily._require_published(paths)


def build_result(
    activation_path: Path,
    inspection_path: Path,
    *,
    require_published: bool = True,
) -> dict[str, Any]:
    activation = _load_json(activation_path)
    inspection = _load_json(inspection_path)
    if inspection != inspect_activation(activation_path):
        raise BroadEtfTrendPullbackError("input inspection does not rebuild")
    if require_published:
        _require_published((activation_path, inspection_path))
    graph = daily._load_gzip_json(PRIVATE_INPUT_PATH)
    dates = list(graph["dates"])
    positions = {day: index for index, day in enumerate(dates)}
    rows_by_symbol = graph["rows_by_symbol"]
    closes = {
        symbol: [float(row["c"]) for row in rows_by_symbol[symbol]]
        for symbol in SYMBOLS
    }
    rsi = {symbol: _rsi_wilder(closes[symbol]) for symbol in SYMBOLS}
    records: list[dict[str, Any]] = []
    no_trade_dates = 0
    position_occupied_dates = 0
    next_flat_index = 0
    for day in graph["evaluation_dates"]:
        index = positions[day]
        if index < next_flat_index:
            position_occupied_dates += 1
            continue
        candidates: list[tuple[float, float, str]] = []
        for symbol in SYMBOLS:
            current_rsi = rsi[symbol][index]
            three_session_return = closes[symbol][index] / closes[symbol][index - 3] - 1
            if closes[symbol][index] <= daily._sma(closes[symbol], index, 200):
                continue
            if three_session_return > -0.01:
                continue
            if current_rsi is None or current_rsi > 15:
                continue
            candidates.append((current_rsi, three_session_return, symbol))
        if not candidates:
            no_trade_dates += 1
            continue
        candidates.sort(key=lambda item: (item[0], item[1], item[2]))
        selected_rsi, selected_return, symbol = candidates[0]
        outcomes = {
            str(cost): _outcome(rows_by_symbol[symbol], index, cost)
            for cost in (PRIMARY_COST_BPS, *STRESS_COST_BPS)
        }
        primary = outcomes[str(PRIMARY_COST_BPS)]
        records.append(
            {
                "signal_date": day,
                "symbol": symbol,
                "rsi2": selected_rsi,
                "three_session_return": selected_return,
                "entry_date": dates[index + 1],
                "exit_date": primary["exit_date"],
                "exit_reason": primary["exit_reason"],
                "stop_executed": primary["stop_executed"],
                "net_r": primary["net_r"],
                "stress_10bps_r": outcomes["10"]["net_r"],
                "stress_20bps_r": outcomes["20"]["net_r"],
            }
        )
        next_flat_index = int(primary["exit_index"])
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
            "decision_dates": len(graph["evaluation_dates"]),
            "closed_signals": len(records),
            "no_trade_dates": no_trade_dates,
            "position_occupied_dates": position_occupied_dates,
            "rule_violations": 0,
        },
        "primary_5bps": primary_metrics,
        "stress": stress,
        "stage0_survived": not blockers,
        "stage0_blockers": blockers,
        "next_action": (
            "freeze representative development without changing rules"
            if not blockers
            else "retire this exact variant and advance to the next frozen mechanism"
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
        raise BroadEtfTrendPullbackError("result content hash is invalid")
    if recorded != build_result(
        activation_path, inspection_path, require_published=False
    ):
        raise BroadEtfTrendPullbackError("result does not independently rebuild")
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
    inspection["inspection_sha256"] = common._self_hash(
        inspection, "inspection_sha256"
    )
    return inspection


def _one(pattern: str, description: str) -> Path:
    matches = sorted(PROJECT_ROOT.glob(pattern))
    if len(matches) != 1:
        raise BroadEtfTrendPullbackError(
            f"expected one {description}; found {len(matches)}"
        )
    return matches[0]


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "command",
        choices=("acquire", "freeze", "inspect-inputs", "evaluate", "inspect-result"),
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "acquire":
            result = acquire_inputs()
            output = {
                key: result[key]
                for key in (
                    "valid",
                    "qualified_common_dates",
                    "first_common_date",
                    "last_common_date",
                )
            }
        elif args.command == "freeze":
            result = build_activation(write_private=True)
            path = default_activation_path(result)
            daily._write_json(result, path)
            output = {
                "manifest_sha256": result["manifest_sha256"],
                "written": str(path.relative_to(PROJECT_ROOT)),
            }
        elif args.command == "inspect-inputs":
            activation_path = _one(
                "strategy_tournament/second_wave/activations/"
                "broad-etf-trend-pullback-v1-*.json",
                "broad ETF activation",
            )
            result = inspect_activation(activation_path)
            path = (
                PROJECT_ROOT
                / "strategy_tournament/second_wave/inspections/"
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
                "broad-etf-trend-pullback-v1-*.json",
                "broad ETF activation",
            )
            inspection_path = _one(
                "strategy_tournament/second_wave/inspections/"
                "broad-etf-trend-pullback-v1-input-*.json",
                "broad ETF input inspection",
            )
            result = build_result(activation_path, inspection_path)
            path = (
                PROJECT_ROOT
                / "research_results/"
                f"2026-07-21-broad-etf-trend-pullback-stage0-"
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
                "broad-etf-trend-pullback-v1-*.json",
                "broad ETF activation",
            )
            inspection_path = _one(
                "strategy_tournament/second_wave/inspections/"
                "broad-etf-trend-pullback-v1-input-*.json",
                "broad ETF input inspection",
            )
            result_path = _one(
                "research_results/2026-07-21-broad-etf-trend-pullback-stage0-*.json",
                "broad ETF Stage 0 result",
            )
            result = inspect_result(activation_path, inspection_path, result_path)
            path = (
                PROJECT_ROOT
                / "strategy_tournament/second_wave/inspections/"
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

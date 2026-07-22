"""Acquire, freeze, inspect, and evaluate sector ETF rotation Stage 0."""

from __future__ import annotations

import argparse
import gzip
import json
import math
import os
import statistics
import subprocess
import sys
from collections.abc import Mapping, Sequence
from datetime import datetime
from pathlib import Path
from typing import Any

import etf_or_momentum_stage0 as common
from historical_concurrency import ordered_bounded_results
from historical_service import RecordingHistoricalClient
from historical_store import HistoricalDayStore, sha256_file
from ibkr_historical import IBKRConfig, IBKRHistoricalClient
from second_wave_slate import SECTOR_ETFS


PROJECT_ROOT = Path(__file__).resolve().parent
DATA_ROOT = Path("/Users/ensomniac/trade/historical_data")
SLATE_PATH = (
    PROJECT_ROOT
    / "strategy_tournament/second_wave/manifests/"
    "portfolio-stage0-second-wave-slate-"
    "4c513f9de6417a48dccae5bf5ce1e5c59d68a1d2d0b6e65bba41a3f4a5cac822.json"
)
SLATE_INSPECTION_PATH = (
    PROJECT_ROOT
    / "strategy_tournament/second_wave/inspections/"
    "portfolio-stage0-second-wave-slate-"
    "3c12ba6f0ea4a49db5888904015a73d42f54cf1da2a43447ece8b741d1e2c6c0.json"
)
COLLECTION_STATUS_PATH = (
    PROJECT_ROOT / "strategy_tournament/second_wave/sector_etf_rotation/collection-status.json"
)
PRIVATE_INPUT_PATH = (
    DATA_ROOT
    / "_derived/sector_etf_rotation_stage0/"
    "dataset-sector-etf-rotation-stage0-2026-07-21-v1/frozen-inputs.json.gz"
)
VARIANT_ID = "sector-etf-rotation-v1"
SYMBOLS = ("SPY", *SECTOR_ETFS)
COLLECTION_START = "2022-01-03"
EVALUATION_START = "2023-01-03"
EVALUATION_END = "2025-12-31"
COLLECTION_END_EXCLUSIVE = "2026-01-09"
SCHEMA_VERSION = 1
PRIMARY_COST_BPS = 5
STRESS_COST_BPS = (10, 20)


class SectorEtfRotationError(RuntimeError):
    """The frozen sector-rotation evidence is incomplete or inconsistent."""


def _load_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SectorEtfRotationError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise SectorEtfRotationError(f"{path} must contain an object")
    return value


def _load_gzip_json(path: Path) -> dict[str, Any]:
    try:
        with gzip.open(path, "rt", encoding="utf-8") as stream:
            value = json.load(stream)
    except (OSError, json.JSONDecodeError) as exc:
        raise SectorEtfRotationError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise SectorEtfRotationError(f"{path} must contain an object")
    return value


def _write_json(value: Mapping[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


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


def _variant() -> dict[str, Any]:
    slate = _load_json(SLATE_PATH)
    if slate.get("manifest_sha256") != common._self_hash(slate, "manifest_sha256"):
        raise SectorEtfRotationError("second-wave slate hash is invalid")
    matches = [item for item in slate["variants"] if item["variant_id"] == VARIANT_ID]
    if len(matches) != 1:
        raise SectorEtfRotationError("sector ETF variant is missing from the slate")
    inspection = _load_json(SLATE_INSPECTION_PATH)
    if (
        inspection.get("inspection_sha256")
        != common._self_hash(inspection, "inspection_sha256")
        or inspection.get("manifest_sha256") != slate["manifest_sha256"]
        or inspection.get("return_evaluation_authorized") is not True
    ):
        raise SectorEtfRotationError("second-wave slate inspection is invalid")
    return dict(matches[0])


def _dataset(store: HistoricalDayStore, symbol: str, day: str) -> dict[str, Any] | None:
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


def acquire_inputs(store: HistoricalDayStore | None = None) -> dict[str, Any]:
    """Collect daily bars without selecting signals or computing returns."""

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
            SYMBOLS, collect, max_workers=config.max_concurrent_requests
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
        "collection_kind": "sector-etf-rotation-stage0-daily-inputs",
        "variant_id": VARIANT_ID,
        "symbols": list(SYMBOLS),
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
    _write_json(status, COLLECTION_STATUS_PATH)
    if not status["valid"]:
        raise SectorEtfRotationError("sector ETF daily input collection is incomplete")
    return status


def _validate_daily_row(raw: Any, symbol: str, day: str) -> dict[str, Any]:
    if not isinstance(raw, Mapping):
        raise SectorEtfRotationError(f"{symbol} {day} daily row is malformed")
    row = dict(raw)
    try:
        open_price = float(row["o"])
        high = float(row["h"])
        low = float(row["l"])
        close = float(row["c"])
        volume = int(row["v"])
        observed = datetime.fromisoformat(str(row["t"]))
    except (KeyError, TypeError, ValueError) as exc:
        raise SectorEtfRotationError(f"{symbol} {day} daily row fields are invalid") from exc
    if observed.date().isoformat() != day or observed.tzinfo is None:
        raise SectorEtfRotationError(f"{symbol} {day} daily timestamp drifted")
    if not all(math.isfinite(value) and value > 0 for value in (open_price, high, low, close)):
        raise SectorEtfRotationError(f"{symbol} {day} has nonpositive prices")
    if low > min(open_price, close) or high < max(open_price, close) or low > high:
        raise SectorEtfRotationError(f"{symbol} {day} has invalid OHLC ordering")
    if volume < 0 or row.get("i") is not False:
        raise SectorEtfRotationError(f"{symbol} {day} has unusable volume")
    return row


def _build_input_graph(store: HistoricalDayStore) -> dict[str, Any]:
    common_dates = set(store.dates(SYMBOLS[0]))
    for symbol in SYMBOLS[1:]:
        common_dates &= set(store.dates(symbol))
    dates = []
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
            row = _validate_daily_row(dataset["rows"][0], symbol, day)
            rows_by_symbol[symbol].append(row)
            dataset_ids_by_symbol[symbol].append(str(dataset["id"]))
    evaluation_dates = [
        day for day in dates if EVALUATION_START <= day <= EVALUATION_END
    ]
    positions = {day: index for index, day in enumerate(dates)}
    if not evaluation_dates:
        raise SectorEtfRotationError("no frozen evaluation dates are available")
    for day in evaluation_dates:
        index = positions[day]
        if index < 50 or index + 5 >= len(dates):
            raise SectorEtfRotationError(f"{day} lacks frozen lookback or outcome bars")
    graph: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "dataset_id": "dataset-sector-etf-rotation-stage0-2026-07-21-v1",
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
        _write_gzip_json(graph, PRIVATE_INPUT_PATH)
    if not PRIVATE_INPUT_PATH.is_file():
        raise SectorEtfRotationError("private frozen input graph is missing")
    recorded_graph = _load_gzip_json(PRIVATE_INPUT_PATH)
    if recorded_graph != graph:
        raise SectorEtfRotationError("private frozen input graph does not rebuild")
    activation: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "contract_kind": "stage0-activation",
        "campaign_id": "multi-strategy-portfolio-validation-v1",
        "tournament_wave": 2,
        "variant_ordinal": 1,
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
        "selection_contract": variant["signal"],
        "outcome_contract": {
            **variant["execution"],
            **variant["exit"],
            "planned_risk_r": "cost-adjusted entry minus cost-adjusted planned stop",
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
        raise SectorEtfRotationError("activation content hash is invalid")
    rebuilt = build_activation()
    if recorded != rebuilt:
        raise SectorEtfRotationError("activation does not rebuild")
    graph = _load_gzip_json(PRIVATE_INPUT_PATH)
    if graph.get("input_sha256") != common._self_hash(graph, "input_sha256"):
        raise SectorEtfRotationError("private input content hash is invalid")
    if graph["input_sha256"] != recorded["source_selection"]["input_sha256"]:
        raise SectorEtfRotationError("activation input identity drifted")
    if sha256_file(PRIVATE_INPUT_PATH) != recorded["source_selection"][
        "private_input_file_sha256"
    ]:
        raise SectorEtfRotationError("private input file hash drifted")
    for symbol in SYMBOLS:
        rows = graph["rows_by_symbol"][symbol]
        if len(rows) != len(graph["dates"]):
            raise SectorEtfRotationError(f"{symbol} input denominator drifted")
        for day, row in zip(graph["dates"], rows, strict=True):
            _validate_daily_row(row, symbol, day)
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


def _sma(values: Sequence[float], end: int, length: int) -> float:
    if end + 1 < length:
        raise SectorEtfRotationError("moving-average lookback is incomplete")
    return statistics.fmean(values[end - length + 1 : end + 1])


def _atr(rows: Sequence[Mapping[str, Any]], end: int, length: int = 14) -> float:
    if end < length:
        raise SectorEtfRotationError("ATR lookback is incomplete")
    ranges = []
    for index in range(end - length + 1, end + 1):
        high = float(rows[index]["h"])
        low = float(rows[index]["l"])
        previous_close = float(rows[index - 1]["c"])
        ranges.append(max(high - low, abs(high - previous_close), abs(low - previous_close)))
    return statistics.fmean(ranges)


def _outcome(
    rows: Sequence[Mapping[str, Any]], signal_index: int, cost_bps: int
) -> dict[str, Any]:
    entry_index = signal_index + 1
    exit_index = entry_index + 4
    raw_entry = float(rows[entry_index]["o"])
    stop = raw_entry - 1.5 * _atr(rows, signal_index)
    if stop <= 0 or stop >= raw_entry:
        raise SectorEtfRotationError("planned stop is invalid")
    exit_price = float(rows[exit_index]["c"])
    exit_reason = "time_exit"
    exit_day = datetime.fromisoformat(str(rows[exit_index]["t"])).date().isoformat()
    for index in range(entry_index, exit_index + 1):
        row = rows[index]
        open_price = float(row["o"])
        if open_price <= stop:
            exit_price = open_price
            exit_reason = "stop_gap"
        elif float(row["l"]) <= stop:
            exit_price = stop
            exit_reason = "stop"
        else:
            continue
        exit_day = datetime.fromisoformat(str(row["t"])).date().isoformat()
        break
    cost = cost_bps / 10_000
    entry_fill = raw_entry * (1 + cost)
    exit_fill = exit_price * (1 - cost)
    stop_fill = stop * (1 - cost)
    planned_risk = entry_fill - stop_fill
    return {
        "net_r": (exit_fill - entry_fill) / planned_risk,
        "exit_reason": exit_reason,
        "exit_date": exit_day,
        "stop_executed": exit_reason.startswith("stop"),
    }


def _git(*args: str) -> str:
    completed = subprocess.run(
        ["git", *args], cwd=PROJECT_ROOT, check=False, capture_output=True, text=True
    )
    if completed.returncode:
        raise SectorEtfRotationError(completed.stderr.strip() or "Git command failed")
    return completed.stdout.strip()


def _require_published(paths: Sequence[Path]) -> None:
    if _git("status", "--porcelain", "--untracked-files=all"):
        raise SectorEtfRotationError("evaluation requires a clean worktree")
    upstream = _git("rev-parse", "--abbrev-ref", "--symbolic-full-name", "@{u}")
    if _git("rev-parse", "HEAD") != _git("rev-parse", upstream):
        raise SectorEtfRotationError("evaluation requires HEAD to equal upstream")
    for path in paths:
        relative = path.resolve().relative_to(PROJECT_ROOT).as_posix()
        _git("ls-files", "--error-unmatch", relative)


def build_result(
    activation_path: Path,
    inspection_path: Path,
    *,
    require_published: bool = True,
) -> dict[str, Any]:
    activation = _load_json(activation_path)
    inspection = _load_json(inspection_path)
    if inspection != inspect_activation(activation_path):
        raise SectorEtfRotationError("input inspection does not rebuild")
    if require_published:
        _require_published((activation_path, inspection_path))
    graph = _load_gzip_json(PRIVATE_INPUT_PATH)
    dates = list(graph["dates"])
    positions = {day: index for index, day in enumerate(dates)}
    rows_by_symbol = graph["rows_by_symbol"]
    closes = {
        symbol: [float(row["c"]) for row in rows_by_symbol[symbol]]
        for symbol in SYMBOLS
    }
    records = []
    no_trade_dates = 0
    next_flat_index = 0
    for day in graph["evaluation_dates"]:
        index = positions[day]
        if index < next_flat_index:
            continue
        spy_return = closes["SPY"][index] / closes["SPY"][index - 20] - 1
        candidates = []
        for symbol in SECTOR_ETFS:
            momentum = closes[symbol][index] / closes[symbol][index - 20] - 1
            if closes[symbol][index] <= _sma(closes[symbol], index, 50):
                continue
            if momentum <= spy_return:
                continue
            candidates.append((momentum, symbol))
        if not candidates:
            no_trade_dates += 1
            continue
        candidates.sort(key=lambda item: (-item[0], item[1]))
        symbol = candidates[0][1]
        outcomes = {
            str(cost): _outcome(rows_by_symbol[symbol], index, cost)
            for cost in (PRIMARY_COST_BPS, *STRESS_COST_BPS)
        }
        primary = outcomes[str(PRIMARY_COST_BPS)]
        records.append(
            {
                "signal_date": day,
                "symbol": symbol,
                "entry_date": dates[index + 1],
                "exit_date": primary["exit_date"],
                "exit_reason": primary["exit_reason"],
                "stop_executed": primary["stop_executed"],
                "net_r": primary["net_r"],
                "stress_10bps_r": outcomes["10"]["net_r"],
                "stress_20bps_r": outcomes["20"]["net_r"],
            }
        )
        next_flat_index = index + 5
    primary = common._metrics([float(item["net_r"]) for item in records])
    stress = {
        "10": common._metrics([float(item["stress_10bps_r"]) for item in records]),
        "20": common._metrics([float(item["stress_20bps_r"]) for item in records]),
    }
    gate = activation["stage0_gate"]
    blockers = []
    if len(records) < int(gate["minimum_closed_signals"]):
        blockers.append("closed signals are below the Stage 0 minimum")
    if primary["expectancy_r"] is None or primary["expectancy_r"] <= 0:
        blockers.append("primary expectancy is not positive")
    if not primary["profit_factor_infinite"] and (
        primary["profit_factor"] is None
        or primary["profit_factor"] < float(gate["minimum_profit_factor"])
    ):
        blockers.append("primary profit factor is below the Stage 0 minimum")
    if primary["maximum_drawdown_r"] > float(gate["maximum_drawdown_r"]):
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
            "rule_violations": 0,
        },
        "primary_5bps": primary,
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
    if recorded.get("result_sha256") != common._self_hash(
        recorded, "result_sha256"
    ):
        raise SectorEtfRotationError("result content hash is invalid")
    rebuilt = build_result(
        activation_path, inspection_path, require_published=False
    )
    if recorded != rebuilt:
        raise SectorEtfRotationError("result does not independently rebuild")
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
        raise SectorEtfRotationError(f"expected one {description}; found {len(matches)}")
    return matches[0]


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "command", choices=("acquire", "freeze", "inspect-inputs", "evaluate", "inspect-result")
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "acquire":
            result = acquire_inputs()
            output = {key: result[key] for key in (
                "valid", "qualified_common_dates", "first_common_date", "last_common_date"
            )}
        elif args.command == "freeze":
            result = build_activation(write_private=True)
            path = default_activation_path(result)
            _write_json(result, path)
            output = {"manifest_sha256": result["manifest_sha256"], "written": str(path.relative_to(PROJECT_ROOT))}
        elif args.command == "inspect-inputs":
            activation_path = _one(
                "strategy_tournament/second_wave/activations/sector-etf-rotation-v1-*.json",
                "sector ETF activation",
            )
            result = inspect_activation(activation_path)
            path = (
                PROJECT_ROOT / "strategy_tournament/second_wave/inspections/"
                f"sector-etf-rotation-v1-input-{result['inspection_sha256']}.json"
            )
            _write_json(result, path)
            output = {"inspection_sha256": result["inspection_sha256"], "written": str(path.relative_to(PROJECT_ROOT))}
        elif args.command == "evaluate":
            activation_path = _one(
                "strategy_tournament/second_wave/activations/sector-etf-rotation-v1-*.json",
                "sector ETF activation",
            )
            inspection_path = _one(
                "strategy_tournament/second_wave/inspections/sector-etf-rotation-v1-input-*.json",
                "sector ETF input inspection",
            )
            result = build_result(activation_path, inspection_path)
            path = (
                PROJECT_ROOT / "research_results/"
                f"2026-07-21-sector-etf-rotation-stage0-{result['result_sha256']}.json"
            )
            _write_json(result, path)
            output = {
                "result_sha256": result["result_sha256"],
                "stage0_survived": result["stage0_survived"],
                "closed_signals": result["denominator"]["closed_signals"],
                "written": str(path.relative_to(PROJECT_ROOT)),
            }
        else:
            activation_path = _one(
                "strategy_tournament/second_wave/activations/sector-etf-rotation-v1-*.json",
                "sector ETF activation",
            )
            inspection_path = _one(
                "strategy_tournament/second_wave/inspections/sector-etf-rotation-v1-input-*.json",
                "sector ETF input inspection",
            )
            result_path = _one(
                "research_results/2026-07-21-sector-etf-rotation-stage0-*.json",
                "sector ETF Stage 0 result",
            )
            result = inspect_result(activation_path, inspection_path, result_path)
            path = (
                PROJECT_ROOT / "strategy_tournament/second_wave/inspections/"
                f"sector-etf-rotation-v1-result-{result['inspection_sha256']}.json"
            )
            _write_json(result, path)
            output = {"inspection_sha256": result["inspection_sha256"], "written": str(path.relative_to(PROJECT_ROOT))}
        print(json.dumps(output, indent=2, sort_keys=True))
        return 0
    except (OSError, SectorEtfRotationError, ValueError) as exc:
        print(
            json.dumps({"error": str(exc), "error_type": type(exc).__name__}, indent=2),
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

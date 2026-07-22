"""Acquire, freeze, inspect, and evaluate close-to-open ETF momentum Stage 0."""

from __future__ import annotations

import argparse
import json
import math
import statistics
import sys
from collections.abc import Mapping, Sequence
from datetime import datetime, time
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import broad_etf_trend_pullback_stage0 as broad
import etf_or_momentum_stage0 as common
import sector_etf_rotation_stage0 as daily
from historical_concurrency import ordered_bounded_results
from historical_service import RecordingHistoricalClient
from historical_store import HistoricalDayStore, sha256_file
from ibkr_historical import IBKRConfig, IBKRHistoricalClient


PROJECT_ROOT = Path(__file__).resolve().parent
DATA_ROOT = Path("/Users/ensomniac/trade/historical_data")
EASTERN = ZoneInfo("America/New_York")
SLATE_PATH = daily.SLATE_PATH
SLATE_INSPECTION_PATH = daily.SLATE_INSPECTION_PATH
COLLECTION_STATUS_PATH = (
    PROJECT_ROOT
    / "strategy_tournament/second_wave/close_to_open_etf_momentum/"
    "collection-status.json"
)
PRIVATE_INPUT_PATH = (
    DATA_ROOT
    / "_derived/close_to_open_etf_momentum_stage0/"
    "dataset-close-to-open-etf-momentum-stage0-2026-07-21-v1/"
    "frozen-inputs.json.gz"
)
VARIANT_ID = "close-to-open-etf-momentum-v1"
SYMBOLS = broad.SYMBOLS
COLLECTION_START = daily.COLLECTION_START
EVALUATION_START = daily.EVALUATION_START
EVALUATION_END = daily.EVALUATION_END
COLLECTION_END_EXCLUSIVE = daily.COLLECTION_END_EXCLUSIVE
SCHEMA_VERSION = 1
PRIMARY_COST_BPS = 5
STRESS_COST_BPS = (10, 20)


class CloseToOpenEtfMomentumError(RuntimeError):
    """The frozen close-to-open ETF evidence is incomplete or inconsistent."""


def _load_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise CloseToOpenEtfMomentumError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise CloseToOpenEtfMomentumError(f"{path} must contain an object")
    return value


def _variant() -> dict[str, Any]:
    slate = _load_json(SLATE_PATH)
    if slate.get("manifest_sha256") != common._self_hash(slate, "manifest_sha256"):
        raise CloseToOpenEtfMomentumError("second-wave slate hash is invalid")
    matches = [item for item in slate["variants"] if item["variant_id"] == VARIANT_ID]
    if len(matches) != 1:
        raise CloseToOpenEtfMomentumError("close-to-open variant is missing")
    inspection = _load_json(SLATE_INSPECTION_PATH)
    if (
        inspection.get("inspection_sha256")
        != common._self_hash(inspection, "inspection_sha256")
        or inspection.get("manifest_sha256") != slate["manifest_sha256"]
        or inspection.get("return_evaluation_authorized") is not True
    ):
        raise CloseToOpenEtfMomentumError("second-wave slate inspection is invalid")
    return dict(matches[0])


def _intraday_dataset(
    store: HistoricalDayStore, symbol: str, day: str
) -> dict[str, Any] | None:
    value = store.select_dataset(
        symbol,
        day,
        kind="bars",
        channel="trades",
        timeframe="15m",
        providers=("ibkr",),
        require_complete=True,
        feed="smart",
        adjustment="provider_adjusted_unknown_basis",
    )
    if value is None or int(value.get("quality", {}).get("row_count", 0)) < 1:
        return None
    return value


def acquire_inputs(store: HistoricalDayStore | None = None) -> dict[str, Any]:
    """Collect 15-minute bars without selecting signals or computing returns."""

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
                bar_size="15 mins",
                what="TRADES",
                use_rth=True,
            )
            return len(rows)

        for outcome in ordered_bounded_results(
            SYMBOLS, collect, max_workers=config.max_concurrent_requests
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
        and all(daily._dataset(target_store, symbol, day) for symbol in SYMBOLS)
        and all(_intraday_dataset(target_store, symbol, day) for symbol in SYMBOLS)
    ]
    full_signal_dates = [
        day
        for day in qualified
        if all(
            int(_intraday_dataset(target_store, symbol, day)["quality"]["row_count"])
            == 26
            for symbol in SYMBOLS
        )
    ]
    status = {
        "schema_version": SCHEMA_VERSION,
        "collection_kind": "close-to-open-etf-momentum-stage0-15m-inputs",
        "variant_id": VARIANT_ID,
        "symbols": list(SYMBOLS),
        "collection_start": COLLECTION_START,
        "collection_end_exclusive": COLLECTION_END_EXCLUSIVE,
        "provider": "IBKR",
        "timeframe": "15m",
        "completed": completed,
        "failures": failures,
        "qualified_common_dates": len(qualified),
        "complete_twenty_six_bar_dates": len(full_signal_dates),
        "first_common_date": qualified[0] if qualified else None,
        "last_common_date": qualified[-1] if qualified else None,
        "provider_telemetry": telemetry,
        "signal_selections": 0,
        "returns_computed": 0,
        "broker_actions": 0,
        "valid": not failures and len(qualified) >= 800 and len(full_signal_dates) >= 700,
    }
    daily._write_json(status, COLLECTION_STATUS_PATH)
    if not status["valid"]:
        raise CloseToOpenEtfMomentumError("close-to-open input collection is incomplete")
    return status


def _validate_intraday_rows(
    raw_rows: Any, symbol: str, day: str
) -> list[dict[str, Any]]:
    if not isinstance(raw_rows, list) or not raw_rows:
        raise CloseToOpenEtfMomentumError(f"{symbol} {day} 15m rows are missing")
    rows: list[dict[str, Any]] = []
    previous: datetime | None = None
    for raw in raw_rows:
        if not isinstance(raw, Mapping):
            raise CloseToOpenEtfMomentumError(f"{symbol} {day} 15m row is malformed")
        row = dict(raw)
        try:
            observed = datetime.fromisoformat(str(row["t"]))
            prices = tuple(float(row[field]) for field in ("o", "h", "l", "c"))
            volume = int(row["v"])
        except (KeyError, TypeError, ValueError) as exc:
            raise CloseToOpenEtfMomentumError(
                f"{symbol} {day} 15m row fields are invalid"
            ) from exc
        local = observed.astimezone(EASTERN) if observed.tzinfo else observed.replace(
            tzinfo=EASTERN
        )
        if local.date().isoformat() != day or local.minute % 15 != 0:
            raise CloseToOpenEtfMomentumError(
                f"{symbol} {day} 15m timestamp drifted"
            )
        if not time(9, 30) <= local.time().replace(tzinfo=None) < time(16, 0):
            raise CloseToOpenEtfMomentumError(f"{symbol} {day} row is outside RTH")
        if previous is not None and local <= previous:
            raise CloseToOpenEtfMomentumError(f"{symbol} {day} rows are unordered")
        if not all(math.isfinite(value) and value > 0 for value in prices):
            raise CloseToOpenEtfMomentumError(f"{symbol} {day} prices are invalid")
        open_price, high, low, close = prices
        if low > min(open_price, close) or high < max(open_price, close) or low > high:
            raise CloseToOpenEtfMomentumError(f"{symbol} {day} OHLC is invalid")
        if volume < 0 or row.get("i") is not False:
            raise CloseToOpenEtfMomentumError(f"{symbol} {day} volume is unusable")
        previous = local
        rows.append(row)
    return rows


def _build_input_graph(store: HistoricalDayStore) -> dict[str, Any]:
    common_dates = set(store.dates(SYMBOLS[0]))
    for symbol in SYMBOLS[1:]:
        common_dates &= set(store.dates(symbol))
    dates: list[str] = []
    daily_rows: dict[str, list[dict[str, Any]]] = {symbol: [] for symbol in SYMBOLS}
    intraday_rows: dict[str, list[list[dict[str, Any]]]] = {
        symbol: [] for symbol in SYMBOLS
    }
    daily_ids: dict[str, list[str]] = {symbol: [] for symbol in SYMBOLS}
    intraday_ids: dict[str, list[str]] = {symbol: [] for symbol in SYMBOLS}
    for day in sorted(common_dates):
        if not COLLECTION_START <= day < COLLECTION_END_EXCLUSIVE:
            continue
        day_daily = {symbol: daily._dataset(store, symbol, day) for symbol in SYMBOLS}
        day_intraday = {
            symbol: _intraday_dataset(store, symbol, day) for symbol in SYMBOLS
        }
        if any(value is None for value in (*day_daily.values(), *day_intraday.values())):
            continue
        dates.append(day)
        for symbol in SYMBOLS:
            daily_dataset = day_daily[symbol]
            intraday_dataset = day_intraday[symbol]
            assert daily_dataset is not None and intraday_dataset is not None
            daily_rows[symbol].append(
                daily._validate_daily_row(daily_dataset["rows"][0], symbol, day)
            )
            intraday_rows[symbol].append(
                _validate_intraday_rows(intraday_dataset["rows"], symbol, day)
            )
            daily_ids[symbol].append(str(daily_dataset["id"]))
            intraday_ids[symbol].append(str(intraday_dataset["id"]))
    evaluation_dates = []
    for index, day in enumerate(dates):
        if not EVALUATION_START <= day <= EVALUATION_END:
            continue
        if index < 20 or index + 1 >= len(dates):
            continue
        if all(len(intraday_rows[symbol][index]) == 26 for symbol in SYMBOLS):
            evaluation_dates.append(day)
    if len(evaluation_dates) < 700:
        raise CloseToOpenEtfMomentumError("frozen evaluation calendar is incomplete")
    graph: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "dataset_id": "dataset-close-to-open-etf-momentum-stage0-2026-07-21-v1",
        "variant_id": VARIANT_ID,
        "symbols": list(SYMBOLS),
        "dates": dates,
        "evaluation_dates": evaluation_dates,
        "daily_rows_by_symbol": daily_rows,
        "intraday_rows_by_symbol": intraday_rows,
        "daily_dataset_ids_by_symbol": daily_ids,
        "intraday_dataset_ids_by_symbol": intraday_ids,
        "provider": "ibkr",
        "feed": "smart",
        "adjustment": "provider_adjusted_unknown_basis",
        "daily_timeframe": "1d",
        "signal_timeframe": "15m",
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
        raise CloseToOpenEtfMomentumError("private frozen input graph is missing")
    recorded_graph = daily._load_gzip_json(PRIVATE_INPUT_PATH)
    if recorded_graph != graph:
        raise CloseToOpenEtfMomentumError("private frozen input graph does not rebuild")
    symbol_dates = len(graph["dates"]) * len(SYMBOLS)
    activation: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "contract_kind": "stage0-activation",
        "campaign_id": "multi-strategy-portfolio-validation-v1",
        "tournament_wave": 2,
        "variant_ordinal": 3,
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
            "daily_input_symbol_dates": symbol_dates,
            "intraday_input_symbol_dates": symbol_dates,
            "input_sha256": graph["input_sha256"],
            "private_input_file_sha256": sha256_file(PRIVATE_INPUT_PATH),
        },
        "selection_contract": variant["signal"],
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
            "daily_input_symbol_dates": symbol_dates,
            "intraday_input_symbol_dates": symbol_dates,
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
        raise CloseToOpenEtfMomentumError("activation content hash is invalid")
    if recorded != build_activation():
        raise CloseToOpenEtfMomentumError("activation does not rebuild")
    graph = daily._load_gzip_json(PRIVATE_INPUT_PATH)
    if graph.get("input_sha256") != common._self_hash(graph, "input_sha256"):
        raise CloseToOpenEtfMomentumError("private input content hash is invalid")
    if graph["input_sha256"] != recorded["source_selection"]["input_sha256"]:
        raise CloseToOpenEtfMomentumError("activation input identity drifted")
    if sha256_file(PRIVATE_INPUT_PATH) != recorded["source_selection"][
        "private_input_file_sha256"
    ]:
        raise CloseToOpenEtfMomentumError("private input file hash drifted")
    for symbol in SYMBOLS:
        for index, day in enumerate(graph["dates"]):
            daily._validate_daily_row(
                graph["daily_rows_by_symbol"][symbol][index], symbol, day
            )
            _validate_intraday_rows(
                graph["intraday_rows_by_symbol"][symbol][index], symbol, day
            )
    symbol_dates = len(graph["dates"]) * len(SYMBOLS)
    inspection: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "inspection_kind": "stage0-activation-input-inspection",
        "variant_id": VARIANT_ID,
        "manifest_sha256": recorded["manifest_sha256"],
        "manifest_file_sha256": sha256_file(path),
        "input_sha256": graph["input_sha256"],
        "daily_input_symbol_dates": symbol_dates,
        "intraday_input_symbol_dates": symbol_dates,
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


def _outcome(
    signal_rows: Sequence[Mapping[str, Any]],
    next_rows: Sequence[Mapping[str, Any]],
    prior_atr: float,
    cost_bps: int,
) -> dict[str, Any]:
    if len(signal_rows) != 26 or not next_rows:
        raise CloseToOpenEtfMomentumError("outcome bars are incomplete")
    raw_entry = float(signal_rows[25]["o"])
    stop = raw_entry - prior_atr
    if stop <= 0 or stop >= raw_entry:
        raise CloseToOpenEtfMomentumError("planned stop is invalid")
    entry_bar = signal_rows[25]
    next_open = float(next_rows[0]["o"])
    if float(entry_bar["l"]) <= stop:
        exit_price = stop
        exit_reason = "stop"
    elif next_open <= stop:
        exit_price = next_open
        exit_reason = "stop_gap"
    else:
        exit_price = next_open
        exit_reason = "next_open"
    cost = cost_bps / 10_000
    entry_fill = raw_entry * (1 + cost)
    exit_fill = exit_price * (1 - cost)
    stop_fill = stop * (1 - cost)
    planned_risk = entry_fill - stop_fill
    return {
        "net_r": (exit_fill - entry_fill) / planned_risk,
        "exit_reason": exit_reason,
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
        raise CloseToOpenEtfMomentumError("input inspection does not rebuild")
    if require_published:
        daily._require_published((activation_path, inspection_path))
    graph = daily._load_gzip_json(PRIVATE_INPUT_PATH)
    dates = list(graph["dates"])
    positions = {day: index for index, day in enumerate(dates)}
    daily_rows = graph["daily_rows_by_symbol"]
    intraday_rows = graph["intraday_rows_by_symbol"]
    daily_closes = {
        symbol: [float(row["c"]) for row in daily_rows[symbol]]
        for symbol in SYMBOLS
    }
    records: list[dict[str, Any]] = []
    no_trade_dates = 0
    for day in graph["evaluation_dates"]:
        index = positions[day]
        candidates: list[tuple[float, str]] = []
        for symbol in SYMBOLS:
            rows = intraday_rows[symbol][index]
            signal_close = float(rows[24]["c"])
            session_return = signal_close / float(rows[0]["o"]) - 1
            prior_sma20 = statistics.fmean(daily_closes[symbol][index - 20 : index])
            if session_return < 0.0075 or signal_close <= prior_sma20:
                continue
            candidates.append((session_return, symbol))
        if not candidates:
            no_trade_dates += 1
            continue
        candidates.sort(key=lambda item: (-item[0], item[1]))
        session_return, symbol = candidates[0]
        prior_atr = daily._atr(daily_rows[symbol], index - 1)
        outcomes = {
            str(cost): _outcome(
                intraday_rows[symbol][index],
                intraday_rows[symbol][index + 1],
                prior_atr,
                cost,
            )
            for cost in (PRIMARY_COST_BPS, *STRESS_COST_BPS)
        }
        primary = outcomes[str(PRIMARY_COST_BPS)]
        records.append(
            {
                "signal_date": day,
                "symbol": symbol,
                "session_return": session_return,
                "entry_date": day,
                "exit_date": dates[index + 1],
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
            "decision_dates": len(graph["evaluation_dates"]),
            "closed_signals": len(records),
            "no_trade_dates": no_trade_dates,
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
        raise CloseToOpenEtfMomentumError("result content hash is invalid")
    if recorded != build_result(
        activation_path, inspection_path, require_published=False
    ):
        raise CloseToOpenEtfMomentumError("result does not independently rebuild")
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
        raise CloseToOpenEtfMomentumError(
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
                    "complete_twenty_six_bar_dates",
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
                "close-to-open-etf-momentum-v1-*.json",
                "close-to-open activation",
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
                "close-to-open-etf-momentum-v1-*.json",
                "close-to-open activation",
            )
            inspection_path = _one(
                "strategy_tournament/second_wave/inspections/"
                "close-to-open-etf-momentum-v1-input-*.json",
                "close-to-open input inspection",
            )
            result = build_result(activation_path, inspection_path)
            path = (
                PROJECT_ROOT
                / "research_results/"
                f"2026-07-21-close-to-open-etf-momentum-stage0-"
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
                "close-to-open-etf-momentum-v1-*.json",
                "close-to-open activation",
            )
            inspection_path = _one(
                "strategy_tournament/second_wave/inspections/"
                "close-to-open-etf-momentum-v1-input-*.json",
                "close-to-open input inspection",
            )
            result_path = _one(
                "research_results/2026-07-21-close-to-open-etf-momentum-stage0-*.json",
                "close-to-open Stage 0 result",
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

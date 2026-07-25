"""Freeze and collect the exact SEC PEAD graph from Yahoo daily history."""

from __future__ import annotations

import argparse
import json
import time
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import quote
from zoneinfo import ZoneInfo

import requests

import earnings_sec_eps_capacity as v5
import earnings_sec_market_data as market
import outcome_exposure
import strategy_discovery
from historical_store import HistoricalDayStore, canonical_sha256, sha256_file


PROJECT_ROOT = Path(__file__).resolve().parent
CAMPAIGN_ID = market.CAMPAIGN_ID
FAMILY_ID = market.FAMILY_ID
SUCCESSOR_ID = "earnings-positive-surprise-drift-v9-sec-yahoo-development"
DEFAULT_ROOT = (
    PROJECT_ROOT / "strategy_tournament/v2/continuous" / SUCCESSOR_ID
)
SOURCE_ID = "yahoo-chart-no-purchase-v1"
ENDPOINT_TEMPLATE = (
    "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
)
EASTERN = ZoneInfo("America/New_York")
MASSIVE_CONTRACT = (
    market.DEFAULT_ROOT
    / "market-data-contract"
    / "contract-ac48ecddc2d3dc64daa1a8d5d212227ad46df082d767091915ee7c2b7cbfcc7d.json"
)
MASSIVE_FAILURE_INSPECTION = (
    market.DEFAULT_ROOT
    / "market-data-source-failure-inspection"
    / "inspection-c0fe59ed6228e20edeedfbad294ef7034334b2ed2277e04c6507ecf4954fd60c.json"
)
PRIVATE_NAMESPACE = "_derived/earnings_sec_yahoo_data"
PACE_SECONDS = 0.20


class EarningsSecYahooDataError(RuntimeError):
    """The exact Yahoo source contract, response, or cache drifted."""


def _period(day: str) -> int:
    return int(datetime.fromisoformat(f"{day}T00:00:00+00:00").timestamp())


def _exclusive_period(day: str) -> int:
    result = datetime.fromisoformat(f"{day}T00:00:00+00:00") + timedelta(
        days=1
    )
    return int(result.timestamp())


def _lineage() -> tuple[dict[str, Any], dict[str, Any]]:
    strategy_discovery.require_committed(MASSIVE_CONTRACT)
    strategy_discovery.require_committed(MASSIVE_FAILURE_INSPECTION)
    contract = market._read(MASSIVE_CONTRACT)
    failure = market._read(MASSIVE_FAILURE_INSPECTION)
    if not (
        contract.get("contract_sha256")
        == "ac48ecddc2d3dc64daa1a8d5d212227ad46df082d767091915ee7c2b7cbfcc7d"
        and failure.get("state")
        == "DEVELOPMENT_SOURCE_PERMISSION_FAILURE_INSPECTED"
        and failure.get("valid") is True
        and failure.get("contract_sha256") == contract["contract_sha256"]
        and failure.get("same_source_retry_authorized") is False
        and failure.get("provider_purchase_authorized") is False
        and failure.get("exact_no_purchase_fallback_authorized") is True
        and failure.get("development_prices_retained") is False
    ):
        raise EarningsSecYahooDataError(
            "Massive failure is not independently closed for fallback"
        )
    return contract, failure


def build_contract(*, created_at: str) -> dict[str, Any]:
    for path in (
        Path(__file__).resolve(),
        PROJECT_ROOT / "earnings_sec_yahoo_data_inspection.py",
    ):
        strategy_discovery.require_committed(path)
    massive, failure = _lineage()
    index = outcome_exposure.read_index()
    outcome_exposure.assert_untouched(
        massive["development_scope"], index
    )
    outcome_exposure.assert_untouched(
        massive["confirmation_scope"], index
    )
    outcome_exposure.assert_disjoint(
        [massive["development_scope"], massive["confirmation_scope"]]
    )
    requests_: list[dict[str, Any]] = []
    for prior in massive["requests"]:
        symbol = str(prior["symbol"])
        request = {
            "method": "GET",
            "endpoint": ENDPOINT_TEMPLATE.format(symbol=quote(symbol, safe="")),
            "parameters": {
                "period1": _period(str(prior["start"])),
                "period2": _exclusive_period(str(prior["end"])),
                "interval": "1d",
                "events": "history",
                "includeAdjustedClose": "true",
            },
            "symbol": symbol,
            "start": prior["start"],
            "end": prior["end"],
            "predecessor_request_sha256": prior["request_sha256"],
        }
        request["request_sha256"] = v5.self_hash(
            request, "request_sha256"
        )
        requests_.append(request)
    value: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": "earnings-sec-yahoo-development-source-contract",
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "successor_id": SUCCESSOR_ID,
        "source_id": SOURCE_ID,
        "created_at": market._timestamp(created_at, "created_at"),
        "prior_source": {
            "contract_path": market._repo_path(MASSIVE_CONTRACT),
            "contract_file_sha256": sha256_file(MASSIVE_CONTRACT),
            "contract_sha256": massive["contract_sha256"],
            "failure_inspection_path": market._repo_path(
                MASSIVE_FAILURE_INSPECTION
            ),
            "failure_inspection_file_sha256": sha256_file(
                MASSIVE_FAILURE_INSPECTION
            ),
            "failure_inspection_sha256": failure["inspection_sha256"],
        },
        "selection_sha256": massive["selection_sha256"],
        "selection_summary": massive["selection_summary"],
        "development_scope": massive["development_scope"],
        "confirmation_scope": massive["confirmation_scope"],
        "provider": {
            "name": "Yahoo Finance historical chart JSON",
            "no_purchase_required": True,
            "endpoint_template": ENDPOINT_TEMPLATE,
            "exchange_timezone_required": "America/New_York",
        },
        "requests": requests_,
        "request_policy": {
            "authorized_provider_requests": len(requests_),
            "pacing_seconds_between_requests": PACE_SECONDS,
            "retries_permitted": 0,
            "substitutions_permitted": 0,
            "resume_from_hash_valid_tasks": True,
            "http_404_or_chart_not_found": "retained_as_missing_history",
            "confirmation_requests_permitted": 0,
        },
        "data_semantics": {
            "raw_ohlcv_used": True,
            "provider_raw_ohlc_split_adjusted": True,
            "dividend_adjusted_close_used": False,
            "null_rows": "omitted_as_missing_sessions",
            "missing_symbol_or_session": "missed_trade_never_substitute",
            "development_prices_only": True,
        },
        "exact_graph_preservation": {
            "symbols_changed": False,
            "dates_changed": False,
            "strategy_rules_changed": False,
            "confirmation_scope_changed": False,
        },
        "implementation_hashes": {
            name: sha256_file(PROJECT_ROOT / name)
            for name in (
                "earnings_sec_yahoo_data.py",
                "earnings_sec_yahoo_data_inspection.py",
            )
        },
        "market_prices_accessed": False,
        "strategy_metrics_computed": 0,
        "confirmation_prices_accessed": False,
        "broker_actions": 0,
    }
    value["contract_sha256"] = v5.self_hash(value, "contract_sha256")
    return value


def freeze_contract(
    *,
    created_at: str,
    root: Path = DEFAULT_ROOT,
) -> tuple[Path, dict[str, Any]]:
    value = build_contract(created_at=created_at)
    path = (
        root
        / "yahoo-contract"
        / f"contract-{value['contract_sha256']}.json"
    )
    market._write(path, value)
    return path, value


def _parse_response(
    request: Mapping[str, Any],
    payload: Any,
) -> dict[str, Any]:
    chart = payload.get("chart") if isinstance(payload, Mapping) else None
    results = chart.get("result") if isinstance(chart, Mapping) else None
    error = chart.get("error") if isinstance(chart, Mapping) else None
    if error is not None or not isinstance(results, list) or not results:
        value = {
            "schema_version": 1,
            "request_sha256": request["request_sha256"],
            "symbol": request["symbol"],
            "status": "PERMANENT_MISSING",
            "missing_reason": "Yahoo chart result unavailable",
            "rows": [],
        }
        value["task_sha256"] = v5.self_hash(value, "task_sha256")
        return value
    if len(results) != 1:
        raise EarningsSecYahooDataError(
            "Yahoo chart returned an ambiguous result set"
        )
    result = results[0]
    meta = result.get("meta") if isinstance(result, Mapping) else None
    timestamps = result.get("timestamp") if isinstance(result, Mapping) else None
    indicators = result.get("indicators") if isinstance(result, Mapping) else None
    quotes = (
        indicators.get("quote")
        if isinstance(indicators, Mapping)
        else None
    )
    quote_row = quotes[0] if isinstance(quotes, list) and quotes else None
    if not (
        isinstance(meta, Mapping)
        and str(meta.get("symbol", "")).upper() == request["symbol"]
        and meta.get("exchangeTimezoneName") == "America/New_York"
        and isinstance(timestamps, list)
        and isinstance(quote_row, Mapping)
    ):
        raise EarningsSecYahooDataError(
            "Yahoo chart identity, timezone, or quote arrays drifted"
        )
    arrays = {
        field: quote_row.get(field)
        for field in ("open", "high", "low", "close", "volume")
    }
    if any(
        not isinstance(values, list) or len(values) != len(timestamps)
        for values in arrays.values()
    ):
        raise EarningsSecYahooDataError(
            "Yahoo chart OHLCV arrays are incomplete"
        )
    rows: list[dict[str, Any]] = []
    for index, timestamp in enumerate(timestamps):
        values = [arrays[field][index] for field in arrays]
        if any(item is None for item in values):
            continue
        day = (
            datetime.fromtimestamp(int(timestamp), UTC)
            .astimezone(EASTERN)
            .date()
            .isoformat()
        )
        if not request["start"] <= day <= request["end"]:
            raise EarningsSecYahooDataError(
                "Yahoo chart row escaped the frozen development range"
            )
        try:
            row = {
                "symbol": request["symbol"],
                "date": day,
                "open": float(arrays["open"][index]),
                "high": float(arrays["high"][index]),
                "low": float(arrays["low"][index]),
                "close": float(arrays["close"][index]),
                "volume": int(arrays["volume"][index]),
            }
        except (TypeError, ValueError) as exc:
            raise EarningsSecYahooDataError(
                "Yahoo chart contains malformed OHLCV"
            ) from exc
        if (
            min(row[field] for field in ("open", "high", "low", "close"))
            <= 0
            or row["volume"] < 0
            or row["low"] > min(row["open"], row["close"])
            or row["high"] < max(row["open"], row["close"])
        ):
            raise EarningsSecYahooDataError(
                "Yahoo chart contains invalid OHLCV"
            )
        rows.append(row)
    dates = [row["date"] for row in rows]
    if dates != sorted(dates) or len(dates) != len(set(dates)):
        raise EarningsSecYahooDataError(
            "Yahoo chart dates are not unique and chronological"
        )
    status = "COMPLETE" if rows else "PERMANENT_MISSING"
    value = {
        "schema_version": 1,
        "request_sha256": request["request_sha256"],
        "symbol": request["symbol"],
        "status": status,
        "missing_reason": None if rows else "Yahoo chart returned no rows",
        "rows": rows,
    }
    value["task_sha256"] = v5.self_hash(value, "task_sha256")
    return value


def _fetch(
    request: Mapping[str, Any],
    session: requests.Session,
    telemetry: dict[str, Any],
) -> dict[str, Any]:
    started = time.monotonic()
    telemetry["requests"] += 1
    try:
        response = session.get(
            request["endpoint"],
            params=request["parameters"],
            headers={
                "User-Agent": "robinhood-codex-historical-research/1.0"
            },
            timeout=30,
        )
    except requests.RequestException as exc:
        telemetry["failures"] += 1
        raise EarningsSecYahooDataError(
            "Yahoo development request failed before a response"
        ) from exc
    finally:
        telemetry["request_seconds"] += time.monotonic() - started
    if response.status_code == 404:
        return _parse_response(request, {"chart": {"result": None, "error": {}}})
    if response.status_code >= 400:
        telemetry["failures"] += 1
        raise EarningsSecYahooDataError(
            f"Yahoo development request returned HTTP {response.status_code}"
        )
    try:
        payload = response.json()
    except ValueError as exc:
        telemetry["failures"] += 1
        raise EarningsSecYahooDataError(
            "Yahoo development response is not JSON"
        ) from exc
    return _parse_response(request, payload)


def _task_path(
    store: HistoricalDayStore,
    contract_sha256: str,
    request_sha256: str,
) -> Path:
    return (
        store.root
        / PRIVATE_NAMESPACE
        / contract_sha256
        / "tasks"
        / f"{request_sha256}.json.gz"
    )


def collect_development(
    contract_path: Path,
    inspection_path: Path,
    *,
    collected_at: str,
    store: HistoricalDayStore | None = None,
    root: Path = DEFAULT_ROOT,
    fetcher: Callable[[Mapping[str, Any]], dict[str, Any]] | None = None,
) -> tuple[Path, dict[str, Any]]:
    strategy_discovery.require_committed(Path(__file__).resolve())
    strategy_discovery.require_committed(contract_path)
    strategy_discovery.require_committed(inspection_path)
    contract = market._read(contract_path)
    inspection = market._read(inspection_path)
    if not (
        contract.get("contract_sha256")
        == v5.self_hash(contract, "contract_sha256")
        and inspection.get("contract_sha256") == contract["contract_sha256"]
        and inspection.get("state") == "YAHOO_CONTRACT_INSPECTED_READY"
        and inspection.get("valid") is True
        and inspection.get("development_provider_access_authorized") is True
    ):
        raise EarningsSecYahooDataError(
            "Yahoo development source authorization is invalid"
        )
    historical_store = store or HistoricalDayStore.from_env()
    selection = market._development_selection(historical_store)
    if canonical_sha256(selection) != contract["selection_sha256"]:
        raise EarningsSecYahooDataError(
            "Yahoo development metadata selection drifted"
        )
    telemetry: dict[str, Any] = {
        "requests": 0,
        "request_seconds": 0.0,
        "pacing_wait_seconds": 0.0,
        "cache_hits": 0,
        "failures": 0,
    }
    http = requests.Session()
    tasks: list[dict[str, Any]] = []
    try:
        for index, request in enumerate(contract["requests"]):
            task_path = _task_path(
                historical_store,
                contract["contract_sha256"],
                request["request_sha256"],
            )
            if task_path.is_file():
                task = market._read_private(task_path)
                if not (
                    task.get("request_sha256")
                    == request["request_sha256"]
                    and task.get("task_sha256")
                    == v5.self_hash(task, "task_sha256")
                ):
                    raise EarningsSecYahooDataError(
                        "cached Yahoo task hash differs"
                    )
                telemetry["cache_hits"] += 1
            else:
                if (
                    fetcher is None
                    and index > 0
                    and telemetry["requests"] > 0
                ):
                    time.sleep(PACE_SECONDS)
                    telemetry["pacing_wait_seconds"] += PACE_SECONDS
                task = (
                    fetcher(request)
                    if fetcher is not None
                    else _fetch(request, http, telemetry)
                )
                if fetcher is not None:
                    telemetry["requests"] += 1
                    if "task_sha256" not in task:
                        task["task_sha256"] = v5.self_hash(
                            task, "task_sha256"
                        )
                if not (
                    task.get("request_sha256")
                    == request["request_sha256"]
                    and task.get("task_sha256")
                    == v5.self_hash(task, "task_sha256")
                ):
                    raise EarningsSecYahooDataError(
                        "Yahoo task does not match frozen request"
                    )
                market._write_private(task_path, task)
            tasks.append(task)
    finally:
        http.close()
    if (
        telemetry["requests"] + telemetry["cache_hits"]
        != len(contract["requests"])
    ):
        raise EarningsSecYahooDataError(
            "Yahoo request and cache accounting is incomplete"
        )
    daily_bars: dict[str, list[dict[str, Any]]] = {}
    missing: dict[str, str] = {}
    for task in tasks:
        symbol = str(task["symbol"])
        if task["status"] == "COMPLETE":
            daily_bars[symbol] = list(task["rows"])
        elif task["status"] == "PERMANENT_MISSING":
            missing[symbol] = str(task["missing_reason"])
        else:
            raise EarningsSecYahooDataError("Yahoo task status is invalid")
    dataset = {
        "schema_version": 1,
        "family_id": FAMILY_ID,
        "evaluation_dates": selection["development_dates"],
        "event_metadata_by_date": selection["event_metadata_by_date"],
        "daily_bars": daily_bars,
        "missing_symbols": missing,
        "source_semantics": {
            "provider": "Yahoo Finance historical chart JSON",
            "contract_sha256": contract["contract_sha256"],
            "raw_ohlcv_used": True,
            "dividend_adjusted_close_used": False,
            "confirmation_prices_accessed": False,
        },
    }
    relative = (
        Path(PRIVATE_NAMESPACE)
        / contract["contract_sha256"]
        / "development.json.gz"
    )
    private_path = historical_store.root / relative
    market._write_private(private_path, dataset)
    value: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": "earnings-sec-yahoo-development-collection",
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "successor_id": SUCCESSOR_ID,
        "source_id": SOURCE_ID,
        "state": "YAHOO_DEVELOPMENT_COLLECTED_UNINSPECTED",
        "collected_at": market._timestamp(collected_at, "collected_at"),
        "contract_path": market._repo_path(contract_path),
        "contract_file_sha256": sha256_file(contract_path),
        "contract_sha256": contract["contract_sha256"],
        "inspection_path": market._repo_path(inspection_path),
        "inspection_file_sha256": sha256_file(inspection_path),
        "inspection_sha256": inspection["inspection_sha256"],
        "external_relative_path": str(relative),
        "external_file_sha256": sha256_file(private_path),
        "dataset_sha256": canonical_sha256(dataset),
        "symbols_requested": len(contract["requests"]),
        "symbols_complete": len(daily_bars),
        "symbols_permanently_missing": len(missing),
        "row_count": sum(len(rows) for rows in daily_bars.values()),
        "provider_telemetry": telemetry,
        "substitutions": 0,
        "retries": 0,
        "strategy_metrics_computed": 0,
        "confirmation_prices_accessed": False,
        "broker_actions": 0,
    }
    value["collection_sha256"] = v5.self_hash(
        value, "collection_sha256"
    )
    path = (
        root
        / "yahoo-collection"
        / f"collection-{value['collection_sha256']}.json"
    )
    market._write(path, value)
    outcome_exposure.ensure_record(
        outcome_exposure.build_record(
            exposure_id=(
                f"source-{FAMILY_ID}-yahoo-"
                f"{value['collection_sha256'][:16]}"
            ),
            campaign_id=CAMPAIGN_ID,
            lane="development",
            recorded_at=value["collected_at"],
            source_path=market._repo_path(path),
            source_sha256=sha256_file(path),
            scope=contract["development_scope"],
        )
    )
    return path, value


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    freeze = subparsers.add_parser("freeze-contract")
    freeze.add_argument("--created-at", required=True)
    collect = subparsers.add_parser("collect-development")
    collect.add_argument("contract", type=Path)
    collect.add_argument("inspection", type=Path)
    collect.add_argument("--collected-at", required=True)
    args = parser.parse_args(argv)
    if args.command == "freeze-contract":
        path, value = freeze_contract(created_at=args.created_at)
        digest = value["contract_sha256"]
        state = "YAHOO_CONTRACT_FROZEN"
    else:
        path, value = collect_development(
            args.contract,
            args.inspection,
            collected_at=args.collected_at,
        )
        digest = value["collection_sha256"]
        state = value["state"]
    print(
        json.dumps(
            {
                "path": market._repo_path(path),
                "sha256": digest,
                "state": state,
                "provider_telemetry": value.get("provider_telemetry", {}),
                "symbols": value.get(
                    "symbols_complete", len(value.get("requests", []))
                ),
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

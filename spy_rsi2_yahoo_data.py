"""Freeze and collect a no-purchase Yahoo fallback for fixed SPY RSI(2)."""

from __future__ import annotations

import argparse
import hashlib
import json
import time
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import requests

import dense_strategy_runtime as runtime
import outcome_exposure
import spy_rsi2_data as shared
import strategy_discovery
from historical_store import (
    HistoricalStoreConfig,
    canonical_sha256,
    sha256_file,
)


PROJECT_ROOT = Path(__file__).resolve().parent
CAMPAIGN_ID = shared.CAMPAIGN_ID
FAMILY_ID = shared.FAMILY_ID
SUCCESSOR_ID = shared.SUCCESSOR_ID
SOURCE_ID = "yahoo-chart-no-purchase-v1"
DEFAULT_ROOT = shared.DEFAULT_ROOT
ENDPOINT = "https://query1.finance.yahoo.com/v8/finance/chart/SPY"
EASTERN = ZoneInfo("America/New_York")


class SpyRsi2YahooError(RuntimeError):
    """The frozen Yahoo fallback or returned SPY history drifted."""


def _period(day: str) -> int:
    return int(datetime.fromisoformat(f"{day}T00:00:00+00:00").timestamp())


def build_source_contract(
    *,
    created_at: str,
    massive_failure_inspection_path: Path,
) -> dict[str, Any]:
    for path in (
        Path(__file__).resolve(),
        PROJECT_ROOT / "spy_rsi2_yahoo_inspection.py",
        PROJECT_ROOT / "dense_strategy_runtime.py",
        PROJECT_ROOT / "dense_strategy_plugin.py",
        massive_failure_inspection_path,
    ):
        strategy_discovery.require_committed(path)
    failure_inspection = shared._read(massive_failure_inspection_path)
    if not (
        failure_inspection.get("state")
        == "DEVELOPMENT_SOURCE_FAILURE_INSPECTED"
        and failure_inspection.get("valid") is True
        and failure_inspection.get("source_retry_authorized") is False
        and failure_inspection.get("provider_purchase_authorized") is False
    ):
        raise SpyRsi2YahooError("prior Massive failure is not independently closed")
    created = shared._timestamp(created_at, "created_at")
    split = shared.partitions()
    development_scope = {
        "dates": split["development_dates"],
        "symbols": ["SPY"],
    }
    confirmation_scope = {
        "dates": split["confirmation_dates"],
        "symbols": ["SPY"],
    }
    index = outcome_exposure.read_index()
    outcome_exposure.assert_untouched(development_scope, index)
    outcome_exposure.assert_untouched(confirmation_scope, index)
    outcome_exposure.assert_disjoint([development_scope, confirmation_scope])
    end_exclusive = (
        datetime.fromisoformat(
            f"{split['development_dates'][-1]}T00:00:00+00:00"
        )
        + timedelta(days=1)
    ).date().isoformat()
    request = {
        "method": "GET",
        "endpoint": ENDPOINT,
        "parameters": {
            "period1": _period(split["warmup_dates"][0]),
            "period2": _period(end_exclusive),
            "interval": "1d",
            "events": "history",
            "includeAdjustedClose": "true",
        },
        "response_semantics": {
            "chart_result_count": 1,
            "symbol": "SPY",
            "exchange_timezone": "America/New_York",
            "raw_ohlc_used": True,
            "raw_close_adjustment": "splits_only",
            "dividend_adjusted_close_used": False,
            "complete_frozen_xnys_denominator_required": True,
        },
    }
    request["request_sha256"] = hashlib.sha256(
        shared.canonical_bytes(request)
    ).hexdigest()
    value: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": "spy-rsi2-yahoo-development-source-contract",
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "successor_id": SUCCESSOR_ID,
        "source_id": SOURCE_ID,
        "created_at": created,
        "calendar": {
            "exchange": "XNYS",
            "builder": "spy_rsi2_data.xnys_sessions",
            "extraordinary_full_day_closures": sorted(
                day.isoformat() for day in shared.EXTRAORDINARY_CLOSURES
            ),
        },
        **split,
        "development_scope": development_scope,
        "confirmation_scope": confirmation_scope,
        "provider": "Yahoo Finance historical chart JSON",
        "provider_history_page": "https://finance.yahoo.com/quote/SPY/history/",
        "instrument_identity_page": (
            "https://www.ssga.com/us/en/individual/etfs/"
            "state-street-spdr-sp-500-etf-trust-spy"
        ),
        "development_request": request,
        "request_policy": {
            "no_purchase_required": True,
            "authorized_provider_requests": 1,
            "retries_permitted": 0,
            "substitutions_permitted": 0,
            "confirmation_request_permitted": False,
        },
        "frozen_rule": {
            "selection_mode": "development_search",
            "trial_count": 1,
            "parameters": dict(shared.PARAMETERS),
            "long_only": True,
            "entry": "next XNYS session open",
            "cost_floor_fraction": 0.005,
            "stop": "1.5 times completed ATR14 below entry",
            "exit": "stop first, completed SMA5 reclaim close, or fifth close",
            "costs_bps_per_side": [5, 10, 20],
        },
        "related_adverse_history": {
            "family_id": runtime.ETF_PULLBACK_FAMILY,
            "finding": (
                "The earlier 32-trial ETF pullback family was rejected; this "
                "fixed successor changes entry and exit semantics without "
                "reusing its evidence."
            ),
            "promotion_evidence_reused": False,
        },
        "prior_source_failure": {
            "inspection_path": shared._repo_path(
                massive_failure_inspection_path
            ),
            "inspection_file_sha256": sha256_file(
                massive_failure_inspection_path
            ),
            "inspection_sha256": failure_inspection["inspection_sha256"],
            "retry_permitted": False,
            "promotion_evidence_created": False,
        },
        "implementation_hashes": {
            name: sha256_file(PROJECT_ROOT / name)
            for name in (
                "spy_rsi2_yahoo_data.py",
                "spy_rsi2_yahoo_inspection.py",
                "spy_rsi2_data.py",
                "dense_strategy_runtime.py",
                "dense_strategy_plugin.py",
            )
        },
        "market_prices_accessed": False,
        "strategy_metrics_computed": 0,
        "confirmation_prices_accessed": False,
        "broker_actions": 0,
    }
    value["contract_sha256"] = shared.self_hash(value, "contract_sha256")
    return value


def freeze_source_contract(
    *,
    created_at: str,
    massive_failure_inspection_path: Path,
    root: Path = DEFAULT_ROOT,
) -> tuple[Path, dict[str, Any]]:
    value = build_source_contract(
        created_at=created_at,
        massive_failure_inspection_path=massive_failure_inspection_path,
    )
    path = (
        root
        / "yahoo-source-contract"
        / f"contract-{value['contract_sha256']}.json"
    )
    shared._write(path, value)
    return path, value


def _fetch(
    contract: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    request = contract["development_request"]
    started = time.monotonic()
    try:
        response = requests.get(
            request["endpoint"],
            params=request["parameters"],
            headers={"User-Agent": "robinhood-codex-historical-research/1.0"},
            timeout=30,
        )
    except requests.RequestException as exc:
        raise SpyRsi2YahooError(
            "Yahoo development request failed before a response"
        ) from exc
    elapsed = time.monotonic() - started
    if response.status_code >= 400:
        raise SpyRsi2YahooError(
            f"Yahoo development request returned HTTP {response.status_code}"
        )
    try:
        payload = response.json()
    except ValueError as exc:
        raise SpyRsi2YahooError("Yahoo development response is not JSON") from exc
    chart = payload.get("chart") if isinstance(payload, Mapping) else None
    results = chart.get("result") if isinstance(chart, Mapping) else None
    if (
        not isinstance(chart, Mapping)
        or chart.get("error") is not None
        or not isinstance(results, list)
        or len(results) != 1
    ):
        raise SpyRsi2YahooError("Yahoo chart result is missing or ambiguous")
    result = results[0]
    meta = result.get("meta") if isinstance(result, Mapping) else None
    timestamps = result.get("timestamp") if isinstance(result, Mapping) else None
    indicators = result.get("indicators") if isinstance(result, Mapping) else None
    quote = (
        indicators.get("quote", [None])[0]
        if isinstance(indicators, Mapping)
        else None
    )
    if (
        not isinstance(meta, Mapping)
        or meta.get("symbol") != "SPY"
        or meta.get("exchangeTimezoneName") != "America/New_York"
        or not isinstance(timestamps, list)
        or not isinstance(quote, Mapping)
    ):
        raise SpyRsi2YahooError("Yahoo chart identity or timezone drifted")
    arrays = {
        field: quote.get(field)
        for field in ("open", "high", "low", "close", "volume")
    }
    if any(
        not isinstance(values, list) or len(values) != len(timestamps)
        for values in arrays.values()
    ):
        raise SpyRsi2YahooError("Yahoo chart OHLCV arrays are incomplete")
    rows: list[dict[str, Any]] = []
    for index, timestamp in enumerate(timestamps):
        try:
            rows.append(
                {
                    "date": datetime.fromtimestamp(
                        int(timestamp),
                        UTC,
                    )
                    .astimezone(EASTERN)
                    .date()
                    .isoformat(),
                    "open": float(arrays["open"][index]),
                    "high": float(arrays["high"][index]),
                    "low": float(arrays["low"][index]),
                    "close": float(arrays["close"][index]),
                    "volume": int(arrays["volume"][index]),
                }
            )
        except (TypeError, ValueError) as exc:
            raise SpyRsi2YahooError(
                "Yahoo chart contains a null or malformed OHLCV row"
            ) from exc
    return rows, {
        "requests": 1,
        "request_seconds": elapsed,
        "pacing_wait_seconds": 0.0,
        "cache_hits": 0,
        "failures": 0,
    }


def _ensure_exposure(
    path: Path,
    collection: Mapping[str, Any],
    dates: Sequence[str],
) -> None:
    outcome_exposure.ensure_record(
        outcome_exposure.build_record(
            exposure_id=(
                f"source-{FAMILY_ID}-yahoo-"
                f"{str(collection['collection_sha256'])[:16]}"
            ),
            campaign_id=CAMPAIGN_ID,
            lane="development",
            recorded_at=str(collection["collected_at"]),
            source_path=shared._repo_path(path),
            source_sha256=sha256_file(path),
            scope={"dates": list(dates), "symbols": ["SPY"]},
        )
    )


def collect_development(
    contract_path: Path,
    inspection_path: Path,
    *,
    collected_at: str,
    root: Path = DEFAULT_ROOT,
    store_config: HistoricalStoreConfig | None = None,
    fetcher: Callable[
        [Mapping[str, Any]],
        tuple[list[dict[str, Any]], dict[str, Any]],
    ]
    | None = None,
) -> tuple[Path, dict[str, Any]]:
    strategy_discovery.require_committed(contract_path)
    strategy_discovery.require_committed(inspection_path)
    contract = shared._read(contract_path)
    inspection = shared._read(inspection_path)
    if not (
        contract.get("source_id") == SOURCE_ID
        and contract.get("contract_sha256")
        == shared.self_hash(contract, "contract_sha256")
        and inspection.get("contract_sha256") == contract["contract_sha256"]
        and inspection.get("state") == "SOURCE_CONTRACT_INSPECTED_READY"
        and inspection.get("valid") is True
    ):
        raise SpyRsi2YahooError("Yahoo development source authorization is invalid")
    expected_dates = [
        *contract["warmup_dates"],
        *contract["development_dates"],
    ]
    existing = []
    for path in sorted(
        (root / "yahoo-development-collection").glob("collection-*.json")
    ):
        value = shared._read(path)
        if value.get("contract_sha256") == contract["contract_sha256"]:
            existing.append((path, value))
    if len(existing) > 1:
        raise SpyRsi2YahooError("multiple Yahoo collections bind one contract")
    if existing:
        _ensure_exposure(existing[0][0], existing[0][1], expected_dates)
        return existing[0]
    rows, telemetry = (fetcher or _fetch)(contract)
    if [row.get("date") for row in rows] != expected_dates:
        raise SpyRsi2YahooError(
            "Yahoo rows do not match the frozen XNYS denominator"
        )
    dataset = {
        "family_id": FAMILY_ID,
        "evaluation_dates": list(contract["development_dates"]),
        "symbols": ["SPY"],
        "daily_bars": {"SPY": rows},
        "source_collection": {
            "source_id": SOURCE_ID,
            "contract_sha256": contract["contract_sha256"],
            "provider_telemetry": telemetry,
        },
    }
    runtime.prepare_dataset(dataset)
    store = store_config or HistoricalStoreConfig.from_env()
    relative = (
        Path("_derived")
        / "spy_rsi2_trend_pullback"
        / SOURCE_ID
        / contract["contract_sha256"]
        / "development.json.gz"
    )
    private_path = store.root / relative
    shared._write_private(private_path, dataset, store)
    value: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": "spy-rsi2-yahoo-development-collection",
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "successor_id": SUCCESSOR_ID,
        "source_id": SOURCE_ID,
        "state": "DEVELOPMENT_COLLECTED_UNINSPECTED",
        "collected_at": shared._timestamp(collected_at, "collected_at"),
        "contract_path": shared._repo_path(contract_path),
        "contract_file_sha256": sha256_file(contract_path),
        "contract_sha256": contract["contract_sha256"],
        "inspection_path": shared._repo_path(inspection_path),
        "inspection_file_sha256": sha256_file(inspection_path),
        "external_relative_path": str(relative),
        "external_file_sha256": sha256_file(private_path),
        "dataset_sha256": canonical_sha256(dataset),
        "format": "json.gz",
        "row_count": len(rows),
        "warmup_session_count": len(contract["warmup_dates"]),
        "development_session_count": len(contract["development_dates"]),
        "provider_telemetry": telemetry,
        "provider_requests": int(telemetry.get("requests", -1)),
        "substitutions": 0,
        "retries": 0,
        "strategy_metrics_computed": 0,
        "confirmation_prices_accessed": False,
        "broker_actions": 0,
    }
    if value["provider_requests"] != 1:
        raise SpyRsi2YahooError("Yahoo collection must use exactly one request")
    value["collection_sha256"] = shared.self_hash(value, "collection_sha256")
    path = (
        root
        / "yahoo-development-collection"
        / f"collection-{value['collection_sha256']}.json"
    )
    shared._write(path, value)
    _ensure_exposure(path, value, expected_dates)
    return path, value


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    freeze = subparsers.add_parser("freeze-source")
    freeze.add_argument("massive_failure_inspection", type=Path)
    freeze.add_argument("--created-at", required=True)
    collect = subparsers.add_parser("collect-development")
    collect.add_argument("contract", type=Path)
    collect.add_argument("inspection", type=Path)
    collect.add_argument("--collected-at", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "freeze-source":
            path, value = freeze_source_contract(
                created_at=args.created_at,
                massive_failure_inspection_path=args.massive_failure_inspection,
            )
        else:
            path, value = collect_development(
                args.contract,
                args.inspection,
                collected_at=args.collected_at,
            )
        print(
            json.dumps(
                {
                    "path": str(path),
                    "state": value.get("state", "SOURCE_FROZEN"),
                    "sha256": value.get(
                        "collection_sha256",
                        value.get("contract_sha256"),
                    ),
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    except (
        SpyRsi2YahooError,
        shared.SpyRsi2DataError,
        OSError,
        outcome_exposure.OutcomeExposureError,
    ) as exc:
        print(
            json.dumps(
                {"error": str(exc), "error_type": type(exc).__name__},
                indent=2,
            )
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

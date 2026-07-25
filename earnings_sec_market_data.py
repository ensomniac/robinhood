"""Freeze and collect development-only SEC PEAD daily market data."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import time
from collections.abc import Callable, Mapping, Sequence
from datetime import date, datetime
from pathlib import Path
from typing import Any

import requests

import earnings_sec_cover_identity as cover
import earnings_sec_eps_capacity as v5
import outcome_exposure
import portfolio_maturity
import spy_rsi2_data
import strategy_discovery
from historical_providers import (
    HistoricalProviderError,
    MassiveConfig,
    MassiveHistoricalClient,
)
from historical_store import (
    DEFAULT_ENV_PATH,
    HistoricalDayStore,
    canonical_sha256,
    sha256_file,
)


PROJECT_ROOT = Path(__file__).resolve().parent
CAMPAIGN_ID = portfolio_maturity.V2_CAMPAIGN_ID
FAMILY_ID = "earnings-sec-yoy-eps-reaction-drift"
SUCCESSOR_ID = "earnings-positive-surprise-drift-v8-sec-development-data"
DEFAULT_ROOT = (
    PROJECT_ROOT / "strategy_tournament/v2/continuous" / SUCCESSOR_ID
)
PRIVATE_NAMESPACE = "_derived/earnings_sec_market_data"
COVER_INSPECTION = (
    cover.DEFAULT_ROOT
    / "cover-result-inspection"
    / "inspection-d92f088b79121b384fe010e768e1c6f41c97c49e8db44c45b5dbe6fdb2bc9cd6.json"
)
WARMUP_START = date(2009, 11, 16)
DEVELOPMENT_START = date(2010, 1, 4)
DEVELOPMENT_END = date(2010, 12, 23)
SETTLEMENT_END = date(2010, 12, 31)
CONFIRMATION_START = date(2011, 1, 10)
CONFIRMATION_END = date(2011, 12, 23)


class EarningsSecMarketDataError(RuntimeError):
    """The development-only market-data contract or collection drifted."""


class _CountingSession:
    def __init__(self, telemetry: dict[str, Any]):
        self.telemetry = telemetry
        self.session = requests.Session()

    def get(self, *args: Any, **kwargs: Any) -> requests.Response:
        started = time.monotonic()
        self.telemetry["requests"] += 1
        try:
            return self.session.get(*args, **kwargs)
        finally:
            self.telemetry["request_seconds"] += time.monotonic() - started

    def close(self) -> None:
        self.session.close()


def _read(path: Path) -> dict[str, Any]:
    return cover._read(path)


def _write(path: Path, value: Mapping[str, Any]) -> None:
    cover._write(path, value)


def _repo_path(path: Path) -> str:
    return cover._repo_path(path)


def _timestamp(value: str, field: str) -> str:
    return v5._timestamp(value, field)


def _sessions(start: date, end: date) -> list[str]:
    return spy_rsi2_data.xnys_sessions(start, end)


def _reaction_date(accepted: str, sessions: Sequence[str]) -> str | None:
    try:
        observed = datetime.fromisoformat(accepted)
    except ValueError as exc:
        raise EarningsSecMarketDataError(
            "SEC accepted timestamp is malformed"
        ) from exc
    accepted_date = observed.date().isoformat()
    for session in sessions:
        if session > accepted_date:
            return session
        if (
            session == accepted_date
            and (observed.hour, observed.minute, observed.second)
            < (9, 25, 0)
        ):
            return session
    return None


def _cover_lineage(
    store: HistoricalDayStore,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    strategy_discovery.require_committed(COVER_INSPECTION)
    inspection = _read(COVER_INSPECTION)
    result_path = PROJECT_ROOT / inspection["result_path"]
    strategy_discovery.require_committed(result_path)
    result = _read(result_path)
    info = result["private_artifact"]
    raw = (store.root / info["cache_relative_path"]).read_bytes()
    if hashlib.sha256(raw).hexdigest() != info["file_sha256"]:
        raise EarningsSecMarketDataError(
            "private cover result file hash differs"
        )
    private = json.loads(gzip.decompress(raw))
    if not (
        private.get("content_sha256") == info["content_sha256"]
        and private.get("content_sha256")
        == v5.self_hash(private, "content_sha256")
    ):
        raise EarningsSecMarketDataError(
            "private cover result content differs"
        )
    if not (
        inspection.get("state") == "SEC_COMMON_EQUITY_CAPACITY_READY"
        and inspection.get("valid") is True
        and inspection.get("development_market_data_contract_freeze_permitted")
        is True
        and inspection.get("market_price_access_permitted") is False
        and inspection.get("result_sha256") == result["result_sha256"]
        and len(private.get("events", [])) == 399
    ):
        raise EarningsSecMarketDataError(
            "SEC common-equity capacity lineage differs"
        )
    return inspection, list(private["events"])


def _development_selection(
    store: HistoricalDayStore,
) -> dict[str, Any]:
    inspection, events = _cover_lineage(store)
    full_calendar = _sessions(WARMUP_START, CONFIRMATION_END)
    development_calendar = [
        day
        for day in full_calendar
        if DEVELOPMENT_START.isoformat() <= day <= DEVELOPMENT_END.isoformat()
    ]
    confirmation_dates = [
        day
        for day in full_calendar
        if CONFIRMATION_START.isoformat() <= day <= CONFIRMATION_END.isoformat()
    ]
    metadata = {day: [] for day in development_calendar}
    confirmation_metadata = {day: [] for day in confirmation_dates}
    for event in events:
        accepted_date = str(event["accepted_date"])
        if v5.DEVELOPMENT_START <= accepted_date <= v5.DEVELOPMENT_END:
            target = metadata
        elif v5.CONFIRMATION_START <= accepted_date <= v5.CONFIRMATION_END:
            target = confirmation_metadata
        else:
            continue
        reaction = _reaction_date(str(event["accepted"]), full_calendar)
        if reaction not in target:
            continue
        prior_eps = float(event["prior_year_eps"])
        current_eps = float(event["current_eps"])
        change_ratio = float(event["eps_yoy_change_ratio"])
        target[reaction].append(
            {
                "adsh": str(event["adsh"]),
                "symbol": str(event["ticker"]),
                "accepted": str(event["accepted"]),
                "accepted_date": accepted_date,
                "report_period": str(event["period"]),
                "reaction_date": reaction,
                "current_eps": current_eps,
                "prior_eps": prior_eps,
                "eps_change": float(event["eps_yoy_change"]),
                "eps_change_ratio": change_ratio,
                "security_identity_state": str(
                    event["security_identity_state"]
                ),
            }
        )
    for day, rows in metadata.items():
        metadata[day] = sorted(
            rows,
            key=lambda row: (
                -float(row["eps_change_ratio"]),
                str(row["symbol"]),
                str(row["adsh"]),
            ),
        )
    for day, rows in confirmation_metadata.items():
        confirmation_metadata[day] = sorted(
            rows,
            key=lambda row: (
                -float(row["eps_change_ratio"]),
                str(row["symbol"]),
                str(row["adsh"]),
            ),
        )
    symbols = sorted(
        {
            str(row["symbol"])
            for rows in metadata.values()
            for row in rows
        }
    )
    signal_dates = [day for day, rows in metadata.items() if rows]
    confirmation_signal_dates = [
        day for day, rows in confirmation_metadata.items() if rows
    ]
    confirmation_symbols = sorted(
        {
            str(row["symbol"])
            for rows in confirmation_metadata.values()
            for row in rows
        }
    )
    if not (
        len(signal_dates) >= 50
        and len(symbols) >= 50
        and len(confirmation_signal_dates) >= 50
        and len(confirmation_symbols) >= 50
    ):
        raise EarningsSecMarketDataError(
            "development reaction capacity is insufficient"
        )
    return {
        "schema_version": 1,
        "family_id": FAMILY_ID,
        "successor_id": SUCCESSOR_ID,
        "cover_inspection_sha256": inspection["inspection_sha256"],
        "warmup_dates": _sessions(WARMUP_START, DEVELOPMENT_START)[:-1],
        "development_dates": development_calendar,
        "settlement_dates": _sessions(
            DEVELOPMENT_END, SETTLEMENT_END
        )[1:],
        "confirmation_dates": confirmation_dates,
        "event_metadata_by_date": metadata,
        "development_event_count": sum(len(rows) for rows in metadata.values()),
        "development_signal_dates": signal_dates,
        "development_symbols": symbols,
        "confirmation_event_count": sum(
            len(rows) for rows in confirmation_metadata.values()
        ),
        "confirmation_signal_dates": confirmation_signal_dates,
        "confirmation_symbols": confirmation_symbols,
        "selection_uses_price_contents": False,
        "confirmation_outcomes_accessed": False,
    }


def _scope(dates: Sequence[str], symbols: Sequence[str]) -> dict[str, Any]:
    return {"dates": list(dates), "symbols": list(symbols)}


def build_contract(
    *,
    created_at: str,
    store: HistoricalDayStore | None = None,
) -> dict[str, Any]:
    for path in (
        Path(__file__).resolve(),
        PROJECT_ROOT / "earnings_sec_market_data_inspection.py",
    ):
        strategy_discovery.require_committed(path)
    historical_store = store or HistoricalDayStore.from_env()
    selection = _development_selection(historical_store)
    opened_dates = [
        *selection["warmup_dates"],
        *selection["development_dates"],
        *selection["settlement_dates"],
    ]
    development_scope = _scope(
        opened_dates, selection["development_symbols"]
    )
    confirmation_scope = {
        "dates": selection["confirmation_dates"],
        "symbols": selection["confirmation_symbols"],
    }
    index = outcome_exposure.read_index()
    outcome_exposure.assert_untouched(development_scope, index)
    outcome_exposure.assert_untouched(confirmation_scope, index)
    outcome_exposure.assert_disjoint(
        [development_scope, confirmation_scope]
    )
    requests_: list[dict[str, Any]] = []
    for symbol in selection["development_symbols"]:
        request = {
            "method": "GET",
            "path": (
                f"/v2/aggs/ticker/{symbol}/range/1/day/"
                f"{opened_dates[0]}/{opened_dates[-1]}"
            ),
            "parameters": {
                "adjusted": "true",
                "sort": "asc",
                "limit": 50_000,
            },
            "symbol": symbol,
            "start": opened_dates[0],
            "end": opened_dates[-1],
        }
        request["request_sha256"] = hashlib.sha256(
            v5.canonical_bytes(request)
        ).hexdigest()
        requests_.append(request)
    value: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": "earnings-sec-development-market-data-contract",
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "successor_id": SUCCESSOR_ID,
        "created_at": _timestamp(created_at, "created_at"),
        "source_lineage": {
            "cover_inspection_path": _repo_path(COVER_INSPECTION),
            "cover_inspection_file_sha256": sha256_file(COVER_INSPECTION),
            "cover_inspection_sha256": selection[
                "cover_inspection_sha256"
            ],
        },
        "selection_sha256": canonical_sha256(selection),
        "selection_summary": {
            key: selection[key]
            for key in (
                "development_event_count",
                "development_signal_dates",
                "development_symbols",
                "warmup_dates",
                "development_dates",
                "settlement_dates",
                "confirmation_dates",
                "confirmation_event_count",
                "confirmation_signal_dates",
                "confirmation_symbols",
            )
        },
        "development_scope": development_scope,
        "confirmation_scope": confirmation_scope,
        "provider": {
            "name": "Massive SIP adjusted daily aggregates",
            "base_url": "https://api.massive.com",
            "adjusted": True,
            "point_in_time_ticker": (
                "same-accession SEC TradingSymbol queried over only its "
                "historical development range"
            ),
        },
        "requests": requests_,
        "request_policy": {
            "authorized_provider_requests": len(requests_),
            "retries_permitted": 0,
            "substitutions_permitted": 0,
            "resume_from_hash_valid_tasks": True,
            "permanent_missing_symbol": "retained_as_missing_history",
            "confirmation_requests_permitted": 0,
        },
        "data_semantics": {
            "daily_ohlcv": "split-adjusted regular-session aggregates",
            "complete_row_validation": True,
            "missing_symbol_or_session": "missed_trade_never_substitute",
            "development_prices_only": True,
        },
        "implementation_hashes": {
            name: sha256_file(PROJECT_ROOT / name)
            for name in (
                "earnings_sec_market_data.py",
                "earnings_sec_market_data_inspection.py",
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
    store: HistoricalDayStore | None = None,
    root: Path = DEFAULT_ROOT,
) -> tuple[Path, dict[str, Any]]:
    value = build_contract(created_at=created_at, store=store)
    path = (
        root
        / "market-data-contract"
        / f"contract-{value['contract_sha256']}.json"
    )
    _write(path, value)
    return path, value


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


def _write_private(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    raw = gzip.compress(v5.canonical_bytes(value), mtime=0)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_bytes(raw)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _read_private(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(gzip.decompress(path.read_bytes()))
    except (OSError, json.JSONDecodeError) as exc:
        raise EarningsSecMarketDataError(
            f"private development task is unreadable: {path.name}"
        ) from exc
    if not isinstance(value, dict):
        raise EarningsSecMarketDataError(
            "private development task must be an object"
        )
    return value


def _fetch_request(
    request: Mapping[str, Any],
    client: MassiveHistoricalClient,
) -> dict[str, Any]:
    try:
        rows = client.fetch_daily_bars(
            str(request["symbol"]),
            str(request["start"]),
            str(request["end"]),
            adjusted=True,
        )
        status = "COMPLETE"
        reason = None
    except HistoricalProviderError as exc:
        if exc.category != "permanent_fidelity":
            raise
        rows = []
        status = "PERMANENT_MISSING"
        reason = str(exc)
    value = {
        "schema_version": 1,
        "request_sha256": request["request_sha256"],
        "symbol": request["symbol"],
        "status": status,
        "missing_reason": reason,
        "rows": rows,
    }
    value["task_sha256"] = v5.self_hash(value, "task_sha256")
    return value


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
    contract = _read(contract_path)
    inspection = _read(inspection_path)
    if not (
        contract.get("contract_sha256")
        == v5.self_hash(contract, "contract_sha256")
        and inspection.get("contract_sha256") == contract["contract_sha256"]
        and inspection.get("state") == "MARKET_DATA_CONTRACT_INSPECTED_READY"
        and inspection.get("valid") is True
        and inspection.get("development_provider_access_authorized") is True
    ):
        raise EarningsSecMarketDataError(
            "development market-data authorization is invalid"
        )
    historical_store = store or HistoricalDayStore.from_env()
    selection = _development_selection(historical_store)
    if canonical_sha256(selection) != contract["selection_sha256"]:
        raise EarningsSecMarketDataError(
            "development metadata selection drifted"
        )
    telemetry: dict[str, Any] = {
        "requests": 0,
        "request_seconds": 0.0,
        "pacing_wait_seconds": 0.0,
        "cache_hits": 0,
        "failures": 0,
    }
    session: _CountingSession | None = None
    client: MassiveHistoricalClient | None = None
    if fetcher is None:
        config = MassiveConfig.optional_from_env(DEFAULT_ENV_PATH)
        if config is None:
            raise EarningsSecMarketDataError(
                "configured Massive read-only data lane is required"
            )
        if config.base_url != contract["provider"]["base_url"]:
            raise EarningsSecMarketDataError(
                "configured Massive base URL differs from the contract"
            )
        session = _CountingSession(telemetry)
        client = MassiveHistoricalClient(config, session=session)  # type: ignore[arg-type]
    tasks: list[dict[str, Any]] = []
    try:
        for request in contract["requests"]:
            path = _task_path(
                historical_store,
                contract["contract_sha256"],
                request["request_sha256"],
            )
            if path.is_file():
                task = _read_private(path)
                if (
                    task.get("request_sha256")
                    != request["request_sha256"]
                    or task.get("task_sha256")
                    != v5.self_hash(task, "task_sha256")
                ):
                    raise EarningsSecMarketDataError(
                        "cached task request hash differs"
                    )
                telemetry["cache_hits"] += 1
            else:
                try:
                    task = (
                        fetcher(request)
                        if fetcher is not None
                        else _fetch_request(request, client)  # type: ignore[arg-type]
                    )
                except HistoricalProviderError:
                    telemetry["failures"] += 1
                    raise
                if "task_sha256" not in task:
                    task["task_sha256"] = v5.self_hash(
                        task, "task_sha256"
                    )
                if not (
                    task.get("request_sha256")
                    == request["request_sha256"]
                    and task["task_sha256"]
                    == v5.self_hash(task, "task_sha256")
                ):
                    raise EarningsSecMarketDataError(
                        "fetched task does not match the frozen request"
                    )
                _write_private(path, task)
                if fetcher is not None:
                    telemetry["requests"] += 1
            tasks.append(task)
    finally:
        if client is not None:
            client.close()
        if session is not None:
            session.close()
    expected_requests = len(contract["requests"])
    if telemetry["requests"] + telemetry["cache_hits"] != expected_requests:
        raise EarningsSecMarketDataError(
            "request and cache accounting is incomplete"
        )
    daily_bars: dict[str, list[dict[str, Any]]] = {}
    missing: dict[str, str] = {}
    for task in tasks:
        symbol = str(task["symbol"])
        if task.get("status") == "COMPLETE":
            daily_bars[symbol] = list(task["rows"])
        elif task.get("status") == "PERMANENT_MISSING":
            missing[symbol] = str(task.get("missing_reason") or "missing")
        else:
            raise EarningsSecMarketDataError(
                "development task status is invalid"
            )
    dataset = {
        "schema_version": 1,
        "family_id": FAMILY_ID,
        "evaluation_dates": selection["development_dates"],
        "event_metadata_by_date": selection["event_metadata_by_date"],
        "daily_bars": daily_bars,
        "missing_symbols": missing,
        "source_semantics": {
            "provider": "Massive SIP adjusted daily aggregates",
            "contract_sha256": contract["contract_sha256"],
            "confirmation_prices_accessed": False,
        },
    }
    relative = (
        Path(PRIVATE_NAMESPACE)
        / contract["contract_sha256"]
        / "development.json.gz"
    )
    private_path = historical_store.root / relative
    _write_private(private_path, dataset)
    value: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": "earnings-sec-development-market-data-collection",
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "successor_id": SUCCESSOR_ID,
        "state": "DEVELOPMENT_DATA_COLLECTED_UNINSPECTED",
        "collected_at": _timestamp(collected_at, "collected_at"),
        "contract_path": _repo_path(contract_path),
        "contract_file_sha256": sha256_file(contract_path),
        "contract_sha256": contract["contract_sha256"],
        "inspection_path": _repo_path(inspection_path),
        "inspection_file_sha256": sha256_file(inspection_path),
        "inspection_sha256": inspection["inspection_sha256"],
        "external_relative_path": str(relative),
        "external_file_sha256": sha256_file(private_path),
        "dataset_sha256": canonical_sha256(dataset),
        "symbols_requested": expected_requests,
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
        / "market-data-collection"
        / f"collection-{value['collection_sha256']}.json"
    )
    _write(path, value)
    outcome_exposure.ensure_record(
        outcome_exposure.build_record(
            exposure_id=(
                f"source-{FAMILY_ID}-"
                f"{value['collection_sha256'][:16]}"
            ),
            campaign_id=CAMPAIGN_ID,
            lane="development",
            recorded_at=value["collected_at"],
            source_path=_repo_path(path),
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
        state = "MARKET_DATA_CONTRACT_FROZEN"
        digest = value["contract_sha256"]
    else:
        path, value = collect_development(
            args.contract,
            args.inspection,
            collected_at=args.collected_at,
        )
        state = value["state"]
        digest = value["collection_sha256"]
    print(
        json.dumps(
            {
                "path": _repo_path(path),
                "sha256": digest,
                "state": state,
                "provider_telemetry": value.get("provider_telemetry", {}),
                "symbols": value.get(
                    "symbols_complete",
                    len(value.get("requests", [])),
                ),
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

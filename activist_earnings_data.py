"""Freeze, collect, and inspect development-only activist earnings daily data."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import time
from collections.abc import Mapping, Sequence
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote

import requests

import activist_earnings_discovery as family
import outcome_exposure
import strategy_discovery
from historical_store import HistoricalDayStore, canonical_sha256, sha256_file
from learning_data import freeze_dataset_contract


PROJECT_ROOT = Path(__file__).resolve().parent
FAMILY_ID = family.FAMILY_ID
CAMPAIGN_ID = family.CAMPAIGN_ID
DEFAULT_ROOT = PROJECT_ROOT / "strategy_tournament/v2/discovery" / FAMILY_ID
PRIVATE_NAMESPACE = Path("_derived/activist_earnings_data")
ENDPOINT_TEMPLATE = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
SOURCE_ID = "yahoo-chart-no-purchase-v1"
REQUEST_START = "2021-12-01"
REQUEST_END = family.DEVELOPMENT_END
PACE_SECONDS = 0.20


class ActivistEarningsDataError(RuntimeError):
    """A frozen request, provider response, or private dataset is invalid."""


def _canonical(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode()


def _hash(value: Any) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _relative(path: Path) -> str:
    return str(path.resolve().relative_to(PROJECT_ROOT.resolve()))


def _timestamp(value: str, field: str) -> str:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ActivistEarningsDataError(f"{field} is invalid") from exc
    if parsed.tzinfo is None:
        raise ActivistEarningsDataError(f"{field} needs a timezone")
    return value


def _period(day: str) -> int:
    return int(datetime.fromisoformat(f"{day}T00:00:00+00:00").timestamp())


def _exclusive_period(day: str) -> int:
    return int(
        (
            datetime.fromisoformat(f"{day}T00:00:00+00:00")
            + timedelta(days=1)
        ).timestamp()
    )


def _write(
    value: Mapping[str, Any], directory: Path, stem: str
) -> tuple[Path, dict[str, Any]]:
    content = dict(value)
    content.pop("artifact_sha256", None)
    digest = _hash(content)
    artifact = {**content, "artifact_sha256": digest}
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{stem}-{digest}.json"
    rendered = json.dumps(artifact, indent=2, sort_keys=True) + "\n"
    if path.exists() and path.read_text(encoding="utf-8") != rendered:
        raise ActivistEarningsDataError("content-addressed artifact collision")
    if not path.exists():
        temporary = path.with_suffix(".json.tmp")
        temporary.write_text(rendered, encoding="utf-8")
        temporary.replace(path)
    return path, artifact


def _load(path: Path, kind: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ActivistEarningsDataError(f"cannot read {path}: {exc}") from exc
    supplied = value.get("artifact_sha256")
    content = {key: item for key, item in value.items() if key != "artifact_sha256"}
    expected = _hash(content)
    if (
        supplied != expected
        or not path.name.endswith(f"-{expected}.json")
        or value.get("artifact_kind") != kind
    ):
        raise ActivistEarningsDataError("artifact hash, name, or kind drifted")
    return value


def _private_inventory(contract: Mapping[str, Any]) -> dict[str, Any]:
    manifest_path = PROJECT_ROOT / str(contract["capacity_manifest"])
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    binding = manifest["dataset_payload"]["activist_earnings_capacity"][
        "private_inventory"
    ]
    if binding.get("storage") != "LOCAL_HISTORICAL_DATA_ROOT":
        raise ActivistEarningsDataError("capacity inventory storage drifted")
    store = HistoricalDayStore.from_env()
    path = store.root / str(binding["relative_path"])
    if not path.is_file() or sha256_file(path) != binding["file_sha256"]:
        raise ActivistEarningsDataError("capacity inventory file drifted")
    with gzip.open(path, "rt", encoding="utf-8") as source:
        value = json.load(source)
    if canonical_sha256(value) != binding["content_sha256"]:
        raise ActivistEarningsDataError("capacity inventory content drifted")
    return value


def _request(symbol: str) -> dict[str, Any]:
    value = {
        "method": "GET",
        "endpoint": ENDPOINT_TEMPLATE.format(symbol=quote(symbol, safe="")),
        "parameters": {
            "period1": _period(REQUEST_START),
            "period2": _exclusive_period(REQUEST_END),
            "interval": "1d",
            "events": "history",
            "includeAdjustedClose": "true",
        },
        "symbol": symbol,
        "start": REQUEST_START,
        "end": REQUEST_END,
    }
    return {**value, "request_sha256": _hash(value)}


def build_contract(search_path: Path, created_at: str) -> dict[str, Any]:
    search = strategy_discovery.load_artifact(
        search_path, expected_kind="frozen-development-search"
    )
    if search["state"] != "SEARCH_FROZEN":
        raise ActivistEarningsDataError("development search is not frozen")
    contract = search["family_contract"]
    inventory = _private_inventory(contract)
    events = inventory["development_events"]
    symbols = sorted({str(row["symbol"]) for row in events})
    if (
        contract["family_id"] != FAMILY_ID
        or len(contract["trial_family"]) != 32
        or len(symbols) != 40
        or any(row["globally_exposed_before_freeze"] is not True for row in events)
    ):
        raise ActivistEarningsDataError(
            "frozen search or contaminated development inventory drifted"
        )
    outcome_exposure.assert_untouched(
        contract["confirmation_scope"], outcome_exposure.read_index()
    )
    requests_ = [_request(symbol) for symbol in symbols]
    return {
        "schema_version": 1,
        "artifact_kind": "activist-earnings-development-source-contract",
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "created_at": _timestamp(created_at, "created_at"),
        "state": "SOURCE_CONTRACT_FROZEN",
        "search_path": _relative(search_path),
        "search_sha256": search["artifact_sha256"],
        "development_dates": contract["development_dates"],
        "development_scope": contract["development_scope"],
        "confirmation_scope": contract["confirmation_scope"],
        "event_inventory_content_sha256": canonical_sha256(inventory),
        "development_event_count": len(events),
        "symbols": symbols,
        "provider": {
            "source_id": SOURCE_ID,
            "name": "Yahoo Finance historical chart JSON",
            "no_purchase_required": True,
            "exchange_timezone_required": "America/New_York",
        },
        "requests": requests_,
        "request_policy": {
            "authorized_provider_requests": len(requests_),
            "pacing_seconds_between_requests": PACE_SECONDS,
            "retries_permitted": 0,
            "substitutions_permitted": 0,
            "resume_from_hash_valid_tasks": True,
            "permanent_missing": "retained_as_missing_trade_data",
            "confirmation_requests_permitted": 0,
        },
        "data_semantics": {
            "raw_ohlcv_used": True,
            "dividend_adjusted_close_used": False,
            "null_rows": "omitted_as_missing_sessions",
            "missing_symbol_or_session": "missed_trade_never_substitute",
            "development_prices_only": True,
        },
        "market_prices_accessed": False,
        "strategy_metrics_computed": 0,
        "confirmation_prices_accessed": False,
        "broker_actions": 0,
    }


def freeze_contract(
    search_path: Path, created_at: str
) -> tuple[Path, dict[str, Any]]:
    strategy_discovery.require_committed(search_path)
    return _write(
        build_contract(search_path, created_at),
        DEFAULT_ROOT / "development-source-contract",
        "source-contract",
    )


def inspect_contract(
    contract_path: Path, inspected_at: str
) -> tuple[Path, dict[str, Any]]:
    strategy_discovery.require_committed(contract_path)
    contract = _load(
        contract_path, "activist-earnings-development-source-contract"
    )
    search_path = PROJECT_ROOT / str(contract["search_path"])
    strategy_discovery.require_committed(search_path)
    rebuilt = build_contract(search_path, str(contract["created_at"]))
    if {
        key: item for key, item in contract.items() if key != "artifact_sha256"
    } != rebuilt:
        raise ActivistEarningsDataError(
            "source contract does not match an independent rebuild"
        )
    payload = {
        "schema_version": 1,
        "artifact_kind": "activist-earnings-development-source-inspection",
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "inspected_at": _timestamp(inspected_at, "inspected_at"),
        "state": "SOURCE_CONTRACT_INSPECTED_READY",
        "contract_path": _relative(contract_path),
        "contract_sha256": contract["artifact_sha256"],
        "search_path": contract["search_path"],
        "search_sha256": contract["search_sha256"],
        "authorized_provider_requests": len(contract["requests"]),
        "confirmation_requests_permitted": 0,
        "inspection": {
            "contract_hash_rebuilt": True,
            "search_binding_rebuilt": True,
            "event_inventory_rebuilt": True,
            "request_hashes_rebuilt": True,
            "development_scope_rebuilt": True,
            "confirmation_untouched_rebuilt": True,
            "zero_preinspection_price_access_rebuilt": True,
            "valid": True,
        },
        "development_provider_access_authorized": True,
        "confirmation_provider_access_authorized": False,
        "market_prices_accessed": False,
        "strategy_metrics_computed": 0,
        "broker_actions": 0,
    }
    return _write(
        payload,
        DEFAULT_ROOT / "development-source-contract-inspection",
        "source-inspection",
    )


def _parse_response(
    request: Mapping[str, Any], payload: Any
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
        return {**value, "task_sha256": _hash(value)}
    if len(results) != 1:
        raise ActivistEarningsDataError("Yahoo returned ambiguous chart results")
    result = results[0]
    meta = result.get("meta") if isinstance(result, Mapping) else None
    timestamps = result.get("timestamp") if isinstance(result, Mapping) else None
    indicators = result.get("indicators") if isinstance(result, Mapping) else None
    quotes = indicators.get("quote") if isinstance(indicators, Mapping) else None
    quote_row = quotes[0] if isinstance(quotes, list) and quotes else None
    if not (
        isinstance(meta, Mapping)
        and str(meta.get("symbol", "")).upper() == request["symbol"]
        and meta.get("exchangeTimezoneName") == "America/New_York"
        and isinstance(timestamps, list)
        and isinstance(quote_row, Mapping)
    ):
        raise ActivistEarningsDataError(
            "Yahoo identity, timezone, or quote arrays drifted"
        )
    arrays = {
        field: quote_row.get(field)
        for field in ("open", "high", "low", "close", "volume")
    }
    if any(
        not isinstance(values, list) or len(values) != len(timestamps)
        for values in arrays.values()
    ):
        raise ActivistEarningsDataError("Yahoo OHLCV arrays are incomplete")
    rows: list[dict[str, Any]] = []
    for index, raw_timestamp in enumerate(timestamps):
        values = [arrays[field][index] for field in arrays]
        if any(item is None for item in values):
            continue
        timezone_name = str(meta["exchangeTimezoneName"])
        day = (
            datetime.fromtimestamp(int(raw_timestamp), timezone.utc)
            .astimezone(family.ZoneInfo(timezone_name))
            .date()
            .isoformat()
        )
        if not request["start"] <= day <= request["end"]:
            raise ActivistEarningsDataError("Yahoo row escaped frozen dates")
        row = {
            "date": day,
            "open": float(arrays["open"][index]),
            "high": float(arrays["high"][index]),
            "low": float(arrays["low"][index]),
            "close": float(arrays["close"][index]),
            "volume": int(arrays["volume"][index]),
        }
        if (
            min(row[field] for field in ("open", "high", "low", "close")) <= 0
            or row["volume"] < 0
            or row["low"] > min(row["open"], row["close"])
            or row["high"] < max(row["open"], row["close"])
        ):
            raise ActivistEarningsDataError("Yahoo returned invalid OHLCV")
        rows.append(row)
    dates = [row["date"] for row in rows]
    if dates != sorted(dates) or len(dates) != len(set(dates)):
        raise ActivistEarningsDataError(
            "Yahoo dates are not unique and chronological"
        )
    value = {
        "schema_version": 1,
        "request_sha256": request["request_sha256"],
        "symbol": request["symbol"],
        "status": "COMPLETE" if rows else "PERMANENT_MISSING",
        "missing_reason": None if rows else "Yahoo returned no rows",
        "rows": rows,
    }
    return {**value, "task_sha256": _hash(value)}


def _fetch(
    request: Mapping[str, Any],
    session: requests.Session,
    telemetry: dict[str, Any],
) -> dict[str, Any]:
    started = time.monotonic()
    telemetry["requests"] += 1
    try:
        response = session.get(
            str(request["endpoint"]),
            params=request["parameters"],
            headers={"User-Agent": "robinhood-codex-historical-research/1.0"},
            timeout=30,
        )
    except requests.RequestException as exc:
        telemetry["failures"] += 1
        raise ActivistEarningsDataError(
            "Yahoo request failed before a response"
        ) from exc
    finally:
        telemetry["request_seconds"] += time.monotonic() - started
    if response.status_code == 400:
        telemetry["permanent_missing_responses"] += 1
        value = {
            "schema_version": 1,
            "request_sha256": request["request_sha256"],
            "symbol": request["symbol"],
            "status": "PERMANENT_MISSING",
            "missing_reason": "Yahoo HTTP 400 retained as permanent missing",
            "rows": [],
        }
        return {**value, "task_sha256": _hash(value)}
    if response.status_code == 404:
        return _parse_response(request, {"chart": {"result": None, "error": {}}})
    if response.status_code >= 400:
        telemetry["failures"] += 1
        raise ActivistEarningsDataError(
            f"Yahoo request returned HTTP {response.status_code}"
        )
    try:
        payload = response.json()
    except ValueError as exc:
        telemetry["failures"] += 1
        raise ActivistEarningsDataError("Yahoo response is not JSON") from exc
    return _parse_response(request, payload)


def _write_private(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    rendered = _canonical(value)
    if path.exists():
        with gzip.open(path, "rb") as source:
            if source.read() != rendered:
                raise ActivistEarningsDataError(
                    "private content-addressed file drifted"
                )
        return
    temporary = path.with_suffix(".json.gz.tmp")
    with temporary.open("wb") as target:
        with gzip.GzipFile(filename="", mode="wb", fileobj=target, mtime=0) as stream:
            stream.write(rendered)
    temporary.replace(path)


def _read_private(path: Path) -> dict[str, Any]:
    with gzip.open(path, "rt", encoding="utf-8") as source:
        value = json.load(source)
    if not isinstance(value, dict):
        raise ActivistEarningsDataError("private input must contain an object")
    return value


def _event_metadata(
    inventory: Mapping[str, Any], development_dates: Sequence[str]
) -> dict[str, list[dict[str, Any]]]:
    result: dict[str, list[dict[str, Any]]] = {
        day: [] for day in development_dates
    }
    for event in inventory["development_events"]:
        reaction_date = str(event["reaction_date"])
        if reaction_date not in result:
            continue
        result[reaction_date].append(
            {
                "symbol": event["symbol"],
                "security_identity_state": "VERIFIED_ACTIVIST_COMMON_EQUITY",
                "reaction_date": reaction_date,
                "sec_form": "8-K",
                "sec_item": "2.02",
                "timing": event["timing"],
                "accepted_at": event["accepted_at"],
                "accession": event["accession"],
                "activist_start_accepted_at": event[
                    "activist_start_accepted_at"
                ],
                "activist_start_accession": event[
                    "activist_start_accession"
                ],
            }
        )
    for day in result:
        result[day] = sorted(
            result[day], key=lambda row: (row["symbol"], row["accession"])
        )
    return result


def _filtered_scope(
    scope: Mapping[str, Any], symbols: set[str]
) -> dict[str, Any]:
    symbols_by_date = {
        day: sorted(set(day_symbols) & symbols)
        for day, day_symbols in scope["symbols_by_date"].items()
        if set(day_symbols) & symbols
    }
    dates = sorted(symbols_by_date)
    if not dates:
        raise ActivistEarningsDataError("partial exposure scope is empty")
    return {
        "dates": dates,
        "symbols_by_date": {
            day: symbols_by_date[day] for day in dates
        },
    }


def build_failure(
    contract_path: Path,
    inspection_path: Path,
    observed_at: str,
) -> dict[str, Any]:
    contract = _load(
        contract_path, "activist-earnings-development-source-contract"
    )
    inspection = _load(
        inspection_path, "activist-earnings-development-source-inspection"
    )
    if not (
        inspection["state"] == "SOURCE_CONTRACT_INSPECTED_READY"
        and inspection["contract_sha256"] == contract["artifact_sha256"]
    ):
        raise ActivistEarningsDataError("failure source authorization drifted")
    store = HistoricalDayStore.from_env()
    task_root = (
        store.root
        / PRIVATE_NAMESPACE
        / contract["artifact_sha256"]
        / "tasks"
    )
    cached_requests: list[dict[str, Any]] = []
    rows_retained = 0
    failed_request: Mapping[str, Any] | None = None
    for ordinal, request in enumerate(contract["requests"], 1):
        task_path = task_root / f"{request['request_sha256']}.json.gz"
        if not task_path.is_file():
            failed_request = request
            break
        task = _read_private(task_path)
        content = {
            key: item for key, item in task.items() if key != "task_sha256"
        }
        if (
            task.get("request_sha256") != request["request_sha256"]
            or task.get("task_sha256") != _hash(content)
        ):
            raise ActivistEarningsDataError("checkpointed task drifted")
        rows_retained += len(task["rows"])
        cached_requests.append(request)
    if (
        failed_request is None
        or failed_request["symbol"] != "GRTX"
        or len(cached_requests) != 16
    ):
        raise ActivistEarningsDataError(
            "observed HTTP 400 request boundary drifted"
        )
    exposed_symbols = {str(request["symbol"]) for request in cached_requests}
    return {
        "schema_version": 1,
        "artifact_kind": "activist-earnings-development-source-failure",
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "observed_at": _timestamp(observed_at, "observed_at"),
        "state": "HTTP_400_SOURCE_POLICY_FAILURE",
        "contract_path": _relative(contract_path),
        "contract_sha256": contract["artifact_sha256"],
        "inspection_path": _relative(inspection_path),
        "inspection_sha256": inspection["artifact_sha256"],
        "failed_request": {
            "ordinal": 17,
            "symbol": failed_request["symbol"],
            "request_sha256": failed_request["request_sha256"],
            "endpoint": failed_request["endpoint"],
        },
        "error": {
            "category": "unregistered_http_status",
            "http_status": 400,
            "sanitized_message": "Yahoo request returned HTTP 400",
        },
        "partial_exposure_scope": _filtered_scope(
            contract["development_scope"], exposed_symbols
        ),
        "failure_boundary": {
            "provider_requests": 17,
            "provider_responses": 17,
            "tasks_checkpointed": 16,
            "rows_retained": rows_retained,
            "symbols_with_retained_rows": sorted(exposed_symbols),
            "development_prices_retained": True,
            "strategy_metrics_computed": 0,
            "winner_selection_executed": False,
            "confirmation_prices_accessed": False,
            "broker_actions": 0,
        },
        "disposition": {
            "failed_symbol_retry_permitted": False,
            "failed_symbol_permanent_missing_registration_permitted": True,
            "same_contract_resume_after_registration_permitted": True,
            "later_http_400_is_permanent_missing": True,
            "substitutions_permitted": 0,
            "confirmation_access_permitted": False,
        },
    }


def record_failure(
    contract_path: Path,
    inspection_path: Path,
    observed_at: str,
) -> tuple[Path, dict[str, Any]]:
    strategy_discovery.require_committed(contract_path)
    strategy_discovery.require_committed(inspection_path)
    path, artifact = _write(
        build_failure(contract_path, inspection_path, observed_at),
        DEFAULT_ROOT / "development-source-failure",
        "source-failure",
    )
    outcome_exposure.ensure_record(
        outcome_exposure.build_record(
            exposure_id=(
                f"development-partial-{FAMILY_ID}-"
                f"{artifact['artifact_sha256'][:16]}"
            ),
            campaign_id=CAMPAIGN_ID,
            lane="development",
            recorded_at=str(artifact["observed_at"]),
            source_path=_relative(path),
            source_sha256=sha256_file(path),
            scope=artifact["partial_exposure_scope"],
        )
    )
    return path, artifact


def inspect_failure(
    failure_path: Path, inspected_at: str
) -> tuple[Path, dict[str, Any]]:
    strategy_discovery.require_committed(failure_path)
    failure = _load(
        failure_path, "activist-earnings-development-source-failure"
    )
    contract_path = PROJECT_ROOT / str(failure["contract_path"])
    inspection_path = PROJECT_ROOT / str(failure["inspection_path"])
    rebuilt = build_failure(
        contract_path, inspection_path, str(failure["observed_at"])
    )
    if {
        key: item for key, item in failure.items() if key != "artifact_sha256"
    } != rebuilt:
        raise ActivistEarningsDataError("source failure rebuild differs")
    matches = [
        record
        for record in outcome_exposure.read_index()
        if record["source_path"] == _relative(failure_path)
    ]
    if not (
        len(matches) == 1
        and matches[0]["scope"] == failure["partial_exposure_scope"]
        and matches[0]["lane"] == "development"
    ):
        raise ActivistEarningsDataError(
            "partial development exposure is not indexed"
        )
    payload = {
        "schema_version": 1,
        "artifact_kind": "activist-earnings-development-source-failure-inspection",
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "inspected_at": _timestamp(inspected_at, "inspected_at"),
        "state": "HTTP_400_FAILURE_INSPECTED_TERMINAL",
        "failure_path": _relative(failure_path),
        "failure_sha256": failure["artifact_sha256"],
        "checks": {
            "failure_exactly_rebuilt": True,
            "failed_request_17_grtx": True,
            "sixteen_tasks_rebuilt": True,
            "partial_exposure_indexed": True,
            "zero_metrics_or_winner": True,
            "confirmation_closed": True,
            "failed_symbol_retry_forbidden": True,
            "permanent_missing_registration_bounded": True,
            "valid": True,
        },
        "failed_request": failure["failed_request"],
        "permanent_missing_registration_authorized": True,
        "same_contract_resume_authorized": True,
        "confirmation_access_permitted": False,
        "broker_actions": 0,
    }
    return _write(
        payload,
        DEFAULT_ROOT / "development-source-failure-inspection",
        "source-failure-inspection",
    )


def register_permanent_missing(
    failure_inspection_path: Path, registered_at: str
) -> tuple[Path, dict[str, Any]]:
    strategy_discovery.require_committed(failure_inspection_path)
    inspection = _load(
        failure_inspection_path,
        "activist-earnings-development-source-failure-inspection",
    )
    if not (
        inspection["state"] == "HTTP_400_FAILURE_INSPECTED_TERMINAL"
        and inspection["permanent_missing_registration_authorized"] is True
        and inspection["same_contract_resume_authorized"] is True
        and inspection["failed_request"]["symbol"] == "GRTX"
    ):
        raise ActivistEarningsDataError(
            "permanent-missing registration is not authorized"
        )
    failure_path = PROJECT_ROOT / str(inspection["failure_path"])
    failure = _load(
        failure_path, "activist-earnings-development-source-failure"
    )
    contract_path = PROJECT_ROOT / str(failure["contract_path"])
    contract = _load(
        contract_path, "activist-earnings-development-source-contract"
    )
    request = contract["requests"][16]
    if request["request_sha256"] != inspection["failed_request"]["request_sha256"]:
        raise ActivistEarningsDataError("failed request binding drifted")
    task_content = {
        "schema_version": 1,
        "request_sha256": request["request_sha256"],
        "symbol": request["symbol"],
        "status": "PERMANENT_MISSING",
        "missing_reason": (
            "Inspected Yahoo HTTP 400 retained without retry or substitution"
        ),
        "rows": [],
    }
    task = {**task_content, "task_sha256": _hash(task_content)}
    store = HistoricalDayStore.from_env()
    task_path = (
        store.root
        / PRIVATE_NAMESPACE
        / contract["artifact_sha256"]
        / "tasks"
        / f"{request['request_sha256']}.json.gz"
    )
    if task_path.exists():
        raise ActivistEarningsDataError(
            "failed request already has a checkpoint; refusing overwrite"
        )
    _write_private(task_path, task)
    payload = {
        "schema_version": 1,
        "artifact_kind": "activist-earnings-permanent-missing-registration",
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "registered_at": _timestamp(registered_at, "registered_at"),
        "state": "PERMANENT_MISSING_REGISTERED_READY_TO_RESUME",
        "failure_inspection_path": _relative(failure_inspection_path),
        "failure_inspection_sha256": inspection["artifact_sha256"],
        "request_sha256": request["request_sha256"],
        "symbol": request["symbol"],
        "task_sha256": task["task_sha256"],
        "provider_requests": 0,
        "retries": 0,
        "substitutions": 0,
        "confirmation_prices_accessed": False,
        "broker_actions": 0,
    }
    return _write(
        payload,
        DEFAULT_ROOT / "development-permanent-missing-registration",
        "permanent-missing",
    )


def collect(
    contract_path: Path,
    inspection_path: Path,
    collected_at: str,
) -> tuple[Path, dict[str, Any]]:
    strategy_discovery.require_committed(contract_path)
    strategy_discovery.require_committed(inspection_path)
    contract = _load(
        contract_path, "activist-earnings-development-source-contract"
    )
    inspection = _load(
        inspection_path, "activist-earnings-development-source-inspection"
    )
    if not (
        inspection["state"] == "SOURCE_CONTRACT_INSPECTED_READY"
        and inspection["contract_sha256"] == contract["artifact_sha256"]
        and inspection["development_provider_access_authorized"] is True
        and inspection["confirmation_provider_access_authorized"] is False
    ):
        raise ActivistEarningsDataError("provider authorization is invalid")
    store = HistoricalDayStore.from_env()
    task_root = (
        store.root
        / PRIVATE_NAMESPACE
        / contract["artifact_sha256"]
        / "tasks"
    )
    telemetry: dict[str, Any] = {
        "requests": 0,
        "request_seconds": 0.0,
        "pacing_wait_seconds": 0.0,
        "cache_hits": 0,
        "failures": 0,
        "permanent_missing_responses": 0,
    }
    tasks: list[dict[str, Any]] = []
    http = requests.Session()
    try:
        for index, request in enumerate(contract["requests"]):
            task_path = task_root / f"{request['request_sha256']}.json.gz"
            if task_path.is_file():
                task = _read_private(task_path)
                telemetry["cache_hits"] += 1
            else:
                if index and telemetry["requests"]:
                    time.sleep(PACE_SECONDS)
                    telemetry["pacing_wait_seconds"] += PACE_SECONDS
                task = _fetch(request, http, telemetry)
                _write_private(task_path, task)
            content = {
                key: item for key, item in task.items() if key != "task_sha256"
            }
            if (
                task.get("request_sha256") != request["request_sha256"]
                or task.get("task_sha256") != _hash(content)
            ):
                raise ActivistEarningsDataError("Yahoo task binding drifted")
            tasks.append(task)
    finally:
        http.close()
    if telemetry["requests"] + telemetry["cache_hits"] != len(
        contract["requests"]
    ):
        raise ActivistEarningsDataError("request accounting is incomplete")
    failure_inspections = sorted(
        (DEFAULT_ROOT / "development-source-failure-inspection").glob(
            "*.json"
        )
    )
    if failure_inspections:
        if len(failure_inspections) != 1:
            raise ActivistEarningsDataError(
                "source failure inspection count is ambiguous"
            )
        failure_inspection = _load(
            failure_inspections[0],
            "activist-earnings-development-source-failure-inspection",
        )
        if not (
            failure_inspection["state"]
            == "HTTP_400_FAILURE_INSPECTED_TERMINAL"
            and failure_inspection["same_contract_resume_authorized"] is True
        ):
            raise ActivistEarningsDataError(
                "source failure resume authorization drifted"
            )
        telemetry["prior_provider_requests"] = 17
        telemetry["prior_failed_responses"] = 1
        telemetry["provider_requests_lifetime"] = (
            17 + telemetry["requests"]
        )
    else:
        telemetry["prior_provider_requests"] = 0
        telemetry["prior_failed_responses"] = 0
        telemetry["provider_requests_lifetime"] = telemetry["requests"]
    search_path = PROJECT_ROOT / str(contract["search_path"])
    search = strategy_discovery.load_artifact(
        search_path, expected_kind="frozen-development-search"
    )
    inventory = _private_inventory(search["family_contract"])
    daily_bars = {
        str(task["symbol"]): list(task["rows"])
        for task in tasks
        if task["status"] == "COMPLETE"
    }
    missing = {
        str(task["symbol"]): str(task["missing_reason"])
        for task in tasks
        if task["status"] == "PERMANENT_MISSING"
    }
    dataset = {
        "schema_version": 1,
        "family_id": FAMILY_ID,
        "sample_phase": "development",
        "evaluation_dates": contract["development_dates"],
        "event_metadata_by_date": _event_metadata(
            inventory, contract["development_dates"]
        ),
        "daily_bars": daily_bars,
        "missing_symbols": missing,
        "source_semantics": {
            "provider": SOURCE_ID,
            "contract_sha256": contract["artifact_sha256"],
            "raw_ohlcv_used": True,
            "dividend_adjusted_close_used": False,
            "confirmation_prices_accessed": False,
        },
    }
    content_sha256 = canonical_sha256(dataset)
    relative = (
        PRIVATE_NAMESPACE
        / contract["artifact_sha256"]
        / content_sha256
        / "development.json.gz"
    )
    private_path = store.root / relative
    _write_private(private_path, dataset)
    payload = {
        "schema_version": 1,
        "artifact_kind": "activist-earnings-development-collection",
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "collected_at": _timestamp(collected_at, "collected_at"),
        "state": "DEVELOPMENT_COLLECTED_UNINSPECTED",
        "contract_path": _relative(contract_path),
        "contract_sha256": contract["artifact_sha256"],
        "inspection_path": _relative(inspection_path),
        "inspection_sha256": inspection["artifact_sha256"],
        "search_path": contract["search_path"],
        "search_sha256": contract["search_sha256"],
        "private_dataset": {
            "storage": "LOCAL_HISTORICAL_DATA_ROOT",
            "relative_path": str(relative),
            "content_sha256": content_sha256,
            "file_sha256": sha256_file(private_path),
            "compression": "gzip",
            "format": "canonical-json",
        },
        "symbols_requested": len(tasks),
        "symbols_complete": len(daily_bars),
        "symbols_permanently_missing": len(missing),
        "row_count": sum(len(rows) for rows in daily_bars.values()),
        "event_count": sum(
            len(rows) for rows in dataset["event_metadata_by_date"].values()
        ),
        "provider_telemetry": telemetry,
        "substitutions": 0,
        "retries": 0,
        "strategy_metrics_computed": 0,
        "confirmation_prices_accessed": False,
        "broker_actions": 0,
    }
    path, artifact = _write(
        payload,
        DEFAULT_ROOT / "development-collection",
        "development-collection",
    )
    outcome_exposure.ensure_record(
        outcome_exposure.build_record(
            exposure_id=(
                f"development-source-{FAMILY_ID}-"
                f"{artifact['artifact_sha256'][:16]}"
            ),
            campaign_id=CAMPAIGN_ID,
            lane="development",
            recorded_at=str(artifact["collected_at"]),
            source_path=_relative(path),
            source_sha256=sha256_file(path),
            scope=contract["development_scope"],
        )
    )
    return path, artifact


def inspect_collection(
    collection_path: Path, inspected_at: str
) -> tuple[Path, Path, dict[str, Any]]:
    strategy_discovery.require_committed(collection_path)
    collection = _load(
        collection_path, "activist-earnings-development-collection"
    )
    private = collection["private_dataset"]
    store = HistoricalDayStore.from_env()
    private_path = store.root / str(private["relative_path"])
    if (
        not private_path.is_file()
        or sha256_file(private_path) != private["file_sha256"]
    ):
        raise ActivistEarningsDataError("private development file drifted")
    dataset = _read_private(private_path)
    if canonical_sha256(dataset) != private["content_sha256"]:
        raise ActivistEarningsDataError("private development content drifted")
    search_path = PROJECT_ROOT / str(collection["search_path"])
    search = strategy_discovery.load_artifact(
        search_path, expected_kind="frozen-development-search"
    )
    contract = search["family_contract"]
    inventory = _private_inventory(contract)
    expected_metadata = _event_metadata(
        inventory, contract["development_dates"]
    )
    bars = dataset.get("daily_bars")
    if not (
        dataset.get("family_id") == FAMILY_ID
        and dataset.get("sample_phase") == "development"
        and dataset.get("evaluation_dates") == contract["development_dates"]
        and dataset.get("event_metadata_by_date") == expected_metadata
        and isinstance(bars, Mapping)
        and collection["event_count"] == sum(map(len, expected_metadata.values()))
        and collection["row_count"]
        == sum(len(rows) for rows in bars.values())
        and collection["symbols_complete"] == len(bars)
        and collection["symbols_complete"]
        + collection["symbols_permanently_missing"]
        == collection["symbols_requested"]
    ):
        raise ActivistEarningsDataError("development dataset rebuild failed")
    for symbol, rows in bars.items():
        if not isinstance(rows, list) or [
            row.get("date") for row in rows
        ] != sorted({str(row.get("date")) for row in rows}):
            raise ActivistEarningsDataError(
                f"daily rows are invalid for {symbol}"
            )
    exposure_matches = [
        record
        for record in outcome_exposure.read_index()
        if record["source_path"] == _relative(collection_path)
    ]
    if not (
        len(exposure_matches) == 1
        and exposure_matches[0]["scope"] == contract["development_scope"]
        and exposure_matches[0]["lane"] == "development"
    ):
        raise ActivistEarningsDataError(
            "development source exposure was not indexed exactly"
        )
    payload = {
        "schema_version": 1,
        "artifact_kind": "activist-earnings-development-collection-inspection",
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "inspected_at": _timestamp(inspected_at, "inspected_at"),
        "state": "DATASET_INSPECTED_READY",
        "collection_path": _relative(collection_path),
        "collection_sha256": collection["artifact_sha256"],
        "search_path": collection["search_path"],
        "search_sha256": collection["search_sha256"],
        "private_dataset": private,
        "symbols_complete": collection["symbols_complete"],
        "symbols_permanently_missing": collection[
            "symbols_permanently_missing"
        ],
        "row_count": collection["row_count"],
        "event_count": collection["event_count"],
        "provider_telemetry": collection["provider_telemetry"],
        "inspection": {
            "collection_hash_rebuilt": True,
            "private_file_hash_rebuilt": True,
            "private_content_hash_rebuilt": True,
            "search_binding_rebuilt": True,
            "event_metadata_rebuilt": True,
            "daily_row_accounting_rebuilt": True,
            "development_exposure_record_rebuilt": True,
            "confirmation_prices_absent": True,
            "valid": True,
        },
        "strategy_metrics_computed": 0,
        "confirmation_prices_accessed": False,
        "broker_actions": 0,
    }
    inspection_path, inspection = _write(
        payload,
        DEFAULT_ROOT / "development-collection-inspection",
        "development-inspection",
    )
    manifest = {
        "schema_version": 1,
        "dataset_id": (
            f"dataset-{FAMILY_ID}-development-"
            f"{collection['search_sha256'][:16]}"
        ),
        "registered_at": inspected_at,
        "requested_dates": contract["development_dates"],
        "dataset_payload": {
            "lane": "development",
            "claim_scope": "DEVELOPMENT_ONLY",
            "evidence_paths": [
                collection["search_path"],
                _relative(collection_path),
                _relative(inspection_path),
                "strategy_tournament/v2/OUTCOME_EXPOSURE_INDEX.jsonl",
            ],
            "inspected": True,
            "point_in_time_evidence": True,
            "development_search_sha256": collection["search_sha256"],
            "activist_earnings_runtime": {
                "family_id": FAMILY_ID,
                "sample_phase": "development",
                "private_dataset": private,
                "collection_inspection_sha256": inspection[
                    "artifact_sha256"
                ],
                "confirmation_prices_accessed": False,
            },
        },
    }
    manifest_path, _manifest = freeze_dataset_contract(
        manifest, DEFAULT_ROOT / "development-dataset"
    )
    return inspection_path, manifest_path, inspection


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    freeze = subparsers.add_parser("freeze-contract")
    freeze.add_argument("search", type=Path)
    freeze.add_argument("--created-at", required=True)
    inspect_source = subparsers.add_parser("inspect-contract")
    inspect_source.add_argument("contract", type=Path)
    inspect_source.add_argument("--inspected-at", required=True)
    failure = subparsers.add_parser("record-failure")
    failure.add_argument("contract", type=Path)
    failure.add_argument("inspection", type=Path)
    failure.add_argument("--observed-at", required=True)
    inspect_failure_parser = subparsers.add_parser("inspect-failure")
    inspect_failure_parser.add_argument("failure", type=Path)
    inspect_failure_parser.add_argument("--inspected-at", required=True)
    register = subparsers.add_parser("register-permanent-missing")
    register.add_argument("failure_inspection", type=Path)
    register.add_argument("--registered-at", required=True)
    collect_parser = subparsers.add_parser("collect")
    collect_parser.add_argument("contract", type=Path)
    collect_parser.add_argument("inspection", type=Path)
    collect_parser.add_argument("--collected-at", required=True)
    inspect_data = subparsers.add_parser("inspect-collection")
    inspect_data.add_argument("collection", type=Path)
    inspect_data.add_argument("--inspected-at", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    if args.command == "freeze-contract":
        path, value = freeze_contract(args.search, args.created_at)
        result: Any = {
            "path": _relative(path),
            "state": value["state"],
            "requests": len(value["requests"]),
            "artifact_sha256": value["artifact_sha256"],
        }
    elif args.command == "inspect-contract":
        path, value = inspect_contract(args.contract, args.inspected_at)
        result = {
            "path": _relative(path),
            "state": value["state"],
            "authorized_provider_requests": value[
                "authorized_provider_requests"
            ],
            "artifact_sha256": value["artifact_sha256"],
        }
    elif args.command == "record-failure":
        path, value = record_failure(
            args.contract, args.inspection, args.observed_at
        )
        result = {
            "path": _relative(path),
            "state": value["state"],
            "failed_request": value["failed_request"],
            "failure_boundary": value["failure_boundary"],
            "artifact_sha256": value["artifact_sha256"],
        }
    elif args.command == "inspect-failure":
        path, value = inspect_failure(args.failure, args.inspected_at)
        result = {
            "path": _relative(path),
            "state": value["state"],
            "checks": value["checks"],
            "artifact_sha256": value["artifact_sha256"],
        }
    elif args.command == "register-permanent-missing":
        path, value = register_permanent_missing(
            args.failure_inspection, args.registered_at
        )
        result = {
            "path": _relative(path),
            "state": value["state"],
            "symbol": value["symbol"],
            "provider_requests": value["provider_requests"],
            "artifact_sha256": value["artifact_sha256"],
        }
    elif args.command == "collect":
        path, value = collect(
            args.contract, args.inspection, args.collected_at
        )
        result = {
            "path": _relative(path),
            "state": value["state"],
            "symbols_complete": value["symbols_complete"],
            "symbols_permanently_missing": value[
                "symbols_permanently_missing"
            ],
            "row_count": value["row_count"],
            "provider_telemetry": value["provider_telemetry"],
            "artifact_sha256": value["artifact_sha256"],
        }
    else:
        inspection, manifest, value = inspect_collection(
            args.collection, args.inspected_at
        )
        result = {
            "inspection": _relative(inspection),
            "manifest": _relative(manifest),
            "state": value["state"],
            "row_count": value["row_count"],
            "artifact_sha256": value["artifact_sha256"],
        }
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

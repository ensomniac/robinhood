"""Freeze and collect development-only SPY data for the fixed RSI(2) rule."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import math
import os
import shutil
import time
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import requests

import dense_strategy_runtime as runtime
import outcome_exposure
import strategy_discovery
from historical_providers import MassiveConfig
from historical_store import (
    DEFAULT_ENV_PATH,
    HistoricalStoreConfig,
    canonical_json_bytes,
    canonical_sha256,
    sha256_file,
)


PROJECT_ROOT = Path(__file__).resolve().parent
CAMPAIGN_ID = "multi-strategy-portfolio-validation-v2"
FAMILY_ID = runtime.SPY_RSI2_PULLBACK_FAMILY
SUCCESSOR_ID = "broad-etf-trend-pullback-v3-fixed-spy-rsi2"
DEFAULT_ROOT = PROJECT_ROOT / "strategy_tournament/v2/continuous" / SUCCESSOR_ID
EASTERN = ZoneInfo("America/New_York")

WARMUP_START = date(1997, 1, 2)
DEVELOPMENT_START = date(1998, 1, 2)
DEVELOPMENT_END = date(2005, 12, 30)
CONFIRMATION_END = date(2013, 12, 31)
MAX_ROWS = 50_000
EXTRAORDINARY_CLOSURES = {
    date(2001, 9, 11),
    date(2001, 9, 12),
    date(2001, 9, 13),
    date(2001, 9, 14),
    date(2004, 6, 11),
    date(2007, 1, 2),
    date(2012, 10, 29),
    date(2012, 10, 30),
}
PARAMETERS = {
    "trend_sma": 200,
    "rsi2_maximum": 10.0,
    "mean_reversion_sma": 5,
    "stop_atr14": 1.5,
    "maximum_hold_sessions": 5,
}


class SpyRsi2DataError(RuntimeError):
    """A frozen SPY source contract, request, or retained dataset drifted."""


def canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode()


def self_hash(value: Mapping[str, Any], field: str) -> str:
    return hashlib.sha256(
        canonical_bytes({key: item for key, item in value.items() if key != field})
    ).hexdigest()


def _timestamp(value: str, field: str) -> str:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise SpyRsi2DataError(f"{field} must be an ISO timestamp") from exc
    if parsed.tzinfo is None:
        raise SpyRsi2DataError(f"{field} must include a timezone")
    return parsed.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _repo_path(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(PROJECT_ROOT.resolve()))
    except ValueError as exc:
        raise SpyRsi2DataError(f"path is outside the repository: {path}") from exc


def _read(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SpyRsi2DataError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise SpyRsi2DataError(f"{path} must contain an object")
    return value


def _write(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    rendered = json.dumps(value, indent=2, sort_keys=True) + "\n"
    if path.exists() and path.read_text(encoding="utf-8") != rendered:
        raise SpyRsi2DataError(f"hash-addressed artifact drifted: {path}")
    path.write_text(rendered, encoding="utf-8")


def _easter_sunday(year: int) -> date:
    """Gregorian Easter using the anonymous Gregorian computus."""

    a = year % 19
    b, c = divmod(year, 100)
    d, e = divmod(b, 4)
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i, k = divmod(c, 4)
    ell = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * ell) // 451
    month = (h + ell - 7 * m + 114) // 31
    day = (h + ell - 7 * m + 114) % 31 + 1
    return date(year, month, day)


def _nth_weekday(year: int, month: int, weekday: int, ordinal: int) -> date:
    first = date(year, month, 1)
    offset = (weekday - first.weekday()) % 7
    return first + timedelta(days=offset + 7 * (ordinal - 1))


def _last_weekday(year: int, month: int, weekday: int) -> date:
    following = date(year + (month == 12), month % 12 + 1, 1)
    result = following - timedelta(days=1)
    return result - timedelta(days=(result.weekday() - weekday) % 7)


def _observed(day: date, *, saturday_unobserved: bool = False) -> date | None:
    if day.weekday() == 5:
        return None if saturday_unobserved else day - timedelta(days=1)
    if day.weekday() == 6:
        return day + timedelta(days=1)
    return day


def _closures(year: int) -> set[date]:
    closures = {
        _nth_weekday(year, 2, 0, 3),
        _easter_sunday(year) - timedelta(days=2),
        _last_weekday(year, 5, 0),
        _nth_weekday(year, 9, 0, 1),
        _nth_weekday(year, 11, 3, 4),
    }
    if year >= 1998:
        closures.add(_nth_weekday(year, 1, 0, 3))
    for holiday, saturday_unobserved in (
        (date(year, 1, 1), True),
        (date(year, 7, 4), False),
        (date(year, 12, 25), False),
    ):
        observed = _observed(
            holiday,
            saturday_unobserved=saturday_unobserved,
        )
        if observed is not None:
            closures.add(observed)
    return closures | {day for day in EXTRAORDINARY_CLOSURES if day.year == year}


def xnys_sessions(start: date, end: date) -> list[str]:
    if end < start:
        raise SpyRsi2DataError("XNYS range end precedes start")
    closures = {
        day
        for year in range(start.year, end.year + 1)
        for day in _closures(year)
    }
    result: list[str] = []
    current = start
    while current <= end:
        if current.weekday() < 5 and current not in closures:
            result.append(current.isoformat())
        current += timedelta(days=1)
    return result


def partitions() -> dict[str, list[str]]:
    all_sessions = xnys_sessions(WARMUP_START, CONFIRMATION_END)
    warmup = [day for day in all_sessions if day < DEVELOPMENT_START.isoformat()]
    development = [
        day
        for day in all_sessions
        if DEVELOPMENT_START.isoformat() <= day <= DEVELOPMENT_END.isoformat()
    ]
    after_development = [
        day for day in all_sessions if day > DEVELOPMENT_END.isoformat()
    ]
    embargo = after_development[:5]
    confirmation = after_development[5:]
    return {
        "warmup_dates": warmup,
        "development_dates": development,
        "embargo_dates": embargo,
        "confirmation_dates": confirmation,
    }


def build_source_contract(*, created_at: str) -> dict[str, Any]:
    for path in (
        Path(__file__).resolve(),
        PROJECT_ROOT / "spy_rsi2_data_inspection.py",
        PROJECT_ROOT / "dense_strategy_runtime.py",
        PROJECT_ROOT / "dense_strategy_plugin.py",
    ):
        strategy_discovery.require_committed(path)
    created = _timestamp(created_at, "created_at")
    split = partitions()
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
    request = {
        "method": "GET",
        "path": (
            f"/v2/aggs/ticker/SPY/range/1/day/"
            f"{split['warmup_dates'][0]}/{split['development_dates'][-1]}"
        ),
        "parameters": {
            "adjusted": "true",
            "sort": "asc",
            "limit": MAX_ROWS,
        },
    }
    request["request_sha256"] = hashlib.sha256(canonical_bytes(request)).hexdigest()
    value: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": "spy-rsi2-development-source-contract",
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "successor_id": SUCCESSOR_ID,
        "created_at": created,
        "calendar": {
            "exchange": "XNYS",
            "regular_holiday_rules": (
                "New Year, MLK from 1998, Washington birthday, Good Friday, "
                "Memorial, Independence, Labor, Thanksgiving, Christmas"
            ),
            "extraordinary_full_day_closures": sorted(
                day.isoformat() for day in EXTRAORDINARY_CLOSURES
            ),
            "calendar_builder_sha256": sha256_file(Path(__file__).resolve()),
        },
        **split,
        "development_scope": development_scope,
        "confirmation_scope": confirmation_scope,
        "provider": "Massive SIP adjusted daily aggregates",
        "development_request": request,
        "request_policy": {
            "authorized_provider_requests": 1,
            "unexpected_pagination_fails_closed": True,
            "retries_permitted": 0,
            "substitutions_permitted": 0,
            "confirmation_request_permitted": False,
        },
        "frozen_rule": {
            "selection_mode": "development_search",
            "trial_count": 1,
            "parameters": dict(PARAMETERS),
            "long_only": True,
            "entry": "next XNYS session open",
            "cost_floor": (
                "completed SMA5 divided by observable entry open minus one "
                "must be at least 0.005"
            ),
            "stop": "1.5 times completed ATR14 below entry",
            "exit": "stop first, completed SMA5 reclaim close, or fifth close",
            "costs_bps_per_side": [5, 10, 20],
        },
        "related_adverse_history": {
            "family_id": runtime.ETF_PULLBACK_FAMILY,
            "finding": (
                "The earlier 32-trial ETF pullback family was rejected; this "
                "successor removes the three-session-decline condition and "
                "uses a dynamic completed-SMA5 exit without reusing evidence."
            ),
            "promotion_evidence_reused": False,
        },
        "implementation_hashes": {
            name: sha256_file(PROJECT_ROOT / name)
            for name in (
                "spy_rsi2_data.py",
                "spy_rsi2_data_inspection.py",
                "dense_strategy_runtime.py",
                "dense_strategy_plugin.py",
            )
        },
        "market_prices_accessed": False,
        "strategy_metrics_computed": 0,
        "confirmation_prices_accessed": False,
        "broker_actions": 0,
    }
    value["contract_sha256"] = self_hash(value, "contract_sha256")
    return value


def freeze_source_contract(
    *,
    created_at: str,
    root: Path = DEFAULT_ROOT,
) -> tuple[Path, dict[str, Any]]:
    value = build_source_contract(created_at=created_at)
    path = (
        root
        / "source-contract"
        / f"contract-{value['contract_sha256']}.json"
    )
    _write(path, value)
    return path, value


def _fetch_development(
    contract: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    config = MassiveConfig.optional_from_env(DEFAULT_ENV_PATH)
    if config is None:
        raise SpyRsi2DataError("MASSIVE_API_KEY is not configured")
    request = contract["development_request"]
    url = f"{config.base_url}{request['path']}"
    params = {**request["parameters"], "apiKey": config.api_key}
    started = time.monotonic()
    try:
        response = requests.get(url, params=params, timeout=config.timeout_seconds)
    except requests.RequestException as exc:
        raise SpyRsi2DataError(
            "Massive development request failed before a response"
        ) from exc
    elapsed = time.monotonic() - started
    if response.status_code >= 400:
        raise SpyRsi2DataError(
            f"Massive development request returned HTTP {response.status_code}"
        )
    try:
        payload = response.json()
    except ValueError as exc:
        raise SpyRsi2DataError("Massive development response is not JSON") from exc
    if (
        not isinstance(payload, Mapping)
        or str(payload.get("status", "")).upper() not in {"OK", "DELAYED"}
        or payload.get("next_url")
        or not isinstance(payload.get("results"), list)
    ):
        raise SpyRsi2DataError(
            "Massive development response requires substitution or pagination"
        )
    rows: list[dict[str, Any]] = []
    for raw in payload["results"]:
        if not isinstance(raw, Mapping):
            raise SpyRsi2DataError("Massive development row is not an object")
        try:
            observed = datetime.fromtimestamp(float(raw["t"]) / 1000, UTC)
            rows.append(
                {
                    "date": observed.astimezone(EASTERN).date().isoformat(),
                    "open": float(raw["o"]),
                    "high": float(raw["h"]),
                    "low": float(raw["l"]),
                    "close": float(raw["c"]),
                    "volume": int(float(raw["v"])),
                }
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise SpyRsi2DataError(
                "Massive development row is malformed"
            ) from exc
    return rows, {
        "requests": 1,
        "request_seconds": elapsed,
        "pacing_wait_seconds": 0.0,
        "cache_hits": 0,
        "failures": 0,
    }


def _write_private(
    path: Path,
    value: Mapping[str, Any],
    config: HistoricalStoreConfig,
) -> None:
    encoded = gzip.compress(canonical_json_bytes(value) + b"\n", mtime=0)
    config.root.mkdir(parents=True, exist_ok=True)
    if shutil.disk_usage(config.root).free - len(encoded) < config.min_free_bytes:
        raise SpyRsi2DataError("historical store disk reserve would be breached")
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        if path.read_bytes() != encoded:
            raise SpyRsi2DataError("immutable private SPY dataset drifted")
        return
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_bytes(encoded)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _ensure_development_exposure(
    path: Path,
    collection: Mapping[str, Any],
    dates: Sequence[str],
) -> None:
    outcome_exposure.ensure_record(
        outcome_exposure.build_record(
            exposure_id=(
                f"source-{FAMILY_ID}-"
                f"{str(collection['collection_sha256'])[:16]}"
            ),
            campaign_id=CAMPAIGN_ID,
            lane="development",
            recorded_at=str(collection["collected_at"]),
            source_path=_repo_path(path),
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
    contract = _read(contract_path)
    inspection = _read(inspection_path)
    if not (
        contract.get("contract_sha256")
        == self_hash(contract, "contract_sha256")
        and inspection.get("contract_sha256") == contract["contract_sha256"]
        and inspection.get("state") == "SOURCE_CONTRACT_INSPECTED_READY"
        and inspection.get("valid") is True
    ):
        raise SpyRsi2DataError("development source authorization is invalid")
    observed_at = _timestamp(collected_at, "collected_at")
    expected_dates = [
        *contract["warmup_dates"],
        *contract["development_dates"],
    ]
    existing = []
    for path in sorted(
        (root / "development-collection").glob("collection-*.json")
    ):
        value = _read(path)
        if (
            value.get("contract_sha256") == contract["contract_sha256"]
            and value.get("state") == "DEVELOPMENT_COLLECTED_UNINSPECTED"
        ):
            existing.append((path, value))
    if len(existing) > 1:
        raise SpyRsi2DataError("multiple development collections bind one contract")
    if existing:
        _ensure_development_exposure(
            existing[0][0],
            existing[0][1],
            expected_dates,
        )
        return existing[0]
    store = store_config or HistoricalStoreConfig.from_env()
    relative = (
        Path("_derived")
        / "spy_rsi2_trend_pullback"
        / contract["contract_sha256"]
        / "development.json.gz"
    )
    private_path = store.root / relative
    if private_path.exists():
        try:
            with gzip.open(private_path, "rt", encoding="utf-8") as handle:
                dataset = json.load(handle)
        except (OSError, json.JSONDecodeError) as exc:
            raise SpyRsi2DataError(
                "resumable private SPY dataset is unreadable"
            ) from exc
        source_collection = dataset.get("source_collection", {})
        telemetry = source_collection.get("provider_telemetry")
        rows = dataset.get("daily_bars", {}).get("SPY", [])
        if (
            not isinstance(telemetry, Mapping)
            or telemetry.get("requests") != 1
            or source_collection.get("contract_sha256")
            != contract["contract_sha256"]
        ):
            raise SpyRsi2DataError(
                "resumable private SPY dataset lacks exact request provenance"
            )
    else:
        rows, telemetry = (fetcher or _fetch_development)(contract)
        dataset = {
            "family_id": FAMILY_ID,
            "evaluation_dates": list(contract["development_dates"]),
            "symbols": ["SPY"],
            "daily_bars": {"SPY": rows},
            "source_collection": {
                "contract_sha256": contract["contract_sha256"],
                "provider_telemetry": telemetry,
            },
        }
    if (
        [row.get("date") for row in rows] != expected_dates
        or len(rows) != len(expected_dates)
        or any(
            isinstance(row.get(field), bool)
            or not isinstance(row.get(field), (int, float))
            or not math.isfinite(float(row[field]))
            for row in rows
            for field in ("open", "high", "low", "close", "volume")
        )
    ):
        raise SpyRsi2DataError(
            "development bars do not match the frozen XNYS denominator"
        )
    runtime.prepare_dataset(dataset)
    _write_private(private_path, dataset, store)
    value: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": "spy-rsi2-development-collection",
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "successor_id": SUCCESSOR_ID,
        "state": "DEVELOPMENT_COLLECTED_UNINSPECTED",
        "collected_at": observed_at,
        "contract_path": _repo_path(contract_path),
        "contract_file_sha256": sha256_file(contract_path),
        "contract_sha256": contract["contract_sha256"],
        "inspection_path": _repo_path(inspection_path),
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
        raise SpyRsi2DataError("development collection must use exactly one request")
    value["collection_sha256"] = self_hash(value, "collection_sha256")
    path = (
        root
        / "development-collection"
        / f"collection-{value['collection_sha256']}.json"
    )
    _write(path, value)
    _ensure_development_exposure(path, value, expected_dates)
    return path, value


def record_collection_failure(
    contract_path: Path,
    inspection_path: Path,
    *,
    failed_at: str,
    http_status: int,
    root: Path = DEFAULT_ROOT,
) -> tuple[Path, dict[str, Any]]:
    strategy_discovery.require_committed(contract_path)
    strategy_discovery.require_committed(inspection_path)
    contract = _read(contract_path)
    inspection = _read(inspection_path)
    if not (
        contract.get("contract_sha256")
        == self_hash(contract, "contract_sha256")
        and inspection.get("contract_sha256") == contract["contract_sha256"]
        and inspection.get("state") == "SOURCE_CONTRACT_INSPECTED_READY"
        and inspection.get("valid") is True
        and http_status in {401, 403}
    ):
        raise SpyRsi2DataError("development source failure binding is invalid")
    value: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": "spy-rsi2-development-collection-failure",
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "successor_id": SUCCESSOR_ID,
        "state": "DEVELOPMENT_SOURCE_PERMANENTLY_UNAVAILABLE",
        "failed_at": _timestamp(failed_at, "failed_at"),
        "contract_path": _repo_path(contract_path),
        "contract_file_sha256": sha256_file(contract_path),
        "contract_sha256": contract["contract_sha256"],
        "inspection_path": _repo_path(inspection_path),
        "inspection_file_sha256": sha256_file(inspection_path),
        "request_sha256": contract["development_request"]["request_sha256"],
        "http_status": http_status,
        "failure_category": "permanent_permission",
        "provider_telemetry": {
            "requests": 1,
            "request_seconds": None,
            "pacing_wait_seconds": 0.0,
            "cache_hits": 0,
            "failures": 1,
        },
        "retained_rows": 0,
        "private_artifacts_created": 0,
        "retries": 0,
        "substitutions": 0,
        "strategy_metrics_computed": 0,
        "confirmation_prices_accessed": False,
        "broker_actions": 0,
    }
    value["failure_sha256"] = self_hash(value, "failure_sha256")
    path = (
        root
        / "development-collection-failure"
        / f"failure-{value['failure_sha256']}.json"
    )
    _write(path, value)
    return path, value


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    freeze = subparsers.add_parser("freeze-source")
    freeze.add_argument("--created-at", required=True)
    collect = subparsers.add_parser("collect-development")
    collect.add_argument("contract", type=Path)
    collect.add_argument("inspection", type=Path)
    collect.add_argument("--collected-at", required=True)
    failure = subparsers.add_parser("record-failure")
    failure.add_argument("contract", type=Path)
    failure.add_argument("inspection", type=Path)
    failure.add_argument("--failed-at", required=True)
    failure.add_argument("--http-status", type=int, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "freeze-source":
            path, value = freeze_source_contract(created_at=args.created_at)
        elif args.command == "collect-development":
            path, value = collect_development(
                args.contract,
                args.inspection,
                collected_at=args.collected_at,
            )
        else:
            path, value = record_collection_failure(
                args.contract,
                args.inspection,
                failed_at=args.failed_at,
                http_status=args.http_status,
            )
        print(
            json.dumps(
                {
                    "path": str(path),
                    "state": value.get("state", "SOURCE_FROZEN"),
                    "sha256": value.get(
                        "collection_sha256",
                        value.get(
                            "failure_sha256",
                            value.get("contract_sha256"),
                        ),
                    ),
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    except (
        SpyRsi2DataError,
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

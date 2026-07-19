"""Collect the frozen dynamic scanner inputs from Alpaca historical SIP data.

The first scanner contract assumed Massive market-wide flat files.  This adapter
provides a non-S3 contract without weakening the information set: raw SIP
15-minute bars between 09:30 and 16:00 supply minute-derived regular-session
ADV/ATR inputs and raw SIP 09:30-09:34 minute bars supply every opening-window
input.  Provider observations are retained in the canonical external
per-symbol/day store.  A deterministic external CSV index is derived solely to
reuse the already-frozen scanner calculation engine.

This module has no broker or order surface.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import json
import os
import sys
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, time as wall_time, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import requests
from dotenv import dotenv_values

from historical_store import (
    HistoricalDayStore,
    HistoricalStoreConfig,
    HistoricalStoreError,
    build_dataset,
    compact_bar,
)
from learning_data import (
    LearningDataError,
    load_security_master,
    security_master_sha256,
    security_record_covers,
    validate_dataset_payload,
)
from scanner_replay import (
    EASTERN,
    MassiveReferenceConfig,
    PROJECT_ROOT,
    ScannerReplayError,
    _sha256_file,
    _sha256_json,
    _write_json,
    build_scanner_replay,
    collect_split_actions,
    load_calendar,
    load_selection,
    required_sessions,
    validate_rules,
    write_security_master_snapshot,
)


UTC = timezone.utc
DATASET_ID = "dataset-production-scanner-replay-2026-07-19-v4"
ALPACA_BARS_URL = "https://data.alpaca.markets/v2/stocks/bars"
DEFAULT_SELECTION = (
    PROJECT_ROOT
    / "historical_batches"
    / "scanner_replay"
    / "selection-2026-07-18-20-days.json"
)
DEFAULT_RULES = (
    PROJECT_ROOT / "historical_batches" / "scanner_replay" / "scanner-rules-v2.json"
)
DEFAULT_CALENDAR = (
    PROJECT_ROOT
    / "historical_batches"
    / "scanner_replay"
    / "session-calendar-2025-12-through-2026-06.json"
)
DEFAULT_SECURITY_MASTER = PROJECT_ROOT / "learning" / "SECURITY_MASTER.jsonl"
DEFAULT_SECURITY_SOURCE = (
    PROJECT_ROOT
    / "historical_batches"
    / "scanner_replay"
    / "security-master-source.json"
)
DEFAULT_MANIFEST_ROOT = (
    PROJECT_ROOT / "historical_batches" / "scanner_replay" / "manifests"
)
DEFAULT_RUN_ROOT = PROJECT_ROOT / "learning_runs" / "scanner_replay_alpaca"
DEFAULT_SUMMARY = PROJECT_ROOT / "research_results" / "2026-07-19-scanner-replay.json"
DEFAULT_SPLITS = PROJECT_ROOT / "learning_runs" / "scanner_replay" / "splits.json.gz"
DEFAULT_SPLIT_SOURCE = (
    PROJECT_ROOT
    / "historical_batches"
    / "scanner_replay"
    / "split-actions-source.json"
)
DEFAULT_STRATEGY_SOURCE = (
    PROJECT_ROOT
    / "historical_batches"
    / "scanner_expansion"
    / "production-strategy-source.json"
)
CSV_FIELDS = (
    "ticker",
    "volume",
    "open",
    "close",
    "high",
    "low",
    "window_start",
    "transactions",
)


def _read_object(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ScannerReplayError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise ScannerReplayError(f"{path} must contain an object")
    return value


def _repo_path(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(PROJECT_ROOT))
    except ValueError as exc:
        raise ScannerReplayError(
            f"path must remain inside the repository: {path}"
        ) from exc


def _timestamp_now() -> str:
    return datetime.now(EASTERN).isoformat()


def _gzip_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".partial")
    with temporary.open("wb") as raw:
        with gzip.GzipFile(fileobj=raw, mode="wb", mtime=0) as compressed:
            import io

            with io.TextIOWrapper(compressed, encoding="utf-8", newline="") as target:
                writer = csv.DictWriter(target, fieldnames=CSV_FIELDS)
                writer.writeheader()
                writer.writerows(rows)
    temporary.replace(path)


def _read_csv_rows(path: Path) -> list[dict[str, Any]]:
    try:
        with gzip.open(path, "rt", encoding="utf-8", newline="") as source:
            return [dict(row) for row in csv.DictReader(source)]
    except (OSError, csv.Error) as exc:
        raise ScannerReplayError(f"cannot read reusable scanner index {path}") from exc


def _split_event_count(path: Path) -> int:
    try:
        with gzip.open(path, "rt", encoding="utf-8") as source:
            rows = json.load(source)
    except (OSError, json.JSONDecodeError) as exc:
        raise ScannerReplayError(f"cannot read split actions {path}") from exc
    if not isinstance(rows, list):
        raise ScannerReplayError("split actions must contain an array")
    return len(rows)


def _validate_index_attestation(
    attestation: Mapping[str, Any],
    *,
    day: str,
    dataset_id: str,
    source_path: Path,
) -> None:
    expected_source = {
        "provider": "Alpaca",
        "endpoint": ALPACA_BARS_URL,
        "feed": "sip",
        "adjustment": "raw",
        "asof": "-",
    }
    if (
        attestation.get("schema_version") != 1
        or attestation.get("dataset_id") != dataset_id
        or attestation.get("date") != day
        or attestation.get("status") != "READY"
        or attestation.get("source") != expected_source
        or attestation.get("source_sha256") != _sha256_file(source_path)
    ):
        raise ScannerReplayError(f"scanner index failed attestation for {day}")
    if attestation.get("reused_source") is not None and not isinstance(
        attestation["reused_source"], Mapping
    ):
        raise ScannerReplayError(f"scanner reuse provenance is malformed for {day}")


@dataclass(frozen=True)
class AlpacaBulkConfig:
    api_key: str
    api_secret: str
    base_url: str = "https://data.alpaca.markets"
    timeout_seconds: float = 60.0
    minimum_interval_seconds: float = 0.32
    batch_size: int = 500
    max_attempts: int = 8

    @classmethod
    def from_env(cls, path: Path) -> "AlpacaBulkConfig":
        values: dict[str, Any] = {}
        if path.exists():
            values.update(dotenv_values(path, interpolate=False))
        names = (
            "ALPACA_KEY",
            "ALPACA_SECRET",
            "APCA_API_KEY_ID",
            "APCA_API_SECRET_KEY",
            "ALPACA_DATA_BASE_URL",
            "APCA_API_DATA_URL",
            "ALPACA_TIMEOUT_SECONDS",
            "ALPACA_SCANNER_MINIMUM_INTERVAL_SECONDS",
            "ALPACA_SCANNER_BATCH_SIZE",
        )
        for name in names:
            if name in os.environ:
                values[name] = os.environ[name]
        key = str(
            values.get("ALPACA_KEY") or values.get("APCA_API_KEY_ID") or ""
        ).strip()
        secret = str(
            values.get("ALPACA_SECRET") or values.get("APCA_API_SECRET_KEY") or ""
        ).strip()
        if not key or not secret:
            raise ScannerReplayError(
                "Alpaca key and secret are required for scanner collection",
                category="configuration",
            )
        base_url = str(
            values.get("ALPACA_DATA_BASE_URL")
            or values.get("APCA_API_DATA_URL")
            or "https://data.alpaca.markets"
        ).rstrip("/")
        parsed = urlparse(base_url)
        if parsed.scheme != "https" or parsed.netloc not in {
            "data.alpaca.markets",
            "data.sandbox.alpaca.markets",
        }:
            raise ScannerReplayError(
                "Alpaca bulk collection requires an official HTTPS data host"
            )
        try:
            timeout = float(values.get("ALPACA_TIMEOUT_SECONDS") or 60.0)
            interval = float(
                values.get("ALPACA_SCANNER_MINIMUM_INTERVAL_SECONDS") or 0.32
            )
            batch_size = int(values.get("ALPACA_SCANNER_BATCH_SIZE") or 500)
        except (TypeError, ValueError) as exc:
            raise ScannerReplayError(
                "Alpaca scanner timing and batch values must be numeric"
            ) from exc
        if timeout <= 0 or interval < 0 or not 1 <= batch_size <= 500:
            raise ScannerReplayError(
                "Alpaca scanner timing or batch values are out of range"
            )
        return cls(key, secret, base_url, timeout, interval, batch_size)

    def public_dict(self) -> dict[str, Any]:
        return {
            "base_url": self.base_url,
            "timeout_seconds": self.timeout_seconds,
            "minimum_interval_seconds": self.minimum_interval_seconds,
            "batch_size": self.batch_size,
            "feed": "sip",
            "adjustment": "raw",
            "asof": "-",
            "credentials_configured": bool(self.api_key and self.api_secret),
        }


class AlpacaBulkBarsClient:
    """Rate-limited, retrying Alpaca multi-symbol historical bars client."""

    def __init__(
        self,
        config: AlpacaBulkConfig,
        *,
        session: requests.Session | None = None,
        sleeper: Any = time.sleep,
        monotonic: Any = time.monotonic,
    ):
        self.config = config
        self.session = session or requests.Session()
        self._owns_session = session is None
        self._sleeper = sleeper
        self._monotonic = monotonic
        self._last_request_started: float | None = None
        self.request_count = 0
        self.request_seconds = 0.0
        self.retry_count = 0

    def close(self) -> None:
        if self._owns_session:
            self.session.close()

    def __enter__(self) -> "AlpacaBulkBarsClient":
        return self

    def __exit__(self, exc_type, exc, traceback_obj) -> None:
        self.close()

    def _throttle(self) -> None:
        if self._last_request_started is not None:
            remaining = self.config.minimum_interval_seconds - (
                self._monotonic() - self._last_request_started
            )
            if remaining > 0:
                self._sleeper(remaining)
        self._last_request_started = self._monotonic()

    @staticmethod
    def _retry_delay(response: requests.Response | Any, attempt: int) -> float:
        retry_after = response.headers.get("Retry-After")
        if retry_after:
            try:
                return min(60.0, max(0.0, float(retry_after)))
            except ValueError:
                pass
        reset = response.headers.get("X-RateLimit-Reset")
        if reset:
            try:
                return min(60.0, max(0.0, float(reset) - time.time() + 0.1))
            except ValueError:
                pass
        return min(60.0, float(2**attempt))

    def _get(self, params: Mapping[str, Any]) -> Mapping[str, Any]:
        attempts = 0
        while True:
            attempts += 1
            self._throttle()
            started = self._monotonic()
            self.request_count += 1
            try:
                response = self.session.get(
                    f"{self.config.base_url}/v2/stocks/bars",
                    params=dict(params),
                    headers={
                        "APCA-API-KEY-ID": self.config.api_key,
                        "APCA-API-SECRET-KEY": self.config.api_secret,
                    },
                    timeout=self.config.timeout_seconds,
                )
            except (requests.Timeout, requests.ConnectionError) as exc:
                self.request_seconds += self._monotonic() - started
                if attempts >= self.config.max_attempts:
                    raise ScannerReplayError(
                        "Alpaca scanner transport retries exhausted",
                        category="retryable_transport",
                    ) from exc
                self.retry_count += 1
                self._sleeper(min(60.0, float(2**attempts)))
                continue
            self.request_seconds += self._monotonic() - started
            if response.status_code == 429 or response.status_code >= 500:
                if attempts >= self.config.max_attempts:
                    raise ScannerReplayError(
                        f"Alpaca scanner HTTP {response.status_code}",
                        category="retryable_provider",
                    )
                self.retry_count += 1
                self._sleeper(self._retry_delay(response, attempts))
                continue
            if response.status_code >= 400:
                category = (
                    "permission" if response.status_code in (401, 403) else "fidelity"
                )
                raise ScannerReplayError(
                    f"Alpaca scanner HTTP {response.status_code}", category=category
                )
            try:
                payload = response.json()
            except ValueError as exc:
                raise ScannerReplayError(
                    "Alpaca scanner returned invalid JSON"
                ) from exc
            if not isinstance(payload, Mapping):
                raise ScannerReplayError("Alpaca scanner response must be an object")
            return payload

    def fetch(
        self,
        symbols: Sequence[str],
        *,
        timeframe: str,
        start: datetime,
        end: datetime,
    ) -> tuple[dict[str, list[dict[str, Any]]], int]:
        requested = tuple(dict.fromkeys(str(item).strip().upper() for item in symbols))
        if not requested or any(not item for item in requested):
            raise ScannerReplayError(
                "Alpaca scanner symbol batch is empty or malformed"
            )
        params: dict[str, Any] = {
            "symbols": ",".join(requested),
            "timeframe": timeframe,
            "start": start.astimezone(UTC).isoformat().replace("+00:00", "Z"),
            "end": end.astimezone(UTC).isoformat().replace("+00:00", "Z"),
            "limit": 10000,
            "feed": "sip",
            "adjustment": "raw",
            "asof": "-",
            "sort": "asc",
        }
        output: dict[str, list[dict[str, Any]]] = {}
        pages = 0
        while True:
            pages += 1
            if pages > 100:
                raise ScannerReplayError("Alpaca scanner pagination exceeded 100 pages")
            payload = self._get(params)
            bars = payload.get("bars", {})
            if not isinstance(bars, Mapping):
                raise ScannerReplayError("Alpaca scanner bars must be keyed by symbol")
            for raw_symbol, raw_rows in bars.items():
                symbol = str(raw_symbol).strip().upper()
                if symbol not in requested or not isinstance(raw_rows, list):
                    raise ScannerReplayError(
                        "Alpaca scanner returned an unexpected symbol or row set"
                    )
                output.setdefault(symbol, []).extend(
                    dict(row) for row in raw_rows if isinstance(row, Mapping)
                )
            token = payload.get("next_page_token")
            if not token:
                break
            params["page_token"] = str(token)
        return output, pages


def _parse_provider_bar(
    row: Mapping[str, Any], *, day: str, window: str
) -> dict[str, Any]:
    try:
        provider_time = str(row["t"])
        observed = datetime.fromisoformat(provider_time.replace("Z", "+00:00"))
        if observed.tzinfo is None:
            observed = observed.replace(tzinfo=UTC)
        opened = float(row["o"])
        high = float(row["h"])
        low = float(row["l"])
        closed = float(row["c"])
        volume = int(float(row["v"]))
        transactions = int(float(row.get("n") or 0))
        wap = float(row.get("vw") or 0)
    except (KeyError, TypeError, ValueError) as exc:
        raise ScannerReplayError("Alpaca scanner bar is malformed") from exc
    if min(opened, high, low, closed) <= 0 or volume < 0 or transactions < 0:
        raise ScannerReplayError("Alpaca scanner bar has invalid numeric fields")
    if not low <= min(opened, closed) <= max(opened, closed) <= high:
        raise ScannerReplayError("Alpaca scanner OHLC ordering is invalid")
    normalized_time = observed.astimezone(EASTERN)
    if normalized_time.date().isoformat() != day:
        raise ScannerReplayError("Alpaca bar is outside the requested session")
    if window == "opening":
        if not wall_time(9, 30) <= normalized_time.time() < wall_time(9, 35):
            raise ScannerReplayError("Alpaca opening bar is outside the frozen window")
    elif window == "regular":
        if not wall_time(9, 30) <= normalized_time.time() < wall_time(16, 0):
            raise ScannerReplayError(
                "Alpaca regular-session bar is outside market hours"
            )
    else:
        raise ScannerReplayError(f"unsupported Alpaca bar window {window!r}")
    return {
        "time_et": normalized_time.isoformat(),
        "open": opened,
        "high": high,
        "low": low,
        "close": closed,
        "volume": volume,
        "count": transactions,
        "wap": wap,
        "interpolated": False,
        "provider_timestamp": provider_time,
    }


def _aggregate_regular(
    rows: Sequence[Mapping[str, Any]], *, day: str
) -> dict[str, Any] | None:
    if not rows:
        return None
    ordered = sorted(rows, key=lambda item: str(item["time_et"]))
    timestamps = [str(item["time_et"]) for item in ordered]
    if len(timestamps) != len(set(timestamps)) or len(ordered) > 26:
        raise ScannerReplayError(f"Alpaca regular-session rows are ambiguous on {day}")
    volume = sum(int(item["volume"]) for item in ordered)
    weighted = sum(
        float(item.get("wap") or 0) * int(item["volume"]) for item in ordered
    )
    return {
        "time_et": datetime.combine(
            date.fromisoformat(day), wall_time(0), tzinfo=EASTERN
        ).isoformat(),
        "open": float(ordered[0]["open"]),
        "high": max(float(item["high"]) for item in ordered),
        "low": min(float(item["low"]) for item in ordered),
        "close": float(ordered[-1]["close"]),
        "volume": volume,
        "count": sum(int(item["count"]) for item in ordered),
        "wap": weighted / volume if volume else 0.0,
        "interpolated": False,
        "derived_from": "Alpaca SIP raw 15Min bars within 09:30-16:00 ET",
    }


def _opening_is_exact(rows: Sequence[Mapping[str, Any]], day: str) -> bool:
    expected = [
        datetime.combine(date.fromisoformat(day), wall_time(9, 30), tzinfo=EASTERN)
        + timedelta(minutes=index)
        for index in range(5)
    ]
    observed = [datetime.fromisoformat(str(row["time_et"])) for row in rows]
    return observed == expected


def _canonical_datasets(
    *,
    day: str,
    regular_rows: Sequence[Mapping[str, Any]],
    opening_rows: Sequence[Mapping[str, Any]],
    captured_at: str,
) -> list[dict[str, Any]]:
    request_basis = {
        "source_type": "alpaca_multi_symbol_scanner_collection",
        "endpoint": ALPACA_BARS_URL,
        "session_date": day,
        "feed": "sip",
        "adjustment": "raw",
        "asof": "-",
        "captured_at": captured_at,
    }
    datasets: list[dict[str, Any]] = []
    regular_aggregate = _aggregate_regular(regular_rows, day=day)
    if regular_rows:
        datasets.append(
            build_dataset(
                kind="bars",
                provider="alpaca",
                rows=[compact_bar(row, day=day) for row in regular_rows],
                channel="trades",
                timeframe="15m",
                feed="sip",
                adjustment="raw",
                session="regular",
                scope="full_session",
                quality={"complete": True, "sparse_intervals_allowed": True},
                provenance={**request_basis, "timeframe": "15Min"},
            )
        )
        datasets.append(
            build_dataset(
                kind="derived",
                provider="alpaca",
                rows=[compact_bar(regular_aggregate, day=day)],
                channel="minute_aggregate_regular",
                timeframe="1d",
                feed="sip",
                adjustment="raw",
                session="regular",
                scope="full_session",
                quality={"complete": True},
                provenance={
                    **request_basis,
                    "timeframe": "15Min",
                    "derivation": "OHLCV aggregation of returned regular-session bars",
                },
            )
        )
    if opening_rows:
        exact = _opening_is_exact(opening_rows, day)
        datasets.append(
            build_dataset(
                kind="bars",
                provider="alpaca",
                rows=[compact_bar(row, day=day) for row in opening_rows],
                channel="trades",
                timeframe="1m",
                feed="sip",
                adjustment="raw",
                session="regular",
                scope="opening_window_09_30_09_35",
                quality={"complete": False, "opening_window_complete": exact},
                limitations=["partial_session_opening_window_only"],
                provenance={**request_basis, "timeframe": "1Min"},
            )
        )
    return datasets


def _derived_rows(
    symbol: str,
    *,
    day: str,
    daily_row: Mapping[str, Any],
    opening_rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    if opening_rows:
        if float(daily_row["high"]) < max(float(item["high"]) for item in opening_rows):
            raise ScannerReplayError(
                f"Alpaca daily high does not contain its opening bars for {symbol} on {day}"
            )
        if float(daily_row["low"]) > min(float(item["low"]) for item in opening_rows):
            raise ScannerReplayError(
                f"Alpaca daily low does not contain its opening bars for {symbol} on {day}"
            )
        if _opening_is_exact(opening_rows, day) and float(daily_row["open"]) != float(
            opening_rows[0]["open"]
        ):
            raise ScannerReplayError(
                f"Alpaca daily open disagrees with 09:30 for {symbol} on {day}"
            )
    rows: list[dict[str, Any]] = []
    opening_volume = 0
    opening_transactions = 0
    for item in opening_rows:
        observed = datetime.fromisoformat(str(item["time_et"])).astimezone(EASTERN)
        opening_volume += int(item["volume"])
        opening_transactions += int(item["count"])
        rows.append(
            {
                "ticker": symbol,
                "volume": int(item["volume"]),
                "open": float(item["open"]),
                "close": float(item["close"]),
                "high": float(item["high"]),
                "low": float(item["low"]),
                "window_start": int(observed.timestamp() * 1_000_000_000),
                "transactions": int(item["count"]),
            }
        )
    residual_volume = int(daily_row["volume"]) - opening_volume
    residual_transactions = int(daily_row["count"]) - opening_transactions
    if residual_volume < 0 or residual_transactions < 0:
        raise ScannerReplayError(
            f"Alpaca daily aggregate is smaller than its opening bars for {symbol} on {day}"
        )
    close_stamp = datetime.combine(
        date.fromisoformat(day), wall_time(15, 59), tzinfo=EASTERN
    )
    rows.append(
        {
            "ticker": symbol,
            "volume": residual_volume,
            "open": float(daily_row["open"]),
            "close": float(daily_row["close"]),
            "high": float(daily_row["high"]),
            "low": float(daily_row["low"]),
            "window_start": int(close_stamp.timestamp() * 1_000_000_000),
            "transactions": residual_transactions,
        }
    )
    return rows


def _target_symbols(
    records: Sequence[Mapping[str, Any]], target_dates: Sequence[str]
) -> list[str]:
    symbols: set[str] = set()
    for day in target_dates:
        observed = date.fromisoformat(day)
        current = {
            str(record["symbol"])
            for record in records
            if record.get("security_type") == "COMMON"
            and security_record_covers(record, observed)
        }
        if not current:
            raise ScannerReplayError(
                f"security master has no common-stock universe for {day}"
            )
        symbols.update(current)
    return sorted(symbols)


def _batches(values: Sequence[str], size: int) -> list[Sequence[str]]:
    return [values[index : index + size] for index in range(0, len(values), size)]


def collect_day(
    day: str,
    symbols: Sequence[str],
    *,
    client: AlpacaBulkBarsClient,
    store: HistoricalDayStore,
    index_root: Path,
    dataset_id: str = DATASET_ID,
    inherited_source: Path | None = None,
    inherited_attestation: Mapping[str, Any] | None = None,
    expected_inherited_dataset_id: str | None = None,
    requested_symbol_total: int | None = None,
    target_symbol_total: int | None = None,
    source_symbol_union_total: int | None = None,
) -> dict[str, Any]:
    output = index_root / "minute_aggs" / day[:4] / f"{day}.csv.gz"
    sidecar = index_root / "attestations" / day[:4] / f"{day}.json"
    if output.exists() and sidecar.exists():
        cached = _read_object(sidecar)
        _validate_index_attestation(
            cached, day=day, dataset_id=dataset_id, source_path=output
        )
        return {**cached, "disposition": "cached"}
    if output.exists() != sidecar.exists():
        raise ScannerReplayError(f"scanner index is partial for {day}")

    inherited_rows: list[dict[str, Any]] = []
    if inherited_source is not None or inherited_attestation is not None:
        if inherited_source is None or inherited_attestation is None:
            raise ScannerReplayError("reusable scanner source is incomplete")
        inherited_dataset_id = expected_inherited_dataset_id or str(
            inherited_attestation.get("dataset_id") or ""
        )
        _validate_index_attestation(
            inherited_attestation,
            day=day,
            dataset_id=inherited_dataset_id,
            source_path=inherited_source,
        )
        inherited_rows = _read_csv_rows(inherited_source)
        if inherited_attestation.get("derived_rows") != len(inherited_rows):
            raise ScannerReplayError("reusable scanner source row count changed")
        inherited_symbols = {str(row.get("ticker") or "") for row in inherited_rows}
        if inherited_symbols.intersection(symbols):
            raise ScannerReplayError("delta collection overlaps inherited symbols")

    session_day = date.fromisoformat(day)
    regular_start = datetime.combine(session_day, wall_time(9, 30), tzinfo=EASTERN)
    regular_end = datetime.combine(
        session_day, wall_time(15, 59, 59, 999999), tzinfo=EASTERN
    )
    opening_start = datetime.combine(session_day, wall_time(9, 30), tzinfo=EASTERN)
    opening_end = datetime.combine(
        session_day, wall_time(9, 34, 59, 999999), tzinfo=EASTERN
    )
    request_before = client.request_count
    retry_before = client.retry_count
    seconds_before = client.request_seconds
    regular: dict[str, list[dict[str, Any]]] = {}
    opening: dict[str, list[dict[str, Any]]] = {}
    page_count = 0
    batches = _batches(symbols, client.config.batch_size)
    for batch in batches:
        values, pages = client.fetch(
            batch, timeframe="15Min", start=regular_start, end=regular_end
        )
        page_count += pages
        for symbol, rows in values.items():
            normalized = sorted(
                (_parse_provider_bar(row, day=day, window="regular") for row in rows),
                key=lambda item: str(item["time_et"]),
            )
            timestamps = [str(item["time_et"]) for item in normalized]
            if len(timestamps) != len(set(timestamps)) or len(normalized) > 26:
                raise ScannerReplayError(
                    f"Alpaca regular-session rows are ambiguous for {symbol} on {day}"
                )
            regular[symbol] = normalized
        values, pages = client.fetch(
            batch, timeframe="1Min", start=opening_start, end=opening_end
        )
        page_count += pages
        for symbol, rows in values.items():
            normalized = sorted(
                (_parse_provider_bar(row, day=day, window="opening") for row in rows),
                key=lambda item: str(item["time_et"]),
            )
            timestamps = [str(item["time_et"]) for item in normalized]
            if len(timestamps) != len(set(timestamps)) or len(normalized) > 5:
                raise ScannerReplayError(
                    f"Alpaca opening rows are ambiguous for {symbol} on {day}"
                )
            opening[symbol] = normalized

    captured_at = datetime.now(UTC).isoformat()
    changed_files = 0
    derived: list[dict[str, Any]] = []
    opening_complete = 0
    for symbol in symbols:
        regular_rows = regular.get(symbol, [])
        daily_row = _aggregate_regular(regular_rows, day=day)
        opening_rows = opening.get(symbol, [])
        datasets = _canonical_datasets(
            day=day,
            regular_rows=regular_rows,
            opening_rows=opening_rows,
            captured_at=captured_at,
        )
        if datasets:
            result = store.merge(symbol, day, datasets=datasets)
            changed_files += int(result["changed"])
        if daily_row is None:
            continue
        opening_complete += int(_opening_is_exact(opening_rows, day))
        derived.extend(
            _derived_rows(
                symbol,
                day=day,
                daily_row=daily_row,
                opening_rows=opening_rows,
            )
        )
    derived = [*inherited_rows, *derived]
    derived.sort(key=lambda item: (int(item["window_start"]), str(item["ticker"])))
    _gzip_csv(output, derived)
    attestation = {
        "schema_version": 1,
        "dataset_id": dataset_id,
        "date": day,
        "status": "READY",
        "source": {
            "provider": "Alpaca",
            "endpoint": ALPACA_BARS_URL,
            "feed": "sip",
            "adjustment": "raw",
            "asof": "-",
        },
        "requested_symbols": requested_symbol_total or len(symbols),
        "target_symbol_union_count": target_symbol_total or len(symbols),
        "source_symbol_union_count": source_symbol_union_total
        or target_symbol_total
        or len(symbols),
        "daily_symbols": int((inherited_attestation or {}).get("daily_symbols", 0))
        + len(regular),
        "regular_session_symbols": int(
            (inherited_attestation or {}).get("regular_session_symbols", 0)
        )
        + len(regular),
        "regular_session_rows": int(
            (inherited_attestation or {}).get("regular_session_rows", 0)
        )
        + sum(len(rows) for rows in regular.values()),
        "opening_symbols": int((inherited_attestation or {}).get("opening_symbols", 0))
        + len(opening),
        "complete_opening_symbols": int(
            (inherited_attestation or {}).get("complete_opening_symbols", 0)
        )
        + opening_complete,
        "derived_rows": len(derived),
        "canonical_files_changed": changed_files,
        "request_batches": len(batches),
        "request_pages": page_count,
        "provider_requests": client.request_count - request_before,
        "provider_retries": client.retry_count - retry_before,
        "provider_request_seconds": round(client.request_seconds - seconds_before, 6),
        "source_sha256": _sha256_file(output),
        "captured_at": captured_at,
        **(
            {
                "reused_source": {
                    "dataset_id": inherited_attestation.get("dataset_id"),
                    "source_sha256": inherited_attestation.get("source_sha256"),
                    "inherited_derived_rows": len(inherited_rows),
                    "delta_symbols_requested": len(symbols),
                }
            }
            if inherited_attestation is not None
            else {}
        ),
    }
    _write_json(sidecar, attestation)
    return {**attestation, "disposition": "collected"}


def freeze_contract(
    *,
    dataset_id: str = DATASET_ID,
    selection_path: Path,
    calendar_path: Path,
    rules_path: Path,
    security_path: Path,
    security_source_path: Path = DEFAULT_SECURITY_SOURCE,
    split_path: Path = DEFAULT_SPLITS,
    split_source_path: Path = DEFAULT_SPLIT_SOURCE,
    strategy_source_path: Path = DEFAULT_STRATEGY_SOURCE,
    output_root: Path,
    index_root: Path,
    reuse_manifest_path: Path | None = None,
) -> tuple[Path, dict[str, Any]]:
    if not dataset_id.startswith("dataset-production-scanner-replay-"):
        raise ScannerReplayError("scanner dataset_id has an invalid namespace")
    selection = load_selection(selection_path)
    calendar = load_calendar(calendar_path)
    requested = sorted(selection["selected_dates"])
    required = required_sessions(requested, calendar)
    existing = list((index_root / "minute_aggs").glob("*/*.csv.gz"))
    if existing:
        raise ScannerReplayError(
            "Alpaca scanner contract must be frozen before collection"
        )
    rules = validate_rules(rules_path)
    snapshot_path, snapshot_hash = write_security_master_snapshot(security_path)
    security_source = _read_object(security_source_path)
    if security_source.get("security_master", {}).get("sha256") != snapshot_hash:
        raise ScannerReplayError("security-master source attestation does not match")
    split_source = _read_object(split_source_path)
    split_artifact = split_source.get("artifact")
    split_provider = split_source.get("source")
    split_range = (
        split_provider.get("query_range")
        if isinstance(split_provider, Mapping)
        else None
    )
    split_hash = _sha256_file(split_path)
    if (
        split_source.get("schema_version") != 1
        or not isinstance(split_artifact, Mapping)
        or not isinstance(split_provider, Mapping)
        or not isinstance(split_range, Mapping)
        or split_provider.get("provider") != "Massive"
        or split_provider.get("endpoint")
        != "https://api.massive.com/stocks/v1/splits"
        or split_artifact.get("sha256") != split_hash
        or split_artifact.get("events") != _split_event_count(split_path)
        or split_artifact.get("local_ignored_path") != _repo_path(split_path)
        or str(split_range.get("execution_date_gte") or "") > required[0]
        or str(split_range.get("execution_date_lte") or "") < max(requested)
    ):
        raise ScannerReplayError("split-actions source attestation does not cover contract")
    strategy_source = _read_object(strategy_source_path)
    strategy_artifact = strategy_source.get("artifact")
    strategy_config_path = PROJECT_ROOT / "strategy_config.toml"
    if (
        strategy_source.get("schema_version") != 1
        or not isinstance(strategy_artifact, Mapping)
        or strategy_artifact.get("path") != "strategy_config.toml"
        or strategy_artifact.get("file_sha256")
        != _sha256_file(strategy_config_path)
        or strategy_artifact.get("strategy_version")
        != rules.get("strategy_version")
    ):
        raise ScannerReplayError("production-strategy source attestation differs")
    records = load_security_master(security_path)
    symbols = _target_symbols(records, requested)
    reusable_source: dict[str, Any] | None = None
    if reuse_manifest_path is not None:
        reuse_manifest = load_contract(reuse_manifest_path)
        reuse_contract = reuse_manifest["collection_contract"]
        compatible_fields = (
            "endpoint",
            "feed",
            "adjustment",
            "symbol_mapping",
            "regular_session_query",
            "opening_query",
            "derived_index_contract",
        )
        expected_contract = {
            "endpoint": ALPACA_BARS_URL,
            "feed": "sip",
            "adjustment": "raw",
            "symbol_mapping": "asof=-; symbol discontinuities remain explicit missing history",
            "regular_session_query": "15Min from 09:30:00 through 15:59:59.999999 ET",
            "opening_query": "1Min from 09:30:00 through 09:34:59.999999 ET",
            "derived_index_contract": (
                "five real opening rows plus one deterministic 15:59 residual row whose "
                "OHLCV reconstructs the aggregate of provider 15-minute regular-session "
                "bars; the residual is an index, not a provider minute observation"
            ),
        }
        if any(
            reuse_contract.get(field) != expected_contract[field]
            for field in compatible_fields
        ):
            raise ScannerReplayError("reusable scanner source contract is incompatible")
        reuse_sessions = sorted(
            set(required).intersection(
                reuse_manifest["collection_contract"]["required_session_dates"]
            )
        )
        reusable_source = {
            "dataset_id": reuse_manifest["dataset_id"],
            "manifest_path": _repo_path(reuse_manifest_path),
            "manifest_sha256": reuse_manifest["manifest_sha256"],
            "session_dates": reuse_sessions,
            "session_count": len(reuse_sessions),
            "source_rows_existed_before_freeze": True,
            "target_outcomes_observed_or_derived": False,
            "reuse_requires_delta_symbol_collection": True,
        }
    payload = {
        "schema_version": 1,
        "dataset_id": dataset_id,
        "registered_at": _timestamp_now(),
        "requested_dates": requested,
        "dataset_payload": {
            "lane": "production_scanner_replay",
            "claim_scope": "PRODUCTION_POLICY_REPLAY",
            "status": "COLLECTING",
            "evidence_paths": [
                _repo_path(selection_path),
                _repo_path(rules_path),
                _repo_path(security_source_path),
            ],
            "inspected": False,
            "universe_contract": {
                "selection_time_et": "09:35:00",
                "information_cutoff": "TARGET_SESSION_09:35_ET",
                "selection_is_dynamic": True,
                "complete_universe": True,
                "scanner_rules_sha256": _sha256_json(rules),
                "security_master_sha256": snapshot_hash,
                "security_master_path": _repo_path(snapshot_path),
                "security_master_attestation_path": _repo_path(security_source_path),
                "security_master_attestation_sha256": _sha256_file(
                    security_source_path
                ),
                "split_actions_sha256": split_hash,
                "split_actions_path": _repo_path(split_path),
                "split_actions_attestation_path": _repo_path(split_source_path),
                "split_actions_attestation_sha256": _sha256_file(
                    split_source_path
                ),
                "production_strategy_sha256": _sha256_file(strategy_config_path),
                "production_strategy_path": "strategy_config.toml",
                "production_strategy_attestation_path": _repo_path(
                    strategy_source_path
                ),
                "production_strategy_attestation_sha256": _sha256_file(
                    strategy_source_path
                ),
            },
        },
        "collection_contract": {
            "source": "Alpaca historical SIP raw regular-session 15-minute and opening-minute bars",
            "endpoint": ALPACA_BARS_URL,
            "feed": "sip",
            "adjustment": "raw",
            "symbol_mapping": "asof=-; symbol discontinuities remain explicit missing history",
            "regular_session_query": "15Min from 09:30:00 through 15:59:59.999999 ET",
            "opening_query": "1Min from 09:30:00 through 09:34:59.999999 ET",
            "canonical_store_required": True,
            "derived_index_contract": (
                "five real opening rows plus one deterministic 15:59 residual row whose "
                "OHLCV reconstructs the aggregate of provider 15-minute regular-session "
                "bars; the residual is an index, not a provider minute observation"
            ),
            "corporate_action_source": "Massive /stocks/v1/splits",
            "split_adjustment_basis": "target-date share basis using only execution dates at or before each target",
            "required_session_dates": required,
            "required_session_count": len(required),
            "session_calendar_path": _repo_path(calendar_path),
            "session_calendar_sha256": _sha256_file(calendar_path),
            "target_symbol_union_count": len(symbols),
            "scanner_engine_sha256": _sha256_file(PROJECT_ROOT / "scanner_replay.py"),
            "adapter_sha256": _sha256_file(Path(__file__)),
            "target_data_collection_must_begin_after_freeze": True,
            "pre_freeze_target_artifact_count": 0,
            "pre_freeze_transport_probe": {
                "counts_only": True,
                "price_rows_retained_or_inspected": False,
                "purpose": "verify multi-symbol SIP entitlement, request scale, and five-minute boundary semantics",
            },
            "pre_freeze_related_contracts": {
                "dataset_ids": [
                    "dataset-production-scanner-replay-2026-07-19-v2",
                    "dataset-production-scanner-replay-2026-07-19-v3",
                ],
                "price_rows_inspected_for_source_fidelity_only": True,
                "dates_symbols_thresholds_or_ranking_changed": False,
                "artifacts_reused_by_this_contract": reusable_source is not None,
            },
            **(
                {"reusable_source": reusable_source}
                if reusable_source is not None
                else {}
            ),
            "substitutions_allowed": False,
            "provider_switching_allowed": False,
        },
        "selection": {
            "seed": selection["seed"],
            "path": _repo_path(selection_path),
            "file_sha256": _sha256_file(selection_path),
            "substitution_allowed": selection.get("substitution_allowed") is True,
            "selection_sha256": _sha256_json(
                {
                    "seed": selection["seed"],
                    "selected_dates": selection["selected_dates"],
                }
            ),
        },
    }
    fingerprint = _sha256_json(payload)
    frozen = {**payload, "manifest_sha256": fingerprint}
    output = output_root / f"{dataset_id}-{fingerprint}.json"
    if output.exists() and _read_object(output) != frozen:
        raise ScannerReplayError("hash-addressed Alpaca scanner contract has changed")
    if not output.exists():
        _write_json(output, frozen)
    return output, frozen


def load_contract(path: Path) -> dict[str, Any]:
    manifest = _read_object(path)
    recorded = manifest.get("manifest_sha256")
    content = dict(manifest)
    content.pop("manifest_sha256", None)
    expected = _sha256_json(content)
    dataset_id = str(manifest.get("dataset_id") or "")
    if (
        not dataset_id.startswith("dataset-production-scanner-replay-")
        or recorded != expected
        or path.name != f"{dataset_id}-{expected}.json"
    ):
        raise ScannerReplayError(
            "frozen Alpaca scanner contract was mutated or renamed"
        )
    validate_dataset_payload(dataset_id, manifest.get("dataset_payload", {}))
    collection = manifest.get("collection_contract")
    if not isinstance(collection, Mapping):
        raise ScannerReplayError("Alpaca scanner contract lacks collection_contract")
    if collection.get("substitutions_allowed") is not False:
        raise ScannerReplayError("Alpaca scanner contract must forbid substitutions")
    return manifest


def verify_contract_inputs(
    manifest: Mapping[str, Any], *, rules_path: Path
) -> tuple[dict[str, Any], Path]:
    rules = validate_rules(rules_path)
    universe = manifest["dataset_payload"]["universe_contract"]
    if universe["scanner_rules_sha256"] != _sha256_json(rules):
        raise ScannerReplayError("scanner rules no longer match the Alpaca contract")
    security_path = PROJECT_ROOT / str(universe["security_master_path"])
    if universe["security_master_sha256"] != security_master_sha256(security_path):
        raise ScannerReplayError(
            "security-master snapshot no longer matches the Alpaca contract"
        )
    source_path = PROJECT_ROOT / str(universe["security_master_attestation_path"])
    expected_source_hash = universe.get("security_master_attestation_sha256")
    if expected_source_hash is not None and expected_source_hash != _sha256_file(
        source_path
    ):
        raise ScannerReplayError(
            "security-master source attestation no longer matches the contract"
        )
    split_path = PROJECT_ROOT / str(universe["split_actions_path"])
    split_source_path = PROJECT_ROOT / str(
        universe["split_actions_attestation_path"]
    )
    if (
        universe["split_actions_sha256"] != _sha256_file(split_path)
        or universe["split_actions_attestation_sha256"]
        != _sha256_file(split_source_path)
    ):
        raise ScannerReplayError("split actions no longer match the contract")
    strategy_path = PROJECT_ROOT / str(universe["production_strategy_path"])
    strategy_source_path = PROJECT_ROOT / str(
        universe["production_strategy_attestation_path"]
    )
    if (
        universe["production_strategy_sha256"] != _sha256_file(strategy_path)
        or universe["production_strategy_attestation_sha256"]
        != _sha256_file(strategy_source_path)
    ):
        raise ScannerReplayError("production strategy no longer matches the contract")
    collection = manifest["collection_contract"]
    selection_contract = manifest.get("selection")
    if isinstance(selection_contract, Mapping) and selection_contract.get("path"):
        selection_path = PROJECT_ROOT / str(selection_contract["path"])
        selection = load_selection(selection_path)
        if (
            selection_contract.get("file_sha256") != _sha256_file(selection_path)
            or selection_contract.get("seed") != selection["seed"]
            or list(manifest["requested_dates"])
            != sorted(selection["selected_dates"])
            or selection_contract.get("substitution_allowed") is not False
        ):
            raise ScannerReplayError("date selection no longer matches the contract")
    if collection.get("scanner_engine_sha256") != _sha256_file(
        PROJECT_ROOT / "scanner_replay.py"
    ):
        raise ScannerReplayError(
            "scanner calculation engine no longer matches the contract"
        )
    if collection.get("adapter_sha256") != _sha256_file(Path(__file__)):
        raise ScannerReplayError(
            "Alpaca scanner adapter no longer matches the contract"
        )
    return rules, security_path


def verify_calendar_contract(
    manifest: Mapping[str, Any], calendar_path: Path
) -> list[str]:
    calendar = load_calendar(calendar_path)
    collection = manifest["collection_contract"]
    recorded_path = collection.get("session_calendar_path")
    recorded_hash = collection.get("session_calendar_sha256")
    if (recorded_path is not None or recorded_hash is not None) and (
        recorded_path != _repo_path(calendar_path)
        or recorded_hash != _sha256_file(calendar_path)
    ):
        raise ScannerReplayError(
            "session calendar no longer matches the Alpaca contract"
        )
    if required_sessions(manifest["requested_dates"], calendar) != list(
        collection["required_session_dates"]
    ):
        raise ScannerReplayError(
            "session calendar no longer matches the Alpaca contract"
        )
    return calendar


def index_root(store: HistoricalDayStore, dataset_id: str = DATASET_ID) -> Path:
    return store.root / "_derived" / "scanner_replay" / dataset_id


def collection_status(
    manifest: Mapping[str, Any], *, store: HistoricalDayStore
) -> dict[str, Any]:
    required = list(manifest["collection_contract"]["required_session_dates"])
    dataset_id = str(manifest["dataset_id"])
    root = index_root(store, dataset_id)
    ready: list[dict[str, Any]] = []
    invalid: list[str] = []
    for day in required:
        source = root / "minute_aggs" / day[:4] / f"{day}.csv.gz"
        sidecar = root / "attestations" / day[:4] / f"{day}.json"
        if not source.exists() and not sidecar.exists():
            continue
        if not source.exists() or not sidecar.exists():
            invalid.append(day)
            continue
        item = _read_object(sidecar)
        try:
            _validate_index_attestation(
                item, day=day, dataset_id=dataset_id, source_path=source
            )
        except ScannerReplayError:
            invalid.append(day)
            continue
        ready.append(item)
    return {
        "valid": not invalid,
        "dataset_id": dataset_id,
        "provider": "Alpaca historical SIP",
        "canonical_store": {"outside_repository": True},
        "session_files": {
            "ready": len(ready),
            "required": len(required),
            "invalid": invalid,
        },
        "canonical_day_merges": sum(
            int(item.get("canonical_files_changed", 0)) for item in ready
        ),
        "provider_requests": sum(
            int(item.get("provider_requests", 0)) for item in ready
        ),
        "provider_retries": sum(int(item.get("provider_retries", 0)) for item in ready),
        "derived_rows": sum(int(item.get("derived_rows", 0)) for item in ready),
        "reused_sessions": sum(
            int(isinstance(item.get("reused_source"), Mapping)) for item in ready
        ),
        "inherited_derived_rows": sum(
            int((item.get("reused_source") or {}).get("inherited_derived_rows", 0))
            for item in ready
        ),
        "delta_symbols_requested": sum(
            int((item.get("reused_source") or {}).get("delta_symbols_requested", 0))
            for item in ready
        ),
        "complete": len(ready) == len(required) and not invalid,
    }


def collect_contract(
    manifest: Mapping[str, Any],
    *,
    config: AlpacaBulkConfig,
    store: HistoricalDayStore,
    max_days: int | None = None,
) -> dict[str, Any]:
    _, security_path = verify_contract_inputs(manifest, rules_path=DEFAULT_RULES)
    records = load_security_master(security_path)
    symbols = _target_symbols(records, manifest["requested_dates"])
    required = list(manifest["collection_contract"]["required_session_dates"])
    dataset_id = str(manifest["dataset_id"])
    root = index_root(store, dataset_id)
    reusable = manifest["collection_contract"].get("reusable_source")
    reusable_days: set[str] = set()
    reusable_root: Path | None = None
    inherited_symbols: set[str] = set()
    if isinstance(reusable, Mapping):
        source_manifest_path = PROJECT_ROOT / str(reusable["manifest_path"])
        source_manifest = load_contract(source_manifest_path)
        if source_manifest.get("manifest_sha256") != reusable.get(
            "manifest_sha256"
        ) or source_manifest.get("dataset_id") != reusable.get("dataset_id"):
            raise ScannerReplayError("reusable scanner contract differs from freeze")
        source_security = PROJECT_ROOT / str(
            source_manifest["dataset_payload"]["universe_contract"][
                "security_master_path"
            ]
        )
        source_universe = source_manifest["dataset_payload"]["universe_contract"]
        if source_universe["security_master_sha256"] != security_master_sha256(
            source_security
        ):
            raise ScannerReplayError("reusable security-master snapshot changed")
        inherited_symbols = set(
            _target_symbols(
                load_security_master(source_security),
                source_manifest["requested_dates"],
            )
        )
        reusable_days = set(str(day) for day in reusable["session_dates"])
        reusable_root = index_root(store, str(reusable["dataset_id"]))
    completed = 0
    with AlpacaBulkBarsClient(config) as client:
        for index, day in enumerate(required, 1):
            existing = root / "minute_aggs" / day[:4] / f"{day}.csv.gz"
            if max_days is not None and completed >= max_days and not existing.exists():
                break
            inherited_source = None
            inherited_attestation = None
            requested_symbols: Sequence[str] = symbols
            if day in reusable_days:
                if reusable_root is None:
                    raise ScannerReplayError("reusable scanner root is missing")
                inherited_source = (
                    reusable_root / "minute_aggs" / day[:4] / f"{day}.csv.gz"
                )
                sidecar = reusable_root / "attestations" / day[:4] / f"{day}.json"
                if not inherited_source.exists() or not sidecar.exists():
                    raise ScannerReplayError(
                        f"frozen reusable scanner session is missing for {day}"
                    )
                inherited_attestation = _read_object(sidecar)
                requested_symbols = sorted(set(symbols) - inherited_symbols)
            result = collect_day(
                day,
                requested_symbols,
                client=client,
                store=store,
                index_root=root,
                dataset_id=dataset_id,
                inherited_source=inherited_source,
                inherited_attestation=inherited_attestation,
                expected_inherited_dataset_id=(
                    str(reusable["dataset_id"])
                    if inherited_attestation is not None
                    and isinstance(reusable, Mapping)
                    else None
                ),
                requested_symbol_total=len(symbols),
                target_symbol_total=len(symbols),
                source_symbol_union_total=len(
                    set(symbols).union(inherited_symbols)
                ),
            )
            completed += int(result["disposition"] == "collected")
            print(
                f"scanner input {index}/{len(required)} {day}: "
                f"{result['daily_symbols']} daily, {result['complete_opening_symbols']} exact opens "
                f"({result['disposition']})",
                flush=True,
            )
    return collection_status(manifest, store=store)


def build_contract(
    manifest: Mapping[str, Any],
    *,
    calendar_path: Path,
    rules_path: Path,
    splits_path: Path,
    store: HistoricalDayStore,
    run_root: Path,
    summary_output: Path,
) -> dict[str, Any]:
    rules, security_path = verify_contract_inputs(manifest, rules_path=rules_path)
    status = collection_status(manifest, store=store)
    if not status["complete"]:
        raise ScannerReplayError(
            f"Alpaca scanner collection is incomplete: {status['session_files']['ready']}/"
            f"{status['session_files']['required']}",
            category="incomplete_collection",
        )
    calendar = verify_calendar_contract(manifest, calendar_path)
    root = index_root(store, str(manifest["dataset_id"]))
    flat_root = run_root / "minute_aggs"
    flat_root.mkdir(parents=True, exist_ok=True)
    for day in manifest["collection_contract"]["required_session_dates"]:
        source = root / "minute_aggs" / day[:4] / f"{day}.csv.gz"
        target = flat_root / f"{day}.csv.gz"
        if target.exists() or target.is_symlink():
            target.unlink()
        target.symlink_to(source)
    source_hashes = [
        _read_object(root / "attestations" / day[:4] / f"{day}.json")["source_sha256"]
        for day in manifest["collection_contract"]["required_session_dates"]
    ]
    summary = build_scanner_replay(
        selected_dates=manifest["requested_dates"],
        calendar=calendar,
        rules=rules,
        security_path=security_path,
        minute_root=flat_root,
        split_path=splits_path,
        detailed_output=run_root / "scanner-replay-detail.json",
        summary_output=summary_output,
        dataset_id=str(manifest["dataset_id"]),
        summary_extension={
            "source": {
                "provider": "Alpaca",
                "feed": "sip",
                "adjustment": "raw",
                "contract_sha256": manifest["manifest_sha256"],
                "session_artifact_count": len(source_hashes),
                "session_artifacts_sha256": _sha256_json(source_hashes),
                "canonical_per_symbol_day_store": True,
                "derived_index_public": False,
            },
        },
    )
    return summary


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env", type=Path, default=PROJECT_ROOT / ".env")
    parser.add_argument("--run-root", type=Path, default=DEFAULT_RUN_ROOT)
    subparsers = parser.add_subparsers(dest="command", required=True)
    freeze = subparsers.add_parser("freeze")
    freeze.add_argument("--dataset-id", default=DATASET_ID)
    freeze.add_argument("--selection", type=Path, default=DEFAULT_SELECTION)
    freeze.add_argument("--calendar", type=Path, default=DEFAULT_CALENDAR)
    freeze.add_argument("--rules", type=Path, default=DEFAULT_RULES)
    freeze.add_argument("--security-master", type=Path, default=DEFAULT_SECURITY_MASTER)
    freeze.add_argument("--security-source", type=Path, default=DEFAULT_SECURITY_SOURCE)
    freeze.add_argument("--splits", type=Path, default=DEFAULT_SPLITS)
    freeze.add_argument("--split-source", type=Path, default=DEFAULT_SPLIT_SOURCE)
    freeze.add_argument(
        "--strategy-source", type=Path, default=DEFAULT_STRATEGY_SOURCE
    )
    freeze.add_argument("--output-root", type=Path, default=DEFAULT_MANIFEST_ROOT)
    freeze.add_argument("--reuse-manifest", type=Path)
    splits = subparsers.add_parser("collect-splits")
    splits.add_argument("--start", required=True)
    splits.add_argument("--end", required=True)
    splits.add_argument("--output", type=Path)
    collect = subparsers.add_parser("collect")
    collect.add_argument("manifest", type=Path)
    collect.add_argument("--max-days", type=int)
    status = subparsers.add_parser("status")
    status.add_argument("manifest", type=Path)
    build = subparsers.add_parser("build")
    build.add_argument("manifest", type=Path)
    build.add_argument("--calendar", type=Path, default=DEFAULT_CALENDAR)
    build.add_argument("--rules", type=Path, default=DEFAULT_RULES)
    build.add_argument("--splits", type=Path)
    build.add_argument("--summary", type=Path, default=DEFAULT_SUMMARY)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        config = HistoricalStoreConfig.from_env(args.env)
        store = HistoricalDayStore(config.root)
        if args.command == "freeze":
            path, manifest = freeze_contract(
                dataset_id=args.dataset_id,
                selection_path=args.selection,
                calendar_path=args.calendar,
                rules_path=args.rules,
                security_path=args.security_master,
                security_source_path=args.security_source,
                split_path=args.splits,
                split_source_path=args.split_source,
                strategy_source_path=args.strategy_source,
                output_root=args.output_root,
                index_root=index_root(store, args.dataset_id),
                reuse_manifest_path=args.reuse_manifest,
            )
            result: Any = {
                "path": str(path),
                "manifest_sha256": manifest["manifest_sha256"],
            }
        elif args.command == "collect-splits":
            result = collect_split_actions(
                start=args.start,
                end=args.end,
                config=MassiveReferenceConfig.from_env(args.env),
                output=args.output or args.run_root / "splits.json.gz",
            )
        else:
            manifest = load_contract(args.manifest)
            if args.command == "collect":
                if args.max_days is not None and args.max_days < 1:
                    raise ScannerReplayError("--max-days must be positive")
                result = collect_contract(
                    manifest,
                    config=AlpacaBulkConfig.from_env(args.env),
                    store=store,
                    max_days=args.max_days,
                )
            elif args.command == "status":
                verify_contract_inputs(manifest, rules_path=DEFAULT_RULES)
                result = collection_status(manifest, store=store)
            else:
                split_path = args.splits or PROJECT_ROOT / str(
                    manifest["dataset_payload"]["universe_contract"][
                        "split_actions_path"
                    ]
                )
                result = build_contract(
                    manifest,
                    calendar_path=args.calendar,
                    rules_path=args.rules,
                    splits_path=split_path,
                    store=store,
                    run_root=args.run_root,
                    summary_output=args.summary,
                )
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    except (
        KeyError,
        OSError,
        ScannerReplayError,
        HistoricalStoreError,
        LearningDataError,
        ValueError,
    ) as exc:
        print(
            json.dumps(
                {
                    "error": str(exc),
                    "error_type": type(exc).__name__,
                    "category": getattr(exc, "category", "validation"),
                },
                indent=2,
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

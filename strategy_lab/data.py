"""Incremental immutable-history catalog and derived feature mart."""

from __future__ import annotations

import gzip
import json
import math
import os
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any, Callable, Iterator, Sequence, TypeVar

import orjson

from learning_data import load_security_master

from .config import LabConfig
from .database import LabDatabase, utc_now
from .hashing import canonical_sha256, file_sha256


class DataError(RuntimeError):
    """Raised when local historical data cannot support a truthful catalog."""


PROVIDER_PRIORITY = {"ibkr": 0, "massive": 1, "alpaca": 2}
ChunkValue = TypeVar("ChunkValue")


@dataclass(frozen=True)
class RawObservation:
    path: str
    size_bytes: int
    modified_ns: int
    symbol: str
    session_date: str
    security_type: str
    open: float
    high: float
    low: float
    close: float
    volume: float
    opening_return_30m: float | None
    opening_range_pct_30m: float | None
    intraday_entry_price: float | None
    intraday_future_high: float | None
    intraday_future_low: float | None
    intraday_exit_price: float | None
    daily_complete: bool
    intraday_complete: bool
    provider: str
    source_identity: str
    disposition: str = "READY"


@dataclass(frozen=True)
class RejectedFile:
    path: str
    size_bytes: int
    modified_ns: int
    symbol: str
    session_date: str
    content_identity: str
    disposition: str
    error: str


def rejected_file(path: Path, symbol: str, error: Exception) -> RejectedFile:
    stat = path.stat()
    session_date = path.name[:-8]
    return RejectedFile(
        path=str(path),
        size_bytes=stat.st_size,
        modified_ns=stat.st_mtime_ns,
        symbol=symbol,
        session_date=session_date,
        content_identity=file_sha256(path),
        disposition="REJECTED_" + type(error).__name__.upper(),
        error=str(error),
    )


def parse_history_task(
    task: tuple[str, str, str | None],
) -> RawObservation | RejectedFile:
    """Parse one immutable file in a process-safe worker boundary."""
    raw_path, symbol, security_type = task
    path = Path(raw_path)
    if security_type is None:
        return rejected_file(
            path,
            symbol,
            DataError("point-in-time security identity is unavailable"),
        )
    try:
        return parse_history_file(path, security_type)
    except Exception as exc:  # one bad file cannot discard the committed batch
        return rejected_file(path, symbol, exc)


def _number(value: Any, field: str, *, positive: bool = False) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise DataError(f"{field} must be numeric")
    result = float(value)
    if not math.isfinite(result) or (positive and result <= 0):
        raise DataError(f"{field} is outside its valid range")
    return result


def _dataset_rank(dataset: dict[str, Any], timeframe: str) -> tuple[int, int, str]:
    quality = dataset.get("quality")
    complete = bool(quality.get("complete")) if isinstance(quality, dict) else False
    provider = str(dataset.get("provider", "")).lower()
    return (
        0 if complete else 1,
        PROVIDER_PRIORITY.get(provider, 99),
        str(dataset.get("id", "")),
    )


def _datasets(payload: dict[str, Any], timeframe: str) -> list[dict[str, Any]]:
    result = [
        item
        for item in payload.get("datasets", [])
        if isinstance(item, dict)
        and item.get("timeframe") == timeframe
        and isinstance(item.get("rows"), list)
        and item["rows"]
    ]
    return sorted(result, key=lambda item: _dataset_rank(item, timeframe))


def _daily_row(dataset: dict[str, Any]) -> tuple[dict[str, Any], bool]:
    rows = dataset["rows"]
    row = rows[-1]
    if not isinstance(row, dict):
        raise DataError("daily dataset row must be an object")
    quality = dataset.get("quality")
    complete = bool(quality.get("complete")) if isinstance(quality, dict) else False
    return row, complete


def _aggregate_intraday(
    dataset: dict[str, Any],
) -> tuple[dict[str, float], dict[str, float | bool | None]]:
    rows = [row for row in dataset["rows"] if isinstance(row, dict)]
    if not rows:
        raise DataError("intraday dataset has no valid rows")
    rows = sorted(rows, key=lambda item: str(item.get("t", "")))
    opens = [_number(row.get("o"), "intraday open", positive=True) for row in rows]
    highs = [_number(row.get("h"), "intraday high", positive=True) for row in rows]
    lows = [_number(row.get("l"), "intraday low", positive=True) for row in rows]
    closes = [_number(row.get("c"), "intraday close", positive=True) for row in rows]
    volumes = [_number(row.get("v", 0), "intraday volume") for row in rows]
    if any(high < low for high, low in zip(highs, lows, strict=True)):
        raise DataError("intraday high is below low")
    quality = dataset.get("quality")
    complete = (
        bool(quality.get("complete")) if isinstance(quality, dict) else False
    ) and len(rows) >= 20
    opening_return = None
    opening_range = None
    entry = future_high = future_low = exit_price = None
    if len(rows) >= 3:
        opening_return = closes[1] / opens[0] - 1.0
        opening_range = max(highs[:2]) / min(lows[:2]) - 1.0
        entry = opens[2]
        future_high = max(highs[2:])
        future_low = min(lows[2:])
        exit_price = closes[-1]
    return (
        {
            "o": opens[0],
            "h": max(highs),
            "l": min(lows),
            "c": closes[-1],
            "v": sum(volumes),
        },
        {
            "complete": complete,
            "opening_return_30m": opening_return,
            "opening_range_pct_30m": opening_range,
            "intraday_entry_price": entry,
            "intraday_future_high": future_high,
            "intraday_future_low": future_low,
            "intraday_exit_price": exit_price,
        },
    )


def parse_history_file(path: Path, security_type: str) -> RawObservation:
    stat = path.stat()
    try:
        with gzip.open(path, "rb") as source:
            payload = orjson.loads(source.read())
    except (OSError, orjson.JSONDecodeError) as exc:
        raise DataError(f"cannot read canonical history {path}: {exc}") from exc
    if not isinstance(payload, dict):
        raise DataError(f"canonical history must contain an object: {path}")
    symbol = str(payload.get("symbol", "")).strip().upper()
    session_date = str(payload.get("date", ""))
    try:
        date.fromisoformat(session_date)
    except ValueError as exc:
        raise DataError(f"history date is invalid: {path}") from exc
    if path.parent.parent.name.lower() != symbol.lower():
        raise DataError(f"history symbol does not match path: {path}")

    intraday_candidates = _datasets(payload, "15m")
    daily_candidates = _datasets(payload, "1d")
    intraday: dict[str, float | bool | None] = {
        "complete": False,
        "opening_return_30m": None,
        "opening_range_pct_30m": None,
        "intraday_entry_price": None,
        "intraday_future_high": None,
        "intraday_future_low": None,
        "intraday_exit_price": None,
    }
    aggregate: dict[str, float] | None = None
    intraday_dataset: dict[str, Any] | None = None
    if intraday_candidates:
        intraday_dataset = intraday_candidates[0]
        aggregate, intraday = _aggregate_intraday(intraday_dataset)

    daily_complete = False
    daily_dataset: dict[str, Any] | None = None
    if daily_candidates:
        daily_dataset = daily_candidates[0]
        daily, daily_complete = _daily_row(daily_dataset)
        aggregate = {
            "o": _number(daily.get("o"), "daily open", positive=True),
            "h": _number(daily.get("h"), "daily high", positive=True),
            "l": _number(daily.get("l"), "daily low", positive=True),
            "c": _number(daily.get("c"), "daily close", positive=True),
            "v": _number(daily.get("v", 0), "daily volume"),
        }
    elif aggregate is not None:
        daily_complete = bool(intraday["complete"])
    if aggregate is None:
        raise DataError(f"history has no usable daily or 15-minute bars: {path}")
    if aggregate["h"] < aggregate["l"]:
        raise DataError(f"daily high is below low: {path}")

    selected = [item for item in (daily_dataset, intraday_dataset) if item]
    provider = str(selected[0].get("provider", "unknown")) if selected else "unknown"
    source_identity = canonical_sha256(
        {
            "path": str(path.relative_to(path.parents[2])),
            "datasets": [
                {
                    "id": item.get("id"),
                    "content_sha256": item.get("content_sha256"),
                    "provider": item.get("provider"),
                    "timeframe": item.get("timeframe"),
                }
                for item in selected
            ],
        }
    )
    return RawObservation(
        path=str(path),
        size_bytes=stat.st_size,
        modified_ns=stat.st_mtime_ns,
        symbol=symbol,
        session_date=session_date,
        security_type=security_type,
        open=aggregate["o"],
        high=aggregate["h"],
        low=aggregate["l"],
        close=aggregate["c"],
        volume=aggregate["v"],
        opening_return_30m=intraday["opening_return_30m"],
        opening_range_pct_30m=intraday["opening_range_pct_30m"],
        intraday_entry_price=intraday["intraday_entry_price"],
        intraday_future_high=intraday["intraday_future_high"],
        intraday_future_low=intraday["intraday_future_low"],
        intraday_exit_price=intraday["intraday_exit_price"],
        daily_complete=daily_complete,
        intraday_complete=bool(intraday["complete"]),
        provider=provider,
        source_identity=source_identity,
    )


def _security_index(known_etfs: Sequence[str] = ()) -> dict[str, list[dict[str, Any]]]:
    records = load_security_master()
    result: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for record in records:
        if record["security_type"] in {"COMMON", "ETF"}:
            result[str(record["symbol"])].append(record)
    for symbol in known_etfs:
        normalized = str(symbol).strip().upper()
        if normalized and normalized not in result:
            result[normalized].append(
                {
                    "symbol": normalized,
                    "security_type": "ETF",
                    "status": "ACTIVE",
                    "valid_from": "1900-01-01",
                    "valid_to": None,
                    "instrument_id": f"configured-etf:{normalized}",
                    "primary_exchange": "CONFIGURED",
                    "provenance_paths": ["config/strategy_lab.toml"],
                }
            )
    return result


def _security_type(records: Sequence[dict[str, Any]], session_date: str) -> str | None:
    target = date.fromisoformat(session_date)
    for record in records:
        observed = record.get("observed_dates")
        if isinstance(observed, list):
            if session_date in observed:
                return str(record["security_type"])
            continue
        start = date.fromisoformat(str(record["valid_from"]))
        end = (
            date.fromisoformat(str(record["valid_to"]))
            if record.get("valid_to")
            else date.max
        )
        if start <= target <= end:
            return str(record["security_type"])
    stable_types = {str(record["security_type"]) for record in records}
    return next(iter(stable_types)) if len(stable_types) == 1 else None


def _chunks(values: Sequence[ChunkValue], size: int) -> Iterator[Sequence[ChunkValue]]:
    for offset in range(0, len(values), size):
        yield values[offset : offset + size]


class HistoricalCatalog:
    def __init__(self, config: LabConfig, database: LabDatabase) -> None:
        self.config = config
        self.database = database

    def _eligible_directories(
        self, symbols: Sequence[str] | None
    ) -> list[tuple[str, Path, list[dict[str, Any]]]]:
        root = self.config.historical_data_root
        security = _security_index(self.config.section("catalog").get("known_etfs", []))
        requested = {symbol.upper() for symbol in symbols} if symbols else None
        entries: list[tuple[str, Path, list[dict[str, Any]]]] = []
        for symbol in sorted(security):
            if requested is not None and symbol not in requested:
                continue
            directory = root / symbol.lower()
            if directory.is_dir():
                entries.append((symbol, directory, security[symbol]))
        if requested is not None:
            missing = sorted(requested.difference(symbol for symbol, _, _ in entries))
            if missing:
                raise DataError(
                    f"requested symbols are unavailable or unidentified: {missing}"
                )
        return entries

    def sync(
        self,
        *,
        symbols: Sequence[str] | None = None,
        maximum_symbols: int | None = None,
        rebuild_features: bool = True,
        progress_callback: Callable[[dict[str, Any]], None] | None = None,
    ) -> dict[str, Any]:
        entries = self._eligible_directories(symbols)
        if maximum_symbols is not None:
            if maximum_symbols < 1:
                raise DataError("maximum_symbols must be positive")
            entries = entries[:maximum_symbols]
        workers = int(self.config.section("catalog")["workers"])
        parsed = skipped = failed = 0
        failures: list[dict[str, str]] = []
        started_at = utc_now()

        def publish(status: str, completed_symbols: int) -> None:
            progress = {
                "status": status,
                "started_at": started_at,
                "updated_at": utc_now(),
                "symbols_completed": completed_symbols,
                "symbols_total": len(entries),
                "files_parsed": parsed,
                "files_unchanged": skipped,
                "files_rejected": failed,
                "provider_requests": 0,
                "broker_actions": 0,
            }
            self.database.set_metadata(
                "catalog_progress",
                json.dumps(progress, sort_keys=True, separators=(",", ":")),
            )
            if progress_callback:
                progress_callback(progress)

        publish("SYNCING", 0)
        with ProcessPoolExecutor(max_workers=workers) as pool:
            for completed_symbols, (symbol, directory, records) in enumerate(
                entries, 1
            ):
                paths = sorted(directory.glob("[0-9][0-9][0-9][0-9]/*.json.gz"))
                existing_rows = self.database.connection.execute(
                    "SELECT path, size_bytes, modified_ns FROM raw_files WHERE symbol = ?",
                    [symbol],
                ).fetchall()
                existing = {
                    str(path): (int(size), int(mtime))
                    for path, size, mtime in existing_rows
                }
                pending: list[Path] = []
                for path in paths:
                    stat = path.stat()
                    if existing.get(str(path)) == (stat.st_size, stat.st_mtime_ns):
                        skipped += 1
                    else:
                        pending.append(path)

                tasks = [
                    (
                        str(path),
                        symbol,
                        _security_type(records, path.name[:-8]),
                    )
                    for path in pending
                ]
                results: list[RawObservation | RejectedFile] = []
                for group in _chunks(tasks, 1000):
                    results.extend(pool.map(parse_history_task, group, chunksize=16))
                ready = [item for item in results if isinstance(item, RawObservation)]
                errors = [item for item in results if isinstance(item, RejectedFile)]
                inspected_at = utc_now()
                ready_observations = [
                    {
                        "symbol": item.symbol,
                        "session_date": item.session_date,
                        "security_type": item.security_type,
                        "open": item.open,
                        "high": item.high,
                        "low": item.low,
                        "close": item.close,
                        "volume": item.volume,
                        "opening_return_30m": item.opening_return_30m,
                        "opening_range_pct_30m": item.opening_range_pct_30m,
                        "intraday_entry_price": item.intraday_entry_price,
                        "intraday_future_high": item.intraday_future_high,
                        "intraday_future_low": item.intraday_future_low,
                        "intraday_exit_price": item.intraday_exit_price,
                        "daily_complete": item.daily_complete,
                        "intraday_complete": item.intraday_complete,
                        "provider": item.provider,
                        "source_identity": item.source_identity,
                        "source_path": item.path,
                    }
                    for item in ready
                ]
                ready_files = [
                    {
                        "path": item.path,
                        "symbol": item.symbol,
                        "session_date": item.session_date,
                        "size_bytes": item.size_bytes,
                        "modified_ns": item.modified_ns,
                        "content_identity": item.source_identity,
                        "disposition": item.disposition,
                        "inspected_at": inspected_at,
                    }
                    for item in ready
                ]
                rejected_files = [
                    {
                        "path": item.path,
                        "symbol": item.symbol,
                        "session_date": item.session_date,
                        "size_bytes": item.size_bytes,
                        "modified_ns": item.modified_ns,
                        "content_identity": item.content_identity,
                        "disposition": item.disposition,
                        "inspected_at": inspected_at,
                    }
                    for item in errors
                ]
                if ready or errors:
                    import pyarrow as pa

                    connection = self.database.connection
                    if ready:
                        connection.register(
                            "_strategy_lab_ready_observations",
                            pa.Table.from_pylist(ready_observations),
                        )
                        connection.register(
                            "_strategy_lab_ready_files",
                            pa.Table.from_pylist(ready_files),
                        )
                    if errors:
                        connection.register(
                            "_strategy_lab_rejected_files",
                            pa.Table.from_pylist(rejected_files),
                        )
                    try:
                        with self.database.transaction():
                            if ready:
                                connection.execute(
                                    """
                                    INSERT OR REPLACE INTO observations BY NAME
                                    SELECT * FROM _strategy_lab_ready_observations
                                    """
                                )
                                connection.execute(
                                    """
                                    INSERT OR REPLACE INTO raw_files BY NAME
                                    SELECT * FROM _strategy_lab_ready_files
                                    """
                                )
                            if errors:
                                connection.execute(
                                    """
                                    DELETE FROM observations
                                    USING _strategy_lab_rejected_files
                                    WHERE observations.source_path =
                                          _strategy_lab_rejected_files.path
                                    """
                                )
                                connection.execute(
                                    """
                                    INSERT OR REPLACE INTO raw_files BY NAME
                                    SELECT * FROM _strategy_lab_rejected_files
                                    """
                                )
                    finally:
                        if ready:
                            connection.unregister(
                                "_strategy_lab_ready_observations"
                            )
                            connection.unregister("_strategy_lab_ready_files")
                        if errors:
                            connection.unregister("_strategy_lab_rejected_files")
                parsed += len(ready)
                failed += len(errors)
                for item in errors[: max(0, 50 - len(failures))]:
                    failures.append({"path": item.path, "error": item.error})
                publish("SYNCING", completed_symbols)

        publish("BUILDING_FEATURES" if rebuild_features else "READY", len(entries))
        feature = self.build_feature_mart() if rebuild_features else None
        publish("READY", len(entries))
        return {
            "status": "READY" if failed == 0 else "READY_WITH_FILE_REJECTIONS",
            "started_at": started_at,
            "completed_at": utc_now(),
            "symbols_considered": len(entries),
            "files_parsed": parsed,
            "files_unchanged": skipped,
            "files_rejected": failed,
            "failure_samples": failures,
            "provider_requests": 0,
            "broker_actions": 0,
            "feature_mart": feature,
        }

    def build_feature_mart(self) -> dict[str, Any]:
        connection = self.database.connection
        session_rows = connection.execute(
            "SELECT DISTINCT session_date FROM observations WHERE daily_complete ORDER BY session_date"
        ).fetchall()
        sessions = [row[0] for row in session_rows]
        catalog = self.config.section("catalog")
        minimum_common = int(catalog["minimum_common_sessions"])
        holdout = max(
            int(catalog["minimum_holdout_sessions"]),
            math.ceil(len(sessions) * float(catalog["holdout_fraction"])),
        )
        embargo = int(catalog["embargo_sessions"])
        development_count = len(sessions) - holdout - embargo
        capacity_state = (
            "CAPACITY_READY"
            if len(sessions) >= minimum_common
            and development_count >= int(catalog["minimum_development_sessions"])
            else "INSUFFICIENT_CAPACITY"
        )
        development_end = (
            sessions[development_count - 1] if development_count > 0 else None
        )
        embargo_start = (
            sessions[development_count]
            if 0 <= development_count < len(sessions)
            else None
        )
        holdout_start_index = development_count + embargo
        holdout_start = (
            sessions[holdout_start_index]
            if 0 <= holdout_start_index < len(sessions)
            else None
        )
        minimum_price = float(catalog["minimum_price"])
        minimum_volume = float(catalog["minimum_median_dollar_volume_20"])
        minimum_prior = int(catalog["minimum_prior_sessions"])
        minimum_coverage = float(catalog["minimum_symbol_coverage"])

        connection.execute("DROP TABLE IF EXISTS features")
        connection.execute(
            f"""
            CREATE TABLE features AS
            WITH session_index AS (
                SELECT
                    session_date,
                    row_number() OVER (ORDER BY session_date) AS market_session_number
                FROM (
                    SELECT DISTINCT session_date
                    FROM observations
                    WHERE daily_complete
                )
            ),
            indexed AS (
                SELECT observations.*, session_index.market_session_number
                FROM observations
                JOIN session_index USING(session_date)
                WHERE observations.daily_complete
            ),
            lagged AS (
                SELECT
                    *,
                    row_number() OVER symbol_window AS symbol_session_number,
                    count(*) OVER (PARTITION BY symbol) / (
                        max(market_session_number) OVER (PARTITION BY symbol)
                        - min(market_session_number) OVER (PARTITION BY symbol)
                        + 1.0
                    ) AS coverage_ratio,
                    lag(close, 1) OVER symbol_window AS previous_close,
                    lag(close, 5) OVER symbol_window AS close_5_sessions_ago,
                    lag(close, 20) OVER symbol_window AS close_20_sessions_ago,
                    avg(close) OVER (
                        PARTITION BY symbol ORDER BY session_date
                        ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
                    ) AS sma_20,
                    avg(close) OVER (
                        PARTITION BY symbol ORDER BY session_date
                        ROWS BETWEEN 49 PRECEDING AND CURRENT ROW
                    ) AS sma_50,
                    avg(volume) OVER (
                        PARTITION BY symbol ORDER BY session_date
                        ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
                    ) AS average_volume_20,
                    median(close * volume) OVER (
                        PARTITION BY symbol ORDER BY session_date
                        ROWS BETWEEN 20 PRECEDING AND 1 PRECEDING
                    ) AS median_dollar_volume_20,
                    min(low) OVER (
                        PARTITION BY symbol ORDER BY session_date
                        ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
                    ) AS low_14,
                    max(high) OVER (
                        PARTITION BY symbol ORDER BY session_date
                        ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
                    ) AS high_14,
                    lead(session_date, 1) OVER symbol_window AS future_date_1,
                    lead(open, 1) OVER symbol_window AS future_open_1,
                    lead(high, 1) OVER symbol_window AS future_high_1,
                    lead(low, 1) OVER symbol_window AS future_low_1,
                    lead(close, 1) OVER symbol_window AS future_close_1,
                    lead(session_date, 2) OVER symbol_window AS future_date_2,
                    lead(open, 2) OVER symbol_window AS future_open_2,
                    lead(high, 2) OVER symbol_window AS future_high_2,
                    lead(low, 2) OVER symbol_window AS future_low_2,
                    lead(close, 2) OVER symbol_window AS future_close_2,
                    lead(session_date, 3) OVER symbol_window AS future_date_3,
                    lead(open, 3) OVER symbol_window AS future_open_3,
                    lead(high, 3) OVER symbol_window AS future_high_3,
                    lead(low, 3) OVER symbol_window AS future_low_3,
                    lead(close, 3) OVER symbol_window AS future_close_3,
                    lead(session_date, 4) OVER symbol_window AS future_date_4,
                    lead(open, 4) OVER symbol_window AS future_open_4,
                    lead(high, 4) OVER symbol_window AS future_high_4,
                    lead(low, 4) OVER symbol_window AS future_low_4,
                    lead(close, 4) OVER symbol_window AS future_close_4,
                    lead(session_date, 5) OVER symbol_window AS future_date_5,
                    lead(open, 5) OVER symbol_window AS future_open_5,
                    lead(high, 5) OVER symbol_window AS future_high_5,
                    lead(low, 5) OVER symbol_window AS future_low_5,
                    lead(close, 5) OVER symbol_window AS future_close_5
                FROM indexed
                WINDOW symbol_window AS (PARTITION BY symbol ORDER BY session_date)
            ),
            ranged AS (
                SELECT
                    *,
                    greatest(
                        high - low,
                        abs(high - previous_close),
                        abs(low - previous_close)
                    ) AS true_range
                FROM lagged
            ),
            featured AS (
                SELECT
                    *,
                    close / previous_close - 1 AS return_1d,
                    close / close_5_sessions_ago - 1 AS return_5d,
                    close / close_20_sessions_ago - 1 AS return_20d,
                    open / previous_close - 1 AS gap_1d,
                    (close - low_14) / nullif(high_14 - low_14, 0) AS range_position_14,
                    volume / nullif(average_volume_20, 0) AS volume_ratio_20,
                    avg(true_range) OVER (
                        PARTITION BY symbol ORDER BY session_date
                        ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
                    ) / close AS atr_pct_14,
                    close / nullif(sma_20, 0) AS close_sma_20_ratio,
                    close / nullif(sma_50, 0) AS close_sma_50_ratio
                FROM ranged
            ),
            market AS (
                SELECT session_date, return_5d AS market_return_5d
                FROM featured WHERE symbol = 'SPY'
            )
            SELECT
                featured.*,
                market.market_return_5d,
                (
                    security_type IN ('COMMON', 'ETF')
                    AND close >= {minimum_price}
                    AND median_dollar_volume_20 >= {minimum_volume}
                    AND symbol_session_number >= {minimum_prior}
                    AND coverage_ratio >= {minimum_coverage}
                ) AS eligible,
                CASE
                    WHEN {repr(development_end.isoformat() if development_end else "0001-01-01")}::DATE >= session_date
                        THEN 'development'
                    WHEN {repr(holdout_start.isoformat() if holdout_start else "9999-12-31")}::DATE <= session_date
                        THEN 'holdout'
                    ELSE 'embargo'
                END AS data_partition
            FROM featured
            LEFT JOIN market USING(session_date)
            """
        )
        feature_path = self.config.feature_path
        temporary = feature_path.with_suffix(".parquet.tmp")
        temporary.unlink(missing_ok=True)
        escaped = str(temporary).replace("'", "''")
        connection.execute(
            f"COPY (SELECT * FROM features ORDER BY session_date, symbol) "
            f"TO '{escaped}' (FORMAT PARQUET, COMPRESSION ZSTD)"
        )
        os.replace(temporary, feature_path)
        feature_sha256 = file_sha256(feature_path)
        counts = connection.execute(
            """
            SELECT count(*), count(DISTINCT symbol), count(DISTINCT session_date),
                   min(session_date), max(session_date)
            FROM features
            """
        ).fetchone()
        raw_counts = connection.execute(
            """
            SELECT
                count(*) FILTER (WHERE disposition='READY'),
                count(*) FILTER (WHERE disposition<>'READY')
            FROM raw_files
            """
        ).fetchone()
        manifest = {
            "schema_version": 1,
            "feature_path": str(feature_path),
            "feature_sha256": feature_sha256,
            "observation_count": int(counts[0]),
            "symbol_count": int(counts[1]),
            "session_count": int(counts[2]),
            "ready_file_count": int(raw_counts[0]),
            "rejected_file_count": int(raw_counts[1]),
            "first_session": counts[3].isoformat() if counts[3] else None,
            "last_session": counts[4].isoformat() if counts[4] else None,
            "development_end": development_end.isoformat() if development_end else None,
            "embargo_start": embargo_start.isoformat() if embargo_start else None,
            "holdout_start": holdout_start.isoformat() if holdout_start else None,
            "capacity_state": capacity_state,
            "provider_requests": 0,
            "broker_actions": 0,
        }
        dataset_sha256 = canonical_sha256(manifest)
        data_version_id = f"data-{dataset_sha256[:20]}"
        connection.execute(
            """
            INSERT INTO data_versions VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(data_version_id) DO NOTHING
            """,
            [
                data_version_id,
                utc_now(),
                dataset_sha256,
                feature_sha256,
                int(counts[0]),
                int(counts[1]),
                int(counts[2]),
                counts[3],
                counts[4],
                development_end,
                embargo_start,
                holdout_start,
                capacity_state,
                json.dumps(manifest, sort_keys=True, separators=(",", ":")),
            ],
        )
        self.database.set_metadata("active_data_version_id", data_version_id)
        return {"data_version_id": data_version_id, **manifest}

"""Read-only Interactive Brokers TWS historical-market-data adapter.

This module intentionally exposes no account, portfolio, order, or execution
methods.  It connects to an already authenticated Trader Workstation (or IB
Gateway) socket and fetches historical bars and historical bid/ask ticks for
the offline replay workflow.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import socket
import subprocess
import threading
import time
from collections import Counter
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, time as wall_time, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Mapping, Sequence
from zoneinfo import ZoneInfo

from dotenv import dotenv_values
from ibapi.client import EClient
from ibapi.contract import Contract
from ibapi.wrapper import EWrapper

from historical_metrics import (
    HistoricalMetricError,
    average_daily_volume,
    average_true_range,
)


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_ENV_PATH = PROJECT_ROOT / ".env"
EASTERN = ZoneInfo("America/New_York")
UTC = timezone.utc
INFORMATIONAL_CODES = {
    2104,
    2106,
    2107,
    2108,
    2119,
    2158,
}
CONNECTION_ERROR_CODES = {326, 502, 503, 504, 507, 1100, 1300}
BAR_TYPES = {"TRADES", "MIDPOINT", "BID", "ASK"}
PRE_SESSION_CACHE_VERSION = 2
HISTORICAL_BAR_CHUNK_DAYS = {
    "1 sec": 1,
    "5 secs": 1,
    "15 secs": 1,
    "30 secs": 1,
    "1 min": 2,
    "2 mins": 3,
    "3 mins": 5,
    # IBKR's current TWS API table permits day-duration requests for five-minute
    # bars. Thirty days stays near a few thousand RTH bars while collapsing the
    # preflight's prior-session opening history into one provider request.
    "5 mins": 30,
    "10 mins": 30,
    "15 mins": 30,
    "30 mins": 30,
    "1 hour": 90,
    "1 day": 365,
}


class IBKRHistoricalError(RuntimeError):
    """Base error for safe, read-only IBKR historical collection."""


class IBKRConfigurationError(IBKRHistoricalError):
    """Raised when local TWS configuration is unavailable or invalid."""


class IBKRRequestError(IBKRHistoricalError):
    """Raised when TWS rejects or times out a historical-data request."""

    def __init__(self, message: str, *, error_code: int | None = None):
        super().__init__(message)
        self.error_code = error_code


def historical_error_category(exc: BaseException) -> str:
    """Classify collection failures without treating a disconnect as bad data."""
    if isinstance(exc, IBKRRequestError):
        message = str(exc).lower()
        if exc.error_code in CONNECTION_ERROR_CODES or "timed out" in message:
            return "retryable_transport"
        if (
            exc.error_code == 100
            or "pacing violation" in message
            or "query cancelled" in message
            or "maximum allowed message rate" in message
        ):
            return "retryable_provider"
        return "permanent_fidelity"
    if isinstance(exc, IBKRConfigurationError):
        return "retryable_transport"
    return "local_processing"


def is_retryable_historical_error(exc: BaseException) -> bool:
    return historical_error_category(exc).startswith("retryable_")


def is_symbol_scoped_historical_no_data(exc: BaseException) -> bool:
    """Return whether IBKR reported a permanent no-data gap for one symbol."""
    if not isinstance(exc, IBKRRequestError) or exc.error_code != 162:
        return False
    message = str(exc).lower()
    return "hmds query returned no data" in message


def _env_bool(value: Any, default: bool = False) -> bool:
    if value is None or str(value).strip() == "":
        return default
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise IBKRConfigurationError(f"invalid boolean value: {value!r}")


def _positive_float(value: Any, name: str) -> float:
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise IBKRConfigurationError(f"{name} must be numeric") from exc
    if not math.isfinite(result) or result <= 0:
        raise IBKRConfigurationError(f"{name} must be positive and finite")
    return result


def _integer(value: Any, name: str, *, minimum: int = 0) -> int:
    try:
        result = int(value)
    except (TypeError, ValueError) as exc:
        raise IBKRConfigurationError(f"{name} must be an integer") from exc
    if result < minimum:
        raise IBKRConfigurationError(f"{name} must be >= {minimum}")
    return result


@dataclass(frozen=True)
class IBKRConfig:
    host: str = "127.0.0.1"
    port: int = 7496
    client_id: int = 71
    connect_timeout_seconds: float = 12.0
    request_timeout_seconds: float = 90.0
    minimum_request_spacing_seconds: float = 0.4
    max_concurrent_requests: int = 4
    auto_start_tws: bool = False
    tws_start_timeout_seconds: float = 45.0
    tws_app_path: str = ""
    exchange: str = "SMART"
    currency: str = "USD"
    primary_exchange: str = ""

    @classmethod
    def from_env(cls, path: Path = DEFAULT_ENV_PATH) -> "IBKRConfig":
        values: dict[str, Any] = {}
        if path.exists():
            values.update(dotenv_values(path, interpolate=False))
        for key in (
            "IBKR_HOST",
            "IBKR_PORT",
            "IBKR_CLIENT_ID",
            "IBKR_CONNECT_TIMEOUT_SECONDS",
            "IBKR_REQUEST_TIMEOUT_SECONDS",
            "IBKR_MIN_REQUEST_SPACING_SECONDS",
            "IBKR_MAX_CONCURRENT_REQUESTS",
            "IBKR_AUTO_START_TWS",
            "IBKR_TWS_START_TIMEOUT_SECONDS",
            "IBKR_TWS_APP_PATH",
            "IBKR_EXCHANGE",
            "IBKR_CURRENCY",
            "IBKR_PRIMARY_EXCHANGE",
        ):
            if key in os.environ:
                values[key] = os.environ[key]
        host = str(values.get("IBKR_HOST") or cls.host).strip()
        if not host:
            raise IBKRConfigurationError("IBKR_HOST cannot be empty")
        return cls(
            host=host,
            port=_integer(values.get("IBKR_PORT") or cls.port, "IBKR_PORT", minimum=1),
            client_id=_integer(
                values.get("IBKR_CLIENT_ID") or cls.client_id,
                "IBKR_CLIENT_ID",
            ),
            connect_timeout_seconds=_positive_float(
                values.get("IBKR_CONNECT_TIMEOUT_SECONDS")
                or cls.connect_timeout_seconds,
                "IBKR_CONNECT_TIMEOUT_SECONDS",
            ),
            request_timeout_seconds=_positive_float(
                values.get("IBKR_REQUEST_TIMEOUT_SECONDS")
                or cls.request_timeout_seconds,
                "IBKR_REQUEST_TIMEOUT_SECONDS",
            ),
            minimum_request_spacing_seconds=_positive_float(
                values.get("IBKR_MIN_REQUEST_SPACING_SECONDS")
                or cls.minimum_request_spacing_seconds,
                "IBKR_MIN_REQUEST_SPACING_SECONDS",
            ),
            max_concurrent_requests=_integer(
                values.get("IBKR_MAX_CONCURRENT_REQUESTS")
                or cls.max_concurrent_requests,
                "IBKR_MAX_CONCURRENT_REQUESTS",
                minimum=1,
            ),
            auto_start_tws=_env_bool(
                values.get("IBKR_AUTO_START_TWS"), cls.auto_start_tws
            ),
            tws_start_timeout_seconds=_positive_float(
                values.get("IBKR_TWS_START_TIMEOUT_SECONDS")
                or cls.tws_start_timeout_seconds,
                "IBKR_TWS_START_TIMEOUT_SECONDS",
            ),
            tws_app_path=str(values.get("IBKR_TWS_APP_PATH") or "").strip(),
            exchange=str(values.get("IBKR_EXCHANGE") or cls.exchange).strip().upper(),
            currency=str(values.get("IBKR_CURRENCY") or cls.currency).strip().upper(),
            primary_exchange=str(values.get("IBKR_PRIMARY_EXCHANGE") or "")
            .strip()
            .upper(),
        )

    def public_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class _RequestState:
    kind: str
    event: threading.Event = field(default_factory=threading.Event)
    rows: list[dict[str, Any]] = field(default_factory=list)
    error: dict[str, Any] | None = None


def _socket_is_open(host: str, port: int, timeout: float = 0.5) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def ensure_tws_socket(config: IBKRConfig) -> None:
    if _socket_is_open(config.host, config.port):
        return
    if not config.auto_start_tws:
        raise IBKRConfigurationError(
            f"TWS/IB Gateway is not listening on {config.host}:{config.port}. "
            "Start and log in to Trader Workstation, enable API socket clients, "
            "or set IBKR_AUTO_START_TWS=true."
        )
    if config.host not in {"127.0.0.1", "localhost", "::1"}:
        raise IBKRConfigurationError(
            "automatic TWS launch is allowed only for a local IBKR_HOST"
        )
    if config.tws_app_path:
        app_path = Path(config.tws_app_path).expanduser()
        if not app_path.exists():
            raise IBKRConfigurationError(
                f"IBKR_TWS_APP_PATH does not exist: {app_path}"
            )
        command = ["open", str(app_path)]
    else:
        command = ["open", "-a", "Trader Workstation"]
    subprocess.run(command, check=True, capture_output=True, text=True)
    deadline = time.monotonic() + config.tws_start_timeout_seconds
    while time.monotonic() < deadline:
        if _socket_is_open(config.host, config.port):
            return
        time.sleep(0.5)
    raise IBKRConfigurationError(
        "Trader Workstation was launched but its API socket did not become "
        f"available on {config.host}:{config.port}; log in and verify API settings."
    )


def _coerce_datetime(value: str | datetime) -> datetime:
    if isinstance(value, datetime):
        result = value
    else:
        try:
            result = datetime.fromisoformat(str(value))
        except ValueError as exc:
            raise IBKRConfigurationError(f"invalid ISO datetime: {value!r}") from exc
    if result.tzinfo is None:
        result = result.replace(tzinfo=EASTERN)
    return result.astimezone(UTC)


def _parse_date(value: str | date) -> date:
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(str(value))
    except ValueError as exc:
        raise IBKRConfigurationError(f"invalid ISO date: {value!r}") from exc


def _format_ib_datetime(value: datetime) -> str:
    eastern = value.astimezone(EASTERN)
    return eastern.strftime("%Y%m%d %H:%M:%S") + " US/Eastern"


def _decimal_number(value: Any) -> float:
    if isinstance(value, Decimal):
        return float(value)
    return float(value or 0)


class _IBKRHistoricalConnection(EWrapper, EClient):
    """Internal callback client; never expose this raw EClient to callers."""

    def __init__(self, config: IBKRConfig):
        EClient.__init__(self, self)
        self.config = config
        self._lock = threading.RLock()
        self._ready = threading.Event()
        self._closed = threading.Event()
        self._thread: threading.Thread | None = None
        self._next_request_id = 1
        self._requests: dict[int, _RequestState] = {}
        self._connection_error: dict[str, Any] | None = None
        self._last_request_at = 0.0
        self._request_slots = threading.BoundedSemaphore(
            config.max_concurrent_requests
        )
        self._telemetry_started_at = time.monotonic()
        self._request_counts: Counter[str] = Counter()
        self._request_completed: Counter[str] = Counter()
        self._request_failed: Counter[str] = Counter()
        self._request_duration_seconds: Counter[str] = Counter()
        self._request_max_duration_seconds: dict[str, float] = {}
        self._request_in_flight = 0
        self._request_peak_in_flight = 0
        self._request_pacing_wait_seconds = 0.0
        self._request_slot_wait_seconds = 0.0

    def __enter__(self) -> "_IBKRHistoricalConnection":
        self.connect_and_wait()
        return self

    def __exit__(self, exc_type, exc, traceback_obj) -> None:
        self.close()

    def connect_and_wait(self) -> None:
        ensure_tws_socket(self.config)
        try:
            self.connect(
                self.config.host,
                self.config.port,
                clientId=self.config.client_id,
            )
        except Exception as exc:
            raise IBKRConfigurationError(
                f"failed to connect to TWS at {self.config.host}:{self.config.port}: {exc}"
            ) from exc
        self._thread = threading.Thread(
            target=self.run,
            name="ibkr-historical-reader",
            daemon=True,
        )
        self._thread.start()
        if not self._ready.wait(self.config.connect_timeout_seconds):
            error = self._connection_error
            self.close()
            suffix = f": {error}" if error else ""
            raise IBKRConfigurationError(
                "TWS socket connected but the API handshake did not complete"
                f"{suffix}. Check the TWS API client prompt and client ID."
            )
        if self._connection_error:
            error = self._connection_error
            self.close()
            raise IBKRConfigurationError(f"TWS connection failed: {error}")

    def close(self) -> None:
        try:
            self.disconnect()
        finally:
            self._closed.set()
            if self._thread and self._thread.is_alive():
                self._thread.join(timeout=2.0)

    def nextValidId(self, orderId: int) -> None:  # noqa: N802 - IB callback name
        with self._lock:
            self._next_request_id = max(self._next_request_id, int(orderId), 1)
        self._ready.set()

    def connectionClosed(self) -> None:  # noqa: N802 - IB callback name
        self._closed.set()
        with self._lock:
            requests = list(self._requests.values())
        for state in requests:
            if state.error is None:
                state.error = {"code": 507, "message": "TWS connection closed"}
            state.event.set()

    def error(self, reqId: int, *args: Any) -> None:  # noqa: N802
        if len(args) >= 3:
            _, error_code, error_string = args[:3]
        elif len(args) >= 2:
            error_code, error_string = args[:2]
        else:
            return
        try:
            code = int(error_code)
        except (TypeError, ValueError):
            code = 0
        if code in INFORMATIONAL_CODES:
            return
        error = {"req_id": int(reqId), "code": code, "message": str(error_string)}
        with self._lock:
            state = self._requests.get(int(reqId))
            if state is not None:
                state.error = error
                state.event.set()
            elif int(reqId) < 0 and code in CONNECTION_ERROR_CODES:
                self._connection_error = error
                self._ready.set()

    def historicalData(self, reqId: int, bar: Any) -> None:  # noqa: N802
        with self._lock:
            state = self._requests.get(int(reqId))
        if state is None:
            return
        timestamp = self._bar_timestamp(getattr(bar, "date", ""))
        if timestamp is None:
            return
        eastern = timestamp.astimezone(EASTERN)
        state.rows.append(
            {
                "epoch": int(timestamp.timestamp()),
                "time_et": eastern.isoformat(),
                "date_et": eastern.date().isoformat(),
                "open": float(getattr(bar, "open", 0.0)),
                "high": float(getattr(bar, "high", 0.0)),
                "low": float(getattr(bar, "low", 0.0)),
                "close": float(getattr(bar, "close", 0.0)),
                "volume": int(_decimal_number(getattr(bar, "volume", 0))),
                "count": int(getattr(bar, "barCount", 0) or 0),
                "wap": _decimal_number(getattr(bar, "wap", 0)),
                "interpolated": False,
            }
        )

    def historicalDataEnd(self, reqId: int, start: str, end: str) -> None:  # noqa: N802
        with self._lock:
            state = self._requests.get(int(reqId))
        if state is not None:
            state.event.set()

    def contractDetails(self, reqId: int, details: Any) -> None:  # noqa: N802
        with self._lock:
            state = self._requests.get(int(reqId))
        if state is None:
            return
        contract = details.contract
        state.rows.append(
            {
                "symbol": str(getattr(contract, "symbol", "")),
                "local_symbol": str(getattr(contract, "localSymbol", "")),
                "security_type": str(getattr(contract, "secType", "")),
                "currency": str(getattr(contract, "currency", "")),
                "exchange": str(getattr(contract, "exchange", "")),
                "primary_exchange": str(getattr(contract, "primaryExchange", "")),
                "valid_exchanges": str(getattr(details, "validExchanges", "")),
                "long_name": str(getattr(details, "longName", "")),
                "stock_type": str(getattr(details, "stockType", "")),
                "industry": str(getattr(details, "industry", "")),
                "category": str(getattr(details, "category", "")),
                "subcategory": str(getattr(details, "subcategory", "")),
            }
        )

    def contractDetailsEnd(self, reqId: int) -> None:  # noqa: N802
        with self._lock:
            state = self._requests.get(int(reqId))
        if state is not None:
            state.event.set()

    def historicalTicksBidAsk(
        self, reqId: int, ticks: Sequence[Any], done: bool
    ) -> None:  # noqa: N802
        with self._lock:
            state = self._requests.get(int(reqId))
        if state is None:
            return
        for tick in ticks:
            epoch = int(getattr(tick, "time"))
            eastern = datetime.fromtimestamp(epoch, UTC).astimezone(EASTERN)
            attributes = getattr(tick, "tickAttribBidAsk", None)
            state.rows.append(
                {
                    "epoch": epoch,
                    "time_et": eastern.isoformat(),
                    "bid": float(getattr(tick, "priceBid", 0.0)),
                    "ask": float(getattr(tick, "priceAsk", 0.0)),
                    "bid_size": _decimal_number(getattr(tick, "sizeBid", 0)),
                    "ask_size": _decimal_number(getattr(tick, "sizeAsk", 0)),
                    "bid_past_low": bool(getattr(attributes, "bidPastLow", False)),
                    "ask_past_high": bool(getattr(attributes, "askPastHigh", False)),
                }
            )
        if done:
            state.event.set()

    @staticmethod
    def _bar_timestamp(value: Any) -> datetime | None:
        raw = str(value).strip()
        if not raw:
            return None
        if raw.isdigit() and len(raw) >= 10:
            return datetime.fromtimestamp(int(raw), UTC)
        if raw.isdigit() and len(raw) == 8:
            parsed = datetime.strptime(raw, "%Y%m%d").replace(tzinfo=EASTERN)
            return parsed.astimezone(UTC)
        for fmt in ("%Y%m%d  %H:%M:%S", "%Y%m%d %H:%M:%S"):
            try:
                parsed = datetime.strptime(raw, fmt).replace(tzinfo=EASTERN)
                return parsed.astimezone(UTC)
            except ValueError:
                continue
        return None

    def _contract(self, symbol: str) -> Contract:
        normalized = str(symbol).strip().upper()
        if not normalized or not normalized.replace(".", "").isalnum():
            raise IBKRConfigurationError(f"invalid equity symbol: {symbol!r}")
        contract = Contract()
        contract.symbol = normalized
        contract.secType = "STK"
        contract.exchange = self.config.exchange
        contract.currency = self.config.currency
        if self.config.primary_exchange:
            contract.primaryExchange = self.config.primary_exchange
        return contract

    def _allocate_request(self, kind: str) -> tuple[int, _RequestState]:
        with self._lock:
            request_id = self._next_request_id
            self._next_request_id += 1
            state = _RequestState(kind=kind)
            self._requests[request_id] = state
        return request_id, state

    def _pacing_wait(self) -> float:
        """Reserve a globally spaced send time before sleeping.

        Reserving under the lock is important once multiple collection workers
        share the connection.  Computing a delay and updating the timestamp in
        two separate critical sections lets several threads wake and submit at
        once, defeating the configured pacing interval.
        """
        with self._lock:
            now = time.monotonic()
            scheduled_at = max(
                now,
                self._last_request_at
                + self.config.minimum_request_spacing_seconds,
            )
            self._last_request_at = scheduled_at
            delay = scheduled_at - now
        if delay:
            time.sleep(delay)
        return delay

    def _begin_provider_request(self, kind: str) -> float:
        slot_started = time.monotonic()
        self._request_slots.acquire()
        slot_wait = time.monotonic() - slot_started
        try:
            pacing_wait = self._pacing_wait()
        except Exception:
            self._request_slots.release()
            raise
        with self._lock:
            self._request_counts[kind] += 1
            self._request_in_flight += 1
            self._request_peak_in_flight = max(
                self._request_peak_in_flight,
                self._request_in_flight,
            )
            self._request_pacing_wait_seconds += pacing_wait
            self._request_slot_wait_seconds += slot_wait
        return time.monotonic()

    def _end_provider_request(
        self, kind: str, *, started_at: float, failed: bool
    ) -> None:
        duration = time.monotonic() - started_at
        with self._lock:
            target = self._request_failed if failed else self._request_completed
            target[kind] += 1
            self._request_duration_seconds[kind] += duration
            self._request_max_duration_seconds[kind] = max(
                self._request_max_duration_seconds.get(kind, 0.0),
                duration,
            )
            self._request_in_flight -= 1
        self._request_slots.release()

    def request_telemetry(self) -> dict[str, Any]:
        with self._lock:
            submitted = sum(self._request_counts.values())
            completed = sum(self._request_completed.values())
            failed = sum(self._request_failed.values())
            return {
                "elapsed_seconds": time.monotonic() - self._telemetry_started_at,
                "submitted": submitted,
                "completed": completed,
                "failed": failed,
                "in_flight": self._request_in_flight,
                "peak_in_flight": self._request_peak_in_flight,
                "configured_max_in_flight": self.config.max_concurrent_requests,
                "minimum_request_spacing_seconds": (
                    self.config.minimum_request_spacing_seconds
                ),
                "pacing_wait_seconds": self._request_pacing_wait_seconds,
                "slot_wait_seconds": self._request_slot_wait_seconds,
                "submitted_by_kind": dict(sorted(self._request_counts.items())),
                "completed_by_kind": dict(
                    sorted(self._request_completed.items())
                ),
                "failed_by_kind": dict(sorted(self._request_failed.items())),
                "request_seconds_by_kind": dict(
                    sorted(self._request_duration_seconds.items())
                ),
                "max_request_seconds_by_kind": dict(
                    sorted(self._request_max_duration_seconds.items())
                ),
            }

    def _finish_request(
        self,
        request_id: int,
        state: _RequestState,
        cancel: Any,
    ) -> list[dict[str, Any]]:
        if not state.event.wait(self.config.request_timeout_seconds):
            try:
                cancel(request_id)
            except Exception:
                pass
            raise IBKRRequestError(
                f"{state.kind} request {request_id} timed out after "
                f"{self.config.request_timeout_seconds:.1f}s"
            )
        if state.error:
            raw_code = state.error.get("code")
            error_code = int(raw_code) if isinstance(raw_code, int) else None
            raise IBKRRequestError(
                f"{state.kind} request {request_id} failed: {state.error}",
                error_code=error_code,
            )
        return list(state.rows)

    def fetch_contract_details(self, symbol: str) -> list[dict[str, Any]]:
        """Resolve a stock symbol without requesting price or account data."""
        kind = "contract-details"
        request_id, state = self._allocate_request(kind)
        started_at: float | None = None
        failed = True
        try:
            started_at = self._begin_provider_request(kind)
            self.reqContractDetails(request_id, self._contract(symbol))
            rows = self._finish_request(request_id, state, lambda _: None)
            failed = False
            return rows
        finally:
            with self._lock:
                self._requests.pop(request_id, None)
            if started_at is not None:
                self._end_provider_request(
                    kind, started_at=started_at, failed=failed
                )

    def _request_bars(
        self,
        symbol: str,
        end: datetime,
        duration: str,
        bar_size: str,
        what: str,
        use_rth: bool,
    ) -> list[dict[str, Any]]:
        kind = "historical-bars"
        request_id, state = self._allocate_request(kind)
        started_at: float | None = None
        failed = True
        try:
            started_at = self._begin_provider_request(kind)
            self.reqHistoricalData(
                request_id,
                self._contract(symbol),
                _format_ib_datetime(end),
                duration,
                bar_size,
                what,
                int(use_rth),
                2,
                False,
                [],
            )
            rows = self._finish_request(
                request_id, state, self.cancelHistoricalData
            )
            failed = False
            return rows
        finally:
            with self._lock:
                self._requests.pop(request_id, None)
            if started_at is not None:
                self._end_provider_request(
                    kind, started_at=started_at, failed=failed
                )

    def fetch_bars(
        self,
        symbol: str,
        start: str | datetime,
        end: str | datetime,
        *,
        bar_size: str = "1 min",
        what: str = "TRADES",
        use_rth: bool = True,
    ) -> list[dict[str, Any]]:
        start_utc = _coerce_datetime(start)
        end_utc = _coerce_datetime(end)
        if end_utc <= start_utc:
            raise IBKRConfigurationError("end must be after start")
        normalized_what = str(what).strip().upper()
        if normalized_what not in BAR_TYPES:
            raise IBKRConfigurationError(f"what must be one of {sorted(BAR_TYPES)}")
        chunk_days = HISTORICAL_BAR_CHUNK_DAYS.get(bar_size)
        if chunk_days is None:
            raise IBKRConfigurationError(f"unsupported bar size: {bar_size}")
        cursor = end_utc
        rows: list[dict[str, Any]] = []
        while cursor > start_utc:
            chunk_start = max(start_utc, cursor - timedelta(days=chunk_days))
            duration_days = max(
                1, math.ceil((cursor - chunk_start).total_seconds() / 86400)
            )
            rows.extend(
                self._request_bars(
                    symbol,
                    cursor,
                    f"{duration_days} D",
                    bar_size,
                    normalized_what,
                    use_rth,
                )
            )
            cursor = chunk_start
        unique: dict[int, dict[str, Any]] = {}
        for row in rows:
            epoch = int(row["epoch"])
            if int(start_utc.timestamp()) <= epoch < int(end_utc.timestamp()):
                unique[epoch] = row
        return [unique[key] for key in sorted(unique)]

    def _request_bid_ask_ticks(
        self, symbol: str, start: datetime, use_rth: bool
    ) -> list[dict[str, Any]]:
        kind = "historical-bid-ask-ticks"
        request_id, state = self._allocate_request(kind)
        started_at: float | None = None
        failed = True
        try:
            started_at = self._begin_provider_request(kind)
            self.reqHistoricalTicks(
                request_id,
                self._contract(symbol),
                _format_ib_datetime(start),
                "",
                1000,
                "BID_ASK",
                int(use_rth),
                False,
                [],
            )
            rows = self._finish_request(
                request_id,
                state,
                lambda _: None,
            )
            failed = False
            return rows
        finally:
            with self._lock:
                self._requests.pop(request_id, None)
            if started_at is not None:
                self._end_provider_request(
                    kind, started_at=started_at, failed=failed
                )

    def fetch_bid_ask_ticks(
        self,
        symbol: str,
        start: str | datetime,
        end: str | datetime,
        *,
        use_rth: bool = True,
    ) -> list[dict[str, Any]]:
        start_utc = _coerce_datetime(start)
        end_utc = _coerce_datetime(end)
        if end_utc <= start_utc:
            raise IBKRConfigurationError("end must be after start")
        cursor = start_utc
        rows: list[dict[str, Any]] = []
        while cursor < end_utc:
            page = self._request_bid_ask_ticks(symbol, cursor, use_rth)
            if not page:
                break
            rows.extend(page)
            last_epoch = max(int(row["epoch"]) for row in page)
            if last_epoch >= int(end_utc.timestamp()):
                break
            next_cursor = datetime.fromtimestamp(last_epoch + 1, UTC)
            if next_cursor <= cursor:
                break
            cursor = next_cursor
        unique: dict[tuple[Any, ...], dict[str, Any]] = {}
        for row in rows:
            epoch = int(row["epoch"])
            if int(start_utc.timestamp()) <= epoch <= int(end_utc.timestamp()):
                key = (
                    epoch,
                    row["bid"],
                    row["ask"],
                    row["bid_size"],
                    row["ask_size"],
                )
                unique[key] = row
        return sorted(unique.values(), key=lambda row: int(row["epoch"]))


class IBKRHistoricalClient:
    """Public historical-only facade over the internal asynchronous client."""

    provider_name = "Interactive Brokers TWS API"
    cache_namespace = "ibkr"

    def __init__(self, config: IBKRConfig):
        self.config = config
        self._connection = _IBKRHistoricalConnection(config)

    def __enter__(self) -> "IBKRHistoricalClient":
        self.connect_and_wait()
        return self

    def __exit__(self, exc_type, exc, traceback_obj) -> None:
        self.close()

    def connect_and_wait(self) -> None:
        self._connection.connect_and_wait()

    def close(self) -> None:
        self._connection.close()

    def fetch_bars(
        self,
        symbol: str,
        start: str | datetime,
        end: str | datetime,
        *,
        bar_size: str = "1 min",
        what: str = "TRADES",
        use_rth: bool = True,
    ) -> list[dict[str, Any]]:
        return self._connection.fetch_bars(
            symbol,
            start,
            end,
            bar_size=bar_size,
            what=what,
            use_rth=use_rth,
        )

    def fetch_contract_details(self, symbol: str) -> list[dict[str, Any]]:
        return self._connection.fetch_contract_details(symbol)

    def fetch_bid_ask_ticks(
        self,
        symbol: str,
        start: str | datetime,
        end: str | datetime,
        *,
        use_rth: bool = True,
    ) -> list[dict[str, Any]]:
        return self._connection.fetch_bid_ask_ticks(
            symbol,
            start,
            end,
            use_rth=use_rth,
        )

    def request_telemetry(self) -> dict[str, Any]:
        return self._connection.request_telemetry()

    @staticmethod
    def _bar_timestamp(value: Any) -> datetime | None:
        return _IBKRHistoricalConnection._bar_timestamp(value)


def probe_historical_symbol(
    client: IBKRHistoricalClient,
    symbol: str,
) -> dict[str, Any]:
    """Return availability-only evidence suitable for a pre-freeze universe gate.

    Error 200 is scoped to an unresolvable security definition and may safely
    skip one draft-pool symbol. Connection, permission, pacing, and other
    provider errors remain batch blockers and are deliberately re-raised.
    """
    normalized = str(symbol).strip().upper()
    try:
        details = client.fetch_contract_details(normalized)
    except IBKRRequestError as exc:
        if exc.error_code == 200:
            return {
                "symbol": normalized,
                "viable": False,
                "reason": "unresolvable_security_definition",
                "error_code": exc.error_code,
            }
        raise
    matching = [
        row
        for row in details
        if str(row.get("symbol", "")).upper() == normalized
        and row.get("security_type") == "STK"
        and row.get("currency") == "USD"
        and row.get("stock_type") == "COMMON"
    ]
    if not matching:
        return {
            "symbol": normalized,
            "viable": False,
            "reason": "no_matching_us_common_stock_contract",
            "error_code": None,
        }
    return {
        "symbol": normalized,
        "viable": True,
        "reason": "contract_resolved",
        "error_code": None,
        "stock_type": "COMMON",
    }


def probe_historical_candidate_with_history(
    client: IBKRHistoricalClient,
    symbol: str,
    session_date: str | date,
    *,
    minimum_average_daily_volume_14: float | None = None,
    minimum_daily_atr_14: float | None = None,
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    """Check only pre-session facts required by the frozen replay contract.

    This deliberately ends every market-data request before the target date. It
    may reject an unusable listing/history record, but cannot observe the target
    opening, breakout, quotes, or outcome. The returned reusable history is
    immutable pre-session input that later bundle collection can consume without
    repeating the same IBKR requests.
    """
    normalized = str(symbol).strip().upper()
    if not normalized or not normalized.replace(".", "").isalnum():
        raise IBKRConfigurationError(f"invalid equity symbol: {symbol!r}")
    day = _parse_date(session_date)
    target_start = datetime.combine(day, wall_time(0), tzinfo=EASTERN)

    try:
        daily_rows = client.fetch_bars(
            normalized,
            datetime.combine(day - timedelta(days=75), wall_time(0), tzinfo=EASTERN),
            target_start,
            bar_size="1 day",
            what="TRADES",
        )
    except IBKRRequestError as exc:
        if exc.error_code == 200:
            return (
                {
                    "symbol": normalized,
                    "viable": False,
                    "reason": "unresolvable_security_definition",
                    "error_code": exc.error_code,
                    "prior_opening_sessions": None,
                    "prior_daily_sessions": 0,
                },
                None,
            )
        if is_symbol_scoped_historical_no_data(exc):
            return (
                {
                    "symbol": normalized,
                    "viable": False,
                    "reason": "missing_prior_daily_history",
                    "error_code": exc.error_code,
                    "prior_opening_sessions": None,
                    "prior_daily_sessions": 0,
                },
                None,
            )
        raise
    prior_daily = [
        row
        for row in daily_rows
        if datetime.fromtimestamp(int(row["epoch"]), UTC).astimezone(EASTERN).date()
        < day
    ]
    if len(prior_daily) < 15:
        return (
            {
                "symbol": normalized,
                "viable": False,
                "reason": "insufficient_prior_daily_history",
                "error_code": None,
                "prior_opening_sessions": None,
                "prior_daily_sessions": len(prior_daily),
            },
            None,
        )

    daily_metrics: dict[str, float] = {}
    if minimum_average_daily_volume_14 is not None:
        try:
            daily_metrics["average_daily_volume_14"] = average_daily_volume(
                prior_daily, 14
            )
        except HistoricalMetricError as exc:
            raise IBKRRequestError(
                f"cannot calculate pre-session average daily volume: {exc}"
            ) from exc
        if daily_metrics["average_daily_volume_14"] < float(
            minimum_average_daily_volume_14
        ):
            return (
                {
                    "symbol": normalized,
                    "viable": False,
                    "reason": "average_daily_volume_below_strategy_minimum",
                    "error_code": None,
                    "prior_opening_sessions": None,
                    "prior_daily_sessions": len(prior_daily),
                    **daily_metrics,
                    "minimum_average_daily_volume_14": float(
                        minimum_average_daily_volume_14
                    ),
                },
                None,
            )
    if minimum_daily_atr_14 is not None:
        try:
            daily_metrics["daily_atr_14"] = average_true_range(prior_daily, 14)
        except HistoricalMetricError as exc:
            raise IBKRRequestError(
                f"cannot calculate pre-session daily ATR: {exc}"
            ) from exc
        if daily_metrics["daily_atr_14"] < float(minimum_daily_atr_14):
            return (
                {
                    "symbol": normalized,
                    "viable": False,
                    "reason": "daily_atr_below_strategy_minimum",
                    "error_code": None,
                    "prior_opening_sessions": None,
                    "prior_daily_sessions": len(prior_daily),
                    **daily_metrics,
                    "minimum_daily_atr_14": float(minimum_daily_atr_14),
                },
                None,
            )

    # Most buffered names fail immutable daily gates.  Resolve the explicit US
    # stock contract only for survivors instead of paying for a separate lookup
    # that cannot affect an already-rejected candidate.  The daily request above
    # already uses the same STK/USD/SMART contract shape and safely maps an IBKR
    # security-definition error to a symbol-scoped skip.
    contract = probe_historical_symbol(client, normalized)
    if contract.get("viable") is not True:
        return contract, None

    try:
        opening_rows = client.fetch_bars(
            normalized,
            datetime.combine(day - timedelta(days=28), wall_time(0), tzinfo=EASTERN),
            target_start,
            bar_size="5 mins",
            what="TRADES",
        )
    except IBKRRequestError as exc:
        if is_symbol_scoped_historical_no_data(exc):
            return (
                {
                    "symbol": normalized,
                    "viable": False,
                    "reason": "missing_prior_opening_history",
                    "error_code": exc.error_code,
                    "prior_opening_sessions": 0,
                    "prior_daily_sessions": len(prior_daily),
                    **daily_metrics,
                },
                None,
            )
        raise
    opening_by_day = {
        str(row["date_et"]): row
        for row in opening_rows
        if datetime.fromtimestamp(int(row["epoch"]), UTC).astimezone(EASTERN).time()
        == wall_time(9, 30)
        and str(row.get("date_et", "")) < day.isoformat()
    }
    prior_dates = sorted(opening_by_day)[-14:]
    if len(prior_dates) < 14:
        return (
            {
                "symbol": normalized,
                "viable": False,
                "reason": "insufficient_prior_opening_history",
                "error_code": None,
                "prior_opening_sessions": len(prior_dates),
                "prior_daily_sessions": len(prior_daily),
                **daily_metrics,
            },
            None,
        )
    if any(int(opening_by_day[key].get("volume", 0)) <= 0 for key in prior_dates):
        return (
            {
                "symbol": normalized,
                "viable": False,
                "reason": "nonpositive_prior_opening_volume",
                "error_code": None,
                "prior_opening_sessions": len(prior_dates),
                "prior_daily_sessions": len(prior_daily),
                **daily_metrics,
            },
            None,
        )
    result = {
        "symbol": normalized,
        "viable": True,
        "reason": "pre_session_history_available",
        "error_code": None,
        "prior_opening_sessions": len(prior_dates),
        "prior_daily_sessions": len(prior_daily),
        "target_session_prices_observed": False,
        **daily_metrics,
    }
    history = {
        "schema_version": PRE_SESSION_CACHE_VERSION,
        "symbol": normalized,
        "session_date": day.isoformat(),
        "target_session_prices_observed": False,
        "prior_opening_bars": [opening_by_day[key] for key in prior_dates],
        "daily_bars": prior_daily[-20:],
    }
    return result, history


def probe_historical_candidate(
    client: IBKRHistoricalClient,
    symbol: str,
    session_date: str | date,
) -> dict[str, Any]:
    """Return the public availability result without reusable raw history."""
    result, _ = probe_historical_candidate_with_history(client, symbol, session_date)
    return result


def _latest_completed_minute_volume(
    bars: Sequence[Mapping[str, Any]], snapshot_at: datetime
) -> int:
    candidates = [
        row for row in bars if int(row["epoch"]) + 60 <= int(snapshot_at.timestamp())
    ]
    if not candidates:
        return 0
    return int(max(candidates, key=lambda row: int(row["epoch"]))["volume"])


def select_quote_snapshots(
    ticks: Sequence[Mapping[str, Any]],
    evaluation_at: datetime,
    minute_bars: Sequence[Mapping[str, Any]],
    *,
    count: int = 3,
    span_seconds: int = 10,
) -> list[dict[str, Any]]:
    if evaluation_at.tzinfo is None:
        evaluation_at = evaluation_at.replace(tzinfo=EASTERN)
    evaluation_at = evaluation_at.astimezone(UTC)
    if count < 2:
        raise IBKRConfigurationError("quote snapshot count must be at least 2")
    interval = span_seconds / (count - 1)
    targets = [
        evaluation_at - timedelta(seconds=span_seconds - interval * index)
        for index in range(count)
    ]
    snapshots: list[dict[str, Any]] = []
    for target in targets:
        eligible = [
            row for row in ticks if int(row["epoch"]) <= int(target.timestamp())
        ]
        if not eligible:
            raise IBKRRequestError(
                f"no historical bid/ask tick exists at or before {target.isoformat()}"
            )
        tick = max(eligible, key=lambda row: int(row["epoch"]))
        observed = datetime.fromtimestamp(int(tick["epoch"]), UTC)
        age = (target - observed).total_seconds()
        snapshots.append(
            {
                "snapshot_time_et": target.astimezone(EASTERN).isoformat(),
                "observed_at_et": observed.astimezone(EASTERN).isoformat(),
                "age_seconds": age,
                "bid": float(tick["bid"]),
                "ask": float(tick["ask"]),
                "bid_depth": int(float(tick["bid_size"])),
                "ask_depth": int(float(tick["ask_size"])),
                "recent_real_1m_volume": _latest_completed_minute_volume(
                    minute_bars, target
                ),
                "depth_scope": "historical top-of-book size",
            }
        )
    return snapshots


def collect_quote_evidence(
    client: IBKRHistoricalClient,
    symbol: str,
    session_start: datetime,
    evaluation: datetime,
    minute_bars: Sequence[Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Collect fresh snapshots or preserve a same-session stale-quote reject.

    The normal request stays narrow. When no quote exists in that window, a
    single same-session fallback obtains the last observable quote state so the
    evaluator can reject it for age rather than losing the entire candidate.
    Absence of any regular-session quote remains a hard fidelity blocker.
    """
    quote_end = evaluation + timedelta(seconds=1)
    starts = (evaluation - timedelta(seconds=15), session_start)
    last_error: IBKRRequestError | None = None
    for quote_start in starts:
        ticks = client.fetch_bid_ask_ticks(symbol, quote_start, quote_end, use_rth=True)
        try:
            snapshots = select_quote_snapshots(
                ticks,
                evaluation,
                minute_bars,
                count=3,
                span_seconds=10,
            )
            return ticks, snapshots
        except IBKRRequestError as exc:
            last_error = exc
    if last_error is not None:
        raise last_error
    raise IBKRRequestError("no historical bid/ask quote evidence was returned")


def _opening_bar_from_minutes(
    minute_bars: Sequence[Mapping[str, Any]], day: date
) -> dict[str, Any]:
    opening_minutes = []
    for row in minute_bars:
        observed = datetime.fromtimestamp(int(row["epoch"]), UTC).astimezone(EASTERN)
        if observed.date() == day and wall_time(9, 30) <= observed.time() < wall_time(
            9, 35
        ):
            opening_minutes.append((observed, row))
    opening_minutes.sort(key=lambda value: value[0])
    if len(opening_minutes) != 5:
        raise IBKRRequestError(
            f"opening range needs five one-minute bars, got {len(opening_minutes)}"
        )
    first_time, first = opening_minutes[0]
    _, last = opening_minutes[-1]
    return {
        "epoch": int(first["epoch"]),
        "date_et": day.isoformat(),
        "time_et": first_time.isoformat(),
        "open": float(first["open"]),
        "high": max(float(row["high"]) for _, row in opening_minutes),
        "low": min(float(row["low"]) for _, row in opening_minutes),
        "close": float(last["close"]),
        "volume": sum(int(row["volume"]) for _, row in opening_minutes),
    }


def _validate_pre_session_history(
    value: Mapping[str, Any], symbol: str, day: date
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    normalized = symbol.upper()
    if value.get("schema_version") != PRE_SESSION_CACHE_VERSION:
        raise IBKRConfigurationError("pre-session cache version is unsupported")
    if value.get("symbol") != normalized or value.get("session_date") != day.isoformat():
        raise IBKRConfigurationError("pre-session cache identity does not match request")
    if value.get("target_session_prices_observed") is not False:
        raise IBKRConfigurationError("pre-session cache crossed the target boundary")
    opening = value.get("prior_opening_bars")
    daily = value.get("daily_bars")
    if not isinstance(opening, list) or len(opening) != 14:
        raise IBKRConfigurationError("pre-session cache needs 14 opening bars")
    if not isinstance(daily, list) or len(daily) < 15:
        raise IBKRConfigurationError("pre-session cache needs 15 daily bars")
    opening_rows = [dict(row) for row in opening if isinstance(row, Mapping)]
    daily_rows = [dict(row) for row in daily if isinstance(row, Mapping)]
    if len(opening_rows) != 14 or len(daily_rows) != len(daily):
        raise IBKRConfigurationError("pre-session cache bars must be objects")
    if any(int(row.get("volume", 0)) <= 0 for row in opening_rows):
        raise IBKRConfigurationError("pre-session cache opening volume must be positive")
    boundary = datetime.combine(day, wall_time(0), tzinfo=EASTERN).astimezone(UTC)
    if any(int(row["epoch"]) >= int(boundary.timestamp()) for row in opening_rows):
        raise IBKRConfigurationError("pre-session opening cache crossed target date")
    if any(int(row["epoch"]) >= int(boundary.timestamp()) for row in daily_rows):
        raise IBKRConfigurationError("pre-session daily cache crossed target date")
    return opening_rows, daily_rows


def collect_candidate_history(
    client: IBKRHistoricalClient,
    symbol: str,
    session_date: str | date,
    evaluation_time_et: str,
    *,
    session_bars: Sequence[Mapping[str, Any]] | None = None,
    pre_session_history: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    provider_name = str(getattr(client, "provider_name", "Interactive Brokers TWS API"))
    day = _parse_date(session_date)
    try:
        evaluation_clock = wall_time.fromisoformat(evaluation_time_et)
    except ValueError as exc:
        raise IBKRConfigurationError("evaluation_time_et must be HH:MM:SS") from exc
    evaluation = datetime.combine(day, evaluation_clock, tzinfo=EASTERN)
    if not wall_time(9, 35) <= evaluation_clock <= wall_time(10, 30):
        raise IBKRConfigurationError(
            "evaluation_time_et must be between 09:35:00 and 10:30:00 ET"
        )
    session_start = datetime.combine(day, wall_time(9, 30), tzinfo=EASTERN)
    session_end = datetime.combine(day, wall_time(16, 0), tzinfo=EASTERN)
    minute_bars = (
        list(session_bars)
        if session_bars is not None
        else client.fetch_bars(
            symbol, session_start, session_end, bar_size="1 min", what="TRADES"
        )
    )
    if pre_session_history is not None:
        prior_opening_bars, daily_bars = _validate_pre_session_history(
            pre_session_history, symbol, day
        )
        current_opening = _opening_bar_from_minutes(minute_bars, day)
    else:
        lookback_start = session_start - timedelta(days=45)
        opening_end = datetime.combine(day, wall_time(9, 35), tzinfo=EASTERN)
        opening_bars = client.fetch_bars(
            symbol,
            lookback_start,
            opening_end,
            bar_size="5 mins",
            what="TRADES",
        )
        opening_by_day = {
            row["date_et"]: row
            for row in opening_bars
            if datetime.fromtimestamp(int(row["epoch"]), UTC)
            .astimezone(EASTERN)
            .time()
            == wall_time(9, 30)
        }
        prior_dates = sorted(
            key for key in opening_by_day if key < day.isoformat()
        )[-14:]
        if len(prior_dates) != 14:
            raise IBKRRequestError(
                f"{provider_name} returned only {len(prior_dates)} prior 09:30 "
                "five-minute bars; 14 are required"
            )
        current_opening = opening_by_day.get(day.isoformat())
        if current_opening is None:
            raise IBKRRequestError(
                f"{provider_name} did not return the selected day's 09:30 opening bar"
            )
        prior_opening_bars = [opening_by_day[key] for key in prior_dates]
        daily_bars = client.fetch_bars(
            symbol,
            datetime.combine(day - timedelta(days=75), wall_time(0), tzinfo=EASTERN),
            datetime.combine(day, wall_time(0), tzinfo=EASTERN),
            bar_size="1 day",
            what="TRADES",
        )
        if len(daily_bars) < 15:
            raise IBKRRequestError(
                f"{provider_name} returned only {len(daily_bars)} prior daily bars; "
                "at least 15 are required"
            )
    bid_ask_ticks, snapshots = collect_quote_evidence(
        client,
        symbol,
        session_start,
        evaluation,
        minute_bars,
    )
    return {
        "schema_version": 1,
        "provider": provider_name,
        "provenance": {
            "session_bars": provider_name,
            "opening_volume_history": provider_name,
            "daily_bars": provider_name,
            "historical_quotes_and_depth": provider_name,
        },
        "captured_at": datetime.now(UTC).isoformat(),
        "request": {
            "symbol": symbol.upper(),
            "date": day.isoformat(),
            "evaluation_time_et": evaluation_clock.isoformat(),
            "regular_hours_only": True,
        },
        "session_bars": minute_bars,
        "session_bar_quality": {
            "expected_regular_session_minutes": 390,
            "actual_trade_minutes": len(minute_bars),
            "complete": len(minute_bars) == 390,
            "interpolated_bars": 0,
        },
        "opening_bar": current_opening,
        "prior_opening_bars": prior_opening_bars,
        "prior_opening_volumes": [
            int(row["volume"]) for row in prior_opening_bars
        ],
        "daily_bars": daily_bars[-20:],
        "bid_ask_ticks": bid_ask_ticks,
        "quote_snapshots": snapshots,
        "limitations": [
            "IBKR historical volume is filtered and may differ from an unfiltered consolidated feed.",
            "Historical bid/ask ticks expose top-of-book sizes, not the full historical depth ladder.",
            "Historical scanner-universe capture and point-in-time catalysts must come from separate sources.",
        ],
    }


def _write_json(value: Any, output: Path | None) -> None:
    rendered = json.dumps(value, indent=2, sort_keys=True) + "\n"
    if output is None:
        print(rendered, end="")
        return
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(rendered, encoding="utf-8")
    print(json.dumps({"written": str(output), "bytes": len(rendered.encode())}))


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", type=Path, default=DEFAULT_ENV_PATH)
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser(
        "check", help="connect and confirm a read-only TWS API handshake"
    )

    probe = subparsers.add_parser(
        "probe",
        help="resolve a stock and optionally verify pre-session history",
    )
    probe.add_argument("symbol")
    probe.add_argument(
        "--date",
        help="target date; checks required prior history without target-session prices",
    )

    bars = subparsers.add_parser("bars", help="fetch historical OHLCV bars")
    bars.add_argument("symbol")
    bars.add_argument(
        "--start", required=True, help="ISO datetime; naive values are ET"
    )
    bars.add_argument("--end", required=True, help="ISO datetime; naive values are ET")
    bars.add_argument("--bar-size", default="1 min")
    bars.add_argument("--what", choices=sorted(BAR_TYPES), default="TRADES")
    bars.add_argument("--all-hours", action="store_true")
    bars.add_argument("--output", type=Path)

    quotes = subparsers.add_parser(
        "quotes", help="fetch historical top-of-book bid/ask ticks with sizes"
    )
    quotes.add_argument("symbol")
    quotes.add_argument(
        "--start", required=True, help="ISO datetime; naive values are ET"
    )
    quotes.add_argument(
        "--end", required=True, help="ISO datetime; naive values are ET"
    )
    quotes.add_argument("--all-hours", action="store_true")
    quotes.add_argument("--output", type=Path)

    candidate = subparsers.add_parser(
        "candidate",
        help="collect one replay candidate's session, lookback, and quote inputs",
    )
    candidate.add_argument("symbol")
    candidate.add_argument("--date", required=True)
    candidate.add_argument("--evaluation-time", required=True)
    candidate.add_argument("--output", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        config = IBKRConfig.from_env(args.env_file)
        with IBKRHistoricalClient(config) as client:
            if args.command == "check":
                result: Any = {
                    "connected": True,
                    "read_only_scope": "historical market data only",
                    "config": config.public_dict(),
                }
            elif args.command == "probe":
                result = (
                    probe_historical_candidate(client, args.symbol, args.date)
                    if args.date
                    else probe_historical_symbol(client, args.symbol)
                )
            elif args.command == "bars":
                result = {
                    "provider": "Interactive Brokers TWS API",
                    "symbol": args.symbol.upper(),
                    "bar_size": args.bar_size,
                    "what": args.what,
                    "bars": client.fetch_bars(
                        args.symbol,
                        args.start,
                        args.end,
                        bar_size=args.bar_size,
                        what=args.what,
                        use_rth=not args.all_hours,
                    ),
                }
            elif args.command == "quotes":
                result = {
                    "provider": "Interactive Brokers TWS API",
                    "symbol": args.symbol.upper(),
                    "ticks": client.fetch_bid_ask_ticks(
                        args.symbol,
                        args.start,
                        args.end,
                        use_rth=not args.all_hours,
                    ),
                }
            else:
                result = collect_candidate_history(
                    client,
                    args.symbol,
                    args.date,
                    args.evaluation_time,
                )
        _write_json(result, getattr(args, "output", None))
        return 0
    except (IBKRHistoricalError, OSError, ValueError) as exc:
        print(
            json.dumps(
                {"error": str(exc), "error_type": type(exc).__name__},
                indent=2,
            )
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

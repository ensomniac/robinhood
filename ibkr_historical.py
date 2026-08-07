"""Read-only Interactive Brokers TWS historical-market-data adapter.

This module intentionally exposes no account, portfolio, order, or execution
methods.  It connects to an already authenticated Trader Workstation (or IB
Gateway) socket and fetches historical bars and historical bid/ask ticks for
canonical data workflows.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import socket
import subprocess
import threading
import time
from collections import Counter
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Mapping, Sequence
from zoneinfo import ZoneInfo

from dotenv import dotenv_values
from ibapi.client import EClient
from ibapi.contract import Contract
from ibapi.wrapper import EWrapper

from historical_store import HistoricalDayStore, HistoricalStoreError


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_ENV_PATH = PROJECT_ROOT / ".env"
DEFAULT_CONTRACT_CACHE_ROOT = PROJECT_ROOT / "historical_data" / "contracts"
EASTERN = ZoneInfo("America/New_York")
UTC = timezone.utc
BLOCKED_TWS_METHODS = frozenset(
    {
        "cancelAccountSummary",
        "cancelAccountUpdatesMulti",
        "cancelMktData",
        "cancelPnL",
        "cancelPnLSingle",
        "cancelPositions",
        "cancelPositionsMulti",
        "cancelRealTimeBars",
        "cancelScannerSubscription",
        "cancelTickByTickData",
        "cancelOrder",
        "exerciseOptions",
        "placeOrder",
        "replaceFA",
        "reqAccountSummary",
        "reqAccountUpdates",
        "reqAccountUpdatesMulti",
        "reqAllOpenOrders",
        "reqAutoOpenOrders",
        "reqCompletedOrders",
        "reqExecutions",
        "reqGlobalCancel",
        "reqMktData",
        "reqOpenOrders",
        "reqOrderBound",
        "reqPnL",
        "reqPnLSingle",
        "reqPositions",
        "reqPositionsMulti",
        "reqRealTimeBars",
        "reqScannerSubscription",
        "reqTickByTickData",
        "requestFA",
    }
)
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
CONTRACT_CACHE_SCHEMA_VERSION = 1
CONTRACT_CACHE_RESOLVED_TTL_SECONDS = 30 * 24 * 60 * 60
CONTRACT_CACHE_UNRESOLVABLE_TTL_SECONDS = 24 * 60 * 60
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


def _canonical_sha256(value: Any) -> str:
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


class ContractDetailsCache:
    """Integrity-check expiring symbol proofs shared across replay dates.

    Contract details are current provider metadata, not immutable historical
    prices. Positive results therefore expire after 30 days by default, while
    an unresolvable-symbol result expires after one day. Provider, permission,
    pacing, and transport errors are never cached.
    """

    def __init__(
        self,
        root: Path,
        *,
        exchange: str = "SMART",
        currency: str = "USD",
        primary_exchange: str = "",
        resolved_ttl_seconds: float = CONTRACT_CACHE_RESOLVED_TTL_SECONDS,
        unresolvable_ttl_seconds: float = CONTRACT_CACHE_UNRESOLVABLE_TTL_SECONDS,
    ):
        if resolved_ttl_seconds <= 0 or unresolvable_ttl_seconds <= 0:
            raise IBKRConfigurationError("contract cache TTLs must be positive")
        self.root = Path(root)
        self.request_identity = {
            "security_type": "STK",
            "exchange": str(exchange).strip().upper(),
            "currency": str(currency).strip().upper(),
            "primary_exchange": str(primary_exchange).strip().upper(),
        }
        if (
            not self.request_identity["exchange"]
            or not self.request_identity["currency"]
        ):
            raise IBKRConfigurationError(
                "contract cache exchange and currency cannot be empty"
            )
        self.namespace = _canonical_sha256(self.request_identity)[:12]
        self.resolved_ttl_seconds = float(resolved_ttl_seconds)
        self.unresolvable_ttl_seconds = float(unresolvable_ttl_seconds)
        self._lock = threading.Lock()
        self._hits = 0
        self._misses = 0
        self._invalid = 0
        self._expired = 0
        self._writes = 0

    @staticmethod
    def _symbol(value: str) -> str:
        normalized = str(value).strip().upper()
        if not normalized or not normalized.replace(".", "").isalnum():
            raise IBKRConfigurationError(f"invalid equity symbol: {value!r}")
        return normalized

    def _path(self, symbol: str) -> Path:
        return self.root / self.namespace / f"{self._symbol(symbol)}.json"

    def _request(self, symbol: str) -> dict[str, str]:
        return {"symbol": self._symbol(symbol), **self.request_identity}

    def load(self, symbol: str) -> dict[str, Any] | None:
        normalized = self._symbol(symbol)
        path = self._path(normalized)
        if not path.is_file():
            with self._lock:
                self._misses += 1
            return None
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            content_hash = payload.pop("content_sha256")
            request = payload["request"]
            outcome = payload["outcome"]
            expires_at = datetime.fromisoformat(payload["expires_at"])
            valid = (
                payload.get("schema_version") == CONTRACT_CACHE_SCHEMA_VERSION
                and payload.get("provider") == "Interactive Brokers TWS API"
                and request == self._request(normalized)
                and isinstance(outcome, dict)
                and outcome.get("status") in {"resolved", "unresolvable"}
                and isinstance(content_hash, str)
                and content_hash == _canonical_sha256(payload)
                and expires_at.tzinfo is not None
            )
            if outcome.get("status") == "resolved":
                details = outcome.get("details")
                valid = (
                    valid
                    and isinstance(details, list)
                    and all(isinstance(row, dict) for row in details)
                )
            else:
                valid = valid and outcome.get("error_code") == 200
        except (KeyError, OSError, TypeError, ValueError, json.JSONDecodeError):
            valid = False
            expires_at = datetime.min.replace(tzinfo=UTC)
        if not valid:
            with self._lock:
                self._invalid += 1
                self._misses += 1
            return None
        if expires_at.astimezone(UTC) <= datetime.now(UTC):
            with self._lock:
                self._expired += 1
                self._misses += 1
            return None
        with self._lock:
            self._hits += 1
        return dict(outcome)

    def _store(
        self, symbol: str, outcome: Mapping[str, Any], ttl_seconds: float
    ) -> None:
        normalized = self._symbol(symbol)
        captured_at = datetime.now(UTC)
        payload: dict[str, Any] = {
            "schema_version": CONTRACT_CACHE_SCHEMA_VERSION,
            "provider": "Interactive Brokers TWS API",
            "captured_at": captured_at.isoformat(),
            "expires_at": (captured_at + timedelta(seconds=ttl_seconds)).isoformat(),
            "privacy_class": "public_market_metadata",
            "request": self._request(normalized),
            "outcome": dict(outcome),
        }
        payload["content_sha256"] = _canonical_sha256(payload)
        rendered = json.dumps(payload, indent=2, sort_keys=True) + "\n"
        path = self._path(normalized)
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_suffix(".json.tmp")
        temporary.write_text(rendered, encoding="utf-8")
        temporary.replace(path)
        with self._lock:
            self._writes += 1

    def store_details(self, symbol: str, details: Sequence[Mapping[str, Any]]) -> None:
        self._store(
            symbol,
            {"status": "resolved", "details": [dict(row) for row in details]},
            self.resolved_ttl_seconds,
        )

    def store_unresolvable(self, symbol: str) -> None:
        self._store(
            symbol,
            {"status": "unresolvable", "error_code": 200},
            self.unresolvable_ttl_seconds,
        )

    def stats(self) -> dict[str, Any]:
        with self._lock:
            return {
                "namespace": self.namespace,
                "hits": self._hits,
                "misses": self._misses,
                "invalid": self._invalid,
                "expired": self._expired,
                "writes": self._writes,
                "resolved_ttl_seconds": self.resolved_ttl_seconds,
                "unresolvable_ttl_seconds": self.unresolvable_ttl_seconds,
            }


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
    def __getattribute__(self, name: str) -> Any:
        if name in BLOCKED_TWS_METHODS:
            raise IBKRConfigurationError(
                f"{name} is disabled in the historical-data-only adapter"
            )
        return super().__getattribute__(name)

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
        self._request_slots = threading.BoundedSemaphore(config.max_concurrent_requests)
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
                self._last_request_at + self.config.minimum_request_spacing_seconds,
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
                "completed_by_kind": dict(sorted(self._request_completed.items())),
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
                self._end_provider_request(kind, started_at=started_at, failed=failed)

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
            rows = self._finish_request(request_id, state, self.cancelHistoricalData)
            failed = False
            return rows
        finally:
            with self._lock:
                self._requests.pop(request_id, None)
            if started_at is not None:
                self._end_provider_request(kind, started_at=started_at, failed=failed)

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
                self._end_provider_request(kind, started_at=started_at, failed=failed)

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

    def __init__(
        self,
        config: IBKRConfig,
        *,
        contract_cache_root: Path | None = DEFAULT_CONTRACT_CACHE_ROOT,
        refresh_contract_details: bool = False,
    ):
        self.config = config
        self._connection = _IBKRHistoricalConnection(config)
        self._contract_cache = (
            ContractDetailsCache(
                contract_cache_root,
                exchange=config.exchange,
                currency=config.currency,
                primary_exchange=config.primary_exchange,
            )
            if contract_cache_root is not None
            else None
        )
        self._refresh_contract_details = bool(refresh_contract_details)
        self._contract_lock_guard = threading.Lock()
        self._contract_locks: dict[str, threading.Lock] = {}
        self._contract_memory: dict[str, dict[str, Any]] = {}

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
        normalized = ContractDetailsCache._symbol(symbol)

        def cached(*, allow_disk: bool = True) -> list[dict[str, Any]] | None:
            with self._contract_lock_guard:
                outcome = self._contract_memory.get(normalized)
            if outcome is None:
                if (
                    not allow_disk
                    or self._contract_cache is None
                    or self._refresh_contract_details
                ):
                    return None
                outcome = self._contract_cache.load(normalized)
                if outcome is None:
                    return None
                with self._contract_lock_guard:
                    self._contract_memory[normalized] = outcome
            if outcome["status"] == "unresolvable":
                raise IBKRRequestError(
                    f"cached contract-details request for {normalized} is unresolvable",
                    error_code=200,
                )
            return [dict(row) for row in outcome["details"]]

        result = cached()
        if result is not None:
            return result
        with self._contract_lock_guard:
            symbol_lock = self._contract_locks.setdefault(normalized, threading.Lock())
        with symbol_lock:
            result = cached(allow_disk=False)
            if result is not None:
                return result
            try:
                details = self._connection.fetch_contract_details(normalized)
            except IBKRRequestError as exc:
                if exc.error_code == 200 and self._contract_cache is not None:
                    self._contract_cache.store_unresolvable(normalized)
                if exc.error_code == 200:
                    with self._contract_lock_guard:
                        self._contract_memory[normalized] = {
                            "status": "unresolvable",
                            "error_code": 200,
                        }
                raise
            if self._contract_cache is not None:
                self._contract_cache.store_details(normalized, details)
            with self._contract_lock_guard:
                self._contract_memory[normalized] = {
                    "status": "resolved",
                    "details": [dict(row) for row in details],
                }
            return details

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
        telemetry = self._connection.request_telemetry()
        telemetry["contract_cache"] = (
            self._contract_cache.stats()
            if self._contract_cache is not None
            else {"enabled": False}
        )
        return telemetry

    @staticmethod
    def _bar_timestamp(value: Any) -> datetime | None:
        return _IBKRHistoricalConnection._bar_timestamp(value)


def probe_historical_symbol(
    client: IBKRHistoricalClient,
    symbol: str,
) -> dict[str, Any]:
    """Return read-only contract availability for one historical-data symbol.

    Error 200 is scoped to an unresolvable security definition. Connection,
    permission, pacing, and other provider errors are deliberately re-raised.
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
    parser.add_argument(
        "--contract-cache-root",
        type=Path,
        default=DEFAULT_CONTRACT_CACHE_ROOT,
        help="expiring integrity-checked symbol proof cache",
    )
    parser.add_argument(
        "--fresh-contracts",
        action="store_true",
        help="bypass cached contract details and refresh provider truth",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser(
        "check", help="connect and confirm a read-only TWS API handshake"
    )

    probe = subparsers.add_parser(
        "probe",
        help="resolve one stock contract without requesting prices",
    )
    probe.add_argument("symbol")

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

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        config = IBKRConfig.from_env(args.env_file)
        store = HistoricalDayStore.from_env(args.env_file)
        with IBKRHistoricalClient(
            config,
            contract_cache_root=args.contract_cache_root,
            refresh_contract_details=args.fresh_contracts,
        ) as raw_client:
            # Imported here to avoid a module cycle: the provider orchestrator
            # also depends on this low-level read-only adapter.
            from historical_service import RecordingHistoricalClient

            client = RecordingHistoricalClient(raw_client, store)
            if args.command == "check":
                result: Any = {
                    "connected": True,
                    "read_only_scope": "historical market data only",
                    "config": config.public_dict(),
                }
            elif args.command == "probe":
                result = probe_historical_symbol(client, args.symbol)
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
            else:
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
        _write_json(result, getattr(args, "output", None))
        return 0
    except (HistoricalStoreError, IBKRHistoricalError, OSError, ValueError) as exc:
        print(
            json.dumps(
                {"error": str(exc), "error_type": type(exc).__name__},
                indent=2,
            )
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

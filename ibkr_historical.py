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


class IBKRHistoricalError(RuntimeError):
    """Base error for safe, read-only IBKR historical collection."""


class IBKRConfigurationError(IBKRHistoricalError):
    """Raised when local TWS configuration is unavailable or invalid."""


class IBKRRequestError(IBKRHistoricalError):
    """Raised when TWS rejects or times out a historical-data request."""


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
            primary_exchange=str(
                values.get("IBKR_PRIMARY_EXCHANGE") or ""
            ).strip().upper(),
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
            raise IBKRConfigurationError(
                f"invalid ISO datetime: {value!r}"
            ) from exc
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
                    "bid_past_low": bool(
                        getattr(attributes, "bidPastLow", False)
                    ),
                    "ask_past_high": bool(
                        getattr(attributes, "askPastHigh", False)
                    ),
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

    def _pacing_wait(self) -> None:
        with self._lock:
            delay = max(
                0.0,
                self.config.minimum_request_spacing_seconds
                - (time.monotonic() - self._last_request_at),
            )
        if delay:
            time.sleep(delay)
        with self._lock:
            self._last_request_at = time.monotonic()

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
            raise IBKRRequestError(
                f"{state.kind} request {request_id} failed: {state.error}"
            )
        return list(state.rows)

    def _request_bars(
        self,
        symbol: str,
        end: datetime,
        duration: str,
        bar_size: str,
        what: str,
        use_rth: bool,
    ) -> list[dict[str, Any]]:
        self._pacing_wait()
        request_id, state = self._allocate_request("historical-bars")
        try:
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
            return self._finish_request(
                request_id, state, self.cancelHistoricalData
            )
        finally:
            with self._lock:
                self._requests.pop(request_id, None)

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
            raise IBKRConfigurationError(
                f"what must be one of {sorted(BAR_TYPES)}"
            )
        chunk_days = {
            "1 sec": 1,
            "5 secs": 1,
            "15 secs": 1,
            "30 secs": 1,
            "1 min": 2,
            "2 mins": 3,
            "3 mins": 5,
            "5 mins": 7,
            "10 mins": 14,
            "15 mins": 21,
            "30 mins": 30,
            "1 hour": 90,
            "1 day": 365,
        }.get(bar_size)
        if chunk_days is None:
            raise IBKRConfigurationError(f"unsupported bar size: {bar_size}")
        cursor = end_utc
        rows: list[dict[str, Any]] = []
        while cursor > start_utc:
            chunk_start = max(start_utc, cursor - timedelta(days=chunk_days))
            duration_days = max(1, math.ceil((cursor - chunk_start).total_seconds() / 86400))
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
        self._pacing_wait()
        request_id, state = self._allocate_request("historical-bid-ask-ticks")
        try:
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
            return self._finish_request(
                request_id,
                state,
                lambda _: None,
            )
        finally:
            with self._lock:
                self._requests.pop(request_id, None)

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

    @staticmethod
    def _bar_timestamp(value: Any) -> datetime | None:
        return _IBKRHistoricalConnection._bar_timestamp(value)


def _latest_completed_minute_volume(
    bars: Sequence[Mapping[str, Any]], snapshot_at: datetime
) -> int:
    candidates = [
        row
        for row in bars
        if int(row["epoch"]) + 60 <= int(snapshot_at.timestamp())
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
        eligible = [row for row in ticks if int(row["epoch"]) <= int(target.timestamp())]
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


def collect_candidate_history(
    client: IBKRHistoricalClient,
    symbol: str,
    session_date: str | date,
    evaluation_time_et: str,
) -> dict[str, Any]:
    day = _parse_date(session_date)
    try:
        evaluation_clock = wall_time.fromisoformat(evaluation_time_et)
    except ValueError as exc:
        raise IBKRConfigurationError(
            "evaluation_time_et must be HH:MM:SS"
        ) from exc
    evaluation = datetime.combine(day, evaluation_clock, tzinfo=EASTERN)
    if not wall_time(9, 35) <= evaluation_clock <= wall_time(10, 30):
        raise IBKRConfigurationError(
            "evaluation_time_et must be between 09:35:00 and 10:30:00 ET"
        )
    session_start = datetime.combine(day, wall_time(9, 30), tzinfo=EASTERN)
    session_end = datetime.combine(day, wall_time(16, 0), tzinfo=EASTERN)
    minute_bars = client.fetch_bars(
        symbol, session_start, session_end, bar_size="1 min", what="TRADES"
    )
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
    prior_dates = sorted(key for key in opening_by_day if key < day.isoformat())[-14:]
    if len(prior_dates) != 14:
        raise IBKRRequestError(
            f"IBKR returned only {len(prior_dates)} prior 09:30 five-minute bars; 14 are required"
        )
    current_opening = opening_by_day.get(day.isoformat())
    if current_opening is None:
        raise IBKRRequestError("IBKR did not return the selected day's 09:30 opening bar")
    daily_bars = client.fetch_bars(
        symbol,
        datetime.combine(day - timedelta(days=75), wall_time(0), tzinfo=EASTERN),
        datetime.combine(day, wall_time(0), tzinfo=EASTERN),
        bar_size="1 day",
        what="TRADES",
    )
    if len(daily_bars) < 15:
        raise IBKRRequestError(
            f"IBKR returned only {len(daily_bars)} prior daily bars; at least 15 are required"
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
        "provider": "Interactive Brokers TWS API",
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
        "prior_opening_bars": [opening_by_day[key] for key in prior_dates],
        "prior_opening_volumes": [
            int(opening_by_day[key]["volume"]) for key in prior_dates
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
    subparsers.add_parser("check", help="connect and confirm a read-only TWS API handshake")

    bars = subparsers.add_parser("bars", help="fetch historical OHLCV bars")
    bars.add_argument("symbol")
    bars.add_argument("--start", required=True, help="ISO datetime; naive values are ET")
    bars.add_argument("--end", required=True, help="ISO datetime; naive values are ET")
    bars.add_argument("--bar-size", default="1 min")
    bars.add_argument("--what", choices=sorted(BAR_TYPES), default="TRADES")
    bars.add_argument("--all-hours", action="store_true")
    bars.add_argument("--output", type=Path)

    quotes = subparsers.add_parser(
        "quotes", help="fetch historical top-of-book bid/ask ticks with sizes"
    )
    quotes.add_argument("symbol")
    quotes.add_argument("--start", required=True, help="ISO datetime; naive values are ET")
    quotes.add_argument("--end", required=True, help="ISO datetime; naive values are ET")
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

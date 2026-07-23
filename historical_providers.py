"""Provider-neutral historical market-data clients for replay collection."""

from __future__ import annotations

import os
import time as time_module
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping, Protocol, Sequence, runtime_checkable
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

import requests
from dotenv import dotenv_values


EASTERN = ZoneInfo("America/New_York")
UTC = timezone.utc
DEFAULT_BASE_URL = "https://api.massive.com"
DEFAULT_ALPACA_DATA_URL = "https://data.alpaca.markets"


class HistoricalProviderError(RuntimeError):
    """A sanitized provider failure with an explicit retry classification."""

    def __init__(
        self,
        message: str,
        *,
        category: str,
        retry_after_seconds: float | None = None,
    ):
        super().__init__(message)
        self.category = category
        self.retryable = category.startswith("retryable_")
        self.retry_after_seconds = retry_after_seconds


def _retry_after_seconds(response: requests.Response) -> float | None:
    headers = getattr(response, "headers", None)
    raw = headers.get("Retry-After") if headers is not None else None
    if raw is None:
        return None
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return None
    return max(value, 0.0)


@runtime_checkable
class HistoricalMarketDataClient(Protocol):
    provider_name: str
    cache_namespace: str

    def fetch_bars(
        self,
        symbol: str,
        start: str | datetime,
        end: str | datetime,
        *,
        bar_size: str = "1 min",
        what: str = "TRADES",
        use_rth: bool = True,
    ) -> list[dict[str, Any]]: ...

    def fetch_bid_ask_ticks(
        self,
        symbol: str,
        start: str | datetime,
        end: str | datetime,
        *,
        use_rth: bool = True,
    ) -> list[dict[str, Any]]: ...


def _coerce_datetime(value: str | datetime) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=EASTERN)
    return parsed.astimezone(UTC)


@dataclass(frozen=True)
class MassiveConfig:
    api_key: str
    base_url: str = DEFAULT_BASE_URL
    timeout_seconds: float = 30.0

    @classmethod
    def optional_from_env(cls, path: Path) -> "MassiveConfig | None":
        values: dict[str, Any] = {}
        if path.exists():
            values.update(dotenv_values(path, interpolate=False))
        for key in (
            "MASSIVE_API_KEY",
            "MASSIVE_BASE_URL",
            "MASSIVE_TIMEOUT_SECONDS",
        ):
            if key in os.environ:
                values[key] = os.environ[key]
        api_key = str(values.get("MASSIVE_API_KEY") or "").strip()
        if not api_key:
            return None
        base_url = str(values.get("MASSIVE_BASE_URL") or DEFAULT_BASE_URL).rstrip("/")
        parsed = urlparse(base_url)
        if parsed.scheme != "https" or not parsed.netloc:
            raise HistoricalProviderError(
                "MASSIVE_BASE_URL must be an HTTPS origin",
                category="local_configuration",
            )
        try:
            timeout = float(values.get("MASSIVE_TIMEOUT_SECONDS") or 30.0)
        except (TypeError, ValueError) as exc:
            raise HistoricalProviderError(
                "MASSIVE_TIMEOUT_SECONDS must be numeric",
                category="local_configuration",
            ) from exc
        if timeout <= 0:
            raise HistoricalProviderError(
                "MASSIVE_TIMEOUT_SECONDS must be positive",
                category="local_configuration",
            )
        return cls(api_key=api_key, base_url=base_url, timeout_seconds=timeout)

    def public_dict(self) -> dict[str, Any]:
        return {
            "base_url": self.base_url,
            "timeout_seconds": self.timeout_seconds,
            "api_key_configured": bool(self.api_key),
        }


class MassiveHistoricalClient:
    """Read-only Massive SIP adapter normalized to the replay client contract."""

    provider_name = "Massive SIP REST API"
    cache_namespace = "massive"

    def __init__(
        self,
        config: MassiveConfig,
        *,
        session: requests.Session | None = None,
    ):
        self.config = config
        self._session = session or requests.Session()
        self._owns_session = session is None

    def __enter__(self) -> "MassiveHistoricalClient":
        return self

    def __exit__(self, exc_type, exc, traceback_obj) -> None:
        self.close()

    def close(self) -> None:
        if self._owns_session:
            self._session.close()

    def _request_pages(
        self,
        path: str,
        *,
        params: Mapping[str, Any],
    ) -> list[dict[str, Any]]:
        origin = urlparse(self.config.base_url).netloc
        url = f"{self.config.base_url}{path}"
        next_params: dict[str, Any] | None = dict(params)
        rows: list[dict[str, Any]] = []
        pages = 0
        while url:
            pages += 1
            if pages > 500:
                raise HistoricalProviderError(
                    "Massive pagination exceeded 500 pages",
                    category="permanent_fidelity",
                )
            request_params = dict(next_params or {})
            request_params["apiKey"] = self.config.api_key
            try:
                response = self._session.get(
                    url,
                    params=request_params,
                    timeout=self.config.timeout_seconds,
                )
            except (requests.Timeout, requests.ConnectionError) as exc:
                raise HistoricalProviderError(
                    "Massive request failed due to a transport error",
                    category="retryable_transport",
                ) from exc
            except requests.RequestException as exc:
                raise HistoricalProviderError(
                    "Massive request failed before a response was received",
                    category="retryable_provider",
                ) from exc
            if response.status_code == 429 or response.status_code >= 500:
                raise HistoricalProviderError(
                    f"Massive HTTP {response.status_code}",
                    category="retryable_provider",
                    retry_after_seconds=_retry_after_seconds(response),
                )
            if response.status_code >= 400:
                category = (
                    "permanent_permission"
                    if response.status_code in (401, 403)
                    else "permanent_fidelity"
                )
                raise HistoricalProviderError(
                    f"Massive HTTP {response.status_code}", category=category
                )
            try:
                payload = response.json()
            except ValueError as exc:
                raise HistoricalProviderError(
                    "Massive returned invalid JSON",
                    category="retryable_provider",
                ) from exc
            if not isinstance(payload, Mapping):
                raise HistoricalProviderError(
                    "Massive response must be an object",
                    category="permanent_fidelity",
                )
            status = str(payload.get("status", "")).upper()
            if status not in ("OK", "DELAYED"):
                raise HistoricalProviderError(
                    f"Massive response status was {status or 'missing'}",
                    category="permanent_fidelity",
                )
            page_rows = payload.get("results", [])
            if not isinstance(page_rows, list):
                raise HistoricalProviderError(
                    "Massive results must be an array",
                    category="permanent_fidelity",
                )
            rows.extend(dict(row) for row in page_rows if isinstance(row, Mapping))
            next_url = payload.get("next_url")
            if not next_url:
                break
            parsed_next = urlparse(str(next_url))
            if parsed_next.scheme != "https" or parsed_next.netloc != origin:
                raise HistoricalProviderError(
                    "Massive returned an unsafe pagination URL",
                    category="permanent_fidelity",
                )
            url = str(next_url)
            next_params = None
        return rows

    def _minute_aggregates(
        self, symbol: str, start: datetime, end: datetime
    ) -> list[dict[str, Any]]:
        start_et = start.astimezone(EASTERN)
        end_et = end.astimezone(EASTERN)
        inclusive_end = end_et
        if end_et.time() == time(0, 0):
            inclusive_end = end_et.replace(microsecond=0) - timedelta(microseconds=1)
        path = (
            f"/v2/aggs/ticker/{symbol.upper()}/range/1/minute/"
            f"{start_et.date().isoformat()}/{inclusive_end.date().isoformat()}"
        )
        raw = self._request_pages(
            path,
            params={"adjusted": "true", "sort": "asc", "limit": 50000},
        )
        normalized: list[dict[str, Any]] = []
        for row in raw:
            try:
                observed = datetime.fromtimestamp(float(row["t"]) / 1000, UTC)
                epoch = int(observed.timestamp())
                if not int(start.timestamp()) <= epoch < int(end.timestamp()):
                    continue
                eastern = observed.astimezone(EASTERN)
                if not time(9, 30) <= eastern.time() < time(16, 0):
                    continue
                normalized.append(
                    {
                        "epoch": epoch,
                        "time_et": eastern.isoformat(),
                        "date_et": eastern.date().isoformat(),
                        "open": float(row["o"]),
                        "high": float(row["h"]),
                        "low": float(row["l"]),
                        "close": float(row["c"]),
                        "volume": int(float(row["v"])),
                        "count": int(row.get("n") or 0),
                        "wap": float(row.get("vw") or 0),
                        "interpolated": False,
                    }
                )
            except (KeyError, TypeError, ValueError) as exc:
                raise HistoricalProviderError(
                    "Massive aggregate row is malformed",
                    category="permanent_fidelity",
                ) from exc
        return sorted(normalized, key=lambda row: int(row["epoch"]))

    def fetch_grouped_daily(
        self, session_date: str, *, adjusted: bool = True
    ) -> list[dict[str, Any]]:
        """Return the exhausted U.S. stock daily cross-section."""

        try:
            requested = datetime.fromisoformat(session_date).date()
        except ValueError as exc:
            raise HistoricalProviderError(
                "grouped daily date must be ISO formatted",
                category="local_configuration",
            ) from exc
        raw = self._request_pages(
            f"/v2/aggs/grouped/locale/us/market/stocks/{requested.isoformat()}",
            params={
                "adjusted": str(bool(adjusted)).lower(),
                "include_otc": "false",
            },
        )
        rows: list[dict[str, Any]] = []
        symbols: set[str] = set()
        for row in raw:
            try:
                symbol = str(row["T"]).strip().upper()
                observed = datetime.fromtimestamp(float(row["t"]) / 1000, UTC)
                values = {
                    "symbol": symbol,
                    "date": observed.astimezone(EASTERN).date().isoformat(),
                    "open": float(row["o"]),
                    "high": float(row["h"]),
                    "low": float(row["l"]),
                    "close": float(row["c"]),
                    "volume": int(float(row["v"])),
                    "count": int(row.get("n") or 0),
                    "wap": float(row.get("vw") or 0),
                }
            except (KeyError, TypeError, ValueError) as exc:
                raise HistoricalProviderError(
                    "Massive grouped aggregate row is malformed",
                    category="permanent_fidelity",
                ) from exc
            if (
                not symbol
                or values["date"] != requested.isoformat()
                or symbol in symbols
                or values["open"] <= 0
                or values["high"] <= 0
                or values["low"] <= 0
                or values["close"] <= 0
                or values["volume"] < 0
            ):
                raise HistoricalProviderError(
                    "Massive grouped aggregate scope or values are invalid",
                    category="permanent_fidelity",
                )
            symbols.add(symbol)
            rows.append(values)
        if not rows:
            raise HistoricalProviderError(
                "Massive grouped aggregate response is empty",
                category="permanent_fidelity",
            )
        return sorted(rows, key=lambda item: str(item["symbol"]))

    @staticmethod
    def _aggregate_rows(
        rows: Sequence[Mapping[str, Any]], bar_size: str
    ) -> list[dict[str, Any]]:
        grouped: dict[tuple[Any, ...], list[Mapping[str, Any]]] = defaultdict(list)
        for row in rows:
            observed = datetime.fromtimestamp(int(row["epoch"]), UTC).astimezone(
                EASTERN
            )
            if bar_size == "5 mins":
                minute = observed.minute - observed.minute % 5
                key = (observed.date(), observed.hour, minute)
            elif bar_size == "1 day":
                key = (observed.date(),)
            else:
                raise HistoricalProviderError(
                    f"Massive does not support bar size {bar_size}",
                    category="local_configuration",
                )
            grouped[key].append(row)
        result: list[dict[str, Any]] = []
        for values in grouped.values():
            ordered = sorted(values, key=lambda row: int(row["epoch"]))
            volume = sum(int(row["volume"]) for row in ordered)
            weighted = sum(
                float(row.get("wap") or 0) * int(row["volume"]) for row in ordered
            )
            first = ordered[0]
            stamp = datetime.fromtimestamp(int(first["epoch"]), UTC).astimezone(EASTERN)
            if bar_size == "5 mins":
                stamp = stamp.replace(minute=stamp.minute - stamp.minute % 5, second=0)
            else:
                stamp = datetime.combine(stamp.date(), time(0), tzinfo=EASTERN)
            result.append(
                {
                    "epoch": int(stamp.timestamp()),
                    "time_et": stamp.isoformat(),
                    "date_et": stamp.date().isoformat(),
                    "open": float(first["open"]),
                    "high": max(float(row["high"]) for row in ordered),
                    "low": min(float(row["low"]) for row in ordered),
                    "close": float(ordered[-1]["close"]),
                    "volume": volume,
                    "count": sum(int(row.get("count") or 0) for row in ordered),
                    "wap": weighted / volume if volume else 0.0,
                    "interpolated": False,
                }
            )
        return sorted(result, key=lambda row: int(row["epoch"]))

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
        if what.upper() != "TRADES" or not use_rth:
            raise HistoricalProviderError(
                "Massive replay adapter supports regular-hours TRADES only",
                category="local_configuration",
            )
        start_utc = _coerce_datetime(start)
        end_utc = _coerce_datetime(end)
        if end_utc <= start_utc:
            raise HistoricalProviderError(
                "end must be after start", category="local_configuration"
            )
        rows = self._minute_aggregates(symbol, start_utc, end_utc)
        if bar_size == "1 min":
            return rows
        if bar_size in ("5 mins", "1 day"):
            return self._aggregate_rows(rows, bar_size)
        raise HistoricalProviderError(
            f"Massive does not support bar size {bar_size}",
            category="local_configuration",
        )

    def fetch_bid_ask_ticks(
        self,
        symbol: str,
        start: str | datetime,
        end: str | datetime,
        *,
        use_rth: bool = True,
    ) -> list[dict[str, Any]]:
        if not use_rth:
            raise HistoricalProviderError(
                "Massive replay adapter supports regular-hours quotes only",
                category="local_configuration",
            )
        start_utc = _coerce_datetime(start)
        end_utc = _coerce_datetime(end)
        adjustment_factor = self._split_adjustment_factor(
            symbol, start_utc.astimezone(EASTERN).date().isoformat()
        )
        raw = self._request_pages(
            f"/v3/quotes/{symbol.upper()}",
            params={
                "timestamp.gte": int(start_utc.timestamp() * 1_000_000_000),
                "timestamp.lte": int(end_utc.timestamp() * 1_000_000_000),
                "sort": "timestamp",
                "order": "asc",
                "limit": 50000,
            },
        )
        ticks: list[dict[str, Any]] = []
        for row in raw:
            try:
                nanoseconds = int(row["sip_timestamp"])
                observed = datetime.fromtimestamp(nanoseconds / 1_000_000_000, UTC)
                if not start_utc <= observed <= end_utc:
                    continue
                eastern = observed.astimezone(EASTERN)
                if not time(9, 30) <= eastern.time() < time(16, 0):
                    continue
                bid = float(row.get("bid_price") or 0) * adjustment_factor
                ask = float(row.get("ask_price") or 0) * adjustment_factor
                if bid <= 0 or ask <= 0:
                    continue
                ticks.append(
                    {
                        "epoch": int(observed.timestamp()),
                        "time_et": eastern.isoformat(),
                        "bid": bid,
                        "ask": ask,
                        "bid_size": int(
                            float(row.get("bid_size") or 0) / adjustment_factor
                        ),
                        "ask_size": int(
                            float(row.get("ask_size") or 0) / adjustment_factor
                        ),
                        "split_adjustment_factor": adjustment_factor,
                        "participant_timestamp_ns": int(
                            row.get("participant_timestamp") or nanoseconds
                        ),
                        "sip_timestamp_ns": nanoseconds,
                    }
                )
            except (KeyError, TypeError, ValueError) as exc:
                raise HistoricalProviderError(
                    "Massive quote row is malformed",
                    category="permanent_fidelity",
                ) from exc
        return sorted(ticks, key=lambda row: int(row["sip_timestamp_ns"]))

    def _split_adjustment_factor(self, symbol: str, historical_date: str) -> float:
        rows = self._request_pages(
            "/stocks/v1/splits",
            params={
                "ticker": symbol.upper(),
                "execution_date.gt": historical_date,
                "sort": "execution_date.asc",
                "limit": 1,
            },
        )
        if not rows:
            return 1.0
        try:
            factor = float(rows[0]["historical_adjustment_factor"])
        except (KeyError, TypeError, ValueError) as exc:
            raise HistoricalProviderError(
                "Massive split record lacks a valid historical adjustment factor",
                category="permanent_fidelity",
            ) from exc
        if factor <= 0:
            raise HistoricalProviderError(
                "Massive split adjustment factor must be positive",
                category="permanent_fidelity",
            )
        return factor


@dataclass(frozen=True)
class AlpacaConfig:
    """Credentials and fidelity controls for Alpaca historical stock data."""

    api_key: str
    api_secret: str
    base_url: str = DEFAULT_ALPACA_DATA_URL
    timeout_seconds: float = 30.0
    minimum_interval_seconds: float = 0.35
    feed: str = "sip"
    adjustment: str = "raw"

    @classmethod
    def optional_from_env(cls, path: Path) -> "AlpacaConfig | None":
        values: dict[str, Any] = {}
        if path.exists():
            values.update(dotenv_values(path, interpolate=False))
        keys = (
            "ALPACA_KEY",
            "ALPACA_SECRET",
            "ALPACA_ENDPOINT",
            "ALPACA_DATA_BASE_URL",
            "ALPACA_FEED",
            "ALPACA_ADJUSTMENT",
            "ALPACA_TIMEOUT_SECONDS",
            "ALPACA_MINIMUM_INTERVAL_SECONDS",
            "APCA_API_KEY_ID",
            "APCA_API_SECRET_KEY",
            "APCA_API_DATA_URL",
        )
        for key in keys:
            if key in os.environ:
                values[key] = os.environ[key]
        api_key = str(
            values.get("ALPACA_KEY") or values.get("APCA_API_KEY_ID") or ""
        ).strip()
        secret = str(
            values.get("ALPACA_SECRET") or values.get("APCA_API_SECRET_KEY") or ""
        ).strip()
        if not api_key and not secret:
            return None
        if not api_key or not secret:
            raise HistoricalProviderError(
                "Alpaca requires both key and secret",
                category="local_configuration",
            )
        configured_url = str(
            values.get("ALPACA_DATA_BASE_URL") or values.get("APCA_API_DATA_URL") or ""
        ).strip()
        if not configured_url:
            # ALPACA_ENDPOINT is commonly a paper-trading URL. It is accepted
            # only when it already names an Alpaca market-data host; trading
            # credentials still authenticate against data.alpaca.markets.
            legacy = str(values.get("ALPACA_ENDPOINT") or "").strip()
            legacy_host = urlparse(legacy).netloc.lower() if legacy else ""
            if legacy_host in {
                "data.alpaca.markets",
                "data.sandbox.alpaca.markets",
            }:
                configured_url = legacy
        base_url = (configured_url or DEFAULT_ALPACA_DATA_URL).rstrip("/")
        parsed = urlparse(base_url)
        if parsed.scheme != "https" or parsed.netloc not in {
            "data.alpaca.markets",
            "data.sandbox.alpaca.markets",
        }:
            raise HistoricalProviderError(
                "Alpaca historical data URL must use an official HTTPS data host",
                category="local_configuration",
            )
        feed = str(values.get("ALPACA_FEED") or "sip").strip().lower()
        if feed not in {"sip", "iex", "otc", "boats"}:
            raise HistoricalProviderError(
                "ALPACA_FEED must be sip, iex, otc, or boats",
                category="local_configuration",
            )
        adjustment = str(values.get("ALPACA_ADJUSTMENT") or "raw").strip().lower()
        if adjustment not in {"raw", "split", "dividend", "spin-off", "all"}:
            raise HistoricalProviderError(
                "ALPACA_ADJUSTMENT is unsupported",
                category="local_configuration",
            )
        try:
            timeout = float(values.get("ALPACA_TIMEOUT_SECONDS") or 30.0)
            minimum_interval = float(
                values.get("ALPACA_MINIMUM_INTERVAL_SECONDS") or 0.35
            )
        except (TypeError, ValueError) as exc:
            raise HistoricalProviderError(
                "Alpaca timing values must be numeric",
                category="local_configuration",
            ) from exc
        if timeout <= 0 or minimum_interval < 0:
            raise HistoricalProviderError(
                "Alpaca timing values are out of range",
                category="local_configuration",
            )
        return cls(
            api_key=api_key,
            api_secret=secret,
            base_url=base_url,
            timeout_seconds=timeout,
            minimum_interval_seconds=minimum_interval,
            feed=feed,
            adjustment=adjustment,
        )

    def public_dict(self) -> dict[str, Any]:
        return {
            "base_url": self.base_url,
            "timeout_seconds": self.timeout_seconds,
            "minimum_interval_seconds": self.minimum_interval_seconds,
            "feed": self.feed,
            "adjustment": self.adjustment,
            "credentials_configured": bool(self.api_key and self.api_secret),
        }


class AlpacaHistoricalClient:
    """Read-only Alpaca adapter with explicit feed and adjustment provenance."""

    provider_name = "Alpaca Market Data API"
    cache_namespace = "alpaca"

    def __init__(
        self,
        config: AlpacaConfig,
        *,
        session: requests.Session | None = None,
        sleeper: Any = time_module.sleep,
        monotonic: Any = time_module.monotonic,
    ):
        self.config = config
        self._session = session or requests.Session()
        self._owns_session = session is None
        self._sleeper = sleeper
        self._monotonic = monotonic
        self._last_request_started: float | None = None

    def __enter__(self) -> "AlpacaHistoricalClient":
        return self

    def __exit__(self, exc_type, exc, traceback_obj) -> None:
        self.close()

    def close(self) -> None:
        if self._owns_session:
            self._session.close()

    @property
    def feed(self) -> str:
        return self.config.feed

    @property
    def adjustment(self) -> str:
        return self.config.adjustment

    def _throttle(self) -> None:
        if self._last_request_started is not None:
            elapsed = self._monotonic() - self._last_request_started
            remaining = self.config.minimum_interval_seconds - elapsed
            if remaining > 0:
                self._sleeper(remaining)
        self._last_request_started = self._monotonic()

    def _request_pages(
        self,
        path: str,
        *,
        params: Mapping[str, Any],
        result_key: str,
    ) -> list[dict[str, Any]]:
        url = f"{self.config.base_url}{path}"
        request_params = dict(params)
        rows: list[dict[str, Any]] = []
        pages = 0
        while True:
            pages += 1
            if pages > 500:
                raise HistoricalProviderError(
                    "Alpaca pagination exceeded 500 pages",
                    category="permanent_fidelity",
                )
            self._throttle()
            try:
                response = self._session.get(
                    url,
                    params=request_params,
                    headers={
                        "APCA-API-KEY-ID": self.config.api_key,
                        "APCA-API-SECRET-KEY": self.config.api_secret,
                    },
                    timeout=self.config.timeout_seconds,
                )
            except (requests.Timeout, requests.ConnectionError) as exc:
                raise HistoricalProviderError(
                    "Alpaca request failed due to a transport error",
                    category="retryable_transport",
                ) from exc
            except requests.RequestException as exc:
                raise HistoricalProviderError(
                    "Alpaca request failed before a response was received",
                    category="retryable_provider",
                ) from exc
            if response.status_code == 429 or response.status_code >= 500:
                raise HistoricalProviderError(
                    f"Alpaca HTTP {response.status_code}",
                    category="retryable_provider",
                    retry_after_seconds=_retry_after_seconds(response),
                )
            if response.status_code >= 400:
                category = (
                    "permanent_permission"
                    if response.status_code in (401, 403)
                    else "permanent_fidelity"
                )
                raise HistoricalProviderError(
                    f"Alpaca HTTP {response.status_code}", category=category
                )
            try:
                payload = response.json()
            except ValueError as exc:
                raise HistoricalProviderError(
                    "Alpaca returned invalid JSON",
                    category="retryable_provider",
                ) from exc
            if not isinstance(payload, Mapping):
                raise HistoricalProviderError(
                    "Alpaca response must be an object",
                    category="permanent_fidelity",
                )
            page_rows = payload.get(result_key, [])
            # Alpaca uses null as an explicit no-observations response for
            # some historical symbols/windows. Preserve that as an empty
            # successful result so callers can record the fidelity gap.
            if page_rows is None:
                page_rows = []
            if not isinstance(page_rows, list):
                raise HistoricalProviderError(
                    f"Alpaca {result_key} must be an array",
                    category="permanent_fidelity",
                )
            rows.extend(dict(row) for row in page_rows if isinstance(row, Mapping))
            token = payload.get("next_page_token")
            if not token:
                return rows
            request_params["page_token"] = str(token)

    @staticmethod
    def _observed(value: Any) -> datetime:
        if not isinstance(value, str):
            raise HistoricalProviderError(
                "Alpaca row timestamp is missing",
                category="permanent_fidelity",
            )
        try:
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise HistoricalProviderError(
                "Alpaca row timestamp is malformed",
                category="permanent_fidelity",
            ) from exc
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC)

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
        if what.upper() != "TRADES":
            raise HistoricalProviderError(
                "Alpaca historical adapter supports TRADES bars only",
                category="local_configuration",
            )
        timeframe = {
            "1 min": "1Min",
            "5 mins": "5Min",
            "15 mins": "15Min",
            "1 day": "1Day",
        }.get(bar_size)
        if timeframe is None:
            raise HistoricalProviderError(
                f"Alpaca does not support bar size {bar_size}",
                category="local_configuration",
            )
        start_utc = _coerce_datetime(start)
        end_utc = _coerce_datetime(end)
        if end_utc <= start_utc:
            raise HistoricalProviderError(
                "end must be after start", category="local_configuration"
            )
        normalized_symbol = str(symbol).strip().upper()
        raw = self._request_pages(
            f"/v2/stocks/{normalized_symbol}/bars",
            params={
                "timeframe": timeframe,
                "start": start_utc.isoformat(),
                "end": end_utc.isoformat(),
                "limit": 10000,
                "sort": "asc",
                "feed": self.config.feed,
                "adjustment": self.config.adjustment,
                # Point-in-time security identity is resolved by the sourced
                # local security master. Do not let the provider remap an old
                # ticker to a later underlying entity.
                "asof": "-",
            },
            result_key="bars",
        )
        rows: list[dict[str, Any]] = []
        for row in raw:
            try:
                observed = self._observed(row["t"])
                if not start_utc <= observed < end_utc:
                    continue
                eastern = observed.astimezone(EASTERN)
                if (
                    bar_size != "1 day"
                    and use_rth
                    and not (time(9, 30) <= eastern.time() < time(16, 0))
                ):
                    continue
                rows.append(
                    {
                        "epoch": int(observed.timestamp()),
                        "time_et": eastern.isoformat(),
                        "date_et": eastern.date().isoformat(),
                        "open": float(row["o"]),
                        "high": float(row["h"]),
                        "low": float(row["l"]),
                        "close": float(row["c"]),
                        "volume": int(float(row.get("v") or 0)),
                        "count": int(row.get("n") or 0),
                        "wap": float(row.get("vw") or 0),
                        "interpolated": False,
                    }
                )
            except (KeyError, TypeError, ValueError) as exc:
                raise HistoricalProviderError(
                    "Alpaca bar row is malformed",
                    category="permanent_fidelity",
                ) from exc
        return sorted(rows, key=lambda row: int(row["epoch"]))

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
            raise HistoricalProviderError(
                "end must be after start", category="local_configuration"
            )
        normalized_symbol = str(symbol).strip().upper()
        raw = self._request_pages(
            f"/v2/stocks/{normalized_symbol}/quotes",
            params={
                "start": start_utc.isoformat(),
                "end": end_utc.isoformat(),
                "limit": 10000,
                "sort": "asc",
                "feed": self.config.feed,
                "asof": "-",
            },
            result_key="quotes",
        )
        rows: list[dict[str, Any]] = []
        for row in raw:
            try:
                observed = self._observed(row["t"])
                if not start_utc <= observed <= end_utc:
                    continue
                eastern = observed.astimezone(EASTERN)
                if use_rth and not time(9, 30) <= eastern.time() < time(16, 0):
                    continue
                bid = float(row.get("bp") or 0)
                ask = float(row.get("ap") or 0)
                if bid <= 0 or ask <= 0:
                    continue
                rows.append(
                    {
                        "epoch": int(observed.timestamp()),
                        "time_et": eastern.isoformat(),
                        "bid": bid,
                        "ask": ask,
                        "bid_size": int(float(row.get("bs") or 0)),
                        "ask_size": int(float(row.get("as") or 0)),
                        "bid_exchange": row.get("bx"),
                        "ask_exchange": row.get("ax"),
                        "conditions": row.get("c"),
                        "tape": row.get("z"),
                        "source_timestamp": row.get("t"),
                    }
                )
            except (KeyError, TypeError, ValueError) as exc:
                raise HistoricalProviderError(
                    "Alpaca quote row is malformed",
                    category="permanent_fidelity",
                ) from exc
        return sorted(rows, key=lambda row: int(row["epoch"]))

    def fetch_trades(
        self,
        symbol: str,
        start: str | datetime,
        end: str | datetime,
        *,
        use_rth: bool = True,
    ) -> list[dict[str, Any]]:
        """Return raw historical SIP trades without inventing bar semantics."""

        start_utc = _coerce_datetime(start)
        end_utc = _coerce_datetime(end)
        if end_utc <= start_utc:
            raise HistoricalProviderError(
                "end must be after start", category="local_configuration"
            )
        normalized_symbol = str(symbol).strip().upper()
        raw = self._request_pages(
            f"/v2/stocks/{normalized_symbol}/trades",
            params={
                "start": start_utc.isoformat(),
                "end": end_utc.isoformat(),
                "limit": 10000,
                "sort": "asc",
                "feed": self.config.feed,
                # Keep ticker changes explicit. The scanner security master is
                # the identity authority for each historical date.
                "asof": "-",
            },
            result_key="trades",
        )
        rows: list[dict[str, Any]] = []
        for row in raw:
            try:
                observed = self._observed(row["t"])
                if not start_utc <= observed < end_utc:
                    continue
                eastern = observed.astimezone(EASTERN)
                if use_rth and not time(9, 30) <= eastern.time() < time(16, 0):
                    continue
                price = float(row["p"])
                size = int(float(row["s"]))
                if price <= 0 or size <= 0:
                    continue
                rows.append(
                    {
                        "epoch": int(observed.timestamp()),
                        "time_et": eastern.isoformat(),
                        "price": price,
                        "size": size,
                        "exchange": row.get("x"),
                        "conditions": row.get("c"),
                        "trade_id": row.get("i"),
                        "tape": row.get("z"),
                        "source_timestamp": row.get("t"),
                    }
                )
            except (KeyError, TypeError, ValueError) as exc:
                raise HistoricalProviderError(
                    "Alpaca trade row is malformed",
                    category="permanent_fidelity",
                ) from exc
        return sorted(
            rows,
            key=lambda row: (
                str(row.get("source_timestamp", "")),
                str(row.get("trade_id", "")),
            ),
        )

    def fetch_news(
        self,
        symbols: Sequence[str],
        start: str | datetime,
        end: str | datetime,
        *,
        include_content: bool = False,
    ) -> list[dict[str, Any]]:
        """Return time-bounded Benzinga articles supplied by Alpaca.

        This is discovery evidence only. Callers must not equate an article
        returned here with the strategy's verified-primary-catalyst gate.
        """

        normalized_symbols = sorted(
            {str(symbol).strip().upper() for symbol in symbols if str(symbol).strip()}
        )
        if not normalized_symbols:
            raise HistoricalProviderError(
                "news symbols must be non-empty", category="local_configuration"
            )
        start_utc = _coerce_datetime(start)
        end_utc = _coerce_datetime(end)
        if end_utc <= start_utc:
            raise HistoricalProviderError(
                "end must be after start", category="local_configuration"
            )
        raw = self._request_pages(
            "/v1beta1/news",
            params={
                "symbols": ",".join(normalized_symbols),
                "start": start_utc.isoformat(),
                "end": end_utc.isoformat(),
                "limit": 50,
                "sort": "asc",
                "include_content": str(bool(include_content)).lower(),
            },
            result_key="news",
        )
        rows: list[dict[str, Any]] = []
        for row in raw:
            try:
                created = self._observed(row["created_at"])
                if not start_utc <= created <= end_utc:
                    continue
                article_symbols = sorted(
                    {
                        str(symbol).strip().upper()
                        for symbol in row.get("symbols", [])
                        if str(symbol).strip()
                    }
                )
                rows.append(
                    {
                        "article_id": row.get("id"),
                        "created_at": created.isoformat(),
                        "updated_at": row.get("updated_at"),
                        "headline": str(row.get("headline") or ""),
                        "summary": str(row.get("summary") or ""),
                        "author": row.get("author"),
                        "source": row.get("source"),
                        "url": row.get("url"),
                        "symbols": article_symbols,
                        **(
                            {"content": str(row.get("content") or "")}
                            if include_content
                            else {}
                        ),
                    }
                )
            except (KeyError, TypeError, ValueError) as exc:
                raise HistoricalProviderError(
                    "Alpaca news row is malformed",
                    category="permanent_fidelity",
                ) from exc
        return sorted(
            rows,
            key=lambda row: (
                str(row["created_at"]),
                str(row.get("article_id", "")),
            ),
        )

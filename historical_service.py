"""Cache-first historical market-data services and provider orchestration."""

from __future__ import annotations

from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from time import monotonic
from typing import Any, Callable, Iterator, Sequence, TypeVar
from zoneinfo import ZoneInfo

from historical_providers import (
    AlpacaConfig,
    AlpacaHistoricalClient,
    HistoricalMarketDataClient,
    HistoricalProviderError,
    MassiveConfig,
    MassiveHistoricalClient,
)
from historical_store import (
    HistoricalDayStore,
    aggregate_bars,
    build_dataset,
    compact_bar,
    compact_quote,
    expand_bar,
    expand_quote,
    provider_id,
)
from ibkr_historical import (
    IBKRConfig,
    IBKRConfigurationError,
    IBKRHistoricalClient,
    IBKRHistoricalError,
    historical_error_category,
)


EASTERN = ZoneInfo("America/New_York")
UTC = timezone.utc
T = TypeVar("T")
PROVIDER_ORDER = ("ibkr", "massive", "alpaca")
PROVIDER_NAMES = {
    "ibkr": "Interactive Brokers TWS API",
    "massive": "Massive SIP REST API",
    "alpaca": "Alpaca Market Data API",
}
PROVIDER_FEEDS = {"ibkr": "smart", "massive": "sip", "alpaca": "sip"}
PROVIDER_ADJUSTMENTS = {
    "ibkr": "provider_adjusted_unknown_basis",
    "massive": "split_adjusted",
    "alpaca": "raw",
}


def _coerce(value: str | datetime) -> datetime:
    parsed = value if isinstance(value, datetime) else datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=EASTERN)
    return parsed.astimezone(UTC)


def _provider_metadata(client: HistoricalMarketDataClient) -> tuple[str, str, str]:
    provider = provider_id(str(getattr(client, "provider_name", "unknown")))
    if provider == "alpaca":
        return provider, str(getattr(client, "feed", "unknown")), str(
            getattr(client, "adjustment", "raw")
        )
    if provider == "massive":
        return provider, "sip", "split_adjusted"
    if provider == "ibkr":
        return provider, "smart", "provider_adjusted_unknown_basis"
    return provider, "unknown", "unknown"


class RecordingHistoricalClient:
    """Persist every successful provider response in canonical daily shape."""

    def __init__(self, client: HistoricalMarketDataClient, store: HistoricalDayStore):
        self.client = client
        self.store = store
        self.provider_name = str(getattr(client, "provider_name", type(client).__name__))
        self.cache_namespace = str(
            getattr(client, "cache_namespace", provider_id(self.provider_name))
        )

    def __getattr__(self, name: str) -> Any:
        """Delegate provider-specific read methods such as IBKR contract lookup."""

        return getattr(self.client, name)

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
        rows = self.client.fetch_bars(
            symbol,
            start,
            end,
            bar_size=bar_size,
            what=what,
            use_rth=use_rth,
        )
        provider, feed, adjustment = _provider_metadata(self.client)
        timeframe = {"1 min": "1m", "5 mins": "5m", "1 day": "1d"}.get(
            bar_size, bar_size.replace(" ", "")
        )
        expected = {"1m": 390, "5m": 78, "1d": 1}.get(timeframe)
        grouped: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            day = str(row.get("date_et") or "")
            if not day:
                day = datetime.fromtimestamp(int(row["epoch"]), UTC).astimezone(
                    EASTERN
                ).date().isoformat()
            grouped.setdefault(day, []).append(compact_bar(row, day=day))
        request = {
            "symbol": str(symbol).upper(),
            "start": _coerce(start).isoformat(),
            "end": _coerce(end).isoformat(),
            "bar_size": bar_size,
            "what": what,
            "use_rth": use_rth,
        }
        captured_at = datetime.now(UTC).isoformat()
        for day, values in grouped.items():
            values.sort(key=lambda row: str(row["t"]))
            complete = expected is not None and len(values) == expected
            self.store.merge(
                symbol,
                day,
                datasets=[
                    build_dataset(
                        kind="bars",
                        provider=provider,
                        rows=values,
                        channel=what.lower(),
                        timeframe=timeframe,
                        feed=feed,
                        adjustment=adjustment,
                        session="regular" if use_rth else "all",
                        scope="full_session" if complete else "observed_window",
                        quality={"complete": complete},
                        provenance={
                            "source_type": "live_provider_collection",
                            "captured_at": captured_at,
                            "request": request,
                        },
                    )
                ],
            )
        return rows

    def fetch_bid_ask_ticks(
        self,
        symbol: str,
        start: str | datetime,
        end: str | datetime,
        *,
        use_rth: bool = True,
    ) -> list[dict[str, Any]]:
        rows = self.client.fetch_bid_ask_ticks(
            symbol, start, end, use_rth=use_rth
        )
        provider, feed, adjustment = _provider_metadata(self.client)
        grouped: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            observed = datetime.fromtimestamp(int(row["epoch"]), UTC).astimezone(EASTERN)
            grouped.setdefault(observed.date().isoformat(), []).append(
                compact_quote(row, day=observed.date().isoformat())
            )
        request = {
            "symbol": str(symbol).upper(),
            "start": _coerce(start).isoformat(),
            "end": _coerce(end).isoformat(),
            "use_rth": use_rth,
        }
        captured_at = datetime.now(UTC).isoformat()
        for day, values in grouped.items():
            values.sort(key=lambda row: str(row["t"]))
            self.store.merge(
                symbol,
                day,
                datasets=[
                    build_dataset(
                        kind="quotes",
                        provider=provider,
                        rows=values,
                        channel="top_of_book",
                        feed=feed,
                        adjustment=adjustment,
                        session="regular" if use_rth else "all",
                        scope="observed_window",
                        quality={"complete": None},
                        provenance={
                            "source_type": "live_provider_collection",
                            "captured_at": captured_at,
                            "request": request,
                        },
                    )
                ],
            )
        return rows


class LocalHistoricalClient:
    """Serve one provider's immutable observations from the canonical store."""

    def __init__(
        self,
        store: HistoricalDayStore,
        provider: str,
        *,
        feed: str | None = None,
        adjustment: str | None = None,
    ):
        self.store = store
        self.cache_namespace = provider_id(provider)
        self.feed = feed or PROVIDER_FEEDS.get(self.cache_namespace)
        self.adjustment = adjustment or PROVIDER_ADJUSTMENTS.get(
            self.cache_namespace
        )
        self.provider_name = PROVIDER_NAMES.get(
            self.cache_namespace, f"Local historical cache ({self.cache_namespace})"
        )

    def _days(self, symbol: str, start: datetime, end: datetime) -> list[str]:
        return [
            day
            for day in self.store.dates(symbol)
            if start.astimezone(EASTERN).date()
            <= date.fromisoformat(day)
            <= end.astimezone(EASTERN).date()
        ]

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
                "local canonical cache supports regular-hours TRADES only",
                category="local_cache_miss",
            )
        start_utc = _coerce(start)
        end_utc = _coerce(end)
        timeframe = {"1 min": "1m", "5 mins": "5m", "1 day": "1d"}.get(
            bar_size
        )
        if timeframe is None:
            raise HistoricalProviderError(
                f"local cache does not support bar size {bar_size}",
                category="local_configuration",
            )
        output: list[dict[str, Any]] = []
        for day in self._days(symbol, start_utc, end_utc):
            dataset = self.store.select_dataset(
                symbol,
                day,
                kind="bars",
                channel="trades",
                timeframe=timeframe,
                providers=(self.cache_namespace,),
                require_complete=True,
                feed=self.feed,
                adjustment=self.adjustment,
            )
            rows: list[dict[str, Any]] = []
            if dataset is not None:
                rows = [expand_bar(row) for row in dataset["rows"]]
            elif timeframe in {"5m", "1d"}:
                minute = self.store.select_dataset(
                    symbol,
                    day,
                    kind="bars",
                    channel="trades",
                    timeframe="1m",
                    providers=(self.cache_namespace,),
                    require_complete=True,
                    feed=self.feed,
                    adjustment=self.adjustment,
                )
                if minute is not None:
                    rows = aggregate_bars(
                        [expand_bar(row) for row in minute["rows"]], timeframe
                    )
            for row in rows:
                epoch = int(row["epoch"])
                if int(start_utc.timestamp()) <= epoch < int(end_utc.timestamp()):
                    output.append(row)
        if not output:
            raise HistoricalProviderError(
                f"canonical {self.cache_namespace} cache miss for {symbol}",
                category="local_cache_miss",
            )
        return sorted(output, key=lambda row: int(row["epoch"]))

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
                "local canonical cache supports regular-hours quotes only",
                category="local_cache_miss",
            )
        start_utc = _coerce(start)
        end_utc = _coerce(end)
        output: list[dict[str, Any]] = []
        for day in self._days(symbol, start_utc, end_utc):
            document = self.store.load(symbol, day)
            if document is None:
                continue
            for dataset in document["datasets"]:
                if (
                    dataset.get("kind") != "quotes"
                    or dataset.get("channel") != "top_of_book"
                    or provider_id(str(dataset.get("provider", "")))
                    != self.cache_namespace
                    or (self.feed is not None and dataset.get("feed") != self.feed)
                    or (
                        self.adjustment is not None
                        and dataset.get("adjustment") != self.adjustment
                    )
                ):
                    continue
                for row in dataset["rows"]:
                    expanded = expand_quote(row)
                    epoch = int(expanded["epoch"])
                    if int(start_utc.timestamp()) <= epoch <= int(end_utc.timestamp()):
                        output.append(expanded)
        if not output:
            raise HistoricalProviderError(
                f"canonical {self.cache_namespace} quote cache miss for {symbol}",
                category="local_cache_miss",
            )
        deduplicated = {
            (
                int(row["epoch"]),
                float(row["bid"]),
                float(row["ask"]),
                int(row.get("bid_size", 0)),
                int(row.get("ask_size", 0)),
                str(row.get("source_timestamp", "")),
            ): row
            for row in output
        }
        return sorted(deduplicated.values(), key=lambda row: int(row["epoch"]))


@dataclass(frozen=True, slots=True)
class ProviderAttempt:
    provider: str
    status: str
    category: str | None
    elapsed_seconds: float
    error: str | None = None

    def public_dict(self) -> dict[str, Any]:
        return {
            "provider": self.provider,
            "status": self.status,
            "category": self.category,
            "elapsed_seconds": self.elapsed_seconds,
            "error": self.error,
        }


def collect_with_fallback(
    clients: Sequence[HistoricalMarketDataClient],
    operation: Callable[[HistoricalMarketDataClient], T],
) -> tuple[T, HistoricalMarketDataClient, list[ProviderAttempt]]:
    result, client, attempts, last_error = try_collect_with_fallback(
        clients, operation
    )
    if client is not None and result is not None:
        return result, client, attempts
    if last_error is None:
        raise HistoricalProviderError(
            "no historical providers are configured",
            category="local_configuration",
        )
    raise HistoricalProviderError(
        "all historical providers failed; "
        + "; ".join(
            f"{attempt.provider}={attempt.category}" for attempt in attempts
        ),
        category=(
            last_error.category
            if isinstance(last_error, HistoricalProviderError)
            else historical_error_category(last_error)
            if isinstance(last_error, IBKRHistoricalError)
            else "local_io"
        ),
    ) from last_error


def try_collect_with_fallback(
    clients: Sequence[HistoricalMarketDataClient],
    operation: Callable[[HistoricalMarketDataClient], T],
) -> tuple[
    T | None,
    HistoricalMarketDataClient | None,
    list[ProviderAttempt],
    Exception | None,
]:
    """Return attempts on total failure so a caller can open a later lane."""

    attempts: list[ProviderAttempt] = []
    last_error: Exception | None = None
    for client in clients:
        started = monotonic()
        name = str(getattr(client, "provider_name", type(client).__name__))
        try:
            result = operation(client)
            attempts.append(
                ProviderAttempt(name, "success", None, monotonic() - started)
            )
            return result, client, attempts, None
        except (HistoricalProviderError, IBKRHistoricalError, OSError) as exc:
            last_error = exc
            category = (
                exc.category
                if isinstance(exc, HistoricalProviderError)
                else historical_error_category(exc)
                if isinstance(exc, IBKRHistoricalError)
                else "local_io"
            )
            attempts.append(
                ProviderAttempt(
                    name,
                    "failed",
                    category,
                    monotonic() - started,
                    str(exc),
                )
            )
    return None, None, attempts, last_error


def local_cache_clients(
    env_file: Path, store: HistoricalDayStore
) -> list[LocalHistoricalClient]:
    """Build fidelity-qualified cache lanes without connecting to a provider."""

    alpaca_config = AlpacaConfig.optional_from_env(env_file)
    return [
        LocalHistoricalClient(store, "ibkr"),
        LocalHistoricalClient(store, "massive"),
        LocalHistoricalClient(
            store,
            "alpaca",
            feed=alpaca_config.feed if alpaca_config is not None else "sip",
            adjustment=(
                alpaca_config.adjustment if alpaca_config is not None else "raw"
            ),
        ),
    ]


@dataclass
class OpenProviderSet:
    cache_clients: list[LocalHistoricalClient]
    live_clients: list[HistoricalMarketDataClient]
    startup_attempts: list[ProviderAttempt]

    @property
    def all_clients(self) -> list[HistoricalMarketDataClient]:
        return [*self.cache_clients, *self.live_clients]


@contextmanager
def open_provider_set(
    env_file: Path,
    store: HistoricalDayStore,
) -> Iterator[OpenProviderSet]:
    massive_config = MassiveConfig.optional_from_env(env_file)
    alpaca_config = AlpacaConfig.optional_from_env(env_file)
    cache_clients = local_cache_clients(env_file, store)
    live: list[HistoricalMarketDataClient] = []
    closers: list[Callable[[], None]] = []
    startup: list[ProviderAttempt] = []
    started = monotonic()
    try:
        try:
            ibkr = IBKRHistoricalClient(IBKRConfig.from_env(env_file))
            ibkr.connect_and_wait()
            live.append(RecordingHistoricalClient(ibkr, store))
            closers.append(ibkr.close)
            startup.append(
                ProviderAttempt(
                    PROVIDER_NAMES["ibkr"], "ready", None, monotonic() - started
                )
            )
        except (IBKRConfigurationError, IBKRHistoricalError, OSError) as exc:
            startup.append(
                ProviderAttempt(
                    PROVIDER_NAMES["ibkr"],
                    "unavailable",
                    historical_error_category(exc)
                    if isinstance(exc, IBKRHistoricalError)
                    else "local_configuration",
                    monotonic() - started,
                    str(exc),
                )
            )
        if massive_config is not None:
            massive = MassiveHistoricalClient(massive_config)
            live.append(RecordingHistoricalClient(massive, store))
            closers.append(massive.close)
            startup.append(
                ProviderAttempt(PROVIDER_NAMES["massive"], "ready", None, 0.0)
            )
        else:
            startup.append(
                ProviderAttempt(
                    PROVIDER_NAMES["massive"],
                    "unconfigured",
                    "local_configuration",
                    0.0,
                )
            )
        if alpaca_config is not None:
            alpaca = AlpacaHistoricalClient(alpaca_config)
            live.append(RecordingHistoricalClient(alpaca, store))
            closers.append(alpaca.close)
            startup.append(
                ProviderAttempt(PROVIDER_NAMES["alpaca"], "ready", None, 0.0)
            )
        else:
            startup.append(
                ProviderAttempt(
                    PROVIDER_NAMES["alpaca"],
                    "unconfigured",
                    "local_configuration",
                    0.0,
                )
            )
        yield OpenProviderSet(cache_clients, live, startup)
    finally:
        for close in reversed(closers):
            try:
                close()
            except Exception:
                pass

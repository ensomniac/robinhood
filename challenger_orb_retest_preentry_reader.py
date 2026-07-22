"""Read the challenger's exact frozen regular-session request windows locally."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any, Callable

from historical_providers import HistoricalProviderError
from historical_store import (
    HistoricalDayStore,
    expand_bar,
    expand_trade,
    provider_id,
)


def _coerce(value: str | datetime) -> datetime:
    parsed = value if isinstance(value, datetime) else datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        raise HistoricalProviderError(
            "frozen causal request timestamp is timezone-naive",
            category="local_configuration",
        )
    return parsed.astimezone(UTC)


def _provenance_covers(
    dataset: Mapping[str, Any],
    *,
    start: datetime,
    end: datetime,
    bar_size: str | None,
) -> bool:
    provenance = dataset.get("provenance")
    samples = provenance.get("samples", []) if isinstance(provenance, Mapping) else []
    for sample in samples:
        request = sample.get("request") if isinstance(sample, Mapping) else None
        if not isinstance(request, Mapping) or request.get("use_rth") is not True:
            continue
        if bar_size is not None and request.get("bar_size") != bar_size:
            continue
        try:
            captured_start = _coerce(str(request["start"]))
            captured_end = _coerce(str(request["end"]))
        except (KeyError, TypeError, ValueError):
            continue
        if captured_start == start and captured_end == end:
            return True
    return False


class FrozenCausalWindowClient:
    """Serve one exact provider window, including bounded RTH observations."""

    def __init__(
        self,
        store: HistoricalDayStore,
        provider: str,
        *,
        feed: str,
        adjustment: str,
    ) -> None:
        self.store = store
        self.provider = provider_id(provider)
        self.feed = feed
        self.adjustment = adjustment

    def _rows(
        self,
        symbol: str,
        start: str | datetime,
        end: str | datetime,
        *,
        kind: str,
        channel: str,
        timeframe: str | None,
        bar_size: str | None,
        expand: Callable[[Mapping[str, Any]], dict[str, Any]],
    ) -> list[dict[str, Any]]:
        start_utc, end_utc = _coerce(start), _coerce(end)
        days = [
            day
            for day in self.store.dates(symbol)
            if start_utc.date() <= datetime.fromisoformat(day).date() <= end_utc.date()
        ]
        candidates: list[list[dict[str, Any]]] = []
        for day in days:
            document = self.store.load(symbol, day)
            if document is None:
                continue
            for dataset in document.get("datasets", []):
                if not (
                    isinstance(dataset, Mapping)
                    and dataset.get("kind") == kind
                    and dataset.get("channel") == channel
                    and dataset.get("timeframe") == timeframe
                    and provider_id(str(dataset.get("provider", ""))) == self.provider
                    and dataset.get("feed") == self.feed
                    and dataset.get("adjustment") == self.adjustment
                    and dataset.get("quality", {}).get("requested_window_complete")
                    is True
                    and _provenance_covers(
                        dataset, start=start_utc, end=end_utc, bar_size=bar_size
                    )
                ):
                    continue
                rows = []
                for compact in dataset.get("rows", []):
                    row = expand(compact)
                    observed = _coerce(
                        str(row.get("source_timestamp") or row["time_et"])
                    )
                    if start_utc <= observed < end_utc:
                        rows.append(row)
                candidates.append(rows)
        unique = {
            tuple(
                sorted(
                    (
                        str(row.get("source_timestamp") or row.get("time_et")),
                        str(row),
                    )
                    for row in rows
                )
            ): rows
            for rows in candidates
        }
        if len(unique) != 1:
            raise HistoricalProviderError(
                f"exact frozen {self.provider} window is missing or ambiguous for {symbol}",
                category="local_cache_miss",
            )
        return next(iter(unique.values()))

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
        if bar_size != "1 min" or what != "TRADES" or use_rth is not True:
            raise HistoricalProviderError(
                "frozen causal reader received an unsupported bar request",
                category="local_configuration",
            )
        return self._rows(
            symbol,
            start,
            end,
            kind="bars",
            channel="trades",
            timeframe="1m",
            bar_size=bar_size,
            expand=expand_bar,
        )

    def fetch_trades(
        self,
        symbol: str,
        start: str | datetime,
        end: str | datetime,
        *,
        use_rth: bool = True,
    ) -> list[dict[str, Any]]:
        if use_rth is not True:
            raise HistoricalProviderError(
                "frozen causal reader received an unsupported trade request",
                category="local_configuration",
            )
        return self._rows(
            symbol,
            start,
            end,
            kind="trades",
            channel="sale",
            timeframe=None,
            bar_size=None,
            expand=expand_trade,
        )

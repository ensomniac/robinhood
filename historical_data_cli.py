"""Operate the canonical local historical-data store without broker actions."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date, datetime, time
from pathlib import Path
from time import monotonic
from typing import Any, Sequence
from zoneinfo import ZoneInfo

from historical_providers import HistoricalProviderError
from historical_service import (
    collect_with_fallback,
    local_cache_clients,
    open_provider_set,
    try_collect_with_fallback,
)
from historical_store import DEFAULT_ENV_PATH, HistoricalDayStore, HistoricalStoreError


EASTERN = ZoneInfo("America/New_York")


def _session(day: str) -> tuple[datetime, datetime]:
    parsed = date.fromisoformat(day)
    return (
        datetime.combine(parsed, time(9, 30), tzinfo=EASTERN),
        datetime.combine(parsed, time(16, 0), tzinfo=EASTERN),
    )


def _provider_check(args: argparse.Namespace, store: HistoricalDayStore) -> dict[str, Any]:
    start, _ = _session(args.date)
    end = start.replace(minute=35)
    results = []
    with open_provider_set(args.env_file, store) as providers:
        for startup in providers.startup_attempts:
            matching = [
                client
                for client in providers.live_clients
                if client.provider_name == startup.provider
            ]
            if not matching:
                results.append(startup.public_dict())
                continue
            client = matching[0]
            started = monotonic()
            try:
                rows = client.fetch_bars(
                    args.symbol,
                    start,
                    end,
                    bar_size="1 min",
                    what="TRADES",
                    use_rth=True,
                )
                results.append(
                    {
                        "provider": client.provider_name,
                        "status": "success",
                        "rows": len(rows),
                        "elapsed_seconds": monotonic() - started,
                    }
                )
            except Exception as exc:
                results.append(
                    {
                        "provider": client.provider_name,
                        "status": "failed",
                        "error_type": type(exc).__name__,
                        "error": str(exc),
                        "elapsed_seconds": monotonic() - started,
                    }
                )
    return {
        "operation": "provider_check",
        "symbol": args.symbol.upper(),
        "date": args.date,
        "results": results,
        "valid": all(row["status"] == "success" for row in results),
    }


def _fetch(args: argparse.Namespace, store: HistoricalDayStore) -> dict[str, Any]:
    start, end = _session(args.date)

    def operation(candidate):
        values = candidate.fetch_bars(
            args.symbol,
            start,
            end,
            bar_size="1 min",
            what="TRADES",
            use_rth=True,
        )
        if len(values) != 390:
            raise HistoricalProviderError(
                f"full regular session needs 390 minute bars, got {len(values)}",
                category="permanent_fidelity",
            )
        return values
    rows, client, cache_attempts, _ = try_collect_with_fallback(
        local_cache_clients(args.env_file, store), operation
    )
    live_attempts = []
    if client is None or rows is None:
        with open_provider_set(args.env_file, store) as providers:
            rows, client, live_attempts = collect_with_fallback(
                providers.live_clients, operation
            )
    return {
        "operation": "fetch",
        "symbol": args.symbol.upper(),
        "date": args.date,
        "provider": client.provider_name,
        "rows": len(rows),
        "canonical_path": str(store.path_for(args.symbol, args.date)),
        "attempts": [
            {**attempt.public_dict(), "lane": "cache"}
            for attempt in cache_attempts
        ]
        + [
            {**attempt.public_dict(), "lane": "live"}
            for attempt in live_attempts
        ],
        "valid": bool(rows),
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", type=Path, default=DEFAULT_ENV_PATH)
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("check", help="validate the configured store and audit files")
    providers = subparsers.add_parser(
        "check-providers", help="test IBKR, Massive, and Alpaca independently"
    )
    providers.add_argument("--symbol", default="AAPL")
    providers.add_argument("--date", required=True)
    fetch = subparsers.add_parser(
        "fetch", help="serve from cache or pull IBKR, then Massive, then Alpaca"
    )
    fetch.add_argument("symbol")
    fetch.add_argument("--date", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        store = HistoricalDayStore.from_env(args.env_file)
        if args.command == "check":
            result = store.audit()
        elif args.command == "check-providers":
            result = _provider_check(args, store)
        else:
            result = _fetch(args, store)
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0 if result.get("valid") else 1
    except (
        HistoricalProviderError,
        HistoricalStoreError,
        OSError,
        ValueError,
    ) as exc:
        print(
            json.dumps(
                {"error": str(exc), "error_type": type(exc).__name__}, indent=2
            ),
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

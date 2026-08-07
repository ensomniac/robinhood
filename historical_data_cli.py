"""Operate the canonical local historical-data store without broker actions."""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import sys
from contextlib import ExitStack
from datetime import UTC, date, datetime, time
from pathlib import Path
from time import monotonic
from typing import Any, Mapping, Sequence
from zoneinfo import ZoneInfo

from historical_providers import HistoricalProviderError
from historical_service import (
    OpenProviderSet,
    local_cache_clients,
    open_provider_set,
    try_collect_with_fallback,
)
from historical_store import (
    DEFAULT_ENV_PATH,
    HistoricalDayStore,
    HistoricalStoreError,
    normalize_symbol,
)


EASTERN = ZoneInfo("America/New_York")
REFRESH_SCHEMA_VERSION = 1
REFRESH_KIND = "robinhood_codex_historical_refresh_checkpoint"
CHECKPOINT_FIELDS = {
    "canonical_path",
    "completed_at",
    "kind",
    "manifest_sha256",
    "provider",
    "record_sha256",
    "request",
    "request_sha256",
    "rows",
    "schema_version",
}


def _canonical(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode()


def _hash(value: Any) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _session(day: str) -> tuple[datetime, datetime]:
    parsed = date.fromisoformat(day)
    return (
        datetime.combine(parsed, time(9, 30), tzinfo=EASTERN),
        datetime.combine(parsed, time(16, 0), tzinfo=EASTERN),
    )


def _normalize_request(value: Any) -> dict[str, str]:
    if not isinstance(value, Mapping) or set(value) != {"date", "symbol"}:
        raise HistoricalStoreError(
            "each refresh request needs exactly date and symbol"
        )
    raw_day = value.get("date")
    raw_symbol = value.get("symbol")
    if not isinstance(raw_day, str):
        raise HistoricalStoreError("refresh request date must be an ISO string")
    try:
        parsed_day = date.fromisoformat(raw_day)
    except ValueError as exc:
        raise HistoricalStoreError("refresh request date must be ISO YYYY-MM-DD") from exc
    if raw_day != parsed_day.isoformat():
        raise HistoricalStoreError("refresh request date must be canonical ISO")
    if not isinstance(raw_symbol, str):
        raise HistoricalStoreError("refresh request symbol must be a string")
    return {"date": raw_day, "symbol": normalize_symbol(raw_symbol)}


def _load_refresh_manifest(path: Path) -> tuple[dict[str, Any], str]:
    resolved = path.resolve()
    if not resolved.is_file():
        raise HistoricalStoreError(f"refresh manifest is unavailable: {resolved}")
    if resolved.stat().st_size > 16 * 1024 * 1024:
        raise HistoricalStoreError("refresh manifest is unexpectedly large")
    try:
        value = json.loads(resolved.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise HistoricalStoreError(f"cannot read refresh manifest: {exc}") from exc
    if not isinstance(value, Mapping) or set(value) != {"schema_version", "requests"}:
        raise HistoricalStoreError(
            "refresh manifest needs exactly schema_version and requests"
        )
    if value.get("schema_version") != REFRESH_SCHEMA_VERSION:
        raise HistoricalStoreError("refresh manifest schema_version must be 1")
    raw_requests = value.get("requests")
    if not isinstance(raw_requests, list) or not raw_requests:
        raise HistoricalStoreError("refresh manifest requests must be non-empty")
    requests = sorted(
        (_normalize_request(item) for item in raw_requests),
        key=lambda item: (item["date"], item["symbol"]),
    )
    identities = [(item["date"], item["symbol"]) for item in requests]
    if len(identities) != len(set(identities)):
        raise HistoricalStoreError("refresh manifest contains duplicate requests")
    normalized = {"schema_version": REFRESH_SCHEMA_VERSION, "requests": requests}
    return normalized, _hash(normalized)


def _request_operation(request: Mapping[str, str]):
    start, end = _session(request["date"])

    def operation(client):
        rows = client.fetch_bars(
            request["symbol"],
            start,
            end,
            bar_size="1 min",
            what="TRADES",
            use_rth=True,
        )
        if not rows:
            raise HistoricalProviderError(
                "full regular-session request returned no minute bars",
                category="permanent_fidelity",
            )
        return rows

    return operation


def _attempts(values, lane: str) -> list[dict[str, Any]]:
    return [{**attempt.public_dict(), "lane": lane} for attempt in values]


def _cache_request(
    request: Mapping[str, str], env_file: Path, store: HistoricalDayStore
) -> tuple[Any, Any, list[Any], Exception | None]:
    return try_collect_with_fallback(
        local_cache_clients(env_file, store), _request_operation(request)
    )


def _live_request(
    request: Mapping[str, str], providers: OpenProviderSet
) -> tuple[Any, Any, list[Any], Exception | None]:
    return try_collect_with_fallback(
        providers.live_clients, _request_operation(request)
    )


def _success(
    request: Mapping[str, str],
    store: HistoricalDayStore,
    rows: Sequence[Any],
    client: Any,
    cache_attempts: Sequence[Any],
    live_attempts: Sequence[Any],
) -> dict[str, Any]:
    return {
        "status": "completed",
        "request": dict(request),
        "provider": str(client.provider_name),
        "rows": len(rows),
        "canonical_path": str(store.path_for(request["symbol"], request["date"])),
        "attempts": [
            *_attempts(cache_attempts, "cache"),
            *_attempts(live_attempts, "live"),
        ],
    }


def _fetch(args: argparse.Namespace, store: HistoricalDayStore) -> dict[str, Any]:
    request = _normalize_request({"symbol": args.symbol, "date": args.date})
    rows, client, cache_attempts, cache_error = _cache_request(
        request, args.env_file, store
    )
    live_attempts: Sequence[Any] = []
    last_error = cache_error
    if client is None or rows is None:
        with open_provider_set(args.env_file, store) as providers:
            rows, client, live_attempts, last_error = _live_request(request, providers)
    if client is None or rows is None:
        attempts = [
            *_attempts(cache_attempts, "cache"),
            *_attempts(live_attempts, "live"),
        ]
        categories = "; ".join(
            f"{item['provider']}={item['category']}" for item in attempts
        )
        raise HistoricalProviderError(
            f"all historical providers failed{': ' + categories if categories else ''}",
            category=getattr(last_error, "category", "local_configuration"),
        )
    result = _success(
        request, store, rows, client, cache_attempts, live_attempts
    )
    return {"operation": "fetch", "valid": True, **result}


def _checkpoint_path(store: HistoricalDayStore, manifest_sha256: str) -> Path:
    return store.root / "_operations" / "refresh" / f"{manifest_sha256}.jsonl"


def _validate_checkpoint(
    value: Any,
    *,
    manifest_sha256: str,
    store: HistoricalDayStore,
) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise HistoricalStoreError("refresh checkpoint must contain an object")
    if set(value) != CHECKPOINT_FIELDS:
        raise HistoricalStoreError("refresh checkpoint fields drifted")
    record = dict(value)
    supplied = record.pop("record_sha256", None)
    if record.get("schema_version") != REFRESH_SCHEMA_VERSION:
        raise HistoricalStoreError("refresh checkpoint schema drifted")
    if record.get("kind") != REFRESH_KIND:
        raise HistoricalStoreError("refresh checkpoint kind drifted")
    if record.get("manifest_sha256") != manifest_sha256:
        raise HistoricalStoreError("refresh checkpoint manifest hash drifted")
    request = _normalize_request(record.get("request"))
    if record.get("request_sha256") != _hash(request):
        raise HistoricalStoreError("refresh checkpoint request hash drifted")
    rows = record.get("rows")
    if isinstance(rows, bool) or not isinstance(rows, int) or rows < 1:
        raise HistoricalStoreError("refresh checkpoint rows must be positive")
    if not isinstance(record.get("provider"), str) or not record["provider"]:
        raise HistoricalStoreError("refresh checkpoint provider is missing")
    expected_path = store.path_for(request["symbol"], request["date"])
    try:
        relative = expected_path.relative_to(store.root).as_posix()
    except ValueError as exc:
        raise HistoricalStoreError("canonical path escapes the store") from exc
    if record.get("canonical_path") != relative:
        raise HistoricalStoreError("refresh checkpoint canonical path drifted")
    completed_at = record.get("completed_at")
    if not isinstance(completed_at, str):
        raise HistoricalStoreError("refresh checkpoint completion time is missing")
    try:
        parsed = datetime.fromisoformat(completed_at.replace("Z", "+00:00"))
    except ValueError as exc:
        raise HistoricalStoreError("refresh checkpoint completion time is invalid") from exc
    if parsed.tzinfo is None:
        raise HistoricalStoreError("refresh checkpoint completion needs a timezone")
    if supplied != _hash(record):
        raise HistoricalStoreError("refresh checkpoint record hash drifted")
    return {**record, "request": request, "record_sha256": supplied}


def _read_checkpoints(
    path: Path, *, manifest_sha256: str, store: HistoricalDayStore
) -> dict[str, dict[str, Any]]:
    if not path.exists():
        return {}
    records: dict[str, dict[str, Any]] = {}
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if not line:
            raise HistoricalStoreError(
                f"refresh checkpoint contains blank line {line_number}"
            )
        try:
            record = _validate_checkpoint(
                json.loads(line), manifest_sha256=manifest_sha256, store=store
            )
        except (json.JSONDecodeError, HistoricalStoreError) as exc:
            raise HistoricalStoreError(
                f"refresh checkpoint line {line_number}: {exc}"
            ) from exc
        identity = str(record["request_sha256"])
        if identity in records:
            raise HistoricalStoreError("refresh checkpoint request is duplicated")
        records[identity] = record
    return records


def _append_checkpoint(
    path: Path,
    *,
    manifest_sha256: str,
    request: Mapping[str, str],
    result: Mapping[str, Any],
    store: HistoricalDayStore,
) -> dict[str, Any]:
    canonical_path = store.path_for(request["symbol"], request["date"])
    relative = canonical_path.relative_to(store.root).as_posix()
    content = {
        "schema_version": REFRESH_SCHEMA_VERSION,
        "kind": REFRESH_KIND,
        "manifest_sha256": manifest_sha256,
        "request": dict(request),
        "request_sha256": _hash(dict(request)),
        "provider": str(result["provider"]),
        "rows": int(result["rows"]),
        "canonical_path": relative,
        "completed_at": datetime.now(UTC).isoformat(),
    }
    record = {**content, "record_sha256": _hash(content)}
    path.parent.mkdir(parents=True, exist_ok=True)
    line = _canonical(record) + b"\n"
    descriptor = os.open(path, os.O_APPEND | os.O_CREAT | os.O_RDWR, 0o600)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        os.write(descriptor, line)
        os.fsync(descriptor)
    finally:
        fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)
    return record


def _refresh(args: argparse.Namespace, store: HistoricalDayStore) -> dict[str, Any]:
    manifest, manifest_sha256 = _load_refresh_manifest(args.manifest)
    path = _checkpoint_path(store, manifest_sha256)
    lock_path = path.with_suffix(".lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(lock_path, os.O_APPEND | os.O_CREAT | os.O_RDWR, 0o600)
    try:
        fcntl.flock(descriptor, fcntl.LOCK_EX)
        return _run_refresh(
            args,
            store,
            manifest=manifest,
            manifest_sha256=manifest_sha256,
            path=path,
        )
    finally:
        fcntl.flock(descriptor, fcntl.LOCK_UN)
        os.close(descriptor)


def _run_refresh(
    args: argparse.Namespace,
    store: HistoricalDayStore,
    *,
    manifest: Mapping[str, Any],
    manifest_sha256: str,
    path: Path,
) -> dict[str, Any]:
    checkpoints = _read_checkpoints(
        path, manifest_sha256=manifest_sha256, store=store
    )
    expected = {_hash(request): request for request in manifest["requests"]}
    unknown = sorted(set(checkpoints) - set(expected))
    if unknown:
        raise HistoricalStoreError(
            "refresh checkpoint contains requests outside the frozen manifest"
        )

    results: list[dict[str, Any]] = []
    for identity, checkpoint in checkpoints.items():
        request = expected[identity]
        rows, client, attempts, _ = _cache_request(request, args.env_file, store)
        if client is None or rows is None:
            raise HistoricalStoreError(
                "checkpointed refresh request is no longer available from the "
                f"canonical store: {request}"
            )
        results.append(
            {
                "status": "checkpointed",
                "request": request,
                "provider": checkpoint["provider"],
                "rows": checkpoint["rows"],
                "canonical_path": str(
                    store.path_for(request["symbol"], request["date"])
                ),
                "attempts": _attempts(attempts, "cache_validation"),
            }
        )

    failures: list[dict[str, Any]] = []
    startup_attempts: list[dict[str, Any]] = []
    with ExitStack() as stack:
        providers: OpenProviderSet | None = None
        for request in manifest["requests"]:
            identity = _hash(request)
            if identity in checkpoints:
                continue
            rows, client, cache_attempts, cache_error = _cache_request(
                request, args.env_file, store
            )
            live_attempts: Sequence[Any] = []
            last_error = cache_error
            if client is None or rows is None:
                if providers is None:
                    providers = stack.enter_context(
                        open_provider_set(args.env_file, store)
                    )
                    startup_attempts = [
                        attempt.public_dict() for attempt in providers.startup_attempts
                    ]
                rows, client, live_attempts, last_error = _live_request(
                    request, providers
                )
            if client is None or rows is None:
                failure = {
                    "status": "failed",
                    "request": request,
                    "error_type": type(last_error).__name__
                    if last_error is not None
                    else "HistoricalProviderError",
                    "error": str(last_error)
                    if last_error is not None
                    else "no historical providers are configured",
                    "attempts": [
                        *_attempts(cache_attempts, "cache"),
                        *_attempts(live_attempts, "live"),
                    ],
                }
                failures.append(failure)
                results.append(failure)
                continue
            result = _success(
                request, store, rows, client, cache_attempts, live_attempts
            )
            _append_checkpoint(
                path,
                manifest_sha256=manifest_sha256,
                request=request,
                result=result,
                store=store,
            )
            results.append(result)

    final_checkpoints = _read_checkpoints(
        path, manifest_sha256=manifest_sha256, store=store
    )
    results.sort(key=lambda item: (item["request"]["date"], item["request"]["symbol"]))
    return {
        "operation": "refresh",
        "valid": not failures,
        "manifest": str(args.manifest.resolve()),
        "manifest_sha256": manifest_sha256,
        "requests": len(manifest["requests"]),
        "completed": len(final_checkpoints),
        "checkpointed_before": len(checkpoints),
        "failed": len(failures),
        "checkpoint_path": str(path),
        "provider_startup_attempts": startup_attempts,
        "results": results,
    }


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
        "symbol": normalize_symbol(args.symbol),
        "date": args.date,
        "results": results,
        "valid": all(row["status"] == "success" for row in results),
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
    refresh = subparsers.add_parser(
        "refresh", help="resume a neutral manifest of exact symbol/date requests"
    )
    refresh.add_argument("--manifest", type=Path, required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        store = HistoricalDayStore.from_env(args.env_file)
        if args.command == "check":
            result = store.audit()
        elif args.command == "check-providers":
            result = _provider_check(args, store)
        elif args.command == "refresh":
            result = _refresh(args, store)
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

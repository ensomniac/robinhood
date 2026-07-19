"""Migrate legacy daily market data into the canonical external history store.

The migration is idempotent and non-destructive. Every source file is recorded
by SHA-256 as migrated, retained non-daily evidence, or omitted non-data. Raw
source trees are not deleted; removal is a separate operation after audit.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
from collections import Counter, defaultdict
from datetime import date, datetime, time, timezone
from functools import lru_cache
from pathlib import Path
from typing import Any, Mapping, Sequence
from zoneinfo import ZoneInfo

from historical_store import (
    DEFAULT_ENV_PATH,
    HistoricalDayStore,
    HistoricalStoreError,
    build_context,
    build_dataset,
    compact_bar,
    compact_quote,
    normalize_date,
    normalize_symbol,
    provider_id,
    sha256_file,
)


PROJECT_ROOT = Path(__file__).resolve().parent
EASTERN = ZoneInfo("America/New_York")
UTC = timezone.utc
DEFAULT_LEGACY_INTRADAY_ROOT = Path("/Users/ensomniac/trade/data/intraday")
DEFAULT_REPO_HISTORY_ROOT = PROJECT_ROOT / "historical_data"
MIGRATION_SCHEMA_VERSION = 1
MIGRATION_ID = "robinhood-codex-canonical-day-v1"


class HistoricalMigrationError(RuntimeError):
    """Raised when a migration source cannot be accounted for safely."""


@lru_cache(maxsize=None)
def _source_sha256(path: Path) -> str:
    return sha256_file(path)


def _json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise HistoricalMigrationError(f"cannot read {path}: {exc}") from exc


def _source_provenance(
    path: Path,
    root: Path,
    *,
    captured_at: str | None = None,
    request: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    value: dict[str, Any] = {
        "migration_id": MIGRATION_ID,
        "source_root": root.name,
        "source_path": str(path.relative_to(root)),
        "source_sha256": _source_sha256(path),
    }
    if captured_at:
        value["captured_at"] = captured_at
    if request:
        value["request"] = dict(request)
    return value


def _day_code(value: str) -> str:
    try:
        year, month, day = (int(part) for part in value.split("_"))
        return date(year, month, day).isoformat()
    except (TypeError, ValueError) as exc:
        raise HistoricalMigrationError(f"invalid legacy day code: {value}") from exc


def _legacy_timeframe(interval: str, data: Mapping[str, Any]) -> str:
    mapping = {
        "minute": "1m",
        "three_min": "3m",
        "five_min": "5m",
        "thirty_min": "30m",
        "hour": "1h",
    }
    value = mapping.get(interval)
    if value is None:
        raw = str(data.get("bar_size") or interval).strip().lower()
        value = raw.replace(" mins", "m").replace(" min", "m").replace(" hour", "h")
    return value


def _provider_from_candidate(
    candidate: Mapping[str, Any], bundle: Mapping[str, Any]
) -> str:
    symbol = str(candidate.get("symbol", "")).upper()
    source = bundle.get("source")
    if isinstance(source, Mapping):
        by_symbol = source.get("candidate_provider_by_symbol")
        if isinstance(by_symbol, Mapping) and isinstance(by_symbol.get(symbol), str):
            return str(by_symbol[symbol])
    provenance = candidate.get("data_provenance")
    if isinstance(provenance, Mapping):
        for value in provenance.values():
            if isinstance(value, str) and value:
                return value
    return "replay_bundle"


def _quality_for_rows(
    rows: Sequence[Mapping[str, Any]],
    *,
    complete: bool | None,
    reported_complete: bool | None = None,
) -> dict[str, Any]:
    times = [str(row.get("t", "")) for row in rows if row.get("t")]
    quality: dict[str, Any] = {
        "complete": complete,
        "start_et": min(times) if times else None,
        "end_et": max(times) if times else None,
    }
    if reported_complete is not None:
        quality["reported_complete"] = reported_complete
    return quality


class PendingSymbol:
    def __init__(self, symbol: str):
        self.symbol = normalize_symbol(symbol)
        self.days: dict[str, dict[str, list[dict[str, Any]]]] = defaultdict(
            lambda: {"datasets": [], "contexts": []}
        )

    def dataset(self, day: str, value: Mapping[str, Any]) -> None:
        self.days[normalize_date(day)]["datasets"].append(dict(value))

    def context(self, day: str, value: Mapping[str, Any]) -> None:
        self.days[normalize_date(day)]["contexts"].append(dict(value))

    def flush(self, store: HistoricalDayStore) -> Counter[str]:
        counts: Counter[str] = Counter()
        for day, values in sorted(self.days.items()):
            result = store.merge(
                self.symbol,
                day,
                datasets=values["datasets"],
                contexts=values["contexts"],
            )
            counts["day_files_touched"] += 1
            counts["day_files_changed"] += int(bool(result["changed"]))
            counts["datasets_submitted"] += len(values["datasets"])
            counts["contexts_submitted"] += len(values["contexts"])
        return counts


class MigrationLedger:
    def __init__(self):
        self.rows: list[dict[str, Any]] = []
        self._paths: set[str] = set()

    def add(
        self,
        path: Path,
        root: Path,
        *,
        status: str,
        category: str,
        datasets: int = 0,
        contexts: int = 0,
        reason: str | None = None,
    ) -> None:
        identity = f"{root}:{path.relative_to(root)}"
        if identity in self._paths:
            return
        self._paths.add(identity)
        row: dict[str, Any] = {
            "source_root": str(root),
            "source_path": str(path.relative_to(root)),
            "source_sha256": _source_sha256(path),
            "bytes": path.stat().st_size,
            "status": status,
            "category": category,
            "datasets": datasets,
            "contexts": contexts,
        }
        if reason:
            row["reason"] = reason
        self.rows.append(row)

    def contains(self, path: Path, root: Path) -> bool:
        return f"{root}:{path.relative_to(root)}" in self._paths

    def write(self, root: Path, summary: Mapping[str, Any]) -> Path:
        output_root = root / "_migrations" / MIGRATION_ID
        output_root.mkdir(parents=True, exist_ok=True)
        ledger_path = output_root / "source-files.jsonl.gz"
        temporary = ledger_path.with_name(f".{ledger_path.name}.{os.getpid()}.tmp")
        with temporary.open("wb") as raw:
            with gzip.GzipFile(fileobj=raw, mode="wb", compresslevel=6, mtime=0) as stream:
                for row in sorted(
                    self.rows,
                    key=lambda value: (value["source_root"], value["source_path"]),
                ):
                    stream.write(
                        (json.dumps(row, sort_keys=True) + "\n").encode("utf-8")
                    )
        os.replace(temporary, ledger_path)
        summary_path = output_root / "summary.json"
        rendered = json.dumps(dict(summary), indent=2, sort_keys=True) + "\n"
        temporary_summary = summary_path.with_name(
            f".{summary_path.name}.{os.getpid()}.tmp"
        )
        temporary_summary.write_text(rendered, encoding="utf-8")
        os.replace(temporary_summary, summary_path)
        return summary_path


class LegacyMigrator:
    def __init__(
        self,
        store: HistoricalDayStore,
        legacy_intraday_root: Path,
        repo_history_root: Path,
    ):
        self.store = store
        self.legacy_intraday_root = legacy_intraday_root.resolve()
        self.repo_history_root = repo_history_root.resolve()
        self.ledger = MigrationLedger()
        self.counts: Counter[str] = Counter()

    def _add_legacy_intraday(self, pending: PendingSymbol, path: Path) -> None:
        relative = path.relative_to(self.legacy_intraday_root)
        if len(relative.parts) != 4:
            raise HistoricalMigrationError(f"unexpected legacy path: {relative}")
        symbol, interval, day_code, filename = relative.parts
        if normalize_symbol(symbol) != pending.symbol:
            raise HistoricalMigrationError("legacy symbol index mismatch")
        day = _day_code(day_code)
        payload = _json(path)
        if not isinstance(payload, Mapping) or not isinstance(payload.get("data"), Mapping):
            raise HistoricalMigrationError(f"legacy payload is malformed: {path}")
        data = payload["data"]
        ticks = data.get("ticks")
        if not isinstance(ticks, list):
            raise HistoricalMigrationError(f"legacy ticks are missing: {path}")
        provenance = _source_provenance(
            path,
            self.legacy_intraday_root,
            captured_at=str(payload.get("last_processed") or "") or None,
            request={
                key: data[key]
                for key in ("start_date", "end_date", "bar_size", "what", "useRTH")
                if key in data
            },
        )
        reported_complete = payload.get("complete", payload.get("ticks_complete"))
        timeframe = _legacy_timeframe(interval, data)
        what = str(data.get("what") or "unknown").strip().lower()
        if filename == "ticks_composite.json":
            rows = []
            for row in ticks:
                if not isinstance(row, Mapping):
                    raise HistoricalMigrationError(f"malformed composite row: {path}")
                compact = compact_bar(
                    {
                        "t": row.get("t"),
                        "open": row.get("trades", {}).get("o"),
                        "high": row.get("trades", {}).get("h"),
                        "low": row.get("trades", {}).get("l"),
                        "close": row.get("trades", {}).get("c"),
                        "volume": row.get("trades", {}).get("v", 0),
                        "count": row.get("trades", {}).get("n", 0),
                        "wap": row.get("trades", {}).get("vwap", 0),
                        "legacy_composite": dict(row),
                    },
                    day=day,
                )
                rows.append(compact)
            dataset_kind = "derived"
            channel = "legacy_composite"
            limitations = (
                "Legacy composite intersects trade, midpoint, and bid/ask bars.",
                "Legacy spread_proxy is not a directly observed bid/ask spread.",
            )
        else:
            rows = [
                compact_bar(row, day=day)
                for row in ticks
                if isinstance(row, Mapping)
            ]
            if len(rows) != len(ticks):
                raise HistoricalMigrationError(f"malformed legacy bar row: {path}")
            dataset_kind = "bars"
            channel = what
            limitations = ()
            if what in {"bid_ask", "midpoint"}:
                limitations = (
                    "IBKR historical aggregate bar; not an individual quote event.",
                    "No top-of-book sizes are available in this legacy series.",
                )
        complete = bool(reported_complete)
        pending.dataset(
            day,
            build_dataset(
                kind=dataset_kind,
                provider="ibkr",
                rows=rows,
                channel=channel,
                timeframe=timeframe,
                feed="smart_legacy",
                adjustment="provider_default_unknown",
                session="regular" if int(data.get("useRTH", 1)) else "all",
                scope="full_session" if complete else "partial_session",
                quality=_quality_for_rows(
                    rows,
                    complete=complete,
                    reported_complete=bool(reported_complete),
                ),
                limitations=limitations,
                provenance=provenance,
            ),
        )
        metadata = {
            key: value for key, value in payload.items() if key != "data"
        }
        metadata["data"] = {
            key: value for key, value in data.items() if key != "ticks"
        }
        metadata["extracted_ticks"] = {
            "row_count": len(ticks),
            "content_sha256": hashlib.sha256(
                json.dumps(ticks, sort_keys=True, separators=(",", ":")).encode()
            ).hexdigest(),
        }
        pending.context(
            day,
            build_context(
                kind="legacy_intraday_capture",
                provider="ibkr",
                payload=metadata,
                observed_at=str(payload.get("last_processed") or "") or None,
                provenance=provenance,
            ),
        )
        self.ledger.add(
            path,
            self.legacy_intraday_root,
            status="migrated",
            category="legacy_iabapp_intraday",
            datasets=1,
            contexts=1,
        )
        self.counts["legacy_intraday_files"] += 1

    def _add_daily_rows(
        self,
        pending: PendingSymbol,
        rows: Sequence[Mapping[str, Any]],
        *,
        provider: str,
        channel: str,
        timeframe: str,
        scope: str,
        adjustment: str,
        provenance: Mapping[str, Any],
        limitations: Sequence[str] = (),
    ) -> int:
        added = 0
        for row in rows:
            day = str(row.get("date_et") or "")
            if not day:
                try:
                    day = datetime.fromtimestamp(int(row["epoch"]), UTC).astimezone(
                        EASTERN
                    ).date().isoformat()
                except (KeyError, TypeError, ValueError) as exc:
                    raise HistoricalMigrationError("historical bar lacks a date") from exc
            compact = compact_bar(row, day=day)
            pending.dataset(
                day,
                build_dataset(
                    kind="bars",
                    provider=provider,
                    rows=[compact],
                    channel=channel,
                    timeframe=timeframe,
                    feed="smart" if provider_id(provider) == "ibkr" else "unknown",
                    adjustment=adjustment,
                    session="regular",
                    scope=scope,
                    quality=_quality_for_rows([compact], complete=True),
                    limitations=limitations,
                    provenance=provenance,
                ),
            )
            added += 1
        return added

    def _add_ibkr_candidate(self, pending: PendingSymbol, path: Path) -> None:
        payload = _json(path)
        if not isinstance(payload, Mapping):
            raise HistoricalMigrationError(f"IBKR cache is malformed: {path}")
        request = payload.get("request") if isinstance(payload.get("request"), Mapping) else {}
        symbol = normalize_symbol(str(request.get("symbol") or payload.get("symbol") or ""))
        if symbol != pending.symbol:
            raise HistoricalMigrationError("IBKR candidate symbol index mismatch")
        day = normalize_date(str(request.get("date") or payload.get("date") or ""))
        provider = str(payload.get("provider") or "Interactive Brokers TWS API")
        captured_at = str(payload.get("captured_at") or "") or None
        provenance = _source_provenance(
            path,
            self.repo_history_root,
            captured_at=captured_at,
            request=request,
        )
        dataset_count = 0
        session_rows = payload.get("session_bars")
        if isinstance(session_rows, list):
            compact_rows = [
                compact_bar(row, day=day)
                for row in session_rows
                if isinstance(row, Mapping)
            ]
            if len(compact_rows) != len(session_rows):
                raise HistoricalMigrationError(f"malformed session bar: {path}")
            quality = payload.get("session_bar_quality")
            complete = (
                bool(quality.get("complete"))
                if isinstance(quality, Mapping) and "complete" in quality
                else len(compact_rows) == 390
            )
            pending.dataset(
                day,
                build_dataset(
                    kind="bars",
                    provider=provider,
                    rows=compact_rows,
                    channel="trades",
                    timeframe="1m",
                    feed="smart",
                    adjustment="provider_adjusted_unknown_basis",
                    session="regular",
                    scope="full_session" if complete else "partial_session",
                    quality={
                        **_quality_for_rows(compact_rows, complete=complete),
                        **(dict(quality) if isinstance(quality, Mapping) else {}),
                    },
                    limitations=tuple(payload.get("limitations") or ()),
                    provenance=provenance,
                ),
            )
            dataset_count += 1
        daily_rows = payload.get("daily_bars")
        if isinstance(daily_rows, list):
            dataset_count += self._add_daily_rows(
                pending,
                [row for row in daily_rows if isinstance(row, Mapping)],
                provider=provider,
                channel="trades",
                timeframe="1d",
                scope="daily_summary",
                adjustment="provider_adjusted_unknown_basis",
                provenance=provenance,
                limitations=tuple(payload.get("limitations") or ()),
            )
        opening_rows = payload.get("prior_opening_bars")
        if isinstance(opening_rows, list):
            dataset_count += self._add_daily_rows(
                pending,
                [row for row in opening_rows if isinstance(row, Mapping)],
                provider=provider,
                channel="trades",
                timeframe="5m",
                scope="opening_window_only",
                adjustment="provider_adjusted_unknown_basis",
                provenance=provenance,
                limitations=tuple(payload.get("limitations") or ()),
            )
        quote_rows = payload.get("bid_ask_ticks")
        if isinstance(quote_rows, list):
            compact_rows = [
                compact_quote(row, day=day)
                for row in quote_rows
                if isinstance(row, Mapping)
            ]
            if len(compact_rows) != len(quote_rows):
                raise HistoricalMigrationError(f"malformed quote row: {path}")
            pending.dataset(
                day,
                build_dataset(
                    kind="quotes",
                    provider=provider,
                    rows=compact_rows,
                    channel="top_of_book",
                    feed="smart",
                    adjustment="provider_adjusted_unknown_basis",
                    session="regular",
                    scope="observed_window",
                    quality=_quality_for_rows(compact_rows, complete=None),
                    limitations=tuple(payload.get("limitations") or ()),
                    provenance=provenance,
                ),
            )
            dataset_count += 1
        stripped = {
            key: value
            for key, value in payload.items()
            if key
            not in {
                "session_bars",
                "daily_bars",
                "prior_opening_bars",
                "bid_ask_ticks",
            }
        }
        stripped["extracted"] = {
            "session_bars": len(session_rows) if isinstance(session_rows, list) else 0,
            "daily_bars": len(daily_rows) if isinstance(daily_rows, list) else 0,
            "prior_opening_bars": len(opening_rows) if isinstance(opening_rows, list) else 0,
            "bid_ask_ticks": len(quote_rows) if isinstance(quote_rows, list) else 0,
        }
        pending.context(
            day,
            build_context(
                kind="candidate_market_evidence",
                provider=provider,
                payload=stripped,
                observed_at=captured_at,
                provenance=provenance,
            ),
        )
        self.ledger.add(
            path,
            self.repo_history_root,
            status="migrated",
            category="candidate_market_evidence",
            datasets=dataset_count,
            contexts=1,
        )
        self.counts["ibkr_candidate_files"] += 1

    def _add_preflight(self, pending: PendingSymbol, path: Path) -> None:
        payload = _json(path)
        if not isinstance(payload, Mapping):
            raise HistoricalMigrationError(f"preflight payload is malformed: {path}")
        symbol = normalize_symbol(str(payload.get("symbol") or path.stem))
        if symbol != pending.symbol:
            raise HistoricalMigrationError("preflight symbol index mismatch")
        day = normalize_date(str(payload.get("session_date") or path.parent.name))
        provider = str(payload.get("provider") or "Interactive Brokers TWS API")
        captured_at = str(payload.get("captured_at") or "") or None
        provenance = _source_provenance(
            path,
            self.repo_history_root,
            captured_at=captured_at,
        )
        history = payload.get("pre_session_history")
        dataset_count = 0
        if isinstance(history, Mapping):
            daily_rows = history.get("daily_bars")
            if isinstance(daily_rows, list):
                dataset_count += self._add_daily_rows(
                    pending,
                    [row for row in daily_rows if isinstance(row, Mapping)],
                    provider=provider,
                    channel="trades",
                    timeframe="1d",
                    scope="daily_summary",
                    adjustment="provider_adjusted_unknown_basis",
                    provenance=provenance,
                )
            opening_rows = history.get("prior_opening_bars")
            if isinstance(opening_rows, list):
                dataset_count += self._add_daily_rows(
                    pending,
                    [row for row in opening_rows if isinstance(row, Mapping)],
                    provider=provider,
                    channel="trades",
                    timeframe="5m",
                    scope="opening_window_only",
                    adjustment="provider_adjusted_unknown_basis",
                    provenance=provenance,
                )
        stripped = {key: value for key, value in payload.items() if key != "pre_session_history"}
        stripped["extracted_pre_session_history"] = {
            "present": isinstance(history, Mapping),
            "content_sha256": (
                hashlib.sha256(
                    json.dumps(history, sort_keys=True, separators=(",", ":")).encode()
                ).hexdigest()
                if isinstance(history, Mapping)
                else None
            ),
            "daily_bars": (
                len(history.get("daily_bars", [])) if isinstance(history, Mapping) else 0
            ),
            "prior_opening_bars": (
                len(history.get("prior_opening_bars", []))
                if isinstance(history, Mapping)
                else 0
            ),
        }
        pending.context(
            day,
            build_context(
                kind="historical_preflight",
                provider=provider,
                payload=stripped,
                observed_at=captured_at,
                provenance=provenance,
            ),
        )
        self.ledger.add(
            path,
            self.repo_history_root,
            status="migrated",
            category="historical_preflight",
            datasets=dataset_count,
            contexts=1,
        )
        self.counts["preflight_files"] += 1

    def migrate_high_volume_sources(self) -> None:
        by_symbol: dict[str, dict[str, list[Path]]] = defaultdict(
            lambda: {"legacy": [], "ibkr": [], "preflight": []}
        )
        if self.legacy_intraday_root.is_dir():
            for path in self.legacy_intraday_root.glob("*/*/*/*.json"):
                by_symbol[normalize_symbol(path.relative_to(self.legacy_intraday_root).parts[0])][
                    "legacy"
                ].append(path)
        ibkr_root = self.repo_history_root / "ibkr"
        if ibkr_root.is_dir():
            for path in ibkr_root.glob("*.json"):
                rest = path.stem[11:]
                if rest.endswith("-benchmark"):
                    rest = rest[: -len("-benchmark")]
                by_symbol[normalize_symbol(rest)]["ibkr"].append(path)
        preflight_root = self.repo_history_root / "preflight"
        if preflight_root.is_dir():
            for path in preflight_root.glob("*/*.json"):
                by_symbol[normalize_symbol(path.stem)]["preflight"].append(path)
        for index, (symbol, paths) in enumerate(sorted(by_symbol.items()), start=1):
            pending = PendingSymbol(symbol)
            for path in sorted(paths["legacy"]):
                self._add_legacy_intraday(pending, path)
            for path in sorted(paths["ibkr"]):
                self._add_ibkr_candidate(pending, path)
            for path in sorted(paths["preflight"]):
                self._add_preflight(pending, path)
            self.counts.update(pending.flush(self.store))
            if index % 50 == 0 or index == len(by_symbol):
                print(
                    json.dumps(
                        {
                            "migration": MIGRATION_ID,
                            "symbols_complete": index,
                            "symbols_total": len(by_symbol),
                        }
                    ),
                    flush=True,
                )

    def migrate_replay_bundles(self) -> None:
        for path in sorted(self.repo_history_root.glob("????-??-??.json")):
            bundle = _json(path)
            if not isinstance(bundle, Mapping):
                raise HistoricalMigrationError(f"replay bundle is malformed: {path}")
            day = normalize_date(str(bundle.get("date") or path.stem))
            candidates = bundle.get("candidates")
            if not isinstance(candidates, list):
                raise HistoricalMigrationError(f"replay bundle lacks candidates: {path}")
            provenance = _source_provenance(
                path,
                self.repo_history_root,
                captured_at=(
                    str(bundle.get("source", {}).get("captured_at") or "") or None
                    if isinstance(bundle.get("source"), Mapping)
                    else None
                ),
            )
            dataset_count = 0
            context_count = 0
            bundle_context = {
                key: value for key, value in bundle.items() if key != "candidates"
            }
            for candidate in candidates:
                if not isinstance(candidate, Mapping):
                    raise HistoricalMigrationError(f"malformed bundle candidate: {path}")
                symbol = normalize_symbol(str(candidate.get("symbol") or ""))
                provider = _provider_from_candidate(candidate, bundle)
                bars = candidate.get("bars")
                datasets = []
                if isinstance(bars, list):
                    compact_rows = []
                    for row in bars:
                        if not isinstance(row, Mapping):
                            raise HistoricalMigrationError(f"malformed bundle bar: {path}")
                        enriched = dict(row)
                        if "time_et" in enriched and "T" not in str(enriched["time_et"]):
                            enriched["time_et"] = datetime.combine(
                                date.fromisoformat(day),
                                time.fromisoformat(str(enriched["time_et"])),
                                tzinfo=EASTERN,
                            ).isoformat()
                        compact_rows.append(compact_bar(enriched, day=day))
                    datasets.append(
                        build_dataset(
                            kind="bars",
                            provider=provider,
                            rows=compact_rows,
                            channel="trades",
                            timeframe="1m",
                            feed="provider_replay",
                            adjustment="split_adjusted",
                            session="regular",
                            scope="full_session",
                            quality=_quality_for_rows(
                                compact_rows, complete=len(compact_rows) == 390
                            ),
                            provenance=provenance,
                        )
                    )
                    dataset_count += 1
                stripped = {key: value for key, value in candidate.items() if key != "bars"}
                stripped["replay_bundle"] = bundle_context
                stripped["extracted_bars"] = len(bars) if isinstance(bars, list) else 0
                context = build_context(
                    kind="replay_candidate",
                    provider=provider,
                    payload=stripped,
                    observed_at=(
                        str(bundle.get("source", {}).get("captured_at") or "") or None
                        if isinstance(bundle.get("source"), Mapping)
                        else None
                    ),
                    provenance=provenance,
                )
                self.store.merge(symbol, day, datasets=datasets, contexts=[context])
                context_count += 1
            self.ledger.add(
                path,
                self.repo_history_root,
                status="migrated",
                category="replay_bundle",
                datasets=dataset_count,
                contexts=context_count,
            )
            self.counts["replay_bundle_files"] += 1
            self.counts["replay_candidate_contexts"] += context_count

    def migrate_contract_contexts(self) -> None:
        root = self.repo_history_root / "contracts"
        if not root.is_dir():
            return
        for path in sorted(root.glob("*/*.json")):
            payload = _json(path)
            if not isinstance(payload, Mapping):
                raise HistoricalMigrationError(f"contract cache is malformed: {path}")
            request = payload.get("request")
            if not isinstance(request, Mapping):
                raise HistoricalMigrationError(f"contract request is missing: {path}")
            symbol = normalize_symbol(str(request.get("symbol") or path.stem))
            captured_at = str(payload.get("captured_at") or "")
            if not captured_at:
                raise HistoricalMigrationError(f"contract capture time is missing: {path}")
            day = datetime.fromisoformat(captured_at).astimezone(EASTERN).date().isoformat()
            provenance = _source_provenance(
                path,
                self.repo_history_root,
                captured_at=captured_at,
                request=request,
            )
            self.store.merge(
                symbol,
                day,
                contexts=[
                    build_context(
                        kind="security_contract",
                        provider=str(payload.get("provider") or "ibkr"),
                        payload=payload,
                        observed_at=captured_at,
                        provenance=provenance,
                    )
                ],
            )
            self.ledger.add(
                path,
                self.repo_history_root,
                status="migrated",
                category="security_contract",
                contexts=1,
            )
            self.counts["contract_contexts"] += 1

    def migrate_earnings_contexts(self) -> None:
        root = self.repo_history_root / "discovery"
        if not root.is_dir():
            return
        for path in sorted(root.glob("earnings*.json")):
            payload = _json(path)
            if not isinstance(payload, Mapping) or not isinstance(payload.get("events"), list):
                continue
            captured_at = str(payload.get("captured_at") or "") or None
            provenance = _source_provenance(
                path,
                self.repo_history_root,
                captured_at=captured_at,
            )
            migrated = 0
            for event in payload["events"]:
                if not isinstance(event, Mapping) or not isinstance(event.get("report"), Mapping):
                    continue
                symbol = str(event.get("symbol") or "").strip().upper()
                report_day = str(event["report"].get("date") or "")
                try:
                    symbol = normalize_symbol(symbol)
                    report_day = normalize_date(report_day)
                except HistoricalStoreError:
                    continue
                self.store.merge(
                    symbol,
                    report_day,
                    contexts=[
                        build_context(
                            kind="earnings_event",
                            provider=str(payload.get("provider") or "unknown"),
                            payload=dict(event),
                            observed_at=captured_at,
                            provenance=provenance,
                        )
                    ],
                )
                migrated += 1
            self.ledger.add(
                path,
                self.repo_history_root,
                status="migrated" if migrated else "retained",
                category="earnings_discovery",
                contexts=migrated,
                reason=None if migrated else "no compatible dated symbol events",
            )
            self.counts["earnings_contexts"] += migrated
        for path in sorted(root.glob("nasdaq-earnings-*.json")):
            payload = _json(path)
            data = payload.get("data") if isinstance(payload, Mapping) else None
            rows = data.get("rows") if isinstance(data, Mapping) else None
            if not isinstance(rows, list):
                continue
            report_day = normalize_date(path.stem.removeprefix("nasdaq-earnings-"))
            provenance = _source_provenance(path, self.repo_history_root)
            migrated = 0
            for row in rows:
                if not isinstance(row, Mapping):
                    continue
                try:
                    symbol = normalize_symbol(str(row.get("symbol") or ""))
                except HistoricalStoreError:
                    continue
                self.store.merge(
                    symbol,
                    report_day,
                    contexts=[
                        build_context(
                            kind="earnings_event",
                            provider="Nasdaq earnings calendar",
                            payload={"date": report_day, **dict(row)},
                            provenance=provenance,
                        )
                    ],
                )
                migrated += 1
            self.ledger.add(
                path,
                self.repo_history_root,
                status="migrated" if migrated else "retained",
                category="nasdaq_earnings_discovery",
                contexts=migrated,
                reason=None if migrated else "no compatible dated symbol events",
            )
            self.counts["earnings_contexts"] += migrated

    def account_remaining_files(self) -> None:
        for root in (self.legacy_intraday_root, self.repo_history_root):
            if not root.is_dir():
                continue
            for path in sorted(item for item in root.rglob("*") if item.is_file()):
                if self.ledger.contains(path, root):
                    continue
                if path.name == ".DS_Store":
                    self.ledger.add(
                        path,
                        root,
                        status="omitted_non_data",
                        category="filesystem_metadata",
                        reason="macOS Finder metadata is not historical evidence",
                    )
                    continue
                relative = path.relative_to(root)
                category = "retained_non_daily_artifact"
                if relative.parts and relative.parts[0] in {
                    "manifests",
                    "discovery",
                    "sec",
                }:
                    category = f"retained_{relative.parts[0]}_artifact"
                self.ledger.add(
                    path,
                    root,
                    status="retained",
                    category=category,
                    reason="not a canonical per-symbol daily observation; source remains unchanged",
                )

    def run(self) -> dict[str, Any]:
        started_at = datetime.now(UTC)
        self.migrate_high_volume_sources()
        self.migrate_replay_bundles()
        self.migrate_contract_contexts()
        self.migrate_earnings_contexts()
        self.account_remaining_files()
        provider_alias_repair = self.store.repair_provider_aliases()
        audit = self.store.audit()
        source_counts = Counter(row["status"] for row in self.ledger.rows)
        source_bytes = Counter()
        for row in self.ledger.rows:
            source_bytes[row["status"]] += int(row["bytes"])
        summary = {
            "schema_version": MIGRATION_SCHEMA_VERSION,
            "migration_id": MIGRATION_ID,
            "started_at": started_at.isoformat(),
            "completed_at": datetime.now(UTC).isoformat(),
            "non_destructive": True,
            "source_roots": {
                "legacy_intraday": str(self.legacy_intraday_root),
                "repo_history": str(self.repo_history_root),
            },
            "source_files": len(self.ledger.rows),
            "source_status_counts": dict(sorted(source_counts.items())),
            "source_status_bytes": dict(sorted(source_bytes.items())),
            "migration_counts": dict(sorted(self.counts.items())),
            "provider_alias_repair": provider_alias_repair,
            "store_audit": audit,
            "valid": audit["valid"] and len(self.ledger.rows) > 0,
        }
        summary_path = self.ledger.write(self.store.root, summary)
        summary["local_summary_path"] = str(summary_path)
        return summary


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env-file", type=Path, default=DEFAULT_ENV_PATH)
    parser.add_argument(
        "--legacy-intraday-root",
        type=Path,
        default=DEFAULT_LEGACY_INTRADAY_ROOT,
    )
    parser.add_argument(
        "--repo-history-root",
        type=Path,
        default=DEFAULT_REPO_HISTORY_ROOT,
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        store = HistoricalDayStore.from_env(args.env_file)
        result = LegacyMigrator(
            store,
            args.legacy_intraday_root,
            args.repo_history_root,
        ).run()
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0 if result["valid"] else 1
    except (HistoricalMigrationError, HistoricalStoreError, OSError) as exc:
        print(
            json.dumps({"error": str(exc), "error_type": type(exc).__name__}, indent=2),
            file=os.sys.stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

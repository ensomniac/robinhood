"""Freeze and collect the exact selected-candidate scanner-replay join.

This workflow consumes the already-inspected dynamic 09:35 scanner output.  It
does not select new symbols, invent a strategy variant, or promote a rule.  The
exact security-date list and row-level artifacts remain outside the public
repository under ``LOCAL_HISTORICAL_DATA_ROOT``; public files contain only
hashes, counts, coverage, and explicit fidelity blockers.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import io
import json
import os
import shutil
import sys
import time
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from datetime import date, datetime, time as wall_time, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, TypeVar
from zoneinfo import ZoneInfo

from historical_providers import (
    AlpacaConfig,
    AlpacaHistoricalClient,
    HistoricalProviderError,
)
from historical_service import RecordingHistoricalClient
from historical_service import LocalHistoricalClient
from historical_store import (
    HistoricalDayStore,
    HistoricalStoreConfig,
    build_context,
    expand_bar,
)
from learning_data import (
    LearningDataError,
    freeze_dataset_contract,
    load_frozen_dataset_contract,
)


PROJECT_ROOT = Path(__file__).resolve().parent
EASTERN = ZoneInfo("America/New_York")
UTC = timezone.utc
DATASET_ID = "dataset-selected-candidate-join-2026-07-19-v1"
DEFAULT_SOURCE_SUMMARY = (
    PROJECT_ROOT / "research_results" / "2026-07-19-scanner-replay.json"
)
DEFAULT_OUTPUT_ROOT = (
    PROJECT_ROOT / "historical_batches" / "selected_candidate_join" / "manifests"
)
DEFAULT_PUBLIC_STATUS = (
    PROJECT_ROOT / "historical_batches" / "selected_candidate_join" / "collection-status.json"
)
DEFAULT_PUBLIC_RESULT = (
    PROJECT_ROOT / "research_results" / "2026-07-19-selected-candidate-join.json"
)
BENCHMARKS = ("SPY", "QQQ")
MINIMUM_FREE_BYTES = 10 * 1024**3
NEWS_LOOKBACK_DAYS = 4
DEFAULT_MINIMUM_INTERVAL_SECONDS = 0.32
T = TypeVar("T")


class SelectedCandidateJoinError(RuntimeError):
    """A frozen-input, collection, or fidelity failure."""

    def __init__(self, message: str, *, category: str = "fidelity"):
        super().__init__(message)
        self.category = category
        self.retryable = category.startswith("retryable_")


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")


def _sha256_json(value: Any) -> str:
    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _read_object(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SelectedCandidateJoinError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise SelectedCandidateJoinError(f"{path} must contain an object")
    return value


def _read_gzip_object(path: Path) -> dict[str, Any]:
    try:
        with gzip.open(path, "rt", encoding="utf-8") as source:
            value = json.load(source)
    except (OSError, json.JSONDecodeError) as exc:
        raise SelectedCandidateJoinError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise SelectedCandidateJoinError(f"{path} must contain an object")
    return value


def _write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    rendered = json.dumps(value, indent=2, sort_keys=True) + "\n"
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(rendered, encoding="utf-8")
    os.replace(temporary, path)


def _gzip_bytes(value: Any) -> bytes:
    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode="wb", compresslevel=6, mtime=0) as stream:
        stream.write(json.dumps(value, indent=2, sort_keys=True).encode("utf-8"))
        stream.write(b"\n")
    return buffer.getvalue()


def _write_gzip_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_bytes(_gzip_bytes(value))
    os.replace(temporary, path)


def _timestamp_now() -> str:
    return datetime.now(UTC).isoformat()


def _repo_path(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(PROJECT_ROOT))
    except ValueError as exc:
        raise SelectedCandidateJoinError(
            f"public evidence path must stay in the repository: {path}"
        ) from exc


def _private_root(store_root: Path, dataset_id: str) -> Path:
    return store_root / "_derived" / "selected_candidate_join" / dataset_id


def _private_selection_path(store_root: Path, dataset_id: str) -> Path:
    return _private_root(store_root, dataset_id) / "selected-pairs.json.gz"


def _private_status_path(store_root: Path, dataset_id: str) -> Path:
    return _private_root(store_root, dataset_id) / "collection-status.json"


def _private_trigger_path(store_root: Path, dataset_id: str) -> Path:
    return _private_root(store_root, dataset_id) / "trigger-index.json.gz"


def _private_tape_path(store_root: Path, dataset_id: str) -> Path:
    return _private_root(store_root, dataset_id) / "trigger-tape-index.json.gz"


def _load_scanner_selection(
    summary_path: Path,
) -> tuple[dict[str, Any], dict[str, Any]]:
    summary = _read_object(summary_path)
    required = {
        "status": "READY",
        "selection_time_et": "09:35:00",
        "information_cutoff": "TARGET_SESSION_09:35_ET",
        "complete_universe": True,
        "selection_is_dynamic": True,
    }
    for field, expected in required.items():
        if summary.get(field) != expected:
            raise SelectedCandidateJoinError(
                f"scanner summary {field} must be {expected!r}"
            )
    source_id = str(summary.get("dataset_id") or "")
    if source_id != "dataset-production-scanner-replay-2026-07-19-v4":
        raise SelectedCandidateJoinError("unexpected scanner source dataset")
    detailed = summary.get("detailed_artifact")
    if not isinstance(detailed, Mapping) or detailed.get("public") is not False:
        raise SelectedCandidateJoinError("scanner detail privacy contract is missing")
    detail_path = PROJECT_ROOT / str(detailed.get("local_path") or "")
    if not detail_path.is_file() or _sha256_file(detail_path) != detailed.get("sha256"):
        raise SelectedCandidateJoinError("scanner detail hash does not match summary")
    detail = _read_object(detail_path)
    for field in (
        "scanner_rules_sha256",
        "security_master_sha256",
        "split_actions_sha256",
    ):
        if detail.get(field) != summary.get(field):
            raise SelectedCandidateJoinError(f"scanner detail {field} mismatch")
    dates = summary.get("dates")
    detail_dates = detail.get("dates")
    if not isinstance(dates, list) or not isinstance(detail_dates, Mapping):
        raise SelectedCandidateJoinError("scanner dates are malformed")
    pairs: list[dict[str, Any]] = []
    daily_hashes: list[dict[str, Any]] = []
    for public_day in dates:
        if not isinstance(public_day, Mapping):
            raise SelectedCandidateJoinError("scanner public date must be an object")
        day = str(public_day.get("date") or "")
        date.fromisoformat(day)
        raw_day = detail_dates.get(day)
        if not isinstance(raw_day, Mapping):
            raise SelectedCandidateJoinError(f"scanner detail lacks {day}")
        symbols = raw_day.get("selected_symbols")
        evaluations = raw_day.get("evaluations")
        if not isinstance(symbols, list) or not isinstance(evaluations, list):
            raise SelectedCandidateJoinError(f"scanner selected rows are malformed for {day}")
        by_symbol = {
            str(row.get("symbol")): row
            for row in evaluations
            if isinstance(row, Mapping) and isinstance(row.get("symbol"), str)
        }
        shortlist: list[dict[str, Any]] = []
        for symbol in symbols:
            row = by_symbol.get(str(symbol))
            if row is None or row.get("disposition") != "eligible":
                raise SelectedCandidateJoinError(
                    f"scanner selection is not eligible for {day}"
                )
            rank = int(row["opening_rvol_rank"])
            selected = {
                "date": day,
                "symbol": str(symbol),
                "instrument_id": str(row["instrument_id"]),
                "primary_exchange": str(row["primary_exchange"]),
                "rank": rank,
                "scanner_fields": {
                    key: row[key]
                    for key in (
                        "open_price",
                        "opening_high",
                        "opening_low",
                        "opening_close",
                        "opening_volume",
                        "opening_relative_volume",
                        "opening_return",
                        "average_daily_volume_14",
                        "daily_atr_14",
                        "prior_close",
                    )
                },
            }
            pairs.append(selected)
            shortlist.append(
                {
                    "symbol": selected["symbol"],
                    "instrument_id": selected["instrument_id"],
                    "opening_relative_volume": selected["scanner_fields"][
                        "opening_relative_volume"
                    ],
                    "opening_return": selected["scanner_fields"]["opening_return"],
                    "rank": rank,
                }
            )
        if len(symbols) != int(public_day.get("shortlist_count", -1)):
            raise SelectedCandidateJoinError(f"scanner shortlist count mismatch for {day}")
        observed_hash = _sha256_json(shortlist)
        if observed_hash != public_day.get("shortlist_sha256"):
            raise SelectedCandidateJoinError(f"scanner shortlist hash mismatch for {day}")
        daily_hashes.append(
            {
                "date": day,
                "shortlist_count": len(shortlist),
                "shortlist_sha256": observed_hash,
            }
        )
    requested = [str(item) for item in summary.get("requested_dates", [])]
    if requested != [item["date"] for item in daily_hashes]:
        raise SelectedCandidateJoinError("scanner requested-date order mismatch")
    private = {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "source_dataset_id": source_id,
        "source_summary_sha256": _sha256_file(summary_path),
        "source_detail_sha256": str(detailed["sha256"]),
        "selection_time_et": "09:35:00",
        "information_cutoff": "TARGET_SESSION_09:35_ET",
        "selected_pair_count": len(pairs),
        "selected_pairs": pairs,
    }
    public = {
        "source_dataset_id": source_id,
        "source_summary_sha256": _sha256_file(summary_path),
        "source_detail_sha256": str(detailed["sha256"]),
        "requested_dates": requested,
        "selected_pair_count": len(pairs),
        "daily_shortlists": daily_hashes,
        "private_selection_content_sha256": _sha256_json(private),
    }
    return private, public


def freeze_join(
    *,
    summary_path: Path,
    env_path: Path,
    output_root: Path,
) -> tuple[Path, dict[str, Any]]:
    config = HistoricalStoreConfig.from_env(env_path)
    private, public = _load_scanner_selection(summary_path)
    selection_path = _private_selection_path(config.root, DATASET_ID)
    if selection_path.exists():
        existing = _read_gzip_object(selection_path)
        if _sha256_json(existing) != public["private_selection_content_sha256"]:
            raise SelectedCandidateJoinError("private selected-pair artifact changed")
    else:
        _write_gzip_json(selection_path, private)
    free_bytes = shutil.disk_usage(config.root).free
    if free_bytes < MINIMUM_FREE_BYTES:
        raise SelectedCandidateJoinError(
            "historical store has less than the 10 GiB collection reserve",
            category="capacity",
        )
    contract = {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "registered_at": _timestamp_now(),
        "requested_dates": public["requested_dates"],
        "dataset_payload": {
            "lane": "development",
            "claim_scope": "DEVELOPMENT_ONLY",
            "status": "COLLECTING",
            "evidence_paths": [
                _repo_path(summary_path),
                "SCANNER_REPLAY.md",
                "STRATEGY_LEARNING_EXECUTION_PLAN.md",
            ],
            "inspected": False,
        },
        "selection_contract": public,
        "collection_contract": {
            "collector_sha256": _sha256_file(Path(__file__)),
            "provider": "Alpaca Market Data API",
            "feed": "sip",
            "adjustment": "raw",
            "symbol_mapping": "asof=-; point-in-time scanner identity remains authoritative",
            "candidate_bars": "1Min regular session 09:30-16:00 ET",
            "benchmarks": list(BENCHMARKS),
            "benchmark_bars": "1Min regular session 09:30-16:00 ET",
            "news_window": (
                f"{NEWS_LOOKBACK_DAYS} calendar days before target through "
                "target 09:35 ET, created_at bounded"
            ),
            "news_role": "secondary catalyst discovery only; not verified catalyst",
            "trigger_tape": (
                "raw SIP trades for the first 1Min bar whose high crosses the "
                "opening-range high, then SIP top-of-book around the first observed trade cross"
            ),
            "raw_and_symbol_rows_public": False,
            "canonical_store_required": True,
            "provider_switching_allowed": False,
            "substitutions_allowed": False,
            "source_corpus_already_inspected": True,
            "alpha_or_confirmation_claim_allowed": False,
        },
        "capacity_contract": {
            "minimum_free_bytes": MINIMUM_FREE_BYTES,
            "observed_free_bytes_at_freeze": free_bytes,
            "pilot_required_before_bulk_collection": True,
        },
    }
    return freeze_dataset_contract(contract, output_root)


def _load_private_selection(
    manifest: Mapping[str, Any], store_root: Path
) -> dict[str, Any]:
    dataset_id = str(manifest["dataset_id"])
    selection = _read_gzip_object(_private_selection_path(store_root, dataset_id))
    expected = manifest["selection_contract"]["private_selection_content_sha256"]
    if _sha256_json(selection) != expected:
        raise SelectedCandidateJoinError("private selection no longer matches manifest")
    if int(selection.get("selected_pair_count", -1)) != int(
        manifest["selection_contract"]["selected_pair_count"]
    ):
        raise SelectedCandidateJoinError("private selected-pair count changed")
    return selection


def _load_join_contract(path: Path, store_root: Path) -> tuple[dict[str, Any], dict[str, Any]]:
    try:
        manifest = load_frozen_dataset_contract(path)
    except LearningDataError as exc:
        raise SelectedCandidateJoinError(str(exc)) from exc
    if manifest.get("dataset_id") != DATASET_ID:
        raise SelectedCandidateJoinError("unexpected selected-candidate dataset id")
    if manifest["collection_contract"].get("collector_sha256") != _sha256_file(
        Path(__file__)
    ):
        raise SelectedCandidateJoinError("collector no longer matches frozen contract")
    return manifest, _load_private_selection(manifest, store_root)


def _session(day: str) -> tuple[datetime, datetime]:
    parsed = date.fromisoformat(day)
    return (
        datetime.combine(parsed, wall_time(9, 30), tzinfo=EASTERN),
        datetime.combine(parsed, wall_time(16, 0), tzinfo=EASTERN),
    )


def _rate_interval(env_path: Path) -> float:
    raw = os.environ.get("ALPACA_MINIMUM_INTERVAL_SECONDS")
    if raw is None and env_path.exists():
        from dotenv import dotenv_values

        raw = dotenv_values(env_path, interpolate=False).get(
            "ALPACA_MINIMUM_INTERVAL_SECONDS"
        )
    try:
        value = float(raw or DEFAULT_MINIMUM_INTERVAL_SECONDS)
    except (TypeError, ValueError) as exc:
        raise SelectedCandidateJoinError(
            "ALPACA_MINIMUM_INTERVAL_SECONDS must be numeric"
        ) from exc
    if value < 0:
        raise SelectedCandidateJoinError(
            "ALPACA_MINIMUM_INTERVAL_SECONDS must be nonnegative"
        )
    return value


class _RateGate:
    def __init__(self, interval: float):
        self.interval = interval
        self.last = 0.0

    def wait(self) -> None:
        remaining = self.interval - (time.monotonic() - self.last)
        if remaining > 0:
            time.sleep(remaining)
        self.last = time.monotonic()


def _retry(gate: _RateGate, operation: Callable[[], T]) -> T:
    last: Exception | None = None
    for attempt in range(1, 4):
        gate.wait()
        try:
            return operation()
        except HistoricalProviderError as exc:
            last = exc
            if not exc.retryable or attempt == 3:
                raise
            time.sleep(float(2 ** (attempt - 1)))
    assert last is not None
    raise last


def _alpaca_bar(store: HistoricalDayStore, symbol: str, day: str) -> dict[str, Any] | None:
    return store.select_dataset(
        symbol,
        day,
        kind="bars",
        channel="trades",
        timeframe="1m",
        providers=("alpaca",),
        require_complete=True,
        feed="sip",
        adjustment="raw",
    )


def _ensure_bar(
    recorder: RecordingHistoricalClient,
    store: HistoricalDayStore,
    gate: _RateGate,
    symbol: str,
    day: str,
) -> tuple[int, bool]:
    cached = _alpaca_bar(store, symbol, day)
    if cached is not None:
        return int(cached["quality"]["row_count"]), True
    start, end = _session(day)
    rows = _retry(
        gate,
        lambda: recorder.fetch_bars(
            symbol, start, end, bar_size="1 min", what="TRADES", use_rth=True
        ),
    )
    if not rows:
        raise SelectedCandidateJoinError(
            "full-session Alpaca SIP request returned no bars",
            category="permanent_fidelity",
        )
    stored = _alpaca_bar(store, symbol, day)
    if stored is None:
        raise SelectedCandidateJoinError("successful bar request was not canonicalized")
    return len(rows), False


def _news_context_exists(
    store: HistoricalDayStore, symbol: str, day: str, dataset_id: str
) -> bool:
    document = store.load(symbol, day)
    if document is None:
        return False
    return any(
        context.get("kind") == "selected_candidate_catalyst_candidates"
        and context.get("provider") == "alpaca"
        and context.get("payload", {}).get("dataset_id") == dataset_id
        for context in document["contexts"]
    )


def _collect_news_for_day(
    client: AlpacaHistoricalClient,
    store: HistoricalDayStore,
    gate: _RateGate,
    *,
    dataset_id: str,
    day: str,
    symbols: Sequence[str],
) -> tuple[int, bool]:
    if all(_news_context_exists(store, symbol, day, dataset_id) for symbol in symbols):
        return 0, True
    parsed = date.fromisoformat(day)
    cutoff = datetime.combine(parsed, wall_time(9, 35), tzinfo=EASTERN)
    start = datetime.combine(
        parsed - timedelta(days=NEWS_LOOKBACK_DAYS), wall_time(0), tzinfo=EASTERN
    )
    articles = _retry(gate, lambda: client.fetch_news(symbols, start, cutoff))
    for symbol in symbols:
        matching = [row for row in articles if symbol in row.get("symbols", [])]
        context = build_context(
            kind="selected_candidate_catalyst_candidates",
            provider="alpaca",
            observed_at=cutoff.isoformat(),
            payload={
                "dataset_id": dataset_id,
                "window_start": start.isoformat(),
                "information_cutoff": cutoff.isoformat(),
                "article_count": len(matching),
                "articles": matching,
                "evidence_role": "secondary_discovery_only",
                "verified_catalyst": False,
                "primary_source_required": True,
            },
            provenance={
                "source_type": "Alpaca historical news API",
                "feed_source": "Benzinga",
                "captured_at": _timestamp_now(),
            },
        )
        store.merge(symbol, day, contexts=[context])
    return len(articles), False


def _paths_size(paths: Sequence[Path]) -> int:
    return sum(path.stat().st_size for path in paths if path.exists())


def pilot(
    *,
    manifest_path: Path,
    env_path: Path,
    public_status_path: Path,
) -> dict[str, Any]:
    store = HistoricalDayStore.from_env(env_path)
    manifest, selection = _load_join_contract(manifest_path, store.root)
    pairs = selection["selected_pairs"]
    if not isinstance(pairs, list) or not pairs:
        raise SelectedCandidateJoinError("private selection is empty")
    pair = pairs[0]
    day = str(pair["date"])
    symbol = str(pair["symbol"])
    observed_paths = [store.path_for(item, day) for item in (symbol, *BENCHMARKS)]
    before = _paths_size(observed_paths)
    started = time.monotonic()
    config = AlpacaConfig.optional_from_env(env_path)
    if config is None:
        raise SelectedCandidateJoinError("Alpaca credentials are not configured")
    gate = _RateGate(_rate_interval(env_path))
    bar_rows = 0
    cache_hits = 0
    with AlpacaHistoricalClient(config) as client:
        recorder = RecordingHistoricalClient(client, store)
        for item in (symbol, *BENCHMARKS):
            count, cached = _ensure_bar(recorder, store, gate, item, day)
            bar_rows += count
            cache_hits += int(cached)
        articles, news_cached = _collect_news_for_day(
            client,
            store,
            gate,
            dataset_id=str(manifest["dataset_id"]),
            day=day,
            symbols=[symbol],
        )
    after = _paths_size(observed_paths)
    sample_bytes = max(after, before, 1)
    projected_upper_bytes = sample_bytes * (
        int(selection["selected_pair_count"]) + len(BENCHMARKS) * len(manifest["requested_dates"])
    )
    free_bytes = shutil.disk_usage(store.root).free
    passed = free_bytes - projected_upper_bytes >= int(
        manifest["capacity_contract"]["minimum_free_bytes"]
    )
    private_status = {
        "schema_version": 1,
        "dataset_id": manifest["dataset_id"],
        "phase": "PILOT_COMPLETE" if passed else "CAPACITY_BLOCKED",
        "updated_at": _timestamp_now(),
        "pilot": {
            "security_date_count": 1,
            "benchmark_date_count": len(BENCHMARKS),
            "bar_rows": bar_rows,
            "cache_hits": cache_hits,
            "news_cache_hit": news_cached,
            "article_count": articles,
            "sample_canonical_bytes": sample_bytes,
            "elapsed_seconds": time.monotonic() - started,
        },
        "capacity": {
            "free_bytes": free_bytes,
            "projected_upper_bytes": projected_upper_bytes,
            "minimum_reserve_bytes": MINIMUM_FREE_BYTES,
            "passed": passed,
        },
        "errors": [],
    }
    private_path = _private_status_path(store.root, str(manifest["dataset_id"]))
    _write_json(private_path, private_status)
    public = {
        "schema_version": 1,
        "dataset_id": manifest["dataset_id"],
        "manifest_sha256": manifest["manifest_sha256"],
        "status": private_status["phase"],
        "pilot": private_status["pilot"],
        "capacity": private_status["capacity"],
        "private_status_sha256": _sha256_file(private_path),
        "symbols_public": False,
        "raw_rows_public": False,
    }
    _write_json(public_status_path, public)
    if not passed:
        raise SelectedCandidateJoinError("capacity pilot failed", category="capacity")
    return public


def collect(
    *,
    manifest_path: Path,
    env_path: Path,
    public_status_path: Path,
) -> dict[str, Any]:
    store = HistoricalDayStore.from_env(env_path)
    manifest, selection = _load_join_contract(manifest_path, store.root)
    private_path = _private_status_path(store.root, str(manifest["dataset_id"]))
    if not private_path.exists():
        raise SelectedCandidateJoinError("capacity pilot must run before collection")
    previous = _read_object(private_path)
    if previous.get("capacity", {}).get("passed") is not True:
        raise SelectedCandidateJoinError("capacity pilot did not pass")
    config = AlpacaConfig.optional_from_env(env_path)
    if config is None:
        raise SelectedCandidateJoinError("Alpaca credentials are not configured")
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for pair in selection["selected_pairs"]:
        grouped[str(pair["date"])].append(dict(pair))
    gate = _RateGate(_rate_interval(env_path))
    counts: Counter[str] = Counter()
    errors: list[dict[str, Any]] = []
    started = time.monotonic()
    with AlpacaHistoricalClient(config) as client:
        recorder = RecordingHistoricalClient(client, store)
        for date_index, day in enumerate(manifest["requested_dates"], 1):
            pairs = grouped[str(day)]
            for pair in pairs:
                try:
                    row_count, cached = _ensure_bar(
                        recorder, store, gate, str(pair["symbol"]), str(day)
                    )
                    counts["candidate_bar_dates"] += 1
                    counts["candidate_bar_rows"] += row_count
                    counts["candidate_bar_cache_hits"] += int(cached)
                except (HistoricalProviderError, SelectedCandidateJoinError, OSError) as exc:
                    errors.append(
                        {
                            "date": day,
                            "symbol": pair["symbol"],
                            "kind": "candidate_bars",
                            "error_type": type(exc).__name__,
                            "category": getattr(exc, "category", "local_io"),
                            "error": str(exc),
                        }
                    )
            for benchmark in BENCHMARKS:
                try:
                    row_count, cached = _ensure_bar(
                        recorder, store, gate, benchmark, str(day)
                    )
                    counts["benchmark_bar_dates"] += 1
                    counts["benchmark_bar_rows"] += row_count
                    counts["benchmark_bar_cache_hits"] += int(cached)
                except (HistoricalProviderError, SelectedCandidateJoinError, OSError) as exc:
                    errors.append(
                        {
                            "date": day,
                            "symbol": benchmark,
                            "kind": "benchmark_bars",
                            "error_type": type(exc).__name__,
                            "category": getattr(exc, "category", "local_io"),
                            "error": str(exc),
                        }
                    )
            try:
                article_count, cached = _collect_news_for_day(
                    client,
                    store,
                    gate,
                    dataset_id=str(manifest["dataset_id"]),
                    day=str(day),
                    symbols=[str(pair["symbol"]) for pair in pairs],
                )
                counts["news_date_queries"] += 1
                counts["news_articles_returned"] += article_count
                counts["news_date_cache_hits"] += int(cached)
            except (HistoricalProviderError, SelectedCandidateJoinError, OSError) as exc:
                errors.append(
                    {
                        "date": day,
                        "kind": "news",
                        "error_type": type(exc).__name__,
                        "category": getattr(exc, "category", "local_io"),
                        "error": str(exc),
                    }
                )
            private = {
                **previous,
                "phase": "COLLECTING",
                "updated_at": _timestamp_now(),
                "dates_attempted": date_index,
                "counts": dict(sorted(counts.items())),
                "errors": errors,
                "elapsed_seconds": time.monotonic() - started,
            }
            _write_json(private_path, private)
            print(
                f"collected {date_index}/{len(manifest['requested_dates'])} dates; "
                f"errors={len(errors)}",
                flush=True,
            )
    expected_pairs = int(selection["selected_pair_count"])
    expected_benchmarks = len(BENCHMARKS) * len(manifest["requested_dates"])
    complete = (
        counts["candidate_bar_dates"] == expected_pairs
        and counts["benchmark_bar_dates"] == expected_benchmarks
        and counts["news_date_queries"] == len(manifest["requested_dates"])
        and not errors
    )
    private = {
        **previous,
        "phase": "BASE_JOIN_COMPLETE" if complete else "INCOMPLETE",
        "updated_at": _timestamp_now(),
        "dates_attempted": len(manifest["requested_dates"]),
        "counts": dict(sorted(counts.items())),
        "expected": {
            "candidate_bar_dates": expected_pairs,
            "benchmark_bar_dates": expected_benchmarks,
            "news_date_queries": len(manifest["requested_dates"]),
        },
        "errors": errors,
        "elapsed_seconds": time.monotonic() - started,
    }
    _write_json(private_path, private)
    public = {
        "schema_version": 1,
        "dataset_id": manifest["dataset_id"],
        "manifest_sha256": manifest["manifest_sha256"],
        "status": private["phase"],
        "counts": private["counts"],
        "expected": private["expected"],
        "error_count": len(errors),
        "error_categories": dict(
            sorted(Counter(str(item["category"]) for item in errors).items())
        ),
        "elapsed_seconds": private["elapsed_seconds"],
        "private_status_sha256": _sha256_file(private_path),
        "symbols_public": False,
        "raw_rows_public": False,
    }
    _write_json(public_status_path, public)
    return public


def _bar_clock(row: Mapping[str, Any]) -> wall_time:
    return datetime.fromisoformat(str(row["time_et"])).astimezone(EASTERN).time()


def _vwap(rows: Sequence[Mapping[str, Any]]) -> float | None:
    volume = sum(int(row.get("volume", 0)) for row in rows)
    if volume <= 0:
        return None
    weighted = 0.0
    for row in rows:
        row_volume = int(row.get("volume", 0))
        price = float(row.get("wap") or 0)
        if price <= 0:
            price = (
                float(row["high"]) + float(row["low"]) + float(row["close"])
            ) / 3
        weighted += price * row_volume
    return weighted / volume


def _benchmark_at(
    store: HistoricalDayStore, symbol: str, day: str, cutoff: datetime
) -> dict[str, Any] | None:
    dataset = _alpaca_bar(store, symbol, day)
    if dataset is None:
        return None
    rows = [expand_bar(row) for row in dataset["rows"]]
    completed = [
        row
        for row in rows
        if datetime.fromisoformat(row["time_et"]).astimezone(EASTERN) < cutoff
    ]
    if not completed:
        return None
    current_vwap = _vwap(completed)
    prior_vwap = _vwap(completed[:-5] or completed[:1])
    if current_vwap is None or prior_vwap is None:
        return None
    return {
        "last": float(completed[-1]["close"]),
        "vwap": current_vwap,
        "above_vwap": float(completed[-1]["close"]) >= current_vwap,
        "vwap_flat_or_rising": current_vwap >= prior_vwap,
        "return_from_open": float(completed[-1]["close"])
        / float(completed[0]["open"])
        - 1,
    }


def derive_triggers(
    *,
    manifest_path: Path,
    env_path: Path,
    public_result_path: Path,
) -> dict[str, Any]:
    store = HistoricalDayStore.from_env(env_path)
    manifest, selection = _load_join_contract(manifest_path, store.root)
    records: list[dict[str, Any]] = []
    blockers: Counter[str] = Counter()
    trigger_count = 0
    for pair in selection["selected_pairs"]:
        symbol = str(pair["symbol"])
        day = str(pair["date"])
        dataset = _alpaca_bar(store, symbol, day)
        record: dict[str, Any] = {
            "date": day,
            "symbol": symbol,
            "instrument_id": pair["instrument_id"],
            "rank": pair["rank"],
            "opening_high": pair["scanner_fields"]["opening_high"],
            "status": "NO_CROSS_BEFORE_CUTOFF",
            "first_crossing_minute_et": None,
        }
        if dataset is None:
            record["status"] = "MISSING_CANDIDATE_BARS"
            blockers[record["status"]] += 1
            records.append(record)
            continue
        rows = [expand_bar(row) for row in dataset["rows"]]
        rows.sort(key=lambda row: str(row["time_et"]))
        opening = [
            row
            for row in rows
            if wall_time(9, 30) <= _bar_clock(row) < wall_time(9, 35)
        ]
        if len(opening) != 5:
            record["status"] = "INCOMPLETE_OPENING_MINUTES"
            blockers[record["status"]] += 1
            records.append(record)
            continue
        observed_opening_high = max(float(row["high"]) for row in opening)
        expected_opening_high = float(record["opening_high"])
        if abs(observed_opening_high - expected_opening_high) > max(
            1e-8, expected_opening_high * 1e-8
        ):
            record["status"] = "OPENING_HIGH_MISMATCH"
            record["observed_opening_high"] = observed_opening_high
            blockers[record["status"]] += 1
            records.append(record)
            continue
        crossing = next(
            (
                row
                for row in rows
                if wall_time(9, 35) <= _bar_clock(row) < wall_time(10, 30)
                and float(row["high"]) > expected_opening_high
            ),
            None,
        )
        if crossing is None:
            records.append(record)
            continue
        crossing_at = datetime.fromisoformat(str(crossing["time_et"])).astimezone(
            EASTERN
        )
        prefix = [
            row
            for row in rows
            if datetime.fromisoformat(str(row["time_et"])).astimezone(EASTERN)
            < crossing_at
        ]
        current_vwap = _vwap(prefix)
        prior_vwap = _vwap(prefix[:-5] or prefix[:1])
        record.update(
            {
                "status": "CROSSING_MINUTE_IDENTIFIED",
                "first_crossing_minute_et": crossing_at.isoformat(),
                "crossing_bar": {
                    key: crossing[key]
                    for key in ("open", "high", "low", "close", "volume", "wap")
                },
                "candidate_vwap_before_crossing_minute": current_vwap,
                "candidate_vwap_flat_or_rising_before_crossing_minute": (
                    current_vwap is not None
                    and prior_vwap is not None
                    and current_vwap >= prior_vwap
                ),
                "benchmarks": {
                    benchmark: _benchmark_at(store, benchmark, day, crossing_at)
                    for benchmark in BENCHMARKS
                },
                "fidelity_boundary": (
                    "minute aggregate identifies a tape window, not the exact clean trade"
                ),
            }
        )
        trigger_count += 1
        records.append(record)
    private = {
        "schema_version": 1,
        "dataset_id": manifest["dataset_id"],
        "manifest_sha256": manifest["manifest_sha256"],
        "derived_at": _timestamp_now(),
        "record_count": len(records),
        "records": records,
    }
    private_path = _private_trigger_path(store.root, str(manifest["dataset_id"]))
    _write_gzip_json(private_path, private)
    statuses = Counter(str(item["status"]) for item in records)
    public = {
        "schema_version": 1,
        "dataset_id": manifest["dataset_id"],
        "manifest_sha256": manifest["manifest_sha256"],
        "status": "TRIGGER_WINDOWS_DERIVED" if len(records) == int(
            selection["selected_pair_count"]
        ) else "INCOMPLETE",
        "claim_boundary": (
            "Pipeline-fidelity evidence on an already-inspected selection corpus; "
            "not alpha, confirmation, promotion, or a production strategy variant."
        ),
        "selected_pair_count": int(selection["selected_pair_count"]),
        "trigger_window_count": trigger_count,
        "status_counts": dict(sorted(statuses.items())),
        "blocker_counts": dict(sorted(blockers.items())),
        "private_trigger_index_sha256": _sha256_file(private_path),
        "symbols_public": False,
        "raw_rows_public": False,
        "remaining_fidelity_gaps": [
            "raw SIP trade sequence and trigger-time NBBO are not yet joined",
            "Alpaca/Benzinga news is discovery evidence, not primary catalyst verification",
            "historical full-depth liquidity is unavailable from this source",
            "point-in-time tradability and halt state remain unproven",
            "resistance and sector-relative-strength contracts remain unspecified",
        ],
    }
    _write_json(public_result_path, public)
    return public


def _precise_timestamp(row: Mapping[str, Any]) -> datetime:
    raw = str(row.get("source_timestamp") or row.get("time_et") or "")
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError as exc:
        raise SelectedCandidateJoinError("tape row timestamp is malformed") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed.astimezone(EASTERN)


def _cached_trades(
    store: HistoricalDayStore, symbol: str, start: datetime, end: datetime
) -> list[dict[str, Any]] | None:
    client = LocalHistoricalClient(
        store, "alpaca", feed="sip", adjustment="raw"
    )
    try:
        return client.fetch_trades(symbol, start, end, use_rth=True)
    except HistoricalProviderError as exc:
        if exc.category == "local_cache_miss":
            return None
        raise


def _cached_quotes(
    store: HistoricalDayStore, symbol: str, start: datetime, end: datetime
) -> list[dict[str, Any]] | None:
    client = LocalHistoricalClient(
        store, "alpaca", feed="sip", adjustment="raw"
    )
    try:
        return client.fetch_bid_ask_ticks(symbol, start, end, use_rth=True)
    except HistoricalProviderError as exc:
        if exc.category == "local_cache_miss":
            return None
        raise


def _select_quote_snapshots(
    quotes: Sequence[Mapping[str, Any]], trigger_at: datetime
) -> list[dict[str, Any]]:
    ordered = sorted(quotes, key=_precise_timestamp)
    snapshots: list[dict[str, Any]] = []
    for offset in (0, 5, 10):
        target = trigger_at + timedelta(seconds=offset)
        available = [row for row in ordered if _precise_timestamp(row) <= target]
        if not available:
            continue
        row = available[-1]
        observed = _precise_timestamp(row)
        age = (target - observed).total_seconds()
        snapshots.append(
            {
                "target_at_et": target.isoformat(),
                "observed_at_et": observed.isoformat(),
                "age_seconds": age,
                "bid": float(row["bid"]),
                "ask": float(row["ask"]),
                "bid_size": int(row.get("bid_size", 0)),
                "ask_size": int(row.get("ask_size", 0)),
            }
        )
    return snapshots


def collect_trigger_tape(
    *,
    manifest_path: Path,
    env_path: Path,
    public_status_path: Path,
) -> dict[str, Any]:
    store = HistoricalDayStore.from_env(env_path)
    manifest, _selection = _load_join_contract(manifest_path, store.root)
    trigger_path = _private_trigger_path(store.root, str(manifest["dataset_id"]))
    if not trigger_path.exists():
        raise SelectedCandidateJoinError("derive trigger windows before tape collection")
    trigger_index = _read_gzip_object(trigger_path)
    if trigger_index.get("manifest_sha256") != manifest["manifest_sha256"]:
        raise SelectedCandidateJoinError("trigger index does not match manifest")
    config = AlpacaConfig.optional_from_env(env_path)
    if config is None:
        raise SelectedCandidateJoinError("Alpaca credentials are not configured")
    gate = _RateGate(_rate_interval(env_path))
    tape_records: list[dict[str, Any]] = []
    errors: list[dict[str, Any]] = []
    counts: Counter[str] = Counter()
    started = time.monotonic()
    triggers = [
        row
        for row in trigger_index.get("records", [])
        if isinstance(row, Mapping)
        and row.get("status") == "CROSSING_MINUTE_IDENTIFIED"
    ]
    with AlpacaHistoricalClient(config) as client:
        recorder = RecordingHistoricalClient(client, store)
        for index, trigger in enumerate(triggers, 1):
            symbol = str(trigger["symbol"])
            day = str(trigger["date"])
            crossing = datetime.fromisoformat(
                str(trigger["first_crossing_minute_et"])
            ).astimezone(EASTERN)
            minute_end = crossing + timedelta(minutes=1)
            try:
                trades = _cached_trades(store, symbol, crossing, minute_end)
                trade_cached = trades is not None
                if trades is None:
                    trades = _retry(
                        gate,
                        lambda: recorder.fetch_trades(
                            symbol, crossing, minute_end, use_rth=True
                        ),
                    )
                if not trades:
                    raise SelectedCandidateJoinError(
                        "crossing bar returned no raw trades",
                        category="permanent_fidelity",
                    )
                opening_high = float(trigger["opening_high"])
                cross = next(
                    (row for row in trades if float(row["price"]) > opening_high),
                    None,
                )
                if cross is None:
                    raise SelectedCandidateJoinError(
                        "raw trade window did not reproduce aggregate high cross",
                        category="permanent_fidelity",
                    )
                trade_at = _precise_timestamp(cross)
                quote_start = trade_at - timedelta(seconds=1)
                quote_end = trade_at + timedelta(seconds=11)
                quotes = _cached_quotes(store, symbol, quote_start, quote_end)
                quote_cached = quotes is not None
                if quotes is None:
                    quotes = _retry(
                        gate,
                        lambda: recorder.fetch_bid_ask_ticks(
                            symbol, quote_start, quote_end, use_rth=True
                        ),
                    )
                snapshots = _select_quote_snapshots(quotes, trade_at)
                record = {
                    "date": day,
                    "symbol": symbol,
                    "instrument_id": trigger["instrument_id"],
                    "rank": trigger["rank"],
                    "opening_high": opening_high,
                    "crossing_minute_et": crossing.isoformat(),
                    "first_observed_trade_cross": {
                        "observed_at_et": trade_at.isoformat(),
                        "price": float(cross["price"]),
                        "size": int(cross["size"]),
                        "exchange": cross.get("exchange"),
                        "conditions": cross.get("conditions"),
                        "tape": cross.get("tape"),
                    },
                    "trade_row_count": len(trades),
                    "quote_row_count": len(quotes),
                    "quote_snapshots": snapshots,
                    "clean_break_status": "UNRESOLVED_CONDITION_SEMANTICS",
                    "cache": {"trades": trade_cached, "quotes": quote_cached},
                }
                context = build_context(
                    kind="selected_candidate_trigger_tape",
                    provider="alpaca",
                    observed_at=(trade_at + timedelta(seconds=10)).isoformat(),
                    payload={
                        "dataset_id": manifest["dataset_id"],
                        **{key: value for key, value in record.items() if key != "cache"},
                    },
                    provenance={
                        "source_type": "Alpaca historical SIP trades and quotes",
                        "captured_at": _timestamp_now(),
                    },
                )
                store.merge(symbol, day, contexts=[context])
                tape_records.append(record)
                counts["completed_trigger_tapes"] += 1
                counts["trade_rows"] += len(trades)
                counts["quote_rows"] += len(quotes)
                counts["three_snapshot_windows"] += int(len(snapshots) == 3)
                counts["trade_cache_hits"] += int(trade_cached)
                counts["quote_cache_hits"] += int(quote_cached)
            except (HistoricalProviderError, SelectedCandidateJoinError, OSError) as exc:
                errors.append(
                    {
                        "date": day,
                        "symbol": symbol,
                        "kind": "trigger_tape",
                        "error_type": type(exc).__name__,
                        "category": getattr(exc, "category", "local_io"),
                        "error": str(exc),
                    }
                )
            if shutil.disk_usage(store.root).free < MINIMUM_FREE_BYTES:
                raise SelectedCandidateJoinError(
                    "historical store crossed the 10 GiB reserve during tape collection",
                    category="capacity",
                )
            if index % 10 == 0 or index == len(triggers):
                checkpoint = {
                    "schema_version": 1,
                    "dataset_id": manifest["dataset_id"],
                    "manifest_sha256": manifest["manifest_sha256"],
                    "updated_at": _timestamp_now(),
                    "expected_trigger_tapes": len(triggers),
                    "records": tape_records,
                    "counts": dict(sorted(counts.items())),
                    "errors": errors,
                }
                _write_gzip_json(
                    _private_tape_path(store.root, str(manifest["dataset_id"])),
                    checkpoint,
                )
                print(
                    f"joined tape {index}/{len(triggers)}; errors={len(errors)}",
                    flush=True,
                )
    complete = len(tape_records) == len(triggers) and not errors
    private = {
        "schema_version": 1,
        "dataset_id": manifest["dataset_id"],
        "manifest_sha256": manifest["manifest_sha256"],
        "updated_at": _timestamp_now(),
        "status": "TAPE_JOIN_COMPLETE" if complete else "INCOMPLETE",
        "expected_trigger_tapes": len(triggers),
        "records": tape_records,
        "counts": dict(sorted(counts.items())),
        "errors": errors,
        "elapsed_seconds": time.monotonic() - started,
    }
    private_path = _private_tape_path(store.root, str(manifest["dataset_id"]))
    _write_gzip_json(private_path, private)
    public = {
        "schema_version": 1,
        "dataset_id": manifest["dataset_id"],
        "manifest_sha256": manifest["manifest_sha256"],
        "status": private["status"],
        "expected_trigger_tapes": len(triggers),
        "counts": private["counts"],
        "error_count": len(errors),
        "error_categories": dict(
            sorted(Counter(str(item["category"]) for item in errors).items())
        ),
        "elapsed_seconds": private["elapsed_seconds"],
        "private_tape_index_sha256": _sha256_file(private_path),
        "symbols_public": False,
        "raw_rows_public": False,
        "clean_break_status": "UNRESOLVED_CONDITION_SEMANTICS",
    }
    _write_json(public_status_path, public)
    return public


def _candidate_news_context(
    store: HistoricalDayStore, symbol: str, day: str, dataset_id: str
) -> Mapping[str, Any] | None:
    document = store.load(symbol, day)
    if document is None:
        return None
    matches = [
        context
        for context in document["contexts"]
        if context.get("kind") == "selected_candidate_catalyst_candidates"
        and context.get("provider") == "alpaca"
        and context.get("payload", {}).get("dataset_id") == dataset_id
    ]
    return matches[-1] if matches else None


def inspect_join(
    *,
    manifest_path: Path,
    env_path: Path,
    public_result_path: Path,
) -> dict[str, Any]:
    store = HistoricalDayStore.from_env(env_path)
    manifest, selection = _load_join_contract(manifest_path, store.root)
    trigger_path = _private_trigger_path(store.root, str(manifest["dataset_id"]))
    tape_path = _private_tape_path(store.root, str(manifest["dataset_id"]))
    if not trigger_path.exists() or not tape_path.exists():
        raise SelectedCandidateJoinError("trigger and tape joins must exist before inspection")
    trigger = _read_gzip_object(trigger_path)
    tape = _read_gzip_object(tape_path)
    if trigger.get("manifest_sha256") != manifest["manifest_sha256"] or tape.get(
        "manifest_sha256"
    ) != manifest["manifest_sha256"]:
        raise SelectedCandidateJoinError("private join artifacts do not match manifest")
    tape_by_key = {
        (str(row["date"]), str(row["symbol"])): row
        for row in tape.get("records", [])
        if isinstance(row, Mapping)
    }
    counts: Counter[str] = Counter()
    spread_fractions: list[float] = []
    news_articles = 0
    for pair in selection["selected_pairs"]:
        day = str(pair["date"])
        symbol = str(pair["symbol"])
        counts["selected_pairs"] += 1
        counts["candidate_bars_complete"] += int(
            _alpaca_bar(store, symbol, day) is not None
        )
        news = _candidate_news_context(store, symbol, day, str(manifest["dataset_id"]))
        if news is not None:
            counts["news_contexts_complete"] += 1
            article_count = int(news.get("payload", {}).get("article_count", 0))
            news_articles += article_count
            counts["pairs_with_news_candidates"] += int(article_count > 0)
        tape_row = tape_by_key.get((day, symbol))
        if tape_row is None:
            continue
        counts["trigger_tapes_complete"] += 1
        snapshots = tape_row.get("quote_snapshots", [])
        if len(snapshots) == 3:
            counts["three_snapshot_windows"] += 1
        quote_gate = True
        for snapshot in snapshots:
            bid = float(snapshot["bid"])
            ask = float(snapshot["ask"])
            if bid <= 0 or ask < bid or float(snapshot["age_seconds"]) > 5:
                quote_gate = False
                continue
            midpoint = (bid + ask) / 2
            spread_fractions.append((ask - bid) / midpoint if midpoint else 0.0)
        counts["basic_fresh_uncrossed_quote_windows"] += int(
            len(snapshots) == 3 and quote_gate
        )
        if snapshots:
            final_ask = float(snapshots[-1]["ask"])
            counts["post_snapshot_chase_cap_pass"] += int(
                final_ask
                <= float(tape_row["opening_high"]) * (1 + 0.0015)
            )
    benchmark_expected = len(BENCHMARKS) * len(manifest["requested_dates"])
    benchmark_complete = sum(
        _alpaca_bar(store, symbol, str(day)) is not None
        for day in manifest["requested_dates"]
        for symbol in BENCHMARKS
    )
    counts["benchmark_bar_dates_complete"] = benchmark_complete
    inspected_complete = (
        counts["candidate_bars_complete"] == counts["selected_pairs"]
        and counts["news_contexts_complete"] == counts["selected_pairs"]
        and benchmark_complete == benchmark_expected
        and tape.get("status") == "TAPE_JOIN_COMPLETE"
    )
    sorted_spreads = sorted(spread_fractions)
    median_spread = (
        sorted_spreads[len(sorted_spreads) // 2] if sorted_spreads else None
    )
    public = {
        "schema_version": 1,
        "dataset_id": manifest["dataset_id"],
        "manifest_sha256": manifest["manifest_sha256"],
        "status": "READY" if inspected_complete else "INCOMPLETE",
        "inspected": True,
        "claim_scope": "DEVELOPMENT_ONLY",
        "source_corpus_already_inspected": True,
        "strategy_version_changed": False,
        "strategy_promotion_earned": False,
        "counts": dict(sorted(counts.items())),
        "expected": {
            "selected_pairs": int(selection["selected_pair_count"]),
            "benchmark_bar_dates": benchmark_expected,
            "trigger_tapes": int(tape.get("expected_trigger_tapes", 0)),
        },
        "news_articles": news_articles,
        "quote_spread_observations": len(spread_fractions),
        "median_observed_spread_fraction": median_spread,
        "private_selection_content_sha256": manifest["selection_contract"][
            "private_selection_content_sha256"
        ],
        "private_trigger_index_sha256": _sha256_file(trigger_path),
        "private_tape_index_sha256": _sha256_file(tape_path),
        "symbols_public": False,
        "raw_rows_public": False,
        "findings": [
            "Exact scanner-selected security-date identities remained frozen and private.",
            "Candidate and benchmark bars use the same raw Alpaca SIP basis as scanner v4.",
            "Raw SIP trades identify an observed crossing sequence; quote snapshots are formed only afterward.",
            "News coverage measures discovery availability and does not satisfy verified catalyst.",
        ],
        "production_blockers": [
            "No candidate has primary issuer, SEC, or exchange catalyst verification from this join.",
            "Clean-break semantics are not yet frozen against SIP trade-condition update rules.",
            "Historical top-of-book is not full-depth liquidity at the planned entry limit.",
            "Point-in-time tradability, halt risk, resistance, and sector-relative strength remain incomplete.",
            "This already-inspected corpus cannot support alpha or confirmation claims.",
        ],
        "decision": (
            "Keep 2026-07-15-orb-v3 unchanged; use this dataset only to close and "
            "measure production-replay input fidelity before freezing independent dates."
        ),
    }
    _write_json(public_result_path, public)
    return public


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env", type=Path, default=PROJECT_ROOT / ".env")
    subparsers = parser.add_subparsers(dest="command", required=True)
    freeze = subparsers.add_parser("freeze")
    freeze.add_argument("--summary", type=Path, default=DEFAULT_SOURCE_SUMMARY)
    freeze.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    for name in ("pilot", "collect", "derive", "collect-tape", "inspect"):
        command = subparsers.add_parser(name)
        command.add_argument("manifest", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        if args.command == "freeze":
            path, manifest = freeze_join(
                summary_path=args.summary,
                env_path=args.env,
                output_root=args.output_root,
            )
            result: Any = {
                "path": str(path),
                "dataset_id": manifest["dataset_id"],
                "manifest_sha256": manifest["manifest_sha256"],
                "selected_pair_count": manifest["selection_contract"][
                    "selected_pair_count"
                ],
            }
        elif args.command == "pilot":
            result = pilot(
                manifest_path=args.manifest,
                env_path=args.env,
                public_status_path=DEFAULT_PUBLIC_STATUS,
            )
        elif args.command == "collect":
            result = collect(
                manifest_path=args.manifest,
                env_path=args.env,
                public_status_path=DEFAULT_PUBLIC_STATUS,
            )
        elif args.command == "derive":
            result = derive_triggers(
                manifest_path=args.manifest,
                env_path=args.env,
                public_result_path=DEFAULT_PUBLIC_RESULT,
            )
        elif args.command == "collect-tape":
            result = collect_trigger_tape(
                manifest_path=args.manifest,
                env_path=args.env,
                public_status_path=DEFAULT_PUBLIC_STATUS,
            )
        else:
            result = inspect_join(
                manifest_path=args.manifest,
                env_path=args.env,
                public_result_path=DEFAULT_PUBLIC_RESULT,
            )
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    except (
        HistoricalProviderError,
        LearningDataError,
        SelectedCandidateJoinError,
        OSError,
        ValueError,
    ) as exc:
        print(
            json.dumps(
                {
                    "error": str(exc),
                    "error_type": type(exc).__name__,
                    "category": getattr(exc, "category", "validation"),
                },
                indent=2,
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

"""Freeze, collect, and inspect the causal development pre-entry evidence graph.

The collector is intentionally outcome blind. It is separately frozen against
the inspected non-return contract, requires its own source and manifest to be
committed and pushed before provider access, checkpoints every one-second tape
window, and never requests a target price row after the final decision snapshot.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import io
import json
import os
import shutil
import subprocess
import sys
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from typing import Any

import requests

import development_non_return as source_contract
import development_sec_submissions as publication_gate
from historical_providers import (
    AlpacaConfig,
    AlpacaHistoricalClient,
    HistoricalProviderError,
)
from historical_service import LocalHistoricalClient, RecordingHistoricalClient
from historical_store import EASTERN, HistoricalDayStore, HistoricalStoreError
from learning_data import (
    LearningDataError,
    freeze_dataset_contract,
    load_frozen_dataset_contract,
)
from nasdaq_halts import NasdaqHaltClient, NasdaqHaltError
from scanner_replay import (
    MassiveReferenceConfig,
    ScannerReplayError,
    collect_split_actions,
)
from sip_trade_conditions import classify_trade_conditions


PROJECT_ROOT = Path(__file__).resolve().parent
DATASET_ID = "dataset-development-non-return-preentry-collection-2026-07-20-v1"
BASE_MANIFEST = (
    PROJECT_ROOT
    / "historical_batches/development_tranche_v2/non_return_manifests"
    / (
        "dataset-development-non-return-qualification-2026-07-20-v5-"
        "bd4425a033feb6fc54ab4d6d4130f78beaa6d77ecc805d4ce627babbfcf7571e.json"
    )
)
BASE_PUBLIC_STATUS = (
    PROJECT_ROOT
    / "historical_batches/development_tranche_v2/non-return-contract-status.json"
)
DEFAULT_MANIFEST_ROOT = (
    PROJECT_ROOT
    / "historical_batches/development_tranche_v2/non_return_collection_manifests"
)
DEFAULT_CONTRACT_STATUS = (
    PROJECT_ROOT
    / "historical_batches/development_tranche_v2/non-return-collection-contract-status.json"
)
DEFAULT_COLLECTION_STATUS = (
    PROJECT_ROOT
    / "historical_batches/development_tranche_v2/non-return-collection-status.json"
)
PRIVATE_NAMESPACE = "_derived/development_non_return_collection"
CALENDAR_FILE = "calendar.json.gz"
SPLITS_FILE = "splits.json.gz"
INDEX_FILE = "collection-index.json.gz"
PAIR_DIRECTORY = "pairs"
HALT_DIRECTORY = "nasdaq-halts"
EXPECTED_PAIRS = 21
SEARCH_SECONDS_PER_PAIR = 55 * 60
QUOTE_SIZE_SHARES_EFFECTIVE = date(2025, 11, 3)
QUOTE_SIZE_SOURCE_URL = "https://docs.alpaca.markets/us/v1.1/changelog?page=3"


class DevelopmentNonReturnCollectionError(RuntimeError):
    """The provider collection is unsafe, incomplete, or contract-incompatible."""


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode()


def _sha256_json(value: Any) -> str:
    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _gzip_bytes(value: Any) -> bytes:
    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode="wb", compresslevel=6, mtime=0) as target:
        target.write(json.dumps(value, indent=2, sort_keys=True).encode())
        target.write(b"\n")
    return buffer.getvalue()


def _write_gzip(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_bytes(_gzip_bytes(value))
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _read_gzip(path: Path) -> dict[str, Any]:
    try:
        with gzip.open(path, "rt", encoding="utf-8") as source:
            value = json.load(source)
    except (OSError, json.JSONDecodeError) as exc:
        raise DevelopmentNonReturnCollectionError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise DevelopmentNonReturnCollectionError(f"{path} must contain an object")
    return value


def _write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_text(
            json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise DevelopmentNonReturnCollectionError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise DevelopmentNonReturnCollectionError(f"{path} must contain an object")
    return value


def _repo_path(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(PROJECT_ROOT))
    except ValueError as exc:
        raise DevelopmentNonReturnCollectionError(
            f"public path must be repository relative: {path}"
        ) from exc


def _private_root(store_root: Path) -> Path:
    return store_root / PRIVATE_NAMESPACE / DATASET_ID


def _calendar_path(store_root: Path) -> Path:
    return _private_root(store_root) / CALENDAR_FILE


def _splits_path(store_root: Path) -> Path:
    return _private_root(store_root) / SPLITS_FILE


def _index_path(store_root: Path) -> Path:
    return _private_root(store_root) / INDEX_FILE


def _pair_key(pair: Mapping[str, Any]) -> str:
    return source_contract._sha256_json((pair["date"], pair["instrument_id"]))


def _pair_path(store_root: Path, pair: Mapping[str, Any]) -> Path:
    return _private_root(store_root) / PAIR_DIRECTORY / f"{_pair_key(pair)}.json.gz"


def _search_minute_path(
    store_root: Path, pair: Mapping[str, Any], minute_start: datetime
) -> Path:
    stamp = minute_start.strftime("%Y%m%dT%H%M%S%z")
    return (
        _private_root(store_root)
        / PAIR_DIRECTORY
        / _pair_key(pair)
        / "search-minutes"
        / f"{stamp}.json.gz"
    )


def _target_artifacts(store_root: Path) -> list[Path]:
    root = _private_root(store_root)
    return sorted(path for path in root.rglob("*") if path.is_file()) if root.exists() else []


def _published(path: Path) -> dict[str, str]:
    try:
        return publication_gate._published_source(path)
    except (
        publication_gate.DevelopmentSecSubmissionsError,
        subprocess.SubprocessError,
    ) as exc:
        raise DevelopmentNonReturnCollectionError(str(exc)) from exc


def _binding(path: Path) -> dict[str, str]:
    return {"path": _repo_path(path), "sha256": _sha256_file(path)}


def _verify_binding(value: Mapping[str, Any]) -> None:
    raw = value.get("path")
    if not isinstance(raw, str) or not raw:
        raise DevelopmentNonReturnCollectionError("bound public path is missing")
    relative = Path(raw)
    if relative.is_absolute() or ".." in relative.parts:
        raise DevelopmentNonReturnCollectionError("bound public path is unsafe")
    path = PROJECT_ROOT / relative
    if not path.is_file() or _sha256_file(path) != value.get("sha256"):
        raise DevelopmentNonReturnCollectionError(f"bound artifact drifted: {relative}")


def _load_base(
    env_path: Path,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any], HistoricalDayStore]:
    manifest = load_frozen_dataset_contract(BASE_MANIFEST)
    public = _read_json(BASE_PUBLIC_STATUS)
    if (
        manifest.get("dataset_id") != source_contract.DATASET_ID
        or manifest.get("manifest_sha256")
        != "bd4425a033feb6fc54ab4d6d4130f78beaa6d77ecc805d4ce627babbfcf7571e"
        or public.get("manifest_sha256") != manifest.get("manifest_sha256")
        or public.get("status") != "FROZEN_READY"
        or public.get("inspected") is not True
        or public.get("target_artifacts") != 0
        or public.get("target_outcomes_observed_or_derived") is not False
        or manifest.get("acquisition_contract", {}).get(
            "provider_rows_after_final_decision_snapshot_allowed"
        )
        is not False
        or manifest.get("outcome_lock", {}).get(
            "target_outcomes_observed_or_derived"
        )
        is not False
    ):
        raise DevelopmentNonReturnCollectionError("base non-return contract is not ready")
    store = HistoricalDayStore.from_env(env_path)
    private_path = source_contract._private_contract_path(store.root)
    private = _read_gzip(private_path)
    selection = private.get("selection")
    graph = private.get("request_graph")
    if (
        not isinstance(selection, Mapping)
        or not isinstance(graph, Mapping)
        or selection.get("positive_pair_count") != EXPECTED_PAIRS
        or _sha256_json(selection)
        != manifest.get("selection_contract", {}).get(
            "private_positive_selection_sha256"
        )
        or _sha256_json(graph)
        != manifest.get("acquisition_contract", {}).get(
            "private_request_graph_sha256"
        )
        or private.get("target_outcomes_observed_or_derived") is not False
    ):
        raise DevelopmentNonReturnCollectionError("private base graph drifted")
    return manifest, public, private, store


def _implementation_contract() -> dict[str, Any]:
    names = (
        "development_non_return_collection.py",
        "development_non_return.py",
        "historical_providers.py",
        "historical_service.py",
        "historical_store.py",
        "nasdaq_halts.py",
        "scanner_replay.py",
        "sip_bar_aggregation.py",
        "sip_trade_conditions.py",
    )
    return {name: _binding(PROJECT_ROOT / name) for name in names}


def _provider_contract(env_path: Path) -> dict[str, Any]:
    alpaca = AlpacaConfig.optional_from_env(env_path)
    if alpaca is None or alpaca.feed != "sip" or alpaca.adjustment != "raw":
        raise DevelopmentNonReturnCollectionError("raw Alpaca SIP credentials are required")
    massive = MassiveReferenceConfig.from_env(env_path)
    return {"alpaca": alpaca.public_dict(), "massive": massive.public_dict()}


def _expected_contract(
    *,
    base_manifest: Mapping[str, Any],
    provider_contract: Mapping[str, Any],
    free_bytes: int,
) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "requested_dates": list(base_manifest["requested_dates"]),
        "dataset_payload": {
            "lane": "development",
            "claim_scope": "DEVELOPMENT_ONLY",
            "status": "COLLECTING",
            "evidence_paths": [
                _repo_path(BASE_MANIFEST),
                _repo_path(BASE_PUBLIC_STATUS),
                "DEVELOPMENT_NON_RETURN.md",
                "PRODUCTION_STRATEGY_VALIDATION.md",
            ],
            "inspected": False,
        },
        "lineage_contract": {
            "base_manifest": {
                **_binding(BASE_MANIFEST),
                "manifest_sha256": base_manifest["manifest_sha256"],
            },
            "base_public_status": _binding(BASE_PUBLIC_STATUS),
            "private_base_contract_sha256": base_manifest["selection_contract"][
                "private_positive_selection_sha256"
            ],
            "private_request_graph_sha256": base_manifest["acquisition_contract"][
                "private_request_graph_sha256"
            ],
            "positive_pair_identity_sha256": base_manifest["selection_contract"][
                "positive_pair_identity_sha256"
            ],
            "positive_pairs": EXPECTED_PAIRS,
        },
        "implementation_contract": {
            "files": _implementation_contract(),
            "collector_must_be_committed_and_pushed_before_provider_access": True,
            "manifest_must_be_committed_and_pushed_before_provider_access": True,
        },
        "provider_contract": dict(provider_contract),
        "collection_contract": {
            "calendar_query": {
                "start": source_contract.CALENDAR_QUERY_START,
                "end": source_contract.CALENDAR_QUERY_END,
                "provider": "Alpaca Market Calendar API",
            },
            "split_query_count": 1,
            "pair_count": EXPECTED_PAIRS,
            "maximum_one_second_trade_windows": (
                EXPECTED_PAIRS * SEARCH_SECONDS_PER_PAIR
            ),
            "one_second_windows_checkpointed": True,
            "search_stops_at_first_clean_cross": True,
            "later_search_windows_after_clean_cross_allowed": False,
            "candidate_opening_bars_end_at_09_35": True,
            "conditional_bars_fully_completed_before_final_decision_minute": True,
            "decision_trade_prefix_end_exclusive_at_plus_10_seconds": True,
            "quote_window_end_inclusive_at_plus_10_seconds": True,
            "provider_rows_after_final_decision_allowed": False,
            "whole_provider_market_fidelity": "ALPACA_RAW_SIP",
            "provider_switching_allowed": False,
            "symbol_or_date_substitution_allowed": False,
            "local_exact_cache_first": True,
            "atomic_pair_checkpoints": True,
            "quote_size_source_url": QUOTE_SIZE_SOURCE_URL,
            "quote_size_shares_effective": QUOTE_SIZE_SHARES_EFFECTIVE.isoformat(),
        },
        "capacity_contract": {
            "minimum_free_bytes": source_contract.MINIMUM_RESERVE_BYTES,
            "observed_free_bytes_at_freeze": free_bytes,
            "historical_deletion_allowed": False,
        },
        "privacy_contract": {
            "private_root": f"LOCAL_HISTORICAL_DATA_ROOT/{PRIVATE_NAMESPACE}/{DATASET_ID}/",
            "public_aggregates_and_hashes_only": True,
            "symbols_dates_instrument_ids_raw_rows_and_requests_public": False,
        },
        "outcome_lock": {
            "target_returns_prices_after_decision_and_outcomes_allowed": False,
            "post_entry_data_access_allowed": False,
            "outcome_fields_allowed": False,
            "target_outcomes_observed_or_derived": False,
            "separate_inspected_qualification_required": True,
        },
    }


def _matching_manifest(
    root: Path, expected: Mapping[str, Any]
) -> tuple[Path, dict[str, Any]] | None:
    matches = sorted(root.glob(f"{DATASET_ID}-*.json"))
    if len(matches) > 1:
        raise DevelopmentNonReturnCollectionError("multiple collector manifests exist")
    if not matches:
        return None
    manifest = load_frozen_dataset_contract(matches[0])
    for key, value in expected.items():
        if key == "capacity_contract":
            observed = manifest.get(key, {})
            if (
                observed.get("minimum_free_bytes") != value["minimum_free_bytes"]
                or observed.get("historical_deletion_allowed") is not False
            ):
                raise DevelopmentNonReturnCollectionError("collector capacity drifted")
            continue
        if manifest.get(key) != value:
            raise DevelopmentNonReturnCollectionError(f"collector {key} drifted")
    return matches[0], manifest


def _contract_status(
    manifest: Mapping[str, Any], *, status: str, inspected: bool
) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "status": status,
        "inspected": inspected,
        "positive_pairs": EXPECTED_PAIRS,
        "maximum_one_second_trade_windows": EXPECTED_PAIRS
        * SEARCH_SECONDS_PER_PAIR,
        "target_artifacts": 0,
        "symbols_dates_instrument_ids_raw_rows_and_requests_public": False,
        "provider_access_performed": False,
        "post_entry_data_access_allowed": False,
        "target_outcomes_observed_or_derived": False,
    }


def freeze(
    *, env_path: Path, output_root: Path, public_status_path: Path
) -> tuple[Path, dict[str, Any]]:
    base, _public, _private, store = _load_base(env_path)
    _published(Path(__file__))
    _published(BASE_MANIFEST)
    if _target_artifacts(store.root):
        raise DevelopmentNonReturnCollectionError(
            "collector target artifacts exist before its manifest"
        )
    free = shutil.disk_usage(store.root).free
    if free < source_contract.MINIMUM_RESERVE_BYTES:
        raise DevelopmentNonReturnCollectionError("historical store is below reserve")
    expected = _expected_contract(
        base_manifest=base,
        provider_contract=_provider_contract(env_path),
        free_bytes=free,
    )
    existing = _matching_manifest(output_root, expected)
    if existing is None:
        path, manifest = freeze_dataset_contract(
            {**expected, "registered_at": datetime.now(UTC).isoformat()}, output_root
        )
    else:
        path, manifest = existing
    _write_json(
        public_status_path,
        _contract_status(manifest, status="FROZEN_WAITING_INSPECTION", inspected=False),
    )
    return path, manifest


def inspect_contract(
    *, manifest_path: Path, env_path: Path, public_status_path: Path
) -> dict[str, Any]:
    manifest = load_frozen_dataset_contract(manifest_path)
    if manifest.get("dataset_id") != DATASET_ID:
        raise DevelopmentNonReturnCollectionError("unexpected collector dataset")
    base, _public, _private, store = _load_base(env_path)
    expected = _expected_contract(
        base_manifest=base,
        provider_contract=_provider_contract(env_path),
        free_bytes=int(manifest["capacity_contract"]["observed_free_bytes_at_freeze"]),
    )
    for key, value in expected.items():
        if manifest.get(key) != value:
            raise DevelopmentNonReturnCollectionError(f"frozen collector {key} drifted")
    if _target_artifacts(store.root):
        raise DevelopmentNonReturnCollectionError(
            "collector artifacts appeared before contract inspection"
        )
    result = _contract_status(manifest, status="FROZEN_READY", inspected=True)
    result["inspection"] = {
        "base_lineage_rebuilt": True,
        "implementation_hashes_rebuilt": True,
        "provider_configuration_rebuilt": True,
        "request_boundaries_rebuilt": True,
        "privacy_and_outcome_locks_rebuilt": True,
        "zero_target_artifacts_rechecked": True,
        "valid": True,
    }
    _write_json(public_status_path, result)
    return result


def _observed(row: Mapping[str, Any]) -> datetime:
    raw = str(row.get("source_timestamp") or row.get("time_et") or "")
    try:
        value = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError as exc:
        raise DevelopmentNonReturnCollectionError("provider row timestamp is malformed") from exc
    if value.tzinfo is None:
        raise DevelopmentNonReturnCollectionError("provider row timestamp lacks timezone")
    return value.astimezone(EASTERN)


def _validate_boundary(
    rows: Sequence[Mapping[str, Any]], *, start: datetime, end: datetime, inclusive_end: bool
) -> None:
    for row in rows:
        observed = _observed(row)
        inside = start <= observed <= end if inclusive_end else start <= observed < end
        if not inside:
            raise DevelopmentNonReturnCollectionError(
                "provider returned a row outside the causal request boundary"
            )


def search_clean_cross(
    *,
    fetch_second: Callable[[datetime, datetime], Sequence[Mapping[str, Any]]],
    start: datetime,
    end: datetime,
    opening_high: float,
    checkpoint: Callable[
        [datetime, Sequence[Mapping[str, Any]], Mapping[str, Any] | None], None
    ]
    | None = None,
) -> tuple[dict[str, Any] | None, int]:
    """Search every second in order and stop without requesting past a clean cross."""

    if end <= start or opening_high <= 0:
        raise DevelopmentNonReturnCollectionError("clean-cross search inputs are invalid")
    cursor = start
    windows = 0
    while cursor < end:
        following = min(cursor + timedelta(seconds=1), end)
        rows = list(fetch_second(cursor, following))
        _validate_boundary(rows, start=cursor, end=following, inclusive_end=False)
        windows += 1
        ordered = sorted(rows, key=lambda row: (_observed(row), str(row.get("trade_id", ""))))
        clean = next(
            (
                dict(row)
                for row in ordered
                if float(row.get("price", 0)) > opening_high
                and classify_trade_conditions(
                    row.get("tape"), row.get("conditions")
                ).establishes_continuous_cross
            ),
            None,
        )
        if checkpoint is not None:
            checkpoint(following, rows, clean)
        if clean is not None:
            return clean, windows
        cursor = following
    return None, windows


def quote_snapshots(
    quotes: Sequence[Mapping[str, Any]], clean_at: datetime
) -> list[dict[str, Any]]:
    ordered = sorted(quotes, key=_observed)
    snapshots: list[dict[str, Any]] = []
    for offset in (0, 5, 10):
        target = clean_at + timedelta(seconds=offset)
        available = [row for row in ordered if _observed(row) <= target]
        if not available:
            return []
        row = available[-1]
        observed = _observed(row)
        size = int(row.get("ask_size", 0))
        if clean_at.date() < QUOTE_SIZE_SHARES_EFFECTIVE:
            size *= 100
        snapshots.append(
            {
                "target_at_et": target.isoformat(),
                "observed_at_et": observed.isoformat(),
                "age_seconds": (target - observed).total_seconds(),
                "bid": float(row.get("bid", 0)),
                "ask": float(row.get("ask", 0)),
                "bid_size": int(row.get("bid_size", 0))
                * (100 if clean_at.date() < QUOTE_SIZE_SHARES_EFFECTIVE else 1),
                "ask_size": size,
            }
        )
    return snapshots


def _collect_calendar(
    *, env_path: Path, output: Path
) -> tuple[list[dict[str, str]], str]:
    if output.exists():
        cached = _read_gzip(output)
        rows = cached.get("rows")
        if not isinstance(rows, list):
            raise DevelopmentNonReturnCollectionError("cached calendar is malformed")
        return [dict(row) for row in rows if isinstance(row, Mapping)], "CACHE"
    config = AlpacaConfig.optional_from_env(env_path)
    if config is None:
        raise DevelopmentNonReturnCollectionError("Alpaca configuration is missing")
    response = requests.get(
        "https://api.alpaca.markets/v2/calendar",
        params={
            "start": source_contract.CALENDAR_QUERY_START,
            "end": source_contract.CALENDAR_QUERY_END,
        },
        headers={
            "APCA-API-KEY-ID": config.api_key,
            "APCA-API-SECRET-KEY": config.api_secret,
        },
        timeout=config.timeout_seconds,
    )
    if response.status_code != 200:
        raise DevelopmentNonReturnCollectionError(
            f"Alpaca calendar HTTP {response.status_code}"
        )
    try:
        payload = response.json()
    except ValueError as exc:
        raise DevelopmentNonReturnCollectionError(
            "Alpaca calendar returned invalid JSON"
        ) from exc
    if not isinstance(payload, list) or not payload:
        raise DevelopmentNonReturnCollectionError("Alpaca calendar is empty")
    rows: list[dict[str, str]] = []
    for raw in payload:
        if not isinstance(raw, Mapping):
            raise DevelopmentNonReturnCollectionError("Alpaca calendar row is malformed")
        day = date.fromisoformat(str(raw.get("date"))).isoformat()
        opened = time.fromisoformat(str(raw.get("open"))).isoformat()
        closed = time.fromisoformat(str(raw.get("close"))).isoformat()
        rows.append({"date": day, "open_et": opened, "close_et": closed})
    rows.sort(key=lambda row: row["date"])
    if len({row["date"] for row in rows}) != len(rows):
        raise DevelopmentNonReturnCollectionError("Alpaca calendar repeats a date")
    value = {
        "schema_version": 1,
        "query": {
            "start": source_contract.CALENDAR_QUERY_START,
            "end": source_contract.CALENDAR_QUERY_END,
        },
        "rows": rows,
    }
    _write_gzip(output, value)
    return rows, "PROVIDER"


def _prior_sessions(calendar: Sequence[Mapping[str, Any]], target: str) -> list[str]:
    days = [str(row["date"]) for row in calendar]
    try:
        index = days.index(target)
    except ValueError as exc:
        raise DevelopmentNonReturnCollectionError(f"calendar lacks target date {target}") from exc
    if index < 252:
        raise DevelopmentNonReturnCollectionError(
            f"calendar lacks 252 prior sessions for {target}"
        )
    return days[index - 252 : index]


def _fetch_bars(
    *,
    local: LocalHistoricalClient,
    recording: RecordingHistoricalClient,
    symbol: str,
    start: datetime,
    end: datetime,
    bar_size: str,
    use_rth: bool,
) -> tuple[list[dict[str, Any]], str]:
    try:
        rows = local.fetch_bars(
            symbol, start, end, bar_size=bar_size, use_rth=use_rth
        )
        origin = "CACHE"
    except HistoricalProviderError as exc:
        if exc.category != "local_cache_miss":
            raise
        rows = recording.fetch_bars(
            symbol, start, end, bar_size=bar_size, use_rth=use_rth
        )
        origin = "PROVIDER"
    _validate_boundary(rows, start=start, end=end, inclusive_end=False)
    return rows, origin


def _fetch_trades(
    *,
    local: LocalHistoricalClient,
    recording: RecordingHistoricalClient,
    symbol: str,
    start: datetime,
    end: datetime,
) -> tuple[list[dict[str, Any]], str]:
    try:
        rows = local.fetch_trades(symbol, start, end, use_rth=True)
        origin = "CACHE"
    except HistoricalProviderError as exc:
        if exc.category != "local_cache_miss":
            raise
        rows = recording.fetch_trades(symbol, start, end, use_rth=True)
        origin = "PROVIDER"
    _validate_boundary(rows, start=start, end=end, inclusive_end=False)
    return rows, origin


def _fetch_quotes(
    *,
    local: LocalHistoricalClient,
    recording: RecordingHistoricalClient,
    symbol: str,
    start: datetime,
    end: datetime,
) -> tuple[list[dict[str, Any]], str]:
    try:
        rows = local.fetch_bid_ask_ticks(symbol, start, end, use_rth=True)
        origin = "CACHE"
    except HistoricalProviderError as exc:
        if exc.category != "local_cache_miss":
            raise
        rows = recording.fetch_bid_ask_ticks(symbol, start, end, use_rth=True)
        origin = "PROVIDER"
    _validate_boundary(rows, start=start, end=end, inclusive_end=True)
    return rows, origin


def _initial_pair_state(
    pair: Mapping[str, Any], manifest_sha256: str
) -> dict[str, Any]:
    day = str(pair["date"])
    return {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "manifest_sha256": manifest_sha256,
        "collector_sha256": _sha256_file(Path(__file__)),
        "pair_key": _pair_key(pair),
        "date": day,
        "symbol": pair["symbol"],
        "instrument_id": pair["instrument_id"],
        "rank": pair["rank"],
        "scanner_fields": pair["scanner_fields"],
        "status": "COLLECTING",
        "next_search_at_et": datetime.combine(
            date.fromisoformat(day), time(9, 35), tzinfo=EASTERN
        ).isoformat(),
        "search_windows_complete": 0,
        "search_minute_files": [],
        "active_search_minute": None,
        "clean_cross": None,
        "requests": {},
        "terminal_disposition": None,
        "target_outcome_observed_or_derived": False,
    }


def _load_pair_state(
    path: Path, pair: Mapping[str, Any], manifest_sha256: str
) -> dict[str, Any]:
    if not path.exists():
        return _initial_pair_state(pair, manifest_sha256)
    state = _read_gzip(path)
    if (
        state.get("manifest_sha256") != manifest_sha256
        or state.get("collector_sha256") != _sha256_file(Path(__file__))
        or state.get("pair_key") != _pair_key(pair)
        or state.get("target_outcome_observed_or_derived") is not False
    ):
        raise DevelopmentNonReturnCollectionError("pair checkpoint drifted")
    return state


def _flush_search_minute(
    *,
    store_root: Path,
    pair: Mapping[str, Any],
    state: dict[str, Any],
) -> None:
    active = state.get("active_search_minute")
    if not isinstance(active, Mapping):
        return
    minute_start = datetime.fromisoformat(str(active["minute_start_et"]))
    path = _search_minute_path(store_root, pair, minute_start)
    value = {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "pair_key": state["pair_key"],
        "minute_start_et": minute_start.isoformat(),
        "windows_complete": int(active["windows_complete"]),
        "observations": list(active["observations"]),
        "target_outcome_observed_or_derived": False,
    }
    if path.exists():
        if _read_gzip(path) != value:
            raise DevelopmentNonReturnCollectionError(
                "captured search-minute evidence changed on resume"
            )
    else:
        _write_gzip(path, value)
    metadata = {
        "minute_start_et": minute_start.isoformat(),
        "windows_complete": value["windows_complete"],
        "observations": len(value["observations"]),
        "sha256": _sha256_file(path),
    }
    existing = state.get("search_minute_files")
    if not isinstance(existing, list):
        raise DevelopmentNonReturnCollectionError("search-minute index is malformed")
    if any(row.get("minute_start_et") == minute_start.isoformat() for row in existing):
        raise DevelopmentNonReturnCollectionError("search minute would be written twice")
    existing.append(metadata)
    state["active_search_minute"] = None


def _record_search_window(
    *,
    store_root: Path,
    pair: Mapping[str, Any],
    state_path: Path,
    state: dict[str, Any],
    next_at: datetime,
    rows: Sequence[Mapping[str, Any]],
    clean: Mapping[str, Any] | None,
) -> None:
    window_start = next_at - timedelta(seconds=1)
    minute_start = window_start.replace(second=0, microsecond=0)
    active = state.get("active_search_minute")
    if isinstance(active, Mapping) and active.get(
        "minute_start_et"
    ) != minute_start.isoformat():
        _flush_search_minute(store_root=store_root, pair=pair, state=state)
        active = None
    if not isinstance(active, Mapping):
        active = {
            "minute_start_et": minute_start.isoformat(),
            "windows_complete": 0,
            "observations": [],
        }
        state["active_search_minute"] = active
    active["windows_complete"] = int(active["windows_complete"]) + 1
    active["observations"].extend(dict(row) for row in rows)
    state["next_search_at_et"] = next_at.isoformat()
    state["search_windows_complete"] = int(state["search_windows_complete"]) + 1
    if clean is not None:
        clean_at = _observed(clean)
        state["clean_cross"] = {
            "observed_at_et": clean_at.isoformat(),
            "price": float(clean["price"]),
            "conditions": clean.get("conditions"),
            "tape": clean.get("tape"),
        }
    _write_gzip(state_path, state)


def _collect_pair(
    *,
    pair: Mapping[str, Any],
    manifest_sha256: str,
    store: HistoricalDayStore,
    provider: AlpacaHistoricalClient,
    calendar: Sequence[Mapping[str, Any]],
    halt_client: NasdaqHaltClient,
) -> dict[str, Any]:
    path = _pair_path(store.root, pair)
    state = _load_pair_state(path, pair, manifest_sha256)
    if state.get("status") == "TERMINAL":
        return state
    day = date.fromisoformat(str(pair["date"]))
    symbol = str(pair["symbol"])
    opening_high = float(pair["scanner_fields"]["opening_high"])
    local = LocalHistoricalClient(store, "alpaca", feed="sip", adjustment="raw")
    recording = RecordingHistoricalClient(provider, store)
    open_at = datetime.combine(day, time(9, 30), tzinfo=EASTERN)
    search_at = datetime.combine(day, time(9, 35), tzinfo=EASTERN)
    cutoff = datetime.combine(day, time(10, 30), tzinfo=EASTERN)

    if "opening_bars" not in state["requests"]:
        rows, origin = _fetch_bars(
            local=local,
            recording=recording,
            symbol=symbol,
            start=open_at,
            end=search_at,
            bar_size="1 min",
            use_rth=True,
        )
        state["requests"]["opening_bars"] = {
            "origin": origin,
            "rows": len(rows),
            "sha256": _sha256_json(rows),
            "observations": rows,
            "start_et": open_at.isoformat(),
            "end_et": search_at.isoformat(),
        }
        _write_gzip(path, state)

    cursor = datetime.fromisoformat(str(state["next_search_at_et"]))

    def checkpoint(
        next_at: datetime,
        rows: Sequence[Mapping[str, Any]],
        clean_row: Mapping[str, Any] | None,
    ) -> None:
        _record_search_window(
            store_root=store.root,
            pair=pair,
            state_path=path,
            state=state,
            next_at=next_at,
            rows=rows,
            clean=clean_row,
        )

    stored_clean = state.get("clean_cross")
    if isinstance(stored_clean, Mapping):
        clean = {
            **dict(stored_clean),
            "source_timestamp": stored_clean["observed_at_et"],
        }
    else:
        clean, _windows = search_clean_cross(
            fetch_second=lambda start, end: provider.fetch_trades(
                symbol, start, end, use_rth=True
            ),
            start=cursor,
            end=cutoff,
            opening_high=opening_high,
            checkpoint=checkpoint,
        )
    if clean is None:
        state["status"] = "TERMINAL"
        state["terminal_disposition"] = "NO_CLEAN_CROSS_BEFORE_CUTOFF"
        state["provider_rows_after_final_decision"] = False
        _write_gzip(path, state)
        return state

    clean_at = _observed(clean)
    final_at = clean_at + timedelta(seconds=10)
    state["clean_cross"] = {
        "observed_at_et": clean_at.isoformat(),
        "price": float(clean["price"]),
        "conditions": clean.get("conditions"),
        "tape": clean.get("tape"),
    }
    _write_gzip(path, state)
    if final_at > cutoff:
        state["status"] = "TERMINAL"
        state["terminal_disposition"] = "FINAL_DECISION_AFTER_ENTRY_CUTOFF"
        state["provider_rows_after_final_decision"] = False
        _write_gzip(path, state)
        return state

    decision_trades, decision_origin = _fetch_trades(
        local=local,
        recording=recording,
        symbol=symbol,
        start=open_at,
        end=final_at,
    )
    quote_start = clean_at - timedelta(seconds=1)
    quotes, quote_origin = _fetch_quotes(
        local=local,
        recording=recording,
        symbol=symbol,
        start=quote_start,
        end=final_at,
    )
    snapshots = quote_snapshots(quotes, clean_at)
    completed_end = final_at.replace(second=0, microsecond=0)
    completed: dict[str, Any] = {}
    for requested in (symbol, "QQQ", "SPY"):
        rows, origin = _fetch_bars(
            local=local,
            recording=recording,
            symbol=requested,
            start=open_at,
            end=completed_end,
            bar_size="1 min",
            use_rth=True,
        )
        completed[requested] = {
            "origin": origin,
            "rows": len(rows),
            "sha256": _sha256_json(rows),
            "observations": rows,
            "end_et": completed_end.isoformat(),
        }

    premarket_start = datetime.combine(day, time(4, 0), tzinfo=EASTERN)
    premarket, premarket_origin = _fetch_bars(
        local=local,
        recording=recording,
        symbol=symbol,
        start=premarket_start,
        end=open_at,
        bar_size="1 min",
        use_rth=False,
    )
    prior = _prior_sessions(calendar, day.isoformat())
    history_start = datetime.combine(
        date.fromisoformat(prior[0]), time(9, 30), tzinfo=EASTERN
    )
    history, history_origin = _fetch_bars(
        local=local,
        recording=recording,
        symbol=symbol,
        start=history_start,
        end=open_at,
        bar_size="15 mins",
        use_rth=True,
    )
    halts, halt_source = halt_client.fetch_day(day.isoformat())
    causal_halts = [
        row
        for row in halts
        if str(row.get("symbol", "")).upper() == symbol.upper()
        and datetime.fromisoformat(str(row["halted_at_et"])) <= final_at
    ]
    state["requests"].update(
        {
            "decision_trade_prefix": {
                "origin": decision_origin,
                "rows": len(decision_trades),
                "sha256": _sha256_json(decision_trades),
                "observations": decision_trades,
                "start_et": open_at.isoformat(),
                "end_et": final_at.isoformat(),
            },
            "quote_window": {
                "origin": quote_origin,
                "rows": len(quotes),
                "sha256": _sha256_json(quotes),
                "observations": quotes,
                "start_et": quote_start.isoformat(),
                "end_et_inclusive": final_at.isoformat(),
                "snapshots": snapshots,
            },
            "completed_bars": completed,
            "premarket": {
                "origin": premarket_origin,
                "rows": len(premarket),
                "sha256": _sha256_json(premarket),
                "observations": premarket,
                "start_et": premarket_start.isoformat(),
                "end_et": open_at.isoformat(),
            },
            "history": {
                "origin": history_origin,
                "rows": len(history),
                "sha256": _sha256_json(history),
                "observations": history,
                "required_sessions": len(prior),
                "start_et": history_start.isoformat(),
                "end_et": open_at.isoformat(),
            },
            "halts": {
                "source_sha256": _sha256_json(halt_source),
                "causal_records": causal_halts,
                "causal_record_count": len(causal_halts),
            },
        }
    )
    state["status"] = "TERMINAL"
    state["terminal_disposition"] = "PREENTRY_INPUTS_COLLECTED"
    state["final_decision_at_et"] = final_at.isoformat()
    state["provider_rows_after_final_decision"] = False
    state["target_outcome_observed_or_derived"] = False
    _write_gzip(path, state)
    return state


def _public_collection_status(
    *,
    manifest: Mapping[str, Any],
    states: Sequence[Mapping[str, Any]],
    store_root: Path,
    blocker: str | None = None,
) -> dict[str, Any]:
    terminal = Counter(str(row.get("terminal_disposition") or "INCOMPLETE") for row in states)
    complete = len(states) == EXPECTED_PAIRS and all(
        row.get("status") == "TERMINAL" for row in states
    )
    index = _index_path(store_root)
    return {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "status": "COLLECTION_COMPLETE" if complete else "COLLECTION_INCOMPLETE",
        "blocker": blocker,
        "pairs_expected": EXPECTED_PAIRS,
        "pairs_recorded": len(states),
        "pairs_terminal": sum(row.get("status") == "TERMINAL" for row in states),
        "terminal_counts": dict(sorted(terminal.items())),
        "one_second_windows_complete": sum(
            int(row.get("search_windows_complete", 0)) for row in states
        ),
        "private_index_sha256": _sha256_file(index) if index.exists() else None,
        "symbols_dates_instrument_ids_raw_rows_and_requests_public": False,
        "provider_rows_after_final_decision": False,
        "post_entry_data_access_allowed": False,
        "target_outcomes_observed_or_derived": False,
    }


def _search_evidence(
    *,
    store_root: Path,
    pair: Mapping[str, Any],
    state: Mapping[str, Any],
) -> tuple[list[dict[str, Any]], int]:
    rows: list[dict[str, Any]] = []
    windows = 0
    files = state.get("search_minute_files")
    if not isinstance(files, list):
        raise DevelopmentNonReturnCollectionError("search-minute file index is missing")
    for metadata in files:
        if not isinstance(metadata, Mapping):
            raise DevelopmentNonReturnCollectionError(
                "search-minute file metadata is malformed"
            )
        minute_start = datetime.fromisoformat(str(metadata["minute_start_et"]))
        path = _search_minute_path(store_root, pair, minute_start)
        if not path.is_file() or _sha256_file(path) != metadata.get("sha256"):
            raise DevelopmentNonReturnCollectionError("search-minute file drifted")
        value = _read_gzip(path)
        observations = value.get("observations")
        if (
            value.get("dataset_id") != DATASET_ID
            or value.get("pair_key") != state.get("pair_key")
            or value.get("minute_start_et") != metadata.get("minute_start_et")
            or value.get("windows_complete") != metadata.get("windows_complete")
            or value.get("target_outcome_observed_or_derived") is not False
            or not isinstance(observations, list)
            or len(observations) != metadata.get("observations")
        ):
            raise DevelopmentNonReturnCollectionError(
                "search-minute evidence is malformed"
            )
        windows += int(value["windows_complete"])
        rows.extend(dict(row) for row in observations if isinstance(row, Mapping))
    active = state.get("active_search_minute")
    if active is not None:
        if not isinstance(active, Mapping) or not isinstance(
            active.get("observations"), list
        ):
            raise DevelopmentNonReturnCollectionError(
                "active search-minute evidence is malformed"
            )
        windows += int(active["windows_complete"])
        rows.extend(
            dict(row)
            for row in active["observations"]
            if isinstance(row, Mapping)
        )
    return rows, windows


def collect(
    *, manifest_path: Path, env_path: Path, public_status_path: Path
) -> dict[str, Any]:
    manifest = load_frozen_dataset_contract(manifest_path)
    if manifest.get("dataset_id") != DATASET_ID:
        raise DevelopmentNonReturnCollectionError("unexpected collector manifest")
    _published(Path(__file__))
    _published(manifest_path)
    base, _public, private, store = _load_base(env_path)
    expected = _expected_contract(
        base_manifest=base,
        provider_contract=_provider_contract(env_path),
        free_bytes=int(manifest["capacity_contract"]["observed_free_bytes_at_freeze"]),
    )
    for key, value in expected.items():
        if manifest.get(key) != value:
            raise DevelopmentNonReturnCollectionError(
                f"published collector {key} drifted"
            )
    for value in manifest["implementation_contract"]["files"].values():
        if not isinstance(value, Mapping):
            raise DevelopmentNonReturnCollectionError(
                "collector implementation binding is malformed"
            )
        _verify_binding(value)
    for value in manifest["lineage_contract"].values():
        if isinstance(value, Mapping) and "path" in value:
            _verify_binding(value)
    if manifest["lineage_contract"]["base_manifest"]["manifest_sha256"] != base[
        "manifest_sha256"
    ]:
        raise DevelopmentNonReturnCollectionError("collector base manifest drifted")
    if shutil.disk_usage(store.root).free < source_contract.MINIMUM_RESERVE_BYTES:
        raise DevelopmentNonReturnCollectionError("historical store is below reserve")
    alpaca = AlpacaConfig.optional_from_env(env_path)
    if alpaca is None or alpaca.public_dict() != manifest["provider_contract"]["alpaca"]:
        raise DevelopmentNonReturnCollectionError("Alpaca configuration drifted")
    massive = MassiveReferenceConfig.from_env(env_path)
    if massive.public_dict() != manifest["provider_contract"]["massive"]:
        raise DevelopmentNonReturnCollectionError("Massive configuration drifted")

    calendar, calendar_origin = _collect_calendar(
        env_path=env_path, output=_calendar_path(store.root)
    )
    split_status = collect_split_actions(
        start=source_contract.CALENDAR_QUERY_START,
        end=source_contract.CALENDAR_QUERY_END,
        config=massive,
        output=_splits_path(store.root),
    )
    pairs = private["selection"]["positive_pairs"]
    states: list[dict[str, Any]] = []
    halt_client = NasdaqHaltClient(
        _private_root(store.root) / HALT_DIRECTORY
    )
    with AlpacaHistoricalClient(alpaca) as provider:
        for index, pair in enumerate(pairs, 1):
            state = _collect_pair(
                pair=pair,
                manifest_sha256=str(manifest["manifest_sha256"]),
                store=store,
                provider=provider,
                calendar=calendar,
                halt_client=halt_client,
            )
            states.append(state)
            index_value = {
                "schema_version": 1,
                "dataset_id": DATASET_ID,
                "manifest_sha256": manifest["manifest_sha256"],
                "calendar_origin": calendar_origin,
                "calendar_sha256": _sha256_file(_calendar_path(store.root)),
                "split_status": split_status,
                "pairs_expected": EXPECTED_PAIRS,
                "pair_files": [
                    {
                        "pair_key": str(item["pair_key"]),
                        "sha256": _sha256_file(
                            _private_root(store.root)
                            / PAIR_DIRECTORY
                            / f"{item['pair_key']}.json.gz"
                        ),
                    }
                    for item in states
                ],
                "target_outcomes_observed_or_derived": False,
            }
            _write_gzip(_index_path(store.root), index_value)
            print(
                f"pair {index}/{len(pairs)}: {state['terminal_disposition'] or state['status']}",
                flush=True,
            )
            if shutil.disk_usage(store.root).free < source_contract.MINIMUM_RESERVE_BYTES:
                raise DevelopmentNonReturnCollectionError(
                    "historical store crossed the 20-GiB reserve"
                )
    result = _public_collection_status(
        manifest=manifest, states=states, store_root=store.root
    )
    _write_json(public_status_path, result)
    return result


def inspect_collection(
    *, manifest_path: Path, env_path: Path, public_status_path: Path
) -> dict[str, Any]:
    manifest = load_frozen_dataset_contract(manifest_path)
    if manifest.get("dataset_id") != DATASET_ID:
        raise DevelopmentNonReturnCollectionError("unexpected collector manifest")
    base, _public, private, store = _load_base(env_path)
    expected = _expected_contract(
        base_manifest=base,
        provider_contract=_provider_contract(env_path),
        free_bytes=int(manifest["capacity_contract"]["observed_free_bytes_at_freeze"]),
    )
    for key, value in expected.items():
        if manifest.get(key) != value:
            raise DevelopmentNonReturnCollectionError(
                f"collector inspection {key} drifted"
            )
    index = _read_gzip(_index_path(store.root))
    pairs = private["selection"]["positive_pairs"]
    states = [_read_gzip(_pair_path(store.root, pair)) for pair in pairs]
    if (
        index.get("manifest_sha256") != manifest["manifest_sha256"]
        or index.get("pairs_expected") != EXPECTED_PAIRS
        or index.get("target_outcomes_observed_or_derived") is not False
        or len(index.get("pair_files", [])) != len(states)
    ):
        raise DevelopmentNonReturnCollectionError("private collection index drifted")
    indexed = {
        str(row["pair_key"]): str(row["sha256"])
        for row in index["pair_files"]
        if isinstance(row, Mapping)
    }
    for pair, state in zip(pairs, states, strict=True):
        path = _pair_path(store.root, pair)
        if (
            indexed.get(_pair_key(pair)) != _sha256_file(path)
            or state.get("manifest_sha256") != manifest["manifest_sha256"]
            or state.get("status") != "TERMINAL"
            or state.get("target_outcome_observed_or_derived") is not False
            or state.get("provider_rows_after_final_decision") is True
        ):
            raise DevelopmentNonReturnCollectionError("pair collection failed inspection")
        day = date.fromisoformat(str(state["date"]))
        open_at = datetime.combine(day, time(9, 30), tzinfo=EASTERN)
        search_at = datetime.combine(day, time(9, 35), tzinfo=EASTERN)
        cursor = datetime.fromisoformat(str(state["next_search_at_et"]))
        expected_windows = int((cursor - search_at).total_seconds())
        search_rows, rebuilt_windows = _search_evidence(
            store_root=store.root, pair=pair, state=state
        )
        if (
            expected_windows != state.get("search_windows_complete")
            or rebuilt_windows != expected_windows
            or cursor > datetime.combine(day, time(10, 30), tzinfo=EASTERN)
        ):
            raise DevelopmentNonReturnCollectionError(
                "chronological search checkpoint does not reconcile"
            )
        _validate_boundary(
            search_rows, start=search_at, end=cursor, inclusive_end=False
        )
        opening = state.get("requests", {}).get("opening_bars")
        opening_rows = (
            opening.get("observations") if isinstance(opening, Mapping) else None
        )
        if (
            not isinstance(opening_rows, list)
            or _sha256_json(opening_rows) != opening.get("sha256")
            or len(opening_rows) != opening.get("rows")
        ):
            raise DevelopmentNonReturnCollectionError(
                "private opening-bar observations drifted"
            )
        _validate_boundary(
            opening_rows, start=open_at, end=search_at, inclusive_end=False
        )
        clean = state.get("clean_cross")
        if isinstance(clean, Mapping):
            decision = classify_trade_conditions(
                clean.get("tape"), clean.get("conditions")
            )
            if (
                not decision.establishes_continuous_cross
                or float(clean.get("price", 0))
                <= float(state["scanner_fields"]["opening_high"])
            ):
                raise DevelopmentNonReturnCollectionError(
                    "stored clean cross does not independently classify"
                )
        if state.get("final_decision_at_et"):
            final = datetime.fromisoformat(str(state["final_decision_at_et"]))
            for name in ("decision_trade_prefix", "quote_window", "completed_bars"):
                if name not in state.get("requests", {}):
                    raise DevelopmentNonReturnCollectionError(
                        f"terminal collected pair lacks {name}"
                    )
            if datetime.fromisoformat(
                str(state["requests"]["decision_trade_prefix"]["end_et"])
            ) != final:
                raise DevelopmentNonReturnCollectionError(
                    "decision prefix does not end at final snapshot"
                )
            decision = state["requests"]["decision_trade_prefix"]
            decision_rows = decision.get("observations")
            quote = state["requests"]["quote_window"]
            quote_rows = quote.get("observations")
            if not isinstance(decision_rows, list) or not isinstance(quote_rows, list):
                raise DevelopmentNonReturnCollectionError(
                    "collected pair lacks private raw decision observations"
                )
            if (
                _sha256_json(decision_rows) != decision.get("sha256")
                or len(decision_rows) != decision.get("rows")
                or _sha256_json(quote_rows) != quote.get("sha256")
                or len(quote_rows) != quote.get("rows")
            ):
                raise DevelopmentNonReturnCollectionError(
                    "private decision observation hash drifted"
                )
            _validate_boundary(
                decision_rows, start=open_at, end=final, inclusive_end=False
            )
            quote_start = datetime.fromisoformat(str(quote["start_et"]))
            _validate_boundary(
                quote_rows, start=quote_start, end=final, inclusive_end=True
            )
            clean_at = datetime.fromisoformat(
                str(state["clean_cross"]["observed_at_et"])
            )
            if quote_snapshots(quote_rows, clean_at) != quote.get("snapshots"):
                raise DevelopmentNonReturnCollectionError(
                    "quote snapshots do not independently rebuild"
                )
            for name in ("opening_bars", "premarket", "history"):
                request = state["requests"].get(name)
                rows = request.get("observations") if isinstance(request, Mapping) else None
                if (
                    not isinstance(rows, list)
                    or _sha256_json(rows) != request.get("sha256")
                    or len(rows) != request.get("rows")
                ):
                    raise DevelopmentNonReturnCollectionError(
                        f"private {name} observations drifted"
                    )
                _validate_boundary(
                    rows,
                    start=datetime.fromisoformat(str(request["start_et"])),
                    end=datetime.fromisoformat(str(request["end_et"])),
                    inclusive_end=False,
                )
            for request in state["requests"]["completed_bars"].values():
                rows = request.get("observations") if isinstance(request, Mapping) else None
                if (
                    not isinstance(rows, list)
                    or _sha256_json(rows) != request.get("sha256")
                    or len(rows) != request.get("rows")
                ):
                    raise DevelopmentNonReturnCollectionError(
                        "private completed-bar observations drifted"
                    )
                _validate_boundary(
                    rows,
                    start=open_at,
                    end=datetime.fromisoformat(str(request["end_et"])),
                    inclusive_end=False,
                )
    result = _public_collection_status(
        manifest=manifest, states=states, store_root=store.root
    )
    if result["status"] != "COLLECTION_COMPLETE":
        raise DevelopmentNonReturnCollectionError("collection is incomplete")
    result["status"] = "COLLECTION_INSPECTED"
    result["inspected"] = True
    result["inspection"] = {
        "manifest_and_implementation_rebuilt": True,
        "pair_hashes_rebuilt": True,
        "chronological_search_cursors_reconciled": True,
        "final_decision_boundaries_rechecked": True,
        "privacy_and_outcome_locks_rechecked": True,
        "valid": True,
    }
    _write_json(public_status_path, result)
    return result


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "command", choices=("freeze", "inspect-contract", "collect", "inspect")
    )
    parser.add_argument("manifest", nargs="?", type=Path)
    parser.add_argument("--env", type=Path, default=PROJECT_ROOT / ".env")
    parser.add_argument("--output-root", type=Path, default=DEFAULT_MANIFEST_ROOT)
    parser.add_argument("--contract-status", type=Path, default=DEFAULT_CONTRACT_STATUS)
    parser.add_argument("--collection-status", type=Path, default=DEFAULT_COLLECTION_STATUS)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "freeze":
            path, manifest = freeze(
                env_path=args.env,
                output_root=args.output_root,
                public_status_path=args.contract_status,
            )
            result: Any = {
                "dataset_id": manifest["dataset_id"],
                "manifest_sha256": manifest["manifest_sha256"],
                "path": _repo_path(path),
                "positive_pairs": EXPECTED_PAIRS,
            }
        elif args.manifest is None:
            raise DevelopmentNonReturnCollectionError(
                f"{args.command} requires a manifest"
            )
        elif args.command == "inspect-contract":
            result = inspect_contract(
                manifest_path=args.manifest,
                env_path=args.env,
                public_status_path=args.contract_status,
            )
        elif args.command == "collect":
            result = collect(
                manifest_path=args.manifest,
                env_path=args.env,
                public_status_path=args.collection_status,
            )
        else:
            result = inspect_collection(
                manifest_path=args.manifest,
                env_path=args.env,
                public_status_path=args.collection_status,
            )
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    except (
        DevelopmentNonReturnCollectionError,
        HistoricalProviderError,
        HistoricalStoreError,
        LearningDataError,
        NasdaqHaltError,
        ScannerReplayError,
        OSError,
        ValueError,
        requests.RequestException,
    ) as exc:
        print(
            json.dumps(
                {
                    "status": "error",
                    "error": str(exc),
                    "error_type": type(exc).__name__,
                },
                indent=2,
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())

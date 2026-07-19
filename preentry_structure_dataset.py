"""Freeze, collect, and inspect outcome-blind ORB pre-entry structure inputs."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import io
import json
import os
import statistics
import sys
import time as time_module
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime, time, timedelta
from pathlib import Path
from typing import Any

import requests

from historical_providers import (
    AlpacaConfig,
    AlpacaHistoricalClient,
    HistoricalProviderError,
)
from historical_service import LocalHistoricalClient, RecordingHistoricalClient
from historical_store import EASTERN, HistoricalDayStore, expand_bar
from learning_data import (
    LearningDataError,
    freeze_dataset_contract,
    load_frozen_dataset_contract,
)
from preentry_structure import (
    CONTRACT_VERSION,
    REQUIRED_LONG_DAILY_BARS,
    SOURCE_URLS,
    PreentryStructureError,
    derive_preentry_structure,
)
from scanner_replay import (
    MassiveReferenceConfig,
    ScannerReplayError,
    collect_split_actions,
    load_split_actions,
    split_adjustment_factor,
)
from strategy_engine import load_config


PROJECT_ROOT = Path(__file__).resolve().parent
DATASET_ID = "dataset-preentry-structure-fidelity-2026-07-19-v2"
SUPERSEDED_DATASET_ID = "dataset-preentry-structure-fidelity-2026-07-19-v1"
SOURCE_DATASET_ID = "dataset-champion-input-readiness-2026-07-19-v1"
SOURCE_MANIFEST = (
    PROJECT_ROOT
    / "historical_batches"
    / "champion_input_readiness"
    / "manifests"
    / "dataset-champion-input-readiness-2026-07-19-v1-00f25fa34ad8b99803e3c5faad6480c3a8394258e5660b9de4a60363b75b955a.json"
)
CALENDAR_PATH = (
    PROJECT_ROOT
    / "historical_batches"
    / "preentry_structure"
    / "session-calendar-2024-12-through-2026-06.json"
)
CALENDAR_SOURCE_PATH = (
    PROJECT_ROOT
    / "historical_batches"
    / "preentry_structure"
    / "session-calendar-source.json"
)
DEFAULT_MANIFEST_ROOT = (
    PROJECT_ROOT / "historical_batches" / "preentry_structure" / "manifests"
)
DEFAULT_PUBLIC_STATUS = (
    PROJECT_ROOT
    / "historical_batches"
    / "preentry_structure"
    / "collection-status.json"
)
DEFAULT_PUBLIC_RESULT = (
    PROJECT_ROOT / "research_results" / "2026-07-19-preentry-structure.json"
)
CALENDAR_START = "2024-12-01"
CALENDAR_END = "2026-06-30"
MINIMUM_FREE_BYTES = 10 * 1024**3


class PreentryDatasetError(RuntimeError):
    """The frozen pre-entry structure dataset cannot progress faithfully."""


def _timestamp_now() -> str:
    return datetime.now(UTC).isoformat()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _canonical_sha256(value: Any) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def _read_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise PreentryDatasetError(f"cannot read {path}: {exc}") from exc


def _read_gzip(path: Path) -> Any:
    try:
        with gzip.open(path, "rt", encoding="utf-8") as source:
            return json.load(source)
    except (OSError, json.JSONDecodeError) as exc:
        raise PreentryDatasetError(f"cannot read {path}: {exc}") from exc


def _write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(
        json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    os.replace(temporary, path)


def _write_gzip(path: Path, value: Any) -> None:
    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode="wb", compresslevel=6, mtime=0) as stream:
        stream.write(json.dumps(value, indent=2, sort_keys=True).encode("utf-8"))
        stream.write(b"\n")
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_bytes(buffer.getvalue())
    os.replace(temporary, path)


def _repo_path(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(PROJECT_ROOT))
    except ValueError as exc:
        raise PreentryDatasetError("public path must be repository-relative") from exc


def _source_paths(root: Path) -> dict[str, Path]:
    return {
        "selection": (
            root
            / "_derived"
            / "selected_candidate_join"
            / "dataset-selected-candidate-join-2026-07-19-v1"
            / "selected-pairs.json.gz"
        ),
        "triggers": (
            root
            / "_derived"
            / "selected_candidate_fidelity"
            / "dataset-selected-candidate-fidelity-2026-07-19-v1"
            / "clean-trigger-index.json.gz"
        ),
        "readiness": (
            root
            / "_derived"
            / "champion_input_readiness"
            / SOURCE_DATASET_ID
            / "readiness-index.json.gz"
        ),
    }


def _private_paths(root: Path) -> dict[str, Path]:
    base = root / "_derived" / "preentry_structure" / DATASET_ID
    return {
        "acquisition": base / "acquisition-index.json.gz",
        "splits": base / "splits.json.gz",
        "result": base / "structure-index.json.gz",
    }


def collect_calendar(
    *, env_path: Path, calendar_path: Path, source_path: Path
) -> dict[str, Any]:
    config = AlpacaConfig.optional_from_env(env_path)
    if config is None:
        raise PreentryDatasetError("Alpaca credentials are required for calendar proof")
    response = requests.get(
        "https://api.alpaca.markets/v2/calendar",
        params={"start": CALENDAR_START, "end": CALENDAR_END},
        headers={
            "APCA-API-KEY-ID": config.api_key,
            "APCA-API-SECRET-KEY": config.api_secret,
        },
        timeout=config.timeout_seconds,
    )
    if response.status_code != 200:
        raise PreentryDatasetError(f"Alpaca calendar HTTP {response.status_code}")
    try:
        payload = response.json()
    except ValueError as exc:
        raise PreentryDatasetError("Alpaca calendar returned invalid JSON") from exc
    if not isinstance(payload, list) or not payload:
        raise PreentryDatasetError("Alpaca calendar response is empty")
    rows: list[dict[str, str]] = []
    seen: set[str] = set()
    for item in payload:
        if not isinstance(item, Mapping):
            raise PreentryDatasetError("Alpaca calendar row is malformed")
        day = date.fromisoformat(str(item.get("date")))
        opened = str(item.get("open") or "")
        closed = str(item.get("close") or "")
        time.fromisoformat(opened)
        time.fromisoformat(closed)
        if day.isoformat() in seen:
            raise PreentryDatasetError("Alpaca calendar contains a duplicate date")
        seen.add(day.isoformat())
        rows.append({"date": day.isoformat(), "open_et": opened, "close_et": closed})
    rows.sort(key=lambda row: row["date"])
    if rows[0]["date"] < CALENDAR_START or rows[-1]["date"] > CALENDAR_END:
        raise PreentryDatasetError("Alpaca calendar response escaped its query")
    _write_json(calendar_path, rows)
    source = {
        "schema_version": 1,
        "provider": "Alpaca Market Calendar API",
        "endpoint": "https://api.alpaca.markets/v2/calendar",
        "query": {"start": CALENDAR_START, "end": CALENDAR_END},
        "retrieved_at": _timestamp_now(),
        "sessions": len(rows),
        "first_session": rows[0]["date"],
        "last_session": rows[-1]["date"],
        "calendar_path": _repo_path(calendar_path),
        "calendar_sha256": _sha256_file(calendar_path),
        "credentials_configured": True,
    }
    _write_json(source_path, source)
    return source


def _calendar() -> list[dict[str, str]]:
    rows = _read_json(CALENDAR_PATH)
    if not isinstance(rows, list) or len(rows) < 252:
        raise PreentryDatasetError("extended session calendar is incomplete")
    normalized = []
    for row in rows:
        if not isinstance(row, Mapping):
            raise PreentryDatasetError("calendar row is malformed")
        day = date.fromisoformat(str(row["date"])).isoformat()
        opened = time.fromisoformat(str(row["open_et"])).isoformat(timespec="minutes")
        closed = time.fromisoformat(str(row["close_et"])).isoformat(timespec="minutes")
        normalized.append({"date": day, "open_et": opened, "close_et": closed})
    if normalized != sorted(normalized, key=lambda row: row["date"]):
        raise PreentryDatasetError("calendar must be strictly ordered")
    if len({row["date"] for row in normalized}) != len(normalized):
        raise PreentryDatasetError("calendar dates must be unique")
    return normalized


def _required_sessions(target: str, calendar: Sequence[Mapping[str, str]]) -> list[str]:
    dates = [str(row["date"]) for row in calendar]
    try:
        index = dates.index(target)
    except ValueError as exc:
        raise PreentryDatasetError(
            f"target date {target} is absent from calendar"
        ) from exc
    if index < REQUIRED_LONG_DAILY_BARS:
        raise PreentryDatasetError(f"calendar lacks 252 prior sessions for {target}")
    return dates[index - REQUIRED_LONG_DAILY_BARS : index]


def _source_records(
    store: HistoricalDayStore,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    paths = _source_paths(store.root)
    selection = _read_gzip(paths["selection"])
    triggers = _read_gzip(paths["triggers"])
    readiness = _read_gzip(paths["readiness"])
    if (
        selection.get("selected_pair_count") != 389
        or triggers.get("status") != "CLEAN_TRIGGER_INSPECTION_COMPLETE"
        or triggers.get("counts", {}).get("crossing_windows") != 325
        or readiness.get("status") != "READINESS_JOIN_COMPLETE"
        or readiness.get("target_outcomes_observed_or_derived") is not False
    ):
        raise PreentryDatasetError("source readiness corpus is incomplete")
    pairs = {
        (str(row["date"]), str(row["instrument_id"])): row
        for row in selection["selected_pairs"]
    }
    records = []
    for trigger in triggers["records"]:
        key = (str(trigger["date"]), str(trigger["instrument_id"]))
        pair = pairs.get(key)
        if pair is None or pair.get("symbol") != trigger.get("symbol"):
            raise PreentryDatasetError(
                "trigger is not bound to exact scanner selection"
            )
        records.append({"trigger": trigger, "pair": pair})
    records.sort(key=lambda row: (row["trigger"]["date"], row["trigger"]["rank"]))
    return records, {name: _sha256_file(path) for name, path in paths.items()}


def freeze_inputs(*, env_path: Path, output_root: Path) -> tuple[Path, dict[str, Any]]:
    store = HistoricalDayStore.from_env(env_path)
    try:
        source_manifest = load_frozen_dataset_contract(SOURCE_MANIFEST)
    except LearningDataError as exc:
        raise PreentryDatasetError(str(exc)) from exc
    records, source_hashes = _source_records(store)
    calendar = _calendar()
    calendar_source = _read_json(CALENDAR_SOURCE_PATH)
    if calendar_source.get("calendar_sha256") != _sha256_file(CALENDAR_PATH):
        raise PreentryDatasetError("calendar source attestation does not match")
    targets = list(source_manifest["requested_dates"])
    for target in targets:
        _required_sessions(target, calendar)
    strategy = load_config()
    risk = strategy.raw["risk"]
    pair_identity = [
        {
            "date": row["trigger"]["date"],
            "instrument_id": row["trigger"]["instrument_id"],
            "rank": row["trigger"]["rank"],
        }
        for row in records
    ]
    contract = {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "registered_at": _timestamp_now(),
        "requested_dates": targets,
        "dataset_payload": {
            "lane": "development",
            "claim_scope": "DEVELOPMENT_ONLY",
            "status": "COLLECTING",
            "inspected": False,
            "evidence_paths": [
                _repo_path(SOURCE_MANIFEST),
                _repo_path(CALENDAR_PATH),
                _repo_path(CALENDAR_SOURCE_PATH),
                "CHAMPION_INPUT_READINESS.md",
                "SIP_BAR_AGGREGATION.md",
            ],
        },
        "selection_contract": {
            "source_dataset_id": SOURCE_DATASET_ID,
            "source_manifest_sha256": source_manifest["manifest_sha256"],
            "source_artifact_sha256": source_hashes,
            "selected_pair_count": 389,
            "clean_trigger_count": 325,
            "clean_trigger_identity_sha256": _canonical_sha256(pair_identity),
            "raw_rows_and_symbols_public": False,
            "exact_quote_snapshots_required": 3,
        },
        "calendar_contract": {
            "path": _repo_path(CALENDAR_PATH),
            "sha256": _sha256_file(CALENDAR_PATH),
            "source_path": _repo_path(CALENDAR_SOURCE_PATH),
            "source_sha256": _sha256_file(CALENDAR_SOURCE_PATH),
            "required_prior_sessions": REQUIRED_LONG_DAILY_BARS,
        },
        "structure_contract": {
            "version": CONTRACT_VERSION,
            "implementation_sha256": _sha256_file(
                PROJECT_ROOT / "preentry_structure.py"
            ),
            "dataset_implementation_sha256": _sha256_file(Path(__file__)),
            "source_urls": list(SOURCE_URLS),
            "opening_support": "exact first-five-minute high",
            "noise_zone": "max(median observed spread dollars, mean absolute adjacent completed 1m close increment over up to 60 target-session bars)",
            "resistance_levels": [
                "complete target-date 04:00-09:30 ET premarket high",
                "prior completed-session high",
                "prior 14-completed-session high",
                "prior 252-completed-session high",
            ],
            "clear_sky_requires": "complete premarket window plus 252 target-basis sessions with no overhead level",
            "atr_stop_fraction": float(risk["atr_stop_fraction"]),
            "maximum_stop_fraction": float(risk["maximum_stop_fraction"]),
            "minimum_resistance_room_fraction": float(
                risk["minimum_resistance_room_fraction"]
            ),
        },
        "market_data_contract": {
            "provider": "Alpaca Market Data API",
            "feed": "sip",
            "adjustment": "raw",
            "premarket": {
                "timeframe": "1m",
                "window_et": "04:00:00-09:30:00",
                "session": "all",
            },
            "daily_high_source": {
                "timeframe": "15m",
                "window_et": "regular session through prior completed date",
                "sessions": 252,
                "daily_high": "maximum raw SIP 15m high",
            },
            "split_source": "Massive /stocks/v1/splits",
            "split_basis": "target-date share basis using execution dates no later than target",
            "missing_history_policy": "provider-exhausted gaps remain unresolved; observed split-adjusted highs may only supply adverse evidence",
            "minimum_free_bytes": MINIMUM_FREE_BYTES,
        },
        "claim_boundary": {
            "target_outcomes_observed_or_derived": False,
            "strategy_variant_invented": False,
            "production_rule_change_earned_by_contract": False,
        },
        "supersedes": {
            "dataset_id": SUPERSEDED_DATASET_ID,
            "reason": "v1 build did not fail closed before indexing a clean-trigger record with fewer than three quote snapshots",
            "dates_identities_thresholds_and_strategy_semantics_changed": False,
        },
    }
    return freeze_dataset_contract(contract, output_root)


def _verify(
    manifest_path: Path, store: HistoricalDayStore
) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, str]]]:
    try:
        manifest = load_frozen_dataset_contract(manifest_path)
    except LearningDataError as exc:
        raise PreentryDatasetError(str(exc)) from exc
    if manifest.get("dataset_id") != DATASET_ID:
        raise PreentryDatasetError("unexpected pre-entry structure dataset")
    expected = {
        "implementation_sha256": _sha256_file(PROJECT_ROOT / "preentry_structure.py"),
        "dataset_implementation_sha256": _sha256_file(Path(__file__)),
    }
    for field, digest in expected.items():
        if manifest["structure_contract"].get(field) != digest:
            raise PreentryDatasetError(f"{field} differs from frozen contract")
    if manifest["calendar_contract"].get("sha256") != _sha256_file(CALENDAR_PATH):
        raise PreentryDatasetError("calendar differs from frozen contract")
    records, source_hashes = _source_records(store)
    if manifest["selection_contract"].get("source_artifact_sha256") != source_hashes:
        raise PreentryDatasetError("source artifacts differ from frozen contract")
    identity = [
        {
            "date": row["trigger"]["date"],
            "instrument_id": row["trigger"]["instrument_id"],
            "rank": row["trigger"]["rank"],
        }
        for row in records
    ]
    if manifest["selection_contract"].get(
        "clean_trigger_identity_sha256"
    ) != _canonical_sha256(identity):
        raise PreentryDatasetError(
            "clean trigger identity differs from frozen contract"
        )
    return manifest, records, _calendar()


def _retry(call: Any, *, attempts: int = 8) -> Any:
    last: Exception | None = None
    for index in range(attempts):
        try:
            return call()
        except HistoricalProviderError as exc:
            last = exc
            if not exc.retryable or index + 1 >= attempts:
                raise
            time_module.sleep(min(30.0, 2.0**index))
    if last is not None:
        raise last
    raise PreentryDatasetError("provider retry loop did not execute")


def _dataset_complete_15m(store: HistoricalDayStore, symbol: str, day: str) -> bool:
    return (
        store.select_dataset(
            symbol,
            day,
            kind="bars",
            channel="trades",
            timeframe="15m",
            providers=("alpaca",),
            require_complete=True,
            feed="sip",
            adjustment="raw",
        )
        is not None
    )


def _calendar_row(calendar: Sequence[Mapping[str, str]], day: str) -> Mapping[str, str]:
    for row in calendar:
        if row["date"] == day:
            return row
    raise PreentryDatasetError(f"calendar row missing for {day}")


def _collection_coverage(
    acquisition: Mapping[str, Any],
    *,
    expected_daily_identities: int,
    expected_premarket_windows: int,
) -> dict[str, int | str]:
    daily = acquisition.get("daily_requests", {})
    premarket = acquisition.get("premarket_requests", {})
    if not isinstance(daily, Mapping) or not isinstance(premarket, Mapping):
        raise PreentryDatasetError("acquisition request maps are malformed")
    daily_requests_complete = sum(
        row.get("status") in {"COMPLETE", "CACHED"}
        for row in daily.values()
        if isinstance(row, Mapping)
    )
    daily_full_coverage = sum(
        row.get("status") in {"COMPLETE", "CACHED"}
        and row.get("complete_sessions") == row.get("required_sessions")
        for row in daily.values()
        if isinstance(row, Mapping)
    )
    premarket_complete = sum(
        row.get("status") in {"COMPLETE", "CACHED"}
        for row in premarket.values()
        if isinstance(row, Mapping)
    )
    status = (
        "COLLECTION_COMPLETE"
        if daily_requests_complete == expected_daily_identities
        and premarket_complete == expected_premarket_windows
        else "COLLECTION_INCOMPLETE"
    )
    return {
        "status": status,
        "daily_identity_requests_complete": daily_requests_complete,
        "daily_identities_full_252_session_coverage": daily_full_coverage,
        "daily_identities_with_coverage_gaps": (
            expected_daily_identities - daily_full_coverage
        ),
        "premarket_windows_complete": premarket_complete,
    }


def collect(
    *, manifest_path: Path, env_path: Path, public_status_path: Path
) -> dict[str, Any]:
    store = HistoricalDayStore.from_env(env_path)
    manifest, records, calendar = _verify(manifest_path, store)
    private = _private_paths(store.root)
    free = os.statvfs(store.root).f_bavail * os.statvfs(store.root).f_frsize
    if free < int(manifest["market_data_contract"]["minimum_free_bytes"]):
        raise PreentryDatasetError("historical store has less than the frozen reserve")
    alpaca = AlpacaConfig.optional_from_env(env_path)
    if alpaca is None or alpaca.feed != "sip" or alpaca.adjustment != "raw":
        raise PreentryDatasetError("raw Alpaca SIP configuration is required")

    split_start = min(
        _required_sessions(str(row["trigger"]["date"]), calendar)[0] for row in records
    )
    split_end = max(str(row["trigger"]["date"]) for row in records)
    split_status = collect_split_actions(
        start=split_start,
        end=split_end,
        config=MassiveReferenceConfig.from_env(env_path),
        output=private["splits"],
    )

    previous: dict[str, Any]
    if private["acquisition"].exists():
        previous = _read_gzip(private["acquisition"])
        if previous.get("manifest_sha256") != manifest["manifest_sha256"]:
            raise PreentryDatasetError("acquisition cache belongs to another manifest")
    else:
        previous = {
            "schema_version": 1,
            "dataset_id": DATASET_ID,
            "manifest_sha256": manifest["manifest_sha256"],
            "status": "COLLECTING",
            "daily_requests": {},
            "premarket_requests": {},
            "errors": [],
        }

    required_by_identity: dict[tuple[str, str], set[str]] = defaultdict(set)
    for row in records:
        trigger = row["trigger"]
        key = (str(trigger["instrument_id"]), str(trigger["symbol"]))
        required_by_identity[key].update(
            _required_sessions(str(trigger["date"]), calendar)
        )

    with AlpacaHistoricalClient(alpaca) as provider:
        recording = RecordingHistoricalClient(provider, store)
        for identity_index, ((instrument_id, symbol), sessions) in enumerate(
            sorted(required_by_identity.items()), 1
        ):
            cached_state = previous["daily_requests"].get(instrument_id, {})
            if cached_state.get("status") in {"COMPLETE", "CACHED"}:
                print(
                    f"daily {identity_index}/{len(required_by_identity)}: CACHED",
                    flush=True,
                )
                continue
            missing = sorted(
                day for day in sessions if not _dataset_complete_15m(store, symbol, day)
            )
            key = instrument_id
            if not missing:
                previous["daily_requests"][key] = {
                    "symbol": symbol,
                    "required_sessions": len(sessions),
                    "complete_sessions": len(sessions),
                    "status": "CACHED",
                }
            else:
                first = _calendar_row(calendar, missing[0])
                last = _calendar_row(calendar, missing[-1])
                start = datetime.combine(
                    date.fromisoformat(first["date"]),
                    time.fromisoformat(first["open_et"]),
                    tzinfo=EASTERN,
                )
                end = datetime.combine(
                    date.fromisoformat(last["date"]), time(16, 0), tzinfo=EASTERN
                )
                try:
                    rows = _retry(
                        lambda: recording.fetch_bars(
                            symbol,
                            start,
                            end,
                            bar_size="15 mins",
                            use_rth=True,
                        )
                    )
                    complete_count = sum(
                        _dataset_complete_15m(store, symbol, day) for day in sessions
                    )
                    previous["daily_requests"][key] = {
                        "symbol": symbol,
                        "required_sessions": len(sessions),
                        "complete_sessions": complete_count,
                        "coverage_gap_sessions": len(sessions) - complete_count,
                        "returned_rows": len(rows),
                        # COMPLETE attests that Alpaca paginated the frozen
                        # request to exhaustion. It does not invent bars for a
                        # pre-listing or renamed-symbol interval.
                        "status": "COMPLETE",
                    }
                except HistoricalProviderError as exc:
                    previous["daily_requests"][key] = {
                        "symbol": symbol,
                        "required_sessions": len(sessions),
                        "complete_sessions": sum(
                            _dataset_complete_15m(store, symbol, day)
                            for day in sessions
                        ),
                        "status": "ERROR",
                        "error_category": exc.category,
                    }
                    previous["errors"].append(
                        {
                            "kind": "daily_history",
                            "instrument_id": instrument_id,
                            "category": exc.category,
                        }
                    )
            previous["updated_at"] = _timestamp_now()
            _write_gzip(private["acquisition"], previous)
            print(
                f"daily {identity_index}/{len(required_by_identity)}: "
                f"{previous['daily_requests'][key]['status']}",
                flush=True,
            )
            if previous["daily_requests"][key].get("error_category") in {
                "local_configuration",
                "permanent_permission",
            }:
                raise PreentryDatasetError(
                    "Alpaca daily collection has a global configuration or permission failure"
                )

        local = LocalHistoricalClient(store, "alpaca", feed="sip", adjustment="raw")
        for index, row in enumerate(records, 1):
            trigger = row["trigger"]
            day = str(trigger["date"])
            symbol = str(trigger["symbol"])
            key = f"{day}|{trigger['instrument_id']}"
            start = datetime.combine(
                date.fromisoformat(day), time(4, 0), tzinfo=EASTERN
            )
            end = datetime.combine(date.fromisoformat(day), time(9, 30), tzinfo=EASTERN)
            cached_state = previous["premarket_requests"].get(key, {})
            if cached_state.get("status") in {"COMPLETE", "CACHED"}:
                disposition = "CACHED"
                row_count = int(cached_state.get("row_count", 0))
            else:
                try:
                    cached_rows = local.fetch_bars(
                        symbol, start, end, bar_size="1 min", use_rth=False
                    )
                    disposition = "CACHED"
                    row_count = len(cached_rows)
                except HistoricalProviderError:
                    try:
                        live_rows = _retry(
                            lambda: recording.fetch_bars(
                                symbol,
                                start,
                                end,
                                bar_size="1 min",
                                use_rth=False,
                            )
                        )
                        disposition = "COMPLETE"
                        row_count = len(live_rows)
                    except HistoricalProviderError as exc:
                        disposition = "ERROR"
                        row_count = 0
                        cached_state = {"error_category": exc.category}
            previous["premarket_requests"][key] = {
                "symbol": symbol,
                "row_count": row_count,
                "status": disposition,
                **(
                    {"error_category": cached_state["error_category"]}
                    if disposition == "ERROR" and "error_category" in cached_state
                    else {}
                ),
            }
            previous["updated_at"] = _timestamp_now()
            _write_gzip(private["acquisition"], previous)
            print(f"premarket {index}/{len(records)}: {disposition}", flush=True)
            if cached_state.get("error_category") in {
                "local_configuration",
                "permanent_permission",
            }:
                raise PreentryDatasetError(
                    "Alpaca premarket collection has a global configuration or permission failure"
                )

    coverage = _collection_coverage(
        previous,
        expected_daily_identities=len(required_by_identity),
        expected_premarket_windows=len(records),
    )
    previous["status"] = coverage["status"]
    previous["split_source"] = split_status
    previous["updated_at"] = _timestamp_now()
    _write_gzip(private["acquisition"], previous)
    public = {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "status": previous["status"],
        "coverage": {
            "trigger_pairs": len(records),
            "daily_identities": len(required_by_identity),
            **{key: value for key, value in coverage.items() if key != "status"},
            "split_events": split_status["events"],
        },
        "private_acquisition_sha256": _sha256_file(private["acquisition"]),
        "private_splits_sha256": _sha256_file(private["splits"]),
        "raw_rows_and_symbols_public": False,
        "target_outcomes_observed_or_derived": False,
    }
    _write_json(public_status_path, public)
    return public


def _expanded_bars(
    store: HistoricalDayStore,
    symbol: str,
    day: str,
    *,
    timeframe: str,
    session: str | None = None,
) -> list[dict[str, Any]]:
    document = store.load(symbol, day)
    if document is None:
        return []
    candidates = []
    for dataset in document["datasets"]:
        if (
            dataset.get("kind") == "bars"
            and dataset.get("channel") == "trades"
            and dataset.get("timeframe") == timeframe
            and dataset.get("provider") == "alpaca"
            and dataset.get("feed") == "sip"
            and dataset.get("adjustment") == "raw"
            and (session is None or dataset.get("session") == session)
        ):
            candidates.append(dataset)
    if not candidates:
        return []
    candidates.sort(
        key=lambda row: (
            0 if row.get("quality", {}).get("complete") is True else 1,
            -int(row.get("quality", {}).get("row_count", 0)),
            str(row.get("id")),
        )
    )
    return [expand_bar(row) for row in candidates[0]["rows"]]


def build(
    *, manifest_path: Path, env_path: Path, public_status_path: Path
) -> dict[str, Any]:
    store = HistoricalDayStore.from_env(env_path)
    manifest, records, calendar = _verify(manifest_path, store)
    private = _private_paths(store.root)
    acquisition = _read_gzip(private["acquisition"])
    if acquisition.get("status") != "COLLECTION_COMPLETE":
        raise PreentryDatasetError("market-data collection is incomplete")
    split_actions = load_split_actions(private["splits"])
    rules = manifest["structure_contract"]
    output: list[dict[str, Any]] = []
    counts: Counter[str] = Counter()
    stop_fractions: list[float] = []
    resistance_rooms: list[float] = []
    for row in records:
        trigger = row["trigger"]
        pair = row["pair"]
        day = str(trigger["date"])
        symbol = str(trigger["symbol"])
        key = f"{day}|{trigger['instrument_id']}"
        snapshots = trigger.get("quote_snapshots")
        if not isinstance(snapshots, list) or len(snapshots) != 3:
            counts["records"] += 1
            counts["unresolved"] += 1
            output.append(
                {
                    "date": day,
                    "instrument_id": trigger["instrument_id"],
                    "symbol": symbol,
                    "rank": trigger["rank"],
                    "status": "UNRESOLVED",
                    "structure": None,
                    "error": "exactly three point-in-time quote snapshots are required",
                    "daily_sessions_present": 0,
                    "premarket_rows": int(
                        acquisition["premarket_requests"][key].get("row_count", 0)
                    ),
                    "target_outcome_observed_or_derived": False,
                }
            )
            continue
        final_snapshot = snapshots[-1]
        observation = datetime.fromisoformat(str(final_snapshot["target_at_et"]))
        entry = float(final_snapshot["ask"])
        spreads = [float(item["ask"]) - float(item["bid"]) for item in snapshots]
        median_spread = statistics.median(spreads)
        regular = _expanded_bars(store, symbol, day, timeframe="1m", session="regular")
        regular = [
            item
            for item in regular
            if datetime.fromisoformat(str(item["time_et"])) + timedelta(minutes=1)
            <= observation
        ]
        premarket_state = acquisition["premarket_requests"][key]
        premarket_start = datetime.combine(
            date.fromisoformat(day), time(4, 0), tzinfo=EASTERN
        )
        premarket_end = datetime.combine(
            date.fromisoformat(day), time(9, 30), tzinfo=EASTERN
        )
        if int(premarket_state.get("row_count", 0)) == 0:
            premarket = []
        else:
            local = LocalHistoricalClient(store, "alpaca", feed="sip", adjustment="raw")
            try:
                premarket = local.fetch_bars(
                    symbol,
                    premarket_start,
                    premarket_end,
                    bar_size="1 min",
                    use_rth=False,
                )
            except HistoricalProviderError as exc:
                raise PreentryDatasetError(
                    f"attested premarket cache is unavailable for {key}"
                ) from exc
        sessions = _required_sessions(day, calendar)
        daily_rows = []
        missing_daily = []
        for session_day in sessions:
            bars = _expanded_bars(
                store, symbol, session_day, timeframe="15m", session="regular"
            )
            if not bars:
                missing_daily.append(session_day)
                continue
            factor = split_adjustment_factor(symbol, session_day, day, split_actions)
            daily_rows.append(
                {
                    "date_et": session_day,
                    "high": max(float(item["high"]) for item in bars) * factor,
                    "split_adjustment_factor": factor,
                }
            )
        try:
            structure = derive_preentry_structure(
                observation_at=observation,
                entry_limit=entry,
                daily_atr_14=float(pair["scanner_fields"]["daily_atr_14"]),
                median_spread_dollars=median_spread,
                completed_regular_bars=regular,
                premarket_bars=premarket,
                premarket_window_complete=premarket_state["status"]
                in {"COMPLETE", "CACHED"},
                target_adjusted_daily_bars=daily_rows,
                daily_history_complete=not missing_daily,
                daily_split_basis_verified=True,
                atr_stop_fraction=float(rules["atr_stop_fraction"]),
                maximum_stop_fraction=float(rules["maximum_stop_fraction"]),
                minimum_room_fraction=float(rules["minimum_resistance_room_fraction"]),
            )
            value = structure.to_dict()
            counts["derived"] += 1
            counts[f"resistance_{structure.resistance.status.lower()}"] += 1
            counts["stop_within_maximum"] += int(
                structure.stop.maximum_stop_fraction_pass
            )
            counts["resistance_room_pass"] += int(
                structure.resistance.minimum_room_pass is True
            )
            counts["structure_both_pass"] += int(
                structure.stop.maximum_stop_fraction_pass
                and structure.resistance.minimum_room_pass is True
            )
            stop_fractions.append(structure.stop.stop_fraction)
            if structure.resistance.resistance_room_fraction is not None:
                resistance_rooms.append(structure.resistance.resistance_room_fraction)
            status = "DERIVED"
            error = None
        except PreentryStructureError as exc:
            value = None
            status = "UNRESOLVED"
            error = str(exc)
            counts["unresolved"] += 1
        counts["records"] += 1
        output.append(
            {
                "date": day,
                "instrument_id": trigger["instrument_id"],
                "symbol": symbol,
                "rank": trigger["rank"],
                "status": status,
                "structure": value,
                "error": error,
                "daily_sessions_present": len(daily_rows),
                "premarket_rows": len(premarket),
                "target_outcome_observed_or_derived": False,
            }
        )
    result = {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "status": "STRUCTURE_BUILD_COMPLETE",
        "updated_at": _timestamp_now(),
        "counts": dict(sorted(counts.items())),
        "stop_fraction_distribution": (
            {
                "minimum": min(stop_fractions),
                "median": statistics.median(stop_fractions),
                "maximum": max(stop_fractions),
            }
            if stop_fractions
            else None
        ),
        "resolved_resistance_room_distribution": (
            {
                "minimum": min(resistance_rooms),
                "median": statistics.median(resistance_rooms),
                "maximum": max(resistance_rooms),
            }
            if resistance_rooms
            else None
        ),
        "records": output,
        "target_outcomes_observed_or_derived": False,
        "errors": [],
    }
    _write_gzip(private["result"], result)
    public = {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "status": result["status"],
        "counts": result["counts"],
        "stop_fraction_distribution": result["stop_fraction_distribution"],
        "resolved_resistance_room_distribution": result[
            "resolved_resistance_room_distribution"
        ],
        "private_result_sha256": _sha256_file(private["result"]),
        "raw_rows_and_symbols_public": False,
        "target_outcomes_observed_or_derived": False,
    }
    _write_json(public_status_path, public)
    return public


def inspect(
    *, manifest_path: Path, env_path: Path, public_result_path: Path
) -> dict[str, Any]:
    store = HistoricalDayStore.from_env(env_path)
    manifest, records, _calendar_rows = _verify(manifest_path, store)
    private = _private_paths(store.root)
    result = _read_gzip(private["result"])
    counts = result.get("counts", {})
    complete = (
        result.get("manifest_sha256") == manifest["manifest_sha256"]
        and result.get("status") == "STRUCTURE_BUILD_COMPLETE"
        and counts.get("records") == len(records) == 325
        and counts.get("derived", 0) + counts.get("unresolved", 0) == 325
        and all(
            row.get("target_outcome_observed_or_derived") is False
            for row in result.get("records", [])
        )
        and result.get("target_outcomes_observed_or_derived") is False
        and result.get("errors") == []
    )
    if not complete:
        raise PreentryDatasetError("pre-entry structure build is incomplete")
    public = {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "status": "READY",
        "inspected": True,
        "claim_scope": "DEVELOPMENT_ONLY",
        "counts": counts,
        "stop_fraction_distribution": result["stop_fraction_distribution"],
        "resolved_resistance_room_distribution": result[
            "resolved_resistance_room_distribution"
        ],
        "private_result_sha256": _sha256_file(private["result"]),
        "findings": {
            "contract_is_outcome_blind": True,
            "missing_long_history_cannot_default_to_clear_sky": True,
            "production_rule_change_earned": False,
        },
        "claim_boundary": (
            "Input-definition and deployability evidence on already-inspected dates; "
            "not returns, alpha, confirmation, or production promotion."
        ),
        "raw_rows_and_symbols_public": False,
    }
    _write_json(public_result_path, public)
    return public


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "command", choices=("calendar", "freeze", "collect", "build", "inspect")
    )
    parser.add_argument("--manifest", type=Path)
    parser.add_argument("--env-file", type=Path, default=PROJECT_ROOT / ".env")
    parser.add_argument("--output-root", type=Path, default=DEFAULT_MANIFEST_ROOT)
    parser.add_argument("--public-status", type=Path, default=DEFAULT_PUBLIC_STATUS)
    parser.add_argument("--public-result", type=Path, default=DEFAULT_PUBLIC_RESULT)
    return parser


def main() -> int:
    args = _parser().parse_args()
    try:
        if args.command == "calendar":
            value = collect_calendar(
                env_path=args.env_file,
                calendar_path=CALENDAR_PATH,
                source_path=CALENDAR_SOURCE_PATH,
            )
        elif args.command == "freeze":
            path, manifest = freeze_inputs(
                env_path=args.env_file, output_root=args.output_root
            )
            value = {"manifest": _repo_path(path), **manifest}
        else:
            if args.manifest is None:
                raise PreentryDatasetError("--manifest is required")
            if args.command == "collect":
                value = collect(
                    manifest_path=args.manifest,
                    env_path=args.env_file,
                    public_status_path=args.public_status,
                )
            elif args.command == "build":
                value = build(
                    manifest_path=args.manifest,
                    env_path=args.env_file,
                    public_status_path=args.public_status,
                )
            else:
                value = inspect(
                    manifest_path=args.manifest,
                    env_path=args.env_file,
                    public_result_path=args.public_result,
                )
    except (
        PreentryDatasetError,
        LearningDataError,
        HistoricalProviderError,
        ScannerReplayError,
        OSError,
        ValueError,
        requests.RequestException,
    ) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}), file=sys.stderr)
        return 2
    print(json.dumps(value, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""Inspect expansion clean triggers while preserving explicit quote gaps.

This outcome-blind workflow consumes the exact 1,460 raw crossing windows from
the inspected expansion join.  A provider no-observation is recorded as missing
NBBO and never fabricated, substituted, or treated as a batch-fatal exception.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import io
import json
import os
import statistics
import sys
from collections import Counter
from collections.abc import Mapping, Sequence
from datetime import timedelta, timezone, datetime
from pathlib import Path
from typing import Any

from historical_providers import (
    AlpacaConfig,
    AlpacaHistoricalClient,
    HistoricalProviderError,
)
from historical_service import LocalHistoricalClient, RecordingHistoricalClient
from historical_store import HistoricalDayStore, HistoricalStoreError, build_context
from learning_data import (
    LearningDataError,
    freeze_dataset_contract,
    load_frozen_dataset_contract,
)
from selected_candidate_join import (
    EASTERN,
    MINIMUM_FREE_BYTES,
    _RateGate,
    _precise_timestamp,
    _rate_interval,
    _retry,
    _select_quote_snapshots,
)
from sip_trade_conditions import (
    CONTINUOUS_CROSS_VERSION,
    RULE_VERSION,
    SOURCE_URL as CONDITION_SOURCE_URL,
    classify_trade_conditions,
)


PROJECT_ROOT = Path(__file__).resolve().parent
DATASET_ID = "dataset-clean-trigger-fidelity-2026-07-19-expansion-v1"
SOURCE_JOIN_ID = "dataset-selected-candidate-join-2026-07-19-expansion-v1"
SOURCE_FIDELITY_ID = "dataset-selected-candidate-fidelity-2026-07-19-expansion-v1"
SOURCE_JOIN_MANIFEST = (
    PROJECT_ROOT
    / "historical_batches"
    / "selected_candidate_join_expansion"
    / "manifests"
    / (
        "dataset-selected-candidate-join-2026-07-19-expansion-v1-"
        "15d8baefcd4bedb3cf473cad4b98638a0c101ae2a1bfc64552d80c72dede2f6b.json"
    )
)
SOURCE_JOIN_RESULT = (
    PROJECT_ROOT
    / "research_results"
    / "2026-07-19-selected-candidate-join-expansion.json"
)
SOURCE_FIDELITY_MANIFEST = (
    PROJECT_ROOT
    / "historical_batches"
    / "selected_candidate_fidelity_expansion"
    / "manifests"
    / (
        "dataset-selected-candidate-fidelity-2026-07-19-expansion-v1-"
        "ee9ba6f3a2d48f3337ee654c545ea3c02287e23f4a4db5a3fb86adf1b0c780ce.json"
    )
)
SOURCE_FIDELITY_STATUS = (
    PROJECT_ROOT
    / "historical_batches"
    / "selected_candidate_fidelity_expansion"
    / "collection-status.json"
)
DEFAULT_OUTPUT_ROOT = (
    PROJECT_ROOT
    / "historical_batches"
    / "clean_trigger_fidelity_expansion"
    / "manifests"
)
DEFAULT_PUBLIC_STATUS = (
    PROJECT_ROOT
    / "historical_batches"
    / "clean_trigger_fidelity_expansion"
    / "collection-status.json"
)
DEFAULT_PUBLIC_RESULT = (
    PROJECT_ROOT
    / "research_results"
    / "2026-07-19-clean-trigger-fidelity-expansion.json"
)


class CleanTriggerFidelityError(RuntimeError):
    """The clean-trigger contract or collection is invalid."""

    def __init__(self, message: str, *, category: str = "fidelity"):
        super().__init__(message)
        self.category = category


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


def _read_object(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise CleanTriggerFidelityError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise CleanTriggerFidelityError(f"{path} must contain an object")
    return value


def _read_gzip(path: Path) -> dict[str, Any]:
    try:
        with gzip.open(path, "rt", encoding="utf-8") as source:
            value = json.load(source)
    except (OSError, json.JSONDecodeError) as exc:
        raise CleanTriggerFidelityError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise CleanTriggerFidelityError(f"{path} must contain an object")
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


def _write_gzip(path: Path, value: Any) -> None:
    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode="wb", compresslevel=6, mtime=0) as target:
        target.write(json.dumps(value, indent=2, sort_keys=True).encode())
        target.write(b"\n")
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_bytes(buffer.getvalue())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _repo_path(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(PROJECT_ROOT))
    except ValueError as exc:
        raise CleanTriggerFidelityError(
            f"evidence path must be repository-relative: {path}"
        ) from exc


def _private_path(store_root: Path, dataset_id: str) -> Path:
    return (
        store_root
        / "_derived"
        / "clean_trigger_fidelity"
        / dataset_id
        / "clean-trigger-index.json.gz"
    )


def _source_trigger_path(store_root: Path) -> Path:
    return (
        store_root
        / "_derived"
        / "selected_candidate_join"
        / SOURCE_JOIN_ID
        / "trigger-index.json.gz"
    )


def _source_sec_path(store_root: Path) -> Path:
    return (
        store_root
        / "_derived"
        / "selected_candidate_fidelity"
        / SOURCE_FIDELITY_ID
        / "primary-catalyst-index.json.gz"
    )


def _source_contracts(env_path: Path) -> tuple[Path, dict[str, Any], list[dict[str, Any]]]:
    join = load_frozen_dataset_contract(SOURCE_JOIN_MANIFEST)
    fidelity = load_frozen_dataset_contract(SOURCE_FIDELITY_MANIFEST)
    join_result = _read_object(SOURCE_JOIN_RESULT)
    fidelity_status = _read_object(SOURCE_FIDELITY_STATUS)
    if (
        join.get("dataset_id") != SOURCE_JOIN_ID
        or join_result.get("manifest_sha256") != join.get("manifest_sha256")
        or join_result.get("status") != "READY"
        or join_result.get("inspected") is not True
    ):
        raise CleanTriggerFidelityError("source join is not inspected READY")
    if (
        fidelity.get("dataset_id") != SOURCE_FIDELITY_ID
        or fidelity_status.get("manifest_sha256") != fidelity.get("manifest_sha256")
        or fidelity_status.get("status") != "SEC_COLLECTION_COMPLETE"
    ):
        raise CleanTriggerFidelityError("source SEC evidence is not complete")
    store_root = HistoricalDayStore.from_env(env_path).root
    source_trigger_path = _source_trigger_path(store_root)
    source_trigger = _read_gzip(source_trigger_path)
    if source_trigger.get("manifest_sha256") != join["manifest_sha256"]:
        raise CleanTriggerFidelityError("source trigger index is not manifest-bound")
    triggers = [
        dict(row)
        for row in source_trigger.get("records", [])
        if isinstance(row, Mapping)
        and row.get("status") == "CROSSING_MINUTE_IDENTIFIED"
    ]
    if len(triggers) != int(join_result.get("expected", {}).get("trigger_tapes", -1)):
        raise CleanTriggerFidelityError("source trigger count differs")
    sec_path = _source_sec_path(store_root)
    if not sec_path.is_file() or _sha256_file(sec_path) != fidelity_status.get(
        "private_sec_index_sha256"
    ):
        raise CleanTriggerFidelityError("source SEC index differs")
    dependency = {
        "source_join_manifest_sha256": join["manifest_sha256"],
        "source_join_result_sha256": _sha256_file(SOURCE_JOIN_RESULT),
        "source_trigger_index_sha256": _sha256_file(source_trigger_path),
        "source_fidelity_manifest_sha256": fidelity["manifest_sha256"],
        "source_fidelity_status_sha256": _sha256_file(SOURCE_FIDELITY_STATUS),
        "source_sec_index_sha256": _sha256_file(sec_path),
        "source_trigger_count": len(triggers),
    }
    return store_root, dependency, triggers


def _matching_manifest(
    output_root: Path, dataset_id: str, expected: Mapping[str, Any]
) -> tuple[Path, dict[str, Any]] | None:
    matches = sorted(output_root.glob(f"{dataset_id}-*.json"))
    if len(matches) > 1:
        raise CleanTriggerFidelityError("clean-trigger dataset has multiple manifests")
    if not matches:
        return None
    manifest = load_frozen_dataset_contract(matches[0])
    for key, value in expected.items():
        if key == "capacity_contract":
            observed = manifest.get(key)
            if not isinstance(observed, Mapping) or observed.get(
                "minimum_free_bytes"
            ) != value.get("minimum_free_bytes"):
                raise CleanTriggerFidelityError("existing capacity contract differs")
            continue
        if manifest.get(key) != value:
            raise CleanTriggerFidelityError(f"existing clean-trigger {key} differs")
    return matches[0], manifest


def freeze_contract(
    *, env_path: Path, output_root: Path, dataset_id: str = DATASET_ID
) -> tuple[Path, dict[str, Any]]:
    store_root, dependency, triggers = _source_contracts(env_path)
    free_bytes = os.statvfs(store_root).f_bavail * os.statvfs(store_root).f_frsize
    if free_bytes < MINIMUM_FREE_BYTES:
        raise CleanTriggerFidelityError("historical reserve is below 10 GiB")
    selection = {
        **dependency,
        "ordered_trigger_identity_sha256": _sha256_json(
            [
                {
                    "date": row["date"],
                    "symbol": row["symbol"],
                    "instrument_id": row["instrument_id"],
                    "rank": row["rank"],
                    "crossing_minute_et": row["first_crossing_minute_et"],
                }
                for row in triggers
            ]
        ),
        "exact_identities_public": False,
    }
    collection = {
        "collector_sha256": _sha256_file(Path(__file__)),
        "provider": "Alpaca Market Data API",
        "feed": "sip",
        "adjustment": "raw",
        "condition_source_url": CONDITION_SOURCE_URL,
        "condition_rule_version": RULE_VERSION,
        "continuous_cross_version": CONTINUOUS_CROSS_VERSION,
        "checkpoint_interval": 25,
        "quote_targets_seconds_after_clean_cross": [0, 5, 10],
        "maximum_quote_age_seconds": 5,
        "no_observation_policy": (
            "after one bounded provider refresh, record MISSING_TRIGGER_NBBO and continue"
        ),
        "provider_switching_allowed": False,
        "substitutions_allowed": False,
        "raw_and_symbol_rows_public": False,
        "target_outcomes_observed_or_derived": False,
        "alpha_or_confirmation_claim_allowed": False,
    }
    expected = {
        "schema_version": 1,
        "dataset_id": dataset_id,
        "requested_dates": load_frozen_dataset_contract(SOURCE_JOIN_MANIFEST)[
            "requested_dates"
        ],
        "dataset_payload": {
            "lane": "development",
            "claim_scope": "DEVELOPMENT_ONLY",
            "status": "COLLECTING",
            "evidence_paths": [
                _repo_path(SOURCE_JOIN_MANIFEST),
                _repo_path(SOURCE_JOIN_RESULT),
                _repo_path(SOURCE_FIDELITY_MANIFEST),
                _repo_path(SOURCE_FIDELITY_STATUS),
                "SCANNER_EXPANSION_FIDELITY.md",
            ],
            "inspected": False,
        },
        "selection_contract": selection,
        "collection_contract": collection,
        "capacity_contract": {
            "minimum_free_bytes": MINIMUM_FREE_BYTES,
            "observed_free_bytes_at_freeze": free_bytes,
        },
    }
    existing = _matching_manifest(output_root, dataset_id, expected)
    if existing is not None:
        return existing
    return freeze_dataset_contract(
        {**expected, "registered_at": datetime.now(timezone.utc).isoformat()},
        output_root,
    )


def _load_manifest(path: Path) -> dict[str, Any]:
    manifest = load_frozen_dataset_contract(path)
    if manifest.get("dataset_id") != DATASET_ID:
        raise CleanTriggerFidelityError("unexpected clean-trigger dataset")
    if manifest.get("collection_contract", {}).get(
        "collector_sha256"
    ) != _sha256_file(Path(__file__)):
        raise CleanTriggerFidelityError("clean-trigger collector changed")
    return manifest


def _local_quotes(
    local: LocalHistoricalClient, symbol: str, start: datetime, end: datetime
) -> list[dict[str, Any]]:
    try:
        return local.fetch_bid_ask_ticks(symbol, start, end, use_rth=True)
    except HistoricalProviderError as exc:
        if exc.category == "local_cache_miss":
            return []
        raise


def _summarize(records: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    counts: Counter[str] = Counter()
    shifts: list[float] = []
    for record in records:
        counts["crossing_windows"] += 1
        status = str(record.get("status") or "UNKNOWN")
        counts[f"status_{status}"] += 1
        first = record.get("first_raw_cross")
        if isinstance(first, Mapping):
            decision = first.get("decision", {})
            counts["first_raw_cross_updates_bar_high"] += int(
                bool(decision.get("updates_minute_high_low"))
            )
            counts["first_raw_cross_is_clean"] += int(
                bool(decision.get("establishes_continuous_cross"))
            )
        clean = record.get("clean_cross")
        if isinstance(clean, Mapping):
            counts["clean_crosses"] += 1
            shift = float(clean["shift_from_first_raw_cross_seconds"])
            shifts.append(shift)
        snapshots = record.get("quote_snapshots", [])
        counts["three_snapshot_windows"] += int(len(snapshots) == 3)
        counts["basic_fresh_uncrossed_windows"] += int(
            record.get("basic_fresh_uncrossed") is True
        )
        counts["final_ask_within_chase_cap"] += int(
            record.get("final_ask_within_chase_cap") is True
        )
        counts["missing_trigger_nbbo"] += int(status == "MISSING_TRIGGER_NBBO")
        counts["collection_errors"] += int(status == "COLLECTION_ERROR")
    return {
        "counts": dict(sorted(counts.items())),
        "shift_summary": {
            "count": len(shifts),
            "median_seconds": statistics.median(shifts) if shifts else None,
            "maximum_seconds": max(shifts) if shifts else None,
            "positive_shift_count": sum(value > 0 for value in shifts),
        },
    }


def collect(
    *, manifest_path: Path, env_path: Path, public_status_path: Path
) -> dict[str, Any]:
    manifest = _load_manifest(manifest_path)
    store_root, dependency, triggers = _source_contracts(env_path)
    for key, value in dependency.items():
        if manifest["selection_contract"].get(key) != value:
            raise CleanTriggerFidelityError(f"source dependency {key} differs")
    private_path = _private_path(store_root, DATASET_ID)
    prior: dict[str, Any] = {}
    if private_path.exists():
        prior = _read_gzip(private_path)
        if prior.get("manifest_sha256") != manifest["manifest_sha256"]:
            raise CleanTriggerFidelityError("checkpoint is not manifest-bound")
    by_key = {
        (str(row["date"]), str(row["symbol"])): dict(row)
        for row in prior.get("records", [])
        if isinstance(row, Mapping) and row.get("status") != "COLLECTION_ERROR"
    }
    local = LocalHistoricalClient(
        HistoricalDayStore(store_root), "alpaca", feed="sip", adjustment="raw"
    )
    config = AlpacaConfig.optional_from_env(env_path)
    if config is None:
        raise CleanTriggerFidelityError("Alpaca credentials are not configured")
    gate = _RateGate(_rate_interval(env_path))
    store = HistoricalDayStore(store_root)
    processed = 0
    errors: list[dict[str, Any]] = []
    with AlpacaHistoricalClient(config) as alpaca:
        recorder = RecordingHistoricalClient(alpaca, store)
        for trigger in triggers:
            key = (str(trigger["date"]), str(trigger["symbol"]))
            if key in by_key:
                continue
            symbol, day = key[1], key[0]
            minute = datetime.fromisoformat(
                str(trigger["first_crossing_minute_et"])
            ).astimezone(EASTERN)
            try:
                trades = local.fetch_trades(
                    symbol, minute, minute + timedelta(minutes=1), use_rth=True
                )
                opening_high = float(trigger["opening_high"])
                price_crosses = [
                    row for row in trades if float(row["price"]) > opening_high
                ]
                if not price_crosses:
                    raise CleanTriggerFidelityError("raw tape has no price cross")
                bar_cross = next(
                    (
                        row
                        for row in price_crosses
                        if classify_trade_conditions(
                            row.get("tape"), row.get("conditions")
                        ).updates_minute_high_low
                    ),
                    None,
                )
                if bar_cross is None:
                    raise CleanTriggerFidelityError(
                        "condition rules cannot reproduce aggregate crossing high"
                    )
                clean_cross = next(
                    (
                        row
                        for row in price_crosses
                        if classify_trade_conditions(
                            row.get("tape"), row.get("conditions")
                        ).establishes_continuous_cross
                    ),
                    None,
                )
                first = price_crosses[0]
                first_decision = classify_trade_conditions(
                    first.get("tape"), first.get("conditions")
                )
                record: dict[str, Any] = {
                    "date": day,
                    "symbol": symbol,
                    "instrument_id": trigger["instrument_id"],
                    "rank": trigger["rank"],
                    "opening_high": opening_high,
                    "crossing_minute_et": minute.isoformat(),
                    "price_cross_count": len(price_crosses),
                    "first_raw_cross": {
                        "observed_at_et": _precise_timestamp(first).isoformat(),
                        "price": float(first["price"]),
                        "conditions": first.get("conditions"),
                        "tape": first.get("tape"),
                        "decision": first_decision.__dict__,
                    },
                    "first_bar_eligible_cross_at_et": _precise_timestamp(
                        bar_cross
                    ).isoformat(),
                    "condition_rule_version": RULE_VERSION,
                    "continuous_cross_version": CONTINUOUS_CROSS_VERSION,
                    "quote_refresh_attempted": False,
                }
                if clean_cross is None:
                    record.update(
                        {
                            "status": "NO_CLEAN_CONTINUOUS_CROSS_IN_MINUTE",
                            "clean_cross": None,
                            "quote_snapshots": [],
                            "basic_fresh_uncrossed": False,
                            "final_ask_within_chase_cap": False,
                        }
                    )
                    observed_at = (minute + timedelta(minutes=1)).isoformat()
                else:
                    clean_at = _precise_timestamp(clean_cross)
                    shift = (clean_at - _precise_timestamp(first)).total_seconds()
                    quote_start = clean_at - timedelta(seconds=1)
                    quote_end = clean_at + timedelta(seconds=11)
                    quotes = _local_quotes(local, symbol, quote_start, quote_end)
                    snapshots = _select_quote_snapshots(quotes, clean_at)
                    if len(snapshots) != 3 or any(
                        float(row["age_seconds"]) > 5 for row in snapshots
                    ):
                        record["quote_refresh_attempted"] = True
                        _retry(
                            gate,
                            lambda: recorder.fetch_bid_ask_ticks(
                                symbol, quote_start, quote_end, use_rth=True
                            ),
                        )
                        quotes = _local_quotes(local, symbol, quote_start, quote_end)
                        snapshots = _select_quote_snapshots(quotes, clean_at)
                    basic = len(snapshots) == 3 and all(
                        float(row["age_seconds"]) <= 5
                        and float(row["bid"]) > 0
                        and float(row["ask"]) > float(row["bid"])
                        for row in snapshots
                    )
                    chase = basic and float(snapshots[-1]["ask"]) <= opening_high * 1.0015
                    status = (
                        "CLEAN_CONTINUOUS_CROSS_IDENTIFIED"
                        if snapshots
                        else "MISSING_TRIGGER_NBBO"
                    )
                    record.update(
                        {
                            "status": status,
                            "clean_cross": {
                                "observed_at_et": clean_at.isoformat(),
                                "price": float(clean_cross["price"]),
                                "size": int(clean_cross["size"]),
                                "exchange": clean_cross.get("exchange"),
                                "conditions": clean_cross.get("conditions"),
                                "tape": clean_cross.get("tape"),
                                "shift_from_first_raw_cross_seconds": shift,
                            },
                            "quote_snapshots": snapshots,
                            "basic_fresh_uncrossed": basic,
                            "final_ask_within_chase_cap": chase,
                        }
                    )
                    observed_at = clean_at.isoformat()
                by_key[key] = record
                store.merge(
                    symbol,
                    day,
                    contexts=[
                        build_context(
                            kind="expansion_clean_trigger_fidelity",
                            provider="alpaca",
                            observed_at=observed_at,
                            payload={"dataset_id": DATASET_ID, **record},
                            provenance={
                                "source_type": "Alpaca historical SIP trades and quotes",
                                "captured_at": datetime.now(timezone.utc).isoformat(),
                                "condition_source_url": CONDITION_SOURCE_URL,
                            },
                        )
                    ],
                )
            except (HistoricalProviderError, CleanTriggerFidelityError, OSError) as exc:
                by_key[key] = {
                    "date": day,
                    "symbol": symbol,
                    "instrument_id": trigger["instrument_id"],
                    "rank": trigger["rank"],
                    "status": "COLLECTION_ERROR",
                    "error_type": type(exc).__name__,
                    "error_category": getattr(exc, "category", "local_io"),
                    "error": str(exc),
                }
                errors.append(by_key[key])
            processed += 1
            if processed % 25 == 0:
                ordered = [
                    by_key[(str(row["date"]), str(row["symbol"]))]
                    for row in triggers
                    if (str(row["date"]), str(row["symbol"])) in by_key
                ]
                summary = _summarize(ordered)
                _write_gzip(
                    private_path,
                    {
                        "schema_version": 1,
                        "dataset_id": DATASET_ID,
                        "manifest_sha256": manifest["manifest_sha256"],
                        "status": "COLLECTING",
                        "updated_at": datetime.now(timezone.utc).isoformat(),
                        "records": ordered,
                        **summary,
                    },
                )
                print(
                    f"clean-trigger fidelity {len(ordered)}/{len(triggers)}; "
                    f"errors={summary['counts'].get('collection_errors', 0)}",
                    flush=True,
                )
    ordered = [
        by_key[(str(row["date"]), str(row["symbol"]))]
        for row in triggers
        if (str(row["date"]), str(row["symbol"])) in by_key
    ]
    summary = _summarize(ordered)
    complete = (
        len(ordered) == len(triggers)
        and summary["counts"].get("collection_errors", 0) == 0
    )
    private = {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "status": "COLLECTION_COMPLETE" if complete else "INCOMPLETE",
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "records": ordered,
        **summary,
    }
    _write_gzip(private_path, private)
    public = {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "status": private["status"],
        "expected_crossing_windows": len(triggers),
        "counts": summary["counts"],
        "clean_trigger_shift_summary": summary["shift_summary"],
        "private_trigger_index_sha256": _sha256_file(private_path),
        "missing_nbbo_is_explicit_blocker": True,
        "symbols_and_raw_rows_public": False,
        "target_outcomes_observed_or_derived": False,
    }
    _write_json(public_status_path, public)
    return public


def inspect(
    *, manifest_path: Path, env_path: Path, public_result_path: Path
) -> dict[str, Any]:
    manifest = _load_manifest(manifest_path)
    store_root, dependency, triggers = _source_contracts(env_path)
    for key, value in dependency.items():
        if manifest["selection_contract"].get(key) != value:
            raise CleanTriggerFidelityError(f"source dependency {key} differs")
    private_path = _private_path(store_root, DATASET_ID)
    private = _read_gzip(private_path)
    if (
        private.get("manifest_sha256") != manifest["manifest_sha256"]
        or private.get("status") != "COLLECTION_COMPLETE"
    ):
        raise CleanTriggerFidelityError("clean-trigger collection is incomplete")
    records = private.get("records", [])
    expected_keys = [(str(row["date"]), str(row["symbol"])) for row in triggers]
    observed_keys = [
        (str(row.get("date")), str(row.get("symbol")))
        for row in records
        if isinstance(row, Mapping)
    ]
    if observed_keys != expected_keys or len(observed_keys) != len(set(observed_keys)):
        raise CleanTriggerFidelityError("clean-trigger identity/order differs")
    if any(
        row.get("condition_rule_version") != RULE_VERSION
        or row.get("continuous_cross_version") != CONTINUOUS_CROSS_VERSION
        for row in records
    ):
        raise CleanTriggerFidelityError("condition rule version differs")
    counts = dict(private["counts"])
    result = {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "status": "READY",
        "inspected": True,
        "claim_scope": "DEVELOPMENT_ONLY",
        "source_trigger_count": len(triggers),
        "counts": counts,
        "clean_trigger_shift_summary": private["shift_summary"],
        "private_trigger_index_sha256": _sha256_file(private_path),
        "findings": {
            "every_source_crossing_classified": True,
            "missing_nbbo_preserved_as_blocker": True,
            "provider_substitution_used": False,
            "production_rule_change_earned": False,
        },
        "target_outcomes_observed_or_derived": False,
        "symbols_and_raw_rows_public": False,
        "claim_boundary": (
            "Clean-trigger and quote-input fidelity only; not trade eligibility, "
            "alpha, confirmation, maturity, or a strategy change."
        ),
    }
    _write_json(public_result_path, result)
    return result


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env", type=Path, default=PROJECT_ROOT / ".env")
    commands = parser.add_subparsers(dest="command", required=True)
    freeze = commands.add_parser("freeze")
    freeze.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    for name in ("collect", "inspect"):
        command = commands.add_parser(name)
        command.add_argument("manifest", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "freeze":
            path, manifest = freeze_contract(
                env_path=args.env, output_root=args.output_root
            )
            result: Any = {
                "path": str(path),
                "dataset_id": manifest["dataset_id"],
                "manifest_sha256": manifest["manifest_sha256"],
                "source_trigger_count": manifest["selection_contract"][
                    "source_trigger_count"
                ],
            }
        elif args.command == "collect":
            result = collect(
                manifest_path=args.manifest,
                env_path=args.env,
                public_status_path=DEFAULT_PUBLIC_STATUS,
            )
        else:
            result = inspect(
                manifest_path=args.manifest,
                env_path=args.env,
                public_result_path=DEFAULT_PUBLIC_RESULT,
            )
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    except (
        CleanTriggerFidelityError,
        HistoricalProviderError,
        HistoricalStoreError,
        LearningDataError,
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

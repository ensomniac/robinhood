"""Freeze, collect, and derive causal challenger pre-entry trigger windows.

Only the 95 independently verified-positive source pairs may enter this
dataset.  Collection stops at 10:30 ET and never reads a post-entry row,
return, or target outcome.  Exact identities and rows stay in the configured
historical store; public artifacts contain aggregate counts and hashes only.
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
import time
from collections import Counter
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime, time as wall_time, timedelta
from pathlib import Path
from typing import Any, Callable
from zoneinfo import ZoneInfo

import challenger_orb_retest as trigger
import challenger_orb_retest_selected_pairs as selected_pairs
import development_catalyst_source_semantics as source_semantics
import development_sec_accession_chain_recovery as accession_recovery
from historical_providers import (
    AlpacaConfig,
    AlpacaHistoricalClient,
    HistoricalProviderError,
)
from historical_service import LocalHistoricalClient, RecordingHistoricalClient
from historical_store import HistoricalDayStore, HistoricalStoreConfig
from learning_data import (
    LearningDataError,
    freeze_dataset_contract,
    load_frozen_dataset_contract,
)
from sip_trade_conditions import CONTINUOUS_CROSS_VERSION, RULE_VERSION


PROJECT_ROOT = Path(__file__).resolve().parent
EASTERN = ZoneInfo("America/New_York")
DATASET_ID = "dataset-challenger-orb-retest-preentry-collection-2026-07-21-v1"
SELECTED_PAIR_MANIFEST = (
    PROJECT_ROOT
    / "historical_batches/challenger_orb_retest_v1/selected_pair_manifests"
    / (
        "dataset-selected-candidate-contract-2026-07-21-challenger-orb-retest-v1-"
        "24ef6d292e27fc13051d85d7e5f134ba576c6ab14174706dd359b17f26965de4.json"
    )
)
SOURCE_SEMANTICS_RESULT = (
    PROJECT_ROOT
    / "research_results/2026-07-21-challenger-sec-source-semantics-inspection.json"
)
ACCESSION_RESULT = (
    PROJECT_ROOT
    / "research_results/2026-07-21-challenger-sec-accession-chain-inspection.json"
)
HYPOTHESIS = (
    PROJECT_ROOT
    / "learning/hypotheses"
    / (
        "experiment-catalyst-orb-retest-v1-"
        "b766000bc84aff7ae836c8dcdc516d4b1c7dc4fb3dcb5f179bb5eb452656d105.json"
    )
)
DEFAULT_OUTPUT_ROOT = (
    PROJECT_ROOT / "historical_batches/challenger_orb_retest_v1/preentry_manifests"
)
DEFAULT_PUBLIC_STATUS = (
    PROJECT_ROOT / "historical_batches/challenger_orb_retest_v1/preentry-status.json"
)
DEFAULT_PUBLIC_RESULT = (
    PROJECT_ROOT / "research_results/2026-07-21-challenger-orb-retest-preentry.json"
)
INSPECTOR = PROJECT_ROOT / "challenger_orb_retest_preentry_inspection.py"
PRIVATE_NAMESPACE = "_derived/challenger_orb_retest_preentry"
EXPECTED_POSITIVE_PAIRS = 95
EXPECTED_POSITIVE_DATES = 47
MINIMUM_FREE_BYTES = 20 * 1024**3
START_ET = wall_time(9, 30)
SEARCH_START_ET = wall_time(9, 35)
CUTOFF_ET = wall_time(10, 30)
BENCHMARKS = ("SPY", "QQQ")


class ChallengerPreentryError(RuntimeError):
    """The causal pre-entry contract or evidence is incomplete."""


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


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ChallengerPreentryError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise ChallengerPreentryError(f"{path} must contain an object")
    return value


def _read_gzip(path: Path) -> dict[str, Any]:
    try:
        with gzip.open(path, "rt", encoding="utf-8") as source:
            value = json.load(source)
    except (OSError, json.JSONDecodeError) as exc:
        raise ChallengerPreentryError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise ChallengerPreentryError(f"{path} must contain an object")
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
        raise ChallengerPreentryError(f"public path is outside repository: {path}") from exc


def _private_root(store_root: Path) -> Path:
    return store_root / PRIVATE_NAMESPACE / DATASET_ID


def _selection_path(store_root: Path) -> Path:
    return _private_root(store_root) / "frozen-selection.json.gz"


def _wrapper_path(store_root: Path, request_sha256: str) -> Path:
    return _private_root(store_root) / "wrappers" / f"{request_sha256}.json.gz"


def _index_path(store_root: Path) -> Path:
    return _private_root(store_root) / "collection-index.json.gz"


def _trigger_path(store_root: Path) -> Path:
    return _private_root(store_root) / "trigger-index.json.gz"


def _published(path: Path) -> dict[str, str]:
    relative = _repo_path(path)
    dirty = subprocess.run(
        ["git", "status", "--porcelain", "--", relative],
        cwd=PROJECT_ROOT,
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    if dirty.strip():
        raise ChallengerPreentryError(f"provider input is not committed: {relative}")
    head = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=PROJECT_ROOT, check=True,
        capture_output=True, text=True,
    ).stdout.strip()
    upstream = subprocess.run(
        ["git", "rev-parse", "@{upstream}"], cwd=PROJECT_ROOT, check=True,
        capture_output=True, text=True,
    ).stdout.strip()
    if head != upstream:
        raise ChallengerPreentryError("provider access requires clean pushed HEAD")
    committed = subprocess.run(
        ["git", "show", f"HEAD:{relative}"], cwd=PROJECT_ROOT, check=True,
        capture_output=True,
    ).stdout
    if hashlib.sha256(committed).hexdigest() != _sha256_file(path):
        raise ChallengerPreentryError(f"committed bytes differ: {relative}")
    return {"path": relative, "sha256": _sha256_file(path), "commit": head}


def _upstream_paths(store_root: Path) -> dict[str, Path]:
    return {
        "selected_pairs": selected_pairs._private_path(
            store_root, selected_pairs.DATASET_ID
        ),
        "source_review": source_semantics._reviewed_path(
            store_root,
            "dataset-primary-source-semantics-contract-2026-07-21-challenger-orb-retest-v1",
            "dataset-development-sec-source-semantics-2026-07-21-challenger-orb-retest-v1",
        ),
        "accession_review": (
            store_root
            / "_derived/challenger_orb_retest_v1/sec_accession_chain_recovery"
            / (
                "dataset-development-sec-accession-chain-recovery-2026-07-21-"
                "challenger-orb-retest-v1/reviewed-result.json.gz"
            )
        ),
    }


def _request(kind: str, symbol: str, day: str) -> dict[str, Any]:
    parsed = date.fromisoformat(day)
    if kind == "candidate_trades":
        start_time = SEARCH_START_ET
    else:
        start_time = START_ET
    start = datetime.combine(parsed, start_time, tzinfo=EASTERN)
    end = datetime.combine(parsed, CUTOFF_ET, tzinfo=EASTERN)
    value = {
        "kind": kind,
        "symbol": symbol,
        "date": day,
        "start_et": start.isoformat(),
        "end_et": end.isoformat(),
        "provider": "alpaca",
        "feed": "sip",
        "adjustment": "raw",
        "use_rth": True,
    }
    return {**value, "request_sha256": _sha256_json(value)}


def build_selection(store_root: Path) -> dict[str, Any]:
    paths = _upstream_paths(store_root)
    selected = selected_pairs._read_gzip(paths["selected_pairs"])
    primary = accession_recovery._read_gzip(paths["source_review"])
    recovered = accession_recovery._read_gzip(paths["accession_review"])
    if not (
        primary.get("status") == "REVIEW_COMPLETE"
        and primary.get("verified_positive_pairs") == 10
        and recovered.get("status") == "REVIEW_COMPLETE"
        and recovered.get("combined_verified_positive_pairs") == 95
        and recovered.get("target_outcomes_observed_or_derived") is False
    ):
        raise ChallengerPreentryError("source-capacity evidence is incomplete")
    positives = {
        str(pair_hash)
        for review in (primary, recovered)
        for pair_hash, disposition in review["pair_dispositions"].items()
        if disposition == "VERIFIED_POSITIVE_PRIMARY"
    }
    pairs = [
        dict(row)
        for row in selected["selected_pairs"]
        if source_semantics._sha256_json(
            (str(row["date"]), str(row["instrument_id"]))
        )
        in positives
    ]
    pairs.sort(key=lambda row: (str(row["date"]), int(row["rank"]), str(row["instrument_id"])))
    dates = sorted({str(row["date"]) for row in pairs})
    if len(pairs) != EXPECTED_POSITIVE_PAIRS or len(dates) != EXPECTED_POSITIVE_DATES:
        raise ChallengerPreentryError("verified-positive pair capacity differs")
    requests = []
    for pair in pairs:
        requests.extend(
            _request(kind, str(pair["symbol"]), str(pair["date"]))
            for kind in ("candidate_bars", "candidate_trades")
        )
    for day in dates:
        requests.extend(_request("benchmark_bars", symbol, day) for symbol in BENCHMARKS)
    requests.sort(key=lambda row: str(row["request_sha256"]))
    if len({row["request_sha256"] for row in requests}) != len(requests):
        raise ChallengerPreentryError("pre-entry requests are not unique")
    return {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "status": "FROZEN_SELECTION",
        "pairs": pairs,
        "requests": requests,
        "counts": {
            "verified_positive_pairs": len(pairs),
            "verified_positive_dates": len(dates),
            "candidate_bar_requests": len(pairs),
            "candidate_trade_requests": len(pairs),
            "benchmark_bar_requests": len(dates) * len(BENCHMARKS),
            "total_requests": len(requests),
            "maximum_daily_signals": len(dates),
        },
        "pair_identity_sha256": _sha256_json(
            [(row["date"], row["instrument_id"]) for row in pairs]
        ),
        "request_graph_sha256": _sha256_json(requests),
        "symbols_dates_rows_and_sources_public": False,
        "post_entry_data_accessed": False,
        "target_outcomes_observed_or_derived": False,
    }


def freeze_inputs(
    *, env_path: Path, output_root: Path, status_path: Path
) -> tuple[Path, dict[str, Any]]:
    for path in (Path(__file__), INSPECTOR):
        _published(path)
    config = HistoricalStoreConfig.from_env(env_path)
    if (
        config.root.resolve().is_relative_to(PROJECT_ROOT.resolve())
        or config.min_free_bytes < MINIMUM_FREE_BYTES
        or shutil.disk_usage(config.root).free < config.min_free_bytes
    ):
        raise ChallengerPreentryError("historical store capacity is unsafe")
    root = _private_root(config.root)
    if root.exists() and any(root.rglob("*")):
        raise ChallengerPreentryError("pre-entry target artifacts exist before freeze")
    selection = build_selection(config.root)
    private_path = _selection_path(config.root)
    _write_gzip(private_path, selection)
    upstream = _upstream_paths(config.root)
    hypothesis = _read_json(HYPOTHESIS)
    if hypothesis.get("contract_sha256") != trigger.HYPOTHESIS_SHA256:
        raise ChallengerPreentryError("challenger hypothesis differs")
    contract = {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "registered_at": datetime.now(UTC).isoformat(),
        "requested_dates": sorted({str(row["date"]) for row in selection["pairs"]}),
        "dataset_payload": {
            "lane": "development",
            "claim_scope": "DEVELOPMENT_ONLY",
            "status": "COLLECTING",
            "evidence_paths": [
                _repo_path(SELECTED_PAIR_MANIFEST),
                _repo_path(SOURCE_SEMANTICS_RESULT),
                _repo_path(ACCESSION_RESULT),
                _repo_path(HYPOTHESIS),
                "CHALLENGER_ORB_RETEST.md",
                "PRODUCTION_STRATEGY_VALIDATION.md",
            ],
            "inspected": False,
        },
        "selection_contract": {
            **selection["counts"],
            "pair_identity_sha256": selection["pair_identity_sha256"],
            "request_graph_sha256": selection["request_graph_sha256"],
            "private_selection_sha256": _sha256_file(private_path),
            "source_private_artifacts": {
                name: _sha256_file(path) for name, path in upstream.items()
            },
            "selection_uses_verified_positive_pairs_only": True,
            "daily_entry_cap": 1,
            "date_symbol_or_missing_input_substitution_allowed": False,
        },
        "collection_contract": {
            "provider": "Alpaca Market Data API",
            "feed": "sip",
            "adjustment": "raw",
            "candidate_bars": "1Min 09:30-10:30 ET",
            "candidate_trades": "raw regular-sale SIP trades 09:35-10:30 ET",
            "benchmark_bars": "SPY and QQQ 1Min 09:30-10:30 ET",
            "whole_provider_fidelity_per_symbol_session": True,
            "local_canonical_cache_first": True,
            "maximum_attempts": 3,
            "checkpoint_unit": "request",
            "provider_switching_allowed": False,
            "substitutions_allowed": False,
        },
        "trigger_contract": {
            "rule_version": trigger.RULE_VERSION,
            "hypothesis_sha256": trigger.HYPOTHESIS_SHA256,
            "sip_condition_rule_version": RULE_VERSION,
            "continuous_cross_version": CONTINUOUS_CROSS_VERSION,
            "search_start_et": SEARCH_START_ET.isoformat(),
            "decision_cutoff_et": CUTOFF_ET.isoformat(),
            "decision_delay_seconds": 10,
            "first_touch_only": True,
            "completed_noninterpolated_retest_bars": True,
            "failed_hold_retention_boundary": "end of first completed touch bar",
            "no_signal_retention_boundary": CUTOFF_ET.isoformat(),
            "minimum_closed_development_signals": 50,
        },
        "implementation_contract": {
            "collector": {"path": _repo_path(Path(__file__)), "sha256": _sha256_file(Path(__file__))},
            "inspector": {"path": _repo_path(INSPECTOR), "sha256": _sha256_file(INSPECTOR)},
            "trigger": {"path": _repo_path(Path(trigger.__file__)), "sha256": _sha256_file(Path(trigger.__file__))},
        },
        "source_bindings": {
            path.name: {"path": _repo_path(path), "sha256": _sha256_file(path)}
            for path in (SELECTED_PAIR_MANIFEST, SOURCE_SEMANTICS_RESULT, ACCESSION_RESULT, HYPOTHESIS)
        },
        "capacity_contract": {
            "minimum_free_bytes": MINIMUM_FREE_BYTES,
            "configured_reserve_bytes": config.min_free_bytes,
            "historical_store_outside_repository": True,
        },
        "outcome_lock": {
            "post_entry_data_access_allowed": False,
            "quote_or_fill_access_allowed": False,
            "return_fields_allowed": False,
            "target_outcomes_observed_or_derived": False,
            "outcome_contract_must_be_separately_frozen": True,
        },
    }
    path, manifest = freeze_dataset_contract(contract, output_root)
    public = {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "status": "FROZEN_AWAITING_INSPECTION",
        "inspected": False,
        "counts": selection["counts"],
        "pair_identity_sha256": selection["pair_identity_sha256"],
        "request_graph_sha256": selection["request_graph_sha256"],
        "private_selection_sha256": _sha256_file(private_path),
        "target_outcomes_observed_or_derived": False,
    }
    _write_json(status_path, public)
    return path, manifest


def _load_contract(
    *, manifest_path: Path, env_path: Path, require_published: bool
) -> tuple[dict[str, Any], HistoricalStoreConfig, dict[str, Any]]:
    manifest = load_frozen_dataset_contract(manifest_path)
    if manifest.get("dataset_id") != DATASET_ID:
        raise ChallengerPreentryError("unexpected pre-entry dataset")
    config = HistoricalStoreConfig.from_env(env_path)
    selection = _read_gzip(_selection_path(config.root))
    contract = manifest["selection_contract"]
    if not (
        _sha256_file(_selection_path(config.root)) == contract["private_selection_sha256"]
        and selection == build_selection(config.root)
        and selection["pair_identity_sha256"] == contract["pair_identity_sha256"]
        and selection["request_graph_sha256"] == contract["request_graph_sha256"]
    ):
        raise ChallengerPreentryError("frozen pre-entry selection changed")
    for value in manifest["implementation_contract"].values():
        path = PROJECT_ROOT / str(value["path"])
        if _sha256_file(path) != value["sha256"]:
            raise ChallengerPreentryError("frozen pre-entry implementation changed")
    for name, expected in contract["source_private_artifacts"].items():
        if _sha256_file(_upstream_paths(config.root)[name]) != expected:
            raise ChallengerPreentryError(f"private source changed: {name}")
    outcome = manifest["outcome_lock"]
    if any(
        outcome.get(field) is not False
        for field in (
            "post_entry_data_access_allowed",
            "quote_or_fill_access_allowed",
            "return_fields_allowed",
            "target_outcomes_observed_or_derived",
        )
    ):
        raise ChallengerPreentryError("pre-entry outcome lock differs")
    if require_published:
        for path in (Path(__file__), INSPECTOR, manifest_path):
            _published(path)
    return manifest, config, selection


def _normalize_bar(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "source_timestamp": str(row.get("source_timestamp") or row["time_et"]),
        "open": float(row["open"]),
        "high": float(row["high"]),
        "low": float(row["low"]),
        "close": float(row["close"]),
        "volume": int(row.get("volume", 0)),
        "wap": float(row.get("wap", 0)),
        "interpolated": bool(row.get("interpolated", False)),
    }


def _normalize_trade(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "source_timestamp": str(row.get("source_timestamp") or row["time_et"]),
        "price": float(row["price"]),
        "size": int(row["size"]),
        "exchange": row.get("exchange"),
        "conditions": row.get("conditions"),
        "trade_id": row.get("trade_id"),
        "tape": row.get("tape"),
    }


def _local_rows(
    local: LocalHistoricalClient, request: Mapping[str, Any]
) -> list[dict[str, Any]]:
    start = datetime.fromisoformat(str(request["start_et"]))
    end = datetime.fromisoformat(str(request["end_et"]))
    if request["kind"] in ("candidate_bars", "benchmark_bars"):
        rows = local.fetch_bars(
            str(request["symbol"]), start, end,
            bar_size="1 min", what="TRADES", use_rth=True,
        )
        return [_normalize_bar(row) for row in rows]
    rows = local.fetch_trades(str(request["symbol"]), start, end, use_rth=True)
    return [_normalize_trade(row) for row in rows]


def _provider_rows(
    recorder: RecordingHistoricalClient, request: Mapping[str, Any]
) -> list[dict[str, Any]]:
    start = datetime.fromisoformat(str(request["start_et"]))
    end = datetime.fromisoformat(str(request["end_et"]))
    if request["kind"] in ("candidate_bars", "benchmark_bars"):
        rows = recorder.fetch_bars(
            str(request["symbol"]), start, end,
            bar_size="1 min", what="TRADES", use_rth=True,
        )
        return [_normalize_bar(row) for row in rows]
    rows = recorder.fetch_trades(
        str(request["symbol"]), start, end, use_rth=True
    )
    return [_normalize_trade(row) for row in rows]


def _attempt(operation: Callable[[], list[dict[str, Any]]]) -> list[dict[str, Any]]:
    last: Exception | None = None
    for number in range(1, 4):
        try:
            return operation()
        except HistoricalProviderError as exc:
            last = exc
            if not exc.retryable or number == 3:
                raise
            time.sleep(2 ** (number - 1))
    assert last is not None
    raise last


def _build_index(
    manifest: Mapping[str, Any], selection: Mapping[str, Any], store_root: Path
) -> dict[str, Any]:
    records = []
    counts: Counter[str] = Counter()
    for request in selection["requests"]:
        request_sha256 = str(request["request_sha256"])
        path = _wrapper_path(store_root, request_sha256)
        if not path.exists():
            continue
        wrapper = _read_gzip(path)
        if not (
            wrapper.get("request_sha256") == request_sha256
            and wrapper.get("manifest_sha256") == manifest["manifest_sha256"]
        ):
            raise ChallengerPreentryError("pre-entry wrapper binding differs")
        counts["terminal_requests"] += 1
        counts["successful_requests"] += wrapper.get("status") == "SUCCESS"
        counts["failed_requests"] += wrapper.get("status") != "SUCCESS"
        counts[f"{request['kind']}_requests"] += 1
        counts["rows"] += int(wrapper.get("row_count") or 0)
        counts[f"{wrapper.get('origin')}_requests"] += 1
        records.append(
            {
                "request_sha256": request_sha256,
                "wrapper_sha256": _sha256_json(wrapper),
                "status": wrapper["status"],
                "kind": request["kind"],
                "origin": wrapper.get("origin"),
                "row_count": wrapper.get("row_count"),
                "rows_sha256": wrapper.get("rows_sha256"),
                "error_category": wrapper.get("error_category"),
            }
        )
    records.sort(key=lambda row: row["request_sha256"])
    counts["expected_requests"] = len(selection["requests"])
    counts["pending_requests"] = counts["expected_requests"] - counts["terminal_requests"]
    return {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "status": "COLLECTION_COMPLETE" if not counts["pending_requests"] else "COLLECTING",
        "counts": dict(sorted(counts.items())),
        "records": records,
        "request_graph_sha256": selection["request_graph_sha256"],
        "post_entry_data_accessed": False,
        "target_outcomes_observed_or_derived": False,
    }


def collect(
    *, manifest_path: Path, env_path: Path, status_path: Path
) -> dict[str, Any]:
    manifest, config, selection = _load_contract(
        manifest_path=manifest_path, env_path=env_path, require_published=True
    )
    alpaca_config = AlpacaConfig.optional_from_env(env_path)
    if alpaca_config is None:
        raise ChallengerPreentryError("Alpaca credentials are not configured")
    store = HistoricalDayStore(config.root)
    local = LocalHistoricalClient(store, "alpaca", feed="sip", adjustment="raw")
    with AlpacaHistoricalClient(alpaca_config) as client:
        recorder = RecordingHistoricalClient(client, store)
        pending = [
            row for row in selection["requests"]
            if not _wrapper_path(config.root, str(row["request_sha256"])).exists()
        ]
        for index, request in enumerate(pending, 1):
            try:
                try:
                    rows = _local_rows(local, request)
                    origin = "LOCAL_CACHE"
                except HistoricalProviderError as exc:
                    if exc.category != "local_cache_miss":
                        raise
                    if shutil.disk_usage(config.root).free < config.min_free_bytes:
                        raise ChallengerPreentryError("historical reserve would be breached")
                    rows = _attempt(lambda: _provider_rows(recorder, request))
                    origin = "ALPACA_DOWNLOAD"
                wrapper = {
                    "schema_version": 1,
                    "dataset_id": DATASET_ID,
                    "manifest_sha256": manifest["manifest_sha256"],
                    "request_sha256": request["request_sha256"],
                    "status": "SUCCESS",
                    "origin": origin,
                    "row_count": len(rows),
                    "rows_sha256": _sha256_json(rows),
                    "error_category": None,
                    "target_outcomes_observed_or_derived": False,
                }
            except (HistoricalProviderError, ChallengerPreentryError, OSError) as exc:
                wrapper = {
                    "schema_version": 1,
                    "dataset_id": DATASET_ID,
                    "manifest_sha256": manifest["manifest_sha256"],
                    "request_sha256": request["request_sha256"],
                    "status": "FAILED",
                    "origin": None,
                    "row_count": 0,
                    "rows_sha256": None,
                    "error_category": getattr(exc, "category", "local_io"),
                    "target_outcomes_observed_or_derived": False,
                }
            _write_gzip(_wrapper_path(config.root, str(request["request_sha256"])), wrapper)
            if index % 25 == 0:
                print(f"pre-entry requests {index}/{len(pending)}", flush=True)
    result = _build_index(manifest, selection, config.root)
    _write_gzip(_index_path(config.root), result)
    public = {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "status": result["status"],
        "counts": result["counts"],
        "private_collection_sha256": _sha256_file(_index_path(config.root)),
        "source_rows_and_identities_public": False,
        "post_entry_data_accessed": False,
        "target_outcomes_observed_or_derived": False,
    }
    _write_json(status_path, public)
    return public


def _load_rows(
    *, request: Mapping[str, Any], wrapper: Mapping[str, Any], local: LocalHistoricalClient
) -> list[dict[str, Any]]:
    if wrapper.get("status") != "SUCCESS":
        raise ChallengerPreentryError("pre-entry request failed")
    if int(wrapper.get("row_count") or 0) == 0:
        rows: list[dict[str, Any]] = []
    else:
        rows = _local_rows(local, request)
    if len(rows) != wrapper.get("row_count") or _sha256_json(rows) != wrapper.get("rows_sha256"):
        raise ChallengerPreentryError("canonical pre-entry rows changed")
    return rows


def _terminal_boundary(
    *, opening_high: float, search_start: datetime, cutoff: datetime,
    trades: Sequence[Mapping[str, Any]], bars: Sequence[Mapping[str, Any]],
) -> datetime:
    initial = trigger._clean_cross(trades, trigger=opening_high)
    if initial is None:
        return cutoff
    initial_at = trigger._observed(initial)
    retest_bar, held = trigger._first_retest_bar(
        bars, initial_break_at=initial_at, opening_high=opening_high, cutoff=cutoff
    )
    if retest_bar is None:
        return cutoff
    retest_end = trigger._observed(retest_bar) + timedelta(minutes=1)
    if held is not True:
        return retest_end
    rebreak = trigger._clean_cross(
        [row for row in trades if trigger._observed(row) >= retest_end],
        trigger=float(retest_bar["high"]),
    )
    if rebreak is None:
        return cutoff
    decision = trigger._observed(rebreak) + timedelta(seconds=10)
    return cutoff if decision > cutoff else decision


def build_trigger_index(
    manifest: Mapping[str, Any], selection: Mapping[str, Any], store_root: Path
) -> dict[str, Any]:
    index = _read_gzip(_index_path(store_root))
    rebuilt = _build_index(manifest, selection, store_root)
    if index != rebuilt or index.get("status") != "COLLECTION_COMPLETE":
        raise ChallengerPreentryError("pre-entry collection is incomplete")
    wrappers = {
        row["request_sha256"]: _read_gzip(_wrapper_path(store_root, row["request_sha256"]))
        for row in index["records"]
    }
    local = LocalHistoricalClient(
        HistoricalDayStore(store_root), "alpaca", feed="sip", adjustment="raw"
    )
    by_key = {
        (row["kind"], row["symbol"], row["date"]): row
        for row in selection["requests"]
    }
    records = []
    counts: Counter[str] = Counter()
    for pair in selection["pairs"]:
        day, symbol = str(pair["date"]), str(pair["symbol"])
        bar_request = by_key[("candidate_bars", symbol, day)]
        trade_request = by_key[("candidate_trades", symbol, day)]
        bars = _load_rows(
            request=bar_request,
            wrapper=wrappers[bar_request["request_sha256"]],
            local=local,
        )
        trades = _load_rows(
            request=trade_request,
            wrapper=wrappers[trade_request["request_sha256"]],
            local=local,
        )
        search_start = datetime.combine(date.fromisoformat(day), SEARCH_START_ET, tzinfo=EASTERN)
        cutoff = datetime.combine(date.fromisoformat(day), CUTOFF_ET, tzinfo=EASTERN)
        causal_bars = [
            row for row in bars
            if search_start <= trigger._observed(row)
            and trigger._observed(row) + timedelta(minutes=1) <= cutoff
        ]
        boundary = _terminal_boundary(
            opening_high=float(pair["scanner_fields"]["opening_high"]),
            search_start=search_start,
            cutoff=cutoff,
            trades=trades,
            bars=causal_bars,
        )
        result = trigger.evaluate_retest_trigger(
            opening_high=float(pair["scanner_fields"]["opening_high"]),
            search_start=search_start,
            cutoff=cutoff,
            captured_through=boundary,
            trades=[row for row in trades if trigger._observed(row) < boundary],
            completed_minute_bars=[
                row for row in causal_bars
                if trigger._observed(row) + timedelta(minutes=1) <= boundary
            ],
            trade_window_complete=True,
            bar_window_complete=True,
        )
        counts[result["terminal_reason"]] += 1
        records.append(
            {
                "pair_hash": source_semantics._sha256_json((day, pair["instrument_id"])),
                "date": day,
                "symbol": symbol,
                "instrument_id": pair["instrument_id"],
                "rank": pair["rank"],
                "scanner_fields": pair["scanner_fields"],
                "causal_boundary_et": boundary.isoformat(),
                "result": result,
            }
        )
    records.sort(key=lambda row: (row["date"], int(row["rank"]), row["instrument_id"]))
    return {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "status": "TRIGGER_REVIEW_COMPLETE",
        "counts": {
            "verified_positive_pairs": len(records),
            "verified_positive_dates": len({row["date"] for row in records}),
            "trigger_found_pairs": counts["TRIGGER_FOUND"],
            "trigger_found_dates": len(
                {row["date"] for row in records if row["result"]["terminal_reason"] == "TRIGGER_FOUND"}
            ),
            "maximum_daily_closed_signals": len(
                {row["date"] for row in records if row["result"]["terminal_reason"] == "TRIGGER_FOUND"}
            ),
            "minimum_required_development_signals": 50,
        },
        "terminal_reason_counts": dict(sorted(counts.items())),
        "records": records,
        "post_entry_data_accessed": False,
        "target_outcomes_observed_or_derived": False,
    }


def derive(
    *, manifest_path: Path, env_path: Path, status_path: Path
) -> dict[str, Any]:
    manifest, config, selection = _load_contract(
        manifest_path=manifest_path, env_path=env_path, require_published=False
    )
    value = build_trigger_index(manifest, selection, config.root)
    _write_gzip(_trigger_path(config.root), value)
    public = {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "status": value["status"],
        "counts": value["counts"],
        "terminal_reason_counts": value["terminal_reason_counts"],
        "private_trigger_sha256": _sha256_file(_trigger_path(config.root)),
        "source_rows_and_identities_public": False,
        "post_entry_data_accessed": False,
        "target_outcomes_observed_or_derived": False,
    }
    _write_json(status_path, public)
    return public


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("freeze", "collect", "derive", "status"))
    parser.add_argument("--manifest", type=Path)
    parser.add_argument("--env-file", type=Path, default=PROJECT_ROOT / ".env")
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--public-status", type=Path, default=DEFAULT_PUBLIC_STATUS)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "freeze":
            path, manifest = freeze_inputs(
                env_path=args.env_file,
                output_root=args.output_root,
                status_path=args.public_status,
            )
            value = {"manifest": _repo_path(path), **manifest}
        elif args.command == "status":
            value = _read_json(args.public_status)
        elif args.manifest is None:
            raise ChallengerPreentryError("--manifest is required")
        elif args.command == "collect":
            value = collect(
                manifest_path=args.manifest,
                env_path=args.env_file,
                status_path=args.public_status,
            )
        else:
            value = derive(
                manifest_path=args.manifest,
                env_path=args.env_file,
                status_path=args.public_status,
            )
    except (
        ChallengerPreentryError,
        HistoricalProviderError,
        LearningDataError,
        OSError,
        subprocess.SubprocessError,
        ValueError,
    ) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}), file=sys.stderr)
        return 2
    print(json.dumps(value, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""Freeze and inspect SEC identity and submissions-request semantics.

This module is deliberately network free. It resolves every frozen selected
pair against the attested point-in-time security master and freezes the exact
SEC submissions request graph before any target response is read. Exact pair,
CIK, and request rows remain in the ignored historical store.
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
from collections import defaultdict
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime, time as wall_time, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import development_catalyst_contract as catalyst_contract
from historical_discovery import SEC_SUBMISSIONS_ROOT, SecConfig
from historical_store import HistoricalStoreConfig, HistoricalStoreError
from learning_data import (
    LearningDataError,
    freeze_dataset_contract,
    load_frozen_dataset_contract,
    load_security_master,
    security_master_sha256,
    security_record_covers,
)
from strategy_engine import StrategyInputError, load_config


PROJECT_ROOT = Path(__file__).resolve().parent
DATASET_ID = "dataset-development-sec-primary-sources-2026-07-19-v2"
SOURCE_CONTRACT = (
    PROJECT_ROOT
    / "historical_batches/development_tranche_v2/catalyst_manifests"
    / (
        "dataset-primary-source-semantics-contract-2026-07-19-development-v2-"
        "04cb264010c5dfd67d6b63290a468f7eefb97aea4dadf00fa97e44d7d3164c2b.json"
    )
)
SELECTED_PAIR_MANIFEST = catalyst_contract.SOURCE_MANIFEST
SECURITY_MASTER = (
    PROJECT_ROOT / "learning/security_masters/development-tranche-v2-master.jsonl"
)
SECURITY_MASTER_SOURCE = catalyst_contract.SECURITY_MASTER_SOURCE
STRATEGY_SOURCE = catalyst_contract.STRATEGY_SOURCE
DEFAULT_OUTPUT_ROOT = (
    PROJECT_ROOT / "historical_batches/development_tranche_v2/sec_manifests"
)
DEFAULT_PUBLIC_STATUS = (
    PROJECT_ROOT / "historical_batches/development_tranche_v2/sec-contract-status.json"
)
DEFAULT_SOURCE_CONTRACT_DOC = PROJECT_ROOT / "DEVELOPMENT_CATALYST_CONTRACT.md"
PRIVATE_NAMESPACE = "_derived/development_sec_sources"
PRIVATE_IDENTITY_FILE = "identity-and-query-map.json.gz"
TARGET_RESPONSE_NAMESPACE = "responses"
SHARED_SEC_CACHE_NAMESPACE = "_sources/sec"
SEC_LOOKBACK_DAYS = 4
SEC_FORMS = ("6-K", "6-K/A", "8-K", "8-K/A")
SEC_WORKERS = 4
SEC_MINIMUM_SPACING_SECONDS = 0.15
SEC_TIMEOUT_SECONDS = 30.0
SEC_MAX_ATTEMPTS = 4
SEC_RETRY_BACKOFF_SECONDS = (0.5, 1.0, 2.0)
MINIMUM_RESERVE_BYTES = 20 * 1024**3
EASTERN = ZoneInfo("America/New_York")


class DevelopmentSecSourceError(RuntimeError):
    """The frozen SEC identity or request contract is unsafe or has drifted."""


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
        raise DevelopmentSecSourceError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise DevelopmentSecSourceError(f"{path} must contain an object")
    return value


def _read_gzip_object(path: Path) -> dict[str, Any]:
    try:
        with gzip.open(path, "rt", encoding="utf-8") as source:
            value = json.load(source)
    except (OSError, json.JSONDecodeError) as exc:
        raise DevelopmentSecSourceError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise DevelopmentSecSourceError(f"{path} must contain an object")
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


def _write_gzip_json(path: Path, value: Any) -> None:
    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode="wb", compresslevel=6, mtime=0) as stream:
        stream.write(json.dumps(value, indent=2, sort_keys=True).encode())
        stream.write(b"\n")
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
        raise DevelopmentSecSourceError(
            f"public path must be repository relative: {path}"
        ) from exc


def _private_root(store_root: Path, dataset_id: str = DATASET_ID) -> Path:
    return store_root / PRIVATE_NAMESPACE / dataset_id


def _private_identity_path(store_root: Path, dataset_id: str = DATASET_ID) -> Path:
    return _private_root(store_root, dataset_id) / PRIVATE_IDENTITY_FILE


def _target_response_root(store_root: Path, dataset_id: str = DATASET_ID) -> Path:
    return _private_root(store_root, dataset_id) / TARGET_RESPONSE_NAMESPACE


def _target_response_artifact_count(
    store_root: Path, dataset_id: str = DATASET_ID
) -> int:
    root = _target_response_root(store_root, dataset_id)
    return sum(1 for path in root.rglob("*") if path.is_file()) if root.exists() else 0


def _selection_private_path(store_root: Path, dataset_id: str) -> Path:
    return (
        store_root
        / "_derived/scanner_selected_pairs"
        / dataset_id
        / "selected-pairs.json.gz"
    )


def _load_pair_surface(
    *,
    source_contract_path: Path,
    selected_manifest_path: Path,
    store_root: Path,
) -> tuple[dict[str, Any], dict[str, Any], list[dict[str, Any]]]:
    source = load_frozen_dataset_contract(source_contract_path)
    selected = load_frozen_dataset_contract(selected_manifest_path)
    rebuilt = catalyst_contract._rebuild_selection(selected, store_root)
    if source.get("selection_contract") != rebuilt:
        raise DevelopmentSecSourceError(
            "source-semantics contract does not bind the selected-pair surface"
        )
    rules = source.get("source_rules")
    acquisition = source.get("acquisition_contract")
    outcome = source.get("outcome_lock")
    if (
        not isinstance(rules, Mapping)
        or rules.get("primary_evidence_only") is not True
        or rules.get("selection_or_source_substitution_allowed") is not False
        or not isinstance(acquisition, Mapping)
        or acquisition.get("primary_source_replacement_with_secondary_news_allowed")
        is not False
        or not isinstance(outcome, Mapping)
        or outcome.get("post_entry_data_access_allowed") is not False
        or outcome.get("target_outcomes_observed_or_derived") is not False
    ):
        raise DevelopmentSecSourceError("source-semantics information lock is open")
    if source.get("requested_dates") != selected.get("requested_dates"):
        raise DevelopmentSecSourceError("source and selection dates differ")

    selection = selected.get("selection_contract")
    if not isinstance(selection, Mapping):
        raise DevelopmentSecSourceError("selected-pair contract is incomplete")
    private = _read_gzip_object(
        _selection_private_path(store_root, str(selected["dataset_id"]))
    )
    expected_hash = str(selection.get("private_selection_content_sha256") or "")
    if _sha256_json(private) != expected_hash:
        raise DevelopmentSecSourceError("private selected-pair content drifted")
    pairs = private.get("selected_pairs")
    if not isinstance(pairs, list) or len(pairs) != rebuilt["selected_pair_count"]:
        raise DevelopmentSecSourceError("private selected-pair surface is incomplete")
    return source, selected, [dict(row) for row in pairs]


def _load_master_contract(
    *,
    master_path: Path,
    master_source_path: Path,
    requested_dates: Sequence[str],
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    source = _read_object(master_source_path)
    details = source.get("security_master")
    if not isinstance(details, Mapping):
        raise DevelopmentSecSourceError("security-master attestation is incomplete")
    if str(details.get("path") or "") != _repo_path(master_path):
        raise DevelopmentSecSourceError("security-master path differs from attestation")
    if source.get("requested_dates") != list(requested_dates):
        raise DevelopmentSecSourceError("security-master dates differ from selection")
    records = load_security_master(master_path)
    canonical_hash = security_master_sha256(master_path)
    if (
        canonical_hash != details.get("sha256")
        or len(records) != int(details.get("records", -1))
        or len({str(row["instrument_id"]) for row in records})
        != int(details.get("instruments", -1))
    ):
        raise DevelopmentSecSourceError("security-master attestation drifted")
    return records, {
        "path": _repo_path(master_path),
        "file_sha256": _sha256_file(master_path),
        "canonical_sha256": canonical_hash,
        "record_count": len(records),
        "instrument_count": len({str(row["instrument_id"]) for row in records}),
        "attestation_path": _repo_path(master_source_path),
        "attestation_sha256": _sha256_file(master_source_path),
    }


def _normalize_cik(value: Any) -> str | None:
    if value is None or not str(value).strip():
        return None
    normalized = str(value).strip()
    if not normalized.isdigit() or len(normalized) > 10 or int(normalized) <= 0:
        raise DevelopmentSecSourceError("security-master CIK is malformed")
    return normalized.zfill(10)


def _resolve_pairs(
    pairs: Sequence[Mapping[str, Any]],
    records: Sequence[Mapping[str, Any]],
    *,
    dataset_id: str = DATASET_ID,
) -> dict[str, Any]:
    indexed: dict[tuple[str, str, str], list[Mapping[str, Any]]] = defaultdict(list)
    for record in records:
        indexed[
            (
                str(record["instrument_id"]),
                str(record["symbol"]),
                str(record["primary_exchange"]),
            )
        ].append(record)

    resolved: list[dict[str, Any]] = []
    for pair in pairs:
        day = str(pair.get("date") or "")
        instrument_id = str(pair.get("instrument_id") or "")
        symbol = str(pair.get("symbol") or "")
        exchange = str(pair.get("primary_exchange") or "")
        rank = int(pair.get("rank", -1))
        parsed = date.fromisoformat(day)
        matches = [
            record
            for record in indexed.get((instrument_id, symbol, exchange), [])
            if security_record_covers(record, parsed)
        ]
        if len(matches) != 1:
            raise DevelopmentSecSourceError(
                f"selected pair does not resolve exactly once on {day}"
            )
        record = matches[0]
        source = record.get("source")
        if not isinstance(source, Mapping):
            raise DevelopmentSecSourceError("security-master source is incomplete")
        cik = _normalize_cik(source.get("cik"))
        cutoff = datetime.combine(parsed, wall_time(9, 35), tzinfo=EASTERN)
        window_start = datetime.combine(
            parsed - timedelta(days=SEC_LOOKBACK_DAYS),
            wall_time(0),
            tzinfo=EASTERN,
        )
        resolved.append(
            {
                "date": day,
                "instrument_id": instrument_id,
                "primary_exchange": exchange,
                "rank": rank,
                "symbol": symbol,
                "master_record_id": str(record["record_id"]),
                "master_valid_from": str(record["valid_from"]),
                "master_valid_to": record.get("valid_to"),
                "identity_source": str(source.get("identity_source") or ""),
                "cik": cik,
                "identity_disposition": (
                    "SEC_SUBMISSIONS_READY" if cik else "CIK_MISSING"
                ),
                "window_start_et": window_start.isoformat(),
                "window_cutoff_et": cutoff.isoformat(),
            }
        )

    if len(resolved) != len(pairs):
        raise DevelopmentSecSourceError("resolved pair count differs")
    ciks = sorted({str(row["cik"]) for row in resolved if row["cik"]})
    requests = [
        {
            "cik": cik,
            "url": f"{SEC_SUBMISSIONS_ROOT}/CIK{cik}.json",
            "shared_cache_relative_path": f"submissions/CIK{cik}.json",
            "target_response_relative_path": f"submissions/CIK{cik}.json",
        }
        for cik in ciks
    ]
    by_day: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in resolved:
        by_day[row["date"]].append(row)
    daily = [
        {
            "date": day,
            "pair_count": len(rows),
            "mapped_count": len(rows),
            "cik_present_count": sum(row["cik"] is not None for row in rows),
            "cik_missing_count": sum(row["cik"] is None for row in rows),
            "private_rows_sha256": _sha256_json(
                sorted(rows, key=lambda row: int(row["rank"]))
            ),
        }
        for day, rows in sorted(by_day.items())
    ]
    return {
        "schema_version": 1,
        "dataset_id": dataset_id,
        "selected_pair_count": len(resolved),
        "mapped_pair_count": len(resolved),
        "cik_present_pair_count": sum(row["cik"] is not None for row in resolved),
        "cik_missing_pair_count": sum(row["cik"] is None for row in resolved),
        "unique_cik_count": len(ciks),
        "listing_scoped_pair_count": sum(
            row["identity_source"] == "composite_figi_listing_scoped"
            for row in resolved
        ),
        "pairs": resolved,
        "submission_requests": requests,
        "submission_request_graph_sha256": _sha256_json(requests),
        "daily_identity_aggregates": daily,
        "daily_identity_aggregates_sha256": _sha256_json(daily),
    }


def _strategy_contract(strategy_source_path: Path) -> dict[str, Any]:
    source = _read_object(strategy_source_path)
    artifact = source.get("artifact")
    if not isinstance(artifact, Mapping):
        raise DevelopmentSecSourceError("strategy attestation is incomplete")
    config_path = PROJECT_ROOT / str(artifact.get("path") or "")
    config = load_config(config_path)
    if (
        _sha256_file(config_path) != artifact.get("file_sha256")
        or config.version != artifact.get("strategy_version")
        or config.rules_hash != artifact.get("canonical_rules_hash")
    ):
        raise DevelopmentSecSourceError("strategy attestation drifted")
    return {
        "strategy_version": config.version,
        "rules_hash": config.rules_hash,
        "config_path": _repo_path(config_path),
        "config_sha256": _sha256_file(config_path),
        "attestation_path": _repo_path(strategy_source_path),
        "attestation_sha256": _sha256_file(strategy_source_path),
    }


def _request_contract(
    env_path: Path,
    private: Mapping[str, Any],
    *,
    dataset_id: str = DATASET_ID,
) -> dict[str, Any]:
    config = SecConfig.from_env(
        env_path,
        Path("unused-network-free-cache-root"),
        workers=SEC_WORKERS,
    )
    if (
        config.minimum_spacing_seconds != SEC_MINIMUM_SPACING_SECONDS
        or config.timeout_seconds != SEC_TIMEOUT_SECONDS
    ):
        raise DevelopmentSecSourceError("SEC client defaults differ from frozen pacing")
    return {
        "stage": "SEC_SUBMISSIONS_ONLY",
        "provider": "SEC_EDGAR",
        "provider_operated_endpoints_only": True,
        "submissions_endpoint_template": f"{SEC_SUBMISSIONS_ROOT}/CIK{{cik_10}}.json",
        "primary_document_endpoint_template": (
            "https://www.sec.gov/Archives/edgar/data/{cik_unpadded}/"
            "{accession_without_dashes}/{primary_document}"
        ),
        "forms": list(SEC_FORMS),
        "window": {
            "calendar_lookback_days": SEC_LOOKBACK_DAYS,
            "start": "TARGET_DATE_MINUS_4_CALENDAR_DAYS_00:00_AMERICA_NEW_YORK",
            "cutoff": "TARGET_DATE_09:35_AMERICA_NEW_YORK_INCLUSIVE",
            "accepted_timestamp_field": "acceptanceDateTime",
            "precise_acceptance_timestamp_required": True,
            "filing_date_alone_is_not_causal_time": True,
        },
        "request_count": int(private["unique_cik_count"]),
        "private_request_graph_sha256": private["submission_request_graph_sha256"],
        "user_agent_sha256": hashlib.sha256(config.user_agent.encode()).hexdigest(),
        "user_agent_identifies_application_and_contact": "@" in config.user_agent,
        "user_agent_public": False,
        "workers": SEC_WORKERS,
        "global_minimum_spacing_seconds": SEC_MINIMUM_SPACING_SECONDS,
        "timeout_seconds": SEC_TIMEOUT_SECONDS,
        "maximum_attempts": SEC_MAX_ATTEMPTS,
        "retry_backoff_seconds": list(SEC_RETRY_BACKOFF_SECONDS),
        "retryable_http_status": "429 or 5xx",
        "retryable_transport_failures": True,
        "cache_policy": {
            "shared_cache_first": True,
            "shared_cache_namespace": (
                f"LOCAL_HISTORICAL_DATA_ROOT/{SHARED_SEC_CACHE_NAMESPACE}/"
            ),
            "shared_cache_may_predate_this_dataset": True,
            "target_response_namespace": (
                "LOCAL_HISTORICAL_DATA_ROOT/"
                f"{PRIVATE_NAMESPACE}/{dataset_id}/{TARGET_RESPONSE_NAMESPACE}/"
            ),
            "target_specific_provenance_required_for_every_request": True,
        },
        "failure_policy": {
            "per_cik_terminal_result_required": True,
            "one_failed_cik_cannot_abort_unrelated_requests": True,
            "missing_cik_remains_terminal_unresolved": True,
            "empty_or_failed_response_is_not_no_filings": True,
            "substitution_allowed": False,
        },
        "staging": {
            "supplemental_submission_files_may_be_discovered_but_not_requested": True,
            "supplemental_request_manifest_required_before_request": True,
            "candidate_documents_may_be_derived_but_not_requested": True,
            "accession_bound_document_manifest_required_before_request": True,
        },
    }


def _implementation_contract() -> dict[str, Any]:
    paths = {
        "sec_contract": Path(__file__),
        "catalyst_contract": Path(catalyst_contract.__file__),
        "historical_discovery": PROJECT_ROOT / "historical_discovery.py",
        "learning_data": PROJECT_ROOT / "learning_data.py",
    }
    return {
        "files": {
            name: {"path": _repo_path(path), "sha256": _sha256_file(path)}
            for name, path in paths.items()
        },
        "python": sys.version.split()[0],
    }


def _stable_contract(
    *,
    source_contract_path: Path,
    selected_manifest_path: Path,
    master_path: Path,
    master_source_path: Path,
    strategy_source_path: Path,
    env_path: Path,
    dataset_id: str = DATASET_ID,
) -> tuple[dict[str, Any], HistoricalStoreConfig, dict[str, Any]]:
    if not dataset_id.startswith("dataset-development-sec-primary-sources-"):
        raise DevelopmentSecSourceError("SEC source dataset namespace is invalid")
    config = HistoricalStoreConfig.from_env(env_path)
    if config.min_free_bytes < MINIMUM_RESERVE_BYTES:
        raise DevelopmentSecSourceError("historical-store reserve is below 20 GiB")
    source, selected, pairs = _load_pair_surface(
        source_contract_path=source_contract_path,
        selected_manifest_path=selected_manifest_path,
        store_root=config.root,
    )
    records, master = _load_master_contract(
        master_path=master_path,
        master_source_path=master_source_path,
        requested_dates=[str(item) for item in selected["requested_dates"]],
    )
    private = _resolve_pairs(pairs, records, dataset_id=dataset_id)
    response_count = _target_response_artifact_count(config.root, dataset_id)
    if response_count:
        raise DevelopmentSecSourceError(
            "target SEC responses exist before the submissions contract freeze"
        )
    public_identity = {
        key: private[key]
        for key in (
            "selected_pair_count",
            "mapped_pair_count",
            "cik_present_pair_count",
            "cik_missing_pair_count",
            "unique_cik_count",
            "listing_scoped_pair_count",
            "submission_request_graph_sha256",
            "daily_identity_aggregates",
            "daily_identity_aggregates_sha256",
        )
    }
    stable = {
        "lineage_contract": {
            "source_semantics_manifest": {
                "path": _repo_path(source_contract_path),
                "sha256": _sha256_file(source_contract_path),
                "manifest_sha256": source["manifest_sha256"],
            },
            "selected_pair_manifest": {
                "path": _repo_path(selected_manifest_path),
                "sha256": _sha256_file(selected_manifest_path),
                "manifest_sha256": selected["manifest_sha256"],
            },
            "private_selection_content_sha256": source["selection_contract"][
                "private_selection_content_sha256"
            ],
            "security_master": master,
            "strategy": _strategy_contract(strategy_source_path),
        },
        "identity_contract": {
            **public_identity,
            "exact_match_fields": [
                "instrument_id",
                "symbol",
                "primary_exchange",
                "point_in_time_coverage",
            ],
            "ambiguous_or_missing_master_match_allowed": False,
            "missing_cik_rows_retained": True,
            "symbols_ciks_and_requests_public": False,
            "private_identity_content_sha256": _sha256_json(private),
            "private_identity_path": (
                "LOCAL_HISTORICAL_DATA_ROOT/"
                f"{PRIVATE_NAMESPACE}/{dataset_id}/{PRIVATE_IDENTITY_FILE}"
            ),
        },
        "request_contract": _request_contract(
            env_path, private, dataset_id=dataset_id
        ),
        "outcome_lock": {
            "target_sources_accessed": False,
            "selected_symbol_detail_accessed": False,
            "target_outcomes_observed_or_derived": False,
            "post_entry_data_access_allowed": False,
            "substitutions_allowed": False,
            "separate_outcome_contract_required": True,
        },
        "pre_freeze_target_response_artifact_count": response_count,
        "implementation_contract": _implementation_contract(),
    }
    return stable, config, private


def _verify_or_write_private(
    *,
    store_root: Path,
    private: Mapping[str, Any],
    write: bool,
    dataset_id: str = DATASET_ID,
) -> None:
    path = _private_identity_path(store_root, dataset_id)
    expected = _sha256_json(private)
    if path.exists():
        if _sha256_json(_read_gzip_object(path)) != expected:
            raise DevelopmentSecSourceError("private SEC identity map drifted")
    elif write:
        _write_gzip_json(path, private)
    else:
        raise DevelopmentSecSourceError("private SEC identity map is missing")


def freeze_contract(
    *,
    source_contract_path: Path = SOURCE_CONTRACT,
    selected_manifest_path: Path = SELECTED_PAIR_MANIFEST,
    master_path: Path = SECURITY_MASTER,
    master_source_path: Path = SECURITY_MASTER_SOURCE,
    strategy_source_path: Path = STRATEGY_SOURCE,
    env_path: Path = PROJECT_ROOT / ".env",
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    dataset_id: str = DATASET_ID,
    source_contract_doc_path: Path = DEFAULT_SOURCE_CONTRACT_DOC,
) -> tuple[Path, dict[str, Any]]:
    stable, config, private = _stable_contract(
        source_contract_path=source_contract_path,
        selected_manifest_path=selected_manifest_path,
        master_path=master_path,
        master_source_path=master_source_path,
        strategy_source_path=strategy_source_path,
        env_path=env_path,
        dataset_id=dataset_id,
    )
    _verify_or_write_private(
        store_root=config.root,
        private=private,
        write=True,
        dataset_id=dataset_id,
    )
    matches = sorted(output_root.glob(f"{dataset_id}-*.json"))
    if len(matches) > 1:
        raise DevelopmentSecSourceError("SEC contract has multiple manifests")
    if matches:
        existing = load_frozen_dataset_contract(matches[0])
        if any(existing.get(key) != value for key, value in stable.items()):
            raise DevelopmentSecSourceError("existing SEC contract drifted")
        return matches[0], existing
    usage = shutil.disk_usage(config.root)
    if usage.free < config.min_free_bytes:
        raise DevelopmentSecSourceError("historical-store reserve is unavailable")
    contract = {
        "schema_version": 1,
        "dataset_id": dataset_id,
        "registered_at": datetime.now(UTC).isoformat(),
        "requested_dates": [
            row["date"]
            for row in stable["identity_contract"]["daily_identity_aggregates"]
        ],
        "dataset_payload": {
            "lane": "development",
            "claim_scope": "DEVELOPMENT_ONLY",
            "status": "COLLECTING",
            "evidence_paths": [
                _repo_path(source_contract_doc_path),
                _repo_path(source_contract_path),
                _repo_path(selected_manifest_path),
                _repo_path(master_source_path),
                "PRODUCTION_STRATEGY_VALIDATION.md",
            ],
            "inspected": False,
        },
        **stable,
        "capacity_contract": {
            "historical_store_outside_repository": not config.root.resolve().is_relative_to(
                PROJECT_ROOT.resolve()
            ),
            "free_bytes_at_freeze": usage.free,
            "reserve_bytes": config.min_free_bytes,
            "minimum_required_reserve_bytes": MINIMUM_RESERVE_BYTES,
            "capacity_ready": True,
            "historical_deletion_allowed": False,
        },
    }
    return freeze_dataset_contract(contract, output_root)


def inspect_contract(
    *,
    manifest_path: Path,
    source_contract_path: Path = SOURCE_CONTRACT,
    selected_manifest_path: Path = SELECTED_PAIR_MANIFEST,
    master_path: Path = SECURITY_MASTER,
    master_source_path: Path = SECURITY_MASTER_SOURCE,
    strategy_source_path: Path = STRATEGY_SOURCE,
    env_path: Path = PROJECT_ROOT / ".env",
    status_path: Path = DEFAULT_PUBLIC_STATUS,
    dataset_id: str = DATASET_ID,
) -> dict[str, Any]:
    manifest = load_frozen_dataset_contract(manifest_path)
    if manifest.get("dataset_id") != dataset_id:
        raise DevelopmentSecSourceError("unexpected SEC contract dataset")
    stable, config, private = _stable_contract(
        source_contract_path=source_contract_path,
        selected_manifest_path=selected_manifest_path,
        master_path=master_path,
        master_source_path=master_source_path,
        strategy_source_path=strategy_source_path,
        env_path=env_path,
        dataset_id=dataset_id,
    )
    for key, value in stable.items():
        if manifest.get(key) != value:
            raise DevelopmentSecSourceError(f"SEC contract {key} drifted")
    _verify_or_write_private(
        store_root=config.root,
        private=private,
        write=False,
        dataset_id=dataset_id,
    )
    capacity = manifest.get("capacity_contract")
    if not isinstance(capacity, Mapping) or any(
        (
            capacity.get("capacity_ready") is not True,
            capacity.get("historical_store_outside_repository") is not True,
            capacity.get("historical_deletion_allowed") is not False,
            int(capacity.get("reserve_bytes", -1)) != config.min_free_bytes,
            int(capacity.get("minimum_required_reserve_bytes", -1))
            != MINIMUM_RESERVE_BYTES,
        )
    ):
        raise DevelopmentSecSourceError("SEC capacity contract is invalid")
    identity = stable["identity_contract"]
    strategy = stable["lineage_contract"]["strategy"]
    status = {
        "schema_version": 1,
        "dataset_id": dataset_id,
        "status": "FROZEN_READY",
        "manifest_sha256": manifest["manifest_sha256"],
        "strategy_version": strategy["strategy_version"],
        "rules_hash": strategy["rules_hash"],
        "requested_dates": len(manifest["requested_dates"]),
        "selected_pair_count": identity["selected_pair_count"],
        "mapped_pair_count": identity["mapped_pair_count"],
        "cik_present_pair_count": identity["cik_present_pair_count"],
        "cik_missing_pair_count": identity["cik_missing_pair_count"],
        "unique_cik_count": identity["unique_cik_count"],
        "listing_scoped_pair_count": identity["listing_scoped_pair_count"],
        "private_identity_content_sha256": identity[
            "private_identity_content_sha256"
        ],
        "submission_request_graph_sha256": identity[
            "submission_request_graph_sha256"
        ],
        "daily_identity_aggregates_sha256": identity[
            "daily_identity_aggregates_sha256"
        ],
        "pre_freeze_target_response_artifact_count": 0,
        "submissions_requested": False,
        "primary_documents_requested": False,
        "substitutions_allowed": False,
        "target_outcomes_observed_or_derived": False,
        "valid": True,
    }
    _write_json(status_path, status)
    return status


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env", type=Path, default=PROJECT_ROOT / ".env")
    parser.add_argument("--dataset-id", default=DATASET_ID)
    parser.add_argument(
        "--source-contract", type=Path, default=SOURCE_CONTRACT
    )
    parser.add_argument(
        "--selected-manifest", type=Path, default=SELECTED_PAIR_MANIFEST
    )
    parser.add_argument("--security-master", type=Path, default=SECURITY_MASTER)
    parser.add_argument(
        "--security-master-source", type=Path, default=SECURITY_MASTER_SOURCE
    )
    parser.add_argument("--strategy-source", type=Path, default=STRATEGY_SOURCE)
    parser.add_argument(
        "--source-contract-doc", type=Path, default=DEFAULT_SOURCE_CONTRACT_DOC
    )
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser("freeze")
    inspect = subparsers.add_parser("inspect")
    inspect.add_argument("manifest", type=Path)
    inspect.add_argument("--status", type=Path, default=DEFAULT_PUBLIC_STATUS)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        common = {
            "source_contract_path": args.source_contract,
            "selected_manifest_path": args.selected_manifest,
            "master_path": args.security_master,
            "master_source_path": args.security_master_source,
            "strategy_source_path": args.strategy_source,
            "env_path": args.env,
            "dataset_id": args.dataset_id,
        }
        if args.command == "freeze":
            path, manifest = freeze_contract(
                output_root=args.output_root,
                source_contract_doc_path=args.source_contract_doc,
                **common,
            )
            value = {
                "dataset_id": args.dataset_id,
                "manifest_sha256": manifest["manifest_sha256"],
                "path": str(path),
                "selected_pair_count": manifest["identity_contract"][
                    "selected_pair_count"
                ],
                "unique_cik_count": manifest["identity_contract"][
                    "unique_cik_count"
                ],
            }
        else:
            value = inspect_contract(
                manifest_path=args.manifest,
                status_path=args.status,
                **common,
            )
        print(json.dumps(value, indent=2, sort_keys=True))
        return 0
    except (
        DevelopmentSecSourceError,
        HistoricalStoreError,
        LearningDataError,
        StrategyInputError,
        OSError,
        ValueError,
    ) as exc:
        print(
            json.dumps(
                {"error": str(exc), "error_type": type(exc).__name__},
                indent=2,
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

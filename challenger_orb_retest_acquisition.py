"""Freeze and operate the challenger point-in-time acquisition boundary.

The generic scanner manifest proves the complete 09:35 universe contract.  This
outer contract additionally binds the preregistered retest trigger and primary
source semantics before any target market request.  It never reads outcomes or
contacts a broker.
"""

from __future__ import annotations

import argparse
import fcntl
import gzip
import hashlib
import importlib.metadata
import json
import os
import shutil
import subprocess
import sys
from contextlib import contextmanager
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import challenger_orb_retest as retest
import challenger_orb_retest_tranche as tranche
import development_catalyst_contract as catalyst_contract
import development_sec_submissions as publication_gate
import scanner_replay
import scanner_replay_alpaca as alpaca
import scanner_replay_inspection
from historical_store import HistoricalDayStore, HistoricalStoreConfig
from learning_data import (
    LearningDataError,
    freeze_dataset_contract,
    load_frozen_dataset_contract,
)


PROJECT_ROOT = Path(__file__).resolve().parent
DATASET_ID = "dataset-challenger-orb-retest-acquisition-2026-07-20-v1"
SCANNER_DATASET_ID = (
    "dataset-production-scanner-replay-2026-07-20-challenger-orb-retest-v1"
)
SELECTION_MANIFEST = (
    PROJECT_ROOT
    / "historical_batches/challenger_orb_retest_v1/selection_manifests"
    / (
        "dataset-challenger-orb-retest-development-2026-07-20-v1-"
        "c1c51924d1bca16797a60e9f92503d36c53700a2e3c102a905900c9ca4ec7dae.json"
    )
)
SELECTION_STATUS = (
    PROJECT_ROOT / "historical_batches/challenger_orb_retest_v1/selection-status.json"
)
RULES = PROJECT_ROOT / "historical_batches/scanner_replay/scanner-rules-v2.json"
STRATEGY_SOURCE = (
    PROJECT_ROOT
    / "historical_batches/scanner_expansion/production-strategy-source.json"
)
DEFAULT_SCANNER_MANIFEST_ROOT = (
    PROJECT_ROOT / "historical_batches/challenger_orb_retest_v1/scanner_manifests"
)
DEFAULT_OUTPUT_ROOT = (
    PROJECT_ROOT / "historical_batches/challenger_orb_retest_v1/acquisition_manifests"
)
DEFAULT_STATUS = (
    PROJECT_ROOT
    / "historical_batches/challenger_orb_retest_v1/acquisition-contract-status.json"
)
RUN_ROOT = PROJECT_ROOT / "learning_runs/challenger_orb_retest_v1/scanner_replay"
REFERENCE_ROOT = RUN_ROOT / "reference"
ACQUISITION_LOCK = RUN_ROOT / "provider-acquisition.lock"
SECURITY_MASTER = (
    PROJECT_ROOT / "historical_batches/challenger_orb_retest_v1/security-master.jsonl"
)
SECURITY_SOURCE = (
    PROJECT_ROOT
    / "historical_batches/challenger_orb_retest_v1/security-master-source.json"
)
SPLITS = RUN_ROOT / "splits.json.gz"
SPLIT_SOURCE = (
    PROJECT_ROOT
    / "historical_batches/challenger_orb_retest_v1/split-actions-source.json"
)
INPUT_STATUS = (
    PROJECT_ROOT
    / "historical_batches/challenger_orb_retest_v1/reference-and-split-status.json"
)
SCANNER_CONTRACT_STATUS = (
    PROJECT_ROOT
    / "historical_batches/challenger_orb_retest_v1/scanner-contract-status.json"
)
REUSE_SCANNER_MANIFEST = (
    PROJECT_ROOT
    / "historical_batches/development_tranche_v3/scanner_manifests"
    / (
        "dataset-production-scanner-replay-2026-07-20-development-v3-"
        "1a37bc3d141dff3741bc965303e490eaa59d8f76071b9e5d1bd2b301eb878926.json"
    )
)
INSPECTOR = PROJECT_ROOT / "challenger_orb_retest_acquisition_inspection.py"


class ChallengerAcquisitionError(RuntimeError):
    """The challenger acquisition boundary is incomplete or has drifted."""


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


def _repo_path(path: Path) -> str:
    try:
        return path.resolve().relative_to(PROJECT_ROOT).as_posix()
    except ValueError as exc:
        raise ChallengerAcquisitionError(
            f"path must remain inside the repository: {path}"
        ) from exc


def _read_object(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ChallengerAcquisitionError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise ChallengerAcquisitionError(f"{path} must contain an object")
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


def _read_gzip_array(path: Path) -> list[dict[str, Any]]:
    try:
        with gzip.open(path, "rt", encoding="utf-8") as source:
            value = json.load(source)
    except (OSError, json.JSONDecodeError) as exc:
        raise ChallengerAcquisitionError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, list) or any(
        not isinstance(row, Mapping) for row in value
    ):
        raise ChallengerAcquisitionError(f"{path} must contain an object array")
    return [dict(row) for row in value]


@contextmanager
def _exclusive_run_lock(path: Path, *, operation: str = "challenger acquisition"):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+", encoding="utf-8") as handle:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise ChallengerAcquisitionError(
                "challenger acquisition is already running"
            ) from exc
        try:
            handle.seek(0)
            handle.truncate()
            handle.write(
                json.dumps(
                    {
                        "operation": operation,
                        "pid": os.getpid(),
                        "started_at": datetime.now(UTC).isoformat(),
                    },
                    sort_keys=True,
                )
                + "\n"
            )
            handle.flush()
            yield
        finally:
            handle.seek(0)
            handle.truncate()
            handle.flush()
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def _binding(path: Path) -> dict[str, str]:
    return {"path": _repo_path(path), "sha256": _sha256_file(path)}


def _verify_binding(value: Mapping[str, Any]) -> None:
    raw = value.get("path")
    if not isinstance(raw, str) or not raw:
        raise ChallengerAcquisitionError("bound path is missing")
    relative = Path(raw)
    if relative.is_absolute() or ".." in relative.parts:
        raise ChallengerAcquisitionError("bound path is unsafe")
    path = PROJECT_ROOT / relative
    if not path.is_file() or _sha256_file(path) != value.get("sha256"):
        raise ChallengerAcquisitionError(f"bound artifact drifted: {relative}")


def _published(path: Path) -> dict[str, str]:
    try:
        return publication_gate._published_source(path)
    except (
        publication_gate.DevelopmentSecSubmissionsError,
        subprocess.SubprocessError,
    ) as exc:
        raise ChallengerAcquisitionError(str(exc)) from exc


def _scanner_manifest_path(root: Path = DEFAULT_SCANNER_MANIFEST_ROOT) -> Path:
    matches = sorted(root.glob(f"{SCANNER_DATASET_ID}-*.json"))
    if len(matches) != 1:
        raise ChallengerAcquisitionError(
            "exactly one frozen challenger scanner manifest is required"
        )
    return matches[0]


def _selection() -> dict[str, Any]:
    selection = _read_object(tranche.DEFAULT_SELECTION)
    status = _read_object(SELECTION_STATUS)
    manifest = load_frozen_dataset_contract(SELECTION_MANIFEST)
    if any(
        (
            selection.get("dataset_id") != tranche.DATASET_ID,
            selection.get("hypothesis_sha256") != retest.HYPOTHESIS_SHA256,
            selection.get("substitution_allowed") is not False,
            selection.get("target_outcomes_observed_or_derived") is not False,
            len(selection.get("selected_dates", [])) != tranche.TARGET_COUNT,
            status.get("status") != "FROZEN_READY",
            status.get("manifest_sha256") != manifest.get("manifest_sha256"),
            manifest.get("selection_contract", {}).get("public_selection_sha256")
            != _sha256_file(tranche.DEFAULT_SELECTION),
        )
    ):
        raise ChallengerAcquisitionError("frozen challenger selection differs")
    return selection


def _validate_scanner_manifest(
    scanner_manifest_path: Path, *, store: HistoricalDayStore
) -> tuple[dict[str, Any], dict[str, Any]]:
    manifest = alpaca.load_contract(scanner_manifest_path)
    if manifest.get("dataset_id") != SCANNER_DATASET_ID:
        raise ChallengerAcquisitionError("scanner dataset identity differs")
    selection = _selection()
    requested = sorted(str(day) for day in selection["selected_dates"])
    if manifest.get("requested_dates") != requested:
        raise ChallengerAcquisitionError("scanner target dates differ")
    alpaca.verify_contract_inputs(manifest, rules_path=RULES)
    calendar = alpaca.verify_calendar_contract(manifest, tranche.CALENDAR)
    required = scanner_replay.required_sessions(requested, calendar)
    collection = manifest["collection_contract"]
    if any(
        (
            collection.get("required_session_dates") != required,
            collection.get("required_session_count") != len(required),
            _sha256_json(required) != selection.get("required_sessions_sha256"),
            collection.get("substitutions_allowed") is not False,
            collection.get("target_data_collection_must_begin_after_freeze")
            is not True,
        )
    ):
        raise ChallengerAcquisitionError("scanner session graph differs")
    status = alpaca.collection_status(manifest, store=store)
    return manifest, status


def _source_rules_contract() -> dict[str, Any]:
    rules = catalyst_contract._source_rules()
    if any(
        (
            rules.get("primary_evidence_only") is not True,
            rules.get("same_day_date_only_fails") is not True,
            rules.get("financing_or_dilution_conflicts_classified_before_positive")
            is not True,
            rules.get("selection_or_source_substitution_allowed") is not False,
        )
    ):
        raise ChallengerAcquisitionError("primary-source rules were weakened")
    return {
        "rules": rules,
        "rules_sha256": _sha256_json(rules),
        "parser_version": catalyst_contract.semantics.PARSER_VERSION,
        "python_version": sys.version.split()[0],
        "pypdf_version": importlib.metadata.version("pypdf"),
        "requests_version": importlib.metadata.version("requests"),
    }


def _provider_config_contract(env_path: Path) -> dict[str, Any]:
    reference = scanner_replay.MassiveReferenceConfig.from_env(env_path)
    market = alpaca.AlpacaBulkConfig.from_env(env_path)
    if reference.base_url != "https://api.massive.com":
        raise ChallengerAcquisitionError(
            "challenger reference collection requires the official Massive origin"
        )
    return {
        "reference": reference.public_dict(),
        "market": {
            "base_url": market.base_url,
            "timeout_seconds": market.timeout_seconds,
            "minimum_interval_seconds": market.minimum_interval_seconds,
            "batch_size": market.batch_size,
            "max_attempts": market.max_attempts,
            "credentials_configured": bool(market.api_key and market.api_secret),
        },
    }


def reference_status() -> dict[str, Any]:
    selection = _selection()
    snapshots: list[dict[str, Any]] = []
    logical_snapshots: list[dict[str, Any]] = []
    missing = 0
    for day in sorted(selection["selected_dates"]):
        path = REFERENCE_ROOT / f"{day}.json.gz"
        if not path.is_file():
            missing += 1
            continue
        rows = _read_gzip_array(path)
        symbols = [str(row.get("ticker") or "").strip().upper() for row in rows]
        if (
            not rows
            or any(not symbol for symbol in symbols)
            or len(symbols) != len(set(symbols))
        ):
            raise ChallengerAcquisitionError(
                f"reference snapshot is empty or ambiguous: {day}"
            )
        raw = {"date": day, "rows": len(rows), "sha256": _sha256_file(path)}
        logical = {
            "date": day,
            "rows": len(rows),
            "content_sha256": _sha256_json(rows),
        }
        snapshots.append(raw)
        logical_snapshots.append(logical)
    return {
        "schema_version": 1,
        "dataset_id": tranche.DATASET_ID,
        "provider": "Massive dated ticker reference",
        "requested": len(selection["selected_dates"]),
        "ready": len(snapshots),
        "missing": missing,
        "complete": missing == 0,
        "snapshot_set_sha256": _sha256_json(snapshots),
        "logical_snapshot_set_sha256": _sha256_json(logical_snapshots),
        "date_substitution_allowed": False,
        "target_market_data_accessed": False,
        "target_outcomes_observed_or_derived": False,
    }


def collect_reference(*, env_path: Path) -> dict[str, Any]:
    _published(Path(__file__))
    selection = _selection()
    with _exclusive_run_lock(
        ACQUISITION_LOCK, operation="dated-reference collection"
    ):
        result = scanner_replay.collect_reference_snapshots(
            selection["selected_dates"],
            config=scanner_replay.MassiveReferenceConfig.from_env(env_path),
            output_root=REFERENCE_ROOT,
        )
    return {
        "schema_version": 1,
        "dataset_id": tranche.DATASET_ID,
        "status": "REFERENCE_COLLECTION_COMPLETE",
        "collection": result,
        "reference": reference_status(),
        "date_substitution_allowed": False,
        "target_market_data_accessed": False,
        "target_outcomes_observed_or_derived": False,
    }


def build_master() -> dict[str, Any]:
    _published(Path(__file__))
    with _exclusive_run_lock(ACQUISITION_LOCK, operation="security-master build"):
        status = reference_status()
        if status["complete"] is not True:
            raise ChallengerAcquisitionError(
                "dated reference collection is incomplete"
            )
        selection = _selection()
        if SECURITY_MASTER.exists() != SECURITY_SOURCE.exists():
            raise ChallengerAcquisitionError(
                "security master and source attestation are only partially present"
            )
        recorded_at = None
        if SECURITY_SOURCE.exists():
            recorded_at = str(_read_object(SECURITY_SOURCE).get("captured_at") or "")
            if not recorded_at:
                raise ChallengerAcquisitionError(
                    "existing security-master attestation lacks captured_at"
                )
        result = scanner_replay.build_security_master(
            selection["selected_dates"],
            snapshots_root=REFERENCE_ROOT,
            output=SECURITY_MASTER,
            source_manifest=SECURITY_SOURCE,
            recorded_at=recorded_at,
        )
    return {
        "schema_version": 1,
        "dataset_id": tranche.DATASET_ID,
        "status": "SECURITY_MASTER_READY",
        "reference": status,
        "security_master": result["security_master"],
        "target_market_data_accessed": False,
        "target_outcomes_observed_or_derived": False,
    }


def _split_attestation() -> dict[str, Any]:
    rows = _read_gzip_array(SPLITS)
    if not rows:
        raise ChallengerAcquisitionError("split action result is unexpectedly empty")
    start, end = "2023-01-04", "2024-12-24"
    ordered: list[tuple[str, str]] = []
    for row in rows:
        execution = str(row.get("execution_date") or "")
        ticker = str(row.get("ticker") or "").strip().upper()
        try:
            split_from = float(row["split_from"])
            split_to = float(row["split_to"])
        except (KeyError, TypeError, ValueError) as exc:
            raise ChallengerAcquisitionError("split row is malformed") from exc
        if (
            not start <= execution <= end
            or not ticker
            or split_from <= 0
            or split_to <= 0
        ):
            raise ChallengerAcquisitionError("split row is outside the frozen query")
        ordered.append((execution, ticker))
    if ordered != sorted(ordered):
        raise ChallengerAcquisitionError(
            "split actions are not deterministically sorted"
        )
    return {
        "schema_version": 1,
        "source": {
            "provider": "Massive",
            "endpoint": "https://api.massive.com/stocks/v1/splits",
            "query_range": {
                "execution_date_gte": start,
                "execution_date_lte": end,
            },
        },
        "artifact": {
            "local_ignored_path": _repo_path(SPLITS),
            "events": len(rows),
            "sha256": _sha256_file(SPLITS),
        },
    }


def collect_splits(*, env_path: Path) -> dict[str, Any]:
    _published(Path(__file__))
    with _exclusive_run_lock(ACQUISITION_LOCK, operation="split-action collection"):
        result = scanner_replay.collect_split_actions(
            start="2023-01-04",
            end="2024-12-24",
            config=scanner_replay.MassiveReferenceConfig.from_env(env_path),
            output=SPLITS,
        )
        attestation = _split_attestation()
        _write_json(SPLIT_SOURCE, attestation)
    return {
        "schema_version": 1,
        "dataset_id": tranche.DATASET_ID,
        "status": "SPLIT_ACTIONS_READY",
        "collection": result,
        "attestation": attestation,
        "target_market_data_accessed": False,
        "target_outcomes_observed_or_derived": False,
    }


def audit_inputs(*, env_path: Path) -> dict[str, Any]:
    reference = reference_status()
    if reference["complete"] is not True:
        raise ChallengerAcquisitionError("dated reference collection is incomplete")
    source = _read_object(SECURITY_SOURCE)
    selection = _selection()
    expected_dates = sorted(selection["selected_dates"])
    snapshot_rows = source.get("snapshots")
    if (
        source.get("requested_dates") != expected_dates
        or source.get("source", {}).get("provider") != "Massive"
        or source.get("source", {}).get("endpoint")
        != scanner_replay.MASSIVE_REFERENCE_URL
        or source.get("source", {}).get("query_contract")
        != {
            "market": "stocks",
            "locale": "us",
            "type": "CS",
            "active": True,
            "point_in_time_parameter": "date",
        }
        or source.get("security_master", {}).get("path") != _repo_path(SECURITY_MASTER)
        or source.get("security_master", {}).get("sha256")
        != scanner_replay.security_master_sha256(SECURITY_MASTER)
        or not isinstance(snapshot_rows, list)
        or [str(row.get("date") or "") for row in snapshot_rows] != expected_dates
    ):
        raise ChallengerAcquisitionError("security-master attestation differs")
    for row in snapshot_rows:
        day = str(row.get("date") or "")
        path = REFERENCE_ROOT / f"{day}.json.gz"
        if (
            day not in expected_dates
            or row.get("sha256") != _sha256_file(path)
            or row.get("rows") != len(_read_gzip_array(path))
        ):
            raise ChallengerAcquisitionError("reference snapshot attestation differs")
    expected_split = _split_attestation()
    if _read_object(SPLIT_SOURCE) != expected_split:
        raise ChallengerAcquisitionError("split-actions attestation differs")
    config = HistoricalStoreConfig.from_env(env_path)
    store = HistoricalDayStore(config.root)
    market_root = alpaca.index_root(store, SCANNER_DATASET_ID)
    pre_freeze_market_artifacts = (
        sum(1 for path in market_root.rglob("*") if path.is_file())
        if market_root.exists()
        else 0
    )
    if pre_freeze_market_artifacts:
        raise ChallengerAcquisitionError(
            "target scanner artifacts exist before scanner freeze"
        )
    result = {
        "schema_version": 1,
        "dataset_id": tranche.DATASET_ID,
        "status": "INPUTS_READY",
        "reference": reference,
        "security_master": source["security_master"],
        "split_actions": expected_split["artifact"],
        "pre_freeze_target_market_artifacts": pre_freeze_market_artifacts,
        "capacity_ready": shutil.disk_usage(config.root).free >= config.min_free_bytes,
        "reserve_bytes": config.min_free_bytes,
        "date_symbol_or_provider_substitution_allowed": False,
        "target_market_data_accessed": False,
        "target_outcomes_observed_or_derived": False,
    }
    if result["capacity_ready"] is not True:
        raise ChallengerAcquisitionError("historical-store reserve is unavailable")
    _write_json(INPUT_STATUS, result)
    return result


def freeze_scanner(*, env_path: Path) -> tuple[Path, dict[str, Any]]:
    _published(Path(__file__))
    _published(SECURITY_MASTER)
    _published(SECURITY_SOURCE)
    _published(SPLIT_SOURCE)
    _published(INPUT_STATUS)
    audit_inputs(env_path=env_path)
    config = HistoricalStoreConfig.from_env(env_path)
    store = HistoricalDayStore(config.root)
    path, manifest = alpaca.freeze_contract(
        dataset_id=SCANNER_DATASET_ID,
        selection_path=tranche.DEFAULT_SELECTION,
        calendar_path=tranche.CALENDAR,
        rules_path=RULES,
        security_path=SECURITY_MASTER,
        security_source_path=SECURITY_SOURCE,
        split_path=SPLITS,
        split_source_path=SPLIT_SOURCE,
        strategy_source_path=STRATEGY_SOURCE,
        output_root=DEFAULT_SCANNER_MANIFEST_ROOT,
        index_root=alpaca.index_root(store, SCANNER_DATASET_ID),
        reuse_manifest_path=REUSE_SCANNER_MANIFEST,
    )
    status = alpaca.collection_status(manifest, store=store)
    if any(
        (
            status["session_files"]["ready"] != 0,
            status["provider_requests"] != 0,
            status["provider_retries"] != 0,
            status["derived_rows"] != 0,
            status["canonical_day_merges"] != 0,
        )
    ):
        raise ChallengerAcquisitionError("scanner zero-state differs after freeze")
    public = {
        "schema_version": 1,
        "dataset_id": SCANNER_DATASET_ID,
        "status": "FROZEN_READY",
        "manifest_path": _repo_path(path),
        "manifest_sha256": manifest["manifest_sha256"],
        "required_sessions": manifest["collection_contract"]["required_session_count"],
        "target_symbol_union_count": manifest["collection_contract"][
            "target_symbol_union_count"
        ],
        "pre_freeze_target_market_artifacts": 0,
        "substitutions_allowed": False,
        "target_outcomes_observed_or_derived": False,
    }
    _write_json(SCANNER_CONTRACT_STATUS, public)
    return path, public


def _expected_contract(
    *,
    scanner_manifest_path: Path,
    store: HistoricalDayStore,
    env_path: Path,
    require_zero_market_state: bool,
) -> dict[str, Any]:
    selection = _selection()
    scanner_manifest, scanner_status = _validate_scanner_manifest(
        scanner_manifest_path, store=store
    )
    if require_zero_market_state and any(
        (
            scanner_status["session_files"]["ready"] != 0,
            scanner_status["provider_requests"] != 0,
            scanner_status["provider_retries"] != 0,
            scanner_status["derived_rows"] != 0,
            scanner_status["canonical_day_merges"] != 0,
        )
    ):
        raise ChallengerAcquisitionError(
            "target scanner artifacts exist before the outer acquisition freeze"
        )
    hypothesis = _read_object(tranche.HYPOTHESIS)
    if hypothesis.get("contract_sha256") != retest.HYPOTHESIS_SHA256:
        raise ChallengerAcquisitionError("challenger hypothesis differs")
    collection = scanner_manifest["collection_contract"]
    provider_config = _provider_config_contract(env_path)
    return {
        "implementation_contract": {
            "controller": _binding(Path(__file__)),
            "inspector": _binding(INSPECTOR),
            "trigger": _binding(Path(retest.__file__)),
            "scanner_engine": _binding(Path(scanner_replay.__file__)),
            "scanner_adapter": _binding(Path(alpaca.__file__)),
            "scanner_inspector": _binding(Path(scanner_replay_inspection.__file__)),
            "source_rules_builder": _binding(Path(catalyst_contract.__file__)),
            "source_semantics": _binding(Path(catalyst_contract.semantics.__file__)),
        },
        "upstream_contract": {
            "hypothesis": _binding(tranche.HYPOTHESIS),
            "selection": _binding(tranche.DEFAULT_SELECTION),
            "selection_manifest": _binding(SELECTION_MANIFEST),
            "selection_status": _binding(SELECTION_STATUS),
            "calendar": _binding(tranche.CALENDAR),
            "calendar_source": _binding(tranche.CALENDAR_SOURCE),
            "calendar_status": _binding(tranche.CALENDAR_STATUS),
            "scanner_manifest": _binding(scanner_manifest_path),
            "scanner_rules": _binding(RULES),
            "strategy_source": _binding(STRATEGY_SOURCE),
        },
        "reference_identity_contract": {
            "provider": "Massive dated ticker reference",
            "endpoint": scanner_replay.MASSIVE_REFERENCE_URL,
            "query": {
                "market": "stocks",
                "locale": "us",
                "type": "CS",
                "active": True,
                "point_in_time_parameter": "date",
                "sort": "ticker",
                "order": "asc",
                "limit": 1000,
            },
            "requested_dates_sha256": selection["selected_dates_sha256"],
            "requested_date_count": len(selection["selected_dates"]),
            "provider_config": provider_config["reference"],
        },
        "full_universe_market_contract": {
            "scanner_dataset_id": SCANNER_DATASET_ID,
            "scanner_manifest_sha256": scanner_manifest["manifest_sha256"],
            "provider": collection["source"],
            "endpoint": collection["endpoint"],
            "feed": collection["feed"],
            "adjustment": collection["adjustment"],
            "regular_session_query": collection["regular_session_query"],
            "opening_query": collection["opening_query"],
            "required_session_count": collection["required_session_count"],
            "required_sessions_sha256": selection["required_sessions_sha256"],
            "complete_universe": True,
            "canonical_store_required": True,
            "whole_provider_fidelity_per_symbol_session": True,
            "substitutions_allowed": False,
            "pre_freeze_ready_sessions": 0,
            "pre_freeze_provider_requests": 0,
            "pre_freeze_provider_retries": 0,
            "pre_freeze_derived_rows": 0,
            "pre_freeze_canonical_day_merges": 0,
            "provider_config": provider_config["market"],
        },
        "primary_source_contract": _source_rules_contract(),
        "selected_symbol_contract": {
            "full_universe_detail_forbidden": True,
            "scanner_selected_symbols_only": True,
            "provider_priority": [
                "local canonical cache",
                "IBKR",
                "Massive",
                "Alpaca",
            ],
            "whole_provider_fidelity_per_symbol_session": True,
            "initial_break_search": "causal one-second regular-sale trades from 09:35 ET",
            "retest_input": "fully completed noninterpolated one-minute bars after the initial-break minute",
            "rebreak_search": "causal one-second regular-sale trades after the held retest",
            "decision_boundary": "ten seconds after first condition-valid rebreak, no later than 10:30 ET",
            "failed_hold_retention_boundary": "end of first completed touch bar",
            "no_signal_retention_boundary": "10:30 ET",
            "exact_pair_graph_requires_separate_post-scanner_freeze": True,
        },
        "outcome_lock": {
            "post_entry_data_access_allowed": False,
            "return_fields_allowed": False,
            "outcome_contract_must_be_separately_frozen": True,
            "date_symbol_provider_or_missing_input_substitution_allowed": False,
            "target_outcomes_observed_or_derived": False,
        },
    }


def freeze_contract(
    *,
    scanner_manifest_path: Path,
    env_path: Path,
    output_root: Path = DEFAULT_OUTPUT_ROOT,
) -> tuple[Path, dict[str, Any]]:
    _published(Path(__file__))
    _published(INSPECTOR)
    _published(scanner_manifest_path)
    config = HistoricalStoreConfig.from_env(env_path)
    store = HistoricalDayStore(config.root)
    usage = shutil.disk_usage(config.root)
    if usage.free < config.min_free_bytes:
        raise ChallengerAcquisitionError("historical-store reserve is unavailable")
    stable = _expected_contract(
        scanner_manifest_path=scanner_manifest_path,
        store=store,
        env_path=env_path,
        require_zero_market_state=True,
    )
    selection = _selection()
    contract = {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "registered_at": datetime.now(UTC).isoformat(),
        "requested_dates": sorted(selection["selected_dates"]),
        "dataset_payload": {
            "lane": "development",
            "claim_scope": "DEVELOPMENT_ONLY",
            "status": "COLLECTING",
            "evidence_paths": [
                "CHALLENGER_ORB_RETEST.md",
                _repo_path(tranche.HYPOTHESIS),
                _repo_path(SELECTION_MANIFEST),
                _repo_path(scanner_manifest_path),
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
            "capacity_ready": True,
            "historical_deletion_allowed": False,
        },
    }
    return freeze_dataset_contract(contract, output_root)


def inspect_contract(
    *, manifest_path: Path, scanner_manifest_path: Path, env_path: Path
) -> dict[str, Any]:
    manifest = load_frozen_dataset_contract(manifest_path)
    if manifest.get("dataset_id") != DATASET_ID:
        raise ChallengerAcquisitionError("acquisition dataset identity differs")
    config = HistoricalStoreConfig.from_env(env_path)
    store = HistoricalDayStore(config.root)
    stable = _expected_contract(
        scanner_manifest_path=scanner_manifest_path,
        store=store,
        env_path=env_path,
        require_zero_market_state=True,
    )
    for key, value in stable.items():
        if manifest.get(key) != value:
            raise ChallengerAcquisitionError(f"acquisition {key} drifted")
    capacity = manifest.get("capacity_contract")
    if not isinstance(capacity, Mapping) or any(
        (
            capacity.get("capacity_ready") is not True,
            capacity.get("historical_store_outside_repository") is not True,
            capacity.get("historical_deletion_allowed") is not False,
            int(capacity.get("reserve_bytes", -1)) != config.min_free_bytes,
        )
    ):
        raise ChallengerAcquisitionError("capacity contract is invalid")
    return {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "status": "FROZEN_READY",
        "inspected": True,
        "requested_dates": len(manifest["requested_dates"]),
        "required_sessions": stable["full_universe_market_contract"][
            "required_session_count"
        ],
        "pre_freeze_target_market_artifacts": 0,
        "source_rules_sha256": stable["primary_source_contract"]["rules_sha256"],
        "substitutions_allowed": False,
        "target_outcomes_observed_or_derived": False,
        "valid": True,
    }


def collect_scanner(
    *, manifest_path: Path, env_path: Path, max_days: int | None
) -> dict[str, Any]:
    _published(Path(__file__))
    _published(manifest_path)
    outer = load_frozen_dataset_contract(manifest_path)
    if outer.get("dataset_id") != DATASET_ID:
        raise ChallengerAcquisitionError("acquisition dataset identity differs")
    scanner_binding = outer.get("upstream_contract", {}).get("scanner_manifest")
    if not isinstance(scanner_binding, Mapping):
        raise ChallengerAcquisitionError("scanner manifest binding is missing")
    _verify_binding(scanner_binding)
    scanner_path = PROJECT_ROOT / str(scanner_binding["path"])
    config = HistoricalStoreConfig.from_env(env_path)
    store = HistoricalDayStore(config.root)
    stable = _expected_contract(
        scanner_manifest_path=scanner_path,
        store=store,
        env_path=env_path,
        require_zero_market_state=False,
    )
    for key, value in stable.items():
        if outer.get(key) != value:
            raise ChallengerAcquisitionError(f"acquisition {key} drifted")
    if max_days is not None and max_days < 1:
        raise ChallengerAcquisitionError("--max-days must be positive")
    with _exclusive_run_lock(
        ACQUISITION_LOCK, operation="full-universe scanner collection"
    ):
        return alpaca.collect_contract(
            alpaca.load_contract(scanner_path),
            config=alpaca.AlpacaBulkConfig.from_env(env_path),
            store=store,
            max_days=max_days,
        )


def status(*, manifest_path: Path, env_path: Path) -> dict[str, Any]:
    outer = load_frozen_dataset_contract(manifest_path)
    scanner_binding = outer.get("upstream_contract", {}).get("scanner_manifest")
    if not isinstance(scanner_binding, Mapping):
        raise ChallengerAcquisitionError("scanner manifest binding is missing")
    _verify_binding(scanner_binding)
    scanner_path = PROJECT_ROOT / str(scanner_binding["path"])
    store = HistoricalDayStore.from_env(env_path)
    scanner_manifest, scanner_status = _validate_scanner_manifest(
        scanner_path, store=store
    )
    return {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "manifest_sha256": outer["manifest_sha256"],
        "scanner_dataset_id": scanner_manifest["dataset_id"],
        "scanner": scanner_status,
        "target_outcomes_observed_or_derived": False,
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env", type=Path, default=PROJECT_ROOT / ".env")
    parser.add_argument("--scanner-manifest", type=Path)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("collect-reference")
    sub.add_parser("reference-status")
    sub.add_parser("build-master")
    sub.add_parser("collect-splits")
    sub.add_parser("audit-inputs")
    sub.add_parser("freeze-scanner")
    sub.add_parser("freeze")
    inspect_parser = sub.add_parser("inspect")
    inspect_parser.add_argument("manifest", type=Path)
    collect_parser = sub.add_parser("collect-scanner")
    collect_parser.add_argument("manifest", type=Path)
    collect_parser.add_argument("--max-days", type=int)
    status_parser = sub.add_parser("status")
    status_parser.add_argument("manifest", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "collect-reference":
            result = collect_reference(env_path=args.env)
        elif args.command == "reference-status":
            result = reference_status()
        elif args.command == "build-master":
            result = build_master()
        elif args.command == "collect-splits":
            result = collect_splits(env_path=args.env)
        elif args.command == "audit-inputs":
            result = audit_inputs(env_path=args.env)
        elif args.command == "freeze-scanner":
            _path, result = freeze_scanner(env_path=args.env)
        elif args.command == "freeze":
            scanner_path = args.scanner_manifest or _scanner_manifest_path()
            path, manifest = freeze_contract(
                scanner_manifest_path=scanner_path, env_path=args.env
            )
            result = {
                "schema_version": 1,
                "dataset_id": DATASET_ID,
                "path": _repo_path(path),
                "manifest_sha256": manifest["manifest_sha256"],
                "status": "FROZEN_READY",
                "inspected": False,
                "target_outcomes_observed_or_derived": False,
            }
            _write_json(DEFAULT_STATUS, result)
        elif args.command == "inspect":
            scanner_path = args.scanner_manifest or _scanner_manifest_path()
            result = inspect_contract(
                manifest_path=args.manifest,
                scanner_manifest_path=scanner_path,
                env_path=args.env,
            )
            _write_json(DEFAULT_STATUS, result)
        elif args.command == "collect-scanner":
            result = collect_scanner(
                manifest_path=args.manifest,
                env_path=args.env,
                max_days=args.max_days,
            )
        else:
            result = status(manifest_path=args.manifest, env_path=args.env)
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    except (
        ChallengerAcquisitionError,
        LearningDataError,
        scanner_replay.ScannerReplayError,
        OSError,
        ValueError,
    ) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())

"""Freeze an exact disjoint 100-session production-development tranche."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import io
import json
import math
import os
import random
import re
import shutil
import sys
from collections.abc import Mapping, Sequence
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from historical_store import HistoricalDayStore
from learning_data import (
    LearningDataError,
    freeze_dataset_contract,
    load_frozen_dataset_contract,
)
from learning_registry import REGISTRY_ROOT, RegistryError, current_entities
from scanner_replay import load_selection, required_sessions


PROJECT_ROOT = Path(__file__).resolve().parent
DATASET_ID = "dataset-development-tranche-2026-07-19-v2"
SEED = 2026071902
TARGET_COUNT = 100
ELIGIBLE_START = "2025-01-02"
ELIGIBLE_END = "2025-11-28"
PRIOR_SESSIONS = 15
CALENDAR = (
    PROJECT_ROOT
    / "historical_batches"
    / "preentry_structure"
    / "session-calendar-2024-12-through-2026-06.json"
)
CALENDAR_SOURCE = (
    PROJECT_ROOT
    / "historical_batches"
    / "preentry_structure"
    / "session-calendar-source.json"
)
SIGNALS = PROJECT_ROOT / "SIGNALS.jsonl"
ARCHIVED_CONTEXT_ROOT = PROJECT_ROOT / "trades" / "archived"
DATASET_REGISTRY_ROOT = REGISTRY_ROOT
PRIOR_SELECTIONS = (
    PROJECT_ROOT
    / "historical_batches"
    / "scanner_replay"
    / "selection-2026-07-18-20-days.json",
    PROJECT_ROOT
    / "historical_batches"
    / "scanner_expansion"
    / "selection-2026-07-19-100-days.json",
)
PRIOR_SCANNER_STATUS = (
    PROJECT_ROOT / "historical_batches" / "scanner_expansion" / "collection-status.json"
)
PRIOR_RUN_ROOT = PROJECT_ROOT / "learning_runs" / "scanner_expansion"
PRIOR_DERIVED_ROOT = (
    PROJECT_ROOT.parent
    / "historical_data"
    / "_derived"
    / "scanner_replay"
    / "dataset-production-scanner-replay-2026-07-19-expansion-v1"
)
SOURCE_SEMANTICS_RESULT = (
    PROJECT_ROOT
    / "research_results"
    / "2026-07-19-catalyst-issuer-chain-semantics.json"
)
DEFAULT_SELECTION = (
    PROJECT_ROOT
    / "historical_batches"
    / "development_tranche_v2"
    / "selection-2026-07-19-100-days.json"
)
DEFAULT_OUTPUT_ROOT = (
    PROJECT_ROOT / "historical_batches" / "development_tranche_v2" / "manifests"
)
PRIVATE_ROOT_NAME = "development_tranche_v2"
DATE_PATTERN = re.compile(r"(?<!\d)(20\d{2})[-_]([01]\d)[-_]([0-3]\d)(?!\d)")


class DevelopmentTrancheError(RuntimeError):
    """The disjoint tranche contract cannot be proven or honored."""


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
    try:
        with path.open("rb") as source:
            for chunk in iter(lambda: source.read(1024 * 1024), b""):
                digest.update(chunk)
    except OSError as exc:
        raise DevelopmentTrancheError(f"cannot hash {path}: {exc}") from exc
    return digest.hexdigest()


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise DevelopmentTrancheError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise DevelopmentTrancheError(f"{path} must contain an object")
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


def _read_gzip(path: Path) -> dict[str, Any]:
    try:
        with gzip.open(path, "rt", encoding="utf-8") as source:
            value = json.load(source)
    except (OSError, json.JSONDecodeError) as exc:
        raise DevelopmentTrancheError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise DevelopmentTrancheError(f"{path} must contain an object")
    return value


def _repo_path(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(PROJECT_ROOT))
    except ValueError as exc:
        raise DevelopmentTrancheError(
            f"public path must be repository-relative: {path}"
        ) from exc


def _private_path(store_root: Path) -> Path:
    return (
        store_root / "_derived" / PRIVATE_ROOT_NAME / DATASET_ID / "exclusions.json.gz"
    )


def _normalize_date(value: str) -> str:
    try:
        return date.fromisoformat(value).isoformat()
    except ValueError as exc:
        raise DevelopmentTrancheError(f"invalid target date: {value}") from exc


def _calendar_dates(path: Path = CALENDAR) -> list[str]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise DevelopmentTrancheError(f"cannot read calendar: {exc}") from exc
    if not isinstance(value, list) or not value:
        raise DevelopmentTrancheError("calendar must be a non-empty array")
    dates: list[str] = []
    for row in value:
        raw = row.get("date") if isinstance(row, Mapping) else row
        if not isinstance(raw, str):
            raise DevelopmentTrancheError("calendar row lacks an ISO date")
        dates.append(_normalize_date(raw))
    if dates != sorted(set(dates)):
        raise DevelopmentTrancheError("calendar dates must be unique and chronological")
    return dates


def _signal_exclusions(path: Path = SIGNALS) -> tuple[set[str], int]:
    dates: set[str] = set()
    records = 0
    try:
        with path.open(encoding="utf-8") as source:
            for number, raw in enumerate(source, start=1):
                if not raw.strip():
                    continue
                records += 1
                try:
                    row = json.loads(raw)
                except json.JSONDecodeError as exc:
                    raise DevelopmentTrancheError(
                        f"invalid signal JSON at line {number}"
                    ) from exc
                if not isinstance(row, Mapping) or not isinstance(row.get("date"), str):
                    raise DevelopmentTrancheError(f"signal line {number} lacks a date")
                dates.add(_normalize_date(str(row["date"])))
    except OSError as exc:
        raise DevelopmentTrancheError(f"cannot read signal ledger: {exc}") from exc
    return dates, records


def _archived_context_exclusions(
    root: Path = ARCHIVED_CONTEXT_ROOT,
) -> tuple[set[str], list[dict[str, str]]]:
    dates: set[str] = set()
    artifacts: list[dict[str, str]] = []
    for path in sorted(item for item in root.rglob("*") if item.is_file()):
        if path.name == ".gitkeep":
            continue
        relative = _repo_path(path)
        matches = list(DATE_PATTERN.finditer(relative))
        if not matches:
            raise DevelopmentTrancheError(
                f"archived context path lacks a session date: {relative}"
            )
        normalized = {_normalize_date("-".join(match.groups())) for match in matches}
        if len(normalized) != 1:
            raise DevelopmentTrancheError(
                f"archived context path has conflicting dates: {relative}"
            )
        dates.update(normalized)
        artifacts.append({"path": relative, "sha256": _sha256_file(path)})
    if not artifacts:
        raise DevelopmentTrancheError("archived context set is unexpectedly empty")
    return dates, artifacts


def _extract_target_dates(value: Any) -> set[str]:
    """Extract only explicitly labeled target-date collections from evidence."""

    dates: set[str] = set()
    if isinstance(value, Mapping):
        for key, child in value.items():
            if key in {"requested_dates", "selected_dates"}:
                if not isinstance(child, list):
                    continue
                if any(not isinstance(item, str) for item in child):
                    raise DevelopmentTrancheError(f"{key} must contain ISO dates")
                dates.update(_normalize_date(item) for item in child)
            elif key == "frozen_dates":
                if not isinstance(child, list):
                    raise DevelopmentTrancheError("frozen_dates must be an array")
                for item in child:
                    raw = item.get("date") if isinstance(item, Mapping) else item
                    if not isinstance(raw, str):
                        raise DevelopmentTrancheError(
                            "frozen_dates rows must contain ISO dates"
                        )
                    dates.add(_normalize_date(raw))
            elif key == "candidates_by_date":
                if not isinstance(child, Mapping):
                    raise DevelopmentTrancheError(
                        "candidates_by_date must be an object"
                    )
                dates.update(_normalize_date(str(item)) for item in child)
            dates.update(_extract_target_dates(child))
    elif isinstance(value, list):
        for child in value:
            dates.update(_extract_target_dates(child))
    return dates


def _registered_inspected_exclusions(
    *, registry_root: Path = DATASET_REGISTRY_ROOT
) -> tuple[set[str], dict[str, Any]]:
    """Bind every JSON artifact cited by a currently inspected dataset."""

    try:
        entities = current_entities("datasets", registry_root)
    except RegistryError as exc:
        raise DevelopmentTrancheError(f"dataset registry is invalid: {exc}") from exc
    evidence_paths: set[str] = set()
    inspected_entities = 0
    inspected_events: list[dict[str, Any]] = []
    for event in entities.values():
        payload = event["payload"]
        if payload.get("inspected") is not True:
            continue
        inspected_entities += 1
        inspected_events.append(event)
        evidence_paths.update(
            item
            for item in payload.get("evidence_paths", [])
            if Path(item).suffix == ".json"
        )
    dates: set[str] = set()
    artifacts: list[dict[str, Any]] = []
    for relative in sorted(evidence_paths):
        path = PROJECT_ROOT / relative
        value = _read_json(path)
        artifact_dates = sorted(_extract_target_dates(value))
        dates.update(artifact_dates)
        artifacts.append(
            {
                "path": relative,
                "sha256": _sha256_file(path),
                "target_dates": len(artifact_dates),
                "target_dates_sha256": _sha256_json(artifact_dates),
            }
        )
    registry_path = registry_root / "DATASETS.jsonl"
    return dates, {
        "registry_path": _repo_path(registry_path),
        "registry_sha256": _sha256_file(registry_path),
        "inspected_entity_set_sha256": _sha256_json(
            sorted(inspected_events, key=lambda item: str(item["entity_id"]))
        ),
        "inspected_entities": inspected_entities,
        "json_evidence_artifacts": len(artifacts),
        "artifact_set_sha256": _sha256_json(artifacts),
        "artifacts": artifacts,
    }


def build_exclusion_snapshot(
    *,
    signal_path: Path = SIGNALS,
    archive_root: Path = ARCHIVED_CONTEXT_ROOT,
    prior_selection_paths: Sequence[Path] = PRIOR_SELECTIONS,
    inspected_evidence: tuple[set[str], dict[str, Any]] | None = None,
) -> dict[str, Any]:
    signal_dates, signal_records = _signal_exclusions(signal_path)
    archive_dates, archive_artifacts = _archived_context_exclusions(archive_root)
    evidence_dates, evidence_artifacts = (
        inspected_evidence
        if inspected_evidence is not None
        else _registered_inspected_exclusions()
    )
    selection_dates: set[str] = set()
    selection_artifacts = []
    for path in prior_selection_paths:
        value = load_selection(path)
        selection_dates.update(value["selected_dates"])
        selection_artifacts.append(
            {
                "path": _repo_path(path),
                "sha256": _sha256_file(path),
                "dates": len(value["selected_dates"]),
            }
        )
    excluded = sorted(signal_dates | archive_dates | selection_dates | evidence_dates)
    return {
        "schema_version": 1,
        "signal_ledger": {
            "path": _repo_path(signal_path),
            "sha256": _sha256_file(signal_path),
            "records": signal_records,
            "dates": len(signal_dates),
        },
        "archived_contexts": {
            "root": _repo_path(archive_root),
            "files": len(archive_artifacts),
            "dates": len(archive_dates),
            "artifact_set_sha256": _sha256_json(archive_artifacts),
            "artifacts": archive_artifacts,
        },
        "prior_selections": selection_artifacts,
        "registered_inspected_evidence": evidence_artifacts,
        "source_date_counts": {
            "signal_ledger": len(signal_dates),
            "archived_contexts": len(archive_dates),
            "prior_scanner_selections": len(selection_dates),
            "registered_inspected_evidence": len(evidence_dates),
        },
        "excluded_dates": excluded,
        "excluded_dates_sha256": _sha256_json(excluded),
        "target_outcomes_observed_or_derived": False,
    }


def build_selection(
    *,
    calendar_dates: Sequence[str],
    exclusion_snapshot: Mapping[str, Any],
    seed: int = SEED,
    target_count: int = TARGET_COUNT,
) -> tuple[dict[str, Any], list[str]]:
    calendar = [_normalize_date(value) for value in calendar_dates]
    if calendar != sorted(set(calendar)):
        raise DevelopmentTrancheError("calendar dates must be unique and chronological")
    excluded = set(exclusion_snapshot.get("excluded_dates", []))
    eligible = [
        value
        for index, value in enumerate(calendar)
        if ELIGIBLE_START <= value <= ELIGIBLE_END
        and value not in excluded
        and index >= PRIOR_SESSIONS
    ]
    if len(eligible) < target_count:
        raise DevelopmentTrancheError(
            f"eligible disjoint pool has {len(eligible)} dates, needs {target_count}"
        )
    selected = random.Random(seed).sample(eligible, target_count)
    if set(selected) & excluded:
        raise DevelopmentTrancheError("selected tranche overlaps excluded dates")
    required = required_sessions(selected, calendar, prior_sessions=PRIOR_SESSIONS)
    value = {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "seed": seed,
        "selection_algorithm": (
            f"random.Random(seed).sample(eligible_dates, {target_count})"
        ),
        "eligible_start": ELIGIBLE_START,
        "eligible_end": ELIGIBLE_END,
        "eligible_date_count": len(eligible),
        "eligible_dates_sha256": _sha256_json(eligible),
        "excluded_date_count": len(excluded),
        "excluded_dates_sha256": exclusion_snapshot["excluded_dates_sha256"],
        "selected_dates": selected,
        "selected_dates_sha256": _sha256_json(selected),
        "required_prior_sessions": PRIOR_SESSIONS,
        "required_session_count": len(required),
        "required_sessions_sha256": _sha256_json(required),
        "substitution_allowed": False,
        "target_outcomes_observed_or_derived": False,
    }
    return value, required


def _directory_bytes(path: Path) -> int:
    if not path.exists():
        return 0
    return sum(item.stat().st_size for item in path.rglob("*") if item.is_file())


def _capacity_projection(
    *, required_session_count: int, store: HistoricalDayStore
) -> dict[str, Any]:
    prior = _read_json(PRIOR_SCANNER_STATUS)
    collection = prior.get("collection", {})
    if not (
        prior.get("status") == "READY"
        and collection.get("sessions_ready") == 133
        and collection.get("provider_requests") == 2808
        and collection.get("canonical_day_merges") == 103453
    ):
        raise DevelopmentTrancheError("prior scanner capacity evidence differs")
    prior_run_bytes = _directory_bytes(PRIOR_RUN_ROOT)
    prior_derived_bytes = _directory_bytes(PRIOR_DERIVED_ROOT)
    prior_bytes = prior_run_bytes + prior_derived_bytes
    projected_bytes = math.ceil(
        prior_bytes * required_session_count / int(collection["sessions_ready"]) * 2
    )
    projected_requests = math.ceil(
        int(collection["provider_requests"]) / 20 * required_session_count
    )
    projected_merges = math.ceil(
        int(collection["canonical_day_merges"]) / 20 * required_session_count
    )
    free = shutil.disk_usage(store.root).free
    headroom = free - store.min_free_bytes
    if projected_bytes > headroom:
        raise DevelopmentTrancheError(
            "projected acquisition would breach historical store reserve"
        )
    return {
        "basis_dataset_id": prior["dataset_id"],
        "basis_status_sha256": _sha256_file(PRIOR_SCANNER_STATUS),
        "basis_sessions": collection["sessions_ready"],
        "basis_provider_requests": collection["provider_requests"],
        "basis_canonical_merges": collection["canonical_day_merges"],
        "basis_private_run_bytes": prior_run_bytes,
        "basis_private_derived_bytes": prior_derived_bytes,
        "safety_factor": 2,
        "projected_provider_request_upper_bound": projected_requests,
        "projected_canonical_merge_upper_bound": projected_merges,
        "projected_private_bytes": projected_bytes,
        "free_bytes_at_freeze": free,
        "reserve_bytes": store.min_free_bytes,
        "capacity_ready": True,
    }


def freeze_inputs(
    *, env_path: Path, selection_path: Path, output_root: Path
) -> tuple[Path, dict[str, Any]]:
    source_semantics = _read_json(SOURCE_SEMANTICS_RESULT)
    if not (
        source_semantics.get("status") == "READY"
        and source_semantics.get("inspected") is True
        and source_semantics.get("combined_exact_deduplicated_verified_positive_pairs")
        == 3
        and source_semantics.get("capacity_gate_passed") is False
        and source_semantics.get("target_outcomes_observed_or_derived") is False
    ):
        raise DevelopmentTrancheError("source-recovery exit evidence differs")
    calendar = _calendar_dates()
    calendar_source = _read_json(CALENDAR_SOURCE)
    if not (
        calendar_source.get("calendar_sha256") == _sha256_file(CALENDAR)
        and calendar_source.get("sessions") == len(calendar)
        and calendar_source.get("provider") == "Alpaca Market Calendar API"
    ):
        raise DevelopmentTrancheError("calendar source attestation differs")
    store = HistoricalDayStore.from_env(env_path)
    if shutil.disk_usage(store.root).free < store.min_free_bytes:
        raise DevelopmentTrancheError("historical store reserve is not ready")
    exclusions = build_exclusion_snapshot()
    selection, required = build_selection(
        calendar_dates=calendar, exclusion_snapshot=exclusions
    )
    _write_json(selection_path, selection)
    private_path = _private_path(store.root)
    _write_gzip(private_path, exclusions)
    capacity = _capacity_projection(required_session_count=len(required), store=store)
    contract = {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "registered_at": datetime.now(timezone.utc).isoformat(),
        "requested_dates": sorted(selection["selected_dates"]),
        "dataset_payload": {
            "lane": "development",
            "claim_scope": "DEVELOPMENT_ONLY",
            "status": "COLLECTING",
            "evidence_paths": [
                _repo_path(SOURCE_SEMANTICS_RESULT),
                _repo_path(CALENDAR),
                _repo_path(CALENDAR_SOURCE),
                _repo_path(selection_path),
                "SCANNER_EXPANSION.md",
                "PRODUCTION_STRATEGY_VALIDATION.md",
            ],
            "inspected": False,
        },
        "selection_contract": {
            "implementation_sha256": _sha256_file(Path(__file__)),
            "source_semantics_result_sha256": _sha256_file(SOURCE_SEMANTICS_RESULT),
            "calendar_sha256": _sha256_file(CALENDAR),
            "calendar_source_sha256": _sha256_file(CALENDAR_SOURCE),
            "signals_sha256": exclusions["signal_ledger"]["sha256"],
            "archived_context_artifact_set_sha256": exclusions["archived_contexts"][
                "artifact_set_sha256"
            ],
            "inspected_evidence_artifact_set_sha256": exclusions[
                "registered_inspected_evidence"
            ]["artifact_set_sha256"],
            "dataset_registry_sha256": exclusions["registered_inspected_evidence"][
                "registry_sha256"
            ],
            "prior_selection_artifacts": exclusions["prior_selections"],
            "private_exclusion_sha256": _sha256_file(private_path),
            "public_selection_sha256": _sha256_file(selection_path),
            "exclusion_source_date_counts": exclusions["source_date_counts"],
            "excluded_date_count": selection["excluded_date_count"],
            "excluded_dates_sha256": selection["excluded_dates_sha256"],
            "eligible_start": ELIGIBLE_START,
            "eligible_end": ELIGIBLE_END,
            "eligible_date_count": selection["eligible_date_count"],
            "eligible_dates_sha256": selection["eligible_dates_sha256"],
            "seed": SEED,
            "selected_date_count": TARGET_COUNT,
            "selected_dates_sha256": selection["selected_dates_sha256"],
            "required_prior_sessions": PRIOR_SESSIONS,
            "required_session_count": selection["required_session_count"],
            "required_sessions_sha256": selection["required_sessions_sha256"],
            "disjoint_from_every_frozen_exclusion": True,
            "date_substitution_allowed": False,
            "target_outcomes_observed_or_derived": False,
        },
        "acquisition_contract": {
            "reference_identity_provider": "Massive dated common-stock snapshots",
            "full_universe_coarse_market_provider": (
                "local exact-contract cache, then whole-session Alpaca SIP"
            ),
            "selected_symbol_provider_priority": [
                "local canonical cache",
                "IBKR",
                "Massive",
                "Alpaca",
            ],
            "whole_provider_fidelity_per_symbol_session": True,
            "full_universe_detail_forbidden": True,
            "selected_symbols_only_minute_tape_quote_detail": True,
            "point_in_time_security_master_required_before_market_freeze": True,
            "split_actions_required_before_market_freeze": True,
            "provider_queries_required_before_market_freeze": True,
            "primary_catalyst_source_rules_required_before_market_freeze": True,
            "zero_substitution_policy_required_before_market_freeze": True,
            "scanner_manifest_must_be_committed_before_market_collection": True,
            "historical_store_reserve_enforced": True,
            "capacity_projection": capacity,
            "target_outcomes_observed_or_derived": False,
        },
    }
    return freeze_dataset_contract(contract, output_root)


def inspect(
    *, manifest_path: Path, env_path: Path, selection_path: Path
) -> dict[str, Any]:
    manifest = load_frozen_dataset_contract(manifest_path)
    if manifest.get("dataset_id") != DATASET_ID:
        raise DevelopmentTrancheError("unexpected development tranche dataset")
    contract = manifest["selection_contract"]
    store = HistoricalDayStore.from_env(env_path)
    private_path = _private_path(store.root)
    if _sha256_file(private_path) != contract.get("private_exclusion_sha256"):
        raise DevelopmentTrancheError("private exclusion snapshot changed")
    if _sha256_file(selection_path) != contract.get("public_selection_sha256"):
        raise DevelopmentTrancheError("public date selection changed")
    if _sha256_file(Path(__file__)) != contract.get("implementation_sha256"):
        raise DevelopmentTrancheError("selection implementation changed")
    exclusions = _read_gzip(private_path)
    selection = _read_json(selection_path)
    current_exclusions = build_exclusion_snapshot()
    frozen_evidence = exclusions["registered_inspected_evidence"]
    current_evidence = current_exclusions["registered_inspected_evidence"]
    comparisons = {
        "signal ledger": (
            exclusions["signal_ledger"]["sha256"],
            current_exclusions["signal_ledger"]["sha256"],
        ),
        "archived contexts": (
            exclusions["archived_contexts"]["artifact_set_sha256"],
            current_exclusions["archived_contexts"]["artifact_set_sha256"],
        ),
        "prior selections": (
            _sha256_json(exclusions["prior_selections"]),
            _sha256_json(current_exclusions["prior_selections"]),
        ),
        "inspected dataset entities": (
            frozen_evidence["inspected_entity_set_sha256"],
            current_evidence["inspected_entity_set_sha256"],
        ),
        "inspected evidence artifacts": (
            frozen_evidence["artifact_set_sha256"],
            current_evidence["artifact_set_sha256"],
        ),
        "excluded dates": (
            exclusions["excluded_dates_sha256"],
            current_exclusions["excluded_dates_sha256"],
        ),
    }
    for label, (frozen_hash, current_hash) in comparisons.items():
        if frozen_hash != current_hash:
            raise DevelopmentTrancheError(f"{label} changed after freeze")
    rebuilt, required = build_selection(
        calendar_dates=_calendar_dates(),
        exclusion_snapshot=exclusions,
        seed=int(selection["seed"]),
        target_count=TARGET_COUNT,
    )
    if rebuilt != selection:
        raise DevelopmentTrancheError("date selection does not rebuild")
    if set(selection["selected_dates"]) & set(exclusions["excluded_dates"]):
        raise DevelopmentTrancheError("frozen selection is not disjoint")
    return {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "status": "FROZEN_READY",
        "selected_dates": len(selection["selected_dates"]),
        "eligible_dates": selection["eligible_date_count"],
        "excluded_dates": selection["excluded_date_count"],
        "required_sessions": len(required),
        "disjoint": True,
        "selection_rebuilt": True,
        "date_substitution_allowed": False,
        "next_action": (
            "Commit this selection manifest before dated reference collection; "
            "then build and freeze the point-in-time master and market manifest."
        ),
        "target_outcomes_observed_or_derived": False,
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("freeze", "inspect"))
    parser.add_argument("--manifest", type=Path)
    parser.add_argument("--env-file", type=Path, default=PROJECT_ROOT / ".env")
    parser.add_argument("--selection", type=Path, default=DEFAULT_SELECTION)
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    return parser


def main() -> int:
    args = _parser().parse_args()
    try:
        if args.command == "freeze":
            path, manifest = freeze_inputs(
                env_path=args.env_file,
                selection_path=args.selection,
                output_root=args.output_root,
            )
            output = {"manifest": _repo_path(path), **manifest}
        elif args.manifest is None:
            raise DevelopmentTrancheError("--manifest is required")
        else:
            output = inspect(
                manifest_path=args.manifest,
                env_path=args.env_file,
                selection_path=args.selection,
            )
    except (
        DevelopmentTrancheError,
        LearningDataError,
        OSError,
        ValueError,
    ) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}), file=sys.stderr)
        return 2
    print(json.dumps(output, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

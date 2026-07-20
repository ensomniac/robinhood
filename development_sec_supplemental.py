"""Freeze and inspect exact target-window SEC supplemental submissions requests."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import io
import json
import os
import shutil
import sys
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import development_sec_sources as source_contract
import development_sec_submissions as submissions
from historical_store import HistoricalStoreConfig, HistoricalStoreError
from learning_data import (
    LearningDataError,
    freeze_dataset_contract,
    load_frozen_dataset_contract,
)


PROJECT_ROOT = Path(__file__).resolve().parent
DATASET_ID = "dataset-development-sec-supplemental-2026-07-19-v2"
SOURCE_MANIFEST = submissions.MANIFEST
SOURCE_STATUS = submissions.DEFAULT_PUBLIC_STATUS
SOURCE_INSPECTION = submissions.DEFAULT_PUBLIC_INSPECTION
DEFAULT_OUTPUT_ROOT = (
    PROJECT_ROOT
    / "historical_batches/development_tranche_v2/sec_supplemental_manifests"
)
DEFAULT_PUBLIC_STATUS = (
    PROJECT_ROOT
    / "historical_batches/development_tranche_v2/sec-supplemental-contract-status.json"
)
PRIVATE_NAMESPACE = "_derived/development_sec_sources"
PRIVATE_CONTRACT_FILE = "supplemental-request-map.json.gz"
TARGET_RESPONSE_NAMESPACE = "responses/supplemental"


class DevelopmentSecSupplementalError(RuntimeError):
    """The supplemental request graph is unsafe, incomplete, or drifted."""


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
        raise DevelopmentSecSupplementalError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise DevelopmentSecSupplementalError(f"{path} must contain an object")
    return value


def _read_gzip_object(path: Path) -> dict[str, Any]:
    try:
        with gzip.open(path, "rt", encoding="utf-8") as source:
            value = json.load(source)
    except (OSError, json.JSONDecodeError) as exc:
        raise DevelopmentSecSupplementalError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise DevelopmentSecSupplementalError(f"{path} must contain an object")
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
        raise DevelopmentSecSupplementalError(
            f"public path must be repository relative: {path}"
        ) from exc


def _private_contract_path(store_root: Path) -> Path:
    return (
        store_root
        / PRIVATE_NAMESPACE
        / source_contract.DATASET_ID
        / "supplemental_contracts"
        / DATASET_ID
        / PRIVATE_CONTRACT_FILE
    )


def _target_response_root(store_root: Path) -> Path:
    return (
        store_root
        / PRIVATE_NAMESPACE
        / source_contract.DATASET_ID
        / TARGET_RESPONSE_NAMESPACE
    )


def _target_response_count(store_root: Path) -> int:
    root = _target_response_root(store_root)
    return sum(1 for path in root.rglob("*") if path.is_file()) if root.exists() else 0


def _parse_range(value: Any) -> date | None:
    if not isinstance(value, str):
        return None
    try:
        return date.fromisoformat(value)
    except ValueError:
        return None


def _select_requests(
    *,
    descriptors: Sequence[Mapping[str, Any]],
    pairs: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    windows: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for pair in pairs:
        cik = pair.get("cik")
        if not cik:
            continue
        start = datetime.fromisoformat(str(pair["window_start_et"])).date()
        end = date.fromisoformat(str(pair["date"]))
        if end < start:
            raise DevelopmentSecSupplementalError("pair source window is reversed")
        windows[str(cik)].append(
            {
                "date": pair["date"],
                "instrument_id": pair["instrument_id"],
                "primary_exchange": pair["primary_exchange"],
                "rank": pair["rank"],
                "symbol": pair["symbol"],
                "start": start.isoformat(),
                "end": end.isoformat(),
            }
        )
    decisions: list[dict[str, Any]] = []
    selected: list[dict[str, Any]] = []
    counts: Counter[str] = Counter()
    seen_urls: set[str] = set()
    for descriptor in descriptors:
        cik = str(descriptor.get("cik") or "")
        url = str(descriptor.get("url") or "")
        if not cik or not url or url in seen_urls:
            raise DevelopmentSecSupplementalError(
                "supplemental descriptor identity is missing or duplicated"
            )
        seen_urls.add(url)
        relevant_windows = windows.get(cik, [])
        if not relevant_windows:
            raise DevelopmentSecSupplementalError(
                "supplemental descriptor has no frozen pair window"
            )
        filing_from = _parse_range(descriptor.get("filing_from"))
        filing_to = _parse_range(descriptor.get("filing_to"))
        if filing_from is None or filing_to is None or filing_to < filing_from:
            disposition = "SELECTED_CONSERVATIVE_INVALID_RANGE"
            matches = list(relevant_windows)
        else:
            matches = [
                window
                for window in relevant_windows
                if filing_from <= date.fromisoformat(window["end"])
                and date.fromisoformat(window["start"]) <= filing_to
            ]
            disposition = (
                "SELECTED_WINDOW_OVERLAP" if matches else "EXCLUDED_OUTSIDE_WINDOWS"
            )
        decision = {
            "descriptor": dict(descriptor),
            "disposition": disposition,
            "matching_windows": matches,
        }
        decisions.append(decision)
        counts[disposition] += 1
        if disposition.startswith("SELECTED_"):
            selected.append(
                {
                    **dict(descriptor),
                    "matching_windows": matches,
                    "selection_disposition": disposition,
                    "target_response_relative_path": (
                        "supplemental/" + hashlib.sha256(url.encode()).hexdigest() + ".json"
                    ),
                }
            )
    selected.sort(key=lambda row: row["url"])
    decisions.sort(key=lambda row: row["descriptor"]["url"])
    return {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "descriptor_count": len(descriptors),
        "selected_request_count": len(selected),
        "excluded_request_count": len(descriptors) - len(selected),
        "disposition_counts": dict(sorted(counts.items())),
        "decisions": decisions,
        "selected_requests": selected,
        "selected_request_graph_sha256": _sha256_json(selected),
        "decision_graph_sha256": _sha256_json(decisions),
    }


def _load_source_state(
    *, manifest_path: Path, env_path: Path
) -> tuple[dict[str, Any], HistoricalStoreConfig, dict[str, Any], dict[str, Any]]:
    submissions._published_source(Path(submissions.__file__))
    submissions._published_source(manifest_path)
    manifest, config, private, _publication = submissions._load_contract(
        manifest_path=manifest_path,
        env_path=env_path,
        require_published=False,
    )
    rebuilt = submissions._build_collection_index(
        manifest=manifest,
        private=private,
        store_root=config.root,
    )
    stored = _read_gzip_object(submissions._collection_index_path(config.root))
    if rebuilt != stored:
        raise DevelopmentSecSupplementalError("submissions private index drifted")
    recorded_public = _read_object(SOURCE_STATUS)
    if recorded_public.get("collector_sha256") != _sha256_file(
        Path(submissions.__file__)
    ):
        raise DevelopmentSecSupplementalError("submissions collector drifted")
    publication = {
        "collector": {"commit": recorded_public.get("collector_commit")}
    }
    public = submissions._public_status(index=rebuilt, publication=publication)
    if recorded_public != public:
        raise DevelopmentSecSupplementalError("submissions public status drifted")
    inspection = _read_object(SOURCE_INSPECTION)
    if (
        inspection.get("status") != "SUBMISSIONS_INSPECTED"
        or inspection.get("valid") is not True
        or inspection.get("private_collection_content_sha256")
        != _sha256_json(rebuilt)
        or rebuilt["counts"].get("pending_requests") != 0
    ):
        raise DevelopmentSecSupplementalError("submissions inspection is incomplete")
    return manifest, config, private, rebuilt


def _implementation_contract() -> dict[str, Any]:
    paths = {
        "supplemental_contract": Path(__file__),
        "submissions_collector": Path(submissions.__file__),
        "sec_source_contract": Path(source_contract.__file__),
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
    manifest_path: Path,
    env_path: Path,
    require_published_implementation: bool,
) -> tuple[dict[str, Any], HistoricalStoreConfig, dict[str, Any]]:
    if require_published_implementation:
        submissions._published_source(Path(__file__))
    source_manifest, config, private, collection = _load_source_state(
        manifest_path=manifest_path, env_path=env_path
    )
    selection = _select_requests(
        descriptors=collection["supplemental_submission_requests"],
        pairs=private["pairs"],
    )
    response_count = _target_response_count(config.root)
    if response_count:
        raise DevelopmentSecSupplementalError(
            "target supplemental responses exist before contract freeze"
        )
    request_contract = source_manifest["request_contract"]
    stable = {
        "lineage_contract": {
            "sec_source_manifest": {
                "path": _repo_path(manifest_path),
                "sha256": _sha256_file(manifest_path),
                "manifest_sha256": source_manifest["manifest_sha256"],
            },
            "submissions_status": {
                "path": _repo_path(SOURCE_STATUS),
                "sha256": _sha256_file(SOURCE_STATUS),
            },
            "submissions_inspection": {
                "path": _repo_path(SOURCE_INSPECTION),
                "sha256": _sha256_file(SOURCE_INSPECTION),
            },
            "private_collection_content_sha256": _sha256_json(collection),
            "source_descriptor_graph_sha256": collection[
                "supplemental_request_graph_sha256"
            ],
            "strategy": source_manifest["lineage_contract"]["strategy"],
        },
        "selection_contract": {
            "descriptor_count": selection["descriptor_count"],
            "selected_request_count": selection["selected_request_count"],
            "excluded_request_count": selection["excluded_request_count"],
            "disposition_counts": selection["disposition_counts"],
            "selected_request_graph_sha256": selection[
                "selected_request_graph_sha256"
            ],
            "decision_graph_sha256": selection["decision_graph_sha256"],
            "private_contract_content_sha256": _sha256_json(selection),
            "private_contract_path": (
                "LOCAL_HISTORICAL_DATA_ROOT/"
                f"{PRIVATE_NAMESPACE}/{source_contract.DATASET_ID}/"
                f"supplemental_contracts/{DATASET_ID}/{PRIVATE_CONTRACT_FILE}"
            ),
            "symbols_ciks_filenames_urls_and_windows_public": False,
            "invalid_or_missing_descriptor_range_policy": (
                "include conservatively; never treat as outside the target window"
            ),
            "outside_window_descriptors_requested": False,
        },
        "request_contract": {
            "stage": "SEC_SUPPLEMENTAL_SUBMISSIONS_ONLY",
            "provider": "SEC_EDGAR",
            "provider_operated_endpoints_only": True,
            "request_count": selection["selected_request_count"],
            "private_request_graph_sha256": selection[
                "selected_request_graph_sha256"
            ],
            "user_agent_sha256": request_contract["user_agent_sha256"],
            "workers": request_contract["workers"],
            "global_minimum_spacing_seconds": request_contract[
                "global_minimum_spacing_seconds"
            ],
            "timeout_seconds": request_contract["timeout_seconds"],
            "maximum_attempts": request_contract["maximum_attempts"],
            "retry_backoff_seconds": request_contract["retry_backoff_seconds"],
            "shared_cache_first": True,
            "per_request_terminal_result_required": True,
            "one_failure_cannot_abort_unrelated_requests": True,
            "substitution_allowed": False,
            "primary_document_access_allowed": False,
        },
        "outcome_lock": {
            "primary_documents_requested": False,
            "target_outcomes_observed_or_derived": False,
            "post_entry_data_access_allowed": False,
            "substitutions_allowed": False,
            "separate_document_manifest_required": True,
        },
        "pre_freeze_target_response_artifact_count": response_count,
        "implementation_contract": _implementation_contract(),
    }
    return stable, config, selection


def _verify_or_write_private(
    *, config: HistoricalStoreConfig, selection: Mapping[str, Any], write: bool
) -> None:
    path = _private_contract_path(config.root)
    expected = _sha256_json(selection)
    if path.exists():
        if _sha256_json(_read_gzip_object(path)) != expected:
            raise DevelopmentSecSupplementalError(
                "private supplemental request graph drifted"
            )
    elif write:
        _write_gzip_json(path, selection)
    else:
        raise DevelopmentSecSupplementalError(
            "private supplemental request graph is missing"
        )


def freeze_contract(
    *,
    manifest_path: Path = SOURCE_MANIFEST,
    env_path: Path = PROJECT_ROOT / ".env",
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    require_published_implementation: bool = True,
) -> tuple[Path, dict[str, Any]]:
    stable, config, selection = _stable_contract(
        manifest_path=manifest_path,
        env_path=env_path,
        require_published_implementation=require_published_implementation,
    )
    _verify_or_write_private(config=config, selection=selection, write=True)
    matches = sorted(output_root.glob(f"{DATASET_ID}-*.json"))
    if len(matches) > 1:
        raise DevelopmentSecSupplementalError(
            "supplemental contract has multiple manifests"
        )
    if matches:
        existing = load_frozen_dataset_contract(matches[0])
        if any(existing.get(key) != value for key, value in stable.items()):
            raise DevelopmentSecSupplementalError(
                "existing supplemental contract drifted"
            )
        return matches[0], existing
    usage = shutil.disk_usage(config.root)
    if usage.free < config.min_free_bytes:
        raise DevelopmentSecSupplementalError("historical-store reserve is unavailable")
    contract = {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "registered_at": datetime.now(UTC).isoformat(),
        "requested_dates": load_frozen_dataset_contract(manifest_path)[
            "requested_dates"
        ],
        "dataset_payload": {
            "lane": "development",
            "claim_scope": "DEVELOPMENT_ONLY",
            "status": "COLLECTING",
            "evidence_paths": [
                "DEVELOPMENT_SEC_SOURCES.md",
                _repo_path(manifest_path),
                _repo_path(SOURCE_STATUS),
                _repo_path(SOURCE_INSPECTION),
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
            "minimum_required_reserve_bytes": source_contract.MINIMUM_RESERVE_BYTES,
            "capacity_ready": True,
            "historical_deletion_allowed": False,
        },
    }
    return freeze_dataset_contract(contract, output_root)


def inspect_contract(
    *,
    manifest_path: Path,
    source_manifest_path: Path = SOURCE_MANIFEST,
    env_path: Path = PROJECT_ROOT / ".env",
    status_path: Path = DEFAULT_PUBLIC_STATUS,
    require_published_implementation: bool = True,
) -> dict[str, Any]:
    manifest = load_frozen_dataset_contract(manifest_path)
    if manifest.get("dataset_id") != DATASET_ID:
        raise DevelopmentSecSupplementalError("unexpected supplemental dataset")
    stable, config, selection = _stable_contract(
        manifest_path=source_manifest_path,
        env_path=env_path,
        require_published_implementation=require_published_implementation,
    )
    for key, value in stable.items():
        if manifest.get(key) != value:
            raise DevelopmentSecSupplementalError(
                f"supplemental contract {key} drifted"
            )
    _verify_or_write_private(config=config, selection=selection, write=False)
    capacity = manifest.get("capacity_contract")
    if not isinstance(capacity, Mapping) or any(
        (
            capacity.get("capacity_ready") is not True,
            capacity.get("historical_store_outside_repository") is not True,
            capacity.get("historical_deletion_allowed") is not False,
            int(capacity.get("reserve_bytes", -1)) != config.min_free_bytes,
        )
    ):
        raise DevelopmentSecSupplementalError("supplemental capacity contract is invalid")
    public = stable["selection_contract"]
    status = {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "status": "FROZEN_READY",
        "manifest_sha256": manifest["manifest_sha256"],
        "descriptor_count": public["descriptor_count"],
        "selected_request_count": public["selected_request_count"],
        "excluded_request_count": public["excluded_request_count"],
        "disposition_counts": public["disposition_counts"],
        "selected_request_graph_sha256": public[
            "selected_request_graph_sha256"
        ],
        "decision_graph_sha256": public["decision_graph_sha256"],
        "private_contract_content_sha256": public[
            "private_contract_content_sha256"
        ],
        "pre_freeze_target_response_artifact_count": 0,
        "supplemental_files_requested": False,
        "primary_documents_requested": False,
        "target_outcomes_observed_or_derived": False,
        "substitutions_allowed": False,
        "valid": True,
    }
    _write_json(status_path, status)
    return status


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env", type=Path, default=PROJECT_ROOT / ".env")
    parser.add_argument("--source-manifest", type=Path, default=SOURCE_MANIFEST)
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
        if args.command == "freeze":
            path, manifest = freeze_contract(
                manifest_path=args.source_manifest,
                env_path=args.env,
                output_root=args.output_root,
            )
            value = {
                "dataset_id": DATASET_ID,
                "manifest_sha256": manifest["manifest_sha256"],
                "path": str(path),
                "descriptor_count": manifest["selection_contract"][
                    "descriptor_count"
                ],
                "selected_request_count": manifest["selection_contract"][
                    "selected_request_count"
                ],
            }
        else:
            value = inspect_contract(
                manifest_path=args.manifest,
                source_manifest_path=args.source_manifest,
                env_path=args.env,
                status_path=args.status,
            )
        print(json.dumps(value, indent=2, sort_keys=True))
        return 0
    except (
        DevelopmentSecSupplementalError,
        HistoricalStoreError,
        LearningDataError,
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

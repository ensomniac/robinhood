"""Run the frozen selected-candidate join on the 100-date scanner expansion.

The original join implementation is evidence-bound to the completed 389-pair
development corpus.  This adapter does not copy or edit that implementation.
It freezes a new manifest and private namespace, verifies both source selection
and dependency hashes, and then delegates collection stages to the unchanged
collector.  Exact identities and raw rows remain outside Git.
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
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import selected_candidate_join as legacy_join
from historical_providers import HistoricalProviderError
from historical_store import HistoricalStoreConfig, HistoricalStoreError
from learning_data import (
    LearningDataError,
    freeze_dataset_contract,
    load_frozen_dataset_contract,
)


PROJECT_ROOT = Path(__file__).resolve().parent
DATASET_ID = "dataset-selected-candidate-join-2026-07-19-expansion-v1"
SOURCE_DATASET_ID = "dataset-selected-candidate-contract-2026-07-19-expansion-v1"
SOURCE_MANIFEST = (
    PROJECT_ROOT
    / "historical_batches"
    / "scanner_expansion_selected_pairs"
    / "manifests"
    / (
        "dataset-selected-candidate-contract-2026-07-19-expansion-v1-"
        "369efda1967ad83096e3c26672e349b44bb8d0afd694f391490042a9e22c140b.json"
    )
)
DEFAULT_OUTPUT_ROOT = (
    PROJECT_ROOT
    / "historical_batches"
    / "selected_candidate_join_expansion"
    / "manifests"
)
DEFAULT_PUBLIC_STATUS = (
    PROJECT_ROOT
    / "historical_batches"
    / "selected_candidate_join_expansion"
    / "collection-status.json"
)
DEFAULT_PUBLIC_RESULT = (
    PROJECT_ROOT
    / "research_results"
    / "2026-07-19-selected-candidate-join-expansion.json"
)


class ExpansionJoinError(RuntimeError):
    """The expansion join contract or adapter is invalid."""

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


def _read_gzip_object(path: Path) -> dict[str, Any]:
    try:
        with gzip.open(path, "rt", encoding="utf-8") as source:
            value = json.load(source)
    except (OSError, json.JSONDecodeError) as exc:
        raise ExpansionJoinError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise ExpansionJoinError(f"{path} must contain an object")
    return value


def _gzip_bytes(value: Any) -> bytes:
    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode="wb", compresslevel=6, mtime=0) as target:
        target.write(json.dumps(value, indent=2, sort_keys=True).encode())
        target.write(b"\n")
    return buffer.getvalue()


def _write_private(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_bytes(_gzip_bytes(value))
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _source_private_path(store_root: Path, dataset_id: str) -> Path:
    return (
        store_root
        / "_derived"
        / "scanner_selected_pairs"
        / dataset_id
        / "selected-pairs.json.gz"
    )


def _destination_private_path(store_root: Path, dataset_id: str) -> Path:
    return (
        store_root
        / "_derived"
        / "selected_candidate_join"
        / dataset_id
        / "selected-pairs.json.gz"
    )


def _public_selection(
    source_manifest: Mapping[str, Any], private: Mapping[str, Any]
) -> dict[str, Any]:
    source = source_manifest.get("selection_contract")
    if not isinstance(source, Mapping):
        raise ExpansionJoinError("source selection contract is missing")
    requested = [str(item) for item in source_manifest.get("requested_dates", [])]
    if requested != [str(item) for item in source.get("requested_dates", [])]:
        raise ExpansionJoinError("source requested-date contract differs")
    if int(source.get("selected_pair_count", -1)) != int(
        private.get("selected_pair_count", -2)
    ):
        raise ExpansionJoinError("source selected-pair count differs")
    daily = source.get("daily_shortlists")
    if not isinstance(daily, list) or [str(item.get("date")) for item in daily] != requested:
        raise ExpansionJoinError("source daily shortlist contract differs")
    return {
        "source_selection_dataset_id": source_manifest["dataset_id"],
        "source_selection_manifest_sha256": source_manifest["manifest_sha256"],
        "source_scanner_dataset_id": source.get("source_dataset_id"),
        "source_scanner_manifest_sha256": source.get("source_manifest_sha256"),
        "source_scanner_summary_sha256": source.get("source_summary_sha256"),
        "source_scanner_detail_sha256": source.get("source_detail_sha256"),
        "requested_dates": requested,
        "selected_pair_count": int(private["selected_pair_count"]),
        "daily_shortlists": daily,
        "private_selection_content_sha256": _sha256_json(private),
    }


def _matching_existing_manifest(
    output_root: Path, dataset_id: str, expected: Mapping[str, Any]
) -> tuple[Path, dict[str, Any]] | None:
    matches = sorted(output_root.glob(f"{dataset_id}-*.json"))
    if len(matches) > 1:
        raise ExpansionJoinError("expansion join has multiple manifests")
    if not matches:
        return None
    manifest = load_frozen_dataset_contract(matches[0])
    for key, value in expected.items():
        if key == "capacity_contract":
            observed = manifest.get(key)
            if not isinstance(observed, Mapping) or any(
                observed.get(field) != value.get(field)
                for field in ("minimum_free_bytes", "pilot_required_before_bulk_collection")
            ):
                raise ExpansionJoinError(f"existing expansion join {key} differs")
            continue
        if manifest.get(key) != value:
            raise ExpansionJoinError(f"existing expansion join {key} differs")
    return matches[0], manifest


def freeze_expansion_join(
    *,
    source_manifest_path: Path,
    env_path: Path,
    output_root: Path,
    dataset_id: str = DATASET_ID,
) -> tuple[Path, dict[str, Any]]:
    if not dataset_id.startswith("dataset-selected-candidate-join-"):
        raise ExpansionJoinError("expansion join dataset namespace is invalid")
    source_manifest = load_frozen_dataset_contract(source_manifest_path)
    source_id = str(source_manifest.get("dataset_id") or "")
    if not source_id.startswith("dataset-selected-candidate-contract-"):
        raise ExpansionJoinError("source is not a selected-candidate contract")
    store_root = HistoricalStoreConfig.from_env(env_path).root
    source_private = _read_gzip_object(_source_private_path(store_root, source_id))
    source_contract = source_manifest.get("selection_contract", {})
    if _sha256_json(source_private) != source_contract.get(
        "private_selection_content_sha256"
    ):
        raise ExpansionJoinError("source private selection differs from manifest")
    if source_private.get("dataset_id") != source_id:
        raise ExpansionJoinError("source private selection identity differs")
    private = {
        **source_private,
        "dataset_id": dataset_id,
        "source_selection_dataset_id": source_id,
        "source_selection_manifest_sha256": source_manifest["manifest_sha256"],
    }
    public = _public_selection(source_manifest, private)
    destination = _destination_private_path(store_root, dataset_id)
    if destination.exists():
        if _sha256_json(_read_gzip_object(destination)) != _sha256_json(private):
            raise ExpansionJoinError("expansion private selection changed")
    else:
        _write_private(destination, private)
    free_bytes = shutil.disk_usage(store_root).free
    if free_bytes < legacy_join.MINIMUM_FREE_BYTES:
        raise ExpansionJoinError(
            "historical store has less than the 10 GiB collection reserve",
            category="capacity",
        )
    collection = {
        "adapter_sha256": _sha256_file(Path(__file__)),
        "collector_path": "selected_candidate_join.py",
        "collector_sha256": _sha256_file(PROJECT_ROOT / "selected_candidate_join.py"),
        "provider": "Alpaca Market Data API",
        "feed": "sip",
        "adjustment": "raw",
        "symbol_mapping": (
            "asof=-; frozen point-in-time scanner identity remains authoritative"
        ),
        "candidate_bars": "1Min regular session 09:30-16:00 ET",
        "benchmarks": list(legacy_join.BENCHMARKS),
        "benchmark_bars": "1Min regular session 09:30-16:00 ET",
        "news_window": (
            f"{legacy_join.NEWS_LOOKBACK_DAYS} calendar days before target through "
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
        "target_outcomes_observed_or_derived": False,
        "alpha_or_confirmation_claim_allowed": False,
    }
    capacity = {
        "minimum_free_bytes": legacy_join.MINIMUM_FREE_BYTES,
        "observed_free_bytes_at_freeze": free_bytes,
        "pilot_required_before_bulk_collection": True,
    }
    expected = {
        "schema_version": 1,
        "dataset_id": dataset_id,
        "requested_dates": public["requested_dates"],
        "dataset_payload": {
            "lane": "development",
            "claim_scope": "DEVELOPMENT_ONLY",
            "status": "COLLECTING",
            "evidence_paths": [
                str(source_manifest_path.resolve().relative_to(PROJECT_ROOT)),
                "SCANNER_SELECTED_PAIRS.md",
                "STRATEGY_LEARNING_EXECUTION_PLAN.md",
            ],
            "inspected": False,
        },
        "selection_contract": public,
        "collection_contract": collection,
        "capacity_contract": capacity,
    }
    existing = _matching_existing_manifest(output_root, dataset_id, expected)
    if existing is not None:
        return existing
    contract = {
        **expected,
        "registered_at": datetime.now(timezone.utc).isoformat(),
    }
    return freeze_dataset_contract(contract, output_root)


def _activate_legacy(manifest_path: Path) -> dict[str, Any]:
    manifest = load_frozen_dataset_contract(manifest_path)
    dataset_id = str(manifest.get("dataset_id") or "")
    if not dataset_id.startswith("dataset-selected-candidate-join-"):
        raise ExpansionJoinError("join manifest dataset namespace is invalid")
    contract = manifest.get("collection_contract")
    if not isinstance(contract, Mapping):
        raise ExpansionJoinError("join collection contract is missing")
    if contract.get("adapter_sha256") != _sha256_file(Path(__file__)):
        raise ExpansionJoinError("expansion adapter no longer matches manifest")
    if contract.get("collector_sha256") != _sha256_file(
        PROJECT_ROOT / "selected_candidate_join.py"
    ):
        raise ExpansionJoinError("legacy collector no longer matches manifest")
    legacy_join.DATASET_ID = dataset_id
    return manifest


def _run_stage(
    stage: str, *, manifest_path: Path, env_path: Path
) -> dict[str, Any]:
    _activate_legacy(manifest_path)
    operations: dict[str, tuple[Callable[..., dict[str, Any]], str, Path]] = {
        "pilot": (legacy_join.pilot, "public_status_path", DEFAULT_PUBLIC_STATUS),
        "collect": (legacy_join.collect, "public_status_path", DEFAULT_PUBLIC_STATUS),
        "derive": (
            legacy_join.derive_triggers,
            "public_result_path",
            DEFAULT_PUBLIC_RESULT,
        ),
        "collect-tape": (
            legacy_join.collect_trigger_tape,
            "public_status_path",
            DEFAULT_PUBLIC_STATUS,
        ),
        "inspect": (
            legacy_join.inspect_join,
            "public_result_path",
            DEFAULT_PUBLIC_RESULT,
        ),
    }
    operation, output_name, output_path = operations[stage]
    return operation(
        manifest_path=manifest_path,
        env_path=env_path,
        **{output_name: output_path},
    )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env", type=Path, default=PROJECT_ROOT / ".env")
    subparsers = parser.add_subparsers(dest="command", required=True)
    freeze = subparsers.add_parser("freeze")
    freeze.add_argument("--source-manifest", type=Path, default=SOURCE_MANIFEST)
    freeze.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    for name in ("pilot", "collect", "derive", "collect-tape", "inspect"):
        command = subparsers.add_parser(name)
        command.add_argument("manifest", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        if args.command == "freeze":
            path, manifest = freeze_expansion_join(
                source_manifest_path=args.source_manifest,
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
        else:
            result = _run_stage(
                args.command,
                manifest_path=args.manifest,
                env_path=args.env,
            )
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    except (
        ExpansionJoinError,
        HistoricalProviderError,
        HistoricalStoreError,
        LearningDataError,
        legacy_join.SelectedCandidateJoinError,
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

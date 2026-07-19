"""Close catalyst-candidate and clean-trigger gaps on the scanner expansion.

This adapter freezes an exact point-in-time CIK map for the 1,987 expansion
pairs, validates the completed base join, and reuses the unchanged fidelity
collector in a new private namespace.  It does not read outcomes, classify a
strategy return, or change production rules.
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
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import selected_candidate_fidelity as legacy_fidelity
from historical_discovery import HistoricalDiscoveryError
from historical_providers import HistoricalProviderError
from historical_store import HistoricalStoreConfig, HistoricalStoreError
from learning_data import (
    LearningDataError,
    freeze_dataset_contract,
    load_frozen_dataset_contract,
)
from scanner_replay import _instrument_id
from selected_candidate_join import _load_private_selection


PROJECT_ROOT = Path(__file__).resolve().parent
DATASET_ID = "dataset-selected-candidate-fidelity-2026-07-19-expansion-v1"
SOURCE_DATASET_ID = "dataset-selected-candidate-join-2026-07-19-expansion-v1"
SOURCE_MANIFEST = (
    PROJECT_ROOT
    / "historical_batches"
    / "selected_candidate_join_expansion"
    / "manifests"
    / (
        "dataset-selected-candidate-join-2026-07-19-expansion-v1-"
        "15d8baefcd4bedb3cf473cad4b98638a0c101ae2a1bfc64552d80c72dede2f6b.json"
    )
)
SOURCE_RESULT = (
    PROJECT_ROOT
    / "research_results"
    / "2026-07-19-selected-candidate-join-expansion.json"
)
REFERENCE_ROOT = PROJECT_ROOT / "learning_runs" / "scanner_expansion" / "reference"
DEFAULT_OUTPUT_ROOT = (
    PROJECT_ROOT
    / "historical_batches"
    / "selected_candidate_fidelity_expansion"
    / "manifests"
)
DEFAULT_PUBLIC_STATUS = (
    PROJECT_ROOT
    / "historical_batches"
    / "selected_candidate_fidelity_expansion"
    / "collection-status.json"
)
DEFAULT_PUBLIC_RESULT = (
    PROJECT_ROOT
    / "research_results"
    / "2026-07-19-selected-candidate-fidelity-expansion.json"
)


class ExpansionFidelityError(RuntimeError):
    """The expansion fidelity contract or evidence is invalid."""

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
        raise ExpansionFidelityError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise ExpansionFidelityError(f"{path} must contain an object")
    return value


def _read_gzip(path: Path) -> Any:
    try:
        with gzip.open(path, "rt", encoding="utf-8") as source:
            return json.load(source)
    except (OSError, json.JSONDecodeError) as exc:
        raise ExpansionFidelityError(f"cannot read {path}: {exc}") from exc


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
        raise ExpansionFidelityError(f"path must be repository-relative: {path}") from exc


def _private_root(store_root: Path, dataset_id: str) -> Path:
    return store_root / "_derived" / "selected_candidate_fidelity" / dataset_id


def _selection_path(store_root: Path, dataset_id: str) -> Path:
    return _private_root(store_root, dataset_id) / "selection-and-cik-map.json.gz"


def _sec_path(store_root: Path, dataset_id: str) -> Path:
    return _private_root(store_root, dataset_id) / "primary-catalyst-index.json.gz"


def _trigger_path(store_root: Path, dataset_id: str) -> Path:
    return _private_root(store_root, dataset_id) / "clean-trigger-index.json.gz"


def _source_trigger_path(store_root: Path, source_dataset_id: str) -> Path:
    return (
        store_root
        / "_derived"
        / "selected_candidate_join"
        / source_dataset_id
        / "trigger-index.json.gz"
    )


def _point_in_time_cik_map(
    pairs: Sequence[Mapping[str, Any]], reference_root: Path
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    by_day: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for pair in pairs:
        by_day[str(pair["date"])].append(pair)
    mapped: list[dict[str, Any]] = []
    sources: list[dict[str, Any]] = []
    for day in sorted(by_day):
        path = reference_root / f"{day}.json.gz"
        rows = _read_gzip(path)
        if not isinstance(rows, list):
            raise ExpansionFidelityError(f"reference snapshot is malformed: {path}")
        exact: dict[tuple[str, str, str], Mapping[str, Any]] = {}
        duplicates: set[tuple[str, str, str]] = set()
        for row in rows:
            if not isinstance(row, Mapping):
                continue
            instrument_id, _basis = _instrument_id(row)
            key = (
                instrument_id,
                str(row.get("ticker") or "").strip().upper(),
                str(row.get("primary_exchange") or "UNKNOWN").strip().upper(),
            )
            if key in exact:
                duplicates.add(key)
            exact[key] = row
        for pair in by_day[day]:
            key = (
                str(pair["instrument_id"]),
                str(pair["symbol"]).strip().upper(),
                str(pair["primary_exchange"]).strip().upper(),
            )
            row = exact.get(key)
            if row is None or key in duplicates:
                raise ExpansionFidelityError(
                    f"cannot uniquely map frozen scanner identity on {day}"
                )
            cik = str(row.get("cik") or "").lstrip("0")
            if not cik:
                raise ExpansionFidelityError(f"point-in-time source lacks CIK on {day}")
            mapped.append(
                {
                    **dict(pair),
                    "cik": cik,
                    "issuer_name": str(row.get("name") or ""),
                    "cik_match_basis": "exact_instrument_ticker_exchange",
                    "cik_source": {
                        "snapshot_path": _repo_path(path),
                        "snapshot_sha256": _sha256_file(path),
                        "last_updated_utc": row.get("last_updated_utc"),
                    },
                }
            )
        sources.append(
            {"date": day, "row_count": len(rows), "sha256": _sha256_file(path)}
        )
    return mapped, sources


def _matching_manifest(
    output_root: Path, dataset_id: str, expected: Mapping[str, Any]
) -> tuple[Path, dict[str, Any]] | None:
    matches = sorted(output_root.glob(f"{dataset_id}-*.json"))
    if len(matches) > 1:
        raise ExpansionFidelityError("expansion fidelity has multiple manifests")
    if not matches:
        return None
    manifest = load_frozen_dataset_contract(matches[0])
    for key, value in expected.items():
        if key == "capacity_contract":
            observed = manifest.get(key)
            if not isinstance(observed, Mapping) or observed.get(
                "minimum_free_bytes"
            ) != value.get("minimum_free_bytes"):
                raise ExpansionFidelityError("existing capacity contract differs")
            continue
        if manifest.get(key) != value:
            raise ExpansionFidelityError(f"existing fidelity {key} differs")
    return matches[0], manifest


def freeze_expansion_fidelity(
    *,
    source_manifest_path: Path,
    source_result_path: Path,
    reference_root: Path,
    env_path: Path,
    output_root: Path,
    dataset_id: str = DATASET_ID,
) -> tuple[Path, dict[str, Any]]:
    source_manifest = load_frozen_dataset_contract(source_manifest_path)
    if source_manifest.get("dataset_id") != SOURCE_DATASET_ID:
        raise ExpansionFidelityError("unexpected expansion join source")
    source_result = _read_object(source_result_path)
    if (
        source_result.get("dataset_id") != SOURCE_DATASET_ID
        or source_result.get("manifest_sha256") != source_manifest["manifest_sha256"]
        or source_result.get("status") != "READY"
        or source_result.get("inspected") is not True
    ):
        raise ExpansionFidelityError("source expansion join is not inspected READY")
    store_root = HistoricalStoreConfig.from_env(env_path).root
    source_selection = _load_private_selection(source_manifest, store_root)
    pairs, references = _point_in_time_cik_map(
        source_selection["selected_pairs"], reference_root
    )
    source_trigger_path = _source_trigger_path(store_root, SOURCE_DATASET_ID)
    source_trigger = _read_gzip(source_trigger_path)
    if not isinstance(source_trigger, Mapping) or source_trigger.get(
        "manifest_sha256"
    ) != source_manifest["manifest_sha256"]:
        raise ExpansionFidelityError("source trigger index is not manifest-bound")
    trigger_count = sum(
        isinstance(row, Mapping) and row.get("status") == "CROSSING_MINUTE_IDENTIFIED"
        for row in source_trigger.get("records", [])
    )
    if trigger_count != int(source_result.get("expected", {}).get("trigger_tapes", -1)):
        raise ExpansionFidelityError("source trigger count differs from inspection")
    private = {
        "schema_version": 1,
        "dataset_id": dataset_id,
        "source_dataset_id": SOURCE_DATASET_ID,
        "source_manifest_sha256": source_manifest["manifest_sha256"],
        "selected_pair_count": len(pairs),
        "unique_cik_count": len({str(row["cik"]) for row in pairs}),
        "pairs": pairs,
    }
    private_path = _selection_path(store_root, dataset_id)
    if private_path.exists():
        if _sha256_json(_read_gzip(private_path)) != _sha256_json(private):
            raise ExpansionFidelityError("private expansion CIK map changed")
    else:
        _write_gzip(private_path, private)
    free_bytes = shutil.disk_usage(store_root).free
    if free_bytes < legacy_fidelity.MINIMUM_FREE_BYTES:
        raise ExpansionFidelityError("historical reserve is below 10 GiB")
    collection = {
        "adapter_sha256": _sha256_file(Path(__file__)),
        "collector_path": "selected_candidate_fidelity.py",
        "collector_sha256": _sha256_file(
            PROJECT_ROOT / "selected_candidate_fidelity.py"
        ),
        "identity_builder_path": "scanner_replay.py",
        "identity_builder_sha256": _sha256_file(PROJECT_ROOT / "scanner_replay.py"),
        "sec_source": "SEC EDGAR submissions JSON and primary filing documents",
        "sec_forms": sorted(legacy_fidelity.RELEVANT_FORMS),
        "sec_window": (
            f"{legacy_fidelity.SEC_LOOKBACK_DAYS} calendar days before target "
            "through target 09:35 ET"
        ),
        "cik_mapping": (
            "dated Massive snapshot exact instrument-id, ticker, and exchange match"
        ),
        "condition_source_url": legacy_fidelity.CONDITION_SOURCE_URL,
        "condition_rule_version": legacy_fidelity.RULE_VERSION,
        "continuous_cross_version": legacy_fidelity.CONTINUOUS_CROSS_VERSION,
        "trigger_contract": (
            "first price above the frozen opening high whose conditions both "
            "update a minute high and establish a continuous regular-sale cross"
        ),
        "raw_and_symbol_rows_public": False,
        "canonical_store_required": True,
        "source_corpus_already_inspected": True,
        "target_outcomes_observed_or_derived": False,
        "alpha_or_confirmation_claim_allowed": False,
    }
    expected = {
        "schema_version": 1,
        "dataset_id": dataset_id,
        "requested_dates": list(source_manifest["requested_dates"]),
        "dataset_payload": {
            "lane": "development",
            "claim_scope": "DEVELOPMENT_ONLY",
            "status": "COLLECTING",
            "evidence_paths": [
                _repo_path(source_manifest_path),
                _repo_path(source_result_path),
                "SCANNER_EXPANSION_JOIN.md",
                "STRATEGY_LEARNING_EXECUTION_PLAN.md",
            ],
            "inspected": False,
        },
        "selection_contract": {
            "source_dataset_id": SOURCE_DATASET_ID,
            "source_manifest_sha256": source_manifest["manifest_sha256"],
            "source_result_sha256": _sha256_file(source_result_path),
            "source_trigger_index_sha256": _sha256_file(source_trigger_path),
            "source_trigger_count": trigger_count,
            "selected_pair_count": len(pairs),
            "unique_cik_count": private["unique_cik_count"],
            "private_selection_content_sha256": _sha256_json(private),
            "reference_snapshots": references,
            "symbols_and_ciks_public": False,
        },
        "collection_contract": collection,
        "capacity_contract": {
            "minimum_free_bytes": legacy_fidelity.MINIMUM_FREE_BYTES,
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


def _activate_legacy(manifest_path: Path) -> dict[str, Any]:
    manifest = load_frozen_dataset_contract(manifest_path)
    dataset_id = str(manifest.get("dataset_id") or "")
    source_id = str(manifest.get("selection_contract", {}).get("source_dataset_id") or "")
    contract = manifest.get("collection_contract")
    if not isinstance(contract, Mapping):
        raise ExpansionFidelityError("fidelity collection contract is missing")
    if contract.get("adapter_sha256") != _sha256_file(Path(__file__)):
        raise ExpansionFidelityError("expansion fidelity adapter changed")
    if contract.get("collector_sha256") != _sha256_file(
        PROJECT_ROOT / "selected_candidate_fidelity.py"
    ):
        raise ExpansionFidelityError("legacy fidelity collector changed")
    legacy_fidelity.DATASET_ID = dataset_id
    legacy_fidelity.SOURCE_DATASET_ID = source_id
    legacy_fidelity._private_root = lambda root, dataset_id=dataset_id: _private_root(
        root, dataset_id
    )
    legacy_fidelity._private_selection_path = lambda root: _selection_path(
        root, dataset_id
    )
    legacy_fidelity._private_sec_path = lambda root: _sec_path(root, dataset_id)
    legacy_fidelity._private_trigger_path = lambda root: _trigger_path(root, dataset_id)
    return manifest


def inspect_expansion_fidelity(
    *, manifest_path: Path, env_path: Path, public_result_path: Path
) -> dict[str, Any]:
    manifest = _activate_legacy(manifest_path)
    store_root = HistoricalStoreConfig.from_env(env_path).root
    selection = _read_gzip(_selection_path(store_root, str(manifest["dataset_id"])))
    sec_path = _sec_path(store_root, str(manifest["dataset_id"]))
    trigger_path = _trigger_path(store_root, str(manifest["dataset_id"]))
    sec = _read_gzip(sec_path)
    triggers = _read_gzip(trigger_path)
    if (
        not isinstance(sec, Mapping)
        or not isinstance(triggers, Mapping)
        or sec.get("manifest_sha256") != manifest["manifest_sha256"]
        or triggers.get("manifest_sha256") != manifest["manifest_sha256"]
    ):
        raise ExpansionFidelityError("private fidelity evidence is not manifest-bound")
    sec_counts = dict(sec.get("counts", {}))
    trigger_counts = dict(triggers.get("counts", {}))
    expected_pairs = int(manifest["selection_contract"]["selected_pair_count"])
    expected_triggers = int(manifest["selection_contract"]["source_trigger_count"])
    complete = (
        int(selection.get("selected_pair_count", -1)) == expected_pairs
        and int(sec_counts.get("selected_pairs", 0)) == expected_pairs
        and int(trigger_counts.get("crossing_windows", 0)) == expected_triggers
        and not sec.get("errors")
        and not triggers.get("errors")
    )
    result = {
        "schema_version": 1,
        "dataset_id": manifest["dataset_id"],
        "manifest_sha256": manifest["manifest_sha256"],
        "status": "READY" if complete else "INCOMPLETE",
        "inspected": complete,
        "claim_scope": "DEVELOPMENT_ONLY",
        "source_dataset_id": manifest["selection_contract"]["source_dataset_id"],
        "source_corpus_already_inspected": True,
        "selected_pair_count": expected_pairs,
        "sec_counts": sec_counts,
        "clean_trigger_counts": trigger_counts,
        "clean_trigger_shift_summary": triggers.get("shift_summary"),
        "private_selection_content_sha256": manifest["selection_contract"][
            "private_selection_content_sha256"
        ],
        "private_sec_index_sha256": _sha256_file(sec_path),
        "private_trigger_index_sha256": _sha256_file(trigger_path),
        "findings": {
            "point_in_time_cik_coverage_complete": True,
            "primary_filing_presence_is_not_positive_verification": True,
            "clean_cross_is_narrower_than_bar_high_eligibility": True,
            "production_rule_change_earned": False,
        },
        "remaining_fidelity_gaps": [
            "primary filing candidates require source-grounded directional classification",
            "historical full-depth liquidity is unavailable from Alpaca top-of-book",
            "point-in-time tradability and halt state are not yet joined",
            "resistance and sector-relative-strength contracts remain unspecified",
        ],
        "target_outcomes_observed_or_derived": False,
        "claim_boundary": (
            "Pipeline-fidelity evidence only; not alpha, confirmation, promotion, "
            "or permission to invent a strategy variant."
        ),
        "symbols_ciks_filings_and_raw_rows_public": False,
    }
    _write_json(public_result_path, result)
    return result


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env", type=Path, default=PROJECT_ROOT / ".env")
    commands = parser.add_subparsers(dest="command", required=True)
    freeze = commands.add_parser("freeze")
    freeze.add_argument("--source-manifest", type=Path, default=SOURCE_MANIFEST)
    freeze.add_argument("--source-result", type=Path, default=SOURCE_RESULT)
    freeze.add_argument("--reference-root", type=Path, default=REFERENCE_ROOT)
    freeze.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    for name in ("collect-sec", "collect-triggers", "inspect"):
        command = commands.add_parser(name)
        command.add_argument("manifest", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "freeze":
            path, manifest = freeze_expansion_fidelity(
                source_manifest_path=args.source_manifest,
                source_result_path=args.source_result,
                reference_root=args.reference_root,
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
                "source_trigger_count": manifest["selection_contract"][
                    "source_trigger_count"
                ],
                "unique_cik_count": manifest["selection_contract"][
                    "unique_cik_count"
                ],
            }
        elif args.command == "inspect":
            result = inspect_expansion_fidelity(
                manifest_path=args.manifest,
                env_path=args.env,
                public_result_path=DEFAULT_PUBLIC_RESULT,
            )
        else:
            _activate_legacy(args.manifest)
            operation = (
                legacy_fidelity.collect_sec
                if args.command == "collect-sec"
                else legacy_fidelity.collect_clean_triggers
            )
            result = operation(
                manifest_path=args.manifest,
                env_path=args.env,
                public_status_path=DEFAULT_PUBLIC_STATUS,
            )
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    except (
        ExpansionFidelityError,
        HistoricalDiscoveryError,
        HistoricalProviderError,
        HistoricalStoreError,
        LearningDataError,
        legacy_fidelity.SelectedCandidateFidelityError,
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

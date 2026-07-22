"""Freeze the timezone-compatibility repair for challenger qualification inspection."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import challenger_orb_retest_qualification_collection as collection
from historical_store import HistoricalStoreError
from learning_data import LearningDataError, load_frozen_dataset_contract


PROJECT_ROOT = Path(__file__).resolve().parent
DATASET_ID = (
    "dataset-challenger-orb-retest-qualification-inspection-compatibility-"
    "2026-07-22-v1"
)
SOURCE_MANIFEST_SHA256 = (
    "96a6119ed7b7fa1d1c7b799dea7de044e64a052b34e2fd070ae0532bdd0e8b3c"
)
SOURCE_MANIFEST = (
    PROJECT_ROOT
    / "historical_batches/challenger_orb_retest_v1_tranche3/"
    "qualification_collection_manifests/"
    "dataset-challenger-orb-retest-entry-qualification-collection-2026-07-22-"
    "v1-96a6119ed7b7fa1d1c7b799dea7de044e64a052b34e2fd070ae0532bdd0e8b3c.json"
)
SOURCE_STATUS = collection.DEFAULT_COLLECTION_STATUS
ORIGINAL_INSPECTOR = (
    PROJECT_ROOT / "challenger_orb_retest_qualification_collection_inspection.py"
)
INSPECTOR = (
    PROJECT_ROOT
    / "challenger_orb_retest_qualification_collection_compatibility_inspection.py"
)
DEFAULT_OUTPUT_ROOT = (
    PROJECT_ROOT
    / "historical_batches/challenger_orb_retest_v1_tranche3/"
    "qualification_inspection_compatibility_manifests"
)
DEFAULT_STATUS = (
    PROJECT_ROOT
    / "historical_batches/challenger_orb_retest_v1_tranche3/"
    "qualification-inspection-compatibility-status.json"
)
DEFAULT_RESULT = (
    PROJECT_ROOT
    / "research_results/"
    "2026-07-22-challenger-orb-retest-qualification-collection-inspection.json"
)


class ChallengerQualificationCompatibilityError(RuntimeError):
    """The isolated timestamp-compatibility contract or its source differs."""


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _sha256_json(value: Any) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ChallengerQualificationCompatibilityError(
            f"cannot read {path}: {exc}"
        ) from exc
    if not isinstance(value, dict):
        raise ChallengerQualificationCompatibilityError(f"{path} must be an object")
    return value


def _write_json(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    rendered = json.dumps(dict(value), indent=2, sort_keys=True) + "\n"
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(rendered, encoding="utf-8")
    temporary.replace(path)


def parse_aware(value: Any) -> datetime:
    parsed = datetime.fromisoformat(str(value))
    if parsed.tzinfo is None:
        raise ChallengerQualificationCompatibilityError(
            "compatibility timestamps must be timezone aware"
        )
    return parsed


def same_instant(left: Any, right: Any) -> bool:
    """Return true only for timezone-aware timestamps at the exact same instant."""

    try:
        return parse_aware(left).astimezone(UTC) == parse_aware(right).astimezone(UTC)
    except (TypeError, ValueError, ChallengerQualificationCompatibilityError):
        return False


def _pair_key(pair: Mapping[str, Any]) -> str:
    return collection._SourceContract._sha256_json(
        (pair["date"], pair["instrument_id"])
    )


def _private_root(store_root: Path) -> Path:
    return store_root / collection.PRIVATE_NAMESPACE / collection.DATASET_ID


def _index_path(store_root: Path) -> Path:
    return _private_root(store_root) / collection.base.INDEX_FILE


def _pair_path(store_root: Path, pair: Mapping[str, Any]) -> Path:
    return (
        _private_root(store_root)
        / collection.base.PAIR_DIRECTORY
        / f"{_pair_key(pair)}.json.gz"
    )


def _source_state(
    env_path: Path,
) -> tuple[
    dict[str, Any],
    dict[str, Any],
    list[dict[str, Any]],
    list[dict[str, Any]],
    Any,
]:
    manifest = load_frozen_dataset_contract(SOURCE_MANIFEST)
    status = _read_json(SOURCE_STATUS)
    if not (
        manifest.get("manifest_sha256") == SOURCE_MANIFEST_SHA256
        and status.get("dataset_id") == collection.DATASET_ID
        and status.get("manifest_sha256") == SOURCE_MANIFEST_SHA256
        and status.get("status") == "COLLECTION_COMPLETE"
        and status.get("pairs_expected") == collection.EXPECTED_PAIRS
        and status.get("pairs_terminal") == collection.EXPECTED_PAIRS
        and status.get("post_entry_data_access_allowed") is False
        and status.get("target_outcomes_observed_or_derived") is False
    ):
        raise ChallengerQualificationCompatibilityError(
            "uninspected qualification collection source differs"
        )
    _base, _public, private, store = collection._load_base(env_path)
    pairs = private["selection"]["positive_pairs"]
    states = [
        collection.base._read_gzip(_pair_path(store.root, pair))
        for pair in pairs
    ]
    return manifest, status, pairs, states, store


def build_compatibility_snapshot(env_path: Path) -> dict[str, Any]:
    """Characterize only the timezone-string mismatch without accepting evidence."""

    manifest, status, pairs, states, store = _source_state(env_path)
    index_path = _index_path(store.root)
    index = collection.base._read_gzip(index_path)
    indexed = {
        str(row["pair_key"]): str(row["sha256"])
        for row in index.get("pair_files", [])
        if isinstance(row, Mapping)
    }
    observed_representation_differences = 0
    decision_representation_differences = 0
    for pair, state in zip(pairs, states, strict=True):
        path = _pair_path(store.root, pair)
        trigger = pair["trigger"]
        basic = (
            indexed.get(_pair_key(pair))
            == collection.base._sha256_file(path)
            and state.get("manifest_sha256") == manifest["manifest_sha256"]
            and state.get("status") == "TERMINAL"
            and state.get("terminal_disposition") == "PREENTRY_INPUTS_COLLECTED"
            and state.get("target_outcome_observed_or_derived") is False
            and state.get("provider_rows_after_final_decision") is False
            and state.get("frozen_retest_trigger") == trigger
            and float(state.get("clean_cross", {}).get("price", 0))
            == float(trigger["rebreak_price"])
            and state.get("search_windows_complete") == 0
            and state.get("search_minute_files") == []
            and state.get("active_search_minute") is None
        )
        observed = state.get("clean_cross", {}).get("observed_at_et")
        decision = state.get("final_decision_at_et")
        if not (
            basic
            and same_instant(observed, trigger["rebreak_at_et"])
            and same_instant(decision, trigger["decision_at_et"])
        ):
            raise ChallengerQualificationCompatibilityError(
                "collection mismatch exceeds timezone representation"
            )
        observed_representation_differences += observed != trigger["rebreak_at_et"]
        decision_representation_differences += decision != trigger["decision_at_et"]
    if not (
        len(pairs) == collection.EXPECTED_PAIRS
        and len(states) == collection.EXPECTED_PAIRS
        and len(indexed) == collection.EXPECTED_PAIRS
        and observed_representation_differences == collection.EXPECTED_PAIRS
        and decision_representation_differences == collection.EXPECTED_PAIRS
    ):
        raise ChallengerQualificationCompatibilityError(
            "timezone representation incident denominator differs"
        )
    return {
        "source_pairs": len(pairs),
        "source_distinct_trigger_sessions": collection.EXPECTED_DATES,
        "observed_at_semantic_matches": len(pairs),
        "decision_at_semantic_matches": len(pairs),
        "observed_at_representation_differences": (
            observed_representation_differences
        ),
        "decision_at_representation_differences": (
            decision_representation_differences
        ),
        "maximum_absolute_timestamp_delta_seconds": 0,
        "source_manifest_file_sha256": _sha256_file(SOURCE_MANIFEST),
        "source_status_file_sha256": _sha256_file(SOURCE_STATUS),
        "private_index_sha256": _sha256_file(index_path),
        "target_outcomes_observed_or_derived": False,
    }


def _implementation_contract() -> dict[str, dict[str, str]]:
    paths = {
        "compatibility_freezer": Path(__file__),
        "compatibility_inspector": INSPECTOR,
        "frozen_collector": Path(collection.__file__),
        "original_inspector": ORIGINAL_INSPECTOR,
    }
    return {
        name: {
            "path": collection.base._repo_path(path),
            "sha256": _sha256_file(path),
        }
        for name, path in paths.items()
    }


def _write_manifest(
    value: Mapping[str, Any], output_root: Path
) -> tuple[Path, dict[str, Any]]:
    content = dict(value)
    content.pop("manifest_sha256", None)
    fingerprint = _sha256_json(content)
    frozen = {**content, "manifest_sha256": fingerprint}
    path = output_root / f"{DATASET_ID}-{fingerprint}.json"
    if path.exists() and _read_json(path) != frozen:
        raise ChallengerQualificationCompatibilityError(
            "hash-addressed compatibility manifest has other content"
        )
    _write_json(path, frozen)
    return path, frozen


def load_manifest(path: Path) -> dict[str, Any]:
    manifest = _read_json(path)
    content = dict(manifest)
    recorded = content.pop("manifest_sha256", None)
    expected = _sha256_json(content)
    if not (
        manifest.get("schema_version") == 1
        and manifest.get("dataset_id") == DATASET_ID
        and recorded == expected
        and path.name == f"{DATASET_ID}-{expected}.json"
    ):
        raise ChallengerQualificationCompatibilityError(
            "compatibility manifest was mutated or renamed"
        )
    return manifest


def freeze(
    *, env_path: Path, output_root: Path, status_path: Path
) -> tuple[Path, dict[str, Any]]:
    for path in (
        Path(__file__),
        INSPECTOR,
        Path(collection.__file__),
        ORIGINAL_INSPECTOR,
        SOURCE_MANIFEST,
        SOURCE_STATUS,
    ):
        collection.base._published(path)
    if DEFAULT_RESULT.exists():
        raise ChallengerQualificationCompatibilityError(
            "compatibility inspection result exists before freeze"
        )
    snapshot = build_compatibility_snapshot(env_path)
    contract = {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "registered_at": datetime.now(UTC).isoformat(),
        "claim_scope": "DEVELOPMENT_ONLY",
        "source_contract": {
            "collection_dataset_id": collection.DATASET_ID,
            "collection_manifest_sha256": SOURCE_MANIFEST_SHA256,
            "collection_manifest_path": collection.base._repo_path(SOURCE_MANIFEST),
            "collection_manifest_file_sha256": _sha256_file(SOURCE_MANIFEST),
            "collection_status_path": collection.base._repo_path(SOURCE_STATUS),
            "collection_status_file_sha256": _sha256_file(SOURCE_STATUS),
            "private_index_sha256": snapshot["private_index_sha256"],
        },
        "compatibility_contract": {
            "permitted_fields": [
                "clean_cross.observed_at_et",
                "final_decision_at_et",
            ],
            "comparison": "ISO_8601_TIMEZONE_AWARE_EXACT_UTC_INSTANT",
            "maximum_absolute_delta_seconds": 0,
            "naive_timestamps_allowed": False,
            "all_other_original_checks_required": True,
            "provider_refetch_allowed": False,
            "source_artifact_mutation_allowed": False,
        },
        "incident_snapshot": snapshot,
        "implementation_contract": _implementation_contract(),
        "outcome_lock": {
            "post_entry_data_access_allowed": False,
            "return_fields_allowed": False,
            "target_outcomes_observed_or_derived": False,
        },
        "privacy_contract": {
            "symbols_dates_instrument_ids_raw_rows_and_requests_public": False,
            "aggregate_counts_and_hashes_only": True,
        },
    }
    path, manifest = _write_manifest(contract, output_root)
    status = {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "status": "FROZEN_WAITING_INSPECTION",
        "inspected": False,
        "source_pairs": snapshot["source_pairs"],
        "semantically_equal_timestamp_pairs": snapshot["source_pairs"],
        "maximum_absolute_timestamp_delta_seconds": 0,
        "post_entry_data_access_allowed": False,
        "target_outcomes_observed_or_derived": False,
    }
    _write_json(status_path, status)
    return path, manifest


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("freeze", "status"))
    parser.add_argument("--env-file", type=Path, default=PROJECT_ROOT / ".env")
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--status", type=Path, default=DEFAULT_STATUS)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "status":
            value = _read_json(args.status)
        else:
            path, manifest = freeze(
                env_path=args.env_file,
                output_root=args.output_root,
                status_path=args.status,
            )
            value = {"manifest": collection.base._repo_path(path), **manifest}
    except (
        ChallengerQualificationCompatibilityError,
        collection.ChallengerQualificationCollectionError,
        collection.base.DevelopmentNonReturnCollectionError,
        HistoricalStoreError,
        LearningDataError,
        OSError,
        ValueError,
    ) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}), file=sys.stderr)
        return 2
    print(json.dumps(value, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

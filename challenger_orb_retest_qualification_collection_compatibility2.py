"""Freeze the v2 timezone-compatible qualification inspection call contract."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import challenger_orb_retest_qualification_collection as collection
import challenger_orb_retest_qualification_collection_compatibility as v1
from historical_store import HistoricalStoreError
from learning_data import LearningDataError


PROJECT_ROOT = Path(__file__).resolve().parent
DATASET_ID = (
    "dataset-challenger-orb-retest-qualification-inspection-compatibility-"
    "2026-07-22-v2"
)
V1_MANIFEST_SHA256 = (
    "0e8c901eac1f3beb32ba4c248a986baed47aa6ada393920aff4205299fadd961"
)
V1_MANIFEST = (
    PROJECT_ROOT
    / "historical_batches/challenger_orb_retest_v1_tranche3/"
    "qualification_inspection_compatibility_manifests/"
    "dataset-challenger-orb-retest-qualification-inspection-compatibility-"
    "2026-07-22-v1-0e8c901eac1f3beb32ba4c248a986baed47aa6ada393920aff4205299fadd961.json"
)
V1_STATUS = v1.DEFAULT_STATUS
V1_INSPECTOR = v1.INSPECTOR
INSPECTOR = (
    PROJECT_ROOT
    / "challenger_orb_retest_qualification_collection_compatibility_inspection2.py"
)
DEFAULT_OUTPUT_ROOT = (
    PROJECT_ROOT
    / "historical_batches/challenger_orb_retest_v1_tranche3/"
    "qualification_inspection_compatibility_manifests"
)
DEFAULT_STATUS = (
    PROJECT_ROOT
    / "historical_batches/challenger_orb_retest_v1_tranche3/"
    "qualification-inspection-compatibility-v2-status.json"
)
DEFAULT_RESULT = v1.DEFAULT_RESULT


class ChallengerQualificationCompatibility2Error(RuntimeError):
    """The v2 quote-rebuild compatibility contract or source differs."""


def collector_local_time(value: Any) -> datetime:
    return v1.parse_aware(value).astimezone(collection.base.EASTERN)


def build_quote_call_snapshot(env_path: Path) -> dict[str, Any]:
    """Prove the stored snapshots rebuild from the reconciled collector-local time."""

    _manifest, _status, pairs, states, _store = v1._source_state(env_path)
    stored_matches = 0
    source_representation_mismatches = 0
    empty_snapshot_sets = 0
    for pair, state in zip(pairs, states, strict=True):
        trigger = pair["trigger"]
        quote = state["requests"]["quote_window"]
        expected = quote.get("snapshots")
        stored_clean = v1.parse_aware(state["clean_cross"]["observed_at_et"])
        source_clean = v1.parse_aware(trigger["rebreak_at_et"])
        if not v1.same_instant(stored_clean, source_clean):
            raise ChallengerQualificationCompatibility2Error(
                "quote rebuild clean time differs as an instant"
            )
        stored_result = collection.base.quote_snapshots(
            quote["observations"], stored_clean
        )
        source_result = collection.base.quote_snapshots(
            quote["observations"], source_clean
        )
        if stored_result != expected:
            raise ChallengerQualificationCompatibility2Error(
                "stored quote snapshots do not rebuild"
            )
        stored_matches += 1
        source_representation_mismatches += source_result != expected
        empty_snapshot_sets += expected == []
    if not (
        stored_matches == collection.EXPECTED_PAIRS
        and source_representation_mismatches + empty_snapshot_sets
        == collection.EXPECTED_PAIRS
    ):
        raise ChallengerQualificationCompatibility2Error(
            "quote call-site incident denominator differs"
        )
    return {
        "source_pairs": len(pairs),
        "stored_collector_local_snapshot_rebuilds": stored_matches,
        "source_utc_representation_snapshot_mismatches": (
            source_representation_mismatches
        ),
        "empty_snapshot_sets_unaffected": empty_snapshot_sets,
        "maximum_absolute_timestamp_delta_seconds": 0,
        "v1_manifest_file_sha256": v1._sha256_file(V1_MANIFEST),
        "v1_status_file_sha256": v1._sha256_file(V1_STATUS),
        "source_collection_status_file_sha256": v1._sha256_file(v1.SOURCE_STATUS),
        "private_index_sha256": v1.build_compatibility_snapshot(env_path)[
            "private_index_sha256"
        ],
        "target_outcomes_observed_or_derived": False,
    }


def _implementation_contract() -> dict[str, dict[str, str]]:
    paths = {
        "compatibility_v2_freezer": Path(__file__),
        "compatibility_v2_inspector": INSPECTOR,
        "compatibility_v1_freezer": Path(v1.__file__),
        "compatibility_v1_inspector": V1_INSPECTOR,
        "frozen_collector": Path(collection.__file__),
    }
    return {
        name: {"path": collection.base._repo_path(path), "sha256": v1._sha256_file(path)}
        for name, path in paths.items()
    }


def _write_manifest(
    value: Mapping[str, Any], output_root: Path
) -> tuple[Path, dict[str, Any]]:
    content = dict(value)
    content.pop("manifest_sha256", None)
    fingerprint = v1._sha256_json(content)
    frozen = {**content, "manifest_sha256": fingerprint}
    path = output_root / f"{DATASET_ID}-{fingerprint}.json"
    if path.exists() and v1._read_json(path) != frozen:
        raise ChallengerQualificationCompatibility2Error(
            "hash-addressed v2 compatibility manifest has other content"
        )
    v1._write_json(path, frozen)
    return path, frozen


def load_manifest(path: Path) -> dict[str, Any]:
    manifest = v1._read_json(path)
    content = dict(manifest)
    recorded = content.pop("manifest_sha256", None)
    expected = v1._sha256_json(content)
    if not (
        manifest.get("schema_version") == 1
        and manifest.get("dataset_id") == DATASET_ID
        and recorded == expected
        and path.name == f"{DATASET_ID}-{expected}.json"
    ):
        raise ChallengerQualificationCompatibility2Error(
            "v2 compatibility manifest was mutated or renamed"
        )
    return manifest


def freeze(
    *, env_path: Path, output_root: Path, status_path: Path
) -> tuple[Path, dict[str, Any]]:
    for path in (
        Path(__file__),
        INSPECTOR,
        Path(v1.__file__),
        V1_INSPECTOR,
        Path(collection.__file__),
        V1_MANIFEST,
        V1_STATUS,
        v1.SOURCE_MANIFEST,
        v1.SOURCE_STATUS,
    ):
        collection.base._published(path)
    if DEFAULT_RESULT.exists():
        raise ChallengerQualificationCompatibility2Error(
            "qualification inspection result exists before v2 freeze"
        )
    v1_manifest = v1.load_manifest(V1_MANIFEST)
    v1_status = v1._read_json(V1_STATUS)
    if not (
        v1_manifest["manifest_sha256"] == V1_MANIFEST_SHA256
        and v1_status.get("manifest_sha256") == V1_MANIFEST_SHA256
        and v1_status.get("status") == "FROZEN_WAITING_INSPECTION"
        and v1_status.get("inspected") is False
        and v1_status.get("post_entry_data_access_allowed") is False
        and v1_status.get("target_outcomes_observed_or_derived") is False
    ):
        raise ChallengerQualificationCompatibility2Error(
            "failed v1 compatibility boundary differs"
        )
    snapshot = build_quote_call_snapshot(env_path)
    contract = {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "registered_at": datetime.now(UTC).isoformat(),
        "claim_scope": "DEVELOPMENT_ONLY",
        "source_contract": {
            "v1_manifest_sha256": V1_MANIFEST_SHA256,
            "v1_manifest_path": collection.base._repo_path(V1_MANIFEST),
            "v1_manifest_file_sha256": v1._sha256_file(V1_MANIFEST),
            "v1_status_path": collection.base._repo_path(V1_STATUS),
            "v1_status_file_sha256": v1._sha256_file(V1_STATUS),
            "source_collection_manifest_sha256": v1.SOURCE_MANIFEST_SHA256,
            "source_collection_status_file_sha256": v1._sha256_file(
                v1.SOURCE_STATUS
            ),
            "private_index_sha256": snapshot["private_index_sha256"],
        },
        "compatibility_contract": {
            "v1_exact_utc_instant_contract_unchanged": True,
            "quote_snapshot_clean_time": (
                "STORED_COLLECTOR_EASTERN_AFTER_EXACT_INSTANT_RECONCILIATION"
            ),
            "generated_snapshot_must_equal_stored_snapshot_exactly": True,
            "maximum_absolute_delta_seconds": 0,
            "provider_refetch_allowed": False,
            "source_artifact_mutation_allowed": False,
            "v1_public_status_mutation_allowed": False,
            "all_other_v1_inspection_checks_required": True,
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
    v1._write_json(
        status_path,
        {
            "schema_version": 1,
            "dataset_id": DATASET_ID,
            "manifest_sha256": manifest["manifest_sha256"],
            "status": "FROZEN_WAITING_INSPECTION",
            "inspected": False,
            "source_pairs": snapshot["source_pairs"],
            "stored_collector_local_snapshot_rebuilds": snapshot[
                "stored_collector_local_snapshot_rebuilds"
            ],
            "maximum_absolute_timestamp_delta_seconds": 0,
            "post_entry_data_access_allowed": False,
            "target_outcomes_observed_or_derived": False,
        },
    )
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
            value = v1._read_json(args.status)
        else:
            path, manifest = freeze(
                env_path=args.env_file,
                output_root=args.output_root,
                status_path=args.status,
            )
            value = {"manifest": collection.base._repo_path(path), **manifest}
    except (
        ChallengerQualificationCompatibility2Error,
        v1.ChallengerQualificationCompatibilityError,
        collection.ChallengerQualificationCollectionError,
        collection.base.DevelopmentNonReturnCollectionError,
        HistoricalStoreError,
        LearningDataError,
        OSError,
        TypeError,
        ValueError,
    ) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}), file=sys.stderr)
        return 2
    print(json.dumps(value, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

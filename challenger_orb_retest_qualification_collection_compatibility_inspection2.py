"""Inspect qualification collection through the frozen v2 quote-call contract."""

from __future__ import annotations

import argparse
import json
import sys
import tempfile
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import challenger_orb_retest_qualification_collection as collection
import challenger_orb_retest_qualification_collection_compatibility as v1
import challenger_orb_retest_qualification_collection_compatibility2 as v2
import challenger_orb_retest_qualification_collection_compatibility_inspection as v1_inspection
from historical_store import HistoricalStoreError
from learning_data import LearningDataError


class ChallengerQualificationCompatibilityInspection2Error(RuntimeError):
    """The v2 compatibility manifest or sandboxed full inspection differs."""


def _verify_binding(binding: Any) -> None:
    if not isinstance(binding, Mapping):
        raise ChallengerQualificationCompatibilityInspection2Error(
            "v2 implementation binding is malformed"
        )
    path = v2.PROJECT_ROOT / str(binding.get("path"))
    if v1._sha256_file(path) != binding.get("sha256"):
        raise ChallengerQualificationCompatibilityInspection2Error(
            "v2 implementation binding differs"
        )


def inspect(
    *,
    manifest_path: Path,
    env_path: Path,
    compatibility_status_path: Path,
    collection_status_path: Path,
    result_path: Path,
) -> dict[str, Any]:
    manifest = v2.load_manifest(manifest_path)
    status = v1._read_json(compatibility_status_path)
    source = manifest["source_contract"]
    if not (
        status.get("manifest_sha256") == manifest["manifest_sha256"]
        and status.get("status") == "FROZEN_WAITING_INSPECTION"
        and status.get("inspected") is False
        and status.get("post_entry_data_access_allowed") is False
        and status.get("target_outcomes_observed_or_derived") is False
        and v1._sha256_file(v2.V1_MANIFEST) == source["v1_manifest_file_sha256"]
        and v1._sha256_file(v2.V1_STATUS) == source["v1_status_file_sha256"]
        and v1._sha256_file(v1.SOURCE_STATUS)
        == source["source_collection_status_file_sha256"]
        and not result_path.exists()
    ):
        raise ChallengerQualificationCompatibilityInspection2Error(
            "v2 compatibility zero-result state differs"
        )
    for binding in manifest["implementation_contract"].values():
        _verify_binding(binding)
    rebuilt = v2.build_quote_call_snapshot(env_path)
    if rebuilt != manifest["incident_snapshot"]:
        raise ChallengerQualificationCompatibilityInspection2Error(
            "v2 quote-call snapshot differs"
        )

    original_quote_snapshots = collection.base.quote_snapshots

    def collector_local_quote_snapshots(
        quotes: Any, clean_at: Any
    ) -> list[dict[str, Any]]:
        return original_quote_snapshots(
            quotes, v2.collector_local_time(clean_at)
        )

    with tempfile.TemporaryDirectory(
        prefix=".qualification-inspection-v2-", dir=v2.PROJECT_ROOT
    ) as raw:
        temporary = Path(raw)
        temporary_v1_status = temporary / "v1-status.json"
        temporary_collection_status = temporary / "collection-status.json"
        temporary_result = temporary / "result.json"
        v1._write_json(temporary_v1_status, v1._read_json(v2.V1_STATUS))
        collection.base.quote_snapshots = collector_local_quote_snapshots
        try:
            result = v1_inspection.inspect(
                manifest_path=v2.V1_MANIFEST,
                env_path=env_path,
                compatibility_status_path=temporary_v1_status,
                collection_status_path=temporary_collection_status,
                result_path=temporary_result,
            )
        finally:
            collection.base.quote_snapshots = original_quote_snapshots
    if not (
        result.get("status") == "COLLECTION_INSPECTED"
        and result.get("inspected") is True
        and result.get("pairs_terminal") == collection.EXPECTED_PAIRS
        and result.get("timezone_representation_pairs_reconciled")
        == collection.EXPECTED_PAIRS
        and result.get("maximum_absolute_timestamp_delta_seconds") == 0
        and result.get("post_entry_data_access_allowed") is False
        and result.get("target_outcomes_observed_or_derived") is False
        and result.get("valid") is True
    ):
        raise ChallengerQualificationCompatibilityInspection2Error(
            "sandboxed full qualification inspection differs"
        )
    final_result = {
        **result,
        "inspection_contract_id": v2.DATASET_ID,
        "inspection_manifest_sha256": manifest["manifest_sha256"],
        "quote_snapshot_clean_time": (
            "STORED_COLLECTOR_EASTERN_AFTER_EXACT_INSTANT_RECONCILIATION"
        ),
        "v1_public_status_mutated": False,
    }
    final_result["inspection"] = {
        **result["inspection"],
        "v1_full_inspection_rerun_in_temporary_sandbox": True,
        "stored_quote_snapshots_rebuilt_exactly": True,
        "v1_public_status_preserved": True,
    }
    ready = {
        "schema_version": 1,
        "dataset_id": v2.DATASET_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "status": "READY",
        "inspected": True,
        "source_collection_status": "COLLECTION_INSPECTED",
        "source_pairs": collection.EXPECTED_PAIRS,
        "stored_collector_local_snapshot_rebuilds": rebuilt[
            "stored_collector_local_snapshot_rebuilds"
        ],
        "maximum_absolute_timestamp_delta_seconds": 0,
        "result_path": collection.base._repo_path(result_path),
        "v1_public_status_mutated": False,
        "post_entry_data_access_allowed": False,
        "target_outcomes_observed_or_derived": False,
        "valid": True,
    }
    v1._write_json(result_path, final_result)
    v1._write_json(collection_status_path, final_result)
    v1._write_json(compatibility_status_path, ready)
    return final_result


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("manifest", type=Path)
    parser.add_argument("--env-file", type=Path, default=v2.PROJECT_ROOT / ".env")
    parser.add_argument("--compatibility-status", type=Path, default=v2.DEFAULT_STATUS)
    parser.add_argument("--collection-status", type=Path, default=v1.SOURCE_STATUS)
    parser.add_argument("--result", type=Path, default=v2.DEFAULT_RESULT)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        value = inspect(
            manifest_path=args.manifest,
            env_path=args.env_file,
            compatibility_status_path=args.compatibility_status,
            collection_status_path=args.collection_status,
            result_path=args.result,
        )
    except (
        ChallengerQualificationCompatibilityInspection2Error,
        v2.ChallengerQualificationCompatibility2Error,
        v1.ChallengerQualificationCompatibilityError,
        v1_inspection.ChallengerQualificationCompatibilityInspectionError,
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

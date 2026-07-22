"""Freeze and run the challenger trigger engine through its exact-window reader."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import challenger_orb_retest_preentry as preentry
import challenger_orb_retest_preentry_reader as causal_reader
from historical_store import HistoricalStoreConfig, HistoricalStoreError
from learning_data import (
    LearningDataError,
    freeze_dataset_contract,
    load_frozen_dataset_contract,
)


PROJECT_ROOT = Path(__file__).resolve().parent
DATASET_ID = "dataset-challenger-orb-retest-trigger-review-2026-07-21-v1"
SOURCE_MANIFEST = (
    PROJECT_ROOT
    / "historical_batches/challenger_orb_retest_v1/preentry_manifests"
    / (
        "dataset-challenger-orb-retest-preentry-collection-2026-07-21-v1-"
        "e9bc5a8a57c8695455398b31291e4d4e3267612b04f0fef5742ad0fc57846b77.json"
    )
)
COLLECTION_INSPECTION = (
    PROJECT_ROOT
    / "research_results/2026-07-21-challenger-orb-retest-preentry-collection-inspection.json"
)
COLLECTION_STATUS = (
    PROJECT_ROOT / "historical_batches/challenger_orb_retest_v1/preentry-status.json"
)
INSPECTOR = PROJECT_ROOT / "challenger_orb_retest_trigger_runner_inspection.py"
DEFAULT_OUTPUT_ROOT = (
    PROJECT_ROOT / "historical_batches/challenger_orb_retest_v1/trigger_manifests"
)
DEFAULT_STATUS = (
    PROJECT_ROOT / "historical_batches/challenger_orb_retest_v1/trigger-status.json"
)


class ChallengerTriggerRunnerError(RuntimeError):
    """The trigger compatibility contract is incomplete or has drifted."""


def _repo_path(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(PROJECT_ROOT))
    except ValueError as exc:
        raise ChallengerTriggerRunnerError(f"path is outside repository: {path}") from exc


def _source_state(env_path: Path) -> tuple[dict[str, Any], HistoricalStoreConfig, dict[str, Any]]:
    manifest, config, selection = preentry._load_contract(
        manifest_path=SOURCE_MANIFEST,
        env_path=env_path,
        require_published=False,
    )
    inspection = preentry._read_json(COLLECTION_INSPECTION)
    status = preentry._read_json(COLLECTION_STATUS)
    if not (
        inspection.get("status") == "COLLECTION_INSPECTED"
        and inspection.get("valid") is True
        and inspection.get("manifest_sha256") == manifest["manifest_sha256"]
        and inspection.get("counts", {}).get("successful_requests") == 284
        and inspection.get("counts", {}).get("failed_requests") == 0
        and inspection.get("counts", {}).get("pending_requests") == 0
        and inspection.get("counts", {}).get("rows") == 3_195_784
        and inspection.get("target_outcomes_observed_or_derived") is False
        and inspection.get("inspection", {}).get("zero_trigger_artifacts_verified")
        is True
        and status.get("status") == "COLLECTION_INSPECTED"
        and status.get("inspected") is True
        and status.get("trigger_artifacts_present") is False
    ):
        raise ChallengerTriggerRunnerError("inspected causal collection differs")
    return manifest, config, selection


def _implementation_contract() -> dict[str, dict[str, str]]:
    paths = {
        "runner": Path(__file__),
        "inspector": INSPECTOR,
        "reader": Path(causal_reader.__file__),
        "preentry_engine": Path(preentry.__file__),
        "trigger_engine": Path(preentry.trigger.__file__),
        "preentry_result_inspector": preentry.INSPECTOR,
    }
    return {
        name: {"path": _repo_path(path), "sha256": preentry._sha256_file(path)}
        for name, path in paths.items()
    }


def freeze_inputs(
    *, env_path: Path, output_root: Path, status_path: Path
) -> tuple[Path, dict[str, Any]]:
    for path in (Path(__file__), INSPECTOR, Path(causal_reader.__file__)):
        preentry._published(Path(path))
    for path in (SOURCE_MANIFEST, COLLECTION_INSPECTION, COLLECTION_STATUS):
        preentry._published(path)
    source_manifest, config, selection = _source_state(env_path)
    if preentry._trigger_path(config.root).exists():
        raise ChallengerTriggerRunnerError("trigger artifact exists before runner freeze")
    inspection = preentry._read_json(COLLECTION_INSPECTION)
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
                _repo_path(SOURCE_MANIFEST),
                _repo_path(COLLECTION_INSPECTION),
                _repo_path(COLLECTION_STATUS),
                "CHALLENGER_ORB_RETEST.md",
                "PRODUCTION_STRATEGY_VALIDATION.md",
            ],
            "inspected": False,
        },
        "source_contract": {
            "preentry_manifest_sha256": source_manifest["manifest_sha256"],
            "preentry_manifest_file_sha256": preentry._sha256_file(SOURCE_MANIFEST),
            "collection_inspection_sha256": preentry._sha256_file(
                COLLECTION_INSPECTION
            ),
            "private_collection_file_sha256": inspection[
                "private_collection_file_sha256"
            ],
            "wrapper_set_sha256": inspection["wrapper_set_sha256"],
            "canonical_row_set_sha256": inspection["canonical_row_set_sha256"],
            "request_graph_sha256": selection["request_graph_sha256"],
        },
        "trigger_contract": dict(source_manifest["trigger_contract"]),
        "implementation_contract": _implementation_contract(),
        "compatibility_contract": {
            "only_runtime_substitution": "preentry.LocalHistoricalClient",
            "replacement": "FrozenCausalWindowClient",
            "provider": "alpaca",
            "feed": "sip",
            "adjustment": "raw",
            "exact_request_provenance_required": True,
            "trigger_rule_change_allowed": False,
        },
        "outcome_lock": dict(source_manifest["outcome_lock"]),
    }
    path, manifest = freeze_dataset_contract(contract, output_root)
    public = {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "source_preentry_manifest_sha256": source_manifest["manifest_sha256"],
        "status": "FROZEN_AWAITING_INSPECTION",
        "inspected": False,
        "counts": selection["counts"],
        "trigger_artifacts_present": False,
        "post_entry_data_accessed": False,
        "target_outcomes_observed_or_derived": False,
    }
    preentry._write_json(status_path, public)
    return path, manifest


def load_contract(
    *, manifest_path: Path, env_path: Path, require_published: bool
) -> tuple[dict[str, Any], HistoricalStoreConfig, dict[str, Any]]:
    manifest = load_frozen_dataset_contract(manifest_path)
    if manifest.get("dataset_id") != DATASET_ID:
        raise ChallengerTriggerRunnerError("unexpected trigger runner dataset")
    source_manifest, config, selection = _source_state(env_path)
    source = manifest.get("source_contract", {})
    inspection = preentry._read_json(COLLECTION_INSPECTION)
    if not (
        source.get("preentry_manifest_sha256") == source_manifest["manifest_sha256"]
        and source.get("preentry_manifest_file_sha256")
        == preentry._sha256_file(SOURCE_MANIFEST)
        and source.get("collection_inspection_sha256")
        == preentry._sha256_file(COLLECTION_INSPECTION)
        and source.get("private_collection_file_sha256")
        == inspection["private_collection_file_sha256"]
        and source.get("wrapper_set_sha256") == inspection["wrapper_set_sha256"]
        and source.get("canonical_row_set_sha256")
        == inspection["canonical_row_set_sha256"]
        and source.get("request_graph_sha256") == selection["request_graph_sha256"]
        and manifest.get("trigger_contract") == source_manifest["trigger_contract"]
        and manifest.get("outcome_lock") == source_manifest["outcome_lock"]
    ):
        raise ChallengerTriggerRunnerError("trigger runner source binding differs")
    for value in manifest.get("implementation_contract", {}).values():
        path = PROJECT_ROOT / str(value["path"])
        if preentry._sha256_file(path) != value.get("sha256"):
            raise ChallengerTriggerRunnerError("trigger runner implementation drifted")
    if require_published:
        for path in (manifest_path, Path(__file__), INSPECTOR):
            preentry._published(path)
    return manifest, config, selection


@contextmanager
def using_exact_reader() -> Iterator[None]:
    original = preentry.LocalHistoricalClient
    preentry.LocalHistoricalClient = causal_reader.FrozenCausalWindowClient
    try:
        yield
    finally:
        preentry.LocalHistoricalClient = original


def derive(
    *, manifest_path: Path, env_path: Path, status_path: Path
) -> dict[str, Any]:
    manifest, _config, _selection = load_contract(
        manifest_path=manifest_path,
        env_path=env_path,
        require_published=True,
    )
    status = preentry._read_json(status_path)
    if not (
        status.get("manifest_sha256") == manifest["manifest_sha256"]
        and status.get("status") == "FROZEN_READY"
        and status.get("inspected") is True
        and status.get("trigger_artifacts_present") is False
    ):
        raise ChallengerTriggerRunnerError("trigger runner is not FROZEN_READY")
    with using_exact_reader():
        result = preentry.derive(
            manifest_path=SOURCE_MANIFEST,
            env_path=env_path,
            status_path=preentry.DEFAULT_PUBLIC_STATUS,
        )
    public = {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "source_preentry_manifest_sha256": result["manifest_sha256"],
        "status": "TRIGGER_REVIEW_COMPLETE_UNINSPECTED",
        "inspected": False,
        "counts": result["counts"],
        "terminal_reason_counts": result["terminal_reason_counts"],
        "private_trigger_sha256": result["private_trigger_sha256"],
        "post_entry_data_accessed": False,
        "target_outcomes_observed_or_derived": False,
    }
    preentry._write_json(status_path, public)
    return public


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("freeze", "derive", "status"))
    parser.add_argument("--manifest", type=Path)
    parser.add_argument("--env-file", type=Path, default=PROJECT_ROOT / ".env")
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--status", type=Path, default=DEFAULT_STATUS)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "freeze":
            path, manifest = freeze_inputs(
                env_path=args.env_file,
                output_root=args.output_root,
                status_path=args.status,
            )
            value = {"manifest": _repo_path(path), **manifest}
        elif args.command == "status":
            value = preentry._read_json(args.status)
        elif args.manifest is None:
            raise ChallengerTriggerRunnerError("--manifest is required")
        else:
            value = derive(
                manifest_path=args.manifest,
                env_path=args.env_file,
                status_path=args.status,
            )
    except (
        ChallengerTriggerRunnerError,
        preentry.ChallengerPreentryError,
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

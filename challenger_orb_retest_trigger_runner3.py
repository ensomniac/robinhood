"""Freeze and run the third-tranche trigger through its exact-window reader."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import challenger_orb_retest_preentry3 as preentry3
import challenger_orb_retest_trigger_runner as base
from historical_store import HistoricalStoreConfig, HistoricalStoreError
from learning_data import LearningDataError


PROJECT_ROOT = Path(__file__).resolve().parent
BASE_PATH = Path(base.__file__).resolve()
BASE_IMPLEMENTATION_SHA256 = (
    "99f6739a68b2f01592dd5e87985ad38bdf241c147c7b7f250bf2af3d0970b4f5"
)
DATASET_ID = (
    "dataset-challenger-orb-retest-trigger-review-2026-07-22-tranche3-v2"
)
SOURCE_MANIFEST = (
    PROJECT_ROOT
    / "historical_batches/challenger_orb_retest_v1_tranche3/"
    "preentry_manifests"
    / (
        "dataset-challenger-orb-retest-preentry-collection-2026-07-22-"
        "tranche3-v2-"
        "45e9441bbc43f6b79cac90e6a84e375da2b9e56cb1f6d07ae0362f8f64152500.json"
    )
)
COLLECTION_INSPECTION = (
    PROJECT_ROOT
    / "research_results/"
    "2026-07-22-challenger-orb-retest-preentry-tranche3-collection-inspection.json"
)
COLLECTION_STATUS = (
    PROJECT_ROOT
    / "historical_batches/challenger_orb_retest_v1_tranche3/preentry-status.json"
)
INSPECTOR = PROJECT_ROOT / "challenger_orb_retest_trigger_runner_inspection3.py"
DEFAULT_OUTPUT_ROOT = (
    PROJECT_ROOT
    / "historical_batches/challenger_orb_retest_v1_tranche3/trigger_manifests"
)
DEFAULT_STATUS = (
    PROJECT_ROOT
    / "historical_batches/challenger_orb_retest_v1_tranche3/trigger-status.json"
)
EXPECTED_REQUESTS = 86
EXPECTED_ROWS = 739_435
EXPECTED_PAIRS = 23
EXPECTED_DATES = 20


class ChallengerTriggerRunner3Error(RuntimeError):
    """The third-tranche runner adapter or frozen base has drifted."""


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _source_state(
    env_path: Path,
) -> tuple[dict[str, Any], HistoricalStoreConfig, dict[str, Any]]:
    preentry = preentry3.base
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
        and inspection.get("counts", {}).get("successful_requests")
        == EXPECTED_REQUESTS
        and inspection.get("counts", {}).get("failed_requests") == 0
        and inspection.get("counts", {}).get("pending_requests") == 0
        and inspection.get("counts", {}).get("rows") == EXPECTED_ROWS
        and inspection.get("target_outcomes_observed_or_derived") is False
        and inspection.get("inspection", {}).get("zero_trigger_artifacts_verified")
        is True
        and status.get("status") == "COLLECTION_INSPECTED"
        and status.get("inspected") is True
        and status.get("trigger_artifacts_present") is False
        and selection.get("counts", {}).get("verified_positive_pairs")
        == EXPECTED_PAIRS
        and selection.get("counts", {}).get("verified_positive_dates")
        == EXPECTED_DATES
    ):
        raise ChallengerTriggerRunner3Error(
            "inspected third-tranche causal collection differs"
        )
    return manifest, config, selection


def configure_base() -> None:
    """Install the third-tranche contract into the hash-pinned runner."""

    if _sha256_file(BASE_PATH) != BASE_IMPLEMENTATION_SHA256:
        raise ChallengerTriggerRunner3Error(
            "frozen base trigger runner implementation drifted"
        )
    preentry3.configure_base()
    values: dict[str, Any] = {
        "preentry": preentry3.base,
        "DATASET_ID": DATASET_ID,
        "SOURCE_MANIFEST": SOURCE_MANIFEST,
        "COLLECTION_INSPECTION": COLLECTION_INSPECTION,
        "COLLECTION_STATUS": COLLECTION_STATUS,
        "INSPECTOR": INSPECTOR,
        "DEFAULT_OUTPUT_ROOT": DEFAULT_OUTPUT_ROOT,
        "DEFAULT_STATUS": DEFAULT_STATUS,
        "_source_state": _source_state,
        "__file__": str(Path(__file__).resolve()),
    }
    for name, value in values.items():
        setattr(base, name, value)


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
        configure_base()
        if args.command == "freeze":
            path, manifest = base.freeze_inputs(
                env_path=args.env_file,
                output_root=args.output_root,
                status_path=args.status,
            )
            value = {"manifest": base._repo_path(path), **manifest}
        elif args.command == "status":
            value = preentry3.base._read_json(args.status)
        elif args.manifest is None:
            raise ChallengerTriggerRunner3Error("--manifest is required")
        else:
            value = base.derive(
                manifest_path=args.manifest,
                env_path=args.env_file,
                status_path=args.status,
            )
    except (
        ChallengerTriggerRunner3Error,
        base.ChallengerTriggerRunnerError,
        preentry3.ChallengerPreentry3Error,
        preentry3.base.ChallengerPreentryError,
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

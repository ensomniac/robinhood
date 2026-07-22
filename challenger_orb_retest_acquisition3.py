"""Operate the challenger's third-tranche pre-outcome acquisition boundary."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import challenger_orb_retest_acquisition2 as base
import challenger_orb_retest_tranche3 as tranche3
from learning_data import LearningDataError
from scanner_replay import ScannerReplayError


PROJECT_ROOT = Path(__file__).resolve().parent
BASE_PATH = Path(base.__file__).resolve()
BASE_IMPLEMENTATION_SHA256 = (
    "64f4dd1091053e6b3ec751248a8d46d5a15a70a3141672773e89d76912355240"
)
DATASET_ID = "dataset-challenger-orb-retest-acquisition-2026-07-22-tranche3-v1"
SCANNER_DATASET_ID = (
    "dataset-production-scanner-replay-2026-07-22-"
    "challenger-orb-retest-tranche3-v1"
)
SELECTION_MANIFEST = (
    tranche3.DEFAULT_MANIFEST_ROOT
    / (
        "dataset-challenger-orb-retest-development-2026-07-22-v3-"
        "90dde41a23e82c07f7fa28f4b45ab60944cd7a4745a0614b60eea7ecab152bfa.json"
    )
)
SELECTION_STATUS = tranche3.DEFAULT_STATUS
ROOT = PROJECT_ROOT / "historical_batches/challenger_orb_retest_v1_tranche3"
DEFAULT_SCANNER_MANIFEST_ROOT = ROOT / "scanner_manifests"
DEFAULT_OUTPUT_ROOT = ROOT / "acquisition_manifests"
DEFAULT_STATUS = ROOT / "acquisition-contract-status.json"
RUN_ROOT = PROJECT_ROOT / "learning_runs/challenger_orb_retest_v1_tranche3/scanner_replay"
REFERENCE_ROOT = RUN_ROOT / "reference"
ACQUISITION_LOCK = RUN_ROOT / "provider-acquisition.lock"
SECURITY_MASTER = ROOT / "security-master.jsonl"
SECURITY_SOURCE = ROOT / "security-master-source.json"
SPLITS = RUN_ROOT / "splits.json.gz"
SPLIT_SOURCE = ROOT / "split-actions-source.json"
INPUT_STATUS = ROOT / "reference-and-split-status.json"
SCANNER_CONTRACT_STATUS = ROOT / "scanner-contract-status.json"
REUSE_SCANNER_MANIFEST = base.REUSE_SCANNER_MANIFEST
INSPECTOR = PROJECT_ROOT / "challenger_orb_retest_acquisition3_inspection.py"


class ChallengerAcquisition3Error(RuntimeError):
    """The third-tranche acquisition adapter or frozen base has drifted."""


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _module_values() -> dict[str, Any]:
    tranche3.configure_base()
    return {
        "tranche": tranche3.base,
        "DATASET_ID": DATASET_ID,
        "SCANNER_DATASET_ID": SCANNER_DATASET_ID,
        "SELECTION_MANIFEST": SELECTION_MANIFEST,
        "SELECTION_STATUS": SELECTION_STATUS,
        "ROOT": ROOT,
        "DEFAULT_SCANNER_MANIFEST_ROOT": DEFAULT_SCANNER_MANIFEST_ROOT,
        "DEFAULT_OUTPUT_ROOT": DEFAULT_OUTPUT_ROOT,
        "DEFAULT_STATUS": DEFAULT_STATUS,
        "RUN_ROOT": RUN_ROOT,
        "REFERENCE_ROOT": REFERENCE_ROOT,
        "ACQUISITION_LOCK": ACQUISITION_LOCK,
        "SECURITY_MASTER": SECURITY_MASTER,
        "SECURITY_SOURCE": SECURITY_SOURCE,
        "SPLITS": SPLITS,
        "SPLIT_SOURCE": SPLIT_SOURCE,
        "INPUT_STATUS": INPUT_STATUS,
        "SCANNER_CONTRACT_STATUS": SCANNER_CONTRACT_STATUS,
        "REUSE_SCANNER_MANIFEST": REUSE_SCANNER_MANIFEST,
        "INSPECTOR": INSPECTOR,
        "__file__": str(Path(__file__).resolve()),
    }


@contextmanager
def configured() -> Iterator[Any]:
    """Scope third-tranche values through the proven acquisition adapter."""

    if _sha256_file(BASE_PATH) != BASE_IMPLEMENTATION_SHA256:
        raise ChallengerAcquisition3Error(
            "frozen second-tranche acquisition adapter drifted"
        )
    values = _module_values()
    original = {name: getattr(base, name) for name in values}
    try:
        for name, value in values.items():
            setattr(base, name, value)
        with base.configured() as acquisition:
            yield acquisition
    finally:
        for name, value in original.items():
            setattr(base, name, value)


def _call(name: str, *args: Any, **kwargs: Any):
    with configured() as acquisition:
        return getattr(acquisition, name)(*args, **kwargs)


def collect_splits(*, env_path: Path) -> dict[str, Any]:
    values = _module_values()
    original = {name: getattr(base, name) for name in values}
    try:
        for name, value in values.items():
            setattr(base, name, value)
        return base.collect_splits(env_path=env_path)
    finally:
        for name, value in original.items():
            setattr(base, name, value)


def _scanner_manifest_path() -> Path:
    with configured() as acquisition:
        return acquisition._scanner_manifest_path(DEFAULT_SCANNER_MANIFEST_ROOT)


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
    collect = sub.add_parser("collect-scanner")
    collect.add_argument("manifest", type=Path)
    collect.add_argument("--max-days", type=int)
    status = sub.add_parser("status")
    status.add_argument("manifest", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "collect-reference":
            result = _call("collect_reference", env_path=args.env)
        elif args.command == "reference-status":
            result = _call("reference_status")
        elif args.command == "build-master":
            result = _call("build_master")
        elif args.command == "collect-splits":
            result = collect_splits(env_path=args.env)
        elif args.command == "audit-inputs":
            result = _call("audit_inputs", env_path=args.env)
        elif args.command == "freeze-scanner":
            _path, result = _call("freeze_scanner", env_path=args.env)
        elif args.command == "freeze":
            scanner_path = args.scanner_manifest or _scanner_manifest_path()
            path, manifest = _call(
                "freeze_contract",
                scanner_manifest_path=scanner_path,
                env_path=args.env,
                output_root=DEFAULT_OUTPUT_ROOT,
            )
            result = {
                "schema_version": 1,
                "dataset_id": DATASET_ID,
                "path": base.base._repo_path(path),
                "manifest_sha256": manifest["manifest_sha256"],
                "status": "FROZEN_READY",
                "inspected": False,
                "target_outcomes_observed_or_derived": False,
            }
            base.base._write_json(DEFAULT_STATUS, result)
        elif args.command == "collect-scanner":
            result = _call(
                "collect_scanner",
                manifest_path=args.manifest,
                env_path=args.env,
                max_days=args.max_days,
            )
        else:
            result = _call("status", manifest_path=args.manifest, env_path=args.env)
    except (
        ChallengerAcquisition3Error,
        base.ChallengerAcquisition2Error,
        base.base.ChallengerAcquisitionError,
        LearningDataError,
        ScannerReplayError,
        OSError,
        ValueError,
    ) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}), file=sys.stderr)
        return 2
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

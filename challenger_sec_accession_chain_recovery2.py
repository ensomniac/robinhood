"""Run frozen SEC accession recovery for challenger ORB retest tranche two.

This adapter gives the second challenger tranche an isolated dataset namespace
and exact source-review lineage without modifying either the first challenger
adapter or the previously frozen base recovery engine.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import development_sec_accession_chain_recovery as base
from historical_discovery import HistoricalDiscoveryError
from historical_store import HistoricalStoreError
from learning_data import LearningDataError


PROJECT_ROOT = Path(__file__).resolve().parent
BASE_PATH = Path(base.__file__).resolve()
BASE_IMPLEMENTATION_SHA256 = (
    "e8800c0c20fcd8dfab9f345159fdfe710fe58ef53adf14b7639953c0b00112ac"
)
DATASET_ID = (
    "dataset-development-sec-accession-chain-recovery-2026-07-22-"
    "challenger-orb-retest-tranche2-v1"
)
SOURCE_DATASET_ID = (
    "dataset-development-sec-primary-sources-2026-07-22-"
    "challenger-orb-retest-tranche2-v1"
)
SOURCE_SEMANTICS_DATASET_ID = (
    "dataset-primary-source-semantics-contract-2026-07-22-"
    "challenger-orb-retest-tranche2-v1"
)
SOURCE_REVIEW_DATASET_ID = (
    "dataset-development-sec-source-semantics-2026-07-22-"
    "challenger-orb-retest-tranche2-v1"
)
SOURCE_SEMANTICS_MANIFEST = (
    PROJECT_ROOT
    / "historical_batches/challenger_orb_retest_v1_tranche2/"
    "sec_semantics_manifests"
    / (
        "dataset-development-sec-source-semantics-2026-07-22-"
        "challenger-orb-retest-tranche2-v1-"
        "cd0e30db1b35bcef88772e9c9cf1af7f2a683ecd27cdc2d831674257c9ef784e.json"
    )
)
SOURCE_SEMANTICS_RESULT = (
    PROJECT_ROOT
    / "research_results/"
    "2026-07-22-challenger-orb-retest-sec-source-semantics-tranche2-inspection.json"
)
DEFAULT_OUTPUT_ROOT = (
    PROJECT_ROOT
    / "historical_batches/challenger_orb_retest_v1_tranche2/"
    "sec_accession_chain_manifests"
)
DEFAULT_PUBLIC_STATUS = (
    PROJECT_ROOT
    / "historical_batches/challenger_orb_retest_v1_tranche2/"
    "sec-accession-chain-status.json"
)
DEFAULT_PUBLIC_RESULT = (
    PROJECT_ROOT
    / "research_results/"
    "2026-07-22-challenger-orb-retest-sec-accession-chain-tranche2-inspection.json"
)
PRIVATE_NAMESPACE = (
    "_derived/challenger_orb_retest_v1_tranche2/sec_accession_chain_recovery"
)
EXPECTED_PRIOR_POSITIVES = 18
EXPECTED_UNRESOLVED_PAIRS = 365
EXPECTED_PRIOR_JOINS = 434
EXPECTED_UNIQUE_ACCESSIONS = 425

_BASE_BUILD_SELECTION = base.build_selection


class ChallengerSecAccessionChain2Error(RuntimeError):
    """The second challenger recovery adapter or frozen base has drifted."""


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def build_selection(reviewed: Mapping[str, Any]) -> dict[str, Any]:
    """Apply the base selector with the second tranche's frozen counts."""

    return _BASE_BUILD_SELECTION(
        reviewed,
        expected_pairs=EXPECTED_UNRESOLVED_PAIRS,
        expected_joins=EXPECTED_PRIOR_JOINS,
        expected_accessions=EXPECTED_UNIQUE_ACCESSIONS,
    )


def configure_base() -> None:
    """Install immutable second-tranche constants into the base engine."""

    if _sha256_file(BASE_PATH) != BASE_IMPLEMENTATION_SHA256:
        raise ChallengerSecAccessionChain2Error(
            "frozen base recovery implementation drifted"
        )
    values: dict[str, Any] = {
        "DATASET_ID": DATASET_ID,
        "SOURCE_DATASET_ID": SOURCE_DATASET_ID,
        "SOURCE_SEMANTICS_DATASET_ID": SOURCE_SEMANTICS_DATASET_ID,
        "SOURCE_REVIEW_DATASET_ID": SOURCE_REVIEW_DATASET_ID,
        "SOURCE_SEMANTICS_MANIFEST": SOURCE_SEMANTICS_MANIFEST,
        "SOURCE_SEMANTICS_RESULT": SOURCE_SEMANTICS_RESULT,
        "DEFAULT_OUTPUT_ROOT": DEFAULT_OUTPUT_ROOT,
        "DEFAULT_PUBLIC_STATUS": DEFAULT_PUBLIC_STATUS,
        "DEFAULT_PUBLIC_RESULT": DEFAULT_PUBLIC_RESULT,
        "PRIVATE_NAMESPACE": PRIVATE_NAMESPACE,
        "EXPECTED_PRIOR_POSITIVES": EXPECTED_PRIOR_POSITIVES,
        "EXPECTED_UNRESOLVED_PAIRS": EXPECTED_UNRESOLVED_PAIRS,
        "EXPECTED_PRIOR_JOINS": EXPECTED_PRIOR_JOINS,
        "EXPECTED_UNIQUE_ACCESSIONS": EXPECTED_UNIQUE_ACCESSIONS,
        "build_selection": build_selection,
        "__file__": str(Path(__file__).resolve()),
    }
    for name, value in values.items():
        setattr(base, name, value)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("freeze", "collect", "review", "inspect"))
    parser.add_argument("--manifest", type=Path)
    parser.add_argument("--env-file", type=Path, default=PROJECT_ROOT / ".env")
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--public-status", type=Path, default=DEFAULT_PUBLIC_STATUS)
    parser.add_argument("--public-result", type=Path, default=DEFAULT_PUBLIC_RESULT)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        configure_base()
        if args.command == "freeze":
            path, manifest = base.freeze_inputs(
                env_path=args.env_file,
                output_root=args.output_root,
                status_path=args.public_status,
            )
            value = {"manifest": base._repo_path(path), **manifest}
        elif args.manifest is None:
            raise ChallengerSecAccessionChain2Error("--manifest is required")
        elif args.command == "collect":
            value = base.collect(
                manifest_path=args.manifest,
                env_path=args.env_file,
                status_path=args.public_status,
            )
        elif args.command == "review":
            value = base.review(
                manifest_path=args.manifest,
                env_path=args.env_file,
                status_path=args.public_status,
            )
        else:
            value = base.inspect(
                manifest_path=args.manifest,
                env_path=args.env_file,
                result_path=args.public_result,
            )
    except (
        ChallengerSecAccessionChain2Error,
        base.DevelopmentSecAccessionChainError,
        HistoricalDiscoveryError,
        HistoricalStoreError,
        LearningDataError,
        OSError,
        subprocess.CalledProcessError,
        ValueError,
    ) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}), file=sys.stderr)
        return 2
    print(json.dumps(value, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

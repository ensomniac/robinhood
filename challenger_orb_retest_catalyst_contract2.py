"""Freeze the second ORB-retest tranche's outcome-blind source contract.

The adapter preserves the hash-bound generic source-contract builder while
placing this tranche in its own private namespace and binding both adapters,
the base implementation, and the source-semantics parser before any selected
symbol source is accessed.
"""

from __future__ import annotations

import argparse
import importlib.metadata
import json
import shutil
import subprocess
import sys
from collections.abc import Callable, Mapping, Sequence
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import catalyst_source_semantics as semantics
import development_catalyst_contract as base
import development_sec_submissions as publication_gate
from historical_store import HistoricalStoreConfig, HistoricalStoreError
from learning_data import (
    LearningDataError,
    freeze_dataset_contract,
    load_frozen_dataset_contract,
)


PROJECT_ROOT = Path(__file__).resolve().parent
BASE_BUILDER = PROJECT_ROOT / "development_catalyst_contract.py"
INSPECTOR = PROJECT_ROOT / "challenger_orb_retest_catalyst_contract2_inspection.py"
DATASET_ID = (
    "dataset-primary-source-semantics-contract-2026-07-22-"
    "challenger-orb-retest-tranche2-v1"
)
SELECTION_DATASET_ID = (
    "dataset-selected-candidate-contract-2026-07-22-"
    "challenger-orb-retest-tranche2-v1"
)
PRIVATE_NAMESPACE = "_derived/challenger_orb_retest_v1_tranche2/catalyst_sources"
ROOT = PROJECT_ROOT / "historical_batches/challenger_orb_retest_v1_tranche2"
SOURCE_MANIFEST = (
    ROOT
    / "selected_pair_manifests"
    / (
        f"{SELECTION_DATASET_ID}-"
        "9cdb9fd014870b7518613ee096922dd838a1c9e7094c3f6bd4e7b392ae95a511.json"
    )
)
SCANNER_MANIFEST = (
    ROOT
    / "scanner_manifests"
    / (
        "dataset-production-scanner-replay-2026-07-21-"
        "challenger-orb-retest-tranche2-v1-"
        "5805ca2d6d3e1df680b0793855e3c64867a596b30e920b6f47c372df3c92da99.json"
    )
)
SCANNER_SUMMARY = (
    PROJECT_ROOT
    / "research_results/2026-07-22-challenger-orb-retest-scanner-tranche2-v1.json"
)
SCANNER_INSPECTION = (
    PROJECT_ROOT
    / (
        "research_results/2026-07-22-challenger-orb-retest-scanner-"
        "tranche2-v1-inspection.json"
    )
)
SECURITY_MASTER_SOURCE = ROOT / "security-master-source.json"
STRATEGY_SOURCE = (
    PROJECT_ROOT / "historical_batches/scanner_expansion/production-strategy-source.json"
)
DEFAULT_OUTPUT_ROOT = ROOT / "catalyst_manifests"
DEFAULT_PUBLIC_STATUS = ROOT / "catalyst-contract-status.json"
DEFAULT_SELECTION_DOC = PROJECT_ROOT / "CHALLENGER_ORB_RETEST.md"
_BASE_STABLE_CONTRACT = base._stable_contract


class ChallengerCatalystContract2Error(RuntimeError):
    """The second-tranche source contract cannot be proven safely."""


def source_paths(
    *,
    source_manifest_path: Path = SOURCE_MANIFEST,
    scanner_manifest_path: Path = SCANNER_MANIFEST,
    scanner_summary_path: Path = SCANNER_SUMMARY,
    scanner_inspection_path: Path = SCANNER_INSPECTION,
    security_master_source_path: Path = SECURITY_MASTER_SOURCE,
    strategy_source_path: Path = STRATEGY_SOURCE,
    selection_doc_path: Path = DEFAULT_SELECTION_DOC,
) -> dict[str, Path]:
    return {
        "selected_pair_manifest": source_manifest_path,
        "scanner_manifest": scanner_manifest_path,
        "scanner_summary": scanner_summary_path,
        "scanner_inspection": scanner_inspection_path,
        "security_master_source": security_master_source_path,
        "strategy_source": strategy_source_path,
        "selection_doc": selection_doc_path,
    }


def implementation_paths() -> dict[str, Path]:
    return {
        "contract_adapter": Path(__file__),
        "base_contract_builder": BASE_BUILDER,
        "inspector_adapter": INSPECTOR,
        "source_semantics": Path(semantics.__file__),
    }


def _implementation_contract() -> dict[str, Any]:
    return {
        "files": {
            name: {"path": base._repo_path(path), "sha256": base._sha256_file(path)}
            for name, path in implementation_paths().items()
        },
        "parser_version": semantics.PARSER_VERSION,
        "dependencies": {
            "python": sys.version.split()[0],
            "pypdf": importlib.metadata.version("pypdf"),
            "requests": importlib.metadata.version("requests"),
        },
    }


@contextmanager
def configured_base():
    values = {
        "DATASET_ID": DATASET_ID,
        "SOURCE_MANIFEST": SOURCE_MANIFEST,
        "SCANNER_MANIFEST": SCANNER_MANIFEST,
        "SCANNER_SUMMARY": SCANNER_SUMMARY,
        "SCANNER_INSPECTION": SCANNER_INSPECTION,
        "SECURITY_MASTER_SOURCE": SECURITY_MASTER_SOURCE,
        "STRATEGY_SOURCE": STRATEGY_SOURCE,
        "DEFAULT_OUTPUT_ROOT": DEFAULT_OUTPUT_ROOT,
        "DEFAULT_PUBLIC_STATUS": DEFAULT_PUBLIC_STATUS,
        "DEFAULT_SELECTION_DOC": DEFAULT_SELECTION_DOC,
        "PRIVATE_NAMESPACE": PRIVATE_NAMESPACE,
        "_implementation_contract": _implementation_contract,
    }
    original = {name: getattr(base, name) for name in values}
    try:
        for name, value in values.items():
            setattr(base, name, value)
        yield base
    finally:
        for name, value in original.items():
            setattr(base, name, value)


def stable_contract(
    *,
    source_manifest_path: Path,
    env_path: Path,
    dataset_id: str = DATASET_ID,
    scanner_manifest_path: Path = SCANNER_MANIFEST,
    scanner_summary_path: Path = SCANNER_SUMMARY,
    scanner_inspection_path: Path = SCANNER_INSPECTION,
    security_master_source_path: Path = SECURITY_MASTER_SOURCE,
    strategy_source_path: Path = STRATEGY_SOURCE,
) -> tuple[dict[str, Any], HistoricalStoreConfig]:
    with configured_base():
        return _BASE_STABLE_CONTRACT(
            source_manifest_path=source_manifest_path,
            env_path=env_path,
            dataset_id=dataset_id,
            scanner_manifest_path=scanner_manifest_path,
            scanner_summary_path=scanner_summary_path,
            scanner_inspection_path=scanner_inspection_path,
            security_master_source_path=security_master_source_path,
            strategy_source_path=strategy_source_path,
        )


def _published(path: Path) -> Mapping[str, str]:
    try:
        return publication_gate._published_source(path)
    except (
        publication_gate.DevelopmentSecSubmissionsError,
        subprocess.SubprocessError,
    ) as exc:
        raise ChallengerCatalystContract2Error(str(exc)) from exc


def publication_contract(
    paths: Mapping[str, Path],
    *,
    require_published: bool,
    publication_checker: Callable[[Path], Mapping[str, str]],
) -> dict[str, dict[str, str]]:
    if not require_published:
        return {}
    return {
        name: dict(publication_checker(path))
        for name, path in {**implementation_paths(), **paths}.items()
    }


def freeze_contract(
    *,
    source_manifest_path: Path = SOURCE_MANIFEST,
    env_path: Path = PROJECT_ROOT / ".env",
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    dataset_id: str = DATASET_ID,
    scanner_manifest_path: Path = SCANNER_MANIFEST,
    scanner_summary_path: Path = SCANNER_SUMMARY,
    scanner_inspection_path: Path = SCANNER_INSPECTION,
    security_master_source_path: Path = SECURITY_MASTER_SOURCE,
    strategy_source_path: Path = STRATEGY_SOURCE,
    selection_doc_path: Path = DEFAULT_SELECTION_DOC,
    require_published: bool = True,
    publication_checker: Callable[[Path], Mapping[str, str]] = _published,
) -> tuple[Path, dict[str, Any]]:
    if dataset_id != DATASET_ID:
        raise ChallengerCatalystContract2Error("second-tranche source identity differs")
    stable, config = stable_contract(
        source_manifest_path=source_manifest_path,
        env_path=env_path,
        dataset_id=dataset_id,
        scanner_manifest_path=scanner_manifest_path,
        scanner_summary_path=scanner_summary_path,
        scanner_inspection_path=scanner_inspection_path,
        security_master_source_path=security_master_source_path,
        strategy_source_path=strategy_source_path,
    )
    paths = source_paths(
        source_manifest_path=source_manifest_path,
        scanner_manifest_path=scanner_manifest_path,
        scanner_summary_path=scanner_summary_path,
        scanner_inspection_path=scanner_inspection_path,
        security_master_source_path=security_master_source_path,
        strategy_source_path=strategy_source_path,
        selection_doc_path=selection_doc_path,
    )
    publication = publication_contract(
        paths,
        require_published=require_published,
        publication_checker=publication_checker,
    )
    matches = sorted(output_root.glob(f"{dataset_id}-*.json"))
    if len(matches) > 1:
        raise ChallengerCatalystContract2Error("source contract has multiple manifests")
    if matches:
        existing = load_frozen_dataset_contract(matches[0])
        expected = {**stable, "publication_contract": publication}
        if any(existing.get(key) != value for key, value in expected.items()):
            raise ChallengerCatalystContract2Error("existing source contract drifted")
        return matches[0], existing
    usage = shutil.disk_usage(config.root)
    if usage.free < config.min_free_bytes:
        raise ChallengerCatalystContract2Error(
            "historical-store reserve is unavailable"
        )
    contract = {
        "schema_version": 1,
        "dataset_id": dataset_id,
        "registered_at": datetime.now(UTC).isoformat(),
        "requested_dates": [
            row["date"] for row in stable["selection_contract"]["daily_shortlists"]
        ],
        "dataset_payload": {
            "lane": "development",
            "claim_scope": "DEVELOPMENT_ONLY",
            "status": "COLLECTING",
            "evidence_paths": [
                base._repo_path(selection_doc_path),
                base._repo_path(source_manifest_path),
                base._repo_path(scanner_inspection_path),
                "PRODUCTION_STRATEGY_VALIDATION.md",
            ],
            "inspected": False,
        },
        **stable,
        "publication_contract": publication,
        "capacity_contract": {
            "historical_store_outside_repository": not config.root.resolve().is_relative_to(
                PROJECT_ROOT.resolve()
            ),
            "free_bytes_at_freeze": usage.free,
            "reserve_bytes": config.min_free_bytes,
            "capacity_ready": True,
            "historical_deletion_allowed": False,
        },
    }
    return freeze_dataset_contract(contract, output_root)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env", type=Path, default=PROJECT_ROOT / ".env")
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        path, manifest = freeze_contract(env_path=args.env, output_root=args.output_root)
        print(
            json.dumps(
                {
                    "dataset_id": manifest["dataset_id"],
                    "manifest_sha256": manifest["manifest_sha256"],
                    "path": base._repo_path(path),
                    "selected_pair_count": manifest["selection_contract"][
                        "selected_pair_count"
                    ],
                    "target_sources_accessed": False,
                    "target_outcomes_observed_or_derived": False,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    except (
        ChallengerCatalystContract2Error,
        base.DevelopmentCatalystContractError,
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

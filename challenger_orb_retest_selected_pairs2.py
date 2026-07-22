"""Freeze the second ORB-retest tranche's exact private scanner selection.

This adapter keeps the already hash-bound first-corpus freezer immutable.  It
reuses that proven validation logic while binding this adapter, the base
implementation, and a tranche-specific independent inspector into the new
manifest.  Exact date-security identities remain outside Git and outcomes stay
closed.
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
from collections.abc import Callable, Mapping, Sequence
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import challenger_orb_retest_selected_pairs as base
from historical_store import HistoricalStoreConfig, HistoricalStoreError
from learning_data import (
    LearningDataError,
    freeze_dataset_contract,
    load_frozen_dataset_contract,
)
from learning_experiment import LearningExperimentError


PROJECT_ROOT = Path(__file__).resolve().parent
BASE_FREEZER = PROJECT_ROOT / "challenger_orb_retest_selected_pairs.py"
BASE_INSPECTOR = PROJECT_ROOT / "challenger_orb_retest_selected_pairs_inspection.py"
INSPECTOR = PROJECT_ROOT / "challenger_orb_retest_selected_pairs2_inspection.py"
SELECTION_PRIMITIVE = PROJECT_ROOT / "scanner_selected_pairs.py"
TRIGGER = PROJECT_ROOT / "challenger_orb_retest.py"

DATASET_ID = (
    "dataset-selected-candidate-contract-2026-07-22-"
    "challenger-orb-retest-tranche2-v1"
)
PREENTRY_DATASET_ID = (
    "dataset-challenger-orb-retest-preentry-collection-2026-07-22-tranche2-v1"
)
SCANNER_DATASET_ID = (
    "dataset-production-scanner-replay-2026-07-21-"
    "challenger-orb-retest-tranche2-v1"
)
EXPECTED_SELECTED_PAIRS = 1826
EXPECTED_DATES = 100
MINIMUM_FREE_BYTES = 20 * 1024**3
SCANNER_MANIFEST_SHA256 = (
    "5805ca2d6d3e1df680b0793855e3c64867a596b30e920b6f47c372df3c92da99"
)
OUTER_MANIFEST_SHA256 = (
    "3dbdef70b6b9a5f95599d55dc843e1dd9620e144daacc53219dc7eadb9d0289c"
)
HYPOTHESIS_SHA256 = "b766000bc84aff7ae836c8dcdc516d4b1c7dc4fb3dcb5f179bb5eb452656d105"
PRIMARY_TRIAL_ID = "trial-8a93c0ab15c85248"

ROOT = PROJECT_ROOT / "historical_batches/challenger_orb_retest_v1_tranche2"
DEFAULT_SUMMARY = (
    PROJECT_ROOT
    / "research_results/2026-07-22-challenger-orb-retest-scanner-tranche2-v1.json"
)
DEFAULT_INSPECTION = (
    PROJECT_ROOT
    / (
        "research_results/2026-07-22-challenger-orb-retest-scanner-"
        "tranche2-v1-inspection.json"
    )
)
DEFAULT_SCANNER_MANIFEST = (
    ROOT
    / "scanner_manifests"
    / (
        "dataset-production-scanner-replay-2026-07-21-"
        "challenger-orb-retest-tranche2-v1-"
        f"{SCANNER_MANIFEST_SHA256}.json"
    )
)
DEFAULT_OUTER_MANIFEST = (
    ROOT
    / "acquisition_manifests"
    / (
        "dataset-challenger-orb-retest-acquisition-2026-07-21-tranche2-v1-"
        f"{OUTER_MANIFEST_SHA256}.json"
    )
)
DEFAULT_HYPOTHESIS = (
    PROJECT_ROOT
    / "learning/hypotheses"
    / (
        "experiment-catalyst-orb-retest-v1-"
        f"{HYPOTHESIS_SHA256}.json"
    )
)
DEFAULT_OUTPUT_ROOT = ROOT / "selected_pair_manifests"
DEFAULT_STATUS = ROOT / "selected-pair-contract-status.json"


class ChallengerSelectedPairs2Error(RuntimeError):
    """The second-tranche selected-pair boundary is incomplete or drifted."""


def source_paths() -> dict[str, Path]:
    return {
        "scanner_summary": DEFAULT_SUMMARY,
        "scanner_inspection": DEFAULT_INSPECTION,
        "scanner_manifest": DEFAULT_SCANNER_MANIFEST,
        "outer_manifest": DEFAULT_OUTER_MANIFEST,
        "hypothesis": DEFAULT_HYPOTHESIS,
    }


def implementation_paths() -> dict[str, Path]:
    return {
        "freezer": Path(__file__),
        "base_freezer": BASE_FREEZER,
        "inspector": INSPECTOR,
        "base_inspector": BASE_INSPECTOR,
        "selection_primitive": SELECTION_PRIMITIVE,
        "trigger": TRIGGER,
    }


@contextmanager
def configured_base():
    """Scope this tranche's identities around the immutable base validator."""

    values = {
        "DATASET_ID": DATASET_ID,
        "PREENTRY_DATASET_ID": PREENTRY_DATASET_ID,
        "SCANNER_DATASET_ID": SCANNER_DATASET_ID,
        "EXPECTED_SELECTED_PAIRS": EXPECTED_SELECTED_PAIRS,
        "EXPECTED_DATES": EXPECTED_DATES,
        "MINIMUM_FREE_BYTES": MINIMUM_FREE_BYTES,
        "SCANNER_MANIFEST_SHA256": SCANNER_MANIFEST_SHA256,
        "OUTER_MANIFEST_SHA256": OUTER_MANIFEST_SHA256,
        "HYPOTHESIS_SHA256": HYPOTHESIS_SHA256,
        "PRIMARY_TRIAL_ID": PRIMARY_TRIAL_ID,
        "DEFAULT_SUMMARY": DEFAULT_SUMMARY,
        "DEFAULT_INSPECTION": DEFAULT_INSPECTION,
        "DEFAULT_SCANNER_MANIFEST": DEFAULT_SCANNER_MANIFEST,
        "DEFAULT_OUTER_MANIFEST": DEFAULT_OUTER_MANIFEST,
        "DEFAULT_HYPOTHESIS": DEFAULT_HYPOTHESIS,
        "DEFAULT_OUTPUT_ROOT": DEFAULT_OUTPUT_ROOT,
        "DEFAULT_STATUS": DEFAULT_STATUS,
    }
    original = {name: getattr(base, name) for name in values}
    try:
        for name, value in values.items():
            setattr(base, name, value)
        yield base
    finally:
        for name, value in original.items():
            setattr(base, name, value)


def _validated_selection(
    *,
    dataset_id: str,
    summary_path: Path,
    inspection_path: Path,
    scanner_manifest_path: Path,
    outer_manifest_path: Path,
    hypothesis_path: Path,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    with configured_base():
        return base._validate_sources(
            dataset_id=dataset_id,
            summary_path=summary_path,
            inspection_path=inspection_path,
            scanner_manifest_path=scanner_manifest_path,
            outer_manifest_path=outer_manifest_path,
            hypothesis_path=hypothesis_path,
        )


def _private_path(store_root: Path, dataset_id: str) -> Path:
    return (
        store_root
        / "_derived/scanner_selected_pairs"
        / dataset_id
        / "selected-pairs.json.gz"
    )


def _downstream_artifacts(store_root: Path) -> list[Path]:
    root = (
        store_root
        / "_derived/challenger_orb_retest_preentry"
        / PREENTRY_DATASET_ID
    )
    return sorted(path for path in root.rglob("*") if path.is_file()) if root.exists() else []


def freeze_selected_pairs(
    *,
    dataset_id: str = DATASET_ID,
    summary_path: Path = DEFAULT_SUMMARY,
    inspection_path: Path = DEFAULT_INSPECTION,
    scanner_manifest_path: Path = DEFAULT_SCANNER_MANIFEST,
    outer_manifest_path: Path = DEFAULT_OUTER_MANIFEST,
    hypothesis_path: Path = DEFAULT_HYPOTHESIS,
    env_path: Path = PROJECT_ROOT / ".env",
    output_root: Path = DEFAULT_OUTPUT_ROOT,
    status_path: Path = DEFAULT_STATUS,
    require_published: bool = True,
    publication_checker: Callable[[Path], Mapping[str, str]] = base._published,
) -> tuple[Path, dict[str, Any]]:
    if dataset_id != DATASET_ID:
        raise ChallengerSelectedPairs2Error("second-tranche dataset identity differs")
    private, public, hypothesis = _validated_selection(
        dataset_id=dataset_id,
        summary_path=summary_path,
        inspection_path=inspection_path,
        scanner_manifest_path=scanner_manifest_path,
        outer_manifest_path=outer_manifest_path,
        hypothesis_path=hypothesis_path,
    )
    config = HistoricalStoreConfig.from_env(env_path)
    free_bytes = shutil.disk_usage(config.root).free
    reserve_bytes = max(MINIMUM_FREE_BYTES, config.min_free_bytes)
    if free_bytes < reserve_bytes:
        raise ChallengerSelectedPairs2Error("historical store reserve is not available")
    if _downstream_artifacts(config.root):
        raise ChallengerSelectedPairs2Error(
            "second-tranche downstream artifacts existed before pair freeze"
        )
    private_path = _private_path(config.root, dataset_id)
    if private_path.exists():
        if base._sha256_json(base._read_gzip(private_path)) != base._sha256_json(private):
            raise ChallengerSelectedPairs2Error("private selected pairs changed")
    else:
        base.selected_pairs._write_private(private_path, private)

    public = {
        **public,
        "source_inspection_sha256": base._sha256_file(inspection_path),
        "source_outer_manifest_sha256": base._sha256_file(outer_manifest_path),
        "hypothesis_contract_sha256": hypothesis["contract_sha256"],
        "primary_trial_id": hypothesis["primary_trial_id"],
    }
    sources = {
        "scanner_summary": summary_path,
        "scanner_inspection": inspection_path,
        "scanner_manifest": scanner_manifest_path,
        "outer_manifest": outer_manifest_path,
        "hypothesis": hypothesis_path,
    }
    implementations = implementation_paths()
    publication = (
        {
            name: dict(publication_checker(path))
            for name, path in {**implementations, **sources}.items()
        }
        if require_published
        else {}
    )
    mechanism = {
        "rule_version": "catalyst-orb-retest-v1",
        "hypothesis_contract_sha256": hypothesis["contract_sha256"],
        "primary_trial_id": hypothesis["primary_trial_id"],
        "initial_break_entry_allowed": False,
        "retest_rule": (
            "first completed later bar touches opening high and closes at or above"
        ),
        "entry_trigger": "first condition-valid rebreak of held retest-bar high",
        "decision_snapshot_seconds_after_rebreak": 10,
        "entry_cutoff_et": "10:30:00",
        "one_trial_no_parameter_selection": True,
    }
    permitted = {
        "selected_symbols_only": True,
        "point_in_time_primary_source_evidence": True,
        "condition_valid_sip_trades_through_terminal_boundary": True,
        "completed_noninterpolated_retest_bars": True,
        "quote_book_and_tape_through_final_decision": True,
        "preentry_structure_benchmark_halt_and_liquidity": True,
        "full_universe_selected_detail": False,
        "post_entry_rows": False,
        "returns_or_outcomes": False,
    }
    outcome_lock = {
        "post_entry_data_access_allowed": False,
        "return_fields_allowed": False,
        "target_outcomes_observed_or_derived": False,
        "outcome_contract_must_be_separately_frozen": True,
        "date_symbol_provider_or_missing_input_substitution_allowed": False,
    }
    downstream_zero = {
        "dataset_id": PREENTRY_DATASET_ID,
        "pre_freeze_artifact_count": 0,
        "private_location": (
            "LOCAL_HISTORICAL_DATA_ROOT/_derived/"
            f"challenger_orb_retest_preentry/{PREENTRY_DATASET_ID}"
        ),
    }
    contract = {
        "schema_version": 1,
        "dataset_id": dataset_id,
        "registered_at": datetime.now(UTC).isoformat(),
        "requested_dates": public["requested_dates"],
        "dataset_payload": {
            "lane": "development",
            "claim_scope": "DEVELOPMENT_ONLY",
            "status": "FROZEN",
            "evidence_paths": [
                base._repo_path(summary_path),
                base._repo_path(inspection_path),
                base._repo_path(hypothesis_path),
                "CHALLENGER_ORB_RETEST.md",
                "PRODUCTION_STRATEGY_VALIDATION.md",
            ],
            "inspected": False,
        },
        "selection_contract": public,
        "mechanism_contract": mechanism,
        "permitted_next_inputs": permitted,
        "outcome_lock": outcome_lock,
        "downstream_zero_state": downstream_zero,
        "capacity_contract": {
            "minimum_free_bytes": MINIMUM_FREE_BYTES,
            "configured_reserve_bytes": config.min_free_bytes,
            "enforced_reserve_bytes": reserve_bytes,
            "free_bytes_at_freeze": free_bytes,
            "capacity_ready": True,
            "historical_store_outside_repository": True,
            "historical_deletion_allowed": False,
        },
        "source_bindings": {name: base._binding(path) for name, path in sources.items()},
        "implementation_contract": {
            name: base._binding(path) for name, path in implementations.items()
        },
        "publication_contract": publication,
    }
    existing = sorted(output_root.glob(f"{dataset_id}-*.json"))
    if len(existing) > 1:
        raise ChallengerSelectedPairs2Error("multiple selected-pair manifests exist")
    if existing:
        manifest = load_frozen_dataset_contract(existing[0])
        immutable_fields = (
            "dataset_id",
            "requested_dates",
            "dataset_payload",
            "selection_contract",
            "mechanism_contract",
            "permitted_next_inputs",
            "outcome_lock",
            "downstream_zero_state",
            "source_bindings",
            "implementation_contract",
            "publication_contract",
        )
        if (
            any(manifest.get(field) != contract[field] for field in immutable_fields)
            or manifest.get("capacity_contract", {}).get("minimum_free_bytes")
            != MINIMUM_FREE_BYTES
            or manifest.get("capacity_contract", {}).get("configured_reserve_bytes")
            != config.min_free_bytes
            or manifest.get("capacity_contract", {}).get("enforced_reserve_bytes")
            != reserve_bytes
            or manifest.get("capacity_contract", {}).get("capacity_ready") is not True
            or manifest.get("capacity_contract", {}).get(
                "historical_store_outside_repository"
            )
            is not True
            or manifest.get("capacity_contract", {}).get("historical_deletion_allowed")
            is not False
        ):
            raise ChallengerSelectedPairs2Error("existing selected-pair contract differs")
        path = existing[0]
    else:
        path, manifest = freeze_dataset_contract(contract, output_root)
    base._write_json(
        status_path,
        {
            "schema_version": 1,
            "dataset_id": dataset_id,
            "manifest_path": base._repo_path(path),
            "manifest_sha256": manifest["manifest_sha256"],
            "status": "FROZEN_AWAITING_INSPECTION",
            "inspected": False,
            "selected_pair_count": EXPECTED_SELECTED_PAIRS,
            "requested_dates": EXPECTED_DATES,
            "pre_freeze_downstream_artifacts": 0,
            "target_outcomes_observed_or_derived": False,
        },
    )
    return path, manifest


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env", type=Path, default=PROJECT_ROOT / ".env")
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--status", type=Path, default=DEFAULT_STATUS)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        path, manifest = freeze_selected_pairs(
            env_path=args.env,
            output_root=args.output_root,
            status_path=args.status,
        )
        print(
            json.dumps(
                {
                    "dataset_id": manifest["dataset_id"],
                    "manifest_sha256": manifest["manifest_sha256"],
                    "path": base._repo_path(path),
                    "selected_pair_count": manifest["selection_contract"][
                        "selected_pair_count"
                    ],
                    "target_outcomes_observed_or_derived": False,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    except (
        ChallengerSelectedPairs2Error,
        base.ChallengerSelectedPairsError,
        HistoricalStoreError,
        LearningDataError,
        LearningExperimentError,
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

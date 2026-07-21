"""Freeze the exact private shortlist for the ORB retest challenger.

This boundary consumes only the independently inspected 09:35 scanner result.
Exact date-symbol identities remain in the external historical store.  The
public manifest contains counts and hashes and authorizes only a separately
frozen, outcome-blind causal pre-entry acquisition contract.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import shutil
import subprocess
import sys
from collections.abc import Callable, Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import development_sec_submissions as publication_gate
from learning_experiment import LearningExperimentError, load_hypothesis_contract
import scanner_selected_pairs as selected_pairs
from historical_store import HistoricalStoreConfig, HistoricalStoreError
from learning_data import (
    LearningDataError,
    freeze_dataset_contract,
    load_frozen_dataset_contract,
)


PROJECT_ROOT = Path(__file__).resolve().parent
DATASET_ID = "dataset-selected-candidate-contract-2026-07-21-challenger-orb-retest-v1"
PREENTRY_DATASET_ID = "dataset-challenger-orb-retest-preentry-collection-2026-07-21-v1"
SCANNER_DATASET_ID = (
    "dataset-production-scanner-replay-2026-07-21-challenger-orb-retest-v2"
)
EXPECTED_SELECTED_PAIRS = 1826
EXPECTED_DATES = 100
MINIMUM_FREE_BYTES = 20 * 1024**3
SCANNER_MANIFEST_SHA256 = (
    "77dc80b560b67aaf3ab3991e7a67ffef0cb344ab79ece4e79e868d4b50b96640"
)
OUTER_MANIFEST_SHA256 = (
    "b580483bc8150725018edb74534a2884d248108737fcd83c5af6e63269f9d8e9"
)
HYPOTHESIS_SHA256 = "b766000bc84aff7ae836c8dcdc516d4b1c7dc4fb3dcb5f179bb5eb452656d105"
PRIMARY_TRIAL_ID = "trial-8a93c0ab15c85248"
DEFAULT_SUMMARY = (
    PROJECT_ROOT / "research_results/2026-07-21-challenger-orb-retest-scanner-v2.json"
)
DEFAULT_INSPECTION = (
    PROJECT_ROOT
    / "research_results/2026-07-21-challenger-orb-retest-scanner-v2-inspection.json"
)
DEFAULT_SCANNER_MANIFEST = (
    PROJECT_ROOT
    / "historical_batches/challenger_orb_retest_v1/scanner_manifests"
    / (
        "dataset-production-scanner-replay-2026-07-21-challenger-orb-retest-v2-"
        "77dc80b560b67aaf3ab3991e7a67ffef0cb344ab79ece4e79e868d4b50b96640.json"
    )
)
DEFAULT_OUTER_MANIFEST = (
    PROJECT_ROOT
    / "historical_batches/challenger_orb_retest_v1/acquisition_manifests"
    / (
        "dataset-challenger-orb-retest-acquisition-2026-07-21-v2-"
        "b580483bc8150725018edb74534a2884d248108737fcd83c5af6e63269f9d8e9.json"
    )
)
DEFAULT_HYPOTHESIS = (
    PROJECT_ROOT
    / "learning/hypotheses"
    / (
        "experiment-catalyst-orb-retest-v1-"
        "b766000bc84aff7ae836c8dcdc516d4b1c7dc4fb3dcb5f179bb5eb452656d105.json"
    )
)
DEFAULT_OUTPUT_ROOT = (
    PROJECT_ROOT / "historical_batches/challenger_orb_retest_v1/selected_pair_manifests"
)
DEFAULT_STATUS = (
    PROJECT_ROOT
    / "historical_batches/challenger_orb_retest_v1/selected-pair-contract-status.json"
)


class ChallengerSelectedPairsError(RuntimeError):
    """The challenger selection boundary is incomplete or has drifted."""


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


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ChallengerSelectedPairsError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise ChallengerSelectedPairsError(f"{path} must contain an object")
    return value


def _read_gzip(path: Path) -> dict[str, Any]:
    try:
        with gzip.open(path, "rt", encoding="utf-8") as source:
            value = json.load(source)
    except (OSError, json.JSONDecodeError) as exc:
        raise ChallengerSelectedPairsError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise ChallengerSelectedPairsError(f"{path} must contain an object")
    return value


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


def _repo_path(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(PROJECT_ROOT))
    except ValueError as exc:
        raise ChallengerSelectedPairsError(
            f"public path must be repository relative: {path}"
        ) from exc


def _binding(path: Path) -> dict[str, str]:
    return {"path": _repo_path(path), "sha256": _sha256_file(path)}


def _published(path: Path) -> dict[str, str]:
    try:
        return publication_gate._published_source(path)
    except (
        publication_gate.DevelopmentSecSubmissionsError,
        subprocess.SubprocessError,
    ) as exc:
        raise ChallengerSelectedPairsError(str(exc)) from exc


def _private_path(store_root: Path, dataset_id: str) -> Path:
    return (
        store_root
        / "_derived/scanner_selected_pairs"
        / dataset_id
        / "selected-pairs.json.gz"
    )


def _downstream_root(store_root: Path) -> Path:
    return store_root / "_derived/challenger_orb_retest_preentry" / PREENTRY_DATASET_ID


def _target_artifacts(store_root: Path) -> list[Path]:
    root = _downstream_root(store_root)
    return (
        sorted(path for path in root.rglob("*") if path.is_file())
        if root.exists()
        else []
    )


def _validate_sources(
    *,
    dataset_id: str,
    summary_path: Path,
    inspection_path: Path,
    scanner_manifest_path: Path,
    outer_manifest_path: Path,
    hypothesis_path: Path,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    scanner_manifest = load_frozen_dataset_contract(scanner_manifest_path)
    outer_manifest = load_frozen_dataset_contract(outer_manifest_path)
    summary = _read_json(summary_path)
    inspection = _read_json(inspection_path)
    try:
        hypothesis = load_hypothesis_contract(hypothesis_path)
    except LearningExperimentError as exc:
        raise ChallengerSelectedPairsError(str(exc)) from exc
    if (
        scanner_manifest.get("dataset_id") != SCANNER_DATASET_ID
        or scanner_manifest.get("manifest_sha256") != SCANNER_MANIFEST_SHA256
        or outer_manifest.get("manifest_sha256") != OUTER_MANIFEST_SHA256
        or outer_manifest.get("full_universe_market_contract", {}).get(
            "scanner_manifest_sha256"
        )
        != scanner_manifest.get("manifest_sha256")
    ):
        raise ChallengerSelectedPairsError("challenger scanner lineage differs")
    detail = summary.get("detailed_artifact")
    if not isinstance(detail, Mapping):
        raise ChallengerSelectedPairsError("scanner private-detail binding is missing")
    if (
        summary.get("dataset_id") != SCANNER_DATASET_ID
        or summary.get("status") != "READY"
        or summary.get("source", {}).get("contract_sha256")
        != scanner_manifest.get("manifest_sha256")
        or len(summary.get("requested_dates", [])) != EXPECTED_DATES
        or inspection.get("dataset_id") != SCANNER_DATASET_ID
        or inspection.get("status") != "INSPECTED"
        or inspection.get("valid") is not True
        or inspection.get("manifest_sha256") != scanner_manifest.get("manifest_sha256")
        or inspection.get("summary_sha256") != _sha256_file(summary_path)
        or inspection.get("detail_sha256") != detail.get("sha256")
        or inspection.get("completed_dates") != EXPECTED_DATES
        or inspection.get("total_selected") != EXPECTED_SELECTED_PAIRS
    ):
        raise ChallengerSelectedPairsError("scanner result is not independently ready")
    if (
        hypothesis.get("contract_sha256") != HYPOTHESIS_SHA256
        or hypothesis.get("primary_trial_id") != PRIMARY_TRIAL_ID
        or hypothesis.get("falsification_criteria", {}).get("minimum_closed_signals")
        != 50
    ):
        raise ChallengerSelectedPairsError("challenger hypothesis differs")
    private, public = selected_pairs.load_scanner_selection(
        summary_path, dataset_id=dataset_id
    )
    selected = private.get("selected_pairs")
    if (
        private.get("selected_pair_count") != EXPECTED_SELECTED_PAIRS
        or public.get("selected_pair_count") != EXPECTED_SELECTED_PAIRS
        or not isinstance(selected, list)
        or len({(row["date"], row["instrument_id"]) for row in selected})
        != EXPECTED_SELECTED_PAIRS
        or len(public.get("requested_dates", [])) != EXPECTED_DATES
    ):
        raise ChallengerSelectedPairsError("private scanner selection is incomplete")
    return private, public, hypothesis


def freeze_selected_pairs(
    *,
    dataset_id: str,
    summary_path: Path,
    inspection_path: Path,
    scanner_manifest_path: Path,
    outer_manifest_path: Path,
    hypothesis_path: Path,
    env_path: Path,
    output_root: Path,
    status_path: Path,
    require_published: bool = True,
    publication_checker: Callable[[Path], Mapping[str, str]] = _published,
) -> tuple[Path, dict[str, Any]]:
    if not dataset_id.startswith("dataset-selected-candidate-contract-"):
        raise ChallengerSelectedPairsError("selected-pair dataset namespace is invalid")
    private, public, hypothesis = _validate_sources(
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
        raise ChallengerSelectedPairsError("historical store reserve is not available")
    downstream_artifacts = _target_artifacts(config.root)
    if downstream_artifacts:
        raise ChallengerSelectedPairsError(
            "challenger selected-detail artifacts existed before pair freeze"
        )
    private_path = _private_path(config.root, dataset_id)
    if private_path.exists():
        if _sha256_json(_read_gzip(private_path)) != _sha256_json(private):
            raise ChallengerSelectedPairsError("private selected pairs changed")
    else:
        selected_pairs._write_private(private_path, private)
    public = {
        **public,
        "source_inspection_sha256": _sha256_file(inspection_path),
        "source_outer_manifest_sha256": _sha256_file(outer_manifest_path),
        "hypothesis_contract_sha256": hypothesis["contract_sha256"],
        "primary_trial_id": hypothesis["primary_trial_id"],
    }
    implementation_paths = {
        "freezer": Path(__file__),
        "inspector": PROJECT_ROOT
        / "challenger_orb_retest_selected_pairs_inspection.py",
        "selection_primitive": PROJECT_ROOT / "scanner_selected_pairs.py",
        "trigger": PROJECT_ROOT / "challenger_orb_retest.py",
    }
    source_paths = {
        "scanner_summary": summary_path,
        "scanner_inspection": inspection_path,
        "scanner_manifest": scanner_manifest_path,
        "outer_manifest": outer_manifest_path,
        "hypothesis": hypothesis_path,
    }
    publication = (
        {
            name: dict(publication_checker(path))
            for name, path in {**implementation_paths, **source_paths}.items()
        }
        if require_published
        else {}
    )
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
                _repo_path(summary_path),
                _repo_path(inspection_path),
                _repo_path(hypothesis_path),
                "CHALLENGER_ORB_RETEST.md",
                "PRODUCTION_STRATEGY_VALIDATION.md",
            ],
            "inspected": False,
        },
        "selection_contract": public,
        "mechanism_contract": {
            "rule_version": "catalyst-orb-retest-v1",
            "hypothesis_contract_sha256": hypothesis["contract_sha256"],
            "primary_trial_id": hypothesis["primary_trial_id"],
            "initial_break_entry_allowed": False,
            "retest_rule": "first completed later bar touches opening high and closes at or above",
            "entry_trigger": "first condition-valid rebreak of held retest-bar high",
            "decision_snapshot_seconds_after_rebreak": 10,
            "entry_cutoff_et": "10:30:00",
            "one_trial_no_parameter_selection": True,
        },
        "permitted_next_inputs": {
            "selected_symbols_only": True,
            "point_in_time_primary_source_evidence": True,
            "condition_valid_sip_trades_through_terminal_boundary": True,
            "completed_noninterpolated_retest_bars": True,
            "quote_book_and_tape_through_final_decision": True,
            "preentry_structure_benchmark_halt_and_liquidity": True,
            "full_universe_selected_detail": False,
            "post_entry_rows": False,
            "returns_or_outcomes": False,
        },
        "outcome_lock": {
            "post_entry_data_access_allowed": False,
            "return_fields_allowed": False,
            "target_outcomes_observed_or_derived": False,
            "outcome_contract_must_be_separately_frozen": True,
            "date_symbol_provider_or_missing_input_substitution_allowed": False,
        },
        "downstream_zero_state": {
            "dataset_id": PREENTRY_DATASET_ID,
            "pre_freeze_artifact_count": 0,
            "private_location": (
                "LOCAL_HISTORICAL_DATA_ROOT/_derived/"
                f"challenger_orb_retest_preentry/{PREENTRY_DATASET_ID}"
            ),
        },
        "capacity_contract": {
            "minimum_free_bytes": MINIMUM_FREE_BYTES,
            "configured_reserve_bytes": config.min_free_bytes,
            "enforced_reserve_bytes": reserve_bytes,
            "free_bytes_at_freeze": free_bytes,
            "capacity_ready": True,
            "historical_store_outside_repository": True,
            "historical_deletion_allowed": False,
        },
        "source_bindings": {
            name: _binding(path) for name, path in source_paths.items()
        },
        "implementation_contract": {
            name: _binding(path) for name, path in implementation_paths.items()
        },
        "publication_contract": publication,
    }
    existing = sorted(output_root.glob(f"{dataset_id}-*.json"))
    if len(existing) > 1:
        raise ChallengerSelectedPairsError("multiple selected-pair manifests exist")
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
            raise ChallengerSelectedPairsError(
                "existing selected-pair contract differs"
            )
        path = existing[0]
    else:
        path, manifest = freeze_dataset_contract(contract, output_root)
    status = {
        "schema_version": 1,
        "dataset_id": dataset_id,
        "manifest_path": _repo_path(path),
        "manifest_sha256": manifest["manifest_sha256"],
        "status": "FROZEN_AWAITING_INSPECTION",
        "inspected": False,
        "selected_pair_count": EXPECTED_SELECTED_PAIRS,
        "requested_dates": EXPECTED_DATES,
        "pre_freeze_downstream_artifacts": 0,
        "target_outcomes_observed_or_derived": False,
    }
    _write_json(status_path, status)
    return path, manifest


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset-id", default=DATASET_ID)
    parser.add_argument("--summary", type=Path, default=DEFAULT_SUMMARY)
    parser.add_argument("--inspection", type=Path, default=DEFAULT_INSPECTION)
    parser.add_argument(
        "--scanner-manifest", type=Path, default=DEFAULT_SCANNER_MANIFEST
    )
    parser.add_argument("--outer-manifest", type=Path, default=DEFAULT_OUTER_MANIFEST)
    parser.add_argument("--hypothesis", type=Path, default=DEFAULT_HYPOTHESIS)
    parser.add_argument("--env", type=Path, default=PROJECT_ROOT / ".env")
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--status", type=Path, default=DEFAULT_STATUS)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        path, manifest = freeze_selected_pairs(
            dataset_id=args.dataset_id,
            summary_path=args.summary,
            inspection_path=args.inspection,
            scanner_manifest_path=args.scanner_manifest,
            outer_manifest_path=args.outer_manifest,
            hypothesis_path=args.hypothesis,
            env_path=args.env,
            output_root=args.output_root,
            status_path=args.status,
        )
        print(
            json.dumps(
                {
                    "dataset_id": manifest["dataset_id"],
                    "manifest_sha256": manifest["manifest_sha256"],
                    "path": _repo_path(path),
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
        ChallengerSelectedPairsError,
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

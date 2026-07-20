"""Independently inspect a frozen SEC accession-chain zero-response contract."""

from __future__ import annotations

import argparse
import json
import shutil
import sys
from pathlib import Path
from typing import Any

import development_sec_accession_chain_recovery as recovery
from historical_store import HistoricalStoreConfig, HistoricalStoreError
from learning_data import LearningDataError, load_frozen_dataset_contract


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_MANIFEST = (
    PROJECT_ROOT
    / "historical_batches/development_tranche_v3/sec_accession_chain_manifests"
    / (
        "dataset-development-sec-accession-chain-recovery-2026-07-20-v3-"
        "dd47c42489dd0b894fb5d2d9960c344a1ceb7f85483e32ab3a767669eaf97603.json"
    )
)
DEFAULT_STATUS = recovery.DEFAULT_PUBLIC_STATUS


class DevelopmentSecAccessionChainInspectionError(RuntimeError):
    """The frozen accession-chain contract does not independently rebuild."""


def _private_artifact_counts(store_root: Path) -> dict[str, int]:
    private_root = recovery._private_root(store_root)
    wrapper_root = private_root / "wrappers"
    return {
        "frozen_selections": int(recovery._selection_path(store_root).is_file()),
        "terminal_wrappers": (
            sum(1 for path in wrapper_root.rglob("*.json.gz") if path.is_file())
            if wrapper_root.exists()
            else 0
        ),
        "collection_indexes": int(recovery._index_path(store_root).is_file()),
        "reviewed_results": int(recovery._reviewed_path(store_root).is_file()),
    }


def _write_json(path: Path, value: Any) -> None:
    recovery._write_json(path, value)


def inspect_contract(
    *, manifest_path: Path, env_path: Path, status_path: Path
) -> dict[str, Any]:
    manifest = load_frozen_dataset_contract(manifest_path)
    if manifest.get("dataset_id") != recovery.DATASET_ID:
        raise DevelopmentSecAccessionChainInspectionError(
            "unexpected accession-chain dataset"
        )
    config = HistoricalStoreConfig.from_env(env_path)
    if (
        config.min_free_bytes < recovery.MINIMUM_RESERVE_BYTES
        or shutil.disk_usage(config.root).free < config.min_free_bytes
        or config.root.resolve().is_relative_to(PROJECT_ROOT.resolve())
    ):
        raise DevelopmentSecAccessionChainInspectionError(
            "historical store capacity is unsafe"
        )
    selection_path = recovery._selection_path(config.root)
    selection = recovery._read_gzip(selection_path)
    reviewed_path = recovery._upstream_reviewed_path(config.root)
    reviewed = recovery._read_gzip(reviewed_path)
    rebuilt = recovery.build_selection(reviewed)
    contract = manifest.get("selection_contract", {})
    if not (
        rebuilt == selection
        and recovery._sha256_file(selection_path)
        == contract.get("private_selection_sha256")
        and recovery._sha256_file(reviewed_path)
        == contract.get("source_semantics_private_result_sha256")
        and recovery._sha256_file(recovery.SOURCE_SEMANTICS_RESULT)
        == contract.get("source_semantics_result_sha256")
        and selection.get("pair_identity_sha256")
        == contract.get("pair_identity_sha256")
        and selection.get("request_graph_sha256")
        == contract.get("request_graph_sha256")
        and selection.get("join_graph_sha256") == contract.get("join_graph_sha256")
        and selection.get("counts")
        == {
            "candidate_pairs": contract.get("candidate_pairs"),
            "prior_pair_source_joins": contract.get("prior_pair_source_joins"),
            "unique_accessions": contract.get("unique_accessions"),
        }
        and selection.get("target_outcomes_observed_or_derived") is False
    ):
        raise DevelopmentSecAccessionChainInspectionError(
            "selection or upstream lineage does not rebuild"
        )
    source_manifest = load_frozen_dataset_contract(
        recovery.SOURCE_SEMANTICS_MANIFEST
    )
    request = manifest.get("request_contract", {})
    review = manifest.get("review_contract", {})
    outcome = manifest.get("outcome_lock", {})
    if not (
        source_manifest.get("manifest_sha256")
        == contract.get("source_semantics_manifest_sha256")
        and recovery._sha256_file(Path(recovery.__file__))
        == review.get("implementation_sha256")
        and recovery._sha256_file(Path(recovery.upstream_semantics.__file__))
        == review.get("upstream_classifier_sha256")
        and recovery._sha256_file(Path(recovery.shared_semantics.__file__))
        == review.get("shared_semantics_sha256")
        and request.get("provider") == "SEC_EDGAR"
        and request.get("endpoint") == "ACCESSION_COMPLETE_SUBMISSION_TEXT"
        and int(request.get("request_count", -1))
        == int(selection["counts"]["unique_accessions"])
        and request.get("provider_substitution_allowed") is False
        and request.get("secondary_news_substitution_allowed") is False
        and request.get("historical_store_reserve_enforced") is True
        and int(request.get("minimum_required_reserve_bytes", -1))
        == recovery.MINIMUM_RESERVE_BYTES
        and review.get("parser_version") == recovery.PARSER_VERSION
        and review.get("document_type_prefix") == "EX-99"
        and review.get("financing_or_dilution_precedes_positive") is True
        and review.get("network_access_allowed_during_review") is False
        and review.get("target_outcomes_observed_or_derived") is False
        and outcome.get("post_entry_data_access_allowed") is False
        and outcome.get("target_outcomes_observed_or_derived") is False
        and outcome.get("production_rule_change_allowed") is False
    ):
        raise DevelopmentSecAccessionChainInspectionError(
            "request, implementation, review, or outcome contract differs"
        )
    artifacts = _private_artifact_counts(config.root)
    if artifacts != {
        "frozen_selections": 1,
        "terminal_wrappers": 0,
        "collection_indexes": 0,
        "reviewed_results": 0,
    }:
        raise DevelopmentSecAccessionChainInspectionError(
            "target collection or review artifacts exist before inspection"
        )
    public = {
        "schema_version": 1,
        "dataset_id": recovery.DATASET_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "status": "FROZEN_READY",
        "inspected": True,
        "claim_scope": "DEVELOPMENT_ONLY",
        "counts": dict(selection["counts"]),
        "pair_identity_sha256": selection["pair_identity_sha256"],
        "request_graph_sha256": selection["request_graph_sha256"],
        "join_graph_sha256": selection["join_graph_sha256"],
        "private_selection_sha256": recovery._sha256_file(selection_path),
        "private_artifact_counts": artifacts,
        "free_bytes": shutil.disk_usage(config.root).free,
        "reserve_bytes": config.min_free_bytes,
        "inspection": {
            "upstream_lineage_rebuilt": True,
            "complete_unresolved_pair_surface_rebuilt": True,
            "request_and_join_graphs_rebuilt": True,
            "implementation_and_rule_hashes_verified": True,
            "privacy_boundary_verified": True,
            "zero_response_and_review_artifacts_verified": True,
            "outcome_lock_verified": True,
        },
        "next_required_stage": (
            "commit and push this inspected zero-response contract before running "
            "the exact accession collection"
        ),
        "claim_boundary": (
            "Frozen SEC accession request and issuer-filed EX-99 review contract "
            "only. No accession source, semantic result, target outcome, alpha, "
            "maturity, or production-rule claim is established."
        ),
        "source_semantics_classified": False,
        "outcome_contract_permitted": False,
        "production_rule_change_earned": False,
        "symbols_ciks_accessions_urls_sources_text_and_rows_public": False,
        "target_outcomes_observed_or_derived": False,
        "valid": True,
    }
    _write_json(status_path, public)
    return public


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--env-file", type=Path, default=PROJECT_ROOT / ".env")
    parser.add_argument("--status", type=Path, default=DEFAULT_STATUS)
    return parser


def main() -> int:
    args = _parser().parse_args()
    try:
        value = inspect_contract(
            manifest_path=args.manifest,
            env_path=args.env_file,
            status_path=args.status,
        )
    except (
        DevelopmentSecAccessionChainInspectionError,
        recovery.DevelopmentSecAccessionChainError,
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

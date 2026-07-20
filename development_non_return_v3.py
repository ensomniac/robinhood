"""Freeze the combined v3 source-positive non-return qualification surface.

This network-free adapter combines the already-inspected primary-document and
accession-chain source decisions, rejoins the exact positive hashes to the
second-tranche scanner selection, and freezes the causal pre-entry request
graph.  It never reads a target return or a provider row after the final
decision snapshot.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import development_catalyst_contract as source_contract
import development_non_return as base
import development_sec_accession_chain_recovery as accession_recovery
from historical_store import HistoricalStoreConfig, HistoricalStoreError
from learning_data import (
    LearningDataError,
    freeze_dataset_contract,
    load_frozen_dataset_contract,
)


PROJECT_ROOT = Path(__file__).resolve().parent
DATASET_ID = "dataset-development-non-return-qualification-2026-07-20-v3"
SELECTED_PAIR_MANIFEST = (
    PROJECT_ROOT
    / "historical_batches/development_tranche_v3/selected_pair_manifests"
    / (
        "dataset-selected-candidate-contract-2026-07-20-development-v3-"
        "f00e8393391d8daee026e326d98643d8f07e2f38f847e4b257c6caa1b73c8897.json"
    )
)
SEMANTICS_MANIFEST = accession_recovery.SOURCE_SEMANTICS_MANIFEST
SEMANTICS_RESULT = accession_recovery.SOURCE_SEMANTICS_RESULT
ACCESSION_MANIFEST = (
    PROJECT_ROOT
    / "historical_batches/development_tranche_v3/sec_accession_chain_manifests"
    / (
        "dataset-development-sec-accession-chain-recovery-2026-07-20-v3-"
        "dd47c42489dd0b894fb5d2d9960c344a1ceb7f85483e32ab3a767669eaf97603.json"
    )
)
ACCESSION_RESULT = accession_recovery.DEFAULT_PUBLIC_RESULT
DEFAULT_OUTPUT_ROOT = (
    PROJECT_ROOT / "historical_batches/development_tranche_v3/non_return_manifests"
)
DEFAULT_PUBLIC_STATUS = (
    PROJECT_ROOT
    / "historical_batches/development_tranche_v3/non-return-contract-status.json"
)
PRIVATE_NAMESPACE = "_derived/development_non_return"
PRIVATE_CONTRACT_FILE = "frozen-positive-preentry-contract.json.gz"
EXPECTED_SOURCE_PAIRS = 1_871
EXPECTED_PRIOR_POSITIVES = 19
EXPECTED_RECOVERY_PAIRS = 377
EXPECTED_RECOVERED_POSITIVES = 83
EXPECTED_COMBINED_POSITIVES = 102
MINIMUM_SURVIVORS = 20
MINIMUM_RESERVE_BYTES = 20 * 1024**3
CALENDAR_QUERY_START = "2023-12-01"
CALENDAR_QUERY_END = "2025-12-31"


class DevelopmentNonReturnV3Error(RuntimeError):
    """The v3 source-positive pre-entry contract is unsafe or stale."""


def _repo_path(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(PROJECT_ROOT))
    except ValueError as exc:
        raise DevelopmentNonReturnV3Error(
            f"public evidence path must be repository relative: {path}"
        ) from exc


def _private_root(store_root: Path) -> Path:
    return store_root / PRIVATE_NAMESPACE / DATASET_ID


def _private_contract_path(store_root: Path) -> Path:
    return _private_root(store_root) / PRIVATE_CONTRACT_FILE


def _target_artifact_count(store_root: Path) -> int:
    root = _private_root(store_root)
    if not root.exists():
        return 0
    selection = _private_contract_path(store_root).resolve()
    return sum(
        1
        for path in root.rglob("*")
        if path.is_file() and path.resolve() != selection
    )


def build_combined_pair_dispositions(
    prior_reviewed: Mapping[str, Any], recovered_reviewed: Mapping[str, Any]
) -> dict[str, str]:
    prior = prior_reviewed.get("pair_dispositions")
    recovered = recovered_reviewed.get("pair_dispositions")
    if not (
        prior_reviewed.get("dataset_id")
        == accession_recovery.SOURCE_REVIEW_DATASET_ID
        and prior_reviewed.get("status") == "REVIEW_COMPLETE"
        and prior_reviewed.get("verified_positive_pairs")
        == EXPECTED_PRIOR_POSITIVES
        and prior_reviewed.get("target_outcomes_observed_or_derived") is False
        and isinstance(prior, Mapping)
        and len(prior) == EXPECTED_SOURCE_PAIRS
        and recovered_reviewed.get("dataset_id") == accession_recovery.DATASET_ID
        and recovered_reviewed.get("status") == "REVIEW_COMPLETE"
        and recovered_reviewed.get("recovered_verified_positive_pairs")
        == EXPECTED_RECOVERED_POSITIVES
        and recovered_reviewed.get("combined_verified_positive_pairs")
        == EXPECTED_COMBINED_POSITIVES
        and recovered_reviewed.get("target_outcomes_observed_or_derived") is False
        and isinstance(recovered, Mapping)
        and len(recovered) == EXPECTED_RECOVERY_PAIRS
    ):
        raise DevelopmentNonReturnV3Error(
            "prior or recovered private source decisions are incomplete"
        )
    prior_values = {str(key): str(value) for key, value in prior.items()}
    recovered_values = {str(key): str(value) for key, value in recovered.items()}
    if not set(recovered_values) <= set(prior_values):
        raise DevelopmentNonReturnV3Error(
            "recovered decisions are outside the source pair denominator"
        )
    if any(
        prior_values[key] != "DOCUMENT_SEMANTICS_UNRESOLVED"
        for key in recovered_values
    ):
        raise DevelopmentNonReturnV3Error(
            "recovery attempts to overwrite a resolved source decision"
        )
    combined = {**prior_values, **recovered_values}
    positives = {
        key for key, disposition in combined.items()
        if disposition == "VERIFIED_POSITIVE_PRIMARY"
    }
    prior_positives = {
        key for key, disposition in prior_values.items()
        if disposition == "VERIFIED_POSITIVE_PRIMARY"
    }
    recovered_positives = {
        key for key, disposition in recovered_values.items()
        if disposition == "VERIFIED_POSITIVE_PRIMARY"
    }
    if not (
        len(prior_positives) == EXPECTED_PRIOR_POSITIVES
        and len(recovered_positives) == EXPECTED_RECOVERED_POSITIVES
        and prior_positives.isdisjoint(recovered_positives)
        and len(positives) == EXPECTED_COMBINED_POSITIVES
    ):
        raise DevelopmentNonReturnV3Error(
            "combined positive source capacity differs"
        )
    return combined


def build_request_graph(selection: Mapping[str, Any]) -> dict[str, Any]:
    request = base.build_request_graph(selection)
    graph = request["graph"]
    graph["calendar_query"]["start"] = CALENDAR_QUERY_START
    graph["calendar_query"]["end"] = CALENDAR_QUERY_END
    graph["split_action_query"]["execution_date_gte"] = CALENDAR_QUERY_START
    graph["split_action_query"]["execution_date_lte"] = CALENDAR_QUERY_END
    request["request_graph_sha256"] = base._sha256_json(graph)
    return request


def _load_sources(
    env_path: Path,
) -> tuple[HistoricalStoreConfig, dict[str, Any], dict[str, Any]]:
    config = HistoricalStoreConfig.from_env(env_path)
    selected_manifest = load_frozen_dataset_contract(SELECTED_PAIR_MANIFEST)
    semantics_manifest = load_frozen_dataset_contract(SEMANTICS_MANIFEST)
    accession_manifest = load_frozen_dataset_contract(ACCESSION_MANIFEST)
    semantics_result = base._read_object(SEMANTICS_RESULT)
    accession_result = base._read_object(ACCESSION_RESULT)
    if not (
        selected_manifest.get("requested_dates")
        == semantics_manifest.get("requested_dates")
        and int(
            selected_manifest.get("selection_contract", {}).get(
                "selected_pair_count", -1
            )
        )
        == EXPECTED_SOURCE_PAIRS
        and semantics_manifest.get("manifest_sha256")
        == semantics_result.get("manifest_sha256")
        and semantics_result.get("status") == "READY"
        and semantics_result.get("inspected") is True
        and semantics_result.get("verified_positive_pairs")
        == EXPECTED_PRIOR_POSITIVES
        and semantics_result.get("target_outcomes_observed_or_derived") is False
        and accession_manifest.get("manifest_sha256")
        == accession_result.get("manifest_sha256")
        and accession_result.get("status") == "READY"
        and accession_result.get("inspected") is True
        and accession_result.get("combined_verified_positive_pairs")
        == EXPECTED_COMBINED_POSITIVES
        and accession_result.get("positive_capacity_gate_passed") is True
        and accession_result.get("target_outcomes_observed_or_derived") is False
    ):
        raise DevelopmentNonReturnV3Error(
            "inspected source semantics or accession recovery differs"
        )
    source_private_path = source_contract._selection_private_path(
        config.root, str(selected_manifest["dataset_id"])
    )
    source_private = base._read_gzip(source_private_path)
    if source_contract._sha256_json(source_private) != selected_manifest.get(
        "selection_contract", {}
    ).get("private_selection_content_sha256"):
        raise DevelopmentNonReturnV3Error("private scanner selection drifted")
    prior_path = accession_recovery._upstream_reviewed_path(config.root)
    recovered_path = accession_recovery._reviewed_path(config.root)
    prior_reviewed = base._read_gzip(prior_path)
    recovered_reviewed = base._read_gzip(recovered_path)
    if not (
        base._sha256_file(prior_path) == semantics_result.get("private_result_sha256")
        and base._sha256_file(recovered_path)
        == accession_result.get("private_result_sha256")
    ):
        raise DevelopmentNonReturnV3Error("private reviewed source hashes differ")
    combined = build_combined_pair_dispositions(prior_reviewed, recovered_reviewed)
    selection = base.build_positive_selection(
        source_private,
        combined,
        expected_source_pairs=EXPECTED_SOURCE_PAIRS,
        expected_positive_pairs=EXPECTED_COMBINED_POSITIVES,
    )
    selection["dataset_id"] = DATASET_ID
    bindings = {
        "selected_pair_manifest": {
            "path": _repo_path(SELECTED_PAIR_MANIFEST),
            "sha256": base._sha256_file(SELECTED_PAIR_MANIFEST),
            "manifest_sha256": selected_manifest["manifest_sha256"],
        },
        "source_semantics_manifest": {
            "path": _repo_path(SEMANTICS_MANIFEST),
            "sha256": base._sha256_file(SEMANTICS_MANIFEST),
            "manifest_sha256": semantics_manifest["manifest_sha256"],
        },
        "source_semantics_result": {
            "path": _repo_path(SEMANTICS_RESULT),
            "sha256": base._sha256_file(SEMANTICS_RESULT),
        },
        "accession_recovery_manifest": {
            "path": _repo_path(ACCESSION_MANIFEST),
            "sha256": base._sha256_file(ACCESSION_MANIFEST),
            "manifest_sha256": accession_manifest["manifest_sha256"],
        },
        "accession_recovery_result": {
            "path": _repo_path(ACCESSION_RESULT),
            "sha256": base._sha256_file(ACCESSION_RESULT),
        },
        "private_source_selection_sha256": base._sha256_file(source_private_path),
        "private_source_review_sha256": base._sha256_file(prior_path),
        "private_accession_review_sha256": base._sha256_file(recovered_path),
    }
    return config, selection, bindings


def _implementation_contract() -> dict[str, Any]:
    value = base._implementation_contract()
    value["files"] = {
        **value["files"],
        Path(__file__).name: {
            "path": Path(__file__).name,
            "sha256": base._sha256_file(Path(__file__)),
        },
        "development_sec_accession_chain_recovery.py": {
            "path": "development_sec_accession_chain_recovery.py",
            "sha256": base._sha256_file(Path(accession_recovery.__file__)),
        },
    }
    return value


def _expected_contract(
    *,
    selected_manifest: Mapping[str, Any],
    selection: Mapping[str, Any],
    bindings: Mapping[str, Any],
    request: Mapping[str, Any],
    free_bytes: int,
) -> dict[str, Any]:
    value = base._expected_contract(
        selected_manifest=selected_manifest,
        selection=selection,
        bindings=bindings,
        request=request,
        free_bytes=free_bytes,
    )
    value["dataset_id"] = DATASET_ID
    value["dataset_payload"]["evidence_paths"] = [
        _repo_path(SELECTED_PAIR_MANIFEST),
        _repo_path(SEMANTICS_MANIFEST),
        _repo_path(SEMANTICS_RESULT),
        _repo_path(ACCESSION_MANIFEST),
        _repo_path(ACCESSION_RESULT),
        "DEVELOPMENT_SEC_SOURCES_V3.md",
        "DEVELOPMENT_NON_RETURN.md",
        "PRODUCTION_STRATEGY_VALIDATION.md",
    ]
    value["acquisition_contract"]["calendar_query"] = {
        "start": CALENDAR_QUERY_START,
        "end": CALENDAR_QUERY_END,
        "provider": "Alpaca Market Calendar API",
    }
    value["implementation_contract"] = _implementation_contract()
    value["privacy_contract"]["private_contract_location"] = (
        f"LOCAL_HISTORICAL_DATA_ROOT/{PRIVATE_NAMESPACE}/{DATASET_ID}/"
        f"{PRIVATE_CONTRACT_FILE}"
    )
    return value


def _matching_manifest(
    output_root: Path, expected: Mapping[str, Any]
) -> tuple[Path, dict[str, Any]] | None:
    matches = sorted(output_root.glob(f"{DATASET_ID}-*.json"))
    if len(matches) > 1:
        raise DevelopmentNonReturnV3Error("v3 non-return contract has multiple manifests")
    if not matches:
        return None
    manifest = load_frozen_dataset_contract(matches[0])
    for key, value in expected.items():
        if key == "capacity_contract":
            if manifest.get(key, {}).get("minimum_free_bytes") != value[
                "minimum_free_bytes"
            ]:
                raise DevelopmentNonReturnV3Error(
                    "existing v3 capacity contract differs"
                )
            continue
        if manifest.get(key) != value:
            raise DevelopmentNonReturnV3Error(
                f"existing v3 non-return {key} differs"
            )
    return matches[0], manifest


def _private_value(
    selection: Mapping[str, Any], request: Mapping[str, Any]
) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "selection": selection,
        "request_graph": request["graph"],
        "request_counts": request["counts"],
        "request_graph_sha256": request["request_graph_sha256"],
        "target_artifacts_at_freeze": 0,
        "target_outcomes_observed_or_derived": False,
    }


def _public_status(
    *,
    manifest: Mapping[str, Any],
    selection: Mapping[str, Any],
    request: Mapping[str, Any],
    private_path: Path,
    status: str,
    inspected: bool,
) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "status": status,
        "inspected": inspected,
        "source_selected_pairs": selection["source_selected_pair_count"],
        "verified_positive_pairs": selection["positive_pair_count"],
        "minimum_non_return_survivors_before_outcomes": MINIMUM_SURVIVORS,
        "request_counts": dict(request["counts"]),
        "private_positive_selection_sha256": base._sha256_json(selection),
        "positive_pair_identity_sha256": selection["positive_pair_identity_sha256"],
        "positive_date_identity_sha256": request["positive_date_identity_sha256"],
        "private_request_graph_sha256": request["request_graph_sha256"],
        "private_contract_file_sha256": base._sha256_file(private_path),
        "target_artifacts": 0,
        "post_entry_requests_allowed": False,
        "outcome_contract_permitted": False,
        "symbols_dates_rows_requests_and_raw_inputs_public": False,
        "target_outcomes_observed_or_derived": False,
    }


def freeze(
    *, env_path: Path, output_root: Path, public_status_path: Path
) -> tuple[Path, dict[str, Any]]:
    config, selection, bindings = _load_sources(env_path)
    if _target_artifact_count(config.root):
        raise DevelopmentNonReturnV3Error(
            "v3 pre-entry target artifacts exist before freeze"
        )
    free_bytes = shutil.disk_usage(config.root).free
    if free_bytes < MINIMUM_RESERVE_BYTES:
        raise DevelopmentNonReturnV3Error(
            "historical store is below the 20-GiB reserve"
        )
    request = build_request_graph(selection)
    selected_manifest = load_frozen_dataset_contract(SELECTED_PAIR_MANIFEST)
    expected = _expected_contract(
        selected_manifest=selected_manifest,
        selection=selection,
        bindings=bindings,
        request=request,
        free_bytes=free_bytes,
    )
    existing = _matching_manifest(output_root, expected)
    private = _private_value(selection, request)
    private_path = _private_contract_path(config.root)
    if private_path.exists():
        if base._sha256_json(base._read_gzip(private_path)) != base._sha256_json(
            private
        ):
            raise DevelopmentNonReturnV3Error("private v3 pre-entry contract changed")
    else:
        base._write_gzip(private_path, private)
    if existing is None:
        path, manifest = freeze_dataset_contract(
            {**expected, "registered_at": datetime.now(UTC).isoformat()}, output_root
        )
    else:
        path, manifest = existing
    _write_json(
        public_status_path,
        _public_status(
            manifest=manifest,
            selection=selection,
            request=request,
            private_path=private_path,
            status="FROZEN_WAITING_INSPECTION",
            inspected=False,
        ),
    )
    return path, manifest


def inspect(
    *, manifest_path: Path, env_path: Path, public_status_path: Path
) -> dict[str, Any]:
    manifest = load_frozen_dataset_contract(manifest_path)
    if manifest.get("dataset_id") != DATASET_ID:
        raise DevelopmentNonReturnV3Error("unexpected v3 non-return dataset")
    config, selection, bindings = _load_sources(env_path)
    request = build_request_graph(selection)
    expected = _expected_contract(
        selected_manifest=load_frozen_dataset_contract(SELECTED_PAIR_MANIFEST),
        selection=selection,
        bindings=bindings,
        request=request,
        free_bytes=int(manifest["capacity_contract"]["observed_free_bytes_at_freeze"]),
    )
    for key, value in expected.items():
        if manifest.get(key) != value:
            raise DevelopmentNonReturnV3Error(f"frozen v3 non-return {key} drifted")
    private_path = _private_contract_path(config.root)
    if base._read_gzip(private_path) != _private_value(selection, request):
        raise DevelopmentNonReturnV3Error("private v3 pre-entry contract differs")
    if _target_artifact_count(config.root):
        raise DevelopmentNonReturnV3Error(
            "v3 pre-entry target artifacts appeared before inspection"
        )
    result = _public_status(
        manifest=manifest,
        selection=selection,
        request=request,
        private_path=private_path,
        status="FROZEN_READY",
        inspected=True,
    )
    result["inspection"] = {
        "source_selection_rebuilt": True,
        "prior_and_recovered_positive_hashes_deduplicated": True,
        "positive_pair_hash_join_rebuilt": True,
        "coarse_scanner_gates_rechecked": True,
        "request_graph_rebuilt": True,
        "implementation_hashes_rebuilt": True,
        "strategy_version_and_rules_hash_rebuilt": True,
        "private_public_boundary_rechecked": True,
        "zero_target_artifacts_rechecked": True,
        "valid": True,
    }
    _write_json(public_status_path, result)
    return result


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


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("freeze", "inspect"))
    parser.add_argument("manifest", nargs="?", type=Path)
    parser.add_argument("--env", type=Path, default=PROJECT_ROOT / ".env")
    parser.add_argument("--output-root", type=Path, default=DEFAULT_OUTPUT_ROOT)
    parser.add_argument("--public-status", type=Path, default=DEFAULT_PUBLIC_STATUS)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "freeze":
            path, manifest = freeze(
                env_path=args.env,
                output_root=args.output_root,
                public_status_path=args.public_status,
            )
            result: Any = {
                "path": _repo_path(path),
                "dataset_id": manifest["dataset_id"],
                "manifest_sha256": manifest["manifest_sha256"],
                "verified_positive_pairs": manifest["selection_contract"][
                    "verified_positive_pair_count"
                ],
            }
        elif args.manifest is None:
            raise DevelopmentNonReturnV3Error("inspect requires a manifest")
        else:
            result = inspect(
                manifest_path=args.manifest,
                env_path=args.env,
                public_status_path=args.public_status,
            )
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    except (
        DevelopmentNonReturnV3Error,
        base.DevelopmentNonReturnError,
        HistoricalStoreError,
        LearningDataError,
        OSError,
        ValueError,
    ) as exc:
        print(
            json.dumps(
                {"status": "error", "error": str(exc), "error_type": type(exc).__name__},
                indent=2,
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())

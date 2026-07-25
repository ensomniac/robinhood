"""Rebind an inspected development dataset after an implementation-only refresh."""

from __future__ import annotations

import argparse
import copy
import json
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import strategy_discovery
from learning_data import (
    LearningDataError,
    freeze_dataset_contract,
    load_frozen_dataset_contract,
)


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_ROOT = strategy_discovery.DEFAULT_ROOT
INSPECTION_KIND = "development-dataset-binding-inspection"
INSPECTION_STATE = "DEVELOPMENT_DATASET_BINDING_INSPECTED"


class StrategyDatasetRebindError(RuntimeError):
    """A dataset cannot be proven identical across an implementation refresh."""


def _recorded_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _source_manifest(
    *,
    root: Path,
    family_id: str,
    source_search_sha256: str,
) -> tuple[Path, dict[str, Any]]:
    directory = root / family_id / "development-dataset"
    matches: list[tuple[Path, dict[str, Any]]] = []
    for path in sorted(directory.glob("dataset-*.json")):
        manifest = load_frozen_dataset_contract(path)
        payload = manifest["dataset_payload"]
        dense_runtime = payload.get("dense_runtime")
        if (
            payload.get("lane") == "development"
            and payload.get("development_search_sha256")
            == source_search_sha256
            and isinstance(dense_runtime, Mapping)
            and dense_runtime.get("family_id") == family_id
        ):
            matches.append((path, manifest))
    if len(matches) != 1:
        raise StrategyDatasetRebindError(
            "expected exactly one inspected source dataset manifest; "
            f"found {len(matches)}"
        )
    return matches[0]


def _collection_inspection(
    manifest: Mapping[str, Any],
    *,
    enforce_commit: bool,
) -> tuple[Path, dict[str, Any]]:
    candidates: list[tuple[Path, dict[str, Any]]] = []
    for raw_path in manifest["dataset_payload"]["evidence_paths"]:
        path = PROJECT_ROOT / str(raw_path)
        if not path.is_file():
            continue
        try:
            artifact = strategy_discovery.load_artifact(path)
        except strategy_discovery.StrategyDiscoveryError:
            continue
        if artifact.get("artifact_kind") == "dense-data-collection-inspection":
            candidates.append((path, artifact))
    if len(candidates) != 1:
        raise StrategyDatasetRebindError(
            "source manifest lacks one collection inspection"
        )
    path, inspection = candidates[0]
    if enforce_commit:
        strategy_discovery.require_committed(path)
    runtime = manifest["dataset_payload"]["dense_runtime"]
    checks = inspection.get("checks")
    if not (
        inspection.get("state") == "DATASET_INSPECTED_READY"
        and inspection.get("family_id") == runtime.get("family_id")
        and inspection.get("lane") == "development"
        and inspection.get("dataset_sha256") == runtime.get("dataset_sha256")
        and inspection.get("external_file_sha256")
        == runtime.get("external_file_sha256")
        and inspection.get("evaluation_dates")
        == manifest.get("requested_dates")
        and isinstance(checks, Mapping)
        and checks
        and all(value is True for value in checks.values())
    ):
        raise StrategyDatasetRebindError(
            "source collection inspection binding is invalid"
        )
    return path, inspection


def rebind_development_dataset(
    refresh_path: Path,
    search_path: Path,
    *,
    root: Path = DEFAULT_ROOT,
    registered_at: str | None = None,
    enforce_commit: bool = True,
) -> tuple[Path, dict[str, Any]]:
    if enforce_commit:
        strategy_discovery.require_committed(refresh_path)
        strategy_discovery.require_committed(search_path)
    refresh = strategy_discovery.load_artifact(
        refresh_path,
        expected_kind="implementation-refresh-inspection",
    )
    source_search_path = PROJECT_ROOT / str(refresh["source_search_path"])
    if enforce_commit:
        strategy_discovery.require_committed(source_search_path)
    source_search = strategy_discovery.load_artifact(
        source_search_path,
        expected_kind="frozen-development-search",
    )
    search = strategy_discovery.load_artifact(
        search_path,
        expected_kind="frozen-development-search",
    )
    source_contract = source_search["family_contract"]
    contract = search["family_contract"]
    family_id = str(contract["family_id"])
    refreshed_contract_path = (
        PROJECT_ROOT / str(refresh["refreshed_contract_path"])
    )
    if enforce_commit:
        strategy_discovery.require_committed(refreshed_contract_path)
    refreshed_contract = strategy_discovery._read_object(
        refreshed_contract_path
    )
    if not (
        refresh.get("state") == "IMPLEMENTATION_REFRESH_INSPECTED"
        and refresh.get("inspection", {}).get("valid") is True
        and refresh.get("only_implementation_hashes_changed") is True
        and refresh.get("source_development_evaluations") == 0
        and refresh.get("strategy_outcomes_accessed") is False
        and refresh.get("confirmation_access_permitted") is False
        and refresh.get("source_search_sha256")
        == source_search["artifact_sha256"]
        and search.get("state") == "SEARCH_FROZEN"
        and contract == refreshed_contract
        and contract.get("implementation_hashes")
        == refresh.get("refreshed_implementation_hashes")
        and strategy_discovery._contract_without_implementation_hashes(
            source_contract
        )
        == strategy_discovery._contract_without_implementation_hashes(
            contract
        )
    ):
        raise StrategyDatasetRebindError(
            "implementation refresh and new search are not semantically identical"
        )
    strategy_discovery._assert_implementation_current(
        contract,
        enforce_commit=enforce_commit,
    )
    source_manifest_path, source_manifest = _source_manifest(
        root=root,
        family_id=family_id,
        source_search_sha256=source_search["artifact_sha256"],
    )
    if enforce_commit:
        strategy_discovery.require_committed(source_manifest_path)
    collection_inspection_path, collection_inspection = (
        _collection_inspection(
            source_manifest,
            enforce_commit=enforce_commit,
        )
    )
    if source_manifest["requested_dates"] != contract["development_dates"]:
        raise StrategyDatasetRebindError(
            "source dataset dates differ from the refreshed search"
        )

    payload = copy.deepcopy(source_manifest)
    payload.pop("manifest_sha256", None)
    payload["dataset_id"] = (
        f"dataset-{family_id}-development-"
        f"{search['artifact_sha256'][:16]}"
    )
    payload["registered_at"] = registered_at or _recorded_now()
    dataset_payload = payload["dataset_payload"]
    dataset_payload["development_search_sha256"] = search[
        "artifact_sha256"
    ]
    evidence_paths = list(dataset_payload["evidence_paths"])
    for path in (refresh_path, search_path):
        relative = strategy_discovery._relative(path)
        if relative not in evidence_paths:
            evidence_paths.append(relative)
    dataset_payload["evidence_paths"] = evidence_paths
    dataset_payload["dataset_binding_refresh"] = {
        "source_manifest_path": strategy_discovery._relative(
            source_manifest_path
        ),
        "source_manifest_sha256": source_manifest["manifest_sha256"],
        "source_search_sha256": source_search["artifact_sha256"],
        "refreshed_search_sha256": search["artifact_sha256"],
        "implementation_refresh_path": strategy_discovery._relative(
            refresh_path
        ),
        "implementation_refresh_sha256": refresh["artifact_sha256"],
        "collection_inspection_path": strategy_discovery._relative(
            collection_inspection_path
        ),
        "collection_inspection_sha256": collection_inspection[
            "artifact_sha256"
        ],
        "private_dataset_sha256": dataset_payload["dense_runtime"][
            "dataset_sha256"
        ],
        "private_file_sha256": dataset_payload["dense_runtime"][
            "external_file_sha256"
        ],
        "only_search_and_lineage_metadata_changed": True,
        "provider_requests": 0,
        "external_dataset_opened": False,
        "strategy_outcomes_accessed": False,
    }
    manifest_path, manifest = freeze_dataset_contract(
        payload,
        root / family_id / "development-dataset",
    )
    inspection_payload = {
        "schema_version": 1,
        "artifact_kind": INSPECTION_KIND,
        "campaign_id": strategy_discovery.CAMPAIGN_ID,
        "family_id": family_id,
        "state": INSPECTION_STATE,
        "source_manifest_path": strategy_discovery._relative(
            source_manifest_path
        ),
        "source_manifest_sha256": source_manifest["manifest_sha256"],
        "refreshed_manifest_path": strategy_discovery._relative(
            manifest_path
        ),
        "refreshed_manifest_sha256": manifest["manifest_sha256"],
        "source_search_sha256": source_search["artifact_sha256"],
        "refreshed_search_sha256": search["artifact_sha256"],
        "implementation_refresh_path": strategy_discovery._relative(
            refresh_path
        ),
        "implementation_refresh_sha256": refresh["artifact_sha256"],
        "collection_inspection_path": strategy_discovery._relative(
            collection_inspection_path
        ),
        "collection_inspection_sha256": collection_inspection[
            "artifact_sha256"
        ],
        "requested_date_count": len(manifest["requested_dates"]),
        "private_dataset_sha256": dataset_payload["dense_runtime"][
            "dataset_sha256"
        ],
        "private_file_sha256": dataset_payload["dense_runtime"][
            "external_file_sha256"
        ],
        "provider_telemetry": {
            "requests": 0,
            "request_seconds": 0.0,
            "pacing_wait_seconds": 0.0,
            "cache_hits": 1,
            "failures": 0,
            "dataset_loads": 0,
        },
        "strategy_outcomes_accessed": False,
        "confirmation_access_permitted": False,
        "broker_actions_permitted": False,
        "inspection": {
            "source_manifest_hash_rebuilt": True,
            "source_collection_inspection_rebuilt": True,
            "search_semantics_equality_rebuilt": True,
            "implementation_refresh_rebuilt": True,
            "requested_dates_equality_rebuilt": True,
            "private_dataset_hashes_preserved": True,
            "only_search_and_lineage_metadata_changed": True,
            "valid": True,
        },
    }
    return strategy_discovery._write_artifact(
        inspection_payload,
        root / family_id / "development-dataset-binding-inspection",
        f"{family_id}-development-dataset-binding-inspection",
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    subparsers = parser.add_subparsers(dest="command", required=True)
    rebind = subparsers.add_parser("rebind")
    rebind.add_argument("refresh", type=Path)
    rebind.add_argument("search", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        path, artifact = rebind_development_dataset(
            args.refresh,
            args.search,
            root=args.root,
        )
        print(
            json.dumps(
                {
                    "written": strategy_discovery._relative(path),
                    "artifact_sha256": artifact["artifact_sha256"],
                    "state": artifact["state"],
                    "refreshed_manifest": artifact[
                        "refreshed_manifest_path"
                    ],
                    "provider_requests": 0,
                    "strategy_outcomes_accessed": False,
                    "broker_actions_permitted": False,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    except (
        StrategyDatasetRebindError,
        strategy_discovery.StrategyDiscoveryError,
        LearningDataError,
        OSError,
        ValueError,
    ) as exc:
        print(
            json.dumps(
                {"error": str(exc), "error_type": type(exc).__name__},
                indent=2,
                sort_keys=True,
            )
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

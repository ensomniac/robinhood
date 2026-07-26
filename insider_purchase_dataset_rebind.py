"""Bind an already-inspected Form 4 dataset to a replacement frozen search.

This transition opens no private rows and contacts no provider.  It proves the
old and replacement searches have identical semantics after implementation
hashes are removed, then republishes only dataset metadata and adds the exact
manifest path to a new family contract.
"""

from __future__ import annotations

import argparse
import copy
import json
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import strategy_discovery
from historical_store import sha256_file
from learning_data import freeze_dataset_contract, load_frozen_dataset_contract


PROJECT_ROOT = Path(__file__).resolve().parent


class InsiderPurchaseDatasetRebindError(RuntimeError):
    """The inspected dataset or replacement search lineage drifted."""


def _relative(path: Path) -> str:
    return str(path.resolve().relative_to(PROJECT_ROOT.resolve()))


def _load_search(path: Path) -> dict[str, Any]:
    return strategy_discovery.load_artifact(
        path, expected_kind="frozen-development-search"
    )


def _semantic_contract(value: Mapping[str, Any]) -> dict[str, Any]:
    return strategy_discovery._contract_without_implementation_hashes(
        value
    )


def build_manifest_value(
    replacement_search_path: Path,
    source_manifest_path: Path,
    *,
    registered_at: str,
) -> dict[str, Any]:
    replacement = _load_search(replacement_search_path)
    replacement_family_id = str(
        replacement["family_contract"]["family_id"]
    )
    source_manifest = load_frozen_dataset_contract(source_manifest_path)
    payload = copy.deepcopy(source_manifest["dataset_payload"])
    source_search_sha256 = payload.get("development_search_sha256")
    if not isinstance(source_search_sha256, str):
        raise InsiderPurchaseDatasetRebindError(
            "source dataset search binding is missing"
        )
    source_search_paths = [
        path
        for path in (
            PROJECT_ROOT
            / "strategy_tournament/v2/discovery"
            / replacement_family_id
            / "search"
        ).glob("*.json")
        if _load_search(path).get("artifact_sha256")
        == source_search_sha256
    ]
    if len(source_search_paths) != 1:
        raise InsiderPurchaseDatasetRebindError(
            "source dataset search is absent or ambiguous"
        )
    source_search = _load_search(source_search_paths[0])
    if not (
        replacement["state"] == "SEARCH_FROZEN"
        and source_search["state"] == "SEARCH_FROZEN"
        and replacement_family_id
        == source_search["family_contract"]["family_id"]
        and replacement["trial_count"] == source_search["trial_count"] == 32
        and _semantic_contract(replacement["family_contract"])
        == _semantic_contract(source_search["family_contract"])
        and source_manifest["requested_dates"]
        == replacement["family_contract"]["development_dates"]
        and payload.get("lane") == "development"
        and payload.get("inspected") is True
        and payload.get("claim_scope") == "DEVELOPMENT_ONLY"
        and isinstance(payload.get("form4_purchase_runtime"), Mapping)
        and payload["form4_purchase_runtime"].get("family_id")
        == replacement_family_id
        and payload["form4_purchase_runtime"].get("sample_phase")
        == "development"
    ):
        raise InsiderPurchaseDatasetRebindError(
            "replacement search changes dataset or strategy semantics"
        )
    evidence_paths = list(payload.get("evidence_paths", []))
    for path in (
        _relative(source_manifest_path),
        _relative(replacement_search_path),
    ):
        if path not in evidence_paths:
            evidence_paths.append(path)
    payload.update(
        {
            "evidence_paths": evidence_paths,
            "development_search_sha256": replacement[
                "artifact_sha256"
            ],
            "dataset_rebind": {
                "source_manifest_path": _relative(source_manifest_path),
                "source_manifest_file_sha256": sha256_file(
                    source_manifest_path
                ),
                "source_search_path": _relative(source_search_paths[0]),
                "source_search_sha256": source_search_sha256,
                "replacement_search_path": _relative(
                    replacement_search_path
                ),
                "replacement_search_sha256": replacement[
                    "artifact_sha256"
                ],
                "semantic_contract_identical": True,
                "private_rows_opened": False,
                "provider_requests": 0,
                "confirmation_accessed": False,
            },
        }
    )
    return {
        "schema_version": 1,
        "dataset_id": (
            f"{source_manifest['dataset_id']}-rebound-"
            f"{replacement['artifact_sha256'][:16]}"
        ),
        "registered_at": registered_at,
        "requested_dates": source_manifest["requested_dates"],
        "dataset_payload": payload,
    }


def build_bound_contract(
    replacement_search_path: Path, rebound_manifest_path: Path
) -> dict[str, Any]:
    replacement = _load_search(replacement_search_path)
    manifest = load_frozen_dataset_contract(rebound_manifest_path)
    contract = copy.deepcopy(replacement["family_contract"])
    if not (
        manifest["requested_dates"] == contract["development_dates"]
        and manifest["dataset_payload"].get(
            "development_search_sha256"
        )
        == replacement["artifact_sha256"]
        and manifest["dataset_payload"].get("inspected") is True
    ):
        raise InsiderPurchaseDatasetRebindError(
            "rebound dataset does not match replacement search"
        )
    contract["dataset_manifest"] = _relative(rebound_manifest_path)
    return strategy_discovery._validate_family_contract(
        strategy_discovery._contract_without_implementation_hashes(
            contract
        )
    )


def freeze(
    replacement_search_path: Path,
    source_manifest_path: Path,
    *,
    registered_at: str,
) -> tuple[Path, Path, dict[str, Any]]:
    for path in (
        Path(__file__).resolve(),
        replacement_search_path,
        source_manifest_path,
    ):
        strategy_discovery.require_committed(path)
    search = _load_search(replacement_search_path)
    family_id = str(search["family_contract"]["family_id"])
    root = (
        PROJECT_ROOT
        / "strategy_tournament/v2/discovery"
        / family_id
    )
    manifest_path, manifest = freeze_dataset_contract(
        build_manifest_value(
            replacement_search_path,
            source_manifest_path,
            registered_at=registered_at,
        ),
        root / "development-dataset-rebound",
    )
    contract = build_bound_contract(
        replacement_search_path, manifest_path
    )
    contract_path = strategy_discovery._write_family_contract(
        contract,
        root / "family-contract",
    )
    return manifest_path, contract_path, manifest


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("replacement_search", type=Path)
    parser.add_argument("source_manifest", type=Path)
    parser.add_argument("--registered-at", required=True)
    args = parser.parse_args(argv)
    manifest, contract, value = freeze(
        args.replacement_search,
        args.source_manifest,
        registered_at=args.registered_at,
    )
    print(
        json.dumps(
            {
                "state": "DATASET_REBOUND_UNINSPECTED",
                "manifest": _relative(manifest),
                "manifest_sha256": value["manifest_sha256"],
                "family_contract": _relative(contract),
                "private_rows_opened": False,
                "provider_requests": 0,
                "confirmation_accessed": False,
                "broker_actions": 0,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())

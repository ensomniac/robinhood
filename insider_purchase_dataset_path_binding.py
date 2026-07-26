"""Freeze an absolute spelling of one already-bound Form 4 dataset path.

The discovery controller compares an explicit manifest path byte-for-byte with
the plugin result.  The plugin resolves paths before returning them, so this
administrative transition changes only the spelling of the same committed file
from repository-relative to resolved absolute form.
"""

from __future__ import annotations

import argparse
import copy
import json
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import strategy_discovery
from learning_data import load_frozen_dataset_contract


PROJECT_ROOT = Path(__file__).resolve().parent


class InsiderPurchaseDatasetPathBindingError(RuntimeError):
    """The explicit dataset path does not resolve to the frozen manifest."""


def build_contract(search_path: Path) -> dict[str, Any]:
    search = strategy_discovery.load_artifact(
        search_path, expected_kind="frozen-development-search"
    )
    contract = copy.deepcopy(search["family_contract"])
    raw_manifest = contract.get("dataset_manifest")
    if not isinstance(raw_manifest, str) or not raw_manifest:
        raise InsiderPurchaseDatasetPathBindingError(
            "source contract lacks an explicit dataset manifest"
        )
    relative_path = Path(raw_manifest)
    if relative_path.is_absolute():
        raise InsiderPurchaseDatasetPathBindingError(
            "source dataset manifest is already absolute"
        )
    manifest_path = (PROJECT_ROOT / relative_path).resolve()
    strategy_discovery.require_committed(manifest_path)
    manifest = load_frozen_dataset_contract(manifest_path)
    if not (
        manifest["requested_dates"] == contract["development_dates"]
        and manifest["dataset_payload"].get("lane") == "development"
        and manifest["dataset_payload"].get("inspected") is True
        and manifest["dataset_payload"].get("form4_purchase_runtime", {}).get(
            "family_id"
        )
        == contract["family_id"]
    ):
        raise InsiderPurchaseDatasetPathBindingError(
            "explicit dataset manifest content drifted"
        )
    contract["dataset_manifest"] = str(manifest_path)
    validated = strategy_discovery._validate_family_contract(
        strategy_discovery._contract_without_implementation_hashes(
            contract
        )
    )
    original = strategy_discovery._contract_without_implementation_hashes(
        search["family_contract"]
    )
    rebound = strategy_discovery._contract_without_implementation_hashes(
        validated
    )
    original_path = original.pop("dataset_manifest")
    rebound_path = rebound.pop("dataset_manifest")
    if not (
        original == rebound
        and (PROJECT_ROOT / original_path).resolve()
        == Path(rebound_path).resolve()
        == manifest_path
    ):
        raise InsiderPurchaseDatasetPathBindingError(
            "dataset path binding changed strategy semantics"
        )
    return validated


def freeze(search_path: Path) -> tuple[Path, dict[str, Any]]:
    strategy_discovery.require_committed(Path(__file__).resolve())
    strategy_discovery.require_committed(search_path)
    contract = build_contract(search_path)
    root = (
        PROJECT_ROOT
        / "strategy_tournament/v2/discovery"
        / str(contract["family_id"])
        / "family-contract"
    )
    path = strategy_discovery._write_family_contract(contract, root)
    return path, contract


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("search", type=Path)
    args = parser.parse_args(argv)
    path, contract = freeze(args.search)
    print(
        json.dumps(
            {
                "state": "DATASET_PATH_BOUND_UNINSPECTED",
                "family_contract": str(
                    path.resolve().relative_to(PROJECT_ROOT.resolve())
                ),
                "dataset_manifest": contract["dataset_manifest"],
                "same_file_identity": True,
                "strategy_semantics_changed": False,
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

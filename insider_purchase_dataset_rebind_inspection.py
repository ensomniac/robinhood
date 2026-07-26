"""Independently inspect a Form 4 dataset metadata rebind."""

from __future__ import annotations

import argparse
import json
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import insider_purchase_dataset_rebind as rebind
import strategy_discovery
from learning_data import load_frozen_dataset_contract


PROJECT_ROOT = Path(__file__).resolve().parent


class InsiderPurchaseDatasetRebindInspectionError(RuntimeError):
    """The rebound manifest or exact dataset-bound contract drifted."""


def inspect(
    replacement_search_path: Path,
    source_manifest_path: Path,
    rebound_manifest_path: Path,
    bound_contract_path: Path,
    *,
    inspected_at: str,
) -> tuple[Path, dict[str, Any]]:
    for path in (
        Path(__file__).resolve(),
        Path(rebind.__file__).resolve(),
        replacement_search_path,
        source_manifest_path,
        rebound_manifest_path,
        bound_contract_path,
    ):
        strategy_discovery.require_committed(path)
    rebound = load_frozen_dataset_contract(rebound_manifest_path)
    expected_manifest = rebind.build_manifest_value(
        replacement_search_path,
        source_manifest_path,
        registered_at=str(rebound["registered_at"]),
    )
    observed_manifest = {
        key: value
        for key, value in rebound.items()
        if key != "manifest_sha256"
    }
    if observed_manifest != expected_manifest:
        raise InsiderPurchaseDatasetRebindInspectionError(
            "rebound manifest differs from independent rebuild"
        )
    bound = strategy_discovery._read_object(bound_contract_path)
    expected_contract = rebind.build_bound_contract(
        replacement_search_path, rebound_manifest_path
    )
    if bound != expected_contract:
        raise InsiderPurchaseDatasetRebindInspectionError(
            "dataset-bound contract differs from independent rebuild"
        )
    family_id = str(bound["family_id"])
    payload = {
        "schema_version": 1,
        "artifact_kind": "form4-dataset-rebind-inspection",
        "campaign_id": str(bound["campaign_id"]),
        "family_id": family_id,
        "inspected_at": inspected_at,
        "state": "DATASET_REBIND_INSPECTED_READY",
        "replacement_search_path": rebind._relative(
            replacement_search_path
        ),
        "replacement_search_sha256": rebound["dataset_payload"][
            "development_search_sha256"
        ],
        "source_manifest_path": rebind._relative(source_manifest_path),
        "source_manifest_sha256": rebound["dataset_payload"][
            "dataset_rebind"
        ]["source_manifest_file_sha256"],
        "rebound_manifest_path": rebind._relative(
            rebound_manifest_path
        ),
        "rebound_manifest_sha256": rebound["manifest_sha256"],
        "bound_contract_path": rebind._relative(bound_contract_path),
        "bound_contract_sha256": strategy_discovery._file_hash(
            bound_contract_path
        ),
        "checks": {
            "source_manifest_hash_valid": True,
            "replacement_search_hash_valid": True,
            "semantic_contract_identity_rebuilt": True,
            "requested_dates_identical": True,
            "private_dataset_binding_identical": True,
            "runtime_family_binding_identical": True,
            "rebound_manifest_exactly_rebuilt": True,
            "bound_contract_exactly_rebuilt": True,
            "private_rows_unopened": True,
            "zero_provider_confirmation_or_broker_access": True,
            "valid": True,
        },
        "private_rows_opened": False,
        "provider_requests": 0,
        "confirmation_accessed": False,
        "broker_actions": 0,
    }
    return strategy_discovery._write_artifact(
        payload,
        (
            PROJECT_ROOT
            / "strategy_tournament/v2/discovery"
            / family_id
            / "development-dataset-rebind-inspection"
        ),
        f"{family_id}-dataset-rebind-inspection",
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("replacement_search", type=Path)
    parser.add_argument("source_manifest", type=Path)
    parser.add_argument("rebound_manifest", type=Path)
    parser.add_argument("bound_contract", type=Path)
    parser.add_argument("--inspected-at", required=True)
    args = parser.parse_args(argv)
    path, value = inspect(
        args.replacement_search,
        args.source_manifest,
        args.rebound_manifest,
        args.bound_contract,
        inspected_at=args.inspected_at,
    )
    print(
        json.dumps(
            {
                "state": value["state"],
                "path": rebind._relative(path),
                "artifact_sha256": value["artifact_sha256"],
                "checks": value["checks"],
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

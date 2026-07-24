"""Collect the frozen fixed-rule replication development minutes.

This wrapper reuses the already-audited oversold collection implementation
while replacing only its immutable dataset paths and inventory loader.  It
keeps the same commit-before-cache/provider-access and global outcome-exposure
boundaries.
"""

from __future__ import annotations

import argparse
import json
from collections.abc import Sequence
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import outcome_exposure
import oversold_fixed_rule_replication as replication
import oversold_replication_development_collection as base
import strategy_discovery
from historical_store import HistoricalDayStore, sha256_file


PROJECT_ROOT = Path(__file__).resolve().parent
DATASET_ID = replication.DATASET_ID
SUCCESSOR_ID = replication.SUCCESSOR_ID
EXPOSURE_ID = f"{SUCCESSOR_ID}-development-v1"
OUTPUT_ROOT = (
    PROJECT_ROOT / "strategy_tournament/v2/oversold_fixed_rule" / DATASET_ID
)
CONTRACT_ROOT = OUTPUT_ROOT / "collection-contract"
CONTRACT_INSPECTION_ROOT = OUTPUT_ROOT / "collection-contract-inspection"
AUTHORIZATION_ROOT = OUTPUT_ROOT / "development-exposure-authorization"
STATUS_ROOT = OUTPUT_ROOT / "collection-status"
DATA_INSPECTION_ROOT = OUTPUT_ROOT / "data-inspection"


class OversoldFixedRuleCollectionError(RuntimeError):
    """The fixed-rule collection chain is incomplete."""


def _one(root: Path) -> Path:
    paths = sorted(root.glob("*.json"))
    if len(paths) != 1:
        raise OversoldFixedRuleCollectionError(
            f"expected one artifact in {root}; found {len(paths)}"
        )
    return paths[0]


def load_artifact(
    path: Path, *, identity_field: str, expected_kind: str
) -> dict[str, Any]:
    return replication._load_hashed(
        path,
        identity_field=identity_field,
        expected_kind=expected_kind,
    )


def _load_inventory(
    store: HistoricalDayStore, *, require_committed: bool
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    boundary_path = _one(replication.BOUNDARY_ROOT)
    inspection_path = _one(replication.BOUNDARY_INSPECTION_ROOT)
    if require_committed:
        for path in (boundary_path, inspection_path):
            strategy_discovery.require_committed(path)
    boundary = replication._load_hashed(
        boundary_path,
        identity_field="contract_sha256",
        expected_kind="oversold_fixed_rule_replication_boundary",
    )
    inspection = replication._load_hashed(
        inspection_path,
        identity_field="inspection_sha256",
        expected_kind="oversold_fixed_rule_replication_boundary_inspection",
    )
    inventory_path = replication.development_inventory_path(store)
    inventory = replication._read_gzip(inventory_path)
    if not (
        inspection["state"] == "BOUNDARY_INSPECTED_COLLECTION_READY"
        and inspection["valid"] is True
        and inspection["contract_sha256"] == boundary["contract_sha256"]
        and inventory["content_sha256"]
        == boundary["private_development_inventory"]["content_sha256"]
        and sha256_file(inventory_path)
        == boundary["private_development_inventory"]["file_sha256"]
    ):
        raise OversoldFixedRuleCollectionError("inventory chain drifted")
    # The reused collector expects this small public-contract shape.
    inventory_contract = {
        "contract_sha256": boundary["contract_sha256"],
        "inventory": {
            "private_content_sha256": inventory["content_sha256"],
            "private_file_sha256": sha256_file(inventory_path),
        },
    }
    return inventory_contract, inspection, inventory


@contextmanager
def configured():
    values = {
        "DATASET_ID": DATASET_ID,
        "SUCCESSOR_ID": SUCCESSOR_ID,
        "EXPOSURE_ID": EXPOSURE_ID,
        "INVENTORY_CONTRACT": _one(replication.BOUNDARY_ROOT),
        "INVENTORY_INSPECTION": _one(
            replication.BOUNDARY_INSPECTION_ROOT
        ),
        "OUTPUT_ROOT": OUTPUT_ROOT,
        "CONTRACT_ROOT": CONTRACT_ROOT,
        "CONTRACT_INSPECTION_ROOT": CONTRACT_INSPECTION_ROOT,
        "AUTHORIZATION_ROOT": AUTHORIZATION_ROOT,
        "STATUS_ROOT": STATUS_ROOT,
        "DATA_INSPECTION_ROOT": DATA_INSPECTION_ROOT,
        "_load_inventory": _load_inventory,
        "_inventory_path": replication.development_inventory_path,
        "_input_index_path": replication.development_input_index_path,
    }
    original = {name: getattr(base, name) for name in values}
    try:
        for name, value in values.items():
            setattr(base, name, value)
        yield
    finally:
        for name, value in original.items():
            setattr(base, name, value)


def freeze_contract(*, created_at: str) -> tuple[Path, dict[str, Any]]:
    with configured():
        return base.freeze_contract(created_at=created_at)


def inspect_contract(contract_path: Path) -> tuple[Path, dict[str, Any]]:
    strategy_discovery.require_committed(contract_path)
    with configured():
        contract = base._load_contract(contract_path)
        expected = base._contract_content(
            created_at=str(contract["created_at"]),
            store=HistoricalDayStore.from_env(),
        )
        checks = {
            "exact_rebuild": {
                key: value
                for key, value in contract.items()
                if key != "contract_sha256"
            }
            == expected,
            "complete_scope": contract["candidate_symbol_sessions"]
            == sum(
                len(symbols)
                for symbols in contract["scope"][
                    "symbols_by_date"
                ].values()
            ),
            "all_dates_signal_capable": not contract["zero_signal_dates"],
            "substitutions_forbidden": contract["collection"][
                "substitutions_allowed"
            ]
            is False,
            "authorization_precedes_access": contract["outcome_boundary"][
                "authorization_must_be_committed_before_cache_or_provider_access"
            ]
            is True,
            "confirmation_locked": contract["outcome_boundary"][
                "confirmation_access_permitted"
            ]
            is False,
            "no_provider_access": contract["provider_requests"] == 0,
            "no_outcomes": contract["return_metrics_computed"] == 0,
            "no_broker": contract["broker_actions"] == 0,
        }
        if not all(checks.values()):
            raise OversoldFixedRuleCollectionError(
                "collection contract inspection failed"
            )
        content = {
            "schema_version": 1,
            "artifact_kind": (
                "oversold_replication_development_collection_contract_inspection"
            ),
            "campaign_id": replication.CAMPAIGN_ID,
            "family_id": replication.FAMILY_ID,
            "successor_id": SUCCESSOR_ID,
            "dataset_id": DATASET_ID,
            "state": "DEVELOPMENT_COLLECTION_CONTRACT_INSPECTED_READY",
            "contract_path": replication._repo_path(contract_path),
            "contract_file_sha256": sha256_file(contract_path),
            "contract_sha256": contract["contract_sha256"],
            "checks": checks,
            "evaluation_dates": contract["date_count"],
            "signal_dates": contract["signal_date_count"],
            "candidate_symbol_sessions": contract[
                "candidate_symbol_sessions"
            ],
            "provider_access_permitted_after_committed_authorization": True,
            "confirmation_access_permitted": False,
            "broker_actions_permitted": False,
            "valid": True,
        }
        return base._publish(
            CONTRACT_INSPECTION_ROOT,
            "oversold-replication-development-collection-contract-inspection",
            content,
            "inspection_sha256",
        )


def authorize(
    contract_path: Path, inspection_path: Path
) -> tuple[Path, dict[str, Any]]:
    with configured():
        return base.authorize_development_exposure(
            contract_path, inspection_path
        )


def collect(
    contract_path: Path,
    inspection_path: Path,
    authorization_path: Path,
    *,
    max_dates: int | None = None,
) -> tuple[Path, dict[str, Any]]:
    with configured():
        return base.collect(
            contract_path,
            inspection_path,
            authorization_path,
            max_dates=max_dates,
        )


def inspect_data(
    contract_path: Path,
    inspection_path: Path,
    authorization_path: Path,
    status_path: Path,
) -> tuple[Path, dict[str, Any]]:
    with configured():
        return base.inspect_data(
            contract_path,
            inspection_path,
            authorization_path,
            status_path,
        )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    freeze = sub.add_parser("freeze-contract")
    freeze.add_argument("--created-at", required=True)
    inspect = sub.add_parser("inspect-contract")
    inspect.add_argument("contract", type=Path)
    auth = sub.add_parser("authorize-development")
    auth.add_argument("contract", type=Path)
    auth.add_argument("inspection", type=Path)
    collect_parser = sub.add_parser("collect")
    collect_parser.add_argument("contract", type=Path)
    collect_parser.add_argument("inspection", type=Path)
    collect_parser.add_argument("authorization", type=Path)
    collect_parser.add_argument("--max-dates", type=int)
    data = sub.add_parser("inspect-data")
    data.add_argument("contract", type=Path)
    data.add_argument("inspection", type=Path)
    data.add_argument("authorization", type=Path)
    data.add_argument("status", type=Path)
    args = parser.parse_args(argv)
    try:
        if args.command == "freeze-contract":
            path, value = freeze_contract(created_at=args.created_at)
        elif args.command == "inspect-contract":
            path, value = inspect_contract(args.contract)
        elif args.command == "authorize-development":
            path, value = authorize(args.contract, args.inspection)
        elif args.command == "collect":
            path, value = collect(
                args.contract,
                args.inspection,
                args.authorization,
                max_dates=args.max_dates,
            )
        else:
            path, value = inspect_data(
                args.contract,
                args.inspection,
                args.authorization,
                args.status,
            )
        print(
            json.dumps(
                {
                    "path": replication._repo_path(path),
                    "state": value["state"],
                    "candidate_symbol_sessions": value[
                        "candidate_symbol_sessions"
                    ],
                    "provider_telemetry": value.get(
                        "provider_telemetry",
                        {"requests": value.get("provider_requests", 0)},
                    ),
                    "confirmation_accessed": False,
                    "broker_actions": 0,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    except (
        KeyError,
        OSError,
        OversoldFixedRuleCollectionError,
        base.OversoldReplicationCollectionError,
        outcome_exposure.OutcomeExposureError,
        strategy_discovery.StrategyDiscoveryError,
        ValueError,
    ) as exc:
        print(
            json.dumps(
                {
                    "state": "BLOCKED",
                    "error": str(exc),
                    "confirmation_accessed": False,
                    "broker_actions": 0,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())

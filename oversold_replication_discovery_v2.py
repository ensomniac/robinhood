"""Freeze the completed-reserve oversold development-search family."""

from __future__ import annotations

import argparse
import hashlib
import json
from collections.abc import Sequence
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import oversold_replication_confirmation_v2 as confirmation
import oversold_replication_discovery as base
import oversold_replication_reserve_v2 as reserve
import strategy_discovery
from historical_store import HistoricalDayStore


PROJECT_ROOT = Path(__file__).resolve().parent
SUCCESSOR_ID = reserve.SUCCESSOR_ID
EXPERIMENT_ID = f"experiment-{SUCCESSOR_ID}"
DEFAULT_ROOT = PROJECT_ROOT / "strategy_tournament/v2/continuous"


class OversoldReplicationDiscoveryV2Error(RuntimeError):
    """The completed-reserve family boundary is incomplete or drifted."""


def _confirmation_chain(
    store: HistoricalDayStore,
) -> tuple[
    Path,
    dict[str, Any],
    Path,
    dict[str, Any],
    dict[str, Any],
]:
    contract_path = reserve._one(confirmation.CONTRACT_ROOT)
    inspection_path = reserve._one(confirmation.INSPECTION_ROOT)
    for path in (contract_path, inspection_path):
        strategy_discovery.require_committed(path)
    contract = reserve._load_hashed(
        contract_path,
        identity_field="contract_sha256",
        expected_kind=(
            "oversold_replication_confirmation_inventory_contract"
        ),
    )
    inspection = reserve._load_hashed(
        inspection_path,
        identity_field="inspection_sha256",
        expected_kind=(
            "oversold_replication_confirmation_inventory_inspection"
        ),
    )
    inventory = confirmation._read_gzip(
        confirmation._private_path(store)
    )
    if not (
        inspection["state"]
        == "CONFIRMATION_INVENTORY_INSPECTED_READY"
        and inspection["valid"] is True
        and inspection["contract_sha256"]
        == contract["contract_sha256"]
        and inventory["content_sha256"]
        == contract["private_inventory_content_sha256"]
        and inventory["lane"] == "confirmation"
        and inventory["target_outcomes_observed_or_derived"] is False
    ):
        raise OversoldReplicationDiscoveryV2Error(
            "completed confirmation inventory chain is invalid"
        )
    reserve._assert_dates_globally_untouched(
        inventory["evaluation_dates"],
        reserve.outcome_exposure.read_index(),
    )
    return (
        contract_path,
        contract,
        inspection_path,
        inspection,
        inventory,
    )


@contextmanager
def configured():
    values = {
        "SUCCESSOR_ID": SUCCESSOR_ID,
        "EXPERIMENT_ID": EXPERIMENT_ID,
        "_confirmation_chain": _confirmation_chain,
    }
    original = {name: getattr(base, name) for name in values}
    try:
        for name, value in values.items():
            setattr(base, name, value)
        yield
    finally:
        for name, value in original.items():
            setattr(base, name, value)


def build_family_contract(
    *,
    created_at: str,
    store: HistoricalDayStore | None = None,
) -> dict[str, Any]:
    with configured():
        contract = base.build_family_contract(
            created_at=created_at,
            store=store,
        )
    contract["successor_id"] = SUCCESSOR_ID
    contract["experiment_id"] = EXPERIMENT_ID
    contract["material_difference_rationale"] = (
        "This exact replication preserves the complete 32-trial development "
        "grid and inspected 100-signal 2023-2024 corpus while binding a "
        "separately inspected 40-session completed reserve whose target "
        "scanner inputs stop at 09:35 ET."
    )
    contract["implementation_files"] = [
        "oversold_replication_discovery_v2.py",
        "oversold_replication_plugin.py",
        "oversold_reversal_plugin.py",
        "oversold_replication_development_collection.py",
        "oversold_replication_confirmation_v2.py",
        "oversold_replication_confirmation_collection.py",
        "oversold_replication_confirmation_dataset.py",
        "oversold_scanner_builder_v2.py",
        "oversold_scanner_target_audit.py",
        "dense_strategy_runtime.py",
        "learning_statistics.py",
        "learning_experiment.py",
        "strategy_discovery.py",
        "outcome_exposure.py",
        "portfolio_maturity.py",
        "portfolio_config.toml",
    ]
    strategy_discovery._validate_family_contract(contract)
    return contract


def freeze_family(
    *,
    created_at: str,
    store: HistoricalDayStore | None = None,
) -> tuple[Path, dict[str, Any]]:
    for path in (
        Path(__file__).resolve(),
        Path(base.plugin.__file__).resolve(),
        PROJECT_ROOT / "oversold_reversal_plugin.py",
        PROJECT_ROOT / "oversold_replication_development_collection.py",
        PROJECT_ROOT / "oversold_replication_confirmation_v2.py",
        PROJECT_ROOT / "oversold_replication_confirmation_collection.py",
        PROJECT_ROOT / "oversold_replication_confirmation_dataset.py",
        PROJECT_ROOT / "oversold_scanner_builder_v2.py",
        PROJECT_ROOT / "oversold_scanner_target_audit.py",
        Path(base.runtime.__file__).resolve(),
        PROJECT_ROOT / "learning_statistics.py",
        PROJECT_ROOT / "learning_experiment.py",
        PROJECT_ROOT / "strategy_discovery.py",
        PROJECT_ROOT / "outcome_exposure.py",
        PROJECT_ROOT / "portfolio_maturity.py",
        PROJECT_ROOT / "portfolio_config.toml",
    ):
        strategy_discovery.require_committed(path)
    contract = build_family_contract(
        created_at=created_at,
        store=store,
    )
    digest = hashlib.sha256(base._canonical(contract)).hexdigest()
    path = (
        DEFAULT_ROOT
        / SUCCESSOR_ID
        / "family-contract"
        / f"contract-{digest}.json"
    )
    base._write(path, contract)
    return path, contract


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("freeze-family", "status"))
    parser.add_argument("--created-at")
    args = parser.parse_args(argv)
    try:
        if args.command == "status":
            paths = sorted(
                (DEFAULT_ROOT / SUCCESSOR_ID / "family-contract").glob(
                    "contract-*.json"
                )
            )
            value = {
                "state": (
                    "FAMILY_FROZEN"
                    if len(paths) == 1
                    else "READY_TO_FREEZE"
                ),
                "contracts": len(paths),
                "successor_id": SUCCESSOR_ID,
            }
        else:
            if not args.created_at:
                raise OversoldReplicationDiscoveryV2Error(
                    "--created-at is required"
                )
            path, contract = freeze_family(
                created_at=args.created_at
            )
            value = {
                "state": "FAMILY_FROZEN",
                "path": base._repo_path(path),
                "successor_id": SUCCESSOR_ID,
                "trials": base.development._trial_count(),
                "development_signal_dates": len(
                    contract["development_signal_dates"]
                ),
                "confirmation_signal_capacity": contract[
                    "confirmation_signal_capacity"
                ],
            }
        print(json.dumps(value, indent=2, sort_keys=True))
        return 0
    except (
        KeyError,
        OSError,
        OversoldReplicationDiscoveryV2Error,
        base.OversoldReplicationDiscoveryError,
        reserve.OversoldReplicationReserveError,
        ValueError,
    ) as exc:
        print(
            json.dumps(
                {"state": "BLOCKED", "error": str(exc)},
                indent=2,
                sort_keys=True,
            )
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())

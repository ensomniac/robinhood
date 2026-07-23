"""Independently inspect the rolling strategy-discovery authorization."""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import rolling_discovery_authorization as authorization


class RollingDiscoveryAuthorizationInspectionError(RuntimeError):
    """The rolling authorization cannot be independently rebuilt."""


def inspect(
    authorization_path: Path,
    *,
    status_path: Path = authorization.DEFAULT_STATUS,
) -> dict[str, Any]:
    observed = authorization.load_authorization(authorization_path)
    expected = authorization.build_authorization()
    pending = authorization._read(status_path)
    if observed != expected:
        raise RollingDiscoveryAuthorizationInspectionError(
            "rolling authorization does not independently rebuild"
        )
    for binding in observed["terminal_predecessors"]:
        path = authorization.PROJECT_ROOT / binding["path"]
        if authorization._file_hash(path) != binding["file_sha256"]:
            raise RollingDiscoveryAuthorizationInspectionError(
                f"terminal predecessor drifted: {binding['candidate_id']}"
            )
    if not (
        pending.get("state") == "PENDING_INDEPENDENT_INSPECTION"
        and pending.get("authorization_sha256")
        == observed["authorization_sha256"]
        and pending.get("available_slot_count") == 0
        and pending.get("provider_access_permitted") is False
        and pending.get("target_outcome_access_permitted") is False
        and pending.get("broker_actions_permitted") is False
        and observed.get("active_family_count") == 0
        and observed.get("released_slot_count") == 3
        and observed.get("available_slot_count") == 3
        and all(observed["selection_accounting"].values())
        and observed["preservation_contract"]
        == {
            "failed_corpus_parameter_repair_allowed": False,
            "confirmation_reuse_allowed": False,
            "evidence_or_promotion_gate_weakening_allowed": False,
            "risk_protection_or_broker_gate_weakening_allowed": False,
            "maximum_parallel_outcome_active_families": 3,
        }
        and observed.get("provider_access_permitted_before_inspection") is False
        and observed.get("target_outcome_access_permitted_before_family_freeze")
        is False
        and observed.get("broker_actions_permitted") is False
    ):
        raise RollingDiscoveryAuthorizationInspectionError(
            "rolling authorization boundary differs"
        )
    result: dict[str, Any] = {
        "schema_version": authorization.SCHEMA_VERSION,
        "campaign_id": authorization.CAMPAIGN_ID,
        "authorization_path": authorization._path_text(authorization_path),
        "authorization_sha256": observed["authorization_sha256"],
        "state": "ROLLING_DISCOVERY_AUTHORIZED",
        "activation_policy": authorization.POLICY,
        "active_family_count": 0,
        "available_slot_count": 3,
        "selection_accounting_complete": True,
        "terminal_predecessors_inspected": 3,
        "provider_access_permitted": True,
        "target_outcome_access_permitted": False,
        "broker_actions_permitted": False,
        "inspection": {
            "explicit_user_authorization_rebuilt": True,
            "terminal_predecessors_rebuilt": True,
            "rolling_slot_limit_rebuilt": True,
            "cumulative_selection_accounting_rebuilt": True,
            "all_other_gates_preserved": True,
            "valid": True,
        },
        "valid": True,
    }
    temporary = status_path.with_name(f".{status_path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_text(
            json.dumps(result, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        os.replace(temporary, status_path)
    finally:
        temporary.unlink(missing_ok=True)
    return result


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("authorization", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        result = inspect(args.authorization)
    except (
        OSError,
        ValueError,
        authorization.RollingDiscoveryAuthorizationError,
        RollingDiscoveryAuthorizationInspectionError,
    ) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}), file=sys.stderr)
        return 2
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

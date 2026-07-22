"""Independently inspect portfolio research campaign v2 authorization."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import portfolio_successor_activation as activation


class PortfolioSuccessorActivationInspectionError(RuntimeError):
    """The frozen successor authorization does not independently rebuild."""


def inspect_authorization(
    *, authorization_path: Path, status_path: Path = activation.DEFAULT_STATUS
) -> dict[str, Any]:
    observed = activation.load_authorization(authorization_path)
    expected = activation.build_authorization()
    status = activation._read_json(status_path)
    if observed != expected:
        raise PortfolioSuccessorActivationInspectionError(
            "successor authorization does not rebuild"
        )
    for relative, digest in observed["implementation_and_plan_hashes"].items():
        path = activation.PROJECT_ROOT / relative
        if activation._hash_file(path) != digest:
            raise PortfolioSuccessorActivationInspectionError(
                f"bound authorization artifact drifted: {relative}"
            )
    if not (
        status.get("authorization_sha256")
        == observed["authorization_sha256"]
        and status.get("status") == "AUTHORIZED_PENDING_INSPECTION"
        and status.get("inspected") is False
        and status.get("candidate_preregistration_permitted") is False
        and status.get("provider_access_permitted") is False
        and status.get("outcome_access_permitted") is False
        and status.get("broker_actions_permitted") is False
        and observed["authorization_source"] == "EXPLICIT_USER_MESSAGE"
        and observed["authorization_text"] == activation.AUTHORIZATION_TEXT
        and observed["preservation_contract"]
        == {
            "v1_evidence_remains_adverse_and_immutable": True,
            "failed_corpus_parameter_repair_allowed": False,
            "prior_evidence_relabeling_allowed": False,
            "promotion_or_risk_gate_weakening_allowed": False,
        }
        and observed["first_authorized_action"]
        == "PRIORITY_ONE_OUTCOME_BLIND_CAPACITY_PREFLIGHT"
        and observed["outcome_access_permitted"] is False
        and observed["broker_actions_permitted"] is False
    ):
        raise PortfolioSuccessorActivationInspectionError(
            "successor authorization boundary differs"
        )
    result = {
        "schema_version": activation.SCHEMA_VERSION,
        "campaign_id": activation.CAMPAIGN_ID,
        "authorization_sha256": observed["authorization_sha256"],
        "status": "AUTHORIZED_READY",
        "inspected": True,
        "maximum_new_mechanism_families_per_iso_week": 3,
        "first_authorized_action": observed["first_authorized_action"],
        "candidate_preregistration_permitted": True,
        "provider_access_permitted": False,
        "outcome_access_permitted": False,
        "broker_actions_permitted": False,
        "inspection": {
            "explicit_authorization_text_rebuilt": True,
            "proposal_scope_rebuilt": True,
            "implementation_and_plan_hashes_rebuilt": True,
            "v1_evidence_preservation_rebuilt": True,
            "risk_and_promotion_gates_preserved": True,
            "valid": True,
        },
        "valid": True,
    }
    status_path.write_text(
        json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return result


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("authorization", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        result: dict[str, Any] = inspect_authorization(
            authorization_path=args.authorization
        )
    except (
        PortfolioSuccessorActivationInspectionError,
        activation.PortfolioSuccessorActivationError,
        OSError,
        ValueError,
    ) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}), file=sys.stderr)
        return 2
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

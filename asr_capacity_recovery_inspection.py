"""Independently inspect the frozen ASR v2 source recovery contract."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import asr_capacity_recovery as recovery


class AsrCapacityRecoveryInspectionError(RuntimeError):
    """The ASR recovery contract does not independently rebuild."""


def inspect_contract(
    contract_path: Path, *, status_path: Path = recovery.DEFAULT_STATUS
) -> dict[str, Any]:
    recorded = recovery.load_contract(contract_path)
    expected = recovery.build_contract()
    status = recovery.read_object(status_path)
    for relative, digest in recorded["implementation_hashes"].items():
        if recovery.file_hash(recovery.PROJECT_ROOT / relative) != digest:
            raise AsrCapacityRecoveryInspectionError(
                f"bound implementation drifted: {relative}"
            )
    window = recorded["source_contract"]["window_algorithm"]
    if not (
        recorded == expected
        and status.get("status") == "RECOVERY_CONTRACT_PENDING_INSPECTION"
        and status.get("contract_sha256") == recorded["contract_sha256"]
        and recorded["family_budget_contract"]["new_mechanism_family_slot_consumed"]
        is False
        and recorded["family_budget_contract"]["mechanism_rule_changed"] is False
        and recorded["preserved_predecessor"]["same_contract_repair_permitted"]
        is False
        and recorded["preserved_predecessor"]["evidence_credit_inherited"] is False
        and recorded["prior_source_metadata_exposure"]["market_outcomes_accessed"]
        is False
        and recorded["source_contract"]["search_phrases"]
        == list(recovery.v1.SEARCH_PHRASES)
        and window["traversal"] == "depth_first_oldest_half_first"
        and window["single_date_inexact_action"] == "fail_closed"
        and window["maximum_split_depth"] == 16
        and recorded["event_contract"]
        == recovery.v1.build_contract()["event_contract"]
        and recorded["capacity_gate"]
        == recovery.v1.build_contract()["capacity_gate"]
        and recorded["access_contract"][
            "sec_search_access_before_independent_inspection_permitted"
        ]
        is False
        and recorded["access_contract"]["matched_document_access_permitted_by_this_contract"]
        is False
        and recorded["access_contract"]["market_price_access_permitted"] is False
        and recorded["access_contract"]["forward_return_access_permitted"] is False
        and recorded["access_contract"]["broker_actions_permitted"] is False
        and recorded["filing_hit_count"] is None
        and recorded["verified_event_count"] is None
        and recorded["market_outcomes_accessed"] is False
        and recorded["broker_actions"] == 0
    ):
        raise AsrCapacityRecoveryInspectionError(
            "ASR recovery contract or zero-outcome boundary differs"
        )
    result: dict[str, Any] = {
        "schema_version": recovery.SCHEMA_VERSION,
        "inspection_kind": "outcome-blind-asr-source-recovery-contract-inspection",
        "campaign_id": recovery.CAMPAIGN_ID,
        "candidate_id": recovery.CANDIDATE_ID,
        "contract_sha256": recorded["contract_sha256"],
        "status": "RECOVERY_CONTRACT_INSPECTED",
        "predecessor_retirement_rebuilt": True,
        "preoutcome_authorization_rebuilt": True,
        "same_mechanism_family_rebuilt": True,
        "zero_new_family_slot_rebuilt": True,
        "recursive_window_algorithm_rebuilt": True,
        "search_phrases_rebuilt": True,
        "event_semantics_rebuilt": True,
        "capacity_dispositions_rebuilt": True,
        "zero_outcome_boundary_rebuilt": True,
        "sec_search_access_permitted": True,
        "provider_access_scope": (
            "only recursive exact-total EFTS search pages for the four preserved "
            "phrases and 2010-01-01 through 2025-12-31"
        ),
        "matched_document_access_permitted": False,
        "market_price_access_permitted": False,
        "outcome_access_permitted": False,
        "broker_actions_permitted": False,
        "filing_hit_count": None,
        "verified_event_count": None,
        "market_outcomes_accessed": False,
        "maturity_effect": "NONE",
        "valid": True,
    }
    result["inspection_sha256"] = recovery.self_hash(result, "inspection_sha256")
    recovery.write_object(result, status_path)
    return result


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("inspect",))
    parser.add_argument("contract", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        result = inspect_contract(args.contract)
    except (
        AsrCapacityRecoveryInspectionError,
        recovery.AsrCapacityRecoveryError,
        OSError,
        ValueError,
    ) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}), file=sys.stderr)
        return 2
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

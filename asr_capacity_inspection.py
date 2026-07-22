"""Independently inspect the frozen ASR outcome-blind capacity contract."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import asr_capacity as capacity


class AsrCapacityInspectionError(RuntimeError):
    """The ASR capacity contract does not independently rebuild."""


def inspect_contract(
    contract_path: Path, *, status_path: Path = capacity.DEFAULT_STATUS
) -> dict[str, Any]:
    recorded = capacity.load_contract(contract_path)
    expected = capacity.build_contract()
    status = capacity._read_object(status_path)
    for relative, digest in recorded["authority_hashes"].items():
        if capacity._file_hash(capacity.PROJECT_ROOT / relative) != digest:
            raise AsrCapacityInspectionError(f"bound authority drifted: {relative}")
    if not (
        recorded == expected
        and status.get("status") == "CAPACITY_CONTRACT_PENDING_INSPECTION"
        and status.get("contract_sha256") == recorded["contract_sha256"]
        and recorded["new_mechanism_family_slot"] == 3
        and recorded["source_contract"]["collection_start"] == "2010-01-01"
        and recorded["source_contract"]["collection_end_inclusive"] == "2025-12-31"
        and len(recorded["source_contract"]["search_phrases"]) == 4
        and recorded["event_contract"]["all_required_positive_groups_must_match"]
        is True
        and recorded["capacity_gate"]["retire_below_verified_events"] == 50
        and recorded["capacity_gate"]["fast_lane_minimum_verified_events"] == 100
        and recorded["denominator_contract"]["terminal_reason_required_for_every_hit"]
        is True
        and recorded["access_contract"]["sec_access_before_contract_inspection_permitted"]
        is False
        and recorded["access_contract"]["market_price_access_permitted"] is False
        and recorded["access_contract"]["forward_return_access_permitted"] is False
        and recorded["access_contract"]["broker_actions_permitted"] is False
        and recorded["filing_hit_count"] is None
        and recorded["verified_event_count"] is None
        and recorded["market_price_values_accessed"] == 0
        and recorded["returns_computed"] == 0
        and recorded["market_outcomes_accessed"] is False
        and recorded["broker_actions"] == 0
    ):
        raise AsrCapacityInspectionError("ASR contract or zero-outcome boundary differs")
    result: dict[str, Any] = {
        "schema_version": capacity.SCHEMA_VERSION,
        "inspection_kind": "outcome-blind-asr-capacity-contract-inspection",
        "campaign_id": capacity.CAMPAIGN_ID,
        "candidate_id": capacity.CANDIDATE_ID,
        "contract_sha256": recorded["contract_sha256"],
        "status": "CAPACITY_CONTRACT_INSPECTED",
        "date_window_rebuilt": True,
        "search_phrases_rebuilt": True,
        "eligibility_patterns_rebuilt": True,
        "capacity_dispositions_rebuilt": True,
        "authority_hashes_rebuilt": True,
        "zero_outcome_boundary_rebuilt": True,
        "sec_source_access_permitted": True,
        "provider_access_scope": (
            "the exact EFTS phrases, date window, matched SEC Archives documents, "
            "and accession submissions frozen by the contract"
        ),
        "capacity_classification_permitted": True,
        "market_price_access_permitted": False,
        "outcome_access_permitted": False,
        "broker_actions_permitted": False,
        "filing_hit_count": None,
        "verified_event_count": None,
        "returns_computed": 0,
        "market_outcomes_accessed": False,
        "maturity_effect": "NONE",
        "valid": True,
    }
    result["inspection_sha256"] = capacity._self_hash(result, "inspection_sha256")
    capacity._write_object(result, status_path)
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
        AsrCapacityInspectionError,
        capacity.AsrCapacityError,
        OSError,
        ValueError,
    ) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}), file=sys.stderr)
        return 2
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

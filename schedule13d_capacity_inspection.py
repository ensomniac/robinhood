"""Independently inspect the v2 Schedule 13D capacity contract."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import schedule13d_capacity as capacity


class Schedule13dCapacityInspectionError(RuntimeError):
    """The Schedule 13D capacity contract does not independently rebuild."""


def inspect_contract(
    contract_path: Path, *, status_path: Path = capacity.DEFAULT_STATUS
) -> dict[str, Any]:
    recorded = capacity.load_contract(contract_path)
    expected = capacity.build_contract()
    status = capacity._read_json(status_path)
    if recorded != expected:
        raise Schedule13dCapacityInspectionError("capacity contract does not rebuild")
    for relative, digest in recorded["implementation_and_authority_hashes"].items():
        if capacity.successor._hash_file(capacity.PROJECT_ROOT / relative) != digest:
            raise Schedule13dCapacityInspectionError(
                f"bound Schedule 13D artifact drifted: {relative}"
            )
    if not (
        status.get("status") == "CAPACITY_CONTRACT_PENDING_INSPECTION"
        and status.get("contract_sha256") == recorded["contract_sha256"]
        and status.get("sec_source_access_permitted") is False
        and status.get("capacity_classification_permitted") is False
        and status.get("market_price_access_permitted") is False
        and status.get("outcome_access_permitted") is False
        and status.get("broker_actions_permitted") is False
        and recorded["new_mechanism_family_slot"] == 2
        and recorded["maximum_new_mechanism_families_this_iso_week"] == 3
        and recorded["source_contract"]["forms_included"] == ["SC 13D"]
        and recorded["event_contract"]["initial_schedule_only"] is True
        and recorded["security_contract"][
            "current_or_future_symbol_mapping_permitted"
        ]
        is False
        and recorded["capacity_gate"]["minimum_verified_events"] == 80
        and recorded["denominator_contract"][
            "terminal_reason_required_for_every_index_row"
        ]
        is True
        and recorded["access_contract"]["market_price_access_permitted"] is False
        and recorded["access_contract"]["stage0_outcome_access_permitted"] is False
        and recorded["access_contract"]["broker_actions_permitted"] is False
        and recorded["filing_count"] is None
        and recorded["verified_event_count"] is None
        and recorded["market_outcomes_accessed"] is False
    ):
        raise Schedule13dCapacityInspectionError(
            "Schedule 13D capacity, anti-tuning, or access boundary differs"
        )
    result = {
        "schema_version": capacity.SCHEMA_VERSION,
        "inspection_kind": "outcome-blind-capacity-contract-inspection",
        "campaign_id": capacity.CAMPAIGN_ID,
        "candidate_id": capacity.CANDIDATE_ID,
        "contract_sha256": recorded["contract_sha256"],
        "contract_file_sha256": capacity.successor._hash_file(contract_path),
        "status": "CAPACITY_CONTRACT_INSPECTED",
        "sec_source_access_permitted": True,
        "sec_source_access_scope": (
            "exact quarterly indexes, accession-bound SC 13D documents, and causal "
            "issuer-symbol filings only"
        ),
        "capacity_classification_permitted": True,
        "market_price_access_permitted": False,
        "outcome_access_permitted": False,
        "broker_actions_permitted": False,
        "inspection": {
            "authorization_and_family_order_rebuilt": True,
            "initial_form_and_date_scope_rebuilt": True,
            "acceptance_timing_rebuilt": True,
            "item4_semantics_rebuilt": True,
            "causal_symbol_mapping_rebuilt": True,
            "common_share_filter_rebuilt": True,
            "deduplication_and_cooldown_rebuilt": True,
            "complete_denominator_rebuilt": True,
            "minimum_80_event_gate_rebuilt": True,
            "implementation_hashes_rebuilt": True,
            "zero_market_outcome_boundary_rebuilt": True,
            "valid": True,
        },
        "filing_count": None,
        "verified_event_count": None,
        "returns_computed": 0,
        "market_outcomes_accessed": False,
        "maturity_effect": "NONE",
        "valid": True,
    }
    result["inspection_sha256"] = capacity.successor._self_hash(
        result, "inspection_sha256"
    )
    capacity._write_json(result, status_path)
    return result


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("contract", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        result: dict[str, Any] = inspect_contract(args.contract)
    except (
        Schedule13dCapacityInspectionError,
        capacity.Schedule13dCapacityError,
        OSError,
        ValueError,
    ) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}), file=sys.stderr)
        return 2
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

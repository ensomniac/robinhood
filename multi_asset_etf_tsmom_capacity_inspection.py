"""Independently inspect the v2 ETF trend outcome-blind capacity contract."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import multi_asset_etf_tsmom_capacity as capacity


class MultiAssetEtfTsmomCapacityInspectionError(RuntimeError):
    """The priority-one capacity contract does not independently rebuild."""


def inspect_contract(
    contract_path: Path, *, status_path: Path = capacity.DEFAULT_STATUS
) -> dict[str, Any]:
    recorded = capacity.load_contract(contract_path)
    expected = capacity.build_contract()
    status = capacity._read_json(status_path)
    if recorded != expected:
        raise MultiAssetEtfTsmomCapacityInspectionError(
            "capacity contract does not rebuild"
        )
    for relative, digest in recorded["implementation_and_authority_hashes"].items():
        if capacity.successor._hash_file(capacity.PROJECT_ROOT / relative) != digest:
            raise MultiAssetEtfTsmomCapacityInspectionError(
                f"bound capacity artifact drifted: {relative}"
            )
    if not (
        status.get("contract_sha256") == recorded["contract_sha256"]
        and status.get("status") == "CAPACITY_CONTRACT_PENDING_INSPECTION"
        and status.get("provider_access_permitted") is False
        and status.get("capacity_signal_count_permitted") is False
        and status.get("outcome_access_permitted") is False
        and status.get("broker_actions_permitted") is False
        and recorded["new_mechanism_family_slot"] == 1
        and recorded["maximum_new_mechanism_families_this_iso_week"] == 3
        and recorded["distinctness_contract"]["uses_own_price_history_only"]
        is True
        and recorded["distinctness_contract"]["cross_sectional_ranking_used"]
        is False
        and recorded["schedule_contract"][
            "maximum_candidate_opportunities_per_week"
        ]
        == 1
        and recorded["signal_contract"]["lookback_sessions"] == 252
        and recorded["capacity_gate"]["minimum_stage0_signals"] == 30
        and recorded["capacity_gate"]["minimum_total_historical_signals"] == 50
        and recorded["access_contract"]["stage0_outcome_access_permitted"]
        is False
        and recorded["access_contract"]["broker_actions_permitted"] is False
        and recorded["stage0_handoff"]["maximum_holding_sessions"] == 5
        and recorded["stage0_handoff"]["cost_grid_bps_per_side"] == [5, 10, 20]
    ):
        raise MultiAssetEtfTsmomCapacityInspectionError(
            "capacity, anti-tuning, or access boundary differs"
        )
    result = {
        "schema_version": capacity.SCHEMA_VERSION,
        "inspection_kind": "outcome-blind-capacity-contract-inspection",
        "campaign_id": capacity.CAMPAIGN_ID,
        "candidate_id": capacity.CANDIDATE_ID,
        "contract_sha256": recorded["contract_sha256"],
        "contract_file_sha256": capacity.successor._hash_file(contract_path),
        "status": "CAPACITY_CONTRACT_INSPECTED",
        "provider_access_permitted": True,
        "provider_access_scope": "exact frozen daily input collection only",
        "capacity_signal_count_permitted": True,
        "outcome_access_permitted": False,
        "broker_actions_permitted": False,
        "inspection": {
            "authorization_rebuilt": True,
            "weekly_family_limit_rebuilt": True,
            "distinct_mechanism_rebuilt": True,
            "universe_and_dates_rebuilt": True,
            "causal_signal_rule_rebuilt": True,
            "missing_data_policy_rebuilt": True,
            "capacity_thresholds_rebuilt": True,
            "implementation_hashes_rebuilt": True,
            "zero_outcome_boundary_rebuilt": True,
            "valid": True,
        },
        "signal_count": None,
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
        MultiAssetEtfTsmomCapacityInspectionError,
        capacity.MultiAssetEtfTsmomCapacityError,
        OSError,
        ValueError,
    ) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}), file=sys.stderr)
        return 2
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

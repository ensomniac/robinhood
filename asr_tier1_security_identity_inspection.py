"""Inspect the frozen identity rule applied to ASR tier one."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import asr_tier1_security_identity as tier1_identity


class AsrTier1SecurityIdentityInspectionError(RuntimeError):
    """The tier-one identity artifact does not independently rebuild."""


def inspect_contract(
    path: Path,
    *,
    status_path: Path = tier1_identity.DEFAULT_CONTRACT_STATUS,
) -> dict[str, Any]:
    recorded = tier1_identity.load_contract(path)
    expected = tier1_identity.build_contract()
    status = tier1_identity.read_object(status_path)
    for relative, digest in recorded["implementation_hashes"].items():
        if (
            tier1_identity.identity.file_hash(tier1_identity.PROJECT_ROOT / relative)
            != digest
        ):
            raise AsrTier1SecurityIdentityInspectionError(
                f"tier-one identity implementation drifted: {relative}"
            )
    if not (
        recorded == expected
        and status.get("status") == "TIER1_IDENTITY_CONTRACT_PENDING_INSPECTION"
        and recorded["source_lineage"]["semantic_event_count"] == 59
        and recorded["source_lineage"]["qualified_accession_count"] == 27
        and recorded["identity_rule"]["rule_change_permitted"] is False
        and recorded["capacity_contract"][
            "same_accession_events_are_one_trade_opportunity"
        ]
        is True
        and recorded["access_contract"]["new_provider_access_permitted"] is False
        and recorded["access_contract"]["market_price_access_permitted"] is False
        and recorded["verified_event_count"] is None
        and recorded["market_outcomes_accessed"] is False
    ):
        raise AsrTier1SecurityIdentityInspectionError(
            "tier-one identity contract or zero-outcome boundary differs"
        )
    result: dict[str, Any] = {
        "schema_version": 1,
        "inspection_kind": "outcome-blind-asr-tier1-identity-contract-inspection",
        "campaign_id": tier1_identity.semantic.tier.capacity.CAMPAIGN_ID,
        "candidate_id": tier1_identity.semantic.tier.capacity.CANDIDATE_ID,
        "application_id": tier1_identity.APPLICATION_ID,
        "contract_sha256": recorded["contract_sha256"],
        "status": "TIER1_IDENTITY_CONTRACT_INSPECTED",
        "source_lineage_rebuilt": True,
        "frozen_identity_rule_rebuilt": True,
        "independent_signal_count_rule_rebuilt": True,
        "local_identity_access_permitted": True,
        "provider_access_permitted": False,
        "market_price_access_permitted": False,
        "outcome_access_permitted": False,
        "broker_actions_permitted": False,
        "verified_event_count": None,
        "market_outcomes_accessed": False,
        "maturity_effect": "NONE",
        "valid": True,
    }
    result["inspection_sha256"] = tier1_identity.self_hash(result, "inspection_sha256")
    tier1_identity.write_object(result, status_path)
    return result


def inspect_result(
    path: Path,
    *,
    output_root: Path = tier1_identity.DEFAULT_RESULT_ROOT / "inspections",
    store_root: Path | None = None,
) -> tuple[Path, dict[str, Any]]:
    recorded = tier1_identity.read_object(path)
    if recorded.get("result_sha256") != tier1_identity.self_hash(
        recorded, "result_sha256"
    ):
        raise AsrTier1SecurityIdentityInspectionError(
            "tier-one identity result hash is invalid"
        )
    root = store_root or tier1_identity.semantic.tier.shared._store().root
    info = recorded["private_result"]
    private_path = root / str(info["cache_relative_path"])
    compressed = private_path.read_bytes() if private_path.is_file() else b""
    private = tier1_identity._read_gzip(private_path)
    _result, _inspection, semantic_result, collection = tier1_identity._lineage(root)
    rebuilt = tier1_identity.rebuild_result(semantic_result, collection, root)
    if not (
        hashlib.sha256(compressed).hexdigest() == info["file_sha256"]
        and len(compressed) == info["bytes"]
        and private.get("private_result_sha256") == info["private_result_sha256"]
        and private.get("private_result_sha256")
        == tier1_identity.self_hash(private, "private_result_sha256")
        and private == rebuilt
        and recorded["verified_event_count"] == private["verified_event_count"]
        and recorded["independent_disclosure_signal_count"]
        == private["independent_disclosure_signal_count"]
        and recorded["security_identity_resolution_complete"] is True
        and recorded["market_price_values_accessed"] == 0
        and recorded["returns_computed"] == 0
        and recorded["market_outcomes_accessed"] is False
        and recorded["broker_actions"] == 0
    ):
        raise AsrTier1SecurityIdentityInspectionError(
            "tier-one identity result does not independently rebuild"
        )
    result: dict[str, Any] = {
        "schema_version": 1,
        "inspection_kind": "outcome-blind-asr-tier1-identity-result-inspection",
        "campaign_id": tier1_identity.semantic.tier.capacity.CAMPAIGN_ID,
        "candidate_id": tier1_identity.semantic.tier.capacity.CANDIDATE_ID,
        "application_id": tier1_identity.APPLICATION_ID,
        "result_sha256": recorded["result_sha256"],
        "private_result_sha256": private["private_result_sha256"],
        "terminal_counts_rebuilt": True,
        "security_identity_rebuilt": True,
        "independent_signal_count_rebuilt": True,
        "verified_event_count": recorded["verified_event_count"],
        "independent_disclosure_signal_count": recorded[
            "independent_disclosure_signal_count"
        ],
        "combined_capacity_contract_freeze_permitted": True,
        "market_price_access_permitted": False,
        "outcome_access_permitted": False,
        "broker_actions_permitted": False,
        "market_outcomes_accessed": False,
        "maturity_effect": "NONE",
        "valid": True,
    }
    result["inspection_sha256"] = tier1_identity.self_hash(result, "inspection_sha256")
    output_root.mkdir(parents=True, exist_ok=True)
    output = output_root / f"tier1-identity-{result['inspection_sha256']}.json"
    tier1_identity.write_object(result, output)
    return output, result


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("inspect-contract", "inspect-result"))
    parser.add_argument("path", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "inspect-contract":
            result: dict[str, Any] = inspect_contract(args.path)
        else:
            output, value = inspect_result(args.path)
            result = {
                **value,
                "written": str(output.relative_to(tier1_identity.PROJECT_ROOT)),
            }
    except (
        AsrTier1SecurityIdentityInspectionError,
        tier1_identity.AsrTier1SecurityIdentityError,
        OSError,
        ValueError,
    ) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}), file=sys.stderr)
        return 2
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

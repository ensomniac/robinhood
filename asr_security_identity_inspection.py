"""Inspect ASR filing-cover security-identity artifacts."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import asr_security_identity as identity


class AsrSecurityIdentityInspectionError(RuntimeError):
    """The security-identity artifact does not independently rebuild."""


def inspect_contract(
    path: Path,
    *,
    status_path: Path = identity.DEFAULT_CONTRACT_STATUS,
) -> dict[str, Any]:
    recorded = identity.load_contract(path)
    expected = identity.build_contract()
    status = identity.read_object(status_path)
    for relative, digest in recorded["implementation_hashes"].items():
        if identity.file_hash(identity.PROJECT_ROOT / relative) != digest:
            raise AsrSecurityIdentityInspectionError(
                f"security-identity implementation drifted: {relative}"
            )
    if not (
        recorded == expected
        and status.get("status") == "SECURITY_IDENTITY_CONTRACT_PENDING_INSPECTION"
        and recorded["source_lineage"]["precise_semantic_event_count"] == 507
        and recorded["identity_contract"][
            "exactly_one_eligible_identity_required_per_accession"
        ]
        is True
        and recorded["identity_contract"]["external_identity_substitution_permitted"]
        is False
        and recorded["capacity_contract"]["fast_lane_threshold"] == 100
        and recorded["access_contract"]["new_provider_access_permitted"] is False
        and recorded["access_contract"]["market_price_access_permitted"] is False
        and recorded["verified_event_count"] is None
        and recorded["market_outcomes_accessed"] is False
    ):
        raise AsrSecurityIdentityInspectionError(
            "security-identity contract or zero-outcome boundary differs"
        )
    result: dict[str, Any] = {
        "schema_version": 1,
        "inspection_kind": "outcome-blind-asr-security-identity-contract-inspection",
        "campaign_id": identity.resolution.tier2a.capacity.CAMPAIGN_ID,
        "candidate_id": identity.resolution.tier2a.capacity.CANDIDATE_ID,
        "identity_id": identity.IDENTITY_ID,
        "contract_sha256": recorded["contract_sha256"],
        "status": "SECURITY_IDENTITY_CONTRACT_INSPECTED",
        "source_lineage_rebuilt": True,
        "cover_table_classifier_rebuilt": True,
        "ambiguity_and_exchange_gates_rebuilt": True,
        "capacity_thresholds_rebuilt": True,
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
    result["inspection_sha256"] = identity.self_hash(result, "inspection_sha256")
    identity.write_object(result, status_path)
    return result


def inspect_result(
    path: Path,
    *,
    output_root: Path = identity.DEFAULT_RESULT_ROOT / "inspections",
    store_root: Path | None = None,
) -> tuple[Path, dict[str, Any]]:
    recorded = identity.read_object(path)
    if recorded.get("result_sha256") != identity.self_hash(recorded, "result_sha256"):
        raise AsrSecurityIdentityInspectionError(
            "security-identity result hash is invalid"
        )
    root = store_root or identity.resolution.tier1.shared._store().root
    info = recorded["private_result"]
    private_path = root / str(info["cache_relative_path"])
    compressed = private_path.read_bytes() if private_path.is_file() else b""
    private = identity._read_gzip(private_path)
    _result, _inspection, precise, collection = identity._lineage(root)
    rebuilt = identity.rebuild_result(precise, collection, root)
    if not (
        hashlib.sha256(compressed).hexdigest() == info["file_sha256"]
        and len(compressed) == info["bytes"]
        and private.get("private_result_sha256") == info["private_result_sha256"]
        and private.get("private_result_sha256")
        == identity.self_hash(private, "private_result_sha256")
        and private == rebuilt
        and recorded["verified_event_count"] == private["verified_event_count"]
        and recorded["capacity_disposition"] == private["capacity_disposition"]
        and recorded["security_identity_resolution_complete"] is True
        and recorded["market_price_values_accessed"] == 0
        and recorded["returns_computed"] == 0
        and recorded["market_outcomes_accessed"] is False
        and recorded["broker_actions"] == 0
    ):
        raise AsrSecurityIdentityInspectionError(
            "security-identity result does not independently rebuild"
        )
    admitted = (
        recorded["capacity_disposition"] == "ADMITTED_TO_DEVELOPMENT_SEARCH_PIPELINE"
        and recorded["verified_event_count"] >= 100
    )
    result: dict[str, Any] = {
        "schema_version": 1,
        "inspection_kind": "outcome-blind-asr-security-identity-result-inspection",
        "campaign_id": identity.resolution.tier2a.capacity.CAMPAIGN_ID,
        "candidate_id": identity.resolution.tier2a.capacity.CANDIDATE_ID,
        "identity_id": identity.IDENTITY_ID,
        "result_sha256": recorded["result_sha256"],
        "private_result_sha256": private["private_result_sha256"],
        "terminal_counts_rebuilt": True,
        "security_identity_rebuilt": True,
        "event_deduplication_rebuilt": True,
        "verified_event_count": recorded["verified_event_count"],
        "capacity_disposition": recorded["capacity_disposition"],
        "development_search_contract_freeze_permitted": admitted,
        "market_price_access_permitted": False,
        "outcome_access_permitted": False,
        "broker_actions_permitted": False,
        "market_outcomes_accessed": False,
        "maturity_effect": "NONE",
        "valid": True,
    }
    result["inspection_sha256"] = identity.self_hash(result, "inspection_sha256")
    output_root.mkdir(parents=True, exist_ok=True)
    output = output_root / f"identity-{result['inspection_sha256']}.json"
    identity.write_object(result, output)
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
                "written": str(output.relative_to(identity.PROJECT_ROOT)),
            }
    except (
        AsrSecurityIdentityInspectionError,
        identity.AsrSecurityIdentityError,
        OSError,
        ValueError,
    ) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}), file=sys.stderr)
        return 2
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

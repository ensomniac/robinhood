"""Inspect the ASR tier-1 semantic contract and result."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import asr_semantic_tier as semantic


class AsrSemanticTierInspectionError(RuntimeError):
    """The semantic contract or result does not independently rebuild."""


def inspect_contract(
    path: Path,
    *,
    status_path: Path = semantic.DEFAULT_CONTRACT_STATUS,
) -> dict[str, Any]:
    recorded = semantic.load_contract(path)
    expected = semantic.build_contract()
    status = semantic.read_object(status_path)
    for relative, digest in recorded["implementation_hashes"].items():
        if semantic.file_hash(semantic.PROJECT_ROOT / relative) != digest:
            raise AsrSemanticTierInspectionError(
                f"semantic implementation drifted: {relative}"
            )
    if not (
        recorded == expected
        and status.get("status") == "SEMANTIC_CONTRACT_PENDING_INSPECTION"
        and status.get("contract_sha256") == recorded["contract_sha256"]
        and recorded["source_lineage"]["inspected_success_count"] == 196
        and recorded["source_lineage"]["source_failure_count"] == 5
        and recorded["source_lineage"]["unselected_unique_hit_count"] == 17_278
        and recorded["classification_contract"]["all_positive_groups_same_window"]
        is True
        and recorded["capacity_contract"][
            "formal_verified_event_count_requires_security_identity"
        ]
        is True
        and recorded["access_contract"]["new_provider_access_permitted"] is False
        and recorded["access_contract"]["market_price_access_permitted"] is False
        and recorded["access_contract"]["forward_return_access_permitted"] is False
        and recorded["access_contract"]["broker_actions_permitted"] is False
        and recorded["verified_event_count"] is None
        and recorded["market_outcomes_accessed"] is False
    ):
        raise AsrSemanticTierInspectionError(
            "semantic contract or zero-outcome boundary differs"
        )
    result: dict[str, Any] = {
        "schema_version": 1,
        "inspection_kind": "outcome-blind-asr-semantic-contract-inspection",
        "campaign_id": semantic.tier.capacity.CAMPAIGN_ID,
        "candidate_id": semantic.tier.capacity.CANDIDATE_ID,
        "contract_sha256": recorded["contract_sha256"],
        "status": "SEMANTIC_CONTRACT_INSPECTED",
        "source_lineage_rebuilt": True,
        "same_window_patterns_rebuilt": True,
        "date_and_notional_resolution_rebuilt": True,
        "event_deduplication_rebuilt": True,
        "zero_outcome_boundary_rebuilt": True,
        "local_source_text_access_permitted": True,
        "provider_access_permitted": False,
        "market_price_access_permitted": False,
        "outcome_access_permitted": False,
        "broker_actions_permitted": False,
        "verified_event_count": None,
        "market_outcomes_accessed": False,
        "maturity_effect": "NONE",
        "valid": True,
    }
    result["inspection_sha256"] = semantic.self_hash(result, "inspection_sha256")
    semantic.write_object(result, status_path)
    return result


def inspect_result(
    path: Path | None = None,
    *,
    output_root: Path = semantic.DEFAULT_RESULT_ROOT / "inspections",
    store_root: Path | None = None,
) -> tuple[Path, dict[str, Any]]:
    result_path = path or next(
        value
        for value in sorted(semantic.DEFAULT_RESULT_ROOT.glob("*.json"))
        if "inspections" not in value.parts
    )
    recorded = semantic.read_object(result_path)
    if recorded.get("result_sha256") != semantic.self_hash(
        recorded, "result_sha256"
    ):
        raise AsrSemanticTierInspectionError("semantic result hash is invalid")
    root = store_root or semantic.tier.shared._store().root
    private_info = recorded["private_result"]
    private_path = root / str(private_info["cache_relative_path"])
    compressed = private_path.read_bytes() if private_path.is_file() else b""
    if (
        hashlib.sha256(compressed).hexdigest() != private_info["file_sha256"]
        or len(compressed) != private_info["bytes"]
    ):
        raise AsrSemanticTierInspectionError("private semantic result drifted")
    private = json.loads(gzip.decompress(compressed))
    if private.get("private_result_sha256") != semantic.self_hash(
        private, "private_result_sha256"
    ):
        raise AsrSemanticTierInspectionError(
            "private semantic result hash is invalid"
        )
    rebuilt_path, rebuilt = semantic.evaluate(
        result_root=output_root / "_rebuild",
        status_path=output_root / "_rebuild-status.json",
        store_root=root,
    )
    rebuilt_private_info = rebuilt["private_result"]
    rebuilt_private_path = root / str(rebuilt_private_info["cache_relative_path"])
    rebuilt_private = json.loads(gzip.decompress(rebuilt_private_path.read_bytes()))
    if not (
        private == rebuilt_private
        and recorded["terminal_counts"] == rebuilt["terminal_counts"]
        and recorded["semantically_qualified_unique_event_count"]
        == rebuilt["semantically_qualified_unique_event_count"]
        and recorded["tier_2_open_candidate"] == rebuilt["tier_2_open_candidate"]
        and recorded["security_identity_resolution_complete"] is False
        and recorded["verified_event_count"] is None
        and recorded["market_price_values_accessed"] == 0
        and recorded["returns_computed"] == 0
        and recorded["market_outcomes_accessed"] is False
        and recorded["broker_actions"] == 0
        and recorded["valid"] is True
    ):
        raise AsrSemanticTierInspectionError(
            "semantic result does not independently rebuild"
        )
    rebuilt_path.unlink(missing_ok=True)
    (output_root / "_rebuild-status.json").unlink(missing_ok=True)
    rebuild_dir = output_root / "_rebuild"
    if rebuild_dir.exists():
        rebuild_dir.rmdir()
    result: dict[str, Any] = {
        "schema_version": 1,
        "inspection_kind": "outcome-blind-asr-semantic-result-inspection",
        "campaign_id": semantic.tier.capacity.CAMPAIGN_ID,
        "candidate_id": semantic.tier.capacity.CANDIDATE_ID,
        "contract_sha256": recorded["contract_sha256"],
        "result_sha256": recorded["result_sha256"],
        "private_result_sha256": private["private_result_sha256"],
        "terminal_counts_rebuilt": True,
        "event_deduplication_rebuilt": True,
        "semantically_qualified_unique_event_count": recorded[
            "semantically_qualified_unique_event_count"
        ],
        "tier_2_open": recorded["tier_2_open_candidate"],
        "security_identity_resolution_required": recorded[
            "security_identity_resolution_required"
        ],
        "security_identity_resolution_complete": False,
        "verified_event_count": None,
        "market_price_access_permitted": False,
        "outcome_access_permitted": False,
        "broker_actions_permitted": False,
        "market_outcomes_accessed": False,
        "maturity_effect": "NONE",
        "valid": True,
    }
    result["inspection_sha256"] = semantic.self_hash(result, "inspection_sha256")
    output_root.mkdir(parents=True, exist_ok=True)
    output = output_root / (
        f"{semantic.tier.capacity.CANDIDATE_ID}-semantic-tier1-"
        f"{result['inspection_sha256']}.json"
    )
    semantic.write_object(result, output)
    return output, result


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("inspect-contract", "inspect-result"))
    parser.add_argument("path", type=Path, nargs="?")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "inspect-contract":
            if args.path is None:
                raise AsrSemanticTierInspectionError("contract path is required")
            result: dict[str, Any] = inspect_contract(args.path)
        else:
            path, value = inspect_result(args.path)
            result = {
                **value,
                "written": str(path.relative_to(semantic.PROJECT_ROOT)),
            }
    except (
        AsrSemanticTierInspectionError,
        semantic.AsrSemanticTierError,
        OSError,
        ValueError,
    ) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}), file=sys.stderr)
        return 2
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

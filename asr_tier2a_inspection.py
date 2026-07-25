"""Inspect ASR tier-2A contracts, documents, and semantic results."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import asr_tier2a as tier2a


class AsrTier2aInspectionError(RuntimeError):
    """The tier-2A artifact does not independently rebuild."""


def inspect_contract(
    path: Path, *, status_path: Path = tier2a.DEFAULT_CONTRACT_STATUS
) -> dict[str, Any]:
    recorded = tier2a.load_contract(path)
    root = tier2a.tier1.shared._store().root
    expected, graph, graph_path = tier2a.build_contract(store_root=root)
    raw = graph_path.read_bytes() if graph_path.is_file() else b""
    expected["private_graph"]["file_sha256"] = hashlib.sha256(raw).hexdigest()
    expected["private_graph"]["bytes"] = len(raw)
    expected["contract_sha256"] = tier2a.self_hash(expected, "contract_sha256")
    status = tier2a.read_object(status_path)
    if not (
        recorded == expected
        and tier2a._read_gzip(graph_path) == graph
        and status.get("status") == "TIER2A_CONTRACT_PENDING_INSPECTION"
        and recorded["selection_contract"]["selected_hit_count"] == 1_823
        and recorded["selection_contract"]["selected_accession_count"] == 1_273
        and recorded["selection_contract"]["candidate_url_count"] == 1_830
        and recorded["access_contract"]["complete_submission_access_permitted"] is False
        and recorded["access_contract"]["market_price_access_permitted"] is False
        and recorded["verified_event_count"] is None
        and recorded["market_outcomes_accessed"] is False
    ):
        raise AsrTier2aInspectionError(
            "tier-2A contract or zero-outcome boundary differs"
        )
    result: dict[str, Any] = {
        "schema_version": 1,
        "inspection_kind": "outcome-blind-asr-tier2a-contract-inspection",
        "campaign_id": tier2a.capacity.CAMPAIGN_ID,
        "candidate_id": tier2a.capacity.CANDIDATE_ID,
        "tier_id": tier2a.TIER_ID,
        "contract_sha256": recorded["contract_sha256"],
        "status": "TIER2A_CONTRACT_INSPECTED",
        "selection_rebuilt": True,
        "private_graph_rebuilt": True,
        "selected_hit_count": 1_823,
        "selected_accession_count": 1_273,
        "candidate_url_count": 1_830,
        "provider_access_permitted": True,
        "provider_access_scope": "the 1,830 frozen exact matched-document candidates",
        "local_document_semantics_permitted": False,
        "complete_submission_access_permitted": False,
        "market_price_access_permitted": False,
        "outcome_access_permitted": False,
        "broker_actions_permitted": False,
        "verified_event_count": None,
        "market_outcomes_accessed": False,
        "maturity_effect": "NONE",
        "valid": True,
    }
    result["inspection_sha256"] = tier2a.self_hash(result, "inspection_sha256")
    tier2a.write_object(result, status_path)
    return result


def inspect_collection(
    *,
    output_path: Path = tier2a.DEFAULT_RESULT_ROOT / "collection-inspection.json",
    store_root: Path | None = None,
) -> dict[str, Any]:
    root = store_root or tier2a.tier1.shared._store().root
    recorded, private = tier2a._load_private_collection(root)
    success_bytes = 0
    for record in private["records"]:
        raw = (root / record["source_cache_relative_path"]).read_bytes()
        if (
            hashlib.sha256(raw).hexdigest() != record["source_sha256"]
            or len(raw) != record["source_bytes"]
            or not raw
            or any(marker in raw for marker in tier2a.SEC_DENIAL_MARKERS)
        ):
            raise AsrTier2aInspectionError("retained tier-2A document drifted")
        success_bytes += len(raw)
    if not (
        private["request_count"] == recorded["request_count"] == 1_823
        and private["success_count"] == recorded["success_count"]
        and private["failure_count"] == recorded["failure_count"]
        and success_bytes == recorded["source_bytes"]
        and recorded["local_document_semantics_permitted"] is False
        and recorded["complete_submission_access_permitted"] is False
        and recorded["verified_event_count"] is None
        and recorded["market_outcomes_accessed"] is False
    ):
        raise AsrTier2aInspectionError(
            "tier-2A collection or zero-outcome boundary differs"
        )
    result: dict[str, Any] = {
        "schema_version": 1,
        "inspection_kind": "outcome-blind-asr-tier2a-collection-inspection",
        "campaign_id": tier2a.capacity.CAMPAIGN_ID,
        "candidate_id": tier2a.capacity.CANDIDATE_ID,
        "tier_id": tier2a.TIER_ID,
        "collection_sha256": recorded["collection_sha256"],
        "request_count": recorded["request_count"],
        "success_count": recorded["success_count"],
        "failure_count": recorded["failure_count"],
        "source_bytes_rebuilt": success_bytes,
        "private_collection_rehashed": True,
        "local_document_semantics_permitted": True,
        "complete_submission_access_permitted": False,
        "market_price_access_permitted": False,
        "outcome_access_permitted": False,
        "broker_actions_permitted": False,
        "verified_event_count": None,
        "market_outcomes_accessed": False,
        "maturity_effect": "NONE",
        "valid": True,
    }
    result["inspection_sha256"] = tier2a.self_hash(result, "inspection_sha256")
    tier2a.write_object(result, output_path)
    return result


def inspect_result(
    path: Path,
    *,
    output_root: Path = tier2a.DEFAULT_RESULT_ROOT / "inspections",
    store_root: Path | None = None,
) -> tuple[Path, dict[str, Any]]:
    recorded = tier2a.read_object(path)
    if recorded.get("result_sha256") != tier2a.self_hash(recorded, "result_sha256"):
        raise AsrTier2aInspectionError("tier-2A result hash is invalid")
    root = store_root or tier2a.tier1.shared._store().root
    info = recorded["private_result"]
    private_path = root / str(info["cache_relative_path"])
    raw = private_path.read_bytes() if private_path.is_file() else b""
    private = tier2a._read_gzip(private_path)
    collection_status, collection = tier2a._load_private_collection(root)
    rebuilt = tier2a.rebuild_semantic_result(collection, root)
    if not (
        hashlib.sha256(raw).hexdigest() == info["file_sha256"]
        and len(raw) == info["bytes"]
        and private.get("private_result_sha256") == info["private_result_sha256"]
        and private.get("private_result_sha256")
        == tier2a.self_hash(private, "private_result_sha256")
        and private == rebuilt
        and recorded["collection_sha256"] == collection_status["collection_sha256"]
        and recorded["document_semantic_candidate_count"]
        == private["document_semantic_candidate_count"]
        and recorded["qualified_accession_count"]
        == private["qualified_accession_count"]
        and recorded["combined_tier1_plus_tier2a_candidate_ceiling"]
        == 59 + private["document_semantic_candidate_count"]
        and recorded["precise_acceptance_resolution_complete"] is False
        and recorded["verified_event_count"] is None
        and recorded["market_outcomes_accessed"] is False
    ):
        raise AsrTier2aInspectionError(
            "tier-2A result or zero-outcome boundary differs"
        )
    result: dict[str, Any] = {
        "schema_version": 1,
        "inspection_kind": "outcome-blind-asr-tier2a-result-inspection",
        "campaign_id": tier2a.capacity.CAMPAIGN_ID,
        "candidate_id": tier2a.capacity.CANDIDATE_ID,
        "tier_id": tier2a.TIER_ID,
        "result_sha256": recorded["result_sha256"],
        "terminal_counts_rebuilt": True,
        "document_semantic_candidate_count": recorded[
            "document_semantic_candidate_count"
        ],
        "qualified_accession_count": recorded["qualified_accession_count"],
        "combined_tier1_plus_tier2a_candidate_ceiling": recorded[
            "combined_tier1_plus_tier2a_candidate_ceiling"
        ],
        "qualified_submission_manifest_freeze_permitted": bool(
            recorded["qualified_accession_count"]
        ),
        "complete_submission_access_permitted": False,
        "verified_event_count": None,
        "market_price_access_permitted": False,
        "outcome_access_permitted": False,
        "broker_actions_permitted": False,
        "market_outcomes_accessed": False,
        "maturity_effect": "NONE",
        "valid": True,
    }
    result["inspection_sha256"] = tier2a.self_hash(result, "inspection_sha256")
    output_root.mkdir(parents=True, exist_ok=True)
    output = output_root / f"tier2a-{result['inspection_sha256']}.json"
    tier2a.write_object(result, output)
    return output, result


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "command", choices=("inspect-contract", "inspect-collection", "inspect-result")
    )
    parser.add_argument("path", type=Path, nargs="?")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "inspect-contract":
            if args.path is None:
                raise AsrTier2aInspectionError("contract path is required")
            result: dict[str, Any] = inspect_contract(args.path)
        elif args.command == "inspect-collection":
            result = inspect_collection()
        else:
            if args.path is None:
                raise AsrTier2aInspectionError("result path is required")
            output, value = inspect_result(args.path)
            result = {
                **value,
                "written": str(output.relative_to(tier2a.PROJECT_ROOT)),
            }
    except (
        AsrTier2aInspectionError,
        tier2a.AsrTier2aError,
        OSError,
        ValueError,
    ) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}), file=sys.stderr)
        return 2
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

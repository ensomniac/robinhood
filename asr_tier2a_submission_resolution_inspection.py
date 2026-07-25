"""Inspect tier-2A complete-submission resolution artifacts."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import asr_tier2a_submission_resolution as resolution


class AsrTier2aSubmissionResolutionInspectionError(RuntimeError):
    """The resolution artifact does not independently rebuild."""


def inspect_contract(
    path: Path,
    *,
    status_path: Path = resolution.DEFAULT_CONTRACT_STATUS,
) -> dict[str, Any]:
    recorded = resolution.load_contract(path)
    root = resolution.tier1.shared._store().root
    expected, graph, graph_path = resolution.build_contract(store_root=root)
    raw = graph_path.read_bytes() if graph_path.is_file() else b""
    expected["private_graph"]["file_sha256"] = hashlib.sha256(raw).hexdigest()
    expected["private_graph"]["bytes"] = len(raw)
    expected["contract_sha256"] = resolution.self_hash(expected, "contract_sha256")
    status = resolution.read_object(status_path)
    if not (
        recorded == expected
        and resolution._read_gzip(graph_path) == graph
        and status.get("status") == "RESOLUTION_CONTRACT_PENDING_INSPECTION"
        and recorded["request_contract"]["request_count"] == 252
        and recorded["request_contract"]["candidate_url_count"] == 252
        and recorded["classification_contract"][
            "security_identity_still_separately_required"
        ]
        is True
        and recorded["access_contract"]["security_identity_access_permitted"] is False
        and recorded["access_contract"]["market_price_access_permitted"] is False
        and recorded["verified_event_count"] is None
        and recorded["market_outcomes_accessed"] is False
    ):
        raise AsrTier2aSubmissionResolutionInspectionError(
            "resolution contract or zero-outcome boundary differs"
        )
    result: dict[str, Any] = {
        "schema_version": 1,
        "inspection_kind": "outcome-blind-asr-tier2a-resolution-contract-inspection",
        "campaign_id": resolution.tier2a.capacity.CAMPAIGN_ID,
        "candidate_id": resolution.tier2a.capacity.CANDIDATE_ID,
        "resolution_id": resolution.RESOLUTION_ID,
        "contract_sha256": recorded["contract_sha256"],
        "status": "RESOLUTION_CONTRACT_INSPECTED",
        "qualified_accession_graph_rebuilt": True,
        "request_count": 252,
        "candidate_url_count": 252,
        "provider_access_permitted": True,
        "provider_access_scope": "252 exact qualified complete submissions",
        "local_semantics_permitted": False,
        "security_identity_access_permitted": False,
        "market_price_access_permitted": False,
        "outcome_access_permitted": False,
        "broker_actions_permitted": False,
        "verified_event_count": None,
        "market_outcomes_accessed": False,
        "maturity_effect": "NONE",
        "valid": True,
    }
    result["inspection_sha256"] = resolution.self_hash(result, "inspection_sha256")
    resolution.write_object(result, status_path)
    return result


def inspect_collection(
    *,
    output_path: Path = resolution.DEFAULT_COLLECTION_INSPECTION,
    store_root: Path | None = None,
) -> dict[str, Any]:
    root = store_root or resolution.tier1.shared._store().root
    recorded, private = resolution.load_private_collection(root)
    source_bytes = 0
    acceptance_headers = 0
    for record in private["records"]:
        raw = (root / str(record["source_cache_relative_path"])).read_bytes()
        match = resolution.submissions.ACCEPTANCE_PATTERN.search(raw)
        if (
            hashlib.sha256(raw).hexdigest() != record["source_sha256"]
            or len(raw) != record["source_bytes"]
            or match is None
            or match.group(1).decode() != record["acceptance_datetime_raw"]
        ):
            raise AsrTier2aSubmissionResolutionInspectionError(
                "retained complete submission drifted"
            )
        source_bytes += len(raw)
        acceptance_headers += 1
    if not (
        private["request_count"] == recorded["request_count"] == 252
        and private["success_count"] == recorded["success_count"]
        and private["failure_count"] == recorded["failure_count"]
        and source_bytes == recorded["source_bytes"]
        and acceptance_headers == recorded["success_count"]
        and recorded["local_semantics_permitted"] is False
        and recorded["security_identity_access_permitted"] is False
        and recorded["market_price_access_permitted"] is False
        and recorded["verified_event_count"] is None
        and recorded["market_outcomes_accessed"] is False
    ):
        raise AsrTier2aSubmissionResolutionInspectionError(
            "resolution collection or zero-outcome boundary differs"
        )
    result: dict[str, Any] = {
        "schema_version": 1,
        "inspection_kind": "outcome-blind-asr-tier2a-resolution-collection-inspection",
        "campaign_id": resolution.tier2a.capacity.CAMPAIGN_ID,
        "candidate_id": resolution.tier2a.capacity.CANDIDATE_ID,
        "resolution_id": resolution.RESOLUTION_ID,
        "collection_sha256": recorded["collection_sha256"],
        "request_count": recorded["request_count"],
        "success_count": recorded["success_count"],
        "failure_count": recorded["failure_count"],
        "source_bytes_rebuilt": source_bytes,
        "acceptance_headers_rebuilt": acceptance_headers,
        "private_collection_rehashed": True,
        "local_semantics_permitted": True,
        "security_identity_access_permitted": False,
        "market_price_access_permitted": False,
        "outcome_access_permitted": False,
        "broker_actions_permitted": False,
        "verified_event_count": None,
        "market_outcomes_accessed": False,
        "maturity_effect": "NONE",
        "valid": True,
    }
    result["inspection_sha256"] = resolution.self_hash(result, "inspection_sha256")
    resolution.write_object(result, output_path)
    return result


def inspect_result(
    path: Path,
    *,
    output_root: Path = resolution.DEFAULT_RESULT_ROOT / "inspections",
    store_root: Path | None = None,
) -> tuple[Path, dict[str, Any]]:
    recorded = resolution.read_object(path)
    if recorded.get("result_sha256") != resolution.self_hash(recorded, "result_sha256"):
        raise AsrTier2aSubmissionResolutionInspectionError(
            "precise semantic result hash is invalid"
        )
    root = store_root or resolution.tier1.shared._store().root
    info = recorded["private_result"]
    private_path = root / str(info["cache_relative_path"])
    compressed = private_path.read_bytes() if private_path.is_file() else b""
    private = resolution._read_gzip(private_path)
    collection_status, collection = resolution.load_private_collection(root)
    rebuilt = resolution.rebuild_result(collection, root)
    if not (
        hashlib.sha256(compressed).hexdigest() == info["file_sha256"]
        and len(compressed) == info["bytes"]
        and private.get("private_result_sha256") == info["private_result_sha256"]
        and private.get("private_result_sha256")
        == resolution.self_hash(private, "private_result_sha256")
        and private == rebuilt
        and recorded["collection_sha256"] == collection_status["collection_sha256"]
        and recorded["terminal_counts"] == private["terminal_counts"]
        and recorded["precise_semantic_event_count"]
        == private["precise_semantic_event_count"]
        and recorded["security_identity_resolution_complete"] is False
        and recorded["verified_event_count"] is None
        and recorded["market_outcomes_accessed"] is False
    ):
        raise AsrTier2aSubmissionResolutionInspectionError(
            "precise semantic result does not independently rebuild"
        )
    result: dict[str, Any] = {
        "schema_version": 1,
        "inspection_kind": "outcome-blind-asr-tier2a-precise-result-inspection",
        "campaign_id": resolution.tier2a.capacity.CAMPAIGN_ID,
        "candidate_id": resolution.tier2a.capacity.CANDIDATE_ID,
        "resolution_id": resolution.RESOLUTION_ID,
        "result_sha256": recorded["result_sha256"],
        "terminal_counts_rebuilt": True,
        "event_deduplication_rebuilt": True,
        "precise_semantic_event_count": recorded["precise_semantic_event_count"],
        "security_identity_manifest_freeze_permitted": bool(
            recorded["precise_semantic_event_count"]
        ),
        "security_identity_access_permitted": False,
        "verified_event_count": None,
        "market_price_access_permitted": False,
        "outcome_access_permitted": False,
        "broker_actions_permitted": False,
        "market_outcomes_accessed": False,
        "maturity_effect": "NONE",
        "valid": True,
    }
    result["inspection_sha256"] = resolution.self_hash(result, "inspection_sha256")
    output_root.mkdir(parents=True, exist_ok=True)
    output = output_root / f"precise-{result['inspection_sha256']}.json"
    resolution.write_object(result, output)
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
                raise AsrTier2aSubmissionResolutionInspectionError(
                    "contract path is required"
                )
            result: dict[str, Any] = inspect_contract(args.path)
        elif args.command == "inspect-collection":
            result = inspect_collection()
        else:
            if args.path is None:
                raise AsrTier2aSubmissionResolutionInspectionError(
                    "result path is required"
                )
            output, value = inspect_result(args.path)
            result = {
                **value,
                "written": str(output.relative_to(resolution.PROJECT_ROOT)),
            }
    except (
        AsrTier2aSubmissionResolutionInspectionError,
        resolution.AsrTier2aSubmissionResolutionError,
        OSError,
        ValueError,
    ) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}), file=sys.stderr)
        return 2
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

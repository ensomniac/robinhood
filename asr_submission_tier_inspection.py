"""Independently inspect the frozen ASR complete-submission tier."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import asr_submission_tier as tier


class AsrSubmissionTierInspectionError(RuntimeError):
    """The ASR submission tier does not independently rebuild."""


def inspect_contract(
    contract_path: Path,
    *,
    status_path: Path = tier.DEFAULT_STATUS,
    store_root: Path | None = None,
) -> dict[str, Any]:
    root = store_root or tier.shared._store().root
    recorded = tier.load_contract(contract_path)
    expected, expected_graph, expected_path = tier.build_contract(store_root=root)
    raw_path = root / str(recorded["private_graph"]["cache_relative_path"])
    raw = raw_path.read_bytes() if raw_path.is_file() else b""
    private = tier._read_gzip(raw_path) if raw else {}
    expected["private_graph"]["private_graph_file_sha256"] = hashlib.sha256(
        raw
    ).hexdigest()
    expected["private_graph"]["private_graph_bytes"] = len(raw)
    expected["contract_sha256"] = tier.self_hash(expected, "contract_sha256")
    status = tier.read_object(status_path)
    for relative, digest in recorded["implementation_hashes"].items():
        if tier.file_hash(tier.PROJECT_ROOT / relative) != digest:
            raise AsrSubmissionTierInspectionError(
                f"bound implementation drifted: {relative}"
            )
    if not (
        raw
        and raw_path == expected_path
        and private == expected_graph
        and private.get("private_graph_sha256")
        == tier.self_hash(private, "private_graph_sha256")
        and recorded == expected
        and status.get("status") == "SUBMISSION_TIER_CONTRACT_PENDING_INSPECTION"
        and status.get("contract_sha256") == recorded["contract_sha256"]
        and recorded["selection_contract"]["selected_hit_count"] == 204
        and recorded["selection_contract"]["selected_accession_count"] == 201
        and recorded["selection_contract"]["candidate_url_count"] == 248
        and recorded["selection_contract"]["unselected_unique_hit_count"] == 17_278
        and recorded["staged_capacity_contract"][
            "tier_2_may_open_only_if_verified_unique_events_below"
        ]
        == 100
        and recorded["access_contract"]["filing_semantic_classification_permitted"]
        is False
        and recorded["access_contract"]["market_price_access_permitted"] is False
        and recorded["access_contract"]["forward_return_access_permitted"] is False
        and recorded["access_contract"]["broker_actions_permitted"] is False
        and recorded["verified_event_count"] is None
        and recorded["market_outcomes_accessed"] is False
    ):
        raise AsrSubmissionTierInspectionError(
            "submission tier or zero-outcome boundary differs"
        )
    result: dict[str, Any] = {
        "schema_version": 1,
        "inspection_kind": "outcome-blind-asr-submission-tier-inspection",
        "campaign_id": tier.capacity.CAMPAIGN_ID,
        "candidate_id": tier.capacity.CANDIDATE_ID,
        "tier_id": tier.TIER_ID,
        "contract_sha256": recorded["contract_sha256"],
        "status": "SUBMISSION_TIER_CONTRACT_INSPECTED",
        "denominator_lineage_rebuilt": True,
        "selection_phrase_rebuilt": True,
        "private_request_graph_rebuilt": True,
        "selected_hit_count": 204,
        "selected_accession_count": 201,
        "candidate_url_count": 248,
        "unselected_unique_hit_count": 17_278,
        "provider_access_permitted": True,
        "provider_access_scope": (
            "the 248 frozen same-accession SEC candidate URLs for 201 accessions"
        ),
        "filing_semantic_classification_permitted": False,
        "market_price_access_permitted": False,
        "outcome_access_permitted": False,
        "broker_actions_permitted": False,
        "verified_event_count": None,
        "market_outcomes_accessed": False,
        "maturity_effect": "NONE",
        "valid": True,
    }
    result["inspection_sha256"] = tier.self_hash(result, "inspection_sha256")
    tier.write_object(result, status_path)
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
        AsrSubmissionTierInspectionError,
        tier.AsrSubmissionTierError,
        OSError,
        ValueError,
    ) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}), file=sys.stderr)
        return 2
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""Inspect retained ASR high-precision complete submissions."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import sys
from pathlib import Path
from typing import Any

import asr_submission_collection as collection
import asr_submission_tier as tier


DEFAULT_ROOT = (
    tier.PROJECT_ROOT
    / "strategy_tournament/v2/asr/submissions/tier1/collection-inspections"
)


class AsrSubmissionCollectionInspectionError(RuntimeError):
    """The retained ASR submissions do not independently rebuild."""


def inspect_collection(
    path: Path = collection.STATUS_PATH,
    *,
    output_root: Path = DEFAULT_ROOT,
    store_root: Path | None = None,
) -> tuple[Path, dict[str, Any]]:
    recorded = tier.read_object(path)
    if recorded.get("collection_sha256") != tier.self_hash(
        recorded, "collection_sha256"
    ):
        raise AsrSubmissionCollectionInspectionError(
            "submission collection hash is invalid"
        )
    root = store_root or tier.shared._store().root
    private_info = recorded["private_collection"]
    private_path = root / str(private_info["cache_relative_path"])
    compressed = private_path.read_bytes() if private_path.is_file() else b""
    if (
        hashlib.sha256(compressed).hexdigest() != private_info["file_sha256"]
        or len(compressed) != private_info["bytes"]
    ):
        raise AsrSubmissionCollectionInspectionError(
            "private collection file drifted"
        )
    private = json.loads(gzip.decompress(compressed))
    if not isinstance(private, dict):
        raise AsrSubmissionCollectionInspectionError(
            "private collection is invalid"
        )
    if private.get("private_collection_sha256") != tier.self_hash(
        private, "private_collection_sha256"
    ):
        raise AsrSubmissionCollectionInspectionError(
            "private collection content hash is invalid"
        )
    rebuilt_bytes = 0
    rebuilt_acceptance = 0
    seen_ordinals: set[int] = set()
    for record in private.get("records", []):
        ordinal = int(record["ordinal"])
        if ordinal in seen_ordinals:
            raise AsrSubmissionCollectionInspectionError(
                "collection repeats an accession ordinal"
            )
        seen_ordinals.add(ordinal)
        source_path = root / str(record["source_cache_relative_path"])
        raw = source_path.read_bytes() if source_path.is_file() else b""
        match = collection.ACCEPTANCE_PATTERN.search(raw)
        if (
            hashlib.sha256(raw).hexdigest() != record["source_sha256"]
            or len(raw) != record["source_bytes"]
            or b"<SEC-DOCUMENT" not in raw[:4096].upper()
            or match is None
            or match.group(1).decode() != record["acceptance_datetime_raw"]
        ):
            raise AsrSubmissionCollectionInspectionError(
                "retained complete submission drifted"
            )
        rebuilt_bytes += len(raw)
        rebuilt_acceptance += 1
    valid = not private.get("failures") and len(seen_ordinals) == 201
    if not (
        private.get("request_count") == recorded["request_count"] == 201
        and private.get("success_count") == recorded["success_count"]
        and private.get("failure_count") == recorded["failure_count"]
        and private.get("attempted_candidate_count")
        == recorded["attempted_candidate_count"]
        and rebuilt_bytes == recorded["source_bytes"]
        and recorded["state"]
        == (
            "SUBMISSIONS_COLLECTED_READY_FOR_INSPECTION"
            if valid
            else "SUBMISSIONS_COLLECTED_WITH_FAILURES"
        )
        and recorded["filing_semantic_classification_permitted"] is False
        and recorded["verified_event_count"] is None
        and recorded["market_price_values_accessed"] == 0
        and recorded["returns_computed"] == 0
        and recorded["market_outcomes_accessed"] is False
        and recorded["broker_actions"] == 0
        and recorded["valid"] is valid
    ):
        raise AsrSubmissionCollectionInspectionError(
            "submission collection or zero-outcome boundary differs"
        )
    result: dict[str, Any] = {
        "schema_version": 1,
        "inspection_kind": "outcome-blind-asr-submission-collection-inspection",
        "campaign_id": tier.capacity.CAMPAIGN_ID,
        "candidate_id": tier.capacity.CANDIDATE_ID,
        "tier_id": tier.TIER_ID,
        "contract_sha256": recorded["contract_sha256"],
        "collection_sha256": recorded["collection_sha256"],
        "state": recorded["state"],
        "request_count": recorded["request_count"],
        "success_count": recorded["success_count"],
        "failure_count": recorded["failure_count"],
        "source_bytes_rebuilt": rebuilt_bytes,
        "acceptance_datetime_presence_rebuilt": rebuilt_acceptance,
        "private_collection_rehashed": True,
        "filing_semantic_classification_permitted": valid,
        "market_price_access_permitted": False,
        "outcome_access_permitted": False,
        "broker_actions_permitted": False,
        "verified_event_count": None,
        "market_outcomes_accessed": False,
        "maturity_effect": "NONE",
        "valid": True,
    }
    result["inspection_sha256"] = tier.self_hash(result, "inspection_sha256")
    output_root.mkdir(parents=True, exist_ok=True)
    output = output_root / (
        f"{tier.capacity.CANDIDATE_ID}-{tier.TIER_ID}-"
        f"{result['inspection_sha256']}.json"
    )
    tier.write_object(result, output)
    return output, result


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("inspect",))
    return parser


def main(argv: list[str] | None = None) -> int:
    _parser().parse_args(argv)
    try:
        path, result = inspect_collection()
        rendered = {
            **result,
            "written": str(path.relative_to(tier.PROJECT_ROOT)),
        }
    except (
        AsrSubmissionCollectionInspectionError,
        collection.AsrSubmissionCollectionError,
        tier.AsrSubmissionTierError,
        OSError,
        ValueError,
    ) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}), file=sys.stderr)
        return 2
    print(json.dumps(rendered, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

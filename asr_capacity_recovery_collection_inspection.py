"""Inspect the retained ASR v2 recursive EFTS denominator."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import asr_capacity_recovery as recovery
import asr_capacity_recovery_collection as collection


DEFAULT_ROOT = (
    recovery.PROJECT_ROOT
    / "strategy_tournament/v2/asr/recovery/search/inspections"
)


class AsrCapacityRecoveryCollectionInspectionError(RuntimeError):
    """The retained recursive ASR denominator does not independently rebuild."""


def inspect_collection(
    path: Path = collection.STATUS_PATH,
    *,
    output_root: Path = DEFAULT_ROOT,
    store_root: Path | None = None,
) -> tuple[Path, dict[str, Any]]:
    recorded = recovery.read_object(path)
    if recorded.get("collection_sha256") != recovery.self_hash(
        recorded, "collection_sha256"
    ):
        raise AsrCapacityRecoveryCollectionInspectionError(
            "collection hash is invalid"
        )
    root = store_root or collection._store().root
    rebuilt_hits_by_phrase: dict[str, set[str]] = {}
    leaf_page_hits: dict[tuple[str, str, str], list[tuple[int, list[str]]]] = {}
    inexact_pages = 0
    for summary in recorded.get("page_summaries", []):
        if not isinstance(summary, dict):
            raise AsrCapacityRecoveryCollectionInspectionError(
                "page summary is invalid"
            )
        raw_path = root / str(summary["cache_relative_path"])
        if not raw_path.resolve().is_relative_to(root.resolve()):
            raise AsrCapacityRecoveryCollectionInspectionError(
                "retained EFTS path is unsafe"
            )
        raw_bytes = raw_path.read_bytes() if raw_path.is_file() else b""
        if (
            hashlib.sha256(raw_bytes).hexdigest() != summary["raw_sha256"]
            or len(raw_bytes) != summary["raw_bytes"]
        ):
            raise AsrCapacityRecoveryCollectionInspectionError(
                "retained EFTS page drifted"
            )
        raw = json.loads(raw_bytes)
        total, relation = collection._total(raw)
        hits = collection._hits(raw)
        if (
            total != summary["reported_total"]
            or relation != summary["total_relation"]
            or len(hits) != summary["returned_hits"]
        ):
            raise AsrCapacityRecoveryCollectionInspectionError(
                "retained EFTS page summary differs"
            )
        if relation == "gte":
            inexact_pages += 1
            continue
        key = (
            str(summary["phrase_sha256"]),
            str(summary["window_start"]),
            str(summary["window_end"]),
        )
        leaf_page_hits.setdefault(key, []).append(
            (int(summary["offset"]), [str(item["_id"]) for item in hits])
        )

    for (phrase_hash, _start, _end), pages in leaf_page_hits.items():
        identifiers = [
            identifier
            for _offset, page in sorted(pages)
            for identifier in page
        ]
        if len(identifiers) != len(set(identifiers)):
            raise AsrCapacityRecoveryCollectionInspectionError(
                "an exact leaf contains duplicate hit identities"
            )
        rebuilt_hits_by_phrase.setdefault(phrase_hash, set()).update(identifiers)

    complete = recorded.get("state") == "SEARCH_DENOMINATOR_COMPLETE"
    private_index = recorded.get("private_hit_index")
    private_unique_ids: set[str] = set()
    private_index_rehashed = False
    if complete:
        if not isinstance(private_index, dict):
            raise AsrCapacityRecoveryCollectionInspectionError(
                "complete denominator lacks private hit index"
            )
        private_path = root / str(private_index["cache_relative_path"])
        if not private_path.resolve().is_relative_to(root.resolve()):
            raise AsrCapacityRecoveryCollectionInspectionError(
                "private hit index path is unsafe"
            )
        compressed = private_path.read_bytes() if private_path.is_file() else b""
        if (
            hashlib.sha256(compressed).hexdigest() != private_index["sha256"]
            or len(compressed) != private_index["bytes"]
        ):
            raise AsrCapacityRecoveryCollectionInspectionError(
                "private hit index drifted"
            )
        private = json.loads(gzip.decompress(compressed))
        if not isinstance(private, dict) or not isinstance(private.get("hits"), list):
            raise AsrCapacityRecoveryCollectionInspectionError(
                "private hit index is invalid"
            )
        private_unique_ids = {
            str(item.get("_id")) for item in private["hits"] if isinstance(item, dict)
        }
        if len(private_unique_ids) != len(private["hits"]) or "" in private_unique_ids:
            raise AsrCapacityRecoveryCollectionInspectionError(
                "private hit index identities are invalid"
            )
        private_index_rehashed = True

    rebuilt_phrase_counts = {
        key: len(value) for key, value in sorted(rebuilt_hits_by_phrase.items())
    }
    union_ids = set().union(*rebuilt_hits_by_phrase.values()) if rebuilt_hits_by_phrase else set()
    expected_state = (
        "SEARCH_DENOMINATOR_COMPLETE"
        if recorded.get("blocked_single_date") is None
        else "BLOCKED_INEXACT_SINGLE_DATE"
    )
    if not (
        recorded["state"] == expected_state
        and recorded["inexact_parent_window_count"] == inexact_pages
        and recorded["phrase_hit_counts_by_hash"] == rebuilt_phrase_counts
        and recorded["all_leaf_totals_exact"] is complete
        and recorded["pagination_complete"] is complete
        and (
            (
                recorded["filing_hit_count"] == sum(rebuilt_phrase_counts.values())
                and recorded["unique_hit_count"] == len(union_ids)
                and private_unique_ids == union_ids
                and private_index_rehashed
            )
            if complete
            else (
                recorded["filing_hit_count"] is None
                and recorded["unique_hit_count"] is None
                and private_index is None
            )
        )
        and recorded["matched_document_access_performed"] is False
        and recorded["capacity_classification_complete"] is False
        and recorded["verified_event_count"] is None
        and recorded["market_price_values_accessed"] == 0
        and recorded["returns_computed"] == 0
        and recorded["market_outcomes_accessed"] is False
        and recorded["broker_actions"] == 0
        and recorded["valid"] is True
    ):
        raise AsrCapacityRecoveryCollectionInspectionError(
            "recursive denominator or zero-outcome boundary differs"
        )
    result: dict[str, Any] = {
        "schema_version": 1,
        "inspection_kind": "outcome-blind-asr-recursive-denominator-inspection",
        "campaign_id": recovery.CAMPAIGN_ID,
        "candidate_id": recovery.CANDIDATE_ID,
        "contract_sha256": recorded["contract_sha256"],
        "collection_sha256": recorded["collection_sha256"],
        "state": recorded["state"],
        "retained_pages_rehashed": True,
        "recursive_leaf_totals_rebuilt": True,
        "phrase_hit_counts_rebuilt": True,
        "private_hit_index_rehashed": private_index_rehashed,
        "exact_leaf_window_count": recorded["exact_leaf_window_count"],
        "inexact_parent_window_count": inexact_pages,
        "filing_hit_count": recorded["filing_hit_count"],
        "unique_hit_count": recorded["unique_hit_count"],
        "denominator_complete": complete,
        "matched_document_manifest_freeze_permitted": complete,
        "matched_document_access_permitted": False,
        "capacity_classification_complete": False,
        "verified_event_count": None,
        "market_price_values_accessed": 0,
        "returns_computed": 0,
        "market_outcomes_accessed": False,
        "broker_actions": 0,
        "maturity_effect": "NONE",
        "valid": True,
    }
    result["inspection_sha256"] = recovery.self_hash(result, "inspection_sha256")
    output_root.mkdir(parents=True, exist_ok=True)
    output = output_root / (
        f"{recovery.CANDIDATE_ID}-search-{result['inspection_sha256']}.json"
    )
    recovery.write_object(result, output)
    return output, result


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("inspect",))
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    _parser().parse_args(argv)
    try:
        path, result = inspect_collection()
        rendered = {
            **result,
            "written": str(path.relative_to(recovery.PROJECT_ROOT)),
        }
    except (
        AsrCapacityRecoveryCollectionInspectionError,
        collection.AsrCapacityRecoveryCollectionError,
        recovery.AsrCapacityRecoveryError,
        OSError,
        ValueError,
    ) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}), file=sys.stderr)
        return 2
    print(json.dumps(rendered, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

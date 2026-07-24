"""Inspect the ASR v3 retained single-page EFTS denominator."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import asr_capacity_no_pagination as capacity
import asr_capacity_no_pagination_collection as collection
import asr_capacity_recovery_collection as shared


DEFAULT_ROOT = (
    capacity.PROJECT_ROOT
    / "strategy_tournament/v2/asr/no-pagination/search/inspections"
)


class AsrCapacityNoPaginationCollectionInspectionError(RuntimeError):
    """The retained single-page denominator does not independently rebuild."""


def inspect_collection(
    path: Path = collection.STATUS_PATH,
    *,
    output_root: Path = DEFAULT_ROOT,
    store_root: Path | None = None,
) -> tuple[Path, dict[str, Any]]:
    recorded = capacity.read_object(path)
    if recorded.get("collection_sha256") != capacity.self_hash(
        recorded, "collection_sha256"
    ):
        raise AsrCapacityNoPaginationCollectionInspectionError(
            "collection hash is invalid"
        )
    root = store_root or shared._store().root
    leaf_ids_by_phrase: dict[str, set[str]] = {}
    canonical_by_id: dict[str, dict[str, Any]] = {}
    split_parents = 0
    terminal_leaves = 0
    for summary in recorded.get("page_summaries", []):
        if not isinstance(summary, dict) or summary.get("offset") != 0:
            raise AsrCapacityNoPaginationCollectionInspectionError(
                "page summary is invalid or uses offset pagination"
            )
        raw_path = root / str(summary["cache_relative_path"])
        if not raw_path.resolve().is_relative_to(root.resolve()):
            raise AsrCapacityNoPaginationCollectionInspectionError(
                "retained EFTS path is unsafe"
            )
        raw_bytes = raw_path.read_bytes() if raw_path.is_file() else b""
        if (
            hashlib.sha256(raw_bytes).hexdigest() != summary["raw_sha256"]
            or len(raw_bytes) != summary["raw_bytes"]
        ):
            raise AsrCapacityNoPaginationCollectionInspectionError(
                "retained EFTS page drifted"
            )
        raw = json.loads(raw_bytes)
        total, relation = shared._total(raw)
        hits = shared._hits(raw)
        if (
            total != summary["reported_total"]
            or relation != summary["total_relation"]
            or len(hits) != summary["returned_hits"]
        ):
            raise AsrCapacityNoPaginationCollectionInspectionError(
                "retained page summary differs"
            )
        is_parent = relation == "gte" or total > capacity.PAGE_SIZE
        if is_parent:
            split_parents += 1
            continue
        terminal_leaves += 1
        if len(hits) != total:
            raise AsrCapacityNoPaginationCollectionInspectionError(
                "terminal leaf count differs from exact total"
            )
        phrase_hash = str(summary["phrase_sha256"])
        phrase_ids = leaf_ids_by_phrase.setdefault(phrase_hash, set())
        for hit in hits:
            canonical = collection._canonical_hit(hit)
            identifier = str(canonical["_id"])
            if identifier in phrase_ids:
                raise AsrCapacityNoPaginationCollectionInspectionError(
                    "terminal windows repeat an identity within one phrase"
                )
            phrase_ids.add(identifier)
            prior = canonical_by_id.get(identifier)
            if prior is not None and prior != canonical:
                raise AsrCapacityNoPaginationCollectionInspectionError(
                    "cross-phrase source fields conflict"
                )
            canonical_by_id[identifier] = canonical

    complete = recorded.get("state") == "SEARCH_DENOMINATOR_COMPLETE"
    private_index = recorded.get("private_hit_index")
    private_by_id: dict[str, dict[str, Any]] = {}
    private_index_rehashed = False
    if complete:
        if not isinstance(private_index, Mapping):
            raise AsrCapacityNoPaginationCollectionInspectionError(
                "complete denominator lacks private hit index"
            )
        private_path = root / str(private_index["cache_relative_path"])
        compressed = private_path.read_bytes() if private_path.is_file() else b""
        if (
            hashlib.sha256(compressed).hexdigest() != private_index["sha256"]
            or len(compressed) != private_index["bytes"]
        ):
            raise AsrCapacityNoPaginationCollectionInspectionError(
                "private hit index drifted"
            )
        private = json.loads(gzip.decompress(compressed))
        if not isinstance(private, dict) or not isinstance(private.get("hits"), list):
            raise AsrCapacityNoPaginationCollectionInspectionError(
                "private hit index is invalid"
            )
        for item in private["hits"]:
            canonical = collection._canonical_hit(item)
            identifier = str(canonical["_id"])
            if identifier in private_by_id:
                raise AsrCapacityNoPaginationCollectionInspectionError(
                    "private hit index repeats an identity"
                )
            private_by_id[identifier] = canonical
        private_index_rehashed = True

    phrase_counts = {
        key: len(value) for key, value in sorted(leaf_ids_by_phrase.items())
    }
    expected_state = (
        "SEARCH_DENOMINATOR_COMPLETE"
        if recorded.get("blocked_single_date") is None
        else "BLOCKED_SINGLE_DATE_OVER_PAGE"
    )
    if not (
        recorded["state"] == expected_state
        and recorded["terminal_leaf_window_count"] == terminal_leaves
        and recorded["split_parent_window_count"] == split_parents
        and recorded["phrase_hit_counts_by_hash"] == phrase_counts
        and recorded["offset_pagination_requests"] == 0
        and recorded["all_terminal_totals_exact_and_at_most_page"] is complete
        and recorded["denominator_complete"] is complete
        and (
            (
                recorded["filing_hit_count"] == sum(phrase_counts.values())
                and recorded["unique_hit_count"] == len(canonical_by_id)
                and private_by_id == canonical_by_id
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
        raise AsrCapacityNoPaginationCollectionInspectionError(
            "single-page denominator or zero-outcome boundary differs"
        )
    result: dict[str, Any] = {
        "schema_version": 1,
        "inspection_kind": "outcome-blind-asr-single-page-denominator-inspection",
        "campaign_id": capacity.CAMPAIGN_ID,
        "candidate_id": capacity.CANDIDATE_ID,
        "contract_sha256": recorded["contract_sha256"],
        "collection_sha256": recorded["collection_sha256"],
        "state": recorded["state"],
        "retained_pages_rehashed": True,
        "single_page_leaf_totals_rebuilt": True,
        "offset_pagination_requests_rebuilt": 0,
        "phrase_hit_counts_rebuilt": True,
        "private_hit_index_rehashed": private_index_rehashed,
        "terminal_leaf_window_count": terminal_leaves,
        "split_parent_window_count": split_parents,
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
    result["inspection_sha256"] = capacity.self_hash(result, "inspection_sha256")
    output_root.mkdir(parents=True, exist_ok=True)
    output = output_root / (
        f"{capacity.CANDIDATE_ID}-search-{result['inspection_sha256']}.json"
    )
    capacity.write_object(result, output)
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
            "written": str(path.relative_to(capacity.PROJECT_ROOT)),
        }
    except (
        AsrCapacityNoPaginationCollectionInspectionError,
        collection.AsrCapacityNoPaginationCollectionError,
        capacity.AsrCapacityNoPaginationError,
        OSError,
        ValueError,
    ) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}), file=sys.stderr)
        return 2
    print(json.dumps(rendered, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

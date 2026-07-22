"""Inspect the ASR EFTS denominator collection and retained raw pages."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import asr_capacity as capacity
import asr_capacity_collection as collection


DEFAULT_ROOT = capacity.PROJECT_ROOT / "strategy_tournament/v2/asr/search/inspections"


class AsrCapacityCollectionInspectionError(RuntimeError):
    """The ASR search denominator does not rebuild from retained pages."""


def inspect_collection(
    path: Path = collection.STATUS_PATH,
    *,
    output_root: Path = DEFAULT_ROOT,
    store_root: Path | None = None,
) -> tuple[Path, dict[str, Any]]:
    recorded = capacity._read_object(path)
    if recorded.get("collection_sha256") != capacity._self_hash(
        recorded, "collection_sha256"
    ):
        raise AsrCapacityCollectionInspectionError("collection hash is invalid")
    root = store_root or collection._store().root
    rebuilt: list[dict[str, Any]] = []
    for summary in recorded.get("page_summaries", []):
        if not isinstance(summary, dict):
            raise AsrCapacityCollectionInspectionError("page summary is invalid")
        raw_path = root / str(summary["cache_relative_path"])
        if not raw_path.resolve().is_relative_to(root.resolve()):
            raise AsrCapacityCollectionInspectionError("retained EFTS path is unsafe")
        if (
            not raw_path.is_file()
            or hashlib.sha256(raw_path.read_bytes()).hexdigest()
            != summary["raw_sha256"]
            or raw_path.stat().st_size != summary["raw_bytes"]
        ):
            raise AsrCapacityCollectionInspectionError("retained EFTS page drifted")
        raw = json.loads(raw_path.read_text(encoding="utf-8"))
        total, relation = collection._total(raw)
        hits = raw.get("hits", {}).get("hits", [])
        rebuilt.append(
            {
                "reported_total": total,
                "total_relation": relation,
                "returned_hits": len(hits),
            }
        )
    inexact = any(item["total_relation"] != "eq" for item in rebuilt)
    if not (
        len(rebuilt) == len(recorded["page_summaries"])
        and recorded["query_count"] == 4
        and recorded["all_reported_totals_exact"] is (not inexact)
        and recorded["state"]
        == (
            "BLOCKED_INEXACT_EFTS_DENOMINATOR"
            if inexact
            else "SEARCH_DENOMINATOR_COMPLETE"
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
        raise AsrCapacityCollectionInspectionError(
            "search denominator state or zero-outcome boundary differs"
        )
    result: dict[str, Any] = {
        "schema_version": 1,
        "inspection_kind": "outcome-blind-asr-efts-denominator-inspection",
        "campaign_id": capacity.CAMPAIGN_ID,
        "candidate_id": capacity.CANDIDATE_ID,
        "contract_sha256": collection.CONTRACT_SHA256,
        "collection_sha256": recorded["collection_sha256"],
        "state": recorded["state"],
        "raw_pages_rehashed": True,
        "reported_totals_rebuilt": True,
        "all_reported_totals_exact": not inexact,
        "provider_limitation": (
            "EFTS returned a lower-bound denominator, so the frozen completeness "
            "gate forbids pagination, matched-document access, and event classification"
            if inexact
            else None
        ),
        "capacity_classification_complete": False,
        "verified_event_count": None,
        "market_price_values_accessed": 0,
        "returns_computed": 0,
        "market_outcomes_accessed": False,
        "broker_actions": 0,
        "maturity_effect": "NONE",
        "valid": True,
    }
    result["inspection_sha256"] = capacity._self_hash(result, "inspection_sha256")
    output_root.mkdir(parents=True, exist_ok=True)
    output = output_root / (
        f"{capacity.CANDIDATE_ID}-search-{result['inspection_sha256']}.json"
    )
    capacity._write_object(result, output)
    return output, result


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("inspect",))
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    _parser().parse_args(argv)
    try:
        path, result = inspect_collection()
        rendered = {**result, "written": str(path.relative_to(capacity.PROJECT_ROOT))}
    except (
        AsrCapacityCollectionInspectionError,
        collection.AsrCapacityCollectionError,
        OSError,
        ValueError,
    ) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}), file=sys.stderr)
        return 2
    print(json.dumps(rendered, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""Publish the exact ASR capacity contract's inspected source disposition."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import asr_capacity as capacity
import asr_capacity_collection as collection


INSPECTION_SHA256 = (
    "6465a585ec28c5da35ccafb139fed33c894384b922b6ca1fa56310d917311912"
)
INSPECTION_PATH = (
    capacity.PROJECT_ROOT
    / "strategy_tournament/v2/asr/search/inspections/"
    f"{capacity.CANDIDATE_ID}-search-{INSPECTION_SHA256}.json"
)
DEFAULT_ROOT = capacity.PROJECT_ROOT / "strategy_tournament/v2/asr/dispositions"


class AsrCapacityDispositionError(RuntimeError):
    """The exact ASR source blocker cannot be dispositioned safely."""


def build_disposition() -> dict[str, Any]:
    collected = capacity._read_object(collection.STATUS_PATH)
    inspected = capacity._read_object(INSPECTION_PATH)
    if not (
        collected.get("collection_sha256")
        == capacity._self_hash(collected, "collection_sha256")
        and inspected.get("inspection_sha256") == INSPECTION_SHA256
        and inspected.get("inspection_sha256")
        == capacity._self_hash(inspected, "inspection_sha256")
        and inspected.get("collection_sha256") == collected["collection_sha256"]
        and inspected.get("state") == "BLOCKED_INEXACT_EFTS_DENOMINATOR"
        and inspected.get("all_reported_totals_exact") is False
        and inspected.get("capacity_classification_complete") is False
        and inspected.get("verified_event_count") is None
        and inspected.get("market_price_values_accessed") == 0
        and inspected.get("returns_computed") == 0
        and inspected.get("market_outcomes_accessed") is False
        and inspected.get("broker_actions") == 0
        and inspected.get("valid") is True
    ):
        raise AsrCapacityDispositionError("ASR collection inspection is not the blocker")
    result: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": "outcome-blind-capacity-source-retirement",
        "campaign_id": capacity.CAMPAIGN_ID,
        "candidate_id": capacity.CANDIDATE_ID,
        "strategy_version": capacity.STRATEGY_VERSION,
        "mechanism_family": capacity.THEME_ID,
        "contract_sha256": collection.CONTRACT_SHA256,
        "collection_sha256": collected["collection_sha256"],
        "collection_inspection_sha256": inspected["inspection_sha256"],
        "disposition": "RETIRED_INSUFFICIENT_SOURCE_COMPLETENESS",
        "reason": (
            "the frozen primary phrase returned an inexact EFTS lower-bound total, "
            "so the complete denominator cannot be reconstructed under this contract"
        ),
        "count_based_capacity_disposition": None,
        "verified_event_count": None,
        "first_pilot_fast_lane_eligible": False,
        "same_contract_query_or_partition_repair_permitted": False,
        "same_contract_matched_document_access_permitted": False,
        "future_new_version_requires_new_preoutcome_authorization": True,
        "market_price_values_accessed": 0,
        "returns_computed": 0,
        "market_outcomes_accessed": False,
        "broker_actions": 0,
        "maturity_effect": "RETIRED_NOT_PILOT_READY",
    }
    result["disposition_sha256"] = capacity._self_hash(
        result, "disposition_sha256"
    )
    return result


def write_disposition(*, root: Path = DEFAULT_ROOT) -> tuple[Path, dict[str, Any]]:
    value = build_disposition()
    path = root / f"{capacity.CANDIDATE_ID}-{value['disposition_sha256']}.json"
    if path.exists() and capacity._read_object(path) != value:
        raise AsrCapacityDispositionError("content-addressed disposition differs")
    capacity._write_object(value, path)
    return path, value


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("publish", "status"))
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "publish":
            path, value = write_disposition()
            result = {**value, "written": str(path.relative_to(capacity.PROJECT_ROOT))}
        else:
            matches = sorted(DEFAULT_ROOT.glob("*.json"))
            if len(matches) != 1:
                raise AsrCapacityDispositionError("expected one ASR disposition")
            result = capacity._read_object(matches[0])
    except (
        AsrCapacityDispositionError,
        collection.AsrCapacityCollectionError,
        capacity.AsrCapacityError,
        OSError,
        ValueError,
    ) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}), file=sys.stderr)
        return 2
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

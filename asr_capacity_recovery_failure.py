"""Inspect and retire ASR recovery v2 after unstable EFTS pagination."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections import Counter
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import asr_capacity_recovery as recovery
import asr_capacity_recovery_collection as collection


V2_CONTRACT_SHA256 = (
    "74f42e7005868db4badef1e2bd473237a0c4a995220ff80bc01054686f2dcf96"
)
V2_CONTRACT_PATH = (
    recovery.DEFAULT_ROOT
    / f"{recovery.CANDIDATE_ID}-{V2_CONTRACT_SHA256}.json"
)
V2_INSPECTION_SHA256 = (
    "2dbe58ee6b64a21bffe7c761f80e6bc2f3a0f61279d2d7ddc178dd1f9a370067"
)
FAILED_PHRASE = recovery.v1.SEARCH_PHRASES[0]
FAILED_WINDOW_START = "2010-01-01"
FAILED_WINDOW_END = "2017-12-31"
DEFAULT_ROOT = (
    recovery.PROJECT_ROOT / "strategy_tournament/v2/asr/recovery/dispositions"
)


class AsrCapacityRecoveryFailureError(RuntimeError):
    """The v2 source failure cannot be reconstructed safely."""


def _authority() -> tuple[dict[str, Any], dict[str, Any]]:
    contract = recovery.load_contract(V2_CONTRACT_PATH)
    inspection = recovery.read_object(recovery.DEFAULT_STATUS)
    if not (
        contract.get("contract_sha256") == V2_CONTRACT_SHA256
        and inspection.get("inspection_sha256") == V2_INSPECTION_SHA256
        and inspection.get("inspection_sha256")
        == recovery.self_hash(inspection, "inspection_sha256")
        and inspection.get("contract_sha256") == V2_CONTRACT_SHA256
        and inspection.get("status") == "RECOVERY_CONTRACT_INSPECTED"
        and inspection.get("market_outcomes_accessed") is False
        and inspection.get("broker_actions_permitted") is False
    ):
        raise AsrCapacityRecoveryFailureError("v2 recovery authority differs")
    return contract, inspection


def build_disposition(*, store_root: Path | None = None) -> dict[str, Any]:
    contract, inspection = _authority()
    root = store_root or collection._store().root
    page_size = int(contract["source_contract"]["page_size"])
    first_path = collection._cache_path(
        root,
        V2_CONTRACT_SHA256,
        FAILED_PHRASE,
        FAILED_WINDOW_START,
        FAILED_WINDOW_END,
        0,
        page_size,
    )
    try:
        first = json.loads(first_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise AsrCapacityRecoveryFailureError(
            "cannot read the failed exact leaf"
        ) from exc
    total, relation = collection._total(first)
    if relation != "eq" or total <= page_size:
        raise AsrCapacityRecoveryFailureError(
            "the failed window is not an exact paginated leaf"
        )
    identifiers: list[str] = []
    page_hashes: list[str] = []
    for offset in range(0, total, page_size):
        path = collection._cache_path(
            root,
            V2_CONTRACT_SHA256,
            FAILED_PHRASE,
            FAILED_WINDOW_START,
            FAILED_WINDOW_END,
            offset,
            page_size,
        )
        try:
            raw = path.read_bytes()
            response = json.loads(raw)
        except (OSError, json.JSONDecodeError) as exc:
            raise AsrCapacityRecoveryFailureError(
                f"cannot read failed leaf offset {offset}"
            ) from exc
        if collection._total(response) != (total, "eq"):
            raise AsrCapacityRecoveryFailureError(
                "the failed leaf total changed across retained pages"
            )
        identifiers.extend(str(item["_id"]) for item in collection._hits(response))
        page_hashes.append(hashlib.sha256(raw).hexdigest())
    counts = Counter(identifiers)
    duplicated_ids = sorted(key for key, count in counts.items() if count > 1)
    if len(identifiers) != total or not duplicated_ids:
        raise AsrCapacityRecoveryFailureError(
            "retained pages do not reproduce unstable pagination"
        )
    result: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": "outcome-blind-asr-source-recovery-retirement",
        "campaign_id": recovery.CAMPAIGN_ID,
        "theme_id": recovery.THEME_ID,
        "candidate_id": recovery.CANDIDATE_ID,
        "strategy_version": recovery.STRATEGY_VERSION,
        "contract_sha256": V2_CONTRACT_SHA256,
        "contract_inspection_sha256": inspection["inspection_sha256"],
        "disposition": "RETIRED_UNSTABLE_EFTS_PAGINATION",
        "failed_phrase_sha256": hashlib.sha256(FAILED_PHRASE.encode()).hexdigest(),
        "failed_window": [FAILED_WINDOW_START, FAILED_WINDOW_END],
        "reported_exact_total": total,
        "retained_page_count": len(page_hashes),
        "retained_page_hashes_sha256": hashlib.sha256(
            recovery.canonical_bytes(page_hashes)
        ).hexdigest(),
        "returned_hit_rows": len(identifiers),
        "unique_hit_ids": len(counts),
        "duplicated_hit_id_count": len(duplicated_ids),
        "duplicate_excess_rows": len(identifiers) - len(counts),
        "duplicate_ids_retained_privately": True,
        "reason": (
            "EFTS returned an exact total but repeated filing identities across "
            "offset pages, so the complete denominator cannot be proven"
        ),
        "same_contract_resume_or_repair_permitted": False,
        "separately_frozen_no_pagination_version_permitted": True,
        "matched_document_access_performed": False,
        "verified_event_count": None,
        "market_price_values_accessed": 0,
        "returns_computed": 0,
        "market_outcomes_accessed": False,
        "broker_actions": 0,
        "maturity_effect": "RETIRED_NOT_PILOT_READY",
    }
    result["disposition_sha256"] = recovery.self_hash(
        result, "disposition_sha256"
    )
    return result


def write_disposition(
    *, output_root: Path = DEFAULT_ROOT, store_root: Path | None = None
) -> tuple[Path, dict[str, Any]]:
    value = build_disposition(store_root=store_root)
    path = output_root / (
        f"{recovery.CANDIDATE_ID}-{value['disposition_sha256']}.json"
    )
    if path.exists() and recovery.read_object(path) != value:
        raise AsrCapacityRecoveryFailureError(
            "content-addressed v2 disposition differs"
        )
    recovery.write_object(value, path)
    return path, value


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("inspect-retire",))
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    _parser().parse_args(argv)
    try:
        path, result = write_disposition()
        rendered = {
            **result,
            "written": str(path.relative_to(recovery.PROJECT_ROOT)),
        }
    except (
        AsrCapacityRecoveryFailureError,
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

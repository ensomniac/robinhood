"""Record the terminal v12 SEC archive-root HTTP 404 boundary."""

from __future__ import annotations

import argparse
import json
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import earnings_sec_expansion_capacity as capacity
import earnings_sec_expansion_collection as collection
import strategy_discovery
from historical_store import HistoricalDayStore, sha256_file


PLAN = (
    capacity.DEFAULT_ROOT
    / "metadata-collection-plan"
    / "plan-b17f1cd4982ee5e7c043dedda85e4bacbcbbe4679e91c12cc51c74c40b434b43.json"
)
PLAN_INSPECTION = (
    capacity.DEFAULT_ROOT
    / "metadata-collection-plan-inspection"
    / "inspection-59705455815bd3b1f87c2f2d4418024fbdb78d88028335ff0f4bbafd2885c336.json"
)
OFFICIAL_ARCHIVE_ROOT = (
    "https://www.sec.gov/files/dera/data/"
    "financial-statement-notes-data-sets"
)


class EarningsSecExpansionFailureError(RuntimeError):
    """The exact v12 source failure boundary drifted."""


def build_failure(
    *,
    failed_at: str,
    store: HistoricalDayStore | None = None,
) -> dict[str, Any]:
    for path in (
        Path(__file__).resolve(),
        capacity.PROJECT_ROOT
        / "earnings_sec_expansion_failure_inspection.py",
        PLAN,
        PLAN_INSPECTION,
    ):
        strategy_discovery.require_committed(path)
    plan = capacity._read(PLAN)
    inspection = capacity._read(PLAN_INSPECTION)
    rebuilt = collection.build_plan(created_at=str(plan["created_at"]))
    if not (
        plan == rebuilt
        and plan.get("plan_sha256")
        == capacity.self_hash(plan, "plan_sha256")
        and inspection.get("inspection_sha256")
        == capacity.self_hash(inspection, "inspection_sha256")
        and inspection.get("plan_sha256") == plan["plan_sha256"]
        and inspection.get("state")
        == "SEC_EXPANSION_COLLECTION_PLAN_INSPECTED_READY"
        and inspection.get("valid") is True
    ):
        raise EarningsSecExpansionFailureError(
            "v12 plan lineage is not valid"
        )
    historical_store = store or HistoricalDayStore.from_env()
    first = plan["requests"][0]
    destination = collection._archive_path(
        historical_store,
        plan["plan_sha256"],
        first["request_sha256"],
    )
    if destination.exists():
        raise EarningsSecExpansionFailureError(
            "v12 first request unexpectedly retained an archive"
        )
    expected_failed_url = (
        "https://www.sec.gov/files/dera/data/"
        "financial-statement-and-notes-data-sets/2012q1_notes.zip"
    )
    if first["url"] != expected_failed_url:
        raise EarningsSecExpansionFailureError(
            "v12 first request URL differs"
        )
    value: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": "earnings-sec-expansion-source-failure",
        "campaign_id": capacity.CAMPAIGN_ID,
        "family_id": capacity.FAMILY_ID,
        "successor_id": capacity.SUCCESSOR_ID,
        "state": "SEC_EXPANSION_ARCHIVE_ROOT_404_TERMINAL",
        "failed_at": capacity._timestamp(failed_at, "failed_at"),
        "plan_path": capacity._repo_path(PLAN),
        "plan_file_sha256": sha256_file(PLAN),
        "plan_sha256": plan["plan_sha256"],
        "inspection_path": capacity._repo_path(PLAN_INSPECTION),
        "inspection_file_sha256": sha256_file(PLAN_INSPECTION),
        "inspection_sha256": inspection["inspection_sha256"],
        "failed_request": first,
        "provider_response": {
            "http_status": 404,
            "classification": "PERMANENT_SOURCE_PATH_NOT_FOUND",
            "response_body_retained": False,
        },
        "root_path_disposition": {
            "frozen_root": capacity.ARCHIVE_ROOT,
            "official_index_root": OFFICIAL_ARCHIVE_ROOT,
            "difference": (
                "official path omits the word 'and' from the frozen root"
            ),
            "same_version_retry_permitted": False,
        },
        "provider_telemetry": {
            "requests": 1,
            "cache_hits": 0,
            "failures": 1,
            "retained_archives": 0,
            "retained_bytes": 0,
        },
        "metadata_rows_accessed": 0,
        "market_prices_accessed": False,
        "forward_returns_accessed": False,
        "strategy_metrics_computed": 0,
        "confirmation_outcomes_accessed": False,
        "broker_actions": 0,
        "successor_authority": {
            "source_root_correction_permitted_after_inspection": True,
            "same_archive_filename_graph_required": True,
            "same_event_semantics_required": True,
            "same_selection_accounting_required": True,
            "metadata_scope_may_be_reduced_outcome_blind_for_collection_cost": True,
            "market_price_access_permitted": False,
        },
    }
    value["failure_sha256"] = capacity.self_hash(
        value, "failure_sha256"
    )
    return value


def record_failure(
    *,
    failed_at: str,
    store: HistoricalDayStore | None = None,
    root: Path = capacity.DEFAULT_ROOT,
) -> tuple[Path, dict[str, Any]]:
    value = build_failure(failed_at=failed_at, store=store)
    path = (
        root
        / "metadata-source-failure"
        / f"failure-{value['failure_sha256']}.json"
    )
    capacity._write(path, value)
    return path, value


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("record",))
    parser.add_argument("--failed-at", required=True)
    args = parser.parse_args(argv)
    path, value = record_failure(failed_at=args.failed_at)
    print(
        json.dumps(
            {
                "path": capacity._repo_path(path),
                "failure_sha256": value["failure_sha256"],
                "state": value["state"],
                "provider_requests": 1,
                "retained_bytes": 0,
                "market_prices_accessed": False,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())

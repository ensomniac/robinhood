"""Independently inspect the collected dense-v2 session calendar."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import dense_capacity_inventory
import dense_session_calendar as calendar
import outcome_exposure
import strategy_discovery


PROJECT_ROOT = Path(__file__).resolve().parent
INSPECTION_KIND = "dense-session-calendar-data-inspection"


class DenseSessionCalendarInspectionError(RuntimeError):
    """The calendar bytes, provider binding, or untouched capacity differ."""


def inspect(
    collection_path: Path,
    *,
    inspected_at: str,
    root: Path = calendar.DEFAULT_ROOT,
    index_path: Path = outcome_exposure.DEFAULT_INDEX,
    enforce_commit: bool = True,
) -> tuple[Path, dict[str, Any]]:
    if enforce_commit:
        strategy_discovery.require_committed(collection_path)
    status = strategy_discovery.load_artifact(
        collection_path, expected_kind=calendar.COLLECTION_KIND
    )
    if not (
        status.get("state") == "CALENDAR_COLLECTED_UNINSPECTED"
        and status.get("provider_requests") == 1
        and status.get("market_prices_accessed") is False
        and status.get("target_outcomes_accessed") is False
        and status.get("broker_actions") == 0
    ):
        raise DenseSessionCalendarInspectionError("calendar collection status is unsafe")
    calendar_path = PROJECT_ROOT / str(status["calendar_path"])
    source_path = PROJECT_ROOT / str(status["source_path"])
    if not calendar_path.is_file() or not source_path.is_file():
        raise DenseSessionCalendarInspectionError("calendar output is incomplete")
    rows = json.loads(calendar_path.read_text(encoding="utf-8"))
    normalized = calendar.normalize_rows(rows)
    source = json.loads(source_path.read_text(encoding="utf-8"))
    observed_hash = strategy_discovery._file_hash(calendar_path)
    if not (
        normalized == rows
        and observed_hash == status["calendar_sha256"] == source["calendar_sha256"]
        and source["contract_sha256"] == status["contract_sha256"]
        and source["target_outcomes_accessed"] is False
    ):
        raise DenseSessionCalendarInspectionError("calendar reconstruction differs")
    records = outcome_exposure.read_index(index_path)
    exposed = dense_capacity_inventory._globally_exposed_dates(records)
    allocations = dense_capacity_inventory._allocate(
        dense_capacity_inventory._calendar(calendar_path), exposed
    )
    full_sessions = sum(
        len(item["warmup"]) + len(item["evidence"]) for item in allocations
    )
    payload = {
        "schema_version": 1,
        "artifact_kind": INSPECTION_KIND,
        "campaign_id": status["campaign_id"],
        "state": "CALENDAR_INSPECTED_READY",
        "collection_path": calendar._repo_path(collection_path),
        "collection_sha256": status["artifact_sha256"],
        "calendar_path": status["calendar_path"],
        "calendar_sha256": observed_hash,
        "source_path": status["source_path"],
        "sessions": len(rows),
        "full_sessions": len(dense_capacity_inventory._calendar(calendar_path)),
        "untouched_dense_allocation_sessions": full_sessions,
        "outcome_exposure_index_sha256": outcome_exposure.audit(index_path)[
            "index_sha256"
        ],
        "checks": {
            "provider_scope_rebuilt": True,
            "calendar_rows_rebuilt": True,
            "calendar_hash_rebuilt": True,
            "source_binding_rebuilt": True,
            "three_disjoint_warmup_and_evidence_segments_available": True,
            "target_outcomes_absent": True,
        },
        "provider_requests": 1,
        "market_prices_accessed": False,
        "target_outcomes_accessed": False,
        "broker_actions": 0,
        "inspected_at": inspected_at,
    }
    return strategy_discovery._write_artifact(
        payload, root / "data-inspection", "dense-session-calendar-data-inspection"
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("collection", type=Path)
    parser.add_argument("--inspected-at", required=True)
    parser.add_argument("--root", type=Path, default=calendar.DEFAULT_ROOT)
    parser.add_argument("--index", type=Path, default=outcome_exposure.DEFAULT_INDEX)
    args = parser.parse_args()
    try:
        path, artifact = inspect(
            args.collection,
            inspected_at=args.inspected_at,
            root=args.root,
            index_path=args.index,
        )
        print(
            json.dumps(
                {
                    "written": calendar._repo_path(path),
                    "artifact_sha256": artifact["artifact_sha256"],
                    "state": artifact["state"],
                    "target_outcomes_accessed": False,
                    "broker_actions": 0,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    except (
        DenseSessionCalendarInspectionError,
        calendar.DenseSessionCalendarError,
        outcome_exposure.OutcomeExposureError,
        strategy_discovery.StrategyDiscoveryError,
        OSError,
        ValueError,
    ) as exc:
        print(json.dumps({"error": str(exc), "error_type": type(exc).__name__}, indent=2))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

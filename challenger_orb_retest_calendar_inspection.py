"""Independently inspect the challenger calendar contract and result."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Mapping, Sequence
from datetime import date, time
from pathlib import Path
from typing import Any

import challenger_orb_retest_calendar as calendar
from learning_data import LearningDataError, load_frozen_dataset_contract


DEFAULT_RESULT = calendar.PROJECT_ROOT / "research_results/2026-07-20-challenger-orb-retest-calendar-inspection.json"


class ChallengerCalendarInspectionError(RuntimeError):
    """Independent calendar reconstruction found a mismatch."""


def inspect_contract(manifest_path: Path) -> dict[str, Any]:
    manifest = load_frozen_dataset_contract(manifest_path)
    if manifest.get("dataset_id") != calendar.DATASET_ID:
        raise ChallengerCalendarInspectionError("calendar dataset identity differs")
    expected = calendar._expected_contract()
    for key, value in expected.items():
        if manifest.get(key) != value:
            raise ChallengerCalendarInspectionError(f"calendar contract drifted at {key}")
    for value in manifest["implementation_contract"].values():
        calendar._verify_binding(value)
    if calendar.DEFAULT_CALENDAR.exists() or calendar.DEFAULT_SOURCE.exists():
        raise ChallengerCalendarInspectionError("calendar output exists before collection")
    return {
        "schema_version": 1,
        "dataset_id": calendar.DATASET_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "status": "FROZEN_READY",
        "inspected": True,
        "pre_freeze_output_artifacts": 0,
        "implementation_and_query_rebuilt": True,
        "target_outcomes_observed_or_derived": False,
    }


def _independent_rows(value: Any) -> list[dict[str, str]]:
    if not isinstance(value, list) or len(value) < 850:
        raise ChallengerCalendarInspectionError("calendar coverage is incomplete")
    rebuilt: list[dict[str, str]] = []
    for row in value:
        if not isinstance(row, Mapping):
            raise ChallengerCalendarInspectionError("calendar row is malformed")
        try:
            day = date.fromisoformat(str(row["date"]))
            opened = time.fromisoformat(str(row["open_et"]))
            closed = time.fromisoformat(str(row["close_et"]))
        except (KeyError, ValueError) as exc:
            raise ChallengerCalendarInspectionError("calendar row is malformed") from exc
        if day.weekday() >= 5 or opened >= closed:
            raise ChallengerCalendarInspectionError("calendar session is invalid")
        rebuilt.append(
            {
                "date": day.isoformat(),
                "open_et": opened.isoformat(timespec="minutes"),
                "close_et": closed.isoformat(timespec="minutes"),
            }
        )
    dates = [row["date"] for row in rebuilt]
    if dates != sorted(set(dates)):
        raise ChallengerCalendarInspectionError("calendar dates do not reconcile")
    if dates[0] < calendar.CALENDAR_START or dates[-1] > calendar.CALENDAR_END:
        raise ChallengerCalendarInspectionError("calendar escaped frozen query")
    return rebuilt


def inspect_result(manifest_path: Path, output_path: Path) -> dict[str, Any]:
    manifest = load_frozen_dataset_contract(manifest_path)
    if manifest.get("dataset_id") != calendar.DATASET_ID:
        raise ChallengerCalendarInspectionError("calendar dataset identity differs")
    for value in manifest["implementation_contract"].values():
        calendar._verify_binding(value)
    rows = _independent_rows(calendar._read_json(calendar.DEFAULT_CALENDAR))
    source = calendar._read_json(calendar.DEFAULT_SOURCE)
    if not isinstance(source, Mapping):
        raise ChallengerCalendarInspectionError("calendar source is malformed")
    expected = {
        "dataset_id": calendar.DATASET_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "provider": "Alpaca Market Calendar API",
        "endpoint": calendar.ENDPOINT,
        "query": {"start": calendar.CALENDAR_START, "end": calendar.CALENDAR_END},
        "sessions": len(rows),
        "first_session": rows[0]["date"],
        "last_session": rows[-1]["date"],
        "calendar_path": calendar._repo_path(calendar.DEFAULT_CALENDAR),
        "calendar_sha256": calendar._sha256_file(calendar.DEFAULT_CALENDAR),
        "target_outcomes_observed_or_derived": False,
    }
    for key, value in expected.items():
        if source.get(key) != value:
            raise ChallengerCalendarInspectionError(f"calendar source drifted at {key}")
    result = {
        "schema_version": 1,
        "dataset_id": calendar.DATASET_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "status": "COLLECTION_INSPECTED",
        "inspected": True,
        "sessions": len(rows),
        "first_session": rows[0]["date"],
        "last_session": rows[-1]["date"],
        "calendar_sha256": expected["calendar_sha256"],
        "inspection": {
            "query_and_source_rebuilt": True,
            "session_rows_rebuilt": True,
            "hashes_and_counts_rebuilt": True,
            "outcome_lock_rechecked": True,
            "valid": True,
        },
        "target_outcomes_observed_or_derived": False,
    }
    calendar._write_json(output_path, result)
    calendar._write_json(calendar.DEFAULT_COLLECTION_STATUS, result)
    return result


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    contract = sub.add_parser("inspect-contract")
    contract.add_argument("manifest", type=Path)
    result = sub.add_parser("inspect")
    result.add_argument("manifest", type=Path)
    parser.add_argument("--output", type=Path, default=DEFAULT_RESULT)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "inspect-contract":
            result = inspect_contract(args.manifest)
            calendar._write_json(calendar.DEFAULT_CONTRACT_STATUS, result)
        else:
            result = inspect_result(args.manifest, args.output)
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    except (
        calendar.ChallengerCalendarError,
        ChallengerCalendarInspectionError,
        LearningDataError,
        OSError,
    ) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())

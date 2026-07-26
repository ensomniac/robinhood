"""Independently rebuild and inspect the merged pre-FOMC exchange calendar."""

from __future__ import annotations

import argparse
import json
from collections.abc import Mapping, Sequence
from datetime import datetime
from pathlib import Path
from typing import Any

import fomc_preannouncement as family
import strategy_discovery
from historical_store import sha256_file


class FomcPreannouncementInspectionError(ValueError):
    """The committed calendar lineage cannot be independently reproduced."""


def _timestamp(value: str) -> str:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise FomcPreannouncementInspectionError(
            "inspected_at is invalid"
        ) from exc
    if parsed.tzinfo is None:
        raise FomcPreannouncementInspectionError(
            "inspected_at must include a timezone"
        )
    return value


def _lineage(*, enforce_commit: bool) -> tuple[Path, dict[str, Any]]:
    matches = sorted((family.DEFAULT_ROOT / "calendar-lineage").glob("*.json"))
    if len(matches) != 1:
        raise FomcPreannouncementInspectionError(
            "expected one exact calendar-lineage artifact"
        )
    path = matches[0]
    if enforce_commit:
        strategy_discovery.require_committed(path)
        strategy_discovery.require_committed(family.CALENDAR_PATH)
        strategy_discovery.require_committed(Path(__file__).resolve())
    value = strategy_discovery.load_artifact(
        path, expected_kind=family.CALENDAR_LINEAGE_KIND
    )
    return path, value


def _independent_merge() -> list[dict[str, str]]:
    merged: dict[str, dict[str, str]] = {}
    for path in family.SOURCE_CALENDARS:
        raw = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(raw, list):
            raise FomcPreannouncementInspectionError(
                f"source calendar is not an array: {path}"
            )
        for raw_row in raw:
            if not isinstance(raw_row, Mapping):
                continue
            day = raw_row.get("date")
            if not (
                isinstance(day, str)
                and "2009-01-01" <= day <= family.CONFIRMATION_END
                and raw_row.get("open_et") == "09:30"
                and raw_row.get("close_et") == "16:00"
            ):
                continue
            row = {"date": day, "open_et": "09:30", "close_et": "16:00"}
            if day in merged and merged[day] != row:
                raise FomcPreannouncementInspectionError(
                    f"source calendar conflict on {day}"
                )
            merged[day] = row
    return [merged[day] for day in sorted(merged)]


def inspect(
    *, inspected_at: str, enforce_commit: bool = True
) -> tuple[Path, dict[str, Any]]:
    _timestamp(inspected_at)
    lineage_path, lineage = _lineage(enforce_commit=enforce_commit)
    rebuilt = _independent_merge()
    observed = json.loads(family.CALENDAR_PATH.read_text(encoding="utf-8"))
    dates = [row["date"] for row in rebuilt]
    decisions = family.decision_dates()
    split = family.partitions()
    checks = {
        "lineage_state": lineage.get("state")
        == "CALENDAR_MERGED_UNINSPECTED",
        "calendar_path_bound": lineage.get("calendar_path")
        == family._repo_path(family.CALENDAR_PATH),
        "calendar_hash_rebuilt": lineage.get("calendar_sha256")
        == sha256_file(family.CALENDAR_PATH),
        "calendar_rows_rebuilt": observed == rebuilt,
        "source_authorities_rebuilt": lineage.get("source_authorities")
        == family._source_authorities(enforce_commit=enforce_commit),
        "chronological_unique_sessions": dates == sorted(set(dates)),
        "decision_dates_are_sessions": set(decisions).issubset(dates),
        "development_capacity_64": len(split["development_signal_dates"]) == 64,
        "scheduled_confirmation_inventory_55": len(
            split["confirmation_inventory_signal_dates"]
        )
        == 55,
        "untouched_confirmation_capacity_35": len(
            split["confirmation_signal_dates"]
        )
        == 35,
        "contaminated_confirmation_entries_excluded_20": len(
            split["confirmation_excluded_exposed_signal_dates"]
        )
        == 20,
        "five_session_embargo": len(split["embargo_dates"]) == 5,
        "zero_price_access": lineage.get("market_prices_accessed") is False
        and lineage.get("target_outcomes_accessed") is False,
        "zero_broker_actions": lineage.get("broker_actions") == 0,
    }
    if not all(checks.values()):
        raise FomcPreannouncementInspectionError(
            f"calendar inspection failed: {[key for key, ok in checks.items() if not ok]}"
        )
    payload = {
        "schema_version": 1,
        "artifact_kind": family.CALENDAR_INSPECTION_KIND,
        "campaign_id": family.CAMPAIGN_ID,
        "state": "CALENDAR_INSPECTED_READY",
        "lineage_path": family._repo_path(lineage_path),
        "lineage_sha256": lineage["artifact_sha256"],
        "calendar_path": family._repo_path(family.CALENDAR_PATH),
        "calendar_sha256": sha256_file(family.CALENDAR_PATH),
        "sessions": len(rebuilt),
        "first_session": dates[0],
        "last_session": dates[-1],
        "development_signal_capacity": 64,
        "scheduled_confirmation_signal_inventory": 55,
        "untouched_confirmation_signal_capacity": 35,
        "checks": checks,
        "provider_requests_added": 0,
        "market_prices_accessed": False,
        "target_outcomes_accessed": False,
        "broker_actions": 0,
        "inspected_at": inspected_at,
    }
    return strategy_discovery._write_artifact(
        payload,
        family.DEFAULT_ROOT / "calendar-inspection",
        family.CALENDAR_INSPECTION_KIND,
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--inspected-at", required=True)
    args = parser.parse_args(argv)
    try:
        path, value = inspect(inspected_at=args.inspected_at)
        print(
            json.dumps(
                {
                    "path": family._repo_path(path),
                    "state": value["state"],
                    "sessions": value["sessions"],
                    "development_signal_capacity": value[
                        "development_signal_capacity"
                    ],
                    "confirmation_signal_capacity": value[
                        "untouched_confirmation_signal_capacity"
                    ],
                    "market_prices_accessed": False,
                    "broker_actions": 0,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    except (
        FomcPreannouncementInspectionError,
        family.FomcPreannouncementError,
        strategy_discovery.StrategyDiscoveryError,
        OSError,
        json.JSONDecodeError,
    ) as exc:
        print(
            json.dumps(
                {"error": str(exc), "error_type": type(exc).__name__},
                indent=2,
                sort_keys=True,
            )
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

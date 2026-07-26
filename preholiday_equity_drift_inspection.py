"""Independently inspect the pre-holiday calendar and clean capacity."""

from __future__ import annotations

import argparse
import json
from collections.abc import Mapping, Sequence
from datetime import date, datetime
from pathlib import Path
from typing import Any

import outcome_exposure
import preholiday_equity_drift as family
import strategy_discovery
from historical_store import sha256_file


class PreholidayEquityDriftInspectionError(ValueError):
    """The committed calendar lineage cannot be independently reproduced."""


def _timestamp(value: str) -> str:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise PreholidayEquityDriftInspectionError(
            "inspected_at is invalid"
        ) from exc
    if parsed.tzinfo is None:
        raise PreholidayEquityDriftInspectionError(
            "inspected_at must include a timezone"
        )
    if parsed.date() > date.today():
        raise PreholidayEquityDriftInspectionError(
            "inspected_at cannot be future-dated"
        )
    return value


def _lineage(*, enforce_commit: bool) -> tuple[Path, dict[str, Any]]:
    matches = sorted(
        (family.DEFAULT_ROOT / "calendar-lineage").glob("*.json")
    )
    if len(matches) != 1:
        raise PreholidayEquityDriftInspectionError(
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
            raise PreholidayEquityDriftInspectionError(
                f"source calendar is not an array: {path}"
            )
        for raw_row in raw:
            if not isinstance(raw_row, Mapping):
                continue
            day = raw_row.get("date")
            close_et = raw_row.get("close_et")
            if not (
                isinstance(day, str)
                and "2009-01-01" <= day <= family.CONFIRMATION_END
                and raw_row.get("open_et") == "09:30"
                and close_et in {"13:00", "16:00"}
            ):
                continue
            row = {
                "date": day,
                "open_et": "09:30",
                "close_et": str(close_et),
            }
            if day in merged and merged[day] != row:
                raise PreholidayEquityDriftInspectionError(
                    f"source calendar conflict on {day}"
                )
            merged[day] = row
    return [merged[day] for day in sorted(merged)]


def _independent_events(
    rows: Sequence[Mapping[str, str]],
) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    for current, following in zip(rows, rows[1:], strict=False):
        current_day = str(current["date"])
        gap_days = (
            date.fromisoformat(str(following["date"]))
            - date.fromisoformat(current_day)
        ).days
        if (
            current_day < family.DEVELOPMENT_START
            or gap_days in {1, 3}
            or current_day
            in family.UNSCHEDULED_CLOSURE_PRIOR_SESSIONS
        ):
            continue
        events.append(
            {
                "signal_date": current_day,
                "scheduled_close_et": current["close_et"],
                "next_session_date": following["date"],
                "calendar_gap_days": gap_days,
            }
        )
    return events


def inspect(
    *, inspected_at: str, enforce_commit: bool = True
) -> tuple[Path, dict[str, Any]]:
    _timestamp(inspected_at)
    lineage_path, lineage = _lineage(enforce_commit=enforce_commit)
    rebuilt = _independent_merge()
    events = _independent_events(rebuilt)
    observed = json.loads(
        family.CALENDAR_PATH.read_text(encoding="utf-8")
    )
    dates = [row["date"] for row in rebuilt]
    split = family.partitions()
    checks = {
        "lineage_state": (
            lineage.get("state") == "CALENDAR_MERGED_UNINSPECTED"
        ),
        "calendar_path_bound": (
            lineage.get("calendar_path")
            == family._repo_path(family.CALENDAR_PATH)
        ),
        "calendar_hash_rebuilt": (
            lineage.get("calendar_sha256")
            == sha256_file(family.CALENDAR_PATH)
        ),
        "calendar_rows_rebuilt": observed == rebuilt,
        "source_authorities_rebuilt": (
            lineage.get("source_authorities")
            == family._source_authorities(
                enforce_commit=enforce_commit
            )
        ),
        "chronological_unique_sessions": dates == sorted(set(dates)),
        "early_closes_retained": (
            sum(row["close_et"] == "13:00" for row in rebuilt) > 0
        ),
        "event_inventory_rebuilt": events == family.preholiday_events(),
        "formal_capacity_145": len(events) == 145,
        "development_capacity_80": (
            len(split["development_signal_dates"]) == 80
        ),
        "scheduled_confirmation_inventory_65": (
            len(split["confirmation_inventory_signal_dates"]) == 65
        ),
        "untouched_confirmation_capacity_48": (
            len(split["confirmation_signal_dates"]) == 48
        ),
        "contaminated_confirmation_entries_excluded_17": (
            len(
                split[
                    "confirmation_excluded_exposed_signal_dates"
                ]
            )
            == 17
        ),
        "five_session_embargo": len(split["embargo_dates"]) == 5,
        "zero_price_access": (
            lineage.get("market_prices_accessed") is False
            and lineage.get("target_outcomes_accessed") is False
        ),
        "zero_broker_actions": lineage.get("broker_actions") == 0,
    }
    if not all(checks.values()):
        raise PreholidayEquityDriftInspectionError(
            "calendar inspection failed: "
            f"{[key for key, ok in checks.items() if not ok]}"
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
        "outcome_exposure_index_sha256": sha256_file(
            outcome_exposure.DEFAULT_INDEX
        ),
        "sessions": len(rebuilt),
        "first_session": dates[0],
        "last_session": dates[-1],
        "formal_signal_capacity": len(events),
        "development_signal_capacity": 80,
        "scheduled_confirmation_signal_inventory": 65,
        "untouched_confirmation_signal_capacity": 48,
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
                    "formal_signal_capacity": value[
                        "formal_signal_capacity"
                    ],
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
        PreholidayEquityDriftInspectionError,
        family.PreholidayEquityDriftError,
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

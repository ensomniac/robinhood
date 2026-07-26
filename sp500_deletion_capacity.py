"""Derive S&P 500 deletion capacity from inspected official release pages."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from datetime import date, datetime
from pathlib import Path
from typing import Any

import sp500_addition_capacity as source
import strategy_discovery
from historical_store import sha256_file


PROJECT_ROOT = Path(__file__).resolve().parent
CAMPAIGN_ID = "multi-strategy-portfolio-validation-v2"
FAMILY_ID = "sp500-deletion-forced-selling-rebound"
MECHANISM_FAMILY = "large-index-deletion-forced-selling-rebound"
DEFAULT_ROOT = (
    PROJECT_ROOT / "strategy_tournament/v2/sp500-deletion-capacity"
)
SOURCE_INSPECTION = (
    PROJECT_ROOT
    / "strategy_tournament/v2/sp500-index-addition-capacity/"
    "capacity-inspection/"
    "sp500-addition-capacity-inspection-"
    "39c62f408f2dccd31435d579d55c971b4b907ea0b14e1c7fd20dc7c84f215e22.json"
)
FAST_LANE_EVENTS = 100
RETIRE_BELOW_EVENTS = 50


class Sp500DeletionCapacityError(RuntimeError):
    """Official deletion capacity or its predecessor evidence drifted."""


def _controller_hashes() -> dict[str, str]:
    result: dict[str, str] = {}
    for relative in (
        "sp500_deletion_capacity.py",
        "sp500_addition_capacity.py",
    ):
        path = PROJECT_ROOT / relative
        strategy_discovery.require_committed(path)
        result[relative] = sha256_file(path)
    return result


def _timestamp(value: str, field: str) -> str:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise Sp500DeletionCapacityError(f"{field} is invalid") from exc
    if parsed.tzinfo is None:
        raise Sp500DeletionCapacityError(f"{field} needs a timezone")
    return value


def _relative(path: Path) -> str:
    return str(path.resolve().relative_to(PROJECT_ROOT.resolve()))


def _event(
    *,
    published: datetime,
    effective: date,
    company_name: str,
    ticker: str,
    source_url: str,
) -> dict[str, Any]:
    return {
        "announcement_at": published.isoformat(),
        "announcement_date": published.date().isoformat(),
        "effective_date": effective.isoformat(),
        "index_name": "S&P 500",
        "action": "Deletion",
        "company_name": company_name,
        "ticker": ticker,
        "source_url": source_url,
    }


def _structured_tickers(
    raw_ticker: str, *, source_url: str
) -> tuple[str, ...]:
    """Normalize one official cell, including explicit dual-class tickers."""

    tickers = tuple(
        ticker.upper().replace(" ", "")
        for ticker in raw_ticker.split("/")
        if ticker.strip()
    )
    if (
        not tickers
        or len(tickers) > 2
        or any(
            re.fullmatch(r"[A-Z][A-Z0-9.-]{0,11}", ticker) is None
            for ticker in tickers
        )
        or len(set(tickers)) != len(tickers)
    ):
        raise Sp500DeletionCapacityError(
            f"{source_url}: invalid structured ticker {raw_ticker}"
        )
    return tickers


def parse_release(
    raw: bytes,
    *,
    source_url: str,
    listed_date: str,
) -> dict[str, Any]:
    """Extract only exact S&P 500 deletion rows from one official release."""

    parser = source._ReleaseParser()
    parser.feed(raw.decode("utf-8", errors="strict"))
    parser.close()
    if parser.itemdate is None:
        return {
            "source_url": source_url,
            "listed_date": listed_date,
            "eligible_events": [],
            "ineligible_structured_rows": [],
            "terminal_reason": "MISSING_PUBLICATION_TIMESTAMP",
        }
    published = source._parse_itemdate(parser.itemdate)
    if published.date().isoformat() != listed_date:
        raise Sp500DeletionCapacityError(
            f"{source_url}: listing date and ITEMDATE differ"
        )
    events: list[dict[str, Any]] = []
    ineligible_rows: list[dict[str, str]] = []
    visible_text = re.sub(
        r"\s+", " ", " ".join(parser.visible_chunks)
    ).strip()
    for table in parser.tables:
        inherited_effective_date: str | None = None
        for row in table:
            normalized = [
                value.replace("®", "").replace("\xa0", " ").strip()
                for value in row
            ]
            if len(normalized) < 5:
                continue
            if normalized[0]:
                inherited_effective_date = normalized[0]
            for offset in range(0, len(normalized) - 4):
                segment = normalized[offset : offset + 6]
                if not (
                    len(segment) >= 5
                    and segment[1] == "S&P 500"
                    and segment[2].casefold() == "deletion"
                ):
                    continue
                effective_cell = (
                    segment[0] or inherited_effective_date or ""
                )
                tickers = _structured_tickers(
                    segment[4], source_url=source_url
                )
                if effective_cell.casefold() in {
                    "",
                    "tba",
                    "to be announced",
                }:
                    ineligible_rows.extend(
                        {
                            "ticker": ticker,
                            "raw_effective_date": effective_cell,
                            "terminal_reason": "MISSING_EFFECTIVE_DATE",
                        }
                        for ticker in tickers
                    )
                    continue
                effective = source._parse_effective_date(
                    effective_cell, announcement=published.date()
                )
                if effective <= published.date():
                    raise Sp500DeletionCapacityError(
                        f"{source_url}: effective date is not later"
                    )
                events.extend(
                    _event(
                        published=published,
                        effective=effective,
                        company_name=segment[3],
                        ticker=ticker,
                        source_url=source_url,
                    )
                    for ticker in tickers
                )
        if not table or not table[0]:
            continue
        legacy_heading = re.fullmatch(
            r"S&P 500(?: INDEX)?\s*[\-\u2013]\s*(.+)",
            re.sub(r"\s+", " ", table[0][0]).strip(),
            re.IGNORECASE,
        )
        if legacy_heading is None:
            continue
        raw_effective = legacy_heading.group(1).strip()
        effective: date | None
        if raw_effective.casefold() in {"tba", "to be announced"}:
            effective = None
        else:
            effective = source._parse_effective_date(
                raw_effective, announcement=published.date()
            )
            if effective <= published.date():
                raise Sp500DeletionCapacityError(
                    f"{source_url}: effective date is not later"
                )
        action: str | None = None
        for row in table[1:]:
            if len(row) < 2:
                continue
            row_action = row[0].strip().upper()
            if row_action in {"ADDED", "DELETED"}:
                action = row_action
            company = row[1].strip()
            if (
                action != "DELETED"
                or not company
                or company.casefold() == "company"
            ):
                continue
            ticker = source._legacy_company_ticker(company, visible_text)
            if ticker is None:
                ineligible_rows.append(
                    {
                        "company_name": company,
                        "ticker": "",
                        "raw_effective_date": raw_effective,
                        "terminal_reason": "UNRESOLVED_TICKER_IDENTITY",
                    }
                )
                continue
            if effective is None:
                ineligible_rows.append(
                    {
                        "ticker": ticker,
                        "raw_effective_date": raw_effective,
                        "terminal_reason": "MISSING_EFFECTIVE_DATE",
                    }
                )
                continue
            events.append(
                _event(
                    published=published,
                    effective=effective,
                    company_name=company,
                    ticker=ticker,
                    source_url=source_url,
                )
            )
    unique = {
        (event["ticker"], event["effective_date"]): event
        for event in events
    }
    retained = [unique[key] for key in sorted(unique)]
    return {
        "source_url": source_url,
        "listed_date": listed_date,
        "publication_timestamp": published.isoformat(),
        "eligible_events": retained,
        "ineligible_structured_rows": ineligible_rows,
        "terminal_reason": (
            "ELIGIBLE_SP500_DELETION"
            if retained
            else (
                (
                    ineligible_rows[0]["terminal_reason"]
                    if len(
                        {
                            row["terminal_reason"]
                            for row in ineligible_rows
                        }
                    )
                    == 1
                    else "INELIGIBLE_STRUCTURED_ROWS"
                )
                if ineligible_rows
                else "NO_STRUCTURED_SP500_DELETION"
            )
        ),
    }


def _deduplicate(
    releases: Sequence[Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], int]:
    by_identity: dict[tuple[str, str], dict[str, Any]] = {}
    duplicate_count = 0
    for release in releases:
        for raw in release["eligible_events"]:
            event = dict(raw)
            key = (str(event["ticker"]), str(event["effective_date"]))
            existing = by_identity.get(key)
            if existing is None:
                by_identity[key] = event
                continue
            duplicate_count += 1
            if (
                str(event["announcement_at"]),
                str(event["source_url"]),
            ) < (
                str(existing["announcement_at"]),
                str(existing["source_url"]),
            ):
                by_identity[key] = event
    return [
        by_identity[key] for key in sorted(by_identity)
    ], duplicate_count


def build_capacity(*, created_at: str) -> dict[str, Any]:
    source_inspection = strategy_discovery.load_artifact(
        SOURCE_INSPECTION,
        expected_kind="sp500-addition-capacity-inspection",
    )
    if not (
        source_inspection["state"] == "CAPACITY_READY_FAST_LANE"
        and source_inspection["market_outcomes_accessed"] is False
        and source_inspection["target_return_access_permitted"] is False
        and source_inspection["broker_actions_permitted"] is False
    ):
        raise Sp500DeletionCapacityError(
            "official release source inspection is not outcome-blind"
        )
    collection_path = PROJECT_ROOT / str(
        source_inspection["collection_path"]
    )
    collection = source._load_artifact(
        collection_path, "sp500-addition-release-page-collection"
    )
    if not (
        collection["artifact_sha256"]
        == source_inspection["collection_sha256"]
        and collection["state"] == "RELEASE_PAGES_COLLECTED_UNINSPECTED"
        and collection["market_outcomes_accessed"] is False
        and collection["market_price_requests"] == 0
        and collection["confirmation_accessed"] is False
        and collection["broker_actions"] == 0
    ):
        raise Sp500DeletionCapacityError(
            "official release-page collection binding drifted"
        )
    parsed_releases: list[dict[str, Any]] = []
    denominator: list[dict[str, Any]] = []
    total_bytes = 0
    for task in collection["tasks"]:
        cache = source._source_path(task["cache_relative_path"])
        raw = source._read_cached(cache)
        if (
            hashlib.sha256(raw).hexdigest() != task["raw_sha256"]
            or len(raw) != task["raw_bytes"]
        ):
            raise Sp500DeletionCapacityError(
                f"release cache drifted: {task['url']}"
            )
        parsed = parse_release(
            raw,
            source_url=task["url"],
            listed_date=task["listed_date"],
        )
        parsed_releases.append(parsed)
        denominator.append(
            {
                "source_url": task["url"],
                "listed_date": task["listed_date"],
                "title": task["title"],
                "terminal_reason": parsed["terminal_reason"],
                "eligible_event_count": len(parsed["eligible_events"]),
                "ineligible_structured_row_count": len(
                    parsed["ineligible_structured_rows"]
                ),
            }
        )
        total_bytes += len(raw)
    events, duplicate_count = _deduplicate(parsed_releases)
    signal_dates = sorted(
        {str(event["announcement_date"]) for event in events}
    )
    if len(events) < RETIRE_BELOW_EVENTS:
        state = "RETIRED_INSUFFICIENT_FORMAL_CAPACITY"
    elif len(events) < FAST_LANE_EVENTS:
        state = "DEFERRED_SINGLE_RULE_CAPACITY"
    else:
        state = "CAPACITY_READY_FAST_LANE_UNINSPECTED"
    return {
        "schema_version": 1,
        "artifact_kind": "sp500-deletion-capacity",
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "mechanism_family": MECHANISM_FAMILY,
        "created_at": _timestamp(created_at, "created_at"),
        "state": state,
        "controller_hashes": _controller_hashes(),
        "source_inspection_path": _relative(SOURCE_INSPECTION),
        "source_inspection_sha256": source_inspection["artifact_sha256"],
        "source_collection_path": _relative(collection_path),
        "source_collection_sha256": collection["artifact_sha256"],
        "source_task_count": len(collection["tasks"]),
        "source_bytes_rebuilt": total_bytes,
        "provider_requests": 0,
        "market_price_requests": 0,
        "market_outcomes_accessed": False,
        "target_return_access_permitted": False,
        "confirmation_accessed": False,
        "eligible_event_count": len(events),
        "signal_date_count": len(signal_dates),
        "duplicate_event_count": duplicate_count,
        "capacity_policy": {
            "retire_below_verified_events": RETIRE_BELOW_EVENTS,
            "fast_lane_at_verified_events": FAST_LANE_EVENTS,
        },
        "denominator": denominator,
        "events": events,
        "broker_actions_permitted": False,
    }


def create_capacity(
    *, created_at: str, root: Path = DEFAULT_ROOT
) -> tuple[Path, dict[str, Any]]:
    strategy_discovery.require_committed(Path(__file__).resolve())
    strategy_discovery.require_committed(SOURCE_INSPECTION)
    payload = build_capacity(created_at=created_at)
    return strategy_discovery._write_artifact(
        payload, root / "capacity", "sp500-deletion-capacity"
    )


def inspect_capacity(
    capacity_path: Path,
    *,
    inspected_at: str,
    root: Path = DEFAULT_ROOT,
) -> tuple[Path, dict[str, Any]]:
    strategy_discovery.require_committed(capacity_path)
    capacity = strategy_discovery.load_artifact(
        capacity_path, expected_kind="sp500-deletion-capacity"
    )
    rebuilt = build_capacity(created_at=str(capacity["created_at"]))
    if {
        key: item
        for key, item in capacity.items()
        if key != "artifact_sha256"
    } != rebuilt:
        raise Sp500DeletionCapacityError(
            "deletion capacity rebuild differs"
        )
    ready = capacity["eligible_event_count"] >= FAST_LANE_EVENTS
    payload = {
        "schema_version": 1,
        "artifact_kind": "sp500-deletion-capacity-inspection",
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "mechanism_family": MECHANISM_FAMILY,
        "inspected_at": _timestamp(inspected_at, "inspected_at"),
        "state": (
            "CAPACITY_READY_FAST_LANE"
            if ready
            else capacity["state"]
        ),
        "capacity_path": _relative(capacity_path),
        "capacity_sha256": capacity["artifact_sha256"],
        "eligible_event_count": capacity["eligible_event_count"],
        "signal_date_count": capacity["signal_date_count"],
        "events": capacity["events"],
        "checks": {
            "capacity_exactly_rebuilt": True,
            "official_source_inspection_rebuilt": True,
            "all_release_hashes_rebuilt": True,
            "publication_timestamps_rebuilt": True,
            "effective_dates_rebuilt": True,
            "ticker_identities_rebuilt": True,
            "deduplication_rebuilt": True,
            "zero_market_outcomes_rebuilt": True,
            "capacity_policy_rebuilt": True,
            "valid": True,
        },
        "development_search_permitted": ready,
        "target_return_access_permitted": False,
        "market_outcomes_accessed": False,
        "provider_requests": 0,
        "broker_actions_permitted": False,
    }
    return strategy_discovery._write_artifact(
        payload,
        root / "capacity-inspection",
        "sp500-deletion-capacity-inspection",
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    create = subparsers.add_parser("create")
    create.add_argument("--created-at", required=True)
    inspect = subparsers.add_parser("inspect")
    inspect.add_argument("capacity", type=Path)
    inspect.add_argument("--inspected-at", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.command == "create":
        path, artifact = create_capacity(created_at=args.created_at)
    else:
        path, artifact = inspect_capacity(
            args.capacity, inspected_at=args.inspected_at
        )
    print(
        json.dumps(
            {
                "path": _relative(path),
                "state": artifact["state"],
                "eligible_event_count": artifact[
                    "eligible_event_count"
                ],
                "signal_date_count": artifact["signal_date_count"],
                "artifact_sha256": artifact["artifact_sha256"],
                "provider_requests": artifact["provider_requests"],
                "market_outcomes_accessed": artifact[
                    "market_outcomes_accessed"
                ],
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""Freeze outcome-blind capacity for external S&P 400/600 additions.

The source bytes were previously collected and independently inspected for the
S&P 500 addition campaign.  This controller does not contact a provider.  It
freezes the exact committed page-cache graph before a new parser may inspect
MidCap 400 and SmallCap 600 rows.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from datetime import date
from pathlib import Path
from typing import Any

import outcome_exposure
import sp500_addition_capacity as source
import strategy_discovery
from historical_store import sha256_file


PROJECT_ROOT = Path(__file__).resolve().parent
CAMPAIGN_ID = source.CAMPAIGN_ID
FAMILY_ID = "sp-mid-small-external-index-addition-forced-demand"
SUCCESSOR_ID = f"{FAMILY_ID}-v1"
MECHANISM_FAMILY = "external-mid-small-index-addition-forced-demand"
PUBLIC_ROOT = (
    PROJECT_ROOT
    / "strategy_tournament/v2/sp-mid-small-index-addition-capacity"
)
SOURCE_COLLECTION_PATH = (
    PROJECT_ROOT
    / "strategy_tournament/v2/sp500-index-addition-capacity/page-collection/"
    "sp500-addition-page-collection-"
    "bf81bbd364497d9dca6df9b2fdbbac2cad41424849b557139d4f29795202817f.json"
)
SOURCE_COLLECTION_SHA256 = (
    "bf81bbd364497d9dca6df9b2fdbbac2cad41424849b557139d4f29795202817f"
)
SOURCE_INSPECTION_PATH = (
    PROJECT_ROOT
    / "strategy_tournament/v2/sp500-index-addition-capacity/"
    "capacity-inspection/sp500-addition-capacity-inspection-"
    "39c62f408f2dccd31435d579d55c971b4b907ea0b14e1c7fd20dc7c84f215e22.json"
)
SOURCE_INSPECTION_SHA256 = (
    "39c62f408f2dccd31435d579d55c971b4b907ea0b14e1c7fd20dc7c84f215e22"
)
TARGET_INDICES = ("S&P MidCap 400", "S&P SmallCap 600")
COMPOSITE_INDICES = ("S&P 500", *TARGET_INDICES)
DEVELOPMENT_END = "2018-12-31"
CONFIRMATION_START = "2019-01-01"
CONFIRMATION_END = "2025-12-31"
MINIMUM_TOTAL_SIGNAL_DATES = 50
FAST_LANE_TOTAL_SIGNAL_DATES = 100
MINIMUM_DEVELOPMENT_SIGNAL_DATES = 30
MINIMUM_CONFIRMATION_SIGNAL_DATES = 20
IMPLEMENTATION_FILES = (
    "sp_mid_small_addition_capacity.py",
    "sp_mid_small_addition_capacity_inspection.py",
    "sp500_addition_capacity.py",
    "outcome_exposure.py",
    "strategy_discovery.py",
    "portfolio_maturity.py",
    "portfolio_config.toml",
    "PORTFOLIO_VALIDATION_V2.md",
)


class SpMidSmallAdditionCapacityError(RuntimeError):
    """The frozen source graph or event denominator did not rebuild."""


def _canonical(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode()


def _self_hash(
    value: Mapping[str, Any], field: str = "artifact_sha256"
) -> str:
    payload = dict(value)
    payload.pop(field, None)
    return hashlib.sha256(_canonical(payload)).hexdigest()


def _implementation_hashes() -> dict[str, str]:
    return {
        relative: sha256_file(PROJECT_ROOT / relative)
        for relative in IMPLEMENTATION_FILES
    }


def _load(path: Path, kind: str) -> dict[str, Any]:
    artifact = strategy_discovery.load_artifact(path, expected_kind=kind)
    if artifact.get("artifact_sha256") != source._self_hash(artifact):
        raise SpMidSmallAdditionCapacityError(
            f"{path}: artifact hash drifted"
        )
    return artifact


def _source_collection() -> dict[str, Any]:
    collection = _load(
        SOURCE_COLLECTION_PATH,
        "sp500-addition-release-page-collection",
    )
    inspection = _load(
        SOURCE_INSPECTION_PATH,
        "sp500-addition-capacity-inspection",
    )
    if not (
        collection.get("artifact_sha256") == SOURCE_COLLECTION_SHA256
        and collection.get("state") == "RELEASE_PAGES_COLLECTED_UNINSPECTED"
        and collection.get("task_count") == 312
        and collection.get("market_price_requests") == 0
        and collection.get("market_outcomes_accessed") is False
        and collection.get("broker_actions") == 0
        and inspection.get("artifact_sha256") == SOURCE_INSPECTION_SHA256
        and inspection.get("collection_sha256") == SOURCE_COLLECTION_SHA256
        and inspection.get("state") == "CAPACITY_READY_FAST_LANE"
        and inspection.get("release_count") == 312
        and inspection.get("inspection", {}).get("valid") is True
        and inspection.get("market_outcomes_accessed") is False
        and inspection.get("broker_actions_permitted") is False
    ):
        raise SpMidSmallAdditionCapacityError(
            "committed official-page source lineage is not reusable"
        )
    return collection


def source_tasks() -> list[dict[str, Any]]:
    """Return the exact previously inspected cache identities."""

    collection = _source_collection()
    return [
        {
            "ordinal": task["ordinal"],
            "listed_date": task["listed_date"],
            "title": task["title"],
            "url": task["url"],
            "url_sha256": task["url_sha256"],
            "task_id": task["task_id"],
            "cache_relative_path": task["cache_relative_path"],
            "raw_sha256": task["raw_sha256"],
            "raw_bytes": task["raw_bytes"],
        }
        for task in collection["tasks"]
    ]


def freeze_contract(
    *,
    created_at: str,
    root: Path = PUBLIC_ROOT,
    enforce_commit: bool = True,
    recovery_inspection_path: Path | None = None,
) -> tuple[Path, dict[str, Any]]:
    """Freeze the no-request source-reuse graph before parsing new rows."""

    source._timestamp(created_at)
    existing_contracts = sorted(
        (root / "source-reuse-contract").glob(
            "sp-mid-small-addition-contract-*.json"
        )
    )
    if existing_contracts and recovery_inspection_path is None:
        raise SpMidSmallAdditionCapacityError(
            "initial source-reuse contract already exists; "
            "an inspected source-schema failure is required"
        )
    if enforce_commit:
        for path in (
            SOURCE_COLLECTION_PATH,
            SOURCE_INSPECTION_PATH,
            source.ROLLING_AUTHORIZATION_PATH,
            source.ROLLING_STATUS_PATH,
            *[PROJECT_ROOT / relative for relative in IMPLEMENTATION_FILES],
        ):
            strategy_discovery.require_committed(path)
        if recovery_inspection_path is not None:
            strategy_discovery.require_committed(recovery_inspection_path)
    source._validate_rolling_authority(enforce_commit=enforce_commit)
    recovery: dict[str, Any] | None = None
    if recovery_inspection_path is not None:
        recovery = _load(
            recovery_inspection_path,
            "sp-mid-small-addition-capacity-failure-inspection",
        )
        if not (
            recovery.get("state") == "CAPACITY_PARSE_FAILURE_INSPECTED"
            and recovery.get("failed_contract_sha256")
            == "64d010d8f9cedfd93293796b8c2b475a27732fadb0288f7a12e53d65c18546a0"
            and recovery.get("failed_task_ordinal") == 179
            and recovery.get("failed_listed_date") == "2018-11-26"
            and recovery.get("failure_type")
            == "Sp500AdditionCapacityError"
            and recovery.get("failure_message")
            == "effective date is unparseable: DECMEBER 3, 2018"
            and recovery.get("recovery_permitted") is True
            and recovery.get("provider_requests") == 0
            and recovery.get("market_outcomes_accessed") is False
            and recovery.get("broker_actions_permitted") is False
        ):
            raise SpMidSmallAdditionCapacityError(
                "source-schema recovery inspection is not exact and ready"
            )
    tasks = source_tasks()
    exposure_audit = outcome_exposure.audit()
    contract: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": "sp-mid-small-external-addition-capacity-contract",
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "successor_id": SUCCESSOR_ID,
        "mechanism_family": MECHANISM_FAMILY,
        "research_generation": "new_mechanism_family",
        "new_mechanism_family_slot_consumed": True,
        "created_at": created_at,
        "state": "SOURCE_REUSE_CONTRACT_FROZEN",
        "mechanism": (
            "External additions to the S&P MidCap 400 or SmallCap 600 "
            "create index-tracker buying without a same-security offsetting "
            "deletion from another S&P Composite 1500 size index."
        ),
        "source": {
            "provider": "S&P Global official press archive",
            "source_collection_path": source._repo_path(
                SOURCE_COLLECTION_PATH
            ),
            "source_collection_sha256": SOURCE_COLLECTION_SHA256,
            "source_inspection_path": source._repo_path(
                SOURCE_INSPECTION_PATH
            ),
            "source_inspection_sha256": SOURCE_INSPECTION_SHA256,
            "reuse_only": True,
            "new_provider_requests_permitted": False,
        },
        "tasks": tasks,
        "task_count": len(tasks),
        "event_semantics": {
            "eligible_indices": list(TARGET_INDICES),
            "eligible_action": "Addition",
            "same_ticker_effective_date_deletion_from_indices": list(
                COMPOSITE_INDICES
            ),
            "same_ticker_cross_index_transfer": (
                "ineligible_preserve_denominator"
            ),
            "publication_timestamp_source": "HTML ITEMDATE comment",
            "effective_date_source": (
                "structured row or same-index legacy table heading"
            ),
            "ticker_source": (
                "structured ticker cell or nearest official inline "
                "exchange-ticker identity"
            ),
            "duplicate_identity": (
                "ticker plus effective date plus destination index"
            ),
            "duplicate_resolution": (
                "earliest publication timestamp then lexical source URL"
            ),
            "candidate_entry": (
                "first complete regular session after publication"
            ),
            "maximum_one_new_family_entry_per_announcement_date": True,
        },
        "capacity_gate": {
            "minimum_total_signal_dates": MINIMUM_TOTAL_SIGNAL_DATES,
            "fast_lane_total_signal_dates": FAST_LANE_TOTAL_SIGNAL_DATES,
            "minimum_development_signal_dates": (
                MINIMUM_DEVELOPMENT_SIGNAL_DATES
            ),
            "minimum_confirmation_signal_dates": (
                MINIMUM_CONFIRMATION_SIGNAL_DATES
            ),
        },
        "partition": {
            "development_end": DEVELOPMENT_END,
            "confirmation_start": CONFIRMATION_START,
            "confirmation_end": CONFIRMATION_END,
            "exact_pair_and_warmup_exposure_check_pending_family_freeze": True,
        },
        "outcome_exposure_index_sha256": exposure_audit["index_sha256"],
        "implementation_hashes": _implementation_hashes(),
        "provider_requests_before_contract_freeze": 0,
        "cache_rows_accessed_before_contract_freeze": 0,
        "market_price_access_permitted": False,
        "target_return_access_permitted": False,
        "confirmation_access_permitted": False,
        "broker_actions_permitted": False,
        "market_outcomes_accessed": False,
    }
    if recovery is not None:
        contract["source_schema_recovery"] = {
            "failure_inspection_path": source._repo_path(
                recovery_inspection_path
            ),
            "failure_inspection_sha256": recovery["artifact_sha256"],
            "failed_contract_sha256": recovery[
                "failed_contract_sha256"
            ],
            "prior_cache_pages_opened": recovery["cache_pages_opened"],
            "failed_task_ordinal": recovery["failed_task_ordinal"],
            "failed_source_url": recovery["failed_source_url"],
            "normalization": {
                "exact_source_token": "DECMEBER",
                "canonical_token": "DECEMBER",
                "field": "legacy same-index effective-date heading",
                "maximum_replacements_per_page": 1,
            },
            "all_tasks_dates_urls_and_bytes_unchanged": True,
            "new_provider_requests_permitted": False,
            "market_outcomes_accessed": False,
        }
    contract["artifact_sha256"] = _self_hash(contract)
    path = (
        root
        / "source-reuse-contract"
        / f"sp-mid-small-addition-contract-{contract['artifact_sha256']}.json"
    )
    source._write(path, contract)
    return path, contract


def _normalize_index(raw: str) -> str | None:
    value = re.sub(r"\s+", " ", raw.replace("®", "")).strip()
    aliases = {
        "S&P 500": "S&P 500",
        "S&P 500 INDEX": "S&P 500",
        "S&P MIDCAP 400": "S&P MidCap 400",
        "S&P MIDCAP 400 INDEX": "S&P MidCap 400",
        "S&P SMALLCAP 600": "S&P SmallCap 600",
        "S&P SMALLCAP 600 INDEX": "S&P SmallCap 600",
    }
    return aliases.get(value.upper())


def _event(
    *,
    published: Any,
    effective: date,
    index_name: str,
    action: str,
    company_name: str,
    ticker: str,
    source_url: str,
) -> dict[str, Any]:
    return {
        "announcement_at": published.isoformat(),
        "announcement_date": published.date().isoformat(),
        "effective_date": effective.isoformat(),
        "index_name": index_name,
        "action": action,
        "company_name": company_name,
        "ticker": ticker,
        "source_url": source_url,
    }


def _effective_date(
    raw: str,
    *,
    announcement: date,
    normalize_known_official_typo: bool,
) -> date:
    value = raw
    if normalize_known_official_typo:
        if value.count("DECMEBER") > 1:
            raise SpMidSmallAdditionCapacityError(
                "official month typo occurs more than once"
            )
        value = value.replace("DECMEBER", "DECEMBER")
    return source._parse_effective_date(value, announcement=announcement)


def parse_release(
    raw: bytes,
    *,
    source_url: str,
    listed_date: str,
    normalize_known_official_typo: bool = True,
) -> dict[str, Any]:
    """Parse all Composite 1500 size-index actions and retain pure additions."""

    parser = source._ReleaseParser()
    parser.feed(raw.decode("utf-8", errors="strict"))
    parser.close()
    if parser.itemdate is None:
        return {
            "source_url": source_url,
            "listed_date": listed_date,
            "eligible_events": [],
            "all_index_actions": [],
            "ineligible_rows": [],
            "terminal_reason": "MISSING_PUBLICATION_TIMESTAMP",
        }
    published = source._parse_itemdate(parser.itemdate)
    if published.date().isoformat() != listed_date:
        raise SpMidSmallAdditionCapacityError(
            f"{source_url}: listing date and ITEMDATE differ"
        )
    visible_text = re.sub(
        r"\s+", " ", " ".join(parser.visible_chunks)
    ).strip()
    actions: list[dict[str, Any]] = []
    ineligible: list[dict[str, str]] = []
    for table in parser.tables:
        inherited_effective_date: str | None = None
        for row in table:
            normalized = [
                re.sub(
                    r"\s+",
                    " ",
                    value.replace("®", "").replace("\xa0", " "),
                ).strip()
                for value in row
            ]
            if len(normalized) < 5:
                continue
            if normalized[0]:
                inherited_effective_date = normalized[0]
            for offset in range(0, len(normalized) - 4):
                segment = normalized[offset : offset + 6]
                index_name = _normalize_index(segment[1])
                action = segment[2].casefold()
                if index_name is None or action not in {"addition", "deletion"}:
                    continue
                effective_cell = (
                    segment[0] or inherited_effective_date or ""
                )
                ticker = segment[4].upper().replace(" ", "")
                if not re.fullmatch(r"[A-Z][A-Z0-9.-]{0,11}", ticker):
                    raise SpMidSmallAdditionCapacityError(
                        f"{source_url}: invalid structured ticker {ticker}"
                    )
                if effective_cell.casefold() in {
                    "",
                    "tba",
                    "to be announced",
                }:
                    ineligible.append(
                        {
                            "ticker": ticker,
                            "index_name": index_name,
                            "action": action.title(),
                            "terminal_reason": "MISSING_EFFECTIVE_DATE",
                        }
                    )
                    continue
                effective = _effective_date(
                    effective_cell,
                    announcement=published.date(),
                    normalize_known_official_typo=(
                        normalize_known_official_typo
                    ),
                )
                if effective <= published.date():
                    raise SpMidSmallAdditionCapacityError(
                        f"{source_url}: effective date is not later"
                    )
                actions.append(
                    _event(
                        published=published,
                        effective=effective,
                        index_name=index_name,
                        action=action.title(),
                        company_name=segment[3],
                        ticker=ticker,
                        source_url=source_url,
                    )
                )
        if not table or not table[0]:
            continue
        heading = re.fullmatch(
            r"(S&P (?:500|MIDCAP 400|SMALLCAP 600)(?: INDEX)?)"
            r"\s*[\-\u2013]\s*(.+)",
            re.sub(r"\s+", " ", table[0][0]).strip(),
            re.IGNORECASE,
        )
        if heading is None:
            continue
        index_name = _normalize_index(heading.group(1))
        if index_name is None:
            continue
        raw_effective = heading.group(2).strip()
        effective = (
            None
            if raw_effective.casefold() in {"tba", "to be announced"}
            else _effective_date(
                raw_effective,
                announcement=published.date(),
                normalize_known_official_typo=(
                    normalize_known_official_typo
                ),
            )
        )
        if effective is not None and effective <= published.date():
            raise SpMidSmallAdditionCapacityError(
                f"{source_url}: effective date is not later"
            )
        action: str | None = None
        for row in table[1:]:
            if len(row) < 2:
                continue
            row_action = row[0].strip().upper()
            if row_action in {"ADDED", "DELETED"}:
                action = "Addition" if row_action == "ADDED" else "Deletion"
            company = row[1].strip()
            if action is None or not company or company.casefold() == "company":
                continue
            ticker = source._legacy_company_ticker(company, visible_text)
            if ticker is None:
                ineligible.append(
                    {
                        "company_name": company,
                        "index_name": index_name,
                        "action": action,
                        "terminal_reason": "UNRESOLVED_TICKER_IDENTITY",
                    }
                )
                continue
            if effective is None:
                ineligible.append(
                    {
                        "ticker": ticker,
                        "index_name": index_name,
                        "action": action,
                        "terminal_reason": "MISSING_EFFECTIVE_DATE",
                    }
                )
                continue
            actions.append(
                _event(
                    published=published,
                    effective=effective,
                    index_name=index_name,
                    action=action,
                    company_name=company,
                    ticker=ticker,
                    source_url=source_url,
                )
            )
    action_identity = {
        (
            row["ticker"],
            row["effective_date"],
            row["index_name"],
            row["action"],
        ): row
        for row in actions
    }
    all_actions = [action_identity[key] for key in sorted(action_identity)]
    deletions = {
        (row["ticker"], row["effective_date"])
        for row in all_actions
        if row["action"] == "Deletion"
        and row["index_name"] in COMPOSITE_INDICES
    }
    eligible: list[dict[str, Any]] = []
    transfer_count = 0
    for row in all_actions:
        if row["action"] != "Addition" or row["index_name"] not in TARGET_INDICES:
            continue
        if (row["ticker"], row["effective_date"]) in deletions:
            transfer_count += 1
            ineligible.append(
                {
                    "ticker": row["ticker"],
                    "index_name": row["index_name"],
                    "action": row["action"],
                    "terminal_reason": "CROSS_INDEX_TRANSFER_EXCLUDED",
                }
            )
            continue
        eligible.append(row)
    eligible.sort(
        key=lambda row: (
            row["announcement_at"],
            row["ticker"],
            row["effective_date"],
            row["index_name"],
        )
    )
    terminal_reason = (
        "ELIGIBLE_EXTERNAL_MID_SMALL_ADDITION"
        if eligible
        else (
            "CROSS_INDEX_TRANSFERS_ONLY"
            if transfer_count
            else "NO_ELIGIBLE_EXTERNAL_MID_SMALL_ADDITION"
        )
    )
    return {
        "source_url": source_url,
        "listed_date": listed_date,
        "publication_timestamp": published.isoformat(),
        "eligible_events": eligible,
        "all_index_actions": all_actions,
        "ineligible_rows": ineligible,
        "terminal_reason": terminal_reason,
    }


def capacity_state(
    *,
    total_dates: int,
    development_dates: int,
    confirmation_dates: int,
) -> str:
    if (
        total_dates >= FAST_LANE_TOTAL_SIGNAL_DATES
        and development_dates >= MINIMUM_DEVELOPMENT_SIGNAL_DATES
        and confirmation_dates >= MINIMUM_CONFIRMATION_SIGNAL_DATES
    ):
        return "CAPACITY_READY_FAST_LANE"
    if (
        total_dates >= MINIMUM_TOTAL_SIGNAL_DATES
        and development_dates >= MINIMUM_DEVELOPMENT_SIGNAL_DATES
        and confirmation_dates >= MINIMUM_CONFIRMATION_SIGNAL_DATES
    ):
        return "CAPACITY_READY_LATER_SINGLE_RULE"
    return "RETIRED_INSUFFICIENT_FORMAL_CAPACITY"


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    freeze = sub.add_parser("freeze-contract")
    freeze.add_argument("--created-at", required=True)
    recovery = sub.add_parser("freeze-recovery")
    recovery.add_argument("failure_inspection", type=Path)
    recovery.add_argument("--created-at", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        path, artifact = freeze_contract(
            created_at=args.created_at,
            recovery_inspection_path=(
                args.failure_inspection
                if args.command == "freeze-recovery"
                else None
            ),
        )
    except (
        SpMidSmallAdditionCapacityError,
        strategy_discovery.StrategyDiscoveryError,
        outcome_exposure.OutcomeExposureError,
        OSError,
        ValueError,
    ) as exc:
        print(
            json.dumps(
                {"error": str(exc), "error_type": type(exc).__name__},
                indent=2,
                sort_keys=True,
            )
        )
        return 1
    print(
        json.dumps(
            {
                "artifact_sha256": artifact["artifact_sha256"],
                "state": artifact["state"],
                "written": source._repo_path(path),
                "provider_requests": 0,
                "market_outcomes_accessed": False,
                "broker_actions_permitted": False,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""Build an event-first SEC earnings inventory before market-data access.

The official SEC bulk submissions archive is intersected with the complete
point-in-time common-stock security master used by the inspected scanner.  The
result is a preregistered 2023 development and 2024 confirmation event graph.
No market price, forward return, filing document, or broker interface is used.
"""

from __future__ import annotations

import argparse
import bisect
import hashlib
import json
import os
import time
import zipfile
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from datetime import datetime
from pathlib import Path, PurePosixPath
from typing import Any
from zoneinfo import ZoneInfo

import requests

import outcome_exposure
import sec_earnings_gap_capacity as narrow
import strategy_discovery
from gap_protection_successor import CALENDAR_PATH, PARAMETER_GRID
from historical_discovery import SecConfig, _accepted_at, _filing_items
from historical_store import HistoricalDayStore, sha256_file
from learning_data import security_master_sha256
from scanner_replay import load_calendar


PROJECT_ROOT = Path(__file__).resolve().parent
CAMPAIGN_ID = narrow.CAMPAIGN_ID
FAMILY_ID = "sec-filed-earnings-gap-continuation-event-first"
DATASET_ID = "dataset-sec-filed-earnings-event-first-2023-2024-v1"
DEFAULT_ROOT = (
    PROJECT_ROOT
    / "strategy_tournament/v2/continuous"
    / FAMILY_ID
    / DATASET_ID
)
PREDECESSOR_INSPECTION = (
    PROJECT_ROOT
    / "strategy_tournament/v2/continuous/"
    "sec-filed-earnings-gap-continuation-search/"
    "dataset-sec-filed-earnings-gap-continuation-2026-07-26-v1/"
    "capacity-inspection/"
    "inspection-"
    "2d2d02922e0a55c74bf373fa36c4d9d9a19faa54e3ea9867574d8e67c872de0b"
    ".json"
)
SECURITY_MASTER = narrow.SECURITY_MASTER
SECURITY_MASTER_ATTESTATION = narrow.SECURITY_MASTER_ATTESTATION
BULK_URL = (
    "https://www.sec.gov/Archives/edgar/daily-index/"
    "bulkdata/submissions.zip"
)
SEC_API_DOCUMENTATION = (
    "https://www.sec.gov/search-filings/"
    "edgar-application-programming-interfaces"
)
EASTERN = ZoneInfo("America/New_York")
DEVELOPMENT_START = "2023-01-03"
DEVELOPMENT_END = "2023-12-29"
EMBARGO_DATES = ["2024-01-02", "2024-01-03", "2024-01-04", "2024-01-05", "2024-01-08"]
CONFIRMATION_START = "2024-01-09"
CONFIRMATION_END = "2024-12-31"
SIGNAL_CUTOFF = "09:25:00"
MINIMUM_TOTAL_SIGNAL_DAYS = 100
MINIMUM_DEVELOPMENT_SIGNAL_DAYS = 50
MINIMUM_CONFIRMATION_SIGNAL_DAYS = 30
PRIOR_SELECTION_TRIALS = 96
FUTURE_TRIALS = 32


class SecEarningsFullInventoryError(RuntimeError):
    """The event-first SEC inventory is invalid or incomplete."""


def _timestamp(value: str, field: str) -> str:
    return narrow._timestamp(value, field)


def _repo_path(path: Path) -> str:
    return narrow._repo_path(path)


def _read_json(path: Path) -> dict[str, Any]:
    return narrow._read_json(path)


def _write_json(path: Path, value: Mapping[str, Any]) -> None:
    narrow._write_json(path, value)


def _write_gzip(path: Path, value: Any) -> None:
    narrow._write_gzip(path, value)


def _read_gzip(path: Path) -> dict[str, Any]:
    return narrow._read_gzip(path)


def self_hash(value: Mapping[str, Any], field: str) -> str:
    return narrow.self_hash(value, field)


def content_hash(value: Any) -> str:
    return narrow.content_hash(value)


def _private_root(store: HistoricalDayStore) -> Path:
    return store.root / "_derived" / "sec_earnings_full_inventory" / DATASET_ID


def _archive_path(store: HistoricalDayStore) -> Path:
    return _private_root(store) / "submissions.zip"


def _private_inventory_path(store: HistoricalDayStore) -> Path:
    return _private_root(store) / "event-inventory.json.gz"


def _predecessor() -> dict[str, Any]:
    value = _read_json(PREDECESSOR_INSPECTION)
    if not (
        value.get("inspection_sha256")
        == self_hash(value, "inspection_sha256")
        and value.get("state") == "RETIRED_INSUFFICIENT_CAPACITY"
        and value.get("capacity_adequate") is False
        and value.get("development_outcome_access_permitted") is False
        and value.get("valid") is True
    ):
        raise SecEarningsFullInventoryError(
            "narrow predecessor retirement is invalid"
        )
    return {
        "inspection_path": _repo_path(PREDECESSOR_INSPECTION),
        "inspection_file_sha256": sha256_file(PREDECESSOR_INSPECTION),
        "inspection_sha256": value["inspection_sha256"],
        "state": value["state"],
        "predecessor_outcomes_accessed": False,
        "predecessor_repair_permitted": False,
        "successor_change": (
            "replace the sparse scanner-date denominator with the complete "
            "SEC-event denominator for the source security-master universe"
        ),
    }


def _master_attestation() -> dict[str, Any]:
    value = _read_json(SECURITY_MASTER_ATTESTATION)
    semantic = security_master_sha256(SECURITY_MASTER)
    if value.get("security_master", {}).get("sha256") != semantic:
        raise SecEarningsFullInventoryError(
            "security master differs from committed attestation"
        )
    return {
        "path": _repo_path(SECURITY_MASTER),
        "file_sha256": sha256_file(SECURITY_MASTER),
        "semantic_sha256": semantic,
        "attestation_path": _repo_path(SECURITY_MASTER_ATTESTATION),
        "attestation_file_sha256": sha256_file(SECURITY_MASTER_ATTESTATION),
        "records": value["security_master"]["records"],
        "instruments": value["security_master"]["instruments"],
        "requested_dates": len(value["requested_dates"]),
        "first_requested_date": value["requested_dates"][0],
        "last_requested_date": value["requested_dates"][-1],
    }


def build_contract(
    *,
    created_at: str,
    enforce_committed: bool = True,
) -> dict[str, Any]:
    inputs = (
        Path(__file__).resolve(),
        PROJECT_ROOT / "sec_earnings_full_inventory_inspection.py",
        PREDECESSOR_INSPECTION,
        SECURITY_MASTER_ATTESTATION,
        CALENDAR_PATH,
        outcome_exposure.DEFAULT_INDEX,
    )
    if enforce_committed:
        for path in inputs:
            strategy_discovery.require_committed(path)
    value: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": "sec-earnings-full-inventory-contract",
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "dataset_id": DATASET_ID,
        "state": "EVENT_FIRST_CAPACITY_CONTRACT_FROZEN",
        "created_at": _timestamp(created_at, "created_at"),
        "predecessor": _predecessor(),
        "source_security_master": _master_attestation(),
        "calendar": {
            "path": _repo_path(CALENDAR_PATH),
            "sha256": sha256_file(CALENDAR_PATH),
        },
        "provider_request": {
            "ordinal": 0,
            "method": "GET",
            "url": BULK_URL,
            "documentation": SEC_API_DOCUMENTATION,
            "archive_semantics": (
                "official nightly public EDGAR filing history for all filers"
            ),
            "timeout_seconds": 900.0,
            "retries_permitted": 0,
            "substitutions_permitted": 0,
            "hash_valid_cache_resume_permitted": True,
        },
        "event_semantics": {
            "forms": ["8-K"],
            "required_items": ["2.02"],
            "excluded_items": ["3.02"],
            "amendments_permitted": False,
            "accession_required": True,
            "acceptance_timestamp_required": True,
            "primary_document_required": True,
            "filing_documents_accessed": False,
            "signal_window_start": (
                "previous_regular_session_16:00:00_ET_exclusive"
            ),
            "signal_window_end": (
                f"signal_regular_session_{SIGNAL_CUTOFF}_ET_inclusive"
            ),
            "duplicate_accession_receives_zero_credit": True,
        },
        "point_in_time_universe": {
            "security_type": "COMMON",
            "identity_source": "Massive composite FIGI security master",
            "cik_must_match": True,
            "signal_date_must_be_within_valid_from_and_valid_to": True,
            "fallback_identity_permitted": False,
            "multiple_valid_common_share_classes": (
                "retain each distinct composite FIGI listing"
            ),
            "current_ticker_fallback_permitted": False,
        },
        "partitions": {
            "development": [DEVELOPMENT_START, DEVELOPMENT_END],
            "development_may_be_explicitly_contaminated_training": True,
            "embargo_dates": EMBARGO_DATES,
            "confirmation": [CONFIRMATION_START, CONFIRMATION_END],
            "confirmation_pair_overlap_with_global_index": "zero_credit",
            "outcome_exposure_index_sha256": outcome_exposure.audit()[
                "index_sha256"
            ],
        },
        "capacity_thresholds": {
            "signal_unit": "distinct signal date",
            "minimum_total_signal_days": MINIMUM_TOTAL_SIGNAL_DAYS,
            "minimum_development_signal_days": MINIMUM_DEVELOPMENT_SIGNAL_DAYS,
            "minimum_confirmation_signal_days": MINIMUM_CONFIRMATION_SIGNAL_DAYS,
        },
        "future_search": {
            "selection_mode": "development_search",
            "parameter_grid": PARAMETER_GRID,
            "current_trial_count": FUTURE_TRIALS,
            "prior_selection_trials": PRIOR_SELECTION_TRIALS,
            "cumulative_selection_trial_count": (
                PRIOR_SELECTION_TRIALS + FUTURE_TRIALS
            ),
            "maximum_hold_sessions": 1,
            "confirmation_cannot_influence_selection": True,
        },
        "implementation_hashes": {
            "sec_earnings_full_inventory.py": sha256_file(
                Path(__file__).resolve()
            ),
            "sec_earnings_full_inventory_inspection.py": sha256_file(
                PROJECT_ROOT / "sec_earnings_full_inventory_inspection.py"
            ),
            "outcome_exposure.py": sha256_file(
                PROJECT_ROOT / "outcome_exposure.py"
            ),
        },
        "provider_requests_executed": 0,
        "market_prices_accessed": False,
        "forward_returns_accessed": False,
        "strategy_metrics_computed": 0,
        "confirmation_outcomes_accessed": False,
        "broker_actions": 0,
    }
    value["contract_sha256"] = self_hash(value, "contract_sha256")
    return value


def freeze_contract(
    *,
    created_at: str,
    root: Path = DEFAULT_ROOT,
) -> tuple[Path, dict[str, Any]]:
    value = build_contract(created_at=created_at)
    path = (
        root
        / "capacity-contract"
        / f"contract-{value['contract_sha256']}.json"
    )
    _write_json(path, value)
    return path, value


def _load_contract(path: Path) -> dict[str, Any]:
    value = _read_json(path)
    if value.get("contract_sha256") != self_hash(value, "contract_sha256"):
        raise SecEarningsFullInventoryError("contract hash is invalid")
    return value


def _load_contract_inspection(
    path: Path, contract: Mapping[str, Any]
) -> dict[str, Any]:
    value = _read_json(path)
    if not (
        value.get("inspection_sha256") == self_hash(value, "inspection_sha256")
        and value.get("state") == "EVENT_FIRST_CONTRACT_INSPECTED_READY"
        and value.get("contract_sha256") == contract["contract_sha256"]
        and value.get("provider_access_authorized") is True
        and value.get("valid") is True
    ):
        raise SecEarningsFullInventoryError(
            "contract inspection is invalid"
        )
    return value


def _validate_archive(path: Path) -> dict[str, Any]:
    try:
        with zipfile.ZipFile(path) as archive:
            infos = archive.infolist()
            if len(infos) < 10_000:
                raise SecEarningsFullInventoryError(
                    "SEC bulk archive has too few members"
                )
            primary = 0
            for info in infos:
                pure = PurePosixPath(info.filename)
                if pure.is_absolute() or ".." in pure.parts:
                    raise SecEarningsFullInventoryError(
                        "SEC bulk archive contains an unsafe member"
                    )
                name = pure.name
                if (
                    name.startswith("CIK")
                    and name.endswith(".json")
                    and "-submissions-" not in name
                ):
                    primary += 1
            if primary < 5_000:
                raise SecEarningsFullInventoryError(
                    "SEC bulk archive has too few primary CIK files"
                )
    except (OSError, zipfile.BadZipFile) as exc:
        raise SecEarningsFullInventoryError(
            f"SEC bulk archive is unreadable: {exc}"
        ) from exc
    return {
        "members": len(infos),
        "primary_cik_files": primary,
        "bytes": path.stat().st_size,
        "sha256": sha256_file(path),
    }


def _download_archive(
    *,
    path: Path,
    contract: Mapping[str, Any],
    env_path: Path,
    session: requests.Session | None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    if path.is_file():
        return _validate_archive(path), {
            "requests": 0,
            "cache_hits": 1,
            "failures": 0,
            "request_seconds": 0.0,
            "download_bytes": 0,
        }
    config = SecConfig.from_env(env_path, path.parent, workers=1)
    own = session is None
    http = session or requests.Session()
    http.headers.update(
        {"User-Agent": config.user_agent, "Accept-Encoding": "gzip, deflate"}
    )
    temporary = path.with_suffix(path.suffix + ".tmp")
    started = time.monotonic()
    size = 0
    digest = hashlib.sha256()
    try:
        response = http.get(
            str(contract["provider_request"]["url"]),
            stream=True,
            timeout=float(contract["provider_request"]["timeout_seconds"]),
        )
        if response.status_code >= 400:
            raise SecEarningsFullInventoryError(
                f"SEC bulk request returned HTTP {response.status_code}"
            )
        path.parent.mkdir(parents=True, exist_ok=True)
        with temporary.open("wb") as target:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if not chunk:
                    continue
                target.write(chunk)
                digest.update(chunk)
                size += len(chunk)
            target.flush()
            os.fsync(target.fileno())
        temporary.replace(path)
    except requests.RequestException as exc:
        raise SecEarningsFullInventoryError(
            f"SEC bulk request failed: {exc}"
        ) from exc
    finally:
        temporary.unlink(missing_ok=True)
        if own:
            http.close()
    info = _validate_archive(path)
    if info["sha256"] != digest.hexdigest() or info["bytes"] != size:
        raise SecEarningsFullInventoryError(
            "downloaded SEC bulk archive hash accounting failed"
        )
    return info, {
        "requests": 1,
        "cache_hits": 0,
        "failures": 0,
        "request_seconds": time.monotonic() - started,
        "download_bytes": size,
    }


def _member_index(archive: zipfile.ZipFile) -> dict[str, str]:
    by_name: dict[str, list[str]] = defaultdict(list)
    for name in archive.namelist():
        by_name[PurePosixPath(name).name].append(name)
    ambiguous = [name for name, values in by_name.items() if len(values) != 1]
    if ambiguous:
        raise SecEarningsFullInventoryError(
            "SEC bulk archive contains ambiguous basenames"
        )
    return {name: values[0] for name, values in by_name.items()}


def _read_member(
    archive: zipfile.ZipFile,
    member: str,
) -> tuple[Mapping[str, Any], str, int]:
    try:
        raw = archive.read(member)
        value = json.loads(raw)
    except (KeyError, OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise SecEarningsFullInventoryError(
            f"cannot read SEC archive member {member}: {exc}"
        ) from exc
    if not isinstance(value, Mapping):
        raise SecEarningsFullInventoryError(
            f"SEC archive member is not an object: {member}"
        )
    return value, hashlib.sha256(raw).hexdigest(), len(raw)


def _security_by_cik(
    rows: Sequence[Mapping[str, Any]],
) -> dict[str, list[Mapping[str, Any]]]:
    result: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        source = row.get("source")
        cik = (
            str(source.get("cik") or "").zfill(10)
            if isinstance(source, Mapping)
            else ""
        )
        if (
            cik
            and row.get("security_type") == "COMMON"
            and str(row.get("instrument_id") or "").startswith(
                "FIGI-COMPOSITE:"
            )
        ):
            result[cik].append(row)
    return dict(result)


def _listings(
    rows: Sequence[Mapping[str, Any]],
    signal_date: str,
) -> list[dict[str, str]]:
    listings: dict[tuple[str, str, str], dict[str, str]] = {}
    for row in rows:
        if not (
            str(row.get("valid_from") or "") <= signal_date
            <= str(row.get("valid_to") or "")
        ):
            continue
        symbol = str(row.get("symbol") or "").strip().upper()
        instrument_id = str(row.get("instrument_id") or "")
        exchange = str(row.get("primary_exchange") or "")
        if not symbol or not instrument_id or not exchange:
            continue
        listings[(instrument_id, symbol, exchange)] = {
            "instrument_id": instrument_id,
            "symbol": symbol,
            "primary_exchange": exchange,
        }
    return [listings[key] for key in sorted(listings)]


def _signal_windows(
    calendar: Sequence[str],
) -> tuple[list[datetime], list[tuple[str, datetime, datetime, str]]]:
    ordered = sorted(set(calendar))
    windows: list[tuple[str, datetime, datetime, str]] = []
    for index, day in enumerate(ordered):
        if index == 0 or not DEVELOPMENT_START <= day <= CONFIRMATION_END:
            continue
        if day <= DEVELOPMENT_END:
            partition = "development"
        elif day in EMBARGO_DATES:
            partition = "embargo"
        elif day >= CONFIRMATION_START:
            partition = "confirmation"
        else:
            continue
        start = datetime.fromisoformat(
            f"{ordered[index - 1]}T16:00:00"
        ).replace(tzinfo=EASTERN)
        end = datetime.fromisoformat(
            f"{day}T{SIGNAL_CUTOFF}"
        ).replace(tzinfo=EASTERN)
        windows.append((day, start, end, partition))
    return [window[2] for window in windows], windows


def signal_for_acceptance(
    accepted: datetime,
    *,
    ends: Sequence[datetime],
    windows: Sequence[tuple[str, datetime, datetime, str]],
) -> tuple[str, str] | None:
    index = bisect.bisect_left(ends, accepted)
    if index >= len(windows):
        return None
    day, start, end, partition = windows[index]
    if start < accepted <= end:
        return day, partition
    return None


def _submission_rows(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    return narrow.submission_rows(payload)


def _history_files(payload: Mapping[str, Any]) -> list[str]:
    return narrow.relevant_history_files(payload)


def _filing_events(
    rows: Sequence[Mapping[str, Any]],
    *,
    ends: Sequence[datetime],
    windows: Sequence[tuple[str, datetime, datetime, str]],
) -> list[dict[str, Any]]:
    candidates: list[dict[str, Any]] = []
    accession_counts: Counter[str] = Counter()
    for row in rows:
        accepted = _accepted_at(row.get("acceptanceDateTime"))
        items = _filing_items(row.get("items"))
        accession = str(row.get("accessionNumber") or "").strip()
        primary = str(row.get("primaryDocument") or "").strip()
        if not (
            row.get("form") == "8-K"
            and accepted is not None
            and "2.02" in items
            and "3.02" not in items
            and accession
            and primary
        ):
            continue
        signal = signal_for_acceptance(
            accepted,
            ends=ends,
            windows=windows,
        )
        if signal is None:
            continue
        day, partition = signal
        accession_counts[accession] += 1
        candidates.append(
            {
                "signal_date": day,
                "partition": partition,
                "accepted_at": accepted.isoformat(),
                "accession": accession,
                "items": items,
                "primary_document": primary,
            }
        )
    return sorted(
        (
            row
            for row in candidates
            if accession_counts[row["accession"]] == 1
        ),
        key=lambda row: (
            row["signal_date"],
            row["accepted_at"],
            row["accession"],
        ),
    )


def _pair_exposed(
    day: str,
    symbol: str,
    records: Sequence[Mapping[str, Any]],
) -> bool:
    return bool(
        outcome_exposure.find_overlaps(
            {"dates": [day], "symbols": [symbol]},
            records,
        )
    )


def derive_inventory(
    archive_path: Path,
    *,
    security_rows: Sequence[Mapping[str, Any]],
    calendar: Sequence[str],
    exposure_records: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    by_cik = _security_by_cik(security_rows)
    ends, windows = _signal_windows(calendar)
    events: list[dict[str, Any]] = []
    source_files: list[dict[str, Any]] = []
    reasons: Counter[str] = Counter()
    with zipfile.ZipFile(archive_path) as archive:
        members = _member_index(archive)
        for cik in sorted(by_cik):
            primary_name = f"CIK{cik}.json"
            member = members.get(primary_name)
            if member is None:
                reasons["MISSING_PRIMARY_CIK_FILE"] += 1
                continue
            primary, digest, size = _read_member(archive, member)
            rows = _submission_rows(primary)
            source_files.append(
                {
                    "cik": cik,
                    "name": primary_name,
                    "sha256": digest,
                    "bytes": size,
                }
            )
            missing_history = False
            for history_name in _history_files(primary):
                history_member = members.get(history_name)
                if history_member is None:
                    reasons["MISSING_REFERENCED_HISTORY_FILE"] += 1
                    missing_history = True
                    break
                history, history_digest, history_size = _read_member(
                    archive, history_member
                )
                rows.extend(narrow._columnar_rows(history))
                source_files.append(
                    {
                        "cik": cik,
                        "name": history_name,
                        "sha256": history_digest,
                        "bytes": history_size,
                    }
                )
            if missing_history:
                continue
            filings = _filing_events(rows, ends=ends, windows=windows)
            for filing in filings:
                listings = _listings(by_cik[cik], filing["signal_date"])
                if not listings:
                    reasons["NO_POINT_IN_TIME_COMMON_LISTING"] += 1
                    continue
                for listing in listings:
                    exposed = _pair_exposed(
                        filing["signal_date"],
                        listing["symbol"],
                        exposure_records,
                    )
                    if filing["partition"] == "confirmation" and exposed:
                        reasons["CONFIRMATION_PAIR_EXPOSED_ZERO_CREDIT"] += 1
                        continue
                    events.append(
                        {
                            **filing,
                            **listing,
                            "cik": cik,
                            "development_pair_previously_exposed": (
                                exposed
                                if filing["partition"] == "development"
                                else False
                            ),
                        }
                    )
                    reasons[f"{filing['partition'].upper()}_PAIR_RETAINED"] += 1
    events.sort(
        key=lambda row: (
            row["signal_date"],
            row["symbol"],
            row["accession"],
            row["instrument_id"],
        )
    )
    source_files.sort(key=lambda row: (row["cik"], row["name"]))
    signal_days = {
        partition: sorted(
            {
                row["signal_date"]
                for row in events
                if row["partition"] == partition
            }
        )
        for partition in ("development", "embargo", "confirmation")
    }
    value = {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "events": events,
        "signal_days": signal_days,
        "source_files": source_files,
        "reason_counts": dict(sorted(reasons.items())),
        "outcome_exposure_index_sha256": outcome_exposure.audit()[
            "index_sha256"
        ],
        "market_prices_accessed": False,
        "forward_returns_accessed": False,
        "confirmation_outcomes_accessed": False,
    }
    value["content_sha256"] = content_hash(value)
    return value


def collect(
    contract_path: Path,
    inspection_path: Path,
    *,
    collected_at: str,
    env_path: Path = PROJECT_ROOT / ".env",
    root: Path = DEFAULT_ROOT,
    store: HistoricalDayStore | None = None,
    session: requests.Session | None = None,
) -> tuple[Path, dict[str, Any]]:
    historical = store or HistoricalDayStore.from_env(env_path)
    for path in (contract_path, inspection_path):
        strategy_discovery.require_committed(path)
    contract = _load_contract(contract_path)
    inspection = _load_contract_inspection(inspection_path, contract)
    if contract["partitions"][
        "outcome_exposure_index_sha256"
    ] != outcome_exposure.audit()["index_sha256"]:
        raise SecEarningsFullInventoryError(
            "outcome-exposure index drifted after event-first freeze"
        )
    archive_path = _archive_path(historical)
    archive_info, telemetry = _download_archive(
        path=archive_path,
        contract=contract,
        env_path=env_path,
        session=session,
    )
    private = derive_inventory(
        archive_path,
        security_rows=narrow._security_rows(SECURITY_MASTER),
        calendar=load_calendar(CALENDAR_PATH),
        exposure_records=outcome_exposure.read_index(),
    )
    private["contract_sha256"] = contract["contract_sha256"]
    private["contract_inspection_sha256"] = inspection["inspection_sha256"]
    private["archive_sha256"] = archive_info["sha256"]
    private["content_sha256"] = content_hash(
        {
            key: item
            for key, item in private.items()
            if key != "content_sha256"
        }
    )
    private_path = _private_inventory_path(historical)
    _write_gzip(private_path, private)
    days = private["signal_days"]
    value: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": "sec-earnings-full-inventory-collection",
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "dataset_id": DATASET_ID,
        "state": "EVENT_FIRST_CAPACITY_COLLECTED_AWAITING_INSPECTION",
        "collected_at": _timestamp(collected_at, "collected_at"),
        "contract_path": _repo_path(contract_path),
        "contract_sha256": contract["contract_sha256"],
        "contract_inspection_path": _repo_path(inspection_path),
        "contract_inspection_sha256": inspection["inspection_sha256"],
        "archive": archive_info,
        "provider_telemetry": telemetry,
        "source_cik_count": len(_security_by_cik(narrow._security_rows())),
        "source_file_count": len(private["source_files"]),
        "retained_event_pairs": len(private["events"]),
        "development_signal_days": len(days["development"]),
        "embargo_signal_days": len(days["embargo"]),
        "confirmation_signal_days": len(days["confirmation"]),
        "total_creditable_signal_days": (
            len(days["development"]) + len(days["confirmation"])
        ),
        "reason_counts": private["reason_counts"],
        "private_inventory_content_sha256": private["content_sha256"],
        "private_inventory_file_sha256": sha256_file(private_path),
        "filing_documents_accessed": False,
        "market_prices_accessed": False,
        "forward_returns_accessed": False,
        "strategy_metrics_computed": 0,
        "confirmation_outcomes_accessed": False,
        "broker_actions": 0,
    }
    value["collection_sha256"] = self_hash(value, "collection_sha256")
    path = (
        root
        / "capacity-collection"
        / f"collection-{value['collection_sha256']}.json"
    )
    _write_json(path, value)
    return path, value


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    freeze = subparsers.add_parser("freeze-contract")
    freeze.add_argument("--created-at", required=True)
    collect_parser = subparsers.add_parser("collect")
    collect_parser.add_argument("contract", type=Path)
    collect_parser.add_argument("inspection", type=Path)
    collect_parser.add_argument("--collected-at", required=True)
    args = parser.parse_args(argv)
    try:
        if args.command == "freeze-contract":
            path, value = freeze_contract(created_at=args.created_at)
        else:
            path, value = collect(
                args.contract,
                args.inspection,
                collected_at=args.collected_at,
            )
        print(
            json.dumps(
                {
                    "path": _repo_path(path),
                    "state": value["state"],
                    "sha256": value.get("collection_sha256")
                    or value.get("contract_sha256"),
                    "market_prices_accessed": False,
                    "broker_actions": 0,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    except (
        SecEarningsFullInventoryError,
        narrow.SecEarningsGapCapacityError,
        strategy_discovery.StrategyDiscoveryError,
        OSError,
        KeyError,
        ValueError,
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


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())

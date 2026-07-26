"""Freeze and collect outcome-blind SEC earnings-gap capacity evidence.

This lane intersects the already-inspected gap-protection pre-entry inventory
with point-in-time SEC 8-K Item 2.02 filings.  It never reads post-entry prices
or confirmation outcomes.  A separate inspector must approve the contract
before provider access and independently rebuild the collected capacity.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import io
import json
from collections.abc import Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import strategy_discovery
from gap_protection_successor import (
    CALENDAR_PATH,
    PARAMETER_GRID,
    _inventory_path,
)
from historical_discovery import (
    SEC_SUBMISSIONS_ROOT,
    SecClient,
    SecConfig,
    _accepted_at,
    _filing_items,
)
from historical_store import HistoricalDayStore, sha256_file
from learning_data import security_master_sha256
from scanner_replay import load_calendar


PROJECT_ROOT = Path(__file__).resolve().parent
CAMPAIGN_ID = "multi-strategy-portfolio-validation-v2"
FAMILY_ID = "sec-filed-earnings-gap-continuation-search"
DATASET_ID = "dataset-sec-filed-earnings-gap-continuation-2026-07-26-v1"
DEFAULT_ROOT = (
    PROJECT_ROOT
    / "strategy_tournament/v2/continuous"
    / FAMILY_ID
    / DATASET_ID
)
SOURCE_PREENTRY_CONTRACT = (
    PROJECT_ROOT
    / "strategy_tournament/v2/gap_protection_successor/"
    "dataset-equity-gap-protection-continuation-2026-07-24-v2/"
    "preentry-contract/"
    "gap-protection-preentry-contract-"
    "b2c223bcfe81f92a96882b7974774d424b96f45c32bd89e0e02dae44305ad6d1.json"
)
SOURCE_PREENTRY_SUMMARY = (
    PROJECT_ROOT
    / "strategy_tournament/v2/gap_protection_successor/"
    "dataset-equity-gap-protection-continuation-2026-07-24-v2/"
    "preentry-summary/"
    "gap-protection-preentry-summary-"
    "f22b4392e65ea0c3983982481c9b08a59927180b83b51326f4cb5e14794c1218.json"
)
SOURCE_PREENTRY_INSPECTION = (
    PROJECT_ROOT
    / "strategy_tournament/v2/gap_protection_successor/"
    "dataset-equity-gap-protection-continuation-2026-07-24-v2/"
    "preentry-inspection/"
    "gap-protection-preentry-inspection-"
    "b82ec51a28d5852538fc1e518d5136dd7ea0060222529f60d0e4a1352e7d6a7c.json"
)
SECURITY_MASTER = (
    PROJECT_ROOT
    / "learning/security_masters/"
    "security-master-"
    "b9dac192763decf4d8a5fd7b843f261b65554918ad77a3b6984872a5aa393158"
    ".jsonl.gz"
)
SECURITY_MASTER_ATTESTATION = (
    PROJECT_ROOT
    / "historical_batches/challenger_orb_retest_v1/"
    "security-master-source.json"
)
EASTERN = ZoneInfo("America/New_York")
FILING_WINDOW_CUTOFF = "09:25:00"
MINIMUM_TOTAL_SIGNAL_DAYS = 50
MINIMUM_DEVELOPMENT_SIGNAL_DAYS = 30
MINIMUM_CONFIRMATION_SIGNAL_DAYS = 20
SEC_WORKERS = 4


class SecEarningsGapCapacityError(RuntimeError):
    """The SEC earnings-gap capacity graph is invalid or incomplete."""


def canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")


def content_hash(value: Any) -> str:
    return hashlib.sha256(canonical_bytes(value)).hexdigest()


def self_hash(value: Mapping[str, Any], field: str) -> str:
    return content_hash({key: item for key, item in value.items() if key != field})


def _timestamp(value: str, field: str) -> str:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise SecEarningsGapCapacityError(f"{field} must be ISO-8601") from exc
    if parsed.tzinfo is None:
        raise SecEarningsGapCapacityError(f"{field} must include a timezone")
    return parsed.isoformat()


def _repo_path(path: Path) -> str:
    try:
        return path.resolve().relative_to(PROJECT_ROOT.resolve()).as_posix()
    except ValueError as exc:
        raise SecEarningsGapCapacityError(f"path escaped repository: {path}") from exc


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise SecEarningsGapCapacityError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise SecEarningsGapCapacityError(f"{path} must contain an object")
    return value


def _write_json(path: Path, value: Mapping[str, Any]) -> None:
    rendered = json.dumps(value, indent=2, sort_keys=True) + "\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and path.read_text(encoding="utf-8") != rendered:
        raise SecEarningsGapCapacityError(f"content-addressed file drifted: {path}")
    if not path.exists():
        temporary = path.with_suffix(path.suffix + ".tmp")
        temporary.write_text(rendered, encoding="utf-8")
        temporary.replace(path)


def _gzip_bytes(value: Any) -> bytes:
    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode="wb", mtime=0) as stream:
        stream.write(canonical_bytes(value))
        stream.write(b"\n")
    return buffer.getvalue()


def _write_gzip(path: Path, value: Any) -> None:
    payload = _gzip_bytes(value)
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and path.read_bytes() != payload:
        raise SecEarningsGapCapacityError(f"private evidence drifted: {path}")
    if not path.exists():
        temporary = path.with_suffix(path.suffix + ".tmp")
        temporary.write_bytes(payload)
        temporary.replace(path)


def _read_gzip(path: Path) -> dict[str, Any]:
    try:
        with gzip.open(path, "rt", encoding="utf-8") as stream:
            value = json.load(stream)
    except (OSError, json.JSONDecodeError) as exc:
        raise SecEarningsGapCapacityError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise SecEarningsGapCapacityError(f"{path} must contain an object")
    return value


def _private_root(store: HistoricalDayStore) -> Path:
    return store.root / "_derived" / "sec_earnings_gap_capacity" / DATASET_ID


def _private_plan_path(store: HistoricalDayStore) -> Path:
    return _private_root(store) / "request-plan.json.gz"


def _private_collection_path(store: HistoricalDayStore) -> Path:
    return _private_root(store) / "capacity-collection.json.gz"


def _security_rows(path: Path = SECURITY_MASTER) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    try:
        with gzip.open(path, "rt", encoding="utf-8") as stream:
            for line in stream:
                row = json.loads(line)
                if isinstance(row, dict):
                    rows.append(row)
    except (OSError, json.JSONDecodeError) as exc:
        raise SecEarningsGapCapacityError(
            f"cannot read security master {path}: {exc}"
        ) from exc
    return rows


def _source_inventory(store: HistoricalDayStore) -> dict[str, Any]:
    inventory = _read_gzip(_inventory_path(store))
    if not (
        inventory.get("dataset_id")
        == "dataset-equity-gap-protection-continuation-2026-07-24-v2"
        and inventory.get("target_outcomes_observed_or_derived") is False
        and inventory.get("confirmation_outcomes_permitted") is False
    ):
        raise SecEarningsGapCapacityError(
            "source pre-entry inventory is not outcome blind"
        )
    return inventory


def build_request_plan(
    inventory: Mapping[str, Any],
    security_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    by_identity: dict[tuple[str, str], list[Mapping[str, Any]]] = {}
    for row in security_rows:
        key = (str(row.get("instrument_id") or ""), str(row.get("symbol") or ""))
        if not all(key):
            continue
        by_identity.setdefault(key, []).append(row)

    partition_by_day = {
        day: partition
        for partition, days in inventory["partitions"].items()
        for day in days
    }
    mapped: list[dict[str, str]] = []
    unmatched: list[dict[str, str]] = []
    for day in sorted(inventory["candidates_by_date"]):
        partition = partition_by_day.get(day)
        if partition is None:
            continue
        for candidate in inventory["candidates_by_date"][day]:
            symbol = str(candidate["symbol"])
            instrument_id = str(candidate["instrument_id"])
            key = (instrument_id, symbol)
            dated_ciks = {
                str(row.get("source", {}).get("cik") or "").zfill(10)
                for row in by_identity.get(key, [])
                if day in row.get("observed_dates", [])
                and isinstance(row.get("source"), Mapping)
                and row["source"].get("cik")
            }
            if len(dated_ciks) != 1:
                unmatched.append(
                    {
                        "date": day,
                        "partition": partition,
                        "symbol": symbol,
                        "instrument_id": instrument_id,
                        "reason": "no_unique_point_in_time_cik",
                    }
                )
                continue
            cik = dated_ciks.pop()
            mapped.append(
                {
                    "date": day,
                    "partition": partition,
                    "symbol": symbol,
                    "instrument_id": instrument_id,
                    "cik": cik,
                }
            )
    result = {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "source_inventory_content_sha256": inventory["content_sha256"],
        "mapped_pairs": mapped,
        "unmatched_pairs": unmatched,
        "unique_ciks": sorted({row["cik"] for row in mapped}),
        "market_prices_accessed": False,
        "forward_returns_accessed": False,
        "confirmation_outcomes_accessed": False,
    }
    result["content_sha256"] = content_hash(result)
    return result


def _partition_counts(plan: Mapping[str, Any]) -> dict[str, int]:
    return {
        partition: sum(
            row["partition"] == partition for row in plan["mapped_pairs"]
        )
        for partition in ("development", "embargo", "confirmation")
    }


def _trial_count() -> int:
    result = 1
    for values in PARAMETER_GRID.values():
        result *= len(values)
    return result


def build_contract(
    *,
    created_at: str,
    store: HistoricalDayStore | None = None,
    enforce_committed: bool = True,
) -> tuple[dict[str, Any], dict[str, Any]]:
    historical = store or HistoricalDayStore.from_env()
    inputs = (
        Path(__file__).resolve(),
        PROJECT_ROOT / "sec_earnings_gap_capacity_inspection.py",
        SOURCE_PREENTRY_CONTRACT,
        SOURCE_PREENTRY_SUMMARY,
        SOURCE_PREENTRY_INSPECTION,
        SECURITY_MASTER_ATTESTATION,
        CALENDAR_PATH,
    )
    if enforce_committed:
        for path in inputs:
            strategy_discovery.require_committed(path)
    source_inspection = _read_json(SOURCE_PREENTRY_INSPECTION)
    security_attestation = _read_json(SECURITY_MASTER_ATTESTATION)
    if not (
        source_inspection.get("state") == "PREENTRY_INSPECTED_READY"
        and source_inspection.get("valid") is True
    ):
        raise SecEarningsGapCapacityError("source pre-entry inspection is not ready")
    if security_attestation.get("security_master", {}).get(
        "sha256"
    ) != security_master_sha256(SECURITY_MASTER):
        raise SecEarningsGapCapacityError(
            "ignored security master differs from its committed attestation"
        )
    inventory = _source_inventory(historical)
    plan = build_request_plan(inventory, _security_rows())
    counts = _partition_counts(plan)
    private_bytes = _gzip_bytes(plan)
    value: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": "sec-earnings-gap-capacity-contract",
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "dataset_id": DATASET_ID,
        "state": "OUTCOME_BLIND_CAPACITY_FROZEN",
        "created_at": _timestamp(created_at, "created_at"),
        "source_binding": {
            "preentry_contract_path": _repo_path(SOURCE_PREENTRY_CONTRACT),
            "preentry_contract_file_sha256": sha256_file(
                SOURCE_PREENTRY_CONTRACT
            ),
            "preentry_summary_path": _repo_path(SOURCE_PREENTRY_SUMMARY),
            "preentry_summary_file_sha256": sha256_file(
                SOURCE_PREENTRY_SUMMARY
            ),
            "preentry_inspection_path": _repo_path(SOURCE_PREENTRY_INSPECTION),
            "preentry_inspection_file_sha256": sha256_file(
                SOURCE_PREENTRY_INSPECTION
            ),
            "private_inventory_file_sha256": sha256_file(
                _inventory_path(historical)
            ),
            "private_inventory_content_sha256": inventory["content_sha256"],
            "security_master_path": _repo_path(SECURITY_MASTER),
            "security_master_file_sha256": sha256_file(SECURITY_MASTER),
            "security_master_semantic_sha256": security_master_sha256(
                SECURITY_MASTER
            ),
            "security_master_attestation_path": _repo_path(
                SECURITY_MASTER_ATTESTATION
            ),
            "security_master_attestation_sha256": sha256_file(
                SECURITY_MASTER_ATTESTATION
            ),
            "calendar_path": _repo_path(CALENDAR_PATH),
            "calendar_sha256": sha256_file(CALENDAR_PATH),
        },
        "point_in_time_identity": {
            "match": ["instrument_id", "symbol"],
            "signal_date_must_be_in_observed_dates": True,
            "cik_source": "frozen security-master source.cik",
            "fallbacks_permitted": False,
            "mapped_candidate_pairs": len(plan["mapped_pairs"]),
            "unmatched_candidate_pairs": len(plan["unmatched_pairs"]),
            "unique_ciks": len(plan["unique_ciks"]),
            "mapped_pairs_by_partition": counts,
            "private_plan_content_sha256": plan["content_sha256"],
            "private_plan_file_sha256": hashlib.sha256(private_bytes).hexdigest(),
            "symbols_and_ciks_public": False,
        },
        "sec_event_semantics": {
            "source": "SEC EDGAR submissions JSON",
            "forms": ["8-K"],
            "required_items": ["2.02"],
            "excluded_items": ["3.02"],
            "amendments_permitted": False,
            "window_start": "previous_regular_session_16:00:00_ET_exclusive",
            "window_end": f"signal_session_{FILING_WINDOW_CUTOFF}_ET_inclusive",
            "acceptance_timestamp_required": True,
            "primary_document_required": True,
            "accession_required": True,
            "historical_submission_files_must_be_followed": True,
            "filing_documents_accessed": False,
        },
        "capacity_thresholds": {
            "signal_unit": "distinct date with at least one eligible candidate",
            "minimum_total_signal_days": MINIMUM_TOTAL_SIGNAL_DAYS,
            "minimum_development_signal_days": MINIMUM_DEVELOPMENT_SIGNAL_DAYS,
            "minimum_confirmation_signal_days": MINIMUM_CONFIRMATION_SIGNAL_DAYS,
            "embargo_signal_days_receive_zero_credit": True,
        },
        "future_search_contract": {
            "selection_mode": "development_search",
            "parameter_grid": PARAMETER_GRID,
            "trial_count": _trial_count(),
            "one_entry_per_family_per_day": True,
            "maximum_hold_sessions": 1,
            "confirmation_cannot_influence_selection": True,
        },
        "implementation_hashes": {
            "sec_earnings_gap_capacity.py": sha256_file(Path(__file__).resolve()),
            "sec_earnings_gap_capacity_inspection.py": sha256_file(
                PROJECT_ROOT / "sec_earnings_gap_capacity_inspection.py"
            ),
            "historical_discovery.py": sha256_file(
                PROJECT_ROOT / "historical_discovery.py"
            ),
        },
        "authorized_sec_submission_requests": len(plan["unique_ciks"]),
        "provider_requests_executed": 0,
        "market_prices_accessed": False,
        "forward_returns_accessed": False,
        "strategy_metrics_computed": 0,
        "confirmation_outcomes_accessed": False,
        "broker_actions": 0,
    }
    value["contract_sha256"] = self_hash(value, "contract_sha256")
    return value, plan


def freeze_contract(
    *,
    created_at: str,
    root: Path = DEFAULT_ROOT,
    store: HistoricalDayStore | None = None,
) -> tuple[Path, dict[str, Any]]:
    historical = store or HistoricalDayStore.from_env()
    value, plan = build_contract(created_at=created_at, store=historical)
    _write_gzip(_private_plan_path(historical), plan)
    path = (
        root
        / "capacity-contract"
        / f"contract-{value['contract_sha256']}.json"
    )
    _write_json(path, value)
    return path, value


def _columnar_rows(value: Mapping[str, Any]) -> list[dict[str, Any]]:
    fields = {key: rows for key, rows in value.items() if isinstance(rows, list)}
    count = max((len(rows) for rows in fields.values()), default=0)
    return [
        {
            key: rows[index] if index < len(rows) else None
            for key, rows in fields.items()
        }
        for index in range(count)
    ]


def submission_rows(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    filings = payload.get("filings")
    recent = filings.get("recent") if isinstance(filings, Mapping) else None
    if not isinstance(recent, Mapping):
        return []
    return _columnar_rows(recent)


def relevant_history_files(payload: Mapping[str, Any]) -> list[str]:
    filings = payload.get("filings")
    files = filings.get("files") if isinstance(filings, Mapping) else None
    if not isinstance(files, list):
        return []
    names: list[str] = []
    for row in files:
        if not isinstance(row, Mapping):
            continue
        start = str(row.get("filingFrom") or "")
        end = str(row.get("filingTo") or "")
        name = str(row.get("name") or "")
        if name and start <= "2024-12-31" and end >= "2023-01-01":
            names.append(name)
    return sorted(set(names))


def previous_session_map(calendar: Sequence[str]) -> dict[str, str]:
    ordered = sorted(set(calendar))
    return {day: ordered[index - 1] for index, day in enumerate(ordered) if index}


def eligible_filings(
    rows: Sequence[Mapping[str, Any]],
    *,
    previous_session: str,
    signal_date: str,
) -> list[dict[str, Any]]:
    start = datetime.fromisoformat(
        f"{previous_session}T16:00:00"
    ).replace(tzinfo=EASTERN)
    end = datetime.fromisoformat(
        f"{signal_date}T{FILING_WINDOW_CUTOFF}"
    ).replace(tzinfo=EASTERN)
    result: list[dict[str, Any]] = []
    for row in rows:
        accepted = _accepted_at(row.get("acceptanceDateTime"))
        items = _filing_items(row.get("items"))
        accession = str(row.get("accessionNumber") or "").strip()
        primary = str(row.get("primaryDocument") or "").strip()
        if not (
            row.get("form") == "8-K"
            and accepted is not None
            and start < accepted <= end
            and "2.02" in items
            and "3.02" not in items
            and accession
            and primary
        ):
            continue
        result.append(
            {
                "accepted_at": accepted.isoformat(),
                "accession": accession,
                "items": items,
                "primary_document": primary,
            }
        )
    return sorted(result, key=lambda row: (row["accepted_at"], row["accession"]))


def _load_contract(path: Path) -> dict[str, Any]:
    value = _read_json(path)
    if value.get("contract_sha256") != self_hash(value, "contract_sha256"):
        raise SecEarningsGapCapacityError("capacity contract hash is invalid")
    return value


def _load_inspection(path: Path, contract: Mapping[str, Any]) -> dict[str, Any]:
    value = _read_json(path)
    if not (
        value.get("inspection_sha256") == self_hash(value, "inspection_sha256")
        and value.get("state") == "CAPACITY_CONTRACT_INSPECTED_READY"
        and value.get("valid") is True
        and value.get("contract_sha256") == contract["contract_sha256"]
    ):
        raise SecEarningsGapCapacityError("capacity contract inspection is invalid")
    return value


def _load_submission_corpus(
    *,
    client: SecClient,
    ciks: Sequence[str],
    cache_root: Path,
) -> tuple[dict[str, list[dict[str, Any]]], list[dict[str, Any]]]:
    def main_request(cik: str) -> tuple[str, Mapping[str, Any], Path]:
        path = cache_root / "submissions" / f"CIK{cik}.json"
        payload = client.json(f"{SEC_SUBMISSIONS_ROOT}/CIK{cik}.json", path)
        return cik, payload, path

    with ThreadPoolExecutor(max_workers=client.config.workers) as executor:
        main_results = list(executor.map(main_request, ciks))

    rows_by_cik: dict[str, list[dict[str, Any]]] = {}
    source_files: list[dict[str, Any]] = []
    history_requests: list[tuple[str, str]] = []
    for cik, payload, path in main_results:
        rows_by_cik[cik] = submission_rows(payload)
        source_files.append(
            {
                "cik": cik,
                "kind": "main",
                "cache_path": path.relative_to(cache_root.parent).as_posix(),
                "sha256": sha256_file(path),
            }
        )
        history_requests.extend(
            (cik, name) for name in relevant_history_files(payload)
        )

    def history_request(
        request: tuple[str, str],
    ) -> tuple[str, str, Mapping[str, Any], Path]:
        cik, name = request
        path = cache_root / "submissions" / "history" / name
        payload = client.json(f"{SEC_SUBMISSIONS_ROOT}/{name}", path)
        return cik, name, payload, path

    with ThreadPoolExecutor(max_workers=client.config.workers) as executor:
        history_results = list(executor.map(history_request, history_requests))
    for cik, name, payload, path in history_results:
        rows_by_cik[cik].extend(_columnar_rows(payload))
        source_files.append(
            {
                "cik": cik,
                "kind": "history",
                "name": name,
                "cache_path": path.relative_to(cache_root.parent).as_posix(),
                "sha256": sha256_file(path),
            }
        )
    source_files.sort(
        key=lambda row: (row["cik"], row["kind"], row.get("name", ""))
    )
    return rows_by_cik, source_files


def build_eligibility(
    plan: Mapping[str, Any],
    rows_by_cik: Mapping[str, Sequence[Mapping[str, Any]]],
    calendar: Sequence[str],
) -> dict[str, Any]:
    previous = previous_session_map(calendar)
    eligible_pairs: list[dict[str, Any]] = []
    for pair in plan["mapped_pairs"]:
        day = pair["date"]
        if day not in previous:
            raise SecEarningsGapCapacityError(
                f"calendar lacks a prior session for {day}"
            )
        filings = eligible_filings(
            rows_by_cik.get(pair["cik"], []),
            previous_session=previous[day],
            signal_date=day,
        )
        if filings:
            eligible_pairs.append({**pair, "filings": filings})
    signal_days = {
        partition: sorted(
            {
                row["date"]
                for row in eligible_pairs
                if row["partition"] == partition
            }
        )
        for partition in ("development", "embargo", "confirmation")
    }
    return {
        "eligible_pairs": eligible_pairs,
        "signal_days": signal_days,
    }


def collect_capacity(
    *,
    contract_path: Path,
    inspection_path: Path,
    collected_at: str,
    env_path: Path = PROJECT_ROOT / ".env",
    root: Path = DEFAULT_ROOT,
    store: HistoricalDayStore | None = None,
) -> tuple[Path, dict[str, Any]]:
    historical = store or HistoricalDayStore.from_env(env_path)
    for path in (contract_path, inspection_path):
        strategy_discovery.require_committed(path)
    contract = _load_contract(contract_path)
    inspection = _load_inspection(inspection_path, contract)
    plan = _read_gzip(_private_plan_path(historical))
    if not (
        plan.get("content_sha256")
        == contract["point_in_time_identity"]["private_plan_content_sha256"]
        and sha256_file(_private_plan_path(historical))
        == contract["point_in_time_identity"]["private_plan_file_sha256"]
    ):
        raise SecEarningsGapCapacityError("private request plan drifted")
    cache_root = historical.root / "_sources" / "sec"
    config = SecConfig.from_env(
        env_path,
        cache_root,
        workers=SEC_WORKERS,
    )
    client = SecClient(config)
    rows_by_cik, source_files = _load_submission_corpus(
        client=client,
        ciks=plan["unique_ciks"],
        cache_root=cache_root,
    )
    eligibility = build_eligibility(
        plan,
        rows_by_cik,
        load_calendar(CALENDAR_PATH),
    )
    private = {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "contract_sha256": contract["contract_sha256"],
        "inspection_sha256": inspection["inspection_sha256"],
        "plan_content_sha256": plan["content_sha256"],
        "source_files": source_files,
        **eligibility,
        "market_prices_accessed": False,
        "forward_returns_accessed": False,
        "confirmation_outcomes_accessed": False,
    }
    private["content_sha256"] = content_hash(private)
    private_path = _private_collection_path(historical)
    _write_gzip(private_path, private)
    signal_days = private["signal_days"]
    telemetry = client.stats()
    value: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": "sec-earnings-gap-capacity-collection",
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "dataset_id": DATASET_ID,
        "state": "CAPACITY_COLLECTED_AWAITING_INSPECTION",
        "collected_at": _timestamp(collected_at, "collected_at"),
        "contract_path": _repo_path(contract_path),
        "contract_sha256": contract["contract_sha256"],
        "inspection_path": _repo_path(inspection_path),
        "inspection_sha256": inspection["inspection_sha256"],
        "private_collection_content_sha256": private["content_sha256"],
        "private_collection_file_sha256": sha256_file(private_path),
        "source_file_count": len(source_files),
        "mapped_candidate_pairs": len(plan["mapped_pairs"]),
        "eligible_candidate_pairs": len(private["eligible_pairs"]),
        "development_signal_days": len(signal_days["development"]),
        "embargo_signal_days": len(signal_days["embargo"]),
        "confirmation_signal_days": len(signal_days["confirmation"]),
        "total_creditable_signal_days": (
            len(signal_days["development"])
            + len(signal_days["confirmation"])
        ),
        "provider_telemetry": telemetry,
        "metadata_rows_accessed": sum(
            len(rows) for rows in rows_by_cik.values()
        ),
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
    collect = subparsers.add_parser("collect")
    collect.add_argument("contract", type=Path)
    collect.add_argument("inspection", type=Path)
    collect.add_argument("--collected-at", required=True)
    args = parser.parse_args(argv)
    try:
        if args.command == "freeze-contract":
            path, value = freeze_contract(created_at=args.created_at)
        else:
            path, value = collect_capacity(
                contract_path=args.contract,
                inspection_path=args.inspection,
                collected_at=args.collected_at,
            )
        print(
            json.dumps(
                {
                    "path": _repo_path(path),
                    "state": value["state"],
                    "sha256": value.get("contract_sha256")
                    or value.get("collection_sha256"),
                    "market_prices_accessed": False,
                    "broker_actions": 0,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    except (
        SecEarningsGapCapacityError,
        strategy_discovery.StrategyDiscoveryError,
        OSError,
        KeyError,
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


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())

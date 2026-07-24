"""Freeze and collect outcome-blind inputs for earnings-gap continuation.

This family combines a company-verified positive earnings surprise with the
already frozen point-in-time 2-8% opening-gap universe.  Event metadata is
collected before any new family outcome is evaluated.  The previously exposed
January-August 2025 gap corpus is development-only contaminated training; the
later reserve remains locked until an exact winner is frozen.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import io
import json
import os
from calendar import monthrange
from collections import defaultdict
from collections.abc import Mapping, Sequence
from datetime import date, datetime
from pathlib import Path
from typing import Any

import equity_gap_continuation_validation as gap
import outcome_exposure
import portfolio_maturity
import strategy_discovery
from historical_store import HistoricalDayStore, canonical_sha256, sha256_file


PROJECT_ROOT = Path(__file__).resolve().parent
CAMPAIGN_ID = portfolio_maturity.V2_CAMPAIGN_ID
FAMILY_ID = "earnings-gap-continuation"
MECHANISM_FAMILY = FAMILY_ID
STRATEGY_ID = FAMILY_ID
SUCCESSOR_ID = "earnings-gap-continuation-v1-development-search"
SCHEMA_VERSION = 1
DEFAULT_ROOT = (
    PROJECT_ROOT
    / "strategy_tournament/v2/continuous"
    / SUCCESSOR_ID
)
SOURCE_MANIFEST = (
    PROJECT_ROOT
    / "strategy_validation/equity_gap_continuation/manifests/"
    "equity-gap-continuation-v1-"
    "0145f77948ff8d7398c69ec7ae2229b72cfb7a07bab055c3dcfb15762d5cea43.json"
)
SOURCE_FREEZE_INSPECTION = (
    PROJECT_ROOT
    / "strategy_validation/equity_gap_continuation/inspections/"
    "equity-gap-continuation-v1-freeze-"
    "1aa8fad408438bf19824ae1c01ede6ea35c446238d75113666e337e0fdecec67.json"
)
EVENT_START = "2025-01-01"
EVENT_END = "2025-12-31"
SESSION_CALENDAR = (
    PROJECT_ROOT
    / "historical_batches/preentry_structure/"
    "session-calendar-2024-12-through-2026-06.json"
)
PRIOR_DISCARDED_PROVIDER_REQUESTS = 24
SUPERSEDED_EVENT_CONTRACT_SHA256 = (
    "2c11eaea9ca9ae08f0f09b5029efd116d6e69f7293e2c6bde7c96bd5dc632a08"
)
RETAINED_PROVIDER_CONTRACT_SHA256 = (
    "a1cbdb6050afcf1af7c0639d0374c49ed77a172a14ad510c133919adbf78a945"
)


class EarningsGapContinuationError(RuntimeError):
    """The event collection or frozen family boundary is invalid."""


def _canonical(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode()


def _hash(value: Any) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _self_hash(value: Mapping[str, Any], field: str) -> str:
    return _hash({key: item for key, item in value.items() if key != field})


def _read(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise EarningsGapContinuationError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise EarningsGapContinuationError(f"{path} must contain an object")
    return value


def _write_json(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    rendered = json.dumps(value, indent=2, sort_keys=True) + "\n"
    if path.exists() and path.read_text(encoding="utf-8") == rendered:
        return
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_text(rendered, encoding="utf-8")
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _gzip_bytes(value: Any) -> bytes:
    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode="wb", compresslevel=6, mtime=0) as stream:
        stream.write(json.dumps(value, sort_keys=True).encode())
        stream.write(b"\n")
    return buffer.getvalue()


def _write_gzip(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_bytes(_gzip_bytes(value))
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _load_gzip(path: Path) -> Any:
    try:
        with gzip.open(path, "rt", encoding="utf-8") as stream:
            return json.load(stream)
    except (OSError, json.JSONDecodeError) as exc:
        raise EarningsGapContinuationError(f"cannot read {path}: {exc}") from exc


def _repo_path(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(PROJECT_ROOT.resolve()))
    except ValueError as exc:
        raise EarningsGapContinuationError(f"path escaped repository: {path}") from exc


def _timestamp(value: str, field: str) -> None:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise EarningsGapContinuationError(f"{field} is invalid") from exc
    if parsed.tzinfo is None:
        raise EarningsGapContinuationError(f"{field} needs a timezone")


def _event_windows() -> list[dict[str, Any]]:
    return [
        {
            "start_date": date(2025, month, 1).isoformat(),
            "days": monthrange(2025, month)[1],
            "filter": None,
        }
        for month in range(1, 13)
    ]


def _source_graph(
    *, enforce_commit: bool
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    if enforce_commit:
        for path in (SOURCE_MANIFEST, SOURCE_FREEZE_INSPECTION):
            strategy_discovery.require_committed(path)
    manifest = _read(SOURCE_MANIFEST)
    inspection = _read(SOURCE_FREEZE_INSPECTION)
    store = HistoricalDayStore.from_env()
    selection = gap._load_gzip(gap._selection_path(store))
    if not (
        manifest.get("campaign_id") == "multi-strategy-portfolio-validation-v1"
        and manifest.get("mechanism_family") == "equity-gap-continuation"
        and manifest.get("private_selection", {}).get("contains_target_returns")
        is False
        and canonical_sha256(selection)
        == manifest.get("private_selection", {}).get("content_sha256")
        and inspection.get("valid") is True
        and inspection.get("confirmation_untouched") is True
        and selection.get("target_outcomes_observed_or_derived") is False
        and len(selection.get("phases", {}).get("development", {}).get("dates", []))
        == 120
        and len(selection.get("embargo_dates", [])) == 5
        and len(selection.get("phases", {}).get("confirmation", {}).get("dates", []))
        == 75
    ):
        raise EarningsGapContinuationError(
            "point-in-time equity-gap source graph is invalid"
        )
    return manifest, inspection, selection


def build_event_collection_contract(
    *, created_at: str, enforce_commit: bool
) -> dict[str, Any]:
    """Build the exact zero-market-outcome Robinhood calendar request contract."""

    _timestamp(created_at, "created_at")
    manifest, inspection, selection = _source_graph(
        enforce_commit=enforce_commit
    )
    if enforce_commit:
        strategy_discovery.require_committed(Path(__file__).resolve())
    development_dates = selection["phases"]["development"]["dates"]
    confirmation_dates = selection["phases"]["confirmation"]["dates"]
    value: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "artifact_kind": "earnings-gap-event-collection-contract",
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "successor_id": SUCCESSOR_ID,
        "created_at": created_at,
        "provider": "Robinhood read-only market-wide earnings calendar",
        "provider_method": "get_earnings_calendar",
        "event_start": EVENT_START,
        "event_end": EVENT_END,
        "requests": _event_windows(),
        "logical_provider_requests": 12,
        "prior_discarded_provider_requests": PRIOR_DISCARDED_PROVIDER_REQUESTS,
        "total_provider_requests_after_collection": (
            12 + PRIOR_DISCARDED_PROVIDER_REQUESTS
        ),
        "supersedes_event_contract_sha256": SUPERSEDED_EVENT_CONTRACT_SHA256,
        "transport_recovery": (
            "The first twelve responses were discarded when stdin ingestion "
            "waited for terminal EOF. The next twelve were discarded when the "
            "PTY input queue rejected oversized response lines. No event "
            "artifact, prices, forward returns, strategy metrics, or broker "
            "actions were produced. Recovery spools the exact retry outside Git "
            "before one deterministic file ingestion."
        ),
        "selection_fields": [
            "symbol",
            "report_date",
            "timing",
            "verified",
            "actual_eps",
            "estimated_eps",
        ],
        "selection_rule": (
            "Retain company-verified reports with numeric actual EPS strictly "
            "above numeric estimated EPS; before-market reports map to the same "
            "session and after-market reports map to the next frozen session."
        ),
        "market_prices_accessed": False,
        "forward_returns_accessed": False,
        "broker_actions_permitted": False,
        "development_training_contaminated": True,
        "confirmation_locked": True,
        "development_dates_sha256": _hash(development_dates),
        "confirmation_dates_sha256": _hash(confirmation_dates),
        "source_bindings": {
            _repo_path(SOURCE_MANIFEST): sha256_file(SOURCE_MANIFEST),
            _repo_path(SOURCE_FREEZE_INSPECTION): sha256_file(
                SOURCE_FREEZE_INSPECTION
            ),
            "source_manifest_sha256": manifest["manifest_sha256"],
            "source_freeze_inspection_sha256": inspection["inspection_sha256"],
            "private_selection_content_sha256": canonical_sha256(selection),
        },
        "implementation_sha256": sha256_file(Path(__file__).resolve()),
    }
    value["contract_sha256"] = _self_hash(value, "contract_sha256")
    return value


def freeze_event_collection(
    *, created_at: str, root: Path = DEFAULT_ROOT
) -> tuple[Path, dict[str, Any]]:
    value = build_event_collection_contract(
        created_at=created_at, enforce_commit=True
    )
    path = (
        root
        / "event-contract"
        / f"earnings-gap-event-contract-{value['contract_sha256']}.json"
    )
    _write_json(path, value)
    return path, value


def inspect_event_collection(
    contract_path: Path,
    *,
    inspected_at: str,
    root: Path = DEFAULT_ROOT,
) -> tuple[Path, dict[str, Any]]:
    _timestamp(inspected_at, "inspected_at")
    strategy_discovery.require_committed(contract_path)
    recorded = _read(contract_path)
    if recorded.get("contract_sha256") != _self_hash(
        recorded, "contract_sha256"
    ):
        raise EarningsGapContinuationError("event contract hash is invalid")
    rebuilt = build_event_collection_contract(
        created_at=str(recorded.get("created_at")), enforce_commit=True
    )
    if recorded != rebuilt:
        raise EarningsGapContinuationError("event contract does not rebuild")
    value: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "artifact_kind": "earnings-gap-event-contract-inspection",
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "inspected_at": inspected_at,
        "contract_path": _repo_path(contract_path),
        "contract_file_sha256": sha256_file(contract_path),
        "contract_sha256": recorded["contract_sha256"],
        "logical_provider_requests": recorded["logical_provider_requests"],
        "prior_discarded_provider_requests": recorded[
            "prior_discarded_provider_requests"
        ],
        "market_prices_accessed": False,
        "forward_returns_accessed": False,
        "broker_actions_permitted": False,
        "collection_authorized": True,
        "valid": True,
    }
    value["inspection_sha256"] = _self_hash(value, "inspection_sha256")
    path = (
        root
        / "event-contract-inspection"
        / f"earnings-gap-event-contract-inspection-{value['inspection_sha256']}.json"
    )
    _write_json(path, value)
    return path, value


def _find_results(value: Any) -> list[Any] | None:
    if isinstance(value, Mapping):
        data = value.get("data")
        if isinstance(data, Mapping) and isinstance(data.get("results"), list):
            return data["results"]
        if isinstance(value.get("results"), list):
            return value["results"]
        for key in ("structuredContent", "structured_content"):
            found = _find_results(value.get(key))
            if found is not None:
                return found
        content = value.get("content")
        if isinstance(content, list):
            for item in content:
                found = _find_results(item)
                if found is not None:
                    return found
                if isinstance(item, Mapping) and isinstance(item.get("text"), str):
                    try:
                        decoded = json.loads(item["text"])
                    except json.JSONDecodeError:
                        continue
                    found = _find_results(decoded)
                    if found is not None:
                        return found
    return None


def _number(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if result == result and abs(result) != float("inf") else None


def _normalize_calendar_row(raw: Mapping[str, Any]) -> dict[str, Any] | None:
    symbol = str(raw.get("symbol", "")).strip().upper()
    report = raw.get("report") if isinstance(raw.get("report"), Mapping) else raw
    eps = raw.get("eps") if isinstance(raw.get("eps"), Mapping) else raw
    report_date = report.get("date", raw.get("report_date"))
    timing = report.get("timing", raw.get("timing"))
    verified = report.get("verified", raw.get("verified"))
    actual = _number(eps.get("actual", raw.get("actual_eps")))
    estimate = _number(eps.get("estimate", raw.get("estimated_eps")))
    try:
        parsed_date = date.fromisoformat(str(report_date))
    except ValueError:
        return None
    if (
        not symbol
        or not EVENT_START <= parsed_date.isoformat() <= EVENT_END
        or timing not in {"am", "pm"}
    ):
        return None
    return {
        "symbol": symbol,
        "report_date": parsed_date.isoformat(),
        "timing": str(timing),
        "verified": verified is True,
        "actual_eps": actual,
        "estimated_eps": estimate,
    }


def _normalized_events(
    lines: Sequence[str],
) -> tuple[list[dict[str, Any]], int, int]:
    """Normalize calendar rows and exclude every ambiguous event identity."""

    normalized: list[dict[str, Any]] = []
    provider_rows = 0
    response_count = 0
    for line in lines:
        if not line.strip() or line.strip() == "__END__":
            continue
        try:
            item = json.loads(line)
        except json.JSONDecodeError as exc:
            raise EarningsGapContinuationError(
                "event response line is not JSON"
            ) from exc
        if not isinstance(item, Mapping):
            raise EarningsGapContinuationError("event response line is malformed")
        raw_results = _find_results(item.get("response"))
        if raw_results is None:
            raise EarningsGapContinuationError(
                f"{item.get('start_date')}: earnings calendar response is unusable"
            )
        response_count += 1
        provider_rows += len(raw_results)
        for raw in raw_results:
            if isinstance(raw, Mapping):
                row = _normalize_calendar_row(raw)
                if row is not None:
                    normalized.append(row)
    grouped: dict[
        tuple[str, str, str], list[dict[str, Any]]
    ] = defaultdict(list)
    for row in normalized:
        grouped[
            (row["symbol"], row["report_date"], row["timing"])
        ].append(row)
    ambiguous_event_identities = sum(
        1 for values in grouped.values() if len(values) != 1
    )
    rows = sorted(
        (
            values[0]
            for values in grouped.values()
            if len(values) == 1
        ),
        key=lambda row: (row["report_date"], row["symbol"], row["timing"]),
    )
    return rows, provider_rows, ambiguous_event_identities


def _raw_spool_lines(path: Path) -> list[str]:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        raise EarningsGapContinuationError(
            f"cannot read raw event spool: {exc}"
        ) from exc
    payload_lines = [
        line
        for line in lines
        if line.strip() and line.strip() != "__END__"
    ]
    if len(payload_lines) != 12 or lines[-1].strip() != "__END__":
        raise EarningsGapContinuationError(
            "raw event spool does not contain twelve terminated responses"
        )
    starts: list[str] = []
    for line in payload_lines:
        try:
            value = json.loads(line)
        except json.JSONDecodeError as exc:
            raise EarningsGapContinuationError(
                "raw event spool is not JSONL"
            ) from exc
        if not isinstance(value, Mapping):
            raise EarningsGapContinuationError(
                "raw event spool row is malformed"
            )
        starts.append(str(value.get("start_date", "")))
    expected = [item["start_date"] for item in _event_windows()]
    if starts != expected:
        raise EarningsGapContinuationError(
            "raw event spool windows drifted"
        )
    return lines


def freeze_normalization_contract(
    provider_contract_path: Path,
    provider_inspection_path: Path,
    *,
    raw_spool_path: Path,
    created_at: str,
    root: Path = DEFAULT_ROOT,
) -> tuple[Path, dict[str, Any]]:
    """Freeze the zero-request duplicate policy against retained raw responses."""

    _timestamp(created_at, "created_at")
    for path in (provider_contract_path, provider_inspection_path):
        strategy_discovery.require_committed(path)
    provider_contract = _read(provider_contract_path)
    provider_inspection = _read(provider_inspection_path)
    if not (
        provider_contract.get("contract_sha256")
        == RETAINED_PROVIDER_CONTRACT_SHA256
        and provider_contract.get("prior_discarded_provider_requests") == 24
        and provider_inspection.get("contract_sha256")
        == provider_contract.get("contract_sha256")
        and provider_inspection.get("collection_authorized") is True
        and provider_inspection.get("valid") is True
    ):
        raise EarningsGapContinuationError(
            "retained provider contract chain is invalid"
        )
    _raw_spool_lines(raw_spool_path)
    value: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "artifact_kind": "earnings-gap-event-normalization-contract",
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "created_at": created_at,
        "provider_contract_path": _repo_path(provider_contract_path),
        "provider_contract_sha256": provider_contract["contract_sha256"],
        "provider_inspection_path": _repo_path(provider_inspection_path),
        "provider_inspection_sha256": provider_inspection[
            "inspection_sha256"
        ],
        "raw_spool": (
            "LOCAL_HISTORICAL_DATA_ROOT/_spool/"
            f"{raw_spool_path.name}"
        ),
        "raw_spool_file_sha256": sha256_file(raw_spool_path),
        "raw_response_count": 12,
        "prior_discarded_provider_requests": 24,
        "effective_provider_requests": 12,
        "total_provider_requests": 36,
        "new_provider_requests_authorized": 0,
        "normalization_rule": (
            "Normalize the declared symbol, report date, am/pm timing, "
            "verification, actual EPS, and estimated EPS fields. Group by exact "
            "symbol/report-date/timing and exclude every identity represented by "
            "more than one provider row; never choose among duplicate EPS values."
        ),
        "normalization_selected_without_forward_outcomes": True,
        "market_prices_accessed": False,
        "forward_returns_accessed": False,
        "broker_actions_permitted": False,
        "implementation_sha256": sha256_file(Path(__file__).resolve()),
    }
    value["contract_sha256"] = _self_hash(value, "contract_sha256")
    path = (
        root
        / "event-normalization-contract"
        / f"earnings-gap-event-normalization-contract-"
        f"{value['contract_sha256']}.json"
    )
    _write_json(path, value)
    return path, value


def inspect_normalization_contract(
    contract_path: Path,
    *,
    raw_spool_path: Path,
    inspected_at: str,
    root: Path = DEFAULT_ROOT,
) -> tuple[Path, dict[str, Any]]:
    _timestamp(inspected_at, "inspected_at")
    strategy_discovery.require_committed(contract_path)
    recorded = _read(contract_path)
    if not (
        recorded.get("artifact_kind")
        == "earnings-gap-event-normalization-contract"
        and recorded.get("contract_sha256")
        == _self_hash(recorded, "contract_sha256")
        and recorded.get("implementation_sha256")
        == sha256_file(Path(__file__).resolve())
        and recorded.get("raw_spool_file_sha256")
        == sha256_file(raw_spool_path)
        and recorded.get("new_provider_requests_authorized") == 0
    ):
        raise EarningsGapContinuationError(
            "event normalization contract is invalid"
        )
    lines = _raw_spool_lines(raw_spool_path)
    rows, provider_rows, ambiguous = _normalized_events(lines)
    value: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "artifact_kind": "earnings-gap-event-normalization-inspection",
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "inspected_at": inspected_at,
        "contract_path": _repo_path(contract_path),
        "contract_file_sha256": sha256_file(contract_path),
        "contract_sha256": recorded["contract_sha256"],
        "raw_spool_file_sha256": recorded["raw_spool_file_sha256"],
        "raw_response_count": 12,
        "provider_rows": provider_rows,
        "normalized_unambiguous_events": len(rows),
        "excluded_ambiguous_event_identities": ambiguous,
        "new_provider_requests": 0,
        "market_prices_accessed": False,
        "forward_returns_accessed": False,
        "broker_actions": 0,
        "ingestion_authorized": True,
        "valid": True,
    }
    value["inspection_sha256"] = _self_hash(value, "inspection_sha256")
    path = (
        root
        / "event-normalization-inspection"
        / f"earnings-gap-event-normalization-inspection-"
        f"{value['inspection_sha256']}.json"
    )
    _write_json(path, value)
    return path, value


def ingest_normalized_events(
    contract_path: Path,
    inspection_path: Path,
    *,
    raw_spool_path: Path,
    collected_at: str,
    root: Path = DEFAULT_ROOT,
    store: HistoricalDayStore | None = None,
) -> tuple[Path, dict[str, Any]]:
    """Persist the inspected, unambiguous event metadata without new requests."""

    _timestamp(collected_at, "collected_at")
    for path in (contract_path, inspection_path):
        strategy_discovery.require_committed(path)
    contract = _read(contract_path)
    inspection = _read(inspection_path)
    if not (
        contract.get("contract_sha256")
        == _self_hash(contract, "contract_sha256")
        and contract.get("implementation_sha256")
        == sha256_file(Path(__file__).resolve())
        and contract.get("raw_spool_file_sha256")
        == sha256_file(raw_spool_path)
        and inspection.get("contract_sha256")
        == contract.get("contract_sha256")
        and inspection.get("raw_spool_file_sha256")
        == contract.get("raw_spool_file_sha256")
        and inspection.get("ingestion_authorized") is True
        and inspection.get("valid") is True
    ):
        raise EarningsGapContinuationError(
            "normalization inspection does not authorize ingestion"
        )
    rows, provider_rows, ambiguous = _normalized_events(
        _raw_spool_lines(raw_spool_path)
    )
    source = store or HistoricalDayStore.from_env()
    private = (
        source.root
        / "_derived/earnings_gap_continuation"
        / contract["contract_sha256"]
        / "event-calendar.json.gz"
    )
    payload = {
        "schema_version": SCHEMA_VERSION,
        "normalization_contract_sha256": contract["contract_sha256"],
        "normalization_inspection_sha256": inspection[
            "inspection_sha256"
        ],
        "collected_at": collected_at,
        "provider": "Robinhood read-only market-wide earnings calendar",
        "provider_requests": contract["total_provider_requests"],
        "effective_provider_requests": contract[
            "effective_provider_requests"
        ],
        "discarded_provider_requests": contract[
            "prior_discarded_provider_requests"
        ],
        "provider_rows": provider_rows,
        "excluded_ambiguous_event_identities": ambiguous,
        "market_prices_accessed": False,
        "forward_returns_accessed": False,
        "broker_actions": 0,
        "events": rows,
    }
    if private.exists() and _load_gzip(private) != payload:
        raise EarningsGapContinuationError(
            "private normalized event payload drifted"
        )
    if not private.exists():
        _write_gzip(private, payload)
    positive = [
        row
        for row in rows
        if row["verified"]
        and row["actual_eps"] is not None
        and row["estimated_eps"] is not None
        and row["actual_eps"] > row["estimated_eps"]
    ]
    value: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "artifact_kind": "earnings-gap-event-collection",
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "collected_at": collected_at,
        "normalization_contract_path": _repo_path(contract_path),
        "normalization_contract_sha256": contract["contract_sha256"],
        "normalization_inspection_path": _repo_path(inspection_path),
        "normalization_inspection_sha256": inspection[
            "inspection_sha256"
        ],
        "provider_requests": contract["total_provider_requests"],
        "effective_provider_requests": contract[
            "effective_provider_requests"
        ],
        "discarded_provider_requests": contract[
            "prior_discarded_provider_requests"
        ],
        "provider_rows": provider_rows,
        "normalized_events": len(rows),
        "excluded_ambiguous_event_identities": ambiguous,
        "verified_positive_surprises": len(positive),
        "private_payload_file_sha256": sha256_file(private),
        "private_payload_content_sha256": canonical_sha256(payload),
        "private_payload": (
            "LOCAL_HISTORICAL_DATA_ROOT/_derived/earnings_gap_continuation/"
            f"{contract['contract_sha256']}/event-calendar.json.gz"
        ),
        "new_provider_requests_during_ingestion": 0,
        "market_prices_accessed": False,
        "forward_returns_accessed": False,
        "broker_actions": 0,
        "state": "EVENT_METADATA_COLLECTED_UNINSPECTED",
    }
    value["collection_sha256"] = _self_hash(value, "collection_sha256")
    path = (
        root
        / "event-collection"
        / f"earnings-gap-event-collection-{value['collection_sha256']}.json"
    )
    _write_json(path, value)
    return path, value


def inspect_event_capacity(
    collection_path: Path,
    *,
    inspected_at: str,
    root: Path = DEFAULT_ROOT,
    store: HistoricalDayStore | None = None,
) -> tuple[Path, dict[str, Any]]:
    """Join event metadata to the gap denominator without opening outcomes."""

    _timestamp(inspected_at, "inspected_at")
    strategy_discovery.require_committed(collection_path)
    strategy_discovery.require_committed(SESSION_CALENDAR)
    collection = _read(collection_path)
    if not (
        collection.get("artifact_kind") == "earnings-gap-event-collection"
        and collection.get("collection_sha256")
        == _self_hash(collection, "collection_sha256")
        and collection.get("market_prices_accessed") is False
        and collection.get("forward_returns_accessed") is False
        and collection.get("broker_actions") == 0
    ):
        raise EarningsGapContinuationError(
            "event collection artifact is invalid"
        )
    source = store or HistoricalDayStore.from_env()
    private = (
        source.root
        / "_derived/earnings_gap_continuation"
        / collection["normalization_contract_sha256"]
        / "event-calendar.json.gz"
    )
    payload = _load_gzip(private)
    if not (
        sha256_file(private) == collection["private_payload_file_sha256"]
        and canonical_sha256(payload)
        == collection["private_payload_content_sha256"]
        and payload.get("forward_returns_accessed") is False
        and payload.get("market_prices_accessed") is False
    ):
        raise EarningsGapContinuationError(
            "private event metadata drifted"
        )
    try:
        calendar_rows = json.loads(
            SESSION_CALENDAR.read_text(encoding="utf-8")
        )
    except (OSError, json.JSONDecodeError) as exc:
        raise EarningsGapContinuationError(
            f"session calendar is unavailable: {exc}"
        ) from exc
    sessions = [
        str(row["date"])
        for row in calendar_rows
        if isinstance(row, Mapping) and row.get("date")
    ]
    if sessions != sorted(set(sessions)):
        raise EarningsGapContinuationError(
            "session calendar is invalid"
        )
    reaction_rows: dict[
        tuple[str, str], list[dict[str, Any]]
    ] = defaultdict(list)
    for row in payload["events"]:
        if not (
            row["verified"]
            and row["actual_eps"] is not None
            and row["estimated_eps"] is not None
            and row["actual_eps"] > row["estimated_eps"]
            and row["report_date"] in sessions
        ):
            continue
        index = sessions.index(row["report_date"])
        reaction_day = (
            row["report_date"]
            if row["timing"] == "am"
            else sessions[index + 1]
            if index + 1 < len(sessions)
            else None
        )
        if reaction_day is not None:
            reaction_rows[(reaction_day, row["symbol"])].append(row)
    event_pairs = {
        pair for pair, rows in reaction_rows.items() if len(rows) == 1
    }
    source_selection = gap._load_gzip(gap._selection_path(source))
    if (
        canonical_sha256(source_selection)
        != _read(SOURCE_MANIFEST)["private_selection"]["content_sha256"]
    ):
        raise EarningsGapContinuationError(
            "point-in-time gap selection drifted"
        )

    def phase_pairs(phase: str) -> set[tuple[str, str]]:
        value = source_selection["phases"][phase]
        return {
            (day, str(row["symbol"]))
            for day in value["dates"]
            for row in value["candidates_by_date"][day]
        }

    development_pairs = sorted(phase_pairs("development") & event_pairs)
    raw_confirmation_pairs = sorted(
        phase_pairs("confirmation") & event_pairs
    )
    indexed = outcome_exposure.read_index()
    exposed_pairs: set[tuple[str, str]] = set()
    exposed_dates: set[str] = set()
    for record in indexed:
        for day, symbol in outcome_exposure.scope_pairs(record["scope"]):
            if symbol == "*":
                exposed_dates.add(day)
            else:
                exposed_pairs.add((day, symbol))
    clean_confirmation_pairs = [
        pair
        for pair in raw_confirmation_pairs
        if pair[0] not in exposed_dates and pair not in exposed_pairs
    ]
    development_signal_dates = sorted(
        {day for day, _symbol in development_pairs}
    )
    raw_confirmation_signal_dates = sorted(
        {day for day, _symbol in raw_confirmation_pairs}
    )
    clean_confirmation_signal_dates = sorted(
        {day for day, _symbol in clean_confirmation_pairs}
    )
    confirmation_floor = 20
    blockers: list[str] = []
    if len(development_signal_dates) < 50:
        blockers.append(
            "development one-entry-per-day capacity is below 50"
        )
    if len(clean_confirmation_signal_dates) < confirmation_floor:
        blockers.append(
            "untouched confirmation one-entry-per-day capacity is below 20"
        )
    value: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "artifact_kind": "earnings-gap-event-capacity-inspection",
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "successor_id": SUCCESSOR_ID,
        "inspected_at": inspected_at,
        "collection_path": _repo_path(collection_path),
        "collection_file_sha256": sha256_file(collection_path),
        "collection_sha256": collection["collection_sha256"],
        "provider_requests_during_inspection": 0,
        "market_prices_accessed": False,
        "forward_returns_accessed": False,
        "strategy_returns_computed": 0,
        "broker_actions": 0,
        "verified_positive_reaction_pairs": len(event_pairs),
        "development_event_gap_pairs": len(development_pairs),
        "development_signal_dates": len(development_signal_dates),
        "raw_confirmation_event_gap_pairs": len(
            raw_confirmation_pairs
        ),
        "raw_confirmation_signal_dates": len(
            raw_confirmation_signal_dates
        ),
        "excluded_confirmation_pairs": (
            len(raw_confirmation_pairs)
            - len(clean_confirmation_pairs)
        ),
        "untouched_confirmation_event_gap_pairs": len(
            clean_confirmation_pairs
        ),
        "untouched_confirmation_signal_dates": len(
            clean_confirmation_signal_dates
        ),
        "maximum_one_family_entry_per_day": True,
        "configured_total_signal_floor": 50,
        "configured_confirmation_signal_floor": confirmation_floor,
        "blockers": blockers,
        "state": (
            "CAPACITY_READY"
            if not blockers
            else "INSUFFICIENT_POWER_CAPACITY"
        ),
        "confirmation_access_permitted": False,
        "valid": True,
    }
    value["inspection_sha256"] = _self_hash(value, "inspection_sha256")
    path = (
        root
        / "event-capacity-inspection"
        / f"earnings-gap-event-capacity-inspection-"
        f"{value['inspection_sha256']}.json"
    )
    _write_json(path, value)
    return path, value


def ingest_event_calendars(
    contract_path: Path,
    inspection_path: Path,
    lines: Sequence[str],
    *,
    collected_at: str,
    root: Path = DEFAULT_ROOT,
    store: HistoricalDayStore | None = None,
) -> tuple[Path, dict[str, Any]]:
    """Ingest exactly one response per frozen monthly request from stdin."""

    _timestamp(collected_at, "collected_at")
    strategy_discovery.require_committed(contract_path)
    strategy_discovery.require_committed(inspection_path)
    contract = _read(contract_path)
    inspection = _read(inspection_path)
    if not (
        inspection.get("contract_sha256") == contract.get("contract_sha256")
        and inspection.get("collection_authorized") is True
        and inspection.get("valid") is True
    ):
        raise EarningsGapContinuationError(
            "event contract inspection does not authorize collection"
        )
    responses: dict[str, Any] = {}
    for line in lines:
        if not line.strip() or line.strip() == "__END__":
            continue
        try:
            item = json.loads(line)
        except json.JSONDecodeError as exc:
            raise EarningsGapContinuationError(
                "event response line is not JSON"
            ) from exc
        if not isinstance(item, Mapping):
            raise EarningsGapContinuationError("event response line is malformed")
        start_date = str(item.get("start_date", ""))
        if start_date in responses:
            raise EarningsGapContinuationError("event response window repeats")
        responses[start_date] = item.get("response")
    expected = {str(item["start_date"]) for item in contract["requests"]}
    if set(responses) != expected:
        raise EarningsGapContinuationError(
            f"event responses cover {len(responses)} of {len(expected)} windows"
        )
    normalized: list[dict[str, Any]] = []
    provider_rows = 0
    for start_date in sorted(responses):
        raw_results = _find_results(responses[start_date])
        if raw_results is None:
            raise EarningsGapContinuationError(
                f"{start_date}: earnings calendar response is unusable"
            )
        provider_rows += len(raw_results)
        for raw in raw_results:
            if isinstance(raw, Mapping):
                row = _normalize_calendar_row(raw)
                if row is not None:
                    normalized.append(row)
    unique = {
        (
            row["symbol"],
            row["report_date"],
            row["timing"],
        ): row
        for row in normalized
    }
    if len(unique) != len(normalized):
        raise EarningsGapContinuationError("earnings calendar contains duplicates")
    rows = sorted(
        unique.values(),
        key=lambda row: (row["report_date"], row["symbol"], row["timing"]),
    )
    source = store or HistoricalDayStore.from_env()
    private = (
        source.root
        / "_derived/earnings_gap_continuation"
        / contract["contract_sha256"]
        / "event-calendar.json.gz"
    )
    payload = {
        "schema_version": SCHEMA_VERSION,
        "contract_sha256": contract["contract_sha256"],
        "inspection_sha256": inspection["inspection_sha256"],
        "collected_at": collected_at,
        "provider": contract["provider"],
        "provider_requests": len(expected) + PRIOR_DISCARDED_PROVIDER_REQUESTS,
        "effective_provider_requests": len(expected),
        "discarded_provider_requests": PRIOR_DISCARDED_PROVIDER_REQUESTS,
        "provider_rows": provider_rows,
        "market_prices_accessed": False,
        "forward_returns_accessed": False,
        "broker_actions": 0,
        "events": rows,
    }
    if private.exists() and _load_gzip(private) != payload:
        raise EarningsGapContinuationError("private event payload drifted")
    if not private.exists():
        _write_gzip(private, payload)
    positive = [
        row
        for row in rows
        if row["verified"]
        and row["actual_eps"] is not None
        and row["estimated_eps"] is not None
        and row["actual_eps"] > row["estimated_eps"]
    ]
    value: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "artifact_kind": "earnings-gap-event-collection",
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "collected_at": collected_at,
        "contract_path": _repo_path(contract_path),
        "contract_sha256": contract["contract_sha256"],
        "inspection_path": _repo_path(inspection_path),
        "inspection_sha256": inspection["inspection_sha256"],
        "provider_requests": len(expected) + PRIOR_DISCARDED_PROVIDER_REQUESTS,
        "effective_provider_requests": len(expected),
        "discarded_provider_requests": PRIOR_DISCARDED_PROVIDER_REQUESTS,
        "provider_rows": provider_rows,
        "normalized_events": len(rows),
        "verified_positive_surprises": len(positive),
        "private_payload_file_sha256": sha256_file(private),
        "private_payload_content_sha256": canonical_sha256(payload),
        "private_payload": (
            "LOCAL_HISTORICAL_DATA_ROOT/_derived/earnings_gap_continuation/"
            f"{contract['contract_sha256']}/event-calendar.json.gz"
        ),
        "market_prices_accessed": False,
        "forward_returns_accessed": False,
        "broker_actions": 0,
        "state": "EVENT_METADATA_COLLECTED_UNINSPECTED",
    }
    value["collection_sha256"] = _self_hash(value, "collection_sha256")
    path = (
        root
        / "event-collection"
        / f"earnings-gap-event-collection-{value['collection_sha256']}.json"
    )
    _write_json(path, value)
    return path, value


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    freeze = commands.add_parser("freeze-event-contract")
    freeze.add_argument("--created-at", required=True)
    inspect = commands.add_parser("inspect-event-contract")
    inspect.add_argument("contract", type=Path)
    inspect.add_argument("--inspected-at", required=True)
    ingest = commands.add_parser("ingest-event-calendars")
    ingest.add_argument("contract", type=Path)
    ingest.add_argument("inspection", type=Path)
    ingest.add_argument("--collected-at", required=True)
    normalize = commands.add_parser("freeze-event-normalization")
    normalize.add_argument("provider_contract", type=Path)
    normalize.add_argument("provider_inspection", type=Path)
    normalize.add_argument("--raw-spool", type=Path, required=True)
    normalize.add_argument("--created-at", required=True)
    inspect_normalize = commands.add_parser("inspect-event-normalization")
    inspect_normalize.add_argument("contract", type=Path)
    inspect_normalize.add_argument("--raw-spool", type=Path, required=True)
    inspect_normalize.add_argument("--inspected-at", required=True)
    ingest_normalize = commands.add_parser("ingest-normalized-events")
    ingest_normalize.add_argument("contract", type=Path)
    ingest_normalize.add_argument("inspection", type=Path)
    ingest_normalize.add_argument("--raw-spool", type=Path, required=True)
    ingest_normalize.add_argument("--collected-at", required=True)
    capacity = commands.add_parser("inspect-event-capacity")
    capacity.add_argument("collection", type=Path)
    capacity.add_argument("--inspected-at", required=True)
    return parser


def main() -> int:
    args = _parser().parse_args()
    try:
        if args.command == "freeze-event-contract":
            path, value = freeze_event_collection(created_at=args.created_at)
        elif args.command == "inspect-event-contract":
            path, value = inspect_event_collection(
                args.contract, inspected_at=args.inspected_at
            )
        elif args.command == "ingest-event-calendars":
            lines: list[str] = []
            for line in os.sys.stdin:
                lines.append(line)
                if line.strip() == "__END__":
                    break
            path, value = ingest_event_calendars(
                args.contract,
                args.inspection,
                lines,
                collected_at=args.collected_at,
            )
        elif args.command == "freeze-event-normalization":
            path, value = freeze_normalization_contract(
                args.provider_contract,
                args.provider_inspection,
                raw_spool_path=args.raw_spool,
                created_at=args.created_at,
            )
        elif args.command == "inspect-event-normalization":
            path, value = inspect_normalization_contract(
                args.contract,
                raw_spool_path=args.raw_spool,
                inspected_at=args.inspected_at,
            )
        elif args.command == "ingest-normalized-events":
            path, value = ingest_normalized_events(
                args.contract,
                args.inspection,
                raw_spool_path=args.raw_spool,
                collected_at=args.collected_at,
            )
        else:
            path, value = inspect_event_capacity(
                args.collection,
                inspected_at=args.inspected_at,
            )
        print(
            json.dumps(
                {
                    "path": _repo_path(path),
                    "state": value.get(
                        "state", value.get("artifact_kind")
                    ),
                    "sha256": value.get(
                        "collection_sha256",
                        value.get(
                            "inspection_sha256", value.get("contract_sha256")
                        ),
                    ),
                    "provider_requests": value.get(
                        "provider_requests",
                        value.get("logical_provider_requests", 0),
                    ),
                    "market_prices_accessed": False,
                    "forward_returns_accessed": False,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    except (
        EarningsGapContinuationError,
        OSError,
        outcome_exposure.OutcomeExposureError,
        strategy_discovery.StrategyDiscoveryError,
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


if __name__ == "__main__":
    raise SystemExit(main())

"""Freeze the point-in-time activist-issuer earnings-reaction family.

This module is deliberately outcome blind.  It combines already-inspected
Schedule 13D issuer identity with later SEC 8-K Item 2.02 acceptance metadata,
reserves exact event-level development and confirmation scopes, and writes the
row-level event inventory only to the ignored historical-data store.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import subprocess
from bisect import bisect_left, bisect_right
from collections import defaultdict
from collections.abc import Mapping, Sequence
from datetime import datetime, time, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import outcome_exposure
from historical_store import HistoricalStoreConfig
from learning_data import freeze_dataset_contract
from learning_experiment import DEVELOPMENT_SEARCH_RULE, validate_hypothesis_contract


PROJECT_ROOT = Path(__file__).resolve().parent
FAMILY_ID = "activist-issuer-earnings-reaction-continuation"
EXPERIMENT_ID = f"{FAMILY_ID}-v1-development-search"
CAMPAIGN_ID = "multi-strategy-portfolio-validation-v2"
FAMILY_ROOT = (
    PROJECT_ROOT / "strategy_tournament/v2/continuous" / EXPERIMENT_ID
)
CAPACITY_ROOT = FAMILY_ROOT / "capacity-contract"
INSPECTION_ROOT = FAMILY_ROOT / "capacity-inspection"
MANIFEST_ROOT = FAMILY_ROOT / "capacity"
CONTRACT_ROOT = FAMILY_ROOT / "family-contract"
PRIVATE_NAMESPACE = Path("_derived/activist_earnings_capacity")
SEMANTIC_RELATIVE_PATH = Path(
    "_derived/schedule13d_capacity/"
    "dataset-schedule-13d-capacity-2026-07-22-v1/semantic/"
    "semantic-capacity.json.gz"
)
SYMBOL_RELATIVE_PATH = Path(
    "_derived/schedule13d_capacity/"
    "dataset-schedule-13d-capacity-2026-07-22-v1/symbols/documents/"
    "symbol-resolution.json"
)
SUBMISSIONS_RELATIVE_ROOT = Path(
    "_sources/sec/"
    "schedule13d-issuer-submissions-v1/submissions"
)
CALENDAR_PATHS = (
    Path(
        "historical_batches/continuous_v2/"
        "session-calendar-2014-01-through-2022-12.json"
    ),
    Path(
        "historical_batches/challenger_orb_retest_v1/"
        "session-calendar-2023-01-through-2026-07.json"
    ),
)
SOURCE_INSPECTIONS = (
    Path(
        "strategy_tournament/v2/schedule13d/semantic/inspections/"
        "schedule-13d-activist-continuation-v1-semantic-"
        "9973ee5a49484f2ec041e439c8310f622f8eedb7be431bfff35039e7be1f7522.json"
    ),
    Path(
        "strategy_tournament/v2/schedule13d/symbols/inspections/"
        "schedule-13d-activist-continuation-v1-capacity-"
        "7459f2d37566019dbaef074ac2468102f8287d9ec1b76e00e272085f5f1c98c6.json"
    ),
    Path(
        "strategy_tournament/v2/schedule13d/symbols/submissions/inspections/"
        "schedule-13d-activist-continuation-v1-submissions-"
        "ced65dcd2ce7a536a55d3189ea50c58093af4ca3a5244b9a56b1994cbebce518.json"
    ),
)
DEVELOPMENT_START = "2022-02-23"
DEVELOPMENT_END = "2023-08-31"
EMBARGO_START = "2023-09-01"
EMBARGO_END = "2023-09-08"
CONFIRMATION_START = "2023-09-11"
CONFIRMATION_END = "2024-12-31"
NEW_YORK = ZoneInfo("America/New_York")


class ActivistEarningsDiscoveryError(RuntimeError):
    """A source, boundary, or content-addressed artifact is invalid."""


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


def _file_hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _relative(path: Path) -> str:
    return str(path.resolve().relative_to(PROJECT_ROOT.resolve()))


def _require_committed(path: Path) -> None:
    relative = _relative(path)
    tracked = subprocess.run(
        ["git", "ls-files", "--error-unmatch", "--", relative],
        cwd=PROJECT_ROOT,
        capture_output=True,
    )
    clean = subprocess.run(
        ["git", "diff", "--quiet", "HEAD", "--", relative],
        cwd=PROJECT_ROOT,
    )
    if tracked.returncode or clean.returncode:
        raise ActivistEarningsDiscoveryError(
            f"predecessor must be committed and unchanged: {relative}"
        )


def _write_artifact(
    payload: Mapping[str, Any], directory: Path, stem: str
) -> tuple[Path, dict[str, Any]]:
    content = dict(payload)
    content.pop("artifact_sha256", None)
    digest = _hash(content)
    artifact = {**content, "artifact_sha256": digest}
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f"{stem}-{digest}.json"
    rendered = json.dumps(artifact, indent=2, sort_keys=True) + "\n"
    if path.exists() and path.read_text(encoding="utf-8") != rendered:
        raise ActivistEarningsDiscoveryError(
            "content-addressed artifact has other content"
        )
    if not path.exists():
        temporary = path.with_suffix(".json.tmp")
        temporary.write_text(rendered, encoding="utf-8")
        temporary.replace(path)
    return path, artifact


def _load_artifact(path: Path, kind: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ActivistEarningsDiscoveryError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise ActivistEarningsDiscoveryError("artifact must contain an object")
    supplied = value.get("artifact_sha256")
    content = {key: item for key, item in value.items() if key != "artifact_sha256"}
    expected = _hash(content)
    if (
        supplied != expected
        or not path.name.endswith(f"-{expected}.json")
        or value.get("artifact_kind") != kind
    ):
        raise ActivistEarningsDiscoveryError("artifact hash, name, or kind is invalid")
    return value


def _parse_timestamp(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=NEW_YORK)
    return parsed.astimezone(timezone.utc)


def _load_calendar() -> list[str]:
    dates: set[str] = set()
    for relative in CALENDAR_PATHS:
        rows = json.loads((PROJECT_ROOT / relative).read_text(encoding="utf-8"))
        dates.update(str(row["date"]) for row in rows)
    result = sorted(dates)
    if len(result) != len(dates):
        raise ActivistEarningsDiscoveryError("calendar dates are not unique")
    return result


def _load_gzip_object(path: Path) -> dict[str, Any]:
    with gzip.open(path, "rt", encoding="utf-8") as source:
        value = json.load(source)
    if not isinstance(value, dict):
        raise ActivistEarningsDiscoveryError(f"{path} must contain an object")
    return value


def _historical_path(relative: Path) -> Path:
    return HistoricalStoreConfig.from_env().root / relative


def _identity_intervals() -> dict[str, list[dict[str, str]]]:
    semantic_rows = _load_gzip_object(
        _historical_path(SEMANTIC_RELATIVE_PATH)
    )["records"]
    semantic_by_ordinal = {int(row["ordinal"]): row for row in semantic_rows}
    symbol_rows = json.loads(
        _historical_path(SYMBOL_RELATIVE_PATH).read_text(encoding="utf-8")
    )["records"]
    identities: list[dict[str, str]] = []
    for row in symbol_rows:
        symbols = row.get("symbols")
        if row.get("verified_event") is not True or not (
            isinstance(symbols, list) and len(symbols) == 1
        ):
            continue
        event = semantic_by_ordinal[int(row["event_ordinal"])]
        identities.append(
            {
                "subject_cik": str(int(row["subject_cik"])),
                "symbol": str(symbols[0]),
                "accepted_at": str(event["accepted_at"]),
                "accession": str(event["accession"]),
                "identity_source": "RECOVERED_CAUSAL_DEI_TRADING_SYMBOL",
            }
        )
    for event in semantic_rows:
        if event.get("verified_event") is True and event.get("event_symbol"):
            identities.append(
                {
                    "subject_cik": str(int(event["subject_cik"])),
                    "symbol": str(event["event_symbol"]),
                    "accepted_at": str(event["accepted_at"]),
                    "accession": str(event["accession"]),
                    "identity_source": "VERIFIED_EVENT_EVENT_FILING_SYMBOL",
                }
            )
    by_cik: dict[str, list[dict[str, str]]] = defaultdict(list)
    for row in identities:
        by_cik[row["subject_cik"]].append(row)
    result: dict[str, list[dict[str, str]]] = {}
    for cik, rows in by_cik.items():
        deduplicated = {
            (row["accepted_at"], row["accession"], row["symbol"]): row for row in rows
        }
        result[cik] = sorted(
            deduplicated.values(),
            key=lambda row: (
                _parse_timestamp(row["accepted_at"]),
                row["accession"],
                row["symbol"],
            ),
        )
    return result


def _active_identity(
    intervals: Sequence[Mapping[str, str]], accepted_at: str
) -> Mapping[str, str] | None:
    accepted = _parse_timestamp(accepted_at)
    eligible = [
        row for row in intervals if _parse_timestamp(str(row["accepted_at"])) < accepted
    ]
    return eligible[-1] if eligible else None


def _exposure_sets() -> tuple[set[tuple[str, str]], set[str]]:
    pairs: set[tuple[str, str]] = set()
    wildcard_dates: set[str] = set()
    for record in outcome_exposure.read_index():
        scope = record["scope"]
        if "symbols" in scope:
            if scope["symbols"] == ["*"]:
                wildcard_dates.update(scope["dates"])
            else:
                pairs.update(
                    (day, symbol)
                    for day in scope["dates"]
                    for symbol in scope["symbols"]
                )
        else:
            for day in scope["dates"]:
                pairs.update(
                    (day, symbol) for symbol in scope["symbols_by_date"][day]
                )
    return pairs, wildcard_dates


def derive_inventory() -> dict[str, Any]:
    calendar = _load_calendar()
    intervals = _identity_intervals()
    raw_events: list[dict[str, Any]] = []
    intraday_excluded = 0
    missing_submission_inputs = 0
    for cik, identity_rows in sorted(intervals.items()):
        submission_path = (
            _historical_path(SUBMISSIONS_RELATIVE_ROOT)
            / f"CIK{int(cik):010d}.json"
        )
        if not submission_path.is_file():
            missing_submission_inputs += 1
            continue
        recent = json.loads(
            submission_path.read_text(encoding="utf-8")
        )["filings"]["recent"]
        for index, form in enumerate(recent["form"]):
            items = [item.strip() for item in str(recent["items"][index]).split(",")]
            if form != "8-K" or "2.02" not in items:
                continue
            accepted_at = str(recent["acceptanceDateTime"][index])
            identity = _active_identity(identity_rows, accepted_at)
            if identity is None:
                continue
            accepted_et = _parse_timestamp(accepted_at).astimezone(NEW_YORK)
            clock = accepted_et.timetz().replace(tzinfo=None)
            accepted_day = accepted_et.date().isoformat()
            if clock < time(9, 30):
                position = bisect_left(calendar, accepted_day)
                timing = "pre_market"
            elif clock >= time(16, 0):
                position = bisect_right(calendar, accepted_day)
                timing = "after_market"
            else:
                intraday_excluded += 1
                continue
            if position >= len(calendar) or position + 5 >= len(calendar):
                continue
            reaction_date = calendar[position]
            raw_events.append(
                {
                    "subject_cik": cik,
                    "symbol": identity["symbol"],
                    "activist_start_accepted_at": identity["accepted_at"],
                    "activist_start_accession": identity["accession"],
                    "identity_source": identity["identity_source"],
                    "accepted_at": accepted_at,
                    "accepted_at_et": accepted_et.isoformat(),
                    "accession": str(recent["accessionNumber"][index]),
                    "filing_date": str(recent["filingDate"][index]),
                    "report_date": str(recent["reportDate"][index]),
                    "primary_document": str(recent["primaryDocument"][index]),
                    "items": items,
                    "timing": timing,
                    "reaction_date": reaction_date,
                    "entry_date": calendar[position + 1],
                    "scope_dates": calendar[position : position + 6],
                }
            )
    deduplicated: dict[tuple[str, str], dict[str, Any]] = {}
    for event in raw_events:
        key = (event["symbol"], event["reaction_date"])
        current = deduplicated.get(key)
        if current is None or (
            _parse_timestamp(event["accepted_at"]),
            event["accession"],
        ) < (
            _parse_timestamp(current["accepted_at"]),
            current["accession"],
        ):
            deduplicated[key] = event
    events = sorted(
        deduplicated.values(),
        key=lambda row: (row["reaction_date"], row["symbol"], row["accession"]),
    )
    pairs, wildcard_dates = _exposure_sets()
    for event in events:
        event["globally_exposed_before_freeze"] = any(
            day in wildcard_dates or (day, event["symbol"]) in pairs
            for day in event["scope_dates"]
        )
    development = [
        row
        for row in events
        if DEVELOPMENT_START <= row["entry_date"] <= DEVELOPMENT_END
        and row["scope_dates"][-1] <= DEVELOPMENT_END
    ]
    confirmation_all = [
        row
        for row in events
        if CONFIRMATION_START <= row["entry_date"] <= CONFIRMATION_END
        and row["scope_dates"][-1] <= CONFIRMATION_END
    ]
    confirmation_clean = [
        row
        for row in confirmation_all
        if row["globally_exposed_before_freeze"] is False
    ]
    return {
        "schema_version": 1,
        "family_id": FAMILY_ID,
        "experiment_id": EXPERIMENT_ID,
        "source_semantics": {
            "activist_identity": (
                "first observable verified Schedule 13D issuer symbol, with later "
                "verified symbol changes treated as point-in-time intervals"
            ),
            "earnings_event": "SEC form exactly 8-K with exact items token 2.02",
            "acceptance_timezone": "SEC UTC converted to America/New_York",
            "pre_market": "reaction on same or next available session",
            "after_market": "reaction on next available session",
            "regular_session_acceptance": "excluded without substitution",
            "duplicate_rule": (
                "same symbol and reaction date keeps earliest acceptance then accession"
            ),
            "maximum_scope": "reaction session plus next five sessions",
        },
        "counts": {
            "verified_identity_events": sum(len(rows) for rows in intervals.values()),
            "verified_identity_ciks": len(intervals),
            "item_202_events_after_activist_before_time_filter": (
                len(raw_events) + intraday_excluded
            ),
            "eligible_non_rth_item_202_events": len(raw_events),
            "regular_session_events_excluded": intraday_excluded,
            "deduplicated_events": len(events),
            "missing_submission_inputs": missing_submission_inputs,
            "development_events": len(development),
            "development_entry_dates": len(
                {row["entry_date"] for row in development}
            ),
            "development_symbols": len({row["symbol"] for row in development}),
            "development_clean_events": sum(
                row["globally_exposed_before_freeze"] is False
                for row in development
            ),
            "confirmation_events_before_exposure_filter": len(confirmation_all),
            "confirmation_clean_events": len(confirmation_clean),
            "confirmation_clean_entry_dates": len(
                {row["entry_date"] for row in confirmation_clean}
            ),
            "confirmation_clean_symbols": len(
                {row["symbol"] for row in confirmation_clean}
            ),
        },
        "development_events": development,
        "confirmation_clean_events": confirmation_clean,
        "confirmation_excluded_events": [
            row for row in confirmation_all if row["globally_exposed_before_freeze"]
        ],
        "market_prices_accessed": False,
        "returns_computed": False,
        "provider_requests": 0,
        "broker_actions": 0,
    }


def _write_private_inventory(inventory: Mapping[str, Any]) -> dict[str, Any]:
    content_sha256 = _hash(inventory)
    store = HistoricalStoreConfig.from_env()
    relative = PRIVATE_NAMESPACE / content_sha256 / "events.json.gz"
    path = store.root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        temporary = path.with_suffix(".json.gz.tmp")
        with temporary.open("wb") as target:
            with gzip.GzipFile(filename="", mode="wb", fileobj=target, mtime=0) as stream:
                stream.write(_canonical(inventory))
        temporary.replace(path)
    return {
        "storage": "LOCAL_HISTORICAL_DATA_ROOT",
        "relative_path": str(relative),
        "content_sha256": content_sha256,
        "file_sha256": _file_hash(path),
        "compression": "gzip",
        "format": "canonical-json",
    }


def _source_hashes() -> dict[str, Any]:
    submission_files = sorted(
        _historical_path(SUBMISSIONS_RELATIVE_ROOT).glob("CIK*.json")
    )
    submission_index = [
        {
            "name": path.name,
            "sha256": _file_hash(path),
            "size": path.stat().st_size,
        }
        for path in submission_files
    ]
    return {
        "semantic_sha256": _file_hash(
            _historical_path(SEMANTIC_RELATIVE_PATH)
        ),
        "symbol_resolution_sha256": _file_hash(
            _historical_path(SYMBOL_RELATIVE_PATH)
        ),
        "submission_file_count": len(submission_files),
        "submission_index_sha256": _hash(submission_index),
        "calendar_sha256": _hash(
            {
                str(path): _file_hash(PROJECT_ROOT / path)
                for path in CALENDAR_PATHS
            }
        ),
        "outcome_exposure_index_sha256": _file_hash(
            outcome_exposure.DEFAULT_INDEX
        ),
    }


def freeze_capacity(created_at: str) -> tuple[Path, dict[str, Any]]:
    for path in (*SOURCE_INSPECTIONS, *CALENDAR_PATHS):
        _require_committed(PROJECT_ROOT / path)
    _require_committed(outcome_exposure.DEFAULT_INDEX)
    inventory = derive_inventory()
    binding = _write_private_inventory(inventory)
    payload = {
        "schema_version": 1,
        "artifact_kind": "activist-earnings-capacity-contract",
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "experiment_id": EXPERIMENT_ID,
        "created_at": created_at,
        "state": "CAPACITY_FROZEN_UNINSPECTED",
        "source_hashes": _source_hashes(),
        "source_inspections": [str(path) for path in SOURCE_INSPECTIONS],
        "event_inventory_binding": binding,
        "counts": inventory["counts"],
        "partitions": {
            "development": [DEVELOPMENT_START, DEVELOPMENT_END],
            "embargo": [EMBARGO_START, EMBARGO_END],
            "confirmation": [CONFIRMATION_START, CONFIRMATION_END],
        },
        "development_contamination_policy": (
            "explicitly contaminated training only; cannot satisfy confirmation"
        ),
        "confirmation_selection": (
            "event retained only when symbol and every reaction-through-five-day "
            "scope pair is absent from the global outcome-exposure index"
        ),
        "market_prices_accessed": False,
        "returns_computed": False,
        "provider_requests": 0,
        "broker_actions": 0,
    }
    return _write_artifact(payload, CAPACITY_ROOT, "capacity-contract")


def inspect_capacity(
    capacity_path: Path, inspected_at: str
) -> tuple[Path, dict[str, Any]]:
    _require_committed(capacity_path)
    frozen = _load_artifact(
        capacity_path, "activist-earnings-capacity-contract"
    )
    rebuilt = derive_inventory()
    rebuilt_binding = _write_private_inventory(rebuilt)
    if (
        frozen["source_hashes"] != _source_hashes()
        or frozen["counts"] != rebuilt["counts"]
        or frozen["event_inventory_binding"] != rebuilt_binding
    ):
        raise ActivistEarningsDiscoveryError(
            "capacity source, inventory, or partition drifted"
        )
    counts = rebuilt["counts"]
    ready = (
        counts["development_events"] >= 50
        and counts["confirmation_clean_events"] >= 20
        and counts["confirmation_clean_entry_dates"] >= 20
        and counts["development_clean_events"] == 0
    )
    payload = {
        "schema_version": 1,
        "artifact_kind": "activist-earnings-capacity-inspection",
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "experiment_id": EXPERIMENT_ID,
        "inspected_at": inspected_at,
        "state": "CAPACITY_READY" if ready else "INSUFFICIENT_POWER_CAPACITY",
        "capacity_contract_path": _relative(capacity_path),
        "capacity_contract_sha256": frozen["artifact_sha256"],
        "source_hashes": frozen["source_hashes"],
        "event_inventory_binding": rebuilt_binding,
        "counts": counts,
        "inspection": {
            "artifact_hash_rebuilt": True,
            "source_hashes_rebuilt": True,
            "identity_intervals_rebuilt": True,
            "sec_item_202_filter_rebuilt": True,
            "timestamp_partition_rebuilt": True,
            "deduplication_rebuilt": True,
            "global_exposure_filter_rebuilt_once": True,
            "development_contamination_rebuilt": True,
            "confirmation_capacity_rebuilt": True,
            "valid": ready,
        },
        "market_prices_accessed": False,
        "returns_computed": False,
        "provider_requests": 0,
        "broker_actions": 0,
    }
    return _write_artifact(payload, INSPECTION_ROOT, "capacity-inspection")


def _calendar_slice(start: str, end: str) -> list[str]:
    return [day for day in _load_calendar() if start <= day <= end]


def _scope(events: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    symbols_by_date: dict[str, set[str]] = defaultdict(set)
    for event in events:
        for day in event["scope_dates"]:
            symbols_by_date[str(day)].add(str(event["symbol"]))
    dates = sorted(symbols_by_date)
    return {
        "dates": dates,
        "symbols_by_date": {
            day: sorted(symbols_by_date[day]) for day in dates
        },
    }


def freeze_family(
    inspection_path: Path, created_at: str
) -> tuple[Path, Path]:
    _require_committed(inspection_path)
    inspection = _load_artifact(
        inspection_path, "activist-earnings-capacity-inspection"
    )
    if inspection["state"] != "CAPACITY_READY":
        raise ActivistEarningsDiscoveryError(
            f"capacity is not ready: {inspection['state']}"
        )
    inventory = derive_inventory()
    if _write_private_inventory(inventory) != inspection["event_inventory_binding"]:
        raise ActivistEarningsDiscoveryError("inspected private inventory drifted")
    development_dates = _calendar_slice(DEVELOPMENT_START, DEVELOPMENT_END)
    embargo_dates = _calendar_slice(EMBARGO_START, EMBARGO_END)
    confirmation_dates = _calendar_slice(CONFIRMATION_START, CONFIRMATION_END)
    development_events = inventory["development_events"]
    confirmation_events = inventory["confirmation_clean_events"]
    evidence_paths = [
        _relative(inspection_path),
        *[str(path) for path in SOURCE_INSPECTIONS],
        *[str(path) for path in CALENDAR_PATHS],
        _relative(outcome_exposure.DEFAULT_INDEX),
    ]
    dataset = {
        "schema_version": 1,
        "dataset_id": f"dataset-{EXPERIMENT_ID}-capacity",
        "registered_at": created_at,
        "requested_dates": development_dates,
        "dataset_payload": {
            "lane": "development",
            "claim_scope": "DEVELOPMENT_ONLY",
            "evidence_paths": evidence_paths,
            "inspected": True,
            "point_in_time_evidence": True,
            "activist_earnings_capacity": {
                "family_id": FAMILY_ID,
                "experiment_id": EXPERIMENT_ID,
                "capacity_inspection_path": _relative(inspection_path),
                "capacity_inspection_sha256": inspection["artifact_sha256"],
                "private_inventory": inspection["event_inventory_binding"],
                "development_event_pairs": len(development_events),
                "development_entry_dates": len(
                    {row["entry_date"] for row in development_events}
                ),
                "confirmation_clean_event_pairs": len(confirmation_events),
                "confirmation_clean_entry_dates": len(
                    {row["entry_date"] for row in confirmation_events}
                ),
                "provider_requests": 0,
                "confirmation_access_permitted": False,
            },
        },
    }
    manifest_path, _manifest = freeze_dataset_contract(dataset, MANIFEST_ROOT)
    parameter_grid = {
        "maximum_hold_sessions": [2, 5],
        "minimum_close_location": [0.50, 0.75],
        "minimum_reaction_opening_gap_fraction": [0.02, 0.04],
        "reaction_confirmation": ["close>open", "close>prior_close"],
        "stop_atr14": [1.0, 1.5],
    }
    contract = {
        "schema_version": 1,
        "campaign_id": CAMPAIGN_ID,
        "experiment_id": EXPERIMENT_ID,
        "family_id": FAMILY_ID,
        "mechanism_family": FAMILY_ID,
        "strategy_id": FAMILY_ID,
        "created_at": created_at,
        "status": "INVENTED",
        "dataset_lane": "development",
        "selection_mode": "development_search",
        "winner_selection": DEVELOPMENT_SEARCH_RULE,
        "mechanism": (
            "After an issuer enters a point-in-time verified Schedule 13D activist "
            "universe, a later SEC Item 2.02 earnings reaction may continue because "
            "activist pressure can prolong repricing and capital-allocation demand."
        ),
        "expected_holding_behavior": (
            "Enter the session after a completed positive earnings reaction and hold "
            "at most five sessions under an ATR structural stop."
        ),
        "entry_rule": (
            "For pre-market or after-market Item 2.02 filings only, observe the full "
            "reaction session; require frozen gap, confirmation, close-location, "
            "price, liquidity, and cost-floor gates; rank one issuer per day; enter "
            "the next session open."
        ),
        "stop_rule": (
            "Stop 1.0 or 1.5 ATR14 below entry; nonpositive, missing, or otherwise "
            "invalid structural stops are rejected; same-interval ambiguity is "
            "stop-first."
        ),
        "exit_rule": (
            "Exit at the stop or the close of the second or fifth session, with a "
            "five-session absolute maximum."
        ),
        "ranking_rule": (
            "Higher reaction opening gap, then higher close location, then higher "
            "reaction dollar volume, then canonical symbol."
        ),
        "selection_rule": (
            "Evaluate every frozen trial through rolling-origin OOF account paths and "
            "apply the repository development-search winner rule exactly."
        ),
        "primary_outcome": (
            "Selection-adjusted chronological net account growth after 5/10/20-bps "
            "per-side costs."
        ),
        "material_difference_rationale": (
            "This combines a verified activist issuer-state mechanism with actual "
            "SEC Item 2.02 acceptance timestamps and completed reaction bars; it is "
            "not the retired Schedule 13D disclosure-day continuation rule or the "
            "retired 10-Q EPS fact family."
        ),
        "parameter_grid": parameter_grid,
        "development_dates": development_dates,
        "development_signal_dates": sorted(
            {row["entry_date"] for row in development_events}
        ),
        "embargo_dates": embargo_dates,
        "confirmation_dates": confirmation_dates,
        "confirmation_signal_dates": sorted(
            {row["entry_date"] for row in confirmation_events}
        ),
        "confirmation_signal_capacity": len(
            {row["entry_date"] for row in confirmation_events}
        ),
        "development_scope": _scope(development_events),
        "confirmation_scope": _scope(confirmation_events),
        "universe": sorted(
            {
                row["symbol"]
                for row in [*development_events, *confirmation_events]
            }
        ),
        "universe_requirements": {
            "security_type": "verified long common equity only",
            "prior_close_minimum": 10.0,
            "prior_20_session_median_dollar_volume_minimum": 50_000_000,
            "identity": "point-in-time verified Schedule 13D issuer symbol interval",
        },
        "execution_assumptions": {
            "direction": "long_only",
            "entry": "next session open after full reaction close",
            "maximum_new_entries_per_family_per_day": 1,
            "maximum_hold_sessions": 5,
            "same_interval_ambiguity": "stop_first",
            "missing_data": "missed_trade_no_substitution",
            "minimum_expected_gross_to_primary_round_trip_cost": 5.0,
        },
        "falsification_criteria": {
            "selection_aware_gates": "DEVELOPMENT_SEARCH_RULE",
            "stress_growth": "positive at 20 bps per side",
            "stress_profit_factor": "at least 1.20",
            "drawdown": "at most 6R",
            "concentration": "positive without five best trades",
            "chronology": "both halves positive",
        },
        "falsifiers": {
            "mechanism": "fails selection-aware development",
            "confirmation": "exact frozen winner fails untouched evidence",
            "implementation": "any rule, capture, or hash violation",
        },
        "minimum_evidence": {
            "required_total_signals": "max(50, frozen power target)",
            "required_confirmation_signals": (
                "max(20, ceil(required_total_signals * 0.30))"
            ),
            "confidence": "one-sided stationary-bootstrap 90 percent",
        },
        "partitions": {
            "rolling_origin": True,
            "confirmation_untouched": True,
            "embargo_sessions": 5,
            "development_contamination": "explicit_training_only",
        },
        "costs_bps_per_side": [5, 10, 20],
        "capacity_policy": {"retire_below": 50, "fast_lane_at": 100},
        "capacity_manifest": _relative(manifest_path),
        "implementation_files": [
            "activist_earnings_discovery.py",
            "activist_earnings_plugin.py",
            "dense_strategy_runtime.py",
            "strategy_discovery.py",
        ],
        "plugin": {
            "module": "activist_earnings_plugin",
            "preflight": "preflight",
            "evaluate_development": "evaluate_development",
            "evaluate_confirmation": "evaluate_confirmation",
            "evaluate_production": "evaluate_production",
        },
        "contamination_risks": [
            (
                "Every development event scope is already globally exposed and is "
                "restricted to contaminated training."
            ),
            (
                "Confirmation is event-wise reserved and must be rechecked against "
                "the global outcome-exposure index before access."
            ),
        ],
        "production_compatibility_risks": [
            "SEC acceptance and filing identity must be fresh and point-in-time.",
            "Overnight positions require confirmed GTC protection and gap-risk sizing.",
            "Sparse signals may make the frozen power target infeasible.",
        ],
    }
    validated = validate_hypothesis_contract(contract)
    content = dict(validated)
    digest = _hash(content)
    CONTRACT_ROOT.mkdir(parents=True, exist_ok=True)
    contract_path = CONTRACT_ROOT / f"contract-{digest}.json"
    rendered = json.dumps(content, indent=2, sort_keys=True) + "\n"
    if contract_path.exists() and contract_path.read_text(encoding="utf-8") != rendered:
        raise ActivistEarningsDiscoveryError("family contract content collision")
    if not contract_path.exists():
        temporary = contract_path.with_suffix(".json.tmp")
        temporary.write_text(rendered, encoding="utf-8")
        temporary.replace(contract_path)
    return manifest_path, contract_path


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    freeze = subparsers.add_parser("freeze-capacity")
    freeze.add_argument("--created-at", required=True)
    inspect = subparsers.add_parser("inspect-capacity")
    inspect.add_argument("capacity", type=Path)
    inspect.add_argument("--inspected-at", required=True)
    family = subparsers.add_parser("freeze-family")
    family.add_argument("inspection", type=Path)
    family.add_argument("--created-at", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    if args.command == "freeze-capacity":
        path, value = freeze_capacity(args.created_at)
        result: Any = {"path": _relative(path), **value}
    elif args.command == "inspect-capacity":
        path, value = inspect_capacity(args.capacity, args.inspected_at)
        result = {"path": _relative(path), **value}
    else:
        manifest, contract = freeze_family(args.inspection, args.created_at)
        result = {
            "manifest": _relative(manifest),
            "family_contract": _relative(contract),
            "market_prices_accessed": False,
            "provider_requests": 0,
            "broker_actions": 0,
        }
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

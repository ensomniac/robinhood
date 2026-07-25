"""Freeze and collect outcome-blind SEC quarterly EPS-event capacity.

This source lane deliberately stops before market-price or forward-return
access.  It uses the SEC Financial Statement and Notes archives because those
archives contain both as-filed numeric facts and point-in-time cover facts such
as TradingSymbol and SecurityExchangeName.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import io
import json
import os
import re
import time
import zipfile
from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from datetime import date, datetime
from pathlib import Path
from typing import Any

import requests

import outcome_exposure
import strategy_discovery
from historical_discovery import SecConfig
from historical_store import HistoricalDayStore, sha256_file


PROJECT_ROOT = Path(__file__).resolve().parent
CAMPAIGN_ID = "multi-strategy-portfolio-validation-v2"
FAMILY_ID = "earnings-positive-surprise-drift"
SUCCESSOR_ID = "earnings-positive-surprise-drift-v5-sec-yoy-eps-reaction"
DEFAULT_ROOT = PROJECT_ROOT / "strategy_tournament/v2/continuous" / SUCCESSOR_ID
PRIVATE_NAMESPACE = "_derived/earnings_sec_eps_capacity"
DATASET_PAGE = (
    "https://www.sec.gov/data-research/sec-markets-data/"
    "financial-statement-notes-data-sets"
)
DOCUMENTATION_URL = "https://www.sec.gov/dera/data/fsnds.pdf"
ARCHIVE_ROOT = (
    "https://www.sec.gov/files/dera/data/"
    "financial-statement-notes-data-sets"
)
DEVELOPMENT_START = "2010-01-01"
DEVELOPMENT_END = "2010-12-17"
EMBARGO_START = "2010-12-18"
EMBARGO_END = "2011-01-09"
CONFIRMATION_START = "2011-01-10"
CONFIRMATION_END = "2011-12-31"
MINIMUM_UNIQUE_EVENTS = 50
MINIMUM_DEVELOPMENT_DATES = 30
MINIMUM_CONFIRMATION_DATES = 20
MINIMUM_SPACING_SECONDS = 0.20
CSV_FIELD_SIZE_LIMIT = 2**31 - 1
TICKER_PATTERN = re.compile(r"^[A-Z][A-Z0-9.-]{0,9}$")
COMMON_TITLE_PATTERN = re.compile(r"\bcommon\b", re.IGNORECASE)
DISALLOWED_TITLE_PATTERN = re.compile(
    r"\b(?:preferred|preference|depositary|warrant|right|unit|note|bond|debt)\b",
    re.IGNORECASE,
)
ALLOWED_EXCHANGES = {
    "NASDAQ",
    "NASDAQ GLOBAL MARKET",
    "NASDAQ CAPITAL MARKET",
    "NASDAQ GLOBAL SELECT MARKET",
    "NYSE",
    "NYSE AMERICAN",
    "NYSE ARCA",
    "NEW YORK STOCK EXCHANGE",
}
EPS_TAGS = (
    "EarningsPerShareDiluted",
    "EarningsPerShareBasicAndDiluted",
    "EarningsPerShareBasic",
)
ARCHIVES = (
    ("2010q1", "2010q1_notes_1.zip"),
    ("2010q2", "2010q2_notes_0.zip"),
    ("2010q3", "2010q3_notes_0.zip"),
    ("2010q4", "2010q4_notes.zip"),
    ("2011q1", "2011q1_notes.zip"),
    ("2011q2", "2011q2_notes.zip"),
    ("2011q3", "2011q3_notes.zip"),
    ("2011q4", "2011q4_notes.zip"),
)


class EarningsSecEpsCapacityError(RuntimeError):
    """The frozen SEC metadata graph or derived event denominator drifted."""


def canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode()


def self_hash(value: Mapping[str, Any], field: str) -> str:
    return hashlib.sha256(
        canonical_bytes({key: item for key, item in value.items() if key != field})
    ).hexdigest()


def _read(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise EarningsSecEpsCapacityError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise EarningsSecEpsCapacityError(f"{path} must contain an object")
    return value


def _write(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")


def _write_private(path: Path, value: Mapping[str, Any]) -> bytes:
    raw = gzip.compress(canonical_bytes(value), mtime=0)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_bytes(raw)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)
    return raw


def _timestamp(value: str, name: str) -> str:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise EarningsSecEpsCapacityError(f"{name} must be ISO-8601") from exc
    if parsed.tzinfo is None:
        raise EarningsSecEpsCapacityError(f"{name} must include a timezone")
    return parsed.isoformat().replace("+00:00", "Z")


def _repo_path(path: Path) -> str:
    return str(path.resolve().relative_to(PROJECT_ROOT.resolve()))


def _requests() -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    for ordinal, (quarter, filename) in enumerate(ARCHIVES):
        request: dict[str, Any] = {
            "ordinal": ordinal,
            "quarter": quarter,
            "method": "GET",
            "url": f"{ARCHIVE_ROOT}/{filename}",
            "filename": filename,
        }
        request["request_sha256"] = hashlib.sha256(
            canonical_bytes(request)
        ).hexdigest()
        result.append(request)
    return result


def build_contract(*, created_at: str) -> dict[str, Any]:
    for path in (
        Path(__file__).resolve(),
        PROJECT_ROOT / "earnings_sec_eps_capacity_inspection.py",
    ):
        strategy_discovery.require_committed(path)
    value: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": "earnings-sec-fsnds-eps-capacity-contract",
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "successor_id": SUCCESSOR_ID,
        "created_at": _timestamp(created_at, "created_at"),
        "provider": "U.S. SEC Financial Statement and Notes Data Sets",
        "dataset_page": DATASET_PAGE,
        "documentation_url": DOCUMENTATION_URL,
        "requests": _requests(),
        "authorized_provider_requests": len(ARCHIVES),
        "request_policy": {
            "exact_archives_only": True,
            "minimum_spacing_seconds": MINIMUM_SPACING_SECONDS,
            "retries_permitted": 0,
            "substitutions_permitted": 0,
            "interrupted_collection_may_reuse_hash_valid_zip": True,
            "unexpected_archive_members_fail_closed": True,
        },
        "event_semantics": {
            "forms": ["10-Q"],
            "amendments_excluded": True,
            "as_filed_acceptance_timestamp_required": True,
            "entry_observation_boundary": (
                "first regular-session open strictly after SEC acceptance"
            ),
            "identity_source": (
                "same accession TXT TradingSymbol, SecurityExchangeName, "
                "and Security12bTitle facts grouped by dimh"
            ),
            "exactly_one_us_common_equity_identity_required": True,
            "external_or_current_ticker_mapping_permitted": False,
            "eps_tags_priority": list(EPS_TAGS),
            "eps_unit": "USD/shares",
            "quarter_duration": 1,
            "consolidated_nondimensional_only": True,
            "current_period_matches_submission_period": True,
            "prior_comparison_days": [300, 430],
            "positive_yoy_eps_change_required": True,
            "duplicate_accession_or_event_key_receives_zero_credit": True,
            "event_key": ["accepted", "ticker", "adsh"],
        },
        "partitions": {
            "development": [DEVELOPMENT_START, DEVELOPMENT_END],
            "embargo": [EMBARGO_START, EMBARGO_END],
            "confirmation": [CONFIRMATION_START, CONFIRMATION_END],
            "confirmation_market_outcomes_remain_untouched": True,
        },
        "capacity_thresholds": {
            "minimum_unique_events": MINIMUM_UNIQUE_EVENTS,
            "minimum_development_event_dates": MINIMUM_DEVELOPMENT_DATES,
            "minimum_confirmation_event_dates": MINIMUM_CONFIRMATION_DATES,
        },
        "existing_family_rule_policy": {
            "existing_pead_mechanism": True,
            "new_mechanism_family_slot_consumed": False,
            "prior_family_trials_must_enter_selection_correction": True,
            "no_exact_prior_version_repair": True,
        },
        "outcome_exposure_index_sha256": outcome_exposure.audit()["index_sha256"],
        "implementation_hashes": {
            "earnings_sec_eps_capacity.py": sha256_file(Path(__file__).resolve()),
            "earnings_sec_eps_capacity_inspection.py": sha256_file(
                PROJECT_ROOT / "earnings_sec_eps_capacity_inspection.py"
            ),
            "outcome_exposure.py": sha256_file(PROJECT_ROOT / "outcome_exposure.py"),
            "strategy_discovery.py": sha256_file(
                PROJECT_ROOT / "strategy_discovery.py"
            ),
        },
        "market_prices_accessed": False,
        "forward_returns_accessed": False,
        "strategy_metrics_computed": 0,
        "confirmation_outcomes_accessed": False,
        "broker_actions": 0,
    }
    value["contract_sha256"] = self_hash(value, "contract_sha256")
    return value


def freeze_contract(
    *, created_at: str, root: Path = DEFAULT_ROOT
) -> tuple[Path, dict[str, Any]]:
    value = build_contract(created_at=created_at)
    path = (
        root
        / "metadata-contract"
        / f"contract-{value['contract_sha256']}.json"
    )
    _write(path, value)
    return path, value


def _member_name(archive: zipfile.ZipFile, expected: str) -> str:
    matches = [
        name
        for name in archive.namelist()
        if Path(name).name.casefold() == expected.casefold()
    ]
    if len(matches) != 1:
        raise EarningsSecEpsCapacityError(
            f"archive needs exactly one {expected}, found {len(matches)}"
        )
    return matches[0]


def _rows(archive: zipfile.ZipFile, filename: str) -> Iterable[dict[str, str]]:
    member = _member_name(archive, filename)
    prior_limit = csv.field_size_limit()
    csv.field_size_limit(CSV_FIELD_SIZE_LIMIT)
    try:
        with archive.open(member) as raw:
            with io.TextIOWrapper(raw, encoding="utf-8", newline="") as text:
                yield from csv.DictReader(text, delimiter="\t")
    finally:
        csv.field_size_limit(prior_limit)


def _normalized_text(value: Any) -> str:
    return " ".join(str(value or "").split()).strip()


def _integer(value: Any, default: int = -1) -> int:
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return default


def _float(value: Any) -> float | None:
    try:
        result = float(str(value).strip())
    except (TypeError, ValueError):
        return None
    if result != result or result in (float("inf"), float("-inf")):
        return None
    return result


def _submissions(archive: zipfile.ZipFile) -> dict[str, dict[str, str]]:
    result: dict[str, dict[str, str]] = {}
    for row in _rows(archive, "sub.tsv"):
        adsh = _normalized_text(row.get("adsh"))
        if (
            adsh
            and _normalized_text(row.get("form")).upper() == "10-Q"
            and _normalized_text(row.get("accepted"))
            and _normalized_text(row.get("period"))
            and _normalized_text(row.get("cik"))
        ):
            result[adsh] = {
                "adsh": adsh,
                "cik": _normalized_text(row.get("cik")),
                "name": _normalized_text(row.get("name")),
                "sic": _normalized_text(row.get("sic")),
                "period": _normalized_text(row.get("period")),
                "fy": _normalized_text(row.get("fy")),
                "fp": _normalized_text(row.get("fp")),
                "filed": _normalized_text(row.get("filed")),
                "accepted": _normalized_text(row.get("accepted")),
            }
    return result


def _tag(value: Any) -> str:
    return _normalized_text(value).split(":")[-1]


def _identities(
    archive: zipfile.ZipFile,
    submissions: Mapping[str, Mapping[str, str]],
) -> dict[str, dict[str, str]]:
    facts: dict[str, dict[str, dict[str, set[str]]]] = defaultdict(
        lambda: defaultdict(lambda: defaultdict(set))
    )
    permitted_tags = {"TradingSymbol", "SecurityExchangeName", "Security12bTitle"}
    for row in _rows(archive, "txt.tsv"):
        adsh = _normalized_text(row.get("adsh"))
        tag = _tag(row.get("tag"))
        if adsh not in submissions or tag not in permitted_tags:
            continue
        dimh = _normalized_text(row.get("dimh"))
        value = _normalized_text(row.get("value"))
        if not dimh or not value or _integer(row.get("iprx"), 1) != 1:
            continue
        facts[adsh][dimh][tag].add(value)
    result: dict[str, dict[str, str]] = {}
    for adsh, dimensions in facts.items():
        eligible: dict[tuple[str, str, str], dict[str, str]] = {}
        for dimh, values in dimensions.items():
            if not all(len(values.get(tag, set())) == 1 for tag in permitted_tags):
                continue
            ticker = next(iter(values["TradingSymbol"])).upper()
            exchange = next(iter(values["SecurityExchangeName"])).upper()
            title = next(iter(values["Security12bTitle"]))
            if (
                TICKER_PATTERN.fullmatch(ticker)
                and exchange in ALLOWED_EXCHANGES
                and COMMON_TITLE_PATTERN.search(title)
                and not DISALLOWED_TITLE_PATTERN.search(title)
            ):
                eligible[(ticker, exchange, title.casefold())] = {
                    "ticker": ticker,
                    "exchange": exchange,
                    "security_title": title,
                    "identity_dimh": dimh,
                }
        if len(eligible) == 1:
            result[adsh] = next(iter(eligible.values()))
    return result


def _eps_events(
    archive: zipfile.ZipFile,
    submissions: Mapping[str, Mapping[str, str]],
    identities: Mapping[str, Mapping[str, str]],
) -> list[dict[str, Any]]:
    facts: dict[str, dict[str, dict[str, list[float]]]] = defaultdict(
        lambda: defaultdict(lambda: defaultdict(list))
    )
    for row in _rows(archive, "num.tsv"):
        adsh = _normalized_text(row.get("adsh"))
        tag = _tag(row.get("tag"))
        if adsh not in identities or tag not in EPS_TAGS:
            continue
        unit = _normalized_text(row.get("uom")).casefold()
        dimn = _integer(row.get("dimn"), 0)
        dimh = _normalized_text(row.get("dimh"))
        no_dimensions = dimn == 0 or dimh == hashlib.md5(b"").hexdigest()
        value = _float(row.get("value"))
        if not (
            unit in {"usd/shares", "usd / shares"}
            and _integer(row.get("qtrs")) == 1
            and _integer(row.get("iprx"), 1) == 1
            and no_dimensions
            and not _normalized_text(row.get("coreg"))
            and value is not None
        ):
            continue
        facts[adsh][tag][_normalized_text(row.get("ddate"))].append(value)
    events: list[dict[str, Any]] = []
    for adsh, submission in submissions.items():
        if adsh not in identities:
            continue
        period = submission["period"]
        try:
            current_date = date(
                int(period[:4]), int(period[4:6]), int(period[6:8])
            )
        except (ValueError, IndexError):
            continue
        selected: tuple[str, float, float, str] | None = None
        for tag in EPS_TAGS:
            by_date = facts.get(adsh, {}).get(tag, {})
            current_values = by_date.get(period, [])
            if len(current_values) != 1:
                continue
            prior_candidates: list[tuple[int, str, float]] = []
            for ddate, values in by_date.items():
                if len(values) != 1 or ddate == period:
                    continue
                try:
                    prior_date = date(
                        int(ddate[:4]), int(ddate[4:6]), int(ddate[6:8])
                    )
                except (ValueError, IndexError):
                    continue
                age = (current_date - prior_date).days
                if 300 <= age <= 430:
                    prior_candidates.append((abs(age - 365), ddate, values[0]))
            if not prior_candidates:
                continue
            prior_candidates.sort()
            if (
                len(prior_candidates) > 1
                and prior_candidates[0][0] == prior_candidates[1][0]
            ):
                continue
            _distance, prior_period, prior_eps = prior_candidates[0]
            selected = (tag, current_values[0], prior_eps, prior_period)
            break
        if selected is None:
            continue
        tag, current_eps, prior_eps, prior_period = selected
        change = current_eps - prior_eps
        if change <= 0:
            continue
        scale = max(abs(prior_eps), 0.05)
        identity = identities[adsh]
        events.append(
            {
                **submission,
                **identity,
                "eps_tag": tag,
                "current_eps": current_eps,
                "prior_year_eps": prior_eps,
                "prior_period": prior_period,
                "eps_yoy_change": change,
                "eps_yoy_change_ratio": change / scale,
            }
        )
    return events


def derive_archive_events(path: Path) -> tuple[list[dict[str, Any]], dict[str, int]]:
    try:
        with zipfile.ZipFile(path) as archive:
            if archive.testzip() is not None:
                raise EarningsSecEpsCapacityError(
                    f"SEC archive failed CRC validation: {path}"
                )
            submissions = _submissions(archive)
            identities = _identities(archive, submissions)
            events = _eps_events(archive, submissions, identities)
    except (OSError, csv.Error, UnicodeDecodeError, zipfile.BadZipFile) as exc:
        raise EarningsSecEpsCapacityError(
            f"SEC archive is unreadable: {path}"
        ) from exc
    counts = {
        "eligible_10q_submissions": len(submissions),
        "resolved_common_equity_identities": len(identities),
        "positive_yoy_eps_events": len(events),
    }
    return events, counts


def _download(
    request: Mapping[str, Any],
    *,
    destination: Path,
    session: requests.Session,
    timeout_seconds: float,
) -> tuple[dict[str, Any], float]:
    started = time.monotonic()
    try:
        response = session.get(
            str(request["url"]), stream=True, timeout=timeout_seconds
        )
    except requests.RequestException as exc:
        raise EarningsSecEpsCapacityError(
            f"SEC archive request failed for {request['quarter']}"
        ) from exc
    if response.status_code >= 400:
        raise EarningsSecEpsCapacityError(
            f"SEC archive {request['quarter']} returned HTTP "
            f"{response.status_code}"
        )
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(f".{destination.name}.{os.getpid()}.tmp")
    digest = hashlib.sha256()
    size = 0
    try:
        with temporary.open("wb") as stream:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if not chunk:
                    continue
                stream.write(chunk)
                digest.update(chunk)
                size += len(chunk)
            stream.flush()
            os.fsync(stream.fileno())
        with zipfile.ZipFile(temporary) as archive:
            if archive.testzip() is not None:
                raise EarningsSecEpsCapacityError(
                    f"SEC archive {request['quarter']} failed CRC validation"
                )
            for required in ("sub.tsv", "num.tsv", "txt.tsv"):
                _member_name(archive, required)
        os.replace(temporary, destination)
    finally:
        temporary.unlink(missing_ok=True)
    elapsed = time.monotonic() - started
    return {
        "quarter": request["quarter"],
        "request_sha256": request["request_sha256"],
        "cache_relative_path": str(
            Path(PRIVATE_NAMESPACE) / "archives" / destination.name
        ),
        "file_sha256": digest.hexdigest(),
        "bytes": size,
        "etag": response.headers.get("ETag"),
        "last_modified": response.headers.get("Last-Modified"),
        "cache_hit": False,
    }, elapsed


def _cached_archive(path: Path, request: Mapping[str, Any]) -> dict[str, Any] | None:
    if not path.is_file():
        return None
    try:
        with zipfile.ZipFile(path) as archive:
            if archive.testzip() is not None:
                return None
            for required in ("sub.tsv", "num.tsv", "txt.tsv"):
                _member_name(archive, required)
    except (OSError, zipfile.BadZipFile, EarningsSecEpsCapacityError):
        return None
    return {
        "quarter": request["quarter"],
        "request_sha256": request["request_sha256"],
        "cache_relative_path": str(
            Path(PRIVATE_NAMESPACE) / "archives" / path.name
        ),
        "file_sha256": sha256_file(path),
        "bytes": path.stat().st_size,
        "etag": None,
        "last_modified": None,
        "cache_hit": True,
    }


def collect(
    contract_path: Path,
    inspection_path: Path,
    *,
    collected_at: str,
    store: HistoricalDayStore | None = None,
    session: requests.Session | None = None,
    timeout_seconds: float = 180.0,
    minimum_spacing_seconds: float = MINIMUM_SPACING_SECONDS,
) -> tuple[Path, dict[str, Any]]:
    strategy_discovery.require_committed(Path(__file__).resolve())
    strategy_discovery.require_committed(contract_path)
    strategy_discovery.require_committed(inspection_path)
    contract = _read(contract_path)
    inspection = _read(inspection_path)
    if not (
        contract.get("contract_sha256") == self_hash(contract, "contract_sha256")
        and inspection.get("contract_sha256") == contract["contract_sha256"]
        and inspection.get("state") == "SEC_EPS_CONTRACT_INSPECTED_READY"
        and inspection.get("valid") is True
    ):
        raise EarningsSecEpsCapacityError(
            "committed SEC EPS contract or inspection is invalid"
        )
    source = store or HistoricalDayStore.from_env()
    config = SecConfig.from_env(
        PROJECT_ROOT / ".env",
        source.root / PRIVATE_NAMESPACE,
        workers=1,
    )
    own_session = session is None
    http = session or requests.Session()
    http.headers.update(
        {"User-Agent": config.user_agent, "Accept-Encoding": "gzip, deflate"}
    )
    archives: list[dict[str, Any]] = []
    request_seconds = 0.0
    pacing_wait_seconds = 0.0
    provider_requests = 0
    try:
        for ordinal, request in enumerate(contract["requests"]):
            destination = (
                source.root
                / PRIVATE_NAMESPACE
                / "archives"
                / f"{request['request_sha256']}.zip"
            )
            cached = _cached_archive(destination, request)
            if cached is not None:
                archives.append(cached)
                continue
            if ordinal and minimum_spacing_seconds > 0:
                time.sleep(minimum_spacing_seconds)
                pacing_wait_seconds += minimum_spacing_seconds
            info, elapsed = _download(
                request,
                destination=destination,
                session=http,
                timeout_seconds=timeout_seconds,
            )
            archives.append(info)
            request_seconds += elapsed
            provider_requests += 1
    finally:
        if own_session:
            http.close()
    all_events: list[dict[str, Any]] = []
    archive_counts: list[dict[str, Any]] = []
    for request, archive_info in zip(contract["requests"], archives, strict=True):
        path = source.root / str(archive_info["cache_relative_path"])
        events, counts = derive_archive_events(path)
        all_events.extend(events)
        archive_counts.append(
            {
                "quarter": request["quarter"],
                "request_sha256": request["request_sha256"],
                **counts,
            }
        )
    event_counts: dict[tuple[str, str, str], int] = defaultdict(int)
    for event in all_events:
        event_counts[
            (str(event["accepted"]), str(event["ticker"]), str(event["adsh"]))
        ] += 1
    unique_events = [
        event
        for event in all_events
        if event_counts[
            (str(event["accepted"]), str(event["ticker"]), str(event["adsh"]))
        ]
        == 1
    ]
    unique_events.sort(key=lambda item: (item["accepted"], item["ticker"], item["adsh"]))
    private: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": "private-earnings-sec-eps-capacity",
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "successor_id": SUCCESSOR_ID,
        "contract_sha256": contract["contract_sha256"],
        "archives": archives,
        "archive_counts": archive_counts,
        "events": unique_events,
        "duplicate_event_rows_zero_credit": len(all_events) - len(unique_events),
        "market_prices_accessed": False,
        "forward_returns_accessed": False,
        "strategy_metrics_computed": 0,
        "confirmation_outcomes_accessed": False,
        "broker_actions": 0,
    }
    private["content_sha256"] = self_hash(private, "content_sha256")
    relative = (
        Path(PRIVATE_NAMESPACE)
        / "events"
        / f"{private['content_sha256']}.json.gz"
    )
    raw = _write_private(source.root / relative, private)
    value: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": "earnings-sec-fsnds-eps-capacity-collection",
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "successor_id": SUCCESSOR_ID,
        "state": "SEC_EPS_METADATA_COLLECTED_UNINSPECTED",
        "collected_at": _timestamp(collected_at, "collected_at"),
        "contract_path": _repo_path(contract_path),
        "contract_file_sha256": sha256_file(contract_path),
        "contract_sha256": contract["contract_sha256"],
        "inspection_path": _repo_path(inspection_path),
        "inspection_file_sha256": sha256_file(inspection_path),
        "inspection_sha256": inspection["inspection_sha256"],
        "archive_artifacts": archives,
        "private_event_artifact": {
            "cache_relative_path": str(relative),
            "content_sha256": private["content_sha256"],
            "file_sha256": hashlib.sha256(raw).hexdigest(),
            "compressed_bytes": len(raw),
        },
        "provider_telemetry": {
            "request_count": provider_requests,
            "request_seconds": round(request_seconds, 6),
            "pacing_wait_seconds": round(pacing_wait_seconds, 6),
            "cache_hits": sum(bool(item["cache_hit"]) for item in archives),
            "failures": 0,
            "retries": 0,
            "substitutions": 0,
        },
        "eligible_event_count": len(unique_events),
        "market_prices_accessed": False,
        "forward_returns_accessed": False,
        "strategy_metrics_computed": 0,
        "confirmation_outcomes_accessed": False,
        "broker_actions": 0,
    }
    value["collection_sha256"] = self_hash(value, "collection_sha256")
    path = (
        DEFAULT_ROOT
        / "metadata-collection"
        / f"collection-{value['collection_sha256']}.json"
    )
    _write(path, value)
    return path, value


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    freeze = subparsers.add_parser("freeze-contract")
    freeze.add_argument("--created-at", required=True)
    collect_parser = subparsers.add_parser("collect")
    collect_parser.add_argument("contract", type=Path)
    collect_parser.add_argument("inspection", type=Path)
    collect_parser.add_argument("--collected-at", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.command == "freeze-contract":
        path, value = freeze_contract(created_at=args.created_at)
        digest = value["contract_sha256"]
        state = "SEC_EPS_CONTRACT_FROZEN"
        requests_count = 0
    else:
        path, value = collect(
            args.contract,
            args.inspection,
            collected_at=args.collected_at,
        )
        digest = value["collection_sha256"]
        state = value["state"]
        requests_count = value["provider_telemetry"]["request_count"]
    print(
        json.dumps(
            {
                "path": _repo_path(path),
                "sha256": digest,
                "state": state,
                "provider_requests": requests_count,
                "eligible_events": value.get("eligible_event_count", 0),
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

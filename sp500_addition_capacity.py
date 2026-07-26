"""Freeze and collect outcome-blind S&P 500 addition source capacity.

The workflow deliberately separates the archive-index request graph from the
individual release-page graph.  No release page can be opened until the exact
URLs returned by the frozen archive queries have been independently inspected,
committed, and bound into a second contract.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import html
import json
import os
import re
import time
from collections.abc import Mapping, Sequence
from datetime import date, datetime
from html.parser import HTMLParser
from pathlib import Path
from typing import Any
from urllib.parse import urlencode, urlparse

import requests

import portfolio_maturity
import strategy_discovery
from historical_store import HistoricalDayStore, sha256_file


PROJECT_ROOT = Path(__file__).resolve().parent
CAMPAIGN_ID = portfolio_maturity.V2_CAMPAIGN_ID
FAMILY_ID = "sp500-index-addition-forced-demand"
SUCCESSOR_ID = "sp500-index-addition-forced-demand-v1"
MECHANISM_FAMILY = "large-index-addition-forced-demand"
ROLLING_AUTHORIZATION_SHA256 = (
    "bedfb0ea139cfe806b1b780828e59f1d98e3a125c50ae2b8d86b662d5d5e5585"
)
ROLLING_AUTHORIZATION_PATH = (
    PROJECT_ROOT
    / "strategy_tournament/v2/rolling_authorization/"
    f"rolling-discovery-authorization-{ROLLING_AUTHORIZATION_SHA256}.json"
)
ROLLING_STATUS_PATH = (
    PROJECT_ROOT / "strategy_tournament/v2/rolling_authorization/status.json"
)
PUBLIC_ROOT = (
    PROJECT_ROOT / "strategy_tournament/v2/sp500-index-addition-capacity"
)
SOURCE_START_YEAR = 2010
SOURCE_END_YEAR = 2025
ARCHIVE_ENDPOINT = "https://press.spglobal.com/index.php"
ARCHIVE_PAGE_SIZE = 100
REQUEST_TIMEOUT_SECONDS = 30
MINIMUM_REQUEST_INTERVAL_SECONDS = 0.25
MINIMUM_TOTAL_SIGNAL_DATES = 50
FAST_LANE_TOTAL_SIGNAL_DATES = 100
MINIMUM_DEVELOPMENT_SIGNAL_DATES = 30
MINIMUM_CONFIRMATION_SIGNAL_DATES = 20
DEVELOPMENT_END = "2021-12-31"
CONFIRMATION_START = "2022-01-01"
CONFIRMATION_END = "2025-12-31"
USER_AGENT = "robinhood-codex-public-research/1.0"
IMPLEMENTATION_FILES = (
    "sp500_addition_capacity.py",
    "sp500_addition_capacity_inspection.py",
    "outcome_exposure.py",
    "strategy_discovery.py",
    "portfolio_maturity.py",
    "portfolio_config.toml",
    "PORTFOLIO_VALIDATION_V2.md",
)


class Sp500AdditionCapacityError(RuntimeError):
    """The source graph or capacity boundary is incomplete."""


def _canonical(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode()


def _self_hash(value: Mapping[str, Any], field: str = "artifact_sha256") -> str:
    payload = dict(value)
    payload.pop(field, None)
    return hashlib.sha256(_canonical(payload)).hexdigest()


def _timestamp(value: str) -> None:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise Sp500AdditionCapacityError("timestamp is invalid") from exc
    if parsed.tzinfo is None:
        raise Sp500AdditionCapacityError("timestamp needs a timezone")


def _read(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise Sp500AdditionCapacityError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise Sp500AdditionCapacityError(f"{path} must contain an object")
    return value


def _write(path: Path, value: Mapping[str, Any]) -> None:
    rendered = json.dumps(value, indent=2, sort_keys=True) + "\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and path.read_text(encoding="utf-8") == rendered:
        return
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        temporary.write_text(rendered, encoding="utf-8")
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _repo_path(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(PROJECT_ROOT.resolve()))
    except ValueError as exc:
        raise Sp500AdditionCapacityError(
            f"path escaped repository: {path}"
        ) from exc


def _source_root() -> Path:
    return (
        HistoricalDayStore.from_env().root
        / "_source/sp500_index_addition_capacity"
    )


def _relative_source_path(path: Path) -> str:
    root = HistoricalDayStore.from_env().root.resolve()
    try:
        return str(path.resolve().relative_to(root))
    except ValueError as exc:
        raise Sp500AdditionCapacityError(
            "source cache escaped the historical store"
        ) from exc


def _source_path(relative: str) -> Path:
    root = HistoricalDayStore.from_env().root.resolve()
    path = (root / relative).resolve()
    if root not in path.parents:
        raise Sp500AdditionCapacityError(
            "source cache reference escaped the historical store"
        )
    return path


def _load_artifact(path: Path, kind: str) -> dict[str, Any]:
    artifact = strategy_discovery.load_artifact(path, expected_kind=kind)
    if artifact.get("artifact_sha256") != _self_hash(artifact):
        raise Sp500AdditionCapacityError(f"{path}: artifact hash drifted")
    return artifact


def _require_committed(paths: Sequence[Path]) -> None:
    for path in paths:
        strategy_discovery.require_committed(path)


def _archive_query_url(year: int) -> str:
    query = urlencode(
        {
            "s": "2429",
            "l": str(ARCHIVE_PAGE_SIZE),
            "year": str(year),
            "keywords": "S&P 500",
            "titles_only": "1",
        }
    )
    return f"{ARCHIVE_ENDPOINT}?{query}"


def archive_tasks() -> list[dict[str, Any]]:
    return [
        {
            "ordinal": year - SOURCE_START_YEAR + 1,
            "year": year,
            "url": _archive_query_url(year),
            "task_id": hashlib.sha256(
                f"archive-index|{year}|{_archive_query_url(year)}".encode()
            ).hexdigest(),
        }
        for year in range(SOURCE_START_YEAR, SOURCE_END_YEAR + 1)
    ]


def _implementation_hashes() -> dict[str, str]:
    return {
        relative: sha256_file(PROJECT_ROOT / relative)
        for relative in IMPLEMENTATION_FILES
    }


def _validate_rolling_authority(*, enforce_commit: bool) -> None:
    if enforce_commit:
        _require_committed(
            [
                ROLLING_AUTHORIZATION_PATH,
                ROLLING_STATUS_PATH,
                *[PROJECT_ROOT / path for path in IMPLEMENTATION_FILES],
            ]
        )
    authorization = _read(ROLLING_AUTHORIZATION_PATH)
    status = _read(ROLLING_STATUS_PATH)
    if not (
        authorization.get("authorization_sha256")
        == ROLLING_AUTHORIZATION_SHA256
        and authorization.get("activation_policy")
        == "ROLLING_TERMINAL_REPLACEMENT"
        and authorization.get("maximum_concurrent_active_mechanism_families")
        == 3
        and authorization.get("target_outcome_access_permitted_before_family_freeze")
        is False
        and authorization.get("broker_actions_permitted") is False
        and status.get("state") == "ROLLING_DISCOVERY_AUTHORIZED"
        and status.get("available_slot_count", 0) >= 1
        and status.get("provider_access_permitted") is True
        and status.get("target_outcome_access_permitted") is False
        and status.get("broker_actions_permitted") is False
        and status.get("valid") is True
    ):
        raise Sp500AdditionCapacityError(
            "rolling discovery authority does not expose a safe slot"
        )


def freeze_index_contract(
    *,
    created_at: str,
    root: Path = PUBLIC_ROOT,
    enforce_commit: bool = True,
) -> tuple[Path, dict[str, Any]]:
    """Freeze the sixteen official archive queries before source access."""

    _timestamp(created_at)
    _validate_rolling_authority(enforce_commit=enforce_commit)
    contract: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": "sp500-addition-index-query-contract",
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "successor_id": SUCCESSOR_ID,
        "mechanism_family": MECHANISM_FAMILY,
        "research_generation": "new_mechanism_family",
        "new_mechanism_family_slot_consumed": True,
        "rolling_authorization_path": _repo_path(
            ROLLING_AUTHORIZATION_PATH
        ),
        "rolling_authorization_sha256": ROLLING_AUTHORIZATION_SHA256,
        "created_at": created_at,
        "state": "INDEX_QUERY_CONTRACT_FROZEN",
        "mechanism": (
            "announced S&P 500 additions create preregistered index-tracker "
            "demand that must be completed by the published effective date"
        ),
        "source": {
            "provider": "S&P Global official press archive",
            "host": "press.spglobal.com",
            "archive_endpoint": ARCHIVE_ENDPOINT,
            "source_start_year": SOURCE_START_YEAR,
            "source_end_year": SOURCE_END_YEAR,
            "archive_page_size": ARCHIVE_PAGE_SIZE,
            "query": {
                "section": "2429",
                "keywords": "S&P 500",
                "titles_only": True,
                "one_year_per_request": True,
            },
            "maximum_results_per_year": ARCHIVE_PAGE_SIZE,
            "pagination_or_date_substitution_permitted": False,
            "request_timeout_seconds": REQUEST_TIMEOUT_SECONDS,
            "minimum_request_interval_seconds": (
                MINIMUM_REQUEST_INTERVAL_SECONDS
            ),
            "user_agent": USER_AGENT,
        },
        "tasks": archive_tasks(),
        "task_count": len(archive_tasks()),
        "release_page_access_permitted": False,
        "market_price_access_permitted": False,
        "target_return_access_permitted": False,
        "confirmation_access_permitted": False,
        "broker_actions_permitted": False,
        "capacity_gate": {
            "minimum_total_signal_dates": MINIMUM_TOTAL_SIGNAL_DATES,
            "fast_lane_total_signal_dates": FAST_LANE_TOTAL_SIGNAL_DATES,
            "minimum_development_signal_dates": (
                MINIMUM_DEVELOPMENT_SIGNAL_DATES
            ),
            "minimum_confirmation_signal_dates": (
                MINIMUM_CONFIRMATION_SIGNAL_DATES
            ),
            "maximum_one_new_family_entry_per_announcement_date": True,
        },
        "partition": {
            "development_end": DEVELOPMENT_END,
            "confirmation_start": CONFIRMATION_START,
            "confirmation_end": CONFIRMATION_END,
            "exact market-session embargo_frozen_later": True,
        },
        "denominator": {
            "every_archive_result_retained": True,
            "irrelevant_or_unparseable_release_retained_with_reason": True,
            "same_ticker_effective_date_duplicates_keep_earliest_publication": True,
            "missing_publication_timestamp_or_structured_addition_row": (
                "ineligible_preserve_denominator"
            ),
        },
        "implementation_hashes": _implementation_hashes(),
        "provider_requests_before_contract_freeze": 0,
        "market_outcomes_accessed": False,
    }
    contract["artifact_sha256"] = _self_hash(contract)
    path = (
        root
        / "index-contract"
        / f"sp500-addition-index-contract-{contract['artifact_sha256']}.json"
    )
    _write(path, contract)
    return path, contract


_SEARCH_COUNT_RE = re.compile(
    r'<div class="wd_search_count">Your search returned\s+'
    r"([0-9,]+)\s+results?</div>",
    re.IGNORECASE,
)
_SEARCH_ROW_RE = re.compile(
    r'<div class="wd_date">\s*(.*?)\s*</div>\s*'
    r'<div class="wd_title"><a href="([^"]+)">(.*?)</a></div>',
    re.IGNORECASE | re.DOTALL,
)


def parse_archive_index(
    raw: bytes, *, expected_year: int
) -> tuple[int, list[dict[str, Any]]]:
    """Parse one official archive result page without opening result URLs."""

    text = raw.decode("utf-8", errors="strict")
    count_match = _SEARCH_COUNT_RE.search(text)
    if count_match is None:
        raise Sp500AdditionCapacityError(
            f"{expected_year}: archive result count is missing"
        )
    result_count = int(count_match.group(1).replace(",", ""))
    if result_count > ARCHIVE_PAGE_SIZE:
        raise Sp500AdditionCapacityError(
            f"{expected_year}: frozen single-page query would truncate results"
        )
    rows: list[dict[str, Any]] = []
    for raw_date, url, raw_title in _SEARCH_ROW_RE.findall(text):
        title = re.sub(
            r"\s+",
            " ",
            html.unescape(re.sub(r"<[^>]+>", " ", raw_title)),
        ).strip()
        listed_date = datetime.strptime(
            html.unescape(raw_date).strip(), "%b %d, %Y"
        ).date()
        parsed = urlparse(html.unescape(url))
        if (
            parsed.scheme != "https"
            or parsed.netloc != "press.spglobal.com"
            or not parsed.path.startswith(f"/{expected_year}-")
            or parsed.query
            or parsed.fragment
            or listed_date.year != expected_year
            or not title
        ):
            raise Sp500AdditionCapacityError(
                f"{expected_year}: archive result identity is invalid"
            )
        canonical_url = f"https://press.spglobal.com{parsed.path}"
        rows.append(
            {
                "listed_date": listed_date.isoformat(),
                "title": title,
                "url": canonical_url,
                "url_sha256": hashlib.sha256(
                    canonical_url.encode()
                ).hexdigest(),
            }
        )
    if len(rows) != result_count or len({row["url"] for row in rows}) != len(rows):
        raise Sp500AdditionCapacityError(
            f"{expected_year}: archive rows do not match the declared count"
        )
    return result_count, rows


def _fetch_exact(url: str) -> bytes:
    response = requests.get(
        url,
        headers={"User-Agent": USER_AGENT, "Accept": "text/html"},
        timeout=REQUEST_TIMEOUT_SECONDS,
        allow_redirects=True,
    )
    if (
        response.status_code != 200
        or response.url.split("?", 1)[0] != url.split("?", 1)[0]
        or not response.content
    ):
        raise Sp500AdditionCapacityError(
            f"exact source request failed: {url} status={response.status_code}"
        )
    return response.content


def _cache_bytes(path: Path, raw: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        with gzip.open(path, "rb") as stream:
            existing = stream.read()
        if existing != raw:
            raise Sp500AdditionCapacityError(
                f"content-addressed cache drifted: {path}"
            )
        return
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        with temporary.open("wb") as output:
            with gzip.GzipFile(
                filename="", mode="wb", fileobj=output, mtime=0
            ) as stream:
                stream.write(raw)
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _read_cached(path: Path) -> bytes:
    try:
        with gzip.open(path, "rb") as stream:
            return stream.read()
    except OSError as exc:
        raise Sp500AdditionCapacityError(
            f"cannot read source cache {path}: {exc}"
        ) from exc


def _task_cache_path(
    *,
    contract_sha256: str,
    lane: str,
    task_id: str,
) -> Path:
    return (
        _source_root()
        / contract_sha256
        / lane
        / f"{task_id}.html.gz"
    )


def collect_index(
    contract_path: Path,
    inspection_path: Path,
    *,
    collected_at: str,
    root: Path = PUBLIC_ROOT,
) -> tuple[Path, dict[str, Any]]:
    """Execute only the committed, independently inspected archive queries."""

    _timestamp(collected_at)
    _require_committed([contract_path, inspection_path])
    contract = _load_artifact(
        contract_path, "sp500-addition-index-query-contract"
    )
    inspection = _load_artifact(
        inspection_path, "sp500-addition-index-contract-inspection"
    )
    if not (
        contract.get("state") == "INDEX_QUERY_CONTRACT_FROZEN"
        and inspection.get("state") == "INDEX_QUERY_CONTRACT_INSPECTED_READY"
        and inspection.get("contract_sha256") == contract["artifact_sha256"]
        and inspection.get("archive_index_access_permitted") is True
        and inspection.get("release_page_access_permitted") is False
        and inspection.get("market_price_access_permitted") is False
        and inspection.get("broker_actions_permitted") is False
    ):
        raise Sp500AdditionCapacityError(
            "index collection predecessor chain is not ready"
        )
    started = time.monotonic()
    request_seconds = 0.0
    pacing_wait_seconds = 0.0
    requests = 0
    cache_hits = 0
    completed: list[dict[str, Any]] = []
    last_request_completed: float | None = None
    for task in contract["tasks"]:
        cache = _task_cache_path(
            contract_sha256=contract["artifact_sha256"],
            lane="archive-index",
            task_id=task["task_id"],
        )
        if cache.exists():
            raw = _read_cached(cache)
            cache_hits += 1
        else:
            if last_request_completed is not None:
                wait = max(
                    0.0,
                    MINIMUM_REQUEST_INTERVAL_SECONDS
                    - (time.monotonic() - last_request_completed),
                )
                if wait:
                    time.sleep(wait)
                    pacing_wait_seconds += wait
            requested = time.monotonic()
            raw = _fetch_exact(task["url"])
            request_seconds += time.monotonic() - requested
            requests += 1
            last_request_completed = time.monotonic()
            _cache_bytes(cache, raw)
        result_count, _rows = parse_archive_index(
            raw, expected_year=task["year"]
        )
        completed.append(
            {
                **task,
                "cache_relative_path": _relative_source_path(cache),
                "raw_sha256": hashlib.sha256(raw).hexdigest(),
                "raw_bytes": len(raw),
                "result_count": result_count,
            }
        )
    status: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": "sp500-addition-index-collection",
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "successor_id": SUCCESSOR_ID,
        "collected_at": collected_at,
        "state": "INDEX_QUERY_COLLECTION_UNINSPECTED",
        "contract_path": _repo_path(contract_path),
        "contract_sha256": contract["artifact_sha256"],
        "inspection_path": _repo_path(inspection_path),
        "inspection_sha256": inspection["artifact_sha256"],
        "tasks": completed,
        "task_count": len(completed),
        "declared_result_count": sum(
            row["result_count"] for row in completed
        ),
        "provider_telemetry": {
            "requests": requests,
            "cache_hits": cache_hits,
            "failures": 0,
            "request_seconds": request_seconds,
            "pacing_wait_seconds": pacing_wait_seconds,
            "elapsed_seconds": time.monotonic() - started,
        },
        "release_page_requests": 0,
        "market_price_requests": 0,
        "market_outcomes_accessed": False,
        "confirmation_accessed": False,
        "broker_actions": 0,
    }
    status["artifact_sha256"] = _self_hash(status)
    path = (
        root
        / "index-collection"
        / f"sp500-addition-index-collection-{status['artifact_sha256']}.json"
    )
    _write(path, status)
    return path, status


def freeze_page_contract(
    listing_inspection_path: Path,
    *,
    created_at: str,
    root: Path = PUBLIC_ROOT,
) -> tuple[Path, dict[str, Any]]:
    """Freeze every exact release URL before opening any release page."""

    _timestamp(created_at)
    _require_committed([listing_inspection_path])
    listing = _load_artifact(
        listing_inspection_path,
        "sp500-addition-index-collection-inspection",
    )
    entries = listing.get("entries")
    if not (
        listing.get("state") == "INDEX_QUERY_COLLECTION_INSPECTED_READY"
        and listing.get("release_page_contract_permitted") is True
        and isinstance(entries, list)
        and entries
        and listing.get("market_price_access_permitted") is False
        and listing.get("broker_actions_permitted") is False
    ):
        raise Sp500AdditionCapacityError(
            "listing inspection does not authorize page freeze"
        )
    tasks = [
        {
            "ordinal": ordinal,
            "listed_date": entry["listed_date"],
            "title": entry["title"],
            "url": entry["url"],
            "url_sha256": entry["url_sha256"],
            "task_id": hashlib.sha256(
                f"release-page|{entry['url']}".encode()
            ).hexdigest(),
        }
        for ordinal, entry in enumerate(entries, start=1)
    ]
    contract: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": "sp500-addition-release-page-contract",
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "successor_id": SUCCESSOR_ID,
        "created_at": created_at,
        "state": "RELEASE_PAGE_CONTRACT_FROZEN",
        "listing_inspection_path": _repo_path(listing_inspection_path),
        "listing_inspection_sha256": listing["artifact_sha256"],
        "tasks": tasks,
        "task_count": len(tasks),
        "source_contract": {
            "exact_https_url_only": True,
            "redirect_outside_exact_canonical_url_permitted": False,
            "pagination_or_url_substitution_permitted": False,
            "request_timeout_seconds": REQUEST_TIMEOUT_SECONDS,
            "minimum_request_interval_seconds": (
                MINIMUM_REQUEST_INTERVAL_SECONDS
            ),
            "user_agent": USER_AGENT,
        },
        "event_semantics": {
            "eligible_index": "S&P 500",
            "eligible_action": "Addition",
            "publication_timestamp_source": "HTML ITEMDATE comment",
            "effective_date_source": "structured release summary table",
            "ticker_source": "structured release summary table",
            "same_day_entry_permitted": False,
            "candidate_entry": (
                "first complete regular session after the publication date"
            ),
            "maximum_one_new_family_entry_per_announcement_date": True,
            "duplicate_identity": "ticker plus effective date",
            "duplicate_resolution": (
                "earliest publication timestamp then lexical source URL"
            ),
            "unparseable_or_irrelevant_release": (
                "ineligible_preserve_denominator"
            ),
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
        },
        "implementation_hashes": _implementation_hashes(),
        "provider_requests_before_contract_freeze": 0,
        "market_price_access_permitted": False,
        "target_return_access_permitted": False,
        "confirmation_access_permitted": False,
        "broker_actions_permitted": False,
        "market_outcomes_accessed": False,
    }
    contract["artifact_sha256"] = _self_hash(contract)
    path = (
        root
        / "page-contract"
        / f"sp500-addition-page-contract-{contract['artifact_sha256']}.json"
    )
    _write(path, contract)
    return path, contract


def collect_pages(
    contract_path: Path,
    inspection_path: Path,
    *,
    collected_at: str,
    root: Path = PUBLIC_ROOT,
) -> tuple[Path, dict[str, Any]]:
    """Fetch the exact committed release-page graph with resumable caching."""

    _timestamp(collected_at)
    _require_committed([contract_path, inspection_path])
    contract = _load_artifact(
        contract_path, "sp500-addition-release-page-contract"
    )
    inspection = _load_artifact(
        inspection_path, "sp500-addition-page-contract-inspection"
    )
    if not (
        contract.get("state") == "RELEASE_PAGE_CONTRACT_FROZEN"
        and inspection.get("state") == "RELEASE_PAGE_CONTRACT_INSPECTED_READY"
        and inspection.get("contract_sha256") == contract["artifact_sha256"]
        and inspection.get("release_page_access_permitted") is True
        and inspection.get("market_price_access_permitted") is False
        and inspection.get("broker_actions_permitted") is False
    ):
        raise Sp500AdditionCapacityError(
            "release-page predecessor chain is not ready"
        )
    started = time.monotonic()
    request_seconds = 0.0
    pacing_wait_seconds = 0.0
    requests = 0
    cache_hits = 0
    completed: list[dict[str, Any]] = []
    last_request_completed: float | None = None
    for task in contract["tasks"]:
        cache = _task_cache_path(
            contract_sha256=contract["artifact_sha256"],
            lane="release-pages",
            task_id=task["task_id"],
        )
        if cache.exists():
            raw = _read_cached(cache)
            cache_hits += 1
        else:
            if last_request_completed is not None:
                wait = max(
                    0.0,
                    MINIMUM_REQUEST_INTERVAL_SECONDS
                    - (time.monotonic() - last_request_completed),
                )
                if wait:
                    time.sleep(wait)
                    pacing_wait_seconds += wait
            requested = time.monotonic()
            raw = _fetch_exact(task["url"])
            request_seconds += time.monotonic() - requested
            requests += 1
            last_request_completed = time.monotonic()
            _cache_bytes(cache, raw)
        completed.append(
            {
                **task,
                "cache_relative_path": _relative_source_path(cache),
                "raw_sha256": hashlib.sha256(raw).hexdigest(),
                "raw_bytes": len(raw),
            }
        )
    status: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": "sp500-addition-release-page-collection",
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "successor_id": SUCCESSOR_ID,
        "collected_at": collected_at,
        "state": "RELEASE_PAGES_COLLECTED_UNINSPECTED",
        "contract_path": _repo_path(contract_path),
        "contract_sha256": contract["artifact_sha256"],
        "inspection_path": _repo_path(inspection_path),
        "inspection_sha256": inspection["artifact_sha256"],
        "tasks": completed,
        "task_count": len(completed),
        "provider_telemetry": {
            "requests": requests,
            "cache_hits": cache_hits,
            "failures": 0,
            "request_seconds": request_seconds,
            "pacing_wait_seconds": pacing_wait_seconds,
            "elapsed_seconds": time.monotonic() - started,
        },
        "market_price_requests": 0,
        "market_outcomes_accessed": False,
        "confirmation_accessed": False,
        "broker_actions": 0,
    }
    status["artifact_sha256"] = _self_hash(status)
    path = (
        root
        / "page-collection"
        / f"sp500-addition-page-collection-{status['artifact_sha256']}.json"
    )
    _write(path, status)
    return path, status


class _ReleaseParser(HTMLParser):
    """Extract ITEMDATE and table cells from one official release."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.itemdate: str | None = None
        self.tables: list[list[list[str]]] = []
        self._table_depth = 0
        self._current_table: list[list[str]] | None = None
        self._current_row: list[str] | None = None
        self._current_cell: list[str] | None = None

    def handle_comment(self, data: str) -> None:
        match = re.search(r"ITEMDATE:\s*(.*?)\s*$", data)
        if match:
            if self.itemdate is not None:
                raise Sp500AdditionCapacityError(
                    "release contains multiple ITEMDATE comments"
                )
            self.itemdate = match.group(1)

    def handle_starttag(
        self, tag: str, attrs: list[tuple[str, str | None]]
    ) -> None:
        del attrs
        if tag == "table":
            if self._table_depth == 0:
                self._current_table = []
            self._table_depth += 1
        elif tag == "tr" and self._table_depth:
            self._current_row = []
        elif tag in {"td", "th"} and self._current_row is not None:
            self._current_cell = []

    def handle_data(self, data: str) -> None:
        if self._current_cell is not None:
            self._current_cell.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag in {"td", "th"} and self._current_cell is not None:
            assert self._current_row is not None
            value = re.sub(r"\s+", " ", "".join(self._current_cell)).strip()
            self._current_row.append(value)
            self._current_cell = None
        elif tag == "tr" and self._current_row is not None:
            if self._current_table is not None and any(self._current_row):
                self._current_table.append(self._current_row)
            self._current_row = None
        elif tag == "table" and self._table_depth:
            self._table_depth -= 1
            if self._table_depth == 0 and self._current_table is not None:
                self.tables.append(self._current_table)
                self._current_table = None


def _parse_itemdate(raw: str) -> datetime:
    match = re.fullmatch(
        r"(\d{4}-\d{2}-\d{2})\s+(\d{2}:\d{2}:\d{2})\s+(EST|EDT)",
        raw.strip(),
    )
    if match is None:
        raise Sp500AdditionCapacityError("release ITEMDATE is invalid")
    offset = "-05:00" if match.group(3) == "EST" else "-04:00"
    return datetime.fromisoformat(
        f"{match.group(1)}T{match.group(2)}{offset}"
    )


def _parse_effective_date(raw: str, *, announcement: date) -> date:
    cleaned = re.sub(r"\s+", " ", raw).strip().replace("Sept.", "Sep")
    cleaned = re.sub(r"\b([A-Z][a-z]{2})\.", r"\1", cleaned)
    formats = ("%B %d, %Y", "%b %d, %Y", "%B %d", "%b %d")
    for fmt in formats:
        try:
            parsed = datetime.strptime(cleaned, fmt).date()
        except ValueError:
            continue
        if "%Y" not in fmt:
            year = announcement.year
            if parsed.month < announcement.month - 6:
                year += 1
            parsed = parsed.replace(year=year)
        return parsed
    raise Sp500AdditionCapacityError(
        f"effective date is unparseable: {raw}"
    )


def parse_release(
    raw: bytes,
    *,
    source_url: str,
    listed_date: str,
) -> dict[str, Any]:
    """Extract structured S&P 500 additions; retain all other releases."""

    parser = _ReleaseParser()
    parser.feed(raw.decode("utf-8", errors="strict"))
    parser.close()
    if parser.itemdate is None:
        return {
            "source_url": source_url,
            "listed_date": listed_date,
            "eligible_events": [],
            "terminal_reason": "MISSING_PUBLICATION_TIMESTAMP",
        }
    published = _parse_itemdate(parser.itemdate)
    if published.date().isoformat() != listed_date:
        raise Sp500AdditionCapacityError(
            f"{source_url}: listing date and ITEMDATE differ"
        )
    events: list[dict[str, Any]] = []
    for table in parser.tables:
        for row in table:
            normalized = [
                value.replace("®", "").replace("\xa0", " ").strip()
                for value in row
            ]
            if len(normalized) < 5:
                continue
            for offset in range(0, len(normalized) - 4):
                segment = normalized[offset : offset + 6]
                if (
                    len(segment) >= 5
                    and segment[1] == "S&P 500"
                    and segment[2].casefold() == "addition"
                ):
                    effective = _parse_effective_date(
                        segment[0], announcement=published.date()
                    )
                    ticker = segment[4].upper().replace(" ", "")
                    if not re.fullmatch(r"[A-Z][A-Z0-9.-]{0,11}", ticker):
                        raise Sp500AdditionCapacityError(
                            f"{source_url}: invalid structured ticker {ticker}"
                        )
                    if effective <= published.date():
                        raise Sp500AdditionCapacityError(
                            f"{source_url}: effective date is not later"
                        )
                    events.append(
                        {
                            "announcement_at": published.isoformat(),
                            "announcement_date": published.date().isoformat(),
                            "effective_date": effective.isoformat(),
                            "index_name": "S&P 500",
                            "action": "Addition",
                            "company_name": segment[3],
                            "ticker": ticker,
                            "source_url": source_url,
                        }
                    )
    unique = {
        (event["ticker"], event["effective_date"]): event for event in events
    }
    retained = [unique[key] for key in sorted(unique)]
    return {
        "source_url": source_url,
        "listed_date": listed_date,
        "publication_timestamp": published.isoformat(),
        "eligible_events": retained,
        "terminal_reason": (
            "ELIGIBLE_SP500_ADDITION"
            if retained
            else "NO_STRUCTURED_SP500_ADDITION"
        ),
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    freeze_index = sub.add_parser("freeze-index")
    freeze_index.add_argument("--created-at", required=True)
    collect_index_parser = sub.add_parser("collect-index")
    collect_index_parser.add_argument("contract", type=Path)
    collect_index_parser.add_argument("inspection", type=Path)
    collect_index_parser.add_argument("--collected-at", required=True)
    freeze_pages = sub.add_parser("freeze-pages")
    freeze_pages.add_argument("listing_inspection", type=Path)
    freeze_pages.add_argument("--created-at", required=True)
    collect_pages_parser = sub.add_parser("collect-pages")
    collect_pages_parser.add_argument("contract", type=Path)
    collect_pages_parser.add_argument("inspection", type=Path)
    collect_pages_parser.add_argument("--collected-at", required=True)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "freeze-index":
            path, artifact = freeze_index_contract(created_at=args.created_at)
        elif args.command == "collect-index":
            path, artifact = collect_index(
                args.contract,
                args.inspection,
                collected_at=args.collected_at,
            )
        elif args.command == "freeze-pages":
            path, artifact = freeze_page_contract(
                args.listing_inspection,
                created_at=args.created_at,
            )
        else:
            path, artifact = collect_pages(
                args.contract,
                args.inspection,
                collected_at=args.collected_at,
            )
    except (
        Sp500AdditionCapacityError,
        strategy_discovery.StrategyDiscoveryError,
        OSError,
        requests.RequestException,
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
                "written": _repo_path(path),
                "market_outcomes_accessed": artifact.get(
                    "market_outcomes_accessed", False
                ),
                "broker_actions": artifact.get(
                    "broker_actions",
                    int(artifact.get("broker_actions_permitted") is True),
                ),
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

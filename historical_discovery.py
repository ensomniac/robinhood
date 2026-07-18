"""Build point-in-time historical draft universes from earnings and SEC data.

The connector ingestion command turns Robinhood earnings-calendar tool results
into a durable ignored artifact.  The build command cross-checks those events
against SEC EDGAR 8-K metadata, supplements sparse dates with material prior-day
8-K events, screens strong financing/dilution language, and emits ranked draft
pools without reading target-session market prices.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import threading
import time
from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, time as wall_time
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import requests
from dotenv import dotenv_values

from historical_concurrency import ordered_bounded_results


PROJECT_ROOT = Path(__file__).resolve().parent
EASTERN = ZoneInfo("America/New_York")
DEFAULT_CACHE_ROOT = PROJECT_ROOT / "historical_data" / "sec"
DEFAULT_ENV_PATH = PROJECT_ROOT / ".env"
SEC_TICKERS_URL = "https://www.sec.gov/files/company_tickers_exchange.json"
SEC_SUBMISSIONS_ROOT = "https://data.sec.gov/submissions"
MATERIAL_ITEMS = {
    "1.01",
    "1.02",
    "2.01",
    "2.02",
    "2.05",
    "2.06",
    "5.02",
    "7.01",
    "8.01",
}
ITEM_PRIORITY = {
    "2.02": 0,
    "1.01": 1,
    "2.01": 2,
    "1.02": 3,
    "2.05": 4,
    "2.06": 5,
    "8.01": 6,
    "7.01": 7,
    "5.02": 8,
}
STRONG_DILUTION_PATTERNS = (
    "at-the-market offering",
    "at the market offering",
    "registered direct offering",
    "public offering of common stock",
    "private placement of common stock",
    "securities purchase agreement",
    "equity line of credit",
    "common stock purchase agreement",
    "sale of shares of common stock",
    "issuance of shares of common stock",
)


class HistoricalDiscoveryError(RuntimeError):
    """Raised when source evidence cannot produce an auditable draft."""


def _atomic_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    rendered = json.dumps(value, indent=2, sort_keys=True) + "\n"
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(rendered, encoding="utf-8")
    temporary.replace(path)


def _load_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise HistoricalDiscoveryError(f"cannot read {path}: {exc}") from exc


def extract_earnings_events(payload: Any) -> list[dict[str, Any]]:
    """Normalize one or many connector result envelopes."""
    envelopes = payload if isinstance(payload, list) else [payload]
    events: list[dict[str, Any]] = []
    for envelope in envelopes:
        parsed_payloads: list[Mapping[str, Any]] = []
        if isinstance(envelope, Mapping):
            if isinstance(envelope.get("data"), Mapping):
                parsed_payloads.append(envelope)
            content = envelope.get("content")
            if isinstance(content, list):
                for block in content:
                    if not isinstance(block, Mapping) or not isinstance(
                        block.get("text"), str
                    ):
                        continue
                    try:
                        parsed = json.loads(str(block["text"]))
                    except json.JSONDecodeError as exc:
                        raise HistoricalDiscoveryError(
                            "earnings connector returned invalid JSON text"
                        ) from exc
                    if isinstance(parsed, Mapping):
                        parsed_payloads.append(parsed)
        for parsed in parsed_payloads:
            data = parsed.get("data")
            rows = data.get("results") if isinstance(data, Mapping) else None
            if not isinstance(rows, list):
                continue
            events.extend(dict(row) for row in rows if isinstance(row, Mapping))

    unique: dict[tuple[Any, ...], dict[str, Any]] = {}
    for event in events:
        report = event.get("report")
        if not isinstance(report, Mapping) or not isinstance(report.get("date"), str):
            continue
        symbol = str(event.get("symbol") or "").strip().upper()
        if not symbol:
            continue
        normalized = dict(event)
        normalized["symbol"] = symbol
        key = (
            symbol,
            report.get("date"),
            report.get("timing"),
            event.get("year"),
            event.get("quarter"),
        )
        previous = unique.get(key)
        if previous is None:
            unique[key] = normalized
            continue
        previous_report = previous.get("report")
        if isinstance(previous_report, Mapping) and (
            report.get("verified") is True
            and previous_report.get("verified") is not True
        ):
            unique[key] = normalized
    return sorted(
        unique.values(),
        key=lambda row: (
            str(row["report"]["date"]),
            str(row["symbol"]),
            int(row.get("year") or 0),
            int(row.get("quarter") or 0),
        ),
    )


class _SecRateLimiter:
    def __init__(self, minimum_spacing_seconds: float):
        self.minimum_spacing_seconds = minimum_spacing_seconds
        self._lock = threading.Lock()
        self._next_at = 0.0

    def wait(self) -> None:
        with self._lock:
            now = time.monotonic()
            scheduled = max(now, self._next_at)
            self._next_at = scheduled + self.minimum_spacing_seconds
        delay = scheduled - now
        if delay > 0:
            time.sleep(delay)


@dataclass(frozen=True)
class SecConfig:
    user_agent: str
    cache_root: Path
    workers: int = 4
    minimum_spacing_seconds: float = 0.15
    timeout_seconds: float = 30.0

    @classmethod
    def from_env(
        cls,
        env_path: Path,
        cache_root: Path,
        *,
        workers: int,
    ) -> "SecConfig":
        values: dict[str, Any] = {}
        if env_path.exists():
            values.update(dotenv_values(env_path, interpolate=False))
        values.update(
            {key: os.environ[key] for key in ("SEC_USER_AGENT",) if key in os.environ}
        )
        user_agent = str(
            values.get("SEC_USER_AGENT")
            or "robinhood-codex historical research ryan@ensomniac.com"
        ).strip()
        if "@" not in user_agent:
            raise HistoricalDiscoveryError(
                "SEC_USER_AGENT must identify the application and an email address"
            )
        if isinstance(workers, bool) or workers < 1:
            raise HistoricalDiscoveryError("SEC workers must be positive")
        return cls(user_agent=user_agent, cache_root=cache_root, workers=workers)


class SecClient:
    def __init__(self, config: SecConfig):
        self.config = config
        self._limiter = _SecRateLimiter(config.minimum_spacing_seconds)
        self._local = threading.local()
        self._stats_lock = threading.Lock()
        self._cache_hits = 0
        self._downloads = 0
        self._download_bytes = 0

    def _session(self) -> requests.Session:
        session = getattr(self._local, "session", None)
        if session is None:
            session = requests.Session()
            session.headers.update(
                {
                    "User-Agent": self.config.user_agent,
                    "Accept-Encoding": "gzip, deflate",
                }
            )
            self._local.session = session
        return session

    def _get(self, url: str, path: Path) -> bytes:
        if path.exists():
            with self._stats_lock:
                self._cache_hits += 1
            return path.read_bytes()
        last_error: Exception | None = None
        for attempt in range(4):
            self._limiter.wait()
            try:
                response = self._session().get(
                    url,
                    timeout=self.config.timeout_seconds,
                )
                if response.status_code == 429 or response.status_code >= 500:
                    raise requests.HTTPError(f"SEC HTTP {response.status_code}")
                response.raise_for_status()
                content = response.content
                path.parent.mkdir(parents=True, exist_ok=True)
                temporary = path.with_suffix(path.suffix + ".tmp")
                temporary.write_bytes(content)
                temporary.replace(path)
                with self._stats_lock:
                    self._downloads += 1
                    self._download_bytes += len(content)
                return content
            except (requests.RequestException, OSError) as exc:
                last_error = exc
                if attempt < 3:
                    time.sleep(0.5 * (2**attempt))
        raise HistoricalDiscoveryError(f"SEC request failed for {url}: {last_error}")

    def json(self, url: str, path: Path) -> Mapping[str, Any]:
        try:
            parsed = json.loads(self._get(url, path))
        except json.JSONDecodeError as exc:
            raise HistoricalDiscoveryError(
                f"SEC returned invalid JSON for {url}"
            ) from exc
        if not isinstance(parsed, Mapping):
            raise HistoricalDiscoveryError(f"SEC JSON must be an object: {url}")
        return parsed

    def text(self, url: str, path: Path) -> str:
        return self._get(url, path).decode("utf-8", errors="replace")

    def stats(self) -> dict[str, Any]:
        with self._stats_lock:
            return {
                "cache_hits": self._cache_hits,
                "downloads": self._downloads,
                "download_bytes": self._download_bytes,
                "workers": self.config.workers,
                "minimum_spacing_seconds": self.config.minimum_spacing_seconds,
            }


def _quarter(day: date) -> int:
    return (day.month - 1) // 3 + 1


def _master_url(day: str) -> str:
    parsed = date.fromisoformat(day)
    return (
        "https://www.sec.gov/Archives/edgar/daily-index/"
        f"{parsed.year}/QTR{_quarter(parsed)}/master.{parsed.strftime('%Y%m%d')}.idx"
    )


def _quarter_index_url(day: str) -> str:
    parsed = date.fromisoformat(day)
    return (
        "https://www.sec.gov/Archives/edgar/daily-index/"
        f"{parsed.year}/QTR{_quarter(parsed)}/index.json"
    )


def _load_master_index(client: SecClient, day: str) -> str:
    parsed = date.fromisoformat(day)
    expected_name = f"master.{parsed.strftime('%Y%m%d')}.idx"
    path = client.config.cache_root / "master" / f"{day}.idx"
    try:
        return client.text(_master_url(day), path)
    except HistoricalDiscoveryError as original:
        quarter = _quarter(parsed)
        listing = client.json(
            _quarter_index_url(day),
            client.config.cache_root
            / "master"
            / f"{parsed.year}-QTR{quarter}-index.json",
        )
        directory = listing.get("directory")
        items = directory.get("item") if isinstance(directory, Mapping) else None
        if not isinstance(items, list):
            raise original
        names = {
            str(item.get("name"))
            for item in items
            if isinstance(item, Mapping) and item.get("name")
        }
        if expected_name in names:
            raise original

        # EDGAR has no daily master file on some market-open federal holidays.
        # Cache the verified empty result so a resumed large batch does not
        # repeatedly request a resource the official quarter index omits.
        path.parent.mkdir(parents=True, exist_ok=True)
        temporary = path.with_suffix(path.suffix + ".tmp")
        temporary.write_bytes(b"")
        temporary.replace(path)
        return ""


def parse_master_index(text: str) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    started = False
    for line in text.splitlines():
        if not started:
            if line.startswith("-----"):
                started = True
            continue
        parts = line.split("|")
        if len(parts) != 5:
            continue
        cik, company, form, filed_on, filename = (part.strip() for part in parts)
        rows.append(
            {
                "cik": cik,
                "company": company,
                "form": form,
                "filed_on": filed_on,
                "filename": filename,
            }
        )
    return rows


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


def _submission_recent(payload: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    filings = payload.get("filings")
    recent = filings.get("recent") if isinstance(filings, Mapping) else None
    if not isinstance(recent, Mapping):
        return {}
    return {
        str(row.get("accessionNumber")): row
        for row in _columnar_rows(recent)
        if row.get("accessionNumber")
    }


def _accession(filename: str) -> str:
    return Path(filename).stem


def _filing_items(value: Any) -> list[str]:
    if not isinstance(value, str):
        return []
    return sorted(set(re.findall(r"\d+\.\d+", value)))


def _accepted_at(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=EASTERN)
    return parsed.astimezone(EASTERN)


def _surprise(event: Mapping[str, Any]) -> float | None:
    eps = event.get("eps")
    if not isinstance(eps, Mapping):
        return None
    try:
        actual = float(eps["actual"])
        estimate = float(eps["estimate"])
    except (KeyError, TypeError, ValueError):
        return None
    return abs(actual - estimate) / max(abs(estimate), 0.10)


def _document_has_dilution(text: str, items: Sequence[str]) -> bool:
    if "3.02" in items:
        return True
    normalized = re.sub(r"\s+", " ", text.lower())
    return any(pattern in normalized for pattern in STRONG_DILUTION_PATTERNS)


def _prior_trading_days(calendar: Sequence[str], day: str, *, count: int) -> list[str]:
    ordered = sorted(calendar)
    try:
        index = ordered.index(day)
    except ValueError as exc:
        raise HistoricalDiscoveryError(
            f"selected date is not in calendar: {day}"
        ) from exc
    if index < count:
        raise HistoricalDiscoveryError(
            f"calendar lacks {count} prior sessions for {day}"
        )
    return ordered[index - count : index]


def _ticker_map(payload: Mapping[str, Any]) -> dict[str, list[dict[str, str]]]:
    fields = payload.get("fields")
    data = payload.get("data")
    if not isinstance(fields, list) or not isinstance(data, list):
        raise HistoricalDiscoveryError("SEC ticker exchange file has invalid shape")
    result: dict[str, list[dict[str, str]]] = defaultdict(list)
    for values in data:
        if not isinstance(values, list):
            continue
        row = dict(zip((str(field) for field in fields), values))
        exchange = str(row.get("exchange") or "")
        ticker = str(row.get("ticker") or "").strip().upper()
        cik = str(row.get("cik") or "").lstrip("0")
        if (
            exchange not in {"Nasdaq", "NYSE"}
            or not ticker
            or not ticker.replace(".", "").isalnum()
            or not cik
        ):
            continue
        result[cik].append(
            {
                "ticker": ticker,
                "exchange": exchange,
                "name": str(row.get("name") or ""),
            }
        )
    # The SEC file can map one operating registrant to common, preferred, and
    # depositary tickers. Historical discovery needs one issuer candidate, not
    # every financing instrument. Prefer the shortest symbol (then lexical
    # order) and leave final common-stock proof to IBKR stockType=COMMON.
    return {
        cik: [min(rows, key=lambda row: (len(row["ticker"]), row["ticker"]))]
        for cik, rows in result.items()
    }


def _ciks_by_ticker(
    tickers_by_cik: Mapping[str, Sequence[Mapping[str, str]]],
) -> dict[str, set[str]]:
    result: dict[str, set[str]] = defaultdict(set)
    for cik, rows in tickers_by_cik.items():
        for row in rows:
            ticker = row.get("ticker")
            if ticker:
                result[str(ticker).upper()].add(cik)
    return result


def _build_filing_record(
    master: Mapping[str, str],
    metadata: Mapping[str, Any],
    ticker: Mapping[str, str],
) -> dict[str, Any] | None:
    accepted = _accepted_at(metadata.get("acceptanceDateTime"))
    primary = str(metadata.get("primaryDocument") or "").strip()
    accession = str(metadata.get("accessionNumber") or _accession(master["filename"]))
    items = _filing_items(metadata.get("items"))
    if accepted is None or not primary or not MATERIAL_ITEMS.intersection(items):
        return None
    cik = str(master["cik"]).lstrip("0")
    accession_path = accession.replace("-", "")
    source_url = (
        f"https://www.sec.gov/Archives/edgar/data/{cik}/{accession_path}/{primary}"
    )
    return {
        "symbol": ticker["ticker"],
        "exchange": ticker["exchange"],
        "company": ticker["name"],
        "cik": cik,
        "accession": accession,
        "accepted_at": accepted.isoformat(),
        "filed_on": master["filed_on"],
        "items": items,
        "primary_document": primary,
        "source_url": source_url,
    }


def build_draft(
    selection: Mapping[str, Any],
    trading_days: Sequence[str],
    earnings_events: Sequence[Mapping[str, Any]],
    client: SecClient,
    *,
    buffer_limit: int,
) -> dict[str, Any]:
    selected = selection.get("selected_dates")
    if not isinstance(selected, list) or not all(
        isinstance(day, str) for day in selected
    ):
        raise HistoricalDiscoveryError("selection needs selected_dates")
    if isinstance(buffer_limit, bool) or buffer_limit < 10:
        raise HistoricalDiscoveryError("buffer_limit must be at least 10")
    prior = {day: _prior_trading_days(trading_days, day, count=2) for day in selected}
    source_days = sorted(
        set(selected) | {source_day for days in prior.values() for source_day in days}
    )

    ticker_payload = client.json(
        SEC_TICKERS_URL,
        client.config.cache_root / "company_tickers_exchange.json",
    )
    tickers_by_cik = _ticker_map(ticker_payload)
    ciks_by_ticker = _ciks_by_ticker(tickers_by_cik)

    relevant_symbols: set[str] = set()
    source_days_by_target = {day: set(prior[day]) | {day} for day in selected}
    relevant_days = set(source_days)
    for event in earnings_events:
        report = event.get("report")
        eps = event.get("eps")
        if (
            isinstance(report, Mapping)
            and report.get("verified") is True
            and report.get("date") in relevant_days
            and isinstance(eps, Mapping)
            and eps.get("actual") is not None
            and eps.get("estimate") is not None
        ):
            relevant_symbols.add(str(event.get("symbol") or "").upper())
    relevant_ciks = {
        cik for symbol in relevant_symbols for cik in ciks_by_ticker.get(symbol, set())
    }

    def load_master(day: str) -> tuple[str, list[dict[str, str]]]:
        text = _load_master_index(client, day)
        return day, parse_master_index(text)

    master_by_day: dict[str, list[dict[str, str]]] = {}
    for outcome in ordered_bounded_results(
        source_days, load_master, max_workers=client.config.workers
    ):
        day, rows = outcome.unwrap()
        master_by_day[day] = [
            row
            for row in rows
            if row["form"] == "8-K" and row["cik"].lstrip("0") in relevant_ciks
        ]

    ciks = sorted(
        {row["cik"].lstrip("0") for rows in master_by_day.values() for row in rows}
    )

    def load_submission(cik: str) -> tuple[str, Mapping[str, Any]]:
        padded = cik.zfill(10)
        payload = client.json(
            f"{SEC_SUBMISSIONS_ROOT}/CIK{padded}.json",
            client.config.cache_root / "submissions" / f"CIK{padded}.json",
        )
        return cik, payload

    submissions: dict[str, dict[str, dict[str, Any]]] = {}
    for outcome in ordered_bounded_results(
        ciks, load_submission, max_workers=client.config.workers
    ):
        cik, payload = outcome.unwrap()
        submissions[cik] = _submission_recent(payload)

    filings_by_day: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for day, masters in master_by_day.items():
        for master in masters:
            cik = master["cik"].lstrip("0")
            metadata = submissions.get(cik, {}).get(_accession(master["filename"]))
            if not isinstance(metadata, Mapping):
                continue
            for ticker in tickers_by_cik[cik]:
                record = _build_filing_record(master, metadata, ticker)
                if record is not None:
                    filings_by_day[day].append(record)

    events_by_key: dict[tuple[str, str, str], Mapping[str, Any]] = {}
    for event in earnings_events:
        report = event.get("report")
        eps = event.get("eps")
        if (
            not isinstance(report, Mapping)
            or report.get("verified") is not True
            or report.get("timing") not in {"am", "pm"}
            or not isinstance(report.get("date"), str)
            or not isinstance(eps, Mapping)
            or eps.get("actual") is None
            or eps.get("estimate") is None
        ):
            continue
        key = (
            str(event.get("symbol") or "").upper(),
            str(report["date"]),
            str(report["timing"]),
        )
        candidate = events_by_key.get(key)
        if candidate is None or (_surprise(event) or -1) > (_surprise(candidate) or -1):
            events_by_key[key] = event

    preliminary_by_day: dict[str, list[dict[str, Any]]] = {}
    for day in selected:
        boundary = datetime.combine(date.fromisoformat(day), wall_time(9, 30), EASTERN)
        source_rows = [
            row
            for source_day in sorted(source_days_by_target[day])
            for row in filings_by_day[source_day]
        ]
        eligible = [
            row
            for row in source_rows
            if (_accepted_at(row["accepted_at"]) or boundary) <= boundary
        ]
        by_symbol: dict[str, dict[str, Any]] = {}
        for filing in eligible:
            symbol = filing["symbol"]
            accepted = _accepted_at(filing["accepted_at"])
            timing = "am" if filing["filed_on"] == day else "pm"
            event = events_by_key.get((symbol, filing["filed_on"], timing))
            surprise = _surprise(event) if event is not None else None
            priority = min(
                ITEM_PRIORITY[item] for item in filing["items"] if item in ITEM_PRIORITY
            )
            row = dict(filing)
            row["earnings"] = dict(event) if event is not None else None
            row["surprise"] = surprise
            row["rank_key"] = (
                0 if surprise is not None and "2.02" in filing["items"] else 1,
                -(surprise or 0.0),
                priority,
                -(accepted.timestamp() if accepted is not None else 0.0),
                symbol,
            )
            previous_row = by_symbol.get(symbol)
            if previous_row is None or row["rank_key"] < previous_row["rank_key"]:
                by_symbol[symbol] = row
        preliminary_by_day[day] = sorted(
            by_symbol.values(), key=lambda row: row["rank_key"]
        )[: buffer_limit * 2]

    unique_documents = {
        row["source_url"]: row for rows in preliminary_by_day.values() for row in rows
    }

    def load_document(item: tuple[str, Mapping[str, Any]]) -> tuple[str, bool]:
        url, row = item
        safe_name = hashlib.sha256(url.encode("utf-8")).hexdigest() + ".html"
        text = client.text(
            url,
            client.config.cache_root / "documents" / safe_name,
        )
        return url, _document_has_dilution(text, row["items"])

    dilution_by_url: dict[str, bool] = {}
    for outcome in ordered_bounded_results(
        sorted(unique_documents.items()),
        load_document,
        max_workers=client.config.workers,
    ):
        url, conflict = outcome.unwrap()
        dilution_by_url[url] = conflict

    pools: dict[str, list[dict[str, Any]]] = {}
    discovery_reports: dict[str, dict[str, Any]] = {}
    for day in selected:
        pool: list[dict[str, Any]] = []
        rejected_dilution = 0
        for row in preliminary_by_day[day]:
            if dilution_by_url.get(row["source_url"], False):
                rejected_dilution += 1
                continue
            event = row.get("earnings")
            eps = event.get("eps") if isinstance(event, Mapping) else None
            report = event.get("report") if isinstance(event, Mapping) else None
            pool.append(
                {
                    "symbol": row["symbol"],
                    "surprise_rank": len(pool) + 1,
                    "report_date": (
                        report.get("date")
                        if isinstance(report, Mapping)
                        else row["filed_on"]
                    ),
                    "report_timing": (
                        report.get("timing")
                        if isinstance(report, Mapping)
                        else ("am" if row["filed_on"] == day else "pm")
                    ),
                    "eps_estimate": (
                        float(eps["estimate"])
                        if isinstance(eps, Mapping) and eps.get("estimate") is not None
                        else None
                    ),
                    "eps_actual": (
                        float(eps["actual"])
                        if isinstance(eps, Mapping) and eps.get("actual") is not None
                        else None
                    ),
                    "is_common_stock": True,
                    "common_stock_basis": (
                        "SEC NYSE/Nasdaq operating-company registrant; "
                        "IBKR stockType COMMON required by preflight"
                    ),
                    "dilution_conflict": False,
                    "catalyst": {
                        "source_url": row["source_url"],
                        "published_at": row["accepted_at"],
                        "point_in_time": True,
                    },
                    "discovery": {
                        "source": "SEC EDGAR 8-K daily index and submissions metadata",
                        "form": "8-K",
                        "filing_items": ",".join(row["items"]),
                        "accepted_at": row["accepted_at"],
                        "exchange": row["exchange"],
                        "accession": row["accession"],
                        "earnings_calendar_cross_check": event is not None,
                    },
                }
            )
            if len(pool) == buffer_limit:
                break
        pools[day] = pool
        discovery_reports[day] = {
            "source_sessions": sorted(source_days_by_target[day]),
            "sec_material_rows_before_symbol_dedup": len(
                [
                    row
                    for source_day in source_days_by_target[day]
                    for row in filings_by_day[source_day]
                ]
            ),
            "ranked_before_dilution_screen": len(preliminary_by_day[day]),
            "dilution_conflicts_rejected": rejected_dilution,
            "draft_pool_count": len(pool),
            "discovery_ready": len(pool) >= 10,
        }

    selection_seed = selection.get("seed")
    return {
        "schema_version": 1,
        "parent_selection_file": selection.get("selection_file"),
        "selection_seed": selection_seed,
        "prepared_at": datetime.now(UTC).isoformat(),
        "scanner": {
            "provider": "Robinhood earnings calendar + SEC EDGAR daily index, submissions metadata, and issuer filings",
            "point_in_time": True,
            "selection_policy": "All dates frozen before source collection; no target-session market prices were requested.",
            "universe_scope": "Verified earnings symbols from the prior two sessions and target date, cross-checked against mapped NYSE/Nasdaq 8-K filings accepted by 09:30 ET.",
            "ranking": "Verified Item 2.02 earnings by normalized absolute EPS surprise, then material SEC item priority, acceptance time, and symbol.",
            "eligibility": "Direct material issuer 8-K accepted before the replay boundary, mapped to NYSE/Nasdaq; IBKR COMMON stockType required before freeze.",
            "dilution_screen": "Item 3.02 plus strong primary-document financing and common-stock issuance phrases.",
            "buffer_target": f"Up to {buffer_limit} candidates per date before IBKR preflight; minimum 10.",
            "universe_capture_complete": True,
            "target_session_prices_observed": False,
            "sec_cache": client.stats(),
            "dates": discovery_reports,
        },
        "candidate_pool_by_date": pools,
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)

    ingest = commands.add_parser(
        "ingest-earnings",
        help="read connector result JSON from stdin and write normalized events",
    )
    ingest.add_argument("--output", type=Path, required=True)

    build = commands.add_parser(
        "build",
        help="build ranked SEC-cross-checked draft pools for a frozen selection",
    )
    build.add_argument("selection", type=Path)
    build.add_argument("trading_days", type=Path)
    build.add_argument("earnings", type=Path)
    build.add_argument("--output", type=Path, required=True)
    build.add_argument("--cache-root", type=Path, default=DEFAULT_CACHE_ROOT)
    build.add_argument("--env-file", type=Path, default=DEFAULT_ENV_PATH)
    build.add_argument("--workers", type=int, default=4)
    build.add_argument("--buffer-limit", type=int, default=80)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    try:
        if args.command == "ingest-earnings":
            raw = sys.stdin.readline()
            if not raw:
                raise HistoricalDiscoveryError("stdin did not contain connector JSON")
            events = extract_earnings_events(json.loads(raw))
            payload = {
                "schema_version": 1,
                "provider": "Robinhood market-wide earnings calendar",
                "captured_at": datetime.now(UTC).isoformat(),
                "event_count": len(events),
                "events": events,
            }
            _atomic_json(args.output, payload)
            print(json.dumps({"written": str(args.output), "events": len(events)}))
            return 0

        selection = _load_json(args.selection)
        calendar = _load_json(args.trading_days)
        earnings = _load_json(args.earnings)
        if not isinstance(selection, Mapping) or not isinstance(calendar, list):
            raise HistoricalDiscoveryError("invalid selection or trading-day input")
        events = earnings.get("events") if isinstance(earnings, Mapping) else None
        if not isinstance(events, list):
            raise HistoricalDiscoveryError("earnings input needs an events array")
        selection = dict(selection)
        selection["selection_file"] = str(args.selection)
        client = SecClient(
            SecConfig.from_env(
                args.env_file,
                args.cache_root,
                workers=args.workers,
            )
        )
        draft = build_draft(
            selection,
            calendar,
            [row for row in events if isinstance(row, Mapping)],
            client,
            buffer_limit=args.buffer_limit,
        )
        _atomic_json(args.output, draft)
        ready = sum(
            len(pool) >= 10 for pool in draft["candidate_pool_by_date"].values()
        )
        print(
            json.dumps(
                {
                    "written": str(args.output),
                    "dates": len(draft["candidate_pool_by_date"]),
                    "discovery_ready_dates": ready,
                    "sec_cache": client.stats(),
                },
                indent=2,
            )
        )
        return 0
    except (
        HistoricalDiscoveryError,
        OSError,
        ValueError,
        json.JSONDecodeError,
    ) as exc:
        print(
            json.dumps({"error": str(exc), "error_type": type(exc).__name__}),
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

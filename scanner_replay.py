"""Build a point-in-time, full-universe 09:35 ET scanner replay dataset.

The collector deliberately separates three phases:

1. collect dated security-reference snapshots and build the security master;
2. freeze the dates, source identities, and scanner rules without target bars;
3. ingest market-wide SIP minute files and derive the 09:35 rankings.

Raw provider files and detailed per-symbol evaluations stay below the ignored
``learning_runs`` tree. Compact, hash-bound provenance and coverage summaries
are suitable for the public repository. This module never calls a broker.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import hmac
import json
import os
import sys
import time
from collections import Counter, defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, time as wall_time, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlparse
from zoneinfo import ZoneInfo

import requests
from dotenv import dotenv_values

from learning_data import (
    LearningDataError,
    load_security_master,
    security_record_covers,
    security_master_sha256,
    validate_dataset_payload,
    validate_security_record,
)


PROJECT_ROOT = Path(__file__).resolve().parent
EASTERN = ZoneInfo("America/New_York")
UTC = timezone.utc
DEFAULT_RUN_ROOT = PROJECT_ROOT / "learning_runs" / "scanner_replay"
DEFAULT_SECURITY_MASTER = PROJECT_ROOT / "learning" / "SECURITY_MASTER.jsonl"
DEFAULT_RULES = (
    PROJECT_ROOT / "historical_batches" / "scanner_replay" / "scanner-rules-v1.json"
)
DEFAULT_SOURCE_MANIFEST = (
    PROJECT_ROOT
    / "historical_batches"
    / "scanner_replay"
    / "security-master-source.json"
)
MASSIVE_REFERENCE_URL = "https://api.massive.com/v3/reference/tickers"
MASSIVE_FLAT_ENDPOINT = "https://files.massive.com"
MASSIVE_FLAT_BUCKET = "flatfiles"
MASSIVE_FLAT_REGION = "us-east-1"
MASSIVE_MINUTE_PREFIX = "us_stocks_sip/minute_aggs_v1"
MINUTE_COLUMNS = {
    "ticker",
    "volume",
    "open",
    "close",
    "high",
    "low",
    "window_start",
    "transactions",
}


class ScannerReplayError(RuntimeError):
    """A fidelity, source, or local-state failure."""

    def __init__(self, message: str, *, category: str = "fidelity"):
        super().__init__(message)
        self.category = category
        self.retryable = category.startswith("retryable_")


def _read_object(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ScannerReplayError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise ScannerReplayError(f"{path} must contain an object")
    return value


def _write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    rendered = json.dumps(value, indent=2, sort_keys=True) + "\n"
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(rendered, encoding="utf-8")
    temporary.replace(path)


def _repo_path(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(PROJECT_ROOT))
    except ValueError as exc:
        raise ScannerReplayError(
            f"path must remain inside the repository: {path}"
        ) from exc


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha256_json(value: Any) -> str:
    rendered = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
    return _sha256_bytes(rendered)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _timestamp_now() -> str:
    return datetime.now(EASTERN).isoformat()


def _iso_dates(value: Any, field: str) -> list[str]:
    if not isinstance(value, list) or not value:
        raise ScannerReplayError(f"{field} must be a non-empty date array")
    parsed: list[date] = []
    for item in value:
        if not isinstance(item, str):
            raise ScannerReplayError(f"{field} must contain ISO dates")
        try:
            parsed.append(date.fromisoformat(item))
        except ValueError as exc:
            raise ScannerReplayError(f"{field} contains an invalid date") from exc
    if len(parsed) != len(set(parsed)) or parsed != sorted(parsed):
        raise ScannerReplayError(f"{field} must be unique and chronological")
    return [item.isoformat() for item in parsed]


def load_selection(path: Path, *, expected_count: int | None = None) -> dict[str, Any]:
    value = _read_object(path)
    selected = value.get("selected_dates")
    if not isinstance(selected, list):
        raise ScannerReplayError("selection must contain selected_dates")
    normalized: list[str] = []
    for item in selected:
        if not isinstance(item, str):
            raise ScannerReplayError("selected_dates must contain ISO dates")
        try:
            normalized.append(date.fromisoformat(item).isoformat())
        except ValueError as exc:
            raise ScannerReplayError("selected_dates contains an invalid date") from exc
    if len(normalized) != len(set(normalized)):
        raise ScannerReplayError("selected_dates must be unique")
    if not normalized:
        raise ScannerReplayError("selected_dates must not be empty")
    if expected_count is not None and len(normalized) != expected_count:
        raise ScannerReplayError(
            f"selection must freeze exactly {expected_count} dates"
        )
    seed = value.get("seed")
    if not isinstance(seed, int):
        raise ScannerReplayError("selection must contain an integer seed")
    return {**value, "selected_dates": normalized, "seed": seed}


def load_calendar(path: Path) -> list[str]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ScannerReplayError(f"cannot read calendar {path}: {exc}") from exc
    return _iso_dates(value, "calendar")


def required_sessions(
    selected_dates: Sequence[str], calendar: Sequence[str], *, prior_sessions: int = 15
) -> list[str]:
    ordered = _iso_dates(list(calendar), "calendar")
    positions = {day: index for index, day in enumerate(ordered)}
    required: set[str] = set()
    for target in selected_dates:
        if target not in positions:
            raise ScannerReplayError(f"selected date {target} is absent from calendar")
        index = positions[target]
        if index < prior_sessions:
            raise ScannerReplayError(
                f"calendar lacks {prior_sessions} prior sessions for {target}"
            )
        required.update(ordered[index - prior_sessions : index + 1])
    return sorted(required)


@dataclass(frozen=True)
class MassiveReferenceConfig:
    api_key: str
    base_url: str = "https://api.massive.com"
    timeout_seconds: float = 30.0
    minimum_interval_seconds: float = 12.5

    @classmethod
    def from_env(cls, path: Path) -> "MassiveReferenceConfig":
        values: dict[str, Any] = {}
        if path.exists():
            values.update(dotenv_values(path, interpolate=False))
        for key in (
            "MASSIVE_API_KEY",
            "MASSIVE_BASE_URL",
            "MASSIVE_TIMEOUT_SECONDS",
            "MASSIVE_REFERENCE_MINIMUM_INTERVAL_SECONDS",
        ):
            if key in os.environ:
                values[key] = os.environ[key]
        api_key = str(values.get("MASSIVE_API_KEY") or "").strip()
        if not api_key:
            raise ScannerReplayError(
                "MASSIVE_API_KEY is required for dated reference snapshots",
                category="configuration",
            )
        base_url = str(
            values.get("MASSIVE_BASE_URL") or "https://api.massive.com"
        ).rstrip("/")
        parsed = urlparse(base_url)
        if parsed.scheme != "https" or not parsed.netloc:
            raise ScannerReplayError("MASSIVE_BASE_URL must be an HTTPS origin")
        try:
            timeout = float(values.get("MASSIVE_TIMEOUT_SECONDS") or 30.0)
            interval = float(
                values.get("MASSIVE_REFERENCE_MINIMUM_INTERVAL_SECONDS") or 12.5
            )
        except (TypeError, ValueError) as exc:
            raise ScannerReplayError("Massive timing values must be numeric") from exc
        if timeout <= 0 or interval < 0:
            raise ScannerReplayError("Massive timing values are out of range")
        return cls(api_key, base_url, timeout, interval)

    def public_dict(self) -> dict[str, Any]:
        return {
            "base_url": self.base_url,
            "timeout_seconds": self.timeout_seconds,
            "minimum_interval_seconds": self.minimum_interval_seconds,
            "api_key_configured": bool(self.api_key),
        }


class MassiveReferenceCollector:
    """Rate-limited dated ticker snapshots with safe pagination."""

    def __init__(
        self,
        config: MassiveReferenceConfig,
        *,
        session: requests.Session | None = None,
        sleeper: Any = time.sleep,
    ):
        self.config = config
        self.session = session or requests.Session()
        self._owns_session = session is None
        self._sleeper = sleeper
        self._last_request_started: float | None = None

    def close(self) -> None:
        if self._owns_session:
            self.session.close()

    def __enter__(self) -> "MassiveReferenceCollector":
        return self

    def __exit__(self, exc_type, exc, traceback_obj) -> None:
        self.close()

    def _throttle(self) -> None:
        if self._last_request_started is not None:
            elapsed = time.monotonic() - self._last_request_started
            remaining = self.config.minimum_interval_seconds - elapsed
            if remaining > 0:
                self._sleeper(remaining)
        self._last_request_started = time.monotonic()

    def _get(self, url: str, params: Mapping[str, Any]) -> Mapping[str, Any]:
        origin = urlparse(self.config.base_url).netloc
        parsed = urlparse(url)
        if parsed.scheme != "https" or parsed.netloc != origin:
            raise ScannerReplayError("Massive pagination left the configured origin")
        attempts = 0
        while True:
            attempts += 1
            self._throttle()
            try:
                response = self.session.get(
                    url,
                    params={**params, "apiKey": self.config.api_key},
                    timeout=self.config.timeout_seconds,
                )
            except (requests.Timeout, requests.ConnectionError) as exc:
                if attempts >= 6:
                    raise ScannerReplayError(
                        "Massive reference transport retries exhausted",
                        category="retryable_transport",
                    ) from exc
                self._sleeper(min(60.0, 2.0**attempts))
                continue
            if response.status_code == 429 or response.status_code >= 500:
                if attempts >= 10:
                    raise ScannerReplayError(
                        f"Massive reference HTTP {response.status_code}",
                        category="retryable_provider",
                    )
                retry_after = response.headers.get("Retry-After")
                try:
                    delay = float(retry_after) if retry_after else 60.0
                except ValueError:
                    delay = 60.0
                self._sleeper(
                    min(60.0, max(delay, self.config.minimum_interval_seconds))
                )
                continue
            if response.status_code >= 400:
                category = (
                    "permission"
                    if response.status_code in (401, 403)
                    else "permanent_provider"
                )
                raise ScannerReplayError(
                    f"Massive reference HTTP {response.status_code}", category=category
                )
            try:
                payload = response.json()
            except ValueError as exc:
                raise ScannerReplayError(
                    "Massive reference returned invalid JSON"
                ) from exc
            if not isinstance(payload, Mapping) or str(
                payload.get("status", "")
            ).upper() not in (
                "OK",
                "DELAYED",
            ):
                raise ScannerReplayError("Massive reference response status is invalid")
            return payload

    def fetch(self, as_of: str) -> list[dict[str, Any]]:
        date.fromisoformat(as_of)
        url = f"{self.config.base_url}/v3/reference/tickers"
        params: dict[str, Any] = {
            "market": "stocks",
            "locale": "us",
            "type": "CS",
            "date": as_of,
            "active": "true",
            "sort": "ticker",
            "order": "asc",
            "limit": 1000,
        }
        rows: list[dict[str, Any]] = []
        pages = 0
        while url:
            pages += 1
            if pages > 50:
                raise ScannerReplayError("Massive reference exceeded 50 pages")
            payload = self._get(url, params)
            values = payload.get("results")
            if not isinstance(values, list):
                raise ScannerReplayError("Massive reference results must be an array")
            rows.extend(dict(item) for item in values if isinstance(item, Mapping))
            next_url = payload.get("next_url")
            url = str(next_url) if next_url else ""
            params = {}
        symbols = [str(item.get("ticker") or "") for item in rows]
        if (
            not rows
            or any(not symbol for symbol in symbols)
            or len(symbols) != len(set(symbols))
        ):
            raise ScannerReplayError("Massive reference snapshot is empty or ambiguous")
        return sorted(rows, key=lambda item: str(item["ticker"]))

    def fetch_splits(self, start: str, end: str) -> list[dict[str, Any]]:
        date.fromisoformat(start)
        date.fromisoformat(end)
        url = f"{self.config.base_url}/stocks/v1/splits"
        params: dict[str, Any] = {
            "execution_date.gte": start,
            "execution_date.lte": end,
            "sort": "execution_date.asc",
            "limit": 5000,
        }
        rows: list[dict[str, Any]] = []
        pages = 0
        while url:
            pages += 1
            if pages > 20:
                raise ScannerReplayError("Massive split history exceeded 20 pages")
            payload = self._get(url, params)
            values = payload.get("results")
            if not isinstance(values, list):
                raise ScannerReplayError("Massive split results must be an array")
            rows.extend(dict(item) for item in values if isinstance(item, Mapping))
            next_url = payload.get("next_url")
            url = str(next_url) if next_url else ""
            params = {}
        for row in rows:
            try:
                execution = date.fromisoformat(str(row["execution_date"]))
                split_from = float(row["split_from"])
                split_to = float(row["split_to"])
                ticker = str(row["ticker"]).strip().upper()
            except (KeyError, TypeError, ValueError) as exc:
                raise ScannerReplayError("Massive split row is malformed") from exc
            if not date.fromisoformat(start) <= execution <= date.fromisoformat(end):
                raise ScannerReplayError(
                    "Massive split row is outside the requested range"
                )
            if split_from <= 0 or split_to <= 0 or not ticker:
                raise ScannerReplayError("Massive split ratio or ticker is invalid")
        return sorted(
            rows, key=lambda item: (str(item["execution_date"]), str(item["ticker"]))
        )


def collect_reference_snapshots(
    selected_dates: Sequence[str],
    *,
    config: MassiveReferenceConfig,
    output_root: Path,
) -> dict[str, Any]:
    output_root.mkdir(parents=True, exist_ok=True)
    completed: list[dict[str, Any]] = []
    with MassiveReferenceCollector(config) as collector:
        for index, day in enumerate(selected_dates, 1):
            output = output_root / f"{day}.json.gz"
            if output.exists():
                with gzip.open(output, "rt", encoding="utf-8") as source:
                    cached = json.load(source)
                if not isinstance(cached, list) or not cached:
                    raise ScannerReplayError(
                        f"cached reference snapshot is invalid: {day}"
                    )
                rows = cached
                disposition = "cached"
            else:
                rows = collector.fetch(day)
                temporary = output.with_suffix(output.suffix + ".tmp")
                with gzip.open(temporary, "wt", encoding="utf-8") as target:
                    json.dump(rows, target, sort_keys=True, separators=(",", ":"))
                temporary.replace(output)
                disposition = "collected"
            completed.append(
                {
                    "date": day,
                    "rows": len(rows),
                    "sha256": _sha256_file(output),
                    "disposition": disposition,
                }
            )
            print(
                f"reference {index}/{len(selected_dates)} {day}: {len(rows)} rows ({disposition})",
                flush=True,
            )
    return {
        "valid": True,
        "provider": "Massive dated ticker reference",
        "endpoint": MASSIVE_REFERENCE_URL,
        "requested_dates": list(selected_dates),
        "snapshots": completed,
    }


def collect_split_actions(
    *,
    start: str,
    end: str,
    config: MassiveReferenceConfig,
    output: Path,
) -> dict[str, Any]:
    if output.exists():
        with gzip.open(output, "rt", encoding="utf-8") as source:
            rows = json.load(source)
        disposition = "cached"
    else:
        with MassiveReferenceCollector(config) as collector:
            rows = collector.fetch_splits(start, end)
        output.parent.mkdir(parents=True, exist_ok=True)
        temporary = output.with_suffix(output.suffix + ".tmp")
        with gzip.open(temporary, "wt", encoding="utf-8") as target:
            json.dump(rows, target, sort_keys=True, separators=(",", ":"))
        temporary.replace(output)
        disposition = "collected"
    if not isinstance(rows, list):
        raise ScannerReplayError("cached split history is malformed")
    return {
        "valid": True,
        "provider": "Massive stock splits",
        "endpoint": "https://api.massive.com/stocks/v1/splits",
        "start": start,
        "end": end,
        "events": len(rows),
        "sha256": _sha256_file(output),
        "disposition": disposition,
    }


def _security_type(value: Any) -> str:
    normalized = str(value or "").upper()
    if normalized == "CS":
        return "COMMON"
    if normalized.startswith("ADR"):
        return "ADR"
    if normalized in {"ETF", "ETV"}:
        return "ETF"
    return "OTHER"


def _instrument_id(row: Mapping[str, Any]) -> tuple[str, str]:
    # A share-class FIGI can intentionally span multiple simultaneously tradable
    # listings.  Composite FIGI is the narrowest stable identity Massive exposes
    # in this endpoint; retain share class only as a fallback.
    for field, prefix in (
        ("composite_figi", "FIGI-COMPOSITE"),
        ("share_class_figi", "FIGI-SHARE"),
    ):
        value = str(row.get(field) or "").strip()
        if value:
            return f"{prefix}:{value}", field
    identity = "|".join(
        str(row.get(field) or "").strip()
        for field in ("ticker", "primary_exchange", "cik", "name")
    )
    return f"MASSIVE-FALLBACK:{_sha256_bytes(identity.encode())[:24]}", "fallback"


def _snapshot_key(row: Mapping[str, Any]) -> tuple[str, str, str, str]:
    instrument_id, _ = _instrument_id(row)
    return (
        instrument_id,
        str(row.get("ticker") or "").strip().upper(),
        str(row.get("primary_exchange") or "UNKNOWN").strip().upper(),
        _security_type(row.get("type")),
    )


def build_security_master(
    selected_dates: Sequence[str],
    *,
    snapshots_root: Path,
    output: Path,
    source_manifest: Path,
    recorded_at: str | None = None,
) -> dict[str, Any]:
    dates = _iso_dates(sorted(selected_dates), "selected_dates")
    by_date: dict[str, dict[tuple[str, str, str, str], Mapping[str, Any]]] = {}
    source_rows: list[dict[str, Any]] = []
    for day in dates:
        path = snapshots_root / f"{day}.json.gz"
        try:
            with gzip.open(path, "rt", encoding="utf-8") as source:
                raw = json.load(source)
        except (OSError, json.JSONDecodeError) as exc:
            raise ScannerReplayError(
                f"cannot read reference snapshot {day}: {exc}"
            ) from exc
        if not isinstance(raw, list) or not raw:
            raise ScannerReplayError(f"reference snapshot {day} is empty")
        mapped: dict[tuple[str, str, str, str], Mapping[str, Any]] = {}
        for item in raw:
            if not isinstance(item, Mapping):
                raise ScannerReplayError(
                    f"reference snapshot {day} has a malformed row"
                )
            key = _snapshot_key(item)
            if not key[1] or key in mapped:
                raise ScannerReplayError(
                    f"reference snapshot {day} has ambiguous identities"
                )
            mapped[key] = item
        by_date[day] = mapped
        source_rows.append(
            {"date": day, "rows": len(raw), "sha256": _sha256_file(path)}
        )

    all_keys = sorted({key for snapshot in by_date.values() for key in snapshot})
    records: list[dict[str, Any]] = []
    provenance = _repo_path(source_manifest)
    stamp = recorded_at or _timestamp_now()
    for key in all_keys:
        present = [day for day in dates if key in by_date[day]]
        instrument_id, symbol, exchange, security_type = key
        first_row = by_date[present[0]][key]
        _, identity_source = _instrument_id(first_row)
        valid_from = present[0]
        valid_to = present[-1]
        status = "ACTIVE" if valid_to == dates[-1] else "RETIRED"
        record = {
            "schema_version": 1,
            "record_id": _sha256_json(
                {
                    "instrument_id": instrument_id,
                    "symbol": symbol,
                    "exchange": exchange,
                    "observed_dates": present,
                }
            )[:32],
            "instrument_id": instrument_id,
            "symbol": symbol,
            "primary_exchange": exchange,
            "security_type": security_type,
            "valid_from": valid_from,
            "valid_to": valid_to,
            "observed_dates": present,
            "status": status,
            "recorded_at": stamp,
            "provenance_paths": [provenance],
            "source": {
                "provider": "Massive",
                "identity_source": identity_source,
                "ticker_type": first_row.get("type"),
                "cik": first_row.get("cik"),
                "composite_figi": first_row.get("composite_figi"),
                "share_class_figi": first_row.get("share_class_figi"),
            },
        }
        validate_security_record(record)
        records.append(record)

    rendered = "".join(
        json.dumps(item, sort_keys=True, separators=(",", ":")) + "\n"
        for item in sorted(records, key=lambda item: str(item["record_id"]))
    )
    if output.exists() and output.read_text(encoding="utf-8") != rendered:
        raise ScannerReplayError(
            "security master already exists with different content; append a sourced revision instead"
        )
    output.parent.mkdir(parents=True, exist_ok=True)
    if output.exists():
        loaded = load_security_master(output)
    else:
        temporary = output.with_suffix(output.suffix + ".tmp")
        try:
            temporary.write_text(rendered, encoding="utf-8")
            loaded = load_security_master(temporary)
            temporary.replace(output)
        finally:
            temporary.unlink(missing_ok=True)
    manifest = {
        "schema_version": 1,
        "source": {
            "provider": "Massive",
            "endpoint": MASSIVE_REFERENCE_URL,
            "query_contract": {
                "market": "stocks",
                "locale": "us",
                "type": "CS",
                "active": True,
                "point_in_time_parameter": "date",
            },
            "documentation_url": "https://massive.com/docs/rest/stocks/tickers/all-tickers",
        },
        "captured_at": stamp,
        "requested_dates": dates,
        "snapshots": source_rows,
        "security_master": {
            "path": _repo_path(output),
            "records": len(loaded),
            "instruments": len({item["instrument_id"] for item in loaded}),
            "fallback_identity_records": sum(
                item.get("source", {}).get("identity_source") == "fallback"
                for item in loaded
            ),
            "sha256": security_master_sha256(output),
        },
    }
    _write_json(source_manifest, manifest)
    return manifest


def write_security_master_snapshot(
    security_path: Path = DEFAULT_SECURITY_MASTER,
    *,
    output_root: Path = PROJECT_ROOT / "learning" / "security_masters",
) -> tuple[Path, str]:
    records = load_security_master(security_path)
    if not records:
        raise ScannerReplayError("cannot snapshot an empty security master")
    fingerprint = security_master_sha256(security_path)
    output = output_root / f"security-master-{fingerprint}.jsonl.gz"
    rendered = "".join(
        json.dumps(item, sort_keys=True, separators=(",", ":")) + "\n"
        for item in records
    ).encode()
    if output.exists():
        if security_master_sha256(output) != fingerprint:
            raise ScannerReplayError("versioned security-master snapshot is corrupted")
        return output, fingerprint
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = output.with_suffix(output.suffix + ".tmp")
    with temporary.open("wb") as raw:
        with gzip.GzipFile(fileobj=raw, mode="wb", mtime=0) as compressed:
            compressed.write(rendered)
    temporary.replace(output)
    return output, fingerprint


@dataclass(frozen=True)
class MassiveFlatFileConfig:
    access_key_id: str
    secret_access_key: str
    endpoint: str = MASSIVE_FLAT_ENDPOINT
    bucket: str = MASSIVE_FLAT_BUCKET
    region: str = MASSIVE_FLAT_REGION
    timeout_seconds: float = 120.0

    @classmethod
    def from_env(cls, path: Path) -> "MassiveFlatFileConfig":
        values: dict[str, Any] = {}
        if path.exists():
            values.update(dotenv_values(path, interpolate=False))
        names = (
            "MASSIVE_S3_ACCESS_KEY_ID",
            "MASSIVE_S3_SECRET_ACCESS_KEY",
            "MASSIVE_S3_ENDPOINT",
            "MASSIVE_S3_BUCKET",
            "MASSIVE_S3_REGION",
            "MASSIVE_S3_TIMEOUT_SECONDS",
        )
        for name in names:
            if name in os.environ:
                values[name] = os.environ[name]
        access = str(values.get("MASSIVE_S3_ACCESS_KEY_ID") or "").strip()
        secret = str(values.get("MASSIVE_S3_SECRET_ACCESS_KEY") or "").strip()
        if not access or not secret:
            raise ScannerReplayError(
                "Massive flat-file credentials are not configured; add the existing Dashboard-issued MASSIVE_S3_ACCESS_KEY_ID and MASSIVE_S3_SECRET_ACCESS_KEY to .env",
                category="configuration",
            )
        endpoint = str(
            values.get("MASSIVE_S3_ENDPOINT") or MASSIVE_FLAT_ENDPOINT
        ).rstrip("/")
        parsed = urlparse(endpoint)
        if parsed.scheme != "https" or not parsed.netloc:
            raise ScannerReplayError("MASSIVE_S3_ENDPOINT must be an HTTPS origin")
        try:
            timeout = float(values.get("MASSIVE_S3_TIMEOUT_SECONDS") or 120.0)
        except (TypeError, ValueError) as exc:
            raise ScannerReplayError(
                "MASSIVE_S3_TIMEOUT_SECONDS must be numeric"
            ) from exc
        return cls(
            access,
            secret,
            endpoint,
            str(values.get("MASSIVE_S3_BUCKET") or MASSIVE_FLAT_BUCKET),
            str(values.get("MASSIVE_S3_REGION") or MASSIVE_FLAT_REGION),
            timeout,
        )

    def public_dict(self) -> dict[str, Any]:
        return {
            "endpoint": self.endpoint,
            "bucket": self.bucket,
            "region": self.region,
            "timeout_seconds": self.timeout_seconds,
            "credentials_configured": bool(
                self.access_key_id and self.secret_access_key
            ),
        }


def _signing_key(secret: str, stamp: str, region: str, service: str) -> bytes:
    def sign(key: bytes, message: str) -> bytes:
        return hmac.new(key, message.encode(), hashlib.sha256).digest()

    dated = sign(("AWS4" + secret).encode(), stamp)
    regional = sign(dated, region)
    serviced = sign(regional, service)
    return sign(serviced, "aws4_request")


def _signed_s3_headers(
    config: MassiveFlatFileConfig, key: str, now: datetime
) -> tuple[str, dict[str, str]]:
    observed = now.astimezone(UTC)
    amz_date = observed.strftime("%Y%m%dT%H%M%SZ")
    date_stamp = observed.strftime("%Y%m%d")
    parsed = urlparse(config.endpoint)
    encoded_key = "/".join(quote(part, safe="") for part in key.split("/"))
    canonical_uri = f"/{quote(config.bucket, safe='')}/{encoded_key}"
    payload_hash = hashlib.sha256(b"").hexdigest()
    canonical_headers = (
        f"host:{parsed.netloc}\n"
        f"x-amz-content-sha256:{payload_hash}\n"
        f"x-amz-date:{amz_date}\n"
    )
    signed_headers = "host;x-amz-content-sha256;x-amz-date"
    canonical_request = "\n".join(
        ("GET", canonical_uri, "", canonical_headers, signed_headers, payload_hash)
    )
    scope = f"{date_stamp}/{config.region}/s3/aws4_request"
    string_to_sign = "\n".join(
        (
            "AWS4-HMAC-SHA256",
            amz_date,
            scope,
            hashlib.sha256(canonical_request.encode()).hexdigest(),
        )
    )
    signature = hmac.new(
        _signing_key(config.secret_access_key, date_stamp, config.region, "s3"),
        string_to_sign.encode(),
        hashlib.sha256,
    ).hexdigest()
    authorization = (
        f"AWS4-HMAC-SHA256 Credential={config.access_key_id}/{scope}, "
        f"SignedHeaders={signed_headers}, Signature={signature}"
    )
    return f"{config.endpoint}{canonical_uri}", {
        "Authorization": authorization,
        "x-amz-content-sha256": payload_hash,
        "x-amz-date": amz_date,
    }


class MassiveFlatFileClient:
    def __init__(
        self, config: MassiveFlatFileConfig, *, session: requests.Session | None = None
    ):
        self.config = config
        self.session = session or requests.Session()
        self._owns_session = session is None

    def close(self) -> None:
        if self._owns_session:
            self.session.close()

    def __enter__(self) -> "MassiveFlatFileClient":
        return self

    def __exit__(self, exc_type, exc, traceback_obj) -> None:
        self.close()

    @staticmethod
    def key_for(day: str) -> str:
        parsed = date.fromisoformat(day)
        return (
            f"{MASSIVE_MINUTE_PREFIX}/{parsed.year:04d}/{parsed.month:02d}/{day}.csv.gz"
        )

    def download(self, day: str, output: Path) -> dict[str, Any]:
        key = self.key_for(day)
        url, headers = _signed_s3_headers(self.config, key, datetime.now(UTC))
        try:
            response = self.session.get(
                url,
                headers=headers,
                stream=True,
                timeout=self.config.timeout_seconds,
            )
        except (requests.Timeout, requests.ConnectionError) as exc:
            raise ScannerReplayError(
                "Massive flat-file transport failure", category="retryable_transport"
            ) from exc
        if response.status_code in (429,) or response.status_code >= 500:
            raise ScannerReplayError(
                f"Massive flat-file HTTP {response.status_code}",
                category="retryable_provider",
            )
        if response.status_code >= 400:
            category = (
                "permission" if response.status_code in (401, 403) else "fidelity"
            )
            raise ScannerReplayError(
                f"Massive flat-file HTTP {response.status_code}", category=category
            )
        output.parent.mkdir(parents=True, exist_ok=True)
        temporary = output.with_suffix(output.suffix + ".partial")
        digest = hashlib.sha256()
        size = 0
        with temporary.open("wb") as target:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if not chunk:
                    continue
                target.write(chunk)
                digest.update(chunk)
                size += len(chunk)
        try:
            with gzip.open(temporary, "rt", encoding="utf-8", newline="") as source:
                header = next(csv.reader(source))
        except (OSError, StopIteration) as exc:
            temporary.unlink(missing_ok=True)
            raise ScannerReplayError(
                "Massive minute file is not valid gzip CSV"
            ) from exc
        if not MINUTE_COLUMNS.issubset(set(header)):
            temporary.unlink(missing_ok=True)
            raise ScannerReplayError("Massive minute file is missing required columns")
        temporary.replace(output)
        return {"date": day, "bytes": size, "sha256": digest.hexdigest(), "key": key}


def collect_minute_files(
    sessions: Sequence[str], *, config: MassiveFlatFileConfig, output_root: Path
) -> dict[str, Any]:
    output_root.mkdir(parents=True, exist_ok=True)
    artifacts: list[dict[str, Any]] = []
    with MassiveFlatFileClient(config) as client:
        for index, day in enumerate(sessions, 1):
            output = output_root / f"{day}.csv.gz"
            if output.exists():
                artifact = {
                    "date": day,
                    "bytes": output.stat().st_size,
                    "sha256": _sha256_file(output),
                    "key": client.key_for(day),
                    "disposition": "cached",
                }
            else:
                artifact = {**client.download(day, output), "disposition": "collected"}
            artifacts.append(artifact)
            print(
                f"minute {index}/{len(sessions)} {day}: {artifact['bytes']} bytes ({artifact['disposition']})",
                flush=True,
            )
    return {
        "valid": True,
        "provider": "Massive SIP minute flat files",
        "artifacts": artifacts,
    }


@dataclass
class DailyBar:
    open: float
    high: float
    low: float
    close: float
    volume: int
    opening_open: float | None = None
    opening_high: float | None = None
    opening_low: float | None = None
    opening_close: float | None = None
    opening_volume: int = 0
    opening_minutes: int = 0


def load_split_actions(path: Path) -> dict[str, list[dict[str, Any]]]:
    try:
        with gzip.open(path, "rt", encoding="utf-8") as source:
            raw = json.load(source)
    except (OSError, json.JSONDecodeError) as exc:
        raise ScannerReplayError(f"cannot read split history: {path}") from exc
    if not isinstance(raw, list):
        raise ScannerReplayError("split history must be an array")
    result: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in raw:
        if not isinstance(row, Mapping):
            raise ScannerReplayError("split history has a malformed row")
        try:
            ticker = str(row["ticker"]).strip().upper()
            execution = date.fromisoformat(str(row["execution_date"]))
            split_from = float(row["split_from"])
            split_to = float(row["split_to"])
        except (KeyError, TypeError, ValueError) as exc:
            raise ScannerReplayError("split history has invalid fields") from exc
        if not ticker or split_from <= 0 or split_to <= 0:
            raise ScannerReplayError("split history has an invalid ratio")
        result[ticker].append(
            {
                "execution_date": execution,
                "split_from": split_from,
                "split_to": split_to,
                "id": row.get("id"),
            }
        )
    for rows in result.values():
        rows.sort(key=lambda item: item["execution_date"])
    return dict(result)


def split_adjustment_factor(
    symbol: str,
    observed_day: str,
    target_day: str,
    split_actions: Mapping[str, Sequence[Mapping[str, Any]]],
) -> float:
    observed = date.fromisoformat(observed_day)
    target = date.fromisoformat(target_day)
    factor = 1.0
    for event in split_actions.get(symbol, []):
        execution = event["execution_date"]
        if observed < execution <= target:
            factor *= float(event["split_from"]) / float(event["split_to"])
    return factor


def _parse_minute_file(path: Path) -> dict[str, DailyBar]:
    bars: dict[str, DailyBar] = {}
    with gzip.open(path, "rt", encoding="utf-8", newline="") as source:
        reader = csv.DictReader(source)
        if not MINUTE_COLUMNS.issubset(set(reader.fieldnames or [])):
            raise ScannerReplayError(f"{path} is missing minute aggregate columns")
        for row in reader:
            try:
                stamp = datetime.fromtimestamp(
                    int(row["window_start"]) / 1e9, UTC
                ).astimezone(EASTERN)
                if not wall_time(9, 30) <= stamp.time() < wall_time(16, 0):
                    continue
                symbol = row["ticker"].strip().upper()
                opened = float(row["open"])
                high = float(row["high"])
                low = float(row["low"])
                closed = float(row["close"])
                volume = int(float(row["volume"]))
            except (KeyError, TypeError, ValueError) as exc:
                raise ScannerReplayError(f"{path} has a malformed minute row") from exc
            current = bars.get(symbol)
            if current is None:
                current = DailyBar(opened, high, low, closed, volume)
                bars[symbol] = current
            else:
                current.high = max(current.high, high)
                current.low = min(current.low, low)
                current.close = closed
                current.volume += volume
            if wall_time(9, 30) <= stamp.time() < wall_time(9, 35):
                if current.opening_open is None:
                    current.opening_open = opened
                    current.opening_high = high
                    current.opening_low = low
                else:
                    current.opening_high = max(float(current.opening_high), high)
                    current.opening_low = min(float(current.opening_low), low)
                current.opening_close = closed
                current.opening_volume += volume
                current.opening_minutes += 1
    return bars


def _master_by_date(
    records: Sequence[Mapping[str, Any]], day: str
) -> dict[str, Mapping[str, Any]]:
    target = date.fromisoformat(day)
    result: dict[str, Mapping[str, Any]] = {}
    for record in records:
        if (
            not security_record_covers(record, target)
            or record["security_type"] != "COMMON"
        ):
            continue
        symbol = str(record["symbol"])
        if symbol in result:
            raise ScannerReplayError(f"security master has duplicate {symbol} on {day}")
        result[symbol] = record
    return result


def _mean(values: Sequence[float]) -> float:
    return sum(values) / len(values)


def build_scanner_replay(
    *,
    selected_dates: Sequence[str],
    calendar: Sequence[str],
    rules: Mapping[str, Any],
    security_path: Path,
    minute_root: Path,
    split_path: Path,
    detailed_output: Path,
    summary_output: Path,
    dataset_id: str = "dataset-production-scanner-replay-2026-07-18-v1",
    summary_extension: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    targets = _iso_dates(sorted(selected_dates), "selected_dates")
    ordered_calendar = _iso_dates(list(calendar), "calendar")
    required = required_sessions(targets, ordered_calendar)
    missing = [day for day in required if not (minute_root / f"{day}.csv.gz").exists()]
    if missing:
        raise ScannerReplayError(
            f"missing {len(missing)} required market-wide minute files; first={missing[0]}",
            category="incomplete_collection",
        )
    records = load_security_master(security_path)
    if not records:
        raise ScannerReplayError("security master is empty")
    split_actions = load_split_actions(split_path)
    allowed_exchanges = set(rules.get("allowed_primary_exchanges") or [])
    if not allowed_exchanges:
        raise ScannerReplayError("scanner rules need allowed_primary_exchanges")
    thresholds = rules.get("thresholds")
    if not isinstance(thresholds, Mapping):
        raise ScannerReplayError("scanner rules need thresholds")
    min_price = float(thresholds["minimum_open_price"])
    min_adv = float(thresholds["minimum_average_daily_volume_14"])
    min_atr = float(thresholds["minimum_daily_atr_14"])
    min_rvol = float(thresholds["minimum_opening_relative_volume"])
    shortlist_size = int(rules.get("shortlist_size") or 20)

    bars_by_date: dict[str, dict[str, DailyBar]] = {}
    for index, day in enumerate(required, 1):
        bars_by_date[day] = _parse_minute_file(minute_root / f"{day}.csv.gz")
        print(
            f"parsed {index}/{len(required)} {day}: {len(bars_by_date[day])} symbols",
            flush=True,
        )

    positions = {day: index for index, day in enumerate(ordered_calendar)}
    detail: dict[str, Any] = {
        "schema_version": 1,
        "dataset_id": dataset_id,
        "selection_time_et": "09:35:00",
        "information_cutoff": "TARGET_SESSION_09:35_ET",
        "scanner_rules_sha256": _sha256_json(rules),
        "security_master_sha256": security_master_sha256(security_path),
        "split_actions_sha256": _sha256_file(split_path),
        "dates": {},
    }
    public_dates: list[dict[str, Any]] = []
    for target in targets:
        index = positions[target]
        lookback = ordered_calendar[index - 15 : index]
        master = _master_by_date(records, target)
        evaluations: list[dict[str, Any]] = []
        reasons: Counter[str] = Counter()
        for symbol, identity in sorted(master.items()):
            reason: str | None = None
            target_bar = bars_by_date[target].get(symbol)
            prior = [(day, bars_by_date[day].get(symbol)) for day in lookback]
            if str(identity["primary_exchange"]) not in allowed_exchanges:
                reason = "exchange_not_allowed"
            elif target_bar is None or target_bar.opening_minutes != 5:
                reason = "incomplete_target_opening_bar"
            elif any(item is None for _, item in prior):
                reason = "incomplete_prior_session_history"
            else:
                complete_prior = [
                    (day, item) for day, item in prior if item is not None
                ]
                if any(item.opening_minutes != 5 for _, item in complete_prior[-14:]):
                    reason = "incomplete_prior_opening_history"
            row: dict[str, Any] = {
                "symbol": symbol,
                "instrument_id": identity["instrument_id"],
                "primary_exchange": identity["primary_exchange"],
            }
            if reason is None and target_bar is not None:
                complete_prior = [
                    (day, item) for day, item in prior if item is not None
                ]
                factors = {
                    day: split_adjustment_factor(symbol, day, target, split_actions)
                    for day, _ in complete_prior
                }
                previous_day, previous_bar = complete_prior[-1]
                previous_close = previous_bar.close * factors[previous_day]
                opening_volumes = [
                    item.opening_volume / factors[day]
                    for day, item in complete_prior[-14:]
                ]
                adv14 = _mean(
                    [
                        float(item.volume) / factors[day]
                        for day, item in complete_prior[-14:]
                    ]
                )
                true_ranges: list[float] = []
                for offset, (day, item) in enumerate(complete_prior[-14:], 1):
                    earlier_day, earlier_bar = complete_prior[-15 + offset - 1]
                    factor = factors[day]
                    earlier_close = earlier_bar.close * factors[earlier_day]
                    true_ranges.append(
                        max(
                            (item.high - item.low) * factor,
                            abs(item.high * factor - earlier_close),
                            abs(item.low * factor - earlier_close),
                        )
                    )
                opening_mean = _mean([float(item) for item in opening_volumes])
                opening_rvol = (
                    target_bar.opening_volume / opening_mean if opening_mean else 0.0
                )
                atr14 = _mean(true_ranges)
                opening_return = (
                    float(target_bar.opening_close) / previous_close - 1.0
                    if previous_close
                    else 0.0
                )
                row.update(
                    {
                        "open_price": target_bar.opening_open,
                        "opening_high": target_bar.opening_high,
                        "opening_low": target_bar.opening_low,
                        "opening_close": target_bar.opening_close,
                        "opening_volume": target_bar.opening_volume,
                        "prior_opening_volume_mean_14": opening_mean,
                        "opening_relative_volume": opening_rvol,
                        "average_daily_volume_14": adv14,
                        "daily_atr_14": atr14,
                        "split_adjustment_factor_oldest_session": factors[
                            complete_prior[0][0]
                        ],
                        "prior_close": previous_close,
                        "opening_return": opening_return,
                        "bullish_opening_candle": target_bar.opening_close
                        > target_bar.opening_open,
                    }
                )
                if float(target_bar.opening_open) < min_price:
                    reason = "below_minimum_open_price"
                elif adv14 < min_adv:
                    reason = "below_minimum_average_daily_volume"
                elif atr14 < min_atr:
                    reason = "below_minimum_atr"
                elif opening_rvol < min_rvol:
                    reason = "below_minimum_opening_rvol"
                elif target_bar.opening_close <= target_bar.opening_open:
                    reason = "non_bullish_opening_candle"
                elif opening_return <= 0:
                    reason = "non_positive_opening_return"
                else:
                    reason = "eligible"
            assert reason is not None
            row["disposition"] = reason
            reasons[reason] += 1
            evaluations.append(row)
        eligible = [item for item in evaluations if item["disposition"] == "eligible"]
        eligible.sort(
            key=lambda item: (
                -float(item["opening_relative_volume"]),
                -float(item["opening_return"]),
                str(item["symbol"]),
            )
        )
        for rank, item in enumerate(eligible, 1):
            item["opening_rvol_rank"] = rank
        selected = eligible[:shortlist_size]
        detail["dates"][target] = {
            "master_common_stock_count": len(master),
            "evaluations": evaluations,
            "selected_symbols": [item["symbol"] for item in selected],
        }
        public_dates.append(
            {
                "date": target,
                "master_common_stock_count": len(master),
                "evaluated_count": len(evaluations),
                "eligible_count": len(eligible),
                "rejection_counts": dict(sorted(reasons.items())),
                "shortlist_count": len(selected),
                "shortlist_sha256": _sha256_json(
                    [
                        {
                            "symbol": item["symbol"],
                            "instrument_id": item["instrument_id"],
                            "opening_relative_volume": item["opening_relative_volume"],
                            "opening_return": item["opening_return"],
                            "rank": item["opening_rvol_rank"],
                        }
                        for item in selected
                    ]
                ),
            }
        )

    _write_json(detailed_output, detail)
    summary = {
        "schema_version": 1,
        "dataset_id": dataset_id,
        "status": "READY",
        "claim_boundary": (
            "Faithful dynamic 09:35 scanner-universe reconstruction only; catalyst, "
            "NBBO spread, depth, tradability, breakout, and outcomes are not trade eligible."
        ),
        "requested_dates": targets,
        "completed_dates": len(public_dates),
        "selection_time_et": "09:35:00",
        "information_cutoff": "TARGET_SESSION_09:35_ET",
        "complete_universe": True,
        "selection_is_dynamic": True,
        "scanner_rules_sha256": _sha256_json(rules),
        "security_master_sha256": security_master_sha256(security_path),
        "split_actions_sha256": _sha256_file(split_path),
        "detailed_artifact": {
            "local_path": _repo_path(detailed_output),
            "sha256": _sha256_file(detailed_output),
            "public": False,
        },
        "dates": public_dates,
    }
    if summary_extension:
        overlap = set(summary).intersection(summary_extension)
        if overlap:
            raise ScannerReplayError(
                f"scanner summary extension cannot replace {sorted(overlap)}"
            )
        summary.update(summary_extension)
    _write_json(summary_output, summary)
    return summary


def validate_rules(path: Path) -> dict[str, Any]:
    rules = _read_object(path)
    required = {
        "schema_version",
        "selection_time_et",
        "information_cutoff",
        "security_type",
        "allowed_primary_exchanges",
        "thresholds",
        "ranking",
        "shortlist_size",
    }
    if not required.issubset(rules):
        raise ScannerReplayError(
            f"scanner rules missing {sorted(required - set(rules))}"
        )
    if rules["schema_version"] != 1:
        raise ScannerReplayError("scanner rules schema_version must be 1")
    if rules["selection_time_et"] != "09:35:00":
        raise ScannerReplayError("scanner selection_time_et must be 09:35:00")
    if rules["information_cutoff"] != "TARGET_SESSION_09:35_ET":
        raise ScannerReplayError("scanner information cutoff is invalid")
    if rules["security_type"] != "COMMON":
        raise ScannerReplayError("the production scanner replay is common-stock only")
    return rules


def freeze_contract(
    *,
    selection_path: Path,
    calendar_path: Path,
    rules_path: Path,
    security_path: Path,
    output_root: Path,
    minute_root: Path = DEFAULT_RUN_ROOT / "minute_aggs",
) -> tuple[Path, dict[str, Any]]:
    selection = load_selection(selection_path, expected_count=20)
    calendar = load_calendar(calendar_path)
    requested_dates = sorted(selection["selected_dates"])
    required = required_sessions(requested_dates, calendar)
    existing_target_data = [
        day for day in required if (minute_root / f"{day}.csv.gz").exists()
    ]
    if existing_target_data:
        raise ScannerReplayError(
            "scanner contract must be frozen before any required target minute file exists"
        )
    rules = validate_rules(rules_path)
    snapshot_path, snapshot_hash = write_security_master_snapshot(security_path)
    records = load_security_master(security_path)
    for day in requested_dates:
        if not _master_by_date(records, day):
            raise ScannerReplayError(
                f"security master has no common-stock universe for {day}"
            )
    payload = {
        "schema_version": 1,
        "dataset_id": "dataset-production-scanner-replay-2026-07-18-v1",
        "registered_at": _timestamp_now(),
        "requested_dates": requested_dates,
        "dataset_payload": {
            "lane": "production_scanner_replay",
            "claim_scope": "PRODUCTION_POLICY_REPLAY",
            "status": "COLLECTING",
            "evidence_paths": [
                _repo_path(selection_path),
                _repo_path(rules_path),
                _repo_path(DEFAULT_SOURCE_MANIFEST),
            ],
            "inspected": False,
            "universe_contract": {
                "selection_time_et": "09:35:00",
                "information_cutoff": "TARGET_SESSION_09:35_ET",
                "selection_is_dynamic": True,
                "complete_universe": True,
                "scanner_rules_sha256": _sha256_json(rules),
                "security_master_sha256": snapshot_hash,
                "security_master_path": _repo_path(snapshot_path),
                "security_master_attestation_path": _repo_path(DEFAULT_SOURCE_MANIFEST),
            },
        },
        "collection_contract": {
            "source": "Massive SIP market-wide minute aggregates",
            "unadjusted": True,
            "corporate_action_source": "Massive /stocks/v1/splits",
            "split_adjustment_basis": "target-date share basis using only execution dates at or before each target",
            "scanner_implementation_sha256": _sha256_file(Path(__file__)),
            "required_session_dates": required,
            "required_session_count": len(required),
            "target_data_collection_must_begin_after_freeze": True,
            "pre_freeze_target_artifact_count": 0,
            "substitutions_allowed": False,
        },
        "selection": {
            "seed": selection["seed"],
            "selection_sha256": _sha256_json(
                {
                    "seed": selection["seed"],
                    "selected_dates": selection["selected_dates"],
                }
            ),
        },
    }
    content_hash = _sha256_json(payload)
    frozen = {**payload, "manifest_sha256": content_hash}
    output = output_root / f"{payload['dataset_id']}-{content_hash}.json"
    if output.exists() and _read_object(output) != frozen:
        raise ScannerReplayError("hash-addressed scanner contract has changed")
    if not output.exists():
        _write_json(output, frozen)
    return output, frozen


def load_frozen_scanner_contract(path: Path) -> dict[str, Any]:
    manifest = _read_object(path)
    recorded = manifest.get("manifest_sha256")
    content = dict(manifest)
    content.pop("manifest_sha256", None)
    expected = _sha256_json(content)
    expected_name = f"{manifest.get('dataset_id')}-{expected}.json"
    if recorded != expected or path.name != expected_name:
        raise ScannerReplayError("frozen scanner contract was mutated or renamed")
    if manifest.get("schema_version") != 1:
        raise ScannerReplayError("frozen scanner contract schema is invalid")
    requested = _iso_dates(manifest.get("requested_dates"), "requested_dates")
    payload = manifest.get("dataset_payload")
    if not isinstance(payload, Mapping):
        raise ScannerReplayError("frozen scanner contract lacks dataset_payload")
    validate_dataset_payload(str(manifest.get("dataset_id")), payload)
    collection = manifest.get("collection_contract")
    if not isinstance(collection, Mapping):
        raise ScannerReplayError("frozen scanner contract lacks collection_contract")
    required = _iso_dates(
        collection.get("required_session_dates"), "required_session_dates"
    )
    if not set(requested).issubset(required):
        raise ScannerReplayError("frozen scanner contract omits target sessions")
    if collection.get("substitutions_allowed") is not False:
        raise ScannerReplayError("frozen scanner contract must forbid substitutions")
    return manifest


def verify_contract_inputs(
    manifest: Mapping[str, Any], *, rules_path: Path
) -> tuple[dict[str, Any], Path]:
    rules = validate_rules(rules_path)
    universe = manifest["dataset_payload"]["universe_contract"]
    if universe["scanner_rules_sha256"] != _sha256_json(rules):
        raise ScannerReplayError("scanner rules no longer match the frozen contract")
    security_path = PROJECT_ROOT / str(universe["security_master_path"])
    if universe["security_master_sha256"] != security_master_sha256(security_path):
        raise ScannerReplayError(
            "security-master snapshot no longer matches the contract"
        )
    recorded_implementation = manifest["collection_contract"].get(
        "scanner_implementation_sha256"
    )
    if recorded_implementation != _sha256_file(Path(__file__)):
        raise ScannerReplayError(
            "scanner implementation no longer matches the frozen contract"
        )
    return rules, security_path


def collection_status(
    *,
    selection_path: Path,
    calendar_path: Path,
    snapshots_root: Path,
    minute_root: Path,
    security_path: Path = DEFAULT_SECURITY_MASTER,
    split_path: Path | None = None,
) -> dict[str, Any]:
    selection = load_selection(selection_path)
    calendar = load_calendar(calendar_path)
    required = required_sessions(selection["selected_dates"], calendar)
    references = [
        day
        for day in selection["selected_dates"]
        if (snapshots_root / f"{day}.json.gz").exists()
    ]
    minutes = [day for day in required if (minute_root / f"{day}.csv.gz").exists()]
    resolved_splits = split_path or minute_root.parent / "splits.json.gz"
    return {
        "valid": True,
        "selected_dates": selection["selected_dates"],
        "reference_snapshots": {
            "ready": len(references),
            "required": len(selection["selected_dates"]),
        },
        "security_master": {
            "path": _repo_path(security_path),
            "ready": security_path.exists(),
        },
        "split_actions": {
            "path": _repo_path(resolved_splits),
            "ready": resolved_splits.exists(),
            "sha256": _sha256_file(resolved_splits)
            if resolved_splits.exists()
            else None,
        },
        "market_collection": {
            "must_follow_frozen_manifest": True,
            "recommended_non_s3_adapter": "scanner_replay_alpaca.py",
            "status_after_freeze": "scanner_replay_alpaca.py status <manifest>",
            "legacy_flat_minute_files_present": len(minutes),
            "required_session_count": len(required),
            "legacy_flat_files_required": False,
        },
    }


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env", type=Path, default=PROJECT_ROOT / ".env")
    parser.add_argument("--run-root", type=Path, default=DEFAULT_RUN_ROOT)
    subparsers = parser.add_subparsers(dest="command", required=True)

    references = subparsers.add_parser("collect-reference")
    references.add_argument("selection", type=Path)

    master = subparsers.add_parser("build-master")
    master.add_argument("selection", type=Path)
    master.add_argument("--output", type=Path, default=DEFAULT_SECURITY_MASTER)
    master.add_argument("--source-manifest", type=Path, default=DEFAULT_SOURCE_MANIFEST)

    freeze = subparsers.add_parser("freeze")
    freeze.add_argument("selection", type=Path)
    freeze.add_argument("calendar", type=Path)
    freeze.add_argument("--rules", type=Path, default=DEFAULT_RULES)
    freeze.add_argument("--security-master", type=Path, default=DEFAULT_SECURITY_MASTER)
    freeze.add_argument(
        "--output-root",
        type=Path,
        default=PROJECT_ROOT / "historical_batches" / "scanner_replay" / "manifests",
    )

    minutes = subparsers.add_parser("collect-minutes")
    minutes.add_argument("manifest", type=Path)

    splits = subparsers.add_parser("collect-splits")
    splits.add_argument("manifest", type=Path)

    build = subparsers.add_parser("build")
    build.add_argument("manifest", type=Path)
    build.add_argument("calendar", type=Path)
    build.add_argument("--rules", type=Path, default=DEFAULT_RULES)
    build.add_argument("--splits", type=Path, default=None)
    build.add_argument(
        "--summary",
        type=Path,
        default=PROJECT_ROOT / "research_results" / "2026-07-18-scanner-replay.json",
    )

    status = subparsers.add_parser("status")
    status.add_argument("selection", type=Path)
    status.add_argument("calendar", type=Path)
    status.add_argument("--security-master", type=Path, default=DEFAULT_SECURITY_MASTER)
    status.add_argument("--splits", type=Path)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    snapshots = args.run_root / "reference"
    minutes = args.run_root / "minute_aggs"
    try:
        if args.command == "collect-reference":
            selection = load_selection(args.selection)
            result: Any = collect_reference_snapshots(
                selection["selected_dates"],
                config=MassiveReferenceConfig.from_env(args.env),
                output_root=snapshots,
            )
        elif args.command == "build-master":
            selection = load_selection(args.selection)
            result = build_security_master(
                selection["selected_dates"],
                snapshots_root=snapshots,
                output=args.output,
                source_manifest=args.source_manifest,
            )
        elif args.command == "freeze":
            path, manifest = freeze_contract(
                selection_path=args.selection,
                calendar_path=args.calendar,
                rules_path=args.rules,
                security_path=args.security_master,
                output_root=args.output_root,
            )
            result = {"path": str(path), "manifest_sha256": manifest["manifest_sha256"]}
        elif args.command == "collect-minutes":
            manifest = load_frozen_scanner_contract(args.manifest)
            verify_contract_inputs(manifest, rules_path=DEFAULT_RULES)
            result = collect_minute_files(
                manifest["collection_contract"]["required_session_dates"],
                config=MassiveFlatFileConfig.from_env(args.env),
                output_root=minutes,
            )
            _write_json(args.run_root / "minute-collection.json", result)
        elif args.command == "collect-splits":
            manifest = load_frozen_scanner_contract(args.manifest)
            verify_contract_inputs(manifest, rules_path=DEFAULT_RULES)
            needed = manifest["collection_contract"]["required_session_dates"]
            result = collect_split_actions(
                start=needed[0],
                end=max(manifest["requested_dates"]),
                config=MassiveReferenceConfig.from_env(args.env),
                output=args.run_root / "splits.json.gz",
            )
        elif args.command == "build":
            manifest = load_frozen_scanner_contract(args.manifest)
            rules, security_path = verify_contract_inputs(
                manifest, rules_path=args.rules
            )
            calendar = load_calendar(args.calendar)
            if (
                required_sessions(manifest["requested_dates"], calendar)
                != manifest["collection_contract"]["required_session_dates"]
            ):
                raise ScannerReplayError(
                    "session calendar no longer matches the frozen collection contract"
                )
            result = build_scanner_replay(
                selected_dates=manifest["requested_dates"],
                calendar=calendar,
                rules=rules,
                security_path=security_path,
                minute_root=minutes,
                split_path=args.splits or args.run_root / "splits.json.gz",
                detailed_output=args.run_root / "scanner-replay-detail.json",
                summary_output=args.summary,
            )
        else:
            result = collection_status(
                selection_path=args.selection,
                calendar_path=args.calendar,
                snapshots_root=snapshots,
                minute_root=minutes,
                security_path=args.security_master,
                split_path=args.splits,
            )
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    except (
        KeyError,
        OSError,
        ScannerReplayError,
        LearningDataError,
        ValueError,
    ) as exc:
        print(
            json.dumps(
                {
                    "error": str(exc),
                    "error_type": type(exc).__name__,
                    "category": getattr(exc, "category", "validation"),
                },
                indent=2,
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

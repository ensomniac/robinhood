"""Canonical, provider-aware local store for per-symbol daily market history.

The repository keeps code, manifests, and compact evidence.  Licensed or bulky
per-day observations live below ``LOCAL_HISTORICAL_DATA_ROOT`` as deterministic
gzip-compressed JSON documents:

    <root>/<symbol-lower>/<year>/<YYYY-MM-DD>.json.gz

Each document can retain multiple provider series and point-in-time contexts
without merging incompatible feeds or silently overwriting corrected data.
"""

from __future__ import annotations

import fcntl
import gzip
import hashlib
import json
import math
import os
import re
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import date, datetime, time, timezone
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping, Sequence
from zoneinfo import ZoneInfo

from dotenv import dotenv_values


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_ENV_PATH = PROJECT_ROOT / ".env"
EASTERN = ZoneInfo("America/New_York")
UTC = timezone.utc
STORE_SCHEMA_VERSION = 1
STORE_KIND = "us_equity_daily_history"
SYMBOL_PATTERN = re.compile(r"^[A-Z0-9][A-Z0-9.-]{0,31}$")
PROVENANCE_SAMPLE_LIMIT = 8


class HistoricalStoreError(RuntimeError):
    """Raised when local historical data cannot be trusted or stored safely."""


def canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")


def canonical_sha256(value: Any) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def normalize_symbol(value: str) -> str:
    symbol = str(value).strip().upper()
    if not SYMBOL_PATTERN.fullmatch(symbol) or ".." in symbol:
        raise HistoricalStoreError(f"invalid equity symbol: {value!r}")
    return symbol


def normalize_date(value: str | date) -> str:
    try:
        parsed = value if isinstance(value, date) else date.fromisoformat(str(value))
    except ValueError as exc:
        raise HistoricalStoreError(f"invalid ISO session date: {value!r}") from exc
    return parsed.isoformat()


def provider_id(value: str) -> str:
    normalized = re.sub(r"[^a-z0-9]+", "_", str(value).strip().lower()).strip("_")
    aliases = {
        "interactive_brokers_tws_api": "ibkr",
        "interactive_brokers": "ibkr",
        "ibkr_legacy_iabapp": "ibkr",
        "interactive_brokers_tws_api_pre_session_history": "ibkr",
        "massive_sip_rest_api": "massive",
        "massive": "massive",
        "alpaca_market_data_api": "alpaca",
        "alpaca": "alpaca",
        "replay_bundle": "replay_bundle",
    }
    return aliases.get(normalized, normalized or "unknown")


def _finite_number(value: Any, field: str) -> float:
    if isinstance(value, bool):
        raise HistoricalStoreError(f"{field} must be numeric")
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise HistoricalStoreError(f"{field} must be numeric") from exc
    if not math.isfinite(number):
        raise HistoricalStoreError(f"{field} must be finite")
    return number


def _timestamp_et(row: Mapping[str, Any], day: str | None = None) -> str:
    raw = row.get("time_et")
    if isinstance(raw, str) and raw:
        try:
            parsed = datetime.fromisoformat(raw)
        except ValueError as exc:
            raise HistoricalStoreError(f"invalid time_et: {raw}") from exc
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=EASTERN)
        return parsed.astimezone(EASTERN).isoformat()
    raw = row.get("t", row.get("epoch"))
    if isinstance(raw, str):
        try:
            parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError as exc:
            raise HistoricalStoreError(f"invalid timestamp: {raw}") from exc
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=EASTERN)
        return parsed.astimezone(EASTERN).isoformat()
    if isinstance(raw, (int, float)) and not isinstance(raw, bool):
        scale = 1.0
        absolute = abs(float(raw))
        if absolute > 1e17:
            scale = 1_000_000_000.0
        elif absolute > 1e14:
            scale = 1_000_000.0
        elif absolute > 1e11:
            scale = 1_000.0
        return datetime.fromtimestamp(float(raw) / scale, UTC).astimezone(EASTERN).isoformat()
    if day is not None and isinstance(row.get("date_et"), str):
        parsed_day = normalize_date(str(row["date_et"]))
        return datetime.combine(date.fromisoformat(parsed_day), time(0), tzinfo=EASTERN).isoformat()
    raise HistoricalStoreError("row is missing a usable timestamp")


def compact_bar(row: Mapping[str, Any], *, day: str | None = None) -> dict[str, Any]:
    """Normalize one provider bar while preserving non-canonical fields in ``x``."""

    aliases = {
        "open": "o",
        "high": "h",
        "low": "l",
        "close": "c",
        "volume": "v",
        "count": "n",
        "wap": "vw",
    }
    result: dict[str, Any] = {"t": _timestamp_et(row, day)}
    consumed = {"t", "epoch", "time_et", "date_et"}
    for source, target in aliases.items():
        value = row.get(source, row.get(target))
        if value is None:
            continue
        consumed.update((source, target))
        number = _finite_number(value, source)
        result[target] = int(number) if target in {"v", "n"} else number
    if row.get("interpolated") is not None:
        result["i"] = bool(row["interpolated"])
        consumed.add("interpolated")
    extra = {str(key): value for key, value in row.items() if key not in consumed}
    if extra:
        result["x"] = extra
    return result


def expand_bar(row: Mapping[str, Any]) -> dict[str, Any]:
    try:
        observed = datetime.fromisoformat(str(row["t"]))
    except (KeyError, ValueError) as exc:
        raise HistoricalStoreError("stored bar has an invalid timestamp") from exc
    if observed.tzinfo is None:
        observed = observed.replace(tzinfo=EASTERN)
    observed = observed.astimezone(EASTERN)
    result: dict[str, Any] = {
        "epoch": int(observed.timestamp()),
        "time_et": observed.isoformat(),
        "date_et": observed.date().isoformat(),
        "open": float(row["o"]),
        "high": float(row["h"]),
        "low": float(row["l"]),
        "close": float(row["c"]),
        "volume": int(row.get("v", 0)),
        "count": int(row.get("n", 0)),
        "wap": float(row.get("vw", 0)),
        "interpolated": bool(row.get("i", False)),
    }
    extra = row.get("x")
    if isinstance(extra, Mapping):
        for key, value in extra.items():
            result.setdefault(str(key), value)
    return result


def compact_quote(row: Mapping[str, Any], *, day: str | None = None) -> dict[str, Any]:
    result: dict[str, Any] = {"t": _timestamp_et(row, day)}
    aliases = {
        "bid": "bp",
        "ask": "ap",
        "bid_size": "bs",
        "ask_size": "as",
        "bid_price": "bp",
        "ask_price": "ap",
    }
    consumed = {"t", "epoch", "time_et", "date_et"}
    for source, target in aliases.items():
        if source not in row or target in result:
            continue
        consumed.add(source)
        number = _finite_number(row[source], source)
        result[target] = int(number) if target in {"bs", "as"} else number
    passthrough = {
        "sip_timestamp_ns": "sip_ns",
        "participant_timestamp_ns": "participant_ns",
        "bid_exchange": "bx",
        "ask_exchange": "ax",
        "conditions": "cnd",
        "tape": "tape",
    }
    for source, target in passthrough.items():
        if source in row:
            result[target] = row[source]
            consumed.add(source)
    extra = {str(key): value for key, value in row.items() if key not in consumed}
    if extra:
        result["x"] = extra
    return result


def expand_quote(row: Mapping[str, Any]) -> dict[str, Any]:
    try:
        observed = datetime.fromisoformat(str(row["t"]))
    except (KeyError, ValueError) as exc:
        raise HistoricalStoreError("stored quote has an invalid timestamp") from exc
    if observed.tzinfo is None:
        observed = observed.replace(tzinfo=EASTERN)
    observed = observed.astimezone(EASTERN)
    result: dict[str, Any] = {
        "epoch": int(observed.timestamp()),
        "time_et": observed.isoformat(),
        "bid": float(row["bp"]),
        "ask": float(row["ap"]),
        "bid_size": int(row.get("bs", 0)),
        "ask_size": int(row.get("as", 0)),
    }
    reverse = {
        "sip_ns": "sip_timestamp_ns",
        "participant_ns": "participant_timestamp_ns",
        "bx": "bid_exchange",
        "ax": "ask_exchange",
        "cnd": "conditions",
        "tape": "tape",
    }
    for source, target in reverse.items():
        if source in row:
            result[target] = row[source]
    extra = row.get("x")
    if isinstance(extra, Mapping):
        for key, value in extra.items():
            result.setdefault(str(key), value)
    return result


@dataclass(frozen=True, slots=True)
class HistoricalStoreConfig:
    root: Path

    @classmethod
    def from_env(
        cls,
        path: Path = DEFAULT_ENV_PATH,
        *,
        require_outside_repo: bool = True,
    ) -> "HistoricalStoreConfig":
        values: dict[str, Any] = {}
        if path.exists():
            values.update(dotenv_values(path, interpolate=False))
        if "LOCAL_HISTORICAL_DATA_ROOT" in os.environ:
            values["LOCAL_HISTORICAL_DATA_ROOT"] = os.environ[
                "LOCAL_HISTORICAL_DATA_ROOT"
            ]
        raw = str(values.get("LOCAL_HISTORICAL_DATA_ROOT") or "").strip()
        if not raw:
            raise HistoricalStoreError(
                "LOCAL_HISTORICAL_DATA_ROOT is required for historical market data"
            )
        root = Path(raw).expanduser()
        if not root.is_absolute():
            raise HistoricalStoreError("LOCAL_HISTORICAL_DATA_ROOT must be absolute")
        resolved_root = root.resolve()
        resolved_project = PROJECT_ROOT.resolve()
        if require_outside_repo and (
            resolved_root == resolved_project or resolved_project in resolved_root.parents
        ):
            raise HistoricalStoreError(
                "LOCAL_HISTORICAL_DATA_ROOT must be outside the public repository"
            )
        return cls(root=resolved_root)

    def public_dict(self) -> dict[str, Any]:
        return {"root": str(self.root), "outside_repository": True}


def _new_document(symbol: str, day: str) -> dict[str, Any]:
    return {
        "schema_version": STORE_SCHEMA_VERSION,
        "kind": STORE_KIND,
        "symbol": symbol,
        "date": day,
        "timezone": "America/New_York",
        "datasets": [],
        "contexts": [],
    }


def _gzip_json_bytes(value: Any) -> bytes:
    import io

    raw = json.dumps(
        value,
        indent=2,
        sort_keys=True,
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8") + b"\n"
    buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=buffer, mode="wb", compresslevel=6, mtime=0) as stream:
        stream.write(raw)
    return buffer.getvalue()


def _load_gzip_json(path: Path) -> Any:
    try:
        with gzip.open(path, "rt", encoding="utf-8") as stream:
            return json.load(stream)
    except (OSError, json.JSONDecodeError) as exc:
        raise HistoricalStoreError(f"cannot read canonical history {path}: {exc}") from exc


def _provenance_sample(value: Mapping[str, Any] | None) -> dict[str, Any] | None:
    if value is None:
        return None
    cleaned = {
        str(key): item
        for key, item in value.items()
        if item is not None and str(key) not in {"api_key", "secret", "password"}
    }
    return cleaned or None


def _merge_provenance(target: dict[str, Any], sample: Mapping[str, Any] | None) -> bool:
    normalized = _provenance_sample(sample)
    if normalized is None:
        return False
    state = target.setdefault(
        "provenance", {"source_count": 0, "source_fingerprints": [], "samples": []}
    )
    samples = state.setdefault("samples", [])
    fingerprints = state.setdefault("source_fingerprints", [])
    fingerprint = canonical_sha256(normalized)
    if fingerprint in fingerprints:
        return False
    fingerprints.append(fingerprint)
    fingerprints.sort()
    state["source_count"] = len(fingerprints)
    if len(samples) < PROVENANCE_SAMPLE_LIMIT:
        samples.append(normalized)
    return True


def _merge_provenance_state(
    target: dict[str, Any], incoming: Mapping[str, Any] | None
) -> bool:
    if not isinstance(incoming, Mapping):
        return False
    state = target.setdefault(
        "provenance", {"source_count": 0, "source_fingerprints": [], "samples": []}
    )
    changed = False
    fingerprints = state.setdefault("source_fingerprints", [])
    for fingerprint in incoming.get("source_fingerprints", []):
        value = str(fingerprint)
        if value and value not in fingerprints:
            fingerprints.append(value)
            changed = True
    fingerprints.sort()
    samples = state.setdefault("samples", [])
    sample_hashes = {
        canonical_sha256(sample) for sample in samples if isinstance(sample, Mapping)
    }
    for sample in incoming.get("samples", []):
        if not isinstance(sample, Mapping):
            continue
        fingerprint = canonical_sha256(sample)
        if fingerprint not in sample_hashes and len(samples) < PROVENANCE_SAMPLE_LIMIT:
            samples.append(dict(sample))
            sample_hashes.add(fingerprint)
            changed = True
    source_count = len(fingerprints)
    if state.get("source_count") != source_count:
        state["source_count"] = source_count
        changed = True
    return changed


def build_dataset(
    *,
    kind: str,
    provider: str,
    rows: Sequence[Mapping[str, Any]],
    channel: str,
    timeframe: str | None = None,
    feed: str = "unknown",
    adjustment: str = "unknown",
    session: str = "regular",
    scope: str = "observed",
    quality: Mapping[str, Any] | None = None,
    limitations: Sequence[str] = (),
    provenance: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    normalized_kind = str(kind).strip().lower()
    if normalized_kind not in {"bars", "quotes", "snapshots", "derived"}:
        raise HistoricalStoreError(f"unsupported dataset kind: {kind}")
    normalized_rows = [dict(row) for row in rows]
    normalized_quality = {
        "row_count": len(normalized_rows),
        **(dict(quality) if quality is not None else {}),
    }
    normalized_limitations = sorted(
        {str(item) for item in limitations if str(item)}
    )
    identity = {
        "kind": normalized_kind,
        "provider": provider_id(provider),
        "channel": str(channel).strip().lower(),
        "timeframe": str(timeframe).strip().lower() if timeframe else None,
        "feed": str(feed).strip().lower(),
        "adjustment": str(adjustment).strip().lower(),
        "session": str(session).strip().lower(),
        "scope": str(scope).strip().lower(),
        "quality": normalized_quality,
        "limitations": normalized_limitations,
        "rows": normalized_rows,
    }
    content_hash = canonical_sha256(identity)
    dataset: dict[str, Any] = {
        "id": (
            f"{normalized_kind}:{identity['provider']}:{identity['channel']}:"
            f"{identity['timeframe'] or 'na'}:{content_hash[:16]}"
        ),
        **identity,
        "content_sha256": content_hash,
    }
    _merge_provenance(dataset, provenance)
    return dataset


def build_context(
    *,
    kind: str,
    provider: str,
    payload: Mapping[str, Any],
    observed_at: str | None = None,
    provenance: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    identity = {
        "kind": str(kind).strip().lower(),
        "provider": provider_id(provider),
        "observed_at": observed_at,
        "payload": dict(payload),
    }
    content_hash = canonical_sha256(identity)
    context: dict[str, Any] = {
        "id": f"context:{identity['provider']}:{identity['kind']}:{content_hash[:16]}",
        **identity,
        "content_sha256": content_hash,
    }
    _merge_provenance(context, provenance)
    return context


class HistoricalDayStore:
    """Atomic, append-by-content storage for canonical day documents."""

    def __init__(self, root: Path):
        self.root = Path(root).expanduser().resolve()
        self.root.mkdir(parents=True, exist_ok=True)
        self._ensure_metadata()

    @classmethod
    def from_env(cls, path: Path = DEFAULT_ENV_PATH) -> "HistoricalDayStore":
        return cls(HistoricalStoreConfig.from_env(path).root)

    def _ensure_metadata(self) -> None:
        path = self.root / "_store.json"
        expected = {
            "schema_version": STORE_SCHEMA_VERSION,
            "kind": "robinhood_codex_historical_store",
            "layout": "<symbol-lower>/<year>/<YYYY-MM-DD>.json.gz",
            "day_schema": STORE_KIND,
            "compression": "gzip",
        }
        if path.exists():
            try:
                current = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError) as exc:
                raise HistoricalStoreError(f"invalid historical store metadata: {exc}") from exc
            if current != expected:
                raise HistoricalStoreError("historical store metadata is incompatible")
            return
        temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
        temporary.write_text(json.dumps(expected, indent=2, sort_keys=True) + "\n")
        os.replace(temporary, path)

    def path_for(self, symbol: str, day: str | date) -> Path:
        normalized_symbol = normalize_symbol(symbol)
        normalized_day = normalize_date(day)
        return (
            self.root
            / normalized_symbol.lower()
            / normalized_day[:4]
            / f"{normalized_day}.json.gz"
        )

    def _lock_path(self, symbol: str, day: str | date) -> Path:
        normalized_symbol = normalize_symbol(symbol)
        normalized_day = normalize_date(day)
        return (
            self.root
            / "_locks"
            / normalized_symbol.lower()
            / normalized_day[:4]
            / f"{normalized_day}.lock"
        )

    @contextmanager
    def _locked(self, symbol: str, day: str | date) -> Iterator[None]:
        lock_path = self._lock_path(symbol, day)
        lock_path.parent.mkdir(parents=True, exist_ok=True)
        with lock_path.open("a+b") as stream:
            fcntl.flock(stream.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(stream.fileno(), fcntl.LOCK_UN)

    def load(self, symbol: str, day: str | date) -> dict[str, Any] | None:
        path = self.path_for(symbol, day)
        if not path.is_file():
            return None
        value = _load_gzip_json(path)
        self._validate_document(value, path)
        return dict(value)

    @staticmethod
    def _validate_document(value: Any, path: Path | None = None) -> None:
        label = str(path) if path is not None else "day document"
        if not isinstance(value, Mapping):
            raise HistoricalStoreError(f"{label} must contain an object")
        if value.get("schema_version") != STORE_SCHEMA_VERSION:
            raise HistoricalStoreError(f"{label} has an unsupported schema")
        if value.get("kind") != STORE_KIND:
            raise HistoricalStoreError(f"{label} has an unsupported kind")
        symbol = normalize_symbol(str(value.get("symbol", "")))
        day = normalize_date(str(value.get("date", "")))
        if path is not None:
            if path.name != f"{day}.json.gz" or path.parent.name != day[:4]:
                raise HistoricalStoreError(f"{label} identity does not match its path")
            if path.parent.parent.name != symbol.lower():
                raise HistoricalStoreError(f"{label} symbol does not match its path")
        for field in ("datasets", "contexts"):
            rows = value.get(field)
            if not isinstance(rows, list) or any(not isinstance(row, Mapping) for row in rows):
                raise HistoricalStoreError(f"{label}.{field} must be an object array")
            identifiers = [str(row.get("id", "")) for row in rows]
            if any(not item for item in identifiers) or len(identifiers) != len(set(identifiers)):
                raise HistoricalStoreError(f"{label}.{field} identifiers must be unique")
        for dataset in value["datasets"]:
            if dataset.get("provider") != provider_id(str(dataset.get("provider", ""))):
                raise HistoricalStoreError(f"{label} contains a noncanonical provider")
            identity = {
                key: dataset.get(key)
                for key in (
                    "kind",
                    "provider",
                    "channel",
                    "timeframe",
                    "feed",
                    "adjustment",
                    "session",
                    "scope",
                    "quality",
                    "limitations",
                    "rows",
                )
            }
            content_hash = canonical_sha256(identity)
            expected_id = (
                f"{dataset.get('kind')}:{dataset.get('provider')}:"
                f"{dataset.get('channel')}:{dataset.get('timeframe') or 'na'}:"
                f"{content_hash[:16]}"
            )
            if dataset.get("content_sha256") != content_hash or dataset.get("id") != expected_id:
                raise HistoricalStoreError(f"{label} contains a corrupt dataset")
        for context in value["contexts"]:
            if context.get("provider") != provider_id(str(context.get("provider", ""))):
                raise HistoricalStoreError(f"{label} contains a noncanonical provider")
            identity = {
                key: context.get(key)
                for key in ("kind", "provider", "observed_at", "payload")
            }
            content_hash = canonical_sha256(identity)
            expected_id = (
                f"context:{context.get('provider')}:{context.get('kind')}:"
                f"{content_hash[:16]}"
            )
            if context.get("content_sha256") != content_hash or context.get("id") != expected_id:
                raise HistoricalStoreError(f"{label} contains a corrupt context")

    @staticmethod
    def _merge_item(collection: list[dict[str, Any]], incoming: Mapping[str, Any]) -> bool:
        identifier = str(incoming.get("id", ""))
        if not identifier:
            raise HistoricalStoreError("stored item is missing an id")
        for existing in collection:
            if existing.get("id") != identifier:
                continue
            changed = False
            provenance = incoming.get("provenance")
            if isinstance(provenance, Mapping):
                changed = _merge_provenance_state(existing, provenance) or changed
            return changed
        collection.append(dict(incoming))
        collection.sort(key=lambda item: str(item.get("id", "")))
        return True

    def merge(
        self,
        symbol: str,
        day: str | date,
        *,
        datasets: Iterable[Mapping[str, Any]] = (),
        contexts: Iterable[Mapping[str, Any]] = (),
    ) -> dict[str, Any]:
        normalized_symbol = normalize_symbol(symbol)
        normalized_day = normalize_date(day)
        path = self.path_for(normalized_symbol, normalized_day)
        with self._locked(normalized_symbol, normalized_day):
            document = self.load(normalized_symbol, normalized_day) or _new_document(
                normalized_symbol, normalized_day
            )
            changed = False
            for dataset in datasets:
                changed = self._merge_item(document["datasets"], dataset) or changed
            for context in contexts:
                changed = self._merge_item(document["contexts"], context) or changed
            if changed or not path.exists():
                path.parent.mkdir(parents=True, exist_ok=True)
                temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
                temporary.write_bytes(_gzip_json_bytes(document))
                os.replace(temporary, path)
            return {
                "path": str(path),
                "changed": changed,
                "datasets": len(document["datasets"]),
                "contexts": len(document["contexts"]),
                "sha256": sha256_file(path),
            }

    def dates(self, symbol: str) -> list[str]:
        directory = self.root / normalize_symbol(symbol).lower()
        if not directory.is_dir():
            return []
        return sorted(path.name[:-8] for path in directory.glob("[0-9][0-9][0-9][0-9]/*.json.gz"))

    def select_dataset(
        self,
        symbol: str,
        day: str | date,
        *,
        kind: str,
        channel: str,
        timeframe: str | None = None,
        providers: Sequence[str] = ("ibkr", "massive", "alpaca"),
        require_complete: bool = False,
        feed: str | None = None,
        adjustment: str | None = None,
    ) -> dict[str, Any] | None:
        document = self.load(symbol, day)
        if document is None:
            return None
        provider_rank = {provider_id(value): index for index, value in enumerate(providers)}
        candidates = []
        for dataset in document["datasets"]:
            if dataset.get("kind") != kind or dataset.get("channel") != channel:
                continue
            if timeframe is not None and dataset.get("timeframe") != timeframe:
                continue
            if feed is not None and dataset.get("feed") != feed:
                continue
            if adjustment is not None and dataset.get("adjustment") != adjustment:
                continue
            if require_complete and dataset.get("quality", {}).get("complete") is not True:
                continue
            provider = provider_id(str(dataset.get("provider", "")))
            if provider not in provider_rank:
                continue
            candidates.append(dataset)
        if not candidates:
            return None
        candidates.sort(
            key=lambda row: (
                provider_rank[provider_id(str(row.get("provider", "")))],
                0 if row.get("quality", {}).get("complete") is True else 1,
                -int(row.get("quality", {}).get("row_count", 0)),
                str(row.get("id", "")),
            )
        )
        return dict(candidates[0])

    def audit(self) -> dict[str, Any]:
        files = sorted(
            path
            for path in self.root.glob("*/[0-9][0-9][0-9][0-9]/*.json.gz")
            if not any(part.startswith("_") for part in path.relative_to(self.root).parts)
        )
        errors: list[dict[str, str]] = []
        dataset_count = 0
        context_count = 0
        symbols: set[str] = set()
        dates: set[str] = set()
        providers: dict[str, int] = {}
        for path in files:
            try:
                value = _load_gzip_json(path)
                self._validate_document(value, path)
                symbols.add(str(value["symbol"]))
                dates.add(str(value["date"]))
                dataset_count += len(value["datasets"])
                context_count += len(value["contexts"])
                for dataset in value["datasets"]:
                    provider = str(dataset.get("provider", "unknown"))
                    providers[provider] = providers.get(provider, 0) + 1
            except HistoricalStoreError as exc:
                errors.append({"path": str(path), "error": str(exc)})
        return {
            "schema_version": STORE_SCHEMA_VERSION,
            "root": str(self.root),
            "files": len(files),
            "symbols": len(symbols),
            "dates": len(dates),
            "datasets": dataset_count,
            "contexts": context_count,
            "providers": dict(sorted(providers.items())),
            "errors": errors,
            "valid": not errors,
        }

    def repair_provider_aliases(self) -> dict[str, Any]:
        """Rewrite known provider aliases without changing observation content."""

        files = sorted(
            path
            for path in self.root.glob("*/[0-9][0-9][0-9][0-9]/*.json.gz")
            if not any(part.startswith("_") for part in path.relative_to(self.root).parts)
        )
        changed_files = 0
        changed_items = 0
        merged_items = 0
        for path in files:
            value = _load_gzip_json(path)
            symbol = normalize_symbol(str(value.get("symbol", "")))
            day = normalize_date(str(value.get("date", "")))
            with self._locked(symbol, day):
                document = dict(_load_gzip_json(path))
                changed = False
                for field in ("datasets", "contexts"):
                    normalized_items: list[dict[str, Any]] = []
                    for original in document[field]:
                        item = dict(original)
                        normalized_provider = provider_id(str(item.get("provider", "")))
                        needs_rekey = False
                        if normalized_provider != item.get("provider"):
                            item["provider"] = normalized_provider
                            needs_rekey = True
                        if (
                            field == "datasets"
                            and normalized_provider == "ibkr"
                            and item.get("feed") == "unknown"
                            and item.get("adjustment")
                            == "provider_adjusted_unknown_basis"
                            and item.get("channel") == "trades"
                            and item.get("timeframe") in {"1d", "5m"}
                            and item.get("scope")
                            in {"daily_summary", "opening_window_only"}
                        ):
                            # The first migration release normalized the verbose
                            # IBKR pre-session provider alias after dataset
                            # construction, leaving these otherwise exact rows
                            # with an incorrect unknown feed. This combination
                            # can only originate from that release.
                            item["feed"] = "smart"
                            needs_rekey = True
                        if needs_rekey:
                            if field == "datasets":
                                identity = {
                                    key: item.get(key)
                                    for key in (
                                        "kind",
                                        "provider",
                                        "channel",
                                        "timeframe",
                                        "feed",
                                        "adjustment",
                                        "session",
                                        "scope",
                                        "quality",
                                        "limitations",
                                        "rows",
                                    )
                                }
                                content_hash = canonical_sha256(identity)
                                item["content_sha256"] = content_hash
                                item["id"] = (
                                    f"{item.get('kind')}:{normalized_provider}:"
                                    f"{item.get('channel')}:{item.get('timeframe') or 'na'}:"
                                    f"{content_hash[:16]}"
                                )
                            else:
                                identity = {
                                    key: item.get(key)
                                    for key in ("kind", "provider", "observed_at", "payload")
                                }
                                content_hash = canonical_sha256(identity)
                                item["content_sha256"] = content_hash
                                item["id"] = (
                                    f"context:{normalized_provider}:{item.get('kind')}:"
                                    f"{content_hash[:16]}"
                                )
                            changed = True
                            changed_items += 1
                        before = len(normalized_items)
                        self._merge_item(normalized_items, item)
                        if len(normalized_items) == before:
                            merged_items += 1
                            changed = True
                    document[field] = normalized_items
                if changed:
                    self._validate_document(document)
                    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
                    temporary.write_bytes(_gzip_json_bytes(document))
                    os.replace(temporary, path)
                    changed_files += 1
        return {
            "files_scanned": len(files),
            "files_changed": changed_files,
            "items_rekeyed": changed_items,
            "items_merged": merged_items,
        }


def aggregate_bars(
    rows: Sequence[Mapping[str, Any]], timeframe: str
) -> list[dict[str, Any]]:
    """Aggregate expanded minute rows into 5-minute or daily provider rows."""

    if timeframe not in {"5m", "1d"}:
        raise HistoricalStoreError(f"unsupported aggregate timeframe: {timeframe}")
    grouped: dict[tuple[Any, ...], list[Mapping[str, Any]]] = {}
    for row in rows:
        observed = datetime.fromtimestamp(int(row["epoch"]), UTC).astimezone(EASTERN)
        if timeframe == "5m":
            key = (observed.date(), observed.hour, observed.minute - observed.minute % 5)
        else:
            key = (observed.date(),)
        grouped.setdefault(key, []).append(row)
    result: list[dict[str, Any]] = []
    for values in grouped.values():
        ordered = sorted(values, key=lambda row: int(row["epoch"]))
        first = ordered[0]
        observed = datetime.fromtimestamp(int(first["epoch"]), UTC).astimezone(EASTERN)
        if timeframe == "5m":
            stamp = observed.replace(minute=observed.minute - observed.minute % 5, second=0, microsecond=0)
        else:
            stamp = datetime.combine(observed.date(), time(0), tzinfo=EASTERN)
        volume = sum(int(row.get("volume", 0)) for row in ordered)
        weighted = sum(float(row.get("wap", 0)) * int(row.get("volume", 0)) for row in ordered)
        result.append(
            {
                "epoch": int(stamp.timestamp()),
                "time_et": stamp.isoformat(),
                "date_et": stamp.date().isoformat(),
                "open": float(first["open"]),
                "high": max(float(row["high"]) for row in ordered),
                "low": min(float(row["low"]) for row in ordered),
                "close": float(ordered[-1]["close"]),
                "volume": volume,
                "count": sum(int(row.get("count", 0)) for row in ordered),
                "wap": weighted / volume if volume else 0.0,
                "interpolated": any(bool(row.get("interpolated", False)) for row in ordered),
            }
        )
    return sorted(result, key=lambda row: int(row["epoch"]))

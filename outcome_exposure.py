"""Audit and extend the immutable global date/instrument exposure history."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from collections.abc import Mapping, Sequence
from datetime import date, datetime
from pathlib import Path
from typing import Any


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_INDEX = PROJECT_ROOT / "history" / "OUTCOME_EXPOSURE_INDEX.jsonl"
BASELINE_EXPOSURE_ID = "baseline-portfolio-signals-v1"
BASELINE_EXPOSURE_IDS = {
    BASELINE_EXPOSURE_ID,
    "baseline-strategy-signals-v1",
}
BASELINE_SOURCE_PATHS = {
    "SIGNALS.jsonl": PROJECT_ROOT / "history" / "legacy" / "SIGNALS.jsonl",
    "PORTFOLIO_SIGNALS.jsonl": (
        PROJECT_ROOT / "history" / "legacy" / "PORTFOLIO_SIGNALS.jsonl"
    ),
}
SCHEMA_VERSION = 1
LANES = {"development", "confirmation", "shadow", "live", "legacy"}


class OutcomeExposureError(ValueError):
    """An exposure record or proposed untouched scope is unsafe."""


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


def _dates(values: Any, field: str) -> list[str]:
    if not isinstance(values, list) or not values:
        raise OutcomeExposureError(f"{field} must be a non-empty array")
    result: list[str] = []
    for value in values:
        if not isinstance(value, str):
            raise OutcomeExposureError(f"{field} must contain ISO dates")
        try:
            date.fromisoformat(value)
        except ValueError as exc:
            raise OutcomeExposureError(f"{field} must contain ISO dates") from exc
        result.append(value)
    if result != sorted(result) or len(result) != len(set(result)):
        raise OutcomeExposureError(f"{field} must be unique and chronological")
    return result


def _symbols(values: Any, field: str) -> list[str]:
    if not isinstance(values, list) or not values:
        raise OutcomeExposureError(f"{field} must be a non-empty array")
    result: list[str] = []
    for value in values:
        if not isinstance(value, str) or not value:
            raise OutcomeExposureError(f"{field} must contain symbols")
        symbol = value.strip().upper()
        if symbol != value or (symbol == "*" and len(values) != 1):
            raise OutcomeExposureError(f"{field} symbols are not canonical")
        result.append(symbol)
    if result != sorted(result) or len(result) != len(set(result)):
        raise OutcomeExposureError(f"{field} must be unique and sorted")
    return result


def validate_scope(value: Any) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise OutcomeExposureError("exposure scope must be an object")
    scope = dict(value)
    dates = _dates(scope.get("dates"), "scope.dates")
    symbols = scope.get("symbols")
    symbols_by_date = scope.get("symbols_by_date")
    if (symbols is None) == (symbols_by_date is None):
        raise OutcomeExposureError(
            "scope needs exactly one of symbols or symbols_by_date"
        )
    if symbols is not None:
        return {"dates": dates, "symbols": _symbols(symbols, "scope.symbols")}
    if not isinstance(symbols_by_date, Mapping) or set(symbols_by_date) != set(dates):
        raise OutcomeExposureError("symbols_by_date must cover every exact date")
    return {
        "dates": dates,
        "symbols_by_date": {
            day: _symbols(symbols_by_date[day], f"symbols_by_date.{day}")
            for day in dates
        },
    }


def scope_pairs(scope: Mapping[str, Any]) -> set[tuple[str, str]]:
    normalized = validate_scope(scope)
    if "symbols" in normalized:
        return {
            (day, symbol)
            for day in normalized["dates"]
            for symbol in normalized["symbols"]
        }
    return {
        (day, symbol)
        for day in normalized["dates"]
        for symbol in normalized["symbols_by_date"][day]
    }


def validate_record(value: Any) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise OutcomeExposureError("exposure record must be an object")
    record = dict(value)
    supplied = record.pop("record_sha256", None)
    if record.get("schema_version") != SCHEMA_VERSION:
        raise OutcomeExposureError("exposure schema_version must be 1")
    for field in ("exposure_id", "campaign_id", "source_path", "source_sha256"):
        if not isinstance(record.get(field), str) or not record[field]:
            raise OutcomeExposureError(f"exposure {field} is missing")
    if len(record["source_sha256"]) != 64:
        raise OutcomeExposureError("exposure source_sha256 must be SHA-256")
    if record.get("lane") not in LANES:
        raise OutcomeExposureError("exposure lane is invalid")
    if record.get("outcomes_accessed") is not True:
        raise OutcomeExposureError("index records only actual outcome exposure")
    recorded_at = record.get("recorded_at")
    if not isinstance(recorded_at, str):
        raise OutcomeExposureError("exposure recorded_at is missing")
    try:
        parsed = datetime.fromisoformat(recorded_at.replace("Z", "+00:00"))
    except ValueError as exc:
        raise OutcomeExposureError("exposure recorded_at is invalid") from exc
    if parsed.tzinfo is None:
        raise OutcomeExposureError("exposure recorded_at needs a timezone")
    record["scope"] = validate_scope(record.get("scope"))
    source_line_count = record.get("source_line_count")
    if source_line_count is not None and (
        isinstance(source_line_count, bool)
        or not isinstance(source_line_count, int)
        or source_line_count < 1
    ):
        raise OutcomeExposureError("source_line_count must be a positive integer")
    expected = _hash(record)
    if supplied != expected:
        raise OutcomeExposureError("exposure record hash is invalid")
    return {**record, "record_sha256": supplied}


def build_record(
    *,
    exposure_id: str,
    campaign_id: str,
    lane: str,
    recorded_at: str,
    source_path: str,
    source_sha256: str,
    scope: Mapping[str, Any],
) -> dict[str, Any]:
    content = {
        "schema_version": SCHEMA_VERSION,
        "exposure_id": exposure_id,
        "campaign_id": campaign_id,
        "lane": lane,
        "recorded_at": recorded_at,
        "source_path": source_path,
        "source_sha256": source_sha256,
        "scope": validate_scope(scope),
        "outcomes_accessed": True,
    }
    return validate_record({**content, "record_sha256": _hash(content)})


def read_index(path: Path = DEFAULT_INDEX) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    records: list[dict[str, Any]] = []
    ids: set[str] = set()
    for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if not line:
            raise OutcomeExposureError(f"{path}: blank line {number}")
        try:
            record = validate_record(json.loads(line))
        except (json.JSONDecodeError, OutcomeExposureError) as exc:
            raise OutcomeExposureError(f"{path}: line {number}: {exc}") from exc
        if record["exposure_id"] in ids:
            raise OutcomeExposureError(
                f"{path}: duplicate exposure_id {record['exposure_id']}"
            )
        ids.add(record["exposure_id"])
        records.append(record)
    return records


def append_record(record: Mapping[str, Any], path: Path = DEFAULT_INDEX) -> None:
    normalized = validate_record(record)
    existing = read_index(path)
    if any(item["exposure_id"] == normalized["exposure_id"] for item in existing):
        raise OutcomeExposureError("exposure_id is already indexed")
    path.parent.mkdir(parents=True, exist_ok=True)
    line = json.dumps(normalized, sort_keys=True, separators=(",", ":")) + "\n"
    descriptor = os.open(path, os.O_APPEND | os.O_CREAT | os.O_WRONLY, 0o644)
    try:
        os.write(descriptor, line.encode())
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def ensure_record(record: Mapping[str, Any], path: Path = DEFAULT_INDEX) -> bool:
    """Append one exposure, or prove an identical record is already present."""

    normalized = validate_record(record)
    matches = [
        item
        for item in read_index(path)
        if item["exposure_id"] == normalized["exposure_id"]
    ]
    if not matches:
        append_record(normalized, path)
        return True
    if len(matches) != 1 or matches[0] != normalized:
        raise OutcomeExposureError("exposure_id exists with different content")
    return False


def find_overlaps(
    scope: Mapping[str, Any], records: Sequence[Mapping[str, Any]]
) -> list[dict[str, str]]:
    proposed = scope_pairs(scope)
    overlaps: list[dict[str, str]] = []
    for raw_record in records:
        record = validate_record(raw_record)
        for day, symbol in sorted(scope_pairs(record["scope"])):
            if (day, symbol) in proposed or (
                symbol == "*" and any(pair[0] == day for pair in proposed)
            ):
                overlaps.append(
                    {
                        "date": day,
                        "symbol": symbol,
                        "exposure_id": record["exposure_id"],
                    }
                )
    return overlaps


def assert_untouched(
    scope: Mapping[str, Any], records: Sequence[Mapping[str, Any]]
) -> None:
    overlaps = find_overlaps(scope, records)
    if overlaps:
        raise OutcomeExposureError(
            f"proposed confirmation scope has {len(overlaps)} prior outcome exposures"
        )


def assert_disjoint(scopes: Sequence[Mapping[str, Any]]) -> None:
    occupied: set[tuple[str, str]] = set()
    for index, scope in enumerate(scopes):
        pairs = scope_pairs(scope)
        if occupied & pairs:
            raise OutcomeExposureError(
                f"confirmation scope {index} overlaps another new family"
            )
        occupied.update(pairs)


def audit(path: Path = DEFAULT_INDEX) -> dict[str, Any]:
    records = read_index(path)
    baseline_required = path.resolve() == DEFAULT_INDEX.resolve()
    baselines = {
        item["exposure_id"]: item
        for item in records
        if item["exposure_id"] in BASELINE_EXPOSURE_IDS
    }
    if baseline_required:
        if set(baselines) != BASELINE_EXPOSURE_IDS:
            raise OutcomeExposureError("global exposure index lacks its legacy baseline")
        for baseline in baselines.values():
            source_path = str(baseline["source_path"])
            source = BASELINE_SOURCE_PATHS.get(
                source_path, PROJECT_ROOT / source_path
            )
            line_count = baseline.get("source_line_count")
            if not source.is_file() or not isinstance(line_count, int):
                raise OutcomeExposureError(
                    "legacy exposure baseline source is unavailable"
                )
            lines = source.read_bytes().splitlines(keepends=True)
            observed = hashlib.sha256(b"".join(lines[:line_count])).hexdigest()
            if len(lines) < line_count or observed != baseline["source_sha256"]:
                raise OutcomeExposureError("legacy exposure baseline source drifted")
    return {
        "valid": True,
        "baseline_complete": set(baselines) == BASELINE_EXPOSURE_IDS,
        "records": len(records),
        "exposed_pairs": sum(len(scope_pairs(item["scope"])) for item in records),
        "index_sha256": hashlib.sha256(path.read_bytes()).hexdigest()
        if path.exists()
        else hashlib.sha256(b"").hexdigest(),
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("audit",))
    parser.add_argument("--index", type=Path, default=DEFAULT_INDEX)
    return parser


def main() -> int:
    args = _parser().parse_args()
    print(json.dumps(audit(args.index), indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""Freeze and collect the public session calendar for challenger acquisition."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
from collections.abc import Mapping, Sequence
from datetime import UTC, date, datetime, time
from pathlib import Path
from typing import Any

import requests

import development_sec_submissions as publication_gate
from historical_providers import AlpacaConfig, HistoricalProviderError
from learning_data import LearningDataError, freeze_dataset_contract, load_frozen_dataset_contract


PROJECT_ROOT = Path(__file__).resolve().parent
DATASET_ID = "dataset-challenger-orb-retest-calendar-2026-07-20-v1"
CALENDAR_START = "2023-01-01"
CALENDAR_END = "2026-07-17"
ENDPOINT = "https://api.alpaca.markets/v2/calendar"
DEFAULT_MANIFEST_ROOT = PROJECT_ROOT / "historical_batches/challenger_orb_retest_v1/calendar_manifests"
DEFAULT_CONTRACT_STATUS = PROJECT_ROOT / "historical_batches/challenger_orb_retest_v1/calendar-contract-status.json"
DEFAULT_COLLECTION_STATUS = PROJECT_ROOT / "historical_batches/challenger_orb_retest_v1/calendar-status.json"
DEFAULT_CALENDAR = PROJECT_ROOT / "historical_batches/challenger_orb_retest_v1/session-calendar-2023-01-through-2026-07.json"
DEFAULT_SOURCE = PROJECT_ROOT / "historical_batches/challenger_orb_retest_v1/session-calendar-source.json"
INSPECTOR = PROJECT_ROOT / "challenger_orb_retest_calendar_inspection.py"


class ChallengerCalendarError(RuntimeError):
    """The calendar contract or provider result is incomplete."""


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False
    ).encode("utf-8")


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as source:
            for chunk in iter(lambda: source.read(1024 * 1024), b""):
                digest.update(chunk)
    except OSError as exc:
        raise ChallengerCalendarError(f"cannot hash {path}: {exc}") from exc
    return digest.hexdigest()


def _repo_path(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(PROJECT_ROOT))
    except ValueError as exc:
        raise ChallengerCalendarError("public path must be repository-relative") from exc


def _write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    os.replace(temporary, path)


def _read_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ChallengerCalendarError(f"cannot read {path}: {exc}") from exc


def _published(path: Path) -> dict[str, str]:
    try:
        return publication_gate._published_source(path)
    except (
        publication_gate.DevelopmentSecSubmissionsError,
        subprocess.SubprocessError,
    ) as exc:
        raise ChallengerCalendarError(str(exc)) from exc


def _binding(path: Path) -> dict[str, str]:
    return {"path": _repo_path(path), "sha256": _sha256_file(path)}


def _verify_binding(value: Mapping[str, Any]) -> None:
    raw = value.get("path")
    if not isinstance(raw, str) or not raw:
        raise ChallengerCalendarError("bound path is missing")
    relative = Path(raw)
    if relative.is_absolute() or ".." in relative.parts:
        raise ChallengerCalendarError("bound path is unsafe")
    path = PROJECT_ROOT / relative
    if not path.is_file() or _sha256_file(path) != value.get("sha256"):
        raise ChallengerCalendarError(f"bound artifact drifted: {relative}")


def _normalize_rows(payload: Any) -> list[dict[str, str]]:
    if not isinstance(payload, list) or not payload:
        raise ChallengerCalendarError("Alpaca calendar response is empty")
    rows: list[dict[str, str]] = []
    for item in payload:
        if not isinstance(item, Mapping):
            raise ChallengerCalendarError("Alpaca calendar row is malformed")
        try:
            day = date.fromisoformat(str(item.get("date")))
            opened = time.fromisoformat(str(item.get("open")))
            closed = time.fromisoformat(str(item.get("close")))
        except ValueError as exc:
            raise ChallengerCalendarError("Alpaca calendar row is malformed") from exc
        if day.weekday() >= 5 or opened >= closed:
            raise ChallengerCalendarError("Alpaca calendar session is invalid")
        rows.append(
            {
                "date": day.isoformat(),
                "open_et": opened.isoformat(timespec="minutes"),
                "close_et": closed.isoformat(timespec="minutes"),
            }
        )
    if rows != sorted(rows, key=lambda row: row["date"]):
        raise ChallengerCalendarError("Alpaca calendar is not chronological")
    dates = [row["date"] for row in rows]
    if len(dates) != len(set(dates)):
        raise ChallengerCalendarError("Alpaca calendar contains duplicate dates")
    if dates[0] < CALENDAR_START or dates[-1] > CALENDAR_END or len(rows) < 850:
        raise ChallengerCalendarError("Alpaca calendar coverage is incomplete")
    return rows


def _expected_contract() -> dict[str, Any]:
    return {
        "implementation_contract": {
            "collector": _binding(Path(__file__)),
            "inspector": _binding(INSPECTOR),
        },
        "calendar_contract": {
            "provider": "Alpaca Market Calendar API",
            "endpoint": ENDPOINT,
            "query": {"start": CALENDAR_START, "end": CALENDAR_END},
            "minimum_sessions": 850,
            "strictly_chronological_unique_dates": True,
            "weekday_sessions_only": True,
            "valid_open_before_close_required": True,
            "date_substitution_allowed": False,
        },
        "output_contract": {
            "calendar_path": _repo_path(DEFAULT_CALENDAR),
            "source_path": _repo_path(DEFAULT_SOURCE),
            "pre_freeze_output_artifacts": int(DEFAULT_CALENDAR.exists())
            + int(DEFAULT_SOURCE.exists()),
            "requested_dates_semantics": "calendar_query_bounds_not_target_sessions",
            "target_outcomes_observed_or_derived": False,
        },
    }


def freeze_contract(*, output_root: Path) -> tuple[Path, dict[str, Any]]:
    _published(Path(__file__))
    _published(INSPECTOR)
    contract = {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "registered_at": datetime.now(UTC).isoformat(),
        "requested_dates": [CALENDAR_START, CALENDAR_END],
        "dataset_payload": {
            "lane": "development",
            "claim_scope": "DEVELOPMENT_ONLY",
            "status": "COLLECTING",
            "evidence_paths": [
                "CHALLENGER_ORB_RETEST.md",
                "PRODUCTION_STRATEGY_VALIDATION.md",
            ],
            "inspected": False,
        },
        **_expected_contract(),
    }
    if contract["output_contract"]["pre_freeze_output_artifacts"] != 0:
        raise ChallengerCalendarError("calendar output exists before freeze")
    return freeze_dataset_contract(contract, output_root)


def collect(*, manifest_path: Path, env_path: Path) -> dict[str, Any]:
    _published(Path(__file__))
    _published(manifest_path)
    manifest = load_frozen_dataset_contract(manifest_path)
    if manifest.get("dataset_id") != DATASET_ID:
        raise ChallengerCalendarError("calendar manifest identity differs")
    for value in manifest["implementation_contract"].values():
        _verify_binding(value)
    if manifest.get("calendar_contract") != _expected_contract()["calendar_contract"]:
        raise ChallengerCalendarError("calendar contract drifted")
    if DEFAULT_CALENDAR.exists() != DEFAULT_SOURCE.exists():
        raise ChallengerCalendarError("calendar output is only partially present")
    if DEFAULT_CALENDAR.exists():
        existing = _read_json(DEFAULT_SOURCE)
        if not isinstance(existing, Mapping):
            raise ChallengerCalendarError("existing calendar source is malformed")
        expected_existing = {
            "dataset_id": DATASET_ID,
            "manifest_sha256": manifest["manifest_sha256"],
            "query": {"start": CALENDAR_START, "end": CALENDAR_END},
            "calendar_sha256": _sha256_file(DEFAULT_CALENDAR),
            "target_outcomes_observed_or_derived": False,
        }
        for key, value in expected_existing.items():
            if existing.get(key) != value:
                raise ChallengerCalendarError(f"existing calendar drifted at {key}")
        result = {
            "schema_version": 1,
            "dataset_id": DATASET_ID,
            "manifest_sha256": manifest["manifest_sha256"],
            "status": "COLLECTION_COMPLETE",
            "inspected": False,
            "sessions": int(existing["sessions"]),
            "first_session": existing["first_session"],
            "last_session": existing["last_session"],
            "calendar_sha256": existing["calendar_sha256"],
            "resumed_from_existing_output": True,
            "target_outcomes_observed_or_derived": False,
        }
        _write_json(DEFAULT_COLLECTION_STATUS, result)
        return result
    try:
        config = AlpacaConfig.optional_from_env(env_path)
    except HistoricalProviderError as exc:
        raise ChallengerCalendarError(str(exc)) from exc
    if config is None:
        raise ChallengerCalendarError("Alpaca credentials are unavailable")
    response = requests.get(
        ENDPOINT,
        params={"start": CALENDAR_START, "end": CALENDAR_END},
        headers={
            "APCA-API-KEY-ID": config.api_key,
            "APCA-API-SECRET-KEY": config.api_secret,
        },
        timeout=config.timeout_seconds,
    )
    if response.status_code != 200:
        raise ChallengerCalendarError(f"Alpaca calendar HTTP {response.status_code}")
    try:
        rows = _normalize_rows(response.json())
    except ValueError as exc:
        raise ChallengerCalendarError("Alpaca calendar returned invalid JSON") from exc
    _write_json(DEFAULT_CALENDAR, rows)
    source = {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "provider": "Alpaca Market Calendar API",
        "endpoint": ENDPOINT,
        "query": {"start": CALENDAR_START, "end": CALENDAR_END},
        "retrieved_at": datetime.now(UTC).isoformat(),
        "sessions": len(rows),
        "first_session": rows[0]["date"],
        "last_session": rows[-1]["date"],
        "calendar_path": _repo_path(DEFAULT_CALENDAR),
        "calendar_sha256": _sha256_file(DEFAULT_CALENDAR),
        "target_outcomes_observed_or_derived": False,
    }
    _write_json(DEFAULT_SOURCE, source)
    result = {
        "schema_version": 1,
        "dataset_id": DATASET_ID,
        "manifest_sha256": manifest["manifest_sha256"],
        "status": "COLLECTION_COMPLETE",
        "inspected": False,
        "sessions": len(rows),
        "first_session": rows[0]["date"],
        "last_session": rows[-1]["date"],
        "calendar_sha256": source["calendar_sha256"],
        "resumed_from_existing_output": False,
        "target_outcomes_observed_or_derived": False,
    }
    _write_json(DEFAULT_COLLECTION_STATUS, result)
    return result


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("freeze")
    collect_parser = sub.add_parser("collect")
    collect_parser.add_argument("manifest", type=Path)
    parser.add_argument("--env", type=Path, default=PROJECT_ROOT / ".env")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "freeze":
            path, manifest = freeze_contract(output_root=DEFAULT_MANIFEST_ROOT)
            result = {
                "schema_version": 1,
                "dataset_id": DATASET_ID,
                "manifest_sha256": manifest["manifest_sha256"],
                "path": _repo_path(path),
                "status": "FROZEN_READY",
                "inspected": False,
                "pre_freeze_output_artifacts": 0,
                "target_outcomes_observed_or_derived": False,
            }
            _write_json(DEFAULT_CONTRACT_STATUS, result)
        else:
            result = collect(manifest_path=args.manifest, env_path=args.env)
        print(json.dumps(result, indent=2, sort_keys=True))
        return 0
    except (
        ChallengerCalendarError,
        HistoricalProviderError,
        LearningDataError,
        OSError,
    ) as exc:
        print(json.dumps({"status": "error", "error": str(exc)}), file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())

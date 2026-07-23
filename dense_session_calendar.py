"""Freeze and collect the outcome-blind extended session calendar for dense v2."""

from __future__ import annotations

import argparse
import json
import os
from collections.abc import Callable, Mapping
from datetime import date, datetime
from pathlib import Path
from typing import Any

import requests

import next_week_discovery_batch as batch
import strategy_discovery
from historical_providers import AlpacaConfig, HistoricalProviderError
from historical_store import DEFAULT_ENV_PATH


PROJECT_ROOT = Path(__file__).resolve().parent
CALENDAR_START = "2020-01-01"
CALENDAR_END = "2026-07-17"
MINIMUM_SESSIONS = 1_600
ENDPOINT = "https://api.alpaca.markets/v2/calendar"
CONTRACT_KIND = "dense-session-calendar-contract"
CONTRACT_INSPECTION_KIND = "dense-session-calendar-contract-inspection"
COLLECTION_KIND = "dense-session-calendar-collection"
DEFAULT_ROOT = PROJECT_ROOT / "strategy_tournament/v2/calendar"
DEFAULT_CALENDAR = (
    PROJECT_ROOT
    / "historical_batches/dense_v2/session-calendar-2020-01-through-2026-07.json"
)
DEFAULT_SOURCE = PROJECT_ROOT / "historical_batches/dense_v2/session-calendar-source.json"
INSPECTOR_PATH = PROJECT_ROOT / "dense_session_calendar_inspection.py"


class DenseSessionCalendarError(RuntimeError):
    """The calendar contract, provider result, or inspection is incomplete."""


def _repo_path(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(PROJECT_ROOT.resolve()))
    except ValueError as exc:
        raise DenseSessionCalendarError(f"path is outside repository: {path}") from exc


def _write_json(path: Path, value: Any) -> None:
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


def _implementation_hashes() -> dict[str, str]:
    paths = (Path(__file__), INSPECTOR_PATH)
    if any(not path.is_file() for path in paths):
        raise DenseSessionCalendarError("calendar implementation is incomplete")
    return {_repo_path(path): strategy_discovery._file_hash(path) for path in paths}


def freeze_contract(
    *,
    created_at: str,
    root: Path = DEFAULT_ROOT,
    calendar_path: Path = DEFAULT_CALENDAR,
    source_path: Path = DEFAULT_SOURCE,
    enforce_commit: bool = True,
) -> tuple[Path, dict[str, Any]]:
    try:
        timestamp = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
    except ValueError as exc:
        raise DenseSessionCalendarError("created_at is invalid") from exc
    if timestamp.tzinfo is None:
        raise DenseSessionCalendarError("created_at must include a timezone")
    if enforce_commit:
        strategy_discovery.require_committed(Path(__file__))
        strategy_discovery.require_committed(INSPECTOR_PATH)
    if calendar_path.exists() or source_path.exists():
        raise DenseSessionCalendarError("calendar output exists before contract freeze")
    payload = {
        "schema_version": 1,
        "artifact_kind": CONTRACT_KIND,
        "campaign_id": batch.CAMPAIGN_ID,
        "state": "CALENDAR_CONTRACT_FROZEN",
        "created_at": created_at,
        "provider": "Alpaca Market Calendar API",
        "endpoint": ENDPOINT,
        "query": {"start": CALENDAR_START, "end": CALENDAR_END},
        "minimum_sessions": MINIMUM_SESSIONS,
        "calendar_path": _repo_path(calendar_path),
        "source_path": _repo_path(source_path),
        "implementation_hashes": _implementation_hashes(),
        "date_substitutions_allowed": False,
        "provider_requests": 0,
        "market_prices_accessed": False,
        "target_outcomes_accessed": False,
        "broker_actions": 0,
    }
    return strategy_discovery._write_artifact(
        payload, root / "contract", "dense-session-calendar-contract"
    )


def inspect_contract(
    contract_path: Path,
    *,
    inspected_at: str,
    root: Path = DEFAULT_ROOT,
    enforce_commit: bool = True,
) -> tuple[Path, dict[str, Any]]:
    if enforce_commit:
        strategy_discovery.require_committed(contract_path)
    contract = strategy_discovery.load_artifact(
        contract_path, expected_kind=CONTRACT_KIND
    )
    if not (
        contract.get("state") == "CALENDAR_CONTRACT_FROZEN"
        and contract.get("implementation_hashes") == _implementation_hashes()
        and contract.get("provider_requests") == 0
        and contract.get("market_prices_accessed") is False
        and contract.get("target_outcomes_accessed") is False
        and contract.get("broker_actions") == 0
    ):
        raise DenseSessionCalendarError("calendar collection contract drifted")
    if (PROJECT_ROOT / contract["calendar_path"]).exists() or (
        PROJECT_ROOT / contract["source_path"]
    ).exists():
        raise DenseSessionCalendarError("calendar output appeared before inspection")
    payload = {
        "schema_version": 1,
        "artifact_kind": CONTRACT_INSPECTION_KIND,
        "campaign_id": batch.CAMPAIGN_ID,
        "state": "CALENDAR_CONTRACT_INSPECTED_READY",
        "contract_path": _repo_path(contract_path),
        "contract_sha256": contract["artifact_sha256"],
        "inspected_at": inspected_at,
        "checks": {
            "provider_query_exact": True,
            "implementation_hashes_exact": True,
            "outputs_absent": True,
            "outcome_boundary_closed": True,
            "substitutions_forbidden": True,
        },
        "provider_access_not_before": batch.ACTIVATION_NOT_BEFORE.isoformat(),
        "provider_requests": 0,
        "market_prices_accessed": False,
        "target_outcomes_accessed": False,
        "broker_actions": 0,
    }
    return strategy_discovery._write_artifact(
        payload, root / "contract-inspection", "dense-session-calendar-contract-inspection"
    )


def normalize_rows(payload: Any) -> list[dict[str, str]]:
    if not isinstance(payload, list) or not payload:
        raise DenseSessionCalendarError("Alpaca calendar response is empty")
    rows: list[dict[str, str]] = []
    for item in payload:
        if not isinstance(item, Mapping):
            raise DenseSessionCalendarError("Alpaca calendar row is malformed")
        try:
            session_date = date.fromisoformat(str(item["date"]))
            opened = str(item.get("open", item.get("open_et")))
            closed = str(item.get("close", item.get("close_et")))
            open_time = datetime.strptime(opened, "%H:%M").time()
            close_time = datetime.strptime(closed, "%H:%M").time()
        except (KeyError, TypeError, ValueError) as exc:
            raise DenseSessionCalendarError("Alpaca calendar row is malformed") from exc
        if session_date.weekday() >= 5 or open_time >= close_time:
            raise DenseSessionCalendarError("Alpaca calendar session is invalid")
        rows.append(
            {
                "date": session_date.isoformat(),
                "open_et": open_time.isoformat(timespec="minutes"),
                "close_et": close_time.isoformat(timespec="minutes"),
            }
        )
    dates = [item["date"] for item in rows]
    if dates != sorted(dates) or len(dates) != len(set(dates)):
        raise DenseSessionCalendarError("calendar dates are not unique and chronological")
    if (
        dates[0] < CALENDAR_START
        or dates[-1] > CALENDAR_END
        or len(rows) < MINIMUM_SESSIONS
    ):
        raise DenseSessionCalendarError("calendar coverage is incomplete")
    return rows


def _single_contract_inspection(
    root: Path, contract: Mapping[str, Any], *, enforce_commit: bool
) -> tuple[Path, dict[str, Any]]:
    paths = sorted((root / "contract-inspection").glob("*.json"))
    if len(paths) != 1:
        raise DenseSessionCalendarError("expected one calendar contract inspection")
    path = paths[0]
    if enforce_commit:
        strategy_discovery.require_committed(path)
    inspection = strategy_discovery.load_artifact(
        path, expected_kind=CONTRACT_INSPECTION_KIND
    )
    if (
        inspection.get("state") != "CALENDAR_CONTRACT_INSPECTED_READY"
        or inspection.get("contract_sha256") != contract["artifact_sha256"]
    ):
        raise DenseSessionCalendarError("calendar contract inspection binding drifted")
    return path, inspection


def collect(
    contract_path: Path,
    *,
    as_of: date | None = None,
    collected_at: str,
    env_path: Path = DEFAULT_ENV_PATH,
    root: Path = DEFAULT_ROOT,
    getter: Callable[..., Any] = requests.get,
    enforce_commit: bool = True,
) -> tuple[Path, dict[str, Any]]:
    current = as_of or date.today()
    if current < batch.ACTIVATION_NOT_BEFORE:
        raise DenseSessionCalendarError(
            f"calendar provider access is closed until {batch.ACTIVATION_NOT_BEFORE}"
        )
    if enforce_commit:
        strategy_discovery.require_committed(contract_path)
    contract = strategy_discovery.load_artifact(
        contract_path, expected_kind=CONTRACT_KIND
    )
    inspection_path, inspection = _single_contract_inspection(
        root, contract, enforce_commit=enforce_commit
    )
    calendar_path = PROJECT_ROOT / str(contract["calendar_path"])
    source_path = PROJECT_ROOT / str(contract["source_path"])
    existing = sorted((root / "collection").glob("*.json"))
    if existing:
        if len(existing) != 1:
            raise DenseSessionCalendarError("multiple calendar collection statuses exist")
        status = strategy_discovery.load_artifact(
            existing[0], expected_kind=COLLECTION_KIND
        )
        if status.get("contract_sha256") != contract["artifact_sha256"]:
            raise DenseSessionCalendarError("existing calendar status is unrelated")
        return existing[0], status
    if source_path.exists() and not calendar_path.exists():
        raise DenseSessionCalendarError("calendar source exists without calendar rows")
    resumed = calendar_path.exists()
    if resumed:
        try:
            rows = normalize_rows(json.loads(calendar_path.read_text(encoding="utf-8")))
        except (OSError, json.JSONDecodeError) as exc:
            raise DenseSessionCalendarError("resumable calendar rows are invalid") from exc
        elapsed = 0.0
    else:
        config = AlpacaConfig.optional_from_env(env_path)
        if config is None:
            raise DenseSessionCalendarError("Alpaca credentials are unavailable")
        started = datetime.now().timestamp()
        response = getter(
            ENDPOINT,
            params=dict(contract["query"]),
            headers={
                "APCA-API-KEY-ID": config.api_key,
                "APCA-API-SECRET-KEY": config.api_secret,
            },
            timeout=config.timeout_seconds,
        )
        elapsed = datetime.now().timestamp() - started
        if response.status_code != 200:
            raise DenseSessionCalendarError(
                f"Alpaca calendar HTTP {response.status_code}"
            )
        try:
            rows = normalize_rows(response.json())
        except ValueError as exc:
            raise DenseSessionCalendarError(
                "Alpaca calendar returned invalid JSON"
            ) from exc
        _write_json(calendar_path, rows)
    source = (
        json.loads(source_path.read_text(encoding="utf-8"))
        if source_path.exists()
        else {
            "schema_version": 1,
            "campaign_id": batch.CAMPAIGN_ID,
            "contract_sha256": contract["artifact_sha256"],
            "contract_inspection_sha256": inspection["artifact_sha256"],
            "provider": contract["provider"],
            "endpoint": contract["endpoint"],
            "query": contract["query"],
            "collected_at": collected_at,
            "sessions": len(rows),
            "first_session": rows[0]["date"],
            "last_session": rows[-1]["date"],
            "calendar_sha256": strategy_discovery._file_hash(calendar_path),
            "provider_requests": 1,
            "request_seconds": elapsed,
            "resumed_after_provider_response": resumed,
            "market_prices_accessed": False,
            "target_outcomes_accessed": False,
            "broker_actions": 0,
        }
    )
    if not (
        isinstance(source, Mapping)
        and source.get("contract_sha256") == contract["artifact_sha256"]
        and source.get("calendar_sha256")
        == strategy_discovery._file_hash(calendar_path)
    ):
        raise DenseSessionCalendarError("resumable calendar source drifted")
    _write_json(source_path, source)
    payload = {
        "schema_version": 1,
        "artifact_kind": COLLECTION_KIND,
        "campaign_id": batch.CAMPAIGN_ID,
        "state": "CALENDAR_COLLECTED_UNINSPECTED",
        "contract_path": _repo_path(contract_path),
        "contract_sha256": contract["artifact_sha256"],
        "contract_inspection_path": _repo_path(inspection_path),
        "contract_inspection_sha256": inspection["artifact_sha256"],
        "calendar_path": contract["calendar_path"],
        "calendar_sha256": source["calendar_sha256"],
        "source_path": contract["source_path"],
        "sessions": len(rows),
        "provider_requests": 1,
        "request_seconds": source["request_seconds"],
        "market_prices_accessed": False,
        "target_outcomes_accessed": False,
        "broker_actions": 0,
        "collected_at": collected_at,
    }
    return strategy_discovery._write_artifact(
        payload, root / "collection", "dense-session-calendar-collection"
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=DEFAULT_ROOT)
    sub = parser.add_subparsers(dest="command", required=True)
    freeze = sub.add_parser("freeze")
    freeze.add_argument("--created-at", required=True)
    inspect = sub.add_parser("inspect-contract")
    inspect.add_argument("contract", type=Path)
    inspect.add_argument("--inspected-at", required=True)
    collect_parser = sub.add_parser("collect")
    collect_parser.add_argument("contract", type=Path)
    collect_parser.add_argument("--as-of", type=date.fromisoformat)
    collect_parser.add_argument("--collected-at", required=True)
    return parser


def main() -> int:
    args = _parser().parse_args()
    try:
        if args.command == "freeze":
            path, artifact = freeze_contract(created_at=args.created_at, root=args.root)
        elif args.command == "inspect-contract":
            path, artifact = inspect_contract(
                args.contract, inspected_at=args.inspected_at, root=args.root
            )
        else:
            path, artifact = collect(
                args.contract,
                as_of=args.as_of,
                collected_at=args.collected_at,
                root=args.root,
            )
        print(
            json.dumps(
                {
                    "written": _repo_path(path),
                    "artifact_sha256": artifact["artifact_sha256"],
                    "state": artifact["state"],
                    "provider_requests": artifact["provider_requests"],
                    "market_prices_accessed": False,
                    "target_outcomes_accessed": False,
                    "broker_actions": 0,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    except (
        DenseSessionCalendarError,
        HistoricalProviderError,
        strategy_discovery.StrategyDiscoveryError,
        OSError,
        ValueError,
    ) as exc:
        print(json.dumps({"error": str(exc), "error_type": type(exc).__name__}, indent=2))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

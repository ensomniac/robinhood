"""Freeze and collect outcome-blind Massive earnings-history capacity."""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import time
from collections.abc import Mapping, Sequence
from datetime import datetime
from pathlib import Path
from typing import Any

import requests
from dotenv import dotenv_values

import strategy_discovery
from historical_store import HistoricalDayStore, sha256_file


PROJECT_ROOT = Path(__file__).resolve().parent
CAMPAIGN_ID = "multi-strategy-portfolio-validation-v2"
FAMILY_ID = "earnings-positive-surprise-drift"
SUCCESSOR_ID = "earnings-positive-surprise-drift-v4-massive-history"
DEFAULT_ROOT = (
    PROJECT_ROOT / "strategy_tournament/v2/continuous" / SUCCESSOR_ID
)
ENDPOINT = "https://api.massive.com/benzinga/v1/earnings"
FIRST_DATE = "2010-04-30"
LAST_DATE = "2024-12-31"
MAX_ROWS_PER_REQUEST = 50_000
MINIMUM_INTERVAL_SECONDS = 0.35
PRIVATE_NAMESPACE = "_derived/earnings_massive_capacity"
DOCUMENTATION_URL = (
    "https://massive.com/docs/rest/partners/benzinga/earnings"
)


class EarningsMassiveCapacityError(RuntimeError):
    """The frozen metadata request graph or retained response drifted."""


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
        raise EarningsMassiveCapacityError(f"cannot read {path}: {exc}") from exc
    if not isinstance(value, dict):
        raise EarningsMassiveCapacityError(f"{path} must contain an object")
    return value


def _write(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(value, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


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
        raise EarningsMassiveCapacityError(
            f"{name} must be an ISO timestamp"
        ) from exc
    if parsed.tzinfo is None:
        raise EarningsMassiveCapacityError(f"{name} must include a timezone")
    return parsed.isoformat().replace("+00:00", "Z")


def _repo_path(path: Path) -> str:
    return str(path.resolve().relative_to(PROJECT_ROOT.resolve()))


def _requests() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for year in range(2010, 2025):
        start = FIRST_DATE if year == 2010 else f"{year}-01-01"
        end = LAST_DATE if year == 2024 else f"{year}-12-31"
        request = {
            "ordinal": len(rows),
            "method": "GET",
            "endpoint": ENDPOINT,
            "parameters": {
                "date.gte": start,
                "date.lte": end,
                "limit": MAX_ROWS_PER_REQUEST,
                "sort": "date.asc,ticker.asc",
            },
        }
        request["request_sha256"] = hashlib.sha256(
            canonical_bytes(request)
        ).hexdigest()
        rows.append(request)
    return rows


def build_contract(*, created_at: str) -> dict[str, Any]:
    for path in (
        Path(__file__).resolve(),
        PROJECT_ROOT / "earnings_massive_capacity_inspection.py",
    ):
        strategy_discovery.require_committed(path)
    created = _timestamp(created_at, "created_at")
    requests_graph = _requests()
    value: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": "earnings-massive-metadata-contract",
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "successor_id": SUCCESSOR_ID,
        "created_at": created,
        "provider": "Massive Benzinga earnings REST API",
        "provider_documentation": DOCUMENTATION_URL,
        "endpoint": ENDPOINT,
        "history_start": FIRST_DATE,
        "history_end": LAST_DATE,
        "requests": requests_graph,
        "authorized_provider_requests": len(requests_graph),
        "request_policy": {
            "one_exact_nonpaginated_request_per_calendar_year": True,
            "maximum_rows_per_request": MAX_ROWS_PER_REQUEST,
            "minimum_interval_seconds": MINIMUM_INTERVAL_SECONDS,
            "unexpected_next_url_fails_closed": True,
            "retry_requests_permitted": 0,
            "substitutions_permitted": 0,
        },
        "capacity_semantics": {
            "metadata_only": True,
            "required_fields": [
                "actual_eps",
                "date",
                "date_status",
                "estimated_eps",
                "ticker",
                "time",
            ],
            "confirmed_reports_only": True,
            "positive_eps_surprise_only": True,
            "duplicate_event_keys_receive_zero_credit": True,
            "event_key": [
                "ticker",
                "date",
                "time",
                "fiscal_year",
                "fiscal_period",
            ],
        },
        "existing_family_rule_policy": {
            "mechanism_parameters_may_not_change": True,
            "all_prior_trials_enter_selection_correction": True,
            "no_new_mechanism_family_slot_consumed": True,
        },
        "implementation_hashes": {
            "earnings_massive_capacity.py": sha256_file(Path(__file__).resolve()),
            "earnings_massive_capacity_inspection.py": sha256_file(
                PROJECT_ROOT / "earnings_massive_capacity_inspection.py"
            ),
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


def _api_key() -> str:
    values: dict[str, Any] = {}
    env_path = PROJECT_ROOT / ".env"
    if env_path.exists():
        values.update(dotenv_values(env_path, interpolate=False))
    if "MASSIVE_API_KEY" in os.environ:
        values["MASSIVE_API_KEY"] = os.environ["MASSIVE_API_KEY"]
    value = str(values.get("MASSIVE_API_KEY") or "").strip()
    if not value:
        raise EarningsMassiveCapacityError("MASSIVE_API_KEY is not configured")
    return value


def _normalize_row(row: Mapping[str, Any]) -> dict[str, Any]:
    allowed = (
        "actual_eps",
        "actual_revenue",
        "benzinga_id",
        "company_name",
        "currency",
        "date",
        "date_status",
        "eps_method",
        "eps_surprise",
        "eps_surprise_percent",
        "estimated_eps",
        "estimated_revenue",
        "fiscal_period",
        "fiscal_year",
        "importance",
        "last_updated",
        "notes",
        "previous_eps",
        "previous_revenue",
        "revenue_method",
        "revenue_surprise",
        "revenue_surprise_percent",
        "ticker",
        "time",
    )
    return {key: row[key] for key in allowed if key in row}


def _request(
    request: Mapping[str, Any],
    *,
    api_key: str,
    session: requests.Session,
    timeout_seconds: float,
) -> tuple[dict[str, Any], float]:
    started = time.monotonic()
    try:
        response = session.get(
            str(request["endpoint"]),
            params={
                **dict(request["parameters"]),
                "apiKey": api_key,
            },
            timeout=timeout_seconds,
        )
    except requests.RequestException as exc:
        raise EarningsMassiveCapacityError(
            "Massive earnings request failed before a response"
        ) from exc
    elapsed = time.monotonic() - started
    if response.status_code in (401, 403):
        raise EarningsMassiveCapacityError(
            f"Massive earnings permission denied with HTTP {response.status_code}"
        )
    if response.status_code >= 400:
        raise EarningsMassiveCapacityError(
            f"Massive earnings returned HTTP {response.status_code}"
        )
    try:
        payload = response.json()
    except ValueError as exc:
        raise EarningsMassiveCapacityError(
            "Massive earnings response was not JSON"
        ) from exc
    if not isinstance(payload, dict) or payload.get("status") != "OK":
        raise EarningsMassiveCapacityError(
            "Massive earnings response status was not OK"
        )
    rows = payload.get("results")
    if not isinstance(rows, list):
        raise EarningsMassiveCapacityError(
            "Massive earnings results must be an array"
        )
    if payload.get("next_url"):
        raise EarningsMassiveCapacityError(
            "Massive earnings annual request exceeded the frozen row limit"
        )
    return {
        "request_sha256": request["request_sha256"],
        "rows": [_normalize_row(row) for row in rows if isinstance(row, Mapping)],
    }, elapsed


def collect(
    contract_path: Path,
    inspection_path: Path,
    *,
    collected_at: str,
    store: HistoricalDayStore | None = None,
    session: requests.Session | None = None,
    timeout_seconds: float = 30.0,
    minimum_interval_seconds: float = MINIMUM_INTERVAL_SECONDS,
) -> tuple[Path, dict[str, Any]]:
    strategy_discovery.require_committed(contract_path)
    strategy_discovery.require_committed(inspection_path)
    contract = _read(contract_path)
    inspection = _read(inspection_path)
    if not (
        contract.get("contract_sha256") == self_hash(contract, "contract_sha256")
        and inspection.get("contract_sha256") == contract["contract_sha256"]
        and inspection.get("state") == "METADATA_CONTRACT_INSPECTED_READY"
        and inspection.get("valid") is True
    ):
        raise EarningsMassiveCapacityError(
            "committed contract or inspection is invalid"
        )
    own_session = session is None
    http = session or requests.Session()
    pages: list[dict[str, Any]] = []
    elapsed_seconds = 0.0
    pacing_wait_seconds = 0.0
    try:
        api_key = _api_key()
        for ordinal, request in enumerate(contract["requests"]):
            if ordinal and minimum_interval_seconds > 0:
                time.sleep(minimum_interval_seconds)
                pacing_wait_seconds += minimum_interval_seconds
            page, elapsed = _request(
                request,
                api_key=api_key,
                session=http,
                timeout_seconds=timeout_seconds,
            )
            pages.append(page)
            elapsed_seconds += elapsed
    finally:
        if own_session:
            http.close()
    completed = _timestamp(collected_at, "collected_at")
    private_value: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": "private-earnings-massive-metadata",
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "successor_id": SUCCESSOR_ID,
        "contract_sha256": contract["contract_sha256"],
        "pages": pages,
        "market_prices_accessed": False,
        "forward_returns_accessed": False,
        "strategy_metrics_computed": 0,
        "broker_actions": 0,
    }
    private_value["content_sha256"] = self_hash(
        private_value, "content_sha256"
    )
    source = store or HistoricalDayStore.from_env()
    relative = (
        Path(PRIVATE_NAMESPACE)
        / f"{private_value['content_sha256']}.json.gz"
    )
    private_path = source.root / relative
    raw = _write_private(private_path, private_value)
    row_count = sum(len(page["rows"]) for page in pages)
    value: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": "earnings-massive-metadata-collection",
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "successor_id": SUCCESSOR_ID,
        "state": "METADATA_COLLECTED_UNINSPECTED",
        "collected_at": completed,
        "contract_path": _repo_path(contract_path),
        "contract_file_sha256": sha256_file(contract_path),
        "contract_sha256": contract["contract_sha256"],
        "inspection_path": _repo_path(inspection_path),
        "inspection_file_sha256": sha256_file(inspection_path),
        "inspection_sha256": inspection["inspection_sha256"],
        "private_artifact": {
            "cache_relative_path": str(relative),
            "content_sha256": private_value["content_sha256"],
            "file_sha256": hashlib.sha256(raw).hexdigest(),
            "compressed_bytes": len(raw),
        },
        "provider_telemetry": {
            "request_count": len(pages),
            "request_seconds": round(elapsed_seconds, 6),
            "pacing_wait_seconds": round(pacing_wait_seconds, 6),
            "cache_hits": 0,
            "failures": 0,
        },
        "row_count": row_count,
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


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    freeze = subparsers.add_parser("freeze-contract")
    freeze.add_argument("--created-at", required=True)
    collect_parser = subparsers.add_parser("collect")
    collect_parser.add_argument("contract", type=Path)
    collect_parser.add_argument("inspection", type=Path)
    collect_parser.add_argument("--collected-at", required=True)
    args = parser.parse_args(argv)
    if args.command == "freeze-contract":
        path, value = freeze_contract(created_at=args.created_at)
        digest = value["contract_sha256"]
        state = "METADATA_CONTRACT_FROZEN"
    else:
        path, value = collect(
            args.contract,
            args.inspection,
            collected_at=args.collected_at,
        )
        digest = value["collection_sha256"]
        state = value["state"]
    print(
        json.dumps(
            {
                "path": _repo_path(path),
                "sha256": digest,
                "state": state,
                "provider_requests": value.get(
                    "provider_telemetry", {}
                ).get("request_count", 0),
                "row_count": value.get("row_count", 0),
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

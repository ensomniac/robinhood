"""Collect the development-only Yahoo graph bound to the v13 search."""

from __future__ import annotations

import argparse
import json
import time
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Any

import requests

import earnings_sec_corrected_expansion as capacity
import earnings_sec_market_data as market
import earnings_sec_reaction_v11_collection as v11_collection
import earnings_sec_reaction_v13_search as search_source
import earnings_sec_yahoo_data as yahoo
import outcome_exposure
import strategy_discovery
from historical_store import (
    HistoricalDayStore,
    canonical_sha256,
    sha256_file,
)


PROJECT_ROOT = Path(__file__).resolve().parent
CAMPAIGN_ID = search_source.CAMPAIGN_ID
FAMILY_ID = search_source.FAMILY_ID
SUCCESSOR_ID = search_source.SUCCESSOR_ID
DEFAULT_ROOT = search_source.DEFAULT_ROOT
PRIVATE_NAMESPACE = "_derived/earnings_sec_reaction_v13"


class EarningsSecReactionV13CollectionError(RuntimeError):
    """The inspected v13 search or development graph drifted."""


def _task_path(
    store: HistoricalDayStore,
    search_sha256: str,
    request_sha256: str,
) -> Path:
    return (
        store.root
        / PRIVATE_NAMESPACE
        / search_sha256
        / "tasks"
        / f"{request_sha256}.json.gz"
    )


def _fetch(
    request: Mapping[str, Any],
    session: requests.Session,
    telemetry: dict[str, Any],
) -> dict[str, Any]:
    return v11_collection._fetch(request, session, telemetry)


def collect(
    search_path: Path,
    inspection_path: Path,
    *,
    collected_at: str,
    store: HistoricalDayStore | None = None,
    root: Path = DEFAULT_ROOT,
    fetcher: Callable[[Mapping[str, Any]], dict[str, Any]] | None = None,
) -> tuple[Path, dict[str, Any]]:
    strategy_discovery.require_committed(Path(__file__).resolve())
    strategy_discovery.require_committed(search_path)
    strategy_discovery.require_committed(inspection_path)
    search = strategy_discovery.load_artifact(
        search_path, expected_kind="frozen-development-search"
    )
    inspection = capacity._read(inspection_path)
    expected_requests = len(
        search["family_contract"]["development_data_requests"]
    )
    if not (
        inspection.get("inspection_sha256")
        == capacity.self_hash(inspection, "inspection_sha256")
        and inspection.get("state")
        == "REACTION_V13_SEARCH_INSPECTED_READY_FOR_COLLECTION"
        and inspection.get("valid") is True
        and inspection.get("search_sha256") == search["artifact_sha256"]
        and inspection.get("development_collection_authorized") is True
        and inspection.get("authorized_provider_requests")
        == expected_requests
        and inspection.get("confirmation_provider_access_authorized")
        is False
    ):
        raise EarningsSecReactionV13CollectionError(
            "v13 search collection authorization is invalid"
        )
    contract = search["family_contract"]
    strategy_discovery._assert_implementation_current(
        contract, enforce_commit=True
    )
    historical_store = store or HistoricalDayStore.from_env()
    selected = search_source.selection(historical_store)
    if (
        canonical_sha256(selected)
        != contract["universe"]["selection_sha256"]
    ):
        raise EarningsSecReactionV13CollectionError(
            "v13 metadata selection drifted"
        )
    telemetry: dict[str, Any] = {
        "requests": 0,
        "request_seconds": 0.0,
        "pacing_wait_seconds": 0.0,
        "cache_hits": 0,
        "failures": 0,
        "permanent_missing_responses": 0,
    }
    http = requests.Session()
    tasks: list[dict[str, Any]] = []
    try:
        for index, request in enumerate(
            contract["development_data_requests"]
        ):
            path = _task_path(
                historical_store,
                search["artifact_sha256"],
                request["request_sha256"],
            )
            if path.is_file():
                task = market._read_private(path)
                if not (
                    task.get("request_sha256")
                    == request["request_sha256"]
                    and task.get("task_sha256")
                    == capacity.self_hash(task, "task_sha256")
                ):
                    raise EarningsSecReactionV13CollectionError(
                        "cached v13 task hash differs"
                    )
                telemetry["cache_hits"] += 1
            else:
                if (
                    fetcher is None
                    and index > 0
                    and telemetry["requests"] > 0
                ):
                    time.sleep(yahoo.PACE_SECONDS)
                    telemetry["pacing_wait_seconds"] += (
                        yahoo.PACE_SECONDS
                    )
                task = (
                    fetcher(request)
                    if fetcher is not None
                    else _fetch(request, http, telemetry)
                )
                if fetcher is not None:
                    telemetry["requests"] += 1
                    if "task_sha256" not in task:
                        task["task_sha256"] = capacity.self_hash(
                            task, "task_sha256"
                        )
                if not (
                    task.get("request_sha256")
                    == request["request_sha256"]
                    and task.get("task_sha256")
                    == capacity.self_hash(task, "task_sha256")
                ):
                    raise EarningsSecReactionV13CollectionError(
                        "v13 task does not match the frozen request"
                    )
                market._write_private(path, task)
            tasks.append(task)
    finally:
        http.close()
    if telemetry["requests"] + telemetry["cache_hits"] != expected_requests:
        raise EarningsSecReactionV13CollectionError(
            "v13 request and cache accounting is incomplete"
        )
    bars: dict[str, list[dict[str, Any]]] = {}
    missing: dict[str, str] = {}
    for task in tasks:
        if task["status"] == "COMPLETE":
            bars[str(task["symbol"])] = list(task["rows"])
        elif task["status"] == "PERMANENT_MISSING":
            missing[str(task["symbol"])] = str(task["missing_reason"])
        else:
            raise EarningsSecReactionV13CollectionError(
                "v13 task status is invalid"
            )
    dataset = {
        "schema_version": 1,
        "family_id": FAMILY_ID,
        "evaluation_dates": selected["development_dates"],
        "event_metadata_by_date": selected[
            "development_metadata_by_date"
        ],
        "daily_bars": bars,
        "missing_symbols": missing,
        "source_semantics": {
            "search_sha256": search["artifact_sha256"],
            "provider": "Yahoo Finance historical chart JSON",
            "raw_ohlcv_used": True,
            "dividend_adjusted_close_used": False,
            "http_400_is_permanent_missing": True,
            "confirmation_prices_accessed": False,
        },
    }
    relative = (
        Path(PRIVATE_NAMESPACE)
        / search["artifact_sha256"]
        / "development.json.gz"
    )
    private_path = historical_store.root / relative
    market._write_private(private_path, dataset)
    value: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": (
            "earnings-sec-reaction-v13-development-collection"
        ),
        "campaign_id": CAMPAIGN_ID,
        "family_id": FAMILY_ID,
        "successor_id": SUCCESSOR_ID,
        "state": "REACTION_V13_DEVELOPMENT_COLLECTED_UNINSPECTED",
        "collected_at": capacity._timestamp(
            collected_at, "collected_at"
        ),
        "search_path": capacity._repo_path(search_path),
        "search_file_sha256": sha256_file(search_path),
        "search_sha256": search["artifact_sha256"],
        "inspection_path": capacity._repo_path(inspection_path),
        "inspection_file_sha256": sha256_file(inspection_path),
        "inspection_sha256": inspection["inspection_sha256"],
        "external_relative_path": str(relative),
        "external_file_sha256": sha256_file(private_path),
        "dataset_sha256": canonical_sha256(dataset),
        "symbols_requested": expected_requests,
        "symbols_complete": len(bars),
        "symbols_permanently_missing": len(missing),
        "row_count": sum(len(rows) for rows in bars.values()),
        "provider_telemetry": telemetry,
        "substitutions": 0,
        "retries": 0,
        "strategy_metrics_computed": 0,
        "confirmation_prices_accessed": False,
        "broker_actions": 0,
    }
    value["collection_sha256"] = capacity.self_hash(
        value, "collection_sha256"
    )
    path = (
        root
        / "development-collection"
        / f"collection-{value['collection_sha256']}.json"
    )
    capacity._write(path, value)
    outcome_exposure.ensure_record(
        outcome_exposure.build_record(
            exposure_id=(
                f"source-{FAMILY_ID}-v13-"
                f"{value['collection_sha256'][:16]}"
            ),
            campaign_id=CAMPAIGN_ID,
            lane="development",
            recorded_at=value["collected_at"],
            source_path=capacity._repo_path(path),
            source_sha256=sha256_file(path),
            scope=contract["development_scope"],
        )
    )
    return path, value


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("search", type=Path)
    parser.add_argument("inspection", type=Path)
    parser.add_argument("--collected-at", required=True)
    args = parser.parse_args(argv)
    path, value = collect(
        args.search,
        args.inspection,
        collected_at=args.collected_at,
    )
    print(
        json.dumps(
            {
                "path": capacity._repo_path(path),
                "sha256": value["collection_sha256"],
                "state": value["state"],
                "provider_telemetry": value["provider_telemetry"],
                "symbols_complete": value["symbols_complete"],
                "symbols_missing": value[
                    "symbols_permanently_missing"
                ],
                "confirmation_prices_accessed": False,
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())

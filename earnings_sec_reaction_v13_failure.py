"""Record the terminal v13 Yahoo invalid-OHLCV source boundary."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

import earnings_sec_corrected_expansion as capacity
import earnings_sec_market_data as market
import earnings_sec_reaction_v13_collection as collection
import outcome_exposure
import strategy_discovery
from historical_store import HistoricalDayStore, sha256_file


SEARCH = (
    capacity.PROJECT_ROOT
    / "strategy_tournament/v2/discovery/"
    "earnings-sec-yoy-eps-reaction-drift/search/"
    "earnings-sec-yoy-eps-reaction-drift-search-"
    "36a9738a580d04c53bd94b5a683b341febdc115e1666396f58d5d1ac22852da7"
    ".json"
)
SEARCH_INSPECTION = (
    capacity.DEFAULT_ROOT
    / "search-inspection"
    / "inspection-"
    "52bbde4c0d4136aaf7c7e6d1a486cee745064afdc1efc0ab719679ebc3ab64cd"
    ".json"
)
RETAINED_TASK_COUNT = 26
FAILED_ORDINAL = 26
FAILED_SYMBOL = "AMCF"
FAILURE_CODE = "YAHOO_INVALID_OHLCV"


class EarningsSecReactionV13FailureError(RuntimeError):
    """The exact v13 partial-source failure boundary drifted."""


def build_failure(
    *,
    failed_at: str,
    store: HistoricalDayStore | None = None,
) -> dict[str, Any]:
    for path in (
        Path(__file__).resolve(),
        capacity.PROJECT_ROOT
        / "earnings_sec_reaction_v13_failure_inspection.py",
        SEARCH,
        SEARCH_INSPECTION,
    ):
        strategy_discovery.require_committed(path)
    search = strategy_discovery.load_artifact(
        SEARCH, expected_kind="frozen-development-search"
    )
    inspection = capacity._read(SEARCH_INSPECTION)
    requests = search["family_contract"]["development_data_requests"]
    if not (
        search.get("artifact_sha256")
        == "36a9738a580d04c53bd94b5a683b341febdc115e1666396f58d5d1ac22852da7"
        and inspection.get("inspection_sha256")
        == capacity.self_hash(inspection, "inspection_sha256")
        and inspection.get("state")
        == "REACTION_V13_SEARCH_INSPECTED_READY_FOR_COLLECTION"
        and inspection.get("valid") is True
        and inspection.get("search_sha256") == search["artifact_sha256"]
        and len(requests) == 635
        and requests[FAILED_ORDINAL]["symbol"] == FAILED_SYMBOL
    ):
        raise EarningsSecReactionV13FailureError(
            "v13 inspected search lineage is invalid"
        )
    historical_store = store or HistoricalDayStore.from_env()
    retained: list[dict[str, Any]] = []
    for request in requests[:RETAINED_TASK_COUNT]:
        path = collection._task_path(
            historical_store,
            search["artifact_sha256"],
            request["request_sha256"],
        )
        if not path.is_file():
            raise EarningsSecReactionV13FailureError(
                "v13 retained task prefix is incomplete"
            )
        task = market._read_private(path)
        if not (
            task.get("request_sha256") == request["request_sha256"]
            and task.get("task_sha256")
            == capacity.self_hash(task, "task_sha256")
            and task.get("symbol") == request["symbol"]
            and task.get("status") in {"COMPLETE", "PERMANENT_MISSING"}
        ):
            raise EarningsSecReactionV13FailureError(
                "v13 retained task prefix differs"
            )
        retained.append(
            {
                "symbol": request["symbol"],
                "request_sha256": request["request_sha256"],
                "task_sha256": task["task_sha256"],
                "task_file_sha256": sha256_file(path),
                "status": task["status"],
                "row_count": len(task["rows"]),
            }
        )
    failed_request = requests[FAILED_ORDINAL]
    failed_path = collection._task_path(
        historical_store,
        search["artifact_sha256"],
        failed_request["request_sha256"],
    )
    if failed_path.exists():
        raise EarningsSecReactionV13FailureError(
            "failed v13 request unexpectedly retained a task"
        )
    accessed_symbols = [
        str(request["symbol"])
        for request in requests[: FAILED_ORDINAL + 1]
    ]
    opened_scope = {
        "dates": list(search["family_contract"]["development_scope"]["dates"]),
        "symbols": accessed_symbols,
    }
    if outcome_exposure.find_overlaps(
        opened_scope, outcome_exposure.read_index()
    ):
        raise EarningsSecReactionV13FailureError(
            "v13 failure scope was already exposed before this attempt"
        )
    value: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": (
            "earnings-sec-reaction-v13-development-source-failure"
        ),
        "campaign_id": capacity.CAMPAIGN_ID,
        "family_id": collection.FAMILY_ID,
        "successor_id": capacity.SUCCESSOR_ID,
        "state": "REACTION_V13_INVALID_OHLCV_TERMINAL",
        "failed_at": capacity._timestamp(failed_at, "failed_at"),
        "search_path": capacity._repo_path(SEARCH),
        "search_file_sha256": sha256_file(SEARCH),
        "search_sha256": search["artifact_sha256"],
        "inspection_path": capacity._repo_path(SEARCH_INSPECTION),
        "inspection_file_sha256": sha256_file(SEARCH_INSPECTION),
        "inspection_sha256": inspection["inspection_sha256"],
        "failure": {
            "code": FAILURE_CODE,
            "exception_type": "EarningsSecYahooDataError",
            "message": "Yahoo chart contains invalid OHLCV",
            "failed_ordinal": FAILED_ORDINAL,
            "failed_request": failed_request,
            "response_body_retained": False,
            "failing_row_retained": False,
        },
        "retained_tasks": retained,
        "provider_telemetry": {
            "attempted_requests": FAILED_ORDINAL + 1,
            "retained_tasks": len(retained),
            "cache_hits": 0,
            "failures": 1,
        },
        "opened_scope": opened_scope,
        "development_prices_accessed": True,
        "strategy_metrics_computed": 0,
        "confirmation_prices_accessed": False,
        "broker_actions": 0,
        "same_version_resume_permitted": False,
        "successor_authority": {
            "permanently_excluded_symbols": accessed_symbols,
            "remaining_untouched_symbols": 635 - len(accessed_symbols),
            "remaining_prefix_successor_permitted_after_inspection": True,
            "invalid_ohlcv_permanent_missing_policy_permitted": True,
            "same_32_trial_grid_required": True,
            "same_64_trial_selection_accounting_required": True,
            "search_freeze_required_before_remaining_access": True,
            "confirmation_access_permitted": False,
        },
    }
    value["failure_sha256"] = capacity.self_hash(
        value, "failure_sha256"
    )
    return value


def record_failure(
    *,
    failed_at: str,
    store: HistoricalDayStore | None = None,
    root: Path = capacity.DEFAULT_ROOT,
) -> tuple[Path, dict[str, Any]]:
    value = build_failure(failed_at=failed_at, store=store)
    path = (
        root
        / "development-source-failure"
        / f"failure-{value['failure_sha256']}.json"
    )
    capacity._write(path, value)
    return path, value


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("record",))
    parser.add_argument("--failed-at", required=True)
    args = parser.parse_args(argv)
    path, value = record_failure(failed_at=args.failed_at)
    print(
        json.dumps(
            {
                "path": capacity._repo_path(path),
                "failure_sha256": value["failure_sha256"],
                "state": value["state"],
                "attempted_requests": value["provider_telemetry"][
                    "attempted_requests"
                ],
                "retained_tasks": value["provider_telemetry"][
                    "retained_tasks"
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

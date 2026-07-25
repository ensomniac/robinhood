"""Record the pre-search Yahoo response-schema failure and exposed scope."""

from __future__ import annotations

import argparse
import json
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import earnings_sec_eps_capacity as v5
import earnings_sec_market_data as market
import earnings_sec_yahoo_data as source
import outcome_exposure
import strategy_discovery
from historical_store import HistoricalDayStore, sha256_file


DEFAULT_ROOT = source.DEFAULT_ROOT
FAILED_SYMBOL = "ANN"
COMPLETED_SYMBOLS = ["AAPL", "ADSK", "ALGN", "AMZN"]
EXPOSED_SYMBOLS = sorted([*COMPLETED_SYMBOLS, FAILED_SYMBOL])


class EarningsSecYahooFailureError(RuntimeError):
    """The exact pre-search Yahoo failure boundary cannot be proven."""


def _task_summary(
    contract: dict[str, Any],
    store: HistoricalDayStore,
) -> tuple[list[dict[str, Any]], int]:
    tasks: list[dict[str, Any]] = []
    rows = 0
    for request in contract["requests"]:
        path = source._task_path(
            store,
            contract["contract_sha256"],
            request["request_sha256"],
        )
        if not path.is_file():
            continue
        task = market._read_private(path)
        if not (
            task.get("request_sha256") == request["request_sha256"]
            and task.get("task_sha256")
            == v5.self_hash(task, "task_sha256")
            and task.get("status") == "COMPLETE"
        ):
            raise EarningsSecYahooFailureError(
                "retained Yahoo task is invalid"
            )
        tasks.append(
            {
                "symbol": task["symbol"],
                "request_sha256": task["request_sha256"],
                "task_sha256": task["task_sha256"],
                "rows": len(task["rows"]),
            }
        )
        rows += len(task["rows"])
    return tasks, rows


def build_failure(
    contract_path: Path,
    inspection_path: Path,
    *,
    observed_at: str,
    store: HistoricalDayStore | None = None,
) -> dict[str, Any]:
    strategy_discovery.require_committed(contract_path)
    strategy_discovery.require_committed(inspection_path)
    contract = market._read(contract_path)
    inspection = market._read(inspection_path)
    if not (
        contract.get("contract_sha256")
        == v5.self_hash(contract, "contract_sha256")
        and inspection.get("contract_sha256") == contract["contract_sha256"]
        and inspection.get("state") == "YAHOO_CONTRACT_INSPECTED_READY"
        and inspection.get("valid") is True
        and contract["implementation_hashes"]["earnings_sec_yahoo_data.py"]
        == sha256_file(source.PROJECT_ROOT / "earnings_sec_yahoo_data.py")
    ):
        raise EarningsSecYahooFailureError(
            "failed Yahoo contract lineage differs"
        )
    historical_store = store or HistoricalDayStore.from_env()
    tasks, retained_rows = _task_summary(contract, historical_store)
    if [task["symbol"] for task in tasks] != COMPLETED_SYMBOLS:
        raise EarningsSecYahooFailureError(
            "Yahoo completed-task boundary differs"
        )
    failed = contract["requests"][len(tasks)]
    if failed["symbol"] != FAILED_SYMBOL:
        raise EarningsSecYahooFailureError(
            "Yahoo failed request boundary differs"
        )
    value: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": "earnings-sec-yahoo-pre-search-failure",
        "campaign_id": source.CAMPAIGN_ID,
        "family_id": source.FAMILY_ID,
        "successor_id": source.SUCCESSOR_ID,
        "state": "YAHOO_PRE_SEARCH_RESPONSE_SCHEMA_FAILURE",
        "observed_at": market._timestamp(observed_at, "observed_at"),
        "contract_path": market._repo_path(contract_path),
        "contract_file_sha256": sha256_file(contract_path),
        "contract_sha256": contract["contract_sha256"],
        "inspection_path": market._repo_path(inspection_path),
        "inspection_file_sha256": sha256_file(inspection_path),
        "inspection_sha256": inspection["inspection_sha256"],
        "completed_tasks": tasks,
        "failed_request": {
            "ordinal": len(tasks) + 1,
            "symbol": failed["symbol"],
            "request_sha256": failed["request_sha256"],
            "endpoint": failed["endpoint"],
        },
        "error": {
            "category": "permanent_response_schema",
            "sanitized_message": (
                "Yahoo chart identity, timezone, or quote arrays drifted"
            ),
        },
        "failure_boundary": {
            "provider_requests": len(tasks) + 1,
            "provider_responses": len(tasks) + 1,
            "tasks_checkpointed": len(tasks),
            "rows_retained": retained_rows,
            "symbols_with_retained_rows": COMPLETED_SYMBOLS,
            "symbols_with_opened_responses": EXPOSED_SYMBOLS,
            "strategy_search_frozen_before_access": False,
            "strategy_metrics_computed": 0,
            "winner_selection_executed": False,
            "confirmation_prices_accessed": False,
            "broker_actions": 0,
        },
        "disposition": {
            "v9_promotion_eligible": False,
            "same_contract_resume_permitted": False,
            "failed_symbol_retry_permitted": False,
            "exposed_symbols_reusable_for_promotion": False,
            "remaining_unopened_symbols_successor_permitted": True,
            "successor_requires_search_and_evaluator_freeze_first": True,
            "strategy_semantics_change_permitted": False,
        },
    }
    value["failure_sha256"] = v5.self_hash(value, "failure_sha256")
    return value


def record_failure(
    contract_path: Path,
    inspection_path: Path,
    *,
    observed_at: str,
    store: HistoricalDayStore | None = None,
    root: Path = DEFAULT_ROOT,
) -> tuple[Path, dict[str, Any]]:
    value = build_failure(
        contract_path,
        inspection_path,
        observed_at=observed_at,
        store=store,
    )
    path = (
        root
        / "yahoo-pre-search-failure"
        / f"failure-{value['failure_sha256']}.json"
    )
    market._write(path, value)
    contract = market._read(contract_path)
    scope = {
        "dates": contract["development_scope"]["dates"],
        "symbols": EXPOSED_SYMBOLS,
    }
    outcome_exposure.ensure_record(
        outcome_exposure.build_record(
            exposure_id=(
                f"source-{source.FAMILY_ID}-yahoo-presearch-"
                f"{value['failure_sha256'][:16]}"
            ),
            campaign_id=source.CAMPAIGN_ID,
            lane="development",
            recorded_at=value["observed_at"],
            source_path=market._repo_path(path),
            source_sha256=sha256_file(path),
            scope=scope,
        )
    )
    return path, value


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("contract", type=Path)
    parser.add_argument("inspection", type=Path)
    parser.add_argument("--observed-at", required=True)
    args = parser.parse_args(argv)
    path, value = record_failure(
        args.contract,
        args.inspection,
        observed_at=args.observed_at,
    )
    print(
        json.dumps(
            {
                "path": market._repo_path(path),
                "sha256": value["failure_sha256"],
                "state": value["state"],
                "failure_boundary": value["failure_boundary"],
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

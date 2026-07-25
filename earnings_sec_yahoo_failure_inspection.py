"""Independently inspect the pre-search Yahoo failure and exposure."""

from __future__ import annotations

import argparse
import json
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import earnings_sec_eps_capacity as v5
import earnings_sec_market_data as market
import earnings_sec_yahoo_data as yahoo
import earnings_sec_yahoo_failure as source
import outcome_exposure
import strategy_discovery
from historical_store import HistoricalDayStore, sha256_file


class EarningsSecYahooFailureInspectionError(RuntimeError):
    """The Yahoo pre-search failure could not be independently closed."""


def inspect_failure(
    failure_path: Path,
    *,
    inspected_at: str,
    store: HistoricalDayStore | None = None,
    root: Path = source.DEFAULT_ROOT,
) -> tuple[Path, dict[str, Any]]:
    strategy_discovery.require_committed(Path(__file__).resolve())
    strategy_discovery.require_committed(failure_path)
    failure = market._read(failure_path)
    contract_path = market.PROJECT_ROOT / failure["contract_path"]
    inspection_path = market.PROJECT_ROOT / failure["inspection_path"]
    rebuilt = source.build_failure(
        contract_path,
        inspection_path,
        observed_at=failure["observed_at"],
        store=store,
    )
    exposure_id = (
        f"source-{yahoo.FAMILY_ID}-yahoo-presearch-"
        f"{failure['failure_sha256'][:16]}"
    )
    exposure = [
        record
        for record in outcome_exposure.read_index()
        if record["exposure_id"] == exposure_id
    ]
    checks = {
        "failure_hash_valid": failure["failure_sha256"]
        == v5.self_hash(failure, "failure_sha256"),
        "failure_exactly_rebuilt": failure == rebuilt,
        "four_tasks_bound": len(failure["completed_tasks"]) == 4
        and [row["symbol"] for row in failure["completed_tasks"]]
        == source.COMPLETED_SYMBOLS,
        "fifth_request_bound": failure["failed_request"]["ordinal"] == 5
        and failure["failed_request"]["symbol"] == source.FAILED_SYMBOL,
        "schema_error_exact": failure["error"]["category"]
        == "permanent_response_schema",
        "exposed_scope_indexed": len(exposure) == 1
        and exposure[0]["scope"]["symbols"] == source.EXPOSED_SYMBOLS,
        "pre_search_access_explicit": failure["failure_boundary"][
            "strategy_search_frozen_before_access"
        ]
        is False,
        "zero_metrics_or_winner": failure["failure_boundary"][
            "strategy_metrics_computed"
        ]
        == 0
        and failure["failure_boundary"]["winner_selection_executed"] is False,
        "confirmation_closed": failure["failure_boundary"][
            "confirmation_prices_accessed"
        ]
        is False,
        "v9_ineligible": failure["disposition"]["v9_promotion_eligible"]
        is False,
        "exposed_symbols_closed": failure["disposition"][
            "exposed_symbols_reusable_for_promotion"
        ]
        is False,
        "search_freeze_required": failure["disposition"][
            "successor_requires_search_and_evaluator_freeze_first"
        ]
        is True,
        "broker_actions_zero": failure["failure_boundary"]["broker_actions"]
        == 0,
    }
    if not all(checks.values()):
        raise EarningsSecYahooFailureInspectionError(
            "Yahoo pre-search failure inspection failed"
        )
    value: dict[str, Any] = {
        "schema_version": 1,
        "artifact_kind": (
            "earnings-sec-yahoo-pre-search-failure-inspection"
        ),
        "campaign_id": yahoo.CAMPAIGN_ID,
        "family_id": yahoo.FAMILY_ID,
        "successor_id": yahoo.SUCCESSOR_ID,
        "state": "YAHOO_PRE_SEARCH_FAILURE_INSPECTED_TERMINAL",
        "inspected_at": market._timestamp(inspected_at, "inspected_at"),
        "failure_path": market._repo_path(failure_path),
        "failure_file_sha256": sha256_file(failure_path),
        "failure_sha256": failure["failure_sha256"],
        "checks": checks,
        "v9_promotion_eligible": False,
        "remaining_unopened_symbols_successor_permitted": True,
        "successor_search_freeze_required_before_access": True,
        "exposed_symbols": source.EXPOSED_SYMBOLS,
        "confirmation_prices_accessed": False,
        "strategy_metrics_computed": 0,
        "broker_actions": 0,
        "valid": True,
    }
    value["inspection_sha256"] = v5.self_hash(
        value, "inspection_sha256"
    )
    path = (
        root
        / "yahoo-pre-search-failure-inspection"
        / f"inspection-{value['inspection_sha256']}.json"
    )
    market._write(path, value)
    return path, value


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("failure", type=Path)
    parser.add_argument("--inspected-at", required=True)
    args = parser.parse_args(argv)
    path, value = inspect_failure(
        args.failure, inspected_at=args.inspected_at
    )
    print(
        json.dumps(
            {
                "path": market._repo_path(path),
                "sha256": value["inspection_sha256"],
                "state": value["state"],
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
